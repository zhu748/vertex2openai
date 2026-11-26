"""Partial optional probability diagnostics cannot invalidate an otherwise valid completion."""
import json
from unittest.mock import AsyncMock

from google.genai import types
from message_processing import logprobs_payload_from_candidate
from test_req_round15_contracts import Round15Harness, request, response
from test_schema_contract_fidelity import MODEL


def strict_json(data):
    def reject(value):
        raise AssertionError("Nonstandard JSON numeric constant: " + value)
    return json.loads(data, parse_constant=reject)


def scored(token="ok", value=-.2):
    return {"token": token, "logProbability": value}


def probability_source(chosen, alternatives):
    src = response(types.Part(text="OK"))
    src.candidates[0].logprobs_result = types.LogprobsResult.model_validate({
        "chosenCandidates": chosen, "topCandidates": [{"candidates": alternatives}]})
    return src


class LogprobWire(Round15Harness):
    async def test_invalid_optional_entries_cannot_break_json_or_either_sse_mode(self):
        expected = [("ok", -.2), ("", 0.), ("fine", -1.)]
        src = probability_source([scored()], [
            scored(), scored("a", "-Infinity"), {"token": "no-score"}, scored("", 0),
            {"logProbability": -1}, scored("b", "NaN"), scored("fine", -1), scored("c", "Infinity")])
        for mode in ("json", "true", "fake"):
            self.settings["fake_streaming"] = mode == "fake"

            async def stream(**kwargs):
                async def gen():
                    yield src
                return gen()

            req = request(stream=mode != "json", logprobs=True).model_copy(update={"model": MODEL})
            r = await self.express(req, AsyncMock(return_value=src), stream)
            if mode == "json":
                choices = strict_json(r.body)["choices"]
                self.assertEqual(choices[0]["message"]["content"], "OK")
            else:
                body = await self.body(r)
                items = [strict_json(line[6:]) for line in body.splitlines()
                         if line.startswith("data: ") and line != "data: [DONE]"]
                self.assertFalse(any("error" in item for item in items))
                choices = [c for item in items for c in item["choices"]]
                self.assertEqual("".join(c["delta"].get("content") or "" for c in choices), "OK")
            token, = [token for c in choices if c.get("logprobs") for token in c["logprobs"]["content"]]
            self.assertEqual([(a["token"], a["logprob"]) for a in token["top_logprobs"]], expected)
            self.assertEqual(token["top_logprobs"][1]["bytes"], [])

    def test_chosen_gap_omits_option_without_shifting_later_tokens(self):
        for bad in ({"token": "mid"}, {"logProbability": -1}, scored("bad", "-Infinity")):
            src = probability_source([scored("first"), bad, scored("last")], [])
            self.assertIsNone(logprobs_payload_from_candidate(src.candidates[0]))
