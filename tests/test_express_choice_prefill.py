"""Exercise Express's real three delivery modes with a local, deterministic Gemini SDK."""
import json
from types import SimpleNamespace
from unittest import mock

import httpx
from google.genai import types

from main import app
from runtime_state import app_state
from test_req_round15_contracts import Round15Harness, chunks, request as sdk_request
from upstreams import express_pipeline, express_sdk


PREFIX = "head-abcdefgh:"


def request(**options):
    return sdk_request(**options).model_copy(update={"model": "gemini-2.5-flash"})


def candidate(index, *parts, finish=None):
    return types.Candidate(index=index, content=types.Content(role="model", parts=list(parts)), finish_reason=finish)


def packet(*candidates):
    return types.GenerateContentResponse(candidates=list(candidates))


def stream_of(*packets):
    async def stream(**kwargs):
        async def source():
            for item in packets:
                yield item
        return source()
    return stream


def choices(body):
    return [c for f in chunks(body) for c in f["choices"]]


def visible_by_index(items):
    result = {}
    for c in items:
        index = c["index"]
        result[index] = result.get(index, "") + (c["delta"].get("content") or "")
    return result


class ExpressChoicePrefillTests(Round15Harness):
    async def test_split_echoes_match_json_and_fake_sse_when_candidate_order_changes(self):
        self.settings.update(inject_prefill=PREFIX, prefill_mode="smart")
        generate = mock.AsyncMock(return_value=packet(
            candidate(1, types.Part(text=PREFIX + "blue"), finish="STOP"),
            candidate(0, types.Part(text=PREFIX + "red"), finish="STOP")))
        stream = stream_of(
            packet(candidate(1, types.Part(text="hea")), candidate(0, types.Part(text="head-abc"))),
            packet(candidate(0, types.Part(text="defgh:red"), finish="STOP")),
            packet(candidate(1, types.Part(text="d-abcdefgh:blue"), finish="STOP")))
        sdk = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
            generate_content=generate, generate_content_stream=stream)))
        self.enterContext(mock.patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        self.enterContext(mock.patch.object(app.state, "express_key_manager", self.key_manager))
        self.enterContext(mock.patch.object(app_state, "get_channel_mode", return_value="express"))
        self.enterContext(mock.patch.object(app_state, "is_web_proxy_enabled", return_value=False))
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                   headers={"Authorization": "Bearer test-key", "X-Resumable": "0"})
        self.addAsyncCleanup(client.aclose)
        path = "/v1/chat/completions"
        data = request(n=2).model_dump(mode="json", exclude_unset=True)
        actual = await client.post(path, json=data)
        self.assertEqual(actual.status_code, 200, actual.text)
        expected = {0: PREFIX + "red", 1: PREFIX + "blue"}
        self.assertEqual({c["index"]: c["message"]["content"] for c in actual.json()["choices"]}, expected)
        for fake in (False, True):
            with self.subTest(fake=fake):
                self.settings["fake_streaming"] = fake
                streamed = await client.post(path, json=dict(data, stream=True))
                self.assertEqual(streamed.status_code, 200, streamed.text)
                body = streamed.text
                self.assertTrue(body.endswith("data: [DONE]\n\n"))
                items = choices(body)
                self.assertEqual(visible_by_index(items), expected)
                for index in expected:
                    mine = [c for c in items if c["index"] == index]
                    self.assertEqual([c["delta"]["role"] for c in mine if "role" in c["delta"]], ["assistant"])
                    self.assertEqual([c["finish_reason"] for c in mine if c.get("finish_reason")], ["stop"])

    async def test_sparse_indices_flush_independently_and_keep_tool_topology(self):
        self.settings.update(inject_prefill=PREFIX, prefill_mode="smart", fake_streaming=False)
        first = candidate(2, types.Part(text="abcd"))
        first.logprobs_result = types.LogprobsResult(chosen_candidates=[
            types.LogprobsResultCandidate(token="abcd", log_probability=-0.5)])
        stream = stream_of(
            packet(first, candidate(0, types.Part(text="head", thought_signature=b"A"))),
            packet(candidate(0, types.Part(function_call=types.FunctionCall(id="t", name="look", args={"n": 2}),
                                           thought_signature=b"B"))),
            packet(candidate(2, types.Part(text="efgh:"), finish="STOP")),
            packet(candidate(0, types.Part(text="-", thought_signature=b"C"), finish="STOP")))
        body = await self.body(await self.express(request(n=3, stream=True), stream=stream))
        items = choices(body)
        self.assertEqual(visible_by_index(items), {2: PREFIX, 0: PREFIX + "head-"})
        logprobs = [(c["index"], p["token"]) for c in items if c.get("logprobs") for p in c["logprobs"]["content"]]
        self.assertEqual(logprobs, [(2, "abcd")])
        finishes = [c for c in items if c.get("finish_reason")]
        self.assertEqual([(c["index"], c["finish_reason"]) for c in finishes], [(2, "stop"), (0, "tool_calls")])
        tool = next(c for c in items if c["delta"].get("tool_calls"))
        self.assertEqual(tool["index"], 0)
        call, = tool["delta"]["tool_calls"]
        self.assertEqual((call["index"], call["function"]["name"], json.loads(call["function"]["arguments"])),
                         (0, "look", {"n": 2}))
        self.assertEqual(call["extra_content"]["google"]["thought_signature"], "Qg==")
        metadata = finishes[-1]["delta"]["extra_content"]["google"]
        self.assertEqual(metadata["part_order"], [
            {"type": "ordinary", "index": 0}, {"type": "tool_call", "index": 0}, {"type": "ordinary", "index": 1}])
        self.assertEqual([p["thought_signature"] for p in metadata["ordinary_parts"]], ["QQ==", "Qw=="])

    async def test_echo_only_is_a_reply_but_idle_attempt_does_not_commit_a_prefill(self):
        self.settings.update(inject_prefill=PREFIX, prefill_mode="smart", fake_streaming=False,
                             retry_max=1, retry_backoff_seconds=0, retry_jitter_seconds=0)
        attempted = 0

        async def stream(**kwargs):
            nonlocal attempted
            attempted += 1
            if attempted == 1:
                async def idle():
                    yield types.GenerateContentResponse(usage_metadata={"prompt_token_count": 1})
                    yield packet(candidate(1, types.Part(thought_signature=b"uncommitted")))
                    raise httpx.ReadTimeout("idle test")
                return idle()
            return await stream_of(packet(candidate(1, types.Part(text=PREFIX), finish="STOP"),
                                          candidate(0, types.Part(text="abcdefgh:"), finish="STOP")))(**kwargs)
        body = await self.body(await self.express(request(n=2, stream=True), stream=stream))
        self.assertEqual(attempted, 2)
        self.assertNotIn('"error"', body)
        self.assertNotIn("dW5jb21taXR0ZWQ=", body)
        items = choices(body)
        self.assertEqual(visible_by_index(items), {1: PREFIX, 0: PREFIX})
        self.assertEqual([c["index"] for c in items if c.get("finish_reason") == "stop"], [1, 0])

    async def test_no_prefill_allocates_no_choice_state_or_phantom_roles(self):
        self.settings.update(inject_prefill="", fake_streaming=False)
        stream = stream_of(packet(candidate(2, types.Part(text=PREFIX + "blue"), finish="STOP"),
                                  candidate(0, types.Part(text="red"), finish="STOP")))
        with mock.patch.object(express_pipeline, "ChoicePrefills", side_effect=AssertionError("unexpected deduper")):
            body = await self.body(await self.express(request(n=3, stream=True), stream=stream))
        items = choices(body)
        self.assertEqual(visible_by_index(items), {2: PREFIX + "blue", 0: "red"})
        self.assertEqual([c["index"] for c in items if c["delta"].get("role") == "assistant"], [2, 0])
