"""Complete batches with colliding real IDs cannot be replayed with distinct results."""
import json
from unittest.mock import patch

from google.genai import types

from message_processing import convert_to_openai_format
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from thought_signatures import signature_store


def output_call(ident=None, city="Ottawa"):
    call = {"name": "weather", "args": {"city": city}}
    if ident is not None:
        call["id"] = ident
    return {"functionCall": call}


class CompleteToolIdRoutes(SchemaRouteHarness):
    async def test_duplicate_real_ids_never_form_an_executable_json_or_fake_stream_batch(self):
        self.reply = [output_call("same"), output_call("same", "Tokyo")]
        with patch.object(signature_store, "register_call_signature", wraps=signature_store.register_call_signature) as register:
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                for protocol in ("openai", "claude"):
                    for stream in (True, False):
                        with self.subTest(channel=channel, protocol=protocol, fake_stream=stream):
                            self.settings["fake_streaming"] = stream
                            self.sent.clear()
                            path, body = _request(protocol, "auto", stream=stream)
                            response = await self.client.post(path, json=body)
                            self.assertEqual(len(self.sent), 1)
                            if stream:
                                self.assertEqual(response.status_code, 200, response.text[:256])
                                events = [json.loads(line[6:]) for line in response.text.splitlines()
                                          if line.startswith("data: {")]
                                errors = [e for e in events if e.get("error")]
                                self.assertEqual(len(errors), 1, events)
                                self.assertNotIn('"tool_use"', response.text)
                                self.assertFalse(any(c.get("delta", {}).get("tool_calls")
                                                     for e in events for c in e.get("choices", [])))
                                self.assertFalse(any(c.get("finish_reason") == "tool_calls"
                                                     for e in events for c in e.get("choices", [])))
                                self.assertNotIn("event: message_stop", response.text)
                                message = errors[0]["error"]["message"]
                            else:
                                self.assertEqual(response.status_code, 502, response.text[:256])
                                message = response.json()["error"]["message"]
                            self.assertIn("重复", message)
            register.assert_not_called()

    async def test_missing_and_distinct_ids_remain_correlatable(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for ids in ((None, None), ("A", "B")):
                with self.subTest(channel=channel, ids=ids):
                    self.reply = [output_call(ids[0]), output_call(ids[1], "Tokyo")]
                    path, body = _request("openai", "auto")
                    reply = await self.client.post(path, json=body)
                    self.assertEqual(reply.status_code, 200, reply.text[:256])
                    emitted = [c["id"] for c in reply.json()["choices"][0]["message"]["tool_calls"]]
                    self.assertEqual(len(set(emitted)), 2)
                    self.assertTrue(all(emitted))
                    if ids[0]:
                        self.assertEqual(emitted, list(ids))

    async def test_native_cookie_output_is_not_rewritten(self):
        self.channel = "web_proxy"
        self.reply = [output_call("same"), output_call("same", "Tokyo")]
        reply = await self.client.post(f"/v1beta/models/{MODEL}:generateContent", json={
            "contents": [{"role": "user", "parts": [{"text": "local fixture"}]}]})
        self.assertEqual(reply.status_code, 200, reply.text)
        ids = [p["functionCall"]["id"] for p in reply.json()["candidates"][0]["content"]["parts"]]
        self.assertEqual(ids, ["same", "same"])

    def test_sdk_candidates_do_not_share_ids_and_nonexecutable_finishes_keep_their_reason(self):
        allowed = types.Part.model_validate(output_call("same"))
        other = types.Part.model_validate(output_call("same", "Tokyo"))
        def candidate(parts, finish, index):
            return types.Candidate(index=index, content=types.Content(role="model", parts=parts), finish_reason=finish)
        for candidates, reasons in (([candidate([allowed], "STOP", 0), candidate([other], "STOP", 1)],
                                     ["tool_calls", "tool_calls"]),
                                    ([candidate([allowed, other], "MAX_TOKENS", 0)], ["length"]),
                                    ([candidate([allowed, other], "SAFETY", 0)], ["content_filter"])):
            result = convert_to_openai_format(types.GenerateContentResponse(candidates=candidates), MODEL)
            self.assertEqual([c["finish_reason"] for c in result["choices"]], reasons)
