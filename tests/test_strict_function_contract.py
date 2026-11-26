"""Strict is a contract, not a hint. No upstream or partial tool batch on refusal."""

import json
from unittest import mock

from routes import chat_api
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness


WEATHER = {"type": "object", "properties": {"city": {"type": "string"}},
           "required": ["city"], "additionalProperties": False}
MODES = [(False, False), (True, False), (True, True)]  # JSON, real SSE, synthetic SSE


class StrictToolRoutes(SchemaRouteHarness):
    async def test_strict_tools_never_reach_generation_or_emit_any_call(self):
        # Even this fully representable declaration cannot guarantee that every
        # returned argument matches. An ordinary model call is not a validator.
        self.reply = {"functionCall": {"name": "quote", "args": {"city": 3}}}
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                for stream, fake in MODES:
                    with self.subTest(channel=channel, protocol=protocol, stream=stream, fake=fake):
                        self.settings["fake_streaming"] = fake
                        reply = await self.send_schema(protocol, "tool", WEATHER, stream=stream,
                                                       tool_fields={"strict": True})
                        self.assertEqual(reply.status_code, 400, reply.text[:256])
                        self.assertTrue(reply.headers["content-type"].startswith("application/json"))
                        error = reply.json()["error"]
                        self.assertEqual(error["type"], "invalid_request_error")
                        self.assertIn("strict", error["message"])
                        self.assertIn("false", error["message"])
                        self.assertEqual(self.sent, [])

    async def test_strict_sibling_fails_before_mixed_selection_and_resume_admission(self):
        self.channel = "mixed"
        self.settings["resumable_enabled"] = True
        tools = [{"type": "function", "function": {"name": "first", "parameters": WEATHER}},
                 {"type": "function", "function": {"name": "quote", "parameters": WEATHER, "strict": True}}]
        body = {"model": MODEL, "messages": MESSAGES, "stream": True, "tools": tools}
        with mock.patch.object(chat_api.credential_manager, "get_mixed_record", return_value=None) as select, \
                mock.patch.object(chat_api, "_resumable_stream", wraps=chat_api._resumable_stream) as admit:
            reply = await self.client.post("/v1/chat/completions", json=body, headers={"X-Resumable": "true"})
        self.assertEqual(reply.status_code, 400, reply.text[:256])
        self.assertIn("tools[1].function.strict", reply.json()["error"]["message"])
        select.assert_not_called()
        admit.assert_not_called()
        self.assertEqual(self.sent, [])

    async def test_unset_and_false_still_deliver_valid_custom_calls(self):
        self.reply = {"functionCall": {"name": "quote", "args": {"city": "Lima"}}}
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                for fields in ({}, {"strict": False}):
                    for stream, fake in MODES:
                        with self.subTest(channel=channel, protocol=protocol, fields=fields, stream=stream, fake=fake):
                            self.settings["fake_streaming"] = fake
                            result = await self.send_schema(protocol, "tool", WEATHER, stream=stream, tool_fields=fields)
                            self.assertEqual(result.status_code, 200, result.text[:256])
                            self.assertEqual(len(self.sent), 1)
                            if stream:
                                frames = [json.loads(s[6:]) for s in result.text.splitlines() if s.startswith("data: {")]
                                if protocol == "openai":
                                    functions = [c["function"] for f in frames for choice in f.get("choices", [])
                                                 for c in choice["delta"].get("tool_calls", []) if "function" in c]
                                    args = json.loads("".join(x.get("arguments", "") for x in functions))
                                else:
                                    args = json.loads("".join(f["delta"]["partial_json"] for f in frames
                                        if f.get("delta", {}).get("type") == "input_json_delta"))
                            elif protocol == "openai":
                                call = result.json()["choices"][0]["message"]["tool_calls"][0]
                                args = json.loads(call["function"]["arguments"])
                            else:
                                args = next(b["input"] for b in result.json()["content"] if b["type"] == "tool_use")
                            self.assertEqual(args, {"city": "Lima"})

    async def test_strict_flag_is_typed_in_each_protocol(self):
        for protocol in ("openai", "claude"):
            with self.subTest(protocol=protocol):
                reply = await self.send_schema(protocol, "tool", WEATHER, tool_fields={"strict": "false"})
                self.assertEqual(reply.status_code, 400)
                self.assertIn("boolean", reply.json()["error"]["message"])
                self.assertEqual(self.sent, [])
