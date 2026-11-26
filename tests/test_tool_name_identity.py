"""The gateway must not normalize a caller-specified tool into another declaration."""
from unittest.mock import patch

from routes import chat_api, claude_api
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness


class ToolNameIdentity(SchemaRouteHarness):
    async def test_reject_ambiguous_declarations_and_choice_before_active_replay_or_count(self):
        with (patch.object(chat_api, "resumable_enabled", return_value=True),
              patch.object(chat_api, "_resumable_json") as buffered,
              patch.object(chat_api, "_resumable_stream") as buffered_stream,
              patch.object(claude_api, "_express_exact_input_tokens") as count):
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                for protocol in ("openai", "claude"):
                    for in_decl, stream in ((True, False), (True, True), (False, False), (False, True)):
                        with self.subTest(channel=channel, protocol=protocol, in_decl=in_decl, stream=stream):
                            path, body = _request(protocol, "named", stream=stream)
                            decl = body["tools"][0]
                            if in_decl:
                                if protocol == "openai":
                                    decl["function"]["name"] = " weather "
                                else:
                                    decl["name"] = " weather "
                            elif protocol == "openai":
                                body["tool_choice"]["function"]["name"] = " weather "
                            else:
                                body["tool_choice"]["name"] = " weather "
                            result = await self.client.post(path, json=body, headers={"X-Resumable": "true"})
                            self.assertEqual(result.status_code, 400, result.text[:200])
                            self.assertNotIn("X-Resume-Id", result.headers)
                            if protocol == "claude":
                                counted = await self.client.post(path + "/count_tokens", json=body)
                                self.assertEqual(counted.status_code, 400)
            buffered.assert_not_called()
            buffered_stream.assert_not_called()
            count.assert_not_called()
        self.assertEqual(self.sent, [])

    async def test_unambiguous_case_and_punctuation_remain_literal(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                with self.subTest(channel=channel, protocol=protocol):
                    path, body = _request(protocol, "named")
                    decl = body["tools"][0]
                    name = "Weather-2_city"
                    if protocol == "openai":
                        decl["function"]["name"] = body["tool_choice"]["function"]["name"] = name
                    else:
                        decl["name"] = body["tool_choice"]["name"] = name
                    self.sent.clear()
                    result = await self.client.post(path, json=body)
                    self.assertEqual(result.status_code, 200, result.text[:200])
                    wire = self.sent[0].get("variables", self.sent[0])
                    self.assertEqual(wire["tools"][0]["functionDeclarations"][0]["name"], name)
                    config = wire["toolConfig"]["functionCallingConfig"]
                    self.assertEqual(config.get("allowedFunctionNames", config.get("allowed_function_names")), [name])
