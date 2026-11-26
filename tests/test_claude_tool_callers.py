"""Do not turn a restricted or deferred Anthropic tool into an immediately callable one."""
from unittest.mock import AsyncMock, patch

from routes import claude_api
from test_parallel_tool_controls import MODES, _request
from test_schema_contract_fidelity import SchemaRouteHarness


class ClaudeToolCallerContracts(SchemaRouteHarness):
    async def test_reject_unrepresentable_semantics_before_generation_and_count(self):
        options = (("allowed_callers", ["code_execution_20250825"]), ("allowed_callers", []),
                   ("allowed_callers", ["direct", "code_execution_20250825"]), ("allowed_callers", "direct"),
                   ("defer_loading", True), ("defer_loading", 0))
        with patch.object(claude_api, "_express_exact_input_tokens", new_callable=AsyncMock) as exact:
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                for key, value in options:
                    for stream, fake in MODES:
                        with self.subTest(channel=channel, key=key, value=value, stream=stream, fake=fake):
                            self.settings["fake_streaming"] = fake
                            path, body = _request("claude", "auto", stream=stream)
                            body["tools"][0][key] = value
                            for endpoint in ([path] if stream else [path, path + "/count_tokens"]):
                                reply = await self.client.post(endpoint, json=body)
                                self.assertEqual(reply.status_code, 400, reply.text[:256])
                                self.assertIn(key, reply.json()["error"]["message"])
            exact.assert_not_awaited()
        self.assertEqual(self.sent, [])

    async def test_immediate_direct_and_default_tools_still_reach_the_same_wire(self):
        self.reply = {"functionCall": {"id": "local", "name": "weather", "args": {"city": "Tokyo"}}}
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            expected = None
            for options in ({}, {"allowed_callers": ["direct"], "defer_loading": False},
                            {"allowed_callers": None, "defer_loading": None}):
                with self.subTest(channel=channel, options=options):
                    path, body = _request("claude", "named")
                    body["tools"][0].update(options)
                    self.sent.clear()
                    reply = await self.client.post(path, json=body)
                    self.assertEqual(reply.status_code, 200, reply.text[:256])
                    self.assertEqual(len(self.sent), 1)
                    wire = self.sent[0].get("variables", self.sent[0])["tools"]
                    if expected is None:
                        expected = wire
                    self.assertEqual(wire, expected)
                    self.assertEqual(wire[0]["functionDeclarations"][0]["name"], "weather")
