"""Operator text must not add a synthetic model turn to a client's tool exchange."""
from test_claude_tool_result_envelope import claude_result_body
from test_parallel_tool_controls import MODES
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness


class ToolHistoryServicePrefill(SchemaRouteHarness):
    async def test_history_only_skips_service_prefix_but_keeps_system_instruction(self):
        self.settings.update(inject_prefill="service-prefix:", prefill_mode="smart",
                             inject_system_instruction="separate-system")
        self.reply = {"text": "done"}
        messages = [*MESSAGES, {"role": "assistant", "tool_calls": [
            {"id": "r", "type": "function", "function": {"name": "weather", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "r", "content": "result"}]
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for path, body in (("/v1/chat/completions", {"model": MODEL, "messages": messages}),
                               ("/v1/messages", claude_result_body("result", False))):
                for stream, fake in MODES:
                    with self.subTest(channel=channel, path=path, stream=stream, fake=fake):
                        self.settings["fake_streaming"] = fake
                        self.sent.clear()
                        response = await self.client.post(path, json={**body, "stream": stream})
                        self.assertEqual(response.status_code, 200, response.text[:256])
                        self.assertEqual(len(self.sent), 1)
                        wire = self.sent[0].get("variables", self.sent[0])
                        self.assertIn("functionResponse", wire["contents"][-1]["parts"][-1])
                        self.assertNotIn("service-prefix:", str(wire["contents"]))
                        self.assertIn("separate-system", str(wire["systemInstruction"]))

    async def test_ordinary_prefix_and_authored_prefill_are_preserved(self):
        self.settings.update(inject_prefill="service-prefix:", prefill_mode="smart")
        self.reply = {"text": "done"}
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for extras in ([], [{"role": "assistant", "content": "my-prefix:"}]):
                with self.subTest(channel=channel, authored=bool(extras)):
                    self.sent.clear()
                    reply = await self.client.post("/v1/chat/completions", json={
                        "model": MODEL, "messages": [*MESSAGES, *extras]})
                    self.assertEqual(reply.status_code, 200, reply.text[:256])
                    wire = self.sent[0].get("variables", self.sent[0])
                    contents = str(wire["contents"])
                    self.assertIn("my-prefix:" if extras else "service-prefix:", contents)
                    if extras:
                        self.assertNotIn("service-prefix:", contents)
