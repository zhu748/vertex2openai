"""Claude explicitly distinguishes failed tools from successful domain values."""
import json

from claude_protocol import anthropic_to_openai_request
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness
from upstreams.cookie_request import _convert_messages_to_contents


def claude_result_body(content, error_flag):
    block = {"type": "tool_result", "tool_use_id": "tool_1", "content": content}
    if error_flag is not None:
        block["is_error"] = error_flag
    return {"max_tokens": 64, "model": MODEL, "messages": [*MESSAGES, {"role": "assistant", "content": [{
        "type": "tool_use", "id": "tool_1", "name": "measurement", "input": {}}]},
        {"role": "user", "content": [block]}]}


class ClaudeToolResultEnvelopeRoutes(SchemaRouteHarness):
    async def send_result(self, channel, protocol, content, error_flag):
        self.channel = channel
        if protocol == "claude":
            path, body = "/v1/messages", claude_result_body(content, error_flag)
        else:
            messages = [*MESSAGES, {"role": "assistant", "tool_calls": [{"id": "tool_1", "type": "function",
                "function": {"name": "measurement", "arguments": "{}"}}]},
                {"role": "tool", "tool_call_id": "tool_1", "content": content}]
            path, body = "/v1/chat/completions", {"model": MODEL, "messages": messages}
        self.sent.clear()
        reply = await self.client.post(path, json=body)
        self.assertEqual(reply.status_code, 200, reply.text[:256])
        self.assertEqual(len(self.sent), 1)
        wire = self.sent[0].get("variables", self.sent[0])
        results = [p["functionResponse"] for c in wire["contents"] for p in c["parts"] if "functionResponse" in p]
        self.assertEqual(len(results), 1, wire)
        self.assertEqual((results[0]["name"], results[0]["id"]), ("measurement", "tool_1"))
        return results[0]["response"]

    async def test_success_collisions_wrap_the_entire_domain_object_once(self):
        cases = ({"error": "measurement uncertainty", "units": "cm"}, {"output": "document", "units": "cm"},
                 {"error": None, "output": {"label": "trial"}, "observations": [0, False]})
        for channel in ("web_proxy", "api_key"):
            for flag in (None, False):
                for domain in cases:
                    with self.subTest(channel=channel, flag=flag, domain=domain):
                        text = json.dumps(domain)
                        content = text if flag is False else [{"type": "text", "text": text}]
                        wire = await self.send_result(channel, "claude", content, flag)
                        self.assertEqual(wire, {"output": domain})

    async def test_true_failures_ordinary_structured_values_and_openai_do_not_change(self):
        data = {"output": "document", "units": "cm"}
        for channel in ("web_proxy", "api_key"):
            for protocol, content, flag, expected in (
                ("claude", json.dumps(data), True, {"error": json.dumps(data)}),
                ("claude", "normal text", False, {"result": "normal text"}),
                ("claude", json.dumps({"units": "cm", "value": 5}), False, {"units": "cm", "value": 5}),
                ("claude", json.dumps(["a", "b"]), False, {"result": ["a", "b"]}),
                ("openai", json.dumps(data), None, data),
            ):
                with self.subTest(channel=channel, protocol=protocol, content=content, flag=flag):
                    self.assertEqual(await self.send_result(channel, protocol, content, flag), expected)

    def test_text_compatibility_keeps_the_same_success_envelope(self):
        domain = {"error": "measurement uncertainty", "output": 4, "units": "cm"}
        request = anthropic_to_openai_request(claude_result_body(json.dumps(domain), False), MODEL)
        contents, _ = _convert_messages_to_contents(request.messages, MODEL, native_tools=False)
        observation = contents[-1]["parts"][0]["text"].split("\n", 1)[1]
        self.assertEqual(json.loads(observation), {"output": domain})
