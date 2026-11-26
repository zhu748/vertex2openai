"""An operator prefix must not turn valid JSON into non-JSON on either bridge."""
import json

from test_schema_contract_fidelity import MODEL, SchemaRouteHarness


SCHEMA = {"type": "object", "properties": {"price": {"type": "integer"}}, "required": ["price"]}
PREFIX = "Answer:"
HISTORY = [{"role": "user", "content": "First question."},
           {"role": "assistant", "content": "Earlier answer."},
           {"role": "user", "content": "What is the price?"}]


def _visible_text(response, protocol, stream):
    if not stream:
        if protocol == "openai":
            return response.json()["choices"][0]["message"]["content"]
        return "".join(b["text"] for b in response.json()["content"] if b["type"] == "text")
    frames = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: {")]
    if protocol == "openai":
        return "".join(c["delta"].get("content") or "" for f in frames for c in f.get("choices", []))
    return "".join(f["delta"]["text"] for f in frames if f.get("delta", {}).get("type") == "text_delta")


class StructuredPrefillRoutes(SchemaRouteHarness):
    async def send(self, protocol, fmt=None, *, stream=False, messages=HISTORY):
        self.sent.clear()
        body = {"model": MODEL, "messages": messages, "stream": stream}
        if protocol == "openai":
            path = "/v1/chat/completions"
            if fmt:
                body["response_format"] = {"type": fmt}
                if fmt == "json_schema":
                    body["response_format"]["json_schema"] = {"name": "price", "schema": SCHEMA}
        else:
            path = "/v1/messages"
            body["max_tokens"] = 48
            if fmt:
                body["output_config"] = {"format": {"type": "json_schema", "schema": SCHEMA}}
        reply = await self.client.post(path, json=body)
        self.assertEqual(reply.status_code, 200, reply.text[:256])
        self.assertEqual(len(self.sent), 1)
        wire = self.sent[0].get("variables", self.sent[0])
        return _visible_text(reply, protocol, stream), wire

    async def test_operator_prefix_does_not_change_structured_prompt_or_response(self):
        self.reply = {"text": '{"price":7}'}
        self.settings.update(inject_prefill=PREFIX, inject_system_instruction="Use the listed prices.",
                             prefill_mode="smart")
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol, fmt in (("openai", "json_object"), ("openai", "json_schema"), ("claude", "json_schema")):
                for stream, fake in ((False, False), (True, False), (True, True)):
                    with self.subTest(channel=channel, protocol=protocol, fmt=fmt, stream=stream, fake=fake):
                        self.settings["fake_streaming"] = fake
                        text, wire = await self.send(protocol, fmt, stream=stream)
                        self.assertEqual(text, self.reply["text"])
                        self.assertEqual(json.loads(text), {"price": 7})
                        self.assertNotIn(PREFIX, json.dumps(wire["contents"]))
                        self.assertIn("Use the listed prices.", json.dumps(wire["systemInstruction"]))
                        self.assertIn({"role": "model", "parts": [{"text": "Earlier answer."}]}, wire["contents"])

    async def test_plain_text_still_uses_prefix_and_client_continuation_is_preserved(self):
        self.settings.update(inject_prefill=PREFIX, prefill_mode="smart")
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                with self.subTest(channel=channel, protocol=protocol):
                    self.reply = {"text": " seven"}
                    text, _ = await self.send(protocol)
                    self.assertEqual(text, PREFIX + self.reply["text"])
                    # This model accepts a trailing model turn. The client, not
                    # the operator, supplied the start of the JSON document.
                    self.reply = {"text": '7}'}
                    continuation = {"role": "assistant", "content": '{"price":'}
                    text, wire = await self.send(protocol, "json_schema", messages=[*HISTORY, continuation])
                    self.assertEqual(text, '{"price":7}')
                    self.assertEqual(wire["contents"][-1], {"role": "model", "parts": [{"text": '{"price":'}]})
