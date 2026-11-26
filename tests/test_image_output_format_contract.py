"""Translated image requests must not silently drop an explicit JSON contract."""
from unittest.mock import patch

from routes import chat_api, claude_api
from test_schema_contract_fidelity import MESSAGES, MODEL as TEXT, SchemaRouteHarness


IMAGE = "gemini-3.1-flash-image-2k"
FORMAT_CASES = (("openai", "json_object"), ("openai", "json_schema"), ("claude", "json_schema"))


class ImageOutputFormatRoutes(SchemaRouteHarness):
    async def send(self, protocol, model, fmt, stream=False):
        self.sent.clear()
        body = {"model": model, "messages": MESSAGES, "stream": stream}
        schema = {"type": "object", "properties": {"color": {"type": "string"}}}
        if protocol == "claude":
            path = "/v1/messages"
            body["max_tokens"] = 64
            if fmt:
                body["output_config"] = {"format": {"type": fmt, "schema": schema}}
        else:
            path = "/v1/chat/completions"
            if fmt:
                body["response_format"] = {"type": fmt}
                if fmt == "json_schema":
                    body["response_format"]["json_schema"] = {"name": "palette", "schema": schema}
        return await self.client.post(path, json=body)

    async def test_image_json_contract_is_http_400_in_all_delivery_modes(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol, fmt in FORMAT_CASES:
                for stream, fake in ((False, False), (True, False), (True, True)):
                    self.settings["fake_streaming"] = fake
                    with self.subTest(channel=channel, protocol=protocol, fmt=fmt, stream=stream, fake=fake):
                        result = await self.send(protocol, IMAGE, fmt, stream)
                        self.assertEqual(result.status_code, 400, result.text[:240])
                        self.assertTrue(result.headers["content-type"].startswith("application/json"))
                        payload = result.json()
                        if protocol == "claude":
                            self.assertEqual(payload["type"], "error")
                        self.assertEqual(payload["error"]["type"], "invalid_request_error")
                        self.assertIn("output_config.format" if protocol == "claude" else "response_format",
                                      payload["error"]["message"])
                        self.assertEqual(self.sent, [])

    async def test_mixed_or_resumable_cannot_override_the_format_error(self):
        self.channel = "mixed"
        self.settings.update(cookie_structured_output=False, resumable_enabled=True, resumable_image_streams=True)
        self.client.headers.update({"X-Resumable": "true", "X-Resume-ID": "nonexistent-fixture"})
        with patch.object(chat_api.credential_manager, "get_mixed_record") as select, \
                patch.object(chat_api, "_resumable_stream") as sse, patch.object(chat_api, "_resumable_json") as js, \
                patch.object(claude_api, "call_upstream") as dispatch:
            for protocol, fmt in FORMAT_CASES:
                for stream in (False, True):
                    result = await self.send(protocol, "gemini-3-pro-image", fmt, stream)
                    self.assertEqual(result.status_code, 400)
            for step in (select, sse, js, dispatch):
                step.assert_not_called()

    async def test_plain_images_and_text_json_still_reach_the_wire(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for model, cases in ((IMAGE, (("openai", None), ("openai", "text"), ("claude", None))),
                                 (TEXT, FORMAT_CASES)):
                for protocol, fmt in cases:
                    with self.subTest(channel=channel, model=model, protocol=protocol, fmt=fmt):
                        self.reply = {"text": '{"color":"blue"}' if model == TEXT else "ok"}
                        reply = await self.send(protocol, model, fmt)
                        self.assertEqual(reply.status_code, 200, reply.text[:240])
                        self.assertEqual(len(self.sent), 1)
                        gen = self.sent[0].get("variables", self.sent[0])["generationConfig"]
                        if model == IMAGE:
                            self.assertIn("IMAGE", gen["responseModalities"])
                            self.assertNotIn("responseMimeType", gen)
                        else:
                            self.assertEqual(gen["responseMimeType"], "application/json")
                            self.assertEqual("responseSchema" in gen, fmt == "json_schema")
