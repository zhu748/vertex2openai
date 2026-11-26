"""Cookie's optional structured-output gate must not block Gemini's default text MIME."""
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness


class NativePlainRoutes(SchemaRouteHarness):
    async def send(self, action, gen, *, snake=False):
        self.sent.clear()
        body = {"contents": [{"role": "user", "parts": [{"text": "hello"}]}],
                "generation_config" if snake else "generationConfig": gen}
        return await self.client.post(f"/v1beta/models/{MODEL}:{action}", json=body)

    async def test_explicit_plain_and_omitted_have_identical_private_generation_config(self):
        self.channel = "web_proxy"
        for enabled in (False, True):
            self.settings["cookie_structured_output"] = enabled
            for action in ("generateContent", "streamGenerateContent", "streamGenerateContent?alt=sse"):
                control = await self.send(action, {"maxOutputTokens": 27})
                self.assertEqual(control.status_code, 200, control.text[:240])
                expected = self.sent[0]["variables"]["generationConfig"]
                for snake in (False, True):
                    with self.subTest(enabled=enabled, action=action, snake=snake):
                        gen = ({"max_output_tokens": 27, "response_mime_type": "text/plain"} if snake
                               else {"maxOutputTokens": 27, "responseMimeType": "text/plain"})
                        result = await self.send(action, gen, snake=snake)
                        self.assertEqual(result.status_code, 200, result.text[:240])
                        self.assertEqual(self.sent[0]["variables"]["generationConfig"], expected)
                        self.assertIn("ok", result.text)

    async def test_nondefault_mime_or_explicit_schema_still_requires_the_feature(self):
        self.channel = "web_proxy"
        self.settings["cookie_structured_output"] = False
        for gen in ({"responseMimeType": "application/json"}, {"response_mime_type": "text/x.enum"},
                    {"responseMimeType": "text/plain", "responseSchema": {"type": "string"}},
                    {"response_mime_type": "text/plain", "response_json_schema": {"type": "string"}},
                    {"responseSchema": {}}):
            with self.subTest(gen=gen):
                result = await self.send("generateContent", gen)
                self.assertEqual(result.status_code, 400)
                error = result.json()["error"]
                self.assertEqual(error["status"], "INVALID_ARGUMENT")
                self.assertIn("cookie_structured_output", error["message"])
                self.assertEqual(self.sent, [])
