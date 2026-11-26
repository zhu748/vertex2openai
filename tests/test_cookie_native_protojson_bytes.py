"""The pinned Gemini SDK echoes URL-safe Base64; Studio's Bytes is standard Base64."""

import base64
import json
from unittest.mock import patch

from google import genai
from google.genai import types
import httpx

import main
from signature_store import SKIP_VALIDATOR_SENTINELS
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import native_passthrough

MODEL = "gemini-3.6-flash"
ROUTE = f"/v1beta/models/{MODEL}:generateContent"
SIGNATURE = b"\xfb\xff"
STANDARD = "+/8="
SDK_SPELLING = "-_8="
PROMPT = types.Content(role="user", parts=[types.Part(text="What is the local color?")])
PROGRAM = {"functionCall": {"name": "color", "id": "color-one", "args": {
    "thought_signature": SDK_SPELLING, "inline_data": {"data": SDK_SPELLING}}}, "thoughtSignature": STANDARD}


class NativeProtoJsonBytes(SchemaRouteHarness):
    async def test_actual_sdk_echo_preserves_part_order_and_bytes_at_final_studio_send(self):
        self.channel = "web_proxy"
        sdk = genai.Client(api_key="test-key", http_options=types.HttpOptions(
            base_url="http://local", api_version="v1beta",
            async_client_args={"transport": httpx.ASGITransport(app=main.app)},
        ))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        statement = {"text": "Checking.", "thoughtSignature": STANDARD}
        media = {"inlineData": {"data": STANDARD, "mimeType": "image/png"}, "thoughtSignature": STANDARD}
        self.reply = [statement, media, PROGRAM]
        first = await sdk.aio.models.generate_content(model=MODEL, contents=[PROMPT])
        history = first.candidates[0].content
        self.assertEqual([p.thought_signature for p in history.parts], [SIGNATURE] * 3)
        self.assertEqual(history.parts[1].inline_data.data, SIGNATURE)

        result = types.Part(function_response=types.FunctionResponse(
            name="color", id="color-one", response={"data": SDK_SPELLING},
            parts=[types.FunctionResponsePart(inline_data=types.FunctionResponseBlob(
                mime_type="image/png", data=SIGNATURE))],
        ))
        self.reply = {"text": "The file is blue."}
        next_reply = await sdk.aio.models.generate_content(
            model=MODEL, contents=[PROMPT, history, types.Content(role="user", parts=[result])])
        self.assertEqual(next_reply.text, "The file is blue.")
        self.assertEqual(len(self.sent), 2)
        sent = self.sent[-1]["variables"]["contents"]
        self.assertEqual(sent[1]["parts"], [statement, media, PROGRAM])
        self.assertEqual(sent[2]["parts"], [{"functionResponse": {
            "id": "color-one", "name": "color", "response": {"data": SDK_SPELLING},
            "parts": [{"inlineData": {"data": STANDARD, "mimeType": "image/png"}}],
        }}])

    async def test_typed_bytes_only_and_invalid_spelling_left_for_provider(self):
        self.channel = "web_proxy"
        cases = (
            ("-_8", STANDARD), (SDK_SPELLING, STANDARD), ("+/8", STANDARD), (STANDARD, STANDARD),
            ("Zg", "Zg=="), ("", ""), (None, None),    # two-, three- and zero-tail lengths
            ("+_8=", "+_8="), ("Zh==", "Zh=="), ("Zg=", "Zg="), ("-_8=\n", "-_8=\n"),
        )
        for raw, normalized in cases:
            with self.subTest(raw=raw):
                literal = {"thought_signature": raw, "inline_data": {"data": raw}}
                payload = {"contents": [{"role": "model", "parts": [
                    {"inline_data": {"mime_type": "image/png", "data": raw}, "thought_signature": raw,
                     "part_metadata": literal},
                    {"function_call": {"name": "color", "args": literal}},
                    {"function_response": {"name": "color", "response": literal, "parts": [
                        {"inline_data": {"mime_type": "image/png", "data": raw}}]}},
                    {"text": "unsigned"},
                ]}]}
                response = await self.client.post(ROUTE, json=payload)
                self.assertEqual(response.status_code, 200, response.text)
                media, call, result, unsigned = self.sent[-1]["variables"]["contents"][0]["parts"]
                self.assertEqual(media["thoughtSignature"], normalized)
                self.assertEqual(media["inlineData"]["data"], normalized)
                self.assertEqual(result["functionResponse"]["parts"][0]["inlineData"]["data"], normalized)
                self.assertEqual(media["partMetadata"], literal)
                self.assertEqual(call["functionCall"]["args"], literal)
                self.assertEqual(result["functionResponse"]["response"], literal)
                self.assertEqual(unsigned, {"text": "unsigned"})

    async def test_existing_explicit_sentinels_only_change_to_graphql_bytes(self):
        self.channel = "web_proxy"
        for sentinel in SKIP_VALIDATOR_SENTINELS:
            literal, encoded = sentinel.decode("ascii"), base64.b64encode(sentinel).decode("ascii")
            for value in (literal, encoded):
                with self.subTest(value=value):
                    response = await self.client.post(ROUTE, json={"contents": [{"role": "model", "parts": [
                        {"text": literal, "thought_signature": value},
                        {"text": "no signature"},
                    ]}]})
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(self.sent[-1]["variables"]["contents"][0]["parts"], [
                        {"text": literal, "thoughtSignature": encoded}, {"text": "no signature"}])

    async def test_express_native_body_stays_byte_exact(self):
        raw = b'{ "contents": [{"parts":[{"inlineData":{"data":"-_8"}, "thoughtSignature":"-_8="}]}] }'
        sent = []

        def respond(request):
            sent.append(request.content)
            return httpx.Response(200, json={"candidates": [{"finishReason": "STOP"}]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as upstream:
            with patch.object(native_passthrough.passthrough_client_pool, "get", return_value=upstream):
                response = await self.client.post(ROUTE, content=raw)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(sent, [raw])
        self.assertEqual(json.loads(raw)["contents"][0]["parts"][0]["thoughtSignature"], SDK_SPELLING)
