"""Exercise actual client routes; only the upstream HTTP transport is simulated."""
import base64
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
from unittest import mock

import httpx

from main import app
from runtime_state import app_state
from test_cookie_tool_stream import RECORD, wire
from test_req_round15_contracts import Round15Harness, chunks
from upstreams import cookie_delivery, cookie_native


def blob(data):
    return {"mimeType": "image/png", "data": base64.b64encode(data).decode()}


THOUGHT_IMAGE = {"inlineData": blob(b"private-pixels"), "thought": True, "thoughtSignature": "dDE="}
THOUGHT_FILE = {"fileData": {"mimeType": "image/png", "fileUri": "https://fixture.test/private.png"},
                "thought": True, "thoughtSignature": "dDI="}
VISIBLE = {"inlineData": blob(b"final-pixels"), "thoughtSignature": "djE="}
PARTS = [{"text": "before"}, THOUGHT_IMAGE, THOUGHT_FILE, {"text": "hidden text", "thought": True},
         {"text": "after"}, VISIBLE]


class CookieThoughtMediaTests(Round15Harness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.raw = wire(*PARTS, finish="STOP")
        upstream = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=self.raw)))
        self.addAsyncCleanup(upstream.aclose)

        @asynccontextmanager
        async def opened(*args, **kwargs):
            yield upstream

        for patch in (mock.patch.object(cookie_delivery, "open_upstream_client", opened),
                      mock.patch.object(cookie_native, "open_upstream_client", opened),
                      mock.patch.object(cookie_delivery, "build_headers", return_value={"X-Fixture": "1"}),
                      mock.patch.object(cookie_native, "build_headers", return_value={"X-Fixture": "1"}),
                      mock.patch.object(app_state, "get_channel_mode", return_value="web_proxy"),
                      mock.patch.object(app_state, "get_cookie_accounts", return_value=[dict(RECORD, enabled=True)]),
                      mock.patch.object(cookie_native.credential_manager, "record_cookie_success"),
                      mock.patch.dict("os.environ", {"IMAGE_OUTPUT_MODE": "url"})):
            patch.start()
            self.addCleanup(patch.stop)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                        base_url="http://fixture.test",
                                        headers={"Authorization": "Bearer test-key", "x-goog-api-key": "test-key"})
        self.addAsyncCleanup(self.client.aclose)

    async def test_chat_media_visibility_and_opaque_signature_slots(self):
        for mode in ("json", "sse", "buffered", "image_sse"):
            for hidden in (True, False):
                with self.subTest(mode=mode, hidden=hidden), tempfile.TemporaryDirectory() as tmp, \
                        mock.patch("image_delivery.store_directory", return_value=Path(tmp)):
                    self.settings.update(fake_streaming=mode == "buffered",
                                         native_thinking_mode="off" if hidden else "request")
                    model = "gemini-3.1-flash-image" if mode == "image_sse" else "gemini-3.5-flash-lite"
                    response = await self.client.post("/v1/chat/completions", json={
                        "model": model, "stream": mode != "json", "messages": [{"role": "user", "content": "draw"}]})
                    self.assertEqual(response.status_code, 200, response.text)
                    if mode == "json":
                        fields = [response.json()["choices"][0]["message"]]
                    else:
                        self.assertTrue(response.text.rstrip().endswith("data: [DONE]"), response.text)
                        self.assertFalse(any("error" in f for f in chunks(response.text)), response.text)
                        fields = [c["delta"] for frame in chunks(response.text) for c in frame["choices"]]
                    content = "".join(f.get("content") or "" for f in fields)
                    reasoning = "".join(f.get("reasoning_content") or "" for f in fields)
                    files = {p.read_bytes(): p.name for p in Path(tmp).iterdir()}
                    self.assertIn("beforeafter", content)
                    self.assertIn(files[b"final-pixels"], content)
                    self.assertNotIn("private.png", content)
                    if hidden:
                        self.assertEqual(set(files), {b"final-pixels"})
                        self.assertEqual(reasoning, "")
                        for private in ("private.png", THOUGHT_IMAGE["inlineData"]["data"], "hidden text"):
                            self.assertNotIn(private, response.text)
                    else:
                        self.assertIn(files[b"private-pixels"], reasoning)
                        self.assertNotIn(files[b"private-pixels"], content)
                        self.assertIn("private.png", reasoning)
                    metadata = next(f["extra_content"]["google"]["ordinary_parts"] for f in fields
                                    if "ordinary_parts" in f.get("extra_content", {}).get("google", {}))
                    signed = [p for p in metadata if p.get("thought_signature")]
                    self.assertEqual([p["thought_signature"] for p in signed], ["dDE=", "dDI=", "djE="])
                    if hidden:
                        self.assertEqual([p["kind"] for p in signed], ["signature_only", "signature_only", "image"])
                    else:
                        self.assertEqual([(p["kind"], p.get("thought")) for p in signed],
                                         [("image", True), ("image", True), ("image", None)])

    async def test_native_preserves_media_parts_in_place_without_registering_downloads(self):
        for route in ("generateContent", "streamGenerateContent?alt=sse"):
            with self.subTest(route=route), mock.patch("image_delivery.image_markdown") as renderer:
                out = await self.client.post(f"/v1beta/models/gemini-3.5-flash-lite:{route}",
                    json={"contents": [{"role": "user", "parts": [{"text": "draw"}]}]})
                self.assertEqual(out.status_code, 200)
                responses = chunks(out.text) if "sse" in route else [out.json()]
                parts = [part for frame in responses for cand in frame["candidates"]
                         for part in cand.get("content", {}).get("parts", [])]
                self.assertEqual(parts, PARTS)
                self.assertEqual(responses[-1]["candidates"][0]["finishReason"], "STOP")
                renderer.assert_not_called()
