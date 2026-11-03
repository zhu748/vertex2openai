"""Client-safe chat delivery: small URLs, exact original bytes, bounded decoding."""
import asyncio
import base64
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory()
os.environ.setdefault("STATE_DIR", _STATE.name)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import httpx
import image_delivery as delivery
from message_processing import _convert_image_to_markdown
from routes.generated_images import router
from upstreams.cookie_response import extract_events


class ImageDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        patch = mock.patch.dict(os.environ, {"STATE_DIR": self.directory.name})
        patch.start()
        self.addCleanup(patch.stop)
        token = delivery.image_base_url.set("https://service.onrender.com")
        self.addCleanup(delivery.image_base_url.reset, token)

    def test_sdk_avoids_base64_encoding_and_preserves_bytes(self):
        raw = os.urandom(3 * 1024 * 1024)
        with mock.patch.object(base64, "b64encode", side_effect=AssertionError("whole image encoded")):
            markdown = _convert_image_to_markdown(raw, "image/png")
        self.assertLess(len(markdown), 200)
        name = markdown.rsplit("/", 1)[1][:-1]
        self.assertEqual(delivery.resolve_image(name).read_bytes(), raw)

    def test_cookie_decodes_only_bounded_slices(self):
        raw = os.urandom(3 * 1024 * 1024 + 7)
        encoded = base64.b64encode(raw).decode()
        decode = base64.b64decode
        sizes = []

        def bounded(value, **kwargs):
            sizes.append(len(value))
            return decode(value, **kwargs)

        obj = {"candidates": [{"content": {"parts": [{"inlineData": {
            "mimeType": "image/png", "data": encoded}}]}}]}
        obj = {"results": [{"data": obj}]}
        with mock.patch.object(base64, "b64decode", side_effect=bounded):
            events = list(extract_events(obj))
        markdown = next(value for kind, value in events if kind == "image")
        self.assertNotIn("base64,", markdown)
        self.assertLessEqual(max(sizes), delivery.CHUNK)
        self.assertEqual(delivery.resolve_image(markdown.rsplit("/", 1)[1][:-1]).read_bytes(), raw)
        native = list(extract_events(obj, preserve_parts=True))
        part = next(value for kind, value in native if kind == "native_part")
        self.assertEqual(part["inlineData"]["data"], encoded)

    def test_invalid_base64_cleans_partial_file(self):
        with self.assertRaises(ValueError):
            delivery.image_markdown("!!!!", "image/png", encoded=True)
        self.assertEqual(list(delivery.store_directory().iterdir()), [])

    def test_expiry_and_path_validation(self):
        md = delivery.image_markdown(b"png", "image/png")
        name = md.rsplit("/", 1)[1][:-1]
        path = delivery.resolve_image(name)
        old = time.time() - delivery.TTL_SECONDS - 1
        os.utime(path, (old, old))
        self.assertIsNone(delivery.resolve_image(name))
        self.assertFalse(path.exists())
        self.assertIsNone(delivery.resolve_image("../web_state.json"))

    def test_disk_budget_evicts_oldest_and_file_limit_rejects(self):
        first = delivery.image_markdown(b"old", "image/png").rsplit("/", 1)[1][:-1]
        with mock.patch.object(delivery, "MAX_STORE_BYTES", 5):
            delivery.image_markdown(b"new", "image/png")
        self.assertIsNone(delivery.resolve_image(first))
        with mock.patch.object(delivery, "MAX_FILE_BYTES", 2):
            with self.assertRaises(ValueError):
                delivery.image_markdown(b"big", "image/png")

    def test_storage_failure_does_not_return_inline_blob(self):
        with mock.patch.object(delivery, "store_directory", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                _convert_image_to_markdown(b"image", "image/png")

    def test_stream_download_range_inline_switch_and_context_reset(self):
        token = delivery.image_base_url.set("")
        self.addCleanup(delivery.image_base_url.reset, token)
        asyncio.run(self._http_roundtrip())

    async def _http_roundtrip(self):
        app = FastAPI()
        app.add_middleware(delivery.ImageDeliveryMiddleware)
        app.include_router(router)
        raw = b"\x89PNG\r\n\x1a\n" + os.urandom(1024)

        @app.post("/v1/chat/completions")
        async def chat():
            async def frames():
                yield "data: " + _convert_image_to_markdown(raw, "image/png") + "\n\n"
            return StreamingResponse(frames(), media_type="text/event-stream")

        with mock.patch.dict(os.environ, {"RENDER_EXTERNAL_URL": "https://service.onrender.com",
                                          "IMAGE_OUTPUT_MODE": "url", "IMAGE_PUBLIC_BASE_URL": ""}):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://internal") as client:
                response = await client.post("/v1/chat/completions")
                self.assertNotIn("base64", response.text)
                url = response.text.split("](")[1].split(")")[0]
                self.assertTrue(url.startswith("https://service.onrender.com/"))
                image = await client.get(url)
                self.assertEqual(image.content, raw)
                self.assertEqual(image.headers["content-type"], "image/png")
                partial = await client.get(url, headers={"Range": "bytes=0-7"})
                self.assertEqual(partial.status_code, 206)
                self.assertEqual(partial.content, raw[:8])
                self.assertEqual((await client.head(url)).content, b"")
                self.assertEqual((await client.get("/v1/generated-images/missing.png")).status_code, 404)
                with mock.patch.dict(os.environ, {"IMAGE_OUTPUT_MODE": "inline"}):
                    inline = await client.post("/v1/chat/completions")
                    self.assertIn("data:image/png;base64,", inline.text)
        self.assertEqual(delivery.image_base_url.get(), "")


if __name__ == "__main__":
    unittest.main()
