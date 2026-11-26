"""A successful download owns its file before headers, including range responses."""
import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
import image_delivery as store
from fastapi import FastAPI
from routes import generated_images
from routes.generated_images import router


class DownloadOwnershipTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(router)
        directory = self.enterContext(TemporaryDirectory())
        self.enterContext(patch.object(store, "store_directory", lambda: Path(directory)))
        self.enterContext(patch.object(store, "MAX_STORE_BYTES", 8))
        token = store.image_base_url.set("https://local.test")
        self.addCleanup(store.image_base_url.reset, token)

    def publish(self, data):
        return store.image_markdown(data, "image/png").rsplit("/", 1)[-1][:-1]

    async def request(self, name, send, *, method="GET", headers=(), pathsend=False):
        async def receive():
            await asyncio.Future()

        path = "/v1/generated-images/" + name
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": method, "scheme": "http", "path": path,
                 "raw_path": path.encode(), "root_path": "", "query_string": b"", "headers": headers,
                 "extensions": {"http.response.pathsend": {}} if pathsend else {},
                 "server": ("test", 80), "client": ("fixture", 1234)}
        await self.app(scope, receive, send)

    async def test_eviction_at_headers_cannot_truncate_accepted_download(self):
        for method, requested, expected, content in (
                ("GET", (), 200, b"12345678"),
                ("GET", ((b"range", b"bytes=2-4"),), 206, b"345"),
                ("GET", ((b"range", b"bytes=0-1,6-7"),), 206, None),
                ("HEAD", ((b"range", b"bytes=0-1"),), 206, b"")):
            with self.subTest(method=method, headers=requested):
                first = self.publish(b"12345678")
                events = []

                async def send(event, events=events, first=first):
                    events.append(event)
                    if event["type"] == "http.response.start":
                        self.publish(b"abcdefgh")
                        self.assertFalse((store.store_directory() / first).exists())

                await self.request(first, send, method=method, headers=requested, pathsend=True)
                self.assertEqual(events[0]["status"], expected)
                self.assertTrue(all(event["type"] == "http.response.body" for event in events[1:]))
                body = b"".join(event.get("body", b"") for event in events[1:])
                length = int(dict(events[0]["headers"])[b"content-length"])
                if method != "HEAD":
                    self.assertEqual(length, len(body))
                if content is None:
                    self.assertIn(b"bytes 0-1/8\r\n\r\n12", body)
                    self.assertIn(b"bytes 6-7/8\r\n\r\n78", body)
                else:
                    self.assertEqual(body, content)

    async def test_conditional_head_invalid_ranges_and_cancel_release_file(self):
        name = self.publish(b"12345678")
        path = "/v1/generated-images/" + name
        opened = []
        original = generated_images.open_image_asset

        def record(name):
            asset = original(name)
            if asset is not None:
                opened.append(asset.file)
            return asset

        with patch.object(generated_images, "open_image_asset", record):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(self.app), base_url="http://local") as client:
                head = await client.head(path)
                self.assertEqual((head.content, head.headers["content-length"]), (b"", "8"))
                etag = head.headers["etag"]
                for r, ir, code, body in (("bytes=1-2", etag, 206, b"23"),
                                          ("bytes=1-2", '"stale"', 200, b"12345678"),
                                          ("bytes=12-20", etag, 416, None),
                                          ("wrong", etag, 400, None)):
                    resp = await client.get(path, headers={"range": r, "if-range": ir})
                    self.assertEqual(resp.status_code, code)
                    if body is not None:
                        self.assertEqual(resp.content, body)
                    self.assertTrue(all(f.closed for f in opened))
            started = asyncio.Event()

            async def hold_headers(event):
                if event["type"] == "http.response.start":
                    started.set()
                    await asyncio.Future()

            task = asyncio.create_task(self.request(name, hold_headers))
            try:
                await asyncio.wait_for(started.wait(), 3)
                self.assertTrue(opened and not opened[-1].closed)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertTrue(all(f.closed for f in opened))
