"""Backpressure on a fake SSE reply must not pin the original SDK image tree."""
import asyncio
import gc
import hashlib
import io
import os
import re
import weakref
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from google.genai import types
from PIL import Image

import image_delivery
import main
from test_req_round15_contracts import Round15Harness, chunks, content, response
from upstreams import express_sdk


class FakeImageLifetimeTests(Round15Harness):
    async def drive_gated_request(self, *, cancel):
        self.settings.update(channel_mode="api_key", fake_streaming=True)
        store = Path(self.enterContext(TemporaryDirectory()))
        self.enterContext(patch.object(image_delivery, "store_directory", return_value=store))
        self.enterContext(patch.dict(os.environ, {"IMAGE_OUTPUT_MODE": "url", "IMAGE_PUBLIC_BASE_URL": "http://test"}))
        refs, digests = [], []

        async def generate(**kwargs):
            with io.BytesIO() as out, Image.new("RGB", (960, 960), "green") as im:
                im.save(out, format="PNG", compress_level=0)
                png = out.getvalue()
            self.assertGreater(len(png), 2 * 1024 * 1024)
            digests.append(hashlib.sha256(png).digest())
            sdk_reply = response(types.Part(inline_data=types.Blob(data=png, mime_type="image/png")), usage=True)
            refs.append(weakref.ref(sdk_reply))
            return sdk_reply

        sdk = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate)))
        self.enterContext(patch.object(main.app.state, "express_key_manager", self.key_manager))
        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        blocked, release = asyncio.Event(), asyncio.Event()

        async def gated_app(scope, receive, send):
            async def on_send(message):
                if not blocked.is_set() and message["type"] == "http.response.body":
                    part = message.get("body", b"").decode()
                    if part.startswith("data: ") and content(part):
                        blocked.set()
                        await release.wait()
                await send(message)
            await main.app(scope, receive, on_send)

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gated_app), base_url="http://test") as client:
            post = asyncio.create_task(client.post("/v1/chat/completions", json={
                "model": "gemini-2.5-flash", "stream": True, "stream_options": {"include_usage": True},
                "messages": [{"role": "user", "content": "a tree"}],
            }, headers={"Authorization": "Bearer test-key", "x-resumable": "0"}))
            try:
                await asyncio.wait_for(blocked.wait(), 5)
                gc.collect()
                pinned = refs[0]() is not None
                if cancel:
                    post.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await post
                else:
                    release.set()
                    result = await asyncio.wait_for(post, 5)
                    self.assertEqual(result.status_code, 200)
                    self.assertTrue(result.text.endswith("data: [DONE]\n\n"))
                    events = chunks(result.text)
                    self.assertEqual([c["finish_reason"] for e in events for c in e["choices"]
                                      if c["finish_reason"]], ["stop"])
                    self.assertEqual(events[-1]["usage"]["total_tokens"], 15)
                    link = re.fullmatch(r"!\[Image\]\((http://test/v1/generated-images/[a-f0-9]{48}\.png)\)",
                                        content(result.text))
                    self.assertIsNotNone(link)
                    downloaded = await client.get(link[1])
                    self.assertEqual(downloaded.status_code, 200)
                    self.assertEqual(hashlib.sha256(downloaded.content).digest(), digests[0])
            finally:
                release.set()
                if not post.done():
                    post.cancel()
                await asyncio.gather(post, return_exceptions=True)
        self.assertFalse(pinned, "the slow client's first real data frame still owns the SDK response")
        gc.collect()
        self.assertIsNone(refs[0]())

    async def test_original_released_before_slow_send_and_complete_image_is_delivered(self):
        await self.drive_gated_request(cancel=False)

    async def test_stop_at_first_data_frame_also_releases_original(self):
        await self.drive_gated_request(cancel=True)
