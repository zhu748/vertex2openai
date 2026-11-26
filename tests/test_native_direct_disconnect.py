"""Native JSON generation follows its sole client; streamed bodies retain ownership."""
import asyncio
import json
import unittest
from unittest.mock import patch

from fastapi.responses import JSONResponse, StreamingResponse

from admission import generation_limiter
from credential_manager import credential_manager
from main import app
from routes import gemini_native
from upstreams import cookie_native, native_passthrough


class NativeDisconnectTests(unittest.IsolatedAsyncioTestCase):
    async def _request(self, *, channel, action, stream=False):
        disconnect = asyncio.Event()
        release, entered, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        received = 0
        messages = []
        async def receive():
            nonlocal received
            received += 1
            if received == 1:
                return {"type": "http.request", "body": b'{"contents":[{"parts":[{"text":"hi"}]}]}',
                        "more_body": False}
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            messages.append(message)

        async def upstream(*args, **kwargs):
            if stream:
                async def body():
                    yield b'[{"candidates":[]}]'
                return StreamingResponse(body(), media_type="application/json")
            try:
                entered.set()
                await release.wait()
                return JSONResponse({"late": True})
            finally:
                closed.set()

        path = f"/v1beta/models/gemini-2.5-flash:{action}"
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "POST", "scheme": "http", "path": path,
                 "raw_path": path.encode(), "query_string": b"alt=json", "server": ("test", 80),
                 "client": ("test", 1), "root_path": "", "headers": [
                     (b"content-type", b"application/json"), (b"x-goog-api-key", b"test-key")]}
        cookie = channel == "cookie"
        initial = generation_limiter.active
        with (patch.object(app.state.express_key_manager, "get_total_keys", return_value=int(not cookie)),
              patch.object(credential_manager, "has_cookie_record", return_value=cookie),
              patch.object(gemini_native.app_state, "get_channel_mode", return_value="web_proxy" if cookie else "api_key"),
              patch.object(cookie_native if cookie else native_passthrough,
                           "_handle_native_request_impl" if cookie else "_passthrough_request_impl", upstream)):
            if stream:
                # A successful handoff retains the leased array body through
                # ASGI delivery. The cancellation helper must not consume it.
                await asyncio.wait_for(app(scope, receive, send), 1)
            else:
                task = asyncio.create_task(app(scope, receive, send))
                try:
                    await asyncio.wait_for(entered.wait(), 1)
                    self.assertEqual(generation_limiter.active, initial + 1)
                    disconnect.set()
                    await asyncio.wait_for(scope["state"]["client_connection"].event.wait(), 1)
                    await asyncio.wait_for(closed.wait(), .4)
                finally:
                    release.set()
                    await asyncio.wait_for(task, 1)
                self.assertEqual(received, 2, "only the middleware reads http.disconnect")
        self.assertEqual(generation_limiter.active, initial)
        self.assertEqual([m["status"] for m in messages if m["type"] == "http.response.start"],
                         [200 if stream else 499])
        result = json.loads(b"".join(m["body"] for m in messages if m["type"] == "http.response.body"))
        if stream:
            self.assertEqual(result, [{"candidates": []}])
        else:
            self.assertEqual((result["error"]["code"], result["error"]["status"]), (499, "CANCELLED"))

    async def test_direct_native_disconnect_cancels_operation_and_releases_lease(self):
        for channel, action in (("express", "generateContent"), ("cookie", "generateContent"),
                                ("express", "countTokens"), ("express", "embedContent")):
            with self.subTest(channel=channel, action=action):
                await self._request(channel=channel, action=action)

    async def test_json_array_stream_keeps_ownership_of_response(self):
        for channel in ("express", "cookie"):
            with self.subTest(channel=channel):
                await self._request(channel=channel, action="streamGenerateContent", stream=True)
