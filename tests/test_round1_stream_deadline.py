"""An absolute response budget belongs to the active read, not the SSE consumer."""
import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.responses import JSONResponse, StreamingResponse

import channel_dispatch
from admission import generation_limiter
from main import app
from models import OpenAIRequest
from request_connection import ClientConnection
from resumable import ResumableStore
from routes import chat_api, gemini_native
from streaming import DONE_FRAME, HEARTBEAT_FRAME
from upstreams.native_passthrough import _relay_stream


class LateResponseStream(httpx.AsyncByteStream):
    def __init__(self):
        self.closed_count = 0

    async def __aiter__(self):
        # Give the regression a finite end even if the deadline does not fire.
        await asyncio.sleep(.18)
        yield b'data: {"candidates":[{"finishReason":"STOP"}]}\n\n'

    async def aclose(self):
        self.closed_count += 1


class StreamDeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_slow_setup_new_task_per_read_expires_and_closes(self):
        release_setup = asyncio.Event()
        source = LateResponseStream()

        async def prepare(*args):
            # Wait for the real route's prepare_sse fast window to elapse.
            await release_setup.wait()
            upstream = httpx.Response(200, headers={"content-type": "text/event-stream"},
                                      stream=source)
            return await _relay_stream(upstream, keepalive=True, deadline=time.monotonic() + .06)

        frames = []
        body = json.dumps({"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}).encode()
        sent_body = False

        async def receive():
            nonlocal sent_body
            if not sent_body:
                sent_body = True
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.Event().wait()

        async def send(message):
            if message["type"] == "http.response.body":
                chunk = message.get("body", b"")
                frames.append(chunk)
                if chunk == b"\n\n":
                    release_setup.set()

        path = "/v1beta/models/gemini-2.5-flash:streamGenerateContent"
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "POST", "scheme": "http", "path": path,
                 "raw_path": path.encode(), "query_string": b"alt=sse", "server": ("test", 80),
                 "client": ("test", 1), "root_path": "",
                 "headers": [(b"content-type", b"application/json"), (b"x-goog-api-key", b"test-key")]}
        with (patch.object(gemini_native, "_passthrough_request", prepare),
              patch.object(app.state.express_key_manager, "get_total_keys", return_value=1),
              patch.object(gemini_native.app_state, "get_channel_mode", return_value="api_key")):
            await asyncio.wait_for(app(scope, receive, send), 2)
        self.assertTrue(release_setup.is_set())
        events = [json.loads(line[5:]) for line in b"".join(frames).splitlines()
                  if line.startswith(b"data:")]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["error"]["code"], 504)
        self.assertEqual(events[0]["error"]["status"], "DEADLINE_EXCEEDED")
        self.assertTrue(events[0]["error"]["message"])
        self.assertEqual(source.closed_count, 1)

    async def test_openai_deadline_does_not_cancel_consumer_between_pulls(self):
        async def events():
            yield 'data: {"choices":[{"delta":{"content":"late"}}]}\n\n'
        model = OpenAIRequest(model="gemini-2.5-flash", messages=[], stream=True)
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        with (patch.object(channel_dispatch.app_state, "get_channel_mode", return_value="api_key"),
              patch.object(channel_dispatch.app_state, "is_web_proxy_enabled", return_value=False),
              patch.object(channel_dispatch, "_other_channel", return_value=None),
              patch.object(channel_dispatch, "request_timeout_seconds", return_value=.05),
              patch.object(channel_dispatch.express_upstream, "chat_completions",
                           new=AsyncMock(return_value=StreamingResponse(events())))):
            response = await channel_dispatch._dispatch_upstream(model, request)
            try:
                self.assertEqual(await anext(response.body_iterator), HEARTBEAT_FRAME)
                # Simulate the ASGI sender (not the upstream reader) slowly flushing a frame.
                await asyncio.sleep(.1)
                remaining_frames = [frame async for frame in response.body_iterator]
            finally:
                await response.aclose()
        error_frame, end = remaining_frames
        self.assertEqual(json.loads(error_frame[5:])["error"]["code"], 504)
        self.assertEqual(end, DONE_FRAME)


    async def test_openai_terminal_was_already_sent_before_total_deadline(self):
        for done in (DONE_FRAME, DONE_FRAME.encode()):
            with self.subTest(done_type=type(done).__name__):
                finalized, closed = [], []

                async def events():
                    try:
                        yield done
                        finalized.append(True)  # Production generators complete bookkeeping here.
                    finally:
                        closed.append(True)

                model = OpenAIRequest(model="gemini-2.5-flash", messages=[], stream=True)
                request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
                with (patch.object(channel_dispatch.app_state, "get_channel_mode", return_value="api_key"),
                      patch.object(channel_dispatch.app_state, "is_web_proxy_enabled", return_value=False),
                      patch.object(channel_dispatch, "_other_channel", return_value=None),
                      patch.object(channel_dispatch, "request_timeout_seconds", return_value=.15),
                      patch.object(channel_dispatch.express_upstream, "chat_completions",
                                   new=AsyncMock(return_value=StreamingResponse(events())))):
                    response = await channel_dispatch._dispatch_upstream(model, request)
                    try:
                        self.assertEqual(await anext(response.body_iterator), HEARTBEAT_FRAME)
                        self.assertEqual(await anext(response.body_iterator), done)
                        # Some wrappers immediately stop pulling at DONE.
                        self.assertEqual(finalized, [True])
                        self.assertEqual(closed, [True])
                        await asyncio.sleep(.20)
                        self.assertEqual([frame async for frame in response.body_iterator], [])
                    finally:
                        await response.aclose()
                self.assertEqual(finalized, [True])
                self.assertEqual(closed, [True])


    async def test_openai_ignores_unexpected_trailer_and_bounds_finalize_pull(self):
        for trailer in ("stalled", "extra_frame", "error"):
            with self.subTest(trailer=trailer):
                closed = asyncio.Event()
                async def events():
                    try:
                        yield DONE_FRAME
                        if trailer == "stalled":
                            await asyncio.Event().wait()
                        elif trailer == "extra_frame":
                            yield 'data: {"late":true}\n\n'
                        else:
                            raise RuntimeError("unexpected post-terminal failure")
                    finally:
                        closed.set()

                model = OpenAIRequest(model="gemini-2.5-flash", messages=[], stream=True)
                request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
                with (patch.object(channel_dispatch.app_state, "get_channel_mode", return_value="api_key"),
                      patch.object(channel_dispatch.app_state, "is_web_proxy_enabled", return_value=False),
                      patch.object(channel_dispatch, "_other_channel", return_value=None),
                      patch.object(channel_dispatch, "_TERMINAL_FINALIZE_TIMEOUT_SECONDS", .01),
                      patch.object(channel_dispatch.express_upstream, "chat_completions",
                                   new=AsyncMock(return_value=StreamingResponse(events())))):
                    response = await channel_dispatch._dispatch_upstream(model, request)
                    try:
                        async with asyncio.timeout(.5):
                            frames = [frame async for frame in response.body_iterator]
                        self.assertEqual(frames, [HEARTBEAT_FRAME, DONE_FRAME])
                        self.assertTrue(closed.is_set())
                    finally:
                        await response.aclose()


class JsonDisconnectTests(unittest.IsolatedAsyncioTestCase):
    async def check_disconnect(self, *, detached=False, in_fallback=False):
        started, release, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()
        disconnect = asyncio.Event()
        finished_normally = []
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)

        async def slow(*_args, **_kwargs):
            started.set()
            try:
                await release.wait()
                finished_normally.append(True)
                return JSONResponse({"choices": [{"message": {"content": "ok"}}]})
            finally:
                stopped.set()

        sent_messages, receives = [], []
        body = json.dumps({"model": "gemini-2.5-flash", "messages": [
            {"role": "user", "content": f"disconnect test: {detached}/{in_fallback}"}]}).encode()

        async def receive():
            receives.append(1)
            if len(receives) == 1:
                return {"type": "http.request", "body": body, "more_body": False}
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            sent_messages.append(message)

        path = "/v1/chat/completions"
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "POST", "scheme": "http", "path": path,
                 "raw_path": path.encode(), "query_string": b"", "server": ("test", 80),
                 "client": ("test", 1), "root_path": "", "headers": [
                     (b"content-type", b"application/json"), (b"authorization", b"Bearer test-key"),
                     (b"x-resumable", b"true" if detached else b"false")]}
        initial_active = generation_limiter.active
        first = (AsyncMock(return_value=JSONResponse({"error": {"code": 503}}, status_code=503))
                 if in_fallback else slow)
        with (patch.object(channel_dispatch.app_state, "get_channel_mode", return_value="api_key"),
              patch.object(channel_dispatch.app_state, "is_web_proxy_enabled", return_value=False),
              patch.object(channel_dispatch, "_other_channel", return_value="cookie" if in_fallback else None),
              patch.object(channel_dispatch.express_upstream, "chat_completions", new=first),
              patch.object(channel_dispatch.cookie_upstream, "chat_completions", new=slow),
              patch.object(chat_api, "resumable_store", store),
              patch.object(chat_api, "resumable_enabled", return_value=True)):
            task = asyncio.create_task(app(scope, receive, send))
            try:
                await asyncio.wait_for(started.wait(), 1.0)
                self.assertEqual(generation_limiter.active, initial_active + 1)
                disconnect.set()
                await asyncio.wait_for(scope["state"]["client_connection"].event.wait(), 1.0)
                if detached:
                    # The HTTP reader finishes even while the real generation
                    # lease and the detached pump are still active.
                    await asyncio.wait_for(asyncio.shield(task), 1.0)
                    self.assertFalse(stopped.is_set())
                    self.assertEqual(generation_limiter.active, initial_active + 1)
                    release.set()
                await asyncio.wait_for(stopped.wait(), .5)
                if detached:
                    receipt = dict(sent_messages[0]["headers"])[b"x-resume-id"].decode()
                    buf = await store.get_exact(receipt)
                    await buf.wait_for(0, 1.0)
                    self.assertEqual(buf.status_code, 200)
                    self.assertEqual(buf.payload, {"choices": [{"message": {"content": "ok"}}]})
                    self.assertFalse(buf.fully_delivered)
            finally:
                release.set()
                await asyncio.wait_for(task, 1.0)
        self.assertEqual(generation_limiter.active, initial_active)
        self.assertEqual(len(receives), 2, "only the existing watcher owns the disconnect receive")
        self.assertEqual(bool(finished_normally), detached)
        self.assertEqual([m["status"] for m in sent_messages if m["type"] == "http.response.start"], [499])

    async def test_direct_disconnect_joins_preparation_and_fallback(self):
        for in_fallback in (False, True):
            with self.subTest(in_fallback=in_fallback):
                await self.check_disconnect(in_fallback=in_fallback)

    async def test_resumable_json_keeps_generating_after_disconnect(self):
        await self.check_disconnect(detached=True)

    async def test_server_cancellation_still_cancels_child_and_removes_listener(self):
        connection = ClientConnection()
        entered, closed = asyncio.Event(), asyncio.Event()

        async def work():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

        owner = asyncio.create_task(connection.while_connected(work))
        await asyncio.wait_for(entered.wait(), .5)
        owner.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await owner
        self.assertTrue(closed.is_set())
        self.assertFalse(connection.disconnected)
        self.assertFalse(connection.listeners)
