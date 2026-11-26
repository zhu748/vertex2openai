"""Absolute setup budgets apply to returned responses, not just cooperative awaits."""
import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.responses import JSONResponse

import channel_dispatch as dispatch
from admission import ManagedStreamingResponse
from main import app
from models import OpenAIRequest
from routes import claude_api, gemini_native
from streaming import DONE_FRAME, StreamDeadlineExceeded


class DispatchResponseBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(patch.object(dispatch.app_state, "get_channel_mode", return_value="api_key"))
        self.enterContext(patch.object(dispatch.app_state, "is_web_proxy_enabled", return_value=False))
        self.request = SimpleNamespace(scope={}, is_disconnected=AsyncMock(return_value=False))
        self.model = OpenAIRequest(model="gemini-3.6-flash", messages=[])

    async def test_public_json_routes_reject_result_returned_in_timeout_handler(self):
        self.enterContext(patch.object(claude_api, "_available_models", return_value=[self.model.model]))
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.enterContext(patch.object(gemini_native, "request_timeout_seconds", return_value=.02))
        self.enterContext(patch.object(dispatch, "request_timeout_seconds", return_value=.02))
        self.enterContext(patch.object(dispatch, "_other_channel", return_value=None))
        late = []

        async def translated(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                late.append(1)
                if len(args) >= 5:
                    return JSONResponse({"candidates": [{"finishReason": "STOP"}]})
                return JSONResponse({"choices": [{"message": {"content": "late"}, "finish_reason": "stop"}]})

        self.enterContext(patch.object(dispatch.express_upstream, "chat_completions", translated))
        self.enterContext(patch.object(gemini_native, "_passthrough_request", translated))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local",
                                     headers={"authorization": "Bearer test-key", "x-resumable": "false"}) as client:
            cases = (
                ("/v1/chat/completions", {"model": self.model.model, "messages": []}, "openai"),
                ("/v1/messages", {"model": self.model.model, "max_tokens": 2,
                                  "messages": [{"role": "user", "content": "hello"}]}, "claude"),
                (f"/v1beta/models/{self.model.model}:generateContent", {"contents": []}, "gemini"),
            )
            for path, body, protocol in cases:
                with self.subTest(protocol=protocol):
                    response = await asyncio.wait_for(client.post(path, json=body), 1)
                    self.assertEqual(response.status_code, 504, response.text)
                    payload = response.json()
                    self.assertIn("总执行期限", payload["error"]["message"])
                    if protocol == "claude":
                        self.assertEqual(payload["type"], "error")
                    else:
                        self.assertEqual(payload["error"]["code"], 504)
                    if protocol == "gemini":
                        self.assertEqual(payload["error"]["status"], "DEADLINE_EXCEEDED")
        self.assertEqual(len(late), 3)

    async def test_native_cookie_json_cannot_return_a_late_success(self):
        from credential_manager import credential_manager
        from upstreams import cookie_native

        async def translated(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return JSONResponse({"candidates": [{"finishReason": "STOP"}]})
        with (patch.object(dispatch.app_state, "get_channel_mode", return_value="web_proxy"),
              patch.object(credential_manager, "has_cookie_record", return_value=True),
              patch.object(gemini_native, "request_timeout_seconds", return_value=.02),
              patch.object(cookie_native, "handle_native_request", new=translated)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
                response = await asyncio.wait_for(client.post(
                    f"/v1beta/models/{self.model.model}:generateContent",
                    headers={"x-goog-api-key": "test-key"}, json={"contents": []}), 1)
        self.assertEqual(response.status_code, 504)
        self.assertEqual(response.json()["error"]["status"], "DEADLINE_EXCEEDED")

    async def test_elapsed_primary_503_never_starts_a_second_generation(self):
        now = [time.monotonic()]
        self.enterContext(patch.object(dispatch, "time", SimpleNamespace(monotonic=lambda: now[0])))
        self.enterContext(patch.object(dispatch, "request_timeout_seconds", return_value=1))
        self.enterContext(patch.object(dispatch, "_other_channel", return_value="cookie"))
        alt = self.enterContext(patch.object(dispatch, "_invoke_channel", new=AsyncMock()))

        async def primary(*_):
            now[0] += 1  # synchronous work, returning at the absolute cutoff
            return JSONResponse({}, status_code=503)

        self.enterContext(patch.object(dispatch.express_upstream, "chat_completions", primary))
        with self.assertRaises(StreamDeadlineExceeded):
            await dispatch._dispatch_upstream(self.model, self.request)
        alt.assert_not_awaited()

    async def test_alternate_cannot_start_even_one_tick_past_the_deadline(self):
        alt = self.enterContext(patch.object(dispatch, "_invoke_channel", new=AsyncMock()))
        with self.assertRaises(StreamDeadlineExceeded):
            await dispatch._try_channel_fallback(
                self.model, self.request, "cookie", "primary", "busy", time.monotonic())
        alt.assert_not_awaited()

    async def test_fast_and_slow_sse_fallback_discard_a_late_alternate(self):
        self.enterContext(patch.object(dispatch, "SETUP_FAST_WINDOW_SECONDS", .004))
        self.enterContext(patch.object(dispatch, "_other_channel", return_value="cookie"))
        self.enterContext(patch.object(dispatch, "request_timeout_seconds", return_value=2))
        self.model.stream = True
        for slow in (False, True):
            with self.subTest(slow=slow):
                now = [time.monotonic()]
                closed = AsyncMock()
                pulled = []
                async def unused():
                    pulled.append(1)
                    yield "data: late\n\n"
                async def alt(*_):
                    now[0] += 3
                    return ManagedStreamingResponse(unused(), cleanup=closed)
                async def primary(*_):
                    if slow:
                        await asyncio.sleep(.02)
                    return JSONResponse({"error": {"code": 503}}, status_code=503)
                with (patch.object(dispatch, "time", SimpleNamespace(monotonic=lambda: now[0])),
                      patch.object(dispatch.express_upstream, "chat_completions", primary),
                      patch.object(dispatch, "_invoke_channel", new=AsyncMock(side_effect=alt)) as alternative):
                    response = await dispatch._dispatch_upstream(self.model, self.request)
                    try:
                        frames = [frame async for frame in response.body_iterator]
                    finally:
                        await response.aclose()
                errors = [json.loads(f[5:])["error"]["code"] for f in frames if '"error"' in f]
                self.assertEqual(errors, [504])
                self.assertEqual(frames.count(DONE_FRAME), 1)
                self.assertFalse(pulled)
                closed.assert_awaited_once()
                alternative.assert_awaited_once()

    async def test_external_cancel_beats_translated_alternate_and_joins_discard(self):
        entered, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        closed = AsyncMock()
        async def cleanup():
            closing.set()
            await release.wait()
            await closed()
        async def unused():
            yield "unused"
        async def alternate(*_):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return ManagedStreamingResponse(unused(), cleanup=cleanup)
        with patch.object(dispatch, "_invoke_channel", alternate):
            task = asyncio.create_task(dispatch._try_channel_fallback(
                self.model, self.request, "cookie", "primary", "busy", time.monotonic() + 30))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                task.cancel()
                await asyncio.wait_for(closing.wait(), .4)
                self.assertFalse(task.done())
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
            self.assertTrue(task.cancelled())
        closed.assert_awaited_once()

    async def test_independent_early_setup_timeout_preserves_detail(self):
        cause = TimeoutError("upstream pool timeout")
        with patch.object(dispatch.express_upstream, "chat_completions", AsyncMock(side_effect=cause)):
            with self.assertRaises(TimeoutError) as caught:
                await dispatch._dispatch_upstream(self.model, self.request)
        self.assertIs(caught.exception, cause)
