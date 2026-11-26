"""The admission lease owns an adopted alternative through its physical close."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.responses import JSONResponse, StreamingResponse

import admission
import channel_dispatch as dispatch
from models import OpenAIRequest
from routes import chat_api
from streaming import DONE_FRAME, HEARTBEAT_FRAME


TOKEN = 'data: {"choices":[{"delta":{"content":"alternate"}}]}\n\n'


class AlternateBody:
    def __init__(self, mode, release, managed):
        self.mode, self.release, self.managed = mode, release, managed
        self.entered, self.closing = asyncio.Event(), asyncio.Event()
        self.i = self.closes = self.transports = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.entered.set()
        if self.mode == "before-token":
            await asyncio.Event().wait()
        self.i += 1
        if self.i == 1:
            return TOKEN
        if self.i == 2:
            return DONE_FRAME
        raise StopAsyncIteration

    async def aclose(self):
        self.closes += 1
        if not self.managed:
            await self.close_transport()

    async def close_transport(self):
        self.transports += 1
        self.closing.set()
        await self.release.wait()


class CrossChannelOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def check(self, *, slow, managed, stream_error, mode):
        limiter = admission.GenerationLimiter(default_limit=1)
        release, primary_closed = asyncio.Event(), AsyncMock()
        body = AlternateBody(mode, release, managed)
        model = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=True)
        request = SimpleNamespace(scope={}, is_disconnected=AsyncMock(return_value=False))

        async def primary(*_):
            if slow:
                await asyncio.sleep(.012)
            if stream_error:
                async def frames():
                    yield 'data: {"error":{"code":503}}\n\n'
                    yield DONE_FRAME
                return admission.ManagedStreamingResponse(frames(), cleanup=primary_closed)
            return JSONResponse({"error": {"code": 503}}, status_code=503)

        async def alt(*_):
            if managed:
                return admission.ManagedStreamingResponse(body, cleanup=body.close_transport)
            return StreamingResponse(body)

        with (patch.object(admission, "generation_limiter", limiter),
              patch.object(dispatch, "SETUP_FAST_WINDOW_SECONDS", .002),
              patch.object(dispatch.app_state, "get_channel_mode", return_value="api_key"),
              patch.object(dispatch.app_state, "is_web_proxy_enabled", return_value=False),
              patch.object(dispatch, "_other_channel", return_value="cookie"),
              patch.object(dispatch.express_upstream, "chat_completions", primary),
              patch.object(dispatch, "_invoke_channel", new=AsyncMock(side_effect=alt)) as alternative):
            response = await chat_api._call_upstream(model, request)
            async def exhaust():
                return [f async for f in response.body_iterator]
            try:
                self.assertEqual(await anext(response.body_iterator), HEARTBEAT_FRAME)
                if mode == "before-token":
                    waiter = asyncio.create_task(anext(response.body_iterator))
                    await asyncio.wait_for(body.entered.wait(), .5)
                    waiter.cancel()  # real pending read cancelled on disconnect
                elif mode == "after-token":
                    self.assertEqual(await anext(response.body_iterator), TOKEN)
                    waiter = asyncio.create_task(response.aclose())
                else:
                    waiter = asyncio.create_task(exhaust())
                try:
                    await asyncio.wait_for(body.closing.wait(), .15)
                    await asyncio.sleep(0)
                    self.assertFalse(waiter.done(), "a close/EOF may not escape the transport")
                    self.assertEqual(limiter.active, 1, "generation still owns a live fallback stream")
                finally:
                    release.set()
                    outcome, = await asyncio.gather(waiter, return_exceptions=True)
                if mode == "before-token":
                    self.assertIsInstance(outcome, asyncio.CancelledError)
                elif mode == "eof":
                    self.assertEqual(outcome, [TOKEN, DONE_FRAME])
            finally:
                release.set()
                await response.aclose()
                await response.aclose()
        self.assertEqual(limiter.active, 0)
        self.assertEqual((body.closes, body.transports), (1, 1))
        alternative.assert_awaited_once()
        if stream_error:
            primary_closed.assert_awaited_once()

    async def test_unused_primary_stream_closes_even_if_wrapper_is_never_read(self):
        release = asyncio.Event()
        release.set()
        body = AlternateBody("before-token", release, managed=True)
        primary = admission.ManagedStreamingResponse(body, cleanup=body.close_transport)
        model = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=True)
        request = SimpleNamespace(scope={}, is_disconnected=AsyncMock(return_value=False))
        with (patch.object(dispatch.app_state, "get_channel_mode", return_value="api_key"),
              patch.object(dispatch.app_state, "is_web_proxy_enabled", return_value=False),
              patch.object(dispatch, "_other_channel", return_value="cookie"),
              patch.object(dispatch, "_invoke_channel", new=AsyncMock()) as alt,
              patch.object(dispatch.express_upstream, "chat_completions", new=AsyncMock(return_value=primary))):
            response = await chat_api._call_upstream(model, request)
            await response.aclose()
            await response.aclose()
        self.assertFalse(body.entered.is_set())
        self.assertEqual((body.closes, body.transports), (1, 1))
        alt.assert_not_awaited()

    async def test_alternative_lifetime(self):
        for slow in (False, True):
            for stream_error in (False, True):
                for managed in (False, True):
                    for mode in ("before-token", "after-token", "eof"):
                        with self.subTest(slow=slow, stream_error=stream_error, managed=managed, mode=mode):
                            await self.check(slow=slow, managed=managed, stream_error=stream_error, mode=mode)
