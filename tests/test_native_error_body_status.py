"""A broken diagnostic body does not undo an HTTP error response's headers."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx

from main import app
from proxy_failover import ProxyChain
from upstreams import native_passthrough as native


class Truncated(httpx.AsyncByteStream):
    def __init__(self, *, block=False):
        self.block = block
        self.reading = asyncio.Event()
        self.closes = 0

    async def __aiter__(self):
        yield b'{"partial":'
        self.reading.set()
        if self.block:
            await asyncio.Event().wait()
        raise httpx.RemoteProtocolError("fixture: response ended early")

    async def aclose(self):
        self.closes += 1


class NativeErrorBodyStatusTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.manager = SimpleNamespace(get_total_keys=lambda: 1, get_express_record=Mock(return_value={"key": "test"}))
        self.enterContext(patch.object(app.state, "express_key_manager", self.manager))
        self.enterContext(patch.object(native.app_state, "get_channel_mode", return_value="api_key"))
        self.enterContext(patch.object(native, "ProxyChain", side_effect=lambda _: ProxyChain(candidates=[""])))
        self.wait = self.enterContext(patch.object(native, "_wait_for_retry", new=AsyncMock(return_value=True)))
        self.client = await self.enterAsyncContext(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://local", headers={"x-goog-api-key": "test-key"}))

    async def setup_transport(self, handler, retries):
        upstream = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        self.enterContext(patch.object(native.passthrough_client_pool, "get", return_value=upstream))
        self.enterContext(patch.object(native, "get_retry_settings", return_value=(retries, 0, 0)))

    async def generate(self):
        return await self.client.post("/v1beta/models/gemini-3.6-flash:generateContent", json={"contents": []})

    async def check_final_error(self, status, retries):
        stream = Truncated()
        send = Mock(side_effect=lambda _: httpx.Response(
            status, stream=stream, headers={"retry-after": "45", "content-type": "text/html"}))
        await self.setup_transport(send, retries)
        result = await self.generate()
        self.assertEqual((result.status_code, result.headers.get("retry-after")), (status, "45"))
        self.assertIn("application/json", result.headers["content-type"])
        self.assertEqual(result.json()["error"]["code"], status)
        self.assertLess(len(result.content), 512)
        self.assertNotIn(b"partial", result.content)
        send.assert_called_once()
        self.wait.assert_not_awaited()
        self.assertEqual(stream.closes, 1)

    async def test_final_rate_limit_keeps_retry_after(self):
        await self.check_final_error(429, retries=0)

    async def test_known_permanent_400_is_not_retried_as_connection_failure(self):
        await self.check_final_error(400, retries=2)

    async def test_retryable_503_still_retries_and_can_succeed(self):
        stream = Truncated()
        send = Mock(side_effect=[
            httpx.Response(503, stream=stream), httpx.Response(200, json={"candidates": []})])
        await self.setup_transport(send, retries=1)
        result = await self.generate()
        self.assertEqual((result.status_code, result.json()), (200, {"candidates": []}))
        self.assertEqual(send.call_count, 2)
        self.wait.assert_awaited_once()
        self.assertEqual(stream.closes, 1)

    async def test_partial_200_is_not_forwarded_as_an_answer(self):
        stream = Truncated()
        send = Mock(side_effect=lambda _: httpx.Response(200, stream=stream))
        await self.setup_transport(send, retries=0)
        result = await self.generate()
        self.assertEqual((result.status_code, result.json()["error"]["code"]), (502, 502))
        self.assertNotIn(b"partial", result.content)
        send.assert_called_once()
        self.assertEqual(stream.closes, 1)

    async def test_buffered_200_joins_implicit_eof_close_on_cancel(self):
        import admission
        started, permit = asyncio.Event(), asyncio.Event()
        closes = []
        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'{"candidates":[]}'
            async def aclose(self):
                closes.append("start")
                started.set()
                await permit.wait()
                closes.append("finished")
        await self.setup_transport(lambda _: httpx.Response(200, stream=Body()), retries=0)
        request = SimpleNamespace(app=app, is_disconnected=AsyncMock(return_value=False))
        limiter = admission.GenerationLimiter(default_limit=1)
        with patch.object(admission, "generation_limiter", limiter):
            task = asyncio.create_task(native._passthrough_request(
                request, "gemini-3.6-flash", "generateContent", "", b"{}"))
            try:
                await asyncio.wait_for(started.wait(), 1)
                task.cancel()
                done, _ = await asyncio.wait({task}, timeout=.02)
                self.assertFalse(done)
                self.assertEqual(limiter.active, 1)
            finally:
                permit.set()
                outcome, = await asyncio.gather(task, return_exceptions=True)
        self.assertIsInstance(outcome, asyncio.CancelledError)
        self.assertEqual((closes, limiter.active), (["start", "finished"], 0))

    async def test_external_cancellation_is_not_synthesized_as_an_http_error(self):
        stream = Truncated(block=True)
        send = Mock(side_effect=lambda _: httpx.Response(429, stream=stream))
        await self.setup_transport(send, retries=2)
        request = SimpleNamespace(app=app, is_disconnected=AsyncMock(return_value=False))
        task = asyncio.create_task(native._passthrough_request_impl(
            request, "gemini-3.6-flash", "generateContent", "", b"{}"))
        await asyncio.wait_for(stream.reading.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        self.wait.assert_not_awaited()
        self.assertEqual(stream.closes, 1)
