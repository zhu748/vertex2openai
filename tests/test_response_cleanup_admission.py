"""Generation and image capacity follow the owned transport, not just an iterator."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from starlette.requests import ClientDisconnect

import admission
import streaming
from models import OpenAIRequest
from routes import chat_api
from proxy_resolver import current_proxy_usage
from request_tracking import RequestTrace, current_trace
from upstreams.native_passthrough import _passthrough_once, _relay_stream


class ResponseCleanupAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.general = admission.GenerationLimiter(default_limit=1)
        self.image = admission.GenerationLimiter(default_limit=1, traced=False)
        self.enterContext(patch.object(admission, "generation_limiter", self.general))
        self.enterContext(patch.object(admission, "image_limiter", self.image))
        self.enterContext(patch.object(admission.app_state, "get_committed_setting", return_value=1))
        self.closing, self.release = asyncio.Event(), asyncio.Event()
        self.reading = asyncio.Event()
        self.closes = 0
        self.messages = []
        self.trace = RequestTrace("/v1beta/models/image:streamGenerateContent")

    async def generate(self, operation):
        @admission.limit_generation
        async def request():
            self.usage = current_proxy_usage.get()
            return await admission.run_with_image_limit("gemini-3.1-flash-image-4k", operation)
        token = current_trace.set(self.trace)
        try:
            return await request()
        finally:
            current_trace.reset(token)

    async def close_transport(self):
        self.closes += 1
        self.closing.set()
        await self.release.wait()

    def assert_held(self):
        self.assertEqual((self.general.active, self.image.active), (1, 1))
        self.assertFalse(self.usage.closed)
        self.assertTrue(self.trace.data["generation_active"])
        for limiter in (self.general, self.image):
            with self.assertRaises(admission.GenerationCapacityError):
                limiter.reserve()

    def assert_settled(self, outcome):
        self.assertEqual((self.general.active, self.image.active), (0, 0))
        self.assertTrue(self.usage.closed)
        self.assertFalse(self.trace.data["generation_active"])
        self.assertEqual(self.trace.data["outcome"], outcome)
        self.assertEqual(self.closes, 1)

    async def asgi(self, response, *, reject_headers=False):
        async def send(message):
            if reject_headers:
                raise OSError("client closed before first pull")
            self.messages.append(message)
        await response({"type": "http", "asgi": {"spec_version": "2.4"}},
                       asyncio.Event().wait, send)

    async def test_unstarted_native_httpx_response_owns_cleanup_through_send_failure(self):
        async def upstream(_request):
            parent = self

            class Bytes(httpx.AsyncByteStream):
                async def __aiter__(self):
                    parent.fail("header send failed before the first pull")
                    yield b""
                async def aclose(self):
                    await parent.close_transport()

            return httpx.Response(200, stream=Bytes(), headers={"content-type": "application/json"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            async def operation():
                upstream = await client.send(client.build_request("POST", "http://offline/"), stream=True)
                return await _relay_stream(upstream)

            response = await self.generate(operation)
            caller = asyncio.create_task(self.asgi(response, reject_headers=True))
            await asyncio.wait_for(self.closing.wait(), 1)
            duplicate_close = asyncio.create_task(response.aclose())
            try:
                self.assert_held()
                # Even losing an independent closer must not cancel physical cleanup.
                await asyncio.sleep(0)
                duplicate_close.cancel()
                self.assert_held()
            finally:
                self.release.set()
                await asyncio.gather(duplicate_close, return_exceptions=True)
            with self.assertRaises(ClientDisconnect):
                await asyncio.wait_for(caller, 1)
            await response.aclose()
            self.assert_settled("cancelled")

    async def test_cancel_during_httpx_implicit_eof_close_joins_physical_transport(self):
        parent = self

        class Bytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'[{"candidates":[{"index":0,"finishReason":"STOP"}]}]'

            async def aclose(self):
                await parent.close_transport()

        transport = httpx.MockTransport(lambda _request: httpx.Response(200, stream=Bytes(),
                                        headers={"content-type": "application/json"}))
        async with httpx.AsyncClient(transport=transport) as client:
            async def operation():
                upstream = await _passthrough_once(
                    client, "local", "test", "streamGenerateContent", "alt=json", b"{}", stream=True)
                return await _relay_stream(upstream)

            response = await self.generate(operation)
            caller = asyncio.create_task(self.asgi(response))
            try:
                await asyncio.wait_for(self.closing.wait(), 1)
                caller.cancel()
                done, _ = await asyncio.wait({caller}, timeout=.02)
                self.assertFalse(done)
                self.assert_held()
            finally:
                self.release.set()
            with self.assertRaises(asyncio.CancelledError):
                await caller
            self.assert_settled("cancelled")

    async def check_body_outcome(self, kind):
        async def frames():
            yield "data: {}\n\n"
            self.reading.set()
            if kind == "error":
                raise RuntimeError("body read failed")
            if kind == "cancel":
                await asyncio.Event().wait()

        async def operation():
            return admission.ManagedStreamingResponse(frames(), cleanup=self.close_transport)
        response = await self.generate(operation)
        caller = asyncio.create_task(self.asgi(response))
        try:
            if kind == "cancel":
                await asyncio.wait_for(self.reading.wait(), 1)
                caller.cancel()
            await asyncio.wait_for(self.closing.wait(), 1)
            self.assert_held()
        finally:
            self.release.set()
        if kind == "eof":
            await caller
        else:
            with self.assertRaises(asyncio.CancelledError if kind == "cancel" else RuntimeError):
                await caller
        await response.aclose()
        self.assert_settled({"eof": "succeeded", "error": "error", "cancel": "cancelled"}[kind])

    async def test_eof(self):
        await self.check_body_outcome("eof")

    async def test_read_error(self):
        await self.check_body_outcome("error")

    async def test_cancel_pending_read(self):
        await self.check_body_outcome("cancel")

    async def test_buffer_terminal_joins_transport_before_releasing_and_does_not_pull_again(self):
        async def frames():
            yield "data: [DONE]\n\n"
            self.fail("no trailer pull after accepted terminal")

        async def operation():
            return admission.ManagedStreamingResponse(frames(), cleanup=self.close_transport)
        response = await self.generate(operation)
        self.assertEqual(await anext(response.body_iterator), "data: [DONE]\n\n")
        closing = asyncio.create_task(response.body_iterator.aclose_terminal("succeeded"))
        try:
            await asyncio.wait_for(self.closing.wait(), 1)
            self.assert_held()
        finally:
            self.release.set()
        await closing
        await response.aclose()
        self.assert_settled("succeeded")

    async def test_slow_setup_retains_slot_through_a_late_transport_handoff(self):
        cancelled, let_setup_return = asyncio.Event(), asyncio.Event()
        async def frames():
            yield "unused"
        async def upstream(*_args):
            self.usage = current_proxy_usage.get()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await let_setup_return.wait()
            return admission.ManagedStreamingResponse(frames(), cleanup=self.close_transport)

        self.enterContext(patch.object(chat_api.express_upstream, "chat_completions", upstream))
        self.enterContext(patch.object(chat_api.app_state, "is_web_proxy_enabled", return_value=False))
        self.enterContext(patch.object(streaming, "SETUP_CLOSE_TIMEOUT_SECONDS", .01))
        token = current_trace.set(self.trace)
        try:
            response = await chat_api._call_upstream(
                OpenAIRequest(model="gemini-3.1-flash-image-4k", messages=[], stream=True),
                SimpleNamespace())
        finally:
            current_trace.reset(token)
        closer = asyncio.create_task(response.aclose())
        try:
            await asyncio.wait_for(cancelled.wait(), 1)
            done, _ = await asyncio.wait({closer}, timeout=.03)
            self.assertFalse(done, "setup can still return a transport after the ordinary close timeout")
            self.assert_held()
            let_setup_return.set()
            await asyncio.wait_for(self.closing.wait(), 1)
            self.assert_held()
        finally:
            let_setup_return.set()
            self.release.set()
            await asyncio.wait_for(closer, 1)
        self.assert_settled("cancelled")

    async def test_failed_closer_does_not_leak_or_repeat_cleanup(self):
        async def frames():
            yield b"unused"
        async def fail():
            await self.close_transport()
            raise RuntimeError("failed to close")
        async def operation():
            return admission.ManagedStreamingResponse(frames(), cleanup=fail)
        response = await self.generate(operation)
        closer = asyncio.create_task(response.aclose())
        try:
            await asyncio.wait_for(self.closing.wait(), 1)
            self.assert_held()
        finally:
            self.release.set()
        for operation in (closer, response.aclose()):
            with self.assertRaisesRegex(RuntimeError, "failed to close"):
                await operation
        self.assert_settled("cancelled")
