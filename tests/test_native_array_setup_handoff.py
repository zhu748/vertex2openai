"""ASGI/HTTPX-only native stream acquisition, delivery and disconnect races."""
import asyncio
import json
import unittest
from unittest.mock import patch

import httpx

from admission import generation_limiter, image_limiter
from credential_manager import credential_manager
from main import app
from proxy_failover import ProxyChain
from request_connection import ClientConnection
from resumable import ResumableStore
from routes import gemini_native
from upstreams import native_passthrough as native

ARRAY = b'[{"candidates":[{"index":0,"content":{"parts":[{"text":"hi"}]},"finishReason":"STOP"}]}]'


class ArrayHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gone, self.entered = asyncio.Event(), asyncio.Event()
        self.closing, self.let_close = asyncio.Event(), asyncio.Event()
        self.sent, self.reads, self.closes = [], 0, 0
        self.base = generation_limiter.active, image_limiter.active
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.enterContext(patch.object(app.state.express_key_manager, "get_express_record",
                                      return_value={"key": "local-only"}))
        self.enterContext(patch.object(credential_manager, "get_cookie_record", side_effect=AssertionError("unexpected Cookie selection")))
        self.enterContext(patch.object(gemini_native.app_state, "get_channel_mode", return_value="api_key"))
        self.enterContext(patch.object(native, "ProxyChain", side_effect=lambda _: ProxyChain(candidates=[""])))
        self.enterContext(patch.object(native.first_reply_module, "timeout_seconds", return_value=60))

    def active(self, delta):
        self.assertEqual((generation_limiter.active, image_limiter.active),
                         (self.base[0] + delta, self.base[1] + delta))

    async def close_bytes(self):
        self.closes += 1
        self.closing.set()
        await self.let_close.wait()

    def byte_stream(self, *, block=False, unused=False):
        parent = self
        class Bytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                parent.assertFalse(unused, "an abandoned setup must never start delivering bytes")
                if block:
                    parent.entered.set()
                    await asyncio.Event().wait()
                yield ARRAY
            async def aclose(self):
                await parent.close_bytes()
        return Bytes()

    def use_transport(self, handler):
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.addAsyncCleanup(client.aclose)
        self.enterContext(patch.object(native.passthrough_client_pool, "get", return_value=client))

    def launch(self, query=b"alt=json", *, spec="2.4", resume=False, on_send=None):
        async def receive():
            self.reads += 1
            if self.reads == 1:
                return {"type": "http.request", "body": b'{"contents":[{"parts":[{"text":"hi"}]}]}',
                        "more_body": False}
            await self.gone.wait()
            return {"type": "http.disconnect"}
        async def send(message):
            self.sent.append(message)
            if on_send is not None:
                on_send(message)
        path = "/v1beta/models/gemini-3.1-flash-image-4k:streamGenerateContent"
        self.scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": spec},
                      "http_version": "1.1", "method": "POST", "scheme": "http", "path": path,
                      "raw_path": path.encode(), "query_string": query, "server": ("local", 80),
                      "client": ("local", 1), "root_path": "", "headers": [
                          (b"content-type", b"application/json"), (b"x-goog-api-key", b"test-key"),
                          (b"x-resumable", b"true" if resume else b"false")]}
        return asyncio.create_task(app(self.scope, receive, send))

    async def assert_disconnect(self, caller):
        try:
            await asyncio.wait_for(self.entered.wait(), 1)
            self.active(1)
            self.gone.set()
            await asyncio.wait_for(self.scope["state"]["client_connection"].event.wait(), 1)
            await asyncio.wait_for(self.closing.wait(), 1)
            self.assertEqual(self.sent, [], "never commit array headers after observed disconnect")
            self.active(1)
        finally:
            self.let_close.set()
            await asyncio.wait_for(caller, 1)
        self.check_499()

    def check_499(self):
        self.assertEqual(self.reads, 2, "only the middleware reads the ASGI receiver")
        self.assertEqual(self.closes, 1)
        self.active(0)
        self.assertEqual([m["status"] for m in self.sent if m["type"] == "http.response.start"], [499])
        body = json.loads(b"".join(m["body"] for m in self.sent if m["type"] == "http.response.body"))
        self.assertEqual((body["error"]["code"], body["error"]["status"]), (499, "CANCELLED"))

    async def test_default_array_disconnect_while_waiting_for_httpx_headers(self):
        async def upstream(_request):
            try:
                self.entered.set()
                await asyncio.Event().wait()
            finally:
                await self.close_bytes()
        self.use_transport(upstream)
        await self.assert_disconnect(self.launch(query=b""))

    async def test_explicit_array_disconnect_during_real_first_reply_prefetch(self):
        self.use_transport(lambda _: httpx.Response(200, stream=self.byte_stream(block=True),
                                                    headers={"content-type": "application/json"}))
        await self.assert_disconnect(self.launch())

    async def test_cancel_suppressed_setup_returns_a_transport_for_disposal_not_delivery(self):
        async def operation(*args):
            try:
                self.entered.set()
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                upstream = httpx.Response(200, stream=self.byte_stream(unused=True))
                return await native._relay_stream(upstream)
        self.enterContext(patch.object(native, "_passthrough_request_impl", operation))
        await self.assert_disconnect(self.launch())

    async def test_disconnect_is_not_relabelled_as_translated_upstream_error(self):
        async def upstream(_request):
            try:
                self.entered.set()
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await self.close_bytes()
                raise RuntimeError("transport cancelled by its client") from None
        self.use_transport(upstream)
        await self.assert_disconnect(self.launch())

    async def test_completed_result_and_disconnect_in_same_loop_turn(self):
        async def operation(*args):
            upstream = httpx.Response(200, stream=self.byte_stream(unused=True))
            self.gone.set()  # wake the sole receiver immediately before returning ownership
            return await native._relay_stream(upstream)
        self.enterContext(patch.object(native, "_passthrough_request_impl", operation))
        caller = self.launch()
        try:
            await asyncio.wait_for(self.closing.wait(), 1)
            self.active(1)
            self.assertFalse(self.sent)
        finally:
            self.let_close.set()
            await asyncio.wait_for(caller, 1)
        self.check_499()

    async def test_normal_prefetched_response_preserves_headers_body_and_ownership(self):
        self.use_transport(lambda _: httpx.Response(200, stream=self.byte_stream(), headers={
            "content-type": "application/json", "retry-after": "2"}))
        self.let_close.set()
        await asyncio.wait_for(self.launch(), 1)
        start, = (m for m in self.sent if m["type"] == "http.response.start")
        self.assertEqual(start["status"], 200)
        self.assertEqual(dict(start["headers"])[b"retry-after"], b"2")
        self.assertEqual(b"".join(m["body"] for m in self.sent if m["type"] == "http.response.body"), ARRAY)
        self.assertEqual(self.closes, 1)
        self.active(0)

    async def check_committed_body(self, *, sse, spec, disconnect=True, slow=False, server_cancel=False):
        head = b'{"candidates":[{"content":{"parts":[{"text":"first fragment"}]}}]}'
        tail = b'{"candidates":[{"finishReason":"STOP"}]}'
        head, tail = ((b"data: " + head + b"\n\n", b"data: " + tail + b"\n\n") if sse
                      else (b"[  " + head + b",", tail + b" ]"))
        allow_read, read_cancelled = asyncio.Event(), asyncio.Event()
        owner = self

        class Bytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield head
                owner.entered.set()
                try:
                    await allow_read.wait()
                except asyncio.CancelledError:
                    read_cancelled.set()
                    raise
                yield tail

            async def aclose(self):
                await owner.close_bytes()

        content_type = "text/event-stream" if sse else "application/json"
        started_sending, head_sent = asyncio.Event(), asyncio.Event()
        async def transport(_):
            if slow:
                await started_sending.wait()  # Real slow-path heartbeat handoff.
            return httpx.Response(200, stream=Bytes(), headers={
                "content-type": content_type, "retry-after": "3"})
        def on_send(message):
            if message["type"] == "http.response.start":
                started_sending.set()
            if message.get("body") == head:
                head_sent.set()
        self.use_transport(transport)
        success = self.enterContext(patch.object(native.stats, "add_success"))
        caller = self.launch(query=b"alt=sse" if sse else b"", spec=spec, on_send=on_send)
        try:
            await asyncio.wait_for(self.entered.wait(), 1)
            await asyncio.wait_for(head_sent.wait(), 1)
            self.active(1)
            if disconnect:
                self.gone.set()
                await asyncio.wait_for(self.scope["state"]["client_connection"].event.wait(), 1)
            else:
                allow_read.set()
            # send() does not raise when disconnected. A quiet second pull
            # must still be cancelled without waiting for a heartbeat or EOF.
            await asyncio.wait_for(self.closing.wait(), 1)
            self.assertEqual(read_cancelled.is_set(), disconnect)
            if server_cancel:
                caller.cancel()  # independent server cancellation while disconnect cleanup is gated
                await asyncio.sleep(0)
            self.active(1)
            self.assertFalse(caller.done())
        finally:
            allow_read.set()
            self.let_close.set()
            if server_cancel:
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(caller, 1)
            else:
                await asyncio.wait_for(caller, 1)
        self.assertEqual(self.closes, 1)
        self.active(0)
        self.assertEqual(self.reads, 2, "no second raw disconnect receiver")
        self.assertFalse(self.scope["state"]["client_connection"].listeners)
        start, = (m for m in self.sent if m["type"] == "http.response.start")
        self.assertEqual(start["status"], 200)
        if not slow:
            self.assertEqual(dict(start["headers"])[b"retry-after"], b"3")
        self.assertEqual(success.call_count, int(not disconnect))
        bodies = [m["body"] for m in self.sent if m["type"] == "http.response.body"]
        self.assertEqual(b"".join(b for b in bodies if b != b"\n\n"),
                         head if disconnect else head + tail)
        self.assertEqual(any(not m.get("more_body") for m in self.sent
                             if m["type"] == "http.response.body"), not disconnect)

    async def test_quiet_committed_array_observes_disconnect_asgi24(self):
        await self.check_committed_body(sse=False, spec="2.4")

    async def test_quiet_committed_sse_observes_disconnect_asgi24(self):
        await self.check_committed_body(sse=True, spec="2.4")

    async def test_quiet_committed_array_observes_disconnect_asgi23(self):
        await self.check_committed_body(sse=False, spec="2.3")

    async def test_quiet_committed_sse_observes_disconnect_asgi23(self):
        await self.check_committed_body(sse=True, spec="2.3")

    async def test_external_cancel_during_disconnected_array_cleanup_still_propagates(self):
        await self.check_committed_body(sse=False, spec="2.4", server_cancel=True)

    async def test_external_cancel_during_disconnected_sse_cleanup_still_propagates(self):
        await self.check_committed_body(sse=True, spec="2.4", server_cancel=True)

    async def test_quiet_committed_slow_sse_uses_its_actual_sender(self):
        await self.check_committed_body(sse=True, spec="2.4", slow=True)

    async def test_detached_native_reader_disconnect_does_not_cancel_generation(self):
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(native, "resumable_store", store))
        self.enterContext(patch.object(gemini_native, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(gemini_native, "image_bypass_enabled", return_value=False))
        owner, release_read, head_sent = self, asyncio.Event(), asyncio.Event()

        class Bytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'data: {"candidates":[{"content":{"parts":[{"text":"first fragment"}]}}]}\n\n'
                owner.entered.set()
                await release_read.wait()
                yield b'data: {"candidates":[{"finishReason":"STOP"}]}\n\n'
            async def aclose(self):
                await owner.close_bytes()

        def on_send(message):
            if b"first fragment" in message.get("body", b""):
                head_sent.set()
        self.use_transport(lambda _: httpx.Response(200, stream=Bytes(), headers={"content-type": "text/event-stream"}))
        caller = self.launch(query=b"alt=sse", resume=True, on_send=on_send)
        try:
            await asyncio.wait_for(self.entered.wait(), 1)
            await asyncio.wait_for(head_sent.wait(), 1)
            self.gone.set()
            await asyncio.wait_for(asyncio.shield(caller), 1)
            self.assertFalse(self.closing.is_set(), "only the replay reader left")
            self.active(1)
            release_read.set()
            await asyncio.wait_for(self.closing.wait(), 1)
            self.active(1)
        finally:
            release_read.set()
            self.let_close.set()
            await asyncio.wait_for(caller, 1)
        receipt = dict(self.sent[0]["headers"])[b"x-resume-id"].decode()
        buffer = await store.get_exact(receipt)
        await asyncio.wait_for(buffer.wait_for(2**62, 1), 2)
        self.assertEqual((buffer.done, buffer.fully_delivered, buffer.status_code), (True, False, 200))
        self.active(0)
        self.assertEqual((self.reads, self.closes), (2, 1))

    async def test_normal_array_delivery_preserves_payload_bytes_and_close(self):
        await self.check_committed_body(sse=False, spec="2.4", disconnect=False)

    async def test_normal_sse_delivery_preserves_payload_bytes_and_close(self):
        await self.check_committed_body(sse=True, spec="2.4", disconnect=False)

    async def test_external_cancellation_in_handoff_tick_disposes_and_propagates(self):
        connection = ClientConnection()
        delivered = []
        async def operation():
            asyncio.get_running_loop().call_soon(waiter.cancel)
            return object()
        async def discard(result):
            delivered.append(result)
            await self.close_bytes()
        waiter = asyncio.create_task(connection.while_connected(operation, discard=discard))
        try:
            await asyncio.wait_for(self.closing.wait(), 1)
            waiter.cancel()  # second cancellation must not interrupt physical disposal
        finally:
            self.let_close.set()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual((self.closes, len(delivered)), (1, 1))
        self.assertEqual(connection.listeners, [])
