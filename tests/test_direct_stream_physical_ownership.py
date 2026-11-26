"""Direct Chat peer loss joins the actual HTTP stream, not only its generator."""

import asyncio
from contextlib import asynccontextmanager
import json
import time
import unittest
from unittest.mock import patch

import anyio
from google import genai
import httpx

from admission import generation_limiter
from http_io import BoundedAsyncClient
from http_options import get_http_options
from request_tracking import request_registry
from streaming import reads_before_deadline
from test_parallel_tool_controls import _request
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery, express_sdk


PARTIAL = "unfinished fixture text"
COMPLETE = "complete fixture text"
PATH, REQUEST = _request("openai", None, tools=False, stream=True)
DIRECT = [(b"x-resumable", b"false")]


def payload(channel, text, *, complete):
    candidate = {"content": {"role": "model", "parts": [{"text": text}]}}
    if complete:
        candidate["finishReason"] = "STOP"
    body = {"candidates": [candidate]}
    if channel == "express":
        return b"data: " + json.dumps(body).encode() + b"\n\n"
    return json.dumps({"results": [{"data": body}]}).encode() + b"\n"


class SlowPhysicalStream(httpx.AsyncByteStream):
    def __init__(self, channel):
        self.tail = payload(channel, COMPLETE, complete=True)
        self.finish = asyncio.Event()
        self.body = payload(channel, PARTIAL, complete=False)
        self.closing, self.release, self.closed = (asyncio.Event() for _ in range(3))
        self.reads = self.closes = 0

    async def __aiter__(self):
        self.reads += 1
        yield self.body
        self.reads += 1
        await self.finish.wait()
        yield self.tail

    async def aclose(self):
        self.closes += 1
        self.closing.set()
        await self.release.wait()
        self.closed.set()


class PhysicalOwnershipTests(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings["upstream_max_concurrency"] = 1
        self.calls = []
        self.physical = []

        async def express(request):
            return self.upstream("express", request)

        async def cookie(request):
            return self.upstream("cookie", request)

        options = get_http_options(base_url="https://offline.invalid", proxy_url="")
        options.async_client_args = dict(options.async_client_args, transport=httpx.MockTransport(express))
        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=options)
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        studio = await self.enterAsyncContext(BoundedAsyncClient(transport=httpx.MockTransport(cookie)))

        @asynccontextmanager
        async def open_studio(*_args, **_kwargs):
            yield studio

        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        self.enterContext(patch.object(cookie_delivery, "open_upstream_client", open_studio))

    def upstream(self, channel, _request):
        self.calls.append(channel)
        headers = {"content-type": "text/event-stream" if channel == "express" else "application/json"}
        if len(self.calls) <= len(self.physical):
            return httpx.Response(200, stream=self.physical[len(self.calls) - 1], headers=headers)
        return httpx.Response(200, content=payload(channel, COMPLETE, complete=True), headers=headers)

    async def check(self, channel, spec, *, fallback, external_cancel):
        self.channel = "api_key" if channel == "express" else "web_proxy"
        self.settings["cross_channel_fallback"] = fallback
        self.calls.clear()
        source = SlowPhysicalStream(channel)
        self.physical = [source]
        peer = Peer(PATH, spec=spec, payload=REQUEST, headers=DIRECT)
        send_returned = asyncio.Event()

        async def drop_during_send(message):
            if PARTIAL.encode() in message.get("body", b""):
                peer.gone.set()
                # Model a write that succeeds at the ASGI boundary but is
                # silently dropped by the transport before we pull again.
                with anyio.CancelScope(shield=True):
                    await peer.observed.wait()
                send_returned.set()

        sender = asyncio.create_task(peer.run(drop_during_send))
        try:
            await asyncio.wait_for(send_returned.wait(), 2)
            await asyncio.wait_for(source.closing.wait(), 2)
            self.assertFalse(sender.done())
            self.assertEqual((self.calls, source.reads, source.closes), ([channel], 1, 1))
            self.assertFalse(source.closed.is_set())
            self.assertEqual(generation_limiter.active, 1)
            follow = {**REQUEST, "messages": [{"role": "user", "content": "another request"}]}

            if external_cancel:
                sender.cancel("independent server cancellation")
                await asyncio.sleep(0)
                sender.cancel("repeated independent server cancellation")
                await asyncio.sleep(0)
            rejected = await asyncio.wait_for(self.client.post(PATH, json=follow), 2)
            self.assertEqual(rejected.status_code, 503)
            self.assertEqual(self.calls, [channel])
            self.assertFalse(sender.done())
            self.assertFalse(source.closed.is_set())

            source.release.set()
            if external_cancel:
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(asyncio.shield(sender), 2)
            else:
                await asyncio.wait_for(asyncio.shield(sender), 2)
            self.assertTrue(source.closed.is_set())
            self.assertEqual((source.closes, generation_limiter.active), (1, 0))
            self.assertEqual(peer.reads, 2)
            self.assertNotIn(b"[DONE]", b"".join(m.get("body", b"") for m in peer.messages))
            trace_id = dict(peer.messages[0]["headers"])[b"x-trace-id"].decode()
            trace = request_registry.get(trace_id).snapshot()
            self.assertEqual((trace["connection"], trace["outcome"], trace["generation_active"]),
                             ("disconnected", "cancelled", False))
            self.assertEqual(trace["sent_content_chars"], 0)

            ready = await asyncio.wait_for(self.client.post(PATH, json=follow), 2)
            self.assertEqual((ready.status_code, self.calls), (200, [channel, channel]))
            self.assertIn(COMPLETE, ready.text)
            self.assertEqual(ready.text.count("[DONE]"), 1)
            self.assertEqual(request_registry.get(ready.headers["x-trace-id"]).snapshot()["outcome"], "succeeded")
        finally:
            source.release.set()
            if not sender.done():
                sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    async def test_silent_peer_drop_holds_admission_until_physical_close(self):
        for channel in ("express", "cookie"):
            for spec in ("2.3", "2.4"):
                for fallback in (False, True):
                    with self.subTest(channel=channel, spec=spec, fallback=fallback):
                        await self.check(channel, spec, fallback=fallback, external_cancel=False)

    async def test_external_server_cancel_still_propagates_after_peer_drop(self):
        for channel in ("express", "cookie"):
            for spec in ("2.3", "2.4"):
                with self.subTest(channel=channel, spec=spec):
                    await self.check(channel, spec, fallback=True, external_cancel=True)

    async def test_disconnect_closes_no_other_stream_on_the_same_sdk_client(self):
        self.settings["upstream_max_concurrency"] = 2
        left, right = self.physical = [SlowPhysicalStream("express"), SlowPhysicalStream("express")]
        hold, keep = [Peer(PATH, payload=REQUEST, headers=DIRECT) for _ in range(2)]
        left_visible, right_visible = asyncio.Event(), asyncio.Event()

        def observe(visible):
            async def on_send(message):
                if PARTIAL.encode() in message.get("body", b""):
                    visible.set()
            return on_send

        first = asyncio.create_task(hold.run(observe(left_visible)))
        second = None
        try:
            await asyncio.wait_for(left_visible.wait(), 2)
            second = asyncio.create_task(keep.run(observe(right_visible)))
            await asyncio.wait_for(right_visible.wait(), 2)
            self.assertEqual(generation_limiter.active, 2)
            await hold.drop()
            await asyncio.wait_for(left.closing.wait(), 2)
            self.assertEqual(generation_limiter.active, 2)
            self.assertFalse(right.closing.is_set())

            right.release.set()
            right.finish.set()
            await asyncio.wait_for(asyncio.shield(second), 2)
            self.assertEqual(self.calls, ["express", "express"])
            self.assertEqual((right.closes, generation_limiter.active), (1, 1))
            self.assertFalse(first.done())
            self.assertFalse(left.closed.is_set())
            self.assertIn(b"[DONE]", b"".join(message.get("body", b"") for message in keep.messages))

            left.release.set()
            await asyncio.wait_for(asyncio.shield(first), 2)
            self.assertEqual((left.closes, generation_limiter.active), (1, 0))
        finally:
            for source in (left, right):
                source.release.set()
            tasks = [task for task in (first, second) if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


class ReadScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_translated_peer_cancellation_keeps_its_anyio_scope(self):
        for outcome in ("eof", "error", "data"):
            with self.subTest(outcome=outcome):
                waiting, scopes = asyncio.Event(), []

                async def adapter():
                    waiting.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        if outcome == "eof":
                            return
                        if outcome == "error":
                            raise httpx.ReadError("transport translated cancellation") from None
                        yield b"abandoned result"

                async def consume():
                    iterator = adapter()
                    try:
                        with anyio.CancelScope() as scope:
                            scopes.append(scope)
                            self.fail(await anext(reads_before_deadline(iterator, time.monotonic() + 5), "EOF"))
                        self.assertTrue(scope.cancelled_caught)
                    finally:
                        await iterator.aclose()

                task = asyncio.create_task(consume())
                try:
                    await asyncio.wait_for(waiting.wait(), 1)
                    scopes[0].cancel()
                    await asyncio.wait_for(asyncio.shield(task), 1)
                finally:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
