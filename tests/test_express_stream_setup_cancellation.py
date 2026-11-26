"""A terminated SDK stream must not turn failed pre-output cleanup into another request."""

import asyncio
import json
from unittest.mock import patch

import httpx
from google import genai

from admission import generation_limiter
from http_options import get_http_options
from proxy_failover import ProxyChain
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from upstreams import express_sdk


SUCCESS = (b'data: {"candidates":[{"content":{"role":"model","parts":[{"text":"second outlet"}]},'
           b'"finishReason":"STOP"}]}\n\n')
OUTLETS = ("http://first.invalid:8080", "http://second.invalid:8081")


class BeforeFirstOutput(httpx.AsyncByteStream):
    def __init__(self, *, read_error=False, close_error=False):
        self.read_error, self.close_error = read_error, close_error
        self.reading, self.closing, self.release = (asyncio.Event() for _ in range(3))
        self.closes = 0
        self.closed = False

    async def __aiter__(self):
        self.reading.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if self.read_error:
                raise httpx.ReadError("read ended during cancellation") from None
            raise
        yield b""  # async byte iterator, no output before cancellation

    async def aclose(self):
        self.closes += 1
        self.closing.set()
        await self.release.wait()
        self.closed = True
        if self.close_error:
            raise httpx.ReadError("transport reported an error after physical close")


class TrueStreamSetupCancellation(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings.update(upstream_max_concurrency=1, first_reply_timeout_seconds=0, retry_max=0)
        self.calls = []
        self.chains = []
        self.source = None
        self.status = 200

        def chain(_):
            value = ProxyChain(candidates=OUTLETS)
            self.chains.append(value)
            return value

        def serve(request):
            self.assertIn(":streamGenerateContent", request.url.path)
            self.calls.append(request)
            if len(self.calls) == 1:
                return httpx.Response(self.status, stream=self.source, headers={
                    "content-type": "text/event-stream" if self.status == 200 else "application/json"})
            return httpx.Response(200, content=SUCCESS, headers={"content-type": "text/event-stream"})

        options = get_http_options(base_url="https://offline.invalid", proxy_url="")
        options.async_client_args = dict(options.async_client_args, transport=httpx.MockTransport(serve))
        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=options)
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        # Both distinct outlet choices return the fixture, never a live client.
        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        self.enterContext(patch.object(express_sdk, "ProxyChain", side_effect=chain))

    @staticmethod
    def peer(spec="2.4"):
        return Peer("/v1/chat/completions", spec=spec, headers=[(b"x-resumable", b"false")], payload={
            "model": MODEL, "messages": [{"role": "user", "content": "hello"}], "stream": True})

    async def cancelled(self, *, server, seconds, read_error=False, close_error=False, spec="2.4"):
        self.settings["first_reply_timeout_seconds"] = seconds
        self.calls.clear()
        self.chains.clear()
        self.source = source = BeforeFirstOutput(read_error=read_error, close_error=close_error)
        peer = self.peer(spec)
        sender = asyncio.create_task(peer.run())
        try:
            await asyncio.wait_for(source.reading.wait(), 2)
            if server:
                sender.cancel("shutdown during SDK startup")
            else:
                await peer.drop()
            await asyncio.wait_for(source.closing.wait(), 2)
            self.assertFalse(sender.done() or source.closed)
            self.assertEqual((len(self.calls), generation_limiter.active, source.closes), (1, 1, 1))
            source.release.set()
            if server:
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(asyncio.shield(sender), 2)
            else:
                await asyncio.wait_for(asyncio.shield(sender), 2)
            self.assertTrue(source.closed)
            self.assertEqual((len(self.calls), self.chains[0].index, source.closes, generation_limiter.active),
                             (1, 0, 1, 0))
            wire = b"".join(msg.get("body", b"") for msg in peer.messages)
            self.assertNotIn(b"second outlet", wire)
            self.assertNotIn(b"[DONE]", wire)
        finally:
            source.release.set()
            if not sender.done():
                sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    async def test_cancelled_preoutput_read_cannot_advance_outlets_or_count_an_unmade_retry(self):
        for seconds in (0, 3):
            for server in (False, True):
                for read, close in ((False, True), (True, False), (True, True), (False, False)):
                    with self.subTest(server=server, timeout=seconds, read_error=read, close_error=close):
                        await self.cancelled(server=server, seconds=seconds, read_error=read, close_error=close)
        for server in (False, True):
            with self.subTest(server=server, asgi="2.3"):
                await self.cancelled(server=server, seconds=0, close_error=True, spec="2.3")

    async def test_regular_http_503_advances_only_after_the_first_transport_finishes(self):
        self.status = 503
        class Unavailable(BeforeFirstOutput):
            async def __aiter__(self):
                yield b'{"error":{"code":503,"message":"not ready"}}'

        self.source = source = Unavailable()
        peer = self.peer()
        sender = asyncio.create_task(peer.run())
        try:
            await asyncio.wait_for(source.closing.wait(), 2)
            self.assertEqual((len(self.calls), self.chains[0].index, generation_limiter.active), (1, 0, 1))
            source.release.set()
            await asyncio.wait_for(asyncio.shield(sender), 2)
            self.assertTrue(source.closed)
            self.assertEqual((len(self.calls), self.chains[0].index, generation_limiter.active, source.closes),
                             (2, 1, 0, 1))
            wire = b"".join(m.get("body", b"") for m in peer.messages)
            self.assertIn(b"second outlet", wire)
            self.assertIn(b"[DONE]", wire)
        finally:
            source.release.set()
            await asyncio.gather(sender, return_exceptions=True)

    async def test_cancel_swallowed_into_usage_does_not_pull_another_private_chunk(self):
        class UsageOnCancel(BeforeFirstOutput):
            extra_pull = False

            async def __aiter__(self):
                self.reading.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    pass  # adapter returns a final piece of buffered metadata
                yield b'data: {"usageMetadata":{"promptTokenCount":1}}\n\n'
                self.extra_pull = True
                yield SUCCESS

        self.source = source = UsageOnCancel()
        peer = self.peer()
        sender = asyncio.create_task(peer.run())
        try:
            await asyncio.wait_for(source.reading.wait(), 2)
            sender.cancel()
            await asyncio.wait_for(source.closing.wait(), 2)
            self.assertFalse(source.extra_pull)
            self.assertEqual((len(self.calls), generation_limiter.active), (1, 1))
            source.release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(sender), 2)
            self.assertEqual((len(self.calls), self.chains[0].index, generation_limiter.active, source.closes),
                             (1, 0, 0, 1))
        finally:
            source.release.set()
            await asyncio.gather(sender, return_exceptions=True)

    async def test_application_deadline_keeps_504_without_dispatching_a_second_model_call(self):
        for first_reply in (False, True):
            with self.subTest(first_reply=first_reply):
                self.calls.clear()
                self.chains.clear()
                self.settings.update(request_timeout_seconds=10 if first_reply else .25,
                                     first_reply_timeout_seconds=.25 if first_reply else 0)
                self.source = source = BeforeFirstOutput(read_error=True, close_error=True)
                peer = self.peer()
                sender = asyncio.create_task(peer.run())
                try:
                    await asyncio.wait_for(source.closing.wait(), 3)
                    self.assertTrue(source.reading.is_set())
                    self.assertEqual((len(self.calls), generation_limiter.active), (1, 1))
                    source.release.set()
                    await asyncio.wait_for(asyncio.shield(sender), 2)
                    wire = b"".join(m.get("body", b"") for m in peer.messages)
                    events = [line[6:] for line in wire.splitlines() if line.startswith(b"data: ")]
                    self.assertEqual((json.loads(events[-2])["error"]["code"], events[-1]), (504, b"[DONE]"))
                    self.assertNotIn(b"second outlet", wire)
                    self.assertEqual((len(self.calls), self.chains[0].index, generation_limiter.active,
                                      source.closes, source.closed), (1, 0, 0, 1, True))
                finally:
                    source.release.set()
                    if not sender.done():
                        sender.cancel()
                    await asyncio.gather(sender, return_exceptions=True)
