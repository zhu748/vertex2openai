"""The native Cookie sender observes disconnect even between upstream reads."""
import asyncio
from contextlib import asynccontextmanager
import json
from unittest.mock import patch

import httpx

from admission import generation_limiter, image_limiter
from request_ingress import ingress_budget
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_native


class CookieSenderLifetimeTests(SchemaRouteHarness):
    async def exercise(self, spec, alt, *, cancel=False):
        self.channel = "web_proxy"
        sent, may_send, may_read, closing, may_close = (asyncio.Event() for _ in range(5))
        closes = []
        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                first = {"results": [{"data": {"candidates": [{"content": {"parts": [{"text": "first reply"}]}}]}}]}
                yield b"[" + json.dumps(first).encode() + b","
                await may_read.wait()
                yield b'{"results":[{"data":{"candidates":[{"finishReason":"STOP"}]}}]}]'

            async def aclose(self):
                closes.append("starting")
                closing.set()
                await may_close.wait()
                closes.append("closed")

        peer = Peer("/v1beta/models/gemini-3-pro-image-preview:streamGenerateContent",
                    query=("alt=" + alt).encode(), spec=spec,
                    payload={"contents": [{"parts": [{"text": "hello"}]}]}, headers=[(b"x-resumable", b"false")])
        async def blocked(message):
            if b"first reply" in message.get("body", b""):
                sent.set()
                await may_send.wait()

        initial = generation_limiter.active, image_limiter.active, tuple(ingress_budget.used)
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Body()))) as client:
            @asynccontextmanager
            async def open_client(*args, **kw):
                yield client

            with patch.object(cookie_native, "open_upstream_client", open_client):
                task = asyncio.create_task(peer.run(blocked))
                try:
                    await asyncio.wait_for(sent.wait(), 1)
                    await peer.drop()
                    await asyncio.wait_for(closing.wait(), 1)
                    self.assertEqual((generation_limiter.active, image_limiter.active),
                                     (initial[0] + 1, initial[1] + 1))
                    if cancel:
                        task.cancel()  # independent server/shutdown cancel during physical cleanup
                    may_close.set()
                    if cancel:
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(asyncio.shield(task), 1)
                    else:
                        await asyncio.wait_for(asyncio.shield(task), 1)
                finally:
                    may_close.set()
                    may_read.set()
                    may_send.set()
                    await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(closes, ["starting", "closed"])
        self.assertEqual((generation_limiter.active, image_limiter.active, tuple(ingress_budget.used)), initial)
        self.assertEqual(peer.reads, 2)
        self.assertFalse(peer.discarded)

    async def test_cookie_sse_and_array_cancel_blocked_send_and_hold_leases_until_physical_close(self):
        for spec in ("2.4", "2.3"):
            for alt in ("sse", "json"):
                with self.subTest(spec=spec, alt=alt):
                    await self.exercise(spec, alt)

    async def test_independent_server_cancel_survives_cookie_stream_disconnect_cleanup(self):
        for alt in ("sse", "json"):
            with self.subTest(alt=alt):
                await self.exercise("2.4", alt, cancel=True)
