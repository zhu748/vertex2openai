"""A Cookie response owns its HTTPX physical close until all leases can release."""
import asyncio
import unittest
from unittest.mock import patch

from fastapi.responses import Response
import httpx

import admission
from http_io import BoundedAsyncClient


class SlowClose(httpx.AsyncByteStream):
    def __init__(self):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = []

    async def __aiter__(self):
        yield b'{"ok":true}'

    async def aclose(self):
        self.calls.append("start")
        self.entered.set()
        await self.release.wait()
        self.calls.append("done")


class CookieOwnership(unittest.IsolatedAsyncioTestCase):
    async def test_buffered_eof(self):
        await self.check("post")

    async def test_streaming_eof(self):
        await self.check("stream")

    async def test_exiting_context_joins_another_readers_eof(self):
        await self.check("concurrent eof")

    async def check(self, mode):
        source = SlowClose()
        limiter = admission.GenerationLimiter(default_limit=1)
        acquired, leave_context = asyncio.Event(), asyncio.Event()
        reader = None
        transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=source))
        async with BoundedAsyncClient(transport=transport) as client:
            @admission.limit_generation
            async def consume():
                nonlocal reader
                if mode == "post":
                    await client.post("https://fixture.invalid")
                else:
                    async with client.stream("POST", "https://fixture.invalid") as response:
                        if mode == "stream":
                            await response.aread()
                        else:
                            # A prefetch task may be at EOF as its context owner exits.
                            reader = asyncio.create_task(response.aread())
                            acquired.set()
                            await leave_context.wait()
                return Response()

            with patch.object(admission, "generation_limiter", limiter):
                task = asyncio.create_task(consume())
                try:
                    await asyncio.wait_for(source.entered.wait(), 1)
                    if mode == "concurrent eof":
                        assert acquired.is_set()
                        leave_context.set()
                        await asyncio.sleep(0)
                    task.cancel()
                    await asyncio.sleep(0)
                    task.cancel()
                    done, _ = await asyncio.wait({task}, timeout=.01)
                    assert not done and limiter.active == 1
                finally:
                    source.release.set()
                    result, = await asyncio.gather(task, return_exceptions=True)
                    if reader is not None:
                        await reader
        assert isinstance(result, asyncio.CancelledError)
        assert source.calls == ["start", "done"] and limiter.active == 0
