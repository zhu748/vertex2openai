"""Check ownership as well as bytes on a quiet SSE connection (no network)."""
import asyncio
from types import SimpleNamespace
import unittest
import weakref

from streaming import HEARTBEAT, HEARTBEAT_FRAME, keepalive_sse, with_heartbeats


class Chunk(str):
    pass


class PausedSource:
    def __init__(self):
        self.started, self.resume = asyncio.Event(), asyncio.Event()
        self.reads = self.active = self.max_active = self.closed = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.reads += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.reads == 1:
                data = Chunk("data: " + "a" * (1 << 20) + "\n\n")
                self.first = weakref.ref(data)
                return data
            if self.reads == 2:
                self.started.set()
                await self.resume.wait()
                return "data: tail\n\n"
            raise StopAsyncIteration
        finally:
            self.active -= 1

    async def aclose(self):
        self.closed += 1


class HeartbeatChunkLifetimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_prior_result_released_on_next_pull_without_changing_delivery_or_close(self):
        for wrap, beat in ((with_heartbeats, HEARTBEAT), (keepalive_sse, HEARTBEAT_FRAME)):
            for ending in ("complete", "close", "disconnect", "cancel"):
                with self.subTest(wrapper=wrap.__name__, ending=ending):
                    source = PausedSource()
                    disconnected = False

                    async def probe():
                        return disconnected

                    request = SimpleNamespace(is_disconnected=probe) if ending == "disconnect" else None
                    events = wrap(source, request=request, interval=.01)
                    self.addAsyncCleanup(events.aclose)
                    if wrap is keepalive_sse:
                        self.assertEqual(await anext(events), HEARTBEAT_FRAME)
                    first = await asyncio.wait_for(anext(events), 2)
                    self.assertEqual(first, "data: " + "a" * (1 << 20) + "\n\n")
                    del first
                    next_frame = asyncio.create_task(anext(events))
                    try:
                        await asyncio.wait_for(source.started.wait(), 2)
                        # Neither the generator, the finished read task, nor its wait
                        # sets should own the delivered item across the next await.
                        self.assertFalse(source.first() is not None)
                        self.assertEqual(await asyncio.wait_for(next_frame, 2), beat)
                        self.assertEqual(await asyncio.wait_for(anext(events), 2), beat)
                        self.assertEqual((source.reads, source.active, source.max_active), (2, 1, 1))
                        if ending == "complete":
                            source.resume.set()
                            self.assertEqual(await asyncio.wait_for(anext(events), 2), "data: tail\n\n")
                            with self.assertRaises(StopAsyncIteration):
                                await anext(events)
                        elif ending == "disconnect":
                            disconnected = True
                            with self.assertRaises(StopAsyncIteration):
                                await asyncio.wait_for(anext(events), 2)
                        elif ending == "cancel":
                            next_frame = asyncio.create_task(anext(events))
                            await asyncio.sleep(0)  # enter the existing read before cancelling
                            next_frame.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await next_frame
                    finally:
                        if not next_frame.done():
                            next_frame.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await next_frame
                        await events.aclose()
                    self.assertEqual((source.active, source.max_active, source.closed), (0, 1, 1))
