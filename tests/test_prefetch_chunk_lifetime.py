"""Release sent prefix items independently of the consumer or physical close."""
import asyncio
import weakref
from unittest import IsolatedAsyncioTestCase

from first_reply import PrefetchedStream


class Chunk(bytearray):
    pass


class ControlledSource:
    def __init__(self):
        self.pulls = self.closes = 0
        self.closing = asyncio.Event()
        self.release = asyncio.Event()

    async def __anext__(self):
        self.pulls += 1
        return b"tail"

    async def aclose(self):
        self.closes += 1
        self.closing.set()
        await self.release.wait()


class PrefixLifetime(IsolatedAsyncioTestCase):
    async def test_drained_entries_do_not_pin_prefix_and_preserve_source_pull_order(self):
        source = ControlledSource()
        blocks = [Chunk(bytes([v])) for v in range(8)]
        refs = [weakref.ref(b) for b in blocks]
        stream = PrefetchedStream(blocks, source)
        del blocks
        for i, ref in enumerate(refs):
            self.assertEqual(await anext(stream), bytes([i]))
            self.assertIsNone(ref())
            self.assertTrue(all(other() is not None for other in refs[i + 1:]))
            self.assertEqual(source.pulls, 0)
        self.assertEqual(await anext(stream), b"tail")
        self.assertEqual(source.pulls, 1)
        source.release.set()
        await stream.aclose()
        self.assertEqual(source.closes, 1)

    async def test_requested_close_releases_even_unsent_blocks_before_physical_close(self):
        source = ControlledSource()
        blocks = [Chunk(b"sent"), Chunk(b"unsent")]
        refs = [weakref.ref(b) for b in blocks]
        stream = PrefetchedStream(blocks, source)
        del blocks
        self.assertEqual(await anext(stream), b"sent")
        close = asyncio.create_task(stream.aclose())
        await source.closing.wait()
        try:
            self.assertEqual([r() for r in refs], [None, None])
            self.assertEqual((close.done(), source.pulls, source.closes), (False, 0, 1))
        finally:
            source.release.set()
            await close
