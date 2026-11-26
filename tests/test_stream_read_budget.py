"""One absolute budget per upstream pull, independent of the consuming task."""
import asyncio
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import httpx

import streaming
from upstreams.native_passthrough import _relay_stream


class StreamReadBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.now = 100.0
        # Advance only the application's clock; the asyncio timer and test
        # runner still use real time. No sleeps on a shared event loop.
        self.enterContext(patch.object(streaming, "time", SimpleNamespace(monotonic=lambda: self.now)))

    async def test_late_read_value_eof_and_error_have_the_same_budget(self):
        for kind in ("data", "eof", "error", "upstream_timeout"):
            with self.subTest(kind=kind):
                self.now = 100.0
                async def source():
                    self.now += 2  # Late without yielding to the timer callback.
                    if kind == "data":
                        yield b"late"
                    elif kind == "error":
                        raise ValueError("late adapter error")
                    elif kind == "upstream_timeout":
                        raise TimeoutError("late upstream timeout")
                iterator = source()
                bounded = streaming.reads_before_deadline(iterator, 101.0)
                try:
                    with self.assertRaises(streaming.StreamDeadlineExceeded):
                        await anext(bounded)
                finally:
                    await bounded.aclose()
                    await iterator.aclose()

    async def test_external_cancel_beats_translated_bytes_eof_errors_and_elapsed_time(self):
        for kind in ("data", "eof", "error", "upstream_timeout"):
            for late in (False, True):
                with self.subTest(kind=kind, late=late):
                    self.now = 100.0
                    reading = asyncio.Event()
                    async def source():
                        reading.set()
                        try:
                            await asyncio.Event().wait()
                        except asyncio.CancelledError:
                            if late:
                                self.now += 2
                            if kind == "data":
                                yield b"discard cancelled result"
                            elif kind == "error":
                                raise ValueError("adapter translated cancel") from None
                            elif kind == "upstream_timeout":
                                raise TimeoutError("adapter translated cancel") from None
                    iterator = source()
                    bounded = streaming.reads_before_deadline(iterator, 101.0)
                    job = asyncio.create_task(anext(bounded))
                    try:
                        await asyncio.wait_for(reading.wait(), 1)
                        job.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await job
                    finally:
                        await bounded.aclose()
                        await iterator.aclose()

    async def test_early_upstream_error_and_timeout_keep_their_identity(self):
        for error in (RuntimeError("upstream failed"), TimeoutError("upstream-specific budget")):
            with self.subTest(error=type(error)):
                async def source():
                    raise error
                    yield
                with self.assertRaises(type(error)) as caught:
                    await anext(streaming.reads_before_deadline(source(), 101.0))
                self.assertIs(caught.exception, error)
        async def empty():
            return
            yield
        self.assertEqual([x async for x in streaming.reads_before_deadline(empty(), 101.0)], [])

    async def test_native_array_late_eof_after_delivered_bytes_is_not_success(self):
        finished, closed = [], []
        first = b'[{"candidates":[{"content":{"parts":[{"text":"hi"}]},"finishReason":"STOP"}]}]'
        owner = self
        class Bytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield first
                finished.append(True)
                owner.now += 2
            async def aclose(self):
                closed.append(True)
        upstream = httpx.Response(200, headers={"content-type": "application/json"}, stream=Bytes())
        response = await _relay_stream(upstream, deadline=101.0)
        try:
            self.assertEqual(await anext(response.body_iterator), first)
            with self.assertRaises(streaming.StreamDeadlineExceeded):
                await anext(response.body_iterator)
        finally:
            await response.aclose()
        self.assertEqual((finished, closed), ([True], [True]))

    async def test_native_sse_delivers_the_prefix_then_one_precise_error(self):
        head = b'data: {"candidates":[{"content":{"parts":[{"text":"hi"}]}}]}\n\n'
        for late in (False, True):
            with self.subTest(late=late):
                self.now = 100.0
                closed = []
                owner = self
                class Bytes(httpx.AsyncByteStream):
                    async def __aiter__(self):
                        yield head
                        if late:
                            owner.now += 2
                        raise httpx.ReadError("transport ended")
                    async def aclose(self):
                        closed.append(True)
                response = await _relay_stream(httpx.Response(200, stream=Bytes(),
                    headers={"content-type": "text/event-stream"}), keepalive=True, deadline=101.0)
                try:
                    frames = [f async for f in response.body_iterator if f.startswith(b"data:")]
                finally:
                    await response.aclose()
                first, terminal = frames
                self.assertEqual(first, head)
                self.assertEqual(json.loads(terminal[5:])["error"]["code"], 504 if late else 502)
                self.assertEqual(closed, [True])
