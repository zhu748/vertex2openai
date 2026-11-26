"""The SSE wire framer and request observer must agree at arbitrary chunk boundaries."""
import asyncio
import codecs
import itertools
import unittest
from unittest import mock

import request_tracking as tracking
from response_validation import IncompleteResponseError
from streaming import sse_frames


async def chunks(parts):
    for part in parts:
        yield part


# CR followed directly by LF is *one* CRLF, not a pair of logical endings.
ENDINGS = tuple(a + b for a, b in itertools.product((b"\n", b"\r", b"\r\n"), repeat=2)
                if (a, b) != (b"\r", b"\n"))


class WireFramingTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_mixed_endings_and_split_points_preserve_raw_events(self):
        for sep in ENDINGS:
            first = b": note\r\nevent: message\ndata: A" + sep
            last = b"data: B" + sep
            wire = first + last
            parts = [[wire[i:i + 1] for i in range(len(wire))]]
            parts.extend((wire[:i], wire[i:]) for i in range(len(wire) + 1))
            for cut, fragments in enumerate(parts):
                with self.subTest(separator=sep, cut=cut):
                    self.assertEqual([f async for f in sse_frames(chunks(fragments))], [first, last])

    async def test_complete_immutable_frame_is_transferred_without_a_staging_copy(self):
        for sep in (b"\n\n", b"\r\n\r\n", b"\n\r\n", b"\r\r\n"):
            frame = b"data: " + (b"x" * 256000) + sep
            with self.subTest(end=sep):
                source = chunks([b"", frame, b"data: next\n\n"])
                events = sse_frames(source, len(frame))
                self.assertIs(await anext(events), frame)
                self.assertEqual(await anext(events), b"data: next\n\n")
                await events.aclose()
                with self.assertRaises(IncompleteResponseError):
                    _ = [item async for item in sse_frames(chunks([frame]), len(frame)-1)]

    async def test_mutable_or_packed_frames_are_not_borrowed(self):
        for wrap in (bytearray, memoryview):
            raw = bytearray(b"data: original\n\n")
            events = sse_frames(chunks([wrap(raw)]))
            first = await anext(events)
            raw[:4] = b"oops"
            self.assertEqual(first, b"data: original\n\n")
            self.assertIs(type(first), bytes)
            await events.aclose()
        events = sse_frames(chunks([b"data: first\n\ndata: last\n", b"\n"]))
        self.assertEqual([item async for item in events], [b"data: first\n\n", b"data: last\n\n"])

    async def test_only_one_stream_initial_utf8_bom_is_ignored(self):
        bom, frame = codecs.BOM_UTF8, b"data: A\n\n"
        wire = bom + bom + frame + bom + frame
        for i in range(5):
            events = sse_frames(chunks([b"", wire[:i], wire[i:]]), max_frame_bytes=len(bom + frame))
            self.assertEqual([item async for item in events], [bom + frame, bom + frame])

    async def test_large_frame_slices_release_view_before_buffer_compaction(self):
        # A partial/multiple-frame chunk exercises the slice path rather than
        # bytes(buffer). Holding an exported view when compacting raises BufferError.
        large = b"data: " + b"z" * (64 * 1024) + b"\n\n"
        small = b"data: b\n\n"
        parts = [small + large + b"dat", b"a: tail\n\n"]
        events = sse_frames(chunks(parts))
        self.assertEqual(await anext(events), small)
        saved = await anext(events)
        self.assertIs(type(saved), bytes)
        self.assertEqual(await anext(events), b"data: tail\n\n")
        with self.assertRaises(StopAsyncIteration):
            await anext(events)
        self.assertEqual(saved, large)

    async def test_single_crlf_is_not_a_blank_line(self):
        with self.assertRaises(IncompleteResponseError):
            _ = [f async for f in sse_frames(chunks([b"data: unfinished\r", b"\n"]))]

    async def test_trailing_cr_waits_for_optional_lf_then_delivers_without_extra_read(self):
        requested_lf = asyncio.Event()
        release_lf = asyncio.Event()
        keep_open = asyncio.Event()
        closed = asyncio.Event()

        async def source():
            try:
                yield b"data: partial\n\r"
                requested_lf.set()
                await release_lf.wait()
                yield b"\n"
                await keep_open.wait()
            finally:
                closed.set()

        events = sse_frames(source())
        first = asyncio.create_task(anext(events))
        try:
            await asyncio.wait_for(requested_lf.wait(), 1)
            self.assertFalse(first.done())
            release_lf.set()
            self.assertEqual(await asyncio.wait_for(first, 1), b"data: partial\n\r\n")
        finally:
            release_lf.set()
            if not first.done():
                first.cancel()
                await asyncio.gather(first, return_exceptions=True)
            await events.aclose()
        self.assertTrue(closed.is_set())

    async def test_limit_includes_all_separator_bytes_even_across_chunks_and_eof(self):
        frame = b"1234\n\r"
        exact = [f async for f in sse_frames(chunks([frame, b""]), len(frame))]
        self.assertEqual(exact, [frame])
        for parts in ([frame], [frame[:4], frame[4:]], [frame, b"\n"]):
            with self.subTest(parts=parts), self.assertRaises(IncompleteResponseError):
                _ = [f async for f in sse_frames(chunks(parts), len(b"".join(parts)) - 1)]


class ReplayDispatchTests(unittest.TestCase):
    def test_data_bearing_dispatch_owns_the_only_server_cursor(self):
        from replay_event import ReplayEvent

        for newline in ("\r", "\n", "\r\n"):
            with self.subTest(newline=newline):
                raw = newline.join(["id: stale-before", "", ": wait", "", "", "event: issue",
                                    'data: {"error":', 'data: {"code":503}}', "id: stale-in",
                                    "", ": tail", "id: stale-after", "", ""])
                event = ReplayEvent(raw)
                self.assertEqual(event.error(), {"code": 503})
                self.assertEqual(event.with_id("rid:0"), newline.join([
                    "", ": wait", "", "", "id: rid:0\nevent: issue", 'data: {"error":',
                    'data: {"code":503}}', "", ": tail", "", ""]))

    def test_two_data_dispatches_in_one_item_never_become_a_joined_payload(self):
        from replay_event import ReplayEvent

        for first in ("data", "data:", 'data: {"error":'):
            with self.subTest(first=first), self.assertRaisesRegex(ValueError, "多个 SSE 事件"):
                ReplayEvent(first + '\n\r\n: intermediate\r\n\rdata: {"code":503}}\n\n')


class ObserverFramingTests(unittest.TestCase):
    def test_bytewise_events_match_wire_including_bare_cr_at_eof(self):
        first = b'data: {"choices":[{"delta":{"content":"ab"}}]}'
        last = b'data: {"choices":[{"delta":{"content":"c"}}]}'
        for sep in ENDINGS:
            with self.subTest(separator=sep):
                trace = tracking.RequestTrace("/v1/chat/completions")
                observer = tracking.PayloadObserver(trace)
                wire = first + sep + last + sep
                for i in range(len(wire)):
                    observer.feed(wire[i:i + 1])
                observer.feed(b"", final=True)
                self.assertEqual(trace.data["generated_frames"], 2)
                self.assertEqual(trace.data["content_chars"], 3)
                self.assertEqual(trace.data["generated_bytes"], len(wire))

    def test_oversized_mixed_event_does_not_swallow_following_data(self):
        for sep in ENDINGS:
            with self.subTest(separator=sep):
                trace = tracking.RequestTrace("/v1/messages")
                observer = tracking.PayloadObserver(trace)
                oversized = b"data: " + b"a" * 1024 + sep
                ok = b'data: {"choices":[{"delta":{"content":"ok"}}]}' + sep
                with mock.patch.object(tracking, "MAX_INSPECTION_BYTES", len(ok)):
                    for part in (oversized[:-2], oversized[-2:-1], oversized[-1:], ok):
                        observer.feed(part)
                    observer.feed(b"", final=True)
                self.assertEqual(trace.data["generated_frames"], 1)
                self.assertEqual(trace.data["inspection_skipped_bytes"], len(oversized))
                self.assertEqual(trace.data["content_chars"], 2)
