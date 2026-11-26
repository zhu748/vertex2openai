"""Shared framing regression tests through both live parser entry points (offline)."""
import json
import tracemalloc
import unittest

from response_validation import IncompleteResponseError
from upstreams import cookie_response, native_passthrough


class Fragments:
    def __init__(self, wire, step):
        self.wire = wire
        self.step = step

    async def aiter_bytes(self):
        for pos in range(0, len(self.wire), self.step):
            yield self.wire[pos:pos + self.step]

    async def aiter_text(self):
        async for part in self.aiter_bytes():
            yield part.decode("ascii")


class FragmentedJSONTests(unittest.IsolatedAsyncioTestCase):
    async def test_cookie_and_native_do_not_keep_three_copies_of_a_fragmented_object(self):
        size = 2 * 1024 * 1024
        events = [{"candidates": [{"content": {"parts": [{"text": "A" * size}]}}]},
                  {"candidates": [{"finishReason": "STOP"}]}]
        wire = json.dumps(events).encode("ascii")
        for native in (False, True):
            with self.subTest(native=native):
                source = Fragments(wire, 16 * 1024)
                tracemalloc.start()
                try:
                    if native:
                        offset = 0
                        async for part in native_passthrough._native_json_chunks(source):
                            self.assertTrue(wire.startswith(part, offset))
                            offset += len(part)
                        self.assertEqual(offset, len(wire))
                    else:
                        count = 0
                        async for obj in cookie_response.iter_json_objects(source):
                            self.assertEqual(obj, events[count])
                            # At yield the parsed value, not the joined JSON, should remain.
                            self.assertLess(tracemalloc.get_traced_memory()[0], size * 1.5)
                            count += 1
                        self.assertEqual(count, len(events))
                    _, peak = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
                # Parse needs the joined string + output, not the input fragments too.
                self.assertLess(peak, size * 2.7)

    async def test_document_terminators_and_invalid_second_object_still_fail(self):
        completed = b'{"candidates":[{"finishReason":"STOP"}]}'
        for tail in (b',{"x":nope}]', b',{"x":', b',]', b'] extra'):
            wire = b'[' + completed + tail
            with self.subTest(tail=tail), self.assertRaises(IncompleteResponseError) as caught:
                async for _ in native_passthrough._native_json_chunks(Fragments(wire, 7)):
                    pass
            self.assertEqual(caught.exception.code, 502)
