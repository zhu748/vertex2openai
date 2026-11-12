"""Reply budgets use exact wire sizes without allocating another full response."""

import asyncio
import json
import threading
import tracemalloc
import unittest
from unittest import mock

import json_encoding
from resumable import ResumableBuffer


class JSONSizeTests(unittest.IsolatedAsyncioTestCase):
    async def test_sizes_match_utf8_and_escaping_in_the_existing_wire_format(self):
        for payload in ({"text": '中文😀\n\t"\\\x00' * 100},
                        {1: [None, True, 1.5], "nested": {"text": "x"}},
                        {"text": "x" * (2 * 1024 * 1024)}):
            with self.subTest(keys=list(payload)):
                self.assertEqual(await json_encoding.json_size_offloop(payload),
                                 len(json.dumps(payload, ensure_ascii=False).encode("utf-8")))

    async def test_large_accounting_runs_outside_the_event_loop(self):
        loop_thread = threading.get_ident()
        workers = []
        original = json_encoding.encoded_json_size

        def measure(value):
            workers.append(threading.get_ident())
            return original(value)

        with mock.patch.object(json_encoding, "encoded_json_size", side_effect=measure):
            await json_encoding.json_size_offloop({"body": "x" * (2 * 1024 * 1024)})
        self.assertEqual(len(workers), 1)
        self.assertNotEqual(workers[0], loop_thread)


class BufferAccountingTests(unittest.IsolatedAsyncioTestCase):
    async def test_exact_byte_boundary_is_enforced_for_multibyte_payloads(self):
        payload = {"text": "中文😀" * 100}
        size = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        for capacity in (size, size - 1):
            with self.subTest(capacity=capacity):
                buf = ResumableBuffer("test-budget", kind="json")
                buf.max_buffer_bytes = capacity
                charges = []
                buf._account_bytes = lambda count, terminal: charges.append(count) or True
                await buf.finish(payload=payload)
                if capacity == size:
                    self.assertIs(buf.payload, payload)
                    self.assertEqual(buf.bytes_stored, size)
                    self.assertEqual(charges, [size])
                else:
                    self.assertEqual(buf.status_code, 503)
                    self.assertEqual(buf.payload["error"]["type"], "buffer_capacity")
                    self.assertNotIn("text", buf.payload)

    async def test_large_buffer_accounting_does_not_materialize_a_full_json_copy(self):
        payload = {"choices": [{"message": {"content": "x" * (8 * 1024 * 1024)}}]}
        buf = ResumableBuffer("test-memory", kind="json")
        buf.max_buffer_bytes = 64 * 1024 * 1024
        tracemalloc.start()
        try:
            await buf.finish(payload=payload)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertIs(buf.payload, payload)
        self.assertLess(peak, 2 * 1024 * 1024)

    async def test_cancelled_accounting_cannot_overwrite_a_terminal_error(self):
        started, release = threading.Event(), threading.Event()

        def measure(value):
            started.set()
            release.wait(2)
            return 42

        buf = ResumableBuffer("test-cancel", kind="json")
        with mock.patch.object(json_encoding, "encoded_json_size", side_effect=measure):
            task = asyncio.create_task(buf.finish(payload={"text": "x" * (2 * 1024 * 1024)}))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                await asyncio.wait_for(buf.fail("cancelled", 503), 1)
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(buf.payload["error"]["message"], "cancelled")
        self.assertEqual(buf.status, "error")
