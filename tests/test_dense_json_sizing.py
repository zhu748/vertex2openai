import asyncio
import json
import threading
from unittest import IsolatedAsyncioTestCase, mock

import json_encoding as encoding


def logprobs(n):
    return {"tokens": [{"text": '中文"🙂', "bytes": [117, 118, 119], "prob": -0.2,
                        "alternatives": [{"text": "ok", "prob": -0.4}]} for _ in range(n)]}


class DenseSizing(IsolatedAsyncioTestCase):
    async def test_dense_job_yields_to_ready_loop_work_and_uses_exact_canonical_size(self):
        payload = logprobs(1400)
        expected = len(json.dumps(payload, ensure_ascii=False).encode())
        entered, release = threading.Event(), threading.Event()
        loop_thread = threading.get_ident()
        exact = encoding.encoded_json_size

        def measure(value):
            self.assertNotEqual(threading.get_ident(), loop_thread)
            entered.set()
            if not release.wait(2):
                raise AssertionError("event loop blocked")
            return exact(value)

        with mock.patch.object(encoding, "encoded_json_size", measure):
            task = asyncio.create_task(encoding.json_size_offloop(payload))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            finally:
                release.set()
            self.assertEqual(await task, expected)

    async def test_cancelled_worker_keeps_reservation_and_saturated_count_stays_cooperative(self):
        one = threading.BoundedSemaphore(1)
        entered, release = threading.Event(), threading.Event()
        actual = encoding.encoded_json_size
        payload = logprobs(500)

        def measure(value):
            entered.set()
            if not release.wait(2):
                raise AssertionError("worker release timed out")
            return actual(value)

        with mock.patch.object(encoding, "_SIZE_WORKERS", one), \
             mock.patch.object(encoding, "encoded_json_size", side_effect=measure) as worker:
            waiting = asyncio.create_task(encoding.json_size_offloop(payload))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                waiting.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await waiting
                next_job = asyncio.create_task(encoding.json_size_offloop(payload))
                await asyncio.sleep(0)
                self.assertFalse(next_job.done())
                self.assertEqual(await next_job, len(json.dumps(payload, ensure_ascii=False).encode()))
                self.assertEqual(worker.call_count, 1)
            finally:
                release.set()
            self.assertTrue(await asyncio.to_thread(one.acquire, True, 1))
            one.release()

    def test_preflight_stops_at_wide_container_without_iterating_the_entire_tree(self):
        class Wide(list):
            def __iter__(self):
                raise AssertionError("eager scan of oversized container")

        self.assertFalse(encoding._small_size_job({"nested": Wide([0] * 300)}))
        self.assertTrue(encoding._small_size_job({"small": (True, 1.2, None)}))
