"""Independent preparation admission, including cancellation and submit failure."""

import asyncio
import threading
import unittest
from unittest import mock

from admission import GenerationCapacityError
import media_tasks


class PreparationIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_executor_only_rejects_its_own_workload(self):
        for heavy in (False, True):
            with self.subTest(heavy=heavy):
                slots = threading.BoundedSemaphore(1)
                slots.acquire()
                name = "_media_slots" if heavy else "_prep_slots"
                try:
                    with mock.patch.object(media_tasks, name, slots):
                        self.assertEqual(await media_tasks.run_media_task(lambda: 42, heavy=not heavy), 42)
                        with self.assertRaises(GenerationCapacityError):
                            await media_tasks.run_media_task(lambda: None, heavy=heavy)
                finally:
                    slots.release()

    async def test_cancelled_work_keeps_its_slot_until_the_thread_finishes(self):
        for heavy in (False, True):
            with self.subTest(heavy=heavy):
                started, release = threading.Event(), threading.Event()
                slots = threading.BoundedSemaphore(1)
                name = "_media_slots" if heavy else "_prep_slots"

                def work():
                    started.set()
                    release.wait(2)

                with mock.patch.object(media_tasks, name, slots):
                    task = asyncio.create_task(media_tasks.run_media_task(work, heavy=heavy))
                    try:
                        self.assertTrue(await asyncio.to_thread(started.wait, 1))
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                        with self.assertRaises(GenerationCapacityError):
                            await media_tasks.run_media_task(lambda: None, heavy=heavy)
                        self.assertEqual(await media_tasks.run_media_task(lambda: 7, heavy=not heavy), 7)
                    finally:
                        release.set()
                        await asyncio.gather(task, return_exceptions=True)
                    async with asyncio.timeout(1):
                        while not slots.acquire(blocking=False):
                            await asyncio.sleep(.001)
                    slots.release()

    async def test_failed_submission_returns_the_matching_slot(self):
        for heavy in (False, True):
            with self.subTest(heavy=heavy):
                slots = threading.BoundedSemaphore(1)
                slot_name = "_media_slots" if heavy else "_prep_slots"
                executor_name = "_media_executor" if heavy else "_prep_executor"
                executor = mock.Mock()
                executor.submit.side_effect = RuntimeError("executor stopped")
                with mock.patch.object(media_tasks, slot_name, slots), \
                     mock.patch.object(media_tasks, executor_name, executor):
                    with self.assertRaisesRegex(RuntimeError, "executor stopped"):
                        await media_tasks.run_media_task(lambda: None, heavy=heavy)
                self.assertTrue(slots.acquire(blocking=False))
                slots.release()
