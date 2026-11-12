"""Log bursts stay bounded before and after reaching the asyncio queue."""

import asyncio
import threading
import unittest

from log_stream import SSELogger


class DeferredLoop:
    def __init__(self):
        self.callbacks = []
        self.closed = False
        self.fail = False

    def is_closed(self):
        return self.closed

    def call_soon_threadsafe(self, callback, *args):
        if self.fail:
            raise RuntimeError("loop stopped")
        self.callbacks.append((callback, args))

    def flush(self):
        callbacks, self.callbacks = self.callbacks, []
        for callback, args in callbacks:
            callback(*args)


def sequences(queue):
    result = []
    while not queue.empty():
        result.append(queue.get_nowait()[0])
    return result


class LogBackpressureTests(unittest.TestCase):
    def test_burst_schedules_one_callback_and_keeps_latest_messages(self):
        loop = DeferredLoop()
        logger = SSELogger(max_history=3, queue_size=8)
        queue = logger.subscribe(loop)
        for index in range(2000):
            logger.push(f"message {index}")
        self.assertEqual(len(loop.callbacks), 1)
        self.assertEqual(queue.qsize(), 0)
        loop.flush()
        self.assertEqual(sequences(queue), list(range(1993, 2001)))
        self.assertEqual([seq for seq, _ in logger.snapshot_history()], [1998, 1999, 2000])

    def test_slow_subscriber_does_not_block_other_subscribers(self):
        fast, slow = DeferredLoop(), DeferredLoop()
        logger = SSELogger(queue_size=2)
        fast_queue, slow_queue = logger.subscribe(fast), logger.subscribe(slow)
        for index in range(6):
            logger.push(str(index))
            fast.flush()
            self.assertEqual(sequences(fast_queue), [index + 1])
        self.assertEqual(len(slow.callbacks), 1)
        slow.flush()
        self.assertEqual(sequences(slow_queue), [5, 6])

    def test_unsubscribe_discards_a_scheduled_batch(self):
        loop = DeferredLoop()
        logger = SSELogger()
        queue = logger.subscribe(loop)
        logger.push("before unsubscribe")
        logger.unsubscribe(queue)
        loop.flush()
        self.assertTrue(queue.empty())
        logger.push("after unsubscribe")
        self.assertFalse(loop.callbacks)

    def test_stopped_loop_does_not_prevent_later_delivery(self):
        loop = DeferredLoop()
        logger = SSELogger(queue_size=1)
        queue = logger.subscribe(loop)
        loop.closed = True
        logger.push("closed")
        loop.closed, loop.fail = False, True
        logger.push("stopped while scheduling")
        loop.fail = False
        logger.push("recovered")
        loop.flush()
        self.assertEqual(sequences(queue), [3])

    def test_multiple_batches_replace_old_queue_items_in_order(self):
        loop = DeferredLoop()
        logger = SSELogger(queue_size=2)
        queue = logger.subscribe(loop)
        for index in range(8):
            logger.push(str(index))
            loop.flush()
        self.assertEqual(sequences(queue), [7, 8])


class ThreadedLogTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_threads_preserve_sequence_on_a_live_loop(self):
        logger = SSELogger(queue_size=1000)
        queue = logger.subscribe()
        threads = [threading.Thread(target=lambda: [logger.push("worker") for _ in range(100)])
                   for _ in range(4)]
        for thread in threads:
            thread.start()
        await asyncio.to_thread(lambda: [thread.join(1) for thread in threads])
        await asyncio.sleep(0)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(sequences(queue), list(range(1, 401)))
