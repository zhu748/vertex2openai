"""The stdout writer remains exclusively owned even when its sink stalls during shutdown."""
from collections import deque
import threading

import logger


def test_blocked_sink_and_full_queue_cannot_lose_the_only_writer(monkeypatch):
    logger.shutdown_stdout_writer()
    monkeypatch.setattr(logger, "_STDOUT_QUEUE", deque(maxlen=2))
    blocked, release = threading.Event(), threading.Event()
    writes = []
    real_print = logger.original_print

    def sink(text, *args, **kwargs):
        if threading.current_thread().name != "stdout-writer":
            return real_print(text, *args, **kwargs)
        if text == "in-flight":
            blocked.set()
            release.wait(5)
        writes.append((text, threading.current_thread()))

    monkeypatch.setattr(logger, "original_print", sink)
    try:
        logger._enqueue_stdout("in-flight", "\n", False)
        assert blocked.wait(2)
        first = logger._writer_thread
        logger._enqueue_stdout("queued-a", "\n", False)
        logger._enqueue_stdout("queued-b", "\n", False)
        assert len(logger._STDOUT_QUEUE) == logger._STDOUT_QUEUE.maxlen
        logger.shutdown_stdout_writer(timeout=.01)
        assert first.is_alive() and logger._writer_thread is first
        # While shutdown owns the blocked writer, new logs must not start a
        # competing consumer or evict entries which shutdown promised to drain.
        for n in range(3):
            logger._enqueue_stdout(f"late-{n}", "\n", False)
            logger.shutdown_stdout_writer(timeout=0)
            assert logger._writer_thread is first
        release.set()
        first.join(2)
        assert not first.is_alive()  # the original stop signal survived a full queue
        assert writes == [(s, first) for s in ("in-flight", "queued-a", "queued-b")]

        # A subsequent lifecycle can start normally and drain its own entries.
        logger._enqueue_stdout("new-a", "\n", False)
        second = logger._writer_thread
        logger._enqueue_stdout("new-b", "\n", False)
        logger.shutdown_stdout_writer(timeout=2)
        logger.shutdown_stdout_writer(timeout=0)
        assert second is not first and not second.is_alive()
        assert writes[-2:] == [("new-a", second), ("new-b", second)]
    finally:
        release.set()
        logger.shutdown_stdout_writer(timeout=2)
