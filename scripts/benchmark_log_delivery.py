"""Measure a console log burst while the subscriber loop cannot drain callbacks."""

import argparse
import json
from pathlib import Path
import sys
import time
import tracemalloc

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from log_stream import SSELogger


class DeferredLoop:
    def __init__(self):
        self.callbacks = []

    def is_closed(self):
        return False

    def call_soon_threadsafe(self, callback, *args):
        self.callbacks.append((callback, args))


def measure(messages=20_000, queue_size=500):
    loop = DeferredLoop()
    logger = SSELogger(queue_size=queue_size)
    queue = logger.subscribe(loop)
    tracemalloc.start()
    try:
        started = time.perf_counter()
        for index in range(messages):
            logger.push(f"{index:06d} " + "x" * 1024)
        elapsed = time.perf_counter() - started
        retained, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    callback_count = len(loop.callbacks)
    for callback, args in loop.callbacks:
        callback(*args)
    delivered = []
    while not queue.empty():
        delivered.append(queue.get_nowait()[0])
    expected = list(range(max(1, messages - queue_size + 1), messages + 1))
    if delivered != expected:
        raise AssertionError("Log burst lost the newest messages or reordered sequences")
    return {"messages": messages, "queue_size": queue_size,
            "scheduled_callbacks": callback_count, "retained_bytes": retained,
            "peak_bytes": peak, "elapsed_seconds": round(elapsed, 4),
            "delivered_latest": len(delivered)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=20_000)
    parser.add_argument("--queue-size", type=int, default=500)
    args = parser.parse_args()
    if args.messages < 1 or args.queue_size < 1:
        parser.error("messages and queue-size must be positive")
    print(json.dumps(measure(args.messages, args.queue_size)))


if __name__ == "__main__":
    main()
