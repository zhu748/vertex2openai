"""Bounded, ordered log delivery from worker threads to console subscribers."""

import asyncio
from collections import deque
from dataclasses import dataclass
import threading
import time

MAX_LOG_CHARS = 8 * 1024


@dataclass
class _Subscriber:
    queue: asyncio.Queue
    loop: asyncio.AbstractEventLoop
    pending: deque
    scheduled: bool = False


class SSELogger:
    """Keep history and both sides of the thread-to-event-loop handoff bounded.

    Each subscriber has at most one scheduled callback. A burst replaces the
    oldest pending messages before creating event-loop handles, so a busy loop
    cannot retain an unbounded number of log strings outside its bounded queue.
    """

    def __init__(self, max_history: int = 100, queue_size: int = 500):
        self.max_history = max_history
        self.queue_size = max(1, queue_size)
        self.history: deque = deque(maxlen=max_history)
        self._seq = 0
        self._subscribers: dict[asyncio.Queue, _Subscriber] = {}
        self._lock = threading.Lock()

    def subscribe(self, loop=None) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=self.queue_size)
        subscriber = _Subscriber(q, loop or asyncio.get_running_loop(),
                                 deque(maxlen=self.queue_size))
        with self._lock:
            self._subscribers[q] = subscriber
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        with self._lock:
            subscriber = self._subscribers.pop(q, None)
            if subscriber is not None:
                subscriber.pending.clear()

    def snapshot_history(self) -> list[tuple]:
        with self._lock:
            return list(self.history)

    @staticmethod
    def _offer(q: asyncio.Queue, msg: tuple) -> None:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            q.get_nowait()
            q.put_nowait(msg)

    def _flush(self, q: asyncio.Queue) -> None:
        # Only the subscriber's loop touches its asyncio.Queue. Publishers can
        # immediately collect the next bounded batch while this one drains.
        with self._lock:
            subscriber = self._subscribers.get(q)
            if subscriber is None:
                return
            batch = tuple(subscriber.pending)
            subscriber.pending.clear()
            subscriber.scheduled = False
        for msg in batch:
            self._offer(q, msg)

    def push(self, plain_text: str) -> None:
        if len(plain_text) > MAX_LOG_CHARS:
            plain_text = plain_text[:MAX_LOG_CHARS] + f"…（已截断，原长度 {len(plain_text)} 字符）"
        formatted = f"[{time.strftime('%H:%M:%S')}] {plain_text}"
        with self._lock:
            self._seq += 1
            item = (self._seq, formatted)
            self.history.append(item)
            for subscriber in self._subscribers.values():
                if subscriber.loop.is_closed():
                    subscriber.pending.clear()
                    continue
                subscriber.pending.append(item)
                if subscriber.scheduled:
                    continue
                subscriber.scheduled = True
                try:
                    subscriber.loop.call_soon_threadsafe(self._flush, subscriber.queue)
                except RuntimeError:
                    subscriber.pending.clear()
                    subscriber.scheduled = False
