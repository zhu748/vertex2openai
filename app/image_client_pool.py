"""Thread-safe, bounded leases for synchronous HTTP image downloads."""

from contextlib import contextmanager, suppress
from dataclasses import dataclass
from threading import Condition

import httpx


@dataclass
class _Entry:
    client: httpx.Client
    users: int = 0
    retired: bool = False


class ImageClientPool:
    def __init__(self, capacity: int = 16):
        if capacity < 1:
            raise ValueError("Image client pool capacity must be positive")
        self.capacity = capacity
        self._entries = {}
        self._changed = Condition()

    def _close(self, key, entry):
        # Retain the slot until close finishes, but allow unrelated keys to proceed.
        try:
            with suppress(Exception):
                entry.client.close()
        finally:
            with self._changed:
                del self._entries[key]
                self._changed.notify_all()

    def reset(self):
        """Retire the cache, closing each client after its last lease ends."""
        idle = []
        with self._changed:
            for key, entry in self._entries.items():
                if not entry.retired:
                    entry.retired = True
                    if not entry.users:
                        idle.append((key, entry))
        for key, entry in idle:
            self._close(key, entry)

    def _acquire(self, key, factory, remaining):
        while True:
            # Recheck the request's own clock after contention, eviction or cancellation.
            budget = remaining()
            with self._changed:
                entry = self._entries.get(key)
                if entry is not None and not entry.retired:
                    entry.users += 1
                    return entry
                if entry is None and len(self._entries) < self.capacity:
                    entry = _Entry(factory(), users=1)
                    self._entries[key] = entry
                    return entry
                victim = next(((k, e) for k, e in self._entries.items()
                               if not e.users and not e.retired), None) if entry is None else None
                if victim is None:
                    self._changed.wait(min(0.25, budget))
                    continue
                victim[1].retired = True
            self._close(*victim)

    @contextmanager
    def lease(self, key, factory, remaining):
        entry = self._acquire(key, factory, remaining)
        try:
            yield entry.client
        finally:
            with self._changed:
                entry.users -= 1
                close = not entry.users and entry.retired
                if not entry.users and not close:
                    self._changed.notify_all()
            if close:
                self._close(key, entry)
