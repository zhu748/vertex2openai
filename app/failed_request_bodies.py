"""Opt-in, process-local copies of complete original failed request bodies."""
import asyncio
from collections import OrderedDict
from dataclasses import dataclass, field
import threading
import time


SETTING = "debug_failed_request_body"
MAX_BODY_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BYTES = 16 * 1024 * 1024
MAX_ENTRIES = 64
MAX_DOWNLOADS = 2
TTL_SECONDS = 15 * 60


@dataclass(repr=False)
class _Capture:
    expires: float
    data: bytearray | bytes = field(default_factory=bytearray)
    complete: bool = False


class FailedRequestBodies:
    def __init__(self, *, max_body=MAX_BODY_BYTES, max_total=MAX_TOTAL_BYTES,
                 max_entries=MAX_ENTRIES, ttl=TTL_SECONDS, clock=time.monotonic):
        self._max_body, self._max_total, self._max_entries = max_body, max_total, max_entries
        self._ttl, self._clock = ttl, clock
        self._lock = threading.Lock()
        self.download_slots = threading.BoundedSemaphore(MAX_DOWNLOADS)
        self._enabled = False
        self._used = 0
        self._pending: dict[str, _Capture] = {}
        self._failed: OrderedDict[str, _Capture] = OrderedDict()

    def set_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._enabled = enabled
            if not enabled:
                self._pending.clear()
                self._failed.clear()
                self._used = 0

    def _drop(self, entries, request_id: str) -> None:
        self._used -= len(entries.pop(request_id).data)

    def _prune(self, now: float) -> None:
        for entries in (self._pending, self._failed):
            for key in [key for key, capture in entries.items() if capture.expires <= now]:
                self._drop(entries, key)

    async def sweep(self) -> None:
        while True:
            await asyncio.sleep(min(self._ttl, 15))
            with self._lock:
                self._prune(self._clock())

    def begin(self, request_id: str, declared: str) -> bool:
        if not self._enabled:  # Disabled production hot path: no allocation/lock.
            return False
        try:
            if declared and int(declared) > self._max_body:
                return False
        except ValueError:
            pass  # A missing/invalid length is bounded from actual ASGI bytes.
        with self._lock:
            if not self._enabled:
                return False
            now = self._clock()
            self._prune(now)
            if len(self._pending) + len(self._failed) >= self._max_entries:
                if not self._failed:
                    return False
                self._drop(self._failed, next(iter(self._failed)))
            self._pending[request_id] = _Capture(now + self._ttl)
            return True

    async def receive(self, raw_receive, request_id: str):
        message = await raw_receive()
        if message["type"] == "http.request":
            self.feed(request_id, message.get("body", b""), final=not message.get("more_body", False))
        return message

    def feed(self, request_id: str, chunk: bytes, *, final: bool) -> None:
        with self._lock:
            capture = self._pending.get(request_id)
            if capture is None or capture.complete:
                return
            if (capture.expires <= self._clock()
                    or len(capture.data) + len(chunk) > min(self._max_body, self._max_total)):
                self._drop(self._pending, request_id)
                return
            while self._used + len(chunk) > self._max_total and self._failed:
                self._drop(self._failed, next(iter(self._failed)))
            if self._used + len(chunk) > self._max_total:
                self._drop(self._pending, request_id)
                return
            capture.data.extend(chunk)
            self._used += len(chunk)
            capture.complete = final

    def finish(self, request_id: str, *, failed: bool) -> None:
        with self._lock:
            capture = self._pending.get(request_id)
            if capture is None:
                return
            now = self._clock()
            if not failed or not capture.complete or capture.expires <= now:
                self._drop(self._pending, request_id)
                return
            self._pending.pop(request_id)
            capture.data = bytes(capture.data)
            capture.expires = now + self._ttl
            self._failed[request_id] = capture

    def _get(self, request_id: str) -> _Capture | None:
        capture = self._failed.get(request_id)
        if capture is not None and capture.expires <= self._clock():
            self._drop(self._failed, request_id)
            return None
        return capture

    def contains(self, request_id: str) -> bool:
        with self._lock:
            return self._get(request_id) is not None

    def get(self, request_id: str) -> bytes | None:
        with self._lock:
            capture = self._get(request_id)
            return capture.data if capture is not None else None
