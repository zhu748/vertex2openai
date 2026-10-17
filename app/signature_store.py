"""Short-lived fallback storage for Gemini thought-signature topology.

OpenAI-compatible responses carry signatures in ``extra_content.google``. This
store is deliberately only a fallback for clients that strip that extension. A
record has three possible states because ``None`` alone is ambiguous:

* ``SIGNED``: this exact function-call part carried a signature.
* ``UNSIGNED_FOLLOWER``: this was a later call in a parallel batch and must stay
  unsigned when replayed.
* ``UNKNOWN``: signature topology was lost; validation may need the documented
  skip sentinel for the first call of a step.
"""

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Optional

SKIP_VALIDATOR_SENTINEL = b"skip_thought_signature_validator"

DEFAULT_TTL_SECONDS = 2 * 60 * 60
DEFAULT_MAX_ENTRIES = 5000


class SignatureState(str, Enum):
    SIGNED = "signed"
    UNSIGNED_FOLLOWER = "unsigned_follower"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SignatureRecord:
    state: SignatureState
    signature: Optional[bytes] = None

    def __post_init__(self) -> None:
        if self.state is SignatureState.SIGNED and not self.signature:
            raise ValueError("signed signature records require signature bytes")
        if self.state is not SignatureState.SIGNED and self.signature is not None:
            raise ValueError("unsigned/unknown signature records cannot carry bytes")


class SignatureStore:
    """Thread-safe tool_call_id -> :class:`SignatureRecord` TTL/LRU cache."""

    def __init__(self, ttl_seconds: int = DEFAULT_TTL_SECONDS,
                 max_entries: int = DEFAULT_MAX_ENTRIES):
        self._ttl = ttl_seconds
        self._max = max_entries
        self._lock = threading.Lock()
        self._data: "OrderedDict[str, tuple[float, SignatureRecord]]" = OrderedDict()

    def put_record(self, call_id: str, record: SignatureRecord) -> None:
        if not call_id:
            return
        now = time.time()
        with self._lock:
            self._data[call_id] = (now, record)
            self._data.move_to_end(call_id)
            self._evict(now)

    def put_unsigned_follower(self, call_id: str) -> None:
        self.put_record(call_id, SignatureRecord(SignatureState.UNSIGNED_FOLLOWER))

    def put_unknown(self, call_id: str) -> None:
        self.put_record(call_id, SignatureRecord(SignatureState.UNKNOWN))

    def get_record(self, call_id: str) -> Optional[SignatureRecord]:
        if not call_id:
            return None
        now = time.time()
        with self._lock:
            item = self._data.get(call_id)
            if item is None:
                return None
            ts, record = item
            if now - ts > self._ttl:
                self._data.pop(call_id, None)
                return None
            self._data.move_to_end(call_id)
            return record

    def _evict(self, now: float) -> None:
        while self._data:
            oldest_key = next(iter(self._data))
            ts, _ = self._data[oldest_key]
            if now - ts > self._ttl:
                self._data.pop(oldest_key, None)
                continue
            break
        while len(self._data) > self._max:
            self._data.popitem(last=False)


signature_store = SignatureStore()
