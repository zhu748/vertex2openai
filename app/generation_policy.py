"""Pin per-generation budgets across detached tasks and adapter fallbacks."""
from contextlib import contextmanager
from contextvars import ContextVar

from runtime_state import app_state

RETRY_KEYS = ("retry_max", "retry_backoff_seconds", "retry_jitter_seconds", "retry_backoff_mode")
# Shared capacity, retention and disconnect grace are live policies, not part of
# an individual generation's contract. Read their latest durable value at use.
_REQUEST_KEYS = (*RETRY_KEYS, "request_timeout_seconds", "resumable_max_buffer_mb",
                 "resumable_enabled", "resumable_image_streams")
_current = ContextVar("generation_policy", default=None)


@contextmanager
def generation_settings_scope():
    token = _current.set((app_state, app_state.get_committed_settings(*_REQUEST_KEYS)))
    try:
        yield
    finally:
        _current.reset(token)


def generation_setting(state, key, default=None):
    saved = _current.get()
    if saved is not None and saved[0] is state and key in saved[1]:
        return saved[1][key]
    return state.get_committed_setting(key, default)


def retry_settings_snapshot(state):
    saved = _current.get()
    if saved is not None and saved[0] is state:
        return saved[1]
    return state.get_committed_settings(*RETRY_KEYS)
