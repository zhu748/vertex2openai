"""Bounded, cancellable preparation of prompts containing synchronous media IO."""

import asyncio
import contextvars
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from admission import GenerationCapacityError
from request_ingress import current_ingress


_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="media")
_slots = threading.BoundedSemaphore(32)
_budget = contextvars.ContextVar("media_budget", default=None)
PREPARATION_TIMEOUT_SECONDS = 30.0


def check_media_budget() -> float:
    """Called between blocking operations; remaining budget is shared by all images."""
    budget = _budget.get()
    if budget is None:
        return PREPARATION_TIMEOUT_SECONDS
    deadline, cancelled = budget
    if cancelled.is_set():
        raise TimeoutError("客户端已取消图片处理。")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("图片处理超过总超时时间。")
    return remaining


async def run_media_task(function, *args, **kwargs):
    # Hold admission until the real thread finishes, even after its awaiting
    # request is cancelled. ThreadPoolExecutor's otherwise unbounded queue can
    # therefore never accumulate abandoned requests without limit.
    if not _slots.acquire(blocking=False):
        raise GenerationCapacityError("图片/提示词处理并发已满，请稍后重试。")
    slots = _slots
    cancelled = threading.Event()
    token = _budget.set((time.monotonic() + PREPARATION_TIMEOUT_SECONDS, cancelled))
    context = contextvars.copy_context()
    _budget.reset(token)
    ingress = current_ingress.get()
    try:
        release_ingress = ingress.retain() if ingress is not None else lambda: None
    except BaseException:
        slots.release()
        raise

    def run():
        check_media_budget()
        result = function(*args, **kwargs)
        check_media_budget()
        return result

    try:
        future = _executor.submit(context.run, run)
    except BaseException:
        slots.release()
        release_ingress()
        raise
    future.add_done_callback(lambda _: slots.release())
    future.add_done_callback(lambda _: release_ingress())
    wrapped = asyncio.wrap_future(future)
    # A cancelled waiter must still retrieve any late worker exception.
    wrapped.add_done_callback(lambda done: None if done.cancelled() else done.exception())
    try:
        async with asyncio.timeout(PREPARATION_TIMEOUT_SECONDS):
            return await asyncio.shield(wrapped)
    finally:
        cancelled.set()
