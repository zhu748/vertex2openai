"""Bounded, cancellable preparation of prompts containing synchronous media IO."""

import asyncio
import contextvars
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from admission import GenerationCapacityError
from request_ingress import current_ingress


# PRF-P1-5（2026-11 审查）：拆分为「轻载 prep」与「重载 media」两个执行器。
# 此前单一 8 线程池同时承载每请求必经的 prompt/请求体构建与重图片解码——
# 8 个慢 PIL 操作（40MP 解码 + LANCZOS，单张 1-3 秒）即可占满全部 worker，
# 之后所有新请求在队列里等到 30 秒超时，表现为全站首字延迟 30 秒（队头
# 阻塞同时打击所有用户，不只生图用户）。
#   heavy=False：纯文本 prompt 构建（每请求必经、绝不应排在图片后面）；
#   heavy=True （默认）：图片解码/下载、生图落盘、Cookie 请求体构建（内含
#               远程图片抓取与压缩，本质上就是媒体工作）。
_prep_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="prep")
_media_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="media")
_prep_slots = threading.BoundedSemaphore(32)
_media_slots = threading.BoundedSemaphore(32)
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


async def run_media_task(function, *args, heavy=True, **kwargs):
    # Hold admission until the real thread finishes, even after its awaiting
    # request is cancelled. ThreadPoolExecutor's otherwise unbounded queue can
    # therefore never accumulate abandoned requests without limit.
    # Admission follows the executor too: a full media queue must not reject
    # text work while the independent preparation executor is available.
    slots = _media_slots if heavy else _prep_slots
    if not slots.acquire(blocking=False):
        raise GenerationCapacityError("图片/提示词处理并发已满，请稍后重试。")
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

    executor = _media_executor if heavy else _prep_executor
    try:
        future = executor.submit(context.run, run)
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
