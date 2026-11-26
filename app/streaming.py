"""Keep quiet SSE connections alive without cancelling or restarting reads."""
import asyncio
import codecs
import re
import time
from contextlib import suppress
from typing import Any, AsyncIterable, AsyncIterator, Callable, Optional

from fastapi.responses import StreamingResponse
from stream_cleanup import await_owned_close


SETUP_CLOSE_TIMEOUT_SECONDS = 5.0


class StreamDeadlineExceeded(TimeoutError):
    """The wall-clock budget for the whole stream was exhausted."""

    def __init__(self) -> None:
        super().__init__("本轮请求超过总执行期限。")


async def reads_before_deadline(iterator: AsyncIterator, deadline: float) -> AsyncIterator:
    """Bound each upstream pull, never the consumer's work between pulls.

    Keepalive wrappers may request each item from a different task. A timeout
    spanning a yield would cancel the *previous* reader rather than the current
    one (or cancel the ASGI sender). The caller owns and closes the iterator.
    """
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise StreamDeadlineExceeded()
        # Each pull may run in a different task. An adapter can swallow its
        # cancellation, or decode/finalize synchronously past the deadline
        # before the timer gets a turn. Check all outcomes, including EOF.
        owner = asyncio.current_task()
        cancellations = owner.cancelling()
        timer = asyncio.timeout(remaining)
        try:
            async with timer:
                item = await anext(iterator)
        except Exception as exc:
            if owner.cancelling() > cancellations:
                raise asyncio.CancelledError() from exc
            if timer.expired() or time.monotonic() >= deadline:
                raise StreamDeadlineExceeded() from exc
            if isinstance(exc, StopAsyncIteration):
                return
            raise
        if owner.cancelling() > cancellations:
            raise asyncio.CancelledError()
        if timer.expired() or time.monotonic() >= deadline:
            raise StreamDeadlineExceeded()
        yield item


async def response_before_deadline(operation: Callable, deadline: float, *,
                                   clock: Callable[[], float] = time.monotonic) -> Any:
    """Transfer a prepared response only if its *whole* setup finished on time.

    Start from a factory: an expired budget must not dispatch another request.
    Timers are cooperative; a transport can translate cancellation into a result,
    or finish synchronous decoding after the cutoff before its timer gets a turn.
    """
    remaining = deadline - clock()
    if remaining <= 0:
        raise StreamDeadlineExceeded()
    task = asyncio.current_task()
    cancellations = task.cancelling()
    timer = asyncio.timeout(remaining)
    try:
        async with timer:
            result = await operation()
    except Exception as exc:
        # A handler may translate cancellation into a transport exception too.
        if task.cancelling() > cancellations:
            raise asyncio.CancelledError() from exc
        if timer.expired() or clock() >= deadline:
            raise StreamDeadlineExceeded() from exc
        raise
    cancelled = task.cancelling() > cancellations
    if cancelled or timer.expired() or clock() >= deadline:
        if isinstance(result, StreamingResponse):
            await await_owned_close(close_stream_response(result))
        if cancelled:
            raise asyncio.CancelledError()
        raise StreamDeadlineExceeded()
    return result


async def close_stream_response(response: Any) -> None:
    """Dispose a stream that was acquired but not transferred to its consumer."""
    if isinstance(response, StreamingResponse):
        close = getattr(response, "aclose", None) or getattr(response.body_iterator, "aclose", None)
        if close:
            with suppress(Exception):
                await close()


async def _close_setup_result(setup: asyncio.Task):
    try:
        response = setup.result()
    except (asyncio.CancelledError, Exception):
        return
    await close_stream_response(response)


def _close_late_setup(setup: asyncio.Task):
    from lifecycle import start_background_task
    start_background_task(_close_setup_result(setup))


async def close_setup_task(setup: asyncio.Task, *, join: bool = False) -> None:
    """等待/取消快窗口 setup 任务，并关闭其持有的流式结果。

    chat_api._call_upstream 与 prepare_sse 的快窗口包装共用：
    - 未完成且未在取消中的任务先取消（cancelling 检查避免重复取消）；
    - StreamingResponse 结果统一走 aclose（触发响应 cleanup 链）。
    """
    if not setup.done() and not setup.cancelling():
        setup.cancel()
    # bounded wait：operation() 若吞掉 CancelledError 不退出，无界 gather 会让
    # cleanup（在 shield 里被调用）永久挂死、连接滞留；5 秒足够任何合理的
    # 取消路径收尾，超时后放弃等待但仍然尝试关闭结果（2026-10 加固）。
    # A managed response already owns a separately cancellable close waiter:
    # do not tell its admission lease that a still-running late setup is gone.
    # wait_for waits for cancellation to finish, so it is not a hard bound
    # when the producer suppresses CancelledError. wait leaves that task alone;
    # a late result is still closed and its exception is always retrieved.
    try:
        done, _ = await asyncio.wait({setup}, timeout=None if join else SETUP_CLOSE_TIMEOUT_SECONDS)
    except asyncio.CancelledError:
        setup.add_done_callback(_close_late_setup)
        raise
    if done:
        await _close_setup_result(setup)
    else:
        setup.add_done_callback(_close_late_setup)


HEARTBEAT = object()
HEARTBEAT_INTERVAL = 10.0
HEARTBEAT_FRAME = ": keep-alive\n\n"
# SSE 端点统一关闭反代缓冲的响应头（2026-11 审查收敛：此前 8 处调用点
# 逐字重复同一 dict，改一处漏一处）。
SSE_NO_BUFFER_HEADERS = {"Cache-Control": "no-store, no-transform", "X-Accel-Buffering": "no"}
# SSE 终止帧（客户端按字面 "data: [DONE]" 识别；2026-11 审查收敛：原先
# 只在 resumable 定义、express_pipeline 用裸字面量比对，改由 streaming 统一
# 导出，resumable 向后兼容再导出）。
DONE_FRAME = "data: [DONE]\n\n"
# 错误消息进入 SSE / 续传缓冲时的截断上限：上游异常文本可能携带巨量堆栈
# 或响应体，统一 1024 字符（此前 express_pipeline 假流式路径用 512、其余 1024，
# 两套口径无业务差异，纯漂移；2026-11 审查收敛）。
MAX_ERROR_MESSAGE_CHARS = 1024

# SSE lines may independently end in CRLF, CR or LF. A CRLF is one
# logical line ending, never two. Defer a trailing CR at a chunk boundary
# until the next byte (or EOF) to preserve the exact wire bytes in the frame.
FRAME_SEP_RE = re.compile(rb"(?:\r\n|\r(?!\n)|\n)(?:\r\n?|\n)")


def sse_separator(buffer: bytes | bytearray, start: int, *, final: bool = False) -> Optional[re.Match[bytes]]:
    match = FRAME_SEP_RE.search(buffer, start)
    if match and not final and match.end() == len(buffer) and buffer[-1] == 13:
        return None
    return match


def _frame_bytes(buffer: bytearray, start: int, end: int) -> bytes:
    """Return owned bytes so the producer can immediately resize its buffer."""
    if not start and end == len(buffer):
        return bytes(buffer)
    if end - start < 16 * 1024:
        return bytes(buffer[start:end])
    # Slicing a bytearray would create a second event-sized temporary. Keep
    # this view entirely inside the copy, including when the consumer closes.
    with memoryview(buffer) as view, view[start:end] as frame:
        return frame.tobytes()


class ByteBoundedQueue:
    """Backpressure on both item count and retained frame bytes.

    One oversized frame may pass into an otherwise empty byte budget, so a
    legal upstream event never deadlocks. Control items carry no byte charge.
    """
    def __init__(self, maxsize: int = 128, max_bytes: int = 2 * 1024 * 1024) -> None:
        self._queue = asyncio.Queue(maxsize=maxsize)
        self._max_bytes = max_bytes
        self._bytes = 0
        self._changed = asyncio.Condition()

    def empty(self) -> bool:
        return self._queue.empty()

    async def put(self, item: Any) -> None:
        size = len(item) if isinstance(item, (bytes, bytearray)) else 0
        async with self._changed:
            await self._changed.wait_for(
                lambda: not size or not self._bytes or self._bytes + size <= self._max_bytes)
            self._bytes += size
        try:
            await self._queue.put((item, size))
        except BaseException:
            async with self._changed:
                self._bytes -= size
                self._changed.notify_all()
            raise

    async def get(self) -> Any:
        item, size = await self._queue.get()
        async with self._changed:
            self._bytes -= size
            self._changed.notify_all()
        return item


async def sse_frames(source: AsyncIterable, max_frame_bytes: int=64 * 1024 * 1024) -> AsyncIterator[bytes]:
    """Frame raw SSE before inserting comments or assigning resume IDs.

    Network chunks can split JSON, UTF-8 or CRLF. Heartbeats must never be
    inserted inside such a chunk's unfinished data line.
    """
    from response_validation import IncompleteResponseError
    buffer = bytearray()
    # 已消费起点：延迟压实（仅当已消费过半才 del），避免每帧都把剩余
    # 字节整体 memmove 一次——单块含大量小事件时旧实现是平方级搬运。
    start = scan = 0
    stream_start = True
    try:
        async for chunk in source:
            # An immutable transport chunk that is exactly one complete event
            # already owns the result. Preserve it through backpressure. A
            # trailing CR is intentionally excluded: it may acquire an LF.
            if (not buffer and type(chunk) is bytes and chunk.endswith(b"\n")
                    and not (stream_start and chunk.startswith(codecs.BOM_UTF8))):
                first = sse_separator(chunk, 0)
                if first is not None and first.end() == len(chunk):
                    if len(chunk) > max_frame_bytes:
                        raise IncompleteResponseError("上游 SSE 事件超过字节上限。")
                    stream_start = False
                    del first
                    yield chunk
                    del chunk
                    continue
                # The prefix has already been scanned on this source; the
                # buffer fallback needs only the actual boundary or its overlap.
                scan = first.start() if first is not None else max(0, len(chunk) - 3)
                del first
            buffer.extend(chunk)
            if stream_start:
                # SSE ignores one leading UTF-8 BOM, including across reads.
                # Strip it before inspecting fields or attaching a replay ID;
                # all other BOMs are ordinary data, even in later frames.
                if len(buffer) < len(codecs.BOM_UTF8) and codecs.BOM_UTF8.startswith(buffer):
                    continue
                if buffer.startswith(codecs.BOM_UTF8):
                    start = scan = len(codecs.BOM_UTF8)
                stream_start = False
            while True:
                match = sse_separator(buffer, scan)
                if match is None:
                    if len(buffer) - start > max_frame_bytes:
                        raise IncompleteResponseError("上游 SSE 事件超过字节上限。")
                    scan = max(start, len(buffer) - 3)
                    break
                end = match.end()
                if end - start > max_frame_bytes:
                    raise IncompleteResponseError("上游 SSE 事件超过字节上限。")
                yield _frame_bytes(buffer, start, end)
                start = end
                scan = start
            if start and start * 2 >= len(buffer):
                del buffer[:start]
                scan -= start
                start = 0
        # A terminal bare CR is unambiguous at EOF. All other complete
        # boundaries have already been delivered inside the loop.
        if match := sse_separator(buffer, scan, final=True):
            yield _frame_bytes(buffer, start, match.end())
            start = match.end()
        if buffer[start:].strip():
            raise IncompleteResponseError("上游 SSE 在事件完整结束前断开。")
    finally:
        close = getattr(source, "aclose", None)
        if close:
            await close()


async def disconnected(request: Optional[Any]) -> bool:
    """客户端是否已断开（request 为 None 或读取异常时按未断开处理）。

    express_pipeline 的重试循环与 with_heartbeats 共用这一个实现，
    替代原先三份逐字相同的内嵌 _client_gone/_disconnected。"""
    if request is None:
        return False
    try:
        return await request.is_disconnected()
    except Exception:
        return False


async def with_heartbeats(source: AsyncIterable, request: Optional[Any]=None, interval: Optional[float]=None) -> AsyncIterator:
    """Keep exactly one pending read; closing the consumer closes the producer.

    wait_for(anext(...)) would cancel the upstream read on every heartbeat.
    A separate pending task allows timeouts to emit comments without touching
    generation. Detached requests keep the existing background-buffer behavior.
    """
    interval = max(0.001, HEARTBEAT_INTERVAL if interval is None else interval)
    iterator = source.__aiter__()
    pending = None
    loop = asyncio.get_running_loop()
    try:
        while True:
            if request is not None and await disconnected(request):
                return
            pending = asyncio.ensure_future(anext(iterator))
            deadline = loop.time() + interval
            while True:
                remaining = max(0, deadline - loop.time())
                # Detached producers have no disconnect observer to poll. A
                # ready upstream still wakes asyncio.wait immediately.
                timeout = remaining if request is None else min(0.5, remaining)
                # Do not retain either wait set: a finished Task owns its result.
                if (await asyncio.wait({pending}, timeout=timeout))[0]:
                    try:
                        item = pending.result()
                    except StopAsyncIteration:
                        return
                    pending = None
                    try:
                        yield item
                    finally:
                        del item  # also release before a possibly slow close
                    break
                if request is not None and await disconnected(request):
                    return
                if loop.time() >= deadline:
                    yield HEARTBEAT
                    deadline = loop.time() + interval
    finally:
        if pending is not None:
            if not pending.done():
                pending.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await pending
        close = getattr(iterator, "aclose", None)
        if close is not None:
            # 清理期间可能再来一次取消（外层超时叠加/停机 gather）：
            # CancelledError 属 BaseException，不补进来会跳过 source.aclose()。
            with suppress(asyncio.CancelledError, Exception):
                await close()


async def keepalive_sse(source: AsyncIterable, request: Optional[Any] = None,
                        interval: Optional[float] = None) -> AsyncIterator:
    events = with_heartbeats(source, request, interval)
    started = False
    try:
        yield HEARTBEAT_FRAME
        started = True
        async for item in events:
            try:
                yield HEARTBEAT_FRAME if item is HEARTBEAT else item
            finally:
                del item  # the upstream read or disconnect check may now stall
    finally:
        await events.aclose()
        if not started:
            close = getattr(source, "aclose", None)
            if close is not None:
                with suppress(Exception):
                    await close()


async def prepare_sse(operation: Callable, request: Optional[Any],
                      error_body: Optional[Callable], on_error: Optional[Callable]) -> StreamingResponse:
    """Keep preparation and first-reply retries alive without losing fast HTTP errors.

    快窗口（_FAST_ERROR_WINDOW，250ms）：上游立即拒绝（DNS 失败/401/429 等
    通常 <250ms）能以真实 HTTP 状态码 + Retry-After 头返回。超窗后只能
    200+SSE 包裹（状态头已提交）；此时把状态码与 Retry-After 以 SSE
    注释行带出，供排障/客户端旁路读取（2026-10 修复：旧实现两者皆丢）。
    """
    # ManagedStreamingResponse 依赖 admission → proxy_resolver 链，保持惰性
    # 导入让 streaming 维持轻量（close_setup_task 只需要 fastapi 的类）。
    from admission import ManagedStreamingResponse
    from retry_policy import bounded_sse_advisory

    _FAST_ERROR_WINDOW = 0.25

    setup = asyncio.create_task(operation())

    async def cleanup():
        await close_setup_task(setup, join=True)

    try:
        done, _ = await asyncio.wait({setup}, timeout=_FAST_ERROR_WINDOW)
        if done:
            return setup.result()
    except BaseException:
        await close_setup_task(setup)
        raise

    async def frames():
        try:
            response = await setup
            if isinstance(response, StreamingResponse):
                async for frame in response.body_iterator:
                    yield frame
            else:
                on_error()
                # 状态头已按 200 提交：把真实状态码与 Retry-After 以 SSE 注释行
                # 透出（协议合法，data 解析器会忽略注释，排障与旁路可读）。
                status = getattr(response, "status_code", 0) or 0
                headers = getattr(response, "headers", None) or {}
                retry_after = bounded_sse_advisory(headers.get("retry-after"))
                if status:
                    yield f": upstream-status {status}\n".encode("ascii")
                if retry_after:
                    yield f": retry-after {retry_after}\n".encode("ascii")
                yield b"data: " + error_body(response) + b"\n\n"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            on_error()
            yield b"data: " + error_body(exc) + b"\n\n"

    return ManagedStreamingResponse(keepalive_sse(frames(), request), media_type="text/event-stream",
                                    headers=SSE_NO_BUFFER_HEADERS,
                                    cleanup=cleanup)
