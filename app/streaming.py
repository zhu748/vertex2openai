"""Keep quiet SSE connections alive without cancelling or restarting reads."""
import asyncio
import re
from contextlib import suppress


HEARTBEAT = object()
HEARTBEAT_INTERVAL = 10.0
HEARTBEAT_FRAME = ": keep-alive\n\n"

# 事件分隔符预编译（re.search 的模块级缓存对直接传 pattern 串也有，但这里
# 主要收益是配合 Pattern.search(buffer, pos) 免去 buffer[scan:] 的切片拷贝：
# 单个网络块含大量小事件时，旧实现每匹配一次就复制一次剩余字节）。
_FRAME_SEP_RE = re.compile(rb"\r\n\r\n|\n\n|\r\r")


async def sse_frames(source, max_frame_bytes=64 * 1024 * 1024):
    """Frame raw SSE before inserting comments or assigning resume IDs.

    Network chunks can split JSON, UTF-8 or CRLF. Heartbeats must never be
    inserted inside such a chunk's unfinished data line.
    """
    from response_validation import IncompleteResponseError
    buffer = bytearray()
    # 已消费起点：延迟压实（仅当已消费过半才 del），避免每帧都把剩余
    # 字节整体 memmove 一次——单块含大量小事件时旧实现是平方级搬运。
    start = 0
    scan = 0
    try:
        async for chunk in source:
            buffer.extend(chunk)
            while True:
                match = _FRAME_SEP_RE.search(buffer, scan)
                if match is None:
                    if len(buffer) - start > max_frame_bytes:
                        raise IncompleteResponseError("上游 SSE 事件超过字节上限。")
                    scan = max(start, len(buffer) - 3)
                    break
                end = match.end()
                if end - start > max_frame_bytes:
                    raise IncompleteResponseError("上游 SSE 事件超过字节上限。")
                yield bytes(buffer[start:end])
                start = end
                scan = start
            if start and start * 2 >= len(buffer):
                del buffer[:start]
                scan -= start
                start = 0
        if buffer[start:].strip():
            raise IncompleteResponseError("上游 SSE 在事件完整结束前断开。")
    finally:
        close = getattr(source, "aclose", None)
        if close:
            await close()


async def _disconnected(request):
    if request is None:
        return False
    try:
        return await request.is_disconnected()
    except Exception:
        return False


async def with_heartbeats(source, request=None, interval=None):
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
            if await _disconnected(request):
                return
            pending = asyncio.ensure_future(anext(iterator))
            deadline = loop.time() + interval
            while True:
                done, _ = await asyncio.wait(
                    {pending}, timeout=min(0.5, max(0, deadline - loop.time())))
                if done:
                    try:
                        item = pending.result()
                    except StopAsyncIteration:
                        return
                    pending = None
                    yield item
                    break
                if await _disconnected(request):
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


async def keepalive_sse(source, request=None, interval=None):
    events = with_heartbeats(source, request, interval)
    started = False
    try:
        yield HEARTBEAT_FRAME
        started = True
        async for item in events:
            yield HEARTBEAT_FRAME if item is HEARTBEAT else item
    finally:
        await events.aclose()
        if not started:
            close = getattr(source, "aclose", None)
            if close is not None:
                with suppress(Exception):
                    await close()


async def prepare_sse(operation, request, error_body, on_error):
    """Keep preparation and first-reply retries alive without losing fast HTTP errors.

    快窗口（_FAST_ERROR_WINDOW，250ms）：上游立即拒绝（DNS 失败/401/429 等
    通常 <250ms）能以真实 HTTP 状态码 + Retry-After 头返回。超窗后只能
    200+SSE 包裹（状态头已提交）；此时把状态码与 Retry-After 以 SSE
    注释行带出，供排障/客户端旁路读取（2026-10 修复：旧实现两者皆丢）。
    """
    from fastapi.responses import StreamingResponse
    from admission import ManagedStreamingResponse

    _FAST_ERROR_WINDOW = 0.25

    setup = asyncio.create_task(operation())

    async def cleanup():
        if not setup.done():
            setup.cancel()
        response = (await asyncio.gather(setup, return_exceptions=True))[0]
        if isinstance(response, StreamingResponse):
            # getattr 默认值不能急切求值：非 StreamingResponse 形态会先抛 AttributeError。
            close = getattr(response, "aclose", None) or response.body_iterator.aclose
            await close()

    try:
        done, _ = await asyncio.wait({setup}, timeout=_FAST_ERROR_WINDOW)
        if done:
            return setup.result()
    except BaseException:
        await cleanup()
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
                retry_after = ""
                headers = getattr(response, "headers", None)
                if headers is not None:
                    try:
                        retry_after = str(headers.get("retry-after") or "")
                    except Exception:
                        retry_after = ""
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
                                    headers={"Cache-Control": "no-store, no-transform", "X-Accel-Buffering": "no"},
                                    cleanup=cleanup)
