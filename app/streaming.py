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
    scan = 0
    try:
        async for chunk in source:
            buffer.extend(chunk)
            while True:
                match = _FRAME_SEP_RE.search(buffer, scan)
                if match is None:
                    if len(buffer) > max_frame_bytes:
                        raise IncompleteResponseError("上游 SSE 事件超过字节上限。")
                    scan = max(0, len(buffer) - 3)
                    break
                end = match.end()
                if end > max_frame_bytes:
                    raise IncompleteResponseError("上游 SSE 事件超过字节上限。")
                yield bytes(buffer[:end])
                del buffer[:end]
                scan = 0
        if buffer.strip():
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
            with suppress(Exception):
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
