"""Keep quiet SSE connections alive without cancelling or restarting reads."""
import asyncio
from contextlib import suppress


HEARTBEAT = object()
HEARTBEAT_INTERVAL = 10.0
HEARTBEAT_FRAME = ": keep-alive\n\n"


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
