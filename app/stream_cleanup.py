"""Cancellation-safe ownership of asynchronous transport closure."""
import asyncio
from typing import Awaitable

import anyio


async def await_owned_close(operation: Awaitable) -> None:
    """Keep ownership through cancellation; never restart an interrupted close."""
    interrupted = None
    # Shield from ASGI/AnyIO scopes and direct Task.cancel(). Keep a strong
    # reference and wait for the same task rather than orphaning a live response.
    with anyio.CancelScope(shield=True):
        closing = asyncio.ensure_future(operation)
        while not closing.done():
            try:
                await asyncio.shield(closing)
            except asyncio.CancelledError as exc:
                interrupted = exc
            except Exception:
                break  # retrieve the close failure below

    if interrupted is not None:
        if not closing.cancelled():
            closing.exception()  # consume any close failure, keeping cancellation primary
        raise interrupted
    try:
        closing.result()
    finally:
        # Retrieve the close outcome even if an outer AnyIO cancellation
        # arrived in the shield, then let that scope observe its cancellation.
        await anyio.lowlevel.checkpoint_if_cancelled()
