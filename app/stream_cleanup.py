"""Cancellation-safe ownership of asynchronous transport closure."""
import asyncio
from typing import Awaitable, Callable, TypeVar

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


_Result = TypeVar("_Result")


class AttemptCancellation:
    """Cancellation belongs to the task doing this attempt, not to its returned status."""
    def __init__(self) -> None:
        self.owner = asyncio.current_task()
        self.initial = self.owner.cancelling()

    async def check(self) -> None:
        # Preserve the identity of a peer's AnyIO cancellation. A server cancel
        # or asyncio.timeout can also be translated to a transport error/data.
        await anyio.lowlevel.checkpoint_if_cancelled()
        if self.owner.cancelling() > self.initial:
            raise asyncio.CancelledError()

    async def transfer(self, result: _Result, discard: Callable[[_Result], Awaitable]) -> _Result:
        """Do not transfer a result acquired after cancellation; join its owner."""
        try:
            await self.check()
        except asyncio.CancelledError as error:
            try:
                await await_owned_close(discard(result))
            except Exception as close_error:
                error.add_note(f"Abandoned result cleanup also failed ({type(close_error).__name__}).")
            raise
        return result
