"""Join HTTPX stream closure when an SDK iterator is stopped between reads."""

import asyncio
from contextvars import ContextVar
from typing import Any, AsyncIterable, Awaitable, Callable, Self

import httpx

from http_io import close_httpx_response, own_httpx_response
from stream_cleanup import await_owned_close


_active_responses = ContextVar("sdk_stream_responses", default=None)


class _Responses:
    def __init__(self) -> None:
        self.live: dict[httpx.Response, asyncio.Event] = {}

    def add(self, response: httpx.Response) -> None:
        # The SDK may do sequential continuation hops. Do not pin their
        # completed HTTP responses or mistake is_closed for physical closure.
        self.live = {earlier: done for earlier, done in self.live.items() if not done.is_set()}
        stream = own_httpx_response(response)
        if stream is not None and not stream.finished.is_set():
            self.live[response] = stream.finished

    async def aclose(self) -> None:
        live, self.live = self.live, {}
        pending = [r for r, done in live.items() if not done.is_set()]
        # Normally exactly one request owns a response. Always join every
        # captured close, even if a transport raises while releasing one.
        if len(pending) == 1:
            await close_httpx_response(pending[0])
        elif pending:
            results = await asyncio.gather(*(close_httpx_response(r) for r in pending), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result


async def capture_sdk_stream_response(response: httpx.Response) -> None:
    """HTTPX response event hook, scoped to one active SDK stream attempt."""
    if (owner := _active_responses.get()) is not None:
        owner.add(response)


class _SDKStream:
    def __init__(self, source: AsyncIterable[Any], responses: _Responses) -> None:
        self._source = source.__aiter__()
        self._responses = responses
        self._closing = None

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Any:
        # The SDK's first request/continuation can occur on a later pull, and
        # keepalives can move consecutive pulls to different asyncio tasks.
        token = _active_responses.set(self._responses)
        try:
            return await anext(self._source)
        finally:
            _active_responses.reset(token)

    async def _close(self) -> None:
        try:
            if close := getattr(self._source, "aclose", None):
                await close()
        finally:
            # google-genai 2.31 closes HTTPX in a nested async iterator. Closing
            # only its public generator can leave that iterator to GC.
            await self._responses.aclose()

    async def aclose(self) -> None:
        if self._closing is None:
            self._closing = asyncio.create_task(self._close())
        await await_owned_close(self._closing)


async def open_sdk_stream(generate: Callable[..., Awaitable[AsyncIterable[Any]]], **kwargs: Any) -> _SDKStream:
    responses = _Responses()
    token = _active_responses.set(responses)
    try:
        return _SDKStream(await generate(**kwargs), responses)
    except BaseException as error:
        try:
            await await_owned_close(responses.aclose())
        except Exception as close_error:
            error.add_note(f"SDK response cleanup also failed ({type(close_error).__name__}).")
        raise
    finally:
        _active_responses.reset(token)
