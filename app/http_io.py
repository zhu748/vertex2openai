"""Bound decoded HTTP bodies before buffering them in memory."""

import asyncio
from contextlib import asynccontextmanager
import io
from typing import AsyncIterator

import anyio
import httpx

from request_errors import declared_body_exceeds
from response_validation import IncompleteResponseError
from stream_cleanup import await_owned_close

MAX_UPSTREAM_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_UPSTREAM_ERROR_BYTES = 1024 * 1024
UNAVAILABLE_DIAGNOSTIC = b"Upstream error details unavailable."


class _OwnedHttpxStream(httpx.AsyncByteStream):
    """HTTPX marks a response closed *before* awaiting its byte stream's close."""
    def __init__(self, source: httpx.AsyncByteStream) -> None:
        self.source = source
        self.finished = asyncio.Event()

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self.source.__aiter__()

    async def aclose(self) -> None:
        try:
            # Also covers HTTPX's implicit close at EOF inside a cancellable pull.
            await await_owned_close(self.source.aclose())
        finally:
            self.finished.set()


def own_httpx_response(response: httpx.Response) -> _OwnedHttpxStream | None:
    """Install a physical-close owner before HTTPX can close implicitly at EOF."""
    if isinstance(response.stream, _OwnedHttpxStream):
        return response.stream
    if not response.is_closed:
        stream = _OwnedHttpxStream(response.stream)
        response.stream = stream
        return stream
    return None


async def close_httpx_response(response: httpx.Response) -> None:
    with anyio.CancelScope(shield=True):
        await response.aclose()
        owner = response.stream
        # Another reader may already have set HTTPX's is_closed at EOF while
        # it is still releasing the connection. Keep the caller's lease too.
        if isinstance(owner, _OwnedHttpxStream) and not owner.finished.is_set():
            await await_owned_close(owner.finished.wait())


async def read_limited_bytes(response, limit: int) -> bytes:
    """Read decoded bytes with a strict budget, even without Content-Length."""
    if declared_body_exceeds(response.headers.get("content-length", ""), limit):
        raise ValueError(f"HTTP 响应超过 {limit} 字节上限。")
    # BytesIO.getvalue() shares its finished immutable bytes buffer on CPython;
    # bytearray -> bytes would keep two complete bodies alive during the copy.
    with io.BytesIO() as data:
        async for chunk in response.aiter_bytes(chunk_size=min(64 * 1024, limit + 1)):
            if data.tell() + len(chunk) > limit:
                raise ValueError(f"HTTP 响应超过 {limit} 字节上限。")
            data.write(chunk)
        return data.getvalue()


async def read_upstream_bytes(response) -> bytes:
    limit = (MAX_UPSTREAM_ERROR_BYTES if response.status_code >= 400
             else MAX_UPSTREAM_RESPONSE_BYTES)
    try:
        # Native non-stream responses have already passed this budget at the
        # transport boundary. Reuse their bytes instead of copying up to 64 MiB
        # again while preparing the downstream response.
        if getattr(response, "is_stream_consumed", False):
            data = response.content
            if len(data) > limit:
                raise ValueError("Buffered body exceeds budget")
            return data
        return await read_limited_bytes(response, limit)
    except ValueError as exc:
        raise IncompleteResponseError(f"上游响应超过读取上限（{limit // 1024} KiB）。") from exc


async def read_status_body(response) -> bytes | None:
    """Only a *known error* may replace unreadable diagnostics with bounded text."""
    if not 400 <= response.status_code <= 599:
        return await read_upstream_bytes(response)
    task = asyncio.current_task()
    cancellations = task.cancelling()
    try:
        return await read_upstream_bytes(response)
    except (httpx.RequestError, OSError, IncompleteResponseError):
        # curl_cffi's stream read failures are OSErrors. Do not catch caller
        # cancellation, and never substitute a partial/made-up successful body.
        if task.cancelling() > cancellations:
            # Some adapters translate Task.cancel() into a read failure.
            raise asyncio.CancelledError from None
        await anyio.lowlevel.checkpoint_if_cancelled()
        return None


def decoded_http_response(response, data: bytes | None) -> httpx.Response:
    """Copy headers for decoded bytes, or drop invalid representation metadata."""
    # aiter_bytes already decompresses. Retaining Content-Encoding would make
    # the new response decompress again; its Content-Length must describe data.
    headers = [(key, value) for key, value in response.headers.multi_items()
               if key.lower() not in ("content-encoding", "content-length", "transfer-encoding")]
    if data is None:
        data = UNAVAILABLE_DIAGNOSTIC[:MAX_UPSTREAM_ERROR_BYTES]
        headers = [(key, value) for key, value in headers if key.lower() != "content-type"]
        headers.append(("content-type", "text/plain; charset=utf-8"))
    try:
        request = response.request
    except (AttributeError, RuntimeError):
        request = None
    return httpx.Response(response.status_code, headers=headers, content=data,
                          request=request if isinstance(request, httpx.Request) else None)


async def buffer_upstream_response(response) -> httpx.Response:
    """The caller retains ownership of the original stream."""
    return decoded_http_response(response, await read_status_body(response))


class BoundedAsyncClient(httpx.AsyncClient):
    """Cookie HTTP client: stream internally even for an ordinary post/get."""

    async def send(self, request, *, stream=False, **kwargs):
        response = await super().send(request, stream=True, **kwargs)
        own_httpx_response(response)
        if stream:
            return response
        try:
            return await buffer_upstream_response(response)
        finally:
            await close_httpx_response(response)

    @asynccontextmanager
    async def stream(self, *args, **kwargs):
        async with super().stream(*args, **kwargs) as response:
            try:
                yield response
            finally:
                await close_httpx_response(response)
