"""Bound decoded HTTP bodies before buffering them in memory."""

import anyio
import httpx

from request_errors import declared_body_exceeds
from response_validation import IncompleteResponseError

MAX_UPSTREAM_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_UPSTREAM_ERROR_BYTES = 1024 * 1024


async def read_limited_bytes(response, limit: int) -> bytes:
    """Read decoded bytes with a strict budget, even without Content-Length."""
    if declared_body_exceeds(response.headers.get("content-length", ""), limit):
        raise ValueError(f"HTTP 响应超过 {limit} 字节上限。")
    data = bytearray()
    async for chunk in response.aiter_bytes(chunk_size=min(64 * 1024, limit + 1)):
        if len(data) + len(chunk) > limit:
            raise ValueError(f"HTTP 响应超过 {limit} 字节上限。")
        data.extend(chunk)
    return bytes(data)


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


async def buffer_upstream_response(response) -> httpx.Response:
    """Return a decoded, buffered response; the caller owns the original stream."""
    data = await read_upstream_bytes(response)
    # aiter_bytes already decompresses. Retaining Content-Encoding would make
    # the new response decompress again; its Content-Length must describe data.
    headers = [(key, value) for key, value in response.headers.multi_items()
               if key.lower() not in ("content-encoding", "content-length", "transfer-encoding")]
    try:
        request = response.request
    except (AttributeError, RuntimeError):
        request = None
    return httpx.Response(response.status_code, headers=headers, content=data, request=request)


class BoundedAsyncClient(httpx.AsyncClient):
    """Cookie HTTP client: stream internally even for an ordinary post/get."""

    async def send(self, request, *, stream=False, **kwargs):
        response = await super().send(request, stream=True, **kwargs)
        if stream:
            return response
        try:
            return await buffer_upstream_response(response)
        finally:
            with anyio.CancelScope(shield=True):
                await response.aclose()
