"""Bound small auxiliary HTTP responses independently of model output streams."""

from request_errors import declared_body_exceeds


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
