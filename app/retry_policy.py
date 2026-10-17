"""HTTP / SDK 共用的重试状态码、等待间隔与断连检测。"""
import asyncio
from email.utils import parsedate_to_datetime
import math
import time
from typing import Optional


def error_status(error: Exception) -> Optional[int]:
    response = getattr(error, "response", None)
    for value in (getattr(error, "code", None), getattr(error, "status_code", None),
                  getattr(response, "status_code", None)):
        if isinstance(value, int) and 400 <= value <= 599:
            return value
    return None


def retry_delay(retry_after: Optional[str], default: float) -> float:
    if not retry_after:
        return default
    try:
        delay = float(retry_after)
    except (TypeError, ValueError):
        try:
            delay = parsedate_to_datetime(retry_after).timestamp() - time.time()
        except (TypeError, ValueError, OverflowError):
            return default
        return max(0.0, min(delay, 120.0))
    return min(delay, 120.0) if math.isfinite(delay) and delay >= 0 else default


def exception_retry_delay(error: Exception, default: float) -> float:
    headers = getattr(getattr(error, "response", None), "headers", None)
    return retry_delay(headers.get("retry-after") if headers is not None else None, default)


async def wait_for_retry(request, delay: float) -> bool:
    """返回 False 表示断连；重试前和等待期间最多每半秒检查一次。"""
    deadline = time.monotonic() + delay
    while True:
        if request is not None:
            try:
                if await request.is_disconnected():
                    return False
            except Exception:
                pass
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return True
        await asyncio.sleep(min(remaining, 0.5))
