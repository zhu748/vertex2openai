"""HTTP / SDK 共用的重试状态码、等待间隔与断连检测。"""
import asyncio
from email.utils import parsedate_to_datetime
import math
import random
import time
from typing import Optional


def error_status(error: Exception) -> Optional[int]:
    response = getattr(error, "response", None)
    for value in (getattr(error, "code", None), getattr(error, "status_code", None),
                  getattr(response, "status_code", None)):
        if isinstance(value, int) and 400 <= value <= 599:
            return value
    return None


def retry_delay(retry_after: Optional[str], default: float, attempt: int = 0) -> float:
    def fallback():
        # Equal jitter preserves a minimum pause and spreads concurrent retries.
        cap = min(120.0, max(0.0, default) * (2 ** min(max(0, attempt), 10)))
        return random.uniform(cap / 2, cap) if cap else 0.0
    if not retry_after:
        return fallback()
    try:
        delay = float(retry_after)
    except (TypeError, ValueError):
        try:
            delay = parsedate_to_datetime(retry_after).timestamp() - time.time()
        except (TypeError, ValueError, OverflowError):
            return fallback()
        return max(0.0, min(delay, 120.0))
    return min(delay, 120.0) if math.isfinite(delay) and delay >= 0 else fallback()


def exception_retry_delay(error: Exception, default: float, attempt: int = 0) -> float:
    headers = getattr(getattr(error, "response", None), "headers", None)
    return retry_delay(headers.get("retry-after") if headers is not None else None, default, attempt)


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
