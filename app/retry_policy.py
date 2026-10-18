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


def retry_delay(retry_after: Optional[str], default: float, attempt: int = 0,
                jitter: float = 1.0) -> float:
    """计算一次重试的等待秒数：**固定间隔 + 小幅抖动**，不随重试次数增长。

    - default：基础间隔秒数（控制台 retry_backoff_seconds，默认 5）；
    - jitter：抖动幅度秒数（控制台 retry_jitter_seconds，默认 1），实际等待在
      [default - jitter/2, default + jitter/2] 内均匀采样，且不低于 0；
    - attempt：仅为兼容旧调用签名保留的形参，**不再参与计算**。旧实现按
      2^attempt 指数放大并封顶 120 秒（默认 10 次重试累计可等 10 分钟以上），
      对本项目「每次重试换 Key/换 Cookie」的语义只会越拖越久，已改为固定间隔；
    - retry_after：上游 Retry-After（秒数或 HTTP 日期）仍被采纳，但钳位到
      [0, default + jitter/2]：更短的服务器提示照常生效，更长的等待不再
      拖住重试（上一身份的 429 对换到的新身份没有约束力）。
    """
    base = max(0.0, default)
    # 抖动只围绕非零基准生效：backoff=0 语义就是「立即重试」，
    # 不再叠加抖动（兼容零退避快速重试的既有用法）。
    half = max(0.0, jitter) / 2.0 if base > 0.0 else 0.0
    cap = base + half

    def fallback():
        if cap <= 0.0:
            return 0.0
        return random.uniform(max(0.0, base - half), cap)

    if not retry_after:
        return fallback()
    try:
        delay = float(retry_after)
    except (TypeError, ValueError):
        try:
            delay = parsedate_to_datetime(retry_after).timestamp() - time.time()
        except (TypeError, ValueError, OverflowError):
            return fallback()
        return max(0.0, min(delay, cap))
    return min(delay, cap) if math.isfinite(delay) and delay >= 0 else fallback()


def exception_retry_delay(error: Exception, default: float, attempt: int = 0,
                          jitter: float = 1.0) -> float:
    headers = getattr(getattr(error, "response", None), "headers", None)
    return retry_delay(headers.get("retry-after") if headers is not None else None,
                       default, attempt, jitter)


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
