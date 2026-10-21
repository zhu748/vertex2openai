"""共享重试配置、可重试性判断、等待与断连检测。"""

import asyncio
from email.utils import parsedate_to_datetime
import math
import random
import re
import time
from typing import Optional, TypeVar

import httpx
from curl_cffi.requests.exceptions import ConnectionError as CurlConnectionError, Timeout as CurlTimeout

import config as app_config
from response_validation import AntiTruncationError, IncompleteResponseError
from runtime_state import app_state
from upstream_errors import error_status
from request_tracking import retry_wait

Number = TypeVar("Number", int, float)

# 文本里的状态码标记：左右数字边界防止 "429" 误中 "4292" / "42912ms"。
_STATUS_IN_TEXT_RE = re.compile(r"(?<![0-9])(429|500|502|503|504)(?![0-9])")


def _retry_setting(key: str, number_type: type[Number], maximum: Number) -> Number:
    default = app_config.DEFAULT_SETTINGS[key]
    try:
        value = number_type(app_state.get_setting(key, default))
    except (TypeError, ValueError, OverflowError):
        value = number_type(default)
    if isinstance(value, float) and not math.isfinite(value):
        value = number_type(default)
    return max(number_type(0), min(maximum, value))


def get_retry_settings() -> tuple[int, float, float]:
    """读取有界重试预算、固定间隔与抖动；异常旧值回退到内置默认。

    retry_max 表示失败后的重试次数，总尝试次数为 retry_max + 1。
    """
    return (
        _retry_setting("retry_max", int, 50),
        _retry_setting("retry_backoff_seconds", float, 120.0),
        _retry_setting("retry_jitter_seconds", float, 60.0),
    )


def is_retryable_status(code: int) -> bool:
    return code == 429 or 500 <= code <= 599


def is_retryable_exception(e: Exception) -> bool:
    if isinstance(e, (AntiTruncationError, IncompleteResponseError)):
        return False
    error_str = str(e).lower()
    code = error_status(e)
    if code is not None:
        return is_retryable_status(code)
    if isinstance(
        e,
        (
            TimeoutError,
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
            CurlTimeout,
            CurlConnectionError,
        ),
    ):
        return True
    if isinstance(e, (ValueError, TypeError, AttributeError, httpx.LocalProtocolError)):
        return False
    # 数字标记用有界正则匹配：裸子串 "429" 会误中 "port 4292"/"elapsed
    # 42912ms"，把不可重试错误判成可重试 → 白白重试烧配额（2026-10 修复）。
    if _STATUS_IN_TEXT_RE.search(error_str):
        return True
    if any(
        marker in error_str
        for marker in (
            "too many requests",
            "quota",
            "resource exhausted",
            "resource_exhausted",
            "try again later",
            "rate limit",
            "overloaded",
            "temporarily unavailable",
            "internal error",
        )
    ):
        return True
    return False


def _retry_delay_value(retry_after: Optional[str], default: float, attempt: int = 0, jitter: float = 1.0) -> float:
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


def retry_delay(retry_after: Optional[str], default: float, attempt: int = 0, jitter: float = 1.0, *, error=None) -> float:
    delay = _retry_delay_value(retry_after, default, attempt, jitter)
    retry_wait(delay, error or "上游返回可重试错误或连接失败，详见关联日志。")
    return delay


def exception_retry_delay(error: Exception, default: float, attempt: int = 0, jitter: float = 1.0) -> float:
    headers = getattr(getattr(error, "response", None), "headers", None)
    return retry_delay(headers.get("retry-after") if headers is not None else None, default, attempt, jitter, error=error)


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
