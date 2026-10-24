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

# 可重试错误的文本关键词（Express / Cookie 两条通道共用一套口径，防两份列表漂移；
# Cookie 通道的 _is_retryable_error 直接引用，不再自带一份）。
# 2026-10 对齐上游 bad-woman/vertex2openai：补充网络截断与服务端 5xx 文本短语——
# 异常被上层包装成普通 Exception/字符串后类型信息丢失（httpx.RemoteProtocolError 等
# isinstance 白名单失效），文本匹配是最后一道防线；上游实测缺失这两个短语时
# "peer closed connection without sending complete message body (incomplete chunked read)"
# 被包装后不会重试。
RETRYABLE_MARKERS = (
    "too many requests",
    "quota",
    "resource exhausted",
    "resource_exhausted",
    "try again later",
    "rate limit",
    "overloaded",
    "temporarily unavailable",
    "internal error",
    # —— 上游补充：服务端 5xx / 超时文本 ——
    "internal",
    "backend error",
    "unavailable",
    "deadline_exceeded",
    "deadline exceeded",
    "timed out",
    "timeout",
    # —— 上游补充：网络层截断 / 连接异常文本 ——
    "remoteprotocolerror",
    "connecterror",
    "readerror",
    "writeerror",
    "proxyerror",
    "pooltimeout",
    "server disconnected",
    "connection reset",
    "incomplete chunked read",
    "peer closed connection",
)

# 坏凭证（鉴权失效 / 项目欠费 / 账号停用）的文本特征：命中后本轮请求内
# 隔离该凭证并切换下一个，不再浪费重试预算（对齐上游
# is_express_auth_or_billing_error，同时适用于 Express Key 与 Cookie 通道）。
AUTH_OR_BILLING_MARKERS = (
    "api_key_invalid",
    "api key not valid",
    "unauthenticated",
    "permission_denied",
    "requires billing",
    "billing to be enabled",
    "account_suspended",
    "consumer_invalid",
)


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


def is_retryable_message(error_msg: str) -> bool:
    """纯文本可重试性判断：状态码标记（带数字边界）或关键词任一命中。"""
    lower = (error_msg or "").lower()
    if _STATUS_IN_TEXT_RE.search(lower):
        return True
    return any(marker in lower for marker in RETRYABLE_MARKERS)


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
    return is_retryable_message(error_str)


def is_auth_or_billing_error(error: object) -> bool:
    """判断异常是否为单个凭证的鉴权失效或所属项目欠费/无权限错误。

    用途（对齐上游“坏凭证自动隔离”体系）：
    - Express Key 遇 401 API_KEY_INVALID / 项目欠费 → 当轮隔离该 Key 并切换下一个；
    - Cookie 通道遇同类错误 → 终止该通道并触发跨通道兜底；
    - 命中时同时打印红色醒目告警，提醒管理员删除或更换凭证。
    注意与可重试错误的区别：可重试错误换凭证后值得再试；坏凭证问题换一个
    凭证才有意义，同一把凭证重试只会得到相同结果。
    """
    if error is None:
        return False
    if getattr(error, "is_auth_or_billing", None) is True:
        return True
    if isinstance(error, (ValueError, TypeError)):
        # 本地参数构造错误与凭证无关（对齐上游：ValueError 直接排除）。
        return False
    code = error_status(error)
    if code in (401, 403):
        return True
    low = str(error).lower()
    return any(marker in low for marker in AUTH_OR_BILLING_MARKERS)


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
