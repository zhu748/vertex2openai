"""共享 HTTP 状态提取、OpenAI 错误体和上游异常映射。"""

import re
from typing import Any

import httpx
from curl_cffi.requests.exceptions import ConnectionError as CurlConnectionError, Timeout as CurlTimeout
from request_tracking import redact


def error_status(error: Exception) -> int | None:
    response = getattr(error, "response", None)
    for value in (
        getattr(error, "code", None),
        getattr(error, "status_code", None),
        getattr(response, "status_code", None),
    ):
        if isinstance(value, int) and 400 <= value <= 599:
            return value
    return None


def google_status_detail(code: int) -> str:
    """HTTP 状态码 → Google 风格 error.status（原生端点错误体共用）。

    gemini_native 的 _native_error 与 resumable 缓冲的 native 错误帧共用
    这一份映射，保证两条路径对同一状态码给出一致的 detail。
    """
    return {
        400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 403: "PERMISSION_DENIED",
        404: "NOT_FOUND", 413: "RESOURCE_EXHAUSTED", 429: "RESOURCE_EXHAUSTED", 500: "INTERNAL",
        502: "UNAVAILABLE", 503: "UNAVAILABLE", 504: "DEADLINE_EXCEEDED",
    }.get(code, "UNKNOWN")


def create_openai_error_response(status_code: int, message: str, error_type: str) -> dict[str, Any]:
    safe_message = redact(message)
    return {"error": {"message": safe_message, "type": error_type, "code": status_code, "param": None}}


def extract_upstream_error(e: Exception) -> tuple[int, str]:
    """从上游异常里尽力提取 (HTTP 状态码, 简明消息)。

    google-genai 的 ClientError/ServerError 带 .code 与结构化 message；
    其余异常回退 500 + 类名+摘要。用于把 404/403/400 等如实透传给客户端，
    避免笼统的 500 Internal Server Error。
    """
    code = error_status(e)
    if code is None and isinstance(e, (TimeoutError, httpx.TimeoutException, CurlTimeout)):
        code = 504
    elif code is None and isinstance(e, (httpx.NetworkError, httpx.RemoteProtocolError, CurlConnectionError)):
        code = 502
    msg = str(e)
    # 从形如 "{'error': {'code':404,'message':...}}" 的 message 里提取更干净的说明
    match = re.search(r"'message':\s*'([^']+)'", msg) or re.search(r'"message":\s*"([^"]+)"', msg)
    if match:
        msg = match.group(1)
    if code is None:
        low = str(e).lower()
        if "not found" in low or "404" in low:
            code = 404
        elif "permission" in low or "403" in low or "denied" in low:
            code = 403
        elif "invalid" in low or "400" in low:
            code = 400
        else:
            code = 500
    return code, redact(msg)
