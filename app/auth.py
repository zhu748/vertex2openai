import secrets

from fastapi import HTTPException, Header, Request
from typing import Optional
from config import API_KEY


def extract_candidate_key(
    request: Request,
    *,
    authorization: str = "",
    x_api_key: str = "",
    x_goog_api_key: str = "",
    allow_query: bool = True,
) -> str:
    """统一提取候选 API Key（QUA-P1-3）：x-api-key > x-goog-api-key > Bearer > ?key=。

    此前同一段提取逻辑在 auth / claude_api / gemini_native / models_api
    四处平行实现，回退顺序各有微妙差异——差异本应只体现在各协议的
    「错误响应形状」，提取逻辑本身不应分叉。统一顺序：协议专用头
    （x-api-key / x-goog-api-key）最优先，通用 Bearer 次之，查询参数
    最后（SEC-P2-1：?key= 会明文进访问日志，仅作兼容兑底）。
    """
    bearer = ""
    if authorization.startswith("Bearer "):
        bearer = authorization[len("Bearer "):]
    for candidate in (x_api_key, x_goog_api_key, bearer):
        if candidate:
            return str(candidate)
    if allow_query:
        return request.query_params.get("key", "")
    return ""


def constant_time_equal(candidate: str, expected: str) -> bool:
    """UTF-8 字节比较，避免 compare_digest(str) 遇到非 ASCII 口令抛异常。"""
    try:
        return secrets.compare_digest(candidate.encode("utf-8"), expected.encode("utf-8"))
    except UnicodeEncodeError:
        return False


def validate_api_key(api_key_to_validate: str) -> bool:
    """校验调用本代理服务的 Bearer Token。

    用 secrets.compare_digest 做**常量时间**比较：`==` 在第一个不匹配
    字符处就返回，理论上允许通过网络时序差异逐字符猜口令。此处返回值
    不泄露长度差异（compare_digest 对同类型 str 比较整串）。
    """
    if not API_KEY:
        return False
    return constant_time_equal(api_key_to_validate, API_KEY)


async def get_api_key(authorization: Optional[str] = Header(None)):
    if authorization is None:
        raise HTTPException(
            status_code=401,
            detail="缺少 API Key，请在请求头中传入 Authorization: Bearer YOUR_API_KEY。",
        )

    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="API Key 格式无效，请使用 Authorization: Bearer YOUR_API_KEY。",
        )

    api_key = authorization.replace("Bearer ", "", 1)
    if not validate_api_key(api_key):
        raise HTTPException(status_code=401, detail="API Key 无效。")

    return api_key


async def get_api_key_flexible(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_goog_api_key: Optional[str] = Header(None),
):
    """鉴权依赖：兼容 Bearer / x-goog-api-key / ?key= 三种写法。

    与原生 Gemini 端点的取 Key 口径一致，便于同一客户端用同一套凭据
    发起生成与轮询 /v1/resumable/{id}（旧实现只认 Bearer，原生客户端
    用 x-goog-api-key 轮询分片会被 401，2026-10 修复）。
    """
    candidate = extract_candidate_key(
        request, authorization=authorization or "",
        x_goog_api_key=x_goog_api_key or "")
    if not candidate:
        raise HTTPException(
            status_code=401,
            detail="缺少 API Key（支持 Authorization: Bearer / x-goog-api-key / ?key= 三种写法）。",
        )
    if not validate_api_key(candidate):
        raise HTTPException(status_code=401, detail="API Key 无效。")
    return candidate
