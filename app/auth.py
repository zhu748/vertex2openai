import secrets

from fastapi import HTTPException, Header, Request
from typing import Optional
from config import API_KEY


def constant_time_equal(candidate: str, expected: str) -> bool:
    """UTF-8 字节比较，避免 compare_digest(str) 遇到非 ASCII 口令抛异常。"""
    return secrets.compare_digest(candidate.encode("utf-8"), expected.encode("utf-8"))


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
    bearer = ""
    if authorization and authorization.startswith("Bearer "):
        bearer = authorization[len("Bearer "):]
    candidate = x_goog_api_key or bearer or request.query_params.get("key", "")
    if not candidate:
        raise HTTPException(
            status_code=401,
            detail="缺少 API Key（支持 Authorization: Bearer / x-goog-api-key / ?key= 三种写法）。",
        )
    if not validate_api_key(candidate):
        raise HTTPException(status_code=401, detail="API Key 无效。")
    return candidate
