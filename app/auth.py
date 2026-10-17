import secrets

from fastapi import HTTPException, Header
from typing import Optional
from config import API_KEY


def validate_api_key(api_key_to_validate: str) -> bool:
    """校验调用本代理服务的 Bearer Token。

    用 secrets.compare_digest 做**常量时间**比较：`==` 在第一个不匹配
    字符处就返回，理论上允许通过网络时序差异逐字符猜口令。此处返回值
    不泄露长度差异（compare_digest 对同类型 str 比较整串）。
    """
    if not API_KEY:
        return False
    return secrets.compare_digest(api_key_to_validate, API_KEY)


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
