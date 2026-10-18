"""Express Key 列表、项目探测与 Cookie 凭证管理。"""

import asyncio

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import config
import express_key_probe
from console_auth import require_auth
from cookie_auth import validate_cookie
from lifecycle import start_background_task
from proxy_resolver import validate_proxy_uri
from runtime_state import app_state


router = APIRouter()


class ExpressKeysBody(BaseModel):
    keys: list = []


class ProbeBody(BaseModel):
    key: str = ""  # 指定单个 Key 探测
    all: bool = False  # 探测全部
    force: bool = False  # 忽略「已有 Project ID / 人工填写」的跳过规则


class KeyProjectBody(BaseModel):
    key: str
    project_id: str = ""


def _express_keys_payload() -> dict:
    """控制台按用户要求**明文**返回 Key（控制台本身有密码保护）；日志里仍打掩码。"""
    records = app_state.get_express_keys()
    return {
        "keys": records,
        "total": len(records),
        "enabled": sum(1 for r in records if r.get("enabled", True)),
        "roundrobin": bool(app_state.get_setting("roundrobin", config.ROUNDROBIN)),
        "env_import_done": True,
    }


@router.get("/api/express-keys")
async def list_express_keys(_auth: bool = Depends(require_auth)):
    return JSONResponse(content=_express_keys_payload())


@router.post("/api/express-keys")
async def save_express_keys(body: ExpressKeysBody, request: Request, _auth: bool = Depends(require_auth)):
    """整表覆盖保存（增删改都走这里），立即热生效，无需重启。

    保存空列表是合法操作：之后即使环境变量 VERTEX_EXPRESS_API_KEY 仍有值，
    启动时也不会再把 Key 复种回来（见 runtime_state.import_env_express_keys_once）。
    Key 级代理（proxy 字段）在此做静态格式校验：畸形代理一旦存进去，
    该 Key 的所有请求都会统一失败且报错晦涩，必须前置拦截。
    """
    for idx, item in enumerate(body.keys):
        if not isinstance(item, dict):
            continue
        key_proxy = str(item.get("proxy", "") or "").strip()
        err = validate_proxy_uri(key_proxy)
        if err:
            masked = express_key_probe.mask_key(str(item.get("key", "") or ""))
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"第 {idx + 1} 个 Key（{masked}）的专属代理格式不合法：{err}",
                },
            )
    saved = await asyncio.to_thread(app_state.set_express_keys, body.keys)
    request.app.state.express_key_manager.refresh_keys()
    # 新加进来、还没有 Project ID 的 Key 立刻在后台探一次
    pending = [r["key"] for r in saved if not r.get("project_id")]
    if pending:
        start_background_task(express_key_probe.probe_all(keys=pending))
    payload = _express_keys_payload()
    payload["status"] = "success"
    return JSONResponse(content=payload)


@router.post("/api/express-keys/probe")
async def probe_express_keys(body: ProbeBody, _auth: bool = Depends(require_auth)):
    """手动触发探测：指定单个 Key，或 all=true 探测全部。"""
    if body.all:
        results = await express_key_probe.probe_all(force=body.force)
    else:
        key = (body.key or "").strip()
        if not key:
            return JSONResponse(status_code=400, content={"error": "请提供 key，或设 all=true。"})
        results = [await express_key_probe.probe_and_store(key, force=body.force)]
    payload = _express_keys_payload()
    payload["results"] = results
    return JSONResponse(content=payload)


@router.post("/api/express-keys/project")
async def set_express_key_project(body: KeyProjectBody, _auth: bool = Depends(require_auth)):
    """人工覆盖某个 Key 的 Project ID（来源标记为 manual，自动探测不会再覆盖它）。"""
    ok = await asyncio.to_thread(
        app_state.update_express_key_project,
        body.key,
        body.project_id,
        source=("manual" if body.project_id.strip() else ""),
    )
    if not ok:
        return JSONResponse(status_code=404, content={"error": "该 Key 不在列表中。"})
    payload = _express_keys_payload()
    payload["status"] = "success"
    return JSONResponse(content=payload)


class CookieSetting(BaseModel):
    cookie: str = ""  # 留空 = 保持现有 Cookie
    project_id: str = ""


@router.post("/api/cookie")
async def set_google_cookie(setting: CookieSetting, _auth: bool = Depends(require_auth)):
    """保存 Cookie 与 Project ID。

    S-1：cookie 传空字符串表示「保持现有 Cookie 不变，只更新 Project ID」，
    这样前端就不需要为了改 Project ID 而把完整 Cookie 再取回来一次。
    """
    new_cookie = (setting.cookie or "").strip()
    project_id = (setting.project_id or "").strip()

    if new_cookie:
        validation = validate_cookie(new_cookie)
        if not validation["valid"]:
            return JSONResponse(status_code=400, content={"error": validation["message"]})
        message = validation["message"]
    else:
        if not app_state.get_google_cookie():
            return JSONResponse(
                status_code=400, content={"error": "尚未配置 Cookie，请粘贴完整的 Google Cookie。"}
            )
        message = "✅ 已保留原有 Cookie，仅更新 Project ID。"

    await asyncio.to_thread(app_state.set_cookie_credentials, new_cookie, project_id)
    return JSONResponse(content={"status": "success", "message": message})
