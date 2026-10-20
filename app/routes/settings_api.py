"""全局设置、模型专属设置与能力摘要。"""

import asyncio
from typing import Literal

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import config
import model_capabilities as mc
from console_auth import require_auth
from model_loader import get_express_models
from runtime_state import app_state
from settings_validation import validate_global_settings_patch, validate_settings_patch


router = APIRouter()


def mask_cookie(cookie_str: str) -> str:
    """S-1：控制台只回显掩码，不再把完整 Google 会话 Cookie 明文吐回前端。"""
    if not cookie_str:
        return ""
    names = []
    for seg in cookie_str.split(";"):
        name = seg.strip().split("=", 1)[0].strip()
        if name:
            names.append(name)
    return f"已配置（共 {len(names)} 个 cookie 字段，{len(cookie_str)} 字符）"


class ModeSetting(BaseModel):
    mode: Literal["api_key", "web_proxy", "mixed"]


@router.get("/api/settings/runtime")
async def get_runtime_settings(_auth: bool = Depends(require_auth)):
    accounts = app_state.get_cookie_accounts()
    cookie = next((r["cookie"] for r in accounts if r["enabled"]), "")
    return JSONResponse(
        content={
            "use_web_proxy": app_state.is_web_proxy_enabled(),
            "mode": app_state.get_channel_mode(),
            "cookie_accounts": {"total": len(accounts), "enabled": sum(r["enabled"] for r in accounts)},
            # S-1：只回显掩码。完整 Cookie 等价于该 Google 账号的完整访问权，
            # 没有任何理由让它出现在前端 JS / 浏览器缓存 / 截图里。
            "google_cookie": mask_cookie(cookie),
            "google_cookie_configured": bool(cookie),
            "google_project_id": app_state.get_project_id(),
        }
    )


@router.post("/api/settings/mode")
async def set_settings_mode(setting: ModeSetting, _auth: bool = Depends(require_auth)):
    await asyncio.to_thread(app_state.set_channel_mode, setting.mode)
    return JSONResponse(content={"status": "success"})


@router.get("/api/settings")
async def get_settings_api(_auth: bool = Depends(require_auth)):
    return JSONResponse(content=app_state.get_settings())


@router.post("/api/settings")
async def update_settings_api(request: Request, _auth: bool = Depends(require_auth)):
    try:
        patch = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "请求体必须是合法 JSON 对象。"})
    if not isinstance(patch, dict):
        return JSONResponse(status_code=400, content={"error": "请求体必须是 JSON 对象。"})
    error = validate_global_settings_patch(patch)
    if error:
        return JSONResponse(status_code=400, content=error)
    updated = await asyncio.to_thread(app_state.update_settings, patch)
    return JSONResponse(content=updated)


@router.get("/api/capabilities")
async def get_capabilities_api(_auth: bool = Depends(require_auth)):
    try:
        models = await get_express_models()
    except Exception:
        models = []
    # 能力摘要要反映该模型生效的采样策略，否则控制台提示与实际下发不一致
    # （设置基底只取一次：旧实现每个模型都全量 deepcopy 整个设置树）
    settings_base = app_state.get_settings()
    caps = {
        m: mc.capabilities_summary(m, app_state.get_effective_settings(m, base=settings_base)) for m in models
    }
    # 附带各模型是否已有专属参数覆盖，供前端标示
    overrides = app_state.get_model_overrides()
    return JSONResponse(content={"models": models, "capabilities": caps, "overrides": overrides})


@router.get("/api/model-overrides")
async def list_model_overrides(_auth: bool = Depends(require_auth)):
    return JSONResponse(content=app_state.get_model_overrides())


@router.post("/api/model-overrides/{model_name}")
async def save_model_override(model_name: str, request: Request, _auth: bool = Depends(require_auth)):
    try:
        patch = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "请求体必须是合法 JSON 对象。"})
    if not isinstance(patch, dict):
        return JSONResponse(status_code=400, content={"error": "请求体必须是 JSON 对象。"})
    # 模型专属键同样做类型/枚举校验（与全局设置共用一套规则）
    verr = validate_settings_patch(patch, keys_scope=set(config.PER_MODEL_KEYS))
    if verr:
        return JSONResponse(status_code=400, content={"error": verr["error"], "field": verr["field"]})
    saved = await asyncio.to_thread(app_state.set_model_override, model_name, patch)
    return JSONResponse(content={"status": "success", "model": model_name, "override": saved})


@router.delete("/api/model-overrides/{model_name}")
async def delete_model_override(model_name: str, _auth: bool = Depends(require_auth)):
    ok = await asyncio.to_thread(app_state.clear_model_override, model_name)
    return JSONResponse(
        content={
            "status": "success" if ok else "not_found",
            "model": model_name,
            "override": app_state.get_model_overrides().get(model_name, {}),
        }
    )
