"""Express Key 列表、项目探测与 Cookie 凭证管理。"""

import asyncio
import math

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import config
import express_key_probe
from console_auth import require_auth
from cookie_auth import validate_cookie
from credential_config import parse_cookie_header
from lifecycle import start_background_task
from proxy_resolver import validate_proxy_uri
from proxy_config import ProxyFormatError, parse_proxy_list
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


def _validate_account_proxies(item, label):
    if "proxy_fallback" in item and not isinstance(item["proxy_fallback"], bool):
        return f"{label} 的 proxy_fallback 必须是布尔值。"
    if "enabled" in item and not isinstance(item["enabled"], bool):
        return f"{label} 的 enabled 必须是布尔值。"
    try:
        proxies = parse_proxy_list(item.get("proxies") if "proxies" in item else item.get("proxy", ""))
    except ProxyFormatError as exc:
        return f"{label}：{exc}"
    if len(proxies) > 32:
        return f"{label} 最多设置 32 条代理。"
    for index, proxy in enumerate(proxies):
        err = validate_proxy_uri(proxy)
        if err:
            return f"{label} 第 {index + 1} 条专属代理格式不合法：{err}"
    return None


@router.get("/api/express-keys")
async def list_express_keys(_auth: bool = Depends(require_auth)):
    return JSONResponse(content=_express_keys_payload(), headers={"Cache-Control": "no-store"})


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
            return JSONResponse(status_code=400, content={"error": "每条 Key 记录必须是对象。"})
        for field in ("key", "project_id", "project_source", "note"):
            if field in item and not isinstance(item[field], str):
                return JSONResponse(status_code=400, content={"error": f"第 {idx + 1} 个 Key 的 {field} 必须是字符串。"})
        detected = item.get("detected_at", 0)
        if (isinstance(detected, bool) or not isinstance(detected, (int, float))
                or detected < 0 or (isinstance(detected, float) and not math.isfinite(detected))):
            return JSONResponse(status_code=400, content={"error": f"第 {idx + 1} 个 Key 的 detected_at 必须是有限的非负时间戳。"})
        err = _validate_account_proxies(item, f"第 {idx + 1} 个 Key")
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
    return JSONResponse(content=payload, headers={"Cache-Control": "no-store"})


class CookieAccountsBody(BaseModel):
    accounts: list = []


def _cookie_accounts_payload():
    from routes.settings_api import mask_cookie
    from credential_manager import credential_manager
    records = app_state.get_cookie_accounts()
    health = credential_manager.cookie_health_snapshot()
    accounts = [{**{k: v for k, v in rec.items() if k != "cookie"},
                 "cookie_configured": bool(rec["cookie"]), "cookie_preview": mask_cookie(rec["cookie"]),
                 "health": health.get(rec["id"], {"cooldown_remaining": 0.0, "failures": 0})}
                for rec in records]
    return {"accounts": accounts, "total": len(accounts), "enabled": sum(r["enabled"] for r in accounts)}


@router.get("/api/cookie-accounts")
async def list_cookie_accounts(_auth: bool = Depends(require_auth)):
    return JSONResponse(content=_cookie_accounts_payload(), headers={"Cache-Control": "no-store"})


@router.post("/api/cookie-accounts")
async def save_cookie_accounts(body: CookieAccountsBody, _auth: bool = Depends(require_auth)):
    existing = {rec["id"]: rec for rec in app_state.get_cookie_accounts()}
    clean, ids, cookies = [], set(), set()
    for index, item in enumerate(body.accounts):
        label = f"第 {index + 1} 个 Cookie 账号"
        if not isinstance(item, dict):
            return JSONResponse(status_code=400, content={"error": f"{label}必须是对象。"})
        record = dict(item)
        record_id = record.get("id", "")
        if not isinstance(record_id, str) or len(record_id) > 100:
            return JSONResponse(status_code=400, content={"error": f"{label}的 ID 无效。"})
        record_id = record_id.strip()
        record["id"] = record_id
        cookie = record.get("cookie", "")
        if not isinstance(cookie, str):
            return JSONResponse(status_code=400, content={"error": f"{label}的 Cookie 必须是字符串。"})
        if cookie.strip():
            try:
                cookie = parse_cookie_header(cookie)
            except (ValueError, TypeError):
                return JSONResponse(status_code=400, content={"error": f"{label}的 Cookie JSON 无效。"})
        else:
            cookie = existing.get(record_id, {}).get("cookie", "")
        validation = validate_cookie(cookie)
        if not validation["valid"]:
            return JSONResponse(status_code=400, content={"error": f"{label}：{validation['message']}"})
        for field in ("project_id", "note"):
            if field in record and not isinstance(record[field], str):
                return JSONResponse(status_code=400, content={"error": f"{label}的 {field} 必须是字符串。"})
        project = record.get("project_id", "").strip()
        if record.get("enabled", True) and not project:
            return JSONResponse(status_code=400, content={"error": f"{label}启用前需要填写 Project ID。"})
        error = _validate_account_proxies(record, label)
        if error:
            return JSONResponse(status_code=400, content={"error": error})
        if (record_id and record_id in ids) or cookie in cookies:
            return JSONResponse(status_code=400, content={"error": f"{label}与前面的账号重复。"})
        ids.add(record_id)
        cookies.add(cookie)
        record.update(cookie=cookie, project_id=project)
        clean.append(record)
    await asyncio.to_thread(app_state.set_cookie_accounts, clean)
    return JSONResponse(content={"status": "success", **_cookie_accounts_payload()},
                        headers={"Cache-Control": "no-store"})


class CookieTestBody(BaseModel):
    id: str = ""  # 指定账号 ID；空 = 测试全部启用账号


async def _probe_cookie_account(record: dict) -> dict:
    """零配额消耗的 Cookie 会话探测：请求不存在的模型名。

    鉴权有效 → 上游返回 gRPC 5 NOT_FOUND / 3 INVALID_ARGUMENT（证明认证通过）；
    Cookie 失效 → HTTP 401/403 或 gRPC 7 PERMISSION_DENIED / 16 UNAUTHENTICATED。
    结果同步写入账号健康状态（成功清零 / 失败累计）。
    """
    import time as _time
    import httpx
    from cookie_auth import BATCH_GRAPHQL_URL, build_headers
    from credential_manager import credential_manager
    from impersonate import open_upstream_client
    from proxy_failover import ProxyChain
    from upstreams.cookie_request import build_probe_body
    from upstreams.cookie_proxy import _grpc_status_code, _is_cookie_expired_error

    started = _time.time()

    def _done(status: str, detail: str, *, account_id: str = "", invalid: bool = False) -> dict:
        if account_id:
            if status == "valid":
                credential_manager.record_cookie_success(account_id)
            elif invalid:
                credential_manager.record_cookie_auth_failure(account_id)
        return {"id": record.get("id", ""), "label": record.get("note", "") or record.get("id", ""),
                "status": status, "detail": detail,
                "elapsed_ms": round((_time.time() - started) * 1000)}

    headers = build_headers(record.get("cookie", ""))
    if headers is None:
        return _done("invalid", "Cookie 缺少 SAPISID 族字段，无法计算认证头。",
                     account_id=record.get("id", ""), invalid=True)
    project = (record.get("project_id", "") or app_state.get_project_id() or "").strip()
    if not project:
        return _done("error", "该账号未填写 Project ID，无法构造探测请求。")

    client_kwargs = {
        "timeout": httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0),
        "follow_redirects": False,
        "trust_env": False,
    }
    if config.SSL_CERT_FILE:
        client_kwargs["verify"] = config.SSL_CERT_FILE
    client_kwargs["_proxy_chain"] = ProxyChain(record)
    body = build_probe_body(project)
    try:
        async with open_upstream_client(client_kwargs) as client:
            resp = await client.post(BATCH_GRAPHQL_URL, headers=headers, json=body)
            status_code, text = resp.status_code, resp.text
    except Exception as exc:
        return _done("error", f"网络/代理错误：{type(exc).__name__}: {exc}")

    if status_code in (401, 403):
        return _done("invalid", f"HTTP {status_code}：会话已失效或无权访问该 Project。",
                     account_id=record.get("id", ""), invalid=True)

    # 解析错误块里的 gRPC 状态码做判定（结构化优先，文本兜底）
    import json as _json
    detail = ""
    grpc = None
    try:
        decoder = _json.JSONDecoder()
        idx = 0
        while idx < len(text):
            while idx < len(text) and text[idx] not in "{[":
                idx += 1
            if idx >= len(text):
                break
            try:
                obj, end = decoder.raw_decode(text, idx)
            except _json.JSONDecodeError:
                break
            idx = end
            if not isinstance(obj, dict):
                continue
            for result in obj.get("results") or []:
                for err in (result or {}).get("errors") or []:
                    grpc = _grpc_status_code(err) or grpc
                    detail = (err or {}).get("message") or detail
    except Exception:
        pass

    if grpc in (7, 16):
        return _done("invalid", f"鉴权失败（gRPC {grpc}）：{detail or 'permission/unauthenticated'}",
                     account_id=record.get("id", ""), invalid=True)
    if grpc is not None:
        # NOT_FOUND(5)/INVALID_ARGUMENT(3)/RESOURCE_EXHAUSTED(8) 等都证明认证已通过
        return _done("valid", f"会话有效（gRPC {grpc}：{detail or '模型不存在属预期'}）",
                     account_id=record.get("id", ""))
    if status_code == 200 and not detail:
        return _done("valid", "会话有效（上游 200 且无错误块）。", account_id=record.get("id", ""))
    if detail and _is_cookie_expired_error(detail):
        return _done("invalid", f"疑似会话失效：{detail}", account_id=record.get("id", ""), invalid=True)
    return _done("error", f"无法判定（HTTP {status_code}）：{(detail or text)[:200]}")


@router.post("/api/cookie-accounts/test")
async def test_cookie_accounts(body: CookieTestBody, _auth: bool = Depends(require_auth)):
    accounts = [rec for rec in app_state.get_cookie_accounts() if rec["enabled"]]
    if body.id.strip():
        accounts = [rec for rec in accounts if rec["id"] == body.id.strip()]
    if not accounts:
        return JSONResponse(status_code=404, content={"error": "没有匹配的启用 Cookie 账号。"})
    results = [await _probe_cookie_account(rec) for rec in accounts]
    valid = sum(1 for r in results if r["status"] == "valid")
    return JSONResponse(content={
        "results": results, "total": len(results), "valid": valid,
        "invalid": len(results) - valid,
    }, headers={"Cache-Control": "no-store"})


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
    return JSONResponse(content=payload, headers={"Cache-Control": "no-store"})


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
    return JSONResponse(content=payload, headers={"Cache-Control": "no-store"})


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
