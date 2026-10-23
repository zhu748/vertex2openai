"""公开健康检查与控制台代理连通性测试。"""

import asyncio
import threading
import time
from typing import Literal

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from console_auth import require_auth
import config
from runtime_state import app_state, get_effective_proxy_url, get_effective_proxy_urls, mask_proxy_url
from proxy_resolver import classify_proxy_uri, validate_proxy_uri
from proxy_config import (BRIDGE_PROXY_SCHEMES, FORM_PROXY_SCHEMES, ProxyFormatError,
                          build_proxy_uri, parse_proxy_list, proxy_fields)
from proxy_failover import PROXY_FAILURE_STATUSES
from proxy_import import MAX_IMPORT_BYTES, preview_proxy_import


router = APIRouter()
PROXY_TEST_TARGET = "https://aiplatform.googleapis.com/"
PROXY_TEST_TIMEOUT_SECONDS = 8.0
PROXY_TEST_TOTAL_SECONDS = 55.0
_probe_slots = threading.BoundedSemaphore(4)


def _healthz_payload() -> dict:
    proxy_url = get_effective_proxy_url()
    express_keys = app_state.get_express_keys()
    # healthz 是被监控高频轮询的公开端点：Cookie 账号列表取一次复用，
    # 不再每条各取一遍（每次调用都做全量记录归一化，全程持锁）。
    # cookie_configured 也从同一份列表推导：旧实现额外调 get_google_cookie()，
    # 其内部又做一遍 get_cookie_accounts() 全量归一化——秒级轮询下每次
    # healthz 白做 2 次归一化 + 1 次 Key 归一化的锁竞争（2026-10 优化）。
    accounts = app_state.get_cookie_accounts()
    return {
        "status": "ok",
        "service": "agentplatform2api",
        "mode": "mixed" if app_state.get_channel_mode() == "mixed" else
                "cookie" if app_state.is_web_proxy_enabled() else "express",
        "express_keys": {
            "total": len(express_keys),
            "enabled": sum(1 for r in express_keys if r.get("enabled", True)),
        },
        "cookie_configured": any(r["enabled"] for r in accounts),
        "cookie_accounts": {"total": len(accounts),
                            "enabled": sum(r["enabled"] for r in accounts)},
        "proxy": {
            "enabled": classify_proxy_uri(proxy_url) != "direct",
            "url": mask_proxy_url(proxy_url),  # 脱敏：绝不回传认证凭据
            "urls": [mask_proxy_url(uri) for uri in get_effective_proxy_urls()],
        },
        "time": int(time.time()),
    }


@router.get("/healthz")
async def healthz_get():
    return JSONResponse(content=_healthz_payload())


@router.head("/healthz")
async def healthz_head():
    # 保留 GET 的表示头部，但在 ASGI 层就省略正文。
    response = JSONResponse(content=_healthz_payload())
    return Response(headers=dict(response.headers))


class ProxyTestBody(BaseModel):
    proxy_url: str = Field(default="", max_length=16384)
    target: Literal["vertex", "cookie"] = "vertex"


class ProxyImportBody(BaseModel):
    text: str = Field(max_length=MAX_IMPORT_BYTES)
    type: str = "socks5"
    existing: list[str] = Field(default_factory=list, max_length=32)


@router.post("/api/proxy/import-preview")
async def preview_import(body: ProxyImportBody, _auth: bool = Depends(require_auth)):
    try:
        protocols = FORM_PROXY_SCHEMES | BRIDGE_PROXY_SCHEMES
        result = preview_proxy_import(body.text, default_scheme=body.type if body.type in protocols else "socks5",
                                      existing=body.existing)
        return JSONResponse(content=result, headers={"Cache-Control": "no-store"})
    except ProxyFormatError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)}, headers={"Cache-Control": "no-store"})


class ProxyParseBody(BaseModel):
    text: str = ""
    type: str = "socks5"
    host: str = ""
    port: str | int = ""
    username: str = ""
    password: str = ""
    options: dict[str, str | int | bool] = Field(default_factory=dict)
    name: str = ""
    draft: bool = False


@router.post("/api/proxy/parse")
async def parse_proxy(body: ProxyParseBody, _auth: bool = Depends(require_auth)):
    try:
        value = body.text.strip() or build_proxy_uri(body.type, body.host, body.port, body.username,
                                                   body.password, options=body.options, name=body.name)
        protocols = FORM_PROXY_SCHEMES | BRIDGE_PROXY_SCHEMES
        proxies = parse_proxy_list(value, default_scheme=body.type if body.type in protocols else "socks5",
                                   allow_incomplete=body.draft)
        if not proxies or len(proxies) > 32:
            raise ProxyFormatError("请填写 1–32 条代理。")
        fields = []
        for index, uri in enumerate(proxies):
            if body.draft and uri.split(":", 1)[0] in BRIDGE_PROXY_SCHEMES:
                # Draft parsing has already validated the endpoint and options.
                # Saving, testing and connecting still require full credentials.
                fields.append(proxy_fields(uri, allow_incomplete=True))
                continue
            error = validate_proxy_uri(uri)
            if error:
                raise ProxyFormatError(f"第 {index + 1} 条代理：{error}")
            fields.append(proxy_fields(uri))
        return JSONResponse(content={"proxies": fields},
                            headers={"Cache-Control": "no-store"})
    except ProxyFormatError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})


@router.post("/api/proxy/test")
async def test_proxy(body: ProxyTestBody, _auth: bool = Depends(require_auth), request: Request = None):
    from proxy_resolver import ProxyUsage, current_proxy_usage

    if not _probe_slots.acquire(blocking=False):
        return JSONResponse(status_code=429, content={"ok": False, "code": "busy", "message": "其他代理正在测试，请稍后重试。"},
                            headers={"Cache-Control": "no-store", "Retry-After": "2"})
    usage = ProxyUsage(temporary=True)
    token = current_proxy_usage.set(usage)
    task = asyncio.create_task(_test_proxy_candidate(body))
    disconnected = asyncio.Event()

    async def watch_disconnect():
        while True:
            if await request.is_disconnected():
                disconnected.set()
                task.cancel()
                return
            await asyncio.sleep(0.2)

    watcher = asyncio.create_task(watch_disconnect()) if request is not None else None
    try:
        async with asyncio.timeout(PROXY_TEST_TOTAL_SECONDS):
            response = await task
        response.headers["Cache-Control"] = "no-store"
        return response
    except asyncio.CancelledError:
        if disconnected.is_set():
            return Response(status_code=499, headers={"Cache-Control": "no-store"})
        raise
    except TimeoutError:
        return JSONResponse(content={"ok": False, "code": "timeout", "latency_ms": 0,
                                     "message": "本次探测超过总时限，节点可能启动较慢，可稍后重试。"},
                            headers={"Cache-Control": "no-store"})
    finally:
        task.cancel()
        if watcher is not None:
            watcher.cancel()
        await asyncio.gather(task, *([watcher] if watcher is not None else []), return_exceptions=True)
        current_proxy_usage.reset(token)
        try:
            await asyncio.to_thread(usage.close)
        finally:
            _probe_slots.release()


async def _test_proxy_candidate(body: ProxyTestBody):
    import httpx as _httpx
    from proxy_resolver import resolve_proxy_uri_async, ProxyResolveError

    candidate = (body.proxy_url or "").strip()
    # 先做静态格式校验，畸形值立即返回（不用等 8 秒超时，错误信息也更直白）
    format_error = validate_proxy_uri(candidate)
    if format_error:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "via_proxy": bool(candidate),
                "proxy": mask_proxy_url(candidate),
                "latency_ms": 0,
                "error": format_error,
                "message": "代理地址格式不合法：" + format_error,
            },
        )
    effective = get_effective_proxy_url()
    # 优先测前端传来的候选值（用户还没保存、想先验证的场景）；
    # 前端为空时测当前已生效的代理；两者都空 = 测直连（验证平台能否直达 Google）。
    target = candidate or effective
    via_proxy = classify_proxy_uri(target) != "direct"
    # 多协议转换：vless/vmess/trojan/ss/socks4/hysteria2/tuic 在此转成本地
    # socks5（首个请求时惰性启动 sing-box）。转换失败（sing-box 缺失等）
    # 给出可操作的安装指引；未发出任何请求、也绝不降级直连。
    setup_started = asyncio.get_running_loop().time()
    try:
        # 异步版：sing-box 首次启动放线程池，不卡事件循环
        resolved_target = await resolve_proxy_uri_async(target)
    except ProxyResolveError as e:
        return JSONResponse(
            content={
                "ok": False,
                "via_proxy": via_proxy,
                "proxy": mask_proxy_url(target),
                "latency_ms": 0,
                "error": str(e),
                "code": "conversion",
                "message": "代理协议转换失败（未发出任何请求，未降级直连）：" + str(e),
            }
        )

    setup_ms = int((asyncio.get_running_loop().time() - setup_started) * 1000)
    started = time.monotonic()
    try:
        client_kwargs = {
            "timeout": _httpx.Timeout(
                connect=PROXY_TEST_TIMEOUT_SECONDS,
                read=PROXY_TEST_TIMEOUT_SECONDS,
                write=PROXY_TEST_TIMEOUT_SECONDS,
                pool=PROXY_TEST_TIMEOUT_SECONDS,
            ),
            "follow_redirects": False,
            "trust_env": False,
        }
        if resolved_target:
            client_kwargs["proxy"] = resolved_target
        if config.SSL_CERT_FILE:
            client_kwargs["verify"] = config.SSL_CERT_FILE
        async with _httpx.AsyncClient(**client_kwargs) as client:
            # 指纹头跟随控制台 express_ua_profile：发往 Google 的探测请求
            # 也不该带 python-httpx UA（风控日志里最显眼的脚本特征）。
            from express_fingerprint import build_direct_headers

            target_url = PROXY_TEST_TARGET if body.target == "vertex" else "https://cloudconsole-pa.clients6.google.com/"
            resp = await client.get(target_url, headers=build_direct_headers())
        latency_ms = int((time.monotonic() - started) * 1000)
        # 403/404 等目标站响应证明链路可达；407 与网关错误则与实际
        # 请求的代理切换规则一致，不能向用户显示为可用出口。
        ok = resp.status_code not in PROXY_FAILURE_STATUSES
        if resp.status_code == 407:
            message = "代理认证失败（HTTP 407），请检查代理账号和密码"
        elif not ok:
            message = f"代理或上游网关返回 HTTP {resp.status_code}，请稍后重测或更换出口"
        else:
            message = ("代理连通成功" if via_proxy else "直连 Google 成功") + f"，目标站返回 HTTP {resp.status_code}"
        return JSONResponse(
            content={
                "ok": ok,
                "via_proxy": via_proxy,
                "proxy": mask_proxy_url(target) if via_proxy else "（直连）",
                "status_code": resp.status_code,
                "latency_ms": latency_ms,
                "setup_ms": setup_ms,
                "code": "reachable" if ok else "authentication" if resp.status_code == 407 else "gateway",
                "target": body.target,
                "message": message,
            }
        )
    except Exception as e:
        latency_ms = int((time.monotonic() - started) * 1000)
        if isinstance(e, _httpx.TimeoutException):
            code, detail = "timeout", "连接或响应超时，可稍后重试。"
        elif isinstance(e, _httpx.ProxyError):
            code, detail = "proxy", "代理握手或认证失败，请检查协议、账号和密码。"
        elif isinstance(e, _httpx.ConnectError):
            code, detail = "connection", "无法建立连接，请检查地址、DNS、端口和证书。"
        else:
            code, detail = "network", "网络请求失败，请检查代理状态后重试。"
        return JSONResponse(
            content={
                "ok": False,
                "via_proxy": via_proxy,
                "proxy": mask_proxy_url(target) if via_proxy else "（直连）",
                "latency_ms": latency_ms,
                "error": type(e).__name__,  # Exception text may contain credentials.
                "code": code,
                "setup_ms": setup_ms,
                "message": ("代理连通失败" if via_proxy else "直连 Google 失败")
                + "：" + detail
                + "若节点实为 TURN / TURNS / SSTP，请在代理编辑器先选择对应类型再粘贴"
                "（不带协议前缀的地址会被识别成 SOCKS5/HTTP），保存后重测。",
            }
        )
