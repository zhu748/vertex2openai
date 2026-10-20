"""公开健康检查与控制台代理连通性测试。"""

import time

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from console_auth import require_auth
import config
from runtime_state import app_state, get_effective_proxy_url, get_effective_proxy_urls, mask_proxy_url
from proxy_resolver import classify_proxy_uri, validate_proxy_uri
from proxy_config import FORM_PROXY_SCHEMES, ProxyFormatError, build_proxy_uri, parse_proxy_list, proxy_fields
from proxy_failover import PROXY_FAILURE_STATUSES


router = APIRouter()
PROXY_TEST_TARGET = "https://aiplatform.googleapis.com/"
PROXY_TEST_TIMEOUT_SECONDS = 8.0


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
    proxy_url: str = ""


class ProxyParseBody(BaseModel):
    text: str = ""
    type: str = "socks5"
    host: str = ""
    port: str | int = ""
    username: str = ""
    password: str = ""


@router.post("/api/proxy/parse")
async def parse_proxy(body: ProxyParseBody, _auth: bool = Depends(require_auth)):
    try:
        value = body.text.strip() or build_proxy_uri(body.type, body.host, body.port, body.username, body.password)
        proxies = parse_proxy_list(value, default_scheme=body.type if body.type in FORM_PROXY_SCHEMES else "socks5")
        if not proxies or len(proxies) > 32:
            raise ProxyFormatError("请填写 1–32 条代理。")
        for index, uri in enumerate(proxies):
            error = validate_proxy_uri(uri)
            if error:
                raise ProxyFormatError(f"第 {index + 1} 条代理：{error}")
        return JSONResponse(content={"proxies": [proxy_fields(uri) for uri in proxies]},
                            headers={"Cache-Control": "no-store"})
    except ProxyFormatError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})


@router.post("/api/proxy/test")
async def test_proxy(body: ProxyTestBody, _auth: bool = Depends(require_auth)):
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
                "message": "代理协议转换失败（未发出任何请求，未降级直连）：" + str(e),
            }
        )

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

            resp = await client.get(PROXY_TEST_TARGET, headers=build_direct_headers())
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
                "message": message,
            }
        )
    except Exception as e:
        latency_ms = int((time.monotonic() - started) * 1000)
        return JSONResponse(
            content={
                "ok": False,
                "via_proxy": via_proxy,
                "proxy": mask_proxy_url(target) if via_proxy else "（直连）",
                "latency_ms": latency_ms,
                "error": f"{type(e).__name__}: {e}",
                "message": ("代理连通失败" if via_proxy else "直连 Google 失败")
                + "：请检查代理地址格式（协议://用户名:密码@IP:端口）、账号密码与代理服务器状态",
            }
        )
