"""控制台页面、登录、统计与运行日志。"""

import asyncio
from functools import lru_cache
from pathlib import Path

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

import config  # QUA-P2-7：环境开关读取集中层
from auth import constant_time_equal
from console_auth import (
    AUTH_COOKIE,
    SESSION_TTL_SECONDS,
    clear_login_failure,
    client_ip,
    is_authenticated,
    issue_session,
    login_retry_after,
    proxy_headers_trusted,
    record_login_failure,
    resolve_admin_password,
    revoke_all_sessions,
    revoke_session,
    require_auth,
)
from console_assets import versioned_assets
from streaming import SSE_NO_BUFFER_HEADERS
from logger import rt_logger, stats
from request_tracking import request_registry


router = APIRouter()
_TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"


@lru_cache(maxsize=2)
def _load_template(name: str) -> str:
    """页面只从随应用分发的模板读取，缓存到进程结束。"""
    return versioned_assets((_TEMPLATE_DIR / name).read_text(encoding="utf-8"))


@router.get("/", response_class=HTMLResponse)
async def dashboard_ui(request: Request):
    name = "dashboard.html" if is_authenticated(request) else "login.html"
    return HTMLResponse(_load_template(name), headers={"Cache-Control": "no-store", "Vary": "Cookie"})


class LoginBody(BaseModel):
    password: str


# SEC-P2-4：强制 Secure Cookie 开关。反代终止 TLS 但未传 x-forwarded-proto
# 时，动态探测会把会话 Cookie 发成非 Secure；设置 FORCE_SECURE_COOKIE=true
# 后无条件带 Secure（即使本地 http 调试——代价是浏览器拒发该 Cookie）。
_FORCE_SECURE_COOKIE = config.env_flag("FORCE_SECURE_COOKIE")  # QUA-P2-7：布尔口径单点定义

# SEC-P2-4：明文 HTTP 传输控制台口令的进程级一次性告警（每个方向只喷一次，
# 避免高频误配部署在日志里刷屏）。
_plain_http_warned = False


def _request_is_https(request: Request) -> bool:
    """动态判定请求是否经 TLS 抵达（反代场景看 x-forwarded-proto）。"""
    if request.url.scheme == "https":
        return True
    return request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip() == "https"


@router.post("/api/login")
async def login(body: LoginBody, request: Request):
    ip = client_ip(request)

    # P2-2：失败三次后指数退避，避免对口令（同时也是 API Key）无限爆破
    remain = login_retry_after(ip)
    if remain > 0:
        # SEC-P3-3：未开 TRUST_PROXY_HEADERS 的反代部署里，全部来访者共享
        # 同一个限速桶（client_ip 退化为反代地址），个别人的爆破会锁死管理员的
        # 登录——在 429 里直接给出设置指引，而不是让人对着「尝试过于频繁」猜。
        message = f"尝试过于频繁，请 {remain} 秒后再试"
        if not proxy_headers_trusted():
            message += "。提示：若本服务部署在反向代理后，请设置 TRUST_PROXY_HEADERS=true，否则所有来访者共享同一限速桶"
        return JSONResponse(status_code=429, content={"error": message},
                            headers={"Retry-After": str(remain), "Cache-Control": "no-store"})

    # SEC-P1-2：控制台口令优先用独立的 ADMIN_PASSWORD（未设置时回退 API_KEY）。
    # 此前直接比对 API_KEY——目标用户常把兼容端点 Key 分享给朋友/社区，
    # 拿到 Key 即可登录控制台导出完整 Google Cookie，两种信任级别混为一体。
    admin_password = resolve_admin_password()
    if admin_password and constant_time_equal(body.password, admin_password):
        clear_login_failure(ip)
        # SEC-P2-4：明文 HTTP 上的登录（且未强制 Secure Cookie）打醒目告警。
        # 登录口令、以及控制台里粘贴/回传的完整 Google Cookie 都在这条链路
        # 上明文过网；公网部署必须配 TLS 反代（README「生产部署」有示例）。
        global _plain_http_warned
        if not _plain_http_warned and not _request_is_https(request) and not _FORCE_SECURE_COOKIE:
            _plain_http_warned = True
            print("⚠️ [安全警告] 刚在明文 HTTP 连接上完成控制台登录：登录口令已明文过网，"
                  "控制台后续传输的 Google Cookie 同样暴露。公网部署请配置 HTTPS 反向代理；"
                  "反代已终止 TLS 但仍看到本告警时，请设置 FORCE_SECURE_COOKIE=true。")
        resp = JSONResponse(content={"ok": True})
        resp.set_cookie(
            AUTH_COOKIE,
            issue_session(),
            httponly=True,
            samesite="lax",
            max_age=SESSION_TTL_SECONDS,
            path="/",
            # 反代后 request.url.scheme 可能是 http，这里同时看 x-forwarded-proto；
            # FORCE_SECURE_COOKIE（SEC-P2-4）强制带 Secure，覆盖反代不传头的场景。
            secure=(_FORCE_SECURE_COOKIE
                    or request.url.scheme == "https"
                    or request.headers.get("x-forwarded-proto", "") == "https"),
        )
        return resp

    record_login_failure(ip)
    print(f"🔐 [登录失败] 来自 {ip} 的密码尝试失败。")
    return JSONResponse(status_code=401, content={"error": "密码错误"})


@router.post("/api/logout")
async def logout(request: Request):
    revoke_session(request.cookies.get(AUTH_COOKIE, ""))
    resp = JSONResponse(content={"ok": True})
    resp.delete_cookie(AUTH_COOKIE, path="/")
    return resp


@router.post("/api/sessions/revoke-all")
async def revoke_all(_auth: bool = Depends(require_auth)):
    """SEC-P2-3：撤销全部控制台会话（含当前会话，撤销后需重新登录）。

    口令疑似泄漏、或在公共电脑登过控制台时的一键止损：旧实现只能等
    30 天 TTL 自然过期（现已收紧为 7 天 + 2 小时闲置双窗口）。
    """
    revoked = revoke_all_sessions()
    print(f"🔐 [会话] 管理员已撤销全部控制台会话（共 {revoked} 个）。")
    resp = JSONResponse(content={"ok": True, "revoked": revoked})
    resp.delete_cookie(AUTH_COOKIE, path="/")
    return resp


@router.get("/api/stats")
async def get_stats_api(_auth: bool = Depends(require_auth)):
    # 概览面板只要 5 个计数：走轻量 summary()（零 snapshot 构造），
    # 旧行为 list(limit=0) 会为每个 trace 拷一份完整快照再全部丢弃。
    return JSONResponse(content={**stats.get_json_stats(), "requests": request_registry.summary()},
                        headers={"Cache-Control": "no-store"})


@router.get("/api/requests")
async def list_requests(_auth: bool = Depends(require_auth), status: str = "all",
                        q: str = Query("", max_length=200), offset: int = Query(0, ge=0),
                        limit: int = Query(50, ge=1, le=100)):
    return JSONResponse(content=request_registry.list(status=status, query=q, offset=offset, limit=limit),
                        headers={"Cache-Control": "no-store"})


@router.get("/api/recent-metrics")
async def recent_metrics(_auth: bool = Depends(require_auth)):
    return JSONResponse(content=request_registry.recent_metrics(), headers={"Cache-Control": "no-store"})


@router.get("/api/requests/{request_id}")
async def request_detail(request_id: str, _auth: bool = Depends(require_auth)):
    trace = request_registry.get(request_id)
    if not trace:
        return JSONResponse(status_code=404, content={"error": "请求记录不存在或已超过保留期限。"})
    return JSONResponse(content=trace.snapshot(detail=True), headers={"Cache-Control": "no-store"})


@router.get("/stream-logs")
async def stream_logs_endpoint(request: Request, _auth: bool = Depends(require_auth)):
    def _event_chunks(seq, msg: str):
        # SSE 规范写法：多行消息必须拆成多个 `data:` 行，浏览器会用换行拼回 e.data。
        # 之前是 yield f"data: {msg}\n\n"，msg 里的裸换行会导致换行后的每一行被浏览器
        # 当成未知字段直接丢弃——多行诊断、traceback 在控制台永远只剩第一行，
        # 只能去 Docker 日志看完整版（2026-10-04 修复）。
        #
        # id 字段携带全局递增 seq：浏览器 EventSource 断线自动重连时会回传
        # Last-Event-ID，服务端据此增量重放——旧实现每次重连全量重放 100 条
        # 历史，前端无条件 append 导致日志成片重复、计数虚高（2026-10 修复）。
        # 不识别 id 的客户端退化为旧行为（全量重放），不受影响。
        yield f"id: {seq}\n"
        for line in msg.splitlines() or [""]:
            yield f"data: {line}\n"
        yield "\n"

    async def log_generator():
        q = rt_logger.subscribe()
        try:
            last_id = -1
            raw = (request.headers.get("last-event-id") or "").strip()
            if raw.isdecimal():
                try:
                    last_id = int(raw)
                except ValueError:
                    pass
            history = rt_logger.snapshot_history()
            # EventSource survives deployments; the process-local sequence does
            # not. An old future cursor must not hide all logs after a restart.
            if last_id > (history[-1][0] if history else 0):
                last_id = -1
            for seq, msg in history:
                if seq <= last_id:
                    continue  # 重连的调用者已经拿到过这条
                for chunk in _event_chunks(seq, msg):
                    yield chunk
                last_id = seq
            while True:
                if await request.is_disconnected():
                    break
                try:
                    seq, msg = await asyncio.wait_for(q.get(), timeout=1.0)
                    if seq <= last_id:
                        continue  # 历史重放与订阅队列的竞态窗口内可能重复交付
                    for chunk in _event_chunks(seq, msg):
                        yield chunk
                    last_id = seq
                except TimeoutError:
                    yield ": keep-alive heartbeat\n\n"
        finally:
            rt_logger.unsubscribe(q)

    return StreamingResponse(
        log_generator(),
        media_type="text/event-stream",
        # 与聊天/原生 SSE 端点同口径：默认开启响应缓冲的反代（nginx 等）
        # 会攒住日志与心跳，控制台「运行日志」长时间不更新（2026-10 修复）。
        headers=SSE_NO_BUFFER_HEADERS,
    )
