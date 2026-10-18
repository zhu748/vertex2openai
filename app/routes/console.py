"""控制台页面、登录、统计与运行日志。"""

import asyncio
from functools import lru_cache
from pathlib import Path

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

import config
from auth import constant_time_equal
from console_auth import (
    AUTH_COOKIE,
    SESSION_TTL_SECONDS,
    clear_login_failure,
    client_ip,
    is_authenticated,
    issue_session,
    login_retry_after,
    record_login_failure,
    revoke_session,
    require_auth,
)
from logger import rt_logger, stats
from request_tracking import request_registry


router = APIRouter()
_TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"


@lru_cache(maxsize=2)
def _load_template(name: str) -> str:
    """页面只从随应用分发的模板读取，缓存到进程结束。"""
    return (_TEMPLATE_DIR / name).read_text(encoding="utf-8")


@router.get("/", response_class=HTMLResponse)
async def dashboard_ui(request: Request):
    name = "dashboard.html" if is_authenticated(request) else "login.html"
    return HTMLResponse(_load_template(name))


class LoginBody(BaseModel):
    password: str


@router.post("/api/login")
async def login(body: LoginBody, request: Request):
    ip = client_ip(request)

    # P2-2：失败三次后指数退避，避免对口令（同时也是 API Key）无限爆破
    remain = login_retry_after(ip)
    if remain > 0:
        return JSONResponse(status_code=429, content={"error": f"尝试过于频繁，请 {remain} 秒后再试"})

    if config.API_KEY and constant_time_equal(body.password, config.API_KEY):
        clear_login_failure(ip)
        resp = JSONResponse(content={"ok": True})
        resp.set_cookie(
            AUTH_COOKIE,
            issue_session(),
            httponly=True,
            samesite="lax",
            max_age=SESSION_TTL_SECONDS,
            path="/",
            # 反代后 request.url.scheme 可能是 http，这里同时看 x-forwarded-proto
            secure=(request.url.scheme == "https" or request.headers.get("x-forwarded-proto", "") == "https"),
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


@router.get("/api/stats")
async def get_stats_api(_auth: bool = Depends(require_auth)):
    return JSONResponse(content={**stats.get_json_stats(), "requests": request_registry.list(limit=0)["summary"]})


@router.get("/api/requests")
async def list_requests(_auth: bool = Depends(require_auth), status: str = "all",
                        q: str = Query("", max_length=200), offset: int = Query(0, ge=0),
                        limit: int = Query(50, ge=1, le=100)):
    return JSONResponse(content=request_registry.list(status=status, query=q, offset=offset, limit=limit),
                        headers={"Cache-Control": "no-store"})


@router.get("/api/requests/{request_id}")
async def request_detail(request_id: str, _auth: bool = Depends(require_auth)):
    trace = request_registry.get(request_id)
    if not trace:
        return JSONResponse(status_code=404, content={"error": "请求记录不存在或已超过保留期限。"})
    return JSONResponse(content=trace.snapshot(detail=True), headers={"Cache-Control": "no-store"})


@router.get("/stream-logs")
async def stream_logs_endpoint(request: Request, _auth: bool = Depends(require_auth)):
    def _event_chunks(msg: str):
        # SSE 规范写法：多行消息必须拆成多个 `data:` 行，浏览器会用换行拼回 e.data。
        # 之前是 yield f"data: {msg}\n\n"，msg 里的裸换行会导致换行后的每一行被浏览器
        # 当成未知字段直接丢弃——多行诊断、traceback 在控制台永远只剩第一行，
        # 只能去 Docker 日志看完整版（2026-10-04 修复）。
        for line in msg.splitlines() or [""]:
            yield f"data: {line}\n"
        yield "\n"

    async def log_generator():
        q = rt_logger.subscribe()
        try:
            for msg in rt_logger.snapshot_history():
                for chunk in _event_chunks(msg):
                    yield chunk
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=1.0)
                    for chunk in _event_chunks(msg):
                        yield chunk
                except asyncio.TimeoutError:
                    yield ": keep-alive heartbeat\n\n"
        finally:
            rt_logger.unsubscribe(q)

    return StreamingResponse(log_generator(), media_type="text/event-stream")
