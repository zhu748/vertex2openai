"""控制台会话与登录限速；API Key 鉴权由 auth.py 负责。"""

import os
import secrets
import threading
import time

from fastapi import HTTPException, Request


AUTH_COOKIE = "ap_session"
DEFAULT_API_KEY = "123456"
SESSION_TTL_SECONDS = 60 * 60 * 24 * 30
# 会话总数上限：旧实现只在 issue_session 时顺带清理过期项、无数量上限，
# 持正确口令的脚本可循环登录无限撑大 dict（30 天 TTL 的内存驻留）。
# 超限时淘汰过期时间最早（最陈旧）的会话。
SESSIONS_MAX_ENTRIES = 256

# 随机会话 token 可单独撤销，过期时间以秒为单位。
_sessions: dict[str, float] = {}
_sessions_lock = threading.Lock()

# IP -> (失败次数, 最近失败时间)，失败三次后指数退避。
_login_failures: dict[str, tuple[int, float]] = {}
_login_lock = threading.Lock()
LOGIN_LOCK_BASE_SECONDS = 2
LOGIN_LOCK_MAX_SECONDS = 300
# 限制失败记录的保留时长和数量。
_LOGIN_FAILURES_TTL_SECONDS = 24 * 3600
_LOGIN_FAILURES_MAX_ENTRIES = 10000
# 仅在可信反代部署中启用 X-Forwarded-For，按真实客户端限速。
_TRUST_PROXY_HEADERS = os.environ.get("TRUST_PROXY_HEADERS", "").strip() not in ("", "0", "false", "False")


def client_ip(request: Request) -> str:
    """登录限速用的客户端 IP。

    取 X-Forwarded-For 的**最右侧**一跳（需 TRUST_PROXY_HEADERS=1）：
    该值由自家可信反代追加，不可被客户端伪造。旧实现取首跳——
    nginx 默认的 $proxy_add_x_forwarded_for 是追加语义，客户端伪造
    `X-Forwarded-For: 1.2.3.4` 后首跳仍是伪造值，攻击者每请求换一个
    假 IP 即可绕过指数退避无限爆破控制台口令（2026-10 修复）。
    多级可信反代链场景请由最外层反代覆盖（而非追加）XFF。
    """
    if _TRUST_PROXY_HEADERS:
        xff = request.headers.get("x-forwarded-for", "")
        if xff:
            return xff.split(",")[-1].strip() or (request.client.host if request.client else "unknown")
    return request.client.host if request.client else "unknown"


def _purge_stale_login_failures_locked(now: float) -> None:
    """惰性清理过期失败记录 + 容量上限淘汰（调用方需已持锁）。"""
    if len(_login_failures) > _LOGIN_FAILURES_MAX_ENTRIES // 2:
        # 过了半满才做全量扫描，均摊成本可忽略
        for ip in list(_login_failures):
            if now - _login_failures[ip][1] > _LOGIN_FAILURES_TTL_SECONDS:
                _login_failures.pop(ip, None)
    while len(_login_failures) >= _LOGIN_FAILURES_MAX_ENTRIES:
        # 仍满：淘汰最近失败时间最早（最陈旧）的条目，保留限速主体功能
        oldest = min(_login_failures, key=lambda k: _login_failures[k][1])
        _login_failures.pop(oldest, None)


def issue_session() -> str:
    token = secrets.token_urlsafe(32)
    now = time.time()
    with _sessions_lock:
        for t, exp in list(_sessions.items()):  # 顺手清理过期会话
            if exp < now:
                _sessions.pop(t, None)
        while len(_sessions) >= SESSIONS_MAX_ENTRIES:
            # 满额：淘汰过期时间最早（最陈旧）的会话，保留限流主体功能。
            oldest = min(_sessions, key=_sessions.get)
            _sessions.pop(oldest, None)
        _sessions[token] = now + SESSION_TTL_SECONDS
    return token


def revoke_session(token: str) -> None:
    with _sessions_lock:
        _sessions.pop(token, None)


def is_authenticated(request: Request) -> bool:
    token = request.cookies.get(AUTH_COOKIE, "")
    if not token:
        return False
    with _sessions_lock:
        exp = _sessions.get(token)
        if exp is None:
            return False
        if exp < time.time():
            _sessions.pop(token, None)
            return False
    return True


def login_retry_after(ip: str) -> int:
    """该 IP 还需等待多少秒才能再次尝试登录（0 = 可以尝试）。"""
    with _login_lock:
        rec = _login_failures.get(ip)
        if not rec:
            return 0
        count, last = rec
        # 记录已过期：当作无记录（计数归零），顺手清理
        if time.time() - last > _LOGIN_FAILURES_TTL_SECONDS:
            _login_failures.pop(ip, None)
            return 0
        if count < 3:
            return 0
        wait = min(LOGIN_LOCK_BASE_SECONDS * (2 ** (count - 3)), LOGIN_LOCK_MAX_SECONDS)
        remain = int(last + wait - time.time())
        return max(0, remain)


def record_login_failure(ip: str) -> None:
    with _login_lock:
        _purge_stale_login_failures_locked(time.time())
        count, _ = _login_failures.get(ip, (0, 0.0))
        _login_failures[ip] = (count + 1, time.time())


def clear_login_failure(ip: str) -> None:
    with _login_lock:
        _login_failures.pop(ip, None)


async def require_auth(request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="未登录")
    return True
