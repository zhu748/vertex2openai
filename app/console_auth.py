"""控制台会话与登录限速；API Key 鉴权由 auth.py 负责。"""

import math
import secrets
import threading
import time

from fastapi import HTTPException, Request

import config
from config import DEFAULT_API_KEY  # noqa: F401 —— 旧导入方（lifecycle 等）从此处转引


def resolve_admin_password() -> str:
    """控制台管理口令（SEC-P1-2）：优先独立的 ADMIN_PASSWORD，未设置时回退 API_KEY。

    拆分动机：API_KEY 常被分享给朋友/社区调用兼容端点，若它与控制台登录
    口令同源，任何拿到 Key 的人都能登录控制台导出完整 Google Cookie。
    设置 ADMIN_PASSWORD 后两者解耦；未设置时保持旧行为，不破坏存量部署。
    运行期动态读取，支持 mock.patch.object(config, ...) 的测试形态。
    """
    return config.ADMIN_PASSWORD or config.API_KEY


AUTH_COOKIE = "ap_session"
# SEC-P2-3（2026-11 收紧）：会话有效期从固定 30 天改为「绝对上限 + 闲置
# 超时」双窗口——泄漏一个 token 的完全控制期从最长 30 天收敛到
# min(7 天, 2 小时无操作)，正常使用体验不变（每次鉴权成功滑动续期）。
SESSION_TTL_SECONDS = 60 * 60 * 24 * 7    # 绝对上限：签发后 7 天必须重新登录
SESSION_IDLE_TTL_SECONDS = 60 * 60 * 2    # 闲置超时：最后活动 2 小时后失效
# 会话总数上限：旧实现只在 issue_session 时顺带清理过期项、无数量上限，
# 持正确口令的脚本可循环登录无限撑大 dict（30 天 TTL 的内存驻留）。
# 超限时淘汰签发时间最早（最陈旧）的会话。
SESSIONS_MAX_ENTRIES = 256

# 随机会话 token 可单独撤销；值 = (签发时间, 最后活动时间)，过期时间
# 按双窗口动态计算（见 _session_expired）。
_sessions: dict[str, tuple[float, float]] = {}
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
# 显式白名单解析：旧实现的黑名单只认 ""/"0"/"false"/"False"，自然写法
# FALSE/OFF/Off/no 全被当成启用——管理员明确关闭反而打开 XFF 信任，
# 客户端可伪造 X-Forwarded-For 逐请求换 IP 绕过登录限速（2026-10 修复）。
_TRUST_PROXY_HEADERS = config.env_flag("TRUST_PROXY_HEADERS")  # QUA-P2-7：布尔口径单点定义


def proxy_headers_trusted() -> bool:
    """是否处于可信反代部署（REQ-P2-6）：仅此时才采信 X-Forwarded-* 头。

    X-Forwarded-Host / X-Forwarded-For 均可由客户端伪造；直连部署下
    无条件采信会让攻击者自带该头绕过 CSRF 的 Origin 校验或伪造限速 IP。
    与登录限速的 XFF 开关共用同一环境变量，避免两套口径。"""
    return _TRUST_PROXY_HEADERS


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


def _session_expired(issued: float, last_seen: float, now: float) -> bool:
    """SEC-P2-3 双窗口裁决：绝对期（签发起 SESSION_TTL_SECONDS）或
    闲置期（最后活动起 SESSION_IDLE_TTL_SECONDS）任一越界即失效。"""
    return (now - issued >= SESSION_TTL_SECONDS
            or now - last_seen >= SESSION_IDLE_TTL_SECONDS)


def issue_session() -> str:
    token = secrets.token_urlsafe(32)
    now = time.time()
    with _sessions_lock:
        for t, (issued, last_seen) in list(_sessions.items()):  # 顺手清理过期会话
            if _session_expired(issued, last_seen, now):
                _sessions.pop(t, None)
        while len(_sessions) >= SESSIONS_MAX_ENTRIES:
            # 满额：淘汰签发时间最早（最陈旧）的会话，保留限流主体功能。
            oldest = min(_sessions, key=lambda k: _sessions[k][0])
            _sessions.pop(oldest, None)
        _sessions[token] = (now, now)
    return token


def revoke_session(token: str) -> None:
    with _sessions_lock:
        _sessions.pop(token, None)


def revoke_all_sessions() -> int:
    """SEC-P2-3：撤销全部控制台会话（「登出所有会话」）。

    口令疑似泄漏（或离开公共电脑）时的一键止损：比逐个等 TTL 快得多。
    返回被撤销的会话数。
    """
    with _sessions_lock:
        count = len(_sessions)
        _sessions.clear()
    return count


def is_authenticated(request: Request) -> bool:
    token = request.cookies.get(AUTH_COOKIE, "")
    if not token:
        return False
    now = time.time()
    with _sessions_lock:
        entry = _sessions.get(token)
        if entry is None:
            return False
        issued, last_seen = entry
        if _session_expired(issued, last_seen, now):
            _sessions.pop(token, None)
            return False
        # 滑动续期：鉴权成功即刷新「最后活动」时间（闲置窗口重算；
        # 绝对窗口不受影响，7 天后仍需重新登录）。
        _sessions[token] = (issued, now)
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
        # Clamp before exponentiation: a long-lived failure counter must not
        # construct an arbitrarily large integer just to cap it at 300 seconds.
        exponent = min(count - 3, LOGIN_LOCK_MAX_SECONDS.bit_length())
        wait = min(LOGIN_LOCK_BASE_SECONDS * (2 ** exponent), LOGIN_LOCK_MAX_SECONDS)
        remain = math.ceil(last + wait - time.time())
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
