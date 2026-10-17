"""TLS / 浏览器指纹伪装模块（Cookie 通道专用，可热开关）。

背景：Cookie 直连通道模拟的是 Cloud Console 前端的 batchGraphql 请求，
但旧实现用 httpx 发包 —— TLS 指纹是 Python/OpenSSL 特征（JA3/JA4 一眼假），
请求头里连 User-Agent 都没有，Google 风控极易把这类“没有浏览器外壳的
前端接口调用”识别为脚本流量。

本模块用 curl_cffi（依赖里早已引入但从未用上）做两件事：
  1. TLS 指纹伪装：impersonate 成真实 Chrome/Edge/Safari 的 TLS 栈；
  2. 浏览器头补齐：User-Agent、sec-ch-ua、sec-fetch-*、accept-language 等
     与所选浏览器档案对齐（authorization/cookie 等 Google 专用头不受影响）。

设置项 cookie_impersonate（控制台「通道与凭证」页可热改）：
  off    = 关闭伪装，退回原 httpx 行为（完全旧兼容）
  chrome = Chrome 最新版指纹（默认）
  edge / safari / firefox / safari_ios = 其它档案
"""

from contextlib import asynccontextmanager
import threading
from typing import Any, AsyncIterator, Callable

import httpx

from runtime_state import app_state
from client_pool import schedule_graceful_close

# 控制台可选的伪装档案（curl_cffi 泛别名会跟随其内置的最新浏览器版本）
SUPPORTED_PROFILES = ("off", "chrome", "edge", "safari", "firefox", "safari_ios")

# 超时总上限：curl 的 timeout 是“整个请求”的硬超时（不像 httpx 的 read 是
# “两块数据之间的间隔”），长思考流可能持续数分钟，这里放宽到 30 分钟。
_CURL_TOTAL_TIMEOUT_SECONDS = 1800.0


def get_impersonate_profile() -> str:
    """读取当前伪装档案（热生效）。非法值一律回退 chrome。"""
    try:
        value = str(app_state.get_setting("cookie_impersonate", "chrome") or "chrome").strip().lower()
    except Exception:
        value = "chrome"
    return value if value in SUPPORTED_PROFILES else "chrome"


def impersonation_active() -> bool:
    return get_impersonate_profile() != "off"


# ============================================================
# 浏览器请求头模板（与 curl_cffi 各档案的 TLS 指纹配套）
# 只补“浏览器外观”，绝不碰 authorization / cookie / origin / referer
# 这些由 cookie_auth.build_headers 负责的 Google 专用头。
# ============================================================
_BROWSER_HEADERS: dict[str, dict[str, str]] = {
    "chrome": {
        "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"),
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not:A-Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    },
    "edge": {
        "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"),
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Chromium";v="142", "Microsoft Edge";v="142", "Not:A-Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    },
    "safari": {
        "user-agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
                       "(KHTML, like Gecko) Version/18.4 Safari/605.1.15"),
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    },
    "safari_ios": {
        "user-agent": ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) "
                       "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1"),
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    },
    "firefox": {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    },
}


def apply_browser_headers(headers: dict | None) -> dict:
    """按当前伪装档案补齐浏览器头（不覆盖已有键；伪装关闭时原样返回拷贝）。

    Google 专用头（authorization/cookie/content-type/x-goog-*/origin/referer/
    x-same-domain）永远以调用方传入的为准，浏览器头只做增量补齐。
    """
    merged = dict(headers or {})
    if not impersonation_active():
        return merged
    profile = get_impersonate_profile()
    for k, v in _BROWSER_HEADERS.get(profile, _BROWSER_HEADERS["chrome"]).items():
        merged.setdefault(k, v)
    return merged


# ============================================================
# curl_cffi 响应垫片：把 curl_cffi Response 适配成 cookie_proxy
# 正在使用的 httpx 接口子集（status_code/text/json/aread/aiter_text）。
# ============================================================
class _CurlRespShim:
    """适配 httpx 接口子集（status_code/text/json/aread/aiter_text）。

    ⚠️ 关键差异（实测 curl_cffi 0.16.3）：流被 aiter_content 消费过后
    再调 acontent() 会**永久死等**（httpx 的 aread() 则是幂等的，返回
    已缓冲内容）。cookie_proxy 的错误分支存在“先 aiter_text 再 aread”
    的用法，因此 shim 在 aiter 时自缓冲，aread 优先回缓冲，绝不二次
    消费 curl 流。
    """

    __slots__ = ("_r", "status_code", "_buffered", "_from_stream")

    def __init__(self, resp, from_stream: bool = False):
        self._r = resp
        self.status_code = resp.status_code
        self._buffered = bytearray()
        # curl_cffi 的流式响应未读时 .content 是 b''（不是 None），
        # 不能拿“content 是否为空”判断是否已缓冲，必须显式标记来源。
        self._from_stream = from_stream

    @property
    def text(self) -> str:
        return self._r.text

    def json(self):
        return self._r.json()

    async def aread(self) -> bytes:
        if self._buffered:            # 流式路径已消费过（可能部分）→ 返回缓冲
            return bytes(self._buffered)
        if not self._from_stream:     # 非流式响应：curl 已缓冲完整 body
            return self._r.content or b""
        # 未消费过的流：直接读全部（带硬超时兜底，防 curl 死等）
        import asyncio as _asyncio
        return await _asyncio.wait_for(self._r.acontent(), timeout=60.0)

    def aiter_text(self) -> AsyncIterator[str]:
        """与 httpx 调用约定一致：普通方法返回异步生成器（不需要 await）。

        cookie_proxy 的用法是 `async for chunk in response.aiter_text():`，
        shim 必须保持同一形态。每块顺带写入自缓冲（供 aread 幂等语义使用）。
        """
        async def _gen():
            async for chunk in self._r.aiter_content():
                if not chunk:
                    continue
                self._buffered.extend(chunk)
                yield chunk.decode("utf-8", errors="replace")
        return _gen()

    async def aclose(self) -> None:
        await self._r.aclose()


class _CurlClientShim:
    """把 curl_cffi AsyncSession 适配成 httpx.AsyncClient 的使用面（post/stream）。"""

    def __init__(self, session, total_timeout: float):
        self._s = session
        self._total_timeout = total_timeout

    async def post(self, url: str, *, headers=None, json=None):
        resp = await self._s.post(url, headers=headers, json=json,
                                  timeout=self._total_timeout)
        return _CurlRespShim(resp, from_stream=False)

    def stream(self, method: str, url: str, *, headers=None, json=None):
        return self._stream_ctx(method, url, headers=headers, json=json)

    @asynccontextmanager
    async def _stream_ctx(self, method: str, url: str, *, headers=None, json=None):
        async with self._s.stream(method, url, headers=headers, json=json,
                                  timeout=self._total_timeout) as resp:
            yield _CurlRespShim(resp, from_stream=True)

    async def aclose(self) -> None:
        await self._s.close()


def _httpx_timeout_to_curl(timeout: Any) -> float:
    """httpx.Timeout / (connect, read, write, pool) 语义 → curl 总超时。"""
    try:
        if isinstance(timeout, httpx.Timeout):
            total = max(timeout.connect or 0, timeout.read or 0, timeout.write or 0,
                        timeout.pool or 0)
            return max(total * 2, _CURL_TOTAL_TIMEOUT_SECONDS)
        if isinstance(timeout, (int, float)):
            return max(float(timeout), _CURL_TOTAL_TIMEOUT_SECONDS)
    except Exception:
        pass
    return _CURL_TOTAL_TIMEOUT_SECONDS


@asynccontextmanager
async def open_upstream_client(client_kwargs: dict):
    """统一上游 HTTP 客户端工厂（cookie_proxy 三处调用点的唯一入口）。

    **池化语义（重要）**：同一（伪装档案, 代理, 超时形态）的请求**共享**
    底层客户端（httpx 连接池 / curl_cffi 连接复用），重试之间、请求
    之间都不再重新 TCP+TLS 握手。旧实现每个请求（甚至每次重试）
    新建客户端再丢弃，仅握手就多花 100~300ms/次，高并发下还攒出大量
    等 GC 的未关闭连接。

    - cookie_impersonate == "off"：httpx.AsyncClient(**client_kwargs)；
    - 否则 curl_cffi AsyncSession(impersonate=档案, proxy=...) 包成
      _CurlClientShim，post/stream 接口与 httpx 同形。

    生命周期：客户端由池统一管理，本上下文管理器退出时**不关闭**它；
    控制台热改代理/伪装档案后新请求自动换新客户端，旧客户端延迟回收
    （不打断飞行中的流）。已验证 curl_cffi 0.16.3 单 session 30 并发
    请求响应无串扰，跨请求共享安全。

    client_kwargs 是 httpx 语义（timeout/follow_redirects/proxy），由工厂内部转换。
    """
    client = _upstream_client_pool.get(client_kwargs, _create_upstream_client)
    yield client


def _create_upstream_client(client_kwargs: dict):
    profile = get_impersonate_profile()
    if profile == "off":
        return httpx.AsyncClient(**client_kwargs)

    from curl_cffi.requests import AsyncSession

    proxy = client_kwargs.get("proxy")
    total_timeout = _httpx_timeout_to_curl(client_kwargs.get("timeout"))
    session_kwargs: dict[str, Any] = {
        "impersonate": profile,
        "timeout": total_timeout,
    }
    if proxy:
        session_kwargs["proxy"] = proxy
    return _CurlClientShim(AsyncSession(**session_kwargs), total_timeout)


# ============================================================
# Cookie 通道客户端池：签名 → 客户端，跨请求复用
# ============================================================
_UPSTREAM_POOL_MAX = 4   # 实际组合 ≤ 2（代理 × 伪装档案），4 已冗余


class _UpstreamClientPool:
    """（伪装档案, 代理, 超时, 重定向, 证书）→ 客户端，LRU 上限 4。

    客户端创建/登记在锁内完成（两个构造器都是纯内存操作，无 IO）；
    被淘汰的客户端交给延迟关闭器（护住飞行中的流）。签名覆盖
    client_kwargs 的全部关键字 + 伪装档案，任一变化自然分桶。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: dict[tuple, Any] = {}

    @staticmethod
    def _signature(client_kwargs: dict) -> tuple:
        timeout = client_kwargs.get("timeout")
        return (
            get_impersonate_profile(),
            str(client_kwargs.get("proxy") or ""),
            repr(timeout),
            bool(client_kwargs.get("follow_redirects", False)),
            str(client_kwargs.get("verify") or ""),
        )

    def get(self, client_kwargs: dict, factory: Callable[[dict], Any]) -> Any:
        sig = self._signature(client_kwargs)
        with self._lock:
            hit = self._entries.get(sig)
            if hit is not None:
                # LRU 触碰：pop 再放回，dict 插入序 = 最近使用序
                self._entries.pop(sig)
                self._entries[sig] = hit
                return hit

        client = factory(client_kwargs)
        stale: list = []
        with self._lock:
            hit = self._entries.get(sig)
            if hit is not None:
                # 竞态窗口：别的协程先建好同签名客户端，用它
                stale.append(client)
                client = hit
            else:
                self._entries[sig] = client
                while len(self._entries) > _UPSTREAM_POOL_MAX:
                    oldest = next(iter(self._entries))
                    if oldest == sig:
                        break
                    stale.append(self._entries.pop(oldest))

        for c in stale:
            schedule_graceful_close(_make_upstream_closer(c))
        return client


def _make_upstream_closer(client: Any) -> Callable[[], Any]:
    async def _aclose():
        try:
            await client.aclose()
        except Exception:
            pass

    return _aclose


_upstream_client_pool = _UpstreamClientPool()
