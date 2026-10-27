"""Cookie transport: use one curl preset for TLS and browser identity headers.

Application/authentication headers describe an API fetch, while curl supplies
its own matching User-Agent and client hints. Navigation-only defaults are
explicitly suppressed. This is protocol consistency, not a guarantee against
upstream account restrictions or private API changes.
"""

from contextlib import asynccontextmanager
import asyncio
import codecs
import threading
from typing import Any, AsyncIterator, Callable

import httpx
from proxy_tls import build_httpx_proxy

from runtime_state import app_state
from client_pool import schedule_graceful_close, _closer

# 控制台可选的伪装档案（curl_cffi 泛别名会跟随其内置的最新浏览器版本）
SUPPORTED_PROFILES = ("off", "chrome", "edge", "safari", "firefox", "safari_ios")

# 超时总上限：curl 的 timeout 是“整个请求”的硬超时（不像 httpx 的 read 是
# “两块数据之间的间隔”），长思考流可能持续数分钟，这里放宽到 30 分钟。
_CURL_TOTAL_TIMEOUT_SECONDS = 1800.0
_CURL_BUFFER_LIMIT_BYTES = 64 * 1024 * 1024
# 流式响应的自缓冲上限：aiter_text 为 aread 幂等语义自留一份副本，
# 但 200 响应实际只会被 aiter 消费（非 200 才先 aread）——旧实现把整个
# 流式响应（生图可达数十 MB base64 × 并发数）全额驻留内存，且超过
# 64MiB 时流中途抛 ValueError 把能正常分块交付的大响应判死。改为：
# 超出 1MiB 即放弃缓冲（错误诊断体远小于此），继续纯透传。
_CURL_SELF_BUFFER_LIMIT_BYTES = 1024 * 1024


def get_impersonate_profile() -> str:
    """读取当前伪装档案（热生效）。非法值一律回退 chrome。"""
    try:
        value = str(app_state.get_setting("cookie_impersonate", "chrome") or "chrome").strip().lower()
    except Exception:
        value = "chrome"
    return value if value in SUPPORTED_PROFILES else "chrome"


def impersonation_active() -> bool:
    return get_impersonate_profile() != "off"


def apply_browser_headers(headers: dict | None) -> dict:
    """Let curl generate identity headers from the same preset as its TLS stack."""
    merged = dict(headers or {})
    if not impersonation_active():
        return merged
    merged.update({
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        # None is curl_cffi's explicit suppression marker, not an empty header.
        "sec-fetch-user": None,
        "upgrade-insecure-requests": None,
    })
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

    自缓冲有 1MiB 上限（_CURL_SELF_BUFFER_LIMIT_BYTES）：超过即放弃
    缓冲纯透传，避免把整个流式响应（生图 base64 数 MB × 并发）驻留
    内存；放弃后再调 aread 返回空字节（当前调用图无此路径，防御性兑底）。
    """

    __slots__ = ("_r", "status_code", "_buffered", "_from_stream", "_consumed", "_dropped")

    def __init__(self, resp, from_stream: bool = False):
        self._r = resp
        self.status_code = resp.status_code
        self._buffered = bytearray()
        # curl_cffi 的流式响应未读时 .content 是 b''（不是 None），
        # 不能拿“content 是否为空”判断是否已缓冲，必须显式标记来源。
        self._from_stream = from_stream
        self._consumed = False
        self._dropped = False

    @property
    def text(self) -> str:
        return self._r.text

    @property
    def headers(self):
        return self._r.headers

    def json(self):
        return self._r.json()

    async def aread(self) -> bytes:
        # _dropped：流式读取中已丢弃超限缓冲——旧实现漏了此态，半消费的
        # curl 流上再调 aread 会重新进入 aiter_content 永久死等到 60s 超时
        # （文档承诺“放弃后再调 aread 返回空字节”，2026-10 修复）。
        if self._consumed or self._dropped or self._buffered:
            return bytes(self._buffered)
        if not self._from_stream:     # 非流式响应：curl 已缓冲完整 body
            return self._r.content or b""
        # Consume incrementally, so an oversized error body cannot bypass the
        # bounded stream buffer by using curl's all-at-once acontent().
        import asyncio as _asyncio
        async with _asyncio.timeout(60.0):
            async for chunk in self._r.aiter_content():
                if len(self._buffered) + len(chunk) > _CURL_BUFFER_LIMIT_BYTES:
                    raise ValueError("上游响应超过 64MiB 读取上限。")
                self._buffered.extend(chunk)
        self._consumed = True
        return bytes(self._buffered)

    def aiter_bytes(self, chunk_size=None) -> AsyncIterator[bytes]:
        """Expose bounded byte reads to auxiliary probes without buffering a body."""
        if chunk_size is not None and chunk_size < 1:
            raise ValueError("chunk_size must be positive")

        async def _gen():
            if not self._from_stream or self._consumed:
                data = bytes(self._buffered) if self._from_stream else (self._r.content or b"")
                size = chunk_size or max(1, len(data))
                for offset in range(0, len(data), size):
                    yield data[offset:offset + size]
                return
            async for chunk in self._r.aiter_content():
                if not chunk:
                    continue
                if not self._dropped:
                    if len(self._buffered) + len(chunk) > _CURL_SELF_BUFFER_LIMIT_BYTES:
                        # 大流式响应：放弃幂等缓冲（错误诊断体远小于此限额），
                        # 不再让整个响应驻留内存、也不再中途判死。
                        self._dropped = True
                        self._buffered.clear()
                    else:
                        self._buffered.extend(chunk)
                size = chunk_size or len(chunk)
                for offset in range(0, len(chunk), size):
                    yield chunk[offset:offset + size]
            self._consumed = True
        return _gen()

    def aiter_text(self) -> AsyncIterator[str]:
        """Decode incrementally, sharing the byte iterator's bounded replay buffer."""
        async def _gen():
            decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
            source = self.aiter_bytes()
            try:
                async for chunk in source:
                    text = decoder.decode(chunk)
                    if text:
                        yield text
            finally:
                await source.aclose()
            tail = decoder.decode(b"", final=True)
            if tail:
                yield tail
        return _gen()

    async def aclose(self) -> None:
        # curl_cffi Response.aclose() only awaits astream_task. On an early
        # exit it otherwise downloads the rest (up to 1800s) into its queue.
        # Cancelling the producer runs AsyncSession's registered cleanup and
        # returns its curl handle to the pool without closing the shared session.
        task = getattr(self._r, "astream_task", None) if self._from_stream else None
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return
        await self._r.aclose()


class _CurlClientShim:
    """把 curl_cffi AsyncSession 适配成 httpx.AsyncClient 的使用面（post/stream）。"""

    def __init__(self, session, post_timeout: float, stream_timeout: float):
        self._s = session
        self._post_timeout = post_timeout
        self._stream_timeout = stream_timeout

    async def post(self, url: str, *, headers=None, json=None):
        resp = await self._s.post(url, headers=headers, json=json,
                                  timeout=self._post_timeout)
        return _CurlRespShim(resp, from_stream=False)

    def stream(self, method: str, url: str, *, headers=None, json=None):
        return self._stream_ctx(method, url, headers=headers, json=json)

    @asynccontextmanager
    async def _stream_ctx(self, method: str, url: str, *, headers=None, json=None):
        # Own response closure: AsyncSession.stream() unconditionally awaits
        # the raw aclose(), which re-raises a cancelled producer on early exit.
        resp = await self._s.request(method, url, headers=headers, json=json,
                                     timeout=self._stream_timeout, stream=True)
        response = _CurlRespShim(resp, from_stream=True)
        try:
            yield response
        finally:
            await response.aclose()

    async def aclose(self) -> None:
        await self._s.close()


def _httpx_timeout_to_curl(timeout: Any) -> tuple:
    """httpx.Timeout / (connect, read, write, pool) 语义 → curl 总超时。

    curl_cffi 的 timeout 是整个请求的硬超时（CURLOPT_TIMEOUT_MS），没有
    "两块数据之间"的 read 超时概念，因此按用途拆两个值（2026-10 修复）：
      - 非流式：max(2×read, 60s)。旧实现统一 max(total*2, 1800) 把
        "2× read"意图恒吞掉，非流式 POST 也挂满 30 分钟，同一故障在
        伪装开关下的探测时间差 10 倍（httpx 180s vs curl 1800s）。
      - 流式：保留 1800s 硬下限（首回复之后可能有长思考停顿，放宽是有意
        行为，且外层 within_request_deadline 有同值总期限兑底）。
    """
    try:
        if isinstance(timeout, httpx.Timeout):
            total = max(timeout.connect or 0, timeout.read or 0, timeout.write or 0,
                        timeout.pool or 0)
            return max(total * 2, 60.0), max(total * 2, _CURL_TOTAL_TIMEOUT_SECONDS)
        if isinstance(timeout, (int, float)):
            v = float(timeout)
            return max(v, 60.0), max(v, _CURL_TOTAL_TIMEOUT_SECONDS)
    except Exception:
        pass
    return 60.0, _CURL_TOTAL_TIMEOUT_SECONDS


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
    from proxy_failover import FailoverHTTPClient
    from proxy_resolver import ProxyUsage
    # Probes/refresh jobs may run outside generation admission. Give every
    # context its own lease so pool eviction cannot close its active transport.
    usage = ProxyUsage()
    try:
        options = dict(client_kwargs)
        chain = options.pop("_proxy_chain", None)
        if chain is not None:
            def factory(raw, resolved):
                candidate = dict(options)
                candidate.pop("proxy", None)
                if resolved:
                    candidate["proxy"] = resolved
                return _upstream_client_pool.get(candidate, _create_upstream_client, usage=usage)
            yield FailoverHTTPClient(chain, factory)
            return
        client = _upstream_client_pool.get(options, _create_upstream_client, usage=usage)
        yield client
    finally:
        usage.close()


def _curl_remote_dns_proxy(proxy: str) -> str:
    """curl_cffi 路径下 socks5:// → socks5h://：SOCKS5 域名解析统一交给代理端。

    libcurl 对 socks5:// 的语义是**本机**先解析域名、再把解析出的 IP 交给代理；
    httpx（httpcore 的 SOCKS 实现）无论 socks5/socks5h 都把**域名**原样交给
    代理解析。面板「测试代理」走 httpx（远端解析）能通过，而 Cookie 通道开启
    浏览器伪装后走 curl_cffi（本机解析）：容器本地 DNS 对 Google 域名被污染
    或不可达时（典型：大陆部署环境），代理会收到一个连不上的 IP，回
    SOCKS5 reply(4) host unreachable（curl 97 ProxyError）——出现「测试通过
    但实战必失败」的口径差。统一改写为 socks5h 让两条路径行为一致；sing-box
    本地入站（socks5://127.0.0.1:port）同样受益——域名交给 sing-box，由
    远端出站节点解析，不再受本机解析结果影响。
    """
    if proxy.startswith("socks5://"):
        return "socks5h://" + proxy[len("socks5://"):]
    return proxy


def _create_upstream_client(client_kwargs: dict):
    profile = get_impersonate_profile()
    if profile == "off":
        options = dict(client_kwargs)
        if isinstance(options.get("proxy"), str):
            options["proxy"] = build_httpx_proxy(options["proxy"], verify=options.get("verify", True))
        return httpx.AsyncClient(**options)

    from curl_cffi.requests import AsyncSession

    proxy = client_kwargs.get("proxy")
    post_timeout, stream_timeout = _httpx_timeout_to_curl(client_kwargs.get("timeout"))
    session_kwargs: dict[str, Any] = {
        "impersonate": profile,
        # session 级默认取流式值；shim 的 post/stream 每次都显式传参覆盖它。
        "timeout": stream_timeout,
        "allow_redirects": bool(client_kwargs.get("follow_redirects", False)),
        "verify": client_kwargs.get("verify", True),
        "trust_env": False,
        "default_headers": True,
    }
    if proxy:
        session_kwargs["proxy"] = _curl_remote_dns_proxy(proxy)
    return _CurlClientShim(AsyncSession(**session_kwargs), post_timeout, stream_timeout)


# ============================================================
# Cookie 通道客户端池：签名 → 客户端，跨请求复用
# ============================================================
# 上限 16：签名含（伪装档案 × 解析后代理 URL × 超时形态）。多 Cookie 账号
# 各挂账号级代理 + 全局兑底时活跃组合轻松超过旧上限 4——每请求 LRU 淘汰
# 意味着新建 + 重新 TCP/TLS 握手（池化核心收益被抵消），每个被淘汰客户端
# 还要挂 7230s 延迟关闭任务。16 覆盖典型多账号部署且内存开销可忽略。
_UPSTREAM_POOL_MAX = 16


class _UpstreamClientPool:
    """（伪装档案, 代理, 超时, 重定向, 证书）→ 客户端，LRU 上限 _UPSTREAM_POOL_MAX。

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
            # verify 用可区分的规范形态：旧写法 str(False or "") == ""，
            # 与“未传 verify”（工厂默认 True）同签名——先到者决定池化客户端
            # 的 TLS 校验行为，后续调用方可能拿到与请求相反的校验配置。
            repr(client_kwargs.get("verify", True)),
        )

    def get(self, client_kwargs: dict, factory: Callable[[dict], Any], *, usage=None) -> Any:
        sig = self._signature(client_kwargs)
        with self._lock:
            hit = self._entries.get(sig)
            if hit is not None:
                # LRU 触碰：pop 再放回，dict 插入序 = 最近使用序
                self._entries.pop(sig)
                self._entries[sig] = hit
                return _closer.track(hit, usage)

        client = _closer.reclaim(("cookie", id(self), sig))
        _closer.ensure_capacity(int(len(self._entries) >= _UPSTREAM_POOL_MAX))
        if client is None:
            client = factory(client_kwargs)
        stale: list = []
        with self._lock:
            hit = self._entries.get(sig)
            if hit is not None:
                # 竞态窗口：别的协程先建好同签名客户端，用它
                stale.append((None, client))
                client = hit
            else:
                self._entries[sig] = client
                while len(self._entries) > _UPSTREAM_POOL_MAX:
                    oldest = next(iter(self._entries))
                    if oldest == sig:
                        break
                    stale.append((("cookie", id(self), oldest), self._entries.pop(oldest)))

        for retired_key, c in stale:
            schedule_graceful_close(_make_upstream_closer(c), key=retired_key, resource=c)
        return _closer.track(client, usage)

    async def aclose(self) -> None:
        with self._lock:
            clients = list(self._entries.values())
            self._entries.clear()
        for client in clients:
            await _make_upstream_closer(client)()


def _make_upstream_closer(client: Any) -> Callable[[], Any]:
    async def _aclose():
        try:
            await client.aclose()
        except Exception:
            pass

    return _aclose


_upstream_client_pool = _UpstreamClientPool()
