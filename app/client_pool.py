"""上游 HTTP 客户端池化复用（Express SDK 通道 / Gemini 原生透传通道）。

为什么池化：旧实现每个请求都新建 genai.Client / httpx.AsyncClient，
请求结束后丢弃——每次都要与 aiplatform.googleapis.com 重新做
TCP + TLS 握手（多 100~300ms 延迟 + CPU），高并发下还会攒出大量
等待 GC 回收的未关闭连接池（fd 压力）。池化后同一（Key, 配置指纹）
的请求共享连接池，HTTP keep-alive 真正生效；重试之间也不再重复握手。

失效与回收：缓存签名包含全部会影响出站形态的设置（代理、自定义证书、
base_url、请求头指纹档案）。控制台热改任一项，新请求立即拿到新形态
客户端；旧客户端进入可复用的空闲缓存。请求作用域持有客户端引用直到
响应结束或流关闭；仅空闲客户端可被容量淘汰或超时回收。

并发安全（跨请求复用同一个 genai.Client）：
- client.aio.models.* 底层共享的 httpx.AsyncClient 本身就是并发安全
  （连接池语义）；
- SDK 内部的 tenacity.AsyncRetrying 每次调用使用本地 RetryCallState，
  跨调用共享的只有 statistics 观测字段（仅计数，不影响重试逻辑）；
- Google 官方示例同样是单 Client 全局复用。
"""

import asyncio
import inspect
import threading
from typing import Any, Callable, Optional

from google import genai

import config as app_config
from runtime_state import get_effective_proxy_url
from proxy_resolver import resolve_proxy_uri, current_proxy_usage
from express_fingerprint import get_express_ua_profile, get_express_ua_custom
from http_options import get_http_options

# SDK Client 池上限：Key 池轮换时最多同时保留这么多个（LRU 淘汰最久未用）
_SDK_POOL_MAX = 6
# 空闲客户端复用窗口；活跃请求由显式引用保护，不受此时限影响
_GRACE_CLOSE_SECONDS = 60.0  # idle reuse window; active leases never expire


def _resolve_proxy_for_signature(key_proxy: Optional[str]) -> str:
    """把（可能为 Key 级的）代理原始 URI 解析成 httpx 可用的 URL。

    key_proxy=None → 跟随全局；"" → 直连；其余 = 该 URI。
    vless/vmess 等分享链接在此转成本地 socks5（惰性启动 sing-box，
    之后纯缓存查找）。同一 URI 在进程生命周期内端口稳定，
    因此签名不会因重复解析而抖动。
    解析失败（sing-box 缺失/链接非法）抛 ProxyResolveError，
    由调用方转成明确的 5xx 响应——绝不静默直连。
    """
    raw = get_effective_proxy_url() if key_proxy is None else str(key_proxy or "").strip()
    return resolve_proxy_uri(raw)


def _sdk_signature(key_proxy: Optional[str] = None) -> tuple:
    """影响 genai.Client 出站形态的全部设置（任一变化 = 必须重建）。

    与 http_options.get_http_options() 消费的输入一一对应：
    代理（解析后 URL，含 Key 级代理）/ 证书 → client_args；
    base_url；指纹档案+自定义串 → headers。
    """
    return (
        "v2",
        _resolve_proxy_for_signature(key_proxy),
        app_config.SSL_CERT_FILE or "",
        app_config.VERTEX_BASE_URL or "",
        get_express_ua_profile(),
        get_express_ua_custom(),
    )


def _direct_signature(proxy_uri: Optional[str] = None) -> tuple:
    """影响裸 httpx 客户端形态的设置（代理 / 自定义证书）。

    Gemini 透传的请求头指纹（express_ua_profile）是**逐请求**注入的
    （build_request headers），不进客户端构造参数，故不在签名内。
    """
    return ("v2", _resolve_proxy_for_signature(proxy_uri), app_config.SSL_CERT_FILE or "")


class PoolCapacityError(RuntimeError):
    code = 503


class _GracefulCloser:
    """Retain retired clients for reuse; never force-close an active client."""

    def __init__(self, max_pending: int = 256):
        self._tasks: set = set()
        self._pending: dict = {}
        self._retired: dict = {}
        self._max_pending = max_pending
        self._users = {}
        self._users_lock = threading.Lock()

    def track(self, resource, usage=None):
        # Generation admission owns this scope through stream EOF/cancellation.
        usage = usage if usage is not None else current_proxy_usage.get()
        if usage is not None:
            usage.include(self, id(resource))
        return resource

    def retain(self, ident, *, temporary=False):
        with self._users_lock:
            self._users[ident] = self._users.get(ident, 0) + 1

    def release(self, ident):
        with self._users_lock:
            count = self._users.get(ident, 0) - 1
            if count > 0:
                self._users[ident] = count
            else:
                self._users.pop(ident, None)

    def discard_unused(self, ident):
        pass  # Temporary probe scopes use the same idle cache as generations.

    def _active(self, resource):
        with self._users_lock:
            return resource is None or self._users.get(id(resource), 0) > 0

    def _detach(self, task):
        entry = self._pending.pop(task, None)
        if entry is not None and self._retired.get(entry[1]) is task:
            self._retired.pop(entry[1], None)
        return entry

    async def _close(self, close):
        try:
            result = close()
            if inspect.isawaitable(result):
                await result
        except Exception:
            pass

    def ensure_capacity(self, count=1):
        # Discard the oldest idle retired clients before refusing active churn.
        if len(self._pending) + count <= self._max_pending:
            return
        for task, (close, _key, resource) in list(self._pending.items()):
            if len(self._pending) + count <= self._max_pending:
                return
            if self._active(resource):
                continue
            self._detach(task)
            task.cancel()
            closing = asyncio.get_running_loop().create_task(self._close(close))
            self._tasks.add(closing)
            closing.add_done_callback(self._tasks.discard)
        if len(self._pending) + count > self._max_pending:
            raise PoolCapacityError("活跃请求占满连接回收队列，请稍后重试。")

    def reclaim(self, key):
        task = self._retired.pop(key, None)
        entry = self._pending.pop(task, None)
        if entry is None:
            return None
        task.cancel()
        return entry[2]

    def schedule(self, close: Callable[[], Any], *, key=None, resource=None) -> None:
        self.ensure_capacity()

        async def _close_later():
            while True:
                await asyncio.sleep(_GRACE_CLOSE_SECONDS)
                if resource is not None and self._active(resource):
                    continue
                entry = self._detach(asyncio.current_task())
                if entry is not None:
                    await self._close(close)
                return

        loop = asyncio.get_running_loop()
        task = loop.create_task(_close_later())
        self._tasks.add(task)
        self._pending[task] = (close, key, resource)
        if key is not None:
            self._retired[key] = task
        task.add_done_callback(self._tasks.discard)

    async def aclose(self) -> None:
        pending = list(self._pending.items())
        self._pending.clear()
        self._retired.clear()
        tasks = list(self._tasks)
        for task, _ in pending:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for _, (close, _key, _resource) in pending:
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass


_closer = _GracefulCloser()


class SdkClientPool:
    """genai.Client 池：api_key → (签名, Client)，LRU 上限 _SDK_POOL_MAX。"""

    def __init__(self, maxsize: int = _SDK_POOL_MAX):
        self._lock = threading.Lock()
        self._maxsize = max(1, int(maxsize))
        self._clients: dict[str, tuple[tuple, Any]] = {}

    def get(self, api_key: str, key_proxy: Optional[str] = None) -> Any:
        """取（或建）该 Key 的池化 Client。只允许在事件循环内调用。

        key_proxy：本次调用使用的出站代理**原始 URI**（Key 级优先/全局
        兑底的裁决由调用方完成，见 runtime_state.get_effective_proxy_uri_for_key）。
        None = 跟随全局；"" = 直连；其余 = 该 URI（vless:// 等分享链接
        在签名/构造时统一转本地 socks5）。

        调用方**不要关闭**返回的 Client——它的生命周期由池管理
        （签名变化 / LRU 淘汰时延迟统一回收）。
        """
        api_key = api_key or ""
        sig = _sdk_signature(key_proxy)
        with self._lock:
            entry = self._clients.get(api_key)
            if entry is not None and entry[0] == sig:
                # LRU 触碰：pop 再放回，保持 dict 插入序 = 最近使用序
                self._clients.pop(api_key)
                self._clients[api_key] = entry
                return _closer.track(entry[1])

        retired_key = ("sdk", id(self), api_key, sig)
        client = _closer.reclaim(retired_key)
        _closer.ensure_capacity(int(entry is not None or len(self._clients) >= self._maxsize))
        if client is None:
            client = genai.Client(
                vertexai=True, api_key=api_key,
                http_options=get_http_options(proxy_url=key_proxy))

        stale: list = []
        with self._lock:
            entry = self._clients.get(api_key)
            if entry is not None and entry[0] == sig:
                # 同签名竞态败者：复用先到协程刚登记的客户端，退役自己的副本。
                stale.append((None, client))
                client = entry[1]
            else:
                if entry is not None:      # 同 Key 旧签名
                    stale.append((("sdk", id(self), api_key, entry[0]), entry[1]))
                self._clients[api_key] = (sig, client)
            while len(self._clients) > self._maxsize:
                oldest = next(iter(self._clients))
                if oldest == api_key:
                    break
                old_sig, old_client = self._clients.pop(oldest)
                stale.append((("sdk", id(self), oldest, old_sig), old_client))

        for retired_key, c in stale:
            _closer.schedule(_make_sdk_closer(c), key=retired_key, resource=c)
        return _closer.track(client)

    def size(self) -> int:
        with self._lock:
            return len(self._clients)

    async def aclose(self) -> None:
        with self._lock:
            clients = [entry[1] for entry in self._clients.values()]
            self._clients.clear()
        for client in clients:
            await _make_sdk_closer(client)()


def _make_sdk_closer(client: Any) -> Callable[[], Any]:
    """genai.Client 的完整关闭：同步 + 异步底层 httpx 客户端都关。"""

    def _close():
        try:
            client.close()                    # 同步 httpx 客户端（本服务只用 aio，池为空，关起来很快）
        except Exception:
            pass

        async def _aclose():
            try:
                await client.aio.aclose()     # 异步 httpx 客户端（真正承载流量的那个）
            except Exception:
                pass

        return _aclose()

    return _close


class HttpxClientPool:
    """httpx.AsyncClient 池（签名 → 客户端），LRU 上限 6。

    Gemini 原生透传通道已支持 **Key 级代理**：不同 Key 可能挂不同出口，
    同一时刻需要的客户端形态不再唯一，单槽换新会造成频繁重建——
    改为按（解析后代理, 证书）分桶 LRU。签名变化（控制台热改代理 /
    换 Key 换代理）时旧客户端延迟回收，不打断飞行中的流。
    """

    def __init__(self, maxsize: int = 6):
        self._lock = threading.Lock()
        self._maxsize = max(1, int(maxsize))
        self._entries: dict[tuple, Any] = {}

    def get(self, factory: Callable[[], Any], proxy_uri: Optional[str] = None) -> Any:
        """取（或建）当前签名对应的共享客户端。调用方**不要关闭**它。

        proxy_uri：该次调用的代理原始 URI（None=跟随全局，""=直连）。
        factory 收到的 client_kwargs 已由调用方按同一代理构造。
        """
        sig = _direct_signature(proxy_uri)
        with self._lock:
            hit = self._entries.get(sig)
            if hit is not None:
                # LRU 触碰：pop 再放回，dict 插入序 = 最近使用序
                self._entries.pop(sig)
                self._entries[sig] = hit
                return _closer.track(hit)

        client = _closer.reclaim(("httpx", id(self), sig))
        _closer.ensure_capacity(int(len(self._entries) >= self._maxsize))
        if client is None:
            client = factory()
        stale: list = []
        with self._lock:
            hit = self._entries.get(sig)
            if hit is not None:
                # 竞态窗口：别的协程刚建好同签名客户端，用它；
                # 我们手里这个没人用过，直接进宽限关闭。
                stale.append((None, client))
                client = hit
            else:
                self._entries[sig] = client
                while len(self._entries) > self._maxsize:
                    oldest = next(iter(self._entries))
                    if oldest == sig:
                        break
                    stale.append((("httpx", id(self), oldest), self._entries.pop(oldest)))

        for retired_key, c in stale:
            _closer.schedule(_make_httpx_closer(c), key=retired_key, resource=c)
        return _closer.track(client)

    async def aclose(self) -> None:
        with self._lock:
            clients = [c for c in self._entries.values()]
            self._entries.clear()
        for client in clients:
            await _make_httpx_closer(client)()


def _make_httpx_closer(client: Any) -> Callable[[], Any]:
    async def _aclose():
        try:
            await client.aclose()
        except Exception:
            pass

    return _aclose


def schedule_graceful_close(close: Callable[[], Any], *, key=None, resource=None) -> None:
    """公开的延迟关闭调度（供 impersonate.py 的 Cookie 通道客户端池复用）。"""
    _closer.schedule(close, key=key, resource=resource)


# ---------- 模块级单例（全进程共享） ----------
express_sdk_pool = SdkClientPool()
passthrough_client_pool = HttpxClientPool()


def get_pooled_express_client(api_key: str, key_proxy: Optional[str] = None) -> Any:
    """Express SDK 通道统一入口：express_sdk / fallback 工厂都用它。

    key_proxy：本次调用的出站代理原始 URI（None=跟随全局，""=直连）。
    调用方用 runtime_state.get_effective_proxy_uri_for_key(key_record)
    完成「Key 级优先 / direct / 全局兑底」的裁决后再传入。
    解析失败（如 sing-box 缺失）抛 ProxyResolveError。
    """
    return express_sdk_pool.get(api_key, key_proxy)


async def close_all_clients() -> None:
    """在事件循环结束前释放当前连接池与延迟回收队列。"""
    from impersonate import _upstream_client_pool

    await asyncio.gather(express_sdk_pool.aclose(), passthrough_client_pool.aclose(),
                         _upstream_client_pool.aclose())
    await _closer.aclose()
