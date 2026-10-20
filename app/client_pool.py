"""上游 HTTP 客户端池化复用（Express SDK 通道 / Gemini 原生透传通道）。

为什么池化：旧实现每个请求都新建 genai.Client / httpx.AsyncClient，
请求结束后丢弃——每次都要与 aiplatform.googleapis.com 重新做
TCP + TLS 握手（多 100~300ms 延迟 + CPU），高并发下还会攒出大量
等待 GC 回收的未关闭连接池（fd 压力）。池化后同一（Key, 配置指纹）
的请求共享连接池，HTTP keep-alive 真正生效；重试之间也不再重复握手。

失效与回收：缓存签名包含全部会影响出站形态的设置（代理、自定义证书、
base_url、请求头指纹档案）。控制台热改任一项，新请求立即拿到新形态
客户端；旧客户端**延迟 _GRACE_CLOSE_SECONDS 再关闭**，避免把仍在
飞行中的长流式请求（上游 read 超时最长 600s，叠加重试更久）拦腰
打断。到点后同时关闭底层同步/异步 httpx 客户端，不留资源悬挂。

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
from proxy_resolver import resolve_proxy_uri
from express_fingerprint import get_express_ua_profile, get_express_ua_custom
from http_options import get_http_options

# SDK Client 池上限：Key 池轮换时最多同时保留这么多个（LRU 淘汰最久未用）
_SDK_POOL_MAX = 6
# 旧客户端延迟关闭的宽限期（秒）：覆盖最长流式 + 重试窗口
_GRACE_CLOSE_SECONDS = 7230.0  # longest supported request (7200s), including hot setting changes


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

    def ensure_capacity(self, count=1):
        if len(self._pending) + count > self._max_pending:
            raise PoolCapacityError("客户端配置切换过于频繁，连接正在回收，请稍后重试。")

    def reclaim(self, key):
        task = self._retired.pop(key, None)
        entry = self._pending.pop(task, None)
        if entry is None:
            return None
        task.cancel()
        return entry[2]

    def schedule(self, close: Callable[[], Any], *, key=None, resource=None) -> None:
        # 容量不足时直接关闭而非抛错：淘汰中的旧客户端（同 Key 换签名 + LRU
        # 可一次产生 2 个 stale）已从池字典弹出，若在 schedule 内抛
        # PoolCapacityError，这些客户端既不在池也不在 pending——连接/fd
        # 只能等 GC 兜底（2026-10 修复）。直接关闭对调用方无感：延迟回收
        # 只是优化，立即回收不会影响活跃请求（被淘汰者已无引用）。
        # get() 入口的 ensure_capacity 预检仍保留 503 语义，拦截高频换签名的场景。
        try:
            self.ensure_capacity()
        except PoolCapacityError:
            result = close()
            if inspect.isawaitable(result):
                task = asyncio.get_running_loop().create_task(result)
                task.add_done_callback(lambda _: None)
            if key is not None:
                self._retired.pop(key, None)
            return

        async def _close_later():
            from resumable import request_timeout_seconds
            await asyncio.sleep(max(_GRACE_CLOSE_SECONDS, request_timeout_seconds() + 30))
            entry = self._pending.pop(asyncio.current_task(), None)
            if entry is None:
                return
            if key is not None and self._retired.get(key) is asyncio.current_task():
                self._retired.pop(key, None)
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass

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
                return entry[1]

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
            if entry is not None:            # 同 Key 旧签名（或竞态里先建好的）
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
        return client

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
                return hit

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
        return client

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
