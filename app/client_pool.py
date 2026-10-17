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
import threading
from typing import Any, Callable, Optional, Tuple

from google import genai

import config as app_config
from runtime_state import get_effective_proxy_url
from express_fingerprint import get_express_ua_profile, get_express_ua_custom
from http_options import get_http_options

# SDK Client 池上限：Key 池轮换时最多同时保留这么多个（LRU 淘汰最久未用）
_SDK_POOL_MAX = 6
# 旧客户端延迟关闭的宽限期（秒）：覆盖最长流式 + 重试窗口
_GRACE_CLOSE_SECONDS = 900.0


def _sdk_signature() -> tuple:
    """影响 genai.Client 出站形态的全部设置（任一变化 = 必须重建）。

    与 http_options.get_http_options() 消费的输入一一对应：
    代理/证书 → client_args；base_url；指纹档案+自定义串 → headers。
    """
    return (
        "v1",
        get_effective_proxy_url(),
        app_config.SSL_CERT_FILE or "",
        app_config.VERTEX_BASE_URL or "",
        get_express_ua_profile(),
        get_express_ua_custom(),
    )


def _direct_signature() -> tuple:
    """影响裸 httpx 客户端形态的设置（代理 / 自定义证书）。

    Gemini 透传的请求头指纹（express_ua_profile）是**逐请求**注入的
    （build_request headers），不进客户端构造参数，故不在签名内。
    """
    return ("v1", get_effective_proxy_url(), app_config.SSL_CERT_FILE or "")


class _GracefulCloser:
    """延迟关闭被淘汰的客户端：先等宽限期（护住飞行中的请求）再关。"""

    def __init__(self):
        self._tasks: set = set()

    def schedule(self, close: Callable[[], Any]) -> None:
        async def _close_later():
            await asyncio.sleep(_GRACE_CLOSE_SECONDS)
            try:
                result = close()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass  # 关闭失败只能交给 GC（与历史行为一致）

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # 不在事件循环里（理论不可达，仅防御）：交给 GC
        task = loop.create_task(_close_later())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


_closer = _GracefulCloser()


class SdkClientPool:
    """genai.Client 池：api_key → (签名, Client)，LRU 上限 _SDK_POOL_MAX。"""

    def __init__(self, maxsize: int = _SDK_POOL_MAX):
        self._lock = threading.Lock()
        self._maxsize = max(1, int(maxsize))
        self._clients: dict[str, tuple[tuple, Any]] = {}

    def get(self, api_key: str) -> Any:
        """取（或建）该 Key 的池化 Client。只允许在事件循环内调用。

        调用方**不要关闭**返回的 Client——它的生命周期由池管理
        （签名变化 / LRU 淘汰时延迟统一回收）。
        """
        api_key = api_key or ""
        sig = _sdk_signature()
        with self._lock:
            entry = self._clients.get(api_key)
            if entry is not None and entry[0] == sig:
                # LRU 触碰：pop 再放回，保持 dict 插入序 = 最近使用序
                self._clients.pop(api_key)
                self._clients[api_key] = entry
                return entry[1]

        client = genai.Client(
            vertexai=True,
            api_key=api_key,
            http_options=get_http_options(),
        )

        stale: list = []
        with self._lock:
            entry = self._clients.get(api_key)
            if entry is not None:            # 同 Key 旧签名（或竞态里先建好的）
                stale.append(entry[1])
            self._clients[api_key] = (sig, client)
            while len(self._clients) > self._maxsize:
                oldest = next(iter(self._clients))
                if oldest == api_key:
                    break
                stale.append(self._clients.pop(oldest)[1])

        for c in stale:
            _closer.schedule(_make_sdk_closer(c))
        return client

    def size(self) -> int:
        with self._lock:
            return len(self._clients)


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


class HttpxClientSlot:
    """单槽 httpx.AsyncClient 池（签名变化即整体换新）。

    Gemini 原生透传通道同一时刻只需要一个客户端形态（签名只含
    代理/证书），用单槽 + 原子换新即可，不需要按 Key 分桶——
    Express Key 走的是逐请求 header，不绑客户端。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._entry: Optional[Tuple[tuple, Any]] = None

    def get(self, factory: Callable[[], Any]) -> Any:
        """取（或建）当前签名对应的共享客户端。调用方**不要关闭**它。"""
        sig = _direct_signature()
        with self._lock:
            if self._entry is not None and self._entry[0] == sig:
                return self._entry[1]

        client = factory()
        with self._lock:
            if self._entry is not None and self._entry[0] == sig:
                # 竞态窗口：别的协程刚建好同签名客户端，用它；
                # 我们手里这个没人用过，直接进宽限关闭。
                stale, client = client, self._entry[1]
            else:
                stale = self._entry[1] if self._entry is not None else None
                self._entry = (sig, client)

        if stale is not None:
            _closer.schedule(_make_httpx_closer(stale))
        return client


def _make_httpx_closer(client: Any) -> Callable[[], Any]:
    async def _aclose():
        try:
            await client.aclose()
        except Exception:
            pass

    return _aclose


def schedule_graceful_close(close: Callable[[], Any]) -> None:
    """公开的延迟关闭调度（供 impersonate.py 的 Cookie 通道客户端池复用）。"""
    _closer.schedule(close)


# ---------- 模块级单例（全进程共享） ----------
express_sdk_pool = SdkClientPool()
passthrough_client_slot = HttpxClientSlot()


def get_pooled_express_client(api_key: str) -> Any:
    """Express SDK 通道统一入口：express_sdk / fallback 工厂都用它。"""
    return express_sdk_pool.get(api_key)
