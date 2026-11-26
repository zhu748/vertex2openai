"""Fail-fast generation admission; detached streams keep their slot until EOF."""
from functools import wraps
import asyncio
import threading

import anyio
from fastapi.responses import StreamingResponse
from runtime_state import app_state
from proxy_resolver import ProxyUsage, current_proxy_usage
from request_tracking import PayloadObserver, current_trace
from stream_cleanup import await_owned_close

# setup 任务的「快窗口」：窗口内完成的异常/错误响应保留真实 HTTP 状态码
# （middleware 能看到），窗口后才建立 SSE 连接（chat_api._resumable_stream
# 与 gemini_native 透传续传共用同一预算；2026-11 审查收敛：此前三处硬编码）。
SETUP_FAST_WINDOW_SECONDS = 0.05


def count_slow_setup_failure(task: asyncio.Task, *, on_error) -> None:
    """慢路径 setup 失败补记一次 on_error()（通常传 stats.add_error）。

    HTTP 200 + SSE 已发出后，后台 setup 稍后失败只会以错误帧进续传缓冲，
    middleware 按状态码统计不到——大盘上 total ≠ success + error 永久漂移。
    快路径（窗口内完成）由 result() re-raise / 真实状态码走 middleware 计数，
    本回调只补慢路径且每个 setup 任务只挂一次；取消（淘汰/停机）不计失败。
    （2026-11 审查收敛：chat_api 与 gemini_native 曾各自复制同一闭包，
    注释互指对方为「同一回调模式」，现为单一实现。on_error 在挂载时绑定，
    保证测试对 stats 的 mock 替换对回调同样生效。）
    """
    if task.cancelled():
        return  # 取消（淘汰/停机）不算失败
    exc = None
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    if exc is not None or not isinstance(task.result(), StreamingResponse):
        on_error()


class GenerationCapacityError(RuntimeError):
    code = 503


class GenerationLimiter:
    """计数控并发闸：满时快速失败（503），不做队列等待。

    参数化（2026-11 第二轮）：除全局 upstream_max_concurrency 外，生图
    另有独立的 image_limiter（image_max_concurrency，默认 1）——4K 生图
    单次峰值 RSS +150~200MB，两路并发叠加即可 OOMKill 512MB 容器，
    需要比文本流紧得多的闸门。
    """

    def __init__(
        self,
        *,
        setting_key: str = "upstream_max_concurrency",
        default_limit: int = 32,
        full_message: str = "上游生成并发已满，请稍后重试。",
        traced: bool = True,
    ):
        self.setting_key = setting_key
        self.default_limit = default_limit
        self.full_message = full_message
        # traced=False：纯计数闸（生图内存闸与全局闸同请求叠加时，里程碑/
        # start_generation/end_generation 只由全局闸记一次，避免重复）。
        self.traced = traced
        self.active = 0
        self._lock = threading.Lock()

    def reserve(self):
        trace = current_trace.get() if self.traced else None
        with self._lock:
            # limit 的读取必须在临界区内：控制台热调 upstream_max_concurrency
            # 的瞬间，锁外读取会让并发 reserve 沿用旧值（TOCTOU，2026-10 修复）。
            limit = max(1, int(app_state.get_committed_setting(self.setting_key, self.default_limit)))
            if self.active >= limit:
                raise GenerationCapacityError(self.full_message)
            self.active += 1
        try:
            if trace:
                trace.start_generation()
        except BaseException:
            # 锁外尾随动作抛异常（trace 是诊断层，不应拖垮准入）：Lease 尚未
            # 创建，调用方拿不到 release 入口——不回滚的话该并发槽永久卡死
            #（无 TTL 兑底，只能重启，2026-10 补齐）。
            with self._lock:
                self.active -= 1
            raise
        limiter = self

        class Lease:
            def __init__(self):
                self.released = False
                self.usage = ProxyUsage()
                self.trace = trace

            def release(self, outcome="succeeded", message=None):
                with limiter._lock:
                    if self.released:
                        return
                    self.released = True
                    limiter.active -= 1
                self.usage.close()
                if self.trace:
                    self.trace.end_generation(outcome, message)
        return Lease()


generation_limiter = GenerationLimiter()

# 生图专用并发闸（内存闸门）：与全局闸独立计数、同请求叠加生效——
# 一路生图同时占用一个全局名额 + 一个生图名额。traced=False：trace
# 里程碑由全局闸统一记录，这里只管名额（双层 LeasedIterator 也不会
# 双计 payload 观测）。
image_limiter = GenerationLimiter(
    setting_key="image_max_concurrency",
    default_limit=1,
    full_message="生图并发已达上限（image_max_concurrency，控制台可调）；"
    "4K 生图内存占用大，请等当前生成完成后再试。",
    traced=False,
)


class LeasedIterator:
    def __init__(self, source, lease, *, sse=True, observe=True, owner=None):
        self.source = source.__aiter__()
        self.lease = lease
        self.owner = owner
        if owner is not None:
            owner.leases.append(lease)
        # An auxiliary memory gate owns no trace counters, but its upstream
        # iterator still runs in the request's diagnostic context. Capture it
        # here so reading/closing from another task preserves that identity.
        self.trace = lease.trace if lease.trace is not None else current_trace.get()
        # 双层租约（全局闸 + 生图闸）同流包装时只让最外层记账，
        # 否则 sent_frames/bytes 会被双计。
        self.observer = (
            PayloadObserver(lease.trace, sse=sse) if (lease.trace and observe) else None
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        token = current_proxy_usage.set(self.lease.usage)
        trace_token = current_trace.set(self.trace)
        try:
            chunk = await anext(self.source)
            if self.observer:
                self.observer.feed(chunk)
            return chunk
        except StopAsyncIteration:
            if self.observer:
                self.observer.feed(b"", final=True)
            await self._finish("succeeded")
            raise
        except BaseException as exc:
            await self._finish("cancelled" if isinstance(exc, (GeneratorExit, asyncio.CancelledError)) else "error", str(exc))
            raise
        finally:
            current_trace.reset(trace_token)
            current_proxy_usage.reset(token)

    async def _finish(self, outcome, message=None):
        if self.owner is None:
            self.lease.release(outcome, message)
        else:
            await self.owner.finish(outcome, message)

    async def aclose(self):
        await self._close("cancelled")

    async def aclose_terminal(self, outcome):
        """A protocol-level terminal is conclusive without reading through to EOF."""
        await self._close(outcome)

    async def _close(self, outcome):
        token = current_trace.set(self.trace)
        proxy_token = current_proxy_usage.set(self.lease.usage)
        try:
            if outcome != "cancelled" and self.observer:
                self.observer.feed(b"", final=True)
            if isinstance(self.source, LeasedIterator):
                await self.source._close(outcome)
            elif self.owner is None and (close := getattr(self.source, "aclose", None)):
                await close()
        finally:
            try:
                await self._finish(outcome)
            finally:
                current_proxy_usage.reset(proxy_token)
                current_trace.reset(token)


class _StreamCleanup:
    """One owner for the original body, its extra transport cleanup and attached leases.

    A body can be drained or closed without calling the ASGI response (e.g. a
    replay pump). Conversely, a send can fail before the first pull. Both paths
    join the same close; cancellation of a waiter cannot release a lease early.
    """
    def __init__(self, body, cleanup):
        self.body = body
        self.cleanup = cleanup
        self.task = None
        self.leases = []
        self.result = None

    async def _close(self):
        try:
            try:
                if close := getattr(self.body, "aclose", None):
                    await close()
            finally:
                await self.cleanup()
        finally:
            for lease in self.leases:
                lease.release(*(self.result or ("cancelled", None)))
            self.leases.clear()

    async def aclose(self):
        if self.task is None:
            self.task = asyncio.create_task(self._close())
            # A cancelled waiter may be the last one. Always retrieve the error.
            self.task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        with anyio.CancelScope(shield=True):
            await asyncio.shield(self.task)

    async def finish(self, outcome, message):
        if self.result is None:
            self.result = (outcome, message)
        await self.aclose()


class ManagedStreamingResponse(StreamingResponse):
    def __init__(self, *args, cleanup=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleanup = cleanup
        self.cleanup_owner = None

    # Bound only by direct routes: detached pumps iterate the body independently.
    client_connection = None

    async def stream_response(self, send):
        connection = self.client_connection
        if connection is None:
            return await super().stream_response(send)
        # ASGI >=2.4 normally stops on a failed send. A quiet upstream might
        # never send again; some servers also silently discard writes. Reuse
        # the middleware's physical disconnect notification, not raw receive.
        owner = asyncio.current_task()
        cancellations = owner.cancelling()
        with anyio.CancelScope() as scope:
            def stop(gone):
                if gone:
                    scope.cancel()
            connection.listen(stop)
            try:
                if not connection.disconnected:
                    await super().stream_response(send)
            finally:
                if stop in connection.listeners:
                    connection.listeners.remove(stop)
        # AnyIO withdraws its own cancellation on exit. An independent server
        # cancellation during shielded transport cleanup must still propagate.
        if owner.cancelling() > cancellations:
            raise asyncio.CancelledError()

    def admission_owner(self):
        if self.cleanup is not None and self.cleanup_owner is None:
            self.cleanup_owner = _StreamCleanup(self.body_iterator, self.cleanup)
        return self.cleanup_owner

    async def aclose(self):
        with anyio.CancelScope(shield=True):
            try:
                if close := getattr(self.body_iterator, "aclose", None):
                    await close()
            finally:
                # Unleased response: the existing ASGI/detached caller owns this
                # inline cleanup. Leased iterators instead share their close owner.
                if self.cleanup is not None and self.cleanup_owner is None:
                    await self.cleanup()

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            if self.client_connection is not None and self.client_connection.disconnected:
                # A direct sender can already be unwinding due to peer loss.
                # Let a second, independent server cancellation interrupt this
                # waiter, not an adapter's best-effort physical close.
                await await_owned_close(self.aclose())
            else:
                await self.aclose()


def _lease_body(response, lease, *, observe=True):
    owner = response.admission_owner() if isinstance(response, ManagedStreamingResponse) else None
    return LeasedIterator(response.body_iterator, lease, owner=owner, observe=observe,
                          sse="text/event-stream" in response.headers.get("content-type", ""))


async def run_with_image_limit(model, operation):
    """Share the image memory gate across all protocols and upstream channels."""
    from model_capabilities import get_profile

    if not get_profile(model)["is_image"]:
        return await operation()
    lease = image_limiter.reserve()
    try:
        response = await operation()
    except BaseException as exc:
        lease.release("cancelled" if isinstance(exc, (asyncio.CancelledError, GeneratorExit)) else "error", str(exc))
        raise
    if isinstance(response, StreamingResponse):
        response.body_iterator = _lease_body(response, lease, observe=False)
    else:
        lease.release("succeeded" if response.status_code < 400 else "error")
    return response


def limit_generation(function):
    @wraps(function)
    async def guarded(*args, **kwargs):
        lease = generation_limiter.reserve()
        token = current_proxy_usage.set(lease.usage)
        try:
            response = await function(*args, **kwargs)
        except BaseException as exc:
            lease.release("cancelled" if isinstance(exc, asyncio.CancelledError) else "error", str(exc))
            raise
        finally:
            current_proxy_usage.reset(token)
        if not isinstance(response, StreamingResponse):
            if lease.trace:
                observer = PayloadObserver(lease.trace, sse=False)
                observer.feed(getattr(response, "body", b""), final=True)
                if response.status_code >= 400:
                    lease.trace.fail(f"上游返回 HTTP {response.status_code}", response.status_code)
            lease.release("succeeded" if response.status_code < 400 else "error")
            return response
        iterator = _lease_body(response, lease)
        if hasattr(response, "aclose"):
            response.body_iterator = iterator
            return response
        return ManagedStreamingResponse(iterator, status_code=response.status_code,
                                        headers=dict(response.headers), background=response.background,
                                        media_type=getattr(response, "media_type", None))
    return guarded
