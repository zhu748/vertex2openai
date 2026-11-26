"""Per-attempt deadline until real model output, independent of SSE keepalives."""
import asyncio
import inspect
import math
import time
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from typing import Any, AsyncIterable, AsyncIterator, Awaitable, Callable, Optional

import anyio

from stream_cleanup import AttemptCancellation, await_owned_close

import config as app_config
from runtime_state import app_state
from request_tracking import current_trace
from response_validation import IncompleteResponseError, native_part_has_reply, sdk_function_call, sdk_media_payload

_active_wait = ContextVar("first_reply_wait", default=None)

# 整调用（非流式 / 假流式 / 生图整取）不参与首回复计时（2026-11 语义修正）：
# 首回复窗口的本职是「真流式下检测上游是否开始吐有效分片」——思考、
# 正文、工具调用、图片任一到达即算回复，此后不再受窗口约束。整取路径
# 要等完整响应才有“首回复”可言，把窗口套上去等于用 15s 卡整段生成
# 时间：慢思考/长输出模型必然 FirstReplyTimeout → 被判可重试 → 同一请求
# 原样重发仍超不过窗口 → retry_max 次空烧配额后 504，且路径由服务端强制
# 选择（假流式开关/生图保护），客户端无法通过改请求规避。旧版仅对生图
# 模型放宽到 max(窗口, 300s) 的补丁（WHOLE_CALL_MIN_SECONDS）由本规则
# 整体取代并移除。整取路径的挂起检测兜底：传输层读超时（Express httpx
# read=600s / Cookie read=180s）+ 整轮 request_timeout_seconds（默认
# 1800s）+ 断连检测照常生效。


def _check_active_budget(guard: Optional["FirstReplyGuard"] = None) -> None:
    active = _active_wait.get()
    if active is None:
        return
    current, owner, cancellations = active
    if guard is not None and guard is not current:
        return
    if owner.cancelling() > cancellations:
        # A transport may translate CancelledError to a network error. Do not
        # open a second route or mark first output when cancellation is pending.
        raise asyncio.CancelledError()
    if current.seconds and not current.ready and time.monotonic() >= current.deadline:
        raise current._timeout_error()


async def check_private_retry() -> None:
    """A nested retry is still owned by the private read that produced its trigger."""
    await anyio.lowlevel.checkpoint_if_cancelled()
    _check_active_budget()


@asynccontextmanager
async def proxy_attempt(enabled: bool) -> AsyncIterator[None]:
    """Each candidate shares the outer wait's deadline; never start one after expiry."""
    if enabled:
        await check_private_retry()
    yield


class FirstReplyTimeout(TimeoutError):
    code = 504

    def __init__(self, message: str, *, seconds: Optional[float] = None) -> None:
        super().__init__(message)
        # 携带触发超时的窗口秒数，供重试日志与请求监控面板展示窗口口径。
        self.seconds = seconds


def timeout_seconds(model: str) -> float:
    # 兜底口径与 DEFAULT_SETTINGS 一致（0=关闭，2026-11-12 用户决策）：
    # 缺键、非有限数或类型错误的存量损坏值一律回落关闭，不再复活旧的 15 秒默认。
    try:
        value = float(app_state.get_effective_settings(model).get("first_reply_timeout_seconds", 0))
        return max(0.0, min(app_config.MAX_TIMEOUT_SECONDS, value)) if math.isfinite(value) else 0.0
    except (TypeError, ValueError, OverflowError):
        return 0.0


async def _close_unused(value: Any) -> None:
    """Close rejected coroutine objects, generators or HTTP responses if owned."""
    close = getattr(value, "aclose", None) or getattr(value, "close", None)
    if close:
        # Keep the deadline (or external cancellation) as the primary outcome.
        with suppress(Exception):
            result = close()
            if inspect.isawaitable(result):
                await await_owned_close(result)


async def _discard_unstarted(operation: Awaitable) -> None:
    if not asyncio.isfuture(operation):
        await _close_unused(operation)
        return
    # A Task/Future is already scheduled and can own a resource even if its
    # cancellation is suppressed, or it finished before this first await.
    owner = asyncio.current_task()
    cancellations = owner.cancelling()
    operation.cancel()
    try:
        result = await operation
    except (asyncio.CancelledError, Exception):
        pass
    else:
        await _close_unused(result)
    if owner.cancelling() > cancellations:
        raise asyncio.CancelledError()


class FirstReplyGuard:
    def __init__(self, model: str, *, seconds: Optional[float] = None) -> None:
        # seconds：调用方预先算好的窗口。同一请求的重试循环共享一次设置
        # 读取——旧实现每次尝试都新建 Guard，每次都深拷贝整棵设置树
        # （含全部 model_overrides），重试风暴下是纯浪费的热路径开销。
        self.seconds = timeout_seconds(model) if seconds is None else max(0.0, float(seconds))
        self.started = time.monotonic()
        self.deadline = self.started + self.seconds
        self.ready = False
        self.trace = current_trace.get()
        if self.trace:
            with self.trace._lock:
                self.trace.data["first_reply_timeout_seconds"] = self.seconds
                self.trace.data["first_reply_deadline"] = time.time() + self.seconds if self.seconds else None

    def _timeout_error(self) -> FirstReplyTimeout:
        message = f"等待模型首回复超过 {self.seconds:g} 秒（思考、正文、工具调用或图片均算回复）"
        if self.trace:
            self.trace.event("first_reply_timeout", message + "，本次上游读取已中断，稍后自动重试。")
        return FirstReplyTimeout(message + "。", seconds=self.seconds)

    async def wait(self, operation: Awaitable) -> Any:
        if self.ready:
            return await operation  # no per-token cancellation bookkeeping after the first reply
        if not self.seconds:
            # Even without a first-reply timer the private startup read belongs
            # to the peer/server or total deadline that cancelled it. Some HTTP
            # adapters return EOF, an error event or buffered usage instead.
            cancellation = AttemptCancellation()
            token = _active_wait.set((self, cancellation.owner, cancellation.initial))
            try:
                result = await operation
                return await cancellation.transfer(result, _close_unused)
            except Exception:
                await cancellation.check()
                raise
            finally:
                _active_wait.reset(token)
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            await _discard_unstarted(operation)
            raise self._timeout_error()

        timer = asyncio.timeout(remaining)
        owner = asyncio.current_task()
        cancellations = owner.cancelling()
        token = _active_wait.set((self, owner, cancellations))
        try:
            async with timer:
                result = await operation
        except Exception as exc:
            if owner.cancelling() > cancellations:
                raise asyncio.CancelledError() from exc
            if isinstance(exc, FirstReplyTimeout):
                raise
            if timer.expired() or time.monotonic() >= self.deadline:
                raise self._timeout_error() from exc
            raise  # preserve an early operation error or genuine EOF
        finally:
            _active_wait.reset(token)

        # asyncio.timeout is cooperative: a ready awaitable can finish without
        # a loop tick, or code can catch CancelledError and return late.
        if owner.cancelling() > cancellations or timer.expired() or time.monotonic() >= self.deadline:
            await _close_unused(result)
            if owner.cancelling() > cancellations:
                raise asyncio.CancelledError()
            raise self._timeout_error()
        return result

    def accept(self, *, generated: bool=True) -> None:
        """Release the delivery gate; only real model output is a first-token sample."""
        if self.ready:
            return
        _check_active_budget(self)
        self.ready = True
        if self.trace:
            with self.trace._lock:
                self.trace.data["first_reply_deadline"] = None
            if generated:
                self.trace.milestone("first_reply", "收到模型首个有效回复，首回复计时结束。")

    async def iterate(self, source: AsyncIterable, predicate: Callable) -> AsyncIterator[Any]:
        # 注意：这里**不能**加 try/finally 去 aclose(iterator)——prefetch 在
        # 首个回复帧后主动 break 并继续经 PrefetchedStream 消费同一 source，
        # 迭代器被消费方提前 abandon 是预期路径（清理交给 prefetch 的
        # except BaseException 分支与消费方自己的 finally，asyncgen GC
        # finalization 作兜底）。
        iterator = source.__aiter__()
        while True:
            try:
                item = await self.wait(anext(iterator))
            except StopAsyncIteration:
                return
            if not self.ready and predicate(item):
                self.accept()
            yield item
            del item  # the next pull may wait indefinitely

    async def prefetch(self, source: AsyncIterable, predicate: Callable, *, max_prefix_bytes: int=2 * 1024 * 1024) -> "PrefetchedStream":
        prefix = []
        size = 0
        try:
            async for item in self.iterate(source, predicate):
                prefix.append(item)
                if self.ready:
                    break
                size += len(item)
                if size > max_prefix_bytes:
                    raise IncompleteResponseError("首回复前的响应缓冲超过字节上限。")
        except BaseException:
            await source.aclose()
            raise
        return PrefetchedStream(prefix, source)


class PrefetchedStream:
    def __init__(self, prefix: list, source: AsyncIterable) -> None:
        # Take the prefetcher's list without copying it or any wire chunks.
        self.prefix = prefix
        self._position = 0
        self.source = source

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._position < len(self.prefix):
            index = self._position
            self._position += 1
            item, self.prefix[index] = self.prefix[index], None
            if self._position == len(self.prefix):
                self.prefix.clear()
            return item
        return await anext(self.source)

    async def aclose(self) -> None:
        # A slow physical close should not pin even the unconsumed prefix.
        self.prefix.clear()
        await self.source.aclose()


def sdk_has_reply(chunk: Any) -> bool:
    for candidate in getattr(chunk, "candidates", None) or []:
        for part in getattr(getattr(candidate, "content", None), "parts", None) or []:
            call = sdk_function_call(part)
            if (getattr(part, "text", None)
                    or (call and (getattr(call, "name", None) or getattr(call, "partial_args", None)))
                    or sdk_media_payload(part) is not None):
                return True
    return False


def native_has_reply(payload: Any) -> bool:
    objects = payload if isinstance(payload, list) else [payload]
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        for candidate in obj.get("candidates") or []:
            content = candidate.get("content") if isinstance(candidate, dict) else None
            if not isinstance(content, dict):
                continue
            if any(map(native_part_has_reply, content.get("parts") or [])):
                return True
    return False


async def timed_call(model: str, operation: Awaitable, *, whole_call: bool=False, seconds: Optional[float]=None) -> Any:
    """whole_call：该操作要等完整响应（非流式 generate_content / 整取 POST）。

    整调用不参与首回复计时（2026-11 语义，见模块头注释）：窗口强制置 0
    （guard.wait 直接放行，trace 记录 0 = 已禁用，面板不再显示倒计时），
    成功仍记 first_reply 里程碑（整取的完整响应到达即首回复）。窗口语义
    只保留给真流式路径：guard.iterate / prefetch，以及不带 whole_call 的
    本调用——test_proxy_failover 直接用它驱动代理出口首回复窗口，不受本
    规则影响。"""
    if whole_call:
        seconds = 0.0
    guard = FirstReplyGuard(model, seconds=seconds)
    result = await guard.wait(operation)
    if getattr(result, "status_code", 200) < 400:
        guard.accept()
    return result
