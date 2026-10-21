"""Per-attempt deadline until real model output, independent of SSE keepalives."""
import asyncio
import math
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar

from runtime_state import app_state
from request_tracking import current_trace
from response_validation import IncompleteResponseError

_active_wait = ContextVar("first_reply_wait", default=None)

# 生图模型的整调用最低预算：生图请求（两条通道都被强制走假流式/整取
# 路径）常需 30-120s，首回复窗口（默认 15s）会把它们打成"每次尝试都超
# 时→重试空烧→最终 504"（FirstReplyTimeout 是 TimeoutError 子类会被判
# 可重试）。用户无法通过改请求规避（路径是服务强制选的），故对生图模型
# 把整调用预算抬到 max(窗口, 300s)；文本模型维持配置窗口语义——用户与
# 测试可以用小窗口约束挂起的非流式尝试（test_retry_windows 依赖此契约）。
WHOLE_CALL_MIN_SECONDS = 300.0


def _is_image_model(model: str) -> bool:
    try:
        from model_capabilities import get_profile
        return bool(get_profile(model).get("is_image"))
    except Exception:
        return False


@asynccontextmanager
async def proxy_attempt(enabled):
    """Give each configured proxy its own first-reply window within the total deadline."""
    active = _active_wait.get() if enabled else None
    if active is None:
        yield
        return
    guard, parent = active
    parent.reschedule(None)
    # 每个出口有自己的窗口，但总等待不得超出自首次等待起的原始预算：
    # 旧实现每换一个出口就把 deadline 重置为满窗口，3 个慢出口 × 15s = 45s
    # 才轮到换 Key 重试，且监控面板的截止时间一直在往后漂（2026-10 修复）。
    remaining = guard.initial_deadline - time.monotonic()
    window = guard.seconds if remaining <= 0 else max(0.001, min(guard.seconds, remaining))
    guard.deadline = time.monotonic() + window
    if guard.trace:
        with guard.trace._lock:
            guard.trace.data["first_reply_deadline"] = time.time() + window
    attempt = asyncio.timeout_at(guard.deadline)
    completed = False
    try:
        async with attempt:
            yield
            completed = True
    except TimeoutError as exc:
        if not attempt.expired():
            raise
        # 与 FirstReplyGuard.wait 的超时分支逐字对齐：异常 message 是外层重试
        # 日志、最终 504 错误响应与 cookie 通道共用的文案源，多出口路径不能
        # 另造一套说法；trace 事件补回旧版"超过 N 秒"说明并预告自动重试。
        if guard.trace:
            guard.trace.event(
                "first_reply_timeout",
                f"等待模型首回复超过 {guard.seconds:g} 秒（思考、正文、工具调用"
                "或图片均算回复），本次上游读取已中断，稍后自动重试。")
        raise FirstReplyTimeout(
            f"等待模型首回复超过 {guard.seconds:g} 秒（思考、正文、工具调用或图片均算回复）。",
            seconds=guard.seconds) from exc
    finally:
        parent.reschedule(guard.deadline if completed else None)


class FirstReplyTimeout(TimeoutError):
    code = 504

    def __init__(self, message, *, seconds=None):
        super().__init__(message)
        # 携带触发超时的窗口秒数，供重试日志与请求监控面板展示窗口口径。
        self.seconds = seconds


def timeout_seconds(model):
    try:
        value = float(app_state.get_effective_settings(model).get("first_reply_timeout_seconds", 15))
        return max(0.0, min(7200.0, value)) if math.isfinite(value) else 15.0
    except (TypeError, ValueError, OverflowError):
        return 15.0


class FirstReplyGuard:
    def __init__(self, model, *, seconds=None):
        # seconds：调用方预先算好的窗口。同一请求的重试循环共享一次设置
        # 读取——旧实现每次尝试都新建 Guard，每次都深拷贝整棵设置树
        # （含全部 model_overrides），重试风暴下是纯浪费的热路径开销。
        self.seconds = timeout_seconds(model) if seconds is None else max(0.0, float(seconds))
        self.started = time.monotonic()
        self.deadline = self.started + self.seconds
        # 原始预算终点：proxy_attempt 每次换出口都会重置 deadline，不记
        # 原始值的话 N 个出口可以把首回复窗口放大成 N×（见 proxy_attempt）。
        self.initial_deadline = self.deadline
        self.ready = False
        self.trace = current_trace.get()
        if self.trace:
            with self.trace._lock:
                self.trace.data["first_reply_timeout_seconds"] = self.seconds
                self.trace.data["first_reply_deadline"] = time.time() + self.seconds if self.seconds else None

    async def wait(self, operation):
        if self.ready or not self.seconds:
            return await operation
        deadline = asyncio.timeout(max(0.001, self.deadline - time.monotonic()))
        token = _active_wait.set((self, deadline))
        try:
            async with deadline:
                return await operation
        except TimeoutError as exc:
            if not deadline.expired():
                raise
            if self.trace:
                self.trace.event("first_reply_timeout", f"等待模型首回复超过 {self.seconds:g} 秒，本次上游读取已中断。")
            raise FirstReplyTimeout(
                f"等待模型首回复超过 {self.seconds:g} 秒（思考、正文、工具调用或图片均算回复）。",
                seconds=self.seconds) from exc
        finally:
            _active_wait.reset(token)

    def accept(self):
        if self.ready:
            return
        self.ready = True
        if self.trace:
            with self.trace._lock:
                self.trace.data["first_reply_deadline"] = None
            self.trace.milestone("first_reply", "收到模型首个有效回复，首回复计时结束。")

    async def iterate(self, source, predicate):
        iterator = source.__aiter__()
        while True:
            try:
                item = await self.wait(anext(iterator))
            except StopAsyncIteration:
                return
            if not self.ready and predicate(item):
                self.accept()
            yield item

    async def prefetch(self, source, predicate, *, max_prefix_bytes=2 * 1024 * 1024):
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
    def __init__(self, prefix, source):
        self.prefix = iter(prefix)
        self.source = source

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.prefix)
        except StopIteration:
            return await anext(self.source)

    async def aclose(self):
        await self.source.aclose()


def sdk_has_reply(chunk):
    for candidate in getattr(chunk, "candidates", None) or []:
        for part in getattr(getattr(candidate, "content", None), "parts", None) or []:
            if (getattr(part, "text", None) or getattr(part, "function_call", None)
                    or getattr(part, "inline_data", None) or getattr(part, "file_data", None)):
                return True
    return False


def native_has_reply(payload):
    objects = payload if isinstance(payload, list) else [payload]
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        for candidate in obj.get("candidates") or []:
            content = candidate.get("content") if isinstance(candidate, dict) else None
            if not isinstance(content, dict):
                continue
            for part in content.get("parts") or []:
                if isinstance(part, dict) and any(part.get(k) for k in ("text", "functionCall", "inlineData", "fileData")):
                    return True
    return False


async def timed_call(model, operation, *, whole_call=False, seconds=None):
    """whole_call：该操作要等完整响应（非流式 generate_content / 整取 POST）。

    生图模型在此路径把预算抬到 max(窗口, WHOLE_CALL_MIN_SECONDS)（见常量
    注释）；文本模型严格用配置窗口（保留"小窗口约束挂起尝试"的既有契约）。"""
    guard = FirstReplyGuard(model, seconds=seconds)
    if whole_call and guard.seconds and _is_image_model(model):
        guard.seconds = max(guard.seconds, WHOLE_CALL_MIN_SECONDS)
        guard.deadline = guard.started + guard.seconds
        guard.initial_deadline = guard.deadline
        if guard.trace:
            with guard.trace._lock:
                guard.trace.data["first_reply_timeout_seconds"] = guard.seconds
                guard.trace.data["first_reply_deadline"] = time.time() + guard.seconds
    result = await guard.wait(operation)
    if getattr(result, "status_code", 200) < 400:
        guard.accept()
    return result
