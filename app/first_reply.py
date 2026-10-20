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


@asynccontextmanager
async def proxy_attempt(enabled):
    """Give each configured proxy its own first-reply window within the total deadline."""
    active = _active_wait.get() if enabled else None
    if active is None:
        yield
        return
    guard, parent = active
    parent.reschedule(None)
    guard.deadline = time.monotonic() + guard.seconds
    if guard.trace:
        with guard.trace._lock:
            guard.trace.data["first_reply_deadline"] = time.time() + guard.seconds
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
    def __init__(self, model):
        self.seconds = timeout_seconds(model)
        self.started = time.monotonic()
        self.deadline = self.started + self.seconds
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

    async def prefetch(self, source, predicate):
        prefix = []
        size = 0
        try:
            async for item in self.iterate(source, predicate):
                prefix.append(item)
                if self.ready:
                    break
                size += len(item)
                if size > 2 * 1024 * 1024:
                    raise IncompleteResponseError("首回复前的元数据缓冲超过 2 MiB。")
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


async def timed_call(model, operation):
    guard = FirstReplyGuard(model)
    result = await guard.wait(operation)
    if getattr(result, "status_code", 200) < 400:
        guard.accept()
    return result
