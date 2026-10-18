"""Per-attempt deadline until real model output, independent of SSE keepalives."""
import asyncio
import math
import time

from runtime_state import app_state
from request_tracking import current_trace
from response_validation import IncompleteResponseError


class FirstReplyTimeout(TimeoutError):
    code = 504


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
        try:
            async with deadline:
                return await operation
        except TimeoutError as exc:
            if not deadline.expired():
                raise
            if self.trace:
                self.trace.event("first_reply_timeout", f"等待模型首回复超过 {self.seconds:g} 秒，本次上游读取已中断。")
            raise FirstReplyTimeout(f"等待模型首回复超过 {self.seconds:g} 秒（思考、正文、工具调用或图片均算回复）。") from exc

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
