"""Fail-fast generation admission; detached streams keep their slot until EOF."""
from functools import wraps
import asyncio
import threading

import anyio
from fastapi.responses import StreamingResponse
from runtime_state import app_state
from proxy_resolver import ProxyUsage, current_proxy_usage
from request_tracking import PayloadObserver, current_trace


class GenerationCapacityError(RuntimeError):
    code = 503


class GenerationLimiter:
    def __init__(self):
        self.active = 0
        self._lock = threading.Lock()

    def reserve(self):
        trace = current_trace.get()
        limit = max(1, int(app_state.get_setting("upstream_max_concurrency", 32)))
        with self._lock:
            if self.active >= limit:
                raise GenerationCapacityError("上游生成并发已满，请稍后重试。")
            self.active += 1
        if trace:
            trace.start_generation()
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


class LeasedIterator:
    def __init__(self, source, lease, *, sse=True):
        self.source = source.__aiter__()
        self.lease = lease
        self.observer = PayloadObserver(lease.trace, sse=sse) if lease.trace else None

    def __aiter__(self):
        return self

    async def __anext__(self):
        token = current_proxy_usage.set(self.lease.usage)
        trace_token = current_trace.set(self.lease.trace)
        try:
            chunk = await anext(self.source)
            if self.observer:
                self.observer.feed(chunk)
            return chunk
        except StopAsyncIteration:
            if self.observer:
                self.observer.feed(b"", final=True)
            self.lease.release()
            raise
        except BaseException as exc:
            self.lease.release("cancelled" if isinstance(exc, (GeneratorExit, asyncio.CancelledError)) else "error", str(exc))
            raise
        finally:
            current_trace.reset(trace_token)
            current_proxy_usage.reset(token)

    async def aclose(self):
        token = current_trace.set(self.lease.trace)
        try:
            close = getattr(self.source, "aclose", None)
            if close:
                await close()
        finally:
            self.lease.release("cancelled")
            current_trace.reset(token)


class ManagedStreamingResponse(StreamingResponse):
    def __init__(self, *args, cleanup=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleanup = cleanup

    async def aclose(self):
        with anyio.CancelScope(shield=True):
            try:
                await self.body_iterator.aclose()
            finally:
                if self.cleanup:
                    await self.cleanup()

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.aclose()


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
        iterator = LeasedIterator(response.body_iterator, lease, sse="text/event-stream" in response.headers.get("content-type", ""))
        if hasattr(response, "aclose"):
            response.body_iterator = iterator
            return response
        return ManagedStreamingResponse(iterator, status_code=response.status_code,
                                        headers=dict(response.headers), background=response.background,
                                        media_type=getattr(response, "media_type", None))
    return guarded
