"""Fail-fast generation admission; detached streams keep their slot until EOF."""
from functools import wraps
import threading

import anyio
from fastapi.responses import StreamingResponse
from runtime_state import app_state
from proxy_resolver import ProxyUsage, current_proxy_usage


class GenerationCapacityError(RuntimeError):
    code = 503


class GenerationLimiter:
    def __init__(self):
        self.active = 0
        self._lock = threading.Lock()

    def reserve(self):
        limit = max(1, int(app_state.get_setting("upstream_max_concurrency", 32)))
        with self._lock:
            if self.active >= limit:
                raise GenerationCapacityError("上游生成并发已满，请稍后重试。")
            self.active += 1
        limiter = self

        class Lease:
            def __init__(self):
                self.released = False
                self.usage = ProxyUsage()

            def release(self):
                with limiter._lock:
                    if self.released:
                        return
                    self.released = True
                    limiter.active -= 1
                self.usage.close()
        return Lease()


generation_limiter = GenerationLimiter()


class LeasedIterator:
    def __init__(self, source, lease):
        self.source = source.__aiter__()
        self.lease = lease

    def __aiter__(self):
        return self

    async def __anext__(self):
        token = current_proxy_usage.set(self.lease.usage)
        try:
            return await anext(self.source)
        except BaseException:
            self.lease.release()
            raise
        finally:
            current_proxy_usage.reset(token)

    async def aclose(self):
        try:
            close = getattr(self.source, "aclose", None)
            if close:
                await close()
        finally:
            self.lease.release()


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
        except BaseException:
            lease.release()
            raise
        finally:
            current_proxy_usage.reset(token)
        if not isinstance(response, StreamingResponse):
            lease.release()
            return response
        iterator = LeasedIterator(response.body_iterator, lease)
        if hasattr(response, "aclose"):
            response.body_iterator = iterator
            return response
        return ManagedStreamingResponse(iterator, status_code=response.status_code,
                                        headers=dict(response.headers), background=response.background)
    return guarded
