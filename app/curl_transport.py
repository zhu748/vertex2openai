"""Cancellation ownership for curl_cffi 0.16.3 before response headers arrive."""
import asyncio
from contextvars import ContextVar
from dataclasses import dataclass, field

import anyio
from curl_cffi.requests import AsyncSession

_request_lease = ContextVar("curl_request_lease", default=None)


@dataclass
class _Lease:
    session: object
    curl: object = None
    released: asyncio.Event = field(default_factory=asyncio.Event)


class CancellationSafeSession(AsyncSession):
    """Keep a request-local handle until the producer's normal cleanup runs.

    curl_cffi detaches its stream producer before awaiting response headers.
    Cancelling that await alone leaves the producer and pooled handle alive.
    Removing the owned handle cancels its future; the producer then releases it.
    ContextVar ownership also covers done callbacks without touching peers.
    """
    async def pop_curl(self):
        curl = await super().pop_curl()
        lease = _request_lease.get()
        if lease is not None and lease.session is self:
            lease.curl = curl
        return curl

    def release_curl(self, curl):
        lease = _request_lease.get()
        owned = lease is not None and lease.session is self and lease.curl is curl
        if owned and lease.released.is_set():
            return
        try:
            super().release_curl(curl)
        finally:
            if owned:
                lease.released.set()

    async def request(self, *args, **kwargs):
        lease = _Lease(self)
        token = _request_lease.set(lease)
        try:
            return await super().request(*args, **kwargs)
        except BaseException:
            if lease.curl is not None and not lease.released.is_set():
                self.acurl.remove_handle(lease.curl)
                with anyio.CancelScope(shield=True):
                    await asyncio.shield(lease.released.wait())
            raise
        finally:
            _request_lease.reset(token)
