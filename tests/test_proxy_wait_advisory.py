"""Only the general retry budget may wait and retry an advised failure."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from proxy_failover import FailoverHTTPClient, ProxyChain

A, B = ("http://a.test", "http://b.test")
FUTURE_DATE = "Fri, 01 Jan 2100 00:00:00 GMT"


@pytest.mark.parametrize("mode", ["status", "raise", "sync-raise", "stream", "stream-raise"])
@pytest.mark.parametrize("hint,advised", [("45", True), (FUTURE_DATE, True), ("0", False),
                                        ("not a date", False), (None, False)])
def test_proxy_operations_preserve_advised_failure(mode, hint, advised):
    async def run():
        chain = ProxyChain(candidates=[A, B])
        calls, closed = [], []
        first = httpx.Response(503, headers={"Retry-After": hint} if hint else {},
                               request=httpx.Request("POST", "http://mock.test"))
        failure = httpx.HTTPStatusError("unavailable", request=first.request, response=first)

        def perform(raw, _resolved):
            calls.append(raw)
            if raw == B:
                return httpx.Response(200)
            if "raise" in mode:
                raise failure
            return first

        @asynccontextmanager
        async def stream(raw):
            result = perform(raw, None)
            try:
                yield result
            finally:
                closed.append(raw)
                await result.aclose()

        async def execute():
            if mode == "sync-raise":
                return chain.run_sync(perform)
            if mode.startswith("stream"):
                client = FailoverHTTPClient(chain, lambda raw, _: SimpleNamespace(stream=lambda: stream(raw)))
                async with client.stream() as response:
                    return response
            async def operation(raw, resolved):
                return perform(raw, resolved)
            return await chain.run(operation)

        with patch("logger.stats.add_retry") as count:
            if advised and "raise" in mode:
                with pytest.raises(httpx.HTTPStatusError) as raised:
                    await execute()
                assert raised.value is failure
            else:
                result = await execute()
                assert result.status_code == (503 if advised else 200)
                if advised:
                    assert result is first and result.headers["retry-after"] == hint
            assert count.call_count == int(not advised)
        assert calls == ([A] if advised else [A, B])
        assert chain.current == (A if advised else B)
        if mode == "stream":
            assert closed == calls

    asyncio.run(run())


def test_positive_advisory_does_not_replay_behind_a_siblings_advanced_cursor():
    async def run():
        chain = ProxyChain(candidates=[A, B])
        first = SimpleNamespace(status_code=503, headers={"retry-after": "1"}, aclose=AsyncMock())
        async def operation(raw, resolved):
            assert raw == A
            assert chain.advance(status=502, failed_index=0)  # another call, no advisory
            return first
        with patch("logger.stats.add_retry") as switched:
            assert await chain.run(operation) is first
            switched.assert_called_once_with()
        first.aclose.assert_not_awaited()
    asyncio.run(run())


def test_cookie_error_event_preserves_503_wait_on_both_failover_layers():
    from upstreams import cookie_transport
    async def run():
        hits = []
        chain = ProxyChain(candidates=[A, B])
        def respond(request):
            hits.append(request)
            return httpx.Response(503, json={"error": "unavailable"}, headers={"Retry-After": "45"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as underlying:
            client = FailoverHTTPClient(chain, lambda _raw, _resolved: underlying)
            events = [event async for event in cookie_transport.stream_events(client, {}, {})]
        [(kind, error)] = events
        assert (kind, error.status_code, error.retry_after) == ("retryable_error", 503, "45")
        assert len(hits) == 1 and chain.current == A
    asyncio.run(run())
