"""One attempt keeps one configured route even when a sibling moves the cursor."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from proxy_failover import FailoverHTTPClient, ProxyChain
from proxy_resolver import ProxyResolveError, resolve_proxy_uri


A, B, C = (f"http://{name}.test:80" for name in ("a", "b", "c"))


@pytest.mark.parametrize("method", ["prepare", "run"])
def test_async_resolution_keeps_raw_and_pinned_result_paired(method):
    async def scenario():
        chain = ProxyChain(candidates=[A, B])
        resolving, release = asyncio.Event(), asyncio.Event()

        async def resolve(raw):
            resolving.set()
            await release.wait()
            return "resolved:" + raw

        def factory(raw, resolved):
            return raw, resolved, resolve_proxy_uri(raw)

        async def operation(raw, resolved):
            return factory(raw, resolved)

        with patch("proxy_failover.resolve_proxy_uri_async", resolve):
            task = asyncio.create_task(chain.prepare(factory) if method == "prepare" else chain.run(operation))
            try:
                await asyncio.wait_for(resolving.wait(), 3)
                assert chain.advance(status=503)  # another user of the same request-local chain
            finally:
                release.set()
            assert await task == (A, "resolved:" + A, "resolved:" + A)
    asyncio.run(scenario())


def test_late_resolution_failure_compares_candidate_index_not_uri():
    async def scenario():
        # Two positions can carry the same raw value. A failure from the first
        # position is not evidence that the new position has already failed.
        chain = ProxyChain(candidates=[A, A, B])
        resolving, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def resolve(raw):
            calls.append(raw)
            if len(calls) == 1:
                resolving.set()
                await release.wait()
                raise ProxyResolveError("mock resolution failure")
            return raw

        with patch("proxy_failover.resolve_proxy_uri_async", resolve):
            task = asyncio.create_task(chain.prepare(lambda raw, resolved: raw))
            try:
                await asyncio.wait_for(resolving.wait(), 3)
                assert chain.advance(status=503)
            finally:
                release.set()
            assert await task == A
        assert calls == [A, A] and chain.index == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("candidates", [[A, B], [A, B, C]], ids=["already-last", "middle"])
@pytest.mark.parametrize("outcome", ["retry-error", "retry-status", "local-error", "final-status"])
def test_stale_async_result_only_retries_transport_failures(candidates, outcome):
    async def scenario():
        chain = ProxyChain(candidates=candidates)
        started, release = asyncio.Event(), asyncio.Event()
        calls = []
        reply = SimpleNamespace(status_code=503 if outcome == "retry-status" else 401, aclose=AsyncMock())
        local_error = httpx.PoolTimeout("local busy")

        async def operation(raw, resolved):
            calls.append(raw)
            if raw == A:
                started.set()
                await release.wait()
                if outcome == "retry-error":
                    raise httpx.ReadError("mock failure on A")
                if outcome == "local-error":
                    raise local_error
                return reply
            return raw

        async def peer(raw, resolved):
            if raw == A:
                raise httpx.ConnectError("mock failure on A")
            return raw

        with patch("logger.stats.add_retry") as switched:
            task = asyncio.create_task(chain.run(operation))
            try:
                await asyncio.wait_for(started.wait(), 3)
                assert await chain.run(peer) == B
            finally:
                release.set()
            if outcome == "local-error":
                with pytest.raises(httpx.PoolTimeout) as raised:
                    await task
                assert raised.value is local_error
            else:
                result = await task
                assert result == (reply if outcome == "final-status" else B)
            switched.assert_called_once_with()
        assert calls == ([A, B] if outcome.startswith("retry-") else [A])
        assert reply.aclose.await_count == int(outcome == "retry-status")
    asyncio.run(scenario())


def test_worker_resolution_and_late_failure_use_the_same_snapshot():
    chain = ProxyChain(candidates=[A, B, C])
    resolving, release = Event(), Event()
    calls = []

    def resolve(raw):
        if raw == A:
            resolving.set()
            assert release.wait(3)
        return "resolved:" + raw

    def operation(raw, resolved):
        calls.append((raw, resolved))
        if resolved == "resolved:" + A:
            raise httpx.ConnectError("mock failure on A")
        return raw

    with patch("proxy_resolver.resolve_proxy_uri", resolve), ThreadPoolExecutor(max_workers=1) as executor:
        task = executor.submit(chain.run_sync, operation)
        try:
            assert resolving.wait(3)
            assert chain.advance(status=503)
        finally:
            release.set()
        assert task.result(3) == B
    assert calls == [(A, "resolved:" + A), (B, "resolved:" + B)]


@pytest.mark.parametrize("failure", ["headers", "enter"])
def test_stale_http_stream_handshake_follows_current_without_skipping(failure):
    async def scenario():
        chain = ProxyChain(candidates=[A, B, C])
        opening, release = asyncio.Event(), asyncio.Event()
        entered, closed = [], []

        def factory(raw, resolved):
            @asynccontextmanager
            async def stream():
                entered.append(raw)
                try:
                    if raw == A:
                        opening.set()
                        await release.wait()
                        if failure == "enter":
                            raise httpx.ConnectError("mock failure on A")
                    yield SimpleNamespace(status_code=503 if raw == A else 200, raw=raw)
                finally:
                    closed.append(raw)
            return SimpleNamespace(stream=stream)

        async def use():
            async with FailoverHTTPClient(chain, factory).stream() as result:
                assert closed == [A]
                return result.raw

        task = asyncio.create_task(use())
        try:
            await asyncio.wait_for(opening.wait(), 3)
            assert chain.advance(status=503)
        finally:
            release.set()
        assert await task == B
        assert entered == closed == [A, B]
    asyncio.run(scenario())
