"""Hot curl capacity is keyed by actual settings and never revokes an active lease."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import client_pool
import impersonate
import runtime_state
from proxy_resolver import ProxyUsage


def test_effective_capacity_selects_new_curl_session_but_not_a_new_httpx_client(tmp_path):
    async def run():
        pool = impersonate._UpstreamClientPool()
        # Real durable saves, real pool and curl session; no requests are issued.
        with (patch.object(runtime_state, "STATE_FILE", str(tmp_path / "web_state.json")),
              patch.object(impersonate, "app_state", runtime_state.AppState()) as state):
            state.update_settings({"cookie_impersonate": "chrome", "upstream_max_concurrency": 1})
            try:
                def get():
                    return pool.get({}, impersonate._create_upstream_client)
                first = get()
                assert first._s.max_clients == 10
                state.update_settings({"upstream_max_concurrency": 9})
                assert get() is first  # same effective floor
                state.update_settings({"upstream_max_concurrency": 32})
                second = get()
                assert second is not first and second._s.max_clients == 32
                assert get() is second
                state.update_settings({"upstream_max_concurrency": 10})
                assert get() is first
                state.update_settings({"cookie_impersonate": "off"})
                plain = get()
                state.update_settings({"upstream_max_concurrency": 64})
                assert get() is plain
            finally:
                await pool.aclose()
    asyncio.run(run())


def test_concurrent_save_between_miss_and_construction_cannot_mislabel_the_session():
    async def run():
        pool = impersonate._UpstreamClientPool()
        opts = {"cookie_impersonate": "chrome", "upstream_max_concurrency": 11}
        def factory(kwargs, shape):
            # Model a settings write after key selection but before construction.
            opts.update(cookie_impersonate="off", upstream_max_concurrency=72)
            return impersonate._create_upstream_client(kwargs, shape)
        with patch.object(impersonate.app_state, "get_committed_settings",
                          side_effect=lambda *keys: opts.copy()) as read:
            try:
                old = pool.get({}, factory)
                assert old._s.max_clients == 11
                read.assert_called_once_with("cookie_impersonate", "upstream_max_concurrency")
                assert pool.get({}, impersonate._create_upstream_client) is not old
            finally:
                await pool.aclose()
    asyncio.run(run())


def test_evicted_old_capacity_is_kept_alive_through_physical_lease_release():
    async def run():
        settings = {"cookie_impersonate": "chrome", "upstream_max_concurrency": 10}
        closer = client_pool._GracefulCloser()
        pool = impersonate._UpstreamClientPool()
        usage = ProxyUsage()
        old_closed = asyncio.Event()
        first = SimpleNamespace(aclose=AsyncMock(side_effect=old_closed.set))
        second = SimpleNamespace(aclose=AsyncMock())
        with (patch.object(impersonate.app_state, "get_committed_settings", side_effect=lambda *keys: settings.copy()),
              patch.object(impersonate, "_UPSTREAM_POOL_MAX", 1),
              patch.object(impersonate, "_closer", closer),
              patch.object(client_pool, "_closer", closer),
              patch.object(client_pool, "_GRACE_CLOSE_SECONDS", 0.005)):
            try:
                assert pool.get({}, lambda _kw, _shape: first, usage=usage) is first
                settings["upstream_max_concurrency"] = 32
                assert pool.get({}, lambda _kw, _shape: second) is second
                await asyncio.sleep(0.025)  # well beyond grace, original lease still live
                first.aclose.assert_not_awaited()
                usage.close()
                await asyncio.wait_for(old_closed.wait(), 2)
                first.aclose.assert_awaited_once()
                second.aclose.assert_not_awaited()
            finally:
                usage.close()
                await pool.aclose()
                await closer.aclose()
        second.aclose.assert_awaited_once()
    asyncio.run(run())
