"""Effective SDK cache capacity and ownership during a live shrink."""
import asyncio
from types import SimpleNamespace
import unittest
from unittest import mock

import client_pool
from proxy_resolver import ProxyUsage, current_proxy_usage


class SdkCapacityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.pool = client_pool.SdkClientPool()
        self.closer = client_pool._GracefulCloser()
        self.clients = []

        def make(**kwargs):
            client = SimpleNamespace(close=mock.Mock(), aio=SimpleNamespace(aclose=mock.AsyncMock()))
            self.clients.append(client)
            return client

        for patcher in (
            mock.patch.object(client_pool, "express_sdk_pool", self.pool),
            mock.patch.object(client_pool, "_closer", self.closer),
            mock.patch.object(client_pool, "_sdk_signature", return_value=("local",)),
            mock.patch.object(client_pool, "get_http_options", return_value=None),
            mock.patch.object(client_pool.genai, "Client", side_effect=make),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.get = client_pool.get_pooled_express_client

    async def asyncTearDown(self):
        await self.pool.aclose()
        await self.closer.aclose()
        for client in self.clients:
            client.close.assert_called_once()
            client.aio.aclose.assert_awaited_once()

    async def test_pinned_limits_below_default_and_shrink_on_hit_keep_active_lease(self):
        usage = ProxyUsage()
        self.addCleanup(usage.close)
        token = current_proxy_usage.set(usage)
        self.addCleanup(current_proxy_usage.reset, token)
        with mock.patch.dict("os.environ", {"SDK_POOL_MAX": "3"}), \
             mock.patch.object(client_pool, "_GRACE_CLOSE_SECONDS", .002):
            for key in "abcde":
                self.get(key)
            self.assertEqual(list(self.pool._clients), list("cde"))
            with mock.patch.dict("os.environ", {"SDK_POOL_MAX": "1"}):
                self.assertIs(self.get("c"), self.clients[2])
                self.assertEqual(list(self.pool._clients), ["c"])
                self.assertEqual(len(self.closer._pending), 4)
                await asyncio.sleep(.015)
                for client in self.clients:
                    client.close.assert_not_called()
                # Reclaim the same leased resource without rebuilding or double-closing it.
                self.assertIs(self.get("d"), self.clients[3])
                self.assertEqual(self.pool.size(), 1)
                usage.close()
                await asyncio.sleep(.015)
                self.assertEqual(len(self.closer._pending), 0)

    async def test_dynamic_limit_tracks_key_count_and_shrinks_on_miss(self):
        with mock.patch.dict("os.environ", {"SDK_POOL_MAX": ""}), \
             mock.patch.object(client_pool.app_state, "get_express_keys", return_value=range(7)) as keys:
            for key in range(9):
                self.get(str(key))
            self.assertEqual(self.pool.size(), 9)
            keys.return_value = []
            self.get("new")
            self.assertEqual(list(self.pool._clients), ["4", "5", "6", "7", "8", "new"])

    async def test_saturated_resize_preserves_ownership_and_can_retry(self):
        self.closer._max_pending = 1
        usage = ProxyUsage()
        self.addCleanup(usage.close)
        token = current_proxy_usage.set(usage)
        self.addCleanup(current_proxy_usage.reset, token)
        with mock.patch.dict("os.environ", {"SDK_POOL_MAX": "4"}):
            for key in "abcd":
                self.get(key)
        with mock.patch.dict("os.environ", {"SDK_POOL_MAX": "1"}):
            with self.assertRaises(client_pool.PoolCapacityError):
                self.get("d")
            self.assertEqual(self.pool.size() + len(self.closer._pending), 4)
            usage.close()
            self.assertIs(self.get("d"), self.clients[3])
            self.assertEqual(self.pool.size(), 1)
