"""Claude exact-token counting is direct work, not a detached generation."""
import asyncio
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import main
from main import app
from logger import ProxyStats
from proxy_failover import ProxyChain
from proxy_resolver import current_proxy_usage
from routes import claude_api
from test_replay_http_delivery import Peer


MODEL = "gemini-2.5-flash"


class CountConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.slots = threading.BoundedSemaphore(1)
        self.enterContext(patch.object(claude_api, "_count_tokens_slots", self.slots))
        self.enterContext(patch.object(claude_api, "get_express_models", return_value=[MODEL]))
        self.enterContext(patch.object(app.state.express_key_manager, "get_express_record",
                                      return_value={"key": "fixture", "project_id": "local"}))
        self.enterContext(patch.object(claude_api, "ProxyChain", side_effect=lambda _: ProxyChain(candidates=[""])))
        self.estimate = self.enterContext(patch.object(claude_api, "estimate_request_tokens", return_value=19))

    def install_count(self, operation):
        sdk = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(count_tokens=operation)))
        self.enterContext(patch.object(claude_api, "get_pooled_express_client", return_value=sdk))

    @staticmethod
    def peer():
        return Peer("/v1/messages/count_tokens", payload={"model": MODEL,
                    "messages": [{"role": "user", "content": "test tokens"}]})

    async def check_cancel(self, *, external=False, translate=False):
        peer = self.peer()
        reading, closing, let_close, let_finish = (asyncio.Event() for _ in range(4))
        usages = []

        async def count(**_):
            usages.append(current_proxy_usage.get())
            reading.set()
            try:
                await let_finish.wait()
            except asyncio.CancelledError:
                closing.set()
                await let_close.wait()
                if translate:
                    # SDK adapters can turn cancellation into an ordinary
                    # transport failure. This must not trigger estimation.
                    raise RuntimeError("adapter stopped") from None
                raise
            return SimpleNamespace(total_tokens=41)

        self.install_count(count)
        caller = asyncio.create_task(peer.run())
        try:
            await asyncio.wait_for(reading.wait(), 1)
            if external:
                caller.cancel()
            else:
                await peer.drop()
                await asyncio.wait_for(peer.scope["state"]["client_connection"].event.wait(), 1)
            await asyncio.wait_for(closing.wait(), 1)
            if external:
                caller.cancel()  # Do not abandon a delayed SDK cleanup on a second cancellation.
                await asyncio.sleep(0)
            self.assertFalse(usages[0].closed)
            self.assertFalse(self.slots.acquire(blocking=False), "actual SDK close still owns the slot")
            self.assertFalse(caller.done())
            self.estimate.assert_not_called()
        finally:
            let_close.set()
            let_finish.set()
            outcome, = await asyncio.gather(caller, return_exceptions=True)
        self.assertEqual(peer.reads, 2, "only the middleware owns raw receive")
        self.assertTrue(usages[0].closed)
        self.assertTrue(self.slots.acquire(blocking=False))
        self.slots.release()
        self.assertFalse(peer.scope["state"]["client_connection"].listeners)
        self.estimate.assert_not_called()
        if external:
            self.assertIsInstance(outcome, asyncio.CancelledError)
            self.assertFalse(peer.messages)
        else:
            self.assertIsNone(outcome)
            start, payload = peer.discarded
            self.assertEqual(start["status"], 499)
            self.assertEqual(json.loads(payload["body"])["type"], "error")

    async def test_disconnect_cancels_and_joins_exact_sdk_count(self):
        await self.check_cancel()

    async def test_disconnect_does_not_estimate_on_translated_sdk_error(self):
        await self.check_cancel(translate=True)

    async def test_server_cancellation_joins_sdk_and_still_propagates(self):
        await self.check_cancel(external=True, translate=True)

    async def test_live_success_and_upstream_failure_keep_counting_contract(self):
        stats = ProxyStats()
        self.enterContext(patch.object(main, "stats", stats))
        self.enterContext(patch.object(claude_api, "stats", stats))
        count = AsyncMock(side_effect=[SimpleNamespace(total_tokens=41), RuntimeError("offline fixture")])
        self.install_count(count)
        for expected in (41, 19):
            peer = self.peer()
            await asyncio.wait_for(peer.run(), 1)
            start, payload = peer.messages
            self.assertEqual(start["status"], 200)
            self.assertEqual(json.loads(payload["body"]), {"input_tokens": expected})
        self.estimate.assert_called_once()
        self.assertEqual(count.await_count, 2)
        self.assertEqual((stats.total_requests, stats.success_requests, stats.error_requests), (2, 2, 0))
        self.assertEqual((stats.prompt_tokens, stats.completion_tokens), (0, 0))

    async def test_rejected_or_unsent_count_is_never_also_a_success(self):
        stats = ProxyStats()
        self.enterContext(patch.object(main, "stats", stats))
        self.enterContext(patch.object(claude_api, "stats", stats))
        count = AsyncMock(return_value=SimpleNamespace(total_tokens=41))
        self.install_count(count)
        invalid = Peer("/v1/messages/count_tokens", payload={"messages": []})
        await invalid.run()
        self.assertEqual((invalid.messages[0]["status"], stats.error_requests), (400, 1))

        lost = self.peer()
        async def drop(message):
            if message["type"] == "http.response.body":
                await lost.drop()
        await lost.run(drop)
        self.assertEqual((stats.total_requests, stats.success_requests, stats.error_requests), (2, 0, 1))
        self.assertEqual((stats.prompt_tokens, stats.completion_tokens), (0, 0))
        count.assert_awaited_once()
