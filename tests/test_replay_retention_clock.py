"""Wall-clock corrections do not change replay TTLs or eviction order."""
import time
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.responses import JSONResponse
from main import app
import resumable
from routes import chat_api


class ReplayClock:
    def __init__(self):
        self.wall = time.time()
        self.tick = time.monotonic()

    def time(self):
        return self.wall

    def monotonic(self):
        return self.tick


class ReplayRetentionClockTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.clock = ReplayClock()
        self.enterContext(patch.object(resumable, "time", self.clock))
        self.enterContext(patch.object(resumable, "ttl_seconds", return_value=600))

    async def test_keyed_post_clock_jumps_keep_one_generation_until_elapsed_ttl(self):
        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        upstream = AsyncMock(return_value=JSONResponse({"choices": []}))
        self.enterContext(patch.object(chat_api, "_call_upstream", upstream))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                     headers={"Authorization": "Bearer test-key", "Idempotency-Key": "clock"}) as client:
            async def post():
                result = await client.post("/v1/chat/completions", json={"model": "gemini-test", "messages": []})
                self.assertEqual(result.status_code, 200)
                return result.headers["x-resume-id"]
            receipt = await post()
            finished_wall = self.clock.wall
            self.clock.wall += 3600
            self.assertEqual(await self.store.sweep(), 0)
            self.assertEqual(await post(), receipt)
            self.clock.wall -= 7200
            self.clock.tick += 599
            buf = await self.store.get_exact(receipt)
            self.assertEqual(buf.meta()["finished_at"], finished_wall)
            self.assertEqual(buf.meta()["age_seconds"], 599)
            self.assertEqual(await self.store.sweep(), 0)
            self.assertEqual(await post(), receipt)
            upstream.assert_awaited_once()
            self.clock.tick += 2
            self.assertEqual(await self.store.sweep(), 1)
            self.assertNotEqual(await post(), receipt)
            self.assertEqual(upstream.await_count, 2)

    async def test_capacity_pressure_evicts_oldest_despite_clock_step(self):
        async def admit(key):
            buf, _ = await self.store.get_or_create_request(key, model="test", kind="stream", fingerprint_key=key)
            await buf.fail("fixture")
            return buf
        with patch.object(resumable, "max_streams", return_value=2):
            first = await admit("first")
            self.clock.tick += 1
            self.clock.wall -= 3600
            second = await admit("second")
            self.clock.tick += 1
            self.clock.wall -= 3600
            third = await admit("third")
            self.assertIsNone(await self.store.get_exact(first.id))
            self.assertIs(await self.store.get_exact(second.id), second)
        with patch.object(resumable, "max_streams", return_value=1):
            self.clock.wall -= 3600
            await self.store.sweep()
            self.assertIsNone(await self.store.get_exact(second.id))
            self.assertIs(await self.store.get_exact(third.id), third)
