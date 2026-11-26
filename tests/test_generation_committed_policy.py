"""Generation budgets use saved configuration, not editor drafts or mixed retries."""
import asyncio
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from fastapi.responses import JSONResponse
import httpx
import generation_policy
import admission
from main import app
import resumable
import retry_policy
from routes import chat_api
import runtime_state


class SavedGenerationPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.enterContext(patch.object(runtime_state, "STATE_FILE", str(Path(folder) / "state.json")))
        self.state = runtime_state.AppState()
        self.old = dict(upstream_max_concurrency=1, request_timeout_seconds=1800,
                        resumable_max_buffer_mb=5, resumable_max_total_mb=8, resumable_max_streams=7,
                        retry_max=0, retry_backoff_seconds=.5,
                        retry_jitter_seconds=.25, retry_backoff_mode="fixed")
        self.new = dict(upstream_max_concurrency=2, request_timeout_seconds=3,
                        resumable_max_buffer_mb=2, resumable_max_total_mb=4, resumable_max_streams=3,
                        retry_max=3, retry_backoff_seconds=1,
                        retry_jitter_seconds=0, retry_backoff_mode="exponential")
        self.state.update_settings(self.old)
        for module in (resumable, retry_policy, admission, generation_policy):
            self.enterContext(patch.object(module, "app_state", self.state))
        self.limiter = admission.GenerationLimiter(traced=False)
        self.store = resumable.ResumableStore()
        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.addAsyncCleanup(self.store.aclose)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                        headers={"Authorization": "Bearer test-key"})
        self.addAsyncCleanup(self.client.aclose)

    async def post(self):
        return await self.client.post("/v1/chat/completions", json={"model": "gemini-test", "messages": []})

    def retry_state(self):
        # Supplying zero jitter lets us observe the mode exactly without sleeping.
        settings = retry_policy.get_retry_settings()
        return (*settings, retry_policy.retry_delay(None, settings[1], 2, jitter=0))

    async def test_draft_failure_never_admits_or_shortens_a_buffer(self):
        writing, release = threading.Event(), threading.Event()
        def reject(_):
            writing.set()
            assert release.wait(5)
            raise OSError("simulated offline persistence failure")
        observations = []
        async def observe(*_):
            observations.append(self.retry_state())
            return JSONResponse({"choices": []})
        lease = self.limiter.reserve()
        self.addCleanup(lease.release)
        with patch.object(self.state, "_write_snapshot", reject), patch.object(chat_api, "_call_upstream", observe):
            task = asyncio.create_task(asyncio.to_thread(self.state.update_settings, self.new))
            try:
                self.assertTrue(await asyncio.to_thread(writing.wait, 5))
                self.assertEqual(self.state.get_setting("retry_max"), 3)  # inspector still sees its draft
                result = await self.post()
                self.assertEqual(result.status_code, 200)
                buf = await self.store.get_exact(result.headers["x-resume-id"])
                self.assertAlmostEqual(buf.deadline_monotonic - buf.started_monotonic, 1800)
                self.assertEqual(buf.max_buffer_bytes, 5 * 1024 * 1024)
                self.assertEqual(resumable.max_streams(), 7)
                self.assertEqual(resumable._byte_limit("resumable_max_total_mb", 256), 8 * 1024 * 1024)
                self.assertEqual(observations, [(0, .5, .25, .5)])
                with self.assertRaises(admission.GenerationCapacityError):
                    self.limiter.reserve()
            finally:
                release.set()
                with self.assertRaises(runtime_state.StatePersistenceError):
                    await task
        self.assertEqual(self.state.get_setting("retry_max"), 0)

    async def test_staged_tighter_gate_does_not_reject_before_a_successful_save(self):
        self.state.update_settings(self.new)
        writing, release = threading.Event(), threading.Event()
        write = self.state._write_snapshot
        def hold(snapshot):
            writing.set()
            assert release.wait(5)
            write(snapshot)
        with patch.object(self.state, "_write_snapshot", hold):
            task = asyncio.create_task(asyncio.to_thread(self.state.update_settings, self.old))
            try:
                self.assertTrue(await asyncio.to_thread(writing.wait, 5))
                leases = [self.limiter.reserve(), self.limiter.reserve()]
                for lease in leases:
                    lease.release()
                self.assertEqual(self.retry_state(), (3, 1, 0, 4))
            finally:
                release.set()
                await task
        lease = self.limiter.reserve()
        self.addCleanup(lease.release)
        with self.assertRaises(admission.GenerationCapacityError):
            self.limiter.reserve()
        self.assertEqual(self.retry_state(), (0, .5, .25, .5))

    async def test_committed_change_only_updates_later_generations_and_live_shared_gates(self):
        entered, proceed = asyncio.Event(), asyncio.Event()
        observations = []
        async def upstream(*_):
            observations.append(self.retry_state())
            if len(observations) == 1:
                entered.set()
                await proceed.wait()
                observations.append(self.retry_state())
            return JSONResponse({"choices": []})
        with patch.object(chat_api, "_call_upstream", upstream):
            task = asyncio.create_task(self.post())
            try:
                await asyncio.wait_for(entered.wait(), 2)
                await asyncio.to_thread(self.state.update_settings, self.new)
                self.assertEqual(resumable.max_streams(), 3)
                self.assertEqual(resumable._byte_limit("resumable_max_total_mb", 256), 4 * 1024 * 1024)
                leases = [self.limiter.reserve(), self.limiter.reserve()]
                for lease in leases:
                    lease.release()
            finally:
                proceed.set()
            before = await task
            after = await self.post()
        self.assertEqual(observations, [(0, .5, .25, .5), (0, .5, .25, .5), (3, 1, 0, 4)])
        for result, timeout, mb in ((before, 1800, 5), (after, 3, 2)):
            self.assertEqual(result.status_code, 200)
            buf = await self.store.get_exact(result.headers["x-resume-id"])
            self.assertAlmostEqual(buf.deadline_monotonic - buf.started_monotonic, timeout)
            self.assertEqual(buf.max_buffer_bytes, mb * 1024 * 1024)
