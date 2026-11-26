"""A deadline ends delivery, not ownership of a still-closing upstream."""
import asyncio
import json
import time
import unittest
from unittest.mock import patch

import httpx
from fastapi.responses import StreamingResponse

from admission import ManagedStreamingResponse
from main import app
import resumable
from routes import chat_api


class ReplayDeadlineOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                        headers={"Authorization": "Bearer test-key"})
        self.addAsyncCleanup(self.client.aclose)

    async def check_deadline(self, *, timer_first, idempotent):
        path = "/v1/chat/completions"
        body = {"model": "gemini-test", "stream": True, "messages": []}
        headers = {"Idempotency-Key": "one operation"} if idempotent else {}
        reading, unwinding, release_pump, closing, release_close = (asyncio.Event() for _ in range(5))

        async def source():
            try:
                yield 'data: {"text":"partial"}\n\n'
                reading.set()
                await asyncio.Event().wait()
            finally:
                unwinding.set()
                await release_pump.wait()

        async def close():
            closing.set()
            await release_close.wait()

        async def fresh():
            yield resumable.DONE_FRAME

        async def post(extra=None):
            return await self.client.post(path, json=body, headers={**headers, **(extra or {})})

        with patch.object(chat_api, "_call_upstream", side_effect=[
                 ManagedStreamingResponse(source(), cleanup=close), StreamingResponse(fresh())]) as upstream, \
             patch.object(resumable, "request_timeout_seconds", return_value=.3 if timer_first else 60):
            initial = asyncio.create_task(post())
            try:
                await asyncio.wait_for(reading.wait(), 2)
                rid, = self.store._buffers
                buf = await self.store.get_exact(rid)
                if timer_first:
                    # No manual sweep. Readers must see the actual deadline
                    # while cancellation is still releasing the upstream.
                    await asyncio.wait_for(unwinding.wait(), 2)
                    await asyncio.wait_for(asyncio.shield(initial), .5)
                else:
                    buf.deadline_monotonic = time.monotonic() - 1
                await asyncio.wait_for(self.store.sweep(), 1)
                await asyncio.wait_for(unwinding.wait(), 1)
                self.assertIs(await self.store.get_exact(rid), buf)
                self.assertIs(await self.store.get(buf.request_key), buf)
                self.assertEqual(self.store._bytes_used, buf.bytes_stored)
                first = await asyncio.wait_for(asyncio.shield(initial), 1)
                replay = await post({"X-Resume-Id": rid})
                for response in (first, replay):
                    self.assertEqual(response.headers["x-resume-id"], rid)
                    values = [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]
                    self.assertEqual(len(values), 3)
                    self.assertEqual(json.loads(values[0]), {"text": "partial"})
                    self.assertEqual((json.loads(values[1])["error"]["code"], values[2]), (504, "[DONE]"))
                self.assertTrue(buf.fully_delivered)
                # Even the complete closing response, not just its pump, is an owner.
                release_pump.set()
                await asyncio.wait_for(closing.wait(), 1)
                with patch.object(resumable, "SETUP_CLOSE_TIMEOUT_SECONDS", .02):
                    twins = await asyncio.gather(post(), post())
                if idempotent:
                    self.assertEqual([r.headers["x-resume-id"] for r in twins], [rid, rid])
                else:
                    self.assertEqual([r.status_code for r in twins], [503, 503])
                self.assertEqual(upstream.await_count, 1)
                buf.finished_monotonic -= 86400
                self.assertEqual(await self.store.sweep(), 0)
                self.assertIs(await self.store.get(buf.request_key), buf)
                self.assertEqual(self.store._bytes_used, buf.bytes_stored)
            finally:
                release_pump.set()
                release_close.set()
                await asyncio.gather(initial, return_exceptions=True)
            await asyncio.wait_for(asyncio.gather(*list(self.store._cleanup_tasks)), 1)
            if idempotent:
                self.assertEqual((await post()).headers["x-resume-id"], rid)
                self.assertEqual(upstream.await_count, 1)
                self.assertEqual(await self.store.sweep(), 1)
                self.assertEqual(self.store._bytes_used, 0)
                self.assertNotEqual((await post()).headers["x-resume-id"], rid)
            else:
                # No sweep is needed to admit a fresh ordinary request after close.
                replacement = (await post()).headers["x-resume-id"]
                self.assertNotEqual(replacement, rid)
                self.assertEqual(await self.store.sweep(), 1)
                owner = await self.store.get(buf.request_key)
                self.assertEqual(owner.id, replacement)
                self.assertEqual(self.store._bytes_used, owner.bytes_stored)
            self.assertEqual(upstream.await_count, 2)

    async def test_actual_timer_then_watchdog_preserves_ordinary_owner(self):
        await self.check_deadline(timer_first=True, idempotent=False)

    async def test_watchdog_then_timer_preserves_idempotent_receipt(self):
        await self.check_deadline(timer_first=False, idempotent=True)

    async def test_deadline_never_interrupts_a_timely_terminals_cleanup(self):
        committed, release, cleaned = (asyncio.Event() for _ in range(3))
        buf = resumable.ResumableBuffer("timely-close")
        buf.deadline_monotonic = time.monotonic() + .07

        async def operation():
            await buf.finish(terminal_frame=resumable.DONE_FRAME)
            committed.set()
            await release.wait()
            cleaned.set()

        worker = asyncio.create_task(resumable.within_request_deadline(buf, operation))
        try:
            await asyncio.wait_for(committed.wait(), 1)
            await asyncio.sleep(.1)
            self.assertFalse(worker.done())
            self.assertTrue(buf.done)
            self.assertEqual(buf.status_code, 200)
            self.assertEqual(buf.next_seq, 1)
        finally:
            release.set()
            await asyncio.gather(worker, return_exceptions=True)
        self.assertTrue(cleaned.is_set())

    async def test_sweeper_first_never_cancels_an_unwinding_worker_twice(self):
        buf, _ = await self.store.create_if_absent("sweeper-first")
        buf.deadline_monotonic = time.monotonic() + .07
        reading, closing, release, cleaned = (asyncio.Event() for _ in range(4))

        async def operation():
            try:
                reading.set()
                await asyncio.Event().wait()
            finally:
                closing.set()
                await release.wait()
                cleaned.set()

        worker = asyncio.create_task(resumable.within_request_deadline(buf, operation))
        self.store.register_pump(buf.id, worker, buffer=buf)
        try:
            await asyncio.wait_for(reading.wait(), 1)
            # Advance only the buffer deadline, not the operation's already
            # scheduled loop wake. The actual alarm fires during this close.
            buf.deadline_monotonic = time.monotonic() - 1
            await self.store.sweep()
            await asyncio.wait_for(closing.wait(), 1)
            await asyncio.sleep(.1)
            self.assertFalse(worker.done() or cleaned.is_set())
            self.assertIs(self.store._live_pump(buf), worker)
            self.assertEqual(buf.status_code, 504)
        finally:
            release.set()
            await asyncio.gather(worker, return_exceptions=True)
        self.assertTrue(cleaned.is_set())
        self.assertTrue(worker.cancelled())

    async def test_json_waiter_gets_the_authoritative_timeout_during_slow_cleanup(self):
        closing, release = asyncio.Event(), asyncio.Event()

        async def upstream(*_):
            try:
                await asyncio.Event().wait()
            finally:
                closing.set()
                await release.wait()

        body = {"model": "gemini-test", "stream": False, "messages": []}
        with patch.object(resumable, "request_timeout_seconds", return_value=.07), \
                patch.object(chat_api, "_call_upstream", side_effect=upstream) as call:
            initial = asyncio.create_task(self.client.post("/v1/chat/completions", json=body))
            try:
                await asyncio.wait_for(closing.wait(), 1)
                response = await asyncio.wait_for(asyncio.shield(initial), .5)
                buf = await self.store.get_exact(response.headers["x-resume-id"])
                self.assertEqual(response.status_code, 504)
                self.assertEqual(response.json(), buf.payload)
                self.assertTrue(buf.fully_delivered)
                replay = await self.client.get("/v1/resumable/" + buf.id)
                self.assertEqual(replay.json()["payload"], buf.payload)
                self.assertIsNotNone(self.store._live_pump(buf))
                self.assertEqual(call.await_count, 1)
            finally:
                release.set()
                await initial

    async def test_fast_setup_window_does_not_delay_a_shorter_absolute_budget(self):
        from upstreams import native_passthrough
        for native in (False, True):
            with self.subTest(native=native):
                closing, release = asyncio.Event(), asyncio.Event()
                body = ({"contents": [{"parts": [{"text": "hi"}]}]} if native else
                        {"model": "gemini-test", "stream": True, "messages": []})
                path = ("/v1beta/models/gemini-test:streamGenerateContent?alt=sse"
                        if native else "/v1/chat/completions")

                async def slow_setup(*_args, **_kwargs):
                    try:
                        await asyncio.Event().wait()
                    finally:
                        closing.set()
                        await release.wait()

                with patch.object(app.state, "express_key_manager", None), \
                        patch.object(native_passthrough, "resumable_store", self.store), \
                        patch.object(native_passthrough, "SETUP_FAST_WINDOW_SECONDS", 2), \
                        patch.object(chat_api, "SETUP_FAST_WINDOW_SECONDS", 2), \
                        patch.object(resumable, "request_timeout_seconds", return_value=.08), \
                        patch.object(chat_api, "_call_upstream", side_effect=slow_setup), \
                        patch.object(native_passthrough, "_passthrough_request", side_effect=slow_setup):
                    call = asyncio.create_task(self.client.post(path, json=body, headers={"X-Resumable": "true"}))
                    try:
                        await asyncio.wait_for(closing.wait(), 1)
                        result = await asyncio.wait_for(asyncio.shield(call), .5)
                        buf = await self.store.get_exact(result.headers["x-resume-id"])
                        self.assertEqual(buf.status_code, 504)
                        self.assertIn('"code": 504', result.text)
                        self.assertIsNotNone(self.store._live_pump(buf))
                    finally:
                        release.set()
                        await call

    async def test_unused_alarms_are_released_when_a_worker_exits(self):
        loop = asyncio.get_running_loop()
        schedule = loop.call_later
        for outcome in ("return", "error", "cancel"):
            with self.subTest(outcome=outcome):
                handles = []
                entered = asyncio.Event()
                buf = resumable.ResumableBuffer("release-alarm")
                buf.deadline_monotonic = time.monotonic() + 600

                def capture(delay, callback, *args, **kwargs):
                    handle = schedule(delay, callback, *args, **kwargs)
                    if 590 <= delay <= 600:
                        handles.append(handle)
                    return handle

                async def work():
                    entered.set()
                    if outcome == "error":
                        raise ValueError("ordinary failure")
                    if outcome == "cancel":
                        await asyncio.Event().wait()

                with patch.object(loop, "call_later", capture):
                    task = asyncio.create_task(resumable.within_request_deadline(buf, work))
                    await asyncio.wait_for(entered.wait(), 1)
                    if outcome == "cancel":
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                self.assertEqual(len(handles), 1)
                self.assertTrue(handles[0].cancelled())
