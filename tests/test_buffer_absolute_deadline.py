"""Absolute execution deadlines are not upstream timeouts and cannot move on handoff."""
import asyncio
import inspect
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.responses import JSONResponse

from admission import ManagedStreamingResponse
from main import app
from response_validation import IncompleteResponseError
import resumable
from routes import chat_api, gemini_native
from upstreams import native_passthrough


class BufferAbsoluteDeadlineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Only the buffer's view of elapsed time moves. The asyncio loop and
        # cleanup watchdogs continue to run on the real monotonic clock.
        self.now = time.monotonic()
        self.enterContext(patch.object(resumable, "time", SimpleNamespace(
            monotonic=lambda: self.now, time=time.time)))
        self.enterContext(patch.object(resumable, "request_timeout_seconds", return_value=30))

    async def test_expired_budget_never_invokes_factory_and_closes_unstarted_coroutine(self):
        for prepared in (False, True):
            with self.subTest(prepared=prepared):
                buf = resumable.ResumableBuffer("preflight")
                self.now = buf.deadline_monotonic
                factory = AsyncMock(return_value="already too late")
                work = factory() if prepared else factory
                try:
                    with self.assertRaises(TimeoutError):
                        await resumable.within_request_deadline(buf, work)
                    factory.assert_not_awaited()
                    if prepared:
                        self.assertEqual(inspect.getcoroutinestate(work), inspect.CORO_CLOSED)
                    else:
                        factory.assert_not_called()
                finally:
                    if prepared and inspect.getcoroutinestate(work) != inspect.CORO_CLOSED:
                        work.close()
                self.assertEqual((buf.status_code, buf.payload["error"]["type"]), (504, "timeout"))

    async def test_sync_overshoot_cannot_return_success_without_yielding_to_the_timer(self):
        buf = resumable.ResumableBuffer("late")
        async def overshoot():
            self.now = buf.deadline_monotonic
            return 42
        with self.assertRaises(TimeoutError):
            await resumable.within_request_deadline(buf, overshoot)
        self.assertEqual((buf.status_code, buf.payload["error"]["type"]), (504, "timeout"))

    async def test_early_upstream_timeout_retains_reason_and_identity(self):
        buf = resumable.ResumableBuffer("upstream")
        error = TimeoutError("upstream read timer")
        with self.assertRaises(TimeoutError) as caught:
            await resumable.within_request_deadline(buf, AsyncMock(side_effect=error))
        self.assertIs(caught.exception, error)
        self.assertEqual(buf.status_code, 504)
        self.assertEqual(buf.payload["error"]["type"], "upstream_error")
        self.assertEqual(buf.error, "upstream read timer")

    async def test_watchdog_does_not_block_store_on_json_size_calculation(self):
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        buf, _ = await store.get_or_create_request("one", model="m", kind="json", fingerprint_key="f")
        entered, release = asyncio.Event(), asyncio.Event()
        async def size(_payload):
            entered.set()
            await release.wait()
            return 42
        with patch.object(resumable, "json_size_offloop", size):
            finishing = asyncio.create_task(buf.finish(payload={"answer": "too late"}))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                self.now = buf.deadline_monotonic
                await asyncio.wait_for(store.sweep(), 1)
                self.assertIs(await store.get_exact(buf.id), buf)
                self.assertEqual(buf.status_code, 504)
            finally:
                release.set()
                await finishing
        self.assertEqual(buf.payload["error"]["code"], 504)
        self.assertEqual(store._bytes_used, buf.bytes_stored)

    async def test_timely_terminal_remains_authoritative_after_slow_cleanup(self):
        buf = resumable.ResumableBuffer("committed")
        async def operation():
            await buf.finish(terminal_frame=resumable.DONE_FRAME)
            self.now = buf.deadline_monotonic + 1
        await resumable.within_request_deadline(buf, operation)
        self.assertEqual((buf.status, buf.status_code, buf.next_seq), ("done", 200, 1))

    async def test_http_json_settlement_and_stream_handoff_do_not_admit_late_success(self):
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(chat_api, "resumable_store", store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                   headers={"Authorization": "Bearer test-key"})
        self.addAsyncCleanup(client.aclose)
        reads = []
        closed = asyncio.Event()
        async def events():
            reads.append(True)
            yield 'data: {"choices":[]}\n\n'
        async def close():
            closed.set()
        async def upstream(req, _client):
            self.now += 31
            if req.stream:
                return ManagedStreamingResponse(events(), cleanup=close)
            return JSONResponse({"answer": "too late"})
        with patch.object(chat_api, "_call_upstream", side_effect=upstream):
            for stream in (False, True):
                response = await client.post("/v1/chat/completions", json={
                    "model": "gemini-test", "stream": stream, "messages": []})
                # A fast setup error may use HTTP 504; a started SSE returns an
                # in-band 504. In neither case can a late success escape.
                if response.headers.get("content-type", "").startswith("text/event-stream"):
                    errors = [json.loads(line[6:]) for line in response.text.splitlines()
                              if line.startswith("data: ") and line != "data: [DONE]"]
                    self.assertEqual([d["error"]["code"] for d in errors], [504])
                else:
                    self.assertEqual(response.status_code, 504, response.text)
                    self.assertEqual(response.json()["error"]["code"], 504)
            await asyncio.wait_for(closed.wait(), 1)
            self.assertFalse(reads)
        self.assertTrue(all(buf.status_code == 504 for buf in store._buffers.values()))

    async def test_expiring_fast_setup_returns_the_canonical_504_and_its_receipt(self):
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                   headers={"Authorization": "Bearer test-key", "X-Resumable": "true"})
        self.addAsyncCleanup(client.aclose)
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.enterContext(patch.object(gemini_native, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        for route, dispatch, path, body in (
                (chat_api, "_call_upstream", "/v1/chat/completions",
                 {"model": "gemini-test", "stream": True, "messages": []}),
                (native_passthrough, "_passthrough_request", "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
                 {"contents": []})):
            started = asyncio.Event()
            async def prepare(*args):
                started.set()
                await asyncio.Event().wait()
            with self.subTest(path=path), patch.object(route, "resumable_store", store), \
                    patch.object(route, "SETUP_FAST_WINDOW_SECONDS", 10), patch.object(route, dispatch, prepare):
                initial = asyncio.create_task(client.post(path, json=body))
                await asyncio.wait_for(started.wait(), 1)
                self.now += 31
                await store.sweep()
                reply = await asyncio.wait_for(initial, 1)
                self.assertEqual(reply.status_code, 504)
                receipt = await store.get_exact(reply.headers["x-resume-id"])
                self.assertEqual(reply.json(), receipt.payload)
                self.assertEqual(receipt.status_code, 504)
                if route is native_passthrough:
                    self.assertEqual(reply.json()["error"]["status"], "DEADLINE_EXCEEDED")
                else:
                    self.assertEqual(reply.json()["error"]["type"], "timeout")

    async def test_stream_setup_failures_are_classified_before_the_receipt_can_be_dropped(self):
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                   headers={"Authorization": "Bearer test-key", "X-Resumable": "true"})
        self.addAsyncCleanup(client.aclose)
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.enterContext(patch.object(gemini_native, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        for route, dispatch, path, body in (
                (chat_api, "_call_upstream", "/v1/chat/completions",
                 {"model": "gemini-test", "stream": True, "messages": []}),
                (native_passthrough, "_passthrough_request", "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
                 {"contents": []})):
            for late, returned in ((False, False), (False, True), (True, False), (True, True)):
                async def prepare(*args):
                    if late:
                        # No await here: the scheduled timer has not run yet.
                        self.now += 31
                    if returned:
                        return JSONResponse({"upstream": "early rejection"}, status_code=429)
                    raise IncompleteResponseError("incomplete setup")

                hdr = {"Idempotency-Key": f"{late}-{returned}"}
                with self.subTest(path=path, late=late, returned=returned), \
                        patch.object(route, "resumable_store", store), \
                        patch.object(route, "SETUP_FAST_WINDOW_SECONDS", 10), \
                        patch.object(route, dispatch, AsyncMock(side_effect=prepare)) as upstream:
                    response = await client.post(path, json=body, headers=hdr)
                    if not late:
                        self.assertEqual(response.status_code, 429 if returned else 502, response.text)
                        if route is chat_api or returned:
                            existing = await store.get_exact(response.headers["x-resume-id"])
                            self.assertIsNotNone(existing)
                            self.assertEqual(existing.status_code, response.status_code)
                        else:
                            self.assertNotIn("x-resume-id", response.headers)
                        if not returned:
                            self.assertIn("incomplete setup", response.text)
                        continue
                    self.assertEqual(response.status_code, 504, response.text)
                    rid = response.headers["x-resume-id"]
                    receipt = await store.get_exact(rid)
                    self.assertEqual(response.json(), receipt.payload)
                    replay = await client.post(path, json=body, headers={"X-Resume-Id": rid, **hdr})
                    self.assertEqual(replay.headers["x-resume-id"], rid)
                    self.assertNotIn("incomplete setup", replay.text)
                    payloads = [json.loads(line[6:]) for line in replay.text.splitlines()
                                if line.startswith("data: ") and line != "data: [DONE]"]
                    self.assertEqual(payloads, [receipt.payload])
                    upstream.assert_awaited_once()

    async def test_late_frame_and_terminal_do_not_overwrite_an_in_budget_prefix(self):
        buf = resumable.ResumableBuffer("stream")
        async def frames():
            yield 'data: {"prefix":1}\n\n'
            self.now = buf.deadline_monotonic + 1
            yield 'data: {"late":2}\n\n'
            yield resumable.DONE_FRAME
        with self.assertRaises(TimeoutError):
            await resumable.within_request_deadline(buf, lambda: resumable.pump_stream(buf, frames()))
        self.assertEqual(buf.status_code, 504)
        self.assertEqual(len(buf.events), 3)
        self.assertIn('"prefix":1', buf.events[0])
        self.assertIn('"code": 504', buf.events[1])
        self.assertTrue(buf.events[-1].endswith(resumable.DONE_FRAME))
