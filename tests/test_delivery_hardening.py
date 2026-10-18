"""Generation isolation, real ASGI delivery failures and bounded recovery."""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-delivery-hardening-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.requests import ClientDisconnect

import config
import main
import resumable
import streaming
import admission
from logger import ProxyStats
from models import OpenAIRequest
from routes import chat_api, gemini_native


def request(headers=None, query=None):
    return SimpleNamespace(headers=headers or {}, query_params=query or {})


async def collect(response):
    return "".join([x.decode() if isinstance(x, bytes) else x async for x in response.body_iterator])


class GenerationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        patch = mock.patch.object(chat_api, "resumable_store", self.store)
        patch.start()
        self.addCleanup(patch.stop)
        self.obj = OpenAIRequest(model="gemini-3.6-flash", messages=[{"role": "user", "content": "hi"}], stream=True)
        self.generations = 0

    async def upstream(self, *args):
        self.generations += 1
        generation = self.generations
        async def frames():
            for part in (0, 1):
                yield 'data: ' + json.dumps({"generation": generation, "part": part}) + '\n\n'
            yield resumable.DONE_FRAME
        return StreamingResponse(frames())

    async def start(self, req=None, obj=None):
        return await chat_api._resumable_stream(req or request(), obj or self.obj, "test-key")

    async def test_old_cursor_cannot_read_new_generation(self):
        with mock.patch.object(chat_api, "_call_upstream", side_effect=self.upstream):
            first = await self.start()
            body = await collect(first)
            cursor = next(line[4:] for line in body.splitlines() if line.startswith("id: "))
            second = await self.start()
            await collect(second)
            resumed = await self.start(request({"last-event-id": cursor}))
            replay = await collect(resumed)
        self.assertNotEqual(first.headers["x-resume-id"], second.headers["x-resume-id"])
        self.assertIn('"generation": 1', replay)
        self.assertNotIn('"generation": 2', replay)
        self.assertIsNotNone(await self.store.get_exact(first.headers["x-resume-id"]))
        self.assertEqual(self.generations, 2)

    async def test_returned_resume_id_is_used_directly_without_original_request_header(self):
        with mock.patch.object(chat_api, "_call_upstream", side_effect=self.upstream):
            first = await self.start(request({"idempotency-key": "original-key"}))
            await collect(first)
            resumed = await self.start(request({"x-resume-id": first.headers["x-resume-id"], "last-event-id": "0"}))
            body = await collect(resumed)
        self.assertEqual(resumed.status_code, 200)
        self.assertIn('"part": 1', body)
        self.assertNotIn('"part": 0', body)
        self.assertEqual(self.generations, 1)

    async def test_idempotency_key_replays_even_after_delivery(self):
        with mock.patch.object(chat_api, "_call_upstream", side_effect=self.upstream):
            first = await self.start(request({"idempotency-key": "one-operation"}))
            original = await collect(first)
            again = await self.start(request({"idempotency-key": "one-operation"}))
            self.assertEqual(await collect(again), original)
            separate = await self.start(request({"idempotency-key": "another-operation"}))
            await collect(separate)
        self.assertEqual(first.headers["x-resume-id"], again.headers["x-resume-id"])
        self.assertNotEqual(first.headers["x-resume-id"], separate.headers["x-resume-id"])
        self.assertEqual(self.generations, 2)

    async def test_mismatched_request_and_conflicting_cursors_are_rejected(self):
        with mock.patch.object(chat_api, "_call_upstream", side_effect=self.upstream):
            first = await self.start()
            await collect(first)
            sid = first.headers["x-resume-id"]
            changed = self.obj.model_copy(update={"model": "different-model"})
            response = await self.start(request({"x-resume-id": sid}), changed)
            self.assertEqual(response.status_code, 409)
            response = await self.start(request({"x-resume-id": sid, "last-event-id": "rid-other:0"}))
            self.assertEqual(response.status_code, 409)
            response = await self.start(request({"last-event-id": "broken"}))
            self.assertEqual(response.status_code, 400)
        self.assertEqual(self.generations, 1)

    async def test_slow_preparation_gets_heartbeat_and_shares_generation(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def setup(*args):
            started.set()
            await release.wait()
            return await self.upstream()
        with mock.patch.object(chat_api, "_call_upstream", side_effect=setup) as call:
            first = await asyncio.wait_for(self.start(), 0.3)
            self.assertTrue(started.is_set())
            self.assertEqual(await anext(first.body_iterator), streaming.HEARTBEAT_FRAME)
            await first.body_iterator.aclose()
            follower = await self.start()
            release.set()
            body = await asyncio.wait_for(collect(follower), 1)
        self.assertIn('"part": 1', body)
        self.assertEqual(call.await_count, 1)

    async def test_real_asgi_failed_done_send_does_not_acknowledge(self):
        body = self.obj.model_dump_json().encode()
        consumed = False
        seen = []
        async def receive():
            nonlocal consumed
            if not consumed:
                consumed = True
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.Event().wait()
        async def send(message):
            if message["type"] == "http.response.body" and b"[DONE]" in message.get("body", b""):
                await asyncio.sleep(0.01)  # allow middleware/producer tasks to advance
                seen.extend(buf.fully_delivered for buf in self.store._buffers.values())
                raise OSError("failed actual network send")
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "POST", "scheme": "http", "path": "/v1/chat/completions",
                 "raw_path": b"/v1/chat/completions", "query_string": b"", "root_path": "",
                 "headers": [(b"content-type", b"application/json"),
                             (b"authorization", ("Bearer " + config.API_KEY).encode())],
                 "client": ("127.0.0.1", 1), "server": ("test", 80)}
        with mock.patch.object(chat_api, "_call_upstream", side_effect=self.upstream), \
             mock.patch.object(chat_api, "resumable_enabled", return_value=True):
            with self.assertRaises(Exception):
                await asyncio.wait_for(main.app(scope, receive, send), 1)
            self.assertEqual(seen, [False])
            self.assertTrue(all(not buf.fully_delivered for buf in self.store._buffers.values()))
            replay = await chat_api._resumable_stream(request(), self.obj, config.API_KEY)
            await collect(replay)
        self.assertEqual(self.generations, 1)

    async def test_browser_can_read_resume_headers(self):
        with mock.patch.object(chat_api, "_call_upstream", side_effect=self.upstream), \
             mock.patch.object(chat_api, "resumable_enabled", return_value=True):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://adapter.test") as client:
                response = await client.post("/v1/chat/completions", json=self.obj.model_dump(),
                                             headers={"Origin": "https://frontend.example", "Authorization": "Bearer " + config.API_KEY})
        self.assertEqual(response.status_code, 200)
        self.assertIn("x-resume-id", response.headers["access-control-expose-headers"].lower())
        self.assertEqual(response.headers["x-accel-buffering"], "no")

    async def test_opt_out_starts_fresh_while_identical_buffer_is_running(self):
        release = asyncio.Event()
        calls = 0

        async def upstream(*args):
            nonlocal calls
            calls += 1
            generation = calls

            async def frames():
                if generation == 1:
                    await release.wait()
                yield 'data: ' + json.dumps({"generation": generation}) + '\n\n'
                yield resumable.DONE_FRAME
            return StreamingResponse(frames())

        with mock.patch.object(chat_api, "_call_upstream", side_effect=upstream), \
             mock.patch.object(chat_api, "resumable_enabled", return_value=True):
            first = await chat_api.chat_completions(request(), self.obj, "test-key")
            buf = await self.store.get_exact(first.headers["x-resume-id"])
            try:
                fresh = await chat_api.chat_completions(request({"x-resumable": "FALSE"}), self.obj, "test-key")
                self.assertIn('"generation": 2', await asyncio.wait_for(collect(fresh), 1))
                self.assertNotIn("x-resume-id", fresh.headers)
                self.assertFalse(buf.done)
                self.assertEqual(calls, 2)
            finally:
                release.set()
            self.assertIn('"generation": 1', await asyncio.wait_for(collect(first), 1))

    async def test_nonstream_opt_out_does_not_create_or_replay_buffer(self):
        obj = self.obj.model_copy(update={"stream": False})
        with mock.patch.object(chat_api, "_call_upstream", return_value=JSONResponse({"answer": "fresh"})) as upstream, \
             mock.patch.object(chat_api, "resumable_enabled", return_value=True):
            for value in ("0", "no", " false "):
                response = await chat_api.chat_completions(request({"x-resumable": value}), obj, "test-key")
                self.assertEqual(json.loads(response.body), {"answer": "fresh"})
            self.assertEqual(upstream.await_count, 3)
        self.assertEqual(await self.store.list_meta(), [])

    async def test_opt_out_rejects_resume_identifiers_without_calling_upstream(self):
        for req in (request({"x-resumable": "false", "x-resume-id": "rid-old"}),
                    request({"x-resumable": "false", "last-event-id": "0"}),
                    request({"x-resumable": "false"}, {"resume_from": "0"})):
            with self.subTest(req=req), mock.patch.object(chat_api, "_call_upstream") as upstream:
                response = await chat_api.chat_completions(req, self.obj, "test-key")
                self.assertEqual(response.status_code, 400)
                upstream.assert_not_called()


class BudgetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)

    async def test_active_capacity_is_rejected_without_cancelling_existing_request(self):
        with mock.patch.object(resumable, "max_streams", return_value=1):
            original, _ = await self.store.get_or_create_request("one", model="m", kind="stream", fingerprint_key="one")
            with self.assertRaises(resumable.BufferCapacityError):
                await self.store.get_or_create_request("two", model="m", kind="stream", fingerprint_key="two")
            self.assertFalse(original.done)
            self.assertIs(await self.store.get_exact(original.id), original)

    async def test_oversized_stream_fails_with_bounded_terminal_error(self):
        buf = await self.store.create("large")
        async def frames():
            yield 'data: {"text":"small"}\n\n'
            yield 'data: ' + 'x' * 20_000 + '\n\n'
        with mock.patch.object(resumable, "_byte_limit", side_effect=lambda key, default: 100 if "buffer" in key else 1000):
            await resumable.pump_stream(buf, frames())
        self.assertEqual(buf.status_code, 503)
        self.assertEqual(buf.status, "error")
        self.assertIn('"small"', "".join(buf.events))
        self.assertLess(buf.bytes_stored, 4096)
        self.assertTrue(buf.events[-1].endswith(resumable.DONE_FRAME))

    async def test_global_byte_budget_and_accounting_release(self):
        one = await self.store.create("one")
        two = await self.store.create("two")
        with mock.patch.object(resumable, "_byte_limit", side_effect=lambda key, default: 100 if "total" in key else 1000):
            await one.append("x" * 80)
            with self.assertRaises(resumable.BufferCapacityError):
                await two.append("x" * 30)
            await self.store.drop("one")
            await two.append("x" * 30)
        self.assertEqual(self.store._bytes_used, 30)

    async def test_oversized_json_does_not_enter_cache(self):
        buf = await self.store.create("json", kind="json")
        with mock.patch.object(resumable, "_byte_limit", return_value=100):
            await buf.finish(payload={"answer": "x" * 10_000})
        self.assertEqual(buf.status_code, 503)
        self.assertNotIn("answer", buf.payload)
        self.assertLess(buf.bytes_stored, 2048)

    async def test_deadline_stops_generation_even_with_connected_reader(self):
        buf = await self.store.create("hung")
        buf.readers = 1
        stopped = asyncio.Event()
        async def operation():
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        with mock.patch.object(resumable, "request_timeout_seconds", return_value=0.01):
            buf.deadline_monotonic = time.monotonic() + 0.01
            with self.assertRaises(TimeoutError):
                await resumable.within_request_deadline(buf, operation)
        self.assertTrue(stopped.is_set())
        self.assertEqual(buf.status_code, 504)
        self.assertTrue(buf.done)

    async def test_sweeper_also_expires_connected_zombies(self):
        buf = await self.store.create("hung-reader")
        buf.deadline_monotonic -= 100000
        buf.readers = 1
        self.assertEqual(await self.store.sweep(), 1)
        self.assertTrue(buf.done)

    async def test_stream_pump_deadline_emits_504_and_terminal_frame(self):
        buf = await self.store.create("stream-timeout")
        closed = asyncio.Event()
        async def source():
            try:
                yield 'data: {"text":"partial"}\n\n'
                await asyncio.Event().wait()
            finally:
                closed.set()
        with mock.patch.object(resumable, "request_timeout_seconds", return_value=0.01):
            buf.deadline_monotonic = time.monotonic() + 0.01
            with self.assertRaises(TimeoutError):
                await resumable.within_request_deadline(buf, lambda: resumable.pump_stream(buf, source()))
        self.assertTrue(closed.is_set())
        self.assertEqual(buf.status_code, 504)
        self.assertEqual(buf.status, "error")
        self.assertIn('"code": 504', "".join(buf.events))
        self.assertTrue(buf.events[-1].endswith(resumable.DONE_FRAME))

    async def test_stream_error_code_is_preserved_in_poll_metadata(self):
        buf = await self.store.create("error")
        async def frames():
            yield 'data: {"error":{"code":429,"message":"quota"}}\n\n'
            yield resumable.DONE_FRAME
        await resumable.pump_stream(buf, frames())
        self.assertEqual(buf.status_code, 429)
        self.assertEqual(buf.status, "error")


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_holds_generation_slot_until_closed(self):
        limiter = admission.GenerationLimiter()
        @admission.limit_generation
        async def call():
            async def source():
                yield "first"
                await asyncio.Event().wait()
            return StreamingResponse(source())
        with mock.patch.object(admission, "generation_limiter", limiter), \
             mock.patch.object(admission.app_state, "get_setting", return_value=1):
            response = await call()
            self.assertEqual(limiter.active, 1)
            with self.assertRaises(admission.GenerationCapacityError):
                await call()
            self.assertEqual(await anext(response.body_iterator), "first")
            await response.body_iterator.aclose()
            await response.body_iterator.aclose()
            self.assertEqual(limiter.active, 0)

    async def test_unstarted_stream_send_failure_releases_slot(self):
        limiter = admission.GenerationLimiter()
        @admission.limit_generation
        async def call():
            async def source():
                yield "unused"
            return StreamingResponse(source())
        async def send(message):
            raise OSError("headers send failed")
        async def receive():
            await asyncio.Event().wait()
        with mock.patch.object(admission, "generation_limiter", limiter):
            response = await call()
            with self.assertRaises(ClientDisconnect):
                await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
        self.assertEqual(limiter.active, 0)

    async def test_detached_generation_keeps_slot_after_frontend_reader_leaves(self):
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        limiter = admission.GenerationLimiter()
        release = asyncio.Event()
        @admission.limit_generation
        async def call(*args):
            async def source():
                await release.wait()
                yield 'data: {"text":"complete"}\n\n'
                yield resumable.DONE_FRAME
            return StreamingResponse(source())
        obj = OpenAIRequest(model="m", messages=[], stream=True)
        with mock.patch.object(admission, "generation_limiter", limiter), \
             mock.patch.object(chat_api, "resumable_store", store), \
             mock.patch.object(chat_api, "_call_upstream", side_effect=call):
            response = await chat_api._resumable_stream(request(), obj, "k")
            await anext(response.body_iterator)
            await response.body_iterator.aclose()
            self.assertEqual(limiter.active, 1)
            release.set()
            await asyncio.gather(*list(store._pumps.values()))
        self.assertEqual(limiter.active, 0)


class NativeRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_raw_chunks_are_framed_before_heartbeats(self):
        release = asyncio.Event()
        payload = 'data: {"text":"你好"}\r\n\r\n'.encode()
        async def source():
            yield payload[:18]
            await release.wait()
            yield payload[18:]
        events = streaming.with_heartbeats(streaming.sse_frames(source()), interval=0.002)
        self.addAsyncCleanup(events.aclose)
        self.assertIs(await anext(events), streaming.HEARTBEAT)
        release.set()
        self.assertEqual(await anext(events), payload)

    async def test_truncated_sse_event_is_an_error(self):
        async def source():
            yield b'data: {"partial":'
        with self.assertRaisesRegex(Exception, "完整结束"):
            _ = [frame async for frame in streaming.sse_frames(source())]

    async def test_native_silent_eof_without_finish_reason_is_reported(self):
        upstream = httpx.Response(200, headers={"content-type": "text/event-stream"},
                                  content=b'data: {"candidates":[{"content":{"parts":[{"text":"partial"}]}}]}\n\n')
        stats = ProxyStats()
        with mock.patch.object(gemini_native, "stats", stats):
            response = await gemini_native._relay_stream(upstream, keepalive=True)
            body = await collect(response)
        self.assertIn('"partial"', body)
        errors = [json.loads(line[6:])["error"] for line in body.splitlines()
                  if line.startswith("data: ") and '"error"' in line]
        self.assertEqual(errors[0]["code"], 502)
        self.assertEqual(stats.error_requests, 1)
        self.assertEqual(stats.success_requests, 0)

    async def test_native_error_event_preserves_status_and_is_not_success(self):
        upstream = httpx.Response(200, headers={"content-type": "text/event-stream"},
                                  content=b'data: {"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota"}}\n\n')
        stats = ProxyStats()
        with mock.patch.object(gemini_native, "stats", stats):
            response = await gemini_native._relay_stream(upstream, keepalive=True)
            body = await collect(response)
        self.assertIn('"code": 429', body)
        self.assertEqual(stats.error_requests, 1)
        self.assertEqual(stats.success_requests, 0)

    async def test_native_sse_heartbeat_and_midstream_error(self):
        closed = []
        class Broken(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'data: {"text":"partial"}\n\n'
                raise httpx.ReadError("connection lost")
            async def aclose(self):
                closed.append(True)
        upstream = httpx.Response(200, stream=Broken(), headers={"content-type": "text/event-stream"})
        stats = ProxyStats()
        with mock.patch.object(gemini_native, "stats", stats):
            response = await gemini_native._relay_stream(upstream, keepalive=True)
            body = await collect(response)
        self.assertTrue(body.startswith(streaming.HEARTBEAT_FRAME))
        self.assertIn('"partial"', body)
        self.assertIn('"error"', body)
        self.assertEqual(stats.success_requests, 0)
        self.assertEqual(stats.error_requests, 1)
        self.assertTrue(closed)

    async def test_opt_in_native_resume_preserves_original_json_without_openai_done(self):
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        async def upstream(*args):
            async def frames():
                yield b'data: {"candidates":[{"index":0,"text":"one"}]}\n\n'
                yield b'data: {"candidates":[{"index":0,"finishReason":"STOP"}]}\n\n'
            return StreamingResponse(frames())
        with mock.patch.object(gemini_native, "resumable_store", store), \
             mock.patch.object(gemini_native, "_passthrough_request", side_effect=upstream) as call:
            first = await gemini_native._resumable_native(request(), "k", "m", "streamGenerateContent", "alt=sse", b'{}', {})
            body = await collect(first)
            again = await gemini_native._resumable_native(
                request({"x-resume-id": first.headers["x-resume-id"], "last-event-id": "0"}),
                "k", "m", "streamGenerateContent", "alt=sse", b'{}', {})
            replay = await collect(again)
        self.assertEqual(call.await_count, 1)
        self.assertNotIn("[DONE]", body)
        self.assertNotIn('"one"', replay)
        self.assertIn('"finishReason":"STOP"', replay)


if __name__ == "__main__":
    unittest.main()
