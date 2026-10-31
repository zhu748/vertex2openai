"""Request diagnostics across real ASGI delivery, detached streams and retries."""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-request-tracking-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.responses import StreamingResponse
from google.genai import types

import api_helpers
import config
import console_auth
import main
import middleware
import request_tracking as tracking
import resumable
from logger import ProxyStats
from routes import chat_api, console, gemini_native


def frame(text="answer", reasoning=None, finish=None):
    delta = {"reasoning_content": reasoning} if reasoning else {"content": text}
    return 'data: ' + json.dumps({"choices": [{"delta": delta, "finish_reason": finish}]}, ensure_ascii=False) + '\n\n'


class DiagnosticTests(unittest.TestCase):
    def test_split_utf8_and_heartbeats_do_not_produce_false_first_text(self):
        trace = tracking.RequestTrace("/v1/chat/completions")
        observer = tracking.PayloadObserver(trace)
        observer.feed(': keep-alive\n\ndata: {"choices":[{"delta":{"role":"assistant","content":""}}]}\n\n')
        self.assertNotIn("first_content", trace.timings)
        observer.feed(frame(reasoning="thought"))
        self.assertNotIn("first_content", trace.timings)
        raw = frame("你好").encode()
        split = raw.index('你'.encode()) + 1
        observer.feed(raw[:split])
        self.assertNotIn("first_content", trace.timings)
        observer.feed(raw[split:])
        self.assertEqual(trace.data["content_chars"], 2)
        self.assertEqual(trace.data["reasoning_chars"], 7)
        self.assertLessEqual(trace.timings["first_reasoning"]["elapsed_ms"], trace.timings["first_content"]["elapsed_ms"])

    def test_native_events_preserve_separate_thought_and_body_metrics(self):
        trace = tracking.RequestTrace("/v1beta/models/m:streamGenerateContent")
        observer = tracking.PayloadObserver(trace)
        observer.feed('data: ' + json.dumps({"candidates": [{"content": {"parts": [
            {"text": "think", "thought": True}, {"text": "body"}]}, "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 10}}) + '\n\n')
        self.assertEqual(trace.data["content_chars"], 4)
        self.assertEqual(trace.data["reasoning_chars"], 5)
        self.assertEqual(trace.data["usage"], {"promptTokenCount": 10})
        self.assertEqual(trace.data["finish_reasons"], ["STOP"])

    def test_stream_error_remains_error_after_successful_http_delivery(self):
        trace = tracking.RequestTrace("/v1/chat/completions")
        trace.start_generation()
        tracking.PayloadObserver(trace).feed('data: {"error":{"code":429,"message":"busy"}}\n\n')
        trace.end_generation()
        trace.end_connection()
        self.assertEqual(trace.snapshot()["status"], "error")
        self.assertEqual(trace.snapshot()["error_code"], 429)

    def test_history_limits_keep_live_background_requests(self):
        registry = tracking.RequestRegistry(max_completed=1, max_bytes=1024)
        active = registry.create("/chat")
        active.start_generation()
        active.end_connection(disconnected=True)
        for _ in range(3):
            trace = registry.create("/chat")
            trace.log("x" * 2000)
            trace.end_connection()
        rows = registry.list()
        self.assertIsNotNone(registry.get(active.id))
        self.assertLessEqual(len(rows["items"]), 2)
        self.assertEqual(rows["summary"]["background"], 1)
        self.assertEqual(rows["summary"]["connections"], 0)
        self.assertEqual(rows["summary"]["generations"], 1)

    def test_diagnostics_are_bounded_and_redact_credentials(self):
        trace = tracking.RequestTrace("/chat")
        secret = "https://user:password@proxy.test/?key=private&access_token=token Bearer secret"
        for _ in range(220):
            trace.log(secret + "x" * 10000)
        self.assertEqual(len(trace.logs), 200)
        self.assertTrue(all(len(item["message"]) <= 2000 for item in trace.logs))
        result = str(trace.snapshot(detail=True))
        for value in ("user:password", "key=private", "access_token=token", "Bearer secret"):
            self.assertNotIn(value, result)
        self.assertLess(trace.retained_bytes, 500000)

    def test_oversized_event_does_not_block_following_diagnostics(self):
        trace = tracking.RequestTrace("/chat")
        observer = tracking.PayloadObserver(trace)
        with mock.patch.object(tracking, "MAX_INSPECTION_BYTES", 100):
            observer.feed(b'data: ' + b'x' * 200 + b'\n\n')
        observer.feed(frame("ok"))
        self.assertEqual(trace.data["content_chars"], 2)
        self.assertGreater(trace.data["inspection_skipped_bytes"], 0)

    def test_malformed_diagnostics_cannot_interrupt_delivery_or_poison_json(self):
        trace = tracking.RequestTrace("/chat")
        observer = tracking.PayloadObserver(trace)
        for data in ('{"choices":[null]}', '[' * 2000 + '0' + ']' * 2000,
                     '{"usage":{"invalid":NaN,"total_tokens":2}}'):
            observer.feed('data: ' + data + '\n\n')
        observer.feed(frame("still running"))
        self.assertEqual(trace.data["content_chars"], 13)
        self.assertEqual(trace.data["usage"], {"total_tokens": 2})
        json.dumps(trace.snapshot(detail=True), allow_nan=False)


class MiddlewareErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_upstream_oserror_is_not_misclassified_as_frontend_disconnect(self):
        registry, stats = tracking.RequestRegistry(), ProxyStats()
        async def app(scope, receive, send):
            raise OSError("upstream socket failed before response")
        async def noop(*args):
            pass
        with mock.patch.object(middleware, "request_registry", registry):
            with self.assertRaises(OSError):
                await middleware.StatsMiddleware(app, stats)({
                    "type": "http", "method": "POST", "path": "/v1/chat/completions", "query_string": b"",
                    "headers": [(b"authorization", b"Bearer " + config.API_KEY.encode())]}, noop, noop)
        self.assertEqual(stats.error_requests, 1)
        self.assertEqual(registry.list()["items"][0]["status"], "error")


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.registry = tracking.RequestRegistry()
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        for patch in (
            mock.patch.object(middleware, "request_registry", self.registry),
            mock.patch.object(console, "request_registry", self.registry),
            mock.patch.object(chat_api, "resumable_store", self.store),
            mock.patch.object(chat_api.app_state, "is_web_proxy_enabled", return_value=False),
            mock.patch.object(chat_api, "resumable_enabled", return_value=True),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.payload = {"model": "gemini-test", "messages": [{"role": "user", "content": "hi"}], "stream": True}
        self.headers = {"Authorization": "Bearer " + config.API_KEY}

    async def asgi(self, send, headers=None):
        body = json.dumps(self.payload).encode()
        consumed = False

        async def receive():
            nonlocal consumed
            if not consumed:
                consumed = True
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.Event().wait()

        values = {"content-type": "application/json", "authorization": "Bearer " + config.API_KEY, **(headers or {})}
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "POST", "scheme": "http", "path": "/v1/chat/completions",
                 "raw_path": b"/v1/chat/completions", "query_string": b"", "root_path": "",
                 "headers": [(k.encode(), v.encode()) for k, v in values.items()],
                 "client": ("127.0.0.1", 1), "server": ("test", 80)}
        return await main.app(scope, receive, send)

    async def test_disconnect_background_generation_and_replay_have_distinct_lifetimes(self):
        release = asyncio.Event()
        started = asyncio.Event()
        response_headers = {}

        async def upstream(*args):
            async def chunks():
                started.set()
                await release.wait()
                tracking.upstream_chunk()
                yield frame("后台回复", finish="stop")
                yield resumable.DONE_FRAME
            return StreamingResponse(chunks(), media_type="text/event-stream")

        async def broken_send(message):
            if message["type"] == "http.response.start":
                response_headers.update(dict(message["headers"]))
            if message["type"] == "http.response.body":
                await started.wait()
                raise OSError("frontend disappeared")

        with mock.patch.object(chat_api.express_upstream, "chat_completions", side_effect=upstream) as call:
            with self.assertRaises(Exception):
                await asyncio.wait_for(self.asgi(broken_send), 2)
            original = self.registry.get(response_headers[b"x-trace-id"].decode())
            self.assertTrue(original.snapshot()["generation_active"])
            self.assertEqual(self.registry.list()["summary"]["background"], 1)
            release.set()
            await asyncio.wait_for(asyncio.gather(*list(self.store._all_pumps)), 2)
            self.assertEqual(original.snapshot()["status"], "succeeded")
            self.assertEqual(original.snapshot()["content_chars"], 4)
            self.assertNotIn("first_sent_content", original.timings)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                replay = await client.post("/v1/chat/completions", json=self.payload, headers=self.headers)
            follower = self.registry.get(replay.headers["x-trace-id"])
            self.assertEqual(follower.data["parent_id"], original.id)
            self.assertIn("first_sent_content", follower.timings)
            self.assertEqual(follower.data["generated_frames"], 0)
            self.assertEqual(call.await_count, 1)
            self.assertEqual(self.registry.list()["summary"]["active"], 0)

    async def test_direct_disconnect_cancels_generation_and_releases_counters(self):
        closed = asyncio.Event()

        async def upstream(*args):
            async def chunks():
                try:
                    yield frame("first")
                    await asyncio.Event().wait()
                finally:
                    closed.set()
            return StreamingResponse(chunks(), media_type="text/event-stream")

        async def broken_send(message):
            if message["type"] == "http.response.body" and b'first' in message.get("body", b""):
                raise OSError("send failed")

        with mock.patch.object(chat_api.express_upstream, "chat_completions", side_effect=upstream):
            with self.assertRaises(Exception):
                await asyncio.wait_for(self.asgi(broken_send, {"x-resumable": "false"}), 2)
        await asyncio.wait_for(closed.wait(), 1)
        record = self.registry.list()["items"][0]
        self.assertFalse(record["generation_active"])
        self.assertEqual(record["status"], "cancelled")
        self.assertNotIn("first_sent_content", record["timings"])

    async def test_monitor_endpoints_require_console_session_and_correlate_logs(self):
        async def upstream(*args):
            print("diagnostic request marker")
            async def chunks():
                yield frame("answer", finish="stop")
                yield resumable.DONE_FRAME
            return StreamingResponse(chunks(), media_type="text/event-stream")

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
            for path in ("/api/requests", "/api/requests/unknown"):
                self.assertEqual((await client.get(path)).status_code, 401)
            with mock.patch.object(chat_api.express_upstream, "chat_completions", side_effect=upstream):
                response = await client.post("/v1/chat/completions", json=self.payload, headers={**self.headers, "Origin": "https://frontend.test"})
            request_id = response.headers["x-trace-id"]
            self.assertIn("x-trace-id", response.headers["access-control-expose-headers"].lower())
            client.cookies.set(console_auth.AUTH_COOKIE, console_auth.issue_session())
            detail = (await client.get("/api/requests/" + request_id)).json()
            self.assertEqual(detail["model"], "gemini-test")
            self.assertEqual(detail["status"], "succeeded")
            self.assertTrue(any("diagnostic request marker" in entry["message"] for entry in detail["logs"]))
            self.assertEqual((await client.get("/api/requests", params={"q": request_id})).json()["total"], 1)
            self.assertEqual((await client.get("/api/requests/unknown")).status_code, 404)

    async def test_concurrent_requests_and_thread_logs_remain_isolated(self):
        release, first_headers = asyncio.Event(), asyncio.Event()

        async def upstream(obj, request):
            name = obj.messages[0].content
            await asyncio.to_thread(print, f"thread marker: {name}")
            async def chunks():
                if name == "hi":
                    await release.wait()
                yield frame(name, finish="stop")
                yield resumable.DONE_FRAME
            return StreamingResponse(chunks(), media_type="text/event-stream")

        async def send(message):
            if message["type"] == "http.response.start":
                first_headers.set()

        with mock.patch.object(chat_api.express_upstream, "chat_completions", side_effect=upstream):
            first = asyncio.create_task(self.asgi(send))
            try:
                await asyncio.wait_for(first_headers.wait(), 2)
                short = dict(self.payload, messages=[{"role": "user", "content": "short"}])
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                    response = await asyncio.wait_for(client.post("/v1/chat/completions", json=short, headers=self.headers), 2)
                self.assertEqual(response.status_code, 200)
                self.assertFalse(first.done())
                self.assertEqual(self.registry.list()["summary"]["generations"], 1)
                self.assertEqual(self.registry.list()["summary"]["connections"], 1)
            finally:
                release.set()
                await asyncio.wait_for(first, 2)
        traces = [self.registry.get(row["id"]) for row in self.registry.list()["items"]]
        for trace in traces:
            markers = [line["message"] for line in trace.logs if "thread marker:" in line["message"]]
            self.assertEqual(len(markers), 1)
            self.assertIn(trace.id, markers[0])
        self.assertEqual(self.registry.list()["summary"]["active"], 0)

    async def test_native_route_retries_and_records_first_text(self):
        calls = 0
        def respond(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(429, json={"error": {"code": 429, "message": "busy"}})
            payload = {"candidates": [{"content": {"parts": [{"text": "native text"}]}, "finishReason": "STOP"}]}
            return httpx.Response(200, content='data: ' + json.dumps(payload) + '\n\n', headers={"content-type": "text/event-stream"})

        manager = SimpleNamespace(get_total_keys=lambda: 1,
                                  get_express_record=lambda **kwargs: {"key": "fake-key", "project_id": "fake-project"})
        settings = dict(config.DEFAULT_SETTINGS, retry_max=1, retry_backoff_seconds=0, express_location="")
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as upstream:
            with mock.patch.object(main.app.state, "express_key_manager", manager), \
                 mock.patch.object(gemini_native, "API_KEY", "native-client"), \
                 mock.patch.object(gemini_native, "passthrough_client_pool", SimpleNamespace(get=lambda *args: upstream)), \
                 mock.patch.object(gemini_native.app_state, "get_settings", return_value=settings), \
                 mock.patch.object(gemini_native.app_state, "get_setting", side_effect=lambda k, d=None: settings.get(k, d)):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                    response = await client.post('/v1beta/models/gemini-test:streamGenerateContent?alt=sse',
                                                 headers={"x-goog-api-key": "native-client"}, json={"contents": []})
        self.assertEqual(response.status_code, 200)
        trace = self.registry.get(response.headers["x-trace-id"])
        self.assertEqual(trace.data["protocol"], "gemini")
        self.assertEqual(trace.data["attempts"], 2)
        self.assertEqual(trace.data["retries"], 1)
        self.assertEqual(trace.data["content_chars"], 11)
        self.assertIn("first_content", trace.timings)
        self.assertIn("first_sent_content", trace.timings)
        self.assertEqual(trace.snapshot()["status"], "succeeded")

    async def test_sdk_retry_has_attempts_first_chunk_and_usage_without_usage_tail(self):
        trace = self.registry.create("/chat")
        token = tracking.current_trace.set(trace)
        self.addCleanup(tracking.current_trace.reset, token)
        calls = 0
        stats = ProxyStats()

        async def generate(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                error = RuntimeError("quota busy")
                error.code = 429
                raise error
            async def chunks():
                yield types.GenerateContentResponse(candidates=[types.Candidate(
                    content=types.Content(parts=[types.Part(text="hello")]), finish_reason="STOP")],
                    usage_metadata=types.GenerateContentResponseUsageMetadata(prompt_token_count=10, candidates_token_count=5))
            return chunks()

        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content_stream=generate)))
        obj = api_helpers.OpenAIRequest(**self.payload)
        settings = {"fake_streaming": False, "retry_max": 1, "retry_backoff_seconds": 0, "retry_jitter_seconds": 0}
        with mock.patch.object(api_helpers, "stats", stats), mock.patch.object(api_helpers.app_state, "get_setting", side_effect=lambda k, d=None: settings.get(k, d)):
            response = await api_helpers.execute_gemini_call(client, obj.model, lambda messages: [], {}, obj)
            async for _ in response.body_iterator:
                pass
        self.assertEqual(trace.data["attempts"], 2)
        self.assertEqual(trace.data["retries"], 1)
        self.assertIn("first_upstream", trace.timings)
        self.assertEqual(trace.data["usage"]["prompt_tokens"], 10)
        self.assertTrue(any(e["kind"] == "retry_wait" for e in trace.events))


if __name__ == "__main__":
    unittest.main()
