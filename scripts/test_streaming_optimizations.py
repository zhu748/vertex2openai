"""Offline regressions for quiet streams, hot settings and tool/error contracts."""
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-stream-opt-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

import anti_truncation as at
import api_helpers
import config
import main
import resumable
import streaming
from logger import ProxyStats
from models import OpenAIRequest
from routes import chat_api
from runtime_state import AppState, app_state
from upstreams import cookie_proxy, express_sdk


MODEL = "gemini-3.6-flash"


def req(**kwargs):
    return OpenAIRequest(model=MODEL, messages=[{"role": "user", "content": "hello"}], **kwargs)


def text_in(body):
    text = []
    for line in body.splitlines():
        if not line.startswith("data:") or line.strip() == "data: [DONE]":
            continue
        payload = json.loads(line[5:])
        for choice in payload.get("choices", []):
            text.append(choice.get("delta", {}).get("content") or "")
    return "".join(text)


class QuietStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_heartbeats_do_not_cancel_or_restart_generation(self):
        release = asyncio.Event()
        closed = asyncio.Event()
        reads = []
        async def source():
            try:
                reads.append(True)
                await release.wait()
                yield "answer"
            finally:
                closed.set()
        events = streaming.with_heartbeats(source(), interval=0.002)
        self.addAsyncCleanup(events.aclose)
        for _ in range(3):
            self.assertIs(await asyncio.wait_for(anext(events), 1), streaming.HEARTBEAT)
        self.assertEqual(len(reads), 1)
        self.assertFalse(closed.is_set())
        release.set()
        self.assertEqual(await anext(events), "answer")
        with self.assertRaises(StopAsyncIteration):
            await anext(events)
        self.assertTrue(closed.is_set())

    async def test_cancelled_reader_closes_pending_upstream(self):
        started, closed = asyncio.Event(), asyncio.Event()
        async def source():
            try:
                started.set()
                await asyncio.Event().wait()
                yield "unreachable"
            finally:
                closed.set()
        events = streaming.with_heartbeats(source())
        read = asyncio.create_task(anext(events))
        await asyncio.wait_for(started.wait(), 1)
        read.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await read
        await events.aclose()
        self.assertTrue(closed.is_set())

    async def test_disconnect_stops_a_quiet_producer(self):
        started, closed = asyncio.Event(), asyncio.Event()
        disconnected = False
        async def probe():
            return disconnected
        async def source():
            try:
                started.set()
                await asyncio.Event().wait()
                yield "unreachable"
            finally:
                closed.set()
        events = streaming.with_heartbeats(source(), SimpleNamespace(is_disconnected=probe), interval=0.01)
        read = asyncio.create_task(anext(events))
        await asyncio.wait_for(started.wait(), 1)
        disconnected = True
        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(read, 1)
        self.assertTrue(closed.is_set())

    async def test_close_after_initial_heartbeat_closes_unused_source(self):
        class Source:
            def __init__(self): self.closed = 0
            def __aiter__(self): return self
            async def __anext__(self): raise AssertionError("Must not start an upstream request")
            async def aclose(self): self.closed += 1
        source = Source()
        frames = streaming.keepalive_sse(source)
        self.assertEqual(await anext(frames), streaming.HEARTBEAT_FRAME)
        await frames.aclose()
        self.assertEqual(source.closed, 1)

    async def test_buffer_reader_heartbeats_do_not_allocate_event_ids(self):
        buf = resumable.ResumableBuffer("quiet-buffer")
        with mock.patch.object(streaming, "HEARTBEAT_INTERVAL", 0.002):
            frames = resumable.sse_from_buffer(buf)
            self.assertEqual(await anext(frames), streaming.HEARTBEAT_FRAME)
            self.assertEqual(await asyncio.wait_for(anext(frames), 1), streaming.HEARTBEAT_FRAME)
            self.assertEqual(buf.next_seq, 0)
            self.assertEqual(buf.readers, 1)
            await buf.append(resumable.frame_with_id(0, 'data: {"choices": []}\n\n'))
            await buf.append(resumable.frame_with_id(1, resumable.DONE_FRAME))
            await buf.finish("done")
            output = "".join([frame async for frame in frames])
        self.assertIn("id: 0\n", output)
        self.assertIn("id: 1\n", output)
        self.assertEqual(buf.next_seq, 2)
        self.assertTrue(buf.fully_delivered)
        self.assertEqual(buf.readers, 0)

    async def test_closing_quiet_buffer_reader_releases_reader_count(self):
        buf = resumable.ResumableBuffer("abandoned-reader")
        with mock.patch.object(streaming, "HEARTBEAT_INTERVAL", 0.002):
            frames = resumable.sse_from_buffer(buf)
            await anext(frames)
            await anext(frames)
            await frames.aclose()
        self.assertEqual(buf.readers, 0)
        self.assertFalse(buf.done)

    async def test_unhandled_stream_error_is_reported_and_counted_once(self):
        stats = ProxyStats()
        closed = asyncio.Event()
        async def source():
            try:
                yield 'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'
                raise httpx.ReadTimeout("quiet upstream failed")
            finally:
                closed.set()
        response = StreamingResponse(source(), media_type="text/event-stream")
        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))
        with mock.patch.object(chat_api.app_state, "is_web_proxy_enabled", return_value=True), \
             mock.patch.object(chat_api.cookie_upstream, "chat_completions", return_value=response), \
             mock.patch.object(chat_api, "stats", stats):
            result = await chat_api._call_upstream(req(stream=True), request)
            body = "".join([frame async for frame in result.body_iterator])
        self.assertTrue(closed.is_set())
        self.assertEqual(text_in(body), "partial")
        self.assertIn('"code": 504', body)
        self.assertNotIn('"finish_reason": "stop"', body)
        self.assertEqual(stats.error_requests, 1)
        self.assertEqual(stats.success_requests, 0)


class SettingsTests(unittest.TestCase):
    def state(self):
        state = AppState()
        state._state = {}
        patch = mock.patch.object(state, "_save")
        patch.start()
        self.addCleanup(patch.stop)
        return state

    def test_console_credentials_override_env_and_explicit_empty_is_respected(self):
        state = self.state()
        with mock.patch.object(config, "GOOGLE_COOKIE", "SAPISID=env-test"), \
             mock.patch.object(config, "GOOGLE_PROJECT_ID", "env-project"), \
             mock.patch.object(cookie_proxy, "app_state", state), \
             mock.patch.object(express_sdk, "app_state", state):
            self.assertEqual(cookie_proxy._get_cookie_string(), "SAPISID=env-test")
            self.assertEqual(cookie_proxy._get_project_id(), "env-project")
            state.set_google_cookie("SAPISID=console-test")
            state.set_project_id("console-project")
            self.assertEqual(cookie_proxy._get_cookie_string(), "SAPISID=console-test")
            self.assertEqual(cookie_proxy._get_project_id(), "console-project")
            path = express_sdk.resolve_express_model_path(MODEL, {"express_location": "global"})
            self.assertIn("projects/console-project/", path)
            owned = express_sdk.resolve_express_model_path(MODEL, {"express_location": "global"}, "key-project")
            self.assertIn("projects/key-project/", owned)
            state.set_google_cookie("")
            state.set_project_id("")
            self.assertEqual(cookie_proxy._get_cookie_string(), "")
            self.assertEqual(cookie_proxy._get_project_id(), "")

    def test_partial_model_patch_preserves_explicit_antitruncation_preference(self):
        state = self.state()
        state.set_model_override("gemini-4-argon", {"anti_truncation_mode": "off"})
        saved = state.set_model_override("gemini-4-argon", {"thinking_g3_level": "low"})
        self.assertEqual(saved["anti_truncation_mode"], "off")
        self.assertEqual(state.get_effective_settings("gemini-4-argon")["anti_truncation_mode"], "off")
        state.clear_model_override("gemini-4-argon")
        self.assertEqual(state.get_effective_settings("gemini-4-argon")["anti_truncation_mode"], "both")

    def test_keep_turn_can_be_saved_from_dashboard(self):
        main.app.dependency_overrides[main.require_auth] = lambda: True
        self.addCleanup(main.app.dependency_overrides.clear)
        client = TestClient(main.app)
        self.addCleanup(client.close)
        with mock.patch.object(main.app_state, "update_settings", return_value={}) as save:
            result = client.post("/api/settings", json={"prefill_mode": "keep_turn"})
            self.assertEqual(result.status_code, 200)
            save.assert_called_once_with({"prefill_mode": "keep_turn"})

    def test_image_system_instruction_uses_model_override_in_both_channels(self):
        obj = OpenAIRequest(model="gemini-nano-banana-2.1", messages=[
            {"role": "system", "content": "draw in ink"}, {"role": "user", "content": "draw a cat"}])
        for enabled in (True, False):
            settings = dict(config.DEFAULT_SETTINGS, image_system_instruction=enabled)
            with mock.patch.object(app_state, "get_effective_settings", return_value=settings), \
                 mock.patch.object(app_state, "get_setting", side_effect=lambda k, d=None: not enabled if k == "image_system_instruction" else d):
                sdk = api_helpers.create_generation_config(obj)
                cookie = cookie_proxy._build_batch_graphql_body("project", obj.model, obj)
            self.assertEqual("system_instruction" in sdk, enabled)
            self.assertEqual("systemInstruction" in cookie["variables"], enabled)

    def test_prepared_decoder_rejects_undeclared_tools(self):
        obj = req().model_copy(update={"model": MODEL + "-antitruncation-stream"})
        _, transport = at.prepare_transport(obj, MODEL, {"anti_truncation_mode": "stream"})
        with self.assertRaises(at.AntiTruncationError):
            transport.decoder().call("hallucinated_tool", {})
        tool = {"type": "function", "function": {"name": "declared", "parameters": {"type": "object"}}}
        _, transport = at.prepare_transport(obj.model_copy(update={"tools": [tool]}), MODEL,
                                            {"anti_truncation_mode": "stream"})
        self.assertIsNone(transport.decoder().call("declared", {}))

    def test_random_tool_name_collision_has_a_bound(self):
        name = "v2o_emit_" + "a" * 24
        obj = req(tools=[{"type": "function", "function": {"name": name}}]).model_copy(
            update={"model": MODEL + "-antitruncation-stream"})
        with mock.patch.object(at.secrets, "token_hex", return_value="a" * 24) as random:
            with self.assertRaises(at.AntiTruncationError):
                at.prepare_transport(obj, MODEL, {"anti_truncation_mode": "stream"})
        self.assertEqual(random.call_count, 8)


class CookieStreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = dict(config.DEFAULT_SETTINGS, anti_truncation_mode="nonstream", retry_max=0,
            retry_backoff_seconds=0, prefill_mode="off", fake_streaming=False, proxy_url="")
        self.request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))
        self.stats = ProxyStats()
        for patch in (
            mock.patch.object(app_state, "get_setting", side_effect=lambda k, d=None: self.settings.get(k, d)),
            mock.patch.object(app_state, "get_effective_settings", side_effect=lambda m, base=None: (self.settings.copy() if base is None else dict(base))),
            mock.patch.object(cookie_proxy, "_get_cookie_string", return_value="SAPISID=test"),
            mock.patch.object(cookie_proxy, "_get_project_id", return_value="project"),
            mock.patch.object(cookie_proxy, "build_headers", return_value={"authorization": "test"}),
            mock.patch.object(cookie_proxy, "stats", self.stats),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def pool(self, client):
        @asynccontextmanager
        async def open_client(kwargs):
            yield client
        patch = mock.patch.object(cookie_proxy, "open_upstream_client", open_client)
        patch.start()
        self.addCleanup(patch.stop)

    def payload(self, body):
        tools = body["variables"].get("tools")
        part = {"text": "answer"}
        if tools:
            name = tools[0]["functionDeclarations"][-1]["name"]
            part = {"functionCall": {"name": name, "args": {"content": "answer"}}}
        return {"results": [{"data": {"candidates": [{"content": {"parts": [part]}, "finishReason": "STOP"}]}}]}

    async def test_buffered_modes_return_sse_and_heartbeats_before_generation_finishes(self):
        for mode, fake in (("nonstream", False), ("off", True)):
            self.settings.update(anti_truncation_mode=mode, fake_streaming=fake)
            release, started = asyncio.Event(), asyncio.Event()
            async def post(url, headers, json):
                started.set()
                await release.wait()
                return httpx.Response(200, json=self.payload(json))
            upstream = mock.AsyncMock(side_effect=post)
            self.pool(SimpleNamespace(post=upstream))
            obj = req(stream=True)
            if mode != "off":
                obj = obj.model_copy(update={"model": MODEL + "-antitruncation-" + mode})
            before = obj.model_dump_json()
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
            self.assertFalse(started.is_set())
            with mock.patch.object(streaming, "HEARTBEAT_INTERVAL", 0.002):
                frames = result.body_iterator
                body = await anext(frames)
                body += await asyncio.wait_for(anext(frames), 1)
                self.assertTrue(started.is_set())
                self.assertEqual(text_in(body), "")
                release.set()
                body += "".join([frame async for frame in frames])
            self.assertEqual(text_in(body), "answer")
            self.assertEqual(upstream.await_count, 1)
            self.assertEqual(obj.model_dump_json(), before)

    async def test_buffered_stream_cancellation_cancels_generation(self):
        closed = asyncio.Event()
        async def post(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()
        self.pool(SimpleNamespace(post=post))
        obj = req(stream=True).model_copy(update={"model": MODEL + "-antitruncation-nonstream"})
        result = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
        with mock.patch.object(streaming, "HEARTBEAT_INTERVAL", 0.002):
            frames = result.body_iterator
            await anext(frames)
            await anext(frames)
            await frames.aclose()
        self.assertTrue(closed.is_set())
        self.assertEqual(self.stats.success_requests, 0)
        self.assertEqual(self.stats.error_requests, 0)

    async def test_buffered_failure_is_one_sse_error(self):
        self.pool(SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(429, text="busy"))))
        obj = req(stream=True).model_copy(update={"model": MODEL + "-antitruncation-nonstream"})
        result = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
        body = "".join([frame async for frame in result.body_iterator])
        self.assertIn('"code": 429', body)
        self.assertNotIn('"finish_reason": "stop"', body)
        self.assertEqual(self.stats.error_requests, 1)
        self.assertEqual(self.stats.success_requests, 0)

    async def test_graphql_numeric_429_retries_but_400_with_quota_word_does_not(self):
        self.settings.update(anti_truncation_mode="off", retry_max=1)
        calls = []
        def handler(request):
            calls.append(True)
            if len(calls) == 1:
                return httpx.Response(200, json={"results": [{"errors": [{"code": 429, "message": "busy"}]}]})
            return httpx.Response(200, json=self.payload(json.loads(request.content)))
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            self.pool(client)
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(req(stream=True), self.request)
            body = "".join([frame async for frame in result.body_iterator])
        self.assertEqual(len(calls), 2)
        self.assertEqual(text_in(body), "answer")
        self.assertEqual(self.stats.retry_counts, 1)
        calls.clear()
        def permanent(request):
            calls.append(True)
            return httpx.Response(200, json={"results": [{"errors": [{"code": 400, "message": "quota option invalid 429"}]}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(permanent)) as client:
            self.pool(client)
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(req(stream=True), self.request)
            body = "".join([frame async for frame in result.body_iterator])
        self.assertEqual(len(calls), 1)
        self.assertIn('"code": 400', body)
        self.assertNotIn('"finish_reason": "stop"', body)

    async def test_http_503_error_keeps_its_status_in_sse(self):
        self.settings["anti_truncation_mode"] = "off"
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503, text="busy"))) as client:
            self.pool(client)
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(req(stream=True), self.request)
            body = "".join([frame async for frame in result.body_iterator])
        self.assertIn('"code": 503', body)
        self.assertEqual(self.stats.error_requests, 1)
        self.assertNotIn('"finish_reason": "stop"', body)

    async def test_cookie_missing_finish_marker_is_not_reported_as_success(self):
        self.settings["anti_truncation_mode"] = "off"
        for stream, fake in ((True, False), (False, False), (True, True)):
            with self.subTest(stream=stream, fake=fake):
                self.settings["fake_streaming"] = fake
                calls = []
                def handler(request):
                    calls.append(True)
                    payload = self.payload(json.loads(request.content))
                    payload["results"][0]["data"]["candidates"][0].pop("finishReason")
                    return httpx.Response(200, json=payload)
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    self.pool(client)
                    result = await cookie_proxy.CookieProxyUpstream().chat_completions(req(stream=stream), self.request)
                    if stream:
                        body = "".join([frame async for frame in result.body_iterator])
                        self.assertIn('"code": 502', body)
                        self.assertNotIn('"finish_reason": "stop"', body)
                    else:
                        self.assertEqual(result.status_code, 502)
                self.assertEqual(len(calls), 1)
                self.assertEqual(self.stats.success_requests, 0)

    async def test_cookie_image_missing_finish_marker_returns_error(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
                200, json={"results": [{"data": {"candidates": [{"content": {"parts": [{"text": "partial"}]}}]}}]}))) as client:
            self.pool(client)
            result = await cookie_proxy._collect_full_response(
                "project", "gemini-3.1-flash-image", req(), {}, {}, 0, 0, self.request)
        self.assertEqual(result["kind"], "error")
        self.assertEqual(result["code"], 502)

    async def test_cookie_error_consumer_closes_its_event_iterator(self):
        self.settings["anti_truncation_mode"] = "off"
        closed = asyncio.Event()
        async def events(*args, **kwargs):
            try:
                yield "fatal_error", cookie_proxy._StreamRequestError("failure", status_code=502)
                await asyncio.Event().wait()
            finally:
                closed.set()
        self.pool(SimpleNamespace())
        with mock.patch.object(cookie_proxy, "_execute_stream_request_generator", side_effect=events):
            response = await cookie_proxy.CookieProxyUpstream().chat_completions(req(stream=True), self.request)
            body = "".join([frame async for frame in response.body_iterator])
        self.assertIn('"code": 502', body)
        self.assertTrue(closed.is_set())

    async def test_native_tool_fallback_closes_rejected_stream_before_replacement(self):
        closed = asyncio.Event()
        async def events(*args, **kwargs):
            try:
                if not closed.is_set():
                    yield "native_tool_unsupported", "unsupported function declarations"
                else:
                    yield "text", "fallback"
            finally:
                closed.set()
        def fallback():
            self.assertTrue(closed.is_set())
            return {"fallback": True}
        with mock.patch.object(cookie_proxy, "_execute_stream_request_generator_once", side_effect=events):
            result = [event async for event in cookie_proxy._execute_stream_request_generator(
                None, {}, {}, fallback_body=fallback)]
        self.assertEqual(result, [("text", "fallback")])

    async def test_image_failure_is_an_error_event_with_upstream_status(self):
        self.pool(SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(400, text="invalid image option"))))
        obj = req(stream=True).model_copy(update={"model": "gemini-3.1-flash-image"})
        result = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
        body = "".join([frame async for frame in result.body_iterator])
        self.assertIn('"code": 400', body)
        self.assertNotIn('"finish_reason": "stop"', body)
        self.assertEqual(self.stats.error_requests, 1)
        self.assertEqual(self.stats.success_requests, 0)

    async def test_invalid_function_names_return_400_in_both_channels(self):
        for name in (None, "", [], "bad\nname"):
            obj = req(tools=[{"type": "function", "function": {"name": name}}])
            for upstream in (cookie_proxy.CookieProxyUpstream(), express_sdk.ExpressSDKUpstream()):
                result = await upstream.chat_completions(obj, None)
                self.assertEqual(result.status_code, 400)


if __name__ == "__main__":
    unittest.main(verbosity=2)
