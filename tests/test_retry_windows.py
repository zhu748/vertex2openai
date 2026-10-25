"""First model reply deadlines and reconnect windows, without upstream traffic."""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-retry-windows-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from google.genai import types

import api_helpers
import config
import first_reply
import main
import middleware
import request_tracking
import retry_policy
import resumable
from logger import ProxyStats
from request_connection import ClientConnection
from routes import gemini_native
from settings_validation import validate_settings_patch
from upstreams import cookie_transport


def text_chunk(text="answer", thought=False, finish="STOP"):
    return types.GenerateContentResponse(candidates=[types.Candidate(
        content=types.Content(parts=[types.Part(text=text, thought=thought)]), finish_reason=finish)])


class FirstReplyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = dict(config.DEFAULT_SETTINGS, retry_max=1, retry_backoff_seconds=0,
                             retry_jitter_seconds=0, fake_streaming=False, fake_streaming_interval=0)
        self.stats = ProxyStats()
        for patch in (
            mock.patch.object(first_reply, "timeout_seconds", return_value=.02),
            mock.patch.object(api_helpers, "stats", self.stats),
            mock.patch.object(api_helpers.app_state, "get_setting", side_effect=lambda k, d=None: self.settings.get(k, d)),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    async def stream(self, generate):
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content_stream=generate)))
        obj = api_helpers.OpenAIRequest(model="gemini-test", messages=[], stream=True)
        response = await api_helpers.execute_gemini_call(client, obj.model, lambda messages: [], {}, obj)
        return ''.join([chunk async for chunk in response.body_iterator])

    async def native_request(self, respond):
        manager = SimpleNamespace(get_total_keys=lambda: 1, get_express_record=lambda **kwargs: {"key": "fake", "project_id": "p"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as upstream:
            with mock.patch.object(main.app.state, "express_key_manager", manager), \
                 mock.patch.object(gemini_native, "API_KEY", "test-client"), \
                 mock.patch.object(gemini_native, "passthrough_client_pool", SimpleNamespace(get=lambda *args: upstream)), \
                 mock.patch.object(gemini_native.app_state, "get_settings", return_value=self.settings):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                    return await asyncio.wait_for(client.post('/v1beta/models/gemini-test:streamGenerateContent?alt=sse',
                        headers={"x-goog-api-key": "test-client"}, json={"contents": []}), 1)

    async def test_inner_network_timeout_is_not_misreported_as_first_reply_deadline(self):
        trace = request_tracking.RequestTrace('/test')
        token = request_tracking.current_trace.set(trace)
        error = TimeoutError('inner pool timeout')
        async def call():
            raise error
        try:
            with self.assertRaises(TimeoutError) as caught:
                await first_reply.FirstReplyGuard('gemini-test').wait(call())
        finally:
            request_tracking.current_trace.reset(token)
        self.assertIs(caught.exception, error)
        self.assertTrue(retry_policy.is_retryable_exception(error))
        self.assertFalse(any(event['kind'] == 'first_reply_timeout' for event in trace.events))

    async def test_disabled_deadline_still_discards_metadata_and_retries_before_output(self):
        calls = 0
        async def generate(**kwargs):
            nonlocal calls
            calls += 1
            async def chunks():
                if calls == 1:
                    yield types.GenerateContentResponse(candidates=[types.Candidate(
                        content=types.Content(parts=[types.Part(thought_signature=b"discard-me")]))])
                    raise httpx.ReadTimeout('network timeout')
                yield text_chunk()
            return chunks()
        with mock.patch.object(first_reply, "timeout_seconds", return_value=0):
            body = await self.stream(generate)
        self.assertEqual(calls, 2)
        self.assertNotIn('ZGlzY2FyZC1tZQ==', body)
        self.assertIn('answer', body)

    async def test_single_key_first_reply_timeout_closes_before_retry(self):
        calls, closed = 0, asyncio.Event()
        async def generate(**kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                self.assertTrue(closed.is_set())
            async def chunks():
                if calls == 1:
                    try:
                        await asyncio.Event().wait()
                    finally:
                        closed.set()
                yield text_chunk()
            return chunks()
        body = await self.stream(generate)
        self.assertEqual(calls, 2)
        self.assertEqual(self.stats.retry_counts, 1)
        self.assertIn('answer', body)
        self.assertNotIn('"error"', body)

    async def test_thought_is_first_reply_and_does_not_time_out_waiting_for_body(self):
        generate = mock.AsyncMock()
        async def chunks():
            yield text_chunk("thinking", thought=True, finish=None)
            await asyncio.sleep(.05)
            yield text_chunk()
        generate.return_value = chunks()
        body = await self.stream(generate)
        self.assertIn('"reasoning_content": "thinking"', body)
        self.assertIn('answer', body)
        self.assertEqual(generate.await_count, 1)

    async def test_usage_and_signature_only_do_not_satisfy_first_reply(self):
        calls = 0
        async def generate(**kwargs):
            nonlocal calls
            calls += 1
            async def chunks():
                if calls == 1:
                    yield types.GenerateContentResponse(candidates=[types.Candidate(
                        content=types.Content(parts=[types.Part(thought_signature=b"discard-me")]))])
                    while True:
                        yield types.GenerateContentResponse(usage_metadata=types.GenerateContentResponseUsageMetadata(prompt_token_count=2))
                        await asyncio.sleep(.003)
                yield text_chunk()
            return chunks()
        body = await self.stream(generate)
        self.assertEqual(calls, 2)
        self.assertNotIn('ZGlzY2FyZC1tZQ==', body)
        self.assertIn('answer', body)

    async def test_deadline_disabled_keeps_slow_first_reply(self):
        async def generate(**kwargs):
            async def chunks():
                await asyncio.sleep(.04)
                yield text_chunk()
            return chunks()
        with mock.patch.object(first_reply, "timeout_seconds", return_value=0):
            body = await self.stream(generate)
        self.assertIn('answer', body)
        self.assertEqual(self.stats.retry_counts, 0)

    async def test_retry_budget_exhaustion_returns_504_and_done(self):
        calls = 0
        async def generate(**kwargs):
            nonlocal calls
            calls += 1
            async def chunks():
                await asyncio.Event().wait()
                yield text_chunk()
            return chunks()
        body = await self.stream(generate)
        self.assertEqual(calls, 2)
        self.assertIn('"code": 504', body)
        self.assertIn('data: [DONE]', body)
        self.assertEqual(self.stats.retry_counts, 1)

    async def test_tool_call_is_first_reply(self):
        async def generate(**kwargs):
            async def chunks():
                yield types.GenerateContentResponse(candidates=[types.Candidate(content=types.Content(parts=[
                    types.Part(function_call=types.FunctionCall(name="weather", args={"city": "NYC"}))]))])
                await asyncio.sleep(.04)
                yield types.GenerateContentResponse(candidates=[types.Candidate(finish_reason="STOP")])
            return chunks()
        body = await self.stream(generate)
        self.assertIn('"name": "weather"', body)
        self.assertNotIn('"error"', body)
        self.assertEqual(self.stats.retry_counts, 0)

    async def test_nonstream_and_fake_stream_retry_inside_existing_budget(self):
        for stream in (False, True):
            calls = 0
            async def generate(**kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    await asyncio.Event().wait()
                return text_chunk()
            client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate)))
            obj = api_helpers.OpenAIRequest(model="gemini-test", messages=[], stream=stream)
            self.settings["fake_streaming"] = True
            response = await api_helpers.execute_gemini_call(client, obj.model, lambda messages: [], {}, obj)
            body = ''.join([chunk async for chunk in response.body_iterator]) if stream else response.body.decode()
            self.assertIn('answer', body)
            self.assertEqual(calls, 2)

    async def test_cookie_timeout_becomes_retryable_event_and_closes_source(self):
        closed = asyncio.Event()
        async def source(*args, **kwargs):
            try:
                yield "usage", {"promptTokenCount": 1}
                await asyncio.Event().wait()
            finally:
                closed.set()
        with mock.patch.object(cookie_transport, "_execute_stream_request_generator_raw", side_effect=source):
            events = [item async for item in cookie_transport.stream_events(None, {}, {}, model="gemini-test")]
        self.assertTrue(closed.is_set())
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0][0], 'retryable_error')
        self.assertEqual(events[0][1].status_code, 504)

    async def test_native_stream_timeout_retries_and_thought_accepts_deadline(self):
        calls, closed = 0, asyncio.Event()
        class NativeBytes(httpx.AsyncByteStream):
            def __init__(self, first):
                self.first = first
            async def __aiter__(self):
                if self.first:
                    await asyncio.Event().wait()
                yield b'data: {"candidates":[{"content":{"parts":[{"text":"thinking","thought":true}]}}]}\n\n'
                await asyncio.sleep(.04)
                yield b'data: {"candidates":[{"content":{"parts":[{"text":"answer"}]},"finishReason":"STOP"}]}\n\n'
            async def aclose(self):
                if self.first:
                    closed.set()
        def respond(request):
            nonlocal calls
            calls += 1
            if calls > 1:
                self.assertTrue(closed.is_set())
            return httpx.Response(200, stream=NativeBytes(calls == 1), headers={"content-type": "text/event-stream"})
        response = await self.native_request(respond)
        self.assertEqual(calls, 2)
        self.assertIn('thinking', response.text)
        self.assertIn('answer', response.text)

    async def test_native_error_before_first_reply_keeps_status_payload_and_retry_after(self):
        for timeout in (0, .02):
            for code in (400, 429):
                with self.subTest(timeout=timeout, code=code):
                    calls, closed = 0, set()
                    payload = {"error": {"code": code, "message": "original upstream error", "details": [{"reason": "test"}]}}
                    class NativeErrorBytes(httpx.AsyncByteStream):
                        def __init__(self, call):
                            self.call = call
                        async def __aiter__(self):
                            yield b'data: ' + json.dumps(payload).encode() + b'\n\n'
                        async def aclose(self):
                            closed.add(self.call)
                    def respond(request):
                        nonlocal calls
                        if calls:
                            self.assertIn(calls, closed)
                        calls += 1
                        return httpx.Response(200, stream=NativeErrorBytes(calls),
                            headers={"content-type": "text/event-stream", "retry-after": "0"})
                    with mock.patch.object(first_reply, 'timeout_seconds', return_value=timeout), \
                         mock.patch.object(gemini_native, '_retry_delay', wraps=gemini_native._retry_delay) as delay:
                        response = await self.native_request(respond)
                    self.assertEqual(response.status_code, code)
                    self.assertEqual(response.json(), payload)
                    self.assertEqual(response.headers['retry-after'], '0')
                    self.assertEqual(calls, 2 if code == 429 else 1)
                    if code == 429:
                        self.assertEqual(delay.call_args.args[0], '0')

    async def test_native_json_array_metadata_is_not_reply_and_split_utf8_is_preserved(self):
        class NativeBytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'[{"usageMetadata":{"promptTokenCount":1}},'
                yield b'{"candidates":[{"content":{"parts":[{"text":"\xe6'
                yield b'\x80\x9d\xe8\x80\x83","thought":true}]},"finishReason":"STOP"}]}'
                await asyncio.sleep(.04)
                yield b']'
        response = httpx.Response(200, stream=NativeBytes())
        guard = first_reply.FirstReplyGuard('gemini-test')
        source = gemini_native._native_json_chunks(response, guard)
        stream = await guard.prefetch(source, lambda _chunk: guard.ready)
        self.assertTrue(guard.ready)
        body = b''.join([part async for part in stream])
        self.assertIn('思考', body.decode())
        self.assertTrue(body.endswith(b']'))
        await stream.aclose()

        class OnlyMetadata(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'[{"usageMetadata":{"promptTokenCount":1}},'
                await asyncio.Event().wait()
        response = httpx.Response(200, stream=OnlyMetadata())
        guard = first_reply.FirstReplyGuard('gemini-test')
        with self.assertRaises(first_reply.FirstReplyTimeout):
            await guard.prefetch(gemini_native._native_json_chunks(response, guard), lambda _chunk: guard.ready)


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        patch = mock.patch.object(resumable, "disconnect_grace_seconds", return_value=10)
        patch.start()
        self.addCleanup(patch.stop)

    async def buffer(self):
        return (await self.store.get_or_create_request("same", model="m", kind="stream", fingerprint_key="fingerprint"))[0]

    async def test_within_window_reuses_and_last_reader_starts_clock(self):
        buf = await self.buffer()
        first, second = ClientConnection(), ClientConnection()
        buf.attach_client(first)
        buf.attach_client(second)
        first.finish(True)
        self.assertIsNone(buf.last_disconnect_monotonic)
        self.assertIs(await self.buffer(), buf)
        second.finish(True)
        self.assertIsNotNone(buf.last_disconnect_monotonic)
        self.assertIs(await self.buffer(), buf)
        third = ClientConnection()
        buf.attach_client(third)
        self.assertIsNone(buf.last_disconnect_monotonic)

    async def test_expired_request_cancels_old_and_admits_one_new_generation(self):
        buf = await self.buffer()
        connection = ClientConnection()
        buf.attach_client(connection)
        started, closed = asyncio.Event(), asyncio.Event()
        async def worker():
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                closed.set()
        task = asyncio.create_task(worker())
        self.store.register_pump(buf.id, task, buffer=buf)
        await started.wait()
        connection.finish(True)
        buf.last_disconnect_monotonic -= 11
        first, second = await asyncio.gather(self.buffer(), self.buffer())
        self.assertTrue(closed.is_set())
        self.assertIs(first, second)
        self.assertNotEqual(first.id, buf.id)
        self.assertIsNone(await self.store.get_exact(buf.id))
        self.assertEqual(buf.status, 'error')

    async def test_zero_grace_replaces_immediately_and_delivered_idempotency_is_retained(self):
        buf = await self.buffer()
        connection = ClientConnection()
        buf.attach_client(connection)
        with mock.patch.object(resumable, "disconnect_grace_seconds", return_value=0):
            connection.finish(True)
            fresh = await self.buffer()
        self.assertNotEqual(buf.id, fresh.id)
        fresh.mark_delivered()
        same, new = await self.store.get_or_create_request("same", model="m", kind="stream", fingerprint_key="fingerprint", idempotent=True)
        self.assertIs(same, fresh)
        self.assertFalse(new)

    async def test_late_client_registration_uses_observed_disconnect_time_and_fixed_window(self):
        buf = await self.buffer()
        connection = ClientConnection()
        connection.finish(True)
        connection.disconnected_monotonic -= 11
        buf.attach_client(connection)
        with mock.patch.object(resumable, "disconnect_grace_seconds", return_value=100):
            self.assertTrue(buf.resume_expired())
            self.assertNotEqual((await self.buffer()).id, buf.id)

    async def test_replacement_is_attributed_to_old_trace(self):
        owner, incoming = request_tracking.RequestTrace('/old'), request_tracking.RequestTrace('/new')
        token = request_tracking.current_trace.set(owner)
        try:
            buf = await self.buffer()
        finally:
            request_tracking.current_trace.reset(token)
        connection = ClientConnection()
        buf.attach_client(connection)
        connection.finish(True)
        buf.last_disconnect_monotonic -= 11
        token = request_tracking.current_trace.set(incoming)
        try:
            await self.buffer()
        finally:
            request_tracking.current_trace.reset(token)
        self.assertEqual(owner.data['outcome'], 'cancelled')
        self.assertIsNone(incoming.data['outcome'])
        self.assertEqual(len(incoming.logs), 0)

    async def test_explicit_expired_cursor_returns_410_without_starting_new_generation(self):
        connection = ClientConnection()
        request = SimpleNamespace(headers={}, query_params={}, scope={"state": {"client_connection": connection}})
        buf, _, _, _ = await resumable.resolve_buffer(self.store, request, 'key', 'body', model='m', kind='stream')
        connection.finish(True)
        buf.last_disconnect_monotonic -= 11
        request.headers = {"x-resume-id": buf.id, "last-event-id": "0"}
        with self.assertRaises(resumable.ResumeRequestError) as error:
            await resumable.resolve_buffer(self.store, request, 'key', 'body', model='m', kind='stream')
        self.assertEqual(error.exception.code, 410)
        self.assertEqual(await self.store.list_meta(), [])

    async def test_replacement_cancellation_cleans_new_placeholder(self):
        buf = await self.buffer()
        connection = ClientConnection()
        buf.attach_client(connection)
        cleanup_started, cleanup_release, started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def worker():
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await cleanup_release.wait()
        task = asyncio.create_task(worker())
        self.store.register_pump(buf.id, task, buffer=buf)
        await started.wait()
        connection.finish(True)
        buf.last_disconnect_monotonic -= 11
        replace = asyncio.create_task(self.buffer())
        await cleanup_started.wait()
        replace.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await replace
        try:
            self.assertEqual(await self.store.list_meta(), [])
        finally:
            cleanup_release.set()
            await asyncio.gather(task, return_exceptions=True)


class ConnectionAndSettingsTests(unittest.IsolatedAsyncioTestCase):
    async def test_receive_transport_failure_is_reported_as_disconnect(self):
        connection = ClientConnection()
        callback = mock.Mock()
        async def receive():
            raise OSError('socket closed')
        receiver = middleware.ConnectionReceiver(receive, connection, callback, lambda: False)
        await receiver.watch()
        self.assertTrue(connection.disconnected)
        callback.assert_called_once()

    def test_native_image_counts_as_generated_and_sent_first_reply(self):
        trace = request_tracking.RequestTrace('/native')
        payload = {'candidates': [{'content': {'parts': [{'inlineData': {'mimeType': 'image/png', 'data': 'test'}}]}}]}
        trace.inspect_payload(payload)
        trace.inspect_payload(payload, sent=True)
        self.assertIn('first_reply', trace.timings)
        self.assertIn('first_sent_reply', trace.timings)
        self.assertNotIn('first_content', trace.timings)

    async def test_disconnect_is_visible_while_application_is_still_waiting(self):
        registry, stats = request_tracking.RequestRegistry(), ProxyStats()
        incoming = asyncio.Queue()
        await incoming.put({"type": "http.request", "body": b"one", "more_body": True})
        await incoming.put({"type": "http.request", "body": b"two", "more_body": False})
        started, release = asyncio.Event(), asyncio.Event()
        async def app(scope, receive, send):
            body = (await receive())["body"] + (await receive())["body"]
            self.assertEqual(body, b"onetwo")
            trace = request_tracking.current_trace.get()
            trace.start_generation()
            started.set()
            await release.wait()
            trace.end_generation()
        async def send(message):
            pass
        with mock.patch.object(middleware, "request_registry", registry):
            task = asyncio.create_task(middleware.StatsMiddleware(app, stats)(
                {"type": "http", "method": "POST", "path": "/v1/chat/completions"}, incoming.get, send))
            try:
                await started.wait()
                await incoming.put({"type": "http.disconnect"})
                for _ in range(50):
                    if registry.list()["summary"]["connections"] == 0:
                        break
                    await asyncio.sleep(.001)
                self.assertFalse(task.done())
                self.assertEqual(registry.list()["summary"]["background"], 1)
                self.assertIsNotNone(registry.list()["items"][0]["disconnected_at"])
            finally:
                release.set()
                await task

    def test_settings_validate_and_model_override_zero_is_supported(self):
        for key in ('first_reply_timeout_seconds', 'resumable_disconnect_grace_seconds'):
            for value in (0, .5, 10, 7200):
                self.assertIsNone(validate_settings_patch({key: value}))
            for value in (-1, float('nan'), True, 7201):
                self.assertIsNotNone(validate_settings_patch({key: value}))
        with mock.patch.object(first_reply.app_state, 'get_effective_settings', return_value={'first_reply_timeout_seconds': 0}):
            self.assertEqual(first_reply.timeout_seconds('model-override'), 0)


if __name__ == '__main__':
    unittest.main()
