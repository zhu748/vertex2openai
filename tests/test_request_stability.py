"""Bounded fault regressions for upload/parse admission and transport ownership."""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-stability-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from pydantic import ValidationError
from starlette.responses import JSONResponse, StreamingResponse

import auth
import client_pool
import config
import impersonate
import main
import media_tasks
import message_processing as mp
import proxy_resolver as pr
import request_ingress as ingress
import resumable
from curl_transport import CancellationSafeSession
from models import MAX_CANDIDATES, OpenAIRequest
from proxy_failover import ProxyChain
from request_connection import BodyTooLargeError
from request_tracking import RequestTrace
from response_validation import IncompleteResponseError
from routes import chat_api, claude_api, gemini_native


class JSONBoundaryTests(unittest.TestCase):
    def test_valid_unicode_and_escaped_literals_at_every_split(self):
        for value in ("中文🌍", "\\ud800", 'a\\\"b', {"[]{}:,'": [False, None, 1, "😺"]}):
            body = json.dumps(value, ensure_ascii=True).encode()
            for split in range(len(body) + 1):
                guard = ingress.JSONPreflight()
                guard.feed(body[:split])
                guard.feed(body[split:], final=True)
            guard = ingress.JSONPreflight()
            for index, byte in enumerate(body):
                guard.feed(bytes([byte]), final=index == len(body) - 1)

    def test_unpaired_surrogates_rejected_at_every_split(self):
        for body in (b'"\\ud800"', b'"\\udfff"', b'"\\ud800x"', b'"\\ud800\\n"',
                     b'"\\ud800\\u1234"', b'"\\ud800\\ud800"', b'"\\u12'):
            for split in range(len(body) + 1):
                with self.subTest(body=body, split=split), self.assertRaises(ingress.IngressError) as caught:
                    guard = ingress.JSONPreflight()
                    guard.feed(body[:split])
                    guard.feed(body[split:], final=True)
                self.assertEqual(caught.exception.status_code, 400)

    def test_depth_and_many_small_objects_are_bounded_before_parsing(self):
        for body in (b'[' * 65, b'[' + b'{},' * 60 + b'{}]'):
            with mock.patch.object(ingress, 'MAX_JSON_ITEMS', 100), self.assertRaises(ingress.IngressError) as caught:
                ingress.JSONPreflight().feed(body, final=True)
            self.assertEqual(caught.exception.status_code, 413)

    def test_string_braces_are_free_but_escape_work_is_bounded(self):
        guard = ingress.JSONPreflight()
        guard.feed(json.dumps('{}[]:,' * 10000).encode(), final=True)
        self.assertEqual(guard.items, 1)
        with mock.patch.object(ingress, 'MAX_JSON_ITEMS', 100), self.assertRaises(ingress.IngressError):
            ingress.JSONPreflight().feed(json.dumps('\\' * 101).encode(), final=True)

    def test_candidate_count_has_a_hard_limit(self):
        for n in (0, -1, MAX_CANDIDATES + 1, 10**9, True, 1.5):
            with self.subTest(n=n), self.assertRaises(ValidationError):
                OpenAIRequest(model='gemini-2.5-flash', messages=[], n=n)
        self.assertEqual(OpenAIRequest(model='gemini-2.5-flash', messages=[], n=MAX_CANDIDATES).n, MAX_CANDIDATES)

    def test_diagnostics_remain_serializable_with_invalid_unicode(self):
        trace = RequestTrace('/v1/chat/completions')
        trace.configure(model='bad\ud800')
        trace.log('bad\udfff')
        trace.inspect_payload({'choices': [{'finish_reason': '\ud800'}]})
        JSONResponse(trace.snapshot(detail=True))
        self.assertFalse(auth.constant_time_equal('\ud800', 'test'))


class UploadBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_detached_task_keeps_input_budget_until_real_completion(self):
        budget = ingress.IngressBudget()
        guard = ingress.IngressReceive(mock.AsyncMock(return_value={
            'type': 'http.request', 'body': b'{}', 'more_body': False}), 100, budget)
        await guard()
        token = ingress.current_ingress.set(guard)
        try:
            task = ingress.hold_ingress(asyncio.create_task(asyncio.Event().wait()))
        finally:
            ingress.current_ingress.reset(token)
        guard.close()
        self.assertEqual(budget.used[:2], [0, 2])
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(budget.used, [0, 0, 0])

    async def test_cancelled_media_worker_keeps_input_budget_until_thread_exits(self):
        budget = ingress.IngressBudget()
        guard = ingress.IngressReceive(mock.AsyncMock(return_value={
            'type': 'http.request', 'body': b'{}', 'more_body': False}), 100, budget)
        await guard()
        started, release = threading.Event(), threading.Event()
        def worker():
            started.set()
            release.wait(2)
        token = ingress.current_ingress.set(guard)
        try:
            task = asyncio.create_task(media_tasks.run_media_task(worker))
        finally:
            ingress.current_ingress.reset(token)
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            guard.close()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.assertEqual(budget.used[:2], [0, 2])
        finally:
            release.set()
            guard.close()
            await asyncio.gather(task, return_exceptions=True)
        async with asyncio.timeout(1):
            while budget.used != [0, 0, 0]:
                await asyncio.sleep(.001)

    async def test_idle_upload_timeout_and_release(self):
        budget = ingress.IngressBudget()
        async def stalled():
            await asyncio.Event().wait()
        guard = ingress.IngressReceive(stalled, 100, budget)
        try:
            with mock.patch.object(ingress, 'UPLOAD_IDLE_SECONDS', .01), self.assertRaises(ingress.IngressError) as caught:
                await asyncio.wait_for(guard(), .5)
            self.assertEqual(caught.exception.status_code, 408)
        finally:
            guard.close()
        self.assertEqual(budget.used, [0, 0, 0])

    async def test_total_upload_timeout_cannot_be_reset_by_chunks(self):
        async def chunk():
            await asyncio.sleep(.001)
            return {'type': 'http.request', 'body': b' ', 'more_body': True}
        guard = ingress.IngressReceive(chunk, 100, ingress.IngressBudget())
        try:
            guard.deadline = ingress.time.monotonic() + .01
            with self.assertRaises(ingress.IngressError) as caught:
                async with asyncio.timeout(.5):
                    while True:
                        await guard()
            self.assertEqual(caught.exception.status_code, 408)
        finally:
            guard.close()

    async def test_aggregate_bytes_capacity_and_idempotent_release(self):
        budget = ingress.IngressBudget(requests=2, body_bytes=6)
        receive = mock.AsyncMock(return_value={'type': 'http.request', 'body': b'"abc', 'more_body': True})
        first = ingress.IngressReceive(receive, 10, budget)
        second = ingress.IngressReceive(receive, 10, budget)
        try:
            await first()
            with self.assertRaises(ingress.IngressError) as caught:
                await second()
            self.assertEqual(caught.exception.status_code, 503)
            self.assertEqual(budget.used[:2], [2, 4])
            with self.assertRaises(ingress.IngressError):
                ingress.IngressReceive(receive, 10, budget)
        finally:
            first.close()
            first.close()
            second.close()
        self.assertEqual(budget.used, [0, 0, 0])

    async def test_finished_upload_releases_slot_while_input_memory_stays_charged(self):
        budget = ingress.IngressBudget(requests=1)
        receive = mock.AsyncMock(return_value={'type': 'http.request', 'body': b'{}', 'more_body': False})
        first = ingress.IngressReceive(receive, 100, budget)
        await first()
        second = ingress.IngressReceive(receive, 100, budget)
        self.assertEqual(budget.used, [1, 2, 1])
        second.close()
        first.close()
        self.assertEqual(budget.used, [0, 0, 0])

    async def test_aggregate_json_work_capacity(self):
        budget = ingress.IngressBudget(items=1)
        guard = ingress.IngressReceive(mock.AsyncMock(return_value={
            'type': 'http.request', 'body': b'["x"]', 'more_body': False}), 100, budget)
        try:
            with self.assertRaises(ingress.IngressError) as caught:
                await guard()
            self.assertEqual(caught.exception.status_code, 503)
        finally:
            guard.close()
        self.assertEqual(budget.used, [0, 0, 0])

    async def test_chunked_body_byte_limit(self):
        receive = mock.AsyncMock(side_effect=[
            {'type': 'http.request', 'body': b'"abc', 'more_body': True},
            {'type': 'http.request', 'body': b'de"', 'more_body': False}])
        guard = ingress.IngressReceive(receive, 5, ingress.IngressBudget())
        try:
            await guard()
            with self.assertRaises(BodyTooLargeError):
                await guard()
        finally:
            guard.close()

    async def test_raw_invalid_utf8_and_split_unicode(self):
        for chunks, accepted in (([b'"\xed', b'\xa0\x80"'], False),
                                 ([b'"\xe4', b'\xb8\xad"'], True), ([b'"\xe4', b''], False)):
            messages = [{'type': 'http.request', 'body': body, 'more_body': i < len(chunks) - 1}
                        for i, body in enumerate(chunks)]
            guard = ingress.IngressReceive(mock.AsyncMock(side_effect=messages), 100, ingress.IngressBudget())
            try:
                if accepted:
                    for _ in chunks:
                        await guard()
                else:
                    with self.assertRaises(ingress.IngressError) as caught:
                        for _ in chunks:
                            await guard()
                    self.assertEqual(caught.exception.status_code, 400)
            finally:
                guard.close()


class IngressASGITests(unittest.IsolatedAsyncioTestCase):
    def scope(self, path, headers=(), query=b''):
        return {'type': 'http', 'http_version': '1.1', 'method': 'POST', 'scheme': 'http',
                'path': path, 'raw_path': path.encode(), 'query_string': query,
                'headers': [(b'content-type', b'application/json'), *headers],
                'server': ('test', 80), 'client': ('127.0.0.1', 1)}

    async def test_auth_denies_all_protocols_without_reading_body(self):
        for path in ('/v1/chat/completions', '/v1/messages', '/v1/messages/count_tokens',
                     '/v1beta/models/gemini-test:generateContent'):
            receive = mock.AsyncMock(side_effect=AssertionError('must not read unauthenticated body'))
            messages = []
            async def send(message):
                messages.append(message)
            await main.app(self.scope(path), receive, send)
            self.assertEqual(messages[0]['status'], 401)
            receive.assert_not_awaited()

    async def test_native_and_claude_credentials_still_accept_all_formats(self):
        key = config.API_KEY.encode()
        for path in ('/v1/messages', '/v1beta/models/gemini-test:generateContent'):
            formats = [([(b'authorization', b'Bearer ' + key)], b''),
                       ([(b'x-goog-api-key', key)], b''), ([], b'key=' + key)]
            if path == '/v1/messages':
                formats.append(([(b'x-api-key', key)], b''))
            for headers, query in formats:
                response = await ingress.early_generation_auth(dict(self.scope(path, headers, query), app=main.app))
                self.assertIsNone(response)

    async def test_partial_upload_returns_protocol_timeout_and_restores_admission(self):
        for path in ('/v1/chat/completions', '/v1/messages', '/v1beta/models/gemini-test:generateContent'):
            calls = 0
            async def receive():
                nonlocal calls
                calls += 1
                if calls == 1:
                    return {'type': 'http.request', 'body': b'{', 'more_body': True}
                await asyncio.Event().wait()
            messages = []
            async def send(message):
                messages.append(message)
            before = list(ingress.ingress_budget.used)
            with mock.patch.object(ingress, 'UPLOAD_IDLE_SECONDS', .01):
                await asyncio.wait_for(main.app(self.scope(path, [(b'authorization', b'Bearer ' + config.API_KEY.encode())]),
                                                receive, send), .5)
            self.assertEqual(messages[0]['status'], 408)
            self.assertEqual(ingress.ingress_budget.used, before)

    async def test_complex_json_and_invalid_unicode_never_reach_upstream(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
            headers = {'Authorization': 'Bearer ' + config.API_KEY, 'Content-Type': 'application/json'}
            body = b'{"model":"gemini-2.5-flash","messages":[],"unused":[' + b'{},' * 100 + b'{}]}'
            with mock.patch.object(ingress, 'MAX_JSON_ITEMS', 100), mock.patch('routes.chat_api._call_upstream') as upstream:
                response = await client.post('/v1/chat/completions', content=body, headers=headers)
                self.assertEqual(response.status_code, 413)
                upstream.assert_not_called()
            for body in (b'{"model":"\\ud800","messages":[]}',
                         b'{"model":"gemini-2.5-flash","messages":[],"n":1000000000}'):
                response = await client.post('/v1/chat/completions', content=body, headers=headers)
                self.assertIn(response.status_code, (400, 422))

    async def test_cancelled_upload_restores_admission(self):
        entered = asyncio.Event()
        async def receive():
            entered.set()
            await asyncio.Event().wait()
        task = asyncio.create_task(main.app(self.scope('/v1/chat/completions', [
            (b'authorization', b'Bearer ' + config.API_KEY.encode())]), receive, mock.AsyncMock()))
        before = list(ingress.ingress_budget.used)
        await asyncio.wait_for(entered.wait(), .5)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(ingress.ingress_budget.used, before)

    async def test_resumable_disconnect_keeps_input_charged_until_background_finishes(self):
        incoming = asyncio.Queue()
        body = json.dumps({'model': 'gemini-3.8-flash', 'messages': [{'role': 'user', 'content': 'hi'}], 'stream': True}).encode()
        await incoming.put({'type': 'http.request', 'body': body, 'more_body': False})
        sent, release = asyncio.Event(), asyncio.Event()
        async def source():
            yield 'data: {"choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}\n\n'
            await release.wait()
            yield 'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
        async def upstream(*args):
            return StreamingResponse(source(), media_type='text/event-stream')
        async def send(message):
            if message['type'] == 'http.response.body' and b'answer' in message.get('body', b''):
                sent.set()
        before = list(ingress.ingress_budget.used)
        store = resumable.ResumableStore()
        with mock.patch.object(chat_api, 'resumable_store', store), \
             mock.patch.object(chat_api, 'resumable_enabled', return_value=True), \
             mock.patch.object(chat_api, '_call_upstream', side_effect=upstream):
            task = asyncio.create_task(main.app(self.scope('/v1/chat/completions', [
                (b'authorization', b'Bearer ' + config.API_KEY.encode())]), incoming.get, send))
            try:
                await asyncio.wait_for(sent.wait(), 1)
                await incoming.put({'type': 'http.disconnect'})
                await asyncio.wait_for(task, 1)
                self.assertEqual(ingress.ingress_budget.used[1], before[1] + len(body))
                release.set()
                async with asyncio.timeout(1):
                    while ingress.ingress_budget.used != before:
                        await asyncio.sleep(.001)
            finally:
                release.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await store.aclose()


class CurlOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.entered = asyncio.Event()
        self.closed = asyncio.Event()
        self.release = asyncio.Event()
        self.handlers = set()
        async def serve(reader, writer):
            self.handlers.add(asyncio.current_task())
            try:
                headers = await reader.readuntil(b'\r\n\r\n')
                length = next((int(line.split(b':', 1)[1]) for line in headers.split(b'\r\n')
                               if line.lower().startswith(b'content-length:')), 0)
                if length:
                    await reader.readexactly(length)
                if b' /slow ' in headers.split(b'\r\n', 1)[0]:
                    self.entered.set()
                    await reader.read()
                    self.closed.set()
                    return
                if b' /peer ' in headers.split(b'\r\n', 1)[0]:
                    self.entered.set()
                    await self.release.wait()
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}')
                await writer.drain()
            except (ConnectionError, asyncio.IncompleteReadError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()
                self.handlers.discard(asyncio.current_task())
        self.server = await asyncio.start_server(serve, '127.0.0.1', 0)
        self.url = f'http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}'
        self.session = CancellationSafeSession(max_clients=1, trust_env=False)
        self.client = impersonate._CurlClientShim(self.session, 2, 2)

    async def asyncTearDown(self):
        self.release.set()
        await self.client.aclose()
        self.server.close()
        await self.server.wait_closed()
        for task in list(self.handlers):
            task.cancel()
        await asyncio.gather(*list(self.handlers), return_exceptions=True)

    async def request(self, mode, path):
        if mode == 'post':
            return await self.client.post(self.url + path, json={})
        async with self.client.stream('POST', self.url + path, json={}) as response:
            await response.aread()
            return response

    async def test_cancel_before_headers_releases_handle_for_both_modes(self):
        for mode in ('post', 'stream'):
            self.entered.clear()
            self.closed.clear()
            slow = asyncio.create_task(self.request(mode, '/slow'))
            await asyncio.wait_for(self.entered.wait(), 1)
            slow.cancel()
            await asyncio.wait_for(asyncio.gather(slow, return_exceptions=True), 1)
            await asyncio.wait_for(self.closed.wait(), 1)
            self.assertEqual(self.session.pool.qsize(), 1)
            response = await asyncio.wait_for(self.request(mode, '/fast'), 1)
            self.assertEqual(response.status_code, 200)

    async def test_first_reply_timeout_cleans_up_before_raising(self):
        with self.assertRaises(TimeoutError):
            async with asyncio.timeout(.03):
                await self.request('post', '/slow')
        self.assertEqual(self.session.pool.qsize(), 1)
        self.assertEqual((await self.request('post', '/fast')).status_code, 200)

    async def test_cancelling_pool_waiter_keeps_active_peer(self):
        peer = asyncio.create_task(self.request('post', '/peer'))
        await asyncio.wait_for(self.entered.wait(), 1)
        waiter = asyncio.create_task(self.request('post', '/fast'))
        await asyncio.sleep(0)
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        self.assertFalse(peer.done())
        self.release.set()
        self.assertEqual((await asyncio.wait_for(peer, 1)).status_code, 200)
        self.assertEqual(self.session.pool.qsize(), 1)


class ProxyAndProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_resolution_does_not_reuse_dead_outlet_from_parent_scope(self):
        manager = pr.ProxyProcessManager()
        uri = 'socks4://127.0.0.1:1080'
        manager._instances[uri] = SimpleNamespace(port=1080, alive=lambda: False)
        threads = []
        def launch(*args):
            threads.append(threading.current_thread().name)
            return SimpleNamespace(port=1081, alive=lambda: True)
        try:
            with mock.patch.object(pr, 'singbox_manager', manager), mock.patch.object(pr, 'ensure_reaper'), \
                 mock.patch.object(manager, '_launch', side_effect=launch), \
                 pr.resolved_proxy_scope(uri, 'socks5://127.0.0.1:1080'):
                self.assertEqual(await pr.resolve_proxy_uri_async(uri), 'socks5://127.0.0.1:1081')
                self.assertNotEqual(threads[0], threading.current_thread().name)
        finally:
            manager._instances.clear()

    async def test_bridge_exit_after_warmup_never_restarts_on_event_loop(self):
        manager = pr.ProxyProcessManager()
        uri = 'socks4://127.0.0.1:1080'
        alive = mock.Mock(side_effect=lambda: alive.call_count == 1)
        manager._instances[uri] = SimpleNamespace(port=1080, last_used=0, alive=alive)
        pool = client_pool.SdkClientPool()
        client = SimpleNamespace(close=lambda: None, aio=SimpleNamespace(aclose=mock.AsyncMock()))
        threads = []
        def launch(*args):
            threads.append(threading.current_thread().name)
            return SimpleNamespace(port=1081, last_used=0, alive=lambda: True)
        try:
            with mock.patch.object(pr, 'singbox_manager', manager), mock.patch.object(pr, 'ensure_reaper'), \
                 mock.patch.object(manager, '_launch', side_effect=launch), \
                 mock.patch.object(client_pool.genai, 'Client', return_value=client), \
                 mock.patch.object(client_pool, 'get_http_options', side_effect=lambda proxy_url: pr.resolve_proxy_uri(proxy_url)):
                result = await ProxyChain(candidates=[uri]).prepare(lambda raw, resolved: pool.get('test', raw))
                self.assertIs(result, client)
                self.assertEqual(threads, [])
                self.assertEqual(await pr.resolve_proxy_uri_async(uri), 'socks5://127.0.0.1:1081')
                self.assertEqual(len(threads), 1)
                self.assertNotEqual(threads[0], threading.current_thread().name)
        finally:
            manager._instances.clear()
            await pool.aclose()

    async def test_resolved_proxy_scopes_are_request_local_and_reset(self):
        async def operation(raw, resolved):
            await asyncio.sleep(0)
            return pr.resolve_proxy_uri(raw)
        async def resolve(raw):
            return 'socks5://127.0.0.1:' + raw.rsplit(':', 1)[1]
        with mock.patch('proxy_failover.resolve_proxy_uri_async', side_effect=resolve):
            values = await asyncio.gather(*(ProxyChain(candidates=[f'socks4://127.0.0.1:{port}']).run(operation)
                                           for port in (1081, 1082)))
        self.assertEqual(values, ['socks5://127.0.0.1:1081', 'socks5://127.0.0.1:1082'])
        self.assertIsNone(pr._resolved_proxy.get())

    async def test_ordinary_tool_json_never_triggers_image_processing(self):
        payload = {'result': [{'type': 'image', 'url': 'record.png', 'width': 640}],
                   'error': [{'type': 'image_url', 'image_url': 'record'}]}
        with mock.patch.object(mp, 'fetch_remote_image', side_effect=AssertionError('unexpected image IO')):
            for content in (payload, json.dumps(payload)):
                response, media = mp.tool_response_with_media(content, 'gemini-3.8-flash')
                self.assertEqual(response, payload)
                self.assertEqual(media, [])

    async def test_claude_sse_keeps_error_classification(self):
        for code, expected in ((401, 'authentication_error'), (429, 'rate_limit_error'),
                               (503, 'overloaded_error'), ('bad', 'api_error')):
            async def source():
                yield 'data: ' + json.dumps({'error': {'code': code, 'message': 'fixture'}}) + '\n\n'
            response = claude_api._claude_stream_response(StreamingResponse(source()), 'claude-test', omit_thinking=False)
            frames = ''.join([frame async for frame in response.body_iterator])
            self.assertIn(expected, frames)

    async def test_native_terminal_state_rejects_content_but_accepts_usage_and_other_candidates(self):
        state = gemini_native._NativeStreamState()
        state.inspect({'candidates': [{'index': 0, 'finishReason': 'STOP', 'content': {'parts': [{'text': 'first'}]}}]})
        state.inspect({'usageMetadata': {'totalTokenCount': 1}})
        state.inspect({'candidates': [{'index': 1, 'content': {'parts': [{'text': 'other'}]}, 'finishReason': 'STOP'}]})
        state.finish()
        with self.assertRaises(IncompleteResponseError):
            state.inspect({'candidates': [{'index': 0, 'content': {'parts': [{'text': 'late'}]}}]})


if __name__ == '__main__':
    unittest.main()
