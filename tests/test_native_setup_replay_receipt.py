"""Native HTTP setup failures: actual ASGI sends and an in-process HTTPX upstream."""
import asyncio
import json
import time
import unittest
from unittest.mock import Mock, patch

import httpx

from admission import generation_limiter
from credential_manager import credential_manager
from main import app
from proxy_failover import ProxyChain
import resumable
from routes import gemini_native
from test_replay_http_delivery import Peer
from upstreams import cookie_native, native_passthrough as native


ERROR = {"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "wait briefly",
                   "details": [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "2s"}]}}
SUCCESS = b'data: {"candidates":[{"index":0,"finishReason":"STOP"}]}\n\n'
SIGNED = (b'data: {"candidates":[{"index":0,"content":{"role":"model","parts":['
          b'{"functionCall":{"id":"wire-id","name":"read","args":{}},"thoughtSignature":"AP9maXh0dXJl"}]}}]}\n\n')


class NativeSetupReceiptTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(patch.object(native, "resumable_store", self.store))
        self.enterContext(patch.object(gemini_native, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(gemini_native.app_state, "get_channel_mode", return_value="api_key"))
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.enterContext(patch.object(app.state.express_key_manager, "get_express_record",
                                      return_value={"key": "local-only"}))
        self.enterContext(patch.object(native, "ProxyChain", side_effect=lambda _: ProxyChain(candidates=[""])))
        self.enterContext(patch.object(native, "get_retry_settings", return_value=(0, 0, 0)))
        self.enterContext(patch.object(native, "SETUP_FAST_WINDOW_SECONDS", 5))
        self.response = lambda: httpx.Response(429, json=ERROR, headers={"Retry-After": "2"})
        self.respond = Mock(side_effect=lambda: self.response())
        async def dispatch(_request):
            response = self.respond()
            return await response if asyncio.iscoroutine(response) else response
        client = httpx.AsyncClient(transport=httpx.MockTransport(dispatch))
        self.addAsyncCleanup(client.aclose)
        self.enterContext(patch.object(native.passthrough_client_pool, "get", return_value=client))

    def success_next(self):
        self.response = lambda: httpx.Response(200, content=SUCCESS, headers={"content-type": "text/event-stream"})

    @staticmethod
    def peer(extra=(), **kw):
        return Peer("/v1beta/models/gemini-test:streamGenerateContent", query=b"alt=sse",
                    payload={"contents": [{"parts": [{"text": "hello"}]}]},
                    headers=[(b"x-resumable", b"true"), *extra], **kw)

    async def completed(self, extra=(), **kw):
        peer = self.peer(extra, **kw)
        await asyncio.wait_for(peer.run(), 2)
        return peer

    @staticmethod
    def body(peer):
        return b"".join(message.get("body", b"") for message in peer.messages)

    def assert_http_error(self, peer):
        self.assertEqual(peer.messages[0]["status"], 429)
        self.assertEqual(json.loads(self.body(peer)), ERROR)
        self.assertEqual(dict(peer.messages[0]["headers"])[b"retry-after"], b"2")

    def signed_next(self):
        self.response = lambda: httpx.Response(
            200, content=SIGNED + SUCCESS, headers={"content-type": "text/event-stream"})

    async def test_explicit_cursor_replays_signed_events_without_a_current_provider(self):
        self.signed_next()
        first = await self.completed()
        self.assertEqual(first.messages[0]["status"], 200)
        signed, stop = [line for line in self.body(first).splitlines() if line.startswith(b"data: ")]
        self.assertIn(b'"thoughtSignature":"AP9maXh0dXJl"', signed)
        rid = first.receipt.encode()

        with patch.object(cookie_native, "handle_native_request", wraps=cookie_native.handle_native_request) as cookie:
            for mode, key_count, has_cookie in (("api_key", 0, False), ("web_proxy", 1, True), ("mixed", 0, True)):
                with self.subTest(mode=mode), \
                        patch.object(gemini_native.app_state, "get_channel_mode", return_value=mode) as channel, \
                        patch.object(app.state.express_key_manager, "get_total_keys", return_value=key_count) as keys, \
                        patch.object(credential_manager, "has_cookie_record", return_value=has_cookie):
                    for headers, expected in (([(b"x-resume-id", rid)], [signed, stop]),
                                              ([(b"last-event-id", rid + b":0")], [stop])):
                        again = await self.completed(headers)
                        self.assertEqual(again.messages[0]["status"], 200)
                        self.assertEqual(again.receipt, first.receipt)
                        self.assertEqual(dict(again.messages[0]["headers"])[b"x-resumable-replay"], b"1")
                        body = self.body(again)
                        self.assertEqual([line for line in body.splitlines() if line.startswith(b"data: ")], expected)
                        self.assertNotIn(b"[DONE]", body)
                        self.assertFalse(again.messages[-1].get("more_body"))
                    channel.assert_not_called()
                    keys.assert_not_called()
            cookie.assert_not_awaited()
        self.assertEqual(self.respond.call_count, 1)
        self.assertEqual(len(self.store._buffers), 1)

    async def test_replay_first_still_checks_auth_identity_cursor_and_policy(self):
        self.signed_next()
        payload = {"contents": [{"parts": [{"text": "hello"}]}]}
        scoped = "/v1beta/models/projects/fixture/locations/global/publishers/google/models/gemini-test:streamGenerateContent"
        first = Peer(scoped, query=b"alt=sse&fixture=a%20b%26c", payload=payload,
                     headers=[(b"x-resumable", b"true")])
        await first.run()
        self.assertEqual(first.messages[0]["status"], 200)
        rid = first.receipt.encode()

        async def reconnect(*, path=scoped, query=b"fixture=a+b%26c&alt=sse", body=payload,
                            cursor=rid, other_headers=(), auth=b"test-key"):
            peer = Peer(path, query=query, payload=body,
                        headers=[(b"x-resume-id", cursor), *other_headers])
            peer.scope["headers"] = ([(k, v) for k, v in peer.scope["headers"] if k != b"authorization"]
                                     + [(b"authorization", b"Bearer " + auth)])
            await asyncio.wait_for(peer.run(), 2)
            return peer

        with patch.object(credential_manager, "has_cookie_record", return_value=True), \
                patch.object(gemini_native.app_state, "get_channel_mode", return_value="web_proxy"), \
                patch.object(cookie_native, "handle_native_request", wraps=cookie_native.handle_native_request) as cookie:
            replay = await reconnect(path=scoped.replace("/v1beta/", "/v1/", 1))
            self.assertEqual(replay.messages[0]["status"], 200)
            self.assertEqual(replay.receipt, first.receipt)
            self.assertIn(b'"thoughtSignature":"AP9maXh0dXJl"', self.body(replay))

            cases = (({"body": {"contents": [{"parts": [{"text": "changed"}]}]}}, 409),
                     ({"path": scoped.replace("/fixture/", "/different/")}, 409),
                     ({"query": b"alt=sse&fixture=changed"}, 409),
                     ({"query": b"alt=json"}, 400),
                     ({"query": b"alt=sse&resume_from=-1"}, 400),
                     ({"query": b"alt=sse&fixture=a+b%26c&resume_from=99"}, 409),
                     ({"cursor": b"rid-unknown"}, 404),
                     ({"other_headers": [(b"x-resumable", b"false")]}, 400),
                     ({"auth": b"invalid"}, 401))
            for kwargs, expected in cases:
                with self.subTest(expected=expected, kwargs=kwargs):
                    result = await reconnect(**kwargs)
                    self.assertEqual(result.messages[0]["status"], expected)
                    self.assertNotIn(b"AP9maXh0dXJl", self.body(result))
            with patch.object(gemini_native, "API_KEY", "another-service-key"):
                different_owner = await reconnect(auth=b"another-service-key")
                self.assertEqual(different_owner.messages[0]["status"], 404)
            with patch.object(gemini_native, "resumable_enabled", return_value=False):
                disabled = await reconnect()
                self.assertEqual(disabled.messages[0]["status"], 409)
            old, = self.store._buffers.values()
            if drain := self.store._draining(old):
                await asyncio.wait_for(drain, 1)
            old.finished_monotonic = time.monotonic() - 24 * 60 * 60
            self.assertEqual(await self.store.sweep(), 1)
            expired = await reconnect()
            self.assertEqual(expired.messages[0]["status"], 404)
            cookie.assert_not_awaited()
        self.assertEqual(self.respond.call_count, 1)
        self.assertFalse(self.store._buffers)

    async def test_delivered_fast_http_429_next_unkeyed_request_regenerates_immediately(self):
        first = await self.completed()
        self.assert_http_error(first)
        old, = self.store._buffers.values()
        self.assertTrue(old.fully_delivered)
        self.success_next()
        retry = await self.completed()
        self.assertEqual(retry.messages[0]["status"], 200)
        self.assertIn(SUCCESS, self.body(retry))
        self.assertEqual(self.respond.call_count, 2)
        self.assertNotEqual(retry.receipt, old.id)

    async def test_unsent_or_ambiguous_error_body_retries_same_http_failure_until_delivered(self):
        for mode in ("headers_drop", "body_drop", "send_error", "cancel"):
            with self.subTest(mode=mode):
                self.respond.reset_mock()
                await self.store.aclose()
                peer = self.peer()
                async def lose(message):
                    if message["type"] == "http.response.start" and mode == "headers_drop":
                        await peer.drop()
                    if message["type"] == "http.response.body":
                        if mode == "body_drop":
                            await peer.drop()
                        elif mode == "send_error":
                            raise OSError("connection closed before writing body")
                        elif mode == "cancel":
                            raise asyncio.CancelledError()
                if mode in ("cancel", "send_error"):
                    with self.assertRaises(asyncio.CancelledError if mode == "cancel" else OSError):
                        await asyncio.wait_for(peer.run(lose), 2)
                else:
                    await asyncio.wait_for(peer.run(lose), 2)
                old, = self.store._buffers.values()
                self.assertFalse(old.fully_delivered or old.has_live_delivery)
                self.assertIsNotNone(old.last_disconnect_monotonic)
                retry = await self.completed()
                self.assert_http_error(retry)
                self.assertEqual((retry.receipt, self.respond.call_count), (old.id, 1))
                self.assertTrue(old.fully_delivered)

    async def test_same_key_keeps_http_status_details_and_hint_while_a_cursor_replays_one_native_event(self):
        key = [(b"idempotency-key", b"one-request")]
        self.assert_http_error(await self.completed(key))
        retry = await self.completed(key)
        self.assert_http_error(retry)
        cursor = await self.completed([(b"x-resume-id", retry.receipt.encode())])
        self.assertEqual(cursor.messages[0]["status"], 200)  # explicit SSE cursor
        self.assertEqual(dict(cursor.messages[0]["headers"])[b"retry-after"], b"2")
        wire = self.body(cursor)
        self.assertIn(f"id: {retry.receipt}:0".encode(), wire)
        self.assertIn(b": upstream-status 429\n: retry-after 2\n", wire)
        self.assertEqual([json.loads(line[6:]) for line in wire.splitlines() if line.startswith(b"data: ")], [ERROR])
        self.assertNotIn(b"[DONE]", wire)
        end = await self.completed([(b"last-event-id", f"{retry.receipt}:0".encode())])
        self.assertNotIn(b"data:", self.body(end))
        self.assertEqual(self.respond.call_count, 1)

    async def test_first_buffered_error_headers_name_the_generation_before_body_delivery(self):
        # Even if the original HTTP error body is lost, the received headers
        # give the caller a server-issued cursor for the original generation.
        self.response = lambda: httpx.Response(429, json=ERROR, headers={
            "Retry-After": "2", "X-Resume-Id": "upstream-id-not-ours"})
        first = self.peer()
        async def drop_body(message):
            if message["type"] == "http.response.body":
                await first.drop()
        await first.run(drop_body)
        buf, = self.store._buffers.values()
        headers = dict(first.messages[0]["headers"])
        self.assertEqual((first.messages[0]["status"], headers[b"retry-after"], first.receipt),
                         (429, b"2", buf.id))
        self.assertNotIn(b"x-resumable-replay", headers)
        self.assertFalse(buf.fully_delivered)
        cursor = await self.completed([(b"x-resume-id", first.receipt.encode())])
        self.assertEqual(cursor.receipt, first.receipt)
        self.assertEqual([json.loads(line[6:]) for line in self.body(cursor).splitlines()
                          if line.startswith(b"data: ")], [ERROR])
        self.assertEqual(self.respond.call_count, 1)

        direct = Peer("/v1beta/models/gemini-test:streamGenerateContent", query=b"alt=sse",
                      payload={"contents": [{"parts": [{"text": "hello"}]}]}, headers=[(b"x-resumable", b"false")])
        await direct.run()
        self.assert_http_error(direct)
        self.assertNotIn(b"x-resume-id", dict(direct.messages[0]["headers"]))
        self.assertEqual(self.respond.call_count, 2)

    async def test_bounded_setup_comments_leave_http_hint_and_exact_event_intact(self):
        key = [(b"idempotency-key", b"native-comment-bound")]
        for hint, expected in (("9" * 128, True), ("9" * 140, False)):
            for slow in (False, True):
                with self.subTest(size=len(hint), slow=slow):
                    await self.store.aclose()
                    self.respond.reset_mock()
                    if slow:
                        closing, release, sent = (asyncio.Event() for _ in range(3))
                        self.hold_error_close(closing, release, [], hint=hint)
                        first = self.peer(key)
                        async def started(message):
                            if message["type"] == "http.response.start":
                                sent.set()
                        with patch.object(native, "SETUP_FAST_WINDOW_SECONDS", 0):
                            issuing = asyncio.create_task(first.run(started))
                            try:
                                await asyncio.wait_for(closing.wait(), 1)
                                await asyncio.wait_for(sent.wait(), 1)
                            finally:
                                release.set()
                                await asyncio.wait_for(issuing, 2)
                        self.assertEqual(first.messages[0]["status"], 200)
                    else:
                        self.response = lambda: httpx.Response(429, json=ERROR, headers={"retry-after": hint})
                        first = await self.completed(key)
                        self.assertEqual(first.messages[0]["status"], 429)
                        self.assertEqual(dict(first.messages[0]["headers"])[b"retry-after"], hint.encode())
                        self.assertEqual(json.loads(self.body(first)), ERROR)

                    http_retry = await self.completed(key)
                    self.assertEqual(http_retry.messages[0]["status"], 429)
                    self.assertEqual(dict(http_retry.messages[0]["headers"])[b"retry-after"], hint.encode())
                    self.assertEqual(json.loads(self.body(http_retry)), ERROR)

                    rid = http_retry.receipt
                    cursor = await self.completed([(b"x-resume-id", rid.encode())])
                    self.assertEqual(cursor.messages[0]["status"], 200)
                    self.assertEqual(dict(cursor.messages[0]["headers"])[b"retry-after"], hint.encode())
                    for peer in ((first, cursor) if slow else (cursor,)):
                        lines = self.body(peer).splitlines()
                        self.assertIn(b": upstream-status 429", lines)
                        self.assertEqual([line for line in lines if line.startswith(b": retry-after ")],
                                         [b": retry-after " + hint.encode()] if expected else [])
                        self.assertEqual([line for line in lines if line.startswith(b"id: ")],
                                         [f"id: {rid}:0".encode()])
                        self.assertEqual([json.loads(line[6:]) for line in lines if line.startswith(b"data: ")],
                                         [ERROR])
                    end = await self.completed([(b"last-event-id", f"{rid}:0".encode())])
                    self.assertNotIn(b"data:", self.body(end))
                    self.assertEqual(self.respond.call_count, 1)

    def hold_error_close(self, closing, release, closes, *, hint="2"):
        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield json.dumps(ERROR).encode()

            async def aclose(self):
                closes.append("start")
                closing.set()
                await release.wait()
                closes.append("finish")

        self.response = lambda: httpx.Response(429, stream=Body(), headers={"retry-after": hint,
                                                                           "content-type": "application/json"})

    async def test_follower_committed_before_known_429_and_slow_physical_close_share_one_dispatch(self):
        closing, release, sse_started = (asyncio.Event() for _ in range(3))
        closes = []
        self.hold_error_close(closing, release, closes)
        baseline = generation_limiter.active
        first, follower = self.peer(), self.peer()
        initial = asyncio.create_task(first.run())
        reading = None
        try:
            await asyncio.wait_for(closing.wait(), 1)
            async def started(message):
                if message["type"] == "http.response.start":
                    sse_started.set()
            reading = asyncio.create_task(follower.run(started))
            await asyncio.wait_for(sse_started.wait(), 1)
            buf, = self.store._buffers.values()
            self.assertFalse(buf.done or buf.fully_delivered)
            self.assertIsNotNone(self.store._draining(buf))
            self.assertEqual((self.respond.call_count, generation_limiter.active), (1, baseline + 1))
        finally:
            release.set()
            await asyncio.wait_for(asyncio.gather(initial, *([reading] if reading is not None else [])), 2)
        self.assert_http_error(first)
        self.assertEqual((follower.messages[0]["status"], closes, generation_limiter.active), (200, ["start", "finish"], baseline))
        wire = self.body(follower)
        self.assertIn(b": upstream-status 429\n: retry-after 2\n", wire)
        self.assertEqual([json.loads(line[6:]) for line in wire.splitlines() if line.startswith(b"data: ")], [ERROR])
        self.assertEqual(self.respond.call_count, 1)
        self.assertTrue(buf.fully_delivered)

    async def test_delivered_deadline_waits_for_old_http_error_connection_before_regenerating(self):
        closing, release, draining = (asyncio.Event() for _ in range(3))
        closes = []
        self.hold_error_close(closing, release, closes)
        first, retry = self.peer(), self.peer()
        second = None
        with patch.object(native, "SETUP_FAST_WINDOW_SECONDS", 0):
            initial = asyncio.create_task(first.run())
            try:
                await asyncio.wait_for(closing.wait(), 1)
                old, = self.store._buffers.values()
                old.deadline_monotonic = time.monotonic() - 1
                await self.store.sweep()  # publish canonical 504 while physical close is still held
                await asyncio.wait_for(asyncio.shield(initial), 1)
                self.assertEqual((old.fully_delivered, old.status_code), (True, 504))
                self.assertIn(b"504", self.body(first))
                accounted = self.store._bytes_used
                current = self.store._draining
                def checked(buf):
                    task = current(buf)
                    if buf is old and task is not None:
                        draining.set()
                    return task
                self.success_next()
                with patch.object(self.store, "_draining", side_effect=checked):
                    second = asyncio.create_task(retry.run())
                    await asyncio.wait_for(draining.wait(), 1)
                    self.assertFalse(second.done())
                    self.assertEqual(self.respond.call_count, 1)
                    self.assertEqual((self.store._requests[old.request_key], self.store._bytes_used), (old.id, accounted))
            finally:
                release.set()
                await asyncio.wait_for(asyncio.gather(initial, *([second] if second is not None else [])), 2)
        self.assertEqual(closes, ["start", "finish"])
        self.assertNotEqual(first.receipt, retry.receipt)
        self.assertEqual(self.respond.call_count, 2)
        self.assertIn(SUCCESS, self.body(retry))

    async def test_expired_undelivered_http_failure_keeps_410_on_exact_cursor_and_allows_fresh_retry(self):
        first = self.peer()
        async def lose(message):
            if message["type"] == "http.response.body":
                await first.drop()
        with patch.object(resumable, "disconnect_grace_seconds", return_value=0):
            await first.run(lose)
        old, = self.store._buffers.values()
        self.assertFalse(old.fully_delivered)
        stale = await self.completed([(b"x-resume-id", old.id.encode())])
        self.assertEqual(stale.messages[0]["status"], 410)
        self.success_next()
        fresh = await self.completed()
        self.assertIn(SUCCESS, self.body(fresh))
        self.assertEqual(self.respond.call_count, 2)

    async def test_unusable_advisory_and_over_budget_diagnostic_are_not_retained(self):
        self.response = lambda: httpx.Response(503, json={"error": {"message": "large" * 5000}},
                                               headers={"Retry-After": "not a retry delay"})
        key = [(b"idempotency-key", b"bounded")]
        first, retry = await self.completed(key), await self.completed(key)
        for peer in (first, retry):
            self.assertEqual(peer.messages[0]["status"], 503)
            self.assertNotIn(b"retry-after", dict(peer.messages[0]["headers"]))
        self.assertIn(b"large", self.body(first))  # original native HTTP body stays byte-faithful
        old, = self.store._buffers.values()
        self.assertNotIn("large", old.error)
        self.assertLess(old.bytes_stored, native._MAX_REPLAY_DIAGNOSTIC_BYTES)
        self.assertEqual(json.loads(self.body(retry))["error"]["status"], "UNAVAILABLE")
        self.assertEqual(self.respond.call_count, 1)
