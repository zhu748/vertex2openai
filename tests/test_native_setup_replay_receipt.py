"""Native HTTP setup failures: actual ASGI sends and an in-process HTTPX upstream."""
import asyncio
import json
import time
import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

import httpx

from admission import generation_limiter, image_limiter
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
    def peer(extra=(), *, model="gemini-test", **kw):
        return Peer(f"/v1beta/models/{model}:streamGenerateContent", query=b"alt=sse",
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

    @contextmanager
    def live_outlets(self, mode, key_count, has_cookie):
        with (patch.object(gemini_native.app_state, "get_channel_mode", return_value=mode) as channel,
              patch.object(app.state.express_key_manager, "get_total_keys", return_value=key_count) as keys,
              patch.object(credential_manager, "has_cookie_record", return_value=has_cookie) as cookies):
            yield channel, keys, cookies

    def configure_capacity(self):
        self.success_next()
        self.enterContext(patch.object(gemini_native, "image_bypass_enabled", return_value=False))
        original = gemini_native.app_state.get_committed_setting
        self.enterContext(patch.object(gemini_native.app_state, "get_committed_setting", side_effect=lambda k, d=None:
            1 if k in {"upstream_max_concurrency", "image_max_concurrency"} else original(k, d)))

    async def test_local_generation_and_image_admission_preserve_a_real_http_receipt_until_delivery(self):
        self.configure_capacity()
        for model, limiter in (("gemini-test", generation_limiter), ("gemini-2.5-flash-image", image_limiter)):
            for lost, keyed in ((False, False), (True, False), (False, True), (True, True)):
                with self.subTest(model=model, lost=lost, keyed=keyed):
                    await self.store.aclose()
                    self.respond.reset_mock()
                    key = [(b"idempotency-key", b"local-capacity")] if keyed else ()
                    expected = {"error": {"code": 503, "message": limiter.full_message, "status": "UNAVAILABLE"}}
                    first = self.peer(key, model=model)

                    async def lose_final(message):
                        if lost and message["type"] == "http.response.body":
                            await first.drop()

                    lease = limiter.reserve()
                    try:
                        await asyncio.wait_for(first.run(lose_final), 2)
                    finally:
                        lease.release()
                    old, = self.store._buffers.values()
                    self.assertEqual((first.messages[0]["status"], first.receipt), (503, old.id))
                    self.assertNotIn(b"x-resumable-replay", dict(first.messages[0]["headers"]))
                    self.assertEqual((old.status_code, old.fully_delivered, old.has_live_delivery, old.payload),
                                     (503, not lost, False, expected))
                    if not lost:
                        self.assertEqual(json.loads(self.body(first)), expected)
                    self.respond.assert_not_called()

                    if lost or keyed:
                        again = await self.completed(key, model=model)
                        self.assertEqual((again.messages[0]["status"], again.receipt, json.loads(self.body(again))),
                                         (503, old.id, expected))
                        self.assertEqual(dict(again.messages[0]["headers"])[b"x-resumable-replay"], b"1")
                        self.assertTrue(old.fully_delivered)
                    fresh = await self.completed(key, model=model)
                    if keyed:
                        self.assertEqual((fresh.messages[0]["status"], fresh.receipt, json.loads(self.body(fresh))),
                                         (503, old.id, expected))
                        self.respond.assert_not_called()
                    else:
                        self.assertNotEqual(fresh.receipt, old.id)
                        self.assertIn(SUCCESS, self.body(fresh))
                        self.respond.assert_called_once()
                    cursor = await self.completed([(b"x-resume-id", old.id.encode())], model=model)
                    wire = self.body(cursor)
                    self.assertEqual((cursor.messages[0]["status"], cursor.receipt), (200, old.id))
                    self.assertEqual([line for line in wire.splitlines() if line.startswith(b"id: ")],
                                     [f"id: {old.id}:0".encode()])
                    self.assertEqual([json.loads(line[6:]) for line in wire.splitlines() if line.startswith(b"data: ")],
                                     [expected])
                    self.assertNotIn(b": upstream-status ", wire)  # rejected locally; no provider status
                    self.assertNotIn(b"[DONE]", wire)
                    end = await self.completed([(b"last-event-id", f"{old.id}:0".encode())], model=model)
                    self.assertNotIn(b"data:", self.body(end))
                    self.assertEqual(self.respond.call_count, 0 if keyed else 1)

                    self.assertEqual((generation_limiter.active, image_limiter.active), (0, 0))

    async def test_follower_already_committed_to_sse_gets_one_local_rejection_and_no_provider_request(self):
        self.configure_capacity()
        leader, follower = self.peer(), self.peer()
        enter, proceed, headers_sent = (asyncio.Event() for _ in range(3))
        original = native._passthrough_request

        async def gated(*args):
            enter.set()
            await proceed.wait()
            return await original(*args)  # real limiter is held, not a mocked exception

        async def started(message):
            if message["type"] == "http.response.start":
                headers_sent.set()

        lease = generation_limiter.reserve()
        try:
            with patch.object(native, "_passthrough_request", gated):
                a = asyncio.create_task(leader.run())
                b = None
                try:
                    await asyncio.wait_for(enter.wait(), 2)
                    b = asyncio.create_task(follower.run(started))
                    await asyncio.wait_for(headers_sent.wait(), 2)
                finally:
                    proceed.set()
                    await asyncio.wait_for(asyncio.gather(a, *([b] if b else [])), 2)
        finally:
            lease.release()
        self.assertEqual((leader.messages[0]["status"], follower.messages[0]["status"]), (503, 200))
        old, = self.store._buffers.values()
        self.assertEqual((leader.receipt, follower.receipt, old.fully_delivered, old.next_seq),
                         (old.id, old.id, True, 1))
        wire = self.body(follower)
        self.assertEqual([json.loads(line[6:]) for line in wire.splitlines() if line.startswith(b"data: ")],
                         [json.loads(self.body(leader))])
        self.assertNotIn(b": upstream-status ", wire)
        self.assertNotIn(b"[DONE]", wire)
        self.respond.assert_not_called()
        healthy = await self.completed()
        self.assertNotEqual(healthy.receipt, old.id)
        self.assertIn(SUCCESS, self.body(healthy))
        self.respond.assert_called_once()

    async def test_explicit_cursor_replays_signed_events_without_a_current_provider(self):
        self.signed_next()
        first = await self.completed()
        self.assertEqual(first.messages[0]["status"], 200)
        signed, stop = [line for line in self.body(first).splitlines() if line.startswith(b"data: ")]
        self.assertIn(b'"thoughtSignature":"AP9maXh0dXJl"', signed)
        rid = first.receipt.encode()
        self.assertTrue(self.body(first).startswith(b": keep-alive\n\n"))
        self.assertEqual([line for line in self.body(first).splitlines() if line.startswith(b"id: ")],
                         [b"id: " + rid + b":0", b"id: " + rid + b":1"])

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

    async def test_implicit_keyed_retries_preserve_wire_and_http_receipts_without_live_outlets(self):
        first_key = [(b"idempotency-key", b"one-generation")]
        outlet_changes = (("api_key", 0, False), ("web_proxy", 1, True), ("mixed", 0, True))
        for signed in (False, True):
            await self.store.aclose()
            self.respond.reset_mock()
            if signed:
                self.signed_next()
            first = await self.completed(first_key)
            self.assertEqual(first.messages[0]["status"], 200 if signed else 429)
            if signed:
                self.assertIn(b'"thoughtSignature":"AP9maXh0dXJl"', self.body(first))
            else:
                self.assert_http_error(first)
            old, = self.store._buffers.values()
            self.assertTrue(old.fully_delivered)
            before = (tuple(old.events), dict(self.store._requests), self.store._bytes_used)
            with (patch.object(self.store, "get_or_create_request", wraps=self.store.get_or_create_request) as admit,
                  patch.object(cookie_native, "handle_native_request", wraps=cookie_native.handle_native_request) as cookie):
                for mode, *outlets in outlet_changes:
                    for spelling in (b"idempotency-key", b"x-idempotency-key"):
                        with self.subTest(signed=signed, mode=mode, spelling=spelling), self.live_outlets(mode, *outlets) as checks:
                            again = await self.completed([(spelling, b"one-generation")])
                            self.assertEqual((again.messages[0]["status"], again.receipt),
                                             (first.messages[0]["status"], first.receipt))
                            self.assertEqual(dict(again.messages[0]["headers"])[b"x-resumable-replay"], b"1")
                            self.assertNotIn(b"[DONE]", self.body(again))
                            if signed:
                                self.assertEqual(self.body(again), self.body(first))
                            else:
                                self.assert_http_error(again)
                            for check in checks:
                                check.assert_not_called()
                cookie.assert_not_awaited()
                admit.assert_not_awaited()
            self.respond.assert_called_once()
            self.assertEqual((tuple(old.events), dict(self.store._requests), self.store._bytes_used), before)
            self.assertEqual((old.has_live_delivery, generation_limiter.active), (False, 0))

    async def test_implicit_live_follower_joins_one_original_provider_after_mode_change(self):
        for key in ([], [(b"idempotency-key", b"live-generation")]):
            with self.subTest(keyed=bool(key)):
                await self.store.aclose()
                self.respond.reset_mock()
                reading, release, started = (asyncio.Event() for _ in range(3))
                closed = Mock()

                class Body(httpx.AsyncByteStream):
                    async def __aiter__(self):
                        reading.set()
                        await release.wait()
                        yield SIGNED + SUCCESS

                    async def aclose(self):
                        closed()

                self.response = lambda: httpx.Response(200, stream=Body(), headers={"content-type": "text/event-stream"})
                leader, follower = self.peer(key), self.peer(key)

                async def header(message):
                    if message["type"] == "http.response.start":
                        started.set()

                a = asyncio.create_task(leader.run())
                b = None
                try:
                    await asyncio.wait_for(reading.wait(), 2)
                    with self.live_outlets("web_proxy", 0, True) as checks:
                        b = asyncio.create_task(follower.run(header))
                        await asyncio.wait_for(started.wait(), 2)
                        old, = self.store._buffers.values()
                        self.assertEqual((len(old.clients), old.done, generation_limiter.active), (2, False, 1))
                        for check in checks:
                            check.assert_not_called()
                finally:
                    release.set()
                    await asyncio.wait_for(asyncio.gather(a, *([b] if b else [])), 2)
                self.assertEqual((leader.messages[0]["status"], follower.messages[0]["status"]), (200, 200))
                self.assertEqual((leader.receipt, follower.receipt, self.body(leader)),
                                 (old.id, old.id, self.body(follower)))
                self.assertIn(b'"thoughtSignature":"AP9maXh0dXJl"', self.body(follower))
                self.assertTrue(old.fully_delivered)
                self.assertFalse(old.has_live_delivery)
                self.respond.assert_called_once()
                closed.assert_called_once()
                self.assertEqual(generation_limiter.active, 0)

    async def test_unkeyed_retry_uses_live_policy_only_after_original_http_result_was_delivered(self):
        self.configure_capacity()
        first = self.peer()

        async def lose_final(message):
            if message["type"] == "http.response.body":
                await first.drop()

        lease = generation_limiter.reserve()
        try:
            await asyncio.wait_for(first.run(lose_final), 2)
        finally:
            lease.release()
        old, = self.store._buffers.values()
        self.assertEqual((first.receipt, old.fully_delivered, old.status_code), (old.id, False, 503))
        with self.live_outlets("web_proxy", 1, True) as checks:
            delivered = await self.completed()
            self.assertEqual((delivered.messages[0]["status"], delivered.receipt, json.loads(self.body(delivered))),
                             (503, old.id, old.payload))
            self.assertTrue(old.fully_delivered)
            for check in checks:
                check.assert_not_called()
            fresh = await self.completed()
            self.assertEqual(fresh.messages[0]["status"], 400)  # Fresh Cookie native SSE still cannot buffer.
            self.assertNotIn(b"x-resume-id", dict(fresh.messages[0]["headers"]))
        self.respond.assert_not_called()
        self.assertEqual(list(self.store._buffers), [old.id])
        healthy = await self.completed()
        self.assertEqual(healthy.messages[0]["status"], 200)
        self.assertNotEqual(healthy.receipt, old.id)
        self.respond.assert_called_once()

    async def test_stale_implicit_retry_does_not_touch_or_retire_the_old_receipt_ahead_of_dispatch(self):
        key = [(b"idempotency-key", b"stale-generation")]
        first = self.peer(key)

        async def lose_final(message):
            if message["type"] == "http.response.body":
                await first.drop()

        with patch.object(resumable, "disconnect_grace_seconds", return_value=0):
            await asyncio.wait_for(first.run(lose_final), 2)
        old, = self.store._buffers.values()
        self.assertTrue(old.resume_expired())
        before = (old.last_access, old._retired_by, dict(self.store._requests), self.store._bytes_used)
        with self.live_outlets("api_key", 0, False):
            failed = await self.completed(key)
            self.assertEqual(failed.messages[0]["status"], 403)
            self.assertNotIn(b"x-resume-id", dict(failed.messages[0]["headers"]))
            self.assertEqual((old.last_access, old._retired_by, dict(self.store._requests), self.store._bytes_used), before)
            stale = await self.completed([(b"x-resume-id", first.receipt.encode())])
            self.assertEqual(stale.messages[0]["status"], 410)
        self.success_next()
        fresh = await self.completed(key)
        self.assertEqual(fresh.messages[0]["status"], 200)
        self.assertNotEqual(fresh.receipt, old.id)
        self.assertEqual(self.respond.call_count, 2)

    async def test_implicit_probe_misses_do_not_allocate_readers_or_override_identity_and_policy(self):
        self.signed_next()
        model = "projects/fixture/locations/global/publishers/google/models/gemini-test"
        path = f"/v1beta/models/{model}:streamGenerateContent"
        payload = {"contents": [{"parts": [{"text": "hello"}]}]}
        key = (b"idempotency-key", b"scoped-operation")
        opt = (b"x-resumable", b"true")

        def peer(*, target=path, url_query=b"alt=sse&fixture=a%20b", headers=(opt, key), auth=None, wire_prefix=b""):
            current = Peer(target, query=url_query, payload=payload, headers=headers)
            current.body = wire_prefix + current.body
            if auth is not None:
                current.scope["headers"] = [(k, v) for k, v in current.scope["headers"] if k != b"authorization"]
                current.scope["headers"].append((b"authorization", b"Bearer " + auth))
            return current

        first = peer()
        await asyncio.wait_for(first.run(), 2)
        old, = self.store._buffers.values()
        cursor = (b"x-resume-id", first.receipt.encode())

        async def check(request, status):
            await asyncio.wait_for(request.run(), 2)
            self.assertEqual(request.messages[0]["status"], status)
            if status != 200:
                self.assertNotIn(b"x-resume-id", dict(request.messages[0]["headers"]))
                self.assertNotIn(SIGNED, self.body(request))

        for headers in ((key,), ((opt[0], b"false"), key)):
            direct = peer(headers=headers)
            await check(direct, 200)
            self.assertNotIn(b"x-resume-id", dict(direct.messages[0]["headers"]))
            self.assertIn(SIGNED, self.body(direct))
        self.assertEqual(list(self.store._buffers), [old.id])

        with (self.live_outlets("api_key", 0, False),
              patch.object(self.store, "get_or_create_request", wraps=self.store.get_or_create_request) as admit):
            equivalent = peer(target=path.replace("/v1beta/", "/v1/", 1), url_query=b"fixture=a+b&alt=sse")
            await check(equivalent, 200)
            self.assertEqual((equivalent.receipt, self.body(equivalent)), (first.receipt, self.body(first)))
            original = (old.last_access, tuple(old.events), dict(self.store._requests), self.store._bytes_used)
            for kwargs, status in (
                    ({"headers": (opt, (key[0], b"unrelated"))}, 403), ({"headers": (key,)}, 403),
                    ({"headers": ((opt[0], b"false"), key)}, 403),
                    ({"headers": ((opt[0], b"false"), cursor)}, 400),
                    ({"target": path.replace("/fixture/", "/different/")}, 403),
                    ({"url_query": b"alt=sse&fixture=different"}, 403), ({"wire_prefix": b" "}, 403),
                    ({"url_query": b"alt=json&fixture=a+b"}, 400), ({"auth": b"bad"}, 401)):
                with self.subTest(kwargs=kwargs):
                    await check(peer(**kwargs), status)
            with patch.object(gemini_native, "API_KEY", "rotated"):
                await check(peer(auth=b"rotated"), 403)
                await check(peer(auth=b"rotated", headers=(cursor,)), 404)
            for setting, value in (("resumable_enabled", False), ("image_bypass_enabled", True)):
                with patch.object(gemini_native, setting, return_value=value):
                    await check(peer(), 403)
                    await check(peer(headers=(cursor,)), 409)
            with patch.object(old, "owner_key_digest", "different"):
                await check(peer(), 404)
            with patch.object(old, "fingerprint_key", "different"):
                await check(peer(), 409)
            with patch.object(old, "kind", "stream"):
                await check(peer(), 403)
            self.assertEqual((old.last_access, tuple(old.events), dict(self.store._requests), self.store._bytes_used),
                             original)
            old.finished_monotonic = time.monotonic() - 24 * 60 * 60
            self.assertEqual(await self.store.sweep(), 1)
            await check(peer(), 403)
            await check(peer(headers=(cursor,)), 404)
            self.assertFalse(self.store._buffers)
            admit.assert_not_awaited()
        self.assertEqual(self.respond.call_count, 3)  # first buffered reply and two explicit direct requests

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
