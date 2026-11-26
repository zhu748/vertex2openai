"""A detached upstream or iterator terminal is not a direct Chat send receipt."""

import asyncio
import base64
from contextlib import AsyncExitStack, ExitStack, aclosing, asynccontextmanager
import json
from unittest.mock import patch

import httpx
from fastapi.responses import JSONResponse
from google import genai
from google.genai import types
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect

import channel_dispatch
import resumable
from routes import chat_api
from runtime_state import app_state
from signature_store import SKIP_VALIDATOR_SENTINEL
from streaming import DONE_FRAME
from test_bounded_signature_origins import BoundedOriginRoutes, original, provider_call, results, wire_calls
from test_claude_omitted_signature_projection import new_cache
from test_parallel_tool_controls import _request
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import express_sdk

MODEL = "gemini-3-flash-preview"
CALL_ID = "existing-weather-call"
A = b"\xfb\xff-original-fixture"
B = b"\xfd-conflicting-fixture"
C = b"\x80-standby-fixture"
ERROR_FRAME = 'data: {"error":{"code":503,"message":"fixture"}}\n\n'


class ChatSignatureDeliveryRoutes(SchemaRouteHarness):
    echo = BoundedOriginRoutes.assistant_echo

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings.update(express_location="global", retry_max=0)
        self.key_manager.get_express_record = lambda **_: {"key": "fixture-key", "project_id": "fixture"}

    def turn(self, mode, **changes):
        self.settings["fake_streaming"] = mode == "fake"
        path, body = _request("openai", "auto", stream=mode != "json")
        body.update(model=MODEL, messages=[{"role": "user", "content": "Local forecast fixture."}])
        body.update(changes)
        return path, body

    async def issue(self, path, body, mode, signature, *, ident=CALL_ID):
        self.reply = [provider_call(ident, signature)]
        response = await self.client.post(path, json=body)
        self.assertEqual(response.status_code, 200, response.text[:180])
        if mode != "json":
            self.assertTrue(response.text.endswith(DONE_FRAME))
        return self.echo(response, "openai", mode)

    async def replay(self, path, body, assistant):
        self.reply = {"text": "Fixture result received."}
        self.finish_reason = "STOP"
        self.sent.clear()
        ids = [call["id"] for call in assistant["tool_calls"]]
        response = await self.client.post(path, json={
            **body, "stream": False, "n": 1,
            "messages": [*body["messages"], assistant, *results("openai", ids, "done")],
        })
        self.assertEqual(response.status_code, 200, response.text[:180])
        wire, = self.sent
        native = wire.get("variables", wire)
        calls = wire_calls(wire)
        answers = [p["functionResponse"]["id"] for turn in native["contents"] for p in turn["parts"]
                   if "functionResponse" in p]
        self.assertEqual([p["functionCall"]["id"] for p in calls], ids)
        self.assertEqual(answers, ids)
        return [SKIP_VALIDATOR_SENTINEL if p.get("thoughtSignature") == SKIP_VALIDATOR_SENTINEL.decode()
                else original(p) for p in calls]

    async def abandon(self, path, body, *, spec, failure, stage, headers):
        peer = Peer(path, payload=body, spec=spec, headers=headers)
        intercepted = []

        async def before_send(message):
            if message["type"] != "http.response.body":
                return
            target = not message.get("more_body") if stage == "final" else b"data: [DONE]" in message["body"]
            if target:
                intercepted.append(message["body"])
                if failure == "raise":
                    raise OSError("fixture peer rejected send")
                await peer.drop()

        if failure == "raise":
            with self.assertRaises((OSError, ClientDisconnect)):
                await asyncio.wait_for(peer.run(before_send), 2)
        else:
            await asyncio.wait_for(peer.run(before_send), 2)
        self.assertEqual(len(intercepted), 1)
        return peer

    def retained_store(self, stack):
        store = resumable.ResumableStore()
        stack.push_async_callback(store.aclose)
        stack.enter_context(patch.object(chat_api, "resumable_store", store))
        stack.enter_context(patch.object(chat_api, "resumable_enabled", return_value=True))
        return store

    @asynccontextmanager
    async def retained_case(self, **case):
        with self.subTest(**case):
            async with AsyncExitStack() as stack:
                yield stack

    async def test_retained_origin_waits_for_one_verified_full_send(self):
        for self.channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                for method, spec, failure in (("GET", "2.4", "silent"), ("POST", "2.4", "raise"),
                                               ("POST", "2.3", "silent")):
                    async with self.retained_case(channel=self.channel, mode=mode, method=method,
                                                  failure=failure, spec=spec) as stack:
                        cache = new_cache(stack)
                        store = self.retained_store(stack)
                        path, body = self.turn(mode)
                        prior = await self.issue(path, body, mode, A)
                        self.assertEqual(await self.replay(path, body, prior), [A])
                        commits = stack.enter_context(patch.object(cache, "register_origin_batches",
                                                                  wraps=cache.register_origin_batches))
                        key = [(b"idempotency-key", b"retained-signature-fixture")]
                        self.reply = [provider_call(CALL_ID, B)]
                        lost = await self.abandon(path, body, spec=spec, failure=failure, stage="final",
                                                  headers=key)
                        rid = lost.receipt
                        buf = await store.get_exact(rid)
                        self.assertEqual(buf.status, "done")
                        self.assertFalse(buf.fully_delivered)
                        self.assertEqual(await self.replay(path, body, prior), [A])
                        self.assertGreater(store._chat_origin_key_bytes, 0)
                        self.assertGreater(store._chat_origin_signature_bytes, 0)
                        self.assertLessEqual(store._chat_origin_key_bytes, buf.bytes_stored)
                        poll_path = "/v1/resumable/" + rid
                        if mode != "json":
                            head = await self.client.get(poll_path, params={"from": 0, "limit": 1})
                            self.assertFalse(head.json()["done"])
                            at_end = await self.client.get(poll_path, params={"from": buf.next_seq})
                            self.assertTrue(at_end.json()["done"])
                            self.assertFalse(at_end.json()["events"])
                            self.assertFalse(buf.fully_delivered)
                            tail = await self.client.get(poll_path, params={"from": buf.next_seq - 1})
                            self.assertTrue(tail.json()["done"])
                            terminal, = tail.json()["events"]
                            self.assertIn(DONE_FRAME, terminal["data"])
                            self.assertTrue(buf.fully_delivered)  # Existing idempotency semantics.
                            cursor = Peer(path, payload=body, headers=[(b"x-resume-id", rid.encode())],
                                          query=f"resume_from={buf.next_seq - 1}".encode())
                            await asyncio.wait_for(cursor.run(), 2)
                            self.assertEqual(cursor.receipt, rid)
                            raw = b"".join(m.get("body", b"") for m in cursor.messages)
                            self.assertIn(DONE_FRAME.encode(), raw)
                            self.assertNotIn(b"tool_calls", raw)
                            self.assertEqual(await self.replay(path, body, prior), [A])
                        self.assertEqual(commits.call_count, 0)

                        # Losing a full replay after others fetched a suffix does not
                        # borrow their successful delivery or discard the proof.
                        again = await self.abandon(path, body, spec="2.4", failure="silent", stage="final",
                            headers=[(b"x-resume-id", rid.encode())])
                        self.assertEqual(again.receipt, rid)
                        self.assertEqual(await self.replay(path, body, prior), [A])
                        self.assertEqual(commits.call_count, 0)

                        self.sent.clear()
                        async def full():
                            if method == "GET":
                                response = await self.client.get(poll_path, params={"from": 0})
                                self.assertEqual(response.status_code, 200)
                                self.assertTrue(response.json()["done"])
                            else:
                                peer = Peer(path, payload=body, headers=key, spec=spec)
                                await asyncio.wait_for(peer.run(), 2)
                                self.assertEqual(peer.receipt, rid)
                                self.assertEqual(dict(peer.messages[0]["headers"])[b"x-resumable-replay"], b"1")

                        await asyncio.gather(full(), full())
                        self.assertEqual(commits.call_count, 1)
                        self.assertEqual(self.sent, [])  # Same keyed generation, no new call.
                        self.assertTrue(buf.fully_delivered)
                        self.assertEqual((store._chat_origin_key_bytes, store._chat_origin_signature_bytes), (0, 0))
                        self.assertEqual(await self.replay(path, body, prior), [None])

    async def test_abandoned_first_retained_generation_has_no_positive_origin_until_replay(self):
        for self.channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                async with self.retained_case(channel=self.channel, mode=mode) as stack:
                    new_cache(stack)
                    self.retained_store(stack)
                    path, body = self.turn(mode)
                    self.reply = [provider_call(CALL_ID, A)]
                    lost = await self.abandon(path, body, spec="2.4", failure="silent", stage="final", headers=[])
                    raw = b"".join(m.get("body", b"") for m in (*lost.messages, *lost.discarded))
                    assistant = self.echo(httpx.Response(200, content=raw), "openai", mode)
                    self.assertEqual(await self.replay(path, body, assistant), [None])
                    replay = Peer(path, payload=body, headers=[(b"x-resume-id", lost.receipt.encode())])
                    await asyncio.wait_for(replay.run(), 2)
                    self.assertEqual(await self.replay(path, body, assistant), [A])

    async def test_retained_synthetic_done_and_replacement_json_cannot_publish_a_stage(self):
        for self.channel, upstream in (("api_key", channel_dispatch.express_upstream),
                                       ("web_proxy", channel_dispatch.cookie_upstream)):
            for mode in ("json", "fake", "stream"):
                async with self.retained_case(channel=self.channel, mode=mode) as stack:
                    new_cache(stack)
                    store = self.retained_store(stack)
                    path, body = self.turn(mode)
                    prior = await self.issue(path, body, mode, A)
                    real = upstream.chat_completions

                    async def wrong_source(*args, **kwargs):
                        response = await real(*args, **kwargs)
                        if mode == "json":
                            return JSONResponse({"error": {"code": 502, "message": "fixture replacement"}})
                        source = response.body_iterator

                        async def frames():
                            async with aclosing(source):
                                async for frame in source:
                                    if frame != DONE_FRAME:
                                        yield frame  # pump_stream synthesizes its usual framing.

                        response.body_iterator = frames()
                        return response

                    self.reply = [provider_call(CALL_ID, B)]
                    peer = Peer(path, payload=body)
                    with patch.object(upstream, "chat_completions", side_effect=wrong_source):
                        await asyncio.wait_for(peer.run(), 2)
                    raw = b"".join(m.get("body", b"") for m in peer.messages)
                    self.assertIn(b'"error"' if mode == "json" else DONE_FRAME.encode(), raw)
                    self.assertTrue((await store.get_exact(peer.receipt)).fully_delivered)
                    self.assertEqual((store._chat_origin_key_bytes, store._chat_origin_signature_bytes), (0, 0))
                    self.assertEqual(await self.replay(path, body, prior), [A])

    async def test_retained_proof_budget_preserves_only_a_fence_until_full_delivery(self):
        for mode in ("json", "stream"):
            for limit in ("_CHAT_ORIGIN_SIGNATURES_PER_BUFFER", "_CHAT_ORIGIN_SIGNATURES_PER_STORE"):
                async with self.retained_case(mode=mode, limit=limit) as stack:
                    new_cache(stack)
                    store = self.retained_store(stack)
                    stack.enter_context(patch.object(resumable, limit, 0))
                    path, body = self.turn(mode)
                    prior = await self.issue(path, body, mode, A)
                    self.reply = [provider_call(CALL_ID, B)]
                    lost = await self.abandon(path, body, spec="2.4", failure="silent", stage="final", headers=[])
                    self.assertGreater(store._chat_origin_key_bytes, 0)
                    self.assertEqual(store._chat_origin_signature_bytes, 0)
                    self.assertEqual(await self.replay(path, body, prior), [A])
                    later = Peer(path, payload=body, headers=[(b"x-resume-id", lost.receipt.encode())])
                    await asyncio.wait_for(later.run(), 2)
                    # The explicit original is still on the public wire, even
                    # when the proof cannot keep a second in-memory copy.
                    raw = b"".join(m.get("body", b"") for m in later.messages)
                    self.assertIn(base64.b64encode(B), raw)
                    self.assertEqual(await self.replay(path, body, prior), [None])
                    self.assertEqual((store._chat_origin_key_bytes, store._chat_origin_signature_bytes), (0, 0))

    async def test_expired_and_over_capacity_retained_replies_do_not_fence_prior_origin(self):
        for mode in ("json", "stream"):
            for loss in ("deadline", "grace", "drop", "body-budget"):
                async with self.retained_case(mode=mode, loss=loss) as stack:
                    new_cache(stack)
                    store = self.retained_store(stack)
                    path, body = self.turn(mode)
                    prior = await self.issue(path, body, mode, A)
                    self.reply = [provider_call(CALL_ID, B)]
                    if loss == "deadline":
                        async def too_late(*_args, **_kwargs):
                            await asyncio.Future()
                        with (patch.object(chat_api, "_call_upstream", side_effect=too_late),
                              patch.object(resumable, "request_timeout_seconds", return_value=0.04)):
                            peer = Peer(path, payload=body)
                            await asyncio.wait_for(peer.run(), 2)
                        self.assertEqual((await store.get_exact(peer.receipt)).status_code, 504)
                    elif loss == "body-budget":
                        byte_limit = resumable._byte_limit
                        with patch.object(resumable, "_byte_limit", side_effect=lambda setting, default:
                                          128 if setting == "resumable_max_buffer_mb" else byte_limit(setting, default)):
                            peer = Peer(path, payload=body)
                            await asyncio.wait_for(peer.run(), 2)
                        self.assertEqual((await store.get_exact(peer.receipt)).status_code, 503)
                    else:
                        lost = await self.abandon(path, body, spec="2.4", failure="silent", stage="final", headers=[])
                        buf = await store.get_exact(lost.receipt)
                        self.assertGreater(store._chat_origin_signature_bytes, 0)
                        if loss == "drop":
                            self.assertTrue(await store.drop(buf.id, expected=buf))
                        else:
                            buf.last_disconnect_grace_seconds = 0
                        peer = Peer(path, payload=body, headers=[(b"x-resume-id", lost.receipt.encode())])
                        await asyncio.wait_for(peer.run(), 2)
                        self.assertEqual(peer.messages[0]["status"], 404 if loss == "drop" else 410)
                    self.assertEqual((store._chat_origin_key_bytes, store._chat_origin_signature_bytes), (0, 0))
                    self.assertEqual(await self.replay(path, body, prior), [A])

    async def test_lost_final_and_done_do_not_fence_an_earlier_delivered_tool_turn(self):
        failures = (("2.4", "raise"), ("2.4", "silent"), ("2.3", "silent"))
        for self.channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                for stage in (("final",) if mode == "json" else ("done", "final")):
                    for spec, failure in failures:
                        with (self.subTest(channel=self.channel, mode=mode, stage=stage, spec=spec, failure=failure),
                              ExitStack() as stack):
                            new_cache(stack)
                            path, body = self.turn(mode)
                            first = await self.issue(path, body, mode, A)
                            self.assertEqual(await self.replay(path, body, first), [A])
                            self.reply = [provider_call(CALL_ID, B)]
                            await self.abandon(path, body, spec=spec, failure=failure, stage=stage,
                                               headers=[(b"x-resumable", b"false")])
                            self.assertEqual(await self.replay(path, body, first), [A])
                            # A real send of B carries the same visible history and does fence A.
                            second = await self.issue(path, body, mode, B)
                            self.assertEqual(first, second)
                            self.assertEqual(await self.replay(path, body, first), [None])

    async def test_implicit_capacity_fallback_uses_same_direct_receipt(self):
        for self.channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                with self.subTest(channel=self.channel, mode=mode), ExitStack() as stack:
                    new_cache(stack)
                    path, body = self.turn(mode)
                    first = await self.issue(path, body, mode, A)
                    self.reply = [provider_call(CALL_ID, B)]
                    with (patch.object(chat_api, "resumable_enabled", return_value=True),
                          patch.object(chat_api, "resolve_buffer",
                                       side_effect=chat_api.FreshReplayCapacityError(503, "fixture capacity"))):
                        lost = await self.abandon(path, body, spec="2.4", failure="silent",
                                                  stage="final", headers=[])
                        headers = dict(lost.messages[0]["headers"])
                        self.assertEqual(headers.get(b"x-resumable"), b"false")
                        self.assertNotIn(b"x-resume-id", headers)
                        self.assertEqual(await self.replay(path, body, first), [A])
                        successful = Peer(path, payload=body)
                        self.reply = [provider_call(CALL_ID, B)]
                        await successful.run()
                    self.assertEqual(dict(successful.messages[0]["headers"]).get(b"x-resumable"), b"false")
                    self.assertEqual(await self.replay(path, body, first), [None])

    async def test_in_flight_final_send_keeps_prior_origin_and_existing_background(self):
        upstream = channel_dispatch.express_upstream
        real = upstream.chat_completions
        for mode in ("json", "stream"):
            with self.subTest(mode=mode), ExitStack() as stack:
                new_cache(stack)
                path, body = self.turn(mode)
                first = await self.issue(path, body, mode, A)
                entered, release = asyncio.Event(), asyncio.Event()
                after = []
                peer = Peer(path, payload=body, headers=[(b"x-resumable", b"false")])

                async def source(*args, **kwargs):
                    response = await real(*args, **kwargs)

                    async def original_background():
                        after.append(True)

                    response.background = BackgroundTask(original_background)
                    return response

                async def wait_for_ack(message):
                    if message["type"] == "http.response.body" and not message.get("more_body"):
                        entered.set()
                        await release.wait()

                self.reply = [provider_call(CALL_ID, B)]
                with patch.object(upstream, "chat_completions", side_effect=source):
                    task = asyncio.create_task(peer.run(wait_for_ack))
                    try:
                        await asyncio.wait_for(entered.wait(), 2)
                    finally:
                        if not entered.is_set():
                            release.set()
                            await task
                try:
                    self.assertEqual(after, [])
                    self.assertEqual(await self.replay(path, body, first), [A])
                finally:
                    release.set()
                    await asyncio.wait_for(task, 2)
                self.assertEqual(after, [True])
                self.assertEqual(await self.replay(path, body, first), [None])

    async def test_fences_without_signature_bytes_also_wait_for_receipt(self):
        for self.channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                with self.subTest(channel=self.channel, mode=mode), ExitStack() as stack:
                    new_cache(stack)
                    path, body = self.turn(mode)
                    first = await self.issue(path, body, mode, A)
                    self.reply = [provider_call(CALL_ID, B)]
                    self.finish_reason = "MAX_TOKENS"
                    await self.abandon(path, body, spec="2.4", failure="silent", stage="final",
                                       headers=[(b"x-resumable", b"false")])
                    self.assertEqual(await self.replay(path, body, first), [A])
                    self.finish_reason = "MAX_TOKENS"
                    second = await self.issue(path, body, mode, B)
                    self.assertEqual(first, second)
                    self.assertEqual(await self.replay(path, body, first), [None])

    async def test_abandoned_first_offer_cannot_create_a_positive_exact_origin(self):
        for self.channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                with self.subTest(channel=self.channel, mode=mode), ExitStack() as stack:
                    new_cache(stack)
                    path, body = self.turn(mode)
                    self.reply = [provider_call(CALL_ID, A)]
                    peer = await self.abandon(path, body, spec="2.4", failure="silent", stage="final",
                                              headers=[(b"x-resumable", b"false")])
                    raw = b"".join(m.get("body", b"") for m in (*peer.messages, *peer.discarded))
                    assistant = self.echo(httpx.Response(200, content=raw), "openai", mode)
                    self.assertEqual(await self.replay(path, body, assistant), [None])
                    self.assertEqual(await self.issue(path, body, mode, A), assistant)
                    self.assertEqual(await self.replay(path, body, assistant), [A])

    async def test_invalid_replacement_json_does_not_piggyback_on_accepted_producer(self):
        for self.channel, upstream in (("api_key", channel_dispatch.express_upstream),
                                       ("web_proxy", channel_dispatch.cookie_upstream)):
            with self.subTest(channel=self.channel), ExitStack() as stack:
                new_cache(stack)
                path, body = self.turn("json")
                prior = await self.issue(path, body, "json", A)
                original_producer = upstream.chat_completions

                async def replace_accepted(*args, **kwargs):
                    source = await original_producer(*args, **kwargs)
                    self.assertEqual(source.status_code, 200)
                    self.assertEqual(json.loads(source.body)["choices"][0]["finish_reason"], "tool_calls")
                    return JSONResponse({"error": {"code": 502, "message": "replacement fixture"}})

                with patch.object(upstream, "chat_completions", side_effect=replace_accepted):
                    self.reply = [provider_call(CALL_ID, B)]
                    result = await self.client.post(path, json=body)
                    self.assertIn("error", result.json())
                self.assertEqual(await self.replay(path, body, prior), [A])

    async def test_abnormal_sse_terminal_paths_do_not_publish_staged_signature(self):
        upstream = channel_dispatch.express_upstream
        real = upstream.chat_completions
        for defect in ("drop_done", "earlier_error", "later_error", "later_raise", "second_done", "no_eof"):
            with self.subTest(defect=defect), ExitStack() as stack:
                new_cache(stack)
                path, body = self.turn("stream")
                prior = await self.issue(path, body, "stream", A)

                async def replace_iterator(*args, **kwargs):
                    response = await real(*args, **kwargs)
                    source = response.body_iterator

                    async def frames():
                        async with aclosing(source):
                            async for frame in source:
                                if frame == DONE_FRAME:
                                    if defect == "drop_done":
                                        return
                                    if defect == "earlier_error":
                                        yield ERROR_FRAME
                                yield frame
                            if defect == "later_error":
                                yield ERROR_FRAME
                            elif defect == "later_raise":
                                raise RuntimeError("fixture post-terminal error")
                            elif defect == "second_done":
                                yield DONE_FRAME
                            elif defect == "no_eof":
                                await asyncio.Event().wait()

                    response.body_iterator = frames()
                    return response

                with patch.object(upstream, "chat_completions", side_effect=replace_iterator):
                    self.reply = [provider_call(CALL_ID, B)]
                    response = await asyncio.wait_for(self.client.post(path, json=body), 2)
                    self.assertEqual(response.status_code, 200)
                    self.assertIn(CALL_ID, response.text)
                    self.assertEqual(response.text.count(DONE_FRAME), 0 if defect == "drop_done" else 1)
                self.assertEqual(await self.replay(path, body, prior), [A])

    async def test_winning_standby_cannot_publish_a_losing_source_and_keeps_its_own_target(self):
        express = channel_dispatch.express_upstream
        real = express.chat_completions
        for mode in ("json", "fake", "stream"):
            with self.subTest(mode=mode), ExitStack() as stack:
                new_cache(stack)
                self.channel = "api_key"
                self.settings["cross_channel_fallback"] = False
                path, body = self.turn(mode)
                prior = await self.issue(path, body, mode, A)

                async def alternate(*args, **kwargs):
                    response = await real(*args, **kwargs)
                    if mode == "json":
                        self.assertEqual(response.status_code, 200)
                        self.reply = [provider_call("standby-call", C)]
                        return JSONResponse({"error": {"code": 503, "message": "fixture"}}, status_code=503)
                    source = response.body_iterator

                    async def frames():
                        # Simulate a tentative producer validated before its
                        # adapter refused to forward any of its own frames.
                        async with aclosing(source):
                            async for _ in source:
                                pass
                        self.reply = [provider_call("standby-call", C)]
                        yield ERROR_FRAME
                        yield DONE_FRAME

                    response.body_iterator = frames()
                    return response

                self.settings["cross_channel_fallback"] = True
                standby = {"id": "standby", "cookie": "SAPISID=offline", "project_id": "standby", "enabled": True}
                with (patch.object(express, "chat_completions", side_effect=alternate),
                      patch.object(channel_dispatch.credential_manager, "has_cookie_record", return_value=True),
                      patch.object(app_state, "get_cookie_accounts", return_value=[standby])):
                    self.reply = [provider_call(CALL_ID, B)]
                    self.sent.clear()
                    result = await self.client.post(path, json=body)
                    self.assertEqual(result.status_code, 200, result.text[:220])
                    self.assertNotIn(CALL_ID, result.text)
                    self.assertEqual(["variables" in b for b in self.sent], [False, True])
                    winner = self.echo(result, "openai", mode)
                self.settings["cross_channel_fallback"] = False
                self.assertEqual(await self.replay(path, body, prior), [A])
                self.assertEqual(await self.replay(path, body, winner), [None])
                self.channel = "web_proxy"
                with patch.object(app_state, "get_cookie_accounts", return_value=[standby]):
                    self.assertEqual(await self.replay(path, body, winner), [C])

    async def test_multi_choice_distinct_origins_and_equal_visible_choices_conflict(self):
        def respond(request):
            body = json.loads(request.content)
            self.sent.append(body)
            if isinstance(self.reply, list):
                candidates = [{"index": i, "content": {"role": "model", "parts": [p]}, "finishReason": "STOP"}
                              for i, p in enumerate(self.reply)]
            else:
                candidates = [{"index": 0, "content": {"role": "model", "parts": [self.reply]}, "finishReason": "STOP"}]
            value = {"candidates": candidates}
            if request.url.path.endswith(":streamGenerateContent"):
                return httpx.Response(200, text=f"data: {json.dumps(value)}\n\n",
                                      headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=value)

        sdk = genai.Client(vertexai=True, api_key="offline-multi", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(respond)}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        with patch.object(express_sdk, "get_pooled_express_client", return_value=sdk):
            for mode in ("json", "fake", "stream"):
                for identical in (False, True):
                    with self.subTest(mode=mode, identical=identical), ExitStack() as stack:
                        new_cache(stack)
                        path, body = self.turn(mode, n=2, model="gemini-2.5-flash")
                        ids = (CALL_ID, CALL_ID if identical else "different-second")
                        self.reply = [provider_call(ident, sig) for ident, sig in zip(ids, (A, B), strict=True)]
                        response = await self.client.post(path, json=body)
                        self.assertEqual(response.status_code, 200, response.text[:160])
                        value = response.json() if mode == "json" else [
                            json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: {")]
                        for i in (0, 1):
                            if mode == "json":
                                rendered = {"choices": [c for c in value["choices"] if c["index"] == i]}
                                one = httpx.Response(200, json=rendered)
                            else:
                                items = [{**f, "choices": [c for c in f["choices"] if c["index"] == i]} for f in value]
                                one = httpx.Response(200, text="".join(f"data: {json.dumps(f)}\n\n" for f in items))
                            assistant = self.echo(one, "openai", mode)
                            self.assertEqual(await self.replay(path, body, assistant), [None if identical else (A, B)[i]])
