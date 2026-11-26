"""A detached upstream or iterator terminal is not a direct Chat send receipt."""

import asyncio
from contextlib import ExitStack, aclosing
import json
from unittest.mock import patch

import httpx
from fastapi.responses import JSONResponse
from google import genai
from google.genai import types
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect

import channel_dispatch
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
