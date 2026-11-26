"""One aggregate logical outcome for the real Chat/Messages stream relay."""
import asyncio
from contextlib import asynccontextmanager
import json
from unittest.mock import patch

from fastapi.responses import JSONResponse
import httpx
from google import genai
from google.genai import types

import admission
import channel_dispatch as dispatch
from logger import ProxyStats
import main
from request_tracking import request_registry
from resumable import ResumableStore
from routes import chat_api, claude_api
from test_parallel_tool_controls import _request
from test_replay_http_delivery import Peer
from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery, express_pipeline, express_sdk


CHANNELS = ("express", "cookie")
DIALECTS = ("openai", "claude")
SENT_TEXT = "winning answer"
USAGE = {"promptTokenCount": 7, "candidatesTokenCount": 3, "totalTokenCount": 10}


class IncompleteStream(httpx.AsyncByteStream):
    def __init__(self, first):
        self.first = first

    async def __aiter__(self):
        yield self.first
        raise httpx.RemoteProtocolError("incomplete provider response after visible text")


class CrossChannelStreamCountsTests(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings.update(cross_channel_fallback=True, retry_max=0)
        self.counters = ProxyStats()
        self.calls = []
        self.answers = {}
        self.partial_source = self.stall = None
        self.started, self.release, self.stopped = (asyncio.Event() for _ in range(3))
        for module in (main, dispatch, express_pipeline, cookie_delivery, chat_api, claude_api):
            self.enterContext(patch.object(module, "stats", self.counters))

        async def express(request):
            return await self.provider("express", request)

        async def cookie(request):
            return await self.provider("cookie", request)

        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(express)}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.MockTransport(cookie)))

        @asynccontextmanager
        async def studio(*_args, **_kwargs):
            yield client

        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        self.enterContext(patch.object(cookie_delivery, "open_upstream_client", studio))

    async def provider(self, channel, request):
        self.calls.append(channel)
        if channel == self.stall:
            self.started.set()
            try:
                await self.release.wait()
            finally:
                self.stopped.set()
        status, hint = self.answers.get(channel, (200, None))
        if status != 200:
            return httpx.Response(status, headers={"Retry-After": hint} if hint else {},
                                  json={"error": {"code": status, "message": f"{channel} fixture failure"}})

        partial = self.partial_source == channel
        candidate = {"content": {"role": "model", "parts": [{"text": SENT_TEXT}]}}
        if not partial:
            candidate["finishReason"] = "STOP"
        result = {"candidates": [candidate], "usageMetadata": USAGE}
        if channel == "cookie":
            result = {"results": [{"data": result}]}
        if channel == "express" and ":streamGenerateContent" in request.url.path:
            first = f"data: {json.dumps(result)}\n\n".encode()
            headers = {"content-type": "text/event-stream"}
        else:
            first, headers = json.dumps(result).encode(), {"content-type": "application/json"}
        if partial:
            return httpx.Response(200, stream=IncompleteStream(first), headers=headers)
        return httpx.Response(200, content=first, headers=headers)

    def select(self, primary):
        self.calls.clear()
        self.answers.clear()
        self.channel = "api_key" if primary == "express" else "web_proxy"
        return "cookie" if primary == "express" else "express"

    def delta(self, before):
        after = self.counters.get_json_stats()
        return tuple(after[k] - before[k] for k in ("total", "success", "error", "prompt_tokens", "completion_tokens"))

    async def send(self, dialect, **params):
        path, body = _request(dialect, None, tools=False, stream=True)
        return await self.client.post(path, json={**body, **params})

    def peer(self, dialect, headers):
        path, body = _request(dialect, None, tools=False, stream=True)
        return Peer(path, payload=body, headers=headers)

    @staticmethod
    def wire(peer):
        return b"".join(m.get("body", b"") for m in peer.messages)

    @staticmethod
    def output_text(body, dialect):
        frames = chunks(body)
        if dialect == "claude":
            return "".join(frame["delta"]["text"] for frame in frames
                           if frame.get("delta", {}).get("type") == "text_delta")
        return "".join(ch.get("delta", {}).get("content") or ""
                       for frame in frames for ch in frame.get("choices", []))

    async def test_winning_standby_drops_only_provisional_error_in_true_and_fake_streams(self):
        for primary in CHANNELS:
            for dialect in DIALECTS:
                for fake in (False, True):
                    with self.subTest(primary=primary, dialect=dialect, fake=fake):
                        self.settings["fake_streaming"] = fake
                        standby = self.select(primary)
                        self.answers[primary] = (503, None)
                        before = self.counters.get_json_stats()
                        reply = await self.send(dialect)
                        self.assertEqual(reply.status_code, 200)
                        self.assertEqual(self.output_text(reply.text, dialect), SENT_TEXT)
                        self.assertNotIn("fixture failure", reply.text)
                        self.assertEqual(reply.text.count("[DONE]" if dialect == "openai" else "event: message_stop"), 1)
                        self.assertEqual(self.calls, [primary, standby])
                        usage = (7, 3) if standby == "express" else (0, 0)
                        self.assertEqual(self.delta(before), (1, 1, 0, *usage))
                        self.assertEqual(request_registry.get(reply.headers["x-trace-id"]).snapshot()["outcome"],
                                         "succeeded")
                        self.assertEqual(admission.generation_limiter.active, 0)

    async def test_advisory_stream_failures_have_one_outcome_and_unchanged_wire(self):
        for primary in CHANNELS:
            for dialect in DIALECTS:
                for advisory_from in ("primary", "standby"):
                    with self.subTest(primary=primary, dialect=dialect, advisory=advisory_from):
                        standby = self.select(primary)
                        final = primary if advisory_from == "primary" else standby
                        self.answers[primary] = (503, None)
                        self.answers[final] = (429, "42")
                        before = self.counters.get_json_stats()
                        reply = await self.send(dialect)
                        self.assertEqual(reply.status_code, 200)
                        self.assertIn(f"{final} fixture failure", reply.text)
                        self.assertNotIn(SENT_TEXT, reply.text)
                        if dialect == "openai":
                            failures = [json.loads(line[6:])["error"] for line in reply.text.splitlines()
                                        if line.startswith('data: {"error"')]
                            failure, = failures
                            self.assertEqual((failure["code"], failure["retry_after"]), (429, "42"))
                            self.assertEqual(reply.text.count("data: [DONE]"), 1)
                        else:
                            self.assertIn(": retry-after 42", reply.text)
                            self.assertEqual(reply.text.count("event: error"), 1)
                        self.assertEqual(self.calls, [primary] if final == primary else [primary, standby])
                        self.assertEqual(self.delta(before), (1, 0, 1, 0, 0))

    async def test_unavailable_standby_replays_and_counts_the_primary_error_once(self):
        for primary in CHANNELS:
            for dialect in DIALECTS:
                for raises in (True, False):
                    with self.subTest(primary=primary, dialect=dialect, raises=raises):
                        self.select(primary)
                        self.answers[primary] = (503, None)
                        with patch.object(dispatch, "_invoke_channel",
                                          side_effect=httpx.ReadTimeout("standby setup") if raises else None,
                                          return_value=JSONResponse({"error": {"code": 403}}, status_code=403)) as alt:
                            before = self.counters.get_json_stats()
                            reply = await self.send(dialect)
                        self.assertIn(f"{primary} fixture failure", reply.text)
                        self.assertNotIn(SENT_TEXT, reply.text)
                        self.assertEqual(self.calls, [primary])
                        alt.assert_awaited_once()
                        self.assertEqual(self.delta(before), (1, 0, 1, 0, 0))

    async def test_partial_output_commits_primary_and_terminal_error_counts_once(self):
        for primary in CHANNELS:
            for dialect in DIALECTS:
                with self.subTest(primary=primary, dialect=dialect):
                    self.select(primary)
                    self.partial_source = primary
                    before = self.counters.get_json_stats()
                    reply = await self.send(dialect)
                    self.assertIn(SENT_TEXT, reply.text)
                    self.assertIn("incomplete provider response after visible text", reply.text)
                    self.assertEqual(self.calls, [primary])
                    self.assertEqual(self.delta(before), (1, 0, 1, 0, 0))

    async def test_direct_disconnect_does_not_count_abandoned_primary_error(self):
        for dialect in DIALECTS:
            with self.subTest(dialect=dialect):
                self.stall = self.select("express")
                self.answers["express"] = (503, None)
                self.started.clear()
                self.stopped.clear()
                before = self.counters.get_json_stats()
                peer = self.peer(dialect, [(b"x-resumable", b"false")])
                sender = asyncio.create_task(peer.run())
                try:
                    await asyncio.wait_for(self.started.wait(), 1)
                    self.assertEqual(self.delta(before), (1, 0, 0, 0, 0))
                    await peer.drop()
                    await asyncio.wait_for(asyncio.shield(sender), 1)
                    self.assertTrue(self.stopped.is_set())
                finally:
                    if not sender.done():
                        sender.cancel()
                    await asyncio.gather(sender, return_exceptions=True)
                self.assertNotIn(b"fixture failure", self.wire(peer))
                self.assertNotIn(b"[DONE]", self.wire(peer))
                self.assertEqual(self.calls, ["express", "cookie"])
                self.assertEqual(self.delta(before), (1, 0, 0, 0, 0))
                self.assertEqual(admission.generation_limiter.active, 0)

    async def test_detached_winner_replays_after_disconnect_without_counting_a_second_generation(self):
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(chat_api, "resumable_store", store))
        self.settings["resumable_enabled"] = True
        self.stall = self.select("cookie")
        self.answers["cookie"] = (503, None)
        before = self.counters.get_json_stats()
        headers = [(b"x-resumable", b"true"), (b"idempotency-key", b"generation-once")]
        peer = self.peer("openai", headers)
        sent = asyncio.Event()

        async def first_headers(message):
            if message["type"] == "http.response.start":
                sent.set()

        sender = asyncio.create_task(peer.run(first_headers))
        follower = None
        try:
            await asyncio.wait_for(sent.wait(), 1)
            await asyncio.wait_for(self.started.wait(), 1)
            await peer.drop()
            await asyncio.wait_for(asyncio.shield(sender), 1)
            self.assertEqual(self.delta(before), (1, 0, 0, 0, 0))
            self.assertFalse(self.stopped.is_set())
            replay = self.peer("openai", [*headers, (b"x-resume-id", peer.receipt.encode())])
            follower = asyncio.create_task(replay.run())
            self.release.set()
            await asyncio.wait_for(asyncio.shield(follower), 1)
            self.assertEqual(replay.receipt, peer.receipt)
            self.assertEqual(dict(replay.messages[0]["headers"])[b"x-resumable-replay"], b"1")
            self.assertIn(SENT_TEXT.encode(), self.wire(replay))
            self.assertNotIn(b"fixture failure", self.wire(replay))
            self.assertEqual(self.wire(replay).count(b"data: [DONE]"), 1)
            self.assertEqual(self.calls, ["cookie", "express"])
            # The POST count includes the separate reader, but it did no generation.
            self.assertEqual(self.delta(before), (2, 1, 0, 7, 3))
        finally:
            self.release.set()
            for task in (sender, follower):
                if task is not None:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_actual_usage_survives_cancellation_before_terminal(self):
        self.select("express")
        path, body = _request("openai", None, tools=False, stream=True)
        body["stream_options"] = {"include_usage": True}
        peer = Peer(path, payload=body, headers=[(b"x-resumable", b"false")])
        usage_offered = asyncio.Event()
        before = self.counters.get_json_stats()

        async def blocked_usage_message(message):
            if (body := message.get("body", b"")).startswith(b"data: ") and b'"prompt_tokens"' in body:
                usage_offered.set()
                await asyncio.Event().wait()

        sender = asyncio.create_task(peer.run(blocked_usage_message))
        try:
            await asyncio.wait_for(usage_offered.wait(), 1)
            self.assertEqual(self.delta(before), (1, 0, 0, 7, 3))
            await peer.drop()
            await asyncio.wait_for(asyncio.shield(sender), 1)
        finally:
            if not sender.done():
                sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)
        self.assertEqual(self.calls, ["express"])
        self.assertNotIn(b"[DONE]", self.wire(peer))
        self.assertEqual(self.delta(before), (1, 0, 0, 7, 3))
