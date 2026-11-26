"""Cross-channel failover is a generation boundary, even before visible text."""
from contextlib import asynccontextmanager
import base64
import time
from unittest.mock import AsyncMock, patch

import httpx

import channel_dispatch as dispatch
from admission import ManagedStreamingResponse
from streaming import DONE_FRAME, HEARTBEAT_FRAME
from test_parallel_tool_controls import _request
from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery

MODEL = "gemini-3-flash-preview"
LOSING = base64.b64encode(b"failed-attempt").decode()
WINNING = base64.b64encode(b"winning-call").decode()
CALL = {"functionCall": {"id": "winning-call", "name": "weather", "args": {}},
        "thoughtSignature": WINNING}


class SignedFailoverTests(SchemaRouteHarness):
    async def test_public_retry_and_next_gemini_wire_use_only_the_winning_generation(self):
        primary = [
            {"results": [{"data": {"candidates": [{"content": {"role": "model", "parts": [
                {"thoughtSignature": LOSING}]}}]}}]},
            {"results": [{"errors": [{"code": 503, "message": "primary interrupted"}]}]},
        ]
        calls = []

        def respond(request):
            calls.append(request)
            return httpx.Response(200, json=primary)

        self.settings["cross_channel_fallback"] = True
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as c:
            @asynccontextmanager
            async def client(*args, **kwargs):
                yield c

            with patch.object(cookie_delivery, "open_upstream_client", client):
                for dialect in ("openai", "claude"):
                    with self.subTest(dialect=dialect):
                        self.channel = "web_proxy"
                        self.reply = [CALL]
                        self.sent.clear()
                        calls.clear()
                        path, body = _request(dialect, "auto", stream=True)
                        body["model"] = MODEL
                        result = await self.client.post(path, json=body)
                        self.assertEqual((result.status_code, len(calls), len(self.sent)), (200, 1, 1))
                        self.assertNotIn("primary interrupted", result.text)
                        rows = chunks(result.text)
                        if dialect == "openai":
                            extra = {}
                            tool_calls = []
                            for row in rows:
                                for choice in row.get("choices", []):
                                    delta = choice["delta"]
                                    extra.update(delta.get("extra_content", {}).get("google", {}))
                                    tool_calls.extend({k: v for k, v in call.items() if k != "index"}
                                                      for call in delta.get("tool_calls", []))
                            turns = [
                                {"role": "assistant", "content": None, "tool_calls": tool_calls,
                                 "extra_content": {"google": extra}},
                                {"role": "tool", "tool_call_id": "winning-call", "content": "ok"},
                            ]
                        else:
                            block, = [r["content_block"] for r in rows
                                      if r["type"] == "content_block_start"
                                      and r["content_block"]["type"] == "tool_use"]
                            turns = [{"role": "assistant", "content": [block]},
                                     {"role": "user", "content": [
                                         {"type": "tool_result", "tool_use_id": block["id"], "content": "ok"}]}]
                            self.assertEqual(sum(r["type"] == "message_stop" for r in rows), 1)
                        self.channel = "api_key"
                        self.reply = [{"text": "done"}]
                        self.sent.clear()
                        body.update(stream=False, messages=[*body["messages"], *turns])
                        follow = await self.client.post(path, json=body)
                        self.assertEqual((follow.status_code, len(self.sent)), (200, 1))
                        signed = [(p.get("functionCall", {}).get("id"), p["thoughtSignature"])
                                  for t in self.sent[0]["contents"] if t["role"] == "model"
                                  for p in t["parts"] if p.get("thoughtSignature")]
                        self.assertEqual(signed, [("winning-call", WINNING)])
                        self.assertNotIn(LOSING, result.text)
                        if dialect == "openai":
                            self.assertEqual(len({r["id"] for r in rows}), 1)
                            self.assertEqual(result.text.count(DONE_FRAME), 1)

    async def relay(self, frames, alternate=None):
        async def body():
            for frame in frames:
                yield frame

        closed = AsyncMock()
        primary = ManagedStreamingResponse(body(), cleanup=closed)
        with patch.object(dispatch, "_try_channel_fallback", new=AsyncMock(return_value=alternate)) as retry:
            response = [f async for f in dispatch._stream_with_fallback_frames(
                primary, None, None, "express", "cookie", time.monotonic() + 5)]
        closed.assert_awaited_once()
        return response, retry.await_count

    async def test_keepalive_bypasses_staging_normal_completion_flushes_original_metadata(self):
        role = 'data: {"id":"one","choices":[{"delta":{"role":"assistant"}}]}\n\n'
        signed = f'data: {{"id":"one","choices":[{{"delta":{{"extra_content":{{"google":{{"thought_signature":"{WINNING}"}}}}}}}}]}}\n\n'
        finish = 'data: {"id":"one","choices":[{"delta":{},"finish_reason":"content_filter"}]}\n\n'
        usage = 'data: {"id":"one","choices":[],"usage":{"total_tokens":2}}\n\n'
        frames, retries = await self.relay([role, signed, HEARTBEAT_FRAME, finish, usage, DONE_FRAME])
        self.assertEqual(frames, [HEARTBEAT_FRAME, role, signed, finish, usage, DONE_FRAME])
        self.assertEqual(retries, 0)

    async def test_either_capacity_commits_primary_without_losing_metadata(self):
        # The UTF-8 frame fits by code points but not by bytes.
        first = 'data: {"id":"一","choices":[]}\n\n'
        error = 'data: {"error":{"code":503,"message":"failed"}}\n\n'
        for budget, prefix in (({"_FALLBACK_PREFIX_MAX_BYTES": len(first)}, [first]),
                               ({"_FALLBACK_PREFIX_MAX_FRAMES": 1}, [first, first])):
            with self.subTest(budget=budget), patch.multiple(dispatch, **budget):
                expected = [*prefix, error, DONE_FRAME]
                frames, retries = await self.relay(expected)
                self.assertEqual(frames, expected)
                self.assertEqual(retries, 0)

    async def test_retry_without_standby_discards_failed_prefix_but_keeps_error(self):
        prefix = 'data: {"id":"failed","choices":[{"delta":{"role":"assistant"}}]}\n\n'
        late = 'data: {"id":"failed","choices":[{"delta":{"content":"late"}}]}\n\n'
        error = 'data: {"error":{"code":503,"message":"failed"}}\n\n'
        frames, retries = await self.relay([prefix, HEARTBEAT_FRAME, error, late, DONE_FRAME])
        self.assertEqual(frames, [HEARTBEAT_FRAME, error, DONE_FRAME])
        self.assertEqual(retries, 1)
