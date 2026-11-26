"""Anthropic route must not manufacture successful completion from broken upstreams."""
import asyncio
import json
import unittest
from unittest import mock

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response, StreamingResponse

from routes import claude_api


def choice(message, finish):
    return {"choices": [{"index": 0, "message": message, "finish_reason": finish}]}


class ClaudeTerminalRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        app = FastAPI()
        app.include_router(claude_api.router)
        app.dependency_overrides[claude_api.get_claude_auth] = lambda: "offline"
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        self.addAsyncCleanup(self.client.aclose)
        patch = mock.patch.object(claude_api, "_available_models", mock.AsyncMock(return_value=[]))
        self.addCleanup(patch.stop)
        patch.start()

    async def send(self, upstream, *, stream=False):
        with mock.patch.object(claude_api, "call_upstream", mock.AsyncMock(return_value=upstream)):
            return await self.client.post("/v1/messages", json={"model": "model", "max_tokens": 32,
                                          "stream": stream, "messages": [{"role": "user", "content": "hi"}]})

    async def test_retry_comment_is_bounded_validated_and_not_an_anthropic_field(self):
        for hint, allowed in (("0", True), ("nonsense", False), ("45\nretry: 0", False),
                              (123, False), ("9" * 256, False)):
            with self.subTest(hint=hint):
                error = {"code": 429, "message": "original quota", "retry_after": hint}
                async def records():
                    yield f"data: {json.dumps({'error': error})}\n\n"
                result = await self.send(StreamingResponse(records()), stream=True)
                self.assertEqual(": retry-after " in result.text, allowed)
                self.assertNotIn("\nretry:", result.text)  # no server-controlled reconnect interval
                wire = [json.loads(line[6:]) for line in result.text.splitlines() if line.startswith("data: ")]
                self.assertEqual([e["type"] for e in wire], ["message_start", "error"])
                self.assertEqual(wire[-1]["error"], {"type": "rate_limit_error", "message": "original quota"})

    async def test_header_only_transcodes_no_unvalidated_headers(self):
        first = JSONResponse({"error": {"message": "original quota", "retry_after": "13"}},
                             status_code=429, headers={"retry-after": "not a delay", "x-private": "internal"})
        result = await self.send(first)
        self.assertEqual(result.status_code, 429)
        self.assertNotIn("retry-after", result.headers)
        self.assertNotIn("x-private", result.headers)
        self.assertEqual(result.json()["error"], {"type": "rate_limit_error", "message": "original quota"})

    async def test_malformed_internal_success_returns_anthropic_502_not_end_turn(self):
        bad_choices = (None, True, [], {}, {"choices": {}}, {"choices": [None]}, {"choices": [[]]},
                       {"choices": [{"message": {"content": "partial"}}]}, choice(None, "stop"),
                       {"choices": [{"message": {"content": "one"}, "finish_reason": "stop"}] * 2},
                       choice({"role": "user", "content": "wrong speaker"}, "stop"),
                       choice({"content": ""}, "stop"), choice({"content": 123}, "length"),
                       choice({"content": "text", "reasoning_content": {}}, "stop"),
                       choice({"content": "text", "tool_calls": {}}, "stop"),
                       choice({"content": "partial"}, None), choice({"content": "partial"}, "new-reason"),
                       choice({"content": "partial"}, []), choice({"content": "partial"}, "tool_calls"))
        bodies = [Response(b"", media_type="application/json"), Response(b"truncated{"),
                  *(JSONResponse(payload) for payload in bad_choices)]
        for upstream in bodies:
            with self.subTest(upstream=upstream.body):
                reply = await self.send(upstream)
                self.assertEqual(reply.status_code, 502)
                body = reply.json()
                self.assertEqual(body["type"], "error")
                self.assertEqual(body["error"]["type"], "api_error")
                self.assertTrue(body["error"]["message"])
                self.assertTrue(body["request_id"])
                self.assertNotIn("stop_reason", body)

    async def test_valid_empty_length_refusal_and_meaningful_stops_stay_successful(self):
        tool = {"type": "function", "id": "tool1", "function": {"name": "echo", "arguments": "{}"}}
        for message, reason, expected in (
            ({"content": None}, "length", "max_tokens"), ({"content": ""}, "content_filter", "refusal"),
            ({"content": "hi"}, "stop", "end_turn"), ({"content": None, "reasoning_content": "thought"}, "stop", "end_turn"),
            ({"content": None, "tool_calls": [tool]}, "tool_calls", "tool_use"),
        ):
            with self.subTest(reason=reason, expected=expected):
                result = await self.send(JSONResponse(choice(message, reason)))
                self.assertEqual(result.status_code, 200)
                self.assertEqual(result.json()["stop_reason"], expected)

    async def _stream_with_aborted_reader(self, n, *, commit=False):
        closed = asyncio.Event()

        async def records():
            try:
                for i in range(n):
                    chunk = {"choices": [{"delta": {"content": f"s{i}"}}]}
                    yield f"data: {json.dumps(chunk)}\n\n"
                if commit:
                    yield 'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n'
                    yield "data: [DONE]\n\n"
                # Only the upstream task dies. The HTTP client remains connected.
                asyncio.current_task().cancel()
                await asyncio.sleep(0)
            finally:
                closed.set()

        # A dead reader should wake delivery immediately, not on the next 60s ping.
        with mock.patch.object(claude_api, "IDLE_PING_SECONDS", 60):
            reply = await asyncio.wait_for(self.send(StreamingResponse(records()), stream=True), 1)
        self.assertEqual(reply.status_code, 200)  # SSE headers were already committed.
        self.assertTrue(closed.is_set())
        return [json.loads(line[6:]) for line in reply.text.splitlines() if line.startswith("data: ")]

    async def test_aborted_reader_drains_queued_frames_before_one_error_and_eof(self):
        for n in (0, 140):  # exercise both a waiting consumer and a full read-ahead queue
            with self.subTest(n=n):
                events = await self._stream_with_aborted_reader(n)
                self.assertEqual([e["delta"]["text"] for e in events if e["type"] == "content_block_delta"],
                                 [f"s{i}" for i in range(n)])
                self.assertEqual([e["type"] for e in events].count("error"), 1)
                self.assertEqual(events[-1]["type"], "error")
                self.assertEqual(events[-1]["error"]["type"], "api_error")
                self.assertNotIn("message_stop", [e["type"] for e in events])
                self.assertNotIn("ping", [e["type"] for e in events])

    async def test_queued_done_wins_over_later_reader_cancellation(self):
        events = await self._stream_with_aborted_reader(3, commit=True)
        self.assertEqual(events[-1]["type"], "message_stop")
        self.assertNotIn("error", [e["type"] for e in events])
        self.assertEqual([e["delta"]["stop_reason"] for e in events if e["type"] == "message_delta"], ["end_turn"])

    async def test_downstream_reader_cancellation_still_propagates_and_closes_source(self):
        entered, closed = asyncio.Event(), asyncio.Event()

        async def idle_source():
            try:
                entered.set()
                await asyncio.Event().wait()
                yield b""
            finally:
                closed.set()

        reply = claude_api._claude_stream_response(StreamingResponse(idle_source()), "model", omit_thinking=False)
        self.assertIn("event: message_start", await anext(reply.body_iterator))
        consumer = asyncio.create_task(anext(reply.body_iterator))
        await asyncio.wait_for(entered.wait(), 1)
        consumer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(consumer, 1)
        self.assertTrue(closed.is_set())
        await reply.aclose()
