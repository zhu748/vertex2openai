"""Buffered OpenAI delivery ends at its first DONE, not transport EOF."""
import asyncio
import json
import unittest
from unittest.mock import patch

import httpx
from fastapi.responses import StreamingResponse

from admission import generation_limiter
from main import app
import resumable
from routes import chat_api


class BufferTerminalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                        headers={"Authorization": "Bearer test-key"})
        self.addAsyncCleanup(self.client.aclose)

    async def check_terminal(self, code):
        closed = asyncio.Event()
        read_beyond_terminal = []
        async def source():
            try:
                if code:
                    yield f'data: {{"error":{{"code":{code},"message":"busy"}}}}\n\n'
                else:
                    yield 'data: {"choices":[{"index":0,"finish_reason":"stop","delta":{}}]}\n\n'
                yield "event: message\ndata: [DONE]\n\n"
                read_beyond_terminal.append(True)
                # A transport cleanup/trailer must not invalidate the result.
                await asyncio.Event().wait()
            finally:
                closed.set()

        path = "/v1/chat/completions"
        body = {"model": "gemini-test", "stream": True, "messages": [{"role": "user", "content": "hello"}]}
        baseline = generation_limiter.active
        with patch.object(chat_api, "_dispatch_upstream", return_value=StreamingResponse(
                source(), media_type="text/event-stream")) as dispatch, \
             patch.object(resumable, "request_timeout_seconds", return_value=.1):
            first = await asyncio.wait_for(self.client.post(path, json=body), 2)
            rid = first.headers["x-resume-id"]
            record = (await self.client.get("/v1/resumable/" + rid)).json()
            replay = await self.client.post(path, json=body, headers={"Last-Event-ID": f"{rid}:0"})
            await asyncio.wait_for(closed.wait(), 1)
            self.assertEqual(first.text.count("[DONE]"), 1)
            self.assertEqual(replay.text.count("[DONE]"), 1)
            # IDs are random hex and can legitimately include "504". Compare
            # the actual event data, including the absence of a second error.
            def data_events(response):
                return [json.loads(line[6:]) for line in response.text.splitlines()
                        if line.startswith("data: ") and line != "data: [DONE]"]

            self.assertEqual(data_events(first), [
                {"error": {"code": code, "message": "busy"}} if code else
                {"choices": [{"index": 0, "finish_reason": "stop", "delta": {}}]}
            ])
            self.assertEqual(data_events(replay), [])
            self.assertEqual([e["seq"] for e in record["events"]], [0, 1])
            self.assertTrue(record["done"])
            self.assertEqual(record["status"], "error" if code else "done")
            buf = await self.store.get_exact(rid)
            self.assertEqual(buf.status_code, code or 200)
            self.assertEqual(buf.owner_trace.data["outcome"], "error" if code else "succeeded")
            self.assertEqual(generation_limiter.active, baseline)
            self.assertFalse(read_beyond_terminal)
            dispatch.assert_awaited_once()

    async def test_buffered_terminal_no_late_timeout_or_false_cancellation(self):
        await self.check_terminal(0)

    async def test_buffered_terminal_does_not_replace_existing_error(self):
        await self.check_terminal(429)

    async def test_poll_settled_while_close_is_pending_and_after_cleanup_timeout(self):
        closing = asyncio.Event()
        closed = asyncio.Event()
        async def source():
            try:
                yield "data: [DONE]\n\n"
            finally:
                closing.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    closed.set()

        body = {"model": "gemini-test", "stream": True, "messages": []}
        with patch.object(chat_api, "_dispatch_upstream", return_value=StreamingResponse(
                source(), media_type="text/event-stream")), \
             patch.object(resumable, "SETUP_CLOSE_TIMEOUT_SECONDS", .08), \
             patch.object(resumable, "request_timeout_seconds", return_value=2):
            response = await asyncio.wait_for(self.client.post("/v1/chat/completions", json=body), .8)
            rid = response.headers["x-resume-id"]
            await asyncio.wait_for(closing.wait(), .5)
            async def poll():
                doc = (await self.client.get("/v1/resumable/" + rid)).json()
                self.assertTrue(doc["done"])
                self.assertEqual(doc["status"], "done")
                self.assertEqual(len(doc["events"]), 1)
                self.assertIn("[DONE]", doc["events"][0]["data"])
            await poll()
            await asyncio.wait_for(closed.wait(), .5)
            await poll()
            self.assertEqual((await self.store.get_exact(rid)).owner_trace.data["outcome"], "succeeded")
