"""A shared stream setup can finish as a real HTTP error after a follower started SSE."""
import asyncio
import json
import unittest
from unittest.mock import patch

import httpx
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.background import BackgroundTask

from resumable import DONE_FRAME, ResumableStore
import resumable
from routes import chat_api
from test_replay_http_delivery import Peer


ERROR = {"error": {"message": "wait for capacity", "type": "rate_limit_error",
                   "code": "capacity", "param": None}}


class OpenAIStreamSetupReceiptTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(chat_api, "SETUP_FAST_WINDOW_SECONDS", 5))

    @staticmethod
    def peer(headers=(), **kw):
        return Peer("/v1/chat/completions", payload={"model": "gemini-test", "stream": True, "messages": []},
                    headers=headers, **kw)

    async def sent(self, headers=(), **kw):
        peer = self.peer(headers, **kw)
        await asyncio.wait_for(peer.run(), 2)
        return peer

    @staticmethod
    def wire(peer):
        return b"".join(m.get("body", b"") for m in peer.messages)

    def assert_http(self, peer, error=ERROR, code=429, hint=b"5"):
        self.assertEqual(peer.messages[0]["status"], code)
        self.assertEqual(json.loads(self.wire(peer)), error)
        headers = dict(peer.messages[0]["headers"])
        self.assertEqual(headers.get(b"retry-after"), hint)
        self.assertNotIn(b"x-provider-internal", headers)

    def assert_sse(self, peer):
        self.assertEqual(peer.messages[0]["status"], 200)
        data = [line[6:] for line in self.wire(peer).splitlines() if line.startswith(b"data: ")]
        self.assertEqual(len(data), 2)
        self.assertEqual(json.loads(data[0]), {"error": {**ERROR["error"], "retry_after": "5"}})
        self.assertEqual(data[1], b"[DONE]")

    async def test_early_followers_explicit_idempotency_and_exact_cursor_share_one_request(self):
        fetched, respond, headers_sent = (asyncio.Event() for _ in range(3))
        closed = []
        async def upstream(*_):
            fetched.set()
            await respond.wait()
            return JSONResponse(ERROR, status_code=429, headers={"retry-after": "5", "x-provider-internal": "no"},
                                background=BackgroundTask(closed.append, "closed"))

        key = [(b"idempotency-key", b"one")]
        leader, follower = self.peer(key), self.peer(key)
        async def seen(message):
            if message["type"] == "http.response.start":
                headers_sent.set()
        with patch.object(chat_api, "_call_upstream", side_effect=upstream) as call:
            first = asyncio.create_task(leader.run())
            await asyncio.wait_for(fetched.wait(), 1)
            later = asyncio.create_task(follower.run(seen))
            try:
                await asyncio.wait_for(headers_sent.wait(), 1)
                self.assertEqual(call.await_count, 1)
            finally:
                respond.set()
                await asyncio.wait_for(asyncio.gather(first, later), 2)
            self.assert_http(leader)
            self.assert_sse(follower)
            self.assertEqual(leader.receipt, follower.receipt)
            retry = await self.sent(key)
            self.assert_http(retry)
            cursor = await self.sent([(b"x-resume-id", leader.receipt.encode())])
            self.assert_sse(cursor)
            self.assertEqual(dict(cursor.messages[0]["headers"])[b"retry-after"], b"5")
            end = await self.sent([(b"last-event-id", f"{leader.receipt}:1".encode())])
            self.assertNotIn(b"data:", self.wire(end))
            self.assertEqual(closed, ["closed"])
            call.assert_awaited_once()

    async def test_lost_slow_reader_does_not_cancel_preparation_or_lose_the_future_error(self):
        entered, release, ready = (asyncio.Event() for _ in range(3))
        async def pending(*_):
            entered.set()
            await release.wait()
            return JSONResponse(ERROR, status_code=429, headers={"retry-after": "5"})

        key = [(b"idempotency-key", b"slow")]
        lost, live = self.peer(key), self.peer(key)
        async def started(message):
            if message["type"] == "http.response.body":
                ready.set()
        with patch.object(chat_api, "SETUP_FAST_WINDOW_SECONDS", 0), \
                patch.object(chat_api, "_call_upstream", side_effect=pending) as call:
            a = asyncio.create_task(lost.run(started))
            b = None
            try:
                await asyncio.wait_for(entered.wait(), 1)
                await asyncio.wait_for(ready.wait(), 1)
                ready.clear()
                await lost.drop()
                await asyncio.wait_for(asyncio.shield(a), 1)
                buf = await self.store.get_exact(lost.receipt)
                self.assertFalse(buf.done or buf.fully_delivered)
                b = asyncio.create_task(live.run(started))
                await asyncio.wait_for(ready.wait(), 1)
            finally:
                release.set()
                await asyncio.wait_for(asyncio.gather(a, *([b] if b else [])), 2)
            self.assert_sse(live)
            self.assertTrue(buf.fully_delivered)
            self.assert_http(await self.sent(key))
            call.assert_awaited_once()

    async def test_lost_http_send_retains_error_until_delivered_ordinary_retry_then_regenerates(self):
        for mode in ("headers", "final", "raise", "cancel"):
            with self.subTest(mode=mode):
                await self.store.aclose()
                peer = self.peer()
                async def fail(message):
                    if mode == "headers" and message["type"] == "http.response.start":
                        await peer.drop()
                    if message["type"] == "http.response.body":
                        if mode == "final":
                            await peer.drop()
                        if mode == "raise":
                            raise OSError("lost final send")
                        if mode == "cancel":
                            raise asyncio.CancelledError
                async def success():
                    yield DONE_FRAME
                with patch.object(chat_api, "_call_upstream", side_effect=[
                    JSONResponse(ERROR, status_code=429, headers={"Retry-After": "5"}),
                    StreamingResponse(success())]) as call:
                    if mode in {"raise", "cancel"}:
                        with self.assertRaises(OSError if mode == "raise" else asyncio.CancelledError):
                            await peer.run(fail)
                    else:
                        await peer.run(fail)
                    old, = self.store._buffers.values()
                    self.assertFalse(old.fully_delivered or old.has_live_delivery)
                    retry = await self.sent()
                    self.assert_http(retry)
                    self.assertEqual(retry.receipt, old.id)
                    self.assertTrue(old.fully_delivered)
                    next_request = await self.sent()
                    self.assertNotEqual(next_request.receipt, old.id)
                    self.assertEqual(call.await_count, 2)
                    self.assertIn(b"data: [DONE]", self.wire(next_request))

    async def test_expired_uncertain_receipt_retains_410_and_new_ordinary_request_can_regenerate(self):
        failed = self.peer()
        async def lose(message):
            if message["type"] == "http.response.body":
                await failed.drop()
        with patch.object(resumable, "disconnect_grace_seconds", return_value=0), \
                patch.object(chat_api, "_call_upstream", return_value=JSONResponse(
                    ERROR, status_code=429, headers={"retry-after": "5"})) as call:
            await failed.run(lose)
            old, = self.store._buffers.values()
            self.assertFalse(old.fully_delivered)
            stale = await self.sent([(b"x-resume-id", old.id.encode())])
            self.assertEqual(stale.messages[0]["status"], 410)
            new = await self.sent()
            self.assert_http(new)
            self.assertNotEqual(new.receipt, old.id)
            self.assertEqual(call.await_count, 2)

    async def test_bounded_and_well_formed_diagnostics_only_no_made_up_200(self):
        key = [(b"idempotency-key", b"bounded")]
        for raw, code, hint, status in ((b"bad!", 429, "not-a-delay", 429),
                                       (b'{"error":{"message":"x","prob":NaN}}', 422, "5"*140, 422),
                                       (JSONResponse({"error": {"message": "z"*16000}}).body, 503, "5", 503),
                                       (b'{"choices":[]}', 200, "", 502)):
            with self.subTest(code=code):
                await self.store.aclose()
                with patch.object(chat_api, "_call_upstream", return_value=Response(
                        raw, status_code=code, headers={"Retry-After": hint})) as call:
                    for _ in range(2):
                        peer = await self.sent(key)
                        self.assertEqual(peer.messages[0]["status"], status)
                        self.assertEqual(dict(peer.messages[0]["headers"]).get(b"retry-after"),
                                         b"5" if hint == "5" else None)
                        self.assertEqual(json.loads(self.wire(peer))["error"]["code"], status)
                        self.assertNotIn(b"NaN", self.wire(peer))
                        self.assertLess(len(self.wire(peer)), 1024)
                    call.assert_awaited_once()

    async def test_raised_http_error_retains_valid_advisory_on_ordinary_and_keyed_delivery(self):
        req = httpx.Request("POST", "http://test/stub")
        err = httpx.HTTPStatusError("busy", request=req, response=httpx.Response(503, headers={"Retry-After": "8"}))
        key = [(b"idempotency-key", b"raised")]
        with patch.object(chat_api, "_call_upstream", side_effect=err) as call:
            for _ in range(2):
                peer = await self.sent(key)
                self.assertEqual(peer.messages[0]["status"], 503)
                self.assertEqual(dict(peer.messages[0]["headers"])[b"retry-after"], b"8")
                self.assertEqual(json.loads(self.wire(peer))["error"]["code"], 503)
            call.assert_awaited_once()
