"""Replay receipts count completed HTTP delivery, not exhaustion of a local iterator."""
import asyncio
import json
import unittest
from unittest.mock import patch

import httpx
from fastapi.responses import JSONResponse, StreamingResponse
import main
from main import app
from logger import ProxyStats
from starlette.requests import ClientDisconnect

import resumable
from routes import chat_api, gemini_native
from upstreams import native_passthrough


class Peer:
    """In-process ASGI peer: its send may legally return after a lost connection."""
    def __init__(self, path, *, method="POST", payload=None, query=b"", headers=(), spec="2.4"):
        self.scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": spec},
                      "scheme": "http", "http_version": "1.1", "server": ("test", 80),
                      "client": ("test", 1), "root_path": "", "method": method, "path": path,
                      "raw_path": path.encode(), "query_string": query, "headers": [
                          (b"authorization", b"Bearer test-key"), (b"content-type", b"application/json"), *headers]}
        self.body = json.dumps(payload).encode() if payload is not None else b""
        self.reads = 0
        self.gone, self.observed = asyncio.Event(), asyncio.Event()
        self.messages, self.discarded = [], []

    async def receive(self):
        self.reads += 1
        if self.reads == 1:
            return {"type": "http.request", "body": self.body}
        await self.gone.wait()
        self.observed.set()
        return {"type": "http.disconnect"}

    async def drop(self):
        self.gone.set()
        await asyncio.wait_for(self.observed.wait(), 1)

    async def run(self, on_send=None):
        async def send(message):
            if on_send is not None:
                await on_send(message)
            (self.discarded if self.gone.is_set() else self.messages).append(message)
        await app(self.scope, self.receive, send)

    @property
    def receipt(self):
        return dict(self.messages[0]["headers"])[b"x-resume-id"].decode()


class ReplayHTTPDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(patch.object(app.state, "express_key_manager", None))  # offline adapter below
        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.enterContext(patch.object(native_passthrough, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(gemini_native, "resumable_enabled", return_value=True))

    @staticmethod
    def stream_peer(native=False, **kwargs):
        if native:
            return Peer("/v1beta/models/gemini-test:streamGenerateContent", query=b"alt=sse",
                        payload={"contents": [{"parts": [{"text": "hello"}]}]},
                        headers=[(b"x-resumable", b"true")], **kwargs)
        return Peer("/v1/chat/completions", payload={"model": "gemini-test", "stream": True,
                                                    "messages": []}, **kwargs)

    @staticmethod
    async def stream_upstream(*args, **kwargs):
        async def chunks():
            yield 'data: {"candidates":[{"finishReason":"STOP"}]}\n\n'
            if args and not isinstance(args[0], str) and hasattr(args[0], "messages"):
                yield resumable.DONE_FRAME
        return StreamingResponse(chunks(), media_type="text/event-stream")

    async def check_stream(self, *, native, target, failure, spec):
        peer = self.stream_peer(native, spec=spec)
        body_marker = b'"finishReason"' if native else b"[DONE]"
        invoked = False

        async def drop(message):
            nonlocal invoked
            if message["type"] != "http.response.body":
                return
            if (target == "eof" and not message.get("more_body")) or (
                    target == "event" and body_marker in message.get("body", b"")):
                invoked = True
                if failure == "raise":
                    raise OSError("closed connection")
                await peer.drop()  # allow the application's sole receiver to observe it before send returns

        with patch.object(chat_api, "_call_upstream", side_effect=self.stream_upstream) as openai, \
                patch.object(native_passthrough, "_passthrough_request", side_effect=self.stream_upstream) as gemini:
            if failure == "raise":
                with self.assertRaises(ClientDisconnect):
                    await asyncio.wait_for(peer.run(drop), 2)
            else:
                await asyncio.wait_for(peer.run(drop), 2)
            self.assertTrue(invoked)
            buf = await self.store.get_exact(peer.receipt)
            self.assertFalse(buf.fully_delivered)
            self.assertEqual(buf.readers, 0)
            self.assertFalse(buf.clients)
            retry = self.stream_peer(native, spec=spec)
            await asyncio.wait_for(retry.run(), 2)
            self.assertEqual(peer.receipt, retry.receipt)
            self.assertEqual(dict(retry.messages[0]["headers"])[b"x-resumable-replay"], b"1")
            self.assertIn(body_marker, b"".join(m.get("body", b"") for m in retry.messages))
            self.assertTrue(buf.fully_delivered)
            self.assertEqual((gemini if native else openai).await_count, 1)

    async def test_lost_terminal_or_eof_preserves_native_and_openai_replays(self):
        for native in (False, True):
            for target in ("event", "eof"):
                for failure, spec in (("noop", "2.4"), ("raise", "2.4"), ("noop", "2.3")):
                    with self.subTest(native=native, target=target, failure=failure, spec=spec):
                        await self.store.aclose()
                        await self.check_stream(native=native, target=target, failure=failure, spec=spec)

    async def test_quiet_disconnected_stream_releases_only_its_reader(self):
        started, release, follower_ready = (asyncio.Event() for _ in range(3))

        async def upstream(*_):
            async def frames():
                started.set()
                await release.wait()
                yield resumable.DONE_FRAME
            return StreamingResponse(frames())

        async def ready(message):
            if message["type"] == "http.response.body":
                follower_ready.set()

        first, follower = self.stream_peer(), self.stream_peer()
        with patch.object(chat_api, "_call_upstream", side_effect=upstream) as call:
            lost = asyncio.create_task(first.run())
            live = None
            try:
                await asyncio.wait_for(started.wait(), 1)
                live = asyncio.create_task(follower.run(ready))
                await asyncio.wait_for(follower_ready.wait(), 1)
                await first.drop()
                await asyncio.wait_for(asyncio.shield(lost), 1)
                buf = await self.store.get_exact(follower.receipt)
                self.assertFalse(buf.done)
                self.assertFalse(buf.fully_delivered)
                self.assertEqual(len(buf.clients), 1)
                self.assertLessEqual(buf.readers, 1)
            finally:
                release.set()
                await asyncio.gather(lost, *([live] if live is not None else []))
            self.assertTrue(buf.fully_delivered)
            self.assertEqual(call.await_count, 1)

    @staticmethod
    def poll_peer(buf, **kwargs):
        return Peer("/v1/resumable/" + buf.id, method="GET", **kwargs)

    async def test_json_receipt_advisory_is_header_only_validated_and_scoped_to_the_original_http_error(self):
        for status, hint, raised, expected in ((200, "45", False, None), (429, "bad-delay", False, None),
                                              (503, "0", False, "0"), (429, "45", True, "45"),
                                              (422, "bad-delay", True, None)):
            with self.subTest(status=status, hint=hint, raised=raised):
                headers = {"Retry-After": hint, "X-Unrelated": "do not forward"}
                payload = {"answer": "ok"} if status == 200 else {"error": {"code": status, "retry_after": "999"}}
                prepared = JSONResponse(payload, status_code=status, headers=headers)
                async def upstream(*_args):
                    if raised:
                        request = httpx.Request("POST", "http://fixture.invalid/")
                        raise httpx.HTTPStatusError("fixture", request=request, response=httpx.Response(
                            status, request=request, headers=headers))
                    return prepared
                with patch.object(chat_api, "_call_upstream", side_effect=upstream) as send:
                    for _ in range(2):  # first delivery and a keyed replay
                        peer = Peer("/v1/chat/completions", payload={"model": "gemini-test", "messages": []},
                                    headers=[(b"idempotency-key", f"{status}-{hint}-{raised}".encode())])
                        await asyncio.wait_for(peer.run(), 2)
                        start = peer.messages[0]
                        received = dict(start["headers"])
                        self.assertEqual((start["status"], received.get(b"retry-after")),
                                         (status, expected.encode() if expected is not None else None))
                        self.assertNotIn(b"x-unrelated", received)
                        if not raised:
                            self.assertEqual(json.loads(peer.messages[-1]["body"]), payload)
                    send.assert_awaited_once()

    async def test_poll_ack_requires_a_live_final_send_and_never_counts_generation(self):
        stats = ProxyStats()
        with patch.object(main, "stats", stats):
            for spec in ("2.3", "2.4"):
                for failure in ("headers_noop", "final_noop", "raise", "success"):
                    with self.subTest(spec=spec, failure=failure):
                        buf, _ = await self.store.create_if_absent(f"{spec}-{failure}", kind="json")
                        await buf.finish(payload={"text": "complete"})
                        peer = self.poll_peer(buf, spec=spec)

                        async def drop(message):
                            if failure == "headers_noop" and message["type"] == "http.response.start":
                                await peer.drop()
                            if message["type"] == "http.response.body":
                                if failure == "raise":
                                    raise OSError("closed connection")
                                if failure == "final_noop":
                                    await peer.drop()

                        if failure == "raise":
                            with self.assertRaises(OSError):
                                await asyncio.wait_for(peer.run(drop), 2)
                        else:
                            await asyncio.wait_for(peer.run(drop), 2)
                        self.assertEqual(buf.fully_delivered, failure == "success")
                        self.assertLessEqual(peer.reads, 2)
                        retry = self.poll_peer(buf, spec=spec)
                        await retry.run()
                        self.assertEqual(json.loads(retry.messages[-1]["body"])["payload"], {"text": "complete"})
                        self.assertTrue(buf.fully_delivered)
        self.assertEqual(stats.total_requests, 0)
        self.assertEqual(stats.error_requests, 0)

    async def test_poll_at_openai_end_reports_done_but_must_deliver_the_terminal_to_consume(self):
        with patch.object(chat_api, "_call_upstream", side_effect=self.stream_upstream) as call:
            original = self.stream_peer()
            async def lose_final(message):
                if message["type"] == "http.response.body" and not message.get("more_body"):
                    await original.drop()
            await original.run(lose_final)
            buf = await self.store.get_exact(original.receipt)
            self.assertFalse(buf.fully_delivered)
            for start, expected_ack in ((buf.next_seq, False), (buf.next_seq - 1, True)):
                poll = self.poll_peer(buf, query=f"from={start}".encode())
                await poll.run()
                content = json.loads(poll.messages[-1]["body"])
                self.assertEqual((content["done"], content["next"]), (True, buf.next_seq))
                self.assertEqual(bool(content["events"]), expected_ack)
                self.assertEqual(buf.fully_delivered, expected_ack)
                if not expected_ack:
                    # The ordinary retry must still use the same undelivered
                    # generation; abandon this retry too, before its final send.
                    retry = self.stream_peer()
                    async def lose_again(message):
                        if message["type"] == "http.response.body" and not message.get("more_body"):
                            await retry.drop()
                    await retry.run(lose_again)
                    self.assertEqual(retry.receipt, original.receipt)
                    self.assertFalse(buf.fully_delivered)
            fresh = self.stream_peer()
            await fresh.run()
            self.assertNotEqual(fresh.receipt, original.receipt)
            self.assertEqual(call.await_count, 2)

    async def test_abandoned_poll_cancels_its_wait_not_the_shared_pump_or_follower(self):
        buf, _ = await self.store.create_if_absent("long-poll", kind="json")
        release, two_readers, cancelled = (asyncio.Event() for _ in range(3))

        async def pump():
            await release.wait()
            await buf.finish(payload={"text": "complete"})

        job = asyncio.create_task(pump())
        self.store.register_pump(buf.id, job, buffer=buf)
        reads = 0
        wait = buf.wait_for

        async def waiting(*args):
            nonlocal reads
            reads += 1
            if reads == 2:
                two_readers.set()
            try:
                await wait(*args)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        first, follower = (self.poll_peer(buf, query=b"wait=25") for _ in range(2))
        with patch.object(buf, "wait_for", waiting):
            lost, live = asyncio.create_task(first.run()), asyncio.create_task(follower.run())
            try:
                await asyncio.wait_for(two_readers.wait(), 1)
                await first.drop()
                await asyncio.wait_for(asyncio.shield(lost), 1)
                self.assertTrue(cancelled.is_set())
                self.assertEqual(first.reads, 2)
                self.assertEqual(first.discarded[0]["status"], 499)
                self.assertFalse(buf.fully_delivered)
                self.assertFalse(job.done() or live.done())
            finally:
                release.set()
                await asyncio.gather(lost, live, job)
        self.assertTrue(buf.fully_delivered)
        self.assertTrue(json.loads(follower.messages[-1]["body"])["done"])

    async def test_abandoned_poll_cancels_its_renderer_before_returning(self):
        buf, _ = await self.store.create_if_absent("encoding", kind="json")
        await buf.finish(payload={"text": "complete"})
        entered, released, stopped = (asyncio.Event() for _ in range(3))
        render = chat_api.json_response_offloop

        async def slow(**kwargs):
            try:
                entered.set()
                await released.wait()
                return await render(**kwargs)
            finally:
                stopped.set()

        peer = self.poll_peer(buf)
        with patch.object(chat_api, "json_response_offloop", slow):
            task = asyncio.create_task(peer.run())
            try:
                await asyncio.wait_for(entered.wait(), 1)
                await peer.drop()
                await asyncio.wait_for(asyncio.shield(task), 1)
                self.assertTrue(stopped.is_set())
                self.assertFalse(buf.fully_delivered)
            finally:
                released.set()
                await task

    async def test_capacity_and_ttl_preserve_every_live_delivery_before_the_first_body(self):
        async def json_upstream(*_):
            return JSONResponse({"text": "complete"})

        for route in ("json", "openai", "native", "poll"):
            with self.subTest(route=route):
                await self.store.aclose()
                if route == "poll":
                    buf, _ = await self.store.create_if_absent("pin-poll", kind="json")
                    await buf.finish(payload={"text": "complete"})
                    peer = self.poll_peer(buf)
                elif route == "json":
                    peer = Peer("/v1/chat/completions", payload={"model": "gemini-test", "messages": []})
                else:
                    peer = self.stream_peer(native=route == "native")
                headers, release = asyncio.Event(), asyncio.Event()

                async def parked(message):
                    if message["type"] == "http.response.start":
                        headers.set()
                        await release.wait()

                with patch.object(resumable, "max_streams", return_value=1), \
                        patch.object(chat_api, "_call_upstream", side_effect=(
                            self.stream_upstream if route == "openai" else json_upstream)), \
                        patch.object(native_passthrough, "_passthrough_request", side_effect=self.stream_upstream):
                    task = asyncio.create_task(peer.run(parked))
                    try:
                        await asyncio.wait_for(headers.wait(), 1)
                        buf, = self.store._buffers.values()
                        await buf.wait_for(1000, 1)
                        if close := self.store._draining(buf):
                            await asyncio.wait_for(asyncio.shield(close), 1)
                        self.assertTrue(buf.done)
                        self.assertEqual(buf.readers, 0)
                        buf.finished_monotonic -= 86400
                        self.assertEqual(await self.store.sweep(), 0)
                        other = Peer("/v1/chat/completions", payload={"model": "other-model", "messages": []})
                        await other.run()
                        self.assertEqual(other.messages[0]["status"], 503)
                        self.assertIs(await self.store.get_exact(buf.id), buf)
                        peer.gone.set()
                        await asyncio.wait_for(peer.observed.wait(), 1)
                    finally:
                        release.set()
                        await task
                    self.assertFalse(buf.fully_delivered)
                    retry = self.poll_peer(buf)
                    await retry.run()
                    self.assertEqual(retry.messages[0]["status"], 200)
                    self.assertTrue(buf.fully_delivered)
                    self.assertEqual(await self.store.sweep(), 1)

    async def test_rejected_polls_do_not_start_receiving_touch_or_pin(self):
        import hashlib
        buf, _ = await self.store.create_if_absent("guard", kind="json")
        buf.owner_key_digest = hashlib.sha256(b"previous-owner").hexdigest()
        last_access = buf.last_access
        for condition, status in (("foreign", 404), ("expired", 410)):
            with self.subTest(condition=condition):
                if condition == "expired":
                    buf.owner_key_digest = hashlib.sha256(b"test-key").hexdigest()
                    buf._retired_by = "grace"
                peer = self.poll_peer(buf)
                await peer.run()
                self.assertEqual(peer.messages[0]["status"], status)
                self.assertEqual(peer.reads, 0)
                self.assertEqual(buf.last_access, last_access)
                self.assertFalse(buf.has_live_delivery)

    async def test_cancelled_long_poll_does_not_prepay_an_unused_wait_window(self):
        import time
        from request_connection import ClientConnection
        buf, _ = await self.store.create_if_absent("sliding-grace", kind="json")
        origin = ClientConnection()
        buf.attach_client(origin)
        origin.finish(True)
        entered = asyncio.Event()
        wait = buf.wait_for

        async def waiting(*args):
            entered.set()
            await wait(*args)

        peer = self.poll_peer(buf, query=b"wait=25")
        with patch.object(buf, "wait_for", waiting):
            task = asyncio.create_task(peer.run())
            await asyncio.wait_for(entered.wait(), 1)
            await peer.drop()
            await asyncio.wait_for(task, 1)
        self.assertFalse(buf.has_live_delivery or buf.done or buf.fully_delivered)
        self.assertLessEqual(buf.last_disconnect_monotonic, time.monotonic())
        self.assertIsNotNone(buf._grace_task)

    async def lose_final_send(self, *, native=False):
        peer = self.stream_peer(native=native)
        async def drop_final(message):
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                await peer.drop()
        await peer.run(drop_final)
        buf = await self.store.get_exact(peer.receipt)
        self.assertFalse(buf.fully_delivered)
        self.assertIsNotNone(buf.last_disconnect_monotonic)
        return buf

    def cursor_peer(self, buf, *, at_end, native=False):
        peer = self.stream_peer(native=native)
        header = ((b"last-event-id", f"{buf.id}:{buf.next_seq - 1}".encode()) if at_end else
                  (b"x-resume-id", buf.id.encode()))
        peer.scope["headers"].append(header)
        return peer

    async def test_cargoless_terminal_cursor_cannot_erase_or_restart_lost_send_window(self):
        with patch.object(chat_api, "_call_upstream", side_effect=self.stream_upstream) as upstream:
            buf = await self.lose_final_send()
            old = (buf.last_disconnect_monotonic, buf.last_disconnected_at, buf.last_disconnect_grace_seconds)
            for _ in range(2):
                tail = self.cursor_peer(buf, at_end=True)
                await tail.run()
                self.assertEqual(tail.messages[0]["status"], 200)
                self.assertNotIn(b"data:", b"".join(m.get("body", b"") for m in tail.messages))
                self.assertFalse(buf.fully_delivered or buf.has_live_delivery)
                self.assertEqual((buf.last_disconnect_monotonic, buf.last_disconnected_at,
                                  buf.last_disconnect_grace_seconds), old)
            buf.last_disconnect_monotonic -= buf.last_disconnect_grace_seconds + 1
            full = self.cursor_peer(buf, at_end=False)
            await full.run()
            self.assertEqual(full.messages[0]["status"], 410)
            self.assertEqual(upstream.call_count, 1)

        # Native EOF itself signifies a complete delivery; it is not an empty
        # OpenAI cursor that still requires a connected [DONE] frame.
        with patch.object(native_passthrough, "_passthrough_request", side_effect=self.stream_upstream) as native:
            buf = await self.lose_final_send(native=True)
            tail = self.cursor_peer(buf, at_end=True, native=True)
            await tail.run()
            self.assertTrue(buf.fully_delivered)
            self.assertEqual(native.call_count, 1)

    async def test_parked_empty_cursor_does_not_mask_real_reader_disconnect_or_expired_window(self):
        with patch.object(chat_api, "_call_upstream", side_effect=self.stream_upstream) as upstream:
            buf = await self.lose_final_send()
            tail = self.cursor_peer(buf, at_end=True)
            parked, may_send = asyncio.Event(), asyncio.Event()
            async def park(message):
                if message["type"] == "http.response.start":
                    parked.set()
                    await may_send.wait()
            observer = asyncio.create_task(tail.run(park))
            try:
                await asyncio.wait_for(parked.wait(), 1)
                self.assertTrue(buf.has_live_delivery)
                # A genuine replay while the observer is parked really loses its
                # terminal write. That, not the observer's eventual clean EOF,
                # establishes the actual last delivery attempt's grace window.
                full = self.cursor_peer(buf, at_end=False)
                async def lose(message):
                    if message["type"] == "http.response.body" and not message.get("more_body", False):
                        await full.drop()
                await full.run(lose)
                self.assertIsNotNone(buf.last_disconnect_monotonic)
                buf.last_disconnect_monotonic -= buf.last_disconnect_grace_seconds + 1
                late = self.cursor_peer(buf, at_end=False)
                await late.run()
                self.assertEqual(late.messages[0]["status"], 410)
                self.assertTrue(buf.has_live_delivery)
            finally:
                may_send.set()
                await asyncio.wait_for(observer, 1)
            self.assertFalse(buf.fully_delivered or buf.has_live_delivery)
            self.assertEqual(upstream.call_count, 1)
