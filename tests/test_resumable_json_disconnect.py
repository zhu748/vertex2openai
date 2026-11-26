"""A disconnected JSON reader neither cancels nor acknowledges the shared result."""
import asyncio
import json
import threading
import unittest
from unittest.mock import patch

import json_encoding
from fastapi.responses import JSONResponse
from main import app
from starlette.background import BackgroundTask
from resumable import ResumableStore
from routes import chat_api


class Client:
    """ASGI transport which silently discards sends after the peer is gone."""
    def __init__(self, *, disconnect_at=None):
        path = "/v1/chat/completions"
        self.scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                      "http_version": "1.1", "method": "POST", "scheme": "http", "path": path,
                      "raw_path": path.encode(), "query_string": b"", "server": ("test", 80),
                      "client": ("test", 1), "root_path": "", "headers": [
                          (b"content-type", b"application/json"), (b"authorization", b"Bearer test-key")]}
        self.gone = asyncio.Event()
        self.disconnect_yielded = asyncio.Event()
        self.reads = 0
        self.messages, self.discarded = [], []
        self.disconnect_at = disconnect_at

    async def receive(self):
        self.reads += 1
        if self.reads == 1:
            return {"type": "http.request", "body": b'{"model":"gemini-test","messages":[],"stream":false}'}
        await self.gone.wait()
        self.disconnect_yielded.set()
        return {"type": "http.disconnect"}

    async def disconnect(self):
        self.gone.set()
        await asyncio.wait_for(self.scope["state"]["client_connection"].event.wait(), 1)

    async def send(self, message):
        if self.disconnect_at == "headers" and message["type"] == "http.response.start":
            await self.disconnect()  # response was rendered; final body not yet sent
        final_body = message["type"] == "http.response.body" and not message.get("more_body")
        if final_body and self.disconnect_at == "during_body":
            # Keep final send parked until the watcher's receive returns the
            # disconnect; send then returns normally but drops the body.
            self.gone.set()
            await self.disconnect_yielded.wait()
        (self.discarded if self.gone.is_set() else self.messages).append(message)
        if final_body and self.disconnect_at == "after_body":
            self.gone.set()  # No yield: the final send returns before the watcher runs.

    async def run(self):
        await app(self.scope, self.receive, self.send)


class BufferedJSONDisconnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = ResumableStore()
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.calls = 0
        self.addAsyncCleanup(self.store.aclose)

        async def upstream(*_args):
            self.calls += 1
            generation = self.calls
            self.entered.set()
            await self.release.wait()
            return JSONResponse({"generation": generation})

        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(chat_api, "_call_upstream", upstream))

    def buffer(self):
        buf, = self.store._buffers.values()
        return buf

    def assert_received(self, client, receipt, *, replay, generation=1):
        start, body = client.messages
        self.assertEqual(start["status"], 200)
        headers = dict(start["headers"])
        self.assertEqual(headers[b"x-resume-id"].decode(), receipt)
        self.assertEqual(headers.get(b"x-resumable-replay"), b"1" if replay else None)
        self.assertEqual(json.loads(body["body"]), {"generation": generation})

    async def test_noop_send_to_disconnected_request_does_not_force_fresh_generation_on_retry(self):
        first = Client()
        task = asyncio.create_task(first.run())
        try:
            await asyncio.wait_for(self.entered.wait(), 1)
            buf = self.buffer()
            await first.disconnect()
        finally:
            self.release.set()
            await asyncio.wait_for(task, 1)
        await buf.wait_for(0, 1)
        acknowledged_before_retry = buf.fully_delivered

        retry = Client()
        await asyncio.wait_for(retry.run(), 1)
        self.assertEqual((acknowledged_before_retry, self.calls), (False, 1))
        self.assertEqual(first.messages, [])
        self.assertEqual(first.reads, 2, "disconnect is consumed only by the middleware")
        self.assert_received(retry, buf.id, replay=True)
        self.assertTrue(buf.fully_delivered)
        # An ordinary repeat *after* real delivery should still regenerate.
        fresh = Client()
        await asyncio.wait_for(fresh.run(), 1)
        fresh_id = dict(fresh.messages[0]["headers"])[b"x-resume-id"].decode()
        self.assertNotEqual(fresh_id, buf.id)
        self.assert_received(fresh, fresh_id, replay=False, generation=2)

    async def check_waiter_disconnect(self, *, drop_replay):
        replay_attached = asyncio.Event()
        resolve = chat_api.resolve_buffer

        async def observed_resolve(*args, **kwargs):
            result = await resolve(*args, **kwargs)
            if not result[1]:
                replay_attached.set()
            return result

        with patch.object(chat_api, "resolve_buffer", observed_resolve):
            first, replay = Client(), Client()
            first_task = asyncio.create_task(first.run())
            replay_task = None
            try:
                await asyncio.wait_for(self.entered.wait(), 1)
                buf = self.buffer()
                replay_task = asyncio.create_task(replay.run())
                await asyncio.wait_for(replay_attached.wait(), 1)
                lost, lost_task, live = ((replay, replay_task, first) if drop_replay
                                         else (first, first_task, replay))
                await lost.disconnect()
                # Neither the shared upstream nor the other HTTP request is released.
                await asyncio.wait_for(asyncio.shield(lost_task), 1)
                self.assertFalse(buf.done)
                self.assertEqual(len(buf.clients), 1)
                self.assertFalse(buf.fully_delivered)
                self.assertEqual(lost.reads, 2)
                self.assertEqual(lost.discarded[0]["status"], 499)
                headers = dict(lost.discarded[0]["headers"])
                self.assertEqual(headers[b"x-resume-id"].decode(), buf.id)
                self.assertEqual(headers.get(b"x-resumable-replay"), b"1" if drop_replay else None)
            finally:
                self.release.set()
                await asyncio.gather(first_task, *([replay_task] if replay_task else []))
            self.assertEqual(self.calls, 1)
            self.assert_received(live, buf.id, replay=not drop_replay)
            self.assertTrue(buf.fully_delivered)

    async def test_initial_waiter_can_leave_without_stopping_live_replay(self):
        await self.check_waiter_disconnect(drop_replay=False)

    async def test_replay_waiter_can_leave_without_stopping_live_initial_request(self):
        await self.check_waiter_disconnect(drop_replay=True)

    async def test_disconnect_after_response_preparation_does_not_ack_noop_body_send(self):
        # Reaches the background task using a successful ASGI send return even
        # though the sole receiver has observed the disconnect before the body.
        self.release.set()
        for _ in range(2):  # initial delivery and an already-complete replay
            lost = Client(disconnect_at="headers")
            await asyncio.wait_for(lost.run(), 1)
            self.assertEqual(lost.discarded[0]["status"], 200)
            self.assertFalse(self.buffer().fully_delivered)
        retry = Client()
        await asyncio.wait_for(retry.run(), 1)
        self.assertEqual(self.calls, 1)
        self.assert_received(retry, self.buffer().id, replay=True)

    async def test_disconnect_during_final_send_keeps_json_replayable(self):
        self.release.set()
        for _ in range(2):  # initial delivery and an already-complete replay
            lost = Client(disconnect_at="during_body")
            await asyncio.wait_for(lost.run(), 1)
            self.assertTrue(lost.disconnect_yielded.is_set())
            self.assertEqual(lost.reads, 2)
            self.assertEqual([message["type"] for message in lost.messages], ["http.response.start"])
            self.assertEqual([message["type"] for message in lost.discarded], ["http.response.body"])
            # Preserve existing connection/stats classification for a send
            # accepted by the server; only the receipt acknowledgment differs.
            self.assertFalse(lost.scope["state"]["client_connection"].disconnected)
        abandoned, *others = self.store._buffers.values()
        acknowledged_before_retry = abandoned.fully_delivered
        retry = Client()
        await asyncio.wait_for(retry.run(), 1)
        self.assertEqual((acknowledged_before_retry, self.calls, len(others)), (False, 1, 0))
        self.assert_received(retry, abandoned.id, replay=True)
        self.assertTrue(abandoned.fully_delivered)

    async def test_post_send_disconnect_still_acknowledges_successful_delivery(self):
        delivered = Client(disconnect_at="after_body")

        class BackgroundAfterObservation(BackgroundTask):
            async def __call__(self):
                # Ensure the watcher sees normal transport closure *before*
                # acknowledgment, not merely that it never got to run.
                await delivered.disconnect_yielded.wait()
                await super().__call__()

        self.release.set()
        with patch.object(chat_api, "BackgroundTask", BackgroundAfterObservation):
            await asyncio.wait_for(delivered.run(), 1)
        buf = self.buffer()
        self.assert_received(delivered, buf.id, replay=False)
        self.assertTrue(buf.fully_delivered)
        self.assertFalse(delivered.scope["state"]["client_connection"].disconnected)

    async def test_disconnect_during_offloop_render_stops_only_the_readers_encoder(self):
        loop = asyncio.get_running_loop()
        entered, stopped = asyncio.Event(), asyncio.Event()
        resume_worker, finished_encoding = threading.Event(), threading.Event()
        chunks = json_encoding.iter_json_bytes

        def gated_chunks(content, **options):
            if not options.get("compact"):
                yield from chunks(content, **options)  # buffer accounting
                return
            try:
                loop.call_soon_threadsafe(entered.set)
                if not resume_worker.wait(3):
                    raise AssertionError("test did not release worker")
                yield from chunks(content, **options)
                finished_encoding.set()
            finally:
                loop.call_soon_threadsafe(stopped.set)

        self.release.set()
        lost = Client()
        with (patch.object(json_encoding, "LARGE_RESPONSE_CHARS", 0),
              patch.object(json_encoding, "iter_json_bytes", gated_chunks)):
            reader = asyncio.create_task(lost.run())
            try:
                await asyncio.wait_for(entered.wait(), 1)
                buf = self.buffer()
                await lost.disconnect()
                await asyncio.wait_for(asyncio.shield(reader), 1)
            finally:
                resume_worker.set()
                await asyncio.wait_for(stopped.wait(), 1)
                await asyncio.wait_for(reader, 1)
        self.assertFalse(finished_encoding.is_set())
        self.assertFalse(buf.fully_delivered)
        retry = Client()
        await asyncio.wait_for(retry.run(), 1)
        self.assert_received(retry, buf.id, replay=True)
        self.assertEqual(self.calls, 1)
