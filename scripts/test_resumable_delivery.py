"""Offline regressions for reconnects, delivery acknowledgments and eviction races."""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-resume-delivery-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.responses import JSONResponse, StreamingResponse

from models import OpenAIRequest
import resumable
from routes import chat_api


def request(headers=None, query=None):
    return SimpleNamespace(headers=headers or {}, query_params=query or {})


async def send_response(response, fail=False):
    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(message):
        if fail and message["type"] == "http.response.body":
            raise OSError("connection lost")

    await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        patch = mock.patch.object(chat_api, "resumable_store", self.store)
        patch.start()
        self.addCleanup(patch.stop)
        self.obj = OpenAIRequest(model="gemini-test", messages=[], stream=True)
        self.key = "test-key"
        self.stream_id = resumable.compute_stream_id(
            {}, self.key, chat_api._request_fingerprint(self.obj))[0]

    async def completed_stream(self):
        buf, _ = await self.store.create_if_absent(self.stream_id)
        for index, text in enumerate(("first", "second")):
            await buf.append(resumable.frame_with_id(index, f'data: {{"text":"{text}"}}\n\n'))
        await buf.append(resumable.frame_with_id(2, resumable.DONE_FRAME))
        await buf.finish()
        buf.mark_delivered()
        return buf

    async def test_explicit_reconnect_preserves_completed_generation(self):
        buf = await self.completed_stream()
        for req in (request({"last-event-id": "0"}), request(query={"resume_from": "1"})):
            with self.subTest(query=req.query_params, headers=req.headers), \
                 mock.patch.object(chat_api, "_call_upstream", side_effect=AssertionError("unexpected generation")) as upstream:
                response = await chat_api._resumable_stream(req, self.obj, self.key)
                upstream.assert_not_called()
                frames = "".join([frame async for frame in response.body_iterator])
                self.assertNotIn('"first"', frames)
                self.assertIn('"second"', frames)
                self.assertIn("id: 2\ndata: [DONE]", frames)
                self.assertEqual(response.headers["x-resumable-replay"], "1")
                self.assertIs(await self.store.get(self.stream_id), buf)

    async def test_missing_resume_buffer_does_not_generate_and_skip_prefix(self):
        with mock.patch.object(chat_api, "_call_upstream", side_effect=AssertionError("unexpected generation")) as upstream:
            response = await chat_api._resumable_stream(
                request({"last-event-id": "10"}), self.obj, self.key)
        upstream.assert_not_called()
        self.assertEqual(response.status_code, 404)
        self.assertIsNone(await self.store.get(self.stream_id))

    async def test_future_cursor_is_rejected_without_marking_delivered(self):
        buf, _ = await self.store.create_if_absent(self.stream_id)
        with mock.patch.object(chat_api, "_call_upstream") as upstream:
            response = await chat_api._resumable_stream(
                request(query={"resume_from": "100"}), self.obj, self.key)
        upstream.assert_not_called()
        self.assertEqual(response.status_code, 409)
        self.assertFalse(buf.fully_delivered)

    async def test_json_poll_returns_body_and_status_before_acknowledging(self):
        buf, _ = await self.store.create_if_absent("json-poll", kind="json")
        payload = {"error": {"code": 429, "message": "quota"}}
        await buf.finish("error", payload=payload, status_code=429)
        response = await chat_api.resumable_poll("json-poll", request(), self.key, 0, 200, 0)
        body = json.loads(response.body)
        self.assertEqual(body["payload"], payload)
        self.assertEqual(body["status_code"], 429)
        self.assertTrue(body["done"])
        self.assertFalse(buf.fully_delivered)
        await send_response(response)
        self.assertTrue(buf.fully_delivered)

    async def test_failed_poll_delivery_preserves_replay(self):
        buf = await self.completed_stream()
        buf.fully_delivered = False
        response = await chat_api.resumable_poll(self.stream_id, request(), self.key, 0, 200, 0)
        with self.assertRaises(OSError):
            await send_response(response, fail=True)
        self.assertFalse(buf.fully_delivered)

    async def test_json_response_is_acknowledged_after_asgi_send(self):
        self.obj.stream = False
        stream_id = resumable.compute_stream_id(
            {}, self.key, chat_api._request_fingerprint(self.obj))[0]
        with mock.patch.object(chat_api, "_call_upstream", return_value=JSONResponse({"answer": "ok"})) as upstream:
            initial = await chat_api._resumable_json(request(), self.obj, self.key)
            buf = await self.store.get(stream_id)
            self.assertFalse(buf.fully_delivered)
            with self.assertRaises(OSError):
                await send_response(initial, fail=True)
            replay = await chat_api._resumable_json(request(), self.obj, self.key)
            self.assertEqual(upstream.await_count, 1)
            self.assertEqual(json.loads(replay.body), {"answer": "ok"})
            self.assertFalse(buf.fully_delivered)
            await send_response(replay)
            self.assertTrue(buf.fully_delivered)
            fresh = await chat_api._resumable_json(request(), self.obj, self.key)
            self.assertEqual(upstream.await_count, 2)
            await send_response(fresh)

    async def test_disconnect_during_setup_keeps_one_upstream_generation(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def setup(*args):
            started.set()
            await release.wait()

            async def frames():
                yield 'data: {"text":"complete"}\n\n'
                yield resumable.DONE_FRAME

            return StreamingResponse(frames())

        with mock.patch.object(chat_api, "_call_upstream", side_effect=setup) as upstream:
            first = asyncio.create_task(chat_api._resumable_stream(request(), self.obj, self.key))
            await asyncio.wait_for(started.wait(), 1)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            follower_task = asyncio.create_task(chat_api._resumable_stream(request(), self.obj, self.key))
            release.set()
            follower = await asyncio.wait_for(follower_task, 1)

            async def collect():
                return "".join([frame async for frame in follower.body_iterator])

            frames = await asyncio.wait_for(collect(), 1)
            self.assertIn('"complete"', frames)
            self.assertIn("data: [DONE]", frames)
            self.assertEqual(upstream.await_count, 1)

    async def test_explicit_resume_at_end_returns_without_new_generation(self):
        buf = await self.completed_stream()
        with mock.patch.object(chat_api, "_call_upstream") as upstream:
            response = await chat_api._resumable_stream(
                request({"last-event-id": str(buf.next_seq - 1)}), self.obj, self.key)
            frames = [frame async for frame in response.body_iterator]
        upstream.assert_not_called()
        self.assertEqual(frames, [resumable.HEARTBEAT_FRAME])

    async def test_setup_failure_preserves_error_code_for_existing_followers(self):
        started, release = asyncio.Event(), asyncio.Event()
        error = Exception("quota exhausted")
        error.code = 429

        async def setup(*args):
            started.set()
            await release.wait()
            raise error

        with mock.patch.object(chat_api, "_call_upstream", side_effect=setup) as upstream:
            first = asyncio.create_task(chat_api._resumable_stream(request(), self.obj, self.key))
            await asyncio.wait_for(started.wait(), 1)
            buf = await self.store.get(self.stream_id)
            follower = await chat_api._resumable_stream(request(), self.obj, self.key)
            release.set()
            with self.assertRaisesRegex(Exception, "quota exhausted"):
                await asyncio.wait_for(first, 1)
            frames = "".join([frame async for frame in follower.body_iterator])
        self.assertEqual(upstream.await_count, 1)
        self.assertIn('"code": 429', frames)
        self.assertIn("data: [DONE]", frames)
        self.assertEqual(buf.status_code, 429)
        self.assertIsNone(await self.store.get(self.stream_id))

    async def test_setup_http_error_is_returned_unchanged(self):
        original = JSONResponse({"error": {"code": 403, "message": "permission denied"}}, status_code=403)
        with mock.patch.object(chat_api, "_call_upstream", return_value=original):
            response = await chat_api._resumable_stream(request(), self.obj, self.key)
        self.assertIs(response, original)
        self.assertIsNone(await self.store.get(self.stream_id))

    async def test_shutdown_stops_setup_task_and_wakes_followers(self):
        started, closed = asyncio.Event(), asyncio.Event()

        async def setup(*args):
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                closed.set()

        with mock.patch.object(chat_api, "_call_upstream", side_effect=setup):
            first = asyncio.create_task(chat_api._resumable_stream(request(), self.obj, self.key))
            await asyncio.wait_for(started.wait(), 1)
            follower = await chat_api._resumable_stream(request(), self.obj, self.key)
            await self.store.aclose()
            with self.assertRaises(asyncio.CancelledError):
                await first
            frames = "".join([frame async for frame in follower.body_iterator])
        self.assertTrue(closed.is_set())
        self.assertIn('"code": 503', frames)
        self.assertIn("data: [DONE]", frames)
        self.assertFalse(self.store._pumps)

    def test_fingerprint_ignores_json_object_order_but_keeps_list_order(self):
        first = OpenAIRequest(model="gemini-test", messages=[],
                              response_format={"type": "json_object", "schema": {"b": 2, "a": 1}},
                              metadata={"tags": ["a", "b"], "name": "你好"})
        reordered = OpenAIRequest(model="gemini-test", messages=[],
                                  metadata={"name": "你好", "tags": ["a", "b"]},
                                  response_format={"schema": {"a": 1, "b": 2}, "type": "json_object"})
        self.assertEqual(chat_api._request_fingerprint(first), chat_api._request_fingerprint(reordered))
        reordered.metadata["tags"].reverse()
        self.assertNotEqual(chat_api._request_fingerprint(first), chat_api._request_fingerprint(reordered))


class BufferLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)

    async def test_evicted_json_cannot_be_overwritten_by_late_success(self):
        buf, _ = await self.store.create_if_absent("late-json", kind="json")
        started = asyncio.Event()

        async def stubborn_upstream():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await buf.finish("done", payload={"answer": "late"}, status_code=200)

        task = asyncio.create_task(stubborn_upstream())
        self.store.register_pump(buf.id, task, buffer=buf)
        await asyncio.wait_for(started.wait(), 1)
        await self.store.drop(buf.id)
        await task
        self.assertEqual(buf.status, "error")
        self.assertEqual(buf.status_code, 503)
        self.assertEqual(buf.payload["error"]["type"], "buffer_evicted")

    async def test_evicted_stream_rejects_late_frames_and_closes_producer(self):
        buf, _ = await self.store.create_if_absent("late-stream")
        started, closed = asyncio.Event(), asyncio.Event()

        async def stubborn_upstream():
            try:
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    yield 'data: {"text":"late"}\n\n'
                    await asyncio.Event().wait()
            finally:
                closed.set()

        task = asyncio.create_task(resumable.pump_stream(buf, stubborn_upstream()))
        self.store.register_pump(buf.id, task, buffer=buf)
        await asyncio.wait_for(started.wait(), 1)
        await self.store.drop(buf.id)
        try:
            await asyncio.wait_for(closed.wait(), 0.2)
            await asyncio.wait_for(task, 1)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertNotIn('"late"', "".join(buf.events))
        self.assertEqual(len(buf.events), 2)
        self.assertEqual(buf.status_code, 503)

    async def test_finished_response_is_immutable(self):
        buf = resumable.ResumableBuffer("finished")
        await buf.append(resumable.DONE_FRAME)
        await buf.finish(payload={"answer": "original"}, status_code=200)
        finished_at = buf.finished_at
        await buf.append('data: {"late":true}\n\n')
        await buf.finish("error", error="late", payload={"error": "late"}, status_code=500)
        self.assertEqual(buf.events, [resumable.DONE_FRAME])
        self.assertEqual(buf.payload, {"answer": "original"})
        self.assertEqual(buf.status, "done")
        self.assertEqual(buf.status_code, 200)
        self.assertEqual(buf.finished_at, finished_at)

    async def test_replacing_buffer_closes_old_generation_and_wakes_readers(self):
        old = await self.store.create("replace")
        task = asyncio.create_task(asyncio.Event().wait())
        self.store.register_pump(old.id, task, buffer=old)
        new = await self.store.create(old.id)
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        self.assertTrue(old.done)
        self.assertEqual(old.status_code, 503)
        self.assertIs(await self.store.get(old.id), new)

    async def test_pump_does_not_read_finished_buffer(self):
        buf = resumable.ResumableBuffer("finished-before-pump")
        await buf.finish("error", status_code=503)
        source = SimpleNamespace(__aiter__=mock.Mock(), aclose=mock.AsyncMock())
        await resumable.pump_stream(buf, source)
        source.__aiter__.assert_not_called()
        source.aclose.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
