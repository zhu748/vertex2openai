"""Offline regressions for reconnects, delivery acknowledgments and eviction races."""
import asyncio
import json
from types import SimpleNamespace
import unittest
from unittest import mock


from fastapi.responses import JSONResponse, StreamingResponse

from models import OpenAIRequest
from request_connection import ClientConnection
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
        old, _ = await self.store.create_if_absent("replace")
        task = asyncio.create_task(asyncio.Event().wait())
        self.store.register_pump(old.id, task, buffer=old)
        # create 的「无条件覆盖」语义经 drop() 显式表达：先丢弃旧代
        # （取消其泵、置 503 终态），再占位建新代。
        await self.store.drop(old.id)
        new, _ = await self.store.create_if_absent(old.id)
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


class GraceExpiryTests(unittest.IsolatedAsyncioTestCase):
    """断开超过续传窗口且无人续接：直接取消在飞上游（2026-11 语义）。

    旧行为是 resume_expired() 只在懒检查点生效（重连 410 / 同指纹替换 /
    面板 meta），断开超窗后无人再来时后台泵会把上游跑到自然结束，最长
    吃满整轮 request_timeout_seconds（默认 1800 秒）——纯浪费配额。"""

    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        patch = mock.patch.object(chat_api, "resumable_store", self.store)
        patch.start()
        self.addCleanup(patch.stop)

    async def buffer(self):
        return (await self.store.get_or_create_request(
            "grace", model="m", kind="stream", fingerprint_key="f"))[0]

    async def immortal_worker(self, buf):
        """注册一个「永远不会自己结束」的泵任务，返回 (任务, 取消标记)。"""
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def worker():
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        task = asyncio.create_task(worker())
        self.store.register_pump(buf.id, task, buffer=buf)
        await asyncio.wait_for(started.wait(), 1)
        return task, cancelled

    async def test_grace_expiry_cancels_running_upstream_and_keeps_buffer(self):
        buf = await self.buffer()
        connection = ClientConnection()
        buf.attach_client(connection)
        task, cancelled = await self.immortal_worker(buf)
        with mock.patch.object(resumable, "disconnect_grace_seconds", return_value=0.05):
            connection.finish(True)
            await asyncio.wait_for(cancelled.wait(), 1)
        self.assertTrue(task.cancelled())
        self.assertEqual(buf.status, "error")
        self.assertEqual(buf.status_code, 410)
        self.assertIn("续传窗口", buf.error)
        # 缓冲本体保留：晚到的重连仍是 410 语义（懒检查口径不变），
        # 容量回收仍归 TTL 清扫——取消只终结上游消耗。
        self.assertIs(await self.store.get_exact(buf.id), buf)

    async def test_reconnect_within_grace_disarms_timer(self):
        buf = await self.buffer()
        first = ClientConnection()
        buf.attach_client(first)
        task, cancelled = await self.immortal_worker(buf)
        with mock.patch.object(resumable, "disconnect_grace_seconds", return_value=0.12):
            first.finish(True)
            await asyncio.sleep(0.03)
            second = ClientConnection()
            buf.attach_client(second)      # 窗口内重连：撤销断开计时
        await asyncio.sleep(0.15)           # 越过原窗口
        self.assertFalse(task.done())
        self.assertFalse(cancelled.is_set())
        self.assertIsNone(buf.last_disconnect_monotonic)
        second.finish(True)                 # 再次断开才重新起表
        self.assertIsNotNone(buf.last_disconnect_monotonic)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_poll_activity_slides_grace_window(self):
        buf = await self.buffer()
        connection = ClientConnection()
        buf.attach_client(connection)
        task, cancelled = await self.immortal_worker(buf)
        with mock.patch.object(resumable, "disconnect_grace_seconds", return_value=0.15):
            connection.finish(True)          # 原窗口到 t≈0.15
            await asyncio.sleep(0.06)
            buf.note_client_activity(0.0)    # 轮询把窗口滑到 t≈0.21
            await asyncio.sleep(0.10)        # t≈0.16：原窗口已过、滑动窗口未到
        self.assertFalse(task.done())        # 轮询中的客户端不被误取消
        await asyncio.wait_for(cancelled.wait(), 1)
        self.assertTrue(task.cancelled())
        self.assertEqual(buf.status_code, 410)

    async def test_long_poll_horizon_prepays_window(self):
        buf = await self.buffer()
        connection = ClientConnection()
        buf.attach_client(connection)
        task, cancelled = await self.immortal_worker(buf)
        with mock.patch.object(resumable, "disconnect_grace_seconds", return_value=0.1):
            connection.finish(True)
            buf.note_client_activity(0.3)   # 长轮询声明在途 0.3s：取消点 ≈ 0.4s
            await asyncio.sleep(0.2)        # 越过 0.1s 原窗口，仍处声明的在途期
        self.assertFalse(task.done())
        await asyncio.wait_for(cancelled.wait(), 1)
        self.assertTrue(task.cancelled())

    async def test_completed_generation_is_not_cancelled_after_grace(self):
        buf = await self.buffer()
        connection = ClientConnection()
        buf.attach_client(connection)
        await buf.append(resumable.frame_with_id(0, 'data: {"text":"done"}\n\n'))
        await buf.finish()
        with mock.patch.object(resumable, "disconnect_grace_seconds", return_value=0.05):
            connection.finish(True)          # 完成后断开：只记标记，不起取消
            await asyncio.sleep(0.12)
        self.assertEqual(buf.status, "done")
        self.assertIs(await self.store.get_exact(buf.id), buf)
        self.assertTrue(buf.resume_expired())  # 懒检查口径不变（晚到重连仍 410）

    async def test_disconnected_json_request_cancels_upstream_after_grace(self):
        """用户原始场景：非流式请求断开超窗 → 取消挂起的上游而不是后台跑完。"""
        connection = ClientConnection()
        req = SimpleNamespace(headers={}, query_params={},
                              scope={"state": {"client_connection": connection}})
        obj = OpenAIRequest(model="gemini-test", messages=[], stream=False)
        cancelled = asyncio.Event()

        async def upstream(*args):
            try:
                await asyncio.Event().wait()   # 模拟挂起的整取上游调用
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def disconnect_soon():
            await asyncio.sleep(0.02)
            connection.finish(True)

        with mock.patch.object(chat_api, "_call_upstream", side_effect=upstream) as call, \
             mock.patch.object(resumable, "disconnect_grace_seconds", return_value=0.05):
            disconnector = asyncio.create_task(disconnect_soon())
            response = await asyncio.wait_for(
                chat_api._resumable_json(req, obj, "test-key"), 2)
            await disconnector
        await asyncio.wait_for(cancelled.wait(), 1)
        self.assertEqual(call.await_count, 1)   # 单次调用即取消，无重试风暴
        body = json.loads(response.body)
        self.assertEqual(response.status_code, 410)
        self.assertIn("续传窗口", body["error"]["message"])


if __name__ == "__main__":
    unittest.main()
