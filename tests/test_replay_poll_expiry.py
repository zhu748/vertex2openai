"""A poll is a reconnect, not a way to revive an expired generation."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

import auth
import middleware
from main import app
from models import OpenAIRequest
from request_connection import ClientConnection
import resumable
from routes import chat_api


class PollWindowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(resumable, "disconnect_grace_seconds", return_value=10))
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                        headers={"Authorization": "Bearer test-key"})
        self.addAsyncCleanup(self.client.aclose)

    async def disconnected(self, completed):
        connection = ClientConnection()
        request = SimpleNamespace(headers={}, query_params={}, scope={"state": {"client_connection": connection}})
        body = {"model": "gemini-test", "stream": True, "messages": []}
        fingerprint = chat_api._request_fingerprint(OpenAIRequest(**body))
        buf, _, _, _ = await resumable.resolve_buffer(
            self.store, request, "test-key", fingerprint, model=body["model"], kind="stream")
        await buf.append(buf.event_frame('data: {"text":"first"}\n\n'))
        connection.finish(True)
        if completed:
            await buf.finish(terminal_frame=resumable.DONE_FRAME)
        return buf, body

    async def check_stale(self, *, completed):
        buf, body = await self.disconnected(completed)
        buf.last_disconnect_monotonic -= 11
        before = buf.last_disconnect_monotonic
        path = "/v1/resumable/" + buf.id
        # Verify ownership before disclosing a stale receipt or cancelling a worker.
        with patch.object(auth, "API_KEY", "rotated-key"):
            other = await self.client.get(path, headers={"Authorization": "Bearer rotated-key"})
        self.assertEqual(other.status_code, 404)
        self.assertEqual(buf.done, completed)
        for _ in range(2):
            poll = await self.client.get(path, params={"wait": 1})
            self.assertEqual(poll.status_code, 410)
            self.assertNotIn("events", poll.json())
            self.assertFalse(buf.fully_delivered)
            self.assertEqual(buf.last_disconnect_monotonic, before)
            self.assertEqual(buf.status_code, 200 if completed else 410)
        again = await self.client.post("/v1/chat/completions", json=body, headers={"X-Resume-Id": buf.id})
        self.assertEqual(again.status_code, 410)
        self.assertIs(await self.store.get_exact(buf.id), buf)

    async def test_running_poll_cannot_extend_an_expired_window(self):
        await self.check_stale(completed=False)

    async def test_completed_poll_cannot_acknowledge_an_expired_undelivered_result(self):
        await self.check_stale(completed=True)

    async def test_future_cursor_rejects_before_watching_waiting_or_renewing(self):
        buf, _ = await self.disconnected(False)
        before = (buf.last_access, buf.last_disconnect_monotonic, buf._grace_task)
        with patch.object(middleware.ConnectionReceiver, "start_watching") as watch, \
             patch.object(buf, "attach_poll", wraps=buf.attach_poll) as attach, \
             patch.object(buf, "wait_for", new_callable=AsyncMock) as wait:
            path = "/v1/resumable/" + buf.id
            params = {"from": buf.next_seq + 1, "wait": 4}
            with patch.object(auth, "API_KEY", "rotated-key"):
                other = await self.client.get(path, params=params, headers={"Authorization": "Bearer rotated-key"})
            self.assertEqual(other.status_code, 404)
            response = await self.client.get(path, params=params)
            self.assertEqual(response.status_code, 409)
            self.assertEqual(response.json()["error"]["type"], "invalid_resume_cursor")
            watch.assert_not_called()
            attach.assert_not_called()
            wait.assert_not_awaited()
        self.assertEqual((buf.last_access, buf.last_disconnect_monotonic, buf._grace_task), before)

        # The exact tail is different: it is a valid long poll for the next event.
        async def emit_next(seq, timeout):
            self.assertEqual((seq, timeout, buf._polls), (1, 4, 1))
            await buf.append(buf.event_frame('data: {"text":"second"}\n\n'))
        with patch.object(buf, "wait_for", side_effect=emit_next) as wait:
            response = await self.client.get(path, params={"from": buf.next_seq, "wait": 4})
            self.assertEqual(response.status_code, 200)
            self.assertEqual([frame["seq"] for frame in response.json()["events"]], [1])
            wait.assert_awaited_once()

    async def test_in_window_long_poll_still_counts_as_activity(self):
        buf, _ = await self.disconnected(False)
        before = buf.last_disconnect_monotonic
        response = await self.client.get("/v1/resumable/" + buf.id, params={"wait": 2})
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["done"])
        self.assertGreaterEqual(buf.last_disconnect_monotonic, before + 2)
        self.assertFalse(buf.fully_delivered)
