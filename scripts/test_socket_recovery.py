"""Loopback HTTP disconnect/reconnect tests with a real Uvicorn ASGI server."""
import asyncio
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-socket-recovery-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
import uvicorn
from fastapi.responses import StreamingResponse
from google.genai import types
from contextlib import asynccontextmanager

import config
import main
import resumable
import streaming
from runtime_state import app_state
from routes import chat_api, gemini_native
from upstreams import express_sdk, cookie_proxy


async def first_event(response):
    event_id = None
    async for line in response.aiter_lines():
        if line.startswith("id:"):
            event_id = line[3:].strip()
        if line.startswith("data:"):
            return event_id, line
    raise AssertionError("No first data event")


class SocketRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.release = asyncio.Event()
        self.patches = [mock.patch.object(chat_api, "resumable_store", self.store),
                        mock.patch.object(gemini_native, "resumable_store", self.store)]
        for patch in self.patches:
            patch.start()
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen()
        self.server = uvicorn.Server(uvicorn.Config(main.app, lifespan="off", log_level="error", access_log=False))
        self.server_task = asyncio.create_task(self.server.serve(sockets=[self.sock]))
        async with asyncio.timeout(3):
            while not self.server.started:
                await asyncio.sleep(0.005)
        self.client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{self.sock.getsockname()[1]}",
                                        trust_env=False, timeout=5)

    async def asyncTearDown(self):
        self.release.set()
        await self.client.aclose()
        await self.store.aclose()
        self.server.should_exit = True
        await asyncio.wait_for(self.server_task, 5)
        self.sock.close()
        for patch in reversed(self.patches):
            patch.stop()

    async def test_fake_stream_keeps_tcp_active_during_preparation_and_generation(self):
        # Scale the idle threshold down to exercise actual socket flushing;
        # no Cloudflare account or Google credentials are involved.
        for web in (False, True):
            for buffered in (False, True):
                preparation, generation, started = asyncio.Event(), asyncio.Event(), asyncio.Event()
                calls = []
                settings = dict(config.DEFAULT_SETTINGS, fake_streaming=True, fake_streaming_interval=0,
                                resumable_enabled=buffered, express_location="", proxy_url="",
                                prefill_mode="off", retry_max=0)
                async def generate(**kwargs):
                    calls.append("sdk")
                    started.set()
                    await generation.wait()
                    return types.GenerateContentResponse(candidates=[types.Candidate(
                        content=types.Content(role="model", parts=[types.Part(text="complete answer")]),
                        finish_reason="STOP")])
                async def post(*args, **kwargs):
                    calls.append("cookie")
                    started.set()
                    await generation.wait()
                    return httpx.Response(200, json={"results": [{"data": {"candidates": [{
                        "content": {"parts": [{"text": "complete answer"}]}, "finishReason": "STOP"}]}}]})
                @asynccontextmanager
                async def open_client(*args, **kwargs):
                    yield SimpleNamespace(post=post)
                target = chat_api.cookie_upstream if web else chat_api.express_upstream
                original = target.chat_completions
                async def slow_prepare(*args):
                    await preparation.wait()
                    return await original(*args)
                key_manager = SimpleNamespace(get_total_keys=lambda: 1,
                    get_express_record=lambda **kwargs: {"key": "offline", "project_id": ""})
                patches = [
                    mock.patch.object(app_state, "is_web_proxy_enabled", return_value=web),
                    mock.patch.object(app_state, "get_effective_settings", side_effect=lambda m, base=None: (dict(settings) if base is None else dict(base))),
                    mock.patch.object(app_state, "get_setting", side_effect=lambda k, d=None: settings.get(k, d)),
                    mock.patch.object(main.app.state, "express_key_manager", key_manager),
                    mock.patch.object(streaming, "HEARTBEAT_INTERVAL", 0.01),
                    mock.patch.object(target, "chat_completions", side_effect=slow_prepare),
                    mock.patch.object(express_sdk, "get_pooled_express_client", return_value=SimpleNamespace(
                        aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate)))),
                    mock.patch.object(cookie_proxy, "_get_cookie_string", return_value="SAPISID=test"),
                    mock.patch.object(cookie_proxy, "_get_project_id", return_value="project"),
                    mock.patch.object(cookie_proxy, "open_upstream_client", open_client),
                ]
                for patch in patches:
                    patch.start()
                try:
                    async with self.client.stream("POST", "/v1/chat/completions",
                        headers={"Authorization": "Bearer " + config.API_KEY,
                                 "Idempotency-Key": f"idle-{web}-{buffered}"},
                        json={"model": "gemini-3.6-flash", "stream": True,
                              "messages": [{"role": "user", "content": "hello"}]}) as response:
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.headers["x-accel-buffering"], "no")
                        self.assertIn("no-transform", response.headers["cache-control"])
                        lines = response.aiter_lines()
                        async def heartbeat():
                            async with asyncio.timeout(0.5):
                                async for line in lines:
                                    if line.startswith(":"):
                                        return
                            raise AssertionError("SSE ended before the next heartbeat")
                        await heartbeat()
                        await heartbeat()
                        self.assertFalse(started.is_set())
                        preparation.set()
                        await asyncio.wait_for(started.wait(), 1)
                        await heartbeat()
                        await heartbeat()
                        generation.set()
                        remainder = "\n".join([line async for line in lines])
                        self.assertIn("complete answer", remainder)
                        self.assertIn("[DONE]", remainder)
                        self.assertEqual(calls, ["cookie" if web else "sdk"])
                finally:
                    preparation.set()
                    generation.set()
                    for patch in reversed(patches):
                        patch.stop()

    async def test_openai_frontend_tcp_disconnect_recovers_original_generation(self):
        completed = asyncio.Event()
        async def call(*args):
            async def frames():
                try:
                    yield 'data: {"choices":[{"delta":{"content":"first"}}]}\n\n'
                    await self.release.wait()
                    yield 'data: {"choices":[{"delta":{"content":"second"}}]}\n\n'
                    yield resumable.DONE_FRAME
                finally:
                    completed.set()
            return StreamingResponse(frames(), media_type="text/event-stream")
        body = {"model": "gemini-test", "messages": [{"role": "user", "content": "hi"}], "stream": True}
        headers = {"Authorization": "Bearer " + config.API_KEY, "Idempotency-Key": "socket-operation"}
        with mock.patch.object(chat_api, "_call_upstream", side_effect=call) as upstream:
            async with self.client.stream("POST", "/v1/chat/completions", headers=headers, json=body) as response:
                self.assertEqual(response.status_code, 200)
                sid = response.headers["x-resume-id"]
                cursor, frame = await first_event(response)
                self.assertIn('"first"', frame)
            # The TCP response is now abandoned. Its independent upstream pump
            # must still finish, and the reconnect must not generate again.
            self.release.set()
            await asyncio.wait_for(completed.wait(), 2)
            recovered = await self.client.post("/v1/chat/completions", json=body,
                                               headers=dict(headers, **{"X-Resume-Id": sid, "Last-Event-ID": cursor}))
            self.assertEqual(recovered.status_code, 200)
            self.assertIn('"second"', recovered.text)
            self.assertNotIn('"first"', recovered.text)
            self.assertIn("[DONE]", recovered.text)
            self.assertEqual(upstream.await_count, 1)

    async def test_native_opt_in_tcp_disconnect_and_poll_recovery(self):
        completed, closed = asyncio.Event(), asyncio.Event()
        requests = []
        class Source(httpx.AsyncByteStream):
            async def __aiter__(inner_self):
                yield b'data: {"candidates":[{"index":0,"content":{"parts":[{"text":"first"}]}}]}\n\n'
                await self.release.wait()
                yield b'data: {"candidates":[{"index":0,"content":{"parts":[{"text":"second"}]},"finishReason":"STOP"}]}\n\n'
                completed.set()
            async def aclose(inner_self):
                closed.set()
        def handle(request):
            requests.append(request)
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Source())
        upstream = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        self.addAsyncCleanup(upstream.aclose)
        manager = SimpleNamespace(get_total_keys=lambda: 1,
                                  get_express_record=lambda **kwargs: {"key": "offline-key", "project_id": ""})
        headers = {"x-goog-api-key": config.API_KEY, "X-Resumable": "true", "Idempotency-Key": "native-socket"}
        body = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}
        path = "/v1beta/models/gemini-test:streamGenerateContent?alt=sse"
        with mock.patch.object(main.app.state, "express_key_manager", manager), \
             mock.patch.object(gemini_native.passthrough_client_pool, "get", return_value=upstream):
            async with self.client.stream("POST", path, headers=headers, json=body) as response:
                self.assertEqual(response.status_code, 200)
                sid = response.headers["x-resume-id"]
                cursor, frame = await first_event(response)
                self.assertIn('"first"', frame)
            self.release.set()
            await asyncio.wait_for(completed.wait(), 2)
            poll = await self.client.get(f"/v1/resumable/{sid}?from=1&wait=1",
                                         headers={"Authorization": "Bearer " + config.API_KEY})
            data = poll.json()
            self.assertEqual(data["kind"], "native")
            self.assertTrue(data["done"])
            recovered = await self.client.post(path, json=body,
                                               headers=dict(headers, **{"X-Resume-Id": sid, "Last-Event-ID": cursor}))
        self.assertIn('"second"', recovered.text)
        self.assertNotIn('"first"', recovered.text)
        self.assertNotIn("[DONE]", recovered.text)
        self.assertEqual(len(requests), 1)
        await asyncio.wait_for(closed.wait(), 1)


if __name__ == "__main__":
    unittest.main()
