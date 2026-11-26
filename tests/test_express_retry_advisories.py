"""A response's Retry-After cannot be shortened on the next Express attempt."""
import asyncio
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx
from google.genai import types

import channel_dispatch
from main import app
from models import OpenAIRequest
from routes import claude_api
from upstreams import express_pipeline as sdk, native_passthrough as native


class ExpressRetryAdvisories(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(patch.object(native.app_state, "get_channel_mode", return_value="api_key"))
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.enterContext(patch.object(app.state.express_key_manager, "get_express_record",
                                      return_value={"key": "fixture"}))
        self.http = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.ASGITransport(app),
            base_url="http://local", headers={"authorization": "Bearer test-key", "x-resumable": "false"}))

    async def test_native_honors_numeric_and_date_advisories_or_returns_the_original_http_response(self):
        future = format_datetime(datetime.now(UTC) + timedelta(seconds=45), usegmt=True)
        for hint, budget, should_wait in (("45", 90, True), (future, 90, True),
                                          ("121", 300, False), ("45", 10, False)):
            with self.subTest(hint=hint, budget=budget):
                send = Mock(side_effect=[httpx.Response(429, headers={"retry-after": hint},
                    json={"error": {"code": 429, "message": "original quota window"}}),
                    httpx.Response(200, json={"candidates": []})])
                async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as client:
                    with (patch.object(native.passthrough_client_pool, "get", return_value=client),
                          patch.object(native, "request_timeout_seconds", return_value=budget),
                          patch.object(native, "get_retry_settings", return_value=(1, 5, 0)),
                          patch.object(native, "_wait_for_retry", new=AsyncMock(return_value=True)) as wait):
                        res = await self.http.post("/v1beta/models/test:generateContent", json={"contents": []})
                if should_wait:
                    self.assertEqual((res.status_code, send.call_count), (200, 2))
                    self.assertGreater(wait.await_args.args[1], 40)
                    self.assertLessEqual(wait.await_args.args[1], 45)
                else:
                    self.assertEqual((res.status_code, send.call_count), (429, 1))
                    self.assertEqual(res.headers["retry-after"], hint)
                    self.assertEqual(res.json()["error"]["message"], "original quota window")
                    wait.assert_not_awaited()

    async def test_sdk_direct_advisory_is_not_a_shorter_backoff_or_a_second_generation(self):
        self.enterContext(patch.object(sdk, "get_retry_settings", return_value=(1, 5, 0)))
        self.enterContext(patch.object(channel_dispatch, "request_timeout_seconds", return_value=10))
        self.enterContext(patch.object(claude_api, "_available_models", return_value=["gemini-3.6-flash"]))
        self.enterContext(patch.object(channel_dispatch.app_state, "is_web_proxy_enabled", return_value=False))
        self.enterContext(patch.object(channel_dispatch, "_other_channel", return_value=None))
        exc = RuntimeError("original quota window")
        exc.code, exc.response = 429, httpx.Response(429, headers={"retry-after": "45"})
        answer = types.GenerateContentResponse(candidates=[types.Candidate(
            content=types.Content(parts=[types.Part(text="ok")]), finish_reason="STOP")])
        generate = AsyncMock(side_effect=[exc, answer])
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate)))
        async def produce(obj, req):
            return await sdk.execute_gemini_call(client, obj.model, lambda _: [], {}, obj, fastapi_request=req)
        with (patch.object(channel_dispatch.express_upstream, "chat_completions", produce),
              patch.object(sdk, "wait_for_retry", new=AsyncMock(return_value=True)) as wait):
            for path, data in (("/v1/chat/completions", {"messages": []}), ("/v1/messages", {
                    "messages": [{"role": "user", "content": "hi"}], "max_tokens": 3})):
                with self.subTest(path=path):
                    generate.reset_mock(side_effect=True)
                    generate.side_effect = [exc, answer]
                    res = await self.http.post(path, json={"model": "gemini-3.6-flash", **data})
                    self.assertEqual(res.status_code, 429)
                    self.assertEqual(res.headers["retry-after"], "45")
                    self.assertIn("original quota window", res.json()["error"]["message"])
                    self.assertEqual(generate.await_count, 1)
                    wait.assert_not_awaited()
            # A wait that fits the budget still recovers, using the full hint.
            with patch.object(channel_dispatch, "request_timeout_seconds", return_value=90):
                generate.side_effect = [exc, answer]
                res = await self.http.post("/v1/chat/completions", json={"model": "gemini-3.6-flash", "messages": []})
                self.assertEqual(res.status_code, 200)
                self.assertEqual(wait.await_args.args[1], 45)

    async def test_sdk_true_and_fake_stream_relay_advisory_without_replaying_generation(self):
        exc = RuntimeError("original quota window")
        exc.code, exc.response = 429, httpx.Response(429, headers={"retry-after": "600"})
        for fake in (False, True):
            with self.subTest(fake=fake):
                create = AsyncMock(side_effect=exc)
                model = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=True)
                client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
                    generate_content=create, generate_content_stream=create)))
                req = SimpleNamespace(scope={}, is_disconnected=AsyncMock(return_value=False))
                async def produce(obj, request):
                    return await sdk.execute_gemini_call(client, obj.model, lambda _: [], {}, obj,
                                                         fastapi_request=request)
                base = sdk.app_state.get_setting
                with (patch.object(sdk.app_state, "get_setting", side_effect=lambda k, d=None:
                              fake if k == "fake_streaming" else base(k, d)),
                      patch.object(sdk, "get_retry_settings", return_value=(1, 0, 0)),
                      patch.object(channel_dispatch, "_other_channel", return_value="cookie"),
                      patch.object(channel_dispatch, "_invoke_channel", new=AsyncMock()) as alt,
                      patch.object(channel_dispatch.express_upstream, "chat_completions", produce)):
                    result = await channel_dispatch._dispatch_upstream(model, req)
                    try:
                        async with asyncio.timeout(1):
                            frames = "".join([f async for f in result.body_iterator])
                    finally:
                        await result.aclose()
                self.assertIn('"retry_after": "600"', frames)
                self.assertIn("original quota window", frames)
                self.assertEqual(create.await_count, 1)
                alt.assert_not_awaited()
