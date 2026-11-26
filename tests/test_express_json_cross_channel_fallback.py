"""Express SDK failures and returned provider errors have the same JSON handoff policy."""
import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
import json
import time
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

import channel_dispatch
from resumable import ResumableStore
from routes import chat_api
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery, express_sdk


class ExpressJsonCrossChannelFallback(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings.update(cross_channel_fallback=True, retry_max=0)
        self.calls = []
        self.primary_status, self.primary_hint = 503, None
        self.standby_status, self.standby_hint = 200, None
        self.transport_failure, self.deadline_tick = None, None
        self.express_entered = asyncio.Event()
        self.express_block = False

        async def primary(request):
            self.calls.append("express")
            self.assertIn(":generateContent", request.url.path)
            self.assertTrue(json.loads(request.content)["contents"])
            self.express_entered.set()
            if self.express_block:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError as exc:
                    # An SDK/transport that translates cancellation into another
                    # exception must not accidentally launch the standby.
                    raise httpx.ReadError("transport unwound", request=request) from exc
            if self.transport_failure is not None:
                raise self.transport_failure
            if self.deadline_tick is not None:
                self.deadline_tick[0] += 1
            return httpx.Response(self.primary_status,
                                  json={"error": {"code": self.primary_status, "message": "original Express failure"}},
                                  headers={"Retry-After": self.primary_hint} if self.primary_hint is not None else {})

        def standby(request):
            self.calls.append("cookie")
            self.assertTrue(json.loads(request.content)["variables"])
            if self.standby_status >= 400:
                headers = {"X-Not-Public": "fixture"}
                if self.standby_hint is not None:
                    headers["Retry-After"] = self.standby_hint
                return httpx.Response(self.standby_status, headers=headers,
                                      json={"error": {"code": self.standby_status, "message": "standby error"}})
            return httpx.Response(200, json={"results": [{"data": {"candidates": [{"index": 0,
                "content": {"role": "model", "parts": [{"text": "standby delivered"}]}, "finishReason": "STOP"
            }]}}]})

        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(primary)}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        studio = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.MockTransport(standby)))

        @asynccontextmanager
        async def open_studio(*args, **kwargs):
            yield studio

        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        self.enterContext(patch.object(cookie_delivery, "open_upstream_client", open_studio))

    async def post(self, protocol, *, headers=None, **params):
        self.calls.clear()
        path, body = _request(protocol, "auto", tools=False)
        return await self.client.post(path, json={**body, **params}, headers=headers)

    async def test_raised_http_and_transport_failures_can_use_the_healthy_standby_once(self):
        for protocol in ("openai", "claude"):
            for status in (401, 403, 429, 503):
                with self.subTest(protocol=protocol, status=status):
                    self.primary_status = status
                    reply = await self.post(protocol)
                    self.assertEqual(reply.status_code, 200, reply.text)
                    self.assertIn("standby delivered", reply.text)
                    self.assertEqual(self.calls, ["express", "cookie"])
            with self.subTest(protocol=protocol, transport_failure="truncated HTTP response"):
                self.transport_failure = httpx.RemoteProtocolError("peer disconnected before a response")
                try:
                    reply = await self.post(protocol)
                    self.assertEqual(reply.status_code, 200, reply.text)
                    self.assertEqual(self.calls, ["express", "cookie"])
                finally:
                    self.transport_failure = None

    async def test_retained_openai_json_delivers_winner_and_never_regenerates_on_replay(self):
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.settings["resumable_enabled"] = True
        headers = {"X-Resumable": "true", "Idempotency-Key": "once"}
        with patch.object(chat_api, "resumable_store", store):
            reply = await self.post("openai", headers=headers)
            self.assertEqual(reply.status_code, 200, reply.text)
            self.assertEqual(self.calls, ["express", "cookie"])
            replay = await self.post("openai", headers={**headers, "X-Resume-Id": reply.headers["X-Resume-Id"]})
            self.assertEqual(replay.status_code, 200, replay.text)
            self.assertEqual(replay.json(), reply.json())
            self.assertEqual(replay.headers["X-Resumable-Replay"], "1")
            self.assertEqual(self.calls, [])

    async def test_invalid_request_delay_disable_and_feature_lock_do_not_start_the_standby(self):
        date = format_datetime(datetime.now(UTC) + timedelta(seconds=180), usegmt=True)
        for protocol in ("openai", "claude"):
            for status, hint, fallback in ((400, None, True), (503, "42", True),
                                           (429, date, True), (503, None, False)):
                with self.subTest(protocol=protocol, status=status, hint=hint, enabled=fallback):
                    self.primary_status, self.primary_hint = status, hint
                    self.settings["cross_channel_fallback"] = fallback
                    reply = await self.post(protocol)
                    self.assertEqual(reply.status_code, status)
                    self.assertIn("original Express failure", reply.json()["error"]["message"])
                    self.assertEqual(reply.headers.get("Retry-After"), hint)
                    self.assertEqual(self.calls, ["express"])
        self.settings.update(cross_channel_fallback=True, cookie_structured_output=False)
        reply = await self.post("openai", response_format={"type": "json_object"})
        self.assertEqual(reply.status_code, 503, reply.text)
        self.assertEqual(self.calls, ["express"])

    async def test_double_failure_preserves_primary_diagnostics_and_only_bounded_standby_delay(self):
        self.standby_status = 429
        for protocol in ("openai", "claude"):
            for hint, expected in (("78", "78"), ("0", None), ("not a delay", None), ("5" * 140, None)):
                with self.subTest(protocol=protocol, hint=hint):
                    self.standby_hint = hint
                    reply = await self.post(protocol)
                    self.assertEqual(reply.status_code, 503)
                    self.assertIn("original Express failure", reply.json()["error"]["message"])
                    self.assertNotIn("standby error", reply.text)
                    self.assertEqual(reply.headers.get("Retry-After"), expected)
                    self.assertNotIn("X-Not-Public", reply.headers)
                    self.assertEqual(self.calls, ["express", "cookie"])

    async def test_unknown_local_exception_and_absolute_deadline_do_not_start_the_standby(self):
        self.transport_failure = ValueError("local unexpected failure")
        reply = await self.post("openai")
        self.assertNotIn("standby delivered", reply.text)
        self.assertEqual(self.calls, ["express"])
        self.transport_failure = None
        self.deadline_tick = [time.monotonic()]
        with (patch.object(channel_dispatch, "time", SimpleNamespace(monotonic=lambda: self.deadline_tick[0])),
              patch.object(channel_dispatch, "request_timeout_seconds", return_value=1)):
            reply = await self.post("openai")
        self.assertEqual(reply.status_code, 504)
        self.assertIn("总执行期限", reply.json()["error"]["message"])
        self.assertEqual(self.calls, ["express"])

    async def test_cancelled_primary_cannot_dispatch_a_second_generation(self):
        self.express_block = True
        post = asyncio.create_task(self.post("openai"))
        try:
            await asyncio.wait_for(self.express_entered.wait(), 2)
        finally:
            post.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await post
        self.assertEqual(self.calls, ["express"])
