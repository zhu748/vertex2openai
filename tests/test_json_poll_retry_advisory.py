"""Polling a retained JSON error keeps the provider's validated delay."""
import asyncio
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

from resumable import ResumableStore
from routes import chat_api
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import express_sdk


class JsonPollRetryAdvisory(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings["resumable_enabled"] = True
        self.store = ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(patch.object(chat_api, "resumable_store", self.store))
        self.status, self.hint, self.calls = 429, "78", 0
        self.started, self.release = asyncio.Event(), None

        async def provider(_request):
            self.calls += 1
            self.started.set()
            if self.release is not None:
                await self.release.wait()
            body = ({"error": {"code": self.status, "message": "original provider error"}}
                    if self.status >= 400 else {"candidates": [{"finishReason": "STOP",
                        "content": {"role": "model", "parts": [{"text": "completed"}]}}]})
            return httpx.Response(self.status, json=body, headers={"Retry-After": self.hint, "X-Provider-Raw": "private"})

        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(provider)}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))

    async def post(self, key, receipt=None):
        headers = {"X-Resumable": "true", "Idempotency-Key": key}
        if receipt is not None:
            headers["X-Resume-Id"] = receipt
        path, body = _request("openai", "auto", tools=False)
        return await self.client.post(path, json=body, headers=headers)

    async def test_completed_poll_has_only_bounded_error_advisory_without_changing_replay(self):
        future = format_datetime(datetime.now(UTC) + timedelta(seconds=180), usegmt=True)
        for i, (status, hint, retained, exposed) in enumerate((
                (429, "78", "78", "78"), (503, future, future, future), (503, "0", "0", "0"),
                (429, "not a delay", None, None), (429, "5" * 129, "5" * 129, None), (200, "78", None, None))):
            with self.subTest(status=status, hint=hint):
                self.status, self.hint = status, hint
                key = f"advisory-{i}"
                first = await self.post(key)
                self.assertEqual((first.status_code, first.headers.get("Retry-After")), (status, retained))
                receipt = first.headers["X-Resume-Id"]
                poll = await self.client.get(f"/v1/resumable/{receipt}")
                self.assertEqual(poll.status_code, 200)
                self.assertNotIn("Retry-After", poll.headers)
                self.assertNotIn("X-Provider-Raw", poll.headers)
                payload = poll.json()
                self.assertTrue(payload["done"])
                self.assertEqual(payload["status_code"], status)
                self.assertEqual(payload["payload"], first.json())
                if exposed is not None:
                    self.assertEqual(payload["retry_after"], exposed)
                else:
                    self.assertNotIn("retry_after", payload)
                replay = await self.post(key, receipt)
                self.assertEqual((replay.status_code, replay.headers.get("Retry-After")), (status, retained))
                self.assertEqual(replay.json(), first.json())
                self.assertEqual(self.calls, i + 1)

    async def test_pending_receipt_does_not_claim_a_provider_advisory(self):
        self.release = asyncio.Event()
        in_flight = asyncio.create_task(self.post("pending"))
        try:
            await asyncio.wait_for(self.started.wait(), 2)
            receipt, = self.store._buffers
            poll = await self.client.get(f"/v1/resumable/{receipt}")
            payload = poll.json()
            self.assertEqual(poll.status_code, 200)
            self.assertFalse(payload["done"])
            self.assertNotIn("retry_after", payload)
        finally:
            self.release.set()
            completed = await in_flight
        self.assertEqual(completed.status_code, 429)
        self.assertEqual(self.calls, 1)
