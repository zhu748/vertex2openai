"""A conclusive single-candidate refusal is deliverable, but never a model token."""
import asyncio
import json
import unittest
from unittest.mock import patch

import httpx

import first_reply
from main import app
from request_tracking import RequestTrace, current_trace
from upstreams import native_passthrough as native


class NativeSafetyFirstReplyTests(unittest.IsolatedAsyncioTestCase):
    async def test_single_safety_releases_gate_for_sse_and_json_array_only(self):
        safety = {"candidates": [{"index": 0, "finishReason": "SAFETY", "safetyRatings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "probability": "HIGH"}]}]}
        usage = {"usageMetadata": {"totalTokenCount": 4}}
        manager = type("Manager", (), {"get_total_keys": lambda _: 1,
                                       "get_express_record": lambda _, **kwargs: {"key": "fixture"}})()
        with (patch.object(app.state, "express_key_manager", manager),
              patch.object(native, "get_retry_settings", return_value=(0, 0, 0)),
              patch.object(first_reply, "timeout_seconds", return_value=.025),
              patch.object(native.app_state, "get_channel_mode", return_value="api_key")):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local",
                                         headers={"x-goog-api-key": "test-key"}) as client:
                for sse in (False, True):
                    head = (b"data: " if sse else b"[") + json.dumps(safety).encode() + (b"\n\n" if sse else b",")
                    tail = (b"data: " if sse else b"") + json.dumps(usage).encode() + (b"\n\n" if sse else b"]")
                    class DelayedUsage(httpx.AsyncByteStream):
                        async def __aiter__(self):
                            yield head
                            await asyncio.sleep(.06)
                            yield tail
                    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(
                            200, headers={"content-type": "text/event-stream" if sse else "application/json"},
                            stream=DelayedUsage()))) as upstream:
                        with patch.object(native.passthrough_client_pool, "get", return_value=upstream):
                            for config, expected in (({}, 200), ({"candidateCount": 1}, 200),
                                                     ({"candidate_count": "1.0"}, 200), ({"candidateCount": None}, 200),
                                                     ({"candidateCount": 2}, 504), ({"candidate_count": 2}, 504)):
                                with self.subTest(sse=sse, config=config):
                                    response = await client.post(
                                        "/v1beta/models/gemini-test:streamGenerateContent" + ("?alt=sse" if sse else ""),
                                        json={"contents": [], "generationConfig": config})
                                    self.assertEqual(response.status_code, expected)
                                    if expected == 200:
                                        self.assertIn(head, response.content)
                                        self.assertTrue(response.content.endswith(tail))

    def test_only_unambiguous_safety_releases_gate_without_a_token_metric(self):
        for candidates, ready in (
            ([{"finishReason": "SAFETY"}], True), ([{"finishReason": "RECITATION"}], True),
            ([{"finishReason": "IMAGE_SAFETY"}], True), ([{"finishReason": "UNRECOGNIZED"}], False),
            ([{"finishReason": "STOP"}], False), ([{"index": 1, "finishReason": "SAFETY"}], False),
            ([{"index": 0, "finishReason": "SAFETY"}, {"index": 1}], False),
        ):
            with self.subTest(candidates=candidates):
                trace = RequestTrace("/native")
                token = current_trace.set(trace)
                try:
                    guard = first_reply.FirstReplyGuard("test", seconds=3)
                    state = native._NativeStreamState(guard)
                    state.inspect({"candidates": candidates})
                    self.assertEqual(guard.ready, ready)
                    self.assertNotIn("first_reply", trace.timings)
                finally:
                    current_trace.reset(token)
