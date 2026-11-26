"""Bounded trace metadata for already-delivered native replies, not token billing."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

import config
from main import app
import middleware
from request_tracking import RequestRegistry, RequestTrace, current_trace, record_usage
from test_gemini_native_stats_retry import KeyManager
from upstreams import native_passthrough


@pytest.mark.parametrize("part,substantive", [
    ({"executableCode": {"language": "PYTHON", "code": "1 + 1"}}, True),
    ({"codeExecutionResult": {"outcome": "OUTCOME_OK"}}, True),
    ({"codeExecutionResult": {"outcome": "OUTCOME_DEADLINE_EXCEEDED"}}, True),
    ({"codeExecutionResult": {"outcome": "OUTCOME_UNSPECIFIED"}}, False),
    ({"executableCode": {"language": "PYTHON", "code": " "}}, False),
    ({"thoughtSignature": "c2ln"}, False),
])
def test_sent_code_observation_does_not_count_text_tools_or_generation(part, substantive):
    trace = RequestTrace("/native")
    trace.inspect_payload({"candidates": [{"content": {"parts": [part]}}]}, sent=True)
    assert ("first_sent_reply" in trace.timings) == substantive
    assert "first_reply" not in trace.timings and "first_sent_content" not in trace.timings
    assert (trace.data["sent_content_chars"], trace.data["sent_reasoning_chars"], trace.data["tool_deltas"]) == (0, 0, 0)


def test_native_cumulative_counters_are_bounded_and_attempt_local():
    trace = RequestTrace("/native")
    trace.attempt()
    trace.inspect_payload({"usageMetadata": {"promptTokenCount": 8, "totalTokenCount": 8,
        "details": {"private": "ignored"}, "extra1": 1}})
    assert trace.data["usage"] == {"promptTokenCount": 8, "totalTokenCount": 8, "extra1": 1}
    trace.inspect_payload({"usageMetadata": {"candidates_token_count": 3, "extra2": 2}})
    assert trace.data["usage"] == {"promptTokenCount": 8, "candidatesTokenCount": 3, "extra2": 2}
    trace.inspect_payload({"usageMetadata": {"totalTokenCount": 11}}, sent=True)
    assert "totalTokenCount" not in trace.data["usage"]  # a sent or replay observer never updates generated usage
    trace.inspect_payload({"usageMetadata": {"promptTokenCount": False, "totalTokenCount": 11}})
    assert trace.data["usage"] == {"promptTokenCount": 8, "candidatesTokenCount": 3, "totalTokenCount": 11}
    trace.attempt()
    assert trace.data["usage"] == {}
    trace.inspect_payload({"usageMetadata": {"candidatesTokenCount": 2}})
    assert trace.data["usage"] == {"candidatesTokenCount": 2}
    # Legacy direct/OpenAI usage does not change spelling or start merging snapshots.
    token = current_trace.set(trace)
    try:
        record_usage({"prompt_tokens": 4})
    finally:
        current_trace.reset(token)
    trace.attempt()
    assert trace.data["usage"] == {"prompt_tokens": 4}
    trace.inspect_payload({"usage": {"total_tokens": 9}})
    assert trace.data["usage"] == {"total_tokens": 9}


class NativeCodeTelemetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_wire_code_and_fragmented_usage_reach_the_trace(self):
        samples = [
            {"candidates": [{"content": {"parts": [{"executableCode": {"language": "PYTHON", "code": "1 + 1"}}]}}],
             "usageMetadata": {"promptTokenCount": 8}},
            {"candidates": [{"finishReason": "STOP"}], "usageMetadata": {"candidatesTokenCount": 3, "totalTokenCount": 11}},
        ]
        reg = RequestRegistry()
        async def wire(req):
            if req.url.params["alt"] == "sse":
                events = b"".join(b"data: " + json.dumps(frame).encode() + b"\n\n" for frame in samples)
                return httpx.Response(200, content=events, headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=samples)

        upstream = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.MockTransport(wire)))
        client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test"))
        with (patch.object(middleware, "request_registry", reg),
              patch.object(app.state, "express_key_manager", KeyManager()),
              patch.object(native_passthrough, "passthrough_client_pool", SimpleNamespace(get=lambda *_, **__: upstream))):
            for alt in ("sse", "json"):
                with self.subTest(alt=alt):
                    res = await client.post(f"/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt={alt}",
                        headers={"x-goog-api-key": config.API_KEY}, json={"contents": [{"parts": [{"text": "example"}]}]})
                    self.assertEqual(res.status_code, 200)
                    self.assertIn(b"executableCode", res.content)
                    trace = reg.get(res.headers["x-trace-id"])
                    self.assertEqual(trace.snapshot()["status"], "succeeded")
                    self.assertIn("first_reply", trace.timings)
                    self.assertIn("first_sent_reply", trace.timings)
                    self.assertNotIn("first_sent_content", trace.timings)
                    self.assertEqual((trace.data["sent_content_chars"], trace.data["tool_deltas"]), (0, 0))
                    self.assertEqual(trace.data["usage"], {"promptTokenCount": 8, "candidatesTokenCount": 3, "totalTokenCount": 11})
