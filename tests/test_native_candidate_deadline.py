"""Native setup deadlines across post-header candidate failover; no sockets or wall-clock sleeps."""
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx

from proxy_failover import ProxyChain
from upstreams import native_passthrough as native


class NativeCandidateDeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def run_case(self, framing, steps, retry_max):
        clock, requests, closes = [100.0], [], []
        answer = {"candidates": [{"content": {"parts": [{"text": "answer"}]}, "finishReason": "STOP"}]}
        def frame(item):
            if framing == "sse":
                return ("data: " + json.dumps(item) + "\n\n").encode()
            return json.dumps(item if isinstance(item, list) else [item]).encode()

        class Body(httpx.AsyncByteStream):
            def __init__(self, index):
                self.index = index

            async def __aiter__(self):
                elapsed, kind = steps[self.index]
                clock[0] += elapsed
                if kind == "drop":
                    raise httpx.ReadError("mock body stopped before delivering output")
                if kind == "rejected_batch":
                    yield frame([answer, {"error": {"code": 502, "message": "mock batch failure"}}])
                else:
                    yield frame(answer)

            async def aclose(self):
                closes.append(self.index)

        def handle(_request):
            index = len(requests)
            requests.append(clock[0])
            return httpx.Response(200, stream=Body(index), headers={
                "content-type": "text/event-stream" if framing == "sse" else "application/json"})

        manager = SimpleNamespace(get_total_keys=lambda: 1, get_express_record=Mock(return_value={"key": "dummy"}))
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)),
                                  is_disconnected=AsyncMock(return_value=False))
        fake_time = SimpleNamespace(monotonic=lambda: clock[0], time=lambda: clock[0])
        client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        self.addAsyncCleanup(client.aclose)
        with patch.object(native.first_reply_module, "time", fake_time), \
             patch.object(native.first_reply_module, "timeout_seconds", return_value=10), \
             patch.object(native, "get_retry_settings", return_value=(retry_max, 0, 0)), \
             patch.object(native, "_wait_for_retry", new=AsyncMock(return_value=True)), \
             patch.object(native, "ProxyChain", side_effect=lambda _: ProxyChain(candidates=["", ""])), \
             patch.object(native.passthrough_client_pool, "get", return_value=client):
            response = await native._passthrough_request_impl(
                request, "gemini-test", "streamGenerateContent", "alt=" + framing, b"{}")
            if hasattr(response, "body_iterator"):
                try:
                    body = b"".join([chunk async for chunk in response.body_iterator])
                finally:
                    await response.aclose()
            else:
                body = response.body
        self.assertEqual(len(requests), len(steps), "no unbudgeted dispatches")
        self.assertEqual(closes, list(range(len(steps))), "every HTTPX response released exactly once")
        return response.status_code, body, manager.get_express_record.call_count

    async def test_post_header_candidates_share_one_absolute_window(self):
        for framing in ("sse", "json"):
            for first in ("drop", "rejected_batch"):
                with self.subTest(framing=framing, first=first):
                    code, body, selections = await self.run_case(framing, [(6, first), (6, "ok")], 0)
                    self.assertEqual((code, selections), (504, 1))
                    self.assertNotIn(b"answer", body)

    async def test_fast_candidate_and_new_logical_retry_can_still_succeed(self):
        for framing in ("sse", "json"):
            for steps, retries in (([(6, "drop"), (3, "ok")], 0),
                                   ([(6, "drop"), (6, "ok"), (9, "ok")], 1)):
                with self.subTest(framing=framing, retries=retries):
                    code, body, selections = await self.run_case(framing, steps, retries)
                    self.assertEqual((code, selections), (200, 1 + retries))
                    self.assertEqual(body.count(b"answer"), 1)
