"""Native Express validation rejects contradictory terminals without rewriting wire data."""

import asyncio
import json
from unittest.mock import patch

import httpx

from admission import generation_limiter
from request_tracking import request_registry
from response_validation import IncompleteResponseError
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from upstreams import native_passthrough as native


def candidate(index, finish=None, text=None):
    value = {"index": index}
    if finish is not None:
        value["finishReason"] = finish
    if text is not None:
        value["content"] = {"role": "model", "parts": [{"text": text, "thoughtSignature": "c2ln"}]}
    return value


def sample(*candidates):
    return {"candidates": list(candidates)}


class NativeTerminalConsistencyTests(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings["first_reply_timeout_seconds"] = 2
        self.chunks = ()
        self.attempts = self.closes = 0
        owner = self

        class Body(httpx.AsyncByteStream):
            def __init__(self, chunks):
                self.chunks = chunks

            async def __aiter__(self):
                for chunk in self.chunks:
                    yield chunk

            async def aclose(self):
                owner.closes += 1

        def upstream(request):
            self.attempts += 1
            content_type = "text/event-stream" if request.url.params["alt"] == "sse" else "application/json"
            return httpx.Response(200, stream=Body(self.chunks), headers={"content-type": content_type})

        client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.MockTransport(upstream)))
        self.enterContext(patch.object(native.passthrough_client_pool, "get", return_value=client))

    async def relay(self, objects, *, sse, expect_broken_array=False):
        self.attempts = self.closes = 0
        encoded = [json.dumps(item, separators=(",", ": ")).encode() for item in objects]
        if sse:
            self.chunks = [b"data: " + item + b"\n\n" for item in encoded]
        else:
            self.chunks = [(b"[" if i == 0 else b"") + item + (b"]" if i == len(encoded) - 1 else b",")
                           for i, item in enumerate(encoded)]
        peer = Peer(f"/v1beta/models/{MODEL}:streamGenerateContent", query=b"alt=sse" if sse else b"alt=json",
                    payload={"contents": [{"parts": [{"text": "hello"}]}]}, headers=[(b"x-resumable", b"false")])
        baseline = generation_limiter.active
        if expect_broken_array:
            with self.assertRaises(IncompleteResponseError):
                await asyncio.wait_for(peer.run(), 2)
        else:
            await asyncio.wait_for(peer.run(), 2)
        self.assertEqual((self.attempts, self.closes, generation_limiter.active), (1, 1, baseline))
        trace_id = dict(peer.messages[0]["headers"])[b"x-trace-id"].decode()
        trace = request_registry.get(trace_id).snapshot()
        return peer.messages[0]["status"], b"".join(m.get("body", b"") for m in peer.messages), trace

    async def test_rejected_before_first_reply_same_frame_or_invalid_bool_index(self):
        for broken in (sample(candidate(0, "MAX_TOKENS", "must not be exposed"), candidate(0, "STOP")),
                       sample(candidate(False, "STOP", "must not be exposed")),
                       sample(candidate(True, "STOP", "must not be exposed"))):
            for sse in (False, True):
                with self.subTest(sse=sse, broken=broken):
                    status, body, trace = await self.relay([broken], sse=sse)
                    self.assertEqual((status, trace["outcome"]), (502, "error"))
                    self.assertEqual(json.loads(body)["error"]["code"], 502)
                    self.assertNotIn(b"must not be exposed", body)
                    self.assertNotIn("first_reply", trace["timings"])

    async def test_late_contradiction_surfaces_error_without_retry_or_rolling_back_visible_contents(self):
        self.settings["retry_max"] = 1
        first = sample(candidate(0, "MAX_TOKENS", "already delivered"))
        bad = {**sample(candidate(0, "STOP")), "usageMetadata": {"totalTokenCount": 123}}
        for sse in (False, True):
            with self.subTest(sse=sse):
                status, body, trace = await self.relay([first, bad], sse=sse, expect_broken_array=not sse)
                self.assertEqual((status, trace["outcome"]), (200, "error"))
                self.assertIn(self.chunks[0], body)
                self.assertNotIn(self.chunks[1], body)
                self.assertIn("first_reply", trace["timings"])
                if sse:
                    data = [json.loads(line[6:]) for line in body.splitlines() if line.startswith(b"data: ")]
                    self.assertEqual(data[0], first)
                    self.assertEqual(data[1]["error"]["code"], 502)
                    self.assertEqual(len(data), 2)

    async def test_repeat_of_identical_known_or_future_reason_sibling_and_usage_remain_byte_exact(self):
        for reason in ("MAX_TOKENS", "FUTURE_PROVIDER_REASON"):
            chunks = [sample(candidate(0, "FINISH_REASON_UNSPECIFIED", "first"), candidate(1, text="second")),
                      sample(candidate(0, reason), candidate(1, "STOP")),
                      sample(candidate(0, reason)), {"usageMetadata": {"totalTokenCount": 4}}]
            for sse in (False, True):
                with self.subTest(sse=sse, reason=reason):
                    status, body, trace = await self.relay(chunks, sse=sse)
                    upstream = b"".join(self.chunks)
                    self.assertEqual((status, body[-len(upstream):], trace["outcome"]), (200, upstream, "succeeded"))
