"""A translated, live SDK stream cannot reopen an already finished Gemini choice."""
import json
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness
from upstreams import express_sdk


def candidate(index, *parts, reason=None):
    row = {"index": index}
    if parts:
        row["content"] = {"role": "model", "parts": list(parts)}
    if reason:
        row["finishReason"] = reason
    return row


def packet(*candidates):
    return {"candidates": list(candidates)}


class ExpressCandidateBoundary(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.docs = []
        self.calls = 0

        def respond(request):
            self.assertIn(":streamGenerateContent", request.url.path)
            self.calls += 1
            return httpx.Response(200, text="".join(f"data: {json.dumps(d)}\n\n" for d in self.docs),
                                  headers={"content-type": "text/event-stream"})

        self.sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(respond)}))
        self.addCleanup(self.sdk.close)
        self.addAsyncCleanup(self.sdk.aio.aclose)
        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=self.sdk))

    async def stream(self, docs, dialect="openai", **options):
        self.docs = docs
        self.calls = 0
        body = {"model": MODEL, "messages": MESSAGES, "stream": True, **options}
        if dialect == "claude":
            body["max_tokens"] = 32
        path = "/v1/messages" if dialect == "claude" else "/v1/chat/completions"
        res = await self.client.post(path, json=body)
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(self.calls, 1)
        return chunks(res.text)

    async def test_conflicting_reason_or_late_part_never_reaches_either_client(self):
        first = candidate(0, {"text": "early"}, reason="MAX_TOKENS")
        for tail in (
            candidate(0, reason="STOP"),
            candidate(0, reason="CONTINUATION"),  # also maps to "length", but not the same reason
            candidate(0, {"text": "late-part"}, reason="MAX_TOKENS"),
            candidate(0, {"functionCall": {"id": "late-call", "name": "late_tool", "args": {}}}),
        ):
            for same_packet in (False, True):
                docs = [packet(first, tail)] if same_packet else [packet(first), packet(tail)]
                for dialect in ("openai", "claude"):
                    with self.subTest(tail=tail, same_packet=same_packet, dialect=dialect):
                        wire = await self.stream(docs, dialect)
                        err, = [r["error"] for r in wire if "error" in r]
                        self.assertTrue(err["message"])
                        self.assertNotIn("late-", json.dumps(wire))
                        if not same_packet:  # earlier data is already visible and cannot be retracted
                            self.assertIn("early", json.dumps(wire))
                        if dialect == "openai":
                            self.assertEqual(err["code"], 502)
                            finishes = [c["finish_reason"] for r in wire for c in r.get("choices", [])
                                        if c.get("finish_reason")]
                            self.assertEqual(finishes, [] if same_packet else ["length"])
                        else:
                            self.assertNotIn("message_stop", [r["type"] for r in wire])
                            self.assertEqual(wire[-1]["type"], "error")

    async def test_sparse_sibling_repeated_terminal_and_usage_trailer(self):
        repeat = {**candidate(7, reason="MAX_TOKENS"),
                  "logprobsResult": {"chosenCandidates": [{"token": "first", "logProbability": -.25}]}}
        docs = [packet(candidate(7, {"text": "first"}, reason="MAX_TOKENS"),
                       candidate(2, {"text": "other"})),
                packet(repeat, candidate(2, {"text": "-ok"}, reason="STOP")),
                {"usageMetadata": {"promptTokenCount": 2, "candidatesTokenCount": 6, "totalTokenCount": 8}}]
        wire = await self.stream(docs, n=2, stream_options={"include_usage": True})
        self.assertFalse(any("error" in r for r in wire))
        choices = [c for r in wire for c in r["choices"]]
        self.assertEqual([(c["index"], c["finish_reason"]) for c in choices if c.get("finish_reason")],
                         [(7, "length"), (2, "stop")])
        self.assertEqual([(c["index"], c["delta"]["content"]) for c in choices if c["delta"].get("content")],
                         [(7, "first"), (2, "other"), (2, "-ok")])
        self.assertEqual([(c["index"], t["token"]) for c in choices if c.get("logprobs")
                          for t in c["logprobs"]["content"]], [(7, "first")])
        self.assertEqual(wire[-1]["usage"]["total_tokens"], 8)

    async def test_claude_accepts_valid_truncation_and_trailing_usage(self):
        wire = await self.stream([
            packet(candidate(0, {"text": "early"}, reason="MAX_TOKENS")),
            packet(candidate(0, reason="MAX_TOKENS")),
            {"usageMetadata": {"candidatesTokenCount": 6}}], "claude")
        self.assertFalse(any("error" in r for r in wire))
        stop, = [r for r in wire if r["type"] == "message_delta"]
        self.assertEqual((stop["delta"]["stop_reason"], stop["usage"]["output_tokens"]), ("max_tokens", 6))
        self.assertEqual(wire[-1]["type"], "message_stop")

    async def test_rejected_tail_closes_owned_sdk_stream_without_reading_past_it(self):
        reads = closes = 0
        generate = self.sdk.aio.models.generate_content_stream

        async def observed(**kwargs):
            source = await generate(**kwargs)
            class Owner:
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    nonlocal reads
                    reads += 1
                    return await anext(source)

                async def aclose(self):
                    nonlocal closes
                    closes += 1
                    await source.aclose()
            return Owner()

        with patch.object(self.sdk.aio.models, "generate_content_stream", observed):
            wire = await self.stream([packet(candidate(0, {"text": "partial"}, reason="MAX_TOKENS")),
                                      packet(candidate(0, reason="STOP")),
                                      packet(candidate(0, {"text": "never-read"}))])
        self.assertIn("error", wire[-1])
        self.assertEqual((reads, closes), (2, 1))

    async def test_authoritative_provider_errors_are_not_replaced(self):
        first = packet(candidate(0, {"text": "early"}, reason="MAX_TOKENS"))
        for tail, code, message in (
            ({"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "upstream quota"}}, 429, "upstream quota"),
            (packet(candidate(0, reason="MALFORMED_FUNCTION_CALL")), 502, "MALFORMED_FUNCTION_CALL"),
            ({**packet(candidate(0, reason="STOP")),
              "promptFeedback": {"blockReason": "SAFETY", "blockReasonMessage": "blocked upstream"}},
             400, "blocked upstream"),
        ):
            with self.subTest(tail=tail):
                wire = await self.stream([first, tail])
                err, = [r["error"] for r in wire if "error" in r]
                self.assertIn(message, err["message"])
                self.assertEqual(err["code"], code)
