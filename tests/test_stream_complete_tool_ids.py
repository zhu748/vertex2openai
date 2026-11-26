"""Validate complete tool IDs before they become executable SSE output."""
from contextlib import asynccontextmanager
import json
from unittest.mock import patch

import httpx
import pytest
from google import genai
from google.genai import types

from signature_store import SignatureRecord, SignatureState, signature_store
from test_express_candidate_terminal import candidate, packet
from test_parallel_tool_controls import _request
from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from upstreams import cookie_delivery, express_sdk


FLASH = "gemini-3-flash-preview"
LITE = "gemini-3.1-flash-lite"  # The Vertex argument assembler is intentionally off.


def call(ident, city, sig):
    fields = {"name": "weather", "args": {"city": city}}
    if ident is not None:
        fields["id"] = ident
    return {"functionCall": fields, "thoughtSignature": sig}


def test_complete_call_guard_is_bounded_even_when_provider_omits_ids():
    from response_validation import MAX_TOOL_CALLS, StreamToolCallIds, ToolStreamError

    seen = StreamToolCallIds()
    for _ in range(MAX_TOOL_CALLS):
        seen.observe(None)
    with pytest.raises(ToolStreamError):
        seen.observe(None)


class StreamCompleteToolIdsRoutes(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.documents = []

        def respond(request):
            body = json.loads(request.content)
            self.sent.append(body)
            if "variables" in body:
                records = [json.dumps({"results": [{"data": doc}]}) for doc in self.documents]
                return httpx.Response(200, text="\n".join(records), headers={"content-type": "application/json"})
            return httpx.Response(200, text="".join(f"data: {json.dumps(doc)}\n\n" for doc in self.documents),
                                  headers={"content-type": "text/event-stream"})

        transport = httpx.MockTransport(respond)
        sdk = genai.Client(vertexai=True, api_key="offline-fixture",
                           http_options=types.HttpOptions(async_client_args={"transport": transport}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        studio = httpx.AsyncClient(transport=transport)
        self.addAsyncCleanup(studio.aclose)

        @asynccontextmanager
        async def open_studio(*args, **kwargs):
            yield studio

        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        self.enterContext(patch.object(cookie_delivery, "open_upstream_client", open_studio))

    async def send(self, model, protocol="openai", n=1):
        path, body = _request(protocol, "auto", stream=True)
        body["model"] = model
        if n > 1:
            body["n"] = n
        self.sent.clear()
        response = await self.client.post(path, json=body)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(self.sent), 1)
        if self.channel == "api_key":
            function_config = self.sent[0].get("toolConfig", {}).get("functionCallingConfig", {})
            flag = function_config.get("streamFunctionCallArguments", function_config.get("stream_function_call_arguments"))
            self.assertEqual(bool(flag), model == FLASH)
        return chunks(response.text)

    @staticmethod
    def openai_events(frames):
        calls = [(choice["index"], call["id"]) for frame in frames
                 for choice in frame.get("choices", []) for call in choice["delta"].get("tool_calls", [])]
        terms = [choice["finish_reason"] for frame in frames
                 for choice in frame.get("choices", []) if choice.get("finish_reason")]
        return calls, terms

    async def test_duplicate_complete_calls_never_finish_successfully_or_cache_second_bytes(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for model in (LITE, FLASH):
                for protocol in ("openai", "claude"):
                    for split in (False, True):
                        with self.subTest(channel=channel, model=model, protocol=protocol, split=split):
                            ident = f"duplicate-{channel}-{model}-{protocol}-{split}"
                            first, later = call(ident, "Paris", "AQI="), call(ident, "Tokyo", "AgM=")
                            self.documents = ([packet(candidate(0, first)), packet(candidate(0, later, reason="STOP"))]
                                              if split else [packet(candidate(0, first, later, reason="STOP"))])
                            frames = await self.send(model, protocol)
                            errors = [f["error"] for f in frames if "error" in f]
                            self.assertEqual(len(errors), 1, frames)
                            self.assertIn("重复", errors[0]["message"])
                            if protocol == "openai":
                                calls, terms = self.openai_events(frames)
                                self.assertEqual((calls, terms), ([(0, ident)] if split else [], []))
                            else:
                                self.assertFalse(any(f.get("type") == "message_stop" or
                                                     f.get("content_block", {}).get("type") == "tool_use"
                                                     for f in frames))
                            record = SignatureRecord(SignatureState.SIGNED, b"\x01\x02") if split else None
                            self.assertEqual(signature_store.get_record(ident), record)

    async def test_one_call_can_still_complete_from_repeated_id_fragments(self):
        p1 = {"functionCall": {"id": "streamed", "name": "weather", "willContinue": True,
                               "partialArgs": [{"jsonPath": "$.city", "stringValue": "Pa", "willContinue": True}]}}
        p2 = {"functionCall": {"id": "streamed", "partialArgs": [{"jsonPath": "$.city", "stringValue": "ris"}]},
              "thoughtSignature": "AQI="}
        self.documents = [packet(candidate(0, p1)), packet(candidate(0, p2, reason="STOP"))]
        frames = await self.send(FLASH)
        self.assertFalse(any("error" in f for f in frames), frames)
        self.assertEqual(self.openai_events(frames), ([(0, "streamed")], ["tool_calls"]))
        payload, = [call for frame in frames for choice in frame.get("choices", [])
                    for call in choice["delta"].get("tool_calls", [])]
        self.assertEqual(json.loads(payload["function"]["arguments"]), {"city": "Paris"})
        self.assertEqual(payload["extra_content"]["google"]["thought_signature"], "AQI=")

    async def test_complete_distinct_and_missing_ids_and_independent_choices(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for ids in (("A", "B"), (None, None)):
                with self.subTest(channel=channel, ids=ids):
                    self.documents = [packet(candidate(0, call(ids[0], "Paris", "AQI="))),
                                      packet(candidate(0, call(ids[1], "Tokyo", "AgM="), reason="STOP"))]
                    frames = await self.send(LITE)
                    self.assertFalse(any("error" in f for f in frames), frames)
                    calls, terms = self.openai_events(frames)
                    public_ids = [ident for _, ident in calls]
                    self.assertEqual(terms, ["tool_calls"])
                    self.assertEqual(len(set(public_ids)), 2)
                    self.assertTrue(all(public_ids))
                    if ids[0]:
                        self.assertEqual(public_ids, list(ids))
        self.channel = "api_key"
        self.documents = [packet(candidate(0, call("reused", "Paris", "AQI="), reason="STOP"),
                                 candidate(1, call("reused", "Tokyo", "AgM="), reason="STOP"))]
        frames = await self.send(MODEL, n=2)
        self.assertFalse(any("error" in f for f in frames), frames)
        self.assertEqual(self.openai_events(frames), ([(0, "reused"), (1, "reused")], ["tool_calls", "tool_calls"]))
