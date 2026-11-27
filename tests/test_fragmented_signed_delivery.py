"""A signed empty thought cannot impersonate a fragmented tool call's closer."""
import asyncio
import base64
import json
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

from admission import generation_limiter
from replay_event import ReplayEvent
import resumable
from routes import chat_api
from sdk_stream import capture_sdk_response
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import express_sdk


MODEL = "gemini-3-flash-preview"
PATH = "/v1/chat/completions"
CALL_ID = "provider-call"
CALL_BYTES = b"\xfb\xff\0provider-call"
THOUGHT_BYTES = b"\xfd\x80\0provider-empty-thought"
CALL_SIGNATURE = base64.b64encode(CALL_BYTES).decode()
THOUGHT_SIGNATURE = base64.b64encode(THOUGHT_BYTES).decode()
PROMPT = [{"role": "user", "content": "Offline tool fixture."}]
KEY = [(b"x-resumable", b"true"), (b"idempotency-key", b"one-assisted-turn")]


def event(part, finish=None):
    candidate = {"index": 0, "content": {"role": "model", "parts": [part]}}
    if finish:
        candidate["finishReason"] = finish
    return b"data: " + json.dumps({"candidates": [candidate]}).encode() + b"\n\n"


def decode_signature(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class FragmentedSignedDelivery(SchemaRouteHarness):
    async def test_completed_call_and_separate_empty_thought_survive_disconnect_replay_and_follow_up(self):
        self.settings.update(fake_streaming=False, retry_max=0, cross_channel_fallback=False,
                             express_location="global")
        self.key_manager.get_express_record = lambda **_: {"key": "offline-key", "project_id": "offline-project"}
        self.channel = "api_key"
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        for enter in (patch.object(chat_api, "resumable_store", store),
                      patch.object(chat_api, "resumable_enabled", return_value=True),
                      patch.object(chat_api, "SETUP_FAST_WINDOW_SECONDS", 0)):
            self.enterContext(enter)

        header = {"functionCall": {"name": "lookup", "id": CALL_ID, "willContinue": True,
                    "partialArgs": [{"jsonPath": "$.query", "stringValue": "Par", "willContinue": True}]},
                  "thoughtSignature": CALL_SIGNATURE}
        empty = {"text": "", "thought": True, "thoughtSignature": THOUGHT_SIGNATURE,
                 "functionCall": {"name": "", "id": "", "args": {}, "partialArgs": [], "willContinue": False}}
        tail = {"functionCall": {"id": CALL_ID, "willContinue": False,
                "partialArgs": [{"jsonPath": "$.query", "stringValue": "is"}]}}
        paused, release, first_headers, follower_headers = (asyncio.Event() for _ in range(4))
        calls, closes = [], []

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield event(header)
                paused.set()
                await release.wait()
                yield event(empty)
                yield event(tail, "STOP")

            async def aclose(self):
                closes.append(True)

        def provider(request):
            calls.append(json.loads(request.content))
            if request.url.path.endswith(":streamGenerateContent"):
                return httpx.Response(200, stream=Body(), headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json={"candidates": [{"index": 0, "finishReason": "STOP",
                "content": {"role": "model", "parts": [{"text": "Tool result accepted."}]}}]})

        sdk = genai.Client(vertexai=True, api_key="offline-key", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(provider),
                               "event_hooks": {"response": [capture_sdk_response]}}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        body = {"model": MODEL, "stream": True, "messages": PROMPT, "tool_choice": "required",
                "tools": [{"type": "function", "function": {"name": "lookup",
                    "parameters": {"type": "object", "properties": {"query": {"type": "string"}}}}}]}
        first, follower = (Peer(PATH, payload=body, headers=KEY) for _ in range(2))

        async def first_started(message):
            if message["type"] == "http.response.start":
                first_headers.set()

        async def follower_send(message):
            if message["type"] == "http.response.start":
                follower_headers.set()
            elif message["type"] == "http.response.body" and not message.get("more_body"):
                await follower.drop()

        baseline = generation_limiter.active
        leader_job, follower_job = asyncio.create_task(first.run(first_started)), None
        try:
            await asyncio.wait_for(first_headers.wait(), 2)
            await asyncio.wait_for(paused.wait(), 2)
            buf, = store._buffers.values()
            await first.drop()
            await asyncio.wait_for(leader_job, 2)
            self.assertEqual((buf.done, buf.has_live_delivery, generation_limiter.active), (False, False, baseline + 1))
            self.assertFalse(any(CALL_ID.encode() in m.get("body", b"") for m in first.messages))
            follower_job = asyncio.create_task(follower.run(follower_send))
            await asyncio.wait_for(follower_headers.wait(), 2)
            self.assertEqual((first.receipt, len(calls)), (buf.id, 1))
        finally:
            release.set()
            await asyncio.wait_for(asyncio.gather(leader_job, *([follower_job] if follower_job is not None else [])), 2)

        self.assertEqual((follower.receipt, buf.status, buf.fully_delivered, buf.has_live_delivery),
                         (buf.id, "done", False, False))
        self.assertEqual((generation_limiter.active, closes), (baseline, [True]))
        final = Peer(PATH, payload=body, headers=KEY)
        await asyncio.wait_for(final.run(), 2)
        self.assertEqual((final.receipt, buf.fully_delivered, len(calls)), (buf.id, True, 1))
        self.assertEqual(dict(final.messages[0]["headers"])[b"x-resumable-replay"], b"1")
        self.assertTrue(calls[0]["toolConfig"]["functionCallingConfig"]["stream_function_call_arguments"])

        frames = [ReplayEvent(m["body"].decode()).data() for m in final.messages if m.get("body")]
        self.assertEqual(frames.count("[DONE]"), 1)
        rows = [json.loads(frame) for frame in frames if frame not in ("", "[DONE]")]
        self.assertTrue(all("error" not in row for row in rows))
        choices = [choice for row in rows for choice in row.get("choices", ())]
        self.assertEqual([choice["finish_reason"] for choice in choices if choice["finish_reason"]], ["tool_calls"])
        deltas = [choice["delta"] for choice in choices]
        tool, = [call for delta in deltas for call in delta.get("tool_calls", ())]
        self.assertEqual((tool["id"], tool["function"]["name"], json.loads(tool["function"]["arguments"])),
                         (CALL_ID, "lookup", {"query": "Paris"}))
        self.assertEqual(decode_signature(tool["extra_content"]["google"]["thought_signature"]), CALL_BYTES)
        extra, = [delta["extra_content"] for delta in deltas if "extra_content" in delta]
        ordinary, = extra["google"]["ordinary_parts"]
        self.assertEqual((ordinary["kind"], ordinary["text"], decode_signature(ordinary["thought_signature"])),
                         ("thought", "", THOUGHT_BYTES))
        self.assertEqual(extra["google"]["part_order"], [{"type": "tool_call", "index": 0},
                                                       {"type": "ordinary", "index": 0}])

        tool = {k: v for k, v in tool.items() if k != "index"}
        next_turn = {**body, "stream": False, "messages": [*PROMPT,
            {"role": "assistant", "content": "", "tool_calls": [tool], "extra_content": extra},
            {"role": "tool", "tool_call_id": CALL_ID, "content": "offline result"}]}
        answer = await self.client.post(PATH, json=next_turn)
        self.assertEqual(answer.status_code, 200, answer.text)
        self.assertEqual(len(calls), 2)  # exactly one original generation and one new logical turn
        turns = calls[1]["contents"]
        signed, thought = turns[1]["parts"]
        self.assertEqual((signed["functionCall"]["id"], decode_signature(signed["thoughtSignature"])),
                         (CALL_ID, CALL_BYTES))
        self.assertEqual((thought["text"], thought["thought"], decode_signature(thought["thoughtSignature"])),
                         ("", True, THOUGHT_BYTES))
        self.assertEqual(turns[2]["parts"][0]["functionResponse"]["id"], CALL_ID)
