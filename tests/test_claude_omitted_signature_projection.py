"""Claude thinking omission must not discard genuine Gemini tool signatures."""
import base64
import json
import unittest
from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import patch

import signature_origin
import thought_signatures
from claude_protocol import ClaudeStreamState
from claude_signature_projection import OmittedClaudeProjection
from fastapi.responses import StreamingResponse
from routes.claude_api import _claude_stream_response
from signature_store import SignatureStore
from test_claude_buffered_tool_order import read_events, reconstruct
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_response

MODEL = "gemini-3-flash-preview"
THINKING = {"type": "enabled", "budget_tokens": 1024, "display": "omitted"}
HIDDEN = "provider's private reasoning"
PREFIX = [{"role": "user", "content": "Get the nearby temperatures."}]
CALLS = (("toolu_first", b"\xfb\xff\0\x01"), ("toolu_unsigned", None), ("toolu_third", b"third bytes"))


def new_cache(stack):
    cache = SignatureStore()
    for module in (signature_origin, thought_signatures, cookie_response):
        stack.enter_context(patch.object(module, "signature_store", cache))
    return cache


def provider_calls(entries=CALLS):
    return [
        {"functionCall": {"id": ident, "name": "weather", "args": {"i": i}},
         **({"thoughtSignature": base64.b64encode(signature).decode()} if signature else {})}
        for i, (ident, signature) in enumerate(entries)
    ]


def signature_from_wire(part):
    raw = part.get("thoughtSignature")
    if not raw or raw == "skip_thought_signature_validator":
        return None
    sig = base64.urlsafe_b64decode(raw)
    return None if sig == b"skip_thought_signature_validator" else sig


class ClaudeOmittedSignatureRoutes(SchemaRouteHarness):
    async def post(self, *, messages=None, omitted=True, session="client-session", stream=False):
        body = {
            "model": MODEL, "max_tokens": 1024, "messages": messages or PREFIX, "stream": stream,
            "tools": [{"name": "weather", "input_schema": {"type": "object"}}],
            "metadata": {"user_id": session},
        }
        if omitted:
            body["thinking"] = THINKING
        return await self.client.post("/v1/messages", json=body)

    def follow_up(self, public, prefix=PREFIX):
        return [*prefix, {"role": "assistant", "content": public},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": block["id"], "content": "ok"}
                    for block in public if block["type"] == "tool_use"]}]

    async def follow_wire(self, public, **kwargs):
        self.sent.clear()
        self.reply = [{"text": "done"}]
        response = await self.post(messages=self.follow_up(public, kwargs.pop("prefix", PREFIX)), **kwargs)
        # With strict replay the adapter may reject an unproven turn locally
        # rather than passing through a request with missing required signatures.
        if response.status_code != 200:
            self.assertEqual(response.status_code, 400, response.text)
            self.assertEqual(self.sent, [])
            return []
        self.assertEqual(len(self.sent), 1)
        wire = self.sent[0].get("variables", self.sent[0])
        self.assertNotIn(HIDDEN, json.dumps(wire))
        return [(p["functionCall"]["id"], signature_from_wire(p))
                for turn in wire["contents"] for p in turn["parts"] if "functionCall" in p]

    async def test_real_calls_replay_unchanged_in_six_modes(self):
        for channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                with self.subTest(channel=channel, mode=mode), ExitStack() as stack:
                    new_cache(stack)
                    self.channel = channel
                    self.settings["fake_streaming"] = mode == "fake"
                    self.reply = [{"text": HIDDEN, "thought": True}, {"text": "Checking."}, *provider_calls()]
                    stream = mode != "json"
                    response = await self.post(stream=stream)
                    self.assertEqual(response.status_code, 200, response.text)
                    if stream:
                        events = read_events(response.text)
                        self.assertEqual(events[-1]["type"], "message_stop")
                        public = reconstruct(self, events)
                    else:
                        public = response.json()["content"]
                    self.assertEqual([b["type"] for b in public], ["text", "tool_use", "tool_use", "tool_use"])
                    self.assertEqual([b["id"] for b in public if b["type"] == "tool_use"], [i for i, _ in CALLS])
                    self.assertNotIn(HIDDEN, response.text)
                    for _, sig in CALLS:
                        if sig:
                            self.assertNotIn(base64.b64encode(sig).decode(), response.text)
                    # Verify migration between Gemini transports, not just a
                    # fast path reading the local response object.
                    self.channel = "web_proxy" if channel == "api_key" else "api_key"
                    self.assertEqual(await self.follow_wire(public), list(CALLS))

    async def test_tool_only_projection_has_no_ordinary_parts_to_describe(self):
        for channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                for entries in (CALLS[:1], CALLS):
                    with self.subTest(channel=channel, mode=mode, calls=len(entries)), ExitStack() as stack:
                        new_cache(stack)
                        self.channel = channel
                        self.settings["fake_streaming"] = mode == "fake"
                        # Express advertises only part_order for tool-only
                        # completions. No text/summary Part is required.
                        self.reply = provider_calls(entries)
                        stream = mode != "json"
                        response = await self.post(stream=stream)
                        self.assertEqual(response.status_code, 200, response.text)
                        public = (reconstruct(self, read_events(response.text)) if stream
                                  else response.json()["content"])
                        self.assertEqual([b["type"] for b in public], ["tool_use"] * len(entries))
                        self.assertEqual(await self.follow_wire(public), list(entries))

    async def test_edited_scope_shape_mode_and_ambiguous_projections_never_borrow(self):
        with ExitStack() as stack:
            new_cache(stack)
            self.reply = [{"text": HIDDEN, "thought": True}, {"text": "Check"}, {"text": "ing."}, *provider_calls()]
            response = await self.post()
            public = response.json()["content"]
            self.assertEqual([b["type"] for b in public[:2]], ["text", "text"])
            self.assertEqual(await self.follow_wire(public), list(CALLS))
            edited = deepcopy(public)
            edited[0]["text"] = "Changed text"
            args = deepcopy(public)
            args[2]["input"]["i"] = 999
            merged = [{"type": "text", "text": "Checking."}, *public[2:]]
            for control, kwargs in (
                (edited, {}), (args, {}), (merged, {}),
                (public, {"session": "unrelated-session"}), (public, {"omitted": False}),
                (public, {"prefix": [{"role": "user", "content": "Unrelated question."}]}),
            ):
                with self.subTest(control=control, kwargs=kwargs):
                    self.assertFalse(any(sig for _, sig in await self.follow_wire(control, **kwargs)))
            # Appear identical to the harness despite incompatible real bytes.
            entries = tuple((i, sig and b"different-" + sig) for i, sig in CALLS)
            self.reply = [{"text": "different hidden text", "thought": True}, {"text": "Check"},
                          {"text": "ing."}, *provider_calls(entries)]
            self.assertEqual((await self.post()).json()["content"], public)
            self.assertFalse(any(sig for _, sig in await self.follow_wire(public)))

    async def test_a_shown_thinking_block_cannot_be_reinterpreted_as_omitted(self):
        with ExitStack() as stack:
            new_cache(stack)
            self.reply = [{"text": HIDDEN, "thought": True}, *provider_calls()]
            response = await self.post(omitted=False)
            public = response.json()["content"]
            self.assertEqual(public[0]["type"], "thinking")
            for omitted in (False, True):
                self.assertFalse(any(sig for _, sig in await self.follow_wire(public[1:], omitted=omitted)))


class ClaudeProjectionTerminals(unittest.IsolatedAsyncioTestCase):
    async def test_failed_incomplete_and_cancelled_streams_never_publish(self):
        for fault in ("error", "no-done", "cancel", None):
            with self.subTest(fault=fault), ExitStack() as stack:
                cache = new_cache(stack)
                projection = OmittedClaudeProjection(b"already scoped prefix", {})
                state = ClaudeStreamState("display", omit_thinking=True, projection=projection)
                call = {
                    "index": 0, "id": "fixture-slot", "type": "function",
                    "function": {"name": "weather", "arguments": "{}"},
                    "extra_content": {"google": {"thought_signature": "Zml4dHVyZQ=="}},
                }

                def frame(delta, reason=None):
                    return "data: " + json.dumps({"id": "same-response", "choices": [
                        {"index": 0, "delta": delta, "finish_reason": reason}]}) + "\n\n"

                async def source(call=call, fault=fault):
                    yield frame({"reasoning_content": HIDDEN})
                    yield frame({"tool_calls": [call]})
                    yield frame({}, "tool_calls")
                    if fault == "error":
                        yield 'data: {"error":{"code":502,"message":"simulated upstream fail"}}\n\n'
                    if fault != "no-done":
                        yield "data: [DONE]\n\n"

                response = _claude_stream_response(
                    StreamingResponse(source()), "display", omit_thinking=True, state=state)
                events = []
                async for event in response.body_iterator:
                    events.extend(read_events(event))
                    if fault == "cancel" and events[-1]["type"] == "message_stop":
                        await response.body_iterator.aclose()
                        break
                self.assertEqual(events[-1]["type"], "error" if fault == "error" else "message_stop")
                self.assertEqual(len(cache._data), 1 if fault is None else 0)
                self.assertFalse(projection.ready)
