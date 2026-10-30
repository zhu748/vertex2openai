"""Real OpenAI tool argument deltas: append-only JSON and delivery before EOF."""
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import random
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-cookie-deltas-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

import config
from logger import ProxyStats
from models import OpenAIRequest
from response_validation import ToolStreamError
from request_tracking import RequestTrace, current_trace
from runtime_state import app_state
from tool_stream import ToolStreamAssembler, ToolStreamEvent
from upstreams import cookie_proxy, cookie_transport
from upstreams.cookie_response import iter_json_objects
from upstreams.cookie_stream_diagnostics import CookieStreamDiagnostics


def feed(stream, **fields):
    parts, _ = stream.restore_wire_parts([{"functionCall": fields}])
    return [part for part in parts if isinstance(part, ToolStreamEvent)]


def argument(events, index=0):
    return "".join(event.data.get("arguments", "") for event in events
                   if event.kind == "tool_delta" and event.data["index"] == index)


def fragment(path, value, field="stringValue", continuing=False):
    return {"jsonPath": path, field: value, "willContinue": continuing}


class JsonDeltaTests(unittest.TestCase):
    def test_string_prefix_arrives_before_call_is_complete(self):
        stream = ToolStreamAssembler(emit_deltas=True)
        events = feed(stream, name="reply", id="a", willContinue=True)
        events += feed(stream, partialArgs=[fragment("$.text", "first-", continuing=True)], willContinue=True)
        self.assertIn("first-", argument(events))
        with self.assertRaises(ValueError):
            json.loads(argument(events))
        self.assertFalse(any(event.kind == "tool_end" for event in events))
        events += feed(stream, partialArgs=[fragment("$.text", "last")])
        self.assertEqual(json.loads(argument(events)), {"text": "first-last"})
        self.assertEqual(sum(event.kind == "tool_end" for event in events), 1)
        stream.validate_end()

    def test_nested_containers_do_not_close_before_late_siblings_arrive(self):
        stream = ToolStreamAssembler(emit_deltas=True)
        events = feed(stream, name="reply", willContinue=True)
        for path, value in (("$.a.x", "one"), ("$.b", "outer"), ("$.a.y", "two")):
            events += feed(stream, partialArgs=[fragment(path, value)], willContinue=True)
            self.assertFalse(any(event.kind == "tool_end" for event in events))
        events += feed(stream)
        self.assertEqual(json.loads(argument(events)), {"a": {"x": "one", "y": "two"}, "b": "outer"})

    def test_array_gaps_reorder_and_json_escapes_are_lossless(self):
        stream = ToolStreamAssembler(emit_deltas=True)
        events = feed(stream, name="reply", willContinue=True)
        for item in (fragment("$.rows[1]", "later"), fragment("$.rows[0]", '"\\\n中😺'),
                     fragment("$.off", False, "boolValue"), fragment("$.zero", 0, "numberValue"),
                     fragment("$.nil", 0, "nullValue")):
            events += feed(stream, partialArgs=[item], willContinue=True)
        events += feed(stream)
        self.assertEqual(json.loads(argument(events)), {"rows": ['"\\\n中😺', "later"], "off": False, "zero": 0, "nil": None})

    def test_split_surrogates_path_only_closure_and_parallel_late_ids(self):
        stream = ToolStreamAssembler(emit_deltas=True)
        events = feed(stream, name="first", willContinue=True)
        events += feed(stream, name="first", id="a", partialArgs=[fragment("$.text", "\ud83d", continuing=True)], willContinue=True)
        events += feed(stream, name="second", id="b", willContinue=True)
        events += feed(stream, id="b", partialArgs=[fragment("$.n", 2, "numberValue")])
        events += feed(stream, id="a", partialArgs=[fragment("$.text", "\ude3a", continuing=True)], willContinue=True)
        events += feed(stream, id="a", partialArgs=[{"jsonPath": "$.text"}])
        self.assertEqual(json.loads(argument(events, 0)), {"text": "😺"})
        self.assertEqual(json.loads(argument(events, 1)), {"n": 2})
        self.assertEqual([event.data["id"] for event in events if event.kind == "tool_delta" and event.data.get("id")], ["a", "b"])
        stream.validate_end()

    def test_full_calls_and_empty_objects_keep_single_name_and_id(self):
        for args in ({}, {"x": "complete", "nested": []}):
            stream = ToolStreamAssembler(emit_deltas=True)
            events = feed(stream, name="reply", id="a", args=args)
            self.assertEqual(json.loads(argument(events)), args)
            self.assertEqual(sum(bool(event.data.get("name")) for event in events if event.kind == "tool_delta"), 1)
            self.assertEqual(sum(bool(event.data.get("id")) for event in events if event.kind == "tool_delta"), 1)

    def test_late_signature_is_retained_without_repeating_name_or_id(self):
        stream = ToolStreamAssembler(emit_deltas=True)
        events = feed(stream, name="reply", id="a", willContinue=True)
        events += feed(stream, id="a", partialArgs=[fragment("$.text", "signed")], willContinue=True)
        ready, _ = stream.restore_wire_parts([{"functionCall": {"id": "a"}, "thoughtSignature": "c2ln"}])
        events += ready
        end = next(event.data for event in events if event.kind == "tool_end")
        self.assertEqual((end["thought_signature"], end["id_sent"]), ("c2ln", True))
        self.assertEqual(json.loads(argument(events)), {"text": "signed"})

    def test_invalid_or_unclosed_data_never_emits_a_tool_end(self):
        for items in ([fragment("$.text", "unfinished", continuing=True)],
                      [fragment("$.rows[1]", "gap")], [fragment("$.n", float("inf"), "numberValue")]):
            stream = ToolStreamAssembler(emit_deltas=True)
            feed(stream, name="reply", willContinue=True)
            with self.subTest(items=items), self.assertRaises(ToolStreamError):
                feed(stream, partialArgs=items)

    def test_random_interleaved_object_and_array_paths_roundtrip(self):
        rng = random.Random(813)
        for _ in range(60):
            stream = ToolStreamAssembler(emit_deltas=True)
            events = feed(stream, name="reply", willContinue=True)
            rows = [{"text": f'{i}:中文"\\😺'} for i in range(4)]
            expected = {"rows": rows, "other": "outside", "nested": {"off": False, "zero": 0}}
            items = [fragment(f"$.rows[{i}].text", row["text"]) for i, row in enumerate(rows)]
            items += [fragment("$.other", "outside"), fragment("$.nested.off", False, "boolValue"),
                      fragment("$.nested.zero", 0, "numberValue")]
            rng.shuffle(items)
            for item in items:
                events += feed(stream, partialArgs=[item], willContinue=True)
            events += feed(stream)
            self.assertEqual(json.loads(argument(events)), expected)


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_framing_diagnostics_distinguish_early_bytes_from_complete_json(self):
        trace = RequestTrace("/v1/chat/completions")
        token = current_trace.set(trace)
        body = {"variables": {"tools": [{"functionDeclarations": [{"name": "reply"}]}],
                             "toolConfig": {"functionCallingConfig": {"streamFunctionCallArguments": True}}}}
        try:
            diagnostics = CookieStreamDiagnostics(body)
            class Source:
                async def aiter_text(self):
                    yield '{"results": [{"data":'
                    self.assertion()
                    yield '{"candidates": []}}]}'
                def assertion(self):
                    self_outer.assertEqual(diagnostics.data["text_chunks"], 1)
                    self_outer.assertEqual(diagnostics.data["json_objects"], 0)
                    self_outer.assertNotIn("first_reply", trace.timings)
            self_outer = self
            async for obj in iter_json_objects(Source(), on_text_chunk=diagnostics.text):
                diagnostics.object(obj)
            self.assertEqual((diagnostics.data["text_chunks"], diagnostics.data["json_objects"]), (2, 1))
            self.assertNotIn("first_reply", trace.timings)
            self.assertNotIn("first_upstream", trace.timings)
            self.assertLessEqual(diagnostics.data["cookie_first_data"], diagnostics.data["cookie_first_json"])
        finally:
            current_trace.reset(token)

    async def test_raw_data_and_empty_json_do_not_disable_first_reply_timeout(self):
        trace = RequestTrace("/v1/chat/completions")
        token = current_trace.set(trace)
        closed = asyncio.Event()
        class Source(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'{"results": []}\n'
                await asyncio.Event().wait()
            async def aclose(self):
                closed.set()
        async def handler(request):
            return httpx.Response(200, stream=Source())
        body = {"variables": {"tools": [{"functionDeclarations": [{"name": "reply"}]}],
                             "toolConfig": {"functionCallingConfig": {"streamFunctionCallArguments": True}}}}
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                with mock.patch.object(cookie_transport, "timeout_seconds", return_value=.02):
                    events = [item async for item in cookie_transport.stream_events(client, {}, body, model="gemini-3.8-flash", emit_tool_deltas=True)]
            self.assertEqual(events[-1][0], "retryable_error")
            self.assertEqual(events[-1][1].status_code, 504)
            self.assertTrue(closed.is_set())
            self.assertEqual(trace.data["cookie_tool_stream"]["json_objects"], 1)
            self.assertNotIn("first_reply", trace.timings)
        finally:
            current_trace.reset(token)

    async def test_client_receives_argument_text_while_upstream_is_still_generating(self):
        release, closed = asyncio.Event(), asyncio.Event()
        def wire(fields, finish=None):
            candidate = {"content": {"parts": [{"functionCall": fields}]}}
            if finish:
                candidate["finishReason"] = finish
            return json.dumps({"results": [{"data": {"candidates": [candidate]}}]}).encode() + b"\n"
        class Source(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield wire({"name": "reply", "willContinue": True})
                yield wire({"partialArgs": [fragment("$.text", "early-", continuing=True)], "willContinue": True})
                await release.wait()
                yield wire({"partialArgs": [fragment("$.text", "late")]}, "STOP")
            async def aclose(self):
                closed.set()
        async def handler(request):
            return httpx.Response(200, stream=Source())
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.addAsyncCleanup(client.aclose)
        @asynccontextmanager
        async def opened(*args, **kwargs):
            yield client
        settings = dict(config.DEFAULT_SETTINGS, fake_streaming=False, proxy_url="", retry_max=0)
        request = OpenAIRequest(model="gemini-3.8-flash", stream=True,
            messages=[{"role": "user", "content": "hello"}], tool_choice="required",
            tools=[{"type": "function", "function": {"name": "reply", "parameters": {"type": "object"}}}])
        with mock.patch.object(cookie_proxy, "open_upstream_client", opened), \
             mock.patch.object(cookie_proxy, "build_headers", return_value={"authorization": "offline"}), \
             mock.patch.object(cookie_proxy, "stats", ProxyStats()), \
             mock.patch.object(cookie_proxy.credential_manager, "record_cookie_success"), \
             mock.patch.object(app_state, "get_effective_settings", return_value=settings), \
             mock.patch.object(app_state, "get_setting", side_effect=lambda k, d=None: settings.get(k, d)), \
             mock.patch.object(cookie_transport, "timeout_seconds", return_value=.1):
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(request,
                SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False)),
                credential_record={"id": "offline", "cookie": "SAPISID=offline", "project_id": "offline"})
            iterator = result.body_iterator.__aiter__()
            pieces = []
            try:
                async with asyncio.timeout(1):
                    while not any("early-" in piece for piece in pieces):
                        pieces.append(await anext(iterator))
                self.assertFalse(release.is_set())
                self.assertFalse(closed.is_set())
                self.assertNotIn('"finish_reason": "tool_calls"', "".join(pieces))
                release.set()
                pieces += [piece async for piece in iterator]
            finally:
                release.set()
                await iterator.aclose()
        body = "".join(pieces)
        chunks = [json.loads(line[6:]) for line in body.splitlines() if line.startswith("data: {")]
        deltas = [call for chunk in chunks for choice in chunk.get("choices", [])
                  for call in choice.get("delta", {}).get("tool_calls", [])]
        self.assertEqual(json.loads("".join(call.get("function", {}).get("arguments", "") for call in deltas)), {"text": "early-late"})
        self.assertEqual(sum(bool(call.get("id")) for call in deltas), 1)
        self.assertEqual(sum(bool(call.get("function", {}).get("name")) for call in deltas), 1)
        self.assertIn('"finish_reason": "tool_calls"', body)
        self.assertTrue(closed.is_set())


class DiagnosticTests(unittest.TestCase):
    def test_upstream_full_objects_and_actual_fragments_are_counted_separately(self):
        trace = RequestTrace("/v1/chat/completions")
        token = current_trace.set(trace)
        try:
            trace.data["first_reply_timeout_seconds"] = 0
            diagnostics = CookieStreamDiagnostics({"variables": {"tools": [{"functionDeclarations": [{}]}],
                "toolConfig": {"functionCallingConfig": {"streamFunctionCallArguments": True}}}})
            diagnostics.object({"results": [{"data": {"candidates": [{"content": {"parts": [
                {"functionCall": {"name": "reply", "args": {"text": "private reply"}}}]}}]}}]})
            diagnostics.finish()
            self.assertEqual(diagnostics.data["full_tool_parts"], 1)
            self.assertEqual(diagnostics.data["fragment_parts"], 0)
            messages = "\n".join(event["message"] for event in trace.events)
            self.assertIn("仅返回完整工具对象", messages)
            self.assertIn("首回复超时：关闭", messages)
            self.assertNotIn("private reply", messages)
            diagnostics.object({"results": [{"data": {"candidates": [{"content": {"parts": [
                {"functionCall": {"willContinue": True, "partialArgs": [fragment("$.text", "private fragment")]}}]}}]}}]})
            self.assertEqual((diagnostics.data["fragment_parts"], diagnostics.data["argument_fragments"]), (1, 1))
            self.assertNotIn("private fragment", "\n".join(event["message"] for event in trace.events))
        finally:
            current_trace.reset(token)

    def test_no_tools_or_empty_native_defaults_do_not_create_tool_diagnostics(self):
        trace = RequestTrace("/v1/chat/completions")
        token = current_trace.set(trace)
        try:
            for variables in ({}, {"tools": None}, {"tools": [], "toolConfig": None}):
                diagnostics = CookieStreamDiagnostics({"variables": variables})
                diagnostics.text("{}")
                diagnostics.object({})
                diagnostics.finish()
            self.assertNotIn("cookie_tool_stream", trace.data)
        finally:
            current_trace.reset(token)


if __name__ == "__main__":
    unittest.main()
