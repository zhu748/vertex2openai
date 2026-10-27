"""Tool declaration identity, history replay and Claude stream integrity."""
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-tool-integrity-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.responses import JSONResponse, StreamingResponse
from google.genai import types

import anti_truncation
import claude_protocol as cp
from generation_config import create_generation_config
from message_processing import create_gemini_prompt, process_gemini_response_to_openai_dict
from models import OpenAIRequest
from request_validation import validate_openai_request
from response_validation import IncompleteResponseError
from routes import claude_api
from schema_validation import SchemaValidationError
from tool_arguments import parse_tool_arguments
from tool_policy import BUILTIN_SEARCH_NAMES
from upstreams.cookie_request import build_request_body, _convert_messages_to_contents, classify_tool_traffic
from upstreams import cookie_proxy
from upstreams.cookie_response import extract_events
from upstreams.express_response import _build_chunk_payload

MODEL = "gemini-2.5-flash"


def tool(name="lookup", **kwargs):
    return {"type": "function", "function": {"name": name, "parameters": {"type": "object"}, **kwargs}}


def request(**kwargs):
    return OpenAIRequest(model=kwargs.pop("model", MODEL),
                         messages=kwargs.pop("messages", [{"role": "user", "content": "hello"}]), **kwargs)


def call(arguments="{}", *, ident="call-1", name="lookup", **kwargs):
    return {"id": ident, "type": "function", "function": {"name": name, "arguments": arguments}, **kwargs}


def chunk(calls, *, finish=None, **kwargs):
    return {"choices": [{"index": 0, "delta": {"tool_calls": calls}, "finish_reason": finish}], **kwargs}


def frames(events):
    return [json.loads(event.split("data: ", 1)[1]) for event in events]


class ToolSelectionTests(unittest.TestCase):
    def test_every_builtin_alias_remains_custom_when_declared_as_function(self):
        for name in BUILTIN_SEARCH_NAMES:
            for choice in (None, "required", tool(name)):
                with self.subTest(name=name, choice=choice):
                    obj = request(tools=[tool(name)], tool_choice=choice)
                    validate_openai_request(obj)
                    sdk = create_generation_config(obj)
                    cookie = build_request_body("p", MODEL, obj)["variables"]
                    self.assertEqual(sdk["tools"][0]["function_declarations"][0]["name"], name)
                    self.assertEqual(cookie["tools"][0]["functionDeclarations"][0]["name"], name)
                    if isinstance(choice, dict):
                        self.assertEqual(cookie["toolConfig"]["functionCallingConfig"]["allowedFunctionNames"], [name])
                    info = classify_tool_traffic(obj.messages, obj.tools)
                    self.assertEqual(info["custom_names"], [name])
                    self.assertFalse(info["builtin_search"])

    def test_cookie_preflight_and_body_agree_about_custom_search(self):
        obj = request(tools=[tool("search")], tool_choice=tool("search"))
        prepared = cookie_proxy._prepare_cookie_generation(obj, SimpleNamespace(), {
            "id": "account", "cookie": "SAPISID=offline", "project_id": "project"})
        self.assertIsInstance(prepared, cookie_proxy.CookieGeneration)
        self.assertFalse(prepared.force_builtin_search)
        self.assertTrue(prepared.native_custom_tools)

    def test_explicit_builtins_and_undeclared_search_choice_still_work(self):
        for obj in (request(tools=[{"type": "web_search"}]),
                    request(tools=[tool()], tool_choice=tool("google_search"))):
            validate_openai_request(obj)
            self.assertEqual(create_generation_config(obj)["tools"], [{"google_search": {}}])
            self.assertEqual(build_request_body("p", MODEL, obj)["variables"]["tools"], [{"googleSearch": {}}])

    def test_legacy_missing_type_is_consistent_in_both_transports(self):
        obj = request(tools=[{"function": tool()["function"]}], tool_choice={"function": {"name": "lookup"}})
        validate_openai_request(obj)
        sdk = create_generation_config(obj)
        cookie = build_request_body("p", MODEL, obj)["variables"]
        self.assertEqual(sdk["tool_config"]["function_calling_config"]["allowed_function_names"], ["lookup"])
        self.assertEqual(cookie["toolConfig"]["functionCallingConfig"]["allowedFunctionNames"], ["lookup"])

    def test_unknown_or_duplicate_declarations_are_not_silently_changed(self):
        for declarations in ([{"type": "computer_use"}], [tool(), tool()],
                             [{**tool(), "type": "web_search"}]):
            with self.subTest(tools=declarations), self.assertRaises(SchemaValidationError):
                validate_openai_request(request(tools=declarations))

    def test_named_search_function_can_use_synthetic_text_transport(self):
        obj = request(model="gemini-4-argon-antitruncation-stream", tools=[tool("search")])
        prepared, transport = anti_truncation.prepare_transport(
            obj, "gemini-4-argon", {"anti_truncation_mode": "stream"})
        self.assertIsNotNone(transport)
        self.assertEqual(prepared.tools[0], tool("search"))


class HistoryArgumentTests(unittest.TestCase):
    def test_invalid_history_is_rejected_by_validation_and_both_converters(self):
        invalid = ['{"secret":', '[1,2]', '"text"', '{"x":NaN}', '{"x":1e999}',
                   {"x": float("nan")}, {"x": "\ud800"}]
        for args in invalid:
            obj = request(messages=[{"role": "assistant", "tool_calls": [call(args)]}])
            for convert in (lambda: validate_openai_request(obj),
                            lambda: create_gemini_prompt(obj.messages, MODEL),
                            lambda: _convert_messages_to_contents(obj.messages, MODEL)):
                with self.subTest(args=repr(args)), self.assertRaises(SchemaValidationError) as caught:
                    convert()
                self.assertNotIn("secret", str(caught.exception))

    def test_omitted_or_empty_arguments_are_empty_objects(self):
        for value in (None, "", "  ", {}, "{}"):
            self.assertEqual(parse_tool_arguments(value), {})
        self.assertEqual(parse_tool_arguments('{"nested":{"x":[1,true,null]}}'), {"nested": {"x": [1, True, None]}})

    def test_duplicate_ids_rejected_only_within_one_assistant_message(self):
        with self.assertRaises(SchemaValidationError):
            validate_openai_request(request(messages=[{"role": "assistant", "tool_calls": [call(), call()]}]))
        validate_openai_request(request(messages=[{"role": "assistant", "tool_calls": [call()]},
            {"role": "tool", "tool_call_id": "call-1", "content": "ok"},
            {"role": "assistant", "tool_calls": [call()]}]))

    def test_bad_call_id_or_type_fails_before_sdk(self):
        for entry in (call(ident=[]), call(type="computer_use")):
            with self.assertRaises(SchemaValidationError):
                validate_openai_request(request(messages=[{"role": "assistant", "tool_calls": [entry]}]))


class ClaudeTopologyTests(unittest.TestCase):
    def test_sdk_to_claude_to_both_transports_keeps_interleaved_order(self):
        original = [types.Part(text="before"), types.Part(function_call=types.FunctionCall(id="a", name="lookup", args={"n": 1})),
                    types.Part(text="between"), types.Part(function_call=types.FunctionCall(id="b", name="lookup", args={"n": 2})),
                    types.Part(text="after")]
        completion = process_gemini_response_to_openai_dict(types.GenerateContentResponse(candidates=[
            types.Candidate(content=types.Content(role="model", parts=original), finish_reason="STOP")]), MODEL)
        claude = cp.openai_completion_to_anthropic(completion, "claude-test", "message")
        self.assertEqual([b["type"] for b in claude["content"]], ["text", "tool_use", "text", "tool_use", "text"])
        body = {"max_tokens": 100, "messages": [{"role": "assistant", "content": claude["content"]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": ident, "content": "done"} for ident in ("b", "a")]}]}
        req = cp.anthropic_to_openai_request(body, MODEL)
        validate_openai_request(req)
        sdk = create_gemini_prompt(req.messages, MODEL)
        cookie = _convert_messages_to_contents(req.messages, MODEL)[0]
        self.assertEqual([p.text if p.text is not None else p.function_call.id for p in sdk[0].parts],
                         ["before", "a", "between", "b", "after"])
        self.assertEqual([p.get("text", p.get("functionCall", {}).get("id")) for p in cookie[0]["parts"]],
                         ["before", "a", "between", "b", "after"])
        self.assertEqual([p.function_response.id for p in sdk[1].parts], ["b", "a"])
        self.assertEqual([p["functionResponse"]["id"] for p in cookie[1]["parts"]], ["b", "a"])

    def test_untrusted_incomplete_topology_does_not_drop_visible_content(self):
        message = {"content": "complete answer", "tool_calls": [call()], "extra_content": {"google": {
            "ordinary_parts": [{"kind": "text", "text": "short"}], "part_order": [{"type": "tool_call", "index": 0}]}}}
        result = cp.openai_completion_to_anthropic({"choices": [{"message": message}]}, "m", "id")
        self.assertEqual(result["content"][0]["text"], "complete answer")
        self.assertEqual(result["content"][1]["type"], "tool_use")

    def test_malformed_upstream_arguments_never_become_an_invented_value_argument(self):
        for args in ('{"x":', '[1]', '{"x":NaN}'):
            with self.subTest(args=args), self.assertRaises(IncompleteResponseError):
                cp.openai_completion_to_anthropic({"choices": [{"message": {"tool_calls": [call(args)]}}]}, "m", "id")

    def test_invalid_claude_description_rejected(self):
        with self.assertRaises(cp.ClaudeRequestError):
            cp.anthropic_to_openai_request({"max_tokens": 1, "messages": [{"role": "user", "content": "x"}],
                                            "tools": [{"name": "lookup", "description": [], "input_schema": {}}]}, MODEL)


class ClaudeStreamIntegrityTests(unittest.TestCase):
    def test_late_ids_and_interleaved_parallel_deltas_preserve_all_arguments(self):
        state = cp.ClaudeStreamState("m")
        state.feed_openai_chunk(chunk([{"index": 0, "function": {"name": "first", "arguments": '{"x":'}},
                                       {"index": 1, "id": "b", "function": {"arguments": '{"y":'}}]))
        state.feed_openai_chunk(chunk([{"index": 1, "function": {"name": "second", "arguments": '2}'}},
                                       {"index": 0, "id": "a", "function": {"arguments": '1}'}}], finish="tool_calls"))
        events = frames(state.finish_events())
        blocks = [e["content_block"] for e in events if e["type"] == "content_block_start"]
        self.assertEqual([(b["id"], b["name"]) for b in blocks], [("a", "first"), ("b", "second")])
        args = [json.loads(e["delta"]["partial_json"]) for e in events if e["type"] == "content_block_delta"]
        self.assertEqual(args, [{"x": 1}, {"y": 2}])
        self.assertEqual(state._tools, {})
        self.assertEqual(state.finish_events(), [])

    def test_conflicting_tool_identity_is_not_silently_reset(self):
        for delta in ({"index": 0, "id": "other"}, {"index": 1, "id": "a"},
                      {"index": 0, "function": {"name": "other"}}):
            state = cp.ClaudeStreamState("m")
            state.feed_openai_chunk(chunk([{**call(), "index": 0, "id": "a"}]))
            with self.subTest(delta=delta), self.assertRaises(IncompleteResponseError):
                state.feed_openai_chunk(chunk([delta]))

    def test_invalid_stream_indices_and_argument_shapes_fail(self):
        for delta in ({"index": -1}, {"index": True}, {"index": "0"}, {"index": 0, "id": []},
                      {"index": 0, "function": {"arguments": {}}}):
            with self.subTest(delta=delta), self.assertRaises(IncompleteResponseError):
                cp.ClaudeStreamState("m").feed_openai_chunk(chunk([delta]))

    def test_argument_buffer_and_call_count_are_bounded(self):
        with mock.patch.object(cp, "MAX_TOOL_STREAM_BYTES", 8):
            state = cp.ClaudeStreamState("m")
            with self.assertRaises(IncompleteResponseError):
                state.feed_openai_chunk(chunk([{**call('"' + 'x' * 10 + '"'), "index": 0}]))
            self.assertEqual(state._tools[0]["parts"], [])
        with mock.patch.object(cp, "MAX_STREAM_TOOL_CALLS", 1):
            state = cp.ClaudeStreamState("m")
            state.feed_openai_chunk(chunk([{**call(), "index": 0}]))
            with self.assertRaises(IncompleteResponseError):
                state.feed_openai_chunk(chunk([{**call(ident="other"), "index": 1}]))

    def test_invalid_complete_json_rejected_but_truncation_remains_visible(self):
        for reason in ("tool_calls", "length", "content_filter"):
            state = cp.ClaudeStreamState("m")
            state.feed_openai_chunk(chunk([{**call('{"x":'), "index": 0}], finish=reason))
            if reason == "tool_calls":
                with self.assertRaises(IncompleteResponseError):
                    state.finish_events()
            else:
                events = frames(state.finish_events())
                args = next(e["delta"]["partial_json"] for e in events if e["type"] == "content_block_delta")
                self.assertEqual(args, '{"x":')
                stop = next(e["delta"]["stop_reason"] for e in events if e["type"] == "message_delta")
                self.assertEqual(stop, "max_tokens" if reason == "length" else "refusal")

    def test_usage_attached_to_a_choice_is_not_lost(self):
        state = cp.ClaudeStreamState("m")
        state.feed_openai_chunk(chunk([], finish="stop", usage={"prompt_tokens": 10, "completion_tokens": 4}))
        events = frames(state.finish_events())
        usage = next(e["usage"] for e in events if e["type"] == "message_delta")
        self.assertEqual((usage["input_tokens"], usage["output_tokens"]), (10, 4))


class IncompleteNativeCalls(unittest.TestCase):
    def test_partial_real_functions_never_become_empty_executable_calls(self):
        for fields in ({"will_continue": True}, {"partial_args": [types.PartialArg(json_path="$.file", string_value="part")]}):
            response = types.GenerateContentResponse(candidates=[types.Candidate(content=types.Content(
                role="model", parts=[types.Part(function_call=types.FunctionCall(name="write_file", **fields))]), finish_reason="STOP")])
            for convert in (lambda: process_gemini_response_to_openai_dict(response, MODEL),
                            lambda: _build_chunk_payload(response, MODEL, "id")):
                with self.subTest(fields=fields), self.assertRaises(IncompleteResponseError):
                    convert()
        for fields in ({"willContinue": True}, {"partialArgs": [{"jsonPath": "$.file", "stringValue": "part"}]}):
            wire = {"results": [{"data": {"candidates": [{"content": {"parts": [
                {"functionCall": {"name": "write_file", **fields}}]}}]}}]}
            for preserve in (True, False):
                with self.assertRaises(IncompleteResponseError):
                    list(extract_events(wire, preserve_parts=preserve))

    def test_no_argument_calls_emit_a_json_object_in_both_delivery_modes(self):
        response = types.GenerateContentResponse(candidates=[types.Candidate(content=types.Content(
            role="model", parts=[types.Part(function_call=types.FunctionCall(name="status"))]), finish_reason="STOP")])
        completion = process_gemini_response_to_openai_dict(response, MODEL)["choices"][0]["message"]
        delta = _build_chunk_payload(response, MODEL, "id")["choices"][0]["delta"]
        for message in (completion, delta):
            self.assertEqual(message["tool_calls"][0]["function"]["arguments"], "{}")


class ClaudeRouteErrors(unittest.IsolatedAsyncioTestCase):
    async def test_bad_nonstream_tool_reply_is_an_anthropic_502(self):
        body = {"model": MODEL, "max_tokens": 10, "messages": [{"role": "user", "content": "x"}]}
        response = JSONResponse({"choices": [{"message": {"tool_calls": [call('not-json')]}}]})
        with mock.patch.object(claude_api, "_available_models", mock.AsyncMock(return_value=[])), \
                mock.patch.object(claude_api, "_call_upstream", mock.AsyncMock(return_value=response)):
            result = await claude_api.claude_messages(SimpleNamespace(json=mock.AsyncMock(return_value=body)), "offline")
        self.assertEqual(result.status_code, 502)
        self.assertEqual(json.loads(result.body)["error"]["type"], "api_error")

    async def test_bad_stream_tool_reply_emits_error_without_tool_execution_or_success(self):
        closed = mock.Mock()
        async def upstream():
            try:
                yield "data: " + json.dumps(chunk([{**call('{"x":'), "index": 0}], finish="tool_calls")) + "\n\n"
                yield "data: [DONE]\n\n"
            finally:
                closed()
        result = claude_api._claude_stream_response(StreamingResponse(upstream()), MODEL, omit_thinking=False)
        events = "".join([item async for item in result.body_iterator])
        self.assertIn("event: error", events)
        self.assertNotIn("event: message_stop", events)
        self.assertNotIn('"type": "tool_use"', events)
        closed.assert_called_once()


if __name__ == "__main__":
    unittest.main()
