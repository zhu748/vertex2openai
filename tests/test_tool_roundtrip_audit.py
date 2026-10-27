"""Offline tool round trips across OpenAI, Claude and both Gemini transports."""

import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-tool-roundtrip-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import claude_protocol as cp
from google.genai import types
from message_processing import create_gemini_prompt, process_gemini_response_to_openai_dict
from models import OpenAIMessage
from response_validation import IncompleteResponseError
from upstreams.cookie_native import NativeAssembler
from upstreams.cookie_request import _convert_messages_to_contents
from upstreams.cookie_response import extract_events, openai_tool_call
from upstreams.express_response import ToolCallIndexer, _build_chunk_payload


def wire_response(parts):
    return {"results": [{"data": {"candidates": [{
        "content": {"role": "model", "parts": parts}, "finishReason": "STOP"
    }]}}]}


def cookie_contents(messages):
    contents, _ = _convert_messages_to_contents(messages, "gemini-2.5-flash")
    return contents


def sdk_contents(messages):
    return [content.model_dump(mode="json", by_alias=True, exclude_none=True)
            for content in create_gemini_prompt(messages, "gemini-2.5-flash")]


def claude_body(**kwargs):
    return {"model": "gemini-2.5-flash", "max_tokens": 512,
            "messages": [{"role": "user", "content": "hello"}], **kwargs}


class CookieCallIdentityTests(unittest.TestCase):
    def test_parallel_same_name_calls_keep_ids_through_results_in_reverse_order(self):
        parts = [{"functionCall": {"id": ident, "name": "lookup", "args": {"city": city}}}
                 for ident, city in (("call-paris", "Paris"), ("call-tokyo", "Tokyo"))]
        original = copy.deepcopy(parts)
        calls = [data for kind, data in extract_events(wire_response(parts)) if kind == "function_call"]
        tools = [openai_tool_call("response-one", index, call) for index, call in enumerate(calls)]
        self.assertEqual([tool["id"] for tool in tools], ["call-paris", "call-tokyo"])
        messages = [OpenAIMessage(role="assistant", content=None, tool_calls=tools),
                    OpenAIMessage(role="tool", tool_call_id="call-tokyo", content='{"temperature":20}'),
                    OpenAIMessage(role="tool", tool_call_id="call-paris", content='{"temperature":10}')]
        for convert in (cookie_contents, sdk_contents):
            with self.subTest(convert=convert.__name__):
                contents = convert(messages)
                self.assertEqual([part["functionCall"]["id"] for part in contents[0]["parts"]],
                                 ["call-paris", "call-tokyo"])
                results = [part["functionResponse"] for part in contents[1]["parts"]]
                self.assertEqual([(result["id"], result["name"], result["response"]["temperature"])
                                  for result in results],
                                 [("call-tokyo", "lookup", 20), ("call-paris", "lookup", 10)])
        self.assertEqual(parts, original)

    def test_native_parts_and_legacy_event_assembly_keep_upstream_call_id(self):
        call = {"id": "native-id", "name": "lookup", "args": {"city": "Paris"}}
        wire = wire_response([{"functionCall": call}])
        for preserve in (True, False):
            with self.subTest(preserve=preserve):
                assembler = NativeAssembler("gemini-test")
                for kind, data in extract_events(wire, preserve_parts=preserve):
                    assembler.feed(kind, data)
                self.assertEqual(assembler.parts[0]["functionCall"], call)

    def test_missing_id_stays_deterministic_and_invalid_id_is_an_upstream_error(self):
        call = {"name": "lookup", "args": {}}
        first = openai_tool_call("r", 0, call)["id"]
        self.assertEqual(first, openai_tool_call("r", 0, call)["id"])
        self.assertNotEqual(first, openai_tool_call("r", 1, call)["id"])
        self.assertNotEqual(first, openai_tool_call("other", 0, call)["id"])
        for invalid in (7, [], {"secret": "value"}):
            with self.subTest(invalid=invalid), self.assertRaises(IncompleteResponseError):
                list(extract_events(wire_response([{"functionCall": {**call, "id": invalid}}])))

    def test_legacy_signature_id_is_removed_from_calls_and_results(self):
        legacy_id = "real-call__thought__c2ln"
        messages = [OpenAIMessage(role="assistant", tool_calls=[{
            "id": legacy_id, "type": "function", "function": {"name": "lookup", "arguments": "{}"}
        }]), OpenAIMessage(role="tool", tool_call_id=legacy_id, content="done")]
        for convert in (cookie_contents, sdk_contents):
            with self.subTest(convert=convert.__name__):
                contents = convert(messages)
                self.assertEqual(contents[0]["parts"][0]["functionCall"]["id"], "real-call")
                self.assertEqual(contents[1]["parts"][0]["functionResponse"]["id"], "real-call")
                self.assertEqual(contents[0]["parts"][0]["thoughtSignature"], "c2ln")


class ToolHistoryTests(unittest.TestCase):
    def test_repeated_metadata_indices_never_duplicate_calls_or_text(self):
        message = OpenAIMessage(role="assistant", content="checking", tool_calls=[{
            "id": "a", "type": "function", "function": {"name": "first", "arguments": "{}"}
        }, {"id": "b", "type": "function", "function": {"name": "second", "arguments": "{}"}}],
            extra_content={"google": {"part_order": [
                {"type": "tool_call", "index": True},
                {"type": "tool_call", "index": 1},
                {"type": "tool_call", "index": 1},
                {"type": "ordinary", "index": False},
                {"type": "ordinary", "index": 0},
                {"type": "ordinary", "index": 0},
                {"type": "tool_call", "index": 0},
                {"type": "tool_call", "index": -1},
                {"type": "ordinary", "index": 100},
            ]}})
        for convert in (cookie_contents, sdk_contents):
            with self.subTest(convert=convert.__name__):
                parts = convert([message])[0]["parts"]
                self.assertEqual(len(parts), 3)
                self.assertEqual(parts[0]["functionCall"]["name"], "second")
                self.assertEqual(parts[1]["text"], "checking")
                self.assertEqual(parts[2]["functionCall"]["name"], "first")

    def test_result_name_uses_preceding_call_when_a_later_turn_reuses_the_id(self):
        messages = []
        for name in ("lookup", "calculate"):
            messages.extend([OpenAIMessage(role="assistant", tool_calls=[{
                "id": "reused", "type": "function", "function": {"name": name, "arguments": "{}"}
            }]), OpenAIMessage(role="tool", tool_call_id="reused", content="done")])
        for convert in (cookie_contents, sdk_contents):
            with self.subTest(convert=convert.__name__):
                contents = convert(messages)
                self.assertEqual([contents[index]["parts"][0]["functionResponse"]["name"]
                                  for index in (1, 3)], ["lookup", "calculate"])


class ClaudeToolContractTests(unittest.TestCase):
    def test_tool_errors_remain_errors_for_both_transports(self):
        for content in ("connection failed", [{"type": "text", "text": "connection failed"}], None):
            body = claude_body(messages=[{"role": "assistant", "content": [{
                "type": "tool_use", "id": "tool-1", "name": "lookup", "input": {}
            }]}, {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "tool-1",
                                                    "is_error": True, "content": content}]}])
            original = copy.deepcopy(body)
            request = cp.anthropic_to_openai_request(body, "gemini-2.5-flash")
            for convert in (cookie_contents, sdk_contents):
                with self.subTest(content=content, convert=convert.__name__):
                    result = convert(request.messages)[1]["parts"][0]["functionResponse"]
                    self.assertEqual(result["response"], {"error": "connection failed" if content else ""})
            self.assertEqual(body, original)

    def test_invalid_tool_payloads_are_request_errors_instead_of_internal_errors(self):
        for invalid in (17, True, {"result": "not a content block array"}, [17],
                        [{"type": "image", "source": {}}, 17], [{"type": "text", "text": 17}]):
            body = claude_body(messages=[{"role": "user", "content": [{
                "type": "tool_result", "tool_use_id": "t", "content": invalid
            }]}])
            with self.subTest(invalid=invalid), self.assertRaises(cp.ClaudeRequestError):
                cp.anthropic_to_openai_request(body, "m")
        for invalid in ("true", 1, []):
            body = claude_body(messages=[{"role": "user", "content": [{
                "type": "tool_result", "tool_use_id": "t", "content": "failed", "is_error": invalid
            }]}])
            with self.subTest(is_error=invalid), self.assertRaises(cp.ClaudeRequestError):
                cp.anthropic_to_openai_request(body, "m")
        for invalid in ([], "schema", True):
            with self.subTest(schema=invalid), self.assertRaises(cp.ClaudeRequestError):
                cp.anthropic_to_openai_request(claude_body(tools=[{"name": "lookup", "input_schema": invalid}]), "m")
        for field in ("temperature", "top_p"):
            for value in (float("nan"), float("inf"), float("-inf"), 10 ** 1000):
                with self.subTest(field=field, value=value), self.assertRaises(cp.ClaudeRequestError):
                    cp.anthropic_to_openai_request(claude_body(**{field: value}), "m")

    def test_invalid_tool_arguments_are_rejected_without_replacing_them_with_empty_args(self):
        for invalid in (float("nan"), float("inf"), object()):
            body = claude_body(messages=[{"role": "assistant", "content": [{
                "type": "tool_use", "id": "t", "name": "lookup", "input": {"value": invalid}
            }]}])
            with self.subTest(value=invalid), self.assertRaises(cp.ClaudeRequestError):
                cp.anthropic_to_openai_request(body, "m")

    def test_cached_usage_keeps_input_totals_consistent_in_stream_and_json(self):
        usage = {"prompt_tokens": 100, "completion_tokens": 7,
                 "prompt_tokens_details": {"cached_tokens": 60}}
        payload = {"choices": [{"message": {"content": "answer"}, "finish_reason": "stop"}], "usage": usage}
        output = cp.openai_completion_to_anthropic(payload, "m", "id")
        state = cp.ClaudeStreamState("m")
        state.feed_usage(usage)
        final_frame = next(frame for frame in state.finish_events() if frame.startswith("event: message_delta\n"))
        stream_usage = json.loads(final_frame.split("data: ", 1)[1])["usage"]
        for mapped in (output["usage"], state.usage, stream_usage):
            self.assertEqual(mapped["input_tokens"], 40)
            self.assertEqual(mapped["input_tokens"] + mapped["cache_read_input_tokens"], 100)
        self.assertEqual(cp._anthropic_usage({"prompt_tokens": 10,
                         "prompt_tokens_details": {"cached_tokens": 20}})["cache_read_input_tokens"], 10)
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                mapped = cp._anthropic_usage({"prompt_tokens": value, "completion_tokens": value,
                                             "prompt_tokens_details": {"cached_tokens": value}})
                self.assertEqual(mapped["input_tokens"], 0)
                self.assertEqual(mapped["output_tokens"], 0)
                self.assertEqual(mapped["cache_read_input_tokens"], 0)


class ClaudeRouteContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_malformed_tool_results_return_anthropic_400_without_calling_upstream(self):
        from routes import claude_api

        for invalid in (17, [17], [{"type": "image", "source": {}}, 17]):
            body = claude_body(messages=[{"role": "user", "content": [{
                "type": "tool_result", "tool_use_id": "t", "content": invalid
            }]}])
            request = SimpleNamespace(json=mock.AsyncMock(return_value=body))
            with self.subTest(invalid=invalid), \
                 mock.patch.object(claude_api, "_available_models", mock.AsyncMock(return_value=[])), \
                 mock.patch.object(claude_api, "_call_upstream", mock.AsyncMock()) as call:
                response = await claude_api.claude_messages(request, "unused")
                self.assertEqual(response.status_code, 400)
                self.assertEqual(json.loads(response.body)["error"]["type"], "invalid_request_error")
                call.assert_not_awaited()


class ToolTerminationTests(unittest.TestCase):
    def test_truncation_and_filter_reasons_survive_tool_calls_in_all_protocols(self):
        for reason, expected, claude_reason in (("STOP", "tool_calls", "tool_use"),
                                               ("MAX_TOKENS", "length", "max_tokens"),
                                               ("SAFETY", "content_filter", "refusal")):
            response = types.GenerateContentResponse(candidates=[types.Candidate(
                content=types.Content(role="model", parts=[types.Part(function_call=types.FunctionCall(
                    id="lookup-1", name="lookup", args={"city": "Paris"}))]), finish_reason=reason)])
            with self.subTest(reason=reason):
                completion = process_gemini_response_to_openai_dict(response, "m")
                self.assertEqual(completion["choices"][0]["finish_reason"], expected)
                self.assertEqual(cp.openai_completion_to_anthropic(completion, "m", "id")["stop_reason"],
                                 claude_reason)
            for indexer in (None, ToolCallIndexer()):
                with self.subTest(reason=reason, indexer=indexer):
                    chunk = _build_chunk_payload(response, "m", "id", indexer=indexer)
                    self.assertEqual(chunk["choices"][0]["finish_reason"], expected)
                    state = cp.ClaudeStreamState("m")
                    state.feed_openai_chunk(chunk)
                    final_frame = next(frame for frame in state.finish_events()
                                       if frame.startswith("event: message_delta\n"))
                    self.assertEqual(json.loads(final_frame.split("data: ", 1)[1])["delta"]["stop_reason"],
                                     claude_reason)

    def test_finish_in_a_later_chunk_keeps_tool_state_but_preserves_truncation(self):
        for reason, expected in (("STOP", "tool_calls"), ("MAX_TOKENS", "length"), ("SAFETY", "content_filter")):
            with self.subTest(reason=reason):
                indexer = ToolCallIndexer()
                first = types.GenerateContentResponse(candidates=[types.Candidate(content=types.Content(parts=[
                    types.Part(function_call=types.FunctionCall(name="lookup", args={}))]))])
                self.assertIsNone(_build_chunk_payload(first, "m", "id", indexer=indexer)["choices"][0]["finish_reason"])
                final = types.GenerateContentResponse(candidates=[types.Candidate(finish_reason=reason)])
                self.assertEqual(_build_chunk_payload(final, "m", "id", indexer=indexer)["choices"][0]["finish_reason"], expected)


if __name__ == "__main__":
    with mock.patch("builtins.print"):
        unittest.main()
