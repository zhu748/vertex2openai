"""Cross-protocol stop, indexing, usage, media and terminal stream contracts."""
import asyncio
import base64
from contextlib import asynccontextmanager
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-protocol-edges-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.responses import StreamingResponse
from google.genai import types
from PIL import Image

import claude_protocol as cp
import api_helpers
import config
from logger import ProxyStats
from message_processing import create_gemini_prompt, process_gemini_response_to_openai_dict
from models import OpenAIMessage, OpenAIRequest
from response_validation import IncompleteResponseError, map_finish_reason_to_openai
from routes import claude_api
from runtime_state import app_state
from upstreams import cookie_proxy
from upstreams.cookie_native import NativeAssembler
from upstreams.cookie_request import _convert_messages_to_contents
from upstreams.cookie_response import extract_events
from upstreams.express_response import _build_chunk_payload
from usage import map_usage, merge_usage_metadata


def response(*parts, finish="STOP", index=0):
    return types.GenerateContentResponse(candidates=[types.Candidate(index=index,
        content=types.Content(role="model", parts=list(parts)), finish_reason=finish)])


def wire(*parts, finish="STOP"):
    return {"results": [{"data": {"candidates": [{"content": {"role": "model", "parts": list(parts)},
                                                "finishReason": finish}]}}]}


def frame(delta=None, finish=None, usage=None):
    payload = {"choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish}]}
    if usage is not None:
        payload["usage"] = usage
    return "data: " + json.dumps(payload) + "\n\n"


class FinishReasonTests(unittest.TestCase):
    def test_new_safety_reasons_remain_refusals_in_both_delivery_modes(self):
        for reason in ("MODEL_ARMOR", "IMAGE_PROHIBITED_CONTENT", "IMAGE_RECITATION"):
            with self.subTest(reason=reason):
                source = response(types.Part(text="filtered"), finish=reason)
                full = process_gemini_response_to_openai_dict(source, "model")
                streamed = _build_chunk_payload(source, "model", "id")
                self.assertEqual(full["choices"][0]["finish_reason"], "content_filter")
                self.assertEqual(streamed["choices"][0]["finish_reason"], "content_filter")
                self.assertEqual(cp.openai_completion_to_anthropic(full, "model", "id")["stop_reason"], "refusal")
                state = cp.ClaudeStreamState("model")
                state.feed_openai_chunk(streamed)
                self.assertIn('"stop_reason": "refusal"', "".join(state.finish_events()))

    def test_failed_tool_or_image_generation_never_becomes_successful_stop(self):
        for reason in ("MALFORMED_FUNCTION_CALL", "UNEXPECTED_TOOL_CALL", "NO_IMAGE", "IMAGE_OTHER"):
            source = response(types.Part(function_call=types.FunctionCall(name="write", args={"text": "x"})), finish=reason)
            for convert in (lambda: map_finish_reason_to_openai(reason),
                            lambda: process_gemini_response_to_openai_dict(source, "model"),
                            lambda: _build_chunk_payload(source, "model", "id"),
                            lambda: list(extract_events(wire({"functionCall": {"name": "write", "args": {}}}, finish=reason)))):
                with self.subTest(reason=reason), self.assertRaises(IncompleteResponseError):
                    convert()

    def test_cookie_native_response_keeps_original_failure_reason(self):
        for reason in ("MALFORMED_FUNCTION_CALL", "NO_IMAGE"):
            events = list(extract_events(wire(finish=reason), preserve_parts=True))
            self.assertEqual(events, [("finish", reason)])


class CandidateIndexTests(unittest.TestCase):
    def test_filtered_or_reordered_candidates_keep_their_upstream_indices(self):
        source = types.GenerateContentResponse(candidates=[
            response(types.Part(text="second"), index=2).candidates[0],
            response(types.Part(text="first"), index=0).candidates[0]])
        full = process_gemini_response_to_openai_dict(source, "gemini-2.5-flash")
        self.assertEqual([choice["index"] for choice in full["choices"]], [2, 0])
        self.assertEqual([_build_chunk_payload(source, "model", "id", i)["choices"][0]["index"]
                          for i in range(2)], [2, 0])

    def test_missing_index_still_uses_position(self):
        source = types.GenerateContentResponse(candidates=[
            types.Candidate(content=types.Content(parts=[types.Part(text="one")]), finish_reason="STOP"),
            types.Candidate(content=types.Content(parts=[types.Part(text="two")]), finish_reason="STOP")])
        self.assertEqual([choice["index"] for choice in process_gemini_response_to_openai_dict(source, "model")["choices"]], [0, 1])


class PartialUsageTests(unittest.TestCase):
    def test_native_and_sdk_snapshots_merge_without_stale_totals(self):
        usage = merge_usage_metadata(None, types.GenerateContentResponseUsageMetadata(
            prompt_token_count=100, cached_content_token_count=20, total_token_count=100))
        usage = merge_usage_metadata(usage, {"candidates_token_count": 5, "thoughtsTokenCount": 2,
                                            "candidatesTokensDetails": [{"modality": "TEXT", "tokenCount": 5}]})
        self.assertEqual(map_usage(usage), {"prompt_tokens": 100, "completion_tokens": 7,
            "total_tokens": 107, "prompt_tokens_details": {"cached_tokens": 20},
            "completion_tokens_details": {"reasoning_tokens": 2}})
        self.assertEqual(usage["candidatesTokensDetails"], [{"modality": "TEXT", "tokenCount": 5}])
        self.assertEqual(merge_usage_metadata(usage, {"promptTokenCount": None, "candidatesTokenCount": "bad"}), usage)
        zero = map_usage(merge_usage_metadata(usage, {"candidatesTokenCount": 0, "thoughtsTokenCount": 0,
                                                   "cachedContentTokenCount": 0}))
        self.assertEqual(zero, {"prompt_tokens": 100, "completion_tokens": 0, "total_tokens": 100})

    def test_cookie_native_and_rest_diagnostics_preserve_partial_usage(self):
        from routes.gemini_native import _NativeStreamState
        bridge, inspector = NativeAssembler("model"), _NativeStreamState()
        for usage in ({"promptTokenCount": 100, "cachedContentTokenCount": 20},
                      {"candidatesTokenCount": 5, "totalTokenCount": 105}):
            bridge.feed("usage", usage)
            inspector.inspect({"usageMetadata": usage})
        expected = {"promptTokenCount": 100, "cachedContentTokenCount": 20,
                    "candidatesTokenCount": 5, "totalTokenCount": 105}
        self.assertEqual(bridge.usage, expected)
        self.assertEqual(inspector.usage, expected)

    def test_output_only_updates_keep_input_cache_and_thinking_counters(self):
        state = cp.ClaudeStreamState("model")
        state.feed_usage({"prompt_tokens": 100, "completion_tokens": 2,
                          "prompt_tokens_details": {"cached_tokens": 20},
                          "completion_tokens_details": {"reasoning_tokens": 1}})
        state.feed_usage({"completion_tokens": 7})
        state.feed_usage({})
        self.assertEqual(state.usage, {"input_tokens": 80, "output_tokens": 7,
            "cache_creation_input_tokens": 0, "cache_read_input_tokens": 20,
            "output_tokens_details": {"thinking_tokens": 1}})

    def test_late_cache_counts_zero_updates_and_cumulative_not_additive(self):
        state = cp.ClaudeStreamState("model")
        state.feed_usage({"prompt_tokens_details": {"cached_tokens": 20}})
        state.feed_usage({"prompt_tokens": 100})
        state.feed_usage({"prompt_tokens": 100})
        self.assertEqual(state.usage["input_tokens"], 80)
        state.feed_usage({"prompt_tokens_details": {"cached_tokens": 0}, "completion_tokens": 0})
        self.assertEqual(state.usage["input_tokens"], 100)
        self.assertEqual(state.usage["cache_read_input_tokens"], 0)

    def test_invalid_partial_counters_do_not_clear_last_valid_counts(self):
        state = cp.ClaudeStreamState("model")
        state.feed_usage({"prompt_tokens": 10, "completion_tokens": 5})
        for invalid in (None, True, float("nan"), float("inf"), "bad"):
            state.feed_usage({"prompt_tokens": invalid, "completion_tokens": invalid})
        self.assertEqual((state.usage["input_tokens"], state.usage["output_tokens"]), (10, 5))


class ClaudeStreamTerminalTests(unittest.IsolatedAsyncioTestCase):
    async def collect(self, pieces, *, hang_after=False):
        closed = asyncio.Event()
        async def upstream():
            try:
                for piece in pieces:
                    yield piece
                if hang_after:
                    await asyncio.Event().wait()
            finally:
                closed.set()
        translated = claude_api._claude_stream_response(StreamingResponse(upstream()), "model", omit_thinking=False)
        async def read():
            return "".join([item async for item in translated.body_iterator])
        result = await asyncio.wait_for(read(), timeout=1)
        self.assertTrue(closed.is_set())
        return result

    async def test_missing_finish_reason_errors_instead_of_successful_message_stop(self):
        for tail in ([], ["data: [DONE]\n\n"]):
            with self.subTest(tail=tail):
                result = await self.collect([frame({"content": "unfinished"}), *tail])
                self.assertIn("event: error", result)
                self.assertNotIn("event: message_stop", result)

    async def test_malformed_json_or_nonobject_event_is_not_silently_dropped(self):
        for invalid in ("not-json", "[]", "null"):
            with self.subTest(invalid=invalid):
                result = await self.collect([frame({"content": "before"}), f"data: {invalid}\n\n", frame(finish="stop")])
                self.assertIn("event: error", result)
                self.assertNotIn("event: message_stop", result)

    async def test_done_finishes_without_waiting_for_network_eof(self):
        result = await self.collect([frame({"content": "answer"}, finish="stop"), "data: [DONE]\n\n"], hang_after=True)
        self.assertIn("event: message_stop", result)
        self.assertNotIn("event: error", result)

    async def test_data_after_done_is_not_appended_to_completed_message(self):
        result = await self.collect([frame({"content": "answer"}, finish="stop"), "data: [DONE]\n\n",
                                     frame({"content": "must not appear"})])
        self.assertNotIn("must not appear", result)

    async def test_split_usage_reaches_final_anthropic_event(self):
        result = await self.collect([frame({"content": "answer"}, usage={"prompt_tokens": 100,
                    "prompt_tokens_details": {"cached_tokens": 20}}), frame(finish="stop"),
                    'data: {"choices":[],"usage":{"completion_tokens":7}}\n\n', "data: [DONE]\n\n"])
        event = next(block for block in result.split("\n\n") if block.startswith("event: message_delta\n"))
        usage = json.loads(event.split("data: ", 1)[1])["usage"]
        self.assertEqual((usage["input_tokens"], usage["cache_read_input_tokens"], usage["output_tokens"]), (80, 20, 7))

    async def test_comments_inside_multiline_data_do_not_split_or_hide_json(self):
        source = (': leading comment\ndata: {"choices":[{"index":0,\n'
                  ': inside JSON event\ndata: "delta":{"content":"joined"},"finish_reason":"stop"}]}\n\n')
        result = await self.collect([source, "data: [DONE]\n\n"])
        self.assertIn('"text": "joined"', result)
        self.assertIn("event: message_stop", result)
        self.assertNotIn("event: error", result)

    async def test_empty_data_keepalives_do_not_corrupt_valid_message(self):
        result = await self.collect(["data: \n\n", frame({"content": "answer"}, finish="stop"), "data: [DONE]\n\n"])
        self.assertIn("event: message_stop", result)
        self.assertNotIn("event: error", result)


class FileReferenceTests(unittest.TestCase):
    def test_cookie_native_keeps_file_uri_mime_display_name_and_signature(self):
        part = {"fileData": {"mimeType": "application/pdf", "fileUri": "gs://bucket/report.pdf", "displayName": "report"},
                "thoughtSignature": "c2ln"}
        assembler = NativeAssembler("model")
        for kind, data in extract_events(wire(part), preserve_parts=True):
            assembler.feed(kind, data)
        self.assertEqual(assembler.payload()["candidates"][0]["content"]["parts"], [part])

    def test_cookie_and_sdk_render_images_and_documents_consistently(self):
        for mime, uri, expected in (("image/png", "https://example.test/image.png", "![Image](https://example.test/image.png)"),
                                    ("application/pdf", "gs://bucket/report.pdf", "[File](gs://bucket/report.pdf)")):
            with self.subTest(mime=mime):
                part = {"fileData": {"mimeType": mime, "fileUri": uri}}
                cookie = [data for kind, data in extract_events(wire(part)) if kind in ("text", "image")]
                self.assertEqual(cookie, [expected])
                source = response(types.Part(file_data=types.FileData(mime_type=mime, file_uri=uri)))
                self.assertEqual(process_gemini_response_to_openai_dict(source, "model")["choices"][0]["message"]["content"], expected)
                self.assertEqual(_build_chunk_payload(source, "model", "id")["choices"][0]["delta"]["content"], expected)

    def test_invalid_file_fields_fail_but_empty_proto_defaults_do_not(self):
        for data in ({"fileUri": 3}, {"fileUri": "https://example.test", "mimeType": []}):
            with self.subTest(data=data), self.assertRaises(IncompleteResponseError):
                list(extract_events(wire({"fileData": data})))
        events = list(extract_events(wire({"text": "normal", "fileData": {"fileUri": None, "mimeType": None}})))
        self.assertEqual(events, [("text", "normal"), ("finish", "STOP")])


class CookieImageSignatureTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_usage_survives_cookie_nonstream_conversion(self):
        first, last = wire({"text": "hello"}, finish=None), wire()
        first["results"][0]["data"]["usageMetadata"] = {"promptTokenCount": 100, "cachedContentTokenCount": 20}
        last["results"][0]["data"]["usageMetadata"] = {"candidatesTokenCount": 5}
        @asynccontextmanager
        async def opened(*args, **kwargs):
            yield SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(200,
                text=json.dumps(first) + "\n" + json.dumps(last))))
        request = OpenAIRequest(model="gemini-3.5-flash-lite", messages=[{"role": "user", "content": "hello"}])
        settings = dict(config.DEFAULT_SETTINGS, proxy_url="", retry_max=0)
        with mock.patch.object(cookie_proxy, "open_upstream_client", opened), \
             mock.patch.object(cookie_proxy, "stats", ProxyStats()), \
             mock.patch.object(cookie_proxy.credential_manager, "record_cookie_success"), \
             mock.patch.object(app_state, "get_effective_settings", return_value=settings), \
             mock.patch.object(app_state, "get_setting", side_effect=lambda k, d=None: settings.get(k, d)):
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(request,
                SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False)),
                credential_record={"id": "offline", "cookie": "SAPISID=offline", "project_id": "offline"})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(json.loads(result.body)["usage"], {"prompt_tokens": 100, "completion_tokens": 5,
            "total_tokens": 105, "prompt_tokens_details": {"cached_tokens": 20}})

    async def test_partial_usage_survives_sdk_stream_conversion(self):
        async def generate(**kwargs):
            async def source():
                first = response(types.Part(text="hello"), finish=None)
                first.usage_metadata = types.GenerateContentResponseUsageMetadata(
                    prompt_token_count=100, cached_content_token_count=20)
                yield first
                last = response(finish="STOP")
                last.usage_metadata = types.GenerateContentResponseUsageMetadata(candidates_token_count=5)
                yield last
            return source()
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content_stream=generate)))
        request = OpenAIRequest(model="gemini-3.5-flash-lite", messages=[{"role": "user", "content": "hello"}],
                                stream=True, stream_options={"include_usage": True})
        settings = dict(config.DEFAULT_SETTINGS, fake_streaming=False, retry_max=0)
        with mock.patch.object(api_helpers, "stats", ProxyStats()), \
             mock.patch.object(app_state, "get_setting", side_effect=lambda k, d=None: settings.get(k, d)):
            result = await api_helpers.execute_gemini_call(client, request.model,
                lambda messages: [types.Content(role="user", parts=[types.Part(text="hello")])], {}, request)
            chunks = [json.loads(line[6:]) async for piece in result.body_iterator
                      for line in piece.splitlines() if line.startswith("data: {")]
        self.assertFalse(any("error" in chunk for chunk in chunks))
        self.assertEqual([chunk["usage"] for chunk in chunks if "usage" in chunk], [
            {"prompt_tokens": 100, "completion_tokens": 5, "total_tokens": 105,
             "prompt_tokens_details": {"cached_tokens": 20}}])

    async def test_image_signature_stays_with_image_in_both_history_converters(self):
        output = io.BytesIO()
        Image.new("RGB", (1, 1), "red").save(output, format="PNG")
        encoded = base64.b64encode(output.getvalue()).decode()
        source = wire({"text": "before", "thoughtSignature": "dGV4dA=="},
                      {"inlineData": {"mimeType": "image/png", "data": encoded}, "thoughtSignature": "aW1hZ2U="})
        @asynccontextmanager
        async def opened(*args, **kwargs):
            yield SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(200, json=source)))
        request = OpenAIRequest(model="gemini-3.5-flash-lite", messages=[{"role": "user", "content": "hello"}])
        settings = dict(config.DEFAULT_SETTINGS, proxy_url="", retry_max=0)
        with mock.patch.object(cookie_proxy, "open_upstream_client", opened), \
             mock.patch.object(cookie_proxy, "stats", ProxyStats()), \
             mock.patch.object(cookie_proxy.credential_manager, "record_cookie_success"), \
             mock.patch.object(app_state, "get_effective_settings", return_value=settings), \
             mock.patch.object(app_state, "get_setting", side_effect=lambda k, d=None: settings.get(k, d)):
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(request,
                SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False)),
                credential_record={"id": "offline", "cookie": "SAPISID=offline", "project_id": "offline"})
            self.assertEqual(result.status_code, 200)
            message = json.loads(result.body)["choices"][0]["message"]
            ordinary = message["extra_content"]["google"]["ordinary_parts"]
            self.assertEqual([(part["kind"], part.get("thought_signature")) for part in ordinary],
                             [("text", "dGV4dA=="), ("image", "aW1hZ2U=")])
            self.assertEqual(ordinary[1]["text"], "")
            history = [OpenAIMessage.model_validate(message)]
            sdk = create_gemini_prompt(history, request.model)[0].parts
            cookie = _convert_messages_to_contents(history, request.model)[0][0]["parts"]
        self.assertEqual(sdk[0].thought_signature, b"text")
        self.assertEqual(sdk[1].inline_data.data, output.getvalue())
        self.assertEqual(sdk[1].thought_signature, b"image")
        self.assertEqual(cookie[1]["thoughtSignature"], "aW1hZ2U=")
        self.assertEqual(cookie[1]["inlineData"]["data"], encoded)


if __name__ == "__main__":
    unittest.main()
