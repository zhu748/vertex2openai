"""Tool generation progress must release the deadline; silent queues must not."""
import asyncio
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-tool-stream-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from google import genai
from google.genai import errors, types

import api_helpers
import first_reply
import tool_stream
from logger import ProxyStats
from models import OpenAIRequest
from response_validation import IncompleteResponseError
from tool_stream import ToolStreamAssembler


def chunk(*parts, finish=None, index=0):
    return types.GenerateContentResponse(candidates=[types.Candidate(
        index=index, content=types.Content(role="model", parts=list(parts)), finish_reason=finish)])


def call(**kwargs):
    return types.Part(function_call=types.FunctionCall(**kwargs))


def fragment(path, value=None, *, continuing=False, kind="string_value", close=False):
    fields = {"json_path": path, "will_continue": continuing}
    if not close:
        fields[kind] = value
    return types.PartialArg(**fields)


def parts(response):
    return response.candidates[0].content.parts


def tool_calls(body):
    return [tc for frame in body.splitlines() if frame.startswith("data: {")
            for choice in json.loads(frame[6:]).get("choices", [])
            for tc in choice.get("delta", {}).get("tool_calls", [])]


class AssemblyTests(unittest.TestCase):
    def test_header_and_partial_json_are_never_executable_calls(self):
        stream = ToolStreamAssembler()
        self.assertEqual(parts(stream.restore_sdk(chunk(call(name="reply", id="a", will_continue=True)))), [])
        self.assertEqual(parts(stream.restore_sdk(chunk(call(id="a", will_continue=True,
            partial_args=[fragment("$.text", "first", continuing=True)])))), [])
        out = stream.restore_sdk(chunk(call(id="a", partial_args=[fragment("$.text", "last")]), finish="STOP"))
        self.assertEqual(parts(out)[0].function_call.args, {"text": "firstlast"})
        self.assertEqual(parts(out)[0].function_call.id, "a")
        self.assertIsNone(parts(out)[0].function_call.partial_args)
        stream.validate_end()

    def test_path_only_close_nested_arrays_scalars_and_split_surrogates(self):
        stream = ToolStreamAssembler()
        items = [fragment("$.rows[0].name", "\ud83d", continuing=True),
                 fragment("$.rows[0].name", "\ude3a", continuing=True),
                 fragment("$.rows[0].name", close=True),
                 fragment("$.rows[0].ok", False, kind="bool_value"),
                 fragment("$.rows[1].n", 0, kind="number_value"),
                 fragment("$['odd.key']", "NULL_VALUE", kind="null_value"),
                 fragment('$["empty"]', "")]
        result = stream.restore_sdk(chunk(call(name="reply", partial_args=items), finish="STOP"))
        self.assertEqual(parts(result)[0].function_call.args,
                         {"rows": [{"name": "😺", "ok": False}, {"n": 0}], "odd.key": None, "empty": ""})

    def test_parallel_ids_and_interleaved_parts_keep_topology_and_signatures(self):
        stream = ToolStreamAssembler()
        header = call(name="first", id="a", will_continue=True).model_copy(update={"thought_signature": b"sig-a"})
        stream.restore_sdk(chunk(header, types.Part(text="between"), call(name="second", id="b", will_continue=True)))
        stream.restore_sdk(chunk(call(id="b", partial_args=[fragment("$.x", "B")])))
        result = stream.restore_sdk(chunk(call(id="a", partial_args=[fragment("$.x", "A")]), finish="STOP"))
        a, between, b = parts(result)
        self.assertEqual((a.function_call.name, between.text, b.function_call.name), ("first", "between", "second"))
        self.assertEqual((a.function_call.args, b.function_call.args), ({"x": "A"}, {"x": "B"}))
        self.assertEqual(a.thought_signature, b"sig-a")
        stream.validate_end()

    def test_late_identity_and_signature_are_kept(self):
        stream = ToolStreamAssembler()
        stream.restore_sdk(chunk(call(name="reply", will_continue=True)))
        final = call(id="late", partial_args=[fragment("$.x", "done")]).model_copy(update={"thought_signature": b"late-sig"})
        result = stream.restore_sdk(chunk(final, finish="STOP"))
        self.assertEqual(parts(result)[0].function_call.id, "late")
        self.assertEqual(parts(result)[0].thought_signature, b"late-sig")

    def test_sequential_same_name_without_ids_and_multiple_candidates(self):
        stream = ToolStreamAssembler()
        for index in (0, 1):
            for text in ("first", "second"):
                stream.restore_sdk(chunk(call(name="reply", will_continue=True), index=index))
                out = stream.restore_sdk(chunk(call(partial_args=[fragment("$.x", text)]), index=index))
                self.assertEqual(parts(out)[0].function_call.args, {"x": text})
        stream.validate_end()

    def test_complete_calls_remain_valid_with_streaming_requested(self):
        original = call(name="reply", args={"data": [1, True, None]})
        self.assertEqual(parts(ToolStreamAssembler().restore_sdk(chunk(original, finish="STOP"))), [original])

    def test_incomplete_calls_fail_at_finish_or_eof(self):
        for reason in (None, "STOP", "MAX_TOKENS", "SAFETY"):
            with self.subTest(reason=reason):
                stream = ToolStreamAssembler()
                stream.restore_sdk(chunk(call(name="reply", will_continue=True)))
                with self.assertRaises(IncompleteResponseError):
                    if reason:
                        stream.restore_sdk(chunk(finish=reason))
                    else:
                        stream.validate_end()

    def test_unclosed_string_and_duplicate_values_fail(self):
        for items in ([fragment("$.x", "unfinished", continuing=True)],
                      [fragment("$.x", "one"), fragment("$.x", "two")],
                      [fragment("$.x", "one"), fragment("$.x.y", "two")],
                      [fragment("$.rows[1]", "gap")],
                      [fragment("$.n", float("inf"), kind="number_value")]):
            with self.subTest(items=items), self.assertRaises(IncompleteResponseError):
                ToolStreamAssembler().restore_sdk(chunk(call(name="reply", partial_args=items)))

    def test_invalid_or_ambiguous_paths_and_parallel_fragments_fail(self):
        for path in ("$", "$.x[*]", "$..x", "$.x[999999999]", "$[0]", "$.a.*", "$.a-b"):
            with self.subTest(path=path), self.assertRaises(IncompleteResponseError):
                ToolStreamAssembler().restore_sdk(chunk(call(name="reply", partial_args=[fragment(path, "x")])))
        stream = ToolStreamAssembler()
        stream.restore_sdk(chunk(call(name="reply", id="a", will_continue=True),
                                 call(name="reply", id="b", will_continue=True)))
        with self.assertRaises(IncompleteResponseError):
            stream.restore_sdk(chunk(call(partial_args=[fragment("$.x", "ambiguous")])))

    def test_byte_and_call_limits_are_enforced(self):
        with mock.patch.object(tool_stream, "MAX_TOOL_STREAM_BYTES", 16), self.assertRaises(IncompleteResponseError):
            ToolStreamAssembler().restore_sdk(chunk(call(name="reply", will_continue=True)))
        with mock.patch.object(tool_stream, "MAX_TOOL_CALLS", 1), self.assertRaises(IncompleteResponseError):
            ToolStreamAssembler().restore_sdk(chunk(call(name="a", args={}), call(name="b", args={})))

    def test_empty_function_metadata_is_not_first_reply(self):
        self.assertFalse(first_reply.sdk_has_reply(chunk(call())))
        self.assertTrue(first_reply.sdk_has_reply(chunk(call(name="reply", will_continue=True))))


class DeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stats = ProxyStats()
        self.settings = {"fake_streaming": False, "fake_streaming_interval": 0,
                         "retry_max": 1, "retry_backoff_seconds": 0, "retry_jitter_seconds": 0}
        for patch in (mock.patch.object(api_helpers, "timeout_seconds", return_value=.03),
                      mock.patch.object(api_helpers, "stats", self.stats),
                      mock.patch.object(api_helpers.app_state, "get_setting",
                                        side_effect=lambda k, d=None: self.settings.get(k, d))):
            patch.start()
            self.addCleanup(patch.stop)

    async def run_stream(self, generate, *, model="gemini-4-argon", client=None, stream=True, choice="required"):
        config = {"tools": [{"function_declarations": [{"name": "reply", "parameters": {"type": "object"}}]}],
                  "tool_config": {"function_calling_config": {"mode": "NONE" if choice == "none" else "ANY",
                                                              "allowed_function_names": ["reply"]}}}
        if client is None:
            client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
                generate_content_stream=generate, generate_content=generate)))
        req = OpenAIRequest(model=model, messages=[{"role": "user", "content": "hello"}],
                            stream=stream, tools=[{"type": "function", "function": {"name": "reply"}}], tool_choice=choice)
        result = await api_helpers.execute_gemini_call(client, model,
            lambda messages: [types.Content(role="user", parts=[types.Part(text="hello")])], config, req)
        if not stream:
            return result.body.decode()
        return "".join([frame async for frame in result.body_iterator])

    async def test_slow_tool_generation_passes_without_extending_first_reply_deadline(self):
        for model in ("gemini-3.5-flash-lite", "gemini-4-argon-antitruncation-stream"):
            with self.subTest(model=model):
                seen = []
                async def generate(**kwargs):
                    seen.append(copy.deepcopy(kwargs["config"]))
                    async def source():
                        yield chunk(call(name="reply", will_continue=True))
                        yield chunk(call(will_continue=True, partial_args=[fragment("$.text", "first", continuing=True)]))
                        await asyncio.sleep(.08)
                        yield chunk(call(partial_args=[fragment("$.text", "last")]), finish="STOP")
                    return source()
                body = await self.run_stream(generate, model=model)
                self.assertEqual(len(seen), 1)
                self.assertTrue(seen[0]["tool_config"]["function_calling_config"]["stream_function_call_arguments"])
                self.assertEqual(seen[0]["tool_config"]["function_calling_config"]["allowed_function_names"], ["reply"])
                self.assertEqual(seen[0]["automatic_function_calling"], {"disable": True})
                calls = tool_calls(body)
                self.assertEqual(len(calls), 1)
                self.assertEqual(json.loads(calls[0]["function"]["arguments"]), {"text": "firstlast"})
                self.assertNotIn('"error"', body)

    async def test_silent_queue_with_metadata_still_times_out_and_closes_before_retry(self):
        attempts, closed = 0, asyncio.Event()
        async def generate(**kwargs):
            nonlocal attempts
            attempts += 1
            if attempts > 1:
                self.assertTrue(closed.is_set())
            async def source():
                if attempts == 1:
                    try:
                        while True:
                            yield types.GenerateContentResponse(usage_metadata=types.GenerateContentResponseUsageMetadata(prompt_token_count=1))
                            await asyncio.sleep(.002)
                    finally:
                        closed.set()
                yield chunk(call(name="reply", args={"text": "success"}), finish="STOP")
            return source()
        body = await self.run_stream(generate)
        self.assertEqual(attempts, 2)
        self.assertEqual(self.stats.retry_counts, 1)
        self.assertEqual(json.loads(tool_calls(body)[0]["function"]["arguments"]), {"text": "success"})

    async def test_failed_partial_call_is_discarded_before_retry(self):
        attempts = 0
        async def generate(**kwargs):
            nonlocal attempts
            attempts += 1
            async def source():
                if attempts == 1:
                    yield chunk(call(name="reply", will_continue=True,
                        partial_args=[fragment("$.text", "discarded", continuing=True)]))
                    raise httpx.ReadTimeout("network interruption")
                yield chunk(call(name="reply", args={"text": "replacement"}), finish="STOP")
            return source()
        body = await self.run_stream(generate)
        self.assertEqual(attempts, 2)
        self.assertNotIn("discarded", body)
        self.assertEqual(len(tool_calls(body)), 1)

    async def test_visible_output_prevents_retry_after_partial_tool_failure(self):
        attempts = 0
        async def generate(**kwargs):
            nonlocal attempts
            attempts += 1
            async def source():
                yield chunk(types.Part(text="visible"))
                yield chunk(call(name="reply", will_continue=True))
                raise httpx.ReadTimeout("network interruption")
            return source()
        body = await self.run_stream(generate)
        self.assertEqual(attempts, 1)
        self.assertIn("visible", body)
        self.assertIn('"error"', body)
        self.assertEqual(tool_calls(body), [])

    async def test_preview_rejection_falls_back_once_without_changing_tool_choice(self):
        seen = []
        async def generate(**kwargs):
            seen.append(copy.deepcopy(kwargs["config"]))
            if len(seen) == 1:
                raise errors.ClientError(400, {"error": {"message": "streamFunctionCallArguments is not supported"}})
            async def source():
                yield chunk(call(name="reply", args={"text": "fallback"}), finish="STOP")
            return source()
        body = await self.run_stream(generate)
        self.assertEqual(len(seen), 2)
        self.assertNotIn("stream_function_call_arguments", seen[1]["tool_config"]["function_calling_config"])
        self.assertEqual(seen[1]["tool_config"]["function_calling_config"]["mode"], "ANY")
        self.assertEqual(len(tool_calls(body)), 1)

    async def test_actual_sdk_serializes_flag_and_parses_native_fragments(self):
        captured = []
        def respond(req):
            captured.append(json.loads(req.content))
            events = [{"name": "reply", "willContinue": True},
                      {"partialArgs": [{"jsonPath": "$.text", "stringValue": "wire", "willContinue": True}], "willContinue": True},
                      {"partialArgs": [{"jsonPath": "$.text", "stringValue": " answer"}]}]
            frames = []
            for i, fc in enumerate(events):
                frames.append({"candidates": [{"content": {"role": "model", "parts": [{"functionCall": fc}]},
                                                **({"finishReason": "STOP"} if i == len(events) - 1 else {})}]})
            return httpx.Response(200, headers={"content-type": "text/event-stream"},
                                  text="".join("data: " + json.dumps(frame) + "\n\n" for frame in frames))
        transport = httpx.MockTransport(respond)
        client = genai.Client(vertexai=True, api_key="offline", http_options=types.HttpOptions(
            async_client_args={"transport": transport, "trust_env": False},
            client_args={"transport": transport, "trust_env": False}))
        try:
            # This checks the real SDK wire contract, not a scheduling deadline.
            with mock.patch.object(api_helpers, "timeout_seconds", return_value=2):
                body = await self.run_stream(None, client=client)
            self.assertEqual(json.loads(tool_calls(body)[0]["function"]["arguments"]), {"text": "wire answer"})
            self.assertEqual(len(captured), 1)
            fcc = captured[0]["toolConfig"]["functionCallingConfig"]
            self.assertTrue(fcc.get("streamFunctionCallArguments", fcc.get("stream_function_call_arguments")))
        finally:
            await client.aio.aclose()
            client.close()

    async def test_nonstream_fake_stream_disabled_tools_and_unsupported_models_keep_existing_path(self):
        cases = [("gemini-4-argon", False, False, "required"),
                 ("gemini-4-argon", True, True, "required"),
                 ("gemini-2.5-flash", True, False, "required"),
                 ("gemini-3.1-flash-lite", True, False, "required"),
                 ("gemini-4-argon", True, False, "none")]
        for model, streaming, fake, choice in cases:
            with self.subTest(model=model, streaming=streaming, fake=fake, choice=choice):
                self.settings["fake_streaming"] = fake
                async def generate(**kwargs):
                    self.assertNotIn("stream_function_call_arguments", str(kwargs["config"]))
                    result = chunk(types.Part(text="ordinary"), finish="STOP")
                    if not streaming or fake:
                        return result
                    async def source():
                        yield result
                    return source()
                self.assertIn("ordinary", await self.run_stream(generate, model=model, stream=streaming, choice=choice))


if __name__ == "__main__":
    unittest.main()
