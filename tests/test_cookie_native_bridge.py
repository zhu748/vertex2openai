# -*- coding: utf-8 -*-
"""Gemini 原生格式 ⇄ Cookie 通道桥接（upstreams/cookie_native）的回归测试。

覆盖：请求转 variables（systemInstruction/tools/结构化输出门控/生图补默认）、
事件流 → 原生响应（流式 parts / 非流式整包 / promptFeedback / inlineData 反解 /
finishReason 缺失报错）、toolConfig 收窄、SSE 错误帧格式。
"""
import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

_TEST_STATE_DIR = tempfile.TemporaryDirectory(prefix="v2o-native-bridge-")
os.environ["STATE_DIR"] = _TEST_STATE_DIR.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
os.chdir(Path(__file__).resolve().parents[1] / "app")

from upstreams import cookie_native  # noqa: E402
from upstreams.cookie_native import (  # noqa: E402
    NativeAssembler,
    NativeBridgeError,
    _google_error_body,
    native_request_to_variables,
)
from schema_validation import SchemaValidationError  # noqa: E402


def _settings(**overrides):
    base = {"cookie_structured_output": False, "safety_score": False, **overrides}
    return mock.patch.object(cookie_native.app_state, "get_setting",
                             side_effect=lambda key, default=None: base.get(key, default))


class RequestConversionTests(unittest.TestCase):
    def test_basic_body_maps_contents_and_generation_config(self):
        parsed = {
            "contents": [{"role": "user", "parts": [{"text": "你好"}]}],
            "generationConfig": {"temperature": 0.7, "maxOutputTokens": 512},
        }
        with _settings():
            variables = native_request_to_variables(parsed, "gemini-3.5-flash-lite", "proj-1")
        self.assertEqual(variables["contents"], parsed["contents"])
        self.assertEqual(variables["generationConfig"]["temperature"], 0.7)
        self.assertEqual(
            variables["model"],
            "projects/proj-1/locations/global/publishers/google/models/gemini-3.5-flash-lite")
        # 未带 safetySettings 时注入默认全 OFF 集
        self.assertTrue(variables["safetySettings"])
        self.assertTrue(all(s["threshold"] == "OFF" for s in variables["safetySettings"]))
        self.assertNotIn("tools", variables)

    def test_system_instruction_string_normalized(self):
        parsed = {"contents": [], "systemInstruction": "你是助手"}
        with _settings():
            variables = native_request_to_variables(parsed, "gemini-3.5-flash-lite", "p")
        self.assertEqual(variables["systemInstruction"], {"parts": [{"text": "你是助手"}]})

    def test_tools_converted_via_ui_schema_and_search_flag(self):
        parsed = {
            "contents": [],
            "tools": [
                {"functionDeclarations": [{
                    "name": "get_weather",
                    "parameters": {"type": "object",
                                   "properties": {"city": {"type": "string"}}},
                }]},
                {"google_search": {}},
            ],
        }
        with mock.patch.object(cookie_native, "print"):
            with _settings():
                variables = native_request_to_variables(parsed, "gemini-3.5-flash-lite", "p")
        decls = variables["tools"][0]["functionDeclarations"]
        self.assertEqual(decls[0]["name"], "get_weather")
        # properties 变成 Studio 私有的 [{key, value}] 形态
        props = decls[0]["parameters"]["properties"]
        self.assertEqual(props, [{"key": "city", "value": {"type": "STRING"}}])

    def test_google_search_only(self):
        parsed = {"contents": [], "tools": [{"googleSearch": {}}]}
        with _settings():
            variables = native_request_to_variables(parsed, "gemini-3.5-flash-lite", "p")
        self.assertEqual(variables["tools"], [{"googleSearch": {}}])

    def test_structured_output_gated_like_openai_channel(self):
        parsed = {"contents": [], "generationConfig": {"responseMimeType": "application/json"}}
        with _settings():
            with self.assertRaises(SchemaValidationError):
                native_request_to_variables(parsed, "gemini-3.5-flash-lite", "p")
        with _settings(cookie_structured_output=True):
            variables = native_request_to_variables(parsed, "gemini-3.5-flash-lite", "p")
        self.assertEqual(variables["generationConfig"]["responseMimeType"], "application/json")

    def test_response_schema_converted_when_enabled(self):
        parsed = {"contents": [], "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {"type": "object", "properties": {"a": {"type": "string"}}}}}
        with _settings(cookie_structured_output=True):
            variables = native_request_to_variables(parsed, "gemini-3.5-flash-lite", "p")
        schema = variables["generationConfig"]["responseSchema"]
        self.assertEqual(schema["properties"], [{"key": "a", "value": {"type": "STRING"}}])

    def test_image_model_gets_default_modalities(self):
        parsed = {"contents": [], "generationConfig": {}}
        with _settings():
            variables = native_request_to_variables(parsed, "gemini-nano-banana-2.1", "p")
        self.assertEqual(variables["generationConfig"]["responseModalities"], ["TEXT", "IMAGE"])

    def test_tool_config_allowed_names_narrowed(self):
        parsed = {
            "contents": [],
            "tools": [{"functionDeclarations": [{"name": "f1", "parameters": {"type": "object"}}]}],
            "toolConfig": {"functionCallingConfig": {
                "mode": "ANY", "allowedFunctionNames": ["f1", "google_search"]}},
        }
        with mock.patch.object(cookie_native, "print"):
            with _settings():
                variables = native_request_to_variables(parsed, "gemini-3.5-flash-lite", "p")
        self.assertEqual(
            variables["toolConfig"]["functionCallingConfig"]["allowedFunctionNames"], ["f1"])


class NativeAssemblerTests(unittest.TestCase):
    def test_stream_parts_and_terminal(self):
        asm = NativeAssembler("gemini-3.5-flash-lite")
        self.assertEqual(asm.stream_event("text", "你好"), {"text": "你好"})
        self.assertEqual(asm.stream_event("thought", "思考"), {"text": "思考", "thought": True})
        self.assertIsNone(asm.stream_event("usage", {"totalTokenCount": 5}))
        self.assertIsNone(asm.stream_event("safety", [{"category": "X"}]))
        self.assertIsNone(asm.stream_event("finish", "STOP"))
        terminal = asm.terminal()
        self.assertEqual(terminal["candidates"][0]["finishReason"], "STOP")
        self.assertEqual(terminal["candidates"][0]["safetyRatings"], [{"category": "X"}])
        self.assertEqual(terminal["usageMetadata"], {"totalTokenCount": 5})
        self.assertEqual(terminal["modelVersion"], "gemini-3.5-flash-lite")

    def test_function_call_part_with_signature(self):
        asm = NativeAssembler("m")
        part = asm.feed("function_call", {
            "name": "f", "args": {"x": 1}, "thought_signature": "c2ln"})
        self.assertEqual(part, {"functionCall": {"name": "f", "args": {"x": 1}},
                                "thoughtSignature": "c2ln"})

    def test_image_markdown_reversed_to_inline_data(self):
        asm = NativeAssembler("m")
        part = asm.feed("image", "![Generated Image](data:image/png;base64,QUJD)")
        self.assertEqual(part, {"inlineData": {"mimeType": "image/png", "data": "QUJD"}})

    def test_missing_finish_raises_incomplete(self):
        from response_validation import IncompleteResponseError
        asm = NativeAssembler("m")
        asm.feed("text", "部分回复")
        with self.assertRaises(IncompleteResponseError):
            asm.terminal()

    def test_blocked_without_parts_returns_prompt_feedback(self):
        asm = NativeAssembler("m")
        asm.feed("blocked", "SAFETY（违规）")
        payload = asm.payload()
        self.assertEqual(payload, {"promptFeedback": {"blockReason": "SAFETY"}})

    def test_nonstream_payload_shape(self):
        asm = NativeAssembler("m")
        asm.feed("text", "答案")
        asm.feed("finish", "STOP")
        asm.feed("usage", {"promptTokenCount": 3})
        payload = asm.payload()
        self.assertEqual(payload["candidates"][0]["content"]["parts"], [{"text": "答案"}])
        self.assertEqual(payload["candidates"][0]["finishReason"], "STOP")
        self.assertEqual(payload["usageMetadata"], {"promptTokenCount": 3})

    def test_standalone_signature_attaches_to_last_part(self):
        asm = NativeAssembler("m")
        asm.feed("text", "hi")
        asm.feed("thought_signature", {"value": "c2ln", "part_kind": "text"})
        self.assertEqual(asm.parts[-1], {"text": "hi", "thoughtSignature": "c2ln"})


class GoogleErrorBodyTests(unittest.TestCase):
    def test_status_details(self):
        self.assertEqual(_google_error_body(429, "x")["error"]["status"], "RESOURCE_EXHAUSTED")
        self.assertEqual(_google_error_body(401, "x")["error"]["status"], "UNAUTHENTICATED")
        self.assertEqual(_google_error_body(503, "x")["error"]["status"], "UNAVAILABLE")
        self.assertEqual(
            _google_error_body(400, "x", "INVALID_ARGUMENT")["error"]["status"], "INVALID_ARGUMENT")


class RetryPumpTests(unittest.TestCase):
    def test_retryable_error_before_content_retries_then_succeeds(self):
        """首个内容前的 429 触发整体重发;退避心跳以 heartbeat 事件透出。"""
        from upstreams.cookie_native import _events_with_retry
        record = {"cookie": "SAPISID=x; SID=y", "project_id": "p", "id": "a1", "enabled": True}
        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))

        err = _StreamRequestErrorSafe("Resource exhausted")

        calls = {"n": 0}

        async def fake_events(client, headers, body, sampler=None, fallback_body=None, *, model=""):
            calls["n"] += 1
            if calls["n"] == 1:
                yield "retryable_error", err
            else:
                yield "text", "ok"
                yield "finish", "STOP"

        with mock.patch.object(cookie_native, "build_headers", return_value={"authorization": "x"}), \
             mock.patch.object(cookie_native, "open_upstream_client", new=_fake_client_ctx), \
             mock.patch.object(cookie_native, "_execute_stream_request_generator", fake_events), \
             mock.patch.object(cookie_native, "_sleep_with_heartbeat", new=_no_wait_heartbeat), \
             mock.patch.object(cookie_native, "ProxyChain", return_value=SimpleNamespace(
                 bind_to_generation=lambda self=None: None)), \
             mock.patch.object(cookie_native, "get_retry_settings", return_value=(3, 0.0, 0.0)):
            async def run():
                return [(k, d) async for k, d in _events_with_retry(request, record, {}, "m")]
            events = asyncio.run(run())
        self.assertEqual(calls["n"], 2)
        self.assertEqual([e[0] for e in events], ["heartbeat", "text", "finish"])

    def test_cookie_error_records_health_failure_and_raises(self):
        from upstreams.cookie_native import _events_with_retry
        record = {"cookie": "SAPISID=x; SID=y", "project_id": "p", "id": "a1", "enabled": True}
        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))

        async def fake_events(client, headers, body, sampler=None, fallback_body=None, *, model=""):
            yield "cookie_error", _StreamRequestErrorSafe("Login Required", status_code=401)

        with mock.patch.object(cookie_native, "build_headers", return_value={"authorization": "x"}), \
             mock.patch.object(cookie_native, "open_upstream_client", new=_fake_client_ctx), \
             mock.patch.object(cookie_native, "_execute_stream_request_generator", fake_events), \
             mock.patch.object(cookie_native, "ProxyChain", return_value=SimpleNamespace(
                 bind_to_generation=lambda self=None: None)), \
             mock.patch.object(cookie_native.credential_manager, "record_cookie_auth_failure") as fail:
            async def run():
                try:
                    async for _ in _events_with_retry(request, record, {}, "m"):
                        pass
                except NativeBridgeError as exc:
                    return exc
            exc = asyncio.run(run())
        self.assertEqual(exc.code, 401)
        fail.assert_called_once_with("a1")


# —— 测试夹具 ——————————————————————————————
class _StreamRequestErrorSafe(str):
    def __new__(cls, message, status_code=None):
        value = super().__new__(cls, message)
        value.status_code = status_code
        return value


class _fake_client_ctx:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


async def _no_wait_heartbeat(total_sec, hb_interval=3.0, request=None):
    yield None


if __name__ == "__main__":
    unittest.main()
