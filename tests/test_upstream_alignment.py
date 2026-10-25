"""上游对齐专项回归：坏凭证隔离、可重试关键词扩充与跨通道兜底。

对齐 bad-woman/vertex2openai 2026-10-23 批次的三项核心优化：
1. RETRYABLE_MARKERS 补充网络截断短语（异常类型信息被包装丢失时的最后防线）；
2. 坏凭证（鉴权失效/欠费）当轮隔离 + 红色告警 + 立即切换；
3. 跨通道兜底（Express ↔ Cookie 互备）：未输出内容前的可兜底错误切换备用通道。
"""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

_TEMP = tempfile.TemporaryDirectory(prefix="v2o-upstream-align-")
os.environ["STATE_DIR"] = _TEMP.name
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

import config
import retry_policy
from express_key_manager import ExpressKeyManager
from fastapi.responses import JSONResponse, StreamingResponse
from models import OpenAIRequest
from routes import chat_api
from runtime_state import app_state
from upstream_errors import create_openai_error_response


class _Err(Exception):
    """带可选 HTTP 状态的测试异常（对齐 error_status 读取的 .code 形态）。"""

    def __init__(self, message, code=None):
        super().__init__(message)
        if code is not None:
            self.code = code


def _api_error(code: int, message: str = "") -> _Err:
    return _Err(message or f"HTTP {code}", code=code)


def _err_frame(code: int, message: str) -> str:
    return "data: " + json.dumps(
        create_openai_error_response(code, message, "upstream_error"), ensure_ascii=False) + "\n\n"


class RetryMarkerTests(unittest.TestCase):
    """上游补充的网络截断 / 5xx 文本短语必须被判为可重试。"""

    def test_incomplete_chunked_read_is_retryable(self):
        self.assertTrue(retry_policy.is_retryable_message(
            "peer closed connection without sending complete message body "
            "(incomplete chunked read)"))

    def test_peer_closed_connection_is_retryable(self):
        self.assertTrue(retry_policy.is_retryable_message(
            "peer closed connection without sending complete message body"))

    def test_server_disconnected_and_connection_reset_are_retryable(self):
        for text in ("server disconnected", "connection reset", "pooltimeout",
                     "deadline_exceeded", "backend error"):
            self.assertTrue(retry_policy.is_retryable_message(text), text)

    def test_wrapped_exception_without_type_falls_back_to_text(self):
        # 异常被包装成普通 Exception（类型信息丢失）时仍应可重试
        wrapped = Exception("RuntimeError: peer closed connection (incomplete chunked read)")
        self.assertTrue(retry_policy.is_retryable_exception(wrapped))

    def test_status_boundary_still_guards_false_positives(self):
        # 既有防线不回归：数字边界防止 "1500ms" 之类误报
        self.assertFalse(retry_policy.is_retryable_message("elapsed 1500ms"))
        self.assertFalse(retry_policy.is_retryable_message("port 4292 refused"))

    def test_permanent_status_codes_stay_non_retryable(self):
        for code in (400, 401, 403, 404):
            self.assertFalse(retry_policy.is_retryable_exception(_api_error(code, "quota")))
        self.assertFalse(retry_policy.is_retryable_exception(
            retry_policy.IncompleteResponseError("响应不完整")) if hasattr(
                retry_policy, "IncompleteResponseError") else False)


class AuthOrBillingTests(unittest.TestCase):
    """坏凭证分类：401/403/欠费关键词命中，参数错误与 429 不命中。"""

    def test_status_401_403_are_bad_credentials(self):
        self.assertTrue(retry_policy.is_auth_or_billing_error(_api_error(401)))
        self.assertTrue(retry_policy.is_auth_or_billing_error(_api_error(403)))

    def test_billing_markers_are_bad_credentials(self):
        for text in ("Project requires billing to be enabled",
                     "API_KEY_INVALID", "api key not valid",
                     "The caller does not have permission (permission_denied)",
                     "account has been suspended (account_suspended)"):
            self.assertTrue(retry_policy.is_auth_or_billing_error(_Err(text)), text)

    def test_value_errors_are_not_credential_issues(self):
        # 对齐上游：本地参数构造错误与凭证无关
        self.assertFalse(retry_policy.is_auth_or_billing_error(ValueError("bad param")))
        self.assertFalse(retry_policy.is_auth_or_billing_error(TypeError("missing field")))

    def test_rate_limit_is_not_bad_credential(self):
        err = _api_error(429, "too many requests")
        self.assertFalse(retry_policy.is_auth_or_billing_error(err))
        self.assertTrue(retry_policy.is_retryable_exception(err))

    def test_none_is_not_bad_credential(self):
        self.assertFalse(retry_policy.is_auth_or_billing_error(None))

    def test_explicit_flag_is_honored(self):
        err = _Err("opaque")
        err.is_auth_or_billing = True
        self.assertTrue(retry_policy.is_auth_or_billing_error(err))


class KeyIsolationTests(unittest.TestCase):
    """express_key_manager：exclude_keys 硬排除坏凭证，同时保留单 Key 全预算语义。"""

    def setUp(self):
        self.manager = ExpressKeyManager()
        self.records = [
            {"key": "k-bad-1", "project_id": "p1", "enabled": True},
            {"key": "k-good-2", "project_id": "p2", "enabled": True},
            {"key": "k-good-3", "project_id": "p3", "enabled": True},
        ]
        patcher = mock.patch.object(app_state, "get_express_keys", return_value=self.records)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_dead_keys_are_excluded_from_selection(self):
        for _ in range(20):
            rec = self.manager.get_express_record(exclude_keys={"k-bad-1"})
            self.assertIsNotNone(rec)
            self.assertNotEqual(rec["key"], "k-bad-1")

    def test_all_keys_dead_returns_none(self):
        self.assertIsNone(self.manager.get_express_record(
            exclude_keys={"k-bad-1", "k-good-2", "k-good-3"}))

    def test_single_key_keeps_full_retry_budget(self):
        # 单 Key + exclude_key 软排除：仍复用同一把（吃满重试预算的既有语义）
        single = [{"key": "k-only", "project_id": "p9", "enabled": True}]
        with mock.patch.object(app_state, "get_express_keys", return_value=single):
            rec = self.manager.get_express_record(exclude_key="k-only")
            self.assertIsNotNone(rec)
            self.assertEqual(rec["key"], "k-only")
            # 但若这把 Key 是坏凭证（硬排除）→ None
            self.assertIsNone(self.manager.get_express_record(
                exclude_key="k-only", exclude_keys={"k-only"}))

    def test_soft_exclude_prefers_alternatives_but_keeps_them(self):
        # 软排除只避开刚失败的：多 Key 时不再选中它
        for _ in range(20):
            rec = self.manager.get_express_record(exclude_key="k-good-2")
            self.assertNotEqual(rec["key"], "k-good-2")


class RetryFactoryErrorPropagationTests(unittest.TestCase):
    """_call_retry_factory：新式工厂收到 error，旧式无参工厂仍可调用。"""

    def test_factory_receives_error_keyword(self):
        seen = {}

        def factory(error=None):
            seen["error"] = error

        err = _api_error(401)
        chat_api  # noqa: B018 - 确认模块可导入
        from api_helpers import _call_retry_factory
        _call_retry_factory(factory, err)
        self.assertIs(seen["error"], err)

    def test_legacy_no_arg_factory_still_works(self):
        # 测试桩常用的无参工厂不能因为新参数而 TypeError
        from api_helpers import _call_retry_factory
        self.assertIsNone(_call_retry_factory(lambda: None, _api_error(401)))
        self.assertEqual(_call_retry_factory(lambda: "x", _api_error(401)), "x")

    def test_var_keyword_factory_receives_error(self):
        seen = {}

        def factory(**kwargs):
            seen.update(kwargs)

        from api_helpers import _call_retry_factory
        err = _api_error(403)
        _call_retry_factory(factory, err)
        self.assertIs(seen.get("error"), err)

    def test_none_error_uses_plain_call(self):
        seen = []

        def factory(error=None):
            seen.append(error)
            return "ok"

        from api_helpers import _call_retry_factory
        self.assertEqual(_call_retry_factory(factory, None), "ok")
        self.assertEqual(seen, [None])


class SSEFrameKindTests(unittest.TestCase):
    """_sse_frame_kind：帧分类是跨通道兜底切换安全线的基础。"""

    def test_keepalive_comment_and_data_frames(self):
        self.assertEqual(chat_api._sse_frame_kind(": keep-alive\n\n")[0], "keepalive")
        ka = json.dumps({"id": "chatcmpl-keepalive", "choices": [
            {"index": 0, "delta": {"content": ""}, "finish_reason": None}]})
        self.assertEqual(chat_api._sse_frame_kind(f"data: {ka}\n\n")[0], "keepalive")

    def test_done_frame(self):
        self.assertEqual(chat_api._sse_frame_kind("data: [DONE]\n\n")[0], "done")

    def test_error_frame_carries_code(self):
        kind, code = chat_api._sse_frame_kind(_err_frame(429, "busy"))
        self.assertEqual(kind, "error")
        self.assertEqual(code, 429)

    def test_content_frames(self):
        for delta in ({"content": "hi"}, {"reasoning_content": "thinking"},
                      {"tool_calls": [{"index": 0, "function": {"name": "f"}}]}):
            frame = "data: " + json.dumps({"choices": [
                {"index": 0, "delta": delta, "finish_reason": None}]}, ensure_ascii=False) + "\n\n"
            self.assertEqual(chat_api._sse_frame_kind(frame)[0], "content", delta)
        finish = "data: " + json.dumps({"choices": [
            {"index": 0, "delta": {}, "finish_reason": "stop"}]}, ensure_ascii=False) + "\n\n"
        self.assertEqual(chat_api._sse_frame_kind(finish)[0], "content")

    def test_role_only_frame_is_not_content(self):
        frame = "data: " + json.dumps({"choices": [
            {"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}, ensure_ascii=False) + "\n\n"
        self.assertEqual(chat_api._sse_frame_kind(frame)[0], "other")

    def test_empty_content_is_not_content(self):
        frame = "data: " + json.dumps({"choices": [
            {"index": 0, "delta": {"content": ""}, "finish_reason": None}]}, ensure_ascii=False) + "\n\n"
        self.assertEqual(chat_api._sse_frame_kind(frame)[0], "other")


class FallbackEligibilityTests(unittest.TestCase):
    """可兜底状态码分类：429/401/403/5xx 可兜底，400/404/422 不可兜底。"""

    def test_eligible_codes(self):
        for code in (401, 403, 429, 500, 502, 503, 504, 599):
            self.assertTrue(chat_api._fallback_eligible_code(code), code)

    def test_ineligible_codes(self):
        for code in (200, 302, 400, 404, 422, 499):
            self.assertFalse(chat_api._fallback_eligible_code(code), code)

    def test_non_int_codes_are_ineligible(self):
        for bad in (None, "429", 429.0, "error"):
            self.assertFalse(chat_api._fallback_eligible_code(bad), bad)


class OtherChannelTests(unittest.TestCase):
    """备用通道选择：开关、凭证就绪与结构化输出门控。"""

    def setUp(self):
        self.request_stub = SimpleNamespace(
            state=SimpleNamespace(express_key_manager=SimpleNamespace(get_total_keys=lambda: 2)))
        self.plain = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=True)
        saved = app_state._state
        self.addCleanup(setattr, app_state, "_state", saved)
        app_state._state = dict(saved)

    def test_express_primary_needs_cookie_record(self):
        app_state.update_settings({"cross_channel_fallback": True})
        with mock.patch.object(chat_api.credential_manager, "get_cookie_record", return_value=None):
            self.assertIsNone(chat_api._other_channel(self.request_stub, "express", self.plain))
        cookie_rec = {"cookie": "SAPISID=x", "project_id": "p", "enabled": True, "id": "c1"}
        with mock.patch.object(chat_api.credential_manager, "get_cookie_record",
                               return_value=cookie_rec):
            self.assertEqual(chat_api._other_channel(self.request_stub, "express", self.plain), "cookie")

    def test_cookie_primary_needs_express_keys(self):
        app_state.update_settings({"cross_channel_fallback": True})
        self.assertEqual(chat_api._other_channel(self.request_stub, "cookie", self.plain), "express")
        empty = SimpleNamespace(state=SimpleNamespace(
            express_key_manager=SimpleNamespace(get_total_keys=lambda: 0)))
        self.assertIsNone(chat_api._other_channel(empty, "cookie", self.plain))

    def test_switch_disables_fallback(self):
        app_state.update_settings({"cross_channel_fallback": False})
        cookie_rec = {"cookie": "SAPISID=x", "project_id": "p", "enabled": True, "id": "c1"}
        with mock.patch.object(chat_api.credential_manager, "get_cookie_record",
                               return_value=cookie_rec):
            self.assertIsNone(chat_api._other_channel(self.request_stub, "express", self.plain))

    def test_structured_output_locks_express(self):
        app_state.update_settings({"cross_channel_fallback": True, "cookie_structured_output": False})
        structured_req = OpenAIRequest(
            model="gemini-3.6-flash", messages=[], stream=True,
            response_format={"type": "json_schema"})
        cookie_rec = {"cookie": "SAPISID=x", "project_id": "p", "enabled": True, "id": "c1"}
        with mock.patch.object(chat_api.credential_manager, "get_cookie_record",
                               return_value=cookie_rec):
            # Express 主通道 + 结构化输出 → 不允许切 Cookie
            self.assertIsNone(chat_api._other_channel(self.request_stub, "express", structured_req))
            # Cookie 主通道 + 结构化输出 → 仍可切回 Express（Cookie 侧已放开）
            self.assertEqual(chat_api._other_channel(self.request_stub, "cookie", structured_req), "express")
        # 打开 cookie_structured_output 后 Express 也可兜底到 Cookie
        app_state.update_settings({"cookie_structured_output": True})
        with mock.patch.object(chat_api.credential_manager, "get_cookie_record",
                               return_value=cookie_rec):
            self.assertEqual(chat_api._other_channel(self.request_stub, "express", structured_req), "cookie")

    def test_unknown_primary_disables_fallback(self):
        self.assertIsNone(chat_api._other_channel(self.request_stub, "", self.plain))


def _stream_response(frames):
    async def gen():
        for f in frames:
            yield f
    return StreamingResponse(gen(), media_type="text/event-stream")


class StreamFallbackTests(unittest.TestCase):
    """流式兜底切换：错误前置吞掉切换，内容后置绝不切换。"""

    def setUp(self):
        saved = app_state._state
        self.addCleanup(setattr, app_state, "_state", saved)
        app_state._state = dict(saved)
        app_state.update_settings({"cross_channel_fallback": True})
        self.request_obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=True)
        self.req_stub = SimpleNamespace(state=SimpleNamespace(
            express_key_manager=SimpleNamespace(get_total_keys=lambda: 1)))

    def _run(self, response, deadline=30.0):
        frames = []
        # 场景：主通道 cookie 失败 → 兜底切换 express
        gen = chat_api._stream_with_fallback_frames(
            response, self.request_obj, self.req_stub,
            fallback_channel="express", primary_channel="cookie", deadline=deadline)

        async def collect():
            async for f in gen:
                frames.append(f)

        asyncio.run(collect())
        return frames

    def test_error_before_content_switches_channel(self):
        err_frame = _err_frame(401, "Cookie 登录态已失效")
        primary = _stream_response([
            ": keep-alive\n\n",
            err_frame,
            "data: [DONE]\n\n",
        ])
        alt_frames = [
            "data: " + json.dumps({"choices": [
                {"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}, ensure_ascii=False) + "\n\n",
            "data: " + json.dumps({"choices": [
                {"index": 0, "delta": {"content": "接力回复"}, "finish_reason": None}]}, ensure_ascii=False) + "\n\n",
            "data: [DONE]\n\n",
        ]
        alt_response = _stream_response(alt_frames)

        async def fake_invoke(request_obj, fastapi_request, channel):
            self.assertEqual(channel, "express")
            return alt_response

        with mock.patch.object(chat_api, "_invoke_channel", fake_invoke):
            frames = self._run(primary)
        # 主通道错误帧与 [DONE] 被吞掉，客户端只看到备用通道输出
        joined = "".join(frames)
        self.assertNotIn("Cookie 登录态已失效", joined)
        self.assertIn("接力回复", joined)
        self.assertEqual(joined.count("[DONE]"), 1)

    def test_error_after_content_never_switches(self):
        err_frame = _err_frame(502, "中途断流")
        content = "data: " + json.dumps({"choices": [
            {"index": 0, "delta": {"content": "半截答案"}, "finish_reason": None}]}, ensure_ascii=False) + "\n\n"
        primary = _stream_response([content, err_frame, "data: [DONE]\n\n"])

        calls = []

        async def fake_invoke(*args, **kwargs):
            calls.append(args)
            return _stream_response(["data: [DONE]\n\n"])

        with mock.patch.object(chat_api, "_invoke_channel", fake_invoke):
            frames = self._run(primary)
        joined = "".join(frames)
        # 已输出内容：错误如实下发，绝不二次调用（半截答案不能重发）
        self.assertIn("半截答案", joined)
        self.assertIn("中途断流", joined)
        self.assertEqual(calls, [])

    def test_ineligible_error_is_passed_through(self):
        err_frame = _err_frame(400, "参数非法")
        primary = _stream_response([err_frame, "data: [DONE]\n\n"])

        async def fake_invoke(*a, **k):  # pragma: no cover - 不应被调用
            raise AssertionError("400 不应触发兜底")

        with mock.patch.object(chat_api, "_invoke_channel", fake_invoke):
            frames = self._run(primary)
        self.assertIn("参数非法", "".join(frames))

    def test_normal_stream_untouched(self):
        content = "data: " + json.dumps({"choices": [
            {"index": 0, "delta": {"content": "正常回复"}, "finish_reason": None}]}, ensure_ascii=False) + "\n\n"
        primary = _stream_response([": keep-alive\n\n", content, "data: [DONE]\n\n"])

        async def fake_invoke(*a, **k):  # pragma: no cover - 不应被调用
            raise AssertionError("正常流不应触发兜底")

        with mock.patch.object(chat_api, "_invoke_channel", fake_invoke):
            frames = self._run(primary)
        self.assertIn("正常回复", "".join(frames))

    def test_fallback_failure_replays_primary_error(self):
        err_frame = _err_frame(429, "上游拥堵")
        primary = _stream_response([err_frame, "data: [DONE]\n\n"])

        async def failing_invoke(*a, **k):
            raise RuntimeError("备用通道崩溃")

        with mock.patch.object(chat_api, "_invoke_channel", failing_invoke):
            frames = self._run(primary)
        joined = "".join(frames)
        # 备用启动失败 → 回放主通道错误帧 + [DONE]
        self.assertIn("上游拥堵", joined)
        self.assertIn("[DONE]", joined)


class JsonFallbackTests(unittest.TestCase):
    """非流式兜底：可兜底错误换通道重试一次，失败保留主通道错误。"""

    def setUp(self):
        saved = app_state._state
        self.addCleanup(setattr, app_state, "_state", saved)
        app_state._state = dict(saved)
        app_state.update_settings({"cross_channel_fallback": True})
        self.request_obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=False)
        self.req_stub = SimpleNamespace(state=SimpleNamespace(
            express_key_manager=SimpleNamespace(get_total_keys=lambda: 1)))

    def test_eligible_error_uses_fallback_success(self):
        primary = JSONResponse(status_code=429, content={"error": {"message": "busy"}})
        success = JSONResponse(status_code=200, content={"id": "x", "choices": []})

        async def fake_invoke(request_obj, fastapi_request, channel):
            self.assertEqual(channel, "express")
            return success

        with mock.patch.object(chat_api, "_invoke_channel", fake_invoke):
            result = asyncio.run(chat_api._apply_json_fallback(
                primary, self.request_obj, self.req_stub, "cookie", 30.0))
        self.assertIs(result, success)

    def test_fallback_failure_keeps_primary_error(self):
        primary = JSONResponse(status_code=503, content={"error": {"message": "down"}})

        async def failing_invoke(*a, **k):
            raise RuntimeError("备用崩溃")

        with mock.patch.object(chat_api, "_invoke_channel", failing_invoke):
            result = asyncio.run(chat_api._apply_json_fallback(
                primary, self.request_obj, self.req_stub, "cookie", 30.0))
        self.assertIs(result, primary)

    def test_ineligible_error_returns_asis(self):
        primary = JSONResponse(status_code=400, content={"error": {"message": "bad request"}})
        result = asyncio.run(chat_api._apply_json_fallback(
            primary, self.request_obj, self.req_stub, "express", 30.0))
        self.assertIs(result, primary)

    def test_no_fallback_channel_returns_asis(self):
        app_state.update_settings({"cross_channel_fallback": False})
        primary = JSONResponse(status_code=500, content={"error": {"message": "oops"}})
        result = asyncio.run(chat_api._apply_json_fallback(
            primary, self.request_obj, self.req_stub, "cookie", 30.0))
        self.assertIs(result, primary)


class BadCredentialWarningTests(unittest.TestCase):
    """_warn_bad_credential：命中告警一次、去重、未命中不打扰。"""

    def test_warning_printed_once_per_credential(self):
        from api_helpers import _warn_bad_credential
        warned = set()
        err = _api_error(401, "API_KEY_INVALID")
        with mock.patch("builtins.print") as fake_print:
            self.assertTrue(_warn_bad_credential(lambda: "第 1 个 Key（AIza…abcd | 项目=p1）", err, warned))
            self.assertTrue(_warn_bad_credential(lambda: "第 1 个 Key（AIza…abcd | 项目=p1）", err, warned))
            calls = [c for c in fake_print.call_args_list if "密钥失效提醒" in str(c)]
        self.assertEqual(len(calls), 1)  # 同一把 Key 只告警一次

    def test_non_credential_error_returns_false(self):
        from api_helpers import _warn_bad_credential
        warned = set()
        self.assertFalse(_warn_bad_credential(lambda: "label", _api_error(429), warned))
        self.assertFalse(_warn_bad_credential(lambda: "label", ValueError("x"), warned))

    def test_none_describer_still_classifies(self):
        from api_helpers import _warn_bad_credential
        warned = set()
        self.assertTrue(_warn_bad_credential(None, _api_error(403), warned))


class ConfigIntegrationTests(unittest.TestCase):
    """cross_channel_fallback 默认关闭，保留显式启用能力。"""

    def test_default_settings_include_cross_channel_fallback(self):
        self.assertIn("cross_channel_fallback", config.DEFAULT_SETTINGS)
        self.assertIs(config.DEFAULT_SETTINGS["cross_channel_fallback"], False)

    def test_setting_validates_as_boolean(self):
        from settings_validation import validate_settings_patch
        self.assertIsNone(validate_settings_patch({"cross_channel_fallback": False}))
        self.assertIsNone(validate_settings_patch({"cross_channel_fallback": True}))
        self.assertIsNotNone(validate_settings_patch({"cross_channel_fallback": "yes"}))


if __name__ == "__main__":
    unittest.main()
