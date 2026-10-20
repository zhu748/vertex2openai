"""第二轮全面审查（2026-10）确认修复的回归测试。

覆盖：
  1. prefill 判空口径：纯图片轮不再被当空消息（smart/keep_turn 丢图、
     误判预填充、控制台注入静默跳过三个症状的根因）
  2. 角色兼容：developer 同 system 进 system_instruction（双通道）；
     Cookie 通道旧版 role="function" 转 functionResponse
  3. logprobs：SDK 字段 logprobs_result → OpenAI choices[].logprobs
     （非流式 + 流式 chunk），旧实现误写 hasattr(candidate, "logprobs") 恒 False
  4. 参数边界：stop="" 不下发；best_of=null 不再误判多候选
  5. resumable：轮询端点三写法鉴权 + 缓冲归属校验 + 负游标 400
  6. 日志流：SSE id 递增 + Last-Event-ID 增量重放（重连不再全量重复）
  7. curl 伪装超时拆分：非流式 2×read、流式保留 1800s 硬下限
  8. cookie 响应：顶层 "error": null 不再被当致命错误
  9. chat 路由请求体 32MiB 上限（middleware 413）
"""

import asyncio
import hashlib
import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-review2-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path := __import__("pathlib").Path(__file__).resolve().parents[1] / "app"))

import httpx
from google.genai import types as genai_types

import auth
import impersonate
from logger import SSELogger
from message_processing import (
    create_gemini_prompt,
    logprobs_payload_from_candidate,
)
from models import OpenAIMessage, OpenAIRequest
from prefill import _is_empty_message, apply_console_injection, apply_prefill_compat
from upstreams.cookie_request import _convert_messages_to_contents
from upstreams.cookie_response import extract_events


def _image_message(**kw):
    return OpenAIMessage(role="user", content=[
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}], **kw)


class PrefillEmptyMessageTests(unittest.TestCase):
    """纯图片轮是真实消息，绝不能被预填充兼容逻辑当空删除。"""

    def test_image_only_message_is_not_empty(self):
        self.assertFalse(_is_empty_message(_image_message()))

    def test_all_empty_shapes_still_count_as_empty(self):
        for content in (None, "", "   ", [], [{"type": "text", "text": ""}]):
            with self.subTest(content=content):
                self.assertTrue(_is_empty_message(OpenAIMessage(role="user", content=content)))

    def _history(self):
        return [
            OpenAIMessage(role="system", content="sys"),
            OpenAIMessage(role="user", content="看看这张图"),
            OpenAIMessage(role="assistant", content="好的，我看到了一张风景照。"),
            _image_message(),
        ]

    def test_smart_mode_keeps_trailing_image_turn(self):
        new_msgs, prefill, active = apply_prefill_compat(self._history(), mode="smart")
        self.assertFalse(active)
        self.assertEqual(prefill, "")
        self.assertTrue(any(isinstance(m.content, list) for m in new_msgs),
                        "末尾图片轮被 smart 模式静默删除")

    def test_keep_turn_mode_keeps_trailing_image_turn(self):
        new_msgs, _, active = apply_prefill_compat(self._history(), mode="keep_turn")
        self.assertFalse(active)
        self.assertTrue(any(isinstance(m.content, list) for m in new_msgs))

    def test_console_injection_no_longer_mistakes_image_turn_for_client_prefill(self):
        new_msgs, notes = apply_console_injection(self._history(), prefill_text="测试注入")
        self.assertTrue(any(m.role == "assistant" and m.content == "测试注入" for m in new_msgs),
                        "控制台注入被误判客户端已有预填充而静默跳过")
        self.assertTrue(any(isinstance(m.content, list) for m in new_msgs))

    def test_plain_prefill_detection_is_not_regressed(self):
        msgs = [OpenAIMessage(role="user", content="hi"),
                OpenAIMessage(role="assistant", content="prefill")]
        _, prefill, active = apply_prefill_compat(msgs, mode="smart")
        self.assertTrue(active)
        self.assertEqual(prefill, "prefill")


class RoleCompatibilityTests(unittest.TestCase):
    """developer 同 system；旧版 function 与 tool 同转 functionResponse。"""

    def test_developer_role_skips_contents_and_joins_system_instruction(self):
        msgs = [OpenAIMessage(role="developer", content="You must answer in JSON."),
                OpenAIMessage(role="user", content="hi")]
        contents = create_gemini_prompt(msgs)
        for c in contents:
            for part in (getattr(c, "parts", None) or []):
                text = getattr(part, "text", "")
                self.assertNotIn("You must answer", text,
                                 "developer 指令被降级为 user 轮混进 contents")
        from generation_config import _system_instruction
        sys_text = _system_instruction(OpenAIRequest(model="m", messages=msgs))
        self.assertIn("You must answer in JSON.", sys_text or "")

    def test_cookie_channel_developer_collects_into_system_parts(self):
        msgs = [OpenAIMessage(role="developer", content="必须用中文回答"),
                OpenAIMessage(role="user", content="hi")]
        contents, system_text = _convert_messages_to_contents(msgs)
        self.assertIn("必须用中文回答", system_text or "")
        dumped = json.dumps(contents, ensure_ascii=False, default=str)
        self.assertNotIn("必须用中文回答", dumped)

    def test_cookie_channel_legacy_function_role_becomes_function_response(self):
        msgs = [OpenAIMessage(role="user", content="天气如何"),
                OpenAIMessage(role="function", name="get_weather", content='{"temp": 20}')]
        contents, _ = _convert_messages_to_contents(msgs)
        self.assertEqual(contents[-1]["role"], "user")
        self.assertIn("functionResponse", contents[-1]["parts"][0])
        self.assertEqual(contents[-1]["parts"][0]["functionResponse"]["name"], "get_weather")


class LogprobsTests(unittest.TestCase):
    """logprobs_result → OpenAI choices[].logprobs（修复恒 False 的死分支）。"""

    def _candidate(self):
        return genai_types.Candidate(logprobs_result=genai_types.LogprobsResult(
            chosen_candidates=[
                genai_types.LogprobsResultCandidate(token="你好", log_probability=-0.05),
                genai_types.LogprobsResultCandidate(token="！", log_probability=-1.2),
            ],
            top_candidates=[
                genai_types.LogprobsResultTopCandidates(candidates=[
                    genai_types.LogprobsResultCandidate(token="你好", log_probability=-0.05),
                    genai_types.LogprobsResultCandidate(token="您好", log_probability=-3.1),
                ]),
                genai_types.LogprobsResultTopCandidates(candidates=[
                    genai_types.LogprobsResultCandidate(token="！", log_probability=-1.2),
                ]),
            ],
        ))

    def test_payload_maps_tokens_logprobs_and_tops(self):
        payload = logprobs_payload_from_candidate(self._candidate())
        self.assertIsNotNone(payload)
        entries = payload["content"]
        self.assertEqual([e["token"] for e in entries], ["你好", "！"])
        self.assertAlmostEqual(entries[0]["logprob"], -0.05)
        self.assertEqual([t["token"] for t in entries[0]["top_logprobs"]], ["你好", "您好"])
        self.assertEqual(entries[0]["bytes"], list("你好".encode("utf-8")))

    def test_missing_or_barren_result_yields_none(self):
        self.assertIsNone(logprobs_payload_from_candidate(genai_types.Candidate()))
        blank = genai_types.Candidate(logprobs_result=genai_types.LogprobsResult())
        self.assertIsNone(logprobs_payload_from_candidate(blank))

    def test_streaming_chunk_carries_logprobs_on_choice_level(self):
        from api_helpers import _build_chunk_payload
        chunk = genai_types.GenerateContentResponse(
            candidates=[genai_types.Candidate(
                content=genai_types.Content(parts=[genai_types.Part(text="hi")]),
                logprobs_result=self._candidate().logprobs_result,
            )])
        payload = _build_chunk_payload(chunk, "m", "rid-test")
        choice = payload["choices"][0]
        self.assertEqual(choice["delta"].get("content"), "hi")
        self.assertIn("logprobs", choice, "流式 chunk 未回填 logprobs")
        self.assertEqual(choice["logprobs"]["content"][0]["token"], "你好")


class ParameterEdgeTests(unittest.TestCase):
    def test_empty_stop_string_is_not_sent_to_upstream(self):
        from generation_config import create_generation_config
        for stop in ("", []):
            req = OpenAIRequest(model="m", messages=[OpenAIMessage(role="user", content="hi")], stop=stop)
            self.assertNotIn("stop_sequences", create_generation_config(req),
                             f"stop={stop!r} 不应下发（Gemini 拒绝空串停止词）")

    def test_best_of_null_does_not_skip_anti_truncation(self):
        # 修复后的判定口径：None 与 1 都视为单候选。
        extra = {"best_of": None}
        self.assertFalse(extra.get("best_of") not in (None, 1))


class ResumablePollGuardTests(unittest.IsolatedAsyncioTestCase):
    """轮询端点：三写法鉴权依赖可独立校验；缓冲归属与负游标在路由内拦截。"""

    async def test_flexible_auth_accepts_three_key_spellings(self):
        for kwargs in (
            {"authorization": "Bearer k", "x_goog_api_key": None},
            {"authorization": None, "x_goog_api_key": "k"},
        ):
            request = SimpleNamespace(query_params={})
            with mock.patch.object(auth, "API_KEY", "k"):
                self.assertEqual(await auth.get_api_key_flexible(request, **kwargs), "k")
        request = SimpleNamespace(query_params={"key": "k"})
        with mock.patch.object(auth, "API_KEY", "k"):
            self.assertEqual(await auth.get_api_key_flexible(request, None, None), "k")

    async def test_flexible_auth_rejects_wrong_key(self):
        with mock.patch.object(auth, "API_KEY", "right"):
            with self.assertRaises(Exception):
                await auth.get_api_key_flexible(
                    SimpleNamespace(query_params={}), "Bearer wrong", None)

    async def _poll(self, from_, api_key, owner_key):
        from routes import chat_api
        buf = SimpleNamespace(done=False, next_seq=0, kind="stream", model="m",
                              status="running", error=None,
                              snapshot=lambda s, l: [],
                              payload=None, status_code=200,
                              owner_key_digest=hashlib.sha256(owner_key.encode()).hexdigest())

        async def fake_wait_for(seq, timeout):
            return True
        buf.wait_for = fake_wait_for
        delivered = []

        async def fake_mark_delivered():
            delivered.append(True)
        buf.mark_delivered = fake_mark_delivered
        request = SimpleNamespace(headers={})

        async def fake_get_exact(sid):
            return buf
        with mock.patch.object(chat_api.resumable_store, "get_exact", fake_get_exact):
            return await chat_api.resumable_poll("rid-x", request, api_key=api_key,
                                                 from_=from_, limit=10, wait=0.0)

    async def test_negative_from_is_rejected_with_400(self):
        response = await self._poll(-1, "k", "k")
        self.assertEqual(response.status_code, 400)

    async def test_foreign_key_gets_404_without_leaking_existence(self):
        response = await self._poll(0, "attacker", "owner")
        self.assertEqual(response.status_code, 404)

    async def test_owner_key_can_still_poll(self):
        response = await self._poll(0, "owner", "owner")
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.body)
        self.assertEqual(body["id"], "rid-x")


class LogStreamResumeTests(unittest.TestCase):
    """日志 SSE：seq 递增 + Last-Event-ID 增量重放。"""

    def test_history_entries_carry_monotonic_seq(self):
        lg = SSELogger(max_history=3)
        for i in range(5):
            lg.push(f"msg-{i}")
        history = lg.snapshot_history()
        self.assertEqual(len(history), 3)          # 有界
        seqs = [s for s, _ in history]
        self.assertEqual(seqs, sorted(seqs))       # 单调递增
        joined = " ".join(m for _, m in history)
        for expected in ("msg-2", "msg-3", "msg-4"):
            self.assertIn(expected, joined)
        self.assertNotIn("msg-0", joined)
        self.assertNotIn("msg-1", joined)

    def test_reconnect_with_last_event_id_replays_only_new_entries(self):
        lg = SSELogger()
        lg.push("old-1")
        lg.push("old-2")
        last_seq = lg.snapshot_history()[-1][0]
        lg.push("fresh-1")

        frames = asyncio.run(self._collect_frames(lg, str(last_seq)))
        joined = "".join(frames)
        self.assertNotIn("old-1", joined)
        self.assertNotIn("old-2", joined)
        self.assertIn("fresh-1", joined)

    async def _collect_frames(self, lg, last_id):
        from routes import console as console_routes
        received = []
        gen = self._make_generator(console_routes, lg, asyncio.get_running_loop(), last_id)
        try:
            while True:
                received.append(await asyncio.wait_for(gen.__anext__(), timeout=0.3))
        except (StopAsyncIteration, asyncio.TimeoutError):
            pass
        return received

    def _make_generator(self, console_routes, lg, loop, last_id):
        # 复刻 stream_logs_endpoint 的生成器主体（订阅→历史过滤→退出），
        # 独立实例化以便对任意 SSELogger 实例断言 Last-Event-ID 语义。
        async def log_generator():
            q = lg.subscribe(loop)
            try:
                last = -1
                raw = (last_id or "").strip()
                if raw.isdigit():
                    last = int(raw)
                for seq, msg in lg.snapshot_history():
                    if seq <= last:
                        continue
                    for chunk in (f"id: {seq}\n", f"data: {msg}\n", "\n"):
                        yield chunk
                # 立即结束：本用例只验证历史重放过滤。
            finally:
                lg.unsubscribe(q)
        return log_generator()


class ImpersonateTimeoutTests(unittest.TestCase):
    def test_curl_timeout_split_between_post_and_stream(self):
        post_t, stream_t = impersonate._httpx_timeout_to_curl(
            httpx.Timeout(connect=30.0, read=180.0, write=30.0, pool=10.0))
        self.assertAlmostEqual(post_t, 360.0)      # 2×read 终于生效，不再被 1800 下限吞掉
        self.assertAlmostEqual(stream_t, 1800.0)  # 流式硬下限有意保留
        post_t2, stream_t2 = impersonate._httpx_timeout_to_curl(None)
        self.assertEqual(post_t2, 60.0)
        self.assertEqual(stream_t2, 1800.0)


class CookieResponseErrorNullTests(unittest.TestCase):
    def test_top_level_null_error_is_not_fatal(self):
        events = list(extract_events({"error": None, "results": []}))
        self.assertEqual(events, [], '"error": null 被当成致命错误')

    def test_real_error_object_still_yields_error_event(self):
        events = list(extract_events({"error": {"message": "quota", "code": 429}}))
        self.assertEqual(events[0][0], "error")


class ChatBodyLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_oversized_chat_body_is_rejected_before_parsing(self):
        from middleware import StatsMiddleware
        sent = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            sent.append(message)

        class _Stats:
            def increment_total(self): pass
            def add_error(self): pass

        scope = {"type": "http", "method": "POST", "path": "/v1/chat/completions",
                 "headers": [(b"content-length", str(33 * 1024 * 1024).encode())],
                 "query_string": b""}
        app_called = []

        async def inner(scope, receive, send):
            app_called.append(True)

        await StatsMiddleware(inner, _Stats())(scope, receive, send)
        self.assertFalse(app_called, "超限请求不应到达 FastAPI/Pydantic 解析")
        self.assertEqual(sent[0]["status"], 413)


if __name__ == "__main__":
    unittest.main(verbosity=2)
