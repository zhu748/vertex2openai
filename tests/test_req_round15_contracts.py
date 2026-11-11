"""第十五轮（REQ 域正确性收尾）契约测试。

覆盖审查报告核心请求链路域的剩余修复项：
- REQ-P2-1：Express content_filter 空响应在延迟结束帧前冲刷 pending_metadata
  （Gemini 3.x 多轮工具对话强校验思考签名，丢签名导致下一轮 400）。
- REQ-P2-2：大负载 JSON 响应离环序列化（字节与直路一致）。
- REQ-P2-7：主通道 error 帧后迟到的 content 帧不再锁死兜底（saw_content 互斥）。
- REQ-P3-3：count_tokens 的图片按 len/4 口径（不再是 len/16）且 tool_result
  内层图片同样计数。
- REQ-P3-4：CancelledError 归因区分 ASGI 断连与服务端取消。
- REQ-P3-5：轮询参数类型错误统一 400 invalid_resume_cursor（非 FastAPI 422）。
- REQ-P3-6：Cookie 流式错误终点前冲刷已暂存的安全分块。
- REQ-P3-7：假流式心跳为注释帧（不入续传缓冲、不占事件序号）。
- REQ-P3-8：生图收集内部的断连错误字典带 code 499（不再呈现为 502）。
- REQ-P3-9：预填充先行帧独立闸门（签名先到不再丢失 prefill）。
- REQ-P3-10：usage 数值字段统一清洗（溢出 / NaN / bool 一律视为缺失）。
"""
import asyncio
import json
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from google.genai import types
from fastapi.responses import JSONResponse, StreamingResponse

import config
from claude_protocol import (
    _anthropic_usage,
    _count_value,
    estimate_request_tokens,
)
from json_encoding import (
    LARGE_RESPONSE_CHARS,
    estimate_json_chars,
    json_response_offloop,
)
from models import OpenAIRequest
from runtime_state import app_state
from upstreams import cookie_delivery, cookie_proxy, express_sdk

MODEL = "gemini-3.6-flash"


def request(**kwargs):
    return OpenAIRequest(model=MODEL, messages=[{"role": "user", "content": "hello"}], **kwargs)


def response(*parts, finish="STOP", usage=False):
    return types.GenerateContentResponse(
        candidates=[types.Candidate(content=types.Content(role="model", parts=list(parts)), finish_reason=finish)],
        usage_metadata=(types.GenerateContentResponseUsageMetadata(
            prompt_token_count=10, candidates_token_count=5, total_token_count=15) if usage else None))


def chunks(body):
    return [json.loads(line[6:]) for line in body.splitlines()
            if line.startswith("data: ") and line != "data: [DONE]"]


def content(body):
    return "".join(c.get("delta", {}).get("content") or ""
                   for chunk in chunks(body) for c in chunk.get("choices", []))


SAFETY_RATINGS = [{"category": "HARM_CATEGORY_HARASSMENT",
                   "probability": "LOW", "probabilityScore": 0.2}]


class Round15Harness(unittest.IsolatedAsyncioTestCase):
    """Express / Cookie 双通道驱动的公共装配（仿 test_anti_truncation）。"""

    async def asyncSetUp(self):
        self.settings = dict(config.DEFAULT_SETTINGS, retry_max=0,
                             retry_backoff_seconds=0, fake_streaming_interval=0,
                             express_location="", proxy_url="", resumable_enabled=False,
                             safety_score=False, inject_prefill="")
        for patch in (
            mock.patch.object(app_state, "get_settings", side_effect=lambda: dict(self.settings)),
            mock.patch.object(app_state, "get_effective_settings",
                              side_effect=lambda model, base=None: (dict(self.settings) if base is None else dict(base))),
            mock.patch.object(app_state, "get_setting",
                              side_effect=lambda key, default=None: self.settings.get(key, default)),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.key_manager = SimpleNamespace(get_total_keys=lambda: 1,
            get_express_record=lambda **kwargs: {"key": "test-key", "project_id": ""})
        self.http_request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(express_key_manager=self.key_manager)),
            is_disconnected=mock.AsyncMock(return_value=False))

    async def express(self, req, generate=None, stream=None):
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
            generate_content=generate, generate_content_stream=stream)))
        patch = mock.patch.object(express_sdk, "get_pooled_express_client", return_value=client)
        patch.start()
        self.addCleanup(patch.stop)
        return await express_sdk.ExpressSDKUpstream().chat_completions(req, self.http_request)

    async def body(self, result):
        return "".join([part async for part in result.body_iterator])

    async def cookie(self, req, events, *, settings=None):
        """以假 stream_events 驱动 Cookie 通道（不触网）。"""
        if settings:
            self.settings.update(settings)
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def open_client(*args, **kwargs):
            yield SimpleNamespace()

        with mock.patch.object(cookie_proxy, "_get_cookie_string", return_value="SAPISID=test"), \
             mock.patch.object(cookie_proxy, "_get_project_id", return_value="test-project"), \
             mock.patch.object(cookie_delivery, "open_upstream_client", open_client), \
             mock.patch.object(cookie_delivery, "stream_events", events):
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(req, self.http_request)
            if hasattr(result, "body_iterator"):
                return result, await self.body(result)
            return result, result.body.decode()


class ExpressContentFilterFlushTests(Round15Harness):
    """REQ-P2-1：content_filter 空响应不得丢弃首回复前暂存的签名帧。"""

    async def test_signature_frames_flush_before_deferred_content_filter_finish(self):
        sig = "c2lnLWFhYQ=="  # bytes: sig-aaa

        async def stream(**kwargs):
            async def iterate():
                # 签名单独到达（无正文）：first_reply 未就绪 → 暂存 pending_metadata
                yield response(types.Part(thought_signature=sig), finish=None)
                # content_filter 结束帧且尚无任何输出 → 延迟到收尾判定
                yield response(finish="SAFETY")
            return iterate()

        result = await self.express(request(stream=True), stream=stream)
        body = await self.body(result)
        # 签名帧必须下发，且先于 content_filter 结束帧（顺序与首回复分支同口径）
        self.assertIn(sig, body)
        self.assertIn("content_filter", body)
        self.assertLess(body.index(sig), body.index("content_filter"))
        self.assertTrue(body.endswith("data: [DONE]\n\n"))

    async def test_normal_stream_unaffected_by_finalize_drain(self):
        async def stream(**kwargs):
            async def iterate():
                yield response(types.Part(text="hello "), finish=None)
                yield response(types.Part(text="world"), finish="STOP", usage=True)
            return iterate()

        result = await self.express(request(stream=True, stream_options={"include_usage": True}),
                                    stream=stream)
        body = await self.body(result)
        self.assertEqual(content(body), "hello world")
        self.assertTrue(body.endswith("data: [DONE]\n\n"))


class OffloopJSONResponseTests(unittest.IsolatedAsyncioTestCase):
    """REQ-P2-2：大负载 JSON 响应移出事件循环序列化，字节与直路一致。"""

    async def test_large_payload_rendered_offloop_matches_direct_path(self):
        big = {"choices": [{"message": {"content": "x" * (3 * 1024 * 1024)}}]}
        self.assertGreater(estimate_json_chars(big), LARGE_RESPONSE_CHARS)
        direct = JSONResponse(content=big)
        offloop = await json_response_offloop(big)
        self.assertEqual(offloop.body, direct.body)
        self.assertEqual(offloop.status_code, 200)
        self.assertEqual(offloop.media_type, direct.media_type)

    async def test_small_payload_stays_on_direct_path(self):
        small = {"a": "x" * 100, "b": [1, 2, {"c": "y"}]}
        self.assertLessEqual(estimate_json_chars(small), LARGE_RESPONSE_CHARS)
        direct = JSONResponse(content=small)
        result = await json_response_offloop(small)
        self.assertEqual(result.body, direct.body)

    def test_estimate_json_chars_is_structural_and_monotonic(self):
        base = estimate_json_chars({"k": "v"})
        self.assertGreater(estimate_json_chars({"k": "v" * 100}), base)
        self.assertGreater(estimate_json_chars(["a", "b"]), estimate_json_chars(["a"]))
        self.assertEqual(estimate_json_chars(5), 8)
        self.assertEqual(estimate_json_chars(None), 8)


class FallbackLateContentTests(Round15Harness):
    """REQ-P2-7：error 帧后迟到的 content 帧不再把流锁死在「无兜底无终止」。"""

    async def _run(self, primary_frames, fallback_frames):
        import channel_dispatch

        async def primary():
            for frame in primary_frames:
                yield frame

        async def alt():
            for frame in fallback_frames:
                yield frame

        async def fake_fallback(*args, **kwargs):
            return StreamingResponse(alt(), media_type="text/event-stream")

        primary_resp = StreamingResponse(primary(), media_type="text/event-stream")
        with mock.patch.object(channel_dispatch, "_try_channel_fallback", fake_fallback):
            collected = []
            async for frame in channel_dispatch._stream_with_fallback_frames(
                primary_resp, request(stream=True), self.http_request,
                "cookie", "express", time.monotonic() + 10):
                collected.append(frame)
        return "".join(collected)

    async def test_late_content_after_error_still_switches_to_fallback(self):
        out = await self._run(
            primary_frames=[
                'data: {"error":{"code":429,"message":"busy"}}\n\n',
                'data: {"choices":[{"delta":{"content":"late"}}]}\n\n',  # 迟到内容：属于已判死的尝试
                'data: [DONE]\n\n',
            ],
            fallback_frames=[
                'data: {"choices":[{"delta":{"content":"cookie answer"}}]}\n\n',
                'data: [DONE]\n\n',
            ],
        )
        # 错误帧与迟到内容都被吞，客户端只看到备用通道接力 + 正常终止
        self.assertNotIn("busy", out)
        self.assertNotIn("late", out)
        self.assertEqual(content(out), "cookie answer")
        self.assertTrue(out.endswith("data: [DONE]\n\n"))

    async def test_content_before_error_disables_fallback(self):
        out = await self._run(
            primary_frames=[
                'data: {"choices":[{"delta":{"content":"real"}}]}\n\n',
                'data: {"error":{"code":429,"message":"busy"}}\n\n',
                'data: [DONE]\n\n',
            ],
            fallback_frames=[
                'data: {"choices":[{"delta":{"content":"SHOULD NOT APPEAR"}}]}\n\n',
            ],
        )
        # 已输出过内容：错误帧原样转发，不切备用通道
        self.assertIn("real", out)
        self.assertIn("busy", out)
        self.assertNotIn("SHOULD NOT APPEAR", out)


class CountTokensImageAccountingTests(unittest.TestCase):
    """REQ-P3-3：图片 token 按 len/4 口径，tool_result 内层图片同样计数。"""

    def test_image_tokens_follow_documented_len_over_4(self):
        body = {"messages": [{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "data": "x" * 4000}}]}]}
        # 旧实现双重除法实际为 len/16（250）；修复后与 docstring 承诺一致（1000）
        self.assertEqual(estimate_request_tokens(body), 1000)

    def test_tool_result_inner_images_are_counted(self):
        body = {"messages": [{"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t", "content": [
                {"type": "text", "text": ""},
                {"type": "image", "source": {"type": "base64", "data": "y" * 400}},
            ]}]}]}
        # 此前 tool_result 内层图片完全不计数；现在 len/4 = 100
        self.assertEqual(estimate_request_tokens(body), 100)


class CancelAttributionTests(unittest.IsolatedAsyncioTestCase):
    """REQ-P3-4：CancelledError 归因区分 ASGI 断连与服务端取消。"""

    async def test_attribution_distinguishes_disconnect_from_server_cancel(self):
        from upstreams.express_pipeline import _cancel_attribution
        gone = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=True))
        alive = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))
        self.assertEqual(await _cancel_attribution(gone), "客户端已断开")
        server_side = await _cancel_attribution(alive)
        self.assertIn("服务端取消", server_side)
        self.assertNotIn("客户端已断开", server_side.replace("非客户端断开", ""))


class PollParamShapeTests(Round15Harness):
    """REQ-P3-5：轮询参数类型错误统一走 400 invalid_resume_cursor。"""

    async def _poll(self, **params):
        from routes.chat_api import resumable_poll
        return await resumable_poll("any-id", self.http_request, api_key="k", **params)

    async def test_non_integer_from_is_openai_400_not_fastapi_422(self):
        resp = await self._poll(from_="abc", limit="200", wait="0")
        self.assertEqual(resp.status_code, 400)
        payload = json.loads(resp.body)
        self.assertEqual(payload["error"]["type"], "invalid_resume_cursor")

    async def test_non_numeric_wait_is_rejected(self):
        for bad in ("soon", "nan", "inf"):
            with self.subTest(wait=bad):
                resp = await self._poll(from_="0", limit="200", wait=bad)
                self.assertEqual(resp.status_code, 400)
                self.assertEqual(json.loads(resp.body)["error"]["type"], "invalid_resume_cursor")

    async def test_negative_from_still_rejected(self):
        resp = await self._poll(from_="-1", limit="200", wait="0")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(json.loads(resp.body)["error"]["type"], "invalid_resume_cursor")


class CookieSafetyFlushTests(Round15Harness):
    """REQ-P3-6：错误终点前冲刷已暂存的安全分块。"""

    async def test_fatal_error_flushes_pending_safety_html(self):
        async def events(client, headers, body, **kwargs):
            yield "text", "partial answer"
            yield "safety", SAFETY_RATINGS
            yield "fatal_error", "upstream exploded"

        result, body = await self.cookie(request(stream=True), events,
                                         settings={"safety_score": True})
        # 安全分块必须先于错误帧出现（旧实现静默丢弃）；
        # 它本身是内容帧，与正常收尾同口径计入 content。
        self.assertIn("--- Safety Ratings ---", body)
        self.assertIn("upstream exploded", body)
        self.assertLess(body.index("--- Safety Ratings ---"), body.index("upstream exploded"))
        self.assertTrue(content(body).startswith("partial answer"))
        self.assertIn("Safety Ratings", content(body))

    async def test_transport_error_also_flushes_safety(self):
        async def events(client, headers, body, **kwargs):
            yield "safety", SAFETY_RATINGS
            yield "transport_error", "connection reset"

        result, body = await self.cookie(request(stream=True), events,
                                         settings={"safety_score": True})
        self.assertIn("--- Safety Ratings ---", body)
        self.assertIn("connection reset", body)
        self.assertLess(body.index("--- Safety Ratings ---"), body.index("connection reset"))


class FakeStreamHeartbeatTests(Round15Harness):
    """REQ-P3-7：假流式心跳为注释帧，不再构造 data 帧污染续传缓冲。"""

    async def test_heartbeat_during_wait_is_comment_frame(self):
        self.settings["fake_streaming_interval"] = 0.01
        self.settings["anti_truncation_mode"] = "both"

        async def generate(**kwargs):
            await asyncio.sleep(0.08)
            return response(types.Part(text="done answer"))

        req = request(stream=True).model_copy(update={"model": MODEL + "-antitruncation-nonstream"})
        result = await self.express(req, generate=generate)
        body = await self.body(result)
        # 等待期间的心跳全部是注释帧（SSE 规范忽略，不进续传缓冲）
        frames = [f for f in body.split("\n\n") if f]
        comments = [f for f in frames if f.startswith(":")]
        self.assertTrue(comments, "等待期间应有注释帧心跳")
        self.assertTrue(all(f.startswith(":") for f in comments))
        self.assertFalse(any("chatcmpl-keepalive" in f for f in frames))
        # 正文不受影响
        self.assertEqual(content(body), "done answer")


class ImageCollectDisconnectTests(Round15Harness):
    """REQ-P3-8：生图收集的断连错误字典带 code 499。"""

    async def test_disconnect_result_carries_499_code(self):
        fr = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=True))
        result = await cookie_delivery._collect_full_response(
            "test-project", MODEL, request(), "SAPISID=test; ", {},
            retry_max=1, backoff_sec=0, jitter_sec=0, fastapi_request=fr)
        self.assertEqual(result["kind"], "error")
        self.assertEqual(result["code"], 499)  # 旧实现缺 code → 消费方按 502 呈现

    async def test_image_frames_present_disconnect_as_499(self):
        async def collect(*args, **kwargs):
            return {"kind": "error", "code": 499, "message": "客户端已断开连接。"}

        gen = SimpleNamespace(
            fastapi_request=SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False)),
            response_id="rid", model_display=MODEL, base_model_name=MODEL,
            project_id="p", request_obj=request(), record=None, account_id=None,
            retry_max=0, backoff_sec=0, jitter_sec=0, cookie_str="SAPISID=x",
            client_kwargs={}, prefill_active=False, force_builtin_search=False,
            prefill_text="", strip_thoughts=False, want_usage=False,
        )
        with mock.patch.object(cookie_delivery, "_collect_full_response", collect):
            frames = [f async for f in cookie_delivery.deliver_image_frames(gen)]
        body = "".join(frames)
        self.assertIn('"code": 499', body)
        self.assertNotIn('"code": 502', body)
        self.assertTrue(body.endswith("data: [DONE]\n\n"))


class PrefillGateTests(Round15Harness):
    """REQ-P3-9：预填充先行帧独立闸门（签名先到不再丢失 prefill）。"""

    async def test_signature_first_does_not_drop_prefill(self):
        async def events(client, headers, body, **kwargs):
            yield "thought_signature", {"value": "c2lnLWZpcnN0", "part_kind": "signature_only"}
            yield "text", "answer body"
            yield "finish", "STOP"

        result, body = await self.cookie(request(stream=True), events,
                                         settings={"inject_prefill": "PREFIX: "})
        # 注入链路会剥掉预填充尾部空白（“PREFIX: ”→ 7 字），拼接后无空格
        self.assertEqual(content(body), "PREFIX:answer body")

    async def test_plain_stream_prefill_unchanged(self):
        async def events(client, headers, body, **kwargs):
            yield "text", "answer body"
            yield "finish", "STOP"

        result, body = await self.cookie(request(stream=True), events,
                                         settings={"inject_prefill": "PREFIX: "})
        self.assertEqual(content(body), "PREFIX:answer body")

    async def test_prefill_emitted_exactly_once(self):
        async def events(client, headers, body, **kwargs):
            yield "text", "answer body"
            yield "thought_signature", {"value": "c2lnLW1pZA==", "part_kind": "text"}
            yield "text", " more"
            yield "finish", "STOP"

        result, body = await self.cookie(request(stream=True), events,
                                         settings={"inject_prefill": "PREFIX: "})
        self.assertEqual(content(body).count("PREFIX:"), 1)
        self.assertEqual(content(body), "PREFIX:answer body more")


class UsageCountValueTests(unittest.TestCase):
    """REQ-P3-10：usage 数值清洗统一实现，溢出/NaN/bool 一律视为缺失。"""

    def test_count_value_sanitizes_all_pathologies(self):
        self.assertEqual(_count_value(5), 5)
        self.assertEqual(_count_value(3.9), 3)
        self.assertEqual(_count_value(0), 0)
        self.assertEqual(_count_value(-2), 0)
        self.assertIsNone(_count_value(float("inf")))
        self.assertIsNone(_count_value(float("-inf")))
        self.assertIsNone(_count_value(float("nan")))
        self.assertIsNone(_count_value(True))
        self.assertIsNone(_count_value("5"))
        self.assertIsNone(_count_value(None))

    def test_anthropic_usage_overflow_falls_back_to_alias_and_zeroes_detail(self):
        usage = _anthropic_usage({
            "prompt_tokens": float("inf"), "input_tokens": 100,
            "completion_tokens": 7,
            "prompt_tokens_details": {"cached_tokens": float("nan")},
            "completion_tokens_details": {"reasoning_tokens": 4},
        })
        self.assertEqual(usage["input_tokens"], 100)
        self.assertEqual(usage["cache_read_input_tokens"], 0)
        self.assertEqual(usage["output_tokens"], 7)
        self.assertEqual(usage["output_tokens_details"], {"thinking_tokens": 4})

    def test_stream_usage_feed_survives_pathological_values(self):
        from claude_protocol import ClaudeStreamState
        state = ClaudeStreamState(display_model="claude-3-7-sonnet")
        # 别名键回退 + NaN 细节字段跳过（保留上一快照，不归零、不毒化）
        state.feed_usage({"prompt_tokens": 10,
                          "prompt_tokens_details": {"cached_tokens": 4}})
        state.feed_usage({"input_tokens": 42,
                          "prompt_tokens_details": {"cached_tokens": float("nan")}})
        self.assertEqual(state.usage["input_tokens"], 38)  # Anthropic 口径：42 - 4（缓存命中从输入中拆出）
        self.assertEqual(state.usage["cache_read_input_tokens"], 4)


if __name__ == "__main__":
    unittest.main()
