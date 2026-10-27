"""Claude（Anthropic）协议转换纯逻辑回归：模型解析、请求/响应/流式状态机。

全部离线：claude_protocol 无 IO、无路由，直接函数级断言；
口径对齐 vertex-master（Go 版）anthropic_handler 的行为规格。
"""
import asyncio
import contextlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-claude-proto-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import claude_protocol as cp
import model_capabilities as mc
from routes import claude_api, models_api
from claude_protocol import (
    ClaudeRequestError,
    ClaudeStreamState,
    anthropic_error_type,
    anthropic_models_payload,
    anthropic_omit_thinking,
    anthropic_stop_reason,
    anthropic_to_openai_request,
    error_event,
    estimate_request_tokens,
    estimate_text_tokens,
    openai_completion_to_anthropic,
    resolve_claude_model,
    thinking_signature,
)
from upstreams.cookie_request import _build_thinking_config

AVAILABLE = ["gemini-3.1-pro-preview", "gemini-3.8-flash", "gemini-3.1-flash-lite"]


def base_body(**overrides):
    body = {
        "model": "claude-sonnet-4-5",
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": "hi"}],
    }
    body.update(overrides)
    return body


def stream_events(frames):
    """把状态机产出的事件串解析成 (event_name, data_dict) 列表。"""
    parsed = []
    for block in "".join(frames).split("\n\n"):
        if not block.strip():
            continue
        lines = block.splitlines()
        name = None
        payload = None
        for line in lines:
            if line.startswith("event: "):
                name = line[len("event: "):]
            elif line.startswith("data: "):
                payload = json.loads(line[len("data: "):])
        if name is not None:
            parsed.append((name, payload))
    return parsed


def named(events, name):
    """取出指定事件名的全部 payload dict。"""
    return [p for n, p in events if n == name]


def deltas_of(events, delta_type):
    """取出全部 content_block_delta 中指定 delta 类型的 payload。"""
    return [p for p in named(events, "content_block_delta")
            if p["delta"]["type"] == delta_type]


class ModelResolutionTests(unittest.TestCase):
    def test_static_alias_hits_and_date_suffix_stripping(self):
        for raw in ("claude-opus-4-1", "claude-opus-4-1-20250805",
                    "claude-3-5-sonnet-20241022", "claude-haiku-4-5-20251001"):
            display, internal = resolve_claude_model(raw, AVAILABLE)
            self.assertEqual(display, raw)  # 回显客户端请求名
            self.assertIn(internal, AVAILABLE)

    def test_latest_suffix_resolution(self):
        display, internal = resolve_claude_model("claude-3-7-sonnet-latest", AVAILABLE)
        self.assertEqual(display, "claude-3-7-sonnet-latest")
        self.assertEqual(internal, "gemini-3.8-flash")

    def test_unknown_claude_name_falls_back_to_family(self):
        display, internal = resolve_claude_model("claude-opus-9", AVAILABLE)
        self.assertEqual(internal, "gemini-3.1-pro-preview")
        display, internal = resolve_claude_model("claude-haiku-x", AVAILABLE)
        self.assertEqual(internal, "gemini-3.1-flash-lite")
        # 家族未命中且清单为空：静态映射优先于家族兜底
        display, internal = resolve_claude_model("claude-sonnet-4-9", [])
        self.assertEqual(internal, "gemini-3.8-flash")

    def test_family_prefers_newest_chat_model_in_catalogue(self):
        catalogue = ["gemini-2.5-flash", "gemini-3.8-flash", "gemini-3.1-pro-preview",
                     "gemini-3.1-flash-lite-image", "veo-3.0"]
        _, internal = resolve_claude_model("claude-sonnet-4-5", catalogue)
        self.assertEqual(internal, "gemini-3.8-flash")
        # 生图 / 视频 模型不参与映射：无可用 Flash-Lite 时 haiku 兑到最新 Flash
        _, internal = resolve_claude_model("claude-haiku-4-5", catalogue)
        self.assertEqual(internal, "gemini-3.8-flash")
        _, internal = resolve_claude_model("claude-opus-4-1", catalogue)
        self.assertEqual(internal, "gemini-3.1-pro-preview")

    def test_non_claude_names_pass_through(self):
        for raw in ("gemini-3.8-flash", "gemini-3.8-flash-search",
                    "gemini-3.8-flash-antitruncation-stream", "whatever"):
            display, internal = resolve_claude_model(raw, AVAILABLE)
            self.assertEqual((display, internal), (raw, raw))


class RequestConversionTests(unittest.TestCase):
    def test_max_tokens_required_and_validated(self):
        with self.assertRaises(ClaudeRequestError):
            anthropic_to_openai_request({"model": "m", "messages": [
                {"role": "user", "content": "hi"}]}, "m")
        for bad in (0, -1, 1.5, "32", True):
            with self.assertRaises(ClaudeRequestError):
                anthropic_to_openai_request(base_body(max_tokens=bad), "m")
        req = anthropic_to_openai_request(base_body(max_tokens=2048.0), "m")
        self.assertEqual(req.max_tokens, 2048)  # 整数浮点可接受
        # count_tokens 场景不要求 max_tokens
        req = anthropic_to_openai_request(
            {"model": "m", "messages": [{"role": "user", "content": "hi"}]},
            "m", require_max_tokens=False)
        self.assertIsNone(req.max_tokens)

    def test_thinking_budget_maps_to_reasoning_effort(self):
        cases = {1024: "low", 4000: "medium", 8192: "medium", 8193: "high", 60000: "high"}
        for budget, effort in cases.items():
            req = anthropic_to_openai_request(
                base_body(thinking={"type": "enabled", "budget_tokens": budget}), "m")
            self.assertEqual(req.reasoning_effort, effort)
        req = anthropic_to_openai_request(base_body(thinking={"type": "disabled"}), "m")
        self.assertEqual(req.reasoning_effort, "off")
        req = anthropic_to_openai_request(base_body(thinking={"type": "adaptive"}), "m")
        self.assertIsNone(req.reasoning_effort)
        for bad in (1023, "2048", None, 1.5):
            with self.assertRaises(ClaudeRequestError):
                anthropic_to_openai_request(
                    base_body(thinking={"type": "enabled", "budget_tokens": bad}), "m")

    def test_system_string_and_block_array(self):
        req = anthropic_to_openai_request(base_body(system="be helpful"), "m")
        self.assertEqual(req.messages[0].role, "system")
        self.assertEqual(req.messages[0].content, "be helpful")
        req = anthropic_to_openai_request(base_body(system=[
            {"type": "text", "text": "part one"},
            {"type": "text", "text": "part two"},
        ]), "m")
        self.assertEqual(req.messages[0].content, "part one\npart two")

    def test_mid_conversation_system_lowered_to_user(self):
        body = base_body(messages=[
            {"role": "user", "content": "first"},
            {"role": "system", "content": "queued steering"},
            {"role": "user", "content": "second"},
        ])
        req = anthropic_to_openai_request(body, "m")
        roles = [(msg.role, msg.content) for msg in req.messages]
        # 开头 system → system；中途 system 原位降级为 user（保留新近性）
        self.assertEqual(roles, [("user", "first"), ("user", "queued steering"),
                                 ("user", "second")])

    def test_leading_system_message_becomes_system_role(self):
        body = base_body(messages=[
            {"role": "system", "content": "top system"},
            {"role": "user", "content": "hi"},
        ])
        req = anthropic_to_openai_request(body, "m")
        self.assertEqual(req.messages[0].role, "system")
        self.assertEqual(req.messages[1].role, "user")

    def test_assistant_history_thinking_blocks_dropped(self):
        body = base_body(messages=[
            {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "secret", "signature": "sig"},
                {"type": "text", "text": "answer body"},
                {"type": "redacted_thinking", "data": "opaque"},
            ]},
            {"role": "user", "content": "next"},
        ])
        req = anthropic_to_openai_request(body, "m")
        assistant = req.messages[0]
        self.assertEqual(assistant.role, "assistant")
        self.assertEqual(assistant.content, "answer body")
        self.assertEqual(req.messages[1].content, "next")

    def test_assistant_tool_use_history(self):
        body = base_body(messages=[
            {"role": "assistant", "content": [
                {"type": "text", "text": "checking"},
                {"type": "tool_use", "id": "toolu_1", "name": "get_weather",
                 "input": {"city": "Beijing"}},
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "sunny"},
            ]},
        ])
        req = anthropic_to_openai_request(body, "m")
        assistant = req.messages[0]
        self.assertEqual(assistant.content, "checking")
        self.assertEqual(len(assistant.tool_calls), 1)
        call = assistant.tool_calls[0]
        self.assertEqual(call["id"], "toolu_1")
        self.assertEqual(call["function"]["name"], "get_weather")
        self.assertEqual(json.loads(call["function"]["arguments"]), {"city": "Beijing"})
        tool = req.messages[1]
        self.assertEqual(tool.role, "tool")
        self.assertEqual(tool.tool_call_id, "toolu_1")
        self.assertEqual(tool.content, "sunny")

    def test_tool_result_order_and_mixed_content_passthrough(self):
        body = base_body(messages=[
            {"role": "user", "content": [
                {"type": "text", "text": "before"},
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "result"},
                {"type": "text", "text": "after"},
            ]},
        ])
        req = anthropic_to_openai_request(body, "m")
        # tool_result 与相邻普通内容分属不同轮次，顺序保留
        self.assertEqual([m.role for m in req.messages], ["user", "tool", "user"])
        self.assertEqual(req.messages[0].content[0].text, "before")
        self.assertEqual(req.messages[1].content, "result")
        self.assertEqual(req.messages[1].tool_call_id, "toolu_1")
        self.assertEqual(req.messages[2].content[0].text, "after")
        # 复杂 tool_result（含 image）：整体透传为 JSON 值
        body = base_body(messages=[{"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t", "content": [
                {"type": "image", "source": {"type": "base64",
                                             "media_type": "image/png", "data": "abc"}},
            ]},
        ]}])
        req = anthropic_to_openai_request(body, "m")
        self.assertIsInstance(req.messages[0].content, list)

    def test_images_become_data_urls(self):
        body = base_body(messages=[{"role": "user", "content": [
            {"type": "text", "text": "look"},
            {"type": "image", "source": {"type": "base64",
                                         "media_type": "image/jpeg", "data": "QUJD"}},
            {"type": "image", "source": {"type": "url",
                                         "url": "https://example.com/cat.png"}},
        ]}])
        req = anthropic_to_openai_request(body, "m")
        parts = req.messages[0].content
        # pydantic 归一为 ContentPartImage 对象：取 .image_url.url 比较
        self.assertEqual(parts[1].image_url.url, "data:image/jpeg;base64,QUJD")
        self.assertEqual(parts[2].image_url.url, "https://example.com/cat.png")
        self.assertEqual(parts[0].text, "look")

    def test_unsupported_blocks_rejected(self):
        for content in (
            [{"type": "video", "source": {}}],
            [{"type": "tool_result", "content": "x"}],  # 缺 tool_use_id
            [{"type": "image", "source": {"type": "file", "file_id": "f"}}],
            [{"type": "text", "text": 42}],
        ):
            with self.assertRaises(ClaudeRequestError):
                anthropic_to_openai_request(
                    base_body(messages=[{"role": "user", "content": content}]), "m")

    def test_tools_and_tool_choice(self):
        tools = [{"name": "get_weather", "description": "weather",
                  "input_schema": {"type": "object",
                                   "properties": {"city": {"type": "string"}}}},
                 {"name": "no_schema"}]
        req = anthropic_to_openai_request(
            base_body(tools=tools, tool_choice={"type": "any"}), "m")
        self.assertEqual([t["function"]["name"] for t in req.tools],
                         ["get_weather", "no_schema"])
        # 缺 input_schema 补空对象 schema
        self.assertEqual(req.tools[1]["function"]["parameters"],
                         {"type": "object", "properties": {}})
        self.assertEqual(req.tool_choice, "required")
        req = anthropic_to_openai_request(
            base_body(tools=tools, tool_choice={"type": "auto",
                                                "disable_parallel_input": True}), "m")
        self.assertEqual(req.tool_choice, "auto")
        req = anthropic_to_openai_request(
            base_body(tools=tools, tool_choice={"type": "none"}), "m")
        self.assertEqual(req.tool_choice, "none")
        req = anthropic_to_openai_request(
            base_body(tools=tools,
                      tool_choice={"type": "tool", "name": "get_weather"}), "m")
        self.assertEqual(req.tool_choice, {"type": "function",
                                           "function": {"name": "get_weather"}})
        # 指定函数必须已声明；any/required 必须有工具
        with self.assertRaises(ClaudeRequestError):
            anthropic_to_openai_request(
                base_body(tools=tools, tool_choice={"type": "tool", "name": "ghost"}), "m")
        with self.assertRaises(ClaudeRequestError):
            anthropic_to_openai_request(base_body(tool_choice={"type": "any"}), "m")

    def test_generation_params_and_stop_sequences(self):
        body = base_body(temperature=0.7, top_p=0.9, top_k=40,
                         stop_sequences=["END", "\n\n"], stream=True)
        req = anthropic_to_openai_request(body, "m")
        self.assertEqual(req.temperature, 0.7)
        self.assertEqual(req.top_p, 0.9)
        self.assertEqual(req.top_k, 40)
        self.assertEqual(req.stop, ["END", "\n\n"])
        self.assertTrue(req.stream)
        self.assertEqual(req.stream_options, {"include_usage": True})
        with self.assertRaises(ClaudeRequestError):
            anthropic_to_openai_request(base_body(temperature="warm"), "m")

    def test_empty_or_invalid_messages_rejected(self):
        for messages in ([], "not a list", None,
                         [{"role": "bogus", "content": "x"}],
                         [{"role": "user"}]):
            with self.assertRaises(ClaudeRequestError):
                anthropic_to_openai_request(base_body(messages=messages), "m")


class NonStreamResponseTests(unittest.TestCase):
    def completion(self, **message):
        message.setdefault("role", "assistant")
        return {"id": "cmpl", "object": "chat.completion", "model": "g",
                "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 11, "completion_tokens": 7}}

    def test_text_only_response_shape(self):
        out = openai_completion_to_anthropic(
            self.completion(content="hello"), "claude-sonnet-4-5", "msg_1")
        self.assertEqual(out["id"], "msg_1")
        self.assertEqual(out["type"], "message")
        self.assertEqual(out["role"], "assistant")
        self.assertEqual(out["model"], "claude-sonnet-4-5")
        self.assertEqual(out["content"], [{"type": "text", "text": "hello"}])
        self.assertEqual(out["stop_reason"], "end_turn")
        self.assertIsNone(out["stop_sequence"])
        self.assertEqual(out["usage"], {"input_tokens": 11, "output_tokens": 7,
                                        "cache_creation_input_tokens": 0,
                                        "cache_read_input_tokens": 0})

    def test_thinking_block_gets_nonempty_signature(self):
        out = openai_completion_to_anthropic(
            self.completion(content="hi", reasoning_content="because"), "m", "msg_2")
        thinking = out["content"][0]
        self.assertEqual(thinking["type"], "thinking")
        self.assertEqual(thinking["thinking"], "because")
        self.assertTrue(thinking["signature"])
        self.assertEqual(thinking["signature"], thinking_signature("because"))
        self.assertEqual(out["content"][1]["type"], "text")

    def test_tool_use_response_and_stop_reason(self):
        out = openai_completion_to_anthropic(self.completion(
            content="",
            tool_calls=[{"id": "toolu_9", "type": "function",
                         "function": {"name": "get_weather",
                                      "arguments": '{"city": "Beijing"}'}}]), "m", "msg_3")
        # 有工具调用时不补空 text 块
        self.assertEqual([b["type"] for b in out["content"]], ["tool_use"])
        block = out["content"][0]
        self.assertEqual(block["id"], "toolu_9")
        self.assertEqual(block["name"], "get_weather")
        self.assertEqual(block["input"], {"city": "Beijing"})
        self.assertEqual(out["stop_reason"], "tool_use")
        # Invalid upstream JSON must fail instead of inventing a different input.
        with self.assertRaises(cp.IncompleteResponseError):
            openai_completion_to_anthropic(self.completion(content="", tool_calls=[{
                "type": "function", "function": {"name": "f", "arguments": "not-json"}}]), "m", "msg_4")
        out = openai_completion_to_anthropic(self.completion(content="", tool_calls=[{
            "type": "function", "function": {"name": "f", "arguments": "{}"}}]), "m", "msg_5")
        self.assertTrue(out["content"][0]["id"].startswith("toolu_"))

    def test_empty_text_block_when_no_content_and_no_tools(self):
        out = openai_completion_to_anthropic(self.completion(content=None), "m", "msg_5")
        self.assertEqual(out["content"], [{"type": "text", "text": ""}])

    def test_stop_reason_mapping(self):
        self.assertEqual(anthropic_stop_reason("stop", False), "end_turn")
        self.assertEqual(anthropic_stop_reason("length", False), "max_tokens")
        self.assertEqual(anthropic_stop_reason("content_filter", False), "refusal")
        self.assertEqual(anthropic_stop_reason("stop", True), "tool_use")
        self.assertEqual(anthropic_stop_reason("tool_calls", False), "tool_use")
        self.assertEqual(anthropic_stop_reason(None, False), "end_turn")


class StreamStateTests(unittest.TestCase):
    def chunk(self, delta=None, finish=None):
        choice = {"index": 0, "delta": delta or {}, "finish_reason": finish}
        return {"id": "cmpl", "object": "chat.completion.chunk", "model": "g",
                "choices": [choice]}

    def test_message_start_then_text_then_finish(self):
        state = ClaudeStreamState("claude-sonnet-4-5", "msg_fixed")
        frames = [state.message_start_event()]
        frames += state.feed_openai_chunk(self.chunk({"content": "Hel"}))
        frames += state.feed_openai_chunk(self.chunk({"content": "lo"}))
        frames += state.feed_openai_chunk(self.chunk({}, finish="stop"))
        frames += state.finish_events()
        events = stream_events(frames)
        self.assertEqual([e[0] for e in events], [
            "message_start", "content_block_start", "content_block_delta",
            "content_block_delta", "content_block_stop", "message_delta", "message_stop"])
        start = events[0][1]["message"]
        self.assertEqual(start["id"], "msg_fixed")
        self.assertEqual(start["model"], "claude-sonnet-4-5")
        self.assertEqual(start["usage"], {"input_tokens": 0, "output_tokens": 0,
                                          "cache_creation_input_tokens": 0,
                                          "cache_read_input_tokens": 0})
        self.assertEqual(events[1][1]["content_block"], {"type": "text", "text": ""})
        deltas = [d["delta"] for d in named(events, "content_block_delta")]
        self.assertEqual([d["text"] for d in deltas], ["Hel", "lo"])
        final = events[-2][1]
        self.assertEqual(final["delta"]["stop_reason"], "end_turn")
        # usage-only 尾块计入 message_delta
        state2 = ClaudeStreamState("m")
        state2.feed_openai_chunk(self.chunk({"content": "x"}))
        state2.feed_openai_chunk({"choices": [],
                                  "usage": {"prompt_tokens": 5, "completion_tokens": 6}})
        events2 = stream_events(state2.finish_events())
        self.assertEqual(events2[-2][1]["usage"]["input_tokens"], 5)
        self.assertEqual(events2[-2][1]["usage"]["output_tokens"], 6)

    def test_thinking_block_signature_delta_precedes_stop(self):
        state = ClaudeStreamState("m")
        frames = []
        frames += state.feed_openai_chunk(self.chunk({"reasoning_content": "think a"}))
        frames += state.feed_openai_chunk(self.chunk({"reasoning_content": " think b"}))
        frames += state.feed_openai_chunk(self.chunk({"content": "answer"}))
        frames += state.finish_events()
        events = stream_events(frames)
        starts = named(events, "content_block_start")
        self.assertEqual([s["content_block"]["type"] for s in starts],
                         ["thinking", "text"])
        sig_positions = [i for i, (n, p) in enumerate(events)
                         if n == "content_block_delta" and p["delta"]["type"] == "signature_delta"]
        self.assertEqual(len(sig_positions), 1)
        # signature_delta 必须紧邻该 thinking 块的 content_block_stop（SDK 校验）
        self.assertEqual(events[sig_positions[0] + 1][0], "content_block_stop")
        sig = events[sig_positions[0]][1]
        self.assertEqual(sig["delta"]["signature"],
                         thinking_signature("think a think b"))
        stops = [s["index"] for s in named(events, "content_block_stop")]
        self.assertEqual(stops, [0, 1])

    def test_same_frame_content_and_tool_calls_kept_both(self):
        """同帧携带 content + tool_calls（_build_chunk_payload 合帧）不得丢内容。"""
        state = ClaudeStreamState("m")
        frames = []
        frames += state.feed_openai_chunk(self.chunk({
            "content": "Let me check",
            "tool_calls": [{"index": 0, "id": "toolu_1", "type": "function",
                            "function": {"name": "get_weather",
                                         "arguments": '{"city"'}}]}))
        frames += state.feed_openai_chunk(self.chunk({
            "tool_calls": [{"index": 0, "function": {"arguments": ': "Beijing"}'}}]}))
        # 第二帧同时带 finish_reason：也必须被采集
        frames += state.feed_openai_chunk(self.chunk({}, finish="tool_calls"))
        frames += state.finish_events()
        events = stream_events(frames)
        text = "".join(d["delta"].get("text", "")
                       for d in deltas_of(events, "text_delta"))
        self.assertEqual(text, "Let me check")
        jsons = [d["delta"]["partial_json"]
                 for d in deltas_of(events, "input_json_delta")]
        self.assertEqual(len(jsons), 1)
        self.assertEqual(json.loads(jsons[0]), {"city": "Beijing"})
        self.assertEqual(events[-2][1]["delta"]["stop_reason"], "tool_use")

    def test_interleaved_text_and_tool_fragments_stay_protocol_valid(self):
        """文本与工具分片交错：所有 input_json_delta 必须指向已 start 且未 stop 的块。"""
        state = ClaudeStreamState("m")
        frames = []
        frames += state.feed_openai_chunk(self.chunk({"content": "answer"}))
        frames += state.feed_openai_chunk(self.chunk({
            "tool_calls": [{"index": 0, "id": "t1",
                            "function": {"name": "f", "arguments": '{"a"'}}]}))
        frames += state.feed_openai_chunk(self.chunk({"content": " more"}))
        frames += state.feed_openai_chunk(self.chunk({
            "tool_calls": [{"index": 0, "function": {"arguments": ": 1}"}}]}))
        frames += state.finish_events()
        events = stream_events(frames)
        open_indexes = set()
        for name, data in events:
            if name == "content_block_start":
                open_indexes.add(data["index"])
            elif name == "content_block_stop":
                open_indexes.discard(data["index"])
            elif name == "content_block_delta" and data["delta"]["type"] == "input_json_delta":
                self.assertIn(data["index"], open_indexes)
        jsons = [d["delta"]["partial_json"]
                 for d in deltas_of(events, "input_json_delta")]
        self.assertEqual(json.loads(jsons[0]), {"a": 1})
        # 两段文本增量都在
        texts = "".join(d["delta"].get("text", "")
                        for d in deltas_of(events, "text_delta"))
        self.assertEqual(texts, "answer more")
        # 块索引单调：thinking/text 段在前，tool_use 段最后统一发射
        starts = [s["index"] for s in named(events, "content_block_start")]
        self.assertEqual(starts, sorted(starts))

    def test_parallel_tools_and_missing_name_late_arrival(self):
        state = ClaudeStreamState("m")
        frames = []
        # 首帧只有 id，name 后到（抗截断增量参数流的真实形态）
        frames += state.feed_openai_chunk(self.chunk({
            "tool_calls": [{"index": 0, "id": "toolu_a",
                            "function": {"arguments": '{"x"'}}]}))
        frames += state.feed_openai_chunk(self.chunk({
            "tool_calls": [{"index": 0, "function": {"name": "f1", "arguments": ": 1}"}},
                           {"index": 1, "id": "toolu_b",
                            "function": {"name": "f2", "arguments": '{"y": 2}'}}]}))
        frames += state.finish_events()
        events = stream_events(frames)
        blocks = [s["content_block"] for s in named(events, "content_block_start")
                  if s["content_block"]["type"] == "tool_use"]
        self.assertEqual([(b["id"], b["name"]) for b in blocks],
                         [("toolu_a", "f1"), ("toolu_b", "f2")])
        jsons = [d["delta"]["partial_json"]
                 for d in deltas_of(events, "input_json_delta")]
        self.assertEqual([json.loads(j) for j in jsons],
                         [{"x": 1}, {"y": 2}])

    def test_empty_tool_arguments_fall_back_to_object(self):
        state = ClaudeStreamState("m")
        state.feed_openai_chunk(self.chunk({
            "tool_calls": [{"index": 0, "id": "t", "function": {"name": "f"}}]}))
        events = stream_events(state.finish_events())
        jsons = [d["delta"]["partial_json"]
                 for d in deltas_of(events, "input_json_delta")]
        self.assertEqual(jsons, ["{}"])

    def test_ping_and_error_event_frames(self):
        self.assertEqual(cp.PING_EVENT, 'event: ping\ndata: {"type": "ping"}\n\n')
        frame = error_event("overloaded_error", "busy")
        name, payload = stream_events([frame])[0]
        self.assertEqual(name, "error")
        self.assertEqual(payload, {"type": "error",
                                   "error": {"type": "overloaded_error",
                                             "message": "busy"}})

    def test_error_type_mapping(self):
        cases = {400: "invalid_request_error", 401: "authentication_error",
                 403: "permission_error", 404: "not_found_error",
                 429: "rate_limit_error", 503: "overloaded_error",
                 529: "overloaded_error", 418: "api_error"}
        for status, expected in cases.items():
            self.assertEqual(anthropic_error_type(status), expected)


class CountTokensTests(unittest.TestCase):
    def test_estimate_covers_system_messages_tools_and_images(self):
        body = {
            "system": "你是一个助手",
            "messages": [
                {"role": "user", "content": "你好世界"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "t", "name": "f", "input": {"a": 1}}]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t", "content": "done"},
                    {"type": "image", "source": {"type": "base64",
                                                 "media_type": "image/png",
                                                 "data": "x" * 400}}]},
            ],
            "tools": [{"name": "f", "description": "do",
                       "input_schema": {"type": "object"}}],
        }
        total = estimate_request_tokens(body)
        # 中文 5 字 ≈ 5 token，其余内容至少 > 0
        self.assertGreaterEqual(total, 5)
        self.assertEqual(estimate_text_tokens(""), 0)
        self.assertEqual(estimate_text_tokens("ABCD"), 1)  # 4 字符 ≈ 1 token
        self.assertEqual(estimate_text_tokens("你好"), 2)
        # 无效输入不抛异常
        self.assertIsInstance(estimate_request_tokens({}), int)
        self.assertIsInstance(estimate_request_tokens(
            {"messages": [{"content": [{"type": "bogus"}]}]}), int)


class ModelsPayloadTests(unittest.TestCase):
    def test_anthropic_models_payload_lists_aliases_then_catalogue(self):
        payload = anthropic_models_payload(["gemini-3.8-flash", "gemini-3.8-flash"])
        ids = [entry["id"] for entry in payload["data"]]
        self.assertEqual(ids[0], "claude-opus-4-1")  # 别名在前
        self.assertIn("gemini-3.8-flash", ids)       # 内部模型并列展示
        self.assertEqual(ids.count("gemini-3.8-flash"), 1)  # 去重
        for entry in payload["data"]:
            self.assertEqual(entry["type"], "model")
            self.assertIn("created_at", entry)
        self.assertFalse(payload["has_more"])
        self.assertEqual(payload["first_id"], ids[0])
        self.assertEqual(payload["last_id"], ids[-1])


class EffortAndDisplayTests(unittest.TestCase):
    """output_config.effort / thinking.display：对齐 vertex-master 的校验与优先级口径。"""

    def test_thinking_pins_effort_over_output_config(self):
        # 优先级：thinking.enabled/disabled 钉住档位（对齐 Go 版：convertChatRequest
        # 中 thinking 块在 reasoning_effort 之后处理且整写覆盖 thinkingConfig）
        req = anthropic_to_openai_request(base_body(
            thinking={"type": "enabled", "budget_tokens": 2000},
            output_config={"effort": "high"}), "gemini-3.8-flash")
        self.assertEqual(req.reasoning_effort, "medium")  # budget 2000 赢，不是 high
        req = anthropic_to_openai_request(base_body(
            thinking={"type": "disabled"},
            output_config={"effort": "high"}), "m")
        self.assertEqual(req.reasoning_effort, "off")  # disabled 赢
        # adaptive 是合并语义（effort 设的档保留，只补 includeThoughts）：effort 仍生效
        req = anthropic_to_openai_request(base_body(
            thinking={"type": "adaptive"},
            output_config={"effort": "low"}), "m")
        self.assertEqual(req.reasoning_effort, "low")

    def test_output_config_effort_validation_regardless_of_thinking(self):
        # 校验始终执行（vertex-master 在 anthropicToChatRequest 先行校验）：
        # effort 非法即便 thinking 已钉住档位也 400
        for bad in ("banana", "", "  ", 3):
            with self.assertRaises(ClaudeRequestError):
                anthropic_to_openai_request(base_body(
                    thinking={"type": "enabled", "budget_tokens": 2000},
                    output_config={"effort": bad}), "m")
        with self.assertRaises(ClaudeRequestError):  # output_config 非对象
            anthropic_to_openai_request(base_body(
                thinking={"type": "disabled"}, output_config="high"), "m")

    def test_output_config_effort_normalization(self):
        cases = {"LOW": "low", "Medium": "medium", "high": "high",
                 "xhigh": "high", "max": "high"}
        for raw, expected in cases.items():
            req = anthropic_to_openai_request(base_body(output_config={"effort": raw}), "m")
            self.assertEqual(req.reasoning_effort, expected, raw)
        # auto / default → 交给控制台/模型默认
        for raw in ("auto", "default", "AUTO"):
            req = anthropic_to_openai_request(base_body(output_config={"effort": raw}), "m")
            self.assertIsNone(req.reasoning_effort, raw)

    def test_output_config_effort_invalid_rejected(self):
        for bad in ("banana", "", "  "):
            with self.assertRaises(ClaudeRequestError):
                anthropic_to_openai_request(base_body(output_config={"effort": bad}), "m")
        with self.assertRaises(ClaudeRequestError):  # 非字符串
            anthropic_to_openai_request(base_body(output_config={"effort": 3}), "m")
        with self.assertRaises(ClaudeRequestError):  # 非对象
            anthropic_to_openai_request(base_body(output_config="high"), "m")

    def test_thinking_display_validation(self):
        # summarized / omitted 放行（大小写不敏感），其余 400
        for display in ("summarized", "omitted", "Summarized", " OMITTED "):
            anthropic_to_openai_request(base_body(
                thinking={"type": "enabled", "budget_tokens": 2048, "display": display}), "m")
        for display in ("raw", "", "full"):
            with self.assertRaises(ClaudeRequestError):
                anthropic_to_openai_request(base_body(
                    thinking={"type": "enabled", "budget_tokens": 2048, "display": display}), "m")
        with self.assertRaises(ClaudeRequestError):  # 非字符串
            anthropic_to_openai_request(base_body(
                thinking={"type": "enabled", "budget_tokens": 2048, "display": 1}), "m")
        # display 缺省：合法
        anthropic_to_openai_request(base_body(
            thinking={"type": "enabled", "budget_tokens": 2048}), "m")


class OmitThinkingTests(unittest.TestCase):
    """thinking.display=omitted：响应侧不回传思考块（对齐 vertex-master
    includeThoughts = display != "omitted"；模型照常思考，用量照计）。"""

    def test_anthropic_omit_thinking_parsing(self):
        self.assertTrue(anthropic_omit_thinking(base_body(
            thinking={"type": "enabled", "budget_tokens": 2048, "display": "omitted"})))
        self.assertTrue(anthropic_omit_thinking(base_body(
            thinking={"type": "enabled", "budget_tokens": 2048, "display": " OMITTED "})))
        # summarized / 缺省 / 非法形状：不压制
        self.assertFalse(anthropic_omit_thinking(base_body(
            thinking={"type": "enabled", "budget_tokens": 2048, "display": "summarized"})))
        self.assertFalse(anthropic_omit_thinking(base_body(
            thinking={"type": "enabled", "budget_tokens": 2048})))
        self.assertFalse(anthropic_omit_thinking(base_body()))
        self.assertFalse(anthropic_omit_thinking(base_body(thinking="enabled")))
        self.assertFalse(anthropic_omit_thinking(base_body(
            thinking={"type": "enabled", "budget_tokens": 2048, "display": 1})))

    def test_nonstream_omitted_drops_thinking_block_keeps_usage(self):
        payload = {
            "choices": [{"message": {"reasoning_content": "because", "content": "hi"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5,
                      "completion_tokens_details": {"reasoning_tokens": 3}},
        }
        out = openai_completion_to_anthropic(payload, "m", "msg_1", omit_thinking=True)
        self.assertEqual([b["type"] for b in out["content"]], ["text"])
        # 模型仍思考：reasoning_tokens 照计入 usage 细分
        self.assertEqual(out["usage"]["output_tokens_details"], {"thinking_tokens": 3})
        # 对照：默认回传 thinking 块
        out2 = openai_completion_to_anthropic(payload, "m", "msg_1")
        self.assertEqual(out2["content"][0]["type"], "thinking")
        self.assertEqual(out2["content"][0]["thinking"], "because")

    def test_stream_omitted_drops_thinking_events(self):
        state = ClaudeStreamState("m", omit_thinking=True)
        events: list[str] = []
        events += state.feed_openai_chunk(
            {"choices": [{"delta": {"reasoning_content": "secret thought"}}]})
        events += state.feed_openai_chunk(
            {"choices": [{"delta": {"content": "answer"}, "finish_reason": "stop"}]})
        events += state.finish_events()
        parsed = stream_events(events)
        # 不开 thinking 块、不发 thinking_delta / signature_delta
        self.assertEqual([p["content_block"]["type"]
                          for p in named(parsed, "content_block_start")], ["text"])
        self.assertFalse(deltas_of(parsed, "thinking_delta"))
        self.assertFalse(deltas_of(parsed, "signature_delta"))
        # 文本块从索引 0 开始（思考块被丢弃后不占用索引）
        text_start = named(parsed, "content_block_start")[0]
        self.assertEqual(text_start["index"], 0)
        self.assertEqual(deltas_of(parsed, "text_delta")[0]["delta"]["text"], "answer")


class EffortChainTests(unittest.TestCase):
    """全链路档位验证：Anthropic thinking/output_config → reasoning_effort
    → resolve_thinking → 各模型合法 thinking_level（折叠对齐 vertex-master
    thinking.go supportedThinkingLevel：缺 MEDIUM 的两档模型 MEDIUM→HIGH，
    NONE/MINIMAL/LOW 向下压到最低合法档）。"""

    def _level(self, body, model):
        req = anthropic_to_openai_request(base_body(**body), model)
        resolved = mc.resolve_thinking(model, req, {})
        self.assertEqual(resolved.get("mode"), "level")
        return resolved["level"]

    def test_budget_to_level_chain(self):
        cases = [
            # enabled：budget 阈值对齐 vertex-master thinkingLevelForBudget
            ({"thinking": {"type": "enabled", "budget_tokens": 1024}},
             "gemini-3.8-flash", "low"),
            ({"thinking": {"type": "enabled", "budget_tokens": 2000}},
             "gemini-3.8-flash", "medium"),
            ({"thinking": {"type": "enabled", "budget_tokens": 8192}},
             "gemini-3.8-flash", "medium"),
            ({"thinking": {"type": "enabled", "budget_tokens": 10000}},
             "gemini-3.8-flash", "high"),
            # MEDIUM 向上折叠（对齐 vertex-master supportedThinkingLevel）：
            # 3-pro 官方仅 LOW/HIGH 两档，budget 2000(→medium) 折到 high；
            # 3.1-pro 有 MEDIUM → 原样；3.1-flash-image 仅 MINIMAL/HIGH → high
            ({"thinking": {"type": "enabled", "budget_tokens": 2000}},
             "gemini-3-pro", "high"),
            ({"thinking": {"type": "enabled", "budget_tokens": 2000}},
             "gemini-3-pro-preview", "high"),
            ({"thinking": {"type": "enabled", "budget_tokens": 2000}},
             "gemini-3.1-pro", "medium"),
            ({"thinking": {"type": "enabled", "budget_tokens": 2000}},
             "gemini-3.1-flash-image", "high"),
            # low 档在 MINIMAL/HIGH 两档模型上向下折叠到 minimal
            ({"thinking": {"type": "enabled", "budget_tokens": 1024}},
             "gemini-3.1-flash-image", "minimal"),
            # disabled：off→minimal，再按模型就近向下夹取
            # （3.7+ Flash 无 minimal → low；3.6- 有 minimal；Pro 最低 low）
            ({"thinking": {"type": "disabled"}}, "gemini-3.8-flash", "low"),
            ({"thinking": {"type": "disabled"}}, "gemini-3.6-flash", "minimal"),
            ({"thinking": {"type": "disabled"}}, "gemini-3-pro", "low"),
            # adaptive：模型默认档（3.7+ Flash 默认 medium，Pro 默认 high）
            ({"thinking": {"type": "adaptive"}}, "gemini-3.8-flash", "medium"),
            ({"thinking": {"type": "adaptive"}}, "gemini-3-pro", "high"),
            # output_config.effort（thinking 缺失时生效）
            ({"output_config": {"effort": "low"}}, "gemini-3.8-flash", "low"),
            ({"output_config": {"effort": "xhigh"}}, "gemini-3.8-flash", "high"),
            ({"output_config": {"effort": "auto"}}, "gemini-3.8-flash", "medium"),
            # thinking 钉住：effort 不覆盖（budget 1024→low 赢过 high）
            ({"thinking": {"type": "enabled", "budget_tokens": 1024},
              "output_config": {"effort": "high"}}, "gemini-3.8-flash", "low"),
        ]
        for body, model, expected in cases:
            self.assertEqual(self._level(body, model), expected, f"{body} @ {model}")

    def test_claude_code_efforts_map_to_highest_tier(self):
        """Claude Code 思考强度 high/xhigh/max → 模型最高合法档 high；
        medium/low 在模型支持时原样、不支持时按官方档位表折叠。"""
        for eff in ("high", "xhigh", "max"):
            for model in ("gemini-3-pro", "gemini-3.8-flash", "gemini-3.6-flash",
                          "gemini-3.1-pro", "gemini-3.1-flash-image"):
                self.assertEqual(
                    self._level({"output_config": {"effort": eff}}, model), "high",
                    f"effort={eff} @ {model}")
        # medium：支持的模型原样保留
        self.assertEqual(self._level({"output_config": {"effort": "medium"}},
                                     "gemini-3.8-flash"), "medium")
        self.assertEqual(self._level({"output_config": {"effort": "medium"}},
                                     "gemini-3.1-pro"), "medium")
        # medium：两档模型向上折叠到 high
        self.assertEqual(self._level({"output_config": {"effort": "medium"}},
                                     "gemini-3-pro"), "high")
        self.assertEqual(self._level({"output_config": {"effort": "medium"}},
                                     "gemini-3.1-flash-image"), "high")
        # low：支持的模型原样保留
        self.assertEqual(self._level({"output_config": {"effort": "low"}},
                                     "gemini-3-pro"), "low")

    def test_level_sets_match_official_docs(self):
        """per-model 官方档位表对齐 vertex-master thinkingKindForModel。"""
        sets = {
            "gemini-3-pro": {"low", "high"},
            "gemini-3-pro-preview": {"low", "high"},
            "gemini-3.1-pro": {"low", "medium", "high"},
            "gemini-3.8-flash": {"low", "medium", "high"},
            "gemini-4-argon": {"low", "medium", "high"},
            "gemini-3.6-flash": {"minimal", "low", "medium", "high"},
            "gemini-3.1-flash-lite": {"minimal", "low", "medium", "high"},
            "gemini-3.1-flash-image": {"minimal", "high"},
            "gemini-3-pro-image": {"high"},
        }
        for model, expected in sets.items():
            self.assertEqual(mc.get_profile(model)["thinking_levels"], expected,
                             f"{model} 档位表")

    def test_collapse_table_matches_vertex_master(self):
        """档位折叠表逐行对齐 vertex-master supportedThinkingLevel。"""
        collapse = mc._collapse_level
        # LevelsLowHigh（gemini-3-pro：LOW/HIGH）
        low_high = {"low", "high"}
        self.assertEqual(collapse("medium", low_high), "high")
        self.assertEqual(collapse("minimal", low_high), "low")
        self.assertEqual(collapse("low", low_high), "low")
        # LevelsNoMinimal（3.1-pro / 3.8-flash：LOW/MEDIUM/HIGH）
        no_minimal = {"low", "medium", "high"}
        self.assertEqual(collapse("minimal", no_minimal), "low")
        self.assertEqual(collapse("medium", no_minimal), "medium")
        # LevelsMinimalHigh（3.1-flash-image：MINIMAL/HIGH）
        minimal_high = {"minimal", "high"}
        self.assertEqual(collapse("low", minimal_high), "minimal")
        self.assertEqual(collapse("medium", minimal_high), "high")
        # LevelsAll（3.6-flash：四档全有）
        levels_all = {"minimal", "low", "medium", "high"}
        self.assertEqual(collapse("medium", levels_all), "medium")
        self.assertEqual(collapse("minimal", levels_all), "minimal")

    def test_wire_format_thinking_config(self):
        # Cookie 通道线格式：档位大写 + includeThoughts
        req = anthropic_to_openai_request(base_body(
            thinking={"type": "enabled", "budget_tokens": 10000}), "gemini-3.8-flash")
        cfg = _build_thinking_config("gemini-3.8-flash", req, settings={})
        self.assertEqual(cfg, {"thinkingLevel": "HIGH", "includeThoughts": True})
        # display=omitted 只影响响应侧，不影响档位线格式
        req = anthropic_to_openai_request(base_body(
            thinking={"type": "enabled", "budget_tokens": 2048, "display": "omitted"}),
            "gemini-3.8-flash")
        cfg = _build_thinking_config("gemini-3.8-flash", req, settings={})
        self.assertEqual(cfg, {"thinkingLevel": "MEDIUM", "includeThoughts": True})


class RouteStreamingTests(unittest.IsolatedAsyncioTestCase):
    """/v1/messages 路由层：空闲 ping 保活、流事件完整性、错误体 request_id。"""

    def _request(self, body):
        namespace = SimpleNamespace(json=mock.AsyncMock(return_value=body),
                                    headers={}, query_params={})
        namespace.app = SimpleNamespace()  # 直调路由不会触碰 app.state
        return namespace

    async def _run(self, body, upstream_response):
        with mock.patch.object(claude_api, "get_express_models",
                               mock.AsyncMock(return_value=list(AVAILABLE))), \
             mock.patch.object(claude_api, "_call_upstream",
                               mock.AsyncMock(return_value=upstream_response)):
            return await claude_api.claude_messages(self._request(body), "test-key")

    async def test_idle_gap_gets_ping_keepalive_and_stream_stays_intact(self):
        import asyncio
        from fastapi.responses import StreamingResponse

        async def slow_source():
            yield b'data: {"choices":[{"delta":{"content":"Hel"}}]}\n\n'
            await asyncio.sleep(0.2)  # 静默窗口：IDLE_PING_SECONDS 被压到 0.03 → 触发多个 ping
            yield b'data: {"choices":[{"delta":{"content":"lo"}}]}\n\n'
            yield b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n'
            yield b'data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2}}\n\n'
            yield b'data: [DONE]\n\n'

        body = base_body(stream=True)
        with mock.patch.object(claude_api, "IDLE_PING_SECONDS", 0.03):
            result = await self._run(body, StreamingResponse(slow_source(), media_type="text/event-stream"))
            # 必须在补丁生效期间消费流（ping 周期在运行时读取模块全局）
            parts = [chunk if isinstance(chunk, str) else chunk.decode()
                     async for chunk in result.body_iterator]
        stream = "".join(parts)
        events = stream_events([stream])

        # ping 保活：静默期至少补发一个（这是 vertex-master 10s ticker 的等价实现）
        self.assertGreaterEqual(len(named(events, "ping")), 1)
        # 协议完整性：start 先行 → 文本块 → delta(usage) → stop
        self.assertEqual(events[0][0], "message_start")
        self.assertEqual(events[-1][0], "message_stop")
        self.assertEqual("".join(d["delta"]["text"] for d in deltas_of(events, "text_delta")), "Hello")
        delta = named(events, "message_delta")[0]
        self.assertEqual(delta["delta"]["stop_reason"], "end_turn")
        self.assertEqual(delta["usage"]["input_tokens"], 5)
        self.assertEqual(delta["usage"]["output_tokens"], 2)

    async def test_error_bodies_carry_request_id(self):
        from fastapi.responses import JSONResponse

        upstream = JSONResponse(status_code=404, content={"error": {"message": "model not found"}})
        result = await self._run(base_body(), upstream)  # 非流式快速失败路径
        self.assertEqual(result.status_code, 404)
        payload = json.loads(result.body)
        self.assertEqual(payload["type"], "error")
        self.assertEqual(payload["error"]["type"], "not_found_error")
        self.assertTrue(payload["request_id"].startswith("req_"), payload["request_id"])

    async def test_count_tokens_error_has_anthropic_shape(self):
        bad = SimpleNamespace(json=mock.AsyncMock(side_effect=ValueError("not json")), headers={})
        result = await claude_api.claude_count_tokens(bad, "test-key")
        self.assertEqual(result.status_code, 400)
        payload = json.loads(result.body)
        self.assertEqual(payload["type"], "error")
        self.assertEqual(payload["error"]["type"], "invalid_request_error")
        self.assertTrue(payload["request_id"].startswith("req_"))


class UsageDetailsTests(unittest.TestCase):
    """上游 usage 细分字段（cached / reasoning）→ Anthropic usage 映射。"""

    def test_cache_read_and_thinking_details_mapped(self):
        usage = cp._anthropic_usage({
            "prompt_tokens": 100, "completion_tokens": 50,
            "prompt_tokens_details": {"cached_tokens": 60},
            "completion_tokens_details": {"reasoning_tokens": 30},
        })
        # Anthropic 的普通输入与缓存读取分开计数，二者之和等于总输入。
        self.assertEqual(usage["input_tokens"], 40)
        self.assertEqual(usage["output_tokens"], 50)
        self.assertEqual(usage["cache_read_input_tokens"], 60)
        self.assertEqual(usage["cache_creation_input_tokens"], 0)
        self.assertEqual(usage["output_tokens_details"], {"thinking_tokens": 30})

    def test_details_absent_when_missing_or_zero(self):
        usage = cp._anthropic_usage({"prompt_tokens": 10, "completion_tokens": 5})
        self.assertEqual(usage["cache_read_input_tokens"], 0)
        self.assertNotIn("output_tokens_details", usage)
        # 零值 details 不产出 output_tokens_details
        usage = cp._anthropic_usage({
            "prompt_tokens": 10, "completion_tokens": 5,
            "completion_tokens_details": {"reasoning_tokens": 0}})
        self.assertNotIn("output_tokens_details", usage)
        # 非法形状不炸
        usage = cp._anthropic_usage({"prompt_tokens_details": "bad",
                                     "completion_tokens_details": [1]})
        self.assertEqual(usage["cache_read_input_tokens"], 0)
        self.assertNotIn("output_tokens_details", usage)


class StreamBufferTests(unittest.TestCase):
    """流式缓冲优化：增量哈希伪签 + 工具参数分片列表累积的正确性。"""

    def test_incremental_thinking_signature_matches_reference(self):
        state = ClaudeStreamState("claude-sonnet-4-5")
        full_thinking = "Let me think about this problem carefully. "
        for piece in [full_thinking[i:i+7] for i in range(0, len(full_thinking), 7)]:
            state.feed_openai_chunk(
                {"choices": [{"delta": {"reasoning_content": piece}}]})
        # UTF-8 边界安全：分片 update 与整段 encode 后哈希值一致
        events = stream_events(state.finish_events())
        sigs = [p["delta"]["signature"] for p in deltas_of(events, "signature_delta")]
        self.assertEqual(sigs, [cp.thinking_signature(full_thinking)])

    def test_multibyte_char_split_across_frames_keeps_signature_stable(self):
        # 多字节字符不会跨 str 分片（Python str 完整码点），但混合分片下
        # 逐帧 encode 与整段 encode 必须字节一致——这是增量哈希正确性的前提
        text = "思考内容测试ABC123"
        state = ClaudeStreamState("m")
        for piece in [text[i:i+3] for i in range(0, len(text), 3)]:
            state.feed_openai_chunk({"choices": [{"delta": {"reasoning_content": piece}}]})
        events = stream_events(state.finish_events())
        sigs = [p["delta"]["signature"] for p in deltas_of(events, "signature_delta")]
        self.assertEqual(sigs, [cp.thinking_signature(text)])

    def test_tool_arguments_accumulated_via_list_join(self):
        state = ClaudeStreamState("m")
        fragments = ['{"co', 'ntent": ', '"写入这段', '很长的文件内容', '"}']
        for i, frag in enumerate(fragments):
            state.feed_openai_chunk({"choices": [{"delta": {"tool_calls": [
                {"index": 0, "id": "t1", "function": {"name": "write", "arguments": frag}}]}}]})
        events = stream_events(state.finish_events())
        json_deltas = deltas_of(events, "input_json_delta")
        self.assertEqual(len(json_deltas), 1)
        self.assertEqual(json_deltas[0]["delta"]["partial_json"], "".join(fragments))
        starts = named(events, "content_block_start")
        self.assertEqual(starts[0]["content_block"]["type"], "tool_use")
        self.assertEqual(starts[0]["content_block"]["id"], "t1")


class BodyTooLargeShapeTests(unittest.TestCase):
    """413 错误体形状：/v1/messages 走 Anthropic 形状，其余保持 OpenAI 形状。"""

    def _handler(self):
        import main  # 触发完整应用装配（含异常处理器注册）
        return main._body_too_large_error_handler

    def test_claude_path_gets_anthropic_shape(self):
        handler = self._handler()
        from request_connection import BodyTooLargeError
        request = SimpleNamespace(url=SimpleNamespace(path="/v1/messages"))
        response = asyncio.run(handler(request, BodyTooLargeError(limit=2048)))
        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 413)
        self.assertEqual(payload["type"], "error")
        self.assertEqual(payload["error"]["type"], "invalid_request_error")
        self.assertTrue(payload["request_id"].startswith("req_"))

    def test_openai_path_keeps_openai_shape(self):
        handler = self._handler()
        from request_connection import BodyTooLargeError
        request = SimpleNamespace(url=SimpleNamespace(path="/v1/chat/completions"))
        response = asyncio.run(handler(request, BodyTooLargeError(limit=2048)))
        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 413)
        self.assertEqual(payload["error"]["type"], "invalid_request_error")
        self.assertNotIn("request_id", payload)


class MethodGuardTests(unittest.TestCase):
    """方法约束（对齐 vertex-master）：非 POST/GET 访问 Claude 端点 → 405 Anthropic 形状。

    FastAPI 路由层对未注册方法的默认 405 是 {"detail":...} 形状，
    Claude 客户端无法解析；这里用最小 FastAPI 应用验证真实路由匹配结果。
    """

    def _client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        app = FastAPI()
        app.include_router(claude_api.router)
        return TestClient(app)

    def test_get_put_delete_on_messages_return_anthropic_405(self):
        client = self._client()
        for method in ("get", "put", "delete", "patch"):
            response = getattr(client, method)("/v1/messages")
            self.assertEqual(response.status_code, 405, method)
            payload = response.json()
            self.assertEqual(payload["type"], "error")
            self.assertEqual(payload["error"]["type"], "invalid_request_error")
            self.assertTrue(payload["request_id"].startswith("req_"))

    def test_count_tokens_non_post_guarded(self):
        client = self._client()
        response = client.get("/v1/messages/count_tokens")
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.json()["error"]["type"], "invalid_request_error")

    def test_post_routes_not_hijacked_by_guards(self):
        """POST 仍路由到真实端点（鉴权依赖先于业务执行 → ClaudeAPIError）。"""
        client = self._client()
        with self.assertRaises(claude_api.ClaudeAPIError):
            client.post("/v1/messages", json={})
        with self.assertRaises(claude_api.ClaudeAPIError):
            client.post("/v1/messages/count_tokens", json={})

    def test_hello_get_ok_and_post_guarded(self):
        client = self._client()
        response = client.get("/api/hello")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        response = client.post("/api/hello")
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.json()["type"], "error")


class ExactCountTokensTests(unittest.IsolatedAsyncioTestCase):
    """count_tokens 两级口径：Express 精确计数优先，失败/无 Key 回退启发式。"""

    def _body(self):
        return base_body(
            system="You are a helpful assistant.",
            tools=[{"name": "get_weather", "description": "Get weather",
                    "input_schema": {"type": "object",
                                     "properties": {"city": {"type": "string"}}}}],
            messages=[{"role": "user", "content": [
                {"type": "text", "text": "check the weather"}]}],
        )

    def _request(self, manager):
        namespace = SimpleNamespace(json=mock.AsyncMock(return_value=self._body()),
                                    headers={}, query_params={})
        if manager is None:
            namespace.app = SimpleNamespace()  # 无 express_key_manager → 估算
        else:
            namespace.app = SimpleNamespace(
                state=SimpleNamespace(express_key_manager=manager))
        return namespace

    async def _run(self, manager, count_call=None, prepare_error=None):
        request = self._request(manager)
        patches = [mock.patch.object(
            claude_api, "get_express_models",
            mock.AsyncMock(return_value=list(AVAILABLE)))]
        if count_call is not None or prepare_error is not None:
            count_call = count_call or mock.AsyncMock(return_value=SimpleNamespace(total_tokens=123))
            sdk_client = SimpleNamespace(aio=SimpleNamespace(
                models=SimpleNamespace(count_tokens=count_call)))
            wrapper = SimpleNamespace(client=sdk_client)
            # side_effect 优先于 return_value：有异常则抛、无异常则返回 wrapper
            wrapper.prepare = mock.AsyncMock(side_effect=prepare_error, return_value=wrapper)
            patches.append(mock.patch.object(
                claude_api, "FailoverSDKClient", mock.MagicMock(return_value=wrapper)))
            patches.append(mock.patch.object(claude_api, "ProxyChain", mock.MagicMock()))
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            return await claude_api.claude_count_tokens(request, "test-key")

    async def test_exact_count_via_express_channel(self):
        count_call = mock.AsyncMock(return_value=SimpleNamespace(total_tokens=123))
        manager = SimpleNamespace(get_express_record=lambda: {
            "key": "express-key", "project_id": "proj-1"})
        result = await self._run(manager, count_call=count_call)
        self.assertEqual(json.loads(result.body), {"input_tokens": 123})
        # 计数口径：claude-sonnet-4-5 → gemini-3.8-flash；模型路径按 Key 的
        # 项目钉定（与真实生成请求同款路径，Key 项目 proj-1 必须体现在路径里）
        kwargs = count_call.call_args.kwargs
        self.assertIn("gemini-3.8-flash", kwargs["model"])
        self.assertIn("projects/proj-1/", kwargs["model"])
        self.assertTrue(kwargs["contents"])
        self.assertEqual(kwargs["config"].system_instruction,
                         "You are a helpful assistant.")
        self.assertTrue(kwargs["config"].tools)

    async def test_no_express_manager_falls_back_to_estimate(self):
        body = self._body()
        request = SimpleNamespace(json=mock.AsyncMock(return_value=body),
                                  headers={}, query_params={})
        request.app = SimpleNamespace()
        with mock.patch.object(claude_api, "get_express_models",
                               mock.AsyncMock(return_value=list(AVAILABLE))):
            result = await claude_api.claude_count_tokens(request, "test-key")
        self.assertEqual(json.loads(result.body),
                         {"input_tokens": estimate_request_tokens(body)})

    async def test_upstream_error_falls_back_to_estimate(self):
        body = self._body()
        manager = SimpleNamespace(get_express_record=lambda: {
            "key": "express-key", "project_id": "proj-1"})
        result = await self._run(
            manager, count_call=mock.AsyncMock(side_effect=RuntimeError("boom")))
        self.assertEqual(json.loads(result.body),
                         {"input_tokens": estimate_request_tokens(body)})

    async def test_proxy_resolve_error_falls_back_to_estimate(self):
        body = self._body()
        manager = SimpleNamespace(get_express_record=lambda: {
            "key": "express-key", "project_id": "proj-1"})
        result = await self._run(manager, prepare_error=claude_api.ProxyResolveError("no bridge"))
        self.assertEqual(json.loads(result.body),
                         {"input_tokens": estimate_request_tokens(body)})

    async def test_empty_record_falls_back_to_estimate(self):
        body = self._body()
        manager = SimpleNamespace(get_express_record=lambda: None)
        result = await self._run(manager)
        self.assertEqual(json.loads(result.body),
                         {"input_tokens": estimate_request_tokens(body)})


class ToolResultCacheControlTests(unittest.TestCase):
    """tool_result 复杂块透传：cache_control 必须剥离（全程剥离承诺）。"""

    def test_complex_blocks_strip_cache_control_without_mutating_input(self):
        content = [
            {"type": "text", "text": "partial",
             "cache_control": {"type": "ephemeral"}},
            {"type": "image",
             "source": {"type": "base64", "media_type": "image/png", "data": "abc"},
             "cache_control": {"type": "ephemeral"}},
        ]
        out = cp._tool_result_payload(content, "label")
        self.assertEqual([b.get("cache_control") for b in out], [None, None])
        self.assertEqual(out[1]["source"]["data"], "abc")  # 其余字段完整保留
        # 入参不被原地修改
        self.assertIn("cache_control", content[0])

    def test_text_only_tool_result_joins_to_string(self):
        content = [
            {"type": "text", "text": "a", "cache_control": {"type": "ephemeral"}},
            {"type": "text", "text": "b"},
        ]
        self.assertEqual(cp._tool_result_payload(content, "label"), "a\nb")

    def test_full_conversion_tool_result_has_no_cache_control(self):
        body = base_body(messages=[{
            "role": "user",
            "content": [{"type": "tool_result", "tool_use_id": "t1", "content": [
                {"type": "image",
                 "source": {"type": "base64", "media_type": "image/png", "data": "xyz"},
                 "cache_control": {"type": "ephemeral"}}]}],
        }])
        request = anthropic_to_openai_request(body, "gemini-3.8-flash")
        tool_message = request.messages[-1]
        self.assertEqual(tool_message.role, "tool")
        self.assertEqual(tool_message.tool_call_id, "t1")
        self.assertNotIn("cache_control", json.dumps(tool_message.content))


class ModelRetrieveTests(unittest.IsolatedAsyncioTestCase):
    """GET /v1/models/{id} 双格式检索：Anthropic retrieve / OpenAI retrieve / 404 分形状。"""

    def _request(self, headers):
        namespace = SimpleNamespace(headers=headers, query_params={})
        namespace.app = SimpleNamespace(state=SimpleNamespace(
            express_key_manager=SimpleNamespace(get_total_keys=lambda: 1)))
        return namespace

    async def test_anthropic_retrieve_by_alias_and_gemini_id(self):
        request = self._request({"anthropic-version": "2023-06-01"})
        with mock.patch.object(claude_api, "get_express_models",
                               mock.AsyncMock(return_value=list(AVAILABLE))):
            entry = await models_api.retrieve_model("claude-sonnet-4-5", request, "test-key")
            self.assertEqual(entry["id"], "claude-sonnet-4-5")
            self.assertEqual(entry["type"], "model")
            self.assertIn("created_at", entry)
            entry = await models_api.retrieve_model("gemini-3.8-flash", request, "test-key")
            self.assertEqual(entry["id"], "gemini-3.8-flash")

    async def test_anthropic_retrieve_unknown_returns_not_found_shape(self):
        request = self._request({"x-api-key": "test-key"})
        with mock.patch.object(claude_api, "get_express_models",
                               mock.AsyncMock(return_value=list(AVAILABLE))), \
             self.assertRaises(claude_api.ClaudeAPIError) as ctx:
            await models_api.retrieve_model("no-such-model", request, "test-key")
        payload = json.loads(ctx.exception.response.body)
        self.assertEqual(ctx.exception.response.status_code, 404)
        self.assertEqual(payload["error"]["type"], "not_found_error")
        self.assertTrue(payload["request_id"].startswith("req_"))

    async def test_openai_retrieve_and_404_detail_shape(self):
        from fastapi import HTTPException
        request = self._request({})
        with mock.patch.object(models_api, "get_express_models",
                               mock.AsyncMock(return_value=list(AVAILABLE))):
            entry = await models_api.retrieve_model("gemini-3.8-flash", request, "test-key")
            self.assertEqual(entry["object"], "model")
            self.assertEqual(entry["owned_by"], "google")
            with self.assertRaises(HTTPException) as ctx:
                await models_api.retrieve_model("no-such-model", request, "test-key")
            self.assertEqual(ctx.exception.status_code, 404)
            self.assertIn("no-such-model", ctx.exception.detail)


if __name__ == "__main__":
    unittest.main()
