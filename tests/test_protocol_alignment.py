"""三协议（OpenAI / Claude / Gemini 原生）模型目录与运行日志对齐回归。

背景：Gemini 格式与 Claude 格式端点是后加的，此前存在三类不对齐：
1. Anthropic 目录不列 -search / 抗截断变体（请求侧却透传支持）；
2. Gemini 原生目录不列 -search 变体，透传也不支持该后缀（上游 404）；
3. 原生透传 / Claude 路由的完成日志缺模型、耗时与 token 用量。

全部离线：纯函数直调 + MockTransport 透传 + 路由直调（capsys 断言日志）。
"""
import asyncio
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from unittest import mock

_STATE_DIR = tempfile.TemporaryDirectory(prefix="v2o-proto-align-")
os.environ["STATE_DIR"] = _STATE_DIR.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

import anti_truncation
import config
import main
from logger import ProxyStats
from routes import claude_api, gemini_native

AVAILABLE = ["gemini-3.8-flash", "gemini-3.1-pro-preview", "gemini-3.1-flash-lite"]


# ============================================================
# 1. 目录变体展开（单一事实源：anti_truncation.catalog_variant_ids）
# ============================================================
class CatalogVariantTests(unittest.TestCase):
    def test_plain_text_model_expands_search_and_enabled_suffixes(self):
        ids = anti_truncation.catalog_variant_ids(
            "gemini-3.8-flash", {"anti_truncation_mode": "stream"})
        self.assertEqual(ids, ["gemini-3.8-flash", "gemini-3.8-flash-search",
                               "gemini-3.8-flash-antitruncation-stream"])

    def test_plain_model_without_antitruncation_still_lists_search(self):
        ids = anti_truncation.catalog_variant_ids("gemini-3.8-flash", {})
        self.assertEqual(ids, ["gemini-3.8-flash", "gemini-3.8-flash-search"])

    def test_image_model_has_no_search_variant(self):
        self.assertEqual(anti_truncation.catalog_variant_ids("gemini-image-gen", {}), ["gemini-image-gen"])
        self.assertFalse(anti_truncation.search_variant_available("nano-banana-2.1"))

    def test_suffixed_raw_entry_lists_itself_only_when_enabled(self):
        raw = "gemini-3.8-flash-antitruncation-nonstream"
        self.assertEqual(anti_truncation.catalog_variant_ids(
            raw, {"anti_truncation_mode": "nonstream"}), [raw])
        self.assertEqual(anti_truncation.catalog_variant_ids(
            raw, {"anti_truncation_mode": "off"}), [])

    def test_search_rule_requires_gemini_and_rejects_suffix(self):
        self.assertTrue(anti_truncation.search_variant_available("gemini-3-pro"))
        self.assertFalse(anti_truncation.search_variant_available("gemini-3-pro-search"))
        self.assertFalse(anti_truncation.search_variant_available("claude-sonnet-4-5"))


# ============================================================
# 2. Anthropic 目录对齐（claude_api.anthropic_models_data）
# ============================================================
class AnthropicCatalogTests(unittest.IsolatedAsyncioTestCase):
    async def _data(self, models, settings=None):
        with mock.patch.object(claude_api, "get_express_models",
                               mock.AsyncMock(return_value=list(models))), \
             mock.patch.object(claude_api.app_state, "get_settings",
                               side_effect=lambda: dict(settings or {})), \
             mock.patch.object(claude_api.app_state, "get_effective_settings",
                               side_effect=lambda model, base=None: dict(settings or {})):
            return await claude_api.anthropic_models_data()

    async def test_variants_listed_alongside_aliases(self):
        data = await self._data(AVAILABLE, {"anti_truncation_mode": "stream"})
        ids = [entry["id"] for entry in data["data"]]
        self.assertEqual(ids[0], "claude-opus-4-1")  # 别名仍在前
        self.assertIn("gemini-3.8-flash-search", ids)
        self.assertIn("gemini-3.8-flash-antitruncation-stream", ids)
        self.assertIn("gemini-3.8-flash", ids)

    async def test_default_settings_list_search_only(self):
        data = await self._data(AVAILABLE)
        ids = [entry["id"] for entry in data["data"]]
        self.assertIn("gemini-3.8-flash-search", ids)
        self.assertNotIn("gemini-3.8-flash-antitruncation-stream", ids)

    async def test_payload_backcompat_single_argument(self):
        payload = anti_truncation  # noqa: F841  （仅确认模块可导入；目录函数在下面直接断言）
        from claude_protocol import anthropic_models_payload
        result = anthropic_models_payload(["gemini-3.8-flash"])
        self.assertEqual([e["id"] for e in result["data"]][-1], "gemini-3.8-flash")


# ============================================================
# 3. Gemini 原生目录对齐（-search 列出 / 抗截断仍不列）
# ============================================================
class NativeCatalogTests(unittest.IsolatedAsyncioTestCase):
    def _request(self):
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
            express_key_manager=SimpleNamespace(get_total_keys=lambda: 1))))
        return request

    async def test_native_catalog_ids_rule(self):
        ids = gemini_native._native_catalog_ids(["gemini-3-pro", "nano-banana-2.1", "gemini-3-pro-search"])
        self.assertIn("gemini-3-pro", ids)
        self.assertIn("gemini-3-pro-search", ids)
        self.assertIn("nano-banana-2.1", ids)
        self.assertNotIn("nano-banana-2.1-search", ids)
        self.assertEqual(ids.count("gemini-3-pro-search"), 1)  # 去重

    async def test_native_list_models_contains_search_variants(self):
        with mock.patch.object(gemini_native, "get_express_models",
                               mock.AsyncMock(return_value=["gemini-test"])):
            result = await gemini_native.native_list_models(self._request(), "key")
        names = [m["name"] for m in result["models"]]
        self.assertIn("models/gemini-test", names)
        self.assertIn("models/gemini-test-search", names)
        self.assertNotIn("antitruncation", json.dumps(result))
        entry = next(m for m in result["models"] if m["name"] == "models/gemini-test-search")
        self.assertIn("google_search", entry["description"])

    async def test_native_get_model_accepts_variant(self):
        with mock.patch.object(gemini_native, "get_express_models",
                               mock.AsyncMock(return_value=["gemini-test"])):
            found = await gemini_native.native_get_model("gemini-test-search", self._request(), "key")
            self.assertEqual(found["name"], "models/gemini-test-search")
            missing = await gemini_native.native_get_model("gemini-test-antitruncation-stream",
                                                           self._request(), "key")
            self.assertEqual(missing.status_code, 404)


# ============================================================
# 4. 透传 -search 变体（剥后缀 + 注入 google_search 工具）
# ============================================================
class KeyManager:
    def __init__(self, keys=("key-1",)):
        self.records = [{"key": key, "project_id": f"project-{i}"} for i, key in enumerate(keys)]

    def get_total_keys(self):
        return len(self.records)

    def get_express_record(self, exclude_key=None):
        return dict(self.records[0])


class SearchVariantPassthroughTests(unittest.TestCase):
    def setUp(self):
        self.stats = ProxyStats()
        self.settings = dict(config.DEFAULT_SETTINGS, retry_max=2,
                             retry_backoff_seconds=0, express_location="")
        self.requests = []
        self.manager = KeyManager()

        def handler(request):
            self.requests.append(request)
            return httpx.Response(200, json={"candidates": [
                {"content": {"parts": [{"text": "ok"}]}, "finishReason": "STOP"}],
                "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 2,
                                  "totalTokenCount": 5}})

        self.upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        patches = [
            mock.patch.object(main, "stats", self.stats),
            mock.patch.object(gemini_native, "stats", self.stats),
            mock.patch.object(gemini_native, "API_KEY", "client-key"),
            mock.patch.object(main.app.state, "express_key_manager", self.manager),
            mock.patch.object(gemini_native, "passthrough_client_pool",
                              types.SimpleNamespace(get=lambda factory, proxy_uri=None: self.upstream)),
            mock.patch.object(gemini_native.app_state, "get_settings",
                              side_effect=lambda: self.settings.copy()),
            mock.patch.object(gemini_native.app_state, "get_setting",
                              side_effect=lambda key, default=None: self.settings.get(key, default)),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.addCleanup(lambda: asyncio.run(self.upstream.aclose()))
        self.client = TestClient(main.app)

    def test_generate_content_strips_suffix_and_injects_search_tool(self):
        response = self.client.post(
            "/v1beta/models/gemini-test-search:generateContent",
            headers={"x-goog-api-key": "client-key"}, json={"contents": [{"role": "user"}]})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(self.requests), 1)
        sent = self.requests[0]
        self.assertIn("/models/gemini-test:generateContent", str(sent.url))
        self.assertNotIn("gemini-test-search", str(sent.url))
        body = json.loads(sent.content)
        self.assertEqual(body["tools"], [{"google_search": {}}])
        self.assertIn("contents", body)  # 原字段保留

    def test_count_tokens_strips_suffix_without_body_rewrite(self):
        response = self.client.post(
            "/v1beta/models/gemini-test-search:countTokens",
            headers={"x-goog-api-key": "client-key"}, json={"contents": [{"role": "user"}]})
        self.assertEqual(response.status_code, 200)
        sent = self.requests[0]
        self.assertIn("/models/gemini-test:countTokens", str(sent.url))
        self.assertEqual(json.loads(sent.content), {"contents": [{"role": "user"}]})

    def test_unsupported_variant_returns_400(self):
        response = self.client.post(
            "/v1beta/models/nano-banana-search:generateContent",
            headers={"x-goog-api-key": "client-key"}, json={"contents": []})
        self.assertEqual(response.status_code, 400)
        self.assertIn("-search", json.loads(response.content)["error"]["message"])

    def test_existing_search_tool_not_duplicated(self):
        response = self.client.post(
            "/v1beta/models/gemini-test-search:generateContent",
            headers={"x-goog-api-key": "client-key"},
            json={"contents": [], "tools": [{"google_search": {}}, {"functionDeclarations": []}]})
        self.assertEqual(response.status_code, 200)
        body = json.loads(self.requests[0].content)
        self.assertEqual(len(body["tools"]), 2)
        self.assertEqual(body["tools"][0], {"google_search": {}})

    def test_plain_model_body_forwarded_verbatim(self):
        raw = b'{"contents":[{"role":"user"}]  , "nested": {"k": 1}}'
        response = self.client.post(
            "/v1beta/models/gemini-test:generateContent",
            headers={"x-goog-api-key": "client-key", "content-type": "application/json"},
            content=raw)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.requests[0].content, raw)  # 字节级透传不被破坏


# ============================================================
# 5. 原生用量提取与日志
# ============================================================
class NativeUsageTests(unittest.TestCase):
    def test_body_usage_extraction(self):
        body = json.dumps({"candidates": [], "usageMetadata": {
            "promptTokenCount": 7, "totalTokenCount": 9}}).encode()
        usage = gemini_native._native_body_usage(body)
        self.assertEqual(usage["promptTokenCount"], 7)
        self.assertIsNone(gemini_native._native_body_usage(b'{"candidates": []}'))
        self.assertIsNone(gemini_native._native_body_usage(b"not-json-but-has-usageMetadata"))

    def test_state_captures_last_usage_metadata(self):
        state = gemini_native._NativeStreamState()
        state.inspect({"candidates": [{"content": {"parts": [{"text": "a"}]}}],
                       "usageMetadata": {"promptTokenCount": 1, "totalTokenCount": 2}})
        state.inspect({"candidates": [{"content": {"parts": [{"text": "b"}]},
                                       "finishReason": "STOP"}],
                       "usageMetadata": {"promptTokenCount": 3, "totalTokenCount": 6}})
        self.assertEqual(state.usage["promptTokenCount"], 3)
        self.assertEqual(state.usage["totalTokenCount"], 6)

    def test_usage_log_format(self):
        buffer = io.StringIO()
        with mock.patch("sys.stdout", buffer):
            gemini_native._log_native_usage(
                {"promptTokenCount": 10, "candidatesTokenCount": 4,
                 "thoughtsTokenCount": 2, "totalTokenCount": 16})
        self.assertIn("💰 [算力消耗统计]", buffer.getvalue())
        self.assertIn("提示词 10", buffer.getvalue())
        self.assertIn("生成 4", buffer.getvalue())
        self.assertIn("思考 2", buffer.getvalue())
        self.assertIn("总计 16", buffer.getvalue())

    def test_usage_log_skips_empty(self):
        buffer = io.StringIO()
        with mock.patch("sys.stdout", buffer):
            gemini_native._log_native_usage(None)
            gemini_native._log_native_usage({})
        self.assertEqual(buffer.getvalue(), "")


# ============================================================
# 6. Claude 完成日志（流式 / 非流式）
# ============================================================
def base_body(**overrides):
    body = {"model": "claude-sonnet-4-5", "max_tokens": 128,
            "messages": [{"role": "user", "content": "hi"}]}
    body.update(overrides)
    return body


class ClaudeCompletionLogTests(unittest.IsolatedAsyncioTestCase):
    def _request(self, body):
        namespace = SimpleNamespace(json=mock.AsyncMock(return_value=body),
                                    headers={}, query_params={})
        namespace.app = SimpleNamespace()
        return namespace

    async def _run(self, body, upstream_response):
        with mock.patch.object(claude_api, "get_express_models",
                               mock.AsyncMock(return_value=list(AVAILABLE))), \
             mock.patch.object(claude_api, "_call_upstream",
                               mock.AsyncMock(return_value=upstream_response)):
            return await claude_api.claude_messages(self._request(body), "test-key")

    async def test_stream_completion_log_with_tokens(self):
        async def source():
            yield b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'
            yield b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n'
            yield b'data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2}}\n\n'
            yield b'data: [DONE]\n\n'

        buffer = io.StringIO()
        with mock.patch("sys.stdout", buffer):
            result = await self._run(base_body(stream=True),
                                     StreamingResponse(source(), media_type="text/event-stream"))
            parts = [chunk if isinstance(chunk, str) else chunk.decode()
                     async for chunk in result.body_iterator]
        log = buffer.getvalue()
        self.assertIn("✅ [Claude] 模型 claude-sonnet-4-5 | 流式翻译完成", log)
        self.assertIn("输入 5 / 输出 2 tokens", log)
        self.assertTrue(parts)

    async def test_nonstream_completion_log(self):
        upstream_payload = {"id": "x", "object": "chat.completion", "created": 1,
                            "model": "gemini-3.8-flash",
                            "choices": [{"index": 0, "message": {"role": "assistant",
                                                                 "content": "ok"},
                                         "finish_reason": "stop"}],
                            "usage": {"prompt_tokens": 3, "completion_tokens": 1}}
        from fastapi.responses import JSONResponse
        buffer = io.StringIO()
        with mock.patch("sys.stdout", buffer):
            result = await self._run(base_body(), JSONResponse(content=upstream_payload))
        self.assertEqual(result.status_code, 200)
        log = buffer.getvalue()
        self.assertIn("✅ [Claude] 模型 claude-sonnet-4-5 | 非流式响应完成", log)
        self.assertIn("输入 3 / 输出 1 tokens", log)

    async def test_stream_failure_has_no_completion_log(self):
        from fastapi.responses import JSONResponse
        buffer = io.StringIO()
        with mock.patch("sys.stdout", buffer):
            await self._run(base_body(stream=True),
                            JSONResponse(status_code=500, content={"error": {"message": "boom"}}))
        self.assertNotIn("流式翻译完成", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
