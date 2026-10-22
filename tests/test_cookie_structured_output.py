"""Cookie 通道结构化输出（cookie_structured_output 开关）的回归测试。

契约：
- 开关默认关：response_format 显式 400 拒绝（保持历史行为，含快速失败路径）；
- 开关开：json_object -> responseMimeType；json_schema -> 追加 UI Schema 形态的
  responseSchema（先经 sanitize_for_gemini 严格方言净化）；
- 生图模型不接结构化输出；入参 schema 不被改写。
"""

import asyncio
import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-cookie-so-test-")
os.environ.setdefault("STATE_DIR", _STATE.name)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from models import OpenAIRequest  # noqa: E402
from runtime_state import app_state  # noqa: E402
from schema_validation import SchemaValidationError  # noqa: E402
import upstreams.cookie_request as cookie_request  # noqa: E402
import upstreams.cookie_proxy as cookie_proxy  # noqa: E402


JSON_PATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "json_patch": {
            "type": "array",
            "items": {
                "anyOf": [
                    {"type": "object",
                     "properties": {"op": {"type": "string"}, "path": {"type": "string"},
                                    "value": {}},
                     "required": ["op", "path"]},
                ]
            },
        }
    },
    "required": ["json_patch"],
}

USER_MSG = [{"role": "user", "content": "hi"}]


def _request(response_format=None, model="gemini-3.6-flash") -> OpenAIRequest:
    return OpenAIRequest(model=model, messages=USER_MSG, response_format=response_format)


def _build(request, model="gemini-3.6-flash") -> dict:
    return cookie_request.build_request_body("probe-project", model, request)


def _enabled():
    return mock.patch.object(app_state, "get_setting",
                             side_effect=lambda key, default=None:
                             True if key == "cookie_structured_output"
                             else default)


class TestDisabledByDefault(unittest.TestCase):
    def test_json_object_rejected(self):
        with self.assertRaises(SchemaValidationError):
            _build(_request({"type": "json_object"}))

    def test_json_schema_rejected(self):
        with self.assertRaises(SchemaValidationError):
            _build(_request({"type": "json_schema",
                             "json_schema": {"schema": {"type": "object"}}}))

    def test_rejection_message_mentions_probe_and_setting(self):
        try:
            _build(_request({"type": "json_object"}))
        except SchemaValidationError as exc:
            self.assertIn("probe_cookie_structured_output", str(exc))
            self.assertIn("cookie_structured_output", str(exc))

    def test_text_and_absent_format_still_pass(self):
        for fmt in (None, {"type": "text"}):
            body = _build(_request(fmt))
            gen = body["variables"]["generationConfig"]
            self.assertNotIn("responseMimeType", gen)
            self.assertNotIn("responseSchema", gen)


class TestEnabledWiring(unittest.TestCase):
    def test_json_object_mime_only(self):
        with _enabled():
            body = _build(_request({"type": "json_object"}))
        gen = body["variables"]["generationConfig"]
        self.assertEqual(gen["responseMimeType"], "application/json")
        self.assertNotIn("responseSchema", gen)

    def test_json_schema_full_ui_schema_shape(self):
        with _enabled():
            body = _build(_request({"type": "json_schema",
                                    "json_schema": {"name": "patch",
                                                    "schema": copy.deepcopy(JSON_PATCH_SCHEMA)}}))
        gen = body["variables"]["generationConfig"]
        self.assertEqual(gen["responseMimeType"], "application/json")
        schema = gen["responseSchema"]
        # UI Schema 形态：大写类型 + [{key, value}] 属性表
        self.assertEqual(schema["type"], "OBJECT")
        props = schema["properties"]
        self.assertIsInstance(props, list)
        patch_prop = next(p for p in props if p["key"] == "json_patch")
        self.assertEqual(patch_prop["value"]["type"], "ARRAY")
        # items 节点即 anyOf 联合（无独立 type）
        union = patch_prop["value"]["items"]["anyOf"]
        op_member = next(m for m in union if m.get("type") == "OBJECT")
        value_prop = next(p for p in op_member["properties"] if p["key"] == "value")
        # 宽松空节点被净化为全类型联合，且转成 UI Schema 大写类型
        member_types = {m.get("type") for m in value_prop["value"]["anyOf"]}
        self.assertEqual(member_types,
                         {"STRING", "NUMBER", "INTEGER", "BOOLEAN", "NULL", "OBJECT", "ARRAY"})

    def test_type_array_normalized_before_ui_schema(self):
        schema = {"type": "object",
                  "properties": {"value": {"type": ["string", "null"]}}}
        with _enabled():
            body = _build(_request({"type": "json_schema",
                                    "json_schema": {"schema": schema}}))
        props = body["variables"]["generationConfig"]["responseSchema"]["properties"]
        value_prop = next(p for p in props if p["key"] == "value")
        self.assertEqual(value_prop["value"], {"type": "STRING", "nullable": True})

    def test_json_schema_without_schema_object_degrades_to_mime(self):
        with _enabled():
            body = _build(_request({"type": "json_schema", "json_schema": {}}))
        gen = body["variables"]["generationConfig"]
        self.assertEqual(gen["responseMimeType"], "application/json")
        self.assertNotIn("responseSchema", gen)

    def test_input_schema_not_mutated(self):
        schema = copy.deepcopy(JSON_PATCH_SCHEMA)
        with _enabled():
            _build(_request({"type": "json_schema", "json_schema": {"schema": schema}}))
        self.assertEqual(schema, JSON_PATCH_SCHEMA)

    def test_image_model_rejected_even_when_enabled(self):
        with _enabled():
            with self.assertRaises(SchemaValidationError):
                cookie_request._structured_output(
                    _request({"type": "json_object"}), {"is_image": True})

    def test_unknown_format_type_rejected(self):
        with _enabled():
            with self.assertRaises(SchemaValidationError):
                cookie_request._structured_output(_request({"type": "xml"}), {"is_image": False})

    def test_prefill_path_unaffected(self):
        # build_request_body 的签名不变：cookie_proxy 的既有调用点无需改动
        with _enabled():
            body = cookie_request.build_request_body(
                "p", "gemini-3.6-flash", _request({"type": "json_object"}),
                prefill_active=True)
        self.assertIn("responseMimeType", body["variables"]["generationConfig"])


class TestProxyFastFailGate(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_rejects_before_network(self):
        obj = _request({"type": "json_object"})
        with mock.patch.object(cookie_proxy, "open_upstream_client") as network:
            response = await cookie_proxy.CookieProxyUpstream().chat_completions(
                obj, SimpleNamespace())
        self.assertEqual(response.status_code, 400)
        network.assert_not_called()
        body = json.loads(response.body)
        self.assertIn("cookie_structured_output", body["error"]["message"])

    async def test_enabled_passes_the_gate(self):
        obj = _request({"type": "json_object"})
        gate_passed = False

        async def fake_prepare_messages(request_obj, *args, **kwargs):
            return request_obj, "", False

        # 停在认证检查（无 Cookie 环境必然 401）即可证明结构化输出门已放行
        with _enabled(), \
                mock.patch.object(cookie_proxy, "prepare_messages", side_effect=fake_prepare_messages), \
                mock.patch.object(cookie_proxy, "prepare_transport",
                                  side_effect=lambda req, *a, **k: (req, None)), \
                mock.patch.object(cookie_proxy, "open_upstream_client") as network:
            response = await cookie_proxy.CookieProxyUpstream().chat_completions(
                obj, SimpleNamespace())
        gate_passed = True  # 未在门控处 400 即通过
        self.assertTrue(gate_passed)
        # 无 Cookie 配置：应落在 401 认证错误，而不是 400 结构化输出拒绝
        self.assertEqual(response.status_code, 401)
        network.assert_not_called()


class TestMixedModeRouting(unittest.TestCase):
    """混合模式：结构化请求默认只选 Express；开关开启后 Cookie 记录也参与选择。"""

    def _express_only_flag(self, enable_setting: bool) -> bool:
        import routes.chat_api as chat_api
        from credential_manager import credential_manager

        captured = {}

        def fake_get_mixed_record(key_manager, express_only=None):
            captured["express_only"] = express_only
            return None  # 无可用记录 -> 401，足以断言路由参数

        def run():
            return asyncio.run(self._call(chat_api))

        # 关闭分支走真实默认值（临时 STATE_DIR 下 cookie_structured_output=False）；
        # 开启分支仅对该键放行，其余设置委托真实 app_state。
        with mock.patch.object(app_state, "get_channel_mode", return_value="mixed"), \
                mock.patch.object(credential_manager, "get_mixed_record",
                                  side_effect=fake_get_mixed_record):
            if enable_setting:
                with _enabled():
                    response = run()
            else:
                response = run()
        self.assertEqual(response.status_code, 401)
        return captured["express_only"]

    @staticmethod
    def _call(chat_api):
        request = _request({"type": "json_object"})
        fastapi_request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(express_key_manager=object())))
        return chat_api._call_upstream(request, fastapi_request)

    def test_default_routes_structured_to_express_only(self):
        # 临时 STATE_DIR 下的真实默认值：cookie_structured_output=False
        self.assertTrue(self._express_only_flag(enable_setting=False))

    def test_enabled_lets_cookie_records_participate(self):
        self.assertFalse(self._express_only_flag(enable_setting=True))


if __name__ == "__main__":
    unittest.main(verbosity=2)
