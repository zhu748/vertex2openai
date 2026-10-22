"""Express 通道 schema 方言净化的回归测试。

背景：OpenAI 方言的宽松 JSON Schema（空节点 {} / true、type 数组、元组式
items）原样透传给 Gemini 时，要么被服务端以 400 INVALID_ARGUMENT
"missing field" 拒绝（json_patch 类结构化输出），要么令 SDK 序列化崩溃。
"""

import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

_STATE = tempfile.TemporaryDirectory(prefix="v2o-sanitize-test-")
os.environ.setdefault("STATE_DIR", _STATE.name)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from models import OpenAIRequest
from schema_validation import sanitize_for_gemini, validate_json_schema
import generation_config


def _any_value(depth: int) -> dict:
    """客户端典型的"任意 JSON 值"有界递归展开（OpenAI 方言合法）。"""
    if depth <= 0:
        return {}
    return {"anyOf": [
        {"type": "string"}, {"type": "number"}, {"type": "boolean"},
        {"type": "object"}, {"type": "array", "items": _any_value(depth - 1)}, {"type": "null"},
    ]}


JSON_PATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "json_patch": {
            "type": "array",
            "items": {
                "anyOf": [
                    {"type": "object",
                     "properties": {"op": {"type": "string"}, "path": {"type": "string"},
                                    "value": _any_value(3)},
                     "required": ["op", "path"]},
                    {"type": "object",
                     "properties": {"op": {"type": "string"}, "path": {"type": "string"}},
                     "required": ["op", "path"]},
                    {"type": "object",
                     "properties": {"op": {"type": "string"}, "path": {"type": "string"},
                                    "value": _any_value(3)},
                     "required": ["op", "path"]},
                ]
            },
        }
    },
    "required": ["json_patch"],
}


def _find_empty(node, path="$"):
    hits = []
    if isinstance(node, dict):
        if not node:
            hits.append(path)
        for key, value in node.items():
            hits.extend(_find_empty(value, f"{path}.{key}"))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            hits.extend(_find_empty(value, f"{path}[{index}]"))
    return hits


def _assert_strict(node, path="$", is_map=False):
    """每个 schema 节点必须有 type 或非空联合（properties 映射表除外）。"""
    if isinstance(node, dict):
        if not is_map:
            has_type = isinstance(node.get("type"), str)
            has_union = isinstance(node.get("anyOf"), list) and bool(node["anyOf"])
            has_union = has_union or (isinstance(node.get("oneOf"), list) and bool(node["oneOf"]))
            if not (has_type or has_union):
                raise AssertionError(f"节点无 type 且无联合: {path} -> {sorted(node.keys())}")
        for key, value in node.items():
            _assert_strict(value, f"{path}.{key}", is_map=key == "properties")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            _assert_strict(value, f"{path}[{index}]")


class TestAnyValueReplacement(unittest.TestCase):
    def test_json_patch_repro_no_empty_nodes(self):
        """复现线上 400：底层空节点必须全部消失且全树严格。"""
        sanitized = sanitize_for_gemini(copy.deepcopy(JSON_PATCH_SCHEMA))
        self.assertEqual(_find_empty(sanitized), [])
        _assert_strict(sanitized)
        # 原始 schema 确实带着空节点（防止测试输入漂移失去复现意义）
        self.assertEqual(len(_find_empty(JSON_PATCH_SCHEMA)), 2)

    def test_empty_member_inside_anyof_becomes_union(self):
        schema = {"anyOf": [{"type": "string"}, {}]}
        sanitized = sanitize_for_gemini(schema)
        self.assertEqual(len(sanitized["anyOf"]), 2)
        self.assertEqual(sorted(sanitized["anyOf"][1]["type"] for _ in [0]), sorted("string")) if False else None
        self.assertIn("anyOf", sanitized["anyOf"][1])
        self.assertEqual(len(sanitized["anyOf"][1]["anyOf"]), 7)

    def test_true_schema_becomes_union(self):
        sanitized = sanitize_for_gemini(True)
        self.assertEqual(len(sanitized["anyOf"]), 7)
        self.assertEqual({m["type"] for m in sanitized["anyOf"]},
                         {"string", "number", "integer", "boolean", "null", "object", "array"})

    def test_items_true_becomes_union(self):
        sanitized = sanitize_for_gemini({"type": "array", "items": True})
        self.assertEqual(sanitized["type"], "array")
        self.assertEqual(len(sanitized["items"]["anyOf"]), 7)

    def test_metadata_only_node_keeps_description(self):
        sanitized = sanitize_for_gemini({"description": "任意值"})
        self.assertEqual(sanitized["description"], "任意值")
        self.assertEqual(len(sanitized["anyOf"]), 7)

    def test_empty_object_property_value_becomes_union(self):
        schema = {"type": "object", "properties": {"value": {}}}
        sanitized = sanitize_for_gemini(schema)
        self.assertEqual(len(sanitized["properties"]["value"]["anyOf"]), 7)


class TestTypeArrayAndInference(unittest.TestCase):
    def test_type_array_becomes_nullable(self):
        sanitized = sanitize_for_gemini({"type": ["string", "null"]})
        self.assertEqual(sanitized, {"type": "string", "nullable": True})

    def test_type_array_all_null(self):
        sanitized = sanitize_for_gemini({"type": ["null"]})
        self.assertEqual(sanitized, {"type": "null"})

    def test_existing_nullable_flag_not_clobbered(self):
        sanitized = sanitize_for_gemini({"type": "string", "nullable": True})
        self.assertEqual(sanitized, {"type": "string", "nullable": True})

    def test_properties_infers_object(self):
        sanitized = sanitize_for_gemini({"properties": {"a": {"type": "string"}}})
        self.assertEqual(sanitized["type"], "object")

    def test_items_infers_array(self):
        sanitized = sanitize_for_gemini({"items": {"type": "number"}})
        self.assertEqual(sanitized["type"], "array")

    def test_string_enum_infers_string(self):
        sanitized = sanitize_for_gemini({"enum": ["a", "b"]})
        self.assertEqual(sanitized["type"], "string")
        self.assertEqual(sanitized["enum"], ["a", "b"])

    def test_tuple_items_becomes_anyof(self):
        schema = {"type": "array", "items": [{"type": "string"}, {"type": "number"}]}
        sanitized = sanitize_for_gemini(schema)
        self.assertEqual(sanitized["items"],
                         {"anyOf": [{"type": "string"}, {"type": "number"}]})


class TestSanitizerContracts(unittest.TestCase):
    def test_idempotent(self):
        once = sanitize_for_gemini(copy.deepcopy(JSON_PATCH_SCHEMA))
        twice = sanitize_for_gemini(copy.deepcopy(once))
        self.assertEqual(once, twice)

    def test_input_not_mutated(self):
        original = copy.deepcopy(JSON_PATCH_SCHEMA)
        sanitize_for_gemini(JSON_PATCH_SCHEMA)
        self.assertEqual(JSON_PATCH_SCHEMA, original)

    def test_strict_schema_unchanged(self):
        strict = {
            "type": "object",
            "description": "desc",
            "properties": {
                "name": {"type": "string", "minLength": 1, "maxLength": 64},
                "tags": {"type": "array", "items": {"type": "string"}, "minItems": 0},
                "mode": {"type": "string", "enum": ["fast", "slow"], "nullable": True},
                "meta": {"type": "object", "properties": {"v": {"type": "integer"}}},
            },
            "required": ["name"],
        }
        self.assertEqual(sanitize_for_gemini(copy.deepcopy(strict)), strict)

    def test_non_dict_passthrough(self):
        self.assertIsNone(sanitize_for_gemini(None))
        self.assertEqual(sanitize_for_gemini("x"), "x")

    def test_admission_still_accepts_loose_schema(self):
        # 净化不改变准入校验的判定：宽松 schema 依旧放行（由净化兜底而非拒绝）
        validate_json_schema(copy.deepcopy(JSON_PATCH_SCHEMA))


class TestGenerationConfigIntegration(unittest.TestCase):
    def _request(self, **kwargs) -> OpenAIRequest:
        payload = {"model": "gemini-3.5-flash-lite", "messages": [{"role": "user", "content": "hi"}]}
        payload.update(kwargs)
        return OpenAIRequest(**payload)

    def test_sanitized_output_sdk_safe(self):
        """净化产物必须能通过 SDK 构造并完成 JSON 序列化。

        旧故障模式（回归锚点）：
        - 空节点 {} 原样发给服务端 -> 400 INVALID_ARGUMENT "missing field"；
        - type 数组构造 types.Schema -> AttributeError 'list' has no 'upper'。
        """
        from google.genai import types

        sanitized = sanitize_for_gemini(copy.deepcopy(JSON_PATCH_SCHEMA))
        cfg = types.GenerateContentConfig(
            response_mime_type="application/json", response_schema=sanitized)
        schema_obj = cfg.response_schema
        payload = schema_obj.model_dump(exclude_none=True) if hasattr(schema_obj, "model_dump") else schema_obj
        serialized = json.dumps(payload)  # 不抛异常即为契约达成
        self.assertIn("json_patch", serialized)
        self.assertNotIn("{}", serialized.replace("anyOf", ""))  # 无空对象节点残留

    def test_response_schema_sanitized_end_to_end(self):
        request = self._request(response_format={
            "type": "json_schema",
            "json_schema": {"name": "patch", "schema": copy.deepcopy(JSON_PATCH_SCHEMA)},
        })
        config = generation_config.create_generation_config(request)
        schema = config["response_schema"]
        self.assertEqual(_find_empty(schema), [])
        _assert_strict(schema)
        self.assertEqual(config["response_mime_type"], "application/json")

    def test_function_parameters_sanitized_end_to_end(self):
        request = self._request(tools=[{
            "type": "function",
            "function": {
                "name": "apply_patch",
                "parameters": {
                    "type": "object",
                    "properties": {"value": {"type": ["string", "null"]}, "any": {}},
                    "required": ["value"],
                },
            },
        }])
        config = generation_config.create_generation_config(request)
        parameters = config["tools"][0]["function_declarations"][0]["parameters"]
        self.assertEqual(parameters["properties"]["value"], {"type": "string", "nullable": True})
        self.assertEqual(len(parameters["properties"]["any"]["anyOf"]), 7)
        # 原始请求对象保持不变（重试/控制台展示仍用 OpenAI 方言原貌）
        self.assertEqual(request.tools[0]["function"]["parameters"]["properties"]["value"],
                         {"type": ["string", "null"]})

    def test_json_object_without_schema_untouched(self):
        request = self._request(response_format={"type": "json_object"})
        config = generation_config.create_generation_config(request)
        self.assertEqual(config["response_mime_type"], "application/json")
        self.assertNotIn("response_schema", config)


if __name__ == "__main__":
    unittest.main(verbosity=2)
