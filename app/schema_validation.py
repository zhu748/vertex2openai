"""Defensive validation for client-supplied JSON Schemas."""

import copy
import json
from typing import Any


MAX_SCHEMA_DEPTH = 32
MAX_SCHEMA_NODES = 10_000
MAX_SCHEMA_SERIALIZED_BYTES = 1_000_000
MAX_SANITIZE_DEPTH = 64
_JSON_SCHEMA_TYPES = {"object", "array", "string", "integer", "number", "boolean", "null"}
_SCHEMA_MAP_KEYS = {"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
_SCHEMA_SINGLE_KEYS = {"items", "additionalProperties", "not", "contains", "propertyNames", "if", "then", "else"}
_SCHEMA_LIST_KEYS = {"oneOf", "anyOf", "allOf", "prefixItems"}

# 任意 JSON 值的 Gemini 严格方言等价物。OpenAI 方言用空 schema（{} / true）表达
# "anything"，但 Gemini 的 Schema proto 拒绝空消息（400 INVALID_ARGUMENT
# "missing field"）。此处用全类型联合展开；OBJECT 不带 properties、ARRAY 不带
# items 在 Gemini 里都是"无约束"（两字段均为 Optional），因此无需人工递归截断。
_GEMINI_ANY_TYPES = ("string", "number", "integer", "boolean", "null", "object", "array")


class SchemaValidationError(ValueError):
    """A client schema is malformed or exceeds defensive complexity limits."""


def validate_json_schema(schema: Any, *, label: str = "schema") -> None:
    if not isinstance(schema, dict):
        raise SchemaValidationError(f"{label} must be a JSON object")
    try:
        serialized = json.dumps(schema, ensure_ascii=False, separators=(",", ":"),
                                check_circular=True, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise SchemaValidationError(f"{label} is not a valid JSON Schema object: {exc}") from None
    if len(serialized.encode("utf-8")) > MAX_SCHEMA_SERIALIZED_BYTES:
        raise SchemaValidationError(
            f"{label} exceeds the {MAX_SCHEMA_SERIALIZED_BYTES}-byte serialized-size limit")

    nodes = 0
    active: set[int] = set()

    def visit(node: Any, depth: int, path: str) -> None:
        nonlocal nodes
        if depth > MAX_SCHEMA_DEPTH:
            raise SchemaValidationError(f"{label} exceeds maximum depth {MAX_SCHEMA_DEPTH} at {path}")
        nodes += 1
        if nodes > MAX_SCHEMA_NODES:
            raise SchemaValidationError(f"{label} exceeds maximum node count {MAX_SCHEMA_NODES}")
        if not isinstance(node, dict):
            raise SchemaValidationError(f"{path} must be a JSON object")
        ident = id(node)
        if ident in active:
            raise SchemaValidationError(f"{label} contains a circular reference at {path}")
        active.add(ident)
        try:
            raw_type = node.get("type")
            types = raw_type if isinstance(raw_type, list) else [raw_type]
            if raw_type is not None:
                if not types or any(not isinstance(t, str) or t not in _JSON_SCHEMA_TYPES for t in types):
                    raise SchemaValidationError(f"{path}.type contains an unsupported JSON Schema type")
            required = node.get("required")
            if required is not None and (not isinstance(required, list)
                                         or any(not isinstance(name, str) for name in required)):
                raise SchemaValidationError(f"{path}.required must be an array of strings")

            for key in _SCHEMA_MAP_KEYS:
                value = node.get(key)
                if value is None:
                    continue
                if not isinstance(value, dict):
                    raise SchemaValidationError(f"{path}.{key} must be an object")
                for name, child in value.items():
                    if not isinstance(name, str):
                        raise SchemaValidationError(f"{path}.{key} keys must be strings")
                    visit(child, depth + 1, f"{path}.{key}.{name}")

            for key in _SCHEMA_SINGLE_KEYS:
                value = node.get(key)
                if value is None or isinstance(value, bool):
                    continue
                if key == "items" and isinstance(value, list):
                    for index, child in enumerate(value):
                        visit(child, depth + 1, f"{path}.{key}[{index}]")
                else:
                    visit(value, depth + 1, f"{path}.{key}")

            for key in _SCHEMA_LIST_KEYS:
                value = node.get(key)
                if value is None:
                    continue
                if not isinstance(value, list):
                    raise SchemaValidationError(f"{path}.{key} must be an array")
                for index, child in enumerate(value):
                    visit(child, depth + 1, f"{path}.{key}[{index}]")
        finally:
            active.remove(ident)

    visit(schema, 0, label)


def validate_request_schemas(request: Any) -> None:
    for index, tool in enumerate(getattr(request, "tools", None) or []):
        if not isinstance(tool, dict) or (tool.get("type") != "function" and "function" not in tool):
            continue
        function = tool.get("function")
        if not isinstance(function, dict):
            raise SchemaValidationError(f"tools[{index}].function must be an object")
        name = function.get("name")
        if not isinstance(name, str) or not name.strip():
            raise SchemaValidationError(f"tools[{index}].function.name must be a non-empty string")
        if any(ord(char) < 32 or ord(char) == 127 for char in name):
            raise SchemaValidationError(f"tools[{index}].function.name must not contain control characters")
        parameters = function.get("parameters")
        if parameters is not None:
            validate_json_schema(parameters, label=f"tools[{index}].function.parameters")

    response_format = getattr(request, "response_format", None)
    if isinstance(response_format, dict) and response_format.get("type") == "json_schema":
        json_schema = response_format.get("json_schema")
        if not isinstance(json_schema, dict):
            raise SchemaValidationError("response_format.json_schema must be an object")
        schema = json_schema.get("schema")
        validate_json_schema(schema, label="response_format.json_schema.schema")


def _any_value_schema(description: str | None = None) -> dict:
    """构造"任意 JSON 值"的 Gemini 严格方言节点。"""
    node: dict[str, Any] = {
        "anyOf": [{"type": name} for name in _GEMINI_ANY_TYPES],
    }
    if description:
        node["description"] = description
    return node


def _sanitize_node(node: Any, depth: int) -> Any:
    """OpenAI 宽松方言 -> Gemini 严格 Schema 方言（生成新树，不改写入参）。

    只转换会被服务端/SDK 拒绝的写法，其余键原样透传（含未知键——服务端
    对未知字段宽容，主动剥离反而可能改变既有可用行为）：
    1. 空节点（{} / true / 仅元数据）→ 全类型联合（含 description 保留）；
    2. type 数组（["string","null"]）→ 首个非 null 类型 + nullable:true
       （否则 SDK 序列化时直接 AttributeError，请求根本发不出去）；
    3. 缺 type 但有 properties/items/纯字符串 enum → 结构推断（与 Cookie
       通道 _cookie_ui_schema 同口径）；
    4. items 为数组（draft-07 元组形式）→ anyOf 联合（Gemini items 是单
       Schema）；items:true → 全类型联合。
    """
    if node is True:
        return _any_value_schema()
    if not isinstance(node, dict):
        return node
    if depth > MAX_SANITIZE_DEPTH:
        # 防御：未经 validate_json_schema 的病态输入（如循环引用）兜底。
        return copy.deepcopy(node)

    raw_type = node.get("type")
    nullable = False
    if isinstance(raw_type, list):
        non_null = [t for t in raw_type if isinstance(t, str) and t.lower() != "null"]
        nullable = bool(non_null) and len(non_null) != len(raw_type)
        # 仅 ["null"] 时语义是"只能为 null"，不是"任意值"。
        raw_type = non_null[0].lower() if non_null else ("null" if raw_type else None)

    if raw_type is None and "anyOf" not in node and "oneOf" not in node:
        if isinstance(node.get("properties"), dict):
            raw_type = "object"
        elif "items" in node:
            raw_type = "array"
        elif node.get("enum") and all(isinstance(v, str) for v in node["enum"]):
            raw_type = "string"
        elif not any(
            key in node for key in ("properties", "items", "enum", "format", "minimum", "maximum",
                                    "minLength", "maxLength", "pattern", "minItems", "maxItems")
        ):
            # 空节点或仅剩 title/default/$comment 等元数据：约束为空 = "any"。
            return _any_value_schema(node.get("description") if isinstance(node.get("description"), str) else None)

    out: dict[str, Any] = {}
    if raw_type is not None:
        out["type"] = raw_type
    if nullable or node.get("nullable") is True:
        out["nullable"] = True

    for key, value in node.items():
        if key in ("type", "nullable"):
            continue
        if key == "properties" and isinstance(value, dict):
            out["properties"] = {name: _sanitize_node(child, depth + 1) for name, child in value.items()}
        elif key == "items":
            if isinstance(value, list):
                out["items"] = {"anyOf": [_sanitize_node(member, depth + 1) for member in value]}
            elif value is True:
                out["items"] = _any_value_schema()
            elif isinstance(value, dict):
                out["items"] = _sanitize_node(value, depth + 1)
            elif value is False or value is None:
                continue
            else:
                out["items"] = value
        elif key in ("anyOf", "oneOf") and isinstance(value, list):
            out[key] = [_sanitize_node(member, depth + 1) for member in value]
        else:
            out[key] = value
    return out


def sanitize_for_gemini(schema: Any) -> Any:
    """把客户端 JSON Schema 规范成 Gemini 严格方言（幂等，不改写入参）。

    背景：Express 通道原样透传 OpenAI 方言 schema，json_patch 类"value 可为
    任意 JSON"的递归展开在底层留空节点（{}），Gemini 服务端 proto 校验以
    400 INVALID_ARGUMENT "missing field" 拒绝整份请求。
    """
    return _sanitize_node(schema, 0)
