"""Defensive validation for client-supplied JSON Schemas."""

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
            for key in ("description", "title", "format"):
                if key in node and not isinstance(node[key], str):
                    raise SchemaValidationError(f"{path}.{key} must be a string")
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
        description = function.get("description")
        if description is not None and not isinstance(description, str):
            raise SchemaValidationError(f"tools[{index}].function.description must be a string")
        strict = function.get("strict")
        if strict is not None and not isinstance(strict, bool):
            raise SchemaValidationError(f"tools[{index}].function.strict must be a boolean")
        parameters = function.get("parameters")
        if parameters is not None:
            validate_json_schema(parameters, label=f"tools[{index}].function.parameters")
            sanitize_for_gemini(parameters)

    response_format = getattr(request, "response_format", None)
    if isinstance(response_format, dict) and response_format.get("type") == "json_schema":
        json_schema = response_format.get("json_schema")
        if not isinstance(json_schema, dict):
            raise SchemaValidationError("response_format.json_schema must be an object")
        schema = json_schema.get("schema")
        validate_json_schema(schema, label="response_format.json_schema.schema")
        sanitize_for_gemini(schema)


def _any_value_schema(description: str | None = None) -> dict:
    """构造"任意 JSON 值"的 Gemini 严格方言节点。"""
    node: dict[str, Any] = {
        "anyOf": [{"type": name} for name in _GEMINI_ANY_TYPES],
    }
    if description:
        node["description"] = description
    return node


class _ExpansionBudget:
    def __init__(self):
        self.nodes = 0
        self.bytes = 0

    def visit(self):
        self.nodes += 1
        if self.nodes > MAX_SCHEMA_NODES:
            raise SchemaValidationError("Schema 展开超过节点数上限。")

    def charge(self, value):
        self.bytes += len(json.dumps(value, ensure_ascii=False).encode("utf-8"))
        if self.bytes > MAX_SCHEMA_SERIALIZED_BYTES:
            raise SchemaValidationError("Schema 展开超过字节上限。")


def _sanitize_node(node: Any, depth: int, budget: _ExpansionBudget) -> Any:
    """OpenAI 宽松方言 -> Gemini 严格 Schema 方言（生成新树，不改写入参）。

    只转换会被服务端/SDK 拒绝的写法，其余键原样透传（含未知键——服务端
    对未知字段宽容，主动剥离反而可能改变既有可用行为）：
    1. 空节点（{} / true / 仅元数据）→ 全类型联合（含 description 保留）；
    2. type 数组 → anyOf；单类型 + null 使用 nullable:true
       （否则 SDK 序列化时直接 AttributeError，请求根本发不出去）；
    3. 缺 type 但有 properties/items/纯字符串 enum → 结构推断（与 Cookie
       通道 _cookie_ui_schema 同口径）；
    4. items 为数组（draft-07 元组形式）→ anyOf 联合（Gemini items 是单
       Schema）；items:true → 全类型联合。
    """
    budget.visit()
    if node is True:
        return _any_value_schema()
    if not isinstance(node, dict):
        return node
    if depth > MAX_SANITIZE_DEPTH:
        raise SchemaValidationError("Schema 转换超过最大深度。")

    raw_type = node.get("type")
    nullable = False
    if isinstance(raw_type, list):
        if not raw_type or any(not isinstance(t, str) or t.lower() not in _JSON_SCHEMA_TYPES for t in raw_type):
            raise SchemaValidationError("Schema.type 包含不支持的类型。")
        raw_type = [t.lower() for t in raw_type]
    if isinstance(raw_type, list) and len(set(raw_type) - {"null"}) > 1:
        return {"anyOf": [_sanitize_node({**node, "type": name}, depth + 1, budget)
                          for name in dict.fromkeys(raw_type)]}
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
            description = node.get("description") if isinstance(node.get("description"), str) else None
            budget.charge(description)
            return _any_value_schema(description)

    out: dict[str, Any] = {}
    if raw_type is not None:
        out["type"] = raw_type
    if nullable or node.get("nullable") is True:
        out["nullable"] = True

    for key, value in node.items():
        budget.charge(key)
        if key in ("type", "nullable"):
            continue
        if key == "properties" and isinstance(value, dict):
            out["properties"] = {}
            for name, child in value.items():
                budget.charge(name)
                out["properties"][name] = _sanitize_node(child, depth + 1, budget)
        elif key == "items":
            if isinstance(value, list):
                out["items"] = {"anyOf": [_sanitize_node(member, depth + 1, budget) for member in value]}
            elif value is True:
                out["items"] = _any_value_schema()
            elif isinstance(value, dict):
                out["items"] = _sanitize_node(value, depth + 1, budget)
            elif value is False or value is None:
                continue
            else:
                out["items"] = value
        elif key in ("anyOf", "oneOf") and isinstance(value, list):
            out[key] = [_sanitize_node(member, depth + 1, budget) for member in value]
        else:
            budget.charge(value)
            out[key] = value
    return out


def _resolve_schema_references(schema: dict) -> dict:
    """Resolve local JSON pointers with bounded expansion; never discard constraints."""
    budget = _ExpansionBudget()

    def expand(node, depth, refs):
        budget.visit()
        if depth > MAX_SCHEMA_DEPTH:
            raise SchemaValidationError("Schema 引用展开超过深度或节点数上限。")
        if not isinstance(node, dict):
            return node
        if "allOf" in node:
            raise SchemaValidationError("Gemini Schema 暂不支持 allOf，请合并约束后再提交。")
        if "$ref" in node:
            ref = node["$ref"]
            if not isinstance(ref, str) or not ref.startswith("#/"):
                raise SchemaValidationError("Schema 仅支持本地 JSON Pointer 引用（#/...）。")
            if ref in refs:
                raise SchemaValidationError("Gemini Schema 暂不支持递归 $ref。")
            siblings = set(node) - {"$ref", "$defs", "definitions", "$schema", "description", "title", "default", "example"}
            if siblings:
                raise SchemaValidationError("$ref 的同级约束暂不支持，请将约束放入引用定义。")
            target = schema
            try:
                for token in ref[2:].split("/"):
                    token = token.replace("~1", "/").replace("~0", "~")
                    target = target[token] if isinstance(target, dict) else target[int(token)]
            except (KeyError, IndexError, TypeError, ValueError):
                raise SchemaValidationError(f"Schema 引用不存在：{ref}") from None
            if not isinstance(target, dict):
                raise SchemaValidationError(f"Schema 引用目标必须是对象：{ref}")
            resolved = expand(target, depth + 1, refs | {ref})
            for key in {"description", "title", "default", "example"} & node.keys():
                budget.charge(key)
                budget.charge(node[key])
                resolved[key] = node[key]
            return resolved
        out = {}
        for key, value in node.items():
            budget.charge(key)
            if key in {"$defs", "definitions", "$schema"}:
                continue
            if key in _SCHEMA_MAP_KEYS and isinstance(value, dict):
                out[key] = {}
                for name, child in value.items():
                    budget.charge(name)
                    out[key][name] = expand(child, depth + 1, refs)
            elif key in _SCHEMA_SINGLE_KEYS and isinstance(value, dict):
                out[key] = expand(value, depth + 1, refs)
            elif (key in _SCHEMA_LIST_KEYS or key == "items") and isinstance(value, list):
                out[key] = [expand(child, depth + 1, refs) for child in value]
            else:
                budget.charge(value)
                out[key] = value
        return out

    return expand(schema, 0, set())


def sanitize_for_gemini(schema: Any) -> Any:
    """Convert OpenAI schemas without mutating input or weakening refs/unions."""
    if isinstance(schema, dict):
        schema = _resolve_schema_references(schema)
    result = _sanitize_node(schema, 0, _ExpansionBudget())
    # Include structural overhead/expanded unconstrained nodes in the final cap.
    if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > MAX_SCHEMA_SERIALIZED_BYTES:
        raise SchemaValidationError("Schema 转换结果超过字节上限。")
    return result
