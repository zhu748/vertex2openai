"""Defensive validation for client-supplied JSON Schemas."""

import json
import re
from typing import Any, Literal
from urllib.parse import quote, unquote

SDKCarrier = Literal["response", "function"]


MAX_SCHEMA_DEPTH = 32
MAX_SCHEMA_NODES = 10_000
MAX_SCHEMA_SERIALIZED_BYTES = 1_000_000
MAX_SANITIZE_DEPTH = 64
_JSON_SCHEMA_TYPES = {"object", "array", "string", "integer", "number", "boolean", "null"}
_SCHEMA_MAP_KEYS = {"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
_SCHEMA_SINGLE_KEYS = {"items", "additionalProperties", "not", "contains", "propertyNames", "if", "then", "else"}
_SCHEMA_LIST_KEYS = {"oneOf", "anyOf", "allOf", "prefixItems"}
_METADATA_KEYS = {"title", "description", "default", "example", "examples", "$comment",
                  "deprecated", "readOnly", "writeOnly"}
# Neither the pinned SDK's typed FunctionDeclaration nor its response_schema
# carrier supports these JSON Schema assertions. In particular, oneOf is NOT
# anyOf: matching overlapping branches must fail with oneOf.
_UNSUPPORTED_ASSERTIONS = {
    "oneOf", "not", "prefixItems", "contains", "minContains", "maxContains",
    "patternProperties", "propertyNames", "if", "then", "else", "dependentSchemas",
    "dependentRequired", "dependencies", "unevaluatedItems", "unevaluatedProperties",
    "additionalItems", "uniqueItems", "multipleOf", "exclusiveMinimum", "exclusiveMaximum",
}

# 任意 JSON 值的 Gemini 严格方言等价物。OpenAI 方言用空 schema（{} / true）表达
# "anything"，但 Gemini 的 Schema proto 拒绝空消息（400 INVALID_ARGUMENT
# "missing field"）。此处用全类型联合展开；OBJECT 不带 properties、ARRAY 不带
# items 在 Gemini 里都是"无约束"（两字段均为 Optional），因此无需人工递归截断。
_GEMINI_ANY_TYPES = ("string", "number", "integer", "boolean", "null", "object", "array")


class SchemaValidationError(ValueError):
    """A client schema is malformed or exceeds defensive complexity limits."""

    code = 400


def validate_schema_size(schema: dict, *, label: str = "schema") -> None:
    try:
        serialized = json.dumps(schema, ensure_ascii=False, separators=(",", ":"),
                                check_circular=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise SchemaValidationError(f"{label} is not a valid JSON Schema object: {exc}") from None
    if len(serialized) > MAX_SCHEMA_SERIALIZED_BYTES:
        raise SchemaValidationError(
            f"{label} exceeds the {MAX_SCHEMA_SERIALIZED_BYTES}-byte serialized-size limit")


def validate_json_schema(schema: Any, *, label: str = "schema") -> None:
    if not isinstance(schema, dict):
        raise SchemaValidationError(f"{label} must be a JSON object")
    validate_schema_size(schema, label=label)
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


def validate_tool_strict(strict: Any, label: str) -> None:
    if strict is not None and not isinstance(strict, bool):
        raise SchemaValidationError(f"{label} must be a boolean")
    if strict is True:
        # The transports do not validate a complete result batch against the
        # original declaration before exposing its calls (including streaming).
        raise SchemaValidationError(f"{label}=true is unsupported: this bridge cannot guarantee strict "
                                    "tool arguments. If non-strict calls are acceptable, omit strict or set "
                                    "it to false and validate the arguments in your application.")


def validate_openai_tool_strictness(tools: Any) -> None:
    """Cheap capability check before channel selection/replay, even in mixed mode."""
    for i, tool in enumerate(tools or []):
        function = tool.get("function") if isinstance(tool, dict) else None
        if isinstance(function, dict) and "strict" in function:
            validate_tool_strict(function["strict"], f"tools[{i}].function.strict")


def _needs_schema_preflight(schema: dict) -> bool:
    """Cheap bounded preflight: visit only schema positions, never literal example/default data."""
    remaining = MAX_SCHEMA_NODES

    def scan(node, depth):
        nonlocal remaining
        if not isinstance(node, dict):
            return False
        remaining -= 1
        if remaining < 0 or depth > MAX_SCHEMA_DEPTH or "$ref" in node or isinstance(node.get("items"), list):
            return True  # Full validation owns limits, references and the exact error path.
        for key, value in node.items():
            if key in _SCHEMA_MAP_KEYS and isinstance(value, dict):
                children = value.values()
            elif key in _SCHEMA_LIST_KEYS and isinstance(value, list):
                children = value
            elif key in _SCHEMA_SINGLE_KEYS and isinstance(value, dict):
                children = (value,)
            else:
                continue
            if any(scan(child, depth + 1) for child in children):
                return True
        return False

    return scan(schema, 0)


def validate_tool_name_identity(name: Any, label: str) -> None:
    if isinstance(name, str) and name.strip() and name != name.strip():
        raise SchemaValidationError(f"{label} must not have leading or trailing whitespace")


def _validate_choice_name(request: Any) -> None:
    choice = getattr(request, "tool_choice", None)
    if isinstance(choice, dict) and isinstance(choice.get("function"), dict):
        validate_tool_name_identity(choice["function"].get("name"), "tool_choice.function.name")


def preflight_schema_contracts(request: Any) -> None:
    """Reject invalid references and positional items before replay or mixed credential selection."""
    def check(schema, label):
        if isinstance(schema, dict) and _needs_schema_preflight(schema):
            validate_json_schema(schema, label=label)
            sanitize_for_gemini(schema, label=label)

    _validate_choice_name(request)
    fmt = request.response_format or {}
    fmt_schema = fmt.get("json_schema")
    if fmt.get("type") == "json_schema" and isinstance(fmt_schema, dict):
        check(fmt_schema.get("schema"), "response_format.json_schema.schema")
    for i, tool in enumerate(request.tools or ()):
        fn = tool.get("function")
        if isinstance(fn, dict):
            validate_tool_name_identity(fn.get("name"), f"tools[{i}].function.name")
            check(fn.get("parameters"), f"tools[{i}].function.parameters")


def validate_request_schemas(request: Any, *, sdk: bool = False) -> None:
    _validate_choice_name(request)
    for index, tool in enumerate(getattr(request, "tools", None) or []):
        if not isinstance(tool, dict) or (tool.get("type") != "function" and "function" not in tool):
            continue
        function = tool.get("function")
        if not isinstance(function, dict):
            raise SchemaValidationError(f"tools[{index}].function must be an object")
        name = function.get("name")
        if not isinstance(name, str) or not name.strip():
            raise SchemaValidationError(f"tools[{index}].function.name must be a non-empty string")
        validate_tool_name_identity(name, f"tools[{index}].function.name")
        if any(ord(char) < 32 or ord(char) == 127 for char in name):
            raise SchemaValidationError(f"tools[{index}].function.name must not contain control characters")
        description = function.get("description")
        if description is not None and not isinstance(description, str):
            raise SchemaValidationError(f"tools[{index}].function.description must be a string")
        validate_tool_strict(function.get("strict"), f"tools[{index}].function.strict")
        parameters = function.get("parameters")
        if parameters is not None:
            validate_json_schema(parameters, label=f"tools[{index}].function.parameters")
            sanitize_for_gemini(parameters, label=f"tools[{index}].function.parameters",
                                sdk_carrier="function" if sdk else None)

    response_format = getattr(request, "response_format", None)
    if isinstance(response_format, dict) and response_format.get("type") == "json_schema":
        json_schema = response_format.get("json_schema")
        if not isinstance(json_schema, dict):
            raise SchemaValidationError("response_format.json_schema must be an object")
        schema = json_schema.get("schema")
        validate_json_schema(schema, label="response_format.json_schema.schema")
        sanitize_for_gemini(schema, label="response_format.json_schema.schema", sdk_carrier="response" if sdk else None)


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


def _normalize_positional_items(node: dict, path: str) -> dict:
    """Draft 7 tuple items constrain a prefix; unmentioned later slots are unrestricted."""
    items = node["items"]
    limit = node.get("maxItems")
    if (node.get("type") == "array" and items and type(limit) is int and 0 <= limit <= len(items)):
        if limit == 0:
            return {k: v for k, v in node.items() if k != "items"}
        # Only reachable slots matter. Compare JSON representations, not Python
        # equality (true == 1); reject if equality cannot be proved exactly.
        if limit == 1:
            return {**node, "items": items[0]}
        first = json.dumps(items[0], sort_keys=True)
        if all(json.dumps(items[i], sort_keys=True) == first for i in range(1, limit)):
            return {**node, "items": items[0]}
    raise SchemaValidationError(f"{path}.items: positional tuple items cannot be represented by this "
                                "schema carrier. Use items as a single schema only for a uniform list; "
                                "items.anyOf would not preserve positions or the unrestricted tail.")


def _normalize_assertions(node: dict, path: str) -> dict:
    """Only lower a constraint when the target has the same allowed values."""
    for key in node:
        if key in _UNSUPPORTED_ASSERTIONS:
            raise SchemaValidationError(f"{path}.{key}: Gemini cannot represent this JSON Schema assertion; "
                                        "rewrite the constraint before submitting.")
    if isinstance(node.get("items"), list):
        node = _normalize_positional_items(node, path)
    enum = node.get("enum")
    if "enum" in node and (not isinstance(enum, list) or not enum or not all(isinstance(x, str) for x in enum)):
        raise SchemaValidationError(f"{path}.enum: Gemini requires a non-empty array of strings; "
                                    "non-string enum values are unsupported.")
    if "const" in node:
        value = node["const"]
        raw_type = node.get("type")
        types = raw_type if isinstance(raw_type, list) else [raw_type]
        if (not isinstance(value, str)
                or raw_type is not None and not any(isinstance(t, str) and t.lower() == "string" for t in types)
                or enum is not None and value not in enum
                or "anyOf" in node or node.get("nullable") is True):
            raise SchemaValidationError(f"{path}.const: use a compatible string literal or a string enum; "
                                        "this combination cannot be represented exactly.")
        # Do not overwrite a contradictory enum or allow a null branch. A
        # singleton enum is understood by both carriers (raw const is not).
        node = {k: v for k, v in node.items() if k != "const"}
        node.update(type="string", enum=[value])
    if node.get("items") is False:
        # Without type:array, JSON Schema also accepts every non-array value;
        # with prefixItems, the prefix may be nonempty. Never reduce either
        # case to just the empty array. Reject malformed/unsatisfiable bounds.
        raw_type = node.get("type")
        minimum, maximum = node.get("minItems", 0), node.get("maxItems", 0)
        if (not isinstance(raw_type, str) or raw_type.lower() != "array"
                or type(minimum) is not int or minimum != 0 or type(maximum) is not int or maximum < 0):
            raise SchemaValidationError(f"{path}.items: items:false is supported only for an explicit array "
                                        "that permits the empty array; use maxItems:0 if that is intended.")
        node = {k: v for k, v in node.items() if k != "items"}
        node["maxItems"] = 0
    return node


def _sdk_examples(node: dict, path: str) -> dict:
    """The typed Vertex Schema has a single example field, never an examples list."""
    examples = node["examples"]
    if not isinstance(examples, list):
        raise SchemaValidationError(f"{path}.examples must be an array")
    if len(examples) > 1 or (examples and (examples[0] is None or "example" in node)):
        raise SchemaValidationError(f"{path}.examples: the Express typed schema supports one non-null example. "
                                    "Choose one non-null example or omit the annotation.")
    node = {k: v for k, v in node.items() if k != "examples"}
    if examples:
        node["example"] = examples[0]
    return node


def _sanitize_node(node: Any, depth: int, budget: _ExpansionBudget, path: str,
                   sdk_carrier: SDKCarrier | None) -> Any:
    """Produce a new Gemini Schema tree; never mistake an assertion for metadata."""
    budget.visit()
    if node is True:
        return _any_value_schema()
    if not isinstance(node, dict):
        return node
    if depth > MAX_SANITIZE_DEPTH:
        raise SchemaValidationError("Schema 转换超过最大深度。")

    node = _normalize_assertions(node, path)
    if sdk_carrier and "examples" in node:
        node = _sdk_examples(node, path)
    raw_type = node.get("type")
    nullable = False
    if isinstance(raw_type, list):
        if not raw_type or any(not isinstance(t, str) or t.lower() not in _JSON_SCHEMA_TYPES for t in raw_type):
            raise SchemaValidationError("Schema.type 包含不支持的类型。")
        raw_type = [t.lower() for t in raw_type]
        # A string enum intersects the type list; null cannot satisfy it.
        if node.get("enum"):
            if "string" not in raw_type:
                raise SchemaValidationError(f"{path}.enum: string values cannot satisfy the declared type list.")
            raw_type = "string"
    if isinstance(raw_type, list) and len(set(raw_type) - {"null"}) > 1:
        return {"anyOf": [_sanitize_node({**node, "type": name}, depth + 1, budget, path, sdk_carrier)
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
        elif node.keys() <= _METADATA_KEYS:
            # Only annotations (or {}) can represent an arbitrary JSON value.
            description = node.get("description") if isinstance(node.get("description"), str) else None
            budget.charge(description)
            result = _any_value_schema(description)
            for key in ("example", "examples"):
                if key in node:
                    budget.charge(node[key])
                    result[key] = node[key]
            return result
        else:
            key = next(k for k in node if k not in _METADATA_KEYS)
            raise SchemaValidationError(f"{path}.{key}: cannot infer a Gemini type without changing the schema; "
                                        "declare an explicit compatible type.")

    out: dict[str, Any] = {}
    if raw_type is not None:
        # The response transformer rewrites lowercase JSON "null" to an
        # unconstrained nullable object (and crashes for annotated branches).
        # Use the SDK's public Type.NULL enum spelling to encode literal null.
        out["type"] = "NULL" if sdk_carrier == "response" and raw_type == "null" else raw_type
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
                out["properties"][name] = _sanitize_node(child, depth + 1, budget, f"{path}.properties.{name}", sdk_carrier)
        elif key == "items":
            if value is True:
                out["items"] = _any_value_schema()
            elif isinstance(value, dict):
                out["items"] = _sanitize_node(value, depth + 1, budget, f"{path}.items", sdk_carrier)
            elif value is None:
                raise SchemaValidationError(f"{path}.items must be a schema or boolean")
            else:
                out["items"] = value
        elif key == "additionalProperties" and isinstance(value, dict):
            out[key] = _sanitize_node(value, depth + 1, budget, f"{path}.{key}", sdk_carrier)
        elif key == "anyOf" and isinstance(value, list):
            out[key] = [_sanitize_node(member, depth + 1, budget, f"{path}.{key}[{i}]", sdk_carrier)
                        for i, member in enumerate(value)]
        else:
            budget.charge(value)
            out[key] = value
    return out


# RFC 6901 §§4,6: decode the URI fragment first, then interpret pointer tokens.
_BAD_PERCENT = re.compile(r"%(?![0-9a-fA-F]{2})")
_BAD_TILDE = re.compile(r"~(?![01])")
_ARRAY_INDEX = re.compile(r"0|[1-9][0-9]*")


def _local_pointer(ref: Any) -> str:
    if not isinstance(ref, str) or not ref.startswith("#"):
        raise SchemaValidationError("Schema 仅支持本地 JSON Pointer 引用（#/...）。")
    pointer = ref[1:]
    if "%" in pointer:
        if _BAD_PERCENT.search(pointer):
            raise SchemaValidationError("$ref 包含无效的百分号编码。")
        try:
            pointer = unquote(pointer, errors="strict")
        except UnicodeDecodeError:
            raise SchemaValidationError("$ref 的百分号编码必须是有效的 UTF-8。") from None
    if pointer and not pointer.startswith("/"):
        raise SchemaValidationError("Schema 仅支持本地 JSON Pointer 引用（#/...）。")
    return pointer


def native_definition_ref(ref: Any) -> str:
    """Typed Gemini ref names a child of root defs; preserve escaped property names exactly."""
    pointer = _local_pointer(ref)
    if not pointer.startswith("/defs/") or "/" in (name := pointer[6:]):
        raise SchemaValidationError("原生 Schema.ref 必须引用根 defs 中的定义（#/defs/名称）。")
    # Re-encode percent literals: the shared resolver decodes the resulting
    # fragment exactly once, then interprets ~1/~0 with its usual validation.
    try:
        return "#/$defs/" + quote(name, safe="~")
    except UnicodeError:
        raise SchemaValidationError("原生 Schema.ref 必须为有效 Unicode。") from None


def _local_ref_target(root: dict, ref: Any) -> dict:
    pointer = _local_pointer(ref)
    if not pointer:
        return root
    target = root
    for token in pointer[1:].split("/"):
        if "~" in token:
            if _BAD_TILDE.search(token):
                raise SchemaValidationError("$ref 的 JSON Pointer 仅支持 ~0 和 ~1 转义。")
            token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(target, dict) and token in target:
            target = target[token]
        elif (isinstance(target, list) and len(token) <= len(str(len(target)))
              and _ARRAY_INDEX.fullmatch(token) and (index := int(token)) < len(target)):
            target = target[index]
        else:
            raise SchemaValidationError(f"Schema 引用不存在或索引无效：{ref[:160]}")
    if not isinstance(target, dict):
        raise SchemaValidationError(f"Schema 引用目标必须是对象：{ref[:160]}")
    return target


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
            target = _local_ref_target(schema, node["$ref"])
            target_id = id(target)  # URI aliases can identify the same active schema.
            if target_id in refs:
                raise SchemaValidationError("Gemini Schema 暂不支持递归 $ref。")
            siblings = set(node) - {"$ref", "$defs", "definitions", "$schema", "description", "title", "default", "example"}
            if siblings:
                raise SchemaValidationError("$ref 的同级约束暂不支持，请将约束放入引用定义。")
            resolved = expand(target, depth + 1, refs | {target_id})
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


def sanitize_for_gemini(schema: Any, *, label: str = "schema", sdk_carrier: SDKCarrier | None = None) -> Any:
    """Convert OpenAI schemas without mutating input or weakening refs/unions."""
    if isinstance(schema, dict):
        schema = _resolve_schema_references(schema)
    result = _sanitize_node(schema, 0, _ExpansionBudget(), label, sdk_carrier)
    # Include structural overhead/expanded unconstrained nodes in the final cap.
    if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > MAX_SCHEMA_SERIALIZED_BYTES:
        raise SchemaValidationError("Schema 转换结果超过字节上限。")
    return result
