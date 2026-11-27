"""Gemini 原生格式 ⇄ Cookie 通道（batchGraphql）桥接。

Express 通道可以字节级透传原生 REST 请求；Cookie 通道的私有 GraphQL 协议
做不到——本模块做的是**协议级转码**：

  请求侧：Gemini 原生 JSON（contents/generationConfig/tools/safetySettings…）
          → batchGraphql variables（复用 cookie_request 的 UI Schema 转换与
          requestContext/querySignature 信封）。
  响应侧：extract_events 的事件流（text/thought/image/function_call/finish…）
          → 重组回 Gemini 原生形状（candidates.content.parts / finishReason /
          usageMetadata / promptFeedback.blockReason），支持 SSE、JSON 数组流
          与非流式 JSON，保留每个 part 的签名；流式保留原候选的并行调用分组。

传输层完全复用 Cookie 通道基建：SAPISIDHASH 认证、curl_cffi 伪装、代理链
故障转移、首回复窗口、429 退避重试、账号健康隔离。

与 OpenAI 兼容通道的差异（有意为之）：
- 不做控制台消息注入 / 预填充兼容 / 抗截断传输——原生请求自带完整语义；
- thinkingConfig / 采样参数以客户端原生字段为准（不注入默认值），但按模型
  档案规范化后发送：thinkingLevel 大小写/别名/非法档位折叠、thinkingBudget
  换算档位、废弃采样参数剥离——私有协议是 GraphQL 强类型校验，原样透传
  会被 $generationConfig 校验直接 400；
- countTokens / embedContent：上游无对应 GraphQL 操作，明确拒绝。

由 routes/gemini_native.py 在「无 Express Key 且有启用 Cookie 账号」时启用。
"""

import asyncio
import base64
import json
import re
import time
from contextlib import aclosing
from typing import Any, Callable, Optional

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

import config as app_config
import model_capabilities as mc
from json_encoding import LargeJSONResponse, estimate_json_chars, iter_json_bytes
from admission import GenerationCapacityError, limit_generation, run_with_image_limit
from channel_capabilities import CookieModelScopeError, cookie_model_id, cookie_model_scope
from cookie_auth import (
    STREAM_GENERATE_OPERATION_NAME,
    build_headers,
    get_stream_query_signature,
)
from credential_manager import credential_manager
from impersonate import open_upstream_client
from logger import stats
from proxy_failover import ProxyChain
from protojson import int32_value, standard_base64
from response_validation import IncompleteResponseError, PendingMetadataGate
from resumable import request_timeout_seconds
from retry_policy import advisory_fits, get_retry_settings, note_retry_deadline
from runtime_state import app_state
from schema_validation import (
    MAX_SCHEMA_DEPTH, MAX_SCHEMA_NODES, SchemaValidationError, native_definition_ref,
    sanitize_for_gemini, validate_json_schema, validate_schema_size,
)
from signature_store import SKIP_VALIDATOR_SENTINELS
from streaming import (
    GEMINI_HEARTBEAT_FRAME, HEARTBEAT, SSE_NO_BUFFER_HEADERS, StreamDeadlineExceeded,
    reads_before_deadline, with_heartbeats,
)
from tool_policy import has_search_suffix
from usage import merge_usage_metadata
from upstream_errors import google_status_detail
from upstreams.cookie_candidate_metadata import check_grounding_references, merge_candidate_metadata
from upstreams.cookie_response import RawSampler
from upstreams.cookie_transport import (
    _event_has_reply,
    cookie_retry_backoff_heartbeats,
    stream_events,
)
from upstreams.cookie_request import (
    COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE,
    NATIVE_GENERATION_KEYS,
    build_request_context,
    cookie_ui_schema,
    native_default_safety_settings,
)

# 生图 markdown（extract_events 的 image 事件载荷）→ inlineData 反解
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
# 图像模型响应预算（2026-11 P2-3）：4K 高细节 PNG 的 base64 单对象可达
# 53-93MB（实测记录见 cookie_response.CompleteTextObjectCursor 注释），
# 64MB 会把同一张经 /v1/chat/completions 能成功交付的 4K 图在 Gemini
# 原生端点（models/*:generateContent）误拒为 502。对齐 OpenAI 通道的
# 放宽语义，图像模型按 128MB 预算（覆盖实测上限 + 余量）。
IMAGE_RESPONSE_BYTES = 128 * 1024 * 1024

_IMAGE_MD_RE = re.compile(r"^!\[.*?\]\(data:([^;,]+);base64,([A-Za-z0-9+/=]+)\)$", re.S)


def _bridge_response_budget(model: str) -> int:
    """原生桥响应预算：图像模型按放宽后的 128MB，其余维持 64MB。"""
    try:
        is_image = bool(mc.get_profile(model)["is_image"])
    except Exception:
        is_image = False
    return IMAGE_RESPONSE_BYTES if is_image else MAX_RESPONSE_BYTES

# 原生请求里上游 GraphQL 无法承载、需要静默丢弃的字段（丢弃时打一条日志）
_DROPPED_NATIVE_KEYS = ("labels",)

# 原生 generationConfig 采样键（camelCase）→ 模型档案采样键（OpenAI 命名）。
# 用于按模型档案剥离官方已废弃 / 私有协议拒绝的采样参数（与 OpenAI 通道同口径）。
_NATIVE_SAMPLING_KEYS = {
    "temperature": "temperature", "topP": "top_p", "topK": "top_k",
    "candidateCount": "candidate_count", "seed": "seed",
    "stopSequences": "stop_sequences", "maxOutputTokens": "max_output_tokens",
}


def _native_thinking_budget(value: Any) -> int:
    """ProtoJSON int32 permits quoted numbers and integer-valued exponents, not bools."""
    integer = int32_value(value)
    if integer is not None and integer >= -1:
        return integer
    raise SchemaValidationError("thinkingBudget 必须是 -1、0 或 int32 范围内的正整数。")


def _normalize_thinking_config(gen: dict, profile: dict) -> None:
    """原生 thinkingConfig → 私有协议线格式（原地修改 gen）。

    官方 REST 接受小写 thinkingLevel（low/high…）与若干别名，而 batchGraphql 的
    google_cloud_aiplatform_ui_GenerationConfig_ThinkingConfig_ThinkingLevel 只认
    大写枚举，且档位集合因模型而异（3 Pro 无 MEDIUM、3.7+ 无 MINIMAL）——原样
    透传必 400（真机：gemini-3.5-flash-lite thinkingLevel 小写被拒）。
    归一化规则与 OpenAI 兼容通道（resolve_thinking）对齐：
    别名折叠（max→high / off→minimal）、非法档位就近折叠（_collapse_level）、
    Gemini 3.x 官方已废弃的 thinkingBudget 换算成档位（<=1024 LOW、<=8192
    MEDIUM、其余 HIGH，与 claude_protocol 同口径）。
    """
    tc = gen.get("thinkingConfig")
    if tc is None:
        return
    if not isinstance(tc, dict):
        print("⚠️ [Cookie 原生桥] thinkingConfig 非法（需对象），已丢弃。")
        gen.pop("thinkingConfig", None)
        return
    cleaned = dict(tc)
    level = cleaned.pop("thinkingLevel", None)
    budget = cleaned.pop("thinkingBudget", None)
    include = cleaned.get("includeThoughts")
    if include is not None and not isinstance(include, bool):
        raise SchemaValidationError("includeThoughts 必须是布尔值（true/false），或为 null。")
    if budget is not None:
        if level is not None:
            raise SchemaValidationError("不能同时指定 thinkingBudget 与 thinkingLevel。")
        budget = _native_thinking_budget(budget)
    kind = profile.get("thinking_kind")
    if kind is None:
        if budget is not None:
            raise SchemaValidationError("Cookie 通道未验证该模型的 thinkingBudget，请改用支持目标预算的通道。")
        print("⚠️ [Cookie 原生桥] 该模型不支持思考参数，已丢弃 thinkingConfig。")
        gen.pop("thinkingConfig", None)
        return

    levels = profile.get("thinking_levels") or {"low", "high"}
    if kind == "budget" and budget is not None:
        out = {"thinkingBudget": budget}
    else:
        if level is not None:
            raw = str(level).strip().lower()
            if raw in ("auto", "dynamic", ""):   # 动态思考 → 模型默认档
                lv = profile.get("default_level", "high")
            else:
                lv = mc._norm_effort(raw) or profile.get("default_level", "high")
            if lv in ("off", "none", "disabled"):
                lv = "minimal"
        elif budget is not None:
            if budget == 0:
                raise SchemaValidationError("此 Cookie 模型无法完全关闭思考（thinkingBudget=0）；"
                                            "请改用支持关闭思考的模型，或明确选择 thinkingLevel。")
            # -1 requests automatic behavior. Don't pin a fixed level to it.
            lv = None if budget == -1 else (
                "low" if budget <= 1024 else "medium" if budget <= 8192 else "high")
        else:
            lv = None
        out = {"thinkingLevel": mc._collapse_level(lv, levels).upper()} if lv else {}

    if include is not None:
        out["includeThoughts"] = include
    unknown = [k for k in cleaned if k != "includeThoughts"]
    if unknown:
        print(f"⚠️ [Cookie 原生桥] thinkingConfig 不支持的字段已丢弃：{sorted(unknown)}")
    if out:
        gen["thinkingConfig"] = out
    else:
        gen.pop("thinkingConfig", None)


def _google_error_body(code: int, message: str, status_detail: Optional[str] = None, *,
                       retry_after: Optional[str] = None) -> dict:
    """Google 风格错误体（原生 SDK 按 error.message 展示）。

    状态映射统一走 upstream_errors.google_status_detail——与 gemini_native
    的 _native_error、resumable 的 native 错误帧共用同一份映射，保证同一
    状态码在所有路径给出一致的 detail（2026-11 审查修复：此前此处内联的
    平行映射与共享版在 413/502/默认值上互相矛盾）。
    """
    error = {"code": code, "message": message, "status": status_detail or google_status_detail(code)}
    if retry_after is not None:
        # A committed SSE/array cannot change HTTP headers. ErrorInfo's string
        # metadata carries both delay-seconds and HTTP-date without conversion.
        error["details"] = [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                             "reason": "UPSTREAM_HTTP_ERROR", "domain": "upstream",
                             "metadata": {"retryAfter": retry_after}}]
    return {"error": error}


def _native_error(code: int, message: str, status_detail: Optional[str] = None) -> JSONResponse:
    return JSONResponse(status_code=code, content=_google_error_body(code, message, status_detail))


class NativeBridgeError(Exception):
    """桥接层终结性错误（携带 Google 风格错误码）。"""

    def __init__(self, code: int, message: str, retry_after: Optional[str] = None):
        self.retry_after = retry_after
        self.code = code if isinstance(code, int) and 400 <= code <= 599 else 502
        self.message = message
        super().__init__(message)


# ============================================================
# 请求侧：Gemini 原生 JSON → batchGraphql variables
# ============================================================
def _normalize_system_instruction(si: Any) -> Optional[dict]:
    """原生 systemInstruction 允许 string / {parts:[...]}，统一成后者。"""
    if si is None:
        return None
    if isinstance(si, str):
        return {"parts": [{"text": si}]} if si else None
    if isinstance(si, dict):
        parts = si.get("parts")
        if isinstance(parts, list) and parts:
            return {"parts": parts}
    return None


_ProtoShape = dict | tuple | Callable[[Any], Any] | None


def _proto_shape(leaves: str = "", **children: _ProtoShape) -> dict:
    """Both ProtoJSON spellings point to one canonical field and a typed child, if any."""
    shape = {}
    for snake, child in {**dict.fromkeys(leaves.split()), **children}.items():
        first, *tail = snake.split("_")
        camel = first + "".join(part.title() for part in tail)
        shape[snake] = shape[camel] = (camel, child)
    return shape


def _proto_message(obj: Any, shape: _ProtoShape) -> Any:
    """Copy on change. Recurse only through declared message/repeated-message edges."""
    if shape is None:
        return obj
    if callable(shape):
        return shape(obj)
    if isinstance(shape, tuple):
        if not isinstance(obj, list):
            return obj
        result = obj
        for index, value in enumerate(obj):
            child = _proto_message(value, shape[0])
            if child is not value:
                if result is obj:
                    result = obj.copy()
                result[index] = child
        return result
    if not isinstance(obj, dict):
        return obj
    result = obj
    for key, value in obj.items():
        if key not in shape:
            continue
        camel, nested = shape[key]
        if key != camel and camel in obj:
            raise SchemaValidationError(f"不能同时声明 {camel} 与 {key}。")
        child = _proto_message(value, nested)
        if key != camel or child is not value:
            if result is obj:
                result = obj.copy()
            if key != camel:
                del result[key]
            result[camel] = child
    return result


_GRAPHQL_SIGNATURE_SENTINELS = {
    literal.decode("ascii"): base64.b64encode(literal).decode("ascii") for literal in SKIP_VALIDATOR_SENTINELS
}
_MAX_SENTINEL_CHARS = max(map(len, _GRAPHQL_SIGNATURE_SENTINELS))


def _graphql_bytes(value: Any) -> Any:
    # ProtoJSON accepts both alphabets and optional padding. The private
    # Bytes scalar expects standard Base64; leave invalid inputs to its own
    # validation rather than substituting different bytes.
    canonical = standard_base64(value)
    return value if canonical is None else canonical


def _graphql_signature(value: Any) -> Any:
    if isinstance(value, str) and len(value) <= _MAX_SENTINEL_CHARS:
        if (encoded := _GRAPHQL_SIGNATURE_SENTINELS.get(value)) is not None:
            return encoded  # Explicit REST sentinel; not a model signature.
    return _graphql_bytes(value)


# Only protocol envelopes belong here. In particular args, response, schema
# properties, metadata and all free-form dictionaries keep their original keys/values.
_BLOB = _proto_shape("mime_type display_name", data=_graphql_bytes)
_FILE = _proto_shape("file_uri mime_type display_name")
_RESPONSE_PART = _proto_shape(inline_data=_BLOB, file_data=_FILE)
_PARTIAL_ARG = _proto_shape("json_path bool_value null_value number_value string_value will_continue")
_PART = _proto_shape(
    "part_metadata code_execution_result executable_code speech_metadata media_processing",
    thought_signature=_graphql_signature,
    inline_data=_BLOB, file_data=_FILE,
    media_resolution=_proto_shape("num_tokens"), video_metadata=_proto_shape("start_offset end_offset"),
    audio_transcription=_proto_shape("language_code speaker_label start_offset end_offset",
                                     words=(_proto_shape("start_offset end_offset"),)),
    function_call=_proto_shape("will_continue", partial_args=(_PARTIAL_ARG,)),
    function_response=_proto_shape("will_continue", parts=(_RESPONSE_PART,)),
    tool_call=_proto_shape("tool_type"), tool_response=_proto_shape("tool_type"),
)
_CONTENT = _proto_shape(parts=(_PART,))
_GENERATION_CONFIG = _proto_shape(
    "top_p top_k max_output_tokens candidate_count stop_sequences response_mime_type response_schema "
    "response_json_schema response_modalities media_resolution response_logprobs presence_penalty "
    "frequency_penalty audio_timestamp",
    thinking_config=_proto_shape("include_thoughts thinking_level thinking_budget"),
    image_config=_proto_shape(
        "image_size aspect_ratio output_mime_type output_compression_quality person_generation prominent_people",
        image_output_options=_proto_shape("compression_quality mime_type")),
)
_REQUEST_FIELDS = _proto_shape(
    "cached_content safety_settings", contents=(_CONTENT,), system_instruction=_CONTENT,
    generation_config=_GENERATION_CONFIG,
    tool_config=_proto_shape(
        "include_server_side_tool_invocations", retrieval_config=_proto_shape("language_code lat_lng"),
        function_calling_config=_proto_shape("allowed_function_names stream_function_call_arguments")),
)
_DECLARATION_FIELDS = _proto_shape("parameters_json_schema response_json_schema")
_TOOL_FIELDS = _proto_shape("google_search function_declarations")

_TYPED_SCHEMA_FIELDS = _proto_shape(
    "additional_properties any_of max_items min_items max_length min_length "
    "max_properties min_properties property_ordering")


def _normalize_typed_schema(schema: dict) -> dict:
    """Normalize only public Schema messages; names and literal values are application data."""
    remaining = MAX_SCHEMA_NODES
    has_typed_refs = False

    def visit(node: Any, depth: int) -> Any:
        nonlocal remaining, has_typed_refs
        remaining -= 1
        if depth > MAX_SCHEMA_DEPTH or remaining < 0:
            raise SchemaValidationError("Schema 超过深度或节点数上限。")
        if not isinstance(node, dict):
            return node
        out = _proto_message(node, _TYPED_SCHEMA_FIELDS)
        if "ref" in out:
            if "$ref" in out:
                raise SchemaValidationError("原生 Schema 不能同时声明 ref 与 $ref。")
            if out is node:
                out = node.copy()
            reference = out.pop("ref")
            if reference is not None:
                has_typed_refs = True
                out["$ref"] = native_definition_ref(reference)
        for key in ("items", "additionalProperties", "anyOf", "properties", "defs"):
            value = out.get(key)
            if key in ("items", "additionalProperties") and isinstance(value, dict):
                updated = visit(value, depth + 1)
            elif ((key == "anyOf" and isinstance(value, list))
                  or (key in ("properties", "defs") and isinstance(value, dict))):
                if len(value) > remaining:  # avoid copying a map already known to exceed the budget
                    raise SchemaValidationError("Schema 超过节点数上限。")
                updated = value
                entries = enumerate(value) if isinstance(value, list) else value.items()
                for name, child in entries:
                    canonical = visit(child, depth + 1)
                    if canonical is not child:
                        if updated is value:
                            updated = value.copy()
                        updated[name] = canonical
            else:
                continue
            if updated is not value:
                if out is node:
                    out = node.copy()
                out[key] = updated
        return out

    result = visit(schema, 0)
    if has_typed_refs:
        validate_schema_size(schema)  # Includes definitions pruned after reference expansion.
        if "defs" not in result or not isinstance(result["defs"], dict) or "$defs" in result:
            raise SchemaValidationError("原生 Schema.ref 需要根 defs 定义表，不能混用 $defs。")
        result = {**result, "$defs": result["defs"]}
        del result["defs"]
    return result


def _native_declaration(decl: Any) -> dict:
    if not isinstance(decl, dict) or not isinstance(decl.get("name"), str) or not decl["name"].strip():
        raise SchemaValidationError("functionDeclarations 的每个函数必须包含非空名称。")
    decl = _proto_message(decl, _DECLARATION_FIELDS)
    if decl.get("response") is not None and decl.get("responseJsonSchema") is not None:
        raise SchemaValidationError("函数不能同时声明 response 与 responseJsonSchema。")
    if decl.get("response") is not None or decl.get("responseJsonSchema") is not None:
        raise SchemaValidationError("Cookie 通道无法保留函数的 response / responseJsonSchema，请改用 Express 通道。")
    if decl.get("behavior") is not None:
        raise SchemaValidationError("Cookie 通道无法保留函数的 behavior，请改用 Express 通道。")
    converted = {"name": decl["name"]}
    if decl.get("description") is not None:
        converted["description"] = decl["description"]
    parameters = decl.get("parameters")
    json_schema = decl.get("parametersJsonSchema")
    if parameters is not None and json_schema is not None:
        raise SchemaValidationError("函数不能同时声明 parameters 与 parametersJsonSchema。")
    if json_schema is not None:
        validate_json_schema(json_schema, label=f"{decl['name']}.parametersJsonSchema")
        parameters = sanitize_for_gemini(json_schema)
    elif parameters is not None:
        if not isinstance(parameters, dict):
            raise SchemaValidationError("函数 parameters 必须是 JSON 对象。")
        parameters = _normalize_typed_schema(parameters)
    converted["parameters"] = cookie_ui_schema(parameters if parameters is not None else {"type": "object"})
    return converted


def _native_tools_to_variables(tools: Any) -> tuple[list[dict], bool, list[str]]:
    """Convert supported tools; reject a tool the Cookie bridge cannot represent."""
    declarations: list[dict] = []
    use_search = False
    if tools is not None and not isinstance(tools, list):
        raise SchemaValidationError("tools 必须是工具数组。")
    for tool in tools or []:
        if not isinstance(tool, dict):
            raise SchemaValidationError("tools 的每一项必须是 JSON 对象。")
        tool = _proto_message(tool, _TOOL_FIELDS)
        for key, value in tool.items():
            if value is not None and key not in ("googleSearch", "functionDeclarations"):
                raise SchemaValidationError(f"Cookie 通道暂不支持工具 {key[:64]}，请改用 Express 通道。")
        search = tool.get("googleSearch")
        if search is not None:
            if search != {}:
                raise SchemaValidationError("Cookie 通道仅支持空配置 googleSearch，请改用 Express 通道。")
            use_search = True
        functions = tool.get("functionDeclarations")
        if functions is not None:
            if not isinstance(functions, list):
                raise SchemaValidationError("functionDeclarations 必须是函数数组。")
            declarations.extend(_native_declaration(decl) for decl in functions)
    return declarations, use_search, [d["name"] for d in declarations]


def _sanitize_tool_config(tool_config: Any, declared_names: list[str]) -> Optional[dict]:
    """原生 toolConfig 透传，仅把 allowedFunctionNames 收窄到已声明函数
    （内建搜索没有 functionDeclaration 名字，写进去会让 wire config 自非法）。"""
    if not isinstance(tool_config, dict):
        if tool_config is not None:
            raise SchemaValidationError("toolConfig 必须是 JSON 对象。")
        return None
    # These public controls aren't part of the Cookie request contract. Do not
    # drop an explicit search restriction (or pass it through for mixed tools).
    cleaned = dict(tool_config)
    retrieval = cleaned.pop("retrievalConfig", None)
    if retrieval is not None and (not isinstance(retrieval, dict) or any(
            value is not None for value in retrieval.values())):
        raise SchemaValidationError("Cookie 通道不支持 toolConfig.retrievalConfig（搜索语言或位置），请改用 Express 通道。")
    invocations = cleaned.pop("includeServerSideToolInvocations", None)
    if invocations is not None and invocations is not False:
        raise SchemaValidationError("Cookie 通道不支持 toolConfig.includeServerSideToolInvocations，"
                                    "需要服务端工具调用记录时请改用 Express 通道。")
    config = cleaned.get("functionCallingConfig")
    if not isinstance(config, dict):
        if config is not None:
            raise SchemaValidationError("toolConfig.functionCallingConfig 必须是 JSON 对象。")
        return cleaned or None
    fcc = dict(config)
    allowed = fcc.get("allowedFunctionNames")
    if allowed is not None and (
        not isinstance(allowed, list)
        or any(not isinstance(name, str) or not name.strip() for name in allowed)
    ):
        raise SchemaValidationError("toolConfig.functionCallingConfig.allowedFunctionNames 必须是非空名称的字符串数组。")
    if isinstance(allowed, list):
        kept = [n for n in allowed if n in declared_names]
        if allowed and not kept:
            # Dropping the only requested names would turn ANY into a request
            # for any other function, changing the client's explicit choice.
            raise SchemaValidationError("toolConfig.allowedFunctionNames 没有任何已声明的函数。")
        if kept:
            fcc["allowedFunctionNames"] = kept
        else:
            fcc.pop("allowedFunctionNames", None)
        if set(allowed) != set(kept):
            print("⚠️ [Cookie 原生桥] toolConfig.allowedFunctionNames 含未声明函数，已收窄到已声明集合。")
    cleaned["functionCallingConfig"] = fcc
    return cleaned


def _require_no_continuation(parsed: dict) -> None:
    # This protocol's model cursor is distinct from the gateway's SSE receipt.
    # A new batchGraphql call cannot continue the supplied provider response.
    if parsed.get("continuationToken") is not None or parsed.get("continuation_token") is not None:
        raise SchemaValidationError("Cookie 通道不支持原生 continuationToken；请使用可原样传输此字段的通道。")


def native_request_to_variables(parsed: dict, model_name: str, project_id: str,
                                *, requested_model: str = "") -> dict:
    """Gemini 原生请求体 → batchGraphql variables（未包信封）。

    raises SchemaValidationError：结构化输出被策略拒绝（与 OpenAI 通道同口径）。
    """
    model_name = cookie_model_id(model_name, project_id)
    _require_no_continuation(parsed)
    parsed = _proto_message(parsed, _REQUEST_FIELDS)
    if parsed.get("cachedContent") is not None:
        raise SchemaValidationError("Cookie 通道无法保留 cachedContent 上下文，请改用 Express 通道。")
    profile = mc.apply_sampling_policy(
        mc.get_profile(model_name), app_state.get_effective_settings(model_name)
    )

    gen = parsed.get("generationConfig")
    gen = dict(gen) if isinstance(gen, dict) else {}
    # Null on these declared ProtoJSON fields means absent. Preserve nulls in
    # schemas and other application-owned data; alias conflicts were already checked.
    for key in ("candidateCount", "responseSchema", "responseJsonSchema", "responseLogprobs", "logprobs"):
        if gen.get(key) is None:
            gen.pop(key, None)
    has_json_schema = "responseJsonSchema" in gen
    if has_json_schema:
        if gen.get("responseSchema") is not None:
            raise SchemaValidationError("不能同时声明 responseSchema 与 responseJsonSchema。")
        schema = gen.pop("responseJsonSchema")
        validate_json_schema(schema, label="generationConfig.responseJsonSchema")
        gen["responseSchema"] = sanitize_for_gemini(schema)
    if "candidateCount" in gen:
        if int32_value(gen["candidateCount"]) != 1:
            raise SchemaValidationError("Cookie 通道仅支持整数 candidateCount = 1。")
        gen["candidateCount"] = 1
    if gen.get("responseLogprobs") or "logprobs" in gen:
        raise SchemaValidationError("Cookie 通道暂不支持 responseLogprobs / logprobs。")
    unknown_gen = [k for k in gen if k not in NATIVE_GENERATION_KEYS]
    if unknown_gen:
        print(f"⚠️ [Cookie 原生桥] generationConfig 未经验证的字段将原样透传：{unknown_gen}")
    # 采样参数与 thinkingConfig 按模型档案规范化（不注入默认值，只剥离/换算
    # 必败字段）：batchGraphql 是 GraphQL 强类型校验，透传官方已废弃或该模型
    # 不接受的字段会直接 400（Invalid value for variable $generationConfig）。
    allowed = profile.get("allowed_sampling", set())
    for native_key, prof_key in _NATIVE_SAMPLING_KEYS.items():
        if native_key in gen and prof_key not in allowed:
            print(f"⚠️ [Cookie 原生桥] 该模型不支持 {native_key}，已剥离。")
            gen.pop(native_key, None)
    _normalize_thinking_config(gen, profile)
    # Gemini's explicit default is ordinary text, not a request for the
    # private channel's opt-in output-format fields. Leave real schemas gated.
    if gen.get("responseMimeType") == "text/plain" and gen.get("responseSchema") is None:
        gen.pop("responseMimeType")
    if gen.get("responseMimeType") or gen.get("responseSchema") is not None:
        if not app_state.get_setting("cookie_structured_output", False):
            raise SchemaValidationError(COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE)
        schema = gen.get("responseSchema")
        if isinstance(schema, dict) and schema:
            if not has_json_schema:
                schema = _normalize_typed_schema(schema)
            gen["responseSchema"] = cookie_ui_schema(
                {k: v for k, v in schema.items() if k != "$schema"})
    if profile["is_image"] and not gen.get("responseModalities"):
        gen["responseModalities"] = ["TEXT", "IMAGE"]

    for key in _DROPPED_NATIVE_KEYS:
        if parsed.get(key) is not None:
            print(f"⚠️ [Cookie 原生桥] 上游 GraphQL 协议不支持 {key}，已丢弃。")

    variables: dict[str, Any] = {
        "contents": parsed.get("contents") or [],
        "model": f"projects/{project_id}/locations/global/publishers/google/models/{model_name}",
        "generationConfig": gen,
        "safetySettings": parsed.get("safetySettings") or native_default_safety_settings(),
    }

    system = _normalize_system_instruction(parsed.get("systemInstruction"))
    if system:
        variables["systemInstruction"] = system

    declarations, use_search, names = _native_tools_to_variables(parsed.get("tools"))
    tool_config = _sanitize_tool_config(parsed.get("toolConfig"), names)
    function_config = (tool_config or {}).get("functionCallingConfig")
    mode = str(function_config.get("mode") or "AUTO").upper() if isinstance(function_config, dict) else "AUTO"
    if mode == "NONE":
        # Gemini's NONE applies to function calling, not to built-in search.
        declarations = []
    if profile["is_image"]:
        if (mode not in ("AUTO", "MODE_UNSPECIFIED", "NONE")
                or (mode != "NONE" and function_config and function_config.get("allowedFunctionNames"))):
            raise SchemaValidationError("此生图通道无法保留 toolConfig.functionCallingConfig 的显式函数选择；"
                                        "请使用 AUTO/NONE 或支持函数调用的文本模型。")
        declarations = []
    if mode == "ANY" and not declarations:
        raise SchemaValidationError("Cookie 通道的函数调用模式 ANY 需要至少一个已声明的客户端函数。")
    if declarations and use_search:
        if has_search_suffix(requested_model or model_name) and mode in ("AUTO", "MODE_UNSPECIFIED"):
            declarations = []
            print("🔎 [Cookie 原生桥] 搜索后缀优先内建搜索，跳过可选自定义函数。")
        else:
            use_search = False
            print("⚠️ [Cookie 原生桥] batchGraphql 不支持函数与内建搜索混用；已保留自定义函数，忽略 googleSearch。")
    if declarations:
        variables["tools"] = [{"functionDeclarations": declarations}]
        if tool_config:
            variables["toolConfig"] = tool_config
    elif use_search and profile["supports_search"]:
        variables["tools"] = [{"googleSearch": {}}]
    return variables


def build_native_body(parsed: dict, model_name: str, project_id: str,
                      *, requested_model: str = "") -> dict:
    """variables + GraphQL 信封（requestContext / querySignature 均为热改感知）。"""
    return {
        "requestContext": build_request_context(project_id),
        "querySignature": get_stream_query_signature(),
        "operationName": STREAM_GENERATE_OPERATION_NAME,
        "variables": native_request_to_variables(parsed, model_name, project_id,
                                                 requested_model=requested_model),
    }


# ============================================================
# 响应侧：事件流 → Gemini 原生形状
# ============================================================
def _image_part(markdown: str) -> dict:
    """extract_events 的 image 事件是 markdown data URL，反解成 inlineData。"""
    match = _IMAGE_MD_RE.match((markdown or "").strip())
    if match:
        return {"inlineData": {"mimeType": match.group(1), "data": match.group(2)}}
    return {"text": markdown}


def _part_json_size(part: dict) -> int:
    """Measure the existing JSON wire format without copying giant string values.

    JSON string escaping is additive across chunks. Replace large values with
    empty strings in a small skeleton, then count their escaped UTF-8 contents
    in 64 KiB slices. Quotes, separators, and non-string values are still
    measured by the standard encoder, preserving the response budget exactly.
    """
    extra = 0
    chunk_chars = 64 * 1024

    def shrink(value: str):
        nonlocal extra
        if isinstance(value, str) and len(value) > chunk_chars:
            for start in range(0, len(value), chunk_chars):
                extra += len(json.dumps(value[start:start + chunk_chars], ensure_ascii=False).encode("utf-8")) - 2
            return ""
        if isinstance(value, dict):
            return {key: shrink(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [shrink(item) for item in value]
        return value

    skeleton = shrink(part)
    return len(json.dumps(skeleton, ensure_ascii=False).encode("utf-8")) + extra


class NativeAssembler:
    """把 cookie 通道事件流拼装成 Gemini 原生响应。

    流式：stream_event 返回应立即下发的一组 parts（无则 None）；terminal 返回
    收尾帧（finishReason/usageMetadata/safetyRatings）。
    非流式：collect 同样喂事件，payload 一次性出整包。
    """

    def __init__(self, model_version: str, *, retain_parts: bool = True,
                 max_response_bytes: int | None = None):
        self.model_version = model_version
        self.parts: list[dict] = []
        self.retain_parts = retain_parts
        # None = 构造时取模块常量（保持运行期 patch 模块常量的测试口子）。
        self.max_response_bytes = (
            MAX_RESPONSE_BYTES if max_response_bytes is None else max_response_bytes
        )
        self.part_count = 0
        # 累计响应字节（预算口径）与当前 parts 数组的字节（分帧口径）分开记：
        # 2026-11 P2-3 —— 流式路径旧写法在 retain_parts=False 时逐 part 重置
        # response_bytes，「响应预算」实际只约束单 part，与文档语义不符。
        self.response_bytes = 0
        self.last_parts_bytes = 0
        self.finish: Optional[str] = None
        self.usage: Optional[dict] = None
        self.safety: Any = None
        self.candidate_metadata: dict = {}
        self.metadata_bytes = 0
        self.prompt_feedback: Optional[dict] = None
        self.has_content = False

    # -- 事件喂入 --------------------------------------------------------
    def feed(self, kind: str, data: Any) -> dict | list[dict] | None:
        """处理一个事件，返回新增 part 或原候选的一组 parts。"""
        part: Optional[dict] = None
        if kind == "native_parts":
            self._record_parts(data)
            return data
        if kind == "native_part":
            part = data
        elif kind == "text":
            part = {"text": data}
        elif kind == "thought":
            part = {"text": data, "thought": True}
        elif kind == "image":
            part = _image_part(data)
        elif kind == "function_call":
            part = {"functionCall": {"name": data.get("name"), "args": data.get("args") or {}}}
            if data.get("id"):
                part["functionCall"]["id"] = data["id"]
            signature = data.get("thought_signature")
            if signature:
                part["thoughtSignature"] = signature
        elif kind == "thought_signature":
            # 独立签名事件：并入最近一个 part（无承载位置则丢弃）。
            value = data.get("value") if isinstance(data, dict) else None
            if value and self.parts:
                self.parts[-1].setdefault("thoughtSignature", value)
            return None
        elif kind == "finish":
            self.finish = data
        elif kind == "usage":
            self.usage = merge_usage_metadata(self.usage, data)
        elif kind == "safety":
            self.safety = data
        elif kind == "candidate_metadata":
            updated, size = merge_candidate_metadata(self.candidate_metadata, data)
            if self.response_bytes + size - self.metadata_bytes > self.max_response_bytes:
                raise NativeBridgeError(502, "上游响应超过允许的大小。")
            self.response_bytes += size - self.metadata_bytes
            self.candidate_metadata, self.metadata_bytes = updated, size
        elif kind == "blocked":
            self.prompt_feedback = data
        if part is not None:
            self._record_parts([part])
        return part

    def _record_parts(self, parts: list[dict]) -> None:
        if not parts:
            return
        # Validate the whole candidate before delivering any of its Parts.
        # Preserve the cumulative per-Part budget for both delivery modes.
        total = self.response_bytes
        for part in parts:
            total += _part_json_size(part)
            if total > self.max_response_bytes:
                raise NativeBridgeError(502, "上游响应超过允许的大小。")
        # Include the array brackets and two bytes per separator.
        self.last_parts_bytes = total - self.response_bytes + 2 * len(parts)
        self.response_bytes = total
        if self.retain_parts:
            self.parts.extend(parts)
        self.part_count += len(parts)
        self.has_content = True

    def stream_event(self, kind: str, data: Any) -> list[dict] | None:
        parts = self.feed(kind, data)
        return [parts] if isinstance(parts, dict) else parts

    # -- 收尾 ------------------------------------------------------------
    def _require_finish(self) -> str:
        if not self.finish:
            raise IncompleteResponseError("上游连接已结束，但回复缺少结束标记（finishReason）。")
        return self.finish

    def terminal(self) -> Optional[dict]:
        """流式收尾帧：无任何可交付内容时返回 None（由调用方决定报错文案）。"""
        if self.prompt_feedback and not self.has_content:
            payload: dict[str, Any] = {"promptFeedback": self.prompt_feedback}
            if self.usage:
                payload["usageMetadata"] = self.usage
            payload["modelVersion"] = self.model_version
            return payload
        candidate: dict[str, Any] = {"index": 0, "finishReason": self._require_finish()}
        if self.candidate_metadata:
            check_grounding_references(self.candidate_metadata, final=True)
            candidate.update(self.candidate_metadata)
        if self.safety:
            candidate["safetyRatings"] = self.safety
        payload = {"candidates": [candidate], "modelVersion": self.model_version}
        if self.usage:
            payload["usageMetadata"] = self.usage
        return payload

    def payload(self) -> dict:
        """非流式整包。"""
        if self.prompt_feedback and not self.has_content:
            result: dict[str, Any] = {"promptFeedback": self.prompt_feedback}
            if self.usage:
                result["usageMetadata"] = self.usage
            return result
        candidate: dict[str, Any] = {
            "content": {"role": "model", "parts": self.parts},
            "index": 0,
            "finishReason": self._require_finish(),
        }
        if self.candidate_metadata:
            check_grounding_references(self.candidate_metadata, final=True)
            candidate.update(self.candidate_metadata)
        if self.safety:
            candidate["safetyRatings"] = self.safety
        result = {"candidates": [candidate]}
        if self.usage:
            result["usageMetadata"] = self.usage
        return result


# ============================================================
# 事件泵：复用 cookie 通道生成器 + 429 退避重试（首个内容前）
# ============================================================
async def _events_with_retry(fastapi_request: Request, record: dict, body: dict,
                             model_name: str, *, deadline: Optional[float] = None):
    """yield (kind, data) 原始事件；"heartbeat" 用于退避等待期间的 SSE 保活。

    首个内容分片前的可重试错误（429/网络）按控制台预算整体重发；已有内容
    后的错误为终结性（无法安全重放）。鉴权类错误同步写入账号健康状态。
    """
    cookie_str = record["cookie"]
    account_id = record.get("id", "")
    client_kwargs = {
        "timeout": httpx.Timeout(connect=30.0, read=180.0, write=30.0, pool=10.0),
        "follow_redirects": False,
        "trust_env": False,
    }
    if app_config.SSL_CERT_FILE:
        client_kwargs["verify"] = app_config.SSL_CERT_FILE
    proxy_chain = ProxyChain(record)
    # 与 Express 通道同款出站可观测性（直连静默）。
    if proxy_chain.current:
        proxy_chain.log_current()
    client_kwargs["_proxy_chain"] = proxy_chain.bind_to_generation()

    retry_max, backoff_sec, jitter_sec = get_retry_settings()
    for attempt in range(retry_max + 1):
        if await fastapi_request.is_disconnected():
            raise NativeBridgeError(499, "客户端已断开连接。")
        if attempt:
            stats.add_retry()
        req_headers = build_headers(cookie_str)
        async with open_upstream_client(client_kwargs) as client:
            committed = False
            # The inner transport may flush a signed-empty or diagnostic prefix
            # before an error. Neither belongs to a later successful attempt.
            pending = PendingMetadataGate()
            retry_now: Any = None
            sampler = RawSampler()
            events = stream_events(
                client, req_headers, body, sampler=sampler, model=model_name, preserve_parts=True)
            try:
                async for item in events:
                    status, data = item
                    if status in ("text", "thought", "image", "function_call", "thought_signature", "native_part", "native_parts",
                                  "finish", "usage", "blocked", "safety", "candidate_metadata"):
                        if not committed and _event_has_reply(item):
                            committed = True
                            for prefix in pending.drain():
                                yield prefix
                                del prefix
                        if committed:
                            yield item
                        else:
                            pending.add(item, 64 + estimate_json_chars(data))
                    elif status == "cookie_error":
                        credential_manager.record_cookie_auth_failure(account_id, credential=record)
                        raise NativeBridgeError(
                            getattr(data, "status_code", None) or 403, str(data)[:1024],
                            getattr(data, "retry_after", None))
                    elif status in ("retryable_error", "fatal_error"):
                        hint = getattr(data, "retry_after", None)
                        if (not committed and status == "retryable_error" and attempt < retry_max
                                and advisory_fits(hint, fastapi_request, deadline=deadline)):
                            retry_now = data
                            break
                        default_code = 429 if status == "retryable_error" else 502
                        raise NativeBridgeError(
                            getattr(data, "status_code", None) or default_code, str(data)[:1024], hint)
                    del item, data
            finally:
                await events.aclose()

            if retry_now is None:
                # A complete response may have only signed-empty Parts or be
                # a refusal. Release the successful prefix in its original order.
                for item in pending.drain():
                    yield item
                return
            pending.drain()  # release the losing attempt before sleeping
            # 退避等待期间吐心跳保活（原生 SSE 消费方转成空行）；
            # 同一 Cookie 身份重试，Retry-After 有约束力（口径见
            # cookie_transport._cookie_backoff_delay）。
            async for _hb in cookie_retry_backoff_heartbeats(
                fastapi_request,
                retry_after=getattr(retry_now, "retry_after", None),
                backoff_sec=backoff_sec, attempt=attempt, jitter_sec=jitter_sec,
                error=str(retry_now), label="限流/拥堵", log_tag="[Cookie 原生桥]",
            ):
                yield "heartbeat", None
            if await fastapi_request.is_disconnected():
                raise NativeBridgeError(499, "客户端已断开连接。")
    raise NativeBridgeError(429, "上游持续限流，重试多次仍失败。")


# ============================================================
# 端点入口（由 routes/gemini_native.py 调用）
# ============================================================
async def _timed_events(request: Request, record: dict, body: dict, model: str, deadline: float):
    """A single wall-clock budget covers retries, setup and all response chunks."""
    deadline = note_retry_deadline(request, deadline)
    source = _events_with_retry(request, record, body, model, deadline=deadline)
    async with aclosing(with_heartbeats(source, request)) as events:
        try:
            async for item in reads_before_deadline(events, deadline):
                yield ("heartbeat", None) if item is HEARTBEAT else item
        except StreamDeadlineExceeded:
            raise NativeBridgeError(504, "请求超过总超时时间。") from None


async def handle_native_request(fastapi_request: Request, model: str, action: str,
                                parsed: dict, *, requested_model: str = "") -> Response:
    """Apply the same admission and lifetime rules as the Express adapter."""
    resume = (fastapi_request.headers.get("x-resumable", "").lower() in ("1", "true", "yes")
              or fastapi_request.headers.get("x-resume-id")
              or fastapi_request.headers.get("last-event-id", "").strip()
              or fastapi_request.query_params.get("resume_from", "").strip())
    if resume:
        return _native_error(400, "Cookie 原生通道暂不支持断点续传，请配置 Express API Key。", "INVALID_ARGUMENT")
    if fastapi_request.query_params.get("alt", "json") not in ("sse", "json"):
        return _native_error(400, "alt 仅支持 sse 或 json。", "INVALID_ARGUMENT")
    try:
        _require_no_continuation(parsed)
    except SchemaValidationError as exc:
        return _native_error(400, str(exc), "INVALID_ARGUMENT")
    try:
        return await _handle_native_request(fastapi_request, model, action, parsed,
                                            requested_model=requested_model,
                                            deadline=time.monotonic() + request_timeout_seconds())
    except GenerationCapacityError as exc:
        return _native_error(503, str(exc), "RESOURCE_EXHAUSTED")


@limit_generation
async def _handle_native_request(fastapi_request: Request, model: str, action: str,
                                 parsed: dict, *, requested_model: str, deadline: float):
    return await run_with_image_limit(
        model.rpartition("/")[2] if action in ("generateContent", "streamGenerateContent") else "",
        lambda: _handle_native_request_impl(fastapi_request, model, action, parsed,
                                           requested_model=requested_model, deadline=deadline),
    )


async def _handle_native_request_impl(fastapi_request: Request, model: str, action: str,
                                      parsed: dict, *, requested_model: str, deadline: float):
    if action not in ("generateContent", "streamGenerateContent"):
        return _native_error(400, f"Cookie 通道暂不支持 :{action}，请配置 Express API Key。", "INVALID_ARGUMENT")
    scope = cookie_model_scope(model)
    # Snapshot the existing native legacy default in case all enabled rows have
    # an empty project; only a string can name the same explicit destination.
    legacy = app_state.get_project_id() if scope is not None else ""
    legacy_project = legacy.strip() if isinstance(legacy, str) else ""
    try:
        record = credential_manager.get_cookie_record(scope=scope, legacy_project=legacy_project)
    except CookieModelScopeError as exc:
        return _native_error(400, str(exc), "INVALID_ARGUMENT")
    if record is None:
        return _native_error(403, "未配置任何启用的 Cookie 账号。", "PERMISSION_DENIED")
    project_id = record.get("project_id") or (legacy_project if scope is not None else app_state.get_project_id())
    project_id = (project_id or "").strip()
    if not project_id:
        return _native_error(400, "Cookie 账号未配置 Project ID，无法构造上游请求。")
    if build_headers(record.get("cookie", "")) is None:
        return _native_error(401, "Cookie 中未找到 SAPISID，无法计算认证头。")
    account_id = record.get("id", "")
    try:
        body = build_native_body(parsed, model, project_id, requested_model=requested_model)
    except SchemaValidationError as exc:
        return _native_error(400, str(exc), "INVALID_ARGUMENT")

    # After the builder has validated scope, use only the model ID for budget,
    # timeout and image policies. A project name is not a model capability.
    model = body["variables"]["model"].rpartition("/")[2]
    is_stream = action == "streamGenerateContent"
    use_sse = fastapi_request.query_params.get("alt") == "sse"
    print(f"🍪 [Cookie 原生桥] {requested_model or model}:{action} | "
          f"{'SSE' if use_sse and is_stream else 'JSON'} | 协议转码")

    if is_stream:
        async def stream():
            assembler = NativeAssembler(
                requested_model or model, retain_parts=False,
                max_response_bytes=_bridge_response_budget(model))
            first = True

            def encode(payload: dict, *, fragmented: bool=False):
                nonlocal first
                if use_sse:
                    prefix, suffix = b"data: ", b"\n\n"
                else:
                    prefix, suffix = (b"" if first else b","), b""
                    first = False
                if not fragmented:
                    # Keep smaller candidate groups as one text frame.
                    yield prefix.decode("ascii") + json.dumps(payload, ensure_ascii=False) + suffix.decode("ascii")
                    return
                yield prefix
                # These are byte fragments of ONE SSE event/array element.
                # Never turn an inlineData string into multiple model Parts.
                yield from iter_json_bytes(payload)
                yield suffix

            yield GEMINI_HEARTBEAT_FRAME if use_sse else "["
            try:
                async with aclosing(_timed_events(fastapi_request, record, body, model, deadline)) as events:
                    async for kind, data in events:
                        if kind == "heartbeat":
                            yield GEMINI_HEARTBEAT_FRAME if use_sse else " "
                            continue
                        parts = assembler.stream_event(kind, data)
                        if parts:
                            for chunk in encode({"candidates": [{"content": {"role": "model", "parts": parts}, "index": 0}]},
                                                fragmented=assembler.last_parts_bytes > 1024 * 1024):
                                yield chunk
                                del chunk
                        del parts, data
                for chunk in encode(assembler.terminal()):
                    yield chunk
                stats.add_success()
                credential_manager.record_cookie_success(account_id, credential=record)
                print(f"✅ [Cookie 原生桥] {model} | 流式完成 | {assembler.part_count} parts")
            except asyncio.CancelledError:
                raise
            except NativeBridgeError as exc:
                if exc.code == 499:
                    return
                stats.add_error()
                for chunk in encode(_google_error_body(exc.code, exc.message, retry_after=exc.retry_after)):
                    yield chunk
            except IncompleteResponseError as exc:
                stats.add_error()
                for chunk in encode(_google_error_body(502, str(exc), "UNAVAILABLE")):
                    yield chunk
            except Exception as exc:
                stats.add_error()
                for chunk in encode(_google_error_body(502, f"Cookie 通道桥接异常：{type(exc).__name__}: {exc}"[:1024],
                                                       "UNAVAILABLE")):
                    yield chunk
            if not use_sse:
                yield "]"

        return StreamingResponse(stream(), media_type="text/event-stream" if use_sse else "application/json",
                                 headers=SSE_NO_BUFFER_HEADERS)

    assembler = NativeAssembler(
        requested_model or model, max_response_bytes=_bridge_response_budget(model))
    try:
        async with aclosing(_timed_events(fastapi_request, record, body, model, deadline)) as events:
            async for kind, data in events:
                assembler.feed(kind, data)
        payload = assembler.payload()
    except NativeBridgeError as exc:
        # Non-streaming HTTP errors are counted by StatsMiddleware once.
        response = _native_error(exc.code, exc.message)
        if exc.retry_after is not None:
            response.headers["retry-after"] = exc.retry_after
        return response
    except Exception as exc:
        return _native_error(502, str(exc)[:1024], "UNAVAILABLE")
    stats.add_success()
    credential_manager.record_cookie_success(account_id, credential=record)
    print(f"✅ [Cookie 原生桥] {model} | 非流式完成 | {assembler.part_count} parts")
    response_class = LargeJSONResponse if mc.get_profile(model)["is_image"] else JSONResponse
    return response_class(content=payload)
