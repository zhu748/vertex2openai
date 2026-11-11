"""Gemini 原生格式 ⇄ Cookie 通道（batchGraphql）桥接。

Express 通道可以字节级透传原生 REST 请求；Cookie 通道的私有 GraphQL 协议
做不到——本模块做的是**协议级转码**：

  请求侧：Gemini 原生 JSON（contents/generationConfig/tools/safetySettings…）
          → batchGraphql variables（复用 cookie_request 的 UI Schema 转换与
          requestContext/querySignature 信封）。
  响应侧：extract_events 的事件流（text/thought/image/function_call/finish…）
          → 重组回 Gemini 原生形状（candidates.content.parts / finishReason /
          usageMetadata / promptFeedback.blockReason），支持 SSE、JSON 数组流
          与非流式 JSON，保留每个 part 的签名。

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
import json
import re
import time
from contextlib import aclosing
from typing import Any, Optional

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

import config as app_config
import model_capabilities as mc
from json_encoding import LargeJSONResponse, iter_json_bytes
from admission import GenerationCapacityError, limit_generation, run_with_image_limit
from cookie_auth import (
    STREAM_GENERATE_OPERATION_NAME,
    build_headers,
    get_stream_query_signature,
)
from credential_manager import credential_manager
from impersonate import open_upstream_client
from logger import stats
from proxy_failover import ProxyChain
from response_validation import IncompleteResponseError
from resumable import request_timeout_seconds
from retry_policy import get_retry_settings
from runtime_state import app_state
from schema_validation import SchemaValidationError, sanitize_for_gemini, validate_json_schema
from streaming import HEARTBEAT, HEARTBEAT_FRAME, SSE_NO_BUFFER_HEADERS, with_heartbeats
from tool_policy import has_search_suffix
from usage import merge_usage_metadata
from upstream_errors import google_status_detail
from upstreams.cookie_response import RawSampler
from upstreams.cookie_transport import (
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
_DROPPED_NATIVE_KEYS = ("cachedContent", "labels")

# 原生 generationConfig 采样键（camelCase）→ 模型档案采样键（OpenAI 命名）。
# 用于按模型档案剥离官方已废弃 / 私有协议拒绝的采样参数（与 OpenAI 通道同口径）。
_NATIVE_SAMPLING_KEYS = {
    "temperature": "temperature", "topP": "top_p", "topK": "top_k",
    "candidateCount": "candidate_count", "seed": "seed",
    "stopSequences": "stop_sequences", "maxOutputTokens": "max_output_tokens",
}


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
    if profile.get("thinking_kind") is None:
        print("⚠️ [Cookie 原生桥] 该模型不支持思考参数，已丢弃 thinkingConfig。")
        gen.pop("thinkingConfig", None)
        return

    levels = profile.get("thinking_levels") or {"low", "high"}
    cleaned = dict(tc)
    level = cleaned.pop("thinkingLevel", None)
    budget = cleaned.pop("thinkingBudget", None)
    include = cleaned.get("includeThoughts")

    if profile.get("thinking_kind") == "budget" and budget is not None:
        # 未来若出现 budget 型模型：透传整数，丢弃档位
        try:
            out = {"thinkingBudget": int(budget)}
        except (TypeError, ValueError):
            print(f"⚠️ [Cookie 原生桥] thinkingBudget 非法（{budget!r}），已丢弃。")
            out = {}
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
            try:
                b = int(budget)
            except (TypeError, ValueError):
                b = -1
            if b <= 0:
                lv = profile.get("default_level", "high")
            else:
                lv = "low" if b <= 1024 else ("medium" if b <= 8192 else "high")
            print(f"ℹ️ [Cookie 原生桥] thinkingBudget={budget} → 换算档位（私有协议仅接受 thinkingLevel）。")
        else:
            lv = None
        out = {"thinkingLevel": mc._collapse_level(lv, levels).upper()} if lv else {}

    if include is not None:
        out["includeThoughts"] = bool(include)
    unknown = [k for k in cleaned if k != "includeThoughts"]
    if unknown:
        print(f"⚠️ [Cookie 原生桥] thinkingConfig 不支持的字段已丢弃：{sorted(unknown)}")
    if out:
        gen["thinkingConfig"] = out
    else:
        gen.pop("thinkingConfig", None)


def _google_error_body(code: int, message: str, status_detail: Optional[str] = None) -> dict:
    """Google 风格错误体（原生 SDK 按 error.message 展示）。

    状态映射统一走 upstream_errors.google_status_detail——与 gemini_native
    的 _native_error、resumable 的 native 错误帧共用同一份映射，保证同一
    状态码在所有路径给出一致的 detail（2026-11 审查修复：此前此处内联的
    平行映射与共享版在 413/502/默认值上互相矛盾）。
    """
    return {"error": {"code": code, "message": message,
                     "status": status_detail or google_status_detail(code)}}


def _native_error(code: int, message: str, status_detail: Optional[str] = None) -> JSONResponse:
    return JSONResponse(status_code=code, content=_google_error_body(code, message, status_detail))


class NativeBridgeError(Exception):
    """桥接层终结性错误（携带 Google 风格错误码）。"""

    def __init__(self, code: int, message: str):
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


def _native_tools_to_variables(tools: Any) -> tuple[list[dict], bool, list[str]]:
    """原生 tools → (functionDeclarations 列表, 是否启用 googleSearch, 声明名)。

    工具冲突由调用方结合搜索后缀和 functionCallingConfig 决定，避免在
    解析阶段丢掉搜索意图。parameters 走 Studio 私有 UI Schema。
    """
    declarations: list[dict] = []
    use_search = False
    for tool in tools or []:
        if not isinstance(tool, dict):
            continue
        if "googleSearch" in tool or "google_search" in tool:
            use_search = True
            continue
        for decl in tool.get("functionDeclarations") or []:
            if not isinstance(decl, dict) or not str(decl.get("name") or "").strip():
                continue
            converted = {"name": str(decl["name"])}
            if decl.get("description") is not None:
                converted["description"] = decl["description"]
            parameters = decl.get("parameters")
            json_schema = decl.get("parametersJsonSchema")
            if parameters is not None and json_schema is not None:
                raise SchemaValidationError("函数不能同时声明 parameters 与 parametersJsonSchema。")
            if json_schema is not None:
                validate_json_schema(json_schema, label=f"{decl['name']}.parametersJsonSchema")
                parameters = sanitize_for_gemini(json_schema)
            elif parameters is not None and not isinstance(parameters, dict):
                raise SchemaValidationError("函数 parameters 必须是 JSON 对象。")
            converted["parameters"] = cookie_ui_schema(
                parameters if parameters is not None else {"type": "object"})
            declarations.append(converted)
    return declarations, use_search, [d["name"] for d in declarations]


def _sanitize_tool_config(tool_config: Any, declared_names: list[str]) -> Optional[dict]:
    """原生 toolConfig 透传，仅把 allowedFunctionNames 收窄到已声明函数
    （内建搜索没有 functionDeclaration 名字，写进去会让 wire config 自非法）。"""
    if not isinstance(tool_config, dict):
        if tool_config is not None:
            raise SchemaValidationError("toolConfig 必须是 JSON 对象。")
        return None
    config = tool_config.get("functionCallingConfig")
    if not isinstance(config, dict):
        if config is not None:
            raise SchemaValidationError("toolConfig.functionCallingConfig 必须是 JSON 对象。")
        return dict(tool_config) if tool_config else None
    cleaned = dict(tool_config)
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


def native_request_to_variables(parsed: dict, model_name: str, project_id: str,
                                *, requested_model: str = "") -> dict:
    """Gemini 原生请求体 → batchGraphql variables（未包信封）。

    raises SchemaValidationError：结构化输出被策略拒绝（与 OpenAI 通道同口径）。
    """
    profile = mc.apply_sampling_policy(
        mc.get_profile(model_name), app_state.get_effective_settings(model_name)
    )

    gen = parsed.get("generationConfig")
    gen = dict(gen) if isinstance(gen, dict) else {}
    if "responseJsonSchema" in gen:
        if gen.get("responseSchema") is not None:
            raise SchemaValidationError("不能同时声明 responseSchema 与 responseJsonSchema。")
        schema = gen.pop("responseJsonSchema")
        validate_json_schema(schema, label="generationConfig.responseJsonSchema")
        gen["responseSchema"] = sanitize_for_gemini(schema)
    if gen.get("candidateCount", 1) != 1:
        raise SchemaValidationError("Cookie 通道暂不支持 candidateCount != 1。")
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
    if gen.get("responseMimeType") or gen.get("responseSchema"):
        if not app_state.get_setting("cookie_structured_output", False):
            raise SchemaValidationError(COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE)
        schema = gen.get("responseSchema")
        if isinstance(schema, dict) and schema:
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
        declarations = []
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

    流式：stream_event 返回应立即下发的 part（无则 None）；terminal 返回
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
        # 累计响应字节（预算口径）与最后一个 part 的字节（分帧口径）分开记：
        # 2026-11 P2-3 —— 流式路径旧写法在 retain_parts=False 时逐 part 重置
        # response_bytes，「响应预算」实际只约束单 part，与文档语义不符。
        self.response_bytes = 0
        self.last_part_bytes = 0
        self.finish: Optional[str] = None
        self.usage: Optional[dict] = None
        self.safety: Any = None
        self.blocked: Optional[str] = None
        self.has_content = False

    # -- 事件喂入 --------------------------------------------------------
    def feed(self, kind: str, data: Any) -> Optional[dict]:
        """处理一个事件；返回需立即下发的新增 part（流式用），None 表示不发。"""
        part: Optional[dict] = None
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
        elif kind == "blocked":
            self.blocked = str(data).split("（")[0].split("(")[0].strip() or "BLOCKED"
        if part is not None:
            size = _part_json_size(part)
            # 累计语义（2026-11 P2-3）：流式/非流式同口径，预算名副其实；
            # 图像模型经 max_response_bytes 放宽（见 IMAGE_RESPONSE_BYTES）。
            self.response_bytes += size
            self.last_part_bytes = size
            if self.response_bytes > self.max_response_bytes:
                raise NativeBridgeError(502, "上游响应超过允许的大小。")
            if self.retain_parts:
                self.parts.append(part)
            self.part_count += 1
            self.has_content = True
        return part

    # 流式别名（语义清晰）
    def stream_event(self, kind: str, data: Any) -> Optional[dict]:
        return self.feed(kind, data)

    # -- 收尾 ------------------------------------------------------------
    def _require_finish(self) -> str:
        if not self.finish:
            raise IncompleteResponseError("上游连接已结束，但回复缺少结束标记（finishReason）。")
        return self.finish

    def terminal(self) -> Optional[dict]:
        """流式收尾帧：无任何可交付内容时返回 None（由调用方决定报错文案）。"""
        if self.blocked and not self.has_content:
            payload: dict[str, Any] = {"promptFeedback": {"blockReason": self.blocked}}
            if self.usage:
                payload["usageMetadata"] = self.usage
            payload["modelVersion"] = self.model_version
            return payload
        candidate: dict[str, Any] = {"index": 0, "finishReason": self._require_finish()}
        if self.safety:
            candidate["safetyRatings"] = self.safety
        payload = {"candidates": [candidate], "modelVersion": self.model_version}
        if self.usage:
            payload["usageMetadata"] = self.usage
        return payload

    def payload(self) -> dict:
        """非流式整包。"""
        if self.blocked and not self.has_content:
            result: dict[str, Any] = {"promptFeedback": {"blockReason": self.blocked}}
            if self.usage:
                result["usageMetadata"] = self.usage
            return result
        candidate: dict[str, Any] = {
            "content": {"role": "model", "parts": self.parts},
            "index": 0,
            "finishReason": self._require_finish(),
        }
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
                             model_name: str):
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
            has_content = False
            retry_now: Any = None
            sampler = RawSampler()
            events = stream_events(
                client, req_headers, body, sampler=sampler, model=model_name, preserve_parts=True)
            try:
                async for status, data in events:
                    if status in ("text", "thought", "image", "function_call", "thought_signature", "native_part"):
                        has_content = True
                        yield status, data
                    elif status in ("finish", "usage", "blocked", "safety"):
                        yield status, data
                    elif status == "cookie_error":
                        credential_manager.record_cookie_auth_failure(account_id, credential=record)
                        raise NativeBridgeError(
                            getattr(data, "status_code", None) or 403, str(data)[:1024])
                    elif status in ("retryable_error", "fatal_error"):
                        if (not has_content and status == "retryable_error"
                                and attempt < retry_max):
                            retry_now = data
                            break
                        default_code = 429 if status == "retryable_error" else 502
                        raise NativeBridgeError(
                            getattr(data, "status_code", None) or default_code, str(data)[:1024])
            finally:
                await events.aclose()

            if retry_now is None:
                return
            # 退避等待期间吐心跳保活（SSE 消费方转成 : keep-alive）；
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
    source = _events_with_retry(request, record, body, model)
    async with aclosing(with_heartbeats(source, request)) as events:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise NativeBridgeError(504, "请求超过总超时时间。")
            try:
                # Never keep an asyncio timeout scope open across a yield:
                # StreamingResponse may read/close us from a different task.
                async with asyncio.timeout(remaining):
                    item = await anext(events)
            except StopAsyncIteration:
                return
            except TimeoutError:
                raise NativeBridgeError(504, "请求超过总超时时间。") from None
            yield ("heartbeat", None) if item is HEARTBEAT else item


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
        return await _handle_native_request(fastapi_request, model, action, parsed,
                                            requested_model=requested_model,
                                            deadline=time.monotonic() + request_timeout_seconds())
    except GenerationCapacityError as exc:
        return _native_error(503, str(exc), "RESOURCE_EXHAUSTED")


@limit_generation
async def _handle_native_request(fastapi_request: Request, model: str, action: str,
                                 parsed: dict, *, requested_model: str, deadline: float):
    return await run_with_image_limit(
        model if action in ("generateContent", "streamGenerateContent") else "",
        lambda: _handle_native_request_impl(fastapi_request, model, action, parsed,
                                           requested_model=requested_model, deadline=deadline),
    )


async def _handle_native_request_impl(fastapi_request: Request, model: str, action: str,
                                      parsed: dict, *, requested_model: str, deadline: float):
    if action not in ("generateContent", "streamGenerateContent"):
        return _native_error(400, f"Cookie 通道暂不支持 :{action}，请配置 Express API Key。", "INVALID_ARGUMENT")
    record = credential_manager.get_cookie_record()
    if record is None:
        return _native_error(403, "未配置任何启用的 Cookie 账号。", "PERMISSION_DENIED")
    project_id = (record.get("project_id") or app_state.get_project_id() or "").strip()
    if not project_id:
        return _native_error(400, "Cookie 账号未配置 Project ID，无法构造上游请求。")
    if build_headers(record.get("cookie", "")) is None:
        return _native_error(401, "Cookie 中未找到 SAPISID，无法计算认证头。")
    account_id = record.get("id", "")
    try:
        body = build_native_body(parsed, model, project_id, requested_model=requested_model)
    except SchemaValidationError as exc:
        return _native_error(400, str(exc), "INVALID_ARGUMENT")

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

            def encode(payload: bytes, *, fragmented: bool=False):
                nonlocal first
                if use_sse:
                    prefix, suffix = b"data: ", b"\n\n"
                else:
                    prefix, suffix = (b"" if first else b","), b""
                    first = False
                if not fragmented:
                    # Keep ordinary token/tool events as one text frame.
                    yield prefix.decode("ascii") + json.dumps(payload, ensure_ascii=False) + suffix.decode("ascii")
                    return
                yield prefix
                # These are byte fragments of ONE SSE event/array element.
                # Never turn an inlineData string into multiple model Parts.
                yield from iter_json_bytes(payload)
                yield suffix

            yield HEARTBEAT_FRAME if use_sse else "["
            try:
                async with aclosing(_timed_events(fastapi_request, record, body, model, deadline)) as events:
                    async for kind, data in events:
                        if kind == "heartbeat":
                            yield HEARTBEAT_FRAME if use_sse else " "
                            continue
                        part = assembler.stream_event(kind, data)
                        if part is not None:
                            for chunk in encode({"candidates": [{"content": {"role": "model", "parts": [part]}, "index": 0}]},
                                                fragmented=assembler.last_part_bytes > 1024 * 1024):
                                yield chunk
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
                for chunk in encode(_google_error_body(exc.code, exc.message)):
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
        return _native_error(exc.code, exc.message)
    except Exception as exc:
        return _native_error(502, str(exc)[:1024], "UNAVAILABLE")
    stats.add_success()
    credential_manager.record_cookie_success(account_id, credential=record)
    print(f"✅ [Cookie 原生桥] {model} | 非流式完成 | {assembler.part_count} parts")
    response_class = LargeJSONResponse if mc.get_profile(model)["is_image"] else JSONResponse
    return response_class(content=payload)
