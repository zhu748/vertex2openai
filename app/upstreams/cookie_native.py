"""Gemini 原生格式 ⇄ Cookie 通道（batchGraphql）桥接。

Express 通道可以字节级透传原生 REST 请求；Cookie 通道的私有 GraphQL 协议
做不到——本模块做的是**协议级转码**：

  请求侧：Gemini 原生 JSON（contents/generationConfig/tools/safetySettings…）
          → batchGraphql variables（复用 cookie_request 的 UI Schema 转换与
          requestContext/querySignature 信封）。
  响应侧：extract_events 的事件流（text/thought/image/function_call/finish…）
          → 重组回 Gemini 原生形状（candidates.content.parts / finishReason /
          usageMetadata / promptFeedback.blockReason），SSE 与整包 JSON 两种
          交付形态都按原生客户端预期输出。

传输层完全复用 Cookie 通道基建：SAPISIDHASH 认证、curl_cffi 伪装、代理链
故障转移、首回复窗口、429 退避重试、账号健康隔离。

与 OpenAI 兼容通道的差异（有意为之）：
- 不做控制台消息注入 / 预填充兼容 / 抗截断传输——原生请求自带完整语义；
- thinkingConfig / 采样参数由客户端原生字段直接透传，不套模型档案默认；
- countTokens / embedContent：上游无对应 GraphQL 操作，明确拒绝。

由 routes/gemini_native.py 在「无 Express Key 且有启用 Cookie 账号」时启用。
"""

import asyncio
import json
import re
from typing import Any, Optional

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse

import config as app_config
import model_capabilities as mc
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
from retry_policy import get_retry_settings, retry_delay
from runtime_state import app_state
from schema_validation import SchemaValidationError
from streaming import HEARTBEAT_FRAME
from upstreams.cookie_proxy import (
    RawSampler,
    _execute_stream_request_generator,
    _sleep_with_heartbeat,
)
from upstreams.cookie_request import (
    COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE,
    _build_request_context,
    _cookie_ui_schema,
    _native_default_safety_settings,
    _native_supported_generation_keys,
)

# 生图 markdown（extract_events 的 image 事件载荷）→ inlineData 反解
_IMAGE_MD_RE = re.compile(r"^!\[.*?\]\(data:([^;,]+);base64,([A-Za-z0-9+/=]+)\)$", re.S)

# 原生请求里上游 GraphQL 无法承载、需要静默丢弃的字段（丢弃时打一条日志）
_DROPPED_NATIVE_KEYS = ("cachedContent", "labels")


def _google_error_body(code: int, message: str, status_detail: Optional[str] = None) -> dict:
    """Google 风格错误体（原生 SDK 按 error.message 展示）。"""
    detail = status_detail or {
        400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 403: "PERMISSION_DENIED",
        404: "NOT_FOUND", 413: "OUT_OF_RANGE", 429: "RESOURCE_EXHAUSTED",
        503: "UNAVAILABLE", 504: "DEADLINE_EXCEEDED",
    }.get(code, "INTERNAL" if code >= 500 else "FAILED_PRECONDITION")
    return {"error": {"code": code, "message": message, "status": detail}}


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

    与 OpenAI 兼容通道同口径：自定义函数与内建搜索混用会被上游拒绝
    （"Multiple tools are supported only when they are all search tools."），
    函数优先、搜索退位。parameters 走 _cookie_ui_schema（严格方言 → Studio
    私有 UI Schema），与工具参数的转换链完全一致。
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
            converted["parameters"] = _cookie_ui_schema(decl.get("parameters") or {"type": "object"})
            declarations.append(converted)
    if declarations and use_search:
        print("⚠️ [Cookie 原生桥] batchGraphql 不支持函数与内建搜索混用；已保留自定义函数，忽略 googleSearch。")
        use_search = False
    return declarations, use_search, [d["name"] for d in declarations]


def _sanitize_tool_config(tool_config: Any, declared_names: list[str]) -> Optional[dict]:
    """原生 toolConfig 透传，仅把 allowedFunctionNames 收窄到已声明函数
    （内建搜索没有 functionDeclaration 名字，写进去会让 wire config 自非法）。"""
    if not isinstance(tool_config, dict):
        return None
    config = tool_config.get("functionCallingConfig")
    if not isinstance(config, dict):
        return dict(tool_config) if tool_config else None
    cleaned = dict(tool_config)
    fcc = dict(config)
    allowed = fcc.get("allowedFunctionNames")
    if isinstance(allowed, list):
        kept = [n for n in allowed if n in declared_names]
        if kept:
            fcc["allowedFunctionNames"] = kept
        else:
            fcc.pop("allowedFunctionNames", None)
        if set(allowed) != set(kept):
            print("⚠️ [Cookie 原生桥] toolConfig.allowedFunctionNames 含未声明函数，已收窄到已声明集合。")
    cleaned["functionCallingConfig"] = fcc
    return cleaned


def native_request_to_variables(parsed: dict, model_name: str, project_id: str) -> dict:
    """Gemini 原生请求体 → batchGraphql variables（未包信封）。

    raises SchemaValidationError：结构化输出被策略拒绝（与 OpenAI 通道同口径）。
    """
    profile = mc.get_profile(model_name)

    gen = parsed.get("generationConfig")
    gen = dict(gen) if isinstance(gen, dict) else {}
    unknown_gen = [k for k in gen if k not in _native_supported_generation_keys()]
    if unknown_gen:
        print(f"⚠️ [Cookie 原生桥] generationConfig 未经验证的字段将原样透传：{unknown_gen}")
    if gen.get("responseMimeType") or gen.get("responseSchema"):
        if not app_state.get_setting("cookie_structured_output", False):
            raise SchemaValidationError(COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE)
        schema = gen.get("responseSchema")
        if isinstance(schema, dict) and schema:
            gen["responseSchema"] = _cookie_ui_schema(
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
        "safetySettings": parsed.get("safetySettings") or _native_default_safety_settings(),
    }

    system = _normalize_system_instruction(parsed.get("systemInstruction"))
    if system:
        variables["systemInstruction"] = system

    declarations, use_search, names = _native_tools_to_variables(parsed.get("tools"))
    tool_config = _sanitize_tool_config(parsed.get("toolConfig"), names)
    if profile["is_image"]:
        declarations = []
    if declarations:
        variables["tools"] = [{"functionDeclarations": declarations}]
        if tool_config:
            variables["toolConfig"] = tool_config
    elif use_search and profile["supports_search"]:
        variables["tools"] = [{"googleSearch": {}}]
    return variables


def build_native_body(parsed: dict, model_name: str, project_id: str) -> dict:
    """variables + GraphQL 信封（requestContext / querySignature 均为热改感知）。"""
    return {
        "requestContext": _build_request_context(project_id),
        "querySignature": get_stream_query_signature(),
        "operationName": STREAM_GENERATE_OPERATION_NAME,
        "variables": native_request_to_variables(parsed, model_name, project_id),
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


class NativeAssembler:
    """把 cookie 通道事件流拼装成 Gemini 原生响应。

    流式：stream_event 返回应立即下发的 part（无则 None）；terminal 返回
    收尾帧（finishReason/usageMetadata/safetyRatings）。
    非流式：collect 同样喂事件，payload 一次性出整包。
    """

    def __init__(self, model_version: str):
        self.model_version = model_version
        self.parts: list[dict] = []
        self.finish: Optional[str] = None
        self.usage: Optional[dict] = None
        self.safety: Any = None
        self.blocked: Optional[str] = None
        self.has_content = False

    # -- 事件喂入 --------------------------------------------------------
    def feed(self, kind: str, data) -> Optional[dict]:
        """处理一个事件；返回需立即下发的新增 part（流式用），None 表示不发。"""
        part: Optional[dict] = None
        if kind == "text":
            part = {"text": data}
        elif kind == "thought":
            part = {"text": data, "thought": True}
        elif kind == "image":
            part = _image_part(data)
        elif kind == "function_call":
            part = {"functionCall": {"name": data.get("name"), "args": data.get("args") or {}}}
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
            self.usage = data if isinstance(data, dict) else None
        elif kind == "safety":
            self.safety = data
        elif kind == "blocked":
            self.blocked = str(data).split("（")[0].split("(")[0].strip() or "BLOCKED"
        if part is not None:
            self.parts.append(part)
            self.has_content = True
        return part

    # 流式别名（语义清晰）
    def stream_event(self, kind: str, data) -> Optional[dict]:
        return self.feed(kind, data)

    # -- 收尾 ------------------------------------------------------------
    def _require_finish(self) -> str:
        if not self.finish:
            raise IncompleteResponseError("上游连接已结束，但回复缺少结束标记（finishReason）。")
        return self.finish

    def terminal(self) -> Optional[dict]:
        """流式收尾帧：无任何可交付内容时返回 None（由调用方决定报错文案）。"""
        if self.blocked and not self.parts:
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
        if self.blocked and not self.parts:
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
        from proxy_resolver import mask_proxy_uri
        print(f"🌐 [出站代理] 当前出口：{mask_proxy_uri(proxy_chain.current)}")
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
            events = _execute_stream_request_generator(
                client, req_headers, body, sampler=sampler, model=model_name)
            try:
                async for status, data in events:
                    if status in ("text", "thought", "image", "function_call", "thought_signature"):
                        has_content = True
                        yield status, data
                    elif status in ("finish", "usage", "blocked", "safety"):
                        yield status, data
                    elif status == "cookie_error":
                        credential_manager.record_cookie_auth_failure(account_id)
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
            # 退避等待期间吐心跳保活（SSE 消费方转成 : keep-alive）。
            async for _hb in _sleep_with_heartbeat(
                    retry_delay(getattr(retry_now, "retry_after", None),
                                backoff_sec, attempt, jitter_sec, error=str(retry_now)),
                    request=fastapi_request):
                if await fastapi_request.is_disconnected():
                    raise NativeBridgeError(499, "客户端已断开连接。")
                yield "heartbeat", None
    raise NativeBridgeError(429, "上游持续限流，重试多次仍失败。")


# ============================================================
# 端点入口（由 routes/gemini_native.py 调用）
# ============================================================
async def handle_native_request(fastapi_request: Request, model: str, action: str,
                                parsed: dict, *, requested_model: str = ""):
    """Gemini 原生请求走 Cookie 通道桥接。返回 JSONResponse / StreamingResponse。"""
    if action not in ("generateContent", "streamGenerateContent"):
        return _native_error(
            400,
            f"Cookie 通道暂不支持 :{action}（上游 batchGraphql 无对应操作），"
            "请配置 Express API Key 使用该操作。",
            "INVALID_ARGUMENT")

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
        body = build_native_body(parsed, model, project_id)
    except SchemaValidationError as exc:
        return _native_error(400, str(exc), "INVALID_ARGUMENT")

    is_stream = action == "streamGenerateContent"
    print(f"🍪 [Cookie 原生桥] {requested_model or model}:{action} | "
          f"{'流式 SSE' if is_stream else '非流式'} | 协议转码（非字节级透传）")

    if is_stream:
        async def sse():
            assembler = NativeAssembler(requested_model or model)
            yield HEARTBEAT_FRAME
            try:
                async for kind, data in _events_with_retry(fastapi_request, record, body, model):
                    if kind == "heartbeat":
                        yield HEARTBEAT_FRAME
                        continue
                    part = assembler.stream_event(kind, data)
                    if part is not None:
                        yield "data: " + json.dumps({
                            "candidates": [{"content": {"role": "model", "parts": [part]}, "index": 0}],
                        }, ensure_ascii=False) + "\n\n"
                terminal = assembler.terminal()
                yield "data: " + json.dumps(terminal, ensure_ascii=False) + "\n\n"
                stats.add_success()
                credential_manager.record_cookie_success(account_id)
                print(f"✅ [Cookie 原生桥] {model} | 流式完成 | {len(assembler.parts)} parts")
            except asyncio.CancelledError:
                raise
            except NativeBridgeError as exc:
                if exc.code == 499:
                    return
                stats.add_error()
                yield "data: " + json.dumps(
                    _google_error_body(exc.code, exc.message), ensure_ascii=False) + "\n\n"
            except IncompleteResponseError as exc:
                stats.add_error()
                yield "data: " + json.dumps(
                    _google_error_body(502, str(exc), "UNAVAILABLE"), ensure_ascii=False) + "\n\n"
            except Exception as exc:  # 兜底：SSE 已 200 建流，只能以错误帧收尾
                stats.add_error()
                yield "data: " + json.dumps(
                    _google_error_body(502, f"Cookie 通道桥接异常：{type(exc).__name__}: {exc}"[:1024],
                                       "UNAVAILABLE"), ensure_ascii=False) + "\n\n"

        return StreamingResponse(sse(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-store, no-transform",
                                          "X-Accel-Buffering": "no"})

    # 非流式：整包收集后一次返回
    assembler = NativeAssembler(requested_model or model)
    try:
        async for kind, data in _events_with_retry(fastapi_request, record, body, model):
            assembler.feed(kind, data)
        payload = assembler.payload()
    except NativeBridgeError as exc:
        stats.add_error()
        if exc.code == 499:
            return JSONResponse(status_code=499, content=_google_error_body(499, exc.message))
        return JSONResponse(status_code=exc.code, content=_google_error_body(exc.code, exc.message))
    except IncompleteResponseError as exc:
        stats.add_error()
        return JSONResponse(status_code=502, content=_google_error_body(502, str(exc), "UNAVAILABLE"))

    stats.add_success()
    credential_manager.record_cookie_success(account_id)
    print(f"✅ [Cookie 原生桥] {model} | 非流式完成 | {len(assembler.parts)} parts")
    return JSONResponse(content=payload)
