"""Cookie batchGraphql 请求构造：消息、工具、模型参数与协议字段。"""

import asyncio
import json
import time
import uuid
from typing import Any, Optional

import config as app_config
import model_capabilities as mc
from cookie_auth import STREAM_GENERATE_OPERATION_NAME, STREAM_GENERATE_QUERY_SIGNATURE
from message_processing import _coerce_tool_response, openai_content_to_wire_parts
from models import OpenAIRequest, normalize_content_part
from runtime_state import app_state
from schema_validation import SchemaValidationError
from thought_signatures import (
    encode_thought_signature,
    google_metadata,
    is_thought_part,
    requires_signature,
    resolve_tool_call_signature,
    signature_from_extra,
)


def _get_experiment_flags() -> str:
    """获取 experimentFlagsBinary（Express Mode 权限的关键标识，可选，从配置读取）"""
    return app_config.EXPERIMENT_FLAGS or ""


def _build_request_context(project_id: str) -> dict:
    """
    构建 batchGraphql 的 requestContext
    包含 experimentFlagsBinary，这是 Express Mode 权限的关键标识。
    """
    return {
        "clientVersion": "boq_cloud-boq-clientweb-vertexaistudio_20260609.06_p0",
        "pagePath": "/agent-platform/studio/multimodal",
        "pageViewId": int(time.time() * 1000) % (10**15),
        "trackingId": str(int(time.time() * 1000000) % (10**17)),
        "backendOverrides": {},
        "clientSessionId": str(uuid.uuid4()).upper(),
        "projectId": project_id,
        "selectedPurview": {"projectId": project_id},
        "jurisdiction": "global",
        "experimentFlagsBinary": _get_experiment_flags(),
        "localizationData": {"locale": "zh_CN", "timezone": "Asia/Hong_Kong"},
    }


def _build_thinking_config(
    model_name: str, request: OpenAIRequest, prefill_active: bool = False, *, settings: dict | None = None
) -> Optional[dict]:
    """
    按模型能力档案 + 控制台设置 + 单次请求构建 thinkingConfig：
    - Gemini 3 及以上（含生图模型）：thinkingLevel（MINIMAL/LOW/MEDIUM/HIGH，生图默认 HIGH）
    - 其它：None
    prefill_active=True 时按控制台开关压制思考（见 mc.resolve_thinking）。
    """
    if settings is None:
        settings = app_state.get_effective_settings(model_name)
    t = mc.resolve_thinking(model_name, request, settings, prefill_active=prefill_active)
    if t.get("mode") == "level":
        return {"thinkingLevel": t["level"].upper(), "includeThoughts": t.get("include_thoughts", True)}
    if t.get("mode") == "budget":
        return {"thinkingBudget": t["budget"], "includeThoughts": t.get("include_thoughts", True)}
    return None


_BUILTIN_SEARCH_NAMES = {
    "google_search",
    "googlesearch",
    "google_search_retrieval",
    "web_search",
    "websearch",
    "search_web",
    "browse",
    "search",
}


def classify_tool_traffic(messages: list, tools: Any = None) -> dict:
    """识别搜索、自定义函数声明和工具历史，用于通道选择与预填充保护。"""
    declared = bool(tools)
    builtin_search = False
    custom_names: list = []
    for t in tools or []:
        if not isinstance(t, dict):
            continue
        fn = t.get("function") if isinstance(t.get("function"), dict) else {}
        name = str(fn.get("name") or t.get("name") or t.get("type") or "").strip()
        if name.lower().replace("-", "_") in _BUILTIN_SEARCH_NAMES:
            builtin_search = True
        elif name:
            custom_names.append(name)

    history = False
    for m in messages or []:
        if getattr(m, "role", None) in ("tool", "function") or getattr(m, "tool_calls", None):
            history = True
            break

    return {
        "declared": declared,
        "history": history,
        "builtin_search": builtin_search,
        "custom_names": custom_names,
    }


_SCHEMA_TYPES = {
    "object": "OBJECT",
    "array": "ARRAY",
    "string": "STRING",
    "integer": "INTEGER",
    "number": "NUMBER",
    "boolean": "BOOLEAN",
    "null": "NULL",
}


def _cookie_ui_schema(schema: Any) -> Any:
    """OpenAI JSON Schema -> Studio private UI Schema.

    The private batchGraphql input uses upper-case scalar names and encodes an
    object's property map as an ordered ``[{key, value}]`` list.  Recursion is
    required for both nested objects and array item schemas.
    """
    if not isinstance(schema, dict):
        return schema

    out = {}
    raw_type = schema.get("type")
    if isinstance(raw_type, list):
        non_null = [t for t in raw_type if str(t).lower() != "null"]
        raw_type = non_null[0] if non_null else "null"
        if len(non_null) != len(schema.get("type") or []):
            out["nullable"] = True
    if not raw_type:
        if isinstance(schema.get("properties"), dict):
            raw_type = "object"
        elif "items" in schema:
            raw_type = "array"
    if raw_type:
        out["type"] = _SCHEMA_TYPES.get(str(raw_type).lower(), str(raw_type).upper())

    for key, value in schema.items():
        if key == "type":
            continue
        if key == "properties" and isinstance(value, dict):
            out[key] = [{"key": name, "value": _cookie_ui_schema(child)} for name, child in value.items()]
        elif key in {"items", "additionalProperties", "not"} and isinstance(value, dict):
            out[key] = _cookie_ui_schema(value)
        elif key in {"oneOf", "anyOf", "allOf", "prefixItems"} and isinstance(value, list):
            out[key] = [_cookie_ui_schema(item) for item in value]
        else:
            out[key] = value
    return out


def _tool_function(tool: Any) -> dict:
    return tool.get("function") if isinstance(tool, dict) and isinstance(tool.get("function"), dict) else {}


def normalized_tool_name(tool: Any) -> str:
    fn = _tool_function(tool)
    return str(fn.get("name") or (tool.get("name") if isinstance(tool, dict) else "") or "").strip()


def is_builtin_search_name(name: str) -> bool:
    return name.lower().replace("-", "_") in _BUILTIN_SEARCH_NAMES


def _build_function_declarations(tools: Any) -> tuple[list[dict], bool]:
    declarations = []
    builtin_search = False
    for tool in tools or []:
        if not isinstance(tool, dict):
            continue
        name = normalized_tool_name(tool)
        if not name:
            continue
        if is_builtin_search_name(name):
            builtin_search = True
            continue
        fn = _tool_function(tool)
        declaration = {"name": name}
        if fn.get("description") is not None:
            declaration["description"] = fn["description"]
        declaration["parameters"] = _cookie_ui_schema(fn.get("parameters") or {"type": "object"})
        declarations.append(declaration)
    return declarations, builtin_search


def _build_tool_config(tool_choice: Any, function_names: list[str]) -> Optional[dict]:
    """Map OpenAI selection to Studio without naming built-in search as a function."""
    if tool_choice is None:
        mode, allowed = "AUTO", None
    elif isinstance(tool_choice, str):
        choice = tool_choice.lower()
        if choice == "none":
            mode, allowed = "NONE", None
        elif choice == "required":
            mode, allowed = "ANY", function_names or None
        else:  # auto and permissive handling of unknown OpenAI-compatible values
            mode, allowed = "AUTO", None
    elif isinstance(tool_choice, dict):
        name = normalized_tool_name(tool_choice)
        mode = "ANY"
        # googleSearch is a built-in Tool, not a FunctionDeclaration. Putting its
        # OpenAI alias in allowedFunctionNames makes the wire config self-invalid.
        allowed = (
            None
            if (name and is_builtin_search_name(name))
            else ([name] if name else (function_names or None))
        )
    else:
        mode, allowed = "AUTO", None

    config = {"mode": mode}
    if allowed:
        config["allowedFunctionNames"] = allowed
    return {"functionCallingConfig": config}


def _plain_text_of(content: Any) -> str:
    """取消息的纯文本（用于把工具往返渲染成可读文本）。"""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    out = []
    for p in content or []:
        p = normalize_content_part(p)
        if isinstance(p, dict) and p.get("type") == "text":
            out.append(p.get("text") or "")
    if out:
        return "\n".join(out)
    try:
        return json.dumps(content, ensure_ascii=False)
    except Exception:
        return str(content)


def _parse_function_args(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _ordinary_wire_parts_from_extra(message: Any) -> Optional[list[dict]]:
    google = google_metadata(message)
    raw = google.get("ordinary_parts")
    if not isinstance(raw, list):
        return None
    parts = []
    for item in raw:
        if (not isinstance(item, dict) or not isinstance(item.get("kind"), str)
                or item["kind"] not in {"text", "thought", "signature_only"}):
            return None
        text = item.get("text")
        if not isinstance(text, str) or (item["kind"] == "text" and "data:image/" in text):
            return None
        part = {"text": text}
        if item["kind"] == "thought":
            part["thought"] = True
        if item.get("thought_signature"):
            part["thoughtSignature"] = item["thought_signature"]
        parts.append(part)
    return parts


def _attach_message_signature(parts: list[dict], message: Any) -> None:
    signature, kind = signature_from_extra(message)
    encoded = encode_thought_signature(signature)
    if not encoded:
        return
    target = None
    if kind == "thought":
        target = next((p for p in reversed(parts) if is_thought_part(p)), None)
    elif kind == "text":
        target = next((p for p in reversed(parts) if "text" in p and not is_thought_part(p)), None)
    if target is None and kind != "signature_only":
        target = parts[-1] if parts else None
    if target is None:
        target = {"text": ""}
        parts.append(target)
    target["thoughtSignature"] = encoded


def _convert_messages_to_contents(messages: list, model_name: str = "", native_tools: bool = True) -> tuple:
    """OpenAI messages → batchGraphql contents.

    Native mode emits camelCase ``functionCall`` / ``functionResponse`` Parts.
    ``native_tools=False`` is the controlled compatibility fallback used only
    after a clear schema/model rejection from the upstream.
    """
    contents = []
    system_parts = []
    require_sig = requires_signature(model_name)
    call_names = {}
    for message in messages or []:
        for tc in getattr(message, "tool_calls", None) or []:
            tc_id = str((tc or {}).get("id") or "")
            fn_name = str(((tc or {}).get("function") or {}).get("name") or "")
            if tc_id and fn_name:
                call_names[tc_id] = fn_name

    for msg in messages:
        role = msg.role
        content = msg.content

        # developer 与 system 同语义（OpenAI 新约定的最高优先级指令）：
        # 统一收集进 system_parts，不降级为 user 轮混进对话。
        if role in ("system", "developer"):
            if isinstance(content, str):
                system_parts.append(content)
            else:
                for p in content or []:
                    p = normalize_content_part(p)
                    if isinstance(p, dict) and p.get("type") == "text":
                        # or 兑底：text:null 的分段以原始 dict 留存，
                        # get("text", "") 返回 None 会污染 system 文本拼接。
                        system_parts.append(p.get("text") or "")
            continue

        # 旧版 functions 协议的结果消息是 role="function"（与 Express 通道的
        # message_processing 保持同一兼容口径），漏掉会被当成 model 自述文本。
        if role in ("tool", "function"):
            name = getattr(msg, "name", None) or call_names.get(getattr(msg, "tool_call_id", "") or "")
            if native_tools and name:
                contents.append(
                    {
                        "role": "user",
                        "parts": [
                            {"functionResponse": {"name": name, "response": _coerce_tool_response(content)}}
                        ],
                    }
                )
            else:
                body = _plain_text_of(content)
                contents.append(
                    {"role": "user", "parts": [{"text": f"[工具执行结果 · {name or 'tool'}]\n{body}"}]}
                )
            continue

        tool_calls = getattr(msg, "tool_calls", None)
        if role in ("assistant", "model") and tool_calls:
            if not native_tools:
                lines = []
                for tc in tool_calls:
                    fn = (tc or {}).get("function") or {}
                    lines.append(
                        f"[请求调用工具 · {fn.get('name', 'unknown')}] 参数：{fn.get('arguments', '{}')}"
                    )
                said = _plain_text_of(content)
                if said:
                    lines.insert(0, said)
                contents.append({"role": "model", "parts": [{"text": "\n".join(lines)}]})
                continue

            call_parts = []
            for tool_index, tc in enumerate(tool_calls):
                fn = (tc or {}).get("function") or {}
                explicit_sig, _ = signature_from_extra(tc)
                _, signature = resolve_tool_call_signature(
                    str((tc or {}).get("id") or ""),
                    require_signature=require_sig and tool_index == 0,
                    explicit_signature=explicit_sig,
                    explicit_unsigned=tool_index > 0,
                )
                part = {
                    "functionCall": {
                        "name": str(fn.get("name") or "unknown"),
                        "args": _parse_function_args(fn.get("arguments", {})),
                    }
                }
                encoded = encode_thought_signature(signature)
                if encoded:
                    part["thoughtSignature"] = encoded
                call_parts.append(part)

            ordinary_parts = _ordinary_wire_parts_from_extra(msg)
            if ordinary_parts is None:
                ordinary_parts = []
                reasoning = getattr(msg, "reasoning_content", None)
                if reasoning:
                    ordinary_parts.append({"text": reasoning, "thought": True})
                ordinary_parts.extend(openai_content_to_wire_parts(content))
                _attach_message_signature(ordinary_parts, msg)

            google = google_metadata(msg)
            order = google.get("part_order")
            if isinstance(order, list):
                parts, used_calls, used_ordinary = [], set(), set()
                for item in order:
                    if not isinstance(item, dict):
                        continue
                    idx = item.get("index")
                    if (
                        item.get("type") == "tool_call"
                        and isinstance(idx, int)
                        and 0 <= idx < len(call_parts)
                    ):
                        parts.append(call_parts[idx])
                        used_calls.add(idx)
                    elif (
                        item.get("type") == "ordinary"
                        and isinstance(idx, int)
                        and 0 <= idx < len(ordinary_parts)
                    ):
                        parts.append(ordinary_parts[idx])
                        used_ordinary.add(idx)
                parts.extend(p for i, p in enumerate(call_parts) if i not in used_calls)
                parts.extend(p for i, p in enumerate(ordinary_parts) if i not in used_ordinary)
            else:
                parts = call_parts + ordinary_parts
            contents.append({"role": "model", "parts": parts})
            continue

        gemini_role = "user" if role == "user" else "model"
        parts = _ordinary_wire_parts_from_extra(msg) if role in ("assistant", "model") else None
        if parts is None:
            parts = openai_content_to_wire_parts(content)
            if role in ("assistant", "model"):
                _attach_message_signature(parts, msg)
        if parts:
            contents.append({"role": gemini_role, "parts": parts})

    # Keep parallel FunctionResponses adjacent and unsigned: FC1, FC2 then FR1, FR2.
    merged: list = []
    for c in contents:
        if merged and merged[-1]["role"] == c["role"]:
            previous_fr = bool(merged[-1]["parts"]) and all(
                "functionResponse" in p for p in merged[-1]["parts"]
            )
            current_fr = bool(c["parts"]) and all("functionResponse" in p for p in c["parts"])
            # Only adjacent FunctionResponse messages belong to the same parallel
            # result turn. Never merge a later fresh user message into that turn.
            if previous_fr != current_fr:
                merged.append(c)
                continue
            if not (previous_fr and current_fr):
                merged[-1]["parts"].append({"text": "\n\n"})
            merged[-1]["parts"].extend(c["parts"])
        else:
            merged.append(c)

    system_text = "\n".join(t for t in system_parts if t) if system_parts else None
    return merged, system_text


async def build_request_body_async(
    project_id: str,
    model_name: str,
    request: OpenAIRequest,
    prefill_active: bool = False,
    force_search: bool = False,
    native_tools: bool = True,
) -> dict:
    """在线程里构建请求体：内部有远程图片下载与 PIL 压缩，不能阻塞事件循环（P1-2）。"""
    return await asyncio.to_thread(
        build_request_body, project_id, model_name, request, prefill_active, force_search, native_tools
    )


def build_request_body(
    project_id: str,
    model_name: str,
    request: OpenAIRequest,
    prefill_active: bool = False,
    force_search: bool = False,
    native_tools: bool = True,
) -> dict:
    contents, system_text = _convert_messages_to_contents(
        request.messages, model_name=model_name, native_tools=native_tools
    )
    model_path = f"projects/{project_id}/locations/global/publishers/google/models/{model_name}"

    settings = app_state.get_effective_settings(model_name)
    profile = mc.apply_sampling_policy(mc.get_profile(model_name), settings)
    allowed = profile["allowed_sampling"]

    gen_config = {}

    if profile["is_image"]:
        # 生图：设 responseModalities + imageConfig + thinkingConfig；不发采样参数
        gen_config["responseModalities"] = ["TEXT", "IMAGE"]
        img_cfg = {}
        size = mc.resolve_image_size(model_name, request, settings)
        if size:
            img_cfg["imageSize"] = size
        ar = mc.resolve_aspect_ratio(model_name, request, settings)
        if ar:
            img_cfg["aspectRatio"] = ar
        if img_cfg:
            gen_config["imageConfig"] = img_cfg
        # 生图思考档位：三模型默认 HIGH（用户决策 2026-10-13）
        # Nano Banana 家族专属：media_resolution=HIGH（用户决策 2026-10-13）
        if "nano-banana" in (model_name or "").lower():
            gen_config["mediaResolution"] = "HIGH"
    else:
        # 文本/多模态：仅注入该模型支持、且被显式设置（请求或控制台）的采样参数。
        # 与标准（Express）通道对齐：都未设置时**不发**该参数，交给模型默认——
        # 旧版会编造 temperature=1 / topP=0.95 / maxOutputTokens=65535 兜底值发出。
        if "temperature" in allowed:
            tv = (
                request.temperature
                if request.temperature is not None
                else settings.get("default_temperature")
            )
            if tv is not None:
                gen_config["temperature"] = tv
        if "top_p" in allowed:
            pv = request.top_p if request.top_p is not None else settings.get("default_top_p")
            if pv is not None:
                gen_config["topP"] = pv
        if "max_output_tokens" in allowed:
            mv = (
                request.max_tokens
                if request.max_tokens is not None
                else getattr(request, "max_completion_tokens", None)
            )
            if mv is None:
                mv = settings.get("default_max_tokens")
            if mv is not None:
                gen_config["maxOutputTokens"] = mv

        # These are accepted by the shared OpenAI contract. Unsupported private
        # protocol fields must fail explicitly rather than silently change it.
        if request.response_format and request.response_format.get("type") not in (None, "text"):
            raise SchemaValidationError("Cookie 通道尚未验证结构化输出支持，请改用 Express 通道。")
        for source, target in (("top_k", "topK"), ("seed", "seed"), ("n", "candidateCount")):
            capability = "candidate_count" if source == "n" else source
            value = getattr(request, source, None)
            if value is not None and capability in allowed:
                gen_config[target] = value

    thinking_config = _build_thinking_config(
        model_name, request, prefill_active=prefill_active, settings=settings
    )
    if thinking_config:
        gen_config["thinkingConfig"] = thinking_config

    # OFF = 分类器整个关掉，上游**不会回传** safetyRatings；BLOCK_NONE = 照常评分但从不拦截。
    # 想看安全分就必须用后者，因此跟着「输出附加安全分」开关走（默认仍是 OFF，行为不变）。
    _want_scores = bool(app_state.get_setting("safety_score", app_config.SAFETY_SCORE))
    _threshold = "BLOCK_NONE" if _want_scores else "OFF"
    safety_settings = [
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": _threshold},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": _threshold},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": _threshold},
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": _threshold},
        # 显式列出该分类，阈值与其余文本分类统一由 safety_score 设置。
        {"category": "HARM_CATEGORY_JAILBREAK", "threshold": _threshold},
    ]
    # Nano Banana 家族生图：追加 4 个图像安全分类，阈值全 OFF（官方示例基准）；
    # 旧生图模型维持 5 个文本分类不变（用户决策 2026-10-13）。
    if profile["is_image"] and "nano-banana" in (model_name or "").lower():
        safety_settings.extend(
            [
                {"category": "HARM_CATEGORY_IMAGE_HATE", "threshold": "OFF"},
                {"category": "HARM_CATEGORY_IMAGE_DANGEROUS_CONTENT", "threshold": "OFF"},
                {"category": "HARM_CATEGORY_IMAGE_HARASSMENT", "threshold": "OFF"},
                {"category": "HARM_CATEGORY_IMAGE_SEXUALLY_EXPLICIT", "threshold": "OFF"},
            ]
        )

    variables = {
        "contents": contents,
        "model": model_path,
        "generationConfig": gen_config,
        "safetySettings": safety_settings,
    }

    if system_text and (not profile["is_image"] or settings.get("image_system_instruction", False)):
        variables["systemInstruction"] = {"parts": [{"text": system_text}]}

    if request.stop and "stop_sequences" in allowed:
        gen_config["stopSequences"] = request.stop if isinstance(request.stop, list) else [request.stop]

    # Live batchGraphql verification shows that custom functionDeclarations and
    # googleSearch are each supported, but mixing them in one request is rejected:
    # "Multiple tools are supported only when they are all search tools." Choose a
    # single upstream mode deterministically instead of sending a doomed payload.
    declarations, declared_search = (
        _build_function_declarations(request.tools) if native_tools else ([], False)
    )
    if profile["is_image"]:
        declarations = []

    choice_name = normalized_tool_name(request.tool_choice) if isinstance(request.tool_choice, dict) else ""
    forced_search = bool(choice_name and is_builtin_search_name(choice_name))
    tools_disabled = isinstance(request.tool_choice, str) and request.tool_choice.lower() == "none"
    _wants_search = (
        force_search
        or declared_search
        or forced_search
        or (hasattr(request, "model") and request.model.endswith("-search"))
    )

    selected_declarations = []
    use_google_search = False
    if not tools_disabled:
        if forced_search:
            # Built-in search has no functionDeclaration name, so it cannot be
            # referenced by allowedFunctionNames. Send googleSearch alone.
            use_google_search = bool(profile["supports_search"])
        elif declarations:
            # Custom functions take precedence for mixed AUTO/required traffic.
            selected_declarations = declarations
        elif _wants_search and profile["supports_search"]:
            use_google_search = True

    if selected_declarations:
        variables["tools"] = [{"functionDeclarations": selected_declarations}]
        function_names = [item["name"] for item in selected_declarations]
        variables["toolConfig"] = _build_tool_config(request.tool_choice, function_names)
    elif use_google_search:
        variables["tools"] = [{"googleSearch": {}}]
    # With no upstream tools, omitting toolConfig is equivalent to NONE and avoids
    # applying custom functionCallingConfig to the built-in search protocol.

    return {
        "requestContext": _build_request_context(project_id),
        "querySignature": STREAM_GENERATE_QUERY_SIGNATURE,
        "operationName": STREAM_GENERATE_OPERATION_NAME,
        "variables": variables,
    }
