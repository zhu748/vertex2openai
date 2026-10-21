"""OpenAI 请求到 Gemini SDK 生成参数的映射与模型能力裁剪。"""

from typing import Any

from google.genai import types

import config as app_config
import model_capabilities as mc
from models import OpenAIRequest, normalize_content_part
from runtime_state import app_state


_TEXT_SAFETY_CATEGORIES = (
    "HATE_SPEECH",
    "DANGEROUS_CONTENT",
    "SEXUALLY_EXPLICIT",
    "HARASSMENT",
    "JAILBREAK",
)
_IMAGE_SAFETY_CATEGORIES = (
    "IMAGE_HATE",
    "IMAGE_DANGEROUS_CONTENT",
    "IMAGE_HARASSMENT",
    "IMAGE_SEXUALLY_EXPLICIT",
)
_IMAGE_SEARCH_NAMES = {"google_search", "googlesearch", "web_search", "websearch", "search"}


def _system_instruction(request: OpenAIRequest) -> str | None:
    texts = []
    for message in request.messages:
        # developer 与 system 同语义：进 system_instruction，不降级为 user 轮。
        if message.role not in ("system", "developer") or not message.content:
            continue
        if isinstance(message.content, str):
            texts.append(message.content)
        else:
            for part in message.content:
                part = normalize_content_part(part)
                if isinstance(part, dict) and part.get("type") == "text":
                    # text:null 的分段过不了 ContentPartText 校验、以原始 dict 留存；
                    # 键存在但值为 None 时 get("text", "") 返回 None，拼进 join 即 TypeError。
                    texts.append(part.get("text") or "")
    return "\n".join(texts) if texts else None


def _request_parameters(request: OpenAIRequest) -> dict[str, Any]:
    names = {
        "temperature": "temperature",
        "top_p": "top_p",
        "top_k": "top_k",
        "seed": "seed",
        "n": "candidate_count",
    }
    result = {
        target: getattr(request, source)
        for source, target in names.items()
        if getattr(request, source) is not None
    }
    max_tokens = request.max_tokens if request.max_tokens is not None else request.max_completion_tokens
    if max_tokens is not None:
        result["max_output_tokens"] = max_tokens
    if request.stop:
        # 成员级过滤：stop=[""] / ["a", ""] 里的空串对 Gemini 是非法入参
        # （注释早写明，旧实现只挡了整体空值，成员级空串仍下发 → 上游 400）。
        if isinstance(request.stop, str):
            result["stop_sequences"] = [request.stop]
        else:
            seqs = [s for s in request.stop if s]
            if seqs:
                result["stop_sequences"] = seqs

    if request.response_logprobs is not None:
        result["response_logprobs"] = request.response_logprobs
    # bool 是 int 的子类，必须先按 OpenAI 的布尔语义处理。
    if isinstance(request.logprobs, bool):
        if request.logprobs:
            result["response_logprobs"] = True
            result["logprobs"] = request.top_logprobs if isinstance(request.top_logprobs, int) else 1
    elif isinstance(request.logprobs, int):
        result["logprobs"] = request.logprobs
    elif isinstance(request.top_logprobs, int):
        result["logprobs"] = request.top_logprobs
    return result


def _response_format(request: OpenAIRequest) -> dict[str, Any]:
    fmt = request.response_format or {}
    if fmt.get("type") not in ("json_object", "json_schema"):
        return {}
    result: dict[str, Any] = {"response_mime_type": "application/json"}
    json_schema = fmt.get("json_schema")
    if fmt["type"] == "json_schema" and isinstance(json_schema, dict):
        schema = json_schema.get("schema")
        if isinstance(schema, dict):
            result["response_schema"] = {key: value for key, value in schema.items() if key != "$schema"}
    return result


def _function_tools(request: OpenAIRequest) -> list[dict]:
    declarations = []
    for tool in request.tools or []:
        function = tool.get("function") if tool.get("type") == "function" else None
        if not function or not function.get("name"):
            continue
        declaration = {
            key: function[key]
            for key in ("name", "description", "parameters")
            if function.get(key) is not None
        }
        parameters = declaration.get("parameters")
        if isinstance(parameters, dict):
            declaration["parameters"] = {key: value for key, value in parameters.items() if key != "$schema"}
        declarations.append(declaration)
    return [{"function_declarations": declarations}] if declarations else []


def _tool_choice(choice: str | dict | None) -> dict:
    if isinstance(choice, str):
        mode = {"none": "NONE", "auto": "AUTO", "required": "ANY"}.get(choice)
        return {"function_calling_config": {"mode": mode}} if mode else {}
    if isinstance(choice, dict) and choice.get("type") == "function":
        name = (choice.get("function") or {}).get("name")
        if name:
            return {"function_calling_config": {"mode": "ANY", "allowed_function_names": [name]}}
    return {}


def _configure_images(config: dict, request: OpenAIRequest, settings: dict, profile: dict) -> list[dict]:
    config["response_modalities"] = ["TEXT", "IMAGE"]
    if "nano-banana" in request.model.lower():
        config["safety_settings"].extend(
            types.SafetySetting(category=f"HARM_CATEGORY_{name}", threshold="OFF")
            for name in _IMAGE_SAFETY_CATEGORIES
        )
        config["media_resolution"] = types.MediaResolution.MEDIA_RESOLUTION_HIGH

    image_options = {"image_size": mc.resolve_image_size(request.model, request, settings)}
    aspect_ratio = mc.resolve_aspect_ratio(request.model, request, settings)
    if aspect_ratio:
        image_options["aspect_ratio"] = aspect_ratio
    config["image_config"] = types.ImageConfig(**image_options)
    for key in ("response_mime_type", "response_schema", "response_logprobs", "logprobs"):
        config.pop(key, None)

    # 生图仅保留明确声明且未被禁用的搜索，不下发自定义函数。
    declared_search = any(
        str((tool.get("function") or {}).get("name") or "").lower() in _IMAGE_SEARCH_NAMES
        for tool in request.tools or []
    )
    disabled = isinstance(request.tool_choice, str) and request.tool_choice.lower() == "none"
    return (
        [{"google_search": {}}] if declared_search and not disabled and profile.get("supports_search") else []
    )


def create_generation_config(request: OpenAIRequest) -> dict[str, Any]:
    """请求值优先于控制台默认值，最后按模型能力剥离不支持的参数。"""
    settings = app_state.get_effective_settings(request.model)
    profile = mc.apply_sampling_policy(mc.get_profile(request.model), settings)
    config = _request_parameters(request)
    config.update(_response_format(request))
    # 与 Cookie 通道同口径（OFF=关分类器不回评分；BLOCK_NONE=照常评分不拦截）：
    # 跟着「输出附加安全分」开关走。旧实现 Express 恒发 BLOCK_NONE，评分
    # 返回后又被设置开关丢弃——两通道行为漂移且白做上游评分（2026-10 修复）。
    want_scores = bool(settings.get("safety_score", app_config.SAFETY_SCORE))
    threshold = "BLOCK_NONE" if want_scores else "OFF"
    config["safety_settings"] = [
        types.SafetySetting(category=f"HARM_CATEGORY_{name}", threshold=threshold)
        for name in _TEXT_SAFETY_CATEGORIES
    ]

    system = _system_instruction(request)
    if system is not None and (not profile["is_image"] or settings.get("image_system_instruction", False)):
        config["system_instruction"] = system

    if app_state.get_setting("debug_outbound", False):
        for key in ("presence_penalty", "frequency_penalty"):
            value = getattr(request, key)
            if value:
                print(
                    f"🔎 [出站调试] 已剥离 {key}={value}：新版 Gemini 不接受非零惩罚参数，该值不会发给 Google。"
                )

    if profile["is_image"]:
        tools = _configure_images(config, request, settings, profile)
    else:
        tools = _function_tools(request)
        for key, default_key in (
            ("temperature", "default_temperature"),
            ("top_p", "default_top_p"),
            ("max_output_tokens", "default_max_tokens"),
        ):
            if config.get(key) is None and settings.get(default_key) is not None:
                config[key] = settings[default_key]
        tool_config = _tool_choice(request.tool_choice)
        if tool_config:
            config["tool_config"] = tool_config

    mc.sanitize_sampling(config, profile)
    if tools:
        config["tools"] = tools
    return config
