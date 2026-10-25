"""统一 SDK 与原生 usageMetadata 的 OpenAI 用量映射。"""

import json
import re
import time
from typing import Any

from models import normalize_content_part

# CJK 统一表意文字与全角标点：粗估 1 字 ≈ 1 token；其余按 4 字符 ≈ 1 token。
_CJK_RE = re.compile(r"[\u2e80-\u9fff\uf900-\ufaff\uff00-\uffef]")


def estimate_token_count(text: str) -> int:
    """字符级粗估（无分词器依赖）：CJK 字符逐字计，其余 4 字符折 1 token。"""
    if not text:
        return 0
    cjk = len(_CJK_RE.findall(text))
    return cjk + max(0, (len(text) - cjk + 3) // 4)


def estimate_usage(prompt_text: str, completion_text: str) -> dict[str, Any]:
    """上游未回传 usageMetadata 时的粗估（usage_estimation 开关控制启用）。

    返回值携带 "estimated": true，客户端可区分真实计量与估算。
    """
    prompt = estimate_token_count(prompt_text)
    completion = estimate_token_count(completion_text)
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
        "estimated": True,
    }


def messages_prompt_text(messages: Any) -> str:
    """把 OpenAI messages 里全部文本内容拼成一段（估算 prompt 用量用）。"""
    parts: list[str] = []

    def _walk(content: Any) -> None:
        if content is None:
            return
        if isinstance(content, str):
            parts.append(content)
            return
        if isinstance(content, list):
            for item in content:
                item = normalize_content_part(item)
                if isinstance(item, dict) and item.get("type") == "text":
                    text = item.get("text")
                    if isinstance(text, str):
                        parts.append(text)

    for message in messages or []:
        content = getattr(message, "content", None)
        if content is None and isinstance(message, dict):
            content = message.get("content")
        _walk(content)
        calls = message.get("tool_calls") if isinstance(message, dict) else getattr(message, "tool_calls", None)
        for tc in calls or []:
            fn = (tc or {}).get("function") or {}
            args = fn.get("arguments")
            if isinstance(args, str):
                parts.append(args)
    return "\n".join(parts)


def map_usage(metadata: Any) -> dict[str, int]:
    def count(snake: str, camel: str) -> int:
        value = (
            metadata.get(camel, metadata.get(snake))
            if isinstance(metadata, dict)
            else getattr(metadata, snake, None)
        )
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError, OverflowError):
            return 0

    prompt = count("prompt_token_count", "promptTokenCount")
    prompt += count("tool_use_prompt_token_count", "toolUsePromptTokenCount")
    completion = count("candidates_token_count", "candidatesTokenCount")
    thoughts = count("thoughts_token_count", "thoughtsTokenCount")
    completion += thoughts
    cached = count("cached_content_token_count", "cachedContentTokenCount")
    total = count("total_token_count", "totalTokenCount") or (prompt + completion)
    usage: dict[str, Any] = {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total}
    # 细分字段仅在非零时携带（与 OpenAI 官方同形的增量字段，老客户端可安全忽略）：
    # cached → Anthropic 侧映射 cache_read_input_tokens；thinking → 映射
    # output_tokens_details.thinking_tokens（Claude 生态成本统计依赖这两个细节）。
    if cached:
        usage["prompt_tokens_details"] = {"cached_tokens": cached}
    if thoughts:
        usage["completion_tokens_details"] = {"reasoning_tokens": thoughts}
    return usage


def wants_usage(request_obj: Any) -> bool:
    """客户端是否通过 stream_options.include_usage 要求在流末尾附带用量（P1-8）。"""
    opts = getattr(request_obj, "stream_options", None)
    if opts is None and getattr(request_obj, "model_extra", None):
        opts = request_obj.model_extra.get("stream_options")
    if isinstance(opts, dict):
        return bool(opts.get("include_usage"))
    return False


def make_usage_chunk(response_id: str, model: str, usage: dict) -> str:
    """OpenAI 风格的用量尾块（choices 为空，仅携带 usage）。"""
    chunk = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [],
        "usage": usage,
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
