"""统一 SDK 与原生 usageMetadata 的 OpenAI 用量映射。"""

import json
import time
from typing import Any


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
