"""统一 SDK 与原生 usageMetadata 的 OpenAI 用量映射。"""
from typing import Any


def map_usage(metadata: Any) -> dict[str, int]:
    def count(snake: str, camel: str) -> int:
        value = metadata.get(camel, metadata.get(snake)) if isinstance(metadata, dict) else getattr(metadata, snake, None)
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError, OverflowError):
            return 0

    prompt = count("prompt_token_count", "promptTokenCount")
    prompt += count("tool_use_prompt_token_count", "toolUsePromptTokenCount")
    completion = count("candidates_token_count", "candidatesTokenCount")
    completion += count("thoughts_token_count", "thoughtsTokenCount")
    total = count("total_token_count", "totalTokenCount") or (prompt + completion)
    return {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total}
