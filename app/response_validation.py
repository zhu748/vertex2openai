"""校验上游响应是否被拦截、是否完整，并提供明确的错误类型。"""

from typing import Optional


# ============================================================
# Gemini finishReason 分类表与 OpenAI 映射
# （Express / Cookie / 抗截断 / Claude 通道共用；旧实现是四份各自维护的
# 近似列表，改动极易漂移，现收敛为这一份单一事实源。）
# ============================================================
# 内容过滤类：命中即映射为 OpenAI content_filter。
FINISH_CONTENT_FILTER = ("SAFETY", "PROHIBITED_CONTENT", "RECITATION", "BLOCKLIST", "SPII", "IMAGE_SAFETY")
# 工具调用类：命中即映射为 OpenAI tool_calls。
FINISH_TOOL_CALLS = ("TOOL_CODE", "FUNCTION_CALL")
# 截断/过滤类：抗截断传输视为「可提前收尾」的白名单。
FINISH_TRUNCATING = ("MAX_TOKENS",) + FINISH_CONTENT_FILTER
# 全部已知合法原因（抗截断白名单的超集）。
FINISH_KNOWN = ("STOP",) + FINISH_TRUNCATING + FINISH_TOOL_CALLS


def map_finish_reason_to_openai(raw: object, default: Optional[str] = "stop") -> Optional[str]:
    """Gemini finishReason（SDK 枚举或字符串）→ OpenAI finish_reason。

    未知或缺失的原因返回 default：流式路径传 None 保持缺省，
    非流式与 Cookie 通道传 "stop"。
    """
    if hasattr(raw, "name"):
        value = str(raw.name).upper()
    else:
        value = str(raw or "").upper()
    if value == "STOP":
        return "stop"
    if value == "MAX_TOKENS":
        return "length"
    if value in FINISH_CONTENT_FILTER:
        return "content_filter"
    if value in FINISH_TOOL_CALLS:
        return "tool_calls"
    return default


class IncompleteResponseError(ValueError):
    code = 502


class AntiTruncationError(ValueError):
    code = 502


class SafetyBlockedError(ValueError):
    """上游拒绝请求内容；三种 SDK 交付模式均使用 400，且不重试。"""

    code = 400


def has_finish_reason(reason) -> bool:
    value = str(getattr(reason, "value", reason) or "").upper()
    return bool(value and not value.endswith("UNSPECIFIED"))


def require_finish_reason(reason) -> None:
    if not has_finish_reason(reason):
        raise IncompleteResponseError("上游连接已结束，但回复缺少结束标记，内容可能不完整。")


def require_complete_response(response) -> None:
    for candidate in getattr(response, "candidates", None) or []:
        require_finish_reason(getattr(candidate, "finish_reason", None))


def require_unblocked_response(response) -> None:
    """三种 SDK 交付模式共用的请求拦截检查，统一保留上游原因。"""
    feedback = getattr(response, "prompt_feedback", None)
    reason = getattr(feedback, "block_reason", None)
    if reason:
        message = f"请求被上游安全策略拦截：{reason}"
        detail = getattr(feedback, "block_reason_message", None)
        if detail:
            message += f"（{detail}）"
        raise SafetyBlockedError(message)
