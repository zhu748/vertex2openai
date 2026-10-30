"""校验上游响应是否被拦截、是否完整，并提供明确的错误类型。"""

from typing import Optional

from schema_validation import SchemaValidationError
from tool_arguments import parse_tool_arguments


# ============================================================
# Gemini finishReason 分类表与 OpenAI 映射
# （Express / Cookie / 抗截断 / Claude 通道共用；旧实现是四份各自维护的
# 近似列表，改动极易漂移，现收敛为这一份单一事实源。）
# ============================================================
# 内容过滤类：命中即映射为 OpenAI content_filter。
FINISH_CONTENT_FILTER = ("SAFETY", "PROHIBITED_CONTENT", "RECITATION", "BLOCKLIST", "SPII", "IMAGE_SAFETY",
                         "MODEL_ARMOR", "IMAGE_PROHIBITED_CONTENT", "IMAGE_RECITATION")
# These are model-generation failures, not completed executable calls or stops.
FINISH_GENERATION_ERRORS = ("MALFORMED_FUNCTION_CALL", "UNEXPECTED_TOOL_CALL", "NO_IMAGE", "IMAGE_OTHER")
# 工具调用类：命中即映射为 OpenAI tool_calls。
FINISH_TOOL_CALLS = ("TOOL_CODE", "FUNCTION_CALL")
# 截断/过滤类：抗截断传输视为「可提前收尾」的白名单。
FINISH_TRUNCATING = ("MAX_TOKENS",) + FINISH_CONTENT_FILTER
# 全部已知合法原因（抗截断白名单的超集）。
FINISH_KNOWN = ("STOP",) + FINISH_TRUNCATING + FINISH_TOOL_CALLS


def validate_generation_finish_reason(reason) -> None:
    value = str(getattr(reason, "value", reason) or "").upper()
    if value in FINISH_GENERATION_ERRORS:
        raise IncompleteResponseError(f"上游生成异常结束：{value}。")


def map_finish_reason_to_openai(
    raw: object, default: Optional[str] = "stop", *, has_tool_calls: bool = False
) -> Optional[str]:
    """Gemini finishReason（SDK 枚举或字符串）→ OpenAI finish_reason。

    未知或缺失的原因返回 default：流式路径传 None 保持缺省，
    非流式与 Cookie 通道传 "stop"。has_tool_calls 将普通完成改为
    tool_calls，截断和内容过滤原因保持不变。
    """
    if hasattr(raw, "name"):
        value = str(raw.name).upper()
    else:
        value = str(raw or "").upper()
    validate_generation_finish_reason(value)
    if value == "STOP":
        return "tool_calls" if has_tool_calls else "stop"
    if value == "MAX_TOKENS":
        return "length"
    if value in FINISH_CONTENT_FILTER:
        return "content_filter"
    if value in FINISH_TOOL_CALLS:
        return "tool_calls"
    return "tool_calls" if has_tool_calls and default == "stop" else default


class PendingMetadataGate:
    """首回复前的元数据有界暂存（Express 与 Cookie 两通道共用）。

    上游在首回复之前可能先发 finish/usage/安全分等元数据帧；这里攒着、
    等首回复到达后按原顺序冲刷。超过 2 MiB 视为上游异常，立即抛
    IncompleteResponseError——既不无界吃内存，也不静默丢弃诊断线索。
    """

    LIMIT_BYTES = 2 * 1024 * 1024

    def __init__(self):
        self.items: list = []
        self._bytes = 0

    def add(self, item, size: int) -> None:
        """暂存一个元数据帧；size 为其编码字节数，超限即抛错（不入列）。"""
        self._bytes += size
        if self._bytes > self.LIMIT_BYTES:
            raise IncompleteResponseError("首回复前的元数据缓冲超过 2 MiB。")
        self.items.append(item)

    def drain(self) -> list:
        """取出并清空已缓冲的元数据（保持原顺序）。"""
        items = self.items
        self.items = []
        self._bytes = 0
        return items


class IncompleteResponseError(ValueError):
    code = 502


class ToolStreamError(IncompleteResponseError):
    """Local tool-fragment validation failure, never evidence of a bad proxy."""


def complete_function_arguments(call) -> dict:
    """Real tool responses must be complete; private text streaming is decoded first."""
    def field(snake, camel=None):
        if isinstance(call, dict):
            return call.get(camel or snake, call.get(snake))
        return getattr(call, snake, None)

    name = field("name")
    if not isinstance(name, str) or not name.strip():
        raise IncompleteResponseError("上游函数调用缺少有效名称。")
    if field("partial_args", "partialArgs") or field("will_continue", "willContinue"):
        raise IncompleteResponseError("上游返回尚未完成的工具参数；当前工具通道需要完整调用。")
    try:
        return parse_tool_arguments(field("args"))
    except SchemaValidationError:
        raise IncompleteResponseError("上游函数调用参数必须是有效的 JSON 对象。") from None


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
    validate_generation_finish_reason(reason)


def require_complete_response(response) -> None:
    for candidate in getattr(response, "candidates", None) or []:
        require_finish_reason(getattr(candidate, "finish_reason", None))


def require_unblocked_response(response) -> None:
    """三种 SDK 交付模式共用的请求拦截检查，统一保留上游原因。"""
    feedback = getattr(response, "prompt_feedback", None)
    reason = getattr(feedback, "block_reason", None)
    value = str(getattr(reason, "value", reason) or "").upper()
    if value and not value.endswith("UNSPECIFIED"):
        message = f"请求被上游安全策略拦截：{reason}"
        detail = getattr(feedback, "block_reason_message", None)
        if detail:
            message += f"（{detail}）"
        raise SafetyBlockedError(message)
