"""校验上游响应是否被拦截、是否完整，并提供明确的错误类型。"""


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
