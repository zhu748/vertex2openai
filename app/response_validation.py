"""Completion markers distinguish a complete reply from a quiet broken stream."""


class IncompleteResponseError(ValueError):
    code = 502


def has_finish_reason(reason) -> bool:
    value = str(getattr(reason, "value", reason) or "").upper()
    return bool(value and not value.endswith("UNSPECIFIED"))


def require_finish_reason(reason) -> None:
    if not has_finish_reason(reason):
        raise IncompleteResponseError("上游连接已结束，但回复缺少结束标记，内容可能不完整。")


def require_complete_response(response) -> None:
    for candidate in getattr(response, "candidates", None) or []:
        require_finish_reason(getattr(candidate, "finish_reason", None))
