"""校验上游响应是否被拦截、是否完整，并提供明确的错误类型。"""

from hashlib import blake2s
from typing import Optional

from google.genai.types import Language, Outcome, ToolType

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
FINISH_GENERATION_ERRORS = ("MALFORMED_FUNCTION_CALL", "UNEXPECTED_TOOL_CALL", "TOO_MANY_TOOL_CALLS",
                            "NO_IMAGE", "IMAGE_OTHER", "LANGUAGE", "OTHER")
# CONTINUATION is the SDK's per-request token limit, not a completed generation.
FINISH_LENGTH = ("MAX_TOKENS", "CONTINUATION")
# 工具调用类：命中即映射为 OpenAI tool_calls。
FINISH_TOOL_CALLS = ("TOOL_CODE", "FUNCTION_CALL")
# 截断/过滤类：抗截断传输视为「可提前收尾」的白名单。
FINISH_TRUNCATING = FINISH_LENGTH + FINISH_CONTENT_FILTER
# 全部已知合法原因（抗截断白名单的超集）。
FINISH_KNOWN = ("STOP",) + FINISH_TRUNCATING + FINISH_TOOL_CALLS


def validate_generation_finish_reason(reason) -> None:
    value = str(getattr(reason, "value", reason) or "").upper()
    if value in FINISH_GENERATION_ERRORS:
        raise IncompleteResponseError(f"上游生成异常结束：{value}。")
    if has_finish_reason(value) and value not in FINISH_KNOWN:
        # Fail closed on new upstream terminal values. A partial call must never
        # acquire a successful finish merely because its schema looks complete.
        raise IncompleteResponseError(f"无法映射上游结束原因：{value}。")


def map_finish_reason_to_openai(
    raw: object, default: Optional[str] = "stop", *, has_tool_calls: bool = False
) -> Optional[str]:
    """Gemini finishReason（SDK 枚举或字符串）→ OpenAI finish_reason。

    缺失或 UNSPECIFIED 返回 default；未知的终止原因报错。流式路径传 None 保持缺省，
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
    if value in FINISH_LENGTH:
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


MAX_TOOL_CALLS = 1024


class StreamToolCallIds:
    """Validate complete provider calls in one attempt, isolated by candidate."""
    def __init__(self):
        self._count = 0
        self._seen = set()

    def observe(self, ident: Optional[str], candidate: int = 0) -> None:
        self._count += 1
        if self._count > MAX_TOOL_CALLS:
            raise ToolStreamError("上游流式工具调用数量超过上限。")
        if not ident:
            return  # The adapter generates IDs for omitted values.
        # Do not retain unbounded provider ID strings until the stream ends.
        key = (candidate, blake2s(ident.encode("utf-8", "surrogatepass")).digest())
        if key in self._seen:
            raise ToolStreamError("上游在同一候选回复内重复使用工具调用 ID，无法可靠匹配工具结果。")
        self._seen.add(key)


def require_unique_tool_call_ids(ids) -> None:
    """An executable, complete assistant turn must allow unambiguous result correlation."""
    seen = set()
    for ident in ids:
        if not ident:  # Existing adapters supply their normal fallback only for missing IDs.
            continue
        if ident in seen:
            raise ToolStreamError("上游在同一候选回复内重复使用工具调用 ID，无法可靠匹配工具结果。")
        seen.add(ident)


def sdk_function_call(part, *, active=False):
    """Distinguish a real SDK call (or continuation) from a protobuf default message."""
    call = getattr(part, "function_call", None)
    if call is None:
        return None
    if (getattr(call, "name", None) or getattr(call, "id", None) or getattr(call, "args", None)
            or getattr(call, "partial_args", None) or getattr(call, "will_continue", None)):
        return call  # Real (even malformed) tool data must still be validated.
    if active and not getattr(part, "text", None) and sdk_media_payload(part) is None:
        # An otherwise-empty Part can close the current call; a bare late
        # signature can belong to it too. Neither justifies consuming a
        # separate visible text/thought/media Part as that call's terminator.
        return call
    return None


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


# Use concrete SDK enum values, not placeholder/unknown statuses. A successful
# upstream execution can legitimately have no stdout (likewise for a timeout).
_CODE_LANGUAGES = frozenset(x.value for x in Language if x != Language.LANGUAGE_UNSPECIFIED)
_CODE_OUTCOMES = frozenset(x.value for x in Outcome if x != Outcome.OUTCOME_UNSPECIFIED)


def native_code_part_has_reply(part: dict) -> bool:
    """Observe already generated native content; never interpret or run it."""
    executable = part.get("executableCode")
    if isinstance(executable, dict):
        language, code = executable.get("language"), executable.get("code")
        if (isinstance(language, str) and language in _CODE_LANGUAGES
                and isinstance(code, str) and code and not code.isspace()
                and (executable.get("id") is None or isinstance(executable["id"], str))):
            return True

    result = part.get("codeExecutionResult")
    if isinstance(result, dict):
        outcome = result.get("outcome")
        return (isinstance(outcome, str) and outcome in _CODE_OUTCOMES
                and (result.get("output") is None or isinstance(result["output"], str))
                and (result.get("id") is None or isinstance(result["id"], str)))
    return False


_SERVER_TOOL_TYPES = frozenset(x.value for x in ToolType if x != ToolType.TOOL_TYPE_UNSPECIFIED)


def sdk_media_payload(part):
    """Classify only deliverable SDK media; a Blob/FileData container alone is not output."""
    blob = getattr(part, "inline_data", None)
    data = getattr(blob, "data", None)
    if isinstance(data, (bytes, bytearray, memoryview)) and data:
        return "inline", blob
    file = getattr(part, "file_data", None)
    uri = getattr(file, "file_uri", None)
    if isinstance(uri, str) and uri and not uri.isspace():
        return "file", file
    return None


def native_other_part_has_reply(part: dict) -> bool:
    """Observe native non-text output other than client function calls. No decoding or execution."""
    for key, field in (("inlineData", "data"), ("fileData", "fileUri")):
        media = part.get(key)
        if isinstance(media, dict):
            value = media.get(field)
            if isinstance(value, str) and value and not value.isspace():
                return True
    for key, field in (("toolCall", "args"), ("toolResponse", "response")):
        tool = part.get(key)
        if isinstance(tool, dict):
            kind, payload, identifier = tool.get("toolType"), tool.get(field), tool.get("id")
            if (isinstance(kind, str) and kind in _SERVER_TOOL_TYPES
                    and isinstance(payload, dict) and payload
                    and (identifier is None or isinstance(identifier, str))):
                return True
    return native_code_part_has_reply(part)
