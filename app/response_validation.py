"""校验上游响应是否被拦截、是否完整，并提供明确的错误类型。"""

from hashlib import blake2s
from math import isfinite
from typing import Optional

from google.genai.types import BlockedReason, Language, Outcome, ToolType

from schema_validation import SchemaValidationError
from tool_arguments import MAX_ARGUMENT_FIELDS, parse_tool_arguments, partial_argument_path


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


class ResponsesProjectionError(IncompleteResponseError):
    """A provider result that the text/function projection cannot publish; not a network failure."""


class ResponsesMediaError(ResponsesProjectionError):
    """A real provider media Part cannot be represented by this text/function endpoint."""

    def __init__(self):
        super().__init__("Media output is not supported by this Responses endpoint.")


# These native payloads are separate Part kinds, not metadata on text or on a
# client function call. Their signatures cannot be replayed on a blank Part.
_RESPONSES_NATIVE_ARMS = (
    ("executableCode", "executable_code", (("language", None, "LANGUAGE_UNSPECIFIED"), ("code", None, ""))),
    ("codeExecutionResult", "code_execution_result", (("outcome", None, "OUTCOME_UNSPECIFIED"), ("output", None, ""))),
    ("toolCall", "tool_call", (("toolType", "tool_type", "TOOL_TYPE_UNSPECIFIED"), ("args", None, {}))),
    ("toolResponse", "tool_response", (("toolType", "tool_type", "TOOL_TYPE_UNSPECIFIED"), ("response", None, {}))),
    ("functionResponse", "function_response", (("name", None, ""), ("response", None, {}),
        ("willContinue", "will_continue", False), ("scheduling", None, "SCHEDULING_UNSPECIFIED"))),
)


def _part_fields(source, wire, attribute):
    if isinstance(source, dict):
        value = source.get(wire)
        return (value, source[attribute]) if attribute and attribute in source else (value,)
    return (getattr(source, attribute or wire, None),)


def _nondefault_payload(source, wire, attribute, default):
    for value in _part_fields(source, wire, attribute):
        if value is None:
            continue
        if default is False:
            if value is not False:
                return True
        elif value != default and not (isinstance(default, str) and value == ""):
            return True
    return False


def _response_result_media(value):
    for parts in _part_fields(value, "parts", None):
        if parts is None:
            continue
        if not isinstance(parts, list):
            return True
        for part in parts:
            if isinstance(value, dict) and not isinstance(part, dict):
                return True
            for wire, attr, field, sdk_field, default in (
                    ("inlineData", "inline_data", "data", None, "" if isinstance(part, dict) else b""),
                    ("fileData", "file_data", "fileUri", "file_uri", "")):
                for media in _part_fields(part, wire, attr):
                    if (media is not None and isinstance(part, dict) and not isinstance(media, dict)
                            or _nondefault_payload(media, field, sdk_field, default)):
                        return True
    return False


def require_responses_supported_part(part) -> None:
    """Reject unsupported source data before projecting any carrier or tool batch.

    SDK Parts and Studio JSON have the same relevant fields. An empty/null
    message, zero enum, empty map or empty media shell is not a payload.
    """
    for wire, attribute, fields in _RESPONSES_NATIVE_ARMS:
        for value in _part_fields(part, wire, attribute):
            if value is None:
                continue
            if (isinstance(part, dict) and not isinstance(value, dict)
                    or _nondefault_payload(value, "id", None, "")
                    or any(_nondefault_payload(value, *field) for field in fields)
                    or wire == "functionResponse" and _response_result_media(value)):
                raise ResponsesProjectionError("Native tool or code output is not supported by this Responses endpoint.")


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
        # separate text/thought/media Part as that call's terminator.
        if (getattr(part, "text", None) == "" and getattr(part, "thought", None) is True
                and getattr(part, "thought_signature", None)):
            # An explicit signed empty thought is still its own typed Part.
            # The default FunctionCall shell proves no tool ownership.
            return None
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


_UNSPECIFIED_BLOCK = BlockedReason.BLOCKED_REASON_UNSPECIFIED.value
_KNOWN_PROMPT_BLOCKS = frozenset(reason.value for reason in BlockedReason if reason.value != _UNSPECIFIED_BLOCK)


def responses_prompt_block_reason(reason) -> str | None:
    """Only recognized provider feedback can become a typed Responses filter."""
    value = getattr(reason, "value", reason)
    if value is None or value == "":
        return None
    if not isinstance(value, str) or len(value) > 128 or not value.isascii():
        raise IncompleteResponseError("上游 promptFeedback.blockReason 无效。")
    value = value.upper()
    if value == _UNSPECIFIED_BLOCK:
        return None
    # The pinned SDK itself allows unknown enum values. Don't infer either a
    # refusal or a normal completion from an unrecognized explicit status.
    if value not in _KNOWN_PROMPT_BLOCKS:
        raise IncompleteResponseError("上游 promptFeedback.blockReason 无法识别。")
    return value


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


_PARTIAL_ARMS = (("boolValue", "bool_value"), ("numberValue", "number_value"),
                 ("stringValue", "string_value"), ("nullValue", "null_value"))


def _native_partial_delta(arg):
    if not isinstance(arg, dict):
        return None
    if any(camel in arg and snake in arg for camel, snake in
           (("jsonPath", "json_path"), ("willContinue", "will_continue"))):
        return None
    kind, value = None, None
    for camel, snake in _PARTIAL_ARMS:
        if camel in arg:
            if snake in arg:
                return None  # ambiguous aliases even when both values are null
            raw = arg[camel]
        elif snake in arg:
            raw = arg[snake]
        else:
            continue
        # Ordinary ProtoJSON null fields are unset. NullValue is the exception:
        # its explicitly present JSON null selects that oneof arm.
        if raw is None and camel != "nullValue":
            continue
        if kind is not None:
            return None
        kind, value = camel, raw
    if kind is None:
        return None
    continued = arg.get("willContinue", arg.get("will_continue"))
    if continued is not None and (type(continued) is not bool or (continued and kind != "stringValue")):
        return None
    # Keep the NullValue enum spelling emitted by SDKs; neither bool nor zero selects it.
    if not (kind == "boolValue" and type(value) is bool
            or kind == "numberValue" and (type(value) is int or type(value) is float and isfinite(value))
            or kind == "stringValue" and isinstance(value, str)
            or kind == "nullValue" and (value is None or value == "NULL_VALUE")):
        return None
    try:
        path = partial_argument_path(arg.get("jsonPath", arg.get("json_path")))
    except ValueError:
        return None
    return kind, path


class ResponsesTextCallOwner:
    """Identify one private text call without borrowing an unnamed public sibling's arguments."""
    def __init__(self, name):
        self.name = name
        self.open = self.saw_other_fragment = False
        self.ident = None

    def owns(self, name, ident, continuing, fragmented):
        private = name == self.name
        if name and self.open and ident and self.ident and (ident == self.ident) != private:
            raise ResponsesProjectionError("上游正文传输调用的标识发生冲突。")
        if not name and self.open:
            if ident and self.ident:
                private = ident == self.ident
            elif self.saw_other_fragment:
                raise ResponsesProjectionError("上游增量参数无法确定归属。")
            else:
                private = True
        if private:
            self.ident = self.ident or ident
            self.open = continuing is True
        elif fragmented:
            # Conservatively remember ambiguity, but retain no public IDs or fragment bodies.
            self.saw_other_fragment = True
        return private


def responses_partial_call(call) -> bool:
    """Recognize a real, non-executable argument fragment; malformed data is not truncation evidence.

    Test the same protobuf fields on typed SDK Parts and their Cookie JSON equivalent.
    Only the candidate's separate, verified terminal can authorize discarding this batch.
    """
    if isinstance(call, dict):
        def get(key, camel=None):
            if camel and camel in call and key in call:
                raise ResponsesProjectionError("上游增量调用包含重复字段。")
            return call.get(key, call.get(camel))
    else:
        def get(key, camel=None):
            return getattr(call, key, None)

    partial = get("partial_args", "partialArgs")
    continuing = get("will_continue", "willContinue")
    if ((partial is not None and not isinstance(partial, list))
            or (continuing is not None and type(continuing) is not bool)):
        raise ResponsesProjectionError("上游增量调用字段类型无效。")
    if not partial and not continuing:
        return False
    name, ident, args = get("name"), get("id"), get("args")
    if (name is not None and not isinstance(name, str) or ident is not None and not isinstance(ident, str)
            or not (name and name.strip() or ident or partial) or args is not None and args != {}
            or len(partial or ()) > MAX_ARGUMENT_FIELDS):
        raise ResponsesProjectionError("上游增量调用的归属或参数无效。")
    for arg in partial or ():
        # Preserve explicit nullValue as distinct from an absent oneof arm.
        wire = arg.model_dump(by_alias=True, exclude_unset=True) if callable(getattr(arg, "model_dump", None)) else arg
        if _native_partial_delta(wire) is None:
            raise ResponsesProjectionError("上游增量参数缺少有效的路径或唯一类型字段。")
    return True


def _native_partial_args_have_reply(items: list) -> bool:
    # A real non-null delta is independently observable as before. Newly
    # admitting an all-null batch is narrower: every fragment must have an
    # unambiguous typed owner and a different singular path. Do not retain a
    # second copy of every parsed path just to mark progress. A hash collision
    # can only suppress a witness; it never credits malformed input.
    seen_nulls = set()
    invalid_null_batch = False
    for item in items:
        if (delta := _native_partial_delta(item)) is None:
            invalid_null_batch = True
            continue
        kind, path = delta
        if kind != "nullValue":
            return True
        if not invalid_null_batch:
            key = hash(path)
            if len(seen_nulls) >= MAX_ARGUMENT_FIELDS or key in seen_nulls:
                invalid_null_batch = True
            else:
                seen_nulls.add(key)
    return bool(seen_nulls) and not invalid_null_batch


def native_function_call_has_reply(part: dict) -> bool:
    """Observe a named call or actual incremental argument, not a proto shell."""
    call = part.get("functionCall", part.get("function_call"))
    if (not isinstance(call, dict) or "functionCall" in part and "function_call" in part
            or "partialArgs" in call and "partial_args" in call
            or "willContinue" in call and "will_continue" in call):
        return False
    name, call_id, args = call.get("name"), call.get("id"), call.get("args")
    partial = call.get("partialArgs", call.get("partial_args"))
    continued = call.get("willContinue", call.get("will_continue"))
    if any(value is not None and not isinstance(value, kind) for value, kind in (
            (name, str), (call_id, str), (args, dict), (partial, list), (continued, bool))):
        return False
    return bool(name and not name.isspace()) or bool(partial and _native_partial_args_have_reply(partial))


def _native_media_has_reply(part: dict) -> bool:
    for key, field in (("inlineData", "data"), ("fileData", "fileUri")):
        media = part.get(key)
        if isinstance(media, dict):
            value = media.get(field)
            if isinstance(value, str) and value and not value.isspace():
                return True
    return False


def _native_function_response_has_reply(part: dict) -> bool:
    result = part.get("functionResponse", part.get("function_response"))
    if not isinstance(result, dict):
        return False
    name, call_id = result.get("name"), result.get("id")
    if any(value is not None and not isinstance(value, str) for value in (name, call_id)):
        return False
    if not any(value and not value.isspace() for value in (name, call_id)):
        return False
    if isinstance(result.get("response"), dict):
        return True
    parts = result.get("parts")
    return isinstance(parts, list) and any(isinstance(p, dict) and _native_media_has_reply(p) for p in parts)


def native_other_part_has_reply(part: dict) -> bool:
    """Observe native non-text output other than client function calls. No decoding or execution."""
    if _native_media_has_reply(part) or _native_function_response_has_reply(part):
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


def native_part_has_reply(part: object) -> bool:
    if not isinstance(part, dict):
        return False
    text = part.get("text")
    return (isinstance(text, str) and bool(text)
            or native_function_call_has_reply(part) or native_other_part_has_reply(part))
