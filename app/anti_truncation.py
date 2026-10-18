"""Opt-in synthetic text transport for the SDK and Studio channels.

Design references: ken050210/vertex-streaming-anti-truncation and
Xeltra233/Antigravity-gateway (see README for attribution). No tool is executed.
Decode native argument objects directly rather than reparsing OpenAI JSON/SSE.
"""
from dataclasses import dataclass
import json
import re
import secrets
import time

from fastapi.responses import StreamingResponse
from google.genai import types

from models import OpenAIMessage
from response_validation import AntiTruncationError


SUFFIX_MODES = {
    "-antitruncation-nonstream": "nonstream",
    "-antitruncation-stream": "stream",
    "-antitruncation": "stream",  # short alias accepted for manual clients
}


class TransportSelectionError(ValueError):
    code = 400


# 上游结束原因白名单（TextDecoder.finish 两处判定共用；旧实现是两份近似
# 但各自维护的字面量列表，改动极易漂移）：
#   _FINISH_TRUNCATING = 视为“可提前收尾”的截断/内容过滤类原因
#   _FINISH_KNOWN      = 全部已知合法原因（超集）
_FINISH_TRUNCATING = ("MAX_TOKENS", "SAFETY", "PROHIBITED_CONTENT", "RECITATION",
                      "BLOCKLIST", "SPII", "IMAGE_SAFETY")
_FINISH_KNOWN = ("STOP",) + _FINISH_TRUNCATING + ("FUNCTION_CALL", "TOOL_CODE")


def split_model_alias(model: str) -> tuple[str, str | None]:
    """Strip only our suffix; keep the upstream model's original spelling."""
    name = model.strip()
    for suffix, mode in SUFFIX_MODES.items():
        if name.lower().endswith(suffix):
            return name[:-len(suffix)], mode
    return name, None


def supports_transport(model: str) -> bool:
    from model_capabilities import get_profile
    name, _ = split_model_alias(model)
    return (not get_profile(name)["is_image"]
            and not re.search(r"audio|tts|live|embed|video|robotics", name, re.I))


def listed_suffixes(model: str, settings: dict) -> tuple[str, ...]:
    """Only per-model settings publish aliases; a plain ID never opts in."""
    name, _ = split_model_alias(model)
    if name.lower().endswith("-search") or not supports_transport(name):
        return ()
    selected = settings.get("anti_truncation_mode", "off")
    return tuple(suffix for suffix, mode in SUFFIX_MODES.items()
                 if suffix != "-antitruncation" and selected in (mode, "both"))


def supports_argument_stream(model: str) -> bool:
    """Official native stream: Gemini 3+, except 3.1 Flash-Lite."""
    match = re.search(r"gemini-(\d+)", model, re.I)
    return bool(match and int(match[1]) >= 3
                and not re.search(r"gemini-3\.1-flash-lite(?:$|-)", model, re.I)
                and "stream_function_call_arguments" in types.FunctionCallingConfig.model_fields
                and "partial_args" in types.FunctionCall.model_fields)


@dataclass(frozen=True)
class TextTransport:
    tool_name: str
    mode: str
    native_stream: bool = False
    client_tool_names: frozenset[str] = frozenset()

    def decoder(self):
        return TextDecoder(self.tool_name, self.client_tool_names)


def prepare_transport(request, base_model: str, settings: dict, *,
                      search: bool = False, native_stream: bool = False,
                      prefill: bool = False):
    """Called AFTER console injection/prefill so synthetic tools cannot disable them.

    Only an enabled model suffix opts in. Neither console defaults nor the
    legacy request parameter may activate a plain model or override its suffix.
    Genuine tools keep their schemas/signatures; incompatible contracts bypass
    this optional transport instead of silently changing their semantics.
    """
    _, alias_mode = split_model_alias(request.model)
    if alias_mode is None:
        return request, None
    mode = alias_mode
    if f"-antitruncation-{mode}" not in listed_suffixes(base_model, settings):
        raise TransportSelectionError("该模型的抗截断后缀未开启，请先在设置中为此模型开启对应后缀。")
    reason = None
    extra = request.model_extra or {}
    if not supports_transport(base_model):
        reason = "该模型不支持文本函数传输"
    elif search:
        reason = "搜索与合成函数传输不能可靠混用"
    elif request.tool_choice not in (None, "auto"):
        reason = "本轮明确指定了 tool_choice，保留客户端工具选择"
    elif request.response_format and request.response_format.get("type") not in (None, "text"):
        reason = "本轮使用结构化输出"
    elif (request.n not in (None, 1) or extra.get("best_of", 1) != 1):
        reason = "本轮请求多个候选"
    elif extra.get("functions") or extra.get("function_call") or any(
            m.role == "function" or (m.model_extra or {}).get("function_call") for m in request.messages):
        reason = "本轮使用旧版 functions 协议"
    elif any(str((t.get("function") or {}).get("name", "")).lower() in
             {"google_search", "googlesearch", "web_search", "websearch", "search"}
             or t.get("type") != "function" for t in (request.tools or [])):
        reason = "本轮包含内建搜索或非函数工具"
    if reason:
        print(f"ℹ️ [抗截断] 本轮跳过：{reason}。")
        return request, None

    names = {(t.get("function") or {}).get("name") for t in (request.tools or [])}
    for _ in range(8):
        name = "v2o_emit_" + secrets.token_hex(12)
        if name not in names:
            break
    else:
        raise AntiTruncationError("抗截断：无法生成不冲突的临时函数名。")
    tool = {"type": "function", "function": {
        "name": name,
        "description": "Deliver the complete user-visible reply as text. No external action is performed.",
        "parameters": {"type": "object", "properties": {"content": {"type": "string"}},
                       "required": ["content"]},
    }}
    control = (f"Reply transport: put all user-visible answer text in the content string of exactly one "
               f"{name} call. Preserve the requested language, formatting, markup and sections. "
               "This function only delivers text and performs no external action. "
               "Use genuine tools normally when needed; never put their calls inside the content string.")
    if prefill:
        control += (" Continue the existing reply from its exact end. Deliver only new text to append; "
                    "do not repeat or restart the existing prefix. Complete unfinished markup normally.")
    messages = [*request.messages, OpenAIMessage(role="user", content=control)]
    choice = "required" if request.tools else {"type": "function", "function": {"name": name}}
    # Native argument streaming is used only for our one private function. The
    # existing real-tool stream indexer must never see partial genuine tool calls.
    history = any(m.tool_calls or m.role == "tool" for m in request.messages)
    incremental = bool(native_stream and request.stream and mode == "stream"
                       and not request.tools and not history and supports_argument_stream(base_model))
    transport = TextTransport(name, mode, incremental, frozenset(names))
    print(f"🛡️ [抗截断] {base_model} | {mode} | "
          f"{'原生增量参数流' if incremental else '完整函数参数还原'}。")
    return request.model_copy(update={"messages": messages,
                                      "tools": [*(request.tools or []), tool],
                                      "tool_choice": choice}), transport


class TextDecoder:
    """One attempt, one synthetic call; bounded state for native partialArgs.

    Direct text is held until the outcome is known, avoiding duplicate preambles
    when a synthetic call follows. Genuine tool calls and thoughts pass through.
    No upstream signatures from a synthetic call are attached to rewritten text.
    """
    def __init__(self, tool_name: str, client_tool_names=None):
        self.tool_name = tool_name
        self.client_tool_names = client_tool_names
        self.seen = False
        self.closed = False
        self.string_closed = False
        self.pending_high = ""
        self.finish_reason = None
        self.real_tool = False
        self.last_call_owned = False
        self.visible = False
        self.pending_text = []
        self.pending_size = 0

    def _unicode(self, text: str) -> str:
        out = []
        for char in text:
            code = ord(char)
            if self.pending_high:
                high = self.pending_high
                self.pending_high = ""
                if 0xDC00 <= code <= 0xDFFF:
                    out.append(chr(0x10000 + ((ord(high) - 0xD800) << 10) + code - 0xDC00))
                    continue
                raise AntiTruncationError("抗截断：上游返回了无效的 Unicode 代理项。")
            if 0xD800 <= code <= 0xDBFF:
                self.pending_high = char
            elif 0xDC00 <= code <= 0xDFFF:
                raise AntiTruncationError("抗截断：上游返回了无效的 Unicode 代理项。")
            else:
                out.append(char)
        return "".join(out)

    def call(self, name, args, partial=None, will_continue=None) -> str | None:
        if name != self.tool_name and not (not name and self.seen and not self.closed):
            if not name or partial is not None or will_continue:
                raise AntiTruncationError("抗截断：收到无法关联的增量函数调用。")
            if self.client_tool_names is not None and name not in self.client_tool_names:
                raise AntiTruncationError("抗截断：上游返回了未声明的函数调用。")
            self.real_tool = True
            self.last_call_owned = False
            return None
        if self.closed:
            raise AntiTruncationError("抗截断：上游重复调用了正文传输函数。")
        self.seen = True
        self.last_call_owned = True
        self.pending_text.clear()
        self.pending_size = 0
        out = []
        if args is not None and (args or (partial is None and will_continue is not True)):
            if self.string_closed or not isinstance(args, dict) or not isinstance(args.get("content"), str):
                raise AntiTruncationError("抗截断：正文函数的 content 必须是字符串。")
            out.append(self._unicode(args["content"]))
            self.string_closed = True
        for item in partial or []:
            path = getattr(item, "json_path", None)
            value = getattr(item, "string_value", None)
            if self.string_closed or path != "$.content" or not isinstance(value, str):
                raise AntiTruncationError("抗截断：上游增量函数参数的路径或类型不正确。")
            out.append(self._unicode(value))
            if getattr(item, "will_continue", None) is not True:
                self.string_closed = True
        if will_continue is not True:
            if not self.string_closed or self.pending_high:
                raise AntiTruncationError("抗截断：正文函数参数未完整结束。")
            self.closed = True
        text = "".join(out)
        self.visible |= bool(text)
        return text

    def finish(self, reason) -> list:
        reason = getattr(reason, "value", reason)
        reason = str(reason or "").upper()
        if not reason or reason.endswith("UNSPECIFIED"):
            return []
        if self.finish_reason:
            raise AntiTruncationError("抗截断：上游重复发送结束标记。")
        self.finish_reason = reason
        # 结束原因白名单（两处共用一份常量；旧实现两份近似列表各自维护，
        # 改一处漏一处的风险高）：
        #   _FINISH_TRUNCATING = 视为“可提前收尾”的截断/过滤类原因
        #   _FINISH_KNOWN      = 全部已知合法原因
        if self.seen and not self.closed and reason not in _FINISH_TRUNCATING:
            raise AntiTruncationError("抗截断：正文函数未结束，上游提前关闭了回复。")
        if reason not in _FINISH_KNOWN:
            raise AntiTruncationError(f"抗截断：上游回复异常结束（{reason}）。")
        pending = self.pending_text if not self.seen else []
        self.pending_text = []
        return pending

    def validate_end(self):
        if not self.finish_reason:
            raise AntiTruncationError("抗截断：上游连接已结束，但未收到结束标记。")
        if not self.visible and not self.real_tool:
            raise AntiTruncationError("抗截断：上游没有返回正文或真实工具调用。")

    def restore_sdk(self, response):
        candidates = []
        for candidate in response.candidates or []:
            out = []
            for part in getattr(candidate.content, "parts", None) or []:
                if self.finish_reason:
                    raise AntiTruncationError("抗截断：结束标记之后仍有输出。")
                call = part.function_call
                if call is not None:
                    text = self.call(call.name, call.args, getattr(call, "partial_args", None),
                                     getattr(call, "will_continue", None))
                    if text is None:
                        out.append(part)
                    elif text:
                        out.append(types.Part(text=text))
                elif part.text and not part.thought:
                    if not self.seen:
                        self.pending_size += len(part.text)
                        if self.pending_size > 8 * 1024 * 1024:
                            raise AntiTruncationError("抗截断：未按正文函数输出的文本超出缓冲上限。")
                        self.pending_text.append(part)
                elif not part.text and part.thought_signature and self.last_call_owned:
                    # Some endpoints send the function signature in a later,
                    # otherwise empty Part. It still belongs to the hidden call.
                    continue
                else:
                    out.append(part)
            if candidate.finish_reason:
                pending = self.finish(candidate.finish_reason)
                out.extend(pending)
                self.visible |= any(p.text for p in pending)
                self.validate_end()
            content = types.Content(role="model", parts=out)
            candidates.append(candidate.model_copy(update={"content": content}))
        return response.model_copy(update={"candidates": candidates})

    def cookie_event(self, status, data):
        if self.finish_reason and status in ("function_call", "text", "thought", "image"):
            raise AntiTruncationError("抗截断：结束标记之后仍有输出。")
        if status == "function_call":
            text = self.call(data.get("name"), data.get("args"))
            if text is None:
                return [(status, data)]
            return [("text", text)] if text else []
        if status == "text":
            if not self.seen:
                self.pending_size += len(data)
                if self.pending_size > 8 * 1024 * 1024:
                    raise AntiTruncationError("抗截断：未按正文函数输出的文本超出缓冲上限。")
                self.pending_text.append((status, data))
            return []
        if status == "thought_signature":
            if self.last_call_owned and data.get("part_kind") == "signature_only":
                return []
            if not self.seen and self.pending_text:
                self.pending_text.append((status, data))
                return []
        if status == "finish":
            pending = self.finish(data)
            self.visible |= any(kind == "text" and value for kind, value in pending)
            self.validate_end()
            return pending + [(status, data)]
        return [(status, data)]

    async def cookie_stream(self, events):
        try:
            async for status, data in events:
                for event in self.cookie_event(status, data):
                    yield event
            self.validate_end()
        except AntiTruncationError as exc:
            yield "transport_error", str(exc)
        finally:
            close = getattr(events, "aclose", None)
            if close is not None:
                await close()

    def cookie_events(self, events):
        for status, data in events:
            yield from self.cookie_event(status, data)


def buffered_sse(payload: dict, include_usage: bool = False):
    """The client requested SSE, but chose the buffered upstream transport."""
    async def generate():
        base = {"id": payload.get("id"), "object": "chat.completion.chunk",
                "created": payload.get("created", int(time.time())), "model": payload.get("model")}
        for choice in payload.get("choices", []):
            message = dict(choice["message"])
            if message.get("tool_calls"):
                message["tool_calls"] = [dict(call, index=i) for i, call in enumerate(message["tool_calls"])]
            chunk = dict(base, choices=[{"index": choice.get("index", 0), "delta": message,
                                         "finish_reason": None}])
            yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
            chunk = dict(base, choices=[{"index": choice.get("index", 0), "delta": {},
                                         "finish_reason": choice["finish_reason"]}])
            yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
        if include_usage and payload.get("usage"):
            yield f"data: {json.dumps(dict(base, choices=[], usage=payload['usage']))}\n\n"
        yield "data: [DONE]\n\n"
    return StreamingResponse(generate(), media_type="text/event-stream")
