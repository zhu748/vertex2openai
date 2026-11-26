"""The text/function subset of the OpenAI Responses protocol.

The transport deliberately accepts complete responses from the shared Chat
pipeline. It does not implement server-side conversation state or storage.
"""

from dataclasses import dataclass
from itertools import chain
import json
import re
import time
from uuid import uuid4

from json_encoding import estimate_json_chars, iter_json_bytes
from model_capabilities import get_profile
from models import OpenAIMessage, OpenAIRequest, message_text
from ordinary_history import exact_ordinary_slots
from request_tracking import SURROGATE_RE
from response_validation import IncompleteResponseError, MAX_TOOL_CALLS
from schema_validation import SchemaValidationError, validate_tool_strict
from thought_signatures import google_metadata, ordinary_signature, signature_from_extra
from tool_arguments import parse_tool_arguments
from tool_policy import has_search_suffix


MAX_RESPONSE_BYTES = 8 * 1024 * 1024
_MAX_HISTORY_ITEMS = 4096
_MAX_TOOLS = 128
_MAX_TOOL_OUTPUT_PARTS = 128
_MAX_ID = 16 * 1024
_MAX_SIGNATURE = 350_000  # Covers a 256 KiB original signature in Base64.
_FUNCTION_NAME = re.compile(r"[a-zA-Z0-9_-]{1,64}\Z")
_HISTORY_STATUS = (None, "completed", "incomplete")
_TOKEN_STATUS = {"length": "max_output_tokens", "content_filter": "content_filter"}
_KNOWN_FIELDS = frozenset({
    "model", "input", "instructions", "tools", "tool_choice", "parallel_tool_calls",
    "max_output_tokens", "temperature", "top_p", "stream", "store", "background",
    "truncation", "include", "text", "reasoning",
})
_ASSISTANT_EXTRA = "assistant_extra_content"


class ResponsesRequestError(SchemaValidationError):
    def __init__(self, param, message):
        self.param = param
        super().__init__(f"{param}: {message}")


def _field_error(param, message):
    raise ResponsesRequestError(param, message)


def _fields(obj, allowed, param):
    if not isinstance(obj, dict):
        _field_error(param, "must be an object")
    for key, value in obj.items():
        # SDKs sometimes serialize an absent optional field as null.
        if key not in allowed and value is not None:
            _field_error(param, f"unsupported field {ascii(key[:80])}")
    return obj


def _text(value, param, *, nonempty=False, maximum=None):
    if (not isinstance(value, str) or (nonempty and not value)
            or (maximum is not None and len(value) > maximum) or SURROGATE_RE.search(value)):
        _field_error(param, "must be a valid string" + (" and must not be empty" if nonempty else ""))
    return value


def _name(value, param):
    if not isinstance(value, str) or not _FUNCTION_NAME.fullmatch(value):
        _field_error(param, "must be a 1–64 character function name using letters, digits, _ or -")
    return value


def _optional_bool(value, param, default):
    if value is None:
        return default
    if type(value) is not bool:
        _field_error(param, "must be a boolean")
    return value


def _number(value, param, high):
    if value is not None and (type(value) not in (int, float) or not 0 <= value <= high):
        _field_error(param, f"must be a finite number between 0 and {high}")
    return value


def _has_media(text):
    return "![" in text and ("](data:image/" in text or "/v1/generated-images/" in text)


def _text_only(value, param):
    text = _text(value, param)
    # The legacy Chat pipeline interprets these as attached media inside text.
    if _has_media(text):
        _field_error(param, "embedded image attachments are not supported by this endpoint")
    return text


def _call_extra(item, param):
    extra = item.get("extra_content")
    if extra is None:
        return None
    _fields(extra, {"google", "vertex2openai"}, f"{param}.extra_content")
    google = extra.get("google")
    if google is None:
        return None
    _fields(google, {"thought_signature"}, f"{param}.extra_content.google")
    signature = google.get("thought_signature")
    if signature is None:
        return None
    _text(signature, f"{param}.extra_content.google.thought_signature", maximum=_MAX_SIGNATURE)
    # Never manufacture an original out of arbitrary strings or a legacy sentinel.
    raw, _ = signature_from_extra(item, strict_encoding=True)
    encoded = ordinary_signature(raw, wire=True)
    return {"google": {"thought_signature": encoded}} if encoded else None


def _assistant_extra(item, param):
    extra = item.get("extra_content")
    if extra is not None:
        _fields(extra, {"google"}, f"{param}.extra_content")
    return extra


def _valid_assistant_extra(message, extra):
    """Project only genuine ordinary parts that match the visible text.

    In particular, a transport may suppress reasoning from the public reply;
    an extension must not reveal that text or make it proof of replay.
    """
    if not extra:
        return None
    google = google_metadata({"extra_content": extra})
    result = {}
    raw = google.get("ordinary_parts")
    if raw is not None:
        slots = exact_ordinary_slots(message, raw)
        if slots is None:
            return None
        parts = []
        for part, media in slots:
            if media is not None or part["kind"] == "image":
                return None
            clean = {"kind": part["kind"], "text": part["text"]}
            signature = part.get("thought_signature")
            if isinstance(signature, str) and len(signature) <= _MAX_SIGNATURE:
                raw_signature, _ = signature_from_extra(
                    {"extra_content": {"google": {"thought_signature": signature}}}, strict_encoding=True)
                if encoded := ordinary_signature(raw_signature, wire=True):
                    clean["thought_signature"] = encoded
            if part["kind"] == "signature_only" and "thought_signature" not in clean:
                return None  # Dropping/reindexing a signed position would invent topology.
            parts.append(clean)
        result["ordinary_parts"] = parts
    order = google.get("part_order")
    calls = message.get("tool_calls") or ()
    if order is not None:
        count = len(raw or ()) + len(calls)
        seen = set()
        if not isinstance(order, list) or len(order) != count:
            return None
        for slot in order:
            if not isinstance(slot, dict) or slot.keys() != {"type", "index"}:
                return None
            kind, index = slot["type"], slot["index"]
            if (kind not in ("ordinary", "tool_call") or type(index) is not int
                    or not 0 <= index < len((raw or ()) if kind == "ordinary" else calls)
                    or (kind, index) in seen):
                return None
            seen.add((kind, index))
        result["part_order"] = order
    sig, kind = google.get("thought_signature"), google.get("thought_signature_part")
    # When a standalone carrier has no exact Part sequence, honor only a
    # signature position that the actual public history will be able to replay.
    if isinstance(sig, str) and len(sig) <= _MAX_SIGNATURE and (
            kind in (None, "text", "signature_only") or (kind == "thought" and message.get("reasoning_content"))):
        raw_signature, _ = signature_from_extra({"extra_content": extra}, strict_encoding=True)
        if encoded := ordinary_signature(raw_signature, wire=True):
            result["thought_signature"] = encoded
            if kind is not None:
                result["thought_signature_part"] = kind
    return {"google": result} if result else None


def _message_content(value, role, param):
    if isinstance(value, str):
        return _text_only(value, param)
    if not isinstance(value, list) or not value or len(value) > _MAX_HISTORY_ITEMS:
        _field_error(param, "must be a string or a nonempty array of text parts")
    parts = []
    for i, part in enumerate(value):
        path = f"{param}[{i}]"
        _fields(part, {"type", "text", "annotations", "logprobs", "refusal"}, path)
        kind = part.get("type")
        if kind in ("input_text", "output_text") and (kind == "input_text" or role == "assistant"):
            # Annotations on a replayed assistant message describe client
            # presentation rather than instructions; only text enters Gemini.
            parts.append({"type": "text", "text": _text_only(part.get("text"), f"{path}.text")})
        elif kind == "refusal" and role == "assistant":
            parts.append({"type": "refusal", "refusal": _text(part.get("refusal"), f"{path}.refusal")})
        else:
            _field_error(path, "unsupported content type; only text and assistant refusal are supported")
    if len(parts) == 1 and parts[0]["type"] == "text":
        # Exact same native part as a Chat scalar, including in origin proofs.
        return parts[0]["text"]
    return parts


def _function_output(value, param):
    if isinstance(value, str):
        return _text(value, param)
    if not isinstance(value, list) or len(value) > _MAX_TOOL_OUTPUT_PARTS:
        _field_error(param, f"must be a string or an array of at most {_MAX_TOOL_OUTPUT_PARTS} input_text parts")
    parts = []
    for index, part in enumerate(value):
        path = f"{param}[{index}]"
        _fields(part, {"type", "text"}, path)
        if part.get("type") != "input_text":
            _field_error(path, "only input_text is supported in a function output")
        # The native adapters retain each text block as ordered data in
        # FunctionResponse.response. Unlike assistant text, tool text is
        # not parsed for Markdown image attachments.
        parts.append({"type": "text", "text": _text(part.get("text"), f"{path}.text")})
    return parts


class _History:
    def __init__(self, instructions):
        self.messages = ([OpenAIMessage(role="developer", content=instructions)] if instructions else [])
        self.assistant = {}
        self.extra = None
        self.pending = set()
        self.in_results = False
        self.conversation = False

    def _flush(self):
        if not self.assistant:
            return
        extra = _valid_assistant_extra(self.assistant, self.extra)
        if extra:
            self.assistant["extra_content"] = extra
        message = OpenAIMessage(**self.assistant)
        if message.tool_calls and isinstance(message.content, list):
            # The SDK concatenates assistant tool-preamble text before
            # parsing Markdown; a media marker can cross block boundaries.
            _text_only(message_text(message.replay_content()), "input assistant content")
        # Responses call_id never encoded a signature. Keep all IDs opaque; a
        # missing origin must not create a new legacy placeholder fallback.
        message._opaque_tool_call_ids = bool(message.tool_calls)
        self.messages.append(message)
        self.assistant, self.extra = {}, None
        self.conversation = True

    def _stage(self):
        if not self.assistant:
            self.assistant = {"role": "assistant"}

    def _require_results(self, param):
        if self.pending:
            _field_error(param, "each function call must have a matching function_call_output before the next turn")

    def add(self, item, param):
        if not isinstance(item, dict):
            _field_error(param, "must be an input item object")
        kind = item.get("type", "message")
        if item.get("id") is not None:
            _text(item["id"], f"{param}.id", nonempty=True, maximum=_MAX_ID)
        if kind == "function_call_output":
            _fields(item, {"type", "id", "status", "call_id", "output"}, param)
            call_id = _text(item.get("call_id"), f"{param}.call_id", nonempty=True, maximum=_MAX_ID)
            if call_id not in self.pending:
                _field_error(f"{param}.call_id", "does not match an unanswered call in the immediately preceding batch")
            if item.get("status") not in (None, "completed"):
                _field_error(f"{param}.status", "function outputs must be complete")
            output = _function_output(item.get("output"), f"{param}.output")
            self._flush()
            self.pending.remove(call_id)
            self.in_results = True
            self.messages.append(OpenAIMessage(role="tool", tool_call_id=call_id, content=output))
            return

        if self.in_results or kind != "function_call":
            self._require_results(param)
        self.in_results = False
        if kind == "function_call":
            _fields(item, {"type", "id", "status", "call_id", "name", "arguments", "extra_content"}, param)
            if item.get("status") not in (None, "completed"):
                _field_error(f"{param}.status", "historical calls must be complete")
            self._stage()
            call_id = _text(item.get("call_id"), f"{param}.call_id", nonempty=True, maximum=_MAX_ID)
            if call_id in self.pending or len(self.pending) >= MAX_TOOL_CALLS:
                _field_error(f"{param}.call_id", "duplicate ID or too many calls in this batch")
            name = _name(item.get("name"), f"{param}.name")
            arguments = _text(item.get("arguments"), f"{param}.arguments", nonempty=True)
            if not arguments.strip():
                _field_error(f"{param}.arguments", "must contain a JSON object")
            parse_tool_arguments(arguments, label=f"{param}.arguments")
            call = {"id": call_id, "type": "function", "function": {"name": name, "arguments": arguments}}
            if extra := _call_extra(item, param):
                call["extra_content"] = extra
            bridge = (item.get("extra_content") or {}).get("vertex2openai")
            if bridge is not None:
                _fields(bridge, {_ASSISTANT_EXTRA}, f"{param}.extra_content.vertex2openai")
                if carried := bridge.get(_ASSISTANT_EXTRA):
                    if self.extra is not None or self.assistant.get("tool_calls"):
                        _field_error(f"{param}.extra_content", "assistant metadata must be carried only once")
                    self.extra = _assistant_extra({"extra_content": carried}, param)
            self.assistant.setdefault("tool_calls", []).append(call)
            self.pending.add(call_id)
        elif kind == "message":
            _fields(item, {"type", "id", "status", "role", "content", "extra_content"}, param)
            role = item.get("role")
            if role not in ("user", "system", "developer", "assistant"):
                _field_error(f"{param}.role", "must be user, system, developer, or assistant")
            if item.get("status") not in _HISTORY_STATUS:
                _field_error(f"{param}.status", "input message is not complete enough to replay")
            content = _message_content(item.get("content"), role, f"{param}.content")
            if role != "assistant":
                if item.get("extra_content") is not None:
                    _field_error(f"{param}.extra_content", "only assistant output may carry this extension")
                self._flush()
                self.messages.append(OpenAIMessage(role=role, content=content))
                self.conversation |= role == "user" and bool(content)
            else:
                if "content" in self.assistant:
                    self._flush()
                self._stage()
                self.assistant["content"] = content
                if extra := _assistant_extra(item, param):
                    if self.extra is not None:
                        _field_error(f"{param}.extra_content", "assistant metadata must be carried only once")
                    self.extra = extra
        elif kind == "reasoning":
            _fields(item, {"type", "id", "status", "content", "summary", "extra_content"}, param)
            if item.get("status") not in _HISTORY_STATUS or item.get("summary") not in (None, []):
                _field_error(param, "only this gateway's visible reasoning can be replayed")
            content = item.get("content")
            if not isinstance(content, list) or not 0 < len(content) <= _MAX_HISTORY_ITEMS:
                _field_error(f"{param}.content", "must contain visible reasoning_text")
            texts = []
            for index, part in enumerate(content):
                path = f"{param}.content[{index}]"
                _fields(part, {"type", "text"}, path)
                if part.get("type") != "reasoning_text":
                    _field_error(path, "expected a reasoning_text part")
                texts.append(_text_only(part.get("text"), f"{path}.text"))
            # A new reasoning item starts a new assistant turn; the gateway's
            # projection puts optional reasoning before its message and calls.
            self._flush()
            self._stage()
            self.assistant["reasoning_content"] = "".join(texts)
            self.extra = _assistant_extra(item, param)
        else:
            _field_error(f"{param}.type", "unsupported item; use full message and function history")

    def finish(self):
        self._require_results("input")
        self._flush()
        if not self.conversation:
            _field_error("input", "must include conversational content")
        return self.messages


@dataclass
class ResponsesRequest:
    chat: OpenAIRequest
    base: dict
    stream: bool


def parse_responses_request(body):
    body = _fields(body, _KNOWN_FIELDS, "body")
    model = _text(body.get("model"), "model", nonempty=True, maximum=1024)
    if not model.strip() or model != model.strip():
        _field_error("model", "must be a nonempty model name without surrounding whitespace")
    if get_profile(model)["is_image"] or has_search_suffix(model):
        _field_error("model", "this endpoint supports text/function models, not image or search aliases")

    store = _optional_bool(body.get("store"), "store", False)
    if store:
        _field_error("store", "must be false; this gateway does not store Responses")
    if _optional_bool(body.get("background"), "background", False):
        _field_error("background", "background mode is not supported")
    if body.get("include") not in (None, []):
        _field_error("include", "extra output inclusions (including encrypted_content) are not supported")
    if body.get("truncation") not in (None, "disabled"):
        _field_error("truncation", "automatic truncation is not supported")
    if (text := body.get("text")) is not None:
        _fields(text, {"format"}, "text")
        fmt = text.get("format")
        if fmt is not None and _fields(fmt, {"type"}, "text.format").get("type") != "text":
            _field_error("text.format", "only type text is supported")
    effort = None
    if (reasoning := body.get("reasoning")) is not None:
        _fields(reasoning, {"effort", "summary"}, "reasoning")
        effort = reasoning.get("effort")
        if effort not in (None, "none", "minimal", "low", "medium", "high", "xhigh"):
            _field_error("reasoning.effort", "unsupported reasoning effort")
        if reasoning.get("summary") is not None:
            _field_error("reasoning.summary", "OpenAI reasoning summaries are not supported")
    instructions = body.get("instructions")
    if instructions is not None:
        instructions = _text(instructions, "instructions")
    history = _History(instructions)
    items = body.get("input")
    if isinstance(items, str):
        history.add({"role": "user", "content": items}, "input")
    elif isinstance(items, list) and 0 < len(items) <= _MAX_HISTORY_ITEMS:
        for i, item in enumerate(items):
            history.add(item, f"input[{i}]")
    else:
        _field_error("input", "must be a string or a nonempty array with full explicit history")
    messages = history.finish()

    tools = body.get("tools")
    if tools is None:
        tools = []
    if not isinstance(tools, list) or len(tools) > _MAX_TOOLS:
        _field_error("tools", f"must be an array of at most {_MAX_TOOLS} function declarations")
    allowed = set()
    public_tools, chat_tools = [], []
    for i, tool in enumerate(tools):
        param = f"tools[{i}]"
        _fields(tool, {"type", "name", "description", "parameters", "strict"}, param)
        if tool.get("type") != "function":
            _field_error(param, "only type function declarations are supported")
        name = _name(tool.get("name"), f"{param}.name")
        if name in allowed:
            _field_error(f"{param}.name", "duplicate function declaration")
        allowed.add(name)
        validate_tool_strict(tool.get("strict"), f"{param}.strict")
        function = {"name": name, "strict": False}
        if tool.get("description") is not None:
            function["description"] = _text(tool["description"], f"{param}.description")
        if (parameters := tool.get("parameters")) is not None:
            if not isinstance(parameters, dict):
                _field_error(f"{param}.parameters", "must be a JSON Schema object")
            function["parameters"] = parameters
        chat_tools.append({"type": "function", "function": function})
        public_tools.append({"type": "function", "parameters": None, **function})

    choice = body.get("tool_choice")
    if choice is None:
        choice = "auto"
    chat_choice = choice
    if isinstance(choice, dict):
        _fields(choice, {"type", "name"}, "tool_choice")
        if choice.get("type") != "function":
            _field_error("tool_choice", "only named function selection is supported")
        choice_name = _name(choice.get("name"), "tool_choice.name")
        if choice_name not in allowed:
            _field_error("tool_choice.name", "must reference a declared function")
        chat_choice = {"type": "function", "function": {"name": choice_name}}
        choice = {"type": "function", "name": choice_name}
    elif choice not in ("auto", "none", "required"):
        _field_error("tool_choice", "must be auto, none, required, or a named function")
    elif choice == "required" and not allowed:
        _field_error("tool_choice", "required needs a declared function")
    parallel = _optional_bool(body.get("parallel_tool_calls"), "parallel_tool_calls", True)
    stream = _optional_bool(body.get("stream"), "stream", False)
    maximum = body.get("max_output_tokens")
    if maximum is not None and (type(maximum) is not int or not 16 <= maximum <= 2**31 - 1):
        _field_error("max_output_tokens", "must be an integer between 16 and 2147483647")
    temperature = _number(body.get("temperature"), "temperature", 2)
    top_p = _number(body.get("top_p"), "top_p", 1)

    chat = OpenAIRequest(
        model=model, messages=messages, tools=chat_tools, tool_choice=chat_choice,
        parallel_tool_calls=parallel, temperature=temperature, top_p=top_p,
        max_completion_tokens=maximum, stream=False,
        **({"reasoning_effort": effort} if effort is not None else {}),
    )
    base = {
        "id": "resp_" + uuid4().hex, "object": "response", "model": model, "created_at": int(time.time()),
        "completed_at": None, "status": "in_progress", "output": [], "usage": None, "error": None,
        "incomplete_details": None, "instructions": instructions, "max_output_tokens": maximum,
        "parallel_tool_calls": parallel, "previous_response_id": None, "store": False, "background": False,
        "temperature": temperature, "top_p": top_p, "text": {"format": {"type": "text"}},
        "tool_choice": choice, "tools": public_tools, "truncation": "disabled", "metadata": {},
        "reasoning": {"effort": effort, "summary": None} if effort is not None else None,
    }
    return ResponsesRequest(chat, base, stream)


def _usage(value):
    if not isinstance(value, dict):
        return None
    def count(obj, key, default):
        val = obj.get(key) if isinstance(obj, dict) else None
        return val if type(val) is int and val >= 0 else default

    prompt, completion = count(value, "prompt_tokens", None), count(value, "completion_tokens", None)
    if prompt is None or completion is None:
        return None
    details = value.get("prompt_tokens_details")
    usage = {
        "input_tokens": prompt, "output_tokens": completion,
        "total_tokens": count(value, "total_tokens", prompt + completion),
        "input_tokens_details": {
            "cached_tokens": count(details, "cached_tokens", 0),
            # The Responses schema requires an integer. Zero means the
            # gateway has no writes to report, not a Gemini billing claim.
            "cache_write_tokens": count(details, "cache_write_tokens", 0),
        },
        "output_tokens_details": {"reasoning_tokens": count(value.get("completion_tokens_details"), "reasoning_tokens", 0)},
    }
    if value.get("estimated") is True:
        usage["estimated"] = True
    return usage


def _completion_message(message):
    if not isinstance(message, dict) or message.get("role") != "assistant":
        raise IncompleteResponseError("Upstream completion lacks an assistant message.")
    for field in ("content", "reasoning_content", "refusal"):
        value = message.get(field)
        if value is not None and (not isinstance(value, str) or SURROGATE_RE.search(value)):
            raise IncompleteResponseError("Upstream completion contains unsupported assistant content.")
        if value and _has_media(value):
            raise IncompleteResponseError("Image output is not supported by this Responses endpoint.")
    return message


def completion_to_response(completion, base):
    """Return the public Response plus an exact tool-origin choice, if any."""
    choices = completion.get("choices") if isinstance(completion, dict) else None
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise IncompleteResponseError("Upstream completion must contain one candidate.")
    choice = choices[0]
    finish = choice.get("finish_reason")
    if finish not in ("stop", "tool_calls", "length", "content_filter"):
        raise IncompleteResponseError("Upstream completion lacks a supported finish reason.")
    message = _completion_message(choice.get("message"))
    incomplete = _TOKEN_STATUS.get(finish)
    status = "incomplete" if incomplete else "completed"
    output, public_turn = [], dict(message)
    public_turn.pop("extra_content", None)
    visible_owner = None
    reasoning = message.get("reasoning_content")
    if reasoning:
        visible_owner = {"id": "rs_" + uuid4().hex, "type": "reasoning", "status": status,
                         "summary": [], "content": [{"type": "reasoning_text", "text": reasoning}]}
        output.append(visible_owner)
    text, refusal = message.get("content"), message.get("refusal")
    content = []
    if text and text != refusal:
        content.append({"type": "output_text", "text": text, "annotations": [], "logprobs": []})
    if refusal:
        content.append({"type": "refusal", "refusal": refusal})
    if content:
        visible_owner = {"id": "msg_" + uuid4().hex, "type": "message", "role": "assistant",
                         "status": status, "content": content}
        output.append(visible_owner)
    calls = message.get("tool_calls") or []
    tool_items, proof_calls = [], []
    withheld = False
    if not incomplete:
        if not isinstance(calls, list) or len(calls) > MAX_TOOL_CALLS or bool(calls) != (finish == "tool_calls"):
            raise IncompleteResponseError("Upstream completion does not contain a consistent tool batch.")
        choice_mode = base["tool_choice"]
        if (choice_mode == "none" and calls or not calls and (choice_mode == "required" or isinstance(choice_mode, dict))):
            raise IncompleteResponseError("Upstream completion did not honor tool_choice.")
        names = {tool["name"] for tool in base["tools"]}
        if isinstance(choice_mode, dict):
            names = {choice_mode["name"]}
        ids = set()
        for call in calls:
            try:
                if call["type"] != "function":
                    raise ValueError()
                call_id = _text(call["id"], "call_id", nonempty=True, maximum=_MAX_ID)
                name = _name(call["function"]["name"], "name")
                arguments = _text(call["function"]["arguments"], "arguments", nonempty=True)
                if call_id in ids or name not in names or not arguments.strip():
                    raise ValueError()
                parse_tool_arguments(arguments)
                extra = _call_extra(call, "function_call")
            except (KeyError, TypeError, ValueError, RecursionError):
                raise IncompleteResponseError("Upstream completion contains an invalid or undeclared function call.") from None
            ids.add(call_id)
            item = {"id": "fc_" + uuid4().hex, "type": "function_call", "status": "completed",
                    "call_id": call_id, "name": name, "arguments": arguments}
            proof_call = {"id": call_id, "type": "function", "function": {"name": name, "arguments": arguments}}
            if extra:
                item["extra_content"] = proof_call["extra_content"] = extra
            elif google_metadata(call).get("thought_signature"):
                withheld = True  # A discarded carrier is not evidence of an unsigned slot.
            tool_items.append(item)
            proof_calls.append(proof_call)
        if not output and not tool_items:
            raise IncompleteResponseError("Upstream completion does not contain a deliverable output item.")

    source_extra = message.get("extra_content")
    extra = _valid_assistant_extra(message, source_extra) if not incomplete else None
    withheld |= bool(source_extra and source_extra != extra)
    if extra:
        public_turn["extra_content"] = extra
        if visible_owner:
            visible_owner["extra_content"] = extra
        elif tool_items:
            # When the generation contains only calls, there is no ordinary
            # public item to hold e.g. a native signature-only Part.
            tool_items[0].setdefault("extra_content", {})["vertex2openai"] = {_ASSISTANT_EXTRA: extra}
            # Item extension and private per-call projection cannot alias.
            if "extra_content" in proof_calls[0]:
                proof_calls[0]["extra_content"] = {"google": proof_calls[0]["extra_content"]["google"]}
    output.extend(tool_items)
    output_envelope = {
        **base, "status": status, "completed_at": int(time.time()) if status == "completed" else None,
        "output": output, "usage": _usage(completion.get("usage")),
        "incomplete_details": {"reason": incomplete} if incomplete else None,
    }
    # A delivered tool batch with an unreplayable source can look exactly
    # like an older valid reply to a client that drops extensions. Its public
    # identity can veto guessing, but it must not authorize any carrier.
    # Refusal blocks are not representable by the shared plain-history proof.
    proof = None
    if proof_calls and not refusal:
        public_turn["tool_calls"] = proof_calls
        proof = {"finish_reason": finish, "message": public_turn, "withheld": withheld}
    return output_envelope, proof


_SSE_SLICE = 16 * 1024


def _sse_fragments(prefix, payload):
    # The bounded JSON encoder yields punctuation, keys and numbers as
    # independent fragments. Pack tiny fragments into ASGI writes without
    # copying a large encoded string or combining independent SSE events.
    pending = bytearray(prefix)
    for fragment in chain(iter_json_bytes(payload, compact=True, allow_nan=False), (b"\n\n",)):
        if len(pending) + len(fragment) > _SSE_SLICE:
            if pending:
                yield bytes(pending)
                pending.clear()
            if len(fragment) >= _SSE_SLICE:
                yield fragment
                continue
        pending.extend(fragment)
    if pending:
        yield bytes(pending)


class ResponsesEvents:
    """Serialize checked state without collecting frames or giant SSE byte strings."""

    def __init__(self):
        self.sequence = 0

    def event(self, kind, **values):
        payload = {"type": kind, "sequence_number": self.sequence, **values}
        self.sequence += 1
        prefix = f"event: {kind}\ndata: ".encode("ascii")
        if estimate_json_chars(payload) <= _SSE_SLICE:
            yield prefix + json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                                      allow_nan=False).encode("utf-8") + b"\n\n"
        else:
            yield from _sse_fragments(prefix, payload)

    def _deltas(self, kind, text, fields, **extra):
        for start in range(0, len(text), _SSE_SLICE):
            yield from self.event(kind, delta=text[start:start + _SSE_SLICE], **fields, **extra)

    def output(self, response):
        for index, item in enumerate(response["output"]):
            kind = item["type"]
            added = {key: value for key, value in item.items() if key != "extra_content"}
            added["status"] = "in_progress"
            if kind == "function_call":
                added["arguments"] = ""
            else:
                added["content"] = []
            yield from self.event("response.output_item.added", output_index=index, item=added)
            fields = {"item_id": item["id"], "output_index": index}
            if kind == "function_call":
                name, arguments = item["name"], item["arguments"]
                yield from self._deltas("response.function_call_arguments.delta", arguments, fields)
                yield from self.event("response.function_call_arguments.done", arguments=arguments, name=name, **fields)
            else:
                for position, part in enumerate(item["content"]):
                    fields["content_index"] = position
                    part_kind = part["type"]
                    key = "refusal" if part_kind == "refusal" else "text"
                    text = part[key]
                    yield from self.event("response.content_part.added", part={**part, key: ""}, **fields)
                    extra = {"logprobs": []} if part_kind == "output_text" else {}
                    yield from self._deltas(f"response.{part_kind}.delta", text, fields, **extra)
                    yield from self.event(f"response.{part_kind}.done", **{key: text}, **fields, **extra)
                    yield from self.event("response.content_part.done", part=part, **fields)
            yield from self.event("response.output_item.done", output_index=index, item=item)
        yield from self.event(f"response.{response['status']}", response=response)
