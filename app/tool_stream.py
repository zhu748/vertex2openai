"""Validate native tool fragments and optionally emit OpenAI argument deltas.

SDK/native paths assemble complete calls by default. The Cookie OpenAI path
streams append-only JSON prefixes, validates completion, and must not replay
a request after exposing the first tool delta.
"""

import ast
import base64
import json
from collections import deque
from dataclasses import dataclass, field
import math
import re

from google.genai import types
from pydantic import ValidationError

from response_validation import (
    IncompleteResponseError,
    ToolStreamError,
    complete_function_arguments,
    has_finish_reason,
)

MAX_TOOL_STREAM_BYTES = 32 * 1024 * 1024
MAX_TOOL_CALLS = 1024
MAX_ARGUMENT_FIELDS = 65536
_PATH_TOKEN = re.compile(r'''\.([^.[\]]+)|\[(0|[1-9][0-9]*)\]|\[("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')\]''')
_SURROGATES = re.compile("[\ud800-\udfff]")


def _fail(message):
    raise ToolStreamError(f"上游工具参数流：{message}")


def _complete_arguments(call):
    try:
        return complete_function_arguments(call)
    except IncompleteResponseError as error:
        raise ToolStreamError(str(error)) from error


def _wire_sdk_fields(model, value):
    known = {field_name for name, info in model.model_fields.items()
             for field_name in (name, info.alias)}
    # __typename belongs to the GraphQL envelope, not the generated arguments.
    # Unknown non-null fields still fail explicitly instead of losing semantics.
    return {key: item for key, item in value.items()
            if key in known or (key != "__typename" and item is not None)}


def _wire_function_call(fc):
    """Adapt ProtoJSON wire enums before the SDK's narrower literal schema."""
    fields = _wire_sdk_fields(types.FunctionCall, fc)
    for key in ("partialArgs", "partial_args"):
        fragments = fields.get(key)
        if not isinstance(fragments, list):
            continue  # Let SDK validation report the field/type, without its value.
        normalized = []
        for fragment in fragments:
            if isinstance(fragment, dict):
                fragment = _wire_sdk_fields(types.PartialArg, fragment)
                for null_key in ("nullValue", "null_value"):
                    value = fragment.get(null_key)
                    if type(value) is int and value == 0:
                        fragment[null_key] = "NULL_VALUE"
            normalized.append(fragment)
        fields[key] = normalized
    return types.FunctionCall.model_validate(fields)


def _wire_validation_error(error, prefix):
    # Never include SDK error messages or input values: these can contain the
    # generated reply, tool arguments, signatures or a private field name.
    safe_fields = {field_name for model in (types.FunctionCall, types.PartialArg)
                   for name, info in model.model_fields.items()
                   for field_name in (name, info.alias)} | {"thought"}
    details = []
    for problem in error.errors(include_input=False, include_context=False, include_url=False)[:3]:
        location = ".".join(str(item) if isinstance(item, int) or item in safe_fields
                            else "unknown_field" for item in problem["loc"])
        details.append(f"{prefix}{location} ({problem['type']})")
    _fail("函数调用分片字段无效：" + "; ".join(details) + "。")


def _normalize_string(value):
    if not _SURROGATES.search(value):
        return value
    try:
        return value.encode("utf-16-le", errors="surrogatepass").decode("utf-16-le")
    except UnicodeError:
        _fail("字符串包含未配对的 Unicode 代理项。")


def _path_tokens(path):
    # Native partialArgs use singular JSONPaths, never selectors or wildcards.
    if not isinstance(path, str) or not path.startswith("$") or len(path) > 8192:
        _fail("参数路径无效。")
    result, offset = [], 1
    while offset < len(path):
        match = _PATH_TOKEN.match(path, offset)
        if not match:
            _fail("参数路径不是受支持的单值 JSONPath。")
        name, index, quoted = match.groups()
        try:
            token = name if name is not None else int(index) if index is not None else ast.literal_eval(quoted)
        except (ValueError, SyntaxError):
            _fail("参数名的引号或转义无效。")
        if name is not None and ("*" in name or not name.isidentifier()):
            _fail("特殊参数名必须使用括号引号路径。")
        if isinstance(token, int) and token >= MAX_ARGUMENT_FIELDS:
            _fail("数组索引超过上限。")
        result.append(token)
        if len(result) > 64:
            _fail("参数嵌套超过上限。")
        offset = match.end()
    if not result or not isinstance(result[0], str):
        _fail("函数参数必须是对象。")
    return result


@dataclass
class _String:
    parts: list = field(default_factory=list)
    open: bool = True


class _Node(dict):
    def __init__(self):
        super().__init__()
        self.order = []

    def __setitem__(self, key, value):
        if key not in self:
            self.order.append(key)
        super().__setitem__(key, value)


class _JsonPrefix:
    """Append-only JSON: stream strings, keep mutable containers open until end.

    A later JSONPath may add siblings to any container. Never guess that a
    nested container has closed. Subsequent outer siblings wait for completion,
    while the first nested string/array can still stream without reserializing
    its already-sent prefix on every fragment.
    """
    def __init__(self, root):
        self.stack = [{"value": root, "position": 0, "opened": False}]

    def read(self, *, final=False):
        output = []
        while self.stack:
            frame = self.stack[-1]
            value = frame["value"]
            if isinstance(value, _Node):
                array = bool(value) and isinstance(value.order[0], int)
                if not frame["opened"]:
                    output.append("[" if array else "{")
                    frame["opened"] = True
                position = frame["position"]
                if array:
                    key = position if position in value else None
                else:
                    key = value.order[position] if position < len(value.order) else None
                if key is None:
                    if not final:
                        break
                    output.append("]" if array else "}")
                    self.stack.pop()
                    continue
                if position:
                    output.append(",")
                if not array:
                    output.append(json.dumps(_normalize_string(key), ensure_ascii=False) + ":")
                frame["position"] += 1
                self.stack.append({"value": value[key], "position": 0, "opened": False})
            elif isinstance(value, _String):
                if not frame["opened"]:
                    output.append('"')
                    frame["opened"] = True
                for fragment in value.parts[frame["position"]:]:
                    # Normal Chinese/emoji stay UTF-8. Only split surrogate
                    # fragments need ASCII escapes until their pair arrives.
                    output.append(json.dumps(fragment, ensure_ascii=bool(_SURROGATES.search(fragment)))[1:-1])
                frame["position"] = len(value.parts)
                if value.open:
                    break
                output.append('"')
                self.stack.pop()
            else:
                try:
                    output.append(json.dumps(value, ensure_ascii=True, allow_nan=False))
                except ValueError:
                    _fail("数值参数必须是有限数。")
                self.stack.pop()
        return "".join(output)


@dataclass
class ToolStreamEvent:
    kind: str
    data: dict


class _Arguments:
    def __init__(self):
        self.root = _Node()
        self.fields = 0
        self._last_path = None
        self._last_tokens = ()

    def add(self, item):
        if self._last_tokens and item.json_path == self._last_path:
            tokens = self._last_tokens
        else:
            tokens = self._last_tokens = tuple(_path_tokens(item.json_path))
            self._last_path = item.json_path
        node = self.root
        for token in tokens[:-1]:
            self._check_key(node, token)
            if token not in node:
                self._new_field()
                node[token] = _Node()
            node = node[token]
            if not isinstance(node, dict):
                _fail("参数路径与已有值冲突。")
        key = tokens[-1]
        self._check_key(node, key)
        previous = node.get(key)
        values = [(name, getattr(item, name)) for name in
                  ("string_value", "number_value", "bool_value")
                  if getattr(item, name) is not None]
        # GraphQL can return null for every unselected oneof field. Such nulls
        # are defaults, not a second value alongside string/number/bool. A
        # path-only string terminator can contain these same defaults.
        if item.null_value is not None or (not values and "null_value" in item.model_fields_set
                and not (isinstance(previous, _String) and previous.open)):
            values.append(("null_value", None))
        if len(values) > 1:
            _fail("一个参数分片包含多个值类型。")
        if not values:
            # Google also closes a streamed string with a path-only fragment.
            if not isinstance(previous, _String) or not previous.open or item.will_continue:
                _fail("无值分片没有对应的未结束字符串。")
            previous.open = False
            return
        kind, value = values[0]
        if key not in node:
            self._new_field()
        if kind == "string_value":
            if key in node and (not isinstance(previous, _String) or not previous.open):
                _fail("已完成的参数被重复写入。")
            string = previous if isinstance(previous, _String) else _String()
            if value:
                string.parts.append(value)
            string.open = item.will_continue is True
            node[key] = string
        else:
            if key in node or item.will_continue:
                _fail("标量参数重复或包含无效的续传标记。")
            node[key] = None if kind == "null_value" else value

    def _new_field(self):
        self.fields += 1
        if self.fields > MAX_ARGUMENT_FIELDS:
            _fail("参数字段数超过上限。")

    @staticmethod
    def _check_key(node, key):
        if node and isinstance(next(iter(node)), int) != isinstance(key, int):
            _fail("同一路径混用了数组和对象。")

    def finish(self):
        def materialize(value):
            if isinstance(value, _String):
                if value.open:
                    _fail("字符串尚未结束，函数已结束。")
                return _normalize_string("".join(value.parts))
            if isinstance(value, dict):
                if value and isinstance(next(iter(value)), int):
                    if sorted(value) != list(range(len(value))):
                        _fail("数组存在缺失元素。")
                    return [materialize(value[i]) for i in range(len(value))]
                return {_normalize_string(key): materialize(item) for key, item in value.items()}
            if isinstance(value, float) and not math.isfinite(value):
                _fail("数值参数必须是有限数。")
            return value
        # Fold only strings that need it. Serializing the entire argument tree
        # through ASCII JSON expands Chinese text sixfold and duplicates it.
        return materialize(self.root)


@dataclass
class _Call:
    part: object
    arguments: _Arguments = field(default_factory=_Arguments)
    complete: object = None
    stream_index: int = 0
    writer: object = None
    id_sent: bool = False


class ToolStreamAssembler:
    """Per-attempt state; IDs, signatures and interleaved Part order survive."""

    def __init__(self, *, emit_deltas=False):
        self.emit_deltas = emit_deltas
        self.active = {}
        self.queues = {}
        self.ids = {}
        self.bytes = 0
        self.calls = 0

    @staticmethod
    def _header(queue, index, fc):
        data = {"index": index, "name": fc.name, "arguments": ""}
        if fc.id:
            data["id"] = fc.id
        queue.append(ToolStreamEvent("tool_delta", data))

    @staticmethod
    def _end(queue, index, part, id_sent):
        fc = part.function_call
        queue.append(ToolStreamEvent("tool_end", {"index": index, "id_sent": id_sent,
            "name": fc.name, "id": fc.id, "args": fc.args or {},
            "thought_signature": base64.b64encode(part.thought_signature).decode() if part.thought_signature else None}))

    def _account(self, part):
        self.bytes += len(repr(part).encode("utf-8", errors="surrogatepass"))
        if self.bytes > MAX_TOOL_STREAM_BYTES:
            _fail("缓冲超过字节上限。")

    def _feed(self, index, part):
        fc = part.function_call
        active = self.active.setdefault(index, {})
        queue = self.queues.setdefault(index, deque())
        seen_ids = self.ids.setdefault(index, set())
        call = active.get(fc.id) if fc.id else None
        if call is None and (not fc.id or None in active):
            matches = [value for key, value in active.items()
                       if (not fc.id or key is None) and
                       (not fc.name or value.part.function_call.name == fc.name)]
            if len(matches) > 1:
                _fail("缺少调用 ID，无法关联并行分片。")
            call = matches[0] if matches else None
        if call is None:
            if not fc.name or not fc.name.strip():
                _fail("分片没有对应的函数名称。")
            if fc.id and fc.id in seen_ids:
                _fail("调用 ID 被重复使用。")
            if fc.id is None and active:
                _fail("缺少调用 ID，无法开始另一条并行调用。")
            self.calls += 1
            if self.calls > MAX_TOOL_CALLS:
                _fail("调用数量超过上限。")
            if fc.id:
                seen_ids.add(fc.id)
            if not fc.partial_args and fc.will_continue is not True:
                _complete_arguments(fc)
                if self.emit_deltas:
                    index = self.calls - 1
                    self._header(queue, index, fc)
                    queue.append(ToolStreamEvent("tool_delta", {"index": index,
                        "arguments": json.dumps(fc.args or {}, ensure_ascii=False, allow_nan=False)}))
                    self._end(queue, index, part, bool(fc.id))
                else:
                    queue.append(part)
                return
            call = _Call(part)
            active[fc.id] = call
            if self.emit_deltas:
                call.stream_index = self.calls - 1
                call.writer = _JsonPrefix(call.arguments.root)
                self._header(queue, call.stream_index, fc)
                call.id_sent = bool(fc.id)
            else:
                queue.append(call)
        else:
            original = call.part.function_call
            if fc.name and fc.name != original.name:
                _fail("调用中途改变了函数名称。")
            if fc.id and original.id and fc.id != original.id:
                _fail("调用中途改变了 ID。")
            if fc.id and not original.id:
                if fc.id in seen_ids:
                    _fail("调用 ID 被重复使用。")
                del active[None]
                seen_ids.add(fc.id)
                active[fc.id] = call
                call.part = call.part.model_copy(update={"function_call": original.model_copy(update={"id": fc.id})})
            if part.thought_signature:
                if call.part.thought_signature and call.part.thought_signature != part.thought_signature:
                    _fail("同一调用包含冲突的思考签名。")
                call.part = call.part.model_copy(update={"thought_signature": part.thought_signature})
        if fc.args:
            _fail("增量调用混入了完整参数对象。")
        for item in fc.partial_args or []:
            call.arguments.add(item)
        if self.emit_deltas and fc.id and not call.id_sent:
            queue.append(ToolStreamEvent("tool_delta", {"index": call.stream_index, "id": fc.id, "arguments": ""}))
            call.id_sent = True
        if fc.will_continue is not True:
            try:
                args = call.arguments.finish()
            except (ValueError, UnicodeError, RecursionError):
                _fail("参数不是有效的 JSON 对象。")
            complete = call.part.function_call.model_copy(update={
                "args": args, "partial_args": None, "will_continue": None})
            _complete_arguments(complete)
            call.complete = call.part.model_copy(update={"function_call": complete})
            del active[complete.id]
            if self.emit_deltas:
                suffix = call.writer.read(final=True)
                if suffix:
                    queue.append(ToolStreamEvent("tool_delta", {"index": call.stream_index, "arguments": suffix}))
                self._end(queue, call.stream_index, call.complete, call.id_sent)
        elif self.emit_deltas:
            prefix = call.writer.read()
            if prefix:
                queue.append(ToolStreamEvent("tool_delta", {"index": call.stream_index, "arguments": prefix}))

    def restore_sdk(self, response):
        candidates = []
        for ci, candidate in enumerate(response.candidates or []):
            index = candidate.index if candidate.index is not None else ci
            queue = self.queues.setdefault(index, deque())
            for part in getattr(candidate.content, "parts", None) or []:
                if part.function_call is not None:
                    self._account(part)
                    self._feed(index, part)
                else:
                    if queue:
                        self._account(part)
                    queue.append(part)
            if has_finish_reason(candidate.finish_reason) and self.active.get(index):
                _fail("函数参数未完成，上游已结束回复。")
            parts = self._drain(queue)
            content = candidate.content.model_copy(update={"parts": parts}) if candidate.content else None
            candidates.append(candidate.model_copy(update={"content": content}))
        return response.model_copy(update={"candidates": candidates})

    @staticmethod
    def _drain(queue):
        parts = []
        while queue:
            value = queue[0]
            if isinstance(value, _Call):
                if value.complete is None:
                    break
                value = value.complete
            parts.append(value)
            queue.popleft()
        return parts

    def restore_wire_parts(self, parts):
        """Cookie keeps ordinary wire Parts intact; only calls use SDK types.

        A true result denotes real tool-generation progress, never permission
        to execute an incomplete call. Empty proto defaults on text/signature
        Parts must not accidentally close an in-flight function.
        """
        queue = self.queues.setdefault(0, deque())
        progress = False
        for part in parts:
            if not isinstance(part, dict):
                _fail("Part 必须是对象。")
            fc = part.get("functionCall")
            if fc is not None and not isinstance(fc, dict):
                _fail("functionCall 必须是对象。")
            meaningful = fc and any(fc.get(key) for key in ("name", "id", "args", "partialArgs", "willContinue"))
            closing = (fc is not None and self.active.get(0)
                       and not any(part.get(key) for key in ("text", "thoughtSignature", "inlineData", "fileData")))
            if meaningful or closing:
                self._account(part)
                signature = part.get("thoughtSignature")
                if signature:
                    if not isinstance(signature, str):
                        _fail("思考签名必须是字符串。")
                    try:
                        signature = base64.b64decode(signature, validate=True)
                    except ValueError:
                        signature = signature.encode("utf-8")
                try:
                    call = _wire_function_call(fc)
                except ValidationError as error:
                    _wire_validation_error(error, "functionCall.")
                try:
                    # A GraphQL Part includes unrelated oneof defaults and
                    # private metadata. Only the call and its annotations use
                    # SDK types; ordinary wire Parts remain untouched.
                    native_part = types.Part(function_call=call, thought=part.get("thought"),
                                             thought_signature=signature or None)
                except ValidationError as error:
                    _wire_validation_error(error, "")
                self._feed(0, native_part)
                progress |= bool(call.name or call.partial_args)
            else:
                if queue:
                    self._account(part)
                queue.append(part)
        ready = self._drain(queue)
        return [part if isinstance(part, (dict, ToolStreamEvent)) else part.model_dump(mode="json", by_alias=True, exclude_none=True)
                for part in ready], progress

    def validate_end(self):
        if any(self.active.values()) or any(self.queues.values()):
            _fail("连接在函数参数完成前关闭。")
