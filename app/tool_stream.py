"""Assemble native tool fragments after first-reply progress is observed.

The deadline watches raw SDK chunks. Clients still receive complete, validated
calls, never an empty executable call for a native name-only stream header.
"""

import ast
import base64
from collections import deque
from dataclasses import dataclass, field
import json
import re

from google.genai import types
from pydantic import ValidationError

from response_validation import (
    IncompleteResponseError,
    complete_function_arguments,
    has_finish_reason,
)

MAX_TOOL_STREAM_BYTES = 32 * 1024 * 1024
MAX_TOOL_CALLS = 1024
MAX_ARGUMENT_FIELDS = 65536
_PATH_TOKEN = re.compile(r'''\.([^.[\]]+)|\[(0|[1-9][0-9]*)\]|\[("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')\]''')


def _fail(message):
    raise IncompleteResponseError(f"上游工具参数流：{message}")


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


class _Arguments:
    def __init__(self):
        self.root = {}
        self.fields = 0

    def add(self, item):
        tokens = _path_tokens(item.json_path)
        node = self.root
        for token in tokens[:-1]:
            self._check_key(node, token)
            if token not in node:
                self._new_field()
                node[token] = {}
            node = node[token]
            if not isinstance(node, dict):
                _fail("参数路径与已有值冲突。")
        key = tokens[-1]
        self._check_key(node, key)
        values = [(name, getattr(item, name)) for name in
                  ("string_value", "number_value", "bool_value", "null_value")
                  if getattr(item, name) is not None or
                  (name == "null_value" and name in item.model_fields_set)]
        if len(values) > 1:
            _fail("一个参数分片包含多个值类型。")
        previous = node.get(key)
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
                return "".join(value.parts)
            if isinstance(value, dict):
                if value and isinstance(next(iter(value)), int):
                    if sorted(value) != list(range(len(value))):
                        _fail("数组存在缺失元素。")
                    return [materialize(value[i]) for i in range(len(value))]
                return {key: materialize(item) for key, item in value.items()}
            return value
        # Normalize UTF-16 surrogate pairs split across native string fragments.
        return json.loads(json.dumps(materialize(self.root), ensure_ascii=True, allow_nan=False))


@dataclass
class _Call:
    part: object
    arguments: _Arguments = field(default_factory=_Arguments)
    complete: object = None


class ToolStreamAssembler:
    """Per-attempt state; IDs, signatures and interleaved Part order survive."""

    def __init__(self):
        self.active = {}
        self.queues = {}
        self.ids = {}
        self.bytes = 0
        self.calls = 0

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
                complete_function_arguments(fc)
                queue.append(part)
                return
            call = _Call(part)
            active[fc.id] = call
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
        if fc.will_continue is not True:
            try:
                args = call.arguments.finish()
            except (ValueError, UnicodeError, RecursionError):
                _fail("参数不是有效的 JSON 对象。")
            complete = call.part.function_call.model_copy(update={
                "args": args, "partial_args": None, "will_continue": None})
            complete_function_arguments(complete)
            call.complete = call.part.model_copy(update={"function_call": complete})
            del active[complete.id]

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
                    call = types.FunctionCall.model_validate(fc)
                    native_part = types.Part.model_validate({
                        **part, "functionCall": call, "thoughtSignature": signature or None})
                except ValidationError:
                    _fail("函数调用分片字段无效。")
                self._feed(0, native_part)
                progress |= bool(call.name or call.partial_args)
            else:
                if queue:
                    self._account(part)
                queue.append(part)
        ready = self._drain(queue)
        return [part if isinstance(part, dict) else part.model_dump(mode="json", by_alias=True, exclude_none=True)
                for part in ready], progress

    def validate_end(self):
        if any(self.active.values()) or any(self.queues.values()):
            _fail("连接在函数参数完成前关闭。")
