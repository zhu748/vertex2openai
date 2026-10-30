"""Merge official SDK tool-argument fragments into complete calls (Key path).

Express/API-Key streaming can deliver function-call arguments as partialArgs
fragments. This module validates them, assembles each call back into a
complete functionCall before delivery, and preserves IDs, thought signatures
and interleaved Part order. The Cookie private endpoint never sends these
fragments (verified 2026-10-30: only complete tool objects arrive), so the
Cookie channel parses complete calls directly and does not use this module.
"""

import ast
from collections import deque
from dataclasses import dataclass, field
import math
import re

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
                _complete_arguments(fc)
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
            _complete_arguments(complete)
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

    def validate_end(self):
        if any(self.active.values()) or any(self.queues.values()):
            _fail("连接在函数参数完成前关闭。")
