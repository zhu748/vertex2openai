"""Bounded incremental JSON object framing for both upstream wire protocols."""

import io
import json
import re

from response_validation import IncompleteResponseError


MAX_JSON_OBJECT_BYTES = 64 * 1024 * 1024
_OBJECT_TOKENS = re.compile(r'["{}\\]')
_DOCUMENT_TOKENS = re.compile(r'["{}\[\],\\]')


class JsonObjects:
    """Decode each object once, after scanning its boundaries across chunks.

    Cookie batchGraphql allows XSSI prefixes and consecutive objects. Native
    REST requires one complete object or array; document=True also validates
    its separators and closing bracket without retaining the whole response.
    """

    def __init__(self, limit=MAX_JSON_OBJECT_BYTES, *, document=False):
        self.limit = limit
        self.document = document
        self.depth = 0
        self.quoted = False
        self.escaped = False
        self.size = 0
        self.buffer = io.StringIO()
        self._array = False
        self._state = "start"

    def _append(self, fragment):
        # ASCII 快速路径：json_stream 消费的 fragment 来自 aiter_text()，
        # 绝大多数是 ASCII（JSON 结构 + base64）；isascii() 是 O(1) 检查，
        # 免去为计长把整个 fragment 再编码一遍的临时 bytes 分配。
        # 非 ASCII（中文正文等）仍用精确字节长度维持字节上限语义。
        self.size += len(fragment) if fragment.isascii() else len(fragment.encode("utf-8"))
        if self.size > self.limit:
            raise IncompleteResponseError("上游单个 JSON 对象超过字节上限。")
        self.buffer.write(fragment)

    def _separator(self, token):
        if self._state == "start" and token == "[":
            self._array, self._state = True, "first"
        elif self._state in ("start", "first", "value") and token == "{":
            return
        elif self._state in ("first", "separator") and token == "]":
            self._state = "end"
        elif self._state == "separator" and token == ",":
            self._state = "value"
        else:
            raise IncompleteResponseError("上游 JSON 文档格式错误。")

    def feed(self, chunk):
        start = 0 if self.depth else None
        outside = None if self.depth else 0
        skip_until = 1 if self.escaped else 0
        self.escaped = False
        tokens = _DOCUMENT_TOKENS if self.document else _OBJECT_TOKENS
        for match in tokens.finditer(chunk):
            position, token = match.start(), match.group()
            if position < skip_until:
                continue
            if not self.depth:
                if self.document:
                    if chunk[outside:position].strip(" \t\r\n"):
                        raise IncompleteResponseError("上游 JSON 文档格式错误。")
                    self._separator(token)
                    outside = match.end()
                if token == "{":
                    start, self.depth = position, 1
                continue
            if self.quoted:
                if token == "\\":
                    skip_until = position + 2
                elif token == '"':
                    self.quoted = False
            elif token == '"':
                self.quoted = True
            elif token == "{":
                self.depth += 1
            elif token == "}":
                self.depth -= 1
                if not self.depth:
                    self._append(chunk[start:match.end()])
                    try:
                        obj = json.loads(self.buffer.getvalue())
                    except (json.JSONDecodeError, RecursionError) as exc:
                        raise IncompleteResponseError("上游返回无效的 JSON 对象。") from exc
                    self.buffer = io.StringIO()
                    self.size, start = 0, None
                    self._state = "separator" if self._array else "end"
                    outside = match.end()
                    yield obj
        if start is not None:
            self._append(chunk[start:])
        elif self.document and chunk[outside:].strip(" \t\r\n"):
            raise IncompleteResponseError("上游 JSON 文档格式错误。")
        self.escaped = self.quoted and skip_until > len(chunk)

    def finish(self):
        if self.depth:
            raise IncompleteResponseError("上游 JSON 对象尚未完整结束，连接已断开。")
        if self.document and self._state != "end":
            raise IncompleteResponseError("上游 JSON 文档尚未完整结束，连接已断开。")
