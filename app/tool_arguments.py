"""Validation shared by complete tool arguments and singular incremental argument paths."""

import json
import re

from schema_validation import SchemaValidationError


def parse_tool_arguments(value, *, label="tool arguments") -> dict:
    """Missing/empty arguments mean {}; malformed input must never become {}."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return {}
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        if not isinstance(parsed, dict):
            raise SchemaValidationError(f"{label} must be a JSON object")
        # Also rejects non-finite numbers accepted by Python's JSON decoder,
        # cycles and non-JSON values in callers' already-decoded objects.
        json.dumps(parsed, allow_nan=False, ensure_ascii=False).encode("utf-8")
    except SchemaValidationError:
        raise
    except (TypeError, ValueError, UnicodeError, RecursionError, OverflowError):
        raise SchemaValidationError(f"{label} must contain a valid, finite JSON object") from None
    return parsed


MAX_ARGUMENT_FIELDS = 65536
# RFC 9535 §2.5.1.1: non-ASCII scalar values are allowed regardless of
# Unicode category. Names are compared literally, without normalization.
_NAME_FIRST = r"A-Za-z_\x80-\ud7ff\ue000-\U0010ffff"
_PATH_TOKEN = re.compile(rf'''\.([{_NAME_FIRST}][{_NAME_FIRST}0-9]*)|\[(0|[1-9][0-9]*)\]|\[("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')\]''')


def _decode_quoted_name(literal: str) -> str:
    """RFC 9535 §2.3.1.1: JSON escapes, with a matching escaped delimiter."""
    if literal.startswith("'"):
        def json_quote(match: re.Match) -> str:
            token = match.group()
            if token == r"\'":
                return "'"
            if token == '"':
                return r'\"'
            if token == r'\"':
                raise ValueError("Invalid escape in single-quoted JSONPath name")
            return token

        # Transform only quote syntax. Consuming each backslash pair together
        # keeps \\ and Unicode escapes intact for the JSON decoder.
        literal = '"' + re.sub(r'\\.|"', json_quote, literal[1:-1]) + '"'
    name = json.loads(literal)
    # json.loads unifies a correctly escaped pair; reject unpaired surrogates.
    if not name.isascii():
        name.encode("utf-8")  # UnicodeEncodeError is a ValueError
    return name


def partial_argument_path(path: str) -> tuple[str | int, ...]:
    # Native partialArgs use singular JSONPaths, never selectors or wildcards.
    if not isinstance(path, str) or not path.startswith("$") or len(path) > 8192:
        raise ValueError("参数路径无效。")
    result, offset = [], 1
    while offset < len(path):
        match = _PATH_TOKEN.match(path, offset)
        if not match:
            raise ValueError("参数路径不是受支持的单值 JSONPath。")
        name, index, quoted = match.groups()
        try:
            token = name if name is not None else int(index) if index is not None else _decode_quoted_name(quoted)
        except ValueError:
            raise ValueError("参数名的引号或转义无效。") from None
        if isinstance(token, int) and token >= MAX_ARGUMENT_FIELDS:
            raise ValueError("数组索引超过上限。")
        result.append(token)
        if len(result) > 64:
            raise ValueError("参数嵌套超过上限。")
        offset = match.end()
    if not result or not isinstance(result[0], str):
        raise ValueError("函数参数必须是对象。")
    return tuple(result)
