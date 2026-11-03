"""Encode large inline media without full-size JSON text/UTF-8 temporaries."""
import io
import json

from fastapi.responses import JSONResponse

JSON_STRING_CHARS = 64 * 1024


def iter_json_bytes(value, *, compact=False, allow_nan=True):
    """Match json.dumps(ensure_ascii=False), with bounded string encoding.

    JSONEncoder.iterencode alone still materializes each entire quoted string.
    Splitting the string before escaping avoids that copy for inline Base64.
    Each fragment is transport data; concatenation is one JSON document.
    """
    item_sep, key_sep = (b",", b":") if compact else (b", ", b": ")
    active = set()

    def string_chunks(text):
        if len(text) <= JSON_STRING_CHARS:
            yield json.dumps(text, ensure_ascii=False).encode("utf-8")
            return
        yield b'"'
        for start in range(0, len(text), JSON_STRING_CHARS):
            escaped = json.dumps(text[start:start + JSON_STRING_CHARS], ensure_ascii=False)
            yield escaped[1:-1].encode("utf-8")
        yield b'"'

    def encode(node):
        if isinstance(node, str):
            yield from string_chunks(node)
        elif isinstance(node, (dict, list, tuple)):
            ident = id(node)
            if ident in active:
                raise ValueError("Circular reference detected")
            active.add(ident)
            try:
                is_dict = isinstance(node, dict)
                yield b"{" if is_dict else b"["
                for index, item in enumerate(node.items() if is_dict else node):
                    if index:
                        yield item_sep
                    if is_dict:
                        key, item = item
                        if not isinstance(key, str):
                            if key is None or isinstance(key, (bool, int, float)):
                                key = json.dumps(key, allow_nan=allow_nan)
                            else:
                                raise TypeError("JSON object keys must be str, int, float, bool or None")
                        yield from string_chunks(key)
                        yield key_sep
                    yield from encode(item)
                yield b"}" if is_dict else b"]"
            finally:
                active.remove(ident)
        else:
            yield json.dumps(node, ensure_ascii=False, allow_nan=allow_nan).encode("utf-8")

    yield from encode(value)


class LargeJSONResponse(JSONResponse):
    """Keep one final JSON body without also creating whole-body text/bytes copies."""
    def render(self, content):
        with io.BytesIO() as output:
            for chunk in iter_json_bytes(content, compact=True, allow_nan=False):
                output.write(chunk)
            return output.getvalue()
