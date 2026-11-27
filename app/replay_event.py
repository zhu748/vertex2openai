"""Inspect one complete SSE frame for buffering or protocol conversion."""
import json
from itertools import chain


def _lines(text):
    """Yield (start, end, next_start), recognizing only SSE's CR, LF and CRLF."""
    start, size = 0, len(text)
    cr, lf = text.find("\r"), text.find("\n")
    while start < size:
        if cr != -1 and (lf == -1 or cr < lf):
            end = cr
            after = cr + (2 if lf == cr + 1 else 1)
            cr = text.find("\r", after)
            if lf == end + 1:
                lf = text.find("\n", after)
        elif lf != -1:
            end, after = lf, lf + 1
            lf = text.find("\n", after)
        else:
            end = after = size
        yield start, end, after
        start = after


def _append_lines(parts, raw, start, end):
    if start < end:
        # Cutting out an ID can put two formerly separate bare endings next
        # to each other (CR + LF). Keep both logical lines on the wire.
        if parts and parts[-1].endswith("\r") and raw[start] == "\n":
            parts.append("\n")
        parts.append(raw[start:end])


def data_only_frame(raw: str) -> str:
    """Remove SSE control lines, preserving the exact data values and dispatch."""
    parts = []
    for start, end, after in _lines(raw):
        if start == end or raw.startswith("data: ", start, end):
            _append_lines(parts, raw, start, after)
        elif raw.startswith("data:", start, end) or (end == start + 4 and raw.startswith("data", start)):
            # The pinned SDK only recognizes "data: ". A missing colon or
            # optional separator space changes no part of the SSE field value.
            parts.append("data: ")
            _append_lines(parts, raw, min(start + 5, end), after)
    return "".join(parts)


class ReplayEvent:
    """Parse SSE field boundaries once; only allocate joined data when needed."""
    def __init__(self, raw: str):
        self.raw = raw
        self._data = []
        self._ids = []
        dispatch_start = self._data_start = 0
        for start, end, after in _lines(raw):
            if start == end:
                dispatch_start = after
                continue
            if raw.startswith("data:", start, end) or (end == start + 4 and raw.startswith("data", start)):
                # Empty/comment-only dispatches do not deliver a data event.
                if self._data and dispatch_start != self._data_start:
                    raise ValueError("同一续传帧包含多个 SSE 事件。")
                self._data_start = dispatch_start
                value = min(start + 5, end)
                if raw.startswith(" ", value, end):
                    value += 1
                self._data.append((value, end))
            elif raw.startswith("id:", start, end) or (end == start + 2 and raw.startswith("id", start)):
                self._ids.append((start, after))

    @property
    def has_data(self) -> bool:
        return bool(self._data)

    def data(self) -> str:
        """SSE joins the data fields of this physical event with a single LF."""
        if len(self._data) == 1:
            start, end = self._data[0]
            return self.raw[start:end]
        return "\n".join(self.raw[start:end] for start, end in self._data)

    def is_done(self) -> bool:
        if len(self._data) != 1:
            return False
        start, end = self._data[0]
        return self.raw.startswith("[DONE]", start, end) and end == start + 6

    def error(self):
        if not self._data:
            return None
        try:
            obj = json.loads(self.data())
        except ValueError:
            return None
        return obj.get("error") if isinstance(obj, dict) else None

    def with_id(self, cursor: str) -> str:
        if not self.has_data:
            return self.raw
        field = f"id: {cursor}\n"
        if not self._ids and not self._data_start:
            return field + self.raw
        # Bind the cursor to the dispatch that actually carries data. Remove
        # upstream IDs everywhere: even a later data-less ID could override it.
        parts, start = [], 0
        size = len(self.raw)
        for left, right in chain(self._ids, ((size, size),)):
            if start <= self._data_start <= left:
                _append_lines(parts, self.raw, start, self._data_start)
                parts.append(field)
                start = self._data_start
            _append_lines(parts, self.raw, start, left)
            start = right
        return "".join(parts)
