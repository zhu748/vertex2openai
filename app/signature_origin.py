"""Bounded, process-local proof for replaying a stripped assistant tool turn.

The digest covers the authenticated client, resolved model, entire supported
visible prefix, complete ordered tool batch (opaque ID/name/parsed arguments),
and slot. Unsupported or over-budget shapes do not get a partial identity.
Only digests, topology, and genuine response bytes reach the existing TTL/LRU.
"""
import hashlib
import json
import secrets

from signature_store import signature_store
from thought_signatures import signature_from_extra


_KEY = secrets.token_bytes(32)
_MAX_MESSAGES = 256
_MAX_TEXT = 128 * 1024
_MAX_CALLS = 64
_MAX_ARG = 16 * 1024


class _Unsupported(ValueError):
    pass


def _hash(domain, *fields):
    h = hashlib.blake2b(key=_KEY, digest_size=32, person=domain)
    for field in fields:
        h.update(len(field).to_bytes(8, "big"))
        h.update(field)
    return h.digest()


def _encode(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _call_size(call):
    return len(call["id"]) + len(call["function"]["name"]) + len(call["function"].get("arguments", "{}"))


def _call_tag(call):
    fn = call["function"]
    ident, name = call["id"], fn["name"]
    if (not isinstance(ident, str) or not ident or len(ident) > _MAX_ARG or
            not isinstance(name, str) or not name or len(name) > _MAX_ARG or
            call.get("type", "function") != "function" or
            call.keys() - {"id", "type", "function", "index", "extra_content"} or fn.keys() - {"name", "arguments"}):
        raise _Unsupported
    # Both public adapters produce JSON argument strings. Do not stringify a
    # possibly giant arbitrary decoded object just to try a fallback lookup.
    raw = fn.get("arguments", "{}")
    if not isinstance(raw, str) or len(raw) > _MAX_ARG:
        raise _Unsupported
    parsed = json.loads(raw or "{}")
    if not isinstance(parsed, dict):
        raise _Unsupported
    return _hash(b"tool-call-v1", ident.encode(), name.encode(), _encode(parsed))


def _slot_keys(prefix, tags):
    if not prefix or not tags or len(tags) > _MAX_CALLS:
        return ()
    batch = _hash(b"tool-batch-v1", prefix, *tags)
    return tuple(_hash(b"tool-slot-v1", batch, i.to_bytes(2, "big")) for i in range(len(tags)))


class VisibleText:
    """Incremental keyed digest, insensitive to public SSE chunk boundaries."""
    def __init__(self):
        self.size = 0
        self._digest = None
        self._exclamation = False

    def feed(self, text):
        if self.size < 0 or text is None or text == "":
            return
        # Rich media can be a rendered Markdown image or an external asset.
        # Exclude image syntax, not innocent text such as "data:image/png".
        if (not isinstance(text, str) or len(text) > _MAX_TEXT - self.size or
                "![" in text or self._exclamation and text.startswith("[")):
            self.size = -1
            return
        try:
            raw = text.encode()
        except UnicodeError:
            self.size = -1
            return
        self.size += len(raw)
        if self.size > _MAX_TEXT:
            self.size = -1
            return
        self._exclamation = text.endswith("!")
        if self._digest is None:
            self._digest = hashlib.blake2b(key=_KEY, digest_size=32, person=b"tool-visible-v1")
        self._digest.update(raw)

    def result(self):
        return self.size, self._digest.digest() if self._digest and self.size >= 0 else b""


def _visible_field(text):
    field = VisibleText()
    field.feed(text)
    return field.result()


def _step_prefix(prefix, text, reasoning):
    return _hash(b"tool-observed-v1", prefix, text, reasoning) if text or reasoning else prefix


def _plain_metadata(extra, count):
    if not extra:
        return True
    if not isinstance(extra, dict) or extra.keys() - {"google"}:
        return False
    google = extra.get("google", {})
    if not isinstance(google, dict) or google.keys() - {"ordinary_parts", "part_order", "thought_signature",
                                                        "thought_signature_part"}:
        return False
    if google.get("thought_signature_part") not in (None, "text", "thought"):
        return False
    parts = google.get("ordinary_parts", [])
    if (not isinstance(parts, list) or len(parts) > 512 or any(not isinstance(p, dict) or
            p.get("kind") not in ("text", "thought") for p in parts)):
        return False
    order = google.get("part_order")
    if order is None:
        return True
    if not isinstance(order, list) or len(order) != len(parts) + count:
        return False
    # Thin clients cannot prove an interleaved ordinary/tool order. Only accept
    # the unambiguous presentation: ordinary public fields then the ordered batch.
    for n, slot in enumerate(order):
        kind, index = ("ordinary", n) if n < len(parts) else ("tool_call", n - len(parts))
        if (not isinstance(slot, dict) or slot.keys() != {"type", "index"} or slot.get("type") != kind or
                type(slot.get("index")) is not int or slot["index"] != index):
            return False
    return True


def origin_slot(message, index):
    slots = getattr(message, "_signature_slots", ())
    return slots[index] if index < len(slots) else (b"" if getattr(message, "_signature_scoped", False) else None)


def bind_signature_origins(request, api_key):
    """Mark exact prior turn scopes on this private request instance, never on the wire."""
    if not isinstance(api_key, str) or not api_key:
        return
    request._signature_output_prefix = None
    has_tools = bool(request.tools)
    isolated = False
    for i, m in enumerate(request.messages):
        if m.tool_calls:
            # HTTP normally constructs fresh instances. A shallow copy isolates
            # request-private bindings even when an internal caller reuses one.
            if not isolated:
                request.messages = list(request.messages)
                isolated = True
            m = request.messages[i] = m.model_copy()
            m._signature_slots = ()
            m._signature_scoped = True
            has_tools = True
    if (not has_tools or len(request.messages) > _MAX_MESSAGES or len(api_key) > _MAX_ARG or
            len(request.model) > _MAX_ARG):
        return
    try:
        prefix = _hash(b"tool-prefix-v1", api_key.encode(), request.model.strip().encode())
        budget = _MAX_TEXT
        for message_index, m in enumerate(request.messages):
            if request._signature_origin_cutoff == message_index:
                return
            content = m.content
            if content is not None and not isinstance(content, str):
                raise _Unsupported
            budget -= len(content or "")
            if budget < 0 or m.model_extra or m.refusal:
                raise _Unsupported
            calls = m.tool_calls or ()
            if len(calls) > _MAX_CALLS:
                raise _Unsupported
            if calls:
                if (m.role != "assistant" or m.name or m.tool_call_id or
                        not _plain_metadata(m.extra_content, len(calls))):
                    raise _Unsupported
                text_size, text_tag = _visible_field(content)
                reason_size, reason_tag = m._signature_observed_reasoning or _visible_field(m.reasoning_content)
                budget -= sum(_call_size(c) for c in calls) + reason_size + max(0, text_size - len(content or ""))
                if text_size < 0 or reason_size < 0 or budget < 0:
                    raise _Unsupported
                tags = [_call_tag(c) for c in calls]
                if len({c["id"] for c in calls}) != len(calls):
                    raise _Unsupported
                prefix = _step_prefix(prefix, text_tag, reason_tag)
                m._signature_slots = _slot_keys(prefix, tags)
                prefix = _hash(b"tool-step-v1", prefix, *tags)
                continue
            if (m.role not in ("user", "assistant", "tool", "system", "developer") or not isinstance(content, str)
                    or m.reasoning_content or m._signature_observed_reasoning):
                raise _Unsupported
            extra = m.extra_content or {}
            error = False
            if extra:
                if (m.role != "tool" or extra.keys() != {"anthropic"} or
                        not isinstance(extra["anthropic"], dict) or
                        extra["anthropic"].keys() != {"tool_result_is_error"} or
                        type(extra["anthropic"]["tool_result_is_error"]) is not bool):
                    raise _Unsupported
                error = extra["anthropic"]["tool_result_is_error"]
            if len(m.name or "") + len(m.tool_call_id or "") > _MAX_ARG:
                raise _Unsupported
            prefix = _hash(b"tool-message-v1", prefix, _encode([m.role, content, m.name, m.tool_call_id, error]))
        if request._signature_origin_cutoff is None:
            request._signature_output_prefix = prefix
    except (TypeError, AttributeError, KeyError, ValueError, OverflowError, RecursionError):
        # Only complete prefixes are valid. A previously marked earlier turn
        # still has a complete prefix; never bind the incomplete suffix.
        return


class OriginBatch:
    """Collect digests of complete calls, not raw arguments, until a successful terminal."""
    def __init__(self, prefix):
        self.prefix = prefix
        self.tags = []
        self.signatures = []
        self.ids = set()
        self.remaining = _MAX_TEXT
        self.text = VisibleText()
        self.reasoning = VisibleText()

    def discard(self):
        self.prefix = None
        self.tags.clear()
        self.signatures.clear()
        self.ids.clear()

    def add(self, call):
        if not self.prefix:
            return
        try:
            self.remaining -= _call_size(call)
            if len(self.tags) >= _MAX_CALLS or self.remaining < 0:
                raise _Unsupported
            tag = _call_tag(call)
            encoded = ((call.get("extra_content") or {}).get("google") or {}).get("thought_signature")
            if encoded and len(encoded) > (_MAX_ARG + 2) // 3 * 4:
                raise _Unsupported
            signature, _ = signature_from_extra(call)
            self.remaining -= len(signature or b"")
            ident = _hash(b"tool-id-v1", call["id"].encode())
            if self.remaining < 0 or len(signature or b"") > _MAX_ARG or ident in self.ids:
                raise _Unsupported
            self.ids.add(ident)
            self.tags.append(tag)
            self.signatures.append(signature)
        except (TypeError, AttributeError, KeyError, ValueError, OverflowError, RecursionError):
            self.discard()

    def finish_plain_parts(self, parts, order):
        if not _plain_metadata({"google": {"ordinary_parts": parts, "part_order": order}}, len(self.tags)):
            self.discard()
            return
        for p in parts:
            (self.reasoning if p["kind"] == "thought" else self.text).feed(p.get("text"))
        self.finish()

    def finish(self):
        ts, text = self.text.result()
        rs, reasoning = self.reasoning.result()
        if ts < 0 or rs < 0 or ts + rs > self.remaining:
            self.discard()
            return
        prefix = _step_prefix(self.prefix, text, reasoning) if self.prefix else None
        keys = _slot_keys(prefix, self.tags)
        for i, key in enumerate(keys):
            signature_store.register_call_signature(key, self.signatures[i], unsigned_follower=i > 0)
        self.discard()


def publish_completed_origins(prefix, choices):
    if not prefix or len(choices) > 8:
        return
    for choice in choices:
        message = choice.get("message") or {}
        calls = message.get("tool_calls") or ()
        if (choice.get("finish_reason") != "tool_calls" or not 0 < len(calls) <= _MAX_CALLS or
                not _plain_metadata(message.get("extra_content"), len(calls))):
            continue
        origin = OriginBatch(prefix)
        origin.text.feed(message.get("content"))
        origin.reasoning.feed(message.get("reasoning_content"))
        for call in calls:
            origin.add(call)
        origin.finish()


def observe_stream_origin(batches, prefix, choice):
    """Express only: the last SDK frame can contain just the terminal reason."""
    index = choice.get("index", 0)
    if type(index) is not int or not 0 <= index < 8:
        return
    delta = choice.get("delta") or {}
    origin = batches.get(index)
    if origin is None:
        origin = batches[index] = OriginBatch(prefix)
    if not origin.prefix:
        return
    if origin.tags and (delta.get("content") or delta.get("reasoning_content")):
        # Thin flattened fields cannot distinguish source text after a tool.
        origin.discard()
        return
    origin.text.feed(delta.get("content"))
    origin.reasoning.feed(delta.get("reasoning_content"))
    calls = delta.get("tool_calls") or ()
    if not _plain_metadata(delta.get("extra_content"), len(origin.tags) + len(calls)):
        origin.discard()
    if len(calls) > _MAX_CALLS:
        origin.discard()
    if origin.prefix:
        for call in calls:
            origin.add(call)
    if choice.get("finish_reason") == "tool_calls":
        origin.finish()
    elif choice.get("finish_reason"):
        origin.discard()
