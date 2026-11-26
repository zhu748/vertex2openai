"""Bounded, process-local proof for replaying a stripped assistant tool turn.

The digest covers the authenticated client, resolved model, entire supported
visible prefix, complete ordered tool batch (opaque ID/name/parsed arguments),
and slot. Unsupported or over-budget shapes never get an identity for their
suffix. Only digests, topology, and opaque response bytes reach the TTL/LRU.
"""
import hashlib
import json
import secrets

from models import ContentPartText
from signature_store import signature_store
from thought_signatures import signature_from_extra


_KEY = secrets.token_bytes(32)
_MAX_MESSAGES = 256
_MAX_TEXT = 1024 * 1024  # Total UTF-8 proof work per history or candidate.
_MAX_CALLS = 64
_MAX_ID_BYTES = 16 * 1024
_MAX_ARGS_BYTES = 128 * 1024
_MAX_SIGNATURE_BYTES = 256 * 1024
_MAX_ENCODED_SIGNATURE = 4 * ((_MAX_SIGNATURE_BYTES + 2) // 3)
_MAX_BATCH_SIGNATURE_BYTES = 2 * _MAX_SIGNATURE_BYTES
_MAX_ORDINARY_SLOTS = 512
_MAX_BLOCKS = 8192
_BLOCK_COST = 16
_UTF8_CHUNK = 16384
_NULL_SDK_FIELDS = frozenset(("annotations", "audio", "function_call"))


class _Unsupported(ValueError):
    pass


def _digest(domain):
    return hashlib.blake2b(key=_KEY, digest_size=32, person=domain)


def _feed_field(h, field):
    h.update(len(field).to_bytes(8, "big"))
    h.update(field)


def _hash(domain, *fields):
    h = _digest(domain)
    for field in fields:
        _feed_field(h, field)
    return h.digest()


def _encode(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class _Budget:
    def __init__(self):
        self.remaining = _MAX_TEXT

    def consume(self, size):
        self.remaining -= size
        if self.remaining < 0:
            raise _Unsupported

    def encode(self, text, maximum):
        if not isinstance(text, str) or len(text) > min(maximum, self.remaining):
            raise _Unsupported
        raw = text.encode()
        if len(raw) > maximum:
            raise _Unsupported
        self.consume(len(raw))
        return raw

    def observed(self, result):
        size, digest = result
        if size < 0:
            raise _Unsupported
        self.consume(size)
        return digest

    def visible(self, text, *, reject_markdown=True):
        return self.observed(_visible_field(text, self.remaining, reject_markdown=reject_markdown))


def _call_tag(call, budget):
    fn = call["function"]
    ident, name = call["id"], fn["name"]
    if (not ident or not name or
            call.get("type", "function") != "function" or
            call.keys() - {"id", "type", "function", "index", "extra_content"} or fn.keys() - {"name", "arguments"}):
        raise _Unsupported
    # Both public adapters produce JSON argument strings. Do not stringify a
    # possibly giant arbitrary decoded object just to try a fallback lookup.
    raw = fn.get("arguments", "{}")
    raw = budget.encode(raw, _MAX_ARGS_BYTES)
    parsed = json.loads(raw if raw.strip() else b"{}")
    if not isinstance(parsed, dict):
        raise _Unsupported
    return _hash(b"tool-call-v1", budget.encode(ident, _MAX_ID_BYTES),
                 budget.encode(name, _MAX_ID_BYTES), _encode(parsed))


def _slot_keys(prefix, tags):
    if not prefix or not tags or len(tags) > _MAX_CALLS:
        return ()
    batch = _hash(b"tool-batch-v1", prefix, *tags)
    return tuple(_hash(b"tool-slot-v1", batch, i.to_bytes(2, "big")) for i in range(len(tags)))


class VisibleText:
    """Incremental keyed digest, insensitive to public SSE chunk boundaries."""
    def __init__(self, limit=None, *, reject_markdown=True):
        self.size = 0
        self._limit = _MAX_TEXT if limit is None else limit
        self._reject_markdown = reject_markdown
        self._digest = None
        self._exclamation = False

    def feed(self, text):
        if self.size < 0 or text is None or text == "":
            return
        # Rich media can be a rendered Markdown image or an external asset.
        # Exclude image syntax, not innocent text such as "data:image/png".
        if (not isinstance(text, str) or len(text) > self._limit - self.size or
                self._reject_markdown and ("![" in text or self._exclamation and text.startswith("["))):
            self.size = -1
            return
        try:
            if self._digest is None:
                self._digest = _digest(b"tool-visible-v1")
            for start in range(0, len(text), _UTF8_CHUNK):
                raw = text[start:start + _UTF8_CHUNK].encode()
                self.size += len(raw)
                if self.size > self._limit:
                    self.size = -1
                    return
                self._digest.update(raw)
        except UnicodeError:
            self.size = -1
            return
        self._exclamation = text.endswith("!")

    def result(self):
        return self.size, self._digest.digest() if self._digest and self.size >= 0 else b""


def _visible_field(text, limit=None, *, reject_markdown=True):
    field = VisibleText(limit, reject_markdown=reject_markdown)
    field.feed(text)
    return field.result()


def _step_prefix(prefix, text, reasoning):
    return _hash(b"tool-observed-v1", prefix, text, reasoning) if text or reasoning else prefix


def _signature_size(value):
    if value is None:
        return 0
    if isinstance(value, str) and value.isascii() and len(value) <= _MAX_ENCODED_SIGNATURE:
        return len(value)
    if isinstance(value, bytes) and len(value) <= _MAX_SIGNATURE_BYTES:
        return len(value)
    raise _Unsupported


def _plain_google(extra, count):
    if not extra:
        return {}
    if not isinstance(extra, dict) or extra.keys() - {"google"}:
        raise _Unsupported
    google = extra.get("google", {})
    if (not isinstance(google, dict)
            or google.keys() - {"ordinary_parts", "part_order", "thought_signature", "thought_signature_part"}
            or google.get("thought_signature_part") not in (None, "text", "thought")):
        raise _Unsupported
    parts = google.get("ordinary_parts", ())
    if "ordinary_parts" in google and (not isinstance(parts, list) or len(parts) > _MAX_BLOCKS):
        raise _Unsupported
    slots = size = 0
    prior_unsigned = None
    sig_left = _MAX_BATCH_SIGNATURE_BYTES * 2 - _signature_size(google.get("thought_signature"))
    for part in parts:
        if (not isinstance(part, dict) or part.keys() - {"kind", "text", "thought_signature"}
                or part.get("kind") not in ("text", "thought") or not isinstance(part.get("text"), str)):
            raise _Unsupported
        size += len(part["text"]) + _BLOCK_COST
        size_of_sig = _signature_size(part.get("thought_signature"))
        sig_left -= size_of_sig
        # Fragments are only counted as one *proof* slot when adjacent, of
        # the same kind, and unsigned. Wire descriptors are never rewritten.
        if size_of_sig or prior_unsigned != part["kind"]:
            slots += 1
        prior_unsigned = None if size_of_sig else part["kind"]
        if slots > _MAX_ORDINARY_SLOTS or size > _MAX_TEXT or sig_left < 0:
            raise _Unsupported
    order = google.get("part_order")
    if order is None:
        return google
    if not isinstance(order, list) or len(order) != len(parts) + count:
        raise _Unsupported
    # Thin clients cannot prove an interleaved ordinary/tool order. Only accept
    # the unambiguous presentation: ordinary public fields then the ordered batch.
    for n, slot in enumerate(order):
        kind, index = ("ordinary", n) if n < len(parts) else ("tool_call", n - len(parts))
        if (not isinstance(slot, dict) or slot.keys() != {"type", "index"} or slot.get("type") != kind or
                type(slot.get("index")) is not int or slot["index"] != index):
            raise _Unsupported
    return google


def _plain_metadata(extra, count):
    try:
        _plain_google(extra, count)
    except _Unsupported:
        return False
    return True


def _block_text(part):
    if isinstance(part, ContentPartText):
        return part.text
    if (isinstance(part, dict) and part.keys() == {"type", "text"}
            and part["type"] == "text" and isinstance(part["text"], str)):
        return part["text"]
    raise _Unsupported


def _plain_text_tag(text, budget, *, can_embed_media):
    # Ordinary message text is parsed one segment at a time. Unlike deltas,
    # there can be no inline or locally issued attachment across a segment
    # boundary. Tool results and system text on the string path are literal.
    if (can_embed_media and "![" in text
            and ("](data:image/" in text or "/v1/generated-images/" in text)):
        raise _Unsupported
    return budget.visible(text, reject_markdown=False)


def _history_text(content, budget, *, can_embed_media):
    if isinstance(content, str):
        return _hash(b"hist-scalar-v2", _plain_text_tag(content, budget, can_embed_media=can_embed_media))
    if not isinstance(content, list) or len(content) > _MAX_BLOCKS:
        raise _Unsupported
    h = _digest(b"hist-list-v2")
    for part in content:
        budget.consume(_BLOCK_COST)
        _feed_field(h, _plain_text_tag(_block_text(part), budget, can_embed_media=can_embed_media))
    return h.digest()


def _tool_visible_text(content, budget):
    if isinstance(content, list):
        # A single plain segment shares the scalar projection on both wires;
        # multiple list blocks can affect Part boundaries on Cookie.
        if len(content) != 1:
            raise _Unsupported
        content = _block_text(content[0])
    return budget.visible(content)


def _prior_assistant_extra(extra, budget):
    google = _plain_google(extra, 0)
    if not google:
        return b""
    h = _digest(b"hist-extra-v2")

    def feed_signature(value):
        budget.consume(_signature_size(value))
        # An internal bytes carrier is not the same value as its wire string.
        _feed_field(h, b"bytes" if isinstance(value, bytes) else b"text")
        _feed_field(h, value if isinstance(value, bytes) else (value or "").encode("ascii"))

    _feed_field(h, (google.get("thought_signature_part") or "").encode())
    feed_signature(google.get("thought_signature"))
    _feed_field(h, b"1" if "ordinary_parts" in google else b"0")
    for part in google.get("ordinary_parts", ()):
        budget.consume(_BLOCK_COST)
        _feed_field(h, part["kind"].encode())
        _feed_field(h, budget.visible(part["text"]))
        feed_signature(part.get("thought_signature"))
    return h.digest()


def _history_tag(message, budget):
    role = message.role
    if role not in ("user", "assistant", "tool", "system", "developer"):
        raise _Unsupported
    observed_reasoning = message._signature_observed_reasoning
    reason = message.reasoning_content
    if role == "assistant":
        # Claude's visible thinking is identity evidence but not Google wire
        # reasoning. Keep it in a distinct domain from OpenAI reasoning.
        reasoning = (_hash(b"hist-observed-v2", budget.observed(observed_reasoning))
                     if observed_reasoning else budget.visible(reason))
        extra = _prior_assistant_extra(message.extra_content, budget)
    else:
        if reason or observed_reasoning:
            raise _Unsupported
        extra, reasoning = b"", b""
        if message.extra_content:
            if (role != "tool" or message.extra_content.keys() != {"anthropic"} or
                    not isinstance(anthropic := message.extra_content["anthropic"], dict) or
                    anthropic.keys() != {"tool_result_is_error"} or
                    type(anthropic["tool_result_is_error"]) is not bool):
                raise _Unsupported
            # An explicitly successful result can wrap top-level error/output
            # properties; it is not always the same wire as an absent flag.
            extra = b"error" if anthropic["tool_result_is_error"] else b"success"
    content = (b"" if message.content is None and reasoning
               else _history_text(message.content, budget, can_embed_media=role in ("user", "assistant")))
    return _hash(b"hist-message-v2", role.encode(), content, reasoning, extra,
                 budget.encode(message.name or "", _MAX_ID_BYTES),
                 budget.encode(message.tool_call_id or "", _MAX_ID_BYTES))


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
    if not has_tools:
        return
    try:
        budget = _Budget()
        prefix = _hash(b"tool-prefix-v2", budget.encode(api_key, _MAX_ID_BYTES),
                       budget.encode(request.model.strip(), _MAX_ID_BYTES))
        for message_index, m in enumerate(request.messages):
            if message_index == _MAX_MESSAGES or request._signature_origin_cutoff == message_index:
                return
            if m.refusal or any(key not in _NULL_SDK_FIELDS or value is not None
                                for key, value in (m.model_extra or {}).items()):
                raise _Unsupported
            calls = m.tool_calls or ()
            if len(calls) > _MAX_CALLS:
                raise _Unsupported
            if calls:
                if (m.role != "assistant" or m.name or m.tool_call_id or
                        not _plain_metadata(m.extra_content, len(calls))):
                    raise _Unsupported
                text_tag = _tool_visible_text(m.content, budget)
                reason_tag = (budget.observed(m._signature_observed_reasoning) if m._signature_observed_reasoning
                              else budget.visible(m.reasoning_content))
                tags = [_call_tag(c, budget) for c in calls]
                if len({c["id"] for c in calls}) != len(calls):
                    raise _Unsupported
                prefix = _step_prefix(prefix, text_tag, reason_tag)
                m._signature_slots = _slot_keys(prefix, tags)
                prefix = _hash(b"tool-step-v1", prefix, *tags)
                continue
            prefix = _hash(b"tool-message-v2", prefix, _history_tag(m, budget))
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
        self.budget = _Budget()
        self.signature_bytes_left = _MAX_BATCH_SIGNATURE_BYTES
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
            if len(self.tags) >= _MAX_CALLS:
                raise _Unsupported
            tag = _call_tag(call, self.budget)
            encoded = ((call.get("extra_content") or {}).get("google") or {}).get("thought_signature")
            _signature_size(encoded)
            signature, _ = signature_from_extra(call)
            self.signature_bytes_left -= len(signature or b"")
            ident = _hash(b"tool-id-v1", call["id"].encode())
            if self.signature_bytes_left < 0 or len(signature or b"") > _MAX_SIGNATURE_BYTES or ident in self.ids:
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
        if ts < 0 or rs < 0 or ts + rs > self.budget.remaining:
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
