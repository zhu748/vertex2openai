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
from parallel_result_order import order_parallel_results
from signature_store import SignatureRecord, SignatureState, signature_store
from thought_signatures import historical_literal_id, ordinary_signature, replay_tool_call_id


_KEY = secrets.token_bytes(32)
_MAX_MESSAGES = 256
_MAX_TEXT = 1024 * 1024  # Total UTF-8 proof work per history or candidate.
_MAX_CALLS = 64
_MAX_ID_BYTES = 16 * 1024
_MAX_ARGS_BYTES = 128 * 1024
_MAX_OBJECT_DEPTH = 64
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


def _bound_argument_object(value, remaining, depth=0):
    """Reject oversized decoded trees before sorted serialization."""
    remaining -= 1
    if remaining < 0 or depth > _MAX_OBJECT_DEPTH:
        raise _Unsupported
    if isinstance(value, str):
        remaining -= len(value)
    elif isinstance(value, dict):
        if len(value) > remaining // 2:
            raise _Unsupported
        for key, item in value.items():
            if not isinstance(key, str):
                raise _Unsupported
            remaining = _bound_argument_object(key, remaining, depth + 1)
            remaining = _bound_argument_object(item, remaining, depth + 1)
    elif isinstance(value, list):
        if len(value) > remaining:
            raise _Unsupported
        for item in value:
            remaining = _bound_argument_object(item, remaining, depth + 1)
    elif value is not None and not isinstance(value, (bool, int, float)):
        raise _Unsupported
    if remaining < 0:
        raise _Unsupported
    return remaining


def _argument_tag_bytes(raw, budget):
    limit = min(_MAX_ARGS_BYTES, budget.remaining)
    if isinstance(raw, dict):
        _bound_argument_object(raw, limit)
        parsed, raw_size = raw, 0
    elif raw is None:
        parsed, raw_size = {}, 0
    else:
        encoded = budget.encode(raw, _MAX_ARGS_BYTES)
        parsed, raw_size = json.loads(encoded if encoded.strip() else b"{}"), len(encoded)
    if not isinstance(parsed, dict):
        raise _Unsupported
    canonical = _encode(parsed)
    if len(canonical) > limit:
        raise _Unsupported
    budget.consume(max(0, len(canonical) - raw_size))
    return canonical


def _call_tag(call, budget):
    fn = call["function"]
    ident, name = call["id"], fn["name"]
    if (not ident or not name or
            call.get("type", "function") != "function" or
            call.keys() - {"id", "type", "function", "index", "extra_content"} or fn.keys() - {"name", "arguments"}):
        raise _Unsupported
    arguments = _argument_tag_bytes(fn.get("arguments"), budget)
    return _hash(b"tool-call-v1", budget.encode(ident, _MAX_ID_BYTES),
                 budget.encode(name, _MAX_ID_BYTES), arguments)


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


def _history_text(content, budget, *, can_embed_media, is_tool_result):
    # One nonempty plain text block has the same wire Part as scalar text,
    # except on tool results where a list is structured function output.
    if not is_tool_result and isinstance(content, list) and len(content) == 1:
        text = _block_text(content[0])
        if text:
            budget.consume(_BLOCK_COST)
            content = text
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
        if not content:
            return b""
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
    content = (b"" if message.content is None and reasoning else _history_text(
        message.content, budget, can_embed_media=role in ("user", "assistant"), is_tool_result=role == "tool"))
    return _hash(b"hist-message-v2", role.encode(), content, reasoning, extra,
                 budget.encode(message.name or "", _MAX_ID_BYTES),
                 budget.encode(message.tool_call_id or "", _MAX_ID_BYTES))


def origin_slot(message, index):
    slots = getattr(message, "_signature_slots", ())
    return slots[index] if index < len(slots) else (b"" if getattr(message, "_signature_scoped", False) else None)


def _proof_history(messages, stop):
    """Use the adapter order only for immediately adjacent, complete result runs."""
    i = 0
    while i < stop:
        message = messages[i]
        i += 1
        yield message
        calls = message.tool_calls or ()
        end = i + len(calls)
        if (len(calls) < 2 or end > stop
                or end < len(messages) and messages[end].role in ("tool", "function")):
            continue
        results = messages[i:end]
        if any(result.role != "tool" for result in results):
            continue
        # Proof only accepts natural call order (validated by _plain_metadata),
        # so no further projection is needed. Compute literal IDs after the
        # caller has bound the just-yielded assistant slots.
        literals = frozenset(marked for index, call in enumerate(calls)
                             if (marked := historical_literal_id(
                                 call, origin_slot(message, index), message._claude_literal_tool_ids,
                                 opaque_ids=message._opaque_tool_call_ids)) is not None)

        def result_key(value, literals=literals):
            return replay_tool_call_id(value, literals) if isinstance(value, str) and len(value) <= _MAX_ID_BYTES else ""

        yield from order_parallel_results(calls, results, result_key)
        i = end


def _compatible_sdk_defaults(extra):
    return all(key in _NULL_SDK_FIELDS and (
        value is None or key == "annotations" and isinstance(value, list) and not value)
        for key, value in (extra or {}).items())


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
            m._omitted_signature_key = None
            m._signature_scoped = True
            has_tools = True
    if not has_tools:
        return
    try:
        budget = _Budget()
        prefix = _hash(b"tool-prefix-v2", budget.encode(api_key, _MAX_ID_BYTES),
                       budget.encode(request.model.strip(), _MAX_ID_BYTES))
        stop = min(len(request.messages), _MAX_MESSAGES)
        cutoff = request._signature_origin_cutoff
        if cutoff is not None:
            stop = min(stop, cutoff)
        for m in _proof_history(request.messages, stop):
            if m.refusal or not _compatible_sdk_defaults(m.model_extra):
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
        if cutoff is None and stop == len(request.messages):
            request._signature_output_prefix = prefix
    except (TypeError, AttributeError, KeyError, ValueError, OverflowError, RecursionError):
        # Only complete prefixes are valid. A previously marked earlier turn
        # still has a complete prefix; never bind the incomplete suffix.
        return


_UNRESTORABLE_ORIGIN = SignatureRecord(SignatureState.AMBIGUOUS)


def mark_unrestorable_origin(key):
    """An accepted, identifiable generation also counts when its bytes cannot be kept."""
    signature_store.put_record(key, _UNRESTORABLE_ORIGIN)


def is_unrestorable_tool_terminal(finish):
    """Accepted public stop reasons that cannot authorize tool replay bytes."""
    return finish in ("length", "content_filter")


def is_complete_terminal_call(call, index):
    """Representation check; the existing bounded call tag also parses the arguments."""
    fn = call["function"]
    raw, position = fn.get("arguments"), call.get("index", index)
    return (isinstance(call["id"], str) and bool(call["id"].strip())
            and isinstance(fn["name"], str) and bool(fn["name"].strip())
            and (isinstance(raw, dict) or isinstance(raw, str) and bool(raw.strip()))
            and type(position) is int and position == index)


def _commit_origin(keys, signatures, destination):
    if signatures is None:
        for key in keys:
            mark_unrestorable_origin(key)
    else:
        signature_store.register_origin_batch(keys, signatures, destination)


class DeliveredOrigin:
    """Stage at most one verified producer batch until its public response is sent.

    Keep only bounded origin digests, optional original signature bytes, and
    the actual destination. Never keep arguments or message text here.
    """

    def __init__(self):
        self._pending = None
        self._source = None
        self._valid = True
        self._terminal_valid = True
        self._confirmed = False

    def source(self, source_id):
        if not self._valid:
            return
        if not isinstance(source_id, str) or not source_id or len(source_id) > 256:
            self.discard()
        elif self._source is None:
            self._source = source_id
        elif self._source != source_id:
            self.discard()

    def reject_terminal(self):
        # Late ordinary content can still collide after the true-stream
        # projection flattens it; only an incomplete terminal requires the
        # stronger natural-order check already applied to that projection.
        self._terminal_valid = False

    def stage(self, finish, keys, signatures, destination):
        if not self._valid or self._pending is not None:
            self.discard()
            return
        # OriginBatch already verified key/count, per-slot and aggregate
        # byte limits. It hands over immutable snapshots, not its work state.
        self._pending = finish, keys, signatures, destination

    def confirm_batch(self, finish, count):
        if (self._pending is None or self._pending[0] != finish or len(self._pending[1]) != count
                or is_unrestorable_tool_terminal(finish) and not self._terminal_valid):
            self.discard()
            return
        self._confirmed = True

    @property
    def ready(self):
        return self._confirmed

    def discard(self):
        self._valid = False
        self._confirmed = False
        self._pending = None
        self._source = None

    async def publish(self):
        pending = self._pending if self._confirmed else None
        self.discard()
        if pending is not None:
            _commit_origin(*pending[1:])


class OriginBatch:
    """Collect the public identity separately from the originals it can carry."""
    def __init__(self, prefix):
        self.prefix = prefix
        self.tags = []
        self.signatures = []
        self.ids = set()
        self.budget = _Budget()
        self.signature_bytes_left = _MAX_BATCH_SIGNATURE_BYTES
        self.withheld = False
        self.stream_terminal = None
        self.can_fence_terminal = True
        self.text = VisibleText()
        self.reasoning = VisibleText()

    def discard(self):
        self.prefix = None
        self.tags.clear()
        self.signatures.clear()
        self.ids.clear()

    def withhold(self):
        # The rendered tool batch may still identify an older cached reply.
        # Keep collecting its identity, but do not keep partial payload bytes.
        self.withheld = True
        self.signatures.clear()

    def reject_topology(self):
        self.can_fence_terminal = False
        self.withhold()

    def add(self, call):
        if not self.prefix:
            return
        try:
            if len(self.tags) >= _MAX_CALLS:
                raise _Unsupported
            tag = _call_tag(call, self.budget)
            ident = _hash(b"tool-id-v1", call["id"].encode())
            if ident in self.ids:
                raise _Unsupported
            if self.can_fence_terminal:
                self.can_fence_terminal = is_complete_terminal_call(call, len(self.tags))
        except (TypeError, AttributeError, KeyError, ValueError, OverflowError, RecursionError):
            self.discard()
            return
        self.ids.add(ident)
        self.tags.append(tag)
        if self.withheld:
            return
        try:
            encoded = ((call.get("extra_content") or {}).get("google") or {}).get("thought_signature")
            _signature_size(encoded)
            signature = ordinary_signature(encoded)
            self.signature_bytes_left -= len(signature or b"")
            if self.signature_bytes_left < 0 or len(signature or b"") > _MAX_SIGNATURE_BYTES:
                raise _Unsupported
            self.signatures.append(signature)
        except (TypeError, AttributeError, KeyError, ValueError, OverflowError, RecursionError):
            self.withhold()

    def finish_plain_parts(self, parts, order, *, finish_reason="tool_calls", stage_origin=None, destination=None):
        if not self.prefix or len(parts) > _MAX_BLOCKS:
            self.discard()
            return
        for p in parts:
            kind, text = p.get("kind"), p.get("text")
            if kind in ("text", "thought") and isinstance(text, str):
                (self.reasoning if kind == "thought" else self.text).feed(text)
            elif kind == "signature_only" and text == "" and p.get("thought_signature"):
                # This carrier has no public text and cannot be confused
                # with a visible image or another unresolved Part.
                self.reject_topology()
            else:
                self.discard()
                return
        if not _plain_metadata({"google": {"ordinary_parts": parts, "part_order": order}}, len(self.tags)):
            self.reject_topology()
        self.finish(finish_reason=finish_reason, stage_origin=stage_origin, destination=destination)

    def finish(self, *, finish_reason="tool_calls", stage_origin=None, destination=None):
        terminal_fence = is_unrestorable_tool_terminal(finish_reason)
        if finish_reason != "tool_calls" and not terminal_fence:
            self.discard()
            return
        ts, text = self.text.result()
        rs, reasoning = self.reasoning.result()
        if (ts < 0 or rs < 0 or ts + rs > self.budget.remaining
                or terminal_fence and not self.can_fence_terminal):
            self.discard()
            return
        prefix = _step_prefix(self.prefix, text, reasoning) if self.prefix else None
        keys = _slot_keys(prefix, self.tags)
        if keys:
            signatures = None if self.withheld or terminal_fence else self.signatures
            if stage_origin is not None:
                stage_origin(finish_reason, keys, tuple(signatures) if signatures is not None else None, destination)
            else:
                _commit_origin(keys, signatures, destination)
        self.discard()


def publish_completed_origins(prefix, choices, *, stage_origin=None, destination=None):
    if not prefix or len(choices) > 8:
        return
    for choice in choices:
        message = choice.get("message") or {}
        calls = message.get("tool_calls") or ()
        finish = choice.get("finish_reason")
        terminal_fence = is_unrestorable_tool_terminal(finish)
        if (finish != "tool_calls" and not terminal_fence) or not 0 < len(calls) <= _MAX_CALLS:
            continue
        origin = OriginBatch(prefix)
        if terminal_fence:
            origin.withhold()
        if not _plain_metadata(message.get("extra_content"), len(calls)):
            origin.reject_topology()
        origin.text.feed(message.get("content"))
        origin.reasoning.feed(message.get("reasoning_content"))
        for call in calls:
            origin.add(call)
        origin.finish(finish_reason=finish, stage_origin=stage_origin, destination=destination)


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
    finish = choice.get("finish_reason")
    if is_unrestorable_tool_terminal(origin.stream_terminal):
        if (finish not in (None, origin.stream_terminal) or delta.get("tool_calls")
                or delta.get("content") or delta.get("reasoning_content")):
            origin.discard()
        return
    terminal_fence = is_unrestorable_tool_terminal(finish)
    if terminal_fence:
        origin.withhold()
    if origin.tags and (delta.get("content") or delta.get("reasoning_content")):
        # Thin flattened fields cannot distinguish source text after a tool.
        origin.reject_topology()
    origin.text.feed(delta.get("content"))
    origin.reasoning.feed(delta.get("reasoning_content"))
    calls = delta.get("tool_calls") or ()
    if not _plain_metadata(delta.get("extra_content"), len(origin.tags) + len(calls)):
        origin.reject_topology()
    if len(calls) > _MAX_CALLS:
        origin.discard()
    if origin.prefix:
        for call in calls:
            origin.add(call)
    if finish == "tool_calls" or terminal_fence:
        origin.stream_terminal = finish
    elif finish:
        origin.discard()


def publish_stream_origins(batches, *, stage_origin=None, destination=None):
    """Only the accepted producer attempt reaches the public delivery stage."""
    for origin in batches.values():
        if origin.stream_terminal:
            origin.finish(finish_reason=origin.stream_terminal, stage_origin=stage_origin, destination=destination)
