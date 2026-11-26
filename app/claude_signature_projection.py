"""Bounded, Claude-only fallback for thinking.display=omitted tool turns.

The regular origin includes the visible thinking text. For explicit omission
the Claude adapter can instead prove exactly the text and complete tool batch
it delivered. No provider reasoning, client thinking signature, tool args or
IDs are stored here. A single shared TTL/LRU entry holds the whole batch so
conflicts/eviction cannot leave a misleading subset of its signatures.
"""

import base64
import binascii

import signature_origin as origins
from signature_store import SKIP_VALIDATOR_SENTINELS, SignatureState
from thought_signatures import attach_signature_extra, google_metadata


def _scope(metadata):
    # Optional Anthropic session/user metadata narrows the proven scope; it
    # is never an authorization credential or sole source of replay identity.
    if metadata is None or (isinstance(metadata, dict) and "user_id" not in metadata):
        return b""
    if not isinstance(metadata, dict):
        return None
    uid = metadata["user_id"]
    if not isinstance(uid, str) or len(uid) > 1024:
        return None
    try:
        return origins._hash(b"claude-user-v1", uid.encode())
    except UnicodeError:
        return None


_MAX_PUBLIC_TEXT_BLOCKS = origins._MAX_BLOCKS
_LENGTH_BYTES = 4


def _batch_key(scope, slots, shape):
    return origins._hash(b"claude-omit-v1", scope, shape, *slots)


def _shape_field(hasher, piece):
    size, digest = piece
    origins._feed_field(hasher, size.to_bytes(4, "big") + digest)


def _request_shape(message):
    # The Claude converter preserves the boundaries of text blocks before
    # tools in its private metadata. The prefix matcher already proved order
    # and excluded unsupported image/interleaved forms.
    ordinary = google_metadata(message).get("ordinary_parts", [])
    if not isinstance(ordinary, list) or len(ordinary) > _MAX_PUBLIC_TEXT_BLOCKS:
        return None
    shape = origins._digest(b"claude-shape-v1")
    for part in ordinary:
        if not isinstance(part, dict) or part.get("kind") != "text" or not isinstance(part.get("text"), str):
            return None
        piece = origins._visible_field(part["text"])
        if piece[0] < 0:
            return None
        _shape_field(shape, piece)
    return shape.digest()


def _unpack(data, count):
    """Length-delimited bytes; empty slots are observably unsigned."""
    items, offset = [], 0
    for _ in range(count):
        if offset + _LENGTH_BYTES > len(data):
            return None
        size = int.from_bytes(data[offset:offset + _LENGTH_BYTES], "big")
        offset += _LENGTH_BYTES
        if size > origins._MAX_SIGNATURE_BYTES or offset + size > len(data):
            return None
        items.append(data[offset:offset + size])
        offset += size
    return items if offset == len(data) else None


def bind_omitted_claude_projection(request, metadata):
    """Replay only a delivered projection in the same explicit display mode.

    A currently omitted request does not silently treat an edited previous
    visible thinking block as omitted. A hit must come from a Claude response
    produced under the same omission policy. Standard origins are still safe
    when the client actually echoes its thinking block unchanged.
    """
    scope = _scope(metadata)
    for message in request.messages:
        if not message.tool_calls or message._signature_observed_reasoning is not None:
            continue
        slots = message._signature_slots
        shape = _request_shape(message) if scope is not None and slots else None
        record = origins.signature_store.get_record(_batch_key(scope, slots, shape)) if shape is not None else None
        # Do not try a standard origin keyed only to the now missing thinking
        # (nor any bare call-ID alias) if the omitted projection lacks proof.
        message._signature_slots = ()
        if record is None or record.state is not SignatureState.SIGNED:
            continue
        signatures = _unpack(record.signature, len(message.tool_calls))
        if signatures is None:
            continue
        restored = []
        for call, signature in zip(message.tool_calls, signatures, strict=True):
            call = dict(call)
            if signature:
                attach_signature_extra(call, signature)
            restored.append(call)
        message.tool_calls = restored


class OmittedClaudeProjection:
    """Staging area owned by a single actual public response."""
    max_encoded_signature = origins._MAX_ENCODED_SIGNATURE

    def __init__(self, prefix, metadata):
        self.scope = _scope(metadata)
        self.prefix = prefix if self.scope is not None else None
        self.text = origins.VisibleText()
        self._text_block = None
        self._shape = origins._digest(b"claude-shape-v1")
        self._blocks = 0
        self._proof = None
        self._withheld = False
        self._source = None
        self._buffered_after_tool = False
        self._ordered_count = None
        # Per-call base64 tail padding is charged in addition to the raw
        # batch bound; decoded bytes are independently checked at prepare.
        self._remaining_encoded = ((origins._MAX_BATCH_SIGNATURE_BYTES + 2) // 3
                                   + origins._MAX_CALLS - 1) * 4

    def discard(self):
        self.prefix = self._proof = None

    def withhold(self):
        self._withheld = True

    def source(self, source_id):
        if not isinstance(source_id, str) or not source_id or len(source_id) > 256:
            self.discard()
        elif self._source is None:
            self._source = source_id
        elif self._source != source_id:
            self.discard()

    def check_order(self, value):
        if not self.prefix or not value:
            return
        # Express fake streaming announces the complete topology on its
        # opening role frame, before it emits the fragmented tool calls.
        extra = google_metadata({"extra_content": value})
        # Express emits part_order alone for a tool-only response. The
        # topology validator below still requires all contiguous tool slots
        # and rejects an explicit non-list ordinary_parts value.
        order, ordinary = extra.get("part_order"), extra.get("ordinary_parts", [])
        count = 0
        if order is not None:
            if not isinstance(order, list) or not isinstance(ordinary, list):
                self.discard()
                return
            count = len(order) - len(ordinary)
            if (not 0 <= count <= origins._MAX_CALLS
                    or self._ordered_count is not None and self._ordered_count != count):
                self.discard()
                return
            self._ordered_count = count
        if not origins._plain_metadata(value, count):
            self.withhold()

    def reserve_signature(self, size):
        if self._withheld:
            return False
        self._remaining_encoded -= size
        if self._remaining_encoded < 0:
            self.withhold()
            return False
        return True

    def add_text(self, text, *, start):
        if not self.prefix:
            return
        if start:
            if self._blocks >= _MAX_PUBLIC_TEXT_BLOCKS:
                self.discard()
                return
            self._blocks += 1
            if self._text_block is not None:
                _shape_field(self._shape, self._text_block.result())
            self._text_block = origins.VisibleText()
        self.text.feed(text)
        self._text_block.feed(text)
        if self.text.size < 0 or self._text_block.size < 0:
            self.discard()

    def add_buffered_block(self, block):
        if block["type"] == "tool_use":
            self._buffered_after_tool = True
        elif block["type"] == "text" and block["text"] and self._buffered_after_tool:
            self.discard()
        # Text itself is recorded at the point of actual serialization.

    def prepare_json(self, original, translated):
        if not self.prefix or translated.get("stop_reason") != "tool_use":
            return
        choice = original["choices"][0]  # Already validated by the public translator.
        if choice["finish_reason"] != "tool_calls":
            return
        message = choice["message"]
        calls = message.get("tool_calls") or []
        self.check_order(message.get("extra_content"))
        public_tools = []
        for block in translated["content"]:
            self.add_buffered_block(block)
            if not self.prefix:
                return
            if block["type"] == "tool_use":
                public_tools.append(block)
            elif block["type"] == "text":
                self.add_text(block["text"], start=True)
            else:
                self.discard()
                return
        if len(calls) != len(public_tools):
            return
        # Preserve each original call's exact ID and args. No generated IDs
        # or reorderings can become a projection alias.
        if any(call.get("id") != public["id"] for call, public in zip(calls, public_tools, strict=True)):
            return
        self.prepare(
            (call["id"], call["function"]["name"], call["function"].get("arguments"), call)
            for call in calls
        )

    def prepare(self, calls):
        """Pair the actual emitted calls with their internal per-part metadata."""
        if not self.prefix:
            return
        tags, signatures, seen = [], [], set()
        size, text = self.text.result()
        budget = origins._Budget()
        remaining_sig = origins._MAX_BATCH_SIGNATURE_BYTES
        try:
            budget.consume(size)
            for ident, name, arguments, source in calls:
                if (not isinstance(ident, str) or not ident or ident in seen
                        or len(tags) >= origins._MAX_CALLS):
                    raise ValueError
                if arguments is None:
                    arguments = "{}"
                payload = {"id": ident, "function": {"name": name, "arguments": arguments}}
                tags.append(origins._call_tag(payload, budget))
                seen.add(ident)
                if not self._withheld:
                    signature = self._signature(google_metadata(source).get("thought_signature"), remaining_sig)
                    if signature is None:
                        self.withhold()
                        signatures.clear()
                    else:
                        signatures.append(signature)
                        remaining_sig -= len(signature)
        except (AttributeError, binascii.Error, KeyError, TypeError, ValueError, OverflowError, RecursionError):
            self.discard()
            return
        if not tags or self._ordered_count is not None and self._ordered_count != len(tags):
            return
        # A wholly unsigned accepted generation still records actual topology,
        # including the absence of any signature, to detect later collisions.
        prefix = origins._step_prefix(self.prefix, text, b"")
        if self._text_block is not None:
            _shape_field(self._shape, self._text_block.result())
            self._text_block = None
        key = _batch_key(self.scope, origins._slot_keys(prefix, tags), self._shape.digest())
        proof = (None if self._withheld else
                 b"".join(len(signature).to_bytes(_LENGTH_BYTES, "big") + signature for signature in signatures))
        self._proof = key, proof

    def _signature(self, encoded, remaining):
        if encoded is None:
            return b""
        if not isinstance(encoded, str) or len(encoded) > self.max_encoded_signature:
            return None
        try:
            signature = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            return None
        if (not signature or len(signature) > min(remaining, origins._MAX_SIGNATURE_BYTES)
                or signature in SKIP_VALIDATOR_SENTINELS or base64.b64encode(signature).decode() != encoded):
            return None
        return signature

    async def publish(self):
        if self._proof is not None:
            key, data = self._proof
            if data is None:
                origins.mark_unrestorable_origin(key)
            else:
                origins.signature_store.register_call_signature(key, data)
        self.discard()

    @property
    def ready(self):
        return self._proof is not None
