"""Align complete parallel result turns for Gemini's positional function protocol."""
from collections.abc import Iterator, Sequence

import config as app_config
from models import OpenAIMessage, normalize_content_part
from parallel_result_order import order_parallel_results
from signature_origin import origin_slot
from thought_signatures import (google_metadata, historical_literal_id, merge_by_order, replay_tool_call_id,
                                resolve_tool_call_signature, signature_from_extra)


def has_tool_history(messages: Sequence[OpenAIMessage]) -> bool:
    """A declaration-free continuation can still carry a previous call or result."""
    return any(message.role in ("tool", "function") or message.tool_calls for message in messages)


def current_signature_turn_start(messages: Sequence[OpenAIMessage]) -> int:
    """Only a fresh user prompt starts another Gemini validation turn, not a tool result."""
    for i in range(len(messages) - 1, -1, -1):
        msg = messages[i]
        if msg.role != "user":
            continue
        if isinstance(msg.content, str) and msg.content:
            return i
        if isinstance(msg.content, list):
            for block in map(normalize_content_part, msg.content):
                if isinstance(block, dict) and (
                    block.get("type") == "image_url" or block.get("type") == "text" and block.get("text")
                ):
                    return i
    return 0


def first_wire_call_index(message: OpenAIMessage) -> int:
    """Native replay can put a different named slot first using an explicit Part order."""
    order = google_metadata(message).get("part_order")
    if isinstance(order, list):
        for slot in order:
            if (isinstance(slot, dict) and slot.get("type") == "tool_call"
                    and type(i := slot.get("index")) is int and 0 <= i < len(message.tool_calls)):
                return i
    return 0


def history_call_signature(message, index, current_required, literal_ids, *, first):
    call = message.tool_calls[index]
    ident = str(call.get("id") or "")
    explicit, _ = signature_from_extra(call, strict_encoding=app_config.GEMINI_STRICT_THOUGHT_SIGNATURES)
    return resolve_tool_call_signature(
        ident, require_signature=current_required and first, explicit_signature=explicit,
        explicit_unsigned=not first, origin_key=origin_slot(message, index),
        literal_id=ident in literal_ids,
    )


def _base_id(value, literal_ids) -> str:
    return replay_tool_call_id(value, literal_ids) if isinstance(value, str) else ""


def _order_siblings(message: OpenAIMessage, results: Sequence[OpenAIMessage], *,
                    native: bool, literal_ids: frozenset[str]) -> Sequence[OpenAIMessage]:
    calls = message.tool_calls
    if len(calls) != len(results):
        return results
    if native:
        # Match the calls actually placed on the native wire even when exact
        # replay metadata supplies an interleaved or non-default part order.
        calls = merge_by_order(google_metadata(message).get("part_order"), calls, ()) or calls
    return order_parallel_results(calls, results, lambda value: _base_id(value, literal_ids))


def gemini_history_with_ids(
    messages: Sequence[OpenAIMessage], *, native: bool = True
) -> Iterator[tuple[OpenAIMessage, frozenset[str]]]:
    """Carry a declaration's literal IDs only through its immediately adjacent results.

    Correlation by ID is valid for individual explicitly matching results,
    even if a batch is partial. Reorder only a complete, unique matching run.
    No global map, edits to message objects or change of user-turn boundaries.
    """
    empty = frozenset()
    i = 0
    while i < len(messages):
        message = messages[i]
        i += 1
        calls = message.tool_calls if message.role in ("assistant", "model") else None
        if not calls:
            yield message, empty
            continue
        literal_ids = frozenset(ident for n, call in enumerate(calls) if
                                (ident := historical_literal_id(call, origin_slot(message, n),
                                                                message._claude_literal_tool_ids)) is not None)
        yield message, literal_ids
        if len(calls) < 2 and not literal_ids:
            continue
        start = i
        while i < len(messages) and messages[i].role in ("tool", "function"):
            i += 1
        if i > start:
            results = messages[start:i]
            if len(calls) > 1:
                results = _order_siblings(message, results, native=native, literal_ids=literal_ids)
            for result in results:
                yield result, literal_ids


def gemini_history_order(messages: Sequence[OpenAIMessage], *, native: bool = True) -> Iterator[OpenAIMessage]:
    """Iterate the same ordered original messages without the ID encoding context."""
    return (message for message, _ in gemini_history_with_ids(messages, native=native))
