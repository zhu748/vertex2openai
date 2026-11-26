"""Align complete parallel result turns for Gemini's positional function protocol."""
from collections.abc import Iterator, Sequence

from models import OpenAIMessage
from thought_signatures import google_metadata, marked_literal_call_id, merge_by_order, replay_tool_call_id


def has_tool_history(messages: Sequence[OpenAIMessage]) -> bool:
    """A declaration-free continuation can still carry a previous call or result."""
    return any(message.role in ("tool", "function") or message.tool_calls for message in messages)


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
    names = {}
    for call in calls:
        ident = _base_id(call.get("id"), literal_ids)
        name = (call.get("function") or {}).get("name")
        if not ident or not name or ident in names:
            return results
        names[ident] = name
    by_id = {}
    for result in results:
        ident = _base_id(result.tool_call_id, literal_ids)
        if ident not in names or ident in by_id or (result.name and result.name != names[ident]):
            return results
        by_id[ident] = result
    return [by_id[ident] for ident in names]


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
        literal_ids = frozenset(ident for call in calls if (ident := marked_literal_call_id(call)) is not None)
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
