"""Match a complete parallel tool batch without changing input history."""

from collections.abc import Callable, Sequence

from models import OpenAIMessage


def order_parallel_results(
    calls: Sequence[dict],
    results: Sequence[OpenAIMessage],
    id_key: Callable[[object], str],
) -> Sequence[OpenAIMessage]:
    if len(calls) != len(results):
        return results
    names = {}
    for call in calls:
        ident = id_key(call.get("id"))
        name = (call.get("function") or {}).get("name")
        if not ident or not name or ident in names:
            return results
        names[ident] = name
    by_id = {}
    for result in results:
        ident = id_key(result.tool_call_id)
        if ident not in names or ident in by_id or (result.name and result.name != names[ident]):
            return results
        by_id[ident] = result
    return [by_id[ident] for ident in names]
