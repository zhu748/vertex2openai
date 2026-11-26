"""Shared search-tool recognition and explicit model-alias selection."""

from model_capabilities import get_profile, split_model_alias
from models import OpenAIRequest
from schema_validation import SchemaValidationError


BUILTIN_SEARCH_NAMES = frozenset({
    "google_search", "googlesearch", "google_search_retrieval",
    "web_search", "web_search_preview", "websearch", "search_web", "browse", "search",
})


def normalized_tool_name(tool) -> str:
    if not isinstance(tool, dict):
        return ""
    function = tool.get("function")
    function = function if isinstance(function, dict) else {}
    return str(function.get("name") or tool.get("name") or "").strip()


def is_builtin_search_name(name: str) -> bool:
    return name.lower().replace("-", "_") in BUILTIN_SEARCH_NAMES


def is_builtin_search_tool(tool) -> bool:
    if not isinstance(tool, dict):
        return False
    if tool.get("type") == "function" or isinstance(tool.get("function"), dict):
        return False  # A named client function must remain a client function.
    return (is_builtin_search_name(str(tool.get("type") or tool.get("name") or ""))
            or "google_search" in tool or "googleSearch" in tool)


def validate_builtin_search_options(tool: dict, label: str) -> None:
    """Both translated channels only emit a bare Google Search declaration.

    Never silently turn a client constraint into an unrestricted search. Null
    options and empty selectors carry no settings; a supplied allowlist (even
    an empty list) is not equivalent to omitting the allowlist.
    """
    for key, value in tool.items():
        if value is None:
            continue
        if key in ("type", "name") and isinstance(value, str) and is_builtin_search_name(value):
            continue
        if key in ("filters", "user_location") and isinstance(value, dict) and not value:
            continue
        if key in ("google_search", "googleSearch") and isinstance(value, dict) and all(
            item is None or (option in ("exclude_domains", "excludeDomains") and item == [])
            for option, item in value.items()
        ):
            continue
        raise SchemaValidationError(f"{label}.{key} is unsupported: only unconfigured built-in search "
                                    "can be sent to this upstream")


def has_search_suffix(model: str) -> bool:
    # -search 后缀判定统一大小写不敏感（2026-11 C-9）：原生端点与
    # model_capabilities / anti_truncation 均按 lower() 判，仅此处敏感，
    # 造成跨通道行为分叉（大写变体经原生端点可用、OpenAI 通道报 404）。
    return split_model_alias(model)[0].lower().endswith("-search")


def search_suffix_has_priority(request) -> bool:
    """An explicit search alias wins over tools offered for optional AUTO use."""
    choice = request.tool_choice
    automatic = choice is None or (isinstance(choice, str) and choice.lower() == "auto")
    return automatic and has_search_suffix(request.model)


def is_forced_builtin_search(request) -> bool:
    name = normalized_tool_name(request.tool_choice) if isinstance(request.tool_choice, dict) else ""
    declared_names = {normalized_tool_name(tool) for tool in request.tools or []
                      if isinstance(tool.get("function"), dict)}
    return bool(name and is_builtin_search_name(name) and name not in declared_names)


def requires_tool_call(choice: str | dict | None) -> bool:
    """After choice validation, both a named function and 'required' forbid plain text fallback."""
    return isinstance(choice, dict) or (isinstance(choice, str) and choice.lower() == "required")


def validate_model_tool_choice(model: str, choice: str | dict | None) -> None:
    """Image adapters cannot enforce a required function, including a named choice."""
    if requires_tool_call(choice) and get_profile(model)["is_image"]:
        raise SchemaValidationError("Image models cannot enforce a required or named tool_choice. "
                                    "Use auto/none, or choose a text model with function calling.")


def validate_parallel_tool_calls(
    request: OpenAIRequest, *, serial_setting: str = "parallel_tool_calls=false",
) -> None:
    """Reject an unenforceable call-count limit before generation, including replay."""
    choice = request.tool_choice
    if request.parallel_tool_calls is not False or not request.tools:
        return
    if (isinstance(choice, str) and choice.lower() == "none") or get_profile(request.model)["is_image"]:
        return  # No client function can be selected.
    if not any(isinstance(t.get("function"), dict) for t in request.tools):
        return  # Native search is not a callable client function.
    if isinstance(choice, dict) and is_forced_builtin_search(request):
        return
    # allowed_function_names restricts names, not how often the same name can
    # appear. Never discard a second call or release a partial batch on SSE.
    raise SchemaValidationError(f"{serial_setting} is unsupported with active custom tools: the upstream "
                                "cannot limit a response to one tool call. Omit this setting only if "
                                "your client can accept multiple calls in one response.")
