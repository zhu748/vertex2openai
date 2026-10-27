"""Shared search-tool recognition and explicit model-alias selection."""

from anti_truncation import split_model_alias


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


def is_builtin_search_tool(tool, *, include_function_aliases=False) -> bool:
    if not isinstance(tool, dict):
        return False
    if tool.get("type") == "function" or isinstance(tool.get("function"), dict):
        # Cookie's documented legacy aliases do not change the meaning of a
        # genuine custom function called "search" on the Express channel.
        return include_function_aliases and is_builtin_search_name(normalized_tool_name(tool))
    return (is_builtin_search_name(str(tool.get("type") or tool.get("name") or ""))
            or "google_search" in tool or "googleSearch" in tool)


def has_search_suffix(model: str) -> bool:
    return split_model_alias(model)[0].endswith("-search")


def search_suffix_has_priority(request) -> bool:
    """An explicit search alias wins over tools offered for optional AUTO use."""
    choice = request.tool_choice
    automatic = choice is None or (isinstance(choice, str) and choice.lower() == "auto")
    return automatic and has_search_suffix(request.model)
