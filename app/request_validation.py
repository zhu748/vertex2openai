"""Validate loose OpenAI extension dictionaries before either upstream uses them."""

import model_capabilities as mc
from models import MAX_CANDIDATES, normalize_content_part
from tool_arguments import parse_tool_arguments
from schema_validation import SchemaValidationError, validate_request_schemas
from tool_policy import (
    has_search_suffix,
    is_builtin_search_name,
    is_builtin_search_tool,
    normalized_tool_name,
)


def _function(function, label):
    if not isinstance(function, dict):
        raise SchemaValidationError(f"{label} must be an object")
    name = function.get("name")
    if not isinstance(name, str) or not name.strip():
        raise SchemaValidationError(f"{label}.name must be a non-empty string")
    if any(ord(char) < 32 or ord(char) == 127 for char in name):
        raise SchemaValidationError(f"{label}.name must not contain control characters")


def validate_openai_request(request):
    """Keep unknown extensions, but reject shapes that cannot be converted safely.

    Pydantic deliberately accepts arbitrary content/tool dictionaries for client
    compatibility. Their known nested fields still need checking; otherwise an
    AttributeError in a worker or stream becomes a misleading upstream 500.
    """
    validate_request_schemas(request)
    declared_names = set()
    for index, tool in enumerate(request.tools or []):
        if "function" in tool or tool.get("type") == "function":
            if tool.get("type") not in (None, "function"):
                raise SchemaValidationError(f"tools[{index}].type must be function")
            name = tool["function"]["name"]
            if name in declared_names:
                raise SchemaValidationError(f"tools[{index}] duplicates a function name")
            declared_names.add(name)
        elif not is_builtin_search_tool(tool):
            raise SchemaValidationError(f"tools[{index}] has an unsupported tool type")
    if request.n is not None:
        if isinstance(request.n, bool) or not isinstance(request.n, int) or not 1 <= request.n <= MAX_CANDIDATES:
            raise SchemaValidationError(f"n must be an integer between 1 and {MAX_CANDIDATES}")
        if request.n > 1 and "candidate_count" not in mc.get_profile(request.model)["allowed_sampling"]:
            raise SchemaValidationError("This model does not support n > 1")
    choice = request.tool_choice
    if isinstance(choice, dict) and (choice.get("type") == "function" or "function" in choice):
        if choice.get("type") not in (None, "function"):
            raise SchemaValidationError("tool_choice.type must be function")
        _function(choice.get("function"), "tool_choice.function")
        name = normalized_tool_name(choice)
        declared = {normalized_tool_name(tool) for tool in request.tools or []}
        if name not in declared and not is_builtin_search_name(name):
            raise SchemaValidationError(f"tool_choice references unknown function: {name}")
    elif isinstance(choice, dict):
        raise SchemaValidationError("tool_choice must select a function or use auto/none/required")
    if isinstance(choice, str):
        if choice.lower() not in ("auto", "none", "required"):
            raise SchemaValidationError("tool_choice must be auto, none, required, or a named function")
        if choice.lower() == "required" and not (has_search_suffix(request.model) or any(
            normalized_tool_name(tool) or is_builtin_search_tool(tool) for tool in request.tools or []
        )):
            raise SchemaValidationError("tool_choice 'required' requires at least one tool")
    for index, message in enumerate(request.messages):
        label = f"messages[{index}]"
        call_ids = set()
        for call_index, call in enumerate(message.tool_calls or []):
            call_label = f"{label}.tool_calls[{call_index}]"
            if call.get("type") not in (None, "function"):
                raise SchemaValidationError(f"{call_label}.type must be function")
            _function(call.get("function"), f"{call_label}.function")
            call_id = call.get("id")
            if call_id is not None and not isinstance(call_id, str):
                raise SchemaValidationError(f"{call_label}.id must be a string")
            if call_id:
                if call_id in call_ids:
                    raise SchemaValidationError(f"{call_label}.id duplicates another call in this message")
                call_ids.add(call_id)
            parse_tool_arguments(call["function"].get("arguments"), label=f"{call_label}.function.arguments")
        if not isinstance(message.content, list):
            continue
        for part_index, part in enumerate(message.content):
            part = normalize_content_part(part)
            if not isinstance(part, dict):
                continue
            part_label = f"{label}.content[{part_index}]"
            if part.get("type") == "text":
                if part.get("text") is not None and not isinstance(part["text"], str):
                    raise SchemaValidationError(f"{part_label}.text must be a string or null")
            elif part.get("type") == "image_url":
                image = part.get("image_url")
                if image is None:
                    continue
                if isinstance(image, str):
                    continue  # Legacy clients also send the URL directly.
                if not isinstance(image, dict) or not isinstance(image.get("url", ""), str):
                    raise SchemaValidationError(f"{part_label}.image_url.url must be a string")
