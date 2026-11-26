"""Validate loose OpenAI extension dictionaries before either upstream uses them."""

import model_capabilities as mc
from models import MAX_CANDIDATES, message_text, normalize_content_part, wants_json_output
from thought_signatures import google_metadata
from tool_arguments import parse_tool_arguments
from schema_validation import SchemaValidationError, validate_request_schemas
from tool_policy import (
    has_search_suffix,
    is_builtin_search_name,
    is_builtin_search_tool,
    normalized_tool_name,
    validate_builtin_search_options,
    validate_model_tool_choice,
    validate_parallel_tool_calls,
)


def _function(function, label):
    if not isinstance(function, dict):
        raise SchemaValidationError(f"{label} must be an object")
    name = function.get("name")
    if not isinstance(name, str) or not name.strip():
        raise SchemaValidationError(f"{label}.name must be a non-empty string")
    if any(ord(char) < 32 or ord(char) == 127 for char in name):
        raise SchemaValidationError(f"{label}.name must not contain control characters")


def _validate_legacy_functions(request):
    # Older OpenAI clients often serialize inactive fields as null or [].
    # Only ignore scalar choices when the modern request already has exactly
    # that effect; e.g. legacy "none" cannot silently enable modern tools.
    extra = request.model_extra or {}
    if extra.get("functions") not in (None, []):
        raise SchemaValidationError("functions is not supported; use tools with type 'function'")
    choice = extra.get("function_call")
    if choice is None:
        return
    modern = request.tool_choice.lower() if isinstance(request.tool_choice, str) else request.tool_choice
    uses_tools = bool(request.tools or has_search_suffix(request.model) or modern not in (None, "auto", "none"))
    if choice not in ("auto", "none") or (uses_tools and choice != (modern or "auto")):
        raise SchemaValidationError("function_call is not supported; use tool_choice")


def validate_openai_audio(request):
    """A text/image translation must not replace explicitly requested speech or its history."""
    extra = request.model_extra or {}
    modes = extra.get("modalities")
    if (extra.get("audio") is not None or modes == "audio"
            or isinstance(modes, list) and "audio" in modes):
        raise SchemaValidationError("Audio output is not supported by this chat endpoint.")
    for index, message in enumerate(request.messages):
        if message.role in ("assistant", "model") and (message.model_extra or {}).get("audio") is not None:
            raise SchemaValidationError(f"messages[{index}].audio: replay of assistant audio is not supported.")


def validate_openai_logit_bias(request):
    # Token IDs are tokenizer-specific; neither provider adapter can faithfully
    # translate a nonzero OpenAI bias. An all-zero map is behaviorally inert.
    bias = (request.model_extra or {}).get("logit_bias")
    if bias is not None and not (isinstance(bias, dict) and all(
            type(value) in (int, float) and value == 0 for value in bias.values())):
        raise SchemaValidationError("Nonzero or malformed logit_bias is not supported by this chat endpoint.")


def validate_model_response_format(request, *, label="response_format"):
    """The translated image adapters generate TEXT/IMAGE, not a guaranteed JSON response."""
    kind = (request.response_format or {}).get("type")
    if kind not in (None, "text", "json_object", "json_schema"):
        raise SchemaValidationError(f"Unsupported {label}.type; use text, json_object or json_schema.")
    if wants_json_output(request) and mc.get_profile(request.model)["is_image"]:
        raise SchemaValidationError(f"Image models do not support JSON output via {label}. "
                                    "Omit this format or use a text model.")


def validate_openai_search(request):
    """Channel-independent option checks before admitting or looking up a replay."""
    # This bridge's search selectors are the tool declaration and model
    # suffix. A populated Chat-level option cannot be honored by either.
    chat_search = (request.model_extra or {}).get("web_search_options")
    if chat_search is not None and not (isinstance(chat_search, dict) and not chat_search):
        raise SchemaValidationError("web_search_options is unsupported: this gateway cannot implement "
                                    "Chat-level search configuration")
    for index, tool in enumerate(request.tools or []):
        if is_builtin_search_tool(tool):
            validate_builtin_search_options(tool, f"tools[{index}]")


def validate_openai_request(request, *, sdk_schema=False):
    """Keep unknown extensions, but reject shapes that cannot be converted safely.

    Pydantic deliberately accepts arbitrary content/tool dictionaries for client
    compatibility. Their known nested fields still need checking; otherwise an
    AttributeError in a worker or stream becomes a misleading upstream 500.
    """
    validate_model_response_format(request)
    validate_openai_audio(request)
    validate_openai_logit_bias(request)
    _validate_legacy_functions(request)
    validate_openai_search(request)
    validate_request_schemas(request, sdk=sdk_schema)
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
    validate_model_tool_choice(request.model, choice)
    validate_parallel_tool_calls(request)
    for index, message in enumerate(request.messages):
        label = f"messages[{index}]"
        if message.role in ("assistant", "model") and (message.model_extra or {}).get("function_call") is not None:
            raise SchemaValidationError(f"{label}.function_call is not supported; use assistant tool_calls "
                                        "and tool results with matching tool_call_id")
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
        for part_index, part in enumerate(message.content if isinstance(message.content, list) else ()):
            part = normalize_content_part(part)
            if not isinstance(part, dict):
                continue
            part_label = f"{label}.content[{part_index}]"
            # Unimplemented OpenAI content types carry user data, unlike
            # optional extension metadata. Never silently drop them or turn an
            # attachment-only request into the default user prompt. Tool results
            # carry application-defined JSON and are not user content parts.
            if message.role not in ("tool", "function") and part.get("type") in ("input_audio", "file"):
                raise SchemaValidationError(f"{part_label}: unsupported content type {part['type']!r}")
            if part.get("type") == "text":
                if part.get("text") is not None and not isinstance(part["text"], str):
                    raise SchemaValidationError(f"{part_label}.text must be a string or null")
            elif part.get("type") == "refusal" and message.role in ("assistant", "model"):
                if not isinstance(part.get("refusal"), str):
                    raise SchemaValidationError(f"{part_label}.refusal must be a string")
            elif part.get("type") == "image_url":
                image = part.get("image_url")
                if image is None:
                    continue
                if isinstance(image, str):
                    continue  # Legacy clients also send the URL directly.
                if not isinstance(image, dict) or not isinstance(image.get("url", ""), str):
                    raise SchemaValidationError(f"{part_label}.image_url.url must be a string")
        # Exact replay metadata can otherwise override and silently erase a
        # standard refusal. Do not guess where to move signed native parts.
        replay = google_metadata(message).get("ordinary_parts")
        if message.has_assistant_refusal() and isinstance(replay, list):
            native_text = "".join(p["text"] for p in replay if isinstance(p, dict)
                                  and p.get("kind") == "text" and isinstance(p.get("text"), str))
            if native_text != message_text(message.replay_content()):
                raise SchemaValidationError(f"{label}: refusal history conflicts with google.ordinary_parts")
