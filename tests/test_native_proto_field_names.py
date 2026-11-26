"""Both ProtoJSON field spellings have identical meaning; application map keys do not."""
from copy import deepcopy

import pytest

from schema_validation import SchemaValidationError
from upstreams.cookie_native import native_request_to_variables


MODEL = "gemini-3.5-flash-lite"


def convert(req):
    return native_request_to_variables(req, MODEL, "project")


def test_typed_aliases_in_instruction_sampling_and_tool_policy():
    camel = {"systemInstruction": {"parts": [{"text": "请保留"}]}, "generationConfig": {
        "maxOutputTokens": 71, "thinkingConfig": {"includeThoughts": True, "thinkingLevel": "low"}},
        "safetySettings": [{"category": "example", "threshold": "OFF"}],
        "toolConfig": {"functionCallingConfig": {"allowedFunctionNames": ["f"], "mode": "ANY"}},
        "tools": [{"functionDeclarations": [{"name": "f"}]}]}
    snake = {"system_instruction": camel["systemInstruction"], "generation_config": {
        "max_output_tokens": 71, "thinking_config": {"include_thoughts": True, "thinking_level": "low"}},
        "safety_settings": camel["safetySettings"],
        "tool_config": {"function_calling_config": {"allowed_function_names": ["f"], "mode": "ANY"}},
        "tools": [{"function_declarations": [{"name": "f"}]}]}
    original = deepcopy(snake)
    assert convert(snake) == convert(camel)
    assert snake == original


@pytest.mark.parametrize("field", ["cachedContent", "cached_content"])
def test_explicit_cached_context_rejected_on_cookie_not_ignored(field):
    with pytest.raises(SchemaValidationError, match="cachedContent"):
        convert({field: "projects/example/locations/example/cachedContents/test"})


@pytest.mark.parametrize("req,key", [
    ({"system_instruction": "", "systemInstruction": "test"}, "systemInstruction"),
    ({"generation_config": None, "generationConfig": {}}, "generationConfig"),
    ({"generation_config": {"top_p": .1, "topP": .2}}, "topP"),
    ({"generationConfig": {"image_config": {"imageSize": "1K", "image_size": None}}}, "imageSize"),
    ({"tool_config": {"functionCallingConfig": {"allowedFunctionNames": [], "allowed_function_names": []}}},
     "allowedFunctionNames"),
    ({"contents": [{"parts": [{"function_call": {}, "functionCall": {}}]}]}, "functionCall"),
    ({"tools": [{"function_declarations": [{"name": "f", "parametersJsonSchema": {}, "parameters_json_schema": {}}]}]},
     "parametersJsonSchema")])
def test_colliding_protocol_spellings_do_not_select_a_winner(req, key):
    with pytest.raises(SchemaValidationError, match=key):
        convert(req)


@pytest.mark.parametrize("gen", [{"candidate_count": 2}, {"response_logprobs": True}])
def test_snake_aliases_obey_same_capability_checks(gen):
    with pytest.raises(SchemaValidationError):
        convert({"generation_config": gen})


def test_content_parts_normalize_only_protocol_fields_and_preserve_opaque_maps():
    payload = {"function_response": 1, "functionResponse": 2, "image_size": {"mime_type": "do not touch"}}
    schema = {"type": "object", "properties": {k: {"type": "string"} for k in payload}}
    req = {"contents": [{"parts": [
        {"inline_data": {"mime_type": "image/png", "data": "eA=="}, "thought_signature": "YWJj"},
        {"file_data": {"file_uri": "gs://example/file", "mime_type": "text/plain"}},
        {"function_call": {"name": "f", "args": payload}},
        {"function_response": {"name": "f", "response": payload, "parts": [
            {"inline_data": {"mime_type": "image/png", "data": "eQ=="}}]}},
    ]}], "tools": [{"function_declarations": [{"name": "f", "parameters_json_schema": schema}]}]}
    before = deepcopy(req)
    v = convert(req)
    parts = v["contents"][0]["parts"]
    assert parts == [
        {"inlineData": {"mimeType": "image/png", "data": "eA=="}, "thoughtSignature": "YWJj"},
        {"fileData": {"fileUri": "gs://example/file", "mimeType": "text/plain"}},
        {"functionCall": {"name": "f", "args": payload}},
        {"functionResponse": {"name": "f", "response": payload, "parts": [
            {"inlineData": {"mimeType": "image/png", "data": "eQ=="}}]}},
    ]
    assert [entry["key"] for entry in v["tools"][0]["functionDeclarations"][0]["parameters"]["properties"]] == list(payload)
    assert req == before
