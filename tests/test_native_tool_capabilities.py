"""Native tools must not silently lose their requested semantics on the Cookie route."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from schema_validation import SchemaValidationError
from upstreams import cookie_native as bridge


MODEL = "gemini-3.5-flash-lite"


def convert(tools):
    return bridge.native_request_to_variables({"tools": tools}, MODEL, "project")


@pytest.mark.parametrize("kind", ["googleMaps", "google_maps", "codeExecution", "url_context",
                                  "retrieval", "computerUse", "fileSearch", "mcpServers"])
@pytest.mark.parametrize("mixed", [False, True])
def test_unrepresentable_tool_is_never_silently_dropped(kind, mixed):
    tools = [{"functionDeclarations": [{"name": "f"}]}] if mixed else []
    with pytest.raises(SchemaValidationError, match=kind):
        convert(tools + [{kind: {}}])


def test_unsupported_tool_in_same_entry_as_search_is_validated():
    with pytest.raises(SchemaValidationError, match="googleMaps"):
        convert([{"googleSearch": {}, "googleMaps": {}}])


@pytest.mark.parametrize("field", ["response", "responseJsonSchema", "response_json_schema"])
@pytest.mark.parametrize("schema", [{}, {"type": "object", "properties": {"order_id": {"type": "string"}}}])
def test_function_output_constraints_are_not_silently_discarded(field, schema):
    with pytest.raises(SchemaValidationError, match="response"):
        convert([{"functionDeclarations": [{"name": "f", field: schema}]}])


def test_function_output_schema_variants_are_exclusive():
    with pytest.raises(SchemaValidationError, match="不能同时声明 response 与 responseJsonSchema"):
        convert([{"functionDeclarations": [{"name": "f", "response": {}, "responseJsonSchema": {}}]}])


def test_search_options_are_not_silently_removed():
    with pytest.raises(SchemaValidationError, match="googleSearch"):
        convert([{"google_search": {"exclude_domains": ["example.org"]}}])


def test_plain_search_and_input_only_functions_still_work():
    assert convert([{"google_search": {}}])["tools"] == [{"googleSearch": {}}]
    decl = {"name": "f", "parameters": {"type": "object"}, "response": None}
    assert convert([{"functionDeclarations": [decl]}])["tools"][0]["functionDeclarations"] == [
        {"name": "f", "parameters": {"type": "OBJECT"}}]


class BeforeDispatch(unittest.IsolatedAsyncioTestCase):
    async def test_rejected_requests_get_400_before_delivery(self):
        record = {"cookie": "fake", "id": "fake", "project_id": "project"}
        for action in ("generateContent", "streamGenerateContent"):
            with self.subTest(action=action), \
                 patch.object(bridge.credential_manager, "get_cookie_record", return_value=record), \
                 patch.object(bridge, "build_headers", return_value={}), \
                 patch.object(bridge, "_events_with_retry", side_effect=AssertionError("unexpected dispatch")):
                result = await bridge._handle_native_request_impl(
                    SimpleNamespace(query_params={}), MODEL, action, {"tools": [{"googleMaps": {}}]},
                    requested_model=MODEL, deadline=0)
                assert result.status_code == 400
                assert json.loads(result.body)["error"]["status"] == "INVALID_ARGUMENT"


@pytest.mark.parametrize("tools", [
    [{"googleSearch": {}}],
    [{"functionDeclarations": [{"name": "f"}]}],
    [{"googleSearch": {}}, {"functionDeclarations": [{"name": "f"}]}],
])
@pytest.mark.parametrize("config, field", [
    ({"retrievalConfig": {"languageCode": "fr"}}, "retrievalConfig"),
    ({"retrieval_config": {"lat_lng": {"latitude": 45.5, "longitude": -73.6}}}, "retrievalConfig"),
    ({"include_server_side_tool_invocations": True}, "includeServerSideToolInvocations"),
    ({"includeServerSideToolInvocations": "false"}, "includeServerSideToolInvocations"),
])
def test_explicit_unrepresentable_tool_controls_fail_before_dispatch(tools, config, field):
    with pytest.raises(SchemaValidationError, match=field):
        bridge.native_request_to_variables({"tools": tools, "tool_config": config}, MODEL, "project")


@pytest.mark.parametrize("tools", [[{"googleSearch": {}}], [{"functionDeclarations": [{"name": "f"}]}]])
def test_noop_search_settings_do_not_change_wire_or_request(tools):
    config = {"retrievalConfig": {}, "includeServerSideToolInvocations": False}
    req = {"tools": tools, "toolConfig": config.copy()}
    assert bridge.native_request_to_variables(req, MODEL, "project") == convert(tools)
    assert req["toolConfig"] == config
