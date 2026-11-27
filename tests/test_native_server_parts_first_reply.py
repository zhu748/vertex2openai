"""Observe server-side tools/media, without interpreting the payload or executing a tool."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from google.genai import types

import first_reply
from main import app
from request_tracking import RequestTrace
from test_gemini_native_stats_retry import KeyManager
from upstreams import native_passthrough as native


def wire_part(**fields):
    return types.Part(**fields).model_dump(mode="json", by_alias=True, exclude_none=True)


SERVER_CALL = wire_part(tool_call=types.ToolCall(tool_type=types.ToolType.GOOGLE_SEARCH_WEB,
                                                args={"query": "yellow flowers"}, id="one"))
SERVER_RESPONSE = wire_part(tool_response=types.ToolResponse(tool_type=types.ToolType.GOOGLE_SEARCH_WEB,
                                                            response={"results": []}, id="one"))
INLINE = wire_part(inline_data=types.Blob(mime_type="application/octet-stream", data=b"a"))
FILE = wire_part(file_data=types.FileData(mime_type="image/png", file_uri="gs://fixture/example"))
MIME_ONLY = ({"inlineData": {"mimeType": "audio/wav"}}, {"fileData": {"mimeType": "image/png"}})
FUNCTION_RESULT = wire_part(function_response=types.FunctionResponse(name="lookup", response={}))
CALLS = (
    (wire_part(function_call=types.FunctionCall(name="lookup", args={}, id="one")), True),
    (wire_part(function_call=types.FunctionCall(partial_args=[types.PartialArg(json_path="$.field", bool_value=False)],
                                               will_continue=True)), True),
    ({"functionCall": {"name": "", "args": {}, "willContinue": False}}, False),
    ({"functionCall": {"id": "identity-only"}, "thoughtSignature": "AQI="}, False),
    ({"functionCall": {"name": " ", "args": {}}}, False),
    ({"functionCall": {"name": "lookup", "args": []}}, False),
    ({"functionCall": {"partialArgs": [{"jsonPath": "$.field"}], "willContinue": True}}, False),
)

# Vertex's PartialArg.delta is a oneof; google.protobuf.NullValue is present
# when its JSON value is null. Omitted/default fields are not witnesses.
def partial_call(*fields):
    return {"functionCall": {"partialArgs": list(fields), "willContinue": True}}


NULL_PART = partial_call({"jsonPath": "$.slot", "nullValue": None})
SNAKE_NULL = {"function_call": {"partial_args": [{"json_path": "$['a.b'][0]", "null_value": None}]}}
NULL_CONTROLS = (
    (NULL_PART, True), (SNAKE_NULL, True), (partial_call({"jsonPath": "$.x", "nullValue": "NULL_VALUE"}), True),
    (partial_call({"jsonPath": "$.x", "nullValue": None}, {"jsonPath": "$.y", "nullValue": None}), True),
    (partial_call({"jsonPath": "$.x", "nullValue": None, "boolValue": None,
                   "numberValue": None, "stringValue": None}), True),
    (partial_call({"json_path": "$.x", "null_value": "NULL_VALUE", "bool_value": None}), True),
    (partial_call({"jsonPath": "$.x", "boolValue": False, "numberValue": None}), True),
    (partial_call({"jsonPath": "$.x", "numberValue": 0, "stringValue": None}), True),
    (partial_call({"jsonPath": "$.x", "stringValue": "", "boolValue": None, "willContinue": True}), True),
    (partial_call({"jsonPath": "$.x", "boolValue": None, "stringValue": None}), False),
    (partial_call({"jsonPath": "$.x", "boolValue": False, "stringValue": None, "string_value": None}), False),
    (partial_call({"jsonPath": "$.x", "numberValue": 0, "boolValue": None, "willContinue": True}), False),
    ({"partMetadata": {"jsonPath": "$.x", "nullValue": None}}, False),
    (partial_call({"nullValue": None}), False), (partial_call({"jsonPath": "$.", "nullValue": None}), False),
    (partial_call({"jsonPath": "$.x[*]", "nullValue": None}), False),
    (partial_call({"jsonPath": "$", "nullValue": None}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": False}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": 0}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": None, "boolValue": False}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": None, "null_value": None}), False),
    (partial_call({"jsonPath": "$.x", "json_path": "$.y", "nullValue": None}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": None, "willContinue": True}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": None, "willContinue": "no"}), False),
    (partial_call({"jsonPath": "$.x", "boolValue": False, "numberValue": 0}), False),
    (partial_call({"jsonPath": "$.", "boolValue": False}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": None}, {"jsonPath": "$['x']", "nullValue": None}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": None}, {"jsonPath": "$.y", "nullValue": None},
                  {"jsonPath": "$['y']", "nullValue": None}), False),
    (partial_call({"jsonPath": "$.x", "nullValue": None},
                  {"jsonPath": "$.y", "nullValue": None, "boolValue": False}), False),
    ({"functionCall": {**NULL_PART["functionCall"], "partial_args": []}}, False),
    ({**NULL_PART, "function_call": {}}, False),
    (partial_call({"jsonPath": "$.x", "boolValue": False}), True),
    (partial_call({"jsonPath": "$.x", "numberValue": 0}), True),
    (partial_call({"jsonPath": "$.x", "stringValue": "", "willContinue": True}), True),
)


@pytest.mark.parametrize("part,valid", [
    (SERVER_CALL, True), (SERVER_RESPONSE, True), (INLINE, True), (FILE, True), (FUNCTION_RESULT, True),
    *((part, False) for part in MIME_ONLY),
    ({"toolCall": {"id": "only"}}, False),
    ({"toolCall": {"toolType": "TOOL_TYPE_UNSPECIFIED", "args": {"query": "q"}}}, False),
    ({"toolCall": {"toolType": "unknown", "args": {"query": "q"}}}, False),
    ({"toolCall": {"toolType": [], "args": {"query": "q"}}}, False),
    ({"toolCall": {"toolType": "URL_CONTEXT", "args": []}}, False),
    ({"toolResponse": {"toolType": "URL_CONTEXT", "response": {}, "id": "one"}}, False),
    ({"toolResponse": {"toolType": "URL_CONTEXT", "id": 7, "response": {"x": 1}}}, False),
    ({"inlineData": {"data": []}}, False), ({"fileData": {"fileUri": "  "}}, False),
    ({"audioTranscription": {"text": "held policy"}}, False),
])
def test_native_observers_agree_on_nontext_parts(part, valid):
    payload = {"candidates": [{"content": {"parts": [part]}}]}
    assert first_reply.native_has_reply(payload) is valid
    trace = RequestTrace("/native")
    trace.inspect_payload(payload)
    trace.inspect_payload(payload, sent=True)
    assert ("first_reply" in trace.timings) is valid
    assert ("first_sent_reply" in trace.timings) is valid
    assert "first_content" not in trace.timings and "first_tool" not in trace.timings


@pytest.mark.parametrize("part,valid", CALLS + NULL_CONTROLS)
def test_native_function_call_milestones_require_semantic_data(part, valid):
    payload = {"candidates": [{"content": {"parts": [part]}}]}
    assert first_reply.native_has_reply(payload) is valid
    trace = RequestTrace("/native")
    trace.inspect_payload(payload)
    trace.inspect_payload(payload, sent=True)
    assert ("first_reply" in trace.timings) is valid
    assert ("first_sent_reply" in trace.timings) is valid
    assert ("first_tool" in trace.timings) is valid
    assert trace.data["tool_deltas"] == int(valid)


@pytest.mark.parametrize("sse", [False, True])
@pytest.mark.parametrize("part,expected", [(SERVER_CALL, 200), (SERVER_RESPONSE, 200), (FUNCTION_RESULT, 200),
                                          *((part, 504) for part in MIME_ONLY),
                                          *((part, 200 if valid else 504) for part, valid in CALLS + NULL_CONTROLS)])
def test_actual_native_route_delayed_tail(part, expected, sse):
    async def run():
        clock = [100.]
        fake_time = SimpleNamespace(monotonic=lambda: clock[0], time=lambda: clock[0])
        first = json.dumps({"candidates": [{"content": {"parts": [part]}}]}).encode()
        last = b'{"candidates":[{"finishReason":"STOP"}]}'
        head, tail = ((b"data: " + first + b"\n\n", b"data: " + last + b"\n\n")
                      if sse else (b"[" + first + b",", last + b"]"))

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield head
                clock[0] += 20
                yield tail

        def wire(_):
            return httpx.Response(200, stream=Body(), headers={
                "content-type": "text/event-stream" if sse else "application/json"})

        async with (httpx.AsyncClient(transport=httpx.MockTransport(wire)) as upstream,
                    httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client):
            with (patch.object(app.state, "express_key_manager", KeyManager()),
                  patch.object(native.app_state, "get_channel_mode", return_value="api_key"),
                  patch.object(native, "get_retry_settings", return_value=(0, 0, 0)),
                  patch.object(native.passthrough_client_pool, "get", return_value=upstream),
                  patch.object(first_reply, "time", fake_time),
                  patch.object(first_reply, "timeout_seconds", return_value=10)):
                response = await client.post("/v1beta/models/gemini-test:streamGenerateContent"
                    + ("?alt=sse" if sse else ""), headers={"x-goog-api-key": "test-key", "X-Resumable": "false"},
                    json={"contents": [{"parts": [{"text": "hi"}]}]})
            assert response.status_code == expected
            if expected == 200:
                assert response.content.endswith(head + tail)
            else:
                assert response.json()["error"]["code"] == 504

    asyncio.run(run())


def test_official_sdk_explicit_null_field_is_present_and_empty_call_is_not():
    value = types.PartialArg.model_validate({"jsonPath": "$.value", "nullValue": None})
    absent = types.PartialArg.model_validate({"jsonPath": "$.value"})
    for obj, valid in ((value, True), (absent, False)):
        wire = obj.model_dump(mode="json", by_alias=True, exclude_unset=True)
        assert ("nullValue" in wire) is valid
        assert first_reply.native_has_reply({"candidates": [{"content": {"parts": [partial_call(wire)]}}]}) is valid
