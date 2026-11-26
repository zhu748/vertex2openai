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


@pytest.mark.parametrize("part,valid", [
    (SERVER_CALL, True), (SERVER_RESPONSE, True), (INLINE, True), (FILE, True),
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


@pytest.mark.parametrize("sse", [False, True])
@pytest.mark.parametrize("part,expected", [(SERVER_CALL, 200), (SERVER_RESPONSE, 200),
                                          *((part, 504) for part in MIME_ONLY)])
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
