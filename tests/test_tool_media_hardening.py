"""Request-level regressions for tool media, schema fidelity and bounded IO."""
import asyncio
import base64
import copy
import gzip
import io
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-review-hardening-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from google import genai
from google.genai import types
from PIL import Image

import config
import http_io
import impersonate
import logger
import main
import message_processing as mp
from claude_protocol import anthropic_to_openai_request
from credential_manager import credential_manager
from generation_config import create_generation_config
from models import OpenAIRequest
from response_validation import IncompleteResponseError
from runtime_state import app_state
from schema_validation import SchemaValidationError
from upstream_errors import extract_upstream_error
from upstreams import cookie_native, cookie_proxy, cookie_request, cookie_transport
from routes import gemini_native


MODEL = "gemini-3.8-flash"
SCHEMA = {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}


def image_block(color="red"):
    data = io.BytesIO()
    Image.new("RGB", (2, 2), color).save(data, format="PNG")
    return {"type": "image", "source": {"type": "base64", "media_type": "image/png",
            "data": base64.b64encode(data.getvalue()).decode("ascii")}}


def tool_request(*, parallel=False, error=False):
    calls = [{"type": "tool_use", "id": "one", "name": "screenshot", "input": {}}]
    results = [{"type": "tool_result", "tool_use_id": "one", "is_error": error,
                "content": [{"type": "text", "text": "before"}, image_block(),
                            {"type": "text", "text": "after"}]}]
    if parallel:
        calls.append({"type": "tool_use", "id": "two", "name": "screenshot", "input": {}})
        results.append({"type": "tool_result", "tool_use_id": "two", "content": [image_block("blue")]})
    return anthropic_to_openai_request({"max_tokens": 100, "messages": [
        {"role": "assistant", "content": calls}, {"role": "user", "content": results}]}, MODEL)


class ImageToolSelectionTests(unittest.TestCase):
    def test_image_search_is_retained_and_disabled_or_custom_tools_are_omitted(self):
        for declaration in ({"type": "google_search"}, {"type": "web_search"},
                            {"type": "function", "function": {"name": "search"}}):
            for choice in (None, "none"):
                with self.subTest(declaration=declaration, choice=choice):
                    result = create_generation_config(OpenAIRequest(model="gemini-3-pro-image",
                        messages=[{"role": "user", "content": "draw"}],
                        tools=[declaration], tool_choice=choice))
                    expected = declaration["type"] != "function" and choice != "none"
                    self.assertEqual(result.get("tools", []), [{"google_search": {}}] if expected else [])


class GrpcRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_structured_grpc_code_wins_over_misleading_message(self):
        for code, message, expected, status_code in (
            (8, "Please retry the operation.", "retryable_error", 429),
            (14, "Service is restarting.", "retryable_error", 503),
            (3, "Invalid quota parameter.", "fatal_error", 400),
            (5, "Model name contains internal.", "fatal_error", 404),
        ):
            @asynccontextmanager
            async def stream(*args, **kwargs):
                yield httpx.Response(200, json={"results": [{"errors": [
                    {"message": message, "extensions": {"status": {"code": code}}}]}]})
            with self.subTest(code=code):
                events = [event async for event in cookie_transport._execute_stream_request_generator_once(
                    SimpleNamespace(stream=stream), {}, {})]
                self.assertEqual([(kind, data.status_code) for kind, data in events], [(expected, status_code)])


class NativeSchemaTests(unittest.TestCase):
    def test_json_schema_parameters_keep_refs_properties_and_required(self):
        schema = {"$defs": {"path": {"type": "string"}}, "type": "object",
                  "properties": {"path": {"$ref": "#/$defs/path"}}, "required": ["path"]}
        parsed = {"tools": [{"functionDeclarations": [{"name": "read_file", "parametersJsonSchema": schema}]}]}
        before = copy.deepcopy(parsed)
        variables = cookie_native.native_request_to_variables(parsed, MODEL, "p")
        converted = variables["tools"][0]["functionDeclarations"][0]["parameters"]
        self.assertEqual(converted, {"type": "OBJECT", "properties": [
            {"key": "path", "value": {"type": "STRING"}}], "required": ["path"]})
        self.assertEqual(parsed, before)

    def test_conflicting_or_invalid_parameter_schemas_are_rejected(self):
        for fields in ({"parameters": SCHEMA, "parametersJsonSchema": SCHEMA},
                       {"parametersJsonSchema": []}, {"parametersJsonSchema": {"$ref": "https://invalid.test"}}):
            with self.subTest(fields=fields), self.assertRaises(SchemaValidationError):
                cookie_native.native_request_to_variables({"tools": [{"functionDeclarations": [
                    {"name": "read_file", **fields}]}]}, MODEL, "p")

    def test_response_json_schema_obeys_gate_and_converts_without_mutating_request(self):
        parsed = {"generationConfig": {"responseMimeType": "application/json", "responseJsonSchema": SCHEMA}}
        before = copy.deepcopy(parsed)
        with mock.patch.object(app_state, "get_setting", return_value=False), self.assertRaises(SchemaValidationError):
            cookie_native.native_request_to_variables(parsed, MODEL, "p")
        with mock.patch.object(app_state, "get_setting", return_value=True):
            gen = cookie_native.native_request_to_variables(parsed, MODEL, "p")["generationConfig"]
        self.assertNotIn("responseJsonSchema", gen)
        self.assertEqual(gen["responseSchema"]["required"], ["path"])
        self.assertEqual(parsed, before)


class ToolMediaTests(unittest.TestCase):
    def test_parallel_results_keep_ids_images_and_text_in_both_transports(self):
        request = tool_request(parallel=True)
        before = request.model_dump()
        sdk = mp.create_gemini_prompt(request.messages, MODEL)
        self.assertEqual(len(sdk), 2)
        responses = [part.function_response for part in sdk[-1].parts]
        self.assertEqual([part.id for part in responses], ["one", "two"])
        self.assertEqual(responses[0].response["result"], [
            {"type": "text", "text": "before"}, {"$ref": "tool-image-1"},
            {"type": "text", "text": "after"}])
        self.assertTrue(all(part.parts[0].inline_data.data.startswith(b"\x89PNG") for part in responses))
        wire = cookie_request._convert_messages_to_contents(request.messages, MODEL)[0]
        self.assertEqual(len(wire), 2)
        for part in wire[-1]["parts"]:
            response = part["functionResponse"]
            blob = response["parts"][0]["inlineData"]
            self.assertEqual(blob["displayName"], response["response"]["result"][1 if response["id"] == "one" else 0]["$ref"])
            self.assertTrue(base64.b64decode(blob["data"]).startswith(b"\x89PNG"))
        self.assertEqual(request.model_dump(), before)

    def test_error_images_remain_errors_and_never_enter_json_as_base64(self):
        request = tool_request(error=True)
        response = mp.create_gemini_prompt(request.messages, MODEL)[-1].parts[0].function_response
        self.assertIn("error", response.response)
        self.assertNotIn("base64", json.dumps(response.response))
        self.assertEqual(response.parts[0].inline_data.display_name, "tool-image-1")

    def test_cookie_text_fallback_keeps_visual_input_and_never_dumps_base64_as_text(self):
        request = tool_request()
        parts = cookie_request._convert_messages_to_contents(request.messages, MODEL, native_tools=False)[0][-1]["parts"]
        self.assertIn("inlineData", parts[-1])
        self.assertIn("before", parts[0]["text"])
        self.assertNotIn("base64", parts[0]["text"])
        self.assertTrue(cookie_transport.is_native_tool_error("Unknown field parts in FunctionResponse"))

    def test_openai_image_urls_use_bounded_fetch_and_failed_media_is_a_client_error(self):
        block = {"type": "image_url", "image_url": {"url": "https://images.test/screen.png"}}
        data = base64.b64decode(image_block()["source"]["data"])
        with mock.patch.object(mp, "fetch_remote_image", return_value=(data, "image/png")) as fetch:
            response, media = mp.tool_response_with_media([block], MODEL)
        fetch.assert_called_once_with("https://images.test/screen.png")
        self.assertEqual(response["result"], [{"$ref": "tool-image-1"}])
        self.assertEqual(media[0][0], data)
        with mock.patch.object(mp, "fetch_remote_image", return_value=None), self.assertRaises(SchemaValidationError) as error:
            mp.tool_response_with_media([block], MODEL)
        self.assertEqual(extract_upstream_error(error.exception)[0], 400)

    def test_legacy_models_and_malformed_images_are_explicitly_rejected(self):
        for model, block in (("gemini-2.5-flash", image_block()),
                             (MODEL, {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "!!!!"}})):
            with self.subTest(model=model), self.assertRaises(SchemaValidationError):
                mp.tool_response_with_media([block], model)


class CountingStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks, self.reads, self.closed = chunks, 0, False

    async def __aiter__(self):
        for chunk in self.chunks:
            self.reads += 1
            yield chunk

    async def aclose(self):
        self.closed = True


class CookieMediaFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_official_sdk_serializes_native_images_and_parallel_ids(self):
        captured = []

        async def handle(request):
            captured.append(json.loads(request.content))
            return httpx.Response(200, json={"candidates": [{"content": {
                "role": "model", "parts": [{"text": "done"}]}, "finishReason": "STOP"}]})

        client = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(handle)}))
        try:
            request = tool_request(parallel=True)
            await client.aio.models.generate_content(model=MODEL,
                contents=mp.create_gemini_prompt(request.messages, MODEL),
                config={"automatic_function_calling": {"disable": True}})
        finally:
            await client.aio.aclose()
            client.close()
        responses = [part["functionResponse"] for part in captured[0]["contents"][-1]["parts"]]
        self.assertEqual([response["id"] for response in responses], ["one", "two"])
        for response in responses:
            part = response["parts"][0]
            blob = part.get("inlineData", part.get("inline_data"))
            self.assertTrue(base64.b64decode(blob["data"]).startswith(b"\x89PNG"))
            self.assertEqual(blob.get("displayName", blob.get("display_name")), "tool-image-1")
            self.assertNotIn("base64", json.dumps(response["response"]))

    async def test_rejected_multimodal_function_response_degrades_once_with_images(self):
        request = tool_request()
        request.tools = [{"type": "function", "function": {"name": "screenshot", "parameters": {"type": "object"}}}]
        bodies = []

        @asynccontextmanager
        async def stream(*args, **kwargs):
            bodies.append(kwargs["json"])
            if len(bodies) == 1:
                yield httpx.Response(400, text="Unknown field parts in FunctionResponse")
            else:
                yield httpx.Response(200, json={"results": [{"data": {"candidates": [
                    {"content": {"parts": [{"text": "done"}]}, "finishReason": "STOP"}]}}]})

        cache = cookie_proxy._RequestBodyCache("project", MODEL, request, prefill_active=False, force_search=False)
        body = await cache.get(True)
        events = [event async for event in cookie_transport.stream_events(
            SimpleNamespace(stream=stream), {}, body, fallback_body=cache.degraded, model=MODEL)]
        self.assertEqual(len(bodies), 2)
        self.assertIn(("text", "done"), events)
        self.assertTrue(cache.native_latched)
        first = bodies[0]["variables"]["contents"][-1]["parts"][0]["functionResponse"]
        self.assertIn("parts", first)
        fallback = bodies[1]["variables"]["contents"][-1]["parts"]
        self.assertIn("inlineData", fallback[-1])
        self.assertNotIn("functionResponse", fallback[0])
        self.assertNotIn(image_block()["source"]["data"], fallback[0]["text"])


class ResponseBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_httpx_and_curl_posts_decode_gzip_once_and_keep_session_usable(self):
        content = gzip.compress(b'{"ok":true}')

        async def serve(reader, writer):
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
                for line in headers.split(b"\r\n"):
                    if line.lower().startswith(b"content-length:"):
                        await reader.readexactly(int(line.split(b":", 1)[1]))
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                             b"Content-Encoding: gzip\r\nConnection: close\r\nContent-Length: "
                             + str(len(content)).encode() + b"\r\n\r\n" + content)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        async with await asyncio.start_server(serve, "127.0.0.1", 0) as server:
            url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
            for profile in ("off", "chrome"):
                with mock.patch.object(impersonate, "get_impersonate_profile", return_value=profile):
                    client = impersonate._create_upstream_client({"timeout": 2, "trust_env": False})
                try:
                    for _ in range(2):
                        response = await client.post(url, json={})
                        self.assertEqual(response.json(), {"ok": True})
                        self.assertNotIn("content-encoding", response.headers)
                finally:
                    await client.aclose()

    async def test_already_buffered_native_body_reuses_bytes(self):
        body = b"buffered-native-response"
        response = httpx.Response(200, content=body)
        self.assertIs(await http_io.read_upstream_bytes(response), body)

    async def test_httpx_stops_and_closes_chunked_success_and_error_bodies_at_limit(self):
        for status in (200, 503):
            body = CountingStream([b"0123456789"] * 100)
            transport = httpx.MockTransport(lambda request: httpx.Response(status, stream=body))
            with mock.patch.object(http_io, "MAX_UPSTREAM_RESPONSE_BYTES", 15), \
                 mock.patch.object(http_io, "MAX_UPSTREAM_ERROR_BYTES", 15):
                async with http_io.BoundedAsyncClient(transport=transport) as client:
                    with self.assertRaises(IncompleteResponseError):
                        await client.post("https://upstream.test", json={})
            self.assertTrue(body.closed)
            self.assertEqual(body.reads, 2)

    async def test_gzip_is_decoded_once_and_streaming_is_not_whole_response_capped(self):
        raw = b'{"ok":true}'
        transport = httpx.MockTransport(lambda request: httpx.Response(200,
            headers={"content-encoding": "gzip"}, stream=CountingStream([gzip.compress(raw)])))
        async with http_io.BoundedAsyncClient(transport=transport) as client:
            response = await client.post("https://upstream.test")
            self.assertEqual(response.json(), {"ok": True})
            self.assertNotIn("content-encoding", response.headers)
            with mock.patch.object(http_io, "MAX_UPSTREAM_RESPONSE_BYTES", 1):
                async with client.stream("POST", "https://upstream.test") as stream:
                    self.assertEqual(await stream.aread(), raw)

    async def test_curl_post_streams_with_post_timeout_and_closes_on_budget_failure(self):
        reads = []
        async def chunks():
            for _ in range(100):
                reads.append(True)
                yield b"0123456789"
        raw = SimpleNamespace(status_code=200, headers=httpx.Headers(), aiter_content=chunks, aclose=mock.AsyncMock())
        session = SimpleNamespace(request=mock.AsyncMock(return_value=raw))
        with mock.patch.object(http_io, "MAX_UPSTREAM_RESPONSE_BYTES", 15):
            with self.assertRaises(IncompleteResponseError):
                await impersonate._CurlClientShim(session, 7, 1800).post("https://upstream.test", json={})
        self.assertEqual(len(reads), 2)
        raw.aclose.assert_awaited_once()
        self.assertEqual(session.request.await_args.kwargs["timeout"], 7)
        self.assertTrue(session.request.await_args.kwargs["stream"])

    async def test_native_nonstream_is_bounded_even_with_an_ordinary_httpx_client(self):
        body = CountingStream([b"0123456789"] * 100)
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=body))) as client:
            with mock.patch.object(http_io, "MAX_UPSTREAM_RESPONSE_BYTES", 15):
                with self.assertRaises(IncompleteResponseError):
                    await gemini_native._passthrough_once(client, "key", MODEL, "generateContent", "", b"{}", stream=False)
        self.assertTrue(body.closed)
        self.assertEqual(body.reads, 2)


class NativeCounterTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_and_stream_errors_count_once_through_real_middleware(self):
        async def failed(*args, **kwargs):
            raise cookie_native.NativeBridgeError(503, "fixture upstream failure")
            yield
        record = {"id": "fixture", "cookie": "SAPISID=fixture", "project_id": "project", "enabled": True}
        for action in ("generateContent", "streamGenerateContent"):
            counters = logger.ProxyStats()
            with mock.patch.object(main, "stats", counters), mock.patch.object(cookie_native, "stats", counters), \
                 mock.patch.object(credential_manager, "get_cookie_record", return_value=record), \
                 mock.patch.object(app_state, "get_channel_mode", return_value="web_proxy"), \
                 mock.patch.object(cookie_native, "_timed_events", failed), \
                 mock.patch.object(cookie_native, "build_native_body", return_value={}):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://local") as client:
                    response = await client.post(f"/v1beta/models/{MODEL}:{action}?alt=sse",
                        headers={"x-goog-api-key": config.API_KEY}, json={"contents": []})
            self.assertEqual(response.status_code, 503 if action == "generateContent" else 200)
            self.assertIn("fixture upstream failure", response.text)
            stats = counters.get_json_stats()
            self.assertEqual((stats["total"], stats["error"], stats["success"]), (1, 1, 0))


if __name__ == "__main__":
    unittest.main()
