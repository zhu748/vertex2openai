"""Check the actual SDK wire and Studio variables; a valid-looking dict is not enough."""

from contextlib import asynccontextmanager
from copy import deepcopy
import json
import unittest
from unittest import mock

import httpx
from google import genai
from google.genai import types

import main
from routes import claude_api
from runtime_state import app_state
from schema_validation import SchemaValidationError, sanitize_for_gemini
from test_req_round15_contracts import Round15Harness
from upstreams import cookie_delivery, cookie_native, cookie_proxy, express_sdk, native_passthrough


MODEL = "gemini-2.5-flash"
MESSAGES = [{"role": "user", "content": "What is the price?"}]


def nested(leaf):
    return {"type": "object", "properties": {"quotes": {"type": "array", "items": {
        "type": "object", "properties": {"currency": leaf}, "required": ["currency"],
        "additionalProperties": False}}}, "required": ["quotes"]}


def property_of(wire, name):
    properties = wire["properties"]
    if isinstance(properties, list):  # Studio's UI schema
        return next(p["value"] for p in properties if p["key"] == name)
    return properties[name]


class SchemaRouteHarness(Round15Harness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.channel = "api_key"
        self.settings.update(cookie_structured_output=True, fake_streaming=False,
                             cross_channel_fallback=False, redirect_format_model=False)
        self.sent = []
        self.reply = {"text": "ok"}
        self.usage_metadata = None
        self.finish_reason = "STOP"
        def wire_response(request):
            body = json.loads(request.content)
            self.sent.append(body)
            parts = self.reply if isinstance(self.reply, list) else [self.reply]
            candidate = {"candidates": [{"content": {"role": "model", "parts": parts},
                                         "finishReason": self.finish_reason}]}
            if self.usage_metadata is not None:
                candidate["usageMetadata"] = self.usage_metadata
            if "variables" in body:
                return httpx.Response(200, json={"results": [{"data": candidate}]})
            if ":streamGenerateContent" in str(request.url):
                return httpx.Response(200, text=f"data: {json.dumps(candidate)}\n\n",
                                      headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=candidate)

        transport = httpx.MockTransport(wire_response)
        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": transport}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        studio = httpx.AsyncClient(transport=transport)
        self.addAsyncCleanup(studio.aclose)
        @asynccontextmanager
        async def open_studio(*args, **kwargs):
            yield studio
        record = {"id": "fixture", "cookie": "SAPISID=offline", "project_id": "fixture", "enabled": True}
        for patch in (
            mock.patch.object(app_state, "get_channel_mode", side_effect=lambda: self.channel),
            mock.patch.object(main.app.state, "express_key_manager", self.key_manager),
            mock.patch.object(express_sdk, "get_pooled_express_client", return_value=sdk),
            mock.patch.object(app_state, "get_cookie_accounts", return_value=[record]),
            # This harness only sends native requests on Cookie. Never contact a provider if misrouted.
            mock.patch.object(native_passthrough.passthrough_client_pool, "get",
                              side_effect=AssertionError("unexpected Express native dispatch")),
            mock.patch.object(cookie_proxy.credential_manager, "record_cookie_success"),
            mock.patch.object(cookie_delivery, "build_headers", return_value={"authorization": "offline"}),
            mock.patch.object(cookie_native, "build_headers", return_value={"authorization": "offline"}),
            mock.patch.object(cookie_delivery, "open_upstream_client", open_studio),
            mock.patch.object(cookie_native, "open_upstream_client", open_studio),
            mock.patch.object(claude_api, "_available_models", mock.AsyncMock(return_value=[MODEL])),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://local",
                                        headers={"Authorization": "Bearer test-key", "X-Resumable": "false"})
        self.addAsyncCleanup(self.client.aclose)

    async def send_schema(self, protocol, purpose, schema, *, stream=False, tool_fields=None):
        self.sent.clear()
        body = {"model": MODEL, "messages": MESSAGES, "stream": stream}
        if protocol == "openai":
            path = "/v1/chat/completions"
            if purpose == "tool":
                body["tools"] = [{"type": "function", "function": {
                    "name": "quote", "parameters": schema, **(tool_fields or {})}}]
            else:
                body["response_format"] = {"type": "json_schema", "json_schema": {"name": "price", "schema": schema}}
        elif protocol == "claude":
            path = "/v1/messages"
            body["max_tokens"] = 48
            if purpose == "tool":
                body["tools"] = [{"name": "quote", "input_schema": schema, **(tool_fields or {})}]
            else:
                body["output_config"] = {"format": {"type": "json_schema", "schema": schema}}
        else:
            path = f"/v1beta/models/{MODEL}:generateContent"
            body = {"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}
            if purpose == "tool":
                body["tools"] = [{"functionDeclarations": [{"name": "quote", "parametersJsonSchema": schema}]}]
            else:
                body["generationConfig"] = {"responseMimeType": "application/json", "responseJsonSchema": schema}
        return await self.client.post(path, json=body)

    def assert_wire_schema(self, purpose, inspect):
        self.assertEqual(len(self.sent), 1)
        body = self.sent.pop()
        body = body.get("variables", body)
        schema = (body["tools"][0]["functionDeclarations"][0]["parameters"] if purpose == "tool"
                  else body["generationConfig"]["responseSchema"])
        leaf = property_of(property_of(schema, "quotes")["items"], "currency")
        # The typed SDK uses proto field names inside Schema; Studio uses JSON names.
        names = {"min_length": "minLength", "max_items": "maxItems"}
        inspect({names.get(k, k): v for k, v in leaf.items()})

    def routes(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            protocols = ("openai", "claude", "gemini") if channel == "web_proxy" else ("openai", "claude")
            for protocol in protocols:
                for purpose in ("tool", "response"):
                    with self.subTest(channel=channel, protocol=protocol, purpose=purpose):
                        yield protocol, purpose


class SchemaFidelityRoutes(SchemaRouteHarness):
    async def test_exact_representations_reach_both_real_wire_shapes(self):
        for protocol, purpose in self.routes():
            for leaf, expected in [
                    ({"const": "CNY"}, {"type": "STRING", "enum": ["CNY"]}),
                    ({"type": "string", "const": "CNY", "enum": ["CNY", "EUR"], "minLength": 2},
                     {"type": "STRING", "enum": ["CNY"], "minLength": 2}),
                    ({"type": "array", "items": False, "maxItems": 3},
                     {"type": "ARRAY", "maxItems": 0}),
                    ({"type": "array", "items": {"type": "string", "enum": ["CNY", "EUR"]}},
                     {"type": "ARRAY", "items": {"type": "STRING", "enum": ["CNY", "EUR"]}})]:
                with self.subTest(leaf=leaf):
                    schema = nested(leaf)
                    original = deepcopy(schema)
                    reply = await self.send_schema(protocol, purpose, schema)
                    self.assertEqual(reply.status_code, 200, reply.text[:256])
                    self.assert_wire_schema(purpose, lambda wire: self.assertEqual(wire, expected))
                    self.assertEqual(schema, original)

    async def test_unsupported_assertions_rejected_before_any_network_request(self):
        for protocol, purpose in self.routes():
            for leaf, keyword in [
                ({"not": {"type": "string"}}, "not"),
                ({"type": "object", "additionalProperties": {"not": {"type": "string"}}}, "not"),
                ({"oneOf": [{"type": "integer"}, {"type": "number"}]}, "oneOf"),
                ({"type": "integer", "enum": [0, 1]}, "enum"),
                ({"const": 7}, "const"),
                ({"const": "USD", "enum": ["CNY"]}, "const"),
                ({"type": "array", "prefixItems": [{"type": "string"}], "items": False}, "prefixItems"),
                ({"items": False}, "items"),
            ]:
                with self.subTest(leaf=leaf):
                    reply = await self.send_schema(protocol, purpose, nested(leaf))
                    self.assertEqual(reply.status_code, 400, reply.text[:256])
                    error = reply.json()["error"]["message"]
                    self.assertIn(keyword, error)
                    self.assertIn("currency", error)
                    self.assertEqual(self.sent, [])


class NormalizationTests(unittest.TestCase):
    def test_literal_in_ref_and_map_schema_is_not_mutated(self):
        literal = {"type": "string", "const": "CNY", "title": "Currency"}
        schema = {"type": "object", "$defs": {"currency": literal},
                  "additionalProperties": {"$ref": "#/$defs/currency"}}
        original = deepcopy(schema)
        converted = sanitize_for_gemini(schema)
        self.assertEqual(converted["additionalProperties"],
                         {"type": "string", "enum": ["CNY"], "title": "Currency"})
        self.assertEqual(sanitize_for_gemini(converted), converted)
        self.assertEqual(schema, original)

    def test_cannot_replace_assertions_with_unconstrained_or_conflicting_types(self):
        for schema, key in [
            ({"minProperties": 1}, "minProperties"), ({"required": ["id"]}, "required"),
            ({"type": "object", "properties": {"value": {"const": "CNY", "type": ["null", "integer"]}}}, "const"),
            ({"type": "array", "minItems": 1, "items": False}, "items"),
            ({"type": "string", "const": "CNY", "nullable": True}, "const"),
        ]:
            with self.subTest(schema=schema), self.assertRaisesRegex(SchemaValidationError, key):
                sanitize_for_gemini(schema)
