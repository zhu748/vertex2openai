"""Provider resource scope is an address, not a model feature or default."""

from contextlib import asynccontextmanager
import json
from unittest.mock import AsyncMock, patch

import httpx
from google import genai
from google.genai import types

from first_reply import timeout_seconds
from main import app
from model_capabilities import get_profile, supports_transport
from model_resource import model_id_for_policy
from routes import claude_api
from runtime_state import AppState, app_state
from test_req_round15_contracts import Round15Harness
from upstreams import cookie_delivery, cookie_native, cookie_proxy, express_sdk, native_passthrough


MODEL = "gemini-3.6-flash"
PROJECT = "image-audio-studio"
PROMPT = [{"role": "user", "content": "Read the temperature."}]


def resource(project=PROJECT, location="global", model=MODEL):
    return f"projects/{project}/locations/{location}/publishers/google/models/{model}"


def test_policy_recognizes_typed_models_not_opaque_endpoints():
    for name in (resource(), "publishers/google/models/" + MODEL, "models/" + MODEL):
        assert model_id_for_policy(name) == MODEL
        assert get_profile(name) == get_profile(MODEL)
        assert supports_transport(name)
    opaque = f"projects/{PROJECT}/locations/global/endpoints/fixture"
    assert model_id_for_policy(opaque) == opaque
    image = "gemini-3.1-flash-image"
    assert get_profile(resource(model=image)) == get_profile(image)
    assert not supports_transport(resource(model=image))


class TranslatedScopedModelRoutes(Round15Harness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.channel = "api_key"
        self.settings.update(express_location="global", fake_streaming=False, cross_channel_fallback=False,
                             model_overrides={MODEL: {"default_max_tokens": 321, "first_reply_timeout_seconds": 18}})
        self.wire = []
        self.reply = {"text": "A fixture reply."}

        def upstream_wire(request):
            payload = json.loads(request.content)
            query = payload.get("variables", payload)
            self.wire.append((request.url.path, query))
            value = {"candidates": [{"content": {"role": "model", "parts": [self.reply]},
                                     "finishReason": "STOP"}]}
            return httpx.Response(200, json={"results": [{"data": value}]} if "variables" in payload else value)

        transport = httpx.MockTransport(upstream_wire)
        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": transport}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        studio = await self.enterAsyncContext(httpx.AsyncClient(transport=transport))
        record = {"id": "test-account", "cookie": "SAPISID=offline", "project_id": PROJECT, "enabled": True}
        self.key_manager.get_express_record = lambda **_: {"key": "offline-fixture", "project_id": PROJECT}

        @asynccontextmanager
        async def open_studio(*args, **kwargs):
            yield studio

        for member in (
            patch.object(app_state, "get_effective_settings", side_effect=lambda model, base=None:
                AppState.get_effective_settings(app_state, model, base=self.settings if base is None else base)),
            patch.object(app_state, "get_channel_mode", side_effect=lambda: self.channel),
            patch.object(app.state, "express_key_manager", self.key_manager),
            patch.object(app_state, "get_cookie_accounts", return_value=[record]),
            patch.object(express_sdk, "get_pooled_express_client", return_value=sdk),
            patch.object(claude_api, "_available_models", AsyncMock(return_value=[MODEL])),
            patch.object(cookie_proxy, "build_headers", return_value={"authorization": "offline"}),
            patch.object(cookie_delivery, "build_headers", return_value={"authorization": "offline"}),
            patch.object(cookie_native, "build_headers", return_value={"authorization": "offline"}),
            patch.object(cookie_delivery, "open_upstream_client", open_studio),
            patch.object(cookie_native, "open_upstream_client", open_studio),
            patch.object(native_passthrough.passthrough_client_pool, "get",
                         side_effect=AssertionError("unexpected live native client")),
        ):
            self.enterContext(member)
        self.client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
            base_url="http://local", headers={"Authorization": "Bearer test-key", "X-Resumable": "false"}))

    async def send(self, protocol, model, *, tools=False):
        schema = {"type": "object", "properties": {"station": {"type": "string"}}}
        self.reply = ({"functionCall": {"id": "fixture-call", "name": "temperature", "args": {"station": "a"}}}
                      if tools else {"text": "A fixture reply."})
        self.wire.clear()
        if protocol == "chat":
            path, body = "/v1/chat/completions", {"messages": PROMPT}
            if tools:
                body.update(tools=[{"type": "function", "function": {"name": "temperature", "parameters": schema}}],
                            tool_choice="required")
        elif protocol == "messages":
            path, body = "/v1/messages", {"messages": PROMPT, "max_tokens": 128}
            if tools:
                body.update(tools=[{"name": "temperature", "input_schema": schema}], tool_choice={"type": "any"})
        elif protocol == "responses":
            path, body = "/v1/responses", {"input": PROMPT, "store": False}
            if tools:
                body.update(tools=[{"type": "function", "name": "temperature", "parameters": schema}],
                            tool_choice="required")
        else:
            return await self.client.post(f"/v1beta/models/{model}:generateContent",
                                          json={"contents": [{"parts": [{"text": "Read the temperature."}]}]})
        return await self.client.post(path, json={**body, "model": model})

    async def test_qualified_and_bare_text_use_same_provider_url_tools_and_defaults(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("chat", "messages", "responses"):
                with self.subTest(channel=channel, protocol=protocol):
                    bare = await self.send(protocol, MODEL, tools=True)
                    self.assertEqual(bare.status_code, 200, bare.text[:220])
                    path, bare_body = self.wire[0]
                    scoped = await self.send(protocol, resource(), tools=True)
                    self.assertEqual(scoped.status_code, 200, scoped.text[:220])
                    self.assertEqual(len(self.wire), 1)
                    self.assertEqual(self.wire[0], (path, bare_body))
                    self.assertEqual(bare_body["toolConfig"]["functionCallingConfig"]["mode"], "ANY")
                    self.assertEqual(bare_body["generationConfig"]["maxOutputTokens"],
                                     128 if protocol == "messages" else 321)
                    if channel == "web_proxy":
                        self.assertEqual(bare_body["model"], resource())
                    else:
                        self.assertTrue(path.endswith("/" + resource() + ":generateContent"), path)
        self.assertEqual(timeout_seconds(resource()), 18)

    async def test_cookie_scope_is_checked_before_any_translated_or_native_dispatch(self):
        self.channel = "web_proxy"
        for protocol in ("chat", "messages", "responses", "native"):
            for target in (MODEL, resource(), f"models/{MODEL}", f"publishers/google/models/{MODEL}"):
                with self.subTest(protocol=protocol, target=target):
                    reply = await self.send(protocol, target)
                    self.assertEqual(reply.status_code, 200, reply.text[:220])
                    self.assertEqual(len(self.wire), 1)
                    self.assertEqual(self.wire[0][1]["model"], resource())
            for target in (resource(project="other"), resource(location="us-west2")):
                with self.subTest(protocol=protocol, target=target):
                    reply = await self.send(protocol, target)
                    self.assertEqual(reply.status_code, 400, reply.text[:220])
                    self.assertIn("本账号项目 global", reply.json()["error"]["message"])
                    self.assertEqual(self.wire, [])
