"""Offline transport tests: native incremental delivery, real tools, both channels."""
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-antitruncation-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from google.genai import types
from google import genai
from pydantic import ValidationError

import anti_truncation as at
import api_helpers
import retry_policy
import config
import main
from routes import settings_api
import console_auth
import settings_validation
import model_capabilities as mc
import resumable
from models import OpenAIRequest
from routes import models_api
from runtime_state import AppState, app_state
from upstreams import cookie_proxy, express_sdk


MODEL = "gemini-3.6-flash"
TOOL = {"type": "function", "function": {"name": "read_database", "parameters": {
    "type": "object", "properties": {"query": {"type": "string"}}}}}


def request(**kwargs):
    return OpenAIRequest(model=MODEL, messages=[{"role": "user", "content": "hello"}], **kwargs)


def transport_request(mode="stream", **kwargs):
    return request(**kwargs).model_copy(update={"model": MODEL + "-antitruncation-" + mode})


def response(*parts, finish="STOP", usage=False):
    return types.GenerateContentResponse(
        candidates=[types.Candidate(content=types.Content(role="model", parts=list(parts)), finish_reason=finish)],
        usage_metadata=(types.GenerateContentResponseUsageMetadata(
            prompt_token_count=10, candidates_token_count=5, total_token_count=15) if usage else None))


def function(name, args=None, **kwargs):
    return types.Part(function_call=types.FunctionCall(name=name, args=args, **kwargs))


def chunks(body):
    return [json.loads(line[6:]) for line in body.splitlines()
            if line.startswith("data: ") and line != "data: [DONE]"]


def content(body):
    return "".join(c.get("delta", {}).get("content") or ""
                   for chunk in chunks(body) for c in chunk.get("choices", []))


class PrepareTests(unittest.TestCase):
    def isolated_state(self, settings=None):
        state = AppState()
        state._state = {"settings": settings or {}}
        patch = mock.patch.object(state, "_save")
        patch.start()
        self.addCleanup(patch.stop)
        return state

    def test_only_argon_and_38_flash_publish_aliases_and_plain_ids_never_enable(self):
        state = self.isolated_state()
        models = json.loads((Path(__file__).resolve().parents[1] / "vertexModels.json").read_text())["models"]
        enabled = {"gemini-4-argon", "gemini-3.8-flash"}
        self.assertTrue(enabled <= set(models))
        for model in models:
            req = request(stream=True).model_copy(update={"model": model})
            settings = state.get_effective_settings(model)
            _, transport = at.prepare_transport(req, model, settings, native_stream=True)
            with self.subTest(model=model):
                self.assertEqual(settings["anti_truncation_mode"], "both" if model in enabled else "off")
                self.assertIsNone(transport)
                self.assertEqual(len(at.listed_suffixes(model, settings)), 2 if model in enabled else 0)
        self.assertEqual(state.get_model_overrides(), state.get_settings()["model_overrides"])

    def test_model_defaults_survive_legacy_maps_and_partial_saved_parameters(self):
        state = self.isolated_state({"anti_truncation_mode": "off", "model_overrides": {
            "gemini-4-argon": {"thinking_g3_level": "low"},
            MODEL: {"default_max_tokens": 500},
        }})
        for model in ("gemini-4-argon", "gemini-3.8-flash"):
            self.assertEqual(state.get_effective_settings(model)["anti_truncation_mode"], "both")
        self.assertEqual(state.get_effective_settings(MODEL)["anti_truncation_mode"], "off")
        saved = state.set_model_override("gemini-4-argon", {"thinking_g3_level": "medium"})
        self.assertEqual(saved, {"thinking_g3_level": "medium", "anti_truncation_mode": "both"})
        copy = state.get_model_overrides()
        copy["gemini-4-argon"]["anti_truncation_mode"] = "off"
        self.assertEqual(state.get_effective_settings("gemini-4-argon")["anti_truncation_mode"], "both")
        self.assertEqual(config.DEFAULT_SETTINGS["model_overrides"]["gemini-4-argon"]["anti_truncation_mode"], "both")

    def test_defaults_can_be_disabled_and_restored_without_affecting_other_models(self):
        state = self.isolated_state()
        for model in ("gemini-4-argon", "gemini-3.8-flash"):
            state.set_model_override(model, {"anti_truncation_mode": "off"})
            self.assertEqual(state.get_effective_settings(model)["anti_truncation_mode"], "off")
            state.clear_model_override(model)
            self.assertEqual(state.get_effective_settings(model)["anti_truncation_mode"], "both")
        state.set_model_override(MODEL, {"anti_truncation_mode": "stream"})
        state.clear_model_override(MODEL)
        self.assertEqual(state.get_effective_settings(MODEL)["anti_truncation_mode"], "off")
        req = request(stream=True, anti_truncation_mode="off").model_copy(update={"model": "gemini-4-argon"})
        self.assertIsNone(at.prepare_transport(req, req.model, state.get_effective_settings(req.model))[1])

    def test_model_reset_api_reports_builtin_fallback_for_dashboard(self):
        state = self.isolated_state()
        state.set_model_override("gemini-4-argon", {"anti_truncation_mode": "off"})
        with mock.patch.object(settings_api, "app_state", state):
            result = asyncio.run(settings_api.delete_model_override("gemini-4-argon", True))
        payload = json.loads(result.body)
        self.assertEqual(payload["override"], {"anti_truncation_mode": "both"})

    def test_enabled_suffix_selects_mode_and_legacy_parameter_cannot_override_it(self):
        for suffix, mode in at.SUFFIX_MODES.items():
            req = request().model_copy(update={"model": MODEL + suffix})
            self.assertEqual(at.split_model_alias(req.model), (MODEL, mode))
            for legacy in (None, "off", "stream", "nonstream"):
                wrapped, transport = at.prepare_transport(req.model_copy(update={"anti_truncation_mode": legacy}),
                                                         MODEL, {"anti_truncation_mode": "both"})
                self.assertEqual(transport.mode, mode)
                self.assertEqual(wrapped.model, req.model)

    def test_global_model_and_request_settings_cannot_activate_plain_id(self):
        for selected in ("off", "stream", "nonstream", "both"):
            state = self.isolated_state({"anti_truncation_mode": selected})
            self.assertEqual(state.get_effective_settings(MODEL)["anti_truncation_mode"], "off")
            state.set_model_override(MODEL, {"anti_truncation_mode": selected})
            for legacy in (None, "off", "stream", "nonstream"):
                req = request(anti_truncation_mode=legacy)
                self.assertEqual(at.prepare_transport(req, MODEL, state.get_effective_settings(MODEL)), (req, None))

    def test_disabled_or_unlisted_suffix_is_rejected(self):
        for configured in ("off", "stream", "nonstream"):
            for selected in ("stream", "nonstream"):
                if configured == selected:
                    continue
                with self.subTest(configured=configured, selected=selected):
                    with self.assertRaises(at.TransportSelectionError) as error:
                        at.prepare_transport(transport_request(selected), MODEL, {"anti_truncation_mode": configured})
                    self.assertEqual(error.exception.code, 400)

    def test_disabled_request_is_unchanged(self):
        req = request()
        self.assertEqual(at.prepare_transport(req, MODEL, {}), (req, None))

    def test_tools_history_and_source_are_preserved(self):
        req = transport_request(tools=[TOOL], stream=True)
        before = req.model_dump_json()
        wrapped, transport = at.prepare_transport(req, MODEL, {"anti_truncation_mode": "both"}, native_stream=True)
        self.assertEqual(req.model_dump_json(), before)
        self.assertEqual(wrapped.tools[0], TOOL)
        self.assertEqual(wrapped.tool_choice, "required")
        self.assertFalse(transport.native_stream)
        self.assertEqual(wrapped.messages[-1].role, "user")
        # Use the public message model directly; historical calls must not enable
        # partial streaming on genuine tool traffic.
        from models import OpenAIMessage
        history = req.model_copy(update={"tools": None, "messages": [*req.messages,
            OpenAIMessage(role="tool", content="result", tool_call_id="abc")]})
        _, transport = at.prepare_transport(history, MODEL, {"anti_truncation_mode": "both"}, native_stream=True)
        self.assertFalse(transport.native_stream)

    def test_incompatible_contracts_bypass(self):
        cases = [{"tool_choice": "none"}, {"tool_choice": "required"},
                 {"tool_choice": {"type": "function", "function": {"name": "read_database"}}},
                 {"response_format": {"type": "json_object"}}, {"n": 2},
                 {"tools": [{"type": "web_search"}]},
                 {"tools": [{"type": "google_search"}]},
                 {"functions": [{"name": "legacy"}]}]
        for case in cases:
            with self.subTest(case=case):
                req = transport_request(**case)
                self.assertIsNone(at.prepare_transport(req, MODEL, {"anti_truncation_mode": "both"})[1])
        self.assertIsNone(at.prepare_transport(transport_request(), MODEL, {"anti_truncation_mode": "both"}, search=True)[1])
        for model in ("gemini-3.1-flash-image", "gemini-nano-banana-2.1", "text-embedding-005", "gemini-live"):
            self.assertFalse(at.supports_transport(model))

    def test_argument_stream_capability(self):
        for model in (MODEL, "gemini-3.1-pro-preview", "gemini-4-argon"):
            self.assertTrue(at.supports_argument_stream(model))
        for model in ("gemini-2.5-pro", "gemini-3.1-flash-lite", "gemini-3.1-flash-lite-preview"):
            self.assertFalse(at.supports_argument_stream(model))

    def test_settings_validation_inheritance_and_capabilities(self):
        for mode in ("off", "stream", "nonstream", "both"):
            self.assertIsNone(settings_validation.validate_settings_patch({"anti_truncation_mode": mode}))
        for mode in (True, "yes", 1, None):
            self.assertIsNotNone(settings_validation.validate_settings_patch({"anti_truncation_mode": mode}))
            if mode is not None:
                with self.assertRaises(ValidationError):
                    request(anti_truncation_mode=mode)
        app_state.set_model_override(MODEL, {"anti_truncation_mode": "nonstream"})
        self.addCleanup(app_state.clear_model_override, MODEL)
        for suffix in ("-search", "-antitruncation-stream", "-antitruncation-nonstream"):
            self.assertEqual(app_state.get_effective_settings(MODEL + suffix)["anti_truncation_mode"], "nonstream")
        self.assertTrue(mc.capabilities_summary(MODEL)["supports_anti_truncation"])
        self.assertFalse(mc.capabilities_summary("nano-banana-2.1")["supports_anti_truncation"])


class CatalogHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.state = AppState()
        # Reproduce an old persisted global value; it must not publish aliases.
        self.state._state = {"settings": {"anti_truncation_mode": "stream"}}
        self.raw = ["gemini-3.8-flash", "gemini-4-argon", MODEL, "gemini-3.1-pro-preview",
                    "gemini-3.1-flash-image", "gemini-live", "text-embedding-005",
                    MODEL + "-antitruncation-stream"]
        for patch in (
            mock.patch.object(self.state, "_save"),
            mock.patch.object(settings_api, "app_state", self.state),
            mock.patch.object(models_api, "app_state", self.state),
            mock.patch.object(models_api, "get_express_models", mock.AsyncMock(return_value=self.raw)),
            mock.patch.object(main.app.state, "express_key_manager",
                              SimpleNamespace(get_total_keys=lambda: 1)),
            mock.patch.dict(main.app.dependency_overrides, {console_auth.require_auth: lambda: True}),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                        base_url="http://adapter.test",
                                        headers={"Authorization": "Bearer " + config.API_KEY})
        self.addAsyncCleanup(self.client.aclose)

    async def ids(self):
        result = await self.client.get("/v1/models")
        self.assertEqual(result.status_code, 200)
        return {model["id"] for model in result.json()["data"]}

    async def test_default_catalog_and_model_setting_changes_take_effect_immediately(self):
        defaults = {model + suffix for model in ("gemini-3.8-flash", "gemini-4-argon")
                    for suffix in ("-antitruncation-stream", "-antitruncation-nonstream")}
        self.assertEqual({name for name in await self.ids() if "antitruncation" in name}, defaults)
        for selected, visible in (("both", {"stream", "nonstream"}), ("stream", {"stream"}),
                                  ("nonstream", {"nonstream"}), ("off", set())):
            result = await self.client.post("/api/model-overrides/" + MODEL,
                                            json={"anti_truncation_mode": selected})
            self.assertEqual(result.status_code, 200)
            ids = await self.ids()
            self.assertIn(MODEL, ids)
            self.assertIn(MODEL + "-search", ids)
            self.assertEqual({name for name in ids if "antitruncation" in name},
                             defaults | {MODEL + "-antitruncation-" + mode for mode in visible})
            self.assertEqual(at.prepare_transport(request(anti_truncation_mode="stream"), MODEL,
                                                 self.state.get_effective_settings(MODEL))[1], None)
        result = await self.client.post("/api/settings", json={"anti_truncation_mode": "both"})
        self.assertEqual(result.status_code, 400)
        self.assertEqual({name for name in await self.ids() if "antitruncation" in name}, defaults)

    async def test_hiding_and_resetting_builtins_and_custom_aliases(self):
        for model in ("gemini-3.8-flash", "gemini-4-argon", MODEL):
            initial = "off" if model != MODEL else "both"
            result = await self.client.post("/api/model-overrides/" + model,
                                            json={"anti_truncation_mode": initial})
            self.assertEqual(result.status_code, 200)
            ids = await self.ids()
            self.assertEqual(model + "-antitruncation-stream" in ids, initial == "both")
            result = await self.client.delete("/api/model-overrides/" + model)
            self.assertEqual(result.status_code, 200)
            ids = await self.ids()
            self.assertEqual(model + "-antitruncation-stream" in ids, model != MODEL)


class DecoderTests(unittest.TestCase):
    def test_full_restore_discards_preamble_and_private_signature(self):
        decoder = at.TextDecoder("owned")
        raw = response(types.Part(text="duplicate preamble"),
                       types.Part(text="thinking", thought=True),
                       types.Part(function_call=types.FunctionCall(name="owned", args={"content": "中文\n😀"}),
                                  thought_signature=b"private-signature"), usage=True)
        restored = decoder.restore_sdk(raw)
        decoder.validate_end()
        result = api_helpers.convert_to_openai_format(restored, MODEL)
        message = result["choices"][0]["message"]
        self.assertEqual(message["content"], "中文\n😀")
        self.assertEqual(message["reasoning_content"], "thinking")
        self.assertNotIn("owned", json.dumps(result))
        self.assertNotIn("private-signature", json.dumps(result))
        self.assertEqual(raw.candidates[0].content.parts[0].text, "duplicate preamble")
        self.assertEqual(result["usage"]["total_tokens"], 15)

    def test_real_tools_and_their_signatures_survive(self):
        decoder = at.TextDecoder("owned")
        real = types.Part(function_call=types.FunctionCall(name="read_database", args={"query": "x"}),
                          thought_signature=b"real-signature")
        restored = decoder.restore_sdk(response(real, function("owned", {"content": "answer"})))
        result = api_helpers.convert_to_openai_format(restored, MODEL)
        choice = result["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        self.assertEqual(choice["message"]["content"], "answer")
        self.assertEqual([c["function"]["name"] for c in choice["message"]["tool_calls"]], ["read_database"])
        self.assertTrue(choice["message"]["tool_calls"][0]["extra_content"]["google"]["thought_signature"])

    def test_real_tool_only_and_plain_fallback(self):
        for part in (function("read_database", {"query": "x"}), types.Part(text="ordinary refusal")):
            decoder = at.TextDecoder("owned")
            restored = decoder.restore_sdk(response(part))
            decoder.validate_end()
            self.assertEqual(restored.candidates[0].content.parts, [part])

    def test_partial_unicode_and_completion(self):
        decoder = at.TextDecoder("owned")
        first = decoder.restore_sdk(response(function("owned", partial_args=[types.PartialArg(
            json_path="$.content", string_value="a\ud83d", will_continue=True)], will_continue=True), finish=None))
        self.assertEqual(first.candidates[0].content.parts[0].text, "a")
        second = decoder.restore_sdk(response(function(None, partial_args=[types.PartialArg(
            json_path="$.content", string_value="\ude00b")]), finish="STOP"))
        self.assertEqual(second.candidates[0].content.parts[0].text, "😀b")
        decoder.validate_end()

    def test_delayed_synthetic_signature_is_not_reused_as_text_signature(self):
        decoder = at.TextDecoder("owned")
        decoder.restore_sdk(response(function("owned", {"content": "answer"}), finish=None))
        restored = decoder.restore_sdk(response(types.Part(thought_signature=b"private")))
        self.assertEqual(restored.candidates[0].content.parts, [])
        decoder = at.TextDecoder("owned")
        decoder.cookie_event("function_call", {"name": "owned", "args": {"content": "answer"}})
        self.assertEqual(decoder.cookie_event("thought_signature", {"part_kind": "signature_only", "value": "secret"}), [])
        self.assertEqual(decoder.cookie_event("finish", "STOP"), [("finish", "STOP")])

    def test_cookie_plain_fallback_preserves_signature_order(self):
        decoder = at.TextDecoder("owned")
        signature = {"part_kind": "text", "value": "signature"}
        self.assertEqual(decoder.cookie_event("text", "answer"), [])
        self.assertEqual(decoder.cookie_event("thought_signature", signature), [])
        self.assertEqual(decoder.cookie_event("finish", "STOP"), [
            ("text", "answer"), ("thought_signature", signature), ("finish", "STOP")])

    def test_duplicate_malformed_and_incomplete_fail(self):
        for parts in ([function("owned", {"content": 3})],
                      [function("owned", {})],
                      [function("owned", {"content": "a"}), function("owned", {"content": "b"})],
                      [function("owned", partial_args=[types.PartialArg(json_path="$.other", string_value="x")])],
                      [function("owned", partial_args=[types.PartialArg(
                          json_path="$.content", string_value="x", will_continue=True)], will_continue=True)]):
            with self.subTest(parts=parts), self.assertRaises(at.AntiTruncationError):
                at.TextDecoder("owned").restore_sdk(response(*parts))
        decoder = at.TextDecoder("owned")
        decoder.restore_sdk(response(function("owned", {"content": "a"}), finish=None))
        with self.assertRaises(at.AntiTruncationError):
            decoder.validate_end()
        self.assertFalse(retry_policy.is_retryable_exception(at.AntiTruncationError("bad JSON")))

    def test_length_and_content_filter_are_preserved(self):
        for finish in ("MAX_TOKENS", "SAFETY", "PROHIBITED_CONTENT"):
            decoder = at.TextDecoder("owned")
            restored = decoder.restore_sdk(response(function("owned", partial_args=[types.PartialArg(
                json_path="$.content", string_value="partial", will_continue=True)], will_continue=True), finish=finish))
            self.assertEqual(restored.candidates[0].finish_reason.value, finish)
            decoder.validate_end()
        with self.assertRaises(at.AntiTruncationError):
            at.TextDecoder("owned").restore_sdk(response(types.Part(text="thinking", thought=True)))


class ChannelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = dict(config.DEFAULT_SETTINGS, anti_truncation_mode="both", retry_max=1,
                             retry_backoff_seconds=0, fake_streaming_interval=0,
                             express_location="", proxy_url="", resumable_enabled=False)
        for patch in (
            mock.patch.object(app_state, "get_settings", side_effect=lambda: dict(self.settings)),
            mock.patch.object(app_state, "get_effective_settings", side_effect=lambda model, base=None: (dict(self.settings) if base is None else dict(base))),
            mock.patch.object(app_state, "get_setting", side_effect=lambda key, default=None: self.settings.get(key, default)),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.key_manager = SimpleNamespace(get_total_keys=lambda: 1,
            get_express_record=lambda **kwargs: {"key": "test-key", "project_id": ""})
        self.http_request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(express_key_manager=self.key_manager)),
            is_disconnected=mock.AsyncMock(return_value=False))

    async def express(self, req, generate=None, stream=None):
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
            generate_content=generate, generate_content_stream=stream)))
        patch = mock.patch.object(express_sdk, "get_pooled_express_client", return_value=client)
        patch.start()
        self.addCleanup(patch.stop)
        return await express_sdk.ExpressSDKUpstream().chat_completions(req, self.http_request)

    async def body(self, result):
        return "".join([part async for part in result.body_iterator])

    async def test_model_list_variants_and_no_image_or_native_pollution(self):
        raw = [MODEL, "nano-banana-2.1", "gemini-3.1-flash-image", "gemini-live", "text-embedding-005"]
        with mock.patch.object(models_api, "get_express_models", mock.AsyncMock(return_value=raw)):
            result = await models_api.list_models(self.http_request, "test")
        ids = {m["id"] for m in result["data"]}
        self.assertIn(MODEL + "-antitruncation-stream", ids)
        self.assertIn(MODEL + "-antitruncation-nonstream", ids)
        self.assertEqual(len(ids), len(result["data"]))
        self.assertFalse(any("antitruncation" in m and not m.startswith(MODEL) for m in ids))
        from routes import gemini_native
        with mock.patch.object(gemini_native, "get_express_models", mock.AsyncMock(return_value=raw)):
            native = await gemini_native.native_list_models(self.http_request, "test")
        self.assertNotIn("antitruncation", json.dumps(native))

    async def test_express_json_and_client_request_not_mutated(self):
        calls = []
        async def generate(**kwargs):
            calls.append(kwargs)
            name = kwargs["config"]["tools"][0]["function_declarations"][-1]["name"]
            return response(function(name, {"content": "answer"}), usage=True)
        req = request().model_copy(update={"model": MODEL + "-antitruncation-stream"})
        before = req.model_dump_json()
        result = await self.express(req, generate=generate)
        payload = json.loads(result.body)
        self.assertEqual(payload["choices"][0]["message"]["content"], "answer")
        self.assertEqual(payload["choices"][0]["finish_reason"], "stop")
        self.assertEqual(payload["model"], req.model)
        self.assertEqual(calls[0]["model"], MODEL)
        self.assertNotIn("stream_function_call_arguments", calls[0]["config"]["tool_config"]["function_calling_config"])
        self.assertEqual(req.model_dump_json(), before)

    async def test_plain_express_id_never_injects_transport_into_actual_request(self):
        calls = []
        async def generate(**kwargs):
            calls.append(kwargs)
            self.assertFalse(kwargs["config"].get("tools"))
            self.assertNotIn("stream_function_call_arguments", str(kwargs["config"]))
            return response(types.Part(text="ordinary answer"))
        for model in (MODEL, "gemini-3.8-flash", "gemini-4-argon"):
            for legacy in (None, "stream", "nonstream"):
                req = request(anti_truncation_mode=legacy).model_copy(update={"model": model})
                result = await self.express(req, generate=generate)
                self.assertEqual(result.status_code, 200)
                self.assertEqual(json.loads(result.body)["choices"][0]["message"]["content"], "ordinary answer")
                self.assertEqual(calls[-1]["model"], model)

    async def test_plain_cookie_id_never_injects_transport_into_actual_request(self):
        seen = []
        @asynccontextmanager
        async def open_client(*args, **kwargs):
            async def post(url, headers, json):
                seen.append(json)
                self.assertFalse(json["variables"].get("tools"))
                return httpx.Response(200, json={"results": [{"data": {"candidates": [{
                    "content": {"parts": [{"text": "ordinary answer"}]}, "finishReason": "STOP"}]}}]})
            yield SimpleNamespace(post=post)
        with mock.patch.object(cookie_proxy, "_get_cookie_string", return_value="SAPISID=test"), \
             mock.patch.object(cookie_proxy, "_get_project_id", return_value="test-project"), \
             mock.patch.object(cookie_proxy, "open_upstream_client", open_client):
            for model in (MODEL, "gemini-3.8-flash", "gemini-4-argon"):
                for legacy in (None, "stream", "nonstream"):
                    req = request(anti_truncation_mode=legacy).model_copy(update={"model": model})
                    result = await cookie_proxy.CookieProxyUpstream().chat_completions(req, self.http_request)
                    self.assertEqual(result.status_code, 200)
                    self.assertEqual(json.loads(result.body)["choices"][0]["message"]["content"], "ordinary answer")
                    self.assertEqual(seen[-1]["variables"]["model"].rsplit("/", 1)[-1], model)

    async def test_express_native_delivers_before_upstream_finishes(self):
        allow_finish = asyncio.Event()
        async def stream(**kwargs):
            self.assertTrue(kwargs["config"]["tool_config"]["function_calling_config"]["stream_function_call_arguments"])
            name = kwargs["config"]["tools"][0]["function_declarations"][-1]["name"]
            async def iterate():
                yield response(function(name, partial_args=[types.PartialArg(
                    json_path="$.content", string_value="first", will_continue=True)], will_continue=True), finish=None)
                await asyncio.wait_for(allow_finish.wait(), timeout=2)
                yield response(function(None, partial_args=[types.PartialArg(json_path="$.content", string_value="last")]), usage=True)
            return iterate()
        req = transport_request(stream=True, stream_options={"include_usage": True})
        result = await self.express(req, stream=stream)
        body = ""
        async for frame in result.body_iterator:
            body += frame
            if "first" in content(body):
                allow_finish.set()
        self.assertEqual(content(body), "firstlast")
        self.assertNotIn("ordinary_parts", body)
        self.assertNotIn("v2o_emit_", body)
        self.assertTrue(body.endswith("data: [DONE]\n\n"))
        self.assertEqual(chunks(body)[-1]["usage"]["total_tokens"], 15)
        self.assertEqual([c["finish_reason"] for p in chunks(body) for c in p["choices"] if c["finish_reason"]], ["stop"])

    async def test_actual_sdk_wire_and_native_response_parsing(self):
        captured = []
        def handler(req):
            body = json.loads(req.content)
            captured.append(body)
            name = body["tools"][0]["functionDeclarations"][-1]["name"]
            fcc = body["toolConfig"]["functionCallingConfig"]
            # google-genai may serialize nested protobuf fields using their
            # original snake_case names rather than JSON camelCase aliases.
            self.assertTrue(fcc.get("streamFunctionCallArguments", fcc.get("stream_function_call_arguments")))
            self.assertNotIn("antitruncation", str(req.url))
            part = {"functionCall": {"name": name, "partialArgs": [{
                "jsonPath": "$.content", "stringValue": "wire answer"}]}}
            data = {"candidates": [{"content": {"role": "model", "parts": [part]}, "finishReason": "STOP"}]}
            return httpx.Response(200, headers={"content-type": "text/event-stream"},
                                  text="data: " + json.dumps(data) + "\n\n")
        transport = httpx.MockTransport(handler)
        client = genai.Client(vertexai=True, api_key="test-key", http_options=types.HttpOptions(
            async_client_args={"transport": transport, "trust_env": False},
            client_args={"transport": transport, "trust_env": False}))
        try:
            with mock.patch.object(express_sdk, "get_pooled_express_client", return_value=client):
                req = request(stream=True).model_copy(update={"model": MODEL + "-antitruncation-stream"})
                result = await express_sdk.ExpressSDKUpstream().chat_completions(req, self.http_request)
                body = await self.body(result)
            self.assertEqual(content(body), "wire answer")
            self.assertEqual(len(captured), 1)
            self.assertNotIn("v2o_emit_", body)
        finally:
            await client.aio.aclose()
            client.close()

    async def test_express_buffered_sse_and_prefill(self):
        self.settings["inject_prefill"] = "prefix of answer: "
        async def generate(**kwargs):
            name = kwargs["config"]["tools"][0]["function_declarations"][-1]["name"]
            self.assertNotIn("stream_function_call_arguments", kwargs["config"]["tool_config"]["function_calling_config"])
            return response(function(name, {"content": "prefix of answer: rest"}), usage=True)
        result = await self.express(transport_request(mode="nonstream", stream=True,
            stream_options={"include_usage": True}), generate=generate)
        body = await self.body(result)
        self.assertEqual(content(body), "prefix of answer: rest")
        self.assertNotIn("v2o_emit_", body)
        self.assertEqual(chunks(body)[-1]["usage"]["total_tokens"], 15)

    async def test_express_429_and_preview_fallback(self):
        for error_code, message in ((429, "busy"), (400, "streamFunctionCallArguments unsupported")):
            calls = []
            async def stream(**kwargs):
                calls.append(dict(kwargs))
                if len(calls) == 1:
                    err = RuntimeError(message)
                    err.code = error_code
                    raise err
                name = kwargs["config"]["tools"][0]["function_declarations"][-1]["name"]
                async def iterate():
                    yield response(function(name, {"content": "retried"}))
                return iterate()
            result = await self.express(transport_request(stream=True), stream=stream)
            body = await self.body(result)
            self.assertEqual(len(calls), 2)
            self.assertEqual(content(body), "retried")

    async def test_express_interruption_and_invalid_args_are_errors(self):
        for invalid in (False, True):
            async def stream(**kwargs):
                name = kwargs["config"]["tools"][0]["function_declarations"][-1]["name"]
                async def iterate():
                    yield response(function(name, {"content": 123 if invalid else "partial"}), finish=None)
                return iterate()
            result = await self.express(transport_request(stream=True), stream=stream)
            body = await self.body(result)
            self.assertIn('"error"', body)
            self.assertNotIn('"finish_reason": "stop"', body)
            self.assertEqual(content(body), "" if invalid else "partial")

    async def test_error_survives_resumable_storage_and_success_is_not_counted(self):
        from logger import ProxyStats
        stats = ProxyStats()
        async def stream(**kwargs):
            name = kwargs["config"]["tools"][0]["function_declarations"][-1]["name"]
            async def iterate():
                yield response(function(name, {"content": "partial"}), finish=None)
            return iterate()
        with mock.patch.object(api_helpers, "stats", stats):
            result = await self.express(transport_request(stream=True), stream=stream)
            buf = resumable.ResumableBuffer("anti-error", MODEL)
            await resumable.pump_stream(buf, result.body_iterator)
        self.assertEqual(buf.status, "error")
        self.assertTrue(buf.error)
        replay = "".join([frame async for frame in resumable.sse_from_buffer(buf, 0)])
        self.assertEqual(content(replay), "partial")
        self.assertIn('"error"', replay)
        self.assertEqual(stats.error_requests, 1)
        self.assertEqual(stats.success_requests, 0)

    async def cookie(self, req, *, interrupted=False, real_tool=False, retry=False, terminal_status=None):
        seen = []
        @asynccontextmanager
        async def open_client(*args, **kwargs):
            async def post(url, headers, json):
                seen.append(json)
                name = json["variables"]["tools"][0]["functionDeclarations"][-1]["name"]
                parts = [{"functionCall": {"name": name, "args": {"content": "cookie answer"}}}]
                if real_tool:
                    parts.insert(0, {"functionCall": {"name": "read_database", "args": {"query": "x"}},
                                     "thoughtSignature": "cmVhbC1zaWduYXR1cmU="})
                candidate = {"content": {"parts": parts}}
                if not interrupted:
                    candidate["finishReason"] = "STOP"
                payload = {"results": [{"data": {"candidates": [candidate]}}]}
                return httpx.Response(200, json=payload)
            yield SimpleNamespace(post=post)
        async def events(client, headers, body, **kwargs):
            seen.append(body)
            self.assertIsNone(kwargs.get("fallback_body"))
            if terminal_status:
                yield terminal_status, "upstream failure"
                return
            if retry and len(seen) == 1:
                yield "retryable_error", "429 busy"
                return
            name = body["variables"]["tools"][0]["functionDeclarations"][-1]["name"]
            yield "function_call", {"name": name, "args": {"content": "cookie answer"}}
            if real_tool:
                yield "function_call", {"name": "read_database", "args": {"query": "x"},
                                          "thought_signature": "cmVhbC1zaWduYXR1cmU="}
            if not interrupted:
                yield "finish", "STOP"
        with mock.patch.object(cookie_proxy, "_get_cookie_string", return_value="SAPISID=test"), \
             mock.patch.object(cookie_proxy, "_get_project_id", return_value="test-project"), \
             mock.patch.object(cookie_proxy, "open_upstream_client", open_client), \
             mock.patch.object(cookie_proxy, "stream_events", events):
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(req, self.http_request)
            body = await self.body(result) if hasattr(result, "body_iterator") else result.body.decode()
        self.assertNotIn("antitruncation", seen[-1]["variables"]["model"])
        return result, body, seen

    async def test_cookie_json_and_both_stream_modes(self):
        for stream in (False, True):
            for mode in ("stream", "nonstream"):
                req = request(stream=stream).model_copy(update={"model": MODEL + "-antitruncation-" + mode})
                result, body, _ = await self.cookie(req)
                self.assertEqual(result.status_code, 200)
                text = content(body) if stream else json.loads(body)["choices"][0]["message"]["content"]
                self.assertEqual(text, "cookie answer")
                self.assertNotIn("v2o_emit_", body)

    async def test_cookie_preserves_real_calls_and_retries_429(self):
        for stream in (False, True):
            _, body, _ = await self.cookie(transport_request(stream=stream, tools=[TOOL]), real_tool=True)
            self.assertIn("read_database", body)
            self.assertIn("cmVhbC1zaWduYXR1cmU=", body)
            self.assertNotIn("v2o_emit_", body)
        _, body, seen = await self.cookie(transport_request(stream=True), retry=True)
        self.assertEqual(len(seen), 2)
        self.assertEqual(content(body), "cookie answer")

    async def test_cookie_interrupted_response_is_error(self):
        for stream in (False, True):
            result, body, _ = await self.cookie(transport_request(stream=stream), interrupted=True)
            self.assertIn('"error"', body)
            self.assertNotIn('"finish_reason": "stop"', body)
            self.assertEqual(result.status_code, 200 if stream else 502)

    async def test_cookie_fatal_schema_and_exhausted_retries_are_errors(self):
        self.settings["retry_max"] = 0
        for status in ("fatal_error", "retryable_error", "cookie_error"):
            _, body, seen = await self.cookie(transport_request(stream=True, tools=[TOOL]),
                                               terminal_status=status)
            self.assertEqual(len(seen), 1)
            self.assertIn('"error"', body)
            self.assertNotIn('"finish_reason": "stop"', body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
