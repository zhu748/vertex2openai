"""Cookie 搜索后缀、工具冲突与实际出站请求的离线回归。"""

from contextlib import asynccontextmanager
from itertools import product
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-cookie-search-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

import config
from models import OpenAIRequest
from runtime_state import app_state
from upstreams import cookie_proxy, cookie_request


MODEL = "gemini-3.5-flash-lite"
SEARCH = [{"googleSearch": {}}]
CUSTOM = {"type": "function", "function": {
    "name": "get_time_info", "parameters": {"type": "object", "properties": {}}
}}
RECORD = {"id": "test", "cookie": "SAPISID=test", "project_id": "test-project"}


class CookieSearchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = dict(config.DEFAULT_SETTINGS, anti_truncation_mode="both",
                             retry_max=0, fake_streaming=False,
                             fake_streaming_interval=0, proxy_url="")
        for patcher in (
            mock.patch.object(app_state, "get_effective_settings",
                              side_effect=lambda *args, **kwargs: dict(self.settings)),
            mock.patch.object(app_state, "get_setting",
                              side_effect=lambda key, default=None: self.settings.get(key, default)),
            mock.patch.object(cookie_proxy.credential_manager, "record_cookie_success"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        log_patch = mock.patch("builtins.print")
        self.logs = log_patch.start()
        self.addCleanup(log_patch.stop)
        self.http_request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))

    def request(self, model=MODEL + "-search", **kwargs):
        return OpenAIRequest(model=model, messages=[{
            "role": "user", "content": "搜索今天的新闻并给出来源"
        }], **kwargs)

    def prepare(self, request):
        prepared = cookie_proxy._prepare_cookie_generation(request, self.http_request, RECORD)
        self.assertIsInstance(prepared, cookie_proxy.CookieGeneration)
        return prepared

    def variables(self, request):
        prepared = self.prepare(request)
        return cookie_request.build_request_body(
            prepared.project_id, prepared.base_model_name, prepared.request_obj,
            force_search=prepared.force_builtin_search,
        )["variables"]

    def log_text(self):
        return "\n".join(str(call.args[0]) for call in self.logs.call_args_list if call.args)

    async def test_search_reaches_json_stream_and_buffered_upstream_requests(self):
        captured = []

        @asynccontextmanager
        async def open_client(*args, **kwargs):
            async def post(url, headers, json):
                captured.append(json["variables"])
                return httpx.Response(200, json={"results": [{"data": {"candidates": [{
                    "content": {"parts": [{"text": "answer"}]}, "finishReason": "STOP"
                }]}}]})
            yield SimpleNamespace(post=post)

        async def events(client, headers, body, **kwargs):
            captured.append(body["variables"])
            self.assertIsNone(kwargs.get("fallback_body"))
            yield "text", "answer"
            yield "finish", "STOP"

        with mock.patch.object(cookie_proxy, "open_upstream_client", open_client), \
             mock.patch.object(cookie_proxy, "stream_events", events):
            for suffix, tools in product(
                ("-search", "-search ", "-search-antitruncation-stream", "-search-antitruncation-nonstream"),
                (None, [CUSTOM]),
            ):
                for stream, buffered in ((False, False), (True, False), (True, True)):
                    with self.subTest(suffix=suffix, tools=tools, stream=stream, buffered=buffered):
                        self.settings["fake_streaming"] = buffered
                        request = self.request(MODEL + suffix, stream=stream, tools=tools)
                        before = request.model_dump_json()
                        result = await cookie_proxy.CookieProxyUpstream().chat_completions(
                            request, self.http_request, credential_record=RECORD)
                        payload = ("".join([frame async for frame in result.body_iterator])
                                   if stream else result.body.decode())
                        self.assertEqual(result.status_code, 200)
                        self.assertIn("answer", payload)
                        self.assertEqual(captured[-1].get("tools"), SEARCH)
                        self.assertEqual(captured[-1]["model"].rsplit("/", 1)[-1], MODEL)
                        self.assertNotIn("toolConfig", captured[-1])
                        self.assertEqual(request.model_dump_json(), before)

    async def test_search_flag_survives_request_model_normalization_and_degradation(self):
        prepared = self.prepare(self.request())
        normalized = prepared.request_obj.model_copy(update={"model": prepared.base_model_name})
        cache = cookie_proxy._RequestBodyCache(
            prepared.project_id, prepared.base_model_name, normalized,
            prefill_active=False, force_search=prepared.force_builtin_search)
        for body in (await cache.get(True), await cache.degraded()):
            self.assertEqual(body["variables"].get("tools"), SEARCH)

    async def test_tool_call_ids_and_truncation_survive_all_delivery_modes(self):
        call = {"id": "upstream-tool-1", "name": "get_time_info", "args": {}}

        @asynccontextmanager
        async def open_client(*args, **kwargs):
            async def post(url, headers, json):
                return httpx.Response(200, json={"results": [{"data": {"candidates": [{
                    "content": {"parts": [{"functionCall": call}]}, "finishReason": reason
                }]}}]})
            yield SimpleNamespace(post=post)

        async def events(*args, **kwargs):
            yield "function_call", call
            yield "finish", reason

        with mock.patch.object(cookie_proxy, "open_upstream_client", open_client), \
             mock.patch.object(cookie_proxy, "stream_events", events):
            for reason, expected in (("STOP", "tool_calls"), ("MAX_TOKENS", "length"), ("SAFETY", "content_filter")):
                for stream, buffered in ((False, False), (True, False), (True, True)):
                    with self.subTest(reason=reason, stream=stream, buffered=buffered):
                        self.settings["fake_streaming"] = buffered
                        request = self.request(MODEL, stream=stream, tools=[CUSTOM])
                        result = await cookie_proxy.CookieProxyUpstream().chat_completions(
                            request, self.http_request, credential_record=RECORD)
                        payload = ("".join([frame async for frame in result.body_iterator])
                                   if stream else result.body.decode())
                        self.assertEqual(result.status_code, 200)
                        chunks = ([json.loads(line[6:]) for line in payload.splitlines()
                                   if line.startswith("data: ") and line[6:].strip() != "[DONE]"]
                                  if stream else [json.loads(payload)])
                        choices = [choice for chunk in chunks for choice in chunk.get("choices", [])]
                        self.assertEqual([choice["finish_reason"] for choice in choices
                                          if choice.get("finish_reason")], [expected])
                        calls = [call for choice in choices
                                 for call in choice.get("message", choice.get("delta", {})).get("tool_calls", [])]
                        self.assertEqual([call["id"] for call in calls if call.get("id")], ["upstream-tool-1"])

    async def test_plain_model_does_not_enable_search(self):
        self.assertNotIn("tools", self.variables(self.request(MODEL)))

    async def test_native_search_tool_types_survive_cookie_protocol_degradation(self):
        for kind in ("web_search", "web_search_preview", "google_search"):
            prepared = self.prepare(self.request(MODEL, tools=[{"type": kind}]))
            cache = cookie_proxy._RequestBodyCache(
                prepared.project_id, prepared.base_model_name, prepared.request_obj,
                prefill_active=False, force_search=prepared.force_builtin_search)
            with self.subTest(kind=kind):
                for body in (await cache.get(True), await cache.degraded()):
                    self.assertEqual(body["variables"]["tools"], SEARCH)
                    self.assertNotIn("toolConfig", body["variables"])

    async def test_tool_choice_none_disables_search_with_diagnostic(self):
        with mock.patch.object(cookie_request, "_build_function_declarations",
                               wraps=cookie_request._build_function_declarations) as build:
            self.assertNotIn("tools", self.variables(self.request(tools=[CUSTOM], tool_choice="none")))
        build.assert_not_called()
        self.assertIn("tool_choice=none", self.log_text())

    async def test_preferred_search_skips_optional_function_schema_conversion(self):
        with mock.patch.object(cookie_request, "_build_function_declarations",
                               wraps=cookie_request._build_function_declarations) as build:
            self.assertEqual(self.variables(self.request(tools=[CUSTOM]))["tools"], SEARCH)
        build.assert_not_called()

    async def test_search_suffix_wins_over_automatic_time_tool(self):
        for choice in (None, "auto", "AUTO"):
            with self.subTest(choice=choice):
                variables = self.variables(self.request(tools=[CUSTOM], tool_choice=choice))
                self.assertEqual(variables["tools"], SEARCH)
                self.assertNotIn("toolConfig", variables)
        self.assertIn("优先搜索", self.log_text())

    async def test_explicit_custom_tool_selections_keep_priority(self):
        for choice in ("required", CUSTOM):
            with self.subTest(choice=choice):
                variables = self.variables(self.request(tools=[CUSTOM], tool_choice=choice))
                self.assertIn("functionDeclarations", variables["tools"][0])
                self.assertNotIn("googleSearch", variables["tools"][0])
        self.assertIn("忽略搜索", self.log_text())

    async def test_plain_model_with_mixed_tool_declarations_keeps_function_priority(self):
        search_tool = {"type": "function", "function": {"name": "google_search"}}
        variables = self.variables(self.request(MODEL, tools=[CUSTOM, search_tool]))
        self.assertIn("functionDeclarations", variables["tools"][0])

    async def test_forced_search_and_declared_search_still_work(self):
        search_tool = {"type": "web_search"}
        for request in (
            self.request(tools=[CUSTOM, search_tool], tool_choice={"type": "function", "function": {"name": "google_search"}}),
            self.request(MODEL, tools=[search_tool]),
        ):
            with self.subTest(request=request):
                variables = self.variables(request)
                self.assertEqual(variables["tools"], SEARCH)
                self.assertNotIn("toolConfig", variables)
        self.assertIn("已启用 Google Search", self.log_text())

if __name__ == "__main__":
    unittest.main()
