"""模型请求与重试边界回归：模拟 SDK、HTTP 和 Key 池，无外网调用。"""
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

_STATE_DIR = tempfile.TemporaryDirectory(prefix="v2o-model-retry-")
os.environ["STATE_DIR"] = _STATE_DIR.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from curl_cffi.requests.exceptions import Timeout as CurlTimeout
from google.genai import types
from google import genai

import api_helpers
import config
from express_key_manager import ExpressKeyManager
from logger import ProxyStats
from retry_policy import retry_delay
from upstreams import express_sdk
from upstreams import cookie_proxy
from http_options import get_http_options


def api_error(code, message="upstream error", headers=None):
    error = Exception(message)
    error.code = code
    error.response = httpx.Response(code, headers=headers or {})
    return error


def text_response(text="answer"):
    return types.GenerateContentResponse(candidates=[types.Candidate(
        content=types.Content(role="model", parts=[types.Part(text=text)]), finish_reason="STOP")])


class StreamRetryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stats = ProxyStats()
        self.settings = {"retry_max": 1, "retry_backoff_seconds": 0,
                         "fake_streaming": False, "fake_streaming_interval": 0}
        for patch in (
            mock.patch.object(api_helpers, "stats", self.stats),
            mock.patch.object(api_helpers.app_state, "get_setting",
                              side_effect=lambda key, default=None: self.settings.get(key, default)),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    async def stream(self, factory, prefill="", **kwargs):
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content_stream=factory)))
        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=True)
        result = await api_helpers.execute_gemini_call(
            client, obj.model, lambda messages: [], {}, obj, prefill_text=prefill, **kwargs)
        return "".join([chunk async for chunk in result.body_iterator])

    async def test_prefill_does_not_disable_retry_before_upstream_content(self):
        calls = []

        async def factory(**kwargs):
            calls.append(kwargs)

            async def iterate():
                if len(calls) == 1:
                    raise api_error(429)
                yield text_response()
            return iterate()

        body = await self.stream(factory, prefill="prefix:")
        self.assertEqual(len(calls), 2)
        self.assertEqual(body.count("prefix:"), 1)
        self.assertIn("answer", body)
        self.assertEqual(self.stats.retry_counts, 1)

    async def test_usage_only_chunk_does_not_disable_retry(self):
        calls = []

        async def factory(**kwargs):
            calls.append(kwargs)

            async def iterate():
                if len(calls) == 1:
                    yield types.GenerateContentResponse(usage_metadata=types.GenerateContentResponseUsageMetadata(prompt_token_count=2))
                    raise api_error(503)
                yield text_response()
            return iterate()

        body = await self.stream(factory)
        self.assertEqual(len(calls), 2)
        self.assertIn("answer", body)

    async def test_error_after_content_is_reported_without_replaying(self):
        calls = []

        async def factory(**kwargs):
            calls.append(kwargs)

            async def iterate():
                yield text_response("partial")
                raise api_error(429)
            return iterate()

        body = await self.stream(factory)
        errors = [json.loads(line[6:])["error"] for line in body.splitlines()
                  if line.startswith("data: {") and "error" in json.loads(line[6:])]
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["code"], 429)
        self.assertEqual(self.stats.error_requests, 1)

    async def test_permanent_status_overrides_quota_text(self):
        for code in (400, 401, 403, 404):
            self.assertFalse(api_helpers.is_retryable_exception(api_error(code, "quota model-429")))

    async def test_no_enabled_key_stops_retry_without_counting_unsent_attempts(self):
        self.settings["retry_max"] = 5
        factory = mock.AsyncMock(side_effect=api_error(429))
        body = await self.stream(factory, retry_target_factory=lambda: None)
        self.assertEqual(factory.await_count, 1)
        self.assertIn('"code": 429', body)
        self.assertEqual(self.stats.retry_counts, 0)

    async def test_failed_stream_is_closed_before_backoff(self):
        closed = []
        calls = []

        class FailedStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise api_error(429, headers={"retry-after": "0.25"})

            async def aclose(self):
                closed.append(True)

        async def factory(**kwargs):
            calls.append(kwargs)
            return FailedStream()

        async def wait(request, delay):
            self.assertTrue(closed)
            self.assertEqual(delay, 0.25)
            return False

        with mock.patch.object(api_helpers, "wait_for_retry", side_effect=wait):
            await self.stream(factory)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.stats.retry_counts, 0)

    async def test_nonstream_backoff_stops_on_disconnect(self):
        generate = mock.AsyncMock(side_effect=api_error(429))
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate)))
        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[])
        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))
        with mock.patch.object(api_helpers, "wait_for_retry", return_value=False):
            result = await api_helpers.execute_gemini_call(
                client, obj.model, lambda messages: [], {}, obj, fastapi_request=request)
        self.assertEqual(result.status_code, 499)
        self.assertEqual(generate.await_count, 1)
        self.assertEqual(self.stats.retry_counts, 0)

    async def test_nonstream_uses_retry_after_header(self):
        generate = mock.AsyncMock(side_effect=[api_error(429, headers={"retry-after": "2.5"}), text_response()])
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate)))
        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[])
        with mock.patch.object(api_helpers, "wait_for_retry", return_value=True) as wait:
            result = await api_helpers.execute_gemini_call(client, obj.model, lambda messages: [], {}, obj)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(wait.await_args.args[1], 2.5)
        self.assertEqual(self.stats.retry_counts, 1)

    async def test_empty_stream_is_not_counted_as_success(self):
        async def factory(**kwargs):
            async def iterate():
                if False:
                    yield
            return iterate()
        body = await self.stream(factory)
        self.assertIn('"error"', body)
        self.assertEqual(self.stats.success_requests, 0)
        self.assertEqual(self.stats.error_requests, 1)

    async def test_fake_stream_empty_response_is_an_error(self):
        self.settings["fake_streaming"] = True
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
            generate_content=mock.AsyncMock(return_value=types.GenerateContentResponse()))))
        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=True)
        response = await api_helpers.execute_gemini_call(client, obj.model, lambda messages: [], {}, obj)
        body = "".join([chunk async for chunk in response.body_iterator])
        self.assertIn('"error"', body)
        self.assertEqual(self.stats.success_requests, 0)
        self.assertEqual(self.stats.error_requests, 1)

    async def test_real_sdk_respects_wire_attempt_budget(self):
        for retries in (0, 2):
            self.settings["retry_max"] = retries
            calls = []

            def handle(request):
                calls.append(request)
                return httpx.Response(429, json={"error": {"code": 429, "message": "quota exhausted"}})

            options = get_http_options() or types.HttpOptions()
            options.async_client_args = {"transport": httpx.MockTransport(handle)}
            client = genai.Client(vertexai=True, api_key="test-key", http_options=options)
            obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[])
            try:
                with self.assertRaises(Exception) as raised:
                    await api_helpers.execute_gemini_call(client, obj.model, lambda messages: [
                        types.Content(role="user", parts=[types.Part(text="hi")])], {}, obj)
                self.assertEqual(getattr(raised.exception, "code", None), 429, str(raised.exception))
                self.assertEqual(len(calls), retries + 1)
            finally:
                await client.aio.aclose()
                client.close()


class CookieRetryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stats = ProxyStats()
        self.settings = dict(config.DEFAULT_SETTINGS, retry_max=2, retry_backoff_seconds=0, prefill_mode="off")
        self.client = SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(200, json={})))
        self.request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))

        @asynccontextmanager
        async def open_client(kwargs):
            yield self.client

        async def objects(response):
            yield {}

        for patch in (
            mock.patch.object(cookie_proxy, "stats", self.stats),
            mock.patch.object(cookie_proxy, "_get_cookie_string", return_value="test-cookie"),
            mock.patch.object(cookie_proxy, "_get_project_id", return_value="test-project"),
            mock.patch.object(cookie_proxy, "build_headers", return_value={"authorization": "test-auth"}),
            mock.patch.object(cookie_proxy, "open_upstream_client", side_effect=open_client),
            mock.patch.object(cookie_proxy, "build_batch_graphql_body_async", return_value={"variables": {}}),
            mock.patch.object(cookie_proxy, "_iter_json_objects", side_effect=objects),
            mock.patch.object(cookie_proxy, "_extract_from_results", return_value=[("error", {"code": 429, "message": "quota exhausted"})]),
            mock.patch.object(cookie_proxy.app_state, "get_setting",
                              side_effect=lambda key, default=None: self.settings.get(key, default)),
            mock.patch.object(cookie_proxy.app_state, "get_effective_settings",
                              side_effect=lambda model: self.settings.copy()),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    async def test_exhausted_api_error_is_not_successful_text(self):
        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[])
        response = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(self.client.post.await_count, 3)
        self.assertEqual(self.stats.success_requests, 0)
        self.assertEqual(self.stats.retry_counts, 2)

    async def test_permanent_api_error_with_quota_word_is_not_retried(self):
        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[])
        with mock.patch.object(cookie_proxy, "_extract_from_results", return_value=[("error", {"code": 403, "message": "quota denied"})]):
            response = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.client.post.await_count, 1)
        self.assertEqual(self.stats.success_requests, 0)

    async def test_image_collection_reports_exhausted_api_error(self):
        obj = api_helpers.OpenAIRequest(model="gemini-3.1-flash-image", messages=[])
        result = await cookie_proxy._collect_full_response("p", obj.model, obj, {}, {}, 0, 0, self.request)
        self.assertEqual(result["kind"], "error")
        self.assertEqual(result["code"], 429)

    async def test_http_502_504_retry_and_honor_retry_after(self):
        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[])
        self.client.post.side_effect = [httpx.Response(502, headers={"retry-after": "2.5"}), httpx.Response(504), httpx.Response(200, json={})]
        with mock.patch.object(cookie_proxy, "wait_for_retry", return_value=True) as wait, \
             mock.patch.object(cookie_proxy, "_extract_from_results", return_value=[("text", "answer")]):
            response = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.post.await_count, 3)
        self.assertEqual(wait.await_args_list[0].args[1], 2.5)
        self.assertEqual(self.stats.retry_counts, 2)

    async def test_disconnect_during_backoff_does_not_count_unsent_retry(self):
        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[])
        self.client.post.return_value = httpx.Response(429)
        with mock.patch.object(cookie_proxy, "wait_for_retry", return_value=False):
            response = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
        self.assertEqual(response.status_code, 499)
        self.assertEqual(self.client.post.await_count, 1)
        self.assertEqual(self.stats.retry_counts, 0)

    async def test_stream_http_status_classification(self):
        for code, expected in ((502, "retryable_error"), (504, "retryable_error"), (400, "fatal_error")):
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                    lambda request: httpx.Response(code, headers={"retry-after": "2.5"},
                                                   json={"error": "quota model-429"}))) as client:
                events = [event async for event in cookie_proxy._execute_stream_request_generator_once(client, {}, {})]
            self.assertEqual(events[0][0], expected)
            self.assertEqual(events[0][1].retry_after, "2.5")

    async def test_stream_api_error_after_content_is_not_counted_as_success(self):
        async def events(*args, **kwargs):
            yield "text", "partial"
            yield "api_error_text", "connection interrupted"

        obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=True)
        with mock.patch.object(cookie_proxy, "_execute_stream_request_generator", side_effect=events):
            response = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request)
            body = "".join([chunk async for chunk in response.body_iterator])
        self.assertIn('"error"', body)
        self.assertIn("partial", body)
        self.assertEqual(self.stats.success_requests, 0)
        self.assertEqual(self.stats.error_requests, 1)


class KeyRotationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.records = [{"key": "key-1", "project_id": "project-1"},
                        {"key": "key-2", "project_id": "project-2"}]
        self.settings = dict(config.DEFAULT_SETTINGS, retry_max=1, retry_backoff_seconds=0,
                             fake_streaming_interval=0, express_location="global", prefill_mode="off")
        self.stats = ProxyStats()
        for patch in (
            mock.patch.object(api_helpers, "stats", self.stats),
            mock.patch.object(api_helpers.app_state, "get_express_keys", side_effect=lambda: list(self.records)),
            mock.patch.object(api_helpers.app_state, "get_setting",
                              side_effect=lambda key, default=None: self.settings.get(key, default)),
            mock.patch.object(api_helpers.app_state, "get_effective_settings",
                              side_effect=lambda model: self.settings.copy()),
            mock.patch.object(express_sdk, "_log_resolved_endpoint"),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    async def test_sdk_rotates_key_and_pin_fallback_stays_with_selected_project(self):
        for mode in ("json", "stream", "fake"):
            with self.subTest(mode=mode):
                calls = []
                self.settings.update(roundrobin=True, fake_streaming=mode == "fake")
                manager = ExpressKeyManager()

                def client_for(key):
                    async def generate(**kwargs):
                        model = kwargs["model"]
                        calls.append((key, model))
                        if model.startswith("projects/"):
                            raise api_error(404, "Publisher model was not found")
                        if key == "key-1":
                            raise api_error(429)
                        return text_response()

                    async def stream(**kwargs):
                        result = await generate(**kwargs)

                        async def iterate():
                            yield result
                        return iterate()
                    return SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
                        generate_content=generate, generate_content_stream=stream)))

                obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=mode != "json")
                request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)),
                                          is_disconnected=mock.AsyncMock(return_value=False))
                with mock.patch.object(express_sdk, "get_pooled_express_client", side_effect=client_for):
                    result = await express_sdk.ExpressSDKUpstream().chat_completions(obj, request)
                    body = ("".join([c async for c in result.body_iterator])
                            if hasattr(result, "body_iterator") else result.body.decode())
                self.assertIn("answer", body)
                self.assertEqual([key for key, _ in calls], ["key-1", "key-1", "key-2", "key-2"])
                self.assertIn("/project-1/", calls[0][1])
                self.assertIn("/project-2/", calls[2][1])
        self.assertEqual(self.stats.retry_counts, 3)
        self.assertEqual(self.stats.success_requests, 3)

    async def test_random_selection_excludes_failed_key_if_alternative_exists(self):
        self.settings["roundrobin"] = False
        manager = ExpressKeyManager()
        with mock.patch("express_key_manager.random.shuffle", side_effect=lambda records: None):
            self.assertEqual(manager.get_express_record()["key"], "key-1")
            self.assertEqual(manager.get_express_record(exclude_key="key-1")["key"], "key-2")

    async def test_single_key_retries_repeated_429_and_keeps_bare_fallback(self):
        self.records[:] = self.records[:1]
        self.settings.update(retry_max=10, roundrobin=True)
        for mode in ("json", "stream", "fake"):
            with self.subTest(mode=mode):
                calls = []
                bare_attempts = 0
                self.settings["fake_streaming"] = mode == "fake"

                async def generate(**kwargs):
                    nonlocal bare_attempts
                    model = kwargs["model"]
                    calls.append(model)
                    if model.startswith("projects/"):
                        raise api_error(404, "Publisher model was not found")
                    bare_attempts += 1
                    if bare_attempts <= 10:
                        raise api_error(429)
                    return text_response()

                async def stream(**kwargs):
                    response = await generate(**kwargs)

                    async def iterate():
                        yield response
                    return iterate()

                client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
                    generate_content=generate, generate_content_stream=stream)))
                obj = api_helpers.OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=mode != "json")
                request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=ExpressKeyManager())),
                                          is_disconnected=mock.AsyncMock(return_value=False))
                with mock.patch.object(express_sdk, "get_pooled_express_client", return_value=client):
                    response = await express_sdk.ExpressSDKUpstream().chat_completions(obj, request)
                    body = ("".join([chunk async for chunk in response.body_iterator])
                            if hasattr(response, "body_iterator") else response.body.decode())
                self.assertIn("answer", body)
                self.assertEqual(len(calls), 12)  # 一次路径兜底 + 首次生成及十次重试
                self.assertEqual(calls[1:], ["gemini-3.6-flash"] * 11)
        self.assertEqual(self.stats.retry_counts, 30)
        self.assertEqual(self.stats.success_requests, 3)

    async def test_shared_single_key_concurrent_requests_have_independent_budgets(self):
        self.records[:] = self.records[:1]
        self.settings.update(retry_max=2, roundrobin=False, fake_streaming=False, express_location="")
        attempts = {}

        async def generate(**kwargs):
            text = kwargs["contents"][0].parts[0].text
            attempts[text] = attempts.get(text, 0) + 1
            await asyncio.sleep(0)
            if attempts[text] <= 2:
                raise api_error(429)
            return text_response("answer-" + text)

        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate)))
        manager = ExpressKeyManager()
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)),
                                  is_disconnected=mock.AsyncMock(return_value=False))
        upstream = express_sdk.ExpressSDKUpstream()
        with mock.patch.object(express_sdk, "get_pooled_express_client", return_value=client):
            responses = await asyncio.gather(*[
                upstream.chat_completions(api_helpers.OpenAIRequest(
                    model="gemini-3.6-flash", messages=[{"role": "user", "content": f"user-{i}"}]), request)
                for i in range(6)])
        self.assertTrue(all(response.status_code == 200 for response in responses))
        self.assertEqual(attempts, {f"user-{i}": 3 for i in range(6)})
        self.assertEqual(self.stats.retry_counts, 12)
        self.assertEqual(self.stats.success_requests, 6)

    async def test_single_key_can_repeat_and_disabled_keys_are_skipped(self):
        manager = ExpressKeyManager()
        self.records[1]["enabled"] = False
        self.assertEqual(manager.get_express_record(exclude_key="key-1")["key"], "key-1")
        self.records[0]["enabled"] = False
        self.assertIsNone(manager.get_express_record(exclude_key="key-1"))


class RetryPolicyTests(unittest.TestCase):
    def test_sparse_candidate_indices_keep_separate_tool_call_indices(self):
        indexer = api_helpers.ToolCallIndexer()
        for candidate_index in (3, 4):
            response = types.GenerateContentResponse(candidates=[types.Candidate(
                index=candidate_index, content=types.Content(role="model", parts=[types.Part(
                    function_call=types.FunctionCall(name="lookup", args={}))]))])
            chunk = json.loads(api_helpers.convert_chunk_to_openai(response, "gemini-test", "id", indexer=indexer)[6:])
            self.assertEqual(chunk["choices"][0]["index"], candidate_index)
            self.assertEqual(chunk["choices"][0]["delta"]["tool_calls"][0]["index"], 0)

    def test_retry_after_supports_http_dates_and_past_dates(self):
        with mock.patch("retry_policy.time.time", return_value=1000):
            self.assertEqual(retry_delay("Thu, 01 Jan 1970 00:17:00 GMT", 5), 20)
            self.assertEqual(retry_delay("Thu, 01 Jan 1970 00:10:00 GMT", 5), 0)

    def test_permanent_local_errors_are_not_retryable(self):
        self.assertFalse(api_helpers.is_retryable_exception(ValueError("bad quota model-429")))
        self.assertFalse(api_helpers.is_retryable_exception(httpx.LocalProtocolError("Invalid header")))
        self.assertTrue(api_helpers.is_retryable_exception(CurlTimeout("Operation timed out")))
        self.assertEqual(api_helpers.extract_upstream_error(CurlTimeout("Operation timed out"))[0], 504)

    def test_error_status_survives_httpx_and_timeout_exceptions(self):
        response = httpx.Response(429, request=httpx.Request("POST", "https://example.test"))
        self.assertEqual(api_helpers.extract_upstream_error(httpx.HTTPStatusError("rate limit", request=response.request, response=response))[0], 429)
        self.assertEqual(api_helpers.extract_upstream_error(httpx.ReadTimeout("timeout"))[0], 504)

    def test_model_whitespace_and_full_resource_paths(self):
        for name in ("", "   ", "-search", "[EXPRESS] "):
            self.assertIsNotNone(express_sdk._normalize_model_name(name)[2])
        self.assertEqual(express_sdk._normalize_model_name(" [EXPRESS]  gemini-test-search ")[:2], ("gemini-test", True))
        for path in ("tunedModels/custom", "endpoints/custom"):
            self.assertEqual(express_sdk.resolve_express_model_path(path, {"express_location": "global"}, "project"), path)


if __name__ == "__main__":
    unittest.main()
