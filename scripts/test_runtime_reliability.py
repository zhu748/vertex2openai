"""运行态、连接、续传和 SDK 边界回归；不使用真实凭据或外网。"""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE_DIR = tempfile.TemporaryDirectory(prefix="v2o-runtime-test-")
os.environ["STATE_DIR"] = _STATE_DIR.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.testclient import TestClient
from google.genai import types

import api_helpers
import auth
import client_pool
import impersonate
import main
import message_processing
import model_loader
import resumable
from logger import ProxyStats
from models import OpenAIRequest
from routes import chat_api, gemini_native
from usage import map_usage


class ConfigTests(unittest.TestCase):
    def setUp(self):
        main.app.dependency_overrides[main.require_auth] = lambda: True
        self.addCleanup(main.app.dependency_overrides.clear)
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        self.save = mock.patch.object(main.app_state, "update_settings").start()
        self.override = mock.patch.object(main.app_state, "set_model_override").start()
        self.addCleanup(mock.patch.stopall)

    def test_malformed_json_is_rejected_without_saving(self):
        for path in ("/api/settings", "/api/model-overrides/gemini-test"):
            self.assertEqual(self.client.post(path, content=b"{").status_code, 400)
        self.save.assert_not_called()
        self.override.assert_not_called()

    def test_invalid_profile_types_return_400(self):
        for value in ([], {}, None, 2):
            with self.subTest(value=value):
                self.assertEqual(self.client.post("/api/settings", json={"express_ua_profile": value}).status_code, 400)
        self.save.assert_not_called()

    def test_invalid_numbers_cannot_be_persisted(self):
        for key, value in (("default_temperature", float("nan")),
                           ("default_max_tokens", float("inf")), ("retry_max", 1.5)):
            with self.subTest(key=key, value=value):
                response = self.client.post("/api/settings", content=json.dumps({key: value}))
                self.assertEqual(response.status_code, 400)
        self.save.assert_not_called()

    def test_invalid_header_values_are_rejected(self):
        for value in ("tool\r\nInjected: value", "中文", "tool\x00"):
            self.assertEqual(self.client.post("/api/settings", json={"express_ua_custom": value}).status_code, 400)

    def test_proxy_ports_and_protocol_match_httpx_support(self):
        # 非法端口/协议仍被拒；socks4 现在经 sing-box 合法（第十六轮行为变更）
        for url in ("http://localhost:abc", "http://localhost:70000", "http://localhost:0",
                    "gopher://localhost:70", "vless://only-uuid"):
            self.assertTrue(main.validate_proxy_url_format(url), url)
        for url in ("socks5://user:pass@localhost:1080", "socks4://localhost:1080",
                    "socks4a://localhost:1080", "vless://uuid@localhost:443?security=tls&type=ws",
                    "direct", ""):
            self.assertEqual(main.validate_proxy_url_format(url), "", url)

    def test_unicode_login_and_api_key_validation(self):
        with mock.patch.object(auth, "API_KEY", "口令🔑"):
            self.assertTrue(auth.validate_api_key("口令🔑"))
            self.assertFalse(auth.validate_api_key("错误"))
        with mock.patch.object(main.config, "API_KEY", "口令🔑"):
            self.assertEqual(self.client.post("/api/login", json={"password": "口令🔑"}).status_code, 200)
            self.assertEqual(self.client.post("/api/login", json={"password": "错误"}).status_code, 401)
        with mock.patch.object(gemini_native, "API_KEY", "口令🔑"):
            self.assertTrue(asyncio.run(gemini_native._require_native_key(None, "口令🔑")))


class TransportTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def curl_response(chunks):
        async def iterate():
            for chunk in chunks:
                yield chunk
        return SimpleNamespace(status_code=200, aiter_content=iterate,
                               acontent=mock.AsyncMock(return_value=b"payload"))

    async def test_curl_stream_preserves_split_utf8(self):
        body = "你好，Gemini 🌍".encode()
        response = impersonate._CurlRespShim(self.curl_response([bytes([b]) for b in body]), True)
        self.assertEqual("".join([chunk async for chunk in response.aiter_text()]), body.decode())
        self.assertEqual(await response.aread(), body)

    async def test_curl_empty_stream_is_not_consumed_twice(self):
        raw = self.curl_response([])
        response = impersonate._CurlRespShim(raw, True)
        self.assertEqual([c async for c in response.aiter_text()], [])
        self.assertEqual(await response.aread(), b"")
        raw.acontent.assert_not_awaited()

    async def test_curl_aread_is_idempotent(self):
        raw = self.curl_response([])
        response = impersonate._CurlRespShim(raw, True)
        self.assertEqual(await response.aread(), b"payload")
        self.assertEqual(await response.aread(), b"payload")
        raw.acontent.assert_awaited_once()

    async def test_delayed_clients_are_closed_on_shutdown_once(self):
        closer = client_pool._GracefulCloser()
        close = mock.AsyncMock()
        closer.schedule(close)
        await closer.aclose()
        await closer.aclose()
        close.assert_awaited_once()
        self.assertFalse(closer._tasks)

    async def test_current_sdk_httpx_and_cookie_pools_close(self):
        sdk = SimpleNamespace(close=mock.Mock(), aio=SimpleNamespace(aclose=mock.AsyncMock()))
        direct = SimpleNamespace(aclose=mock.AsyncMock())
        direct2 = SimpleNamespace(aclose=mock.AsyncMock())
        cookie = SimpleNamespace(aclose=mock.AsyncMock())
        sdk_pool = client_pool.SdkClientPool()
        pool = client_pool.HttpxClientPool()
        cookie_pool = impersonate._UpstreamClientPool()
        with mock.patch.object(client_pool.genai, "Client", return_value=sdk):
            self.assertIs(sdk_pool.get("test-key"), sdk_pool.get("test-key"))
        # 直连桶复用："" 与 "direct" 哨兵解析后同为直连，共享同一客户端
        self.assertIs(pool.get(lambda: direct, ""), direct)
        self.assertIs(pool.get(lambda: direct, ""), direct)
        self.assertIs(pool.get(lambda: direct2, "direct"), direct)
        # 全局 socks5 代理签名独立成桶
        with mock.patch("config.PROXY_URL", "socks5://1.2.3.4:1080"):
            self.assertIs(pool.get(lambda: direct2, None), direct2)
            self.assertIs(pool.get(lambda: direct2, None), direct2)
        cookie_pool.get({}, lambda kwargs: cookie)
        await asyncio.gather(sdk_pool.aclose(), pool.aclose(), cookie_pool.aclose())
        self.assertEqual(sdk_pool.size(), 0)
        sdk.close.assert_called_once()
        sdk.aio.aclose.assert_awaited_once()
        direct.aclose.assert_awaited_once()
        cookie.aclose.assert_awaited_once()

    async def test_lifespan_awaits_background_tasks_and_pool_cleanup(self):
        tasks = set()
        with mock.patch.object(main, "_background_tasks", tasks), \
             mock.patch.object(main.app_state, "import_env_express_keys_once"), \
             mock.patch.object(main, "get_models_config", return_value={"models": []}), \
             mock.patch.object(main, "schedule_models_refresh"), \
             mock.patch.object(main, "_probe_express_projects_on_startup"), \
             mock.patch.object(main, "close_all_clients") as close_clients, \
             mock.patch.object(main, "close_models_refresh") as close_models:
            async with main.lifespan(main.app):
                extra = main._start_background_task(asyncio.Event().wait())
                await asyncio.sleep(0)
            self.assertTrue(extra.cancelled())
            self.assertFalse(tasks)
            close_clients.assert_awaited_once()
            close_models.assert_awaited_once()


class ResumableTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)

    async def test_same_request_has_only_one_buffer_under_concurrency(self):
        results = await asyncio.gather(*[self.store.create_if_absent("same") for _ in range(20)])
        self.assertEqual(sum(is_new for _, is_new in results), 1)
        self.assertEqual(len({id(buf) for buf, _ in results}), 1)

    async def test_delivered_buffer_replacement_is_atomic(self):
        old, _ = await self.store.create_if_absent("same")
        old.mark_delivered()
        results = await asyncio.gather(*[
            self.store.create_if_absent("same", replace_delivered=True) for _ in range(20)])
        self.assertEqual(sum(is_new for _, is_new in results), 1)
        self.assertFalse(await self.store.drop("same", expected=old))
        self.assertIs(await self.store.get("same"), results[0][0])

    async def test_eviction_stops_pump_and_wakes_readers(self):
        with mock.patch.object(resumable, "max_streams", return_value=1):
            old, _ = await self.store.create_if_absent("old")
            task = asyncio.create_task(asyncio.Event().wait())
            self.store.register_pump("old", task, buffer=old)
            waiter = asyncio.create_task(old.wait_for(100, 60))
            await self.store.create_if_absent("new")
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.wait_for(waiter, 1)
            self.assertTrue(task.cancelled())
            self.assertTrue(old.done)
            self.assertEqual(old.status_code, 503)
            self.assertTrue(old.events[-1].strip().endswith("[DONE]"))

    async def test_old_pump_callback_does_not_remove_replacement(self):
        old, _ = await self.store.create_if_absent("same")
        old_task = asyncio.create_task(asyncio.Event().wait())
        self.store.register_pump("same", old_task, buffer=old)
        await self.store.drop("same")
        new, _ = await self.store.create_if_absent("same")
        new_task = asyncio.create_task(asyncio.Event().wait())
        self.store.register_pump("same", new_task, buffer=new)
        await asyncio.gather(old_task, return_exceptions=True)
        self.assertIs(self.store._pumps.get("same"), new_task)

    async def test_expired_running_buffer_cancels_its_task(self):
        buf, _ = await self.store.create_if_absent("stale")
        buf.created_at -= 100000
        task = asyncio.create_task(asyncio.Event().wait())
        self.store.register_pump("stale", task, buffer=buf)
        self.assertEqual(await self.store.sweep(), 1)
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())

    async def test_resume_header_advances_past_delivered_frame(self):
        request = SimpleNamespace(headers={"last-event-id": "2"}, query_params={"resume_from": "1"})
        self.assertEqual(chat_api._resume_start_seq(request), 3)
        request.headers.clear()
        self.assertEqual(chat_api._resume_start_seq(request), 1)

    async def test_buffered_json_preserves_upstream_error_code(self):
        error = Exception("Resource exhausted")
        error.code = 429
        request = SimpleNamespace(headers={})
        obj = OpenAIRequest(model="gemini-test", messages=[])
        with mock.patch.object(chat_api, "resumable_store", self.store), \
             mock.patch.object(chat_api, "_call_upstream", side_effect=error):
            response = await chat_api._resumable_json(request, obj, "test-key")
        self.assertEqual(response.status_code, 429)
        self.assertEqual(json.loads(response.body)["error"]["code"], 429)


class ModelCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.patches = [mock.patch.object(model_loader, "_model_cache", {"models": ["cached-model"]}),
                        mock.patch.object(model_loader, "_cache_lock", asyncio.Lock()),
                        mock.patch.object(model_loader, "_refresh_task", None),
                        mock.patch.object(model_loader, "_next_refresh_allowed", 0.0)]
        for patch in self.patches:
            patch.start()

    async def asyncTearDown(self):
        await model_loader.close_models_refresh()
        for patch in reversed(self.patches):
            patch.stop()

    async def test_slow_refresh_does_not_block_cached_lists(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def fetch():
            started.set()
            await release.wait()
            return {"models": ["fresh-model"]}

        with mock.patch.object(model_loader, "fetch_and_parse_models_config", side_effect=fetch) as upstream:
            model_loader.schedule_models_refresh()
            await asyncio.wait_for(started.wait(), 1)
            lists = await asyncio.wait_for(asyncio.gather(*[
                model_loader.get_express_models() for _ in range(20)]), 1)
            self.assertTrue(all(models == ["cached-model"] for models in lists))
            self.assertEqual(upstream.await_count, 1)
            release.set()
            await model_loader._refresh_task
            self.assertEqual(await model_loader.get_express_models(), ["fresh-model"])

    async def test_failed_refresh_preserves_cache_and_backs_off(self):
        with mock.patch.object(model_loader, "fetch_and_parse_models_config", return_value=None) as upstream:
            self.assertFalse(await model_loader.refresh_models_config_cache())
            await model_loader.refresh_models_config_cache()
            self.assertEqual(upstream.await_count, 1)
            self.assertEqual(await model_loader.get_express_models(), ["cached-model"])

    async def test_empty_remote_config_cannot_erase_working_cache(self):
        with mock.patch.object(model_loader, "fetch_and_parse_models_config", return_value={"models": []}):
            self.assertFalse(await model_loader.refresh_models_config_cache())
            self.assertEqual(await model_loader.get_express_models(), ["cached-model"])

    async def test_cold_cache_loads_local_without_network(self):
        model_loader._model_cache = None
        with mock.patch.object(model_loader, "_load_local_models_config", return_value={"models": ["local"]}), \
             mock.patch.object(model_loader, "fetch_and_parse_models_config") as upstream:
            self.assertEqual(await model_loader.get_models_config(), {"models": ["local"]})
            upstream.assert_not_called()

    async def test_model_ids_are_trimmed_and_deduplicated(self):
        self.assertEqual(model_loader._normalize_models_config({"models": [" gemini ", "gemini", "", 4]}),
                         {"models": ["gemini"]})


class SdkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stats = ProxyStats()
        self.settings = {"retry_max": 0, "retry_backoff_seconds": 0, "fake_streaming_interval": 0}
        self.patches = [mock.patch.object(api_helpers, "stats", self.stats),
                        mock.patch.object(api_helpers.app_state, "get_setting",
                                          side_effect=lambda key, default=None: self.settings.get(key, default))]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)

    async def test_fallback_has_its_own_attempt_in_all_response_modes(self):
        for mode in ("json", "stream", "fake-stream"):
            with self.subTest(mode=mode):
                calls, closed = [], []
                response = types.GenerateContentResponse(candidates=[types.Candidate(
                    content=types.Content(role="model", parts=[types.Part(text="ok")]), finish_reason="STOP")])

                async def generate(**kwargs):
                    calls.append(kwargs["model"])
                    if kwargs["model"].startswith("projects/"):
                        raise Exception("404 Publisher model was not found")
                    return response

                async def stream(**kwargs):
                    item = await generate(**kwargs)

                    async def iterate():
                        try:
                            yield item
                        finally:
                            closed.append(True)
                    return iterate()

                client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
                    generate_content=generate, generate_content_stream=stream)))
                self.settings["fake_streaming"] = mode == "fake-stream"
                obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=mode != "json")
                result = await api_helpers.execute_gemini_call(
                    client, "projects/p/locations/global/publishers/google/models/gemini-3.6-flash",
                    lambda messages: [], {}, obj, fallback_model="gemini-3.6-flash")
                if hasattr(result, "body_iterator"):
                    body = "".join([chunk async for chunk in result.body_iterator])
                    self.assertIn("ok", body)
                    self.assertIn("[DONE]", body)
                else:
                    self.assertEqual(result.status_code, 200)
                self.assertEqual(len(calls), 2)
                self.assertEqual(calls[-1], "gemini-3.6-flash")
                if mode == "stream":
                    self.assertEqual(closed, [True])
        self.assertEqual(self.stats.success_requests, 3)
        self.assertEqual(self.stats.retry_counts, 0)

    async def test_usage_includes_thinking_and_tool_prompt(self):
        metadata = types.GenerateContentResponseUsageMetadata(
            prompt_token_count=10, candidates_token_count=5, thoughts_token_count=7,
            tool_use_prompt_token_count=3, total_token_count=25)
        expected = {"prompt_tokens": 13, "completion_tokens": 12, "total_tokens": 25}
        self.assertEqual(map_usage(metadata), expected)
        response = types.GenerateContentResponse(usage_metadata=metadata)
        self.assertEqual(api_helpers._record_usage(response), expected)
        self.assertEqual(message_processing.convert_to_openai_format(response, "gemini-test")["usage"], expected)

    async def test_success_without_usage_is_still_counted(self):
        api_helpers._record_usage(SimpleNamespace(usage_metadata=None))
        self.assertEqual(self.stats.success_requests, 1)

    async def test_retry_classification_includes_timeouts_and_server_errors(self):
        self.assertTrue(api_helpers.is_retryable_exception(httpx.ReadTimeout("timeout")))
        self.assertTrue(api_helpers.is_retryable_exception(httpx.ConnectError("network")))
        self.assertTrue(api_helpers.is_retryable_exception(Exception("Resource exhausted")))
        for code in (429, 500, 502, 503, 504):
            error = Exception("upstream error")
            error.code = code
            self.assertTrue(api_helpers.is_retryable_exception(error))
        self.assertFalse(api_helpers.is_retryable_exception(ValueError("invalid argument")))

    async def test_nonfinite_persisted_retry_settings_use_defaults(self):
        self.settings.update(retry_max=float("inf"), retry_backoff_seconds=float("nan"))
        count, delay = api_helpers.get_retry_settings()
        self.assertEqual(count, 10)
        self.assertEqual(delay, 5)


class ImageFetchTests(unittest.TestCase):
    def test_download_stops_at_size_limit_and_closes_response(self):
        read, closed = [], []

        class ImageStream(httpx.SyncByteStream):
            def __iter__(self):
                for _ in range(10):
                    read.append(True)
                    yield b"x" * (64 * 1024)

            def close(self):
                closed.append(True)

        client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(
            200, headers={"content-type": "image/png"}, stream=ImageStream())))
        with mock.patch.object(message_processing.httpx, "Client", return_value=client), \
             mock.patch.object(message_processing, "_is_blocked_host", return_value=False), \
             mock.patch.object(message_processing, "get_effective_proxy_url", return_value=""), \
             mock.patch.object(message_processing, "MAX_REMOTE_IMAGE_BYTES", 64 * 1024):
            self.assertIsNone(message_processing.fetch_remote_image("https://example.test/image"))
        self.assertEqual(len(read), 2)
        self.assertEqual(closed, [True])


if __name__ == "__main__":
    unittest.main()
