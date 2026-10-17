"""Gemini 原生接口统计和重试回归：python scripts/test_gemini_native_stats_retry.py。

通过实际 FastAPI 路由和统计中间件发请求，上游完全使用 MockTransport。
"""
import asyncio
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

_STATE_DIR = tempfile.TemporaryDirectory(prefix="v2o-native-stats-")
os.environ["STATE_DIR"] = _STATE_DIR.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.testclient import TestClient

import config
import main
from logger import ProxyStats
from routes import gemini_native
import retry_policy


class KeyManager:
    def __init__(self, keys=("key-1",)):
        self.records = [{"key": key, "project_id": f"project-{i}"}
                        for i, key in enumerate(keys)]
        self.index = 0

    def get_total_keys(self):
        return len(self.records)

    def get_express_record(self, exclude_key=None):
        if not self.records:
            return None
        record = self.records[self.index % len(self.records)]
        self.index += 1
        return record.copy()


class NativeStatsRetryTests(unittest.TestCase):
    def setUp(self):
        self.stats = ProxyStats()
        self.settings = dict(config.DEFAULT_SETTINGS, retry_max=2,
                             retry_backoff_seconds=0, express_location="")
        self.requests = []
        self.manager = KeyManager()
        self.responder = lambda request: httpx.Response(200, json={"candidates": []})

        def handler(request):
            self.requests.append(request)
            return self.responder(request)

        self.upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        patches = [
            mock.patch.object(main, "stats", self.stats),
            mock.patch.object(gemini_native, "stats", self.stats),
            mock.patch.object(gemini_native, "API_KEY", "client-key"),
            mock.patch.object(main.app.state, "express_key_manager", self.manager),
            mock.patch.object(gemini_native, "passthrough_client_slot",
                              types.SimpleNamespace(get=lambda factory: self.upstream)),
            mock.patch.object(gemini_native.app_state, "get_settings",
                              side_effect=lambda: self.settings.copy()),
            mock.patch.object(gemini_native.app_state, "get_setting",
                              side_effect=lambda key, default=None: self.settings.get(key, default)),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.addCleanup(lambda: asyncio.run(self.upstream.aclose()))
        # 不进入 lifespan：不启动真实上游探测、模型刷新等后台网络任务。
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)

    def post(self, action="generateContent", prefix="v1beta", **kwargs):
        kwargs.setdefault("headers", {"x-goog-api-key": "client-key"})
        if "content" not in kwargs:
            kwargs.setdefault("json", {"contents": [{"parts": [{"text": "hi"}]}]})
        return self.client.post(f"/{prefix}/models/gemini-test:{action}", **kwargs)

    def assert_stats(self, total=1, success=1, error=0, retries=0):
        values = self.stats.get_json_stats()
        self.assertEqual({key: values[key] for key in ("total", "success", "error", "retries")},
                         dict(total=total, success=success, error=error, retries=retries))

    def test_native_actions_and_prefixes_count_once(self):
        def responder(request):
            if ":streamGenerateContent" in str(request.url):
                return httpx.Response(200, content=b'data: {"candidates": []}\n\n',
                                      headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json={"candidates": []})

        self.responder = responder
        count = 0
        for prefix in ("v1beta", "v1"):
            for action in ("generateContent", "streamGenerateContent", "countTokens", "embedContent"):
                with self.subTest(prefix=prefix, action=action):
                    self.assertEqual(self.post(action, prefix).status_code, 200)
                    count += 1
                    self.assert_stats(total=count, success=count)

    def test_single_key_429_retries_for_both_formats(self):
        for action in ("generateContent", "streamGenerateContent"):
            with self.subTest(action=action):
                self.requests.clear()
                self.responder = lambda request: (
                    httpx.Response(429, json={"error": {"code": 429}})
                    if len(self.requests) == 1 else
                    httpx.Response(200, content=b'data: {"candidates": []}\n\n',
                                   headers={"content-type": "text/event-stream"}))
                response = self.post(action)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(len(self.requests), 2)
                self.assertEqual({r.headers["x-goog-api-key"] for r in self.requests}, {"key-1"})
        self.assert_stats(total=2, success=2, retries=2)

    def test_single_key_repeated_429_keeps_bare_fallback_and_full_budget(self):
        self.settings.update(retry_max=10, express_location="global")
        for action in ("generateContent", "streamGenerateContent"):
            with self.subTest(action=action):
                self.requests.clear()
                bare_attempts = 0

                def responder(request):
                    nonlocal bare_attempts
                    if "/projects/" in request.url.path:
                        return httpx.Response(404, json={"error": {"message": "Publisher model was not found"}})
                    bare_attempts += 1
                    if bare_attempts <= 10:
                        return httpx.Response(429, json={"error": {"code": 429}})
                    return httpx.Response(200, content=b'data: {"candidates": []}\n\n',
                                          headers={"content-type": "text/event-stream"})

                self.responder = responder
                self.assertEqual(self.post(action).status_code, 200)
                self.assertEqual(len(self.requests), 12)
                self.assertTrue(all("/projects/" not in req.url.path for req in self.requests[1:]))
                self.assertEqual({req.headers["x-goog-api-key"] for req in self.requests}, {"key-1"})
        self.assert_stats(total=2, success=2, retries=20)

    def test_pool_can_wrap_until_configured_budget_exhausted(self):
        self.manager.records.append({"key": "key-2", "project_id": "project-2"})
        self.settings["retry_max"] = 3
        body = b'{"error":{"code":429,"message":"quota"}}'
        self.responder = lambda request: httpx.Response(429, content=body,
                                                       headers={"retry-after": "0"})
        response = self.post()
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.content, body)
        self.assertEqual(response.headers["retry-after"], "0")
        self.assertEqual([r.headers["x-goog-api-key"] for r in self.requests],
                         ["key-1", "key-2", "key-1", "key-2"])
        self.assert_stats(success=0, error=1, retries=3)

    def test_zero_retries_still_sends_one_request(self):
        self.settings["retry_max"] = 0
        self.responder = lambda request: httpx.Response(429, json={"error": {"code": 429}})
        self.assertEqual(self.post().status_code, 429)
        self.assertEqual(len(self.requests), 1)
        self.assert_stats(success=0, error=1)

    def test_non_retryable_error_is_counted_once(self):
        self.responder = lambda request: httpx.Response(400, json={"error": {"code": 400}})
        self.assertEqual(self.post().status_code, 400)
        self.assertEqual(len(self.requests), 1)
        self.assert_stats(success=0, error=1)

    def test_no_keys_is_counted_as_error(self):
        self.manager.records.clear()
        self.assertEqual(self.post().status_code, 403)
        self.assertEqual(len(self.requests), 0)
        self.assert_stats(success=0, error=1)

    def test_auth_and_invalid_body_are_counted(self):
        self.assertEqual(self.post(headers={"x-goog-api-key": "wrong"}).status_code, 401)
        self.assertEqual(self.post(content=b"not-json").status_code, 400)
        self.assertEqual(len(self.requests), 0)
        self.assert_stats(total=2, success=0, error=2)

    def test_health_and_model_list_do_not_count(self):
        self.assertEqual(self.client.get("/healthz").status_code, 200)
        with mock.patch.object(gemini_native, "get_express_models", return_value=["gemini-test"]):
            response = self.client.get("/v1beta/models", headers={"x-goog-api-key": "client-key"})
            self.assertEqual(response.status_code, 200)
        self.assert_stats(total=0, success=0)

    def test_openai_route_still_counts(self):
        self.assertEqual(self.client.post("/v1/chat/completions", json={}).status_code, 401)
        self.assert_stats(success=0, error=1)


    def test_configured_backoff_and_retry_after(self):
        self.settings["retry_backoff_seconds"] = 7
        for header, expected in ((None, 7), ("0.25", 0.25), ("invalid", 7),
                                 ("-1", 7), ("NaN", 7), ("Infinity", 7), ("300", 120)):
            with self.subTest(retry_after=header):
                self.requests.clear()

                def responder(request):
                    if len(self.requests) == 1:
                        return httpx.Response(429, json={"error": {"code": 429}},
                                              headers={"retry-after": header} if header is not None else {})
                    return httpx.Response(200, json={})

                self.responder = responder
                with mock.patch.object(gemini_native, "_wait_for_retry", return_value=True) as wait:
                    self.assertEqual(self.post().status_code, 200)
                    self.assertEqual(wait.await_args.args[1], expected)
        self.assert_stats(total=7, success=7, retries=7)

    def test_server_errors_use_same_retry_budget(self):
        for status in (500, 502, 503, 504):
            with self.subTest(status=status):
                self.requests.clear()

                def responder(request):
                    if len(self.requests) == 1:
                        return httpx.Response(status, json={"error": {"code": status}})
                    return httpx.Response(200, json={})

                self.responder = responder
                self.assertEqual(self.post().status_code, 200)
                self.assertEqual(len(self.requests), 2)
        self.assert_stats(total=4, success=4, retries=4)

    def test_network_timeout_retries_single_key(self):
        def responder(request):
            if len(self.requests) == 1:
                raise httpx.ReadTimeout("timeout", request=request)
            return httpx.Response(200, json={})
        self.responder = responder
        self.assertEqual(self.post().status_code, 200)
        self.assertEqual(len(self.requests), 2)
        self.assert_stats(retries=1)

    def test_pin_fallback_then_429_rotates_key_and_resolves_its_project(self):
        self.settings["express_location"] = "global"
        self.manager.records.append({"key": "key-2", "project_id": "project-2"})

        def responder(request):
            if len(self.requests) == 1:
                return httpx.Response(404, json={"error": {"message": "Publisher model was not found"}})
            if len(self.requests) == 2:
                return httpx.Response(429, json={"error": {"code": 429}})
            return httpx.Response(200, json={})
        self.responder = responder
        self.assertEqual(self.post().status_code, 200)
        self.assertEqual(len(self.requests), 3)
        self.assertIn("/projects/project-0/", self.requests[0].url.path)
        self.assertNotIn("/projects/", self.requests[1].url.path)
        self.assertIn("/projects/project-2/", self.requests[2].url.path)
        self.assertEqual([r.headers["x-goog-api-key"] for r in self.requests],
                         ["key-1", "key-1", "key-2"])
        self.assert_stats(retries=1)

    def test_disconnect_during_backoff_stops_without_counting_retry(self):
        self.responder = lambda request: httpx.Response(429, json={"error": {"code": 429}})
        with mock.patch.object(gemini_native, "_wait_for_retry", return_value=False):
            self.assertEqual(self.post().status_code, 499)
        self.assertEqual(len(self.requests), 1)
        self.assert_stats(success=0, error=1)

    def test_stream_failure_counts_error_and_closes_upstream(self):
        async def broken_stream():
            yield b'data: {"candidates": []}\n\n'
            raise httpx.ReadError("stream interrupted")

        upstream = httpx.Response(200, content=broken_stream(),
                                  headers={"content-type": "text/event-stream"})

        async def run():
            response = await gemini_native._relay_stream(upstream)
            with self.assertRaises(httpx.ReadError):
                async for _ in response.body_iterator:
                    pass
        asyncio.run(run())
        self.assertTrue(upstream.is_closed)
        self.assert_stats(total=0, success=0, error=1)

    def test_429_stream_response_is_closed_before_retry(self):
        closed = []

        class ErrorStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'{"error":{"code":429}}'

            async def aclose(self):
                closed.append(True)

        def responder(request):
            if len(self.requests) == 1:
                return httpx.Response(429, stream=ErrorStream())
            self.assertEqual(closed, [True])
            return httpx.Response(200, content=b'data: {"candidates": []}\n\n',
                                  headers={"content-type": "text/event-stream"})
        self.responder = responder
        response = self.post("streamGenerateContent")
        self.assertEqual(response.content, b'data: {"candidates": []}\n\n')
        self.assert_stats(retries=1)

    def test_explicit_sse_request_rejects_non_sse_upstream(self):
        response = self.client.post("/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
                                    json={}, headers={"x-goog-api-key": "client-key"})
        self.assertEqual(response.status_code, 502)
        self.assertIn("SSE", response.json()["error"]["message"])
        self.assert_stats(success=0, error=1)

    def test_full_resource_model_paths_are_routable(self):
        for path in ("projects/project/locations/global/publishers/google/models/gemini-test",
                     "publishers/google/models/gemini-test", "tunedModels/custom"):
            with self.subTest(path=path):
                response = self.client.post(f"/v1beta/models/{path}:generateContent", json={},
                                            headers={"x-goog-api-key": "client-key"})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(self.requests[-1].url.path, f"/v1beta1/{path}:generateContent")
        self.assert_stats(total=3, success=3)

    def test_invalid_local_request_error_is_not_retried(self):
        def responder(request):
            raise ValueError("Invalid header for quota model-429")
        self.responder = responder
        self.assertEqual(self.post().status_code, 502)
        self.assertEqual(len(self.requests), 1)
        self.assert_stats(success=0, error=1)

    def test_upstream_base_url_trailing_slash_is_normalized(self):
        with mock.patch.object(gemini_native, "EXPRESS_BASE", "https://example.test/"):
            self.assertEqual(gemini_native._build_upstream_url("gemini-test", "countTokens"),
                             "https://example.test/v1beta1/publishers/google/models/gemini-test:countTokens")


class RetryWaitTests(unittest.IsolatedAsyncioTestCase):
    async def test_disconnect_is_checked_during_backoff(self):
        request = types.SimpleNamespace(is_disconnected=mock.AsyncMock(side_effect=[False, True]))
        with mock.patch.object(retry_policy.asyncio, "sleep", new_callable=mock.AsyncMock) as sleep:
            self.assertFalse(await gemini_native._wait_for_retry(request, 60))
        sleep.assert_awaited_once()
        self.assertLessEqual(sleep.await_args.args[0], 0.5)


if __name__ == "__main__":
    unittest.main()
