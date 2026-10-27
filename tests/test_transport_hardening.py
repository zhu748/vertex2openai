"""Actual SDK/wire behavior, proxy isolation, memory growth and durable saves."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-transport-hardening-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.testclient import TestClient
from google import genai
from google.genai import types

import api_helpers
import client_pool
import config
import cookie_auth
import impersonate
import main
from routes import credentials_api
from routes import settings_api
import console_auth
import image_processing
import proxy_resolver
import retry_policy
import runtime_state
from http_options import get_http_options
from models import OpenAIRequest
from upstreams import express_sdk, cookie_proxy


class StreamSizeTests(unittest.TestCase):
    def bytes_for(self, count):
        indexer = api_helpers.ToolCallIndexer()
        total = 0
        for i in range(count):
            response = types.GenerateContentResponse(candidates=[types.Candidate(
                content=types.Content(role="model", parts=[types.Part(text="x" * 64)]),
                finish_reason="STOP" if i == count - 1 else None)])
            frame = api_helpers.convert_chunk_to_openai(response, "gemini-3.6-flash", "fixed", indexer=indexer)
            total += len(frame.encode())
            if i < count - 1:
                self.assertNotIn("ordinary_parts", frame)
        payload = json.loads(frame[6:])
        self.assertEqual(len(payload["choices"][0]["delta"]["extra_content"]["google"]["ordinary_parts"]), count)
        return total

    def test_plain_stream_size_grows_linearly_and_keeps_final_metadata(self):
        small, large = self.bytes_for(200), self.bytes_for(400)
        self.assertLess(large, small * 2.2)
        self.assertLess(large, 400 * 64 * 20)


class SDKTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_sdk_has_finite_phase_timeouts_and_no_hidden_retries(self):
        captured = []
        def handle(request):
            captured.append(request.extensions["timeout"])
            return httpx.Response(503, json={"error": {"code": 503, "message": "busy"}})
        with mock.patch("http_options.get_effective_proxy_url", return_value=""):
            options = get_http_options(proxy_url="")
        options.async_client_args = dict(options.async_client_args, transport=httpx.MockTransport(handle))
        client = genai.Client(vertexai=True, api_key="offline", http_options=options)
        try:
            with self.assertRaises(Exception):
                await client.aio.models.generate_content(model="gemini-3.6-flash", contents="hi")
        finally:
            client.close()
            await client.aio.aclose()
        self.assertEqual(captured, [{"connect": 30.0, "read": 600.0, "write": 60.0, "pool": 10.0}])

    async def test_retry_and_bare_model_fallback_keep_new_keys_proxy(self):
        for mode in ("json", "stream", "fake"):
            with self.subTest(mode=mode):
                calls, clients = [], {}
                records = [{"key": "A", "project_id": "a", "proxy": "http://proxy-a:8080"},
                           {"key": "B", "project_id": "b", "proxy": "http://proxy-b:8080"}]
                manager = SimpleNamespace(get_total_keys=lambda: 2,
                                          get_express_record=mock.Mock(side_effect=records))
                settings = dict(config.DEFAULT_SETTINGS, retry_max=1, retry_backoff_seconds=0,
                                fake_streaming=mode == "fake", fake_streaming_interval=0,
                                anti_truncation_mode="off")
                def get_client(key, proxy):
                    if (key, proxy) in clients:
                        return clients[key, proxy]
                    async def generate(**kwargs):
                        calls.append((key, proxy, kwargs["model"]))
                        if key == "A" or "/projects/" in "/" + kwargs["model"]:
                            exc = Exception("busy" if key == "A" else "requires billing to be enabled")
                            exc.code = 429 if key == "A" else 403
                            raise exc
                        return types.GenerateContentResponse(candidates=[types.Candidate(
                            content=types.Content(role="model", parts=[types.Part(text="answer")]), finish_reason="STOP")])
                    async def stream(**kwargs):
                        result = await generate(**kwargs)
                        async def iterate():
                            yield result
                        return iterate()
                    clients[key, proxy] = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
                        generate_content=generate, generate_content_stream=stream)))
                    return clients[key, proxy]
                request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)),
                                          is_disconnected=mock.AsyncMock(return_value=False))
                with mock.patch.object(express_sdk, "get_pooled_express_client", side_effect=get_client), \
                     mock.patch("proxy_failover.resolve_proxy_uri_async", new=mock.AsyncMock()) as resolve, \
                     mock.patch.object(express_sdk, "_log_resolved_endpoint"), \
                     mock.patch.object(express_sdk, "create_gemini_prompt", return_value=[]), \
                     mock.patch.object(runtime_state.app_state, "get_effective_settings", return_value=settings), \
                     mock.patch.object(runtime_state.app_state, "get_setting", side_effect=lambda key, default=None: settings.get(key, default)):
                    obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=mode != "json")
                    response = await express_sdk.ExpressSDKUpstream().chat_completions(obj, request)
                    if mode != "json":
                        body = "".join([frame async for frame in response.body_iterator])
                        self.assertIn("answer", body)
                    else:
                        self.assertEqual(response.status_code, 200)
                self.assertEqual([(key, proxy) for key, proxy, _ in calls],
                                 [("A", "http://proxy-a:8080"), ("B", "http://proxy-b:8080"), ("B", "http://proxy-b:8080")])
                self.assertEqual({call.args[0] for call in resolve.await_args_list},
                                 {"http://proxy-a:8080", "http://proxy-b:8080"})


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_seven_rotating_keys_create_seven_clients_not_one_per_request(self):
        closer = client_pool._GracefulCloser()
        pool = client_pool.SdkClientPool(maxsize=6)
        made = []
        def make(**kwargs):
            client = SimpleNamespace(close=mock.Mock(), aio=SimpleNamespace(aclose=mock.AsyncMock()))
            made.append(client)
            return client
        with mock.patch.object(client_pool, "_closer", closer), \
             mock.patch.object(client_pool, "_sdk_signature", return_value=("one-shape",)), \
             mock.patch.object(client_pool, "get_http_options", return_value=None), \
             mock.patch.object(client_pool.genai, "Client", side_effect=make):
            for _ in range(10):
                for key in range(7):
                    pool.get(str(key), "")
            self.assertEqual(len(made), 7)
            self.assertEqual(len(closer._pending), 1)
            await pool.aclose()
            await closer.aclose()
        for client in made:
            client.close.assert_called_once()
            client.aio.aclose.assert_awaited_once()

    async def test_http_pool_reclaims_retired_proxy_clients(self):
        closer = client_pool._GracefulCloser()
        pool = client_pool.HttpxClientPool(maxsize=1)
        factory = mock.Mock(side_effect=lambda: SimpleNamespace(aclose=mock.AsyncMock()))
        with mock.patch.object(client_pool, "_closer", closer), \
             mock.patch.object(client_pool, "_direct_signature", side_effect=lambda uri: (uri,)):
            first = pool.get(factory, "one")
            pool.get(factory, "two")
            self.assertIs(pool.get(factory, "one"), first)
            self.assertEqual(factory.call_count, 2)
            await pool.aclose()
            await closer.aclose()

    async def test_retirement_budget_rejects_churn_without_closing_active_clients(self):
        closer = client_pool._GracefulCloser(max_pending=1)
        pool = client_pool.HttpxClientPool(maxsize=1)
        made = []
        def factory():
            c = SimpleNamespace(aclose=mock.AsyncMock())
            made.append(c)
            return c
        with mock.patch.object(client_pool, "_closer", closer), \
             mock.patch.object(client_pool, "_direct_signature", side_effect=lambda uri: (uri,)):
            usage = proxy_resolver.ProxyUsage()
            token = proxy_resolver.current_proxy_usage.set(usage)
            self.addCleanup(proxy_resolver.current_proxy_usage.reset, token)
            self.addCleanup(usage.close)
            pool.get(factory, "one")
            pool.get(factory, "two")
            with self.assertRaises(client_pool.PoolCapacityError):
                pool.get(factory, "three")
            self.assertEqual(len(made), 2)
            for client in made:
                client.aclose.assert_not_called()
            await pool.aclose()
            await closer.aclose()


class BrowserWireTests(unittest.IsolatedAsyncioTestCase):
    async def test_browser_identity_matches_curl_and_navigation_headers_are_removed(self):
        from curl_cffi.requests import AsyncSession
        captured = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers.get("content-length", "0")))
                captured.append({k.lower(): v for k, v in self.headers.items()})
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}/"
        for profile in ("chrome", "edge", "firefox", "safari", "safari_ios"):
            with self.subTest(profile=profile):
                async with AsyncSession(impersonate=profile, trust_env=False) as baseline:
                    await baseline.post(url, json={})
                expected = captured[-1]
                with mock.patch.object(impersonate, "get_impersonate_profile", return_value=profile):
                    client = impersonate._create_upstream_client({"timeout": 10, "follow_redirects": False})
                    try:
                        await client.post(url, json={}, headers=cookie_auth.build_headers("SAPISID=offline; SID=offline"))
                    finally:
                        await client.aclose()
                actual = captured[-1]
                for name in ("user-agent", "sec-ch-ua", "sec-ch-ua-platform"):
                    self.assertEqual(actual.get(name), expected.get(name))
                self.assertNotIn("sec-fetch-user", actual)
                self.assertNotIn("upgrade-insecure-requests", actual)
                self.assertEqual(actual["sec-fetch-mode"], "cors")


class ImageTests(unittest.TestCase):
    def test_proxy_does_not_disable_private_address_checks(self):
        with mock.patch.object(image_processing, "get_effective_proxy_url", return_value="http://proxy:8080"), \
             mock.patch.object(proxy_resolver, "resolve_proxy_uri", return_value="http://proxy:8080"), \
             mock.patch.object(image_processing.httpx, "Client") as client:
            self.assertIsNone(image_processing.fetch_remote_image("http://127.0.0.1/private.png"))
        client.assert_not_called()

    def test_private_redirect_is_rejected_even_through_proxy(self):
        captured = []
        def handle(request):
            captured.append(request)
            return httpx.Response(302, headers={"location": "http://127.0.0.1/private.png"})
        client = httpx.Client(transport=httpx.MockTransport(handle))
        resolver = image_processing._public_image_addresses
        def addresses(host):
            return ["93.184.216.34"] if host == "images.example" else resolver(host)
        with mock.patch.object(image_processing, "get_effective_proxy_url", return_value="http://proxy:8080"), \
             mock.patch.object(image_processing, "_public_image_addresses", side_effect=addresses), \
             mock.patch.object(image_processing.httpx, "Client", return_value=client):
            self.assertIsNone(image_processing.fetch_remote_image("https://images.example/image.png"))
        self.assertEqual(len(captured), 1)


    def test_connection_uses_validated_ip_with_original_host_and_sni(self):
        captured = []
        def handle(request):
            captured.append(request)
            if len(captured) == 1:
                return httpx.Response(302, headers={"location": "/next.png"})
            return httpx.Response(200, content=b"image", headers={"content-type": "image/png"})
        client = httpx.Client(transport=httpx.MockTransport(handle))
        with mock.patch.object(image_processing, "get_effective_proxy_url", return_value=""), \
             mock.patch.object(image_processing, "_public_image_addresses", return_value=["93.184.216.34"]), \
             mock.patch.object(image_processing.httpx, "Client", return_value=client):
            self.assertEqual(image_processing.fetch_remote_image("https://images.example/one.png"), (b"image", "image/png"))
        self.assertEqual([r.url.host for r in captured], ["93.184.216.34"] * 2)
        self.assertEqual(captured[-1].url.path, "/next.png")
        self.assertEqual(captured[-1].headers["host"], "images.example")
        self.assertEqual(captured[-1].extensions["sni_hostname"], "images.example")

    def test_rebound_private_dns_is_rejected_before_connection(self):
        public = [(2, 1, 6, "", ("93.184.216.34", 0))]
        private = [(2, 1, 6, "", ("127.0.0.1", 0))]
        with mock.patch("socket.getaddrinfo", side_effect=[public, private]), \
             mock.patch.object(image_processing, "get_effective_proxy_url", return_value=""), \
             mock.patch.object(image_processing.httpx, "Client") as client:
            self.assertIsNone(image_processing.fetch_remote_image("https://rebound.example/image"))
        client.return_value.__enter__.return_value.stream.assert_not_called()

    def test_proxy_failure_does_not_fall_back_to_direct_image_fetch(self):
        with mock.patch.object(proxy_resolver, "resolve_proxy_uri", side_effect=proxy_resolver.ProxyResolveError("offline")), \
             mock.patch.object(image_processing.httpx, "Client") as client:
            self.assertIsNone(image_processing.fetch_remote_image("https://images.example/one.png"))
        client.assert_not_called()


class CurlBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_curl_reader_stops_before_unbounded_internal_buffer_growth(self):
        reads = []
        async def chunks():
            for _ in range(100):
                reads.append(True)
                yield b"x" * 10
        raw = SimpleNamespace(status_code=200, aiter_content=chunks)
        # aread（错误诊断体路径）：超限时抛 ValueError，读取停在限额处，
        # 幂等缓冲的硬上限语义不变。
        reads.clear()
        shim = impersonate._CurlRespShim(raw, from_stream=True)
        with mock.patch.object(impersonate, "_CURL_BUFFER_LIMIT_BYTES", 15):
            with self.assertRaisesRegex(ValueError, "读取上限"):
                await shim.aread()
        self.assertEqual(len(reads), 2)
        self.assertLessEqual(len(shim._buffered), 15)

        # aiter（正常流式路径）：超过自缓冲上限后放弃缓冲纯透传，流完整
        # 送达而非中途判死；缓冲不再随响应体无界增长（2026-10 修复：生图
        # 等大响应之前要么全额驻留内存、要么超过硬上限被中断）。
        reads.clear()
        shim = impersonate._CurlRespShim(raw, from_stream=True)
        with mock.patch.object(impersonate, "_CURL_SELF_BUFFER_LIMIT_BYTES", 15):
            received = "".join([chunk async for chunk in shim.aiter_text()])
        self.assertEqual(len(reads), 100)
        self.assertEqual(received, "x" * 1000)
        self.assertTrue(shim._dropped)
        self.assertLessEqual(len(shim._buffered), 15)



class PersistenceTests(unittest.TestCase):
    def test_failed_setting_save_returns_503_and_rolls_back_memory_and_disk(self):
        state = runtime_state.AppState()
        state.update_settings({"retry_max": 3})
        client = TestClient(main.app)
        self.addCleanup(client.close)
        client.cookies.set(console_auth.AUTH_COOKIE, console_auth.issue_session())
        with mock.patch.object(settings_api, "app_state", state), \
             mock.patch.object(runtime_state.os, "replace", side_effect=PermissionError("read-only")):
            response = client.post("/api/settings", json={"retry_max": 9})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(state.get_setting("retry_max"), 3)
        disk = json.loads(Path(runtime_state.STATE_FILE).read_text())
        self.assertEqual(disk["settings"]["retry_max"], 3)
        self.assertFalse(list(Path(_STATE.name).glob(".web_state-*.tmp")))

    def test_cookie_and_project_save_is_atomic(self):
        state = runtime_state.AppState()
        state.set_cookie_credentials("SAPISID=old; SID=old", "old-project")
        client = TestClient(main.app)
        self.addCleanup(client.close)
        client.cookies.set(console_auth.AUTH_COOKIE, console_auth.issue_session())
        with mock.patch.object(credentials_api, "app_state", state), \
             mock.patch.object(runtime_state.os, "replace", side_effect=PermissionError("read-only")):
            response = client.post("/api/cookie", json={"cookie": "SAPISID=new; SID=new", "project_id": "new-project"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(state.get_google_cookie(), "SAPISID=old; SID=old")
        self.assertEqual(state.get_project_id(), "old-project")


class RetryAndContractTests(unittest.IsolatedAsyncioTestCase):
    def test_backoff_is_fixed_with_jitter_and_honors_retry_after(self):
        with mock.patch.object(retry_policy.random, "uniform", side_effect=lambda low, high: (low + high) / 2):
            # 固定间隔 + 抖动：默认 5s ± 0.5s，不随 attempt 指数增长
            self.assertEqual([retry_policy.retry_delay(None, 5, n) for n in range(3)], [5.0, 5.0, 5.0])
            self.assertEqual(retry_policy.retry_delay(None, 5, 20), 5.0)
            # 抖动幅度可配置：0 → 精确 5s；2 → uniform(4, 6) 中点 5s
            self.assertEqual(retry_policy.retry_delay(None, 5, 0, 0), 5.0)
            self.assertEqual(retry_policy.retry_delay(None, 5, 0, 2), 5.0)
            # 更短的上游提示被采纳；更长的提示钳位到窗口上限，且不触发随机
            with mock.patch.object(retry_policy.random, "uniform") as uniform:
                self.assertEqual(retry_policy.retry_delay("2.5", 5, 10), 2.5)
                self.assertEqual(retry_policy.retry_delay("60", 5), 5.5)
                uniform.assert_not_called()
            # backoff=0 → 立即重试，不叠加抖动
            self.assertEqual(retry_policy.retry_delay(None, 0, 3), 0.0)
            self.assertEqual(retry_policy.retry_delay("2", 0), 0.0)

    async def test_cookie_rejects_unverified_structured_output_before_network_call(self):
        for response_format in ({"type": "json_object"}, {"type": "json_schema", "json_schema": {"schema": {"type": "object"}}}):
            obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], response_format=response_format)
            with mock.patch.object(cookie_proxy, "open_upstream_client") as network:
                response = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, SimpleNamespace())
            self.assertEqual(response.status_code, 400)
            network.assert_not_called()


if __name__ == "__main__":
    unittest.main()
