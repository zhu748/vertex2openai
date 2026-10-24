"""Request boundaries, bounded auxiliary traffic and cancellation cleanup."""

import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-optimizations-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.responses import StreamingResponse

import config
import console_auth
import express_key_probe
import keepalive
import lifecycle
import main
import middleware
import model_loader
import streaming
from http_io import read_limited_bytes
from request_errors import declared_body_exceeds
from request_tracking import redact
from settings_validation import validate_global_settings_patch
from upstream_errors import create_openai_error_response, extract_upstream_error


class RequestBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(main.app), base_url="http://test")
        self.addAsyncCleanup(self.client.aclose)

    async def test_declared_oversize_keeps_each_client_protocol_and_cors(self):
        for path, protocol in (("/v1/chat/completions", "openai"),
                               ("/v1/messages", "claude"),
                               ("/v1/messages/count_tokens", "claude"),
                               ("/v1/models/test:generateContent", "gemini"),
                               ("/v1beta/models/test:generateContent", "gemini")):
            with self.subTest(path=path), mock.patch.object(middleware, "MAX_CHAT_BODY_BYTES", 128):
                response = await self.client.post(path, content=b"{}", headers={
                    "Content-Length": "129", "Origin": "https://client.example"})
                self.assertEqual(response.status_code, 413)
                self.assertEqual(response.headers["access-control-allow-origin"], "*")
                self.assertEqual(response.headers["cache-control"], "no-store")
                self.assert_protocol(response.json(), protocol)

    def assert_protocol(self, body, protocol):
        if protocol == "claude":
            self.assertEqual(body["type"], "error")
            self.assertEqual(body["error"]["type"], "invalid_request_error")
            self.assertIn("request_id", body)
        elif protocol == "gemini":
            self.assertEqual(body["error"]["status"], "RESOURCE_EXHAUSTED")
            self.assertEqual(body["error"]["code"], 413)
        else:
            self.assertEqual(body["error"]["type"], "invalid_request_error")

    async def test_chunked_limits_survive_json_parser_exception_handlers(self):
        async def chunks():
            yield b'{"padding":"'
            yield b"x" * 256
            yield b'"}'

        paths = (("/v1/chat/completions", "openai"), ("/v1/messages", "claude"),
                 ("/v1/messages/count_tokens", "claude"),
                 ("/v1beta/models/test:generateContent", "gemini"),
                 ("/api/settings", "openai"), ("/api/model-overrides/test", "openai"),
                 ("/api/express-keys", "openai"), ("/api/login", "openai"))
        with mock.patch.dict(main.app.dependency_overrides, {console_auth.require_auth: lambda: True}), \
             mock.patch.multiple(middleware, MAX_CHAT_BODY_BYTES=128, MANAGEMENT_BODY_BYTES=128, LOGIN_BODY_BYTES=128):
            for path, protocol in paths:
                with self.subTest(path=path):
                    response = await self.client.post(path, content=chunks(), headers={
                        "content-type": "application/json", "authorization": f"Bearer {config.API_KEY}"})
                    self.assertEqual(response.status_code, 413, response.text)
                    self.assert_protocol(response.json(), protocol)

    async def test_management_rejects_declared_oversize_before_auth_or_parsing(self):
        with mock.patch.object(middleware, "MANAGEMENT_BODY_BYTES", 128):
            response = await self.client.post("/api/cookie-accounts", content=b"invalid JSON",
                                              headers={"content-length": "129"})
        self.assertEqual(response.status_code, 413)

    async def test_extreme_content_length_never_causes_integer_conversion_error(self):
        response = await self.client.post("/v1/chat/completions", content=b"{}",
                                          headers={"content-length": "9" * 5000})
        self.assertEqual(response.status_code, 413)
        response = await self.client.post("/v1/chat/completions", content=b"{}",
                                          headers=[(b"content-length", b"\xb2")])
        self.assertLess(response.status_code, 500)

    async def test_non_finite_input_returns_a_serializable_validation_error(self):
        for number in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(number=number):
                response = await self.client.post("/v1/chat/completions", content=(
                    '{"model":"test","messages":[],"temperature":' + number + '}'), headers={
                        "content-type": "application/json", "authorization": f"Bearer {config.API_KEY}"})
                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json()["error"]["type"], "invalid_request_error")
                self.assertEqual(response.json()["detail"][0]["loc"], ["body", "temperature"])

    async def test_validation_errors_do_not_echo_passwords_or_whole_request_bodies(self):
        for path, payload in (("/api/login", {"password": {"secret": "private-login-value"}}),
                              ("/v1/chat/completions", {"messages": [], "secret": "private-login-value"})):
            with self.subTest(path=path):
                response = await self.client.post(path, json=payload, headers={
                    "authorization": f"Bearer {config.API_KEY}"})
                self.assertEqual(response.status_code, 422)
                self.assertNotIn("private-login-value", response.text)
                self.assertNotIn("input", response.json()["detail"][0])

    def test_content_length_comparison_handles_leading_zeroes(self):
        self.assertFalse(declared_body_exceeds("0" * 5000 + "128", 128))
        self.assertTrue(declared_body_exceeds("0" * 5000 + "129", 128))
        for invalid in ("", "²", "-1", "1.5", "NaN"):
            self.assertFalse(declared_body_exceeds(invalid, 128))


class TrackedStream(httpx.AsyncByteStream):
    def __init__(self, chunks=(), *, wait=False, forbid_read=False):
        self.chunks = chunks
        self.wait = wait
        self.forbid_read = forbid_read
        self.reads = 0
        self.closed = asyncio.Event()

    async def __aiter__(self):
        if self.forbid_read:
            raise AssertionError("response body must not be read")
        for chunk in self.chunks:
            self.reads += 1
            yield chunk
        if self.wait:
            await asyncio.Event().wait()

    async def aclose(self):
        self.closed.set()


class AuxiliaryIOTests(unittest.IsolatedAsyncioTestCase):
    async def test_response_budget_stops_chunked_read_and_closes_connection(self):
        source = TrackedStream([b"x" * 32] * 100)
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=source))) as client:
            async with client.stream("GET", "https://test.example") as response:
                with self.assertRaises(ValueError):
                    await read_limited_bytes(response, 100)
        self.assertLessEqual(source.reads, 4)
        self.assertTrue(source.closed.is_set())

    async def test_declared_large_response_is_rejected_without_reading_body(self):
        source = TrackedStream(forbid_read=True)
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
                200, headers={"content-length": "99999999999999999999999"}, stream=source))) as client:
            async with client.stream("GET", "https://test.example") as response:
                with self.assertRaises(ValueError):
                    await read_limited_bytes(response, 100)
        self.assertTrue(source.closed.is_set())

    async def test_model_refresh_preserves_cache_on_oversize_or_endless_body(self):
        original_client = httpx.AsyncClient
        for source in (TrackedStream([b"x" * 64] * 10), TrackedStream(wait=True)):
            with self.subTest(endless=source.wait):
                def client(**kwargs):
                    return original_client(**kwargs, transport=httpx.MockTransport(
                        lambda request: httpx.Response(200, stream=source)))
                with mock.patch.object(model_loader.app_config, "MODELS_CONFIG_URL", "https://catalog.example/models"), \
                     mock.patch.object(model_loader.httpx, "AsyncClient", side_effect=client), \
                     mock.patch.multiple(model_loader, MAX_MODELS_CONFIG_BYTES=128,
                                         MODELS_FETCH_TIMEOUT_SECONDS=.03, _next_refresh_allowed=0,
                                         _model_cache={"models": ["previous"]}), \
                     mock.patch("proxy_failover.resolve_proxy_uri_async", mock.AsyncMock(return_value="")):
                    self.assertFalse(await model_loader.refresh_models_config_cache())
                    self.assertEqual(await model_loader.get_models_config(), {"models": ["previous"]})
                self.assertTrue(source.closed.is_set())

    async def test_valid_model_response_still_loads_and_normalizes(self):
        original_client = httpx.AsyncClient
        source = TrackedStream([b'{"models":[" a ","a","b"]}'])
        def client(**kwargs):
            return original_client(**kwargs, transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=source)))
        with mock.patch.object(model_loader.app_config, "MODELS_CONFIG_URL", "https://catalog.example/models"), \
             mock.patch.object(model_loader.httpx, "AsyncClient", side_effect=client), \
             mock.patch("proxy_failover.resolve_proxy_uri_async", mock.AsyncMock(return_value="")):
            self.assertEqual(await model_loader.fetch_and_parse_models_config(), {"models": ["a", "b"]})
        self.assertTrue(source.closed.is_set())

    async def test_keepalive_follows_redirects_without_reading_any_body(self):
        original_client = httpx.AsyncClient
        sources = [TrackedStream(forbid_read=True), TrackedStream(forbid_read=True)]
        urls, options = [], []
        def handle(request):
            urls.append(str(request.url))
            if len(urls) == 1:
                return httpx.Response(302, headers={"location": "/healthz"}, stream=sources[0])
            return httpx.Response(200, stream=sources[1])
        def client(**kwargs):
            options.append(kwargs)
            return original_client(**kwargs, transport=httpx.MockTransport(handle))
        service = keepalive.KeepAliveService()
        with mock.patch.object(keepalive.httpx, "AsyncClient", side_effect=client):
            task = asyncio.create_task(service._run("https://health.example/start", 3600))
            try:
                await asyncio.wait_for(sources[1].closed.wait(), 1)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(urls, ["https://health.example/start", "https://health.example/healthz"])
        self.assertTrue(all(source.closed.is_set() for source in sources))
        self.assertFalse(options[0]["trust_env"])


class ProbeConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_oversize_probe_response_is_closed_and_reported_as_failure(self):
        original_client = httpx.AsyncClient
        source = TrackedStream([b"x" * 32] * 100)
        def client(**kwargs):
            return original_client(**kwargs, transport=httpx.MockTransport(
                lambda request: httpx.Response(404, stream=source)))
        with mock.patch.object(express_key_probe.httpx, "AsyncClient", side_effect=client), \
             mock.patch.object(express_key_probe, "MAX_PROBE_RESPONSE_BYTES", 100), \
             mock.patch("proxy_failover.resolve_proxy_uri_async", mock.AsyncMock(return_value="")):
            project, message = await express_key_probe.probe_project_id("offline-key")
        self.assertIsNone(project)
        self.assertIn("ValueError", message)
        self.assertTrue(source.closed.is_set())
        self.assertLessEqual(source.reads, 4)

    async def test_large_batch_bounds_workers_preserves_order_and_isolates_errors(self):
        active = maximum = task_count = 0
        async def probe(key, **kwargs):
            nonlocal active, maximum, task_count
            active += 1
            maximum = max(maximum, active)
            task_count = max(task_count, len(asyncio.all_tasks()))
            try:
                await asyncio.sleep(.001)
                if key == "key-9":
                    raise ValueError("offline failure")
                return {"key": key, "ok": True}
            finally:
                active -= 1
        keys = [f"key-{index}" for index in range(100)]
        with mock.patch.object(express_key_probe, "probe_and_store", side_effect=probe):
            results = await express_key_probe.probe_all(keys=keys)
        self.assertEqual([row["key"] for row in results], keys)
        self.assertLessEqual(maximum, express_key_probe.MAX_CONCURRENT_PROBES)
        self.assertLess(task_count, 15)
        self.assertFalse(results[9]["ok"])
        self.assertEqual(sum(row["ok"] for row in results), 99)

    async def test_simultaneous_single_probes_share_network_limit(self):
        original_client = httpx.AsyncClient
        active = maximum = 0
        async def handle(request):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            try:
                await asyncio.sleep(.005)
                return httpx.Response(404, text="projects/offline-project/locations/global/models/test not found")
            finally:
                active -= 1
        def client(**kwargs):
            return original_client(**kwargs, transport=httpx.MockTransport(handle))
        with mock.patch.object(express_key_probe.httpx, "AsyncClient", side_effect=client), \
             mock.patch.object(express_key_probe, "_probe_semaphore", asyncio.Semaphore(4)), \
             mock.patch("proxy_failover.resolve_proxy_uri_async", mock.AsyncMock(return_value="")):
            results = await asyncio.gather(*(express_key_probe.probe_project_id(f"key-{i}") for i in range(12)))
        self.assertTrue(all(project == "offline-project" for project, _ in results))
        self.assertLessEqual(maximum, 4)


class KeepAliveConfigTests(unittest.IsolatedAsyncioTestCase):
    async def test_redirect_loop_is_bounded_without_downloading_redirect_bodies(self):
        original_client = httpx.AsyncClient
        sources = [TrackedStream(forbid_read=True) for _ in range(3)]
        requests = []
        def handle(request):
            source = sources[len(requests)]
            requests.append(request)
            return httpx.Response(302, headers={"location": "/loop"}, stream=source)
        def client(**kwargs):
            return original_client(**kwargs, transport=httpx.MockTransport(handle))
        service = keepalive.KeepAliveService()
        with mock.patch.object(keepalive.httpx, "AsyncClient", side_effect=client), \
             mock.patch.object(keepalive, "_MAX_REDIRECTS", 2):
            task = asyncio.create_task(service._run("https://test.example/loop", 3600))
            try:
                await asyncio.wait_for(sources[-1].closed.wait(), 1)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(len(requests), 3)
        self.assertTrue(all(source.closed.is_set() for source in sources))

    def test_malformed_http_urls_are_rejected_on_save(self):
        for url in ("https://", "http:///healthz", "https://host:wrong", "https://host:99999", "https://host/a\nb"):
            with self.subTest(url=url):
                self.assertIsNotNone(validate_global_settings_patch({"keepalive_url": url}))

    async def test_invalid_legacy_interval_does_not_break_startup(self):
        service = keepalive.KeepAliveService()
        with mock.patch.object(keepalive.app_state, "get_setting", side_effect=lambda key, default=None: {
                "keepalive_url": "https://test.example", "keepalive_interval": "broken"}.get(key, default)):
            await service.start()
        self.assertFalse(service.is_running)

    async def test_concurrent_restarts_never_leave_duplicate_tasks(self):
        service = keepalive.KeepAliveService()
        active = maximum = 0
        async def run(*args):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(.001)
                active -= 1
        with mock.patch.object(service, "_current_config", return_value=("https://test.example", 60)), \
             mock.patch.object(service, "_run", side_effect=run):
            await service.start()
            await asyncio.sleep(0)
            try:
                await asyncio.gather(*(service.restart() for _ in range(10)))
                await asyncio.sleep(0)
                self.assertTrue(service.is_running)
                self.assertEqual(maximum, 1)
            finally:
                await service.stop()
        self.assertEqual(active, 0)


class CleanupAndRedactionTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_resistant_setup_has_bounded_wait_and_late_result_is_closed(self):
        cancelled, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        response = StreamingResponse(iter([b"unused"]))
        response.aclose = mock.AsyncMock(side_effect=closed.set)
        async def setup():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
                return response
        task = asyncio.create_task(setup())
        await asyncio.sleep(0)
        try:
            with mock.patch.object(streaming, "SETUP_CLOSE_TIMEOUT_SECONDS", .02):
                await asyncio.wait_for(streaming.close_setup_task(task), .5)
            self.assertTrue(cancelled.is_set())
            self.assertFalse(task.done())
            release.set()
            await asyncio.wait_for(closed.wait(), 1)
            response.aclose.assert_awaited_once()
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.gather(*list(lifecycle._background_tasks), return_exceptions=True)

    async def test_finished_setup_exception_is_retrieved(self):
        async def setup():
            raise ValueError("expected failure")
        task = asyncio.create_task(setup())
        await asyncio.sleep(0)
        await streaming.close_setup_task(task)

    def test_upstream_errors_hide_all_proxy_schemes_and_full_bearer_tokens(self):
        message = "turns://user:secret@proxy.example sstp://user:password@vpn.example Bearer 密钥+/= https://upstream/?access_token=hidden"
        for rendered in (redact(message), json.dumps(create_openai_error_response(502, message, "upstream_error")),
                         extract_upstream_error(httpx.ConnectError(message))[1]):
            for secret in ("user:secret", "user:password", "密钥", "+/=", "access_token=hidden"):
                self.assertNotIn(secret, rendered)
        self.assertEqual(redact(redact(message)), redact(message))


if __name__ == "__main__":
    unittest.main()
