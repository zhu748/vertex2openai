"""Regression checks for configuration changes and late failure cleanup."""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-final-review-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.responses import Response

import client_pool
import admission
import config
import express_key_probe
import model_loader
import resumable
import proxy_resolver
from models import OpenAIRequest
from routes import chat_api, gemini_native
from runtime_state import AppState


class BufferLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)

    def test_negative_last_event_id_is_invalid_before_incrementing(self):
        for value in ("-1", "rid-example:-1"):
            with self.subTest(value=value), self.assertRaises(resumable.ResumeRequestError) as caught:
                resumable.resume_cursor(SimpleNamespace(headers={"last-event-id": value}, query_params={}))
            self.assertEqual(caught.exception.code, 400)

    async def test_lowering_capacity_does_not_cancel_running_generations(self):
        for key in ("one", "two"):
            await self.store.get_or_create_request(key, model="m", kind="stream", fingerprint_key=key)
        with mock.patch.object(resumable, "max_streams", return_value=1):
            await self.store.sweep()
            self.assertEqual(len(self.store._buffers), 2)
            self.assertTrue(all(not buf.done for buf in self.store._buffers.values()))
            with self.assertRaises(resumable.BufferCapacityError):
                await self.store.get_or_create_request("three", model="m", kind="stream", fingerprint_key="three")

    async def test_deadline_is_fixed_for_admitted_generation(self):
        with mock.patch.object(resumable, "request_timeout_seconds", return_value=10):
            buf = await self.store.create("deadline")
        with mock.patch.object(resumable, "request_timeout_seconds", return_value=0.001):
            await resumable.within_request_deadline(buf, lambda: asyncio.sleep(0.01))
            await self.store.sweep()
        self.assertFalse(buf.done)

    async def test_unexpected_background_failure_wakes_readers_with_error(self):
        buf = await self.store.create("exception")
        async def fail():
            raise RuntimeError("background operation failed")
        with self.assertRaises(RuntimeError):
            await resumable.within_request_deadline(buf, fail)
        self.assertTrue(buf.done)
        self.assertGreaterEqual(buf.status_code, 500)
        self.assertIn("background operation failed", "".join(buf.events))

    async def test_shutdown_awaits_cleanup_of_already_discarded_pump(self):
        buf = await self.store.create("discarded")
        started, closed = asyncio.Event(), asyncio.Event()
        async def pump():
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0.01)
                closed.set()
        task = asyncio.create_task(pump())
        self.store.register_pump(buf.id, task, buffer=buf)
        await started.wait()
        await self.store.drop(buf.id)
        await self.store.aclose()
        self.assertTrue(closed.is_set())
        self.assertTrue(task.done())

    async def test_json_wait_uses_request_budget_instead_of_fixed_600_seconds(self):
        waits = []
        original = resumable.ResumableBuffer.wait_for
        async def wait(buf, seq, timeout):
            waits.append(timeout)
            await original(buf, seq, 1)
        with mock.patch.object(chat_api, "resumable_store", self.store), \
             mock.patch.object(resumable.ResumableBuffer, "wait_for", wait), \
             mock.patch.object(chat_api, "_call_upstream", mock.AsyncMock(return_value=Response(
                 content=b'{"choices":[]}', media_type="application/json"))):
            result = await chat_api._resumable_json(SimpleNamespace(headers={}),
                OpenAIRequest(model="m", messages=[]), "key")
        self.assertEqual(result.status_code, 200)
        self.assertGreater(waits[0], 600)

    async def test_invalid_upstream_json_is_reported_as_502(self):
        with mock.patch.object(chat_api, "resumable_store", self.store), \
             mock.patch.object(chat_api, "_call_upstream", mock.AsyncMock(return_value=Response(
                 content=b'{"truncated":', media_type="application/json"))):
            result = await chat_api._resumable_json(SimpleNamespace(headers={}),
                OpenAIRequest(model="m", messages=[]), "key")
        self.assertEqual(result.status_code, 502)
        self.assertIn("error", json.loads(result.body))


class NativeFailuresTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_or_malformed_sse_is_not_counted_as_success(self):
        for body in (b": keep-alive\n\n", b'data: {"candidates":[null]}\n\n'):
            upstream = httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})
            result = await gemini_native._relay_stream(upstream, keepalive=True)
            frames = b"".join([frame async for frame in result.body_iterator])
            errors = [json.loads(line[5:])["error"] for line in frames.splitlines()
                      if line.startswith(b"data:") and b'"error"' in line]
            self.assertEqual(errors[0]["code"], 502)

    async def test_pool_capacity_failure_uses_google_error_shape_and_503(self):
        request = SimpleNamespace(headers={}, query_params=httpx.QueryParams(),
                                  body=mock.AsyncMock(return_value=b'{"contents":[]}'))
        with mock.patch.object(gemini_native, "_passthrough_request", mock.AsyncMock(
                side_effect=client_pool.PoolCapacityError("pool full"))):
            result = await gemini_native._passthrough_endpoint()("m:generateContent", request, config.API_KEY)
        self.assertEqual(result.status_code, 503)
        self.assertEqual(json.loads(result.body)["error"]["code"], 503)


class ConfigurationTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_model_source_does_not_fall_back_to_another_owners_catalog(self):
        urls, options = [], []
        original = httpx.AsyncClient
        def handle(request):
            urls.append(str(request.url))
            return (httpx.Response(503) if request.url.host == "private.example" else
                    httpx.Response(200, json={"models": ["foreign-model"]}))
        def client(**kwargs):
            options.append(kwargs)
            return original(**dict(kwargs, transport=httpx.MockTransport(handle)))
        with mock.patch.object(model_loader.app_config, "MODELS_CONFIG_URL", "https://private.example/models.json"), \
             mock.patch("proxy_resolver.resolve_proxy_uri_async", mock.AsyncMock(return_value="")), \
             mock.patch.object(model_loader.httpx, "AsyncClient", side_effect=client):
            result = await model_loader.fetch_and_parse_models_config()
        self.assertIsNone(result)
        self.assertEqual(urls, ["https://private.example/models.json"])
        self.assertIs(options[0]["trust_env"], False)

    async def test_late_probe_cannot_overwrite_a_manual_project_edit(self):
        state = AppState()
        state._state = {"express_keys": [{"key": "offline-key"}]}
        async def probe(key):
            state.update_express_key_project(key, "manually-selected", source="manual")
            return "stale-auto-result", "success"
        with mock.patch.object(state, "_save"), \
             mock.patch.object(express_key_probe, "app_state", state), \
             mock.patch.object(express_key_probe, "probe_project_id", side_effect=probe):
            result = await express_key_probe.probe_and_store("offline-key")
        self.assertEqual(state.get_express_keys()[0]["project_id"], "manually-selected")
        self.assertEqual(result["project_id"], "manually-selected")


class GenerationCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_unstarted_sse_closes_delayed_preparation_and_releases_its_slot(self):
        limiter = admission.GenerationLimiter()
        started, closed = asyncio.Event(), asyncio.Event()
        async def prepare(*args):
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                closed.set()
        with mock.patch.object(admission, "generation_limiter", limiter), \
             mock.patch.object(chat_api.app_state, "is_web_proxy_enabled", return_value=False), \
             mock.patch.object(chat_api.express_upstream, "chat_completions", side_effect=prepare):
            response = await chat_api._call_upstream(OpenAIRequest(model="m", messages=[], stream=True),
                SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False)))
            self.assertTrue(started.is_set())
            self.assertEqual(limiter.active, 1)
            await response.aclose()
        self.assertTrue(closed.is_set())
        self.assertEqual(limiter.active, 0)

    async def test_active_proxy_is_not_reaped_until_the_generation_closes(self):
        manager = proxy_resolver.SingboxManager()
        uris = ("vless://a@one.example:443", "vless://b@two.example:443")
        for uri in uris:
            manager._instances[uri] = SimpleNamespace(alive=lambda: True, last_used=0, port=1234)
        @admission.limit_generation
        async def call():
            async def source():
                for uri in uris:
                    await asyncio.to_thread(manager.get_local_socks5, uri)
                    yield "data\n\n"
            return admission.ManagedStreamingResponse(source())
        with mock.patch.object(proxy_resolver, "_idle_minutes", return_value=0.001), \
             mock.patch.object(manager, "_terminate") as terminate:
            response = await call()
            await anext(response.body_iterator)
            await anext(response.body_iterator)
            for instance in manager._instances.values():
                instance.last_used = time.time() - 100
            self.assertEqual(manager.reap_idle(), 0)
            terminate.assert_not_called()
            await response.aclose()
            self.assertFalse(manager._active_uses)
            for instance in manager._instances.values():
                instance.last_used = time.time() - 100
            self.assertEqual(manager.reap_idle(), 2)
            self.assertEqual(terminate.call_count, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
