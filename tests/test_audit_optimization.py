"""Offline regressions for the 2ace593 audit: lifecycle, schemas and editor conflicts."""
import asyncio
import copy
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

_STATE = tempfile.TemporaryDirectory(prefix="v2o-optimization-")
os.environ["STATE_DIR"] = _STATE.name
os.environ["API_KEY"] = "optimization-test"
os.environ["MODELS_CONFIG_URL"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.responses import StreamingResponse
from admission import GenerationCapacityError, generation_limiter, limit_generation
from console_auth import AUTH_COOKIE, issue_session
from credential_manager import credential_manager
from main import app
import media_tasks
from routes import claude_api
from runtime_state import app_state
from schema_validation import SchemaValidationError, sanitize_for_gemini
from upstreams import cookie_native as native
from upstreams.cookie_request import _cookie_ui_schema
from upstreams.cookie_response import extract_events

MODEL = "gemini-3.5-flash-lite"
BODY = {"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}
RECORD = {"id": "account", "cookie": "SAPISID=test; SID=test", "project_id": "project", "enabled": True}
PARTS = [{"text": "answer", "thoughtSignature": "c2ln"},
         {"inlineData": {"mimeType": "image/png", "data": "eA=="}, "thoughtSignature": "aW1n"},
         {"thoughtSignature": "b25seQ=="}]
RAW = {"results": [{"data": {"candidates": [{"content": {"parts": PARTS}, "finishReason": "STOP"}]}}]}


@asynccontextmanager
async def fake_upstream(*args, **kwargs):
    yield object()


async def fake_events(*args, preserve_parts=False, **kwargs):
    for event in extract_events(RAW, preserve_parts=preserve_parts):
        yield event


class NativeLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        app_state.set_channel_mode("web_proxy")
        app_state.set_express_keys([])
        app_state.set_cookie_accounts([RECORD])
        app_state.update_settings({"upstream_max_concurrency": 1})
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                       headers={"x-goog-api-key": "optimization-test"})
        self.patches = [patch.object(native, "open_upstream_client", fake_upstream),
                        patch.object(native, "stream_events", fake_events)]
        for item in self.patches:
            item.start()

    async def asyncTearDown(self):
        for item in reversed(self.patches):
            item.stop()
        await self.client.aclose()
        self.assertEqual(generation_limiter.active, 0, "generation lease leaked")

    async def post(self, action, query=""):
        return await self.client.post(f"/v1beta/models/{MODEL}:{action}{query}", json=BODY)

    async def test_capacity_rejects_native_cookie_requests(self):
        lease = generation_limiter.reserve()
        try:
            response = await self.post("generateContent")
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()["error"]["status"], "RESOURCE_EXHAUSTED")
        finally:
            lease.release()

    async def test_signatures_preserved_in_json_and_sse_and_array_stream(self):
        normal = await self.post("generateContent")
        self.assertEqual(normal.json()["candidates"][0]["content"]["parts"], PARTS)
        for query in ["", "?alt=json", "?alt=sse"]:
            with self.subTest(query=query):
                response = await self.post("streamGenerateContent", query)
                self.assertEqual(response.status_code, 200)
                if query == "?alt=sse":
                    self.assertIn("text/event-stream", response.headers["content-type"])
                    chunks = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]
                else:
                    self.assertIn("application/json", response.headers["content-type"])
                    chunks = response.json()
                parts = [part for chunk in chunks for candidate in chunk.get("candidates", [])
                         for part in candidate.get("content", {}).get("parts", [])]
                self.assertEqual(parts, PARTS)
                self.assertEqual(chunks[-1]["candidates"][0]["finishReason"], "STOP")

    async def test_deadline_covers_active_stream_and_closes_upstream(self):
        for action, query in [("generateContent", ""), ("streamGenerateContent", ""),
                              ("streamGenerateContent", "?alt=sse")]:
            closed = asyncio.Event()

            async def endless(*args, **kwargs):
                try:
                    while True:
                        yield "native_part", {"text": "x"}
                        await asyncio.sleep(.002)
                finally:
                    closed.set()

            with patch.object(native, "_events_with_retry", endless), \
                 patch.object(native, "request_timeout_seconds", return_value=.025):
                response = await asyncio.wait_for(self.post(action, query), timeout=1)
            self.assertTrue(closed.is_set())
            self.assertEqual(generation_limiter.active, 0)
            if action == "generateContent":
                self.assertEqual(response.status_code, 504)
            else:
                self.assertIn('"code": 504', response.text)
                if not query:
                    self.assertEqual(response.json()[-1]["error"]["code"], 504)

    async def test_resume_rejected_before_new_upstream_generation(self):
        with patch.object(native, "_events_with_retry") as upstream:
            response = await self.post("streamGenerateContent", "?alt=sse&resume_from=old:1")
            self.assertEqual(response.status_code, 400)
            upstream.assert_not_called()

    async def test_stream_close_releases_active_upstream_and_generation_slot(self):
        closed = asyncio.Event()

        async def source(*args, **kwargs):
            try:
                yield "native_part", {"text": "first"}
                await asyncio.Event().wait()
            finally:
                closed.set()

        request = SimpleNamespace(headers={}, query_params={"alt": "sse"},
                                  is_disconnected=AsyncMock(return_value=False))
        with patch.object(native, "_events_with_retry", source):
            response = await native.handle_native_request(request, MODEL, "streamGenerateContent", BODY)
            self.assertEqual(generation_limiter.active, 1)
            await anext(response.body_iterator)  # initial keep-alive
            await anext(response.body_iterator)  # first model part
            await asyncio.wait_for(response.aclose(), 1)
        self.assertTrue(closed.is_set())
        self.assertEqual(generation_limiter.active, 0)

    async def test_silent_upstream_gets_heartbeats_without_cancelling_read(self):
        closed = asyncio.Event()

        async def silent(*args, **kwargs):
            try:
                await asyncio.Event().wait()
                yield "finish", "STOP"
            finally:
                closed.set()

        heartbeat = native.with_heartbeats
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        with patch.object(native, "_events_with_retry", silent), \
             patch.object(native, "with_heartbeats", side_effect=lambda source, request: heartbeat(source, request, .005)):
            events = native._timed_events(request, RECORD, {}, MODEL, time.monotonic() + 1)
            self.assertEqual(await asyncio.wait_for(anext(events), .5), ("heartbeat", None))
            self.assertFalse(closed.is_set())
            await events.aclose()
        self.assertTrue(closed.is_set())

    async def test_old_auth_failures_cannot_quarantine_replaced_cookie(self):
        old = app_state.get_cookie_accounts()[0]
        app_state.set_cookie_accounts([{**RECORD, "cookie": "SAPISID=new; SID=new"}])

        async def expired(*args, **kwargs):
            yield "cookie_error", native.NativeBridgeError(401, "expired")

        with patch.object(native, "stream_events", expired):
            for _ in range(3):
                with self.assertRaises(native.NativeBridgeError):
                    async for _event in native._events_with_retry(
                            SimpleNamespace(is_disconnected=AsyncMock(return_value=False)), old, {}, MODEL):
                        pass
        health = credential_manager.cookie_health_snapshot().get("account", {})
        self.assertEqual(health.get("failures", 0), 0)
        self.assertEqual(health.get("cooldown_remaining", 0), 0)

    async def test_hello_head_has_get_headers_without_body(self):
        get = await self.client.get("/api/hello")
        head = await self.client.head("/api/hello")
        self.assertEqual(head.status_code, 200)
        self.assertEqual(head.headers["content-length"], get.headers["content-length"])
        self.assertEqual(head.content, b"")

    async def test_nonstream_response_size_is_bounded(self):
        with patch.object(native, "MAX_RESPONSE_BYTES", 5):
            response = await self.post("generateContent")
        self.assertEqual(response.status_code, 502)

    def test_stream_assembler_does_not_retain_part_history(self):
        assembler = native.NativeAssembler(MODEL, retain_parts=False)
        for _ in range(10_000):
            assembler.feed("native_part", {"text": "x" * 100})
        assembler.feed("finish", "STOP")
        self.assertEqual(assembler.parts, [])
        self.assertEqual(assembler.part_count, 10_000)
        self.assertEqual(assembler.terminal()["candidates"][0]["finishReason"], "STOP")


class CancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_claude_full_queue_disconnect_releases_upstream_and_slot(self):
        closed, full = asyncio.Event(), asyncio.Event()

        async def source():
            try:
                for i in range(1000):
                    if i == 128:
                        full.set()
                    yield 'data: {"choices":[{"delta":{"content":"x"}}]}\n\n'
            finally:
                closed.set()

        @limit_generation
        async def upstream():
            return StreamingResponse(source(), media_type="text/event-stream")

        response = claude_api._claude_stream_response(await upstream(), MODEL, omit_thinking=False)
        await anext(response.body_iterator)
        await asyncio.wait_for(full.wait(), 1)
        await asyncio.wait_for(response.aclose(), 1)
        self.assertTrue(closed.is_set())
        self.assertEqual(generation_limiter.active, 0)

    async def test_count_tokens_has_timeout_and_propagates_cancellation(self):
        started, closed = asyncio.Event(), asyncio.Event()

        async def blocked(*args):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

        with patch.object(claude_api, "_count_exact_input_tokens", blocked), \
             patch.object(claude_api, "COUNT_TOKENS_TIMEOUT_SECONDS", .02):
            self.assertIsNone(await claude_api._express_exact_input_tokens(None, None, MODEL))
            self.assertTrue(closed.is_set())
            started.clear()
            task = asyncio.create_task(claude_api._express_exact_input_tokens(None, None, MODEL))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_count_tokens_saturation_falls_back_without_waiting(self):
        slots = threading.BoundedSemaphore(1)
        slots.acquire()
        with patch.object(claude_api, "_count_tokens_slots", slots), \
             patch.object(claude_api, "_count_exact_input_tokens", AsyncMock()) as upstream:
            self.assertIsNone(await claude_api._express_exact_input_tokens(None, None, MODEL))
            upstream.assert_not_called()
        slots.release()

    async def test_cancelled_media_waiter_retains_capacity_until_worker_exits(self):
        started, release, finished = threading.Event(), threading.Event(), threading.Event()

        def work():
            started.set()
            release.wait(2)
            try:
                media_tasks.check_media_budget()
            finally:
                finished.set()

        with patch.object(media_tasks, "_slots", threading.BoundedSemaphore(1)):
            task = asyncio.create_task(media_tasks.run_media_task(work))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                with self.assertRaises(GenerationCapacityError):
                    await media_tasks.run_media_task(lambda: None)
            finally:
                release.set()
                self.assertTrue(await asyncio.to_thread(finished.wait, 1))
                await asyncio.sleep(.01)
            self.assertEqual(await media_tasks.run_media_task(lambda: 42), 42)

    def test_media_budget_expiry_is_shared_across_operations(self):
        token = media_tasks._budget.set((time.monotonic() - 1, threading.Event()))
        try:
            with self.assertRaises(TimeoutError):
                media_tasks.check_media_budget()
        finally:
            media_tasks._budget.reset(token)


class SchemaTests(unittest.TestCase):
    def test_type_union_retains_all_branches_and_constraints(self):
        schema = {"type": ["string", "integer", "null"], "minimum": 1}
        result = sanitize_for_gemini(schema)
        self.assertEqual([part["type"] for part in result["anyOf"]], ["string", "integer", "null"])
        self.assertTrue(all(part["minimum"] == 1 for part in result["anyOf"]))
        self.assertEqual(schema["type"], ["string", "integer", "null"])

    def test_local_refs_resolve_for_both_adapters_and_escape_json_pointers(self):
        schema = {"type": "object", "properties": {"value": {"$ref": "#/$defs/a~1b~0c"}},
                  "$defs": {"a/b~c": {"type": "integer", "minimum": 1}}}
        self.assertEqual(sanitize_for_gemini(schema)["properties"]["value"], {"type": "integer", "minimum": 1})
        self.assertEqual(_cookie_ui_schema(schema)["properties"][0]["value"], {"type": "INTEGER", "minimum": 1})

    def test_unsupported_intersection_and_recursive_refs_fail_explicitly(self):
        for schema in [{"allOf": [{"type": "integer"}, {"minimum": 10}]},
                       {"$ref": "#/$defs/self", "$defs": {"self": {"$ref": "#/$defs/self"}}},
                       {"$ref": "https://example.invalid/schema"}]:
            with self.subTest(schema=schema), self.assertRaises(SchemaValidationError):
                sanitize_for_gemini(schema)

    def test_reference_and_union_expansion_are_bounded(self):
        large = {"type": "object", "properties": {str(i): {"$ref": "#/$defs/large"} for i in range(20)},
                 "$defs": {"large": {"type": "string", "description": "x" * 100_000}}}
        with self.assertRaises(SchemaValidationError):
            sanitize_for_gemini(large)
        named = {"anyOf": [{"$ref": "#/$defs/large"}] * 5,
                 "$defs": {"large": {"type": "object", "properties": {"x" * 300_000: {"type": "string"}}}}}
        with self.assertRaises(SchemaValidationError):
            sanitize_for_gemini(named)
        nested = {"type": "string"}
        for _ in range(15):
            nested = {"type": ["object", "array"], "properties": {"value": nested}}
        with self.assertRaises(SchemaValidationError):
            sanitize_for_gemini(nested)


class ConsoleConflictTests(unittest.IsolatedAsyncioTestCase):
    async def test_simultaneous_saves_from_one_revision_have_exactly_one_winner(self):
        app_state.set_express_keys([{"key": "test-key", "project_id": "project", "enabled": True}])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                    cookies={AUTH_COOKIE: issue_session()}) as client:
            snapshot = (await client.get("/api/express-keys")).json()
            responses = await asyncio.gather(*[
                client.post("/api/express-keys", json={"keys": [{**snapshot["keys"][0], "note": note}],
                                                      "revision": snapshot["revision"]})
                for note in ("first writer", "second writer")])
            self.assertEqual(sorted(response.status_code for response in responses), [200, 409])
            winner = next(response.json()["keys"][0]["note"] for response in responses if response.status_code == 200)
            self.assertEqual(app_state.get_express_keys()[0]["note"], winner)

    async def test_cookie_rotation_preserves_editor_revision_but_manual_replacement_invalidates_it(self):
        app_state.set_cookie_accounts([RECORD])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                    cookies={AUTH_COOKIE: issue_session()}) as client:
            original = (await client.get("/api/cookie-accounts")).json()
            rotated = "SAPISID=test; SID=test; SIDCC=rotated"
            self.assertTrue(app_state.update_cookie_account_cookie("account", rotated,
                                                                  expected_cookie=RECORD["cookie"]))
            current = (await client.get("/api/cookie-accounts")).json()
            self.assertEqual(original["revision"], current["revision"])
            draft = original["accounts"]
            draft[0]["note"] = "metadata after rotation"
            response = await client.post("/api/cookie-accounts", json={"accounts": draft,
                                                                      "revision": original["revision"]})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(app_state.get_cookie_accounts()[0]["cookie"], rotated)
            app_state.set_cookie_accounts([{**RECORD, "cookie": "SAPISID=replaced; SID=new"}])
            stale = await client.post("/api/cookie-accounts", json={"accounts": response.json()["accounts"],
                                                                  "revision": response.json()["revision"]})
            self.assertEqual(stale.status_code, 409)

    async def test_stale_full_table_saves_return_409_without_reenabling_accounts(self):
        app_state.set_express_keys([{"key": "test-key", "project_id": "project", "enabled": True}])
        app_state.set_cookie_accounts([RECORD])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                    cookies={AUTH_COOKIE: issue_session()}) as client:
            for endpoint, field in [("express-keys", "keys"), ("cookie-accounts", "accounts")]:
                with self.subTest(endpoint=endpoint):
                    snapshot = (await client.get(f"/api/{endpoint}")).json()
                    first, second = copy.deepcopy(snapshot[field]), copy.deepcopy(snapshot[field])
                    first[0]["enabled"] = False
                    a = await client.post(f"/api/{endpoint}", json={field: first, "revision": snapshot["revision"]})
                    self.assertEqual(a.status_code, 200)
                    second[0]["note"] = "stale tab note"
                    b = await client.post(f"/api/{endpoint}", json={field: second, "revision": snapshot["revision"]})
                    self.assertEqual(b.status_code, 409)
                    current = (await client.get(f"/api/{endpoint}")).json()
                    self.assertFalse(current[field][0]["enabled"])
                    self.assertNotEqual(current[field][0].get("note"), "stale tab note")

    async def test_unsupported_schema_returns_400_before_upstream(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                    headers={"Authorization": "Bearer optimization-test"}) as client:
            response = await client.post("/v1/chat/completions", json={"model": MODEL,
                "messages": [{"role": "user", "content": "hello"}],
                "tools": [{"type": "function", "function": {"name": "f", "parameters": {"allOf": []}}}]})
            self.assertEqual(response.status_code, 400)
            self.assertIn("allOf", response.text)


if __name__ == "__main__":
    unittest.main()
