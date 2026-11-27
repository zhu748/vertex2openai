"""Admin-only exports of unmodified failed request entities."""
import asyncio
import json
from unittest.mock import patch

import httpx

import config
import console_auth
from failed_request_bodies import FailedRequestBodies, MAX_DOWNLOADS, SETTING
from main import app
from request_tracking import PayloadObserver, RequestTrace, request_registry
from resumable import ResumableStore
from runtime_state import AppState, app_state
from test_native_sdk_sse_keepalive import GatedBody, NativeSDKHarness
from upstreams import native_passthrough


NATIVE = "/v1beta/models/gemini-4-hydrogen:streamGenerateContent?alt=sse"
BODY = (' { "contents" : [{"role":"model","parts":[{"functionCall":{"name":"compress",'
        '"args":{"text":"测试"}}, "thoughtSignature" : "non-live-fixture"}]}] }\n').encode()
ERROR = {"error": {"code": 400, "status": "INVALID_ARGUMENT", "message": "offline rejection"}}
MIME = {"content-type": "text/event-stream"}
SEPARATE = "offline-console-strong-password"


def event(obj):
    return f"data: {json.dumps(obj)}\r\n\r\n".encode()


class TestFailedBodyRoute(NativeSDKHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings["request_diagnostics"] = False
        self.enterContext(patch.object(config, "ADMIN_PASSWORD", SEPARATE))
        self.store = FailedRequestBodies()
        self.enterContext(patch.object(app_state, "failed_request_bodies", self.store))
        self.client.cookies.set(console_auth.AUTH_COOKIE, console_auth.issue_session())

    async def set_capture(self, enabled):
        with patch.object(app_state, "get_settings", lambda **kw: AppState.get_settings(app_state, **kw)):
            return await self.client.post("/api/settings", json={SETTING: enabled})

    async def request(self, path=NATIVE, body=BODY, *, on_start=None, authenticated=True):
        async def chunks():
            for index in range(0, len(body), 11):
                yield body[index:index + 11]

        async with httpx.AsyncClient(base_url="http://local",
                transport=self.transport(lambda _: None, on_start=on_start),
                headers={"x-goog-api-key": "test-key" if authenticated else "wrong-key",
                         "Authorization": "Bearer " + ("test-key" if authenticated else "wrong-key")}) as client:
            return await client.post(path, content=chunks(), headers={"content-type": "application/json"})

    async def check_export(self, reply, available, body=BODY):
        base = "/api/requests/" + reply.headers["x-trace-id"]
        detail = (await self.client.get(base)).json()
        self.assertIs(detail["raw_body_available"], available)
        self.assertNotIn("non-live-fixture", json.dumps(detail))
        export = await self.client.get(base + "/raw-body")
        if available:
            self.assertEqual(export.status_code, 200)
            self.assertEqual(export.content, body)
            self.assertEqual(export.headers["content-type"], "application/octet-stream")
            self.assertTrue(export.headers["content-disposition"].startswith("attachment; "))
            self.assertEqual(export.headers["cache-control"], "no-store")
            self.assertEqual(export.headers["x-content-type-options"], "nosniff")
        else:
            self.assertEqual(export.status_code, 404)
        return base

    async def test_raw_200_then_sse_error_export_and_auth(self):
        self.assertEqual((await self.set_capture(True)).status_code, 200)
        release = asyncio.Event()

        def provider(*_):
            return httpx.Response(400, headers=MIME, stream=GatedBody(release, event(ERROR)))

        async with self.provider(provider, retries=(2, 0, 0)) as calls:
            reply = await self.request(on_start=lambda _: release.set())
            self.assertEqual(reply.status_code, 200)
            self.assertIn(b"offline rejection", reply.content)
            self.assertEqual([r.content for r in calls], [BODY])
        base = await self.check_export(reply, True)
        async with httpx.AsyncClient(transport=self.transport(lambda _: None), base_url="http://local",
                                     headers={"Authorization": "Bearer test-key"}) as anonymous:
            reply = await anonymous.get(base + "/raw-body")
        self.assertEqual(reply.status_code, 401)
        blocked = await self.client.get(base + "/raw-body", headers={"sec-fetch-site": "cross-site"})
        self.assertEqual(blocked.status_code, 403)
        for _ in range(MAX_DOWNLOADS):
            self.assertTrue(self.store.download_slots.acquire(blocking=False))
        try:
            self.assertEqual((await self.client.get(base + "/raw-body")).status_code, 503)
        finally:
            for _ in range(MAX_DOWNLOADS):
                self.store.download_slots.release()
        self.assertEqual((await self.client.get(base + "/raw-body")).content, BODY)
        self.assertEqual((await self.set_capture(False)).status_code, 200)
        self.assertEqual((await self.client.get(base + "/raw-body")).status_code, 404)

    async def test_detached_native_error_without_reader_still_exports(self):
        self.settings.update(resumable_enabled=True, resumable_disconnect_grace_seconds=5)
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(native_passthrough, "resumable_store", store))
        self.assertEqual((await self.set_capture(True)).status_code, 200)
        disconnect, release = asyncio.Event(), asyncio.Event()
        sent = {}
        first = {"candidates": [{"content": {"role": "model", "parts": [{"text": "offline-first"}]}}]}
        request_sent = False

        async def receive():
            nonlocal request_sent
            if not request_sent:
                request_sent = True
                return {"type": "http.request", "body": BODY, "more_body": False}
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.start":
                sent.update(status=message["status"], headers=dict(message["headers"]))
            elif b"offline-first" in message.get("body", b""):
                disconnect.set()

        path, query = NATIVE.split("?")
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "scheme": "http", "method": "POST", "path": path,
                 "raw_path": path.encode(), "query_string": query.encode(), "root_path": "",
                 "headers": [(b"host", b"local"), (b"content-type", b"application/json"),
                             (b"x-goog-api-key", b"test-key"), (b"x-resumable", b"true"),
                             (b"idempotency-key", b"debug-export-disconnect")],
                 "client": ("127.0.0.1", 12345), "server": ("local", 80)}
        try:
            async with self.provider(lambda *_: httpx.Response(
                    200, headers=MIME, stream=GatedBody(event(first), release, event(ERROR)))) as calls:
                async with asyncio.timeout(4):
                    await app(scope, receive, send)
                    trace_id = sent["headers"][b"x-trace-id"].decode()
                    trace = request_registry.get(trace_id)
                    self.assertTrue(trace.snapshot()["generation_active"])
                    release.set()
                    while trace.ended is None:
                        await asyncio.sleep(0.01)
                self.assertEqual((sent["status"], len(calls), trace.snapshot()["outcome"]), (200, 1, "error"))
                self.assertEqual((await self.client.get(f"/api/requests/{trace_id}/raw-body")).content, BODY)
        finally:
            release.set()
            await self.set_capture(False)

    async def test_default_disabled_success_unauth_and_exact_preparse_bytes(self):
        def provider(*_):
            return httpx.Response(400, json=ERROR)

        async with self.provider(provider):
            disabled = await self.request()
        self.assertEqual(disabled.status_code, 400)
        await self.check_export(disabled, False)
        self.assertEqual((await self.set_capture(True)).status_code, 200)

        def success(*_):
            return httpx.Response(200, json={"candidates": [{"content": {
                "role": "model", "parts": [{"text": "ok"}]}, "finishReason": "STOP"}]})

        path = NATIVE.replace(":streamGenerateContent?alt=sse", ":generateContent")
        async with self.provider(success) as calls:
            ok = await self.request(path)
            unauthorized = await self.request(path, authenticated=False)
            self.assertEqual(len(calls), 1)
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(unauthorized.status_code, 401)
        await self.check_export(ok, False)
        await self.check_export(unauthorized, False)

        invalid = b' { "model" : "offline", bad JSON } \n'
        response = await self.request("/v1/chat/completions", invalid)
        self.assertGreaterEqual(response.status_code, 400)
        await self.check_export(response, True, invalid)
        self.assertEqual((await self.set_capture(False)).status_code, 200)

    async def test_export_precedes_native_alias_rewrite(self):
        self.assertEqual((await self.set_capture(True)).status_code, 200)
        path = NATIVE.replace("hydrogen:streamGenerateContent?alt=sse", "hydrogen-search:generateContent")
        with patch("model_catalog.search_variant_available", return_value=True):
            async with self.provider(lambda *_: httpx.Response(400, json=ERROR)) as calls:
                failure = await self.request(path)
        self.assertEqual(failure.status_code, 400)
        self.assertEqual(len(calls), 1)
        self.assertNotEqual(calls[0].content, BODY)
        self.assertEqual(json.loads(calls[0].content)["tools"], [{"google_search": {}}])
        await self.check_export(failure, True)
        self.assertEqual((await self.set_capture(False)).status_code, 200)

    async def test_fallback_console_login_can_enable_and_download(self):
        self.client.cookies.clear()
        with patch.object(config, "ADMIN_PASSWORD", None):
            # The generation Bearer alone is not a console session.
            self.assertEqual((await self.set_capture(True)).status_code, 401)
            login = await self.client.post("/api/login", json={"password": config.API_KEY})
            self.assertEqual(login.status_code, 200)
            self.assertIn(console_auth.AUTH_COOKIE, self.client.cookies)
            self.assertEqual((await self.set_capture("yes")).status_code, 400)
            self.assertFalse(self.store.begin("not-enabled", "0"))
            self.assertEqual((await self.set_capture(True)).status_code, 200)
            try:
                async with self.provider(lambda *_: httpx.Response(400, json=ERROR)):
                    failure = await self.request()
                base = await self.check_export(failure, True)
                self.client.cookies.clear()
                self.assertEqual((await self.client.get(base + "/raw-body")).status_code, 401)
            finally:
                await self.client.post("/api/login", json={"password": config.API_KEY})
                await self.set_capture(False)


def test_bounds_and_incomplete_capture():
    now = 0.
    store = FailedRequestBodies(max_body=6, max_total=8, max_entries=4, ttl=2, clock=lambda: now)
    assert not store.begin("disabled", "")
    store.set_enabled(True)
    assert not store.begin("declared-large", "7")
    assert store.begin("ok-to-start", "")
    store.feed("ok-to-start", b"too big", final=True)
    store.finish("ok-to-start", failed=True)
    assert store.get("ok-to-start") is None

    for key, payload, final, failed in (("older", b"123456", True, True), ("newer", b"1234", True, True),
                                         ("partial", b"12", False, True), ("ok", b"1234", True, False)):
        assert store.begin(key, "")
        store.feed(key, payload, final=final)
        assert not store.contains(key)
        store.finish(key, failed=failed)
    assert store.get("older") is None
    assert store.get("newer") == b"1234"
    assert store.get("partial") is None
    assert store.get("ok") is None
    now = 2.
    assert store.get("newer") is None
    assert store._used == 0

    assert store.begin("stale", "")
    store.feed("stale", b"12", final=True)
    store.set_enabled(False)
    store.set_enabled(True)
    store.finish("stale", failed=True)
    assert store.get("stale") is None


def test_capture_only_sse_error_does_not_echo_private_diagnostic_text():
    trace = RequestTrace("/v1/responses")
    observer = PayloadObserver(trace, sent=True, inspect=False, inspect_errors=True)
    observer.feed(event({"type": "response.failed", "response": {
        "error": {"message": "non-live-fixture-should-not-enter-trace", "code": 400}}}))
    snapshot = trace.snapshot(detail=True)
    assert snapshot["outcome"] == "error"
    assert snapshot["error_code"] == 400
    assert "non-live-fixture-should-not-enter-trace" not in json.dumps(snapshot)


def test_detached_generation_final_outcome_controls_export():
    store = FailedRequestBodies()
    store.set_enabled(True)
    trace = RequestTrace("/v1/responses")
    assert store.begin(trace.id, "")
    trace.capture_failed_body(store)
    store.feed(trace.id, b"offline request", final=True)
    trace.start_generation()
    trace.end_connection(disconnected=True)
    assert not store.contains(trace.id)
    trace.end_generation("error")
    assert store.get(trace.id) == b"offline request"


def test_client_cancelled_status_is_not_exported_as_failure():
    store = FailedRequestBodies()
    store.set_enabled(True)
    trace = RequestTrace("/v1/responses")
    assert store.begin(trace.id, "")
    trace.capture_failed_body(store)
    store.feed(trace.id, b"offline request", final=True)
    trace.fail("client cancelled", 499)
    trace.end_connection()
    assert store.get(trace.id) is None
