"""Received HTTP errors remain authoritative even when their diagnostic body is cut."""
import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import httpx

from http_io import BoundedAsyncClient
from main import app
from upstreams import cookie_delivery as delivery, cookie_native as native, cookie_transport as transport

MODEL = "gemini-3.5-flash-lite"
RECORD = {"id": "fixture", "cookie": "SAPISID=fixture", "project_id": "fixture"}


class BrokenBody(httpx.AsyncByteStream):
    def __init__(self, *, wait=False):
        self.reading = asyncio.Event()
        self.closes = 0
        self.wait = wait

    async def __aiter__(self):
        yield b'{"partial":"unverified'
        self.reading.set()
        if self.wait:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if self.wait != "translated":
                    raise
        raise httpx.RemoteProtocolError("test fixture cut diagnostic body")

    async def aclose(self):
        self.closes += 1


class CookieDiagnosticStatus(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_httpx_post_uses_headers_not_incomplete_error_body(self):
        for code in (400, 422, 429):
            with self.subTest(code=code):
                body = BrokenBody()
                respond = Mock(return_value=httpx.Response(code, headers={"retry-after": "45"}, stream=body))
                async with BoundedAsyncClient(transport=httpx.MockTransport(respond)) as client:
                    res = await client.post("https://fixture.invalid/generate")
                self.assertEqual((res.status_code, res.headers.get("retry-after")), (code, "45"))
                self.assertTrue(0 < len(res.content) < 200)
                self.assertNotIn(b"partial", res.content)
                self.assertEqual(body.closes, 1)
                respond.assert_called_once()

    async def test_true_stream_preserves_classification_and_valid_advisory(self):
        for code, hint, expected in ((400, "45", "45"), (422, "45", "45"), (429, "45", "45"),
                                     (429, "nonsense", None)):
            with self.subTest(code=code, hint=hint):
                body = BrokenBody()
                respond = Mock(return_value=httpx.Response(code, headers={"retry-after": hint}, stream=body))
                async with BoundedAsyncClient(transport=httpx.MockTransport(respond)) as client:
                    events = [item async for item in transport.stream_events(client, {}, {})]
                (status, error), = events
                self.assertEqual(status, "retryable_error" if code == 429 else "fatal_error")
                self.assertEqual((error.status_code, error.retry_after), (code, expected))
                self.assertNotIn("partial", error)
                self.assertEqual(body.closes, 1)
                respond.assert_called_once()

    async def test_cancel_while_reading_error_is_not_a_synthetic_error_response(self):
        for mode, wait in (("post", True), ("stream", True), ("post", "translated"), ("stream", "translated")):
            with self.subTest(mode=mode, wait=wait):
                body = BrokenBody(wait=wait)
                respond = Mock(return_value=httpx.Response(429, headers={"retry-after": "45"}, stream=body))
                async with BoundedAsyncClient(transport=httpx.MockTransport(respond)) as client:
                    async def run():
                        if mode == "post":
                            return await client.post("https://fixture.invalid")
                        return [event async for event in transport.stream_events(client, {}, {})]
                    task = asyncio.create_task(run())
                    await asyncio.wait_for(body.reading.wait(), 1)
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 1)
                self.assertEqual(body.closes, 1)
                respond.assert_called_once()

    async def test_incomplete_200_still_fails(self):
        async with BoundedAsyncClient(transport=httpx.MockTransport(
                lambda _: httpx.Response(200, stream=BrokenBody()))) as client:
            with self.assertRaises(httpx.RemoteProtocolError):
                await client.post("https://fixture.invalid")
            (kind, error), = [item async for item in transport.stream_events(client, {}, {})]
            self.assertIn(kind, ("retryable_error", "fatal_error"))
            self.assertEqual(error.status_code, 502)

    async def test_actual_curl_whole_and_stream_read_a_cut_local_response(self):
        # Real curl_cffi may report its own IncompleteRead, not an HTTPX subclass.
        from impersonate import _CurlClientShim
        from curl_transport import CancellationSafeSession
        received = []

        async def serve(reader, writer):
            try:
                header = await reader.readuntil(b"\r\n\r\n")
                received.append(header.splitlines()[0])
                prefix = b"" if b"/empty " in received[-1] else b"partial"
                writer.write(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 45\r\n"
                             b"Content-Length: 100\r\nConnection: close\r\n\r\n" + prefix)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        async with await asyncio.start_server(serve, "127.0.0.1", 0) as server:
            url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
            async with CancellationSafeSession(trust_env=False) as session:
                client = _CurlClientShim(session, 2, 2)
                for request_url in (url + "/partial", url + "/empty"):
                    with self.subTest(path=request_url.rsplit("/", 1)[-1]):
                        res = await client.post(request_url)
                        self.assertEqual((res.status_code, res.headers["retry-after"]), (429, "45"))
                        self.assertNotIn(b"partial", res.content)
                        with patch.object(transport, "BATCH_GRAPHQL_URL", request_url):
                            (kind, error), = [item async for item in transport.stream_events(client, {}, {})]
                        self.assertEqual((kind, error.status_code, error.retry_after),
                                         ("retryable_error", 429, "45"))
        self.assertEqual(len(received), 4)


class CookieDiagnosticRoutes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from upstreams import cookie_proxy
        import channel_dispatch
        self.client = await self.enterAsyncContext(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test",
            headers={"x-goog-api-key": "test-key", "authorization": "Bearer test-key", "x-resumable": "0"}))
        self.enterContext(patch.object(app.state, "express_key_manager", SimpleNamespace(get_total_keys=lambda: 0)))
        self.enterContext(patch.object(native.app_state, "get_cookie_accounts",
                                       return_value=[dict(RECORD, enabled=True)]))
        self.enterContext(patch.object(native.app_state, "get_channel_mode", return_value="web_proxy"))
        self.enterContext(patch.object(native.app_state, "is_web_proxy_enabled", return_value=True))
        for module in (native, cookie_proxy):
            self.enterContext(patch.object(module, "get_retry_settings", return_value=(1, 0, 0)))
        # The upstream's 45s hint exceeds the route's budget. Do not replay it
        # early or replace the completed HTTP rejection with a deadline 504.
        self.enterContext(patch.object(channel_dispatch, "request_timeout_seconds", return_value=2))
        self.enterContext(patch.object(native, "request_timeout_seconds", return_value=2))
        base = native.app_state.get_setting
        self.fake = False
        self.enterContext(patch.object(native.app_state, "get_setting", side_effect=lambda key, default=None:
            {"fake_streaming": self.fake, "cookie_impersonate": "off"}.get(key, base(key, default))))
        self.status, self.hint = 400, "45"
        self.bodies, self.recover = [], False
        def respond(_):
            if self.recover and self.bodies:
                self.bodies.append(None)
                return httpx.Response(200, json=[{"results": [{"data": {"candidates": [{
                    "content": {"parts": [{"text": "ok"}]}, "finishReason": "STOP"}]}}]}])
            body = BrokenBody()
            self.bodies.append(body)
            return httpx.Response(self.status, headers={"retry-after": self.hint}, stream=body)
        upstream = await self.enterAsyncContext(BoundedAsyncClient(transport=httpx.MockTransport(respond)))
        @asynccontextmanager
        async def opened(_kwargs):
            yield upstream
        for module in (native, delivery):
            self.enterContext(patch.object(module, "open_upstream_client", opened))
        self.wait = self.enterContext(patch.object(transport, "wait_for_retry",
                                                   side_effect=AssertionError("must not retry early")))

    def assert_one(self):
        counts = [body.closes for body in self.bodies]
        self.bodies.clear()
        self.assertEqual(counts, [1])
        self.wait.assert_not_called()

    async def test_native_json_and_committed_array_sse_keep_error_and_advisory(self):
        for status in (400, 422, 429):
            self.status = status
            for action in ("generateContent", "streamGenerateContent?alt=sse", "streamGenerateContent"):
                with self.subTest(status=status, action=action):
                    result = await self.client.post(f"/v1beta/models/{MODEL}:{action}", json={"contents": []})
                    if action == "generateContent":
                        self.assertEqual((result.status_code, result.headers.get("retry-after")), (status, "45"))
                        payload = result.json()
                    else:
                        self.assertEqual(result.status_code, 200)
                        events = ([json.loads(line[6:]) for line in result.text.splitlines() if line.startswith("data: ")]
                                  if "sse" in action else result.json())
                        payload, = events
                        detail, = payload["error"]["details"]
                        self.assertEqual(detail["metadata"]["retryAfter"], "45")
                    self.assertEqual(payload["error"]["code"], status)
                    self.assertNotIn("partial", result.text)
                    self.assert_one()

    async def test_openai_json_true_and_buffered_sse_do_not_replace_rejection(self):
        for status in (400, 422, 429):
            self.status = status
            for mode in ("json", "sse", "buffered", "image"):
                with self.subTest(status=status, mode=mode):
                    self.fake = mode == "buffered"
                    result = await self.client.post("/v1/chat/completions", json={
                        "model": "gemini-3-pro-image-preview" if mode == "image" else MODEL,
                        "messages": [{"role": "user", "content": "Hello"}], "stream": mode != "json"})
                    if mode == "json":
                        self.assertEqual((result.status_code, result.headers.get("retry-after")), (status, "45"))
                        error = result.json()["error"]
                    else:
                        self.assertEqual(result.status_code, 200)
                        events = [json.loads(line[6:]) for line in result.text.splitlines()
                                  if line.startswith("data: ") and "[DONE]" not in line]
                        error, = [item["error"] for item in events if "error" in item]
                        self.assertEqual(error.get("retry_after"), "45")
                    self.assertEqual(error["code"], status)
                    self.assertNotIn("partial", result.text)
                    self.assert_one()


    async def test_default_openai_json_keeps_the_http_advisory_on_initial_and_retained_errors(self):
        from resumable import ResumableStore
        from routes import chat_api
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(chat_api, "resumable_store", store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.client.headers.pop("x-resumable")
        path, body = "/v1/chat/completions", {"model": MODEL, "messages": [{"role": "user", "content": "Hello"}]}
        for status in (400, 422, 429):
            with self.subTest(status=status):
                self.status = status
                key = {"Idempotency-Key": f"error-{status}"}
                first = await self.client.post(path, json=body, headers=key)
                rid = first.headers["x-resume-id"]
                for response in (first, await self.client.post(path, json=body, headers=key),
                                 await self.client.post(path, json=body, headers={"X-Resume-Id": rid})):
                    self.assertEqual((response.status_code, response.headers.get("retry-after")), (status, "45"))
                    self.assertEqual(response.headers["x-resume-id"], rid)
                    self.assertEqual(response.json(), first.json())
                    self.assertEqual(response.json()["error"]["code"], status)
                self.assert_one()  # same physical response, closed exactly once

    async def test_retryable_status_with_no_positive_wait_can_recover(self):
        self.recover = True
        for status, hint in ((503, "0"), (429, "not-an-advisory")):
            with self.subTest(status=status, hint=hint):
                self.status, self.hint = status, hint
                self.bodies.clear()
                result = await self.client.post(f"/v1beta/models/{MODEL}:generateContent", json={"contents": []})
                self.assertEqual(result.status_code, 200)
                self.assertEqual(result.json()["candidates"][0]["content"]["parts"], [{"text": "ok"}])
                self.assertEqual(len(self.bodies), 2)
                self.assertEqual(self.bodies[0].closes, 1)
