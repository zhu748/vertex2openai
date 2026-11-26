"""Native/Studio HTTP attempts cannot retry an already cancelled generation."""
import asyncio
import json
from unittest.mock import Mock, patch

import httpx

from admission import generation_limiter
from http_io import BoundedAsyncClient
import impersonate
from proxy_failover import ProxyChain
from test_parallel_tool_controls import _request
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from upstreams import cookie_delivery, cookie_native, cookie_proxy, native_passthrough


OUTLETS = ("http://primary.invalid:8080", "http://standby.invalid:8081")
ANSWER = {"candidates": [{"content": {"role": "model", "parts": [{"text": "fixture answer"}]},
                          "finishReason": "STOP"}]}


class AttemptBody(httpx.AsyncByteStream):
    """The adapter can supply one last outcome instead of propagating CancelledError."""
    def __init__(self, *, cookie, mode):
        self.cookie = cookie
        self.mode = mode
        self.reading, self.closing, self.release = (asyncio.Event() for _ in range(3))
        self.reads = self.closes = 0
        self.closed = False

    async def __aiter__(self):
        self.reads += 1
        self.reading.set()
        if self.mode in ("late200", "late503"):
            yield b"{}"  # a cancelled header acquisition must never start this read
            return
        if self.mode == "ordinary503":
            yield b'{"error":{"code":503,"message":"temporarily unavailable"}}'
            return
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if self.mode in ("readerror", "429-error"):
                raise httpx.ReadError("cancelled read") from None
            if self.mode == "429-body":
                yield b'{"error":{"code":429,"message":"temporarily unavailable"}}'
                return
            if self.mode in ("tool400", "tool200"):
                error = {"code": 400, "message": "FunctionDeclaration not supported"}
                payload = {"error": error} if self.mode == "tool400" else {"results": [{"errors": [error]}]}
                yield json.dumps(payload).encode() + b"\n"
                return
            if self.mode == "eof":
                return
            if self.mode != "metadata":
                raise
        usage = {"usageMetadata": {"promptTokenCount": 1}}
        if self.cookie:
            yield json.dumps({"results": [{"data": usage}]}).encode() + b"\n"
        else:
            yield b"data: " + json.dumps(usage).encode() + b"\n\n"
        self.reads += 1
        # If a cancelled private read requested another chunk, record it
        # without hanging the test.
        yield b""

    async def aclose(self):
        self.closes += 1
        self.closing.set()
        await self.release.wait()
        self.closed = True


class HttpAttemptCancellationTests(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings.update(upstream_max_concurrency=1, retry_max=0, first_reply_timeout_seconds=0)
        self.calls, self.chains = [], []

        def chain(_record):
            result = ProxyChain(candidates=OUTLETS)
            self.chains.append(result)
            return result

        # Real route/credential selection, limiter, failover and decoding.
        # Substitute only the two HTTP clients with an in-process transport.
        mock = httpx.MockTransport(self.serve)
        direct = await self.enterAsyncContext(httpx.AsyncClient(transport=mock))
        studio = await self.enterAsyncContext(BoundedAsyncClient(transport=mock))
        self.enterContext(patch.object(native_passthrough.passthrough_client_pool, "get",
                                      side_effect=lambda *_: direct))
        self.enterContext(patch.object(impersonate._upstream_client_pool, "get", return_value=studio))
        for module in (cookie_native, cookie_delivery):
            self.enterContext(patch.object(module, "open_upstream_client", impersonate.open_upstream_client))
        for module in (native_passthrough, cookie_native, cookie_proxy):
            self.enterContext(patch.object(module, "ProxyChain", side_effect=chain))

    async def serve(self, request):
        self.calls.append(request)
        cookie = self.channel == "web_proxy"
        if len(self.calls) > 1:
            if cookie:
                return httpx.Response(200, json={"results": [{"data": ANSWER}]})
            if ":streamGenerateContent" in request.url.path:
                return httpx.Response(200, text="data: " + json.dumps(ANSWER) + "\n\n",
                                      headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=ANSWER)
        self.started.set()
        if self.phase == "headers":
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if self.mode == "readerror":
                    raise httpx.ReadError("cancelled headers") from None
                if self.mode.startswith("late"):
                    return httpx.Response(int(self.mode[4:]), stream=self.body,
                                          headers={"content-type": "application/json"})
                return httpx.Response(503, json={"error": {"code": 503}})
        status = {"ordinary503": 503, "tool400": 400, "429-body": 429, "429-error": 429}.get(self.mode, 200)
        return httpx.Response(status, stream=self.body,
                              headers={"content-type": "application/json" if cookie or not self.stream
                                       else "text/event-stream"})

    def start(self, route, stream, phase, mode, trigger):
        self.calls.clear()
        self.chains.clear()
        self.channel = "api_key" if route == "direct" else "web_proxy"
        self.stream, self.phase, self.mode = stream, phase, mode
        self.settings["request_timeout_seconds"] = .3 if trigger == "deadline" else 20
        self.body = AttemptBody(cookie=self.channel == "web_proxy", mode=mode)
        self.started = asyncio.Event()
        if route == "chat":
            query = b""
            path, payload = _request("openai", "auto" if mode.startswith("tool") else None,
                                     stream=stream, tools=mode.startswith("tool"))
        else:
            action = "streamGenerateContent" if stream else "generateContent"
            path, query = f"/v1beta/models/{MODEL}:{action}", b"alt=sse" if stream else b""
            payload = {"contents": [{"parts": [{"text": "hello"}]}]}
        peer = Peer(path, query=query, payload=payload, headers=[(b"x-resumable", b"false")])
        return peer, asyncio.create_task(peer.run())

    async def abandoned(self, route, stream, phase, mode, trigger):
        peer, sender = self.start(route, stream, phase, mode, trigger)
        try:
            await asyncio.wait_for((self.started if phase == "headers" else self.body.reading).wait(), 2)
            if trigger == "server":
                sender.cancel()
            elif trigger == "peer":
                await peer.drop()
            if phase == "body" or mode.startswith("late"):
                await asyncio.wait_for(self.body.closing.wait(), 2)
                expected_reads = int(phase == "body")
                self.assertEqual((self.body.reads, self.body.closes, generation_limiter.active,
                                  len(self.calls), sender.done()), (expected_reads, 1, 1, 1, False))
                self.body.release.set()
            if trigger == "server":
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(asyncio.shield(sender), 2)
            else:
                await asyncio.wait_for(asyncio.shield(sender), 2)
            wire = b"".join(m.get("body", b"") for m in peer.messages)
            self.assertEqual((len(self.calls), self.chains[0].index, generation_limiter.active), (1, 0, 0))
            self.assertEqual(self.body.closed, phase == "body" or mode.startswith("late"))
            self.assertNotIn(b"fixture answer", wire)
            if trigger == "deadline":
                self.assertIn(b"504", wire)
        finally:
            self.body.release.set()
            if not sender.done():
                sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    async def test_abandoned_http_headers_cannot_select_the_standby(self):
        for route in ("direct", "chat", "native-cookie"):
            for stream, mode, trigger in ((False, "readerror", "peer"), (True, "status", "server"),
                                         (False, "status", "deadline")):
                with self.subTest(route=route, stream=stream, mode=mode, trigger=trigger):
                    await self.abandoned(route, stream, "headers", mode, trigger)

    async def test_owned_response_returned_after_header_cancel_is_closed_without_reading(self):
        for route in ("direct", "chat", "native-cookie"):
            for stream, mode in ((False, "late200"), (False, "late503"), (True, "late200")):
                with self.subTest(route=route, stream=stream, mode=mode):
                    await self.abandoned(route, stream, "headers", mode, "server")

    async def test_cancelled_body_error_eof_or_metadata_cannot_be_used_for_a_retry(self):
        for route in ("direct", "chat", "native-cookie"):
            for stream, mode, trigger in ((False, "readerror", "server"), (True, "readerror", "deadline"),
                                         (True, "eof", "server"), (True, "metadata", "peer"),
                                         (True, "metadata", "server")):
                with self.subTest(route=route, stream=stream, mode=mode, trigger=trigger):
                    await self.abandoned(route, stream, "body", mode, trigger)

    async def test_cancelled_diagnostics_cannot_unlock_key_or_tool_degradation_retries(self):
        self.settings["retry_max"] = 1
        for route, stream, mode, trigger in (
                ("direct", False, "429-body", "server"), ("direct", True, "429-error", "deadline"),
                ("chat", True, "tool400", "server"), ("chat", True, "tool200", "server"),
                ("chat", False, "tool200", "server")):
            with self.subTest(route=route, stream=stream, mode=mode, trigger=trigger):
                await self.abandoned(route, stream, "body", mode, trigger)

    async def test_curl_whole_post_disposes_a_late_header_without_reading_under_repeated_cancel(self):
        # The curl shim also buffers a whole POST internally. Keep this at
        # its actual session boundary without starting a network connection.
        started, closing, release = (asyncio.Event() for _ in range(3))

        class LateResponse:
            status_code = 200
            headers = {}

            def __init__(self):
                self.reads = self.closes = 0

            async def aiter_content(self):
                self.reads += 1
                yield b"{}"

            async def aclose(self):
                self.closes += 1
                closing.set()
                await release.wait()

        raw = LateResponse()
        async def request(*_args, **_kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return raw

        session = Mock(request=request)
        client = impersonate._CurlClientShim(session, 60, 1800)
        task = asyncio.create_task(client.post("https://fixture.invalid/", json={}))
        try:
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            await asyncio.wait_for(closing.wait(), 2)
            task.cancel()
            await asyncio.sleep(0)
            self.assertEqual((task.done(), raw.reads, raw.closes), (False, 0, 1))
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(task), 2)
            self.assertEqual((raw.reads, raw.closes), (0, 1))
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_uncancelled_http_failure_retries_once_only_after_physical_close(self):
        for route in ("direct", "chat", "native-cookie"):
            with self.subTest(route=route):
                peer, sender = self.start(route, True, "body", "ordinary503", "live")
                try:
                    await asyncio.wait_for(self.body.closing.wait(), 2)
                    self.assertEqual((len(self.calls), self.chains[0].index, generation_limiter.active), (1, 0, 1))
                    self.body.release.set()
                    await asyncio.wait_for(asyncio.shield(sender), 2)
                    self.assertEqual((len(self.calls), self.chains[0].index, generation_limiter.active), (2, 1, 0))
                    self.assertTrue(self.body.closed)
                    self.assertIn(b"fixture answer", b"".join(m.get("body", b"") for m in peer.messages))
                finally:
                    self.body.release.set()
                    await asyncio.gather(sender, return_exceptions=True)
