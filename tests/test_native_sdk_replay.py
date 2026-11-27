"""Pinned Python SDK buffered replay over in-process ASGI and offline providers."""
import asyncio
import json
from typing import ClassVar
from unittest.mock import patch

import httpx
from google.genai import errors, types

from admission import generation_limiter
from express_fingerprint import sdk_label
import resumable
import streaming
from test_native_sdk_sse_keepalive import (
    ERROR, HEAD, MODEL, PADDING, PART, PATH, PROMPT, TAIL,
    GatedBody, NativeSDKHarness, encoded, mime,
)
from test_native_setup_replay_receipt import ERROR as RECEIPT_ERROR
from test_replay_http_delivery import Peer
from upstreams import native_passthrough


class NativeSDKReplay(NativeSDKHarness):
    KEY: ClassVar[dict[str, str]] = {"X-Resumable": "true", "Idempotency-Key": "offline-sdk-replay"}
    RAW_PING = b": keep-alive\n\n"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.channel = "api_key"
        self.settings.update(resumable_enabled=True)
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(patch.object(native_passthrough, "resumable_store", self.store))
        # Only explicitly gated cases should lose the race to the fast setup.
        self.enterContext(patch.object(native_passthrough, "SETUP_FAST_WINDOW_SECONDS", 2))

    def sdk_reader(self, headers, sent, starts, *, on_send=None):
        def receive_bytes(data):
            sent.append(data)
            if on_send is not None:
                on_send(data)
        return self.api(receive_bytes, headers=headers, on_start=starts.append)

    async def raw_reader(self, request, headers, query="alt=sse"):
        transport = self.transport(lambda _: None)
        async with httpx.AsyncClient(base_url="http://local", transport=transport) as client:
            return await client.post(PATH + "?" + query,
                                     headers={"x-goog-api-key": "test-key", "content-type": "application/json",
                                              **headers}, content=request.content)

    async def test_sdk_opt_in_and_explicit_offsets_preserve_the_standard_numbered_receipt(self):
        # Include upstream controls next to genuine typed fields. The selected
        # reader must not decode/re-encode these JSON bytes or repurpose a tool ID.
        tool = {"functionCall": {"id": "provider-tool", "name": "lookup", "args": {"q": "中文"}},
                "thoughtSignature": "AQID"}
        signed = {"candidates": [{"index": 0, "content": {"role": "model", "parts": [PART, tool]}}]}
        head = encoded(signed, studio=False)
        tail = encoded(TAIL, studio=False).replace(b"data:  ", b"data:", 1)
        sdk_tail = tail.replace(b"data:", b"data: ", 1)
        controls = b": provider note\r\nevent: message\r\nretry: 8\r\n"
        upstream_wire = b"id: provider-cursor\r\n" + controls + head + tail

        async with self.provider(lambda *_: httpx.Response(200, content=upstream_wire, headers=mime(False))) as requests:
            sent, starts = [], []
            first = await self.receive(self.sdk_reader(self.KEY, sent, starts))
            self.assert_success(first)
            call_part = first[0].candidates[0].content.parts[1]
            self.assertEqual((call_part.function_call.id, call_part.thought_signature), ("provider-tool", b"\x01\x02\x03"))
            self.assertEqual(sent, [PADDING, head, sdk_tail])
            rid = first[0].sdk_http_response.headers["x-resume-id"]
            self.assertNotIn(b"x-resumable-replay", dict(starts[0]["headers"]))

            buf, = self.store._buffers.values()
            expected_raw = (self.RAW_PING + f"id: {rid}:0\n".encode() + controls + head
                            + f"id: {rid}:1\n".encode() + tail)
            snapshot = (tuple(buf.events), self.store._bytes_used)
            self.channel = "web_proxy"  # retained readers do not consult today's outlet

            for headers, expected_bytes, count in (
                (self.KEY, [PADDING, head, sdk_tail], 2),
                ({"X-Resume-Id": rid}, [PADDING, head, sdk_tail], 2),
                ({"Last-Event-ID": f"{rid}:0"}, [PADDING, sdk_tail], 1),
                ({"Last-Event-ID": f"{rid}:1"}, [PADDING], 0),
            ):
                with self.subTest(headers=headers):
                    sent, starts = [], []
                    chunks = await self.receive(self.sdk_reader(headers, sent, starts))
                    self.assertEqual((len(chunks), sent), (count, expected_bytes))
                    if count == 2:
                        self.assert_success(chunks)
                    if count == 1:
                        self.assertEqual(chunks[0].candidates[0].finish_reason, types.FinishReason.STOP)
                    headers = dict(starts[0]["headers"])
                    self.assertEqual((starts[0]["status"], headers[b"x-resume-id"], headers[b"x-resumable-replay"]),
                                     (200, rid.encode(), b"1"))

            for profile in (
                {},
                {"User-Agent": f"{sdk_label()} gl-node/22.14.0"},
                {"User-Agent": f"{sdk_label()}0 gl-python/3.12.1"},
                {"User-Agent": sdk_label(), "X-Goog-Api-Client": "gl-python/3.12.1"},
                {"User-Agent": f"{sdk_label()} gl-python/"},
            ):
                with self.subTest(profile=profile):
                    raw = await self.raw_reader(requests[0], {"X-Resume-Id": rid, **profile}, "alt=sse&resume_from=0")
                    self.assertEqual(raw.content, expected_raw)
                    self.assertEqual(raw.headers["x-resumable-replay"], "1")
            # Either SDK header by itself suffices when it carries both tokens;
            # the client runtime need not equal this server's runtime.
            by_api_client = await self.raw_reader(requests[0], {
                "X-Resume-Id": rid, "X-Goog-Api-Client": f"{sdk_label()} gl-python/3.11.4",
            }, "alt=sse&resume_from=0")
            self.assertEqual(by_api_client.content, PADDING + head + sdk_tail)
            self.assertEqual((tuple(buf.events), self.store._bytes_used), snapshot)
            self.assertEqual((buf.fully_delivered, buf.has_live_delivery, len(requests)), (True, False, 1))

    async def test_running_sdk_and_standard_reader_keep_the_same_pump_when_sdk_peer_drops(self):
        head, tail = (encoded(op, studio=False) for op in (HEAD, TAIL))
        release, raw_started, provider_started = (asyncio.Event() for _ in range(3))
        pings = asyncio.Event()
        closed = asyncio.Event()
        lingered = []

        class Body(GatedBody):
            async def aclose(self):
                closed.set()

        def on_sdk_send(data):
            if data == PADDING:
                lingered.append(data)
                if len(lingered) == 2:
                    pings.set()  # opening and periodic keepalive must both be safe

        def wire(*_):
            provider_started.set()
            return httpx.Response(200, stream=Body(release, head, tail), headers=mime(False))

        with (patch.object(native_passthrough, "SETUP_FAST_WINDOW_SECONDS", 0),
              patch.object(streaming, "HEARTBEAT_INTERVAL", .01)):
            async with self.provider(wire) as requests:
                sent, starts = [], []
                sdk = self.sdk_reader(self.KEY, sent, starts, on_send=on_sdk_send)
                leader = asyncio.create_task(self.receive(sdk))
                raw_task = None
                try:
                    await asyncio.wait_for(pings.wait(), 2)
                    await asyncio.wait_for(provider_started.wait(), 2)
                    self.channel = "web_proxy"
                    buf, = self.store._buffers.values()

                    raw = Peer(PATH, query=b"alt=sse", headers=[(b"x-resume-id", buf.id.encode())])
                    raw.body = requests[0].content
                    async def raw_ready(message):
                        if message["type"] == "http.response.start":
                            raw_started.set()
                    raw_task = asyncio.create_task(raw.run(raw_ready))
                    await asyncio.wait_for(raw_started.wait(), 2)

                    abandoned = Peer(PATH, query=b"alt=sse", headers=[
                        (b"x-resume-id", buf.id.encode()),
                        (b"user-agent", f"{sdk_label()} gl-python/3.11.4".encode()),
                    ])
                    abandoned.body = requests[0].content
                    async def drop_padding(message):
                        if message.get("body") == PADDING:
                            await abandoned.drop()
                    await asyncio.wait_for(abandoned.run(drop_padding), 2)
                    self.assertTrue(abandoned.gone.is_set())
                    self.assertEqual((len(buf.clients), buf.done, buf.fully_delivered, len(requests)), (2, False, False, 1))
                finally:
                    release.set()
                    await asyncio.wait_for(asyncio.gather(leader, *([raw_task] if raw_task else [])), 2)
                self.assert_success(leader.result())
                self.assertEqual([chunk for chunk in sent if chunk != PADDING], [head, tail])
                raw_bytes = b"".join(msg.get("body", b"") for msg in raw.messages)
                self.assertTrue(raw_bytes.startswith(self.RAW_PING))
                self.assertIn(f"id: {buf.id}:0\n".encode() + head, raw_bytes)
                self.assertEqual((closed.is_set(), len(requests), generation_limiter.active), (True, 1, 0))
                self.assertEqual((buf.clients, buf.readers, buf.fully_delivered), ({}, 0, True))

    async def test_dropped_final_send_in_selected_view_does_not_consume_delivery(self):
        wire = b"".join(encoded(item, studio=False) for item in (HEAD, TAIL))
        peers = []

        async def lose_sdk_request(request):
            peer = Peer(request.url.path, query=request.url.query)
            peer.scope["headers"] = [(k.lower(), v) for k, v in request.headers.raw]
            peer.body = request.content  # retain the actual SDK byte fingerprint
            peers.append(peer)

            async def drop_final(message):
                if message["type"] == "http.response.body" and not message.get("more_body"):
                    await peer.drop()

            await asyncio.wait_for(peer.run(drop_final), 2)
            raise httpx.RemoteProtocolError("fixture: send lost before EOF", request=request)

        async with self.provider(lambda *_: httpx.Response(200, content=wire, headers=mime(False))) as requests:
            sdk = self.api(None, headers=self.KEY, transport=httpx.MockTransport(lose_sdk_request))
            with self.assertRaises(httpx.RemoteProtocolError):
                await self.receive(sdk)
            peer, = peers
            buf, = self.store._buffers.values()
            self.assertEqual((buf.fully_delivered, buf.has_live_delivery, buf.readers), (False, False, 0))
            self.assertEqual(b"".join(msg.get("body", b"") for msg in peer.messages), PADDING + wire)
            self.channel = "web_proxy"
            sent, starts = [], []
            self.assert_success(await self.receive(self.sdk_reader(self.KEY, sent, starts)))
            self.assertEqual((buf.fully_delivered, len(requests), generation_limiter.active), (True, 1, 0))
            self.assertEqual(dict(starts[0]["headers"])[b"x-resume-id"], peer.receipt.encode())

    async def test_sdk_late_and_early_429_stay_distinct_from_explicit_sse_error_and_eof(self):
        for late in (False, True):
            with self.subTest(late=late):
                await self.store.aclose()
                self.channel = "api_key"
                allow_error = asyncio.Event()
                def wire(*_):
                    headers = {"content-type": "application/json", "retry-after": "2"}
                    if late:
                        return httpx.Response(429, headers=headers,
                                              stream=GatedBody(allow_error, json.dumps(RECEIPT_ERROR, indent=2).encode()))
                    return httpx.Response(429, json=RECEIPT_ERROR, headers=headers)

                async def expect_error(headers, code, *, start=False):
                    sent, starts = [], []
                    def on_send(data):
                        if data == PADDING and start:
                            allow_error.set()  # the original HTTP status is already committed
                    with self.assertRaises(errors.ClientError) as failed:
                        await self.receive(self.sdk_reader(headers, sent, starts, on_send=on_send))
                    self.assertEqual((failed.exception.code, failed.exception.message, failed.exception.details),
                                     (429, "wait briefly", RECEIPT_ERROR))
                    self.assertEqual(starts[0]["status"], code)
                    self.assertNotIn(b"id: ", b"".join(sent))
                    self.assertNotIn(b": upstream-status", b"".join(sent))
                    return dict(starts[0]["headers"])

                with patch.object(native_passthrough, "SETUP_FAST_WINDOW_SECONDS", 0 if late else 2):
                    async with self.provider(wire) as requests:
                        initial = await expect_error(self.KEY, 200 if late else 429, start=late)
                        self.assertEqual(initial.get(b"retry-after"), None if late else b"2")
                        rid = initial[b"x-resume-id"].decode()
                        self.channel = "web_proxy"
                        retry = await expect_error(self.KEY, 429)
                        cursor = await expect_error({"X-Resume-Id": rid}, 200)
                        for headers in (retry, cursor):
                            self.assertEqual((headers[b"x-resume-id"], headers[b"x-resumable-replay"],
                                              headers[b"retry-after"]), (rid.encode(), b"1", b"2"))
                        raw = await self.raw_reader(requests[0], {"X-Resume-Id": rid})
                        self.assertTrue(raw.content.startswith(
                            self.RAW_PING + f"id: {rid}:0\n".encode() + b": upstream-status 429\n: retry-after 2\n"))
                        sent, starts = [], []
                        self.assertEqual(await self.receive(self.sdk_reader({"Last-Event-ID": f"{rid}:0"}, sent, starts)), [])
                        self.assertEqual((sent, starts[0]["status"], len(requests)), ([PADDING], 200, 1))

    async def test_sdk_midstream_error_remains_typed_and_does_not_reissue_after_signed_prefix(self):
        head = encoded(HEAD, studio=False)
        failure = encoded(ERROR, studio=False)

        async def attempt(headers, *, prefix):
            sent, starts = [], []
            sdk = self.sdk_reader(headers, sent, starts)
            async with asyncio.timeout(3):
                chunks = await sdk.aio.models.generate_content_stream(model=MODEL, contents=PROMPT)
                if prefix:
                    first = await anext(chunks)
                    self.assertEqual(first.candidates[0].content.parts[0].thought_signature, b"\xfb\xff")
                with self.assertRaises(errors.ServerError) as raised:
                    await anext(chunks)
            self.assertEqual((raised.exception.code, raised.exception.details), (503, ERROR))
            self.assertEqual(starts[0]["status"], 200)
            self.assertEqual(sent[0], PADDING)
            self.assertNotIn(b"id: ", b"".join(sent))
            return dict(starts[0]["headers"])[b"x-resume-id"].decode()

        async with self.provider(lambda *_: httpx.Response(200, stream=GatedBody(head, failure), headers=mime(False)),
                                 retries=(1, 0, 0)) as requests:
            rid = await attempt(self.KEY, prefix=True)
            self.channel = "web_proxy"
            self.assertEqual(await attempt({"Last-Event-ID": f"{rid}:0"}, prefix=False), rid)
            self.assertEqual((len(requests), generation_limiter.active), (1, 0))
