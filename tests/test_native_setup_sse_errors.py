"""Native Express setup diagnostics after commitment, through offline ASGI/SDK."""
import asyncio
import json
from unittest.mock import patch

import httpx
from fastapi.responses import Response
from google.genai import errors

from replay_event import ReplayEvent
import resumable
from test_native_sdk_sse_keepalive import GatedBody, NativeSDKHarness, PADDING, PATH, PROMPT
from upstreams import native_passthrough


ERROR = {"code": 400, "status": "INVALID_ARGUMENT",
         "message": "offline fixture: tool call is missing thought_signature",
         "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "INVALID_TOOL_HISTORY"}]}
ENVELOPE = {"error": ERROR}
RETRY = b": upstream-status 400\n: retry-after 2\n"
SSE = "text/event-stream"


def sse_body(payload):
    fields = json.dumps(payload, ensure_ascii=False, indent=2).encode().splitlines()
    return (b": provider is preparing\r\n\r\n"
            + b"\r\n".join(b"data: " + field for field in fields)
            + b"\r\n\r\n: EOF\r\n\r\n")


class NativeSetupSSEErrors(NativeSDKHarness):
    async def sdk_error(self, headers, sent, starts, *, on_start=None):
        def started(message):
            starts.append(message)
            if on_start is not None:
                on_start()

        with self.assertRaises(errors.ClientError) as failed:
            await self.receive(self.api(sent.append, headers=headers, on_start=started))
        return failed.exception

    async def test_late_real_400_sse_diagnostic_survives_as_one_typed_error(self):
        for sdk in (False, True):
            with self.subTest(sdk=sdk):
                sent, starts, release = [], [], asyncio.Event()

                def provider(*_):
                    return httpx.Response(400, headers={"content-type": SSE, "Retry-After": "2"},
                                          stream=GatedBody(release, sse_body(ENVELOPE)))

                def started(message):
                    starts.append(message)
                    release.set()

                async with self.provider(provider, retries=(2, 0, 0)) as calls:
                    if sdk:
                        failed = await self.sdk_error({}, sent, starts, on_start=release.set)
                        self.assertEqual((failed.code, failed.message, failed.details), (400, ERROR["message"], ENVELOPE))
                    else:
                        async with httpx.AsyncClient(base_url="http://local", transport=self.transport(
                                sent.append, on_start=started)) as client:
                            reply = await client.post(PATH + "?alt=sse", json={"contents": PROMPT},
                                                      headers={"x-goog-api-key": "test-key"})
                        self.assertEqual(reply.status_code, 200)
                    self.assertEqual(len(calls), 1)  # 400 must never reissue

                self.assertEqual(starts[0]["status"], 200)  # held body until response start
                self.assertNotIn(b"retry-after", dict(starts[0]["headers"]))
                self.assertEqual(sent[0], PADDING)
                wire = b"".join(sent)
                self.assertEqual(json.loads(ReplayEvent(wire.decode()).data()), ENVELOPE)
                self.assertNotIn(b": provider is preparing", wire)
                self.assertNotIn(b"[DONE]", wire)
                if sdk:
                    self.assertLess(wire.index(b"data: "), wire.index(RETRY))
                else:
                    self.assertLess(wire.index(RETRY), wire.index(b"data: "))

    async def test_late_sdk_uses_http_code_and_receives_error_first_even_with_outer_metadata(self):
        for document in ({"error": {k: v for k, v in ERROR.items() if k != "code"}},
                         {"error": {**ERROR, "code": 429}},
                         {"metadata": {"origin": "fixture"}, "error": ERROR}):
            with self.subTest(document=document):
                sent, starts, release = [], [], asyncio.Event()

                def provider(*_):
                    return httpx.Response(400, headers={"content-type": "application/json"},
                        stream=GatedBody(release, json.dumps(document).encode()))

                async with self.provider(provider, retries=(2, 0, 0)) as calls:
                    failed = await self.sdk_error({}, sent, starts, on_start=release.set)
                    expected = {**document, "error": ERROR}
                    self.assertEqual((failed.code, failed.message, failed.details), (400, ERROR["message"], expected))
                    self.assertEqual((starts[0]["status"], len(calls)), (200, 1))

    async def test_retained_real_400_keeps_error_on_late_current_keyed_and_cursor_paths(self):
        self.settings.update(resumable_enabled=True)
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(native_passthrough, "resumable_store", store))
        release = asyncio.Event()
        key = {"X-Resumable": "true", "Idempotency-Key": "offline-sse-error"}

        def provider(*_):
            return httpx.Response(400, headers={"content-type": SSE, "Retry-After": "2"},
                                  stream=GatedBody(release, sse_body(ENVELOPE)))

        with patch.object(native_passthrough, "SETUP_FAST_WINDOW_SECONDS", 0):
            async with self.provider(provider, retries=(2, 0, 0)) as calls:
                sent, starts = [], []
                first = await self.sdk_error(key, sent, starts, on_start=release.set)
                self.assertEqual((starts[0]["status"], first.details), (200, ENVELOPE))
                rid = dict(starts[0]["headers"])[b"x-resume-id"]
                buf, = store._buffers.values()
                self.assertEqual((buf.id.encode(), buf.status_code, buf.fully_delivered), (rid, 400, True))
                self.assertEqual((buf.next_seq, json.loads(ReplayEvent(buf.events[0]).data())), (1, ENVELOPE))

                self.channel = "web_proxy"
                for headers, status in ((key, 400), ({"X-Resume-Id": rid.decode()}, 200)):
                    sent, starts = [], []
                    failed = await self.sdk_error(headers, sent, starts)
                    self.assertEqual((starts[0]["status"], failed.code, failed.details), (status, 400, ENVELOPE))
                    headers = dict(starts[0]["headers"])
                    self.assertEqual((headers[b"x-resume-id"], headers[b"retry-after"], headers[b"x-resumable-replay"]),
                                     (rid, b"2", b"1"))
                self.assertEqual(len(calls), 1)

    async def test_real_fast_and_non_sse_http_errors_still_forward_the_original_bytes(self):
        raw = sse_body(ENVELOPE)

        def provider(*_):
            return httpx.Response(400, headers={"content-type": SSE, "Retry-After": "2"}, content=raw)

        async with self.provider(provider, retries=(2, 0, 0)) as calls:
            async with httpx.AsyncClient(base_url="http://local", transport=self.transport(lambda _: None)) as cli:
                for path in (PATH + "?alt=sse", PATH + "?alt=json",
                             PATH.replace("streamGenerateContent", "generateContent")):
                    reply = await cli.post(path, json={"contents": PROMPT}, headers={"x-goog-api-key": "test-key"})
                    self.assertEqual((reply.status_code, reply.content, reply.headers["retry-after"]), (400, raw, "2"))
                    self.assertNotIn("x-resume-id", reply.headers)
            self.assertEqual(len(calls), 3)

    def test_convert_only_one_complete_bounded_declared_sse_error(self):
        raw = sse_body(ENVELOPE)
        for bad, ctype in (
            (b"data: " + json.dumps(ENVELOPE).encode() + b"\n", SSE),  # no dispatch boundary
            (raw + raw, SSE),
            (b": keep-alive\n\n" * 64 + raw, SSE),
            (raw, "text/plain"),
            (sse_body({"candidates": [{"finishReason": "STOP"}]}), SSE),
            (sse_body({"error": {**ERROR, "details": [float("nan")]}}), SSE),
        ):
            with self.subTest(ctype=ctype, bad=bad[:60]):
                response = Response(content=bad, status_code=400, media_type=ctype)
                payload = json.loads(native_passthrough.native_setup_error_body(response))
                self.assertEqual(payload["error"]["code"], 400)
                self.assertNotEqual(payload["error"]["message"], ERROR["message"])
                self.assertNotIn("details", payload["error"])
        with patch.object(native_passthrough, "MAX_UPSTREAM_ERROR_BYTES", len(raw) - 1):
            response = Response(content=raw, status_code=400, media_type=SSE)
            self.assertNotIn("details", json.loads(native_passthrough.native_setup_error_body(response))["error"])
