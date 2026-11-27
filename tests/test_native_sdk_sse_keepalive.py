"""Native direct SSE over ASGI with the pinned Google SDK and offline providers."""
import asyncio
import base64
import json
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx
from google import genai
from google.genai import errors, types

from express_fingerprint import sdk_label
from main import app
import streaming
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_native, cookie_transport, native_passthrough


MODEL = "gemini-3.6-flash"
PATH = f"/v1beta/models/{MODEL}:streamGenerateContent"
PROMPT = [{"role": "user", "parts": [{"text": "offline fixture"}]}]
PART = {"text": "收到", "thoughtSignature": "+/8="}
HEAD = {"candidates": [{"index": 0, "content": {"role": "model", "parts": [PART]}}]}
TAIL = {"candidates": [{"index": 0, "finishReason": "STOP"}],
        "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 1, "totalTokenCount": 4}}
ERROR = {"error": {"code": 503, "message": "offline unavailable", "status": "UNAVAILABLE"}}
PADDING = b"\n\n"


class GatedBody(httpx.AsyncByteStream):
    def __init__(self, *steps):
        self.steps = steps

    async def __aiter__(self):
        for step in self.steps:
            if isinstance(step, asyncio.Event):
                await step.wait()
            else:
                yield step


def encoded(value, *, studio):
    if studio:
        return json.dumps({"results": [{"data": value}]}, ensure_ascii=False).encode() + b"\n"
    return b"data:  " + json.dumps(value, ensure_ascii=False).encode() + b"\r\n\r\n"


def mime(studio):
    return {"content-type": "application/json" if studio else "text/event-stream"}


class NativeSDKHarness(SchemaRouteHarness):
    @staticmethod
    def transport(on_send, on_request=None, *, on_start=None):
        async def observe(scope, receive, send):
            if on_request is not None:
                on_request(scope)

            async def send_observed(message):
                if message["type"] == "http.response.start" and on_start is not None:
                    on_start(message)
                elif message["type"] == "http.response.body" and (body := message.get("body")):
                    on_send(body)
                await send(message)

            await app(scope, receive, send_observed)

        return httpx.ASGITransport(app=observe)

    def api(self, on_send, on_request=None, *, headers=None, on_start=None, transport=None):
        if transport is None:
            transport = self.transport(on_send, on_request, on_start=on_start)
        sdk = genai.Client(api_key="test-key", http_options=types.HttpOptions(
            base_url="http://local", api_version="v1beta", headers=headers,
            retry_options=types.HttpRetryOptions(attempts=1),
            async_client_args={"transport": transport}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        return sdk

    @asynccontextmanager
    async def provider(self, responder, retries=(0, 0.0, 0.0)):
        requests = []

        def wire(request):
            requests.append(request)
            return responder(request, len(requests))

        async with httpx.AsyncClient(transport=httpx.MockTransport(wire)) as upstream:
            @asynccontextmanager
            async def open_studio(*_a, **_kw):
                yield upstream

            with (patch.object(native_passthrough.passthrough_client_pool, "get", return_value=upstream),
                  patch.object(native_passthrough, "get_retry_settings", return_value=retries),
                  patch.object(cookie_native, "open_upstream_client", open_studio),
                  patch.object(cookie_native, "get_retry_settings", return_value=retries)):
                yield requests

    async def receive(self, sdk, contents=PROMPT):
        async with asyncio.timeout(3):
            chunks = await sdk.aio.models.generate_content_stream(model=MODEL, contents=contents)
            return [chunk async for chunk in chunks]

    def assert_success(self, chunks):
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0].text, PART["text"])
        self.assertEqual(chunks[0].candidates[0].content.parts[0].thought_signature, b"\xfb\xff")
        self.assertEqual(chunks[1].candidates[0].finish_reason, types.FinishReason.STOP)
        self.assertEqual(chunks[1].usage_metadata.total_token_count, 4)


class NativeSDKKeepalive(NativeSDKHarness):
    async def test_opening_keepalive_is_transport_only_and_signed_part_roundtrips(self):
        sent = []
        sdk = self.api(sent.append)
        for studio in (False, True):
            with self.subTest(studio=studio):
                self.channel = "web_proxy" if studio else "api_key"
                sent.clear()
                head, tail = (encoded(item, studio=studio) for item in (HEAD, TAIL))

                def wire(_request, _count):
                    return httpx.Response(200, content=head + tail, headers=mime(studio))

                async with self.provider(wire) as requests:
                    chunks = await self.receive(sdk)
                    self.assert_success(chunks)
                    if not studio:
                        self.assertEqual(b"".join(sent), PADDING + head + tail)
                    else:
                        self.assertEqual(sent[0], PADDING)
                    self.assert_success(await self.receive(
                        sdk, [types.Content.model_validate(PROMPT[0]), chunks[0].candidates[0].content,
                              types.Content(role="user", parts=[types.Part(text="next")])]))
                self.assertEqual(len(requests), 2)
                request = json.loads(requests[1].content)
                model = request.get("variables", request)["contents"][1]["parts"][0]
                signature = model.get("thoughtSignature", model.get("thought_signature"))
                self.assertIsInstance(signature, str)
                self.assertEqual(base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4)), b"\xfb\xff")
                if studio:
                    self.assertEqual(signature, "+/8=")  # private Bytes scalar is standard Base64

    async def test_owned_nullable_tool_deltas_roundtrip_and_reassemble(self):
        from request_tracking import current_trace
        from tool_stream import ToolStreamAssembler

        def chunk(call, **part_fields):
            return {"candidates": [{"index": 0, "content": {"role": "model",
                    "parts": [{"functionCall": call, **part_fields}]}}]}

        owner = chunk({"name": "lookup", "id": "owner", "willContinue": True}, thoughtSignature="+/8=")
        cases = (
            ({"nullValue": None}, None),
            ({"nullValue": None, "boolValue": None, "stringValue": None, "numberValue": None}, None),
            ({"nullValue": "NULL_VALUE", "numberValue": None}, None),
            ({"boolValue": False, "stringValue": None}, False),
            ({"numberValue": 0, "boolValue": None}, 0.0),
            ({"stringValue": "", "numberValue": None}, ""),
        )
        sent, traces = [], []
        sdk = self.api(sent.append)
        for fields, value in cases:
            with self.subTest(fields=fields):
                sent.clear()
                traces.clear()
                fragment = {"jsonPath": "$.slot", **fields}
                delta = chunk({"id": "owner", "partialArgs": [fragment]})
                wire_bytes = b"".join(encoded(obj, studio=False) for obj in (owner, delta, TAIL))

                def wire(_request, _count):
                    traces.append(current_trace.get())
                    return httpx.Response(200, content=wire_bytes, headers=mime(False))

                async with self.provider(wire) as requests:
                    chunks = await self.receive(sdk)
                self.assertEqual((len(requests), len(chunks)), (1, 3))
                self.assertEqual(b"".join(sent), PADDING + wire_bytes)
                partial = chunks[1].candidates[0].content.parts[0].function_call.partial_args[0]
                self.assertEqual(partial.model_dump(mode="json", by_alias=True, exclude_unset=True), fragment)
                self.assertEqual(traces[0].snapshot()["tool_deltas"], 2)
                assembler = ToolStreamAssembler()
                restored = [assembler.restore_sdk(chunk) for chunk in chunks]
                assembler.validate_end()
                self.assertFalse(restored[0].candidates[0].content.parts)
                call = restored[1].candidates[0].content.parts[0]
                self.assertEqual((call.function_call.name, call.function_call.id, call.function_call.args,
                                  call.thought_signature), ("lookup", "owner", {"slot": value}, b"\xfb\xff"))
                self.assertIs(type(call.function_call.args["slot"]), type(value))

    async def test_periodic_keepalive_waits_during_slow_setup_and_between_real_frames(self):
        for studio in (False, True):
            with self.subTest(studio=studio):
                self.channel = "web_proxy" if studio else "api_key"
                allow_head, allow_tail = asyncio.Event(), asyncio.Event()
                sent, before = [], []
                saw_data = False

                def on_send(body):
                    nonlocal saw_data
                    sent.append(body)
                    if body.startswith(b"data: "):
                        saw_data = True
                    if body in (PADDING, b": keep-alive\n\n"):
                        if saw_data:
                            allow_tail.set()
                        else:
                            before.append(body)
                            if len(before) == 2:
                                allow_head.set()

                head, tail = (encoded(item, studio=studio) for item in (HEAD, TAIL))
                split = head.index(PART["text"].encode()) + 1
                def wire(_request, _count):
                    return httpx.Response(200, headers=mime(studio), stream=GatedBody(
                        head[:split], allow_head, head[split:], allow_tail, tail))

                with patch.object(streaming, "HEARTBEAT_INTERVAL", .01):
                    sdk = self.api(on_send)
                    async with self.provider(wire) as requests:
                        self.assert_success(await self.receive(sdk))
                self.assertEqual(len(requests), 1)
                self.assertTrue(allow_head.is_set() and allow_tail.is_set())
                data = [chunk for chunk in sent if chunk.startswith(b"data: ")]
                self.assertEqual(len(data), 2)
                self.assertTrue(all(item == PADDING for item in sent if item not in data))
                if not studio:
                    self.assertEqual(data, [head, tail])

    async def test_both_backoff_paths_pad_until_one_real_successful_attempt(self):
        for studio in (False, True):
            with self.subTest(studio=studio):
                self.channel = "web_proxy" if studio else "api_key"
                sent = []
                terminal = b"".join(encoded(item, studio=studio) for item in (HEAD, TAIL))

                def wire(_request, count):
                    if count == 1:
                        return httpx.Response(503, json=ERROR)
                    return httpx.Response(200, content=terminal, headers=mime(studio))

                heartbeats = cookie_transport.retry_heartbeats
                # Run the real Cookie backoff heartbeat at its 0.5 s test
                # cadence; exclude the ordinary idle timer on this leg.
                with (patch.object(cookie_transport, "retry_heartbeats", side_effect=
                           lambda delay, request: heartbeats(delay, hb_interval=.5, request=request)),
                      patch.object(streaming, "HEARTBEAT_INTERVAL", 10 if studio else .02)):
                    sdk = self.api(sent.append)
                    async with self.provider(wire, (1, .7 if studio else .32, 0.0)) as requests:
                        self.assert_success(await self.receive(sdk))
                self.assertEqual(len(requests), 2)
                self.assertGreaterEqual(sent.count(PADDING), 2)
                self.assertNotIn(ERROR["error"]["message"].encode(), b"".join(sent))

    async def test_live_failure_after_first_part_remains_sdk_typed_and_does_not_retry(self):
        for studio in (False, True):
            with self.subTest(studio=studio):
                self.channel = "web_proxy" if studio else "api_key"
                sent = []
                sdk = self.api(sent.append)
                head = encoded(HEAD, studio=studio)
                failure = ((json.dumps(ERROR).encode() + b"\n") if studio else encoded(ERROR, studio=False))
                def wire(_request, _count):
                    return httpx.Response(200, headers=mime(studio), stream=GatedBody(head, failure))

                async with self.provider(wire, (1, 0.0, 0.0)) as requests:
                    async with asyncio.timeout(3):
                        chunks = await sdk.aio.models.generate_content_stream(model=MODEL, contents=PROMPT)
                        self.assertEqual((await anext(chunks)).text, PART["text"])
                        with self.assertRaises(errors.ServerError) as caught:
                            await anext(chunks)
                        self.assertEqual(caught.exception.code, 503)
                        self.assertEqual(caught.exception.message, ERROR["error"]["message"])
                self.assertEqual(len(requests), 1)
                self.assertEqual(sent[0], PADDING)

    async def test_slow_http_error_keeps_wire_and_reports_the_real_status_to_pinned_sdk(self):
        self.channel = "api_key"
        error = {"error": {"code": 429, "message": "offline quota", "status": "RESOURCE_EXHAUSTED",
                           "details": [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "2s"}]}}
        advisory = b": upstream-status 429\n: retry-after 2\n"
        for pretty in (False, True):
            for google_sdk in (False, True):
                with self.subTest(pretty=pretty, google_sdk=google_sdk):
                    raw = json.dumps(error, indent=2 if pretty else None).encode()
                    ready, sent, received = asyncio.Event(), [], []

                    def on_send(body):
                        sent.append(body)
                        if body == PADDING:
                            ready.set()  # let the route commit its SSE headers before publishing the error

                    def wire(_request, _count):
                        return httpx.Response(429, headers={"content-type": "application/json", "retry-after": "2"},
                                              stream=GatedBody(ready, raw))

                    def on_request(scope):
                        received.append(dict(scope["headers"]))
                        self.assertEqual(scope["path"], PATH)
                        self.assertEqual(scope["query_string"], b"alt=sse")

                    async with self.provider(wire) as requests:
                        if google_sdk:
                            with self.assertRaises(errors.ClientError) as caught:
                                await self.receive(self.api(on_send, on_request))
                            self.assertEqual((caught.exception.code, caught.exception.message), (429, "offline quota"))
                            self.assertEqual(caught.exception.details, error)
                            for name in (b"user-agent", b"x-goog-api-client"):
                                self.assertIn(sdk_label().encode(), received[0][name].split())
                        else:
                            async with httpx.AsyncClient(base_url="http://local",
                                    transport=self.transport(on_send, on_request)) as client:
                                reply = await client.post(PATH + "?alt=sse",
                                    headers={"x-goog-api-key": "test-key"}, json={"contents": PROMPT})
                            self.assertEqual(reply.status_code, 200)  # already committed, not a second HTTP reply
                            self.assertNotIn(b"x-goog-api-client", received[0])
                    body = b"".join(sent)
                    self.assertEqual(len(requests), 1)  # actual SDK did not reissue a new request
                    self.assertEqual(json.loads(requests[0].content)["contents"], PROMPT)
                    self.assertEqual(requests[0].url.path,
                                     "/v1beta1/publishers/google/models/" + MODEL + ":streamGenerateContent")
                    self.assertEqual(requests[0].url.params["alt"], "sse")
                    lines = b"\n".join(line[6:] for line in body.splitlines() if line.startswith(b"data: "))
                    self.assertEqual(lines, raw)
                    if google_sdk:
                        self.assertLess(body.index(b"data: "), body.index(advisory))
                    else:
                        self.assertLess(body.index(advisory), body.index(b"data: "))
