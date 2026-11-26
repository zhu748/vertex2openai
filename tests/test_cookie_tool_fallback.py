"""Real routes and a local Cookie wire: never silently change the caller's tool contract."""
import asyncio
import json
import unittest
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import AsyncMock, patch

import channel_dispatch
import httpx
from fastapi.responses import JSONResponse
from http_io import BoundedAsyncClient
from test_parallel_tool_controls import MODES, _arguments, _request
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery, cookie_native
from upstreams.cookie_transport import is_native_tool_error_response

REJECTION = "FunctionDeclaration not supported"
SECOND = "ordinary text from a second generation"


def success(part):
    return {"results": [{"data": {"candidates": [
        {"content": {"role": "model", "parts": [part]}, "finishReason": "STOP"}
    ]}}]}


def frames(response):
    return [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: {")]


class CookieToolFallbackRoutes(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.channel = "web_proxy"

        def scripted(request):
            self.sent.append(json.loads(request.content)["variables"])
            if len(self.sent) == 1:
                return self.first
            return httpx.Response(200, json=success({"text": SECOND}))

        studio = BoundedAsyncClient(transport=httpx.MockTransport(scripted))
        self.addAsyncCleanup(studio.aclose)

        @asynccontextmanager
        async def open_studio(*args, **kwargs):
            yield studio

        stub = patch.object(cookie_delivery, "open_upstream_client", open_studio)
        stub.start()
        self.addCleanup(stub.stop)

    async def request_tool(self, first, protocol, choice, stream, fake):
        self.settings["fake_streaming"] = fake
        self.first = first
        self.sent.clear()
        path, body = _request(protocol, choice, stream=stream)
        return await self.client.post(path, json=body)

    def assert_rejected(self, response, stream):
        if stream:
            self.assertTrue(response.headers["content-type"].startswith("text/event-stream"))
            data = frames(response)
            self.assertFalse(any(choice.get("finish_reason") for f in data for choice in f.get("choices", [])))
            self.assertFalse(any(f.get("delta", {}).get("stop_reason") for f in data))
            errors = [f["error"] for f in data if "error" in f]
            self.assertEqual(len(errors), 1, response.text)
            error = errors[0]
        else:
            self.assertEqual(response.status_code, 400)
            error = response.json()["error"]
        self.assertIn(REJECTION, error["message"])
        self.assertNotIn(SECOND, response.text)

    async def test_mandatory_rejection_never_reissues_without_declared_tools(self):
        for protocol in ("openai", "claude"):
            for choice in ("required", "named"):
                for stream, fake in MODES:
                    with self.subTest(protocol=protocol, choice=choice, stream=stream, fake=fake):
                        first = httpx.Response(400, json={"error": {"message": REJECTION}})
                        response = await self.request_tool(first, protocol, choice, stream, fake)
                        self.assertEqual(len(self.sent), 1)
                        config = self.sent[0]["toolConfig"]["functionCallingConfig"]
                        self.assertEqual(config, {"mode": "ANY", "allowedFunctionNames": ["weather"]})
                        self.assert_rejected(response, stream)

    async def test_auto_can_degrade_once_on_genuine_provider_rejection(self):
        for protocol in ("openai", "claude"):
            for stream, fake in MODES:
                with self.subTest(protocol=protocol, stream=stream, fake=fake):
                    first = httpx.Response(400, json={"error": {"message": REJECTION}})
                    response = await self.request_tool(first, protocol, "auto", stream, fake)
                    self.assertEqual(len(self.sent), 2)
                    self.assertEqual(self.sent[0]["toolConfig"]["functionCallingConfig"]["mode"], "AUTO")
                    self.assertNotIn("tools", self.sent[1])
                    self.assertNotIn("toolConfig", self.sent[1])
                    self.assertIn(SECOND, response.text)


    async def test_http_200_hint_applies_to_authoritative_provider_error_only(self):
        self.settings.update(retry_max=1, retry_backoff_seconds=0, retry_jitter_seconds=0)
        # Same-envelope invalid sibling and truncated next object must not replace
        # the first proven error. Non-error model text must never inherit the hint.
        for protocol in ("openai", "claude"):
            for stream, fake in MODES:
                for code, hint, repeats in ((400, "600", 1), (429, "600", 1), (400, "0", 2),
                                            (429, "invalid", 2), (200, "600", 1)):
                    with self.subTest(protocol=protocol, stream=stream, fake=fake, code=code, hint=hint):
                        data = (success({"text": "healthy first"}) if code == 200 else
                                {"results": [{"errors": [{"code": code, "message":
                                REJECTION if code == 400 else "quota"}]}, "malformed sibling"]})
                        content = json.dumps(data) + ("" if code == 200 else '\n{"cut":')
                        response = await self.request_tool(httpx.Response(200, headers={"retry-after": hint},
                                                                     content=content), protocol, "auto", stream, fake)
                        self.assertEqual(len(self.sent), repeats)
                        if code == 200 or repeats == 2:
                            self.assertIn("healthy first" if code == 200 else SECOND, response.text)
                            self.assertNotIn("retry-after", response.headers)
                            self.assertNotIn("retry-after", response.text.lower())
                        else:
                            self.assertNotIn(SECOND, response.text)
                            if stream:
                                self.assertEqual(response.status_code, 200)
                                self.assertIn("600", response.text)
                            else:
                                self.assertEqual((response.status_code, response.headers["retry-after"]), (code, hint))

    async def test_native_and_image_keep_positive_hint_on_genuine_http_200_error(self):
        self.settings.update(retry_max=1, retry_backoff_seconds=0, retry_jitter_seconds=0)
        self.enterContext(patch.object(cookie_native, "open_upstream_client", cookie_delivery.open_upstream_client))
        for mode in ("native_json", "native_array", "native_sse", "compat_image"):
            with self.subTest(mode=mode):
                self.sent.clear()
                self.first = httpx.Response(200, headers={"Retry-After": "600"}, json={
                    "results": [{"errors": [{"code": 429, "message": "quota"}]}]})
                if mode == "compat_image":
                    response = await self.client.post("/v1/chat/completions", json={
                        "model": "gemini-3-pro-image-preview", "messages": [{"role": "user", "content": "Hi"}],
                        "stream": True})
                else:
                    method = ("generateContent" if mode == "native_json" else
                              "streamGenerateContent" + ("?alt=sse" if mode == "native_sse" else ""))
                    response = await self.client.post("/v1beta/models/gemini-2.5-flash:" + method,
                                                      json={"contents": []})
                self.assertEqual(len(self.sent), 1, response.text)
                if mode == "native_json":
                    self.assertEqual((response.status_code, response.headers["retry-after"]), (429, "600"))
                else:
                    self.assertIn("600", response.text)
                self.assertIn("429", response.text)

    async def test_advised_tool_rejection_preserves_first_error_and_never_degrades_early(self):
        future = format_datetime(datetime.now(UTC) + timedelta(hours=1), usegmt=True)
        for code, hint in ((400, "45"), (422, future)):
            for protocol in ("openai", "claude"):
                for stream, fake in MODES:
                    with self.subTest(code=code, protocol=protocol, stream=stream, fake=fake):
                        first = httpx.Response(code, headers={"retry-after": hint},
                                               json={"error": {"message": REJECTION}})
                        result = await self.request_tool(first, protocol, "auto", stream, fake)
                        self.assertIn(REJECTION, result.text)
                        self.assertNotIn(SECOND, result.text)
                        self.assertEqual(len(self.sent), 1)
                        if not stream:
                            self.assertEqual((result.status_code, result.headers["retry-after"]), (code, hint))
                        elif protocol == "claude":
                            self.assertEqual(result.text.count(f": retry-after {hint}\n\n"), 1)
                            error, = [f for f in frames(result) if f.get("type") == "error"]
                            self.assertEqual(set(error["error"]), {"type", "message"})
                        else:
                            error, = [f["error"] for f in frames(result) if "error" in f]
                            self.assertEqual((error["code"], error["retry_after"]), (code, hint))

    async def test_zero_and_malformed_advisories_do_not_disable_optional_tool_fallback(self):
        for hint in ("0", "not a delay"):
            for stream, fake in MODES:
                with self.subTest(hint=hint, stream=stream, fake=fake):
                    result = await self.request_tool(httpx.Response(422, headers={"retry-after": hint},
                        json={"error": {"message": REJECTION}}), "openai", "auto", stream, fake)
                    self.assertEqual(len(self.sent), 2)
                    self.assertIn(SECOND, result.text)

    async def test_cut_advised_429_does_not_start_an_opted_in_alternate_generation(self):
        from test_cookie_diagnostic_status import BrokenBody

        self.settings["cross_channel_fallback"] = True
        alt = AsyncMock(return_value=JSONResponse({"choices": [{"message": {"content": SECOND}}]}))
        with (patch.object(channel_dispatch, "request_timeout_seconds", return_value=2),
              patch.object(channel_dispatch.express_upstream, "chat_completions", alt)):
            for protocol in ("openai", "claude"):
                for stream, fake in MODES:
                    with self.subTest(protocol=protocol, stream=stream, fake=fake):
                        body = BrokenBody()
                        first = httpx.Response(429, headers={"retry-after": "45"}, stream=body)
                        result = await self.request_tool(first, protocol, "auto", stream, fake)
                        self.assertEqual((len(self.sent), body.closes), (1, 1))
                        alt.assert_not_awaited()
                        self.assertNotIn("partial", result.text)
                        self.assertNotIn(SECOND, result.text)
                        if not stream:
                            self.assertEqual((result.status_code, result.headers["retry-after"]), (429, "45"))
                        elif protocol == "claude":
                            self.assertIn(": retry-after 45\n\n", result.text)
                            self.assertIn("rate_limit_error", result.text)
                        else:
                            error, = [f["error"] for f in frames(result) if "error" in f]
                            self.assertEqual((error["code"], error["retry_after"]), (429, "45"))
            # Prove the same configured alternate is enabled, not just absent.
            for hint in ("0", "unparseable"):
                alt.reset_mock()
                result = await self.request_tool(httpx.Response(429, headers={"retry-after": hint},
                    json={"error": {"message": "quota"}}), "openai", "auto", False, False)
                self.assertEqual(len(self.sent), 1)
                alt.assert_awaited_once()
                self.assertIn(SECOND, result.text)

    async def test_precommit_and_committed_header_only_advisories_keep_the_protocol_shape(self):
        self.settings["cross_channel_fallback"] = True
        for protocol in ("openai", "claude"):
            for slow in (False, True):
                async def first(*args, slow=slow, **kwargs):
                    if slow:
                        await asyncio.sleep(0.025)
                    return JSONResponse({"error": {"code": 429, "message": "quota"}}, status_code=429,
                                        headers={"retry-after": "45", "x-private": "not forwarded"})

                with (self.subTest(protocol=protocol, slow=slow),
                      patch.object(channel_dispatch, "SETUP_FAST_WINDOW_SECONDS", 0.001 if slow else 1),
                      patch.object(channel_dispatch.cookie_upstream, "chat_completions", first),
                      patch.object(channel_dispatch.express_upstream, "chat_completions", new_callable=AsyncMock) as alt):
                    result = await self.request_tool(None, protocol, "auto", True, False)
                    alt.assert_not_awaited()
                    if not slow:
                        self.assertEqual((result.status_code, result.headers["retry-after"]), (429, "45"))
                        if protocol == "claude":
                            self.assertNotIn("x-private", result.headers)
                    else:
                        self.assertEqual(result.status_code, 200)
                        self.assertNotIn("retry-after", result.headers)
                        error, = [f["error"] for f in frames(result) if "error" in f]
                        if protocol == "claude":
                            self.assertIn(": retry-after 45\n\n", result.text)
                            self.assertEqual(error, {"type": "rate_limit_error", "message": "quota"})
                        else:
                            self.assertEqual(error["retry_after"], "45")

    async def test_generated_arguments_are_data_not_a_schema_rejection(self):
        args = {"error": "Unknown field in function declaration", "errors": [{"message": REJECTION}]}
        call = {"functionCall": {"id": "tool_1", "name": "weather", "args": args}}
        for protocol in ("openai", "claude"):
            for stream, fake in MODES:
                with self.subTest(protocol=protocol, stream=stream, fake=fake):
                    first = httpx.Response(200, json=success(call))
                    response = await self.request_tool(first, protocol, "auto", stream, fake)
                    self.assertEqual(len(self.sent), 1)
                    self.assertEqual(_arguments(response, protocol, stream), [args])
                    self.assertNotIn(SECOND, response.text)

    async def test_structured_200_provider_errors_are_eligible_before_but_not_after_content(self):
        err = {"code": 400, "message": REJECTION}
        partial = {"data": {"candidates": [{"content": {"parts": [{"text": "a first fragment"}]}}]}}
        for label, obj in (("single", {"error": err}), ("plural", {"errors": [err]}),
                           ("batched", {"results": [{"errors": [err]}]}),
                           ("after_output", {"results": [partial, {"errors": [err]}]})):
            for protocol in ("openai", "claude"):
                for stream, fake in MODES:
                    with self.subTest(label=label, protocol=protocol, stream=stream, fake=fake):
                        # Actual Studio batches can be arrays with an anti-XSSI prefix.
                        first = httpx.Response(200, text=")]}'\n" + json.dumps([obj]))
                        response = await self.request_tool(first, protocol, "auto", stream, fake)
                        if label == "after_output":
                            self.assertEqual(len(self.sent), 1)
                            self.assert_rejected(response, stream)
                        else:
                            self.assertEqual(len(self.sent), 2)
                            self.assertNotIn("tools", self.sent[1])
                            self.assertIn(SECOND, response.text)

    async def test_first_proven_error_wins_over_invalid_siblings_and_truncated_tail(self):
        err = {"errors": [{"code": 400, "message": REJECTION}]}
        sources = {
            "standalone": json.dumps(err),
            "truncated tail": json.dumps(err) + '\n{"results":',
            "invalid later result": json.dumps({"results": [err, {"data": {"candidates": "invalid"}}]}),
        }

        class Body(httpx.AsyncByteStream):
            def __init__(self, wire):
                self.wire = wire
                self.closes = 0

            async def __aiter__(self):
                yield self.wire.encode()

            async def aclose(self):
                self.closes += 1

        for protocol in ("openai", "claude"):
            for stream, fake in MODES:
                for label, wire in sources.items():
                    with self.subTest(protocol=protocol, stream=stream, fake=fake, source=label):
                        body = Body(wire)
                        response = await self.request_tool(httpx.Response(200, stream=body),
                                                           protocol, "required", stream, fake)
                        self.assert_rejected(response, stream)
                        self.assertEqual((len(self.sent), body.closes), (1, 1))

    async def test_4xx_unrelated_diagnostic_does_not_retry_based_on_echoed_data(self):
        for protocol in ("openai", "claude"):
            for stream, fake in MODES:
                with self.subTest(protocol=protocol, stream=stream, fake=fake):
                    first = httpx.Response(400, json={"error": {"message": "ordinary validation error",
                                                                 "details": {"example": REJECTION}}})
                    response = await self.request_tool(first, protocol, "auto", stream, fake)
                    self.assertEqual(len(self.sent), 1)
                    self.assertIn("ordinary validation error", response.text)
                    self.assertNotIn(SECOND, response.text)


class HTTPToolRejectionTests(unittest.TestCase):
    def test_only_bounded_known_validation_diagnostics_can_change_request_shape(self):
        self.assertTrue(is_native_tool_error_response(400, 'Unknown field "parts" in FunctionResponse[0]'))
        self.assertTrue(is_native_tool_error_response(422, b'{"errors":[{"message":"Unknown field in toolConfig"}]}'))
        for status, payload in ((400, '{"error":' + REJECTION), (400, REJECTION + " " * 16384),
                                (400, json.dumps({"example": {"error": REJECTION}})),
                                (401, REJECTION), (403, REJECTION), (429, REJECTION), (503, REJECTION),
                                (200, json.dumps(success({"text": REJECTION})))):
            with self.subTest(status=status, prefix=payload[:32]):
                self.assertFalse(is_native_tool_error_response(status, payload))
