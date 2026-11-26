"""A later accepted but uncacheable reply must fence earlier scoped bytes."""
import base64
import json
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

import signature_origin
import test_bounded_signature_origins as proof
from test_claude_omitted_signature_projection import new_cache
from test_claude_buffered_tool_order import read_events
from test_express_candidate_terminal import candidate, packet
from test_parallel_tool_controls import _request
from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery, express_sdk


TOOLS_ID = "unchanged-public-tool"
SIG_A, SIG_B = b"provider-reply-A", b"provider-reply-B"


def completed_parts(original, case):
    signature = SIG_A
    if not original:
        signature = SIG_B if case != "oversize" else b"B" * (signature_origin._MAX_SIGNATURE_BYTES + 1)
    parts = [{"text": "checking"}, proof.provider_call(TOOLS_ID, signature)]
    if not original and case == "empty_tail":
        parts.append({"text": "", "thoughtSignature": base64.b64encode(b"original-empty-tail").decode()})
    return parts


class StaleSignatureRoutes(SchemaRouteHarness):
    echo = proof.BoundedOriginRoutes.assistant_echo

    @asynccontextmanager
    async def provider(self):
        self.provider_finish = "STOP"
        self.provider_trailer = False

        def reply(request):
            body = json.loads(request.content)
            self.sent.append(body)
            frame = {"candidates": [{"content": {"role": "model", "parts": self.reply},
                                     "finishReason": self.provider_finish}]}
            records = [frame]
            if self.provider_trailer:
                records.append({"candidates": [{"content": {"parts": [{"text": "illegal trailing content"}]}}]})
            if "variables" in body:
                envelopes = [{"results": [{"data": record}]} for record in records]
                return httpx.Response(200, json=envelopes if self.provider_trailer else envelopes[0])
            if ":streamGenerateContent" in request.url.path:
                return httpx.Response(200, text="".join(f"data: {json.dumps(record)}\n\n" for record in records),
                                      headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=frame)

        transport = httpx.MockTransport(reply)
        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": transport}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        async with httpx.AsyncClient(transport=transport) as studio:
            @asynccontextmanager
            async def context(*args, **kwargs):
                yield studio

            with patch.object(express_sdk, "get_pooled_express_client", return_value=sdk), \
                    patch.object(cookie_delivery, "open_upstream_client", context):
                yield

    async def generate(self, path, body, parts):
        self.reply = parts
        result = await self.client.post(path, json=body)
        self.assertEqual(result.status_code, 200, result.text[:400])
        self.assertFalse(any("error" in row for row in chunks(result.text)) if body["stream"] else False)
        return result

    async def follow(self, path, body, protocol, assistant):
        messages = [*body["messages"], assistant, *proof.results(protocol, (TOOLS_ID,), "ok")]
        self.reply = [{"text": "done"}]
        self.sent.clear()
        result = await self.client.post(path, json={**body, "messages": messages, "stream": False})
        self.assertEqual(result.status_code, 200, result.text[:400])
        self.assertEqual(len(self.sent), 1)
        part, = proof.wire_calls(self.sent[0])
        self.assertEqual(part["functionCall"]["id"], TOOLS_ID)
        return part.get("thoughtSignature")

    async def test_accepted_unrestorable_generations_never_borrow_cache_bytes(self):
        self.settings["max_retries"] = 0
        for self.channel in ("api_key", "web_proxy"):
            for dialect, protocol in (("openai", "openai"), ("shown", "claude"), ("omitted", "claude")):
                for mode in ("json", "fake", "stream"):
                    for case in ("empty_tail", "oversize"):
                        with self.subTest(channel=self.channel, dialect=dialect, mode=mode, case=case), ExitStack() as stack:
                            new_cache(stack)
                            self.settings["fake_streaming"] = mode == "fake"
                            path, body = _request(protocol, "auto", stream=mode != "json")
                            body.update(model=proof.MODEL, messages=[{"role": "user", "content": "Weather?"}])
                            if dialect == "omitted":
                                body["thinking"] = {"type": "enabled", "budget_tokens": 1024, "display": "omitted"}
                            first = await self.generate(path, body, completed_parts(True, case))
                            before = self.echo(first, protocol, mode)
                            self.assertEqual(await self.follow(path, body, protocol, before),
                                             base64.b64encode(SIG_A).decode())
                            separate = {**body, "messages": [{"role": "user", "content": "Another question"}]}
                            separate_reply = await self.generate(path, separate, completed_parts(True, case))
                            independent = self.echo(separate_reply, protocol, mode)

                            latter = await self.generate(path, body, completed_parts(False, case))
                            later = self.echo(latter, protocol, mode)
                            self.assertEqual(later, before)  # The thin client cannot distinguish the emissions.
                            # Both echoes now have an ambiguous provenance. No old
                            # signature or new bypass is sent, but default remains
                            # a regular upstream dispatch rather than a local 400.
                            self.assertIsNone(await self.follow(path, body, protocol, later))
                            self.assertIsNone(await self.follow(path, body, protocol, before))
                            self.assertEqual(await self.follow(path, separate, protocol, independent),
                                             base64.b64encode(SIG_A).decode())

    async def test_finish_frame_before_failing_stream_does_not_fence_prior_reply(self):
        self.settings["max_retries"] = 0
        self.channel = "api_key"
        path, body = _request("openai", "auto", stream=True)
        body.update(model=proof.MODEL, messages=[{"role": "user", "content": "Weather?"}])
        with ExitStack() as stack:
            new_cache(stack)
            first = await self.generate(path, body, completed_parts(True, "empty_tail"))
            before = self.echo(first, "openai", "stream")

            def respond(request):
                self.assertIn(":streamGenerateContent", request.url.path)
                frames = [packet(candidate(0, *completed_parts(False, "empty_tail"), reason="STOP")),
                          packet(candidate(0, {"text": "illegal after successful finish"}))]
                return httpx.Response(200, text="".join(f"data: {json.dumps(frame)}\n\n" for frame in frames),
                                      headers={"content-type": "text/event-stream"})

            fixture = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
                async_client_args={"transport": httpx.MockTransport(respond)}))
            self.addCleanup(fixture.close)
            self.addAsyncCleanup(fixture.aio.aclose)
            with patch.object(express_sdk, "get_pooled_express_client", return_value=fixture):
                bad = await self.client.post(path, json=body)
            self.assertTrue(any("error" in row for row in chunks(bad.text)))
            self.assertEqual(await self.follow(path, body, "openai", before), base64.b64encode(SIG_A).decode())

    async def test_public_complete_tool_batches_in_length_and_refusal_fence_older_bytes(self):
        async with self.provider():
            for self.channel in ("api_key", "web_proxy"):
                for dialect, protocol in (("openai", "openai"), ("shown", "claude"), ("omitted", "claude")):
                    for mode in ("json", "fake", "stream"):
                        for terminal in ("MAX_TOKENS", "SAFETY"):
                            with self.subTest(channel=self.channel, dialect=dialect, mode=mode,
                                              terminal=terminal), ExitStack() as stack:
                                new_cache(stack)
                                self.settings["fake_streaming"] = mode == "fake"
                                path, body = _request(protocol, "auto", stream=mode != "json")
                                body.update(model=proof.MODEL, messages=[{"role": "user", "content": "Weather?"}])
                                if dialect == "omitted":
                                    body["thinking"] = {"type": "enabled", "budget_tokens": 1024, "display": "omitted"}
                                self.provider_finish = "STOP"
                                separate = {**body, "messages": [{"role": "user", "content": "Separate weather?"}]}
                                prior = self.echo(await self.generate(path, body, completed_parts(True, "ordinary")),
                                                  protocol, mode)
                                independent = self.echo(await self.generate(path, separate,
                                                        completed_parts(True, "ordinary")), protocol, mode)
                                self.assertEqual(await self.follow(path, body, protocol, prior),
                                                 base64.b64encode(SIG_A).decode())
                                self.provider_finish = terminal
                                latest = await self.generate(path, body, completed_parts(False, "ordinary"))
                                expected = ("length" if terminal == "MAX_TOKENS" else "content_filter")
                                if protocol == "claude":
                                    observed = (latest.json()["stop_reason"] if mode == "json" else
                                                read_events(latest.text)[-2]["delta"]["stop_reason"])
                                    self.assertEqual(observed, "max_tokens" if expected == "length" else "refusal")
                                else:
                                    choices = (latest.json()["choices"] if mode == "json" else
                                               [choice for frame in chunks(latest.text) for choice in frame["choices"]])
                                    self.assertEqual([choice["finish_reason"] for choice in choices
                                                      if choice["finish_reason"]], [expected])
                                later = self.echo(latest, protocol, mode)
                                self.assertEqual(later, prior)  # Standard history cannot carry the stop reason.
                                self.provider_finish = "STOP"
                                self.assertIsNone(await self.follow(path, body, protocol, later))
                                self.assertIsNone(await self.follow(path, body, protocol, prior))
                                self.assertEqual(await self.follow(path, separate, protocol, independent),
                                                 base64.b64encode(SIG_A).decode())

    async def test_invalid_trailing_or_malformed_non_success_reply_does_not_fence_previous(self):
        async with self.provider():
            for self.channel in ("api_key", "web_proxy"):
                for dialect, protocol in (("openai", "openai"), ("shown", "claude"), ("omitted", "claude")):
                    for invalid in ("trailing", "malformed"):
                        with self.subTest(channel=self.channel, dialect=dialect, invalid=invalid), ExitStack() as stack:
                            new_cache(stack)
                            path, body = _request(protocol, "auto", stream=True)
                            body.update(model=proof.MODEL, messages=[{"role": "user", "content": "Weather?"}])
                            if dialect == "omitted":
                                body["thinking"] = {"type": "enabled", "budget_tokens": 1024, "display": "omitted"}
                            self.provider_finish = "STOP"
                            before = self.echo(await self.generate(path, body, completed_parts(True, "ordinary")),
                                               protocol, "stream")
                            self.provider_finish = "MAX_TOKENS"
                            self.provider_trailer = invalid == "trailing"
                            self.reply = completed_parts(False, "ordinary")
                            if invalid == "malformed":
                                self.reply[-1]["functionCall"]["args"] = ["not an object"]
                            incomplete = await self.client.post(path, json=body)
                            if protocol == "claude" and incomplete.status_code == 200:
                                self.assertEqual(read_events(incomplete.text)[-1]["type"], "error")
                            elif incomplete.status_code == 200:
                                self.assertTrue(any("error" in frame for frame in chunks(incomplete.text)))
                            else:
                                self.assertGreaterEqual(incomplete.status_code, 400)
                            self.provider_finish = "STOP"
                            self.provider_trailer = False
                            self.assertEqual(await self.follow(path, body, protocol, before),
                                             base64.b64encode(SIG_A).decode())
