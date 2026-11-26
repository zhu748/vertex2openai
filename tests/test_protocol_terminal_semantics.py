"""Offline wire contracts for Gemini generation outcomes shared by the bridges."""
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest import mock

import httpx
from google.genai import types

from claude_protocol import openai_completion_to_anthropic
from routes import chat_api
from test_req_round15_contracts import Round15Harness, chunks, request, response
from upstreams import cookie_delivery, cookie_proxy
from upstreams.cookie_native import NativeAssembler
from upstreams.cookie_response import extract_events


def wire(parts, finish):
    return {"results": [{"data": {"candidates": [{"content": {"parts": parts}, "finishReason": finish}]}}]}


class TerminalSemanticsTests(Round15Harness):
    async def express_modes(self, source):
        async def generate(**kwargs):
            return source

        async def stream(**kwargs):
            async def records():
                yield source
            return records()

        async def invoke(req, http_request):
            return await self.express(req, generate=generate, stream=stream)

        self.http_request.headers = {}
        self.http_request.query_params = {}
        with mock.patch.object(chat_api, "_call_upstream", invoke):
            yield "json", await chat_api.chat_completions(self.http_request, request(), "test")
            for fake in (False, True):
                self.settings["fake_streaming"] = fake
                yield "fake" if fake else "live", await chat_api.chat_completions(self.http_request, request(stream=True), "test")

    async def test_continuation_is_length_across_actual_delivery_modes(self):
        source = response(types.Part(text="partial"), finish=types.FinishReason.CONTINUATION)
        async for mode, reply in self.express_modes(source):
            with self.subTest(mode=mode):
                self.assertEqual(reply.status_code, 200)
                if mode == "json":
                    body = json.loads(reply.body)
                    self.assertEqual(body["choices"][0]["finish_reason"], "length")
                    self.assertEqual(openai_completion_to_anthropic(body, "model", "id")["stop_reason"], "max_tokens")
                else:
                    body = await self.body(reply)
                    self.assertEqual([c["finish_reason"] for f in chunks(body) for c in f["choices"]
                                      if c["finish_reason"] is not None], ["length"])
                    self.assertEqual(body.count("data: [DONE]"), 1)
        # Include a diagnostic-only Cookie candidate: even without text the token cap isn't STOP.
        for parts in ([{"text": "partial"}], []):
            async def events(*args, **kwargs):
                for event in extract_events(wire(parts, "CONTINUATION")):
                    yield event
            with self.subTest(cookie_parts=parts):
                _, body = await self.cookie(request(stream=True), events, settings={"fake_streaming": False})
                self.assertEqual([c["finish_reason"] for f in chunks(body) for c in f["choices"]
                                  if c["finish_reason"] is not None], ["length"])

    async def test_aborted_generation_never_advertises_an_executable_batch(self):
        call = {"name": "echo", "args": {"word": "hello"}}
        for reason in ("TOO_MANY_TOOL_CALLS", "LANGUAGE", "OTHER"):
            src = response(types.Part(function_call=types.FunctionCall(**call)), finish=reason)
            async for mode, reply in self.express_modes(src):
                with self.subTest(reason=reason, mode=mode):
                    if hasattr(reply, "body_iterator"):
                        body = await self.body(reply)
                        frames = chunks(body)
                        self.assertEqual([int(f["error"]["code"]) for f in frames if "error" in f], [502])
                        self.assertFalse(any(c.get("delta", {}).get("tool_calls") or c.get("finish_reason") in
                                             ("tool_calls", "stop") for f in frames for c in f.get("choices", [])))
                    else:
                        self.assertEqual(reply.status_code, 502)
                        self.assertIn(reason, json.loads(reply.body)["error"]["message"])
        for reason in ("TOO_MANY_TOOL_CALLS", "LANGUAGE", "OTHER", "FUTURE_FINISH"):
            raw = wire([{"functionCall": call}], reason)
            async def events(*args, **kwargs):
                for event in extract_events(raw):
                    yield event
            with self.subTest(cookie_reason=reason):
                _, body = await self.cookie(request(stream=True), events, settings={"fake_streaming": False})
                frames = chunks(body)
                self.assertEqual([int(f["error"]["code"]) for f in frames if "error" in f], [502])
                self.assertNotIn("echo", body)
                # The native contract sends Gemini's actual outcome, not an OpenAI interpretation.
                native = NativeAssembler("model")
                for kind, data in extract_events(raw, preserve_parts=True):
                    native.feed(kind, data)
                self.assertEqual(native.payload()["candidates"][0]["finishReason"], reason)


    async def test_cookie_json_uses_terminal_result_not_apparent_call_or_default_stop(self):
        for reason, parts, status, finish in (
            ("CONTINUATION", [{"text": "partial"}], 200, "length"),
            ("CONTINUATION", [], 200, "length"),
            ("SAFETY", [], 200, "content_filter"),
            ("TOO_MANY_TOOL_CALLS", [{"functionCall": {"name": "echo", "args": {}}}], 502, None),
            ("FUNCTION_CALL", [], 502, None),
        ):
            @asynccontextmanager
            async def opened(*args, **kwargs):
                yield SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(200, json=wire(parts, reason))))

            with self.subTest(reason=reason, status=status, parts=parts), \
                 mock.patch.object(cookie_delivery, "open_upstream_client", opened), \
                 mock.patch.object(cookie_proxy.credential_manager, "record_cookie_success"):
                reply = await cookie_proxy.CookieProxyUpstream().chat_completions(
                    request(), self.http_request,
                    credential_record={"id": "offline", "cookie": "SAPISID=offline", "project_id": "offline"})
                self.assertEqual(reply.status_code, status)
                body = json.loads(reply.body)
                if status == 200:
                    self.assertEqual(body["choices"][0]["finish_reason"], finish)
                else:
                    self.assertNotIn("choices", body)
                    self.assertIn("error", body)
