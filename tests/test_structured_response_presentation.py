"""The safety score display must never change a requested JSON document."""
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest import mock

import httpx
from google.genai import types

from claude_protocol import anthropic_to_openai_request, openai_completion_to_anthropic
from test_req_round15_contracts import MODEL, Round15Harness, SAFETY_RATINGS, content, request, response
from upstreams import cookie_delivery, cookie_proxy


class StructuredPresentationTests(Round15Harness):
    raw_text = '{"a":1}'

    async def test_express_normal_fake_and_true_stream(self):
        source = response(types.Part(text=self.raw_text))
        source.candidates[0].safety_ratings = [types.SafetyRating.model_validate(SAFETY_RATINGS[0])]

        async def generate(**_):
            return source

        async def stream(**_):
            async def sequence():
                yield source
            return sequence()

        self.settings["safety_score"] = True
        for fmt in ({"type": "json_object"}, {"type": "json_schema",
                    "json_schema": {"name": "sample", "schema": {"type": "object"}}}, {"type": "text"}):
            for streaming, fake in ((False, False), (True, False), (True, True)):
                with self.subTest(fmt=fmt["type"], stream=streaming, fake=fake):
                    self.settings["fake_streaming"] = fake
                    result = await self.express(request(stream=streaming, response_format=fmt), generate, stream)
                    text = (content(await self.body(result)) if streaming else
                            json.loads(result.body)["choices"][0]["message"]["content"])
                    if fmt["type"] == "text":
                        self.assertIn("Safety Ratings", text)
                    else:
                        self.assertEqual(json.loads(text), {"a": 1})

    async def test_anthropic_format_through_cookie_collect_and_stream(self):
        self.settings.update(safety_score=True, cookie_structured_output=True)
        req = anthropic_to_openai_request({"model": MODEL, "max_tokens": 20,
            "messages": [{"role": "user", "content": "hello"}],
            "output_config": {"format": {"type": "json_schema", "schema": {"type": "object"}}}}, MODEL)
        candidate = {"content": {"role": "model", "parts": [{"text": self.raw_text}]},
                     "safetyRatings": SAFETY_RATINGS, "finishReason": "STOP"}
        @asynccontextmanager
        async def open_client(*args, **kwargs):
            yield SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(200,
                json={"results": [{"data": {"candidates": [candidate]}}]})))
        with mock.patch.object(cookie_delivery, "open_upstream_client", open_client):
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(req, self.http_request,
                credential_record={"id": "offline", "cookie": "SAPISID=offline", "project_id": "offline"})
        self.assertEqual(result.status_code, 200)
        msg = openai_completion_to_anthropic(json.loads(result.body), MODEL, "msg_test")
        self.assertEqual(json.loads(msg["content"][0]["text"]), {"a": 1})

        async def events(*_, **kwargs):
            yield "text", self.raw_text
            yield "safety", SAFETY_RATINGS
            yield "finish", "STOP"
        self.settings["fake_streaming"] = False
        _, body = await self.cookie(req.model_copy(update={"stream": True}), events)
        self.assertEqual(json.loads(content(body)), {"a": 1})
