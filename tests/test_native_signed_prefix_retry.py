"""Native Cookie attempt boundaries, through ASGI and the pinned Gemini SDK."""
from contextlib import asynccontextmanager
import base64
import json
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

from main import app
from response_validation import PendingMetadataGate
from test_cookie_native_attempt_metadata import RETRY, block
from test_cookie_native_signed_part_payloads import ACTIONS, MODEL, PROMPT, ROUTE, frames, model_parts
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_native


LOSING = [{"thoughtSignature": "AQIDBA=="}, {"text": "", "thought": True, "thoughtSignature": "BQYHCA=="}]
WINNING = [{"thoughtSignature": "CQoLDA=="}, {"text": "", "thought": True, "thoughtSignature": "+/8="}]
REPLY = {"text": "完成", "thoughtSignature": "DQ4="}
FINAL = {"promptTokenCount": 3, "candidatesTokenCount": 1}


def candidate(*parts, finish=None):
    out = {"content": {"role": "model", "parts": list(parts)}}
    if finish is not None:
        out["finishReason"] = finish
    return out


def failed_prefix():
    return [block(usageMetadata={"promptTokenCount": 99}),
            *(block(candidate(part)) for part in LOSING), RETRY]


def winning_reply():
    return [block(candidate(WINNING[0]), usageMetadata={"promptTokenCount": 3}),
            block(candidate(WINNING[1])),
            block(candidate(REPLY, finish="STOP"), usageMetadata={"candidatesTokenCount": 1})]


class NativeSignedPrefixRetry(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.channel = "web_proxy"

    @asynccontextmanager
    async def attempts(self, plans, *, retry_max=1):
        requests = []

        def respond(request):
            requests.append(json.loads(request.content)["variables"]["contents"])
            return httpx.Response(200, json=plans[len(requests) - 1])

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            @asynccontextmanager
            async def opened(*args, **kwargs):
                yield client

            with (patch.object(cookie_native, "open_upstream_client", opened),
                  patch.object(cookie_native, "get_retry_settings", return_value=(retry_max, 0, 0))):
                yield requests

    async def test_partial_signatures_from_retrying_attempt_do_not_escape(self):
        for action in ACTIONS:
            with self.subTest(action=action):
                async with self.attempts([failed_prefix(), winning_reply()]) as requests:
                    response = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                self.assertEqual(len(requests), 2, response.text)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(model_parts(response, action), [*WINNING, REPLY])
                self.assertEqual(frames(response, action)[-1]["usageMetadata"], FINAL)
                for losing in LOSING:
                    self.assertNotIn(losing["thoughtSignature"], response.text)

    async def test_no_retry_budget_or_fatal_error_never_publishes_undeserved_prefix(self):
        for plan, max_retry, code in ((failed_prefix(), 0, 503),
                                      ([block(candidate(LOSING[0])), {"error": {"code": 400, "message": "fixture"}}],
                                       1, 400)):
            for action in ACTIONS:
                with self.subTest(action=action, code=code):
                    async with self.attempts([plan], retry_max=max_retry) as requests:
                        response = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                    self.assertEqual(len(requests), 1)
                    self.assertEqual([frame["error"]["code"] for frame in frames(response, action)], [code])
                    self.assertNotIn("thoughtSignature", response.text)

    async def test_signed_only_complete_stop_and_candidate_refusal_retain_original_slots(self):
        for finish in ("STOP", "SAFETY"):
            for action in ACTIONS:
                with self.subTest(action=action, finish=finish):
                    plan = [block(candidate(WINNING[0])), block(candidate(WINNING[1], finish=finish), usageMetadata=FINAL)]
                    async with self.attempts([plan]) as requests:
                        response = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                    self.assertEqual(len(requests), 1)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(model_parts(response, action), WINNING)
                    self.assertEqual(frames(response, action)[-1]["usageMetadata"], FINAL)
                    self.assertEqual(frames(response, action)[-1]["candidates"][0]["finishReason"], finish)

    async def test_real_semantic_part_commits_before_error_and_never_replays(self):
        semantic = [REPLY,
                    {"functionCall": {"name": "lookup", "args": {}}},
                    {"functionResponse": {"name": "lookup", "response": {}}},
                    {"inlineData": {"mimeType": "audio/wav", "data": "AQI="}},
                    {"executableCode": {"language": "PYTHON", "code": "1 + 1"}},
                    {"toolCall": {"toolType": "GOOGLE_SEARCH_WEB", "args": {"query": "local fixture"}}}]
        for part in semantic:
            for action in ACTIONS:
                with self.subTest(action=action, part=next(iter(part))):
                    plan = [block(candidate(LOSING[0])), block(candidate(part)), RETRY]
                    async with self.attempts([plan, winning_reply()]) as requests:
                        response = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                    self.assertEqual(len(requests), 1)
                    self.assertEqual(frames(response, action)[-1]["error"]["code"], 503)
                    if action.startswith("stream"):
                        self.assertIn(LOSING[0]["thoughtSignature"], response.text)

    async def test_the_outer_prefix_budget_covers_signed_parts_before_terminal(self):
        signature = {"thoughtSignature": base64.b64encode(b"x" * 60).decode()}
        plan = [block(candidate(signature)), block(candidate(signature)), block(candidate(finish="STOP"))]
        for action in ACTIONS:
            with self.subTest(action=action), patch.object(PendingMetadataGate, "LIMIT_BYTES", 270):
                async with self.attempts([plan]) as requests:
                    response = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                self.assertEqual(len(requests), 1)
                self.assertEqual([frame["error"]["code"] for frame in frames(response, action)], [502])
                self.assertNotIn("thoughtSignature", response.text)

    async def test_actual_gemini_sdk_receives_and_echoes_only_the_winning_original_bytes(self):
        sdk = genai.Client(api_key="test-key", http_options=types.HttpOptions(
            base_url="http://local", api_version="v1beta",
            async_client_args={"transport": httpx.ASGITransport(app=app)}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        prompt = types.Content(role="user", parts=[types.Part(text="offline fixture")])
        follow = types.Content(role="user", parts=[types.Part(text="continue")])
        expected = [*WINNING, REPLY]
        final = [block(candidate({"text": "next"}, finish="STOP"))]
        async with self.attempts([failed_prefix(), winning_reply(), final]) as requests:
            initial = await sdk.aio.models.generate_content(model=MODEL, contents=[prompt])
            history = initial.candidates[0].content
            self.assertEqual([part.thought_signature for part in history.parts],
                             [base64.b64decode(item["thoughtSignature"]) for item in expected])
            self.assertEqual([(part.text, part.thought) for part in history.parts],
                             [(item.get("text"), item.get("thought")) for item in expected])
            reply = await sdk.aio.models.generate_content(model=MODEL, contents=[prompt, history, follow])
            self.assertEqual(reply.text, "next")
        self.assertEqual(len(requests), 3)
        self.assertEqual(requests[-1][1]["parts"], expected)
