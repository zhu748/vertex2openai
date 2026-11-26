"""Ordinary clients can still get genuine tool signatures without a replay slot."""
import asyncio
import json
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

from admission import generation_limiter
from resumable import ResumableStore
from routes import chat_api
from test_parallel_tool_controls import _request
from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import express_sdk


SIGNATURE = "AP9maXh0dXJl"
CALL = {"functionCall": {"id": "wire-id", "name": "weather", "args": {"city": "Paris"}},
        "thoughtSignature": SIGNATURE}
MODEL = "gemini-3-flash-preview"


class OptionalReplayCapacityTests(SchemaRouteHarness):
    async def test_only_fresh_implicit_requests_go_direct_while_unrelated_receipt_is_live(self):
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        baseline = generation_limiter.active
        self.settings.update(resumable_enabled=True, resumable_max_streams=1,
                             upstream_max_concurrency=baseline + 2)
        del self.client.headers["x-resumable"]
        started, release = asyncio.Event(), asyncio.Event()
        arrivals = []

        async def respond(request):
            payload = json.loads(request.content)
            prompt = payload["contents"][0]["parts"][0]["text"]
            arrivals.append((prompt, payload))
            if prompt == "hold the only replay slot":
                started.set()
                await release.wait()
            parts = [CALL] if len(payload["contents"]) == 1 else [{"text": "tool complete"}]
            result = {"candidates": [{"content": {"role": "model", "parts": parts}, "finishReason": "STOP"}]}
            if ":streamGenerateContent" in request.url.path:
                return httpx.Response(200, text=f"data: {json.dumps(result)}\n\n",
                                      headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=result)

        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(respond)}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)

        def prepare(prompt, stream):
            path, body = _request("openai", "auto", stream=stream)
            body.update(model=MODEL, messages=[{"role": "user", "content": prompt}])
            return path, body

        path, held = prepare("hold the only replay slot", True)
        headers = {"Idempotency-Key": "retained-fixture"}
        with patch.object(chat_api, "resumable_store", store), \
                patch.object(express_sdk, "get_pooled_express_client", return_value=sdk):
            first = asyncio.create_task(self.client.post(path, json=held, headers=headers))
            try:
                await asyncio.wait_for(started.wait(), 1)
                old, = store._buffers.values()
                self.assertEqual(generation_limiter.active, baseline + 1)
                for stream in (False, True):
                    with self.subTest(stream=stream):
                        path, body = prepare(f"fresh {stream}", stream)
                        total = len(arrivals)
                        response = await self.client.post(path, json=body)
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.headers["x-resumable"], "false")
                        self.assertNotIn("x-resume-id", response.headers)
                        self.assertNotIn("x-resumable-replay", response.headers)
                        self.assertEqual(len(arrivals), total + 1)
                        if stream:
                            self.assertEqual(response.text.count("data: [DONE]"), 1)
                            self.assertNotIn("\nid: ", response.text)
                            calls = [c for row in chunks(response.text) for choice in row["choices"]
                                     for c in choice["delta"].get("tool_calls", [])]
                            call, = calls
                            call.pop("index")
                        else:
                            call, = response.json()["choices"][0]["message"]["tool_calls"]
                        self.assertEqual(call["extra_content"]["google"]["thought_signature"], SIGNATURE)
                        body["messages"].extend((
                            {"role": "assistant", "tool_calls": [call]},
                            {"role": "tool", "tool_call_id": call["id"], "content": "sunny"},
                        ))
                        replay = await self.client.post(path, json=body)
                        self.assertEqual(replay.status_code, 200)
                        self.assertEqual(len(arrivals), total + 2)
                        tool_step, = [p for t in arrivals[-1][1]["contents"] for p in t["parts"]
                                      if "functionCall" in p]
                        self.assertEqual((tool_step["functionCall"]["id"], tool_step["thoughtSignature"]),
                                         ("wire-id", SIGNATURE))
                        self.assertIs(await store.get_exact(old.id), old)
                        self.assertFalse(old.done)
                        self.assertEqual(generation_limiter.active, baseline + 1)

                path, explicit = prepare("explicit newer request", True)
                total = len(arrivals)
                for opts, status in (({"X-Resumable": "true"}, 503),
                                     ({"Idempotency-Key": "fresh-key"}, 503),
                                     ({"X-Idempotency-Key": "fresh-x-key"}, 503),
                                     ({"X-Resume-Id": "rid-missing"}, 404)):
                    with self.subTest(opts=opts):
                        denied = await self.client.post(path, json=explicit, headers=opts)
                        self.assertEqual(denied.status_code, status)
                        self.assertNotIn("x-resumable", denied.headers)
                        self.assertEqual(len(arrivals), total)

                self.settings["upstream_max_concurrency"] = baseline + 1
                _, too_busy = prepare("no generation slot", True)
                blocked = await self.client.post(path, json=too_busy)
                self.assertEqual(blocked.status_code, 503)
                self.assertIn("上游生成并发", blocked.json()["error"]["message"])
                self.assertEqual(len(arrivals), total)
            finally:
                release.set()
                original = await asyncio.wait_for(first, 2)
            self.assertEqual(original.status_code, 200)
            self.assertEqual(original.headers["x-resume-id"], old.id)
            duplicate = await self.client.post(path, json=held, headers=headers)
            self.assertEqual(duplicate.headers["x-resume-id"], old.id)
            self.assertEqual(duplicate.headers["x-resumable-replay"], "1")
            self.assertEqual(len([p for p, _ in arrivals if p == "hold the only replay slot"]), 1)
            self.assertEqual(generation_limiter.active, baseline)
