"""An upstream continuation is not equivalent to reprompting or resuming local SSE delivery."""
from contextlib import asynccontextmanager
import json
from unittest.mock import patch

import httpx

from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from upstreams import cookie_native, native_passthrough

BASE = {"contents": [{"parts": [{"text": "start"}]}]}
ACTIONS = ("generateContent", "streamGenerateContent", "streamGenerateContent?alt=sse")


class NativeContinuation(SchemaRouteHarness):
    async def test_requested_native_token_never_consumes_account_or_reissues_original_prompt(self):
        self.channel = "web_proxy"
        for key in ("continuationToken", "continuation_token"):
            for action in ACTIONS:
                for token in ("dGVzdA==", ""):
                    with self.subTest(key=key, action=action, empty=not token), patch.object(
                        cookie_native, "_handle_native_request", side_effect=AssertionError("admitted generation")
                    ):
                        r = await self.client.post(f"/v1beta/models/{MODEL}:{action}", json={**BASE, key: token})
                        self.assertEqual(r.status_code, 400)
                        self.assertEqual(r.json()["error"]["status"], "INVALID_ARGUMENT")
                        self.assertIn("continuationToken", r.json()["error"]["message"])
                        self.assertEqual(self.sent, [])
        for key in ("continuationToken", "continuation_token"):
            r = await self.client.post(f"/v1beta/models/{MODEL}:generateContent", json={**BASE, key: None})
            self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self.sent), 2)

    async def test_upstream_continuation_is_an_explicit_terminal_failure_and_is_not_retried(self):
        self.channel = "web_proxy"
        self.settings["retry_max"] = 2
        for action in ACTIONS:
            for fields in ({"finishReason": "CONTINUATION", "continuationToken": "opaque-fixture"},
                           {"finishReason": "CONTINUATION"}, {"finishReason": "STOP", "continuationToken": "opaque-fixture"}):
                calls = []
                def reply(req):
                    calls.append(req)
                    candidate = {"content": {"role": "model", "parts": [{"text": "incomplete output"}]}, **fields}
                    return httpx.Response(200, json={"results": [{"data": {"candidates": [candidate]}}]})
                async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as upstream:
                    @asynccontextmanager
                    async def open_studio(*args, **kwargs):
                        yield upstream
                    with patch.object(cookie_native, "open_upstream_client", open_studio):
                        r = await self.client.post(f"/v1beta/models/{MODEL}:{action}", json=BASE)
                with self.subTest(action=action, fields=fields):
                    if action == "generateContent":
                        self.assertEqual(r.status_code, 502)
                        frames = [r.json()]
                    elif action.endswith("alt=sse"):
                        self.assertEqual(r.status_code, 200)
                        frames = [json.loads(line[6:]) for line in r.text.splitlines() if line.startswith("data: ")]
                    else:
                        self.assertEqual(r.status_code, 200)
                        frames = r.json()
                    self.assertEqual(len(calls), 1)
                    self.assertEqual(len(frames), 1)
                    self.assertEqual(frames[0]["error"]["code"], 502)
                    self.assertIn("continuationToken", frames[0]["error"]["message"])
                    self.assertNotIn("opaque-fixture", r.text)
                    self.assertNotIn("incomplete output", r.text)

    async def test_native_sse_keeps_already_emitted_parts_before_late_continuation_failure(self):
        self.channel = "web_proxy"
        def row(candidate):
            return {"results": [{"data": {"candidates": [candidate]}}]}
        earlier = {"content": {"parts": [{"text": "earlier"}]}}
        later = {"content": {"parts": [{"text": "incomplete"}]}, "finishReason": "CONTINUATION"}
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda req: httpx.Response(200, json=[row(earlier), row(later)]))) as upstream:
            @asynccontextmanager
            async def open_studio(*args, **kwargs):
                yield upstream
            with patch.object(cookie_native, "open_upstream_client", open_studio):
                r = await self.client.post(f"/v1beta/models/{MODEL}:streamGenerateContent?alt=sse", json=BASE)
        frames = [json.loads(line[6:]) for line in r.text.splitlines() if line.startswith("data: ")]
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[0]["candidates"][0]["content"]["parts"], [{"text": "earlier"}])
        self.assertEqual(frames[1]["error"]["code"], 502)

    async def test_express_native_remains_byte_passthrough(self):
        self.channel = "api_key"
        wire = b'{ "contents": [], "continuationToken": "dGVzdA==" }'
        reply_body = b'{"candidates":[{"finishReason":"CONTINUATION","continuationToken":"dGVzdA=="}]}'
        seen = []

        def serve(req):
            seen.append(req.content)
            return httpx.Response(200, content=reply_body, headers={"content-type": "application/json"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as upstream:
            with patch.object(native_passthrough.passthrough_client_pool, "get", return_value=upstream):
                r = await self.client.post(f"/v1beta/models/{MODEL}:generateContent", content=wire)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(seen, [wire])
        self.assertEqual(r.content, reply_body)
