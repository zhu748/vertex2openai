"""Candidate finish is not superseded by a later model part or conflicting finish."""
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx

from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness
from upstreams import cookie_delivery, cookie_native


def candidate(reason, text=None):
    row = {"index": 0, "finishReason": reason}
    if text is not None:
        row["content"] = {"role": "model", "parts": [{"text": text}]}
    return {"data": {"candidates": [row]}}


def endpoint(dialect, mode):
    if dialect == "gemini":
        action = {"json": "generateContent", "array": "streamGenerateContent",
                  "sse": "streamGenerateContent?alt=sse"}[mode]
        return f"/v1beta/models/{MODEL}:{action}", {"contents": [{"parts": [{"text": "hi"}]}]}
    return ("/v1/chat/completions" if dialect == "openai" else "/v1/messages",
            {"model": MODEL, "messages": MESSAGES, "stream": mode != "json",
             **({"max_tokens": 32} if dialect == "claude" else {})})


class CookieCandidateFinish(SchemaRouteHarness):
    async def assert_presentations(self, doc, valid, joined):
        self.channel = "web_proxy"
        calls = []

        def respond(_):
            calls.append(1)
            return httpx.Response(200, json=doc)

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            @asynccontextmanager
            async def opened(*args, **kwargs):
                yield client

            with (patch.object(cookie_delivery, "open_upstream_client", opened),
                  patch.object(cookie_native, "open_upstream_client", opened)):
                for dialect in ("openai", "claude", "gemini"):
                    for mode in (("json", "array", "sse") if dialect == "gemini" else ("json", "true", "fake")):
                        calls.clear()
                        self.settings["fake_streaming"] = mode == "fake"
                        path, body = endpoint(dialect, mode)
                        with self.subTest(dialect=dialect, mode=mode):
                            res = await self.client.post(path, json=body)
                            self.assertEqual(len(calls), 1)
                            text = res.text
                            self.assertNotIn("late-part", text)
                            if valid:
                                self.assertEqual(res.status_code, 200, text[:150])
                                self.assertNotIn('"error"', text)
                                self.assertIn("first-part", text)
                                self.assertIn({"openai": '"length"', "claude": '"max_tokens"',
                                               "gemini": '"MAX_TOKENS"'}[dialect], text)
                            else:
                                self.assertEqual(res.status_code, 502 if mode == "json" else 200, text[:150])
                                self.assertIn('"error"', text)
                                self.assertNotIn({"openai": '"stop"', "claude": '"end_turn"',
                                                  "gemini": '"STOP"'}[dialect], text)
                                # Already emitted data cannot be retracted. A
                                # bad suffix never gets sent as part of it.
                                if not joined and mode in ("array", "sse", "true"):
                                    self.assertIn("first-part", text)

    async def test_late_and_valid_tail_records(self):
        first = candidate("MAX_TOKENS", "first-part")
        for tail, valid in ((candidate("STOP", "late-part"), False), (candidate("STOP"), False),
                            (candidate("MAX_TOKENS"), True), ({"data": {"usageMetadata": {"totalTokenCount": 4}}}, True)):
            for joined in (True, False):
                with self.subTest(tail=tail, joined=joined):
                    wire = ({"results": [first, tail]} if joined
                            else [{"results": [first]}, {"results": [tail]}])
                    await self.assert_presentations(wire, valid, joined)
