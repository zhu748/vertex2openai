"""Mock both upstreams at their dispatch boundary; never contact a live model."""
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest import mock

import httpx
from google.genai import types

from models import OpenAIRequest
from request_validation import validate_openai_request
from schema_validation import SchemaValidationError
from test_req_round15_contracts import Round15Harness, response
from upstreams import cookie_delivery, cookie_proxy

MODEL = "gemini-2.5-flash"


class HistoryWireHarness(Round15Harness):
    async def capture_history(self, messages, adapter, **fields):
        req = OpenAIRequest(model=MODEL, messages=messages, **fields)
        if adapter == "express":
            generate = mock.AsyncMock(return_value=response(types.Part(text="reply")))
            result = await self.express(req, generate)
            self.assertEqual(result.status_code, 200)
            generate.assert_awaited_once()
            return [c.model_dump(by_alias=True, exclude_none=True)
                    for c in generate.await_args.kwargs["contents"]]

        post = mock.AsyncMock(return_value=httpx.Response(200, json={"results": [{"data": {
            "candidates": [{"content": {"role": "model", "parts": [{"text": "reply"}]},
                            "finishReason": "STOP"}]}}]}))

        @asynccontextmanager
        async def client(*args, **kwargs):
            yield SimpleNamespace(post=post)

        with mock.patch.object(cookie_delivery, "open_upstream_client", client):
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(
                req, self.http_request, credential_record={"id": "offline", "cookie": "SAPISID=offline",
                                                          "project_id": "offline"})
        self.assertEqual(result.status_code, 200)
        post.assert_awaited_once()
        return post.await_args.kwargs["json"]["variables"]["contents"]


class AssistantRefusalHistoryTests(HistoryWireHarness):
    async def test_refusal_stays_in_its_assistant_turn_between_real_user_messages(self):
        refusal = "I cannot do that."
        typed = {"type": "refusal", "refusal": refusal}
        before = {"type": "text", "text": "before"}
        after = {"type": "text", "text": "after"}
        cases = [
            ({"content": None, "refusal": refusal}, [refusal]),
            ({"content": [typed]}, [refusal]),
            ({"content": [before, typed, after], "refusal": refusal}, ["before", refusal, "after"]),
            ({"content": refusal, "refusal": refusal}, [refusal]),
            ({"content": [before, {"type": "text", "text": refusal}], "refusal": refusal}, ["before", refusal]),
            ({"content": [typed], "refusal": "another reason"}, [refusal, "another reason"]),
            ({"content": "before", "refusal": refusal}, ["before", refusal]),
        ]
        for adapter in ("express", "cookie"):
            for assistant, expected in cases:
                with self.subTest(adapter=adapter, assistant=assistant):
                    got = await self.capture_history([
                        {"role": "user", "content": "A"}, {"role": "assistant", **assistant},
                        {"role": "user", "content": [after, {"type": "refusal", "refusal": "not model text"}],
                         "refusal": "not assistant metadata"},
                    ], adapter)
                    self.assertEqual(got, [
                        {"role": "user", "parts": [{"text": "A"}]},
                        {"role": "model", "parts": [{"text": s} for s in expected]},
                        {"role": "user", "parts": [{"text": "after"}]},
                    ])

    def test_reject_malformed_refusal_or_conflicting_exact_replay(self):
        for assistant in (
            {"content": [{"type": "refusal", "refusal": 3}]},
            {"content": None, "refusal": "no", "extra_content": {"google": {
                "ordinary_parts": [{"kind": "text", "text": "different"}]}}},
        ):
            with self.subTest(assistant=assistant), self.assertRaises(SchemaValidationError):
                validate_openai_request(OpenAIRequest(model=MODEL, messages=[{"role": "assistant", **assistant}]))

    async def test_standalone_refusal_is_history_not_a_continuation_request(self):
        for adapter in ("express", "cookie"):
            for assistant in ({"content": None, "refusal": "no"},
                              {"content": [{"type": "refusal", "refusal": "no"}]}):
                with self.subTest(adapter=adapter, assistant=assistant):
                    self.assertEqual(await self.capture_history([{"role": "assistant", **assistant}], adapter),
                                     [{"role": "model", "parts": [{"text": "no"}]}])
