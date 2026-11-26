"""Responses terminals own tool-batch visibility on the full, buffered provider paths."""
from contextlib import asynccontextmanager
import json
from unittest.mock import patch

import httpx

import anti_truncation
import channel_dispatch
from signature_store import signature_store
from test_responses_api import MODEL, SIGNATURES, assistant, request
import test_responses_outcomes
from test_responses_typed_parts import public
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery


OPEN = {"functionCall": {"name": "count_colors", "id": "unfinished",
                         "partialArgs": [{"jsonPath": "$.shade", "stringValue": "bl", "willContinue": True}],
                         "willContinue": True}, "thoughtSignature": "cGFydGlhbA=="}


class IncompleteResponsesCalls(SchemaRouteHarness):
    post_at = test_responses_outcomes.ResponsesOutcomes.post_at

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.usage_metadata = {"promptTokenCount": 7, "candidatesTokenCount": 5, "totalTokenCount": 12}

    async def test_incomplete_candidate_keeps_only_visible_prefix_and_usage_without_registering_calls(self):
        with patch.object(channel_dispatch.credential_manager, "has_cookie_record", return_value=True):
            self.settings["cross_channel_fallback"] = True
            for self.channel in ("web_proxy", "api_key"):
                for phase in ("json", "early", "late"):
                    for self.finish_reason, reason in (("MAX_TOKENS", "max_output_tokens"), ("SAFETY", "content_filter")):
                        for parts in ([OPEN], [assistant("complete-sibling", SIGNATURES[0])],
                                      [{"text": "visible"}, assistant("complete-sibling", SIGNATURES[0]), OPEN]):
                            with self.subTest(channel=self.channel, phase=phase, parts=len(parts), finish=self.finish_reason):
                                self.sent.clear()
                                self.reply = parts
                                with patch.object(signature_store, "register_call_signature") as register:
                                    result = await self.post_at(request(stream=phase != "json"), phase)
                                    self.assertEqual(result.status_code, 200, result.text[:260])
                                    out = public(result, phase)
                                    self.assertEqual(out["status"], "incomplete")
                                    self.assertEqual(out["incomplete_details"], {"reason": reason})
                                    self.assertEqual(out["usage"]["total_tokens"], 12)
                                    self.assertEqual([i["content"][0]["text"] for i in out["output"]],
                                                     ["visible"] if len(parts) == 3 else [])
                                    for s in ("function_call_arguments", "unfinished", "complete-sibling", "cGFydGlhbA=="):
                                        self.assertNotIn(s, result.text)
                                    register.assert_not_called()
                                    self.assertEqual(len(self.sent), 1)

    async def test_cookie_later_terminal_controls_earlier_open_call_not_result_or_eof(self):
        self.channel = "web_proxy"
        def frame(parts=(), finish=None):
            return {"results": [{"data": {"candidates": [{"content": {"parts": list(parts)},
                **({"finishReason": finish} if finish else {})}]}}]}
        def usage(counts):
            return {"results": [{"data": {"usageMetadata": counts}}]}
        for finish, expected in (("SAFETY", "content_filter"), ("MAX_TOKENS", "max_output_tokens"),
                                 ("STOP", None), ("MALFORMED_FUNCTION_CALL", None), (None, None)):
            with self.subTest(finish=finish):
                rows = [usage({"promptTokenCount": 11}), frame([{"text": "prefix"}]), frame([OPEN]),
                        frame(finish=finish), usage({"candidatesTokenCount": 4, "totalTokenCount": 15})]
                transport = httpx.MockTransport(lambda _: httpx.Response(200, text="\n".join(map(json.dumps, rows))))
                async with httpx.AsyncClient(transport=transport) as client:
                    @asynccontextmanager
                    async def stub(*args, **kwargs):
                        yield client
                    with patch.object(cookie_delivery, "open_upstream_client", stub):
                        result = await self.post_at(request(), "json")
                if expected is None:
                    self.assertEqual(result.status_code, 502, result.text[:220])
                else:
                    self.assertEqual(result.status_code, 200, result.text[:220])
                    out = result.json()
                    self.assertEqual(out["incomplete_details"], {"reason": expected})
                    self.assertEqual(out["output"][0]["content"][0]["text"], "prefix")
                    self.assertEqual(out["usage"]["total_tokens"], 15)
                    self.assertNotIn("cGFydGlhbA==", result.text)

    async def test_invalid_partial_not_reclassified_by_a_token_limit(self):
        for self.channel in ("api_key", "web_proxy"):
            self.finish_reason = "MAX_TOKENS"
            for bad in (
                {"functionCall": {"name": "count_colors", "willContinue": True, "args": {"mixed": True}}},
                {"functionCall": {"name": "count_colors", "partialArgs": [{"jsonPath": "$.x", "boolValue": True,
                                                                           "stringValue": "another arm"}]}},
                {"functionCall": {"name": "count_colors", "partialArgs": [{"stringValue": "missing path"}]}},
            ):
                with self.subTest(channel=self.channel, bad=bad):
                    self.reply = [assistant("whole", SIGNATURES[0]), bad]
                    with patch.object(signature_store, "register_call_signature") as register:
                        result = await self.post_at(request(), "json")
                        self.assertEqual(result.status_code, 502, result.text[:200])
                        register.assert_not_called()

    async def test_private_text_transport_does_not_publish_real_argument_fragments(self):
        # Only generation transport name is fixed, so provider fixtures can return it.
        alias = MODEL + "-antitruncation-nonstream"
        self.settings["anti_truncation_mode"] = "nonstream"
        private = "v2o_emit_fixture"
        def fragment(name, ident, text):
            return {"functionCall": {"name": name, "id": ident, "willContinue": True,
                    "partialArgs": [{"jsonPath": "$.content", "stringValue": text, "willContinue": True}]}}
        with patch.object(anti_truncation.secrets, "token_hex", return_value="fixture"):
            for self.channel in ("api_key", "web_proxy"):
                for self.finish_reason in ("MAX_TOKENS", "SAFETY", "STOP"):
                    for scenario, parts, prefix in (
                        ("only public", [OPEN], ""),
                        ("synthetic", [fragment(private, "private", "visible")], "visible"),
                        ("separate ids", [fragment(private, "private", "v"), fragment("count_colors", "public", "hidden"),
                                          fragment(None, "public", "not text"), fragment(None, "private", "2")], "v2"),
                        ("ambiguous", [fragment(private, None, "public prefix"), fragment("count_colors", "public", "secret"),
                                       fragment(None, None, "not text")], None),
                    ):
                        with self.subTest(channel=self.channel, finish=self.finish_reason, scenario=scenario):
                            self.reply = parts
                            r = await self.post_at(request(model=alias), "json")
                            self.assertNotIn("not text", r.text)
                            self.assertNotIn("secret", r.text)
                            if self.finish_reason == "STOP" or prefix is None:
                                self.assertEqual(r.status_code, 502, r.text[:240])
                                continue
                            self.assertEqual(r.status_code, 200, r.text[:240])
                            out = r.json()
                            self.assertEqual(out["status"], "incomplete")
                            self.assertEqual([item["content"][0]["text"] for item in out["output"]],
                                             [prefix] if prefix else [])

    async def test_dropped_calls_never_turn_a_client_prefill_into_partial_provider_output(self):
        self.settings["prefill_mode"] = "smart"
        prefix = "client prefill (not a candidate)"
        history = [{"role": "user", "content": "Continue."}, {"role": "assistant", "content": prefix}]
        for self.channel in ("api_key", "web_proxy"):
            for parts, texts in (([OPEN], []), ([{"text": "delivered"}, OPEN], ["delivered"])):
                with self.subTest(channel=self.channel, visible=len(texts)):
                    self.reply, self.finish_reason = parts, "MAX_TOKENS"
                    r = await self.post_at(request(input=history), "json")
                    self.assertEqual(r.status_code, 200, r.text[:240])
                    self.assertEqual([i["content"][0]["text"] for i in r.json()["output"]], texts)
