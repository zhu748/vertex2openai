"""Buffered Responses keeps ordinary provider output even through a text-transport alias."""
import base64
from contextlib import asynccontextmanager
import json
from unittest.mock import patch

import httpx

import anti_truncation
import channel_dispatch
from test_responses_api import MODEL, assistant, continuation, request, signature
import test_responses_outcomes
from test_responses_typed_parts import public
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery


class ResponsesTextTransport(SchemaRouteHarness):
    post_at = test_responses_outcomes.ResponsesOutcomes.post_at

    async def test_reasoning_alone_is_a_typed_stop_on_both_aliases_and_channels(self):
        self.settings.update(anti_truncation_mode="both", cross_channel_fallback=True)
        self.usage_metadata = {"promptTokenCount": 7, "candidatesTokenCount": 3, "totalTokenCount": 10}
        visible = "A visible provider thought."
        self.reply = [{"text": visible, "thought": True}]
        with patch.object(channel_dispatch.credential_manager, "has_cookie_record", return_value=True):
            for self.channel in ("api_key", "web_proxy"):
                for alias in ("nonstream", "stream"):
                    for phase in ("json", "early", "late"):
                        with self.subTest(channel=self.channel, alias=alias, phase=phase):
                            self.sent.clear()
                            body = request(model=MODEL + "-antitruncation-" + alias, stream=phase != "json")
                            resp = await self.post_at(body, phase)
                            self.assertEqual(resp.status_code, 200, resp.text[:250])
                            out = public(resp, phase)
                            self.assertEqual(out["status"], "completed")
                            reason, = out["output"]
                            self.assertEqual(reason["type"], "reasoning")
                            self.assertEqual(reason["content"], [{"type": "reasoning_text", "text": visible}])
                            self.assertEqual(out["usage"]["total_tokens"], 10)
                            self.assertEqual(len(self.sent), 1)

    async def test_empty_or_unfinished_stops_do_not_become_visible_reasoning(self):
        self.settings.update(anti_truncation_mode="nonstream", cross_channel_fallback=False)
        visible = {"text": "Real provider thought.", "thought": True}
        partial = {"functionCall": {"name": "count_colors", "id": "unfinished", "willContinue": True}}
        for self.channel in ("api_key", "web_proxy"):
            for self.reply in ([], [{"thought": True, "text": " \t\n"}], [{"thoughtSignature": "YQ=="}],
                               [visible, partial]):
                with self.subTest(channel=self.channel, parts=self.reply):
                    body = request(model=MODEL + "-antitruncation-nonstream")
                    resp = await self.post_at(body, "json")
                    self.assertEqual(resp.status_code, 502, resp.text[:200])

    async def test_buffered_alias_does_not_reorder_or_reassign_genuine_provider_signatures(self):
        self.settings.update(anti_truncation_mode="both", cross_channel_fallback=False)
        alias = MODEL + "-antitruncation-nonstream"
        private_name = "v2o_emit_fixture"

        def signed(part, value):
            return {**part, "thoughtSignature": base64.b64encode(value).decode()}

        text = signed({"text": "direct ☁"}, b"direct")
        thought = signed({"thought": True, "text": "independent 🐾"}, b"thought")
        after = signed({"text": "tail"}, b"last")
        call = assistant("real-id", b"call")
        private = signed({"functionCall": {"id": "owned", "name": private_name,
                          "args": {"content": "decoded"}}}, b"hidden transport")
        bare = signed({}, b"late private fixture")
        decoded = {"text": "decoded"}

        def shape(part):
            if "functionCall" in part:
                return "call", part["functionCall"], signature(part)
            return "thought" if part.get("thought") else "text", part.get("text", ""), signature(part)

        with patch.object(anti_truncation.secrets, "token_hex", return_value="fixture"):
            for self.channel in ("api_key", "web_proxy"):
                for phase in ("json", "early", "late"):
                    for parts, expected in (([text, thought, call, after], [text, thought, call, after]),
                            ([text, thought, private, bare, call], [thought, decoded, call]),
                            ([private, text, call], [decoded, call])):
                        prompt = f"{self.channel}/{phase}/{len(parts)} fixture"
                        with self.subTest(prompt=prompt):
                            self.reply, self.sent = parts, []
                            first = await self.post_at(request(model=alias, stream=phase != "json", input=prompt), phase)
                            self.assertEqual(first.status_code, 200, first.text[:210])
                            out = public(first, phase)
                            self.assertEqual(out["status"], "completed")
                            self.assertNotIn(private_name, first.text)
                            self.assertNotIn(private["thoughtSignature"], first.text)
                            self.assertNotIn(bare["thoughtSignature"], first.text)
                            self.reply = [{"text": "done"}]
                            second = await self.post_at(request(model=alias, input=continuation(prompt, out["output"])), "json")
                            self.assertEqual(second.status_code, 200, second.text[:210])
                            self.assertEqual(len(self.sent), 2)
                            wire = self.sent[-1].get("variables", self.sent[-1])
                            replay, = (t["parts"] for t in wire["contents"] if t["role"] == "model")
                            self.assertEqual(list(map(shape, replay)), list(map(shape, expected)))

    async def test_cookie_private_fragments_in_later_records_keep_earlier_real_siblings(self):
        self.channel = "web_proxy"
        self.settings["anti_truncation_mode"] = "stream"
        private = "v2o_emit_fixture"
        alias = MODEL + "-antitruncation-stream"

        def row(parts=(), finish=None):
            candidate = {"content": {"parts": list(parts)}}
            if finish:
                candidate["finishReason"] = finish
            return {"results": [{"data": {"candidates": [candidate]}}]}

        def fragment(value, *, start):
            field = {"jsonPath": "$.content", "stringValue": value, "willContinue": start}
            return {"functionCall": {"id": "owned", **({"name": private} if start else {}),
                    "willContinue": start, "partialArgs": [field]}}

        rows = [row([{"text": "discard", "thoughtSignature": "SQ=="}, assistant("keep-id", b"call")]),
                row([{"text": "retained thought", "thought": True, "thoughtSignature": "SA=="}]),
                row([fragment("decoded", start=True)]), row([fragment(" tail", start=False)]),
                row([{"text": "also discard", "thoughtSignature": "Rw=="}], "STOP")]
        calls = []

        def reply(req):
            calls.append(req.url.path)
            return httpx.Response(200, text="\n".join(map(json.dumps, rows)))

        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
            @asynccontextmanager
            async def studio(*args, **kwargs):
                yield client

            with patch.object(cookie_delivery, "open_upstream_client", studio), patch.object(
                    anti_truncation.secrets, "token_hex", return_value="fixture"):
                first = await self.post_at(request(model=alias, input="multi-record", stream=True), "late")
        self.assertEqual(len(calls), 1)
        out = public(first, "late")
        self.assertEqual(out["status"], "completed")
        self.assertNotIn("discard", first.text)
        self.assertNotIn("SQ==", first.text)
        self.assertNotIn("Rw==", first.text)
        self.reply = [{"text": "done"}]
        second = await self.post_at(request(model=alias, input=continuation("multi-record", out["output"])), "json")
        self.assertEqual(second.status_code, 200, second.text[:240])
        wire = self.sent[-1]["variables"]
        parts, = (turn["parts"] for turn in wire["contents"] if turn["role"] == "model")
        self.assertEqual(parts[0]["functionCall"]["id"], "keep-id")
        self.assertEqual(signature(parts[0]), b"call")
        self.assertEqual(parts[1], {"text": "retained thought", "thought": True, "thoughtSignature": "SA=="})
        self.assertEqual("".join(p["text"] for p in parts[2:]), "decoded tail")
        self.assertTrue(all(signature(p) is None for p in parts[2:]))
