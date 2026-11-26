"""Use public JSON/SSE and both real adapters, not preflattened Chat dictionaries."""
import base64
from unittest.mock import patch

import channel_dispatch
from signature_store import signature_store
from test_responses_api import SIGNATURES, assistant, calls_in_wire, event_list, request, signature
import test_responses_outcomes
from test_schema_contract_fidelity import SchemaRouteHarness


LITERAL = 'Literal ![picture](data:image/png;base64,Ynl0ZXM=) and /v1/generated-images/example.png'


def public(response, phase):
    return response.json() if phase == "json" else event_list(response.text)[-1]["response"]


class TypedResponsesParts(SchemaRouteHarness):
    post_at = test_responses_outcomes.ResponsesOutcomes.post_at

    async def test_native_text_is_literal_through_input_output_and_signed_continuation(self):
        for self.channel in ("api_key", "web_proxy"):
            for phase in ("json", "late"):
                with self.subTest(channel=self.channel, phase=phase):
                    self.reply = [{"text": LITERAL}, assistant("kept-original", SIGNATURES[0])]
                    user = {"role": "user", "content": [{"type": "input_text", "text": LITERAL}]}
                    prior = [user, {"type": "message", "role": "assistant", "content": [
                        {"type": "refusal", "refusal": LITERAL}]}, {"role": "user", "content": "Continue."}]
                    self.sent.clear()
                    with patch("message_processing._extract_markdown_images_to_parts",
                               side_effect=AssertionError("must not parse literal Responses input")):
                        first = await self.post_at(request(input=prior, stream=phase != "json"), phase)
                        self.assertEqual(first.status_code, 200, first.text[:220])
                        output = public(first, phase)["output"]
                        message, call = output
                        self.assertEqual(message["content"][0]["text"], LITERAL)
                        wire = self.sent[0].get("variables", self.sent[0])
                        self.assertEqual([turn["parts"][0]["text"] for turn in wire["contents"]][:2], [LITERAL, LITERAL])
                        self.assertEqual(base64.b64decode(call["extra_content"]["google"]["thought_signature"]), SIGNATURES[0])
                        self.reply = {"text": "done"}
                        again = await self.post_at(request(input=[*prior, *output, {"type": "function_call_output",
                                                       "call_id": call["call_id"], "output": "ok"}]), "json")
                        self.assertEqual(again.status_code, 200, again.text[:220])
                        replay = self.sent[-1].get("variables", self.sent[-1])
                        self.assertFalse(any("inlineData" in part or "fileData" in part
                            for turn in replay["contents"] for part in turn["parts"]))
                        self.assertEqual(signature(calls_in_wire(self.sent[-1], "functionCall")[0]), SIGNATURES[0])

    async def test_media_is_not_text_and_never_materializes_or_registers_a_sibling(self):
        self.settings["cross_channel_fallback"] = True
        media = [{"fileData": {"mimeType": mime, "fileUri": "gs://only-fixture/test"}}
                 for mime in ("image/png", "application/pdf")]
        media += [{"inlineData": {"mimeType": mime, "data": "Ynl0ZXM="}}
                  for mime in ("image/png", "audio/wav")]
        with patch.object(channel_dispatch.credential_manager, "has_cookie_record", return_value=True):
            for self.channel in ("api_key", "web_proxy"):
                for phase in ("json", "early", "late"):
                    for item in media:
                        with self.subTest(channel=self.channel, phase=phase, item=item):
                            self.sent.clear()
                            self.reply = [{"text": "before"}, assistant("discarded", SIGNATURES[0]), item]
                            with patch("image_delivery.image_markdown") as render, patch.object(
                                    signature_store, "register_call_signature") as register:
                                r = await self.post_at(request(stream=phase != "json"), phase)
                                if phase != "late":
                                    self.assertEqual(r.status_code, 502, r.text[:260])
                                else:
                                    self.assertEqual([e["type"] for e in event_list(r.text)],
                                        ["response.created", "response.in_progress", "response.failed"])
                                for text in ("gs://", "Ynl0ZXM=", "before", "discarded"):
                                    self.assertNotIn(text, r.text)
                                self.assertEqual(len(self.sent), 1)
                                render.assert_not_called()
                                register.assert_not_called()

    async def test_nullable_proto_blob_fields_are_absent_not_a_standby_failure(self):
        self.settings["cross_channel_fallback"] = True
        with patch.object(channel_dispatch.credential_manager, "has_cookie_record", return_value=True):
            for self.channel in ("api_key", "web_proxy"):
                for blob, is_media in (({"mimeType": None, "data": "Ynl0ZXM="}, True),
                                       ({"mimeType": "image/png", "data": None}, False),
                                       ({"mimeType": None, "data": None}, False)):
                    with self.subTest(channel=self.channel, blob=blob):
                        self.sent.clear()
                        self.reply = [{"text": "ordinary text"}, {"inlineData": blob}]
                        reply = await self.post_at(request(), "json")
                        self.assertEqual(len(self.sent), 1)
                        self.assertEqual(reply.status_code, 502 if is_media else 200)
                        if is_media:
                            self.assertIn("Media output", reply.json()["error"]["message"])
                            self.assertNotIn("Ynl0ZXM=", reply.text)
                        else:
                            self.assertEqual(reply.json()["output"][0]["content"][0]["text"], "ordinary text")
