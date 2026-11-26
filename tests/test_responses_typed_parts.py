"""Use public JSON/SSE and both real adapters, not preflattened Chat dictionaries."""
import base64
from unittest.mock import patch

import channel_dispatch
from routes import responses_api
from signature_store import signature_store
from test_responses_api import MODEL, SIGNATURES, assistant, calls_in_wire, continuation, event_list, request, signature
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

    async def test_native_tool_and_code_payloads_do_not_turn_into_blank_signed_parts(self):
        native_arms = (
            {"executableCode": {"language": "PYTHON", "code": "# typed fixture content"}},
            {"codeExecutionResult": {"outcome": "OUTCOME_OK"}},  # No stdout is still a real result.
            {"toolCall": {"toolType": "GOOGLE_SEARCH_WEB", "args": {}}},
            {"toolResponse": {"toolType": "GOOGLE_SEARCH_WEB", "response": {}}},
            {"functionResponse": {"name": "count_colors", "response": {}}},
            {"functionResponse": {"parts": [{"inlineData": {"data": "Ynl0ZXM="}}]}},
            {"functionResponse": {"parts": [{"fileData": {"fileUri": "gs://fixture/native"}}]}},
        )
        self.settings.update(cross_channel_fallback=True, retry_max=2)
        encoded = base64.b64encode(SIGNATURES[1]).decode()
        with patch.object(channel_dispatch.credential_manager, "has_cookie_record", return_value=True):
            for self.channel in ("api_key", "web_proxy"):
                for phase in ("json", "early", "late"):
                    for arm in native_arms:
                        with self.subTest(channel=self.channel, phase=phase, arm=arm):
                            self.sent.clear()
                            self.reply = [{"text": "not a partial success"}, assistant("not-executable", SIGNATURES[0]),
                                          {**arm, "thoughtSignature": encoded}]
                            with patch("image_delivery.image_markdown") as render, patch.object(
                                    signature_store, "register_call_signature") as register, patch.object(
                                    responses_api, "publish_responses_item_origins") as publish:
                                response = await self.post_at(request(stream=phase != "json"), phase)
                                if phase == "late":
                                    self.assertEqual(response.status_code, 200)
                                    events = event_list(response.text)
                                    self.assertEqual([event["type"] for event in events], ["response.created",
                                                      "response.in_progress", "response.failed"])
                                    self.assertEqual(events[-1]["response"]["output"], [])
                                    error = events[-1]["response"]["error"]
                                else:
                                    self.assertEqual(response.status_code, 502, response.text[:240])
                                    error = response.json()["error"]
                                self.assertIn("Native tool or code output", error["message"])
                                for marker in (encoded, "not-executable", "not a partial success", "fixture/native",
                                               "typed fixture content", "Ynl0ZXM="):
                                    self.assertNotIn(marker, response.text)
                                self.assertEqual(len(self.sent), 1)
                                render.assert_not_called()
                                register.assert_not_called()
                                publish.assert_not_called()

    async def test_real_signature_only_part_and_empty_native_shells_survive_exact_replay(self):
        defaults = {"executableCode": {"language": "LANGUAGE_UNSPECIFIED", "code": "", "id": None},
                    "codeExecutionResult": {"outcome": "OUTCOME_UNSPECIFIED", "output": ""},
                    "toolCall": {"toolType": "TOOL_TYPE_UNSPECIFIED", "args": {}}, "toolResponse": {},
                    "functionResponse": {"name": "", "response": {}, "scheduling": "SCHEDULING_UNSPECIFIED",
                                         "willContinue": False, "parts": [{"inlineData": {"data": None,
                                             "mimeType": "image/png"}}, {"fileData": {"fileUri": ""}}]}}
        encoded = base64.b64encode(SIGNATURES[1]).decode()
        for self.channel in ("api_key", "web_proxy"):
            for phase in ("json", "late"):
                for shell in ({}, defaults):
                    with self.subTest(channel=self.channel, phase=phase, shell=bool(shell)):
                        prompt = f"Typed shell fixture: {self.channel}/{phase}/{bool(shell)}"
                        self.reply = [{**shell, "thoughtSignature": encoded}, assistant("recipient", SIGNATURES[0])]
                        first = await self.post_at(request(input=prompt, stream=phase != "json"), phase)
                        self.assertEqual(first.status_code, 200, first.text[:240])
                        call, = public(first, phase)["output"]
                        extra = call["extra_content"]["vertex2openai"]["assistant_extra_content"]["google"]
                        self.assertEqual(extra["ordinary_parts"], [{"kind": "signature_only", "text": "",
                                                                    "thought_signature": encoded}])
                        self.reply = {"text": "done"}
                        followup = await self.post_at(request(input=continuation(prompt, [call])), "json")
                        self.assertEqual(followup.status_code, 200, followup.text[:240])
                        wire = self.sent[-1].get("variables", self.sent[-1])
                        parts = [p for turn in wire["contents"] if turn["role"] == "model" for p in turn["parts"]]
                        self.assertEqual([signature(p) for p in parts], [SIGNATURES[1], SIGNATURES[0]])

    async def test_native_projection_does_not_restrict_legacy_chat_or_native_generate(self):
        arm = {"codeExecutionResult": {"outcome": "OUTCOME_OK", "output": "native artifact"},
               "thoughtSignature": base64.b64encode(SIGNATURES[0]).decode()}
        self.reply = [{"text": "plain chat text"}, arm]
        for self.channel in ("api_key", "web_proxy"):
            with self.subTest(channel=self.channel):
                reply = await self.client.post("/v1/chat/completions", json={
                    "model": MODEL, "messages": [{"role": "user", "content": "fixture"}],
                })
                self.assertEqual(reply.status_code, 200, reply.text[:240])
                self.assertEqual(reply.json()["choices"][0]["message"]["content"], "plain chat text")
        self.channel = "web_proxy"
        reply = await self.client.post(f"/v1beta/models/{MODEL}:generateContent", json={
            "contents": [{"role": "user", "parts": [{"text": "fixture"}]}],
        })
        self.assertEqual(reply.status_code, 200, reply.text[:240])
        self.assertEqual(reply.json()["candidates"][0]["content"]["parts"][-1], arm)
