"""Opaque byte fidelity at both compatible edges; no service or credentials involved."""

import base64
import unittest
from types import SimpleNamespace

from message_processing import create_gemini_prompt
from models import OpenAIMessage
from response_validation import IncompleteResponseError
from signature_store import SKIP_VALIDATOR_SENTINELS
from test_responses_api import PATH, MODEL, assistant, calls_in_wire, continuation, event_list, request, signature, thin_item
from test_schema_contract_fidelity import SchemaRouteHarness
from thought_signatures import (
    decode_thought_signature, encode_thought_signature, signature_bytes, signature_from_extra,
)
from upstreams.cookie_request import _convert_messages_to_contents
from upstreams.cookie_response import apply_wire_signature, extract_events, openai_tool_call


def provider_body(part):
    return {"results": [{"data": {"candidates": [
        {"content": {"role": "model", "parts": [part]}, "finishReason": "STOP"}
    ]}}]}


class ProtoJsonSignatureTests(unittest.TestCase):
    def test_both_provider_dtos_and_explicit_replay_preserve_opaque_bytes(self):
        opaque = b"\xfb\xff"
        for value in ("+/8=", "-_8=", "+/8", "-_8"):
            with self.subTest(value=value):
                # Ingress: provider call / ordinary events -> canonical OpenAI DTO.
                sdk = SimpleNamespace(thought_signature=value)
                self.assertEqual(encode_thought_signature(signature_bytes(sdk, None)), "+/8=")
                tc = openai_tool_call("completion", 0, {"id": "proto:1", "name": "f",
                                                       "thought_signature": value})
                self.assertEqual(tc["extra_content"]["google"]["thought_signature"], "+/8=")
                ordinary = [{"kind": "text", "text": "one"}]
                apply_wire_signature({"value": value, "part_kind": "text"}, ordinary, [])
                self.assertEqual(ordinary[0]["thought_signature"], "+/8=")
                # Replay an external DTO in each accepted spelling, not merely
                # the canonical form emitted by our own adapter.
                carrier = {"google": {"thought_signature": value, "thought_signature_part": "text"}}
                tc["extra_content"] = carrier
                ordinary[0]["thought_signature"] = value
                both = OpenAIMessage(role="assistant", content="one", tool_calls=[tc],
                                     extra_content={"google": {"ordinary_parts": ordinary}})
                top = OpenAIMessage(role="assistant", content="two", extra_content=carrier)
                for msg in (both, top):
                    sdk_parts = create_gemini_prompt([msg], "gemini-2.5-flash")[0].parts
                    wire = _convert_messages_to_contents([msg], "gemini-2.5-flash")[0][0]["parts"]
                    self.assertEqual([p.thought_signature for p in sdk_parts], [opaque] * len(wire))
                    self.assertEqual([p["thoughtSignature"] for p in wire], ["+/8="] * len(wire))
                    self.assertEqual(len(wire), 2 if msg is both else 1)

    def test_invalid_encodings_and_existing_literals_keep_their_historical_bytes(self):
        for literal in ("A", "YQ=", "YWJj=", "+_8=", "Y Q==", "not a signature", "x!==",
                        "fé", "AA==junk", "AA===A", "+/9=", *(v.decode() for v in SKIP_VALIDATOR_SENTINELS)):
            with self.subTest(literal=literal):
                self.assertEqual(decode_thought_signature(literal), literal.encode())
                raw, _ = signature_from_extra({"extra_content": {"google": {"thought_signature": literal}}})
                self.assertEqual(raw, literal.encode())
        self.assertIsNone(decode_thought_signature(""))
        self.assertEqual(decode_thought_signature(b"-_8="), b"-_8=")
        for data in (b"\xfb", b"\xfb\xff", b"\xfb\xff\xfe", b"\x00\x00\x00\x00"):
            for encode in (base64.b64encode, base64.urlsafe_b64encode):
                encoded = encode(data).decode("ascii")
                self.assertEqual(decode_thought_signature(encoded), data)
                self.assertEqual(decode_thought_signature(encoded.rstrip("=")), data)

    def test_malformed_provider_part_is_rejected_before_excerpt_or_call(self):
        malformed = ("YQ=", "+_8=", "Zh==", "Y Q==", "not a signature", 0, False, [], {})
        for preserve in (False, True):
            for value in malformed:
                for part in ({}, {"text": "Check."}, {"text": "Private", "thought": True}, assistant("fixture")):
                    with (self.subTest(preserve=preserve, value=value, part=part),
                          self.assertRaisesRegex(IncompleteResponseError, "thoughtSignature")):
                        event = extract_events(provider_body({**part, "thoughtSignature": value}),
                                               preserve_parts=preserve, strip_thoughts=True)
                        next(event)

    def test_empty_or_null_provider_signature_is_absent(self):
        for preserve in (False, True):
            for empty in ("", None):
                with self.subTest(preserve=preserve, empty=empty):
                    first = ("native_part", {"text": "Check."}) if preserve else ("text", "Check.")
                    self.assertEqual(list(extract_events(provider_body({"text": "Check.", "thoughtSignature": empty}),
                                                         preserve_parts=preserve)),
                                     [first, ("finish", "STOP")])


class CookieProviderSignatureRoutes(SchemaRouteHarness):
    async def test_original_bytes_roundtrip_on_responses_and_native_without_rewriting_application_values(self):
        self.channel = "web_proxy"
        native_path = f"/v1beta/models/{MODEL}:generateContent"
        native_body = {"contents": [{"role": "user", "parts": [{"text": "Count the colors."}]}]}
        for value in ("+/8=", "-_8=", "+/8", "-_8"):
            with self.subTest(value=value):
                call = assistant("count-with-" + value.replace("=", "_"))
                call["functionCall"]["args"] = {"thoughtSignature": "YQ=", "nested": {"thoughtSignature": 0}}
                call["thoughtSignature"] = value
                self.reply = [{"text": "Checking.", "thoughtSignature": value}, call]
                response = await self.client.post(PATH, json=request(stream=True))
                self.assertEqual(response.status_code, 200, response.text)
                public = event_list(response.text)[-1]["response"]
                text, public_call = public["output"]
                self.assertEqual(text["extra_content"]["google"]["ordinary_parts"][0]["thought_signature"], "+/8=")
                self.assertEqual(public_call["extra_content"]["google"]["thought_signature"], "+/8=")
                self.assertIn('"thoughtSignature": "YQ="', public_call["arguments"])
                native = await self.client.post(native_path, json=native_body)
                self.assertEqual(native.status_code, 200, native.text)
                self.assertEqual(native.json()["candidates"][0]["content"]["parts"], self.reply)

    async def test_malformed_provider_data_cannot_publish_a_new_origin_or_reach_native_client(self):
        self.channel = "web_proxy"
        prompt = "One signed call."
        for stream in (False, True):
            with self.subTest(stream=stream):
                call_id = f"valid-initial-signature-{stream}"
                actual = b"\x00\xfb\xfforiginal"
                self.reply = [assistant(call_id, actual)]
                issued = await self.client.post(PATH, json=request(input=prompt, stream=stream))
                self.assertEqual(issued.status_code, 200, issued.text)
                public = event_list(issued.text)[-1]["response"] if stream else issued.json()
                thin = [thin_item(item) for item in public["output"]]

                self.reply = [{**assistant(call_id), "thoughtSignature": "YQ="}]
                broken = await self.client.post(PATH, json=request(input=prompt, stream=stream))
                if broken.headers["content-type"].startswith("text/event-stream"):
                    frames = event_list(broken.text)
                    self.assertEqual(frames[-1]["type"], "response.failed")
                    self.assertFalse(any(event["type"].startswith("response.output_item.") for event in frames))
                else:
                    self.assertEqual(broken.status_code, 502, broken.text)
                self.assertIn("thoughtSignature", broken.text)
                self.assertNotIn("YQ=", broken.text)
                self.assertNotIn(call_id, broken.text)
                native = await self.client.post(f"/v1beta/models/{MODEL}:generateContent", json={
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}]})
                self.assertEqual(native.status_code, 502, native.text)
                self.assertNotIn("YQ=", native.text)

                self.reply = {"text": "Received."}
                replay = await self.client.post(PATH, json=request(input=continuation(prompt, thin)))
                self.assertEqual(replay.status_code, 200, replay.text)
                self.assertEqual([signature(part) for part in calls_in_wire(self.sent[-1], "functionCall")], [actual])
