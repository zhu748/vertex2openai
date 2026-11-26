"""Opaque byte fidelity at both compatible edges; no service or credentials involved."""

import base64
import unittest
from types import SimpleNamespace

from message_processing import create_gemini_prompt
from models import OpenAIMessage
from signature_store import SKIP_VALIDATOR_SENTINELS
from thought_signatures import (
    decode_thought_signature, encode_thought_signature, signature_bytes, signature_from_extra,
)
from upstreams.cookie_request import _convert_messages_to_contents
from upstreams.cookie_response import apply_wire_signature, openai_tool_call


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
