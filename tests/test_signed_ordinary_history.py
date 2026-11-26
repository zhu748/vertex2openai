"""Assistant history is authoritative; private replay hints must project to it exactly."""

import base64
import unittest
from unittest import mock

from image_history import ManagedImageHistory
from message_processing import create_gemini_prompt
from models import OpenAIMessage
from upstreams.cookie_request import _convert_messages_to_contents


def signed(kind, text, data):
    return {"kind": kind, "text": text, "thought_signature": base64.b64encode(data).decode()}


def parts_for(message):
    sdk = create_gemini_prompt([message], "gemini-2.5-flash")
    cookie, _ = _convert_messages_to_contents([message], "gemini-2.5-flash")
    yield "express", [("call" if p.function_call else "thought" if p.thought else "text",
                       p.function_call.id if p.function_call else p.text, p.thought_signature)
                      for turn in sdk for p in turn.parts]
    yield "cookie", [("call" if "functionCall" in p else "thought" if p.get("thought") else "text",
                     p["functionCall"]["id"] if "functionCall" in p else p.get("text"),
                     base64.b64decode(p["thoughtSignature"]) if p.get("thoughtSignature") else None)
                    for turn in cookie for p in turn["parts"]]


CALL = {"type": "function", "id": "ordinary-turn-call", "function": {"name": "inspect", "arguments": "{}"},
        "extra_content": {"google": {"thought_signature": "dG9vbA=="}}}
ORDER = [{"type": "ordinary", "index": 0}, {"type": "tool_call", "index": 0},
         {"type": "ordinary", "index": 1}, {"type": "ordinary", "index": 2},
         {"type": "ordinary", "index": 3}]


class SignedOrdinaryHistoryTests(unittest.TestCase):
    def test_literal_media_scheme_interleaved_call_signed_reasoning_and_empty_tail(self):
        first, last = "Describe data:image/ as prose.", " Then data:image/png."
        raw = [signed("text", first, b"left"), signed("thought", "note", b"idea"),
               signed("text", last, b"right"), signed("signature_only", "", b"tail")]
        # These client block boundaries intentionally differ from Gemini's boundaries.
        for content in (first + last, [{"type": "text", "text": (first + last)[:8]},
                                        {"type": "text", "text": (first + last)[8:]}]):
            message = OpenAIMessage(role="assistant", content=content, reasoning_content="note",
                                    tool_calls=[CALL], extra_content={"google": {
                                        "ordinary_parts": raw, "part_order": ORDER,
                                        "thought_signature": "dGFpbA==", "thought_signature_part": "signature_only"}})
            for channel, transcript in parts_for(message):
                with self.subTest(channel=channel):
                    self.assertEqual(transcript, [("text", first, b"left"), ("call", CALL["id"], b"tool"),
                                                  ("thought", "note", b"idea"), ("text", last, b"right"),
                                                  ("text", "", b"tail")])

    def test_edit_of_either_field_or_empty_carrier_replays_actual_content_without_stale_signatures(self):
        for actual, idea, raw in (("NEW", "reason", [signed("text", "old", b"old"),
                                                     signed("thought", "reason", b"old")]),
                                  ("NEW", "new-reason", [signed("text", "NEW", b"old"),
                                                        signed("thought", "old-reason", b"old")]),
                                  ("NEW", "new-reason", []),
                                  ("NEW", None, [signed("signature_only", "NEW", b"invalid")])):
            for calls in (None, [CALL]):
                google = {"ordinary_parts": raw, "part_order": ORDER, "thought_signature": "b2xk",
                          "thought_signature_part": "text"}
                msg = OpenAIMessage(role="assistant", content=actual, reasoning_content=idea, tool_calls=calls,
                                    extra_content={"google": google})
                for channel, transcript in parts_for(msg):
                    with self.subTest(channel=channel, calls=bool(calls), raw=raw):
                        expected = ([("call", CALL["id"], b"tool")] if calls else [])
                        if idea:
                            expected.append(("thought", idea, None))
                        expected.append(("text", actual, None))
                        self.assertEqual(transcript, expected)

    def test_user_role_never_gets_a_provider_part_thought_or_signature(self):
        for raw in ({}, {"ordinary_parts": []},
                    {"ordinary_parts": [signed("text", "the user's own words", b"old")]}):
            user = OpenAIMessage(role="user", content="the user's own words", reasoning_content="not model text",
                                 extra_content={"google": {**raw, "thought_signature": "b2xk"}})
            for channel, transcript in parts_for(user):
                with self.subTest(channel=channel, raw=raw):
                    self.assertEqual(transcript, [("text", user.content, None)])

    def test_replacement_markdown_on_a_foreign_host_is_text_and_never_signed_or_fetched(self):
        owner = ManagedImageHistory("https://owned.invalid/base")
        url = "https://foreign.invalid/base/v1/generated-images/" + "f" * 48 + ".png"
        content = "![Image](" + url + ")"
        message = OpenAIMessage(role="assistant", content=content, extra_content={"google": {
            "ordinary_parts": [signed("image", "", b"old")], "thought_signature": "b2xk"}})
        with mock.patch.object(ManagedImageHistory, "for_request", return_value=owner), \
             mock.patch("message_processing.fetch_remote_image", side_effect=AssertionError("unexpected fetch")):
            for channel, transcript in parts_for(message):
                with self.subTest(channel=channel):
                    self.assertEqual(transcript, [("text", content, None)])

    def test_rejected_ordinary_positions_do_not_desynchronize_calls_and_their_results(self):
        calls = [{"type": "function", "id": key, "function": {"name": "forecast", "arguments": '{"city":"' + key + '"}'},
                  "extra_content": {"google": {"thought_signature": base64.b64encode(key.encode()).decode()}}}
                 for key in ("A", "B", "C")]
        raw = [signed("text", "old", b"old")]
        # B first, ordinary slot next, A after: C is unsorted remainder.
        order = [{"type": "tool_call", "index": 1}, {"type": "ordinary", "index": 0},
                 {"type": "tool_call", "index": 0}]
        for visible in ("old", "edited"):
            messages = [OpenAIMessage(role="user", content="forecast"),
                        OpenAIMessage(role="assistant", content=visible, tool_calls=calls, extra_content={"google": {
                            "ordinary_parts": raw, "part_order": order, "thought_signature": "b2xk"}}),
                        *(OpenAIMessage(role="tool", tool_call_id=i, content='{"city":"' + i + '"}')
                          for i in ("C", "B", "A"))]
            sdk = [p for turn in create_gemini_prompt(messages, "gemini-2.5-flash") for p in turn.parts]
            cookie = [p for turn in _convert_messages_to_contents(messages, "gemini-2.5-flash")[0] for p in turn["parts"]]
            expect = [(key, key, key.encode()) for key in ("B", "A", "C")]
            self.assertEqual([(p.function_call.id, p.function_call.args["city"], p.thought_signature)
                              for p in sdk if p.function_call], expect)
            self.assertEqual([(p["functionCall"]["id"], p["functionCall"]["args"]["city"], base64.b64decode(p["thoughtSignature"]))
                              for p in cookie if "functionCall" in p], expect)
            self.assertEqual([p.function_response.id for p in sdk if p.function_response], ["B", "A", "C"])
            self.assertEqual([p["functionResponse"]["id"] for p in cookie if "functionResponse" in p], ["B", "A", "C"])
            for rich in ([p.function_call.id if p.function_call else (p.text, p.thought_signature)
                          for p in sdk if p.function_call or p.text == visible],
                         [p["functionCall"]["id"] if "functionCall" in p else
                          (p["text"], base64.b64decode(p["thoughtSignature"]) if p.get("thoughtSignature") else None)
                          for p in cookie if "functionCall" in p or p.get("text") == visible]):
                with self.subTest(visible=visible, rich=rich):
                    self.assertEqual(rich, ["B", ("old", b"old"), "A", "C"] if visible == "old"
                                     else ["B", "A", "C", ("edited", None)])
