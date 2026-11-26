"""Tool limits apply to real tool traffic and blocked order, not drained ordinary content."""
import unittest
from unittest.mock import patch

from google.genai import types

from response_validation import IncompleteResponseError
from test_tool_stream_deadline import call, chunk, parts
from tool_stream import ToolStreamAssembler


class ToolBufferLifetime(unittest.TestCase):
    def test_plain_siblings_not_billed_even_if_another_choice_is_blocked(self):
        stream = ToolStreamAssembler()
        text = types.Part(text="中" * 500)
        with patch("tool_stream.MAX_TOOL_STREAM_BYTES", 400):
            self.assertEqual(parts(stream.restore_sdk(chunk(call(id="a", name="fn", will_continue=True)))), [])
            charged = stream.bytes
            for _ in range(12):
                self.assertEqual(parts(stream.restore_sdk(chunk(text, text, index=1))), [text, text])
            self.assertEqual(stream.bytes, charged)
            self.assertEqual(len(parts(stream.restore_sdk(chunk(call(id="a", will_continue=False))))), 1)
            self.assertIsNone(stream.validate_end())

    def test_blocked_text_is_released_between_barriers_in_the_same_frame(self):
        stream = ToolStreamAssembler()
        text = types.Part(text="x" * 3000)
        def begin(ident):
            return call(id=ident, name="fn", will_continue=True)

        def end(ident):
            return call(id=ident, will_continue=False)

        with patch("tool_stream.MAX_TOOL_STREAM_BYTES", 5000):
            self.assertEqual(parts(stream.restore_sdk(chunk(begin("a"), text))), [])
            res = parts(stream.restore_sdk(chunk(end("a"), begin("b"), text)))
            self.assertEqual([res[0].function_call.id, res[1].text], ["a", text.text])
            res = parts(stream.restore_sdk(chunk(end("b"), text)))
            self.assertEqual([res[0].function_call.id, *[p.text for p in res[1:]]], ["b", text.text, text.text])
            self.assertIsNone(stream.validate_end())

    def test_outstanding_data_from_different_choices_shares_one_retained_limit(self):
        stream = ToolStreamAssembler()
        text = types.Part(text="x" * 400)
        with patch("tool_stream.MAX_TOOL_STREAM_BYTES", 1050):
            self.assertEqual(parts(stream.restore_sdk(chunk(call(id="a", name="fn", will_continue=True), text))), [])
            self.assertEqual(parts(stream.restore_sdk(chunk(call(id="b", name="fn", will_continue=True), index=1))), [])
            with self.assertRaises(IncompleteResponseError):
                stream.restore_sdk(chunk(text, index=1))

    def test_delivered_tool_payloads_still_count_toward_the_cumulative_tool_limit(self):
        stream = ToolStreamAssembler()
        with patch("tool_stream.MAX_TOOL_STREAM_BYTES", 400):
            tc = call(name="fn", args={"key": "1" * 25})
            self.assertEqual(len(parts(stream.restore_sdk(chunk(tc)))), 1)
            with self.assertRaises(IncompleteResponseError):
                for _ in range(3):
                    stream.restore_sdk(chunk(tc))
