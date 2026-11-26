"""Parsed SDK fragments must not be retained by a long-lived unfinished call."""
import gc
import json
import unittest
import weakref

from test_tool_stream_deadline import call, chunk, fragment, parts
from tool_stream import ToolStreamAssembler


class HeaderFragmentLifetime(unittest.TestCase):
    def test_consumed_wrappers_can_be_collected_before_close_without_changing_wire_values(self):
        expected = {f"row{i}": f"值 {i}" for i in range(1400)}
        signature = b"\xff\x00original"
        header = call(name="read", will_continue=True,
                      partial_args=[fragment(f"$.{key}", value) for key, value in expected.items()])
        header.thought_signature = signature
        refs = [weakref.ref(item) for item in header.function_call.partial_args]
        stream = ToolStreamAssembler()
        self.assertEqual(parts(stream.restore_sdk(chunk(header))), [])
        start_bytes = stream.bytes
        self.assertGreater(start_bytes, len(expected))
        # The provider/caller still owns its wrappers; no destructive clear.
        self.assertEqual(header.function_call.partial_args[-1].string_value, expected["row1399"])
        self.assertIsNone(header.function_call.id)
        del header
        gc.collect()
        self.assertEqual(sum(ref() is not None for ref in refs), 0)

        close = call(id="late-id", will_continue=False, partial_args=[fragment("$.last", "✓")])
        delivered, = parts(stream.restore_sdk(chunk(close, finish="STOP")))
        stream.validate_end()
        fc = delivered.function_call
        expected["last"] = "✓"
        self.assertEqual(json.dumps(fc.args, ensure_ascii=False).encode(),
                         json.dumps(expected, ensure_ascii=False).encode())
        self.assertEqual((fc.name, fc.id, delivered.thought_signature), ("read", "late-id", signature))
        self.assertIsNone(fc.partial_args)
        self.assertIsNone(fc.will_continue)
        self.assertGreater(stream.bytes, start_bytes)  # Release lifetime, not the cumulative budget.
        self.assertEqual(close.function_call.partial_args[0].string_value, "✓")
