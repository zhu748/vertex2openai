"""Same-role SDK turns merge in linear work without changing the emitted Contents."""
import unittest

from google.genai import types

from message_processing import _merge_gemini_turns


def turn(role, *parts):
    return types.Content(role=role, parts=list(parts))


def dump(turns):
    return [t.model_dump(mode="json", by_alias=True, exclude_none=True) for t in turns]


class GeminiTurnMergeTests(unittest.TestCase):
    def test_separators_signatures_media_and_tool_result_boundaries(self):
        text = types.Part.from_text
        gap = text(text="\n\n")
        a, b, c = (text(text=s) for s in (" 甲 ", "", "文\n"))
        thought = types.Part(text="why", thought=True, thought_signature=b"sig")
        image = types.Part.from_bytes(data=b"image", mime_type="image/png")
        call = types.Part(function_call=types.FunctionCall(name="f", id="1", args={"a": 0}),
                          thought_signature=b"tool")
        r1, r2 = (types.Part(function_response=types.FunctionResponse(
            name="f", id=i, response={"got": i})) for i in ("1", "2"))
        rows = [
            turn("user", a), turn("user", b), turn("user", image), turn("user", c),
            turn("model", thought), turn("model", a), turn("model", call), turn("model", b),
            turn("user", r1), turn("user", r2), turn("user", a), turn("user", b),
            turn("user", r2), turn("user", r1, c), turn("user", b),
            turn("model"), turn("model", c),
        ]
        expected = [
            turn("user", a, gap, b, image, c),
            turn("model", thought, gap, a, call, b),
            turn("user", r1, r2), turn("user", a, gap, b),
            turn("user", r2), turn("user", r1, c, b),
            turn("model", gap, c),
        ]
        self.assertEqual(dump(_merge_gemini_turns(rows)), dump(expected))
        self.assertEqual(dump(_merge_gemini_turns([])), [{"role": "user", "parts": [{"text": "继续"}]}])

    def test_long_text_and_parallel_results_do_not_rescan_merged_prefix(self):
        # Count real part visits, not machine-dependent test duration. The first
        # row is the mutating output buffer, so prefix rescans are visible too.
        n = 192
        for part, expected in (
            (types.Part(text="ok"), 2 * n - 1),
            (types.Part.from_function_response(name="f", response={"ok": 1}), n),
        ):
            with self.subTest(part=part):
                visits = 0

                class CountedParts(list):
                    def __iter__(self):
                        nonlocal visits
                        for p in super().__iter__():
                            visits += 1
                            yield p

                rows = [turn("user").model_copy(update={"parts": CountedParts([part])})
                        for _ in range(n)]
                merged = _merge_gemini_turns(rows)
                self.assertEqual(len(merged), 1)
                self.assertEqual(len(merged[0].parts), expected)
                self.assertLessEqual(visits, 5 * n)
