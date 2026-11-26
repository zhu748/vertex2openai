"""A forecast template is inserted once, regardless of the upstream text boundaries."""
import unittest

from google.genai import types

from prefill import PrefillDeduper, strip_prefill_overlap
from test_req_round15_contracts import Round15Harness, chunks, content, request, response


FORECAST = "".join(f"{i:04d} weather bulletin for city A; " for i in range(230))
UNICODE = "🌦预报e\u0301晴" * 1400 + "END"


def pieces(text, width):
    return (text[i:i + width] for i in range(0, len(text), width))


class LongPrefillTests(unittest.TestCase):
    def test_partition_invariance_against_buffered_contract(self):
        # Covers a complete echo, mismatch/EOF well past 4K, and the existing
        # longest (up to 4K) suffix rule, including periodic/bordered strings.
        periodic = "0123456789" * 930
        for pre, output in (
            (FORECAST, FORECAST + "next forecast"),
            (FORECAST, FORECAST), (FORECAST, FORECAST[:6800]),
            (FORECAST, FORECAST[:6800] + "x"),
            (UNICODE, UNICODE + "🌧"), (UNICODE, UNICODE[:6001] + "🌵"),
            (periodic, periodic[:6207]), (periodic, periodic[:6207] + "!"),
            (FORECAST, FORECAST[-4096:] + "!"),
            (FORECAST, FORECAST[-7:] + "!"), (FORECAST, ""),
        ):
            expected = strip_prefill_overlap(pre, output)
            for width in (1, 48, 1500, 4096, len(pre) + 100):
                with self.subTest(prefill_length=len(pre), output_length=len(output), width=width):
                    deduper = PrefillDeduper(pre)
                    got = "".join(deduper.feed(t) for t in pieces(output, width)) + deduper.flush()
                    self.assertTrue(got == expected, f"expected {len(expected)} chars, got {len(got)}")
                    self.assertEqual(deduper.flush(), "")
                    self.assertEqual(deduper.feed("later"), "later")

    def test_does_not_store_another_long_prefix_or_delay_novel_text(self):
        class Template(str):
            # A large slice could materialize a duplicate even if not retained.
            def __getitem__(self, key):
                if isinstance(key, slice):
                    start, end, _ = key.indices(len(self))
                    if end - start > 4096:
                        raise AssertionError("unbounded template copy during a matching feed")
                return super().__getitem__(key)

        pre = Template(UNICODE * 15)
        d = PrefillDeduper(pre)
        self.assertEqual(d.feed(""), "")  # no information, even before the first text
        self.assertEqual(d.feed(str(pre)[:50000]), "")
        # Count held (rather than caller-owned) text, without prescribing field names.
        self.assertLessEqual(sum(len(v) for v in vars(d).values() if isinstance(v, str) and v is not pre), 4096)
        self.assertEqual(d.feed(str(pre)[50000:] + "tail"), "tail")

        novel = PrefillDeduper(FORECAST)
        self.assertEqual(novel.feed("☃"), "☃")
        self.assertTrue(novel.done)
        # Even a substring of a large template that cannot start a permitted
        # suffix must not delay the first frame.
        middle = PrefillDeduper(FORECAST)
        self.assertEqual(middle.feed("0001 weather"), "0001 weather")


class LongPrefillDeliveryTests(Round15Harness):
    async def test_both_true_streams_emit_one_forecast_and_keep_signature_and_finish(self):
        self.settings.update(inject_prefill=UNICODE, prefill_mode="smart", fake_streaming=False)
        # Cross the old cap before reaching the end of the template. A signed,
        # visible final part must still follow it and precede the terminal frame.
        for width in (1500, 4096, len(UNICODE) + 10):
            segments = list(pieces(UNICODE, width)) + ["next forecast"]

            async def stream(**kwargs):
                async def source():
                    for t in segments[:-1]:
                        yield response(types.Part(text=t), finish=None)
                    yield response(types.Part(text=segments[-1], thought_signature=b"forecast"), finish="STOP")
                return source()

            async def events(*args, **kwargs):
                for t in segments:
                    yield "text", t
                yield "thought_signature", {"value": "Zm9yZWNhc3Q=", "part_kind": "text"}
                yield "finish", "STOP"

            express_body = await self.body(await self.express(request(stream=True), stream=stream))
            _, cookie_body = await self.cookie(request(stream=True), events)
            for channel, body in (("express", express_body), ("cookie", cookie_body)):
                with self.subTest(channel=channel, width=width):
                    self.assertTrue(body.endswith("data: [DONE]\n\n"))
                    self.assertTrue(content(body) == UNICODE + "next forecast", f"{len(content(body))} visible chars")
                    choices = [c for frame in chunks(body) for c in frame["choices"]]
                    self.assertEqual([c["finish_reason"] for c in choices if c["finish_reason"]], ["stop"])
                    self.assertEqual([c["delta"]["role"] for c in choices if "role" in c["delta"]], ["assistant"])
                    sig_frame = next(i for i, c in enumerate(choices) if "Zm9yZWNhc3Q=" in str(c["delta"]))
                    finish_frame = next(i for i, c in enumerate(choices) if c["finish_reason"] == "stop")
                    self.assertLessEqual(sig_frame, finish_frame)
                    final = next(c["delta"]["extra_content"]["google"] for c in reversed(choices)
                                 if "ordinary_parts" in c["delta"].get("extra_content", {}).get("google", {}))
                    part = final["ordinary_parts"][final["part_order"][-1]["index"]]
                    self.assertEqual((part["text"], part["thought_signature"]), ("next forecast", "Zm9yZWNhc3Q="))
