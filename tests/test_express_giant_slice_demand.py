"""True SSE must only build the large content slices a consumer has requested."""
from unittest.mock import patch

from google.genai import types

from message_processing import GIANT_CONTENT_THRESHOLD, GIANT_CONTENT_SLICE
from test_req_round15_contracts import Round15Harness, chunks, request, response
from upstreams import express_response


class GiantSliceDemandTests(Round15Harness):
    async def test_stopping_real_stream_at_first_data_does_not_slice_the_tail(self):
        self.settings["fake_streaming"] = False
        text = "x" * (4 * 1024 * 1024)
        sliced = 0
        original = express_response.iter_giant_content

        def observe_slices(*args, **kwargs):
            nonlocal sliced
            for piece in original(*args, **kwargs):
                sliced += 1
                yield piece

        async def generate(**kwargs):
            async def source():
                yield response(types.Part(text=text), finish=None)
                yield response(finish="STOP")
            return source()

        with patch.object(express_response, "iter_giant_content", observe_slices):
            result = await self.express(request(stream=True), stream=generate)
            frames = result.body_iterator
            try:
                async for frame in frames:
                    if frame.startswith("data: "):
                        [choice] = chunks(frame)[0]["choices"]
                        self.assertEqual(choice["delta"]["content"], text[:GIANT_CONTENT_SLICE])
                        break
                else:
                    self.fail("no content frame")
                self.assertEqual(sliced, 1)
            finally:
                await frames.aclose()
            self.assertEqual(sliced, 1)

    def test_split_preserves_every_choice_and_each_field_once(self):
        text = "🎋" + "x" * GIANT_CONTENT_THRESHOLD + "終"
        envelope = {"id": "resp", "object": "chat.completion.chunk", "created": 42, "model": "m"}
        logprobs = {"content": [{"token": "x", "logprob": -1}]}
        tools = [{"index": 0, "function": {"arguments": "{}"}}]
        small = {"index": 4, "delta": {"content": "short"}, "finish_reason": "stop"}
        for choices in (
            [small, {"index": 1, "delta": {"role": "assistant", "content": text, "tool_calls": tools},
                     "finish_reason": "tool_calls", "logprobs": logprobs}],
            [{"index": 1, "delta": {"role": "assistant", "content": text, "tool_calls": tools},
              "finish_reason": "tool_calls", "logprobs": logprobs}, small],
        ):
            with self.subTest(order=[c["index"] for c in choices]):
                payload = {**envelope, "choices": choices}
                frames = express_response.split_giant_chunk_payload(payload)
                self.assertIsInstance(frames, list)  # Public compatibility API.
                expected = []
                for c in choices:
                    if c is small:
                        expected.append({**envelope, "choices": [small]})
                        continue
                    for start in range(0, len(text), GIANT_CONTENT_SLICE):
                        first, last = start == 0, start + GIANT_CONTENT_SLICE >= len(text)
                        delta = {"role": "assistant", "tool_calls": tools} if first else {}
                        delta["content"] = text[start:start + GIANT_CONTENT_SLICE]
                        choice = {"index": 1, "delta": delta, "finish_reason": "tool_calls" if last else None}
                        if last:
                            choice["logprobs"] = logprobs
                        expected.append({**envelope, "choices": [choice]})
                # Compare one frame at a time; never render MiB-long strings on a topology error.
                self.assertEqual(len(frames), len(expected))
                for frame, wanted in zip(frames, expected, strict=True):
                    self.assertEqual(frame, wanted)
