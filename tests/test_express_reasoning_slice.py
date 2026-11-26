import json
from unittest import IsolatedAsyncioTestCase

from message_processing import GIANT_CONTENT_SLICE, GIANT_CONTENT_THRESHOLD
from upstreams.express_response import _chunk_openai_response_dict_for_sse, iter_giant_chunk_payload


class ExposedReasoningSlices(IsolatedAsyncioTestCase):
    async def test_true_and_fake_large_reasoning_and_large_text_preserve_topology(self):
        thought = "中" * GIANT_CONTENT_THRESHOLD + "🙂"
        content = "y" * GIANT_CONTENT_THRESHOLD + "!"
        sig = {"marker": "same"}
        tool = {"id": "one", "type": "function", "function": {"name": "f", "arguments": "{}"}}
        metadata = {"content": [{"token": "y", "logprob": -1}]}
        for form in ("true", "fake"):
            with self.subTest(form=form):
                message = {"role": "assistant", "reasoning_content": thought, "content": content,
                           "extra_content": sig, "tool_calls": [tool]}
                if form == "true":
                    case = {"choices": [{"index": 2, "delta": message, "finish_reason": "tool_calls",
                                         "logprobs": metadata}]}
                    frames = list(iter_giant_chunk_payload(case))
                else:
                    case = {"choices": [{"index": 2, "message": message, "finish_reason": "tool_calls",
                                         "logprobs": metadata}]}
                    frames = [json.loads(raw[6:]) async for raw in _chunk_openai_response_dict_for_sse(case)
                              if raw != "data: [DONE]\n\n"]
                parts = [frame["choices"][0] for frame in frames]
                self.assertEqual({part["index"] for part in parts}, {2})
                deltas = [part["delta"] for part in parts]
                for key, want in (("reasoning_content", thought), ("content", content)):
                    data = [d[key] for d in deltas if key in d]
                    self.assertLessEqual(max(map(len, data)), GIANT_CONTENT_SLICE)
                    self.assertEqual("".join(data), want)
                self.assertLess(max(i for i, d in enumerate(deltas) if "reasoning_content" in d),
                                min(i for i, d in enumerate(deltas) if "content" in d))
                self.assertEqual([d["role"] for d in deltas if "role" in d], ["assistant"])
                self.assertEqual([d["extra_content"] for d in deltas if "extra_content" in d], [sig])
                self.assertEqual([tc["id"] for d in deltas for tc in d.get("tool_calls", []) if "id" in tc], ["one"])
                self.assertEqual([p["finish_reason"] for p in parts if p.get("finish_reason")], ["tool_calls"])
                self.assertEqual([p["logprobs"] for p in parts if "logprobs" in p], [metadata])
                self.assertEqual(parts[-1]["finish_reason"], "tool_calls")

    def test_short_reasoning_keeps_original_chunk_identity(self):
        small = {"choices": [{"delta": {"role": "assistant", "reasoning_content": "existing"}}]}
        self.assertIs(next(iter_giant_chunk_payload(small)), small)
