"""Fake SSE must expose the same per-choice OpenAI metadata as buffered delivery."""
import json
from unittest import mock

from google.genai import types

from test_req_round15_contracts import Round15Harness, chunks, request
from upstreams.express_response import _chunk_openai_response_dict_for_sse


class FakeChoiceMetadataTests(Round15Harness):
    async def test_out_of_order_candidates_round_trip_by_vendor_index(self):
        def candidate(index, label, reason):
            return types.Candidate(index=index, finish_reason=reason,
                content=types.Content(role="model", parts=[types.Part(text=label)]),
                logprobs_result=types.LogprobsResult(chosen_candidates=[
                    types.LogprobsResultCandidate(token=label, log_probability=-0.25)]))
        source = types.GenerateContentResponse(candidates=[
            candidate(1, "second answer", "STOP"), candidate(0, "first answer", "MAX_TOKENS")])
        self.settings["fake_streaming"] = True
        generate = mock.AsyncMock(return_value=source)
        req = request(n=2, logprobs=True).model_copy(update={"model": "gemini-2.5-flash"})
        buffered = await self.express(req, generate)
        self.assertEqual(buffered.status_code, 200, buffered.body)
        expected = json.loads(buffered.body)["choices"]
        streamed = await self.express(req.model_copy(update={"stream": True}), generate)
        body = await self.body(streamed)
        frames = [c for f in chunks(body) for c in f["choices"]]
        self.assertEqual(body.count("data: [DONE]"), 1)
        self.assertEqual([c["index"] for c in frames if c["finish_reason"]], [0, 1])
        self.assertEqual(list(dict.fromkeys(c["index"] for c in frames)), [0, 1])
        for original in expected:
            parts = [c for c in frames if c["index"] == original["index"]]
            self.assertEqual([c["delta"]["role"] for c in parts if "role" in c["delta"]], ["assistant"])
            self.assertEqual("".join(c["delta"].get("content", "") for c in parts),
                             original["message"]["content"])
            self.assertEqual([c["finish_reason"] for c in parts if c["finish_reason"]],
                             [original["finish_reason"]])
            self.assertEqual([c["logprobs"] for c in parts if "logprobs" in c], [original["logprobs"]])
            self.assertFalse(any("logprobs" in c["delta"] for c in parts))

    async def test_role_on_empty_choice_and_stable_index_for_tool_deltas(self):
        payload = {"choices": [
            {"index": 4, "message": {"content": None}, "finish_reason": "content_filter"},
            {"index": 0, "message": {"tool_calls": [
                {"id": "once", "function": {"name": "look", "arguments": "{}"}}]}, "finish_reason": "tool_calls"},
            {"message": {"content": "fallback"}, "finish_reason": "stop"},
            {"index": None, "message": {"content": None}, "finish_reason": "stop"}]}
        body = "".join([f async for f in _chunk_openai_response_dict_for_sse(payload)])
        frames = [c for f in chunks(body) for c in f["choices"]]
        self.assertEqual(next(c for c in frames if c["index"] == 4)["delta"].get("role"), "assistant")
        self.assertEqual([c["index"] for c in frames if c["finish_reason"]], [0, 2, 3, 4])
        calls = [(c["index"], tc) for c in frames for tc in c["delta"].get("tool_calls", [])]
        self.assertEqual([index for index, _ in calls], [0, 0])
        self.assertEqual([tc["id"] for _, tc in calls if "id" in tc], ["once"])
        self.assertFalse(any("logprobs" in c for c in frames))
