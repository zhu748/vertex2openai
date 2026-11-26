"""OpenAI frames keep one creation time for the whole completion, not per write."""
import itertools
import time
from unittest import mock

from google.genai import types

from test_req_round15_contracts import Round15Harness, chunks, request, response
from upstreams import cookie_delivery
from upstreams.cookie_response import iter_openai_content_chunks


class CompletionCreatedTests(Round15Harness):
    def assert_envelope(self, body, *, tools=True):
        frames = chunks(body)
        self.assertTrue(body.endswith("data: [DONE]\n\n"))
        self.assertTrue(all(f.get("object") == "chat.completion.chunk" for f in frames))
        choices = [c for f in frames for c in f["choices"]]
        self.assertTrue(any(c["delta"].get("content") for c in choices))
        if tools:
            self.assertTrue(any(c["delta"].get("tool_calls") for c in choices))
        self.assertTrue(any(c["finish_reason"] for c in choices))
        self.assertTrue(any(not f["choices"] and "usage" in f for f in frames))
        self.assertEqual(len({f["id"] for f in frames}), 1)
        self.assertEqual(len({f["created"] for f in frames}), 1)
        return frames[0]["created"]

    async def test_express_real_and_fake_keep_creation_time_including_usage(self):
        def tool():
            return types.Part(function_call=types.FunctionCall(name="look", args={"n": 4}))

        async def generate(**kwargs):
            return response(types.Part(text="answer"), tool(), usage=True)

        async def stream(**kwargs):
            async def source():
                yield response(types.Part(text="answer"), finish=None)
                yield response(tool(), usage=True)
            return source()

        timestamps = []
        with mock.patch("time.time", side_effect=itertools.count(1_700_000_000)):
            for fake in (False, True):
                with self.subTest(fake=fake):
                    self.settings.update(fake_streaming=fake, inject_prefill="prefix:")
                    result = await self.express(request(stream=True, stream_options={"include_usage": True}),
                                                generate=generate, stream=stream)
                    timestamps.append(self.assert_envelope(await self.body(result)))
        self.assertEqual(len(set(timestamps)), 2)

    async def test_cookie_real_keeps_creation_time_across_retry_and_tool_frames(self):
        attempts = []

        async def events(*args, **kwargs):
            attempts.append(time.time())
            if len(attempts) == 1:
                yield "retryable_error", "busy"
                return
            yield "text", "answer"
            yield "function_call", {"name": "look", "args": {}}
            yield "finish", "STOP"

        times = []
        with mock.patch("time.time", side_effect=itertools.count(1_700_000_000)):
            for _ in range(2):
                _, body = await self.cookie(request(stream=True, stream_options={"include_usage": True}), events,
                    settings={"fake_streaming": False, "inject_prefill": "prefix:", "retry_max": 1,
                              "retry_jitter_seconds": 0})
                times.append(self.assert_envelope(body))
        self.assertEqual(len(attempts), 3)
        self.assertLess(times[0], attempts[0])
        self.assertLess(times[0], times[1])

    async def test_cookie_image_slices_role_finish_and_usage(self):
        whole = "![Image](data:image/png;base64," + "A" * (1 << 21) + ")"
        collected = {"kind": "ok", "full_text": whole, "reasoning_text": "drawing", "finish_reason": "stop"}
        req = request(stream=True, stream_options={"include_usage": True})
        req.model = "gemini-3.1-flash-image"
        unused_events = mock.AsyncMock(side_effect=AssertionError("unexpected text transport"))
        with mock.patch("time.time", side_effect=itertools.count(1_700_000_000)), \
             mock.patch.object(cookie_delivery, "_collect_full_response", mock.AsyncMock(return_value=collected)):
            _, body = await self.cookie(req, unused_events, settings={"fake_streaming": False})
        self.assert_envelope(body, tools=False)
        pieces = [c["delta"]["content"] for f in chunks(body) for c in f["choices"] if "content" in c["delta"]]
        self.assertGreater(len(pieces), 1)
        self.assertEqual("".join(pieces), whole)

    def test_standalone_cookie_slice_helper_captures_one_time(self):
        whole = "![Image](data:image/png;base64," + "A" * (1 << 21) + ")"
        with mock.patch("time.time", side_effect=itertools.count(1_700_000_000)):
            frames = chunks("".join(iter_openai_content_chunks("rid", "m", whole)))
        self.assertGreater(len(frames), 1)
        self.assertEqual(len({f["created"] for f in frames}), 1)
