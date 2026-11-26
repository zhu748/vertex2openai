"""Local stream chunk boundaries must not change Cookie completion usage."""
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest import mock

import httpx

from test_req_round15_contracts import Round15Harness, chunks, request
from upstreams import cookie_delivery as delivery, cookie_proxy
from usage import estimate_token_count


class CookieUsageFragmentsTests(Round15Harness):
    async def whole(self, parts):
        wire = {"results": [{"data": {"candidates": [{"content": {"parts": parts}, "finishReason": "STOP"}]}}]}

        @asynccontextmanager
        async def opened(*args, **kwargs):
            yield SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(200, json=wire)))

        with mock.patch.object(delivery, "open_upstream_client", opened), \
             mock.patch.object(delivery.credential_manager, "record_cookie_success"):
            reply = await cookie_proxy.CookieProxyUpstream().chat_completions(
                request(), self.http_request, credential_record={
                    "id": "offline", "cookie": "SAPISID=offline", "project_id": "offline"})
        self.assertEqual(reply.status_code, 200)
        return json.loads(reply.body)

    async def streamed(self, parts):
        async def events(*args, **kwargs):
            for part in parts:
                yield ("thought" if part.get("thought") else "text"), part["text"]
            yield "finish", "STOP"

        _, body = await self.cookie(request(stream=True, stream_options={"include_usage": True}), events)
        self.assertTrue(body.endswith("data: [DONE]\n\n"))
        return next(frame["usage"] for frame in chunks(body) if "usage" in frame)

    async def test_one_or_one_hundred_pieces_agree_with_whole_output(self):
        self.settings.update(usage_estimation=True, fake_streaming=False)
        single = [{"text": "a" * 100}]
        expected = (await self.whole(single))["usage"]
        self.assertEqual(expected["completion_tokens"], 25)
        self.assertEqual(await self.streamed(single), expected)
        self.assertEqual(await self.streamed([{"text": "a"}] * 100), expected)

    async def test_interleaved_fields_and_echoed_prefill_agree_with_whole_output(self):
        self.settings.update(usage_estimation=True, fake_streaming=False,
                             inject_prefill="start", prefill_suppress_thinking=False)
        parts = [{"text": "st"}, {"text": "汉", "thought": True}, {"text": "art"},
                 {"text": "ab한"}, {"text": "🌱a", "thought": True}, {"text": "c日"}]
        parts += [{"text": "b", "thought": True}] * 14
        whole = await self.whole(parts)
        message = whole["choices"][0]["message"]
        self.assertEqual(message["content"], "startab한c日")
        self.assertEqual(message["reasoning_content"], "汉🌱a" + "b" * 14)
        self.assertEqual(whole["usage"]["completion_tokens"],
                         estimate_token_count(message["content"] + "\n" + message["reasoning_content"]))
        self.assertEqual(await self.streamed(parts), whole["usage"])

    async def test_thought_only_uses_the_same_logical_diagnostic_as_whole_output(self):
        self.settings.update(usage_estimation=True, fake_streaming=False)
        parts = [{"text": "still ", "thought": True}, {"text": "thinking", "thought": True}]
        self.assertEqual(await self.streamed(parts), (await self.whole(parts))["usage"])

    async def test_no_estimate_never_walks_completion_parts(self):
        class NoScan(list):
            def __iter__(self):
                raise AssertionError("unneeded scan of the response body")

        for want, enabled, metadata, expected in (
            (False, True, None, None),
            (False, True, {"totalTokenCount": 11}, None),
            (True, False, None, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}),
            (True, True, {"totalTokenCount": 11, "thoughtsTokenCount": 3},
             {"prompt_tokens": 0, "completion_tokens": 3, "total_tokens": 11,
              "completion_tokens_details": {"reasoning_tokens": 3}}),
        ):
            self.settings["usage_estimation"] = enabled
            gen = SimpleNamespace(prefill_text="", request_obj=request(), want_usage=want,
                                  response_id="fixture", model_display="local", response_created=0,
                                  account_id="fixture", record=None, base_model_name="local", start_time=0)
            st = delivery._StreamAttemptState(gen)
            st.has_yielded = st.got_text = True
            st.ordinary_metadata = NoScan([{"kind": "text", "text": "x"}])
            st.finish_raw, st.usage_meta = "STOP", metadata
            with self.subTest(want=want, enabled=enabled, metadata=metadata), \
                 mock.patch.object(delivery.credential_manager, "record_cookie_success"):
                body = "".join([frame async for frame in delivery._finalize_cookie_stream(gen, st)])
                usage = [frame["usage"] for frame in chunks(body) if "usage" in frame]
                self.assertEqual(usage, [] if expected is None else [expected])
