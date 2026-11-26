"""Ensure Express delivery accepts useful parts and explicit filtered terminals."""
import json
from unittest import mock

from google.genai import types

from test_req_round15_contracts import Round15Harness, chunks, content, request, response


class BufferedTerminalTests(Round15Harness):
    async def test_file_reference_and_filtered_terminal_survive_delivery(self):
        for source, reason, text in [
            (response(types.Part(file_data=types.FileData(file_uri="gs://test/doc",
                      mime_type="application/pdf"))), "stop", "[File](gs://test/doc)"),
            (response(finish="SAFETY"), "content_filter", ""),
            (response(types.Part(text="partial"), finish="MAX_TOKENS"), "length", "partial"),
        ]:
            for fake in (False, True):
                with self.subTest(reason=reason, fake=fake):
                    self.settings["fake_streaming"] = fake
                    result = await self.express(request(stream=fake), mock.AsyncMock(return_value=source))
                    if fake:
                        body = await self.body(result)
                        self.assertEqual([c["finish_reason"] for p in chunks(body)
                                          for c in p["choices"] if c["finish_reason"]], [reason])
                        self.assertEqual(content(body), text)
                    else:
                        self.assertEqual(result.status_code, 200)
                        choice = json.loads(result.body)["choices"][0]
                        self.assertEqual(choice["finish_reason"], reason)
                        self.assertEqual(choice["message"].get("content") or "", text)

    async def test_empty_plain_terminal_and_blocked_prompt_remain_failures(self):
        blocked = response(finish="SAFETY")
        blocked.prompt_feedback = types.GenerateContentResponsePromptFeedback(block_reason="SAFETY")
        for source in (response(finish="STOP"), response(finish="MAX_TOKENS"), blocked):
            for fake in (False, True):
                with self.subTest(reason=source.candidates[0].finish_reason, fake=fake):
                    self.settings["fake_streaming"] = fake
                    if fake:
                        result = await self.express(request(stream=True), mock.AsyncMock(return_value=source))
                        self.assertTrue(any("error" in frame for frame in chunks(await self.body(result))))
                    else:
                        with self.assertRaisesRegex(ValueError, "无有效内容|安全策略拦截"):
                            await self.express(request(), mock.AsyncMock(return_value=source))
