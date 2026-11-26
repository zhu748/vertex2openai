"""A completed ordinary Express reply is independent of saturated image workers."""
import threading
from types import SimpleNamespace
from unittest import mock

import httpx
from google.genai import types

import config
import main
import media_tasks
from test_req_round15_contracts import Round15Harness, chunks, content, response
from upstreams import express_response, express_sdk


def test_only_bounded_plain_sdk_candidates_use_text_preparation():
    needs_media = express_response.completed_response_needs_media_pool
    plain = response(types.Part(text="a"), types.Part(text="b", thought=False), usage=True)
    plain.candidates[0].safety_ratings = [types.SafetyRating(category="HARM_CATEGORY_HARASSMENT", probability="LOW")]
    with mock.patch.object(types.GenerateContentResponse, "text",
                           new=mock.PropertyMock(side_effect=AssertionError("computed SDK text"))):
        assert not needs_media(plain)
        assert not needs_media(types.GenerateContentResponse(candidates=plain.candidates * 2))
        for extra in (types.Part(inline_data=types.Blob(data=b"123", mime_type="image/png")),
                      types.Part(file_data=types.FileData(file_uri="gs://fixture/image.png")),
                      types.Part(function_call=types.FunctionCall(name="sample")),
                      types.Part(text="thinking", thought=True),
                      types.Part(text="signed", thought_signature=b"sig"),
                      types.Part(text="unknown", part_metadata={"extra": 1})):
            assert needs_media(response(types.Part(text="first"), extra))
        assert needs_media(response(*[types.Part(text="")] * (express_response._PLAIN_RESPONSE_PARTS + 1)))
        text = "x" * (express_response._PLAIN_RESPONSE_CHARS // 2 + 1)
        assert needs_media(response(types.Part(text=text), types.Part(text=text)))
        assert needs_media(types.GenerateContentResponse(candidates=plain.candidates * 5))
        assert needs_media(response())
        assert needs_media(SimpleNamespace(candidates=plain.candidates))
        for field, value in (("safety_ratings", plain.candidates[0].safety_ratings * 9),
                             ("logprobs_result", types.LogprobsResult())):
            unusual = response(types.Part(text="simple"))
            setattr(unusual.candidates[0], field, value)
            assert needs_media(unusual)


class CompletedAdmissionTests(Round15Harness):
    async def test_saturated_media_queue_does_not_discard_a_completed_small_reply(self):
        whole = mock.AsyncMock()
        sdk = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=whole)))
        media_slot = threading.BoundedSemaphore(1)
        mixed = response(types.Part(text="hello"), types.Part(inline_data=types.Blob(data=b"123", mime_type="image/png")))
        plain = response(types.Part(text="hello"), usage=True)
        with (media_slot, mock.patch.object(media_tasks, "_media_slots", media_slot),
              mock.patch.object(main.app.state, "express_key_manager", self.key_manager),
              mock.patch.object(express_sdk, "get_pooled_express_client", return_value=sdk)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                for fake in (False, True):
                    self.settings.update(fake_streaming=fake, channel_mode="api_key")
                    for expected, source in ((200, plain), (503, mixed)):
                        with self.subTest(fake=fake, expected=expected):
                            whole.reset_mock()
                            whole.return_value = source
                            result = await client.post("/v1/chat/completions", headers={"Authorization": "Bearer " + config.API_KEY},
                                json={"model": "gemini-2.5-flash", "stream": fake, "stream_options": {"include_usage": True},
                                      "messages": [{"role": "user", "content": "hello"}]})
                            whole.assert_awaited_once()
                            if fake:
                                frames = chunks(result.text)
                                if expected == 200:
                                    self.assertEqual(content(result.text), "hello")
                                    self.assertEqual([f["usage"]["total_tokens"] for f in frames if f.get("usage")], [15])
                                    self.assertTrue(result.text.endswith("data: [DONE]\n\n"))
                                else:
                                    self.assertEqual([f["error"]["code"] for f in frames if "error" in f], [503])
                            else:
                                self.assertEqual(result.status_code, expected)
                                if expected == 200:
                                    self.assertEqual(result.json()["choices"][0]["message"]["content"], "hello")
                                    self.assertEqual(result.json()["usage"]["total_tokens"], 15)
