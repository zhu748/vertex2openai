"""Translated SDK media needs actual bytes or a URI, not just a MIME type."""
import json
import time
from contextlib import aclosing
from unittest.mock import AsyncMock

from google.genai import types

from first_reply import FirstReplyGuard, FirstReplyTimeout, sdk_has_reply
from message_processing import process_gemini_response_to_openai_dict
from test_req_round15_contracts import Round15Harness, chunks, content, request, response
from upstreams.express_response import _build_chunk_payload, is_gemini_response_valid


EMPTY = (types.Part(inline_data=types.Blob(mime_type="image/png")),
         types.Part(inline_data=types.Blob(mime_type="image/png", data=b"")),
         types.Part(file_data=types.FileData(mime_type="image/png")),
         types.Part(file_data=types.FileData(mime_type="image/png", file_uri=" \t")))


class SDKMediaPresence(Round15Harness):
    async def test_empty_containers_never_win_first_reply_or_invent_content(self):
        for part in EMPTY:
            self.assertFalse(sdk_has_reply(response(part)))
            self.assertFalse(is_gemini_response_valid(response(part)))
            with self.subTest(part=part):
                src = response(types.Part(text="real"), part)
                for mode in ("json", "true", "fake"):
                    self.settings["fake_streaming"] = mode == "fake"

                    async def stream(**kwargs):
                        async def gen():
                            yield src
                        return gen()

                    r = await self.express(request(stream=mode != "json"), AsyncMock(return_value=src), stream)
                    if mode == "json":
                        msg = json.loads(r.body)["choices"][0]["message"]
                        self.assertEqual(msg["content"], "real")
                        self.assertNotIn('"kind": "image"', json.dumps(msg))
                    else:
                        body = await self.body(r)
                        self.assertEqual(content(body), "real")
                        self.assertNotIn('"kind": "image"', json.dumps(chunks(body)))

    async def test_metadata_only_sdk_chunk_does_not_cancel_first_reply_deadline(self):
        async def source():
            yield response(EMPTY[0], finish=None)
            yield response(types.Part(text="late"))
        guard = FirstReplyGuard("model", seconds=10)
        async with aclosing(source()) as stream, aclosing(guard.iterate(stream, sdk_has_reply)) as observed:
            await anext(observed)
            guard.deadline = time.monotonic() - 1
            with self.assertRaises(FirstReplyTimeout):
                await anext(observed)

    def test_real_bytes_and_files_keep_rendering_and_signature_only_keeps_its_slot(self):
        for part, expected in (
            (types.Part.from_bytes(data=b"x", mime_type="image/png"), "![Image](data:image/png;base64,eA==)"),
            (types.Part.from_uri(file_uri="gs://local/object", mime_type="image/png"), "![Image](gs://local/object)"),
        ):
            src = response(part, types.Part(inline_data=types.Blob(mime_type="image/png"), thought_signature=b"sig"))
            self.assertTrue(sdk_has_reply(src))
            self.assertTrue(is_gemini_response_valid(src))
            msg = process_gemini_response_to_openai_dict(src, "model")["choices"][0]["message"]
            delta = _build_chunk_payload(src, "model", "id")["choices"][0]["delta"]
            for rendered in (msg, delta):
                self.assertEqual(rendered["content"], expected)
                parts = rendered["extra_content"]["google"]["ordinary_parts"]
                self.assertEqual([p["kind"] for p in parts], ["image", "signature_only"])
                self.assertEqual(parts[1]["thought_signature"], "c2ln")
