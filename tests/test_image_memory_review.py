"""Image OOM regressions for pixel copies and large response serialization."""
import asyncio
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import tracemalloc
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-image-memory-review-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from PIL import Image, ImageOps
import image_processing
from fastapi.responses import JSONResponse, StreamingResponse
from http_io import read_limited_bytes
from json_encoding import JSON_STRING_CHARS, LargeJSONResponse, iter_json_bytes
from upstreams import cookie_native
from routes import claude_api
from streaming import ByteBoundedQueue


class ImageOrientationMemoryTests(unittest.TestCase):
    def settings(self):
        values = {"img_compress_enabled": True, "img_compress_max_mb": 0,
                  "img_compress_max_dim": 64, "img_compress_quality": 85}
        return mock.patch.object(image_processing.app_state, "get_setting",
                                 side_effect=lambda key, default=None: values.get(key, default))

    def test_orientation_correction_does_not_copy_an_unrotated_pixel_buffer(self):
        output = io.BytesIO()
        Image.new("RGBA", (96, 64), (120, 80, 40, 128)).save(output, format="PNG")
        with self.settings(), mock.patch.object(ImageOps, "exif_transpose", wraps=ImageOps.exif_transpose) as transpose:
            image_processing.optimize_image_bytes(output.getvalue(), "image/png")
        self.assertEqual(transpose.call_count, 1)
        self.assertTrue(transpose.call_args.kwargs.get("in_place"))

    def test_all_exif_orientations_keep_the_previous_jpeg_bytes(self):
        image = Image.new("RGB", (96, 64))
        image.putdata([(x * 2, y * 3, (x + y) % 255) for y in range(64) for x in range(96)])
        for orientation in range(1, 9):
            with self.subTest(orientation=orientation):
                source = io.BytesIO()
                exif = Image.Exif()
                exif[274] = orientation
                image.save(source, format="PNG", exif=exif)
                data = source.getvalue()
                expected = io.BytesIO()
                with Image.open(io.BytesIO(data)) as original:
                    transposed = ImageOps.exif_transpose(original)
                    transposed.thumbnail((64, 64), Image.Resampling.LANCZOS)
                    transposed.save(expected, format="JPEG", quality=85, optimize=True)
                    transposed.close()
                with self.settings(), mock.patch.object(image_processing, "_MAX_INPUT_PIXELS", 96 * 64 - 1):
                    actual, mime = image_processing.optimize_image_bytes(data, "image/png", max_size_bytes=1024 * 1024)
                # Force compression without triggering the second quality pass.
                self.assertEqual(mime, "image/jpeg")
                self.assertEqual(actual, expected.getvalue())


class JSONEncodingTests(unittest.TestCase):
    def test_wire_bytes_match_the_standard_encoder(self):
        fixtures = [
            {"text": '中文😀\n\t"\\\x00' * 20000, "array": [True, None, 1.2, -4]},
            {1: "int", None: "null", False: "bool", 1.5: "float"},
            {"text": "x" * (JSON_STRING_CHARS - 1) + '"😀\\' + "y" * JSON_STRING_CHARS},
            {"text": "", "repeat": [{"x": [1, 2]}, {"x": [1, 2]}]},
            ([], {}, (), [None], 7, "text"),
        ]
        for compact in (True, False):
            for fixture in fixtures:
                with self.subTest(compact=compact, kind=type(fixture).__name__):
                    expected = json.dumps(fixture, ensure_ascii=False,
                                          separators=(",", ":") if compact else (", ", ": ")).encode()
                    actual = b"".join(iter_json_bytes(fixture, compact=compact))
                    self.assertEqual(actual, expected)

    def test_strict_numbers_bad_types_and_cycles_keep_json_response_errors(self):
        loop = []
        loop.append(loop)
        for content, error in ((loop, ValueError), ({"x": float("nan")}, ValueError),
                               ({"x": float("inf")}, ValueError), ({"x": b"bytes"}, TypeError),
                               ({(1, 2): "key"}, TypeError)):
            with self.subTest(error=error.__name__), self.assertRaises(error):
                LargeJSONResponse(content=content)

    def test_large_json_keeps_headers_status_and_body_without_full_text_copy(self):
        part = {"image": "A" * (8 * 1024 * 1024)}
        expected = JSONResponse(part, status_code=201, headers={"X-Test": "ok"})
        tracemalloc.start()
        try:
            actual = LargeJSONResponse(part, status_code=201, headers={"X-Test": "ok"})
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(actual.body, expected.body)
        self.assertEqual(actual.raw_headers, expected.raw_headers)
        self.assertEqual(actual.status_code, 201)
        self.assertLess(peak, 12 * 1024 * 1024)

    def test_string_fragments_have_a_fixed_memory_bound(self):
        part = {"inlineData": {"data": "\x00" * (1024 * 1024)}}
        chunks = iter_json_bytes(part)
        self.assertLessEqual(max(len(chunk) for chunk in chunks), 6 * JSON_STRING_CHARS)


class HTTPReadMemoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_body_read_has_one_final_buffer_and_preserves_the_limit(self):
        class Source:
            headers = {}

            async def aiter_bytes(self, chunk_size=None):
                for _ in range(128):
                    yield b"A" * (64 * 1024)

        tracemalloc.start()
        try:
            result = await read_limited_bytes(Source(), 8 * 1024 * 1024)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(len(result), 8 * 1024 * 1024)
        self.assertTrue(result.startswith(b"AAAA") and result.endswith(b"AAAA"))
        self.assertLess(peak, 12 * 1024 * 1024)
        with self.assertRaises(ValueError):
            await read_limited_bytes(Source(), 8 * 1024 * 1024 - 1)


class NativeImageDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def native_response(self, alt, *, stream=True):
        self.part = {"inlineData": {"mimeType": "image/png", "data": "A" * (2 * 1024 * 1024)}}
        self.closed = asyncio.Event()

        async def events(*args):
            try:
                yield "native_part", self.part
                yield "finish", "STOP"
            finally:
                self.closed.set()

        record = {"cookie": "SAPISID=test", "project_id": "p", "id": "test"}
        request = SimpleNamespace(query_params={"alt": alt})
        for patcher in (
            mock.patch.object(cookie_native.credential_manager, "get_cookie_record", return_value=record),
            mock.patch.object(cookie_native, "build_headers", return_value={}),
            mock.patch.object(cookie_native, "build_native_body", return_value={}),
            mock.patch.object(cookie_native, "_timed_events", events),
            mock.patch.object(cookie_native.credential_manager, "record_cookie_success"),
        ):
            self.enterContext(patcher)
        return await cookie_native._handle_native_request(
            request, "gemini-3.1-flash-image", "streamGenerateContent" if stream else "generateContent",
            {}, requested_model="gemini-3.1-flash-image", deadline=0)

    async def test_sse_and_array_keep_one_complete_image_with_bounded_writes(self):
        for alt in ("sse", "json"):
            with self.subTest(alt=alt):
                response = await self.native_response(alt)
                try:
                    chunks = [chunk.encode() if isinstance(chunk, str) else chunk async for chunk in response.body_iterator]
                finally:
                    await response.aclose()
                self.assertLess(max(map(len, chunks)), 512 * 1024)
                body = b"".join(chunks)
                if alt == "sse":
                    documents = [json.loads(line[6:]) for line in body.splitlines() if line.startswith(b"data: ")]
                else:
                    documents = json.loads(body)
                self.assertEqual(len(documents), 2)
                self.assertEqual(documents[0]["candidates"][0]["content"]["parts"], [self.part])
                self.assertEqual(documents[1]["candidates"][0]["finishReason"], "STOP")
                self.assertTrue(self.closed.is_set())

    async def test_close_in_the_middle_of_an_image_closes_upstream(self):
        response = await self.native_response("sse")
        try:
            for _ in range(100):
                chunk = await anext(response.body_iterator)
                if len(chunk) > JSON_STRING_CHARS // 2:
                    break
            else:
                self.fail("did not reach the image payload")
        finally:
            await response.aclose()
        self.assertTrue(self.closed.is_set())

    async def test_nonstream_native_image_uses_the_bounded_encoder(self):
        response = await self.native_response("json", stream=False)
        self.assertIsInstance(response, LargeJSONResponse)
        self.assertEqual(json.loads(response.body)["candidates"][0]["content"]["parts"], [self.part])


class ImageReadAheadTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_claude_consumer_does_not_queue_a_whole_image(self):
        produced = 0
        closed = asyncio.Event()
        frame = 'data: ' + json.dumps({"choices": [{"delta": {"content": "A" * (256 * 1024)}}]}) + '\n\n'

        async def upstream():
            nonlocal produced
            try:
                for _ in range(200):
                    produced += 1
                    yield frame
            finally:
                closed.set()

        response = claude_api._claude_stream_response(StreamingResponse(upstream()), "image", omit_thinking=False)
        try:
            await anext(response.body_iterator)
            await asyncio.sleep(0.03)
            self.assertLessEqual(produced, 12)
            self.assertGreater(produced, 1)
        finally:
            await response.aclose()
        self.assertTrue(closed.is_set())

    async def test_cancelled_count_wait_returns_its_byte_reservation(self):
        queue = ByteBoundedQueue(maxsize=1, max_bytes=10)
        await queue.put(b"aaaa")
        pending = asyncio.create_task(queue.put(b"bbbb"))
        await asyncio.sleep(0)
        self.assertEqual(queue._bytes, 8)
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        self.assertEqual(queue._bytes, 4)
        self.assertEqual(await queue.get(), b"aaaa")
        self.assertEqual(queue._bytes, 0)

    async def test_oversize_frame_can_pass_alone_and_next_frame_waits(self):
        queue = ByteBoundedQueue(maxsize=128, max_bytes=10)
        await queue.put(b"x" * 20)
        pending = asyncio.create_task(queue.put(b"next"))
        await asyncio.sleep(0)
        self.assertFalse(pending.done())
        self.assertEqual(await queue.get(), b"x" * 20)
        await pending
        self.assertEqual(await queue.get(), b"next")
        self.assertEqual(queue._bytes, 0)


if __name__ == "__main__":
    unittest.main()
