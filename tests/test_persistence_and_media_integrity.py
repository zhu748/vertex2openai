"""Regressions for concurrent commits, media fidelity and incomplete native streams."""

import base64
import io
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
import threading
import unittest
from unittest import mock
import zlib

_STATE = tempfile.TemporaryDirectory(prefix="v2o-review-integrity-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from PIL import Image

import image_processing
import message_processing
import model_loader
from models import OpenAIMessage
from response_validation import IncompleteResponseError
from routes import gemini_native
import runtime_state
from upstreams import cookie_request


class PersistenceIntegrityTests(unittest.TestCase):
    def test_reload_supersedes_an_older_snapshot_waiting_to_be_written(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
                runtime_state, "STATE_FILE", str(Path(directory) / "state.json")):
            state = runtime_state.AppState()
            state.update_settings({"retry_max": 1})
            waiting, resume = threading.Event(), threading.Event()
            errors = []

            class GateLock:
                lock = threading.Lock()

                def __enter__(self):
                    if threading.current_thread().name == "stale-save":
                        waiting.set()
                        if not resume.wait(5):
                            raise TimeoutError("reload did not finish")
                    self.lock.acquire()

                def __exit__(self, *_):
                    self.lock.release()

            state._io_lock = GateLock()

            def stale_save():
                try:
                    state.update_settings({"retry_max": 2})
                except BaseException as exc:
                    errors.append(exc)

            writer = threading.Thread(target=stale_save, name="stale-save")
            writer.start()
            try:
                self.assertTrue(waiting.wait(5))
                Path(runtime_state.STATE_FILE).write_text('{"settings":{"retry_max":4}}')
                state.reload()
            finally:
                resume.set()
                writer.join(5)
            self.assertFalse(writer.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(state.get_setting("retry_max"), 4)
            self.assertEqual(json.loads(Path(runtime_state.STATE_FILE).read_text())["settings"]["retry_max"], 4)

    def test_late_save_completion_cannot_roll_back_a_newer_durable_commit(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
                runtime_state, "STATE_FILE", str(Path(directory) / "state.json")):
            state = runtime_state.AppState()
            written, resume = threading.Event(), threading.Event()
            self.addCleanup(resume.set)
            errors = []

            class PausedUnlock:
                lock = threading.Lock()

                def __enter__(self):
                    self.lock.acquire()

                def __exit__(self, *_):
                    self.lock.release()
                    if threading.current_thread().name == "first-save":
                        written.set()
                        if not resume.wait(5):
                            raise TimeoutError("second writer did not finish")

            state._io_lock = PausedUnlock()

            def first_save():
                try:
                    state.update_settings({"retry_max": 1})
                except BaseException as exc:
                    errors.append(exc)

            writer = threading.Thread(target=first_save, name="first-save")
            writer.start()
            try:
                self.assertTrue(written.wait(5))
                state.update_settings({"retry_max": 2})
            finally:
                resume.set()
                writer.join(5)
            self.assertFalse(writer.is_alive())
            self.assertEqual(errors, [])
            with mock.patch.object(runtime_state.os, "replace", side_effect=OSError("disk full")):
                with self.assertRaises(runtime_state.StatePersistenceError):
                    state.update_settings({"retry_max": 3})
            self.assertEqual(state.get_setting("retry_max"), 2)
            self.assertEqual(json.loads(Path(runtime_state.STATE_FILE).read_text())["settings"]["retry_max"], 2)


class MediaIntegrityTests(unittest.TestCase):
    @staticmethod
    def png(width, height):
        source = io.BytesIO()
        Image.new("RGB", (1, 1)).save(source, format="PNG")
        raw = source.getvalue()
        header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
        # Change only the IHDR; no huge pixel allocation or decoding is needed.
        return (raw[:8] + struct.pack(">I", 13) + b"IHDR" + header
                + struct.pack(">I", zlib.crc32(b"IHDR" + header)) + raw[33:])

    def test_oversized_headers_are_rejected_with_compression_on_or_off(self):
        for enabled in (True, False):
            for dimension in (9000, 20000):
                with self.subTest(compression=enabled, dimension=dimension), mock.patch.object(
                        image_processing.app_state, "get_setting",
                        side_effect=lambda key, default=None: enabled if key == "img_compress_enabled" else default):
                    with self.assertRaises(ValueError):
                        image_processing.optimize_image_bytes(self.png(dimension, dimension), "image/png")

    def test_unsafe_remote_images_do_not_crash_either_message_converter(self):
        content = [{"type": "text", "text": "keep this text"},
                   {"type": "image_url", "image_url": {"url": "https://images.example/large.png"}}]
        with mock.patch.object(message_processing, "fetch_remote_image", return_value=(self.png(20000, 20000), "image/png")):
            sdk = message_processing.create_gemini_prompt([OpenAIMessage(role="user", content=content)])
            wire = message_processing.openai_content_to_wire_parts(content)
        self.assertEqual(sdk[0].parts[0].text, "keep this text")
        self.assertEqual(len(sdk[0].parts), 1)
        self.assertEqual(wire, [{"text": "keep this text"}])

    def test_small_images_keep_the_original_bytes(self):
        raw = self.png(1, 1)
        with mock.patch.object(image_processing.app_state, "get_setting", side_effect=lambda k, d=None: d):
            self.assertEqual(image_processing.optimize_image_bytes(raw, "image/png"), (raw, "image/png"))

    def test_invalid_inline_base64_is_rejected_and_folded_base64_still_works(self):
        for value in ("@@@@", "", "AA==junk", "A A A ?"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                image_processing.decode_data_url_image(value)
        raw = self.png(1, 1)
        encoded = base64.b64encode(raw).decode()
        folded = "\r\n".join(encoded[i:i + 16] for i in range(0, len(encoded), 16))
        self.assertEqual(image_processing.decode_data_url_image(folded), raw)

    def test_failed_markdown_images_are_replaced_even_when_all_images_fail(self):
        for encoded in ("A", "@@@@", ""):
            with self.subTest(encoded=encoded):
                text = f"  before\n    ![bad](data:image/png;base64,{encoded})\n  after  "
                parts, clean = message_processing._extract_markdown_images_to_parts(text)
                self.assertEqual(parts, [])
                self.assertEqual(clean, "  before\n    [图片无法处理，已省略]\n  after  ")
                self.assertNotIn("base64", clean)

    def test_folded_markdown_images_are_decoded_instead_of_sent_as_text(self):
        raw = self.png(1, 1)
        encoded = base64.b64encode(raw).decode()
        folded = "\r\n".join(encoded[i:i + 16] for i in range(0, len(encoded), 16))
        parts, clean = message_processing._extract_markdown_images_to_parts(
            f"before ![folded](data:image/png;base64,{folded}) after")
        self.assertEqual(len(parts), 1)
        self.assertEqual(parts[0].inline_data.data, raw)
        self.assertEqual(clean, "before  after")

    def test_successful_markdown_extraction_preserves_surrounding_whitespace_and_order(self):
        text = "  before\n    indented\ttext\n![one](data:image/png;base64,AQ==)  ![two](data:image/png;base64,Ag==)\n"
        with mock.patch.object(message_processing, "optimize_image_bytes", side_effect=lambda raw, mime: (raw, mime)):
            parts, clean = message_processing._extract_markdown_images_to_parts(text)
        self.assertEqual([part.inline_data.data for part in parts], [b"\x01", b"\x02"])
        self.assertEqual(clean, "  before\n    indented\ttext\n  \n")


class CookieCompatibilityTests(unittest.TestCase):
    def test_null_tool_text_keeps_the_other_parts_in_compatibility_mode(self):
        message = OpenAIMessage(role="tool", name="lookup", content=[
            {"type": "text", "text": None}, {"type": "text", "text": "kept"}])
        contents, _ = cookie_request._convert_messages_to_contents([message], native_tools=False)
        self.assertIn("kept", contents[0]["parts"][0]["text"])

    def test_legacy_function_results_count_as_tool_history(self):
        message = OpenAIMessage(role="function", name="lookup", content="result")
        self.assertTrue(cookie_request.classify_tool_traffic([message])["history"])


class NativeCompletionTests(unittest.IsolatedAsyncioTestCase):
    async def consume(self, payload, sse):
        encoded = json.dumps(payload).encode()
        response = httpx.Response(200, content=(b"data: " + encoded + b"\n\n") if sse else encoded)
        source = gemini_native._native_sse_frames(response) if sse else gemini_native._native_json_chunks(response)
        return b"".join([chunk async for chunk in source])

    async def test_metadata_only_native_streams_are_incomplete(self):
        for sse in (True, False):
            for payload in ({}, {"usageMetadata": {"totalTokenCount": 42}},
                            {"candidates": [{"content": {"parts": []}}]}):
                with self.subTest(sse=sse, payload=payload), self.assertRaises(IncompleteResponseError):
                    await self.consume(payload, sse)

    async def test_blocked_and_finished_native_streams_keep_their_original_payload(self):
        for sse in (True, False):
            for payload in ({"promptFeedback": {"blockReason": "SAFETY"}},
                            {"candidates": [{"finishReason": "STOP"}]}):
                with self.subTest(sse=sse, payload=payload):
                    result = await self.consume(payload, sse)
                    self.assertIn(json.dumps(payload).encode(), result)


class LocalCatalogTests(unittest.TestCase):
    def test_a_broken_first_catalog_does_not_hide_a_valid_bundled_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            first, bundled = Path(directory) / "override.json", Path(directory) / "bundled.json"
            bundled.write_text('{"models":["bundled-model"]}')
            for content in ('{"models":', '{"unexpected":[]}', '{"models":[]}'):
                first.write_text(content)
                with self.subTest(content=content), mock.patch.object(
                        model_loader, "_LOCAL_MODEL_FILE_CANDIDATES", [first, bundled]):
                    self.assertEqual(model_loader._load_local_models_config(), {"models": ["bundled-model"]})


if __name__ == "__main__":
    unittest.main(verbosity=2)
