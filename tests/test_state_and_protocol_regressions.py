"""Offline regressions for state upgrades, log races and protocol boundaries."""

import asyncio
from contextlib import asynccontextmanager
import json
import io
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-audit-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

import config
import image_processing
import message_processing
import model_capabilities
import runtime_state
from first_reply import FirstReplyGuard
from logger import SSELogger
from models import OpenAIMessage, OpenAIRequest
from pydantic import ValidationError
from response_validation import IncompleteResponseError
from routes import console, gemini_native
from upstreams import cookie_proxy, cookie_request, express_sdk
from PIL import Image


class StateUpgradeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "state.json"
        patch = mock.patch.object(runtime_state, "STATE_FILE", str(self.path))
        patch.start()
        self.addCleanup(patch.stop)

    def test_legacy_thinking_switches_match_the_runtime_and_console(self):
        for flag, expected in (("hide_thoughts", "off"), ("thinking_force_console", "console")):
            with self.subTest(flag=flag):
                self.path.write_text(json.dumps({"settings": {flag: True}}))
                state = runtime_state.AppState()
                settings = state.get_effective_settings("gemini-3.6-flash")
                self.assertEqual(state.get_settings()["native_thinking_mode"], expected)
                result = model_capabilities.resolve_thinking("gemini-3.6-flash", None, settings)
                self.assertEqual(result["include_thoughts"], expected != "off")
                state.update_settings({"native_thinking_mode": "request"})
                self.assertEqual(state.get_settings()["native_thinking_mode"], "request")

    def test_reload_replaces_removed_fields_and_rollback_uses_the_new_snapshot(self):
        self.path.write_text(json.dumps({"settings": {"retry_max": 1}, "google_project_id": "old"}))
        state = runtime_state.AppState()
        self.path.write_text(json.dumps({"settings": {"retry_max": 2}}))
        state.reload()
        self.assertNotIn("google_project_id", state._state)
        with mock.patch.object(runtime_state.os, "replace", side_effect=OSError("disk full")):
            with self.assertRaises(runtime_state.StatePersistenceError):
                state.update_settings({"retry_max": 3})
        self.assertEqual(state.get_setting("retry_max"), 2)
        self.assertNotIn("google_project_id", state._state)


class LogOrderingTests(unittest.IsolatedAsyncioTestCase):
    async def test_log_cursor_from_before_restart_does_not_hide_new_history(self):
        logger = SSELogger()
        logger.push("fresh startup")
        request = SimpleNamespace(headers={"last-event-id": "9000"},
                                  is_disconnected=mock.AsyncMock(return_value=False))
        with mock.patch.object(console, "rt_logger", logger):
            response = await console.stream_logs_endpoint(request, True)
            try:
                first = await asyncio.wait_for(anext(response.body_iterator), 1)
                self.assertEqual(first, "id: 1\n")
            finally:
                await response.body_iterator.aclose()

    async def test_overlap_between_history_and_subscription_is_delivered_once(self):
        class OverlappingLogger(SSELogger):
            def snapshot_history(self):
                self.push("published while taking history")
                return super().snapshot_history()

        logger = OverlappingLogger()
        request = SimpleNamespace(headers={}, is_disconnected=mock.AsyncMock(return_value=False))

        async def event(iterator):
            chunks = []
            while (chunk := await anext(iterator)) != "\n":
                chunks.append(chunk)
            return "".join(chunks)

        with mock.patch.object(console, "rt_logger", logger):
            response = await console.stream_logs_endpoint(request, True)
            try:
                first = await event(response.body_iterator)
                logger.push("next live event")
                await asyncio.sleep(0)
                second = await asyncio.wait_for(event(response.body_iterator), 1)
            finally:
                await response.body_iterator.aclose()
        self.assertTrue(first.startswith("id: 1\n"))
        self.assertTrue(second.startswith("id: 2\n"), second)
        self.assertFalse(logger._subscribers)

    def test_threaded_publishers_schedule_events_in_sequence_order(self):
        first_entered, release_first, second_submitted = (threading.Event() for _ in range(3))
        scheduled = []

        class Loop:
            def is_closed(self):
                return False

            def call_soon_threadsafe(self, callback, queue, item):
                if item[0] == 1:
                    first_entered.set()
                    release_first.wait(1)
                scheduled.append(item[0])
                if item[0] == 2:
                    second_submitted.set()

        logger = SSELogger()
        logger.subscribe(Loop())
        first = threading.Thread(target=logger.push, args=("first",))
        second = threading.Thread(target=logger.push, args=("second",))
        first.start()
        try:
            self.assertTrue(first_entered.wait(1))
            second.start()
            second_submitted.wait(0.05)
        finally:
            release_first.set()
            first.join(1)
            second.join(1)
        self.assertFalse(first.is_alive() or second.is_alive())
        self.assertEqual(scheduled, [1, 2])


class RequestBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_tool_content_parts_remain_json_serializable_in_both_channels(self):
        message = OpenAIMessage(role="tool", name="lookup", content=[{"type": "text", "text": "result"}])
        sdk = message_processing.create_gemini_prompt([message], "gemini-3.6-flash")
        wire, _ = cookie_request._convert_messages_to_contents([message], "gemini-3.6-flash")
        expected = {"result": [{"type": "text", "text": "result"}]}
        self.assertEqual(sdk[0].parts[0].function_response.response, expected)
        self.assertEqual(wire[0]["parts"][0]["functionResponse"]["response"], expected)
        json.dumps(wire)

    def test_unknown_or_malformed_google_metadata_falls_back_to_normal_content(self):
        for google in ([1], {"ordinary_parts": [{"kind": [], "text": "bad"}]}):
            with self.subTest(google=google):
                message = OpenAIMessage(role="assistant", content="plain", extra_content={"google": google})
                sdk = message_processing.create_gemini_prompt([message], "gemini-3.6-flash")
                wire, _ = cookie_request._convert_messages_to_contents([message], "gemini-3.6-flash")
                self.assertEqual(sdk[0].parts[0].text, "plain")
                self.assertEqual(wire[0]["parts"][0]["text"], "plain")

    def test_nonfinite_request_parameters_are_rejected_by_the_contract(self):
        for field in ("temperature", "top_p", "presence_penalty", "frequency_penalty"):
            for value in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(field=field, value=value), self.assertRaises(ValidationError):
                    OpenAIRequest(model="m", messages=[], **{field: value})

    async def test_malformed_nested_fields_fail_before_credentials_or_network(self):
        manager = SimpleNamespace(get_total_keys=lambda: 0)
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)))
        invalid = [
            {"tool_choice": {"type": "function", "function": "bad"}},
            {"tool_choice": {"type": "function", "function": {"name": 1}}},
            {"messages": [{"role": "assistant", "tool_calls": [{"function": "bad"}]}]},
            {"messages": [{"role": "assistant", "tool_calls": [{"function": {"name": []}}]}]},
            {"messages": [{"role": "user", "content": [{"type": "text", "text": 123}]}]},
            {"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": 123}}]}]},
        ]
        with mock.patch.object(cookie_proxy.credential_manager, "get_cookie_record", return_value=None), \
             mock.patch.object(cookie_proxy, "_get_cookie_string", return_value=""):
            for upstream in (express_sdk.ExpressSDKUpstream(), cookie_proxy.CookieProxyUpstream()):
                for patch in invalid:
                    with self.subTest(upstream=type(upstream).__name__, patch=patch):
                        obj = OpenAIRequest(**{"model": "gemini-3.6-flash", "messages": [], **patch})
                        result = await upstream.chat_completions(obj, request)
                        self.assertEqual(result.status_code, 400)
                        self.assertEqual(json.loads(result.body)["error"]["type"], "invalid_request_error")


class ImageCompressionTests(unittest.TestCase):
    def compress(self, image, format, **options):
        source = io.BytesIO()
        image.save(source, format=format, **options)
        with mock.patch.object(image_processing.app_state, "get_setting", side_effect=lambda k, d=None: d):
            data, mime = image_processing.optimize_image_bytes(source.getvalue(), "image/" + format.lower(), 0)
        self.assertEqual(mime, "image/jpeg")
        return Image.open(io.BytesIO(data))

    def test_compression_preserves_camera_orientation(self):
        with Image.new("RGB", (40, 20), "red") as image:
            exif = Image.Exif()
            exif[274] = 6  # Rotate 90 degrees clockwise for display.
            with self.compress(image, "JPEG", exif=exif) as compressed:
                self.assertEqual(compressed.size, (20, 40))

    def test_palette_transparency_is_flattened_to_white(self):
        with Image.new("P", (20, 20), 0) as image:
            image.putpalette([0, 0, 0] + [255, 0, 0] * 255)
            with self.compress(image, "PNG", transparency=0) as compressed:
                self.assertTrue(all(channel > 245 for channel in compressed.getpixel((10, 10))))


class NativeJSONTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def upstream(chunks):
        class Bytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                for chunk in chunks:
                    yield chunk
        return httpx.Response(200, stream=Bytes(), headers={"content-type": "application/json"})

    async def test_json_array_preserves_bytes_and_split_utf8(self):
        body = json.dumps([{"candidates": [{"index": 0, "content": {"parts": [{"text": '你好 {"🌍"}'}]}}]},
                           {"candidates": [{"index": 0, "finishReason": "STOP"}]}], ensure_ascii=False).encode()
        upstream = self.upstream([body[i:i+1] for i in range(len(body))])
        guard = FirstReplyGuard("gemini-3.6-flash")
        result = b"".join([chunk async for chunk in gemini_native._native_json_chunks(upstream, guard)])
        self.assertEqual(result, body)
        self.assertTrue(guard.ready)
        await upstream.aclose()

    async def test_json_stream_cannot_hide_truncation_or_errors_after_first_reply(self):
        prefix = b'[{"candidates":[{"content":{"parts":[{"text":"reply"}]}}]},'
        for tail in (b'{"unfinished":', b'{"error":{"code":503,"message":"overloaded"}}]',
                     b'{"candidates":[{"finishReason":"STOP"}]}', b']'):
            with self.subTest(tail=tail):
                upstream = self.upstream([prefix, tail])
                guard = FirstReplyGuard("gemini-3.6-flash")
                with self.assertRaises((IncompleteResponseError, gemini_native.NativeStreamError)):
                    _ = [chunk async for chunk in gemini_native._native_json_chunks(upstream, guard)]
                self.assertTrue(guard.ready)
                await upstream.aclose()

    async def test_json_validation_remains_active_with_first_reply_timeout_off(self):
        upstream = self.upstream([b'[{"candidates":[{"content":{"parts":[{"text":"reply"}]}}]}'])
        response = await gemini_native._relay_stream(upstream)
        with self.assertRaises(IncompleteResponseError):
            _ = [chunk async for chunk in response.body_iterator]
        await response.aclose()
        self.assertTrue(upstream.is_closed)

    async def test_large_native_image_is_not_mistaken_for_pre_reply_metadata(self):
        body = json.dumps([{"candidates": [{"content": {"parts": [
            {"inlineData": {"mimeType": "image/png", "data": "A" * (3 * 1024 * 1024)}}]},
            "finishReason": "STOP"}]}]).encode()
        upstream = self.upstream([body[i:i+65536] for i in range(0, len(body), 65536)])
        manager = SimpleNamespace(get_total_keys=lambda: 1, get_express_record=lambda **kw:
                                  {"key": "offline", "project_id": "", "proxies": ["direct"]})
        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False),
            app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)))
        with mock.patch.object(gemini_native, "_passthrough_once", mock.AsyncMock(return_value=upstream)), \
             mock.patch.object(gemini_native, "passthrough_client_pool",
                               SimpleNamespace(get=lambda *args: None)), \
             mock.patch.object(runtime_state.app_state, "get_settings", return_value=
                               dict(config.DEFAULT_SETTINGS, express_location="", retry_max=0)):
            response = await gemini_native._passthrough_request(
                request, "gemini-3.6-flash", "streamGenerateContent", "", b"{}")
            try:
                self.assertEqual(response.status_code, 200)
                self.assertEqual(b"".join([chunk async for chunk in response.body_iterator]), body)
            finally:
                await response.aclose()


class CookieImageTests(unittest.IsolatedAsyncioTestCase):
    async def test_image_stream_uses_prefill_thinking_policy_and_hides_thoughts(self):
        settings = dict(config.DEFAULT_SETTINGS, prefill_suppress_thinking=True)
        body_seen = []
        payload = {"results": [{"data": {"candidates": [{"content": {"parts": [
            {"text": "hidden image reasoning", "thought": True},
            {"inlineData": {"mimeType": "image/png", "data": "AA=="}},
        ]}, "finishReason": "STOP"}]}}]}
        response_text = json.dumps(payload)

        async def post(url, *, headers, json):
            body_seen.append(json)
            return SimpleNamespace(status_code=200, text=response_text)

        @asynccontextmanager
        async def client(options):
            yield SimpleNamespace(post=post)

        obj = OpenAIRequest(model="gemini-3.1-flash-image", stream=True, messages=[
            {"role": "user", "content": "draw a tree"},
            {"role": "assistant", "content": "Draw in line art."},
        ])
        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))
        with mock.patch.object(runtime_state.app_state, "get_settings", return_value=settings), \
             mock.patch.object(runtime_state.app_state, "get_setting", side_effect=lambda k, d=None: settings.get(k, d)), \
             mock.patch.object(cookie_proxy, "open_upstream_client", client):
            response = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, request,
                credential_record={"cookie": "SAPISID=offline; SID=offline", "project_id": "project"})
            frames = "".join([frame async for frame in response.body_iterator])
        thinking = body_seen[0]["variables"]["generationConfig"]["thinkingConfig"]
        self.assertFalse(thinking["includeThoughts"])
        self.assertEqual(thinking["thinkingLevel"], "MINIMAL")
        self.assertNotIn("hidden image reasoning", frames)
        self.assertIn("data:image/png;base64,AA==", frames)
        self.assertIn("Draw in line art.", frames)


if __name__ == "__main__":
    unittest.main(verbosity=2)
