"""Offline regressions for the iterative resource and delivery review."""
import asyncio
import json
from types import SimpleNamespace
import unittest
from unittest import mock


from fastapi.responses import JSONResponse, StreamingResponse

import admission
import resumable
from request_tracking import RequestTrace, current_trace
from routes import gemini_native
from upstreams import cookie_native

IMAGE_MODEL = "gemini-3.1-flash-image-4k"


def native_request():
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        express_key_manager=SimpleNamespace(get_total_keys=lambda: 0))))


class NativeImageAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.global_before = admission.generation_limiter.active
        self.image_before = admission.image_limiter.active
        self.settings = mock.patch.object(
            admission.app_state, "get_setting",
            side_effect=lambda key, default=None: 1 if key == "image_max_concurrency" else default,
        )
        self.settings.start()
        self.addCleanup(self.settings.stop)

    async def asyncTearDown(self):
        self.assertEqual(admission.generation_limiter.active, self.global_before)
        self.assertEqual(admission.image_limiter.active, self.image_before)

    async def test_native_channels_reject_images_when_shared_gate_is_full(self):
        lease = admission.image_limiter.reserve()
        try:
            for action in ("generateContent", "streamGenerateContent"):
                with self.subTest(channel="express", action=action):
                    with self.assertRaises(admission.GenerationCapacityError):
                        await gemini_native._passthrough_request(native_request(), IMAGE_MODEL, action, "", b"{}")
                with self.subTest(channel="cookie", action=action):
                    with self.assertRaises(admission.GenerationCapacityError):
                        await cookie_native._handle_native_request(
                            None, IMAGE_MODEL, action, {}, requested_model=IMAGE_MODEL, deadline=0,
                        )
        finally:
            lease.release()

    def channels(self):
        return (
            (gemini_native, "_passthrough_request_impl",
             lambda model, action: gemini_native._passthrough_request(None, model, action, "", b"{}")),
            (cookie_native, "_handle_native_request_impl",
             lambda model, action: cookie_native._handle_native_request(
                 None, model, action, {}, requested_model=model, deadline=0)),
        )

    async def test_both_channels_hold_slot_until_eof_and_close(self):
        for module, name, invoke in self.channels():
            for close_early in (False, True):
                with self.subTest(channel=module.__name__, close_early=close_early):
                    async def frames():
                        self.assertEqual(admission.image_limiter.active, self.image_before + 1)
                        yield b"data: {}\n\n"
                    response = StreamingResponse(frames(), media_type="text/event-stream")
                    with mock.patch.object(module, name, return_value=response):
                        response = await invoke(IMAGE_MODEL, "streamGenerateContent")
                    self.assertEqual(admission.image_limiter.active, self.image_before + 1)
                    if close_early:
                        await response.aclose()
                    else:
                        self.assertEqual([f async for f in response.body_iterator], [b"data: {}\n\n"])
                        await response.aclose()
                    self.assertEqual(admission.image_limiter.active, self.image_before)

    async def test_nonstream_errors_and_setup_cancellation_release_slots(self):
        for module, name, invoke in self.channels():
            for outcome in (JSONResponse({"ok": True}), RuntimeError("failed"), asyncio.CancelledError()):
                with self.subTest(channel=module.__name__, outcome=type(outcome).__name__):
                    async def operation(*args, **kwargs):
                        self.assertEqual(admission.image_limiter.active, self.image_before + 1)
                        if isinstance(outcome, BaseException):
                            raise outcome
                        return outcome
                    with mock.patch.object(module, name, side_effect=operation):
                        if isinstance(outcome, BaseException):
                            with self.assertRaises(type(outcome)):
                                await invoke(IMAGE_MODEL, "generateContent")
                        else:
                            self.assertIs(await invoke(IMAGE_MODEL, "generateContent"), outcome)
                    self.assertEqual(admission.image_limiter.active, self.image_before)

    async def test_text_and_token_count_do_not_take_image_slots(self):
        lease = admission.image_limiter.reserve()
        try:
            for module, name, invoke in self.channels():
                for model, action in (("gemini-3.8-flash", "generateContent"), (IMAGE_MODEL, "countTokens")):
                    with self.subTest(channel=module.__name__, model=model, action=action):
                        response = JSONResponse({"ok": True})
                        with mock.patch.object(module, name, return_value=response) as operation:
                            self.assertIs(await invoke(model, action), response)
                            operation.assert_awaited_once()
        finally:
            lease.release()

    async def test_image_gate_preserves_trace_during_iteration_and_cleanup(self):
        for module, name, invoke in self.channels():
            for close_early in (False, True):
                trace = RequestTrace("/v1beta/models/image:streamGenerateContent")
                observed = []

                class Source:
                    def __aiter__(self):
                        return self

                    async def __anext__(self):
                        observed.append(current_trace.get())
                        raise StopAsyncIteration

                    async def aclose(self):
                        observed.append(current_trace.get())

                token = current_trace.set(trace)
                try:
                    with mock.patch.object(module, name, return_value=StreamingResponse(Source())):
                        response = await invoke(IMAGE_MODEL, "streamGenerateContent")
                    if not close_early:
                        self.assertEqual([frame async for frame in response.body_iterator], [])
                    await response.aclose()
                    with self.subTest(channel=module.__name__, close_early=close_early):
                        self.assertTrue(observed)
                        self.assertTrue(all(value is trace for value in observed))
                        self.assertFalse(trace.data["generation_active"])
                finally:
                    current_trace.reset(token)


class GraceReplacementTests(unittest.IsolatedAsyncioTestCase):
    async def test_replacement_waits_for_grace_cancelled_upstream_cleanup(self):
        store = resumable.ResumableStore()
        self.addAsyncCleanup(store.aclose)
        buf, _ = await store.get_or_create_request("same", model="m", kind="stream", fingerprint_key="f")
        started, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def upstream():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()

        task = asyncio.create_task(upstream())
        store.register_pump(buf.id, task, buffer=buf)
        await started.wait()
        buf.last_disconnect_monotonic = 0
        buf.last_disconnect_grace_seconds = 0
        await store._grace_expired(buf)
        await cleaning.wait()
        replacement = asyncio.create_task(store.get_or_create_request(
            "same", model="m", kind="stream", fingerprint_key="f"))
        try:
            done, _ = await asyncio.wait({replacement}, timeout=0.02)
            self.assertFalse(done, "replacement must not start while the old upstream still owns resources")
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            new, created = await replacement
        self.assertTrue(created)
        self.assertIsNot(new, buf)
        self.assertEqual(buf.status_code, 410)


class NativeSizeAccountingTests(unittest.TestCase):
    def test_large_part_accounting_never_materializes_a_full_json_copy(self):
        part = {"inlineData": {"mimeType": "image/png", "data": "A" * (2 * 1024 * 1024)}}
        expected = len(json.dumps(part, ensure_ascii=False).encode("utf-8"))
        dumps = json.dumps
        largest = 0

        def measured(*args, **kwargs):
            nonlocal largest
            result = dumps(*args, **kwargs)
            largest = max(largest, len(result))
            return result

        assembler = cookie_native.NativeAssembler("image", retain_parts=False)
        with mock.patch.object(cookie_native.json, "dumps", side_effect=measured):
            self.assertIs(assembler.feed("native_part", part), part)
        self.assertEqual(assembler.response_bytes, expected)
        self.assertLess(largest, 512 * 1024)

    def test_accounting_matches_utf8_json_with_escapes_and_nested_arguments(self):
        for text in ("ascii", '中文😀\n\t"\\\x00' * 20000, "x" * 65535 + "😀" + "y" * 65536):
            part = {"functionCall": {"name": "write", "args": {"values": [text, True, None, 1.5]}}}
            with self.subTest(chars=len(text)):
                assembler = cookie_native.NativeAssembler("test")
                assembler.feed("native_part", part)
                self.assertEqual(assembler.response_bytes, len(json.dumps(part, ensure_ascii=False).encode("utf-8")))
                self.assertIs(assembler.parts[0], part)

    def test_size_limit_still_rejects_escaped_and_accumulated_content(self):
        part = {"text": "\n" * 70000}
        size = len(json.dumps(part, ensure_ascii=False).encode("utf-8"))
        with mock.patch.object(cookie_native, "MAX_RESPONSE_BYTES", size):
            assembler = cookie_native.NativeAssembler("test")
            assembler.feed("native_part", part)
            with self.assertRaises(cookie_native.NativeBridgeError):
                assembler.feed("native_part", {"text": "tail"})
        with mock.patch.object(cookie_native, "MAX_RESPONSE_BYTES", size - 1):
            with self.assertRaises(cookie_native.NativeBridgeError):
                cookie_native.NativeAssembler("test", retain_parts=False).feed("native_part", part)


if __name__ == "__main__":
    unittest.main()
