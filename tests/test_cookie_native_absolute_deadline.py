"""A complete-looking signed native Part is not a successful late pull."""
import asyncio
import base64
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

import streaming
from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from upstreams import cookie_native

SIGNATURE = base64.b64encode(b"opaque-signed-call").decode()
PACKET = json.dumps({"results": [{"data": {"candidates": [
    {"content": {"role": "model", "parts": [
        {"functionCall": {"id": "native-call", "name": "weather", "args": {}},
         "thoughtSignature": SIGNATURE}]}, "finishReason": "STOP"}]}}]}).encode()


class CookieNativeDeadlineTests(SchemaRouteHarness):
    async def test_late_signed_part_or_eof_can_never_emit_a_success_terminal(self):
        self.channel = "web_proxy"
        for mode in ("sse", "json"):
            for late in ("part", "eof", None):
                with self.subTest(mode=mode, late=late):
                    now = [10.0]
                    clock = SimpleNamespace(monotonic=lambda: now[0])

                    class Wire(httpx.AsyncByteStream):
                        closed = 0

                        async def __aiter__(self):
                            if late == "part":
                                now[0] = 20.0
                            yield PACKET
                            if late == "eof":
                                now[0] = 20.0

                        async def aclose(self):
                            self.closed += 1

                    wire = Wire()
                    async with httpx.AsyncClient(transport=httpx.MockTransport(
                            lambda _: httpx.Response(200, stream=wire))) as studio:
                        @asynccontextmanager
                        async def opened(*args, **kwargs):
                            yield studio

                        with (patch.object(cookie_native, "open_upstream_client", opened),
                              patch.object(cookie_native, "request_timeout_seconds", return_value=5),
                              patch.object(cookie_native, "time", clock),
                              patch.object(streaming, "time", clock)):
                            got = await self.client.post(
                                f"/v1beta/models/{MODEL}:streamGenerateContent?alt={mode}",
                                json={"contents": [{"role": "user", "parts": [{"text": "hi"}]}]})
                    self.assertEqual((got.status_code, wire.closed), (200, 1), got.text)
                    frames = chunks(got.text) if mode == "sse" else got.json()
                    finishes = [c["finishReason"] for r in frames for c in r.get("candidates", [])
                                if "finishReason" in c]
                    errors = [r["error"]["code"] for r in frames if "error" in r]
                    self.assertEqual((finishes, errors), ([], [504]) if late else (["STOP"], []))
                    self.assertEqual(SIGNATURE in got.text, late != "part")

    async def test_caller_cancel_joins_source_and_does_not_become_a_504(self):
        reading, closed = asyncio.Event(), asyncio.Event()
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))

        async def source(*args, **kwargs):
            try:
                reading.set()
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # A carrier may translate cancellation into ordinary data.
                yield "native_part", {"text": "not delivered"}
            finally:
                closed.set()

        with patch.object(cookie_native, "_events_with_retry", source):
            events = cookie_native._timed_events(request, {}, {}, MODEL, cookie_native.time.monotonic() + 5)
            task = asyncio.create_task(anext(events))
            await asyncio.wait_for(reading.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            await events.aclose()
        self.assertTrue(closed.is_set())
