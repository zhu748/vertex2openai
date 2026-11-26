"""Known single-candidate content-filter terminal is ready without generating a token."""
import json
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from test_native_server_parts_first_reply import KeyManager, app, first_reply, native
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness


class NativeKnownRefusalWire(SchemaRouteHarness):
    async def test_refusal_and_trailing_usage_remain_byte_exact(self):
        for sse in (False, True):
            for count, reason, expected in (("1", "RECITATION", 200), (1, "IMAGE_SAFETY", 200),
                                            (2, "RECITATION", 504), (1, "STOP", 504)):
                with self.subTest(sse=sse, count=count, reason=reason):
                    clock = [100.]
                    fake = SimpleNamespace(monotonic=lambda: clock[0], time=lambda: clock[0])
                    raw = json.dumps({"candidates": [{"index": 0, "finishReason": reason}]}).encode()
                    usage = b'{"usageMetadata":{"totalTokenCount":4}}'
                    head, tail = ((b"data: " + raw + b"\n\n", b"data: " + usage + b"\n\n")
                                  if sse else (b"[" + raw + b",", usage + b"]"))

                    class SlowTail(httpx.AsyncByteStream):
                        async def __aiter__(self):
                            yield head
                            clock[0] += 20
                            yield tail

                    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(
                            200, headers={"content-type": "text/event-stream" if sse else "application/json"},
                            stream=SlowTail()))) as upstream:
                        with (patch.object(app.state, "express_key_manager", KeyManager()),
                              patch.object(native.app_state, "get_channel_mode", return_value="api_key"),
                              patch.object(native, "get_retry_settings", return_value=(0, 0, 0)),
                              patch.object(native.passthrough_client_pool, "get", return_value=upstream),
                              patch.object(first_reply, "time", fake),
                              patch.object(first_reply, "timeout_seconds", return_value=10)):
                            r = await self.client.post(f"/v1beta/models/{MODEL}:streamGenerateContent" +
                                ("?alt=sse" if sse else ""), json={"contents": [{"parts": [{"text": "hi"}]}],
                                                                   "generationConfig": {"candidateCount": count}})
                    self.assertEqual(r.status_code, expected, r.text[:200])
                    if expected == 200:
                        self.assertEqual(r.content.removeprefix(b": keep-alive\n\n") if sse else r.content, head + tail)
