"""Native Express prefetch: real code Parts release the deadline, not metadata."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

import first_reply
from first_reply import FirstReplyGuard, FirstReplyTimeout
from upstreams import native_passthrough as native


@pytest.mark.parametrize("framing", ["sse", "json"])
@pytest.mark.parametrize("part,valid", [
    ({"executableCode": {"language": "PYTHON", "code": "1 + 1", "id": "example"}}, True),
    ({"codeExecutionResult": {"outcome": "OUTCOME_OK", "output": "2", "id": "example"}}, True),
    ({"codeExecutionResult": {"outcome": "OUTCOME_OK"}}, True),  # stdout is optional
    ({"codeExecutionResult": {"outcome": "OUTCOME_FAILED", "output": ""}}, True),
    ({"codeExecutionResult": {"outcome": "OUTCOME_DEADLINE_EXCEEDED"}}, True),
    ({"thoughtSignature": "c2ln"}, False),
    ({"executableCode": {"id": "example", "language": "PYTHON"}}, False),
    ({"executableCode": {"code": "1", "language": "LANGUAGE_UNSPECIFIED"}}, False),
    ({"executableCode": {"code": " ", "language": "PYTHON"}}, False),
    ({"executableCode": {"code": True, "language": "PYTHON"}}, False),
    ({"executableCode": {"code": "1", "language": []}}, False),
    ({"codeExecutionResult": {"id": "example", "output": "orphan"}}, False),
    ({"codeExecutionResult": {"outcome": "OUTCOME_UNSPECIFIED", "output": "ignored"}}, False),
    ({"codeExecutionResult": {"outcome": [], "output": "ignored"}}, False),
    ({"codeExecutionResult": {"outcome": "OUTCOME_OK", "output": {}}}, False),
    ({"codeExecutionResult": {"outcome": "OUTCOME_OK", "id": 7}}, False),
])
def test_late_tail_after_native_part(framing, part, valid):
    asyncio.run(assert_prefetch(framing, part, valid))


async def assert_prefetch(framing, part, valid):
    now = [100.]
    clock = SimpleNamespace(monotonic=lambda: now[0], time=lambda: now[0])
    first = json.dumps({"candidates": [{"content": {"role": "model", "parts": [part]}}],
                        "usageMetadata": {"totalTokenCount": 1}}).encode()
    terminal = b'{"candidates":[{"finishReason":"STOP"}]}'
    if framing == "sse":
        raw = [b": keepalive\n\n", b"data: " + first + b"\n\n", b"data: " + terminal + b"\n\n"]
        media = "text/event-stream"
    else:
        raw = [b"[", first + b", ", terminal + b"]"]
        media = "application/json"

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield raw[0]
            yield raw[1]
            now[0] += 20  # deterministic idle longer than the first-reply window
            yield raw[2]
    response = httpx.Response(200, headers={"content-type": media}, stream=Body())
    try:
        with patch.object(first_reply, "time", clock):
            guard = FirstReplyGuard("test", seconds=10)
            state = native._NativeStreamState(guard)
            if valid:
                prefetched = await native._prefetch_stream_reply(response, guard, state, framing == "sse")
                try:
                    assert guard.ready and now[0] == 100.
                    assert b"".join([fragment async for fragment in prefetched]) == b"".join(raw)
                finally:
                    await prefetched.aclose()
                assert state.finished == {0} and state.usage == {"totalTokenCount": 1}
            else:
                with pytest.raises(FirstReplyTimeout):
                    await native._prefetch_stream_reply(response, guard, state, framing == "sse")
                assert not guard.ready
    finally:
        await response.aclose()
