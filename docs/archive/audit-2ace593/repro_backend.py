"""Offline audit reproducer; prints observed behavior, never contacts Google.

Run after installing requirements-dev.txt. Uses temporary state and fake credentials.
These are diagnostics, not assertions that the defective behavior is desirable.
"""
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'app'))
_state_dir = tempfile.TemporaryDirectory(prefix='vertex-audit-')
os.environ['STATE_DIR'] = _state_dir.name
os.environ['API_KEY'] = 'audit-only'
os.environ['MODELS_CONFIG_URL'] = ''

from fastapi.responses import StreamingResponse
from routes.claude_api import _claude_stream_response
from admission import generation_limiter, limit_generation
from schema_validation import sanitize_for_gemini, validate_json_schema


async def stream_cleanup():
    closed = asyncio.Event()
    produced = 0

    async def source():
        nonlocal produced
        try:
            for _ in range(1000):
                produced += 1
                yield 'data: {"choices":[{"delta":{"content":"x"}}]}\n\n'
        finally:
            closed.set()

    @limit_generation
    async def make_upstream():
        return StreamingResponse(source(), media_type='text/event-stream')
    upstream = await make_upstream()
    response = _claude_stream_response(upstream, 'audit-model', omit_thinking=False)
    await anext(response.body_iterator)  # message_start; emulate a stalled client
    for _ in range(10):
        await asyncio.sleep(0)
    closing = asyncio.create_task(response.body_iterator.aclose())
    done, _ = await asyncio.wait({closing}, timeout=0.15)
    print(json.dumps({'case': 'claude_queue_disconnect', 'produced': produced,
                      'cleanup_completed': bool(done), 'upstream_closed': closed.is_set(),
                      'occupied_slots_after_disconnect': generation_limiter.active}))
    closing.cancel()  # second cancellation only for terminating the reproducer
    await asyncio.gather(closing, return_exceptions=True)


def schemas():
    samples = [
        {'type': 'object', 'properties': {'value': {'type': ['string', 'integer']}}},
        {'type': 'object', 'properties': {'value': {'$ref': '#/$defs/Positive'}},
         '$defs': {'Positive': {'type': 'integer', 'minimum': 1}}},
        {'allOf': [{'type': 'integer'}, {'minimum': 10}]},
    ]
    for original in samples:
        validate_json_schema(original)
        print(json.dumps({'case': 'schema', 'input': original,
                          'output': sanitize_for_gemini(original)}, ensure_ascii=False))


if __name__ == '__main__':
    schemas()
    asyncio.run(stream_cleanup())
