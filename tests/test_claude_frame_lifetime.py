import json
from unittest import IsolatedAsyncioTestCase, mock
import weakref

from starlette.responses import StreamingResponse

from routes import claude_api


class Tracked(dict):
    pass


class ClaudeFrameLifetime(IsolatedAsyncioTestCase):
    async def test_decoded_input_is_released_before_translated_events_or_later_ping(self):
        async def source():
            yield b'data: {"choices":[{"index":0,"delta":{"content":"' + b"x" * 65536 + b'"}}]}\n\n'
            yield b": keep-alive\n\n"
            yield b'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n'
            yield b"data: [DONE]\n\n"

        refs = []
        loads = json.loads

        def track(raw, *args, **kw):
            item = Tracked(loads(raw, *args, **kw))
            refs.append(weakref.ref(item))
            return item

        stream = claude_api._claude_stream_response(
            StreamingResponse(source()), "test", omit_thinking=False).body_iterator
        try:
            with mock.patch.object(claude_api.json, "loads", track):
                self.assertIn("message_start", await anext(stream))
                self.assertIn("content_block_start", await anext(stream))
                self.assertEqual(len(refs), 1)
                self.assertIsNone(refs[0]())
                self.assertIn("x" * 65536, await anext(stream))
                self.assertEqual(await anext(stream), claude_api.PING_EVENT)
                self.assertTrue(all(r() is None for r in refs))
                tail = [value async for value in stream]
                self.assertIn("message_stop", tail[-1])
        finally:
            await stream.aclose()
