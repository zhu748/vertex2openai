"""Do not retain the parsed native response tree across downstream backpressure."""
import json
import weakref
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from upstreams import native_passthrough as native


class ParsedTree(dict):
    pass


class Wire:
    def __init__(self, *frames):
        self.frames = frames

    async def aiter_bytes(self):
        for frame in self.frames:
            yield frame


class FrameLifetime(IsolatedAsyncioTestCase):
    async def test_only_intentional_state_survives_a_frame_and_following_comment(self):
        first = (b'data: {"candidates":[{"content":{"parts":[{"text":"reply"}]},'
                 b'"finishReason":"STOP"}],"usageMetadata":{"totalTokenCount":1}}\r\n\r\n')
        heartbeat = b": heartbeat\n\n"
        refs = []
        original = json.loads

        def tracked(raw):
            obj = ParsedTree(original(raw))
            refs.append(weakref.ref(obj))
            return obj

        state = native._NativeStreamState()
        source = native._native_sse_frames(Wire(first, heartbeat), state=state)
        self.addAsyncCleanup(source.aclose)
        with patch.object(native.json, "loads", tracked):
            self.assertEqual(await anext(source), first)
            self.assertIsNone(refs[0]())
            self.assertEqual(state.usage, {"totalTokenCount": 1})
            self.assertEqual(await anext(source), heartbeat)
            with self.assertRaises(StopAsyncIteration):
                await anext(source)
