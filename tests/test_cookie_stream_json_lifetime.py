"""A paused wire source must not pin the last successfully delivered object or its text."""
import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import weakref

from json_stream import JsonObjects
from upstreams import cookie_response, cookie_transport


class Chunk(str):
    pass


class Object(dict):
    pass


def packet(candidate):
    return json.dumps({"results": [{"data": {"candidates": [candidate]}}]})


class PausedWire:
    status_code = 200

    def __init__(self, *, metadata_only=False):
        self.metadata_only = metadata_only
        self.started, self.resume = asyncio.Event(), asyncio.Event()
        self.reads = self.closes = 0

    def aiter_text(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.reads += 1
        if self.reads == 1:
            data = Chunk('{"note":"no content"}' if self.metadata_only else
                         packet({"content": {"parts": [{"text": "a" * (1 << 20)}]}}))
            self.first = weakref.ref(data)
            return data
        if self.reads == 2:
            self.started.set()
            await self.resume.wait()
            return packet({"finishReason": "STOP"})
        raise StopAsyncIteration

    async def aclose(self):
        self.closes += 1


class CookieJSONLifetimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_objects_in_one_read_do_not_overlap_during_decode(self):
        base_decode = JsonObjects._decode_object
        refs, live_before_decode = [], []

        def tracked(parser):
            live_before_decode.append(any(ref() is not None for ref in refs))
            obj = Object(base_decode(parser))
            refs.append(weakref.ref(obj))
            return obj

        async def wire():
            yield ")]}'\n" + '{"text":"one"}{"text":"two"}'

        source = cookie_response.iter_json_objects(SimpleNamespace(aiter_text=wire))
        self.addAsyncCleanup(source.aclose)
        with patch.object(JsonObjects, "_decode_object", tracked):
            self.assertEqual(await anext(source), {"text": "one"})
            self.assertEqual(await anext(source), {"text": "two"})
            with self.assertRaises(StopAsyncIteration):
                await anext(source)
        self.assertEqual(live_before_decode, [False, False])

    async def test_real_event_transport_releases_delivered_wire_and_tree_on_next_read(self):
        for metadata_only, cancel in ((False, False), (False, True), (True, False)):
            with self.subTest(metadata_only=metadata_only, cancel=cancel):
                wire, sampler = PausedWire(metadata_only=metadata_only), cookie_response.RawSampler()
                base_decode = JsonObjects._decode_object
                refs = {}

                def tracked(parser):
                    obj = Object(base_decode(parser))
                    if not refs:
                        refs["object"] = weakref.ref(obj)
                        if not metadata_only:
                            part = obj["results"][0]["data"]["candidates"][0]["content"]["parts"][0]
                            part["text"] = Chunk(part["text"])
                            refs["text"] = weakref.ref(part["text"])
                    return obj

                @asynccontextmanager
                async def stream(*args, **kwargs):
                    yield wire

                events = cookie_transport.stream_events(SimpleNamespace(stream=stream), {}, {}, sampler)
                with patch.object(JsonObjects, "_decode_object", tracked):
                    if not metadata_only:
                        kind, data = await asyncio.wait_for(anext(events), 2)
                        self.assertEqual(kind, "text")
                        self.assertEqual(data, "a" * (1 << 20))
                        del data
                    read = asyncio.create_task(anext(events))
                    try:
                        await asyncio.wait_for(wire.started.wait(), 2)
                        # The only intentional owner before a real payload is the
                        # empty-response diagnostic sampler, never the raw chunk.
                        self.assertTrue(wire.first() is None, "last raw HTTP chunk still held")
                        self.assertEqual(refs["object"]() is not None, metadata_only)
                        if not metadata_only:
                            self.assertTrue(refs["text"]() is None, "last emitted text still held")
                        if cancel:
                            read.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await read
                        else:
                            wire.resume.set()
                            self.assertEqual(await asyncio.wait_for(read, 2), ("finish", "STOP"))
                            with self.assertRaises(StopAsyncIteration):
                                await anext(events)
                            self.assertEqual(sampler.count, 2)
                            if metadata_only:
                                self.assertIn("no content", sampler.dump())
                    finally:
                        wire.resume.set()
                        await asyncio.gather(read, return_exceptions=True)
                        await events.aclose()
                self.assertEqual(wire.closes, 1)
