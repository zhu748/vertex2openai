"""A reply commits the route, not permanent ownership of its metadata prefix."""
import asyncio
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
import weakref

from google.genai import types
from proxy_failover import FailoverSDKClient, ProxyChain


class Source:
    def __init__(self):
        self.refs = []
        self.pulls = self.closes = 0
        self.closing, self.release = asyncio.Event(), asyncio.Event()

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.pulls += 1
        if self.pulls == 3:
            reply = types.GenerateContentResponse(candidates=[types.Candidate(
                content=types.Content(parts=[types.Part(text="ready")]))])
        elif self.pulls <= 4:
            reply = types.GenerateContentResponse(model_version=str(self.pulls))
        else:
            raise StopAsyncIteration
        self.refs.append(weakref.ref(reply))
        return reply

    async def aclose(self):
        self.closes += 1
        self.closing.set()
        await self.release.wait()


class PrefixLifetime(IsolatedAsyncioTestCase):
    async def test_each_transfer_and_early_close_drop_owned_metadata(self):
        for candidates in (("",), ("", "http://127.0.0.1:1")):
            for take in (1, 4):
                with self.subTest(routes=len(candidates), take=take):
                    source = Source()

                    async def start(**kw):
                        return source

                    models = SimpleNamespace(generate_content_stream=start)
                    client = await FailoverSDKClient(ProxyChain(candidates=candidates), lambda *args:
                                                     SimpleNamespace(aio=SimpleNamespace(models=models))).prepare()
                    stream = await client.generate_content_stream()
                    for i in range(take):
                        item = await anext(stream)
                        self.assertIs(item, source.refs[i]())
                        del item
                        if i < 3:
                            self.assertIsNone(source.refs[i]())
                    self.assertEqual(source.pulls, max(3, take))
                    close = asyncio.create_task(stream.aclose())
                    await source.closing.wait()
                    try:
                        self.assertTrue(all(ref() is None for ref in source.refs))
                        self.assertEqual(source.closes, 1)
                    finally:
                        source.release.set()
                        await close
