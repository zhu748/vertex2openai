"""Media admission is chosen by actual prompt shape, before entering either worker pool."""

import asyncio
import base64
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest import mock

import httpx
from google.genai import types

import media_tasks
import message_processing as mp
from models import OpenAIMessage, OpenAIRequest
from routes import claude_api
from test_req_round15_contracts import Round15Harness, content, response
from upstreams import cookie_delivery, cookie_proxy

MODEL = "gemini-2.5-flash"
URL = "https://images.example.test/fixture"
IMAGE = {"type": "image_url", "image_url": {"url": URL}}
MD = "![x](data:image/png;base64,AAAA)"
CALLS = [{"id": "call", "type": "function", "function": {"name": "weather", "arguments": "{}"}}]
PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jvV8AAAAASUVORK5CYII=")


class PromptShapeTests(unittest.TestCase):
    def test_normalized_blocks_markdown_tool_and_replay_shapes(self):
        for role, value, extra in (
            ("user", [IMAGE], {}),
            ("user", [dict(type="text", text=MD)], {}),
            ("assistant", [dict(type="text", text="!"), dict(type="text", text=MD[1:])], {"tool_calls": CALLS}),
            ("tool", [{"type": "image", "source": {"type": "url", "url": URL}}], {}),
            ("tool", json.dumps({"error": [IMAGE]}), {"extra_content": {"anthropic": {"tool_result_is_error": True}}}),
            ("assistant", None, {"extra_content": {"google": {"ordinary_parts": [{"kind": "image", "text": ""}]}}}),
            ("assistant", None, {"extra_content": {"google": {"ordinary_parts": [{"kind": "thought", "text": "[image]"}]}}}),
            ("assistant", "ok", {"reasoning_content": MD}),
            ("assistant", [dict(type="refusal", refusal=MD)], {}),
        ):
            with self.subTest(role=role, value=value):
                self.assertTrue(media_tasks.prompt_needs_media_pool([OpenAIMessage(role=role, content=value, **extra)]))
        for role, value, extra in (
            ("user", [{"type": "text", "text": "Hello"}], {}),
            ("user", "a normal string", {}),
            ("assistant", "Calling weather", {"tool_calls": CALLS}),
            ("tool", json.dumps({"result": [IMAGE]}), {}),  # ordinary JSON, not an image block
            ("assistant", "hello", {"extra_content": {"google": {"ordinary_parts": [{"kind": "text", "text": "hello"}]}}}),
        ):
            with self.subTest(role=role, value=value):
                self.assertFalse(media_tasks.prompt_needs_media_pool([OpenAIMessage(role=role, content=value, **extra)]))

    def test_unknown_or_expensive_shapes_do_not_consume_the_light_worker(self):
        def user(content):
            return OpenAIMessage(role="user", content=content)
        with mock.patch.object(media_tasks, "_PROMPT_SHAPE_ITEMS", 3), \
                mock.patch.object(media_tasks, "_PROMPT_SHAPE_CHARS", 8):
            for messages in ([user("12345"), user("6789")], [user(None)] * 4,
                             [user([{"type": "text", "text": ""}] * 3)],
                             [user([{"type": "unknown"}])]):
                self.assertTrue(media_tasks.prompt_needs_media_pool(messages))
        tool = [OpenAIMessage(role="tool", content="small")]
        self.assertFalse(media_tasks.prompt_needs_media_pool(tool))
        self.assertTrue(media_tasks.prompt_needs_media_pool(tool, plain_only=True))


class MediaRouteIsolationTests(Round15Harness):
    async def run_client(self, route, *, image=False):
        parts = [IMAGE] if image else [{"type": "text", "text": "hello"}]
        if route == "count":
            blocks = [{"type": "image", "source": {"type": "url", "url": URL}}] if image else parts
            body = {"model": MODEL, "messages": [{"role": "user", "content": blocks}]}
            request = SimpleNamespace(json=mock.AsyncMock(return_value=body), headers={}, query_params={},
                                      app=self.http_request.app)
            result = await claude_api.claude_count_tokens(request, "test-key")
            self.assertEqual(json.loads(result.body), {"input_tokens": 123})  # exact, not the fallback
        elif route == "express":
            req = OpenAIRequest(model=MODEL, messages=[{"role": "user", "content": parts}], stream=True)
            async def stream(**kwargs):
                async def iterate():
                    yield response(types.Part(text="reply"))
                return iterate()
            result = await self.express(req, stream=stream)
            self.assertEqual(content(await self.body(result)), "reply")
        else:
            req = OpenAIRequest(model=MODEL, messages=[{"role": "user", "content": parts}])
            result = await cookie_proxy.CookieProxyUpstream().chat_completions(
                req, self.http_request, credential_record={"id": "offline", "cookie": "SAPISID=offline", "project_id": "offline"})
            self.assertEqual(json.loads(result.body)["choices"][0]["message"]["content"], "reply")

    async def test_gated_image_preparation_does_not_delay_text_in_any_adapter(self):
        @asynccontextmanager
        async def client(*_args, **_kwargs):
            yield SimpleNamespace(post=mock.AsyncMock(return_value=httpx.Response(200, json={"results": [{"data": {
                "candidates": [{"content": {"role": "model", "parts": [{"text": "reply"}]}, "finishReason": "STOP"}]}}]})))

        self.settings["fake_streaming"] = False
        sdk = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
            count_tokens=mock.AsyncMock(return_value=SimpleNamespace(total_tokens=123)))))
        wrapper = SimpleNamespace(prepare=mock.AsyncMock(return_value=SimpleNamespace(client=sdk)))
        with ThreadPoolExecutor(1, "route-prep") as light, ThreadPoolExecutor(1, "route-media") as heavy, \
                mock.patch.object(media_tasks, "_prep_executor", light), \
                mock.patch.object(media_tasks, "_media_executor", heavy), \
                mock.patch.object(claude_api, "FailoverSDKClient", return_value=wrapper), \
                mock.patch.object(claude_api, "get_express_models", mock.AsyncMock(return_value=[MODEL])), \
                mock.patch.object(cookie_delivery, "open_upstream_client", client):
            for route in ("express", "count", "cookie"):
                started, release = threading.Event(), threading.Event()
                workers = []
                def fetch(url):
                    workers.append(threading.current_thread().name)
                    started.set()
                    if not release.wait(5):
                        raise AssertionError("image was not released")
                    return PNG, "image/png"

                with self.subTest(route=route), mock.patch.object(mp, "fetch_remote_image", side_effect=fetch):
                    image_job = asyncio.create_task(self.run_client(route, image=True))
                    try:
                        self.assertTrue(await asyncio.to_thread(started.wait, 2))
                        self.assertTrue(workers[0].startswith("route-media"), workers)
                        async with asyncio.timeout(2):
                            for text_route in ("express", "count", "cookie"):
                                await self.run_client(text_route)
                        self.assertFalse(image_job.done())
                    finally:
                        release.set()
                        await image_job
