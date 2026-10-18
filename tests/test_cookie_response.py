"""batchGraphql 分块、转义、截断和响应体上限回归。"""

import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
from contextlib import asynccontextmanager

_STATE = tempfile.TemporaryDirectory(prefix="v2o-cookie-response-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from models import OpenAIRequest
from logger import ProxyStats
from response_validation import IncompleteResponseError
from runtime_state import app_state
from upstreams import cookie_proxy, cookie_response


class Chunks:
    def __init__(self, chunks):
        self.chunks = chunks

    async def aiter_text(self):
        for chunk in self.chunks:
            yield chunk


async def decode(chunks, **kwargs):
    return [obj async for obj in cookie_response.iter_json_objects(Chunks(chunks), **kwargs)]


class JsonFramingTests(unittest.IsolatedAsyncioTestCase):
    async def test_every_split_preserves_nested_braces_quotes_and_escapes(self):
        objects = [
            {"text": '中文 😀 {} [ ] " \\ \\"', "nested": [{"text": "tail\\"}]},
            {"results": [], "value": None},
        ]
        payload = ")]}'\n" + json.dumps(objects, ensure_ascii=False)
        for split in range(len(payload) + 1):
            with self.subTest(split=split):
                self.assertEqual(await decode([payload[:split], "", payload[split:]]), objects)
        self.assertEqual(await decode(list(payload)), objects)

    async def test_many_objects_and_large_string_survive_small_network_chunks(self):
        objects = [{"data": "A" * (1024 * 1024)}, *[{"text": str(i)} for i in range(50)]]
        payload = json.dumps(objects)
        chunks = (payload[i : i + 1024] for i in range(0, len(payload), 1024))
        self.assertEqual(await decode(chunks), objects)

    async def test_truncated_or_malformed_objects_raise_502(self):
        for payload in ('{"text":"unfinished', '{"data": [1, 2}', '{} {"next":', '{"bad": nope}'):
            with self.subTest(payload=payload), self.assertRaises(IncompleteResponseError) as caught:
                await decode([payload])
            self.assertEqual(caught.exception.code, 502)
            with self.assertRaises(IncompleteResponseError):
                _ = [
                    obj
                    async for obj in cookie_response.iter_json_objects(cookie_response.TextSource(payload))
                ]

    async def test_limit_applies_to_complete_objects_and_utf8_bytes(self):
        payload = json.dumps({"text": "中文"}, ensure_ascii=False)
        size = len(payload.encode("utf-8"))
        self.assertEqual(await decode([payload], max_object_bytes=size), [{"text": "中文"}])
        for chunks in ([payload], list(payload)):
            with self.subTest(chunks=len(chunks)), self.assertRaises(IncompleteResponseError):
                await decode(chunks, max_object_bytes=size - 1)
        with self.assertRaises(IncompleteResponseError):
            _ = [
                obj
                async for obj in cookie_response.iter_json_objects(
                    cookie_response.TextSource(payload), max_object_bytes=size - 1
                )
            ]

    async def test_trailing_truncation_cannot_turn_a_finished_reply_into_success(self):
        complete = {
            "results": [
                {
                    "data": {
                        "candidates": [{"content": {"parts": [{"text": "answer"}]}, "finishReason": "STOP"}]
                    }
                }
            ]
        }
        body = json.dumps(complete) + '{"results":'
        response = SimpleNamespace(status_code=200, text=body, headers={})

        class Client:
            async def post(self, *args, **kwargs):
                return response

        @asynccontextmanager
        async def open_client(*args, **kwargs):
            yield Client()

        stats = ProxyStats()
        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))
        obj = OpenAIRequest(model="gemini-3.6-flash", messages=[])
        with (
            mock.patch.object(cookie_proxy, "open_upstream_client", open_client),
            mock.patch.object(cookie_proxy, "stats", stats),
            mock.patch.object(cookie_proxy, "_get_cookie_string", return_value="SAPISID=test"),
            mock.patch.object(app_state, "get_setting", side_effect=lambda key, default=None: default),
        ):
            result = await cookie_proxy._collect_full_response("p", obj.model, obj, {}, {}, 0, 0, 0, request)
        self.assertEqual(result["kind"], "error")
        self.assertEqual(result["code"], 502)
        self.assertEqual(stats.success_requests, 0)


if __name__ == "__main__":
    unittest.main()
