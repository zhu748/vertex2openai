"""Cookie tool calls: complete-call parsing, failover and first-reply deadlines.

The private batchGraphql endpoint never emits partialArgs fragments (verified
2026-10-30), so the Cookie channel parses complete function calls directly.
Fragment assembly remains a Key/Express capability tested elsewhere.
"""
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-cookie-tool-stream-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

import config
from logger import ProxyStats
from models import OpenAIRequest
from proxy_failover import ProxyChain, is_proxy_failure
from response_validation import IncompleteResponseError, ToolStreamError
from runtime_state import app_state
from upstreams import cookie_native, cookie_proxy, cookie_request, cookie_response, cookie_transport

MODEL = "gemini-4-argon"
RECORD = {"id": "offline", "cookie": "SAPISID=offline", "project_id": "offline"}
TOOL = {"type": "function", "function": {"name": "reply", "parameters": {
    "type": "object", "properties": {"text": {"type": "string"}}}}}


def wire(*parts, finish=None, usage=False):
    candidate = {"content": {"role": "model", "parts": list(parts)}}
    if finish:
        candidate["finishReason"] = finish
    data = {"candidates": [candidate]}
    if usage:
        data["usageMetadata"] = {"promptTokenCount": 1}
    return {"results": [{"data": data}]}


def fc(**fields):
    return {"functionCall": fields}


def private_fc(**fields):
    # GraphQL projection metadata and unused oneof defaults are not SDK Parts.
    return {**fc(__typename="FunctionCall", unusedPrivateField=None, **fields), "__typename": "Part", "text": None, "thought": False,
            "thoughtSignature": "", "inlineData": {}, "functionResponse": {},
            "videoMetadata": {"startOffset": {"seconds": "0", "nanos": 0}}}


def calls_in(body):
    calls = {}
    for line in body.splitlines():
        if not line.startswith("data: {"):
            continue
        for choice in json.loads(line[6:]).get("choices", []):
            for delta in choice.get("delta", {}).get("tool_calls", []):
                call = calls.setdefault(delta["index"], {"function": {"arguments": ""}})
                call.update({key: value for key, value in delta.items() if key != "function"})
                function = delta.get("function", {})
                if function.get("name"):
                    call["function"]["name"] = function["name"]
                call["function"]["arguments"] += function.get("arguments", "")
    complete = []
    for call in calls.values():
        try:
            json.loads(call["function"]["arguments"])
        except ValueError:
            continue
        complete.append(call)
    return complete


class DirectWireParsingTests(unittest.TestCase):
    def test_complete_private_graphql_call_yields_function_call_event(self):
        obj = wire({**private_fc(name="reply", id="private-call", args={"text": "hello"}),
                    "thoughtSignature": "c2ln"})
        calls = [data for kind, data in cookie_response.extract_events(obj) if kind == "function_call"]
        self.assertEqual(len(calls), 1)
        self.assertEqual((calls[0]["name"], calls[0]["args"], calls[0]["id"]),
                         ("reply", {"text": "hello"}, "private-call"))

    def test_fragment_wire_fails_explicitly_as_incomplete(self):
        for fields in ({"willContinue": True}, {"partialArgs": [{"jsonPath": "$.text", "stringValue": "x"}]}):
            with self.subTest(fields=fields), self.assertRaises(IncompleteResponseError):
                list(cookie_response.extract_events(wire(fc(name="reply", **fields))))

    def test_parser_502_is_not_a_proxy_failure(self):
        chain = ProxyChain(candidates=["http://a.test:80", "http://b.test:80"])
        error = ToolStreamError("上游工具参数流：函数调用分片字段无效。")
        self.assertFalse(is_proxy_failure(error))
        self.assertFalse(chain.advance(error))
        self.assertEqual(chain.index, 0)

    def test_signature_only_native_parts_never_count_as_first_reply(self):
        self.assertFalse(cookie_transport._event_has_reply(("native_part", {"thoughtSignature": "c2ln"})))
        self.assertFalse(cookie_transport._event_has_reply(("usage", {"totalTokenCount": 10})))


class CookieStreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = dict(config.DEFAULT_SETTINGS, retry_max=0, retry_backoff_seconds=0,
                             retry_jitter_seconds=0, fake_streaming=False, proxy_url="")
        self.stats = ProxyStats()
        self.request = SimpleNamespace(headers={}, query_params={"alt": "sse"},
                                       is_disconnected=mock.AsyncMock(return_value=False))
        self.captured = []
        self.closed = []
        for patch in (
            mock.patch.object(app_state, "get_effective_settings", side_effect=lambda *a, **k: dict(self.settings)),
            mock.patch.object(app_state, "get_setting", side_effect=lambda k, d=None: self.settings.get(k, d)),
            mock.patch.object(cookie_transport, "timeout_seconds", return_value=.04),
            mock.patch.object(cookie_proxy, "stats", self.stats),
            mock.patch.object(cookie_native, "stats", self.stats),
            mock.patch.object(cookie_proxy, "build_headers", return_value={"authorization": "offline"}),
            mock.patch.object(cookie_native, "build_headers", return_value={"authorization": "offline"}),
            mock.patch.object(cookie_proxy.credential_manager, "record_cookie_success"),
            mock.patch.object(cookie_native.credential_manager, "get_cookie_record", return_value=RECORD),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def pool(self, handler):
        async def respond(request):
            self.captured.append(json.loads(request.content))
            return await handler(len(self.captured))
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.addAsyncCleanup(client.aclose)
        @asynccontextmanager
        async def opened(*args, **kwargs):
            yield client
        for module in (cookie_proxy, cookie_native):
            patch = mock.patch.object(module, "open_upstream_client", opened)
            patch.start()
            self.addCleanup(patch.stop)
        return client

    def response(self, events, *, delay=0, silent=False):
        closed = asyncio.Event()
        self.closed.append(closed)
        class Bytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                for i, event in enumerate(events):
                    if delay and i > 0:
                        await asyncio.sleep(delay)
                    yield json.dumps(event).encode() + b"\n"
                if silent:
                    await asyncio.Event().wait()
            async def aclose(self):
                closed.set()
        return httpx.Response(200, headers={"content-type": "application/json"}, stream=Bytes())

    async def openai(self, *, model=MODEL):
        obj = OpenAIRequest(model=model, stream=True, tool_choice="required", tools=[TOOL],
                            messages=[{"role": "user", "content": "hello"}])
        before = obj.model_dump_json()
        result = await cookie_proxy.CookieProxyUpstream().chat_completions(obj, self.request, credential_record=RECORD)
        body = "".join([frame async for frame in result.body_iterator])
        self.assertEqual(obj.model_dump_json(), before)
        return body

    async def test_parse_failure_with_eleven_proxies_and_retries_stops_after_one_request(self):
        self.settings["retry_max"] = 12
        async def respond(attempt):
            return self.response([wire(fc(name=123, willContinue=True))])
        client = self.pool(respond)
        client.chain = ProxyChain(candidates=[f"http://proxy{i}.test:80" for i in range(11)])
        body = await self.openai(model="gemini-3.5-flash-lite")
        self.assertEqual(len(self.captured), 1)
        self.assertEqual(client.chain.index, 0)
        self.assertEqual(self.stats.retry_counts, 0)
        self.assertIn("functionCall.name", body)
        self.assertEqual(calls_in(body), [])
        self.assertTrue(self.closed[0].is_set())

    async def test_real_gateway_and_network_failures_still_switch_proxy(self):
        for failure in (None, httpx.ReadTimeout("network timeout"), httpx.RemoteProtocolError("truncated transport")):
            with self.subTest(failure=type(failure).__name__):
                self.captured.clear()
                async def respond(attempt):
                    if attempt == 1:
                        if failure is not None:
                            raise failure
                        return httpx.Response(502, text="Bad gateway")
                    return self.response([wire(fc(name="reply", args={"text": "recovered"}), finish="STOP")])
                client = self.pool(respond)
                client.chain = ProxyChain(candidates=["http://a.test:80", "http://b.test:80"])
                body = await self.openai()
                self.assertEqual(len(self.captured), 2)
                self.assertEqual(client.chain.index, 1)
                self.assertNotIn('"error"', body)
                self.assertEqual(json.loads(calls_in(body)[0]["function"]["arguments"]), {"text": "recovered"})

    async def test_silent_queue_still_times_out_and_closes_before_retry(self):
        self.settings["retry_max"] = 1
        async def respond(attempt):
            if attempt == 1:
                return self.response([wire(usage=True)], silent=True)
            self.assertTrue(self.closed[0].is_set())
            return self.response([wire(fc(name="reply", args={"text": "retry"}), finish="STOP")])
        self.pool(respond)
        body = await self.openai()
        self.assertEqual(len(self.captured), 2)
        self.assertEqual(self.stats.retry_counts, 1)
        self.assertEqual(json.loads(calls_in(body)[0]["function"]["arguments"]), {"text": "retry"})

    async def test_incomplete_tool_cannot_be_reported_as_success(self):
        async def respond(attempt):
            return self.response([wire(fc(name="reply", willContinue=True)), wire(finish="STOP")])
        self.pool(respond)
        body = await self.openai()
        self.assertEqual(calls_in(body), [])
        self.assertIn('"code": 502', body)

    async def test_native_bridge_accepts_complete_private_graphql_call(self):
        async def respond(attempt):
            return self.response([wire({**private_fc(name="reply", id="private-call", args={"text": "hello"}),
                                        "thoughtSignature": "c2ln"}, finish="STOP")])
        self.pool(respond)
        parsed = {"contents": [{"role": "user", "parts": [{"text": "hello"}]}],
                  "tools": [{"functionDeclarations": [{"name": "reply", "parameters": {"type": "OBJECT"}}]}],
                  "toolConfig": {"functionCallingConfig": {"mode": "ANY", "allowedFunctionNames": ["reply"]}}}
        result = await cookie_native.handle_native_request(self.request, "gemini-3.5-flash-lite", "streamGenerateContent", parsed)
        body = "".join([frame async for frame in result.body_iterator])
        self.assertNotIn('"error"', body)
        calls = [part for line in body.splitlines() if line.startswith("data: {")
                 for candidate in json.loads(line[6:]).get("candidates", [])
                 for part in candidate.get("content", {}).get("parts", []) if part.get("functionCall")]
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["functionCall"], {"name": "reply", "args": {"text": "hello"}, "id": "private-call"})
        self.assertEqual(calls[0]["thoughtSignature"], "c2ln")
        self.assertEqual(len(self.captured), 1)
        self.assertNotIn("streamFunctionCallArguments", json.dumps(self.captured[0]["variables"]))

    async def test_ordinary_signature_only_parts_do_not_release_native_deadline(self):
        async def respond(attempt):
            return self.response([wire({"thoughtSignature": "c2ln", "functionCall": {}})], silent=True)
        client = self.pool(respond)
        body = cookie_request.build_request_body("offline", MODEL, OpenAIRequest(model=MODEL,
            stream=True, tools=[TOOL], messages=[]))
        events = [event async for event in cookie_transport.stream_events(client, {}, body, model=MODEL, preserve_parts=True)]
        self.assertEqual(events[-1][0], "retryable_error")
        self.assertEqual(events[-1][1].status_code, 504)

    async def test_nonstream_fake_search_none_and_unsupported_models_keep_existing_wire(self):
        for model, stream, fake, tools, choice in (
            (MODEL, False, False, [TOOL], "required"), (MODEL, True, True, [TOOL], "required"),
            (MODEL, True, False, [TOOL], "none"), (MODEL, True, False, [{"type": "web_search"}], "auto"),
            ("gemini-2.5-flash", True, False, [TOOL], "required"),
            ("gemini-3.1-flash-lite", True, False, [TOOL], "required")):
            with self.subTest(model=model, stream=stream, fake=fake, choice=choice):
                self.settings["fake_streaming"] = fake
                obj = OpenAIRequest(model=model, stream=stream, tools=tools, tool_choice=choice, messages=[])
                body = cookie_request.build_request_body("offline", model, obj)
                self.assertNotIn("streamFunctionCallArguments", str(body))


if __name__ == "__main__":
    unittest.main()
