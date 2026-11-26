"""Completed SDK HTTPX reads own physical close even when delivery is cancelled."""

import asyncio
from collections import deque
import json
from unittest.mock import patch

from google import genai
import httpx

from admission import generation_limiter
from http_options import get_http_options
from proxy_failover import ProxyChain
from request_ingress import ingress_budget
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from upstreams import express_sdk


def reply(text):
    return {"candidates": [{"content": {"role": "model", "parts": [{"text": text}]}, "finishReason": "STOP"}]}


class DeferredCloseBody(httpx.AsyncByteStream):
    def __init__(self, text, *, status=200, wait_for_read=False, fail_close=False):
        self.status = status
        self.document = reply(text) if status == 200 else {"error": {"code": status, "message": text}}
        self.reading, self.closing, self.release, self.closed = (asyncio.Event() for _ in range(4))
        self.reader = None
        self.closes = 0
        self.wait_for_read = wait_for_read
        self.fail_close = fail_close

    async def __aiter__(self):
        self.reader = asyncio.current_task()
        self.reading.set()
        if self.wait_for_read:
            await asyncio.Event().wait()
        yield json.dumps(self.document).encode()

    async def aclose(self):
        self.closes += 1
        self.closing.set()
        await self.release.wait()
        self.closed.set()
        if self.fail_close:
            raise httpx.ReadError("mock cleanup raised after closing the transport")


class WholeResponseOwnershipTests(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings.update(upstream_max_concurrency=1, retry_max=1)
        self.sources = deque()
        self.requests = []

        def upstream(request):
            self.requests.append(request)
            self.assertIn(":generateContent", request.url.path)  # not SDK true streaming
            if not self.sources:
                return httpx.Response(200, json=reply("next healthy reply"))
            source = self.sources.popleft()
            return httpx.Response(source.status, stream=source, headers={"content-type": "application/json"})

        options = get_http_options(base_url="https://offline.invalid", proxy_url="")
        options.async_client_args = dict(options.async_client_args, transport=httpx.MockTransport(upstream))
        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=options)
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        self.enterContext(patch.object(express_sdk, "get_pooled_express_client", return_value=sdk))
        # A second candidate is available, even if both routes share a pooled client.
        self.enterContext(patch.object(express_sdk, "ProxyChain", side_effect=lambda _: ProxyChain(candidates=["", ""])))

    @staticmethod
    def request(protocol, stream):
        if protocol == "responses":
            return "/v1/responses", {"model": MODEL, "input": "hello", "stream": stream}
        body = {"model": MODEL, "messages": [{"role": "user", "content": "hello"}], "stream": stream}
        if protocol == "messages":
            return "/v1/messages", {**body, "max_tokens": 32}
        return "/v1/chat/completions", body

    @staticmethod
    async def wait_for_cancel(task):
        while not task.done() and not task.cancelling():
            await asyncio.sleep(.001)

    async def exercise_drop(self, protocol, stream, spec, *, external_cancel=False):
        self.requests.clear()
        self.settings["fake_streaming"] = stream and protocol != "responses"
        path, payload = self.request(protocol, stream)
        body = DeferredCloseBody("peer never received this reply")
        self.sources.append(body)
        known_budget = tuple(ingress_budget.used)
        peer = Peer(path, payload=payload, spec=spec, headers=[(b"x-resumable", b"false")])
        sender = asyncio.create_task(peer.run())
        try:
            await asyncio.wait_for(body.closing.wait(), 2)
            self.assertEqual(generation_limiter.active, 1)
            await peer.drop()
            await asyncio.wait_for(self.wait_for_cancel(body.reader), 2)
            if external_cancel:
                sender.cancel("server shutdown")
                await asyncio.sleep(0)
                sender.cancel("server shutdown again")
            rejected = await asyncio.wait_for(self.client.post(path, json=payload), 2)
            self.assertEqual(rejected.status_code, 529 if protocol == "messages" else 503)
            self.assertEqual((len(self.requests), generation_limiter.active, body.closes), (1, 1, 1))
            self.assertFalse(sender.done() or body.closed.is_set())

            body.release.set()
            if external_cancel:
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(asyncio.shield(sender), 2)
            else:
                await asyncio.wait_for(asyncio.shield(sender), 2)
            self.assertTrue(body.closed.is_set())
            self.assertEqual((len(self.requests), generation_limiter.active, body.closes), (1, 0, 1))
            self.assertEqual((peer.reads, tuple(ingress_budget.used)), (2, known_budget))
            wire = b"".join(message.get("body", b"") for message in peer.messages)
            self.assertNotIn(b"peer never received this reply", wire)
            self.assertNotIn(b"[DONE]", wire)
            self.assertNotIn(b"event: message_stop", wire)
            self.assertNotIn(b"event: response.completed", wire)

            ready = await asyncio.wait_for(self.client.post(path, json=payload), 2)
            self.assertEqual((ready.status_code, len(self.requests)), (200, 2))
            if protocol == "messages" and stream:
                events = [json.loads(line[6:]) for line in ready.text.splitlines() if line.startswith("data: ")]
                text = "".join(event["delta"]["text"] for event in events if event["type"] == "content_block_delta")
                self.assertEqual((text, events[-1]["type"]), ("next healthy reply", "message_stop"))
            else:
                self.assertIn("next healthy reply", ready.text)
        finally:
            body.release.set()
            if not sender.done():
                sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    async def test_drop_at_sdk_eof_holds_admission_through_real_close_for_all_whole_delivery_paths(self):
        for protocol in ("chat", "messages", "responses"):
            for stream in (False, True):
                for spec in ("2.3", "2.4"):
                    with self.subTest(protocol=protocol, stream=stream, spec=spec):
                        await self.exercise_drop(protocol, stream, spec)

    async def test_independent_server_cancel_still_propagates_after_peer_drop_during_close(self):
        for protocol in ("chat", "messages", "responses"):
            for stream in (False, True):
                for spec in ("2.3", "2.4"):
                    with self.subTest(protocol=protocol, stream=stream, spec=spec):
                        await self.exercise_drop(protocol, stream, spec, external_cancel=True)

    async def test_dropping_one_whole_response_does_not_touch_another_on_same_pooled_sdk(self):
        self.settings["upstream_max_concurrency"] = 2
        first, second = DeferredCloseBody("discarded"), DeferredCloseBody("independent reply")
        self.sources.extend((first, second))
        path, payload = self.request("chat", False)
        peer = Peer(path, payload=payload, headers=[(b"x-resumable", b"false")])
        left = asyncio.create_task(peer.run())
        right = None
        try:
            await asyncio.wait_for(first.closing.wait(), 2)
            right = asyncio.create_task(self.client.post(path, json=payload))
            await asyncio.wait_for(second.closing.wait(), 2)
            await peer.drop()
            await asyncio.wait_for(self.wait_for_cancel(first.reader), 2)
            self.assertEqual((generation_limiter.active, len(self.requests)), (2, 2))
            self.assertFalse(second.reader.cancelling())
            second.release.set()
            result = await asyncio.wait_for(asyncio.shield(right), 2)
            self.assertEqual(result.status_code, 200)
            self.assertIn("independent reply", result.text)
            self.assertFalse(left.done() or first.closed.is_set())
            self.assertEqual((generation_limiter.active, second.closes), (1, 1))
            first.release.set()
            await asyncio.wait_for(asyncio.shield(left), 2)
            self.assertTrue(first.closed.is_set() and second.closed.is_set())
            self.assertEqual((generation_limiter.active, first.closes, len(self.requests)), (0, 1, 2))
        finally:
            for source in (first, second):
                source.release.set()
            tasks = [task for task in (left, right) if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_cleanup_error_does_not_translate_server_cancellation_into_proxy_retry(self):
        body = DeferredCloseBody("unused", wait_for_read=True, fail_close=True)
        self.sources.append(body)
        path, payload = self.request("chat", False)
        sender = asyncio.create_task(self.client.post(path, json=payload))
        try:
            await asyncio.wait_for(body.reading.wait(), 2)
            sender.cancel("shutdown while reading")
            await asyncio.wait_for(body.closing.wait(), 2)
            self.assertFalse(sender.done())
            self.assertEqual(generation_limiter.active, 1)
            body.release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(sender), 2)
            self.assertTrue(body.closed.is_set())
            self.assertEqual((len(self.requests), body.closes, generation_limiter.active), (1, 1, 0))
        finally:
            body.release.set()
            await asyncio.gather(sender, return_exceptions=True)

    async def test_regular_unadvised_retry_still_runs_after_first_transport_actually_closes(self):
        body = DeferredCloseBody("outlet not available", status=503)
        self.sources.append(body)
        path, payload = self.request("chat", False)
        sender = asyncio.create_task(self.client.post(path, json=payload))
        try:
            await asyncio.wait_for(body.closing.wait(), 2)
            self.assertEqual((len(self.requests), generation_limiter.active), (1, 1))
            body.release.set()
            result = await asyncio.wait_for(asyncio.shield(sender), 2)
            self.assertEqual(result.status_code, 200)
            self.assertIn("next healthy reply", result.text)
            self.assertTrue(body.closed.is_set())
            self.assertEqual((len(self.requests), body.closes, generation_limiter.active), (2, 1, 0))
        finally:
            body.release.set()
            await asyncio.gather(sender, return_exceptions=True)
