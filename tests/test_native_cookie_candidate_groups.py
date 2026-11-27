"""Preserve the Cookie provider's candidate boundaries on the public Gemini API."""

import asyncio
import json
from unittest.mock import patch

import httpx
from google.genai import types
from test_native_sdk_sse_keepalive import (
    TAIL,
    GatedBody,
    NativeSDKHarness,
    encoded,
    mime,
)
from upstreams import cookie_native, cookie_transport

MODEL = "gemini-4-neon"
PATH = f"/v1beta/models/{MODEL}:streamGenerateContent"
USER = {"role": "user", "parts": [{"text": "Read both fixture colors."}]}
LEADER = {"functionCall": {"id": "one", "name": "read_color_one", "args": {}}, "thoughtSignature": "AQI="}
FOLLOWER = {"functionCall": {"id": "two", "name": "read_color_two", "args": {}}}
EMPTY = {"text": "", "thoughtSignature": "BQY="}
PARTS = [{"text": "Checking.", "thoughtSignature": "AwQ="}, LEADER, FOLLOWER, EMPTY]
RESULTS = {"role": "user", "parts": [
    {"functionResponse": {"name": "read_color_one", "response": {"result": "blue"}}},
    {"functionResponse": {"name": "read_color_two", "response": {"result": "red"}}},
]}
MISSING_SIGNATURE = {"error": {"code": 400, "status": "INVALID_ARGUMENT", "message": "Missing fixture signature"}}


def candidate(parts, **metadata):
    return {"candidates": [{"index": 0, "content": model(parts), **metadata}]}


def model(parts):
    return {"role": "model", "parts": parts}


def response(records, *, studio=True):
    return httpx.Response(200, content=b"".join(encoded(record, studio=studio) for record in records), headers=mime(studio))


def frames(reply, alt):
    assert reply.status_code == 200
    if alt == "json":
        return reply.json()
    return [json.loads(line[6:]) for line in reply.text.split("\n") if line.startswith("data: ")]


class NativeCookieCandidateGroups(NativeSDKHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.channel = "web_proxy"

    async def test_actual_sdk_automatic_parallel_tool_followup_preserves_provider_content(self):
        # Each AFC replay includes the exact Content objects yielded by the
        # pinned SDK. One signed leader + unsigned follower is one model step.
        for channel, stream, parts in (("web_proxy", True, PARTS),
                                       ("web_proxy", True, PARTS[1:]),
                                       ("api_key", True, PARTS),
                                       ("web_proxy", False, PARTS)):
            with self.subTest(channel=channel, stream=stream, parts=len(parts)):
                await self._assert_sdk_parallel_followup(channel, stream, parts)

    async def _assert_sdk_parallel_followup(self, channel, stream, parts):
        self.channel = channel
        studio = channel == "web_proxy"
        observed, called, sent = [], [], []

        def read_color_one() -> str:
            """Return the first fixture color."""
            called.append("one")
            return "blue"

        def read_color_two() -> str:
            """Return the second fixture color."""
            called.append("two")
            return "red"

        head = candidate(parts) if stream else candidate(parts, finishReason="STOP")
        result = candidate([{"text": "Blue and red."}], finishReason="STOP")

        def provider(request, number):
            body = json.loads(request.content)
            observed.append(body.get("variables", body)["contents"])
            if number == 1:
                return response([head, TAIL] if stream else [head], studio=studio)
            # A bridge that splits the source group into Parts causes
            # the second request to fail this provider boundary check.
            if observed[-1] != [USER, model(parts), RESULTS]:
                return httpx.Response(400, json=MISSING_SIGNATURE)
            return response([result], studio=studio)

        sdk = self.api(sent.append)
        chat = sdk.aio.chats.create(model=MODEL, config=types.GenerateContentConfig(
            tools=[read_color_one, read_color_two]))
        async with self.provider(provider) as requests, asyncio.timeout(4):
            if stream:
                output = [chunk async for chunk in await chat.send_message_stream(USER["parts"][0]["text"])]
            else:
                output = [await chat.send_message(USER["parts"][0]["text"])]
        self.assertEqual(len(requests), 2)
        self.assertEqual(called, ["one", "two"])
        self.assertEqual(observed, [[USER], [USER, model(parts), RESULTS]])
        self.assertTrue(any(chunk.text == "Blue and red." for chunk in output))
        self.assertEqual(output[-1].candidates[0].finish_reason, types.FinishReason.STOP)
        if stream:
            first = output[0].candidates[0].content.parts
            self.assertEqual([part.thought_signature for part in first],
                             ([b"\x03\x04"] if parts == PARTS else []) + [b"\x01\x02", None, b"\x05\x06"])
        if not studio:
            self.assertEqual(b"".join(sent), b"\n\n" + encoded(head, studio=False) + encoded(TAIL, studio=False)
                             + b"\n\n" + encoded(result, studio=False))

    async def test_sse_and_array_keep_adjacent_results_in_one_provider_object_separate(self):
        second = [{"functionCall": {"id": "three", "name": "read_color_three", "args": {}}, "thoughtSignature": "+/8="},
                  {"functionCall": {"id": "four", "name": "read_color_four", "args": {}}}]
        third = [{"text": "next object"}, {"thought": True, "text": "observation"}]
        fourth = [{"text": "single call in next result"}, LEADER, EMPTY]
        # The first two results arrive in the same parsed upstream object and
        # the same read. Neither fact authorizes merging their contents.
        envelope = json.dumps({"results": [{"data": candidate(parts)} for parts in (PARTS, second, third)]}).encode()
        upstream = envelope + b"\n" + encoded(candidate(fourth), studio=True) + encoded(TAIL, studio=True)
        for alt in ("sse", "json"):
            with self.subTest(alt=alt):
                async with self.provider(lambda *_: httpx.Response(200, content=upstream, headers=mime(True))) as requests:
                    payloads = frames(await self.client.post(f"{PATH}?alt={alt}", json={"contents": [USER]}), alt)
                self.assertEqual(len(requests), 1)
                self.assertEqual([frame["candidates"][0]["content"] for frame in payloads[:-1]],
                                 [model(PARTS), model(second), *(model([part]) for part in third + fourth)])
                self.assertEqual(payloads[-1]["candidates"], TAIL["candidates"])
                self.assertEqual(payloads[-1]["usageMetadata"], TAIL["usageMetadata"])

    async def test_separate_sequential_call_after_function_results_is_not_inferred_or_repaired(self):
        # A user Content containing only functionResponse Parts does not start
        # a fresh real user turn. A later sequential step has its own leader.
        # This fixture deliberately lacks that leader's signature.
        unrelated = [{"functionCall": {"id": "next", "name": "lookup", "args": {}}}]
        unrelated_result = {"role": "user", "parts": [
            {"functionResponse": {"name": "lookup", "response": {"result": "ok"}}},
        ]}
        observed = []

        def provider(request, number):
            observed.append(json.loads(request.content)["variables"]["contents"])
            if number == 1:
                return response([candidate(PARTS), TAIL])
            if number == 2:
                return response([candidate(unrelated), TAIL])
            return httpx.Response(400, json=MISSING_SIGNATURE)

        history = [USER]
        async with self.provider(provider, retries=(2, 0, 0)) as requests:
            for tool_result in (RESULTS, unrelated_result):
                payloads = frames(await self.client.post(f"{PATH}?alt=sse", json={"contents": history}), "sse")
                self.assertEqual(len(payloads), 2)
                history += [payloads[0]["candidates"][0]["content"], tool_result]
            payloads = frames(await self.client.post(f"{PATH}?alt=sse", json={"contents": history}), "sse")
        self.assertEqual(len(requests), 3)
        self.assertEqual(observed[-1], [USER, model(PARTS), RESULTS, model(unrelated), unrelated_result])
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["error"]["code"], 400)
        self.assertIn("Missing fixture signature", payloads[0]["error"]["message"])

    async def test_signed_only_record_waits_for_reply_and_keeps_legacy_cadence(self):
        parts = [EMPTY, {"text": "", "thought": True, "thoughtSignature": "+/8="}]
        head = candidate(parts)
        for finish in (False, True):
            with self.subTest(finish=finish):
                def provider(*_, with_terminal=finish):
                    if with_terminal:
                        return response([head, TAIL])
                    return httpx.Response(200, headers=mime(True), stream=GatedBody(
                        encoded(head, studio=True), asyncio.Event()))

                with patch.object(cookie_transport, "timeout_seconds", return_value=.1):
                    async with self.provider(provider) as requests, asyncio.timeout(3):
                        payloads = frames(await self.client.post(f"{PATH}?alt=sse", json={"contents": [USER]}), "sse")
                self.assertEqual(len(requests), 1)
                if finish:
                    self.assertEqual(len(payloads), 3)
                    self.assertEqual([frame["candidates"][0]["content"] for frame in payloads[:-1]],
                                     [model([part]) for part in parts])
                    self.assertEqual(payloads[-1]["candidates"], TAIL["candidates"])
                else:
                    self.assertEqual(len(payloads), 1)
                    self.assertEqual(payloads[0]["error"]["code"], 504)

    async def test_budget_rejects_whole_candidate_without_leaking_its_first_part(self):
        first = [{"text": "prior"}]
        second = [{"text": "汉\n" * 20}, LEADER, FOLLOWER]
        # Individually the first Part of `second` still fits, but the whole
        # candidate does not. Include actual UTF-8 and escaped bytes.
        size = sum(len(json.dumps(part, ensure_ascii=False).encode()) for part in first + second)
        for alt in ("sse", "json"):
            with self.subTest(alt=alt), patch.object(cookie_native, "MAX_RESPONSE_BYTES", size - 1):
                async with self.provider(lambda *_: response([candidate(first), candidate(second), TAIL])):
                    payloads = frames(await self.client.post(f"{PATH}?alt={alt}", json={"contents": [USER]}), alt)
                self.assertEqual(len(payloads), 2)
                self.assertEqual(payloads[0]["candidates"][0]["content"], model(first))
                self.assertEqual(payloads[1]["error"]["code"], 502)
                self.assertIn("大小", payloads[1]["error"]["message"])

    async def test_group_size_selects_bounded_transport_fragments_not_new_candidates(self):
        parts = [{"text": "汉\n" * 130_000}, LEADER, FOLLOWER, {"text": "二\n" * 130_000}, EMPTY]
        for alt in ("sse", "json"):
            with self.subTest(alt=alt):
                sent = []
                async with (httpx.AsyncClient(transport=self.transport(sent.append), base_url="http://local",
                                              headers={"Authorization": "Bearer test-key"}) as client,
                            self.provider(lambda *_: response([candidate(parts), TAIL]))):
                    payloads = frames(await client.post(f"{PATH}?alt={alt}", json={"contents": [USER]}), alt)
                self.assertEqual(len(payloads), 2)
                self.assertEqual(payloads[0]["candidates"][0]["content"], model(parts))
                self.assertLess(max(map(len, sent)), 256 * 1024)

    async def test_missing_terminal_after_group_reports_one_error_without_replay(self):
        for alt in ("sse", "json"):
            with self.subTest(alt=alt):
                async with self.provider(lambda *_: response([candidate(PARTS)]), retries=(2, 0, 0)) as requests:
                    payloads = frames(await self.client.post(f"{PATH}?alt={alt}", json={"contents": [USER]}), alt)
                self.assertEqual(len(requests), 1)
                self.assertEqual(len(payloads), 2)
                self.assertEqual(payloads[0]["candidates"][0]["content"], model(PARTS))
                self.assertEqual(payloads[1]["error"]["code"], 502)
