"""Native Express retries an explicit missing-signature rejection only once."""
import asyncio
import copy
import json
from unittest.mock import patch

import httpx

from native_signature_fallback import with_missing_native_signatures
from test_native_sdk_sse_keepalive import GatedBody, NativeSDKHarness, encoded
from upstreams import native_passthrough


PATH = "/v1beta/models/gemini-4-hydrogen:streamGenerateContent?alt=sse"
KEY = "thoughtSignature"
MARKER = "skip_thought_signature_validator"
HEADERS = {"content-type": "text/event-stream"}
ERROR = {"error": {"code": 400, "status": "INVALID_ARGUMENT", "message":
                  "Function call is missing a thought_signature in functionCall parts. "
                  "Additional data, function call `default_api:compress` , position 2."}}
OK = {"candidates": [{"content": {"role": "model", "parts": [{"text": "offline"}]}, "finishReason": "STOP"}]}


def user(*parts):
    return {"role": "user", "parts": list(parts)}


def model(*parts):
    return {"role": "model", "parts": list(parts)}


def call(i, **extra):
    return {"functionCall": {"name": "compress", "id": f"fixture-{i}", "args": {}}, **extra}


def reply(i):
    return {"functionResponse": {"name": "compress", "id": f"fixture-{i}", "response": {"output": "fixture"}}}


def test_export_shape_has_54_calls_and_only_51_leaders_are_eligible():
    history = [user({"text": "offline starting point"})]
    groups = []
    for i in range(51):
        groups.append(len(history))
        ids = [i, i + 100] if i in (4, 5, 28) else [i]
        history.append(model(*(call(n) for n in ids)))
        # One recorded call was abandoned. Do not invent a new result.
        next_items = [{"text": "offline handoff"}] if i in range(13) or i == 14 else []
        if i != 14:
            next_items += [reply(n) for n in ids]
        history.append(user(*next_items))
    original = {"contents": history}
    result = with_missing_native_signatures(json.dumps(original).encode())
    assert result is not None
    actual, count = result
    assert count == 51
    expected = copy.deepcopy(original)
    for i in groups:
        expected["contents"][i]["parts"][0][KEY] = MARKER
    assert json.loads(actual) == expected


def test_never_move_or_replace_real_signature_and_skip_ambiguous_groups():
    original = {"contents": [
        model({"text": "", KEY: "genuine-text"}, call(0, **{KEY: "genuine-tool"}), call(1)),
        model(call(2), call(3, **{KEY: "genuine-later"})),
        model(call(4), reply(4)),
        model({"functionCall": {}, "function_call": {}}),
        model({"function_call": {"name": "compress"}, "thought_signature": None}),
        user(call(5)),
    ]}
    result = with_missing_native_signatures(json.dumps(original).encode())
    assert result is not None
    edited, count = result
    assert count == 1
    original["contents"][4]["parts"][0]["thought_signature"] = MARKER
    assert json.loads(edited) == original
    for payload in (b'{"contents":[],"contents":[]}', b'{"contents":[],"x":1.1234567890123456789}',
                    b'{"contents":[],"x":NaN}'):
        assert with_missing_native_signatures(payload) is None


class TestNativeSignatureRetry(NativeSDKHarness):
    async def request(self, path=PATH, *, headers=None, on_start=None):
        original = json.dumps({"contents": [user({"text": "offline"}), model(call(0), call(1)),
                                           user(reply(0), reply(1))]}, indent=2).encode()
        async with httpx.AsyncClient(transport=self.transport(lambda _: None, on_start=on_start),
                                     base_url="http://local") as client:
            response = await client.post(path, content=original, headers={"x-goog-api-key": "test-key", **(headers or {})})
        return response, original

    async def test_exact_late_http_400_sse_retried_with_same_route_and_one_marker(self):
        ready = asyncio.Event()

        def provider(_, i):
            if i == 1:
                return httpx.Response(400, headers=HEADERS, stream=GatedBody(ready, encoded(ERROR, studio=False)))
            return httpx.Response(200, headers=HEADERS, stream=GatedBody(encoded(OK, studio=False)))

        async with self.provider(provider, retries=(3, 0, 0)) as calls:
            response, original = await self.request(on_start=lambda _: ready.set())
        assert response.status_code == 200
        assert b"offline" in response.content and b"missing a thought_signature" not in response.content
        assert len(calls) == 2
        assert calls[0].content == original
        assert calls[0].url == calls[1].url
        assert calls[0].headers.get("x-goog-api-key") == calls[1].headers.get("x-goog-api-key")
        modified = json.loads(original)
        modified["contents"][1]["parts"][0][KEY] = MARKER
        assert json.loads(calls[1].content) == modified

    async def test_no_other_400_or_retry_after_or_gemini2_rewrite(self):
        for status_body, headers, path in (
            ({"error": {**ERROR["error"], "message": "Invalid thought signature."}}, {}, PATH),
            (ERROR, {"Retry-After": "10"}, PATH),
            (ERROR, {}, PATH.replace("4-hydrogen", "2.5-flash")),
        ):
            with self.subTest(path=path, retry_after=bool(headers)):
                async with self.provider(
                        lambda *_: httpx.Response(400, json=status_body, headers=headers), retries=(3, 0, 0)) as calls:
                    response, original = await self.request(path)
                assert response.status_code == 400
                assert len(calls) == 1 and calls[0].content == original

    async def test_retry_failure_does_not_rotate_or_retry_again(self):
        def provider(_, i):
            return httpx.Response(400 if i == 1 else 503, json=ERROR if i == 1 else {"error": {
                "code": 503, "message": "terminal fixture"}})

        async with self.provider(provider, retries=(4, 0, 0)) as calls:
            response, _ = await self.request()
        assert response.status_code == 503
        assert len(calls) == 2

    async def test_resumable_history_has_only_the_final_provider_reply(self):
        from resumable import ResumableStore
        self.settings["resumable_enabled"] = True
        buf_store = ResumableStore()
        self.addAsyncCleanup(buf_store.aclose)
        self.enterContext(patch.object(native_passthrough, "resumable_store", buf_store))

        def provider(_, i):
            if i == 1:
                return httpx.Response(400, json=ERROR)
            return httpx.Response(200, headers=HEADERS, stream=GatedBody(encoded(OK, studio=False)))

        async with self.provider(provider) as calls:
            response, _ = await self.request(headers={"x-resumable": "true", "idempotency-key": "fallback-offline"})
        assert len(calls) == 2 and "x-resume-id" in response.headers
        assert b"offline" in response.content and b"missing a thought_signature" not in response.content
