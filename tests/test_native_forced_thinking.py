"""Opt-in native thinking override does not change the rest of the request."""
import json
from unittest.mock import patch

import httpx

from native_thinking import forced_native_generation, rewrite_native_generation
from request_tracking import request_registry
from test_native_sdk_sse_keepalive import GatedBody, NativeSDKHarness, encoded, mime
from upstreams import native_passthrough


MODEL = "gemini-4-hydrogen"
PATH = f"/v1beta/models/{MODEL}:generateContent"
STREAM = f"/v1beta/models/{MODEL}:streamGenerateContent?alt=sse"
CONFIG = {"native_thinking_mode": "console", "thinking_g3_level": "xhigh"}
REPLY = {"candidates": [{"content": {"role": "model", "parts": [{"text": "offline"}]}, "finishReason": "STOP"}]}
HISTORY = [{"role": "user", "parts": [{"text": "offline"}]}]


def test_force_replaces_only_the_root_generation_member():
    # Numeric spelling, whitespace, and opaque provider bytes outside that
    # member must remain intact even when conventional JSON dumps would differ.
    prefix = b'''{
   "contents": [{"parts":[{"functionCall":{"name":"fixture","args":{"ratio":0.0125000000000000009}},"thoughtSignature":"A+/="}]}],
   "generation_config":'''
    suffix = b''',"tools":[{"fixture":"} \\"generationConfig\\": {}"}]}'''
    raw = (prefix + b' {"thinking_config":{"thinking_budget":80,"includeThoughts":false}, "max_output_tokens":33}'
           + suffix)
    parsed = json.loads(raw)
    key, gen = forced_native_generation(MODEL, CONFIG, parsed)
    out = rewrite_native_generation(raw, key, gen, has_members=True)
    assert out.startswith(prefix) and out.endswith(suffix)
    assert json.loads(out)[key] == {"thinking_config": {"thinkingLevel": "XHIGH", "includeThoughts": False},
                                   "max_output_tokens": 33}


def test_insert_for_missing_config_and_ignore_non_level_policy():
    raw = b' \n{ "contents": [] } \n'
    result = rewrite_native_generation(raw, "generationConfig", {"thinkingConfig": {"thinkingLevel": "XHIGH"}},
                                       has_members=True)
    assert result.endswith(b' "contents": [] } \n')
    assert json.loads(result)["generationConfig"]["thinkingConfig"]["thinkingLevel"] == "XHIGH"
    assert forced_native_generation("gemini-2.5-flash", CONFIG, {}) is None
    assert forced_native_generation(MODEL, {**CONFIG, "native_thinking_mode": "request"}, {}) is None


class TestNativeForce(NativeSDKHarness):
    async def request(self, body, path=PATH, **headers):
        if not isinstance(body, bytes):
            body = json.dumps(body, ensure_ascii=False, indent=2).encode()
        async with httpx.AsyncClient(transport=self.transport(lambda _: None), base_url="http://local") as client:
            response = await client.post(path, content=body, headers={"x-goog-api-key": "test-key", **headers})
        return response, body

    def provider_response(self, studio, streaming):
        if studio:
            return httpx.Response(200, content=encoded(REPLY, studio=True), headers=mime(True))
        if streaming:
            return httpx.Response(200, headers=mime(False), stream=GatedBody(encoded(REPLY, studio=False)))
        return httpx.Response(200, json=REPLY)

    def fields(self, response):
        return request_registry.get(response.headers["x-trace-id"]).snapshot()

    async def test_consistent_force_on_native_express_and_cookie_json_and_sse(self):
        self.settings.update(CONFIG)
        for studio in (False, True):
            self.channel = "web_proxy" if studio else "api_key"
            for streaming in (False, True):
                for client_level in (None, "LOW", "XHIGH"):
                    with self.subTest(studio=studio, streaming=streaming, client_level=client_level):
                        gen = {"maxOutputTokens": 123}
                        if client_level:
                            gen["thinkingConfig"] = {"thinkingLevel": client_level, "includeThoughts": False}
                        original = {"contents": HISTORY, "generationConfig": gen}
                        async with self.provider(lambda *_: self.provider_response(studio, streaming)) as calls:
                            response, exact = await self.request(original, STREAM if streaming else PATH)
                        assert response.status_code == 200, response.text
                        assert len(calls) == 1
                        body = json.loads(calls[0].content)
                        sent = (body["variables"] if studio else body)["generationConfig"]
                        expected = {"thinkingLevel": "XHIGH"}
                        if client_level:
                            expected["includeThoughts"] = False
                        assert sent["thinkingConfig"] == expected
                        assert sent["maxOutputTokens"] == 123
                        if not studio and client_level == "XHIGH":
                            assert calls[0].content == exact
                        fields = self.fields(response)
                        assert (fields["thinking_level"], fields["thinking_source"], fields["max_output_tokens"]) == (
                            "XHIGH", "console", 123)
                        assert fields["upstream_model"].endswith(MODEL)

    async def test_followed_native_bytes_and_declared_client_vs_unspecified(self):
        self.settings.update(CONFIG, native_thinking_mode="request")
        for cfg, source in (({}, "unspecified"), ({"thinkingConfig": {"thinkingLevel": "LOW"}}, "client")):
            async with self.provider(lambda *_: self.provider_response(False, False)) as calls:
                response, raw = await self.request({"contents": HISTORY, "generationConfig": cfg})
            assert calls[0].content == raw
            fields = self.fields(response)
            assert fields["thinking_level"] == ("LOW" if cfg else None)
            assert fields["thinking_source"] == source

    async def test_existing_replay_does_not_change_after_force_setting_changes(self):
        from resumable import ResumableStore
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(native_passthrough, "resumable_store", store))
        self.settings.update(CONFIG, resumable_enabled=True)
        params = {"x-resumable": "true", "idempotency-key": "native-force-replay"}
        async with self.provider(lambda *_: self.provider_response(False, True)) as calls:
            first, raw = await self.request({"contents": HISTORY}, STREAM, **params)
            assert first.status_code == 200 and first.headers.get("x-resume-id")
            self.settings["thinking_g3_level"] = "low"
            again, _ = await self.request(raw, STREAM, **params)
            assert again.headers["x-resumable-replay"] == "1"
            assert len(calls) == 1
            assert json.loads(calls[0].content)["generationConfig"]["thinkingConfig"]["thinkingLevel"] == "XHIGH"
