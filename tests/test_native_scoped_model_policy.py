"""Native resource prefixes select destinations, not model capabilities."""
import json
import unittest
from unittest.mock import patch

import httpx

from admission import image_limiter
from credential_manager import credential_manager
from main import app
from resumable import ResumableStore
from routes import gemini_native as route
from upstreams import native_passthrough as native

TEXT = "gemini-3.6-flash"
IMAGE = "gemini-3.1-flash-image"
SCOPE = "projects/image-studio/locations/global/publishers/google/models/"
OTHER = "projects/other/locations/global/publishers/google/models/"
BODY = b'{ "contents" : [{"parts":[{"text":"hi"}]}] }'
EVENT = b'{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}'


class ScopedModelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.calls = []

        def upstream(req):
            self.calls.append((req.url.path, req.content))
            if req.url.path.endswith(":streamGenerateContent"):
                return httpx.Response(200, content=b"data: " + EVENT + b"\n\n", headers={"content-type": "text/event-stream"})
            return httpx.Response(200, content=EVENT, headers={"content-type": "application/json"})

        client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.MockTransport(upstream)))
        self.client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.ASGITransport(app),
            base_url="http://local", headers={"x-goog-api-key": "test-key", "content-type": "application/json"}))
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(native, "resumable_store", store))
        self.enterContext(patch.object(route, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.enterContext(patch.object(app.state.express_key_manager, "get_express_record", return_value={"key": "local-only"}))
        self.enterContext(patch.object(credential_manager, "get_cookie_record", side_effect=AssertionError("unexpected Cookie selection")))
        self.enterContext(patch.object(native.passthrough_client_pool, "get", return_value=client))

    async def post(self, model, *, stream=False, headers=None):
        action = "streamGenerateContent?alt=sse" if stream else "generateContent"
        return await self.client.post(f"/v1beta/models/{model}:{action}", content=BODY, headers=headers)

    async def test_text_resumes_but_fingerprints_stay_scoped(self):
        scope = SCOPE + TEXT
        first = await self.post(scope, stream=True, headers={"x-resumable": "true"})
        assert first.status_code == 200 and "x-resume-id" in first.headers
        rid = {"x-resume-id": first.headers["x-resume-id"]}
        same = await self.post(scope, stream=True, headers=rid)
        assert same.status_code == 200 and same.headers["x-resumable-replay"] == "1"
        other = await self.post(OTHER + TEXT, stream=True, headers=rid)
        assert other.status_code == 409
        assert self.calls == [(f"/v1beta1/{scope}:streamGenerateContent", BODY)]

    async def test_only_actual_image_takes_the_image_slot(self):
        lease = image_limiter.reserve()
        try:
            text = await self.post(SCOPE + TEXT)
            image = await self.post(OTHER + IMAGE)
        finally:
            lease.release()
        assert text.status_code == 200 and image.status_code == 503
        assert self.calls == [(f"/v1beta1/{SCOPE}{TEXT}:generateContent", BODY)]
        direct = await self.post(OTHER + IMAGE, stream=True, headers={"x-resumable": "true"})
        assert direct.status_code == 200 and "x-resume-id" not in direct.headers

    async def test_official_image_schema_request_is_not_rewritten_by_translation_policy(self):
        body = b'{ "contents": [{"parts":[{"text":"hi"}]}], "generationConfig":{"responseMimeType":"application/json"} }'
        reply = await self.client.post(f"/v1beta/models/{OTHER}{IMAGE}:generateContent",
                                       content=body, headers={"x-resumable": "false"})
        assert reply.status_code == 200 and reply.content == EVENT
        assert self.calls == [(f"/v1beta1/{OTHER}{IMAGE}:generateContent", body)]

    async def test_first_reply_override_uses_model_id_not_project_path(self):
        with patch.object(native.first_reply_module, "timeout_seconds", return_value=5) as timeout:
            response = await self.post(SCOPE + TEXT, stream=True)
        assert response.status_code == 200 and EVENT in response.content
        timeout.assert_called_once_with(TEXT)

    async def test_only_the_actual_leaf_can_enable_a_synthetic_variant(self):
        search = await self.post(SCOPE + TEXT + "-search")
        assert search.status_code == 200
        path, content = self.calls.pop()
        assert path == f"/v1beta1/{SCOPE}{TEXT}:generateContent"
        assert json.loads(content)["tools"] == [{"google_search": {}}]
        text = await self.post(SCOPE + TEXT + "-4k")
        assert text.status_code == 200
        assert self.calls.pop() == (f"/v1beta1/{SCOPE}{TEXT}-4k:generateContent", BODY)
        image = await self.post(OTHER + IMAGE + "-4k")
        assert image.status_code == 200
        path, content = self.calls.pop()
        assert path == f"/v1beta1/{OTHER}{IMAGE}:generateContent"
        assert json.loads(content)["generationConfig"]["imageConfig"]["imageSize"] == "4K"
