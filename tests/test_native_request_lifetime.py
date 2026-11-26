"""Native wire passthrough releases decoded input; the Cookie bridge still owns it."""
import asyncio
import json
import unittest
import weakref
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi.responses import JSONResponse

import config
from credential_manager import credential_manager
from main import app
from resumable import ResumableStore
from routes import gemini_native
from test_gemini_native_stats_retry import COMPLETE_SSE, KeyManager
from upstreams import cookie_native, native_passthrough


class TaggedRoot(dict):
    pass


class NativeRequestLifetimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.refs = []
        self.wire = []
        self.raw = (b'{ "contents" : [{"role":"user","parts":[{"inlineData":{"mimeType":"image/png","data":"'
                    + b"eHh4" * 4096 + b'"}}]}] }')

        def decode(raw):
            data = json.loads(raw)
            if isinstance(data, dict) and "contents" in data:
                data = TaggedRoot(data)
                self.refs.append(weakref.ref(data))
            return data

        async def respond(req):
            await asyncio.sleep(0)  # Outgoing HTTP is in flight, its producer no longer needs the tree.
            self.wire.append((req, self.refs[-1]() is not None))
            if req.url.params.get("alt") == "sse":
                return httpx.Response(200, content=COMPLETE_SSE, headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json={"candidates": []})

        transport = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.MockTransport(respond)))
        self.client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
            base_url="http://test", headers={"x-goog-api-key": config.API_KEY}))
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        self.enterContext(patch.object(gemini_native, "json", SimpleNamespace(loads=decode)))
        self.enterContext(patch.object(app.state, "express_key_manager", KeyManager()))
        self.enterContext(patch.object(gemini_native.app_state, "get_channel_mode", return_value="api_key"))
        self.enterContext(patch.object(gemini_native, "resumable_enabled", return_value=True))
        self.enterContext(patch.object(native_passthrough, "resumable_store", store))
        self.enterContext(patch.object(native_passthrough, "passthrough_client_pool",
                                      SimpleNamespace(get=lambda *_, **__: transport)))

    async def test_plain_and_variant_wire_paths_release_decoded_tree_before_io(self):
        for model, base, extra in (
            ("gemini-2.5-flash", "gemini-2.5-flash", {}),
            ("gemini-2.5-flash-search", "gemini-2.5-flash", {"tools": [{"google_search": {}}]}),
            ("gemini-3.1-flash-image-4k", "gemini-3.1-flash-image",
             {"generationConfig": {"imageConfig": {"imageSize": "4K"}}}),
        ):
            with self.subTest(model=model):
                res = await self.client.post(f"/v1beta/models/{model}:generateContent?key=unused&extra=a%26b",
                                             content=self.raw)
                self.assertEqual(res.status_code, 200)
                sent, alive = self.wire[-1]
                self.assertFalse(alive)
                self.assertTrue(sent.url.path.endswith(f"/{base}:generateContent"))
                self.assertEqual(list(sent.url.params.multi_items()), [("extra", "a&b")])
                if extra:
                    self.assertEqual(json.loads(sent.content), {**json.loads(self.raw), **extra})
                else:
                    self.assertEqual(sent.content, self.raw)  # including whitespace

    async def test_resume_fingerprint_uses_wire_bytes_and_normalized_query(self):
        path = "/v1beta/models/gemini-2.5-flash:streamGenerateContent"
        res = await self.client.post(path + "?alt=sse&extra=a%26b", content=self.raw, headers={"x-resumable": "true"})
        self.assertEqual(res.status_code, 200)
        self.assertIn(COMPLETE_SSE, res.content)
        sent, alive = self.wire[0]
        self.assertFalse(alive)
        self.assertEqual(sent.content, self.raw)
        rid = {"x-resume-id": res.headers["x-resume-id"]}
        replay = await self.client.post(path + "?extra=a%26b&alt=sse&resume_from=0", content=self.raw, headers=rid)
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(replay.headers["x-resumable-replay"], "1")
        changed = await self.client.post(path + "?alt=sse&extra=a%26b", content=b" " + self.raw, headers=rid)
        self.assertEqual(changed.status_code, 409)
        self.assertEqual(len(self.wire), 1)

    async def test_cookie_bridge_receives_live_decoded_rewritten_input(self):
        async def bridge(request, model, action, parsed, **kwargs):
            self.assertIs(self.refs[-1](), parsed)
            self.assertEqual(parsed, {**json.loads(self.raw), "tools": [{"google_search": {}}]})
            return JSONResponse({"ok": True})

        with (patch.object(gemini_native.app_state, "get_channel_mode", return_value="web_proxy"),
              patch.object(credential_manager, "has_cookie_record", return_value=True),
              patch.object(cookie_native, "handle_native_request", bridge)):
            res = await self.client.post("/v1beta/models/gemini-2.5-flash-search:generateContent", content=self.raw)
        self.assertEqual(res.json(), {"ok": True})
        self.assertFalse(self.wire)
