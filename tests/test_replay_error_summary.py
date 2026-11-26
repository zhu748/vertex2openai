"""Replay diagnostics are bounded metadata, not copies of the authoritative payload."""
import json
import unittest
from unittest.mock import patch

from fastapi.responses import JSONResponse, StreamingResponse
import httpx
from main import app
import resumable
from routes import chat_api, gemini_native
from upstreams import native_passthrough


class ReplaySummaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = resumable.ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        for module in (chat_api, native_passthrough):
            self.enterContext(patch.object(module, "resumable_store", self.store))
        self.enterContext(patch.object(chat_api, "resumable_enabled", return_value=True))
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                        headers={"Authorization": "Bearer test-key"})
        self.addAsyncCleanup(self.client.aclose)

    async def test_json_error_does_not_duplicate_large_detail_in_metadata(self):
        message = "offline fixture " + "detail " * 1200
        async def fail(*_):
            raise RuntimeError(message)
        with patch.object(chat_api, "_call_upstream", fail):
            result = await self.client.post("/v1/chat/completions", json={"model": "gemini-test", "messages": []})
        self.assertEqual(result.status_code, 500)
        body = result.json()
        self.assertEqual(body["error"]["message"], message[:1024])
        self.assertEqual(body["error"]["type"], "upstream_error")
        rid = result.headers["x-resume-id"]
        buf = await self.store.get_exact(rid)
        poll = (await self.client.get("/v1/resumable/" + rid)).json()
        self.assertEqual(poll["payload"], body)
        self.assertEqual([poll["error"], (await self.store.list_meta())[0]["error"]], [buf.error] * 2)
        self.assertLessEqual(len(buf.error), resumable.MAX_ERROR_MESSAGE_CHARS)
        self.assertIn("offline fixture", buf.error)
        self.assertEqual(buf.bytes_stored, self.store._bytes_used)
        self.assertLess(buf.bytes_stored, 2048)

        success = {"choices": [{"message": {"content": message}}]}
        with patch.object(chat_api, "_call_upstream", return_value=JSONResponse(success)):
            good = await self.client.post("/v1/chat/completions", json={"model": "gemini-test", "messages": []})
        self.assertEqual(good.json(), success)
        self.assertIsNone((await self.store.get_exact(good.headers["x-resume-id"])).error)

    async def test_native_error_keeps_wire_details_but_summary_only_uses_redacted_message(self):
        message = "offline Bearer fixture-marker note"
        error = {"code": 503, "message": message, "details": [{"note": "diagnostic " * 1000}]}
        frame = "data: " + json.dumps({"error": error}) + "\n\n"
        async def frames():
            yield frame
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.enterContext(patch.object(gemini_native, "resumable_enabled", return_value=True))
        with patch.object(native_passthrough, "_passthrough_request",
                          return_value=StreamingResponse(frames(), media_type="text/event-stream")):
            result = await self.client.post("/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
                                            json={"contents": []}, headers={"X-Resumable": "true"})
        self.assertEqual(result.status_code, 200)
        self.assertIn(frame, result.text)
        rid = result.headers["x-resume-id"]
        poll = (await self.client.get("/v1/resumable/" + rid)).json()
        self.assertIn(frame, poll["events"][0]["data"])
        self.assertEqual(poll["error"], "offline Bearer *** note")
        buf = await self.store.get_exact(rid)
        self.assertEqual((await self.store.list_meta())[0]["error"], buf.error)
        self.assertEqual(buf.status_code, 503)
        self.assertEqual(buf.bytes_stored, len(poll["events"][0]["data"].encode()))
