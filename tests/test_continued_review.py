"""Real request state, cross-channel recovery and typed prompt estimation."""

import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-continued-review-")
os.environ["STATE_DIR"] = _STATE.name
os.environ["API_KEY"] = "review-test-key"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.testclient import TestClient

import main
from models import OpenAIRequest
from resumable import DetachedRequest
from routes import chat_api
from runtime_state import app_state
from usage import messages_prompt_text


class RequestStateFallbackTests(unittest.TestCase):
    def setUp(self):
        # 测试隔离：本类的模式与设置改动必须在收尾时还原。旧版把
        # web_proxy 模式和 cross_channel_fallback 残留在全局单例里，
        # 排在其后的 gemini 原生透传测试会被迫走 Cookie 桥接而失败。
        self._saved_mode = app_state.get_channel_mode()
        self._saved_settings = app_state.get_settings()
        app_state.update_settings({"cross_channel_fallback": True, "resumable_enabled": False})
        app_state.set_channel_mode("web_proxy")
        self.manager = SimpleNamespace(get_total_keys=lambda: 1)
        patch = mock.patch.object(main.app.state, "express_key_manager", self.manager)
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(self._restore_state)
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)

    def _restore_state(self):
        app_state.set_channel_mode(self._saved_mode)
        changed = {key: self._saved_settings[key]
                   for key in ("cross_channel_fallback", "resumable_enabled")}
        app_state.update_settings(changed)

    def test_real_and_detached_requests_use_application_manager(self):
        request = Request({"type": "http", "app": main.app, "state": {"trace_id": "trace"}})
        self.assertIs(chat_api._express_manager(request), self.manager)
        self.assertIs(chat_api._express_manager(DetachedRequest(request)), self.manager)

    def test_default_keeps_both_single_modes_isolated_even_with_both_credentials_saved(self):
        # Remove the fixture's explicit opt-in: this must test the real default.
        with app_state._lock:
            app_state._state["settings"].pop("cross_channel_fallback", None)
        saved_keys_before, saved_cookies_before = app_state.get_express_keys(), app_state.get_cookie_accounts()
        self.addCleanup(lambda: (app_state.set_express_keys(saved_keys_before),
                                 app_state.set_cookie_accounts(saved_cookies_before)))
        app_state.set_express_keys([{"key": "saved-key", "enabled": True}])
        app_state.set_cookie_accounts([{"cookie": "SAPISID=saved-cookie", "project_id": "p", "enabled": True}])
        saved_keys, saved_cookies = app_state.get_express_keys(), app_state.get_cookie_accounts()

        async def failed_frames():
            yield 'data: {"error":{"code":503,"message":"primary unavailable"}}\n\n'
            yield 'data: [DONE]\n\n'

        for mode in ("api_key", "web_proxy"):
            for stream in (False, True):
                with self.subTest(mode=mode, stream=stream):
                    app_state.set_channel_mode(mode)
                    failure = (StreamingResponse(failed_frames(), media_type="text/event-stream") if stream else
                               JSONResponse(status_code=503, content={"error": {"code": 503, "message": "primary unavailable"}}))
                    primary = mock.AsyncMock(return_value=failure)
                    alternate = mock.AsyncMock(return_value=JSONResponse({"content": "unexpected fallback"}))
                    with (mock.patch.object(chat_api.express_upstream, "chat_completions", primary if mode == "api_key" else alternate),
                          mock.patch.object(chat_api.cookie_upstream, "chat_completions", primary if mode == "web_proxy" else alternate)):
                        response = self.client.post("/v1/chat/completions", headers={"Authorization": "Bearer review-test-key"},
                                                    json={"model": "gemini-3.6-flash", "stream": stream,
                                                          "messages": [{"role": "user", "content": "hello"}]})
                    self.assertEqual(response.status_code, 200 if stream else 503)
                    self.assertIn("primary unavailable", response.text)
                    self.assertNotIn("unexpected fallback", response.text)
                    primary.assert_awaited_once()
                    alternate.assert_not_awaited()
        self.assertEqual(app_state.get_express_keys(), saved_keys)
        self.assertEqual(app_state.get_cookie_accounts(), saved_cookies)

    def test_cookie_failure_recovers_through_express_over_http(self):
        failure = JSONResponse(status_code=503, content={"error": {"code": 503, "message": "busy"}})
        success = JSONResponse({"choices": [{"message": {"content": "recovered"}}]})
        with (mock.patch.object(chat_api.cookie_upstream, "chat_completions", mock.AsyncMock(return_value=failure)),
              mock.patch.object(chat_api.express_upstream, "chat_completions", mock.AsyncMock(return_value=success)) as express):
            response = self.client.post("/v1/chat/completions", headers={"Authorization": "Bearer review-test-key"},
                                   json={"model": "gemini-3.6-flash", "messages": [{"role": "user", "content": "hello"}]})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["choices"][0]["message"]["content"], "recovered")
        express.assert_awaited_once()

    def test_streaming_cookie_failure_recovers_before_any_content(self):
        async def failed():
            yield 'data: {"error":{"code":503,"message":"busy"}}\n\n'
            yield 'data: [DONE]\n\n'

        async def recovered():
            yield 'data: {"choices":[{"delta":{"content":"recovered"}}]}\n\n'
            yield 'data: [DONE]\n\n'

        with (mock.patch.object(chat_api.cookie_upstream, "chat_completions", mock.AsyncMock(
                return_value=StreamingResponse(failed(), media_type="text/event-stream"))),
              mock.patch.object(chat_api.express_upstream, "chat_completions", mock.AsyncMock(
                return_value=StreamingResponse(recovered(), media_type="text/event-stream"))) as express):
            response = self.client.post("/v1/chat/completions", headers={"Authorization": "Bearer review-test-key"},
                                   json={"model": "gemini-3.6-flash", "stream": True,
                                         "messages": [{"role": "user", "content": "hello"}]})
        self.assertEqual(response.status_code, 200)
        self.assertIn("recovered", response.text)
        self.assertNotIn('"error"', response.text)
        self.assertEqual(response.text.count("[DONE]"), 1)
        express.assert_awaited_once()


class PromptEstimationTests(unittest.TestCase):
    def test_typed_and_dictionary_messages_estimate_the_same_prompt(self):
        messages = [
            {"role": "user", "content": [{"type": "text", "text": "你好"},
             {"type": "image_url", "image_url": {"url": "data:image/png;base64,hidden"}},
             {"type": "text", "text": "世界"}]},
            {"role": "assistant", "tool_calls": [{"id": "c", "type": "function", "function": {
                "name": "lookup", "arguments": json.dumps({"city": "Shanghai"})}}]},
        ]
        request = OpenAIRequest(model="gemini-3.6-flash", messages=messages)
        expected = '你好\n世界\n{"city": "Shanghai"}'
        self.assertEqual(messages_prompt_text(request.messages), expected)
        self.assertEqual(messages_prompt_text(messages), expected)
        self.assertNotIn("hidden", messages_prompt_text(request.messages))


if __name__ == "__main__":
    unittest.main()
