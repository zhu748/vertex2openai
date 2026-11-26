"""Reject unrepresentable user data instead of replacing it with an empty prompt."""
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from models import OpenAIRequest
from request_validation import validate_openai_request
from upstreams import cookie_proxy, express_sdk


class UnsupportedMediaTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_adapters_reject_audio_file_only_and_mixed_before_credential_selection(self):
        manager = SimpleNamespace(get_total_keys=mock.Mock(return_value=0))
        http_request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)))
        for adapter in (express_sdk.ExpressSDKUpstream(), cookie_proxy.CookieProxyUpstream()):
            for part in ({"type": "input_audio", "input_audio": {"data": "QUJD", "format": "wav"}},
                         {"type": "file", "file": {"file_id": "demo"}}):
                for prefix in ([], [{"type": "text", "text": "analyze this"}]):
                    with self.subTest(adapter=type(adapter).__name__, kind=part["type"], mixed=bool(prefix)):
                        req = OpenAIRequest(model="gemini-test", messages=[
                            {"role": "user", "content": [*prefix, part]}])
                        with mock.patch.object(cookie_proxy.credential_manager, "get_cookie_record",
                                               return_value=None) as select:
                            result = await adapter.chat_completions(req, http_request)
                        self.assertEqual(result.status_code, 400)
                        error = json.loads(result.body)["error"]
                        self.assertEqual(error["type"], "invalid_request_error")
                        self.assertIn(part["type"], error["message"])
                        self.assertIn(f"messages[0].content[{len(prefix)}]", error["message"])
                        select.assert_not_called()
                        manager.get_total_keys.assert_not_called()

    def test_tool_results_and_unrelated_client_extensions_stay_opaque(self):
        part = {"type": "file", "file": {"name": "application-defined field"}}
        for role in ("tool", "function"):
            req = OpenAIRequest(model="gemini-test", messages=[
                {"role": role, "name": "find_file", "content": [part]}])
            validate_openai_request(req)
            self.assertEqual(req.messages[0].content, [part])
        validate_openai_request(OpenAIRequest(model="gemini-test", messages=[{"role": "user", "content": [
            {"type": "text", "text": "ok", "future_metadata": 1}, {"type": "vendor_marker"}]}]))
