"""Deprecated function fields must never silently change the generation request."""
import json
from types import SimpleNamespace
from unittest import mock

from models import OpenAIRequest
from test_assistant_refusal_history import HistoryWireHarness, MODEL
from upstreams import cookie_proxy, express_sdk

TOOL = {"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}
CALL = {"id": "call-1", "type": "function", "function": {"name": "lookup", "arguments": "{}"}}


class LegacyFunctionFieldsTests(HistoryWireHarness):
    async def test_reject_unsupported_legacy_dialect_at_both_adapter_boundaries(self):
        user = {"role": "user", "content": "hello"}
        history = [user, {"role": "assistant", "content": None, "function_call": CALL["function"]},
                   {"role": "function", "name": "lookup", "content": "result"}]
        cases = [
            ([user], {"functions": [TOOL["function"]]}, "functions"),
            ([user], {"function_call": {"name": "lookup"}}, "function_call"),
            ([user], {"tools": [TOOL], "function_call": "none"}, "function_call"),
            ([user], {"tools": [TOOL], "tool_choice": "required", "function_call": "auto"}, "function_call"),
            (history, {}, "messages[1].function_call"),
        ]
        manager = SimpleNamespace(get_total_keys=mock.Mock(return_value=0))
        http_request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)))
        for adapter in (express_sdk.ExpressSDKUpstream(), cookie_proxy.CookieProxyUpstream()):
            for messages, fields, label in cases:
                with self.subTest(adapter=type(adapter).__name__, fields=fields, label=label):
                    req = OpenAIRequest(model=MODEL, messages=messages, **fields)
                    with mock.patch.object(cookie_proxy.credential_manager, "get_cookie_record",
                                           return_value=None) as select:
                        result = await adapter.chat_completions(req, http_request)
                    self.assertEqual(result.status_code, 400)
                    error = json.loads(result.body)["error"]
                    self.assertEqual(error["type"], "invalid_request_error")
                    self.assertIn(label, error["message"])
                    self.assertIn("tool", error["message"])
                    manager.get_total_keys.assert_not_called()
                    select.assert_not_called()

    async def test_noop_fields_and_modern_role_function_result_still_dispatch(self):
        history = [{"role": "assistant", "tool_calls": [CALL], "function_call": None},
                   {"role": "function", "tool_call_id": "call-1", "content": '{"k":1}'},
                   {"role": "user", "content": "more"}]
        for adapter in ("express", "cookie"):
            for fields in ({"functions": None, "function_call": None}, {"functions": [], "function_call": "auto"},
                           {"function_call": "none"}, {"tools": [TOOL], "function_call": "auto"},
                           {"tools": [TOOL], "tool_choice": "none", "function_call": "none"}):
                with self.subTest(adapter=adapter, fields=fields):
                    sent = await self.capture_history(history, adapter, **fields)
                    result = sent[1]["parts"][0]["functionResponse"]
                    self.assertEqual((result["id"], result["name"], result["response"]), ("call-1", "lookup", {"k": 1}))
