"""Availability checks must not consume a turn in the Cookie scheduler."""
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.responses import JSONResponse

import channel_dispatch as dispatch
from credential_manager import COOKIE_AUTH_FAILURE_THRESHOLD, credential_manager
from main import app
from routes import gemini_native
from runtime_state import app_state
from upstreams import cookie_native, cookie_proxy


def account(label, *, enabled=True, project=True):
    return {"id": label, "cookie": f"SAPISID=fixture-{label}",
            "project_id": "local" if project else "", "enabled": enabled}


class CookieDispatchRotationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.rows = []
        self.enterContext(patch.object(app_state, "get_cookie_accounts", return_value=self.rows))
        self.enterContext(patch.dict(credential_manager._indices, clear=True))
        self.enterContext(patch.dict(credential_manager._cookie_health, clear=True))
        original = app_state.get_setting
        self.enterContext(patch.object(app_state, "get_setting", side_effect=lambda k, default=None:
            True if k in ("roundrobin", "cross_channel_fallback") else original(k, default)))
        self.client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.ASGITransport(app),
            base_url="http://local", headers={"authorization": "Bearer test-key", "x-resumable": "false"}))

    async def test_native_route_uses_each_eligible_account_in_order(self):
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=0))
        self.enterContext(patch.object(app_state, "get_channel_mode", return_value="web_proxy"))
        seen = []

        async def local_events(request, record, body, model, deadline):
            seen.append(record["id"])
            yield "text", "ok"
            yield "finish", "STOP"

        self.enterContext(patch.object(cookie_native, "_timed_events", local_events))
        for size in (2, 3, 4):
            with self.subTest(size=size), patch.dict(credential_manager._indices, clear=True):
                ids = list("ABCD"[:size])
                self.rows[:] = [account("disabled", enabled=False), account("missing", project=False),
                                account("cool"), *map(account, ids)]
                for _ in range(COOKIE_AUTH_FAILURE_THRESHOLD):
                    credential_manager.record_cookie_auth_failure("cool")
                seen.clear()
                for _ in ids * 2:
                    res = await self.client.post("/v1beta/models/gemini-2.5-flash:generateContent",
                        json={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]})
                    self.assertEqual(res.status_code, 200, res.text)
                self.assertEqual(seen, ids * 2)

    async def test_express_and_rejected_native_requests_leave_next_cookie_turn_untouched(self):
        self.rows[:] = [account("A"), account("B")]
        self.enterContext(patch.object(gemini_native, "_passthrough_request",
                                      new=AsyncMock(return_value=JSONResponse({}))))
        for keys, mode, action, status in ((1, "mixed", "generateContent", 200),
                                          (1, "web_proxy", "countTokens", 200),
                                          (0, "web_proxy", "countTokens", 400)):
            with self.subTest(keys=keys, mode=mode), (
                patch.object(app.state.express_key_manager, "get_total_keys", return_value=keys)
            ), patch.object(app_state, "get_channel_mode", return_value=mode):
                res = await self.client.post(f"/v1beta/models/gemini-2.5-flash:{action}", json={})
                self.assertEqual(res.status_code, status, res.text)
                self.assertNotIn("cookie", credential_manager._indices)
        self.assertEqual(credential_manager.get_cookie_record()["id"], "A")

    async def test_fallback_does_not_spend_cookie_turns_before_dispatch(self):
        self.rows[:] = [account("A"), account("B")]
        self.enterContext(patch.object(app_state, "get_channel_mode", return_value="api_key"))
        self.enterContext(patch.object(app_state, "is_web_proxy_enabled", return_value=False))
        seen = []

        async def local_result(prepared):
            seen.append(prepared.account_id)
            return JSONResponse({"choices": [{"message": {"role": "assistant", "content": "ok"}}]})

        self.enterContext(patch.object(cookie_proxy.CookieGeneration, "response", local_result))
        self.enterContext(patch.object(dispatch.express_upstream, "chat_completions",
            new=AsyncMock(return_value=JSONResponse({"error": {"message": "offline"}}, status_code=503))))
        for _ in range(4):
            res = await self.client.post("/v1/chat/completions", json={"model": "gemini-2.5-flash",
                "messages": [{"role": "user", "content": "hello"}]})
            self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(seen, ["A", "B", "A", "B"])

    def test_presence_matches_selector_even_when_all_rows_are_incomplete_or_cooling(self):
        for rows, expected in (([], False), ([account("A", enabled=False)], False),
                               ([account("A", project=False)], True), ([account("A")], True)):
            with self.subTest(rows=rows):
                self.rows[:] = rows
                for _ in range(COOKIE_AUTH_FAILURE_THRESHOLD):
                    credential_manager.record_cookie_auth_failure("A")
                # Even in the all-cooling/missing-project cases the existing selector
                # returns the enabled row; it supplies the normal downstream diagnostic.
                with patch.object(credential_manager, "_select", side_effect=AssertionError("reserved a turn")):
                    self.assertIs(credential_manager.has_cookie_record(), expected)
                self.assertEqual(credential_manager.get_cookie_record() is not None, expected)
