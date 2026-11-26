"""Collected Studio HTTP auth failures feed the same health gate as true SSE."""
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from credential_manager import COOKIE_AUTH_FAILURE_THRESHOLD, COOKIE_COOLDOWN_SECONDS, credential_manager
from main import app
from runtime_state import app_state
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from upstreams import cookie_delivery


class CookieCollectAuthHealthUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_terminal_http_auth_status_reports_the_selected_identity(self):
        record = {"id": "fixture", "cookie": "SAPISID=offline", "project_id": "fixture"}
        gen = SimpleNamespace(
            project_id=record["project_id"], account_id=record["id"], record=record,
            base_model_name=MODEL, start_time=0, request_obj=None, force_builtin_search=False,
            prefill_active=False, fastapi_request=SimpleNamespace(is_disconnected=AsyncMock(return_value=False)),
            retry_max=0, backoff_sec=0, jitter_sec=0,
        )
        with (
            patch.object(cookie_delivery, "_attempt_context", return_value=({}, None, {})),
            patch.object(cookie_delivery, "_collect_with_tool_fallback") as collect,
            patch.object(credential_manager, "record_cookie_auth_failure") as report,
        ):
            for status in (400, 401, 403, 429, 503):
                with self.subTest(status=status):
                    report.reset_mock()
                    # Do not reinterpret a non-auth HTTP status from its body.
                    message = "fixture reports permission denied"
                    collect.return_value = (httpx.Response(status, text=message, headers={"Retry-After": "17"}),
                                            None, None)
                    result = await cookie_delivery.deliver_collect_response(gen)
                    self.assertEqual(result.status_code, status)
                    self.assertEqual(result.headers["retry-after"], "17")
                    self.assertEqual(json.loads(result.body)["error"], {
                        "code": status, "type": "upstream_error", "param": None,
                        "message": message, "retry_after": "17",
                    })
                    if status in (401, 403):
                        report.assert_called_once_with("fixture", credential=record)
                        self.assertIs(report.call_args.kwargs["credential"], record)
                    else:
                        report.assert_not_called()


class CookieCollectAuthHealthRouteTests(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.channel = "mixed"
        self.record = {"id": "fixture", "cookie": "SAPISID=offline", "project_id": "fixture", "enabled": True}
        self.requests = []
        self.status = 401
        self.envelope = False

        def provider(request):
            self.requests.append(request.headers["cookie"])
            body = {"code": self.status, "message": "offline Studio error"}
            if self.envelope:
                return httpx.Response(200, json={"errors": [body]})
            return httpx.Response(self.status, json={"error": body})

        studio = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.MockTransport(provider)))

        @asynccontextmanager
        async def open_studio(*args, **kwargs):
            yield studio

        self.enterContext(patch.object(app.state, "express_key_manager", SimpleNamespace(
            get_records=lambda: [], get_total_keys=lambda: 0)))
        self.enterContext(patch.dict(credential_manager._cookie_health, clear=True))
        self.enterContext(patch("credential_manager.time", SimpleNamespace(time=lambda: 1000.0)))
        self.enterContext(patch.object(app_state, "get_cookie_accounts", return_value=[self.record]))
        self.enterContext(patch.object(cookie_delivery, "build_headers", lambda cookie: {"cookie": cookie}))
        self.enterContext(patch.object(cookie_delivery, "open_upstream_client", open_studio))

    async def send(self, path, *, stream):
        return await self.client.post(path, json={"model": MODEL, "stream": stream, "max_tokens": 32,
                                                 "messages": [{"role": "user", "content": "hello"}]})

    async def test_json_and_fake_sse_reach_cooldown_after_exactly_three_auth_failures(self):
        for path in ("/v1/chat/completions", "/v1/messages"):
            for stream in (False, True):
                self.settings["fake_streaming"] = stream
                for status, envelope in ((401, False), (403, False), (401, True), (503, False)):
                    self.status, self.envelope = status, envelope
                    with (
                        self.subTest(path=path, stream=stream, status=status, envelope=envelope),
                        patch.dict(credential_manager._cookie_health, clear=True),
                    ):
                        self.requests.clear()
                        quarantined = status in (401, 403)
                        for i in range(COOKIE_AUTH_FAILURE_THRESHOLD):
                            r = await self.send(path, stream=stream)
                            self.assertEqual(r.status_code, 200 if stream else status, r.text)
                            self.assertIn("offline Studio error", r.text)
                            health = credential_manager.cookie_health_snapshot()
                            if quarantined:
                                expected = (i + 1) % COOKIE_AUTH_FAILURE_THRESHOLD
                                self.assertEqual(health.get("fixture", {}).get("failures"), expected)
                            else:
                                self.assertEqual(health, {})
                        last = await self.send(path, stream=stream)
                        self.assertEqual(last.status_code, 503 if quarantined or not stream else 200, last.text)
                        self.assertEqual(last.headers.get("retry-after"),
                                         str(COOKIE_COOLDOWN_SECONDS) if quarantined else None)
                        count = COOKIE_AUTH_FAILURE_THRESHOLD + (not quarantined)
                        self.assertEqual(self.requests, [self.record["cookie"]] * count)
