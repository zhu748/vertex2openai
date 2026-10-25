"""Cookie session probe API contracts and reason labels, without real credentials."""
from contextlib import asynccontextmanager
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="cookie-probe-console-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from fastapi.testclient import TestClient
import console_auth
import cookie_probe
import impersonate
import main
from routes import credentials_api


class CookieProbeAPITests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        self.client.cookies.set(console_auth.AUTH_COOKIE, console_auth.issue_session())
        self.records = [{"id": name, "cookie": "SAPISID=private-fixture-" + name,
                         "project_id": "project", "enabled": name != "disabled"}
                        for name in ("valid", "invalid", "network", "disabled")]
        state = mock.patch.object(credentials_api.app_state, "get_cookie_accounts", return_value=self.records)
        state.start()
        self.addCleanup(state.stop)
        probe = mock.patch.object(credentials_api, "probe_cookie_accounts", new_callable=mock.AsyncMock)
        self.probe = probe.start()
        self.addCleanup(probe.stop)
        health = mock.patch("credential_manager.credential_manager.cookie_health_snapshot",
                            return_value={"invalid": {"failures": 2, "cooldown_remaining": 0}})
        health.start()
        self.addCleanup(health.stop)

    def test_probe_requires_console_auth(self):
        self.client.cookies.clear()
        self.assertEqual(self.client.post('/api/cookie-accounts/test', json={}).status_code, 401)
        self.probe.assert_not_called()

    def test_batch_counts_unknown_separately_and_returns_health_without_secrets(self):
        self.probe.return_value = [{"id": "valid", "status": "valid"},
                                   {"id": "invalid", "status": "invalid"},
                                   {"id": "network", "status": "error"}]
        response = self.client.post('/api/cookie-accounts/test', json={})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        data = response.json()
        self.assertEqual([data[key] for key in ('total', 'valid', 'invalid', 'errors')], [3, 1, 1, 1])
        self.assertEqual([r['id'] for r in self.probe.call_args.args[0]], ['valid', 'invalid', 'network'])
        self.assertEqual(data['results'][1]['health']['failures'], 2)
        self.assertNotIn('private-fixture', response.text)

    def test_single_id_uses_saved_record_and_disabled_or_unknown_ids_are_rejected(self):
        self.probe.return_value = [{"id": "valid", "status": "valid"}]
        self.assertEqual(self.client.post('/api/cookie-accounts/test', json={'id': ' valid '}).status_code, 200)
        self.assertEqual(self.probe.call_args.args[0], [self.records[0]])
        self.probe.reset_mock()
        for name in ('disabled', 'missing'):
            self.assertEqual(self.client.post('/api/cookie-accounts/test', json={'id': name}).status_code, 404)
        self.probe.assert_not_called()


class CookieProbeReasonTests(unittest.IsolatedAsyncioTestCase):
    async def run_probe(self, status, code=None):
        @asynccontextmanager
        async def open_client(options):
            body = {'results': [{'errors': [{'message': 'fixture', 'extensions': {'status': {'code': code}}}]}]}
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(status, json=body))) as client:
                yield client

        with mock.patch.object(cookie_probe, 'open_upstream_client', open_client), \
                mock.patch.object(cookie_probe, 'credential_manager') as manager, \
                mock.patch.object(impersonate, 'get_impersonate_profile', return_value='off'):
            result = await cookie_probe.probe_account({'id': 'fixture', 'cookie': 'SAPISID=fixture', 'project_id': 'project'})
        return result, manager

    async def test_permission_rejection_and_authentication_failure_have_different_reasons(self):
        for http_status, code, reason in ((401, None, 'unauthenticated'), (403, None, 'permission_denied'),
                                          (200, 16, 'unauthenticated'), (200, 7, 'permission_denied')):
            with self.subTest(http_status=http_status, code=code):
                result, manager = await self.run_probe(http_status, code)
                self.assertEqual(result['status'], 'invalid')
                self.assertEqual(result['reason'], reason)
                self.assertLessEqual(result['checked_at'], time.time())
                manager.record_cookie_auth_failure.assert_called_once()

    async def test_server_failure_is_unknown_and_not_an_authentication_failure(self):
        for http_status in (429, 503):
            result, manager = await self.run_probe(http_status, 5)
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['reason'], 'upstream_unavailable')
            manager.record_cookie_auth_failure.assert_not_called()
            manager.record_cookie_success.assert_not_called()

    async def test_expected_missing_model_proves_session_acceptance(self):
        result, manager = await self.run_probe(200, 5)
        self.assertEqual(result['status'], 'valid')
        self.assertEqual(result['reason'], 'session_accepted')
        manager.record_cookie_success.assert_called_once()

    async def test_resource_exhaustion_is_accepted_authentication_with_a_visible_limit(self):
        result, manager = await self.run_probe(200, 8)
        self.assertEqual(result['status'], 'valid')
        self.assertEqual(result['reason'], 'resource_exhausted')
        self.assertIn('资源/配额受限', result['detail'])
        manager.record_cookie_auth_failure.assert_not_called()
        manager.record_cookie_success.assert_called_once()


if __name__ == '__main__':
    unittest.main()
