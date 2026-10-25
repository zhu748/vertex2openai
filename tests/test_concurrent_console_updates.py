"""Saved credential preservation must be resolved at commit time."""

import asyncio
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-concurrent-console-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.testclient import TestClient

import console_auth
import main
import lifecycle
import express_key_probe
from routes import credentials_api
from runtime_state import AppState, app_state


class CookieSaveConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.old = {"id": "account", "cookie": "SAPISID=old; SIDCC=old", "project_id": "project", "note": "old"}
        app_state.set_cookie_accounts([self.old])
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        token = console_auth.issue_session()
        self.addCleanup(console_auth.revoke_session, token)
        self.client.cookies.set(console_auth.AUTH_COOKIE, token)
        self.write = app_state.set_cookie_accounts

    def save(self, cookie=""):
        return self.client.post("/api/cookie-accounts", json={"accounts": [
            {**self.old, "cookie": cookie, "note": "edited", "proxies": ["direct"], "proxy_fallback": False}]})

    def test_blank_cookie_keeps_rotation_completed_between_validation_and_commit(self):
        new_cookie = "SAPISID=old; SIDCC=refreshed"

        def race(records, **kwargs):
            app_state.update_cookie_account_cookie("account", new_cookie)
            return self.write(records, **kwargs)

        with mock.patch.object(app_state, "set_cookie_accounts", side_effect=race):
            response = self.save()
        self.assertEqual(response.status_code, 200)
        record = AppState().get_cookie_accounts()[0]
        self.assertEqual(record["cookie"], new_cookie)
        self.assertEqual(record["note"], "edited")
        self.assertEqual(record["proxies"], ["direct"])
        self.assertFalse(record["proxy_fallback"])
        self.assertNotIn(new_cookie, response.text)

    def test_blank_cookie_cannot_resurrect_account_deleted_during_save(self):
        def race(records, **kwargs):
            self.write([])
            return self.write(records, **kwargs)

        with mock.patch.object(app_state, "set_cookie_accounts", side_effect=race):
            response = self.save()
        self.assertEqual(response.status_code, 409)
        self.assertEqual(AppState().get_cookie_accounts(), [])

    def test_explicit_cookie_replacement_remains_authoritative(self):
        replacement = "SAPISID=manual; SIDCC=manual"

        def race(records, **kwargs):
            app_state.update_cookie_account_cookie("account", "SAPISID=old; SIDCC=refreshed")
            return self.write(records, **kwargs)

        with mock.patch.object(app_state, "set_cookie_accounts", side_effect=race):
            response = self.save(replacement)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(AppState().get_cookie_accounts()[0]["cookie"], replacement)

    def test_preserved_cookie_collision_rejects_entire_save(self):
        collision = "SAPISID=collision"

        def race(records, **kwargs):
            app_state.update_cookie_account_cookie("account", collision)
            return self.write(records, **kwargs)

        with mock.patch.object(app_state, "set_cookie_accounts", side_effect=race):
            response = self.client.post("/api/cookie-accounts", json={"accounts": [
                {**self.old, "cookie": "", "note": "not committed"},
                {"id": "new", "cookie": collision, "project_id": "project"}]})
        self.assertEqual(response.status_code, 409)
        records = AppState().get_cookie_accounts()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["note"], "old")
        self.assertEqual(records[0]["cookie"], collision)


class AutomaticKeyProbeTests(unittest.TestCase):
    def setUp(self):
        self.records = [{"key": "disabled-key", "enabled": False},
                        {"key": "active-key", "enabled": True},
                        {"key": "known-key", "enabled": True, "project_id": "known"}]
        app_state.set_express_keys(self.records)

    def test_startup_only_probes_enabled_unresolved_keys(self):
        with mock.patch.object(express_key_probe, "probe_all", mock.AsyncMock(return_value=[])) as probe:
            asyncio.run(lifecycle._probe_express_projects_on_startup())
        probe.assert_awaited_once_with(keys=["active-key"])

    def test_saving_only_schedules_enabled_unresolved_keys(self):
        token = console_auth.issue_session()
        self.addCleanup(console_auth.revoke_session, token)
        client = TestClient(main.app)
        self.addCleanup(client.close)
        client.cookies.set(console_auth.AUTH_COOKIE, token)
        with (mock.patch.object(credentials_api, "start_background_task", side_effect=lambda coroutine: coroutine.close()) as schedule,
              mock.patch.object(express_key_probe, "probe_all", mock.AsyncMock(return_value=[])) as probe):
            response = client.post("/api/express-keys", json={"keys": self.records})
            self.assertEqual(response.status_code, 200)
            schedule.assert_called_once()
            probe.assert_called_once_with(keys=["active-key"])
            schedule.reset_mock()
            probe.reset_mock()
            response = client.post("/api/express-keys", json={"keys": [self.records[0]]})
            self.assertEqual(response.status_code, 200)
            schedule.assert_not_called()
            probe.assert_not_called()

    def test_background_probe_rechecks_disabled_state_but_explicit_force_is_supported(self):
        network = mock.AsyncMock(return_value=("detected", "fixture"))
        with mock.patch.object(express_key_probe, "probe_project_id", network):
            skipped = asyncio.run(express_key_probe.probe_and_store("disabled-key"))
            self.assertIn("停用", skipped["message"])
            network.assert_not_awaited()
            forced = asyncio.run(express_key_probe.probe_and_store("disabled-key", force=True))
            self.assertTrue(forced["ok"])
            network.assert_awaited_once_with("disabled-key")


if __name__ == "__main__":
    unittest.main()
