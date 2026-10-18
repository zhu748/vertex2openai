"""控制台跨路由会话、模板加载与凭证热更新的离线集成回归。"""

import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-console-test-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.testclient import TestClient

import config
import console_auth
import main
from routes import console, credentials_api, settings_api
from runtime_state import AppState


class ConsoleRoutesTests(unittest.TestCase):
    def setUp(self):
        self.state = AppState()
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        for patch in (
            mock.patch.object(config, "API_KEY", "console-test-password"),
            mock.patch.object(settings_api, "app_state", self.state),
            mock.patch.object(credentials_api, "app_state", self.state),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def login(self):
        response = self.client.post("/api/login", json={"password": "console-test-password"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(console_auth.AUTH_COOKIE, response.cookies)
        return response.cookies[console_auth.AUTH_COOKIE]

    def test_every_management_route_requires_a_session(self):
        for method, path in (
            ("GET", "/api/stats"),
            ("GET", "/api/requests"),
            ("GET", "/api/requests/trace-missing"),
            ("GET", "/api/settings"),
            ("GET", "/api/settings/runtime"),
            ("POST", "/api/settings/mode"),
            ("POST", "/api/settings"),
            ("GET", "/api/capabilities"),
            ("GET", "/api/model-overrides"),
            ("POST", "/api/model-overrides/model"),
            ("DELETE", "/api/model-overrides/model"),
            ("GET", "/api/express-keys"),
            ("POST", "/api/express-keys"),
            ("POST", "/api/express-keys/probe"),
            ("POST", "/api/express-keys/project"),
            ("POST", "/api/cookie"),
            ("POST", "/api/proxy/test"),
            ("GET", "/stream-logs"),
        ):
            with self.subTest(method=method, path=path):
                response = self.client.request(method, path, json={})
                self.assertEqual(response.status_code, 401)

    def test_login_session_is_shared_and_revoked_across_routers(self):
        self.assertIn("请输入访问密码", self.client.get("/").text)
        token = self.login()
        self.assertIn("chart.js", self.client.get("/").text)
        for path in ("/api/settings", "/api/express-keys", "/api/stats"):
            self.assertEqual(self.client.get(path).status_code, 200)
        self.assertEqual(self.client.post("/api/logout").status_code, 200)
        # 即使客户端保留旧 Cookie，退出后它也必须在所有管理路由失效。
        self.client.cookies.set(console_auth.AUTH_COOKIE, token)
        self.assertEqual(self.client.get("/api/settings").status_code, 401)
        self.assertEqual(self.client.get("/api/express-keys").status_code, 401)
        self.assertIn("请输入访问密码", self.client.get("/").text)

    def test_templates_are_loaded_independently_of_working_directory(self):
        previous = Path.cwd()
        console._load_template.cache_clear()
        try:
            os.chdir(_STATE.name)
            self.assertEqual(self.client.get("/").status_code, 200)
            self.login()
            response = self.client.get("/")
            self.assertIn("text/html", response.headers["content-type"])
            self.assertIn("chart.js", response.text)
        finally:
            os.chdir(previous)
            console._load_template.cache_clear()

    def test_key_save_refreshes_the_registered_manager(self):
        self.login()
        manager = mock.Mock()
        with (
            mock.patch.object(main.app.state, "express_key_manager", manager),
            mock.patch.object(credentials_api, "start_background_task") as start,
        ):
            response = self.client.post(
                "/api/express-keys",
                json={
                    "keys": [{"key": "example-key", "project_id": "example-project"}],
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["keys"][0]["project_id"], "example-project")
        manager.refresh_keys.assert_called_once_with()
        start.assert_not_called()

    def test_valid_generation_defaults_and_null_resets_are_persisted(self):
        self.login()
        for path in ("/api/settings", "/api/model-overrides/gemini-test"):
            for patch in (
                {"default_temperature": 0, "default_top_p": 0, "default_max_tokens": 1},
                {"default_temperature": 1, "default_top_p": 1, "default_max_tokens": 8192},
                {"default_temperature": None, "default_top_p": None, "default_max_tokens": None},
            ):
                with self.subTest(path=path, patch=patch):
                    response = self.client.post(path, json=patch)
                    self.assertEqual(response.status_code, 200)
                    settings = (self.state.get_settings() if path == "/api/settings"
                                else self.state.get_model_overrides()["gemini-test"])
                    for key, value in patch.items():
                        self.assertEqual(settings[key], value)

    def test_first_reply_and_disconnect_settings_are_saved_at_the_correct_scope(self):
        self.login()
        patch = {"first_reply_timeout_seconds": 25.5, "resumable_disconnect_grace_seconds": 3.5}
        self.assertEqual(self.client.post('/api/settings', json=patch).status_code, 200)
        self.assertEqual(self.client.post('/api/model-overrides/gemini-test',
                         json={"first_reply_timeout_seconds": 0}).status_code, 200)
        for key, value in patch.items():
            self.assertEqual(self.client.get('/api/settings').json()[key], value)
        self.assertEqual(self.state.get_effective_settings('gemini-test')["first_reply_timeout_seconds"], 0)
        reloaded = AppState()
        self.assertEqual(reloaded.get_setting('resumable_disconnect_grace_seconds'), 3.5)
        self.assertEqual(reloaded.get_effective_settings('gemini-test')["first_reply_timeout_seconds"], 0)
        response = self.client.post('/api/model-overrides/gemini-test',
                                    json={"resumable_disconnect_grace_seconds": 2})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('resumable_disconnect_grace_seconds', response.json()['override'])
        self.assertEqual(self.state.get_setting('resumable_disconnect_grace_seconds'), 3.5)


if __name__ == "__main__":
    unittest.main()
