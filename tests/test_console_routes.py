"""控制台跨路由会话、模板加载与凭证热更新的离线集成回归。"""

import os
import re
from html.parser import HTMLParser
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
import console_assets
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
        self.assertIn("/static/dashboard.js", self.client.get("/").text)
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
            self.assertIn("/static/dashboard.js", response.text)
        finally:
            os.chdir(previous)
            console._load_template.cache_clear()

    def test_credential_and_configuration_responses_are_not_cacheable(self):
        self.login()
        with mock.patch.object(settings_api, "get_express_models", mock.AsyncMock(return_value=[])):
            for path in ("/api/express-keys", "/api/cookie-accounts", "/api/settings",
                         "/api/settings/runtime", "/api/model-overrides", "/api/capabilities"):
                with self.subTest(path=path):
                    response = self.client.get(path)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.headers.get("cache-control"), "no-store")
        response = self.client.post("/api/settings", json={"proxy_urls": []})
        self.assertEqual(response.headers.get("cache-control"), "no-store")
        response = self.client.post("/api/express-keys", json={"keys": []})
        self.assertEqual(response.headers.get("cache-control"), "no-store")

    def test_login_and_dashboard_serve_all_assets_locally(self):
        class Assets(HTMLParser):
            def __init__(self):
                super().__init__()
                self.urls = []

            def handle_starttag(self, tag, attrs):
                values = dict(attrs)
                if tag == 'script' and values.get('src'):
                    self.urls.append(values['src'])
                if tag == 'link' and values.get('rel') == 'stylesheet':
                    self.urls.append(values['href'])

        for authenticated in (False, True):
            if authenticated:
                self.login()
            assets = Assets()
            assets.feed(self.client.get('/').text)
            self.assertGreaterEqual(len(assets.urls), 3)
            for url in assets.urls:
                with self.subTest(authenticated=authenticated, asset=url):
                    self.assertTrue(url.startswith('/static/'), url)
                    response = self.client.get(url)
                    self.assertEqual(response.status_code, 200)
                    self.assertGreater(len(response.content), 0)

    def test_console_documents_and_assets_cannot_reuse_a_previous_deployment(self):
        for authenticated in (False, True):
            if authenticated:
                self.login()
            response = self.client.get("/")
            self.assertEqual(response.headers["cache-control"], "no-store")
            self.assertIn("Cookie", response.headers["vary"])
            urls = re.findall(r'(?:src|href)="(/static/[^"?]+\.(?:js|css)\?v=[a-f0-9]{16})"', response.text)
            self.assertGreaterEqual(len(urls), 3)
            self.assertEqual(len({url.split("?v=")[1] for url in urls}), 1)
            for url in urls:
                asset = self.client.get(url)
                self.assertEqual(asset.status_code, 200)
                # 版本化 URL 由内容哈希锁定：内容一变哈希/URL 一起变，
                # 长缓存不会复用上一个部署的代码；无版本 URL 仍强制 revalidate。
                self.assertIn("immutable", asset.headers["cache-control"])
                cached = self.client.get(url, headers={"If-None-Match": asset.headers["etag"]})
                self.assertEqual(cached.status_code, 304)
                self.assertIn("immutable", cached.headers["cache-control"])
            # Browsers that still have old HTML also revalidate bare URLs.
            self.assertIn("must-revalidate", self.client.get('/static/dashboard.js').headers["cache-control"])

    def test_changing_any_console_module_changes_the_shared_asset_version(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(console_assets, "STATIC_DIR", Path(directory)):
            module = Path(directory) / "channel-config.js"
            module.write_text("old component", encoding="utf-8")
            template = '<script src="/static/channel-config.js"></script><script src="/static/dashboard.js"></script>'
            before = console_assets.versioned_assets(template)
            module.write_text("new component", encoding="utf-8")
            after = console_assets.versioned_assets(template)
            self.assertNotEqual(before, after)
            versions = re.findall(r'\?v=([a-f0-9]{16})', after)
            self.assertEqual(versions[0], versions[1])

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

    def test_capabilities_keep_builtin_sampling_advice_when_overridden(self):
        self.login()
        model = 'gemini-4-argon'
        self.state.set_model_override(model, {'sampling_policy': 'allowed'})
        with mock.patch.object(settings_api, 'get_express_models',
                               new=mock.AsyncMock(return_value=[model])):
            cap = self.client.get('/api/capabilities').json()['capabilities'][model]
        self.assertEqual(cap['sampling_advice'], 'recommend_default')
        self.assertEqual(cap['default_sampling_advice'], 'deprecated')


if __name__ == "__main__":
    unittest.main()
