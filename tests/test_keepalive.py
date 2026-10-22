"""出站保活服务：启停生命周期、配置校验、真实 GET 回路与设置热重启钩子。"""

import asyncio
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-keepalive-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import config
import keepalive
from keepalive import KeepAliveService
from runtime_state import AppState
from settings_validation import validate_global_settings_patch


class _FakeState:
    """按 {key: value} 回答 get_setting 的轻量替身（只覆盖保活用到的键）。"""

    def __init__(self, values):
        self.get_setting = mock.Mock(side_effect=values.get)


class KeepAliveServiceTests(unittest.IsolatedAsyncioTestCase):
    def _make_service(self, values):
        """构造一个读到指定配置的保活服务；用例结束时自动还原全局 patch。"""
        service = KeepAliveService()
        patcher = mock.patch.object(keepalive, "app_state", _FakeState(values))
        patcher.start()
        self.addCleanup(patcher.stop)
        return service

    async def test_empty_url_never_starts_a_task(self):
        service = self._make_service({})
        await service.start()
        self.assertFalse(service.is_running)

    async def test_valid_url_starts_then_stop_is_idempotent(self):
        service = self._make_service({"keepalive_url": "https://example.com/healthz",
                                      "keepalive_interval": 60})
        await service.start()
        self.assertTrue(service.is_running)
        await service.stop()
        self.assertFalse(service.is_running)
        await service.stop()  # 幂等：未运行时再停一次不抛错
        self.assertFalse(service.is_running)

    async def test_non_http_scheme_is_rejected(self):
        service = self._make_service({"keepalive_url": "ftp://example.com/healthz",
                                      "keepalive_interval": 60})
        await service.start()
        self.assertFalse(service.is_running)

    async def test_interval_below_floor_is_rejected(self):
        service = self._make_service({"keepalive_url": "https://example.com/healthz",
                                      "keepalive_interval": 1})
        await service.start()
        self.assertFalse(service.is_running)

    async def test_restart_with_cleared_url_stops_running_task(self):
        service = self._make_service({"keepalive_url": "https://example.com/healthz",
                                      "keepalive_interval": 60})
        await service.start()
        self.assertTrue(service.is_running)
        # 控制台清空 URL 保存后的等效操作：配置变为空 → restart 应停止任务。
        keepalive.app_state.get_setting.side_effect = {}.get
        await service.restart()
        self.assertFalse(service.is_running)

    async def test_get_request_actually_reaches_target(self):
        hits = []

        async def handler(reader, writer):
            try:
                await reader.readline()          # request line
                while await reader.readline() not in (b"\r\n", b"", b"\n"):
                    pass                          # drain headers
                hits.append(1)
                writer.write(b"HTTP/1.1 200 OK\r\n"
                             b"Content-Length: 2\r\n"
                             b"Connection: close\r\n\r\nok")
                await writer.drain()
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        self.addCleanup(server.close)
        port = server.sockets[0].getsockname()[1]
        service = self._make_service({"keepalive_url": f"http://127.0.0.1:{port}/healthz",
                                      "keepalive_interval": 3600})
        try:
            await service.start()
            self.assertTrue(service.is_running)
            for _ in range(100):                  # 上限 5 秒等首跳
                if hits:
                    break
                await asyncio.sleep(0.05)
            # 间隔 3600s：服务应立即打出第一发 GET 后进入长眠，只命中一次。
            self.assertEqual(len(hits), 1)
        finally:
            await service.stop()
        self.assertFalse(service.is_running)


class KeepAliveSettingsValidationTests(unittest.TestCase):
    def test_defaults_registered_in_settings(self):
        self.assertIn("keepalive_url", config.DEFAULT_SETTINGS)
        self.assertIn("keepalive_interval", config.DEFAULT_SETTINGS)
        self.assertEqual(config.DEFAULT_SETTINGS["keepalive_url"], "")
        self.assertEqual(config.DEFAULT_SETTINGS["keepalive_interval"], 60)

    def test_interval_range_enforced(self):
        for bad in (3, 100000, True):
            error = validate_global_settings_patch({"keepalive_interval": bad})
            self.assertIsNotNone(error, bad)
        self.assertIsNone(validate_global_settings_patch({"keepalive_interval": 60}))
        self.assertIsNone(validate_global_settings_patch({"keepalive_interval": 86400}))

    def test_url_scheme_validated(self):
        for bad in ("example.com/healthz", "ftp://example.com", 123):
            error = validate_global_settings_patch({"keepalive_url": bad})
            self.assertIsNotNone(error, bad)
        for good in ("", "  ", "https://example.com/healthz", "http://127.0.0.1:8050/healthz"):
            error = validate_global_settings_patch({"keepalive_url": good})
            self.assertIsNone(error, good)


class KeepAliveEnvSeedTests(unittest.TestCase):
    def test_env_vars_seed_initial_settings(self):
        """环境变量 KEEPALIVE_URL / KEEPALIVE_INTERVAL 应成为初始设置值。"""
        repo = Path(__file__).resolve().parents[1]
        code = (
            "import sys; sys.path.insert(0, 'app')\n"
            "import config\n"
            "assert config.DEFAULT_SETTINGS['keepalive_url'] == 'https://example.com/healthz'\n"
            "assert config.DEFAULT_SETTINGS['keepalive_interval'] == 30\n"
            "print('ok')\n"
        )
        env = dict(os.environ, KEEPALIVE_URL="https://example.com/healthz",
                   KEEPALIVE_INTERVAL="30")
        result = subprocess.run([sys.executable, "-c", code], cwd=repo, env=env,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


class SettingsApiKeepAliveHookTests(unittest.TestCase):
    """POST /api/settings 保存保活键后应立即重启保活任务；其他键不触发。"""

    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient

        import console_auth
        import main
        from routes import settings_api

        cls._console_auth = console_auth
        cls._main = main
        cls.settings_api = settings_api
        cls.client = TestClient(main.app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        state = AppState()
        patches = (
            mock.patch.object(config, "API_KEY", "ka-test-password"),
            mock.patch.object(self.settings_api, "app_state", state),
        )
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def _login(self):
        response = self.client.post("/api/login", json={"password": "ka-test-password"})
        self.assertEqual(response.status_code, 200)
        return response.cookies

    def test_saving_keepalive_keys_restarts_service(self):
        cookies = self._login()
        service = mock.AsyncMock()
        with mock.patch.object(self.settings_api, "keepalive_service", service):
            response = self.client.post(
                "/api/settings",
                json={"keepalive_url": "https://example.com/healthz"},
                cookies=cookies,
            )
        self.assertEqual(response.status_code, 200)
        service.restart.assert_awaited_once()

    def test_saving_unrelated_keys_does_not_restart_service(self):
        cookies = self._login()
        service = mock.AsyncMock()
        with mock.patch.object(self.settings_api, "keepalive_service", service):
            response = self.client.post(
                "/api/settings",
                json={"fake_streaming": True},
                cookies=cookies,
            )
        self.assertEqual(response.status_code, 200)
        service.restart.assert_not_awaited()

    def test_invalid_keepalive_url_is_rejected_before_persisting(self):
        cookies = self._login()
        with mock.patch.object(self.settings_api, "app_state") as state:
            response = self.client.post(
                "/api/settings",
                json={"keepalive_url": "example.com/healthz"},
                cookies=cookies,
            )
        self.assertEqual(response.status_code, 400)
        state.update_settings.assert_not_called()


if __name__ == "__main__":
    unittest.main()
