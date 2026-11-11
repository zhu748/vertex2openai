"""阶段一安全止血回归（SEC-P1-1 / SEC-P1-2 / SEC-P3-4 / SEC-P2-5 / QUA-P2-8）。

覆盖 2026-11 全面审查报告路线图第一批：
- 弱口令默认拒绝启动（ALLOW_DEFAULT_KEY 显式放行）
- ADMIN_PASSWORD 与 API_KEY 拆分（分享 Key 不再交出控制台）
- Cookie reveal 二次口令验证 + 指数退避限速 + 审计日志
- 安全响应头（CSP / nosniff / no-referrer / X-Frame-Options）
- 出厂口令常量单一来源
"""

import os
import unittest
from unittest import mock

from fastapi.testclient import TestClient

import config
import console_auth
import lifecycle
from runtime_state import app_state


class WeakKeyStartupPolicyTest(unittest.TestCase):
    """SEC-P1-1：出厂弱口令默认拒绝启动。"""

    def test_factory_default_key_is_rejected(self):
        for weak in (config.DEFAULT_API_KEY, "change-me-please"):
            with self.subTest(weak=weak), \
                 mock.patch.object(config, "API_KEY", weak), \
                 mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("ALLOW_DEFAULT_KEY", None)
                with self.assertRaises(RuntimeError):
                    lifecycle.enforce_strong_api_key()

    def test_allow_default_key_explicitly_permits_weak_key(self):
        for flag in ("1", "true", "yes"):
            with self.subTest(flag=flag), \
                 mock.patch.object(config, "API_KEY", config.DEFAULT_API_KEY), \
                 mock.patch.dict(os.environ, {"ALLOW_DEFAULT_KEY": flag}):
                lifecycle.enforce_strong_api_key()  # 不抛异常即通过

    def test_strong_key_passes_silently(self):
        with mock.patch.object(config, "API_KEY", "a-strong-random-key"):
            lifecycle.enforce_strong_api_key()

    def test_public_host_message_mentions_hosted_environment(self):
        hosted = {"SPACE_ID": "demo"}
        try:
            with mock.patch.object(config, "API_KEY", config.DEFAULT_API_KEY), \
                 mock.patch.dict(os.environ, hosted):
                with self.assertRaises(RuntimeError) as ctx:
                    lifecycle.enforce_strong_api_key()
                self.assertIn("公开托管环境", str(ctx.exception))
        finally:
            for key in hosted:
                os.environ.pop(key, None)

    def test_weak_key_constant_has_single_source(self):
        # QUA-P2-8：console_auth 的 DEFAULT_API_KEY 必须来自 config（消除双定义漂移）。
        self.assertIs(console_auth.DEFAULT_API_KEY, config.DEFAULT_API_KEY)


class AdminPasswordSplitTest(unittest.TestCase):
    """SEC-P1-2：控制台口令与 API Key 拆分。"""

    def setUp(self):
        console_auth._sessions.clear()
        console_auth._login_failures.clear()
        self.client = TestClient(__import__("main").app)
        self.addCleanup(self.client.close)

    def _login(self, password):
        return self.client.post("/api/login", json={"password": password})

    def test_admin_password_takes_precedence_for_console_login(self):
        with mock.patch.object(config, "ADMIN_PASSWORD", "console-only-secret"), \
             mock.patch.object(config, "API_KEY", "shared-api-key"):
            self.assertEqual(self._login("console-only-secret").status_code, 200)
            # 拿到 API Key 的人不再能登录控制台（信任级别拆分的核心语义）。
            self.assertEqual(self._login("shared-api-key").status_code, 401)

    def test_fallback_to_api_key_when_admin_password_unset(self):
        with mock.patch.object(config, "ADMIN_PASSWORD", None), \
             mock.patch.object(config, "API_KEY", "legacy-shared-key"):
            self.assertEqual(self._login("legacy-shared-key").status_code, 200)

    def test_generation_endpoints_still_accept_api_key(self):
        with mock.patch.object(config, "ADMIN_PASSWORD", "console-only-secret"), \
             mock.patch.object(config, "API_KEY", "shared-api-key"), \
             mock.patch("auth.API_KEY", "shared-api-key"):
            # 生成端点鉴权不使用 ADMIN_PASSWORD：Key 仍可正常调用。
            response = self.client.get("/v1/models", headers={"Authorization": "Bearer shared-api-key"})
            self.assertNotEqual(response.status_code, 401, response.text)
            denied = self.client.get("/v1/models", headers={"Authorization": "Bearer console-only-secret"})
            self.assertEqual(denied.status_code, 401)


class RevealPasswordConfirmationTest(unittest.TestCase):
    """SEC-P3-4：Cookie reveal 二次口令验证、限速与审计。"""

    def setUp(self):
        console_auth._sessions.clear()
        console_auth._login_failures.clear()
        self.client = TestClient(__import__("main").app)
        self.addCleanup(self.client.close)

    def _prepare_account(self):
        app_state.set_cookie_accounts([
            {"cookie": "SAPISID=reveal-secret; SID=sid-secret", "project_id": "p1"}])
        accounts = app_state.get_cookie_accounts()
        self.login()
        return accounts[0]["id"]

    def login(self):
        with mock.patch.object(config, "ADMIN_PASSWORD", None), \
             mock.patch.object(config, "API_KEY", "reveal-console-key"):
            response = self.client.post("/api/login", json={"password": "reveal-console-key"})
            self.assertEqual(response.status_code, 200, response.text)

    def test_reveal_requires_matching_password(self):
        account_id = self._prepare_account()
        with mock.patch.object(config, "ADMIN_PASSWORD", None), \
             mock.patch.object(config, "API_KEY", "reveal-console-key"):
            wrong = self.client.post("/api/cookie-accounts/reveal",
                                     json={"id": account_id, "password": "wrong"})
            self.assertEqual(wrong.status_code, 403)
            self.assertNotIn("SAPISID", wrong.text)

            missing = self.client.post("/api/cookie-accounts/reveal", json={"id": account_id})
            self.assertEqual(missing.status_code, 403)

            ok = self.client.post("/api/cookie-accounts/reveal",
                                  json={"id": account_id, "password": "reveal-console-key"})
            self.assertEqual(ok.status_code, 200, ok.text)
            self.assertEqual(ok.json()["cookie"], "SAPISID=reveal-secret; SID=sid-secret")

    def test_reveal_failures_share_login_backoff(self):
        account_id = self._prepare_account()
        with mock.patch.object(config, "ADMIN_PASSWORD", None), \
             mock.patch.object(config, "API_KEY", "reveal-console-key"):
            for _ in range(3):
                self.client.post("/api/cookie-accounts/reveal",
                                 json={"id": account_id, "password": "nope"})
            # 三次失败后即使口令正确也被限速：被劫持的会话无法高频爆破。
            blocked = self.client.post("/api/cookie-accounts/reveal",
                                       json={"id": account_id, "password": "reveal-console-key"})
            self.assertEqual(blocked.status_code, 429)
            self.assertIn("Retry-After", blocked.headers)


class SecurityHeadersTest(unittest.TestCase):
    """SEC-P2-5：全部响应附 nosniff/no-referrer，HTML 额外附 CSP。"""

    def setUp(self):
        console_auth._sessions.clear()
        self.client = TestClient(__import__("main").app)
        self.addCleanup(self.client.close)

    def test_html_responses_carry_csp_and_frame_protection(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["referrer-policy"], "no-referrer")
        csp = response.headers["content-security-policy"]
        self.assertIn("default-src 'self'", csp)
        self.assertIn("frame-ancestors 'none'", csp)
        self.assertIn("object-src 'none'", csp)
        self.assertEqual(response.headers["x-frame-options"], "DENY")

    def test_json_api_responses_carry_nosniff_and_referrer_policy(self):
        response = self.client.get("/healthz")
        self.assertNotEqual(response.status_code, 500)
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["referrer-policy"], "no-referrer")
        # 非 HTML 响应不带 CSP（对 JSON 无意义）。
        self.assertNotIn("content-security-policy", response.headers)

    def test_login_failure_response_carries_headers(self):
        console_auth._login_failures.clear()
        response = self.client.post("/api/login", json={"password": "not-the-password"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")


if __name__ == "__main__":
    unittest.main()
