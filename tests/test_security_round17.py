"""第 17 轮（SEC 域收尾）回归测试。

覆盖 2026-11 全面审查剩余安全项的修复契约：
- SEC-P2-2  web_state.json 可选静态加密（信封往返 / 迁移 / 拒绝降级启动）
- SEC-P2-3  会话双窗口（7 天绝对 + 2 小时闲置 + 滑动续期）与 revoke-all
- SEC-P2-4  明文 HTTP 登录告警（一次性）与 FORCE_SECURE_COOKIE
- SEC-P2-6  keepalive 私网目标显式确认（含设置保存校验联动）
- SEC-P3-1  CORS 默认同源、ALLOWED_ORIGINS 显式放行
- SEC-P3-2  Origin: null 与 Sec-Fetch-Site: cross-site 否决
- SEC-P3-3  登录 429 附 TRUST_PROXY_HEADERS 设置指引
- SEC-P3-5  生图 URL TTL 默认 24 小时（60s–7d 钳位）
- SEC-P3-6  代理连通性测试的内网地址闸门与端点禁用开关
- PRF-P2-5  请求指纹超长串首尾窗口折叠
"""

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.testclient import TestClient

import config
import console_auth
import main
import middleware
import runtime_state
import state_crypto
from routes import console as console_routes
from routes import system_api
from settings_validation import validate_global_settings_patch


PASSPHRASE = "round17-state-passphrase"


def _strip_state_crypto_caches() -> None:
    """跨用例清掉进程级 Fernet 缓存与认领盐，保证用例独立。"""
    state_crypto._fernet_cache.clear()
    state_crypto._process_salt = None


class StateCryptoUnitTests(unittest.TestCase):
    """SEC-P2-2：加密原语的往返 / 拒绝语义 / 盐复用。"""

    def setUp(self):
        _strip_state_crypto_caches()

    def _env(self, value=PASSPHRASE):
        if value is None:
            return mock.patch.dict(os.environ, {}, clear=False)
        return mock.patch.dict(os.environ, {"STATE_ENCRYPTION_KEY": value})

    def test_roundtrip_preserves_state(self):
        snapshot = {"settings": {"retry_max": 7}, "google_cookie": "SAPISID=abc"}
        with self._env():
            envelope = state_crypto.encrypt_state_json(snapshot)
            self.assertTrue(state_crypto.is_encryption_envelope(envelope))
            self.assertEqual(envelope["v"], 1)
            self.assertEqual(envelope["alg"], "fernet-pbkdf2-sha256")
            # 密文里不能出现明文片段（Cookie 值 / 设置键）。
            self.assertNotIn("SAPISID=abc", json.dumps(envelope))
            self.assertEqual(state_crypto.decrypt_state_json(envelope), snapshot)

    def test_no_key_encrypt_is_identity(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("STATE_ENCRYPTION_KEY", None)
            self.assertFalse(state_crypto.encryption_enabled())
            snapshot = {"a": 1}
            # 未设密钥：原样返回（明文行为完全不变）。
            self.assertIs(state_crypto.encrypt_state_json(snapshot), snapshot)
            self.assertIsNone(state_crypto.self_check())

    def test_envelope_without_key_refuses(self):
        with self._env():
            envelope = state_crypto.encrypt_state_json({"x": 1})
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("STATE_ENCRYPTION_KEY", None)
            with self.assertRaises(state_crypto.StateCryptoKeyError) as ctx:
                state_crypto.decrypt_state_json(envelope)
            self.assertIn("未设置 STATE_ENCRYPTION_KEY", str(ctx.exception))

    def test_wrong_passphrase_refuses(self):
        with self._env("correct-horse"):
            envelope = state_crypto.encrypt_state_json({"x": 1})
        with self._env("battery-staple"):
            with self.assertRaises(state_crypto.StateCryptoKeyError):
                state_crypto.decrypt_state_json(envelope)

    def test_reencryption_reuses_adopted_salt(self):
        """解密后重加密复用同一盐：PBKDF2 每进程只派生一次。"""
        with self._env():
            envelope1 = state_crypto.encrypt_state_json({"x": 1})
            state_crypto.decrypt_state_json(envelope1)  # 认领盐
            envelope2 = state_crypto.encrypt_state_json({"x": 2})
            self.assertEqual(envelope1["salt"], envelope2["salt"])
            # 复用盐后 Fernet 命中缓存，不再新增派生条目。
            self.assertEqual(len(state_crypto._fernet_cache), 1)

    def test_missing_cryptography_library_fails_loudly(self):
        blocked = {"cryptography": None, "cryptography.fernet": None}
        with self._env(), mock.patch.dict(sys.modules, blocked):
            error = state_crypto.self_check()
            self.assertIsNotNone(error)
            self.assertIn("cryptography", error)


class AppStateEncryptionTests(unittest.TestCase):
    """SEC-P2-2：与 runtime_state 持久化链路的集成（迁移 / 拒绝启动）。"""

    def setUp(self):
        _strip_state_crypto_caches()
        self.directory = tempfile.mkdtemp(prefix="round17-state-")
        self.state_file = str(Path(self.directory) / "web_state.json")
        self._state_file_patch = mock.patch.object(runtime_state, "STATE_FILE", self.state_file)
        self._state_file_patch.start()
        self.addCleanup(self._state_file_patch.stop)

    def _app_state(self):
        return runtime_state.AppState()

    def test_plaintext_migrates_to_envelope_on_save(self):
        Path(self.state_file).write_text(
            json.dumps({"settings": {"retry_max": 4}, "google_cookie": "SID=secret"}),
            encoding="utf-8")
        with mock.patch.dict(os.environ, {"STATE_ENCRYPTION_KEY": PASSPHRASE}):
            state = self._app_state()
            # 明文读入成功，保存后磁盘升级为密文信封。
            state.update_settings({"retry_max": 9})
            on_disk = json.loads(Path(self.state_file).read_text(encoding="utf-8"))
            self.assertTrue(state_crypto.is_encryption_envelope(on_disk))
            self.assertNotIn("SID=secret", Path(self.state_file).read_text(encoding="utf-8"))
            # 重启（新实例）能解回同样的值。
            reloaded = self._app_state()
            self.assertEqual(reloaded.get_setting("retry_max"), 9)

    def test_envelope_without_key_refuses_to_boot(self):
        with mock.patch.dict(os.environ, {"STATE_ENCRYPTION_KEY": PASSPHRASE}):
            state = self._app_state()
            state.update_settings({"retry_max": 5})
        self.assertTrue(state_crypto.is_encryption_envelope(
            json.loads(Path(self.state_file).read_text(encoding="utf-8"))))
        # 无密钥启动：拒绝（RuntimeError），不进入损坏备份路径。
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("STATE_ENCRYPTION_KEY", None)
            with self.assertRaises(RuntimeError) as ctx:
                self._app_state()
            self.assertIn("STATE_ENCRYPTION_KEY", str(ctx.exception))
            # 文件原样保留（没有被改名成 .corrupt-* 备份）。
            self.assertTrue(Path(self.state_file).exists())
            self.assertEqual([p.name for p in Path(self.directory).iterdir()
                              if ".corrupt-" in p.name], [])

    def test_envelope_wrong_key_refuses_to_boot(self):
        with mock.patch.dict(os.environ, {"STATE_ENCRYPTION_KEY": PASSPHRASE}):
            self._app_state().update_settings({"retry_max": 5})
        with mock.patch.dict(os.environ, {"STATE_ENCRYPTION_KEY": "wrong"}):
            with self.assertRaises(RuntimeError):
                self._app_state()

    def test_without_key_plaintext_path_unchanged(self):
        Path(self.state_file).write_text(json.dumps({"settings": {"retry_max": 3}}),
                                         encoding="utf-8")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("STATE_ENCRYPTION_KEY", None)
            state = self._app_state()
            state.update_settings({"retry_max": 6})
            on_disk = Path(self.state_file).read_text(encoding="utf-8")
            self.assertIn("retry_max", on_disk)  # 仍是明文 JSON
            self.assertEqual(json.loads(on_disk)["settings"]["retry_max"], 6)


class SessionDualWindowTests(unittest.TestCase):
    """SEC-P2-3：7 天绝对窗口 + 2 小时闲置窗口 + 滑动续期 + revoke-all。"""

    def setUp(self):
        console_auth._sessions.clear()
        console_auth._login_failures.clear()
        console_auth._TRUST_PROXY_HEADERS = False
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        patch = mock.patch.object(config, "API_KEY", "round17-console-pw")
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(console_auth._sessions.clear)

    def _login(self):
        response = self.client.post("/api/login", json={"password": "round17-console-pw"})
        self.assertEqual(response.status_code, 200)
        return response.cookies[console_auth.AUTH_COOKIE]

    def _dashboard_shows(self, token):
        # login.html 也引用 dashboard.css，用 dashboard.js（仅大盘加载）与
        # 登录页口令文案做区分。
        response = self.client.get("/", cookies={console_auth.AUTH_COOKIE: token})
        return ("/static/dashboard.js" in response.text
                and "请输入访问密码" not in response.text)

    def test_defaults_are_dual_window(self):
        self.assertEqual(console_auth.SESSION_TTL_SECONDS, 7 * 86400)
        self.assertEqual(console_auth.SESSION_IDLE_TTL_SECONDS, 2 * 3600)

    def test_idle_window_expires_session(self):
        token = self._login()
        now = time.time()
        with console_auth._sessions_lock:
            issued, _ = console_auth._sessions[token]
            console_auth._sessions[token] = (issued, now - 3 * 3600)  # 闲置 3 小时
        self.assertFalse(self._dashboard_shows(token))

    def test_absolute_window_expires_session(self):
        token = self._login()
        now = time.time()
        with console_auth._sessions_lock:
            console_auth._sessions[token] = (now - 8 * 86400, now)  # 签发超 7 天
        self.assertFalse(self._dashboard_shows(token))

    def test_recent_session_slides_and_renews(self):
        token = self._login()
        now = time.time()
        with console_auth._sessions_lock:
            issued, _ = console_auth._sessions[token]
            console_auth._sessions[token] = (issued, now - 90 * 60)  # 闲置 1.5 小时
        self.assertTrue(self._dashboard_shows(token))
        # 鉴权成功滑动续期：last_seen 刷新到当下。
        with console_auth._sessions_lock:
            _, last_seen = console_auth._sessions[token]
            self.assertGreater(last_seen, now - 5)

    def test_revoke_all_sessions_endpoint(self):
        token_a = self._login()
        token_b = self._login()
        self.assertEqual(len(console_auth._sessions), 2)
        response = self.client.post("/api/sessions/revoke-all",
                                    cookies={console_auth.AUTH_COOKIE: token_a})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "revoked": 2})
        self.assertEqual(console_auth._sessions, {})
        self.assertFalse(self._dashboard_shows(token_a))
        self.assertFalse(self._dashboard_shows(token_b))

    def test_revoke_all_requires_auth(self):
        response = self.client.post("/api/sessions/revoke-all")
        self.assertEqual(response.status_code, 401)


class LoginHardeningTests(unittest.TestCase):
    """SEC-P2-4 明文 HTTP 告警 + SEC-P3-3 429 提示。"""

    def setUp(self):
        console_auth._sessions.clear()
        console_auth._login_failures.clear()
        console_auth._TRUST_PROXY_HEADERS = False
        console_routes._plain_http_warned = False
        self.client = TestClient(main.app)  # http://testserver
        self.addCleanup(self.client.close)
        patch = mock.patch.object(config, "API_KEY", "round17-console-pw")
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(console_auth._login_failures.clear)

    def test_plain_http_login_warns_once(self):
        with mock.patch("builtins.print") as printed:
            for _ in range(2):
                response = self.client.post("/api/login", json={"password": "round17-console-pw"})
                self.assertEqual(response.status_code, 200)
        warnings = [str(call) for call in printed.call_args_list
                    if "明文 HTTP" in str(call)]
        self.assertEqual(len(warnings), 1)  # 进程级一次性

    def test_force_secure_cookie_makes_cookie_secure(self):
        with mock.patch.object(console_routes, "_FORCE_SECURE_COOKIE", True):
            response = self.client.post("/api/login", json={"password": "round17-console-pw"})
        self.assertIn("secure", response.headers["set-cookie"].lower())

    def test_forwarded_proto_https_makes_cookie_secure(self):
        response = self.client.post(
            "/api/login", json={"password": "round17-console-pw"},
            headers={"x-forwarded-proto": "https"})
        self.assertIn("secure", response.headers["set-cookie"].lower())

    def test_plain_http_cookie_not_secure(self):
        response = self.client.post("/api/login", json={"password": "round17-console-pw"})
        self.assertNotIn("secure", response.headers["set-cookie"].lower())

    def test_rate_limit_429_hints_trust_proxy_headers(self):
        ip = "testclient"
        for _ in range(4):
            console_auth.record_login_failure(ip)
        remain = console_auth.login_retry_after(ip)
        self.assertGreater(remain, 0)
        response = self.client.post("/api/login", json={"password": "round17-console-pw"})
        self.assertEqual(response.status_code, 429)
        self.assertIn("TRUST_PROXY_HEADERS", response.json()["error"])
        # 反代已可信时：不再附提示。
        console_auth._TRUST_PROXY_HEADERS = True
        try:
            response = self.client.post("/api/login", json={"password": "round17-console-pw"})
            self.assertEqual(response.status_code, 429)
            self.assertNotIn("TRUST_PROXY_HEADERS", response.json()["error"])
        finally:
            console_auth._TRUST_PROXY_HEADERS = False


class CorsSameOriginDefaultTests(unittest.TestCase):
    """SEC-P3-1：CORS 默认同源，ALLOWED_ORIGINS 显式放行。"""

    def test_parse_cors_origins(self):
        parse = main.parse_cors_origins
        self.assertIsNone(parse(""))
        self.assertIsNone(parse("   "))
        self.assertEqual(parse("*"), ["*"])
        self.assertEqual(parse("https://a.com,https://b.com"),
                         ["https://a.com", "https://b.com"])
        self.assertEqual(parse("https://a.com https://b.com"),
                         ["https://a.com", "https://b.com"])
        self.assertEqual(parse(" https://a.com , https://b.com "),
                         ["https://a.com", "https://b.com"])

    def test_default_deployment_installs_no_cors_middleware(self):
        # 测试环境未设 ALLOWED_ORIGINS：默认同源。
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ALLOWED_ORIGINS", None)
            self.assertIsNone(main.parse_cors_origins(os.environ.get("ALLOWED_ORIGINS", "")))

    def test_star_restores_legacy_behavior(self):
        self.assertEqual(main.parse_cors_origins("*"), ["*"])


class CsrfFetchMetadataTests(unittest.TestCase):
    """SEC-P3-2：Origin: null / Sec-Fetch-Site: cross-site 否决。"""

    @staticmethod
    def _scope(headers):
        # ASGI 头是 (bytes, bytes) 序列。
        return {"type": "http",
                "headers": [(k.lower().encode("latin-1"), v.encode("latin-1"))
                             for k, v in headers.items()]}

    def test_origin_null_denied(self):
        self.assertTrue(middleware._csrf_origin_denied(
            self._scope({"origin": "null", "host": "console.example"})))

    def test_origin_null_case_insensitive_denied(self):
        self.assertTrue(middleware._csrf_origin_denied(
            self._scope({"origin": "NULL", "host": "console.example"})))

    def test_sec_fetch_site_cross_site_denied_without_origin(self):
        # 跨站 POST 未带 Origin（老浏览器/特殊客户端）：Sec-Fetch-Site 兜底否决。
        self.assertTrue(middleware._csrf_origin_denied(
            self._scope({"host": "console.example", "sec-fetch-site": "cross-site"})))

    def test_sec_fetch_site_same_site_with_matching_origin_allowed(self):
        self.assertFalse(middleware._csrf_origin_denied(
            self._scope({"origin": "https://console.example", "host": "console.example",
                         "sec-fetch-site": "same-origin"})))

    def test_mismatched_origin_still_denied(self):
        self.assertTrue(middleware._csrf_origin_denied(
            self._scope({"origin": "https://evil.example", "host": "console.example"})))

    def test_no_browser_headers_allowed(self):
        # curl/SDK：无 Origin / Sec-Fetch-Site，放行。
        self.assertFalse(middleware._csrf_origin_denied(self._scope({"host": "console.example"})))


class ImageTtlTests(unittest.TestCase):
    """SEC-P3-5：生图 URL TTL 默认 24 小时，60s–7d 钳位。"""

    @staticmethod
    def _ttl():
        import image_delivery
        return image_delivery.TTL_SECONDS

    def test_default_is_24_hours(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("IMAGE_URL_TTL_SECONDS", None)
            self.assertEqual(self._ttl(), 24 * 3600)

    def test_clamping(self):
        from image_delivery import _resolve_ttl_seconds
        cases = {
            "604800": 604800,      # 7 天上限内
            "60": 60,              # 下限
            "1": 60,               # 钳到下限
            "999999999": 604800,   # 钳到上限
            "0": 60,
            "-5": 60,
            "abc": 24 * 3600,      # 畸形回默认
            "": 24 * 3600,
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                with mock.patch.dict(os.environ, {"IMAGE_URL_TTL_SECONDS": raw}):
                    self.assertEqual(_resolve_ttl_seconds(), expected)


class KeepalivePrivateGateTests(unittest.TestCase):
    """SEC-P2-6：私网保活目标需显式确认（校验层 + 服务层）。"""

    def test_settings_validation_rejects_private_without_flag(self):
        with mock.patch.object(
                __import__("settings_validation"), "keepalive_private_targets_allowed",
                lambda override=None: False):
            result = validate_global_settings_patch({"keepalive_url": "http://10.0.0.5:9090/ping"})
        self.assertIsNotNone(result)
        self.assertIn("keepalive_allow_private", result["error"])

    def test_settings_validation_accepts_private_with_flag(self):
        # 基础设置已开：直接放行。
        with mock.patch.object(
                __import__("settings_validation"), "keepalive_private_targets_allowed",
                lambda override=None: True):
            self.assertIsNone(
                validate_global_settings_patch({"keepalive_url": "http://10.0.0.5:9090/ping"}))
        # 同一补丁里先开开关：override 路径放行。
        with mock.patch.object(
                __import__("settings_validation"), "keepalive_private_targets_allowed",
                lambda override=None: bool(override)):
            self.assertIsNone(validate_global_settings_patch(
                {"keepalive_url": "http://10.0.0.5:9090/ping",
                 "keepalive_allow_private": True}))

    def test_public_url_still_valid_without_flag(self):
        with mock.patch.object(
                __import__("settings_validation"), "keepalive_private_targets_allowed",
                lambda override=None: False):
            self.assertIsNone(
                validate_global_settings_patch({"keepalive_url": "https://example.com/healthz"}))


class ProxyTestPrivateGateTests(unittest.TestCase):
    """SEC-P3-6：内网代理地址默认拒绝；端点可整体禁用。"""

    def test_literal_ip_classification(self):
        is_private = system_api._proxy_host_is_private
        for private in ("socks5://127.0.0.1:1080", "http://10.1.2.3:8080",
                        "http://192.168.1.10:8080", "socks5://user:pass@192.168.0.1:1080",
                        "socks5://[::1]:1080", "http://169.254.1.1:8080"):
            with self.subTest(target=private):
                self.assertTrue(is_private(private))
        for public in ("socks5://8.8.8.8:1080", "http://proxy.example.com:1080", ""):
            with self.subTest(target=public):
                self.assertFalse(is_private(public))

    def _authorized_client(self):
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        override = mock.patch.dict(
            main.app.dependency_overrides, {console_auth.require_auth: lambda: True})
        override.start()
        self.addCleanup(lambda: main.app.dependency_overrides.pop(console_auth.require_auth, None))
        return self.client

    def test_private_proxy_denied_without_flag(self):
        client = self._authorized_client()
        with mock.patch.object(system_api, "app_state") as stub:
            stub.get_setting.return_value = False
            response = client.post("/api/proxy/test", json={"proxy_url": "socks5://10.0.0.5:1080"})
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["code"], "private_address_blocked")
        self.assertIn("proxy_test_allow_private", payload["message"])

    def test_private_proxy_passes_gate_with_flag(self):
        client = self._authorized_client()
        # 通过闸门后应推进到协议转换阶段：mock 转换抛错，证明不再被内网闸门拦截
        # （也证明没有发出任何真实网络请求）。
        import proxy_resolver
        with mock.patch.object(system_api, "app_state") as stub, \
                mock.patch.object(proxy_resolver, "resolve_proxy_uri_async",
                                  side_effect=proxy_resolver.ProxyResolveError("stubbed")):
            stub.get_setting.return_value = True
            response = client.post("/api/proxy/test", json={"proxy_url": "socks5://10.0.0.5:1080"})
        self.assertEqual(response.json()["code"], "conversion")

    def test_endpoint_can_be_disabled(self):
        client = self._authorized_client()
        with mock.patch.object(system_api, "_PROXY_TEST_DISABLED", True):
            response = client.post("/api/proxy/test", json={})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["code"], "disabled")


class FingerprintWeakWindowTests(unittest.TestCase):
    """PRF-P2-5：超长串折叠只取首尾窗口，稳定性与区分度。"""

    @staticmethod
    def _fingerprint(text):
        from routes.chat_api import _request_fingerprint
        from models import OpenAIRequest
        return _request_fingerprint(OpenAIRequest(model="m", messages=[
            {"role": "user", "content": text}]))

    def test_weak_window_stable_for_identical_requests(self):
        big = "QUJD" * 30_000  # 120KB > 64KB 窗口
        self.assertEqual(self._fingerprint(big), self._fingerprint(big))

    def test_weak_window_distinguishes_different_ends(self):
        a = "A" * 100_000
        b = "B" * 100_000
        self.assertNotEqual(self._fingerprint(a), self._fingerprint(b))

    def test_weak_window_documented_collision_semantics(self):
        # 同首尾同长度、仅中段不同：折叠后同指纹（文档化的取舍——
        # 指纹是去重键而非安全边界，语义等同「重复请求」）。
        head, tail = "A" * 4096, "B" * 4096
        first = head + "C" * 70_000 + tail   # 总长 > 64KB，进入窗口折叠
        second = head + "D" * 70_000 + tail
        self.assertGreater(len(first), 64 * 1024)
        self.assertEqual(self._fingerprint(first), self._fingerprint(second))

    def test_below_window_still_full_digest(self):
        # 64KB 以下仍是全量摘要：中段一个字符的差异即可区分。
        head, tail = "A" * 30_000, "B" * 30_000
        first = head + "X" + tail
        second = head + "Y" + tail
        self.assertLessEqual(len(first), 64 * 1024)
        self.assertNotEqual(self._fingerprint(first), self._fingerprint(second))


if __name__ == "__main__":
    unittest.main()
