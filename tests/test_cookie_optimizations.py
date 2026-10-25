"""Cookie 通道优化项的回归测试：

- gRPC 状态码 → HTTP 映射优先于文本关键词
- Cookie 失效关键词收窄（安全拒绝类文案不再误报）
- querySignature / clientVersion / authuser 控制台热覆盖
- 多 Cookie 账号健康隔离（连续失败冷却、成功清零、全冷却回退）
- usage 字符级粗估（CJK 计法）与开关行为
- 重试指数退避模式
"""

import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))
os.chdir(APP_DIR)

from cookie_auth import (  # noqa: E402
    DEFAULT_CLIENT_VERSION,
    STREAM_GENERATE_QUERY_SIGNATURE,
    get_authuser,
    get_client_version,
    get_stream_query_signature,
)


class GrpcStatusMappingTests(unittest.TestCase):
    def test_grpc_resource_exhausted_maps_to_429(self):
        from upstreams.cookie_transport import api_error_details
        data = {"message": "Resource exhausted.",
                "extensions": {"status": {"code": 8, "message": "Resource exhausted."}}}
        code, message = api_error_details(data)
        self.assertEqual(code, 429)
        self.assertEqual(message, "Resource exhausted.")

    def test_grpc_permission_denied_maps_to_403(self):
        from upstreams.cookie_transport import api_error_details
        data = {"message": "Permission denied.",
                "extensions": {"status": {"code": 7}}}
        code, _ = api_error_details(data)
        self.assertEqual(code, 403)

    def test_grpc_unauthenticated_maps_to_401(self):
        from upstreams.cookie_transport import api_error_details
        code, _ = api_error_details({"message": "x", "extensions": {"status": {"code": 16}}})
        self.assertEqual(code, 401)

    def test_top_level_code_wins_over_grpc(self):
        from upstreams.cookie_transport import api_error_details
        code, _ = api_error_details({"message": "x", "code": 503, "extensions": {"status": {"code": 8}}})
        self.assertEqual(code, 503)

    def test_text_fallback_when_no_structured_code(self):
        from upstreams.cookie_transport import api_error_details
        code, _ = api_error_details({"message": "Too many requests, please retry."})
        self.assertEqual(code, 429)
        code, _ = api_error_details({"message": "something odd"})
        self.assertEqual(code, 502)


class CookieKeywordNarrowingTests(unittest.TestCase):
    def test_safety_denial_text_not_cookie_expired(self):
        from upstreams.cookie_transport import is_cookie_expired_error
        self.assertFalse(is_cookie_expired_error("Request denied by safety system"))
        self.assertFalse(is_cookie_expired_error("permission scope too broad"))

    def test_real_auth_failures_still_detected(self):
        from upstreams.cookie_transport import is_cookie_expired_error
        self.assertTrue(is_cookie_expired_error("Permission denied on resource"))
        self.assertTrue(is_cookie_expired_error("Login Required"))
        self.assertTrue(is_cookie_expired_error("session expired"))
        self.assertTrue(is_cookie_expired_error("Request had invalid credentials"))


class HotConfigTests(unittest.TestCase):
    def test_signature_defaults(self):
        with mock.patch("runtime_state.app_state.get_setting", return_value=""):
            self.assertEqual(get_stream_query_signature(), STREAM_GENERATE_QUERY_SIGNATURE)
            self.assertEqual(get_client_version(), DEFAULT_CLIENT_VERSION)
            self.assertEqual(get_authuser(), "0")

    def test_signature_override_wins(self):
        with mock.patch("runtime_state.app_state.get_setting", return_value="NEW/SIGNATURE=="):
            self.assertEqual(get_stream_query_signature(), "NEW/SIGNATURE==")
            self.assertEqual(get_client_version(), "NEW/SIGNATURE==")
            self.assertEqual(get_authuser(), "NEW/SIGNATURE==")

    def test_probe_body_uses_hot_signature(self):
        from upstreams.cookie_request import build_probe_body
        with mock.patch("runtime_state.app_state.get_setting", return_value="sig-override"):
            body = build_probe_body("proj-x")
        self.assertEqual(body["querySignature"], "sig-override")
        model = body["variables"]["model"]
        self.assertIn("proj-x", model)
        self.assertIn("__cookie_health_probe__", model)


class AccountHealthIsolationTests(unittest.TestCase):
    def _manager_with_accounts(self, *accounts):
        import credential_manager
        records = [{"id": f"a{i}", "cookie": f"c{i}", "project_id": "p", "enabled": True,
                    "note": f"acc{i}"} for i in accounts]
        mgr = credential_manager.CredentialManager()
        return mgr, mock.patch.object(credential_manager.app_state, "get_cookie_accounts",
                                      return_value=records)

    def test_failures_below_threshold_keep_account_selected(self):
        mgr, patcher = self._manager_with_accounts(0)
        with patcher:
            mgr.record_cookie_auth_failure("a0")
            mgr.record_cookie_auth_failure("a0")
            record = mgr.get_cookie_record()
        self.assertEqual(record["id"], "a0")

    def test_threshold_puts_account_into_cooldown(self):
        import credential_manager
        mgr, patcher = self._manager_with_accounts(0, 1)
        with patcher:
            for _ in range(credential_manager.COOKIE_AUTH_FAILURE_THRESHOLD):
                mgr.record_cookie_auth_failure("a0")
            # a0 冷却中：唯一健康账号是 a1；随机选择也应落在 a1
            for _ in range(10):
                record = mgr.get_cookie_record()
                self.assertEqual(record["id"], "a1")

    def test_success_resets_failures(self):
        import credential_manager
        mgr, patcher = self._manager_with_accounts(0)
        with patcher:
            for _ in range(credential_manager.COOKIE_AUTH_FAILURE_THRESHOLD - 1):
                mgr.record_cookie_auth_failure("a0")
            mgr.record_cookie_success("a0")
            for _ in range(credential_manager.COOKIE_AUTH_FAILURE_THRESHOLD - 1):
                mgr.record_cookie_auth_failure("a0")
            # 成功清零后，未达到阈值连续失败，账号仍可选
            self.assertEqual(mgr.get_cookie_record()["id"], "a0")

    def test_all_cooldown_falls_back_to_full_pool(self):
        import credential_manager
        mgr, patcher = self._manager_with_accounts(0)
        with patcher:
            for _ in range(credential_manager.COOKIE_AUTH_FAILURE_THRESHOLD):
                mgr.record_cookie_auth_failure("a0")
            record = mgr.get_cookie_record()
        self.assertEqual(record["id"], "a0")  # 全冷却回退：宁可再试也不拒服务

    def test_cooldown_expires(self):
        import credential_manager
        mgr, patcher = self._manager_with_accounts(0, 1)
        with patcher:
            for _ in range(credential_manager.COOKIE_AUTH_FAILURE_THRESHOLD):
                mgr.record_cookie_auth_failure("a0")
            with mock.patch("credential_manager.time.time",
                            return_value=__import__("time").time() + credential_manager.COOKIE_COOLDOWN_SECONDS + 1):
                ids = {mgr.get_cookie_record()["id"] for _ in range(20)}
            self.assertIn("a0", ids)  # 冷却到期后 a0 重新参与

    def test_snapshot_reports_cooldown(self):
        import credential_manager
        mgr, _ = self._manager_with_accounts(0)
        for _ in range(credential_manager.COOKIE_AUTH_FAILURE_THRESHOLD):
            mgr.record_cookie_auth_failure("a0")
        snap = mgr.cookie_health_snapshot()
        self.assertGreater(snap["a0"]["cooldown_remaining"], 0)


class UsageEstimationTests(unittest.TestCase):
    def test_cjk_and_ascii_mix(self):
        from usage import estimate_token_count
        self.assertEqual(estimate_token_count(""), 0)
        self.assertEqual(estimate_token_count("你好世界"), 4)            # 4 CJK = 4
        self.assertEqual(estimate_token_count("abcdefgh"), 2)            # 8 ASCII / 4
        mixed = estimate_token_count("你好abcd")                          # 2 CJK + 4 ASCII
        self.assertEqual(mixed, 2 + 1)

    def test_estimate_flag_and_off_by_default(self):
        from usage import estimate_usage
        est = estimate_usage("hello world", "你好")
        self.assertTrue(est["estimated"])
        self.assertEqual(est["prompt_tokens"], 3)
        self.assertEqual(est["completion_tokens"], 2)
        self.assertEqual(est["total_tokens"], 5)

    def test_final_usage_respects_switch(self):
        from upstreams.cookie_proxy import _final_usage
        request = SimpleNamespace(messages=[SimpleNamespace(content="你好", role="user")])
        with mock.patch("upstreams.cookie_proxy.app_state.get_setting", return_value=False):
            usage = _final_usage(None, request, "回复")
        self.assertEqual(usage, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
        with mock.patch("upstreams.cookie_proxy.app_state.get_setting", return_value=True):
            usage = _final_usage(None, request, "回复")
        self.assertTrue(usage["estimated"])
        self.assertGreater(usage["total_tokens"], 0)

    def test_real_metadata_never_estimated(self):
        from upstreams.cookie_proxy import _final_usage
        meta = {"promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15}
        request = SimpleNamespace(messages=[])
        with mock.patch("upstreams.cookie_proxy.app_state.get_setting", return_value=True):
            usage = _final_usage(meta, request, "文本")
        self.assertNotIn("estimated", usage)
        self.assertEqual(usage["total_tokens"], 15)


class RetryBackoffModeTests(unittest.TestCase):
    def test_fixed_mode_ignores_attempt(self):
        from retry_policy import _retry_delay_value
        with mock.patch("retry_policy.app_state.get_setting", return_value="fixed"):
            self.assertAlmostEqual(_retry_delay_value(None, 5.0, 0, 0.0), 5.0)
            self.assertAlmostEqual(_retry_delay_value(None, 5.0, 4, 0.0), 5.0)

    def test_exponential_mode_grows_and_caps(self):
        from retry_policy import _retry_delay_value
        with mock.patch("retry_policy.app_state.get_setting", return_value="exponential"):
            self.assertAlmostEqual(_retry_delay_value(None, 5.0, 0, 0.0), 5.0)
            self.assertAlmostEqual(_retry_delay_value(None, 5.0, 1, 0.0), 10.0)
            self.assertAlmostEqual(_retry_delay_value(None, 5.0, 2, 0.0), 20.0)
            self.assertAlmostEqual(_retry_delay_value(None, 5.0, 30, 0.0), 120.0)  # 封顶


if __name__ == "__main__":
    unittest.main()
