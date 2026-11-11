"""ENG-P3-7：express_fingerprint / proxy_tls 的直接契约测试。

审查覆盖率实测（2026-11）：express_fingerprint 62%（UA 档案拼接、
importlib 回退、标签补齐未命中）、proxy_tls 73%（三分支缺直接用例）。
本文件补齐两模块的全部分支，纳入统一检查的回归防线。
"""

import ssl
import sys
import unittest
from unittest import mock

import httpx

sys.path.insert(0, ".")

import express_fingerprint as ef
import proxy_tls
from runtime_state import app_state


class GenaiVersionFallbackTests(unittest.TestCase):
    """_genai_version 的三级回退：包属性 → importlib.metadata → 锁定版本。"""

    def test_package_attribute_wins(self):
        with mock.patch.object(ef, "_GENAI_VERSION", None), \
             mock.patch.object(ef._genai_pkg, "__version__", "9.9.9", create=True):
            self.assertEqual(ef._genai_version(), "9.9.9")

    def test_importlib_metadata_fallback(self):
        real_version = __import__("importlib.metadata", fromlist=["version"]).version
        with mock.patch.object(ef, "_GENAI_VERSION", None), \
             mock.patch.object(ef._genai_pkg, "__version__", None, create=True), \
             mock.patch("importlib.metadata.version", return_value="8.8.8"):
            self.assertEqual(ef._genai_version(), "8.8.8")
        _ = real_version  # 仅为显式声明导入意图

    def test_locked_version_last_resort(self):
        with mock.patch.object(ef, "_GENAI_VERSION", None), \
             mock.patch.object(ef._genai_pkg, "__version__", None, create=True), \
             mock.patch("importlib.metadata.version", side_effect=RuntimeError):
            self.assertEqual(ef._genai_version(), "2.31.0")

    def test_labels_format(self):
        # sdk_label 与 python_label 必须与 SDK 内部构造一字不差
        self.assertTrue(ef.sdk_label().startswith("google-genai-sdk/"))
        self.assertTrue(ef.python_label().startswith("gl-python/"))
        self.assertEqual(ef.python_label(), "gl-python/" + sys.version.split()[0])
        self.assertEqual(ef.node_label(), ef._NODE_LABEL)


class UaProfileTests(unittest.TestCase):
    """档案读取（热生效 + 非法值回退）与四档案的 UA 拼装。"""

    def _setting(self, values):
        return mock.patch.object(
            app_state, "get_setting",
            side_effect=lambda key, default=None: values.get(key, default))

    def test_invalid_profile_falls_back_to_sdk(self):
        with self._setting({"express_ua_profile": "bogus"}):
            self.assertEqual(ef.get_express_ua_profile(), "sdk")

    def test_profile_read_error_falls_back_to_sdk(self):
        with mock.patch.object(app_state, "get_setting", side_effect=RuntimeError):
            self.assertEqual(ef.get_express_ua_profile(), "sdk")
            self.assertEqual(ef.get_express_ua_custom(), "")

    def test_custom_read_error_returns_empty(self):
        with mock.patch.object(app_state, "get_setting", side_effect=RuntimeError):
            # get_express_ua_custom 自带异常兜底（空串），不向上抛
            self.assertEqual(ef.get_express_ua_custom(), "")

    def test_sdk_profile_is_official_shape(self):
        with self._setting({"express_ua_profile": "sdk"}):
            ua = ef._build_ua("sdk")
            self.assertEqual(ua, f"{ef.sdk_label()} {ef.python_label()}")

    def test_node_profile(self):
        ua = ef._build_ua("node")
        self.assertEqual(ua, f"{ef.sdk_label()} {ef.node_label()}")
        self.assertNotIn("gl-python", ua)

    def test_gcloud_profile(self):
        ua = ef._build_ua("gcloud")
        self.assertIn(ef.sdk_label(), ua)
        self.assertIn(ef.python_label(), ua)
        self.assertIn(ef._GCLOUD_LABEL, ua)
        self.assertIn(ef._GCLOUD_COMMAND, ua)
        self.assertIn(ef._GCCL_LABEL, ua)

    def test_custom_empty_falls_back_to_official(self):
        with self._setting({"express_ua_custom": ""}):
            self.assertEqual(ef._build_ua("custom"), f"{ef.sdk_label()} {ef.python_label()}")

    def test_custom_token_replacement(self):
        with self._setting({"express_ua_custom": "{node} myclient/1.0"}):
            ua = ef._build_ua("custom")
            # {node} 替换后，SDK 标签缺失 → 整体前置（对齐 append_library_version_headers）
            self.assertTrue(ua.startswith(ef.sdk_label() + " " + ef.python_label()))
            self.assertIn(ef.node_label(), ua)
            self.assertIn("myclient/1.0", ua)

    def test_custom_already_labeled_passthrough(self):
        raw = f"{ef.sdk_label()} {ef.python_label()} edge/2.0"
        with self._setting({"express_ua_custom": raw}):
            self.assertEqual(ef._build_ua("custom"), raw)


class EnsureSdkLabelsTests(unittest.TestCase):
    """标签补齐的三分支（真实 SDK 的输出形态约束）。"""

    def test_missing_both_labels_prefixes_pair(self):
        out = ef._ensure_sdk_labels("curl/8.0")
        self.assertEqual(out, f"{ef.sdk_label()} {ef.python_label()} curl/8.0")

    def test_sdk_present_appends_python(self):
        out = ef._ensure_sdk_labels(f"{ef.sdk_label()} curl/8.0")
        self.assertEqual(out, f"{ef.sdk_label()} curl/8.0 {ef.python_label()}")

    def test_both_present_unchanged(self):
        raw = f"prefix {ef.sdk_label()} mid {ef.python_label()} tail"
        self.assertEqual(ef._ensure_sdk_labels(raw), raw)


class ChannelOverridesTests(unittest.TestCase):
    """SDK 通道默认不覆盖身份；custom/gcloud 才注入双头。"""

    def _setting(self, values):
        return mock.patch.object(
            app_state, "get_setting",
            side_effect=lambda key, default=None: values.get(key, default))

    def test_sdk_and_node_leave_sdk_identity_alone(self):
        for profile in ("sdk", "node"):
            with self._setting({"express_ua_profile": profile}):
                self.assertEqual(ef.build_sdk_channel_overrides(), {})

    def test_custom_profile_overrides_both_headers(self):
        with self._setting({"express_ua_profile": "custom",
                            "express_ua_custom": "myclient/1.0"}):
            overrides = ef.build_sdk_channel_overrides()
            ua = f"{ef.sdk_label()} {ef.python_label()} myclient/1.0"
            self.assertEqual(overrides, {"user-agent": ua, "x-goog-api-client": ua})

    def test_direct_headers_shape(self):
        with self._setting({"express_ua_profile": "sdk"}):
            headers = ef.build_direct_headers("k-1")
            self.assertEqual(headers["x-goog-api-key"], "k-1")
            self.assertEqual(headers["Content-Type"], "application/json")
            self.assertEqual(headers["user-agent"], headers["x-goog-api-client"])
            self.assertIn("google-genai-sdk/", headers["user-agent"])
            # 不带 key 时省略（query 参数鉴权形态）
            self.assertNotIn("x-goog-api-key", ef.build_direct_headers())

    def test_describe_current_profile_branches(self):
        with self._setting({"express_ua_profile": "sdk"}):
            self.assertIn("官方", ef.describe_current_profile())
        with self._setting({"express_ua_profile": "node"}):
            self.assertIn("Node", ef.describe_current_profile())
        with self._setting({"express_ua_profile": "custom", "express_ua_custom": ""}):
            self.assertIn("官方形态", ef.describe_current_profile())
        with self._setting({"express_ua_profile": "gcloud"}):
            self.assertIn("gcloud", ef.describe_current_profile())


class HttpsProxyTrustTests(unittest.TestCase):
    """build_httpx_proxy：HTTPS 代理的自定义信任（六分支契约）。"""

    def test_plain_http_url_returned_unchanged(self):
        self.assertEqual(proxy_tls.build_httpx_proxy("http://127.0.0.1:7890"), "http://127.0.0.1:7890")

    def test_https_with_default_verify_unchanged(self):
        url = "https://proxy.example:443"
        self.assertEqual(proxy_tls.build_httpx_proxy(url, verify=True), url)
        self.assertEqual(proxy_tls.build_httpx_proxy(url, verify=None), url)

    def test_https_with_sslcontext_reuses_it(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        result = proxy_tls.build_httpx_proxy("https://p:443", verify=ctx)
        self.assertIsInstance(result, httpx.Proxy)
        self.assertIs(result.ssl_context, ctx)

    def test_https_verify_false_disables_checks(self):
        result = proxy_tls.build_httpx_proxy("https://p:443", verify=False)
        self.assertIsInstance(result, httpx.Proxy)
        self.assertEqual(result.ssl_context.verify_mode, ssl.CERT_NONE)
        self.assertFalse(result.ssl_context.check_hostname)

    def test_https_capath_when_directory(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with mock.patch.object(ssl, "create_default_context", return_value=ctx) as factory, \
             mock.patch.object(proxy_tls.os.path, "isdir", return_value=True):
            result = proxy_tls.build_httpx_proxy("https://p:443", verify="/etc/ssl/certs")
            self.assertIs(result.ssl_context, ctx)
            factory.assert_called_once_with(capath="/etc/ssl/certs")

    def test_https_cafile_when_file(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with mock.patch.object(ssl, "create_default_context", return_value=ctx) as factory, \
             mock.patch.object(proxy_tls.os.path, "isdir", return_value=False):
            result = proxy_tls.build_httpx_proxy("https://p:443", verify="/etc/ssl/ca.pem")
            self.assertIs(result.ssl_context, ctx)
            factory.assert_called_once_with(cafile="/etc/ssl/ca.pem")


if __name__ == "__main__":
    unittest.main(verbosity=2)


class SingboxDiscoveryTests(unittest.TestCase):
    """ENG-P3-7：proxy_resolver 二进制发现 / 版本缓存 / 收割线程幂等。

    进程管理内部（_launch / reaper 循环）需重度 mock，按审查记录保持现状；
    这里覆盖可直接断言的纯路径与缓存契约。
    """

    def setUp(self):
        import proxy_resolver as pr
        self.pr = pr
        # 每个用例重置进程内缓存，避免用例间顺序耦合
        self._saved = (pr._singbox_bin_cache, pr._SINGBOX_VERSION_CACHE)
        pr._singbox_bin_cache = None
        pr._SINGBOX_VERSION_CACHE = None
        self.addCleanup(self._restore)

    def _restore(self):
        self.pr._singbox_bin_cache, self.pr._SINGBOX_VERSION_CACHE = self._saved

    def test_env_path_candidate_wins_when_executable(self):
        with mock.patch.dict(self.pr.os.environ, {"SINGBOX_BIN": "/opt/sing-box"}), \
             mock.patch.object(self.pr.os.path, "isfile", return_value=True), \
             mock.patch.object(self.pr.os.path, "isdir", return_value=False), \
             mock.patch.object(self.pr.os, "access", return_value=True):
            self.assertEqual(self.pr.find_singbox_binary(), "/opt/sing-box")
        # 探测结果进缓存：再次调用不再触碰文件系统
        with mock.patch.object(self.pr.os.path, "isfile", side_effect=AssertionError):
            self.assertEqual(self.pr.find_singbox_binary(), "/opt/sing-box")

    def test_missing_binary_cached_as_empty(self):
        with mock.patch.dict(self.pr.os.environ, {}, clear=False), \
             mock.patch.object(self.pr.shutil, "which", return_value=None), \
             mock.patch.object(self.pr.os.path, "isfile", return_value=False):
            self.assertIsNone(self.pr.find_singbox_binary())
        # "" 缓存 = 探测过但缺失：重置 isfile 也不会再发现
        with mock.patch.object(self.pr.os.path, "isfile", return_value=True):
            self.assertIsNone(self.pr.find_singbox_binary())

    def test_non_executable_candidate_rejected(self):
        with mock.patch.dict(self.pr.os.environ, {"SINGBOX_BIN": "/opt/noexec"}), \
             mock.patch.object(self.pr.os.path, "isfile", return_value=True), \
             mock.patch.object(self.pr.shutil, "which", return_value=None), \
             mock.patch.object(self.pr.os, "access", return_value=False):
            self.assertIsNone(self.pr.find_singbox_binary())

    def test_version_cache_and_failure_paths(self):
        with mock.patch.object(self.pr, "find_singbox_binary", return_value=None):
            self.assertEqual(self.pr.singbox_version(), "")
            self.assertEqual(self.pr._SINGBOX_VERSION_CACHE, "")
        # 缓存命中：不再探测二进制
        with mock.patch.object(self.pr, "find_singbox_binary", side_effect=AssertionError):
            self.assertEqual(self.pr.singbox_version(), "")

    def test_version_subprocess_output_first_line(self):
        completed = mock.Mock(stdout="sing-box version 1.11.0\n\n", stderr="")
        self.pr._SINGBOX_VERSION_CACHE = None
        with mock.patch.object(self.pr, "find_singbox_binary", return_value="/usr/bin/sing-box"), \
             mock.patch.object(self.pr.subprocess, "run", return_value=completed) as run:
            self.assertEqual(self.pr.singbox_version(), "sing-box version 1.11.0")
            run.assert_called_once()
        self.assertEqual(self.pr._SINGBOX_VERSION_CACHE, "sing-box version 1.11.0")

    def test_version_subprocess_timeout_falls_back_to_empty(self):
        self.pr._SINGBOX_VERSION_CACHE = None
        with mock.patch.object(self.pr, "find_singbox_binary", return_value="/usr/bin/sing-box"), \
             mock.patch.object(self.pr.subprocess, "run", side_effect=self.pr.subprocess.TimeoutExpired("x", 1)):
            self.assertEqual(self.pr.singbox_version(), "")

    def test_ensure_reaper_is_idempotent(self):
        started = []
        with mock.patch.object(self.pr.threading, "Thread", side_effect=lambda *a, **k: started.append(k) or mock.Mock()):
            self.pr.ensure_reaper()
            first = len(started)
            self.pr.ensure_reaper()
            self.assertEqual(len(started), first)
        self.pr._reaper_started = False  # 还原全局开关，不泄漏给其它用例
