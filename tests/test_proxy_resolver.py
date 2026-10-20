"""出站代理解析层回归：多协议分享链接解析 / 校验 / 脱敏 / Key 级优先级。

全部离线：不启动 sing-box、不发包（进程管理走 e2e 脚本 verify_singbox_e2e.py）。
覆盖第十六轮改动：vless/vmess/trojan/ss/socks4/hysteria2/tuic 解析、
direct 哨兵语义、sing-box 缺失时明确报错不直连、Key 级代理优先于全局。
"""
import base64
import json
import os
import time
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

_STATE_DIR = tempfile.TemporaryDirectory(prefix="v2o-proxy-resolver-")
os.environ["STATE_DIR"] = _STATE_DIR.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import proxy_resolver as pr
from proxy_resolver import (
    classify_proxy_uri,
    mask_proxy_uri,
    parse_share_uri_to_outbound,
    resolve_proxy_uri,
    validate_proxy_uri,
    ProxyResolveError,
)
from runtime_state import app_state, get_effective_proxy_uri_for_key

# 结构取自真实 vless 节点的脱敏样本（NL，ws + tls + chrome 指纹）
REAL_VLESS = ("vless://12345678-90ab-4cde-8f90-1234567890ab@203.0.113.10:2096"
              "?path=%2F&security=tls&encryption=none&host=nl-node.example.com&fp=chrome"
              "&type=ws&sni=nl-node.example.com#NL")


def _vmess_uri(**over) -> str:
    payload = {"v": "2", "ps": "vm", "add": "vm.example.com", "port": "443",
               "id": "11111111-2222-3333-4444-555555555555", "aid": "0",
               "scy": "auto", "net": "ws", "type": "none",
               "host": "cdn.example.com", "path": "/ws", "tls": "tls",
               "sni": "vm.example.com", "fp": "chrome"}
    payload.update(over)
    return "vmess://" + base64.b64encode(json.dumps(payload).encode()).decode()


class ParseTests(unittest.TestCase):
    def test_vless_real_link(self):
        ob = parse_share_uri_to_outbound(REAL_VLESS)
        self.assertEqual(ob["type"], "vless")
        self.assertEqual(ob["server"], "203.0.113.10")
        self.assertEqual(ob["server_port"], 2096)
        self.assertEqual(ob["uuid"], "12345678-90ab-4cde-8f90-1234567890ab")
        self.assertEqual(ob["tls"]["server_name"], "nl-node.example.com")
        self.assertTrue(ob["tls"]["utls"]["enabled"])
        self.assertEqual(ob["tls"]["utls"]["fingerprint"], "chrome")
        self.assertEqual(ob["transport"]["type"], "ws")
        self.assertEqual(ob["transport"]["path"], "/")
        self.assertEqual(ob["transport"]["headers"]["Host"], "nl-node.example.com")

    def test_vless_reality_grpc(self):
        uri = ("vless://uuid-123@re.example.com:443?security=reality&pbk=PUBKEY&sid=abcd"
               "&sni=re.example.com&fp=safari&type=grpc&serviceName=grpc-svc&flow=xtls-rprx-vision#R")
        ob = parse_share_uri_to_outbound(uri)
        self.assertEqual(ob["flow"], "xtls-rprx-vision")
        self.assertEqual(ob["tls"]["reality"]["public_key"], "PUBKEY")
        self.assertEqual(ob["tls"]["reality"]["short_id"], "abcd")
        self.assertEqual(ob["transport"]["type"], "grpc")
        self.assertEqual(ob["transport"]["service_name"], "grpc-svc")

    def test_vless_tcp_no_tls(self):
        ob = parse_share_uri_to_outbound("vless://uuid-123@1.2.3.4:80?security=none&type=tcp#x")
        self.assertNotIn("tls", ob)
        self.assertNotIn("transport", ob)

    def test_vmess(self):
        ob = parse_share_uri_to_outbound(_vmess_uri())
        self.assertEqual(ob["type"], "vmess")
        self.assertEqual(ob["server"], "vm.example.com")
        self.assertEqual(ob["server_port"], 443)
        self.assertEqual(ob["uuid"], "11111111-2222-3333-4444-555555555555")
        self.assertTrue(ob["tls"]["enabled"])
        self.assertEqual(ob["transport"]["type"], "ws")
        self.assertEqual(ob["transport"]["headers"]["Host"], "cdn.example.com")
        self.assertEqual(ob["security"], "auto")
        self.assertEqual(ob["alter_id"], 0)

    def test_vmess_grpc_no_padding(self):
        raw = base64.b64encode(json.dumps(
            {"add": "g.io", "port": "8443", "id": "u1", "net": "grpc",
             "path": "svc", "tls": ""}).encode()).decode().rstrip("=")
        ob = parse_share_uri_to_outbound("vmess://" + raw)
        self.assertEqual(ob["server"], "g.io")
        self.assertEqual(ob["server_port"], 8443)
        self.assertEqual(ob["transport"]["type"], "grpc")
        self.assertEqual(ob["transport"]["service_name"], "svc")
        self.assertNotIn("tls", ob)

    def test_trojan(self):
        uri = ("trojan://pass%40word@t.example.com:443?sni=t.example.com&type=ws"
               "&path=%2Ftrojan&fp=chrome&alpn=h2,http/1.1#T")
        ob = parse_share_uri_to_outbound(uri)
        self.assertEqual(ob["type"], "trojan")
        self.assertEqual(ob["password"], "pass@word")
        self.assertTrue(ob["tls"]["enabled"])          # trojan 强制 TLS
        self.assertEqual(ob["tls"]["alpn"], ["h2", "http/1.1"])
        self.assertEqual(ob["transport"]["type"], "ws")
        self.assertEqual(ob["transport"]["path"], "/trojan")

    def test_ss_sip002_b64_userinfo(self):
        userinfo = base64.urlsafe_b64encode(b"aes-256-gcm:pass123").decode().rstrip("=")
        ob = parse_share_uri_to_outbound(f"ss://{userinfo}@5.6.7.8:8388#S1")
        self.assertEqual(ob["type"], "shadowsocks")
        self.assertEqual(ob["method"], "aes-256-gcm")
        self.assertEqual(ob["password"], "pass123")
        self.assertEqual(ob["server"], "5.6.7.8")
        self.assertEqual(ob["server_port"], 8388)

    def test_ss_sip002_plain_userinfo(self):
        # （带 plugin 的链接见 test_ss_plugin_rejected_explicitly：现为明确拒绝）
        ob = parse_share_uri_to_outbound("ss://aes-128-gcm:plainpass@5.6.7.8:8388/?#S2")
        self.assertEqual(ob["method"], "aes-128-gcm")
        self.assertEqual(ob["password"], "plainpass")

    def test_ss_legacy_whole_b64(self):
        inner = base64.b64encode(b"chacha20-ietf-poly1305:pw@9.9.9.9:9999").decode()
        ob = parse_share_uri_to_outbound(f"ss://{inner}#S3")
        self.assertEqual(ob["method"], "chacha20-ietf-poly1305")
        self.assertEqual(ob["password"], "pw")
        self.assertEqual(ob["server"], "9.9.9.9")
        self.assertEqual(ob["server_port"], 9999)

    def test_socks4(self):
        ob = parse_share_uri_to_outbound("socks4://user@10.0.0.1:1080")
        self.assertEqual(ob["type"], "socks")
        self.assertEqual(ob["version"], "4")
        self.assertEqual(ob["username"], "user")
        ob4a = parse_share_uri_to_outbound("socks4a://10.0.0.2:1080")
        self.assertEqual(ob4a["version"], "4a")
        self.assertNotIn("username", ob4a)

    def test_hysteria2(self):
        uri = ("hysteria2://authpass@h.example.com:443?sni=h.example.com&insecure=1"
               "&obfs=salamander&obfs-password=obfspw#H")
        ob = parse_share_uri_to_outbound(uri)
        self.assertEqual(ob["type"], "hysteria2")
        self.assertEqual(ob["password"], "authpass")
        self.assertTrue(ob["tls"]["insecure"])
        self.assertEqual(ob["obfs"]["type"], "salamander")
        self.assertEqual(ob["obfs"]["password"], "obfspw")

    def test_tuic(self):
        uri = "tuic://uuid-tuic:pwd@t.example.com:8443?sni=t.example.com&congestion_control=cubic&alpn=h3#U"
        ob = parse_share_uri_to_outbound(uri)
        self.assertEqual(ob["type"], "tuic")
        self.assertEqual(ob["uuid"], "uuid-tuic")
        self.assertEqual(ob["password"], "pwd")
        self.assertEqual(ob["congestion_control"], "cubic")
        self.assertEqual(ob["tls"]["alpn"], ["h3"])

    def test_invalid(self):
        for bad in ("vless://@host:1", "vless://uuid@host", "vmess://!!!not-base64!!!",
                    "trojan://@host:443", "ss://onlyb64", "socks4://host:0",
                    "hysteria2://@h:443", "tuic://:pwd@h:443", "ftp://host:21"):
            with self.assertRaises(ProxyResolveError, msg=bad):
                parse_share_uri_to_outbound(bad)

    # ---------- 第十七轮修复的回归用例 ----------

    def test_vless_reality_without_fp_gets_default(self):
        # sing-box 要求 REALITY 必带 uTLS 指纹；链接漏带 fp 时补默认 chrome
        uri = "vless://uuid-1@r.example.com:443?security=reality&pbk=KEY&sni=r.example.com#R"
        ob = parse_share_uri_to_outbound(uri)
        self.assertEqual(ob["tls"]["utls"]["fingerprint"], "chrome")

    def test_vless_flow_stripped_without_tls(self):
        # flow 只在 TLS/REALITY 下有意义；裸 TCP 带上会被 sing-box 拒启
        ob = parse_share_uri_to_outbound(
            "vless://uuid-1@1.2.3.4:80?security=none&flow=xtls-rprx-vision&type=tcp#x")
        self.assertNotIn("flow", ob)

    def test_ss_plugin_rejected_explicitly(self):
        # SIP003 plugin 静默忽略会产出永远连不上的代理——必须明确报错
        with self.assertRaises(ProxyResolveError) as ctx:
            parse_share_uri_to_outbound("ss://aes-128-gcm:pw@1.2.3.4:8388?plugin=v2ray-plugin")
        self.assertIn("plugin", str(ctx.exception))

    def test_vmess_b64_tolerates_whitespace(self):
        # 从网页/聊天窗口复制的 vmess 链接常带折行与首尾空白
        raw = base64.b64encode(json.dumps(
            {"add": "a.com", "port": "443", "id": "u"}).encode()).decode()
        broken = "vmess:// " + raw[:15] + "\n" + raw[15:30] + " " + raw[30:]
        ob = parse_share_uri_to_outbound(broken)
        self.assertEqual(ob["server"], "a.com")

    def test_vmess_port_range(self):
        for bad_port in ("0", "65536", "-1"):
            with self.assertRaises(ProxyResolveError, msg=bad_port):
                parse_share_uri_to_outbound(_vmess_uri(port=bad_port))

    def test_ws_path_not_double_decoded(self):
        # parse_qs 已解码一次；再 unquote 会把字面量 "%2F" 错误地变成 "/"
        uri = "vless://uuid-1@1.2.3.4:443?security=tls&type=ws&path=%252Fws&sni=x.com"
        ob = parse_share_uri_to_outbound(uri)
        self.assertEqual(ob["transport"]["path"], "%2Fws")
        # 正常单次编码不受影响
        uri2 = "vless://uuid-1@1.2.3.4:443?security=tls&type=ws&path=%2Fws2&sni=x.com"
        self.assertEqual(parse_share_uri_to_outbound(uri2)["transport"]["path"], "/ws2")

    def test_ss_bad_port_is_actionable_error(self):
        # 端口非数字/越界必须报 ProxyResolveError（而不是裸 ValueError）
        with self.assertRaises(ProxyResolveError):
            parse_share_uri_to_outbound("ss://aes-128-gcm:pw@1.2.3.4:notaport")
        with self.assertRaises(ProxyResolveError):
            parse_share_uri_to_outbound("ss://aes-128-gcm:pw@1.2.3.4:99999")


class ClassifyValidateMaskTests(unittest.TestCase):
    def test_classify(self):
        self.assertEqual(classify_proxy_uri("socks5://1.1.1.1:1080"), "native")
        self.assertEqual(classify_proxy_uri("http://u:p@1.1.1.1:8080"), "native")
        self.assertEqual(classify_proxy_uri("socks5h://1.1.1.1:1080"), "native")
        self.assertEqual(classify_proxy_uri("https://1.1.1.1:443"), "native")
        self.assertEqual(classify_proxy_uri(REAL_VLESS), "singbox")
        self.assertEqual(classify_proxy_uri("vmess://e30="), "singbox")
        self.assertEqual(classify_proxy_uri("socks4://1.1.1.1:1080"), "singbox")
        self.assertEqual(classify_proxy_uri(""), "direct")
        self.assertEqual(classify_proxy_uri("direct"), "direct")
        self.assertEqual(classify_proxy_uri("DIRECT"), "direct")
        self.assertEqual(classify_proxy_uri("直连"), "direct")
        self.assertEqual(classify_proxy_uri("gost://x"), "invalid")

    def test_validate(self):
        self.assertEqual(validate_proxy_uri(""), "")
        self.assertEqual(validate_proxy_uri("direct"), "")
        self.assertEqual(validate_proxy_uri("socks5://u:p@1.2.3.4:1080"), "")
        self.assertEqual(validate_proxy_uri(REAL_VLESS), "")
        self.assertEqual(validate_proxy_uri("vmess://" + base64.b64encode(
            json.dumps({"add": "a.com", "port": "443", "id": "x"}).encode()).decode()), "")
        self.assertNotEqual(validate_proxy_uri("socks5://1.2.3.4:0"), "")
        self.assertNotEqual(validate_proxy_uri("vless://only-uuid"), "")
        self.assertNotEqual(validate_proxy_uri("res://1.2.3.4:1"), "")

    def test_mask_never_leaks_credentials(self):
        cases = {
            "socks5://user:pass@1.2.3.4:1080": "socks5://***@1.2.3.4:1080",
            "http://a:b@h.io:3128": "http://***@h.io:3128",
            REAL_VLESS: "vless://***@203.0.113.10:2096",
        }
        for uri, expect in cases.items():
            masked = mask_proxy_uri(uri)
            self.assertEqual(masked, expect, uri)
            for secret in ("12345678", "user:pass", "pass@"):
                self.assertNotIn(secret, masked)
        self.assertIn("vmess://***", mask_proxy_uri(_vmess_uri()))
        legacy_ss = base64.b64encode(b"chacha20-ietf-poly1305:private-password@9.9.9.9:9999").decode()
        masked_ss = mask_proxy_uri(f"ss://{legacy_ss}#private-label")
        self.assertIn("ss://***", masked_ss)
        self.assertNotIn(legacy_ss.lower(), masked_ss.lower())
        self.assertNotIn("private", masked_ss)
        self.assertEqual(mask_proxy_uri(""), "")
        self.assertIn("direct", mask_proxy_uri("direct"))


class ResolveTests(unittest.TestCase):
    def test_native_passthrough_and_direct(self):
        self.assertEqual(resolve_proxy_uri("socks5://u:p@1.2.3.4:1080"),
                         "socks5://u:p@1.2.3.4:1080")
        self.assertEqual(resolve_proxy_uri("http://1.2.3.4:8080"), "http://1.2.3.4:8080")
        self.assertEqual(resolve_proxy_uri(""), "")
        self.assertEqual(resolve_proxy_uri("direct"), "")
        self.assertEqual(resolve_proxy_uri("无代理"), "")
        self.assertIsNone(resolve_proxy_uri(None) or None)   # None → ""

    def test_singbox_missing_raises_without_direct_fallback(self):
        # 关键安全语义：sing-box 缺失时明确报错，绝不返回空串（直连）。
        with mock.patch.object(pr, "_singbox_bin_cache", ""):
            with self.assertRaises(ProxyResolveError) as ctx:
                resolve_proxy_uri(REAL_VLESS)
            self.assertIn("sing-box", str(ctx.exception))
        # 环境里真的装了 sing-box 时（本用例不 mock），也不允许返回空串：
        # 要么返回本地 socks5，要么抛错。
        try:
            result = resolve_proxy_uri(REAL_VLESS)
            self.assertTrue(result.startswith("socks5://127.0.0.1:"), result)
            # 同 URI 端口稳定复用（池签名不抖动的前提）
            self.assertEqual(resolve_proxy_uri(REAL_VLESS), result)
        except ProxyResolveError:
            pass
        finally:
            pr.singbox_manager.shutdown_all()
            # shutdown_all 会置 _closed 哨兵（进程级停机语义）；测试进程
            # 还要继续跑，必须 reopen 恢复，否则后续用例全报「进程正在退出」。
            pr.singbox_manager.reopen()

    def test_unsupported_scheme_rejected(self):
        with self.assertRaises(ProxyResolveError):
            resolve_proxy_uri("gopher://1.2.3.4:70")

    def test_async_resolve_matches_sync(self):
        import asyncio as _aio
        self.assertEqual(_aio.run(pr.resolve_proxy_uri_async("socks5://1.2.3.4:1080")),
                         "socks5://1.2.3.4:1080")
        self.assertEqual(_aio.run(pr.resolve_proxy_uri_async("")), "")
        self.assertEqual(_aio.run(pr.resolve_proxy_uri_async("direct")), "")
        self.assertEqual(_aio.run(pr.resolve_proxy_uri_async(None)), "")


class _FakeInst:
    """测试替身：只实现管理器用到的接口（不碰真进程）。"""

    def __init__(self, port: int, alive: bool = False):
        self.uri = ""
        self.port = port
        self.process = None
        self.config_path = ""
        self.log_path = ""
        self.started_at = 0.0
        self.last_used = 0.0
        self._alive = alive

    def alive(self) -> bool:
        return self._alive


class ManagerRobustnessTests(unittest.TestCase):
    """进程管理器防御性行为：停机/reopen、崩溃端口复活、死条目清理、锁外回收。"""

    URI = "vless://u@1.2.3.4:443"

    def test_shutdown_all_then_reopen(self):
        mgr = pr.SingboxManager()          # 独立实例，不碰全局单例
        with mock.patch.object(pr, "find_singbox_binary", return_value=None):
            mgr.shutdown_all()
            with self.assertRaises(ProxyResolveError) as ctx:
                mgr.get_local_socks5(self.URI)
            self.assertIn("退出", str(ctx.exception))
            mgr.reopen()
            # reopen 后回到正常语义（此处应为「缺二进制」而非「正在退出」）
            with self.assertRaises(ProxyResolveError) as ctx2:
                mgr.get_local_socks5(self.URI)
            self.assertIn("sing-box", str(ctx2.exception))
            self.assertNotIn("退出", str(ctx2.exception))

    def test_crashed_instance_retries_fresh_port(self):
        # 崩溃实例优先复用原端口；原端口启动失败（被占/配置错）时
        # 必须换新端口重试，而不是永远卡在同一个坏端口上。
        mgr = pr.SingboxManager()
        mgr._instances[self.URI] = _FakeInst(12345)
        calls: list[int] = []

        def fake_launch(uri: str, port: int):
            calls.append(port)
            if len(calls) == 1:
                raise pr.ProxyResolveError("端口被占")
            return _FakeInst(port, alive=True)

        with mock.patch.object(mgr, "_launch", side_effect=fake_launch):
            url = mgr.get_local_socks5(self.URI)
        self.assertEqual(calls[0], 12345)              # 先试旧端口
        self.assertNotEqual(calls[1], 12345)           # 再换新端口
        self.assertTrue(url.startswith("socks5://127.0.0.1:"))
        self.assertNotIn(":12345", url)

    def test_total_failure_cleans_dead_entry(self):
        # 两次尝试都失败：不留死条目（下次取用从全新端口开始），错误上抛。
        mgr = pr.SingboxManager()
        mgr._instances[self.URI] = _FakeInst(23456)

        def always_fail(uri: str, port: int):
            raise pr.ProxyResolveError("坏配置")

        with mock.patch.object(mgr, "_launch", side_effect=always_fail):
            with self.assertRaises(ProxyResolveError):
                mgr.get_local_socks5(self.URI)
        self.assertNotIn(self.URI, mgr._instances)

    def test_reap_idle_terminates_outside_lock(self):
        mgr = pr.SingboxManager()
        inst = _FakeInst(34567, alive=True)
        inst.last_used = time.time() - 99999
        mgr._instances[self.URI] = inst
        terminated: list = []
        with mock.patch.object(mgr, "_terminate", side_effect=lambda i: terminated.append(i)), \
                mock.patch.dict(os.environ, {"SINGBOX_IDLE_MINUTES": "1"}):
            n = mgr.reap_idle()
        self.assertEqual(n, 1)
        self.assertEqual(terminated, [inst])
        self.assertNotIn(self.URI, mgr._instances)


class KeyPriorityTests(unittest.TestCase):
    def setUp(self):
        # 隔离「控制台保存过 proxy_url」的存量状态：本类用例统一测
        # “从未保存”分支（stored=None → 跟随环境变量/全局兑底）。
        self._stored = mock.patch.object(app_state, "get_raw_stored_setting",
                                         return_value=None)
        self._stored.start()
        self.addCleanup(self._stored.stop)

    def tearDown(self):
        app_state.set_express_keys([])

    def test_key_proxy_priority(self):
        # 全局兜底：从未保存过 → 环境变量（patch 掉 import 时固化的 config.PROXY_URL）
        with mock.patch("config.PROXY_URL", "socks5://global:1080"):
            self.assertEqual(get_effective_proxy_uri_for_key(None), "socks5://global:1080")
            self.assertEqual(get_effective_proxy_uri_for_key({}), "socks5://global:1080")
            self.assertEqual(get_effective_proxy_uri_for_key({"proxy": ""}), "socks5://global:1080")
            # Key 级优先
            self.assertEqual(get_effective_proxy_uri_for_key({"proxy": REAL_VLESS}), REAL_VLESS)
            # direct 哨兵 = 强制直连（全局不生效）
            for token in ("direct", "NONE", "直连"):
                self.assertEqual(get_effective_proxy_uri_for_key({"proxy": token}), "")

    def test_console_saved_global_beats_env(self):
        self._stored.stop()          # 本用例用真实存储，撤销 setUp 的 mock
        app_state.update_settings({"proxy_url": "socks5://console:1080"})
        with mock.patch("config.PROXY_URL", "socks5://env:1080"):
            self.assertEqual(get_effective_proxy_uri_for_key(None), "socks5://console:1080")
            # 清空保存 = 明确直连，不被环境变量抢回
            app_state.update_settings({"proxy_url": ""})
            self.assertEqual(get_effective_proxy_uri_for_key(None), "")
        app_state._state.get("settings", {}).pop("proxy_url", None)   # 还原「从未保存」

    def test_record_normalization_keeps_proxy(self):
        app_state.set_express_keys([{"key": "k1", "proxy": REAL_VLESS},
                                    {"key": "k2"},
                                    {"key": "k3", "proxy": "direct"}])
        records = {r["key"]: r for r in app_state.get_express_keys()}
        self.assertEqual(records["k1"]["proxy"], REAL_VLESS)
        self.assertEqual(records["k2"]["proxy"], "")
        self.assertEqual(records["k3"]["proxy"], "direct")
        app_state.set_express_keys([])

    def test_string_records_default_to_global(self):
        app_state.set_express_keys(["plain-key"])
        rec = app_state.get_express_keys()[0]
        self.assertEqual(rec["proxy"], "")
        with mock.patch("config.PROXY_URL", "socks5://global:1080"):
            self.assertEqual(get_effective_proxy_uri_for_key(rec), "socks5://global:1080")


class ConfigShapeTests(unittest.TestCase):
    """离线冒烟：http_options / client_pool 在多协议代理下的签名与构造形态。"""

    def test_sdk_signature_varies_by_key_proxy(self):
        from client_pool import _sdk_signature, _direct_signature
        with mock.patch("config.PROXY_URL", "socks5://global:1080"):
            sig_global = _sdk_signature(None)
            sig_direct = _sdk_signature("")
            self.assertEqual(sig_global[1], "socks5://global:1080")
            self.assertEqual(sig_direct[1], "")                 # direct → 直连
            self.assertNotEqual(sig_direct, sig_global)
            # 真机装了 sing-box 时解析为本地 socks5；没装则抛错——两者都不能是空串
            if not pr.singbox_available():
                with self.assertRaises(ProxyResolveError):
                    _sdk_signature(REAL_VLESS)
            else:
                sig_key = _sdk_signature(REAL_VLESS)
                self.assertTrue(sig_key[1].startswith("socks5://127.0.0.1:"))
                self.assertNotEqual(sig_key[1], sig_global[1])
                # 同 URI 签名稳定
                self.assertEqual(_sdk_signature(REAL_VLESS), sig_key)
                pr.singbox_manager.shutdown_all()
            self.assertEqual(_direct_signature("direct")[1], "")

    def test_http_options_direct(self):
        from http_options import get_http_options
        # This assertion is specifically the no-overrides case. A developer's
        # custom CA/base URL must not turn it into a different configuration.
        with mock.patch("config.SSL_CERT_FILE", None), \
             mock.patch("config.VERTEX_BASE_URL", None), \
             mock.patch("http_options.build_sdk_channel_overrides", return_value={}):
            opts = get_http_options(proxy_url="direct")
            self.assertNotIn("proxy", opts.client_args)
            self.assertEqual(opts.retry_options.attempts, 1)
            self.assertIs(opts.async_client_args["trust_env"], False)
        opts_native = get_http_options(proxy_url="socks5://u:p@1.2.3.4:1080")
        self.assertEqual(opts_native.client_args["proxy"], "socks5://u:p@1.2.3.4:1080")
        self.assertEqual(opts_native.async_client_args["proxy"], "socks5://u:p@1.2.3.4:1080")


if __name__ == "__main__":
    unittest.main(verbosity=2)
