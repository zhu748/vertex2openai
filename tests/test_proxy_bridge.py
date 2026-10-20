"""TURN/SSTP configuration, process ownership and credential handling (offline)."""

import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import time
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-proxy-bridge-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from proxy_config import (ProxyFormatError, build_proxy_uri, parse_bridge_proxy_uri,
                          parse_proxy_list, proxy_fields)
import proxy_resolver as pr


class BridgeConfigurationTests(unittest.TestCase):
    def test_standard_turn_uris_and_defaults(self):
        uri = "TURNS:[2001:db8::1]?transport=tcp&username=u%40ser&password=p%3Aa%23ss"
        config = parse_bridge_proxy_uri(uri)
        self.assertEqual((config["scheme"], config["server"], config["server_port"]),
                         ("turns", "2001:db8::1", 5349))
        self.assertEqual((config["username"], config["password"]), ("u@ser", "p:a#ss"))
        for scheme, port in (("turn", 3478), ("turns", 5349), ("sstp", 443)):
            self.assertEqual(parse_bridge_proxy_uri(f"{scheme}://u:p@host")["server_port"], port)
            self.assertEqual(build_proxy_uri(scheme, "host", "", "u", "p"), f"{scheme}://u:p@host:{port}")
        self.assertIn("***@[2001:db8::1]:5349", pr.mask_proxy_uri(uri))

    def test_advanced_options_and_credentials_survive_editor_round_trip(self):
        uri = build_proxy_uri("sstp", "[2001:db8::1]", 8443, "domain\\user", "p@ss:#", options={
            "sni": "vpn.example.com", "ca": "/app/certificates/公司.pem", "mtu": 1280, "insecure": False})
        fields = proxy_fields(uri)
        self.assertEqual(fields["type"], "sstp")
        self.assertEqual(build_proxy_uri(fields["type"], fields["host"], fields["port"],
                                         fields["username"], fields["password"], options=fields["options"]), uri)
        config = parse_bridge_proxy_uri(uri)
        self.assertEqual(config["tls_server_name"], "vpn.example.com")
        self.assertEqual(config["ca_file"], "/app/certificates/公司.pem")
        self.assertEqual(config["mtu"], 1280)
        self.assertFalse(config["insecure"])

    def test_invalid_protocol_options_are_rejected_without_secrets(self):
        base = "sstp://private-user:private-password@host:443"
        cases = ("turn://u:p@host?transport=udp",
                 "turn://u:p@host?sni=other", base + "?mtu=100", base + "?mtu=nan",
                 base + "?insecure=maybe", base + "?ca=a&ca=b", base + "?unused=secret",
                 base + "?password=private-password", base + "/unsupported",
                 base + "?sni=a%0Ab", "turns://u:p@host:0", "turns://u:p@[invalid]:3478")
        for uri in cases:
            with self.subTest(uri=uri.split(":", 1)[0]):
                with self.assertRaises(ProxyFormatError) as error:
                    parse_bridge_proxy_uri(uri)
                self.assertNotIn("private-password", str(error.exception))
                self.assertNotEqual(pr.validate_proxy_uri(uri), "")

    def test_anonymous_allocation_turn_nodes_are_accepted(self):
        # 开放 TURN 服务器（未认证 Allocate 直接成功）没有凭据也能导入/保存；
        # SSTP 的 MS-CHAPv2 认证仍然强制。
        for uri in ("turn://192.0.2.10:3479", "turn:host.example.com:3478?transport=tcp",
                    "turn:192.0.2.10:3478?transport=tcp&username=&password="):
            with self.subTest(uri=uri):
                config = parse_bridge_proxy_uri(uri)
                self.assertEqual((config["username"], config["password"]), ("", ""))
                self.assertEqual(pr.validate_proxy_uri(uri), "")
        fields = proxy_fields("turn://192.0.2.10:3479")
        self.assertNotIn("missing_fields", fields)
        self.assertNotIn("***@", pr.mask_proxy_uri("turn://192.0.2.10:3479"))
        self.assertIn("***@", pr.mask_proxy_uri("turn://u:p@192.0.2.10:3479"))
        with self.assertRaises(ProxyFormatError):
            parse_bridge_proxy_uri("sstp://192.0.2.10:443")

    def test_ordered_lists_accept_standard_and_url_turn_forms(self):
        turn = "turn:host:3478?username=u&password=p&transport=tcp"
        sstp = "sstp://u:p@vpn.example.com:443"
        self.assertEqual(parse_proxy_list(turn + ", " + sstp + "," + turn),
                         ["turn://u:p@host:3478?transport=tcp", sstp])


class BridgeProcessTests(unittest.TestCase):
    uri = "sstp://private-user:private-password@vpn.example.com:443?sni=cert.example.com"

    def test_missing_binary_fails_closed(self):
        manager = pr.ProxyBridgeManager()
        with mock.patch.object(pr, "find_bridge_binary", return_value=None):
            with self.assertRaises(pr.ProxyResolveError) as error:
                manager.get_local_socks5(self.uri)
        self.assertIn("PROXY_BRIDGE_BIN", str(error.exception))
        self.assertIn("不会降级直连", str(error.exception))

    def test_private_config_stable_port_usage_and_idle_cleanup(self):
        process = mock.Mock()
        process.poll.return_value = None
        seen = []

        def launch(args, **kwargs):
            path = Path(args[-1])
            config = json.loads(path.read_text())
            seen.append(config)
            Path(config["ready_file"]).write_text("ready\n")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
            self.assertNotIn("private-password", " ".join(args))
            return process

        manager = pr.ProxyBridgeManager()
        usage = pr.ProxyUsage()
        token = pr.current_proxy_usage.set(usage)
        try:
            with (mock.patch.object(pr, "find_bridge_binary", return_value="/fake/bridge"),
                  mock.patch.object(pr.subprocess, "Popen", side_effect=launch) as popen,
                  mock.patch.object(pr, "_wait_socks5_ready", return_value=True),
                  mock.patch.object(pr, "_find_free_port", return_value=12080)):
                self.assertEqual(manager.get_local_socks5(self.uri), "socks5://127.0.0.1:12080")
                self.assertEqual(manager.get_local_socks5(self.uri), "socks5://127.0.0.1:12080")
                self.assertEqual(popen.call_count, 1)
            instance = manager._instances[self.uri]
            self.assertFalse(Path(instance.config_path).exists())
            self.assertEqual(seen[0]["tls_server_name"], "cert.example.com")
            self.assertEqual(seen[0]["password"], "private-password")
            with mock.patch.object(manager, "_idle_minutes", return_value=1):
                instance.last_used = time.time() - 120
                self.assertEqual(manager.reap_idle(), 0)
                usage.close()
                instance.last_used = time.time() - 120
                self.assertEqual(manager.reap_idle(), 1)
            process.terminate.assert_called_once()
        finally:
            pr.current_proxy_usage.reset(token)
            usage.close()
            manager.shutdown_all()

    def test_foreign_socks_server_does_not_count_as_child_readiness(self):
        instance = mock.Mock(port=12080)
        instance.alive.return_value = True
        with mock.patch.object(pr, "_wait_socks5_ready", return_value=True) as probe:
            self.assertFalse(pr._wait_bridge_ready(instance, "/nonexistent/v2o-ready-marker", 0.01))
        probe.assert_not_called()

    def test_managers_do_not_delete_each_others_logs(self):
        def launch(args, **kwargs):
            config = json.loads(Path(args[-1]).read_text())
            Path(config["ready_file"]).write_text("ready\n")
            process = mock.Mock()
            process.poll.return_value = None
            return process

        first, second = pr.ProxyBridgeManager(), pr.ProxyBridgeManager()
        with (mock.patch.object(pr, "find_bridge_binary", return_value="/fake/bridge"),
              mock.patch.object(pr.subprocess, "Popen", side_effect=launch),
              mock.patch.object(pr, "_wait_socks5_ready", return_value=True)):
            a = first._launch(self.uri, 12081)
            b = second._launch(self.uri, 12082)
        self.assertNotEqual(a.log_path, b.log_path)
        self.assertTrue(Path(a.log_path).exists())
        self.assertTrue(Path(b.log_path).exists())
        first._terminate(a)
        second._terminate(b)

    def test_failed_startup_removes_config_and_does_not_echo_logs(self):
        process = mock.Mock()
        process.poll.return_value = 1
        paths = []

        def launch(args, **kwargs):
            paths.append(Path(args[-1]))
            kwargs["stdout"].write(b"private-password\nsstp_auth_failed_requires_mschapv2\n")
            return process

        manager = pr.ProxyBridgeManager()
        with (mock.patch.object(pr, "find_bridge_binary", return_value="/fake/bridge"),
              mock.patch.object(pr.subprocess, "Popen", side_effect=launch),
              mock.patch.object(pr, "_wait_socks5_ready", return_value=False)):
            with self.assertRaises(pr.ProxyResolveError) as error:
                manager.get_local_socks5(self.uri)
        self.assertIn("MS-CHAPv2", str(error.exception))
        self.assertNotIn("private-password", str(error.exception))
        self.assertTrue(all(not path.exists() for path in paths))
        self.assertFalse(manager._instances)


if __name__ == "__main__":
    unittest.main()
