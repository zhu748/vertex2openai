"""Regression coverage for supplier/ICE imports and proxy process races."""

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-protocol-audit-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from proxy_config import (ProxyFormatError, build_proxy_uri, normalize_proxy_uri,
                          parse_bridge_proxy_uri, parse_proxy_list, proxy_fields)
import proxy_resolver as pr
from routes import system_api


class ProxyInputTests(unittest.TestCase):
    def test_supplier_formats_use_the_selected_bridge_protocol(self):
        for scheme, port in (("turn", 3478), ("turns", 5349), ("sstp", 443)):
            formats = (f"relay.example:{port}:user:p%40ss%3Aword",
                       f"user:p%40ss%3Aword:relay.example:{port}",
                       f"relay.example:{port}@user:p%40ss%3Aword",
                       f"user:p%40ss%3Aword@relay.example:{port}",
                       f"relay.example|{port}|user|p@ss:word",
                       f"relay.example {port} user p@ss:word",
                       f"{scheme}://relay.example:{port}:user:p%40ss%3Aword")
            for value in formats:
                with self.subTest(scheme=scheme, shape=formats.index(value)):
                    [uri] = parse_proxy_list(value, default_scheme=scheme)
                    fields = proxy_fields(uri)
                    self.assertEqual((fields["type"], fields["host"], fields["port"]),
                                     (scheme, "relay.example", port))
                    self.assertEqual((fields["username"], fields["password"]), ("user", "p@ss:word"))

    def test_base64_credentials_ipv6_defaults_and_options(self):
        auth = base64.urlsafe_b64encode(b"user:pass").decode().rstrip("=")
        for value in (f"sstp://{auth}@[2001:db8::1]", f"{auth}@[2001:db8::1]"):
            [uri] = parse_proxy_list(value, default_scheme="sstp")
            config = parse_bridge_proxy_uri(uri)
            self.assertEqual((config["server"], config["server_port"], config["password"]),
                             ("2001:db8::1", 443, "pass"))
        uri = normalize_proxy_uri("sstp://vpn.example:443:user:pass?sni=cert.example&mtu=1280#%E5%A4%87%E7%94%A8")
        fields = proxy_fields(uri)
        self.assertEqual(fields["name"], "备用")
        self.assertEqual(fields["options"], {"sni": "cert.example", "mtu": "1280"})
        self.assertEqual(normalize_proxy_uri(uri), uri)

    def test_literal_supplier_passwords_cannot_redirect_the_server(self):
        for password in ("p%literal", "p:word@different.example:8443", "p@host:443"):
            for separator in ("|", " "):
                [uri] = parse_proxy_list(separator.join(["vpn.example", "443", "user", password]), default_scheme="sstp")
                fields = proxy_fields(uri)
                self.assertEqual(fields["host"], "vpn.example")
                self.assertEqual(fields["password"], password)
        fields = proxy_fields(normalize_proxy_uri("sstp://vpn.example:443@user:1234?sni=cert.example#backup"))
        self.assertEqual((fields["host"], fields["username"], fields["password"]), ("vpn.example", "user", "1234"))
        self.assertEqual(fields["options"]["sni"], "cert.example")
        self.assertEqual(fields["name"], "backup")

    def test_ice_and_node_json_expand_in_order_without_double_decoding(self):
        ice = {"iceServers": [{"urls": "stun:discovery.example"},
                              {"urls": ["turn:relay.example?transport=tcp", "turns:relay.example"],
                               "username": "user@domain", "credential": "p%2F+:word"}]}
        uris = parse_proxy_list(json.dumps(ice))
        self.assertEqual([proxy_fields(uri)["type"] for uri in uris], ["turn", "turns"])
        for uri in uris:
            self.assertEqual(proxy_fields(uri)["password"], "p%2F+:word")
        node = {"type": "sstp", "server": "vpn.example", "username": "domain\\user",
                "password": "password", "name": "办公出口", "tls_server_name": "cert.example",
                "mtu": 1280, "insecure": False}
        for source in (node, json.dumps(node), [node], [json.dumps(node)]):
            [uri] = parse_proxy_list(source)
            fields = proxy_fields(uri)
            self.assertEqual(fields["name"], "办公出口")
            self.assertEqual(fields["options"]["sni"], "cert.example")
            self.assertEqual(fields["port"], 443)

    def test_bad_json_and_unsafe_options_never_become_direct_or_leak_credentials(self):
        sources = ({"iceServers": [{"urls": "stun:only.example"}]},
                   {"urls": "turn:relay.example", "username": "private-user", "credential": {"secret": "private-password"}},
                   {"urls": "turn:relay.example", "credentialType": "oauth"},
                   {"type": "sstp", "server": "vpn", "password": "private-password", "unknown": True},
                   '{"type":"sstp","insecure":false,"insecure":true}',
                   {"type": "sstp", "server": "vpn", "options": []},
                   {"iceServers": [{"iceServers": []}]})
        for source in sources:
            with self.subTest(shape=type(source).__name__):
                with self.assertRaises(ProxyFormatError) as error:
                    parse_proxy_list(source)
                self.assertNotIn("private-password", str(error.exception))

    def test_malformed_links_are_not_repaired_into_different_credentials(self):
        for suffix in ("?insecure=maybe", "?sni=bad%20name", "?mtu=%2B1400", "?mtu=１４００",
                       "?ca=a&ca=b", "?unknown=secret", "?sni=x%0Ay", "#bad%0Aname"):
            with self.assertRaises(ProxyFormatError):
                normalize_proxy_uri("sstp://private-user:private-password@vpn.example:443" + suffix)
        for password in ("bad%", "bad%XY", "bad%FF"):
            with self.assertRaises(ProxyFormatError):
                normalize_proxy_uri(f"sstp://user:{password}@vpn.example:443")
        with self.assertRaises(ProxyFormatError):
            parse_proxy_list({"urls": "turn:relay.example?transport=udp", "username": "u", "credential": "p"})

    def test_credentials_name_and_tls_options_survive_field_edit(self):
        uri = build_proxy_uri("sstp", "vpn.example", "", "u", "p%2F@:#", options={
            "sni": "cert.example", "ca": "/company CA.pem", "mtu": 1280, "insecure": False}, name="备用 VPN")
        fields = proxy_fields(uri)
        rebuilt = build_proxy_uri(fields["type"], fields["host"], fields["port"],
                                  fields["username"], fields["password"], options=fields["options"], name=fields["name"])
        self.assertEqual(rebuilt, uri)
        self.assertNotIn("p%2F", pr.mask_proxy_uri(uri))


def instance(port=12080):
    return SimpleNamespace(port=port, last_used=time.time(), alive=lambda: True)


class ProcessRaceTests(unittest.TestCase):
    def test_reaper_cannot_race_a_new_launch_on_the_same_uri(self):
        manager = pr.SingboxManager()
        uri = "vless://uuid@relay.example:443"
        old = instance()
        old.last_used = 0
        manager._instances[uri] = old
        terminating, finish, launching = threading.Event(), threading.Event(), threading.Event()

        def terminate(_):
            self.assertFalse(manager._lock.locked())
            terminating.set()
            self.assertTrue(finish.wait(2))

        def launch(*_):
            launching.set()
            return instance(12081)

        with (mock.patch.object(manager, "_terminate", side_effect=terminate),
              mock.patch.object(manager, "_launch", side_effect=launch),
              ThreadPoolExecutor(max_workers=2) as pool):
            reaping = pool.submit(manager.reap_idle)
            self.assertTrue(terminating.wait(1))
            getting = pool.submit(manager.get_local_socks5, uri)
            try:
                self.assertFalse(launching.wait(0.05))
            finally:
                finish.set()
            self.assertEqual(reaping.result(2), 1)
            self.assertEqual(getting.result(2), "socks5://127.0.0.1:12081")

    def test_equivalent_bridge_urls_share_a_session_and_tls_changes_do_not(self):
        manager = pr.ProxyBridgeManager()
        urls = ["turn:relay.example?username=u&password=p&transport=tcp",
                "turn://u:p@relay.example:3478#renamed", "turn://u:p@relay.example:3478"]
        with mock.patch.object(manager, "_launch", return_value=instance()) as launch:
            with ThreadPoolExecutor(max_workers=3) as pool:
                self.assertEqual(set(pool.map(manager.get_local_socks5, urls)), {"socks5://127.0.0.1:12080"})
            self.assertEqual(launch.call_count, 1)
        self.assertNotEqual(manager._cache_uri("sstp://u:p@vpn"), manager._cache_uri("sstp://u:p@vpn?insecure=true"))
        self.assertNotEqual(manager._cache_uri("sstp://u:p@vpn"), manager._cache_uri("sstp://u:p@vpn?sni=cert"))

    def test_unused_startup_locks_do_not_retain_node_credentials(self):
        manager = pr.SingboxManager()
        with mock.patch.object(manager, "_launch", side_effect=pr.ProxyResolveError("offline")):
            for i in range(100):
                with self.assertRaises(pr.ProxyResolveError):
                    manager.get_local_socks5(f"trojan://password-{i}@host:443")
        self.assertEqual(len(manager._uri_locks), 0)

    def test_readiness_accepts_fragmented_socks_greeting(self):
        conn = mock.MagicMock()
        conn.__enter__.return_value = conn
        conn.recv.side_effect = [b"\x05", b"\x00"]
        with mock.patch.object(pr.socket, "create_connection", return_value=conn):
            self.assertTrue(pr._wait_socks5_ready(12080, timeout=0.1))


class AsyncProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_selected_protocol_parses_authenticated_and_bare_endpoint_drafts(self):
        # Representative documentation IPs; recognition must never contact nodes.
        lines = ["test:1234@192.0.2.10:3478", "test:test123@198.51.100.20:3478",
                 "test:test123@203.0.113.30:3479", "test:test@192.0.2.40:3478",
                 "test:test@192.0.2.50:5349", "198.51.100.60:3478",
                 "198.51.100.60:3479", "203.0.113.70:3478", "203.0.113.70:5349"]
        for scheme in ("turn", "turns", "sstp", "http", "https", "socks5", "socks5h", "socks4", "socks4a"):
            response = await system_api.parse_proxy(system_api.ProxyParseBody(
                type=scheme, text="\n".join(lines), draft=True), True)
            self.assertEqual(response.status_code, 200)
            fields = json.loads(response.body)["proxies"]
            self.assertEqual(len(fields), 9)
            self.assertEqual({field["type"] for field in fields}, {scheme})
            self.assertEqual(fields[0]["username"], "test")
            self.assertEqual(fields[0]["password"], "1234")
            self.assertEqual(fields[2]["port"], 3479)
            self.assertEqual(fields[-1]["port"], 5349)
            for field in fields[5:]:
                self.assertEqual(field["username"], "")
                self.assertEqual(field["password"], "")
                # TURN / TURNS 无凭据是合法形态（匿名分配服务器）；仅 SSTP 强制。
                self.assertEqual(field.get("missing_fields", []),
                                 ["username", "password"] if scheme == "sstp" else [])

    async def test_draft_does_not_relax_runtime_or_settings_validation(self):
        from settings_validation import validate_global_settings_patch

        # SSTP 的 MS-CHAPv2 认证是强制的：草稿、保存、运行时都拒绝缺凭据。
        uri = "sstp://192.0.2.10:3478"
        response = await system_api.parse_proxy(system_api.ProxyParseBody(text=uri), True)
        self.assertEqual(response.status_code, 400)
        self.assertTrue(validate_global_settings_patch({"proxy_urls": [uri]}))
        self.assertTrue(pr.validate_proxy_uri(uri))
        with self.assertRaises(pr.ProxyResolveError):
            pr.resolve_proxy_uri(uri)
        # TURN / TURNS 无凭据（匿名分配服务器）现在是合法形态：保存、设置、
        # 校验全链路放行，连通性交给转换器的探测与实际拨号。
        for uri in ("turn://192.0.2.10:3478", "turns://192.0.2.10:5349"):
            response = await system_api.parse_proxy(system_api.ProxyParseBody(text=uri), True)
            self.assertEqual(response.status_code, 200)
            self.assertFalse(validate_global_settings_patch({"proxy_urls": [uri]}))
            self.assertEqual(pr.validate_proxy_uri(uri), "")
        # 草稿模式允许缺凭据（标记待补齐），但非法参数仍然拒绝。
        for uri in ("turn://host?transport=udp", "turn://host:0", "sstp://host?unknown=secret",
                    "sstp://host?mtu=100", "turn://host?realm=a&realm=b", "vless://["):
            response = await system_api.parse_proxy(system_api.ProxyParseBody(text=uri, draft=True), True)
            self.assertEqual(response.status_code, 400)

    async def test_draft_keeps_explicit_scheme_and_decodes_base64_auth(self):
        auth = base64.urlsafe_b64encode(b"test:1234").decode().rstrip("=")
        response = await system_api.parse_proxy(system_api.ProxyParseBody(
            type="turn", text=f"turns://{auth}@192.0.2.10", draft=True), True)
        self.assertEqual(response.status_code, 200)
        fields = json.loads(response.body)["proxies"][0]
        self.assertEqual((fields["type"], fields["port"], fields["username"], fields["password"]),
                         ("turns", 5349, "test", "1234"))
        self.assertNotIn("missing_fields", fields)

    async def test_native_and_cached_bridge_do_not_queue_worker_threads(self):
        manager = pr.ProxyBridgeManager()
        uri = "sstp://u:p@vpn.example:443"
        manager._instances[uri] = instance()
        usage = pr.ProxyUsage()
        token = pr.current_proxy_usage.set(usage)
        try:
            with (mock.patch.object(pr, "bridge_manager", manager),
                  mock.patch.object(asyncio, "to_thread", side_effect=AssertionError("warm proxy dispatched to thread"))):
                self.assertEqual(await pr.resolve_proxy_uri_async("http://host:80"), "http://host:80")
                self.assertEqual(await pr.resolve_proxy_uri_async("direct"), "")
                self.assertEqual(await pr.resolve_proxy_uri_async(uri + "#name"), "socks5://127.0.0.1:12080")
                self.assertEqual(manager._active_uses, {uri: 1})
            usage.close()
            self.assertFalse(manager._active_uses)
        finally:
            pr.current_proxy_usage.reset(token)
            usage.close()

    async def test_parser_api_imports_ice_and_selected_supplier_formats(self):
        for body in (system_api.ProxyParseBody(type="sstp", text="vpn.example:443:user:pass"),
                     system_api.ProxyParseBody(text=json.dumps({"type": "sstp", "server": "vpn.example", "username": "user", "password": "pass"}))):
            response = await system_api.parse_proxy(body, True)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(json.loads(response.body)["proxies"][0]["type"], "sstp")


if __name__ == "__main__":
    unittest.main()
