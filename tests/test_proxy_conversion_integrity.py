"""Proxy conversion must preserve credentials and reject incompatible transports."""

import base64
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from urllib.parse import quote

_STATE = tempfile.TemporaryDirectory(prefix="v2o-proxy-integrity-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from proxy_resolver import ProxyResolveError, parse_share_uri_to_outbound, validate_proxy_uri
from proxy_config import normalize_proxy_uri, proxy_fields


def b64(value):
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def vmess(**values):
    return "vmess://" + b64(json.dumps({"add": "node.example", "port": 443, "id": "id", **values}))


class CredentialIntegrityTests(unittest.TestCase):
    def test_base64_native_and_bridge_credentials_preserve_literal_percent_sequences(self):
        username, password = "user%2Fname", "literal%2F%23+:雪"
        auth = b64(username + ":" + password)
        for scheme in ("http", "https", "socks5", "socks5h", "socks4", "socks4a", "turn", "turns", "sstp"):
            with self.subTest(scheme=scheme):
                uri = normalize_proxy_uri(f"{scheme}://{auth}@node.example:8443")
                fields = proxy_fields(uri)
                self.assertEqual((fields["username"], fields["password"]), (username, password))
                self.assertEqual(normalize_proxy_uri(uri), uri)

    def test_shadowsocks_base64_credentials_are_not_url_decoded_again(self):
        password = "literal%2F%23+:雪"
        for uri in (f"ss://{b64('aes-128-gcm:' + password)}@node.example:8388",
                    "ss://" + b64(f"aes-128-gcm:{password}@node.example:8388"),
                    f"ss://aes-128-gcm:{quote(password, safe='')}@node.example:8388"):
            with self.subTest(uri_kind="userinfo" if "@" in uri else "legacy"):
                self.assertEqual(parse_share_uri_to_outbound(uri)["password"], password)

    def test_hysteria_userpass_auth_retains_both_parts(self):
        for prefix in ("hysteria2", "hy2"):
            for auth in ("alice:p%3Ass%2Fword", "alice%3Ap%3Ass%2Fword"):
                uri = f"{prefix}://{auth}@node.example:443"
                self.assertEqual(parse_share_uri_to_outbound(uri)["password"], "alice:p:ss/word")

    def test_hysteria_omitted_port_uses_standard_default(self):
        self.assertEqual(parse_share_uri_to_outbound("hy2://password@node.example/")["server_port"], 443)

    def test_unsupported_certificate_pinning_and_ech_are_rejected(self):
        for query in ("pinSHA256=deadbeef", "ech=opaque-config"):
            uri = "hy2://password@node.example:443?" + query
            with self.assertRaises(ProxyResolveError):
                parse_share_uri_to_outbound(uri)
            self.assertTrue(validate_proxy_uri(uri))

    def test_bad_shadowsocks_endpoints_do_not_expose_credentials(self):
        for endpoint in ("node.example:nan", "node.example:0", "node.example", ":8388"):
            error = validate_proxy_uri("ss://aes-128-gcm:private-password@" + endpoint)
            self.assertTrue(error)
            self.assertNotIn("private-password", error)


class TransportIntegrityTests(unittest.TestCase):
    def test_tcp_camouflage_is_not_silently_dropped(self):
        for uri in ("vless://id@node.example:443?type=tcp&headerType=http",
                    "trojan://password@node.example:443?type=tcp&headerType=http",
                    vmess(net="tcp", type="http", host="cdn.example", path="/relay")):
            with self.assertRaises(ProxyResolveError):
                parse_share_uri_to_outbound(uri)

    def test_vless_encryption_requirements_are_not_silently_dropped(self):
        uri = "vless://id@node.example:443?security=tls&encryption=unsupported-secret"
        with self.assertRaises(ProxyResolveError) as error:
            parse_share_uri_to_outbound(uri)
        self.assertNotIn("unsupported-secret", str(error.exception))
        self.assertTrue(validate_proxy_uri(uri))

    def test_unknown_tls_mode_is_not_downgraded_to_plaintext(self):
        for uri in ("vless://id@node.example:443?security=unsupported",
                    vmess(tls="unsupported")):
            with self.assertRaises(ProxyResolveError):
                parse_share_uri_to_outbound(uri)
            self.assertTrue(validate_proxy_uri(uri))

    def test_unknown_transport_is_not_silently_changed_to_tcp(self):
        for transport in ("kcp", "xhttp", "splithttp", "unknown"):
            for uri in (f"vless://id@node.example:443?type={transport}",
                        f"trojan://password@node.example:443?type={transport}",
                        vmess(net=transport)):
                with self.subTest(transport=transport, scheme=uri.split(":", 1)[0]):
                    with self.assertRaises(ProxyResolveError):
                        parse_share_uri_to_outbound(uri)
                    self.assertTrue(validate_proxy_uri(uri))

    def test_httpupgrade_and_quic_are_preserved(self):
        for uri in ("vless://id@node.example:443?type=httpupgrade&host=cdn.example&path=%2Fupgrade",
                    vmess(net="httpupgrade", host="cdn.example", path="/upgrade")):
            transport = parse_share_uri_to_outbound(uri)["transport"]
            self.assertEqual(transport, {"type": "httpupgrade", "host": "cdn.example", "path": "/upgrade"})
        self.assertEqual(parse_share_uri_to_outbound(
            "vless://id@node.example:443?security=tls&type=quic")["transport"], {"type": "quic"})

    def test_quic_extra_encryption_cannot_be_silently_dropped(self):
        for uri in ("vless://id@node.example:443?security=tls&type=quic&quicSecurity=aes-128-gcm&key=secret",
                    vmess(net="quic", tls="tls", host="aes-128-gcm", path="secret", type="none")):
            with self.assertRaises(ProxyResolveError) as error:
                parse_share_uri_to_outbound(uri)
            self.assertNotIn("secret", str(error.exception))


if __name__ == "__main__":
    unittest.main()
