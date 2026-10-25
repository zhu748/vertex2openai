"""Opt-in real loopback relay tests; no vendor nodes or Google credentials.

PROXY_E2E_SINGBOX_BIN=/absolute/path/sing-box python tests/test_proxy_protocol_e2e.py
The default offline suite skips these tests when the binary is not supplied.
"""

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-proxy-e2e-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

import config
import impersonate
import proxy_resolver as pr
from client_pool import close_all_clients
from http_options import get_http_options


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def encode(value):
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


@unittest.skipUnless(os.environ.get("PROXY_E2E_SINGBOX_BIN"), "set PROXY_E2E_SINGBOX_BIN for real protocol relays")
class RealProxyProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.root = Path(_STATE.name)
        self.binary = str(Path(os.environ["PROXY_E2E_SINGBOX_BIN"]).resolve())
        self.check_only = os.environ.get("PROXY_E2E_CHECK_ONLY") == "1"
        if not self.check_only:
            probe_path = self.root / "runtime-probe.json"
            probe_path.write_text(json.dumps({"log": {"level": "error"}, "outbounds": [{"type": "direct"}]}))
            probe = await asyncio.create_subprocess_exec(self.binary, "run", "-c", str(probe_path),
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            await asyncio.sleep(.15)
            stopped = probe.returncode is not None
            if not stopped:
                probe.terminate()
            diagnostics, _ = await probe.communicate()
            if stopped:
                if b"netlink socket: operation not permitted" in diagnostics:
                    self.skipTest("environment blocks sing-box netlink initialization; use PROXY_E2E_CHECK_ONLY=1 for config validation")
                self.fail("sing-box cannot start: " + diagnostics.decode())
        self.payload = bytes(range(256)) * 512
        self.requests = 0

        async def origin(reader, writer):
            try:
                await reader.readuntil(b"\r\n\r\n")
                self.requests += 1
                writer.write(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                             + str(len(self.payload)).encode() + b"\r\n\r\n" + self.payload)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        self.origin = await asyncio.start_server(origin, "127.0.0.1", 0)
        port = self.origin.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/probe"
        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        now = datetime.now(timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
                .serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=1))
                .not_valid_after(now+timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
                .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False).sign(key, hashes.SHA256()))
        self.ca, self.key = self.root / "ca.pem", self.root / "key.pem"
        self.ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        self.key.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                             serialization.NoEncryption()))
        self.tls = {"enabled": True, "certificate_path": str(self.ca), "key_path": str(self.key)}

    async def asyncTearDown(self):
        await close_all_clients()
        self.origin.close()
        await self.origin.wait_closed()

    async def run_case(self, name, inbound, make_uri, *, reject=False):
        port = free_port()
        inbound = {"tag": "node", "listen": "127.0.0.1", "listen_port": port, **inbound}
        path = self.root / (name + ".json")
        path.write_text(json.dumps({"log": {"level": "error"}, "inbounds": [inbound],
                                  "outbounds": [{"type": "direct"}]}))
        check = await asyncio.create_subprocess_exec(self.binary, "check", "-c", str(path),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        diagnostics, _ = await check.communicate()
        self.assertEqual(check.returncode, 0, diagnostics.decode())
        uri = make_uri(port)
        if pr.classify_proxy_uri(uri) == "singbox":
            outbound = dict(pr.parse_share_uri_to_outbound(uri), tag="proxy")
            client_path = self.root / (name + "-client.json")
            client_path.write_text(json.dumps({"outbounds": [outbound], "route": {"final": "proxy"}}))
            check = await asyncio.create_subprocess_exec(self.binary, "check", "-c", str(client_path),
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            diagnostics, _ = await check.communicate()
            self.assertEqual(check.returncode, 0, diagnostics.decode())
        if self.check_only:
            print(f"validated {name}: engine configuration only, relay not exercised", flush=True)
            return
        log = (self.root / (name + ".log")).open("wb")
        process = await asyncio.create_subprocess_exec(self.binary, "run", "-c", str(path), stdout=log, stderr=log)
        manager = pr.SingboxManager()
        try:
            # TCP protocols can be probed directly; QUIC-only nodes simply
            # need their process to reach the event loop before the first dial.
            await asyncio.sleep(.12)
            self.assertIsNone(process.returncode, (self.root / (name + ".log")).read_text())
            with (mock.patch.object(pr, "singbox_manager", manager),
                  mock.patch.object(pr, "_singbox_bin_cache", self.binary),
                  mock.patch.object(config, "SSL_CERT_FILE", str(self.ca))):
                resolved = await pr.resolve_proxy_uri_async(uri)
                self.assertEqual(await pr.resolve_proxy_uri_async(uri), resolved)
                for backend in ("httpx", "curl"):
                    before = self.requests
                    try:
                        async with asyncio.timeout(8):
                            if backend == "httpx":
                                options = get_http_options(proxy_url=uri)
                                async with httpx.AsyncClient(**options.async_client_args) as client:
                                    response = await client.get(self.url)
                                    data = response.content
                            else:
                                with mock.patch.object(impersonate, "get_impersonate_profile", return_value="chrome"):
                                    async with impersonate.open_upstream_client({"proxy": resolved, "verify": str(self.ca),
                                            "trust_env": False, "timeout": httpx.Timeout(5)}) as client:
                                        async with client.stream("GET", self.url) as response:
                                            data = await response.aread()
                        if reject:
                            self.fail(f"{name}/{backend}: invalid credentials or certificate were accepted")
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(data, self.payload, f"{name}/{backend}: payload corrupted")
                        self.assertEqual(self.requests, before + 1)
                    except (httpx.TransportError, TimeoutError, pr.ProxyResolveError):
                        if not reject:
                            raise
                        self.assertEqual(self.requests, before)
                    print(f"verified {name}/{backend}: {'rejected' if reject else '128 KiB forwarded'}", flush=True)
                await close_all_clients()
        finally:
            await asyncio.to_thread(manager.shutdown_all)
            if process.returncode is None:
                process.terminate()
            await process.wait()
            log.close()

    async def test_native_http_socks_and_https_proxy(self):
        for scheme in ("http", "https", "socks5", "socks5h"):
            with self.subTest(scheme=scheme):
                inbound = {"type": "http" if scheme.startswith("http") else "socks",
                           "users": [{"username": "user", "password": "p:ss%word"}]}
                if scheme == "https":
                    inbound["tls"] = self.tls
                await self.run_case(scheme, inbound,
                    lambda port, scheme=scheme: f"{scheme}://user:p%3Ass%25word@127.0.0.1:{port}")

    async def test_converted_protocols_and_transports(self):
        uid = "11111111-2222-4333-8444-555555555555"
        for version in ("4", "4a"):
            with self.subTest(protocol="socks"+version):
                await self.run_case("socks"+version, {"type": "socks"},
                    lambda port, version=version: f"socks{version}://127.0.0.1:{port}")
        for protocol in ("vless", "vmess", "trojan"):
            for transport in ("tcp", "ws", "httpupgrade", "grpc", "http", "quic"):
                with self.subTest(protocol=protocol, transport=transport):
                    inbound = {"type": protocol, "users": [{"password": "password"} if protocol == "trojan" else {"uuid": uid}]}
                    tls = protocol == "trojan" or transport == "quic"
                    if tls:
                        inbound["tls"] = self.tls
                    if transport != "tcp":
                        inbound["transport"] = {"type": transport}
                        if transport in ("ws", "httpupgrade", "http"):
                            inbound["transport"]["path"] = "/relay"
                        if transport == "grpc":
                            inbound["transport"]["service_name"] = "relay"
                    def uri(port, protocol=protocol, transport=transport, tls=tls):
                        if protocol == "vmess":
                            return "vmess://" + encode(json.dumps({"add": "127.0.0.1", "port": port, "id": uid,
                                "net": transport, "path": "relay" if transport == "grpc" else "" if transport == "quic" else "/relay",
                                "tls": "tls" if tls else "", "allowInsecure": tls}))
                        auth = "password" if protocol == "trojan" else uid
                        query = f"type={transport}&path=%2Frelay&serviceName=relay"
                        if tls:
                            query += "&security=tls&insecure=1"
                        return f"{protocol}://{auth}@127.0.0.1:{port}?{query}"
                    await self.run_case(protocol+"-"+transport, inbound, uri)
        password = "literal%2F:p@ss"
        for legacy in (False, True):
            with self.subTest(protocol="ss", legacy=legacy):
                await self.run_case("ss-legacy" if legacy else "ss-sip002",
                    {"type": "shadowsocks", "method": "aes-128-gcm", "password": password},
                    lambda port, legacy=legacy: "ss://" + (encode(f"aes-128-gcm:{password}@127.0.0.1:{port}")
                        if legacy else encode("aes-128-gcm:"+password)+f"@127.0.0.1:{port}"))
        await self.run_case("hy2-userpass", {"type": "hysteria2", "users": [{"password": "alice:p:ss"}], "tls": self.tls},
            lambda port: f"hy2://alice:p%3Ass@127.0.0.1:{port}?insecure=1")
        await self.run_case("tuic", {"type": "tuic", "users": [{"uuid": uid, "password": "p:ss"}], "tls": self.tls},
            lambda port: f"tuic://{uid}:p%3Ass@127.0.0.1:{port}?insecure=1&congestion_control=bbr")


if __name__ == "__main__":
    unittest.main()
