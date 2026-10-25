"""Actual HTTP/HTTPS/SOCKS5 relays through both application HTTP backends."""

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import ssl
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-native-relays-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from google import genai
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

import config
import impersonate
from client_pool import close_all_clients
from http_options import get_http_options


class NativeRelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.payload = bytes(range(256)) * 512
        self.seen = []
        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        now = datetime.now(timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
                .serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=1))
                .not_valid_after(now+timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
                .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"),
                    x509.DNSName("target.invalid"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False).sign(key, hashes.SHA256()))
        self.ca, key_file = Path(_STATE.name)/"ca.pem", Path(_STATE.name)/"key.pem"
        self.ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                               serialization.NoEncryption()))
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(self.ca, key_file)
        self.servers = []
        self.ports = {}
        async def secure_origin(reader, writer):
            try:
                await self.reply(reader, writer)
            finally:
                writer.close()
                await writer.wait_closed()
        origin = await asyncio.start_server(secure_origin, "127.0.0.1", 0, ssl=tls)
        self.servers.append(origin)
        self.origin_port = origin.sockets[0].getsockname()[1]
        for scheme, context in (("http", None), ("https", tls), ("socks5", None)):
            server = await asyncio.start_server(
                self.socks if scheme == "socks5" else self.http, "127.0.0.1", 0, ssl=context)
            self.servers.append(server)
            self.ports[scheme] = server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        await close_all_clients()
        for server in self.servers:
            server.close()
            await server.wait_closed()

    async def reply(self, reader, writer):
        request = await reader.readuntil(b"\r\n\r\n")
        self.seen.append(request)
        writer.write(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                     + str(len(self.payload)).encode() + b"\r\n\r\n" + self.payload)
        await writer.drain()

    async def http(self, reader, writer):
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            auth = b"Basic " + base64.b64encode(b"user:p:ss%word")
            headers = {line.partition(b":")[0].lower(): line.partition(b":")[2].strip()
                       for line in request.split(b"\r\n")[1:] if b":" in line}
            if headers.get(b"proxy-authorization") != auth:
                writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                return
            self.seen.append(request)
            if request.startswith(b"CONNECT "):
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
                await self.tunnel(reader, writer)
                return
            # The fixture terminates the proxy request itself; no direct origin
            # route exists. Receiving its marker proves the proxy carried bytes.
            payload = self.payload
            if b":generateContent" in request.split(b"\r\n", 1)[0]:
                payload = json.dumps({"candidates": [{"content": {"role": "model", "parts": [
                    {"text": "proxy-sdk-marker"}]}, "finishReason": "STOP"}]}).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                         + str(len(payload)).encode() + b"\r\n\r\n" + payload)
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def socks(self, reader, writer):
        try:
            version, size = await reader.readexactly(2)
            methods = await reader.readexactly(size)
            if version != 5 or 2 not in methods:
                return
            writer.write(b"\x05\x02")
            await writer.drain()
            version, size = await reader.readexactly(2)
            user = await reader.readexactly(size)
            size = (await reader.readexactly(1))[0]
            password = await reader.readexactly(size)
            if version != 1 or user != b"user" or password != b"p:ss%word":
                writer.write(b"\x01\x01")
                await writer.drain()
                return
            writer.write(b"\x01\x00")
            await writer.drain()
            version, command, _, atyp = await reader.readexactly(4)
            self.assertEqual((version, command, atyp), (5, 1, 3))
            size = (await reader.readexactly(1))[0]
            self.assertEqual(await reader.readexactly(size), b"target.invalid")
            port = int.from_bytes(await reader.readexactly(2), "big")
            writer.write(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x50")
            await writer.drain()
            if port == self.origin_port:
                await self.tunnel(reader, writer)
            else:
                await self.reply(reader, writer)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def tunnel(self, reader, writer):
        remote_reader, remote_writer = await asyncio.open_connection("127.0.0.1", self.origin_port)
        async def copy(source, target):
            while data := await source.read(64 * 1024):
                target.write(data)
                await target.drain()
        tasks = [asyncio.create_task(copy(reader, remote_writer)), asyncio.create_task(copy(remote_reader, writer))]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            remote_writer.close()
            await remote_writer.wait_closed()

    async def test_native_schemes_forward_credentials_dns_and_large_payload(self):
        for scheme in ("http", "https", "socks5", "socks5h"):
            for backend in ("sdk-httpx", "cookie-httpx", "curl"):
                with self.subTest(scheme=scheme, backend=backend):
                    port = self.ports["socks5" if scheme == "socks5h" else scheme]
                    proxy = f"{scheme}://user:p%3Ass%25word@127.0.0.1:{port}"
                    before = len(self.seen)
                    with mock.patch.object(config, "SSL_CERT_FILE", str(self.ca)):
                        async with asyncio.timeout(5):
                            if backend == "sdk-httpx":
                                options = get_http_options(proxy_url=proxy)
                                async with httpx.AsyncClient(**options.async_client_args) as client:
                                    response = await client.get("http://target.invalid/probe")
                                    data = response.content
                            else:
                                with mock.patch.object(impersonate, "get_impersonate_profile", return_value="chrome" if backend == "curl" else "off"):
                                    async with impersonate.open_upstream_client({"proxy": proxy, "verify": str(self.ca),
                                            "trust_env": False, "timeout": httpx.Timeout(5)}) as client:
                                        async with client.stream("GET", "http://target.invalid/probe") as response:
                                            data = await response.aread()
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(data, self.payload)
                    self.assertEqual(len(self.seen), before+1)

    async def test_unknown_ca_is_rejected(self):
        port = self.ports["https"]
        with mock.patch.object(config, "SSL_CERT_FILE", ""):
            options = get_http_options(proxy_url=f"https://user:p%3Ass%25word@127.0.0.1:{port}")
            async with httpx.AsyncClient(**options.async_client_args) as client:
                with self.assertRaises(httpx.ConnectError):
                    await client.get("http://target.invalid/probe")
        self.assertEqual(self.seen, [])

    async def test_google_sdk_uses_custom_https_proxy_trust(self):
        proxy = f"https://user:p%3Ass%25word@127.0.0.1:{self.ports['https']}"
        with mock.patch.object(config, "SSL_CERT_FILE", str(self.ca)):
            client = genai.Client(vertexai=True, api_key="fixture", http_options=get_http_options(
                base_url="http://target.invalid", proxy_url=proxy))
            try:
                async with asyncio.timeout(5):
                    response = await client.aio.models.generate_content(model="gemini-test", contents="hello")
                self.assertEqual(response.text, "proxy-sdk-marker")
                self.assertEqual(len(self.seen), 1)
            finally:
                await client.aio.aclose()
                client.close()

    async def test_https_targets_traverse_real_connect_and_socks_tunnels(self):
        target = f"https://target.invalid:{self.origin_port}/probe"
        for scheme in ("http", "https", "socks5", "socks5h"):
            for backend in ("sdk-httpx", "cookie-httpx", "curl"):
                with self.subTest(scheme=scheme, backend=backend):
                    port = self.ports["socks5" if scheme == "socks5h" else scheme]
                    proxy = f"{scheme}://user:p%3Ass%25word@127.0.0.1:{port}"
                    with mock.patch.object(config, "SSL_CERT_FILE", str(self.ca)):
                        async with asyncio.timeout(5):
                            if backend == "sdk-httpx":
                                options = get_http_options(proxy_url=proxy)
                                async with httpx.AsyncClient(**options.async_client_args) as client:
                                    response = await client.get(target)
                                    data = response.content
                            else:
                                with mock.patch.object(impersonate, "get_impersonate_profile", return_value="chrome" if backend == "curl" else "off"):
                                    async with impersonate.open_upstream_client({"proxy": proxy, "verify": str(self.ca),
                                            "trust_env": False, "timeout": httpx.Timeout(5)}) as client:
                                        async with client.stream("GET", target) as response:
                                            data = await response.aread()
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(data, self.payload)
                    self.assertTrue(self.seen[-1].startswith(b"GET /probe HTTP/1.1"))
                    self.assertNotIn(b"proxy-authorization", self.seen[-1].lower())


if __name__ == "__main__":
    unittest.main()
