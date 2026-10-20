"""健康检查的 HEAD 语义与出站连通性诊断回归。"""

import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-system-test-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

import config
import proxy_resolver
from routes import system_api


class SystemRoutesTests(unittest.IsolatedAsyncioTestCase):
    async def test_proxy_editor_accepts_turn_sstp_and_preserves_tls_settings(self):
        for scheme in ("turn", "turns", "sstp"):
            options = {"sni": "cert.example.com", "ca": "/cert.pem"} if scheme != "turn" else {"realm": "fixture"}
            response = await system_api.parse_proxy(system_api.ProxyParseBody(
                type=scheme, host="proxy.example.com", username="user", password="p@ss", options=options), True)
            self.assertEqual(response.status_code, 200)
            fields = json.loads(response.body)["proxies"][0]
            self.assertEqual(fields["type"], scheme)
            self.assertEqual(fields["options"], options)
            self.assertEqual(fields["password"], "p@ss")

    async def test_health_head_preserves_representation_headers_without_a_body(self):
        with mock.patch.object(system_api, "get_effective_proxy_url", return_value="direct"):
            get_response = await system_api.healthz_get()
            head_response = await system_api.healthz_head()
        self.assertEqual(head_response.status_code, get_response.status_code)
        self.assertEqual(head_response.headers, get_response.headers)
        sent = []

        async def send(message):
            sent.append(message)

        await head_response({"type": "http", "method": "HEAD"}, None, send)
        self.assertEqual(sent[-1], {"type": "http.response.body", "body": b""})
        self.assertFalse(json.loads(get_response.body)["proxy"]["enabled"])

    async def probe(self, candidate, *, effective="", cert="", failure=False):
        options = []
        requests = []
        real_client = httpx.AsyncClient

        async def respond(request):
            requests.append(request)
            if failure:
                raise httpx.ConnectError("offline test", request=request)
            return httpx.Response(403, request=request)

        def client(**kwargs):
            options.append(kwargs)
            # Keep the real HTTP request/response lifecycle, without the network.
            safe = {k: v for k, v in kwargs.items() if k not in ("proxy", "verify")}
            return real_client(transport=httpx.MockTransport(respond), **safe)

        with (
            mock.patch.object(system_api, "get_effective_proxy_url", return_value=effective),
            mock.patch.object(config, "SSL_CERT_FILE", cert),
            mock.patch.object(httpx, "AsyncClient", side_effect=client),
            mock.patch.object(system_api, "time", SimpleNamespace(
                monotonic=mock.Mock(side_effect=[10.0, 10.125]))),
            mock.patch.dict(os.environ, {"HTTPS_PROXY": "http://env-proxy.invalid:1"}),
        ):
            response = await system_api.test_proxy(system_api.ProxyTestBody(proxy_url=candidate), True)
        self.assertEqual(len(requests), 1)
        self.assertEqual(str(requests[0].url), system_api.PROXY_TEST_TARGET)
        self.assertEqual(response.status_code, 200)
        return json.loads(response.body), options[0]

    async def test_direct_tokens_override_saved_and_environment_proxies(self):
        for token in sorted(proxy_resolver.DIRECT_TOKENS):
            with self.subTest(token=token):
                result, options = await self.probe(token, effective="http://saved-proxy.invalid:8080")
                self.assertTrue(result["ok"])
                self.assertFalse(result["via_proxy"])
                self.assertEqual(result["proxy"], "（直连）")
                self.assertEqual(result["latency_ms"], 125)
                self.assertIn("直连 Google 成功", result["message"])
                self.assertIs(options["trust_env"], False)
                self.assertNotIn("proxy", options)

    async def test_explicit_and_saved_proxies_use_configured_ca_and_mask_credentials(self):
        proxy = "http://user:private-password@proxy.invalid:8080"
        for candidate in (proxy, ""):
            with self.subTest(candidate=candidate):
                result, options = await self.probe(candidate, effective=proxy, cert="custom-ca.pem")
                self.assertTrue(result["ok"])
                self.assertTrue(result["via_proxy"])
                self.assertEqual(result["status_code"], 403)
                self.assertNotIn("private-password", result["proxy"])
                self.assertEqual(options["proxy"], proxy)
                self.assertEqual(options["verify"], "custom-ca.pem")
                self.assertIs(options["trust_env"], False)

    async def test_connect_error_has_direct_diagnostics_and_monotonic_latency(self):
        result, _ = await self.probe("direct", failure=True)
        self.assertFalse(result["ok"])
        self.assertFalse(result["via_proxy"])
        self.assertEqual(result["latency_ms"], 125)
        self.assertIn("直连 Google 失败", result["message"])


if __name__ == "__main__":
    unittest.main()
