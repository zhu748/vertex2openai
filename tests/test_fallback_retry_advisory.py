"""Cross-channel JSON errors preserve a validated delay from the standby."""
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery, express_sdk


class FallbackRetryAdvisoryTests(SchemaRouteHarness):
    async def test_double_failure_keeps_primary_status_and_real_standby_advisory(self):
        self.channel = "web_proxy"
        self.settings.update(cross_channel_fallback=True, retry_max=0)
        calls = []
        primary_headers, standby_headers = {}, {}

        def primary(_request):
            calls.append("cookie")
            return httpx.Response(503, json={"error": {"code": 503, "message": "primary unavailable"}},
                                  headers=primary_headers)

        def standby(_request):
            calls.append("express")
            return httpx.Response(429, json={"error": {
                "code": 429, "status": "RESOURCE_EXHAUSTED", "message": "standby throttled"}},
                headers=standby_headers)

        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(standby)}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        async with httpx.AsyncClient(transport=httpx.MockTransport(primary)) as studio:
            @asynccontextmanager
            async def open_studio(*args, **kwargs):
                yield studio

            with patch.object(cookie_delivery, "open_upstream_client", open_studio), \
                    patch.object(express_sdk, "get_pooled_express_client", return_value=sdk):
                for protocol in ("openai", "claude"):
                    path, body = _request(protocol, "auto")
                    for initial, alternate, expected in ((None, "78", "78"),
                                                         (None, "not a delay", None),
                                                         (None, "0", None),
                                                         ("42", "78", "42")):
                        with self.subTest(protocol=protocol, initial=initial, alternate=alternate):
                            primary_headers.clear()
                            if initial is not None:
                                primary_headers["Retry-After"] = initial
                            standby_headers.update({"Retry-After": alternate, "X-Fixture-Other": "not public"})
                            calls.clear()
                            result = await self.client.post(path, json=body)
                            self.assertEqual(result.status_code, 503)
                            self.assertIn("primary unavailable", result.json()["error"]["message"])
                            self.assertEqual(result.headers.get("retry-after"), expected)
                            self.assertNotIn("x-fixture-other", result.headers)
                            self.assertEqual(calls, ["cookie", "express"] if initial is None else ["cookie"])
