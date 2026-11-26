"""Returning unconstrained text is not a successful conversion of a known decoding contract."""
from unittest.mock import patch

from routes import chat_api
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness


class DeclaredGenerationConstraints(SchemaRouteHarness):
    async def assert_rejected_before_receipt(self, field, values):
        self.settings["resumable_enabled"] = True
        with patch.object(chat_api, "resume_intent", side_effect=AssertionError("unexpected replay")):
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                for mode in ("json", "true", "fake"):
                    self.settings["fake_streaming"] = mode == "fake"
                    for value in values:
                        with self.subTest(channel=channel, mode=mode, field=field, value=value):
                            r = await self.client.post("/v1/chat/completions", headers={"X-Resumable": "true"},
                                json={"model": MODEL, "messages": MESSAGES, "stream": mode != "json", field: value})
                            self.assertEqual(r.status_code, 400, r.text[:200])
                            self.assertIn(field, r.json()["error"]["message"])
                            self.assertNotIn("X-Resume-ID", r.headers)
        self.assertEqual(self.sent, [])

    async def test_unknown_explicit_format_is_never_free_text(self):
        await self.assert_rejected_before_receipt("response_format", ({"type": "xml"}, {"type": []}))

    async def test_null_and_explicit_text_remain_compatible(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for fmt in (None, {}, {"type": None}, {"type": "text"}):
                with self.subTest(channel=channel, fmt=fmt):
                    self.sent.clear()
                    r = await self.client.post("/v1/chat/completions",
                        json={"model": MODEL, "messages": MESSAGES, "response_format": fmt})
                    self.assertEqual(r.status_code, 200, r.text[:200])
                    self.assertEqual(len(self.sent), 1)

    async def test_unsupported_token_bias_fails_before_replay(self):
        await self.assert_rejected_before_receipt("logit_bias", ({"123": -100}, {"24": 1.0},
                                                                 {"10": False}, {"1": "0"}, []))

    async def test_inactive_token_bias_preserves_actual_wire(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for bias in (None, {}, {"123": 0, "45": -0.0}):
                with self.subTest(channel=channel, bias=bias):
                    self.sent.clear()
                    r = await self.client.post("/v1/chat/completions", json={"model": MODEL,
                        "messages": MESSAGES, "logit_bias": bias})
                    self.assertEqual(r.status_code, 200, r.text[:200])
                    self.assertEqual(len(self.sent), 1)
                    wire = self.sent[0].get("variables", self.sent[0])
                    self.assertNotIn("logit_bias", wire.get("generationConfig", {}))
