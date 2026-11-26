"""No silently downgraded speech or lost assistant audio history."""
from unittest.mock import patch

from routes import chat_api
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness


class AudioContracts(SchemaRouteHarness):
    async def test_reject_before_receipt_or_dispatch(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for options in (
                {"modalities": ["text", "audio"], "audio": {"voice": "alloy", "format": "wav"}},
                {"modalities": ["audio"]},
                {"audio": {"voice": "alloy"}},
                {"messages": [*MESSAGES, {"role": "assistant", "content": None, "audio": {"id": "earlier"}}]},
                {"messages": [*MESSAGES, {"role": "assistant", "content": "caption", "audio": {"id": "earlier"}}]},
            ):
                for mode in ("json", "true", "fake"):
                    self.settings["fake_streaming"] = mode == "fake"
                    with self.subTest(channel=channel, options=options, mode=mode):
                        for resumable in ("true", "false"):
                            with patch.object(chat_api, "resume_intent", side_effect=AssertionError("receipt admission")):
                                r = await self.client.post("/v1/chat/completions", headers={"X-Resumable": resumable},
                                    json={"model": MODEL, "stream": mode != "json", "messages": MESSAGES, **options})
                            self.assertEqual(r.status_code, 400)
                            self.assertEqual(r.json()["error"]["type"], "invalid_request_error")
                            self.assertIn("audio", r.json()["error"]["message"].lower())
                            self.assertNotIn("x-resume-id", r.headers)
                            self.assertEqual(self.sent, [])

    async def test_null_controls_and_explicit_text_only_keep_generating(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for modes in (None, ["text"]):
                self.sent.clear()
                body = {"model": MODEL, "modalities": modes, "audio": None,
                        "messages": [*MESSAGES, {"role": "assistant", "content": "prior", "audio": None},
                                     {"role": "user", "content": "hello"}]}
                r = await self.client.post("/v1/chat/completions", json=body)
                self.assertEqual(r.status_code, 200)
                self.assertEqual(len(self.sent), 1)
