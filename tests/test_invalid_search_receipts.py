"""OpenAI contract checks must win before a real, enabled replay store."""
from unittest.mock import patch

from resumable import ResumableStore
from routes import chat_api
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness


class InvalidSearchReceipts(SchemaRouteHarness):
    async def test_scoped_search_precedes_replay_and_missing_id(self):
        self.settings["resumable_enabled"] = True
        store = ResumableStore()
        self.addAsyncCleanup(store.aclose)
        path = "/v1/chat/completions"
        with patch.object(chat_api, "resumable_store", store):
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                for stream in (False, True):
                    body = {"model": MODEL, "messages": MESSAGES, "stream": stream}
                    for options in ({"web_search_options": {"search_context_size": "low"}},
                                    {"tools": [{"type": "web_search", "filters": {"allowed_domains": []}}]}):
                        for extra in ({}, {"X-Resume-ID": "not-a-receipt"}):
                            with self.subTest(channel=channel, stream=stream, options=options, extra=extra):
                                headers = {"X-Resumable": "true", **extra}
                                res = await self.client.post(path, headers=headers, json={**body, **options})
                                self.assertEqual(res.status_code, 400, res.text[:200])
                                self.assertEqual(res.json()["error"]["type"], "invalid_request_error")
                                self.assertNotIn("X-Resume-ID", res.headers)
            self.assertEqual(len(store._buffers), 0)
            self.assertEqual(self.sent, [])
            # A permitted selector still allocates, then reuses a genuine id.
            headers = {"X-Resumable": "true", "Idempotency-Key": "valid-search-receipt"}
            body = {"model": MODEL, "messages": MESSAGES, "tools": [{"type": "web_search"}],
                    "web_search_options": {}}
            first = await self.client.post(path, headers=headers, json=body)
            self.assertEqual(first.status_code, 200, first.text[:200])
            rid = first.headers["X-Resume-ID"]
            again = await self.client.post(path, headers={**headers, "X-Resume-ID": rid}, json=body)
            self.assertEqual((again.status_code, again.headers["X-Resume-ID"]), (200, rid))
            self.assertEqual(len(self.sent), 1)
