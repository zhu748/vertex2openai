"""Neither translated transport can honor options on its bare search declaration."""
from unittest.mock import patch

from routes import chat_api
from test_parallel_tool_controls import MODES, _request
from test_schema_contract_fidelity import SchemaRouteHarness


class BuiltinSearchOptions(SchemaRouteHarness):
    async def test_active_options_rejected_before_replay_or_provider(self):
        restricted = (
            {"type": "web_search", "filters": {"allowed_domains": ["example.invalid"]}},
            {"type": "web_search", "filters": {"allowed_domains": []}},
            {"type": "web_search_preview", "user_location": {"type": "approximate", "country": "CA"}},
            {"google_search": {"exclude_domains": ["example.invalid"]}},
            {"googleSearch": {"excludeDomains": ["example.invalid"]}},
            {"name": "web_search", "search_context_size": "low"},
            {"type": "web_search", "external_web_access": False},
        )
        headers = {"X-Resumable": "true", "Idempotency-Key": "search-options"}
        with (patch.object(chat_api, "_resumable_json") as buf_json,
              patch.object(chat_api, "_resumable_stream") as buf_stream):
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                for tool in restricted:
                    for stream, fake in MODES:
                        with self.subTest(channel=channel, tool=tool, stream=stream, fake=fake):
                            path, body = _request("openai", "auto", stream=stream)
                            self.settings["fake_streaming"] = fake
                            reply = await self.client.post(path, headers=headers, json={**body, "tools": [tool]})
                            self.assertEqual(reply.status_code, 400, reply.text[:240])
                            self.assertIn("tools[0]", reply.json()["error"]["message"])
            buf_json.assert_not_called()
            buf_stream.assert_not_called()
        self.assertEqual(self.sent, [])

    async def test_bare_aliases_neutral_options_and_search_named_client_function_still_work(self):
        tools = ({"type": "web_search"}, {"type": "web_search_preview", "user_location": None, "filters": {}},
                 {"type": "google_search", "search_context_size": None}, {"googleSearch": {}},
                 {"google_search": {"exclude_domains": []}}, {"name": "search"})
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for tool in tools:
                with self.subTest(channel=channel, tool=tool):
                    path, body = _request("openai", "auto")
                    self.sent.clear()
                    reply = await self.client.post(path, json={**body, "tools": [tool]})
                    self.assertEqual(reply.status_code, 200, reply.text[:240])
                    self.assertEqual(len(self.sent), 1)
                    wire = self.sent[0].get("variables", self.sent[0])
                    self.assertEqual(wire["tools"], [{"googleSearch": {}}])
            path, body = _request("openai", "auto")
            body["tools"][0]["function"]["name"] = "web_search"
            self.sent.clear()
            reply = await self.client.post(path, json=body)
            self.assertEqual(reply.status_code, 200, reply.text[:240])
            self.assertEqual(len(self.sent), 1)
            wire = self.sent[0].get("variables", self.sent[0])
            self.assertEqual(wire["tools"][0]["functionDeclarations"][0]["name"], "web_search")

    async def test_chat_level_options_never_activate_or_widen_a_search(self):
        headers = {"X-Resumable": "true", "Idempotency-Key": "top-level-search"}
        with (patch.object(chat_api, "_resumable_json") as buf_json,
              patch.object(chat_api, "_resumable_stream") as buf_stream):
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                for selector in ("neither", "tool", "suffix"):
                    path, body = _request("openai", None, tools=False)
                    if selector == "tool":
                        body["tools"] = [{"type": "web_search"}]
                    if selector == "suffix":
                        body["model"] += "-search"
                    for value in ({"user_location": {"type": "approximate", "approximate": {"country": "CA"}}},
                                  {"search_context_size": "low"}, [], False):
                        for stream, fake in MODES:
                            with self.subTest(channel=channel, selector=selector, option=value, mode=(stream, fake)):
                                self.settings["fake_streaming"] = fake
                                reply = await self.client.post(path, headers=headers, json={
                                    **body, "stream": stream, "web_search_options": value})
                                self.assertEqual(reply.status_code, 400, reply.text[:256])
                                self.assertIn("web_search_options", reply.json()["error"]["message"])
        buf_json.assert_not_called()
        buf_stream.assert_not_called()
        self.assertEqual(self.sent, [])

    async def test_chat_null_or_empty_object_preserves_existing_activation(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for selector in ("neither", "tool", "suffix"):
                path, body = _request("openai", None, tools=False)
                if selector == "tool":
                    body["tools"] = [{"type": "web_search"}]
                if selector == "suffix":
                    body["model"] += "-search"
                for value in (None, {}):
                    with self.subTest(channel=channel, selector=selector, value=value):
                        self.sent.clear()
                        reply = await self.client.post(path, json={**body, "web_search_options": value})
                        self.assertEqual(reply.status_code, 200, reply.text[:256])
                        self.assertEqual(len(self.sent), 1)
                        wire = self.sent[0].get("variables", self.sent[0])
                        self.assertEqual(wire.get("tools", []), [{"googleSearch": {}}] if selector != "neither" else [])
