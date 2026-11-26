"""A tuple restricts positions, not all items to the union of its prefix."""
from copy import deepcopy
from unittest.mock import patch

from routes import chat_api, claude_api

from test_schema_contract_fidelity import SchemaRouteHarness, nested, property_of


class PositionalSchemaRoutes(SchemaRouteHarness):
    async def test_invalid_tuple_precedes_buffer_lookup_and_mixed_credential_dispatch(self):
        self.channel = "mixed"
        self.settings["resumable_enabled"] = True
        self.client.headers.update({"X-Resumable": "true", "X-Resume-Id": "r5-existing"})
        schema = nested({"type": "array", "items": [{"type": "string"}, {"type": "integer"}]})
        with patch.object(chat_api, "resolve_buffer", wraps=chat_api.resolve_buffer) as lookup, \
             patch.object(chat_api.credential_manager, "get_mixed_record", return_value=None) as select, \
             patch.object(claude_api, "call_upstream", wraps=claude_api.call_upstream) as dispatch:
            for protocol in ("openai", "claude"):
                for purpose in ("tool", "response"):
                    for stream in (False, True):
                        with self.subTest(protocol=protocol, purpose=purpose, stream=stream):
                            rep = await self.send_schema(protocol, purpose, schema, stream=stream)
                            self.assertEqual(rep.status_code, 400, rep.text[:300])
                            self.assertIn("currency.items", rep.json()["error"]["message"])
            lookup.assert_not_called()
            select.assert_not_called()
            dispatch.assert_not_called()
            self.assertEqual(self.sent, [])

    async def test_unrepresentable_tuples_fail_and_proven_uniform_arrays_keep_their_contract(self):
        string, integer = {"type": "string"}, {"type": "integer"}
        cases = [
            # Even a one-element prefix leaves subsequent items unrestricted.
            ({"type": "array", "items": [string, integer]}, None),
            ({"type": "array", "items": [string]}, None),
            ({"type": "array", "items": [string, integer], "minItems": 2, "maxItems": 2}, None),
            ({"type": "array", "items": [integer, integer], "maxItems": 3}, None),
            # An explicit upper bound lets us prove exact, non-positional cases.
            ({"type": "array", "items": [integer, integer], "maxItems": 2}, ("INTEGER", 2)),
            ({"type": "array", "items": [string, integer], "maxItems": 1}, ("STRING", 1)),
            ({"type": "array", "items": [string, integer], "maxItems": 0}, (None, 0)),
            ({"type": "array", "items": integer, "maxItems": 8}, ("INTEGER", 8)),
        ]
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in (("openai", "claude") if channel == "api_key" else ("openai", "claude", "gemini")):
                for purpose in ("tool", "response"):
                    for schema, expected in cases:
                        with self.subTest(channel=channel, protocol=protocol, purpose=purpose, schema=schema):
                            original = deepcopy(schema)
                            resp = await self.send_schema(protocol, purpose, nested(schema), stream=expected is None)
                            self.assertEqual(schema, original)
                            if expected is None:
                                self.assertEqual(resp.status_code, 400, resp.text[:320])
                                msg = resp.json()["error"]["message"]
                                self.assertIn("currency.items", msg)
                                self.assertIn("position", msg)
                                self.assertEqual(self.sent, [])
                                continue
                            self.assertEqual(resp.status_code, 200, resp.text[:320])
                            self.assertEqual(len(self.sent), 1)
                            wire = self.sent[0].get("variables", self.sent[0])
                            root = (wire["tools"][0]["functionDeclarations"][0]["parameters"] if purpose == "tool"
                                    else wire["generationConfig"]["responseSchema"])
                            leaf = property_of(property_of(root, "quotes")["items"], "currency")
                            self.assertEqual(leaf["type"], "ARRAY")
                            self.assertEqual(int(leaf.get("maxItems", leaf.get("max_items"))), expected[1])
                            self.assertEqual(leaf.get("items"), {"type": expected[0]} if expected[0] else None)
