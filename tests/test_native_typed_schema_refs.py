"""Translate typed root definition references only where the Gemini Schema has that field."""
from unittest.mock import patch

from google.genai import types

import test_native_typed_schema_aliases as alias_fixtures
from test_schema_contract_fidelity import SchemaRouteHarness, property_of


class NativeTypedReferences(SchemaRouteHarness):
    typed = alias_fixtures.NativeTypedSchemaRoutes.typed
    schema_on_wire = alias_fixtures.NativeTypedSchemaRoutes.schema_on_wire

    async def test_sdk_typed_refs_match_an_inlined_schema_and_leave_application_values_intact(self):
        self.channel = "web_proxy"
        literal = {"ref": "#/defs/Missing", "defs": {"literal": True}, "$ref": "#/defs/irrelevant"}
        # The literal percent-encoded-looking key must not be decoded twice.
        name = "宠/物~%2F"
        definition = types.Schema(type="OBJECT", properties={"ref": types.Schema(type="STRING", min_length=3)},
                                  default=literal)
        typed = types.Schema(type="OBJECT", properties={"defs": types.Schema(ref="#/defs/%E5%AE%A0~1%E7%89%A9~0%252F")},
                             defs={name: types.Schema(ref="#/defs/Nested"), "Nested": definition})
        for purpose in ("tool", "response"):
            for aliases in (False, True):
                with self.subTest(purpose=purpose, aliases=aliases):
                    response = await self.typed(purpose, typed.model_dump(mode="json", by_alias=aliases, exclude_none=True))
                    self.assertEqual(response.status_code, 200, response.text)
                    wire = self.schema_on_wire(purpose)
                    leaf = property_of(wire, "defs")
                    self.assertEqual(leaf["default"], literal)
                    self.assertEqual(property_of(leaf, "ref")["minLength"], 3)
                    self.assertNotIn("$defs", wire)
                    control = types.Schema(type="OBJECT", properties={"defs": definition})
                    reply = await self.typed(purpose, control.model_dump(mode="json", by_alias=aliases, exclude_none=True))
                    self.assertEqual(reply.status_code, 200, reply.text)
                    self.assertEqual(self.schema_on_wire(purpose), wire)

    async def test_invalid_refs_and_ambiguous_or_external_targets_are_not_dispatched(self):
        self.channel = "web_proxy"
        cases = ({"ref": "https://example.invalid/#/defs/P", "defs": {"P": {"type": "STRING"}}},
                 {"ref": "#/properties/P", "properties": {"P": {"type": "STRING"}}},
                 {"ref": "#/defs/P", "defs": {}},
                 {"ref": "#/defs/P", "defs": {"P": {"ref": "#/defs/P"}}},
                 {"ref": "#/defs/P", "defs": {"P": {"type": "STRING"}}, "type": "BOOLEAN"},
                 {"ref": "#/defs/P", "$ref": "#/$defs/Q", "defs": {}, "$defs": {}},
                 {"ref": "#/defs/P", "defs": {"P": {"type": "STRING"}}, "$defs": {}},
                 {"ref": "#/defs/%GG", "defs": {"%GG": {"type": "STRING"}}})
        for purpose in ("tool", "response"):
            for schema in cases:
                with self.subTest(purpose=purpose, schema=schema):
                    reply = await self.typed(purpose, schema)
                    self.assertEqual(reply.status_code, 400, reply.text)
                    self.assertEqual(self.sent, [])
            with patch("schema_validation.MAX_SCHEMA_NODES", 8):
                reply = await self.typed(purpose, {"type": "OBJECT", "properties": {
                    str(i): {"ref": "#/defs/P"} for i in range(5)}, "defs": {"P": {"type": "STRING"}}})
                self.assertEqual(reply.status_code, 400, reply.text)
                self.assertEqual(self.sent, [])
            with patch("schema_validation.MAX_SCHEMA_SERIALIZED_BYTES", 450):
                reply = await self.typed(purpose, {"ref": "#/defs/P", "defs": {
                    "P": {"type": "STRING"}, "unused": {"type": "STRING", "description": "x" * 500}}})
                self.assertEqual(reply.status_code, 400, reply.text)
                self.assertEqual(self.sent, [])

    async def test_json_carriers_do_not_reinterpret_typed_ref_spelling(self):
        self.channel = "web_proxy"
        for purpose in ("tool", "response"):
            result = await self.send_schema("gemini", purpose, {
                "type": "object", "properties": {"P": {"ref": "#/defs/Other"}},
                "defs": {"Other": {"type": "string"}}})
            self.assertEqual(result.status_code, 400, result.text)
            self.assertEqual(self.sent, [])
