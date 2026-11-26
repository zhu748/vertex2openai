"""Typed Gemini Schema aliases on the real Cookie bridge, not arbitrary application JSON."""
from copy import deepcopy
from unittest.mock import patch

import pytest

from google.genai import types

from schema_validation import SchemaValidationError
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness, property_of
from upstreams import cookie_native

LITERAL = {"min_items": "unchanged", "minItems": {"any_of": [1, 2]}}
ENUM = ["min_items", "minItems"]
NAMES = ["any_of", "anyOf", "max_items"]


def public_schema():
    return types.Schema(
        type="OBJECT", min_properties=1, max_properties=3, required=NAMES,
        property_ordering=NAMES, default=LITERAL, example=LITERAL,
        defs={"min_length": types.Schema(type="STRING", min_length=3)},
        properties={
            "any_of": types.Schema(any_of=[types.Schema(type="STRING", enum=ENUM),
                                           types.Schema(type="INTEGER")]),
            "anyOf": types.Schema(type="ARRAY", min_items=1, max_items=4,
                                  items=types.Schema(type="STRING", min_length=1, max_length=7)),
            "max_items": types.Schema(type="OBJECT", additional_properties=types.Schema(
                type="ARRAY", items=types.Schema(type="BOOLEAN"), min_items=2))})


def test_typed_walk_is_copy_on_change_bounded_and_does_not_visit_literals():
    schema = public_schema()
    canonical = schema.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert cookie_native._normalize_typed_schema(canonical) is canonical
    source = schema.model_dump(mode="json", exclude_none=True)
    untouched = deepcopy(source)
    result = cookie_native._normalize_typed_schema(source)
    assert source == untouched and result == canonical
    assert result["default"] is source["default"] and result["example"] is source["example"]
    with patch.object(cookie_native, "MAX_SCHEMA_NODES", 2):
        literal = {"type": "OBJECT", "default": canonical}
        assert cookie_native._normalize_typed_schema(literal) is literal
        with pytest.raises(SchemaValidationError):
            cookie_native._normalize_typed_schema({"any_of": [{"type": "STRING"}, {"type": "OBJECT"}]})
    with patch.object(cookie_native, "MAX_SCHEMA_DEPTH", 1), pytest.raises(SchemaValidationError):
        cookie_native._normalize_typed_schema({"properties": {"x": {"items": {"min_length": 1}}}})


class NativeTypedSchemaRoutes(SchemaRouteHarness):
    async def typed(self, purpose, schema):
        self.sent.clear()
        body = {"contents": [{"role": "user", "parts": [{"text": "choose"}]}]}
        if purpose == "tool":
            body["tools"] = [{"function_declarations": [{"name": "choose", "parameters": schema}]}]
        else:
            body["generation_config"] = {"response_mime_type": "application/json", "response_schema": schema}
        return await self.client.post(f"/v1beta/models/{MODEL}:generateContent", json=body)

    def schema_on_wire(self, purpose):
        self.assertEqual(len(self.sent), 1)
        variables = self.sent[0]["variables"]
        return (variables["tools"][0]["functionDeclarations"][0]["parameters"]
                if purpose == "tool" else variables["generationConfig"]["responseSchema"])

    async def test_public_schema_dumps_are_equivalent_and_literal_keys_stay_opaque(self):
        self.channel = "web_proxy"
        schema = public_schema()
        for purpose in ("tool", "response"):
            with self.subTest(purpose=purpose):
                control = await self.typed(purpose, schema.model_dump(mode="json", by_alias=True, exclude_none=True))
                self.assertEqual(control.status_code, 200, control.text)
                expected = self.schema_on_wire(purpose)
                response = await self.typed(purpose, schema.model_dump(mode="json", exclude_none=True))
                self.assertEqual(response.status_code, 200, response.text)
                wire = self.schema_on_wire(purpose)
                self.assertEqual(wire, expected)
                self.assertEqual([p["key"] for p in wire["properties"]], NAMES)
                self.assertEqual(wire["propertyOrdering"], NAMES)
                self.assertEqual(wire["required"], NAMES)
                self.assertEqual(wire["default"], LITERAL)
                self.assertEqual(wire["example"], LITERAL)
                self.assertEqual(wire["defs"]["min_length"]["minLength"], 3)
                self.assertEqual(property_of(wire, "any_of")["anyOf"][0]["enum"], ENUM)
                self.assertEqual(property_of(wire, "anyOf")["items"]["maxLength"], 7)
                self.assertEqual(property_of(wire, "max_items")["additionalProperties"]["minItems"], 2)

    async def test_colliding_typed_aliases_rejected_without_dispatch_in_either_order(self):
        self.channel = "web_proxy"
        for purpose in ("tool", "response"):
            for fields in ([("min_length", 2), ("minLength", 2)], [("minLength", 2), ("min_length", None)]):
                with self.subTest(purpose=purpose, fields=fields):
                    schema = {"type": "OBJECT", "properties": {"min_length": {"type": "ARRAY", "items": {
                        "type": "STRING", **dict(fields)}}}}
                    response = await self.typed(purpose, schema)
                    self.assertEqual(response.status_code, 400, response.text)
                    self.assertIn("minLength", response.json()["error"]["message"])
                    self.assertEqual(self.sent, [])

    async def test_raw_json_schema_is_not_reinterpreted_as_a_proto_message(self):
        self.channel = "web_proxy"
        for purpose in ("tool", "response"):
            with self.subTest(purpose=purpose):
                raw = {"type": "object", "properties": {"any_of": {"any_of": [{"type": "string"}]}}}
                result = await self.send_schema("gemini", purpose, raw)
                self.assertEqual(result.status_code, 400)
                self.assertIn("any_of", result.json()["error"]["message"])
                self.assertEqual(self.sent, [])
                valid = {"type": "object", "properties": {"any_of": {"type": "string", "enum": ENUM}},
                         "default": deepcopy(LITERAL), "example": deepcopy(LITERAL)}
                result = await self.send_schema("gemini", purpose, valid)
                self.assertEqual(result.status_code, 200, result.text)
                wire = self.schema_on_wire(purpose)
                self.assertEqual(wire["default"], LITERAL)
                self.assertEqual(wire["example"], LITERAL)
                self.assertEqual(property_of(wire, "any_of")["enum"], ENUM)
