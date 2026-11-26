"""Local URI-fragment JSON Pointers must not select a different wire schema."""
from copy import deepcopy
from unittest.mock import patch

import pytest

from routes import chat_api, claude_api
from schema_validation import SchemaValidationError, sanitize_for_gemini
from test_schema_contract_fidelity import SchemaRouteHarness, property_of


DEFS = {
    "total price": {"type": "integer", "minimum": 0},
    "total%20price": {"type": "string"},
    "slash/key": {"type": "boolean"},
    "~1": {"type": "number"},
    "a+b": {"type": "boolean"},
    "雪": {"type": "string"},
    "01": {"type": "string"},
    "-1": {"type": "number"},
    "": {"type": "boolean"},
    "variants": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
}
# A URI's fragment is percent-decoded before splitting, then ~1 and ~0
# are interpreted once within each token. Array rules do not apply to map keys.
CASES = {
    "#/$defs/total%20price": ("integer", "total price"),
    "#/$defs/total%2520price": ("string", "total%20price"),
    "#%2F%24defs%2Fslash%7E1key": ("boolean", "slash/key"),
    "#/$defs/~01": ("number", "~1"),
    "#/$defs/a+b": ("boolean", "a+b"),
    "#/$defs/%E9%9B%AA": ("string", "雪"),
    "#/$defs/01": ("string", "01"),
    "#/$defs/-1": ("number", "-1"),
    "#/$defs/": ("boolean", ""),
    "#/$defs/variants/anyOf/%31": ("integer", None),
}


@pytest.mark.parametrize("ref,expected", CASES.items())
def test_local_pointer_selects_exact_definition(ref, expected):
    schema = {"$ref": ref, "$defs": DEFS}
    before = deepcopy(schema)
    typ, key = expected
    assert sanitize_for_gemini(schema) == (DEFS[key] if key is not None else {"type": typ})
    assert schema == before


@pytest.mark.parametrize("token", ["-1", "01", "00", "+1", " 1", "１", "-", "2", "0_1"])
def test_array_index_must_identify_an_existing_canonical_decimal_position(token):
    with pytest.raises(SchemaValidationError):
        sanitize_for_gemini({"$ref": f"#/$defs/variants/anyOf/{token}", "$defs": DEFS})


@pytest.mark.parametrize("key", ["invalid~", "invalid~2", "invalid%", "invalid%fg", "%FF", "%c3%28"])
def test_malformed_escapes_do_not_select_same_spelled_literal_keys(key):
    with pytest.raises(SchemaValidationError, match=r"\$ref"):
        sanitize_for_gemini({"$ref": "#/$defs/" + key, "$defs": {key: {"type": "string"}}})


def test_equivalent_spellings_cannot_escape_cycle_checks():
    with pytest.raises(SchemaValidationError, match=r"递归"):
        sanitize_for_gemini({"$ref": "#/$defs/a", "$defs": {"a": {"$ref": "#%2f%24defs/%61"}}})


class FragmentWireRoutes(SchemaRouteHarness):
    async def test_escaped_reference_types_survive_request_and_both_wire_grammars(self):
        schema = {"type": "object", "$defs": DEFS,
                  "properties": {f"p{i}": {"$ref": ref} for i, ref in enumerate(CASES)}}
        for protocol, purpose in self.routes():
            reply = await self.send_schema(protocol, purpose, schema)
            self.assertEqual(reply.status_code, 200, reply.text[:240])
            self.assertEqual(len(self.sent), 1)
            body = self.sent[0].get("variables", self.sent[0])
            wire = (body["tools"][0]["functionDeclarations"][0]["parameters"] if purpose == "tool"
                    else body["generationConfig"]["responseSchema"])
            for i, (typ, _) in enumerate(CASES.values()):
                self.assertEqual(property_of(wire, f"p{i}")["type"], typ.upper())
            self.assertEqual(property_of(wire, "p0")["minimum"], 0)

    async def test_invalid_array_ref_is_protocol_400_before_network(self):
        schema = {"type": "object", "$defs": DEFS,
                  "properties": {"value": {"$ref": "#/$defs/variants/anyOf/01"}}}
        for protocol, purpose in self.routes():
            reply = await self.send_schema(protocol, purpose, schema)
            self.assertEqual(reply.status_code, 400, reply.text[:240])
            error = reply.json()["error"]
            if protocol == "gemini":
                self.assertEqual(error["status"], "INVALID_ARGUMENT")
            else:
                self.assertEqual(error["type"], "invalid_request_error")
            self.assertEqual(self.sent, [])

    async def test_invalid_array_ref_precedes_replay_or_mixed_selection(self):
        self.channel = "mixed"
        self.settings["resumable_enabled"] = True
        self.client.headers.update({"X-Resumable": "true", "X-Resume-Id": "fragment-fixture"})
        schema = {"type": "object", "$defs": DEFS,
                  "properties": {"value": {"$ref": "#/$defs/variants/anyOf/-1"}}}
        with patch.object(chat_api, "resolve_buffer", wraps=chat_api.resolve_buffer) as lookup, \
                patch.object(chat_api.credential_manager, "get_mixed_record", return_value=None) as select, \
                patch.object(claude_api, "call_upstream", wraps=claude_api.call_upstream) as dispatch:
            for protocol in ("openai", "claude"):
                for purpose in ("tool", "response"):
                    for stream in (False, True):
                        with self.subTest(protocol=protocol, purpose=purpose, stream=stream):
                            reply = await self.send_schema(protocol, purpose, schema, stream=stream)
                            self.assertEqual(reply.status_code, 400, reply.text[:200])
            for step in (select, dispatch, lookup):
                step.assert_not_called()
            self.assertEqual(self.sent, [])
