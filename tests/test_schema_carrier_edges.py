"""Use the SDK's serialized Vertex request, not just the intermediate schema dict."""
from test_schema_contract_fidelity import SchemaRouteHarness, nested, property_of


def _wire_schema(node):
    # Compare the schema, not whether the SDK spelled an int64 as a string or
    # used a proto field name. Never recurse into literal data (e.g. example).
    names = {"any_of": "anyOf", "min_length": "minLength"}
    node = {names.get(k, k): v for k, v in node.items()}
    if "minLength" in node:
        node["minLength"] = int(node["minLength"])
    if "anyOf" in node:
        node["anyOf"] = [_wire_schema(v) for v in node["anyOf"]]
    return node


def _cases(sdk):
    yield {"type": "null"}, {"type": "NULL"}
    yield {"type": ["null"], "title": "Missing"}, {"type": "NULL", "title": "Missing"}
    for extra in ([], [{"type": "integer", "minimum": 4}]):
        branches = [{"type": "string", "minLength": 2, "enum": ["red"]}, *extra, {"type": "null", "title": "Missing"}]
        yield {"anyOf": branches}, {"anyOf": [{**b, "type": b["type"].upper()} for b in branches]}
    types = ["string", "integer", "null"]
    yield {"type": types, "minLength": 2}, {"anyOf": [{"type": t.upper(), "minLength": 2} for t in types]}
    yield {"type": ["string", "null"], "enum": ["red"]}, {"type": "STRING", "enum": ["red"]}
    yield {"type": ["integer", "null"], "enum": ["red"]}, "enum"
    yield {"type": "string", "enum": ["red", "blue"], "examples": ["red"]}, {
        "type": "STRING", "enum": ["red", "blue"], **({"example": "red"} if sdk else {"examples": ["red"]})}
    sample = {"type": "null", "examples": ["literal data, not a schema"]}
    yield {"type": "object", "examples": [sample]}, {"type": "OBJECT", **({"example": sample} if sdk else {"examples": [sample]})}
    for examples in (["red", "blue"], [None]):
        yield {"type": "string", "examples": examples}, "examples" if sdk else {"type": "STRING", "examples": examples}


class SchemaCarrierEdges(SchemaRouteHarness):
    async def test_example_without_type_is_still_an_annotation_not_a_constraint(self):
        sample = {"id": "example only"}
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for purpose in ("response", "tool"):
                with self.subTest(channel=channel, purpose=purpose):
                    result = await self.send_schema("openai", purpose, nested({"examples": [sample]}))
                    self.assertEqual(result.status_code, 200, result.text[:300])
                    body = self.sent[0].get("variables", self.sent[0])
                    schema = (body["tools"][0]["functionDeclarations"][0]["parameters"] if purpose == "tool"
                              else body["generationConfig"]["responseSchema"])
                    leaf = _wire_schema(property_of(property_of(schema, "quotes")["items"], "currency"))
                    self.assertEqual(leaf["example"] if channel == "api_key" else leaf["examples"][0], sample)
                    allowed = {branch["type"].upper() for branch in leaf["anyOf"]}
                    if leaf.get("nullable"):
                        allowed.add("NULL")
                    self.assertEqual(allowed, {"BOOLEAN", "INTEGER", "NUMBER", "STRING", "ARRAY", "OBJECT", "NULL"})

    async def test_constraints_and_examples_at_the_actual_wire(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in (("openai", "claude") if channel == "api_key" else ("openai", "claude", "gemini")):
                for purpose in ("response", "tool"):
                    for schema, expected in _cases(channel == "api_key"):
                        with self.subTest(channel=channel, protocol=protocol, purpose=purpose, schema=schema):
                            reply = await self.send_schema(protocol, purpose, nested(schema))
                            if isinstance(expected, str):
                                self.assertEqual(reply.status_code, 400, reply.text[:300])
                                self.assertIn("currency." + expected, reply.json()["error"]["message"])
                                self.assertEqual(self.sent, [])
                                continue
                            self.assertEqual(reply.status_code, 200, reply.text[:300])
                            self.assertEqual(len(self.sent), 1)
                            body = self.sent[0].get("variables", self.sent[0])
                            wire = (body["tools"][0]["functionDeclarations"][0]["parameters"] if purpose == "tool"
                                    else body["generationConfig"]["responseSchema"])
                            leaf = property_of(property_of(wire, "quotes")["items"], "currency")
                            self.assertEqual(_wire_schema(leaf), expected)
