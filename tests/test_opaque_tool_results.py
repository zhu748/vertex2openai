"""Tool results may be arbitrary text; parse JSON only when both wires can carry it."""
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness


class OpaqueToolResultRoutes(SchemaRouteHarness):
    async def test_nonfinite_looking_results_stay_verbatim_and_finite_containers_stay_structured(self):
        cases = [
            ('  {"measurements":[{"value":NaN}]}\n', None),
            ('[Infinity, "raw"]', None),
            ('{"measurement":-Infinity}', None),
            (' {"measurement":1e999} ', None),
            ('[{"value":-1e999}]', None),
            ('{"measurement":[-1.5e2, null], "label":"NaN"}', {"measurement": [-150.0, None], "label": "NaN"}),
            ('[1, {"temperature":12.5}]', {"result": [1, {"temperature": 12.5}]}),
        ]
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                for raw, structured in cases:
                    with self.subTest(channel=channel, protocol=protocol, raw=raw):
                        body = {"model": MODEL, "messages": list(MESSAGES)}
                        if protocol == "openai":
                            path = "/v1/chat/completions"
                            body["messages"] += [
                                {"role": "assistant", "tool_calls": [{"id": "m1", "type": "function",
                                    "function": {"name": "measure", "arguments": "{}"}}]},
                                {"role": "tool", "tool_call_id": "m1", "content": raw},
                            ]
                        else:
                            path = "/v1/messages"
                            body["max_tokens"] = 48
                            body["messages"] += [
                                {"role": "assistant", "content": [{"type": "tool_use", "id": "m1",
                                    "name": "measure", "input": {}}]},
                                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "m1",
                                    "content": raw}]},
                            ]
                        self.sent.clear()
                        reply = await self.client.post(path, json=body)
                        self.assertEqual(reply.status_code, 200, reply.text[:256])
                        self.assertEqual(len(self.sent), 1)
                        wire = self.sent[0].get("variables", self.sent[0])
                        responses = [p["functionResponse"] for m in wire["contents"] for p in m["parts"]
                                     if "functionResponse" in p]
                        self.assertEqual(len(responses), 1)
                        self.assertEqual(responses[0]["name"], "measure")
                        self.assertEqual(responses[0]["response"], structured if structured is not None else {"result": raw})
