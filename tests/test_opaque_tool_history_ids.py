"""Provider IDs are opaque even if they happen to resemble a legacy history carrier."""
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from thought_signatures import replay_tool_call_id, resolve_tool_call_signature


class OpaqueHistoryIDs(SchemaRouteHarness):
    async def test_exact_emitted_ids_survive_sibling_reordering_and_both_adapters(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                for suffixes in (("_left", "_right"), ("YQ==", "Yg==")):
                    ids = [f"{channel}-{protocol}__thought__{suffix}" for suffix in suffixes]
                    with self.subTest(channel=channel, protocol=protocol, suffixes=suffixes):
                        path, body = _request(protocol, "auto")
                        body["messages"] = [{"role": "user", "content": "offline"}]
                        self.reply = [{"functionCall": {"id": ident, "name": "weather", "args": {}}} for ident in ids]
                        first = await self.client.post(path, json=body)
                        self.assertEqual(first.status_code, 200)
                        if protocol == "openai":
                            assistant = first.json()["choices"][0]["message"]
                            emitted = [call["id"] for call in assistant["tool_calls"]]
                            body["messages"] += [assistant, *({"role": "tool", "tool_call_id": i, "content": "ok"}
                                                              for i in reversed(emitted))]
                        else:
                            blocks = first.json()["content"]
                            emitted = [block["id"] for block in blocks if block["type"] == "tool_use"]
                            body["messages"] += [{"role": "assistant", "content": blocks},
                                                 {"role": "user", "content": [{"type": "tool_result", "tool_use_id": i,
                                                  "content": "ok"} for i in reversed(emitted)]}]
                        self.assertEqual(emitted, ids)
                        self.sent.clear()
                        self.reply = [{"text": "OK"}]
                        second = await self.client.post(path, json=body)
                        self.assertEqual(second.status_code, 200)
                        self.assertEqual(len(self.sent), 1)
                        wire = self.sent[0].get("variables", self.sent[0])
                        parts = [p for c in wire["contents"] for p in c["parts"]]
                        for field in ("functionCall", "functionResponse"):
                            self.assertEqual([p[field]["id"] for p in parts if field in p], ids)

    def test_only_canonical_nonempty_unregistered_historical_ids_are_decoded(self):
        for ident in ("unseen__thought__", "unseen__thought__YQ", "__thought__YQ==", "unseen__thought__YV==",
                      "unseen__thought__YQ==\n", "unseen__thought__a/b?"):
            with self.subTest(ident=ident):
                self.assertEqual(replay_tool_call_id(ident), ident)
                self.assertEqual(resolve_tool_call_signature(ident), (ident, None))
        self.assertEqual(resolve_tool_call_signature("historic__thought__YQ=="), ("historic", b"a"))
        self.assertEqual(resolve_tool_call_signature("historic__thought__YQ==", explicit_signature=b"explicit"),
                         ("historic", b"explicit"))
