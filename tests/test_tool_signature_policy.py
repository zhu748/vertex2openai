"""Exercise the provider wire on real authenticated routes; no credentials, no network."""
import base64
from copy import deepcopy
from unittest.mock import patch

import config
import signature_origin
import signature_store as store_module
import thought_signatures
from test_literal_tool_call_id_carrier import _result, _signature
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_response

MODEL = "gemini-3-flash-preview"


def call(ident):
    return {"id": ident, "type": "function", "function": {"name": "weather", "arguments": "{}"}}


def history(ident, protocol):
    if protocol == "openai":
        return [{"role": "assistant", "content": None, "tool_calls": [call(ident)]}, _result(ident)]
    return [{"role": "assistant", "content": [{"type": "tool_use", "id": ident, "name": "weather", "input": {}}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": ident, "content": "ok"}]}]


class SignaturePolicyRoutes(SchemaRouteHarness):
    async def post(self, turns, *, protocol="openai", stream=False):
        path, body = _request(protocol, "auto", stream=stream)
        body.update(model=MODEL, messages=[*body["messages"], *turns])
        self.sent.clear()
        return await self.client.post(path, json=body)

    def parts(self):
        self.assertEqual(len(self.sent), 1)
        body = self.sent[0].get("variables", self.sent[0])
        return [p for c in body["contents"] for p in c["parts"]]

    async def test_substitute_in_public_metadata_never_masks_the_live_original_or_signs_follower(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            ids = [channel + "-placeholder-" + str(i) for i in range(2)]
            original = b"real\x00\xffprovider-bytes"
            self.reply = [
                {"functionCall": {"id": ident, "name": "weather", "args": {}},
                 **({"thoughtSignature": base64.b64encode(original).decode()} if i == 0 else {})}
                for i, ident in enumerate(ids)]
            first = await self.post([])
            m = first.json()["choices"][0]["message"]
            for known in store_module.SKIP_VALIDATOR_SENTINELS:
                for supplied in (known.decode(), base64.b64encode(known).decode()):
                    echo = deepcopy(m)
                    for c in echo["tool_calls"]:
                        c["extra_content"] = {"google": {"thought_signature": supplied}}
                    self.reply = [{"text": "done"}]
                    reply = await self.post([echo, *map(_result, ids)])
                    self.assertEqual(reply.status_code, 200, reply.text)
                    calls = [p for p in self.parts() if "functionCall" in p]
                    self.assertEqual([_signature(p) for p in calls], [original, None])

    async def test_hot_scoped_literal_wins_after_bare_id_lru_eviction_for_call_and_result(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            tiny = store_module.SignatureStore(max_entries=1)
            with (patch.object(thought_signatures, "signature_store", tiny), patch.object(signature_origin, "signature_store", tiny),
                  patch.object(cookie_response, "signature_store", tiny)):
                ident = channel + "-opaque__thought__Yg=="
                self.reply = [{"functionCall": {"id": ident, "name": "weather", "args": {}},
                               "thoughtSignature": base64.b64encode(b"origin").decode()}]
                self.assertEqual((await self.post([])).status_code, 200)
                self.assertIsNone(tiny.get_record(ident))  # Exact origin evicted the bare key.
                self.reply = [{"text": "done"}]
                out = await self.post(history(ident, "openai"))
                self.assertEqual(out.status_code, 200, out.text)
                parts = self.parts()
                c = next(p for p in parts if "functionCall" in p)
                r = next(p["functionResponse"] for p in parts if "functionResponse" in p)
                self.assertEqual((c["functionCall"]["id"], _signature(c), r["id"]), (ident, b"origin", ident))

    async def test_non_string_metadata_is_never_converted_to_claimed_gemini_bytes(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            h = history(channel + "-malformed", "openai")
            for v in (True, 42, {"field": "value"}, ["not", "bytes"]):
                h[0]["tool_calls"][0]["extra_content"] = {"google": {"thought_signature": v}}
                out = await self.post(h)
                self.assertEqual(out.status_code, 400, out.text)
                self.assertIn("thought_signature", out.json()["error"]["message"])
                self.assertEqual(self.sent, [])

    async def test_empty_and_result_messages_do_not_start_a_new_turn_but_fresh_prompt_does(self):
        self.reply = [{"text": "done"}]
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            old, active = channel + "-old-policy", channel + "-active-policy"
            old_history = history(old, "openai")
            # All missing calls in the retired turn are optional, even with strict policy.
            with patch.object(config, "GEMINI_STRICT_THOUGHT_SIGNATURES", True, create=True):
                out = await self.post([*old_history, {"role": "assistant", "content": "done"},
                                       {"role": "user", "content": [{"type": "text", "text": "new question"}]}])
                self.assertEqual(out.status_code, 200, out.text)
                self.assertNotIn("thoughtSignature", next(p for p in self.parts() if "functionCall" in p))
            # An empty user content is not put on the wire; it must not end active validation.
            out = await self.post([*old_history, {"role": "user", "content": []}, *history(active, "openai")])
            self.assertEqual(out.status_code, 200, out.text)
            self.assertEqual(len([p for p in self.parts() if "functionCall" in p and p.get("thoughtSignature")]), 2)

    async def test_strict_required_missing_is_actionable_without_provider_contact(self):
        self.settings["max_retries"] = 0
        with patch.object(config, "GEMINI_STRICT_THOUGHT_SIGNATURES", True, create=True):
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                for protocol in ("claude", "openai"):
                    for mode in ("json", "stream", "fake"):
                        self.settings["fake_streaming"] = mode == "fake"
                        with self.subTest(channel=channel, protocol=protocol, mode=mode):
                            ident = f"lost-{channel}-{protocol}-{mode}"
                            if protocol == "claude":
                                ident += "__thought__Yg=="  # Cold ID must not be accepted as proof.
                            out = await self.post(history(ident, protocol), protocol=protocol, stream=mode != "json")
                            self.assertEqual(self.sent, [])
                            self.assertEqual(out.status_code, 400, out.text[:500])
                            self.assertIn("Required signature missing", out.json()["error"]["message"])

    async def test_strict_validates_first_call_on_wire_and_does_not_require_unsigned_siblings(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            a, b = (channel + "-ordered-" + n for n in "ab")
            m = {"role": "assistant", "tool_calls": [call(a), call(b)], "extra_content": {"google": {
                "part_order": [{"type": "tool_call", "index": 1}, {"type": "tool_call", "index": 0}]}}}
            signed = {"google": {"thought_signature": base64.b64encode(b"carried").decode()}}
            m["tool_calls"][0]["extra_content"] = signed
            with patch.object(config, "GEMINI_STRICT_THOUGHT_SIGNATURES", True, create=True):
                self.reply = [{"text": "done"}]
                out = await self.post([m, _result(a), _result(b)])
                self.assertEqual(out.status_code, 400, out.text)
                self.assertFalse(self.sent)
                m["tool_calls"][0].pop("extra_content")
                m["tool_calls"][1]["extra_content"] = signed
                out = await self.post([m, _result(a), _result(b)])
                self.assertEqual(out.status_code, 200, out.text)
                self.assertEqual([(p["functionCall"]["id"], _signature(p))
                                  for p in self.parts() if "functionCall" in p], [(b, b"carried"), (a, None)])

    async def test_optional_parts_never_replay_a_client_placeholder_as_an_ordinary_signature(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            literal = store_module.SKIP_VALIDATOR_SENTINEL.decode()
            for google in ({"thought_signature": literal, "thought_signature_part": "text"},
                           {"ordinary_parts": [{"kind": "text", "text": "ok", "thought_signature": literal}]}):
                self.reply = [{"text": "done"}]
                out = await self.post([{"role": "assistant", "content": "ok", "extra_content": {"google": google}},
                                       {"role": "user", "content": "next"}])
                self.assertEqual(out.status_code, 200)
                self.assertFalse(any("thoughtSignature" in p for p in self.parts()))

    async def test_unencoded_historical_text_is_not_accepted_as_original_in_strict_mode(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            with patch.object(config, "GEMINI_STRICT_THOUGHT_SIGNATURES", True, create=True):
                h = history(channel + "-bad-strict", "openai")
                h[0]["tool_calls"][0]["extra_content"] = {"google": {"thought_signature": "not a Base64 signature"}}
                out = await self.post(h)
                self.assertEqual(out.status_code, 400)
                self.assertFalse(self.sent)


def test_private_origin_proves_modern_id_even_for_direct_resolver():
    tiny = store_module.SignatureStore(max_entries=1)
    key = b"verified-origin"
    tiny.register_call_signature(key, b"actual")
    with patch.object(thought_signatures, "signature_store", tiny):
        ident = "opaque__thought__Yg=="
        assert thought_signatures.resolve_tool_call_signature(ident, True, origin_key=key) == (ident, b"actual")


def test_openai_json_cannot_impersonate_the_private_claude_id_dialect():
    from models import OpenAIMessage

    msg = OpenAIMessage.model_validate({"role": "assistant", "tool_calls": [call("old__thought__Yg==")],
                                        "_claude_literal_tool_ids": True})
    assert not msg._claude_literal_tool_ids
