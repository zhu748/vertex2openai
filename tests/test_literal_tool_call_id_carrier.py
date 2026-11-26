"""Public OpenAI tool-call IDs round-trip without treating opaque suffix bytes as signatures."""
import base64
from copy import deepcopy
import hashlib
import json
from unittest.mock import patch

from signature_store import signature_store
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness


def _assistant(reply, stream):
    if not stream:
        return reply.json()["choices"][0]["message"]
    calls = {}
    message = {"role": "assistant", "content": None}
    for line in reply.text.splitlines():
        if not line.startswith("data: {"):
            continue
        event = json.loads(line[6:])
        assert "error" not in event, event
        for choice in event.get("choices", []):
            delta = choice["delta"]
            if delta.get("extra_content"):
                message["extra_content"] = delta["extra_content"]
            for d in delta.get("tool_calls", ()):
                c = calls.setdefault(d["index"], {"type": "function", "function": {"arguments": ""}})
                for key in ("id", "extra_content"):
                    if d.get(key):
                        c[key] = d[key]
                fn = d.get("function", {})
                if fn.get("name"):
                    c["function"]["name"] = fn["name"]
                c["function"]["arguments"] += fn.get("arguments", "")
    message["tool_calls"] = [calls[i] for i in sorted(calls)]
    return message


def _result(ident, n=0):
    return {"role": "tool", "tool_call_id": ident, "content": json.dumps({"n": n})}


def _signature(part):
    return base64.urlsafe_b64decode(part["thoughtSignature"]) if "thoughtSignature" in part else None


class LiteralToolIDCarrier(SchemaRouteHarness):
    async def emit(self, ids, signatures, mode="json", protocol="openai"):
        self.settings["fake_streaming"] = mode == "fake"
        path, body = _request(protocol, "auto", stream=mode != "json")
        body["model"] = "gemini-3-flash-preview"
        self.reply = [
            {"functionCall": {"id": ident, "name": "weather", "args": {"city": str(i)}},
             **({"thoughtSignature": base64.b64encode(sig).decode()} if sig else {})}
            for i, (ident, sig) in enumerate(zip(ids, signatures, strict=True))
        ]
        reply = await self.client.post(path, json=body)
        self.assertEqual(reply.status_code, 200, reply.text)
        return reply

    async def replay(self, messages, *, status=200, protocol="openai"):
        path, body = _request(protocol, "auto")
        body.update(model="gemini-3-flash-preview", messages=[*body["messages"], *messages])
        self.reply = {"text": "done"}
        self.sent.clear()
        reply = await self.client.post(path, json=body)
        self.assertEqual(reply.status_code, status, reply.text)
        if status != 200:
            self.assertFalse(self.sent)
            self.assertIn("Required signature missing", reply.json()["error"]["message"])
            return
        self.assertEqual(len(self.sent), 1)
        wire = self.sent[0].get("variables", self.sent[0])
        parts = [p for msg in wire["contents"] for p in msg["parts"]]
        return [p for p in parts if "functionCall" in p], [p["functionResponse"] for p in parts if "functionResponse" in p]

    async def test_cold_echo_all_modes_exact_call_and_reversed_result_identity(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for mode in ("json", "stream", "fake"):
                prefix = f"literal-modern-{channel}-{mode}"
                ids = [prefix + suffix for suffix in ("__thought__YQ==", "__thought__Yg==", "-ordinary")]
                signatures = [b"provider-first", None, b"provider-final"]
                assistant = _assistant(await self.emit(ids, signatures, mode), mode != "json")
                for i, call in enumerate(assistant["tool_calls"]):
                    self.assertEqual(call["id"], ids[i])
                    marker = call.get("extra_content", {}).get("vertex2openai")
                    expected = {"literal_call_id_sha256_v1": hashlib.sha256(ids[i].encode()).hexdigest()}
                    self.assertEqual(marker, expected if i < 2 else None)
                turns = [assistant, *(_result(ids[i], i) for i in (2, 1, 0))]
                for ttl in (signature_store._ttl, -1):
                    with self.subTest(channel=channel, mode=mode, cold=ttl == -1), patch.object(signature_store, "_ttl", ttl):
                        calls, results = await self.replay(turns)
                        self.assertEqual([c["functionCall"]["id"] for c in calls], ids)
                        self.assertEqual([_signature(c) for c in calls], signatures)
                        self.assertEqual([(r["id"], r["name"], r["response"]["n"]) for r in results],
                                         [(ident, "weather", i) for i, ident in enumerate(ids)])

    async def test_partial_interrupted_and_reused_id_declarations_do_not_leak_identity(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            base = "id-boundary-" + channel
            ids = [base + "__thought__" + suffix for suffix in ("YQ==", "Yg==", "Yw==")]
            first = _assistant(await self.emit(ids, [b"actual", None, None]), False)
            with patch.object(signature_store, "_ttl", -1):
                for indices in ((1, 0), (1, 1, 0)):
                    calls, results = await self.replay([first, *(_result(ids[i], i) for i in indices)])
                    self.assertEqual([c["functionCall"]["id"] for c in calls], ids)
                    self.assertEqual([r["id"] for r in results], [ids[i] for i in indices])
                _, results = await self.replay([first, _result(ids[1]), {"role": "user", "content": "new turn"},
                                                _result(ids[0])])
                self.assertEqual([r["id"] for r in results], [ids[1], base])
                # An actual old carrier, even with a valid explicit signature and
                # modern-looking *message* metadata, is NOT a literal declaration.
                old = deepcopy(first)
                old["tool_calls"] = [old["tool_calls"][0]]
                old["tool_calls"][0]["extra_content"].pop("vertex2openai")
                calls, results = await self.replay([first, *(_result(i) for i in ids), old, _result(ids[0])])
                self.assertEqual([c["functionCall"]["id"] for c in calls], [*ids, base])
                self.assertEqual([r["id"] for r in results], [*ids, base])
                self.assertEqual(_signature(calls[-1]), b"actual")
                old["tool_calls"][0].pop("extra_content")
                calls, results = await self.replay([old, _result(ids[0])])
                self.assertEqual((calls[0]["functionCall"]["id"], _signature(calls[0]), results[0]["id"]),
                                 (base, b"a", base))

    async def test_edited_id_ignores_stale_hint_and_marker_alone_cannot_supply_required_bytes(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            ident = f"edited-{channel}__thought__YQ=="
            first = _assistant(await self.emit([ident], [b"explicit"]), False)
            supplied = deepcopy(first)
            supplied["tool_calls"][0]["extra_content"]["google"]["thought_signature"] = base64.b64encode(b"replacement").decode()
            calls, results = await self.replay([supplied, _result(ident)])
            self.assertEqual((calls[0]["functionCall"]["id"], _signature(calls[0]), results[0]["id"]),
                             (ident, b"replacement", ident))
            with patch.object(signature_store, "_ttl", -1):
                edited = deepcopy(first)
                edited["tool_calls"][0]["id"] = "old__thought__Yg=="
                calls, results = await self.replay([edited, _result("old__thought__Yg==")])
                self.assertEqual((calls[0]["functionCall"]["id"], _signature(calls[0]), results[0]["id"]),
                                 ("old", b"explicit", "old"))
                first["tool_calls"][0]["extra_content"].pop("google")
                first["tool_calls"][0]["function"]["arguments"] = '{"city":"different"}'
                turns = [first, _result(ident)]
                with patch("config.GEMINI_STRICT_THOUGHT_SIGNATURES", False):
                    calls, results = await self.replay(turns)
                    self.assertEqual((calls[0]["functionCall"]["id"], _signature(calls[0]), results[0]["id"]),
                                     (ident, None, ident))
                with patch("config.GEMINI_STRICT_THOUGHT_SIGNATURES", True):
                    await self.replay(turns, status=400)

    async def test_standard_claude_ids_remain_literal_even_on_unmarked_cold_history(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            base = "strict-old-" + channel
            ident = base + "__thought__Yg=="
            for mode in ("json", "stream", "fake"):
                reply = await self.emit([ident], [b"actual"], mode, protocol="claude")
                self.assertNotIn("literal_call_id_sha256_v1", reply.text)
                if mode == "json":
                    blocks = reply.json()["content"]
                    self.assertEqual(set(blocks[0]), {"type", "id", "name", "input"})
                    with patch.object(signature_store, "_ttl", -1):
                        calls, results = await self.replay([
                            {"role": "assistant", "content": blocks},
                            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": ident, "content": "done"}]},
                        ], protocol="claude")
                    self.assertEqual((calls[0]["functionCall"]["id"], _signature(calls[0]), results[0]["id"]),
                                     (ident, None, ident))
