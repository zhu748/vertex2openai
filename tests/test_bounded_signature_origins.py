"""Proof budgets preserve original Gemini bytes without widening origin identity."""

import base64
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import patch

import signature_origin as origin
import signature_store as store_module
from signature_store import SignatureState, SignatureStore
from test_claude_buffered_tool_order import read_events, reconstruct
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from test_signature_origin import call, publish, replay, request, store  # noqa: F401, pytest fixture


MODEL = "gemini-3-flash-preview"


def provider_call(ident, signature=None, city="Paris"):
    part = {"functionCall": {"id": ident, "name": "weather", "args": {"city": city}}}
    if signature:
        part["thoughtSignature"] = base64.b64encode(signature).decode()
    return part


def wire_calls(sent):
    wire = sent.get("variables", sent)
    return [part for turn in wire["contents"] for part in turn["parts"] if "functionCall" in part]


def original(part):
    signature = part.get("thoughtSignature")
    return base64.urlsafe_b64decode(signature) if signature else None


def results(protocol, ids, content):
    if protocol == "claude":
        return [{"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": i, "content": content} for i in ids]}]
    return [{"role": "tool", "tool_call_id": i, "content": content} for i in ids]


class BoundedOriginRoutes(SchemaRouteHarness):
    def assistant_echo(self, response, protocol, mode):
        if protocol == "claude":
            blocks = (response.json()["content"] if mode == "json"
                      else reconstruct(self, read_events(response.text)))
            return {"role": "assistant", "content": blocks}
        if mode == "json":
            msg = response.json()["choices"][0]["message"]
            for item in msg["tool_calls"]:
                item.pop("extra_content", None)
            msg.pop("extra_content", None)
        else:
            text, reasoning, streamed = [], [], {}
            for line in response.text.splitlines():
                if not line.startswith("data: {"):
                    continue
                event = json.loads(line[6:])
                self.assertNotIn("error", event)
                for choice in event.get("choices", ()):
                    delta = choice["delta"]
                    text.append(delta.get("content") or "")
                    reasoning.append(delta.get("reasoning_content") or "")
                    for item in delta.get("tool_calls", ()):
                        call = streamed.setdefault(item["index"], {"type": "function", "function": {"arguments": ""}})
                        if item.get("id"):
                            call["id"] = item["id"]
                        fn = item["function"]
                        if fn.get("name"):
                            call["function"]["name"] = fn["name"]
                        call["function"]["arguments"] += fn.get("arguments", "")
            msg = {"role": "assistant", "content": "".join(text) or None,
                   "reasoning_content": "".join(reasoning) or None,
                   "tool_calls": [streamed[i] for i in sorted(streamed)]}
        # Some SDK dumps include these explicit nulls. Thin echoes need not
        # preserve the Google's optional OpenAI compatibility extension.
        msg.update(function_call=None, audio=None, annotations=None)
        return msg

    async def request_and_wire(self, path, body, parts):
        self.reply = parts
        self.sent.clear()
        response = await self.client.post(path, json=body)
        self.assertEqual(response.status_code, 200, response.text[:300])
        self.assertEqual(len(self.sent), 1)
        return response, wire_calls(self.sent[0])

    async def test_three_steps_preserve_large_originals_with_text_blocks_and_prior_thinking(self):
        self.settings["max_retries"] = 0
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("claude", "openai"):
                for mode in ("json", "fake", "stream"):
                    with self.subTest(channel=channel, protocol=protocol, mode=mode):
                        self.settings["fake_streaming"] = mode == "fake"
                        path, body = _request(protocol, "auto", stream=mode != "json")
                        earlier = ([{"type": "thinking", "thinking": "Check limits", "signature": "claude-summary"},
                                    {"type": "text", "text": "Ready"}] if protocol == "claude" else "Ready")
                        prior = {"role": "assistant", "content": earlier}
                        if protocol == "openai":
                            prior.update(reasoning_content="Check limits", audio=None,
                                         extra_content={"google": {"ordinary_parts": [
                                             {"kind": "thought", "text": "Check limits"}, {"kind": "text", "text": "Ready"}]}})
                        body.update(model=MODEL, messages=[{"role": "user", "content": "Setup"}, prior,
                                    {"role": "user", "content": [{"type": "text", "text": "Weather"},
                                                                 {"type": "text", "text": " precisely"}]}])
                        token = f"origin-r10-{channel}-{protocol}-{mode}"
                        a, b, c = (f"{token}-{n}" for n in "abc")
                        sig_a = b"\xff\x00first-" * 2304  # 18 KiB; original bytes need not be UTF-8.
                        sig_c = b"second-\xfe"
                        res, _ = await self.request_and_wire(path, body, [
                            provider_call(a, sig_a, "x" * (17 * 1024)), provider_call(b)])
                        first_step = self.assistant_echo(res, protocol, mode)
                        log = "剩" * 47000  # 141,000 UTF-8 bytes, not merely 47,000 characters.
                        body["messages"].extend([first_step, *results(protocol, (a, b), log)])
                        res, calls = await self.request_and_wire(path, body, [provider_call(c, sig_c)])
                        self.assertEqual([(p["functionCall"]["id"], original(p)) for p in calls],
                                         [(a, sig_a), (b, None)])
                        body["messages"].extend([self.assistant_echo(res, protocol, mode),
                                                 *results(protocol, (c,), "ok")])
                        body["stream"] = False
                        _res, calls = await self.request_and_wire(path, body, [{"text": "done"}])
                        self.assertEqual([(p["functionCall"]["id"], original(p)) for p in calls],
                                         [(a, sig_a), (b, None), (c, sig_c)])
                        self.assertEqual(calls[0]["functionCall"]["args"]["city"], "x" * (17 * 1024))
                        if mode == "json":
                            # The last byte of a full-length earlier result is
                            # identity evidence for the next tool step only.
                            if protocol == "claude":
                                body["messages"][4]["content"][0]["content"] = log[:-1] + "错"
                            else:
                                body["messages"][4]["content"] = log[:-1] + "错"
                            self.sent.clear()
                            bad = await self.client.post(path, json=body)
                            if bad.status_code == 400:  # strict missing-signature policy
                                self.assertFalse(self.sent)
                            else:
                                self.assertEqual(bad.status_code, 200, bad.text[:300])
                                changed = wire_calls(self.sent[0])
                                self.assertEqual([original(p) for p in changed], [sig_a, None, None])

    async def test_fragments_keep_proof_without_merging_wire_layout(self):
        for self.channel in ("api_key", "web_proxy"):
            for protocol in ("openai", "claude"):
                for mode in ("json", "fake", "stream"):
                    with self.subTest(channel=self.channel, protocol=protocol, mode=mode):
                        self.settings["fake_streaming"] = mode == "fake"
                        path, body = _request(protocol, "auto", stream=mode != "json")
                        body["model"] = MODEL
                        ident = f"r10-fragments-{self.channel}-{protocol}-{mode}"
                        sig = b"\x80\x03same-signed-call"
                        fragments = [{"text": f"{i}."} for i in range(540)]
                        res, _ = await self.request_and_wire(path, body, [*fragments, provider_call(ident, sig)])
                        if protocol == "openai" and mode == "json":
                            encoded = res.json()["choices"][0]["message"]["extra_content"]["google"]
                            self.assertEqual(len(encoded["ordinary_parts"]), len(fragments))
                        body["messages"] = [*body["messages"], self.assistant_echo(res, protocol, mode),
                                            *results(protocol, (ident,), "ok")]
                        body["stream"] = False
                        _res, calls = await self.request_and_wire(path, body, [{"text": "done"}])
                        self.assertEqual([original(p) for p in calls], [sig])

    async def test_message_limit_preserves_already_proven_earlier_call(self):
        for self.channel in ("api_key", "web_proxy"):
            for protocol in ("openai", "claude"):
                with self.subTest(channel=self.channel, protocol=protocol):
                    path, body = _request(protocol, "auto")
                    body["model"] = MODEL
                    body["messages"] = [{"role": "assistant" if i % 2 else "user", "content": str(i)}
                                        for i in range(origin._MAX_MESSAGES - 1)]
                    ident = f"r10-shape-limit-{self.channel}-{protocol}"
                    sig = b"before-scan-cap"
                    answer, _ = await self.request_and_wire(path, body, [provider_call(ident, sig)])
                    body["messages"].extend([self.assistant_echo(answer, protocol, "json"),
                                             *results(protocol, (ident,), "ok")])
                    _res, calls = await self.request_and_wire(path, body, [{"text": "done"}])
                    self.assertEqual([original(p) for p in calls], [sig])


def test_origin_never_normalizes_away_part_boundaries_or_unknown_fields(store):  # noqa: F811
    text = [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]
    prefix = [{"role": "user", "content": text},
              {"role": "assistant", "content": "ok", "reasoning_content": "why",
               "extra_content": {"google": {"ordinary_parts": [
                   {"kind": "text", "text": "ok", "thought_signature": "AAAA"},
                   {"kind": "thought", "text": "why"}]}}}]
    p = request(prefix=prefix)._signature_output_prefix
    assert p
    publish(p, [call("a", sig=b"exact")])
    store.register_call_signature("a", b"exact")
    assert replay(request(prefix=prefix, calls=[call("a")])) == [b"exact"]
    for changed in (
        [{"role": "user", "content": "ab"}, prefix[1]],
        [{"role": "user", "content": [{"type": "text", "text": "ab"}]}, prefix[1]],
        [{"role": "user", "content": text[::-1]}, prefix[1]],
        [{"role": "user", "content": [*text, {"type": "image_url", "image_url": {"url": "https://invalid.example/img"}}]}, prefix[1]],
        [{"role": "user", "content": "![sample](data:image/png;base64,AA==)"}, prefix[1]],
        [prefix[0], dict(prefix[1], reasoning_content="other!")],
        [prefix[0], dict(prefix[1], annotations=["present"])],
        [prefix[0], dict(prefix[1], _private_uncertain=None)],
    ):
        assert replay(request(prefix=changed, calls=[call("a")])) == [None]
    edited = deepcopy(prefix)
    edited[1]["extra_content"]["google"]["ordinary_parts"][0]["thought_signature"] = "BBBB"
    assert replay(request(prefix=edited, calls=[call("a")])) == [None]
    assert request(prefix=[{"role": "user", "content": "没" * (origin._MAX_TEXT // 3 + 1)}])._signature_output_prefix is None
    assert request(prefix=[{"role": "user", "content": text[:1] * (origin._MAX_BLOCKS + 1)}])._signature_output_prefix is None


def test_tool_signature_size_budget_refuses_whole_over_budget_batch(store):  # noqa: F811
    prefix = request()._signature_output_prefix
    store.register_call_signature("a", b"never-use-bare")
    calls = [call("a"), call("b")]
    publish(prefix, [call("a", sig=b"small"), call("b", sig=b"x" * (origin._MAX_SIGNATURE_BYTES + 1))])
    assert replay(request(calls=calls)) == [None, None]
    large = b"\x00" * 200000
    calls += [call("c")]
    publish(prefix, [dict(calling, extra_content={"google": {"thought_signature": base64.b64encode(large).decode()}})
                     for calling in calls])
    assert replay(request(calls=calls)) == [None, None, None]


def test_signature_residency_bounded_across_expiration_displacement_and_oversize():
    cache = SignatureStore(max_bytes=48, max_entries=16, ttl_seconds=10)
    now = [0]
    with patch.object(store_module, "time", SimpleNamespace(monotonic=lambda: now[0])):
        cache.register_call_signature(b"a", b"1" * 25)
        cache.register_call_signature(b"b", b"2" * 14)
        cache.register_call_signature(b"c", b"3" * 60)
        assert cache.get_record(b"c").state is SignatureState.AMBIGUOUS
        assert cache.get_record(b"a").signature == b"1" * 25  # LRU touch
        assert cache._resident_bytes == 42
        now[0] = 5
        cache.register_call_signature(b"d", b"4" * 12)
        assert cache._resident_bytes == 40 and cache.get_record(b"b") is None
        huge_key = "🌒" * 1024
        cache.register_call_signature(huge_key, b"value")
        assert huge_key not in cache._data
        assert cache.get_record(huge_key).state is SignatureState.AMBIGUOUS
        assert cache._resident_bytes == 40
        now[0] = 11
        assert cache.get_record(b"a") is None
        assert cache._resident_bytes == 14
        cache.put_unknown(b"new")
        assert cache._resident_bytes == 16  # expired tombstone record c no longer occupies bytes
        now[0] = 50
        cache.register_call_signature(b"new", b"last")
        assert cache._resident_bytes == 7
        assert len(cache._data) == len(cache._expiry) == 1
