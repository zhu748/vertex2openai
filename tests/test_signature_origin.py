import base64
from copy import deepcopy
from unittest.mock import patch

import pytest

from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from models import OpenAIRequest
import signature_origin as origin
from signature_store import SignatureState, SignatureStore
from thought_signatures import resolve_tool_call_signature
import thought_signatures


MODEL = "gemini-3-flash-preview"
TOOLS = [{"type": "function", "function": {"name": "quote"}}]
PREFIX = [{"role": "system", "content": "Read the quote."}, {"role": "user", "content": "What is the price?"}]


def call(ident, name="quote", arguments='{"symbol":"A","day":1}', sig=None):
    c = {"type": "function", "id": ident, "function": {"name": name, "arguments": arguments}}
    if sig:
        c["extra_content"] = {"google": {"thought_signature": base64.b64encode(sig).decode()}}
    return c


def request(prefix=PREFIX, model=MODEL, tenant="tenant-one", calls=None):
    messages = [*prefix]
    if calls:
        messages.append({"role": "assistant", "content": None, "tool_calls": calls})
    obj = OpenAIRequest(model=model, tools=TOOLS, messages=messages)
    origin.bind_signature_origins(obj, tenant)
    return obj


def publish(prefix, calls, **extra):
    choice = {"finish_reason": "tool_calls", "message": {"content": None, "tool_calls": calls, **extra}}
    origin.publish_completed_origins(prefix, [choice])


def replay(obj):
    msg = obj.messages[-1]
    return [resolve_tool_call_signature(c["id"], require_signature=i == 0, explicit_unsigned=i > 0,
                                        origin_key=origin.origin_slot(msg, i))[1]
            for i, c in enumerate(msg.tool_calls)]


@pytest.fixture
def store(monkeypatch):
    new_store = SignatureStore(max_entries=24, ttl_seconds=10)
    monkeypatch.setattr(thought_signatures, "signature_store", new_store)
    monkeypatch.setattr(origin, "signature_store", new_store)
    return new_store


def test_exact_origin_requires_whole_prefix_batch_and_slot_and_auth(store):
    calls = [call("first"), call("second")]
    for c, sig in zip(calls, (b"first", b"second"), strict=True):
        store.register_call_signature(c["id"], sig)
    prefix = request()._signature_output_prefix
    publish(prefix, [call("first", sig=b"first"), call("second", sig=b"second")])
    assert replay(request(calls=[call("first", arguments='{ "day": 1, "symbol": "A" }'), call("second")])) == [b"first", b"second"]
    for obj in (request(tenant="tenant-two", calls=calls), request(model="gemini-2.5-flash", calls=calls),
                request(prefix=PREFIX[1:], calls=calls), request(calls=list(reversed(calls))),
                request(calls=calls[:1]), request(calls=[call("first", arguments='{"day":2}'), call("second")]),
                request(calls=[call("first", name="other"), call("second")]),
                request(prefix=[*PREFIX, {"role": "tool", "tool_call_id": "older", "content": "revised"}], calls=calls),
                request(prefix=[{"role": "user", "content": [{"type": "text", "text": "unsupported form"}]}], calls=calls)):
        assert not any(replay(obj))
    # Explicit caller-carried bytes always outrank an exact fallback.
    msg = request(calls=calls).messages[-1]
    assert resolve_tool_call_signature("second", explicit_unsigned=True, explicit_signature=b"explicit",
                                       origin_key=origin.origin_slot(msg, 1))[1] == b"explicit"


def test_indistinguishable_and_expired_origins_never_restore_another_signature(store):
    with patch("signature_store.time.monotonic") as clock:
        clock.return_value = 0
        p = request()._signature_output_prefix
        calls = [call("a"), call("b")]
        for sig in (b"one", b"two"):
            store.register_call_signature("a", sig)
            publish(p, [call("a", sig=sig), call("b")])
        msg = request(calls=calls)
        assert replay(msg) == [None, None]
        assert store.get_record(origin.origin_slot(msg.messages[-1], 0)).state is SignatureState.AMBIGUOUS
        assert store.get_record(origin.origin_slot(msg.messages[-1], 1)).state is SignatureState.UNSIGNED_FOLLOWER
        # A different prefix is not ambiguous just because public IDs overlapped.
        steps = [*PREFIX, {"role": "user", "content": "A separate question"}]
        publish(request(prefix=steps)._signature_output_prefix, [call("a", sig=b"other"), call("b", sig=b"later")])
        other = request(prefix=steps, calls=calls)
        assert replay(other) == [b"other", b"later"]
        clock.return_value = 11
        assert store.get_record(origin.origin_slot(other.messages[-1], 1)) is None
        assert replay(other)[1] is None


def test_unrepresentable_choices_do_not_publish_and_budgets_never_truncate(store):
    p = request()._signature_output_prefix
    for c in ({"content": [{"type": "image_url"}]}, {"reasoning_content": "![Image](inline data)"}):
        publish(p, [call("a", sig=b"ineligible")], **c)
    origin.publish_completed_origins(p, [{"finish_reason": "length", "message": {
        "tool_calls": [call("a", arguments='{"unexpected partial JSON"', sig=b"short")]}}])
    publish(p, [call("same", sig=b"one"), call("same", arguments="{}", sig=b"two")])
    assert not store._data
    duplicate = request(calls=[call("same"), call("same", arguments="{}")])
    assert not origin.origin_slot(duplicate.messages[-1], 0)
    assert request(prefix=[{"role": "user", "content": "x" * (origin._MAX_TEXT + 1)}])._signature_output_prefix is None
    assert request(prefix=PREFIX * (origin._MAX_MESSAGES // 2 + 1))._signature_output_prefix is None
    too_many = [call(f"id-{i}", sig=b"no") for i in range(origin._MAX_CALLS + 1)]
    publish(p, too_many)
    assert not store._data
    # A middle unsupported turn disables only the later suffix, never hashes over it.
    partial = deepcopy(PREFIX) + [{"role": "assistant", "tool_calls": [call("a")]},
                                  {"role": "assistant", "content": "thinking",
                                   "extra_content": {"google": {"ordinary_parts": [{"kind": "image", "text": ""}]}}}]
    obj = request(prefix=partial, calls=[call("a")])
    assert origin.origin_slot(obj.messages[-3], 0)
    assert origin.origin_slot(obj.messages[-1], 0) == b"" and obj._signature_output_prefix is None
    assert "_signature" not in obj.model_dump_json()


def test_accepted_unrestorable_origin_is_remembered_even_if_the_signed_variant_arrives_later(store):
    p = request()._signature_output_prefix
    turn = [call("same")]
    completed_but_too_big = call("same", sig=b"b" * (origin._MAX_SIGNATURE_BYTES + 1))
    publish(p, [completed_but_too_big])
    key = origin.origin_slot(request(calls=turn).messages[-1], 0)
    assert store.get_record(key).state is SignatureState.AMBIGUOUS
    publish(p, [call("same", sig=b"another genuine emission")])
    assert store.get_record(key).state is SignatureState.AMBIGUOUS
    assert replay(request(calls=turn)) == [None]


@pytest.mark.parametrize("finish", ["length", "content_filter"])
def test_non_success_only_fences_complete_and_identifiable_terminal_batch(store, finish):
    prefix = request()._signature_output_prefix
    public = request(calls=[call("a")])
    key = origin.origin_slot(public.messages[-1], 0)
    publish(prefix, [call("a", sig=b"prior")])
    published = set(store._data)
    for arguments in (None, "", "  ", "[0]", '{"symbol":0', '{"symbol":NaN}'):
        choice = {"finish_reason": finish, "message": {"tool_calls": [call("a", arguments=arguments, sig=b"bad")]}}
        origin.publish_completed_origins(prefix, [choice])
        assert replay(public) == [b"prior"]
        assert set(store._data) == published
    good = call("a", sig=b"different but not authorized")
    choice = {"finish_reason": finish, "message": {"tool_calls": [{**good, "index": 8}]}}
    origin.publish_completed_origins(prefix, [choice])
    assert replay(public) == [b"prior"]
    choice["message"]["tool_calls"] = [good]
    origin.publish_completed_origins(prefix, [choice])
    assert store.get_record(key).state is SignatureState.AMBIGUOUS
    assert replay(public) == [None]


def test_stream_needs_normal_terminal_and_cannot_publish_text_or_partial_subset(store):
    prefix = request()._signature_output_prefix
    batches = {}
    def event(index, delta, finish=None):
        origin.observe_stream_origin(batches, prefix, {"index": index, "delta": delta, "finish_reason": finish})
    event(0, {"tool_calls": [call("a", sig=b"first")]})
    event(1, {"tool_calls": [call("x", sig=b"partial")]})
    event(1, {"content": "visible"})
    event(1, {}, "tool_calls")
    assert not store._data
    event(0, {"tool_calls": [call("b", sig=b"sibling")]})
    assert not store._data
    event(0, {}, "tool_calls")
    assert not store._data
    origin.publish_stream_origins(batches)
    assert replay(request(calls=[call("a"), call("b")])) == [b"first", b"sibling"]
    short = request(calls=[call("a")])
    assert store.get_record(origin.origin_slot(short.messages[-1], 0)) is None


def test_private_origin_cannot_be_supplied_as_a_request_field(store):
    obj = OpenAIRequest.model_validate({"model": MODEL, "tools": TOOLS, "_signature_output_prefix": "spoof",
        "_stage_terminal_signature_fence": "spoof",
        "messages": [*PREFIX, {"role": "assistant", "tool_calls": [call("a")], "_signature_slots": ["spoof"]}]})
    assert obj._signature_output_prefix is None
    assert obj._stage_terminal_signature_fence is None
    origin.bind_signature_origins(obj, "tenant-one")
    assert origin.origin_slot(obj.messages[-1], 0) == b""


class OriginRoutes(SchemaRouteHarness):
    async def test_interleaved_cross_protocol_turns_restore_both_own_signatures_on_real_wire(self):
        self.settings["max_retries"] = 0
        self.settings["express_location"] = "global"
        # The Studio fixture is project-scoped. Pin Express to that exact
        # publisher resource when checking migration across adapters.
        self.key_manager.get_express_record = lambda **_: {"key": "test-key", "project_id": "fixture"}
        for channel in ("api_key", "web_proxy"):
            for protocol in ("openai", "claude"):
                for mode in ("json", "fake", "stream"):
                    with self.subTest(channel=channel, protocol=protocol, mode=mode):
                        ids = [f"origin-{channel}-{protocol}-{mode}-{i}" for i in (0, 1)]
                        for label in ("Berlin", "Oslo"):
                            self.channel = channel
                            self.settings["fake_streaming"] = mode == "fake"
                            self.reply = [
                                {"functionCall": {"id": ident, "name": "weather", "args": {"city": "Paris"}},
                                 "thoughtSignature": base64.b64encode((label + str(i)).encode()).decode()}
                                for i, ident in enumerate(ids)]
                            path, body = _request(protocol, "auto", stream=mode != "json")
                            body.update(model=MODEL, messages=[{"role": "user", "content": label}])
                            result = await self.client.post(path, json=body)
                            self.assertEqual(result.status_code, 200)
                            for ident in ids:
                                self.assertIn(ident, result.text)
                        for label in ("Oslo", "Berlin"):
                            self.channel = "web_proxy" if channel == "api_key" else "api_key"
                            destination = "claude" if protocol == "openai" else "openai"
                            path, body = _request(destination, "auto")
                            turns = [{"role": "user", "content": label}]
                            if destination == "openai":
                                turns.append({"role": "assistant", "content": None, "tool_calls": [
                                    call(ident, "weather", '{"city":"Paris"}') for ident in ids]})
                                turns.extend({"role": "tool", "tool_call_id": ident, "content": "ok"} for ident in ids)
                            else:
                                turns.extend([{"role": "assistant", "content": [
                                    {"type": "tool_use", "id": ident, "name": "weather", "input": {"city": "Paris"}}
                                    for ident in ids]}, {"role": "user", "content": [
                                    {"type": "tool_result", "tool_use_id": ident, "content": "ok"} for ident in ids]}])
                            body.update(model=MODEL, messages=turns)
                            self.sent.clear()
                            self.reply = [{"text": "done"}]
                            result = await self.client.post(path, json=body)
                            self.assertEqual(result.status_code, 200, result.text)
                            self.assertEqual(len(self.sent), 1)
                            wire = self.sent[0].get("variables", self.sent[0])
                            observed = [(p["functionCall"]["id"], base64.urlsafe_b64decode(p["thoughtSignature"]))
                                        for turn in wire["contents"] for p in turn["parts"] if "functionCall" in p]
                            self.assertEqual(observed, [(ident, (label + str(i)).encode()) for i, ident in enumerate(ids)])


@pytest.mark.parametrize("changed", ["content", "reasoning_content", "missing", "swap"])
def test_mixed_tool_turn_is_bound_to_both_exact_public_fields(store, changed):
    calls = [call("mixed")]
    content, reason = "literal data:image/png. ", "visible thinking"
    p = request()._signature_output_prefix
    fields = {"content": content, "reasoning_content": reason}
    publish(p, [call("mixed", sig=b"mine")], **fields)
    store.register_call_signature("mixed", b"mine")
    def history(values):
        obj = OpenAIRequest(model=MODEL, tools=TOOLS, messages=[*PREFIX, {"role": "assistant", "tool_calls": calls, **values}])
        origin.bind_signature_origins(obj, "tenant-one")
        return obj
    assert replay(history(fields)) == [b"mine"]
    different = dict(fields)
    if changed == "missing":
        different.pop("reasoning_content")
    elif changed == "swap":
        different.update(content=reason, reasoning_content=content)
    else:
        different[changed] += " edited"
    assert replay(history(different)) == [None]
    # No default pure-tools alias was registered for this mixed step.
    assert replay(request(calls=calls)) == [None]


def test_visible_digests_match_across_chunks_and_refuse_images_and_oversize(store):
    from claude_protocol import anthropic_to_openai_request
    p = request()._signature_output_prefix
    mixed = {"content": "quote data:image/png.", "reasoning_content": "why?"}
    batches = {}
    def event(delta, finish=None):
        origin.observe_stream_origin(batches, p, {"index": 0, "delta": delta, "finish_reason": finish})
    for text in ("quote ", "data:", "image/png."):
        event({"content": text})
    for text in ("w", "hy?"):
        event({"reasoning_content": text})
    event({"tool_calls": [call("mixed", sig=b"mine")]}, "tool_calls")
    origin.publish_stream_origins(batches)
    claude = anthropic_to_openai_request({"model": MODEL, "max_tokens": 64, "system": PREFIX[0]["content"],
        "messages": [PREFIX[1], {"role": "assistant", "content": [
            {"type": "thinking", "thinking": mixed["reasoning_content"], "signature": "not-a-Google-signature"},
            {"type": "text", "text": mixed["content"]},
            {"type": "tool_use", "id": "mixed", "name": "quote", "input": {"symbol":"A","day":1}}]}]}, MODEL)
    origin.bind_signature_origins(claude, "tenant-one")
    assert replay(claude) == [b"mine"]
    assert claude.messages[-1].reasoning_content is None
    for pieces in (["leading !", "[Image](anything)"], ["x" * (origin._MAX_TEXT + 1)]):
        seen = origin.VisibleText()
        for text in pieces:
            seen.feed(text)
        assert seen.result() == (-1, b"")


def test_unrepresented_claude_thinking_only_turn_cannot_disappear_from_prefix(store):
    from claude_protocol import anthropic_to_openai_request
    p = request(prefix=PREFIX[1:])._signature_output_prefix
    publish(p, [call("a", sig=b"other")])
    obj = anthropic_to_openai_request({"model": MODEL, "max_tokens": 64, "messages": [
        PREFIX[1], {"role": "assistant", "content": [{"type": "thinking", "thinking": "different hidden step"}]},
        {"role": "assistant", "content": [{"type":"tool_use","id":"a","name":"quote","input":{"symbol":"A","day":1}}]}]}, MODEL)
    origin.bind_signature_origins(obj, "tenant-one")
    assert origin.origin_slot(obj.messages[-1], 0) == b""
    assert obj._signature_output_prefix is None
