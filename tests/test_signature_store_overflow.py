"""Overflow remembers affected tags, not a global time block on new origins."""

from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import patch

import pytest

import signature_origin as origins
import thought_signatures
from signature_store import SKIP_VALIDATOR_SENTINEL, SignatureRecord, SignatureState, SignatureStore
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_response


@pytest.mark.parametrize("known, scope", [("issued", None), (b"exact-origin", b"exact-origin")])
def test_forgotten_live_conflict_remains_local_to_its_tag(known, scope):
    cache = SignatureStore(ttl_seconds=10, max_entries=2)
    with (
        patch("signature_store.time.monotonic") as clock,
        patch.object(thought_signatures, "signature_store", cache),
    ):
        clock.return_value = 0
        cache.register_call_signature(known, b"older")
        cache.register_call_signature(known, b"changed")
        for i in range(24):
            cache.register_call_signature(f"retained-{i}", b"ordinary")
        assert known not in cache._data
        assert not cache._displaced_conflicts
        for now in (0, 5, 10):
            clock.return_value = now
            cache.register_call_signature(f"fresh-{now}", b"genuine")
            assert cache.get_record(f"fresh-{now}") == SignatureRecord(SignatureState.SIGNED, b"genuine")
            assert cache.get_record(known) == SignatureRecord(SignatureState.AMBIGUOUS)
            assert thought_signatures.resolve_tool_call_signature(
                "issued", require_signature=True, origin_key=scope
            ) == ("issued", None)
        clock.return_value = 13
        assert cache.get_record(known) is None
        cache.register_call_signature(known, b"new turn")
        assert cache.get_record(known) == SignatureRecord(SignatureState.SIGNED, b"new turn")


def test_ordinary_churn_preserves_immediate_replays_at_default_capacity():
    cache = SignatureStore()
    with patch("signature_store.time.monotonic", return_value=0):
        for i in range(24000):
            cache.register_call_signature(f"previous-{i}", b"genuine")
        # Over twice the combined number of exact resident+displaced slots.
        # A probabilistic overflow hit may conservatively withhold a fresh
        # record, but the workload must not lose all unrelated signatures.
        signed = 0
        for i in range(600):
            key = f"unrelated-{i}"
            cache.register_call_signature(key, b"genuine")
            signed += cache.get_record(key) == SignatureRecord(SignatureState.SIGNED, b"genuine")
        assert signed >= 594


def test_rolling_buckets_never_forget_an_unexpired_conflict():
    cache = SignatureStore(ttl_seconds=8, max_entries=1)
    with patch("signature_store.time.monotonic") as clock:
        for now in range(48):
            clock.return_value = now
            cache.register_call_signature(f"conflict-{now}", b"a")
            cache.register_call_signature(f"conflict-{now}", b"b")
            cache.register_call_signature(f"normal-a-{now}", b"a")
            cache.register_call_signature(f"normal-b-{now}", b"a")
            assert len(cache._overflow._buckets) <= 5
            assert not cache._displaced_conflicts
            for live in range(max(now - 8, 0), now + 1):
                assert cache.get_record(f"conflict-{live}") == SignatureRecord(SignatureState.AMBIGUOUS)
        clock.return_value = 60
        assert all(cache.get_record(f"conflict-{i}") is None for i in range(48))


class OverflowReplayRoutes(SchemaRouteHarness):
    async def test_normal_fresh_replay_survives_pressure_without_forgiving_old_conflicts(self):
        for dialect in ("openai", "claude"):
            for channel in ("api_key", "web_proxy"):
                self.channel = channel
                with self.subTest(dialect=dialect, channel=channel), ExitStack() as stack:
                    cache = SignatureStore(max_entries=2)
                    for module in (origins, thought_signatures, cookie_response):
                        stack.enter_context(patch.object(module, "signature_store", cache))
                    path, request = _request(dialect, "auto")
                    request.update(model="gemini-3.5-flash", messages=[{"role": "user", "content": "Check weather."}])

                    async def issue(call_id, sample):
                        self.reply = [{"functionCall": {
                            "id": call_id, "name": "weather", "args": {"city": "Paris"}
                        }, "thoughtSignature": sample}]
                        response = await self.client.post(path, json=request)
                        self.assertEqual(response.status_code, 200, response.text[:300])
                        if dialect == "claude":
                            assistant = {"role": "assistant", "content": response.json()["content"]}
                            tool = {"role": "user", "content": [
                                {"type": "tool_result", "tool_use_id": call_id, "content": "clear"}
                            ]}
                        else:
                            assistant = response.json()["choices"][0]["message"]
                            assistant.pop("extra_content", None)
                            for call in assistant["tool_calls"]:
                                call.pop("extra_content", None)
                            tool = {"role": "tool", "tool_call_id": call_id, "content": "clear"}
                        return assistant, tool

                    async def replay(turn):
                        body = deepcopy(request)
                        body["messages"].extend(turn)
                        self.reply = [{"text": "clear"}]
                        self.sent.clear()
                        response = await self.client.post(path, json=body)
                        self.assertEqual(response.status_code, 200, response.text[:300])
                        self.assertEqual(len(self.sent), 1)
                        wire = self.sent[0].get("variables", self.sent[0])
                        call, result = [part for message in wire["contents"] for part in message["parts"]
                                        if "functionCall" in part or "functionResponse" in part]
                        return call, result

                    old = await issue("stale", "AQIDBAUG")
                    await issue("stale", "BwgJCgsM")
                    for i in range(16):
                        cache.register_call_signature(f"pressure-{i}", b"x")
                    fresh = await issue("fresh", "DQ4PEBES")

                    call, result = await replay(fresh)
                    self.assertEqual(call["thoughtSignature"], "DQ4PEBES")
                    self.assertEqual(call["functionCall"]["id"], result["functionResponse"]["id"])
                    self.assertEqual(call["functionCall"]["id"], "fresh")
                    call, _ = await replay(old)
                    self.assertNotIn("thoughtSignature", call, SKIP_VALIDATOR_SENTINEL)
