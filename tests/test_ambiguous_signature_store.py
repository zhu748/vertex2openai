from unittest.mock import patch

import pytest

import thought_signatures
from signature_store import SignatureRecord, SignatureState, SignatureStore


@pytest.mark.parametrize("second", [b"other", None])
def test_overlapping_conflicting_records_never_supply_most_recent_bytes(second):
    store = SignatureStore(ttl_seconds=10, max_entries=4)
    with patch.object(thought_signatures, "signature_store", store), patch("signature_store.time.monotonic") as clock:
        clock.return_value = 0
        store.register_call_signature("x", b"old")
        clock.return_value = 5
        store.register_call_signature("x", second, unsigned_follower=second is None)
        for now in (5, 10, 11, 15):
            clock.return_value = now
            assert store.get_record("x") == SignatureRecord(SignatureState.AMBIGUOUS)
            assert thought_signatures.resolve_tool_call_signature("x", require_signature=True) == ("x", None)
            assert thought_signatures.resolve_tool_call_signature("x", explicit_signature=b"original") == ("x", b"original")
            assert thought_signatures.resolve_tool_call_signature("x", explicit_unsigned=True) == ("x", None)
        clock.return_value = 16
        store.register_call_signature("x", b"fresh")
        assert store.get_record("x") == SignatureRecord(SignatureState.SIGNED, b"fresh")


@pytest.mark.parametrize("pressure", [1, 12])
def test_lru_displacement_cannot_turn_repeated_ids_into_fresh_ones(pressure):
    store = SignatureStore(ttl_seconds=10, max_entries=2)
    with patch("signature_store.time.monotonic") as clock:
        clock.return_value = 0
        store.register_call_signature("x", b"old")
        store.register_call_signature("x", b"other")
        for i in range(pressure + 1):
            store.register_call_signature(f"noise-{i}", b"noise")
        assert "x" not in store._data  # The small primary cache really did evict it.
        clock.return_value = 5
        store.register_call_signature("x", b"other")
        assert store.get_record("x").state is SignatureState.AMBIGUOUS
        assert len(store._data) <= 2 and len(store._expiry) <= 2 and len(store._displaced) <= 2
        assert store._displaced_conflicts <= store._displaced.keys()
        clock.return_value = 100
        store.register_call_signature("x", b"new")
        assert store.get_record("x").signature == b"new"


@pytest.mark.parametrize("scoped", [False, True])
def test_displaced_live_conflict_cannot_become_a_validator_fallback(scoped):
    store = SignatureStore(ttl_seconds=10, max_entries=2)
    origin_key = b"exact-origin-slot" if scoped else None
    key = origin_key or "issued"
    with patch.object(thought_signatures, "signature_store", store), patch("signature_store.time.monotonic") as clock:
        clock.return_value = 0
        store.register_call_signature(key, b"old")
        store.register_call_signature(key, b"new")
        clock.return_value = 1
        store.put_unknown("unrelated-a")
        store.put_unknown("unrelated-b")
        assert key not in store._data
        clock.return_value = 7
        assert thought_signatures.resolve_tool_call_signature("issued", require_signature=True,
                                                              origin_key=origin_key) == ("issued", None)
        assert thought_signatures.resolve_tool_call_signature("issued", explicit_signature=b"passed",
                                                              origin_key=origin_key) == ("issued", b"passed")
        clock.return_value = 12
        assert store.get_record(key) is None
