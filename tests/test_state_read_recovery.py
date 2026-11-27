"""Recovery must protect recoverable state through both startup and hot reload."""
import json
import stat
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from unittest import mock

import pytest

import runtime_state as rs


@pytest.fixture
def state_path(tmp_path, monkeypatch):
    path = tmp_path / "web_state.json"
    monkeypatch.setattr(rs, "STATE_FILE", str(path))
    return path


def backups(path):
    return sorted(path.parent.glob(path.name + ".corrupt-*"))


def test_unreadable_is_not_corrupt_and_startup_import_cannot_replace_it(state_path):
    data = {"settings": {"retry_max": 17}, "express_keys": [{"key": "offline-fixture"}]}
    state_path.write_text(json.dumps(data))
    with mock.patch("builtins.open", side_effect=OSError("transient read failure")):
        with pytest.raises(rs.StatePersistenceError, match="读取"):
            rs.AppState().import_env_express_keys_once([])
    assert json.loads(state_path.read_text()) == data
    assert not backups(state_path)
    state = rs.AppState()
    state.import_env_express_keys_once([])
    assert [item["key"] for item in state.get_express_keys()] == ["offline-fixture"]
    assert state.get_setting("retry_max") == 17


def test_reload_read_failure_keeps_memory_committed_and_sequence(state_path):
    state = rs.AppState()
    state.update_settings({"retry_max": 17})
    seq = state._persisted_seq
    with (mock.patch.object(rs.json, "load", side_effect=OSError("read interrupted")),
          pytest.raises(rs.StatePersistenceError)):
        state.reload()
    assert state.get_setting("retry_max") == state.get_committed_settings("retry_max")["retry_max"] == 17
    assert state._persisted_seq == seq
    assert not backups(state_path)
    state.reload()
    assert state.get_setting("retry_max") == 17


def test_failed_quarantine_leaves_source_for_a_later_successful_recovery(state_path):
    source = b'{"settings":{'
    state_path.write_bytes(source)
    old = Path(str(state_path) + ".corrupt-123")
    old.write_bytes(b"prior fixture")
    with (mock.patch.object(rs.os, "replace", side_effect=OSError("mount unavailable")),
          pytest.raises(rs.StatePersistenceError, match="备份")):
        rs.AppState().import_env_express_keys_once([])
    assert state_path.read_bytes() == source
    assert backups(state_path) == [old]
    state = rs.AppState()
    state.import_env_express_keys_once([])
    assert {p.read_bytes() for p in backups(state_path)} == {source, b"prior fixture"}
    assert json.loads(state_path.read_text())["express_keys_initialized"] is True


@pytest.mark.parametrize(("replacement", "durable_retry"), [
    (None, None),
    (b'{"settings":{', None),
    (b'{"settings":{"retry_max":9}}', 9),
], ids=["removed", "quarantined", "valid"])
def test_successful_reload_discards_superseded_drafts_before_an_unrelated_write(state_path, replacement, durable_retry):
    state = rs.AppState()
    default_retry = state.get_setting("retry_max")
    state.update_settings({"retry_max": 3})
    model = "offline-fixture"
    assert state.get_effective_settings(model)["retry_max"] == 3
    waiting, release = Event(), Event()
    original_save = state._save

    def before_io(seq, snapshot):
        waiting.set()
        assert release.wait(5)
        return original_save(seq, snapshot)

    wanted = default_retry if durable_retry is None else durable_retry
    with mock.patch.object(state, "_save", side_effect=before_io), ThreadPoolExecutor(1) as pool:
        pending = pool.submit(state.update_settings, {"retry_max": 17})
        try:
            assert waiting.wait(5)
            assert state.get_setting("retry_max", committed=False) == 17
            if replacement is None:
                state_path.unlink()
            else:
                state_path.write_bytes(replacement)
            state.reload()
            assert state.get_effective_settings(model)["retry_max"] == wanted
        finally:
            release.set()
        assert pending.result(5)["retry_max"] == wanted

    if replacement is not None and durable_retry is None:
        assert [backup.read_bytes() for backup in backups(state_path)] == [replacement]
    assert state_path.exists() is (durable_retry is not None)
    state.update_settings({"image_max_concurrency": 2})
    durable = json.loads(state_path.read_bytes())["settings"]
    assert durable == ({"image_max_concurrency": 2} if durable_retry is None
                       else {"retry_max": durable_retry, "image_max_concurrency": 2})
    assert state.get_setting("retry_max", committed=False) == wanted
    assert rs.AppState().get_setting("retry_max") == wanted


def test_failed_reload_quarantine_preserves_original_and_both_in_memory_baselines(state_path):
    state = rs.AppState()
    state.update_settings({"retry_max": 3})
    assert state.get_effective_settings("offline-fixture")["retry_max"] == 3
    sequence, revision = state._persisted_seq, state._settings_rev
    source = b'{"settings":{'
    state_path.write_bytes(source)
    with (mock.patch.object(rs.os, "replace", side_effect=OSError("mount unavailable")),
          pytest.raises(rs.StatePersistenceError, match="备份")):
        state.reload()
    assert state_path.read_bytes() == source
    assert not backups(state_path)
    assert state.get_setting("retry_max") == state.get_setting("retry_max", committed=False) == 3
    assert state.get_effective_settings("offline-fixture")["retry_max"] == 3
    assert (state._persisted_seq, state._settings_rev) == (sequence, revision)


def test_noncolliding_private_backups_always_keep_the_latest_two(state_path):
    bodies = [b"{}", b"{", b"[]", b"\xff", b"null", b'"not state"']
    with mock.patch.object(rs.time, "time_ns", return_value=123456789):
        for index, body in enumerate(bodies):
            state_path.write_bytes(body)
            state_path.chmod(0o644)
            rs.AppState()
            if index:
                assert not state_path.exists()
                copies = backups(state_path)
                assert [p.read_bytes() for p in copies] == bodies[max(1, index - 1):index + 1]
                assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in copies)
            else:
                assert state_path.exists()
                assert not backups(state_path)


def test_missing_file_can_initialize_normally(state_path):
    state = rs.AppState()
    state.import_env_express_keys_once([])
    assert json.loads(state_path.read_text())["express_keys_initialized"] is True
    assert not backups(state_path)


def test_replacement_during_read_is_not_quarantined(state_path):
    state_path.write_text("{")
    source = state_path.with_suffix(".new")
    source.write_text('{"settings":{"retry_max":17}}')
    load = rs.json.load

    def replace_after_open(handle):
        source.replace(state_path)
        return load(handle)

    with mock.patch.object(rs.json, "load", side_effect=replace_after_open), pytest.raises(rs.StatePersistenceError):
        rs.AppState()
    assert not backups(state_path)
    assert rs.AppState().get_setting("retry_max") == 17
