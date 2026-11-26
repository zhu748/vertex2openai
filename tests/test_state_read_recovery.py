"""Recovery must protect recoverable state through both startup and hot reload."""
import json
import stat
from pathlib import Path
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
