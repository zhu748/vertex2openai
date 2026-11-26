"""Only a committed credential identity owns (or invalidates) Cookie health."""
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import pytest

import credential_manager
from routes import credentials_api
import runtime_state


@pytest.fixture
def health_state(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    monkeypatch.setattr(credential_manager, "app_state", state)
    manager = credential_manager.CredentialManager()
    monkeypatch.setattr(credential_manager, "credential_manager", manager)
    monkeypatch.setattr(credentials_api, "app_state", state)
    return state, manager


@pytest.mark.parametrize("observe", ["snapshot", "dashboard", "cookie", "mixed", "feedback"])
def test_failed_cookie_edit_never_prunes_or_misattributes_health(health_state, monkeypatch, observe):
    state, manager = health_state
    old = {"id": "fixture", "cookie": "fixture=old", "project_id": "original"}
    new = {**old, "project_id": "replacement"}
    state.set_cookie_accounts([old])
    manager.record_cookie_auth_failure("fixture", credential=old)
    entered, release = Event(), Event()

    def fail_write(_snapshot):
        entered.set()
        assert release.wait(5)
        raise OSError("test write failure")

    with monkeypatch.context() as patch, ThreadPoolExecutor(1) as pool:
        patch.setattr(state, "_write_snapshot", fail_write)
        pending = pool.submit(state.set_cookie_accounts, [new])
        try:
            assert entered.wait(5)
            if observe == "snapshot":
                assert manager.cookie_health_snapshot()["fixture"]["failures"] == 1
            elif observe == "dashboard":
                shown = credentials_api._cookie_accounts_payload()["accounts"][0]
                assert (shown["project_id"], shown["health"]["failures"]) == ("original", 1)
            elif observe == "cookie":
                assert manager.get_cookie_record()["project_id"] == "original"
            elif observe == "mixed":
                keys = SimpleNamespace(get_records=lambda: [])
                assert manager.get_mixed_record(keys)["project_id"] == "original"
            else:
                manager.record_cookie_auth_failure("fixture", credential=old)
                manager.record_cookie_success("fixture", credential=new)
                manager.record_cookie_auth_failure("fixture", credential=new)
            assert manager.cookie_health_snapshot()["fixture"]["failures"] == (2 if observe == "feedback" else 1)
        finally:
            release.set()
        with pytest.raises(runtime_state.StatePersistenceError):
            pending.result(5)
    assert runtime_state.AppState().get_cookie_accounts()[0]["project_id"] == "original"
    assert manager.cookie_health_snapshot()["fixture"]["failures"] == (2 if observe == "feedback" else 1)

    # Reusing a row ID after a *successful* project edit must reset it, and late
    # feedback for the old project must never clear/quarantine its replacement.
    state.set_cookie_accounts([new])
    assert manager.cookie_health_snapshot() == {}
    manager.record_cookie_auth_failure("fixture", credential=new)
    manager.record_cookie_success("fixture", credential=old)
    manager.record_cookie_auth_failure("fixture", credential=old)
    assert manager.cookie_health_snapshot()["fixture"]["failures"] == 1


def test_prior_cookie_receipt_does_not_borrow_same_id_replacements_health(health_state):
    state, manager = health_state
    old = {"id": "fixture", "cookie": "fixture=old", "project_id": "old-project"}
    new = {**old, "project_id": "new-project"}
    # A delayed successful POST can carry an older receipt. Still return its
    # exact rows/revision, never the current same-ID replacement's counters.
    previous = state.set_cookie_accounts([old], with_revision=True)
    state.set_cookie_accounts([new])
    manager.record_cookie_auth_failure("fixture", credential=new)
    prior = credentials_api._cookie_accounts_payload(previous)
    assert prior["revision"] == previous[1]
    old_row, live_row = prior["accounts"][0], credentials_api._cookie_accounts_payload()["accounts"][0]
    assert (old_row["project_id"], old_row["health"]["failures"]) == ("old-project", 0)
    assert (live_row["project_id"], live_row["health"]["failures"]) == ("new-project", 1)
