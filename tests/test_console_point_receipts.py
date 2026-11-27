"""Point-edit acknowledgements belong to this write, not a requested or later draft."""
from concurrent.futures import ThreadPoolExecutor
import json
from threading import Event
from unittest.mock import Mock

from fastapi.testclient import TestClient
import pytest

import console_auth
import cookie_refresh
import main
from routes import credentials_api, settings_api
import runtime_state


MODE = "/api/settings/mode"
PROJECT = "/api/express-keys/project"
KEY = "offline-fixture"


@pytest.fixture
def console(tmp_path, monkeypatch):
    store = tmp_path / "state.json"
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(store))
    state = runtime_state.AppState()
    state.set_express_keys([{"key": KEY, "project_id": "before", "project_source": "auto"}])
    state.set_channel_mode("api_key")
    monkeypatch.setattr(settings_api, "app_state", state)
    monkeypatch.setattr(credentials_api, "app_state", state)
    token = console_auth.issue_session()
    client = TestClient(main.app)
    try:
        client.cookies.set(console_auth.AUTH_COOKIE, token)
        yield client, state, store
    finally:
        client.close()
        console_auth.revoke_session(token)


@pytest.mark.parametrize(("path", "winner", "status"), [
    (MODE, "same", 200), (MODE, "different", 409),
    (PROJECT, "same", 200), (PROJECT, "different", 409),
    (PROJECT, "same-id-auto-source", 409), (PROJECT, "removed", 409),
])
def test_point_edit_reports_the_snapshot_that_won_reload(console, monkeypatch, path, winner, status):
    client, state, store = console
    wanted = {"mode": "mixed"} if path == MODE else {"key": KEY, "project_id": "wanted"}
    entered, release = Event(), Event()
    save = state._save

    def before_io(seq, snapshot):
        entered.set()
        assert release.wait(5)
        return save(seq, snapshot)

    monkeypatch.setattr(state, "_save", before_io)
    with ThreadPoolExecutor(1) as pool:
        post = pool.submit(client.post, path, json=wanted)
        try:
            assert entered.wait(5)
            external = json.loads(store.read_bytes())
            if path == MODE:
                external["upstream_mode"] = "mixed" if winner == "same" else "web_proxy"
                external["use_web_proxy"] = external["upstream_mode"] == "web_proxy"
            elif winner == "removed":
                external["express_keys"] = []
            else:
                external["express_keys"][0].update(
                    project_id="external" if winner == "different" else "wanted",
                    project_source="auto" if winner == "same-id-auto-source" else "manual",
                    detected_at=17, note="external edit",
                )
            store.write_text(json.dumps(external))
            state.reload()
        finally:
            release.set()
        result = post.result(5)
    assert result.status_code == status, result.text
    body = result.json()
    if path == MODE:
        assert body["mode"] == runtime_state.AppState().get_channel_mode() == external["upstream_mode"]
    if status == 409:
        assert body.get("error") and body.get("status") != "success"
    elif path == PROJECT:
        records, revision = runtime_state.AppState().credential_snapshot("keys")
        assert (body["keys"], body["revision"]) == (records, revision)
        assert body["keys"][0]["note"] == "external edit"


@pytest.mark.parametrize(("field", "before", "requested", "winner", "guarded", "status"), [
    ("retry_max", 3, 7, 7, True, 200),
    ("retry_max", 3, 7, 9, True, 409),
    ("retry_max", 3, 7, 9, False, 200),
    ("proxy_urls", [], ["http://new.example"], ["http://new.example:80"], True, 200),
    ("proxy_urls", [], ["http://new.example"], ["http://different.example:80"], True, 409),
    ("proxy_url", "", "http://new.example", "http://new.example:80", True, 200),
    ("proxy_url", "", "http://new.example", "http://different.example:80", True, 409),
    ("cookie_refresh_interval_seconds", 601, 610, 610, True, 200),
    ("cookie_refresh_interval_seconds", 601, 610, 612, True, 409),
])
def test_guarded_global_receipt_must_match_the_accepted_values(console, monkeypatch, field, before,
                                                               requested, winner, guarded, status):
    client, state, store = console
    state.update_settings({field: before})
    notify = Mock()
    monkeypatch.setattr(cookie_refresh, "notify_settings_changed", notify)
    entered, release = Event(), Event()
    save = state._save

    def before_io(seq, snapshot):
        entered.set()
        assert release.wait(5)
        return save(seq, snapshot)

    monkeypatch.setattr(state, "_save", before_io)
    payload = {field: requested}
    if guarded:
        payload["_expected"] = {field: {"present": True, "value": before}}
    with ThreadPoolExecutor(1) as pool:
        post = pool.submit(client.post, "/api/settings", json=payload)
        try:
            assert entered.wait(5)
            external = json.loads(store.read_bytes())
            external["settings"].update({field: winner, "image_max_concurrency": 4})
            if field == "proxy_urls":
                external["settings"]["proxy_url"] = winner[0]
            elif field == "proxy_url":
                external["settings"]["proxy_urls"] = [winner]
            store.write_text(json.dumps(external))
            state.reload()
        finally:
            release.set()
        result = post.result(5)
    assert result.status_code == status, result.text
    if status == 409:
        assert result.json()["field"] == field
    else:
        assert result.json()[field] == winner
        assert result.json()["image_max_concurrency"] == 4
    persisted = runtime_state.AppState().get_settings()
    assert persisted[field] == winner and persisted["image_max_concurrency"] == 4
    assert notify.call_count == (1 if status == 200 and field == "cookie_refresh_interval_seconds" else 0)


@pytest.mark.parametrize("payload", [{"_expected": {}}, {"unused": 17, "_expected": {}}])
def test_guarded_global_noop_and_unknown_fields_keep_legacy_success(console, payload):
    client, state, _ = console
    before = state.get_settings()
    result = client.post("/api/settings", json=payload)
    assert result.status_code == 200
    assert result.json() == before == state.get_settings()


@pytest.mark.parametrize("path", [MODE, PROJECT])
def test_point_receipt_survives_later_failed_draft(console, monkeypatch, path):
    client, state, _ = console
    committed, later_started, release = Event(), Event(), Event()
    save, write, first_seq = state._save, state._write_snapshot, state._save_seq + 1

    def return_after_later_starts(seq, snapshot):
        applied = save(seq, snapshot)
        if seq == first_seq:
            committed.set()
            assert later_started.wait(5)
        return applied

    def fail_later(snapshot):
        if committed.is_set():
            later_started.set()
            assert release.wait(5)
            raise OSError("later draft failed")
        write(snapshot)

    monkeypatch.setattr(state, "_save", return_after_later_starts)
    monkeypatch.setattr(state, "_write_snapshot", fail_later)
    bodies = ([{"mode": "mixed"}, {"mode": "web_proxy"}] if path == MODE else
              [{"key": KEY, "project_id": "first"}, {"key": KEY, "project_id": "later"}])
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(client.post, path, json=bodies[0])
        try:
            assert committed.wait(5)
            later = pool.submit(client.post, path, json=bodies[1])
            result = first.result(4)
            assert result.status_code == 200, result.text
            body = result.json()
            disk = runtime_state.AppState()
            if path == MODE:
                assert body["mode"] == disk.get_channel_mode() == "mixed"
                assert client.get("/api/settings/runtime").json()["mode"] == "mixed"
            else:
                assert (body["keys"], body["revision"]) == disk.credential_snapshot("keys")
                assert body["keys"][0]["project_id"] == "first"
        finally:
            release.set()
        assert later.result(5).status_code == 503


def test_missing_point_target_is_still_not_found_and_empty_project_can_be_saved(console):
    client, _, _ = console
    assert client.post(PROJECT, json={"key": "missing", "project_id": "p"}).status_code == 404
    result = client.post(PROJECT, json={"key": KEY, "project_id": " "})
    assert result.status_code == 200
    row = result.json()["keys"][0]
    assert row["project_id"] == row["project_source"] == ""


def test_project_probe_keeps_compare_and_set_and_reports_a_superseding_manual_edit(console, monkeypatch):
    _, state, store = console
    original = state.get_express_keys()[0]
    assert state.update_express_key_project(KEY, "detected", expected={**original, "note": "stale"}) is False
    entered, release = Event(), Event()
    save = state._save

    def before_io(seq, snapshot):
        entered.set()
        assert release.wait(5)
        return save(seq, snapshot)

    monkeypatch.setattr(state, "_save", before_io)
    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(state.update_express_key_project, KEY, "detected", expected=original)
        try:
            assert entered.wait(5)
            external = json.loads(store.read_bytes())
            external["express_keys"][0].update(project_id="manual", project_source="manual")
            store.write_text(json.dumps(external))
            state.reload()
        finally:
            release.set()
        assert pending.result(5) is False
    assert runtime_state.AppState().get_express_keys()[0]["project_id"] == "manual"
