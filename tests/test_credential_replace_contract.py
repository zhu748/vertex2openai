"""Credential replacement needs an explicit list; an empty list is an intentional clear."""
import pytest
from fastapi.testclient import TestClient

import console_auth
import express_key_manager
import main
from routes import credentials_api, settings_api
import runtime_state


@pytest.fixture
def credential_console(tmp_path, monkeypatch):
    store = tmp_path / "state.json"
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(store))
    state = runtime_state.AppState()
    monkeypatch.setattr(credentials_api, "app_state", state)
    monkeypatch.setattr(settings_api, "app_state", state)
    monkeypatch.setattr(express_key_manager, "app_state", state)
    client = TestClient(main.app)  # Do not start unrelated background workers.
    token = console_auth.issue_session()
    try:
        client.cookies.set(console_auth.AUTH_COOKIE, token)
        yield client, state, store
    finally:
        client.close()
        console_auth.revoke_session(token)


@pytest.fixture(params=[
    ("/api/express-keys", "keys", {"key": "offline-fixture", "project_id": "fixture-project"}),
    ("/api/cookie-accounts", "accounts", {"id": "fixture", "cookie": "SAPISID=offline; SID=offline",
                                         "project_id": "fixture-project"}),
], ids=["express", "cookies"])
def credential_list(request, credential_console):
    path, field, record = request.param
    client, _, store = credential_console
    seeded = client.post(path, json={field: [record]})
    assert seeded.status_code == 200 and seeded.json()["total"] == 1
    return client, path, field, store, seeded.json()["revision"]


@pytest.mark.parametrize("case", ["missing", "typo", "revision-only"])
def test_missing_collection_never_writes(credential_list, case):
    client, path, field, store, revision = credential_list
    before = store.read_bytes()
    body = {"missing": {}, "typo": {field[:-1]: []}, "revision-only": {"revision": revision}}[case]
    result = client.post(path, json=body)
    assert result.status_code == 422
    assert store.read_bytes() == before
    snapshot = client.get(path).json()
    assert snapshot["revision"] == revision and snapshot["total"] == 1


@pytest.mark.parametrize("use_revision", [True, False], ids=["current-revision", "legacy"])
def test_explicit_empty_collection_can_clear_but_not_with_stale_revision(credential_list, use_revision):
    client, path, field, store, revision = credential_list
    before = store.read_bytes()
    assert client.post(path, json={field: [], "revision": "stale"}).status_code == 409
    assert store.read_bytes() == before
    body = {field: [], **({"revision": revision} if use_revision else {})}
    result = client.post(path, json=body)
    assert result.status_code == 200 and result.json()[field] == []
    kind = "keys" if field == "keys" else "cookies"
    assert runtime_state.AppState().credential_snapshot(kind) == ([], result.json()["revision"])

@pytest.mark.parametrize("bad_rows", [
    [{"project_id": "project"}],
    [{"key": ""}],
    [{"key": "  \t  "}],
    [{"key": 123}],
    ["not-a-record"],
    [{"key": "offline-draft", "project_id": "project"}, {"key": " offline-draft ", "project_id": "other"}],
    [{"key": "offline-draft", "project_id": "project"}, {"note": "copied row"}],
], ids=["missing", "empty", "blank", "wrong-type", "not-object", "duplicate", "mixed"])
def test_express_replace_rejects_entire_invalid_batch(credential_console, bad_rows):
    client, state, store = credential_console
    path = "/api/express-keys"
    seeded = client.post(path, json={"keys": [{"key": "offline-original", "project_id": "project"}]})
    assert seeded.status_code == 200
    snapshot = state.credential_snapshot("keys")
    before = store.read_bytes()
    for revision in ({}, {"revision": seeded.json()["revision"]}):
        response = client.post(path, json={"keys": bad_rows, **revision})
        assert response.status_code == 400
        assert "offline-original" not in response.text and "offline-draft" not in response.text
        assert state.credential_snapshot("keys") == snapshot
        assert store.read_bytes() == before
    assert runtime_state.AppState().credential_snapshot("keys") == snapshot


def test_legacy_cookie_save_updates_only_the_enabled_row_and_its_manual_revision(credential_console):
    client, state, store = credential_console
    path = "/api/cookie-accounts"
    disabled = {"id": "parked", "cookie": "SAPISID=parked", "project_id": "parked", "enabled": False}
    active = {"id": "current", "cookie": "SAPISID=old", "project_id": "live-project",
              "proxies": ["direct"], "note": "keep"}
    other = {"id": "other", "cookie": "SAPISID=other", "project_id": "other-project"}
    rows = [disabled, active, other]
    initial = client.post(path, json={"accounts": rows}).json()["revision"]
    previous = state.get_cookie_accounts()
    assert client.get("/api/settings/runtime").json()["google_project_id"] == "live-project"

    rotated, manual = "SAPISID=auto", "SAPISID=manual"
    assert state.update_cookie_account_cookie("current", rotated, expected_cookie=active["cookie"])
    assert client.get(path).json()["revision"] == initial
    assert client.post("/api/cookie", json={"cookie": manual}).status_code == 200
    expected = [previous[0], {**previous[1], "cookie": manual}, previous[2]]
    manual_revision = client.get(path).json()["revision"]
    assert manual_revision != initial
    assert runtime_state.AppState().credential_snapshot("cookies") == (expected, manual_revision)
    assert state.get_google_cookie() == manual
    assert not state.update_cookie_account_cookie("current", "SAPISID=late", expected_cookie=rotated)
    before = store.read_bytes()
    stale = client.post(path, json={"revision": initial, "accounts": rows})
    assert stale.status_code == 409 and store.read_bytes() == before
    assert client.post("/api/cookie", json={"cookie": manual}).status_code == 200
    assert client.get(path).json()["revision"] == manual_revision  # no-op

    same_cookie = client.post("/api/cookie", json={"cookie": disabled["cookie"]})
    assert same_cookie.status_code == 409 and store.read_bytes() == before
    edit = client.post("/api/cookie", json={"project_id": "new-project"})
    assert edit.status_code == 200
    assert client.get("/api/settings/runtime").json()["google_project_id"] == "new-project"
    snapshot, revision = state.credential_snapshot("cookies")
    assert snapshot == [expected[0], {**expected[1], "project_id": "new-project"}, expected[2]]
    assert revision != manual_revision

    assert state.update_cookie_account_cookie("current", rotated, expected_cookie=manual)
    assert client.get(path).json()["revision"] == revision
    draft = [{**r, "cookie": "", "note": "latest" if r["id"] == "current" else r["note"]} for r in snapshot]
    result = client.post(path, json={"revision": revision, "accounts": draft})
    assert result.status_code == 200
    assert runtime_state.AppState().get_cookie_accounts()[1] == {**snapshot[1], "cookie": rotated, "note": "latest"}


def test_legacy_cookie_save_does_not_repurpose_an_inactive_account(credential_console):
    client, state, store = credential_console
    path = "/api/cookie-accounts"
    client.post(path, json={"accounts": [{"id": "disabled", "cookie": "SAPISID=parked", "enabled": False}]})
    baseline = state.credential_snapshot("cookies")
    before = store.read_bytes()
    assert client.post("/api/cookie", json={"project_id": "unused"}).status_code == 400
    assert client.post("/api/cookie", json={"cookie": "SAPISID=new", "project_id": "unused"}).status_code == 409
    assert store.read_bytes() == before and state.credential_snapshot("cookies") == baseline
    assert client.post(path, json={"accounts": []}).status_code == 200
    assert client.post("/api/cookie", json={"cookie": "SAPISID=first", "project_id": "first"}).status_code == 200
    identity = state.get_cookie_accounts()[0]["id"]
    assert client.post("/api/cookie", json={"cookie": "SAPISID=replaced"}).status_code == 200
    active = runtime_state.AppState().get_cookie_accounts()[0]
    assert active["id"] == identity and active["enabled"] and active["cookie"] == "SAPISID=replaced"


def test_legacy_on_disk_cookie_is_migrated_without_changing_its_identity(credential_console):
    client, state, store = credential_console
    store.write_text('{"google_cookie": "SAPISID=legacy", "google_project_id": "legacy-project"}')
    state.reload()
    original, initial = state.credential_snapshot("cookies")
    assert client.post("/api/cookie", json={"cookie": "SAPISID=new"}).status_code == 200
    records, revision = runtime_state.AppState().credential_snapshot("cookies")
    assert records == [{**original[0], "cookie": "SAPISID=new"}] and revision != initial
    assert "SAPISID=legacy" not in store.read_text()


@pytest.mark.parametrize("superseded", [False, True], ids=["later-rollback", "winning-reload"])
def test_list_save_receipt_is_its_applied_revision(credential_console, credential_list, monkeypatch, superseded):
    import json
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    client, state, store = credential_console
    _, path, field, _, revision = credential_list
    kind = "keys" if field == "keys" else "cookies"
    storage = "express_keys" if field == "keys" else "cookie_accounts"
    row = state.credential_snapshot(kind)[0][0]
    incoming = {**row, "note": "accepted"}
    if field == "accounts":
        incoming["cookie"] = ""  # Use the latest token; the receipt must continue to mask it.
    staged, pending, release = Event(), Event(), Event()
    first_seq, save, write = state._save_seq + 1, state._save, state._write_snapshot

    def hold_first(seq, snapshot):
        if superseded and seq == first_seq:
            staged.set()
            assert release.wait(5)
        durable = save(seq, snapshot)
        if not superseded and seq == first_seq:
            staged.set()
            assert pending.wait(5)
        return durable

    def write_later(snapshot):
        if snapshot[storage][0]["note"] == "pending":
            pending.set()
            assert release.wait(5)
            raise OSError("test: update not persisted")
        write(snapshot)

    monkeypatch.setattr(state, "_save", hold_first)
    monkeypatch.setattr(state, "_write_snapshot", write_later)
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(client.post, path, json={field: [incoming], "revision": revision})
        try:
            assert staged.wait(5)
            if superseded:
                reloaded = json.loads(store.read_bytes())
                reloaded[storage][0]["note"] = "reloaded"
                store.write_text(json.dumps(reloaded))
                state.reload()
                release.set()
                expected_note = "reloaded"
            else:
                later = pool.submit(client.post, path, json={field: [{**incoming, "note": "pending"}]})
                expected_note = "accepted"
            result = first.result(4)
            assert result.status_code == 200, result.text
            payload = result.json()
            records, disk_revision = runtime_state.AppState().credential_snapshot(kind)
            assert payload[field][0]["note"] == records[0]["note"] == expected_note
            assert payload["revision"] == disk_revision
            if kind == "cookies":
                assert row["cookie"] not in result.text
                assert "cookie" not in payload[field][0] and "health" in payload[field][0]
        finally:
            release.set()
        if not superseded:
            assert later.result(4).status_code == 503
