"""Full model resets compare *all* saved fields, not just effective/built-in values."""
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

import console_auth
import main
import runtime_state
from routes import settings_api


MODEL = "gemini-3.5-flash-lite"
URL = "/api/model-overrides/" + MODEL


@pytest.fixture
def editor(tmp_path, monkeypatch):
    store = tmp_path / "state.json"
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(store))
    monkeypatch.setattr(settings_api, "app_state", runtime_state.AppState())
    monkeypatch.setattr(settings_api, "get_express_models", AsyncMock(return_value=[MODEL]))
    client = TestClient(main.app)
    token = console_auth.issue_session()
    try:
        client.cookies.set(console_auth.AUTH_COOKIE, token)
        assert client.post(URL, json={"default_temperature": None}).status_code == 200
        caps = client.get("/api/capabilities").json()
        saved = caps["saved_overrides"][MODEL]
        assert caps["overrides"][MODEL] != saved  # includes a built-in fallback
        yield client, store, saved
    finally:
        client.close()
        console_auth.revoke_session(token)


@pytest.mark.parametrize("concurrent", ["added-field", "changed-field", "removed-model"])
def test_stale_full_reset_preserves_newer_persistent_state(editor, concurrent):
    client, store, saved = editor
    if concurrent == "removed-model":
        assert client.delete(URL).status_code == 200
    else:
        patch = {"default_max_tokens": 128} if concurrent == "added-field" else {"default_temperature": .5}
        assert client.post(URL, json=patch).status_code == 200
    before = store.read_bytes()
    result = client.request("DELETE", URL, json={"expected_override": saved})
    assert result.status_code == 409
    assert store.read_bytes() == before


@pytest.mark.parametrize("use_baseline", [True, False], ids=["fresh", "legacy"])
def test_current_or_legacy_reset_restores_builtin(editor, use_baseline):
    client, _, saved = editor
    # Another model's change does not invalidate this editor's baseline.
    assert client.post("/api/model-overrides/other", json={"default_max_tokens": 128}).status_code == 200
    kwargs = {"json": {"expected_override": saved}} if use_baseline else {}
    result = client.request("DELETE", URL, **kwargs)
    assert result.status_code == 200 and result.json()["override"] == {"anti_truncation_mode": "both"}
    assert runtime_state.AppState().get_saved_model_overrides() == {"other": {"default_max_tokens": 128}}


@pytest.mark.parametrize("action", ["POST", "DELETE"])
def test_write_receipt_is_not_reconstructed_from_a_later_writer(editor, monkeypatch, action):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    client, _, initial = editor
    state = settings_api.app_state
    published, release = Event(), Event()
    first_seq, write = state._save_seq + 1, state._save

    def pause_first_receipt(seq, snapshot):
        write(seq, snapshot)
        if seq == first_seq:
            published.set()
            assert release.wait(5)

    monkeypatch.setattr(state, "_save", pause_first_receipt)
    body = {"default_top_p": .65} if action == "POST" else {"expected_override": initial}
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(client.request, action, URL, json=body)
        try:
            assert published.wait(5)
            later = client.post(URL, json={"default_max_tokens": 99})
        finally:
            release.set()
        earlier = first.result()
    earlier_own = {**initial, **body} if action == "POST" else {}
    later_own = {**earlier_own, "default_max_tokens": 99}
    for response, own in ((earlier, earlier_own), (later, later_own)):
        assert response.status_code == 200
        assert response.json()["saved_override"] == own
        assert response.json()["override"] == {"anti_truncation_mode": "both", **own}


def test_editor_reads_cannot_advertise_a_pending_write_that_later_fails(editor, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    client, _, saved = editor
    started, release = Event(), Event()

    def blocked_write(snapshot):
        started.set()
        assert release.wait(5)
        raise OSError("offline fixture disk unavailable")

    monkeypatch.setattr(settings_api.app_state, "_write_snapshot", blocked_write)
    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(client.post, URL, json={"default_top_p": .65})
        try:
            assert started.wait(5)
            caps = client.get("/api/capabilities").json()
            empty = client.post(URL, json={}).json()
        finally:
            release.set()
        assert pending.result().status_code >= 500
    assert caps["saved_overrides"][MODEL] == empty["saved_override"] == saved
    assert caps["overrides"][MODEL] == empty["override"] == {"anti_truncation_mode": "both", **saved}


@pytest.mark.parametrize("action", ["POST", "DELETE"])
@pytest.mark.parametrize("matches", [False, True], ids=["different", "same-result"])
def test_winning_reload_does_not_acknowledge_an_unwritten_model_edit(editor, monkeypatch, action, matches):
    import json
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    client, store, initial = editor
    state = settings_api.app_state
    ready, release = Event(), Event()
    save = state._save

    def before_io(seq, snapshot):
        ready.set()
        assert release.wait(5)
        return save(seq, snapshot)

    monkeypatch.setattr(state, "_save", before_io)
    requested = {**initial, "default_top_p": .65} if action == "POST" else {}
    body = {"default_top_p": .65} if action == "POST" else {"expected_override": initial}
    found = requested if matches else {"default_temperature": .9}
    with ThreadPoolExecutor(1) as pool:
        writer = pool.submit(client.request, action, URL, json=body)
        try:
            assert ready.wait(5)
            updated = json.loads(store.read_bytes())
            updated["settings"]["model_overrides"] = {MODEL: found} if found else {}
            store.write_text(json.dumps(updated))
            state.reload()
        finally:
            release.set()
        response = writer.result(5)
    assert runtime_state.AppState().get_model_override_receipt(MODEL)["saved_override"] == found
    if matches:
        assert response.status_code == 200
        assert response.json()["saved_override"] == found
        assert response.json()["override"] == {"anti_truncation_mode": "both", **found}
    else:
        assert response.status_code == 409
        assert response.json()["field"] == MODEL
