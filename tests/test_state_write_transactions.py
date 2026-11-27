"""Concurrent successful console updates survive a failed write in another domain."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
from unittest import mock

from fastapi.testclient import TestClient
import pytest

import console_auth
import main
from routes import credentials_api, settings_api
import runtime_state


def test_credential_save_survives_failed_settings_write_and_reads_stay_live():
    with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            runtime_state, "STATE_FILE", str(Path(tmp) / "state.json")):
        state = runtime_state.AppState()
        state.update_settings({"retry_max": 2})
        writing, other_writer, fail, proceed = (threading.Event() for _ in range(4))
        real_fsync = runtime_state.os.fsync
        original_save, original_wait = state._save, state._saved_all.wait

        def fail_first_fsync(fd):
            if not writing.is_set():
                writing.set()
                assert fail.wait(5)
                raise OSError("simulated storage failure")
            return real_fsync(fd)

        def observe_wait(*args, **kwargs):
            other_writer.set()  # Correct path: the second mutation waits for the first outcome.
            return original_wait(*args, **kwargs)

        def observe_save(*args, **kwargs):
            if writing.is_set():
                # If a writer has already mutated before registration (old implementation),
                # hold it at that precise boundary until the older write has rolled back.
                other_writer.set()
                assert proceed.wait(5)
            return original_save(*args, **kwargs)

        token = console_auth.issue_session()

        def request(method, path, **kwargs):
            client = TestClient(main.app)
            try:
                client.cookies.set(console_auth.AUTH_COOKIE, token)
                return client.request(method, path, **kwargs)
            finally:
                client.close()

        try:
            with (mock.patch.object(settings_api, "app_state", state),
                  mock.patch.object(credentials_api, "app_state", state),
                  mock.patch.object(runtime_state.os, "fsync", side_effect=fail_first_fsync),
                  mock.patch.object(state._saved_all, "wait", side_effect=observe_wait),
                  mock.patch.object(state, "_save", side_effect=observe_save),
                  ThreadPoolExecutor(max_workers=3) as pool):
                try:
                    first = pool.submit(request, "POST", "/api/settings", json={"retry_max": 5})
                    assert writing.wait(5)
                    second = pool.submit(request, "POST", "/api/cookie-accounts", json={"accounts": [
                        {"id": "fixture", "cookie": "SAPISID=offline-fixture; SID=offline", "project_id": "proj"}]})
                    assert other_writer.wait(5)
                    # The writer is still blocked in fsync. A real GET and in-memory cached
                    # lookup should finish without waiting for it.
                    assert pool.submit(request, "GET", "/api/settings").result(3).status_code == 200
                    pool.submit(state.get_effective_settings, "local-model").result(3)
                    fail.set()
                    assert first.result(3).status_code == 503
                    proceed.set()
                    response = second.result(3)
                    assert response.status_code == 200, response.text
                finally:
                    fail.set()
                    proceed.set()
        finally:
            console_auth.revoke_session(token)

        for result in (state, runtime_state.AppState()):
            assert result.get_effective_settings("local-model")["retry_max"] == 2
            assert [rec["id"] for rec in result.get_cookie_accounts()] == ["fixture"]


@pytest.mark.parametrize("guarded", [False, True], ids=["legacy", "guarded"])
def test_global_receipt_is_its_own_durable_write_not_a_later_failed_draft(tmp_path, monkeypatch, guarded):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    monkeypatch.setattr(settings_api, "app_state", state)
    committed, later_writing, fail = (threading.Event() for _ in range(3))
    save, write = state._save, state._write_snapshot

    def return_late(seq, snapshot):
        saved = save(seq, snapshot)
        if snapshot["settings"]["retry_max"] == 2:
            committed.set()
            assert later_writing.wait(5)
        return saved

    def fail_later(snapshot):
        if snapshot["settings"]["retry_max"] == 9:
            later_writing.set()
            assert fail.wait(5)
            raise OSError("later save failed")
        write(snapshot)

    monkeypatch.setattr(state, "_save", return_late)
    monkeypatch.setattr(state, "_write_snapshot", fail_later)
    token = console_auth.issue_session()

    def post(count, previous):
        client = TestClient(main.app)
        try:
            client.cookies.set(console_auth.AUTH_COOKIE, token)
            body = {"retry_max": count}
            if guarded:
                body["_expected"] = {"retry_max": {"present": True, "value": previous}}
            return client.post("/api/settings", json=body)
        finally:
            client.close()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            try:
                first = pool.submit(post, 2, state.get_setting("retry_max"))
                assert committed.wait(5)
                second = pool.submit(post, 9, 2)
                response = first.result(4)
                assert response.status_code == 200
                assert response.json()["retry_max"] == 2
                assert runtime_state.AppState().get_setting("retry_max") == 2
            finally:
                fail.set()
            assert second.result(5).status_code == 503
    finally:
        console_auth.revoke_session(token)
    assert state.get_setting("retry_max") == 2


def test_queued_refresh_never_uses_a_provisional_credential_but_unrelated_rows_stay_available(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    first, second = {"id": "A", "cookie": "fake=old"}, {"id": "B", "cookie": "fake=other"}
    state.set_cookie_accounts([first, second])
    later_writing, fail = threading.Event(), threading.Event()

    def pending(snapshot):
        later_writing.set()
        assert fail.wait(5)
        raise OSError("temporary save failed")

    monkeypatch.setattr(state, "_write_snapshot", pending)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(state.set_cookie_accounts, [{**first, "cookie": "fake=new"}, second])
        try:
            assert later_writing.wait(5)
            assert state.get_cookie_refresh_account("A") is None
            assert state.get_cookie_refresh_account("B")["cookie"] == "fake=other"
        finally:
            fail.set()
        assert isinstance(future.exception(timeout=5), runtime_state.StatePersistenceError)
    assert state.get_cookie_refresh_account("A")["cookie"] == "fake=old"
