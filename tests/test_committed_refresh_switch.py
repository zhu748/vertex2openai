"""An automatic queue is governed by the durable switch; explicit requests remain independent."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

import httpx
from fastapi.testclient import TestClient

import console_auth
import cookie_refresh
import main
from routes import credentials_api, settings_api
import runtime_state


def test_disabling_mid_round_allows_current_to_finish_and_preserves_manual_refresh(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    state.update_settings({"cookie_auto_refresh": True})
    state.set_cookie_accounts([{"id": name, "cookie": "fixture=" + name} for name in ("a", "b", "c")])
    for module in (cookie_refresh, credentials_api, settings_api):
        monkeypatch.setattr(module, "app_state", state)
    monkeypatch.setattr(cookie_refresh, "_STARTUP_DELAY_SECONDS", 0)
    monkeypatch.setattr(cookie_refresh, "_ACCOUNT_GAP_SECONDS", 0)
    token = console_auth.issue_session()

    async def exercise():
        started, release, done = asyncio.Event(), asyncio.Event(), asyncio.Event()
        calls = []
        async def rotate(record):
            calls.append(record["id"])
            if len(calls) == 1:
                started.set()
                await release.wait()
            return {"id": record["id"], "status": "unchanged"}
        monkeypatch.setattr(cookie_refresh, "rotate_account", rotate)
        monkeypatch.setattr(cookie_refresh, "_log_round", lambda results: done.set())
        monkeypatch.setattr(cookie_refresh, "_loop_lock", asyncio.Lock())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test",
                                     cookies={console_auth.AUTH_COOKIE: token}) as client:
            worker = asyncio.create_task(cookie_refresh.refresh_loop())
            try:
                await asyncio.wait_for(started.wait(), 3)
                saved = await client.post("/api/settings", json={"cookie_auto_refresh": False})
                assert saved.status_code == 200 and not saved.json()["cookie_auto_refresh"]
                release.set()
                await asyncio.wait_for(done.wait(), 3)
                assert calls == ["a"]
                manual = await client.post("/api/cookie-accounts/refresh", json={})
                assert manual.status_code == 200 and manual.json()["unchanged"] == 3
                assert calls == ["a", "a", "b", "c"]
            finally:
                release.set()
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
    try:
        asyncio.run(exercise())
    finally:
        console_auth.revoke_session(token)


def test_queued_settings_wake_compares_its_own_durable_change(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    state.update_settings({"cookie_auto_refresh": False, "cookie_refresh_interval_seconds": 900})
    monkeypatch.setattr(settings_api, "app_state", state)
    monkeypatch.setattr(cookie_refresh, "app_state", state)
    notify = Mock()
    monkeypatch.setattr(cookie_refresh, "notify_settings_changed", notify)
    writing, queued, release = Event(), Event(), Event()
    original_wait = state._saved_all.wait

    def hold_first(snapshot):
        if snapshot["settings"]["cookie_refresh_interval_seconds"] == 800:
            writing.set()
            assert release.wait(5)
            raise OSError("simulated state write failure")
        write(snapshot)

    def watch_queue(*args):
        queued.set()
        return original_wait(*args)

    write = state._write_snapshot
    monkeypatch.setattr(state, "_write_snapshot", hold_first)
    monkeypatch.setattr(state._saved_all, "wait", watch_queue)
    token = console_auth.issue_session()
    def post(data):
        client = TestClient(main.app)
        try:
            client.cookies.set(console_auth.AUTH_COOKIE, token)
            return client.post("/api/settings", json=data)
        finally:
            client.close()
    try:
        with ThreadPoolExecutor(2) as pool:
            pending = pool.submit(post, {"cookie_auto_refresh": True, "cookie_refresh_interval_seconds": 800})
            try:
                assert writing.wait(5)
                assert not cookie_refresh.auto_refresh_enabled()
                assert cookie_refresh.refresh_interval_seconds() == 900
                later = pool.submit(post, {"cookie_auto_refresh": True})
                assert queued.wait(5)
            finally:
                release.set()
            assert pending.result(5).status_code == 503
            assert later.result(5).status_code == 200
        assert cookie_refresh.auto_refresh_enabled() and cookie_refresh.refresh_interval_seconds() == 900
        notify.assert_called_once_with()
    finally:
        console_auth.revoke_session(token)
