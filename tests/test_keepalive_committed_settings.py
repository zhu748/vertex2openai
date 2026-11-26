"""The outbound worker must not start with provisional settings from another save."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading

from fastapi.testclient import TestClient

import console_auth
import keepalive
import main
from routes import settings_api
import runtime_state


def test_concurrent_failed_save_cannot_supply_configuration_to_restarting_worker(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    url = "https://heartbeat.example/healthz"
    state.update_settings({"keepalive_url": url, "keepalive_interval": 60,
                           "keepalive_allow_private": True})
    monkeypatch.setattr(keepalive, "app_state", state)
    monkeypatch.setattr(settings_api, "app_state", state)
    service = keepalive.KeepAliveService()
    monkeypatch.setattr(settings_api, "keepalive_service", service)
    restarting, write_pending, allow_failure, started = (threading.Event() for _ in range(4))
    config_used = []
    original_write, original_restart = state._write_snapshot, service.restart

    async def restart():
        # The first POST has saved its policy. A second save now stages different
        # settings, but is blocked in disk I/O throughout the restart.
        restarting.set()
        assert await asyncio.to_thread(write_pending.wait, 5)
        await original_restart()
        assert await asyncio.to_thread(started.wait, 5)

    async def fake_worker(url, seconds, allow_private=None):
        config_used.append((url, seconds, allow_private))
        started.set()

    def write(snapshot):
        if snapshot["settings"].get("keepalive_interval") == 80:
            write_pending.set()
            assert allow_failure.wait(5)
            raise OSError("simulated storage failure")
        original_write(snapshot)

    monkeypatch.setattr(state, "_write_snapshot", write)
    monkeypatch.setattr(service, "restart", restart)
    monkeypatch.setattr(service, "_run", fake_worker)
    token = console_auth.issue_session()

    def update(patch):
        client = TestClient(main.app)
        try:
            client.cookies.set(console_auth.AUTH_COOKIE, token)
            return client.post("/api/settings", json=patch).status_code
        finally:
            client.close()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            try:
                first = pool.submit(update, {"keepalive_allow_private": False})
                assert restarting.wait(5)
                second = pool.submit(update, {"keepalive_url": "https://other.example/healthz",
                                              "keepalive_interval": 80, "keepalive_allow_private": True})
                assert first.result(5) == 200
            finally:
                allow_failure.set()
            assert second.result(5) == 503
    finally:
        console_auth.revoke_session(token)

    assert runtime_state.AppState().get_setting("keepalive_interval") == 60
    assert config_used == [(url, 60, False)]
