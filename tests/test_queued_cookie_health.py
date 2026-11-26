"""Queued console health requests resolve an eligible identity at actual network admission."""
import asyncio
from contextlib import asynccontextmanager
from unittest.mock import Mock

import httpx

import console_auth
import cookie_probe
import main
from routes import credentials_api
import runtime_state


def test_global_probe_slots_recheck_rows_and_report_indeterminate_outcomes(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    rows = [{"id": k, "cookie": f"SAPISID=fixture-{k}", "project_id": "offline"} for k in "abcdef"]
    state.set_cookie_accounts(rows)
    monkeypatch.setattr(credentials_api, "app_state", state)
    monkeypatch.setattr(cookie_probe, "credential_manager", Mock())
    monkeypatch.setattr(cookie_probe, "build_headers", lambda value: {"cookie": value})
    token = console_auth.issue_session()

    async def exercise():
        started, queued, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        class Slots(asyncio.Semaphore):
            async def acquire(self):
                if self.locked():
                    queued.set()
                return await super().acquire()
        monkeypatch.setattr(cookie_probe, "_probe_semaphore", Slots(4))
        calls = []
        async def upstream(request):
            cookie = request.headers["cookie"]
            calls.append(cookie)
            if len(calls) == 4:
                started.set()
            await release.wait()
            code = {"a": 401, "b": 200, "c": 503, "d": 503, "new": 200}[cookie.rsplit("-", 1)[-1]]
            return httpx.Response(code, json={"results": [{"data": {"known": True}}]})
        @asynccontextmanager
        async def open_client(options):
            async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as session:
                yield session
        monkeypatch.setattr(cookie_probe, "open_upstream_client", open_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test",
                                     cookies={console_auth.AUTH_COOKIE: token}) as client:
            path = "/api/cookie-accounts/test"
            batch = asyncio.create_task(client.post(path, json={}))
            try:
                initial = asyncio.create_task(started.wait())
                await asyncio.wait((initial, batch), return_when=asyncio.FIRST_COMPLETED, timeout=3)
                assert initial.done(), batch.result().text if batch.done() else "network slots not reached"
                # C is queued in a *different* worker set behind the same global slot.
                c_only = asyncio.create_task(client.post(path, json={"id": "c"}))
                await asyncio.wait_for(queued.wait(), 3)
                state.set_cookie_accounts([*rows[:2], {**rows[2], "enabled": False}, rows[3],
                                           {**rows[5], "cookie": "SAPISID=fixture-new", "note": "updated"}])
                release.set()
                first, single = await asyncio.gather(batch, c_only)
            finally:
                release.set()
        assert first.status_code == single.status_code == 200
        summary = first.json()
        assert [summary[k] for k in ("total", "valid", "invalid", "errors", "skipped")] == [6, 2, 1, 2, 1]
        assert [r["id"] for r in summary["results"]] == list("abcdef")
        assert [r["status"] for r in summary["results"]] == ["invalid", "valid", "error", "error", "skipped", "valid"]
        assert summary["results"][-1]["label"] == "updated"
        assert single.json()["skipped"] == 1 and single.json()["invalid"] == 0
        assert calls == [f"SAPISID=fixture-{k}" for k in ("a", "b", "c", "d", "new")]
        assert "SAPISID=" not in first.text + single.text
    try:
        asyncio.run(exercise())
    finally:
        console_auth.revoke_session(token)
