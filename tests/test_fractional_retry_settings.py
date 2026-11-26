"""Console-accepted retry timings must agree with their float-valued runtime reader."""
import json

from fastapi.testclient import TestClient

import console_auth
import main
import retry_policy
import runtime_state
from routes import settings_api


def test_fractional_backoff_and_jitter_round_trip_without_loosening_retry_counts(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    monkeypatch.setattr(settings_api, "app_state", state)
    monkeypatch.setattr(retry_policy, "app_state", state)
    token = console_auth.issue_session()
    client = TestClient(main.app)
    client.cookies.set(console_auth.AUTH_COOKIE, token)
    try:
        values = {"retry_max": 2, "retry_backoff_seconds": .5, "retry_jitter_seconds": .25}
        r = client.post("/api/settings", json=values)
        assert r.status_code == 200
        assert {key: r.json()[key] for key in values} == values
        assert retry_policy.get_retry_settings() == (2, .5, .25)
        assert runtime_state.AppState().get_setting("retry_jitter_seconds") == .25
        for key, upper in (("retry_backoff_seconds", 120), ("retry_jitter_seconds", 60)):
            for bad in (True, -.25, float("nan"), float("inf"), upper + .25):
                r = client.post("/api/settings", content=json.dumps({key: bad}))
                assert r.status_code == 400 and r.json()["field"] == key
        assert client.post("/api/settings", json={"retry_max": .5}).status_code == 400
        assert retry_policy.get_retry_settings() == (2, .5, .25)
        for backoff, jitter in ((0, 0), (120, 60)):
            assert client.post("/api/settings", json={"retry_backoff_seconds": backoff,
                                                      "retry_jitter_seconds": jitter}).status_code == 200
            assert retry_policy.get_retry_settings() == (2, backoff, jitter)
    finally:
        client.close()
        console_auth.revoke_session(token)
