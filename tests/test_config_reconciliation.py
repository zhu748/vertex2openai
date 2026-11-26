"""Regression tests for console worker reconciliation and startup configuration."""
import os
from pathlib import Path
import subprocess
import sys
from unittest import mock

from fastapi.testclient import TestClient

import config
import main
import runtime_state
from routes import settings_api


def test_policy_only_save_reconciles_keepalive_after_persistence(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    state.update_settings({"keepalive_allow_private": True})
    monkeypatch.setattr(settings_api, "app_state", state)
    monkeypatch.setattr(config, "API_KEY", "config-test")
    observed = []

    async def record_policy():
        observed.append(runtime_state.AppState().get_setting("keepalive_allow_private"))

    service = mock.Mock(restart=mock.AsyncMock(side_effect=record_policy))
    monkeypatch.setattr(settings_api, "keepalive_service", service)
    client = TestClient(main.app)
    try:
        assert client.post("/api/login", json={"password": "config-test"}).status_code == 200
        result = client.post("/api/settings", json={"keepalive_allow_private": False})
        assert result.status_code == 200
        assert result.json()["keepalive_allow_private"] is False
        assert observed == [False]
    finally:
        client.close()

def test_first_import_keeps_per_account_projects_across_reload(tmp_path):
    # Exercise config construction and persisted startup import, not just its parser.
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "app"),
               STATE_DIR=str(tmp_path), GOOGLE_COOKIE="dummy=one\ndummy=two",
               GOOGLE_PROJECT_ID="env-default", GOOGLE_PROJECT_IDS='["one","two"]')
    env.pop("GOOGLE_COOKIES", None)
    code = """
import config
from runtime_state import app_state, AppState
app_state.set_project_id("legacy-default")
assert app_state.import_env_cookie_accounts_once() == 2
assert [r["project_id"] for r in AppState().get_cookie_accounts()] == ["one", "two"]
"""
    subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env, check=True, timeout=10)


def test_legacy_saved_default_is_still_applied_without_per_account_projects(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(config, "GOOGLE_COOKIE", "dummy=one")
    monkeypatch.setattr(config, "GOOGLE_COOKIES", None)
    monkeypatch.setattr(config, "GOOGLE_PROJECT_ID", "env-default")
    state = runtime_state.AppState()
    state.set_project_id("legacy-default")
    assert state.import_env_cookie_accounts_once() == 1
    assert runtime_state.AppState().get_cookie_accounts()[0]["project_id"] == "legacy-default"


def test_environment_key_keeps_proxy_order_when_a_json_entry_lists_multiple(tmp_path, monkeypatch):
    from credential_config import parse_key_records

    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    records = parse_key_records(
        '[{"key":"demo","proxies":["http://existing.test:80"]},'
        '"http://first.test:80,http://second.test:80,direct","another-key"]')
    assert state.import_env_express_keys_once(records) == 2
    first, second = runtime_state.AppState().get_express_keys()
    assert first["proxies"] == ["http://existing.test:80", "http://first.test:80",
                                "http://second.test:80", "direct"]
    assert second.get("proxies", []) == []


def test_inline_proxy_fallbacks_keep_the_effective_legacy_primary_only_when_not_overridden(tmp_path, monkeypatch):
    from credential_config import parse_key_records

    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(runtime_state, "get_effective_proxy_urls", lambda: ["http://global.test:80"])
    primary, fallback = "http://primary.test:80", "http://fallback.test:80"
    # The legacy field accepts the same list syntaxes as a normal account.
    # Never resurrect it when the modern field is present, even when empty.
    records = parse_key_records([
        {"key": "legacy", "proxy": primary}, fallback, "direct",
        {"key": "modern", "proxy": primary, "proxies": ["http://modern.test:80"]}, fallback,
        {"key": "empty", "proxy": primary, "proxies": []}, fallback,
        {"key": "old-list", "proxy": primary + "\nhttp://second.test:80"}, fallback,
    ])
    assert runtime_state.AppState().import_env_express_keys_once(records) == 4
    candidates = {record["key"]: runtime_state.get_effective_proxy_uris_for_account(record)
                  for record in runtime_state.AppState().get_express_keys()}
    assert candidates == {
        "legacy": [primary, fallback, "direct", "http://global.test:80"],
        "modern": ["http://modern.test:80", fallback, "http://global.test:80"],
        "empty": [fallback, "http://global.test:80"],
        "old-list": [primary, "http://second.test:80", fallback, "http://global.test:80"],
    }
