"""The global proxy reader must return a parse of its own snapshot under concurrent edits."""
from concurrent.futures import ThreadPoolExecutor
from threading import Event, current_thread

import pytest

from proxy_config import ProxyFormatError
import runtime_state


@pytest.mark.parametrize("same_key", [False, True], ids=["hot-edit", "same-input"])
def test_waiting_parse_cannot_expose_or_publish_a_mismatched_pair(monkeypatch, tmp_path, same_key):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "state.json"))
    state = runtime_state.AppState()
    monkeypatch.setattr(runtime_state, "app_state", state)
    state.update_settings({"proxy_urls": ["http://seed.invalid:8300"]})
    assert runtime_state.get_effective_proxy_urls() == ["http://seed.invalid:8300"]
    older = "http://older.invalid:8301"
    newest = older if same_key else "http://newer.invalid:8302"
    state.update_settings({"proxy_urls": [older]})

    entered, resume = Event(), Event()
    parse = runtime_state.parse_proxy_list

    def delay_one_reader(raw):
        if current_thread().name.startswith("delayed-reader"):
            entered.set()
            assert resume.wait(5)
        return parse(raw)

    monkeypatch.setattr(runtime_state, "parse_proxy_list", delay_one_reader)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="delayed-reader") as pool:
        older_result = pool.submit(runtime_state.get_effective_proxy_urls)
        try:
            assert entered.wait(5)
            if not same_key:
                state.update_settings({"proxy_urls": [newest]})
            assert runtime_state.get_effective_proxy_urls() == [newest]
        finally:
            resume.set()
        assert older_result.result(5) == [older]
    assert runtime_state.get_effective_proxy_urls() == [newest]


def test_parse_failure_never_caches_a_previous_result_under_the_invalid_input(monkeypatch):
    raw = ["http://last-good.invalid:8304"]
    monkeypatch.setattr(runtime_state.app_state, "get_raw_stored_setting", lambda _: raw)
    assert runtime_state.get_effective_proxy_urls() == list(raw)
    raw = '{"iceServers":'
    for _ in range(2):
        with pytest.raises(ProxyFormatError):
            runtime_state.get_effective_proxy_urls()


def test_cache_owns_immutable_input_and_returns_independent_lists(monkeypatch):
    raw = ["http://mutable.invalid:8305"]
    monkeypatch.setattr(runtime_state.app_state, "get_raw_stored_setting", lambda _: raw)
    saved = runtime_state.get_effective_proxy_urls()
    saved.clear()
    assert runtime_state.get_effective_proxy_urls() == raw
    raw[0] = "http://updated.invalid:8306"
    assert runtime_state.get_effective_proxy_urls() == raw


@pytest.mark.parametrize("as_list", [True, False])
def test_structured_imports_continue_to_follow_their_current_value(monkeypatch, as_list):
    record = {"type": "http", "host": "structured.invalid", "port": 8307}
    raw = [record] if as_list else record
    monkeypatch.setattr(runtime_state.app_state, "get_raw_stored_setting", lambda _: raw)
    assert runtime_state.get_effective_proxy_urls() == ["http://structured.invalid:8307"]
    record["port"] = 8308
    assert runtime_state.get_effective_proxy_urls() == ["http://structured.invalid:8308"]
