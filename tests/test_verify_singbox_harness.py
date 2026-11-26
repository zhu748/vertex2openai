"""Exercise the manual harness's orchestration, without network, binaries or subprocesses."""
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import httpx
import pytest

import proxy_resolver as pr
from scripts import verify_singbox_e2e as verify


TARGET = "http://127.0.0.1:48080/probe"
URI = "socks4://127.0.0.1:41080"
LOCAL = "socks5://127.0.0.1:42100"
REVIVED = "socks5://127.0.0.1:42101"


@pytest.fixture
def harness(monkeypatch):
    events = []
    inst = Mock(port=42100)
    inst.alive.return_value = True
    inst.process.kill.side_effect = lambda: events.append("kill")
    inst.process.wait.side_effect = lambda timeout: events.append("wait")
    instances = {URI: inst}

    def shutdown():
        events.append("shutdown")
        instances.clear()

    def fake_get(url):
        assert url == TARGET
        events.append("first GET")
        return httpx.Response(204)

    async def revive(uri):
        assert uri == URI
        assert instances[uri] is inst
        events.append("restart")
        instances[uri] = Mock(port=42101)
        return REVIVED

    async def fake_revived_get(url):
        assert url == TARGET
        events.append("second GET")
        return httpx.Response(204)

    monkeypatch.setattr(pr, "find_singbox_binary", Mock(return_value="/stub/sing-box"))
    monkeypatch.setattr(pr, "singbox_version", lambda: "stub")
    monkeypatch.setattr(pr, "singbox_manager", SimpleNamespace(_instances=instances, shutdown_all=shutdown))
    monkeypatch.setattr(pr, "resolve_proxy_uri", Mock(return_value=LOCAL))
    monkeypatch.setattr(pr, "resolve_proxy_uri_async", AsyncMock(side_effect=revive))
    first = MagicMock()
    first.return_value.__enter__.return_value.get.side_effect = fake_get
    second = MagicMock()
    second.return_value.__aenter__.return_value.get.side_effect = fake_revived_get
    monkeypatch.setattr(httpx, "Client", first)
    monkeypatch.setattr(httpx, "AsyncClient", second)
    # Replace only the script's reference; asyncio still owns its event-loop sockets.
    sock = MagicMock()
    sock.__enter__.return_value = sock
    sock.bind.side_effect = lambda address: events.append(address)
    monkeypatch.setattr(verify, "socket", SimpleNamespace(
        socket=lambda *_: sock, AF_INET=2, SOCK_STREAM=1, SOL_SOCKET=1, SO_REUSEADDR=2))
    child = Mock(return_value=SimpleNamespace(stdout="ok", stderr="", returncode=0))
    monkeypatch.setattr(verify.subprocess, "run", child)

    def run(uri=URI):
        monkeypatch.setattr(sys, "argv", ["verify_singbox_e2e.py", uri, TARGET])
        return verify.main()

    return SimpleNamespace(run=run, events=events, instances=instances, inst=inst, first=first,
                           second=second, child=child)


def test_converted_uri_must_revive_and_probe_before_shutdown(harness):
    assert harness.run("SOCKS4://127.0.0.1:41080/") == 0
    assert harness.events == ["first GET", "kill", "wait", ("127.0.0.1", 42100),
                              "restart", "second GET", "shutdown"]
    assert harness.first.call_args.kwargs["proxy"] == LOCAL
    assert harness.second.call_args.kwargs["proxy"] == REVIVED
    harness.child.assert_called_once()


@pytest.mark.parametrize("failure", ["missing", "dead", "changed-port", "first-probe",
                                     "same-revived-port", "second-probe"])
def test_conversion_cannot_report_success_when_a_required_stage_fails(harness, failure, capsys):
    if failure == "missing":
        harness.instances.clear()
    elif failure == "dead":
        harness.inst.alive.return_value = False
    elif failure == "changed-port":
        pr.resolve_proxy_uri.side_effect = [LOCAL, REVIVED]
    elif failure == "first-probe":
        harness.first.return_value.__enter__.return_value.get.side_effect = httpx.ConnectError("stub")
    elif failure == "same-revived-port":
        pr.resolve_proxy_uri_async.side_effect = None
        pr.resolve_proxy_uri_async.return_value = LOCAL
    elif failure == "second-probe":
        harness.second.return_value.__aenter__.return_value.get.side_effect = httpx.ConnectError("stub")
    assert harness.run() != 0
    assert "全部通过" not in capsys.readouterr().out
    assert harness.events.count("shutdown") == 1
    assert harness.events[-1] == "shutdown"
    harness.child.assert_not_called()


def test_native_loopback_socks5_is_not_a_converted_uri(harness):
    # Looks exactly like a local converter outlet, but its source needs no engine.
    assert harness.run(LOCAL) == 0
    assert harness.events.count("first GET") == 1
    assert "kill" not in harness.events
    pr.resolve_proxy_uri_async.assert_not_awaited()
    pr.find_singbox_binary.assert_not_called()
    harness.child.assert_not_called()


@pytest.mark.parametrize("uri", ["direct", "not-supported://127.0.0.1:41080", "turn://127.0.0.1:41080"])
def test_out_of_scope_routes_cannot_pass_the_singbox_harness(harness, uri):
    assert harness.run(uri) != 0
    pr.resolve_proxy_uri.assert_not_called()
    harness.first.assert_not_called()
    harness.child.assert_not_called()
