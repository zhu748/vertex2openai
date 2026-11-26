"""Only the full pytest stage needs the longer finite project-check budget."""
import subprocess
from types import SimpleNamespace

import pytest

from scripts import check_project


@pytest.mark.parametrize(("timed_out", "seconds"), [(None, None), ("pytest", 240), ("ruff", 120)])
def test_timeouts_remain_failures_with_diagnostics_and_checks_continue(
    monkeypatch, capsys, timed_out, seconds,
):
    calls = []

    def run(command, *, cwd, capture_output, text, timeout):
        assert cwd == check_project.ROOT and capture_output and text
        args = command[1:]
        calls.append((args, timeout))
        if timed_out and args[:2] == ["-m", timed_out] and len(
            [values for values, _ in calls if values[:2] == args[:2]]
        ) == 1:
            # TimeoutExpired exposes bytes even though the subprocess is in text mode.
            raise subprocess.TimeoutExpired(command, timeout, output=b"partial output\n",
                                            stderr="正在运行\n".encode())
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(check_project, "subprocess", SimpleNamespace(
        run=run, TimeoutExpired=subprocess.TimeoutExpired,
    ))
    assert check_project.main() == int(timed_out is not None)
    assert len(calls) == 9
    assert calls[-1] == (["scripts/probe_sdk_channel_profiles.py"], 120)
    assert [(args, limit) for args, limit in calls if limit != 120] == [
        (["-m", "pytest", "tests/", "-q"], 240)
    ]
    captured = capsys.readouterr()
    if timed_out:
        assert "8/9 组检查通过" in captured.out
        assert "partial output" in captured.out
        assert f"检查超时（{seconds} 秒）" in captured.err
        assert "正在运行" in captured.err
    else:
        assert "9/9 组检查通过" in captured.out
        assert captured.err == ""
