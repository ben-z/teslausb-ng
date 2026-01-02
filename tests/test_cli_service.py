"""Tests for the service command in the CLI."""

from subprocess import CompletedProcess
from types import SimpleNamespace

from teslausb import cli


def test_cmd_service_status_returns_exit_code(monkeypatch, tmp_path):
    """Ensure status command returns the underlying exit code."""
    service_path = tmp_path / "teslausb.service"
    service_path.write_text("dummy service")

    calls: list[list[str]] = []

    def fake_run_cmd(cmd, capture_stdout: bool = False):
        calls.append(cmd)
        return CompletedProcess(cmd, 3)

    monkeypatch.setattr(cli, "_run_cmd", fake_run_cmd)
    monkeypatch.setattr(cli, "SYSTEMD_SERVICE_PATH", service_path)

    args = SimpleNamespace(service_command="status", service_parser=None)

    exit_code = cli.cmd_service(args)

    assert exit_code == 3
    assert calls == [["systemctl", "status", "teslausb.service"]]
