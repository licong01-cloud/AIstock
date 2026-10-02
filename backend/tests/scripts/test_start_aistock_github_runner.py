from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "start_aistock_github_runner.ps1"
SUPERVISOR = ROOT / "scripts" / "supervise_aistock_github_runner.ps1"


def _powershell() -> str | None:
    return shutil.which("powershell.exe") or shutil.which("powershell")


def test_runner_start_helper_does_not_depend_on_cim_process_scan() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "Get-CimInstance" not in text
    assert "Win32_Process" not in text
    assert "Get-Process" in text
    assert "supervise-aistock-runner.ps1" in text
    assert "aistock_github_runner_supervisor_state_v1" in text


@pytest.mark.skipif(sys.platform != "win32" or not _powershell(), reason="PowerShell helper is Windows-only")
def test_runner_start_helper_dry_run_outputs_json(tmp_path: Path) -> None:
    wrapper = tmp_path / "run-aistock-runner-hidden.cmd"
    wrapper.write_text("@echo off\r\nexit /b 0\r\n", encoding="utf-8")
    (tmp_path / "supervise-aistock-runner.ps1").write_text("# supervisor\n", encoding="utf-8")

    completed = subprocess.run(
        [
            _powershell() or "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            "-InstallRoot",
            str(tmp_path),
            "-DryRun",
            "-Json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["schema_version"] == "aistock_github_runner_start_v1"
    assert payload["status"] == "would_start"
    assert payload["started"] is False
    assert payload["runner_process_count"] == 0
    assert payload["supervisor"].endswith("supervise-aistock-runner.ps1")


@pytest.mark.skipif(sys.platform != "win32" or not _powershell(), reason="PowerShell helper is Windows-only")
def test_runner_start_helper_missing_wrapper_fails_loud(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            _powershell() or "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            "-InstallRoot",
            str(tmp_path),
            "-DryRun",
            "-Json",
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert "GitHub runner wrapper not found" in (completed.stderr + completed.stdout)


@pytest.mark.skipif(sys.platform != "win32" or not _powershell(), reason="PowerShell helper is Windows-only")
def test_runner_start_helper_preserves_explicit_stop_request(tmp_path: Path) -> None:
    (tmp_path / "run-aistock-runner-hidden.cmd").write_text("@echo off\r\n", encoding="ascii")
    (tmp_path / "supervise-aistock-runner.ps1").write_text("# supervisor\n", encoding="utf-8")
    sentinel = tmp_path / ".aistock-runner-stop"
    sentinel.write_text("operator requested stop\n", encoding="utf-8")

    completed = subprocess.run(
        [
            _powershell() or "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            "-InstallRoot",
            str(tmp_path),
            "-Json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "stop_requested"
    assert payload["started"] is False
    assert sentinel.is_file()


@pytest.mark.skipif(sys.platform != "win32" or not _powershell(), reason="PowerShell helper is Windows-only")
def test_runner_supervisor_dry_run_is_bounded_and_non_mutating(tmp_path: Path) -> None:
    wrapper = tmp_path / "run-aistock-runner-hidden.cmd"
    wrapper.write_text("@echo off\r\nexit /b 0\r\n", encoding="ascii")

    completed = subprocess.run(
        [
            _powershell() or "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SUPERVISOR),
            "-InstallRoot",
            str(tmp_path),
            "-DryRun",
            "-Json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["schema_version"] == "aistock_github_runner_supervisor_v1"
    assert payload["status"] == "would_supervise"
    assert payload["max_restarts"] == 12
    assert not (tmp_path / ".aistock-runner-supervisor.json").exists()
    supervisor_text = SUPERVISOR.read_text(encoding="utf-8")
    assert "Local\\AIstockGitHubRunner-" in supervisor_text
    assert "restart_budget_exhausted" in supervisor_text


@pytest.mark.skipif(sys.platform != "win32" or not _powershell(), reason="PowerShell helper is Windows-only")
def test_runner_supervisor_stops_after_bounded_restart_budget(tmp_path: Path) -> None:
    wrapper = tmp_path / "run-aistock-runner-hidden.cmd"
    wrapper.write_text("@echo off\r\nexit /b 0\r\n", encoding="ascii")

    completed = subprocess.run(
        [
            _powershell() or "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SUPERVISOR),
            "-InstallRoot",
            str(tmp_path),
            "-RestartDelaySeconds",
            "1",
            "-RestartWindowSeconds",
            "60",
            "-MaxRestarts",
            "1",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode != 0
    assert "restart budget exhausted" in (completed.stdout + completed.stderr)
    events = [json.loads(line) for line in (tmp_path / "supervisor.jsonl").read_text(encoding="utf-8-sig").splitlines()]
    assert [event["event"] for event in events].count("runner_wrapper_exited") == 2
    assert events[-1]["event"] == "restart_budget_exhausted"
    assert not (tmp_path / ".aistock-runner-supervisor.json").exists()
