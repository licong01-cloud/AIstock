from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "start_aistock_github_runner.ps1"


def _powershell() -> str | None:
    return shutil.which("powershell.exe") or shutil.which("powershell")


def test_runner_start_helper_does_not_depend_on_cim_process_scan() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "Get-CimInstance" not in text
    assert "Win32_Process" not in text
    assert "Get-Process" in text


@pytest.mark.skipif(sys.platform != "win32" or not _powershell(), reason="PowerShell helper is Windows-only")
def test_runner_start_helper_dry_run_outputs_json(tmp_path: Path) -> None:
    wrapper = tmp_path / "run-aistock-runner-hidden.cmd"
    wrapper.write_text("@echo off\r\nexit /b 0\r\n", encoding="utf-8")

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
