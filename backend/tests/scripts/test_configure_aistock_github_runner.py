from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "configure_aistock_github_runner.ps1"


def _powershell() -> str | None:
    return shutil.which("powershell.exe") or shutil.which("powershell")


def test_runner_configure_helper_keeps_registration_token_out_of_arguments() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "AISTOCK_GITHUB_RUNNER_REGISTRATION_TOKEN" in text
    assert "[string]$RegistrationToken" not in text
    assert "--replace" not in text


@pytest.mark.skipif(sys.platform != "win32" or not _powershell(), reason="PowerShell helper is Windows-only")
def test_runner_configure_helper_dry_run_is_bounded_and_non_mutating(tmp_path: Path) -> None:
    allowed_root = tmp_path / "runners"
    install_root = allowed_root / "security"
    archive = tmp_path / "runner.zip"
    template = tmp_path / "runner.cmd"
    allowed_root.mkdir()
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("config.cmd", "@echo off\r\n")
    archive_sha256 = hashlib.sha256(archive.read_bytes()).hexdigest()
    template.write_text('@echo off\r\ncd /d "C:\\old"\r\ncall run.cmd\r\n', encoding="ascii")

    completed = subprocess.run(
        [
            _powershell() or "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            "-AllowedRoot",
            str(allowed_root),
            "-InstallRoot",
            str(install_root),
            "-ArchivePath",
            str(archive),
            "-ArchiveSha256",
            archive_sha256,
            "-TemplateWrapper",
            str(template),
            "-Json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["schema_version"] == "aistock_github_runner_configure_v1"
    assert payload["status"] == "would_configure"
    assert payload["role"] == "security"
    assert not install_root.exists()


@pytest.mark.skipif(sys.platform != "win32" or not _powershell(), reason="PowerShell helper is Windows-only")
def test_runner_configure_helper_rejects_install_root_outside_boundary(tmp_path: Path) -> None:
    allowed_root = tmp_path / "runners"
    archive = tmp_path / "runner.zip"
    template = tmp_path / "runner.cmd"
    allowed_root.mkdir()
    archive.write_bytes(b"placeholder")
    archive_sha256 = hashlib.sha256(archive.read_bytes()).hexdigest()
    template.write_text("@echo off\r\n", encoding="ascii")

    completed = subprocess.run(
        [
            _powershell() or "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            "-AllowedRoot",
            str(allowed_root),
            "-InstallRoot",
            str(tmp_path / "outside"),
            "-ArchivePath",
            str(archive),
            "-ArchiveSha256",
            archive_sha256,
            "-TemplateWrapper",
            str(template),
            "-Json",
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert "must stay below" in (completed.stdout + completed.stderr)
