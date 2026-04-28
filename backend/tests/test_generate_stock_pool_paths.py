from __future__ import annotations

import hashlib
import subprocess

import pytest

from scripts import generate_stock_pool


def test_generate_stock_pool_resolves_wsl_instruments_without_unc(monkeypatch) -> None:
    monkeypatch.delenv("QLIB_INSTRUMENTS_WSL", raising=False)
    monkeypatch.setenv("QLIB_DATA_PATH_WSL", "/home/test/qlib_bin")

    resolved = generate_stock_pool._resolve_qlib_instruments_wsl()

    assert resolved == "/home/test/qlib_bin/instruments"
    assert "\\\\wsl" not in resolved.lower()


def test_generate_stock_pool_requires_explicit_wsl_distro(monkeypatch) -> None:
    monkeypatch.delenv("AISTOCK_WSL_DISTRO", raising=False)
    monkeypatch.delenv("QLIB_WSL_DISTRO", raising=False)

    with pytest.raises(RuntimeError, match="AISTOCK_WSL_DISTRO"):
        generate_stock_pool._wsl_distro()


def test_generate_stock_pool_windows_path_to_wsl_mount() -> None:
    assert generate_stock_pool._win_to_wsl("F:\\Dev\\AIstock\\stock_pools\\x.txt") == "/mnt/f/Dev/AIstock/stock_pools/x.txt"


def test_sync_pool_to_qlib_streams_file_and_uses_unique_tmp(monkeypatch, tmp_path) -> None:
    payload = b"000001.SZ\t2018-08-01\t2026-03-10\n"
    output = tmp_path / "filtered_pool_test.txt"
    output.write_bytes(payload)
    calls = []

    def fake_run_wsl(script: str, *, timeout: int = 30, input_bytes: bytes | None = None) -> str:
        calls.append((script, timeout, input_bytes))
        assert input_bytes == payload
        return hashlib.sha256(payload).hexdigest() + "\n"

    monkeypatch.setenv("STOCK_POOL_SYNC_TIMEOUT_SEC", "123")
    monkeypatch.setattr(generate_stock_pool, "_run_wsl", fake_run_wsl)

    dest = generate_stock_pool.sync_pool_to_qlib(
        output,
        "filtered_pool_test",
        "/home/test/qlib_bin/instruments",
    )

    assert dest == "/home/test/qlib_bin/instruments/filtered_pool_test.txt"
    assert calls[0][1] == 123
    script = calls[0][0]
    assert "cat > /home/test/qlib_bin/instruments/.filtered_pool_test." in script
    assert "read -r actual_sha _ < /home/test/qlib_bin/instruments/.filtered_pool_test." in script
    assert '"\\$actual_sha"' in script
    assert '"\\$final_sha"' in script
    assert "$(" not in script
    assert "cp " not in script
    assert "/mnt/" not in script
    assert ".filtered_pool_test.tmp" not in script


def test_run_wsl_timeout_has_actionable_error(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_WSL_DISTRO", "Ubuntu-Test")

    def fake_subprocess_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs["timeout"])

    monkeypatch.setattr(generate_stock_pool.subprocess, "run", fake_subprocess_run)

    with pytest.raises(RuntimeError, match="WSL command timed out after 7s"):
        generate_stock_pool._run_wsl("sleep 999", timeout=7)
