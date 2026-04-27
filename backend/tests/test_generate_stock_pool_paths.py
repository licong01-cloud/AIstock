from __future__ import annotations

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
