from __future__ import annotations

import pytest

from backend.services.quantevolver import wsl_runtime_guard as guard


def test_assert_wsl_runtime_blocks_windows(monkeypatch):
    monkeypatch.setattr(guard, "runtime_info", lambda: guard.WslRuntimeInfo(False, "nt", "Windows", "10", "test"))

    with pytest.raises(guard.WslRuntimeRequiredError) as exc:
        guard.assert_wsl_runtime("official_factor_full_compute")

    payload = exc.value.to_dict()
    assert payload["success"] is False
    assert payload["error_code"] == "wsl_runtime_required"
    assert payload["operation"] == "official_factor_full_compute"


def test_assert_wsl_runtime_allows_wsl(monkeypatch):
    monkeypatch.setattr(guard, "runtime_info", lambda: guard.WslRuntimeInfo(True, "posix", "Linux", "microsoft-standard-WSL2", "test"))

    guard.assert_wsl_runtime("correlation_compute_local")
