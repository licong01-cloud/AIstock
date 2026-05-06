from pathlib import Path

import pytest

from backend.services.strategy_package import workspace_policy as policy
from backend.services.trading_core.errors import StrategyPackageValidationError


def test_wsl_process_allows_aistock_owned_mnt_artifact_root(monkeypatch) -> None:
    monkeypatch.setattr(policy, "_is_windows_process", lambda: False)
    monkeypatch.setattr(policy, "_is_wsl_process", lambda: True)
    monkeypatch.setattr(policy, "project_root", lambda: Path("/mnt/f/Dev/AIstock"))

    artifact_root = Path("/mnt/f/Dev/AIstock/rdagent_assets/qe_programs")

    assert policy.is_forbidden_worker_workspace_path(artifact_root) is False
    assert policy.ensure_aistock_artifact_path(artifact_root, purpose="unit test") == artifact_root


def test_wsl_process_still_refuses_worker_workspace_under_mnt(monkeypatch) -> None:
    monkeypatch.setattr(policy, "_is_windows_process", lambda: False)
    monkeypatch.setattr(policy, "_is_wsl_process", lambda: True)
    monkeypatch.setattr(policy, "project_root", lambda: Path("/mnt/f/Dev/AIstock"))

    for worker_path in (
        "/mnt/f/Dev/RD-Agent-main/qe_workspace/demo",
        "/mnt/f/Dev/AIstock/rdagent_assets/qe_workspace/demo",
        "/mnt/f/Dev/RD-Agent-main/rdagent_workspace/task-a",
    ):
        with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
            policy.ensure_not_forbidden_worker_workspace_path(worker_path, purpose="unit test")


def test_windows_process_still_refuses_mnt_shortcut(monkeypatch) -> None:
    monkeypatch.setattr(policy, "_is_windows_process", lambda: True)
    monkeypatch.setattr(policy, "_is_wsl_process", lambda: False)
    monkeypatch.setattr(policy, "project_root", lambda: Path("F:/Dev/AIstock"))

    path = "/mnt/f/Dev/AIstock/rdagent_assets/qe_programs"

    assert policy.is_forbidden_worker_workspace_path(path) is True
    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        policy.ensure_not_forbidden_worker_workspace_path(path, purpose="unit test")
