from __future__ import annotations

from pathlib import Path

from backend.services import manual_factor_service as svc
from backend.services.manual_factor_service import _build_wsl_copy_file_script


def test_wsl_result_copy_script_uses_windows_mount_not_unc() -> None:
    script = _build_wsl_copy_file_script(
        "/home/lc999/factor_workspace/_factor_demo/result.h5",
        Path("F:/Dev/AIstock/.codex_tmp/result.h5"),
    )

    assert "\\\\wsl" not in script.lower()
    assert "wsl.localhost" not in script.lower()
    assert "/mnt/f/Dev/AIstock/.codex_tmp/result.h5" in script
    assert "cp /home/lc999/factor_workspace/_factor_demo/result.h5" in script


def test_factor_workspace_uses_active_dataset_profile_before_legacy_env(monkeypatch) -> None:
    monkeypatch.setattr(svc, "FACTOR_WORKSPACE_WSL", "/legacy/r7/factor_data")
    monkeypatch.setattr(
        "backend.services.quantevolver.node_execution.resolve_default_qe_node_id",
        lambda: "wsl2-5080",
    )
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_active_dataset_profile.resolve_active_dataset_node_binding",
        lambda *, node_id: {
            "node_id": node_id,
            "factor_data_dir": "/releases/r8/components/factor_h5_static_candidate_v2",
        },
    )

    assert svc._require_factor_workspace_wsl() == "/releases/r8/components/factor_h5_static_candidate_v2"


def test_factor_workspace_uses_legacy_env_only_without_active_profile(monkeypatch) -> None:
    monkeypatch.setattr(svc, "FACTOR_WORKSPACE_WSL", "/legacy/factor_data")
    monkeypatch.setattr(
        "backend.services.quantevolver.node_execution.resolve_default_qe_node_id",
        lambda: "wsl2-5080",
    )
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_active_dataset_profile.resolve_active_dataset_node_binding",
        lambda *, node_id: None,
    )

    assert svc._require_factor_workspace_wsl() == "/legacy/factor_data"
