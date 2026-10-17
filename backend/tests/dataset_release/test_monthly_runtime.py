from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from backend.services.dataset_release.monthly_runtime import (
    MonthlyRuntimeConfigurationError,
    MonthlyRuntimeSettings,
    SubmissionOnlyPipeline,
)
from backend.services.dataset_release.monthly_unified import MonthlyReleaseRequest


def _environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, Path]:
    roots = {
        "state": tmp_path / "state",
        "controller": tmp_path / "controller",
        "profiles": tmp_path / "profiles",
        "artifacts": tmp_path / "artifacts",
        "authorizations": tmp_path / "authorizations",
    }
    for path in roots.values():
        path.mkdir()
    active = tmp_path / "active.json"
    active.write_text("{}\n", encoding="utf-8")
    values = {
        "AISTOCK_MONTHLY_RELEASE_STATE_ROOT": roots["state"],
        "AISTOCK_ACTIVE_DATASET_PROFILE_PATH": active,
        "AISTOCK_MONTHLY_CONTROLLER_RELEASE_ROOT": roots["controller"],
        "AISTOCK_MONTHLY_PROFILE_CANDIDATE_ROOT": roots["profiles"],
        "AISTOCK_MONTHLY_RELEASE_ARTIFACT_ROOT": roots["artifacts"],
        "AISTOCK_MONTHLY_WSL_RELEASE_ROOT": "/mnt/wsl/releases",
        "AISTOCK_MONTHLY_NODE1_RELEASE_ROOT": "/home/data/releases",
        "AISTOCK_DATASET_ACTION_AUTHORIZATION_ROOT": roots["authorizations"],
    }
    for name, value in values.items():
        monkeypatch.setenv(name, str(value))
    return {**roots, "active": active}


def test_runtime_settings_expose_paths_but_no_producer_command_configuration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    roots = _environment(monkeypatch, tmp_path)
    monkeypatch.setenv("AISTOCK_MONTHLY_RELEASE_PRODUCER_CONFIG", str(tmp_path / "evil.json"))
    settings = MonthlyRuntimeSettings.from_env()
    assert settings.artifact_root == roots["artifacts"]
    assert not hasattr(settings, "producer_config")
    assert not hasattr(settings, "producer_commands")


def test_submission_runtime_cannot_execute_data_stage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _environment(monkeypatch, tmp_path)
    settings = MonthlyRuntimeSettings.from_env()
    service = settings.service(cutoff_resolver=lambda: date(2026, 9, 30))
    assert isinstance(service.pipeline, SubmissionOnlyPipeline)
    with pytest.raises(MonthlyRuntimeConfigurationError, match="code-registered worker"):
        service.pipeline.run_stage()


def test_runtime_rejects_noncanonical_node_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _environment(monkeypatch, tmp_path)
    monkeypatch.setenv("AISTOCK_MONTHLY_NODE1_RELEASE_ROOT", "/home//bad/")
    with pytest.raises(MonthlyRuntimeConfigurationError, match="canonical POSIX"):
        MonthlyRuntimeSettings.from_env()


def test_runtime_requires_existing_plain_control_roots(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _environment(monkeypatch, tmp_path)
    monkeypatch.setenv("AISTOCK_MONTHLY_RELEASE_ARTIFACT_ROOT", str(tmp_path / "missing"))
    with pytest.raises(MonthlyRuntimeConfigurationError, match="artifact_root is unavailable"):
        MonthlyRuntimeSettings.from_env()


def test_runtime_rejects_overlapping_artifact_and_release_roots(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    roots = _environment(monkeypatch, tmp_path)
    nested = roots["controller"] / "artifacts"
    nested.mkdir()
    monkeypatch.setenv("AISTOCK_MONTHLY_RELEASE_ARTIFACT_ROOT", str(nested))
    with pytest.raises(MonthlyRuntimeConfigurationError, match="must not overlap"):
        MonthlyRuntimeSettings.from_env()


def test_runtime_derives_next_year_release_identity_without_month_constants(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _environment(monkeypatch, tmp_path)
    settings = MonthlyRuntimeSettings.from_env()
    request = MonthlyReleaseRequest(
        target_cutoff=date(2027, 1, 29),
        product_profile="qe_hmm_full_v2",
        idempotency_key="monthly-202701",
    )

    assert settings.candidate_name(request) == (
        "20270129-qe_hmm_full_v2-direct-monthly-v2-unified-candidate"
    )
    assert settings.generation(request) == "20270129-monthly-v2-unified"
    assert settings.revision(request) == "20270129-monthly-v2"
    service = settings.service(cutoff_resolver=lambda: request.target_cutoff)
    assert service.node_root_factory(request) == {
        "wsl2-5080": (
            "/mnt/wsl/releases/"
            "20270129-qe_hmm_full_v2-direct-monthly-v2-unified-candidate"
        ),
        "rdagent-node1": (
            "/home/data/releases/"
            "20270129-qe_hmm_full_v2-direct-monthly-v2-unified-candidate"
        ),
    }
