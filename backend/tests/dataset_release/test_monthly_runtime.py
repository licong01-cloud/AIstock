from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.monthly_runtime import (
    MonthlyRuntimeConfigurationError,
    MonthlyRuntimeSettings,
    SubmissionOnlyPipeline,
)
from backend.services.dataset_release.monthly_unified import MonthlyReleaseRequest
from backend.services.dataset_release.monthly_worker import MonthlyProducerError, _artifact_refs


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


@pytest.mark.parametrize("observed,expected", [
    ("2026-10-29T08:00:00+00:00", date(2026, 9, 30)),
    ("2026-10-30T06:59:59+00:00", date(2026, 9, 30)),
    ("2026-10-30T07:00:00+00:00", date(2026, 10, 30)),
    ("2026-10-31T02:00:00+00:00", date(2026, 10, 30)),
    ("2026-11-01T02:00:00+00:00", date(2026, 10, 30)),
])
def test_cutoff_uses_official_last_closed_trading_day(monkeypatch, observed, expected):
    from backend.services.dataset_release import monthly_runtime
    from backend.services.dataset_release import control_service
    from backend.services import trading_calendar_status

    monkeypatch.setattr(control_service, "resolve_previous_month_trading_cutoff",
                        lambda value: date(2026, 10, 30) if value.month == 11 else date(2026, 9, 30))
    monkeypatch.setattr(trading_calendar_status, "TradingCalendarStatusService", lambda: SimpleNamespace(
        latest_trading_day_on_or_before=lambda end: date(2026, 10, 30) if end.month == 10 else date(2026, 11, 30)))
    assert monthly_runtime.resolve_monthly_cutoff(datetime.fromisoformat(observed).astimezone(UTC)) == expected


def test_cutoff_cannot_invent_missing_official_month(monkeypatch):
    from backend.services.dataset_release.monthly_runtime import resolve_monthly_cutoff
    from backend.services import trading_calendar_status

    monkeypatch.setattr(trading_calendar_status, "TradingCalendarStatusService", lambda: SimpleNamespace(
        latest_trading_day_on_or_before=lambda end: None))
    with pytest.raises(MonthlyRuntimeConfigurationError, match="official calendar"):
        resolve_monthly_cutoff(datetime(2026, 10, 30, 16))


def test_worker_runtime_accepts_planned_profile_root_not_arbitrary_paths(monkeypatch, tmp_path):
    roots = _environment(monkeypatch, tmp_path)
    settings = MonthlyRuntimeSettings.from_env()
    registry = SimpleNamespace(pipeline=lambda **kwargs: SimpleNamespace(**kwargs))
    service = settings.worker_service(registry)
    profile = roots["profiles"] / "candidate.json"
    profile.write_bytes(b"{}\n")
    refs, _ = _artifact_refs(
        [{"id": profile.name, "path": str(profile)}],
        roots=service.pipeline.artifact_roots, label="output",
    )
    assert refs[0]["id"] == profile.name
    outside = tmp_path / "unplanned.json"
    outside.write_bytes(b"{}\n")
    with pytest.raises(MonthlyProducerError, match="escaped"):
        _artifact_refs([{"id": outside.name, "path": str(outside)}],
                       roots=service.pipeline.artifact_roots, label="output")


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
