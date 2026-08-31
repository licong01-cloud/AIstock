from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.advisory_historical_range.composition import (
    build_explicit_historical_range_r5_runtime_factory,
    build_environment_historical_range_r5_application_service,
    explicit_historical_range_connection_factory,
)
from backend.services.advisory_historical_range.service import HistoricalRangeServiceError
from backend.services.advisory_historical_range import runtime_factories


def test_outcome_producer_identity_includes_planner_semantics(tmp_path: Path) -> None:
    planner_path = "backend/services/advisory_historical_range/outcome_planner.py"
    tracked_paths = runtime_factories._OUTCOME_SOURCE_FILES  # noqa: SLF001 - producer identity contract
    for relative_path in set((*tracked_paths, planner_path)):
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"source:{relative_path}\n", encoding="utf-8")

    before = runtime_factories._code_set_hash(tmp_path, tracked_paths)  # noqa: SLF001
    (tmp_path / planner_path).write_text("changed planner timeline semantics\n", encoding="utf-8")
    after = runtime_factories._code_set_hash(tmp_path, tracked_paths)  # noqa: SLF001

    assert planner_path in tracked_paths
    assert after != before


def test_explicit_db_configuration_has_no_legacy_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(HistoricalRangeServiceError) as error:
        explicit_historical_range_connection_factory()
    assert error.value.http_status == 503
    assert "TDX_DB_HOST" in error.value.context["missing_configuration"]


def test_mutation_composition_fails_before_any_business_write(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "TDX_DB_HOST": "127.0.0.1",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock_dev",
        "TDX_DB_USER": "tester",
        "TDX_DB_PASSWORD": "not-used",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    for key in (
        "AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT",
        "AISTOCK_ADVISORY_HISTORICAL_RANGE_TASK_RUNTIME_ROOT",
        "AISTOCK_PACKAGE_ASSET_STORE_ROOT",
        "AISTOCK_REPOSITORY_ROOT",
        "AISTOCK_ADVISORY_HISTORICAL_RANGE_POLICY_COMPONENT_ROOT",
        "AISTOCK_ADVISORY_CALCULATION_EVIDENCE_ROOT",
        "AISTOCK_ADVISORY_DATASET_STORE_ROOT",
    ):
        monkeypatch.delenv(key, raising=False)
    service = build_environment_historical_range_r5_application_service()
    with pytest.raises(HistoricalRangeServiceError) as error:
        service._mutation_runtime_factory()  # noqa: SLF001 - composition boundary contract
    assert error.value.http_status == 503
    assert error.value.context["missing_configuration"]


def test_explicit_mutation_factory_composes_every_r1_r4_boundary(tmp_path: Path) -> None:
    roots = {
        name: tmp_path / name
        for name in (
            "artifacts",
            "runtime",
            "packages",
            "policy-components",
            "calculation-evidence",
            "dataset-store",
        )
    }
    for path in roots.values():
        path.mkdir()
    factory = build_explicit_historical_range_r5_runtime_factory(
        conn_factory=lambda: None,
        artifact_root=roots["artifacts"],
        task_runtime_root=roots["runtime"],
        package_asset_root=roots["packages"],
        repository_root=Path(__file__).resolve().parents[3],
        policy_component_root=roots["policy-components"],
        calculation_evidence_root=roots["calculation-evidence"],
        dataset_store_root=roots["dataset-store"],
    )
    runtime = factory()
    assert runtime.planning is not None
    assert runtime.execution is not None
    assert runtime.outcome is not None
    assert runtime.bridge is not None
    assert runtime.outcome_requests is not None
    assert runtime.bridge_requests is not None
    assert runtime.artifact_store is not None
    assert runtime.outcome_service_factory is not None
