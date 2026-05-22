from __future__ import annotations

from datetime import date

import pytest

from backend.services.selection_center.models import SelectionMode, SelectionRunStatus
from backend.services.selection_center.repository import InMemorySelectionCenterRepository
from backend.services.selection_center.runtime_profile import runtime_profile_config_sha256, validate_runtime_profile_binding
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    InMemorySimulationRuntimeRepository,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime.selection import StrategyPackageSelectionService
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class NoopRefreshAudit:
    def require_success(self, **_kwargs):
        return None


class FakeSuspendLookup:
    def get_suspended_symbols(self, symbols: list[str], trade_date: date) -> dict[str, dict]:
        return {}


class FakeCalendar:
    def ensure_trading_day(self, trade_date: date) -> None:
        return None

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        return [date(2024, 1, 2)]


def _runtime_config(*, top_k: int = 2) -> dict:
    return {
        "runtime_profile": {
            "selection": {"top_k": top_k},
            "tradability": {"exclude_suspended": False},
        }
    }


def _versioned_runtime_config(*, top_k: int = 2) -> dict:
    payload = _runtime_config(top_k=top_k)
    payload["runtime_profile_binding"] = {
        "source": "selection_runtime_profile_version",
        "profile_version_id": "unit_selection_profile_v1",
        "config_sha256": runtime_profile_config_sha256(payload),
        "trade_enabled": True,
    }
    return payload


def _seed_artifact(
    artifact_repo: InMemorySelectionScoreArtifactRepository,
    manifest,
    rows: list[dict],
    *,
    trade_date: date = date(2024, 1, 2),
    runtime_config: dict | None = None,
) -> None:
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=trade_date,
            data_source="DB_HISTORICAL",
            runtime_config_hash=selection_artifact_runtime_hash(runtime_config or {}),
            scores_json=rows,
            score_count=len(rows),
            universe_count=len(rows),
            top_score_symbol=rows[0]["symbol"] if rows else None,
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                "test_seeded": True,
            },
        )
    )


def _package_with_artifact(rows: list[dict]):
    package_repo = InMemoryStrategyPackageRepository()
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {"strategy_id": "pkg_shared_selection"},
            }
        )
    )
    package_repo.save_manifest(manifest)
    _seed_artifact(artifact_repo, manifest, rows)
    runtime = StrategyPackageRuntime(artifact_repository=artifact_repo)
    return package_repo, artifact_repo, manifest, runtime


def _release_for(manifest, runtime_config: dict, repository: InMemorySimulationRuntimeRepository):
    profile_hash = runtime_profile_config_sha256(runtime_config)
    return StrategyRuntimeReleaseService(repository=repository).create_release(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256 or "",
        runtime_profile_id="unit_runtime_profile",
        runtime_profile_version_id="unit_runtime_profile_v1",
        runtime_profile_sha256=profile_hash,
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="unit_execution_policy_v1",
        execution_policy_sha256="unit_execution_policy_hash",
        tail_policy_version_id="unit_tail_policy_v1",
        tail_policy_sha256="unit_tail_policy_hash",
        created_by="unit_test",
        created_reason="shared selection evidence test",
    )


def _selection_service(
    package_repo,
    runtime,
    repository: InMemorySimulationRuntimeRepository,
) -> StrategyPackageSelectionService:
    return StrategyPackageSelectionService(
        package_repository=package_repo,
        runtime=runtime,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        calendar_provider=FakeCalendar(),
        repository=repository,
    )


def test_strategy_package_selection_service_persists_release_backed_evidence() -> None:
    rows = [
        {"symbol": "000001.SZ", "score": 0.9, "rank": 1, "reference_price": 10.0},
        {"symbol": "000002.SZ", "score": 0.8, "rank": 2, "reference_price": 11.0},
    ]
    package_repo, _artifact_repo, manifest, runtime = _package_with_artifact(rows)
    runtime_repo = InMemorySimulationRuntimeRepository()
    config = _runtime_config(top_k=2)
    release = _release_for(manifest, config, runtime_repo)

    result = _selection_service(package_repo, runtime, runtime_repo).run_selection(
        package_ids=[manifest.package_id],
        mode=SelectionMode.SINGLE_PACKAGE,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=config,
        runtime_release=release,
        created_by="unit_test",
    )

    evidence = result.evidence_by_package[manifest.package_id]
    assert evidence.release_id == release.release_id
    assert evidence.release_hash == release.release_hash
    assert evidence.runtime_profile_version_id == release.runtime_profile_version_id
    assert evidence.runtime_profile_hash == release.runtime_profile_sha256
    assert evidence.candidate_count == 2
    assert evidence.excluded_count == 0
    assert runtime_repo.get_daily_selection_evidence(evidence.evidence_id).artifact_hash == evidence.artifact_hash
    assert result.runtime_config["daily_selection_evidence"]["evidence_ids_by_package"][manifest.package_id] == evidence.evidence_id
    assert [item.symbol for item in result.aggregate_results] == ["000001.SZ", "000002.SZ"]


@pytest.mark.parametrize(
    "bad_runtime_config",
    [
        {"broker_backend": "minqmt_sim"},
        {"broker_account_id": "qmt_account"},
        {"capital_allocation": 10_000_000},
        {"execution_policy": {"algo_code": "TWAP"}},
        {"tail_policy": {"unfilled": "cancel"}},
        {"target_positions": [{"symbol": "000001.SZ"}]},
        {"rebalance_intents": []},
        {"nested": {"order_remark_prefix": "should_not_enter_selection"}},
    ],
)
def test_strategy_package_selection_service_rejects_trading_and_broker_fields(bad_runtime_config: dict) -> None:
    package_repo, _artifact_repo, manifest, runtime = _package_with_artifact(
        [{"symbol": "000001.SZ", "score": 0.9, "rank": 1, "reference_price": 10.0}]
    )
    runtime_repo = InMemorySimulationRuntimeRepository()

    with pytest.raises(StrategyPackageValidationError, match="Selection-only signal generation"):
        _selection_service(package_repo, runtime, runtime_repo).run_selection(
            package_ids=[manifest.package_id],
            mode=SelectionMode.SINGLE_PACKAGE,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config=bad_runtime_config,
        )


def test_selection_center_uses_shared_selection_evidence_service() -> None:
    rows = [
        {"symbol": "000001.SZ", "score": 0.9, "rank": 1, "reference_price": 10.0},
        {"symbol": "000002.SZ", "score": 0.8, "rank": 2, "reference_price": 11.0},
    ]
    package_repo, _artifact_repo, manifest, runtime = _package_with_artifact(rows)
    runtime_repo = InMemorySimulationRuntimeRepository()
    strategy_selection_service = _selection_service(package_repo, runtime, runtime_repo)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=runtime,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        strategy_selection_service=strategy_selection_service,
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=_versioned_runtime_config(top_k=2),
    )

    evidence_refs = run.runtime_config["daily_selection_evidence"]
    evidence_id = evidence_refs["evidence_ids_by_package"][manifest.package_id]
    assert run.status == SelectionRunStatus.SUCCEEDED
    assert runtime_repo.get_daily_selection_evidence(evidence_id).package_id == manifest.package_id
    assert evidence_refs["artifact_hash_by_package"][manifest.package_id]
    assert [item.symbol for item in run.aggregate_results] == ["000001.SZ", "000002.SZ"]


def test_strategy_package_selection_refreshes_default_binding_after_pit_finalization() -> None:
    pit_config = {
        "selection_artifact_config": {
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "cutoff_date": "2024-01-02",
        }
    }
    rows = [
        {"symbol": "000001.SZ", "score": 0.9, "rank": 1, "reference_price": 10.0},
        {"symbol": "000002.SZ", "score": 0.8, "rank": 2, "reference_price": 11.0},
    ]
    package_repo, _artifact_repo, manifest, runtime = _package_with_artifact(rows)
    _seed_artifact(_artifact_repo, manifest, rows, trade_date=date(2024, 1, 3), runtime_config=pit_config)
    runtime_repo = InMemorySimulationRuntimeRepository()

    result = _selection_service(package_repo, runtime, runtime_repo).run_selection(
        package_ids=[manifest.package_id],
        mode=SelectionMode.SINGLE_PACKAGE,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
        runtime_config=pit_config,
        created_by="unit_test",
    )

    binding = validate_runtime_profile_binding(result.runtime_config)
    assert result.runtime_config["selection_artifact_config"]["cutoff_date"] == "2024-01-02"
    assert result.runtime_config["point_in_time_context"]["score_trade_date"] == "2024-01-02"
    assert binding["source"] == "platform_default"
    assert binding["config_sha256"] == runtime_profile_config_sha256(result.runtime_config)
    evidence = result.evidence_by_package[manifest.package_id]
    package_binding = result.runtime_config["package_runtime_configs"][manifest.package_id]["runtime_profile_binding"]
    assert evidence.runtime_profile_hash == package_binding["config_sha256"]
    assert package_binding["config_sha256"] == runtime_profile_config_sha256(
        result.runtime_config["package_runtime_configs"][manifest.package_id]
    )
