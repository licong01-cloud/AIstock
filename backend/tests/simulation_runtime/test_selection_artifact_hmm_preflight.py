from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.selection_center.models import SelectionMode
from backend.services.simulation_runtime import SimulationLifecycleScheduler, StaticSimulationRunContextProvider
from backend.services.simulation_runtime.selection import DailySelectionSignalService
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.simulation_runtime.test_lifecycle_scheduler import (
    TRADE_DATE,
    FakeSelectionService,
    _candidate_rows,
    _position_context,
    _release_and_bindings,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class _FailingSelectionService:
    def __init__(self, package_id: str) -> None:
        self.package_id = package_id
        self.calls: list[dict[str, Any]] = []

    def run_selection(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)
        raise DataUnavailableError(
            "HMM coefficient artifact has no coefficients for trade_date",
            context={
                "package_id": self.package_id,
                "trade_date": kwargs["trade_date"].isoformat(),
                "selection_mode": kwargs["mode"].value if isinstance(kwargs.get("mode"), SelectionMode) else kwargs.get("mode"),
            },
        )


def test_scheduler_reuses_daily_selection_once_for_local_and_miniqmt_bindings() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
            }
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert len(fake_selection.calls) == 1
    plans = [item.execution_plan for item in result.results if item.execution_plan is not None]
    assert {plan.selection_evidence_id for plan in plans} == {plans[0].selection_evidence_id}
    assert {plan.selection_evidence_hash for plan in plans} == {plans[0].selection_evidence_hash}


def test_scheduler_reuses_failed_selection_preflight_without_repeating_hmm_sql_work() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    failing_selection = _FailingSelectionService(release.package_id)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=failing_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
            }
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)

    assert result.total_bindings == 2
    assert result.failed_count == 2
    assert len(failing_selection.calls) == 1
    assert {item.error["context"]["package_id"] for item in result.results} == {release.package_id}


class _ForbiddenResolver:
    def __init__(self) -> None:
        self.preflight_calls = 0

    def require_preflight_or_raise(self, **_kwargs: Any) -> None:
        self.preflight_calls += 1
        raise AssertionError("cached authoritative artifact must skip live preflight")


class _ForbiddenArtifactService:
    def __init__(self) -> None:
        self.runtime_asset_resolver = _ForbiddenResolver()
        self.generate_calls = 0

    def generate_from_live_inference(self, **_kwargs: Any) -> None:
        self.generate_calls += 1
        raise AssertionError("cached authoritative artifact must skip live generation")


class _PackageRecord:
    def __init__(self, manifest: Any) -> None:
        self.package_id = manifest.package_id
        self.manifest_sha256 = manifest.manifest_sha256
        self.source_type = manifest.source.source_type.value
        self.source_id = manifest.source.source_id
        self.loop_id = None
        self.run_id = None
        self._manifest = manifest

    def current_manifest(self) -> Any:
        return self._manifest


def test_daily_signal_prepare_window_reuses_cached_authoritative_artifact_without_live_regeneration() -> None:
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {"strategy_id": "pkg_prepare_window_reuse"},
            }
        )
    )
    runtime_config = {
        "selection_artifact_config": {
            "auto_generate": True,
            "include_reference_price": False,
            "pit_mode": "NONE",
        },
        "runtime_profile": {
            "selection": {"top_k": 1},
            "tradability": {"exclude_suspended": False},
        },
    }
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=date(2026, 6, 1),
            data_source="DB_HISTORICAL",
            runtime_config_hash=selection_artifact_runtime_hash(runtime_config),
            scores_json=[
                {
                    "symbol": "000001.SZ",
                    "score": 0.90,
                    "rank": 1,
                    "target_weight": 1.0,
                    "reference_price": 10.0,
                    "component_scores": {"artifact_source": AUTHORITATIVE_SELECTION_SOURCE_TYPE},
                    "reason": "cached_prepare_window_artifact",
                }
            ],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
            },
        )
    )
    artifact_service = _ForbiddenArtifactService()
    signal_service = DailySelectionSignalService(
        runtime=StrategyPackageRuntime(artifact_repository=artifact_repo),
        selection_artifact_service=artifact_service,
    )
    record = _PackageRecord(manifest)

    first = signal_service.build_signal_snapshot(
        record=record,
        trade_date=date(2026, 6, 1),
        data_source="DB_HISTORICAL",
        runtime_config=runtime_config,
    )
    second = signal_service.build_signal_snapshot(
        record=record,
        trade_date=date(2026, 6, 1),
        data_source="DB_HISTORICAL",
        runtime_config=runtime_config,
    )

    assert [item.symbol for item in first.candidates] == ["000001.SZ"]
    assert [item.symbol for item in second.candidates] == ["000001.SZ"]
    assert artifact_service.runtime_asset_resolver.preflight_calls == 0
    assert artifact_service.generate_calls == 0
