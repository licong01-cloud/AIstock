from __future__ import annotations

import inspect
from datetime import date

import pytest

from backend.db import init_trading_core_v2_schema
from backend.routers import quantevolver_evolution
from backend.services.strategy_package.candidate import (
    CandidateStrategyPackageService,
    CandidateStrategyPackageSourceType,
    CandidateStrategyPackageStatus,
    InMemoryCandidateStrategyPackageRepository,
)
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaMode,
    BacktestSummary,
    ExecutionPolicy,
    FactorAsset,
    MinuteExecutionPolicy,
    ModelAsset,
    PackageStatus,
    PortfolioPolicy,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
    UniversePolicy,
)
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import StrategyPackageValidationError


def _trading_core_ddl() -> str:
    return "\n".join(init_trading_core_v2_schema.iter_ddl())


def _make_manifest() -> StrategyPackageManifest:
    component = AlphaComponent(
        alpha_id="alpha_001",
        alpha_name="single_alpha",
        component_weight=1.0,
        factor_ids=["factor_a"],
        holding_period="1day",
        rebalance_frequency="1day",
        score_direction="higher_better",
    )
    return StrategyPackageManifest(
        package_name="candidate_test",
        source=StrategyPackageSource(source_type=SourceType.QE_EXPERIMENT, source_id="qe_exp"),
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        alpha_components=[component],
        alpha_combination_policy=AlphaCombinationPolicy(method="identity", weights={"alpha_001": 1.0}),
        factor_set=[FactorAsset(factor_id="factor_a", factor_name="factor_a")],
        model_asset=ModelAsset(model_id="model_1"),
        strategy_config={"strategy_id": "score_weighted_topk_v2"},
        universe_policy=UniversePolicy(stock_pool="test_pool"),
        portfolio_policy=PortfolioPolicy(topk=10, n_drop=1),
        execution_policy=ExecutionPolicy(backtest_freq="1min"),
        minute_execution_policy=MinuteExecutionPolicy(algo_code="TWAP"),
        backtest_summary=BacktestSummary(
            ic=0.05,
            rank_ic=0.04,
            annual_return=0.2,
            max_drawdown=-0.1,
            raw_metrics={"IC": 0.05},
            sample_start=date(2024, 1, 1),
        ),
        package_status=PackageStatus.BACKTEST_APPROVED,
    )


def test_strategy_package_source_type_accepts_candidate_source() -> None:
    source = StrategyPackageSource(
        source_type=SourceType.CANDIDATE_STRATEGY_PACKAGE,
        source_id="csp_demo",
    )

    assert source.source_type == SourceType.CANDIDATE_STRATEGY_PACKAGE


def test_candidate_requires_explicit_manual_action() -> None:
    service = CandidateStrategyPackageService(repository=InMemoryCandidateStrategyPackageRepository())

    with pytest.raises(StrategyPackageValidationError, match="explicit user action"):
        service.create_candidate(
            source_type=CandidateStrategyPackageSourceType.QE_EVOLUTION_LOOP,
            source_id="qe_task_Loop1",
            created_by="unit_test",
            manual_action=False,
        )


def test_candidate_snapshot_is_idempotent_and_independent_from_qe_source() -> None:
    repo = InMemoryCandidateStrategyPackageRepository()
    service = CandidateStrategyPackageService(repository=repo)

    first = service.create_from_qe_loop(
        task_id="qe_20260513_candidate",
        loop_id="Loop1",
        experiment_id="qe_20260513_candidate_exp",
        created_by="unit_test",
        snapshot_config={"factor_config": {"factor_ids": ["alpha001"]}},
        metric_snapshot={"IC": 0.071},
        artifact_refs={"model_weight": {"uri": "artifact://weights/model.pkl", "sha256": "abc"}},
        completeness={"missing_seed": True},
    )
    second = service.create_from_qe_loop(
        task_id="qe_20260513_candidate",
        loop_id="qe_20260513_candidate_Loop1",
        created_by="unit_test_again",
    )

    assert second.candidate_id == first.candidate_id
    assert first.source_id == "qe_20260513_candidate_Loop1"
    assert first.source_experiment_id == "qe_20260513_candidate_exp"
    assert first.metric_snapshot["IC"] == 0.071
    assert first.completeness["missing_seed"] is True
    assert first.audit_context["manual_action"] is True
    assert first.audit_context["paper_enabled"] is False
    assert first.audit_context["live_approved"] is False


def test_candidate_clone_and_soft_delete_do_not_touch_source_or_archive_refs() -> None:
    repo = InMemoryCandidateStrategyPackageRepository()
    service = CandidateStrategyPackageService(repository=repo)
    source = service.create_from_qe_experiment(
        experiment_id="qe_exp_1",
        created_by="unit_test",
        archive_run_id="qear_run_1",
        display_name="QE Exp 1",
    )

    cloned = service.clone_candidate(source_candidate_id=source.candidate_id, created_by="unit_test")
    deleted = service.delete_candidate(
        candidate_id=source.candidate_id,
        deleted_by="unit_test",
        delete_reason="superseded by clone",
    )

    assert cloned.source_type == CandidateStrategyPackageSourceType.CANDIDATE_STRATEGY_PACKAGE
    assert cloned.source_id == source.candidate_id
    assert cloned.archive_run_id == "qear_run_1"
    assert deleted.status == CandidateStrategyPackageStatus.DELETED
    assert deleted.source_id == "qe_exp_1"
    assert deleted.archive_run_id == "qear_run_1"


def test_strategy_package_can_be_created_from_candidate_manifest_snapshot() -> None:
    candidate_repo = InMemoryCandidateStrategyPackageRepository()
    candidate_service = CandidateStrategyPackageService(repository=candidate_repo)
    manifest = _make_manifest()
    candidate = candidate_service.create_from_qe_experiment(
        experiment_id="qe_exp_candidate_source",
        created_by="unit_test",
        archive_run_id="qear_run_candidate_source",
        snapshot_config={"strategy_package_manifest": manifest.model_dump(mode="json")},
    )
    package_repo = InMemoryStrategyPackageRepository()
    service = StrategyPackageService(
        repository=package_repo,
        candidate_service=candidate_service,
    )

    record = service.create_from_candidate(candidate.candidate_id)

    assert record.source_type == SourceType.CANDIDATE_STRATEGY_PACKAGE.value
    assert record.source_id == candidate.candidate_id
    assert record.run_id == "qear_run_candidate_source"
    assert record.current_manifest().source.source_type == SourceType.CANDIDATE_STRATEGY_PACKAGE


def test_trading_core_schema_declares_durable_candidate_tables_without_qe_cascade() -> None:
    ddl = _trading_core_ddl()

    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.candidate_strategy_package" in ddl
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.candidate_strategy_package_audit" in ddl
    assert "'candidate_strategy_package'" in ddl
    assert "source_id TEXT NOT NULL" in ddl
    assert "archive_run_id TEXT" in ddl
    assert "ON DELETE CASCADE" not in ddl


def test_sota_leaderboard_no_longer_materializes_automatic_candidates() -> None:
    source = inspect.getsource(quantevolver_evolution.get_sota_leaderboard)

    assert "automatic_candidates" not in source
    assert "AUTO_CANDIDATE" not in source
    assert "LEFT JOIN strategy_pkg.promotion_review" not in source
    assert "FROM qe_sota_registry" in source
    assert "JOIN qe_evolution_loops" in source
    assert "Candidate StrategyPackages are now created by explicit user action" in source
