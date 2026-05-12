from __future__ import annotations

import inspect
import re
from datetime import date
from pathlib import Path

import pytest

from backend.db import init_trading_core_v2_schema
from backend.routers import quantevolver_evolution
from backend.routers.strategy_packages import _record_payload
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
    AssetCheck,
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
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import StrategyPackageValidationError

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _trading_core_ddl() -> str:
    return "\n".join(init_trading_core_v2_schema.iter_ddl())


def _candidate_table_fragment(sql: str) -> str:
    marker = "CREATE TABLE IF NOT EXISTS strategy_pkg.candidate_strategy_package ("
    match = re.search(re.escape(marker) + r".*?\n\s*\)(?:;|\n)", sql, flags=re.DOTALL)
    if not match:
        raise AssertionError("candidate_strategy_package table DDL not found")
    return match.group(0)


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
    manifest = StrategyPackageManifest(
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
    return freeze_manifest(manifest)


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


def test_candidate_from_qe_experiment_assembles_strategy_manifest_snapshot() -> None:
    class Resolver:
        def build_from_experiment(self, experiment_id: str):
            manifest = _make_manifest()
            source = manifest.source.model_copy(update={"source_id": experiment_id})
            return freeze_manifest(manifest.model_copy(update={"source": source, "manifest_sha256": None}))

    service = CandidateStrategyPackageService(
        repository=InMemoryCandidateStrategyPackageRepository(),
        resolver=Resolver(),
    )

    candidate = service.create_from_qe_experiment(
        experiment_id="qe_resolved_exp",
        created_by="unit_test",
        snapshot_config={"ui_context": "experiment_detail", "strategy_package_manifest_sha256": "ui_stale"},
        completeness={"ui_payload": True, "strategy_package_manifest_available": False},
        eligibility={"can_create_strategy_package": False},
        audit_context={"ui_action": "add_to_candidate"},
    )

    manifest = candidate.snapshot_config["strategy_package_manifest"]
    assert manifest["source"]["source_id"] == "qe_resolved_exp"
    assert candidate.snapshot_config["ui_context"] == "experiment_detail"
    assert candidate.snapshot_config["strategy_package_manifest_sha256"] == manifest["manifest_sha256"]
    assert candidate.snapshot_config["strategy_package_manifest_source"] == "QEExperimentSourceResolver"
    assert candidate.completeness["strategy_package_manifest_available"] is True
    assert candidate.completeness["strategy_package_manifest_sha256"] == manifest["manifest_sha256"]
    assert candidate.completeness["ui_payload"] is True
    assert candidate.factor_manifest["factor_ids"] == ["factor_a"]
    assert candidate.model_manifest["model_asset"]["model_id"] == "model_1"
    assert candidate.strategy_manifest["minute_execution_policy"]["algo_code"] == "TWAP"
    assert candidate.metric_snapshot["backtest_summary"]["ic"] == 0.05
    assert candidate.eligibility["can_create_strategy_package"] is True
    assert candidate.audit_context["snapshot_assembler"] == "QEExperimentSourceResolver"
    assert candidate.audit_context["snapshot_assembler_status"] == "assembled"
    assert candidate.audit_context["ui_action"] == "add_to_candidate"


def test_candidate_from_qe_loop_uses_server_manifest_but_keeps_durable_source_id() -> None:
    class Resolver:
        def __init__(self) -> None:
            self.called_with: tuple[str, str] | None = None

        def build_from_evolution_loop(self, *, qe_task_id: str, qe_loop_id: str):
            self.called_with = (qe_task_id, qe_loop_id)
            manifest = _make_manifest()
            source = manifest.source.model_copy(
                update={
                    "source_type": SourceType.QE_EVOLUTION_LOOP,
                    "source_id": qe_task_id,
                    "loop_id": qe_loop_id,
                    "run_id": "qe_exp_loop",
                }
            )
            return freeze_manifest(manifest.model_copy(update={"source": source, "manifest_sha256": None}))

    resolver = Resolver()
    service = CandidateStrategyPackageService(
        repository=InMemoryCandidateStrategyPackageRepository(),
        resolver=resolver,
    )

    candidate = service.create_from_qe_loop(
        task_id="qe_task_resolved",
        loop_id="Loop2",
        created_by="unit_test",
    )

    assert resolver.called_with == ("qe_task_resolved", "Loop2")
    assert candidate.source_id == "qe_task_resolved_Loop2"
    assert candidate.source_loop_id == "qe_task_resolved_Loop2"
    assert candidate.snapshot_config["strategy_package_manifest"]["source"]["loop_id"] == "Loop2"


def test_candidate_manifest_failed_asset_checks_disable_package_creation_hint() -> None:
    class Resolver:
        def build_from_experiment(self, experiment_id: str):
            manifest = _make_manifest()
            return freeze_manifest(
                manifest.model_copy(
                    update={
                        "manifest_sha256": None,
                        "asset_checks": [
                            AssetCheck(
                                check_name="model_weight_exists",
                                passed=False,
                                message="missing historical model weight",
                            )
                        ],
                    }
                )
            )

    service = CandidateStrategyPackageService(
        repository=InMemoryCandidateStrategyPackageRepository(),
        resolver=Resolver(),
    )

    candidate = service.create_from_qe_experiment(
        experiment_id="qe_missing_weight",
        created_by="unit_test",
        eligibility={"can_create_strategy_package": True},
    )

    assert candidate.completeness["strategy_package_manifest_available"] is True
    assert candidate.completeness["failed_asset_checks"][0]["check_name"] == "model_weight_exists"
    assert candidate.eligibility["can_create_strategy_package"] is False


def test_candidate_from_qe_source_records_non_blocking_assembler_error() -> None:
    class Resolver:
        def build_from_experiment(self, experiment_id: str):
            raise StrategyPackageValidationError(
                "QE experiment missing execution_algo",
                context={"experiment_id": experiment_id},
            )

    repo = InMemoryCandidateStrategyPackageRepository()
    candidate_service = CandidateStrategyPackageService(repository=repo, resolver=Resolver())

    candidate = candidate_service.create_from_qe_experiment(
        experiment_id="qe_legacy_missing_runtime_contract",
        created_by="unit_test",
        snapshot_config={"ui_context": "legacy_experiment_detail"},
    )

    assert "strategy_package_manifest" not in candidate.snapshot_config
    assert candidate.snapshot_config["ui_context"] == "legacy_experiment_detail"
    assert candidate.completeness["strategy_package_manifest_available"] is False
    assert candidate.completeness["snapshot_assembler_error"]["type"] == "StrategyPackageValidationError"
    assert "missing execution_algo" in candidate.completeness["snapshot_assembler_error"]["message"]
    assert candidate.audit_context["snapshot_assembler_status"] == "failed_non_blocking"

    service = StrategyPackageService(
        repository=InMemoryStrategyPackageRepository(),
        candidate_service=candidate_service,
    )
    with pytest.raises(StrategyPackageValidationError, match="requires a strategy_package_manifest snapshot"):
        service.create_from_candidate(candidate.candidate_id)


def test_candidate_snapshot_refresh_can_enrich_existing_legacy_candidate() -> None:
    class Resolver:
        def __init__(self) -> None:
            self.fail = True

        def build_from_experiment(self, experiment_id: str):
            if self.fail:
                raise StrategyPackageValidationError("legacy QE row missing runtime contract")
            manifest = _make_manifest()
            source = manifest.source.model_copy(update={"source_id": experiment_id})
            return freeze_manifest(manifest.model_copy(update={"source": source, "manifest_sha256": None}))

    resolver = Resolver()
    repo = InMemoryCandidateStrategyPackageRepository()
    service = CandidateStrategyPackageService(repository=repo, resolver=resolver)
    candidate = service.create_from_qe_experiment(
        experiment_id="qe_refresh_exp",
        created_by="unit_test",
    )
    assert "strategy_package_manifest" not in candidate.snapshot_config
    assert candidate.completeness["strategy_package_manifest_available"] is False

    resolver.fail = False
    refreshed = service.refresh_snapshot_from_source(
        candidate_id=candidate.candidate_id,
        refreshed_by="unit_test",
    )

    assert refreshed.candidate_id == candidate.candidate_id
    assert refreshed.snapshot_config["strategy_package_manifest"]["source"]["source_id"] == "qe_refresh_exp"
    assert refreshed.completeness["strategy_package_manifest_available"] is True
    assert refreshed.audit_context["snapshot_refreshed_by"] == "unit_test"

    package_service = StrategyPackageService(
        repository=InMemoryStrategyPackageRepository(),
        candidate_service=service,
    )
    package_record = package_service.create_from_candidate(refreshed.candidate_id)
    payload = _record_payload(package_record)

    assert payload["source_type"] == SourceType.CANDIDATE_STRATEGY_PACKAGE.value
    assert payload["runtime_config_contract"]["equivalence"]["strategy_semantics_shared"] is True
    assert payload["runtime_config_contract"]["qe_backtest"]["adapter"]["kind"] == "qe_qlib_bin"
    assert payload["runtime_config_contract"]["paper_v2"]["adapter"]["kind"] == "paper_v2_db"


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
    assert record.current_manifest().manifest_sha256 == record.manifest_sha256
    assert record.manifest_sha256 != manifest.manifest_sha256


def test_promoted_package_survives_candidate_soft_delete() -> None:
    candidate_repo = InMemoryCandidateStrategyPackageRepository()
    candidate_service = CandidateStrategyPackageService(repository=candidate_repo)
    manifest = _make_manifest()
    candidate = candidate_service.create_from_qe_loop(
        task_id="qe_cleanup_source",
        loop_id="Loop3",
        experiment_id="qe_cleanup_source_L3",
        created_by="unit_test",
        archive_run_id="qear_cleanup_source_L3",
        snapshot_config={"strategy_package_manifest": manifest.model_dump(mode="json")},
    )
    package_repo = InMemoryStrategyPackageRepository()
    service = StrategyPackageService(
        repository=package_repo,
        candidate_service=candidate_service,
    )

    record = service.create_from_candidate(candidate.candidate_id)
    deleted_candidate = candidate_service.delete_candidate(
        candidate_id=candidate.candidate_id,
        deleted_by="unit_test",
        delete_reason="source QE experiment cleanup rehearsal",
    )

    assert deleted_candidate.status == CandidateStrategyPackageStatus.DELETED
    assert package_repo.get(record.package_id).source_id == candidate.candidate_id
    assert package_repo.get(record.package_id).run_id == "qear_cleanup_source_L3"


def test_trading_core_schema_declares_durable_candidate_tables_without_qe_cascade() -> None:
    ddl = _trading_core_ddl()

    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.candidate_strategy_package" in ddl
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.candidate_strategy_package_audit" in ddl
    assert "'candidate_strategy_package'" in ddl
    assert "source_id TEXT NOT NULL" in ddl
    assert "archive_run_id TEXT" in ddl
    assert "ON DELETE CASCADE" not in ddl


def test_candidate_schema_keeps_qe_source_and_archive_refs_non_cascading() -> None:
    ddl_sources = [
        _trading_core_ddl(),
        (_REPO_ROOT / "backend/migrations/trading_core_v2_schema.sql").read_text(encoding="utf-8"),
        (_REPO_ROOT / "backend/migrations/strategy_pkg_candidate_strategy_package_20260513.sql").read_text(
            encoding="utf-8"
        ),
    ]

    for ddl in ddl_sources:
        candidate_table = _candidate_table_fragment(ddl)
        assert "source_id TEXT NOT NULL" in candidate_table
        assert "archive_run_id TEXT" in candidate_table
        assert "REFERENCES" not in candidate_table
        assert "ON DELETE" not in candidate_table


def test_qe_source_delete_paths_do_not_delete_candidate_or_strategy_package_state() -> None:
    deletion_sources = [
        _REPO_ROOT / "backend/routers/quantevolver.py",
        _REPO_ROOT / "backend/services/quantevolver/qe_evolution_service.py",
    ]

    for path in deletion_sources:
        source = path.read_text(encoding="utf-8")
        destructive_strategy_pkg_sql = re.findall(
            r"\b(?:DELETE\s+FROM|UPDATE)\s+strategy_pkg\.[a-z_]+",
            source,
            flags=re.IGNORECASE,
        )
        assert destructive_strategy_pkg_sql == []

    experiment_delete_source = deletion_sources[0].read_text(encoding="utf-8")
    task_delete_source = deletion_sources[1].read_text(encoding="utf-8")
    assert "DELETE FROM qe_experiments" in experiment_delete_source
    assert "deleted_experiment_ids" in experiment_delete_source
    assert "DELETE FROM qe_evolution_tasks" in task_delete_source
    assert "DELETE FROM qe_experiments" in task_delete_source


def test_sota_leaderboard_no_longer_materializes_automatic_candidates() -> None:
    source = inspect.getsource(quantevolver_evolution.get_sota_leaderboard)

    assert "automatic_candidates" not in source
    assert "AUTO_CANDIDATE" not in source
    assert "LEFT JOIN strategy_pkg.promotion_review" not in source
    assert "FROM qe_sota_registry" in source
    assert "JOIN qe_evolution_loops" in source
    assert "Candidate StrategyPackages are now created by explicit user action" in source
