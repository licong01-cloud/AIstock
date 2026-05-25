from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.repository import InMemorySelectionCenterRepository
from backend.services.selection_center.runtime_profile import runtime_profile_config_sha256
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
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
from backend.services.strategy_package.qe_source_resolver import QEExperimentSourceResolver, dict_record_conn
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.selection_center.test_runtime_selection import (
    FakeSuspendLookup,
    NoopRefreshAudit,
)


def _alpha_core_manifest(*, status: PackageStatus = PackageStatus.SELECTION_ENABLED) -> StrategyPackageManifest:
    component = AlphaComponent(
        alpha_id="alpha_001",
        alpha_name="single_alpha",
        component_weight=1.0,
        factor_ids=["factor_a", "factor_b"],
        model_id="model_1",
        model_ref="model_1",
        holding_period="1day",
        rebalance_frequency="1day",
        score_direction="higher_better",
    )
    return freeze_manifest(
        StrategyPackageManifest(
            package_name="alpha_core_boundary",
            source=StrategyPackageSource(source_type=SourceType.QE_EXPERIMENT, source_id="qe_alpha_core"),
            alpha_mode=AlphaMode.SINGLE_ALPHA,
            alpha_components=[component],
            alpha_combination_policy=AlphaCombinationPolicy(method="identity", weights={"alpha_001": 1.0}),
            factor_set=[
                FactorAsset(factor_id="factor_a", factor_name="factor_a"),
                FactorAsset(factor_id="factor_b", factor_name="factor_b"),
            ],
            model_asset=ModelAsset(model_id="model_1", model_type="CATBOOST"),
            source_evidence={
                "schema_version": "strategy_package_source_evidence_v1",
                "authority": "audit_only_not_runtime_authority",
            },
            backtest_context={
                "schema_version": "qe_backtest_context_v1",
                "authority": "source_evidence_not_runtime_authority",
                "daily_strategy": {
                    "strategy_id": "score_weighted_topk_v2",
                    "topk": 2,
                    "n_drop": 1,
                    "custom_params": {
                        "strategy_id": "score_weighted_topk_v2",
                        "topk": 2,
                        "n_drop": 1,
                    },
                },
                "execution": {"backtest_freq": "1min", "execution_algo": "TWAP", "execution_algo_params": {}},
            },
            backtest_summary=BacktestSummary(
                ic=0.05,
                rank_ic=0.04,
                annual_return=0.2,
                max_drawdown=-0.1,
                final_nav=1.2,
                raw_metrics={"IC": 0.05},
                sample_start=date(2024, 1, 1),
            ),
            package_status=status,
        )
    )


def _qe_record() -> dict:
    return {
        "experiment_id": "qe_alpha_core_001",
        "experiment_name": "qe_alpha_core_001",
        "status": "completed",
        "alpha_mode": "single",
        "qe_task_id": "qe_alpha_core_001",
        "qe_loop_id": "Loop7",
        "factor_names": ["factor_a", "factor_b"],
        "model_id": "model_1",
        "strategy_id": "score_weighted_topk_v2",
        "data_split": {"test_start": "2024-01-01", "backtest_end": "2024-03-01"},
        "custom_params": {
            "topk": 2,
            "n_drop": 1,
            "stock_pool": "legacy_pool",
            "backtest_freq": "1min",
            "execution_algo": "TWAP",
            "execution_algo_params": {"split_count": 3},
            "enable_sector_hmm": True,
            "hmm_model_snapshot_id": "old_hmm_snapshot",
            "risk_policy": {"enabled": True},
        },
        "result_metrics": {"IC": 0.05, "Rank IC": 0.04, "final_nav": 1.2, "n_trading_days": 30},
        "workspace_path": "rdagent_assets/qe_experiments/qe_alpha_core_001",
        "created_at": datetime.now(UTC),
        "completed_at": datetime.now(UTC),
    }


def _twap_policy_json() -> dict:
    return {
        "execution_level": "minute",
        "bar_freq": "1m",
        "algo_code": "TWAP",
        "algo_config": {"split_count": 3},
        "fallback_algo_code": None,
        "data_requirements": {
            "requires_minute_bar": True,
            "requires_limit_price": True,
            "requires_suspend_status": True,
            "requires_trade_calendar": True,
        },
        "fallback_policy": {"on_missing_minute_bar": "fail", "on_algo_error": "fail"},
    }


def test_alpha_core_manifest_accepts_factor_model_core_without_platform_runtime_fields() -> None:
    manifest = _alpha_core_manifest()
    payload = manifest.model_dump(mode="json")

    assert manifest.manifest_version == "alpha_core_v1"
    assert manifest.strategy_config == {}
    assert manifest.portfolio_policy is None
    assert manifest.minute_execution_policy is None
    assert manifest.risk_policy is None
    assert payload["source_evidence"]["authority"] == "audit_only_not_runtime_authority"
    assert payload["backtest_context"]["daily_strategy"]["topk"] == 2


def test_alpha_core_manifest_rejects_bound_platform_runtime_fields() -> None:
    base_payload = _alpha_core_manifest().model_dump(mode="json")
    forbidden_overlays = [
        {"strategy_config": {"strategy_id": "score_weighted_topk_v2"}},
        {"universe_policy": UniversePolicy(stock_pool="pool").model_dump(mode="json")},
        {"portfolio_policy": PortfolioPolicy(topk=10, n_drop=1).model_dump(mode="json")},
        {"execution_policy": ExecutionPolicy(backtest_freq="1min").model_dump(mode="json")},
        {"minute_execution_policy": MinuteExecutionPolicy(algo_code="TWAP").model_dump(mode="json")},
        {"risk_policy": {"enforce_a_share_rules": True, "enforce_t_plus_one": True, "enforce_limit_price": True}},
    ]

    for overlay in forbidden_overlays:
        payload = {**base_payload, **overlay, "manifest_sha256": None}
        with pytest.raises(ValidationError, match="alpha_core_v1 manifest cannot bind platform runtime policy fields"):
            StrategyPackageManifest.model_validate(payload)


def test_qe_source_resolver_emits_alpha_core_manifest_with_audit_only_runtime_evidence() -> None:
    resolver = QEExperimentSourceResolver(conn_factory=lambda: dict_record_conn(_qe_record()))

    manifest = resolver.build_from_experiment("qe_alpha_core_001")

    assert manifest.manifest_version == "alpha_core_v1"
    assert manifest.strategy_config == {}
    assert manifest.portfolio_policy is None
    assert manifest.minute_execution_policy is None
    assert manifest.risk_policy is None
    assert manifest.source_evidence["authority"] == "audit_only_not_runtime_authority"
    assert manifest.source_evidence["custom_params"]["enable_sector_hmm"] is True
    assert manifest.backtest_context["authority"] == "source_evidence_not_runtime_authority"
    assert manifest.backtest_context["daily_strategy"]["topk"] == 2
    assert manifest.backtest_context["execution"]["execution_algo"] == "TWAP"


def test_alpha_core_paper_portfolio_requires_manifest_or_explicit_execution_policy() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = _alpha_core_manifest(status=PackageStatus.PAPER_ENABLED)
    package_repo.save_manifest(manifest)
    service = PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo)

    with pytest.raises(StrategyPackageValidationError, match="manifest minute execution policy"):
        service.create_portfolio(
            package_id=manifest.package_id,
            portfolio_name="alpha core missing execution policy",
            initial_cash=100_000,
            start_date=date(2024, 1, 2),
            data_source=MinuteDataSource.DB_HISTORICAL,
        )

    policy = StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name="validated_twap",
        policy_json=_twap_policy_json(),
        source_backtest_id="unit_validated_twap",
        source_backtest_status="BACKTEST_VALIDATED",
        paper_enabled=True,
    )

    portfolio = service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="alpha core explicit execution policy",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={"validated_execution_policy_id": policy.policy_id},
    )

    assert portfolio.execution_policy["validated_execution_policy_id"] == policy.policy_id
    assert portfolio.execution_policy["algo_code"] == "TWAP"


def test_selection_center_uses_runtime_profile_or_source_evidence_not_manifest_runtime_fields() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    rows = [
        {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
        {"symbol": "000002.SZ", "score": 0.98, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
    ]
    manifest = _alpha_core_manifest(status=PackageStatus.SELECTION_ENABLED)
    package_repo.save_manifest(manifest)
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config_hash=selection_artifact_runtime_hash({}),
            scores_json=rows,
            score_count=len(rows),
            universe_count=len(rows),
            top_score_symbol="000001.SZ",
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
            },
        )
    )
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(artifact_repository=artifact_repo),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    runtime_config = {"runtime_profile": {"selection": {"top_k": 1}}}
    runtime_config["runtime_profile_binding"] = {
        "source": "selection_runtime_profile_version",
        "profile_version_id": "unit_selection_profile_v1",
        "config_sha256": runtime_profile_config_sha256(runtime_config),
        "trade_enabled": True,
    }

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=runtime_config,
    )

    package_config = run.runtime_config["package_runtime_configs"][manifest.package_id]
    assert package_config["runtime_profile"]["selection"]["top_k"] == 1
    assert package_config["qe_backtest_runtime_contract"]["portfolio_strategy"]["params"]["topk"] == 2
    assert [item.symbol for item in run.package_results[manifest.package_id]] == ["000001.SZ"]
