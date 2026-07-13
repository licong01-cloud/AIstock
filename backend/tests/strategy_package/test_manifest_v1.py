from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.frozen_runtime_self_check import (
    FrozenRuntimeSelfCheckResult,
    attach_runtime_asset_admission,
)
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaLineage,
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
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import DataUnavailableError


def make_manifest(algo_code: str = "TWAP") -> StrategyPackageManifest:
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
        lineage=AlphaLineage(
            qe_artifact_id="qe_test",
            factor_artifact_refs=["factor_a", "factor_b"],
            model_artifact_ref="model_1",
        ),
    )
    return StrategyPackageManifest(
        manifest_version="1.0",
        package_name="qe_test",
        source=StrategyPackageSource(
            source_type=SourceType.QE_EXPERIMENT,
            source_id="qe_test",
        ),
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        alpha_components=[component],
        alpha_combination_policy=AlphaCombinationPolicy(
            method="identity",
            weights={"alpha_001": 1.0},
            conflict_resolution="highest_score",
        ),
        factor_set=[
            FactorAsset(factor_id="factor_a", factor_name="factor_a"),
            FactorAsset(factor_id="factor_b", factor_name="factor_b"),
        ],
        model_asset=ModelAsset(model_id="model_1"),
        strategy_config={"strategy_id": "score_weighted_topk_v2"},
        universe_policy=UniversePolicy(stock_pool="test_pool"),
        portfolio_policy=PortfolioPolicy(topk=50, n_drop=5),
        execution_policy=ExecutionPolicy(backtest_freq="1min"),
        minute_execution_policy=MinuteExecutionPolicy(
            algo_code=algo_code,
            algo_config={"split_count": 3},
            fallback_algo_code=None,
        ),
        backtest_summary=BacktestSummary(
            ic=0.05,
            rank_ic=0.04,
            annual_return=0.2,
            max_drawdown=-0.1,
            final_nav=1.2,
            n_trading_days=20,
            raw_metrics={"IC": 0.05},
            sample_start=date(2024, 1, 1),
        ),
        package_status=PackageStatus.BACKTEST_APPROVED,
    )


def admit_manifest_for_test(manifest: StrategyPackageManifest) -> StrategyPackageManifest:
    frozen = freeze_manifest(manifest.model_copy(update={"manifest_sha256": None}))
    return attach_runtime_asset_admission(
        frozen,
        FrozenRuntimeSelfCheckResult(
            package_id=frozen.package_id,
            manifest_sha256=frozen.manifest_sha256,
            origin="package_asset",
            model_kind="unit",
            model_expected_features=len(frozen.factor_set),
            dynamic_factor_count=len(frozen.factor_set),
            alpha158_alias_count=0,
            factor_order_count=len(frozen.factor_set),
            feature_count_delta=0,
            model_params_path="unit://params.pkl",
            model_probe_backend="unit",
            leg_results=(
                {component.alpha_id: {"origin": "package_asset"} for component in frozen.alpha_components}
                if frozen.alpha_mode == AlphaMode.MULTI_ALPHA
                else None
            ),
            combined_signal_smoke=(
                {
                    "schema_version": "multi_alpha_parent_combined_signal_smoke_v1",
                    "leg_count": len(frozen.alpha_components),
                    "deterministic_replay": True,
                }
                if frozen.alpha_mode == AlphaMode.MULTI_ALPHA
                else None
            ),
        ),
    )


def test_freeze_manifest_hash_is_stable() -> None:
    manifest = freeze_manifest(make_manifest())
    same = freeze_manifest(manifest)
    assert manifest.manifest_sha256 == same.manifest_sha256
    StrategyPackageValidator().validate_manifest(manifest)


def test_single_alpha_requires_identity_policy() -> None:
    manifest = make_manifest()
    payload = manifest.model_dump(mode="json")
    payload["alpha_combination_policy"] = AlphaCombinationPolicy(
        method="weighted_score",
        weights={"alpha_001": 1.0},
    ).model_dump(mode="json")
    with pytest.raises(ValidationError, match="identity"):
        StrategyPackageManifest.model_validate(payload)


def test_fallback_algo_is_forbidden() -> None:
    with pytest.raises(ValidationError):
        MinuteExecutionPolicy(algo_code="TWAP", fallback_algo_code="CLOSE_PRICE")


def test_paper_readiness_fails_when_v24_model_is_unavailable() -> None:
    manifest = freeze_manifest(make_manifest(algo_code="V24_PLAN"))
    with pytest.raises(DataUnavailableError, match="model_path"):
        StrategyPackageValidator().validate_execution_policy_for_paper(
            package_id=manifest.package_id,
            policy_json=manifest.minute_execution_policy.model_dump(mode="json"),
        )
