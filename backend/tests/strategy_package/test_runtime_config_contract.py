from __future__ import annotations

from datetime import date

import pytest

from backend.services.strategy_package.manifest import freeze_manifest
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
from backend.services.strategy_package.runtime_config import (
    HMMUsagePolicy,
    ModelWeightPolicy,
    PlatformCapabilities,
    PlatformHMMCapability,
    RuntimeAdapterConfig,
    RuntimeAdapterKind,
    build_unified_runtime_config_from_manifest,
    build_default_runtime_config_bundle,
)


def _make_manifest() -> StrategyPackageManifest:
    component = AlphaComponent(
        alpha_id="alpha_001",
        alpha_name="single_alpha",
        component_weight=1.0,
        factor_ids=["factor_a", "factor_b"],
        holding_period="1day",
        rebalance_frequency="1day",
        score_direction="higher_better",
    )
    return freeze_manifest(
        StrategyPackageManifest(
            package_name="runtime_contract_test",
            source=StrategyPackageSource(source_type=SourceType.QE_EXPERIMENT, source_id="qe_runtime_exp"),
            alpha_mode=AlphaMode.SINGLE_ALPHA,
            alpha_components=[component],
            alpha_combination_policy=AlphaCombinationPolicy(method="identity", weights={"alpha_001": 1.0}),
            factor_set=[
                FactorAsset(factor_id="factor_a", factor_name="factor_a"),
                FactorAsset(factor_id="factor_b", factor_name="factor_b"),
            ],
            model_asset=ModelAsset(model_id="model_1", model_type="CATBOOST"),
            strategy_config={
                "strategy_id": "score_weighted_topk_v2",
                "model_training": {"seed": 42, "hyperparameters": {"depth": 6}},
            },
            universe_policy=UniversePolicy(stock_pool="paper_v2_latest"),
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
    )


def _qe_adapter() -> RuntimeAdapterConfig:
    return RuntimeAdapterConfig(
        kind=RuntimeAdapterKind.QE_QLIB_BIN,
        data_source="qlib_bin",
        execution_target="qe_backtest",
        qlib_provider_uri="file:///qlib/cn_data",
    )


def _paper_adapter() -> RuntimeAdapterConfig:
    return RuntimeAdapterConfig(
        kind=RuntimeAdapterKind.PAPER_V2_DB,
        data_source="paper_v2_realtime_db",
        execution_target="aistock_paper",
        db_profile="paper_v2_dev",
    )


def test_strategy_semantics_hash_is_shared_across_qe_and_paper_adapters() -> None:
    manifest = _make_manifest()

    qe_config = build_unified_runtime_config_from_manifest(manifest, adapter=_qe_adapter())
    paper_config = build_unified_runtime_config_from_manifest(manifest, adapter=_paper_adapter())

    assert qe_config.config_sha256 == paper_config.config_sha256
    assert qe_config.runtime_config_sha256 != paper_config.runtime_config_sha256
    assert qe_config.strategy_semantics.minute_execution_policy["algo_code"] == "TWAP"
    assert paper_config.strategy_semantics.strategy_config["model_training"]["seed"] == 42


def test_strategy_semantics_hash_changes_when_strategy_changes() -> None:
    manifest = _make_manifest()
    base_config = build_unified_runtime_config_from_manifest(manifest, adapter=_qe_adapter())
    changed_manifest = freeze_manifest(
        manifest.model_copy(
            update={
                "manifest_sha256": None,
                "portfolio_policy": manifest.portfolio_policy.model_copy(update={"topk": 20}),
            }
        )
    )

    changed_config = build_unified_runtime_config_from_manifest(changed_manifest, adapter=_qe_adapter())

    assert changed_config.config_sha256 != base_config.config_sha256
    assert changed_config.strategy_semantics.portfolio_policy["topk"] == 20


def test_platform_hmm_and_st_pit_snapshots_are_not_locked_into_strategy_semantics() -> None:
    manifest = _make_manifest()
    manifest = freeze_manifest(
        manifest.model_copy(
            update={
                "manifest_sha256": None,
                "strategy_config": {
                    **manifest.strategy_config,
                    "hmm_snapshot_id": "qe_backtest_hmm_snapshot",
                    "_precomputed_hmm_coefficients_json": {"old": "backtest_only"},
                },
                "universe_policy": UniversePolicy(
                    stock_pool="paper_v2_latest",
                    st_pit_snapshot_id="qe_backtest_st_pit_snapshot",
                    st_pit_start_date="2018-01-01",
                    st_pit_end_date="2026-04-27",
                ),
            }
        )
    )

    config = build_unified_runtime_config_from_manifest(
        manifest,
        adapter=_paper_adapter(),
        platform_capabilities=PlatformCapabilities(
            hmm=PlatformHMMCapability(enabled=True, active_model_version="hmm_rt_v2")
        ),
        hmm_usage_policy=HMMUsagePolicy(enabled=True),
    )

    assert "hmm_snapshot_id" not in config.strategy_semantics.strategy_config
    assert "_precomputed_hmm_coefficients_json" not in config.strategy_semantics.strategy_config
    assert "st_pit_snapshot_id" not in config.strategy_semantics.universe_policy
    assert "st_pit_start_date" not in config.strategy_semantics.universe_policy
    assert config.strategy_semantics.hmm_usage_policy.enabled is True
    assert config.platform_capabilities.hmm.active_model_version == "hmm_rt_v2"
    assert config.platform_capabilities.universe.source == "paper_v2_platform_latest"


def test_runtime_adapter_validation_is_explicit() -> None:
    with pytest.raises(ValueError, match="qlib_provider_uri"):
        RuntimeAdapterConfig(
            kind=RuntimeAdapterKind.QE_QLIB_BIN,
            data_source="qlib_bin",
            execution_target="qe_backtest",
        )

    with pytest.raises(ValueError, match="broker_profile"):
        RuntimeAdapterConfig(
            kind=RuntimeAdapterKind.MINI_QMT,
            data_source="mini_qmt_realtime",
            execution_target="mini_qmt_paper",
        )


def test_model_weight_policy_supports_rolling_retrain_without_changing_default_weight_source() -> None:
    manifest = _make_manifest()

    config = build_unified_runtime_config_from_manifest(
        manifest,
        adapter=_paper_adapter(),
        model_weight_policy=ModelWeightPolicy(
            default_weight_source="backtest_manifest",
            rolling_retrain_enabled=True,
            rolling_window_years=3,
            retrain_schedule="daily_after_close",
        ),
    )

    assert config.strategy_semantics.model_weight_policy.default_weight_source == "backtest_manifest"
    assert config.strategy_semantics.model_weight_policy.rolling_retrain_enabled is True
    assert config.strategy_semantics.model_weight_policy.rolling_window_years == 3


def test_default_runtime_config_bundle_exposes_cross_module_equivalence() -> None:
    manifest = _make_manifest()

    bundle = build_default_runtime_config_bundle(manifest)

    assert bundle["config_sha256"] == bundle["qe_backtest"]["config_sha256"]
    assert bundle["config_sha256"] == bundle["paper_v2"]["config_sha256"]
    assert bundle["qe_backtest"]["adapter"]["kind"] == "qe_qlib_bin"
    assert bundle["paper_v2"]["adapter"]["kind"] == "paper_v2_db"
    assert bundle["equivalence"]["strategy_semantics_shared"] is True
    assert bundle["equivalence"]["adapter_specific_runtime_hashes"] is True


def test_platform_capabilities_do_not_expose_reserved_event_signal_fields() -> None:
    payload = PlatformCapabilities().model_dump(mode="json")

    assert set(payload) == {"hmm", "universe"}
