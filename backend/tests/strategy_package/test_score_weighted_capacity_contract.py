from __future__ import annotations

from datetime import date

from backend.services.selection_center.models import SelectionCandidate, SignalSnapshot
from backend.services.strategy_package.backtest_contract import (
    SCORE_WEIGHTED_DEFAULTS,
    SCORE_WEIGHTED_V2_IDS,
    build_backtest_runtime_contract,
    normalize_runtime_config_with_backtest_contract,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus, PortfolioPolicy
from backend.services.strategy_package.runtime import TargetPositionEngine
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


CAPACITY_STRATEGY_ID = "score_weighted_topk_v2_capacity_v1"


def _paper_manifest(*, strategy_id: str, custom_params: dict | None = None, topk: int = 1):
    base = make_manifest()
    params = {"strategy_id": strategy_id}
    if custom_params:
        params.update(custom_params)
    strategy_config = {
        **dict(base.strategy_config),
        "strategy_id": strategy_id,
        "custom_params": params,
    }
    return freeze_manifest(
        base.model_copy(
            update={
                "package_status": PackageStatus.PAPER_ENABLED,
                "portfolio_policy": PortfolioPolicy(topk=topk, n_drop=0),
                "strategy_config": strategy_config,
            }
        )
    )


def _one_candidate_snapshot(manifest) -> SignalSnapshot:
    return SignalSnapshot(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256 or "sha",
        trade_date=date(2026, 5, 8),
        data_source="unit_test",
        candidates=[
            SelectionCandidate(
                symbol="000001.SZ",
                score=10.0,
                rank=1,
                reference_price=10.0,
                reason="unit_test",
            )
        ],
    )


def test_capacity_strategy_contract_uses_new_strategy_id_and_defaults() -> None:
    assert CAPACITY_STRATEGY_ID in SCORE_WEIGHTED_V2_IDS

    manifest = make_manifest().model_copy(
        update={
            "strategy_config": {
                "strategy_id": CAPACITY_STRATEGY_ID,
                "custom_params": {"strategy_id": CAPACITY_STRATEGY_ID},
            }
        }
    )

    contract = build_backtest_runtime_contract(manifest)
    strategy = contract["portfolio_strategy"]

    assert strategy["strategy_id"] == CAPACITY_STRATEGY_ID
    assert strategy["strategy_marker"] == CAPACITY_STRATEGY_ID
    assert strategy["strategy_family"] == "score_weighted_topk_v2"
    assert strategy["capacity_profile"] == "capacity_parameterized_v1"
    assert strategy["params"]["max_single_order_value"] == 1_000_000_000.0
    assert strategy["params"]["max_weight"] == 0.05
    assert strategy["params"]["max_position_ratio"] == 0.95
    assert strategy["params"]["strategy_id"] == CAPACITY_STRATEGY_ID
    assert strategy["params"]["capacity_profile"] == "capacity_parameterized_v1"


def test_legacy_score_weighted_v2_contract_keeps_5m_default_when_capacity_missing() -> None:
    manifest = make_manifest().model_copy(
        update={
            "strategy_config": {
                "strategy_id": "score_weighted_topk_v2",
                "custom_params": {"strategy_id": "score_weighted_topk_v2"},
            }
        }
    )

    contract = build_backtest_runtime_contract(manifest)
    strategy = contract["portfolio_strategy"]

    assert strategy["strategy_family"] == "score_weighted_topk_v2"
    assert strategy["capacity_profile"] == "legacy_5m_cap"
    assert strategy["params"]["max_single_order_value"] == 5_000_000.0


def test_legacy_score_weighted_v2_contract_retains_5m_default() -> None:
    manifest = _paper_manifest(strategy_id="score_weighted_topk_v2")

    config = normalize_runtime_config_with_backtest_contract(manifest, {}, include_contract=True)
    params = config["qe_backtest_runtime_contract"]["portfolio_strategy"]["params"]

    assert SCORE_WEIGHTED_DEFAULTS["max_single_order_value"] == 5_000_000.0
    assert params["strategy_family"] == "score_weighted_topk_v2"
    assert params["capacity_profile"] == "legacy_5m_cap"
    assert params["max_single_order_value"] == 5_000_000.0

    targets = TargetPositionEngine().build_targets(
        snapshot=_one_candidate_snapshot(manifest),
        total_equity=300_000_000.0,
        top_k=1,
        manifest=manifest,
    )
    assert targets[0].metadata["target_value"] == 5_000_000.0
    assert targets[0].target_quantity == 500_000


def test_capacity_strategy_id_uses_new_default_without_mutating_legacy_default() -> None:
    manifest = _paper_manifest(strategy_id=CAPACITY_STRATEGY_ID)

    contract = build_backtest_runtime_contract(manifest)
    strategy = contract["portfolio_strategy"]
    params = strategy["params"]

    assert SCORE_WEIGHTED_DEFAULTS["max_single_order_value"] == 5_000_000.0
    assert strategy["strategy_family"] == "score_weighted_topk_v2"
    assert strategy["capacity_profile"] == "capacity_parameterized_v1"
    assert params["max_single_order_value"] == 1_000_000_000.0


def test_explicit_capacity_parameters_flow_to_paper_target_value() -> None:
    manifest = _paper_manifest(
        strategy_id="score_weighted_topk_v2",
        custom_params={
            "max_single_order_value": 15_000_000.0,
            "max_weight": 0.05,
            "max_position_ratio": 0.95,
            "lot_size": 100,
        },
    )

    config = normalize_runtime_config_with_backtest_contract(manifest, {}, include_contract=True)
    params = config["qe_backtest_runtime_contract"]["portfolio_strategy"]["params"]
    targets = TargetPositionEngine().build_targets(
        snapshot=_one_candidate_snapshot(manifest),
        total_equity=300_000_000.0,
        top_k=1,
        manifest=manifest,
    )

    assert params["max_single_order_value"] == 15_000_000.0
    assert params["max_weight"] == 0.05
    assert params["max_position_ratio"] == 0.95
    assert targets[0].metadata["target_value"] == 15_000_000.0
    assert targets[0].target_quantity == 1_500_000
