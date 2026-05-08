from __future__ import annotations

from backend.services.strategy_package.backtest_contract import SCORE_WEIGHTED_V2_IDS, build_backtest_runtime_contract
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def test_capacity_strategy_contract_uses_new_strategy_id_and_defaults():
    assert "score_weighted_topk_v2_capacity_v1" in SCORE_WEIGHTED_V2_IDS

    manifest = make_manifest().model_copy(
        update={
            "strategy_config": {
                "strategy_id": "score_weighted_topk_v2_capacity_v1",
                "custom_params": {"strategy_id": "score_weighted_topk_v2_capacity_v1"},
            }
        }
    )

    contract = build_backtest_runtime_contract(manifest)
    strategy = contract["portfolio_strategy"]

    assert strategy["strategy_marker"] == "score_weighted_topk_v2_capacity_v1"
    assert strategy["strategy_family"] == "score_weighted_topk_v2"
    assert strategy["capacity_profile"] == "capacity_parameterized_v1"
    assert strategy["params"]["max_single_order_value"] == 1_000_000_000.0
    assert strategy["params"]["max_weight"] == 0.05
    assert strategy["params"]["max_position_ratio"] == 0.95
    assert strategy["params"]["strategy_id"] == "score_weighted_topk_v2_capacity_v1"
    assert strategy["params"]["capacity_profile"] == "capacity_parameterized_v1"


def test_legacy_score_weighted_v2_contract_keeps_5m_default_when_capacity_missing():
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
