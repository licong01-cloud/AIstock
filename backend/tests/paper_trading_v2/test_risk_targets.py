from __future__ import annotations

from datetime import date

import pytest

from backend.services.paper_trading_v2.risk_targets import overlay_risk_forced_exit_targets
from backend.services.selection_center.models import SelectionCandidate, SignalSnapshot, TargetPosition
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus, PortfolioPolicy
from backend.services.strategy_package.runtime import TargetPositionEngine
from backend.services.trading_core.models import PositionLot
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _target(symbol: str, *, quantity: int, reason: str) -> TargetPosition:
    return TargetPosition(
        symbol=symbol,
        target_quantity=quantity,
        target_weight=0.03 if quantity else None,
        reference_price=10.0 if quantity else None,
        score=0.9,
        rank=1,
        reason=reason,
    )


def test_overlay_risk_forced_exit_replaces_same_symbol_target_once() -> None:
    base = [
        _target("000001.SZ", quantity=0, reason="qe_backtest_score_weighted_ghost_sell"),
        _target("000002.SZ", quantity=300, reason="qe_backtest_score_weighted_buy"),
    ]
    forced = [_target("000001.SZ", quantity=0, reason="risk_policy_forced_exit")]

    merged = overlay_risk_forced_exit_targets(base, forced)

    assert [target.symbol for target in merged] == ["000001.SZ", "000002.SZ"]
    assert merged[0].reason == "risk_policy_forced_exit"
    assert merged[0].metadata["overrode_target"]["reason"] == "qe_backtest_score_weighted_ghost_sell"


@pytest.mark.parametrize(
    ("strategy_id", "overridden_reason"),
    [
        ("score_weighted_topk_v1", "qe_backtest_score_weighted_retain"),
        ("score_weighted_topk_v2", "qe_backtest_score_weighted_ghost_sell"),
    ],
)
def test_st_pit_forced_exit_overrides_qe_score_weighted_target_without_duplicates(
    strategy_id: str,
    overridden_reason: str,
) -> None:
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.PAPER_ENABLED,
                "portfolio_policy": PortfolioPolicy(topk=2, n_drop=1),
                "strategy_config": {
                    "strategy_id": strategy_id,
                    "custom_params": {
                        "strategy_id": strategy_id,
                        "topk": 2,
                        "n_drop": 1,
                        "max_n_drop": 1,
                        "enable_dynamic_ndrop": False,
                        "risk_policy": {
                            "enabled": True,
                            "providers": ["st_pit"],
                            "hard_actions": ["block_buy", "force_exit"],
                            "st_universe_key": "shsz_st_pit_active_v1",
                            "strict_data_ready": True,
                        },
                    },
                },
            }
        )
    )
    snapshot = SignalSnapshot(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256 or "",
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        candidates=[
            SelectionCandidate(symbol="000001.SZ", score=0.95, rank=1, reference_price=10.0),
            SelectionCandidate(symbol="000002.SZ", score=0.90, rank=2, reference_price=10.0),
        ],
    )
    current_positions = {
        "000001.SZ": PositionLot(
            portfolio_id="pf_unit",
            symbol="000001.SZ",
            quantity=500,
            available_quantity=500,
            avg_cost=10.0,
            trade_date=date(2024, 1, 2),
        ),
        "000099.SZ": PositionLot(
            portfolio_id="pf_unit",
            symbol="000099.SZ",
            quantity=700,
            available_quantity=700,
            avg_cost=10.0,
            trade_date=date(2024, 1, 2),
        ),
    }

    qe_targets = TargetPositionEngine().build_targets(
        snapshot=snapshot,
        total_equity=100_000,
        top_k=2,
        manifest=manifest,
        current_positions=current_positions,
        current_prices={"000001.SZ": 10.0, "000002.SZ": 10.0, "000099.SZ": 10.0},
    )
    forced_exit = _target("000099.SZ", quantity=0, reason="risk_policy_forced_exit")

    merged = overlay_risk_forced_exit_targets(qe_targets, [forced_exit])

    assert [target.symbol for target in merged].count("000099.SZ") == 1
    outside_target = next(target for target in merged if target.symbol == "000099.SZ")
    assert outside_target.target_quantity == 0
    assert outside_target.reason == "risk_policy_forced_exit"
    assert outside_target.metadata["overrode_target"]["reason"] == overridden_reason
