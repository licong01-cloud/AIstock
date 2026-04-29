from __future__ import annotations

from datetime import date

import pytest

from backend.services.selection_center.models import TargetPosition
from backend.services.strategy_package.runtime import RebalanceEngine
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.services.trading_core.models import PositionLot


def _target(symbol: str, quantity: int) -> TargetPosition:
    return TargetPosition(
        symbol=symbol,
        target_quantity=quantity,
        target_weight=0.1,
        reference_price=10.0,
        score=1.0,
        rank=1,
        reason="unit_test",
    )


def _position(symbol: str, quantity: int) -> PositionLot:
    return PositionLot(
        portfolio_id="paper_unit",
        symbol=symbol,
        quantity=quantity,
        available_quantity=quantity,
        avg_cost=10.0,
        trade_date=date(2026, 4, 29),
    )


def test_rebalance_returns_empty_for_explicit_no_position_diff() -> None:
    intents = RebalanceEngine().build_order_intents(
        package_id="pkg_unit",
        portfolio_id="paper_unit",
        trade_date=date(2026, 4, 29),
        current_positions={"000001.SZ": _position("000001.SZ", 1000)},
        target_positions=[_target("000001.SZ", 1000)],
    )

    assert intents == []


def test_rebalance_still_fails_when_targets_are_missing() -> None:
    with pytest.raises(StrategyPackageValidationError, match="target positions"):
        RebalanceEngine().build_order_intents(
            package_id="pkg_unit",
            portfolio_id="paper_unit",
            trade_date=date(2026, 4, 29),
            current_positions={"000001.SZ": _position("000001.SZ", 1000)},
            target_positions=[],
        )
