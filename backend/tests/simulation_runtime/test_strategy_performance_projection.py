from __future__ import annotations

from datetime import date

from backend.services.simulation_runtime import SimulationBrokerBackend, StrategyPerformanceProjectionService
from backend.services.trading_core.models import PositionLot


def _lot(portfolio_id: str, symbol: str, quantity: int, avg_cost: float) -> PositionLot:
    return PositionLot(
        portfolio_id=portfolio_id,
        symbol=symbol,
        quantity=quantity,
        available_quantity=quantity,
        avg_cost=avg_cost,
        trade_date=date(2026, 5, 20),
    )


def test_strategy_performance_projection_keeps_same_stock_strategy_pnl_independent() -> None:
    service = StrategyPerformanceProjectionService()
    strategy_a = {
        "300604.SZ": _lot("strategy_a", "300604.SZ", 1000, 10.0),
        "300054.SZ": _lot("strategy_a", "300054.SZ", 500, 20.0),
    }
    strategy_b = {"300604.SZ": _lot("strategy_b", "300604.SZ", 2000, 11.0)}
    marks = {"300604.SZ": 12.0, "300054.SZ": 19.0}

    projection_a = service.project_strategy(
        strategy_id="strategy_a",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        initial_capital=10_000_000,
        cash=9_970_000,
        positions=strategy_a,
        marks=marks,
    )
    projection_b = service.project_strategy(
        strategy_id="strategy_b",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        initial_capital=10_000_000,
        cash=9_976_000,
        positions=strategy_b,
        marks=marks,
    )

    assert projection_a.unrealized_pnl == 1500.0
    assert projection_b.unrealized_pnl == 2000.0
    assert service.overlap_symbols([projection_a, projection_b]) == ["300604.SZ"]
    reconciliation = service.reconcile_merged_positions(
        strategy_positions={"strategy_a": strategy_a, "strategy_b": strategy_b},
        broker_positions={"300604.SZ": 3000, "300054.SZ": 500},
    )
    assert all(row.matched for row in reconciliation)


def test_strategy_performance_projection_reports_broker_merged_position_mismatch() -> None:
    service = StrategyPerformanceProjectionService()
    reconciliation = service.reconcile_merged_positions(
        strategy_positions={
            "strategy_a": {"300604.SZ": _lot("strategy_a", "300604.SZ", 1000, 10.0)},
            "strategy_b": {"300604.SZ": _lot("strategy_b", "300604.SZ", 500, 11.0)},
        },
        broker_positions={"300604.SZ": 1400},
    )

    assert reconciliation[0].symbol == "300604.SZ"
    assert reconciliation[0].strategy_quantity == 1500
    assert reconciliation[0].broker_quantity == 1400
    assert reconciliation[0].matched is False
