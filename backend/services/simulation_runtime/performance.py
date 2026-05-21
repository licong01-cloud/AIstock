"""Broker-neutral strategy performance projection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import PositionLot

from .models import SimulationBrokerBackend


@dataclass(frozen=True)
class StrategyPositionProjection:
    symbol: str
    quantity: int
    available_quantity: int
    avg_cost: float
    mark_price: float
    market_value: float
    unrealized_pnl: float


@dataclass(frozen=True)
class StrategyPerformanceProjection:
    strategy_id: str
    broker_backend: SimulationBrokerBackend
    initial_capital: float
    cash: float
    frozen_cash: float
    market_value: float
    realized_pnl: float
    unrealized_pnl: float
    total_equity: float
    nav: float
    positions: tuple[StrategyPositionProjection, ...]


@dataclass(frozen=True)
class MergedPositionReconciliation:
    symbol: str
    strategy_quantity: int
    broker_quantity: int
    matched: bool


class StrategyPerformanceProjectionService:
    """Project per-strategy PnL without treating broker merged positions as strategy lots."""

    def project_strategy(
        self,
        *,
        strategy_id: str,
        broker_backend: SimulationBrokerBackend,
        initial_capital: float,
        cash: float,
        positions: Mapping[str, PositionLot],
        marks: Mapping[str, float],
        frozen_cash: float = 0.0,
        realized_pnl: float = 0.0,
    ) -> StrategyPerformanceProjection:
        if initial_capital <= 0:
            raise DataUnavailableError("initial capital must be positive for strategy performance projection")
        position_rows: list[StrategyPositionProjection] = []
        for symbol, lot in sorted(positions.items()):
            if symbol not in marks:
                raise DataUnavailableError(
                    "mark price missing for strategy performance projection",
                    context={"strategy_id": strategy_id, "symbol": symbol},
                )
            price = float(marks[symbol])
            quantity = int(lot.quantity)
            market_value = quantity * price
            unrealized = quantity * (price - float(lot.avg_cost))
            position_rows.append(
                StrategyPositionProjection(
                    symbol=symbol,
                    quantity=quantity,
                    available_quantity=int(lot.available_quantity),
                    avg_cost=float(lot.avg_cost),
                    mark_price=price,
                    market_value=market_value,
                    unrealized_pnl=unrealized,
                )
            )
        market_value = sum(row.market_value for row in position_rows)
        unrealized_pnl = sum(row.unrealized_pnl for row in position_rows)
        total_equity = float(cash) + float(frozen_cash) + market_value
        return StrategyPerformanceProjection(
            strategy_id=strategy_id,
            broker_backend=broker_backend,
            initial_capital=float(initial_capital),
            cash=float(cash),
            frozen_cash=float(frozen_cash),
            market_value=market_value,
            realized_pnl=float(realized_pnl),
            unrealized_pnl=unrealized_pnl,
            total_equity=total_equity,
            nav=total_equity / float(initial_capital),
            positions=tuple(position_rows),
        )

    @staticmethod
    def overlap_symbols(projections: list[StrategyPerformanceProjection]) -> list[str]:
        counts: dict[str, int] = {}
        for projection in projections:
            for position in projection.positions:
                if position.quantity > 0:
                    counts[position.symbol] = counts.get(position.symbol, 0) + 1
        return sorted(symbol for symbol, count in counts.items() if count > 1)

    @staticmethod
    def reconcile_merged_positions(
        *,
        strategy_positions: Mapping[str, Mapping[str, PositionLot]],
        broker_positions: Mapping[str, int],
    ) -> list[MergedPositionReconciliation]:
        symbols = sorted(set(broker_positions) | {symbol for positions in strategy_positions.values() for symbol in positions})
        rows: list[MergedPositionReconciliation] = []
        for symbol in symbols:
            strategy_quantity = sum(int(positions[symbol].quantity) for positions in strategy_positions.values() if symbol in positions)
            broker_quantity = int(broker_positions.get(symbol, 0))
            rows.append(
                MergedPositionReconciliation(
                    symbol=symbol,
                    strategy_quantity=strategy_quantity,
                    broker_quantity=broker_quantity,
                    matched=strategy_quantity == broker_quantity,
                )
            )
        return rows
