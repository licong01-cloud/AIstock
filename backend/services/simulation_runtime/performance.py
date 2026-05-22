"""Broker-neutral strategy performance projection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
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

    def to_dict(self) -> dict[str, float | int | str]:
        return {
            "symbol": self.symbol,
            "quantity": self.quantity,
            "available_quantity": self.available_quantity,
            "avg_cost": self.avg_cost,
            "mark_price": self.mark_price,
            "market_value": self.market_value,
            "unrealized_pnl": self.unrealized_pnl,
        }


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

    def to_dict(self) -> dict[str, object]:
        return {
            "strategy_id": self.strategy_id,
            "broker_backend": self.broker_backend.value,
            "initial_capital": self.initial_capital,
            "cash": self.cash,
            "frozen_cash": self.frozen_cash,
            "market_value": self.market_value,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_equity": self.total_equity,
            "nav": self.nav,
            "positions": [row.to_dict() for row in self.positions],
        }


@dataclass(frozen=True)
class MergedPositionReconciliation:
    symbol: str
    strategy_quantity: int
    broker_quantity: int
    matched: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "strategy_quantity": self.strategy_quantity,
            "broker_quantity": self.broker_quantity,
            "matched": self.matched,
        }


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

    def project_from_qmt_strategy_ledger(
        self,
        *,
        strategy_id: str,
        repository: object,
        marks: Mapping[str, float],
    ) -> StrategyPerformanceProjection:
        account = repository.get_virtual_account(strategy_id)  # type: ignore[attr-defined]
        lots = repository.list_position_lots(strategy_id)  # type: ignore[attr-defined]
        positions: dict[str, PositionLot] = {}
        for lot in lots:
            remaining = int(getattr(lot, "remaining_quantity", getattr(lot, "quantity", 0)))
            if remaining <= 0:
                continue
            symbol = str(lot.symbol)
            existing = positions.get(symbol)
            quantity = remaining + (existing.quantity if existing else 0)
            available_quantity = int(getattr(lot, "available_quantity", 0)) + (
                existing.available_quantity if existing else 0
            )
            cost_amount = Decimal(str(getattr(lot, "avg_cost"))) * Decimal(str(remaining))
            if existing:
                cost_amount += Decimal(str(existing.avg_cost)) * Decimal(str(existing.quantity))
            avg_cost = float(cost_amount / Decimal(str(quantity))) if quantity else 0.0
            positions[symbol] = PositionLot(
                portfolio_id=strategy_id,
                symbol=symbol,
                quantity=quantity,
                available_quantity=available_quantity,
                avg_cost=avg_cost,
                trade_date=getattr(lot, "open_date"),
            )
        return self.project_strategy(
            strategy_id=strategy_id,
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            initial_capital=float(account.initial_cash),
            cash=float(account.cash),
            frozen_cash=float(account.frozen_cash),
            realized_pnl=float(account.realized_pnl),
            positions=positions,
            marks=marks,
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
