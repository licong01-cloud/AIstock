"""Fail-fast in-memory ledger for Paper Trading v2.

This is the authoritative path for cash and position mutation in the new
Trading Core. Execution algorithms and brokers must emit fills only; they must
not update cash, positions, or NAV directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from .errors import DataUnavailableError, RiskRuleError
from .models import AccountSnapshot, Fill, OrderSide, PositionLot


@dataclass(frozen=True)
class FeeModel:
    """A-share fee model.

    Defaults follow the QE minute backtest config currently used by the sample
    experiment: open_cost=0.000095, close_cost=0.000595, min_cost=5.
    """

    open_cost: float = 0.000095
    close_cost: float = 0.000595
    min_cost: float = 5.0

    def calculate(self, fill: Fill) -> float:
        notional = fill.quantity * fill.price
        rate = self.open_cost if fill.side == OrderSide.BUY else self.close_cost
        return max(notional * rate, self.min_cost)


class InMemoryLedger:
    """Small ledger used by tests and the first Paper v2 vertical slice."""

    def __init__(
        self,
        *,
        portfolio_id: str,
        initial_cash: float,
        fee_model: FeeModel | None = None,
    ) -> None:
        if not portfolio_id:
            raise ValueError("portfolio_id is required")
        if initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        self.portfolio_id = portfolio_id
        self.cash = float(initial_cash)
        self.fee_model = fee_model or FeeModel()
        self.positions: dict[str, PositionLot] = {}
        self.fills: list[Fill] = []

    def apply_fill(self, fill: Fill) -> None:
        if fill.side == OrderSide.BUY:
            self._apply_buy(fill)
        elif fill.side == OrderSide.SELL:
            self._apply_sell(fill)
        else:  # pragma: no cover - enum protects this.
            raise RiskRuleError("unsupported fill side", context={"side": fill.side})
        self.fills.append(fill)

    def settle_trade_date(self, settlement_date: date) -> None:
        """Unlock T+1 shares bought before settlement_date."""

        for symbol, lot in list(self.positions.items()):
            if lot.trade_date < settlement_date:
                self.positions[symbol] = lot.model_copy(
                    update={"available_quantity": lot.quantity}
                )

    def account_snapshot(
        self,
        *,
        prices: dict[str, float],
        snapshot_time: datetime,
    ) -> AccountSnapshot:
        market_value = 0.0
        for symbol, lot in self.positions.items():
            price = prices.get(symbol)
            if price is None or price <= 0:
                raise DataUnavailableError(
                    "missing positive price for held position",
                    context={"portfolio_id": self.portfolio_id, "symbol": symbol},
                )
            market_value += lot.quantity * price
        nav = self.cash + market_value
        return AccountSnapshot(
            portfolio_id=self.portfolio_id,
            cash=self.cash,
            market_value=market_value,
            nav=nav,
            snapshot_time=snapshot_time,
        )

    def _apply_buy(self, fill: Fill) -> None:
        fee = self.fee_model.calculate(fill)
        notional = fill.quantity * fill.price
        total_cost = notional + fee
        if total_cost > self.cash + 1e-8:
            raise RiskRuleError(
                "insufficient cash for buy fill",
                context={
                    "portfolio_id": self.portfolio_id,
                    "symbol": fill.symbol,
                    "cash": self.cash,
                    "total_cost": total_cost,
                },
            )
        self.cash -= total_cost
        current = self.positions.get(fill.symbol)
        fill_date = fill.trade_time.date()
        if current is None:
            self.positions[fill.symbol] = PositionLot(
                portfolio_id=self.portfolio_id,
                symbol=fill.symbol,
                quantity=fill.quantity,
                available_quantity=0,
                avg_cost=notional / fill.quantity,
                trade_date=fill_date,
            )
            return

        new_qty = current.quantity + fill.quantity
        new_avg = ((current.avg_cost * current.quantity) + notional) / new_qty
        self.positions[fill.symbol] = current.model_copy(
            update={
                "quantity": new_qty,
                "avg_cost": new_avg,
                "trade_date": max(current.trade_date, fill_date),
            }
        )

    def _apply_sell(self, fill: Fill) -> None:
        current = self.positions.get(fill.symbol)
        if current is None or current.quantity < fill.quantity:
            raise RiskRuleError(
                "cannot sell more than held quantity",
                context={
                    "portfolio_id": self.portfolio_id,
                    "symbol": fill.symbol,
                    "held_quantity": current.quantity if current else 0,
                    "sell_quantity": fill.quantity,
                },
            )
        if current.available_quantity < fill.quantity:
            raise RiskRuleError(
                "T+1 available quantity is insufficient",
                context={
                    "portfolio_id": self.portfolio_id,
                    "symbol": fill.symbol,
                    "available_quantity": current.available_quantity,
                    "sell_quantity": fill.quantity,
                },
            )

        fee = self.fee_model.calculate(fill)
        notional = fill.quantity * fill.price
        self.cash += notional - fee
        remaining = current.quantity - fill.quantity
        remaining_available = current.available_quantity - fill.quantity
        if remaining == 0:
            del self.positions[fill.symbol]
            return
        self.positions[fill.symbol] = current.model_copy(
            update={
                "quantity": remaining,
                "available_quantity": remaining_available,
            }
        )
