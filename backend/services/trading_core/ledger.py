"""Fail-fast in-memory ledger for Paper Trading v2.

This is the authoritative path for cash and position mutation in the new
Trading Core. Execution algorithms and brokers must emit fills only; they must
not update cash, positions, or NAV directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import logging
from typing import Any

from .errors import DataUnavailableError, RiskRuleError
from .models import AccountSnapshot, Fill, OrderSide, PositionLot


logger = logging.getLogger(__name__)

_MONEY_QUANT = Decimal("0.01")
_ZERO_MONEY = Decimal("0.00")
_A_SHARE_BOARD_LOT = 100


def _decimal_from_value(value: Any, *, field_name: str) -> Decimal:
    try:
        decimal_value = Decimal(repr(value)) if isinstance(value, float) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        logger.error(
            "LocalSim ledger received a non-decimal-compatible money value",
            extra={
                "reason_code": "LOCAL_SIM_DECIMAL_COERCION_FAILED",
                "field_name": field_name,
                "value_repr": repr(value),
            },
        )
        _raise_value_error(
            f"{field_name} must be decimal-compatible",
            reason_code="LOCAL_SIM_DECIMAL_COERCION_FAILED",
            context={"field_name": field_name, "value_repr": repr(value)},
            cause=exc,
        )
    if not decimal_value.is_finite():
        logger.error(
            "LocalSim ledger received a non-finite money value",
            extra={
                "reason_code": "LOCAL_SIM_DECIMAL_NON_FINITE",
                "field_name": field_name,
                "value_repr": repr(value),
            },
        )
        _raise_value_error(
            f"{field_name} must be finite",
            reason_code="LOCAL_SIM_DECIMAL_NON_FINITE",
            context={"field_name": field_name, "value_repr": repr(value)},
        )
    return decimal_value


def _money(value: Any, *, field_name: str) -> Decimal:
    try:
        return _decimal_from_value(value, field_name=field_name).quantize(
            _MONEY_QUANT,
            rounding=ROUND_HALF_UP,
        )
    except InvalidOperation as exc:
        _raise_value_error(
            f"{field_name} cannot be quantized to cents",
            reason_code="LOCAL_SIM_MONEY_QUANTIZATION_FAILED",
            context={"field_name": field_name, "value_repr": repr(value)},
            cause=exc,
        )


def _raise_value_error(
    message: str,
    *,
    reason_code: str,
    context: dict[str, Any],
    cause: Exception | None = None,
) -> None:
    error_message = f"{message}; reason_code={reason_code}; context={context}"
    logger.error(error_message, extra={"reason_code": reason_code, "ledger_context": context})
    if cause is not None:
        raise ValueError(error_message) from cause
    raise ValueError(error_message)


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

    def rate_for(self, side: OrderSide) -> Decimal:
        rate = self.open_cost if side == OrderSide.BUY else self.close_cost
        decimal_rate = _decimal_from_value(rate, field_name="fee_rate")
        if decimal_rate < 0:
            _raise_value_error(
                "fee rate must be non-negative",
                reason_code="LOCAL_SIM_NEGATIVE_FEE_RATE",
                context={"side": side.value, "fee_rate": str(decimal_rate)},
            )
        return decimal_rate

    def min_cost_decimal(self) -> Decimal:
        min_cost = _money(self.min_cost, field_name="min_cost")
        if min_cost < _ZERO_MONEY:
            _raise_value_error(
                "min_cost must be non-negative",
                reason_code="LOCAL_SIM_NEGATIVE_MIN_COMMISSION",
                context={"min_cost": str(min_cost)},
            )
        return min_cost


@dataclass(frozen=True)
class CashLedgerEntry:
    fill_id: str
    portfolio_id: str
    trade_date: date
    symbol: str
    side: OrderSide
    notional: Decimal
    fee: Decimal
    cash_delta: Decimal
    cash_after: Decimal


@dataclass
class _OrderFeeState:
    symbol: str
    side: OrderSide
    cumulative_notional: Decimal
    charged_fee: Decimal


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
            _raise_value_error(
                "portfolio_id is required",
                reason_code="LOCAL_SIM_PORTFOLIO_ID_REQUIRED",
                context={"operation": "init"},
            )
        initial_cash_decimal = _money(initial_cash, field_name="initial_cash")
        if initial_cash_decimal <= _ZERO_MONEY:
            _raise_value_error(
                "initial_cash must be positive",
                reason_code="LOCAL_SIM_INITIAL_CASH_NOT_POSITIVE",
                context={"operation": "init", "initial_cash": str(initial_cash_decimal)},
            )
        self.portfolio_id = portfolio_id
        self._cash = initial_cash_decimal
        self.fee_model = fee_model or FeeModel()
        self.positions: dict[str, PositionLot] = {}
        self.fills: list[Fill] = []
        self.cash_entries: list[CashLedgerEntry] = []
        self._order_fee_state: dict[tuple[str, str, OrderSide], _OrderFeeState] = {}

    @property
    def cash(self) -> float:
        return float(self._cash)

    @cash.setter
    def cash(self, value: float) -> None:
        cash = _money(value, field_name="cash")
        if cash < _ZERO_MONEY:
            _raise_value_error(
                "cash must be non-negative",
                reason_code="LOCAL_SIM_CASH_NEGATIVE",
                context={"operation": "set_cash", "cash": str(cash)},
            )
        self._cash = cash

    @property
    def cash_decimal(self) -> Decimal:
        return self._cash

    def apply_fill(self, fill: Fill) -> None:
        if fill.side == OrderSide.BUY:
            self._validate_board_lot(fill)
            self._apply_buy(fill)
        elif fill.side == OrderSide.SELL:
            self._validate_board_lot(fill)
            self._apply_sell(fill)
        else:  # pragma: no cover - enum protects this.
            self._raise_risk(
                "unsupported fill side",
                context={
                    "reason_code": "LOCAL_SIM_UNSUPPORTED_FILL_SIDE",
                    "portfolio_id": self.portfolio_id,
                    "fill_id": fill.fill_id,
                    "order_id": fill.order_id,
                    "side": fill.side,
                },
            )
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
        market_value = _ZERO_MONEY
        for symbol, lot in self.positions.items():
            price = prices.get(symbol)
            if price is None or price <= 0:
                self._raise_data_unavailable(
                    "missing positive price for held position",
                    context={
                        "reason_code": "LOCAL_SIM_MISSING_POSITIVE_MARK_PRICE",
                        "portfolio_id": self.portfolio_id,
                        "symbol": symbol,
                    },
                )
            market_value += _money(
                Decimal(lot.quantity) * _decimal_from_value(price, field_name="snapshot_price"),
                field_name="market_value",
            )
        market_value = _money(market_value, field_name="market_value")
        nav = _money(self._cash + market_value, field_name="nav")
        return AccountSnapshot(
            portfolio_id=self.portfolio_id,
            cash=float(self._cash),
            market_value=float(market_value),
            nav=float(nav),
            snapshot_time=snapshot_time,
        )

    def _apply_buy(self, fill: Fill) -> None:
        notional, fee = self._calculate_incremental_fee(fill)
        total_cost = _money(notional + fee, field_name="total_cost")
        if total_cost > self._cash:
            self._raise_risk(
                "insufficient cash for buy fill",
                context={
                    "reason_code": "LOCAL_SIM_INSUFFICIENT_CASH",
                    "operation": "apply_buy",
                    "portfolio_id": self.portfolio_id,
                    "order_id": fill.order_id,
                    "fill_id": fill.fill_id,
                    "symbol": fill.symbol,
                    "cash": str(self._cash),
                    "total_cost": str(total_cost),
                },
            )
        new_cash = _money(self._cash - total_cost, field_name="cash_after")
        cash_entry = self._cash_entry(
            fill=fill,
            notional=notional,
            fee=fee,
            cash_delta=-total_cost,
            cash_after=new_cash,
        )
        current = self.positions.get(fill.symbol)
        fill_date = fill.trade_time.date()
        if current is None:
            new_position = PositionLot(
                portfolio_id=self.portfolio_id,
                symbol=fill.symbol,
                quantity=fill.quantity,
                available_quantity=0,
                avg_cost=float(notional / Decimal(fill.quantity)),
                trade_date=fill_date,
            )
        else:
            new_qty = current.quantity + fill.quantity
            current_cost = _money(
                _decimal_from_value(current.avg_cost, field_name="avg_cost") * current.quantity,
                field_name="current_position_cost",
            )
            new_avg = (current_cost + notional) / Decimal(new_qty)
            new_position = current.model_copy(
                update={
                    "quantity": new_qty,
                    "avg_cost": float(new_avg),
                    "trade_date": max(current.trade_date, fill_date),
                }
            )
        self._cash = new_cash
        self.cash_entries.append(cash_entry)
        self._record_order_fee(fill, notional=notional, fee=fee)
        self.positions[fill.symbol] = new_position

    def _apply_sell(self, fill: Fill) -> None:
        current = self.positions.get(fill.symbol)
        if current is None or current.quantity < fill.quantity:
            self._raise_risk(
                "cannot sell more than held quantity",
                context={
                    "reason_code": "LOCAL_SIM_SELL_EXCEEDS_HELD_QUANTITY",
                    "operation": "apply_sell",
                    "portfolio_id": self.portfolio_id,
                    "order_id": fill.order_id,
                    "fill_id": fill.fill_id,
                    "symbol": fill.symbol,
                    "held_quantity": current.quantity if current else 0,
                    "sell_quantity": fill.quantity,
                },
            )
        if current.available_quantity < fill.quantity:
            self._raise_risk(
                "T+1 available quantity is insufficient",
                context={
                    "reason_code": "LOCAL_SIM_T1_AVAILABLE_QUANTITY_INSUFFICIENT",
                    "operation": "apply_sell",
                    "portfolio_id": self.portfolio_id,
                    "order_id": fill.order_id,
                    "fill_id": fill.fill_id,
                    "symbol": fill.symbol,
                    "available_quantity": current.available_quantity,
                    "sell_quantity": fill.quantity,
                },
            )

        notional, fee = self._calculate_incremental_fee(fill)
        cash_delta = _money(notional - fee, field_name="cash_delta")
        new_cash = _money(self._cash + cash_delta, field_name="cash_after")
        cash_entry = self._cash_entry(
            fill=fill,
            notional=notional,
            fee=fee,
            cash_delta=cash_delta,
            cash_after=new_cash,
        )
        remaining = current.quantity - fill.quantity
        remaining_available = current.available_quantity - fill.quantity
        self._cash = new_cash
        self.cash_entries.append(cash_entry)
        self._record_order_fee(fill, notional=notional, fee=fee)
        if remaining == 0:
            del self.positions[fill.symbol]
            return
        self.positions[fill.symbol] = current.model_copy(
            update={
                "quantity": remaining,
                "available_quantity": remaining_available,
            }
        )

    def _validate_board_lot(self, fill: Fill) -> None:
        min_qty = _A_SHARE_BOARD_LOT
        increment = _A_SHARE_BOARD_LOT
        if fill.quantity >= min_qty and fill.quantity % increment == 0:
            return
        current = self.positions.get(fill.symbol)
        is_full_position_sell = (
            fill.side == OrderSide.SELL
            and current is not None
            and fill.quantity == current.quantity
        )
        if is_full_position_sell:
            return
        self._raise_risk(
            "fill quantity violates LocalSim board-lot rules",
            context={
                "reason_code": "LOCAL_SIM_BOARD_LOT_VIOLATION",
                "operation": "apply_fill",
                "portfolio_id": self.portfolio_id,
                "order_id": fill.order_id,
                "fill_id": fill.fill_id,
                "symbol": fill.symbol,
                "side": fill.side.value,
                "fill_quantity": fill.quantity,
                "held_quantity": current.quantity if current else 0,
                "min_qty": min_qty,
                "increment": increment,
                "full_position_sell_allowed": fill.side == OrderSide.SELL,
            },
        )

    def _calculate_incremental_fee(self, fill: Fill) -> tuple[Decimal, Decimal]:
        notional = _fill_notional(fill)
        state_key = self._order_fee_key(fill)
        state = self._order_fee_state.get(state_key)
        cumulative_notional = _money(
            (state.cumulative_notional if state else _ZERO_MONEY) + notional,
            field_name="cumulative_order_notional",
        )
        charged_fee = state.charged_fee if state else _ZERO_MONEY
        target_fee = max(
            _money(
                cumulative_notional * self.fee_model.rate_for(fill.side),
                field_name="cumulative_order_fee",
            ),
            self.fee_model.min_cost_decimal(),
        )
        fee = _money(max(target_fee - charged_fee, _ZERO_MONEY), field_name="incremental_fee")
        return notional, fee

    def _record_order_fee(self, fill: Fill, *, notional: Decimal, fee: Decimal) -> None:
        state_key = self._order_fee_key(fill)
        state = self._order_fee_state.get(state_key)
        if state is None:
            self._order_fee_state[state_key] = _OrderFeeState(
                symbol=fill.symbol,
                side=fill.side,
                cumulative_notional=notional,
                charged_fee=fee,
            )
            return
        state.cumulative_notional = _money(
            state.cumulative_notional + notional,
            field_name="cumulative_order_notional",
        )
        state.charged_fee = _money(
            state.charged_fee + fee,
            field_name="charged_order_fee",
        )

    @staticmethod
    def _order_fee_key(fill: Fill) -> tuple[str, str, OrderSide]:
        return fill.order_id, fill.symbol, fill.side

    def _cash_entry(
        self,
        *,
        fill: Fill,
        notional: Decimal,
        fee: Decimal,
        cash_delta: Decimal,
        cash_after: Decimal,
    ) -> CashLedgerEntry:
        return CashLedgerEntry(
            fill_id=fill.fill_id,
            portfolio_id=self.portfolio_id,
            trade_date=fill.trade_time.date(),
            symbol=fill.symbol,
            side=fill.side,
            notional=notional,
            fee=fee,
            cash_delta=cash_delta,
            cash_after=cash_after,
        )

    def _raise_risk(self, message: str, *, context: dict[str, Any]) -> None:
        reason_code = str(context.get("reason_code") or "LOCAL_SIM_RISK_RULE_ERROR")
        error_message = f"{message}; reason_code={reason_code}"
        logger.warning(
            "LocalSim InMemoryLedger rejected a fill",
            extra={
                "reason_code": reason_code,
                "ledger_context": context,
            },
        )
        raise RiskRuleError(error_message, context=context)

    def _raise_data_unavailable(self, message: str, *, context: dict[str, Any]) -> None:
        reason_code = str(context.get("reason_code") or "LOCAL_SIM_DATA_UNAVAILABLE")
        error_message = f"{message}; reason_code={reason_code}"
        logger.warning(
            "LocalSim InMemoryLedger rejected a snapshot",
            extra={
                "reason_code": reason_code,
                "ledger_context": context,
            },
        )
        raise DataUnavailableError(error_message, context=context)


def _fill_notional(fill: Fill) -> Decimal:
    return _money(
        Decimal(fill.quantity) * _decimal_from_value(fill.price, field_name="fill_price"),
        field_name="fill_notional",
    )
