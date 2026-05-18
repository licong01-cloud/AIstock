"""Read-only MiniQMT multi-strategy ledger domain models.

Phase 1 intentionally models MiniQMT snapshots in memory only. It does not
connect to MiniQMT, submit orders, or persist database rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

BUY_ORDER_TYPE = 23
SELL_ORDER_TYPE = 24
STATUS_OPEN_LIKE = 50
STATUS_CANCELLED = 54
STATUS_FILLED = 56
STATUS_REJECTED = 57


class OrderLifecycle(str, Enum):
    OPEN = "OPEN"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class FrozenCashAction(str, Enum):
    KEEP_BUY_FREEZE = "KEEP_BUY_FREEZE"
    RELEASE_REMAINING_BUY_FREEZE = "RELEASE_REMAINING_BUY_FREEZE"
    SETTLE_BUY_FILL = "SETTLE_BUY_FILL"
    NONE = "NONE"


class AnomalyType(str, Enum):
    BLANK_STRATEGY_NAME = "BLANK_STRATEGY_NAME"
    DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
    DUPLICATE_ORDER_REMARK = "DUPLICATE_ORDER_REMARK"
    DUPLICATE_TRADE_ID = "DUPLICATE_TRADE_ID"
    TRADE_WITHOUT_ORDER = "TRADE_WITHOUT_ORDER"
    TRADE_STRATEGY_MISMATCH = "TRADE_STRATEGY_MISMATCH"
    SELL_WITHOUT_AVAILABLE_LOT = "SELL_WITHOUT_AVAILABLE_LOT"
    UNKNOWN_ORDER_STATUS = "UNKNOWN_ORDER_STATUS"


@dataclass(frozen=True)
class LedgerAnomaly:
    anomaly_type: AnomalyType
    severity: str
    message: str
    order_id: str | None = None
    trade_id: str | None = None
    strategy_name: str | None = None
    order_remark: str | None = None
    symbol: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "anomaly_type": self.anomaly_type.value,
            "severity": self.severity,
            "message": self.message,
            "order_id": self.order_id,
            "trade_id": self.trade_id,
            "strategy_name": self.strategy_name,
            "order_remark": self.order_remark,
            "symbol": self.symbol,
            "context": self.context,
        }


@dataclass(frozen=True)
class RawQmtOrder:
    order_id: str
    order_sysid: str
    stock_code: str
    order_type: int
    order_volume: int
    price_type: int | None
    price: Decimal
    traded_volume: int
    traded_price: Decimal
    order_status: int | None
    status_msg: str
    strategy_name: str
    order_remark: str
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RawQmtOrder":
        return cls(
            order_id=_clean_str(payload.get("order_id")),
            order_sysid=_clean_str(payload.get("order_sysid")),
            stock_code=_clean_str(payload.get("stock_code")),
            order_type=_safe_int(payload.get("order_type")),
            order_volume=_safe_int(payload.get("order_volume")),
            price_type=_safe_optional_int(payload.get("price_type")),
            price=_decimal(payload.get("price")),
            traded_volume=_safe_int(payload.get("traded_volume")),
            traded_price=_decimal(payload.get("traded_price")),
            order_status=_safe_optional_int(payload.get("order_status")),
            status_msg=_clean_str(payload.get("status_msg")),
            strategy_name=_clean_str(payload.get("strategy_name")),
            order_remark=_clean_str(payload.get("order_remark")),
            raw=dict(payload),
        )

    @property
    def remaining_volume(self) -> int:
        return max(0, self.order_volume - self.traded_volume)


@dataclass(frozen=True)
class RawQmtTrade:
    traded_id: str
    stock_code: str
    order_type: int
    traded_time: str
    traded_price: Decimal
    traded_volume: int
    traded_amount: Decimal
    order_id: str
    order_sysid: str
    commission: Decimal
    strategy_name: str
    order_remark: str
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RawQmtTrade":
        return cls(
            traded_id=_clean_str(payload.get("traded_id")),
            stock_code=_clean_str(payload.get("stock_code")),
            order_type=_safe_int(payload.get("order_type")),
            traded_time=_clean_str(payload.get("traded_time")),
            traded_price=_decimal(payload.get("traded_price")),
            traded_volume=_safe_int(payload.get("traded_volume")),
            traded_amount=_decimal(payload.get("traded_amount")),
            order_id=_clean_str(payload.get("order_id")),
            order_sysid=_clean_str(payload.get("order_sysid")),
            commission=_decimal(payload.get("commission")),
            strategy_name=_clean_str(payload.get("strategy_name")),
            order_remark=_clean_str(payload.get("order_remark")),
            raw=dict(payload),
        )


@dataclass(frozen=True)
class OrderLedgerLine:
    order_id: str
    order_sysid: str
    strategy_name: str
    symbol: str
    order_type: int
    order_volume: int
    traded_volume: int
    order_status: int | None
    lifecycle: OrderLifecycle
    frozen_cash_action: FrozenCashAction
    estimated_remaining_notional: Decimal
    order_remark: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "order_sysid": self.order_sysid,
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "order_type": self.order_type,
            "order_volume": self.order_volume,
            "traded_volume": self.traded_volume,
            "order_status": self.order_status,
            "lifecycle": self.lifecycle.value,
            "frozen_cash_action": self.frozen_cash_action.value,
            "estimated_remaining_notional": _to_json_number(self.estimated_remaining_notional),
            "order_remark": self.order_remark,
        }


@dataclass(frozen=True)
class StrategyLot:
    lot_id: str
    strategy_name: str
    symbol: str
    quantity: int
    available_quantity: int
    remaining_quantity: int
    avg_cost: Decimal
    cost_amount: Decimal
    open_order_id: str
    order_remark: str
    trade_ids: tuple[str, ...]
    source_trade_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "lot_id": self.lot_id,
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "quantity": self.quantity,
            "available_quantity": self.available_quantity,
            "remaining_quantity": self.remaining_quantity,
            "avg_cost": _to_json_number(self.avg_cost),
            "cost_amount": _to_json_number(self.cost_amount),
            "open_order_id": self.open_order_id,
            "order_remark": self.order_remark,
            "trade_ids": list(self.trade_ids),
            "source_trade_count": self.source_trade_count,
        }


@dataclass(frozen=True)
class StrategyPosition:
    strategy_name: str
    symbol: str
    quantity: int
    available_quantity: int
    cost_amount: Decimal
    avg_cost: Decimal
    lot_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "quantity": self.quantity,
            "available_quantity": self.available_quantity,
            "cost_amount": _to_json_number(self.cost_amount),
            "avg_cost": _to_json_number(self.avg_cost),
            "lot_count": self.lot_count,
        }


@dataclass(frozen=True)
class StrategyLedgerSnapshot:
    account_id: str | None
    trade_date: str | None
    orders: tuple[OrderLedgerLine, ...]
    lots: tuple[StrategyLot, ...]
    positions: tuple[StrategyPosition, ...]
    anomalies: tuple[LedgerAnomaly, ...]
    overlap_symbols: tuple[str, ...]

    def position_quantity(self, strategy_name: str, symbol: str) -> int:
        for position in self.positions:
            if position.strategy_name == strategy_name and position.symbol == symbol:
                return position.quantity
        return 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "trade_date": self.trade_date,
            "orders": [item.to_dict() for item in self.orders],
            "lots": [item.to_dict() for item in self.lots],
            "positions": [item.to_dict() for item in self.positions],
            "anomalies": [item.to_dict() for item in self.anomalies],
            "overlap_symbols": list(self.overlap_symbols),
            "summary": self.summary(),
        }

    def summary(self) -> dict[str, Any]:
        status_counts: dict[str, int] = {}
        lifecycle_counts: dict[str, int] = {}
        for order in self.orders:
            status_key = str(order.order_status)
            status_counts[status_key] = status_counts.get(status_key, 0) + 1
            lifecycle_counts[order.lifecycle.value] = lifecycle_counts.get(order.lifecycle.value, 0) + 1
        return {
            "orders_count": len(self.orders),
            "lots_count": len(self.lots),
            "positions_count": len(self.positions),
            "anomalies_count": len(self.anomalies),
            "order_status_counts": dict(sorted(status_counts.items())),
            "order_lifecycle_counts": dict(sorted(lifecycle_counts.items())),
            "overlap_symbols": list(self.overlap_symbols),
        }


def classify_order_lifecycle(order_status: int | None) -> OrderLifecycle:
    if order_status == STATUS_OPEN_LIKE:
        return OrderLifecycle.OPEN
    if order_status == STATUS_CANCELLED:
        return OrderLifecycle.CANCELLED
    if order_status == STATUS_FILLED:
        return OrderLifecycle.FILLED
    if order_status == STATUS_REJECTED:
        return OrderLifecycle.REJECTED
    return OrderLifecycle.UNKNOWN


def classify_frozen_cash_action(order: RawQmtOrder) -> FrozenCashAction:
    if order.order_type != BUY_ORDER_TYPE:
        return FrozenCashAction.NONE
    lifecycle = classify_order_lifecycle(order.order_status)
    if lifecycle == OrderLifecycle.OPEN and order.remaining_volume > 0:
        return FrozenCashAction.KEEP_BUY_FREEZE
    if lifecycle in {OrderLifecycle.CANCELLED, OrderLifecycle.REJECTED} and order.remaining_volume > 0:
        return FrozenCashAction.RELEASE_REMAINING_BUY_FREEZE
    if lifecycle == OrderLifecycle.FILLED:
        return FrozenCashAction.SETTLE_BUY_FILL
    return FrozenCashAction.NONE


def _clean_str(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _to_json_number(value: Decimal) -> float:
    return float(value)
