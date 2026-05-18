"""Read-only reconstruction for MiniQMT multi-strategy ledger Phase 1."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable
from uuid import uuid5, NAMESPACE_URL

from .models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    AnomalyType,
    LedgerAnomaly,
    OrderLedgerLine,
    RawQmtOrder,
    RawQmtTrade,
    StrategyLedgerSnapshot,
    StrategyLot,
    StrategyPosition,
    classify_frozen_cash_action,
    classify_order_lifecycle,
)


@dataclass
class _BuyAccumulator:
    strategy_name: str
    symbol: str
    order_id: str
    order_remark: str
    quantity: int = 0
    amount: Decimal = Decimal("0")
    trade_ids: list[str] = field(default_factory=list)

    def add(self, trade: RawQmtTrade) -> None:
        self.quantity += trade.traded_volume
        amount = trade.traded_amount or (trade.traded_price * Decimal(trade.traded_volume))
        self.amount += amount
        self.trade_ids.append(trade.traded_id)

    def to_lot(self) -> StrategyLot:
        avg_cost = self.amount / Decimal(self.quantity) if self.quantity else Decimal("0")
        stable = uuid5(
            NAMESPACE_URL,
            f"qmt-strategy-lot:{self.strategy_name}:{self.symbol}:{self.order_id}:{self.order_remark}",
        ).hex[:16]
        return StrategyLot(
            lot_id=f"lot_{stable}",
            strategy_name=self.strategy_name,
            symbol=self.symbol,
            quantity=self.quantity,
            available_quantity=0,
            remaining_quantity=self.quantity,
            avg_cost=avg_cost,
            cost_amount=self.amount,
            open_order_id=self.order_id,
            order_remark=self.order_remark,
            trade_ids=tuple(self.trade_ids),
            source_trade_count=len(self.trade_ids),
        )


def reconstruct_ledger(
    *,
    orders: Iterable[dict[str, Any]],
    trades: Iterable[dict[str, Any]],
    account_id: str | None = None,
    trade_date: str | None = None,
) -> StrategyLedgerSnapshot:
    """Reconstruct strategy lots from MiniQMT order/trade snapshots.

    The function is intentionally pure and read-only: callers pass already
    collected snapshots, and no QMT client or database is touched.
    """

    raw_orders = [RawQmtOrder.from_dict(item) for item in orders]
    raw_trades = [RawQmtTrade.from_dict(item) for item in trades]
    anomalies: list[LedgerAnomaly] = []

    orders_by_id: dict[str, RawQmtOrder] = {}
    seen_order_ids: set[str] = set()
    remark_counts: dict[str, int] = defaultdict(int)
    for order in raw_orders:
        if order.order_id:
            if order.order_id in seen_order_ids:
                anomalies.append(
                    LedgerAnomaly(
                        anomaly_type=AnomalyType.DUPLICATE_ORDER_ID,
                        severity="ERROR",
                        message="duplicate MiniQMT order_id in input snapshot",
                        order_id=order.order_id,
                        strategy_name=order.strategy_name,
                        order_remark=order.order_remark,
                        symbol=order.stock_code,
                    )
                )
            seen_order_ids.add(order.order_id)
            orders_by_id.setdefault(order.order_id, order)
        if order.order_remark:
            remark_counts[order.order_remark] += 1
        if not order.strategy_name:
            anomalies.append(
                LedgerAnomaly(
                    anomaly_type=AnomalyType.BLANK_STRATEGY_NAME,
                    severity="ERROR",
                    message="MiniQMT order has blank strategy_name and cannot be attributed",
                    order_id=order.order_id,
                    order_remark=order.order_remark,
                    symbol=order.stock_code,
                )
            )
        if classify_order_lifecycle(order.order_status).value == "UNKNOWN":
            anomalies.append(
                LedgerAnomaly(
                    anomaly_type=AnomalyType.UNKNOWN_ORDER_STATUS,
                    severity="WARN",
                    message="MiniQMT order has an unmapped order_status",
                    order_id=order.order_id,
                    strategy_name=order.strategy_name,
                    order_remark=order.order_remark,
                    symbol=order.stock_code,
                    context={"order_status": order.order_status},
                )
            )

    for order in raw_orders:
        if order.order_remark and remark_counts[order.order_remark] > 1:
            anomalies.append(
                LedgerAnomaly(
                    anomaly_type=AnomalyType.DUPLICATE_ORDER_REMARK,
                    severity="ERROR",
                    message="order_remark is not unique in MiniQMT snapshot",
                    order_id=order.order_id,
                    strategy_name=order.strategy_name,
                    order_remark=order.order_remark,
                    symbol=order.stock_code,
                    context={"duplicates": remark_counts[order.order_remark]},
                )
            )

    order_lines = tuple(_build_order_line(order) for order in raw_orders)

    seen_trade_ids: set[str] = set()
    buy_accumulators: dict[tuple[str, str, str, str], _BuyAccumulator] = {}
    sell_trades: list[RawQmtTrade] = []
    for trade in raw_trades:
        if trade.traded_id:
            if trade.traded_id in seen_trade_ids:
                anomalies.append(
                    LedgerAnomaly(
                        anomaly_type=AnomalyType.DUPLICATE_TRADE_ID,
                        severity="ERROR",
                        message="duplicate MiniQMT traded_id in input snapshot",
                        order_id=trade.order_id,
                        trade_id=trade.traded_id,
                        strategy_name=trade.strategy_name,
                        order_remark=trade.order_remark,
                        symbol=trade.stock_code,
                    )
                )
            seen_trade_ids.add(trade.traded_id)
        if not trade.strategy_name:
            anomalies.append(
                LedgerAnomaly(
                    anomaly_type=AnomalyType.BLANK_STRATEGY_NAME,
                    severity="ERROR",
                    message="MiniQMT trade has blank strategy_name and cannot be attributed",
                    order_id=trade.order_id,
                    trade_id=trade.traded_id,
                    order_remark=trade.order_remark,
                    symbol=trade.stock_code,
                )
            )
            continue

        matched_order = orders_by_id.get(trade.order_id)
        if matched_order is None:
            anomalies.append(
                LedgerAnomaly(
                    anomaly_type=AnomalyType.TRADE_WITHOUT_ORDER,
                    severity="ERROR",
                    message="MiniQMT trade cannot be matched to an input order_id",
                    order_id=trade.order_id,
                    trade_id=trade.traded_id,
                    strategy_name=trade.strategy_name,
                    order_remark=trade.order_remark,
                    symbol=trade.stock_code,
                )
            )
            continue
        if matched_order.strategy_name and matched_order.strategy_name != trade.strategy_name:
            anomalies.append(
                LedgerAnomaly(
                    anomaly_type=AnomalyType.TRADE_STRATEGY_MISMATCH,
                    severity="ERROR",
                    message="trade strategy_name does not match its MiniQMT order strategy_name",
                    order_id=trade.order_id,
                    trade_id=trade.traded_id,
                    strategy_name=trade.strategy_name,
                    order_remark=trade.order_remark,
                    symbol=trade.stock_code,
                    context={"order_strategy_name": matched_order.strategy_name},
                )
            )
            continue

        if trade.order_type == BUY_ORDER_TYPE and trade.traded_volume > 0:
            key = (trade.strategy_name, trade.stock_code, trade.order_id, trade.order_remark)
            accumulator = buy_accumulators.get(key)
            if accumulator is None:
                accumulator = _BuyAccumulator(
                    strategy_name=trade.strategy_name,
                    symbol=trade.stock_code,
                    order_id=trade.order_id,
                    order_remark=trade.order_remark,
                )
                buy_accumulators[key] = accumulator
            accumulator.add(trade)
        elif trade.order_type == SELL_ORDER_TYPE and trade.traded_volume > 0:
            sell_trades.append(trade)

    lots = [item.to_lot() for item in buy_accumulators.values()]
    lots.sort(key=lambda item: (item.strategy_name, item.symbol, item.open_order_id))

    # Phase 1 POC has rejected sell orders with zero trades. Keep a guarded FIFO
    # reducer for future fixtures without allowing cross-strategy borrowing.
    lots_by_strategy_symbol: dict[tuple[str, str], list[StrategyLot]] = defaultdict(list)
    for lot in lots:
        lots_by_strategy_symbol[(lot.strategy_name, lot.symbol)].append(lot)
    for sell in sell_trades:
        remaining = sell.traded_volume
        candidates = lots_by_strategy_symbol.get((sell.strategy_name, sell.stock_code), [])
        available = sum(item.remaining_quantity for item in candidates)
        if available < remaining:
            anomalies.append(
                LedgerAnomaly(
                    anomaly_type=AnomalyType.SELL_WITHOUT_AVAILABLE_LOT,
                    severity="ERROR",
                    message="sell trade exceeds reconstructed lots for this strategy and symbol",
                    order_id=sell.order_id,
                    trade_id=sell.traded_id,
                    strategy_name=sell.strategy_name,
                    order_remark=sell.order_remark,
                    symbol=sell.stock_code,
                    context={"sell_quantity": remaining, "available_quantity": available},
                )
            )

    positions = _build_positions(lots)
    overlap_symbols = _build_overlap_symbols(positions)
    return StrategyLedgerSnapshot(
        account_id=account_id,
        trade_date=trade_date,
        orders=order_lines,
        lots=tuple(lots),
        positions=tuple(positions),
        anomalies=tuple(anomalies),
        overlap_symbols=tuple(overlap_symbols),
    )


def _build_order_line(order: RawQmtOrder) -> OrderLedgerLine:
    estimated_remaining_notional = order.price * Decimal(order.remaining_volume)
    return OrderLedgerLine(
        order_id=order.order_id,
        order_sysid=order.order_sysid,
        strategy_name=order.strategy_name,
        symbol=order.stock_code,
        order_type=order.order_type,
        order_volume=order.order_volume,
        traded_volume=order.traded_volume,
        order_status=order.order_status,
        lifecycle=classify_order_lifecycle(order.order_status),
        frozen_cash_action=classify_frozen_cash_action(order),
        estimated_remaining_notional=estimated_remaining_notional,
        order_remark=order.order_remark,
    )


def _build_positions(lots: list[StrategyLot]) -> list[StrategyPosition]:
    grouped: dict[tuple[str, str], list[StrategyLot]] = defaultdict(list)
    for lot in lots:
        grouped[(lot.strategy_name, lot.symbol)].append(lot)
    positions: list[StrategyPosition] = []
    for (strategy_name, symbol), group in sorted(grouped.items()):
        quantity = sum(item.remaining_quantity for item in group)
        available_quantity = sum(item.available_quantity for item in group)
        cost_amount = sum((item.cost_amount for item in group), Decimal("0"))
        avg_cost = cost_amount / Decimal(quantity) if quantity else Decimal("0")
        positions.append(
            StrategyPosition(
                strategy_name=strategy_name,
                symbol=symbol,
                quantity=quantity,
                available_quantity=available_quantity,
                cost_amount=cost_amount,
                avg_cost=avg_cost,
                lot_count=len(group),
            )
        )
    return positions


def _build_overlap_symbols(positions: list[StrategyPosition]) -> list[str]:
    strategies_by_symbol: dict[str, set[str]] = defaultdict(set)
    for position in positions:
        if position.quantity > 0:
            strategies_by_symbol[position.symbol].add(position.strategy_name)
    return sorted(symbol for symbol, strategies in strategies_by_symbol.items() if len(strategies) > 1)
