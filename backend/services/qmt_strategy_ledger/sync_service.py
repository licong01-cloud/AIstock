"""Read-only MiniQMT snapshot synchronization into the strategy ledger.

The service depends on an injected client with query methods only. It never
submits or cancels orders, which keeps Phase 3 safe for fake-client tests and
manual read-only snapshots.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Protocol

from .models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_REJECTED,
    CashEntryType,
    CashLedgerEntry,
    IntentSubmitStatus,
    OrderLedgerRecord,
    OrderStatusEventRecord,
    PositionLotRecord,
    RawQmtOrder,
    RawQmtTrade,
    TradeLedgerRecord,
    UnattributedOrderRecord,
    UnattributedTradeRecord,
    new_id,
)
from .repository import InMemoryQmtStrategyLedgerRepository


class ReadOnlyQmtClient(Protocol):
    def get_orders(self, cancelable_only: bool = False) -> list[dict[str, Any]]:
        ...

    def get_trades(self) -> list[dict[str, Any]]:
        ...

    def get_positions(self) -> list[dict[str, Any]]:
        ...


@dataclass(frozen=True)
class SyncSummary:
    account_id: str
    trade_date: date
    orders_seen: int
    orders_upserted: int
    trades_seen: int
    trades_inserted: int
    trades_existing: int
    unattributed_orders: int
    unattributed_trades: int
    status_events_appended: int
    lots_created: int
    cash_entries_appended: int
    buy_fill_settled_amount: Decimal
    buy_fill_fee_amount: Decimal
    buy_freeze_released_amount: Decimal
    accounts_revalued: int
    positions_seen: int
    raw_positions: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "trade_date": self.trade_date.isoformat(),
            "orders_seen": self.orders_seen,
            "orders_upserted": self.orders_upserted,
            "trades_seen": self.trades_seen,
            "trades_inserted": self.trades_inserted,
            "trades_existing": self.trades_existing,
            "unattributed_orders": self.unattributed_orders,
            "unattributed_trades": self.unattributed_trades,
            "status_events_appended": self.status_events_appended,
            "lots_created": self.lots_created,
            "cash_entries_appended": self.cash_entries_appended,
            "buy_fill_settled_amount": float(self.buy_fill_settled_amount),
            "buy_fill_fee_amount": float(self.buy_fill_fee_amount),
            "buy_freeze_released_amount": float(self.buy_freeze_released_amount),
            "accounts_revalued": self.accounts_revalued,
            "positions_seen": self.positions_seen,
            "raw_positions": list(self.raw_positions),
        }


class QmtStrategyLedgerSyncService:
    def __init__(
        self,
        *,
        repository: InMemoryQmtStrategyLedgerRepository,
        qmt_client: ReadOnlyQmtClient,
        account_id: str,
        trade_date: date,
    ) -> None:
        self._repository = repository
        self._qmt_client = qmt_client
        self._account_id = account_id
        self._trade_date = trade_date

    def sync_snapshot(self) -> SyncSummary:
        orders = self._qmt_client.get_orders(cancelable_only=False)
        trades = self._qmt_client.get_trades()
        positions = self._qmt_client.get_positions()

        strategy_id_by_name = {
            account.strategy_name: account.strategy_id
            for account in self._repository.list_virtual_accounts(account_id=self._account_id)
        }

        orders_by_id = {RawQmtOrder.from_dict(payload).order_id: RawQmtOrder.from_dict(payload) for payload in orders}
        seen_remarks: dict[str, int] = {}
        for order in orders_by_id.values():
            if order.order_remark:
                seen_remarks[order.order_remark] = seen_remarks.get(order.order_remark, 0) + 1

        orders_upserted = 0
        unattributed_orders = 0
        status_events_appended = 0
        terminal_buy_orders: list[tuple[RawQmtOrder, str, str]] = []
        for payload in orders:
            order = RawQmtOrder.from_dict(payload)
            strategy_id = strategy_id_by_name.get(order.strategy_name)
            reason = _order_unattributed_reason(order, strategy_id, seen_remarks)
            if reason is not None:
                self._repository.upsert_unattributed_order(
                    UnattributedOrderRecord(
                        unattributed_id=_unattributed_id("uo", self._account_id, self._trade_date, order.order_id),
                        account_id=self._account_id,
                        trade_date=self._trade_date,
                        qmt_order_id=order.order_id,
                        symbol=order.stock_code,
                        reason=reason,
                        order_remark=order.order_remark,
                        raw_json=payload,
                    )
                )
                unattributed_orders += 1
                continue

            intent = self._repository.get_order_intent_by_remark(self._account_id, order.order_remark)
            if intent is None:
                self._repository.upsert_unattributed_order(
                    UnattributedOrderRecord(
                        unattributed_id=_unattributed_id("uo", self._account_id, self._trade_date, order.order_id),
                        account_id=self._account_id,
                        trade_date=self._trade_date,
                        qmt_order_id=order.order_id,
                        symbol=order.stock_code,
                        reason="UNKNOWN_ORDER_INTENT",
                        order_remark=order.order_remark,
                        raw_json=payload,
                    )
                )
                unattributed_orders += 1
                continue

            self._repository.upsert_order_ledger(
                OrderLedgerRecord(
                    intent_id=intent.intent_id,
                    strategy_id=strategy_id,
                    strategy_name=order.strategy_name,
                    qmt_order_id=order.order_id,
                    qmt_order_sysid=order.order_sysid,
                    symbol=order.stock_code,
                    order_type=order.order_type,
                    order_volume=order.order_volume,
                    traded_volume=order.traded_volume,
                    order_status=order.order_status,
                    account_id=self._account_id,
                    trade_date=self._trade_date,
                    price_type=order.price_type,
                    price=order.price,
                    traded_price=order.traded_price,
                    status_msg=order.status_msg,
                    order_remark=order.order_remark,
                    raw_json=payload,
                )
            )
            self._repository.append_order_status_event(
                OrderStatusEventRecord(
                    event_id=_order_event_id(self._account_id, order.order_id, order.order_status),
                    intent_id=intent.intent_id,
                    qmt_order_id=order.order_id,
                    qmt_order_sysid=order.order_sysid,
                    event_type="STATUS_SYNC",
                    event_time=datetime.now(UTC),
                    account_id=self._account_id,
                    qmt_order_status=order.order_status,
                    status_msg=order.status_msg,
                    raw_json=payload,
                )
            )
            orders_upserted += 1
            status_events_appended += 1
            if order.order_type == BUY_ORDER_TYPE and order.order_status in {STATUS_CANCELLED, STATUS_FILLED, STATUS_REJECTED}:
                terminal_buy_orders.append((order, strategy_id, intent.intent_id))
                submit_status = _intent_status_from_order_status(order.order_status)
                if submit_status is not None:
                    self._repository.set_order_intent_submit_status(
                        intent.intent_id,
                        submit_status,
                        submitted_at=intent.submitted_at,
                        updated_at=datetime.now(UTC),
                    )

        trades_inserted = 0
        trades_existing = 0
        unattributed_trades = 0
        lots_created = 0
        cash_entries_appended = 0
        buy_fill_settled_amount = Decimal("0")
        buy_fill_fee_amount = Decimal("0")
        buy_freeze_released_amount = Decimal("0")
        changed_strategy_ids: set[str] = set()
        for payload in trades:
            trade = RawQmtTrade.from_dict(payload)
            order = orders_by_id.get(trade.order_id)
            strategy_id = strategy_id_by_name.get(trade.strategy_name)
            intent = self._repository.get_order_intent_by_remark(self._account_id, trade.order_remark)
            reason = _trade_unattributed_reason(trade, order, strategy_id, intent is not None)
            if reason is not None:
                self._repository.upsert_unattributed_trade(
                    UnattributedTradeRecord(
                        unattributed_id=_unattributed_id("ut", self._account_id, self._trade_date, trade.traded_id),
                        account_id=self._account_id,
                        trade_date=self._trade_date,
                        trade_id=trade.traded_id,
                        qmt_order_id=trade.order_id,
                        symbol=trade.stock_code,
                        reason=reason,
                        order_remark=trade.order_remark,
                        raw_json=payload,
                    )
                )
                unattributed_trades += 1
                continue

            ledger_trade, inserted = self._repository.upsert_trade_ledger(
                TradeLedgerRecord(
                    trade_id=trade.traded_id,
                    intent_id=intent.intent_id,
                    strategy_id=strategy_id,
                    qmt_order_id=trade.order_id,
                    qmt_order_sysid=trade.order_sysid,
                    symbol=trade.stock_code,
                    side=_side_from_order_type(trade.order_type),
                    price=trade.traded_price,
                    quantity=trade.traded_volume,
                    amount=trade.traded_amount or trade.traded_price * Decimal(trade.traded_volume),
                    commission=trade.commission,
                    trade_date=self._trade_date,
                    account_id=self._account_id,
                    trade_time=_parse_trade_time(self._trade_date, trade.traded_time),
                    order_remark=trade.order_remark,
                    raw_json=payload,
                )
            )
            if inserted:
                trades_inserted += 1
                if trade.order_type == BUY_ORDER_TYPE:
                    self._repository.create_position_lot(
                        PositionLotRecord(
                            lot_id=_lot_id(self._account_id, self._trade_date, trade.traded_id),
                            strategy_id=strategy_id,
                            symbol=trade.stock_code,
                            open_trade_id=trade.traded_id,
                            open_date=self._trade_date,
                            quantity=trade.traded_volume,
                            available_quantity=0,
                            remaining_quantity=trade.traded_volume,
                            avg_cost=trade.traded_price,
                            cost_amount=trade.traded_amount or trade.traded_price * Decimal(trade.traded_volume),
                            account_id=self._account_id,
                            open_time=_parse_trade_time(self._trade_date, trade.traded_time),
                            metadata={"source": "miniqmt_sync"},
                        )
                    )
                    lots_created += 1
            else:
                trades_existing += 1

            if trade.order_type == BUY_ORDER_TYPE:
                fill_entry, inserted_cash = self._settle_buy_fill_once(strategy_id, intent.intent_id, ledger_trade, order)
                if inserted_cash:
                    cash_entries_appended += 1
                    buy_fill_settled_amount += abs(fill_entry.frozen_delta)
                    buy_fill_fee_amount += _money(ledger_trade.commission)
                    changed_strategy_ids.add(strategy_id)
            _ = ledger_trade

        for order, strategy_id, intent_id in terminal_buy_orders:
            release_entry, inserted_cash = self._release_terminal_buy_freeze_once(strategy_id, intent_id, order)
            if release_entry is not None and inserted_cash:
                cash_entries_appended += 1
                buy_freeze_released_amount += release_entry.cash_delta
                changed_strategy_ids.add(strategy_id)

        for strategy_id in strategy_id_by_name.values():
            if self._revalue_strategy_account(strategy_id):
                changed_strategy_ids.add(strategy_id)

        return SyncSummary(
            account_id=self._account_id,
            trade_date=self._trade_date,
            orders_seen=len(orders),
            orders_upserted=orders_upserted,
            trades_seen=len(trades),
            trades_inserted=trades_inserted,
            trades_existing=trades_existing,
            unattributed_orders=unattributed_orders,
            unattributed_trades=unattributed_trades,
            status_events_appended=status_events_appended,
            lots_created=lots_created,
            cash_entries_appended=cash_entries_appended,
            buy_fill_settled_amount=buy_fill_settled_amount,
            buy_fill_fee_amount=buy_fill_fee_amount,
            buy_freeze_released_amount=buy_freeze_released_amount,
            accounts_revalued=len(changed_strategy_ids),
            positions_seen=len(positions),
            raw_positions=tuple(dict(item) for item in positions),
        )

    def _settle_buy_fill_once(
        self,
        strategy_id: str,
        intent_id: str,
        trade: TradeLedgerRecord,
        order: RawQmtOrder,
    ) -> tuple[CashLedgerEntry, bool]:
        account = self._repository.get_virtual_account(strategy_id)
        fill_amount = _money(trade.amount)
        fee_amount = _money(trade.commission)
        remaining_for_intent = self._remaining_frozen_for_intent(strategy_id, intent_id)
        reserved_fill_amount = _reserved_fill_amount(order, trade)
        frozen_release = min(reserved_fill_amount, account.frozen_cash, remaining_for_intent)
        cash_delta = frozen_release - fill_amount - fee_amount
        updated = replace(
            account,
            cash=account.cash + cash_delta,
            frozen_cash=account.frozen_cash - frozen_release,
            updated_at=datetime.now(UTC),
        )
        entry = CashLedgerEntry(
            cash_id=_cash_event_id(self._account_id, self._trade_date, "buy_fill", trade.trade_id),
            strategy_id=strategy_id,
            entry_type=CashEntryType.BUY_FILL,
            cash_delta=cash_delta,
            cash_after=updated.cash,
            frozen_delta=-frozen_release,
            frozen_after=updated.frozen_cash,
            account_id=self._account_id,
            trade_date=self._trade_date,
            intent_id=intent_id,
            trade_id=trade.trade_id,
            symbol=trade.symbol,
            reason=CashEntryType.BUY_FILL.value,
            metadata={
                "source": "miniqmt_sync",
                "fill_amount": str(fill_amount),
                "commission": str(fee_amount),
                "frozen_release": str(frozen_release),
                "reserved_fill_amount": str(reserved_fill_amount),
                "qmt_order_id": trade.qmt_order_id,
            },
        )
        _, inserted = self._repository.apply_cash_entry_once(entry, updated)
        return entry, inserted

    def _release_terminal_buy_freeze_once(
        self,
        strategy_id: str,
        intent_id: str,
        order: RawQmtOrder,
    ) -> tuple[CashLedgerEntry | None, bool]:
        if order.order_status == STATUS_CANCELLED:
            entry_type = CashEntryType.UNFREEZE_CANCEL
            reason = entry_type.value
        elif order.order_status == STATUS_REJECTED:
            entry_type = CashEntryType.UNFREEZE_REJECT
            reason = entry_type.value
        else:
            entry_type = CashEntryType.BUY_FILL
            reason = "BUY_FILL_RESIDUAL_RELEASE"
        cash_id = _cash_event_id(self._account_id, self._trade_date, reason.lower(), order.order_id)
        account = self._repository.get_virtual_account(strategy_id)
        release_amount = min(self._remaining_frozen_for_intent(strategy_id, intent_id), account.frozen_cash)
        if release_amount <= Decimal("0"):
            return None, False
        cash_delta = release_amount
        updated = replace(
            account,
            cash=account.cash + cash_delta,
            frozen_cash=account.frozen_cash - release_amount,
            updated_at=datetime.now(UTC),
        )
        entry = CashLedgerEntry(
            cash_id=cash_id,
            strategy_id=strategy_id,
            entry_type=entry_type,
            cash_delta=cash_delta,
            cash_after=updated.cash,
            frozen_delta=-release_amount,
            frozen_after=updated.frozen_cash,
            account_id=self._account_id,
            trade_date=self._trade_date,
            intent_id=intent_id,
            symbol=order.stock_code,
            reason=reason,
            metadata={
                "source": "miniqmt_sync",
                "qmt_order_id": order.order_id,
                "order_status": order.order_status,
                "order_volume": order.order_volume,
                "traded_volume": order.traded_volume,
                "remaining_volume": order.remaining_volume,
                "order_price": str(order.price),
            },
        )
        _, inserted = self._repository.apply_cash_entry_once(entry, updated)
        return entry, inserted

    def _remaining_frozen_for_intent(self, strategy_id: str, intent_id: str) -> Decimal:
        frozen_delta_total = sum(
            (entry.frozen_delta for entry in self._repository.list_cash_entries(strategy_id) if entry.intent_id == intent_id),
            Decimal("0"),
        )
        return max(_money(frozen_delta_total), Decimal("0"))

    def _revalue_strategy_account(self, strategy_id: str) -> bool:
        account = self._repository.get_virtual_account(strategy_id)
        lots = self._repository.list_position_lots(strategy_id)
        market_value = _money(sum((lot.avg_cost * Decimal(lot.remaining_quantity) for lot in lots), Decimal("0")))
        cost_basis = _money(sum((lot.avg_cost * Decimal(lot.remaining_quantity) for lot in lots), Decimal("0")))
        unrealized_pnl = market_value - cost_basis
        if account.market_value == market_value and account.unrealized_pnl == unrealized_pnl:
            return False
        self._repository.update_virtual_account(
            replace(
                account,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                updated_at=datetime.now(UTC),
            )
        )
        return True


def _order_unattributed_reason(order: RawQmtOrder, strategy_id: str | None, remark_counts: dict[str, int]) -> str | None:
    if not order.strategy_name:
        return "BLANK_STRATEGY_NAME"
    if strategy_id is None:
        return "UNKNOWN_STRATEGY_NAME"
    if not order.order_remark:
        return "BLANK_ORDER_REMARK"
    if remark_counts.get(order.order_remark, 0) > 1:
        return "DUPLICATE_ORDER_REMARK"
    return None


def _trade_unattributed_reason(
    trade: RawQmtTrade,
    order: RawQmtOrder | None,
    strategy_id: str | None,
    has_intent: bool,
) -> str | None:
    if not trade.strategy_name:
        return "BLANK_STRATEGY_NAME"
    if strategy_id is None:
        return "UNKNOWN_STRATEGY_NAME"
    if order is None:
        return "TRADE_WITHOUT_ORDER"
    if order.strategy_name and order.strategy_name != trade.strategy_name:
        return "TRADE_STRATEGY_MISMATCH"
    if not has_intent:
        return "UNKNOWN_ORDER_INTENT"
    return None


def _intent_status_from_order_status(order_status: int | None) -> IntentSubmitStatus | None:
    if order_status == STATUS_FILLED:
        return IntentSubmitStatus.ACCEPTED
    if order_status == STATUS_CANCELLED:
        return IntentSubmitStatus.CANCELLED
    if order_status == STATUS_REJECTED:
        return IntentSubmitStatus.REJECTED
    return None


def _side_from_order_type(order_type: int) -> str:
    if order_type == BUY_ORDER_TYPE:
        return "BUY"
    if order_type == SELL_ORDER_TYPE:
        return "SELL"
    return "UNKNOWN"


def _parse_trade_time(trade_date: date, value: str) -> datetime | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    for fmt in ("%H%M%S", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return datetime.combine(trade_date, parsed.time(), tzinfo=UTC)
        except ValueError:
            continue
    return None


def _unattributed_id(prefix: str, account_id: str, trade_date: date, row_id: str) -> str:
    safe_row_id = row_id or new_id("blank")
    return f"{prefix}_{account_id}_{trade_date.isoformat()}_{safe_row_id}"


def _order_event_id(account_id: str, order_id: str, order_status: int | None) -> str:
    status = "none" if order_status is None else str(order_status)
    return f"evt_{account_id}_{order_id}_{status}"


def _lot_id(account_id: str, trade_date: date, trade_id: str) -> str:
    return f"lot_{account_id}_{trade_date.isoformat()}_{trade_id}"


def _cash_event_id(account_id: str, trade_date: date, event_type: str, row_id: str) -> str:
    safe_row_id = row_id or new_id("blank")
    return f"cash_{account_id}_{trade_date.isoformat()}_{event_type}_{safe_row_id}"


def _reserved_fill_amount(order: RawQmtOrder, trade: TradeLedgerRecord) -> Decimal:
    reserve_price = order.price if order.price > Decimal("0") else trade.price
    return _money(reserve_price * Decimal(trade.quantity))


def _money(value: Decimal) -> Decimal:
    return Decimal(value).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
