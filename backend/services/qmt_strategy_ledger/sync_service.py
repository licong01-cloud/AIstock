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
from zoneinfo import ZoneInfo

from .lot_availability import (
    DbTradingCalendarProvider,
    TradingCalendarProvider,
    effective_lot_available_quantity,
)
from .models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_REJECTED,
    CashEntryType,
    CashLedgerEntry,
    IntentSubmitStatus,
    MiniQmtStrategySlot,
    OrderLedgerRecord,
    OrderStatusEventRecord,
    PositionLotRecord,
    PositionLotStatus,
    RawQmtOrder,
    RawQmtTrade,
    TradeLedgerRecord,
    UnattributedOrderRecord,
    UnattributedTradeRecord,
    new_id,
)
from .repository import InMemoryQmtStrategyLedgerRepository

CHINA_TZ = ZoneInfo("Asia/Shanghai")


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
    sell_fill_received_amount: Decimal
    sell_fill_fee_amount: Decimal
    sell_fill_realized_pnl: Decimal
    buy_freeze_released_amount: Decimal
    accounts_revalued: int
    positions_seen: int
    lots_unlocked: int = 0
    raw_positions: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    stale_orders_skipped: int = 0
    stale_trades_skipped: int = 0
    stale_orders_terminalized: int = 0
    stale_buy_freeze_released_amount: Decimal = Decimal("0")
    stale_broker_snapshot: bool = False
    stale_broker_payload_samples: tuple[dict[str, Any], ...] = field(default_factory=tuple)

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
            "sell_fill_received_amount": float(self.sell_fill_received_amount),
            "sell_fill_fee_amount": float(self.sell_fill_fee_amount),
            "sell_fill_realized_pnl": float(self.sell_fill_realized_pnl),
            "buy_freeze_released_amount": float(self.buy_freeze_released_amount),
            "accounts_revalued": self.accounts_revalued,
            "positions_seen": self.positions_seen,
            "lots_unlocked": self.lots_unlocked,
            "raw_positions": list(self.raw_positions),
            "stale_orders_skipped": self.stale_orders_skipped,
            "stale_trades_skipped": self.stale_trades_skipped,
            "stale_orders_terminalized": self.stale_orders_terminalized,
            "stale_buy_freeze_released_amount": float(self.stale_buy_freeze_released_amount),
            "stale_broker_snapshot": self.stale_broker_snapshot,
            "stale_broker_payload_samples": list(self.stale_broker_payload_samples),
        }


@dataclass(frozen=True)
class _AttributedTrade:
    index: int
    payload: dict[str, Any]
    trade: RawQmtTrade
    order: RawQmtOrder
    strategy_id: str
    intent_id: str
    ledger_trade: TradeLedgerRecord


class QmtStrategyLedgerSyncService:
    def __init__(
        self,
        *,
        repository: InMemoryQmtStrategyLedgerRepository,
        qmt_client: ReadOnlyQmtClient,
        account_id: str,
        trade_date: date,
        calendar_provider: TradingCalendarProvider | None = None,
    ) -> None:
        self._repository = repository
        self._qmt_client = qmt_client
        self._account_id = account_id
        self._trade_date = trade_date
        self._calendar_provider = calendar_provider or DbTradingCalendarProvider()

    def sync_snapshot(self) -> SyncSummary:
        raw_orders = self._qmt_client.get_orders(cancelable_only=False)
        raw_trades = self._qmt_client.get_trades()
        positions = self._qmt_client.get_positions()

        orders: list[dict[str, Any]] = []
        stale_order_ids: set[str] = set()
        stale_orders: list[tuple[dict[str, Any], RawQmtOrder, date]] = []
        stale_payload_samples: list[dict[str, Any]] = []
        stale_orders_skipped = 0
        for payload in raw_orders:
            broker_date = _broker_order_payload_date(payload)
            if broker_date is not None and broker_date != self._trade_date:
                stale_orders_skipped += 1
                stale_order = RawQmtOrder.from_dict(payload)
                order_id = stale_order.order_id
                if order_id:
                    stale_order_ids.add(order_id)
                stale_orders.append((payload, stale_order, broker_date))
                stale_payload_samples.append(
                    _stale_payload_sample(
                        payload_type="order",
                        payload=payload,
                        broker_date=broker_date,
                        expected_trade_date=self._trade_date,
                        reason="BROKER_ORDER_DATE_MISMATCH",
                    )
                )
                continue
            orders.append(payload)

        trades: list[dict[str, Any]] = []
        stale_trade_order_ids: set[str] = set()
        stale_trades_skipped = 0
        for payload in raw_trades:
            trade = RawQmtTrade.from_dict(payload)
            broker_date = _broker_trade_payload_date(payload)
            linked_stale_order = bool(trade.order_id and trade.order_id in stale_order_ids)
            if linked_stale_order or (broker_date is not None and broker_date != self._trade_date):
                if trade.order_id:
                    stale_trade_order_ids.add(trade.order_id)
                stale_trades_skipped += 1
                stale_payload_samples.append(
                    _stale_payload_sample(
                        payload_type="trade",
                        payload=payload,
                        broker_date=broker_date,
                        expected_trade_date=self._trade_date,
                        reason="BROKER_TRADE_LINKED_TO_STALE_ORDER"
                        if linked_stale_order
                        else "BROKER_TRADE_DATE_MISMATCH",
                    )
                )
                continue
            trades.append(payload)

        accounts = self._repository.list_virtual_accounts(account_id=self._account_id)
        strategy_id_by_name = {account.strategy_name: account.strategy_id for account in accounts}
        strategy_name_by_id = {account.strategy_id: account.strategy_name for account in accounts}
        strategy_id_by_remark_prefix = _strategy_id_by_remark_prefix(accounts)
        lots_unlocked = self._unlock_tplus1_lots(strategy_id_by_name.values())

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
            intent = self._repository.get_order_intent_by_remark(self._account_id, order.order_remark) if order.order_remark else None
            strategy_id = _resolve_strategy_id(
                strategy_name=order.strategy_name,
                order_remark=order.order_remark,
                intent=intent,
                strategy_id_by_name=strategy_id_by_name,
                strategy_name_by_id=strategy_name_by_id,
                strategy_id_by_remark_prefix=strategy_id_by_remark_prefix,
            )
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

            canonical_strategy_name = strategy_name_by_id.get(strategy_id, intent.strategy_name)
            self._repository.upsert_order_ledger(
                OrderLedgerRecord(
                    intent_id=intent.intent_id,
                    strategy_id=strategy_id,
                    strategy_name=canonical_strategy_name,
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
            self._repository.delete_unattributed_order(
                account_id=self._account_id,
                trade_date=self._trade_date,
                qmt_order_id=order.order_id,
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
            if (
                order.order_type == BUY_ORDER_TYPE
                and order.order_status in {STATUS_CANCELLED, STATUS_FILLED, STATUS_REJECTED}
                and order.order_id not in stale_trade_order_ids
            ):
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
        sell_fill_received_amount = Decimal("0")
        sell_fill_fee_amount = Decimal("0")
        sell_fill_realized_pnl = Decimal("0")
        buy_freeze_released_amount = Decimal("0")
        stale_orders_terminalized = 0
        stale_buy_freeze_released_amount = Decimal("0")
        changed_strategy_ids: set[str] = set()
        attributed_trades: list[_AttributedTrade] = []
        for index, payload in enumerate(trades):
            trade = RawQmtTrade.from_dict(payload)
            order = orders_by_id.get(trade.order_id)
            intent = self._repository.get_order_intent_by_remark(self._account_id, trade.order_remark)
            strategy_id = _resolve_strategy_id(
                strategy_name=trade.strategy_name,
                order_remark=trade.order_remark,
                intent=intent,
                strategy_id_by_name=strategy_id_by_name,
                strategy_name_by_id=strategy_name_by_id,
                strategy_id_by_remark_prefix=strategy_id_by_remark_prefix,
            )
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

            if order is None or intent is None or strategy_id is None:
                continue
            attributed_trades.append(
                _AttributedTrade(
                    index=index,
                    payload=payload,
                    trade=trade,
                    order=order,
                    strategy_id=strategy_id,
                    intent_id=intent.intent_id,
                    ledger_trade=TradeLedgerRecord(
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
                    ),
                )
            )

        for item in sorted(attributed_trades, key=_trade_settlement_sort_key):
            trade = item.trade
            ledger_trade = item.ledger_trade
            inserted_trade = False
            created_lot = False
            inserted_cash = False
            if trade.order_type == BUY_ORDER_TYPE:
                fill_entry, inserted_trade, created_lot, inserted_cash = self._settle_buy_fill_once(
                    item.strategy_id,
                    item.intent_id,
                    ledger_trade,
                    item.order,
                )
                if inserted_cash:
                    cash_entries_appended += 1
                    buy_fill_settled_amount += abs(fill_entry.frozen_delta)
                    buy_fill_fee_amount += _money(ledger_trade.commission)
                    changed_strategy_ids.add(item.strategy_id)
            elif trade.order_type == SELL_ORDER_TYPE:
                sell_entry, inserted_trade, inserted_cash, realized_pnl = self._settle_sell_fill_once(
                    item.strategy_id,
                    item.intent_id,
                    ledger_trade,
                )
                if inserted_cash:
                    cash_entries_appended += 1
                    sell_fill_received_amount += sell_entry.cash_delta
                    sell_fill_fee_amount += _money(ledger_trade.commission)
                    sell_fill_realized_pnl += realized_pnl
                    changed_strategy_ids.add(item.strategy_id)
            self._repository.delete_unattributed_trade(
                account_id=self._account_id,
                trade_date=self._trade_date,
                trade_id=trade.traded_id,
            )
            if inserted_trade:
                trades_inserted += 1
            else:
                trades_existing += 1
            if created_lot:
                lots_created += 1

        for order, strategy_id, intent_id in terminal_buy_orders:
            release_entry, inserted_cash = self._release_terminal_buy_freeze_once(strategy_id, intent_id, order)
            if release_entry is not None and inserted_cash:
                cash_entries_appended += 1
                buy_freeze_released_amount += release_entry.cash_delta
                changed_strategy_ids.add(strategy_id)

        for payload, order, broker_date in stale_orders:
            terminalized, release_entry, inserted_cash = self._terminalize_stale_order_once(
                payload,
                order,
                broker_date,
                strategy_id_by_name=strategy_id_by_name,
                strategy_name_by_id=strategy_name_by_id,
                strategy_id_by_remark_prefix=strategy_id_by_remark_prefix,
            )
            if terminalized:
                stale_orders_terminalized += 1
                status_events_appended += 1
            if release_entry is not None and inserted_cash:
                cash_entries_appended += 1
                buy_freeze_released_amount += release_entry.cash_delta
                stale_buy_freeze_released_amount += release_entry.cash_delta
                changed_strategy_ids.add(release_entry.strategy_id)

        for strategy_id in strategy_id_by_name.values():
            if self._revalue_strategy_account(strategy_id):
                changed_strategy_ids.add(strategy_id)

        return SyncSummary(
            account_id=self._account_id,
            trade_date=self._trade_date,
            orders_seen=len(raw_orders),
            orders_upserted=orders_upserted,
            trades_seen=len(raw_trades),
            trades_inserted=trades_inserted,
            trades_existing=trades_existing,
            unattributed_orders=unattributed_orders,
            unattributed_trades=unattributed_trades,
            status_events_appended=status_events_appended,
            lots_created=lots_created,
            cash_entries_appended=cash_entries_appended,
            buy_fill_settled_amount=buy_fill_settled_amount,
            buy_fill_fee_amount=buy_fill_fee_amount,
            sell_fill_received_amount=sell_fill_received_amount,
            sell_fill_fee_amount=sell_fill_fee_amount,
            sell_fill_realized_pnl=sell_fill_realized_pnl,
            buy_freeze_released_amount=buy_freeze_released_amount,
            accounts_revalued=len(changed_strategy_ids),
            positions_seen=len(positions),
            lots_unlocked=lots_unlocked,
            raw_positions=tuple(dict(item) for item in positions),
            stale_orders_skipped=stale_orders_skipped,
            stale_trades_skipped=stale_trades_skipped,
            stale_orders_terminalized=stale_orders_terminalized,
            stale_buy_freeze_released_amount=stale_buy_freeze_released_amount,
            stale_broker_snapshot=stale_orders_skipped > 0 or stale_trades_skipped > 0,
            stale_broker_payload_samples=tuple(stale_payload_samples[:10]),
        )

    def _unlock_tplus1_lots(self, strategy_ids: Any) -> int:
        unlocked = 0
        for strategy_id in strategy_ids:
            for lot in self._repository.list_position_lots(strategy_id):
                target_available = effective_lot_available_quantity(lot, self._trade_date, self._calendar_provider)
                if target_available <= lot.available_quantity:
                    continue
                self._repository.update_position_lot(
                    replace(
                        lot,
                        available_quantity=target_available,
                        metadata={
                            **lot.metadata,
                            "tplus1_available_as_of": self._trade_date.isoformat(),
                            "availability_source": "trading_calendar_tplus1",
                        },
                    )
                )
                unlocked += 1
        return unlocked

    def _settle_buy_fill_once(
        self,
        strategy_id: str,
        intent_id: str,
        trade: TradeLedgerRecord,
        order: RawQmtOrder,
    ) -> tuple[CashLedgerEntry, bool, bool, bool]:
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
        lot = PositionLotRecord(
            lot_id=_lot_id(self._account_id, self._trade_date, trade.trade_id),
            strategy_id=strategy_id,
            symbol=trade.symbol,
            open_trade_id=trade.trade_id,
            open_date=self._trade_date,
            quantity=trade.quantity,
            available_quantity=0,
            remaining_quantity=trade.quantity,
            avg_cost=trade.price,
            cost_amount=trade.amount,
            account_id=self._account_id,
            open_time=trade.trade_time,
            metadata={"source": "miniqmt_sync"},
        )
        _, trade_inserted, lot_created, _, cash_inserted = self._repository.apply_buy_trade_fill_once(
            trade,
            lot,
            entry,
            updated,
        )
        return entry, trade_inserted, lot_created, cash_inserted

    def _settle_sell_fill_once(
        self,
        strategy_id: str,
        intent_id: str,
        trade: TradeLedgerRecord,
    ) -> tuple[CashLedgerEntry, bool, bool, Decimal]:
        cash_id = _cash_event_id(self._account_id, self._trade_date, "sell_fill", trade.trade_id)
        if self._repository.get_cash_entry(cash_id) is not None:
            existing = self._repository.get_cash_entry(cash_id)
            account = self._repository.get_virtual_account(strategy_id)
            _, trade_inserted, _, cash_inserted = self._repository.apply_sell_trade_fill_once(
                trade,
                existing,
                account,
                [],
            )
            return existing, trade_inserted, cash_inserted, _money(Decimal(str(existing.metadata.get("realized_pnl", "0"))))

        fill_amount = _money(trade.amount)
        fee_amount = _money(trade.commission)
        realized_pnl, lot_closures, updated_lots = self._close_lots_fifo(strategy_id, trade)
        account = self._repository.get_virtual_account(strategy_id)
        cash_delta = fill_amount - fee_amount
        updated = replace(
            account,
            cash=account.cash + cash_delta,
            realized_pnl=account.realized_pnl + realized_pnl,
            updated_at=datetime.now(UTC),
        )
        entry = CashLedgerEntry(
            cash_id=cash_id,
            strategy_id=strategy_id,
            entry_type=CashEntryType.SELL_FILL,
            cash_delta=cash_delta,
            cash_after=updated.cash,
            frozen_delta=Decimal("0"),
            frozen_after=updated.frozen_cash,
            account_id=self._account_id,
            trade_date=self._trade_date,
            intent_id=intent_id,
            trade_id=trade.trade_id,
            symbol=trade.symbol,
            reason=CashEntryType.SELL_FILL.value,
            metadata={
                "source": "miniqmt_sync",
                "fill_amount": str(fill_amount),
                "commission": str(fee_amount),
                "realized_pnl": str(realized_pnl),
                "qmt_order_id": trade.qmt_order_id,
                "lot_closures": lot_closures,
            },
        )
        _, trade_inserted, _, cash_inserted = self._repository.apply_sell_trade_fill_once(
            trade,
            entry,
            updated,
            updated_lots,
        )
        return entry, trade_inserted, cash_inserted, realized_pnl if cash_inserted else Decimal("0")

    def _close_lots_fifo(self, strategy_id: str, trade: TradeLedgerRecord) -> tuple[Decimal, list[dict[str, Any]], list[PositionLotRecord]]:
        remaining_to_close = int(trade.quantity)
        realized_pnl = Decimal("0")
        lot_closures: list[dict[str, Any]] = []
        updated_lots: list[PositionLotRecord] = []
        for lot in self._repository.list_position_lots(strategy_id, symbol=trade.symbol):
            if remaining_to_close <= 0:
                break
            lot_remaining = max(int(lot.remaining_quantity), 0)
            if lot_remaining <= 0:
                continue
            close_quantity = min(remaining_to_close, lot_remaining)
            close_cost = _money(lot.avg_cost * Decimal(close_quantity))
            gross_proceeds = _money(trade.price * Decimal(close_quantity))
            proportional_fee = _proportional_fee(trade.commission, close_quantity, trade.quantity)
            lot_realized_pnl = _money(gross_proceeds - close_cost - proportional_fee)
            new_remaining = lot_remaining - close_quantity
            new_available = min(max(int(lot.available_quantity), 0), new_remaining)
            new_cost_amount = _money(lot.avg_cost * Decimal(new_remaining))
            new_status = PositionLotStatus.CLOSED if new_remaining == 0 else PositionLotStatus.PARTIALLY_CLOSED
            updated_lot = replace(
                lot,
                available_quantity=new_available,
                remaining_quantity=new_remaining,
                cost_amount=new_cost_amount,
                realized_pnl=lot.realized_pnl + lot_realized_pnl,
                status=new_status,
                metadata={
                    **lot.metadata,
                    "last_close_trade_id": trade.trade_id,
                    "last_close_trade_date": self._trade_date.isoformat(),
                },
            )
            updated_lots.append(updated_lot)
            realized_pnl += lot_realized_pnl
            remaining_to_close -= close_quantity
            lot_closures.append(
                {
                    "lot_id": lot.lot_id,
                    "closed_quantity": close_quantity,
                    "remaining_quantity": new_remaining,
                    "avg_cost": str(lot.avg_cost),
                    "close_price": str(trade.price),
                    "gross_proceeds": str(gross_proceeds),
                    "cost": str(close_cost),
                    "commission": str(proportional_fee),
                    "realized_pnl": str(lot_realized_pnl),
                }
            )
        if remaining_to_close > 0:
            raise ValueError(
                f"strategy lots are insufficient for SELL fill: strategy_id={strategy_id} "
                f"symbol={trade.symbol} trade_id={trade.trade_id} missing_quantity={remaining_to_close}"
            )
        return _money(realized_pnl), lot_closures, updated_lots

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

    def _terminalize_stale_order_once(
        self,
        payload: dict[str, Any],
        order: RawQmtOrder,
        broker_date: date,
        *,
        strategy_id_by_name: dict[str, str],
        strategy_name_by_id: dict[str, str],
        strategy_id_by_remark_prefix: dict[str, str],
    ) -> tuple[bool, CashLedgerEntry | None, bool]:
        if not order.order_remark:
            return False, None, False
        intent = self._repository.get_order_intent_by_remark(self._account_id, order.order_remark)
        if intent is None:
            return False, None, False
        if intent.trade_date != broker_date:
            return False, None, False
        strategy_id = _resolve_strategy_id(
            strategy_name=order.strategy_name,
            order_remark=order.order_remark,
            intent=intent,
            strategy_id_by_name=strategy_id_by_name,
            strategy_name_by_id=strategy_name_by_id,
            strategy_id_by_remark_prefix=strategy_id_by_remark_prefix,
        )
        if strategy_id is None:
            return False, None, False

        # A stale filled/partially filled broker row needs historical replay, not
        # rollover expiry. Only unfilled previous-day orders are terminalized here.
        if order.traded_volume > 0 or order.order_status == STATUS_FILLED:
            return False, None, False
        submit_status = IntentSubmitStatus.REJECTED if order.order_status == STATUS_REJECTED else IntentSubmitStatus.CANCELLED
        if intent.submit_status in {IntentSubmitStatus.CREATED, IntentSubmitStatus.SUBMITTED, IntentSubmitStatus.ACCEPTED}:
            self._repository.set_order_intent_submit_status(
                intent.intent_id,
                submit_status,
                submitted_at=intent.submitted_at,
                updated_at=datetime.now(UTC),
            )
        self._repository.append_order_status_event(
            OrderStatusEventRecord(
                event_id=_stale_order_event_id(self._account_id, self._trade_date, order.order_id),
                intent_id=intent.intent_id,
                qmt_order_id=order.order_id,
                qmt_order_sysid=order.order_sysid,
                event_type="STALE_ORDER_ROLLOVER",
                event_time=datetime.now(UTC),
                account_id=self._account_id,
                qmt_order_status=order.order_status,
                status_msg=order.status_msg,
                raw_json={
                    **dict(payload),
                    "stale_rollover": {
                        "source": "miniqmt_sync",
                        "broker_payload_date": broker_date.isoformat(),
                        "expected_trade_date": self._trade_date.isoformat(),
                        "reason": "BROKER_ORDER_DATE_MISMATCH",
                        "intent_submit_status": submit_status.value,
                    },
                },
            )
        )
        if order.order_type != BUY_ORDER_TYPE:
            return True, None, False
        release_entry, inserted_cash = self._release_stale_buy_freeze_once(strategy_id, intent.intent_id, order, broker_date)
        return True, release_entry, inserted_cash

    def _release_stale_buy_freeze_once(
        self,
        strategy_id: str,
        intent_id: str,
        order: RawQmtOrder,
        broker_date: date,
    ) -> tuple[CashLedgerEntry | None, bool]:
        cash_id = _cash_event_id(self._account_id, self._trade_date, "stale_buy_expire", order.order_id)
        account = self._repository.get_virtual_account(strategy_id)
        release_amount = min(self._remaining_frozen_for_intent(strategy_id, intent_id), account.frozen_cash)
        if release_amount <= Decimal("0"):
            return None, False
        entry_type = CashEntryType.UNFREEZE_REJECT if order.order_status == STATUS_REJECTED else CashEntryType.UNFREEZE_CANCEL
        reason = "STALE_BUY_ORDER_REJECTED" if order.order_status == STATUS_REJECTED else "STALE_BUY_ORDER_EXPIRED"
        updated = replace(
            account,
            cash=account.cash + release_amount,
            frozen_cash=account.frozen_cash - release_amount,
            updated_at=datetime.now(UTC),
        )
        entry = CashLedgerEntry(
            cash_id=cash_id,
            strategy_id=strategy_id,
            entry_type=entry_type,
            cash_delta=release_amount,
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
                "broker_payload_date": broker_date.isoformat(),
                "expected_trade_date": self._trade_date.isoformat(),
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


def _resolve_strategy_id(
    *,
    strategy_name: str,
    order_remark: str,
    intent: Any | None,
    strategy_id_by_name: dict[str, str],
    strategy_name_by_id: dict[str, str],
    strategy_id_by_remark_prefix: dict[str, str],
) -> str | None:
    if strategy_name and strategy_name in strategy_id_by_name:
        return strategy_id_by_name[strategy_name]
    if intent is not None and getattr(intent, "strategy_id", None) in strategy_name_by_id:
        return str(intent.strategy_id)
    if order_remark:
        for prefix, strategy_id in strategy_id_by_remark_prefix.items():
            if order_remark.startswith(f"{prefix}-"):
                return strategy_id
    return None


def _strategy_id_by_remark_prefix(accounts: list[Any]) -> dict[str, str]:
    candidates: dict[str, set[str]] = {}
    for account in accounts:
        prefixes: set[str] = set()
        slot = MiniQmtStrategySlot.from_virtual_account(account)
        if slot is not None:
            prefixes.add(slot.order_remark_prefix)
        prefixes.add(account.strategy_name)
        prefixes.add(account.strategy_id)
        for prefix in prefixes:
            normalized = str(prefix or "").strip()[:20]
            if normalized:
                candidates.setdefault(normalized, set()).add(account.strategy_id)
    return {
        prefix: next(iter(strategy_ids))
        for prefix, strategy_ids in candidates.items()
        if len(strategy_ids) == 1
    }


def _order_unattributed_reason(order: RawQmtOrder, strategy_id: str | None, remark_counts: dict[str, int]) -> str | None:
    if not order.order_remark:
        return "BLANK_ORDER_REMARK"
    if remark_counts.get(order.order_remark, 0) > 1:
        return "DUPLICATE_ORDER_REMARK"
    if strategy_id is None:
        if not order.strategy_name:
            return "BLANK_STRATEGY_NAME"
        return "UNKNOWN_STRATEGY_NAME"
    return None


def _trade_unattributed_reason(
    trade: RawQmtTrade,
    order: RawQmtOrder | None,
    strategy_id: str | None,
    has_intent: bool,
) -> str | None:
    if strategy_id is None:
        if not trade.strategy_name:
            return "BLANK_STRATEGY_NAME"
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


def _trade_settlement_sort_key(item: _AttributedTrade) -> tuple[int, str, datetime, str, int]:
    # Same-batch rebalances can be sell-funded; settle sells before buy cash debits.
    side_priority = 0 if item.trade.order_type == SELL_ORDER_TYPE else 1
    trade_time = item.ledger_trade.trade_time or datetime.min.replace(tzinfo=UTC)
    return (side_priority, item.strategy_id, trade_time, item.trade.traded_id, item.index)


def _parse_trade_time(trade_date: date, value: str) -> datetime | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    parsed_datetime = _parse_payload_datetime(cleaned)
    if parsed_datetime is not None:
        return parsed_datetime
    for fmt in ("%H%M%S", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return datetime.combine(trade_date, parsed.time(), tzinfo=UTC)
        except ValueError:
            continue
    return None


def _broker_order_payload_date(payload: dict[str, Any]) -> date | None:
    return (
        _parse_payload_date(payload.get("order_time_iso"))
        or _parse_payload_date((payload.get("diagnostic") or {}).get("order_time_iso") if isinstance(payload.get("diagnostic"), dict) else None)
        or _parse_payload_date(payload.get("order_datetime"))
        or _parse_payload_date(payload.get("order_date"))
        or _parse_payload_date(payload.get("order_time"))
    )


def _broker_trade_payload_date(payload: dict[str, Any]) -> date | None:
    return (
        _parse_payload_date(payload.get("traded_time_iso"))
        or _parse_payload_date(payload.get("trade_time_iso"))
        or _parse_payload_date(payload.get("trade_datetime"))
        or _parse_payload_date(payload.get("trade_date"))
        or _parse_payload_date(payload.get("traded_date"))
        or _parse_payload_date(payload.get("traded_time"))
    )


def _parse_payload_date(value: Any) -> date | None:
    dt = _parse_payload_datetime(value)
    if dt is not None:
        return dt.astimezone(CHINA_TZ).date() if dt.tzinfo is not None else dt.date()
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    candidates: list[tuple[str, str]] = []
    if len(cleaned) >= 10 and cleaned[4:5] == "-" and cleaned[7:8] == "-":
        candidates.append((cleaned[:10], "%Y-%m-%d"))
    if len(cleaned) == 8 and cleaned.isdigit():
        candidates.append((cleaned, "%Y%m%d"))
    for candidate, fmt in candidates:
        try:
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue
    return None


def _parse_payload_datetime(value: Any) -> datetime | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    normalized = cleaned.replace("Z", "+00:00")
    try:
        if "T" in normalized or "-" in normalized:
            return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    digits = cleaned
    if digits.isdigit() and len(digits) == 14:
        try:
            return datetime.strptime(digits, "%Y%m%d%H%M%S").replace(tzinfo=CHINA_TZ)
        except ValueError:
            return None
    if not digits.isdigit():
        return None
    try:
        raw_epoch = int(digits)
    except ValueError:
        return None
    if len(digits) >= 13:
        raw_epoch = raw_epoch // 1000
    if len(digits) < 10 or raw_epoch < 946684800:
        return None
    try:
        return datetime.fromtimestamp(raw_epoch, tz=UTC).astimezone(CHINA_TZ)
    except (OSError, OverflowError, ValueError):
        return None


def _stale_payload_sample(
    *,
    payload_type: str,
    payload: dict[str, Any],
    broker_date: date | None,
    expected_trade_date: date,
    reason: str,
) -> dict[str, Any]:
    sample_keys = (
        "order_id",
        "traded_id",
        "order_sysid",
        "stock_code",
        "order_time",
        "order_time_iso",
        "traded_time",
        "traded_time_iso",
        "strategy_name",
        "order_remark",
    )
    sample = {key: payload.get(key) for key in sample_keys if key in payload}
    return {
        "payload_type": payload_type,
        "reason": reason,
        "expected_trade_date": expected_trade_date.isoformat(),
        "broker_payload_date": broker_date.isoformat() if broker_date is not None else None,
        "payload": sample,
    }


def _unattributed_id(prefix: str, account_id: str, trade_date: date, row_id: str) -> str:
    safe_row_id = row_id or new_id("blank")
    return f"{prefix}_{account_id}_{trade_date.isoformat()}_{safe_row_id}"


def _order_event_id(account_id: str, order_id: str, order_status: int | None) -> str:
    status = "none" if order_status is None else str(order_status)
    return f"evt_{account_id}_{order_id}_{status}"


def _stale_order_event_id(account_id: str, trade_date: date, order_id: str) -> str:
    safe_order_id = order_id or "blank"
    return f"evt_{account_id}_{trade_date.isoformat()}_stale_rollover_{safe_order_id}"


def _lot_id(account_id: str, trade_date: date, trade_id: str) -> str:
    return f"lot_{account_id}_{trade_date.isoformat()}_{trade_id}"


def _cash_event_id(account_id: str, trade_date: date, event_type: str, row_id: str) -> str:
    safe_row_id = row_id or new_id("blank")
    return f"cash_{account_id}_{trade_date.isoformat()}_{event_type}_{safe_row_id}"


def _reserved_fill_amount(order: RawQmtOrder, trade: TradeLedgerRecord) -> Decimal:
    reserve_price = order.price if order.price > Decimal("0") else trade.price
    return _money(reserve_price * Decimal(trade.quantity))


def _proportional_fee(total_fee: Decimal, quantity: int, total_quantity: int) -> Decimal:
    if total_quantity <= 0:
        return Decimal("0")
    return _money(Decimal(total_fee) * Decimal(quantity) / Decimal(total_quantity))


def _money(value: Decimal) -> Decimal:
    return Decimal(value).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
