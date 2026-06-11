from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    CashEntryType,
    IntentSubmitStatus,
    OrderIntentRecord,
    PositionLotRecord,
    PositionLotStatus,
    UnattributedOrderRecord,
    UnattributedTradeRecord,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)
NEXT_TRADE_DATE = date(2026, 5, 19)
CALENDAR = StaticTradingCalendarProvider([TRADE_DATE, NEXT_TRADE_DATE])


class FakeReadOnlyQmtClient:
    def __init__(
        self,
        *,
        orders: list[dict],
        trades: list[dict],
        positions: list[dict],
    ) -> None:
        self.orders = orders
        self.trades = trades
        self.positions = positions
        self.get_orders_calls = 0

    def get_orders(self, cancelable_only: bool = False) -> list[dict]:
        assert cancelable_only is False
        self.get_orders_calls += 1
        return self.orders

    def get_trades(self) -> list[dict]:
        return self.trades

    def get_positions(self) -> list[dict]:
        return self.positions


def _repo_with_strategy() -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            display_name="POC Strategy A",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    repo.create_order_intent(
        OrderIntentRecord(
            intent_id="intent_a",
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            symbol="300604.SZ",
            side="BUY",
            order_type=BUY_ORDER_TYPE,
            quantity=1000,
            price_type=5,
            order_remark="remark_a",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
        )
    )
    return repo


def _apply_buy_freeze(repo: InMemoryQmtStrategyLedgerRepository, *, amount: Decimal, intent_id: str = "intent_a") -> None:
    _freeze_account(repo, strategy_id="strat_a", amount=amount, intent_id=intent_id)


def _sell_intent(
    repo: InMemoryQmtStrategyLedgerRepository,
    *,
    intent_id: str = "intent_sell",
    strategy_id: str = "strat_a",
    strategy_name: str = "poc_strategy_a",
    symbol: str = "300604.SZ",
    quantity: int = 1000,
    order_remark: str = "remark_sell",
) -> None:
    repo.create_order_intent(
        OrderIntentRecord(
            intent_id=intent_id,
            strategy_id=strategy_id,
            strategy_name=strategy_name,
            symbol=symbol,
            side="SELL",
            order_type=SELL_ORDER_TYPE,
            quantity=quantity,
            price_type=5,
            order_remark=order_remark,
            account_id=ACCOUNT_ID,
            trade_date=NEXT_TRADE_DATE,
        )
    )


def _position_lot(
    repo: InMemoryQmtStrategyLedgerRepository,
    *,
    lot_id: str,
    strategy_id: str = "strat_a",
    symbol: str = "300604.SZ",
    quantity: int = 1000,
    avg_cost: Decimal = Decimal("10"),
) -> None:
    repo.create_position_lot(
        PositionLotRecord(
            lot_id=lot_id,
            strategy_id=strategy_id,
            symbol=symbol,
            open_trade_id=f"trade_{lot_id}",
            open_date=TRADE_DATE,
            quantity=quantity,
            available_quantity=quantity,
            remaining_quantity=quantity,
            avg_cost=avg_cost,
            cost_amount=avg_cost * Decimal(quantity),
            account_id=ACCOUNT_ID,
        )
    )


def _freeze_account(
    repo: InMemoryQmtStrategyLedgerRepository,
    *,
    strategy_id: str,
    amount: Decimal,
    intent_id: str,
    symbol: str = "300604.SZ",
) -> None:
    account = repo.get_virtual_account(strategy_id)
    request = type(
        "FreezeRequest",
        (),
        {"account_id": ACCOUNT_ID, "trade_date": TRADE_DATE, "symbol": symbol},
    )()
    from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService

    QmtManagedOrderService(repository=repo)._apply_cash_entry(
        account,
        request,
        amount,
        CashEntryType.FREEZE_BUY,
        intent_id,
    )


def test_sync_service_upserts_attributed_order_trade_and_lot_without_broker_submit() -> None:
    repo = _repo_with_strategy()
    _apply_buy_freeze(repo, amount=Decimal("10250"))
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_a",
                "order_sysid": "sys_a",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10.25,
                "traded_volume": 1000,
                "traded_price": 10.25,
                "order_status": 56,
                "status_msg": "filled",
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        trades=[
            {
                "traded_id": "trade_a",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "traded_time": "101530",
                "traded_price": 10.25,
                "traded_volume": 1000,
                "traded_amount": 10250,
                "order_id": "order_a",
                "order_sysid": "sys_a",
                "commission": 5,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        positions=[{"stock_code": "300604.SZ", "quantity": 1000, "can_sell": 0}],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.orders_seen == 1
    assert summary.orders_upserted == 1
    assert summary.trades_inserted == 1
    assert summary.trades_existing == 0
    assert summary.lots_created == 1
    assert summary.cash_entries_appended == 1
    assert summary.buy_fill_settled_amount == Decimal("10250.000000")
    assert summary.buy_fill_fee_amount == Decimal("5.000000")
    assert summary.accounts_revalued == 1
    assert summary.unattributed_orders == 0
    assert summary.unattributed_trades == 0
    assert repo.list_position_lots("strat_a", symbol="300604.SZ")[0].remaining_quantity == 1000
    account = repo.get_virtual_account("strat_a")
    assert account.cash == Decimal("9989745.000000")
    assert account.frozen_cash == Decimal("0.000000")
    assert account.market_value == Decimal("10250.000000")
    assert account.unrealized_pnl == Decimal("0.000000")
    assert [entry.entry_type for entry in repo.list_cash_entries("strat_a")] == [
        CashEntryType.FREEZE_BUY,
        CashEntryType.BUY_FILL,
    ]
    assert repo.get_order_intent("intent_a").submit_status == IntentSubmitStatus.ACCEPTED

    idempotent = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()
    assert idempotent.trades_inserted == 0
    assert idempotent.trades_existing == 1
    assert idempotent.lots_created == 0
    assert idempotent.cash_entries_appended == 0
    assert repo.get_virtual_account("strat_a") == account


def test_sync_service_skips_stale_previous_day_broker_orders_and_trades() -> None:
    repo = _repo_with_strategy()
    _apply_buy_freeze(repo, amount=Decimal("10250"))
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_stale",
                "order_sysid": "sys_stale",
                "stock_code": "300604.SZ",
                "order_time": "1779028200",
                "order_time_iso": "2026-05-18T09:10:00+08:00",
                "order_type": BUY_ORDER_TYPE,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10.25,
                "traded_volume": 1000,
                "traded_price": 10.25,
                "order_status": 56,
                "status_msg": "filled",
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        trades=[
            {
                "traded_id": "trade_stale",
                "stock_code": "300604.SZ",
                "order_type": BUY_ORDER_TYPE,
                "traded_time": "101530",
                "traded_price": 10.25,
                "traded_volume": 1000,
                "traded_amount": 10250,
                "order_id": "order_stale",
                "order_sysid": "sys_stale",
                "commission": 5,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        positions=[],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.orders_seen == 1
    assert summary.trades_seen == 1
    assert summary.orders_upserted == 0
    assert summary.trades_inserted == 0
    assert summary.unattributed_orders == 0
    assert summary.unattributed_trades == 0
    assert summary.cash_entries_appended == 0
    assert summary.lots_created == 0
    assert summary.stale_orders_skipped == 1
    assert summary.stale_trades_skipped == 1
    assert summary.stale_broker_snapshot is True
    assert {item["reason"] for item in summary.stale_broker_payload_samples} == {
        "BROKER_ORDER_DATE_MISMATCH",
        "BROKER_TRADE_LINKED_TO_STALE_ORDER",
    }
    assert repo._order_ledgers == {}
    assert repo._trade_ledgers == {}
    assert repo.list_unattributed_orders(account_id=ACCOUNT_ID, trade_date=NEXT_TRADE_DATE) == []
    assert repo.list_unattributed_trades(account_id=ACCOUNT_ID, trade_date=NEXT_TRADE_DATE) == []
    assert repo.list_position_lots("strat_a", symbol="300604.SZ") == []
    assert repo.get_virtual_account("strat_a").frozen_cash == Decimal("10250.000000")


def test_sync_service_skips_trade_with_explicit_mismatched_broker_trade_date() -> None:
    repo = _repo_with_strategy()
    _apply_buy_freeze(repo, amount=Decimal("10250"))
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_current",
                "order_sysid": "sys_current",
                "stock_code": "300604.SZ",
                "order_time_iso": "2026-05-19T09:10:00+08:00",
                "order_type": BUY_ORDER_TYPE,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10.25,
                "traded_volume": 1000,
                "traded_price": 10.25,
                "order_status": 56,
                "status_msg": "filled",
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        trades=[
            {
                "traded_id": "trade_stale_date",
                "stock_code": "300604.SZ",
                "order_type": BUY_ORDER_TYPE,
                "trade_date": "2026-05-18",
                "traded_time": "101530",
                "traded_price": 10.25,
                "traded_volume": 1000,
                "traded_amount": 10250,
                "order_id": "order_current",
                "order_sysid": "sys_current",
                "commission": 5,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        positions=[],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.orders_seen == 1
    assert summary.trades_seen == 1
    assert summary.orders_upserted == 1
    assert summary.trades_inserted == 0
    assert summary.stale_orders_skipped == 0
    assert summary.stale_trades_skipped == 1
    assert summary.stale_broker_payload_samples[0]["reason"] == "BROKER_TRADE_DATE_MISMATCH"
    assert repo._trade_ledgers == {}
    assert repo.list_unattributed_trades(account_id=ACCOUNT_ID, trade_date=NEXT_TRADE_DATE) == []
    assert [entry.entry_type for entry in repo.list_cash_entries("strat_a")] == [CashEntryType.FREEZE_BUY]
    assert repo.get_virtual_account("strat_a").frozen_cash == Decimal("10250.000000")


def test_sync_service_attributes_truncated_strategy_name_by_managed_order_remark() -> None:
    repo = _repo_with_strategy()
    repo.upsert_unattributed_order(
        UnattributedOrderRecord(
            unattributed_id="uo_stale",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
            qmt_order_id="order_truncated",
            symbol="300604.SZ",
            reason="UNKNOWN_STRATEGY_NAME",
            order_remark="remark_a",
        )
    )
    repo.upsert_unattributed_trade(
        UnattributedTradeRecord(
            unattributed_id="ut_stale",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
            trade_id="trade_truncated",
            qmt_order_id="order_truncated",
            symbol="300604.SZ",
            reason="UNKNOWN_STRATEGY_NAME",
            order_remark="remark_a",
        )
    )
    _apply_buy_freeze(repo, amount=Decimal("10005"))
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_truncated",
                "order_sysid": "sys_truncated",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10,
                "traded_volume": 1000,
                "traded_price": 10,
                "order_status": 56,
                "strategy_name": "poc_strategy",
                "order_remark": "remark_a",
            }
        ],
        trades=[
            {
                "traded_id": "trade_truncated",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "traded_time": "102000",
                "traded_price": 10,
                "traded_volume": 1000,
                "traded_amount": 10000,
                "commission": 5,
                "order_id": "order_truncated",
                "strategy_name": "poc_strategy",
                "order_remark": "remark_a",
            }
        ],
        positions=[{"stock_code": "300604.SZ", "quantity": 1000, "can_sell": 0}],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.orders_upserted == 1
    assert summary.trades_inserted == 1
    assert summary.unattributed_orders == 0
    assert summary.unattributed_trades == 0
    lots = repo.list_position_lots("strat_a", symbol="300604.SZ")
    assert [lot.open_trade_id for lot in lots] == ["trade_truncated"]
    assert repo.list_unattributed_orders(account_id=ACCOUNT_ID, trade_date=TRADE_DATE) == []
    assert repo.list_unattributed_trades(account_id=ACCOUNT_ID, trade_date=TRADE_DATE) == []


def test_sync_service_settles_unmanaged_buy_fill_against_cash_without_freeze() -> None:
    repo = _repo_with_strategy()
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_unmanaged",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10,
                "traded_volume": 1000,
                "order_status": 56,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        trades=[
            {
                "traded_id": "trade_unmanaged",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "traded_time": "101530",
                "traded_price": 10,
                "traded_volume": 1000,
                "traded_amount": 10000,
                "order_id": "order_unmanaged",
                "commission": 5,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        positions=[{"stock_code": "300604.SZ", "quantity": 1000, "can_sell": 0}],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.cash_entries_appended == 1
    assert summary.buy_fill_settled_amount == Decimal("0.000000")
    assert summary.buy_fill_fee_amount == Decimal("5.000000")
    account = repo.get_virtual_account("strat_a")
    assert account.cash == Decimal("9989995.000000")
    assert account.frozen_cash == Decimal("0")
    assert account.market_value == Decimal("10000.000000")
    entries = repo.list_cash_entries("strat_a")
    assert len(entries) == 1
    assert entries[0].entry_type == CashEntryType.BUY_FILL
    assert entries[0].cash_delta == Decimal("-10005.000000")
    assert entries[0].frozen_delta == Decimal("0")

    idempotent = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()
    assert idempotent.cash_entries_appended == 0
    assert repo.get_virtual_account("strat_a") == account


def test_sync_service_settles_sell_funded_rebalance_before_buy_when_trades_arrive_buy_first() -> None:
    repo = _repo_with_strategy()
    account = repo.get_virtual_account("strat_a")
    repo.update_virtual_account(replace(account, cash=Decimal("0"), market_value=Decimal("1000")))
    _position_lot(repo, lot_id="lot_sell_fund", quantity=100, avg_cost=Decimal("10"))
    _sell_intent(repo, intent_id="intent_sell_fund", quantity=100, order_remark="remark_sell_fund")
    repo.create_order_intent(
        OrderIntentRecord(
            intent_id="intent_buy_rebalance",
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            symbol="600000.SH",
            side="BUY",
            order_type=BUY_ORDER_TYPE,
            quantity=100,
            price_type=5,
            order_remark="remark_buy_rebalance",
            account_id=ACCOUNT_ID,
            trade_date=NEXT_TRADE_DATE,
        )
    )
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_buy_rebalance",
                "stock_code": "600000.SH",
                "order_type": BUY_ORDER_TYPE,
                "order_volume": 100,
                "price_type": 5,
                "price": 10,
                "traded_volume": 100,
                "traded_price": 10,
                "order_status": 56,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_buy_rebalance",
            },
            {
                "order_id": "order_sell_fund",
                "stock_code": "300604.SZ",
                "order_type": SELL_ORDER_TYPE,
                "order_volume": 100,
                "price_type": 5,
                "price": 12,
                "traded_volume": 100,
                "traded_price": 12,
                "order_status": 56,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_sell_fund",
            },
        ],
        trades=[
            {
                "traded_id": "trade_buy_first",
                "stock_code": "600000.SH",
                "order_type": BUY_ORDER_TYPE,
                "traded_time": "103001",
                "traded_price": 10,
                "traded_volume": 100,
                "traded_amount": 1000,
                "order_id": "order_buy_rebalance",
                "commission": 0,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_buy_rebalance",
            },
            {
                "traded_id": "trade_sell_second",
                "stock_code": "300604.SZ",
                "order_type": SELL_ORDER_TYPE,
                "traded_time": "103000",
                "traded_price": 12,
                "traded_volume": 100,
                "traded_amount": 1200,
                "order_id": "order_sell_fund",
                "commission": 0,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_sell_fund",
            },
        ],
        positions=[{"stock_code": "600000.SH", "quantity": 100, "can_sell": 0}],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    entries = repo.list_cash_entries("strat_a")
    account = repo.get_virtual_account("strat_a")
    assert summary.trades_inserted == 2
    assert summary.lots_created == 1
    assert summary.cash_entries_appended == 2
    assert [entry.entry_type for entry in entries] == [CashEntryType.SELL_FILL, CashEntryType.BUY_FILL]
    assert account.cash == Decimal("200.000000")
    assert account.realized_pnl == Decimal("200.000000")
    assert repo.list_position_lots("strat_a", "300604.SZ")[0].remaining_quantity == 0
    assert repo.list_position_lots("strat_a", "600000.SH")[0].remaining_quantity == 100

    idempotent = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()
    assert idempotent.trades_existing == 2
    assert idempotent.lots_created == 0
    assert idempotent.cash_entries_appended == 0
    assert repo.get_virtual_account("strat_a") == account


def test_sync_service_rolls_back_unfunded_buy_without_partial_trade_or_lot() -> None:
    repo = _repo_with_strategy()
    account = repo.get_virtual_account("strat_a")
    repo.update_virtual_account(replace(account, cash=Decimal("0")))
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_unfunded",
                "stock_code": "300604.SZ",
                "order_type": BUY_ORDER_TYPE,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10,
                "traded_volume": 1000,
                "traded_price": 10,
                "order_status": 56,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        trades=[
            {
                "traded_id": "trade_unfunded",
                "stock_code": "300604.SZ",
                "order_type": BUY_ORDER_TYPE,
                "traded_time": "101530",
                "traded_price": 10,
                "traded_volume": 1000,
                "traded_amount": 10000,
                "order_id": "order_unfunded",
                "commission": 0,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        positions=[],
    )

    with pytest.raises(ValueError, match="cash and frozen_cash must be non-negative"):
        QmtStrategyLedgerSyncService(
            repository=repo,
            qmt_client=client,
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
            calendar_provider=CALENDAR,
        ).sync_snapshot()

    assert repo._trade_ledgers == {}
    assert repo.list_position_lots("strat_a", "300604.SZ") == []
    assert repo.list_cash_entries("strat_a") == []


def test_sync_service_settles_cheaper_fill_and_releases_cancelled_residual_once() -> None:
    repo = _repo_with_strategy()
    _apply_buy_freeze(repo, amount=Decimal("10000"))
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_partial",
                "order_sysid": "sys_partial",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10,
                "traded_volume": 400,
                "traded_price": 10,
                "order_status": 54,
                "status_msg": "cancelled",
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        trades=[
            {
                "traded_id": "trade_partial",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "traded_time": "101530",
                "traded_price": 9.5,
                "traded_volume": 400,
                "traded_amount": 3800,
                "order_id": "order_partial",
                "order_sysid": "sys_partial",
                "commission": 2,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        positions=[{"stock_code": "300604.SZ", "quantity": 400, "can_sell": 0}],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.cash_entries_appended == 2
    assert summary.buy_fill_settled_amount == Decimal("4000.000000")
    assert summary.buy_freeze_released_amount == Decimal("6000.000000")
    account = repo.get_virtual_account("strat_a")
    assert account.cash == Decimal("9996198.000000")
    assert account.frozen_cash == Decimal("0.000000")
    assert account.market_value == Decimal("3800.000000")
    assert [entry.entry_type for entry in repo.list_cash_entries("strat_a")] == [
        CashEntryType.FREEZE_BUY,
        CashEntryType.BUY_FILL,
        CashEntryType.UNFREEZE_CANCEL,
    ]
    assert repo.get_order_intent("intent_a").submit_status == IntentSubmitStatus.CANCELLED

    idempotent = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()
    assert idempotent.cash_entries_appended == 0
    assert repo.get_virtual_account("strat_a") == account


def test_sync_service_releases_rejected_buy_freeze_once() -> None:
    repo = _repo_with_strategy()
    _apply_buy_freeze(repo, amount=Decimal("10000"))
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_rejected",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10,
                "traded_volume": 0,
                "order_status": 57,
                "status_msg": "rejected",
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            }
        ],
        trades=[],
        positions=[],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.cash_entries_appended == 1
    assert summary.buy_freeze_released_amount == Decimal("10000.000000")
    account = repo.get_virtual_account("strat_a")
    assert account.cash == Decimal("10000000.000000")
    assert account.frozen_cash == Decimal("0.000000")
    assert [entry.entry_type for entry in repo.list_cash_entries("strat_a")] == [
        CashEntryType.FREEZE_BUY,
        CashEntryType.UNFREEZE_REJECT,
    ]
    assert repo.get_order_intent("intent_a").submit_status == IntentSubmitStatus.REJECTED

    idempotent = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()
    assert idempotent.cash_entries_appended == 0
    assert repo.get_virtual_account("strat_a") == account


def test_sync_service_values_same_symbol_by_strategy_lots_independently() -> None:
    repo = _repo_with_strategy()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_b",
            strategy_name="poc_strategy_b",
            display_name="POC Strategy B",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    repo.create_order_intent(
        OrderIntentRecord(
            intent_id="intent_b",
            strategy_id="strat_b",
            strategy_name="poc_strategy_b",
            symbol="300604.SZ",
            side="BUY",
            order_type=BUY_ORDER_TYPE,
            quantity=300,
            price_type=5,
            order_remark="remark_b",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
        )
    )
    _apply_buy_freeze(repo, amount=Decimal("2000"), intent_id="intent_a")
    _freeze_account(repo, strategy_id="strat_b", amount=Decimal("6000"), intent_id="intent_b")
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_a",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 100,
                "price_type": 5,
                "price": 20,
                "traded_volume": 100,
                "traded_price": 20,
                "order_status": 56,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            },
            {
                "order_id": "order_b",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 300,
                "price_type": 5,
                "price": 20,
                "traded_volume": 300,
                "traded_price": 20,
                "order_status": 56,
                "strategy_name": "poc_strategy_b",
                "order_remark": "remark_b",
            },
        ],
        trades=[
            {
                "traded_id": "trade_a",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "traded_time": "101530",
                "traded_price": 20,
                "traded_volume": 100,
                "traded_amount": 2000,
                "order_id": "order_a",
                "commission": 0,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_a",
            },
            {
                "traded_id": "trade_b",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "traded_time": "101531",
                "traded_price": 20,
                "traded_volume": 300,
                "traded_amount": 6000,
                "order_id": "order_b",
                "commission": 0,
                "strategy_name": "poc_strategy_b",
                "order_remark": "remark_b",
            },
        ],
        positions=[{"stock_code": "300604.SZ", "quantity": 400, "can_sell": 0}],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.positions_seen == 1
    assert repo.get_virtual_account("strat_a").market_value == Decimal("2000.000000")
    assert repo.get_virtual_account("strat_b").market_value == Decimal("6000.000000")
    assert repo.get_virtual_account("strat_a").frozen_cash == Decimal("0.000000")
    assert repo.get_virtual_account("strat_b").frozen_cash == Decimal("0.000000")


def test_sync_service_full_sell_fill_closes_fifo_lot_and_records_cash_pnl_once() -> None:
    repo = _repo_with_strategy()
    _position_lot(repo, lot_id="lot_sell_full", quantity=1000, avg_cost=Decimal("10"))
    _sell_intent(repo, quantity=1000)
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_sell",
                "stock_code": "300604.SZ",
                "order_type": SELL_ORDER_TYPE,
                "order_volume": 1000,
                "price_type": 5,
                "price": 12,
                "traded_volume": 1000,
                "traded_price": 12,
                "order_status": 56,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_sell",
            }
        ],
        trades=[
            {
                "traded_id": "trade_sell",
                "stock_code": "300604.SZ",
                "order_type": SELL_ORDER_TYPE,
                "traded_time": "103000",
                "traded_price": 12,
                "traded_volume": 1000,
                "traded_amount": 12000,
                "order_id": "order_sell",
                "commission": 6,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_sell",
            }
        ],
        positions=[],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    lot = repo.list_position_lots("strat_a", "300604.SZ")[0]
    account = repo.get_virtual_account("strat_a")
    entries = repo.list_cash_entries("strat_a")
    assert summary.trades_inserted == 1
    assert summary.cash_entries_appended == 1
    assert summary.sell_fill_received_amount == Decimal("11994.000000")
    assert summary.sell_fill_fee_amount == Decimal("6.000000")
    assert summary.sell_fill_realized_pnl == Decimal("1994.000000")
    assert lot.remaining_quantity == 0
    assert lot.available_quantity == 0
    assert lot.status == PositionLotStatus.CLOSED
    assert lot.realized_pnl == Decimal("1994.000000")
    assert account.cash == Decimal("10011994.000000")
    assert account.realized_pnl == Decimal("1994.000000")
    assert account.market_value == Decimal("0.000000")
    assert entries[-1].entry_type == CashEntryType.SELL_FILL
    assert entries[-1].cash_delta == Decimal("11994.000000")
    assert entries[-1].metadata["lot_closures"][0]["lot_id"] == "lot_sell_full"

    idempotent = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()
    assert idempotent.trades_existing == 1
    assert idempotent.cash_entries_appended == 0
    assert repo.get_virtual_account("strat_a") == account
    assert repo.list_position_lots("strat_a", "300604.SZ")[0] == lot


def test_sync_service_partial_sell_fill_partially_closes_fifo_lot() -> None:
    repo = _repo_with_strategy()
    _position_lot(repo, lot_id="lot_sell_partial", quantity=1000, avg_cost=Decimal("10"))
    _sell_intent(repo, quantity=400)
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_sell_partial",
                "stock_code": "300604.SZ",
                "order_type": SELL_ORDER_TYPE,
                "order_volume": 400,
                "price": 11,
                "traded_volume": 400,
                "traded_price": 11,
                "order_status": 56,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_sell",
            }
        ],
        trades=[
            {
                "traded_id": "trade_sell_partial",
                "stock_code": "300604.SZ",
                "order_type": SELL_ORDER_TYPE,
                "traded_time": "103000",
                "traded_price": 11,
                "traded_volume": 400,
                "traded_amount": 4400,
                "order_id": "order_sell_partial",
                "commission": 2,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_sell",
            }
        ],
        positions=[{"stock_code": "300604.SZ", "quantity": 600, "can_sell": 600}],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    lot = repo.list_position_lots("strat_a", "300604.SZ")[0]
    account = repo.get_virtual_account("strat_a")
    assert summary.sell_fill_realized_pnl == Decimal("398.000000")
    assert lot.remaining_quantity == 600
    assert lot.available_quantity == 600
    assert lot.cost_amount == Decimal("6000.000000")
    assert lot.status == PositionLotStatus.PARTIALLY_CLOSED
    assert lot.realized_pnl == Decimal("398.000000")
    assert account.cash == Decimal("10004398.000000")
    assert account.realized_pnl == Decimal("398.000000")
    assert account.market_value == Decimal("6000.000000")


def test_sync_service_same_symbol_sell_closes_only_selling_strategy_lots() -> None:
    repo = _repo_with_strategy()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_b",
            strategy_name="poc_strategy_b",
            display_name="POC Strategy B",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    _position_lot(repo, lot_id="lot_a_sell", strategy_id="strat_a", quantity=1000, avg_cost=Decimal("10"))
    _position_lot(repo, lot_id="lot_b_keep", strategy_id="strat_b", quantity=1000, avg_cost=Decimal("20"))
    _sell_intent(repo, quantity=500)
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_sell_a",
                "stock_code": "300604.SZ",
                "order_type": SELL_ORDER_TYPE,
                "order_volume": 500,
                "price": 12,
                "traded_volume": 500,
                "traded_price": 12,
                "order_status": 56,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_sell",
            }
        ],
        trades=[
            {
                "traded_id": "trade_sell_a",
                "stock_code": "300604.SZ",
                "order_type": SELL_ORDER_TYPE,
                "traded_time": "103000",
                "traded_price": 12,
                "traded_volume": 500,
                "traded_amount": 6000,
                "order_id": "order_sell_a",
                "commission": 3,
                "strategy_name": "poc_strategy_a",
                "order_remark": "remark_sell",
            }
        ],
        positions=[{"stock_code": "300604.SZ", "quantity": 1500, "can_sell": 1500}],
    )

    QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    lot_a = repo.list_position_lots("strat_a", "300604.SZ")[0]
    lot_b = repo.list_position_lots("strat_b", "300604.SZ")[0]
    assert lot_a.remaining_quantity == 500
    assert lot_a.realized_pnl == Decimal("997.000000")
    assert lot_b.remaining_quantity == 1000
    assert lot_b.realized_pnl == Decimal("0")
    assert repo.get_virtual_account("strat_a").realized_pnl == Decimal("997.000000")
    assert repo.get_virtual_account("strat_b").realized_pnl == Decimal("0")


def test_sync_service_unlocks_prior_trading_day_lot_on_tplus1_idempotently() -> None:
    repo = _repo_with_strategy()
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_prior",
            strategy_id="strat_a",
            symbol="300604.SZ",
            open_trade_id="trade_prior",
            open_date=TRADE_DATE,
            quantity=1000,
            available_quantity=0,
            remaining_quantity=1000,
            avg_cost=Decimal("10"),
            cost_amount=Decimal("10000"),
            account_id=ACCOUNT_ID,
        )
    )
    client = FakeReadOnlyQmtClient(orders=[], trades=[], positions=[{"stock_code": "300604.SZ", "can_sell": 1000}])

    same_day = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()
    tplus1 = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()
    rerun = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    lot = repo.list_position_lots("strat_a", "300604.SZ")[0]
    assert same_day.lots_unlocked == 0
    assert tplus1.lots_unlocked == 1
    assert rerun.lots_unlocked == 0
    assert lot.available_quantity == 1000
    assert lot.metadata["tplus1_available_as_of"] == NEXT_TRADE_DATE.isoformat()


def test_sync_service_keeps_same_symbol_strategy_lot_unlocks_independent() -> None:
    repo = _repo_with_strategy()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_b",
            strategy_name="poc_strategy_b",
            display_name="POC Strategy B",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    for strategy_id, lot_id, quantity in (("strat_a", "lot_a_t1", 1000), ("strat_b", "lot_b_t1", 600)):
        repo.create_position_lot(
            PositionLotRecord(
                lot_id=lot_id,
                strategy_id=strategy_id,
                symbol="300604.SZ",
                open_trade_id=f"trade_{lot_id}",
                open_date=TRADE_DATE,
                quantity=quantity,
                available_quantity=0,
                remaining_quantity=quantity,
                avg_cost=Decimal("10"),
                cost_amount=Decimal(quantity * 10),
                account_id=ACCOUNT_ID,
            )
        )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=FakeReadOnlyQmtClient(orders=[], trades=[], positions=[]),
        account_id=ACCOUNT_ID,
        trade_date=NEXT_TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.lots_unlocked == 2
    assert repo.list_position_lots("strat_a", "300604.SZ")[0].available_quantity == 1000
    assert repo.list_position_lots("strat_b", "300604.SZ")[0].available_quantity == 600


def test_sync_service_does_not_unlock_on_non_trading_day() -> None:
    weekend = date(2026, 5, 23)
    calendar = StaticTradingCalendarProvider([TRADE_DATE, NEXT_TRADE_DATE])
    repo = _repo_with_strategy()
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_weekend",
            strategy_id="strat_a",
            symbol="300604.SZ",
            open_trade_id="trade_weekend",
            open_date=TRADE_DATE,
            quantity=1000,
            available_quantity=0,
            remaining_quantity=1000,
            avg_cost=Decimal("10"),
            cost_amount=Decimal("10000"),
            account_id=ACCOUNT_ID,
        )
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=FakeReadOnlyQmtClient(orders=[], trades=[], positions=[]),
        account_id=ACCOUNT_ID,
        trade_date=weekend,
        calendar_provider=calendar,
    ).sync_snapshot()

    assert summary.lots_unlocked == 0
    assert repo.list_position_lots("strat_a", "300604.SZ")[0].available_quantity == 0


def test_sync_service_routes_blank_strategy_duplicate_remark_and_unknown_trade_to_unattributed() -> None:
    repo = _repo_with_strategy()
    client = FakeReadOnlyQmtClient(
        orders=[
            {
                "order_id": "order_blank",
                "stock_code": "300054.SZ",
                "order_type": 23,
                "order_volume": 1000,
                "price_type": 5,
                "price": 20,
                "traded_volume": 0,
                "order_status": 54,
                "strategy_name": "",
                "order_remark": "blank_strategy",
            },
            {
                "order_id": "order_dup_a",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10,
                "traded_volume": 0,
                "order_status": 50,
                "strategy_name": "poc_strategy_a",
                "order_remark": "dup_remark",
            },
            {
                "order_id": "order_dup_b",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "order_volume": 1000,
                "price_type": 5,
                "price": 10,
                "traded_volume": 0,
                "order_status": 57,
                "strategy_name": "poc_strategy_a",
                "order_remark": "dup_remark",
            },
        ],
        trades=[
            {
                "traded_id": "trade_unknown",
                "stock_code": "300604.SZ",
                "order_type": 23,
                "traded_time": "102000",
                "traded_price": 10,
                "traded_volume": 1000,
                "traded_amount": 10000,
                "order_id": "missing_order",
                "strategy_name": "poc_strategy_a",
                "order_remark": "missing_order_remark",
            }
        ],
        positions=[],
    )

    summary = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        calendar_provider=CALENDAR,
    ).sync_snapshot()

    assert summary.orders_seen == 3
    assert summary.orders_upserted == 0
    assert summary.unattributed_orders == 3
    assert summary.unattributed_trades == 1
    assert {record.reason for record in repo.list_unattributed_orders(ACCOUNT_ID, TRADE_DATE)} == {
        "BLANK_STRATEGY_NAME",
        "DUPLICATE_ORDER_REMARK",
    }
    assert repo.list_unattributed_trades(ACCOUNT_ID, TRADE_DATE)[0].reason == "TRADE_WITHOUT_ORDER"
