from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    CashEntryType,
    IntentSubmitStatus,
    OrderIntentRecord,
    PositionLotRecord,
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
