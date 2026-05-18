from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.services.qmt_strategy_ledger.models import BUY_ORDER_TYPE, OrderIntentRecord, VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


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


def test_sync_service_upserts_attributed_order_trade_and_lot_without_broker_submit() -> None:
    repo = _repo_with_strategy()
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
    ).sync_snapshot()

    assert summary.orders_seen == 1
    assert summary.orders_upserted == 1
    assert summary.trades_inserted == 1
    assert summary.trades_existing == 0
    assert summary.lots_created == 1
    assert summary.unattributed_orders == 0
    assert summary.unattributed_trades == 0
    assert repo.list_position_lots("strat_a", symbol="300604.SZ")[0].remaining_quantity == 1000

    idempotent = QmtStrategyLedgerSyncService(
        repository=repo,
        qmt_client=client,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
    ).sync_snapshot()
    assert idempotent.trades_inserted == 0
    assert idempotent.trades_existing == 1
    assert idempotent.lots_created == 0


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
