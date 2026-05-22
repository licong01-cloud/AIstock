from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    IntentSubmitStatus,
    OrderBatchStatus,
    PositionLotRecord,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.order_service import ManagedCancelRequest, ManagedOrderRequest, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)
NEXT_TRADE_DATE = date(2026, 5, 19)
CALENDAR = StaticTradingCalendarProvider([TRADE_DATE, NEXT_TRADE_DATE])


class FakeManagedBroker:
    def __init__(self, order_ids: list[int]) -> None:
        self.order_ids = list(order_ids)
        self.place_order_payloads: list[dict] = []
        self.cancelled: list[str] = []

    def get_positions(self) -> list[dict]:
        return [{"stock_code": "300604.SZ", "quantity": 1000, "can_sell": 1000}]

    def place_order(self, **kwargs):
        self.place_order_payloads.append(kwargs)
        order_id = self.order_ids.pop(0) if self.order_ids else 0
        return order_id, "accepted" if order_id > 0 else "rejected by fake broker"

    def cancel_order(self, order_id: str):
        self.cancelled.append(order_id)
        return True, "cancelled"


def _repo() -> InMemoryQmtStrategyLedgerRepository:
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
    return repo


def _buy_request(order_remark: str, quantity: int = 1000) -> ManagedOrderRequest:
    return ManagedOrderRequest(
        account_id=ACCOUNT_ID,
        strategy_name="poc_strategy_a",
        symbol="300604.SZ",
        side="BUY",
        order_type=BUY_ORDER_TYPE,
        quantity=quantity,
        price_type=5,
        price=Decimal("10"),
        order_remark=order_remark,
        trade_date=TRADE_DATE,
        mode="SIM",
    )


def _sell_request(order_remark: str, quantity: int = 1000) -> ManagedOrderRequest:
    return ManagedOrderRequest(
        account_id=ACCOUNT_ID,
        strategy_name="poc_strategy_a",
        symbol="300604.SZ",
        side="SELL",
        order_type=SELL_ORDER_TYPE,
        quantity=quantity,
        price_type=5,
        price=Decimal("10"),
        order_remark=order_remark,
        trade_date=NEXT_TRADE_DATE,
        mode="SIM",
    )


def _service(repo: InMemoryQmtStrategyLedgerRepository, broker: FakeManagedBroker) -> QmtManagedOrderService:
    return QmtManagedOrderService(repository=repo, broker=broker, calendar_provider=CALENDAR)


def _add_sellable_lot(repo: InMemoryQmtStrategyLedgerRepository, quantity: int = 1000) -> None:
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_a",
            strategy_id="strat_a",
            symbol="300604.SZ",
            open_trade_id="trade_a",
            open_date=TRADE_DATE,
            quantity=quantity,
            available_quantity=0,
            remaining_quantity=quantity,
            avg_cost=Decimal("10"),
            cost_amount=Decimal(quantity * 10),
            account_id=ACCOUNT_ID,
        )
    )


def test_submit_order_creates_intent_freezes_cash_and_calls_fake_broker() -> None:
    repo = _repo()
    broker = FakeManagedBroker(order_ids=[1082167001])

    result = _service(repo, broker).submit_order(_buy_request("remark_buy"))

    assert result.success is True
    assert result.qmt_order_id == "1082167001"
    assert result.broker_called is True
    assert broker.place_order_payloads == [
        {
            "stock_code": "300604.SZ",
            "order_type": 23,
            "order_volume": 1000,
            "price_type": 5,
            "price": 10.0,
            "strategy_name": "poc_strategy_a",
            "order_remark": "remark_buy",
        }
    ]
    intent = repo.get_order_intent_by_remark(ACCOUNT_ID, "remark_buy")
    assert intent is not None
    assert intent.submit_status == IntentSubmitStatus.ACCEPTED
    account = repo.get_virtual_account("strat_a")
    assert account.cash == Decimal("9990000")
    assert account.frozen_cash == Decimal("10000")
    assert repo.list_cash_entries("strat_a")[0].entry_type.value == "FREEZE_BUY"


def test_submit_batch_reports_partial_success_without_auto_cancel() -> None:
    repo = _repo()
    broker = FakeManagedBroker(order_ids=[1082167001, 0])
    service = _service(repo, broker)

    result = service.submit_batch([_buy_request("remark_a"), _buy_request("remark_b")])

    assert result.success is False
    assert result.batch_status == OrderBatchStatus.PARTIAL.value
    assert result.preflight_passed is True
    assert result.total == 2
    assert result.succeeded == 1
    assert result.failed == 1
    assert result.compensation_required is True
    assert result.compensation_actions == (
        {
            "action": "MANAGED_CANCEL",
            "endpoint": "/api/v1/qmt/virtual-strategies/orders/cancel",
            "intent_id": result.results[0].intent_id,
            "qmt_order_id": "1082167001",
            "reason": "broker accepted this item before another batch item failed",
        },
    )
    assert "managed cancel" in result.compensation_hint
    assert broker.cancelled == []
    assert len(broker.place_order_payloads) == 2
    assert repo.get_order_intent_by_remark(ACCOUNT_ID, "remark_b").submit_status == IntentSubmitStatus.REJECTED
    batch = repo.get_order_batch(result.batch_id)
    assert batch is not None
    assert batch.batch_status == OrderBatchStatus.PARTIAL


def test_submit_batch_full_preflight_failure_does_not_call_broker() -> None:
    repo = _repo()
    broker = FakeManagedBroker(order_ids=[1082167001, 1082167002])

    result = _service(repo, broker).submit_batch([_buy_request("remark_dup"), _buy_request("remark_dup")])

    assert result.success is False
    assert result.batch_status == OrderBatchStatus.PREFLIGHT_FAILED.value
    assert result.preflight_passed is False
    assert result.succeeded == 0
    assert result.failed == 2
    assert broker.place_order_payloads == []
    assert repo.get_order_intent_by_remark(ACCOUNT_ID, "remark_dup") is None
    assert {error.code for item in result.results for error in item.preflight.errors} == {"BATCH_DUPLICATE_ORDER_REMARK"}


def test_submit_batch_aggregates_cash_before_broker_call() -> None:
    repo = _repo()
    account = repo.get_virtual_account("strat_a")
    repo.update_virtual_account(replace(account, cash=Decimal("15000")))
    broker = FakeManagedBroker(order_ids=[1082167001, 1082167002])

    result = _service(repo, broker).submit_batch([_buy_request("remark_cash_a"), _buy_request("remark_cash_b")])

    assert result.success is False
    assert result.batch_status == OrderBatchStatus.PREFLIGHT_FAILED.value
    assert broker.place_order_payloads == []
    assert "BATCH_INSUFFICIENT_CASH" in {error.code for item in result.results for error in item.preflight.errors}


def test_submit_batch_aggregates_same_symbol_sell_and_broker_can_sell() -> None:
    repo = _repo()
    _add_sellable_lot(repo, 1000)
    broker = FakeManagedBroker(order_ids=[1082167001, 1082167002])

    result = _service(repo, broker).submit_batch([_sell_request("remark_sell_a", 600), _sell_request("remark_sell_b", 600)])

    assert result.success is False
    assert result.batch_status == OrderBatchStatus.PREFLIGHT_FAILED.value
    assert broker.place_order_payloads == []
    codes = {error.code for item in result.results for error in item.preflight.errors}
    assert {"BATCH_INSUFFICIENT_STRATEGY_AVAILABLE_LOT", "BATCH_INSUFFICIENT_BROKER_CAN_SELL"} <= codes


def test_submit_batch_retry_is_idempotent_and_does_not_call_broker_again() -> None:
    repo = _repo()
    first_broker = FakeManagedBroker(order_ids=[1082167001, 1082167002])
    requests = [_buy_request("remark_retry_a"), _buy_request("remark_retry_b")]

    first = _service(repo, first_broker).submit_batch(requests)
    second_broker = FakeManagedBroker(order_ids=[1082167999, 1082168000])
    second = _service(repo, second_broker).submit_batch(requests)

    assert first.success is True
    assert first.batch_status == OrderBatchStatus.SUCCEEDED.value
    assert second.success is True
    assert second.retry_of_batch_id == first.batch_id
    assert second.batch_id == first.batch_id
    assert second.succeeded == 2
    assert second_broker.place_order_payloads == []
    assert len(repo.list_order_intents_by_batch(first.batch_id)) == 2


def test_submit_batch_preflight_failure_keeps_broker_called_false_for_restart_retry() -> None:
    repo = _repo()
    broker = FakeManagedBroker(order_ids=[1082167001, 1082167002])
    requests = [_buy_request("remark_dup_preflight"), _buy_request("remark_dup_preflight")]

    first = _service(repo, broker).submit_batch(requests)
    second_broker = FakeManagedBroker(order_ids=[1082167999, 1082168000])
    second = _service(repo, second_broker).submit_batch(requests)

    assert first.batch_status == OrderBatchStatus.PREFLIGHT_FAILED.value
    assert all(result.broker_called is False for result in first.results)
    assert second.retry_of_batch_id == first.batch_id
    assert second.batch_status == OrderBatchStatus.PREFLIGHT_FAILED.value
    assert all(result.broker_called is False for result in second.results)
    assert second_broker.place_order_payloads == []


def test_cancel_order_calls_fake_broker_and_releases_frozen_cash() -> None:
    repo = _repo()
    broker = FakeManagedBroker(order_ids=[1082167001])
    service = _service(repo, broker)
    submit = service.submit_order(_buy_request("remark_cancel"))

    cancel = service.cancel_order(
        ManagedCancelRequest(
            account_id=ACCOUNT_ID,
            strategy_name="poc_strategy_a",
            order_remark="remark_cancel",
            qmt_order_id=submit.qmt_order_id,
            trade_date=TRADE_DATE,
            mode="SIM",
        )
    )

    assert cancel.success is True
    assert broker.cancelled == ["1082167001"]
    account = repo.get_virtual_account("strat_a")
    assert account.cash == Decimal("10000000")
    assert account.frozen_cash == Decimal("0")
    assert [entry.entry_type.value for entry in repo.list_cash_entries("strat_a")] == ["FREEZE_BUY", "UNFREEZE_CANCEL"]
