from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.services.qmt_strategy_ledger.models import BUY_ORDER_TYPE, IntentSubmitStatus, VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.order_service import ManagedCancelRequest, ManagedOrderRequest, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


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


def test_submit_order_creates_intent_freezes_cash_and_calls_fake_broker() -> None:
    repo = _repo()
    broker = FakeManagedBroker(order_ids=[1082167001])

    result = QmtManagedOrderService(repository=repo, broker=broker).submit_order(_buy_request("remark_buy"))

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
    service = QmtManagedOrderService(repository=repo, broker=broker)

    result = service.submit_batch([_buy_request("remark_a"), _buy_request("remark_b")])

    assert result.success is False
    assert result.total == 2
    assert result.succeeded == 1
    assert result.failed == 1
    assert result.compensation_required is True
    assert "no automatic cancel" in result.compensation_hint
    assert broker.cancelled == []
    assert len(broker.place_order_payloads) == 2
    assert repo.get_order_intent_by_remark(ACCOUNT_ID, "remark_b").submit_status == IntentSubmitStatus.REJECTED


def test_cancel_order_calls_fake_broker_and_releases_frozen_cash() -> None:
    repo = _repo()
    broker = FakeManagedBroker(order_ids=[1082167001])
    service = QmtManagedOrderService(repository=repo, broker=broker)
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
