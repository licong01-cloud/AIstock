from __future__ import annotations

import re
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    IntentSubmitStatus,
    MiniQmtAccountGroup,
    MiniQmtStrategySlot,
    OrderIntentRecord,
    PositionLotRecord,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.order_service import ManagedOrderRequest, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)
NEXT_TRADE_DATE = date(2026, 5, 19)
WEEKEND_DATE = date(2026, 5, 23)
MONDAY_TRADE_DATE = date(2026, 5, 25)
CALENDAR = StaticTradingCalendarProvider([TRADE_DATE, NEXT_TRADE_DATE, MONDAY_TRADE_DATE])
REPO_ROOT = Path(__file__).resolve().parents[3]


class CountingBroker:
    def __init__(self, positions: list[dict] | None = None) -> None:
        self.positions = positions or []
        self.place_order_calls = 0
        self.cancel_order_calls = 0

    def get_positions(self) -> list[dict]:
        return self.positions

    def place_order(self, **kwargs):
        self.place_order_calls += 1
        return 108, "accepted"

    def cancel_order(self, order_id: str):
        self.cancel_order_calls += 1
        return True, "cancelled"


def _repo(
    cash: Decimal = Decimal("10000000"),
    available_lot: int = 0,
    *,
    open_date: date = TRADE_DATE,
    stored_available_quantity: int | None = None,
) -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            display_name="POC Strategy A",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=cash,
            status=VirtualAccountStatus.ENABLED,
        )
    )
    if available_lot:
        stored_available = available_lot if stored_available_quantity is None else stored_available_quantity
        repo.create_position_lot(
            PositionLotRecord(
                lot_id="lot_a",
                strategy_id="strat_a",
                symbol="300604.SZ",
                open_trade_id="trade_a",
                open_date=open_date,
                quantity=available_lot,
                available_quantity=stored_available,
                remaining_quantity=available_lot,
                avg_cost=Decimal("10"),
                cost_amount=Decimal(available_lot * 10),
                account_id=ACCOUNT_ID,
            )
        )
    return repo


def _service(repo: InMemoryQmtStrategyLedgerRepository, broker: CountingBroker | None = None) -> QmtManagedOrderService:
    return QmtManagedOrderService(repository=repo, broker=broker or CountingBroker(), calendar_provider=CALENDAR)


def _buy_request(**overrides) -> ManagedOrderRequest:
    data = {
        "account_id": ACCOUNT_ID,
        "strategy_name": "poc_strategy_a",
        "symbol": "300604.SZ",
        "side": "BUY",
        "order_type": BUY_ORDER_TYPE,
        "quantity": 1000,
        "price_type": 5,
        "price": Decimal("10"),
        "order_remark": "remark_buy",
        "trade_date": TRADE_DATE,
        "mode": "SIM",
    }
    data.update(overrides)
    return ManagedOrderRequest(**data)


def _sell_request(**overrides) -> ManagedOrderRequest:
    data = {
        "account_id": ACCOUNT_ID,
        "strategy_name": "poc_strategy_a",
        "symbol": "300604.SZ",
        "side": "SELL",
        "order_type": SELL_ORDER_TYPE,
        "quantity": 1000,
        "price_type": 5,
        "price": Decimal("10"),
        "order_remark": "remark_sell",
        "trade_date": TRADE_DATE,
        "mode": "SIM",
    }
    data.update(overrides)
    return ManagedOrderRequest(**data)


def test_preview_rejects_blank_strategy_without_broker_call() -> None:
    broker = CountingBroker()
    result = _service(_repo(), broker).preview_order(_buy_request(strategy_name=" "))

    assert result.allowed is False
    assert [error.code for error in result.errors] == ["BLANK_STRATEGY_NAME"]
    assert broker.place_order_calls == 0


def test_preview_rejects_duplicate_remark_without_broker_call() -> None:
    repo = _repo()
    repo.create_order_intent(
        OrderIntentRecord(
            intent_id="intent_existing",
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            symbol="300604.SZ",
            side="BUY",
            order_type=BUY_ORDER_TYPE,
            quantity=1000,
            price_type=5,
            order_remark="remark_buy",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
        )
    )
    broker = CountingBroker()

    result = _service(repo, broker).preview_order(_buy_request())

    assert result.allowed is False
    assert "DUPLICATE_ORDER_REMARK" in {error.code for error in result.errors}
    assert broker.place_order_calls == 0


def test_preview_rejects_insufficient_cash_and_buy_board_lot() -> None:
    result = _service(_repo(cash=Decimal("100"))).preview_order(_buy_request(quantity=101))

    assert result.allowed is False
    assert {"BUY_BOARD_LOT", "INSUFFICIENT_CASH"} <= {error.code for error in result.errors}


def test_preview_accepts_star_market_buy_quantity_after_minimum() -> None:
    service = _service(_repo())

    assert service.preview_order(_buy_request(symbol="688379.SH", quantity=201)).allowed is True
    assert service.preview_order(_buy_request(symbol="689009.SH", quantity=2706)).allowed is True


def test_preview_response_exposes_single_primary_error_for_operator_action() -> None:
    result = _service(_repo(cash=Decimal("100"))).preview_order(_buy_request(quantity=101))

    payload = result.to_dict()

    assert result.primary_error is result.errors[0]
    assert payload["primary_error_code"] == "BUY_BOARD_LOT"
    assert payload["primary_error"] == payload["errors"][0]
    assert {"BUY_BOARD_LOT", "INSUFFICIENT_CASH"} <= {error["code"] for error in payload["errors"]}


def test_preview_rejects_star_market_buy_quantity_below_minimum() -> None:
    result = _service(_repo()).preview_order(_buy_request(symbol="688379.SH", quantity=199))

    assert result.allowed is False
    [error] = result.errors
    assert error.code == "BUY_BOARD_LOT"
    assert error.context["symbol"] == "688379.SH"
    assert error.context["min_quantity"] == 200
    assert error.context["increment"] == 1
    assert error.context["canonical_quantity"] == 0


def test_preview_rejects_main_board_and_chinext_non_100_share_buys() -> None:
    service = _service(_repo())

    for symbol in ("600000.SH", "000001.SZ", "300604.SZ"):
        result = service.preview_order(_buy_request(symbol=symbol, quantity=101))

        assert result.allowed is False
        [error] = result.errors
        assert error.code == "BUY_BOARD_LOT"
        assert error.context["symbol"] == symbol
        assert error.context["min_quantity"] == 100
        assert error.context["increment"] == 100
        assert error.context["canonical_quantity"] == 100


def test_miniqmt_preflight_does_not_reintroduce_hard_coded_100_share_lot_gate() -> None:
    scanned_files = [
        REPO_ROOT / "backend/services/qmt_strategy_ledger/order_service.py",
        REPO_ROOT / "backend/services/qmt_strategy_ledger/selection_order_builder.py",
        REPO_ROOT / "backend/routers/qmt_strategy_ledger.py",
        REPO_ROOT / "backend/routers/qmt.py",
    ]
    forbidden_patterns = [
        re.compile(r"\b(?:quantity|order_volume|qty)\s*%\s*100\b"),
        re.compile(r"\b100\s*%\s*(?:quantity|order_volume|qty)\b"),
    ]

    violations: list[str] = []
    for path in scanned_files:
        text = path.read_text(encoding="utf-8")
        for pattern in forbidden_patterns:
            if pattern.search(text):
                violations.append(f"{path.relative_to(REPO_ROOT)} matches {pattern.pattern}")

    assert violations == []


def test_preview_accepts_sell_residuals_allowed_by_canonical_board_lot() -> None:
    repo = _repo()
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_star_residual",
            strategy_id="strat_a",
            symbol="688379.SH",
            open_trade_id="trade_star_residual",
            open_date=TRADE_DATE,
            quantity=199,
            available_quantity=199,
            remaining_quantity=199,
            avg_cost=Decimal("10"),
            cost_amount=Decimal("1990"),
            account_id=ACCOUNT_ID,
        )
    )
    broker = CountingBroker(positions=[{"stock_code": "688379.SH", "quantity": 199, "can_sell": 199}])

    result = _service(repo, broker).submit_order(_sell_request(symbol="688379.SH", quantity=199))

    assert result.success is True
    assert result.preflight.allowed is True
    assert result.preflight.errors == ()
    assert broker.place_order_calls == 1


def test_preview_rejects_t1_strategy_lot_shortage() -> None:
    result = _service(_repo(available_lot=0)).preview_order(_sell_request(quantity=1000))

    assert result.allowed is False
    assert [error.code for error in result.errors] == ["INSUFFICIENT_STRATEGY_AVAILABLE_LOT"]


def test_preview_derives_prior_day_lot_sellable_on_next_trading_day() -> None:
    result = _service(_repo(available_lot=1000, stored_available_quantity=0)).preview_order(
        _sell_request(trade_date=NEXT_TRADE_DATE)
    )

    assert result.allowed is True
    assert result.strategy_available_sell_quantity == 1000


def test_preview_keeps_same_day_and_non_trading_day_lot_locked() -> None:
    service = _service(_repo(available_lot=1000, stored_available_quantity=0))

    same_day = service.preview_order(_sell_request(trade_date=TRADE_DATE, order_remark="same_day_sell"))
    weekend = service.preview_order(_sell_request(trade_date=WEEKEND_DATE, order_remark="weekend_sell"))

    assert same_day.allowed is False
    assert weekend.allowed is False
    assert same_day.strategy_available_sell_quantity == 0
    assert weekend.strategy_available_sell_quantity == 0


def test_preview_reserves_pending_sell_intents_without_changing_lot_quantity() -> None:
    repo = _repo(available_lot=1000, stored_available_quantity=0)
    repo.create_order_intent(
        OrderIntentRecord(
            intent_id="intent_pending_sell",
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            symbol="300604.SZ",
            side="SELL",
            order_type=SELL_ORDER_TYPE,
            quantity=400,
            price_type=5,
            order_remark="pending_sell",
            account_id=ACCOUNT_ID,
            trade_date=NEXT_TRADE_DATE,
            submit_status=IntentSubmitStatus.ACCEPTED,
        )
    )

    result = _service(repo).preview_order(
        _sell_request(trade_date=NEXT_TRADE_DATE, quantity=700, order_remark="next_sell")
    )

    assert result.allowed is False
    assert result.strategy_available_sell_quantity == 600
    assert result.pending_sell_quantity == 400
    assert repo.list_position_lots("strat_a", "300604.SZ")[0].remaining_quantity == 1000


def test_submit_rejects_broker_can_sell_shortage_before_order_call() -> None:
    broker = CountingBroker(positions=[{"stock_code": "300604.SZ", "quantity": 1000, "can_sell": 0}])

    result = _service(_repo(available_lot=1000), broker).submit_order(_sell_request())

    assert result.success is False
    assert result.broker_called is False
    assert result.preflight.broker_can_sell == 0
    assert [error.code for error in result.preflight.errors] == ["INSUFFICIENT_BROKER_CAN_SELL"]
    assert broker.place_order_calls == 0


def test_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_account_group_slots(
        MiniQmtAccountGroup(
            account_group_id="ag_minqmt_62266303_sim",
            broker_account_id=ACCOUNT_ID,
            cash_limit=Decimal("15000"),
            slots=(
                MiniQmtStrategySlot(
                    account_group_id="ag_minqmt_62266303_sim",
                    strategy_slot_id="slot_a",
                    strategy_id="strat_a",
                    strategy_name="poc_strategy_a",
                    display_name="POC Strategy A",
                    account_id=ACCOUNT_ID,
                    allocated_cash=Decimal("7500"),
                    order_remark_prefix="ag622-a",
                ),
                MiniQmtStrategySlot(
                    account_group_id="ag_minqmt_62266303_sim",
                    strategy_slot_id="slot_b",
                    strategy_id="strat_b",
                    strategy_name="poc_strategy_b",
                    display_name="POC Strategy B",
                    account_id=ACCOUNT_ID,
                    allocated_cash=Decimal("7500"),
                    order_remark_prefix="ag622-b",
                ),
            ),
        )
    )
    for strategy_id in ("strat_a", "strat_b"):
        account = repo.get_virtual_account(strategy_id)
        repo.update_virtual_account(replace(account, cash=Decimal("10000")))
    broker = CountingBroker()

    result = _service(repo, broker).submit_batch(
        [
            _buy_request(strategy_name="poc_strategy_a", order_remark="ag622-a-1", quantity=1000, price=Decimal("10")),
            _buy_request(strategy_name="poc_strategy_b", order_remark="ag622-b-1", quantity=1000, price=Decimal("10")),
        ]
    )

    assert result.preflight_passed is False
    assert broker.place_order_calls == 0
    assert "BATCH_INSUFFICIENT_ACCOUNT_GROUP_CASH" in {
        error.code for item in result.results for error in item.preflight.errors
    }
