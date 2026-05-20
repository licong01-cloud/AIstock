from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    BindingStatus,
    CashEntryType,
    CashLedgerEntry,
    IntentSubmitStatus,
    OrderIntentRecord,
    PositionLotRecord,
    StrategyBindingSelectionEvidence,
    StrategyPackageBinding,
    TradeLedgerRecord,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


def _account(strategy_id: str = "strat_a", strategy_name: str = "poc_strategy_a") -> VirtualAccount:
    return VirtualAccount(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        display_name=f"{strategy_name} display",
        account_id=ACCOUNT_ID,
        mode="SIM",
        initial_cash=Decimal("10000000"),
        cash=Decimal("10000000"),
        status=VirtualAccountStatus.ENABLED,
    )


def _intent(intent_id: str = "intent_a", order_remark: str = "remark_a") -> OrderIntentRecord:
    return OrderIntentRecord(
        intent_id=intent_id,
        strategy_id="strat_a",
        strategy_name="poc_strategy_a",
        symbol="300604.SZ",
        side="BUY",
        order_type=BUY_ORDER_TYPE,
        quantity=1000,
        price_type=5,
        order_remark=order_remark,
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        package_id="pkg_demo",
        estimated_notional=Decimal("100000"),
    )


def test_in_memory_repository_creates_lists_and_gets_virtual_accounts() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    account = repo.create_virtual_account(_account())

    assert repo.get_virtual_account("strat_a") == account
    assert repo.list_virtual_accounts(account_id=ACCOUNT_ID) == [account]

    with pytest.raises(ValueError, match="strategy_name already exists"):
        repo.create_virtual_account(_account(strategy_id="strat_duplicate"))

    with pytest.raises(ValueError, match="strategy_name must be non-empty"):
        repo.create_virtual_account(_account(strategy_id="blank", strategy_name=" "))


def test_in_memory_repository_enforces_one_active_binding_per_strategy() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(_account())

    active = StrategyPackageBinding(
        binding_id="bind_a",
        strategy_id="strat_a",
        package_id="pkg_a",
        manifest_sha256="sha_a",
        binding_status=BindingStatus.ACTIVE,
    )
    repo.create_package_binding(active)

    assert repo.get_active_package_binding("strat_a") == active

    with pytest.raises(ValueError, match="active package binding already exists"):
        repo.create_package_binding(
            StrategyPackageBinding(
                binding_id="bind_b",
                strategy_id="strat_a",
                package_id="pkg_b",
                manifest_sha256="sha_b",
                binding_status=BindingStatus.ACTIVE,
            )
        )


def test_in_memory_repository_replaces_active_binding_and_keeps_history() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(_account())
    active = repo.create_package_binding(
        StrategyPackageBinding(
            binding_id="bind_a",
            strategy_id="strat_a",
            package_id="pkg_a",
            manifest_sha256="sha_a",
            selection_run_id="sel_a",
            trade_date=TRADE_DATE,
            binding_status=BindingStatus.ACTIVE,
        )
    )
    replacement = StrategyPackageBinding(
        binding_id="bind_b",
        strategy_id="strat_a",
        package_id="pkg_a",
        manifest_sha256="sha_a",
        selection_run_id="sel_b",
        trade_date=date(2026, 5, 19),
        binding_status=BindingStatus.ACTIVE,
    )

    repo.replace_active_package_binding(replacement, replaced_binding_id=active.binding_id, reason="next_day")

    retired = repo.get_package_binding("bind_a")
    assert retired.binding_status == BindingStatus.RETIRED
    assert retired.runtime_config["binding_lifecycle"]["replaced_by_binding_id"] == "bind_b"
    assert repo.get_active_package_binding("strat_a") == replacement
    assert [binding.binding_id for binding in repo.list_package_bindings("strat_a")] == ["bind_a", "bind_b"]


def test_in_memory_repository_records_daily_selection_evidence_idempotently() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(_account())
    binding = repo.create_package_binding(
        StrategyPackageBinding(
            binding_id="bind_a",
            strategy_id="strat_a",
            package_id="pkg_a",
            manifest_sha256="sha_a",
            binding_status=BindingStatus.ACTIVE,
        )
    )
    evidence = StrategyBindingSelectionEvidence(
        evidence_id="ev_a",
        binding_id=binding.binding_id,
        strategy_id="strat_a",
        package_id="pkg_a",
        selection_run_id="sel_a",
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        manifest_sha256="sha_a",
        runtime_config_hash="runtime_hash",
    )

    first = repo.record_binding_selection_evidence(evidence)
    second = repo.record_binding_selection_evidence(
        StrategyBindingSelectionEvidence(**{**evidence.__dict__, "evidence_id": "ev_duplicate"})
    )

    assert first == evidence
    assert second == evidence
    assert repo.get_binding_selection_evidence(binding.binding_id, TRADE_DATE) == evidence
    assert repo.list_binding_selection_evidence(binding.binding_id) == [evidence]


def test_in_memory_repository_enforces_unique_order_remark_per_account() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(_account())
    first = repo.create_order_intent(_intent())

    assert repo.get_order_intent("intent_a") == first
    assert repo.get_order_intent_by_remark(ACCOUNT_ID, "remark_a") == first

    with pytest.raises(ValueError, match="order_remark already exists"):
        repo.create_order_intent(_intent(intent_id="intent_b", order_remark="remark_a"))

    with pytest.raises(ValueError, match="order_remark must be non-empty"):
        repo.create_order_intent(_intent(intent_id="intent_blank", order_remark=" "))


def test_in_memory_repository_upserts_trade_ledger_idempotently() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    trade = TradeLedgerRecord(
        trade_id="trade_a",
        intent_id="intent_a",
        strategy_id="strat_a",
        qmt_order_id="order_a",
        symbol="300604.SZ",
        side="BUY",
        price=Decimal("10.25"),
        quantity=1000,
        amount=Decimal("10250"),
        trade_date=TRADE_DATE,
        account_id=ACCOUNT_ID,
    )

    first, first_inserted = repo.upsert_trade_ledger(trade)
    second, second_inserted = repo.upsert_trade_ledger(trade)

    assert first == trade
    assert first_inserted is True
    assert second == trade
    assert second_inserted is False


def test_in_memory_repository_keeps_cash_entries_append_only_and_lots_filterable() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(_account())
    repo.create_virtual_account(_account(strategy_id="strat_b", strategy_name="poc_strategy_b"))

    entry_a = CashLedgerEntry(
        cash_id="cash_a",
        strategy_id="strat_a",
        entry_type=CashEntryType.INITIAL_ALLOCATE,
        cash_delta=Decimal("10000000"),
        cash_after=Decimal("10000000"),
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
    )
    entry_b = CashLedgerEntry(
        cash_id="cash_b",
        strategy_id="strat_a",
        entry_type=CashEntryType.FREEZE_BUY,
        cash_delta=Decimal("-100000"),
        cash_after=Decimal("9900000"),
        frozen_delta=Decimal("100000"),
        frozen_after=Decimal("100000"),
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        symbol="300604.SZ",
    )
    repo.append_cash_entry(entry_a)
    repo.append_cash_entry(entry_b)

    lot_a = PositionLotRecord(
        lot_id="lot_a",
        strategy_id="strat_a",
        symbol="300604.SZ",
        open_trade_id="trade_a",
        open_date=TRADE_DATE,
        quantity=1000,
        available_quantity=0,
        remaining_quantity=1000,
        avg_cost=Decimal("10.25"),
        cost_amount=Decimal("10250"),
        account_id=ACCOUNT_ID,
    )
    lot_b = PositionLotRecord(
        lot_id="lot_b",
        strategy_id="strat_b",
        symbol="300604.SZ",
        open_trade_id="trade_b",
        open_date=TRADE_DATE,
        quantity=500,
        available_quantity=0,
        remaining_quantity=500,
        avg_cost=Decimal("10.30"),
        cost_amount=Decimal("5150"),
        account_id=ACCOUNT_ID,
    )
    repo.create_position_lot(lot_a)
    repo.create_position_lot(lot_b)

    assert repo.list_cash_entries("strat_a") == [entry_a, entry_b]
    assert repo.list_position_lots("strat_a", symbol="300604.SZ") == [lot_a]

    with pytest.raises(ValueError, match="cash ledger entry already exists"):
        repo.append_cash_entry(entry_a)

    updated_lot = repo.update_position_lot(
        PositionLotRecord(
            **{
                **lot_a.__dict__,
                "available_quantity": 800,
            }
        )
    )
    assert updated_lot.available_quantity == 800
    assert repo.list_position_lots("strat_a", symbol="300604.SZ")[0].available_quantity == 800


def test_in_memory_repository_lists_open_sell_intents_by_strategy_symbol_and_date() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(_account("strat_a", "poc_strategy_a"))
    repo.create_virtual_account(_account("strat_b", "poc_strategy_b"))

    def sell_intent(
        intent_id: str,
        strategy_id: str,
        symbol: str,
        trade_date: date,
        status: IntentSubmitStatus,
    ) -> OrderIntentRecord:
        return OrderIntentRecord(
            intent_id=intent_id,
            strategy_id=strategy_id,
            strategy_name="poc_strategy_a" if strategy_id == "strat_a" else "poc_strategy_b",
            symbol=symbol,
            side="SELL",
            order_type=SELL_ORDER_TYPE,
            quantity=100,
            price_type=5,
            order_remark=intent_id,
            account_id=ACCOUNT_ID,
            trade_date=trade_date,
            submit_status=status,
        )

    repo.create_order_intent(sell_intent("open_a", "strat_a", "300604.SZ", TRADE_DATE, IntentSubmitStatus.ACCEPTED))
    repo.create_order_intent(sell_intent("closed_a", "strat_a", "300604.SZ", TRADE_DATE, IntentSubmitStatus.CANCELLED))
    repo.create_order_intent(sell_intent("other_symbol", "strat_a", "300054.SZ", TRADE_DATE, IntentSubmitStatus.CREATED))
    repo.create_order_intent(sell_intent("other_strategy", "strat_b", "300604.SZ", TRADE_DATE, IntentSubmitStatus.ACCEPTED))

    pending = repo.list_open_sell_intents("strat_a", symbol="300604.SZ", trade_date=TRADE_DATE)

    assert [item.intent_id for item in pending] == ["open_a"]
