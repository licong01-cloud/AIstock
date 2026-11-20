from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.services.qmt_strategy_ledger.models import (
    MiniQmtAccountGroup,
    MiniQmtStrategySlot,
    PositionLotRecord,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"
ACCOUNT_GROUP_ID = "ag_minqmt_62266303_sim"
TRADE_DATE = date(2026, 5, 18)


def _slot(
    suffix: str,
    *,
    cash: str = "50000",
    strategy_name: str | None = None,
    order_remark_prefix: str | None = None,
    legacy_portfolio_id: str | None = None,
) -> MiniQmtStrategySlot:
    return MiniQmtStrategySlot(
        account_group_id=ACCOUNT_GROUP_ID,
        strategy_slot_id=f"slot_{suffix}",
        strategy_id=f"strat_{suffix}",
        strategy_name=strategy_name or f"Strategy{suffix.upper()}",
        display_name=f"Strategy {suffix.upper()}",
        account_id=ACCOUNT_ID,
        allocated_cash=Decimal(cash),
        order_remark_prefix=order_remark_prefix or f"ag622-{suffix}",
        package_id=f"pkg_{suffix}",
        release_id=f"release_{suffix}",
        binding_id=f"binding_{suffix}",
        legacy_portfolio_id=legacy_portfolio_id,
    )


def _group(*slots: MiniQmtStrategySlot, cash_limit: str = "200000") -> MiniQmtAccountGroup:
    return MiniQmtAccountGroup(
        account_group_id=ACCOUNT_GROUP_ID,
        broker_account_id=ACCOUNT_ID,
        cash_limit=Decimal(cash_limit),
        slots=tuple(slots),
    )


def test_account_group_slots_create_n1_and_n2_under_same_minqmt_account() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()

    n1_group = repo.create_account_group_slots(_group(_slot("single"), cash_limit="100000"))
    n1_slots = repo.list_account_group_slots(ACCOUNT_GROUP_ID, broker_account_id=ACCOUNT_ID)

    assert n1_group.allocated_cash_total == Decimal("50000")
    assert [(slot.account_id, slot.strategy_slot_id) for slot in n1_slots] == [(ACCOUNT_ID, "slot_single")]

    repo = InMemoryQmtStrategyLedgerRepository()
    n2_group = repo.create_account_group_slots(_group(_slot("a"), _slot("b", cash="75000"), cash_limit="150000"))
    n2_slots = repo.list_account_group_slots(ACCOUNT_GROUP_ID, broker_account_id=ACCOUNT_ID)

    assert n2_group.allocated_cash_total == Decimal("125000")
    assert {slot.strategy_slot_id for slot in n2_slots} == {"slot_a", "slot_b"}
    assert {account.account_id for account in repo.list_virtual_accounts(ACCOUNT_ID)} == {ACCOUNT_ID}


def test_account_group_slot_count_is_governed_by_cash_not_fixed_package_gate() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    slots = tuple(_slot(f"s{index:02d}", cash="100") for index in range(80))

    group = repo.create_account_group_slots(_group(*slots, cash_limit="8000"))

    assert len(group.slots) == 80
    assert group.allocated_cash_total == Decimal("8000")
    assert len(repo.list_account_group_slots(ACCOUNT_GROUP_ID, broker_account_id=ACCOUNT_ID)) == 80


@pytest.mark.parametrize(
    ("slots", "cash_limit", "message"),
    [
        ((_slot("a"), _slot("b", strategy_name="StrategyA")), "200000", "strategy_name"),
        ((_slot("a"), _slot("b", order_remark_prefix="ag622-a")), "200000", "order_remark_prefix"),
        ((_slot("dup"), _slot("dup", strategy_name="StrategyDupB", order_remark_prefix="ag622-dupb")), "200000", "strategy_slot_id"),
        ((_slot("a", cash="120000"), _slot("b", cash="120000")), "200000", "allocated_cash_total"),
    ],
)
def test_account_group_slot_preflight_rejects_duplicate_or_over_allocated_group(
    slots: tuple[MiniQmtStrategySlot, MiniQmtStrategySlot],
    cash_limit: str,
    message: str,
) -> None:
    repo = InMemoryQmtStrategyLedgerRepository()

    with pytest.raises(ValueError, match=message):
        repo.create_account_group_slots(_group(*slots, cash_limit=cash_limit))


def test_legacy_exclusive_mapping_remains_readable_and_disableable() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_account_group_slots(_group(_slot("legacy", legacy_portfolio_id="paper_legacy_001")))

    [slot] = repo.list_account_group_slots(ACCOUNT_GROUP_ID)
    assert slot.legacy_portfolio_id == "paper_legacy_001"
    assert slot.status == VirtualAccountStatus.ENABLED

    disabled = repo.set_account_group_slot_status(
        account_group_id=ACCOUNT_GROUP_ID,
        strategy_slot_id="slot_legacy",
        status=VirtualAccountStatus.DISABLED,
    )

    assert disabled.legacy_portfolio_id == "paper_legacy_001"
    assert disabled.status == VirtualAccountStatus.DISABLED
    assert repo.get_virtual_account("strat_legacy").status == VirtualAccountStatus.DISABLED


def test_broker_raw_positions_reconcile_against_strategy_virtual_lots_in_account_group() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_account_group_slots(_group(_slot("a"), _slot("b")))
    for strategy_id, quantity in [("strat_a", 7600), ("strat_b", 6600)]:
        repo.create_position_lot(
            PositionLotRecord(
                lot_id=f"lot_{strategy_id}",
                strategy_id=strategy_id,
                symbol="001358.SZ",
                open_trade_id=f"trade_{strategy_id}",
                open_date=TRADE_DATE,
                quantity=quantity,
                available_quantity=0,
                remaining_quantity=quantity,
                avg_cost=Decimal("29.88"),
                cost_amount=Decimal(quantity) * Decimal("29.88"),
                account_id=ACCOUNT_ID,
            )
        )

    report = QmtStrategyLedgerReconciliationService(repository=repo).reconcile_snapshot(
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        broker_positions=[{"stock_code": "001358.SZ", "quantity": 14200}],
    )

    assert report.run.status == "SUCCEEDED"
    assert report.issues == ()
    assert report.strategy_lot_quantities["StrategyA"]["001358.SZ"] == 7600
    assert report.strategy_lot_quantities["StrategyB"]["001358.SZ"] == 6600
