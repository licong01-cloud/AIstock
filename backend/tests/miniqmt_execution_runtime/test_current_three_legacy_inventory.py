from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import (
    CurrentThreeDependentBuyCompletenessV1,
    CurrentThreeFailureV1,
    CurrentThreeInventoryDispositionV1,
    CurrentThreeLegacyInventorySetV1,
    CurrentThreeLegacyStateInventoryV1,
)
from backend.services.miniqmt_execution_runtime.kernel_current_three_inventory import (
    build_current_three_legacy_inventory_set_v1,
)
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTAlgoInstanceStatus,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
)
from backend.services.miniqmt_execution_runtime.repository import InMemoryMiniQMTExecutionRuntimeRepository
from backend.services.trading_core.models import OrderSide


NOW = datetime(2026, 7, 29, 1, 30, tzinfo=UTC)


def _repo(metadata: dict) -> InMemoryMiniQMTExecutionRuntimeRepository:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            runtime_id="runtime_inventory",
            account_group_id="account_inventory",
            trade_date=date(2026, 7, 29),
            runtime_config_hash="runtime-config",
            created_at=NOW,
            updated_at=NOW,
            metadata={"repository_commit_sha": "a" * 40},
        )
    )
    repo.upsert_algo_instance(
        MiniQMTExecutionAlgoInstance(
            algo_instance_id="legacy_algo_inventory",
            runtime_id="runtime_inventory",
            parent_intent_id="parent_inventory",
            strategy_slot_id="slot_inventory",
            symbol="600000.SH",
            side=OrderSide.BUY,
            target_quantity=100,
            remaining_quantity=100,
            algo_code="SNIPER_MINIQMT",
            status=MiniQMTAlgoInstanceStatus.ACTIVE,
            created_at=NOW,
            updated_at=NOW,
            metadata=metadata,
        )
    )
    return repo


def test_inventory_is_read_only_strict_and_session_boundary_eligible() -> None:
    repo = _repo(
        {
            "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
            "legacy_state": {"status": "RUNNING", "timer_count": 0},
        }
    )
    before = repo.list_algo_instances("runtime_inventory")[0]
    read = repo.read_current_three_shadow_snapshot("runtime_inventory")

    inventory_set, dependent = build_current_three_legacy_inventory_set_v1(read)

    assert dependent == ()
    assert inventory_set.total_count == 1
    assert (
        inventory_set.ordered_inventory_items[0].disposition
        is CurrentThreeInventoryDispositionV1.SESSION_BOUNDARY_ELIGIBLE
    )
    assert inventory_set.ordered_inventory_items[0].runtime_effect_applied is False
    assert repo.list_algo_instances("runtime_inventory")[0] == before
    assert CurrentThreeLegacyInventorySetV1.model_validate_json(inventory_set.model_dump_json()) == inventory_set

    excessive = inventory_set.ordered_inventory_items[0].model_dump(mode="python")
    failure = CurrentThreeFailureV1.create(
        field_path="legacy_state",
        reason_code="MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
        context={"condition": "coverage_bound"},
    )
    excessive["ordered_failures"] = tuple(failure for _ in range(257))
    with pytest.raises(ValueError, match="bounded evidence limit"):
        CurrentThreeLegacyStateInventoryV1.model_validate(excessive)


def test_inventory_keeps_dependent_buy_coordinator_outside_algo_parity() -> None:
    repo = _repo(
        {
            "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
            "legacy_state": {"status": "RUNNING"},
            "dependent_buy": True,
            "dependent_buy_status": "DEFERRED",
            "dependent_buy_reason_code": "SELL_PROCEEDS_REQUIRED",
            "dependent_buy_strategy_id": "strategy_1",
            "dependent_buy_required_cash": "1000",
            "dependent_buy_contract": {"sell_parent_intent_ids": ["sell_parent_1"]},
            "dependent_buy_action": {"symbol": "600000.SH", "quantity": 100, "price": "10"},
        }
    )
    read = repo.read_current_three_shadow_snapshot("runtime_inventory")

    inventory_set, dependent = build_current_three_legacy_inventory_set_v1(read)

    assert len(dependent) == 1
    assert (
        dependent[0].evidence_completeness
        is CurrentThreeDependentBuyCompletenessV1.HISTORICAL_LEDGER_IDENTITY_UNAVAILABLE
    )
    assert dependent[0].ordered_failures
    item = inventory_set.ordered_inventory_items[0]
    assert item.dependent_buy_coordination_ref == dependent[0].coordination_ref_sha256
    assert item.disposition is CurrentThreeInventoryDispositionV1.SESSION_BOUNDARY_ELIGIBLE


def test_inventory_exposes_policy_alias_conflict_without_candidate_fallback() -> None:
    repo = _repo(
        {
            "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE", "unknown_policy": 1},
            "legacy_state": {"status": "RUNNING"},
        }
    )
    read = repo.read_current_three_shadow_snapshot("runtime_inventory")

    inventory_set, _ = build_current_three_legacy_inventory_set_v1(read)

    item = inventory_set.ordered_inventory_items[0]
    assert item.disposition is CurrentThreeInventoryDispositionV1.INVALID_VISIBLE
    assert item.candidate_plugin_key is None
    assert item.candidate_plugin_config_sha256 is None
    assert item.ordered_failures


@pytest.mark.parametrize(
    "metadata",
    [
        {"legacy_state": {"status": "RUNNING"}},
        {"config": "not-an-object", "legacy_state": {"status": "RUNNING"}},
        {
            "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
            "setting": {"price_mode": "DIFFERENT"},
            "legacy_state": {"status": "RUNNING"},
        },
    ],
)
def test_inventory_rejects_missing_malformed_or_conflicting_config_authority(metadata: dict) -> None:
    repo = _repo(metadata)
    item = build_current_three_legacy_inventory_set_v1(repo.read_current_three_shadow_snapshot("runtime_inventory"))[
        0
    ].ordered_inventory_items[0]
    assert item.disposition is CurrentThreeInventoryDispositionV1.INVALID_VISIBLE
    assert item.ordered_failures


def test_inventory_distinguishes_terminal_and_active_legacy_owner() -> None:
    terminal_repo = _repo({"config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}})
    terminal_algo = terminal_repo.list_algo_instances("runtime_inventory")[0].model_copy(
        update={"status": MiniQMTAlgoInstanceStatus.COMPLETED}
    )
    terminal_repo.upsert_algo_instance(terminal_algo)
    terminal_item = build_current_three_legacy_inventory_set_v1(
        terminal_repo.read_current_three_shadow_snapshot("runtime_inventory")
    )[0].ordered_inventory_items[0]
    assert terminal_item.disposition is CurrentThreeInventoryDispositionV1.TERMINAL_NO_WRITE

    active_repo = _repo(
        {
            "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
            "legacy_state": {"status": "RUNNING"},
        }
    )
    active_repo.upsert_child_order(
        MiniQMTChildOrder(
            child_order_id="child_inventory",
            runtime_id="runtime_inventory",
            algo_instance_id="legacy_algo_inventory",
            parent_intent_id="parent_inventory",
            strategy_slot_id="slot_inventory",
            symbol="600000.SH",
            side=OrderSide.BUY,
            quantity=100,
            price=10,
            status=MiniQMTChildOrderStatus.SUBMITTED,
            broker_order_id="broker_inventory",
            updated_at=NOW,
        )
    )
    active_item = build_current_three_legacy_inventory_set_v1(
        active_repo.read_current_three_shadow_snapshot("runtime_inventory")
    )[0].ordered_inventory_items[0]
    assert active_item.disposition is CurrentThreeInventoryDispositionV1.ACTIVE_LEGACY_OWNER


def test_inventory_associates_order_and_trade_refs_through_exact_child_owner() -> None:
    repo = _repo(
        {
            "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
            "legacy_state": {"status": "RUNNING"},
        }
    )
    repo.upsert_child_order(
        MiniQMTChildOrder(
            child_order_id="child_inventory",
            runtime_id="runtime_inventory",
            algo_instance_id="legacy_algo_inventory",
            parent_intent_id="parent_inventory",
            strategy_slot_id="slot_inventory",
            symbol="600000.SH",
            side=OrderSide.BUY,
            quantity=100,
            price=10,
            status=MiniQMTChildOrderStatus.FILLED,
            broker_order_id="broker_inventory",
            updated_at=NOW,
        )
    )
    repo.append_event(
        MiniQMTExecutionEvent(
            event_id="order_inventory",
            runtime_id="runtime_inventory",
            sequence=1,
            event_type=MiniQMTExecutionEventType.ORDER_EVENT,
            event_time=NOW,
            source="gateway",
            payload={
                "child_order_id": "child_inventory",
                "broker_order_id": "broker_inventory",
                "status": "FILLED",
                "quantity": 100,
                "price": 10,
            },
        )
    )
    repo.append_event(
        MiniQMTExecutionEvent(
            event_id="trade_inventory",
            runtime_id="runtime_inventory",
            sequence=2,
            event_type=MiniQMTExecutionEventType.TRADE_EVENT,
            event_time=NOW,
            source="gateway",
            payload={
                "child_order_id": "child_inventory",
                "broker_order_id": "broker_inventory",
                "trade_id": "trade_inventory_1",
                "quantity": 100,
                "price": 10,
            },
        )
    )
    item = build_current_three_legacy_inventory_set_v1(repo.read_current_three_shadow_snapshot("runtime_inventory"))[
        0
    ].ordered_inventory_items[0]
    assert [ref.identity for ref in item.ordered_order_event_refs] == ["order_inventory"]
    assert [ref.identity for ref in item.ordered_trade_event_refs] == ["trade_inventory"]


def test_dependent_buy_complete_release_and_invalid_carriers_are_distinct() -> None:
    complete_repo = _repo(
        {
            "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
            "legacy_state": {"status": "RUNNING"},
            "dependent_buy": True,
            "dependent_buy_status": "RELEASED",
            "dependent_buy_reason_code": "MINIQMT_DEPENDENT_BUY_RELEASED_AFTER_SELL_TRADE",
            "dependent_buy_strategy_id": "strategy_1",
            "dependent_buy_required_cash": "1000",
            "dependent_buy_contract": {"sell_parent_intent_ids": ["sell_parent_1"]},
            "dependent_buy_action": {"symbol": "600000.SH", "quantity": 100, "price": "10"},
            "dependent_buy_ledger_authority_source": "qmt_strategy_ledger.virtual_account.cash",
            "dependent_buy_ledger_observation_context": {"projection_id": "ledger_projection_1"},
            "dependent_buy_released_child_order_id": "released_child_1",
        }
    )
    complete = build_current_three_legacy_inventory_set_v1(
        complete_repo.read_current_three_shadow_snapshot("runtime_inventory")
    )[1][0]
    assert complete.evidence_completeness is CurrentThreeDependentBuyCompletenessV1.COMPLETE
    assert complete.ordered_failures == ()

    invalid_repo = _repo(
        {
            "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
            "legacy_state": {"status": "RUNNING"},
            "dependent_buy": True,
            "dependent_buy_status": "UNKNOWN",
            "dependent_buy_contract": "bad",
            "dependent_buy_action": "bad",
            "dependent_buy_required_cash": "not-decimal",
            "dependent_buy_released_child_order_id": "wrong_child",
        }
    )
    invalid = build_current_three_legacy_inventory_set_v1(
        invalid_repo.read_current_three_shadow_snapshot("runtime_inventory")
    )[1][0]
    assert invalid.evidence_completeness is CurrentThreeDependentBuyCompletenessV1.INVALID_VISIBLE
    assert invalid.ordered_failures


def test_inventory_builder_rejects_non_repository_read_carrier() -> None:
    with pytest.raises(TypeError):
        build_current_three_legacy_inventory_set_v1(object())  # type: ignore[arg-type]
