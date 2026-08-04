from __future__ import annotations

from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import (
    CurrentThreeParityInputV1,
    CurrentThreeParityStatusV1,
)
from backend.services.miniqmt_execution_runtime.kernel_current_three_parity import (
    associate_current_three_shadow_commands_v1,
    build_current_three_parity_input_v1,
    build_current_three_parity_receipt_v1,
    build_current_three_parity_trace_v1,
    build_current_three_parity_trace_step_v1,
    build_parity_event_ref_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import SideV1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    BrokerCommandTypeV2,
    BrokerCommandV2,
    OrderTypeV1,
)
from backend.tests.miniqmt_execution_runtime.test_current_three_shadow_source import NOW, _child, _events, _runtime
from backend.tests.miniqmt_execution_runtime.test_current_three_shadow_source import _algo as _shadow_algo
from backend.services.miniqmt_execution_runtime.repository import InMemoryMiniQMTExecutionRuntimeRepository
from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_runner import (
    build_current_three_parity_input_from_shadow_v1,
    run_current_three_committed_parity_v1,
)
import backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_runner as shadow_runner_module
from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import CurrentThreeContractError
import pytest
from backend.services.miniqmt_execution_runtime.kernel_current_three_inventory import (
    build_current_three_legacy_inventory_set_v1,
)
from backend.services.miniqmt_execution_runtime.models import MiniQMTExecutionEvent
from backend.services.miniqmt_execution_runtime.models import MiniQMTExecutionEventType


def _event_ref():
    return build_parity_event_ref_v1(
        step_ordinal=0,
        event_id="event_tick_1",
        event_type="TICK",
        event_source="gateway",
        event_payload_sha256="1" * 64,
        logical_time_utc="2026-07-29T01:30:00Z",
        market_data_projection_id="market_1",
        market_data_projection_sha256="2" * 64,
    )


def _input():
    return build_current_three_parity_input_v1(
        algo_code="SNIPER_MINIQMT",
        runtime_id="runtime_parity",
        parent_intent_id="parent_parity",
        strategy_slot_id="slot_parity",
        symbol="600000.SH",
        side=SideV1.BUY,
        target_quantity=100,
        limit_price_decimal="10",
        pricetick_decimal="0.01",
        min_volume=100,
        volume_increment=100,
        plugin_config={"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        legacy_policy_projection_receipt_sha256="3" * 64,
        ordered_event_refs=(_event_ref(),),
    )


def _trace(*, price: str):
    step = build_current_three_parity_trace_step_v1(
        step_ordinal=0,
        event_type="TICK",
        event_payload_sha256="1" * 64,
        logical_time_utc="2026-07-29T01:30:00Z",
        state_status="RUNNING",
        traded_quantity=0,
        remaining_quantity=100,
        algo_specific_state_projection={"active_order_count": 1},
        ordered_business_effects=(
            {
                "kind": "SUBMIT_LIMIT",
                "side": "BUY",
                "symbol": "600000.SH",
                "canonical_price": price,
                "quantity": 100,
                "cancel_target_ordinal": None,
                "reason_code": "sniper_ask_crossed_limit",
                "market_data_lineage_sha256": "4" * 64,
            },
        ),
        ordered_transport_duplicate_observations=(),
        ordered_timer_effects=(),
        ordered_diagnostic_reason_codes=(),
        terminal_outcome=None,
    )
    return build_current_three_parity_trace_v1(algo_code="SNIPER_MINIQMT", side=SideV1.BUY, ordered_steps=(step,))


def _committed_sniper_repo(*, order_status, include_cumulative: bool = True):
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    repo.upsert_algo_instance(
        _shadow_algo().model_copy(
            update={
                "metadata": {
                    "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
                    "legacy_state": {"status": "RUNNING"},
                    "limit_price_decimal": "10",
                    "pricetick_decimal": "0.01",
                    "min_volume": 100,
                    "volume_increment": 100,
                }
            }
        )
    )
    repo.upsert_child_order(_child())
    session = {
        "session_phase": "CONTINUOUS_AM",
        "session_epoch": "session_shadow_am",
        "exchange_trade_date": "2026-07-29",
    }
    events = (
        _events()[0].model_copy(update={"payload": {**_events()[0].payload, **session, "generation": 1}}),
        MiniQMTExecutionEvent(
            event_id="child_submitted_1",
            runtime_id="runtime_shadow",
            sequence=2,
            event_type=MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED,
            event_time=NOW,
            source="gateway",
            payload={
                "algo_instance_id": "legacy_algo_1",
                "parent_intent_id": "parent_1",
                "strategy_slot_id": "slot_1",
                "child_order_id": "legacy_child_1",
                "broker_order_id": "broker_1",
                "accepted": True,
                "broker_called": True,
            },
        ),
        MiniQMTExecutionEvent(
            event_id="order_1",
            runtime_id="runtime_shadow",
            sequence=3,
            event_type=MiniQMTExecutionEventType.ORDER_EVENT,
            event_time=NOW,
            source="gateway",
            payload={
                **session,
                "child_order_id": "legacy_child_1",
                "broker_order_id": "broker_1",
                "status": order_status,
                "quantity": 100,
                "price": 10,
                **({"traded_volume": 0} if include_cumulative else {}),
            },
        ),
    )
    for event in events:
        repo.append_event(event)
    return repo


def _child_submitted_event(*, sequence: int) -> MiniQMTExecutionEvent:
    return MiniQMTExecutionEvent(
        event_id=f"child_submitted_{sequence}",
        runtime_id="runtime_shadow",
        sequence=sequence,
        event_type=MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED,
        event_time=NOW,
        source="gateway",
        payload={
            "algo_instance_id": "legacy_algo_1",
            "parent_intent_id": "parent_1",
            "strategy_slot_id": "slot_1",
            "child_order_id": "legacy_child_1",
            "broker_order_id": "broker_1",
            "accepted": True,
            "broker_called": True,
        },
    )


def test_parity_input_and_receipt_strict_readback() -> None:
    parity_input = _input()
    trace = _trace(price="10")
    receipt = build_current_three_parity_receipt_v1(
        parity_input=parity_input,
        legacy_source_attribution_sha256="5" * 64,
        plugin_id="aistock.vnpy.sniper",
        plugin_version="3.0.0",
        plugin_manifest_sha256="6" * 64,
        legacy_trace=trace,
        kernel_trace=trace,
    )

    assert receipt.status is CurrentThreeParityStatusV1.PASSED
    assert receipt.broker_called is False
    assert CurrentThreeParityInputV1.model_validate_json(parity_input.model_dump_json()) == parity_input
    assert type(receipt).model_validate_json(receipt.model_dump_json()) == receipt


def test_parity_receipt_fails_loud_on_exact_business_field_drift() -> None:
    receipt = build_current_three_parity_receipt_v1(
        parity_input=_input(),
        legacy_source_attribution_sha256="5" * 64,
        plugin_id="aistock.vnpy.sniper",
        plugin_version="3.0.0",
        plugin_manifest_sha256="6" * 64,
        legacy_trace=_trace(price="10"),
        kernel_trace=_trace(price="9.99"),
    )

    assert receipt.status is CurrentThreeParityStatusV1.FAILED
    assert receipt.ordered_differences
    assert any("canonical_price" in item.field_path for item in receipt.ordered_differences)


def test_parity_difference_evidence_is_bounded_with_explicit_omitted_set_marker() -> None:
    legacy_step = build_current_three_parity_trace_step_v1(
        step_ordinal=0,
        event_type="TICK",
        event_payload_sha256="1" * 64,
        logical_time_utc="2026-07-29T01:30:00Z",
        state_status="RUNNING",
        traded_quantity=0,
        remaining_quantity=100,
        algo_specific_state_projection={f"field_{index:03d}": index for index in range(300)},
        ordered_business_effects=(),
        ordered_transport_duplicate_observations=(),
        ordered_timer_effects=(),
        ordered_diagnostic_reason_codes=(),
        terminal_outcome=None,
    )
    kernel_step = build_current_three_parity_trace_step_v1(
        step_ordinal=0,
        event_type="TICK",
        event_payload_sha256="1" * 64,
        logical_time_utc="2026-07-29T01:30:00Z",
        state_status="RUNNING",
        traded_quantity=0,
        remaining_quantity=100,
        algo_specific_state_projection={f"field_{index:03d}": index + 1 for index in range(300)},
        ordered_business_effects=(),
        ordered_transport_duplicate_observations=(),
        ordered_timer_effects=(),
        ordered_diagnostic_reason_codes=(),
        terminal_outcome=None,
    )
    receipt = build_current_three_parity_receipt_v1(
        parity_input=_input(),
        legacy_source_attribution_sha256="5" * 64,
        plugin_id="aistock.vnpy.sniper",
        plugin_version="3.0.0",
        plugin_manifest_sha256="6" * 64,
        legacy_trace=build_current_three_parity_trace_v1(
            algo_code="SNIPER_MINIQMT", side=SideV1.BUY, ordered_steps=(legacy_step,)
        ),
        kernel_trace=build_current_three_parity_trace_v1(
            algo_code="SNIPER_MINIQMT", side=SideV1.BUY, ordered_steps=(kernel_step,)
        ),
    )
    assert len(receipt.ordered_differences) == 256
    assert receipt.ordered_differences[-1].reason_code == "MINIQMT_K3_PARITY_DIFFERENCE_SET_TRUNCATED"


def test_shadow_command_association_is_exact_one_to_one() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    repo.upsert_algo_instance(_shadow_algo())
    repo.upsert_child_order(_child())
    for event in _events():
        repo.append_event(event)
    read = repo.read_current_three_shadow_snapshot("runtime_shadow")
    parity_input = build_current_three_parity_input_v1(
        algo_code="SNIPER_MINIQMT",
        runtime_id="shadow_kernel_runtime",
        parent_intent_id="parent_1",
        strategy_slot_id="slot_1",
        symbol="600000.SH",
        side=SideV1.BUY,
        target_quantity=100,
        limit_price_decimal="10",
        pricetick_decimal="0.01",
        min_volume=100,
        volume_increment=100,
        plugin_config={"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        legacy_policy_projection_receipt_sha256="3" * 64,
        ordered_event_refs=(_event_ref(),),
    )
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="shadow_kernel_runtime",
        algo_instance_id="kernel_algo_1",
        parent_intent_id="parent_1",
        transition_id="transition_1",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="sniper_ask_crossed_limit",
        metadata={"market_data_lineage": {"market_data_id": "market_1"}},
    )

    associations = associate_current_three_shadow_commands_v1(
        read=read,
        parity_input=parity_input,
        commands_by_step=((command,),),
        legacy_child_order_ids_by_step=(("legacy_child_1",),),
    )

    assert len(associations) == 1
    assert associations[0].legacy_child_order_id == "legacy_child_1"
    assert associations[0].legacy_broker_order_id == "broker_1"


def test_shadow_command_association_rejects_child_from_a_different_step() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    repo.upsert_algo_instance(_shadow_algo())
    repo.upsert_child_order(_child())
    for event in _events():
        repo.append_event(event)
    read = repo.read_current_three_shadow_snapshot("runtime_shadow")
    refs = (
        _event_ref(),
        build_parity_event_ref_v1(
            step_ordinal=1,
            event_id="event_tick_2",
            event_type="TICK",
            event_source="gateway",
            event_payload_sha256="7" * 64,
            logical_time_utc="2026-07-29T01:31:00Z",
            market_data_projection_id="market_2",
            market_data_projection_sha256="8" * 64,
        ),
    )
    parity_input = build_current_three_parity_input_v1(
        algo_code="SNIPER_MINIQMT",
        runtime_id="shadow_kernel_runtime",
        parent_intent_id="parent_1",
        strategy_slot_id="slot_1",
        symbol="600000.SH",
        side=SideV1.BUY,
        target_quantity=100,
        limit_price_decimal="10",
        pricetick_decimal="0.01",
        min_volume=100,
        volume_increment=100,
        plugin_config={"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        legacy_policy_projection_receipt_sha256="3" * 64,
        ordered_event_refs=refs,
    )
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="shadow_kernel_runtime",
        algo_instance_id="kernel_algo_1",
        parent_intent_id="parent_1",
        transition_id="transition_1",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="sniper_ask_crossed_limit",
        metadata={"market_data_lineage": {"market_data_id": "market_2"}},
    )

    with pytest.raises(CurrentThreeContractError) as exc_info:
        associate_current_three_shadow_commands_v1(
            read=read,
            parity_input=parity_input,
            commands_by_step=((), (command,)),
            legacy_child_order_ids_by_step=(("legacy_child_1",), ()),
        )

    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID"


@pytest.mark.parametrize("status", [48, "ACCEPTED", "PART_TRADED"])
def test_committed_parity_uses_the_shared_order_status_authority(status) -> None:
    repo = _committed_sniper_repo(order_status=status)

    receipt = run_current_three_committed_parity_v1(
        repo.read_current_three_shadow_snapshot("runtime_shadow"), legacy_algo_instance_id="legacy_algo_1"
    )

    assert receipt.status is CurrentThreeParityStatusV1.PASSED


def test_committed_parity_preserves_missing_order_cumulative_as_null(monkeypatch) -> None:
    repo = _committed_sniper_repo(order_status="SUBMITTED", include_cumulative=False)
    observed_cumulative = []
    real_factory = shadow_runner_module.create_vnpy_style_core

    def _capturing_factory(*args, **kwargs):
        core = real_factory(*args, **kwargs)
        real_update_order = core.update_order

        def _capture(order):
            observed_cumulative.append(order.traded)
            return real_update_order(order)

        core.update_order = _capture
        return core

    monkeypatch.setattr(shadow_runner_module, "create_vnpy_style_core", _capturing_factory)

    receipt = run_current_three_committed_parity_v1(
        repo.read_current_three_shadow_snapshot("runtime_shadow"), legacy_algo_instance_id="legacy_algo_1"
    )

    assert receipt.status is CurrentThreeParityStatusV1.PASSED
    assert observed_cumulative == [None]


def test_committed_parity_wraps_unknown_order_status_as_typed_k3_failure() -> None:
    repo = _committed_sniper_repo(order_status=999)

    with pytest.raises(CurrentThreeContractError) as exc_info:
        run_current_three_committed_parity_v1(
            repo.read_current_three_shadow_snapshot("runtime_shadow"), legacy_algo_instance_id="legacy_algo_1"
        )

    assert exc_info.value.reason_code == "MINIQMT_K3_ORDER_EVENT_PAYLOAD_INVALID"


def test_committed_sniper_tick_drives_same_legacy_and_kernel_input_without_broker() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    algo = _shadow_algo().model_copy(
        update={
            "metadata": {
                "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
                "legacy_state": {"status": "RUNNING"},
                "limit_price_decimal": "10",
                "pricetick_decimal": "0.01",
                "min_volume": 100,
                "volume_increment": 100,
            }
        }
    )
    repo.upsert_algo_instance(algo)
    repo.upsert_child_order(_child())
    tick: MiniQMTExecutionEvent = _events()[0].model_copy(
        update={
            "payload": {
                **_events()[0].payload,
                "generation": 1,
                "session_phase": "CONTINUOUS_AM",
                "session_epoch": "session_shadow_am",
                "exchange_trade_date": "2026-07-29",
            }
        }
    )
    repo.append_event(tick)
    repo.append_event(_child_submitted_event(sequence=2))
    read = repo.read_current_three_shadow_snapshot("runtime_shadow")

    receipt = run_current_three_committed_parity_v1(read, legacy_algo_instance_id="legacy_algo_1")

    assert receipt.status is CurrentThreeParityStatusV1.PASSED
    assert receipt.broker_called is False


def test_committed_best_limit_uses_same_deterministic_draw_context() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    algo = _shadow_algo().model_copy(
        update={
            "algo_code": "BEST_LIMIT_MINIQMT",
            "metadata": {
                "config": {"min_volume": 100, "max_volume": 100},
                "legacy_state": {"status": "RUNNING"},
                "limit_price_decimal": "10",
                "pricetick_decimal": "0.01",
                "min_volume": 100,
                "volume_increment": 100,
            },
        }
    )
    child = _child().model_copy(update={"price": 9.99, "metadata": {"reason_code": "best_limit_buy_at_bid_price_1"}})
    tick = _events()[0].model_copy(
        update={
            "payload": {
                **_events()[0].payload,
                "generation": 1,
                "session_phase": "CONTINUOUS_AM",
                "session_epoch": "session_shadow_am",
                "exchange_trade_date": "2026-07-29",
            }
        }
    )
    repo.upsert_algo_instance(algo)
    repo.upsert_child_order(child)
    repo.append_event(tick)
    repo.append_event(_child_submitted_event(sequence=2))

    receipt = run_current_three_committed_parity_v1(
        repo.read_current_three_shadow_snapshot("runtime_shadow"), legacy_algo_instance_id="legacy_algo_1"
    )

    assert receipt.status is CurrentThreeParityStatusV1.PASSED


def test_committed_twap_tick_and_active_second_timers_preserve_slice_semantics() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    algo = _shadow_algo().model_copy(
        update={
            "algo_code": "TWAP_LITE_MINIQMT",
            "metadata": {
                "config": {"time": 4, "interval": 2},
                "legacy_state": {"status": "RUNNING", "timer_count": 0, "total_count": 0},
                "limit_price_decimal": "10",
                "pricetick_decimal": "0.01",
                "min_volume": 100,
                "volume_increment": 100,
            },
        }
    )
    child = _child().model_copy(update={"metadata": {"reason_code": "twap_lite_interval_buy"}})
    tick = _events()[0].model_copy(
        update={
            "payload": {
                **_events()[0].payload,
                "generation": 1,
                "session_phase": "CONTINUOUS_AM",
                "session_epoch": "session_shadow_am",
                "exchange_trade_date": "2026-07-29",
            }
        }
    )
    timers = tuple(
        MiniQMTExecutionEvent(
            event_id=f"timer_{index}",
            runtime_id="runtime_shadow",
            sequence=index + 1,
            event_type=MiniQMTExecutionEventType.TIMER,
            event_time=NOW,
            source="runtime",
            payload={
                "timer_name": "TWAP_ACTIVE_SECOND",
                "timer_occurrence_id": f"occurrence_{index}",
                "schedule_epoch": "session_shadow_am",
                "session_epoch": "session_shadow_am",
                "session_phase": "CONTINUOUS_AM",
                "exchange_trade_date": "2026-07-29",
                "algo_instance_id": "legacy_algo_1",
                "monotonic_ns": index,
            },
        )
        for index in (1, 2)
    )
    repo.upsert_algo_instance(algo)
    repo.upsert_child_order(child)
    repo.append_event(tick)
    for timer in timers:
        repo.append_event(timer)
    repo.append_event(_child_submitted_event(sequence=4))

    receipt = run_current_three_committed_parity_v1(
        repo.read_current_three_shadow_snapshot("runtime_shadow"), legacy_algo_instance_id="legacy_algo_1"
    )

    assert receipt.status is CurrentThreeParityStatusV1.PASSED


def test_visible_repeated_cancel_is_transport_suppressed_only_while_first_cancel_is_pending() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    algo = _shadow_algo().model_copy(
        update={
            "metadata": {
                "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
                "legacy_state": {"status": "RUNNING"},
                "limit_price_decimal": "10",
                "pricetick_decimal": "0.01",
                "min_volume": 100,
                "volume_increment": 100,
            }
        }
    )
    repo.upsert_algo_instance(algo)
    repo.upsert_child_order(_child())
    common_tick = {
        **_events()[0].payload,
        "session_phase": "CONTINUOUS_AM",
        "session_epoch": "session_shadow_am",
        "exchange_trade_date": "2026-07-29",
    }
    events = (
        _events()[0].model_copy(update={"payload": {**common_tick, "generation": 1}}),
        _child_submitted_event(sequence=2),
        _events()[1].model_copy(
            update={
                "sequence": 3,
                "payload": {
                    **_events()[1].payload,
                    "session_phase": "CONTINUOUS_AM",
                    "session_epoch": "session_shadow_am",
                    "exchange_trade_date": "2026-07-29",
                },
            }
        ),
        _events()[0].model_copy(
            update={"event_id": "tick_cancel_1", "sequence": 4, "payload": {**common_tick, "generation": 2}}
        ),
        _events()[0].model_copy(
            update={"event_id": "tick_cancel_2", "sequence": 5, "payload": {**common_tick, "generation": 3}}
        ),
    )
    for event in events:
        repo.append_event(event)

    receipt = run_current_three_committed_parity_v1(
        repo.read_current_three_shadow_snapshot("runtime_shadow"), legacy_algo_instance_id="legacy_algo_1"
    )

    assert receipt.status is CurrentThreeParityStatusV1.PASSED


def test_dependent_buy_inventory_is_visible_but_never_consumed_as_algo_local_parity_state() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    algo = _shadow_algo().model_copy(
        update={
            "metadata": {
                "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
                "legacy_state": {"status": "RUNNING"},
                "limit_price_decimal": "10",
                "pricetick_decimal": "0.01",
                "min_volume": 100,
                "volume_increment": 100,
                "dependent_buy": True,
                "dependent_buy_status": "DEFERRED",
                "dependent_buy_reason_code": "SELL_PROCEEDS_REQUIRED",
                "dependent_buy_strategy_id": "strategy_1",
                "dependent_buy_required_cash": "1000",
                "dependent_buy_contract": {"sell_parent_intent_ids": ["sell_parent_1"]},
                "dependent_buy_action": {"symbol": "600000.SH", "quantity": 100, "price": "10"},
            }
        }
    )
    tick = _events()[0].model_copy(
        update={
            "payload": {
                **_events()[0].payload,
                "generation": 1,
                "session_phase": "CONTINUOUS_AM",
                "session_epoch": "session_shadow_am",
                "exchange_trade_date": "2026-07-29",
            }
        }
    )
    repo.upsert_algo_instance(algo)
    repo.upsert_child_order(_child())
    repo.append_event(tick)
    repo.append_event(_child_submitted_event(sequence=2))
    read = repo.read_current_three_shadow_snapshot("runtime_shadow")

    _, dependent = build_current_three_legacy_inventory_set_v1(read)
    receipt = run_current_three_committed_parity_v1(read, legacy_algo_instance_id="legacy_algo_1")

    assert len(dependent) == 1
    assert receipt.execution_coordination_scope == "ALGO_LOCAL_ONLY"
    assert receipt.status is CurrentThreeParityStatusV1.PASSED


def test_parity_input_fails_loud_when_legacy_algo_has_no_committed_business_event() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    repo.upsert_algo_instance(
        _shadow_algo().model_copy(
            update={
                "metadata": {
                    "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
                    "limit_price_decimal": "10",
                    "pricetick_decimal": "0.01",
                    "min_volume": 100,
                    "volume_increment": 100,
                }
            }
        )
    )
    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_parity_input_from_shadow_v1(
            repo.read_current_three_shadow_snapshot("runtime_shadow"),
            legacy_algo_instance_id="legacy_algo_1",
        )
    assert exc_info.value.reason_code == "MINIQMT_K3_PARITY_INPUT_EMPTY"


def test_committed_order_trade_and_eod_callbacks_preserve_full_lifecycle_parity() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    repo.upsert_algo_instance(
        _shadow_algo().model_copy(
            update={
                "metadata": {
                    "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
                    "legacy_state": {"status": "RUNNING"},
                    "limit_price_decimal": "10",
                    "pricetick_decimal": "0.01",
                    "min_volume": 100,
                    "volume_increment": 100,
                }
            }
        )
    )
    repo.upsert_child_order(_child())
    session = {
        "session_phase": "CONTINUOUS_AM",
        "session_epoch": "session_shadow_am",
        "exchange_trade_date": "2026-07-29",
    }
    events = (
        _events()[0].model_copy(update={"payload": {**_events()[0].payload, **session, "generation": 1}}),
        _child_submitted_event(sequence=2),
        _events()[1].model_copy(update={"sequence": 3, "payload": {**_events()[1].payload, **session}}),
        MiniQMTExecutionEvent(
            event_id="trade_1",
            runtime_id="runtime_shadow",
            sequence=4,
            event_type=MiniQMTExecutionEventType.TRADE_EVENT,
            event_time=NOW,
            source="gateway",
            payload={
                **session,
                "child_order_id": "legacy_child_1",
                "broker_order_id": "broker_1",
                "deal_id": "trade_1",
                "quantity": 100,
                "price": 10,
            },
        ),
        MiniQMTExecutionEvent(
            event_id="order_filled_1",
            runtime_id="runtime_shadow",
            sequence=5,
            event_type=MiniQMTExecutionEventType.ORDER_EVENT,
            event_time=NOW,
            source="gateway",
            payload={
                **session,
                "child_order_id": "legacy_child_1",
                "broker_order_id": "broker_1",
                "status": "FILLED",
                "quantity": 100,
                "traded_volume": 100,
                "price": 10,
            },
        ),
        MiniQMTExecutionEvent(
            event_id="runtime_stopped_1",
            runtime_id="runtime_shadow",
            sequence=6,
            event_type=MiniQMTExecutionEventType.RUNTIME_STOPPED,
            event_time=NOW,
            source="runtime",
            payload={**session, "reason": "trading_day_closed"},
        ),
    )
    for event in events:
        repo.append_event(event)

    first = run_current_three_committed_parity_v1(
        repo.read_current_three_shadow_snapshot("runtime_shadow"), legacy_algo_instance_id="legacy_algo_1"
    )
    repeated = run_current_three_committed_parity_v1(
        repo.read_current_three_shadow_snapshot("runtime_shadow"), legacy_algo_instance_id="legacy_algo_1"
    )

    assert first.status is CurrentThreeParityStatusV1.PASSED, [
        (item.step_ordinal, item.field_path) for item in first.ordered_differences
    ]
    assert repeated == first
    assert first.broker_called is False
