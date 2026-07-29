from __future__ import annotations

from datetime import datetime

import pytest

from backend.execution_algos.vnpy_compat.facade import (
    VnpyAlgoEngineFacadeV1,
    VnpyFacadeEffectCollectorV1,
)
from backend.execution_algos.vnpy_compat.facade_projection import (
    ContractData,
    Direction,
    Exchange,
    Offset,
    OrderType,
    TickData,
)
from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v2
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    DeterministicExecutionContextV1,
    ExecutionCommandChildMappingV1,
    OrderTypeV1,
    SessionPhaseV1,
    SideV1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1


class _AlgoOwner:
    def __init__(self, algo_name: str) -> None:
        self.algo_name = algo_name


def _context() -> DeterministicExecutionContextV1:
    manifest = current_three_manifests_v2()[1]
    return DeterministicExecutionContextV1.create(
        runtime_id="runtime_k4_char",
        algo_instance_id="algo_k4_char",
        event_id="event_k4_char",
        delivery_id="delivery_k4_char",
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=2,
        logical_time_utc="2026-07-29T01:31:00Z",
        exchange_trade_date="2026-07-29",
        session_epoch="session_k4_char",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="a" * 64,
    )


def _facade(
    *,
    contract: ContractData | None = None,
    tick: TickData | None = None,
    active_mappings: tuple[ExecutionCommandChildMappingV1, ...] = (),
) -> tuple[VnpyAlgoEngineFacadeV1, VnpyFacadeEffectCollectorV1, _AlgoOwner]:
    context = _context()
    collector = VnpyFacadeEffectCollectorV1.create(
        context,
        "parent_k4_char",
        "transition_k4_char",
    )
    facade = VnpyAlgoEngineFacadeV1._create_characterization_v1(
        deterministic_context=context,
        parent_intent_id="parent_k4_char",
        symbol="600000.SH",
        side=SideV1.BUY,
        contract=contract,
        tick=tick,
        active_mappings=active_mappings,
        manifest=current_three_manifests_v2()[1],
        effect_collector=collector,
    )
    return facade, collector, _AlgoOwner(context.algo_instance_id)


def _contract() -> ContractData:
    return ContractData(
        symbol="600000",
        exchange=Exchange.SSE,
        gateway_name="minqmt_sim",
        min_volume=100.0,
        pricetick=0.01,
    )


def _tick() -> TickData:
    return TickData(
        vt_symbol="600000.SSE",
        datetime=datetime.fromisoformat("2026-07-29T09:31:00+08:00"),
        bid_price_1=10.0,
        bid_volume_1=500.0,
        ask_price_1=10.01,
        ask_volume_1=400.0,
        last_price=10.0,
        limit_up=11.0,
        limit_down=9.0,
    )


def test_characterization_facade_uses_existing_submit_constructor_and_ordinals() -> None:
    facade, collector, algo = _facade(contract=_contract(), tick=_tick())

    facade.write_log("before", algo)
    local_id = facade.send_order(
        algo,
        Direction.LONG,
        10.01,
        150.0,
        OrderType.LIMIT,
        Offset.NONE,
    )

    assert local_id.startswith("mqlocalorder_")
    assert len(collector.diagnostic_observations) == 1
    assert len(collector.broker_commands) == 1
    assert collector.diagnostic_observations[0].ordinal == 0
    assert collector.broker_commands[0].ordinal == 1
    assert collector.broker_commands[0].quantity == 200
    assert collector.broker_commands[0].side is SideV1.BUY


def test_characterization_facade_cancel_uses_exact_owned_mapping_and_broker_identity() -> None:
    context = _context()
    submit = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        parent_intent_id="parent_k4_char",
        transition_id="transition_k4_previous",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="K4_CANCEL_POSITIVE_FIXTURE",
        metadata={},
    )
    mapping = ExecutionCommandChildMappingV1.create(
        command=submit,
        strategy_slot_id="slot_k4_char",
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=2,
        broker_order_id="broker_k4_owned",
        broker_identity_source_event_id="order_event_k4_owned",
        last_order_event_id="order_event_k4_owned",
        last_trade_event_id=None,
        updated_by_event_id="order_event_k4_owned",
        created_at_utc="2026-07-29T01:29:00Z",
        updated_at_utc="2026-07-29T01:30:00Z",
    )
    facade, collector, algo = _facade(contract=_contract(), tick=_tick(), active_mappings=(mapping,))

    facade.cancel_order(algo, mapping.local_vt_orderid)

    assert len(collector.broker_commands) == 1
    cancel = collector.broker_commands[0]
    assert cancel.command_type is BrokerCommandTypeV2.CANCEL_ORDER
    assert cancel.local_vt_orderid == mapping.local_vt_orderid
    assert cancel.owned_broker_order_id == mapping.broker_order_id


def test_missing_contract_and_rounded_zero_are_visible_and_create_no_command() -> None:
    missing, missing_collector, algo = _facade(contract=None)
    assert missing.send_order(algo, Direction.LONG, 10.0, 100.0, OrderType.LIMIT, Offset.NONE) == ""
    assert not missing_collector.broker_commands
    assert [item.reason_code for item in missing_collector.diagnostic_observations] == [
        "MINIQMT_VNPY_FACADE_CONTRACT_UNAVAILABLE"
    ]

    rounded, rounded_collector, rounded_algo = _facade(contract=_contract())
    assert rounded.send_order(rounded_algo, Direction.LONG, 10.0, 49.0, OrderType.LIMIT, Offset.NONE) == ""
    assert not rounded_collector.broker_commands
    assert [item.reason_code for item in rounded_collector.diagnostic_observations] == [
        "MINIQMT_VNPY_FACADE_ROUNDED_VOLUME_ZERO"
    ]


def test_tick_contract_log_and_projection_are_collected_without_event_engine() -> None:
    facade, collector, algo = _facade(contract=_contract(), tick=_tick())
    assert facade.get_tick(algo) == _tick()
    assert facade.get_contract(algo) == _contract()

    facade.write_log("x" * 3000, algo)
    facade.put_algo_event(algo, {"status": "RUNNING", "traded": "0"})

    assert tuple(item.reason_code for item in collector.diagnostic_observations) == (
        "MINIQMT_VNPY_FACADE_ALGO_LOG",
        "MINIQMT_VNPY_FACADE_ALGO_PROJECTION",
    )
    log_context = thaw_json_v1(collector.diagnostic_observations[0].context)
    assert log_context["message_truncated"] is True
    assert log_context["original_length"] == 3000


def test_facade_rejects_owner_enum_numeric_and_collector_reuse_drift() -> None:
    facade, collector, algo = _facade(contract=_contract())
    with pytest.raises(ValueError, match="CONTRACT_INVALID"):
        facade.get_tick(_AlgoOwner("another_algo"))
    with pytest.raises(TypeError, match="strict number"):
        facade.send_order(algo, Direction.LONG, True, 100.0, OrderType.LIMIT, Offset.NONE)
    with pytest.raises(ValueError, match="finite positive"):
        facade.send_order(algo, Direction.LONG, 10.0, float("nan"), OrderType.LIMIT, Offset.NONE)
    with pytest.raises(ValueError, match="single-use"):
        VnpyAlgoEngineFacadeV1._create_characterization_v1(
            deterministic_context=_context(),
            parent_intent_id="parent_k4_char",
            symbol="600000.SH",
            side=SideV1.BUY,
            contract=_contract(),
            tick=None,
            active_mappings=(),
            manifest=current_three_manifests_v2()[1],
            effect_collector=collector,
        )


def test_facade_public_methods_reject_malformed_inputs_without_effects() -> None:
    facade, collector, algo = _facade(contract=_contract())
    baseline = (collector.broker_commands, collector.diagnostic_observations)

    with pytest.raises(ValueError, match="DTO_MAPPING_INVALID"):
        facade.send_order(algo, "LONG", 10, 100, OrderType.LIMIT, Offset.NONE)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="trim-stable"):
        facade.cancel_order(algo, " bad")
    with pytest.raises(ValueError, match="CANCEL_OWNERSHIP_INVALID"):
        facade.cancel_order(algo, "unknown_order")
    with pytest.raises(TypeError, match="strict string"):
        facade.write_log(1, algo)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="string-keyed"):
        facade.put_algo_event(algo, {1: "bad"})  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="non-finite"):
        facade.put_algo_event(algo, {"price": float("inf")})
    with pytest.raises(TypeError, match="unsupported"):
        facade.put_algo_event(algo, {"value": object()})
    with pytest.raises(ValueError, match="maximum depth"):
        facade.put_algo_event(algo, {"nested": [[[[[[[[0]]]]]]]]})

    assert (collector.broker_commands, collector.diagnostic_observations) == baseline


def test_facade_diagnostic_carriers_are_normalized_and_missing_tick_is_visible() -> None:
    facade, collector, algo = _facade(contract=_contract())

    assert facade.get_tick(algo) is None
    facade.put_algo_event(
        algo,
        {
            "flag": True,
            "count": 1,
            "price": 10.5,
            "direction": Direction.LONG,
            "items": (None, "x"),
        },
    )
    facade.write_log("without explicit algo owner")

    assert tuple(item.reason_code for item in collector.diagnostic_observations) == (
        "MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE",
        "MINIQMT_VNPY_FACADE_ALGO_PROJECTION",
        "MINIQMT_VNPY_FACADE_ALGO_LOG",
    )
    projection = thaw_json_v1(collector.diagnostic_observations[1].context)["projection"]
    assert projection["price"] == "10.5"
    assert projection["direction"] == {
        "enum_owner": "Direction",
        "member": "LONG",
        "pinned_value": Direction.LONG.value,
    }


def test_collector_and_characterization_constructor_are_strict() -> None:
    with pytest.raises(TypeError, match="DeterministicExecutionContextV1"):
        VnpyFacadeEffectCollectorV1.create(None, "parent", "transition")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="trim-stable"):
        VnpyFacadeEffectCollectorV1.create(_context(), " parent", "transition")
    with pytest.raises(TypeError, match="use VnpyAlgoEngineFacadeV1.create"):
        VnpyAlgoEngineFacadeV1()

    collector = VnpyFacadeEffectCollectorV1.create(_context(), "parent_k4_char", "transition_k4_char")
    with pytest.raises(TypeError, match="side must be SideV1"):
        VnpyAlgoEngineFacadeV1._create_characterization_v1(
            deterministic_context=_context(),
            parent_intent_id="parent_k4_char",
            symbol="600000.SH",
            side="BUY",  # type: ignore[arg-type]
            contract=_contract(),
            tick=None,
            active_mappings=(),
            manifest=current_three_manifests_v2()[1],
            effect_collector=collector,
        )
    with pytest.raises(TypeError, match="exact K4 facade input"):
        VnpyAlgoEngineFacadeV1.create(None, collector)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="VnpyFacadeEffectCollectorV1"):
        VnpyAlgoEngineFacadeV1._create_characterization_v1(
            deterministic_context=_context(),
            parent_intent_id="parent_k4_char",
            symbol="600000.SH",
            side=SideV1.BUY,
            contract=_contract(),
            tick=None,
            active_mappings=(),
            manifest=current_three_manifests_v2()[1],
            effect_collector=None,  # type: ignore[arg-type]
        )
