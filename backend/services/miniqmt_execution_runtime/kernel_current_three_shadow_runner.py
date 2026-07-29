"""Production-shape, broker-free K3-B committed-fact parity runner."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

from backend.execution_algos.vnpy_style.attribution import source_attribution
from backend.execution_algos.vnpy_style.models import (
    VnpyAction,
    VnpyActionType,
    VnpyOrderUpdate,
    VnpyTick,
    VnpyTradeUpdate,
)
from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3
from backend.execution_algos.vnpy_style.plugin_manifests import (
    LegacyProjectionDriftV1,
    current_three_manifests_v3,
    project_legacy_vnpy_policy_v1,
)
from backend.execution_algos.vnpy_style.registry import create_vnpy_style_core

from .deterministic_context import best_limit_quantity_v1
from .kernel_callback_events import (
    build_kernel_order_event_payload_v1,
    build_kernel_trade_event_payload_v1,
    strict_readback_kernel_event_payload_v1,
)
from .kernel_current_three_contracts import (
    CurrentThreeContractError,
    CurrentThreeParityInputV1,
    CurrentThreeParityReceiptV1,
)
from .kernel_current_three_parity import (
    associate_current_three_shadow_commands_v1,
    build_current_three_parity_input_v1,
    build_current_three_parity_receipt_v1,
    build_current_three_parity_trace_step_v1,
    build_current_three_parity_trace_v1,
    build_parity_event_ref_v1,
)
from .kernel_current_three_shadow_source import (
    CurrentThreeShadowRepositoryReadV1,
    resolve_current_three_event_owner_v1,
)
from .models import MiniQMTExecutionEvent, MiniQMTExecutionEventType
from .plugin_canonical import canonical_decimal_string_v1, hash_hex_v1, thaw_json_v1
from .plugin_canonical import canonical_utc_datetime_v1
from .plugin_contracts import (
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    DeterministicExecutionContextV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    KernelOrderEventPayloadV1,
    KernelProjectionTypeV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    SideV1,
    _algo_instance_id_v2,
)


_BINDING_BY_ALGO = {
    "SNIPER_MINIQMT": "aistock.vnpy.sniper.factory",
    "BEST_LIMIT_MINIQMT": "aistock.vnpy.best_limit.factory",
    "TWAP_LITE_MINIQMT": "aistock.vnpy.twap_lite.factory",
}


def _fail(reason: str, message: str, **context: Any) -> CurrentThreeContractError:
    return CurrentThreeContractError(reason, message, context={"stage": "K3_COMMITTED_PARITY", **context})


def _metadata_config(metadata: dict[str, Any]) -> dict[str, Any]:
    values = [(name, metadata[name]) for name in ("config", "setting", "algo_setting") if name in metadata]
    if not values or any(not isinstance(value, dict) for _, value in values):
        raise _fail(
            "MINIQMT_K3_PLUGIN_CONFIG_INVALID",
            "legacy algo metadata does not contain one strict config object",
            aliases=[name for name, _ in values],
        )
    first = values[0][1]
    if any(value != first for _, value in values[1:]):
        raise _fail(
            "MINIQMT_K3_PLUGIN_CONFIG_INVALID",
            "legacy config aliases conflict",
            aliases=[name for name, _ in values],
        )
    return dict(first)


def _required_contract(metadata: dict[str, Any], field: str) -> Any:
    candidates = [
        metadata.get(field),
        (metadata.get("child_context") or {}).get(field) if isinstance(metadata.get("child_context"), dict) else None,
        (metadata.get("contract") or {}).get(field) if isinstance(metadata.get("contract"), dict) else None,
    ]
    present = [value for value in candidates if value is not None]
    if not present or any(value != present[0] or type(value) is not type(present[0]) for value in present[1:]):
        raise _fail(
            "MINIQMT_K3_EVENT_PAYLOAD_INVALID",
            "legacy algo contract fact is missing or aliases conflict",
            field=field,
            present_count=len(present),
        )
    return present[0]


def _positive_int_contract(metadata: dict[str, Any], field: str) -> int:
    value = _required_contract(metadata, field)
    if type(value) is not int or value <= 0:
        raise _fail(
            "MINIQMT_K3_EVENT_PAYLOAD_INVALID",
            "legacy algo integer contract fact is not a positive strict integer",
            field=field,
            actual_type=type(value).__name__,
        )
    return value


def _decimal_contract(metadata: dict[str, Any], field: str) -> str:
    value = _required_contract(metadata, field)
    if type(value) is not str:
        raise _fail(
            "MINIQMT_K3_EVENT_PAYLOAD_INVALID",
            "legacy algo decimal contract fact is not an exact string carrier",
            field=field,
            actual_type=type(value).__name__,
        )
    try:
        return canonical_decimal_string_v1(value, field_name=field, allow_zero=False)
    except (TypeError, ValueError) as exc:
        raise _fail(
            "MINIQMT_K3_EVENT_PAYLOAD_INVALID",
            "legacy algo decimal contract fact is invalid",
            field=field,
            error_type=type(exc).__name__,
        ) from exc


def _event_owner(read: CurrentThreeShadowRepositoryReadV1, event: MiniQMTExecutionEvent) -> str | None:
    return resolve_current_three_event_owner_v1(event=event, algos=read.algos, children=read.children)


def _selected_events(read: CurrentThreeShadowRepositoryReadV1, algo: Any) -> tuple[MiniQMTExecutionEvent, ...]:
    selected = []
    for event in read.events:
        if event.event_type is MiniQMTExecutionEventType.TICK:
            if event.payload.get("symbol") == algo.symbol:
                selected.append(event)
        elif event.event_type is MiniQMTExecutionEventType.TIMER:
            if algo.algo_code == "TWAP_LITE_MINIQMT" and event.payload.get("algo_instance_id") in (
                None,
                algo.algo_instance_id,
            ):
                selected.append(event)
        elif event.event_type in {MiniQMTExecutionEventType.ORDER_EVENT, MiniQMTExecutionEventType.TRADE_EVENT}:
            if _event_owner(read, event) == algo.algo_instance_id:
                selected.append(event)
        elif event.event_type is MiniQMTExecutionEventType.RUNTIME_STOPPED:
            selected.append(event)
    return tuple(selected)


def _legacy_submitted_child_ids_by_step_v1(
    read: CurrentThreeShadowRepositoryReadV1,
    *,
    algo: Any,
    selected_events: tuple[MiniQMTExecutionEvent, ...],
) -> tuple[tuple[str, ...], ...]:
    """Bind committed child-submit lineage to the exact preceding parity step."""

    selected_sequences = tuple(item.sequence for item in selected_events)
    if tuple(sorted(selected_sequences)) != selected_sequences or len(set(selected_sequences)) != len(
        selected_sequences
    ):
        raise _fail(
            "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
            "selected parity event sequence is not strict and unique",
            algo_instance_id=algo.algo_instance_id,
        )
    by_step: list[list[str]] = [[] for _ in selected_events]
    seen_children: set[str] = set()
    submitted = sorted(
        (item for item in read.events if item.event_type is MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED),
        key=lambda item: (item.sequence, item.event_id),
    )
    for event in submitted:
        owner = resolve_current_three_event_owner_v1(event=event, algos=read.algos, children=read.children)
        if owner != algo.algo_instance_id:
            continue
        preceding = [index for index, sequence in enumerate(selected_sequences) if sequence < event.sequence]
        if not preceding:
            raise _fail(
                "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                "legacy child submission has no preceding parity step",
                event_id=event.event_id,
                child_order_id=event.payload.get("child_order_id"),
            )
        step_ordinal = preceding[-1]
        next_sequence = selected_sequences[step_ordinal + 1] if step_ordinal + 1 < len(selected_sequences) else None
        if next_sequence is not None and event.sequence >= next_sequence:
            raise _fail(
                "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                "legacy child submission cannot be placed inside one parity step",
                event_id=event.event_id,
                child_order_id=event.payload.get("child_order_id"),
            )
        child_id = event.payload["child_order_id"]
        if child_id in seen_children:
            raise _fail(
                "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                "legacy child submission lineage is duplicated",
                event_id=event.event_id,
                child_order_id=child_id,
            )
        seen_children.add(child_id)
        by_step[step_ordinal].append(child_id)
    return tuple(tuple(items) for items in by_step)


def build_current_three_parity_input_from_shadow_v1(
    read: CurrentThreeShadowRepositoryReadV1, *, legacy_algo_instance_id: str
) -> tuple[CurrentThreeParityInputV1, tuple[MiniQMTExecutionEvent, ...]]:
    read.strict_readback_v1()
    matches = [item for item in read.algos if item.algo_instance_id == legacy_algo_instance_id]
    if len(matches) != 1:
        raise _fail(
            "MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
            "parity input requires one exact legacy algo instance",
            legacy_algo_instance_id=legacy_algo_instance_id,
            match_count=len(matches),
        )
    algo = matches[0]
    projection = project_legacy_vnpy_policy_v1(algo.algo_code, _metadata_config(dict(algo.metadata)))
    if projection.drift_classification not in {
        LegacyProjectionDriftV1.NO_DRIFT,
        LegacyProjectionDriftV1.ALIAS_EQUIVALENT,
    }:
        raise _fail(
            "MINIQMT_K3_PLUGIN_CONFIG_INVALID",
            "legacy policy projection is not eligible for parity execution",
            legacy_algo_instance_id=legacy_algo_instance_id,
            drift_classification=projection.drift_classification.value,
        )
    events = _selected_events(read, algo)
    if not events:
        raise _fail(
            "MINIQMT_K3_PARITY_INPUT_EMPTY",
            "legacy algo has no committed business event eligible for parity execution",
            legacy_algo_instance_id=legacy_algo_instance_id,
        )
    source_refs = {item.event_id: item for item in read.snapshot.ordered_legacy_event_refs}
    refs = []
    for ordinal, event in enumerate(events):
        payload = event.payload
        market_id = payload.get("market_data_projection_id")
        market_hash = payload.get("market_data_projection_sha256")
        if event.event_type is MiniQMTExecutionEventType.TICK and (
            type(market_id) is not str or type(market_hash) is not str
        ):
            raise _fail(
                "MINIQMT_K3_MARKET_DATA_LINEAGE_INVALID",
                "committed TICK lacks exact market-data projection identity/hash",
                event_id=event.event_id,
            )
        refs.append(
            build_parity_event_ref_v1(
                step_ordinal=ordinal,
                event_id=event.event_id,
                event_type=event.event_type.value,
                event_source=event.source,
                event_payload_sha256=source_refs[event.event_id].payload_sha256,
                logical_time_utc=event.event_time,
                market_data_projection_id=market_id if type(market_id) is str else None,
                market_data_projection_sha256=market_hash if type(market_hash) is str else None,
                account_projection_id=payload.get("account_projection_id")
                if type(payload.get("account_projection_id")) is str
                else None,
                account_projection_sha256=payload.get("account_projection_sha256")
                if type(payload.get("account_projection_sha256")) is str
                else None,
                contract_projection_id=payload.get("contract_projection_id")
                if type(payload.get("contract_projection_id")) is str
                else None,
                contract_projection_sha256=payload.get("contract_projection_sha256")
                if type(payload.get("contract_projection_sha256")) is str
                else None,
            )
        )
    shadow_runtime_id = (
        "mqshadow_"
        + hash_hex_v1(
            "miniqmt_current_three_shadow_runtime_identity_v1",
            {
                "source_runtime_id": read.snapshot.runtime_id,
                "trade_date": read.snapshot.trade_date,
                "event_set_sha256": read.snapshot.event_set_sha256,
                "algo_set_sha256": read.snapshot.algo_set_sha256,
                "child_set_sha256": read.snapshot.child_set_sha256,
                "legacy_algo_instance_id": legacy_algo_instance_id,
            },
        )[:32]
    )
    return (
        build_current_three_parity_input_v1(
            algo_code=algo.algo_code,
            runtime_id=shadow_runtime_id,
            parent_intent_id=algo.parent_intent_id,
            strategy_slot_id=algo.strategy_slot_id,
            symbol=algo.symbol,
            side=SideV1(algo.side.value),
            target_quantity=algo.target_quantity,
            limit_price_decimal=_decimal_contract(dict(algo.metadata), "limit_price_decimal"),
            pricetick_decimal=_decimal_contract(dict(algo.metadata), "pricetick_decimal"),
            min_volume=_positive_int_contract(dict(algo.metadata), "min_volume"),
            volume_increment=_positive_int_contract(dict(algo.metadata), "volume_increment"),
            plugin_config=thaw_json_v1(projection.candidate_canonical_config),
            legacy_policy_projection_receipt_sha256=projection.receipt_sha256,
            ordered_event_refs=tuple(refs),
        ),
        events,
    )


def _plugin_start(parity_input: CurrentThreeParityInputV1) -> tuple[Any, Any, Any]:
    manifest = next(item for item in current_three_manifests_v3() if item.algo_code == parity_input.algo_code)
    config = thaw_json_v1(parity_input.plugin_config)
    plugin = current_three_process_bindings_v3().resolve(_BINDING_BY_ALGO[parity_input.algo_code])(config)
    algo_instance_id = _algo_instance_id_v2(
        runtime_id=parity_input.runtime_id,
        parent_intent_id=parity_input.parent_intent_id,
        strategy_slot_id=parity_input.strategy_slot_id,
        algo_code=parity_input.algo_code,
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        plugin_manifest_sha256=manifest.manifest_sha256,
        plugin_config_sha256=parity_input.plugin_config_sha256,
    )
    first_time = parity_input.ordered_event_refs[0].logical_time_utc
    start_event = "mqshadowstart_" + parity_input.input_sha256[:32]
    start_delivery = "mqshadowdelivery_" + parity_input.input_sha256[:32]
    context = DeterministicExecutionContextV1.create(
        runtime_id=parity_input.runtime_id,
        algo_instance_id=algo_instance_id,
        event_id=start_event,
        delivery_id=start_delivery,
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=0,
        logical_time_utc=first_time,
        exchange_trade_date=first_time[:10],
        session_epoch=f"shadow_{first_time[:10]}",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="0" * 64,
    )
    contract = {"pricetick_decimal": parity_input.pricetick_decimal, "min_volume": parity_input.min_volume}
    account = {"account_group_id": "K3_SHADOW_OBSERVATION_ONLY"}
    capability = {"route_id": "miniqmt_shadow_no_dispatch", "capabilities": ["L1_ASK", "L1_BID"]}
    start = AlgoStartContextV1(
        schema_version="miniqmt_algo_start_context_v1",
        runtime_id=parity_input.runtime_id,
        algo_instance_id=algo_instance_id,
        parent_intent_id=parity_input.parent_intent_id,
        strategy_slot_id=parity_input.strategy_slot_id,
        symbol=parity_input.symbol,
        side=parity_input.side,
        limit_price_decimal=parity_input.limit_price_decimal,
        parent_quantity=parity_input.target_quantity,
        min_volume=parity_input.min_volume,
        volume_increment=parity_input.volume_increment,
        plugin_manifest=manifest,
        plugin_config=config,
        plugin_config_sha256=parity_input.plugin_config_sha256,
        start_event_id=start_event,
        start_delivery_id=start_delivery,
        deterministic_context=context,
        contract_projection=contract,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
        account_projection=account,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
        market_capability_projection=capability,
        market_capability_projection_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
        execution_plan_id="shadow_plan_" + parity_input.input_sha256[:24],
        execution_plan_sha256=hash_hex_v1("miniqmt_shadow_execution_plan_v1", {"input": parity_input.input_sha256}),
        release_id="shadow_release_" + parity_input.input_sha256[:24],
        release_sha256=hash_hex_v1("miniqmt_shadow_release_v1", {"input": parity_input.input_sha256}),
        policy_id="shadow_policy_" + parity_input.input_sha256[:24],
        policy_sha256=hash_hex_v1("miniqmt_shadow_policy_v1", {"input": parity_input.input_sha256}),
    )
    initialized = plugin.initialize(start)
    return plugin, manifest, initialized.next_state


def build_current_three_shadow_event_v1(
    *,
    parity_input: CurrentThreeParityInputV1,
    raw: MiniQMTExecutionEvent,
    sequence: int,
    association: Any | None,
) -> RuntimeEventEnvelopeV2:
    session_epoch = raw.payload.get("session_epoch", raw.payload.get("schedule_epoch"))
    correlation = {
        "exchange_trade_date": raw.payload.get("exchange_trade_date"),
        "session_epoch": session_epoch,
        "session_phase": raw.payload.get("session_phase"),
    }
    if any(type(value) is not str or not value.strip() for value in correlation.values()):
        raise _fail(
            "MINIQMT_K3_EVENT_PAYLOAD_INVALID",
            "committed event lacks deterministic exchange session correlation",
            event_id=raw.event_id,
            event_type=raw.event_type.value,
            missing_fields=[name for name, value in correlation.items() if type(value) is not str or not value.strip()],
        )
    if raw.event_type is MiniQMTExecutionEventType.TICK:
        payload = {
            "generation": raw.payload["generation"],
            "session_phase": correlation["session_phase"],
            "exchange_time_utc": canonical_utc_datetime_v1(raw.event_time),
            "bid_price_1": canonical_decimal_string_v1(str(raw.payload["bid_price_1"])),
            "ask_price_1": canonical_decimal_string_v1(str(raw.payload["ask_price_1"])),
            "bid_volume_1": raw.payload["bid_volume_1"],
            "ask_volume_1": raw.payload["ask_volume_1"],
        }
        return RuntimeEventEnvelopeV2.create(
            runtime_id=parity_input.runtime_id,
            sequence=sequence,
            event_type=EventTypeV2.TICK,
            event_time_utc=raw.event_time,
            monotonic_ns=None,
            source=EventSourceV2.B0_QUOTE_V2,
            symbol=parity_input.symbol,
            payload_schema_version="miniqmt_market_data_view_v2",
            payload=payload,
            source_identity={"market_data_id": raw.payload["market_data_projection_id"]},
            correlation=correlation,
        )
    if raw.event_type is MiniQMTExecutionEventType.TIMER:
        payload = {
            "timer_occurrence_id": raw.payload["timer_occurrence_id"],
            "timer_name": raw.payload["timer_name"],
            "schedule_epoch": raw.payload["schedule_epoch"],
        }
        return RuntimeEventEnvelopeV2.create(
            runtime_id=parity_input.runtime_id,
            sequence=sequence,
            event_type=EventTypeV2.TIMER,
            event_time_utc=raw.event_time,
            monotonic_ns=raw.payload["monotonic_ns"],
            source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
            symbol=parity_input.symbol,
            payload_schema_version="miniqmt_timer_due_v1",
            payload=payload,
            source_identity={"timer_occurrence_id": payload["timer_occurrence_id"]},
            correlation=correlation,
        )
    if raw.event_type is MiniQMTExecutionEventType.RUNTIME_STOPPED:
        return RuntimeEventEnvelopeV2.create(
            runtime_id=parity_input.runtime_id,
            sequence=sequence,
            event_type=EventTypeV2.EOD,
            event_time_utc=raw.event_time,
            monotonic_ns=None,
            source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
            symbol=parity_input.symbol,
            payload_schema_version="miniqmt_eod_event_v1",
            payload={"reason": raw.payload["reason"]},
            source_identity={
                "runtime_id": parity_input.runtime_id,
                "trade_date": correlation["exchange_trade_date"],
                "session_epoch": correlation["session_epoch"],
            },
            correlation={**correlation, "session_phase": "CLOSED"},
        )
    if association is None:
        raise _fail(
            "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
            "callback event has no exact shadow command association",
            event_id=raw.event_id,
            event_type=raw.event_type.value,
        )
    common = {
        "runtime_id": parity_input.runtime_id,
        "algo_instance_id": association.kernel_algo_instance_id,
        "parent_intent_id": parity_input.parent_intent_id,
        "strategy_slot_id": parity_input.strategy_slot_id,
        "mapping_id": association.mapping_id,
        "command_id": association.kernel_command_id,
        "local_vt_orderid": association.local_vt_orderid,
        "broker_order_id": association.legacy_broker_order_id,
        "symbol": parity_input.symbol,
        "side": parity_input.side,
    }
    if raw.event_type is MiniQMTExecutionEventType.ORDER_EVENT:
        try:
            payload = build_kernel_order_event_payload_v1(
                raw_payload=raw.payload,
                order_event_id=raw.event_id,
                requested_quantity=association.quantity,
                **common,
            )
        except (TypeError, ValueError) as exc:
            raise _fail(
                "MINIQMT_K3_ORDER_EVENT_PAYLOAD_INVALID",
                "committed ORDER callback does not satisfy the unique callback authority",
                event_id=raw.event_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            ) from exc
        event_type = EventTypeV2.ORDER
        schema = "miniqmt_order_event_v1"
        source_identity = {"order_event_id": raw.event_id}
    elif raw.event_type is MiniQMTExecutionEventType.TRADE_EVENT:
        try:
            payload = build_kernel_trade_event_payload_v1(
                raw_payload=raw.payload,
                trade_quantity=raw.payload["quantity"],
                trade_price_decimal=str(raw.payload["price"]),
                **common,
            )
        except (TypeError, ValueError) as exc:
            raise _fail(
                "MINIQMT_K3_TRADE_EVENT_PAYLOAD_INVALID",
                "committed TRADE callback does not satisfy the unique callback authority",
                event_id=raw.event_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            ) from exc
        event_type = EventTypeV2.TRADE
        schema = "miniqmt_trade_fact_v1"
        source_identity = {"trade_id": payload.trade_id}
    else:
        raise _fail(
            "MINIQMT_K3_EVENT_PAYLOAD_INVALID",
            "shadow event type is not supported by the K3 business adapter",
            event_id=raw.event_id,
            event_type=raw.event_type.value,
        )
    return RuntimeEventEnvelopeV2.create(
        runtime_id=parity_input.runtime_id,
        sequence=sequence,
        event_type=event_type,
        event_time_utc=raw.event_time,
        monotonic_ns=None,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        symbol=parity_input.symbol,
        payload_schema_version=schema,
        payload=payload.model_dump(mode="json"),
        source_identity=source_identity,
        correlation=correlation,
    )


def _services(
    *, state: Any, event: RuntimeEventEnvelopeV2, market: tuple[str, dict[str, Any]] | None
) -> AlgoReadOnlyServicesV1:
    delivery_id = (
        "mqshadowdelivery_"
        + hash_hex_v1(
            "miniqmt_current_three_shadow_delivery_identity_v1",
            {"event_id": event.event_id, "algo_instance_id": state.algo_instance_id},
        )[:32]
    )
    refs = ()
    market_id = None
    market_payload = None
    if market is not None:
        market_id, market_payload = market
        ref = ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.MARKET_DATA,
            projection_id=market_id,
            projection_version="1",
            payload_sha256=hash_hex_v1("miniqmt_market_data_projection_v2", market_payload),
            source_event_id=event.event_id,
            logical_at_utc=event.event_time_utc,
        )
        refs = (ref,)
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        projection_refs=refs,
    )
    return AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id=market_id,
        market_data_projection=market_payload,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )


def _business_effects_from_kernel(
    commands: tuple[BrokerCommandV2, ...],
    *,
    lineage_hash: str | None,
    order_facts: dict[str, dict[str, Any]],
    next_ordinal: dict[str, int],
) -> tuple[dict[str, Any], ...]:
    effects = []
    for command in commands:
        target_ordinal = None
        effect_lineage = lineage_hash
        if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            order_facts[command.local_vt_orderid] = {
                "business_effect_ordinal": next_ordinal["value"],
                "lineage_hash": lineage_hash,
                "pending_cancel": None,
            }
        else:
            fact = order_facts.get(command.local_vt_orderid)
            if fact is None:
                raise _fail(
                    "MINIQMT_K3_PARITY_DRIFT",
                    "kernel cancel has no independently observed preceding submit effect",
                    command_id=command.command_id,
                    local_vt_orderid=command.local_vt_orderid,
                )
            target_ordinal = fact["business_effect_ordinal"]
            effect_lineage = fact["lineage_hash"]
            fact["pending_cancel"] = {
                "command_id": command.command_id,
                "status": "PENDING",
                "payload_sha256": command.payload_sha256,
                "reason_code": command.reason_code,
                "business_effect_ordinal": next_ordinal["value"],
            }
        effects.append(
            {
                "kind": command.command_type.value,
                "side": command.side.value,
                "symbol": command.symbol,
                "canonical_price": command.price_decimal,
                "quantity": command.quantity,
                "cancel_target_ordinal": target_ordinal,
                "reason_code": command.reason_code,
                "market_data_lineage_sha256": effect_lineage,
            }
        )
        next_ordinal["value"] += 1
    return tuple(effects)


def _business_effects_from_legacy(
    actions: list[VnpyAction],
    *,
    lineage_hash: str | None,
    symbol: str,
    order_facts: dict[str, dict[str, Any]],
    next_ordinal: dict[str, int],
) -> tuple[dict[str, Any], ...]:
    effects = []
    for action in actions:
        if action.action_type not in {VnpyActionType.SUBMIT, VnpyActionType.CANCEL}:
            continue
        if action.action_type is VnpyActionType.SUBMIT:
            if action.vt_orderid is None or action.direction is None or action.price is None or action.volume is None:
                raise _fail(
                    "MINIQMT_K3_PARITY_DRIFT",
                    "legacy submit action lacks exact business facts",
                    action_type=action.action_type.value,
                )
            action_side = SideV1.BUY if action.direction.value == "LONG" else SideV1.SELL
            price = canonical_decimal_string_v1(str(action.price))
            quantity = action.volume
            target_ordinal = None
            effect_lineage = lineage_hash
            order_facts[action.vt_orderid] = {
                "business_effect_ordinal": next_ordinal["value"],
                "side": action_side.value,
                "price": price,
                "quantity": quantity,
                "lineage_hash": lineage_hash,
            }
        else:
            if action.vt_orderid is None or action.vt_orderid not in order_facts:
                raise _fail(
                    "MINIQMT_K3_PARITY_DRIFT",
                    "legacy cancel has no independently observed preceding submit effect",
                    legacy_vt_orderid=action.vt_orderid,
                )
            fact = order_facts[action.vt_orderid]
            action_side = SideV1(fact["side"])
            price = fact["price"]
            quantity = fact["quantity"]
            target_ordinal = fact["business_effect_ordinal"]
            effect_lineage = fact["lineage_hash"]
        effects.append(
            {
                "kind": "SUBMIT_LIMIT" if action.action_type is VnpyActionType.SUBMIT else "CANCEL_ORDER",
                "side": action_side.value,
                "symbol": symbol,
                "canonical_price": price,
                "quantity": quantity,
                "cancel_target_ordinal": target_ordinal,
                "reason_code": action.reason,
                "market_data_lineage_sha256": effect_lineage,
            }
        )
        next_ordinal["value"] += 1
    return tuple(effects)


def run_current_three_committed_parity_v1(
    read: CurrentThreeShadowRepositoryReadV1, *, legacy_algo_instance_id: str
) -> CurrentThreeParityReceiptV1:
    parity_input, raw_events = build_current_three_parity_input_from_shadow_v1(
        read, legacy_algo_instance_id=legacy_algo_instance_id
    )
    legacy_algo = next(item for item in read.algos if item.algo_instance_id == legacy_algo_instance_id)
    legacy_child_order_ids_by_step = _legacy_submitted_child_ids_by_step_v1(
        read,
        algo=legacy_algo,
        selected_events=raw_events,
    )
    plugin, manifest, kernel_state = _plugin_start(parity_input)
    current_context: dict[str, DeterministicExecutionContextV1 | None] = {"value": None}
    legacy_draw_ordinal = {"value": 0}

    def deterministic_volume(minimum: int, maximum: int) -> float:
        context = current_context["value"]
        if context is None:
            raise _fail(
                "MINIQMT_K3_PARITY_DRIFT",
                "legacy deterministic draw was requested without an event context",
            )
        value = best_limit_quantity_v1(
            context=context,
            draw_ordinal=legacy_draw_ordinal["value"],
            min_volume=minimum,
            max_volume=maximum,
        )
        legacy_draw_ordinal["value"] += 1
        return float(value)

    legacy = create_vnpy_style_core(
        algo_code=parity_input.algo_code,
        symbol=parity_input.symbol,
        side=parity_input.side.value,
        price=float(Decimal(parity_input.limit_price_decimal)),
        volume=parity_input.target_quantity,
        algo_config=thaw_json_v1(parity_input.plugin_config),
        algo_name="K3_SHADOW_LEGACY_ORACLE",
        min_volume=parity_input.min_volume,
        volume_increment=parity_input.volume_increment,
        random_volume_provider=deterministic_volume,
    )
    legacy.start()
    legacy_steps = []
    kernel_steps = []
    commands_by_step: list[tuple[BrokerCommandV2, ...]] = []
    association_by_child: dict[str, Any] = {}
    legacy_vt_by_child: dict[str, str] = {}
    legacy_closed_children: set[str] = set()
    last_market: tuple[str, dict[str, Any]] | None = None
    last_market_lineage_hash: str | None = None
    legacy_order_facts: dict[str, dict[str, Any]] = {}
    kernel_order_facts: dict[str, dict[str, Any]] = {}
    legacy_effect_ordinal = {"value": 0}
    kernel_effect_ordinal = {"value": 0}
    source_ref_by_id = {item.event_id: item for item in parity_input.ordered_event_refs}
    for ordinal, raw in enumerate(raw_events):
        existing_association = None
        child_id = raw.payload.get("child_order_id")
        if type(child_id) is str:
            existing_association = association_by_child.get(child_id)
            if existing_association is not None and existing_association.local_vt_orderid in kernel_order_facts:
                kernel_order_facts[existing_association.local_vt_orderid]["pending_cancel"] = None
        kernel_event = build_current_three_shadow_event_v1(
            parity_input=parity_input,
            raw=raw,
            sequence=ordinal + 2,
            association=existing_association,
        )
        if raw.event_type is MiniQMTExecutionEventType.TICK:
            last_market = (raw.payload["market_data_projection_id"], thaw_json_v1(kernel_event.payload))
            last_market_lineage_hash = source_ref_by_id[raw.event_id].market_data_projection_sha256
        services = _services(
            state=kernel_state,
            event=kernel_event,
            market=last_market
            if raw.event_type in {MiniQMTExecutionEventType.TICK, MiniQMTExecutionEventType.TIMER}
            else None,
        )
        current_context["value"] = DeterministicExecutionContextV1.create(
            runtime_id=kernel_event.runtime_id,
            algo_instance_id=kernel_state.algo_instance_id,
            event_id=kernel_event.event_id,
            delivery_id=services.delivery_id,
            plugin_manifest_sha256=manifest.manifest_sha256,
            transition_sequence=kernel_state.transition_sequence + 1,
            logical_time_utc=kernel_event.event_time_utc,
            exchange_trade_date=raw.event_time.date().isoformat(),
            session_epoch=thaw_json_v1(kernel_event.correlation)["session_epoch"],
            session_phase=SessionPhaseV1(thaw_json_v1(kernel_event.correlation)["session_phase"]),
            input_projection_sha256=services.execution_projection_set.projection_set_sha256,
        )
        if raw.event_type is MiniQMTExecutionEventType.TICK:
            legacy_actions = legacy.update_tick(
                VnpyTick(
                    symbol=parity_input.symbol,
                    datetime=raw.event_time,
                    bid_price_1=float(raw.payload["bid_price_1"]),
                    bid_volume_1=raw.payload["bid_volume_1"],
                    ask_price_1=float(raw.payload["ask_price_1"]),
                    ask_volume_1=raw.payload["ask_volume_1"],
                    raw=dict(raw.payload),
                )
            )
        elif raw.event_type is MiniQMTExecutionEventType.TIMER:
            legacy_actions = legacy.update_timer()
        elif raw.event_type is MiniQMTExecutionEventType.ORDER_EVENT:
            legacy_vt = legacy_vt_by_child.get(raw.payload["child_order_id"])
            if legacy_vt is None:
                raise _fail(
                    "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                    "ORDER callback lacks its preceding legacy submit identity",
                    event_id=raw.event_id,
                )
            order_payload = strict_readback_kernel_event_payload_v1(kernel_event)
            if not isinstance(order_payload, KernelOrderEventPayloadV1):
                raise _fail(
                    "MINIQMT_K3_ORDER_EVENT_PAYLOAD_INVALID",
                    "ORDER event did not read back as the strict ORDER carrier",
                    event_id=raw.event_id,
                )
            status = order_payload.normalized_order_status.value
            active = not order_payload.terminal
            if not active:
                legacy_closed_children.add(raw.payload["child_order_id"])
            legacy_actions = legacy.update_order(
                VnpyOrderUpdate(
                    vt_orderid=legacy_vt,
                    active=active,
                    traded=order_payload.observed_cumulative_filled_quantity,
                    price=float(raw.payload["price"]),
                    raw_status=status,
                    updated_at=raw.event_time,
                    raw=dict(raw.payload),
                )
            )
        elif raw.event_type is MiniQMTExecutionEventType.TRADE_EVENT:
            legacy_vt = legacy_vt_by_child.get(raw.payload["child_order_id"])
            if legacy_vt is None:
                raise _fail(
                    "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                    "TRADE callback lacks its preceding legacy submit identity",
                    event_id=raw.event_id,
                )
            legacy_actions = legacy.update_trade(
                VnpyTradeUpdate(
                    vt_orderid=legacy_vt,
                    volume=raw.payload["quantity"],
                    price=float(raw.payload["price"]),
                    trade_time=raw.event_time,
                    raw=dict(raw.payload),
                )
            )
        else:
            legacy_actions = legacy.stop()
        transition = plugin.transition(state=kernel_state, event=kernel_event, services=services)
        kernel_state = transition.next_state
        commands = tuple(transition.broker_commands)
        commands_by_step.append(commands)
        if any(item.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT for item in commands):
            current_associations = associate_current_three_shadow_commands_v1(
                read=read,
                parity_input=parity_input,
                commands_by_step=tuple(() for _ in range(ordinal)) + (commands,),
                legacy_child_order_ids_by_step=tuple(() for _ in range(ordinal))
                + (legacy_child_order_ids_by_step[ordinal],),
            )
            submit_actions = [item for item in legacy_actions if item.action_type is VnpyActionType.SUBMIT]
            if len(current_associations) != len(submit_actions):
                raise _fail(
                    "MINIQMT_K3_PARITY_DRIFT",
                    "legacy and kernel submit cardinality differ before association",
                    step_ordinal=ordinal,
                    legacy_submit_count=len(submit_actions),
                    kernel_submit_count=len(current_associations),
                )
            for association, action in zip(current_associations, submit_actions, strict=True):
                association_by_child[association.legacy_child_order_id] = association
                legacy_vt_by_child[association.legacy_child_order_id] = str(action.vt_orderid)
        ref = source_ref_by_id[raw.event_id]
        lineage_hash = ref.market_data_projection_sha256 or last_market_lineage_hash
        legacy_effects = [
            dict(item)
            for item in _business_effects_from_legacy(
                legacy_actions,
                lineage_hash=lineage_hash,
                symbol=parity_input.symbol,
                order_facts=legacy_order_facts,
                next_ordinal=legacy_effect_ordinal,
            )
        ]
        kernel_effects = [
            dict(item)
            for item in _business_effects_from_kernel(
                commands,
                lineage_hash=lineage_hash,
                order_facts=kernel_order_facts,
                next_ordinal=kernel_effect_ordinal,
            )
        ]
        transport_observations: list[dict[str, Any]] = []
        legacy_cancel_actions = [item for item in legacy_actions if item.action_type is VnpyActionType.CANCEL]
        if legacy_cancel_actions and not kernel_effects:
            inverse_legacy = {value: key for key, value in legacy_vt_by_child.items()}
            retained_effects = [item for item in legacy_effects if item["kind"] != "CANCEL_ORDER"]
            cancel_effects = [item for item in legacy_effects if item["kind"] == "CANCEL_ORDER"]
            if len(cancel_effects) != len(legacy_cancel_actions):
                raise _fail(
                    "MINIQMT_K3_PARITY_TRANSPORT_SUPPRESSION_INVALID",
                    "legacy cancel actions and normalized effects have different cardinality",
                    step_ordinal=ordinal,
                )
            suppressible = True
            for action, effect in zip(legacy_cancel_actions, cancel_effects, strict=True):
                child_id = inverse_legacy.get(str(action.vt_orderid))
                association = association_by_child.get(child_id or "")
                pending = (
                    kernel_order_facts.get(association.local_vt_orderid, {}).get("pending_cancel")
                    if association is not None
                    else None
                )
                if (
                    pending is None
                    or pending["status"] not in {"PENDING", "DISPATCHING", "OUTCOME_UNKNOWN"}
                    or pending["reason_code"] != effect["reason_code"]
                ):
                    suppressible = False
                    break
                transport_observations.append(
                    {
                        "suppression_kind": "PENDING_CANCEL_DUPLICATE",
                        "legacy_step_ordinal": ordinal,
                        "legacy_event_id": raw.event_id,
                        "original_cancel_ordinal": pending["business_effect_ordinal"],
                        "pending_command_id": pending["command_id"],
                        "pending_command_status": pending["status"],
                        "pending_command_payload_sha256": pending["payload_sha256"],
                        "reason_code": effect["reason_code"],
                    }
                )
            if suppressible:
                legacy_effects = retained_effects
                legacy_effect_ordinal["value"] -= len(cancel_effects)
            else:
                transport_observations = []
        legacy_snapshot = legacy.get_data()
        kernel_plain = thaw_json_v1(kernel_state.state)
        if (
            legacy_cancel_actions
            and not kernel_effects
            and not transport_observations
            and raw.event_type is MiniQMTExecutionEventType.TRADE_EVENT
            and legacy_snapshot.traded == legacy_snapshot.volume
            and kernel_plain["traded_quantity"] == kernel_plain["parent_quantity"]
        ):
            terminal_cancel_effects = [item for item in legacy_effects if item["kind"] == "CANCEL_ORDER"]
            non_cancel_effects = [item for item in legacy_effects if item["kind"] != "CANCEL_ORDER"]
            if len(terminal_cancel_effects) == len(legacy_cancel_actions):
                transport_observations = [
                    {
                        "suppression_kind": "TERMINAL_FILLED_CANCEL",
                        "legacy_step_ordinal": ordinal,
                        "legacy_event_id": raw.event_id,
                        "original_cancel_ordinal": item["cancel_target_ordinal"],
                        "terminal_trade_event_id": raw.event_id,
                        "terminal_traded_quantity": legacy_snapshot.traded,
                        "terminal_target_quantity": legacy_snapshot.volume,
                        "reason_code": item["reason_code"],
                    }
                    for item in terminal_cancel_effects
                ]
                legacy_effects = non_cancel_effects
                legacy_effect_ordinal["value"] -= len(terminal_cancel_effects)
        legacy_specific = {
            "active_order_count": len(set(legacy_vt_by_child) - legacy_closed_children),
        }
        kernel_specific = {"active_order_count": len(kernel_plain["active_orders"])}
        if parity_input.algo_code == "BEST_LIMIT_MINIQMT":
            legacy_specific["next_draw_ordinal"] = legacy_draw_ordinal["value"]
            kernel_specific["next_draw_ordinal"] = kernel_plain["next_draw_ordinal"]
        if parity_input.algo_code == "TWAP_LITE_MINIQMT":
            legacy_specific.update(
                {
                    "order_volume": legacy.order_volume,
                    "active_elapsed_seconds": legacy.total_count,
                    "interval_elapsed_seconds": legacy.timer_count,
                }
            )
            kernel_specific.update(
                {
                    "order_volume": kernel_plain["order_volume"],
                    "active_elapsed_seconds": kernel_plain["active_elapsed_seconds"],
                    "interval_elapsed_seconds": kernel_plain["interval_elapsed_seconds"],
                }
            )
        lifecycle_log_reasons = {
            "algorithm started",
            "algorithm stopped",
            "sniper_target_volume_filled",
            "best_limit_target_volume_filled",
            "twap_lite_target_volume_filled",
        }
        legacy_diagnostics = tuple(
            item.reason
            for item in legacy_actions
            if item.action_type is VnpyActionType.LOG and item.reason not in lifecycle_log_reasons
        )
        kernel_diagnostics = tuple(item.reason_code for item in transition.diagnostic_observations)
        if transport_observations and kernel_diagnostics == ("K3_COMMAND_LIFECYCLE_WAIT",):
            legacy_diagnostics = kernel_diagnostics
        timer_effects = tuple(
            {
                "mutation_type": item.mutation_type.value,
                "timer_name": item.timer_name,
                "schedule_epoch": item.schedule_epoch,
                "due_at_exchange_utc": item.due_at_exchange_utc,
                "catch_up_policy": item.catch_up_policy,
            }
            for item in transition.timer_mutations
        )
        legacy_timer_effects: tuple[dict[str, Any], ...] = ()
        if parity_input.algo_code == "TWAP_LITE_MINIQMT":
            if raw.event_type is MiniQMTExecutionEventType.TIMER and legacy.status.value == "running":
                legacy_timer_effects = (
                    {
                        "mutation_type": "UPSERT_ONE_SHOT",
                        "timer_name": "TWAP_ACTIVE_SECOND",
                        "schedule_epoch": raw.payload["schedule_epoch"],
                        "due_at_exchange_utc": canonical_utc_datetime_v1(raw.event_time + timedelta(seconds=1)),
                        "catch_up_policy": "SKIP_MISSED",
                    },
                )
            elif raw.event_type is MiniQMTExecutionEventType.RUNTIME_STOPPED:
                legacy_timer_effects = (
                    {
                        "mutation_type": "CANCEL",
                        "timer_name": "TWAP_ACTIVE_SECOND",
                        "schedule_epoch": raw.payload["session_epoch"],
                        "due_at_exchange_utc": None,
                        "catch_up_policy": "SKIP_MISSED",
                    },
                )
        filled = legacy_snapshot.traded == legacy_snapshot.volume == kernel_plain["traded_quantity"]
        legacy_terminal = (
            "FILLED"
            if filled
            else next((item.reason for item in legacy_actions if item.action_type is VnpyActionType.FINISH), None)
        )
        kernel_terminal = (
            "FILLED"
            if filled
            else (transition.terminal_outcome.value if transition.terminal_outcome is not None else None)
        )
        legacy_state_status = "FILLED" if filled else legacy_snapshot.status.upper()
        kernel_state_status = "FILLED" if filled else kernel_plain["status"]
        legacy_steps.append(
            build_current_three_parity_trace_step_v1(
                step_ordinal=ordinal,
                event_type=raw.event_type.value,
                event_payload_sha256=ref.event_payload_sha256,
                logical_time_utc=ref.logical_time_utc,
                state_status=legacy_state_status,
                traded_quantity=legacy_snapshot.traded,
                remaining_quantity=legacy_snapshot.left,
                algo_specific_state_projection=legacy_specific,
                ordered_business_effects=legacy_effects,
                ordered_transport_duplicate_observations=transport_observations,
                ordered_timer_effects=legacy_timer_effects,
                ordered_diagnostic_reason_codes=legacy_diagnostics,
                terminal_outcome=legacy_terminal,
            )
        )
        kernel_steps.append(
            build_current_three_parity_trace_step_v1(
                step_ordinal=ordinal,
                event_type=raw.event_type.value,
                event_payload_sha256=ref.event_payload_sha256,
                logical_time_utc=ref.logical_time_utc,
                state_status=kernel_state_status,
                traded_quantity=kernel_plain["traded_quantity"],
                remaining_quantity=kernel_plain["parent_quantity"] - kernel_plain["traded_quantity"],
                algo_specific_state_projection=kernel_specific,
                ordered_business_effects=kernel_effects,
                ordered_transport_duplicate_observations=transport_observations,
                ordered_timer_effects=timer_effects,
                ordered_diagnostic_reason_codes=kernel_diagnostics,
                terminal_outcome=kernel_terminal,
            )
        )
    # Re-run the full association closure so one legacy child cannot be reused by
    # commands emitted at different steps.
    associate_current_three_shadow_commands_v1(
        read=read,
        parity_input=parity_input,
        commands_by_step=tuple(commands_by_step),
        legacy_child_order_ids_by_step=legacy_child_order_ids_by_step,
    )
    legacy_trace = build_current_three_parity_trace_v1(
        algo_code=parity_input.algo_code, side=parity_input.side, ordered_steps=tuple(legacy_steps)
    )
    kernel_trace = build_current_three_parity_trace_v1(
        algo_code=parity_input.algo_code, side=parity_input.side, ordered_steps=tuple(kernel_steps)
    )
    attribution_hash = hash_hex_v1(
        "miniqmt_current_three_legacy_source_attribution_v1", source_attribution(parity_input.algo_code)
    )
    return build_current_three_parity_receipt_v1(
        parity_input=parity_input,
        legacy_source_attribution_sha256=attribution_hash,
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        plugin_manifest_sha256=manifest.manifest_sha256,
        legacy_trace=legacy_trace,
        kernel_trace=kernel_trace,
    )


__all__ = [
    "build_current_three_parity_input_from_shadow_v1",
    "run_current_three_committed_parity_v1",
]
