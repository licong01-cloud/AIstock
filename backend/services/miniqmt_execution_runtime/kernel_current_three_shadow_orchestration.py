"""Strict K3-B builders for broker-neutral K2 durable shadow orchestration."""

from __future__ import annotations

from typing import Any, Sequence

from .kernel_current_three_contracts import CurrentThreeContractError, CurrentThreeParityInputV1
from .kernel_current_three_shadow_source import CurrentThreeShadowRepositoryReadV1
from .kernel_delivery import KernelAlgoCreationRequestV1, KernelDeliveryExecutionInputV1
from .plugin_canonical import hash_hex_v1, thaw_json_v1
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoReadOnlyServicesV1,
    AlgoStateSnapshotV2,
    ConsumedLineageRefV1,
    ConsumedLineageTypeV1,
    DeterministicExecutionContextV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    GatewayCapabilityCatalogV1,
    KernelCommandLifecycleProjectionV1,
    KernelProjectionTypeV1,
    MiniQMTRiskDecisionReceiptV1,
    OMSPreflightDecisionV1,
    OMSPreflightProjectionReceiptV1,
    RiskDecisionActionV1,
    RiskDecisionStageV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
)
from .plugin_registry import PluginRouteCompatibilityReceiptV1


def _fail(reason_code: str, message: str, **context: Any) -> CurrentThreeContractError:
    return CurrentThreeContractError(
        reason_code,
        message,
        context={"stage": "K3_DURABLE_SHADOW_ORCHESTRATION", **context},
    )


def _ref(
    *,
    projection_type: KernelProjectionTypeV1,
    projection_id: str,
    projection_version: str,
    payload_sha256: str,
    source_event_id: str | None,
    logical_at_utc: Any,
) -> ExecutionProjectionRefV1:
    return ExecutionProjectionRefV1.create(
        projection_type=projection_type,
        projection_id=projection_id,
        projection_version=projection_version,
        payload_sha256=payload_sha256,
        source_event_id=source_event_id,
        logical_at_utc=logical_at_utc,
    )


def build_current_three_shadow_creation_request_v1(
    *,
    read: CurrentThreeShadowRepositoryReadV1,
    parity_input: CurrentThreeParityInputV1,
    gateway_catalog: GatewayCapabilityCatalogV1,
) -> KernelAlgoCreationRequestV1:
    """Build one deterministic K2 ALGO_START request from committed legacy facts."""

    read.strict_readback_v1()
    if not isinstance(parity_input, CurrentThreeParityInputV1):
        raise TypeError("parity_input must be CurrentThreeParityInputV1")
    if not isinstance(gateway_catalog, GatewayCapabilityCatalogV1):
        raise TypeError("gateway_catalog must be GatewayCapabilityCatalogV1")
    algos = [
        item
        for item in read.algos
        if item.parent_intent_id == parity_input.parent_intent_id
        and item.strategy_slot_id == parity_input.strategy_slot_id
        and item.algo_code == parity_input.algo_code
    ]
    if len(algos) != 1:
        raise _fail(
            "MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
            "shadow creation request does not close to one legacy algo",
            match_count=len(algos),
        )
    first_event = parity_input.ordered_event_refs[0]
    first_raw = next(item for item in read.events if item.event_id == first_event.event_id)
    session_epoch = first_raw.payload.get("session_epoch", first_raw.payload.get("schedule_epoch"))
    session_phase = first_raw.payload.get("session_phase")
    if type(session_epoch) is not str or type(session_phase) is not str:
        raise _fail(
            "MINIQMT_K3_EVENT_PAYLOAD_INVALID",
            "shadow creation request lacks exact exchange-session authority",
            event_id=first_raw.event_id,
        )
    contract = {
        "symbol": parity_input.symbol,
        "pricetick_decimal": parity_input.pricetick_decimal,
        "min_volume": parity_input.min_volume,
        "volume_increment": parity_input.volume_increment,
        "legacy_algo_fact_set_sha256": read.snapshot.algo_set_sha256,
    }
    account = {
        "account_group_id": read.runtime.account_group_id,
        "legacy_runtime_id": read.runtime.runtime_id,
        "legacy_runtime_fact_sha256": read.snapshot.source_set_sha256,
        "observation_only": True,
    }
    capability = gateway_catalog.model_dump(mode="json")
    contract_hash = hash_hex_v1("miniqmt_contract_projection_v1", contract)
    account_hash = hash_hex_v1("miniqmt_account_projection_v1", account)
    capability_hash = hash_hex_v1("miniqmt_market_capability_projection_v1", capability)
    refs = tuple(
        sorted(
            (
                _ref(
                    projection_type=KernelProjectionTypeV1.CONTRACT,
                    projection_id="mqshadowcontract_" + contract_hash[:32],
                    projection_version="miniqmt_contract_projection_v1",
                    payload_sha256=contract_hash,
                    source_event_id=None,
                    logical_at_utc=first_event.logical_time_utc,
                ),
                _ref(
                    projection_type=KernelProjectionTypeV1.ACCOUNT,
                    projection_id="mqshadowaccount_" + account_hash[:32],
                    projection_version="miniqmt_account_projection_v1",
                    payload_sha256=account_hash,
                    source_event_id=None,
                    logical_at_utc=first_event.logical_time_utc,
                ),
                _ref(
                    projection_type=KernelProjectionTypeV1.MARKET_CAPABILITY,
                    projection_id="mqshadowcapability_" + capability_hash[:32],
                    projection_version="miniqmt_market_capability_projection_v1",
                    payload_sha256=capability_hash,
                    source_event_id=None,
                    logical_at_utc=first_event.logical_time_utc,
                ),
            ),
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    authority = {
        "parity_input_sha256": parity_input.input_sha256,
        "legacy_runtime_id": read.runtime.runtime_id,
        "legacy_algo_instance_id": algos[0].algo_instance_id,
        "source_event_set_sha256": read.snapshot.event_set_sha256,
        "source_child_set_sha256": read.snapshot.child_set_sha256,
    }
    return KernelAlgoCreationRequestV1(
        runtime_id=parity_input.runtime_id,
        parent_intent_id=parity_input.parent_intent_id,
        strategy_slot_id=parity_input.strategy_slot_id,
        symbol=parity_input.symbol,
        side=parity_input.side,
        limit_price_decimal=parity_input.limit_price_decimal,
        parent_quantity=parity_input.target_quantity,
        min_volume=parity_input.min_volume,
        volume_increment=parity_input.volume_increment,
        algo_code=parity_input.algo_code,
        plugin_config=thaw_json_v1(parity_input.plugin_config),
        plugin_config_sha256=parity_input.plugin_config_sha256,
        contract_projection=contract,
        contract_projection_sha256=contract_hash,
        account_projection=account,
        account_projection_sha256=account_hash,
        market_capability_projection=capability,
        market_capability_projection_sha256=capability_hash,
        projection_refs=refs,
        execution_plan_id="mqshadowplan_" + parity_input.input_sha256[:32],
        execution_plan_sha256=hash_hex_v1("miniqmt_current_three_shadow_execution_plan_v1", authority),
        release_id="mqshadowrelease_" + parity_input.input_sha256[:32],
        release_sha256=hash_hex_v1("miniqmt_current_three_shadow_release_v1", authority),
        policy_id="mqshadowpolicy_" + parity_input.plugin_config_sha256[:32],
        policy_sha256=hash_hex_v1(
            "miniqmt_current_three_shadow_policy_v1",
            {"algo_code": parity_input.algo_code, "plugin_config_sha256": parity_input.plugin_config_sha256},
        ),
        logical_time_utc=first_event.logical_time_utc,
        exchange_trade_date=read.snapshot.trade_date,
        session_epoch=session_epoch,
        session_phase=SessionPhaseV1(session_phase),
    )


def build_current_three_shadow_delivery_input_v1(
    *,
    read: CurrentThreeShadowRepositoryReadV1,
    parity_input: CurrentThreeParityInputV1,
    event: RuntimeEventEnvelopeV2,
    delivery: AlgoDeliveryPersistenceV1,
    algo: ExecutionAlgoInstancePersistenceV2,
    previous_state: AlgoStateSnapshotV2,
    lifecycle_projection: KernelCommandLifecycleProjectionV1,
    route_receipt: PluginRouteCompatibilityReceiptV1,
    expected_legacy_child_order_ids: Sequence[str] = (),
) -> KernelDeliveryExecutionInputV1:
    """Build exact read-only inputs; historical child facts never authorize a live route."""

    read.strict_readback_v1()
    child_ids = tuple(expected_legacy_child_order_ids)
    if len(child_ids) != len(set(child_ids)) or len(child_ids) > 1:
        raise _fail(
            "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
            "one shadow delivery can reference at most one unique historical child candidate",
            child_order_ids=list(child_ids),
        )
    child = None
    if child_ids:
        matches = [item for item in read.children if item.child_order_id == child_ids[0]]
        if len(matches) != 1 or matches[0].algo_instance_id not in {item.algo_instance_id for item in read.algos}:
            raise _fail(
                "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                "shadow command authority does not close to one committed legacy child",
                child_order_id=child_ids[0],
                match_count=len(matches),
            )
        child = matches[0]
    refs: list[ExecutionProjectionRefV1] = []
    market_id = None
    market_payload = None
    source_identity = thaw_json_v1(event.source_identity)
    if event.event_type.value == "TICK":
        market_id = source_identity["market_data_id"]
        market_payload = thaw_json_v1(event.payload)
        market_hash = hash_hex_v1("miniqmt_market_data_projection_v2", market_payload)
        refs.append(
            _ref(
                projection_type=KernelProjectionTypeV1.MARKET_DATA,
                projection_id=market_id,
                projection_version="miniqmt_market_data_projection_v2",
                payload_sha256=market_hash,
                source_event_id=event.event_id,
                logical_at_utc=event.event_time_utc,
            )
        )
    if child is not None:
        child_ref = next(
            item for item in read.snapshot.ordered_child_fact_refs if item.identity == child.child_order_id
        )
        historical = {
            "parity_input_sha256": parity_input.input_sha256,
            "legacy_child_order_id": child.child_order_id,
            "legacy_child_payload_sha256": child_ref.payload_sha256,
            "observation_only": True,
        }
        account_hash = hash_hex_v1("miniqmt_current_three_shadow_account_fact_v1", historical)
        oms = OMSPreflightProjectionReceiptV1.create(
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            parent_intent_id=algo.parent_intent_id,
            child_order_id="mqshadowchild_" + child_ref.payload_sha256[:32],
            order_intent_id="mqshadowintent_" + child_ref.payload_sha256[:32],
            strategy_slot_id=algo.strategy_slot_id,
            account_projection_sha256=account_hash,
            cash_fact_sha256=hash_hex_v1("miniqmt_current_three_shadow_cash_fact_v1", historical),
            lot_fact_sha256=hash_hex_v1("miniqmt_current_three_shadow_lot_fact_v1", historical),
            open_order_fact_sha256=hash_hex_v1("miniqmt_current_three_shadow_open_order_fact_v1", historical),
            decision=OMSPreflightDecisionV1.PASS,
            reason_code="K3_SHADOW_HISTORICAL_CHILD_COMMITTED",
            logical_at_utc=event.event_time_utc,
        )
        risk = MiniQMTRiskDecisionReceiptV1.create(
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            event_id=event.event_id,
            child_order_id=oms.child_order_id,
            decision_stage=RiskDecisionStageV1.PRE_SUBMIT,
            action=RiskDecisionActionV1.PASS,
            reason_code="K3_SHADOW_HISTORICAL_CHILD_COMMITTED",
            reason="historical committed child proves that the legacy effect passed its original runtime path",
            metadata=historical,
            logical_at_utc=event.event_time_utc,
        )
        kill_payload = {**historical, "active": False, "event_id": event.event_id}
        kill_hash = hash_hex_v1("miniqmt_kill_switch_state_v1", kill_payload)
        refs.extend(
            (
                _ref(
                    projection_type=KernelProjectionTypeV1.OMS_PREFLIGHT,
                    projection_id=oms.receipt_id,
                    projection_version=oms.schema_version,
                    payload_sha256=oms.receipt_sha256,
                    source_event_id=event.event_id,
                    logical_at_utc=event.event_time_utc,
                ),
                _ref(
                    projection_type=KernelProjectionTypeV1.RISK_DECISION,
                    projection_id=risk.decision_id,
                    projection_version=risk.schema_version,
                    payload_sha256=risk.receipt_sha256,
                    source_event_id=event.event_id,
                    logical_at_utc=event.event_time_utc,
                ),
                _ref(
                    projection_type=KernelProjectionTypeV1.KILL_SWITCH_STATE,
                    projection_id="mqkillswitch_" + kill_hash[:32],
                    projection_version="miniqmt_kill_switch_state_v1",
                    payload_sha256=kill_hash,
                    source_event_id=event.event_id,
                    logical_at_utc=event.event_time_utc,
                ),
                _ref(
                    projection_type=KernelProjectionTypeV1.ROUTE_COMPATIBILITY,
                    projection_id="mqroutecompat_" + route_receipt.receipt_sha256,
                    projection_version=route_receipt.schema_version,
                    payload_sha256=route_receipt.receipt_sha256,
                    source_event_id=event.event_id,
                    logical_at_utc=event.event_time_utc,
                ),
            )
        )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        projection_refs=tuple(sorted(refs, key=lambda item: (item.projection_type.value, item.projection_id))),
    )
    correlation = thaw_json_v1(event.correlation)
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        transition_sequence=previous_state.transition_sequence + 1,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date=correlation["exchange_trade_date"],
        session_epoch=correlation["session_epoch"],
        session_phase=SessionPhaseV1(correlation["session_phase"]),
        input_projection_sha256=projection_set.projection_set_sha256,
    )
    lineages = [
        ConsumedLineageRefV1.create(
            lineage_type=ConsumedLineageTypeV1.EVENT,
            identity=event.event_id,
            payload_sha256=event.payload_sha256,
        )
    ]
    if market_id is not None:
        market_ref = next(item for item in refs if item.projection_type is KernelProjectionTypeV1.MARKET_DATA)
        lineages.append(
            ConsumedLineageRefV1.create(
                lineage_type=ConsumedLineageTypeV1.MARKET_DATA,
                identity=market_id,
                payload_sha256=market_ref.payload_sha256,
            )
        )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id=market_id,
        market_data_projection=market_payload,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    return KernelDeliveryExecutionInputV1(
        services=services,
        deterministic_context=context,
        consumed_lineage_refs=tuple(lineages),
        command_lifecycle_projection=lifecycle_projection,
    )


__all__ = [
    "build_current_three_shadow_creation_request_v1",
    "build_current_three_shadow_delivery_input_v1",
]
