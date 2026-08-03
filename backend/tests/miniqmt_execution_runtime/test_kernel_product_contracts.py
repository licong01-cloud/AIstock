from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    DependentBuyCandidateAuthorityV2,
    DependentBuyCoordinationStatusV1,
    DependentBuyCoordinationV1,
    DependentBuyCoordinationV2,
    DependentBuyDecisionV1,
    DependentBuyDependencyStatusV1,
    DependentBuyLedgerObservationV1,
    DependentBuyLedgerObservationV2,
    DependentBuyReleaseDecisionV1,
    DependentBuyReleaseDecisionV2,
    DependentBuySettledProceedsRefV2,
    DependentBuySellDependencyV1,
    DependentBuySellDependencyV2,
    DependentBuyTriggerEventRefV1,
    DependentBuyTriggerTypeV1,
    KernelProductContractError,
    ProductCommandAggregateDispositionV2,
    ProductCommandAggregateDispositionV3,
    ProductCommandAuthorityItemV2,
    ProductCommandAuthorityItemV3,
    ProductCommandAuthoritySetV2,
    ProductCommandAuthoritySetV3,
    ProductCommandChildMappingStatusV1,
    ProductCommandChildMappingV1,
    ProductCommandDispositionV2,
    ProductCommandDispositionV3,
    ProductCommandEvaluationEvidenceV3,
    ProductCommandLifecycleProjectionItemV2,
    ProductCommandLifecycleProjectionV2,
    ProductCommandLifecycleProjectionItemV3,
    ProductCommandLifecycleProjectionV3,
    ProductLifecycleStatusV2,
    ProductLifecycleStatusV3,
    ProductMaterializationReceiptV2,
    ProductMaterializationReceiptV3,
    ProductRouteCutoverReceiptV1,
    ProductRouteOwnerV1,
    ProductRouteOwnerKindV1,
    hash_hex_v1,
    validate_kernel_product_payload_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    BrokerCommandTypeV2,
    BrokerCommandV2,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    KernelProjectionTypeV1,
    MiniQMTRiskDecisionReceiptV1,
    OMSPreflightDecisionV1,
    OMSPreflightProjectionReceiptV1,
    OrderTypeV1,
    RiskDecisionActionV1,
    RiskDecisionStageV1,
    SideV1,
    command_child_mapping_id_v1,
    execution_child_order_id_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    CompatibilityStatusV1,
    PluginKeyV1,
    PluginRouteCompatibilityReceiptV1,
)


NOW = datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc)


def _sha(char: str) -> str:
    return char * 64


def _trigger(*, sequence: int = 7, event_id: str = "event_sell_trade") -> DependentBuyTriggerEventRefV1:
    return DependentBuyTriggerEventRefV1.create(
        runtime_id="runtime_k6",
        event_id=event_id,
        event_type=DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED,
        event_sequence=sequence,
        source_fact_type="qmt_strategy.trade_ledger",
        source_fact_id=f"trade_{sequence}",
        source_fact_sha256=_sha("a"),
        observed_at_utc=NOW,
    )


def _dependency() -> DependentBuySellDependencyV1:
    return DependentBuySellDependencyV1.create(
        runtime_id="runtime_k6",
        strategy_id="strategy_k6",
        sell_parent_intent_id="intent_sell",
        sell_algo_instance_id="algo_sell",
        latest_order_fact_ref=_sha("b"),
        settled_trade_fact_refs=(_sha("c"),),
        settled_cash_ledger_refs=(_sha("d"),),
        dependency_status=DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
    )


def _ledger(*, available_cash: str = "1000", required_cash: str = "800") -> DependentBuyLedgerObservationV1:
    return DependentBuyLedgerObservationV1.create(
        runtime_id="runtime_k6",
        strategy_id="strategy_k6",
        trade_date="2026-08-01",
        virtual_account_id="account_k6",
        ledger_row_version=9,
        ledger_as_of_utc=NOW,
        available_cash=available_cash,
        required_cash=required_cash,
        ordered_settled_trade_refs=(_sha("c"),),
        ordered_cash_ledger_refs=(_sha("d"),),
        freshness_session_authority_sha256=_sha("e"),
    )


def _coordination(*, status: DependentBuyCoordinationStatusV1) -> DependentBuyCoordinationV1:
    release = status is DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX
    return DependentBuyCoordinationV1.create(
        runtime_id="runtime_k6",
        binding_id="binding_k6",
        trade_date="2026-08-01",
        strategy_id="strategy_k6",
        buy_algo_instance_id="algo_buy",
        buy_parent_intent_id="intent_buy",
        required_cash="800",
        release_command_payload_sha256=_sha("f"),
        ordered_sell_dependencies=(_dependency(),),
        status=status,
        decision_sequence=1 if release else 0,
        last_decision_sha256=_sha("1") if release else None,
        released_command_id="command_buy" if release else None,
        released_outbox_id="command_buy" if release else None,
        row_version=2 if release else 1,
        created_at_utc=NOW,
        updated_at_utc=NOW,
    )


def _authority_item(*, ordinal: int, command_id: str) -> ProductCommandAuthorityItemV2:
    return ProductCommandAuthorityItemV2.create(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        effect_ordinal=ordinal,
        command_id=command_id,
        command_type="SUBMIT_LIMIT",
        command_payload_sha256=_sha("2"),
        plugin_effect_sha256=_sha("3"),
        execution_projection_set_sha256=_sha("4"),
        oms_preflight_receipt_sha256=_sha("5"),
        risk_decision_receipt_sha256=_sha("6"),
        route_compatibility_receipt_sha256=_sha("7"),
        market_data_projection_sha256=_sha("8"),
        account_projection_sha256=_sha("9"),
        contract_projection_sha256=_sha("a"),
        disposition=ProductCommandDispositionV2.MATERIALIZE,
        mapping_id=f"mapping_{command_id}",
        outbox_id=command_id,
        child_order_id=f"child_{command_id}",
    )


def _proceeds_ref(*, broker_trade_id: str = "trade_1", sequence: int = 11) -> DependentBuySettledProceedsRefV2:
    return DependentBuySettledProceedsRefV2.create(
        broker_trade_id=broker_trade_id,
        qmt_trade_ledger_id=f"ledger_{broker_trade_id}",
        qmt_trade_fact_sha256=_sha("1"),
        cash_ledger_id=f"cash_{sequence}",
        cash_ledger_sequence=sequence,
        cash_ledger_fact_sha256=_sha("2"),
        strategy_id="strategy_k6",
        runtime_id="runtime_k6",
        trade_date="2026-08-01",
        sell_parent_intent_id="intent_sell",
    )


def _dependency_v2() -> DependentBuySellDependencyV2:
    return DependentBuySellDependencyV2.create(
        runtime_id="runtime_k6",
        strategy_id="strategy_k6",
        sell_parent_intent_id="intent_sell",
        sell_algo_instance_id="algo_sell",
        latest_order_fact_id="order_fact_sell",
        latest_order_fact_sha256=_sha("3"),
        ordered_settled_proceeds_refs=(_proceeds_ref(),),
        dependency_status=DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
    )


def _coordination_v2(*, row_version: int = 1, release_command_id: str = "command_buy") -> DependentBuyCoordinationV2:
    return DependentBuyCoordinationV2.create(
        runtime_id="runtime_k6",
        binding_id="binding_k6",
        trade_date="2026-08-01",
        strategy_id="strategy_k6",
        buy_algo_instance_id="algo_k6",
        buy_parent_intent_id="intent_buy",
        required_cash="1050",
        release_command_id=release_command_id,
        release_transition_id="transition_k6",
        release_command_authority_item_sha256=_sha("e"),
        release_command_payload_sha256=_sha("f"),
        ordered_sell_dependencies=(_dependency_v2(),),
        status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS,
        decision_sequence=0,
        last_decision_sha256=None,
        released_command_id=None,
        released_outbox_id=None,
        row_version=row_version,
        lease_worker_id=None,
        lease_process_incarnation_id=None,
        lease_epoch=0,
        lease_expires_at_utc=None,
        created_at_utc=NOW,
        updated_at_utc=NOW,
    )


def _decision_v2(
    *,
    decision: DependentBuyDecisionV1 = DependentBuyDecisionV1.WAIT,
    decision_sequence: int = 1,
) -> DependentBuyReleaseDecisionV2:
    release = decision is DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX
    return DependentBuyReleaseDecisionV2.create(
        coordination_id=_coordination_v2().coordination_id,
        decision_sequence=decision_sequence,
        previous_decision_sha256=None if decision_sequence == 1 else _sha("a"),
        trigger_ref_sha256=_sha("b"),
        decision=decision,
        reason_code="MINIQMT_K6_DEPENDENT_BUY_WAIT",
        ledger_observation_sha256=_sha("c"),
        ordered_dependency_sha256s=(_dependency_v2().dependency_sha256,),
        release_event_id="event_release" if release else None,
        release_transition_id="transition_k6" if release else None,
        release_command_authority_set_sha256=_sha("d") if release else None,
        decided_at_utc=NOW,
        worker_id="worker_k6",
        process_incarnation_id="process_k6",
        lease_epoch=1,
    )


def _payload_with(model: object, **updates: object) -> dict[str, object]:
    payload = model.model_dump()  # type: ignore[attr-defined]
    payload.update(updates)
    return payload


def _route_receipt() -> PluginRouteCompatibilityReceiptV1:
    plugin_key = PluginKeyV1(plugin_id="twap", plugin_version="1", manifest_sha256=_sha("4"))
    payload = {
        "schema_version": "plugin_route_compatibility_receipt_v1",
        "plugin_key": plugin_key,
        "algo_code": "TWAP",
        "plugin_manifest_sha256": plugin_key.manifest_sha256,
        "catalog_sha256": _sha("5"),
        "gateway_capability_catalog_sha256": _sha("6"),
        "gateway_route_id": "gateway_sim",
        "required_facade_methods": (),
        "required_gateway_backends": ("QMT",),
        "observed_gateway_backend": "QMT",
        "required_quote_source": "B0_QUOTE_V2",
        "observed_quote_source": "B0_QUOTE_V2",
        "requires_exact_order_id_cancel": False,
        "observed_exact_order_id_cancel": False,
        "observed_idempotent_submit_by_client_ref": False,
        "required_order_types": ("LIMIT",),
        "supported_order_types": ("LIMIT",),
        "required_market_capabilities": {},
        "supported_market_capabilities": (),
        "observed_session_phases": ("CONTINUOUS",),
        "status": CompatibilityStatusV1.PASSED,
        "ordered_failures": (),
        "broker_called": False,
    }
    return PluginRouteCompatibilityReceiptV1(
        **payload,
        receipt_sha256=hash_hex_v1("miniqmt_plugin_route_compatibility_receipt_v1", payload),
    )


def _v3_command(*, ordinal: int = 0, metadata: dict[str, object] | None = None) -> BrokerCommandV2:
    return BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        parent_intent_id="intent_buy",
        transition_id="transition_k6",
        ordinal=ordinal,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.5",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="PLUGIN_SUBMIT",
        metadata={} if metadata is None else metadata,
    )


def _v3_evidence(*, command: BrokerCommandV2, dependent: bool) -> ProductCommandEvaluationEvidenceV3:
    route = _route_receipt()
    oms = OMSPreflightProjectionReceiptV1.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        parent_intent_id=command.parent_intent_id,
        child_order_id="child_k6",
        order_intent_id="order_intent_k6",
        strategy_slot_id="slot_k6",
        account_projection_sha256=_sha("7"),
        cash_fact_sha256=_sha("8"),
        lot_fact_sha256=_sha("9"),
        open_order_fact_sha256=_sha("a"),
        decision=OMSPreflightDecisionV1.REJECT if dependent else OMSPreflightDecisionV1.PASS,
        reason_code="SELL_PROCEEDS_REQUIRED" if dependent else "OMS_PASS",
        logical_at_utc=NOW,
    )
    projection_oms = OMSPreflightProjectionReceiptV1.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        parent_intent_id=command.parent_intent_id,
        child_order_id="child_k6",
        order_intent_id="order_intent_k6",
        strategy_slot_id="slot_k6",
        account_projection_sha256=_sha("7"),
        cash_fact_sha256=_sha("8"),
        lot_fact_sha256=_sha("9"),
        open_order_fact_sha256=_sha("a"),
        decision=OMSPreflightDecisionV1.PASS,
        reason_code="OMS_PASS",
        logical_at_utc=NOW,
    )
    risk = MiniQMTRiskDecisionReceiptV1.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        event_id="event_k6",
        child_order_id=oms.child_order_id,
        decision_stage=RiskDecisionStageV1.PRE_SUBMIT,
        action=RiskDecisionActionV1.PASS,
        reason_code="RISK_PASS",
        reason="risk passed",
        metadata={},
        logical_at_utc=NOW,
    )
    projections = {
        KernelProjectionTypeV1.CONTRACT: ("contract_k6", "miniqmt_contract_projection_v1", {"symbol": "600000.SH"}),
        KernelProjectionTypeV1.MARKET_DATA: (
            "market_k6",
            "miniqmt_market_data_projection_v2",
            {"bid_price_1": "10.49", "ask_price_1": "10.5"},
        ),
        KernelProjectionTypeV1.ACCOUNT: ("account_k6", "miniqmt_account_projection_v1", {"cash": "1000"}),
        KernelProjectionTypeV1.KILL_SWITCH_STATE: (
            "mqkillswitch_k6",
            "miniqmt_kill_switch_state_v1",
            {"active": False},
        ),
    }
    domain_by_type = {
        KernelProjectionTypeV1.CONTRACT: "miniqmt_contract_projection_v1",
        KernelProjectionTypeV1.MARKET_DATA: "miniqmt_market_data_projection_v2",
        KernelProjectionTypeV1.ACCOUNT: "miniqmt_account_projection_v1",
        KernelProjectionTypeV1.KILL_SWITCH_STATE: "miniqmt_kill_switch_state_v1",
    }
    refs = [
        ExecutionProjectionRefV1.create(
            projection_type=projection_type,
            projection_id=identity,
            projection_version=version,
            payload_sha256=hash_hex_v1(domain_by_type[projection_type], payload),
            source_event_id="event_k6",
            logical_at_utc=NOW,
        )
        for projection_type, (identity, version, payload) in projections.items()
    ]
    refs.extend(
        (
            ExecutionProjectionRefV1.create(
                projection_type=KernelProjectionTypeV1.OMS_PREFLIGHT,
                projection_id=projection_oms.receipt_id,
                projection_version=projection_oms.schema_version,
                payload_sha256=projection_oms.receipt_sha256,
                source_event_id="event_k6",
                logical_at_utc=NOW,
            ),
            ExecutionProjectionRefV1.create(
                projection_type=KernelProjectionTypeV1.RISK_DECISION,
                projection_id=risk.decision_id,
                projection_version=risk.schema_version,
                payload_sha256=risk.receipt_sha256,
                source_event_id="event_k6",
                logical_at_utc=NOW,
            ),
            ExecutionProjectionRefV1.create(
                projection_type=KernelProjectionTypeV1.ROUTE_COMPATIBILITY,
                projection_id="mqroutecompat_" + route.receipt_sha256,
                projection_version=route.schema_version,
                payload_sha256=route.receipt_sha256,
                source_event_id="event_k6",
                logical_at_utc=NOW,
            ),
        )
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        event_id="event_k6",
        delivery_id="delivery_k6",
        projection_refs=tuple(sorted(refs, key=lambda item: (item.projection_type.value, item.projection_id))),
    )
    candidate = None
    if dependent:
        candidate = DependentBuyCandidateAuthorityV2.create(
            runtime_id=command.runtime_id,
            binding_id="binding_k6",
            trade_date="2026-08-01",
            strategy_id="strategy_k6",
            buy_algo_instance_id=command.algo_instance_id,
            buy_parent_intent_id=command.parent_intent_id,
            command_id=command.command_id,
            execution_plan_id="plan_k6",
            execution_plan_sha256=_sha("b"),
            plan_parent_relation_sha256=_sha("c"),
            required_cash="1050",
            virtual_account_id="account_k6",
            session_authority_sha256=_sha("d"),
            ordered_sell_dependencies=(_dependency_v2(),),
            oms_preflight_receipt_id=oms.receipt_id,
            oms_preflight_receipt_sha256=oms.receipt_sha256,
            ordered_error_codes=("SELL_PROCEEDS_REQUIRED",),
        )
    return ProductCommandEvaluationEvidenceV3.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id=command.transition_id,
        effect_ordinal=command.ordinal,
        command_id=command.command_id,
        oms_preflight_receipt=oms,
        mini_qmt_risk_decision_receipt=risk,
        plugin_route_compatibility_receipt=route,
        market_data_projection=projections[KernelProjectionTypeV1.MARKET_DATA][2],
        account_projection=projections[KernelProjectionTypeV1.ACCOUNT][2],
        contract_projection=projections[KernelProjectionTypeV1.CONTRACT][2],
        kill_switch_state=projections[KernelProjectionTypeV1.KILL_SWITCH_STATE][2],
        execution_projection_set=projection_set,
        dependent_buy_candidate=candidate,
    )


def _v3_item(*, disposition: ProductCommandDispositionV3, ordinal: int = 0) -> ProductCommandAuthorityItemV3:
    command = _v3_command(ordinal=ordinal)
    evidence = _v3_evidence(
        command=command,
        dependent=disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY,
    )
    coordination_id = None
    if disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY:
        coordination_id = hash_hex_v1(
            "miniqmt_dependent_buy_coordination_id_v2",
            {
                "runtime_id": command.runtime_id,
                "buy_algo_instance_id": command.algo_instance_id,
                "buy_parent_intent_id": command.parent_intent_id,
                "strategy_id": "strategy_k6",
                "trade_date": "2026-08-01",
            },
        )
    rejected = disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS
    child_order_id = execution_child_order_id_v1(
        command_id=command.command_id,
        local_vt_orderid=command.local_vt_orderid,
    )
    mapping_id = command_child_mapping_id_v1(
        command_id=command.command_id,
        local_vt_orderid=command.local_vt_orderid,
        child_order_id=child_order_id,
    )
    return ProductCommandAuthorityItemV3.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id=command.transition_id,
        effect_ordinal=ordinal,
        command_json=command,
        evaluation_evidence=evidence,
        plugin_effect_sha256=_sha("e"),
        disposition=disposition,
        reject_reason_code="OMS_REJECT" if rejected else None,
        reject_context_sha256=_sha("f") if rejected else None,
        coordination_id=coordination_id,
        mapping_id=mapping_id,
        outbox_id=None if disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY else command.command_id,
        child_order_id=child_order_id,
    )


def _recreate_v3_item(
    item: ProductCommandAuthorityItemV3,
    **updates: object,
) -> ProductCommandAuthorityItemV3:
    values: dict[str, object] = {
        "runtime_id": item.runtime_id,
        "algo_instance_id": item.algo_instance_id,
        "event_id": item.event_id,
        "delivery_id": item.delivery_id,
        "transition_id": item.transition_id,
        "effect_ordinal": item.effect_ordinal,
        "command_json": item.command_json,
        "evaluation_evidence": item.evaluation_evidence,
        "plugin_effect_sha256": item.plugin_effect_sha256,
        "disposition": item.disposition,
        "reject_reason_code": item.reject_reason_code,
        "reject_context_sha256": item.reject_context_sha256,
        "coordination_id": item.coordination_id,
        "mapping_id": item.mapping_id,
        "outbox_id": item.outbox_id,
        "child_order_id": item.child_order_id,
    }
    values.update(updates)
    return ProductCommandAuthorityItemV3.create(**values)


def _v3_authority(items: tuple[ProductCommandAuthorityItemV3, ...]) -> ProductCommandAuthoritySetV3:
    projection_sha256 = items[0].execution_projection_set_sha256 if items else _sha("9")
    return ProductCommandAuthoritySetV3.create(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        catalog_sha256=_sha("1"),
        creation_binding_sha256=_sha("2"),
        facade_conformance_set_sha256=_sha("3"),
        execution_projection_set_sha256=projection_sha256,
        transition_receipt_sha256=_sha("4"),
        ordered_items=items,
    )


def test_dependent_buy_contracts_are_strict_hash_closed_and_round_trip() -> None:
    trigger = _trigger()
    dependency = _dependency()
    ledger = _ledger()
    coordination = _coordination(status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS)
    decision = DependentBuyReleaseDecisionV1.create(
        coordination_id=coordination.coordination_id,
        decision_sequence=1,
        previous_decision_sha256=None,
        decision=DependentBuyDecisionV1.WAIT,
        reason_code="MINIQMT_K6_COORDINATION_CASH_STILL_INSUFFICIENT",
        ledger_observation_sha256=ledger.observation_sha256,
        ordered_dependency_sha256s=(dependency.dependency_sha256,),
        trigger_ref_sha256=trigger.trigger_ref_sha256,
        decided_at_utc=NOW,
        worker_id="worker_k6",
        process_incarnation_id="process_k6",
        lease_epoch=1,
    )

    for carrier in (trigger, dependency, ledger, coordination, decision):
        assert type(carrier).model_validate_json(carrier.model_dump_json()) == carrier
    assert ledger.cash_shortfall == "0"
    assert coordination.ordered_sell_dependencies == (dependency,)


def test_dependent_buy_contracts_reject_drift_and_impossible_durable_state() -> None:
    with pytest.raises(ValidationError, match="cash_shortfall"):
        DependentBuyLedgerObservationV1(
            **_ledger().model_dump(exclude={"cash_shortfall", "observation_sha256"}),
            cash_shortfall="1",
            observation_sha256=_sha("0"),
        )
    with pytest.raises(ValidationError, match="released"):
        DependentBuyCoordinationV1(
            **_coordination(status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS).model_dump(
                exclude={"released_command_id", "coordination_sha256"}
            ),
            released_command_id="forged",
            coordination_sha256=_sha("0"),
        )
    with pytest.raises(ValueError):
        DependentBuyCoordinationStatusV1("RELEASE_READY")


def test_product_command_authority_supports_zero_one_and_many_without_positional_identity() -> None:
    common = dict(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        catalog_sha256=_sha("b"),
        creation_binding_sha256=_sha("c"),
        facade_conformance_set_sha256=_sha("d"),
        execution_projection_set_sha256=_sha("4"),
        transition_receipt_sha256=_sha("e"),
    )
    zero = ProductCommandAuthoritySetV2.create(**common, ordered_items=())
    one = ProductCommandAuthoritySetV2.create(**common, ordered_items=(_authority_item(ordinal=0, command_id="cmd_a"),))
    many = ProductCommandAuthoritySetV2.create(
        **common,
        ordered_items=(
            _authority_item(ordinal=0, command_id="cmd_a"),
            _authority_item(ordinal=1, command_id="cmd_b"),
        ),
    )

    assert zero.aggregate_disposition is ProductCommandAggregateDispositionV2.ZERO_COMMAND
    assert one.total_count == 1 and many.total_count == 2
    assert ProductCommandAuthoritySetV2.model_validate_json(many.model_dump_json()) == many
    with pytest.raises(ValidationError, match="ordered"):
        ProductCommandAuthoritySetV2.create(
            **common,
            ordered_items=(
                _authority_item(ordinal=1, command_id="cmd_b"),
                _authority_item(ordinal=0, command_id="cmd_a"),
            ),
        )


def test_product_command_item_rejects_fake_materialization_and_reject_carriers() -> None:
    materialize = _authority_item(ordinal=0, command_id="cmd_a")
    with pytest.raises(ValidationError, match="materialize"):
        ProductCommandAuthorityItemV2(
            **materialize.model_dump(exclude={"mapping_id", "item_sha256"}),
            mapping_id=None,
            item_sha256=_sha("0"),
        )
    with pytest.raises(ValidationError, match="reject"):
        ProductCommandAuthorityItemV2.create(
            **materialize.model_dump(
                exclude={
                    "schema_version",
                    "disposition",
                    "reject_reason_code",
                    "reject_context_sha256",
                    "mapping_id",
                    "outbox_id",
                    "child_order_id",
                    "item_sha256",
                }
            ),
            disposition=ProductCommandDispositionV2.REJECT_SYNCHRONOUS,
            reject_reason_code=None,
            reject_context_sha256=None,
        )


def test_product_lifecycle_and_materialization_receipts_are_factory_hash_closed() -> None:
    authority_item = _authority_item(ordinal=0, command_id="command_k6")
    authority = ProductCommandAuthoritySetV2.create(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        catalog_sha256=_sha("b"),
        creation_binding_sha256=_sha("c"),
        facade_conformance_set_sha256=_sha("d"),
        execution_projection_set_sha256=_sha("4"),
        transition_receipt_sha256=_sha("e"),
        ordered_items=(authority_item,),
    )
    item = ProductCommandLifecycleProjectionItemV2.create(
        authority_item_sha256=authority_item.item_sha256,
        effect_ordinal=0,
        command_id="command_k6",
        mapping_id="mapping_command_k6",
        outbox_id="command_k6",
        child_order_id="child_command_k6",
        lifecycle_status=ProductLifecycleStatusV2.PENDING,
        last_committed_stage="K2_OUTBOX_COMMITTED",
    )
    projection = ProductCommandLifecycleProjectionV2.create(
        authority_set_sha256=authority.authority_set_sha256,
        ordered_items=(item,),
    )
    projection.validate_against_authority_v2(authority)
    receipt = ProductMaterializationReceiptV2.create(
        authority=authority,
        repository_transaction_id="tx_k6",
        independent_readback_sha256=_sha("d"),
    )
    receipt.validate_against_authority_v2(authority)
    assert ProductCommandLifecycleProjectionV2.model_validate_json(projection.model_dump_json()) == projection
    assert ProductMaterializationReceiptV2.model_validate_json(receipt.model_dump_json()) == receipt


def test_product_lifecycle_and_materialization_reject_cross_permutation() -> None:
    authority_items = (
        _authority_item(ordinal=0, command_id="command_a"),
        _authority_item(ordinal=1, command_id="command_b"),
    )
    authority = ProductCommandAuthoritySetV2.create(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        catalog_sha256=_sha("b"),
        creation_binding_sha256=_sha("c"),
        facade_conformance_set_sha256=_sha("d"),
        execution_projection_set_sha256=_sha("4"),
        transition_receipt_sha256=_sha("e"),
        ordered_items=authority_items,
    )
    reversed_items = tuple(
        ProductCommandLifecycleProjectionItemV2.create(
            authority_item_sha256=item.item_sha256,
            effect_ordinal=item.effect_ordinal,
            command_id=item.command_id,
            mapping_id=item.mapping_id,
            outbox_id=item.outbox_id,
            child_order_id=item.child_order_id,
            lifecycle_status=ProductLifecycleStatusV2.PENDING,
            last_committed_stage="K2_OUTBOX_COMMITTED",
        )
        for item in reversed(authority_items)
    )
    with pytest.raises(ValidationError, match="contiguous authority ordinals"):
        ProductCommandLifecycleProjectionV2.create(
            authority_set_sha256=authority.authority_set_sha256,
            ordered_items=reversed_items,
        )
    receipt = ProductMaterializationReceiptV2.create(
        authority=authority,
        repository_transaction_id="tx_k6",
        independent_readback_sha256=_sha("d"),
    )
    forged = ProductMaterializationReceiptV2(
        **receipt.model_dump(exclude={"ordered_outbox_ids", "receipt_sha256"}),
        ordered_outbox_ids=tuple(reversed(receipt.ordered_outbox_ids)),
        receipt_sha256=hash_hex_v1(
            "miniqmt_product_materialization_receipt_v2",
            receipt.model_dump(exclude={"ordered_outbox_ids", "receipt_sha256"})
            | {"ordered_outbox_ids": tuple(reversed(receipt.ordered_outbox_ids))},
        ),
    )
    with pytest.raises(KernelProductContractError) as exc_info:
        forged.validate_against_authority_v2(authority)
    assert exc_info.value.reason_code == "MINIQMT_K6_PRODUCT_MATERIALIZATION_IDENTITY_DRIFT"


def test_durable_contract_readback_uses_bounded_typed_failure() -> None:
    with pytest.raises(KernelProductContractError) as exc_info:
        validate_kernel_product_payload_v1(
            ProductRouteOwnerV1,
            {"schema_version": "wrong", "runtime_id": []},
            stage="REVIEW_READBACK",
        )
    assert exc_info.value.reason_code == "MINIQMT_K6_CONTRACT_INVALID"
    assert exc_info.value.context["stage"] == "REVIEW_READBACK"
    assert exc_info.value.context["failure_count"] >= 1
    assert len(exc_info.value.context["failures"]) <= 256


def test_route_receipt_and_owner_use_immutable_receipt_plus_cas_pointer() -> None:
    receipt = ProductRouteCutoverReceiptV1.create(
        runtime_id="runtime_k6",
        binding_id="binding_k6",
        trade_date="2026-08-01",
        route_epoch=1,
        route_owner=ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY,
        effective_new_instance_sequence=11,
        legacy_active_instance_count=2,
        kernel_active_instance_count=0,
        catalog_sha256=_sha("a"),
        gateway_capability_catalog_sha256=_sha("b"),
        exchange_session_authority_sha256=_sha("c"),
        migration_readback_sha256=_sha("d"),
        product_authority_schema_sha256=_sha("e"),
        previous_receipt_sha256=None,
        created_at_utc=NOW,
    )
    owner = ProductRouteOwnerV1.create(receipt=receipt, row_version=1)
    assert ProductRouteOwnerV1.model_validate_json(owner.model_dump_json()) == owner
    with pytest.raises(ValidationError, match="owner hash"):
        ProductRouteOwnerV1(
            **owner.model_dump(exclude={"current_receipt_sha256", "owner_sha256"}),
            current_receipt_sha256=_sha("0"),
            owner_sha256=_sha("1"),
        )


def test_k6_negative_state_machine_matrix_is_fail_loud() -> None:
    dependency = _dependency()
    with pytest.raises(ValidationError, match="one-to-one"):
        DependentBuySellDependencyV1.create(
            runtime_id="runtime_k6",
            strategy_id="strategy_k6",
            sell_parent_intent_id="intent_sell",
            sell_algo_instance_id="algo_sell",
            latest_order_fact_ref=_sha("b"),
            settled_trade_fact_refs=(_sha("c"),),
            settled_cash_ledger_refs=(),
            dependency_status=DependentBuyDependencyStatusV1.OPEN,
        )
    with pytest.raises(ValidationError, match="requires trade"):
        DependentBuySellDependencyV1.create(
            runtime_id="runtime_k6",
            strategy_id="strategy_k6",
            sell_parent_intent_id="intent_sell",
            sell_algo_instance_id="algo_sell",
            latest_order_fact_ref=_sha("b"),
            settled_trade_fact_refs=(),
            settled_cash_ledger_refs=(),
            dependency_status=DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
        )
    with pytest.raises(ValidationError, match="dependency hash"):
        DependentBuySellDependencyV1(
            **dependency.model_dump(exclude={"dependency_sha256"}),
            dependency_sha256=_sha("0"),
        )

    coordination = _coordination(status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS)
    decision_values = {
        "coordination_id": coordination.coordination_id,
        "decision_sequence": 1,
        "previous_decision_sha256": None,
        "trigger_ref_sha256": _sha("1"),
        "decision": DependentBuyDecisionV1.WAIT,
        "reason_code": "WAIT",
        "ledger_observation_sha256": _sha("2"),
        "ordered_dependency_sha256s": (dependency.dependency_sha256,),
        "decided_at_utc": NOW,
        "worker_id": "worker_k6",
        "process_incarnation_id": "process_k6",
        "lease_epoch": 1,
    }
    with pytest.raises(ValidationError, match="at least one dependency"):
        DependentBuyReleaseDecisionV1.create(**(decision_values | {"ordered_dependency_sha256s": ()}))
    with pytest.raises(ValidationError, match="lacks K2"):
        DependentBuyReleaseDecisionV1.create(
            **(decision_values | {"decision": DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX})
        )
    with pytest.raises(ValidationError, match="cannot carry release"):
        DependentBuyReleaseDecisionV1.create(**(decision_values | {"release_event_id": "event_forged"}))
    with pytest.raises(ValidationError, match="predecessor"):
        DependentBuyReleaseDecisionV1.create(
            **(decision_values | {"decision_sequence": 2, "previous_decision_sha256": None})
        )

    with pytest.raises(ValidationError, match="cardinality"):
        DependentBuyCoordinationV1.create(
            **coordination.model_dump(
                exclude={"schema_version", "coordination_id", "coordination_sha256", "ordered_sell_dependencies"}
            ),
            ordered_sell_dependencies=(),
        )
    forged_owner = DependentBuySellDependencyV1.create(
        runtime_id="runtime_other",
        strategy_id="strategy_k6",
        sell_parent_intent_id="intent_sell",
        sell_algo_instance_id="algo_sell",
        latest_order_fact_ref=_sha("b"),
        settled_trade_fact_refs=(_sha("c"),),
        settled_cash_ledger_refs=(_sha("d"),),
        dependency_status=DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
    )
    with pytest.raises(ValidationError, match="owner differs"):
        DependentBuyCoordinationV1.create(
            **coordination.model_dump(
                exclude={"schema_version", "coordination_id", "coordination_sha256", "ordered_sell_dependencies"}
            ),
            ordered_sell_dependencies=(forged_owner,),
        )
    with pytest.raises(ValueError, match="initial state"):
        _coordination(status=DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX).validate_initial_v1()


def test_k6_product_projection_and_route_negative_matrix_is_fail_loud() -> None:
    materialized = _authority_item(ordinal=0, command_id="cmd_a")
    with pytest.raises(ValidationError, match="outbox identity"):
        ProductCommandAuthorityItemV2.create(
            **materialized.model_dump(exclude={"schema_version", "outbox_id", "item_sha256"}),
            outbox_id="different_outbox",
        )
    with pytest.raises(ValidationError, match="materialized.*identities"):
        ProductCommandLifecycleProjectionItemV2.create(
            authority_item_sha256=_sha("a"),
            effect_ordinal=0,
            command_id="cmd_a",
            lifecycle_status=ProductLifecycleStatusV2.PENDING,
            last_committed_stage="K2_OUTBOX_COMMITTED",
        )
    with pytest.raises(ValidationError, match="ACKED"):
        ProductCommandLifecycleProjectionItemV2.create(
            authority_item_sha256=_sha("a"),
            effect_ordinal=0,
            command_id="cmd_a",
            mapping_id="mapping_a",
            outbox_id="cmd_a",
            child_order_id="child_a",
            lifecycle_status=ProductLifecycleStatusV2.ACKED,
            last_committed_stage="BROKER_CALLBACK",
            broker_called=False,
        )
    with pytest.raises(TypeError, match="authority"):
        ProductMaterializationReceiptV2.create(
            authority={},  # type: ignore[arg-type]
            repository_transaction_id="tx_k6",
            independent_readback_sha256=_sha("d"),
        )
    receipt = ProductRouteCutoverReceiptV1.create(
        runtime_id="runtime_k6",
        binding_id="binding_k6",
        trade_date="2026-08-01",
        route_epoch=1,
        route_owner=ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY,
        effective_new_instance_sequence=11,
        legacy_active_instance_count=1,
        kernel_active_instance_count=0,
        catalog_sha256=_sha("a"),
        gateway_capability_catalog_sha256=_sha("b"),
        exchange_session_authority_sha256=_sha("c"),
        migration_readback_sha256=_sha("d"),
        product_authority_schema_sha256=_sha("e"),
        previous_receipt_sha256=None,
        created_at_utc=NOW,
    )
    owner = ProductRouteOwnerV1.create(receipt=receipt, row_version=1)
    with pytest.raises(ValueError, match="does not close"):
        owner.validate_receipt_v1(receipt.model_copy(update={"binding_id": "binding_other"}))


@pytest.mark.parametrize("bad", [None, True, 1, [], {}, " "])
def test_k6_identities_never_coerce_malformed_values(bad: object) -> None:
    values = _trigger().model_dump()
    values["event_id"] = bad
    with pytest.raises(ValidationError):
        DependentBuyTriggerEventRefV1.model_validate(values)


def test_k6c0_dependent_buy_v2_replaces_positional_hashes_and_fake_row_version() -> None:
    dependency = _dependency_v2()
    observation = DependentBuyLedgerObservationV2.create(
        runtime_id="runtime_k6",
        strategy_id="strategy_k6",
        trade_date="2026-08-01",
        virtual_account_id="account_k6",
        virtual_account_updated_at_utc=NOW,
        latest_cash_ledger_sequence=11,
        ledger_as_of_utc=NOW,
        available_cash="1000",
        required_cash="1050",
        ordered_settled_proceeds_refs=dependency.ordered_settled_proceeds_refs,
        freshness_session_authority_sha256=_sha("e"),
    )
    assert observation.cash_shortfall == "50"
    assert "ledger_row_version" not in type(observation).model_fields
    assert "settled_trade_fact_refs" not in type(dependency).model_fields
    assert DependentBuyLedgerObservationV2.model_validate_json(observation.model_dump_json()) == observation
    with pytest.raises(ValidationError, match="latest cash ledger sequence"):
        DependentBuyLedgerObservationV2.create(
            **observation.model_dump(
                exclude={
                    "schema_version",
                    "latest_cash_ledger_sequence",
                    "cash_shortfall",
                    "ledger_revision_sha256",
                    "observation_sha256",
                }
            ),
            latest_cash_ledger_sequence=10,
        )


def test_k6c0_coordination_v2_validates_exact_initial_and_successor_closure() -> None:
    initial = _coordination_v2()
    initial.validate_initial_v2()
    successor = _coordination_v2(row_version=2)
    successor.validate_successor_v2(initial)
    with pytest.raises(ValueError, match="immutable owner, command, or payload"):
        _coordination_v2(row_version=2, release_command_id="different_command").validate_successor_v2(initial)
    with pytest.raises(ValueError, match="row_version must increase by one"):
        _coordination_v2(row_version=3).validate_successor_v2(initial)


def test_k6c0_v2_dependency_candidate_and_decision_negative_matrix() -> None:
    dependency = _dependency_v2()
    with pytest.raises(ValidationError, match="present together"):
        DependentBuySellDependencyV2.model_validate(_payload_with(dependency, latest_order_fact_id=None))
    with pytest.raises(ValidationError, match="canonical and unique"):
        DependentBuySellDependencyV2.model_validate(
            _payload_with(
                dependency,
                ordered_settled_proceeds_refs=(_proceeds_ref(), _proceeds_ref()),
            )
        )
    with pytest.raises(ValidationError, match="requires at least one"):
        DependentBuySellDependencyV2.create(
            runtime_id="runtime_k6",
            strategy_id="strategy_k6",
            sell_parent_intent_id="intent_sell",
            sell_algo_instance_id="algo_sell",
            dependency_status=DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
            ordered_settled_proceeds_refs=(),
        )

    candidate = _v3_evidence(command=_v3_command(), dependent=True).dependent_buy_candidate
    assert candidate is not None
    with pytest.raises(ValidationError, match="cardinality"):
        DependentBuyCandidateAuthorityV2.model_validate(_payload_with(candidate, ordered_sell_dependencies=()))
    with pytest.raises(ValidationError, match="canonical exact dependent-BUY subset"):
        DependentBuyCandidateAuthorityV2.model_validate(_payload_with(candidate, ordered_error_codes=()))
    with pytest.raises(ValidationError, match="canonical exact dependent-BUY subset"):
        DependentBuyCandidateAuthorityV2.model_validate(
            _payload_with(candidate, ordered_error_codes=("NOT_DEPENDENT_BUY",))
        )

    wait = _decision_v2()
    with pytest.raises(ValidationError, match="at least one dependency"):
        DependentBuyReleaseDecisionV2.model_validate(_payload_with(wait, ordered_dependency_sha256s=()))
    with pytest.raises(ValidationError, match="non-release decision"):
        DependentBuyReleaseDecisionV2.model_validate(_payload_with(wait, release_event_id="event_release"))
    release = _decision_v2(decision=DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX)
    with pytest.raises(ValidationError, match="release decision lacks"):
        DependentBuyReleaseDecisionV2.model_validate(_payload_with(release, release_event_id=None))
    with pytest.raises(ValidationError, match="first decision cannot carry predecessor"):
        DependentBuyReleaseDecisionV2.model_validate(_payload_with(wait, previous_decision_sha256=_sha("9")))
    successor = _decision_v2(decision_sequence=2)
    with pytest.raises(ValidationError, match="successor decision requires predecessor"):
        DependentBuyReleaseDecisionV2.model_validate(_payload_with(successor, previous_decision_sha256=None))


def test_k6c0_v2_ledger_and_coordination_negative_matrix() -> None:
    dependency = _dependency_v2()
    observation = DependentBuyLedgerObservationV2.create(
        runtime_id="runtime_k6",
        strategy_id="strategy_k6",
        trade_date="2026-08-01",
        virtual_account_id="account_k6",
        virtual_account_updated_at_utc=NOW,
        latest_cash_ledger_sequence=11,
        ledger_as_of_utc=NOW,
        available_cash="1000",
        required_cash="1050",
        ordered_settled_proceeds_refs=dependency.ordered_settled_proceeds_refs,
        freshness_session_authority_sha256=_sha("e"),
    )
    with pytest.raises(ValidationError, match="canonical and unique"):
        DependentBuyLedgerObservationV2.model_validate(
            _payload_with(
                observation,
                ordered_settled_proceeds_refs=(_proceeds_ref(), _proceeds_ref()),
            )
        )
    with pytest.raises(ValidationError, match="as-of time"):
        DependentBuyLedgerObservationV2.model_validate(
            _payload_with(observation, ledger_as_of_utc="2026-07-31T23:00:00Z")
        )
    with pytest.raises(ValidationError, match="cash_shortfall"):
        DependentBuyLedgerObservationV2.model_validate(_payload_with(observation, cash_shortfall="0"))
    with pytest.raises(ValidationError, match="ledger revision hash"):
        DependentBuyLedgerObservationV2.model_validate(_payload_with(observation, ledger_revision_sha256=_sha("0")))

    coordination = _coordination_v2()
    with pytest.raises(ValidationError, match="cardinality"):
        DependentBuyCoordinationV2.model_validate(_payload_with(coordination, ordered_sell_dependencies=()))
    with pytest.raises(ValidationError, match="non-released coordination"):
        DependentBuyCoordinationV2.model_validate(
            _payload_with(coordination, released_command_id=coordination.release_command_id)
        )
    with pytest.raises(ValidationError, match="decision sequence requires"):
        DependentBuyCoordinationV2.model_validate(
            _payload_with(coordination, decision_sequence=1, last_decision_sha256=None)
        )
    with pytest.raises(ValidationError, match="leased coordination requires"):
        DependentBuyCoordinationV2.model_validate(_payload_with(coordination, lease_epoch=1))
    terminal = DependentBuyCoordinationV2.create(
        **coordination.model_dump(
            exclude={
                "schema_version",
                "status",
                "released_command_id",
                "released_outbox_id",
                "coordination_id",
                "coordination_sha256",
            }
        ),
        status=DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX,
        released_command_id=coordination.release_command_id,
        released_outbox_id=coordination.release_command_id,
    )
    with pytest.raises(ValueError, match="terminal coordination cannot reopen"):
        _coordination_v2(row_version=terminal.row_version + 1).validate_successor_v2(terminal)


def test_k6c0_v2_coordination_successor_writer_normalizes_timestamp_before_hashing() -> None:
    previous = _coordination_v2()
    successor_payload = previous.model_dump(
        exclude={
            "schema_version",
            "coordination_id",
            "coordination_sha256",
            "status",
            "decision_sequence",
            "last_decision_sha256",
            "released_command_id",
            "released_outbox_id",
            "row_version",
            "updated_at_utc",
        }
    )
    successor = DependentBuyCoordinationV2.create(
        **successor_payload,
        status=DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX,
        decision_sequence=1,
        last_decision_sha256=_sha("e"),
        released_command_id=previous.release_command_id,
        released_outbox_id=previous.release_command_id,
        row_version=2,
        updated_at_utc="2026-08-03T01:33:00Z",
    )

    successor.validate_successor_v2(previous)
    assert DependentBuyCoordinationV2.model_validate_json(successor.model_dump_json()) == successor


def test_k6c0_mapping_supports_exact_deferred_to_reserved_successor_only() -> None:
    authority_item = _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)
    deferred = ProductCommandChildMappingV1.create_deferred(
        authority_item=authority_item,
        strategy_slot_id="slot_k6",
        created_at_utc=NOW,
    )
    reserved = ProductCommandChildMappingV1.create_successor(
        previous=deferred,
        mapping_status=ProductCommandChildMappingStatusV1.RESERVED,
        updated_by_event_id="event_release",
        updated_at_utc=NOW + timedelta(seconds=1),
    )
    assert reserved.validate_successor_v1(deferred) is reserved
    assert ProductCommandChildMappingV1.model_validate_json(reserved.model_dump_json()) == reserved
    terminal = ProductCommandChildMappingV1.create_successor(
        previous=deferred,
        mapping_status=ProductCommandChildMappingStatusV1.TERMINAL,
        updated_by_event_id="event_terminal",
        updated_at_utc=NOW + timedelta(seconds=1),
    )
    assert terminal.mapping_status is ProductCommandChildMappingStatusV1.TERMINAL
    with pytest.raises(ValueError, match="only DEFERRED_DEPENDENT_BUY"):
        deferred.validate_successor_v1(reserved)
    with pytest.raises(ValidationError, match="payload hash"):
        ProductCommandChildMappingV1.model_validate(_payload_with(deferred, requested_quantity=200))
    with pytest.raises(ValidationError, match="version=2"):
        ProductCommandChildMappingV1.model_validate(_payload_with(reserved, mapping_version=3))


def test_k6c0_product_mapping_rejects_invalid_authority_and_successor_inputs() -> None:
    deferred_item = _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)
    deferred = ProductCommandChildMappingV1.create_deferred(
        authority_item=deferred_item,
        strategy_slot_id="slot_k6",
        created_at_utc=NOW,
    )
    with pytest.raises(TypeError, match="authority_item"):
        ProductCommandChildMappingV1.create_deferred(  # type: ignore[arg-type]
            authority_item="not-an-authority-item",
            strategy_slot_id="slot_k6",
            created_at_utc=NOW,
        )
    with pytest.raises(ValueError, match="dependent-BUY authority"):
        ProductCommandChildMappingV1.create_deferred(
            authority_item=_v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE),
            strategy_slot_id="slot_k6",
            created_at_utc=NOW,
        )
    with pytest.raises(TypeError, match="previous"):
        ProductCommandChildMappingV1.create_successor(  # type: ignore[arg-type]
            previous="not-a-product-mapping",
            mapping_status=ProductCommandChildMappingStatusV1.RESERVED,
            updated_by_event_id="event_release",
            updated_at_utc=NOW + timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="RESERVED or TERMINAL"):
        ProductCommandChildMappingV1.create_successor(
            previous=deferred,
            mapping_status=ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY,  # type: ignore[arg-type]
            updated_by_event_id="event_release",
            updated_at_utc=NOW + timedelta(seconds=1),
        )
    with pytest.raises(TypeError, match="previous"):
        deferred.validate_successor_v1("not-a-product-mapping")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="RESERVED or TERMINAL"):
        deferred.validate_successor_v1(deferred)
    other_deferred = ProductCommandChildMappingV1.create_deferred(
        authority_item=deferred_item,
        strategy_slot_id="slot_other",
        created_at_utc=NOW,
    )
    reserved = ProductCommandChildMappingV1.create_successor(
        previous=deferred,
        mapping_status=ProductCommandChildMappingStatusV1.RESERVED,
        updated_by_event_id="event_release",
        updated_at_utc=NOW + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="immutable business payload"):
        reserved.validate_successor_v1(other_deferred)


@pytest.mark.parametrize(
    ("mapping_factory", "updates", "error"),
    (
        ("deferred", {"child_order_id": "child_forged"}, "mapping identity"),
        ("deferred", {"deterministic_client_order_ref": "ref_forged"}, "client reference"),
        ("deferred", {"updated_by_event_id": "event_forged"}, "version=1"),
        ("deferred", {"updated_at_utc": NOW + timedelta(seconds=1)}, "timestamps must be equal"),
        ("reserved", {"updated_at_utc": NOW}, "must advance updated_at_utc"),
        ("reserved", {"mapping_receipt_sha256": _sha("0")}, "receipt hash"),
    ),
)
def test_k6c0_product_mapping_strict_readback_rejects_identity_state_and_hash_drift(
    mapping_factory: str,
    updates: dict[str, object],
    error: str,
) -> None:
    deferred = ProductCommandChildMappingV1.create_deferred(
        authority_item=_v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY),
        strategy_slot_id="slot_k6",
        created_at_utc=NOW,
    )
    mapping = (
        deferred
        if mapping_factory == "deferred"
        else ProductCommandChildMappingV1.create_successor(
            previous=deferred,
            mapping_status=ProductCommandChildMappingStatusV1.RESERVED,
            updated_by_event_id="event_release",
            updated_at_utc=NOW + timedelta(seconds=1),
        )
    )
    with pytest.raises(ValidationError, match=error):
        ProductCommandChildMappingV1.model_validate(_payload_with(mapping, **updates))


def test_k6c0_v3_authority_persists_full_command_evidence_and_three_dispositions() -> None:
    items = (
        _v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE, ordinal=0),
        _v3_item(disposition=ProductCommandDispositionV3.REJECT_SYNCHRONOUS, ordinal=1),
        _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY, ordinal=2),
    )
    authority = ProductCommandAuthoritySetV3.create(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        catalog_sha256=_sha("1"),
        creation_binding_sha256=_sha("2"),
        facade_conformance_set_sha256=_sha("3"),
        execution_projection_set_sha256=items[0].execution_projection_set_sha256,
        transition_receipt_sha256=_sha("4"),
        ordered_items=items,
    )
    assert (authority.materialize_count, authority.reject_count, authority.defer_count) == (1, 1, 1)
    assert ProductCommandAuthoritySetV3.model_validate_json(authority.model_dump_json()) == authority
    receipt = ProductMaterializationReceiptV3.create(
        authority=authority,
        repository_transaction_id="tx_k6c0",
        independent_readback_sha256=_sha("5"),
    )
    receipt.validate_against_authority_v3(authority)
    lifecycle_items = tuple(
        ProductCommandLifecycleProjectionItemV3.create(
            authority_item_sha256=item.item_sha256,
            effect_ordinal=item.effect_ordinal,
            command_id=item.command_id,
            disposition=item.disposition,
            mapping_id=item.mapping_id,
            outbox_id=item.outbox_id,
            child_order_id=item.child_order_id,
            lifecycle_status=(
                ProductLifecycleStatusV3.DEFERRED_DEPENDENT_BUY
                if item.disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY
                else ProductLifecycleStatusV3.SYNCHRONOUS_REJECTED
                if item.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS
                else ProductLifecycleStatusV3.PENDING
            ),
            last_committed_stage="PRODUCT_AUTHORITY_COMMITTED",
            broker_called=False if item.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS else None,
        )
        for item in items
    )
    lifecycle = ProductCommandLifecycleProjectionV3.create(
        runtime_id=authority.runtime_id,
        algo_instance_id=authority.algo_instance_id,
        event_id=authority.event_id,
        delivery_id=authority.delivery_id,
        transition_id=authority.transition_id,
        authority_set_sha256=authority.authority_set_sha256,
        ordered_item_projections=lifecycle_items,
    )
    lifecycle.validate_against_authority_v3(authority)


def test_k6c0_v3_aggregate_variants_and_authority_negative_matrix() -> None:
    zero = _v3_authority(())
    materialized = _v3_authority((_v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE),))
    rejected = _v3_authority((_v3_item(disposition=ProductCommandDispositionV3.REJECT_SYNCHRONOUS),))
    deferred = _v3_authority((_v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY),))
    assert tuple(item.aggregate_disposition.value for item in (zero, materialized, rejected, deferred)) == (
        "ZERO_COMMAND",
        "MATERIALIZE_ALL_ACCEPTED_COMMANDS",
        "ALL_REJECTED",
        "ALL_DEFERRED",
    )
    mixed_items = (
        _v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE, ordinal=0),
        _v3_item(disposition=ProductCommandDispositionV3.REJECT_SYNCHRONOUS, ordinal=1),
    )
    mixed = _v3_authority(mixed_items)
    with pytest.raises(ValidationError, match="sorted and unique"):
        ProductCommandAuthoritySetV3.model_validate(
            _payload_with(mixed, ordered_items=(mixed_items[1], mixed_items[0]))
        )
    with pytest.raises(ValidationError, match="owner differs"):
        ProductCommandAuthoritySetV3.model_validate(_payload_with(mixed, runtime_id="runtime_other"))
    with pytest.raises(ValidationError, match="counts do not close"):
        ProductCommandAuthoritySetV3.model_validate(_payload_with(mixed, materialize_count=0))
    with pytest.raises(ValidationError, match="aggregate disposition mismatch"):
        ProductCommandAuthoritySetV3.model_validate(
            _payload_with(mixed, aggregate_disposition=ProductCommandAggregateDispositionV3.ALL_REJECTED)
        )


def test_k6c0_v3_evidence_item_lifecycle_and_receipt_negative_matrix() -> None:
    command = _v3_command()
    evidence = _v3_evidence(command=command, dependent=False)
    with pytest.raises(ValidationError, match="owner differs from execution projection set"):
        ProductCommandEvaluationEvidenceV3.model_validate(_payload_with(evidence, runtime_id="runtime_other"))

    materialized = _v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE)
    with pytest.raises(ValidationError, match="evidence owner differs"):
        ProductCommandAuthorityItemV3.model_validate(_payload_with(materialized, runtime_id="runtime_other"))
    with pytest.raises(ValidationError, match="exact outbox"):
        ProductCommandAuthorityItemV3.model_validate(_payload_with(materialized, outbox_id=None))
    rejected = _v3_item(disposition=ProductCommandDispositionV3.REJECT_SYNCHRONOUS)
    with pytest.raises(ValidationError, match="rejection evidence"):
        ProductCommandAuthorityItemV3.model_validate(_payload_with(rejected, reject_reason_code=None))
    deferred = _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)
    with pytest.raises(ValidationError, match="dependent-BUY defer requires"):
        ProductCommandAuthorityItemV3.model_validate(_payload_with(deferred, outbox_id=deferred.command_id))

    authority = _v3_authority((materialized,))
    lifecycle_item = ProductCommandLifecycleProjectionItemV3.create(
        authority_item_sha256=materialized.item_sha256,
        effect_ordinal=0,
        command_id=materialized.command_id,
        disposition=materialized.disposition,
        mapping_id=materialized.mapping_id,
        outbox_id=materialized.outbox_id,
        child_order_id=materialized.child_order_id,
        lifecycle_status=ProductLifecycleStatusV3.PENDING,
        last_committed_stage="PRODUCT_AUTHORITY_COMMITTED",
        broker_called=None,
    )
    lifecycle = ProductCommandLifecycleProjectionV3.create(
        runtime_id=authority.runtime_id,
        algo_instance_id=authority.algo_instance_id,
        event_id=authority.event_id,
        delivery_id=authority.delivery_id,
        transition_id=authority.transition_id,
        authority_set_sha256=authority.authority_set_sha256,
        ordered_item_projections=(lifecycle_item,),
    )
    lifecycle.validate_against_authority_v3(authority)
    with pytest.raises(KernelProductContractError, match="lifecycle V3 differs"):
        ProductCommandLifecycleProjectionV3.create(
            runtime_id=authority.runtime_id,
            algo_instance_id=authority.algo_instance_id,
            event_id=authority.event_id,
            delivery_id=authority.delivery_id,
            transition_id=authority.transition_id,
            authority_set_sha256=_sha("8"),
            ordered_item_projections=(lifecycle_item,),
        ).validate_against_authority_v3(authority)

    receipt = ProductMaterializationReceiptV3.create(
        authority=authority,
        repository_transaction_id="tx_k6c0",
        independent_readback_sha256=_sha("7"),
    )
    forged = ProductMaterializationReceiptV3.model_construct(
        **{**receipt.model_dump(), "independent_readback_sha256": _sha("6")}
    )
    with pytest.raises(KernelProductContractError, match="materialization receipt V3 differs"):
        forged.validate_against_authority_v3(authority)


@pytest.mark.parametrize(
    ("disposition", "updates"),
    (
        (ProductCommandDispositionV3.MATERIALIZE, {"mapping_id": "mapping_forged"}),
        (ProductCommandDispositionV3.MATERIALIZE, {"child_order_id": "child_forged"}),
        (ProductCommandDispositionV3.REJECT_SYNCHRONOUS, {"mapping_id": "mapping_forged"}),
        (ProductCommandDispositionV3.REJECT_SYNCHRONOUS, {"child_order_id": "child_forged"}),
        (ProductCommandDispositionV3.DEFER_DEPENDENT_BUY, {"mapping_id": "mapping_forged"}),
        (ProductCommandDispositionV3.DEFER_DEPENDENT_BUY, {"child_order_id": "child_forged"}),
    ),
)
def test_k6c0_v3_authority_item_rejects_non_deterministic_mapping_lineage(
    disposition: ProductCommandDispositionV3,
    updates: dict[str, str],
) -> None:
    item = _v3_item(disposition=disposition)
    with pytest.raises(ValidationError, match="mapping identity"):
        _recreate_v3_item(item, **updates)


def test_k6c0_v3_shared_projection_ref_remains_distinct_from_per_command_oms_receipt() -> None:
    command = _v3_command()
    evidence = _v3_evidence(command=command, dependent=True)
    oms_ref = next(
        item
        for item in evidence.execution_projection_set.ordered_projection_refs
        if item.projection_type is KernelProjectionTypeV1.OMS_PREFLIGHT
    )
    assert oms_ref.payload_sha256 != evidence.oms_preflight_receipt.receipt_sha256
    assert ProductCommandEvaluationEvidenceV3.model_validate_json(evidence.model_dump_json()) == evidence


def test_k6c0_v3_cancel_authority_reuses_existing_mapping_lineage() -> None:
    submit = _v3_command()
    child_order_id = execution_child_order_id_v1(
        command_id=submit.command_id,
        local_vt_orderid=submit.local_vt_orderid,
    )
    mapping_id = command_child_mapping_id_v1(
        command_id=submit.command_id,
        local_vt_orderid=submit.local_vt_orderid,
        child_order_id=child_order_id,
    )
    cancel = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=submit.runtime_id,
        algo_instance_id=submit.algo_instance_id,
        parent_intent_id=submit.parent_intent_id,
        transition_id=submit.transition_id,
        ordinal=submit.ordinal,
        local_vt_orderid=submit.local_vt_orderid,
        symbol=submit.symbol,
        side=submit.side,
        order_type=submit.order_type,
        price_decimal=submit.price_decimal,
        quantity=submit.quantity,
        owned_broker_order_id="broker_existing",
        reason_code="PLUGIN_CANCEL",
        metadata={"submit_command_id": submit.command_id},
    )
    evidence = _v3_evidence(command=cancel, dependent=False)
    item = ProductCommandAuthorityItemV3.create(
        runtime_id=cancel.runtime_id,
        algo_instance_id=cancel.algo_instance_id,
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id=cancel.transition_id,
        effect_ordinal=cancel.ordinal,
        command_json=cancel,
        evaluation_evidence=evidence,
        plugin_effect_sha256=_sha("e"),
        disposition=ProductCommandDispositionV3.MATERIALIZE,
        mapping_id=mapping_id,
        outbox_id=cancel.command_id,
        child_order_id=child_order_id,
    )
    assert (item.mapping_id, item.child_order_id) == (mapping_id, child_order_id)
    with pytest.raises(ValidationError, match="mapping identity"):
        _recreate_v3_item(item, mapping_id="mapping_forged")


@pytest.mark.parametrize(
    ("disposition", "lifecycle_status", "broker_called", "qmt_order_id", "error"),
    (
        (
            ProductCommandDispositionV3.MATERIALIZE,
            ProductLifecycleStatusV3.SYNCHRONOUS_REJECTED,
            False,
            None,
            "materialized lifecycle status",
        ),
        (
            ProductCommandDispositionV3.REJECT_SYNCHRONOUS,
            ProductLifecycleStatusV3.PENDING,
            None,
            None,
            "synchronous reject lifecycle",
        ),
        (
            ProductCommandDispositionV3.MATERIALIZE,
            ProductLifecycleStatusV3.PENDING,
            True,
            "broker_forged",
            "pre-dispatch lifecycle",
        ),
        (
            ProductCommandDispositionV3.MATERIALIZE,
            ProductLifecycleStatusV3.ACKED_REJECTED,
            False,
            None,
            "ACKED_REJECTED lifecycle",
        ),
        (
            ProductCommandDispositionV3.MATERIALIZE,
            ProductLifecycleStatusV3.ACKED_REJECTED,
            True,
            "broker_forged",
            "ACKED_REJECTED lifecycle",
        ),
        (
            ProductCommandDispositionV3.MATERIALIZE,
            ProductLifecycleStatusV3.FAILED_RETRYABLE,
            None,
            None,
            "FAILED_RETRYABLE lifecycle",
        ),
        (
            ProductCommandDispositionV3.MATERIALIZE,
            ProductLifecycleStatusV3.FAILED_TERMINAL,
            False,
            "broker_forged",
            "accepted order identity",
        ),
    ),
)
def test_k6c0_v3_lifecycle_rejects_impossible_disposition_and_broker_facts(
    disposition: ProductCommandDispositionV3,
    lifecycle_status: ProductLifecycleStatusV3,
    broker_called: bool | None,
    qmt_order_id: str | None,
    error: str,
) -> None:
    item = _v3_item(disposition=disposition)
    with pytest.raises(ValidationError, match=error):
        ProductCommandLifecycleProjectionItemV3.create(
            authority_item_sha256=item.item_sha256,
            effect_ordinal=item.effect_ordinal,
            command_id=item.command_id,
            disposition=item.disposition,
            mapping_id=item.mapping_id,
            outbox_id=item.outbox_id,
            child_order_id=item.child_order_id,
            lifecycle_status=lifecycle_status,
            last_committed_stage="FORGED_STAGE",
            broker_called=broker_called,
            qmt_order_id=qmt_order_id,
        )


@pytest.mark.parametrize(
    ("lifecycle_status", "broker_called", "error"),
    (
        (ProductLifecycleStatusV3.PENDING, None, "pre-dispatch lifecycle"),
        (ProductLifecycleStatusV3.FAILED_RETRYABLE, False, "FAILED_RETRYABLE lifecycle"),
    ),
)
def test_k6c0_v3_pre_call_lifecycle_rejects_reconciliation_receipt(
    lifecycle_status: ProductLifecycleStatusV3,
    broker_called: bool | None,
    error: str,
) -> None:
    item = _v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE)
    with pytest.raises(ValidationError, match=error):
        ProductCommandLifecycleProjectionItemV3.create(
            authority_item_sha256=item.item_sha256,
            effect_ordinal=item.effect_ordinal,
            command_id=item.command_id,
            disposition=item.disposition,
            mapping_id=item.mapping_id,
            outbox_id=item.outbox_id,
            child_order_id=item.child_order_id,
            lifecycle_status=lifecycle_status,
            last_committed_stage="PRE_CALL",
            broker_called=broker_called,
            reconciliation_receipt_sha256=_sha("e"),
        )


@pytest.mark.parametrize(
    ("lifecycle_status", "broker_called", "qmt_order_id", "reconciliation_receipt_sha256"),
    (
        (ProductLifecycleStatusV3.PENDING, None, None, None),
        (ProductLifecycleStatusV3.CLAIMED, None, None, None),
        (ProductLifecycleStatusV3.DISPATCHING, None, None, None),
        (ProductLifecycleStatusV3.ACKED, True, "broker_accepted", None),
        (ProductLifecycleStatusV3.ACKED_REJECTED, True, None, None),
        (ProductLifecycleStatusV3.FAILED_RETRYABLE, False, None, None),
        (ProductLifecycleStatusV3.OUTCOME_UNKNOWN, None, None, None),
        (ProductLifecycleStatusV3.RECONCILING, None, None, _sha("c")),
        (ProductLifecycleStatusV3.FAILED_TERMINAL, False, None, None),
        (ProductLifecycleStatusV3.FAILED_TERMINAL, True, "broker_accepted", _sha("d")),
    ),
)
def test_k6c0_v3_materialized_lifecycle_preserves_valid_k2_status_matrix(
    lifecycle_status: ProductLifecycleStatusV3,
    broker_called: bool | None,
    qmt_order_id: str | None,
    reconciliation_receipt_sha256: str | None,
) -> None:
    item = _v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE)
    projected = ProductCommandLifecycleProjectionItemV3.create(
        authority_item_sha256=item.item_sha256,
        effect_ordinal=item.effect_ordinal,
        command_id=item.command_id,
        disposition=item.disposition,
        mapping_id=item.mapping_id,
        outbox_id=item.outbox_id,
        child_order_id=item.child_order_id,
        lifecycle_status=lifecycle_status,
        last_committed_stage="K2_DURABLE_READBACK",
        broker_called=broker_called,
        qmt_order_id=qmt_order_id,
        reconciliation_receipt_sha256=reconciliation_receipt_sha256,
    )
    assert projected.lifecycle_status is lifecycle_status


def test_k6c0_v3_lifecycle_closes_mapping_outbox_child_to_authority_item() -> None:
    item = _v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE)
    authority = _v3_authority((item,))
    lifecycle_item = ProductCommandLifecycleProjectionItemV3.create(
        authority_item_sha256=item.item_sha256,
        effect_ordinal=item.effect_ordinal,
        command_id=item.command_id,
        disposition=item.disposition,
        mapping_id="mapping_forged",
        outbox_id=item.outbox_id,
        child_order_id="child_forged",
        lifecycle_status=ProductLifecycleStatusV3.PENDING,
        last_committed_stage="PRODUCT_AUTHORITY_COMMITTED",
        broker_called=None,
    )
    lifecycle = ProductCommandLifecycleProjectionV3.create(
        runtime_id=authority.runtime_id,
        algo_instance_id=authority.algo_instance_id,
        event_id=authority.event_id,
        delivery_id=authority.delivery_id,
        transition_id=authority.transition_id,
        authority_set_sha256=authority.authority_set_sha256,
        ordered_item_projections=(lifecycle_item,),
    )
    with pytest.raises(KernelProductContractError, match="lifecycle V3 differs"):
        lifecycle.validate_against_authority_v3(authority)


def test_k6c0_v3_deferred_lifecycle_rejects_reconciliation_evidence() -> None:
    item = _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)
    with pytest.raises(ValidationError, match="broker, callback or reconciliation"):
        ProductCommandLifecycleProjectionItemV3.create(
            authority_item_sha256=item.item_sha256,
            effect_ordinal=item.effect_ordinal,
            command_id=item.command_id,
            disposition=item.disposition,
            mapping_id=item.mapping_id,
            outbox_id=None,
            child_order_id=item.child_order_id,
            lifecycle_status=ProductLifecycleStatusV3.DEFERRED_DEPENDENT_BUY,
            last_committed_stage="PRODUCT_AUTHORITY_COMMITTED",
            broker_called=None,
            reconciliation_receipt_sha256=_sha("f"),
        )


def test_k6c0_v3_reader_rejects_k6a_hash_only_v2_and_full_payload_drift() -> None:
    v2 = ProductCommandAuthoritySetV2.create(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        catalog_sha256=_sha("1"),
        creation_binding_sha256=_sha("2"),
        facade_conformance_set_sha256=_sha("3"),
        execution_projection_set_sha256=_sha("4"),
        transition_receipt_sha256=_sha("5"),
        ordered_items=(),
    )
    with pytest.raises(KernelProductContractError):
        validate_kernel_product_payload_v1(
            ProductCommandAuthoritySetV3,
            v2.model_dump(mode="json"),
            stage="PRODUCT_ROOT_READBACK",
        )
    item = _v3_item(disposition=ProductCommandDispositionV3.MATERIALIZE)
    payload = item.model_dump(mode="json")
    payload["command_json"]["quantity"] = 200
    payload["item_sha256"] = hash_hex_v1(
        "miniqmt_product_command_authority_item_v3",
        {key: value for key, value in payload.items() if key != "item_sha256"},
    )
    with pytest.raises(KernelProductContractError):
        validate_kernel_product_payload_v1(
            ProductCommandAuthorityItemV3,
            payload,
            stage="PRODUCT_ITEM_READBACK",
        )


def test_k6c0_v3_command_and_evidence_bounds_fail_loud() -> None:
    command = _v3_command(metadata={"oversized": "x" * (17 * 1024)})
    evidence = _v3_evidence(command=command, dependent=False)
    with pytest.raises(ValidationError, match="16KiB"):
        ProductCommandAuthorityItemV3.create(
            runtime_id=command.runtime_id,
            algo_instance_id=command.algo_instance_id,
            event_id="event_k6",
            delivery_id="delivery_k6",
            transition_id=command.transition_id,
            effect_ordinal=0,
            command_json=command,
            evaluation_evidence=evidence,
            plugin_effect_sha256=_sha("e"),
            disposition=ProductCommandDispositionV3.MATERIALIZE,
            mapping_id="mapping_oversized",
            outbox_id=command.command_id,
            child_order_id="child_oversized",
        )
