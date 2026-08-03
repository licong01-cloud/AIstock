from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.tests.miniqmt_execution_runtime.test_kernel_product_contracts import (
    _v3_command,
    _v3_evidence,
)
from backend.tests.miniqmt_execution_runtime.test_vnpy_facade_kernel_invocation import _v2_candidate
from backend.services.miniqmt_execution_runtime.kernel_product_authority import (
    bind_product_transition_receipt_v3,
    build_product_command_authority_set_v3,
    evaluate_product_command_authority_v3,
    product_transition_commit_identity_from_authority_v3,
)
from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    DependentBuyCandidateAuthorityV2,
    DependentBuySellDependencyV2,
    DependentBuySettledProceedsRefV2,
    KernelProductContractError,
    ProductCommandDispositionV3,
    ProductCommandEvaluationEvidenceV3,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    AlgoTransitionV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    KernelProjectionTypeV1,
    MiniQMTRiskDecisionReceiptV1,
    OMSPreflightDecisionV1,
    OMSPreflightProjectionReceiptV1,
    OrderTypeV1,
    SideV1,
    RiskDecisionActionV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    algo_transition_id_v1,
)


NOW = datetime(2026, 8, 1, 1, 31, tzinfo=UTC)


def _authority():
    return _v2_candidate()[2]


def _evidence_for_authority(
    command: BrokerCommandV2,
    *,
    dependent: bool = False,
    oms_reason: str | None = None,
    event_id: str = "event_k6",
    delivery_id: str = "delivery_k6",
) -> ProductCommandEvaluationEvidenceV3:
    authority = _authority()
    source_command = command
    if dependent and command.runtime_id != "runtime_k6":
        source_command = BrokerCommandV2.create(
            command_type=command.command_type,
            runtime_id="runtime_k6",
            algo_instance_id=command.algo_instance_id,
            parent_intent_id=command.parent_intent_id,
            transition_id=command.transition_id,
            ordinal=command.ordinal,
            local_vt_orderid=None,
            symbol=command.symbol,
            side=command.side,
            order_type=command.order_type,
            price_decimal=command.price_decimal,
            quantity=command.quantity,
            owned_broker_order_id=command.owned_broker_order_id,
            reason_code=command.reason_code,
            metadata=thaw_json_v1(command.metadata),
        )
    base = _v3_evidence(command=source_command, dependent=dependent)
    route = authority.route_compatibility_receipt
    refs = tuple(
        ExecutionProjectionRefV1.create(
            projection_type=item.projection_type,
            projection_id=item.projection_id,
            projection_version=item.projection_version,
            payload_sha256=item.payload_sha256,
            source_event_id=event_id,
            logical_at_utc=item.logical_at_utc,
        )
        for item in base.execution_projection_set.ordered_projection_refs
        if item.projection_type is not KernelProjectionTypeV1.ROUTE_COMPATIBILITY
    ) + (
        ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.ROUTE_COMPATIBILITY,
            projection_id="mqroutecompat_" + route.receipt_sha256,
            projection_version=route.schema_version,
            payload_sha256=route.receipt_sha256,
            source_event_id=event_id,
            logical_at_utc=NOW,
        ),
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        event_id=event_id,
        delivery_id=delivery_id,
        projection_refs=tuple(sorted(refs, key=lambda item: (item.projection_type.value, item.projection_id))),
    )
    oms = base.oms_preflight_receipt
    if oms.runtime_id != command.runtime_id:
        oms = OMSPreflightProjectionReceiptV1.create(
            runtime_id=command.runtime_id,
            algo_instance_id=command.algo_instance_id,
            parent_intent_id=command.parent_intent_id,
            child_order_id=base.oms_preflight_receipt.child_order_id,
            order_intent_id=base.oms_preflight_receipt.order_intent_id,
            strategy_slot_id=base.oms_preflight_receipt.strategy_slot_id,
            account_projection_sha256=base.oms_preflight_receipt.account_projection_sha256,
            cash_fact_sha256=base.oms_preflight_receipt.cash_fact_sha256,
            lot_fact_sha256=base.oms_preflight_receipt.lot_fact_sha256,
            open_order_fact_sha256=base.oms_preflight_receipt.open_order_fact_sha256,
            decision=base.oms_preflight_receipt.decision,
            reason_code=base.oms_preflight_receipt.reason_code,
            logical_at_utc=base.oms_preflight_receipt.logical_at_utc,
        )
    risk = MiniQMTRiskDecisionReceiptV1.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        event_id=event_id,
        child_order_id=base.mini_qmt_risk_decision_receipt.child_order_id,
        decision_stage=base.mini_qmt_risk_decision_receipt.decision_stage,
        action=base.mini_qmt_risk_decision_receipt.action,
        reason_code=base.mini_qmt_risk_decision_receipt.reason_code,
        reason=base.mini_qmt_risk_decision_receipt.reason,
        metadata=thaw_json_v1(base.mini_qmt_risk_decision_receipt.metadata),
        logical_at_utc=base.mini_qmt_risk_decision_receipt.logical_at_utc,
    )
    candidate = base.dependent_buy_candidate
    if candidate is not None and candidate.runtime_id != command.runtime_id:
        adjusted_dependencies = []
        for dependency in candidate.ordered_sell_dependencies:
            adjusted_refs = tuple(
                DependentBuySettledProceedsRefV2.create(
                    **{
                        **ref.model_dump(
                            mode="python",
                            exclude={"schema_version", "proceeds_ref_sha256"},
                        ),
                        "runtime_id": command.runtime_id,
                    }
                )
                for ref in dependency.ordered_settled_proceeds_refs
            )
            adjusted_dependencies.append(
                DependentBuySellDependencyV2.create(
                    runtime_id=command.runtime_id,
                    strategy_id=dependency.strategy_id,
                    sell_parent_intent_id=dependency.sell_parent_intent_id,
                    sell_algo_instance_id=dependency.sell_algo_instance_id,
                    latest_order_fact_id=dependency.latest_order_fact_id,
                    latest_order_fact_sha256=dependency.latest_order_fact_sha256,
                    ordered_settled_proceeds_refs=adjusted_refs,
                    dependency_status=dependency.dependency_status,
                )
            )
        candidate = DependentBuyCandidateAuthorityV2.create(
            **{
                **candidate.model_dump(
                    mode="python",
                    exclude={"schema_version", "candidate_sha256", "ordered_sell_dependencies"},
                ),
                "runtime_id": command.runtime_id,
                "buy_algo_instance_id": command.algo_instance_id,
                "buy_parent_intent_id": command.parent_intent_id,
                "command_id": command.command_id,
                "oms_preflight_receipt_id": oms.receipt_id,
                "oms_preflight_receipt_sha256": oms.receipt_sha256,
                "ordered_sell_dependencies": tuple(adjusted_dependencies),
            }
        )
    if oms_reason is not None:
        oms = OMSPreflightProjectionReceiptV1.create(
            runtime_id=command.runtime_id,
            algo_instance_id=command.algo_instance_id,
            parent_intent_id=command.parent_intent_id,
            child_order_id=base.oms_preflight_receipt.child_order_id,
            order_intent_id=base.oms_preflight_receipt.order_intent_id,
            strategy_slot_id=base.oms_preflight_receipt.strategy_slot_id,
            account_projection_sha256=base.oms_preflight_receipt.account_projection_sha256,
            cash_fact_sha256=base.oms_preflight_receipt.cash_fact_sha256,
            lot_fact_sha256=base.oms_preflight_receipt.lot_fact_sha256,
            open_order_fact_sha256=base.oms_preflight_receipt.open_order_fact_sha256,
            decision=OMSPreflightDecisionV1.REJECT,
            reason_code=oms_reason,
            logical_at_utc=NOW,
        )
        candidate = None
    return ProductCommandEvaluationEvidenceV3.create(
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        event_id=event_id,
        delivery_id=delivery_id,
        transition_id=command.transition_id,
        effect_ordinal=command.ordinal,
        command_id=command.command_id,
        oms_preflight_receipt=oms,
        mini_qmt_risk_decision_receipt=risk,
        plugin_route_compatibility_receipt=route,
        market_data_projection=thaw_json_v1(base.market_data_projection),
        account_projection=thaw_json_v1(base.account_projection),
        contract_projection=thaw_json_v1(base.contract_projection),
        kill_switch_state=thaw_json_v1(base.kill_switch_state),
        execution_projection_set=projection_set,
        dependent_buy_candidate=candidate,
    )


@pytest.mark.parametrize(
    ("dependent", "oms_reason", "expected"),
    (
        (False, None, ProductCommandDispositionV3.MATERIALIZE),
        (True, None, ProductCommandDispositionV3.DEFER_DEPENDENT_BUY),
        (False, "INSUFFICIENT_CASH", ProductCommandDispositionV3.REJECT_SYNCHRONOUS),
    ),
)
def test_product_command_evaluator_uses_exact_frozen_authority(
    dependent: bool,
    oms_reason: str | None,
    expected: ProductCommandDispositionV3,
) -> None:
    authority = _authority()
    command = _v3_command()
    evidence = _evidence_for_authority(command, dependent=dependent, oms_reason=oms_reason)

    item = evaluate_product_command_authority_v3(
        command=command,
        evidence=evidence,
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
    )

    assert item.disposition is expected
    assert item.command_json == command
    assert item.evaluation_evidence == evidence
    assert item.outbox_id == (
        None if expected is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY else command.command_id
    )
    assert (item.reject_reason_code is not None) is (expected is ProductCommandDispositionV3.REJECT_SYNCHRONOUS)
    assert (item.coordination_id is not None) is (expected is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)


def test_product_command_evaluator_accepts_exact_multi_reason_dependent_buy_subset() -> None:
    authority = _authority()
    command = _v3_command()
    evidence = _evidence_for_authority(command, dependent=True)
    candidate = evidence.dependent_buy_candidate
    assert candidate is not None
    multi_reason_candidate = DependentBuyCandidateAuthorityV2.create(
        **candidate.model_dump(
            mode="python",
            exclude={"schema_version", "ordered_error_codes", "candidate_sha256"},
        ),
        ordered_error_codes=("ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED", "SELL_PROCEEDS_REQUIRED"),
    )
    evidence = ProductCommandEvaluationEvidenceV3.create(
        **evidence.model_dump(
            mode="python",
            exclude={"schema_version", "dependent_buy_candidate", "evidence_sha256"},
        ),
        dependent_buy_candidate=multi_reason_candidate,
    )

    item = evaluate_product_command_authority_v3(
        command=command,
        evidence=evidence,
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
    )

    assert item.disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY


def test_product_command_evaluator_rejects_catalog_binding_drift_fail_loud() -> None:
    authority = _authority()
    command = _v3_command()
    evidence = _evidence_for_authority(command)
    forged = authority.model_copy(
        update={
            "plugin_catalog_snapshot": authority.plugin_catalog_snapshot.model_copy(update={"catalog_sha256": "0" * 64})
        }
    )

    with pytest.raises(KernelProductContractError) as raised:
        evaluate_product_command_authority_v3(
            command=command,
            evidence=evidence,
            catalog=authority.plugin_catalog_snapshot,
            creation_binding=forged,
        )

    assert raised.value.reason_code == "MINIQMT_K6_PRODUCT_AUTHORITY_INVALID"
    assert raised.value.context["stage"] == "STRICT_INPUT_READBACK"


def test_product_command_evaluator_preserves_exact_cancel_to_submit_lineage() -> None:
    authority = _authority()
    submit = _v3_command()
    submit_item = evaluate_product_command_authority_v3(
        command=submit,
        evidence=_evidence_for_authority(submit),
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
    )
    cancel = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=submit.runtime_id,
        algo_instance_id=submit.algo_instance_id,
        parent_intent_id=submit.parent_intent_id,
        transition_id=submit.transition_id,
        ordinal=1,
        local_vt_orderid=submit.local_vt_orderid,
        symbol=submit.symbol,
        side=submit.side,
        order_type=submit.order_type,
        price_decimal=submit.price_decimal,
        quantity=submit.quantity,
        owned_broker_order_id="qmt_order_accepted_1",
        reason_code="PLUGIN_CANCEL",
        metadata={"submit_command_id": submit.command_id},
    )

    cancel_item = evaluate_product_command_authority_v3(
        command=cancel,
        evidence=_evidence_for_authority(cancel),
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
    )

    assert cancel_item.disposition is ProductCommandDispositionV3.MATERIALIZE
    assert cancel_item.outbox_id == cancel.command_id
    assert (cancel_item.mapping_id, cancel_item.child_order_id) == (
        submit_item.mapping_id,
        submit_item.child_order_id,
    )


def test_product_command_evaluator_rejects_ambiguous_cancel_lineage_fail_loud() -> None:
    authority = _authority()
    submit = _v3_command()
    cancel = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=submit.runtime_id,
        algo_instance_id=submit.algo_instance_id,
        parent_intent_id=submit.parent_intent_id,
        transition_id=submit.transition_id,
        ordinal=1,
        local_vt_orderid=submit.local_vt_orderid,
        symbol=submit.symbol,
        side=submit.side,
        order_type=submit.order_type,
        price_decimal=submit.price_decimal,
        quantity=submit.quantity,
        owned_broker_order_id="qmt_order_accepted_1",
        reason_code="PLUGIN_CANCEL",
        metadata={"submit_command_id": submit.command_id, "mapping_id": "mqmap_ambiguous"},
    )

    with pytest.raises(KernelProductContractError) as raised:
        evaluate_product_command_authority_v3(
            command=cancel,
            evidence=_evidence_for_authority(cancel),
            catalog=authority.plugin_catalog_snapshot,
            creation_binding=authority,
        )

    assert raised.value.reason_code == "MINIQMT_K6_PRODUCT_AUTHORITY_INVALID"
    assert raised.value.context["stage"] == "CANCEL_MAPPING_LINEAGE"


@pytest.mark.parametrize(
    "metadata",
    (
        {"mapping_id": "mqmap_existing"},
        {"mapping_id": " bad ", "child_order_id": "mqchild_existing"},
    ),
)
def test_product_command_evaluator_rejects_incomplete_or_noncanonical_mapping_lineage(
    metadata: dict[str, str],
) -> None:
    authority = _authority()
    submit = _v3_command()
    cancel = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=submit.runtime_id,
        algo_instance_id=submit.algo_instance_id,
        parent_intent_id=submit.parent_intent_id,
        transition_id=submit.transition_id,
        ordinal=1,
        local_vt_orderid=submit.local_vt_orderid,
        symbol=submit.symbol,
        side=submit.side,
        order_type=submit.order_type,
        price_decimal=submit.price_decimal,
        quantity=submit.quantity,
        owned_broker_order_id="qmt_order_accepted_1",
        reason_code="PLUGIN_CANCEL",
        metadata=metadata,
    )

    with pytest.raises(KernelProductContractError) as raised:
        evaluate_product_command_authority_v3(
            command=cancel,
            evidence=_evidence_for_authority(cancel),
            catalog=authority.plugin_catalog_snapshot,
            creation_binding=authority,
        )

    assert raised.value.context["stage"] == "CANCEL_MAPPING_LINEAGE"


def test_product_command_evaluator_accepts_exact_mapping_and_child_cancel_lineage() -> None:
    authority = _authority()
    submit = _v3_command()
    submit_item = evaluate_product_command_authority_v3(
        command=submit,
        evidence=_evidence_for_authority(submit),
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
    )
    cancel = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=submit.runtime_id,
        algo_instance_id=submit.algo_instance_id,
        parent_intent_id=submit.parent_intent_id,
        transition_id=submit.transition_id,
        ordinal=1,
        local_vt_orderid=submit.local_vt_orderid,
        symbol=submit.symbol,
        side=submit.side,
        order_type=submit.order_type,
        price_decimal=submit.price_decimal,
        quantity=submit.quantity,
        owned_broker_order_id="qmt_order_accepted_1",
        reason_code="PLUGIN_CANCEL",
        metadata={"mapping_id": submit_item.mapping_id, "child_order_id": submit_item.child_order_id},
    )

    cancel_item = evaluate_product_command_authority_v3(
        command=cancel,
        evidence=_evidence_for_authority(cancel),
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
    )

    assert (cancel_item.mapping_id, cancel_item.child_order_id) == (
        submit_item.mapping_id,
        submit_item.child_order_id,
    )


def test_product_command_evaluator_rejects_candidate_without_exact_oms_rejection() -> None:
    authority = _authority()
    command = _v3_command()
    evidence = _evidence_for_authority(command, dependent=True)
    oms = evidence.oms_preflight_receipt
    accepted_oms = OMSPreflightProjectionReceiptV1.create(
        runtime_id=oms.runtime_id,
        algo_instance_id=oms.algo_instance_id,
        parent_intent_id=oms.parent_intent_id,
        child_order_id=oms.child_order_id,
        order_intent_id=oms.order_intent_id,
        strategy_slot_id=oms.strategy_slot_id,
        account_projection_sha256=oms.account_projection_sha256,
        cash_fact_sha256=oms.cash_fact_sha256,
        lot_fact_sha256=oms.lot_fact_sha256,
        open_order_fact_sha256=oms.open_order_fact_sha256,
        decision=OMSPreflightDecisionV1.PASS,
        reason_code="ACCEPTED",
        logical_at_utc=oms.logical_at_utc,
    )
    evidence = ProductCommandEvaluationEvidenceV3.create(
        **evidence.model_dump(
            mode="python",
            exclude={"schema_version", "oms_preflight_receipt", "evidence_sha256"},
        ),
        oms_preflight_receipt=accepted_oms,
    )

    with pytest.raises(KernelProductContractError) as raised:
        evaluate_product_command_authority_v3(
            command=command,
            evidence=evidence,
            catalog=authority.plugin_catalog_snapshot,
            creation_binding=authority,
        )

    assert raised.value.context["stage"] == "DEPENDENT_BUY_CANDIDATE_CLOSURE"


def test_product_command_evaluator_applies_exact_risk_kill_switch_rejection() -> None:
    authority = _authority()
    command = _v3_command()
    evidence = _evidence_for_authority(command)
    risk = evidence.mini_qmt_risk_decision_receipt
    killed_risk = MiniQMTRiskDecisionReceiptV1.create(
        runtime_id=risk.runtime_id,
        algo_instance_id=risk.algo_instance_id,
        event_id=risk.event_id,
        child_order_id=risk.child_order_id,
        decision_stage=risk.decision_stage,
        action=RiskDecisionActionV1.KILL_SWITCH,
        reason_code="MINIQMT_RISK_KILL_SWITCH",
        reason="risk kill switch active",
        metadata=thaw_json_v1(risk.metadata),
        logical_at_utc=risk.logical_at_utc,
    )
    evidence = ProductCommandEvaluationEvidenceV3.create(
        **evidence.model_dump(
            mode="python",
            exclude={"schema_version", "mini_qmt_risk_decision_receipt", "evidence_sha256"},
        ),
        mini_qmt_risk_decision_receipt=killed_risk,
    )

    item = evaluate_product_command_authority_v3(
        command=command,
        evidence=evidence,
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
    )

    assert item.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS
    assert item.reject_reason_code == "MINIQMT_RISK_KILL_SWITCH"


def _aggregate_fixture():
    authority = _authority()
    transition_id = algo_transition_id_v1(
        delivery_id="delivery_k6",
        event_id="event_k6",
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        transition_sequence=1,
    )
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        parent_intent_id="intent_buy",
        transition_id=transition_id,
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.5",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="PLUGIN_SUBMIT",
        metadata={},
    )
    evidence = _evidence_for_authority(command)
    state_payload = {"test": "k6-c1"}
    state = AlgoStateSnapshotV2(
        schema_version="execution_algo_state_snapshot_v2",
        algo_instance_id=command.algo_instance_id,
        plugin_id=authority.manifest.plugin_id,
        plugin_version=authority.manifest.plugin_version,
        plugin_manifest_sha256=authority.manifest.manifest_sha256,
        state_schema_version=authority.manifest.state_schema_version,
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id=evidence.delivery_id,
        last_closed_delivery_sequence=1,
        state=state_payload,
        state_sha256=hash_hex_v1("execution_algo_state_v2", state_payload),
        last_applied_event_id=evidence.event_id,
        updated_at_utc=NOW,
    )
    effect_payload = {
        "next_state_sha256": state.state_sha256,
        "ordered_command_ids": [command.command_id],
        "ordered_timer_mutation_ids": [],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=state,
        broker_commands=(command,),
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect_payload),
    )
    receipt = AlgoTransitionReceiptV1.create(
        delivery_id=evidence.delivery_id,
        event_id=evidence.event_id,
        runtime_id=evidence.runtime_id,
        algo_instance_id=evidence.algo_instance_id,
        plugin_id=authority.manifest.plugin_id,
        plugin_version=authority.manifest.plugin_version,
        plugin_manifest_sha256=authority.manifest.manifest_sha256,
        transition_sequence=1,
        before_state_sha256_or_INIT="INIT",
        after_state_sha256=state.state_sha256,
        ordered_command_ids=(command.command_id,),
        ordered_timer_mutation_ids=(),
        ordered_diagnostic_observation_ids=(),
        ordered_consumed_lineage_refs=(),
        execution_projection_set_sha256=evidence.execution_projection_set.projection_set_sha256,
        effect_set_sha256=transition.effect_set_sha256,
        terminal_outcome=None,
        logical_applied_at_utc=NOW,
        transaction_commit_identity="mqtx_k6_c1_test",
    )
    receipt = bind_product_transition_receipt_v3(
        transition=transition,
        transition_receipt=receipt,
        ordered_evidence=(evidence,),
        timer_schedules=(),
    )
    return authority, transition, receipt, evidence


def test_product_authority_builder_closes_exact_transition_command_set_and_zero_command() -> None:
    authority, transition, receipt, evidence = _aggregate_fixture()
    aggregate = build_product_command_authority_set_v3(
        transition=transition,
        transition_receipt=receipt,
        projection_set=evidence.execution_projection_set,
        ordered_evidence=(evidence,),
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
        timer_schedules=(),
    )
    assert aggregate.total_count == 1
    assert aggregate.ordered_items[0].command_json == transition.broker_commands[0]
    assert aggregate.catalog_sha256 == authority.plugin_catalog_snapshot.catalog_sha256
    assert aggregate.creation_binding_sha256 == authority.authority_input_sha256
    assert aggregate.facade_conformance_set_sha256 == authority.facade_conformance_set_v2.receipt_set_sha256

    state = transition.next_state
    empty_effect_payload = {
        "next_state_sha256": state.state_sha256,
        "ordered_command_ids": [],
        "ordered_timer_mutation_ids": [],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    empty_transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=state,
        broker_commands=(),
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", empty_effect_payload),
    )
    empty_receipt = AlgoTransitionReceiptV1.create(
        **receipt.model_dump(
            mode="python",
            exclude={
                "schema_version",
                "transition_id",
                "ordered_command_ids",
                "command_set_sha256",
                "effect_set_sha256",
                "receipt_sha256",
            },
        ),
        ordered_command_ids=(),
        effect_set_sha256=empty_transition.effect_set_sha256,
    )
    empty_receipt = bind_product_transition_receipt_v3(
        transition=empty_transition,
        transition_receipt=empty_receipt,
        ordered_evidence=(),
        timer_schedules=(),
    )
    zero = build_product_command_authority_set_v3(
        transition=empty_transition,
        transition_receipt=empty_receipt,
        projection_set=evidence.execution_projection_set,
        ordered_evidence=(),
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
        timer_schedules=(),
    )
    assert zero.total_count == 0
    assert zero.aggregate_disposition.value == "ZERO_COMMAND"


def test_product_authority_builder_rejects_missing_or_reordered_evidence() -> None:
    authority, transition, receipt, evidence = _aggregate_fixture()
    with pytest.raises(KernelProductContractError, match="exact transition command set"):
        build_product_command_authority_set_v3(
            transition=transition,
            transition_receipt=receipt,
            projection_set=evidence.execution_projection_set,
            ordered_evidence=(),
            catalog=authority.plugin_catalog_snapshot,
            creation_binding=authority,
            timer_schedules=(),
        )
    with pytest.raises(KernelProductContractError, match="strict tuple"):
        build_product_command_authority_set_v3(
            transition=transition,
            transition_receipt=receipt,
            projection_set=evidence.execution_projection_set,
            ordered_evidence=[],  # type: ignore[arg-type]
            catalog=authority.plugin_catalog_snapshot,
            creation_binding=authority,
            timer_schedules=(),
        )


def test_product_transaction_readback_rejects_timer_cardinality_drift() -> None:
    authority_input, transition, receipt, evidence = _aggregate_fixture()
    aggregate = build_product_command_authority_set_v3(
        transition=transition,
        transition_receipt=receipt,
        projection_set=evidence.execution_projection_set,
        ordered_evidence=(evidence,),
        catalog=authority_input.plugin_catalog_snapshot,
        creation_binding=authority_input,
        timer_schedules=(),
    )

    with pytest.raises(KernelProductContractError) as raised:
        product_transition_commit_identity_from_authority_v3(
            authority=aggregate,
            transition_receipt=receipt,
            timer_schedules=(object(),),  # type: ignore[arg-type]
            diagnostic_observations=(),
        )

    assert raised.value.context["stage"] == "PRODUCT_TRANSACTION_IDENTITY_READBACK"


def test_product_transaction_identity_binds_exact_initial_timer_schedule_receipt() -> None:
    _, base_transition, base_receipt, evidence = _aggregate_fixture()
    timer = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=base_receipt.algo_instance_id,
        transition_id=base_receipt.transition_id,
        ordinal=1,
        timer_name="next_slice",
        schedule_epoch="session_k6c1",
        due_at_exchange_utc="2026-08-01T01:32:00Z",
        catch_up_policy="EXPIRE_IF_LATE",
        payload={"slice": 2},
    )
    schedules = tuple(
        ExecutionAlgoTimerScheduleV1.create(
            runtime_id=base_receipt.runtime_id,
            mutation=timer,
            status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
            emitted_event_id=None,
            lease_owner=None,
            lease_epoch=0,
            lease_fence_token=None,
            lease_expires_at_utc=None,
            row_version=1,
            created_at_utc=created_at,
            updated_at_utc=created_at,
            closed_at_utc=None,
        )
        for created_at in ("2026-08-01T01:31:00Z", "2026-08-01T01:31:01Z")
    )
    effect_payload = {
        "next_state_sha256": base_transition.next_state.state_sha256,
        "ordered_command_ids": [base_transition.broker_commands[0].command_id],
        "ordered_timer_mutation_ids": [timer.mutation_identity_v1()],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=base_transition.next_state,
        broker_commands=base_transition.broker_commands,
        timer_mutations=(timer,),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect_payload),
    )
    provisional = AlgoTransitionReceiptV1.create(
        **base_receipt.model_dump(
            mode="python",
            exclude={
                "schema_version",
                "transition_id",
                "ordered_timer_mutation_ids",
                "timer_set_sha256",
                "effect_set_sha256",
                "transaction_commit_identity",
                "receipt_sha256",
            },
        ),
        ordered_timer_mutation_ids=(timer.mutation_identity_v1(),),
        effect_set_sha256=transition.effect_set_sha256,
        transaction_commit_identity="mqtx_timer_provisional",
    )

    first = bind_product_transition_receipt_v3(
        transition=transition,
        transition_receipt=provisional,
        ordered_evidence=(evidence,),
        timer_schedules=(schedules[0],),
    )
    second = bind_product_transition_receipt_v3(
        transition=transition,
        transition_receipt=provisional,
        ordered_evidence=(evidence,),
        timer_schedules=(schedules[1],),
    )

    assert first.transaction_commit_identity != second.transaction_commit_identity
    with pytest.raises(KernelProductContractError, match="timer schedules"):
        bind_product_transition_receipt_v3(
            transition=transition,
            transition_receipt=provisional,
            ordered_evidence=(evidence,),
            timer_schedules=(),
        )
    wrong_timer = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=base_receipt.algo_instance_id,
        transition_id=base_receipt.transition_id,
        ordinal=1,
        timer_name="wrong_slice",
        schedule_epoch="session_k6c1",
        due_at_exchange_utc="2026-08-01T01:32:00Z",
        catch_up_policy="EXPIRE_IF_LATE",
        payload={"slice": 2},
    )
    wrong_schedule = ExecutionAlgoTimerScheduleV1.create(
        runtime_id=base_receipt.runtime_id,
        mutation=wrong_timer,
        status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
        emitted_event_id=None,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=1,
        created_at_utc="2026-08-01T01:31:00Z",
        updated_at_utc="2026-08-01T01:31:00Z",
        closed_at_utc=None,
    )
    with pytest.raises(KernelProductContractError, match="differs from its durable schedule"):
        bind_product_transition_receipt_v3(
            transition=transition,
            transition_receipt=provisional,
            ordered_evidence=(evidence,),
            timer_schedules=(wrong_schedule,),
        )
    with pytest.raises(KernelProductContractError, match="different evaluation evidence"):
        bind_product_transition_receipt_v3(
            transition=transition,
            transition_receipt=provisional,
            ordered_evidence=(),
            timer_schedules=(schedules[0],),
        )


def test_product_authority_builder_handles_exact_mixed_n_command_set() -> None:
    authority, original_transition, original_receipt, _ = _aggregate_fixture()
    commands = tuple(
        BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
            runtime_id="runtime_k6",
            algo_instance_id="algo_buy",
            parent_intent_id="intent_buy",
            transition_id=original_receipt.transition_id,
            ordinal=ordinal,
            local_vt_orderid=None,
            symbol=f"60000{ordinal}.SH",
            side=SideV1.BUY,
            order_type=OrderTypeV1.LIMIT,
            price_decimal=str(10 + ordinal),
            quantity=100,
            owned_broker_order_id=None,
            reason_code="PLUGIN_SUBMIT",
            metadata={"slice": ordinal},
        )
        for ordinal in range(3)
    )
    evidences = (
        _evidence_for_authority(commands[0]),
        _evidence_for_authority(commands[1], oms_reason="INSUFFICIENT_CASH"),
        _evidence_for_authority(commands[2], dependent=True),
    )
    effect_payload = {
        "next_state_sha256": original_transition.next_state.state_sha256,
        "ordered_command_ids": [item.command_id for item in commands],
        "ordered_timer_mutation_ids": [],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=original_transition.next_state,
        broker_commands=commands,
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect_payload),
    )
    provisional = AlgoTransitionReceiptV1.create(
        **original_receipt.model_dump(
            mode="python",
            exclude={
                "schema_version",
                "transition_id",
                "ordered_command_ids",
                "command_set_sha256",
                "effect_set_sha256",
                "transaction_commit_identity",
                "receipt_sha256",
            },
        ),
        ordered_command_ids=tuple(item.command_id for item in commands),
        effect_set_sha256=transition.effect_set_sha256,
        transaction_commit_identity="mqtx_mixed_provisional",
    )
    receipt = bind_product_transition_receipt_v3(
        transition=transition,
        transition_receipt=provisional,
        ordered_evidence=evidences,
        timer_schedules=(),
    )
    aggregate = build_product_command_authority_set_v3(
        transition=transition,
        transition_receipt=receipt,
        projection_set=evidences[0].execution_projection_set,
        ordered_evidence=evidences,
        catalog=authority.plugin_catalog_snapshot,
        creation_binding=authority,
        timer_schedules=(),
    )
    assert (aggregate.materialize_count, aggregate.reject_count, aggregate.defer_count, aggregate.total_count) == (
        1,
        1,
        1,
        3,
    )
    assert aggregate.aggregate_disposition.value == "MIXED_PER_COMMAND"

    first_deferred = _evidence_for_authority(commands[0], dependent=True)
    first_deferred = ProductCommandEvaluationEvidenceV3.create(
        **first_deferred.model_dump(
            mode="python",
            exclude={"schema_version", "execution_projection_set", "evidence_sha256"},
        ),
        execution_projection_set=evidences[0].execution_projection_set,
    )
    duplicate_coordination_evidences = (first_deferred, evidences[1], evidences[2])
    duplicate_receipt = bind_product_transition_receipt_v3(
        transition=transition,
        transition_receipt=provisional,
        ordered_evidence=duplicate_coordination_evidences,
        timer_schedules=(),
    )
    with pytest.raises(KernelProductContractError) as raised:
        build_product_command_authority_set_v3(
            transition=transition,
            transition_receipt=duplicate_receipt,
            projection_set=evidences[0].execution_projection_set,
            ordered_evidence=duplicate_coordination_evidences,
            catalog=authority.plugin_catalog_snapshot,
            creation_binding=authority,
            timer_schedules=(),
        )
    assert raised.value.context["stage"] == "DEPENDENT_BUY_COORDINATION_CARDINALITY"
