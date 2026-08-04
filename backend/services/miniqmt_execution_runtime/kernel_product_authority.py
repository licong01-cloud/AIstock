"""Pure K6-C1 product command authority evaluation and aggregation.

The evaluator consumes only frozen, hash-closed inputs.  It never calls OMS,
risk, a gateway, a broker, or a process-local plugin binding.
"""

from __future__ import annotations

from typing import Any

from backend.execution_algos.vnpy_compat.facade_contracts import VnpyFacadeAuthorityInputV2

from .kernel_product_contracts import (
    DependentBuyCandidateAuthorityV2,
    KernelProductContractError,
    ProductCommandAuthorityItemV3,
    ProductCommandAuthoritySetV3,
    ProductCommandDispositionV3,
    ProductCommandEvaluationEvidenceV3,
)
from .plugin_canonical import hash_hex_v1, json_safe_evidence_v1, thaw_json_v1
from .plugin_contracts import (
    AlgoTransitionReceiptV1,
    AlgoTransitionV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    DiagnosticObservationV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionProjectionSetV1,
    MiniQMTPluginContractError,
    OMSPreflightDecisionV1,
    RiskDecisionActionV1,
    SideV1,
    command_child_mapping_id_v1,
    execution_child_order_id_v1,
    transaction_commit_identity_v1,
)
from .plugin_registry import CompatibilityStatusV1, PluginCatalogSnapshotV1


_DEPENDENT_BUY_REASON_CODES = frozenset(
    {
        "SELL_PROCEEDS_REQUIRED",
        "ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED",
    }
)


def _product_authority_invalid(message: str, *, stage: str, **context: Any) -> KernelProductContractError:
    return KernelProductContractError(
        "MINIQMT_K6_PRODUCT_AUTHORITY_INVALID",
        message,
        context={"stage": stage, **context},
    )


def _strict_product_tuple_v3(
    values: Any,
    *,
    model_type: type[Any],
    field_name: str,
    stage: str,
) -> tuple[Any, ...]:
    """Strictly rebuild every public tuple item before business-field access."""

    if type(values) is not tuple:
        raise _product_authority_invalid(
            f"{field_name} must be one strict tuple",
            stage=stage,
            field_name=field_name,
            actual_type=type(values).__name__,
        )
    rebuilt: list[Any] = []
    for ordinal, value in enumerate(values):
        try:
            if not isinstance(value, model_type):
                raise TypeError(f"item must be {model_type.__name__}")
            rebuilt.append(model_type.model_validate_json(value.model_dump_json(), strict=True))
        except (AttributeError, TypeError, ValueError) as exc:
            raise _product_authority_invalid(
                f"{field_name} item failed strict readback",
                stage=stage,
                field_name=field_name,
                item_ordinal=ordinal,
                actual_type=type(value).__name__,
                error_type=type(exc).__name__,
                error=json_safe_evidence_v1(exc),
            ) from exc
    return tuple(rebuilt)


def _strict_transition_inputs_v3(
    *,
    transition: Any,
    transition_receipt: Any,
    stage: str,
) -> tuple[AlgoTransitionV1, AlgoTransitionReceiptV1]:
    try:
        if not isinstance(transition, AlgoTransitionV1):
            raise TypeError("transition must be AlgoTransitionV1")
        if not isinstance(transition_receipt, AlgoTransitionReceiptV1):
            raise TypeError("transition_receipt must be AlgoTransitionReceiptV1")
        return (
            AlgoTransitionV1.model_validate_json(transition.model_dump_json(), strict=True),
            AlgoTransitionReceiptV1.model_validate_json(transition_receipt.model_dump_json(), strict=True),
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _product_authority_invalid(
            "product transition inputs failed strict readback",
            stage=stage,
            transition_type=type(transition).__name__,
            transition_receipt_type=type(transition_receipt).__name__,
            error_type=type(exc).__name__,
            error=json_safe_evidence_v1(exc),
        ) from exc


def _strict_transition_receipt_v3(value: Any, *, stage: str) -> AlgoTransitionReceiptV1:
    try:
        if not isinstance(value, AlgoTransitionReceiptV1):
            raise TypeError("transition_receipt must be AlgoTransitionReceiptV1")
        return AlgoTransitionReceiptV1.model_validate_json(value.model_dump_json(), strict=True)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _product_authority_invalid(
            "product transition receipt failed strict readback",
            stage=stage,
            actual_type=type(value).__name__,
            error_type=type(exc).__name__,
            error=json_safe_evidence_v1(exc),
        ) from exc


def _strict_inputs_v3(
    *,
    command: BrokerCommandV2,
    evidence: ProductCommandEvaluationEvidenceV3,
    catalog: PluginCatalogSnapshotV1,
    creation_binding: VnpyFacadeAuthorityInputV2,
) -> tuple[BrokerCommandV2, ProductCommandEvaluationEvidenceV3, PluginCatalogSnapshotV1, VnpyFacadeAuthorityInputV2]:
    try:
        command_readback = BrokerCommandV2.model_validate_json(command.model_dump_json())
        evidence_readback = ProductCommandEvaluationEvidenceV3.model_validate_json(evidence.model_dump_json())
        catalog_readback = PluginCatalogSnapshotV1.model_validate_json(catalog.model_dump_json(), strict=True)
        binding_readback = VnpyFacadeAuthorityInputV2.model_validate_json(
            creation_binding.model_dump_json(), strict=True
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _product_authority_invalid(
            "product command authority input failed strict readback",
            stage="STRICT_INPUT_READBACK",
            input_types={
                "command": type(command).__name__,
                "evidence": type(evidence).__name__,
                "catalog": type(catalog).__name__,
                "creation_binding": type(creation_binding).__name__,
            },
            error_type=type(exc).__name__,
            error=json_safe_evidence_v1(exc),
        ) from exc
    if binding_readback.plugin_catalog_snapshot != catalog_readback:
        raise _product_authority_invalid(
            "creation binding catalog differs from supplied product catalog",
            stage="CATALOG_BINDING_CLOSURE",
            expected_catalog_sha256=catalog_readback.catalog_sha256,
            actual_catalog_sha256=binding_readback.plugin_catalog_snapshot.catalog_sha256,
        )
    route = evidence_readback.plugin_route_compatibility_receipt
    if route.plugin_key != binding_readback.plugin_key or route.algo_code != binding_readback.manifest.algo_code:
        raise _product_authority_invalid(
            "command route receipt differs from the frozen creation plugin",
            stage="PLUGIN_BINDING_CLOSURE",
            route_plugin_key=route.plugin_key.canonical_payload_v1(),
            creation_plugin_key=binding_readback.plugin_key.canonical_payload_v1(),
            route_algo_code=route.algo_code,
            creation_algo_code=binding_readback.manifest.algo_code,
        )
    try:
        route.validate_against_authority_v1(
            catalog_snapshot=catalog_readback,
            gateway_catalog=binding_readback.gateway_capability_catalog,
        )
    except MiniQMTPluginContractError as exc:
        raise _product_authority_invalid(
            "route compatibility receipt failed exact catalog/gateway authority readback",
            stage="ROUTE_AUTHORITY_READBACK",
            command_id=command_readback.command_id,
            route_receipt_sha256=route.receipt_sha256,
            authority_reason_code=exc.reason_code.value,
            authority_message=exc.message,
            authority_context=exc.context,
        ) from exc
    except (TypeError, ValueError) as exc:
        raise _product_authority_invalid(
            "route compatibility receipt failed exact catalog/gateway authority readback",
            stage="ROUTE_AUTHORITY_READBACK",
            command_id=command_readback.command_id,
            route_receipt_sha256=route.receipt_sha256,
            error_type=type(exc).__name__,
            error=json_safe_evidence_v1(exc),
        ) from exc
    return command_readback, evidence_readback, catalog_readback, binding_readback


def _command_lineage_v3(command: BrokerCommandV2) -> tuple[str, str]:
    if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
        child_order_id = execution_child_order_id_v1(
            command_id=command.command_id,
            local_vt_orderid=command.local_vt_orderid,
        )
        mapping_id = command_child_mapping_id_v1(
            command_id=command.command_id,
            local_vt_orderid=command.local_vt_orderid,
            child_order_id=child_order_id,
        )
        return mapping_id, child_order_id
    metadata = thaw_json_v1(command.metadata)
    submit_command_id = metadata.get("submit_command_id")
    mapping_id = metadata.get("mapping_id")
    if (submit_command_id is None) == (mapping_id is None):
        raise _product_authority_invalid(
            "CANCEL command lacks one exact existing mapping lineage reference",
            stage="CANCEL_MAPPING_LINEAGE",
            command_id=command.command_id,
        )
    if submit_command_id is not None:
        if (
            type(submit_command_id) is not str
            or not submit_command_id
            or submit_command_id != submit_command_id.strip()
        ):
            raise _product_authority_invalid(
                "CANCEL submit command identity is not canonical",
                stage="CANCEL_MAPPING_LINEAGE",
                command_id=command.command_id,
            )
        child_order_id = execution_child_order_id_v1(
            command_id=submit_command_id,
            local_vt_orderid=command.local_vt_orderid,
        )
        return (
            command_child_mapping_id_v1(
                command_id=submit_command_id,
                local_vt_orderid=command.local_vt_orderid,
                child_order_id=child_order_id,
            ),
            child_order_id,
        )
    if type(mapping_id) is not str or not mapping_id or mapping_id != mapping_id.strip():
        raise _product_authority_invalid(
            "CANCEL mapping identity is not canonical",
            stage="CANCEL_MAPPING_LINEAGE",
            command_id=command.command_id,
        )
    child_order_id = metadata.get("child_order_id")
    if type(child_order_id) is not str or not child_order_id or child_order_id != child_order_id.strip():
        raise _product_authority_invalid(
            "CANCEL mapping reference requires the exact child order identity",
            stage="CANCEL_MAPPING_LINEAGE",
            command_id=command.command_id,
            mapping_id=mapping_id,
        )
    return mapping_id, child_order_id


def _reject_context_v3(
    *,
    command: BrokerCommandV2,
    evidence: ProductCommandEvaluationEvidenceV3,
    reason_code: str,
    source: str,
) -> str:
    return hash_hex_v1(
        "miniqmt_product_command_reject_context_v3",
        {
            "command_id": command.command_id,
            "command_payload_sha256": command.payload_sha256,
            "evidence_sha256": evidence.evidence_sha256,
            "reason_code": reason_code,
            "source": source,
            "oms_preflight_receipt_sha256": evidence.oms_preflight_receipt.receipt_sha256,
            "risk_decision_receipt_sha256": evidence.mini_qmt_risk_decision_receipt.receipt_sha256,
            "route_compatibility_receipt_sha256": evidence.plugin_route_compatibility_receipt.receipt_sha256,
        },
    )


def _dependent_buy_coordination_id_v3(candidate: DependentBuyCandidateAuthorityV2) -> str:
    return hash_hex_v1(
        "miniqmt_dependent_buy_coordination_id_v2",
        {
            "runtime_id": candidate.runtime_id,
            "buy_algo_instance_id": candidate.buy_algo_instance_id,
            "buy_parent_intent_id": candidate.buy_parent_intent_id,
            "strategy_id": candidate.strategy_id,
            "trade_date": candidate.trade_date.isoformat(),
            "virtual_account_id": candidate.virtual_account_id,
        },
    )


def evaluate_product_command_authority_v3(
    *,
    command: BrokerCommandV2,
    evidence: ProductCommandEvaluationEvidenceV3,
    catalog: PluginCatalogSnapshotV1,
    creation_binding: VnpyFacadeAuthorityInputV2,
) -> ProductCommandAuthorityItemV3:
    """Evaluate one exact command without side effects or caller-supplied disposition."""

    command, evidence, _, _ = _strict_inputs_v3(
        command=command,
        evidence=evidence,
        catalog=catalog,
        creation_binding=creation_binding,
    )
    if (
        evidence.command_id != command.command_id
        or evidence.effect_ordinal != command.ordinal
        or evidence.runtime_id != command.runtime_id
        or evidence.algo_instance_id != command.algo_instance_id
        or evidence.transition_id != command.transition_id
    ):
        raise _product_authority_invalid(
            "command and evaluation evidence identities differ",
            stage="COMMAND_EVIDENCE_CLOSURE",
            command_id=command.command_id,
            evidence_command_id=evidence.command_id,
        )
    kill_switch = thaw_json_v1(evidence.kill_switch_state)
    if type(kill_switch.get("active")) is not bool:
        raise _product_authority_invalid(
            "kill-switch projection lacks a strict boolean active fact",
            stage="KILL_SWITCH_AUTHORITY",
            command_id=command.command_id,
            actual_type=type(kill_switch.get("active")).__name__,
        )

    route = evidence.plugin_route_compatibility_receipt
    risk = evidence.mini_qmt_risk_decision_receipt
    oms = evidence.oms_preflight_receipt
    candidate = evidence.dependent_buy_candidate
    disposition = ProductCommandDispositionV3.MATERIALIZE
    reject_reason: str | None = None
    reject_context: str | None = None
    coordination_id: str | None = None

    if route.status is not CompatibilityStatusV1.PASSED:
        disposition = ProductCommandDispositionV3.REJECT_SYNCHRONOUS
        reject_reason = "MINIQMT_PLUGIN_ROUTE_COMPATIBILITY_FAILED"
        reject_context = _reject_context_v3(
            command=command,
            evidence=evidence,
            reason_code=reject_reason,
            source="ROUTE_COMPATIBILITY",
        )
    elif command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT and (
        bool(kill_switch["active"]) or risk.action is RiskDecisionActionV1.KILL_SWITCH
    ):
        disposition = ProductCommandDispositionV3.REJECT_SYNCHRONOUS
        reject_reason = (
            risk.reason_code if risk.action is RiskDecisionActionV1.KILL_SWITCH else "MINIQMT_KILL_SWITCH_ACTIVE"
        )
        reject_context = _reject_context_v3(
            command=command,
            evidence=evidence,
            reason_code=reject_reason,
            source="RISK_OR_KILL_SWITCH",
        )
    elif oms.decision is OMSPreflightDecisionV1.REJECT:
        dependent_buy = (
            command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
            and command.side is SideV1.BUY
            and oms.reason_code in _DEPENDENT_BUY_REASON_CODES
            and candidate is not None
            and oms.reason_code in candidate.ordered_error_codes
            and candidate.oms_preflight_receipt_id == oms.receipt_id
            and candidate.oms_preflight_receipt_sha256 == oms.receipt_sha256
        )
        if dependent_buy:
            disposition = ProductCommandDispositionV3.DEFER_DEPENDENT_BUY
            assert candidate is not None
            coordination_id = _dependent_buy_coordination_id_v3(candidate)
        else:
            disposition = ProductCommandDispositionV3.REJECT_SYNCHRONOUS
            reject_reason = oms.reason_code
            reject_context = _reject_context_v3(
                command=command,
                evidence=evidence,
                reason_code=reject_reason,
                source="OMS_PREFLIGHT",
            )
    elif candidate is not None:
        raise _product_authority_invalid(
            "dependent-BUY candidate is present without the exact OMS rejection",
            stage="DEPENDENT_BUY_CANDIDATE_CLOSURE",
            command_id=command.command_id,
            oms_decision=oms.decision.value,
            oms_reason_code=oms.reason_code,
        )

    mapping_id, child_order_id = _command_lineage_v3(command)
    return ProductCommandAuthorityItemV3.create(
        runtime_id=evidence.runtime_id,
        algo_instance_id=evidence.algo_instance_id,
        event_id=evidence.event_id,
        delivery_id=evidence.delivery_id,
        transition_id=evidence.transition_id,
        effect_ordinal=evidence.effect_ordinal,
        command_json=command,
        evaluation_evidence=evidence,
        plugin_effect_sha256=hash_hex_v1(
            "miniqmt_product_plugin_command_effect_v3",
            {
                "effect_ordinal": command.ordinal,
                "command_id": command.command_id,
                "command_payload_sha256": command.payload_sha256,
            },
        ),
        disposition=disposition,
        reject_reason_code=reject_reason,
        reject_context_sha256=reject_context,
        coordination_id=coordination_id,
        mapping_id=mapping_id,
        outbox_id=None if disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY else command.command_id,
        child_order_id=child_order_id,
    )


def build_product_command_authority_set_v3(
    *,
    transition: AlgoTransitionV1,
    transition_receipt: AlgoTransitionReceiptV1,
    projection_set: ExecutionProjectionSetV1,
    ordered_evidence: tuple[ProductCommandEvaluationEvidenceV3, ...],
    catalog: PluginCatalogSnapshotV1,
    creation_binding: VnpyFacadeAuthorityInputV2,
    timer_schedules: tuple[ExecutionAlgoTimerScheduleV1, ...],
) -> ProductCommandAuthoritySetV3:
    """Build the exact 0..N aggregate from one applied plugin transition."""

    transition, transition_receipt = _strict_transition_inputs_v3(
        transition=transition,
        transition_receipt=transition_receipt,
        stage="AGGREGATE_STRICT_READBACK",
    )
    try:
        projection_set = ExecutionProjectionSetV1.model_validate_json(projection_set.model_dump_json())
        catalog = PluginCatalogSnapshotV1.model_validate_json(catalog.model_dump_json(), strict=True)
        creation_binding = VnpyFacadeAuthorityInputV2.model_validate_json(
            creation_binding.model_dump_json(), strict=True
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _product_authority_invalid(
            "product command aggregate input failed strict readback",
            stage="AGGREGATE_STRICT_READBACK",
            error_type=type(exc).__name__,
            error=json_safe_evidence_v1(exc),
        ) from exc
    ordered_evidence = _strict_product_tuple_v3(
        ordered_evidence,
        model_type=ProductCommandEvaluationEvidenceV3,
        field_name="ordered_evidence",
        stage="AGGREGATE_EVIDENCE_SET",
    )
    commands = transition.broker_commands
    command_ids = tuple(command.command_id for command in commands)
    evidence_keys = tuple((item.effect_ordinal, item.command_id) for item in ordered_evidence)
    command_keys = tuple((item.ordinal, item.command_id) for item in commands)
    if evidence_keys != command_keys or len(set(command_ids)) != len(command_ids):
        raise _product_authority_invalid(
            "ordered evidence is not the exact transition command set",
            stage="AGGREGATE_EVIDENCE_SET",
            expected=command_keys,
            actual=evidence_keys,
        )
    owner = (
        transition_receipt.runtime_id,
        transition_receipt.algo_instance_id,
        transition_receipt.event_id,
        transition_receipt.delivery_id,
    )
    if (
        transition_receipt.ordered_command_ids != command_ids
        or transition_receipt.effect_set_sha256 != transition.effect_set_sha256
        or transition_receipt.after_state_sha256 != transition.next_state.state_sha256
        or transition_receipt.execution_projection_set_sha256 != projection_set.projection_set_sha256
        or owner
        != (
            projection_set.runtime_id,
            projection_set.algo_instance_id,
            projection_set.event_id,
            projection_set.delivery_id,
        )
        or transition_receipt.transition_id
        != (commands[0].transition_id if commands else transition_receipt.transition_id)
    ):
        raise _product_authority_invalid(
            "transition, receipt and projection set do not form one exact authority",
            stage="TRANSITION_RECEIPT_CLOSURE",
            transition_id=transition_receipt.transition_id,
        )
    expected_transaction_identity = product_transition_commit_identity_v3(
        transition=transition,
        transition_receipt=transition_receipt,
        ordered_evidence=ordered_evidence,
        timer_schedules=timer_schedules,
    )
    if transition_receipt.transaction_commit_identity != expected_transaction_identity:
        raise _product_authority_invalid(
            "transition receipt does not use the K6 product transaction identity",
            stage="PRODUCT_TRANSACTION_IDENTITY",
            transition_id=transition_receipt.transition_id,
            expected=expected_transaction_identity,
            actual=transition_receipt.transaction_commit_identity,
        )
    if (
        creation_binding.plugin_catalog_snapshot != catalog
        or creation_binding.manifest.manifest_sha256 != transition_receipt.plugin_manifest_sha256
        or creation_binding.manifest.plugin_id != transition_receipt.plugin_id
        or creation_binding.manifest.plugin_version != transition_receipt.plugin_version
    ):
        raise _product_authority_invalid(
            "transition plugin differs from catalog creation authority",
            stage="TRANSITION_PLUGIN_CLOSURE",
            transition_id=transition_receipt.transition_id,
            plugin_manifest_sha256=transition_receipt.plugin_manifest_sha256,
        )
    items = tuple(
        evaluate_product_command_authority_v3(
            command=command,
            evidence=evidence,
            catalog=catalog,
            creation_binding=creation_binding,
        )
        for command, evidence in zip(commands, ordered_evidence, strict=True)
    )
    coordination_ids = tuple(item.coordination_id for item in items if item.coordination_id is not None)
    if len(coordination_ids) != len(set(coordination_ids)):
        raise _product_authority_invalid(
            "one transition cannot create duplicate dependent-BUY coordination owners",
            stage="DEPENDENT_BUY_COORDINATION_CARDINALITY",
            transition_id=transition_receipt.transition_id,
            coordination_ids=coordination_ids,
        )
    return ProductCommandAuthoritySetV3.create(
        runtime_id=transition_receipt.runtime_id,
        algo_instance_id=transition_receipt.algo_instance_id,
        event_id=transition_receipt.event_id,
        delivery_id=transition_receipt.delivery_id,
        transition_id=transition_receipt.transition_id,
        catalog_sha256=catalog.catalog_sha256,
        creation_binding_sha256=creation_binding.authority_input_sha256,
        facade_conformance_set_sha256=creation_binding.facade_conformance_set_v2.receipt_set_sha256,
        execution_projection_set_sha256=projection_set.projection_set_sha256,
        transition_receipt_sha256=transition_receipt.receipt_sha256,
        ordered_items=items,
    )


def product_transition_commit_identity_v3(
    *,
    transition: AlgoTransitionV1,
    transition_receipt: AlgoTransitionReceiptV1,
    ordered_evidence: tuple[ProductCommandEvaluationEvidenceV3, ...],
    timer_schedules: tuple[ExecutionAlgoTimerScheduleV1, ...],
) -> str:
    """Return the non-circular transaction identity for one K6 product transition."""

    transition, transition_receipt = _strict_transition_inputs_v3(
        transition=transition,
        transition_receipt=transition_receipt,
        stage="PRODUCT_TRANSACTION_IDENTITY",
    )
    ordered_evidence = _strict_product_tuple_v3(
        ordered_evidence,
        model_type=ProductCommandEvaluationEvidenceV3,
        field_name="ordered_evidence",
        stage="PRODUCT_TRANSACTION_IDENTITY",
    )
    timer_schedules = _strict_product_tuple_v3(
        timer_schedules,
        model_type=ExecutionAlgoTimerScheduleV1,
        field_name="timer_schedules",
        stage="PRODUCT_TIMER_SCHEDULE_CLOSURE",
    )
    if len(ordered_evidence) != len(transition.broker_commands):
        raise _product_authority_invalid(
            "product transaction evidence does not close to commands",
            stage="PRODUCT_TRANSACTION_IDENTITY",
            command_count=len(transition.broker_commands),
            evidence_count=len(ordered_evidence),
        )
    command_inputs = tuple(
        {
            "effect_ordinal": command.ordinal,
            "command_id": command.command_id,
            "command_payload_sha256": command.payload_sha256,
            "evaluation_evidence_sha256": evidence.evidence_sha256,
        }
        for command, evidence in zip(transition.broker_commands, ordered_evidence, strict=True)
    )
    if len(timer_schedules) != len(transition.timer_mutations):
        raise _product_authority_invalid(
            "product transaction timer schedules do not close to timer mutations",
            stage="PRODUCT_TIMER_SCHEDULE_CLOSURE",
            timer_mutation_count=len(transition.timer_mutations),
            timer_schedule_count=len(timer_schedules),
        )
    for mutation, schedule in zip(transition.timer_mutations, timer_schedules, strict=True):
        if (
            mutation.schedule_id != schedule.schedule_id
            or mutation.algo_instance_id != schedule.algo_instance_id
            or mutation.timer_name != schedule.timer_name
            or mutation.schedule_epoch != schedule.schedule_epoch
            or (
                mutation.due_at_exchange_utc is not None
                and mutation.due_at_exchange_utc != schedule.due_at_exchange_utc
            )
        ):
            raise _product_authority_invalid(
                "product timer mutation differs from its durable schedule",
                stage="PRODUCT_TIMER_SCHEDULE_CLOSURE",
                mutation_identity=mutation.mutation_identity_v1(),
                schedule_id=schedule.schedule_id,
            )
    return _product_transition_commit_identity_from_parts_v3(
        transition_receipt=transition_receipt,
        effect_set_sha256=transition.effect_set_sha256,
        command_inputs=command_inputs,
        timer_schedule_receipt_sha256s=tuple(item.schedule_receipt_sha256 for item in timer_schedules),
        diagnostic_context_sha256s=tuple(item.context_sha256 for item in transition.diagnostic_observations),
    )


def _product_transition_commit_identity_from_parts_v3(
    *,
    transition_receipt: AlgoTransitionReceiptV1,
    effect_set_sha256: str,
    command_inputs: tuple[dict[str, Any], ...],
    timer_schedule_receipt_sha256s: tuple[str, ...],
    diagnostic_context_sha256s: tuple[str, ...],
) -> str:
    product_input_set_sha256 = hash_hex_v1(
        "miniqmt_product_transition_commit_input_set_v3",
        {
            "transition_id": transition_receipt.transition_id,
            "execution_projection_set_sha256": transition_receipt.execution_projection_set_sha256,
            "after_state_sha256": transition_receipt.after_state_sha256,
            "effect_set_sha256": effect_set_sha256,
            "ordered_command_inputs": list(command_inputs),
            "ordered_timer_schedule_receipt_sha256s": list(timer_schedule_receipt_sha256s),
            "ordered_diagnostic_context_sha256s": list(diagnostic_context_sha256s),
        },
    )
    output_identities = (
        transition_receipt.transition_id,
        *transition_receipt.ordered_command_ids,
        *transition_receipt.ordered_timer_mutation_ids,
        *transition_receipt.ordered_diagnostic_observation_ids,
    )
    return transaction_commit_identity_v1(
        operation="APPLY_CLAIMED_DELIVERY_ATOMIC_PRODUCT_V3",
        owner_identities=(
            transition_receipt.runtime_id,
            transition_receipt.algo_instance_id,
            transition_receipt.event_id,
            transition_receipt.delivery_id,
        ),
        input_hashes=(product_input_set_sha256,),
        output_identities=output_identities,
    )


def product_transition_commit_identity_from_authority_v3(
    *,
    authority: ProductCommandAuthoritySetV3,
    transition_receipt: AlgoTransitionReceiptV1,
    timer_schedules: tuple[ExecutionAlgoTimerScheduleV1, ...],
    diagnostic_observations: tuple[DiagnosticObservationV1, ...],
) -> str:
    """Recompute the product transaction identity from durable V3 command/evidence facts."""

    try:
        if not isinstance(authority, ProductCommandAuthoritySetV3):
            raise TypeError("authority must be ProductCommandAuthoritySetV3")
        authority = ProductCommandAuthoritySetV3.model_validate_json(authority.model_dump_json(), strict=True)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _product_authority_invalid(
            "durable product authority failed strict readback",
            stage="PRODUCT_TRANSACTION_IDENTITY_READBACK",
            actual_type=type(authority).__name__,
            error_type=type(exc).__name__,
            error=json_safe_evidence_v1(exc),
        ) from exc
    transition_receipt = _strict_transition_receipt_v3(
        transition_receipt,
        stage="PRODUCT_TRANSACTION_IDENTITY_READBACK",
    )
    timer_schedules = _strict_product_tuple_v3(
        timer_schedules,
        model_type=ExecutionAlgoTimerScheduleV1,
        field_name="timer_schedules",
        stage="PRODUCT_TRANSACTION_IDENTITY_READBACK",
    )
    diagnostic_observations = _strict_product_tuple_v3(
        diagnostic_observations,
        model_type=DiagnosticObservationV1,
        field_name="diagnostic_observations",
        stage="PRODUCT_TRANSACTION_IDENTITY_READBACK",
    )
    if (
        authority.transition_id != transition_receipt.transition_id
        or authority.transition_receipt_sha256 != transition_receipt.receipt_sha256
        or tuple(item.command_id for item in authority.ordered_items) != transition_receipt.ordered_command_ids
        or tuple(item.observation_id for item in diagnostic_observations)
        != transition_receipt.ordered_diagnostic_observation_ids
        or len(timer_schedules) != len(transition_receipt.ordered_timer_mutation_ids)
    ):
        raise _product_authority_invalid(
            "durable authority and transition receipt identities differ",
            stage="PRODUCT_TRANSACTION_IDENTITY_READBACK",
            authority_set_sha256=authority.authority_set_sha256,
        )
    command_inputs = tuple(
        {
            "effect_ordinal": item.effect_ordinal,
            "command_id": item.command_id,
            "command_payload_sha256": item.command_payload_sha256,
            "evaluation_evidence_sha256": item.evaluation_evidence.evidence_sha256,
        }
        for item in authority.ordered_items
    )
    return _product_transition_commit_identity_from_parts_v3(
        transition_receipt=transition_receipt,
        effect_set_sha256=transition_receipt.effect_set_sha256,
        command_inputs=command_inputs,
        timer_schedule_receipt_sha256s=tuple(item.schedule_receipt_sha256 for item in timer_schedules),
        diagnostic_context_sha256s=tuple(item.context_sha256 for item in diagnostic_observations),
    )


def bind_product_transition_receipt_v3(
    *,
    transition: AlgoTransitionV1,
    transition_receipt: AlgoTransitionReceiptV1,
    ordered_evidence: tuple[ProductCommandEvaluationEvidenceV3, ...],
    timer_schedules: tuple[ExecutionAlgoTimerScheduleV1, ...],
) -> AlgoTransitionReceiptV1:
    """Replace only the repository transaction identity with the exact K6 product identity."""

    transition, transition_receipt = _strict_transition_inputs_v3(
        transition=transition,
        transition_receipt=transition_receipt,
        stage="PRODUCT_TRANSACTION_IDENTITY",
    )
    ordered_evidence = _strict_product_tuple_v3(
        ordered_evidence,
        model_type=ProductCommandEvaluationEvidenceV3,
        field_name="ordered_evidence",
        stage="PRODUCT_TRANSACTION_IDENTITY",
    )
    timer_schedules = _strict_product_tuple_v3(
        timer_schedules,
        model_type=ExecutionAlgoTimerScheduleV1,
        field_name="timer_schedules",
        stage="PRODUCT_TIMER_SCHEDULE_CLOSURE",
    )
    if tuple(item.command_id for item in transition.broker_commands) != transition_receipt.ordered_command_ids:
        raise _product_authority_invalid(
            "cannot bind a receipt to a different transition command set",
            stage="PRODUCT_TRANSACTION_IDENTITY",
            transition_id=transition_receipt.transition_id,
        )
    if tuple((item.effect_ordinal, item.command_id) for item in ordered_evidence) != tuple(
        (item.ordinal, item.command_id) for item in transition.broker_commands
    ):
        raise _product_authority_invalid(
            "cannot bind a receipt to a different evaluation evidence set",
            stage="PRODUCT_TRANSACTION_IDENTITY",
            transition_id=transition_receipt.transition_id,
        )
    transaction_identity = product_transition_commit_identity_v3(
        transition=transition,
        transition_receipt=transition_receipt,
        ordered_evidence=ordered_evidence,
        timer_schedules=timer_schedules,
    )
    payload = transition_receipt.model_dump(
        mode="python",
        exclude={"schema_version", "transition_id", "transaction_commit_identity", "receipt_sha256"},
    )
    return AlgoTransitionReceiptV1.create(
        **payload,
        transaction_commit_identity=transaction_identity,
    )


__all__ = [
    "bind_product_transition_receipt_v3",
    "build_product_command_authority_set_v3",
    "evaluate_product_command_authority_v3",
    "product_transition_commit_identity_v3",
    "product_transition_commit_identity_from_authority_v3",
]
