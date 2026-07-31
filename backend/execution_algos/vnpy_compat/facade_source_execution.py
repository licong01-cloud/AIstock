"""Pure single-process pinned-source executor for K4-B characterization.

Process lifecycle belongs to the services runner.  This module never imports a
repository, network client, broker, subprocess, or wall-clock owner.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    hash_hex_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    BrokerCommandTypeV2,
    CurrentThreeActiveOrderStatusV3,
    DiagnosticSeverityV1,
    EventTypeV2,
    KernelCommandOutcomeEventPayloadV1,
    KernelCommandOutcomeV1,
    KernelOrderEventPayloadV1,
    KernelOrderReconcileEventPayloadV1,
    KernelTradeEventPayloadV1,
    NormalizedOrderStatusV1,
    RuntimeEventEnvelopeV2,
    SideV1,
    TerminalOutcomeV1,
    algo_transition_id_v1,
    execution_child_order_id_v1,
    strict_readback_kernel_event_payload_v1,
    validate_json_schema_instance_v1,
    bounded_exception_summary_v1,
    stable_exception_reason_code_v1,
)

from .facade import VnpyAlgoEngineFacadeV1, VnpyFacadeEffectCollectorV1, VnpyFacadeTraceCollectorV2
from .facade_adapter import state_mapping_set_sha256_v1
from .facade_characterization import (
    PINNED_SOURCE_ROOT,
    VnpyFacadeDeterministicUniformV1,
    _facade_callable_signature_payload_v1,
    build_vnpy_facade_state_mappings_v1,
    build_vnpy_facade_terminal_mappings_v1,
    load_pinned_vnpy_algorithm_classes_v1,
)
from .facade_contracts import (
    VnpyFacadeActiveOrderV1,
    VnpyFacadeCharacterizationRequirementV1,
    VnpyFacadeCharacterizationStartContextV2,
    VnpyFacadeCharacterizationVectorV2,
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeConformanceFailureV1,
    VnpyFacadeContractError,
    VnpyFacadeContractV1,
    VnpyFacadeDeterministicInputsV1,
    VnpyFacadeExecutedVectorResultV1,
    VnpyFacadeSourceExecutionSetV1,
    VnpyFacadeSourceExecutorBindingV1,
    VnpyFacadeSourceManifestV1,
    VnpyFacadeSourceStateEnvelopeV1,
    VnpyFacadeStateFieldMappingV1,
    VnpyFacadeStateValueV1,
    VnpyFacadeTraceCallV1,
    VnpyFacadeTraceEffectV1,
    build_vnpy_facade_market_data_lineage_v1,
)
from .facade_projection import (
    AlgoStatus,
    Direction,
    Offset,
    OrderData,
    Status,
    TradeData,
    project_contract_data_v1,
    project_order_status_v1,
    project_tick_data_v1,
)

_SUPPORTED_ALGOS = (
    "BEST_LIMIT_MINIQMT",
    "ICEBERG",
    "SNIPER_MINIQMT",
    "STOP",
    "TWAP_LITE_MINIQMT",
)


def _source_failure(field_path: str, reason_code: str, **context: Any) -> VnpyFacadeConformanceFailureV1:
    return VnpyFacadeConformanceFailureV1.create(
        field_path=field_path,
        reason_code=reason_code,
        context=context,
    )


def _safe_exception_evidence_v1(exc: Exception) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[3]
    summary = bounded_exception_summary_v1(
        exc,
        redacted_values=(str(root), str(root).replace("\\", "\\\\"), root.as_posix()),
    )
    render_error_type = summary.pop("renderer_error_type")
    return {
        **summary,
        "message_render_error_type": render_error_type,
    }


def _canonical_module_sha256_v1() -> str:
    payload = Path(__file__).read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(payload).hexdigest()


def _source_executor_signature_payload_v1() -> dict[str, Any]:
    repository_root = Path(__file__).resolve().parents[3]
    return {
        "callable_ref": (
            "backend.execution_algos.vnpy_compat.facade_source_execution:execute_vnpy_facade_source_vectors_v1"
        ),
        **_facade_callable_signature_payload_v1(
            execute_vnpy_facade_source_vectors_v1,
            root=repository_root,
        ),
    }


def build_vnpy_facade_source_executor_binding_v1(
    *,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    vector_artifact_sha256: str,
    vector_artifact_file_sha256: str,
) -> VnpyFacadeSourceExecutorBindingV1:
    if not isinstance(source_manifest, VnpyFacadeSourceManifestV1):
        raise TypeError("source_manifest must be VnpyFacadeSourceManifestV1")
    if not isinstance(facade_contract, VnpyFacadeContractV1):
        raise TypeError("facade_contract must be VnpyFacadeContractV1")
    signature_payload = _source_executor_signature_payload_v1()
    return VnpyFacadeSourceExecutorBindingV1.create(
        executor_ref=signature_payload["callable_ref"],
        executor_signature_sha256=hash_hex_v1("miniqmt_vnpy_facade_source_executor_signature_v1", signature_payload),
        executor_source_sha256=_canonical_module_sha256_v1(),
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        facade_contract_sha256=facade_contract.facade_contract_sha256,
        implementation_binding_set_sha256=facade_contract.implementation_binding_set_sha256,
        isolated_module_binding_set_sha256=facade_contract.isolated_module_binding_set_sha256,
        dto_mapping_set_sha256=facade_contract.dto_mapping_set_sha256,
        state_mapping_set_sha256=facade_contract.state_mapping_set_sha256,
        terminal_mapping_set_sha256=facade_contract.terminal_mapping_set_sha256,
        vector_artifact_sha256=vector_artifact_sha256,
        vector_artifact_file_sha256=vector_artifact_file_sha256,
        supported_algo_codes=_SUPPORTED_ALGOS,
    )


def readback_vnpy_facade_source_executor_binding_v1(
    payload: Any,
    *,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    vector_artifact_sha256: str,
    vector_artifact_file_sha256: str,
) -> VnpyFacadeSourceExecutorBindingV1:
    supplied = VnpyFacadeSourceExecutorBindingV1.model_validate(payload, strict=True)
    expected = build_vnpy_facade_source_executor_binding_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        vector_artifact_sha256=vector_artifact_sha256,
        vector_artifact_file_sha256=vector_artifact_file_sha256,
    )
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_SOURCE_EXECUTOR_INVALID",
            "source executor binding conflicts with live callable/source authority",
            context={
                "expected_binding_sha256": expected.binding_sha256,
                "actual_binding_sha256": supplied.binding_sha256,
            },
        )
    return expected


def _state_value_v1(value: Any) -> Any:
    if isinstance(value, Enum):
        return {"enum_owner": type(value).__name__, "member": value.name, "pinned_value": value.value}
    if type(value) in (str, int, bool) or value is None:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("source algorithm state contains a non-finite float")
        normalized = format(Decimal(str(value)), "f")
        if "." in normalized:
            normalized = normalized.rstrip("0").rstrip(".")
        return "0" if normalized in {"", "-0"} else normalized
    raise TypeError(f"source algorithm state contains unsupported {type(value).__name__}")


def _decode_state_value_v1(item: VnpyFacadeStateValueV1) -> Any:
    value = thaw_json_v1(item.value)
    if isinstance(value, dict) and set(value) == {"enum_owner", "member", "pinned_value"}:
        if value["enum_owner"] == "Status":
            return Status[value["member"]]
        raise ValueError(f"unsupported source state enum owner {value['enum_owner']}")
    if "float" in item.value_type:
        return float(value)
    if "int" in item.value_type:
        if type(value) is not int:
            raise TypeError(f"source integer state {item.name} is not a strict integer")
        return value
    return value


def _state_entries_v1(
    algorithm: Any,
    mappings: tuple[VnpyFacadeStateFieldMappingV1, ...],
    *,
    role: str,
) -> tuple[VnpyFacadeStateValueV1, ...]:
    return tuple(
        sorted(
            (
                VnpyFacadeStateValueV1.create(
                    name=item.attribute_name,
                    value=_state_value_v1(getattr(algorithm, item.attribute_name)),
                    value_type=item.value_type,
                )
                for item in mappings
                if item.field_role.value == role
            ),
            key=lambda item: item.name,
        )
    )


def _replace_active_order_v1(item: VnpyFacadeActiveOrderV1, **updates: Any) -> VnpyFacadeActiveOrderV1:
    return VnpyFacadeActiveOrderV1.create(
        **{
            **item.canonical_payload_v1(exclude={"schema_version", "active_order_sha256"}),
            **updates,
        }
    )


def _active_orders_v1(
    *,
    algorithm: Any,
    before: VnpyFacadeSourceStateEnvelopeV1 | None,
    effect_collector: VnpyFacadeEffectCollectorV1,
    event: RuntimeEventEnvelopeV2 | None,
    active_mappings: Sequence[Any],
    market_data_lineage_or_none: dict[str, Any] | None,
) -> tuple[VnpyFacadeActiveOrderV1, ...]:
    by_local = {item.local_vt_orderid: item for item in (() if before is None else before.ordered_active_orders)}
    retained_ids = {mapping.local_vt_orderid for mapping in active_mappings}
    for mapping in active_mappings:
        existing = by_local.get(mapping.local_vt_orderid)
        if existing is None:
            raise ValueError("source active mapping has no predecessor active-order state")
        if (
            existing.command_id != mapping.command_id
            or existing.child_order_id != mapping.child_order_id
            or existing.symbol != mapping.symbol
            or existing.side != mapping.side.value
            or existing.price_decimal != mapping.requested_price_decimal
            or existing.requested_quantity != mapping.requested_quantity
        ):
            raise ValueError("source active mapping conflicts with predecessor active-order identity")
        if event is None or event.event_type not in {
            EventTypeV2.COMMAND_OUTCOME,
            EventTypeV2.ORDER,
            EventTypeV2.TRADE,
            EventTypeV2.RECONCILE,
        }:
            if existing.broker_order_id != mapping.broker_order_id:
                raise ValueError("source active mapping mutable facts drifted without a lifecycle event")
    for command in effect_collector.broker_commands:
        if command.command_type.value == "CANCEL_ORDER":
            existing = by_local.get(command.local_vt_orderid)
            if existing is None or existing.broker_order_id != command.owned_broker_order_id:
                raise ValueError("source cancel command conflicts with active-order authority")
            by_local[command.local_vt_orderid] = _replace_active_order_v1(
                existing,
                status="CANCEL_PENDING",
                pending_command_type="CANCEL_ORDER",
                pending_command_id=command.command_id,
            )
        else:
            if market_data_lineage_or_none is None:
                raise ValueError("source submit command has no exact native market-data lineage")
            by_local[command.local_vt_orderid] = VnpyFacadeActiveOrderV1.create(
                local_vt_orderid=command.local_vt_orderid,
                broker_order_id=None,
                command_id=command.command_id,
                child_order_id=execution_child_order_id_v1(
                    command_id=command.command_id,
                    local_vt_orderid=command.local_vt_orderid,
                ),
                symbol=command.symbol,
                side=command.side.value,
                price_decimal=command.price_decimal,
                requested_quantity=command.quantity,
                cumulative_quantity=0,
                remaining_quantity=command.quantity,
                status="COMMAND_PENDING",
                pending_command_type="SUBMIT_LIMIT",
                pending_command_id=command.command_id,
                last_order_event_id=None,
                last_trade_event_id=None,
                last_command_outcome_event_id=None,
                last_oms_reconcile_event_id=None,
                terminal_order_status=None,
                terminal_observed_cumulative_filled_quantity=None,
                market_data_lineage=market_data_lineage_or_none,
            )
            retained_ids.add(command.local_vt_orderid)
    if event is not None and event.event_type is EventTypeV2.ORDER:
        payload = strict_readback_kernel_event_payload_v1(event)
        if not isinstance(payload, KernelOrderEventPayloadV1):
            raise ValueError("source ORDER event did not read back as the strict ORDER payload")
        current = by_local.get(payload.local_vt_orderid)
        if current is None:
            raise ValueError("source ORDER callback has no active-order authority")
        if current.broker_order_id not in (None, payload.broker_order_id):
            raise ValueError("source ORDER broker identity conflicts with active-order authority")
        cumulative = payload.observed_cumulative_filled_quantity
        remaining = payload.observed_remaining_quantity
        if cumulative is None:
            cumulative = current.cumulative_quantity
            remaining = current.remaining_quantity
        if cumulative < current.cumulative_quantity or cumulative + remaining != current.requested_quantity:
            raise ValueError("source ORDER quantity closure conflicts with active-order authority")
        if payload.terminal:
            if payload.observed_cumulative_filled_quantity is None or cumulative > current.cumulative_quantity:
                by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                    current,
                    broker_order_id=payload.broker_order_id,
                    status=CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
                    pending_command_type=None,
                    pending_command_id=None,
                    last_order_event_id=event.event_id,
                    terminal_order_status=payload.normalized_order_status,
                    terminal_observed_cumulative_filled_quantity=(payload.observed_cumulative_filled_quantity),
                )
            else:
                by_local.pop(payload.local_vt_orderid)
                retained_ids.discard(payload.local_vt_orderid)
        else:
            by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                current,
                broker_order_id=payload.broker_order_id,
                cumulative_quantity=cumulative,
                remaining_quantity=remaining,
                status=(
                    CurrentThreeActiveOrderStatusV3.PARTIALLY_FILLED
                    if payload.normalized_order_status is NormalizedOrderStatusV1.PARTIALLY_FILLED or cumulative > 0
                    else CurrentThreeActiveOrderStatusV3.SUBMITTED
                ),
                pending_command_type=None,
                pending_command_id=None,
                last_order_event_id=event.event_id,
            )
    elif event is not None and event.event_type is EventTypeV2.TRADE:
        payload = strict_readback_kernel_event_payload_v1(event)
        if not isinstance(payload, KernelTradeEventPayloadV1):
            raise ValueError("source TRADE event did not read back as the strict TRADE payload")
        current = by_local.get(payload.local_vt_orderid)
        if current is None or current.broker_order_id not in (None, payload.broker_order_id):
            raise ValueError("source TRADE has no exact active-order/broker authority")
        cumulative = current.cumulative_quantity + payload.trade_quantity
        if cumulative > current.requested_quantity:
            raise ValueError("source TRADE quantity exceeds active-order request")
        terminal_observed = current.terminal_observed_cumulative_filled_quantity
        if (
            current.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING
            and terminal_observed is not None
            and cumulative >= terminal_observed
        ):
            by_local.pop(payload.local_vt_orderid)
            retained_ids.discard(payload.local_vt_orderid)
        else:
            by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                current,
                broker_order_id=payload.broker_order_id,
                cumulative_quantity=cumulative,
                remaining_quantity=current.requested_quantity - cumulative,
                status=(
                    CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING
                    if current.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING
                    else CurrentThreeActiveOrderStatusV3.PARTIALLY_FILLED
                ),
                pending_command_type=None,
                pending_command_id=None,
                last_trade_event_id=event.event_id,
            )
    elif event is not None and event.event_type is EventTypeV2.COMMAND_OUTCOME:
        payload = strict_readback_kernel_event_payload_v1(event)
        if not isinstance(payload, KernelCommandOutcomeEventPayloadV1):
            raise ValueError("source COMMAND_OUTCOME did not read back as its strict payload")
        current = by_local.get(payload.local_vt_orderid)
        if current is None:
            effect_collector.append_diagnostic(
                severity=DiagnosticSeverityV1.INFO,
                reason_code="MINIQMT_VNPY_FACADE_COMMAND_OUTCOME_CALLBACK_PRECEDED",
                message="a preceding callback already closed the source facade active order",
                context={
                    "event_id": event.event_id,
                    "local_vt_orderid": payload.local_vt_orderid,
                    "command_id": payload.command_id,
                },
            )
            retained_ids.discard(payload.local_vt_orderid)
            return tuple(by_local[key] for key in sorted(retained_ids & set(by_local)))
        if current.pending_command_id != payload.command_id:
            raise ValueError("source COMMAND_OUTCOME does not own an active pending command")
        if payload.outcome is KernelCommandOutcomeV1.CONFLICT:
            raise ValueError("source COMMAND_OUTCOME reported a durable identity conflict")
        if payload.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            if payload.outcome is KernelCommandOutcomeV1.ACCEPTED:
                by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                    current,
                    broker_order_id=payload.broker_order_id,
                    status=CurrentThreeActiveOrderStatusV3.SUBMITTED,
                    pending_command_type=None,
                    pending_command_id=None,
                    last_command_outcome_event_id=event.event_id,
                )
            elif payload.outcome in {
                KernelCommandOutcomeV1.REJECTED,
                KernelCommandOutcomeV1.PRE_CALL_TERMINAL,
            }:
                by_local.pop(payload.local_vt_orderid)
                retained_ids.discard(payload.local_vt_orderid)
            else:
                by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                    current,
                    status=CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
                    last_command_outcome_event_id=event.event_id,
                )
        else:
            if payload.broker_order_id != current.broker_order_id:
                raise ValueError("source CANCEL outcome broker identity conflicts with active order")
            if payload.outcome is KernelCommandOutcomeV1.ACCEPTED:
                by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                    current,
                    status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                    last_command_outcome_event_id=event.event_id,
                )
            elif payload.outcome in {
                KernelCommandOutcomeV1.REJECTED,
                KernelCommandOutcomeV1.PRE_CALL_TERMINAL,
            }:
                by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                    current,
                    status=(
                        CurrentThreeActiveOrderStatusV3.PARTIALLY_FILLED
                        if current.cumulative_quantity > 0
                        else CurrentThreeActiveOrderStatusV3.SUBMITTED
                    ),
                    pending_command_type=None,
                    pending_command_id=None,
                    last_command_outcome_event_id=event.event_id,
                )
            else:
                by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                    current,
                    status=CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
                    last_command_outcome_event_id=event.event_id,
                )
    elif event is not None and event.event_type is EventTypeV2.RECONCILE:
        payload = strict_readback_kernel_event_payload_v1(event)
        if not isinstance(payload, KernelOrderReconcileEventPayloadV1):
            raise ValueError("source RECONCILE did not read back as its strict payload")
        current = by_local.get(payload.local_vt_orderid)
        if current is None:
            effect_collector.append_diagnostic(
                severity=DiagnosticSeverityV1.INFO,
                reason_code="MINIQMT_VNPY_FACADE_RECONCILE_CALLBACK_PRECEDED",
                message="reconciliation observed an already closed source facade active order",
                context={
                    "event_id": event.event_id,
                    "local_vt_orderid": payload.local_vt_orderid,
                },
            )
            retained_ids.discard(payload.local_vt_orderid)
            return tuple(by_local[key] for key in sorted(retained_ids & set(by_local)))
        if current.broker_order_id != payload.broker_order_id:
            raise ValueError("source RECONCILE has no exact active-order/broker authority")
        if payload.authoritative_cumulative_filled_quantity != current.cumulative_quantity:
            raise ValueError("source RECONCILE cumulative differs from exact TRADE-applied state")
        if payload.authoritative_remaining_quantity != current.remaining_quantity:
            raise ValueError("source RECONCILE remaining quantity conflicts with active-order state")
        if payload.terminal and current.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING:
            by_local.pop(payload.local_vt_orderid)
            retained_ids.discard(payload.local_vt_orderid)
        else:
            by_local[payload.local_vt_orderid] = _replace_active_order_v1(
                current,
                last_oms_reconcile_event_id=event.event_id,
            )
    source_orders = getattr(algorithm, "active_orders", None)
    if type(source_orders) is not dict or any(
        type(local_id) is not str or not isinstance(order, OrderData) or order.vt_orderid != local_id
        for local_id, order in source_orders.items()
    ):
        raise TypeError("source algorithm active_orders carrier is invalid")
    if (
        event is None or event.event_type in {EventTypeV2.TICK, EventTypeV2.TIMER, EventTypeV2.ORDER, EventTypeV2.TRADE}
    ) and not set(source_orders).issubset(by_local):
        raise ValueError("source algorithm active order is absent from command/mapping authority")
    if event is None or event.event_type in {
        EventTypeV2.TICK,
        EventTypeV2.TIMER,
        EventTypeV2.ORDER,
        EventTypeV2.TRADE,
    }:
        retained_ids |= set(source_orders)
    missing = retained_ids - set(by_local)
    if missing:
        raise ValueError("source active mapping has no exact active-order state")
    return tuple(by_local[key] for key in sorted(retained_ids))


def _extract_source_state_v1(
    *,
    algorithm: Any,
    start: VnpyFacadeCharacterizationStartContextV2,
    source_identity_sha256: str,
    mappings: tuple[VnpyFacadeStateFieldMappingV1, ...],
    before: VnpyFacadeSourceStateEnvelopeV1 | None,
    effect_collector: VnpyFacadeEffectCollectorV1,
    event: RuntimeEventEnvelopeV2 | None,
    active_mappings: Sequence[Any],
    read_only_services: Any,
) -> VnpyFacadeSourceStateEnvelopeV1:
    expected_attributes = {item.attribute_name for item in mappings} | {"algo_engine"}
    actual_attributes = set(vars(algorithm))
    if actual_attributes != expected_attributes:
        raise ValueError(
            f"source algorithm attributes drifted missing={sorted(expected_attributes - actual_attributes)} "
            f"extra={sorted(actual_attributes - expected_attributes)}"
        )
    return VnpyFacadeSourceStateEnvelopeV1.create(
        runtime_id=start.runtime_id,
        algo_instance_id=start.algo_instance_id,
        algo_code=start.manifest_view.algo_code,
        source_identity_sha256=source_identity_sha256,
        manifest_view_sha256=start.manifest_view.view_sha256,
        algo_name=algorithm.algo_name,
        symbol=start.symbol,
        direction_member=algorithm.direction.name,
        offset_member=algorithm.offset.name,
        limit_price_decimal=str(algorithm.price),
        target_volume_decimal=str(algorithm.volume),
        status_member=algorithm.status.name,
        traded_volume_decimal=str(algorithm.traded),
        traded_price_decimal=str(algorithm.traded_price),
        contract_projection=thaw_json_v1(start.contract_projection),
        ordered_active_orders=_active_orders_v1(
            algorithm=algorithm,
            before=before,
            effect_collector=effect_collector,
            event=event,
            active_mappings=active_mappings,
            market_data_lineage_or_none=(
                None
                if read_only_services is None
                or not any(
                    item.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT for item in effect_collector.broker_commands
                )
                else build_vnpy_facade_market_data_lineage_v1(
                    services=read_only_services,
                    deterministic_context=start.deterministic_context,
                )
            ),
        ),
        ordered_parameters=_state_entries_v1(algorithm, mappings, role="PARAMETER"),
        ordered_variables=_state_entries_v1(algorithm, mappings, role="VARIABLE"),
        state_mapping_set_sha256=state_mapping_set_sha256_v1(mappings),
    )


def _restore_source_algorithm_v1(
    *,
    algorithm_class: type[Any],
    before: VnpyFacadeSourceStateEnvelopeV1,
    facade: VnpyAlgoEngineFacadeV1,
    mappings: tuple[VnpyFacadeStateFieldMappingV1, ...],
) -> Any:
    algorithm = algorithm_class.__new__(algorithm_class)
    values = {
        "algo_name": before.algo_name,
        "vt_symbol": before.symbol.replace(".SH", ".SSE").replace(".SZ", ".SZSE").replace(".BJ", ".BSE"),
        "direction": Direction[before.direction_member],
        "offset": Offset[before.offset_member],
        "price": float(before.limit_price_decimal),
        "volume": float(before.target_volume_decimal),
        "status": AlgoStatus[before.status_member],
        "traded": float(before.traded_volume_decimal),
        "traded_price": float(before.traded_price_decimal),
        "active_orders": {
            item.local_vt_orderid: OrderData(
                vt_orderid=item.local_vt_orderid,
                status=(Status.PARTTRADED if item.cumulative_quantity else Status.NOTTRADED),
                traded=float(item.cumulative_quantity),
                price=float(item.price_decimal),
            )
            for item in before.ordered_active_orders
            if item.status
            not in {
                CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
                CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
                CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
            }
        },
        **{item.name: _decode_state_value_v1(item) for item in before.ordered_parameters},
        **{item.name: _decode_state_value_v1(item) for item in before.ordered_variables},
    }
    object.__setattr__(algorithm, "algo_engine", facade)
    for mapping in mappings:
        if mapping.attribute_name == "algo_engine":
            continue
        if mapping.attribute_name not in values:
            raise ValueError(f"source state is missing mapped field {mapping.attribute_name}")
        object.__setattr__(algorithm, mapping.attribute_name, values[mapping.attribute_name])
    return algorithm


def _callback_v1(
    *,
    algorithm: Any,
    event: RuntimeEventEnvelopeV2,
    facade: VnpyAlgoEngineFacadeV1,
    before: VnpyFacadeSourceStateEnvelopeV1,
    trace: VnpyFacadeTraceCollectorV2,
) -> None:
    payload = thaw_json_v1(event.payload)
    if event.event_type is EventTypeV2.TICK:
        tick = facade.get_tick(algorithm)
        if tick is not None:
            trace.invoke_v1(
                method_name="update_tick",
                normalized_arguments={"tick": tick},
                operation=lambda: algorithm.update_tick(tick),
            )
    elif event.event_type is EventTypeV2.TIMER:
        trace.invoke_v1(
            method_name="update_timer",
            normalized_arguments={},
            operation=algorithm.update_timer,
        )
    elif event.event_type is EventTypeV2.ORDER:
        prior = next(
            (item for item in before.ordered_active_orders if item.local_vt_orderid == payload["local_vt_orderid"]),
            None,
        )
        if prior is None:
            raise ValueError("ORDER callback has no source active-order authority")
        cumulative = payload["observed_cumulative_filled_quantity"]
        if cumulative is None:
            cumulative = prior.cumulative_quantity
        order = OrderData(
            vt_orderid=payload["local_vt_orderid"],
            status=project_order_status_v1(NormalizedOrderStatusV1(payload["normalized_order_status"])),
            traded=float(cumulative),
            price=float(prior.price_decimal),
        )
        trace.invoke_v1(
            method_name="update_order",
            normalized_arguments={"order": order},
            operation=lambda: algorithm.update_order(order),
        )
    elif event.event_type is EventTypeV2.TRADE:
        trade = TradeData(
            vt_orderid=payload["local_vt_orderid"],
            vt_tradeid=payload["trade_id"],
            price=float(payload["trade_price_decimal"]),
            volume=float(payload["trade_quantity"]),
            datetime=datetime.fromisoformat(event.event_time_utc.replace("Z", "+00:00")),
        )
        trace.invoke_v1(
            method_name="update_trade",
            normalized_arguments={"trade": trade},
            operation=lambda: algorithm.update_trade(trade),
        )
    elif event.event_type in {
        EventTypeV2.COMMAND_OUTCOME,
        EventTypeV2.SESSION,
        EventTypeV2.RECONCILE,
        EventTypeV2.EOD,
    }:
        # These are K2/K3 lifecycle facts, not pinned AlgoTemplate callbacks.
        # Their explicit source characterization is a zero-call/zero-effect step.
        return
    else:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "source vector event has no pinned callback mapping",
            context={"event_type": event.event_type.value, "event_id": event.event_id},
        )


def _terminal_outcome_v1(
    *,
    state: VnpyFacadeSourceStateEnvelopeV1,
    event_type: EventTypeV2,
    terminal_mappings: Sequence[Any],
) -> TerminalOutcomeV1 | None:
    if state.ordered_active_orders:
        return None
    relation = "FULL" if float(state.traded_volume_decimal) >= float(state.target_volume_decimal) else "RESIDUAL"
    for mapping in terminal_mappings:
        if (
            mapping.algo_code == state.algo_code
            and mapping.algo_status_member == state.status_member
            and mapping.trigger_event_type in {"ANY", event_type.value}
            and mapping.required_active_child_closure == "CLEAN"
            and mapping.traded_relation in {"ANY", relation}
        ):
            return (
                None
                if mapping.terminal_outcome_or_none is None
                else TerminalOutcomeV1(mapping.terminal_outcome_or_none)
            )
    return None


def _step_start_context_v1(
    *,
    vector: VnpyFacadeCharacterizationVectorV2,
    scenario_start: VnpyFacadeCharacterizationStartContextV2,
) -> VnpyFacadeCharacterizationStartContextV2:
    return VnpyFacadeCharacterizationStartContextV2.create(
        vector_id=vector.vector_id,
        runtime_id=scenario_start.runtime_id,
        algo_instance_id=scenario_start.algo_instance_id,
        parent_intent_id=scenario_start.parent_intent_id,
        strategy_slot_id=scenario_start.strategy_slot_id,
        symbol=scenario_start.symbol,
        side=scenario_start.side,
        limit_price_decimal=scenario_start.limit_price_decimal,
        parent_quantity=scenario_start.parent_quantity,
        contract_projection=thaw_json_v1(scenario_start.contract_projection),
        deterministic_context=vector.deterministic_context,
        manifest_view=scenario_start.manifest_view,
        canonical_config=thaw_json_v1(scenario_start.canonical_config),
    )


def _execute_vector_body_v1(
    *,
    vector: VnpyFacadeCharacterizationVectorV2,
    scenario_start: VnpyFacadeCharacterizationStartContextV2,
    source_identity_sha256: str,
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    state_mappings: tuple[VnpyFacadeStateFieldMappingV1, ...],
    terminal_mappings: Sequence[Any],
    effect_collector: VnpyFacadeEffectCollectorV1,
    trace: VnpyFacadeTraceCollectorV2,
    uniform: VnpyFacadeDeterministicUniformV1,
    source_root: Path,
) -> VnpyFacadeExecutedVectorResultV1:
    def traced_uniform(a: float, b: float) -> float:
        return trace.invoke_v1(
            method_name="uniform",
            normalized_arguments={"lower": a, "upper": b},
            operation=lambda: uniform(a, b),
        )

    classes = load_pinned_vnpy_algorithm_classes_v1(
        source_root=source_root,
        deterministic_uniform=traced_uniform,
    )
    algorithm_class = classes[vector.algo_code]
    step_start = _step_start_context_v1(vector=vector, scenario_start=scenario_start)
    contract_payload = thaw_json_v1(step_start.contract_projection)
    contract = project_contract_data_v1(
        symbol=step_start.symbol,
        gateway_name=contract_payload["gateway_name"],
        min_volume=contract_payload["min_volume"],
        pricetick_decimal=contract_payload["pricetick_decimal"],
    )
    tick = None
    if vector.read_only_services_or_null is not None:
        market = vector.read_only_services_or_null.market_data_projection
        if market is not None:
            tick = project_tick_data_v1(symbol=step_start.symbol, payload=thaw_json_v1(market))
    facade = VnpyAlgoEngineFacadeV1._create_characterization_v2(
        manifest_view=step_start.manifest_view,
        characterization_context=step_start,
        trace_collector=trace,
        contract=contract,
        tick=tick,
        active_mappings=vector.ordered_active_mappings,
    )
    before = vector.before_state_or_null
    if vector.invocation_phase == "INITIALIZE":
        constructed: list[Any] = []

        def construct() -> dict[str, str]:
            algorithm = algorithm_class(
                facade,
                step_start.algo_instance_id,
                step_start.symbol.replace(".SH", ".SSE").replace(".SZ", ".SZSE").replace(".BJ", ".BSE"),
                Direction.LONG if step_start.side is SideV1.BUY else Direction.SHORT,
                Offset.NONE,
                float(step_start.limit_price_decimal),
                float(step_start.parent_quantity),
                thaw_json_v1(step_start.canonical_config),
            )
            constructed.append(algorithm)
            return {
                "class_ref": f"{algorithm_class.__module__}:{algorithm_class.__qualname__}",
                "algo_name": algorithm.algo_name,
            }

        trace.invoke_v1(
            method_name="__init__",
            normalized_arguments={
                "algo_name": step_start.algo_instance_id,
                "vt_symbol": step_start.symbol,
                "direction": Direction.LONG if step_start.side is SideV1.BUY else Direction.SHORT,
                "offset": Offset.NONE,
                "price": float(step_start.limit_price_decimal),
                "volume": float(step_start.parent_quantity),
                "setting": thaw_json_v1(step_start.canonical_config),
            },
            operation=construct,
        )
        if len(constructed) != 1:
            raise ValueError("source constructor did not create exactly one algorithm object")
        algorithm = constructed[0]
        trace.invoke_v1(method_name="start", normalized_arguments={}, operation=algorithm.start)
        event = None
        event_type = EventTypeV2.ALGO_START
    else:
        if before is None or vector.runtime_event_or_null is None:
            raise ValueError("transition vector lacks source predecessor/event")
        algorithm = _restore_source_algorithm_v1(
            algorithm_class=algorithm_class,
            before=before,
            facade=facade,
            mappings=state_mappings,
        )
        event = vector.runtime_event_or_null
        _callback_v1(algorithm=algorithm, event=event, facade=facade, before=before, trace=trace)
        event_type = event.event_type
    if getattr(algorithm, "algo_engine", None) is not facade:
        raise ValueError("source algorithm replaced its transition-local facade")
    after = _extract_source_state_v1(
        algorithm=algorithm,
        start=step_start,
        source_identity_sha256=source_identity_sha256,
        mappings=state_mappings,
        before=before,
        effect_collector=effect_collector,
        event=event,
        active_mappings=vector.ordered_active_mappings,
        read_only_services=vector.read_only_services_or_null,
    )
    uniform.freeze_trace_v1()
    calls, effects = trace.freeze_v1()
    return VnpyFacadeExecutedVectorResultV1.create(
        vector_id=vector.vector_id,
        vector_sha256=vector.vector_sha256,
        scenario_id=vector.scenario_id,
        step_ordinal=vector.step_ordinal,
        source_executor_binding_sha256=source_executor_binding.binding_sha256,
        source_identity_sha256=source_identity_sha256,
        invocation_status="COMPLETED",
        actual_ordered_facade_calls=calls,
        actual_ordered_effects=effects,
        actual_after_state_or_null=after,
        actual_terminal_outcome=_terminal_outcome_v1(
            state=after,
            event_type=event_type,
            terminal_mappings=terminal_mappings,
        ),
        consumed_deterministic_inputs=vector.explicit_deterministic_inputs,
        ordered_execution_failures=(),
    )


def _execute_vector_v1(
    *,
    vector: VnpyFacadeCharacterizationVectorV2,
    scenario_start: VnpyFacadeCharacterizationStartContextV2,
    source_identity_sha256: str,
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    state_mappings: tuple[VnpyFacadeStateFieldMappingV1, ...],
    terminal_mappings: Sequence[Any],
    source_root: Path,
) -> VnpyFacadeExecutedVectorResultV1:
    effect_collector = VnpyFacadeEffectCollectorV1.create(
        vector.deterministic_context,
        scenario_start.parent_intent_id,
        algo_transition_id_v1(
            delivery_id=vector.deterministic_context.delivery_id,
            event_id=vector.deterministic_context.event_id,
            runtime_id=vector.deterministic_context.runtime_id,
            algo_instance_id=vector.deterministic_context.algo_instance_id,
            transition_sequence=vector.deterministic_context.transition_sequence,
        ),
    )
    trace = VnpyFacadeTraceCollectorV2(vector_id=vector.vector_id, effect_collector=effect_collector)
    uniform = VnpyFacadeDeterministicUniformV1(vector.explicit_deterministic_inputs)
    try:
        return _execute_vector_body_v1(
            vector=vector,
            scenario_start=scenario_start,
            source_identity_sha256=source_identity_sha256,
            source_executor_binding=source_executor_binding,
            state_mappings=state_mappings,
            terminal_mappings=terminal_mappings,
            effect_collector=effect_collector,
            trace=trace,
            uniform=uniform,
            source_root=source_root,
        )
    except Exception as exc:
        try:
            calls, effects = trace.snapshot_v1()
        except Exception as snapshot_error:
            return _failed_result_v1(
                vector=vector,
                source_executor_binding=source_executor_binding,
                source_identity_sha256=source_identity_sha256,
                exc=exc,
                trace_snapshot_error=snapshot_error,
                consumed_deterministic_inputs=uniform.consumed_inputs_v1(),
            )
        return _failed_result_v1(
            vector=vector,
            source_executor_binding=source_executor_binding,
            source_identity_sha256=source_identity_sha256,
            exc=exc,
            actual_ordered_facade_calls=calls,
            actual_ordered_effects=effects,
            consumed_deterministic_inputs=uniform.consumed_inputs_v1(),
        )


def _failed_result_v1(
    *,
    vector: VnpyFacadeCharacterizationVectorV2,
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    source_identity_sha256: str,
    exc: Exception,
    actual_ordered_facade_calls: tuple[VnpyFacadeTraceCallV1, ...] = (),
    actual_ordered_effects: tuple[VnpyFacadeTraceEffectV1, ...] = (),
    consumed_deterministic_inputs: VnpyFacadeDeterministicInputsV1 | None = None,
    trace_snapshot_error: Exception | None = None,
) -> VnpyFacadeExecutedVectorResultV1:
    evidence = _safe_exception_evidence_v1(exc)
    if trace_snapshot_error is not None:
        evidence["trace_snapshot_failure"] = _safe_exception_evidence_v1(trace_snapshot_error)
    failure = _source_failure(
        f"vectors.{vector.vector_id}",
        stable_exception_reason_code_v1(
            exc,
            default="MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED",
        ),
        vector_id=vector.vector_id,
        scenario_id=vector.scenario_id,
        step_ordinal=vector.step_ordinal,
        **evidence,
    )
    return VnpyFacadeExecutedVectorResultV1.create(
        vector_id=vector.vector_id,
        vector_sha256=vector.vector_sha256,
        scenario_id=vector.scenario_id,
        step_ordinal=vector.step_ordinal,
        source_executor_binding_sha256=source_executor_binding.binding_sha256,
        source_identity_sha256=source_identity_sha256,
        invocation_status="FAILED",
        actual_ordered_facade_calls=actual_ordered_facade_calls,
        actual_ordered_effects=actual_ordered_effects,
        actual_after_state_or_null=None,
        actual_terminal_outcome=None,
        consumed_deterministic_inputs=(
            VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=())
            if consumed_deterministic_inputs is None
            else consumed_deterministic_inputs
        ),
        ordered_execution_failures=(failure,),
    )


def execute_vnpy_facade_source_vectors_v1(
    *,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
    ordered_vectors: tuple[VnpyFacadeCharacterizationVectorV2, ...],
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeSourceExecutionSetV1, ...]:
    """Execute every required V2 vector and retain all deterministic failures."""

    if not isinstance(source_executor_binding, VnpyFacadeSourceExecutorBindingV1):
        raise TypeError("source_executor_binding must be VnpyFacadeSourceExecutorBindingV1")
    binding = source_executor_binding
    if (
        binding.facade_source_manifest_sha256 != source_manifest.manifest_sha256
        or binding.facade_contract_sha256 != facade_contract.facade_contract_sha256
        or binding.implementation_binding_set_sha256 != facade_contract.implementation_binding_set_sha256
        or binding.isolated_module_binding_set_sha256 != facade_contract.isolated_module_binding_set_sha256
        or binding.dto_mapping_set_sha256 != facade_contract.dto_mapping_set_sha256
        or binding.state_mapping_set_sha256 != facade_contract.state_mapping_set_sha256
        or binding.terminal_mapping_set_sha256 != facade_contract.terminal_mapping_set_sha256
    ):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_SOURCE_EXECUTOR_INVALID",
            "source executor binding conflicts with the supplied source/facade authority",
            context={"binding_sha256": binding.binding_sha256},
        )
    requirement_by_algo = {item.algo_code: item for item in requirements}
    requested_algos = tuple(sorted(requirement_by_algo))
    if (
        not requested_algos
        or len(requirement_by_algo) != len(requirements)
        or not set(requested_algos).issubset(_SUPPORTED_ALGOS)
    ):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_SOURCE_EXECUTOR_INVALID",
            "source executor requires one unique supported requirement per requested algorithm",
            context={"actual_algo_codes": sorted(requirement_by_algo)},
        )
    sources = {
        item.algo_code_or_helper_name: item
        for item in source_manifest.ordered_sources
        if item.source_role.value == "ALGORITHM"
    }
    state_all = build_vnpy_facade_state_mappings_v1(source_root=source_root)
    terminal_all = build_vnpy_facade_terminal_mappings_v1(source_root=source_root)
    result_sets: list[VnpyFacadeSourceExecutionSetV1] = []
    for algo_code in requested_algos:
        requirement = requirement_by_algo[algo_code]
        vectors = tuple(item for item in ordered_vectors if item.algo_code == algo_code)
        vector_set_sha = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_vector_set_v1",
            [item.canonical_payload_v1() for item in vectors],
        )
        results: list[VnpyFacadeExecutedVectorResultV1] = []
        failures: list[VnpyFacadeConformanceFailureV1] = []
        actual_by_id: dict[str, VnpyFacadeSourceStateEnvelopeV1] = {}
        start_by_scenario: dict[str, VnpyFacadeCharacterizationStartContextV2] = {}
        for vector in vectors:
            if vector.invocation_phase == "INITIALIZE" and vector.start_context_or_null is not None:
                start_by_scenario[vector.scenario_id] = vector.start_context_or_null
            scenario_start = start_by_scenario.get(vector.scenario_id)
            if scenario_start is None:
                exc = ValueError("scenario transition has no exact initialization context")
                result = _failed_result_v1(
                    vector=vector,
                    source_executor_binding=binding,
                    source_identity_sha256=sources[algo_code].source_identity_sha256,
                    exc=exc,
                )
            else:
                predecessor = None
                if vector.predecessor_vector_id_or_INIT != "INIT":
                    predecessor = actual_by_id.get(vector.predecessor_vector_id_or_INIT)
                    if predecessor is None or predecessor != vector.before_state_or_null:
                        exc = ValueError("scenario predecessor actual state differs from vector before state")
                        result = _failed_result_v1(
                            vector=vector,
                            source_executor_binding=binding,
                            source_identity_sha256=sources[algo_code].source_identity_sha256,
                            exc=exc,
                        )
                        results.append(result)
                        failures.extend(result.ordered_execution_failures)
                        continue
                try:
                    validate_json_schema_instance_v1(
                        schema=requirement.config_schema,
                        instance=vector.canonical_config,
                        contract_name=f"K4 facade {algo_code} characterization config",
                    )
                    result = _execute_vector_v1(
                        vector=vector,
                        scenario_start=scenario_start,
                        source_identity_sha256=sources[algo_code].source_identity_sha256,
                        source_executor_binding=binding,
                        state_mappings=tuple(item for item in state_all if item.algo_code == algo_code),
                        terminal_mappings=tuple(item for item in terminal_all if item.algo_code == algo_code),
                        source_root=source_root,
                    )
                except Exception as exc:
                    result = _failed_result_v1(
                        vector=vector,
                        source_executor_binding=binding,
                        source_identity_sha256=sources[algo_code].source_identity_sha256,
                        exc=exc,
                    )
            results.append(result)
            if result.invocation_status == "COMPLETED" and result.actual_after_state_or_null is not None:
                actual_by_id[vector.vector_id] = result.actual_after_state_or_null
                mismatches = {
                    "calls": result.actual_ordered_facade_calls != vector.expected_ordered_facade_calls,
                    "effects": result.actual_ordered_effects != vector.expected_ordered_effects,
                    "after_state": result.actual_after_state_or_null != vector.expected_after_state,
                    "terminal": result.actual_terminal_outcome != vector.expected_terminal_outcome,
                }
                if any(mismatches.values()):
                    failures.append(
                        _source_failure(
                            f"vectors.{vector.vector_id}.expected_trace",
                            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
                            vector_id=vector.vector_id,
                            mismatched_fields=sorted(name for name, drift in mismatches.items() if drift),
                        )
                    )
            else:
                failures.extend(result.ordered_execution_failures)
        if not vectors:
            failures.append(
                _source_failure(
                    f"algorithms.{algo_code}.vectors",
                    "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
                    algo_code=algo_code,
                    reason="required vector set is empty",
                )
            )
        result_sets.append(
            VnpyFacadeSourceExecutionSetV1.create(
                algo_code=algo_code,
                characterization_requirement_sha256=requirement.requirement_sha256,
                source_executor_binding_sha256=binding.binding_sha256,
                facade_source_manifest_sha256=source_manifest.manifest_sha256,
                facade_contract_sha256=facade_contract.facade_contract_sha256,
                vector_set_sha256=vector_set_sha,
                ordered_results=tuple(results),
                ordered_failures=tuple(failures),
                status=(VnpyFacadeCompatibilityStatusV1.FAILED if failures else VnpyFacadeCompatibilityStatusV1.PASSED),
            )
        )
    return tuple(result_sets)


__all__ = [
    "build_vnpy_facade_source_executor_binding_v1",
    "execute_vnpy_facade_source_vectors_v1",
    "readback_vnpy_facade_source_executor_binding_v1",
]
