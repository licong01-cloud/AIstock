"""Pure K3-B parity input, trace and immutable receipt authority."""

from __future__ import annotations

from enum import Enum
from typing import Any, Sequence

from pydantic import BaseModel

from .kernel_current_three_contracts import (
    MAX_K3_FAILURES,
    CurrentThreeParityDifferenceV1,
    CurrentThreeParityBusinessEffectV1,
    CurrentThreeParityEventRefV1,
    CurrentThreeParityInputV1,
    CurrentThreeParityReceiptV1,
    CurrentThreeParityStatusV1,
    CurrentThreeParityTraceStepV1,
    CurrentThreeParityTraceV1,
    CurrentThreeParityTimerEffectV1,
    CurrentThreeTransportDuplicateObservationV1,
    CurrentThreeShadowCommandAssociationV1,
    CurrentThreeContractError,
)
from .kernel_current_three_shadow_source import CurrentThreeShadowRepositoryReadV1
from .plugin_canonical import canonical_utc_datetime_v1, hash_hex_v1
from .plugin_contracts import SideV1
from .plugin_contracts import (
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    ExecutionCommandChildMappingV1,
)
from decimal import Decimal


def _canonical(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return [_canonical(item) for item in value]
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {key: _canonical(member) for key, member in value.items()}
    return value


def _hash(domain: str, payload: dict[str, Any]) -> str:
    return hash_hex_v1(domain, _canonical(payload))


def build_parity_event_ref_v1(
    *,
    step_ordinal: int,
    event_id: str,
    event_type: str,
    event_source: str,
    event_payload_sha256: str,
    logical_time_utc: Any,
    market_data_projection_id: str | None = None,
    market_data_projection_sha256: str | None = None,
    account_projection_id: str | None = None,
    account_projection_sha256: str | None = None,
    contract_projection_id: str | None = None,
    contract_projection_sha256: str | None = None,
) -> CurrentThreeParityEventRefV1:
    payload = {
        "schema_version": "miniqmt_current_three_parity_event_ref_v1",
        "step_ordinal": step_ordinal,
        "event_id": event_id,
        "event_type": event_type,
        "event_source": event_source,
        "event_payload_sha256": event_payload_sha256,
        "logical_time_utc": canonical_utc_datetime_v1(logical_time_utc, field_name="logical_time_utc"),
        "market_data_projection_id": market_data_projection_id,
        "market_data_projection_sha256": market_data_projection_sha256,
        "account_projection_id": account_projection_id,
        "account_projection_sha256": account_projection_sha256,
        "contract_projection_id": contract_projection_id,
        "contract_projection_sha256": contract_projection_sha256,
    }
    return CurrentThreeParityEventRefV1(
        **payload,
        event_ref_sha256=_hash("miniqmt_current_three_parity_event_ref_v1", payload),
    )


def build_current_three_parity_input_v1(
    *,
    algo_code: str,
    runtime_id: str,
    parent_intent_id: str,
    strategy_slot_id: str,
    symbol: str,
    side: SideV1,
    target_quantity: int,
    limit_price_decimal: str,
    pricetick_decimal: str,
    min_volume: int,
    volume_increment: int,
    plugin_config: dict[str, Any],
    legacy_policy_projection_receipt_sha256: str,
    ordered_event_refs: Sequence[CurrentThreeParityEventRefV1],
) -> CurrentThreeParityInputV1:
    refs = tuple(ordered_event_refs)
    config_hash = hash_hex_v1("miniqmt_plugin_config_v2", plugin_config)
    event_set_hash = hash_hex_v1(
        "miniqmt_current_three_parity_event_set_v1",
        [{"event_id": item.event_id, "event_ref_sha256": item.event_ref_sha256} for item in refs],
    )
    payload = {
        "schema_version": "miniqmt_current_three_parity_input_v1",
        "algo_code": algo_code,
        "runtime_id": runtime_id,
        "parent_intent_id": parent_intent_id,
        "strategy_slot_id": strategy_slot_id,
        "symbol": symbol,
        "side": side,
        "target_quantity": target_quantity,
        "limit_price_decimal": limit_price_decimal,
        "pricetick_decimal": pricetick_decimal,
        "min_volume": min_volume,
        "volume_increment": volume_increment,
        "plugin_config": plugin_config,
        "plugin_config_sha256": config_hash,
        "legacy_policy_projection_receipt_sha256": legacy_policy_projection_receipt_sha256,
        "execution_coordination_scope": "ALGO_LOCAL_ONLY",
        "ordered_event_refs": refs,
        "event_set_sha256": event_set_hash,
    }
    return CurrentThreeParityInputV1(
        **payload,
        input_sha256=_hash("miniqmt_current_three_parity_input_v1", payload),
    )


def build_current_three_parity_trace_step_v1(
    *,
    step_ordinal: int,
    event_type: str,
    event_payload_sha256: str,
    logical_time_utc: Any,
    state_status: str,
    traded_quantity: int,
    remaining_quantity: int,
    algo_specific_state_projection: dict[str, Any],
    ordered_business_effects: Sequence[dict[str, Any]],
    ordered_transport_duplicate_observations: Sequence[dict[str, Any]],
    ordered_timer_effects: Sequence[dict[str, Any]],
    ordered_diagnostic_reason_codes: Sequence[str],
    terminal_outcome: dict[str, Any] | str | None,
) -> CurrentThreeParityTraceStepV1:
    business_effects = tuple(
        CurrentThreeParityBusinessEffectV1(
            schema_version="miniqmt_current_three_parity_business_effect_v1",
            **{**item, "side": SideV1(item["side"])},
        )
        for item in ordered_business_effects
    )
    transport_observations = tuple(
        CurrentThreeTransportDuplicateObservationV1(
            schema_version="miniqmt_current_three_transport_duplicate_observation_v1", **item
        )
        for item in ordered_transport_duplicate_observations
    )
    timer_effects = tuple(
        CurrentThreeParityTimerEffectV1(schema_version="miniqmt_current_three_parity_timer_effect_v1", **item)
        for item in ordered_timer_effects
    )
    payload = {
        "schema_version": "miniqmt_current_three_parity_trace_step_v1",
        "step_ordinal": step_ordinal,
        "event_type": event_type,
        "event_payload_sha256": event_payload_sha256,
        "logical_time_utc": canonical_utc_datetime_v1(logical_time_utc, field_name="logical_time_utc"),
        "state_status": state_status,
        "traded_quantity": traded_quantity,
        "remaining_quantity": remaining_quantity,
        "algo_specific_state_projection": algo_specific_state_projection,
        "ordered_business_effects": business_effects,
        "ordered_transport_duplicate_observations": transport_observations,
        "ordered_timer_effects": timer_effects,
        "ordered_diagnostic_reason_codes": tuple(ordered_diagnostic_reason_codes),
        "terminal_outcome": terminal_outcome,
    }
    return CurrentThreeParityTraceStepV1(
        **payload,
        step_sha256=_hash("miniqmt_current_three_parity_trace_step_v1", payload),
    )


def build_current_three_parity_trace_v1(
    *, algo_code: str, side: SideV1, ordered_steps: Sequence[CurrentThreeParityTraceStepV1]
) -> CurrentThreeParityTraceV1:
    steps = tuple(ordered_steps)
    hash_payload = {
        "algo_code": algo_code,
        "side": side.value,
        "ordered_steps": [{"step_ordinal": item.step_ordinal, "step_sha256": item.step_sha256} for item in steps],
    }
    return CurrentThreeParityTraceV1(
        schema_version="miniqmt_current_three_parity_trace_v1",
        algo_code=algo_code,
        side=side,
        ordered_steps=steps,
        trace_sha256=hash_hex_v1("miniqmt_current_three_parity_trace_v1", hash_payload),
    )


def _walk_differences(
    legacy: Any,
    kernel: Any,
    *,
    step_ordinal: int,
    path: str,
    output: list[CurrentThreeParityDifferenceV1],
) -> None:
    if legacy == kernel:
        return
    if isinstance(legacy, dict) and isinstance(kernel, dict):
        for key in sorted(set(legacy) | set(kernel)):
            _walk_differences(
                legacy.get(key, {"__missing__": True}),
                kernel.get(key, {"__missing__": True}),
                step_ordinal=step_ordinal,
                path=f"{path}.{key}",
                output=output,
            )
        return
    if isinstance(legacy, list) and isinstance(kernel, list):
        for index in range(max(len(legacy), len(kernel))):
            left = legacy[index] if index < len(legacy) else {"__missing__": True}
            right = kernel[index] if index < len(kernel) else {"__missing__": True}
            _walk_differences(
                left,
                right,
                step_ordinal=step_ordinal,
                path=f"{path}[{index}]",
                output=output,
            )
        return
    left_hash = hash_hex_v1("miniqmt_current_three_parity_difference_value_v1", _canonical(legacy))
    right_hash = hash_hex_v1("miniqmt_current_three_parity_difference_value_v1", _canonical(kernel))
    context = {"field_path": path, "legacy_type": type(legacy).__name__, "kernel_type": type(kernel).__name__}
    output.append(
        CurrentThreeParityDifferenceV1(
            schema_version="miniqmt_current_three_parity_difference_v1",
            step_ordinal=step_ordinal,
            field_path=path,
            legacy_value_sha256=left_hash,
            kernel_value_sha256=right_hash,
            reason_code="MINIQMT_K3_PARITY_DRIFT",
            context_sha256=hash_hex_v1("miniqmt_current_three_parity_difference_context_v1", context),
        )
    )


def _differences(
    legacy_trace: CurrentThreeParityTraceV1, kernel_trace: CurrentThreeParityTraceV1
) -> tuple[CurrentThreeParityDifferenceV1, ...]:
    raw: list[CurrentThreeParityDifferenceV1] = []
    steps = max(len(legacy_trace.ordered_steps), len(kernel_trace.ordered_steps))
    for index in range(steps):
        legacy = (
            legacy_trace.ordered_steps[index].canonical_payload_v1(exclude={"step_sha256"})
            if index < len(legacy_trace.ordered_steps)
            else {"__missing__": True}
        )
        kernel = (
            kernel_trace.ordered_steps[index].canonical_payload_v1(exclude={"step_sha256"})
            if index < len(kernel_trace.ordered_steps)
            else {"__missing__": True}
        )
        _walk_differences(legacy, kernel, step_ordinal=index, path=f"steps[{index}]", output=raw)
    ordered = sorted(raw, key=lambda item: item.sort_key_v1())
    if len(ordered) <= MAX_K3_FAILURES:
        return tuple(ordered)
    omitted = ordered[MAX_K3_FAILURES - 1 :]
    omitted_hash = hash_hex_v1(
        "miniqmt_current_three_parity_omitted_difference_set_v1",
        [item.canonical_payload_v1() for item in omitted],
    )
    marker = CurrentThreeParityDifferenceV1(
        schema_version="miniqmt_current_three_parity_difference_v1",
        step_ordinal=max(item.step_ordinal for item in ordered),
        field_path="zzzz.__truncated__",
        legacy_value_sha256=omitted_hash,
        kernel_value_sha256=omitted_hash,
        reason_code="MINIQMT_K3_PARITY_DIFFERENCE_SET_TRUNCATED",
        context_sha256=hash_hex_v1(
            "miniqmt_current_three_parity_difference_context_v1",
            {"omitted_count": len(omitted), "omitted_difference_set_sha256": omitted_hash},
        ),
    )
    return tuple(sorted((*ordered[: MAX_K3_FAILURES - 1], marker), key=lambda item: item.sort_key_v1()))


def build_current_three_parity_receipt_v1(
    *,
    parity_input: CurrentThreeParityInputV1,
    legacy_source_attribution_sha256: str,
    plugin_id: str,
    plugin_version: str,
    plugin_manifest_sha256: str,
    legacy_trace: CurrentThreeParityTraceV1,
    kernel_trace: CurrentThreeParityTraceV1,
) -> CurrentThreeParityReceiptV1:
    if legacy_trace.algo_code != parity_input.algo_code or kernel_trace.algo_code != parity_input.algo_code:
        raise ValueError("parity traces do not belong to the parity input algorithm")
    if legacy_trace.side is not parity_input.side or kernel_trace.side is not parity_input.side:
        raise ValueError("parity traces do not belong to the parity input side")
    differences = _differences(legacy_trace, kernel_trace)
    status = CurrentThreeParityStatusV1.PASSED if not differences else CurrentThreeParityStatusV1.FAILED
    payload = {
        "schema_version": "miniqmt_current_three_parity_receipt_v1",
        "algo_code": parity_input.algo_code,
        "legacy_source_attribution_sha256": legacy_source_attribution_sha256,
        "plugin_id": plugin_id,
        "plugin_version": plugin_version,
        "plugin_manifest_sha256": plugin_manifest_sha256,
        "plugin_config_sha256": parity_input.plugin_config_sha256,
        "parity_input_sha256": parity_input.input_sha256,
        "execution_coordination_scope": "ALGO_LOCAL_ONLY",
        "ordered_event_refs": parity_input.ordered_event_refs,
        "event_set_sha256": parity_input.event_set_sha256,
        "legacy_trace_sha256": legacy_trace.trace_sha256,
        "kernel_trace_sha256": kernel_trace.trace_sha256,
        "ordered_differences": differences,
        "status": status,
        "broker_called": False,
    }
    return CurrentThreeParityReceiptV1(
        **payload,
        receipt_sha256=_hash("miniqmt_current_three_parity_receipt_v1", payload),
    )


def associate_current_three_shadow_commands_v1(
    *,
    read: CurrentThreeShadowRepositoryReadV1,
    parity_input: CurrentThreeParityInputV1,
    commands_by_step: Sequence[Sequence[BrokerCommandV2]],
) -> tuple[CurrentThreeShadowCommandAssociationV1, ...]:
    """Join committed legacy children to K3 submit commands by exact business facts."""

    read.strict_readback_v1()
    child_ref_by_id = {item.identity: item for item in read.snapshot.ordered_child_fact_refs}
    legacy_algos = [
        item
        for item in read.algos
        if item.parent_intent_id == parity_input.parent_intent_id
        and item.strategy_slot_id == parity_input.strategy_slot_id
        and item.algo_code == parity_input.algo_code
    ]
    if len(legacy_algos) != 1:
        raise CurrentThreeContractError(
            "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
            "parity input does not close to one legacy algo owner",
            context={
                "stage": "K3_SHADOW_ASSOCIATION",
                "parity_input_sha256": parity_input.input_sha256,
                "legacy_algo_match_count": len(legacy_algos),
            },
        )
    legacy_algo_id = legacy_algos[0].algo_instance_id
    used_children: set[str] = set()
    associations: list[CurrentThreeShadowCommandAssociationV1] = []

    def child_reason(child: Any) -> str | None:
        present = [(name, child.metadata[name]) for name in ("reason_code", "vnpy_reason") if name in child.metadata]
        if not present:
            return None
        first = present[0][1]
        if type(first) is not str or not first or any(value != first for _, value in present[1:]):
            raise CurrentThreeContractError(
                "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                "legacy child reason aliases are missing, malformed or conflicting",
                context={
                    "stage": "K3_SHADOW_ASSOCIATION",
                    "child_order_id": child.child_order_id,
                    "reason_aliases": [name for name, _ in present],
                },
            )
        return first

    for step_ordinal, commands in enumerate(commands_by_step):
        for effect_ordinal, command in enumerate(commands):
            if command.command_type is not BrokerCommandTypeV2.SUBMIT_LIMIT:
                continue
            matches = []
            for child in read.children:
                reason = child_reason(child)
                if (
                    child.algo_instance_id == legacy_algo_id
                    and child.parent_intent_id == parity_input.parent_intent_id
                    and child.symbol == command.symbol
                    and child.side.value == command.side.value
                    and Decimal(str(child.price)) == Decimal(command.price_decimal)
                    and child.quantity == command.quantity
                    and reason == command.reason_code
                ):
                    matches.append(child)
            if len(matches) != 1:
                raise CurrentThreeContractError(
                    "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                    "shadow submit command does not have one exact committed legacy child",
                    context={
                        "stage": "K3_SHADOW_ASSOCIATION",
                        "parity_input_sha256": parity_input.input_sha256,
                        "step_ordinal": step_ordinal,
                        "business_effect_ordinal": effect_ordinal,
                        "command_id": command.command_id,
                        "match_count": len(matches),
                    },
                )
            child = matches[0]
            if child.child_order_id in used_children or child.broker_order_id is None:
                raise CurrentThreeContractError(
                    "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
                    "shadow legacy child was reused or lacks a broker identity",
                    context={
                        "stage": "K3_SHADOW_ASSOCIATION",
                        "child_order_id": child.child_order_id,
                        "broker_order_id": child.broker_order_id,
                    },
                )
            used_children.add(child.child_order_id)
            mapping = ExecutionCommandChildMappingV1.create(
                command=command,
                strategy_slot_id=parity_input.strategy_slot_id,
                mapping_status=CommandChildMappingStatusV1.RESERVED,
                mapping_version=1,
                broker_order_id=None,
                broker_identity_source_event_id=None,
                last_order_event_id=None,
                last_trade_event_id=None,
                updated_by_event_id=None,
                created_at_utc=parity_input.ordered_event_refs[step_ordinal].logical_time_utc,
                updated_at_utc=parity_input.ordered_event_refs[step_ordinal].logical_time_utc,
            )
            payload = {
                "schema_version": "miniqmt_current_three_shadow_command_association_v1",
                "parity_input_sha256": parity_input.input_sha256,
                "step_ordinal": step_ordinal,
                "business_effect_ordinal": effect_ordinal,
                "legacy_algo_instance_id": child.algo_instance_id,
                "legacy_child_order_id": child.child_order_id,
                "legacy_broker_order_id": child.broker_order_id,
                "legacy_child_payload_sha256": child_ref_by_id[child.child_order_id].payload_sha256,
                "kernel_runtime_id": command.runtime_id,
                "kernel_algo_instance_id": command.algo_instance_id,
                "transition_id": command.transition_id,
                "kernel_command_id": command.command_id,
                "mapping_id": mapping.mapping_id,
                "local_vt_orderid": command.local_vt_orderid,
                "symbol": command.symbol,
                "side": command.side,
                "canonical_price": command.price_decimal,
                "quantity": command.quantity,
                "reason_code": command.reason_code,
            }
            associations.append(
                CurrentThreeShadowCommandAssociationV1(
                    **payload,
                    association_sha256=_hash("miniqmt_current_three_shadow_command_association_v1", payload),
                )
            )
    return tuple(associations)


__all__ = [
    "build_current_three_parity_input_v1",
    "build_current_three_parity_receipt_v1",
    "build_current_three_parity_trace_step_v1",
    "build_current_three_parity_trace_v1",
    "build_parity_event_ref_v1",
    "associate_current_three_shadow_commands_v1",
]
