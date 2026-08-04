"""Final K6 dependent-BUY coordinator decision authority.

The coordinator is deliberately a pure product component.  It consumes only
strict K6 durable carriers that a repository has reconstructed from K2 and
``qmt_strategy_ledger`` facts.  It neither evaluates an algorithm nor calls a
broker.  The repository owns source-fact reconstruction, locking, fencing,
CAS and the same-command K2 outbox write.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from collections.abc import Mapping
from typing import Any, Final

from .kernel_product_contracts import (
    DependentBuyCoordinationStatusV1,
    DependentBuyCoordinationV2,
    DependentBuyDecisionV1,
    DependentBuyDependencyStatusV1,
    DependentBuyLedgerObservationV2,
    DependentBuyReleaseDecisionV2,
    DependentBuySellDependencyV2,
    DependentBuyTriggerEventRefV1,
    DependentBuyTriggerTypeV1,
    ProductCommandAuthorityItemV3,
    ProductCommandAuthoritySetV3,
    ProductCommandChildMappingV1,
    ProductCommandChildMappingStatusV1,
    ProductCommandDispositionV3,
)
from .plugin_canonical import canonical_decimal_string_v1, canonical_utc_datetime_v1, hash_hex_v1, thaw_json_v1
from .plugin_contracts import BrokerCommandOutboxStatusV1, BrokerCommandOutboxV1


_WAIT_REASON: Final = "MINIQMT_DEPENDENT_BUY_WAITING_SELL_PROCEEDS"
_RELEASE_REASON: Final = "MINIQMT_DEPENDENT_BUY_RELEASED_TO_K2_OUTBOX"
_BLOCK_REASON: Final = "MINIQMT_DEPENDENT_BUY_SELL_PROCEEDS_UNAVAILABLE"
_EOD_REASON: Final = "MINIQMT_DEPENDENT_BUY_EOD_RESIDUAL"

_TRADE_LEDGER_COLUMNS: Final = (
    "trade_id",
    "intent_id",
    "strategy_id",
    "qmt_order_id",
    "qmt_order_sysid",
    "symbol",
    "side",
    "price",
    "quantity",
    "amount",
    "commission",
    "trade_date",
    "account_id",
    "trade_time",
    "order_remark",
    "raw_json",
)
_CASH_LEDGER_COLUMNS: Final = (
    "cash_id",
    "cash_sequence",
    "strategy_id",
    "account_id",
    "trade_date",
    "entry_type",
    "cash_delta",
    "cash_after",
    "frozen_delta",
    "frozen_after",
    "intent_id",
    "trade_id",
    "symbol",
    "reason",
    "metadata",
    "created_at",
)


def _strict_row(row: Mapping[str, Any], *, columns: tuple[str, ...], carrier_name: str) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise TypeError(f"{carrier_name} must be a mapping")
    actual = set(row)
    expected = set(columns)
    if actual != expected:
        raise ValueError(
            f"{carrier_name} columns differ from source authority: "
            f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}"
        )
    return {column: row[column] for column in columns}


def _strict_optional_identity(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{field_name} must be null or one canonical strict identity")
    return value


def _strict_identity(value: object, *, field_name: str) -> str:
    normalized = _strict_optional_identity(value, field_name=field_name)
    if normalized is None:
        raise ValueError(f"{field_name} must be one canonical strict identity")
    return normalized


def _strict_optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str:
        raise ValueError(f"{field_name} must be null or one string")
    return value


def _strict_text(value: object, *, field_name: str) -> str:
    normalized = _strict_optional_text(value, field_name=field_name)
    if normalized is None:
        raise ValueError(f"{field_name} must be one string")
    return normalized


def qmt_trade_ledger_fact_sha256_v1(row: Mapping[str, Any]) -> str:
    """Rebuild one exact K6 settled SELL trade fact from a selected SQL row."""

    values = _strict_row(row, columns=_TRADE_LEDGER_COLUMNS, carrier_name="qmt trade ledger row")
    for field_name in (
        "trade_id",
        "intent_id",
        "strategy_id",
        "qmt_order_id",
        "symbol",
        "account_id",
    ):
        values[field_name] = _strict_identity(values[field_name], field_name=field_name)
    values["qmt_order_sysid"] = _strict_optional_text(values["qmt_order_sysid"], field_name="qmt_order_sysid")
    values["order_remark"] = _strict_text(values["order_remark"], field_name="order_remark")
    if values["side"] != "SELL":
        raise ValueError("dependent-BUY settled trade ledger side must be SELL")
    if type(values["quantity"]) is not int or values["quantity"] <= 0:
        raise ValueError("trade ledger quantity must be one strict positive integer")
    for field_name in ("price", "amount", "commission"):
        values[field_name] = canonical_decimal_string_v1(values[field_name], field_name=field_name)
    if values["trade_time"] is not None:
        values["trade_time"] = canonical_utc_datetime_v1(values["trade_time"], field_name="trade_time")
    if not isinstance(values["raw_json"], Mapping):
        raise ValueError("trade ledger raw_json must be one JSON object")
    values["raw_json"] = dict(values["raw_json"])
    if type(values["trade_date"]) is not date:
        raise ValueError("trade ledger trade_date must be one date")
    values["trade_date"] = values["trade_date"].isoformat()
    return hash_hex_v1("miniqmt_k6_qmt_trade_ledger_fact_v1", values)


def cash_ledger_fact_sha256_v1(row: Mapping[str, Any]) -> str:
    """Rebuild one exact append-only K6 cash-ledger fact from a selected SQL row."""

    values = _strict_row(row, columns=_CASH_LEDGER_COLUMNS, carrier_name="cash ledger row")
    for field_name in ("cash_id", "strategy_id", "account_id"):
        values[field_name] = _strict_identity(values[field_name], field_name=field_name)
    for field_name in ("intent_id", "trade_id", "symbol"):
        values[field_name] = _strict_optional_identity(values[field_name], field_name=field_name)
    values["reason"] = _strict_optional_text(values["reason"], field_name="reason")
    if type(values["cash_sequence"]) is not int or values["cash_sequence"] <= 0:
        raise ValueError("cash ledger sequence must be one strict positive integer")
    if type(values["entry_type"]) is not str or values["entry_type"] not in {
        "INITIAL_ALLOCATE",
        "FREEZE_BUY",
        "UNFREEZE_CANCEL",
        "UNFREEZE_REJECT",
        "BUY_FILL",
        "SELL_FILL",
        "FEE",
        "MANUAL_ADJUST",
    }:
        raise ValueError("cash ledger entry_type is not a strict ledger authority value")
    if values["entry_type"] != "SELL_FILL":
        raise ValueError("dependent-BUY settled cash ledger entry_type must be SELL_FILL")
    for field_name in ("cash_delta", "cash_after", "frozen_delta", "frozen_after"):
        values[field_name] = canonical_decimal_string_v1(values[field_name], field_name=field_name)
    if not isinstance(values["metadata"], Mapping):
        raise ValueError("cash ledger metadata must be one JSON object")
    values["metadata"] = dict(values["metadata"])
    if type(values["trade_date"]) is not date:
        raise ValueError("cash ledger trade_date must be one date")
    values["trade_date"] = values["trade_date"].isoformat()
    values["created_at"] = canonical_utc_datetime_v1(values["created_at"], field_name="created_at")
    return hash_hex_v1("miniqmt_k6_cash_ledger_fact_v1", values)


def _strict_roundtrip(model_type: type[object], value: object, *, field_name: str) -> object:
    if not isinstance(value, model_type):
        raise TypeError(f"{field_name} must be {model_type.__name__}")
    try:
        return model_type.model_validate_json(value.model_dump_json(), strict=True)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} fails strict durable carrier validation") from exc


def _validate_evaluation_closure(
    *,
    coordination: DependentBuyCoordinationV2,
    trigger: DependentBuyTriggerEventRefV1,
    ledger_observation: DependentBuyLedgerObservationV2,
    session_authority_sha256: str,
) -> None:
    if coordination.status is not DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS:
        raise ValueError("dependent-BUY terminal coordination cannot be evaluated again")
    if trigger.runtime_id != coordination.runtime_id:
        raise ValueError("dependent-BUY trigger runtime differs from coordination")
    if (
        ledger_observation.runtime_id,
        ledger_observation.strategy_id,
        ledger_observation.trade_date,
        ledger_observation.required_cash,
        ledger_observation.virtual_account_id,
    ) != (
        coordination.runtime_id,
        coordination.strategy_id,
        coordination.trade_date,
        coordination.required_cash,
        coordination.virtual_account_id,
    ):
        raise ValueError("dependent-BUY ledger observation owner, account, or required cash differs from coordination")
    if (
        type(session_authority_sha256) is not str
        or len(session_authority_sha256) != 64
        or any(character not in "0123456789abcdef" for character in session_authority_sha256)
    ):
        raise ValueError("session_authority_sha256 must be one lowercase SHA-256")
    if session_authority_sha256 != coordination.session_authority_sha256:
        raise ValueError("repository session authority differs from frozen dependent-BUY authority")
    if ledger_observation.freshness_session_authority_sha256 != session_authority_sha256:
        raise ValueError("dependent-BUY ledger observation session authority differs from repository readback")
    trigger_time = datetime.fromisoformat(str(trigger.observed_at_utc).replace("Z", "+00:00"))
    ledger_time = datetime.fromisoformat(str(ledger_observation.ledger_as_of_utc).replace("Z", "+00:00"))
    if ledger_time < trigger_time:
        raise ValueError("dependent-BUY ledger observation predates trigger evidence")
    dependency_refs = tuple(
        ref for dependency in coordination.ordered_sell_dependencies for ref in dependency.ordered_settled_proceeds_refs
    )
    if ledger_observation.ordered_settled_proceeds_refs != tuple(
        sorted(dependency_refs, key=lambda item: item.sort_key_v2())
    ):
        raise ValueError("dependent-BUY ledger observation differs from frozen settled proceeds closure")


def evaluate_dependent_buy_decision_v2(
    *,
    coordination: DependentBuyCoordinationV2,
    trigger: DependentBuyTriggerEventRefV1,
    ledger_observation: DependentBuyLedgerObservationV2,
    session_authority_sha256: str,
) -> tuple[DependentBuyDecisionV1, str]:
    """Return the only legal decision for one reconstructed K6-B trigger.

    This function deliberately does not fabricate a decision receipt.  The
    durable writer must first lock and independently rebuild the session and
    ledger facts; it then calls :func:`build_dependent_buy_release_decision_v2`
    with its exact worker fence.
    """

    coordination = _strict_roundtrip(DependentBuyCoordinationV2, coordination, field_name="coordination")
    trigger = _strict_roundtrip(DependentBuyTriggerEventRefV1, trigger, field_name="trigger")
    ledger_observation = _strict_roundtrip(
        DependentBuyLedgerObservationV2,
        ledger_observation,
        field_name="ledger_observation",
    )
    _validate_evaluation_closure(
        coordination=coordination,
        trigger=trigger,
        ledger_observation=ledger_observation,
        session_authority_sha256=session_authority_sha256,
    )

    if trigger.event_type is DependentBuyTriggerTypeV1.SESSION_EOD:
        return DependentBuyDecisionV1.EOD_RESIDUAL, _EOD_REASON

    cash_is_sufficient = Decimal(ledger_observation.available_cash) >= Decimal(coordination.required_cash)
    if trigger.event_type in {
        DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED,
        DependentBuyTriggerTypeV1.ACCOUNT_REFRESHED,
    }:
        return (
            (DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX, _RELEASE_REASON)
            if cash_is_sufficient
            else (DependentBuyDecisionV1.WAIT, _WAIT_REASON)
        )

    if trigger.event_type is DependentBuyTriggerTypeV1.SELL_ORDER_TERMINAL:
        all_terminal_without_proceeds = all(
            dependency.dependency_status is DependentBuyDependencyStatusV1.TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS
            for dependency in coordination.ordered_sell_dependencies
        )
        if all_terminal_without_proceeds and Decimal(ledger_observation.cash_shortfall) > Decimal("0"):
            return DependentBuyDecisionV1.BLOCK, _BLOCK_REASON
        return DependentBuyDecisionV1.WAIT, _WAIT_REASON

    raise ValueError(f"unsupported dependent-BUY trigger type: {trigger.event_type}")


def build_dependent_buy_release_decision_v2(
    *,
    coordination: DependentBuyCoordinationV2,
    trigger: DependentBuyTriggerEventRefV1,
    ledger_observation: DependentBuyLedgerObservationV2,
    decision: DependentBuyDecisionV1,
    reason_code: str,
    decided_at_utc: object,
    worker_id: str,
    process_incarnation_id: str,
    lease_epoch: int,
    session_authority_sha256: str,
    release_event_id: str | None = None,
    release_command_authority_set_sha256: str | None = None,
) -> DependentBuyReleaseDecisionV2:
    """Build one hash-closed durable decision after a repository evaluation."""

    coordination = _strict_roundtrip(DependentBuyCoordinationV2, coordination, field_name="coordination")
    trigger = _strict_roundtrip(DependentBuyTriggerEventRefV1, trigger, field_name="trigger")
    ledger_observation = _strict_roundtrip(
        DependentBuyLedgerObservationV2,
        ledger_observation,
        field_name="ledger_observation",
    )
    expected_decision, expected_reason = evaluate_dependent_buy_decision_v2(
        coordination=coordination,
        trigger=trigger,
        ledger_observation=ledger_observation,
        session_authority_sha256=session_authority_sha256,
    )
    if (decision, reason_code) != (expected_decision, expected_reason):
        raise ValueError("dependent-BUY decision does not equal pure state-machine result")
    is_release = decision is DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX
    if is_release != (release_event_id is not None and release_command_authority_set_sha256 is not None):
        raise ValueError("dependent-BUY release identities must be complete only for RELEASE")
    return DependentBuyReleaseDecisionV2.create(
        coordination_id=coordination.coordination_id,
        decision_sequence=coordination.decision_sequence + 1,
        previous_decision_sha256=coordination.last_decision_sha256,
        trigger_ref_sha256=trigger.trigger_ref_sha256,
        decision=decision,
        reason_code=reason_code,
        ledger_observation_sha256=ledger_observation.observation_sha256,
        ordered_dependency_sha256s=tuple(
            sorted(item.dependency_sha256 for item in coordination.ordered_sell_dependencies)
        ),
        release_event_id=release_event_id,
        release_transition_id=coordination.release_transition_id if is_release else None,
        release_command_authority_set_sha256=release_command_authority_set_sha256,
        decided_at_utc=decided_at_utc,
        worker_id=worker_id,
        process_incarnation_id=process_incarnation_id,
        lease_epoch=lease_epoch,
    )


def build_dependent_buy_trigger_bundle_v2(
    *,
    coordination: DependentBuyCoordinationV2,
    authority_item: ProductCommandAuthorityItemV3,
    authority_set: ProductCommandAuthoritySetV3,
    mapping: ProductCommandChildMappingV1,
    observed_dependencies: tuple[DependentBuySellDependencyV2, ...],
    trigger: DependentBuyTriggerEventRefV1,
    ledger_observation: DependentBuyLedgerObservationV2,
    session_authority_sha256: str,
    decided_at_utc: object,
    worker_id: str,
    process_incarnation_id: str,
    lease_epoch: int,
    lease_expires_at_utc: object,
) -> tuple[
    DependentBuyReleaseDecisionV2,
    DependentBuyCoordinationV2,
    ProductCommandChildMappingV1,
    BrokerCommandOutboxV1 | None,
]:
    """Build the complete K6-B successor set for one already-locked trigger.

    The repository alone supplies the locked, independently reconstructed
    inputs and writes this tuple atomically.  In particular, the function does
    not create an event, transition, command or broker side effect.
    """

    coordination = _strict_roundtrip(DependentBuyCoordinationV2, coordination, field_name="coordination")
    authority_item = _strict_roundtrip(ProductCommandAuthorityItemV3, authority_item, field_name="authority_item")
    authority_set = _strict_roundtrip(ProductCommandAuthoritySetV3, authority_set, field_name="authority_set")
    mapping = _strict_roundtrip(ProductCommandChildMappingV1, mapping, field_name="mapping")
    if not isinstance(observed_dependencies, tuple):
        raise TypeError("observed_dependencies must be one strict tuple")
    observed_dependencies = tuple(
        _strict_roundtrip(DependentBuySellDependencyV2, item, field_name="observed dependency")
        for item in observed_dependencies
    )
    if authority_item not in authority_set.ordered_items:
        raise ValueError("deferred authority item is absent from the strict authority set")
    if (
        authority_set.runtime_id != coordination.runtime_id
        or authority_set.transition_id != coordination.release_transition_id
        or authority_item.runtime_id != authority_set.runtime_id
        or authority_item.transition_id != authority_set.transition_id
    ):
        raise ValueError("deferred authority item does not close to the strict authority-set owner")
    command = authority_item.command_json
    if (
        authority_item.disposition is not ProductCommandDispositionV3.DEFER_DEPENDENT_BUY
        or authority_item.coordination_id != coordination.coordination_id
        or authority_item.command_id != coordination.release_command_id
        or authority_item.transition_id != coordination.release_transition_id
        or authority_item.item_sha256 != coordination.release_command_authority_item_sha256
        or command.payload_sha256 != coordination.release_command_payload_sha256
    ):
        raise ValueError("deferred authority item does not close to dependent-BUY coordination")
    if (
        mapping.mapping_status is not ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY
        or mapping.coordination_id != coordination.coordination_id
        or mapping.authority_item_sha256 != authority_item.item_sha256
        or mapping.command_id != command.command_id
        or mapping.created_transition_id != command.transition_id
    ):
        raise ValueError("deferred mapping does not close to dependent-BUY authority")
    if tuple(item.sell_parent_intent_id for item in observed_dependencies) != tuple(
        item.sell_parent_intent_id for item in coordination.ordered_sell_dependencies
    ):
        raise ValueError("observed SELL dependencies do not close to frozen dependent-BUY dependency set")
    evaluation_values = coordination.model_dump(
        mode="python",
        exclude={"schema_version", "coordination_sha256", "ordered_sell_dependencies"},
    )
    evaluation_coordination = DependentBuyCoordinationV2.create(
        **evaluation_values,
        ordered_sell_dependencies=observed_dependencies,
    )
    if type(lease_epoch) is not int or lease_epoch <= 0:
        raise ValueError("lease_epoch must be one strict positive integer")
    previous_lease = (
        coordination.lease_worker_id,
        coordination.lease_process_incarnation_id,
        coordination.lease_epoch,
    )
    if coordination.lease_epoch == 0:
        if lease_epoch != 1:
            raise ValueError("first dependent-BUY coordinator lease epoch must be one")
    elif lease_epoch not in {coordination.lease_epoch, coordination.lease_epoch + 1}:
        raise ValueError("dependent-BUY lease epoch is not current or exact successor")
    elif lease_epoch == coordination.lease_epoch and previous_lease[:2] != (worker_id, process_incarnation_id):
        raise ValueError("same dependent-BUY lease epoch cannot change worker owner")
    decided_at = datetime.fromisoformat(str(decided_at_utc).replace("Z", "+00:00"))
    lease_expires_at = datetime.fromisoformat(str(lease_expires_at_utc).replace("Z", "+00:00"))
    if lease_expires_at <= decided_at:
        raise ValueError("dependent-BUY lease expiry must be strictly after its decision time")

    decision, reason_code = evaluate_dependent_buy_decision_v2(
        coordination=evaluation_coordination,
        trigger=trigger,
        ledger_observation=ledger_observation,
        session_authority_sha256=session_authority_sha256,
    )
    release = decision is DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX
    receipt = build_dependent_buy_release_decision_v2(
        coordination=evaluation_coordination,
        trigger=trigger,
        ledger_observation=ledger_observation,
        decision=decision,
        reason_code=reason_code,
        decided_at_utc=decided_at_utc,
        worker_id=worker_id,
        process_incarnation_id=process_incarnation_id,
        lease_epoch=lease_epoch,
        session_authority_sha256=session_authority_sha256,
        release_event_id=trigger.event_id if release else None,
        release_command_authority_set_sha256=authority_set.authority_set_sha256 if release else None,
    )
    status = {
        DependentBuyDecisionV1.WAIT: DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS,
        DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX: DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX,
        DependentBuyDecisionV1.BLOCK: DependentBuyCoordinationStatusV1.BLOCKED_SELL_PROCEEDS_UNAVAILABLE,
        DependentBuyDecisionV1.EOD_RESIDUAL: DependentBuyCoordinationStatusV1.EOD_RESIDUAL,
    }[decision]
    coordination_values = evaluation_coordination.model_dump(
        mode="python",
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
            "lease_worker_id",
            "lease_process_incarnation_id",
            "lease_epoch",
            "lease_expires_at_utc",
            "updated_at_utc",
        },
    )
    successor = DependentBuyCoordinationV2.create(
        **coordination_values,
        status=status,
        decision_sequence=receipt.decision_sequence,
        last_decision_sha256=receipt.decision_sha256,
        released_command_id=command.command_id if release else None,
        released_outbox_id=command.command_id if release else None,
        row_version=coordination.row_version + 1,
        lease_worker_id=worker_id,
        lease_process_incarnation_id=process_incarnation_id,
        lease_epoch=lease_epoch,
        lease_expires_at_utc=lease_expires_at_utc,
        updated_at_utc=decided_at_utc,
    )
    successor.validate_successor_v2(coordination)
    if decision is DependentBuyDecisionV1.WAIT:
        return receipt, successor, mapping, None
    updated_mapping = ProductCommandChildMappingV1.create_successor(
        previous=mapping,
        mapping_status=(
            ProductCommandChildMappingStatusV1.RESERVED if release else ProductCommandChildMappingStatusV1.TERMINAL
        ),
        updated_by_event_id=trigger.event_id,
        updated_at_utc=decided_at_utc,
    )
    if not release:
        return receipt, successor, updated_mapping, None
    outbox = BrokerCommandOutboxV1.create(
        command=command,
        mapping_id=mapping.mapping_id,
        status=BrokerCommandOutboxStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at=None,
        dispatch_attempt_id=None,
        callback_watermark_before_call=None,
        next_attempt_at_utc=None,
        broker_called=None,
        broker_order_id=None,
        ack_receipt_json=None,
        ack_receipt_sha256=None,
        non_acceptance_receipt=None,
        unknown_outcome_receipt=None,
        reconcile_receipt=None,
        last_error_json=None,
        row_version=1,
        created_at_utc=decided_at_utc,
        updated_at_utc=decided_at_utc,
        closed_at_utc=None,
    )
    if thaw_json_v1(outbox.payload_json) != command.model_dump(mode="json"):
        raise ValueError("dependent-BUY release outbox payload differs from frozen authority command")
    return receipt, successor, updated_mapping, outbox


__all__ = [
    "build_dependent_buy_release_decision_v2",
    "build_dependent_buy_trigger_bundle_v2",
    "cash_ledger_fact_sha256_v1",
    "evaluate_dependent_buy_decision_v2",
    "qmt_trade_ledger_fact_sha256_v1",
]
