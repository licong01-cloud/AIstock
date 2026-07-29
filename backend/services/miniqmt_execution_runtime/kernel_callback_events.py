"""Strict callback and command-outcome payload writer/readback authority."""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from backend.services.qmt_strategy_ledger.models import TradeLedgerRecord
from backend.services.qmt_strategy_ledger.tca_models import canonical_trade_fact_sha256

from .plugin_canonical import canonical_decimal_string_v1, hash_hex_v1, thaw_json_v1
from .plugin_contracts import (
    BrokerCommandTypeV2,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerReconciliationOutcomeV1,
    BrokerCommandV2,
    ExecutionCommandChildMappingV1,
    KernelErrorEvidenceV1,
    KernelCommandOutcomeEventPayloadV1,
    KernelCommandOutcomeV1,
    KernelOrderEventPayloadV1,
    KernelOrderReconcileEventPayloadV1,
    KernelTradeEventPayloadV1,
    KernelTradeFactRefV1,
    NormalizedOrderStatusV1,
    RuntimeEventEnvelopeV2,
    SideV1,
)

_NUMERIC_ORDER_STATUS = {
    48: NormalizedOrderStatusV1.ACCEPTED,
    49: NormalizedOrderStatusV1.ACCEPTED,
    50: NormalizedOrderStatusV1.ACCEPTED,
    51: NormalizedOrderStatusV1.ACCEPTED,
    52: NormalizedOrderStatusV1.PARTIALLY_FILLED,
    53: NormalizedOrderStatusV1.PARTIALLY_FILLED,
    55: NormalizedOrderStatusV1.PARTIALLY_FILLED,
    54: NormalizedOrderStatusV1.CANCELLED,
    56: NormalizedOrderStatusV1.FILLED,
    57: NormalizedOrderStatusV1.REJECTED,
}
_TEXT_ORDER_STATUS = {
    **{
        value: NormalizedOrderStatusV1.ACCEPTED
        for value in ("OPEN", "SUBMITTED", "PENDING", "CANCEL_REQUESTED", "ACTIVE", "ACCEPTED")
    },
    **{
        value: NormalizedOrderStatusV1.PARTIALLY_FILLED
        for value in ("PARTIALLY_FILLED", "PARTIAL_FILLED", "PART_TRADED")
    },
    "CANCELLED": NormalizedOrderStatusV1.CANCELLED,
    "CANCELED": NormalizedOrderStatusV1.CANCELLED,
    "FILLED": NormalizedOrderStatusV1.FILLED,
    "ALL_TRADED": NormalizedOrderStatusV1.FILLED,
    "REJECTED": NormalizedOrderStatusV1.REJECTED,
    "BROKER_REJECTED": NormalizedOrderStatusV1.REJECTED,
}
_STATUS_ALIASES = ("order_status", "status", "order_status_code", "status_code")
_CUMULATIVE_ALIASES = (
    "traded_volume",
    "filled_quantity",
    "filled_volume",
    "cumulative_quantity",
    "traded_quantity",
)
_TRADE_ID_ALIASES = ("trade_id", "traded_id", "deal_id", "qmt_trade_id", "native_trade_id")


def normalize_qmt_order_status_v1(raw_status: object) -> NormalizedOrderStatusV1:
    if type(raw_status) is int:
        try:
            return _NUMERIC_ORDER_STATUS[raw_status]
        except KeyError as exc:
            raise ValueError("unknown QMT numeric order status") from exc
    if type(raw_status) is str:
        try:
            return _TEXT_ORDER_STATUS[raw_status]
        except KeyError as exc:
            raise ValueError("unknown or noncanonical QMT text order status") from exc
    raise TypeError("QMT order status must be a strict integer code or exact text literal")


def _mapping(raw_payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw_payload, Mapping):
        raise TypeError("raw callback payload must be a mapping")
    payload = dict(raw_payload)
    if any(type(key) is not str for key in payload):
        raise TypeError("raw callback payload keys must be strict strings")
    hash_hex_v1("miniqmt_gateway_source_payload_v1", payload)
    return payload


def _one_alias(payload: Mapping[str, Any], aliases: Sequence[str], *, field_name: str) -> Any | None:
    present = [(name, payload[name]) for name in aliases if name in payload]
    if not present:
        return None
    first = present[0][1]
    if any(value != first or type(value) is not type(first) for _, value in present[1:]):
        raise ValueError(f"{field_name} aliases conflict")
    return first


def _normalized_status_from_payload(payload: Mapping[str, Any]) -> NormalizedOrderStatusV1:
    values = [payload[name] for name in _STATUS_ALIASES if name in payload]
    if not values:
        raise ValueError("QMT order callback is missing order status")
    normalized = tuple(normalize_qmt_order_status_v1(value) for value in values)
    if len(set(normalized)) != 1:
        raise ValueError("QMT numeric/text order status aliases conflict")
    return normalized[0]


def normalize_qmt_order_callback_observation_v1(
    raw_payload: Mapping[str, Any],
) -> tuple[NormalizedOrderStatusV1, int | None]:
    """Read the one authoritative QMT ORDER status/cumulative observation."""

    payload = _mapping(raw_payload)
    status = _normalized_status_from_payload(payload)
    cumulative = _one_alias(payload, _CUMULATIVE_ALIASES, field_name="cumulative quantity")
    if cumulative is not None and (type(cumulative) is not int or cumulative < 0):
        raise TypeError("ORDER cumulative quantity must be a nonnegative strict integer")
    return status, cumulative


def resolve_qmt_trade_identity_alias_v1(raw_payload: Mapping[str, Any]) -> str | None:
    """Read one exact raw broker trade identity without manufacturing a fallback."""

    payload = _mapping(raw_payload)
    raw_trade_id = _one_alias(payload, _TRADE_ID_ALIASES, field_name="broker trade identity")
    if raw_trade_id is not None and (
        type(raw_trade_id) is not str or not raw_trade_id or raw_trade_id.strip() != raw_trade_id
    ):
        raise ValueError("TRADE broker identity must be one exact non-empty string alias")
    return raw_trade_id


def build_kernel_order_event_payload_v1(
    *,
    raw_payload: Mapping[str, Any],
    order_event_id: str,
    runtime_id: str,
    algo_instance_id: str,
    parent_intent_id: str,
    strategy_slot_id: str,
    mapping_id: str,
    command_id: str,
    local_vt_orderid: str,
    broker_order_id: str,
    symbol: str,
    side: SideV1 | str,
    requested_quantity: int,
) -> KernelOrderEventPayloadV1:
    payload = _mapping(raw_payload)
    status, cumulative = normalize_qmt_order_callback_observation_v1(payload)
    if type(requested_quantity) is not int or requested_quantity <= 0:
        raise TypeError("requested_quantity must be a positive strict integer")
    if cumulative is not None and cumulative > requested_quantity:
        raise ValueError("ORDER cumulative quantity exceeds requested quantity")
    fact = {
        "order_event_id": order_event_id,
        "runtime_id": runtime_id,
        "algo_instance_id": algo_instance_id,
        "parent_intent_id": parent_intent_id,
        "strategy_slot_id": strategy_slot_id,
        "mapping_id": mapping_id,
        "command_id": command_id,
        "local_vt_orderid": local_vt_orderid,
        "broker_order_id": broker_order_id,
        "symbol": symbol,
        "side": SideV1(side).value,
        "normalized_order_status": status.value,
        "observed_cumulative_filled_quantity": cumulative,
        "observed_remaining_quantity": None if cumulative is None else requested_quantity - cumulative,
        "terminal": status
        in {NormalizedOrderStatusV1.FILLED, NormalizedOrderStatusV1.CANCELLED, NormalizedOrderStatusV1.REJECTED},
        "source_payload_sha256": hash_hex_v1("miniqmt_gateway_source_payload_v1", payload),
    }
    return KernelOrderEventPayloadV1(
        **{
            **fact,
            "side": SideV1(side),
            "normalized_order_status": status,
            "fact_sha256": hash_hex_v1("miniqmt_kernel_order_event_payload_v1", fact),
        }
    )


def build_kernel_trade_event_payload_v1(
    *,
    raw_payload: Mapping[str, Any],
    runtime_id: str,
    algo_instance_id: str,
    parent_intent_id: str,
    strategy_slot_id: str,
    mapping_id: str,
    command_id: str,
    local_vt_orderid: str,
    broker_order_id: str,
    symbol: str,
    side: SideV1 | str,
    trade_quantity: int,
    trade_price_decimal: Any,
    persisted_qmt_strategy_trade: TradeLedgerRecord | None = None,
) -> KernelTradeEventPayloadV1:
    payload = _mapping(raw_payload)
    raw_trade_id = resolve_qmt_trade_identity_alias_v1(payload)
    qmt_trade_id: str | None = None
    if persisted_qmt_strategy_trade is not None:
        if not isinstance(persisted_qmt_strategy_trade, TradeLedgerRecord):
            raise TypeError("persisted_qmt_strategy_trade must be a TradeLedgerRecord readback")
        trade = persisted_qmt_strategy_trade
        expected_qmt_hash = canonical_trade_fact_sha256(
            account_id=trade.account_id,
            trade_date=trade.trade_date,
            trade_id=trade.trade_id,
            qmt_order_id=trade.qmt_order_id,
            symbol=trade.symbol,
            side=trade.side,
            price=trade.price,
            quantity=trade.quantity,
        )
        if trade.canonical_trade_fact_sha256 != expected_qmt_hash:
            raise ValueError("persisted QMT strategy trade fact hash does not close to its canonical payload")
        if trade.first_ingest_source not in {"BROKER_CALLBACK", "BROKER_SNAPSHOT_SYNC"}:
            raise ValueError("persisted QMT strategy trade requires an approved first-ingest source")
        if trade.first_ingested_at is None:
            raise ValueError("persisted QMT strategy trade requires a first-ingested timestamp")
        if (
            trade.intent_id != parent_intent_id
            or trade.qmt_order_id != broker_order_id
            or trade.symbol != symbol
            or trade.side != SideV1(side).value
            or trade.quantity != trade_quantity
            or canonical_decimal_string_v1(
                trade.price, field_name="persisted_qmt_strategy_trade.price", allow_zero=False
            )
            != canonical_decimal_string_v1(trade_price_decimal, field_name="trade_price_decimal", allow_zero=False)
            or trade.amount != trade.price * trade.quantity
        ):
            raise ValueError("persisted QMT strategy trade authority conflicts with callback trade facts")
        qmt_trade_id = trade.trade_id
    if raw_trade_id is None:
        raw_trade_id = qmt_trade_id
    elif qmt_trade_id is not None and raw_trade_id != qmt_trade_id:
        raise ValueError("broker and QMT strategy trade identities conflict")
    if type(raw_trade_id) is not str or not raw_trade_id or raw_trade_id.strip() != raw_trade_id:
        raise ValueError("TRADE requires a real broker or hash-closed QMT strategy trade identity")
    if type(trade_quantity) is not int or trade_quantity <= 0:
        raise TypeError("trade_quantity must be a positive strict integer")
    fact = {
        "trade_id": raw_trade_id,
        "runtime_id": runtime_id,
        "algo_instance_id": algo_instance_id,
        "parent_intent_id": parent_intent_id,
        "strategy_slot_id": strategy_slot_id,
        "mapping_id": mapping_id,
        "command_id": command_id,
        "local_vt_orderid": local_vt_orderid,
        "broker_order_id": broker_order_id,
        "symbol": symbol,
        "side": SideV1(side).value,
        "trade_quantity": trade_quantity,
        "trade_price_decimal": canonical_decimal_string_v1(
            trade_price_decimal, field_name="trade_price_decimal", allow_zero=False
        ),
        "source_payload_sha256": hash_hex_v1("miniqmt_gateway_source_payload_v1", payload),
    }
    return KernelTradeEventPayloadV1(
        **{
            **fact,
            "side": SideV1(side),
            "fact_sha256": hash_hex_v1("miniqmt_kernel_trade_event_payload_v1", fact),
        }
    )


def build_kernel_command_outcome_event_payload_v1(**values: Any) -> KernelCommandOutcomeEventPayloadV1:
    fact = {
        **values,
        "command_type": BrokerCommandTypeV2(values["command_type"]).value,
        "outcome": KernelCommandOutcomeV1(values["outcome"]).value,
    }
    fact.pop("fact_sha256", None)
    return KernelCommandOutcomeEventPayloadV1(
        **{
            **fact,
            "command_type": BrokerCommandTypeV2(values["command_type"]),
            "outcome": KernelCommandOutcomeV1(values["outcome"]),
            "fact_sha256": hash_hex_v1("miniqmt_kernel_command_outcome_payload_v1", fact),
        }
    )


def build_kernel_command_outcome_event_payload_from_durable_v1(
    *,
    mapping: ExecutionCommandChildMappingV1,
    outbox: BrokerCommandOutboxV1,
) -> KernelCommandOutcomeEventPayloadV1:
    """Rebuild the only valid COMMAND_OUTCOME payload from locked durable facts."""

    if not isinstance(mapping, ExecutionCommandChildMappingV1):
        raise TypeError("mapping must be ExecutionCommandChildMappingV1")
    if not isinstance(outbox, BrokerCommandOutboxV1):
        raise TypeError("outbox must be BrokerCommandOutboxV1")
    command = BrokerCommandV2.model_validate_json(
        json.dumps(thaw_json_v1(outbox.payload_json), sort_keys=True, separators=(",", ":"))
    )
    if (
        outbox.mapping_id != mapping.mapping_id
        or outbox.runtime_id != mapping.runtime_id
        or outbox.algo_instance_id != mapping.algo_instance_id
        or outbox.parent_intent_id != mapping.parent_intent_id
        or outbox.local_vt_orderid != mapping.local_vt_orderid
        or command.command_id != outbox.command_id
        or command.command_type is not outbox.command_type
    ):
        raise ValueError("durable outbox, command and mapping identities do not close")

    outcome, authority_sha256 = derive_kernel_command_outcome_authority_v1(outbox)
    broker_order_id = derive_kernel_command_outcome_broker_order_id_v1(
        mapping=mapping,
        outbox=outbox,
        command=command,
        outcome=outcome,
    )
    receipt_payload = {
        "command_id": outbox.command_id,
        "mapping_id": outbox.mapping_id,
        "command_type": outbox.command_type.value,
        "outbox_row_version": outbox.row_version,
        "outbox_status": outbox.status.value,
        "outcome_receipt_sha256": authority_sha256,
        "broker_order_id_or_null": broker_order_id,
    }
    receipt_sha256 = hash_hex_v1("miniqmt_kernel_outbox_outcome_receipt_v1", receipt_payload)
    return build_kernel_command_outcome_event_payload_v1(
        receipt_id=f"mqoutcomercpt_{receipt_sha256}",
        receipt_sha256=receipt_sha256,
        runtime_id=outbox.runtime_id,
        algo_instance_id=outbox.algo_instance_id,
        parent_intent_id=outbox.parent_intent_id,
        strategy_slot_id=mapping.strategy_slot_id,
        mapping_id=outbox.mapping_id,
        command_id=outbox.command_id,
        command_type=outbox.command_type,
        local_vt_orderid=outbox.local_vt_orderid,
        broker_order_id=broker_order_id,
        outcome=outcome,
        outbox_status=outbox.status.value,
        outbox_row_version=outbox.row_version,
        outcome_receipt_sha256=authority_sha256,
        outbox_terminal=outbox.status
        in {
            BrokerCommandOutboxStatusV1.ACKED,
            BrokerCommandOutboxStatusV1.ACKED_REJECTED,
            BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
        },
        order_terminal=outbox.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
        and outcome in {KernelCommandOutcomeV1.REJECTED, KernelCommandOutcomeV1.PRE_CALL_TERMINAL},
    )


def derive_kernel_command_outcome_authority_v1(
    outbox: BrokerCommandOutboxV1,
) -> tuple[KernelCommandOutcomeV1, str]:
    if outbox.status in {BrokerCommandOutboxStatusV1.ACKED, BrokerCommandOutboxStatusV1.ACKED_REJECTED}:
        if outbox.ack_receipt_json is None or outbox.ack_receipt_sha256 != outbox.ack_receipt_json.receipt_sha256:
            raise ValueError("terminal ACK outbox lacks its exact ACK authority")
        return (
            KernelCommandOutcomeV1.ACCEPTED
            if outbox.status is BrokerCommandOutboxStatusV1.ACKED
            else KernelCommandOutcomeV1.REJECTED,
            outbox.ack_receipt_sha256,
        )
    if outbox.status is BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN:
        if outbox.unknown_outcome_receipt is None:
            raise ValueError("OUTCOME_UNKNOWN outbox lacks its exact unknown receipt")
        return KernelCommandOutcomeV1.OUTCOME_UNKNOWN, outbox.unknown_outcome_receipt.receipt_sha256
    if outbox.status is not BrokerCommandOutboxStatusV1.FAILED_TERMINAL:
        raise ValueError("outbox status does not publish a COMMAND_OUTCOME event")
    if outbox.reconcile_receipt is not None:
        outcome = {
            BrokerReconciliationOutcomeV1.UNIQUE_ACCEPTED: KernelCommandOutcomeV1.ACCEPTED,
            BrokerReconciliationOutcomeV1.UNIQUE_REJECTED: KernelCommandOutcomeV1.REJECTED,
            BrokerReconciliationOutcomeV1.NOT_FOUND: KernelCommandOutcomeV1.CONFLICT,
            BrokerReconciliationOutcomeV1.CONFLICT: KernelCommandOutcomeV1.CONFLICT,
        }[outbox.reconcile_receipt.outcome]
        return outcome, outbox.reconcile_receipt.receipt_sha256
    if outbox.non_acceptance_receipt is not None:
        return KernelCommandOutcomeV1.REJECTED, outbox.non_acceptance_receipt.receipt_sha256
    if outbox.unknown_outcome_receipt is not None:
        return KernelCommandOutcomeV1.CONFLICT, outbox.unknown_outcome_receipt.receipt_sha256
    if outbox.last_error_json is None:
        raise ValueError("FAILED_TERMINAL outbox lacks every approved outcome authority")
    evidence = KernelErrorEvidenceV1.model_validate_json(
        json.dumps(thaw_json_v1(outbox.last_error_json), sort_keys=True, separators=(",", ":"))
    )
    return (
        KernelCommandOutcomeV1.PRE_CALL_TERMINAL
        if evidence.broker_called is False
        else KernelCommandOutcomeV1.CONFLICT,
        evidence.evidence_sha256,
    )


def derive_kernel_command_outcome_broker_order_id_v1(
    *,
    mapping: ExecutionCommandChildMappingV1,
    outbox: BrokerCommandOutboxV1,
    command: BrokerCommandV2,
    outcome: KernelCommandOutcomeV1,
) -> str | None:
    if command.command_type is BrokerCommandTypeV2.CANCEL_ORDER:
        if mapping.broker_order_id is None or command.owned_broker_order_id != mapping.broker_order_id:
            raise ValueError("CANCEL outcome does not close to the mapped broker order identity")
        if outbox.broker_order_id not in {None, mapping.broker_order_id}:
            raise ValueError("CANCEL outbox broker identity conflicts with its target mapping")
        return mapping.broker_order_id
    identities = {
        value
        for value in (
            mapping.broker_order_id,
            outbox.broker_order_id,
            None if outbox.ack_receipt_json is None else outbox.ack_receipt_json.broker_order_id,
            None if outbox.reconcile_receipt is None else outbox.reconcile_receipt.broker_order_id,
        )
        if value is not None
    }
    if len(identities) > 1:
        raise ValueError("SUBMIT outcome contains conflicting broker order identities")
    broker_order_id = next(iter(identities), None)
    if outcome is KernelCommandOutcomeV1.ACCEPTED and broker_order_id is None:
        raise ValueError("accepted SUBMIT outcome requires one exact broker order identity")
    return broker_order_id


def build_kernel_order_reconcile_event_payload_v1(
    *,
    ordered_trade_refs: Sequence[KernelTradeFactRefV1],
    requested_quantity: int,
    **values: Any,
) -> KernelOrderReconcileEventPayloadV1:
    if type(requested_quantity) is not int or requested_quantity <= 0:
        raise TypeError("requested_quantity must be a positive strict integer")
    cumulative = values.get("authoritative_cumulative_filled_quantity")
    remaining = values.get("authoritative_remaining_quantity")
    if type(cumulative) is not int or type(remaining) is not int:
        raise TypeError("RECONCILE cumulative and remaining quantities must be strict integers")
    if cumulative < 0 or remaining < 0 or cumulative + remaining != requested_quantity:
        raise ValueError("RECONCILE cumulative and remaining quantities must close to requested quantity")
    refs = tuple(ordered_trade_refs)
    trade_set_sha256 = hash_hex_v1(
        "miniqmt_kernel_order_reconcile_trade_set_v1",
        [item.model_dump(mode="json") for item in refs],
    )
    status = NormalizedOrderStatusV1(values["normalized_order_status"])
    fact = {
        **values,
        "side": SideV1(values["side"]).value,
        "normalized_order_status": status.value,
        "ordered_trade_refs": [item.model_dump(mode="json") for item in refs],
        "trade_set_sha256": trade_set_sha256,
        "terminal": status
        in {NormalizedOrderStatusV1.FILLED, NormalizedOrderStatusV1.CANCELLED, NormalizedOrderStatusV1.REJECTED},
    }
    fact.pop("fact_sha256", None)
    return KernelOrderReconcileEventPayloadV1(
        **{
            **fact,
            "side": SideV1(values["side"]),
            "normalized_order_status": status,
            "ordered_trade_refs": refs,
            "fact_sha256": hash_hex_v1("miniqmt_kernel_order_reconcile_payload_v1", fact),
        }
    )


def strict_readback_kernel_event_payload_v1(event: RuntimeEventEnvelopeV2) -> object:
    model_by_schema = {
        "miniqmt_order_event_v1": KernelOrderEventPayloadV1,
        "miniqmt_trade_fact_v1": KernelTradeEventPayloadV1,
        "miniqmt_command_outcome_v1": KernelCommandOutcomeEventPayloadV1,
        "miniqmt_reconciliation_receipt_v1": KernelOrderReconcileEventPayloadV1,
    }
    try:
        model = model_by_schema[event.payload_schema_version]
    except KeyError as exc:
        raise ValueError("runtime event does not use a strict callback/outcome payload schema") from exc
    return model.model_validate_json(json.dumps(thaw_json_v1(event.payload), sort_keys=True, separators=(",", ":")))


__all__ = [
    "build_kernel_command_outcome_event_payload_v1",
    "build_kernel_command_outcome_event_payload_from_durable_v1",
    "derive_kernel_command_outcome_authority_v1",
    "derive_kernel_command_outcome_broker_order_id_v1",
    "build_kernel_order_event_payload_v1",
    "build_kernel_order_reconcile_event_payload_v1",
    "build_kernel_trade_event_payload_v1",
    "normalize_qmt_order_callback_observation_v1",
    "normalize_qmt_order_status_v1",
    "resolve_qmt_trade_identity_alias_v1",
    "strict_readback_kernel_event_payload_v1",
]
