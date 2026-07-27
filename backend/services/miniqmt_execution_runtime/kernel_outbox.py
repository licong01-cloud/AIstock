"""K2-D durable broker outbox dispatcher and exact reconciliation.

This module is intentionally shadow-only.  It owns no scheduler and is never
instantiated by the current product runtime; K3 may wire the public seams after
parity acceptance.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from typing import Any, Protocol, Sequence

from backend.services.trading_core.models import OrderSide

from .gateway import MiniQMTGateway, MiniQMTGatewayCancelAck, MiniQMTGatewayOrderAck
from .models import MiniQMTChildOrder, MiniQMTChildOrderStatus
from .plugin_canonical import canonical_decimal_string_v1, hash_hex_v1, thaw_json_v1
from .plugin_contracts import (
    BrokerAckSourceV1,
    BrokerCommandAckReceiptV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    BrokerDispatchAttemptStageV1,
    BrokerDispatchAttemptV1,
    BrokerNonAcceptanceReceiptV1,
    BrokerOutcomeReconciliationReceiptV1,
    BrokerReconciliationOutcomeV1,
    BrokerUncertainStageV1,
    BrokerUnknownOutcomeReceiptV1,
    CommandChildMappingStatusV1,
    ExecutionCommandChildMappingV1,
    EventSourceV2,
    EventTypeV2,
    GatewayCapabilityCatalogV1,
    KernelErrorEvidenceV1,
    RuntimeEventEnvelopeV2,
    canonical_utc_datetime_v1,
    kernel_lease_fence_token_v1,
)


_PRE_CALL_DELAYS_SECONDS = (0, 1, 2, 4, 8)
_RECONCILE_DELAYS_SECONDS = (0, 1, 2, 5, 10, 20, 30, 30, 30, 30)
_REJECTED_ORDER_STATUSES = frozenset({"REJECTED", "FAILED", "INVALID", "CANCEL_REJECTED"})


def _bounded_text(error: BaseException, *, limit: int = 512) -> str:
    try:
        rendered = str(error)
    except Exception as render_error:  # noqa: BLE001
        rendered = f"<{type(error).__name__}; renderer={type(render_error).__name__}>"
    return rendered[:limit]


class KernelOutboxDispatchError(RuntimeError):
    """Fail-loud dispatch failure with stable reason and durable context."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = dict(context)
        super().__init__(f"{message}; reason_code={reason_code}; context={self.context}")


class KernelGatewayPreCallError(KernelOutboxDispatchError):
    """A technical failure proven to occur before a broker method call."""


@dataclass(frozen=True)
class GatewayReconciliationSnapshotV1:
    orders: tuple[dict[str, Any], ...]
    trades: tuple[dict[str, Any], ...]

    def __post_init__(self) -> None:
        if any(type(item) is not dict for item in (*self.orders, *self.trades)):
            raise TypeError("broker reconciliation snapshots must contain exact dict rows")
        object.__setattr__(
            self,
            "orders",
            tuple(_strict_snapshot_row(item, kind="order") for item in self.orders),
        )
        object.__setattr__(
            self,
            "trades",
            tuple(_strict_snapshot_row(item, kind="trade") for item in self.trades),
        )


class KernelOutboxRepositoryV1(Protocol):
    def read_command_identity_chain(self, command_id: str) -> dict[str, Any]: ...

    def claim_outbox_command(self, **values: Any) -> BrokerCommandOutboxV1: ...

    def compare_and_swap_mapping_outbox(self, **values: Any) -> dict[str, Any]: ...

    def append_dispatch_attempt(self, attempt: BrokerDispatchAttemptV1) -> BrokerDispatchAttemptV1: ...

    def read_reconciliation_receipt(
        self, command_id: str, reconcile_attempt: int
    ) -> BrokerOutcomeReconciliationReceiptV1 | None: ...

    def append_reconciliation_receipt(
        self, receipt: BrokerOutcomeReconciliationReceiptV1
    ) -> BrokerOutcomeReconciliationReceiptV1: ...

    def read_callback_watermark(self, *, runtime_id: str) -> str: ...

    def count_matching_callback_events(
        self,
        *,
        command_id: str,
        runtime_id: str,
        callback_watermark_before: str,
        callback_watermark_after: str,
    ) -> int: ...

    def read_runtime_event(self, event_id: str) -> RuntimeEventEnvelopeV2: ...


class KernelGatewayAdapterV1(Protocol):
    def validate_pre_call(
        self,
        *,
        command: BrokerCommandV2,
        mapping: ExecutionCommandChildMappingV1,
        gateway_catalog: GatewayCapabilityCatalogV1,
    ) -> None: ...

    def dispatch(
        self, *, command: BrokerCommandV2, mapping: ExecutionCommandChildMappingV1
    ) -> MiniQMTGatewayOrderAck | MiniQMTGatewayCancelAck: ...

    def reconciliation_snapshot(self, *, runtime_id: str) -> GatewayReconciliationSnapshotV1: ...


class MiniQMTKernelGatewayAdapterV1:
    """Production-capable adapter around the existing MiniQMTGateway protocol.

    Durable callback watermark authority belongs to the repository event sequence,
    never to this transport adapter.
    """

    def __init__(
        self,
        *,
        gateway: MiniQMTGateway,
    ) -> None:
        self._gateway = gateway

    def validate_pre_call(
        self,
        *,
        command: BrokerCommandV2,
        mapping: ExecutionCommandChildMappingV1,
        gateway_catalog: GatewayCapabilityCatalogV1,
    ) -> None:
        _validate_gateway_authority(command=command, mapping=mapping, gateway_catalog=gateway_catalog)
        method_name = (
            "submit_child_order" if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT else "cancel_child_order"
        )
        if not callable(getattr(self._gateway, method_name, None)):
            raise KernelGatewayPreCallError(
                "MINIQMT_COMMAND_OUTBOX_GATEWAY_METHOD_UNAVAILABLE",
                "configured MiniQMT gateway does not expose the required broker method",
                context={"command_id": command.command_id, "command_type": command.command_type.value},
            )

    def dispatch(
        self, *, command: BrokerCommandV2, mapping: ExecutionCommandChildMappingV1
    ) -> MiniQMTGatewayOrderAck | MiniQMTGatewayCancelAck:
        child = _child_order(command=command, mapping=mapping)
        if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            ack = self._gateway.submit_child_order(child)
        else:
            ack = self._gateway.cancel_child_order(child, reason=command.reason_code)
        if not isinstance(ack, (MiniQMTGatewayOrderAck, MiniQMTGatewayCancelAck)):
            return ack
        raw = _strict_snapshot_row(ack.raw, kind="gateway_ack")
        if raw.get("broker_called") is True and type(raw.get("exception_type")) is str:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_GATEWAY_CALL_UNKNOWN",
                "gateway raised after entering the broker call boundary",
                context={
                    "command_id": command.command_id,
                    "exception_type": raw["exception_type"],
                },
            )
        broker_order_id = ack.broker_order_id
        if (
            command.command_type is BrokerCommandTypeV2.CANCEL_ORDER
            and ack.accepted is False
            and broker_order_id == command.owned_broker_order_id
        ):
            # A rejected CANCEL refers to the target order through the command;
            # it did not accept/create a new broker identity for this outbox.
            broker_order_id = None
        ack_type = (
            MiniQMTGatewayOrderAck
            if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
            else MiniQMTGatewayCancelAck
        )
        return ack_type(
            accepted=ack.accepted,
            broker_order_id=broker_order_id,
            message=ack.message,
            raw=raw,
        )

    def reconciliation_snapshot(self, *, runtime_id: str) -> GatewayReconciliationSnapshotV1:
        orders = self._gateway.sync_orders(runtime_id=runtime_id)
        trades = self._gateway.sync_trades(runtime_id=runtime_id)
        if type(orders) is not list or type(trades) is not list:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_SNAPSHOT_INVALID",
                "gateway reconciliation methods must return lists",
                context={"runtime_id": runtime_id},
            )
        return GatewayReconciliationSnapshotV1(
            orders=tuple(_strict_snapshot_row(item, kind="order") for item in orders),
            trades=tuple(_strict_snapshot_row(item, kind="trade") for item in trades),
        )


class KernelOutboxDispatcherV1:
    """Three-phase durable dispatcher for one committed outbox command."""

    def __init__(
        self,
        *,
        repository: KernelOutboxRepositoryV1,
        gateway: KernelGatewayAdapterV1,
        gateway_catalog: GatewayCapabilityCatalogV1,
        lease_owner: str,
        process_incarnation_id: str,
    ) -> None:
        self.repository = repository
        self.gateway = gateway
        self.gateway_catalog = GatewayCapabilityCatalogV1.model_validate(
            gateway_catalog.model_dump(mode="python"), strict=True
        )
        self.lease_owner = _identity(lease_owner, "lease_owner")
        self.process_incarnation_id = _identity(process_incarnation_id, "process_incarnation_id")
        if not self.lease_owner.endswith(f":{self.process_incarnation_id}"):
            raise ValueError("lease_owner must close to the exact process incarnation")

    def dispatch_one(
        self,
        *,
        command_id: str,
        observed_at_utc: Any,
        lease_expires_at_utc: Any,
    ) -> BrokerCommandOutboxV1:
        now = canonical_utc_datetime_v1(observed_at_utc, field_name="observed_at_utc")
        command_id = _identity(command_id, "command_id")
        chain = self.repository.read_command_identity_chain(command_id)
        previous = _strict_chain(chain, command_id=command_id)
        mapping, outbox = previous
        if outbox.status not in {
            BrokerCommandOutboxStatusV1.PENDING,
            BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
        }:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTBOX_NOT_DISPATCHABLE",
                "outbox command is not eligible for a broker call",
                context={"command_id": command_id, "status": outbox.status.value},
            )
        if outbox.next_attempt_at_utc is not None and outbox.next_attempt_at_utc > now:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTBOX_RETRY_NOT_DUE",
                "durable retry time has not arrived",
                context={"command_id": command_id, "next_attempt_at_utc": outbox.next_attempt_at_utc},
            )
        lease_epoch = outbox.lease_epoch + 1
        fence = kernel_lease_fence_token_v1(
            owner_type="OUTBOX_COMMAND",
            owner_id=command_id,
            lease_epoch=lease_epoch,
            lease_owner=self.lease_owner,
        )
        claimed = self.repository.claim_outbox_command(
            command_id=command_id,
            lease_owner=self.lease_owner,
            lease_epoch=lease_epoch,
            lease_fence_token=fence,
            lease_expires_at=lease_expires_at_utc,
            updated_at_utc=now,
            expected_row_version=outbox.row_version,
        )
        command = _command(claimed)
        attempt = BrokerDispatchAttemptV1.create(
            command_id=command_id,
            attempt_count=claimed.attempt_count,
            lease_epoch=claimed.lease_epoch,
            lease_fence_token=fence,
            process_incarnation_id=self.process_incarnation_id,
            stage=BrokerDispatchAttemptStageV1.CLAIMED,
            started_at_utc=now,
            finished_at_utc=None,
            pre_call_complete=False,
            broker_called=None,
            outcome=None,
            error_reason_code=None,
            error_context_sha256=None,
            authority_receipt_sha256=None,
        )
        self.repository.append_dispatch_attempt(attempt)
        try:
            self.gateway.validate_pre_call(command=command, mapping=mapping, gateway_catalog=self.gateway_catalog)
        except KernelGatewayPreCallError as exc:
            return self._close_pre_call_failure(
                mapping=mapping,
                claimed=claimed,
                command=command,
                attempt=attempt,
                now=now,
                error=exc,
            )
        try:
            callback_watermark_before_call = self.repository.read_callback_watermark(runtime_id=command.runtime_id)
            callback_watermark_before_call = _identity(
                callback_watermark_before_call,
                "callback_watermark_before_call",
            )
        except Exception as exc:
            error = KernelGatewayPreCallError(
                "MINIQMT_COMMAND_OUTBOX_CALLBACK_WATERMARK_UNAVAILABLE",
                "callback watermark was unavailable before the broker-call boundary",
                context={
                    "command_id": command.command_id,
                    "exception_type": type(exc).__name__,
                    "message": _bounded_text(exc),
                },
            )
            return self._close_pre_call_failure(
                mapping=mapping,
                claimed=claimed,
                command=command,
                attempt=attempt,
                now=now,
                error=error,
            )
        self.repository.append_dispatch_attempt(
            _attempt_stage(
                attempt, stage=BrokerDispatchAttemptStageV1.PRE_CALL, finished_at_utc=now, pre_call_complete=True
            )
        )
        dispatch_mapping = (
            _mapping_successor(
                mapping,
                command=command,
                status=CommandChildMappingStatusV1.DISPATCHING,
                observed_at_utc=now,
            )
            if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
            else mapping
        )
        dispatching = _outbox_successor(
            claimed,
            command=command,
            status=BrokerCommandOutboxStatusV1.DISPATCHING,
            observed_at_utc=now,
            dispatch_attempt_id=attempt.dispatch_attempt_id,
            callback_watermark_before_call=callback_watermark_before_call,
        )
        self.repository.compare_and_swap_mapping_outbox(
            mapping=dispatch_mapping,
            outbox=dispatching,
            expected_mapping_version=mapping.mapping_version,
            expected_outbox_row_version=claimed.row_version,
            expected_lease_owner=claimed.lease_owner,
            expected_lease_epoch=claimed.lease_epoch,
            expected_lease_fence_token=claimed.lease_fence_token,
        )
        self.repository.append_dispatch_attempt(
            _attempt_stage(
                attempt,
                stage=BrokerDispatchAttemptStageV1.DISPATCHING_COMMITTED,
                finished_at_utc=now,
                pre_call_complete=True,
            )
        )
        try:
            ack = self.gateway.dispatch(command=command, mapping=dispatch_mapping)
        except Exception as exc:
            return self._close_unknown(
                mapping=dispatch_mapping,
                dispatching=dispatching,
                command=command,
                attempt=attempt,
                now=now,
                error=exc,
                callback_watermark=callback_watermark_before_call,
            )
        try:
            return self._close_ack(
                mapping=dispatch_mapping,
                dispatching=dispatching,
                command=command,
                attempt=attempt,
                ack=ack,
                now=now,
            )
        except KernelOutboxDispatchError as exc:
            return self._close_unknown(
                mapping=dispatch_mapping,
                dispatching=dispatching,
                command=command,
                attempt=attempt,
                now=now,
                error=exc,
                callback_watermark=callback_watermark_before_call,
            )

    def _close_pre_call_failure(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        claimed: BrokerCommandOutboxV1,
        command: BrokerCommandV2,
        attempt: BrokerDispatchAttemptV1,
        now: str,
        error: KernelGatewayPreCallError,
    ) -> BrokerCommandOutboxV1:
        terminal = claimed.attempt_count >= len(_PRE_CALL_DELAYS_SECONDS)
        evidence = KernelErrorEvidenceV1.create(
            stage="OUTBOX_PRE_CALL",
            stable_reason_code=error.reason_code,
            exception=error,
            message=str(error),
            retryable=not terminal,
            terminal=terminal,
            broker_called=False,
            primary_context={"command_id": command.command_id, "mapping_id": mapping.mapping_id, **error.context},
            secondary_errors=[],
        )
        status = (
            BrokerCommandOutboxStatusV1.FAILED_TERMINAL if terminal else BrokerCommandOutboxStatusV1.FAILED_RETRYABLE
        )
        next_attempt = None if terminal else _plus_seconds(now, _PRE_CALL_DELAYS_SECONDS[claimed.attempt_count])
        successor = _outbox_successor(
            claimed,
            command=command,
            status=status,
            observed_at_utc=now,
            clear_lease=True,
            dispatch_attempt_id=attempt.dispatch_attempt_id,
            broker_called=False,
            last_error_json=evidence.model_dump(mode="json"),
            next_attempt_at_utc=next_attempt,
            closed_at_utc=now if terminal else None,
        )
        self.repository.append_dispatch_attempt(
            _attempt_stage(
                attempt,
                stage=BrokerDispatchAttemptStageV1.PRE_CALL,
                finished_at_utc=now,
                pre_call_complete=False,
                broker_called=False,
                outcome=status.value,
                error_reason_code=error.reason_code,
                error_context_sha256=evidence.context_sha256,
                authority_receipt_sha256=evidence.evidence_sha256,
            )
        )
        self.repository.compare_and_swap_mapping_outbox(
            mapping=mapping,
            outbox=successor,
            expected_mapping_version=mapping.mapping_version,
            expected_outbox_row_version=claimed.row_version,
            expected_lease_owner=claimed.lease_owner,
            expected_lease_epoch=claimed.lease_epoch,
            expected_lease_fence_token=claimed.lease_fence_token,
        )
        return successor

    def _close_ack(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        dispatching: BrokerCommandOutboxV1,
        command: BrokerCommandV2,
        attempt: BrokerDispatchAttemptV1,
        ack: MiniQMTGatewayOrderAck | MiniQMTGatewayCancelAck,
        now: str,
    ) -> BrokerCommandOutboxV1:
        if not isinstance(ack, (MiniQMTGatewayOrderAck, MiniQMTGatewayCancelAck)):
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_GATEWAY_ACK_INVALID",
                "gateway returned an unsupported ACK carrier",
                context={"command_id": command.command_id, "ack_type": type(ack).__name__},
            )
        if type(ack.accepted) is not bool or type(ack.message) is not str or type(ack.raw) is not dict:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_GATEWAY_ACK_SCHEMA_INVALID",
                "gateway ACK fields must use exact bool, string and dict carriers",
                context={
                    "command_id": command.command_id,
                    "accepted_type": type(ack.accepted).__name__,
                    "message_type": type(ack.message).__name__,
                    "raw_type": type(ack.raw).__name__,
                },
            )
        if ack.raw.get("broker_called") is not True:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_GATEWAY_ACK_BROKER_FACT_MISSING",
                "gateway ACK must explicitly prove broker_called=true",
                context={"command_id": command.command_id},
            )
        broker_order_id = _optional_identity(ack.broker_order_id, "broker_order_id")
        if ack.accepted != (broker_order_id is not None):
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_GATEWAY_ACK_IDENTITY_CONFLICT",
                "gateway ACK acceptance conflicts with broker order identity",
                context={"command_id": command.command_id},
            )
        ack_payload_hash = hash_hex_v1(
            "miniqmt_gateway_ack_payload_v1",
            {"accepted": ack.accepted, "broker_order_id": broker_order_id, "message": str(ack.message), "raw": ack.raw},
        )
        receipt = BrokerCommandAckReceiptV1.create(
            command_id=command.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=dispatching.deterministic_client_order_ref,
            gateway_route_id=self.gateway_catalog.route_id,
            gateway_catalog_sha256=self.gateway_catalog.catalog_sha256,
            source=BrokerAckSourceV1.SYNCHRONOUS_RETURN,
            accepted=ack.accepted,
            broker_order_id=broker_order_id,
            reason_code="MINIQMT_BROKER_ACCEPTED" if ack.accepted else "MINIQMT_BROKER_REJECTED",
            ack_payload_sha256=ack_payload_hash,
            observed_at_utc=now,
        )
        if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT and not ack.accepted:
            final_mapping = _mapping_successor(
                mapping,
                command=command,
                status=CommandChildMappingStatusV1.BROKER_REJECTED,
                observed_at_utc=now,
            )
        else:
            # A synchronous accepted ACK is durable outbox evidence, not an
            # ORDER/TRADE/RECONCILE event. The mapping remains DISPATCHING
            # until atomic ingress attaches exact event lineage.
            final_mapping = mapping
        outbox_status = (
            BrokerCommandOutboxStatusV1.ACKED if ack.accepted else BrokerCommandOutboxStatusV1.ACKED_REJECTED
        )
        final_outbox = _outbox_successor(
            dispatching,
            command=command,
            status=outbox_status,
            observed_at_utc=now,
            clear_lease=True,
            broker_called=True,
            broker_order_id=broker_order_id,
            ack_receipt_json=receipt,
            ack_receipt_sha256=receipt.receipt_sha256,
            closed_at_utc=now,
        )
        self.repository.append_dispatch_attempt(
            _attempt_stage(
                attempt,
                stage=BrokerDispatchAttemptStageV1.GATEWAY_RETURNED,
                finished_at_utc=now,
                pre_call_complete=True,
                broker_called=True,
                outcome=outbox_status.value,
                authority_receipt_sha256=receipt.receipt_sha256,
            )
        )
        self.repository.compare_and_swap_mapping_outbox(
            mapping=final_mapping,
            outbox=final_outbox,
            expected_mapping_version=mapping.mapping_version,
            expected_outbox_row_version=dispatching.row_version,
            expected_lease_owner=dispatching.lease_owner,
            expected_lease_epoch=dispatching.lease_epoch,
            expected_lease_fence_token=dispatching.lease_fence_token,
        )
        self.repository.append_dispatch_attempt(
            _attempt_stage(
                attempt,
                stage=BrokerDispatchAttemptStageV1.COMPLETION_COMMITTED,
                finished_at_utc=now,
                pre_call_complete=True,
                broker_called=True,
                outcome=outbox_status.value,
                authority_receipt_sha256=receipt.receipt_sha256,
            )
        )
        return final_outbox

    def _close_unknown(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        dispatching: BrokerCommandOutboxV1,
        command: BrokerCommandV2,
        attempt: BrokerDispatchAttemptV1,
        now: str,
        error: BaseException,
        callback_watermark: str,
    ) -> BrokerCommandOutboxV1:
        receipt = BrokerUnknownOutcomeReceiptV1.create(
            command_id=command.command_id,
            dispatch_attempt_id=attempt.dispatch_attempt_id,
            mapping_id=mapping.mapping_id,
            lease_fence_token=dispatching.lease_fence_token,
            uncertain_stage=BrokerUncertainStageV1.GATEWAY_RETURN,
            callback_watermark=callback_watermark,
            reason_code="MINIQMT_COMMAND_OUTCOME_GATEWAY_UNKNOWN",
            observed_at_utc=now,
        )
        unknown_mapping = (
            _mapping_successor(
                mapping,
                command=command,
                status=CommandChildMappingStatusV1.OUTCOME_UNKNOWN,
                observed_at_utc=now,
            )
            if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
            else mapping
        )
        unknown = _outbox_successor(
            dispatching,
            command=command,
            status=BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
            observed_at_utc=now,
            clear_lease=True,
            unknown_outcome_receipt=receipt,
        )
        self.repository.append_dispatch_attempt(
            _attempt_stage(
                attempt,
                stage=BrokerDispatchAttemptStageV1.GATEWAY_RETURNED,
                finished_at_utc=now,
                pre_call_complete=True,
                broker_called=None,
                outcome=BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN.value,
                error_reason_code="MINIQMT_COMMAND_OUTCOME_GATEWAY_UNKNOWN",
                error_context_sha256=hash_hex_v1(
                    "miniqmt_gateway_exception_context_v1",
                    {
                        "command_id": command.command_id,
                        "exception_type": type(error).__name__,
                        "message": _safe_text(error),
                    },
                ),
                authority_receipt_sha256=receipt.receipt_sha256,
            )
        )
        self.repository.compare_and_swap_mapping_outbox(
            mapping=unknown_mapping,
            outbox=unknown,
            expected_mapping_version=mapping.mapping_version,
            expected_outbox_row_version=dispatching.row_version,
            expected_lease_owner=dispatching.lease_owner,
            expected_lease_epoch=dispatching.lease_epoch,
            expected_lease_fence_token=dispatching.lease_fence_token,
        )
        return unknown


class KernelOutboxRecoveryV1:
    """Recover expired durable leases without guessing whether broker was called."""

    def __init__(self, *, repository: KernelOutboxRepositoryV1) -> None:
        self.repository = repository

    def recover_stale_one(self, *, command_id: str, observed_at_utc: Any) -> BrokerCommandOutboxV1:
        now = canonical_utc_datetime_v1(observed_at_utc, field_name="observed_at_utc")
        mapping, outbox = _strict_chain(self.repository.read_command_identity_chain(command_id), command_id=command_id)
        if outbox.status not in {
            BrokerCommandOutboxStatusV1.CLAIMED,
            BrokerCommandOutboxStatusV1.DISPATCHING,
        }:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTBOX_STALE_STATE_INVALID",
                "only an expired claimed or dispatching lease can be recovered",
                context={"command_id": command_id, "status": outbox.status.value},
            )
        if outbox.lease_expires_at is None or outbox.lease_expires_at > now:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTBOX_LEASE_NOT_EXPIRED",
                "outbox lease is not yet stale",
                context={"command_id": command_id, "lease_expires_at": outbox.lease_expires_at},
            )
        command = _command(outbox)
        if outbox.status is BrokerCommandOutboxStatusV1.CLAIMED:
            terminal = outbox.attempt_count >= len(_PRE_CALL_DELAYS_SECONDS)
            status = (
                BrokerCommandOutboxStatusV1.FAILED_TERMINAL
                if terminal
                else BrokerCommandOutboxStatusV1.FAILED_RETRYABLE
            )
            error = KernelErrorEvidenceV1.create(
                stage="OUTBOX_STALE_CLAIM_RECOVERY",
                stable_reason_code="MINIQMT_COMMAND_OUTBOX_STALE_CLAIM_RECOVERED",
                exception=KernelOutboxDispatchError(
                    "MINIQMT_COMMAND_OUTBOX_STALE_CLAIM_RECOVERED",
                    "expired pre-call claim recovered without a broker call",
                    context={"command_id": command_id},
                ),
                message="expired pre-call claim recovered without a broker call",
                retryable=not terminal,
                terminal=terminal,
                broker_called=False,
                primary_context={"command_id": command_id, "attempt_count": outbox.attempt_count},
                secondary_errors=[],
            )
            successor = _outbox_successor(
                outbox,
                command=command,
                status=status,
                observed_at_utc=now,
                clear_lease=True,
                broker_called=False,
                last_error_json=error.model_dump(mode="json"),
                next_attempt_at_utc=(
                    None if terminal else _plus_seconds(now, _PRE_CALL_DELAYS_SECONDS[outbox.attempt_count])
                ),
                closed_at_utc=now if terminal else None,
            )
            self._cas(mapping=mapping, outbox=outbox, successor_mapping=mapping, successor_outbox=successor)
            return successor
        watermark = outbox.callback_watermark_before_call
        if watermark is None or outbox.dispatch_attempt_id is None or outbox.lease_fence_token is None:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTBOX_RECOVERY_AUTHORITY_MISSING",
                "stale dispatch cannot be recovered without durable pre-call identity",
                context={"command_id": command_id},
            )
        receipt = BrokerUnknownOutcomeReceiptV1.create(
            command_id=command.command_id,
            dispatch_attempt_id=outbox.dispatch_attempt_id,
            mapping_id=mapping.mapping_id,
            lease_fence_token=outbox.lease_fence_token,
            uncertain_stage=BrokerUncertainStageV1.GATEWAY_CALL,
            callback_watermark=watermark,
            reason_code="MINIQMT_COMMAND_OUTCOME_STALE_DISPATCH_UNKNOWN",
            observed_at_utc=now,
        )
        successor_mapping = (
            _mapping_successor(
                mapping,
                command=command,
                status=CommandChildMappingStatusV1.OUTCOME_UNKNOWN,
                observed_at_utc=now,
            )
            if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
            else mapping
        )
        successor = _outbox_successor(
            outbox,
            command=command,
            status=BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
            observed_at_utc=now,
            clear_lease=True,
            broker_called=None,
            unknown_outcome_receipt=receipt,
        )
        self._cas(
            mapping=mapping,
            outbox=outbox,
            successor_mapping=successor_mapping,
            successor_outbox=successor,
        )
        return successor

    def _cas(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        outbox: BrokerCommandOutboxV1,
        successor_mapping: ExecutionCommandChildMappingV1,
        successor_outbox: BrokerCommandOutboxV1,
    ) -> None:
        self.repository.compare_and_swap_mapping_outbox(
            mapping=successor_mapping,
            outbox=successor_outbox,
            expected_mapping_version=mapping.mapping_version,
            expected_outbox_row_version=outbox.row_version,
            expected_lease_owner=outbox.lease_owner,
            expected_lease_epoch=outbox.lease_epoch,
            expected_lease_fence_token=outbox.lease_fence_token,
        )


class KernelOutboxReconcilerV1:
    """Exact snapshot reconciler that never re-submits an unresolved command."""

    def __init__(
        self,
        *,
        repository: KernelOutboxRepositoryV1,
        gateway: KernelGatewayAdapterV1,
        gateway_catalog: GatewayCapabilityCatalogV1,
    ) -> None:
        self.repository = repository
        self.gateway = gateway
        self.gateway_catalog = GatewayCapabilityCatalogV1.model_validate(
            gateway_catalog.model_dump(mode="python"), strict=True
        )

    def reconcile_one(
        self,
        *,
        command_id: str,
        observed_at_utc: Any,
        eod_event_id: str | None = None,
    ) -> BrokerCommandOutboxV1:
        now = canonical_utc_datetime_v1(observed_at_utc, field_name="observed_at_utc")
        mapping, outbox = _strict_chain(self.repository.read_command_identity_chain(command_id), command_id=command_id)
        if outbox.status not in {BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN, BrokerCommandOutboxStatusV1.RECONCILING}:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_NOT_RECONCILABLE",
                "outbox command is not in an unknown outcome state",
                context={"command_id": command_id, "status": outbox.status.value},
            )
        eod_event = None
        if eod_event_id is not None:
            eod_event = self.repository.read_runtime_event(_identity(eod_event_id, "eod_event_id"))
            if (
                eod_event.runtime_id != outbox.runtime_id
                or eod_event.event_type is not EventTypeV2.EOD
                or eod_event.source is not EventSourceV2.EXCHANGE_SESSION_CLOCK
            ):
                raise KernelOutboxDispatchError(
                    "MINIQMT_COMMAND_OUTCOME_EOD_AUTHORITY_INVALID",
                    "EOD reconciliation requires an exact persisted exchange-session EOD event",
                    context={"command_id": command_id, "event_id": eod_event.event_id},
                )
        if eod_event is None and outbox.next_attempt_at_utc is not None and outbox.next_attempt_at_utc > now:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_RECONCILE_NOT_DUE",
                "durable reconciliation cadence has not reached its next observation time",
                context={"command_id": command_id, "next_attempt_at_utc": outbox.next_attempt_at_utc},
            )
        command = _command(outbox)
        attempt_number = 1 if outbox.reconcile_receipt is None else outbox.reconcile_receipt.reconcile_attempt + 1
        receipt = self.repository.read_reconciliation_receipt(command.command_id, attempt_number)
        if receipt is None:
            snapshot = self.gateway.reconciliation_snapshot(runtime_id=command.runtime_id)
            callback_watermark = self.repository.read_callback_watermark(runtime_id=command.runtime_id)
            order_matches, trade_matches = _snapshot_matches(snapshot=snapshot, outbox=outbox, mapping=mapping)
            outcome, broker_called, broker_order_id = _reconcile_outcome(
                order_matches=order_matches, trade_matches=trade_matches
            )
            receipt = BrokerOutcomeReconciliationReceiptV1.create(
                command_id=command.command_id,
                reconcile_attempt=attempt_number,
                query_criteria_sha256=hash_hex_v1(
                    "miniqmt_command_reconcile_query_v1",
                    {
                        "command_id": command.command_id,
                        "client_ref": outbox.deterministic_client_order_ref,
                        "local_vt_orderid": mapping.local_vt_orderid,
                        "broker_order_id": mapping.broker_order_id,
                        "callback_watermark_before": outbox.callback_watermark_before_call,
                        "callback_watermark_after": callback_watermark,
                    },
                ),
                callback_watermark=callback_watermark,
                ordered_matched_order_ids=tuple(sorted(_snapshot_ids(order_matches, "order"))),
                ordered_matched_trade_ids=tuple(sorted(_snapshot_ids(trade_matches, "trade"))),
                order_snapshot_sha256=hash_hex_v1("miniqmt_reconcile_order_snapshot_v1", list(snapshot.orders)),
                trade_snapshot_sha256=hash_hex_v1("miniqmt_reconcile_trade_snapshot_v1", list(snapshot.trades)),
                outcome=outcome,
                broker_called=broker_called,
                broker_order_id=broker_order_id,
                reason_code=f"MINIQMT_COMMAND_OUTCOME_{outcome.value}",
                observed_at_utc=now,
            )
            self.repository.append_reconciliation_receipt(receipt)
        eod_requires_fresh_attempt = eod_event is not None and receipt.observed_at_utc < eod_event.event_time_utc
        outcome = receipt.outcome
        broker_called = receipt.broker_called
        broker_order_id = receipt.broker_order_id
        if outbox.status is BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN:
            first_reconciling = _outbox_successor(
                outbox,
                command=command,
                status=BrokerCommandOutboxStatusV1.RECONCILING,
                observed_at_utc=now,
                clear_lease=True,
                reconcile_receipt=receipt,
                next_attempt_at_utc=(
                    _plus_seconds(now, _RECONCILE_DELAYS_SECONDS[receipt.reconcile_attempt])
                    if outcome is BrokerReconciliationOutcomeV1.NOT_FOUND
                    else None
                ),
            )
            self._cas(
                mapping=mapping,
                successor_mapping=mapping,
                outbox=outbox,
                successor_outbox=first_reconciling,
            )
            outbox = first_reconciling
            can_prove_retry = (
                outcome is BrokerReconciliationOutcomeV1.NOT_FOUND
                and outbox.unknown_outcome_receipt is not None
                and outbox.unknown_outcome_receipt.callback_watermark != receipt.callback_watermark
                and (
                    (
                        command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
                        and self.gateway_catalog.idempotent_submit_by_client_ref
                    )
                    or (
                        command.command_type is BrokerCommandTypeV2.CANCEL_ORDER
                        and self.gateway_catalog.exact_order_id_cancel
                    )
                )
            )
            if eod_requires_fresh_attempt:
                return first_reconciling
            if outcome is BrokerReconciliationOutcomeV1.NOT_FOUND and not can_prove_retry and eod_event is None:
                return first_reconciling
        if outcome is BrokerReconciliationOutcomeV1.NOT_FOUND:
            return self._close_not_found(
                mapping=mapping,
                outbox=outbox,
                command=command,
                receipt=receipt,
                now=now,
                force_terminal=eod_event is not None and not eod_requires_fresh_attempt,
            )
        if outcome is BrokerReconciliationOutcomeV1.CONFLICT:
            return self._close_terminal_conflict(
                mapping=mapping, outbox=outbox, command=command, receipt=receipt, now=now
            )
        accepted = outcome is BrokerReconciliationOutcomeV1.UNIQUE_ACCEPTED
        ack = BrokerCommandAckReceiptV1.create(
            command_id=command.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=outbox.deterministic_client_order_ref,
            gateway_route_id=self.gateway_catalog.route_id,
            gateway_catalog_sha256=self.gateway_catalog.catalog_sha256,
            source=BrokerAckSourceV1.RECONCILIATION,
            accepted=accepted,
            broker_order_id=broker_order_id if accepted else None,
            reason_code=receipt.reason_code,
            ack_payload_sha256=receipt.receipt_sha256,
            observed_at_utc=now,
        )
        # A reconciliation receipt closes the outbox outcome. It is not a
        # RuntimeEventEnvelopeV2 and cannot mutate mapping event lineage;
        # K2-B's atomic RECONCILE ingress owns that mutation.
        final_mapping = mapping
        final = _outbox_successor(
            outbox,
            command=command,
            status=BrokerCommandOutboxStatusV1.ACKED if accepted else BrokerCommandOutboxStatusV1.ACKED_REJECTED,
            observed_at_utc=now,
            clear_lease=True,
            broker_called=True,
            broker_order_id=broker_order_id if accepted else None,
            ack_receipt_json=ack,
            ack_receipt_sha256=ack.receipt_sha256,
            reconcile_receipt=receipt,
            closed_at_utc=now,
        )
        self._cas(mapping=mapping, successor_mapping=final_mapping, outbox=outbox, successor_outbox=final)
        return final

    def _close_not_found(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        outbox: BrokerCommandOutboxV1,
        command: BrokerCommandV2,
        receipt: BrokerOutcomeReconciliationReceiptV1,
        now: str,
        force_terminal: bool = False,
    ) -> BrokerCommandOutboxV1:
        unknown = outbox.unknown_outcome_receipt
        can_retry = (
            command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
            and self.gateway_catalog.idempotent_submit_by_client_ref
        ) or (command.command_type is BrokerCommandTypeV2.CANCEL_ORDER and self.gateway_catalog.exact_order_id_cancel)
        matching_callback_count = 0
        if can_retry and unknown is not None and unknown.callback_watermark != receipt.callback_watermark:
            matching_callback_count = self.repository.count_matching_callback_events(
                command_id=command.command_id,
                runtime_id=command.runtime_id,
                callback_watermark_before=unknown.callback_watermark,
                callback_watermark_after=receipt.callback_watermark,
            )
        if (
            not force_terminal
            and can_retry
            and unknown is not None
            and unknown.callback_watermark != receipt.callback_watermark
            and matching_callback_count == 0
        ):
            non_acceptance = BrokerNonAcceptanceReceiptV1.create(
                command_id=command.command_id,
                deterministic_client_order_ref=outbox.deterministic_client_order_ref,
                gateway_route_id=self.gateway_catalog.route_id,
                gateway_catalog_sha256=self.gateway_catalog.catalog_sha256,
                query_criteria_sha256=receipt.query_criteria_sha256,
                callback_watermark_before=unknown.callback_watermark,
                callback_watermark_after=receipt.callback_watermark,
                order_snapshot_sha256=receipt.order_snapshot_sha256,
                trade_snapshot_sha256=receipt.trade_snapshot_sha256,
                observed_at_utc=now,
                reason_code="MINIQMT_COMMAND_OUTCOME_EXACT_NON_ACCEPTANCE_PROVEN",
            )
            error = KernelErrorEvidenceV1.create(
                stage="OUTBOX_RECONCILE",
                stable_reason_code="MINIQMT_COMMAND_OUTCOME_EXACT_NON_ACCEPTANCE_PROVEN",
                exception=KernelOutboxDispatchError(
                    "MINIQMT_COMMAND_OUTCOME_EXACT_NON_ACCEPTANCE_PROVEN",
                    "gateway snapshots proved the command was not accepted",
                    context={"command_id": command.command_id},
                ),
                message="gateway snapshots proved the command was not accepted",
                retryable=True,
                terminal=False,
                broker_called=False,
                primary_context={"command_id": command.command_id, "reconcile_attempt": receipt.reconcile_attempt},
                secondary_errors=[],
            )
            retry = _outbox_successor(
                outbox,
                command=command,
                status=BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
                observed_at_utc=now,
                clear_lease=True,
                broker_called=False,
                non_acceptance_receipt=non_acceptance,
                reconcile_receipt=receipt,
                last_error_json=error.model_dump(mode="json"),
                next_attempt_at_utc=_plus_seconds(now, _PRE_CALL_DELAYS_SECONDS[min(outbox.attempt_count, 4)]),
            )
            self._cas(mapping=mapping, successor_mapping=mapping, outbox=outbox, successor_outbox=retry)
            return retry
        if not force_terminal and receipt.reconcile_attempt < len(_RECONCILE_DELAYS_SECONDS):
            reconciling = _outbox_successor(
                outbox,
                command=command,
                status=BrokerCommandOutboxStatusV1.RECONCILING,
                observed_at_utc=now,
                clear_lease=True,
                reconcile_receipt=receipt,
                next_attempt_at_utc=_plus_seconds(now, _RECONCILE_DELAYS_SECONDS[receipt.reconcile_attempt]),
            )
            self._cas(mapping=mapping, successor_mapping=mapping, outbox=outbox, successor_outbox=reconciling)
            return reconciling
        return self._close_terminal_conflict(mapping=mapping, outbox=outbox, command=command, receipt=receipt, now=now)

    def _close_terminal_conflict(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        outbox: BrokerCommandOutboxV1,
        command: BrokerCommandV2,
        receipt: BrokerOutcomeReconciliationReceiptV1,
        now: str,
    ) -> BrokerCommandOutboxV1:
        # Terminalize only the command latest-view. The OUTCOME_UNKNOWN
        # mapping remains the durable economic-risk anchor until a real
        # callback/reconciliation event proves the child outcome.
        terminal_mapping = mapping
        error = KernelErrorEvidenceV1.create(
            stage="OUTBOX_RECONCILE",
            stable_reason_code="MINIQMT_COMMAND_OUTCOME_UNRESOLVED",
            exception=KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_UNRESOLVED",
                "broker outcome could not be uniquely reconstructed",
                context={"command_id": command.command_id, "outcome": receipt.outcome.value},
            ),
            message="broker outcome could not be uniquely reconstructed",
            retryable=False,
            terminal=True,
            broker_called=receipt.broker_called,
            primary_context={
                "command_id": command.command_id,
                "reconcile_attempt": receipt.reconcile_attempt,
                "matched_orders": list(receipt.ordered_matched_order_ids),
                "matched_trades": list(receipt.ordered_matched_trade_ids),
            },
            secondary_errors=[],
        )
        terminal = _outbox_successor(
            outbox,
            command=command,
            status=BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
            observed_at_utc=now,
            clear_lease=True,
            broker_called=receipt.broker_called,
            reconcile_receipt=receipt,
            last_error_json=error.model_dump(mode="json"),
            closed_at_utc=now,
        )
        self._cas(mapping=mapping, successor_mapping=terminal_mapping, outbox=outbox, successor_outbox=terminal)
        return terminal

    def _cas(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        successor_mapping: ExecutionCommandChildMappingV1,
        outbox: BrokerCommandOutboxV1,
        successor_outbox: BrokerCommandOutboxV1,
    ) -> None:
        self.repository.compare_and_swap_mapping_outbox(
            mapping=successor_mapping,
            outbox=successor_outbox,
            expected_mapping_version=mapping.mapping_version,
            expected_outbox_row_version=outbox.row_version,
            expected_lease_owner=outbox.lease_owner,
            expected_lease_epoch=outbox.lease_epoch,
            expected_lease_fence_token=outbox.lease_fence_token,
        )


def _validate_gateway_authority(
    *, command: BrokerCommandV2, mapping: ExecutionCommandChildMappingV1, gateway_catalog: GatewayCapabilityCatalogV1
) -> None:
    if gateway_catalog.quote_source != "B0_QUOTE_V2" or gateway_catalog.gateway_backend != "minqmt_sim":
        raise KernelGatewayPreCallError(
            "MINIQMT_COMMAND_OUTBOX_GATEWAY_AUTHORITY_INVALID",
            "K2 dispatcher requires the approved B0 MiniQMT gateway authority",
            context={"command_id": command.command_id, "route_id": gateway_catalog.route_id},
        )
    if command.order_type not in gateway_catalog.order_types:
        raise KernelGatewayPreCallError(
            "MINIQMT_COMMAND_OUTBOX_ORDER_TYPE_UNSUPPORTED",
            "command order type is absent from the strict gateway catalog",
            context={"command_id": command.command_id, "order_type": command.order_type.value},
        )
    if command.command_type is BrokerCommandTypeV2.CANCEL_ORDER and not gateway_catalog.exact_order_id_cancel:
        raise KernelGatewayPreCallError(
            "MINIQMT_COMMAND_OUTBOX_EXACT_CANCEL_UNSUPPORTED",
            "CANCEL_ORDER requires exact-order-id cancel capability",
            context={"command_id": command.command_id},
        )
    if (
        command.local_vt_orderid != mapping.local_vt_orderid
        or command.runtime_id != mapping.runtime_id
        or command.algo_instance_id != mapping.algo_instance_id
        or command.parent_intent_id != mapping.parent_intent_id
        or command.symbol != mapping.symbol
        or command.side.value != mapping.side.value
        or command.quantity != mapping.requested_quantity
    ):
        raise KernelGatewayPreCallError(
            "MINIQMT_COMMAND_OUTBOX_MAPPING_CONFLICT",
            "command and durable child mapping identities do not close",
            context={"command_id": command.command_id, "mapping_id": mapping.mapping_id},
        )
    if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
        if command.command_id != mapping.command_id or mapping.mapping_status not in {
            CommandChildMappingStatusV1.RESERVED,
            CommandChildMappingStatusV1.DISPATCHING,
            CommandChildMappingStatusV1.OUTCOME_UNKNOWN,
        }:
            raise KernelGatewayPreCallError(
                "MINIQMT_COMMAND_OUTBOX_SUBMIT_MAPPING_STATE_INVALID",
                "SUBMIT command does not own an eligible durable mapping",
                context={"command_id": command.command_id, "mapping_status": mapping.mapping_status.value},
            )
    elif (
        mapping.mapping_status is not CommandChildMappingStatusV1.BROKER_ACCEPTED
        or command.owned_broker_order_id != mapping.broker_order_id
    ):
        raise KernelGatewayPreCallError(
            "MINIQMT_COMMAND_OUTBOX_CANCEL_OWNERSHIP_INVALID",
            "CANCEL command does not close to an accepted durable broker identity",
            context={"command_id": command.command_id, "mapping_id": mapping.mapping_id},
        )


def _child_order(*, command: BrokerCommandV2, mapping: ExecutionCommandChildMappingV1) -> MiniQMTChildOrder:
    return MiniQMTChildOrder(
        child_order_id=mapping.child_order_id,
        runtime_id=command.runtime_id,
        algo_instance_id=command.algo_instance_id,
        parent_intent_id=command.parent_intent_id,
        strategy_slot_id=mapping.strategy_slot_id,
        symbol=command.symbol,
        side=OrderSide(command.side.value),
        quantity=command.quantity,
        price=float(Decimal(command.price_decimal)),
        price_type=11,
        status=MiniQMTChildOrderStatus.SUBMITTING,
        broker_order_id=command.owned_broker_order_id,
        metadata={
            "order_remark": mapping.order_remark,
            "deterministic_client_order_ref": mapping.deterministic_client_order_ref,
            "local_vt_orderid": mapping.local_vt_orderid,
            "command_id": command.command_id,
        },
    )


def _strict_chain(
    chain: dict[str, Any], *, command_id: str
) -> tuple[ExecutionCommandChildMappingV1, BrokerCommandOutboxV1]:
    if type(chain) is not dict or set(chain) != {"mapping", "outbox"}:
        raise KernelOutboxDispatchError(
            "MINIQMT_COMMAND_OUTBOX_CHAIN_INVALID",
            "repository command chain must contain exactly mapping and outbox",
            context={"command_id": command_id},
        )
    mapping = chain["mapping"]
    outbox = chain["outbox"]
    if not isinstance(mapping, ExecutionCommandChildMappingV1) or not isinstance(outbox, BrokerCommandOutboxV1):
        raise KernelOutboxDispatchError(
            "MINIQMT_COMMAND_OUTBOX_CHAIN_INVALID",
            "repository command chain contains non-strict carriers",
            context={"command_id": command_id},
        )
    if outbox.command_id != command_id or mapping.mapping_id != outbox.mapping_id:
        raise KernelOutboxDispatchError(
            "MINIQMT_COMMAND_OUTBOX_CHAIN_IDENTITY_DRIFT",
            "repository command chain identities do not close",
            context={"command_id": command_id},
        )
    return mapping, outbox


def _command(outbox: BrokerCommandOutboxV1) -> BrokerCommandV2:
    return BrokerCommandV2.model_validate_json(
        json.dumps(thaw_json_v1(outbox.payload_json), sort_keys=True, separators=(",", ":"))
    )


def _mapping_successor(
    previous: ExecutionCommandChildMappingV1,
    *,
    command: BrokerCommandV2,
    status: CommandChildMappingStatusV1,
    observed_at_utc: Any,
    broker_order_id: str | None = None,
    source_event_id: str | None = None,
) -> ExecutionCommandChildMappingV1:
    successor = ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id=previous.strategy_slot_id,
        mapping_status=status,
        mapping_version=previous.mapping_version + 1,
        broker_order_id=broker_order_id,
        broker_identity_source_event_id=source_event_id if broker_order_id is not None else None,
        last_order_event_id=source_event_id if broker_order_id is not None else previous.last_order_event_id,
        last_trade_event_id=previous.last_trade_event_id,
        updated_by_event_id=source_event_id if broker_order_id is not None else previous.updated_by_event_id,
        created_at_utc=previous.created_at_utc,
        updated_at_utc=observed_at_utc,
    )
    successor.validate_successor_v1(previous)
    return successor


def _outbox_successor(
    previous: BrokerCommandOutboxV1,
    *,
    command: BrokerCommandV2,
    status: BrokerCommandOutboxStatusV1,
    observed_at_utc: Any,
    clear_lease: bool = False,
    **overrides: Any,
) -> BrokerCommandOutboxV1:
    values = previous.model_dump(mode="python")
    for key in (
        "schema_version",
        "command_id",
        "transition_id",
        "ordinal",
        "runtime_id",
        "algo_instance_id",
        "parent_intent_id",
        "mapping_id",
        "command_type",
        "local_vt_orderid",
        "payload_json",
        "payload_sha256",
        "deterministic_client_order_ref",
        "outbox_row_sha256",
    ):
        values.pop(key, None)
    values.update(status=status, row_version=previous.row_version + 1, updated_at_utc=observed_at_utc)
    if clear_lease:
        values.update(lease_owner=None, lease_fence_token=None, lease_expires_at=None)
    values.update(overrides)
    successor = BrokerCommandOutboxV1.create(command=command, mapping_id=previous.mapping_id, **values)
    successor.validate_successor_v1(previous)
    return successor


def _attempt_stage(
    previous: BrokerDispatchAttemptV1, *, stage: BrokerDispatchAttemptStageV1, **updates: Any
) -> BrokerDispatchAttemptV1:
    payload = previous.model_dump(mode="python")
    payload.pop("schema_version", None)
    payload.pop("dispatch_attempt_id", None)
    payload.pop("attempt_receipt_sha256", None)
    payload.update(stage=stage, **updates)
    return BrokerDispatchAttemptV1.create(**payload)


def _snapshot_matches(
    *, snapshot: GatewayReconciliationSnapshotV1, outbox: BrokerCommandOutboxV1, mapping: ExecutionCommandChildMappingV1
) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
    identities = {
        outbox.deterministic_client_order_ref,
        mapping.order_remark,
        mapping.local_vt_orderid,
        *(tuple([mapping.broker_order_id]) if mapping.broker_order_id else ()),
    }
    keys = (
        "deterministic_client_order_ref",
        "client_order_ref",
        "order_remark",
        "local_vt_orderid",
        "broker_order_id",
        "order_id",
        "qmt_order_id",
    )

    def matches(row: dict[str, Any]) -> bool:
        for key in keys:
            if key not in row or row[key] is None:
                continue
            if _snapshot_identity(row[key], field_name=key) in identities:
                return True
        return False

    return tuple(row for row in snapshot.orders if matches(row)), tuple(row for row in snapshot.trades if matches(row))


def _reconcile_outcome(
    *, order_matches: Sequence[dict[str, Any]], trade_matches: Sequence[dict[str, Any]]
) -> tuple[BrokerReconciliationOutcomeV1, bool | None, str | None]:
    order_ids = _snapshot_ids(order_matches, "order")
    trade_order_ids = {
        value
        for row in trade_matches
        for value in (_row_identity(row, "broker_order_id", "order_id", "qmt_order_id"),)
        if value
    }
    combined = set(order_ids) | trade_order_ids
    if not combined:
        return BrokerReconciliationOutcomeV1.NOT_FOUND, None, None
    if len(combined) != 1:
        return BrokerReconciliationOutcomeV1.CONFLICT, None, None
    broker_order_id = next(iter(combined))
    statuses = {str(row.get("status") or row.get("order_status") or "").strip().upper() for row in order_matches}
    rejected = statuses & _REJECTED_ORDER_STATUSES
    accepted = statuses - _REJECTED_ORDER_STATUSES - {""}
    if rejected and (accepted or trade_matches):
        return BrokerReconciliationOutcomeV1.CONFLICT, None, None
    if statuses and statuses.issubset(_REJECTED_ORDER_STATUSES) and not trade_matches:
        return BrokerReconciliationOutcomeV1.UNIQUE_REJECTED, True, None
    return BrokerReconciliationOutcomeV1.UNIQUE_ACCEPTED, True, broker_order_id


def _snapshot_ids(rows: Sequence[dict[str, Any]], kind: str) -> tuple[str, ...]:
    key_sets = {
        "order": ("broker_order_id", "order_id", "qmt_order_id"),
        "trade": ("trade_id", "broker_trade_id", "qmt_trade_id"),
    }
    result: set[str] = set()
    for row in rows:
        identity = _row_identity(row, *key_sets[kind])
        if identity is None:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_IDENTITY_MISSING",
                f"matched {kind} snapshot row lacks authoritative identity",
                context={"row_sha256": hash_hex_v1("miniqmt_reconcile_row_v1", row)},
            )
        result.add(identity)
    return tuple(sorted(result))


def _row_identity(row: dict[str, Any], *keys: str) -> str | None:
    values = {
        normalized
        for key in keys
        for normalized in (_snapshot_identity(row.get(key), field_name=key),)
        if normalized is not None
    }
    if len(values) > 1:
        raise KernelOutboxDispatchError(
            "MINIQMT_COMMAND_OUTCOME_ALIAS_CONFLICT",
            "one broker snapshot row carries conflicting identity aliases",
            context={"keys": list(keys), "values": sorted(values)},
        )
    return next(iter(values), None)


def _strict_snapshot_row(value: Any, *, kind: str) -> dict[str, Any]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        raise KernelOutboxDispatchError(
            "MINIQMT_COMMAND_OUTCOME_SNAPSHOT_INVALID",
            f"{kind} snapshot row must be a strict string-keyed dict",
            context={"row_type": type(value).__name__},
        )
    normalized = {key: _canonical_snapshot_value(member, field_path=key) for key, member in value.items()}
    hash_hex_v1("miniqmt_reconcile_snapshot_row_v1", normalized)
    return normalized


def _canonical_snapshot_value(value: Any, *, field_path: str) -> Any:
    if value is None or type(value) in (bool, int, str):
        return value
    if isinstance(value, float):
        return canonical_decimal_string_v1(
            Decimal(str(value)),
            field_name=field_path,
            allow_zero=True,
        )
    if isinstance(value, Decimal):
        return canonical_decimal_string_v1(value, field_name=field_path, allow_zero=True)
    if isinstance(value, datetime):
        return canonical_utc_datetime_v1(value, field_name=field_path)
    if type(value) is list:
        return [
            _canonical_snapshot_value(member, field_path=f"{field_path}[{index}]") for index, member in enumerate(value)
        ]
    if type(value) is dict and all(type(key) is str for key in value):
        return {
            key: _canonical_snapshot_value(member, field_path=f"{field_path}.{key}") for key, member in value.items()
        }
    raise KernelOutboxDispatchError(
        "MINIQMT_COMMAND_OUTCOME_SNAPSHOT_VALUE_INVALID",
        "broker snapshot contains a non-canonical value",
        context={"field_path": field_path, "value_type": type(value).__name__},
    )


def _snapshot_identity(value: Any, *, field_name: str) -> str | None:
    if value is None:
        return None
    if type(value) is int:
        if value < 0:
            raise KernelOutboxDispatchError(
                "MINIQMT_COMMAND_OUTCOME_IDENTITY_INVALID",
                "numeric broker identity cannot be negative",
                context={"field": field_name, "value": value},
            )
        return str(value)
    if type(value) is str and value.strip() and value == value.strip():
        return value
    raise KernelOutboxDispatchError(
        "MINIQMT_COMMAND_OUTCOME_IDENTITY_INVALID",
        "broker identity must be a trim-stable string or non-negative integer",
        context={"field": field_name, "value_type": type(value).__name__},
    )


def _identity(value: Any, field_name: str) -> str:
    if type(value) is not str or not value.strip() or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty trim-stable strict string")
    return value


def _optional_identity(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _identity(value, field_name)


def _plus_seconds(value: str, seconds: int) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return canonical_utc_datetime_v1(parsed + timedelta(seconds=seconds), field_name="next_attempt_at_utc")


def _safe_text(error: BaseException) -> str:
    try:
        return str(error)[:512]
    except Exception as render_error:
        return f"<{type(error).__name__}: renderer failed with {type(render_error).__name__}>"
