"""Restart-safe typed cancellation delivery for durable multi-alpha attempts."""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import asdict
from typing import Any, Mapping, Sequence

from backend.services.multi_alpha.durable_models import (
    OwnershipToken,
    kill_intent_hash_for,
    process_identity_hash_for,
)
from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepository
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceSubmissionInspection,
    QEWorkspaceSubmissionTransportError,
    QEWorkspaceTypedKillError,
    QEWorkspaceTypedKillReceipt,
)


logger = logging.getLogger(__name__)

_TERMINAL_REMOTE_STATUSES = frozenset({"completed", "failed", "cancelled"})
_DELIVERED_TYPED_RECEIPT_STATUSES = frozenset({"cancelled", "completed"})
_RECONCILING_TYPED_RECEIPT_STATUSES = frozenset({"requested", "signal_sent", "reconciling", "failed"})
_NEXT_GENERATION_TERMINAL_REASONS = frozenset(
    {"kill_execution_incarnation_mismatch", "kill_process_started_race"},
)


class DurableCancellationDeliveryError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class DurableCancellationDeliveryWorker:
    """Claims a durable delivery row and obtains one typed QE-node receipt.

    It never invokes the legacy PID-only endpoint.  A transport failure or a
    receipt with incomplete execution evidence is persisted as visible
    reconciliation state; neither is reinterpreted as a terminal experiment or a
    reason to remove a research direction.
    """

    def __init__(
        self,
        *,
        repository: MultiAlphaDurableRepository | None = None,
        workspace_client_factory: Any = QEWorkspaceClient.for_node,
        retry_seconds: int = 5,
    ) -> None:
        if retry_seconds < 0:
            raise DurableCancellationDeliveryError(
                "cancellation retry interval must be non-negative",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"retry_seconds": retry_seconds},
            )
        self._repository = repository or MultiAlphaDurableRepository()
        self._workspace_client_factory = workspace_client_factory
        self._retry_seconds = int(retry_seconds)

        self._last_claimed_delivery_id: str | None = None

    @property
    def last_claimed_delivery_id(self) -> str | None:
        return self._last_claimed_delivery_id

    async def deliver_once(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_delivery_ids: Sequence[str] = (),
    ) -> bool:
        delivery = await asyncio.to_thread(
            self._repository.claim_next_cancel_delivery,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            excluded_delivery_ids=excluded_delivery_ids,
        )
        if delivery is None:
            return False
        token = _ownership_token(delivery)
        delivery_id = str(delivery["delivery_id"])
        self._last_claimed_delivery_id = delivery_id
        client: Any | None = None
        try:
            await asyncio.to_thread(
                self._repository.mark_run_cancelling_from_delivery,
                delivery_id,
                token=token,
            )
            client = self._workspace_client_factory(str(delivery["node_id"]))
            inspection = await client.inspect_loop_submission(
                str(delivery["qe_task_id"]),
                str(delivery["qe_loop_id"]),
                submission_intent_hash=str(delivery["submission_intent_hash"]),
            )
            existing_receipt = _mapping(delivery.get("kill_receipt_json"))
            if existing_receipt.get("outcome"):
                await self._reconcile_existing_typed_receipt(
                    delivery=delivery,
                    token=token,
                    inspection=inspection,
                )
                return True
            if existing_receipt.get("status"):
                if _may_advance_generation(existing_receipt, inspection):
                    expected_identity = (
                        inspection.process_identity if inspection.status in {"started", "running"} else None
                    )
                    updated = await asyncio.to_thread(
                        self._repository.advance_cancel_delivery_generation,
                        delivery_id,
                        token=token,
                        expected_process_identity=expected_identity,
                        expected_process_identity_hash=(
                            process_identity_hash_for(expected_identity)
                            if expected_identity is not None
                            else None
                        ),
                        remote_status=str(inspection.status),
                    )
                    token = _ownership_token(updated)
                    delivery = updated
                else:
                    await self._reconcile_existing_typed_receipt(
                        delivery=delivery,
                        token=token,
                        inspection=inspection,
                    )
                    return True
            token, delivery = await self._record_submission_inspection(
                delivery=delivery,
                token=token,
                inspection=inspection,
            )
            current_status = str(delivery["status"])
            if current_status == "succeeded":
                return True

            expected_identity = _mapping_or_none(delivery.get("expected_process_identity_json"))
            if inspection.status == "reserved":
                expected_identity = None
                expected_phase = "pre_process_start"
            elif inspection.status in {"started", "running"}:
                if expected_identity is None:
                    await self._keep_reconciling(
                        delivery=delivery,
                        token=token,
                        phase="typed_kill_execution_identity_unavailable",
                        reason_code="kill_execution_evidence_unavailable",
                        error={
                            "reason_code": "kill_execution_evidence_unavailable",
                            "message": "QE submission receipt does not expose pid/pgid/start_time_ticks",
                            "context": _inspection_evidence(inspection),
                        },
                    )
                    return True
                expected_phase = None
            else:
                await self._keep_reconciling(
                    delivery=delivery,
                    token=token,
                    phase="typed_kill_remote_status_unmapped",
                    reason_code="kill_remote_status_unmapped",
                    error={
                        "reason_code": "kill_remote_status_unmapped",
                        "message": "QE submission receipt status is not mapped for typed cancellation",
                        "context": _inspection_evidence(inspection),
                    },
                )
                return True

            kill_hash = str(delivery.get("kill_intent_hash") or "")
            if not kill_hash:
                raise DurableCancellationDeliveryError(
                    "durable cancellation intent was not persisted before remote delivery",
                    reason_code="multi_alpha_cancel_intent_missing",
                    context={"delivery_id": delivery_id},
                )
            typed_receipt = await client.kill_loop_typed(
                str(delivery["qe_task_id"]),
                str(delivery["qe_loop_id"]),
                command_id=str(delivery["originating_command_id"]),
                kill_intent_generation=int(delivery["kill_intent_generation"]),
                kill_intent_hash=kill_hash,
                expected_submission_intent_hash=str(delivery["submission_intent_hash"]),
                expected_process_identity=expected_identity,
                expected_phase=expected_phase,
            )
            await self._persist_typed_receipt(
                delivery=delivery,
                token=token,
                receipt=typed_receipt,
            )
            return True
        except QEWorkspaceSubmissionTransportError as exc:
            await self._keep_reconciling(
                delivery=delivery,
                token=token,
                phase="typed_kill_submission_inspection_transport_unknown",
                reason_code=exc.reason_code,
                error=_exception_payload(exc),
            )
            return True
        except QEWorkspaceTypedKillError as exc:
            await self._keep_reconciling(
                delivery=delivery,
                token=token,
                phase="typed_kill_delivery_unresolved",
                reason_code=exc.reason_code,
                error=_exception_payload(exc),
            )
            return True
        except Exception as exc:
            await self._keep_reconciling(
                delivery=delivery,
                token=token,
                phase="typed_kill_worker_error",
                reason_code="multi_alpha_typed_kill_worker_error",
                error=_exception_payload(exc),
            )
            logger.exception("durable typed cancel delivery failed: delivery_id=%s", delivery_id)
            return True
        finally:
            if client is not None:
                await _close_client(client)

    async def _reconcile_existing_typed_receipt(
        self,
        *,
        delivery: Mapping[str, Any],
        token: OwnershipToken,
        inspection: QEWorkspaceSubmissionInspection,
    ) -> None:
        """Observe a prior typed receipt without manufacturing a new signal."""

        delivery_id = str(delivery["delivery_id"])
        status = str(inspection.status)
        if status == "not_reserved" or status in _TERMINAL_REMOTE_STATUSES:
            await asyncio.to_thread(
                self._repository.transition_cancel_delivery_with_event,
                delivery_id,
                token=token,
                expected_statuses=(str(delivery["status"]),),
                next_status="succeeded",
                remote_status=status,
                kill_receipt=None,
                reason_code="typed_kill_receipt_reconciled_terminal",
            )
            return
        updated = await asyncio.to_thread(
            self._repository.record_cancel_delivery_evidence,
            delivery_id,
            token=token,
            expected_statuses=(str(delivery["status"]),),
            phase="typed_kill_receipt_reconciled_active",
            remote_status=status,
            kill_receipt=None,
            reason_code="typed_kill_receipt_reconciled_active",
            persist_kill_intent=False,
        )
        await self._yield_reconciling(
            delivery_id=delivery_id,
            token=_ownership_token(updated),
            phase="typed_kill_receipt_reconciled_active",
        )

    async def _record_submission_inspection(
        self,
        *,
        delivery: Mapping[str, Any],
        token: OwnershipToken,
        inspection: QEWorkspaceSubmissionInspection,
    ) -> tuple[OwnershipToken, Mapping[str, Any]]:
        delivery_id = str(delivery["delivery_id"])
        evidence = {"source": "qe_submission_inspection", **_inspection_evidence(inspection)}
        status = str(inspection.status)
        if status == "not_reserved":
            updated = await asyncio.to_thread(
                self._repository.transition_cancel_delivery_with_event,
                delivery_id,
                token=token,
                expected_statuses=(str(delivery["status"]),),
                next_status="succeeded",
                remote_status=status,
                kill_receipt=evidence,
                reason_code="kill_target_not_reserved",
            )
            return _ownership_token(updated), updated
        if status in _TERMINAL_REMOTE_STATUSES:
            updated = await asyncio.to_thread(
                self._repository.transition_cancel_delivery_with_event,
                delivery_id,
                token=token,
                expected_statuses=(str(delivery["status"]),),
                next_status="succeeded",
                remote_status=status,
                kill_receipt=evidence,
                reason_code="kill_target_already_terminal",
            )
            return _ownership_token(updated), updated
        if status == "reserved" and delivery.get("expected_process_identity_hash") is not None:
            raise DurableCancellationDeliveryError(
                "remote submission regressed from a recorded process incarnation to reserved",
                reason_code="kill_execution_incarnation_mismatch",
                context={
                    "delivery_id": delivery_id,
                    "inspection": _inspection_evidence(inspection),
                    "recorded_process_identity_hash": delivery.get("expected_process_identity_hash"),
                },
            )
        if status == "reserved":
            expected_identity = None
            expected_identity_hash = None
        elif status in {"started", "running"}:
            expected_identity = inspection.process_identity
            expected_identity_hash = (
                process_identity_hash_for(expected_identity)
                if expected_identity is not None
                else None
            )
        else:
            expected_identity = None
            expected_identity_hash = None
        if status in {"started", "running"} and expected_identity is None:
            updated = await asyncio.to_thread(
                self._repository.record_cancel_delivery_evidence,
                delivery_id,
                token=token,
                expected_statuses=(str(delivery["status"]),),
                phase="typed_kill_submission_identity_unavailable",
                remote_status=status,
                kill_receipt=evidence,
                reason_code="kill_execution_evidence_unavailable",
                persist_kill_intent=False,
            )
            return _ownership_token(updated), updated
        generation = int(delivery.get("kill_intent_generation") or 1)
        kill_hash = kill_intent_hash_for(
            kill_target_key=str(delivery["kill_target_key"]),
            process_identity_hash=expected_identity_hash,
            generation=generation,
        )
        updated = await asyncio.to_thread(
            self._repository.record_cancel_delivery_evidence,
            delivery_id,
            token=token,
            expected_statuses=(str(delivery["status"]),),
            phase="typed_kill_submission_inspected",
            remote_status=status,
            kill_receipt=evidence,
            expected_process_identity=expected_identity,
            expected_process_identity_hash=expected_identity_hash,
            kill_intent_generation=generation,
            kill_intent_hash=kill_hash,
            reason_code="typed_kill_submission_inspected",
        )
        return _ownership_token(updated), updated

    async def _persist_typed_receipt(
        self,
        *,
        delivery: Mapping[str, Any],
        token: OwnershipToken,
        receipt: QEWorkspaceTypedKillReceipt,
    ) -> None:
        payload = asdict(receipt)
        receipt_status = receipt.status
        delivery_id = str(delivery["delivery_id"])
        status = str(delivery["status"])
        if receipt_status in _DELIVERED_TYPED_RECEIPT_STATUSES:
            await asyncio.to_thread(
                self._repository.transition_cancel_delivery_with_event,
                delivery_id,
                token=token,
                expected_statuses=(status,),
                next_status="succeeded",
                remote_status=receipt_status,
                kill_receipt=payload,
                expected_process_identity=receipt.expected_process_identity,
                expected_process_identity_hash=(
                    process_identity_hash_for(receipt.expected_process_identity)
                    if receipt.expected_process_identity is not None
                    else None
                ),
                kill_intent_generation=receipt.kill_intent_generation,
                kill_intent_hash=receipt.kill_intent_hash,
                reason_code=receipt.terminal_reason or "typed_kill_terminal_receipt",
            )
            return
        if receipt_status not in _RECONCILING_TYPED_RECEIPT_STATUSES:
            raise DurableCancellationDeliveryError(
                "typed QE kill receipt has an unrecognized status",
                reason_code="multi_alpha_typed_kill_receipt_invalid",
                context={"delivery_id": delivery_id, "status": receipt_status},
            )
        receipt_reason = receipt.terminal_reason or "typed_kill_receipt_reconciling"
        receipt_message = _receipt_message(receipt)
        if status == "sending":
            updated = await asyncio.to_thread(
                self._repository.transition_cancel_delivery_with_event,
                delivery_id,
                token=token,
                expected_statuses=("sending",),
                next_status="reconciling",
                remote_status=receipt_status,
                kill_receipt=payload,
                expected_process_identity=receipt.expected_process_identity,
                expected_process_identity_hash=(
                    process_identity_hash_for(receipt.expected_process_identity)
                    if receipt.expected_process_identity is not None
                    else None
                ),
                kill_intent_generation=receipt.kill_intent_generation,
                kill_intent_hash=receipt.kill_intent_hash,
                error={
                    "reason_code": receipt_reason,
                    "message": receipt_message,
                    "receipt": payload,
                },
                next_delivery_seconds=self._retry_seconds,
                reason_code=receipt_reason,
            )
            await self._yield_reconciling(
                delivery_id=delivery_id,
                token=_ownership_token(updated),
                phase="typed_kill_receipt_reconciling",
            )
            return
        updated = await asyncio.to_thread(
            self._repository.record_cancel_delivery_evidence,
            delivery_id,
            token=token,
            expected_statuses=("reconciling",),
            phase="typed_kill_receipt_reconciling",
            remote_status=receipt_status,
            kill_receipt=payload,
            expected_process_identity=receipt.expected_process_identity,
            expected_process_identity_hash=(
                process_identity_hash_for(receipt.expected_process_identity)
                if receipt.expected_process_identity is not None
                else None
            ),
            kill_intent_generation=receipt.kill_intent_generation,
            kill_intent_hash=receipt.kill_intent_hash,
            error={
                "reason_code": receipt_reason,
                "message": receipt_message,
                "receipt": payload,
            },
            next_delivery_seconds=self._retry_seconds,
            reason_code=receipt_reason,
        )
        await self._yield_reconciling(
            delivery_id=delivery_id,
            token=_ownership_token(updated),
            phase="typed_kill_receipt_reconciling",
        )

    async def _keep_reconciling(
        self,
        *,
        delivery: Mapping[str, Any],
        token: OwnershipToken,
        phase: str,
        reason_code: str,
        error: Mapping[str, Any],
    ) -> None:
        delivery_id = str(delivery["delivery_id"])
        status = str(delivery["status"])
        if status == "sending":
            updated = await asyncio.to_thread(
                self._repository.transition_cancel_delivery_with_event,
                delivery_id,
                token=token,
                expected_statuses=("sending",),
                next_status="reconciling",
                remote_status=reason_code,
                error=error,
                next_delivery_seconds=self._retry_seconds,
                reason_code=reason_code,
            )
        else:
            updated = await asyncio.to_thread(
                self._repository.record_cancel_delivery_evidence,
                delivery_id,
                token=token,
                expected_statuses=("reconciling",),
                phase=phase,
                remote_status=reason_code,
                error=error,
                next_delivery_seconds=self._retry_seconds,
                reason_code=reason_code,
                persist_kill_intent=bool(delivery.get("kill_intent_hash")),
            )
        await self._yield_reconciling(
            delivery_id=delivery_id,
            token=_ownership_token(updated),
            phase=phase,
        )

    async def _yield_reconciling(
        self,
        *,
        delivery_id: str,
        token: OwnershipToken,
        phase: str,
    ) -> None:
        await asyncio.to_thread(
            self._repository.yield_cancel_delivery_ownership,
            delivery_id,
            token=token,
            phase=phase,
        )


def _ownership_token(row: Mapping[str, Any]) -> OwnershipToken:
    return OwnershipToken(
        owner_id=str(row["owner_id"]),
        fencing_token=int(row["fencing_token"]),
        row_version=int(row["row_version"]),
    )


def _inspection_evidence(inspection: QEWorkspaceSubmissionInspection) -> dict[str, Any]:
    return {
        "schema_version": inspection.schema_version,
        "task_id": inspection.task_id,
        "loop_id": inspection.loop_id,
        "status": inspection.status,
        "submission_intent_hash": inspection.submission_intent_hash,
        "request_digest": inspection.request_digest,
        "pid": inspection.pid,
        "process_identity": inspection.process_identity,
        "created_at": inspection.created_at,
        "updated_at": inspection.updated_at,
        "started_at": inspection.started_at,
        "running_at": inspection.running_at,
        "finished_at": inspection.finished_at,
    }


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mapping_or_none(value: Any) -> dict[str, int] | None:
    if not isinstance(value, Mapping):
        return None
    return {str(key): int(item) for key, item in value.items()}


def _may_advance_generation(
    receipt: Mapping[str, Any],
    inspection: QEWorkspaceSubmissionInspection,
) -> bool:
    """A new generation is legal only for a proven no-signal spawn/incarnation race."""

    return (
        str(receipt.get("status") or "") == "failed"
        and receipt.get("signal_sent") is False
        and str(receipt.get("terminal_reason") or "") in _NEXT_GENERATION_TERMINAL_REASONS
        and str(inspection.status) in {"reserved", "started", "running"}
        and (
            inspection.status == "reserved"
            or inspection.process_identity is not None
        )
    )


def _receipt_message(receipt: QEWorkspaceTypedKillReceipt) -> str:
    if receipt.error and isinstance(receipt.error.get("message"), str):
        return str(receipt.error["message"])
    if receipt.terminal_reason:
        return receipt.terminal_reason
    return f"typed kill receipt remains {receipt.status}"


def _exception_payload(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, (QEWorkspaceSubmissionTransportError, QEWorkspaceTypedKillError)):
        return {
            "reason_code": exc.reason_code,
            "message": str(exc),
            "context": dict(exc.context),
        }
    if isinstance(exc, DurableCancellationDeliveryError):
        return {
            "reason_code": exc.reason_code,
            "message": str(exc),
            "context": dict(exc.context),
        }
    return {
        "reason_code": f"{type(exc).__module__}.{type(exc).__name__}",
        "message": str(exc),
        "context": {},
    }


async def _close_client(client: Any) -> None:
    close = getattr(client, "close", None)
    if close is None:
        return
    try:
        result = close()
        if inspect.isawaitable(result):
            await result
    except Exception:
        # The durable delivery outcome has already been persisted.  Closing a
        # transient HTTP client cannot change that fact; retain a server log.
        logger.warning("failed to close QE workspace client after typed kill", exc_info=True)
