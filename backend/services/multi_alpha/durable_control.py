"""P0-2 durable multi-alpha control command facade.

HTTP/MCP callers only persist a command here. Remote termination and recovery
materialization are owned by restart-safe workers; this module never sends a
best-effort kill or treats an HTTP result as terminal evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from backend.services.multi_alpha.durable_models import (
    CONTROL_ACTIONS,
    DurableCommandSpec,
    OwnershipToken,
    command_target_key_for,
    control_command_payload,
    make_command_id,
    sha256_identity,
)
from backend.services.multi_alpha.durable_repository import (
    TERMINAL_RUN_STATUSES,
    MultiAlphaDurableRepository,
    MultiAlphaDurableRepositoryError,
)
from backend.services.multi_alpha.durable_wakeup import notify_durable_orchestrator


@dataclass(frozen=True)
class DurableControlResult:
    command: Mapping[str, Any]
    idempotent_identity_confirmed: bool
    capabilities: Mapping[str, Any]


class DurableControlError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class DurableMultiAlphaControlService:
    """Canonical command creation and local command-worker application."""

    def __init__(self, repository: MultiAlphaDurableRepository | None = None) -> None:
        self._repository = repository or MultiAlphaDurableRepository()

    @property
    def repository(self) -> MultiAlphaDurableRepository:
        return self._repository

    def submit(
        self,
        *,
        run_id: str,
        action: str,
        idempotency_key: str,
        requested_by: str,
        request: Mapping[str, Any] | None = None,
        child_id: str | None = None,
        attempt_id: str | None = None,
        scope: Mapping[str, Any] | None = None,
    ) -> DurableControlResult:
        """Persist one exact command; no remote operation is performed here."""

        self._repository.preflight_p0_2_schema(raise_on_error=True)
        normalized_action, normalized_request = _normalize_action(action=action, request=request)
        if normalized_action not in CONTROL_ACTIONS:
            raise DurableControlError(
                "unsupported durable control action",
                reason_code="multi_alpha_invalid_control_command",
                context={"action": normalized_action, "allowed": sorted(CONTROL_ACTIONS)},
            )
        if not str(idempotency_key or "").strip():
            raise DurableControlError(
                "Idempotency-Key is required for durable control",
                reason_code="multi_alpha_idempotency_key_required",
            )
        normalized_scope = dict(scope) if scope is not None else None
        scope_hash = sha256_identity(normalized_scope) if normalized_scope is not None else None
        payload = control_command_payload(
            action=normalized_action,
            run_id=run_id,
            child_id=child_id,
            attempt_id=attempt_id,
            request=normalized_request,
            scope=normalized_scope,
        )
        spec = DurableCommandSpec(
            command_id=make_command_id(run_id, idempotency_key),
            run_id=run_id,
            action=normalized_action,
            target_key=command_target_key_for(
                action=normalized_action,
                run_id=run_id,
                child_id=child_id,
                attempt_id=attempt_id,
            ),
            idempotency_key=idempotency_key,
            payload_hash=sha256_identity(payload),
            request=normalized_request,
            requested_by=requested_by,
            child_id=child_id,
            attempt_id=attempt_id,
            scope=normalized_scope,
            scope_hash=scope_hash,
        )
        command = self._repository.create_or_get_command(spec)
        # Wake only after the durable command transaction has committed.
        notify_durable_orchestrator()
        return DurableControlResult(
            command=command,
            idempotent_identity_confirmed=str(command.get("command_id")) == spec.command_id,
            capabilities=self.capabilities(run_id=run_id, child_id=child_id, attempt_id=attempt_id),
        )

    def capabilities(
        self,
        *,
        run_id: str,
        child_id: str | None = None,
        attempt_id: str | None = None,
    ) -> dict[str, Any]:
        """Return observable state/evidence; it never approves or hides research."""

        run = self._repository.get_run(run_id)
        if run is None:
            raise DurableControlError(
                "durable run does not exist",
                reason_code="multi_alpha_entity_not_found",
                context={"run_id": run_id},
            )
        run_status = str(run.get("status") or "")
        terminal = run_status in TERMINAL_RUN_STATUSES
        child = self._repository.get_child(child_id) if child_id is not None else None
        attempt = self._repository.get_attempt(attempt_id) if attempt_id is not None else None
        if child_id is not None and child is None:
            raise DurableControlError(
                "durable child does not exist",
                reason_code="multi_alpha_entity_not_found",
                context={"run_id": run_id, "child_id": child_id},
            )
        if child is not None and str(child.get("run_id") or "") != run_id:
            raise DurableControlError(
                "durable child does not belong to the requested run",
                reason_code="multi_alpha_entity_not_found",
                context={"run_id": run_id, "child_id": child_id},
            )
        if attempt_id is not None and attempt is None:
            raise DurableControlError(
                "durable attempt does not exist",
                reason_code="multi_alpha_entity_not_found",
                context={"run_id": run_id, "attempt_id": attempt_id},
            )
        if attempt is not None and str(attempt.get("run_id") or "") != run_id:
            raise DurableControlError(
                "durable attempt does not belong to the requested run",
                reason_code="multi_alpha_entity_not_found",
                context={"run_id": run_id, "attempt_id": attempt_id},
            )
        if (
            child_id is not None
            and attempt is not None
            and str(attempt.get("child_id") or "") != child_id
        ):
            raise DurableControlError(
                "durable attempt does not belong to the requested child",
                reason_code="multi_alpha_entity_not_found",
                context={
                    "run_id": run_id,
                    "child_id": child_id,
                    "attempt_id": attempt_id,
                },
            )
        return {
            "run_id": run_id,
            "run_status": run_status,
            "run_terminal": terminal,
            "actions": {
                "pause": {"state": "available" if not terminal else "already_terminal"},
                "resume": {"state": "available" if run_status in {"paused", "pause_requested"} else "state_dependent"},
                "cancel": {"state": "available" if not terminal else "already_terminal"},
                "reconcile": {"state": "available"},
                "attempt_cancel": {
                    "state": "available" if attempt is not None else "requires_exact_attempt",
                    "attempt_status": attempt.get("status") if attempt is not None else None,
                },
                "child_retry": {
                    "state": "available" if child is not None else "requires_exact_child",
                    "child_status": child.get("status") if child is not None else None,
                    "source_run_terminal": terminal,
                },
            },
            "evidence": {
                "execution_identity": run.get("execution_identity_json"),
                "execution_identity_hash": run.get("execution_identity_hash"),
                "execution_identity_evidence": run.get("execution_identity_evidence_json"),
                "child": dict(child) if child is not None else None,
                "attempt": dict(attempt) if attempt is not None else None,
            },
        }

    def apply_one_local_command(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_command_ids: Sequence[str] = (),
        min_recheck_interval_seconds: int = 0,
    ) -> dict[str, Any] | None:
        """Claim and atomically apply one local command intent.

        When the command remains reconciling, ownership is deliberately yielded
        so a restart or another worker can rediscover it from the durable row.
        Re-claiming a reconciling command is throttled to
        min_recheck_interval_seconds so unchanged-state control reconciliation
        does not poll the event table every orchestrator cycle.
        """

        command = self._repository.claim_next_command(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            excluded_command_ids=excluded_command_ids,
            min_recheck_interval_seconds=min_recheck_interval_seconds,
        )
        if command is None:
            return None
        token = _ownership_token(command)
        try:
            if str(command.get("status")) == "applying":
                command = self._repository.apply_control_command_intent(
                    str(command["command_id"]),
                    token=token,
                )
                token = _ownership_token(command)
            if str(command.get("status")) == "reconciling":
                command = self._repository.reconcile_control_command(
                    str(command["command_id"]),
                    token=token,
                )
                if str(command.get("status")) == "reconciling":
                    command = self._repository.yield_command_ownership(
                        str(command["command_id"]),
                        token=_ownership_token(command),
                        phase="control_reconciliation_pending",
                        write_event=False,
                    )
            return command
        except MultiAlphaDurableRepositoryError:
            raise
        except Exception as exc:
            raise DurableControlError(
                "durable control worker failed before remote evidence was inferred",
                reason_code="multi_alpha_control_worker_failed",
                context={"command_id": command.get("command_id"), "error_type": type(exc).__name__, "message": str(exc)},
            ) from exc


def _normalize_action(
    *,
    action: str,
    request: Mapping[str, Any] | None,
) -> tuple[str, dict[str, Any]]:
    normalized_action = str(action or "").strip().lower()
    normalized_request = dict(request or {})
    if normalized_action == "stop":
        normalized_action = "cancel"
        normalized_request["requested_alias"] = "stop"
    return normalized_action, normalized_request


def _ownership_token(row: Mapping[str, Any]) -> OwnershipToken:
    return OwnershipToken(
        owner_id=str(row["owner_id"]),
        fencing_token=int(row["fencing_token"]),
        row_version=int(row["row_version"]),
    )
