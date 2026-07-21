"""Deterministic P0-2 recovery planning for QE multi-alpha children.

The planner is deliberately pure: it freezes what should be recovered and
what evidence is missing. It never substitutes a current node, dataset,
materializer, or retry mode. Execution is handled by the repository/adapter
after the resulting scope has been persisted.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from backend.services.multi_alpha.durable_execution_adapter import (
    QEWorkspacePredBacktestAdapter,
)
from backend.services.multi_alpha.durable_repository import (
    MultiAlphaDurableRepository,
)

from backend.services.multi_alpha.durable_models import (
    ATTEMPT_RETRY_MODES,
    CHILD_EXECUTION_DISPOSITIONS,
    DurableAttemptSpec,
    DurableChildSpec,
    DurableContractError,
    DurableRunSpec,
    OwnershipToken,
    RetryMode,
    artifact_manifest_hash_for,
    durable_run_request_payload,
    make_command_id,
    make_attempt_id,
    make_child_id,
    make_successor_run_id,
    request_hash_for,
    sha256_identity,
)
from backend.services.multi_alpha.durable_identity import legacy_execution_identity_evidence


RECOVERY_PLAN_SCHEMA_VERSION = "multi_alpha_recovery_scope_v1"
RECOVERY_TERMINAL_CHILD_STATUSES = frozenset(
    {"succeeded", "not_computable", "not_recovered", "failed", "cancelled"}
)


@dataclass(frozen=True)
class RecoveryPlanEntry:
    source_child_id: str
    child_key: str
    child_kind: str
    source_status: str
    disposition: str
    source_attempt_id: str | None
    source_attempt_status: str | None
    source_lineage: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.disposition not in CHILD_EXECUTION_DISPOSITIONS:
            raise DurableContractError(
                "recovery plan has unsupported execution disposition",
                reason_code="multi_alpha_invalid_recovery_plan",
                context={"disposition": self.disposition, "child_key": self.child_key},
            )
        if self.disposition == "preserve_unavailable" and self.source_status == "succeeded":
            raise DurableContractError(
                "successful source child cannot be silently preserved as unavailable",
                reason_code="multi_alpha_invalid_recovery_plan",
                context={"child_key": self.child_key},
            )

    def as_scope_row(self) -> dict[str, Any]:
        return {
            "source_child_id": self.source_child_id,
            "child_key": self.child_key,
            "child_kind": self.child_kind,
            "source_status": self.source_status,
            "disposition": self.disposition,
            "source_attempt_id": self.source_attempt_id,
            "source_attempt_status": self.source_attempt_status,
            "source_lineage": dict(self.source_lineage),
            "source_lineage_hash": sha256_identity(dict(self.source_lineage)),
        }


@dataclass(frozen=True)
class RecoveryPlan:
    source_run_id: str
    command_id: str
    target_child_id: str
    target_child_key: str
    retry_mode: str
    entries: tuple[RecoveryPlanEntry, ...]
    scope: Mapping[str, Any]
    scope_hash: str
    successor_run_id: str

    def __post_init__(self) -> None:
        if self.retry_mode not in ATTEMPT_RETRY_MODES - {RetryMode.INITIAL.value}:
            raise DurableContractError(
                "recovery plan requires a non-initial retry mode",
                reason_code="multi_alpha_invalid_recovery_plan",
            )
        if self.scope_hash != sha256_identity(dict(self.scope)):
            raise DurableContractError(
                "recovery plan scope hash does not match canonical scope",
                reason_code="multi_alpha_identity_hash_mismatch",
                context={"expected": sha256_identity(dict(self.scope)), "actual": self.scope_hash},
            )
        expected_successor = make_successor_run_id(
            source_run_id=self.source_run_id,
            command_id=self.command_id,
            scope_hash=self.scope_hash,
        )
        if self.successor_run_id != expected_successor:
            raise DurableContractError(
                "recovery successor run identity does not match frozen scope",
                reason_code="multi_alpha_identity_hash_mismatch",
                context={"expected": expected_successor, "actual": self.successor_run_id},
            )

    @property
    def entries_by_disposition(self) -> dict[str, tuple[RecoveryPlanEntry, ...]]:
        result: dict[str, list[RecoveryPlanEntry]] = {}
        for entry in self.entries:
            result.setdefault(entry.disposition, []).append(entry)
        return {key: tuple(value) for key, value in result.items()}


@dataclass(frozen=True)
class RecoveryPreview:
    """Immutable preview shared by HTTP, MCP, UI, and the recovery worker.

    ``state_allowed`` describes topology semantics only.  Evidence gaps remain
    visible and never erase a research direction or silently change retry mode.
    """

    topology: str
    source_run_id: str
    target_child_id: str
    retry_mode: str
    command_id: str
    scope: Mapping[str, Any]
    scope_hash: str
    successor_run_id: str | None
    state_allowed: bool
    evidence: Mapping[str, Any]
    plan: RecoveryPlan | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "topology": self.topology,
            "source_run_id": self.source_run_id,
            "target_child_id": self.target_child_id,
            "retry_mode": self.retry_mode,
            "command_id": self.command_id,
            "scope": dict(self.scope),
            "scope_hash": self.scope_hash,
            "successor_run_id": self.successor_run_id,
            "state_allowed": self.state_allowed,
            "evidence": dict(self.evidence),
            "dependency_plan": (
                [entry.as_scope_row() for entry in self.plan.entries]
                if self.plan is not None
                else list(self.scope.get("dependency_plan") or [])
            ),
        }


class DurableRecoveryService:
    """Build deterministic recovery previews from durable source facts.

    This service intentionally does not submit remote QE work.  It freezes a
    topology and evidence set; the command worker later re-evaluates the exact
    same scope under source-row locks before any file publication or DB write.
    """

    def __init__(
        self,
        repository: Any,
        *,
        recovery_materializer_identity: Mapping[str, Any] | None = None,
        recovery_materializer_identity_resolver: Any | None = None,
    ) -> None:
        self._repository = repository
        self._recovery_materializer_identity = (
            dict(recovery_materializer_identity)
            if recovery_materializer_identity is not None
            else None
        )
        self._recovery_materializer_identity_resolver = (
            recovery_materializer_identity_resolver
        )

    def preview(
        self,
        *,
        source_run_id: str,
        target_child_id: str,
        retry_mode: str,
        idempotency_key: str,
    ) -> RecoveryPreview:
        if retry_mode not in ATTEMPT_RETRY_MODES - {RetryMode.INITIAL.value}:
            raise DurableContractError(
                "recovery retry mode is invalid",
                reason_code="multi_alpha_invalid_recovery_plan",
                context={"retry_mode": retry_mode},
            )
        source_run = self._repository.get_run(source_run_id)
        if source_run is None:
            raise DurableContractError(
                "source durable run does not exist",
                reason_code="multi_alpha_recovery_source_run_not_found",
                context={"source_run_id": source_run_id},
            )
        source_children = tuple(self._repository.list_children(source_run_id))
        target = next(
            (row for row in source_children if str(row.get("child_id") or "") == target_child_id),
            None,
        )
        if target is None:
            raise DurableContractError(
                "recovery target child is absent from the source run",
                reason_code="multi_alpha_recovery_target_not_found",
                context={"source_run_id": source_run_id, "target_child_id": target_child_id},
            )
        attempts_by_child = {
            str(child["child_id"]): tuple(self._repository.list_attempts(str(child["child_id"])))
            for child in source_children
        }
        command_id = make_command_id(source_run_id, idempotency_key)
        execution_identity = _execution_identity_from_source(source_run, source_children, attempts_by_child)
        identity_evidence = legacy_execution_identity_evidence(execution_identity)
        formula_version = _business_formula_version(execution_identity)
        source_status = str(source_run.get("status") or "")
        recovery_materializer_identity = self._materializer_identity_for(
            source_run=source_run,
            retry_mode=retry_mode,
        )

        if source_status in {"succeeded", "partial_failed", "partial_recovered", "failed", "cancelled"}:
            plan = build_recovery_plan(
                source_run=source_run,
                source_children=source_children,
                source_attempts_by_child=attempts_by_child,
                command_id=command_id,
                target_child_id=target_child_id,
                retry_mode=retry_mode,
                execution_identity=execution_identity,
                recovery_materializer_identity=recovery_materializer_identity,
                business_formula_version=formula_version,
            )
            evidence = {
                **recovery_execution_evidence(plan),
                "execution_identity": identity_evidence,
                "state_allowed": True,
                "topology_reason": "source_run_terminal",
            }
            return RecoveryPreview(
                topology="successor_recovery_run",
                source_run_id=source_run_id,
                target_child_id=target_child_id,
                retry_mode=retry_mode,
                command_id=command_id,
                scope=plan.scope,
                scope_hash=plan.scope_hash,
                successor_run_id=plan.successor_run_id,
                state_allowed=True,
                evidence=evidence,
                plan=plan,
            )

        return self._preview_in_place_results_only(
            source_run=source_run,
            source_children=source_children,
            target=target,
            attempts_by_child=attempts_by_child,
            command_id=command_id,
            retry_mode=retry_mode,
            execution_identity=execution_identity,
            identity_evidence=identity_evidence,
            formula_version=formula_version,
        )

    def preview_for_command(self, command: Mapping[str, Any]) -> RecoveryPreview:
        """Build a command-owned preview without reconstructing an idempotency key."""

        if str(command.get("action") or "") != "child_retry":
            raise DurableContractError(
                "recovery preview requires a child_retry command",
                reason_code="multi_alpha_invalid_recovery_plan",
            )
        request = dict(command.get("request_json") or {})
        retry_mode = str(request.get("retry_mode") or "")
        source_run_id = _required_text(command.get("run_id"), field="command.run_id")
        target_child_id = _required_text(command.get("child_id"), field="command.child_id")
        source_run = self._repository.get_run(source_run_id)
        if source_run is None:
            raise DurableContractError(
                "source durable run does not exist",
                reason_code="multi_alpha_recovery_source_run_not_found",
                context={"source_run_id": source_run_id},
            )
        source_children = tuple(self._repository.list_children(source_run_id))
        attempts_by_child = {
            str(child["child_id"]): tuple(self._repository.list_attempts(str(child["child_id"])))
            for child in source_children
        }
        target = next(
            (row for row in source_children if str(row.get("child_id") or "") == target_child_id),
            None,
        )
        if target is None:
            raise DurableContractError(
                "recovery target child is absent from the source run",
                reason_code="multi_alpha_recovery_target_not_found",
                context={"source_run_id": source_run_id, "target_child_id": target_child_id},
            )
        execution_identity = _execution_identity_from_source(source_run, source_children, attempts_by_child)
        identity_evidence = legacy_execution_identity_evidence(execution_identity)
        formula_version = _business_formula_version(execution_identity)
        recovery_materializer_identity = self._materializer_identity_for(
            source_run=source_run,
            retry_mode=retry_mode,
        )
        if str(source_run.get("status") or "") in {"succeeded", "partial_failed", "partial_recovered", "failed", "cancelled"}:
            plan = build_recovery_plan(
                source_run=source_run,
                source_children=source_children,
                source_attempts_by_child=attempts_by_child,
                command_id=_required_text(command.get("command_id"), field="command.command_id"),
                target_child_id=target_child_id,
                retry_mode=retry_mode,
                execution_identity=execution_identity,
                recovery_materializer_identity=recovery_materializer_identity,
                business_formula_version=formula_version,
            )
            return RecoveryPreview(
                topology="successor_recovery_run",
                source_run_id=source_run_id,
                target_child_id=target_child_id,
                retry_mode=retry_mode,
                command_id=str(command["command_id"]),
                scope=plan.scope,
                scope_hash=plan.scope_hash,
                successor_run_id=plan.successor_run_id,
                state_allowed=True,
                evidence={
                    **recovery_execution_evidence(plan),
                    "execution_identity": identity_evidence,
                    "state_allowed": True,
                    "topology_reason": "source_run_terminal",
                },
                plan=plan,
            )
        return self._preview_in_place_results_only(
            source_run=source_run,
            source_children=source_children,
            target=target,
            attempts_by_child=attempts_by_child,
            command_id=str(command["command_id"]),
            retry_mode=retry_mode,
            execution_identity=execution_identity,
            identity_evidence=identity_evidence,
            formula_version=formula_version,
        )

    def _materializer_identity_for(
        self,
        *,
        source_run: Mapping[str, Any],
        retry_mode: str,
    ) -> Mapping[str, Any] | None:
        if retry_mode != RetryMode.REMATERIALIZE_AND_BACKTEST.value:
            return self._recovery_materializer_identity
        if self._recovery_materializer_identity is not None:
            return dict(self._recovery_materializer_identity)
        resolver = self._recovery_materializer_identity_resolver
        if resolver is None:
            resolver = QEWorkspacePredBacktestAdapter(
                repository=self._repository,
            ).recovery_materializer_identity_for_run
        identity = resolver(source_run)
        if not isinstance(identity, Mapping) or not identity:
            raise DurableContractError(
                "rematerialization has no exact recovery materializer identity",
                reason_code="rematerialize_recovery_code_identity_missing",
            )
        return dict(identity)

    def _preview_in_place_results_only(
        self,
        *,
        source_run: Mapping[str, Any],
        source_children: Sequence[Mapping[str, Any]],
        target: Mapping[str, Any],
        attempts_by_child: Mapping[str, Sequence[Mapping[str, Any]]],
        command_id: str,
        retry_mode: str,
        execution_identity: Mapping[str, Any] | None,
        identity_evidence: Mapping[str, Any],
        formula_version: str,
    ) -> RecoveryPreview:
        target_id = str(target["child_id"])
        attempts = tuple(attempts_by_child.get(target_id) or ())
        selected = _select_source_attempt(target, attempts)
        active_attempts = [
            row for row in attempts
            if str(row.get("status") or "") in {"queued", "submitting", "running", "reconciling"}
        ]
        exact_state = (
            retry_mode == RetryMode.RESULTS_ONLY.value
            and str(target.get("status") or "") == "reconciling"
            and selected is not None
            and str(selected.get("status") or "") == "succeeded"
            and not active_attempts
        )
        lineage = _source_lineage(
            source_run=source_run,
            child=target,
            attempt=selected,
            execution_identity=execution_identity,
            recovery_materializer_identity=self._recovery_materializer_identity,
            business_formula_version=formula_version,
            retry_mode=retry_mode,
            disposition="reuse_result",
        )
        scope = {
            "schema_version": RECOVERY_PLAN_SCHEMA_VERSION,
            "topology": "append_results_reference_in_place",
            "source_run_id": source_run.get("id"),
            "target_child_id": target_id,
            "target_child_key": target.get("child_key"),
            "retry_mode": retry_mode,
            "request_hash": source_run.get("request_hash"),
            "roster_hash": source_run.get("roster_hash"),
            "execution_identity": dict(execution_identity) if execution_identity is not None else None,
            "business_formula_version": formula_version,
            "dependency_plan": [
                {
                    "source_child_id": target_id,
                    "child_key": target.get("child_key"),
                    "child_kind": target.get("child_kind"),
                    "source_status": target.get("status"),
                    "disposition": "reuse_result",
                    "source_attempt_id": selected.get("attempt_id") if selected is not None else None,
                    "source_attempt_status": selected.get("status") if selected is not None else None,
                    "source_lineage": lineage,
                    "source_lineage_hash": sha256_identity(lineage),
                }
            ],
            "active_attempt_ids": [str(row.get("attempt_id")) for row in active_attempts],
        }
        scope_hash = sha256_identity(scope)
        missing: list[str] = []
        if not exact_state:
            missing.append("recovery_source_run_nonterminal")
        if selected is None or str(selected.get("status") or "") != "succeeded":
            missing.append("results_only_successful_source_attempt_missing")
        if selected is not None and not selected.get("result_manifest_hash"):
            missing.append("results_only_result_manifest_missing")
        evidence = {
            "retry_mode": retry_mode,
            "complete": not missing,
            "evidence_gaps": missing,
            "acquisition_suggestions": _acquisition_suggestions(missing),
            "execution_identity": dict(identity_evidence),
            "state_allowed": exact_state,
            "topology_reason": "nonterminal_results_only_reference" if exact_state else "source_run_or_child_not_ready",
        }
        return RecoveryPreview(
            topology="append_results_reference_in_place",
            source_run_id=str(source_run["id"]),
            target_child_id=target_id,
            retry_mode=retry_mode,
            command_id=command_id,
            scope=scope,
            scope_hash=scope_hash,
            successor_run_id=None,
            state_allowed=exact_state,
            evidence=evidence,
            plan=None,
        )


@dataclass(frozen=True)
class SuccessorRecoverySpecs:
    """Fully deterministic successor rows prepared before one DB transaction."""

    run_spec: DurableRunSpec
    child_specs: tuple[DurableChildSpec, ...]
    attempt_specs: tuple[DurableAttemptSpec, ...]


class DurableRecoveryWorker:
    """Execute one already-durable child recovery without changing its intent.

    The worker is deliberately separate from command creation.  It replays the
    frozen command scope under a fresh lease, publishes only exact successor
    artifacts before database visibility, and records missing historical
    evidence as a visible reconciling state rather than changing the requested
    research operation or retry mode.
    """

    _EVIDENCE_PENDING_REASON_CODES = frozenset(
        {
            "results_only_successful_source_attempt_missing",
            "results_only_result_manifest_missing",
            "results_only_artifact_missing",
            "backtest_prediction_missing",
            "backtest_prediction_hash_mismatch",
            "backtest_identity_missing",
            "rematerialize_source_identity_missing",
            "rematerialize_recovery_code_identity_missing",
            "recovery_materializer_unavailable",
            "legacy_execution_identity_incomplete",
            "multi_alpha_execution_identity_incomplete",
            "multi_alpha_request_snapshot_missing",
        }
    )

    def __init__(
        self,
        *,
        repository: MultiAlphaDurableRepository,
        adapter: QEWorkspacePredBacktestAdapter,
        recovery_service: DurableRecoveryService | None = None,
    ) -> None:
        self._repository = repository
        self._adapter = adapter
        self._recovery_service = recovery_service or DurableRecoveryService(
            repository,
            recovery_materializer_identity_resolver=(
                adapter.recovery_materializer_identity_for_run
            ),
        )
        self._last_claimed_command_id: str | None = None

    @property
    def last_claimed_command_id(self) -> str | None:
        return self._last_claimed_command_id

    def execute_once(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_command_ids: Sequence[str] = (),
    ) -> bool:
        """Claim and execute one child_retry command.

        ``True`` means a durable item was claimed (including an explicit
        evidence-pending or stale-scope outcome); callers can therefore bound
        work fairly without inferring that all recovery work succeeded.
        """

        command = self._repository.claim_next_command(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            excluded_command_ids=excluded_command_ids,
            actions=("child_retry",),
        )
        if command is None:
            self._last_claimed_command_id = None
            return False
        command_id = str(command["command_id"])
        self._last_claimed_command_id = command_id
        token = _ownership_token(command)
        try:
            if str(command.get("status") or "") == "applying":
                command = self._repository.apply_control_command_intent(
                    command_id,
                    token=token,
                )
                token = _ownership_token(command)
            if str(command.get("status") or "") != "reconciling":
                return True

            preview = self._recovery_service.preview_for_command(command)
            if str(command.get("scope_hash") or "") != preview.scope_hash:
                self._fail_scope_stale(
                    command=command,
                    token=token,
                    preview=preview,
                    reason="command_scope_hash_differs_from_current_frozen_scope",
                )
                return True
            if not preview.state_allowed:
                self._fail_scope_stale(
                    command=command,
                    token=token,
                    preview=preview,
                    reason="source_state_no_longer_matches_requested_recovery_topology",
                )
                return True
            if preview.topology == "append_results_reference_in_place":
                self._execute_in_place_reference(
                    command=command,
                    token=token,
                    preview=preview,
                )
                return True
            if preview.topology != "successor_recovery_run" or preview.plan is None:
                raise DurableContractError(
                    "recovery preview returned an unsupported topology",
                    reason_code="multi_alpha_invalid_recovery_plan",
                    context={"command_id": command_id, "topology": preview.topology},
                )
            self._execute_successor(
                command=command,
                token=token,
                preview=preview,
            )
            return True
        except Exception as exc:
            # Staging-manifest persistence advances the fencing row version.
            # Re-read the lease-owned command before recording any later
            # evidence/error so a genuine post-publication failure is not
            # hidden behind an avoidable stale-token CAS failure.
            current = self._repository.get_command(command_id)
            if (
                current is not None
                and str(current.get("owner_id") or "") == owner_id
                and str(current.get("status") or "") in {"applying", "reconciling"}
            ):
                token = _ownership_token(current)
            if self._is_evidence_pending(exc):
                self._record_evidence_pending(
                    command_id=command_id,
                    token=token,
                    evidence={
                        "reason_code": _reason_code(exc),
                        "message": str(exc),
                        "context": _error_context(exc),
                        "acquisition_suggestions": _acquisition_suggestions_for_error(exc),
                    },
                    phase="recovery_evidence_pending",
                )
                return True
            self._fail_command(
                command_id=command_id,
                token=token,
                error=exc,
                phase="recovery_execution_failed",
            )
            return True

    def _execute_in_place_reference(
        self,
        *,
        command: Mapping[str, Any],
        token: OwnershipToken,
        preview: RecoveryPreview,
    ) -> None:
        child = self._repository.get_child(preview.target_child_id)
        if child is None:
            raise DurableContractError(
                "in-place recovery target child disappeared",
                reason_code="recovery_scope_stale",
                context={"child_id": preview.target_child_id},
            )
        selected_attempt_id = str(child.get("selected_attempt_id") or "").strip()
        if not selected_attempt_id:
            raise DurableContractError(
                "in-place recovery target has no selected attempt",
                reason_code="results_only_successful_source_attempt_missing",
                context={"child_id": preview.target_child_id},
            )
        payload = self._adapter.load_recovery_source_result_payload(
            source_run_id=preview.source_run_id,
            source_child_id=preview.target_child_id,
            source_attempt_id=selected_attempt_id,
        )
        source_attempt = self._repository.get_attempt(selected_attempt_id)
        if source_attempt is None:
            raise DurableContractError(
                "in-place recovery selected source attempt disappeared",
                reason_code="recovery_scope_stale",
                context={"attempt_id": selected_attempt_id},
            )
        manifest = _reference_result_manifest(
            source_attempt=source_attempt,
            payload=payload,
            business_formula_version=str(preview.scope.get("business_formula_version") or ""),
            execution_disposition="reuse_result",
        )
        self._repository.append_results_reference_in_place(
            command_id=str(command["command_id"]),
            token=token,
            expected_scope_hash=preview.scope_hash,
            result_manifest=manifest,
        )

    def _execute_successor(
        self,
        *,
        command: Mapping[str, Any],
        token: OwnershipToken,
        preview: RecoveryPreview,
    ) -> None:
        plan = preview.plan
        if plan is None:
            raise DurableContractError(
                "successor recovery is missing its frozen plan",
                reason_code="multi_alpha_invalid_recovery_plan",
            )
        # A results-only successor with no succeeded source result has no
        # remote execution branch.  Keep it discoverable and retriable when the
        # historical result is later acquired; do not reinterpret it as
        # backtest_only or full training.
        if plan.retry_mode == RetryMode.RESULTS_ONLY.value and any(
            entry.disposition == "execute" for entry in plan.entries
        ):
            raise DurableContractError(
                "results_only recovery needs a verified succeeded source result",
                reason_code="results_only_successful_source_attempt_missing",
                context={
                    "command_id": command.get("command_id"),
                    "target_child_id": plan.target_child_id,
                },
            )

        source_run = self._repository.get_run(plan.source_run_id)
        if source_run is None:
            raise DurableContractError(
                "recovery source run disappeared before successor publication",
                reason_code="recovery_scope_stale",
                context={"source_run_id": plan.source_run_id},
            )
        source_children = tuple(self._repository.list_children(plan.source_run_id))
        attempts_by_child = {
            str(child["child_id"]): tuple(self._repository.list_attempts(str(child["child_id"])))
            for child in source_children
        }
        source_result_payloads: dict[str, Mapping[str, Any]] = {}
        for entry in plan.entries:
            if entry.disposition not in {"reuse_result", "recompute_derived"}:
                continue
            if entry.source_attempt_id is None:
                raise DurableContractError(
                    "recovery reference entry is missing its source attempt",
                    reason_code="results_only_artifact_missing",
                    context={"source_child_id": entry.source_child_id},
                )
            source_result_payloads[entry.source_attempt_id] = (
                self._adapter.load_recovery_source_result_payload(
                    source_run_id=plan.source_run_id,
                    source_child_id=entry.source_child_id,
                    source_attempt_id=entry.source_attempt_id,
                )
            )
        specs = build_successor_recovery_specs(
            plan=plan,
            source_run=source_run,
            source_children=source_children,
            source_attempts_by_child=attempts_by_child,
            source_result_payloads=source_result_payloads,
        )
        staging_manifest = recovery_staging_manifest(plan=plan, recovery_specs=specs)
        command_after_staging = self._repository.record_recovery_staging_manifest(
            str(command["command_id"]),
            token=token,
            staging_manifest=staging_manifest,
        )
        staging_token = _ownership_token(command_after_staging)
        child_spec_by_source = {
            str(spec.source_child_id): spec
            for spec in specs.child_specs
            if spec.source_child_id is not None
        }
        attempt_spec_by_child = {spec.child_id: spec for spec in specs.attempt_specs}
        for entry in plan.entries:
            if entry.disposition != "execute":
                continue
            child_spec = child_spec_by_source.get(entry.source_child_id)
            if child_spec is None:
                raise DurableContractError(
                    "recovery execute entry has no successor child specification",
                    reason_code="multi_alpha_invalid_recovery_plan",
                    context={"source_child_id": entry.source_child_id},
                )
            attempt_spec = attempt_spec_by_child.get(child_spec.child_id)
            if attempt_spec is None:
                raise DurableContractError(
                    "recovery execute entry has no successor attempt specification",
                    reason_code="multi_alpha_invalid_recovery_plan",
                    context={"source_child_id": entry.source_child_id},
                )
            if entry.source_attempt_id is None:
                raise DurableContractError(
                    "recovery execute entry has no frozen source attempt",
                    reason_code="backtest_prediction_missing",
                    context={"source_child_id": entry.source_child_id},
                )
            lineage_hash = sha256_identity(dict(entry.source_lineage))
            if plan.retry_mode == RetryMode.BACKTEST_ONLY.value:
                self._adapter.stage_backtest_only_recovery_artifacts(
                    source_run_id=plan.source_run_id,
                    source_child_id=entry.source_child_id,
                    source_attempt_id=entry.source_attempt_id,
                    successor_run_id=plan.successor_run_id,
                    successor_child_id=child_spec.child_id,
                    successor_attempt_id=attempt_spec.attempt_id,
                    successor_input_manifest_hash=child_spec.input_manifest_hash,
                    source_lineage_hash=lineage_hash,
                )
            elif plan.retry_mode == RetryMode.REMATERIALIZE_AND_BACKTEST.value:
                self._adapter.stage_rematerialized_recovery_artifacts(
                    source_run=source_run,
                    source_child=next(
                        child for child in source_children
                        if str(child["child_id"]) == entry.source_child_id
                    ),
                    source_attempt_id=entry.source_attempt_id,
                    successor_run_spec=specs.run_spec,
                    successor_child_spec=child_spec,
                    successor_attempt_spec=attempt_spec,
                    source_lineage=dict(entry.source_lineage),
                )
            else:
                raise DurableContractError(
                    "recovery retry mode cannot publish a remote execute artifact",
                    reason_code="multi_alpha_invalid_recovery_plan",
                    context={"retry_mode": plan.retry_mode},
                )
        self._repository.materialize_successor_recovery(
            command_id=str(command["command_id"]),
            token=staging_token,
            recovery_specs=specs,
            staging_manifest=staging_manifest,
        )

    def _fail_scope_stale(
        self,
        *,
        command: Mapping[str, Any],
        token: OwnershipToken,
        preview: RecoveryPreview,
        reason: str,
    ) -> None:
        self._repository.transition_command_with_event(
            str(command["command_id"]),
            token=token,
            expected_statuses=("reconciling",),
            next_status="failed",
            response={
                "recovery": "scope_stale",
                "current_scope_hash": preview.scope_hash,
                "requested_scope_hash": command.get("scope_hash"),
                "topology": preview.topology,
                "evidence": dict(preview.evidence),
            },
            reason_code="recovery_scope_stale",
            error={
                "reason": reason,
                "source_run_id": preview.source_run_id,
                "target_child_id": preview.target_child_id,
            },
        )

    def _record_evidence_pending(
        self,
        *,
        command_id: str,
        token: OwnershipToken,
        evidence: Mapping[str, Any],
        phase: str,
    ) -> None:
        self._repository.record_recovery_pending_evidence(
            command_id,
            token=token,
            evidence=evidence,
            phase=phase,
        )

    def _fail_command(
        self,
        *,
        command_id: str,
        token: OwnershipToken,
        error: Exception,
        phase: str,
    ) -> None:
        self._repository.transition_command_with_event(
            command_id,
            token=token,
            expected_statuses=("reconciling", "applying"),
            next_status="failed",
            response={"recovery": "execution_error"},
            reason_code=_reason_code(error),
            error={
                "reason_code": _reason_code(error),
                "message": str(error),
                "context": _error_context(error),
                "error_type": type(error).__name__,
            },
        )

    def _is_evidence_pending(self, error: Exception) -> bool:
        reason_code = _reason_code(error)
        return reason_code in self._EVIDENCE_PENDING_REASON_CODES


def recovery_staging_manifest(
    *,
    plan: RecoveryPlan,
    recovery_specs: SuccessorRecoverySpecs,
) -> dict[str, Any]:
    """Describe all pre-DB successor file publications by content identity."""

    attempt_by_child = {spec.child_id: spec for spec in recovery_specs.attempt_specs}
    staged: list[dict[str, Any]] = []
    for entry in plan.entries:
        if entry.disposition != "execute":
            continue
        successor_child_id = make_child_id(plan.successor_run_id, entry.child_key)
        successor_attempt = attempt_by_child.get(successor_child_id)
        if successor_attempt is None:
            raise DurableContractError(
                "execute recovery entry has no successor remote attempt",
                reason_code="multi_alpha_invalid_recovery_plan",
                context={"child_key": entry.child_key},
            )
        lineage = dict(entry.source_lineage)
        source_artifact_manifest = lineage.get("artifact_manifest")
        if not isinstance(source_artifact_manifest, Mapping):
            raise DurableContractError(
                "execute recovery entry has no frozen source artifact manifest",
                reason_code="backtest_prediction_missing",
                context={"child_key": entry.child_key},
            )
        staged.append(
            {
                "source_run_id": plan.source_run_id,
                "source_child_id": entry.source_child_id,
                "source_attempt_id": entry.source_attempt_id,
                "source_artifact_manifest_hash": entry.source_lineage.get("artifact_manifest_hash"),
                "source_artifact_manifest": dict(source_artifact_manifest),
                "successor_run_id": plan.successor_run_id,
                "successor_child_id": successor_child_id,
                "successor_attempt_id": successor_attempt.attempt_id,
                "successor_input_manifest_hash": next(
                    spec.input_manifest_hash
                    for spec in recovery_specs.child_specs
                    if spec.child_id == successor_child_id
                ),
                "source_lineage_hash": sha256_identity(dict(entry.source_lineage)),
            }
        )
    return {
        "schema_version": "multi_alpha_recovery_staging_manifest_v1",
        "command_id": plan.command_id,
        "source_run_id": plan.source_run_id,
        "successor_run_id": plan.successor_run_id,
        "recovery_scope_hash": plan.scope_hash,
        "staged_execute_attempts": staged,
    }


def build_successor_recovery_specs(
    *,
    plan: RecoveryPlan,
    source_run: Mapping[str, Any],
    source_children: Sequence[Mapping[str, Any]],
    source_attempts_by_child: Mapping[str, Sequence[Mapping[str, Any]]],
    source_result_payloads: Mapping[str, Mapping[str, Any]],
) -> SuccessorRecoverySpecs:
    """Construct all successor rows without selecting current defaults.

    ``source_result_payloads`` is supplied by the execution adapter after it has
    verified the source result/artifact content.  It must contain exact metrics
    and materialization metadata for every reference/derived child; this keeps
    their read model usable even after an explicit source workspace deletion.
    """

    source_children_by_id = {str(row["child_id"]): row for row in source_children}
    source_run_id = _required_text(source_run.get("id"), field="source_run.id")
    if source_run_id != plan.source_run_id:
        raise DurableContractError(
            "successor recovery source run differs from frozen plan",
            reason_code="source_lineage_mismatch",
            context={"plan_source_run_id": plan.source_run_id, "source_run_id": source_run_id},
        )
    roster = _mapping_sequence_or_error(source_run.get("roster_json"), field="source_run.roster_json")
    walk_forward = _mapping_or_error(source_run.get("walk_forward_json"), field="source_run.walk_forward_json")
    backtest_config = _mapping_or_error(source_run.get("backtest_config_json"), field="source_run.backtest_config_json")
    node_parallelism = _mapping_or_error(
        source_run.get("node_parallelism_json") or {},
        field="source_run.node_parallelism_json",
    )
    source_execution_identity = _execution_identity_from_source(
        source_run,
        source_children,
        source_attempts_by_child,
    )
    source_identity_evidence = source_run.get("execution_identity_evidence_json")
    if not isinstance(source_identity_evidence, Mapping):
        source_identity_evidence = legacy_execution_identity_evidence(source_execution_identity)
    run_payload = durable_run_request_payload(
        roster_hash=_required_text(source_run.get("roster_hash"), field="source_run.roster_hash"),
        roster=roster,
        oos_start=source_run.get("oos_start"),
        oos_end=source_run.get("oos_end"),
        normalize_method=_required_text(source_run.get("normalize_method"), field="source_run.normalize_method"),
        walk_forward=walk_forward,
        backtest_config=backtest_config,
        baseline_leg_id=source_run.get("baseline_leg_id"),
        retry_of_run_id=source_run_id,
        node_parallelism=node_parallelism,
        recovery_kind="child_targeted",
        recovery_scope=plan.scope,
        recovery_scope_hash=plan.scope_hash,
        execution_identity=source_execution_identity,
        execution_identity_hash=(sha256_identity(source_execution_identity) if source_execution_identity is not None else None),
        execution_identity_evidence=source_identity_evidence,
    )
    run_spec = DurableRunSpec(
        run_id=plan.successor_run_id,
        task_id=_required_text(source_run.get("task_id"), field="source_run.task_id"),
        request_hash=request_hash_for(run_payload),
        roster_hash=_required_text(source_run.get("roster_hash"), field="source_run.roster_hash"),
        roster=roster,
        oos_start=source_run.get("oos_start"),
        oos_end=source_run.get("oos_end"),
        normalize_method=_required_text(source_run.get("normalize_method"), field="source_run.normalize_method"),
        walk_forward=walk_forward,
        backtest_config=backtest_config,
        baseline_leg_id=source_run.get("baseline_leg_id"),
        retry_of_run_id=source_run_id,
        node_parallelism=node_parallelism,
        recovery_kind="child_targeted",
        recovery_scope=plan.scope,
        recovery_scope_hash=plan.scope_hash,
        execution_identity=source_execution_identity,
        execution_identity_hash=(sha256_identity(source_execution_identity) if source_execution_identity is not None else None),
        execution_identity_evidence=source_identity_evidence,
    )
    child_specs: list[DurableChildSpec] = []
    attempt_specs: list[DurableAttemptSpec] = []
    for entry in plan.entries:
        source_child = source_children_by_id.get(entry.source_child_id)
        if source_child is None:
            raise DurableContractError(
                "source child disappeared while building successor rows",
                reason_code="recovery_scope_stale",
                context={"source_child_id": entry.source_child_id},
            )
        successor_child_id = make_child_id(plan.successor_run_id, entry.child_key)
        source_input = _mapping_or_error(source_child.get("input_manifest_json"), field="source_child.input_manifest_json")
        input_manifest = {
            **source_input,
            "run_id": plan.successor_run_id,
            "recovery": {
                "schema_version": RECOVERY_PLAN_SCHEMA_VERSION,
                "source_run_id": plan.source_run_id,
                "source_child_id": entry.source_child_id,
                "source_attempt_id": entry.source_attempt_id,
                "scope_hash": plan.scope_hash,
                "retry_mode": plan.retry_mode,
                "execution_disposition": entry.disposition,
            },
        }
        input_manifest_hash = artifact_manifest_hash_for(input_manifest)
        child_status = {
            "execute": "queued",
            "reuse_result": "reconciling",
            "recompute_derived": "reconciling",
            "preserve_unavailable": "not_recovered",
        }[entry.disposition]
        child_spec = DurableChildSpec(
            child_id=successor_child_id,
            run_id=plan.successor_run_id,
            child_key=entry.child_key,
            child_kind=entry.child_kind,
            ordinal=int(source_child.get("ordinal") or 0),
            input_manifest=input_manifest,
            input_manifest_hash=input_manifest_hash,
            status=child_status,
            weighting_scheme=source_child.get("weighting_scheme"),
            dropped_leg_id=source_child.get("dropped_leg_id"),
            prediction_artifact_uri=(
                source_child.get("prediction_artifact_uri") if entry.disposition == "execute" else None
            ),
            prediction_artifact_hash=(
                source_child.get("prediction_artifact_hash") if entry.disposition == "execute" else None
            ),
            source_kind="runtime" if entry.disposition == "execute" else "recovery_reference",
            source_child_id=entry.source_child_id,
            execution_disposition=entry.disposition,
            source_lineage=entry.source_lineage,
            source_lineage_hash=sha256_identity(dict(entry.source_lineage)),
        )
        child_specs.append(child_spec)
        if entry.disposition == "preserve_unavailable":
            continue
        source_attempt = _source_attempt_for_entry(entry, source_attempts_by_child)
        successor_attempt_id = make_attempt_id(successor_child_id, 1)
        if entry.disposition == "execute":
            if plan.retry_mode == RetryMode.RESULTS_ONLY.value:
                # Never turn a results-only request into a remote submission.
                # The recovery worker keeps its evidence/acquisition state
                # visible until a verified historical result can be supplied.
                raise DurableContractError(
                    "results_only recovery cannot execute a missing remote result",
                    reason_code="results_only_successful_source_attempt_missing",
                    context={
                        "source_child_id": entry.source_child_id,
                        "source_attempt_id": entry.source_attempt_id,
                    },
                )
            node_id = source_attempt.get("node_id") if source_attempt is not None else None
            if not isinstance(node_id, str) or not node_id:
                raise DurableContractError(
                    "backtest recovery source attempt has no frozen node identity",
                    reason_code="backtest_identity_missing",
                    context={"source_child_id": entry.source_child_id, "source_attempt_id": entry.source_attempt_id},
                )
            attempt_specs.append(
                DurableAttemptSpec(
                    attempt_id=successor_attempt_id,
                    run_id=plan.successor_run_id,
                    child_id=successor_child_id,
                    attempt_no=1,
                    retry_mode=plan.retry_mode,
                    source_attempt_id=str(source_attempt["attempt_id"]),
                    execution_kind="remote_execution",
                    node_id=node_id,
                    status="queued",
                    phase="recovery_queued",
                )
            )
            continue
        if source_attempt is None or entry.source_attempt_id is None:
            raise DurableContractError(
                "reference/derived recovery has no frozen source attempt",
                reason_code="results_only_artifact_missing",
                context={"source_child_id": entry.source_child_id},
            )
        result_payload = source_result_payloads.get(str(source_attempt["attempt_id"]))
        if not isinstance(result_payload, Mapping):
            raise DurableContractError(
                "reference/derived recovery source result payload is unavailable",
                reason_code="results_only_artifact_missing",
                context={"source_attempt_id": source_attempt["attempt_id"]},
            )
        result_manifest = {
            "schema_version": "multi_alpha_recovery_reference_result_v1",
            "source_attempt_id": source_attempt["attempt_id"],
            "source_result_manifest": dict(source_attempt.get("result_manifest_json") or {}),
            "source_result_manifest_hash": source_attempt.get("result_manifest_hash"),
            "source_artifact_manifest": dict(source_attempt.get("artifact_manifest_json") or {}),
            "metrics": _mapping_or_error(result_payload.get("metrics"), field="source_result_payload.metrics"),
            "materialization_metadata": _mapping_or_error(
                result_payload.get("materialization_metadata"),
                field="source_result_payload.materialization_metadata",
            ),
            "business_formula_version": plan.scope.get("business_formula_version"),
            "execution_disposition": entry.disposition,
        }
        result_manifest_hash = artifact_manifest_hash_for(result_manifest)
        attempt_specs.append(
            DurableAttemptSpec(
                attempt_id=successor_attempt_id,
                run_id=plan.successor_run_id,
                child_id=successor_child_id,
                attempt_no=1,
                retry_mode=RetryMode.RESULTS_ONLY.value,
                source_attempt_id=str(source_attempt["attempt_id"]),
                execution_kind=("derived_result" if entry.disposition == "recompute_derived" else "reference_result"),
                status="succeeded",
                phase="recovery_reference_result",
                artifact_manifest=dict(source_attempt.get("artifact_manifest_json") or {}),
                result_manifest=result_manifest,
                result_manifest_hash=result_manifest_hash,
            )
        )
    return SuccessorRecoverySpecs(
        run_spec=run_spec,
        child_specs=tuple(child_specs),
        attempt_specs=tuple(attempt_specs),
    )


def build_recovery_plan(
    *,
    source_run: Mapping[str, Any],
    source_children: Sequence[Mapping[str, Any]],
    source_attempts_by_child: Mapping[str, Sequence[Mapping[str, Any]]],
    command_id: str,
    target_child_id: str,
    retry_mode: str,
    execution_identity: Mapping[str, Any] | None,
    recovery_materializer_identity: Mapping[str, Any] | None,
    business_formula_version: str,
) -> RecoveryPlan:
    """Freeze a full child closure for one terminal-source recovery request."""

    if retry_mode not in ATTEMPT_RETRY_MODES - {RetryMode.INITIAL.value}:
        raise DurableContractError(
            "recovery retry mode is invalid",
            reason_code="multi_alpha_invalid_recovery_plan",
            context={"retry_mode": retry_mode},
        )
    source_run_id = _required_text(source_run.get("id"), field="source_run.id")
    source_run_status = _required_text(source_run.get("status"), field="source_run.status")
    if source_run_status not in {"succeeded", "partial_failed", "partial_recovered", "failed", "cancelled"}:
        raise DurableContractError(
            "targeted successor recovery requires a terminal source run",
            reason_code="multi_alpha_recovery_source_run_not_terminal",
            context={"source_run_id": source_run_id, "status": source_run_status},
        )
    if not isinstance(source_children, Sequence) or not source_children:
        raise DurableContractError(
            "source recovery run has no durable children",
            reason_code="multi_alpha_recovery_source_children_missing",
        )
    target = next(
        (child for child in source_children if str(child.get("child_id") or "") == target_child_id),
        None,
    )
    if target is None:
        raise DurableContractError(
            "recovery target child is absent from the source run",
            reason_code="multi_alpha_recovery_target_not_found",
            context={"source_run_id": source_run_id, "target_child_id": target_child_id},
        )
    if str(target.get("status") or "") not in RECOVERY_TERMINAL_CHILD_STATUSES:
        raise DurableContractError(
            "non-terminal child recovery must use the narrow in-place results-only path",
            reason_code="multi_alpha_recovery_source_child_not_terminal",
            context={"target_child_id": target_child_id, "status": target.get("status")},
        )
    if str(target.get("status") or "") == "succeeded":
        raise DurableContractError(
            "successful child is immutable; changed research inputs require a new ordinary QE run",
            reason_code="multi_alpha_recovery_successful_child_immutable",
            context={"target_child_id": target_child_id},
        )

    target_key = _required_text(target.get("child_key"), field="target.child_key")
    target_kind = _required_text(target.get("child_kind"), field="target.child_kind")
    source_materializer_identity = (
        execution_identity.get("materializer")
        if isinstance(execution_identity, Mapping)
        else None
    )
    materializer_identity_changed = bool(
        retry_mode == RetryMode.REMATERIALIZE_AND_BACKTEST.value
        and isinstance(source_materializer_identity, Mapping)
        and isinstance(recovery_materializer_identity, Mapping)
        and sha256_identity(dict(source_materializer_identity))
        != sha256_identity(dict(recovery_materializer_identity))
    )
    sorted_children = sorted(source_children, key=lambda item: (int(item.get("ordinal") or 0), str(item.get("child_id") or "")))
    entries: list[RecoveryPlanEntry] = []
    for child in sorted_children:
        child_id = _required_text(child.get("child_id"), field="child.child_id")
        child_key = _required_text(child.get("child_key"), field="child.child_key")
        child_kind = _required_text(child.get("child_kind"), field="child.child_kind")
        child_status = _required_text(child.get("status"), field="child.status")
        attempts = tuple(source_attempts_by_child.get(child_id) or ())
        selected_attempt = _select_source_attempt(child, attempts)
        disposition = _disposition_for_child(
            child=child,
            target_child_id=target_child_id,
            target_kind=target_kind,
            target_weighting_scheme=target.get("weighting_scheme"),
            retry_mode=retry_mode,
            selected_attempt=selected_attempt,
            materializer_identity_changed=materializer_identity_changed,
        )
        lineage = _source_lineage(
            source_run=source_run,
            child=child,
            attempt=selected_attempt,
            execution_identity=execution_identity,
            recovery_materializer_identity=recovery_materializer_identity,
            business_formula_version=business_formula_version,
            retry_mode=retry_mode,
            disposition=disposition,
        )
        entries.append(
            RecoveryPlanEntry(
                source_child_id=child_id,
                child_key=child_key,
                child_kind=child_kind,
                source_status=child_status,
                disposition=disposition,
                source_attempt_id=(str(selected_attempt.get("attempt_id")) if selected_attempt else None),
                source_attempt_status=(str(selected_attempt.get("status")) if selected_attempt else None),
                source_lineage=lineage,
            )
        )

    scope = {
        "schema_version": RECOVERY_PLAN_SCHEMA_VERSION,
        "source_run_id": source_run_id,
        "target_child_id": target_child_id,
        "target_child_key": target_key,
        "retry_mode": retry_mode,
        "request_hash": source_run.get("request_hash"),
        "roster_hash": source_run.get("roster_hash"),
        "dataset_identity": dict(execution_identity or {}).get("dataset"),
        "execution_identity": dict(execution_identity) if execution_identity is not None else None,
        "source_prediction_hashes": _prediction_hashes(entries),
        "backtest_runtime_identity": dict(execution_identity or {}).get("runtime"),
        "source_materializer_code_identity": dict(execution_identity or {}).get("materializer"),
        "recovery_materializer_code_identity": (
            dict(recovery_materializer_identity) if recovery_materializer_identity is not None else None
        ),
        "materializer_identity_changed": materializer_identity_changed,
        "business_formula_version": business_formula_version,
        "dependency_plan": [entry.as_scope_row() for entry in entries],
    }
    scope_hash = sha256_identity(scope)
    return RecoveryPlan(
        source_run_id=source_run_id,
        command_id=command_id,
        target_child_id=target_child_id,
        target_child_key=target_key,
        retry_mode=retry_mode,
        entries=tuple(entries),
        scope=scope,
        scope_hash=scope_hash,
        successor_run_id=make_successor_run_id(
            source_run_id=source_run_id,
            command_id=command_id,
            scope_hash=scope_hash,
        ),
    )


def recovery_execution_evidence(plan: RecoveryPlan) -> dict[str, Any]:
    """Return evidence gaps and acquisition advice; never reject research direction."""

    target = next(entry for entry in plan.entries if entry.source_child_id == plan.target_child_id)
    gaps: list[str] = []
    lineage = dict(target.source_lineage)
    identity = lineage.get("execution_identity")
    if not isinstance(identity, Mapping):
        gaps.append("legacy_execution_identity_incomplete")
    if plan.retry_mode == RetryMode.RESULTS_ONLY.value:
        if target.source_attempt_status != "succeeded":
            gaps.append("results_only_successful_source_attempt_missing")
        result_hash = lineage.get("result_manifest_hash")
        if not isinstance(result_hash, str) or not result_hash:
            gaps.append("results_only_result_manifest_missing")
    elif plan.retry_mode == RetryMode.BACKTEST_ONLY.value:
        if not lineage.get("prediction_artifact_uri") or not lineage.get("prediction_artifact_hash"):
            gaps.append("backtest_prediction_missing")
    elif plan.retry_mode == RetryMode.REMATERIALIZE_AND_BACKTEST.value:
        if not isinstance(lineage.get("source_input_manifest"), Mapping):
            gaps.append("rematerialize_source_identity_missing")
        if not isinstance(lineage.get("recovery_materializer_identity"), Mapping):
            gaps.append("rematerialize_recovery_code_identity_missing")
    return {
        "retry_mode": plan.retry_mode,
        "complete": not gaps,
        "evidence_gaps": gaps,
        "acquisition_suggestions": _acquisition_suggestions(gaps),
        "scope_hash": plan.scope_hash,
        "successor_run_id": plan.successor_run_id,
    }


def _disposition_for_child(
    *,
    child: Mapping[str, Any],
    target_child_id: str,
    target_kind: str,
    target_weighting_scheme: Any,
    retry_mode: str,
    selected_attempt: Mapping[str, Any] | None,
    materializer_identity_changed: bool,
) -> str:
    child_id = str(child.get("child_id") or "")
    child_status = str(child.get("status") or "")
    if child_id == target_child_id:
        # A terminal child can have a successful remote result but fail only
        # during downstream business assembly.  In that exact case reuse the
        # frozen result for results_only; do not manufacture a second remote
        # execution.  If the successful result is absent, the worker returns
        # explicit acquisition evidence and never changes the requested mode.
        if (
            retry_mode == RetryMode.RESULTS_ONLY.value
            and selected_attempt is not None
            and str(selected_attempt.get("status") or "") == "succeeded"
        ):
            return "reuse_result"
        return "execute"
    if child_status != "succeeded":
        return "preserve_unavailable"
    if materializer_identity_changed:
        # A rematerializer code change affects every successful raw result in
        # the successor.  Reusing any of them would mix outputs produced by
        # different materializer identities and make downstream scheme/LOO
        # comparisons semantically ambiguous.
        return "execute"
    child_kind = str(child.get("child_kind") or "")
    # Recomputing a baseline changes scheme deltas and any LOO derivation that
    # depends on the full scheme. Recomputing a scheme changes only its LOO
    # descendants. The formulas themselves remain in DurableBusinessResultAssembler.
    if target_kind == "baseline" and child_kind in {"scheme", "loo"}:
        return "recompute_derived"
    if (
        target_kind == "scheme"
        and child_kind == "loo"
        and child.get("weighting_scheme") == target_weighting_scheme
    ):
        return "recompute_derived"
    # results_only still references an already verified result; it never
    # creates remote execution merely because the child is in the closure.
    return "reuse_result"


def _select_source_attempt(
    child: Mapping[str, Any],
    attempts: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    selected_attempt_id = str(child.get("selected_attempt_id") or "").strip()
    if selected_attempt_id:
        selected = next(
            (item for item in attempts if str(item.get("attempt_id") or "") == selected_attempt_id),
            None,
        )
        if selected is not None:
            return selected
    succeeded = [item for item in attempts if str(item.get("status") or "") == "succeeded"]
    if succeeded:
        return max(
            succeeded,
            key=lambda item: (int(item.get("attempt_no") or 0), str(item.get("attempt_id") or "")),
        )
    if attempts:
        return max(
            attempts,
            key=lambda item: (int(item.get("attempt_no") or 0), str(item.get("attempt_id") or "")),
        )
    return None


def _source_lineage(
    *,
    source_run: Mapping[str, Any],
    child: Mapping[str, Any],
    attempt: Mapping[str, Any] | None,
    execution_identity: Mapping[str, Any] | None,
    recovery_materializer_identity: Mapping[str, Any] | None,
    business_formula_version: str,
    retry_mode: str,
    disposition: str,
) -> dict[str, Any]:
    input_manifest = dict(child.get("input_manifest_json") or {})
    result_manifest = dict(attempt.get("result_manifest_json") or {}) if attempt is not None else {}
    artifact_manifest = dict(attempt.get("artifact_manifest_json") or {}) if attempt is not None else {}
    return {
        "source_run_id": source_run.get("id"),
        "source_run_request_hash": source_run.get("request_hash"),
        "source_child_id": child.get("child_id"),
        "source_child_key": child.get("child_key"),
        "source_child_kind": child.get("child_kind"),
        "source_child_status": child.get("status"),
        "source_attempt_id": attempt.get("attempt_id") if attempt is not None else None,
        "source_attempt_status": attempt.get("status") if attempt is not None else None,
        "source_attempt_execution_kind": attempt.get("execution_kind") if attempt is not None else None,
        "source_attempt_retry_mode": attempt.get("retry_mode") if attempt is not None else None,
        "source_input_manifest": input_manifest,
        "source_input_manifest_hash": child.get("input_manifest_hash"),
        "prediction_artifact_uri": child.get("prediction_artifact_uri"),
        "prediction_artifact_hash": child.get("prediction_artifact_hash"),
        "artifact_manifest": artifact_manifest,
        "artifact_manifest_hash": artifact_manifest_hash_for(artifact_manifest),
        "result_manifest": result_manifest,
        "result_manifest_hash": attempt.get("result_manifest_hash") if attempt is not None else None,
        "execution_identity": dict(execution_identity) if execution_identity is not None else None,
        "recovery_materializer_identity": (
            dict(recovery_materializer_identity) if recovery_materializer_identity is not None else None
        ),
        "business_formula_version": business_formula_version,
        "retry_mode": retry_mode,
        "disposition": disposition,
    }


def _prediction_hashes(entries: Sequence[RecoveryPlanEntry]) -> list[dict[str, Any]]:
    hashes: list[dict[str, Any]] = []
    for entry in entries:
        lineage = entry.source_lineage
        hashes.append(
            {
                "child_key": entry.child_key,
                "prediction_artifact_hash": lineage.get("prediction_artifact_hash"),
                "source_attempt_id": entry.source_attempt_id,
            }
        )
    return hashes


def _acquisition_suggestions(gaps: Sequence[str]) -> list[str]:
    suggestions: list[str] = []
    if "legacy_execution_identity_incomplete" in gaps:
        suggestions.append("locate immutable dataset/runtime/code manifests for the historical attempt")
    if any("prediction" in gap for gap in gaps):
        suggestions.append("locate or republish the exact prediction artifact and verify SHA-256")
    if any("result_manifest" in gap for gap in gaps):
        suggestions.append("collect the exact source result manifest and preserve its content hash")
    if any("materialize" in gap for gap in gaps):
        suggestions.append("capture frozen source input and materializer code identity before rematerialization")
    return suggestions


def _execution_identity_from_source(
    source_run: Mapping[str, Any],
    source_children: Sequence[Mapping[str, Any]],
    attempts_by_child: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any] | None:
    """Read only persisted identity evidence; never reconstruct mutable defaults."""

    candidates: list[dict[str, Any]] = []
    run_identity = source_run.get("execution_identity_json")
    if isinstance(run_identity, Mapping) and run_identity:
        candidates.append(dict(run_identity))
    for child in source_children:
        input_manifest = child.get("input_manifest_json")
        if isinstance(input_manifest, Mapping):
            identity = input_manifest.get("execution_identity")
            if isinstance(identity, Mapping) and identity:
                candidates.append(dict(identity))
        for attempt in attempts_by_child.get(str(child.get("child_id") or ""), ()):
            artifact_manifest = attempt.get("artifact_manifest_json")
            if isinstance(artifact_manifest, Mapping):
                identity = artifact_manifest.get("execution_identity")
                if isinstance(identity, Mapping) and identity:
                    candidates.append(dict(identity))
    if not candidates:
        return None
    expected_hash = sha256_identity(candidates[0])
    if any(sha256_identity(candidate) != expected_hash for candidate in candidates[1:]):
        raise DurableContractError(
            "source execution identities conflict across durable manifests",
            reason_code="source_lineage_mismatch",
            context={"identity_hashes": sorted({sha256_identity(candidate) for candidate in candidates})},
        )
    persisted_hash = source_run.get("execution_identity_hash")
    if persisted_hash is not None and str(persisted_hash) != expected_hash:
        raise DurableContractError(
            "source run execution identity hash does not match persisted identity",
            reason_code="source_lineage_mismatch",
            context={"expected": expected_hash, "actual": persisted_hash},
        )
    return candidates[0]


def _business_formula_version(execution_identity: Mapping[str, Any] | None) -> str:
    if isinstance(execution_identity, Mapping):
        formula = execution_identity.get("business_formula")
        if isinstance(formula, Mapping) and isinstance(formula.get("formula_version"), str) and formula["formula_version"].strip():
            return str(formula["formula_version"])
    # This is an explicit historical-evidence marker included in the frozen scope,
    # never a substitute for a current business formula or runtime default.
    return "legacy_execution_identity_incomplete"


def _source_attempt_for_entry(
    entry: RecoveryPlanEntry,
    source_attempts_by_child: Mapping[str, Sequence[Mapping[str, Any]]],
) -> Mapping[str, Any] | None:
    if entry.source_attempt_id is None:
        return None
    return next(
        (
            row
            for row in source_attempts_by_child.get(entry.source_child_id, ())
            if str(row.get("attempt_id") or "") == entry.source_attempt_id
        ),
        None,
    )


def _reference_result_manifest(
    *,
    source_attempt: Mapping[str, Any],
    payload: Mapping[str, Any],
    business_formula_version: str,
    execution_disposition: str,
) -> dict[str, Any]:
    """Build the immutable result payload used by explicit reference rows."""

    source_result_manifest = _mapping_or_error(
        source_attempt.get("result_manifest_json"),
        field="source_attempt.result_manifest_json",
    )
    source_result_manifest_hash = _required_text(
        source_attempt.get("result_manifest_hash"),
        field="source_attempt.result_manifest_hash",
    )
    if artifact_manifest_hash_for(source_result_manifest) != source_result_manifest_hash:
        raise DurableContractError(
            "source result manifest content does not match its persisted hash",
            reason_code="results_only_artifact_missing",
            context={"source_attempt_id": source_attempt.get("attempt_id")},
        )
    return {
        "schema_version": "multi_alpha_recovery_reference_result_v1",
        "source_attempt_id": _required_text(
            source_attempt.get("attempt_id"),
            field="source_attempt.attempt_id",
        ),
        "source_result_manifest": source_result_manifest,
        "source_result_manifest_hash": source_result_manifest_hash,
        "source_artifact_manifest": _mapping_or_error(
            source_attempt.get("artifact_manifest_json") or {},
            field="source_attempt.artifact_manifest_json",
        ),
        "metrics": _mapping_or_error(payload.get("metrics"), field="source_result_payload.metrics"),
        "materialization_metadata": _mapping_or_error(
            payload.get("materialization_metadata"),
            field="source_result_payload.materialization_metadata",
        ),
        "business_formula_version": business_formula_version,
        "execution_disposition": execution_disposition,
    }


def _ownership_token(row: Mapping[str, Any]) -> OwnershipToken:
    return OwnershipToken(
        owner_id=str(row.get("owner_id") or ""),
        fencing_token=int(row.get("fencing_token") or 0),
        row_version=int(row.get("row_version") or 0),
    )


def _reason_code(error: Exception) -> str:
    return str(getattr(error, "reason_code", type(error).__name__))


def _error_context(error: Exception) -> dict[str, Any]:
    context = getattr(error, "context", {})
    return dict(context) if isinstance(context, Mapping) else {}


def _acquisition_suggestions_for_error(error: Exception) -> list[str]:
    reason_code = _reason_code(error)
    suggestions: list[str] = []
    if "result" in reason_code:
        suggestions.append("locate and verify the exact historical result manifest and metrics artifact")
    if "prediction" in reason_code or "artifact" in reason_code:
        suggestions.append("locate or republish the exact prediction artifact and verify SHA-256")
    if "identity" in reason_code or "materializer" in reason_code:
        suggestions.append("capture the frozen dataset/runtime/materializer identity from the owning QE environment")
    if not suggestions:
        suggestions.append("inspect the durable recovery event and acquire the exact missing source evidence")
    return suggestions


def _mapping_or_error(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DurableContractError(
            "frozen recovery value must be an object",
            reason_code="source_lineage_mismatch",
            context={"field": field, "value_type": type(value).__name__},
        )
    return dict(value)


def _mapping_sequence_or_error(value: Any, *, field: str) -> list[dict[str, Any]]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or any(not isinstance(item, Mapping) for item in value)
    ):
        raise DurableContractError(
            "frozen recovery value must be an array of objects",
            reason_code="source_lineage_mismatch",
            context={"field": field, "value_type": type(value).__name__},
        )
    return [dict(item) for item in value]


def _required_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DurableContractError(
            "recovery identity field must be a non-empty string",
            reason_code="multi_alpha_invalid_recovery_plan",
            context={"field": field, "value": value},
        )
    return value.strip()
