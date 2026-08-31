from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Sequence


TASK_SOURCE_KINDS = frozenset({"ui", "api", "mcp", "legacy_backfill"})
RUN_STATUSES = frozenset(
    {
        "queued",
        "preparing",
        "running",
        "pause_requested",
        "paused",
        "cancel_requested",
        "cancelling",
        "succeeded",
        "partial_failed",
        "partial_recovered",
        "failed",
        "cancelled",
    }
)
RUN_RECOVERY_KINDS = frozenset({"child_targeted"})
CHILD_KINDS = frozenset({"baseline", "scheme", "loo"})
CHILD_STATUSES = frozenset(
    {
        "pending",
        "materializing",
        "queued",
        "running",
        "reconciling",
        "cancel_requested",
        "cancelling",
        "succeeded",
        "not_computable",
        "not_recovered",
        "failed",
        "cancelled",
    }
)
CHILD_SOURCE_KINDS = frozenset({"runtime", "legacy_result_backfill", "recovery_reference"})
CHILD_EXECUTION_DISPOSITIONS = frozenset(
    {"execute", "reuse_result", "recompute_derived", "preserve_unavailable"}
)
ATTEMPT_RETRY_MODES = frozenset(
    {"initial", "backtest_only", "results_only", "rematerialize_and_backtest"}
)
ATTEMPT_STATUSES = frozenset(
    {"queued", "submitting", "running", "reconciling", "succeeded", "failed", "cancelled"}
)
ATTEMPT_EXECUTION_KINDS = frozenset({"remote_execution", "reference_result", "derived_result"})
CONTROL_ACTIONS = frozenset({"pause", "resume", "cancel", "reconcile", "attempt_cancel", "child_retry"})
COMMAND_STATUSES = frozenset({"accepted", "applying", "reconciling", "succeeded", "failed", "superseded"})
CANCEL_DELIVERY_STATUSES = frozenset({"pending", "sending", "reconciling", "succeeded", "failed"})
EVENT_TYPES = frozenset(
    {"created", "claimed", "submitted", "status", "log", "reconciled", "control", "result", "error", "terminal"}
)

_IDENTITY_COMPONENT = re.compile(r"^[A-Za-z0-9_.:-]+$")


class DurableContractError(ValueError):
    """Structured contract violation before any database write occurs."""

    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class RetryMode(str, Enum):
    INITIAL = "initial"
    BACKTEST_ONLY = "backtest_only"
    RESULTS_ONLY = "results_only"
    REMATERIALIZE_AND_BACKTEST = "rematerialize_and_backtest"


class ControlAction(str, Enum):
    PAUSE = "pause"
    RESUME = "resume"
    CANCEL = "cancel"
    RECONCILE = "reconcile"
    ATTEMPT_CANCEL = "attempt_cancel"
    CHILD_RETRY = "child_retry"


class ChildExecutionDisposition(str, Enum):
    EXECUTE = "execute"
    REUSE_RESULT = "reuse_result"
    RECOMPUTE_DERIVED = "recompute_derived"
    PRESERVE_UNAVAILABLE = "preserve_unavailable"


class AttemptExecutionKind(str, Enum):
    REMOTE_EXECUTION = "remote_execution"
    REFERENCE_RESULT = "reference_result"
    DERIVED_RESULT = "derived_result"


@dataclass(frozen=True)
class OwnershipToken:
    owner_id: str
    fencing_token: int
    row_version: int

    def __post_init__(self) -> None:
        _require_identity_component(self.owner_id, field="owner_id")
        if self.fencing_token < 1:
            raise DurableContractError(
                "fencing_token must be positive after a claim",
                reason_code="multi_alpha_invalid_ownership_token",
                context={"fencing_token": self.fencing_token},
            )
        if self.row_version < 1:
            raise DurableContractError(
                "row_version must be positive",
                reason_code="multi_alpha_invalid_ownership_token",
                context={"row_version": self.row_version},
            )


@dataclass(frozen=True)
class DurableTaskSpec:
    task_id: str
    task_name: str
    roster_hash: str
    roster: Sequence[Mapping[str, Any]]
    default_request: Mapping[str, Any]
    source_kind: str
    description: str | None = None
    legacy_group_key: str | None = None
    created_by: str | None = None

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.task_id, prefix="mact_", field="task_id")
        if not self.task_name.strip():
            raise DurableContractError(
                "task_name must not be empty",
                reason_code="multi_alpha_invalid_task",
            )
        if not self.roster_hash.strip():
            raise DurableContractError(
                "roster_hash must not be empty",
                reason_code="multi_alpha_invalid_task",
            )
        if (
            not isinstance(self.roster, Sequence)
            or isinstance(self.roster, (str, bytes))
            or any(not isinstance(item, Mapping) for item in self.roster)
        ):
            raise DurableContractError(
                "roster must be an array",
                reason_code="multi_alpha_invalid_task",
            )
        if not isinstance(self.default_request, Mapping):
            raise DurableContractError(
                "default_request must be an object",
                reason_code="multi_alpha_invalid_task",
            )
        _require_choice(self.source_kind, TASK_SOURCE_KINDS, field="source_kind")
        durable_task_identity_payload(
            roster_hash=self.roster_hash,
            roster=self.roster,
            default_request=self.default_request,
            legacy_group_key=self.legacy_group_key,
        )


@dataclass(frozen=True)
class DurableRunSpec:
    run_id: str
    task_id: str
    request_hash: str
    roster_hash: str
    roster: Sequence[Mapping[str, Any]]
    oos_start: date | str
    oos_end: date | str
    normalize_method: str
    walk_forward: Mapping[str, Any]
    backtest_config: Mapping[str, Any]
    baseline_leg_id: str | None = None
    retry_of_run_id: str | None = None
    node_parallelism: Mapping[str, Any] | None = None
    recovery_kind: str | None = None
    recovery_scope: Mapping[str, Any] | None = None
    recovery_scope_hash: str | None = None
    execution_identity: Mapping[str, Any] | None = None
    execution_identity_hash: str | None = None
    execution_identity_evidence: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.run_id, prefix="macb_", field="run_id")
        _require_prefixed_identity(self.task_id, prefix="mact_", field="task_id")
        _require_sha256(self.request_hash, field="request_hash")
        _require_choice(self.normalize_method, frozenset({"zscore", "rank"}), field="normalize_method")
        if self.retry_of_run_id is not None:
            _require_prefixed_identity(self.retry_of_run_id, prefix="macb_", field="retry_of_run_id")
        if self.recovery_kind is not None:
            _require_choice(self.recovery_kind, RUN_RECOVERY_KINDS, field="recovery_kind")
        if self.recovery_scope is not None and not isinstance(self.recovery_scope, Mapping):
            raise DurableContractError(
                "recovery_scope must be an object",
                reason_code="multi_alpha_invalid_recovery_scope",
            )
        recovery_scope = dict(self.recovery_scope or {})
        if self.recovery_kind is None:
            if recovery_scope or self.recovery_scope_hash is not None:
                raise DurableContractError(
                    "non-recovery run cannot carry recovery scope identity",
                    reason_code="multi_alpha_invalid_recovery_scope",
                )
        else:
            if self.retry_of_run_id is None or not recovery_scope or self.recovery_scope_hash is None:
                raise DurableContractError(
                    "child-targeted recovery requires source run and frozen recovery scope",
                    reason_code="multi_alpha_invalid_recovery_scope",
                )
            _require_sha256(self.recovery_scope_hash, field="recovery_scope_hash")
            expected_scope_hash = sha256_identity(recovery_scope)
            if self.recovery_scope_hash != expected_scope_hash:
                raise DurableContractError(
                    "recovery_scope_hash does not match canonical recovery scope",
                    reason_code="multi_alpha_identity_hash_mismatch",
                    context={
                        "field": "recovery_scope_hash",
                        "expected": expected_scope_hash,
                        "actual": self.recovery_scope_hash,
                    },
                )
        if self.execution_identity is not None and not isinstance(self.execution_identity, Mapping):
            raise DurableContractError(
                "execution_identity must be an object when present",
                reason_code="multi_alpha_execution_identity_invalid",
            )
        if self.execution_identity_evidence is not None and not isinstance(self.execution_identity_evidence, Mapping):
            raise DurableContractError(
                "execution_identity_evidence must be an object when present",
                reason_code="multi_alpha_execution_identity_invalid",
            )
        execution_identity = dict(self.execution_identity or {})
        if execution_identity:
            if self.execution_identity_hash is None:
                raise DurableContractError(
                    "execution_identity requires execution_identity_hash",
                    reason_code="multi_alpha_execution_identity_invalid",
                )
            _require_sha256(self.execution_identity_hash, field="execution_identity_hash")
            expected_execution_identity_hash = sha256_identity(execution_identity)
            if self.execution_identity_hash != expected_execution_identity_hash:
                raise DurableContractError(
                    "execution_identity_hash does not match canonical execution identity",
                    reason_code="multi_alpha_execution_identity_hash_mismatch",
                    context={
                        "expected": expected_execution_identity_hash,
                        "actual": self.execution_identity_hash,
                    },
                )
        elif self.execution_identity_hash is not None:
            raise DurableContractError(
                "execution_identity_hash cannot exist without execution_identity",
                reason_code="multi_alpha_execution_identity_invalid",
            )
        if self.execution_identity_evidence is not None:
            evidence = dict(self.execution_identity_evidence)
            complete = evidence.get("complete")
            if not isinstance(complete, bool):
                raise DurableContractError(
                    "execution_identity_evidence.complete must be boolean",
                    reason_code="multi_alpha_execution_identity_invalid",
                )
            if complete != bool(execution_identity):
                raise DurableContractError(
                    "execution identity and evidence completeness disagree",
                    reason_code="multi_alpha_execution_identity_invalid",
                )
        if _as_date(self.oos_end) < _as_date(self.oos_start):
            raise DurableContractError(
                "oos_end must be on or after oos_start",
                reason_code="multi_alpha_invalid_run_window",
                context={"oos_start": str(self.oos_start), "oos_end": str(self.oos_end)},
            )
        expected_hash = request_hash_for(self.canonical_request_payload())
        if self.request_hash != expected_hash:
            raise DurableContractError(
                "request_hash does not match the canonical durable run request",
                reason_code="multi_alpha_identity_hash_mismatch",
                context={"field": "request_hash", "expected": expected_hash, "actual": self.request_hash},
            )

    def canonical_request_payload(self) -> dict[str, Any]:
        return durable_run_request_payload(
            roster_hash=self.roster_hash,
            roster=self.roster,
            oos_start=self.oos_start,
            oos_end=self.oos_end,
            normalize_method=self.normalize_method,
            walk_forward=self.walk_forward,
            backtest_config=self.backtest_config,
            baseline_leg_id=self.baseline_leg_id,
            retry_of_run_id=self.retry_of_run_id,
            node_parallelism=self.node_parallelism,
            recovery_kind=self.recovery_kind,
            recovery_scope=self.recovery_scope,
            recovery_scope_hash=self.recovery_scope_hash,
            execution_identity=self.execution_identity,
            execution_identity_hash=self.execution_identity_hash,
            execution_identity_evidence=self.execution_identity_evidence,
        )


@dataclass(frozen=True)
class DurableChildSpec:
    child_id: str
    run_id: str
    child_key: str
    child_kind: str
    ordinal: int
    input_manifest: Mapping[str, Any]
    input_manifest_hash: str
    status: str = "pending"
    weighting_scheme: str | None = None
    dropped_leg_id: str | None = None
    prediction_artifact_uri: str | None = None
    prediction_artifact_hash: str | None = None
    source_kind: str = "runtime"
    source_child_id: str | None = None
    execution_disposition: str = "execute"
    source_lineage: Mapping[str, Any] | None = None
    source_lineage_hash: str | None = None

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.child_id, prefix="macbc_", field="child_id")
        _require_prefixed_identity(self.run_id, prefix="macb_", field="run_id")
        _require_identity_component(self.child_key, field="child_key")
        _require_choice(self.child_kind, CHILD_KINDS, field="child_kind")
        _require_choice(self.status, CHILD_STATUSES, field="status")
        _require_choice(self.source_kind, CHILD_SOURCE_KINDS, field="source_kind")
        _require_choice(
            self.execution_disposition,
            CHILD_EXECUTION_DISPOSITIONS,
            field="execution_disposition",
        )
        _require_sha256(self.input_manifest_hash, field="input_manifest_hash")
        expected_manifest_hash = artifact_manifest_hash_for(self.input_manifest)
        if self.input_manifest_hash != expected_manifest_hash:
            raise DurableContractError(
                "input_manifest_hash does not match the canonical child input manifest",
                reason_code="multi_alpha_identity_hash_mismatch",
                context={
                    "field": "input_manifest_hash",
                    "expected": expected_manifest_hash,
                    "actual": self.input_manifest_hash,
                },
            )
        if self.prediction_artifact_hash is not None:
            _require_sha256(self.prediction_artifact_hash, field="prediction_artifact_hash")
        if self.source_child_id is not None:
            _require_prefixed_identity(self.source_child_id, prefix="macbc_", field="source_child_id")
        if self.source_lineage is not None and not isinstance(self.source_lineage, Mapping):
            raise DurableContractError(
                "source_lineage must be an object",
                reason_code="multi_alpha_invalid_recovery_lineage",
            )
        if (self.source_lineage is None) != (self.source_lineage_hash is None):
            raise DurableContractError(
                "source_lineage and source_lineage_hash must be supplied together",
                reason_code="multi_alpha_invalid_recovery_lineage",
            )
        if self.source_lineage_hash is not None:
            _require_sha256(self.source_lineage_hash, field="source_lineage_hash")
            expected_lineage_hash = sha256_identity(dict(self.source_lineage or {}))
            if self.source_lineage_hash != expected_lineage_hash:
                raise DurableContractError(
                    "source_lineage_hash does not match canonical source lineage",
                    reason_code="multi_alpha_identity_hash_mismatch",
                    context={
                        "field": "source_lineage_hash",
                        "expected": expected_lineage_hash,
                        "actual": self.source_lineage_hash,
                    },
                )
        if self.source_child_id is None:
            if self.source_lineage is not None or self.execution_disposition != "execute":
                raise DurableContractError(
                    "child without source child cannot claim recovery lineage or result reuse",
                    reason_code="multi_alpha_invalid_recovery_lineage",
                )
            if self.source_kind == "recovery_reference":
                raise DurableContractError(
                    "recovery reference child requires source_child_id",
                    reason_code="multi_alpha_invalid_recovery_lineage",
                )
        elif self.source_lineage is None:
            raise DurableContractError(
                "recovery child requires frozen source lineage",
                reason_code="multi_alpha_invalid_recovery_lineage",
            )
        if self.source_kind == "recovery_reference" and self.execution_disposition == "execute":
            raise DurableContractError(
                "recovery reference child cannot be dispatched as a remote execution",
                reason_code="multi_alpha_invalid_recovery_lineage",
            )
        if self.status == "not_recovered" and self.execution_disposition != "preserve_unavailable":
            raise DurableContractError(
                "not_recovered child must preserve unavailable source evidence",
                reason_code="multi_alpha_invalid_recovery_lineage",
            )
        if self.ordinal < 0:
            raise DurableContractError(
                "child ordinal must be non-negative",
                reason_code="multi_alpha_invalid_child",
                context={"ordinal": self.ordinal},
            )
        if self.child_kind == "baseline" and (self.weighting_scheme is not None or self.dropped_leg_id is not None):
            raise DurableContractError(
                "baseline child cannot carry scheme or dropped leg",
                reason_code="multi_alpha_invalid_child",
            )
        if self.child_kind == "scheme" and (not self.weighting_scheme or self.dropped_leg_id is not None):
            raise DurableContractError(
                "scheme child requires weighting_scheme and no dropped leg",
                reason_code="multi_alpha_invalid_child",
            )
        if self.child_kind == "loo" and (not self.weighting_scheme or not self.dropped_leg_id):
            raise DurableContractError(
                "loo child requires weighting_scheme and dropped_leg_id",
                reason_code="multi_alpha_invalid_child",
            )


@dataclass(frozen=True)
class DurableAttemptSpec:
    attempt_id: str
    child_id: str
    attempt_no: int
    retry_mode: str
    retry_of_attempt_id: str | None = None
    run_id: str | None = None
    source_attempt_id: str | None = None
    execution_kind: str = "remote_execution"
    node_id: str | None = None
    qe_task_id: str | None = None
    qe_loop_id: str | None = None
    submission_intent_hash: str | None = None
    status: str = "queued"
    phase: str | None = None
    artifact_manifest: Mapping[str, Any] | None = None
    result_manifest: Mapping[str, Any] | None = None
    result_manifest_hash: str | None = None

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.attempt_id, prefix="macba_", field="attempt_id")
        _require_prefixed_identity(self.child_id, prefix="macbc_", field="child_id")
        if self.run_id is not None:
            _require_prefixed_identity(self.run_id, prefix="macb_", field="run_id")
        _require_choice(self.retry_mode, ATTEMPT_RETRY_MODES, field="retry_mode")
        _require_choice(self.status, ATTEMPT_STATUSES, field="status")
        _require_choice(self.execution_kind, ATTEMPT_EXECUTION_KINDS, field="execution_kind")
        if self.attempt_no < 1:
            raise DurableContractError(
                "attempt_no must be positive",
                reason_code="multi_alpha_invalid_attempt",
                context={"attempt_no": self.attempt_no},
            )
        if self.retry_of_attempt_id is not None:
            _require_prefixed_identity(self.retry_of_attempt_id, prefix="macba_", field="retry_of_attempt_id")
        if self.source_attempt_id is not None:
            _require_prefixed_identity(self.source_attempt_id, prefix="macba_", field="source_attempt_id")
        self._validate_lineage()
        if (self.qe_task_id is None) != (self.qe_loop_id is None):
            raise DurableContractError(
                "qe_task_id and qe_loop_id must both be present or both be absent",
                reason_code="multi_alpha_invalid_remote_identity",
            )
        if self.execution_kind != AttemptExecutionKind.REMOTE_EXECUTION.value:
            if any(
                value is not None
                for value in (self.qe_task_id, self.qe_loop_id, self.submission_intent_hash, self.node_id)
            ):
                raise DurableContractError(
                    "reference or derived result cannot carry remote execution identity",
                    reason_code="multi_alpha_invalid_remote_identity",
                )
            if self.result_manifest_hash is None:
                raise DurableContractError(
                    "reference or derived result requires verified result_manifest_hash",
                    reason_code="multi_alpha_invalid_attempt_result_manifest",
                )
            if self.status != "succeeded":
                raise DurableContractError(
                    "reference or derived result must be terminal succeeded",
                    reason_code="multi_alpha_invalid_attempt_lineage",
                )
        elif self.qe_task_id is None:
            if self.submission_intent_hash is not None:
                raise DurableContractError(
                    "submission_intent_hash requires a complete remote identity",
                    reason_code="multi_alpha_invalid_remote_identity",
                )
        else:
            if self.submission_intent_hash is None:
                raise DurableContractError(
                    "remote identity requires submission_intent_hash",
                    reason_code="multi_alpha_invalid_remote_identity",
                )
            _require_sha256(self.submission_intent_hash, field="submission_intent_hash")
            expected_intent_hash = submission_intent_hash_for(
                child_id=self.child_id,
                attempt_no=self.attempt_no,
                retry_mode=self.retry_mode,
                retry_of_attempt_id=self.retry_of_attempt_id,
                source_attempt_id=self.source_attempt_id,
                execution_kind=self.execution_kind,
                node_id=self.node_id,
                qe_task_id=self.qe_task_id,
                qe_loop_id=self.qe_loop_id,
            )
            if self.submission_intent_hash != expected_intent_hash:
                raise DurableContractError(
                    "submission_intent_hash does not match the canonical remote submission intent",
                    reason_code="multi_alpha_identity_hash_mismatch",
                    context={
                        "field": "submission_intent_hash",
                        "expected": expected_intent_hash,
                        "actual": self.submission_intent_hash,
                    },
                )
        if self.result_manifest_hash is not None:
            _require_sha256(self.result_manifest_hash, field="result_manifest_hash")
            if self.result_manifest is not None:
                expected_result_hash = artifact_manifest_hash_for(self.result_manifest)
                if self.result_manifest_hash != expected_result_hash:
                    raise DurableContractError(
                        "result_manifest_hash does not match canonical result manifest",
                        reason_code="multi_alpha_identity_hash_mismatch",
                        context={
                            "field": "result_manifest_hash",
                            "expected": expected_result_hash,
                            "actual": self.result_manifest_hash,
                        },
                    )

    def _validate_lineage(self) -> None:
        if self.execution_kind == AttemptExecutionKind.REMOTE_EXECUTION.value:
            if self.retry_mode == RetryMode.INITIAL.value:
                if (
                    self.attempt_no != 1
                    or self.retry_of_attempt_id is not None
                    or self.source_attempt_id is not None
                ):
                    raise DurableContractError(
                        "initial remote attempt must be attempt 1 without retry or source lineage",
                        reason_code="multi_alpha_invalid_attempt_lineage",
                    )
                return
            if self.source_attempt_id is not None:
                if self.attempt_no != 1 or self.retry_of_attempt_id is not None:
                    raise DurableContractError(
                        "successor remote attempt must be attempt 1 with source_attempt_id only",
                        reason_code="multi_alpha_invalid_attempt_lineage",
                    )
                if self.retry_mode == RetryMode.RESULTS_ONLY.value:
                    raise DurableContractError(
                        "results_only retry cannot create a remote successor attempt",
                        reason_code="multi_alpha_invalid_attempt_lineage",
                    )
                return
            if self.attempt_no <= 1 or self.retry_of_attempt_id is None:
                raise DurableContractError(
                    "same-child remote retry requires previous attempt lineage",
                    reason_code="multi_alpha_invalid_attempt_lineage",
                )
            if self.retry_mode == RetryMode.RESULTS_ONLY.value:
                raise DurableContractError(
                    "results_only retry cannot create a remote attempt",
                    reason_code="multi_alpha_invalid_attempt_lineage",
                )
            return

        if self.retry_mode != RetryMode.RESULTS_ONLY.value:
            raise DurableContractError(
                "reference or derived result is only valid for results_only recovery",
                reason_code="multi_alpha_invalid_attempt_lineage",
            )
        if self.source_attempt_id is not None:
            if self.attempt_no != 1 or self.retry_of_attempt_id is not None:
                raise DurableContractError(
                    "successor result reference must be attempt 1 with source_attempt_id only",
                    reason_code="multi_alpha_invalid_attempt_lineage",
                )
            return
        if (
            self.execution_kind != AttemptExecutionKind.REFERENCE_RESULT.value
            or self.attempt_no <= 1
            or self.retry_of_attempt_id is None
        ):
            raise DurableContractError(
                "in-place results_only recovery must append a reference result to a prior attempt",
                reason_code="multi_alpha_invalid_attempt_lineage",
            )


@dataclass(frozen=True)
class DurableCommandSpec:
    command_id: str
    run_id: str
    action: str
    target_key: str
    idempotency_key: str
    payload_hash: str
    request: Mapping[str, Any]
    requested_by: str
    child_id: str | None = None
    attempt_id: str | None = None
    scope: Mapping[str, Any] | None = None
    scope_hash: str | None = None

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.command_id, prefix="macmd_", field="command_id")
        _require_prefixed_identity(self.run_id, prefix="macb_", field="run_id")
        _require_choice(self.action, CONTROL_ACTIONS, field="action")
        _require_identity_component(self.target_key, field="target_key")
        _require_identity_component(self.idempotency_key, field="idempotency_key")
        _require_identity_component(self.requested_by, field="requested_by")
        _require_sha256(self.payload_hash, field="payload_hash")
        if not isinstance(self.request, Mapping):
            raise DurableContractError(
                "control command request must be an object",
                reason_code="multi_alpha_invalid_control_command",
            )
        if self.scope is not None and not isinstance(self.scope, Mapping):
            raise DurableContractError(
                "control command scope must be an object",
                reason_code="multi_alpha_invalid_control_command",
            )
        if (self.scope is None) != (self.scope_hash is None):
            raise DurableContractError(
                "control command scope and scope_hash must be supplied together",
                reason_code="multi_alpha_invalid_control_command",
            )
        if self.scope_hash is not None:
            _require_sha256(self.scope_hash, field="scope_hash")
            expected_scope_hash = sha256_identity(dict(self.scope or {}))
            if self.scope_hash != expected_scope_hash:
                raise DurableContractError(
                    "scope_hash does not match canonical command scope",
                    reason_code="multi_alpha_identity_hash_mismatch",
                    context={
                        "field": "scope_hash",
                        "expected": expected_scope_hash,
                        "actual": self.scope_hash,
                    },
                )
        expected_payload_hash = sha256_identity(
            control_command_payload(
                action=self.action,
                run_id=self.run_id,
                child_id=self.child_id,
                attempt_id=self.attempt_id,
                request=self.request,
                scope=self.scope,
            )
        )
        if self.payload_hash != expected_payload_hash:
            raise DurableContractError(
                "payload_hash does not match canonical control command payload",
                reason_code="multi_alpha_identity_hash_mismatch",
                context={
                    "field": "payload_hash",
                    "expected": expected_payload_hash,
                    "actual": self.payload_hash,
                },
            )
        if self.action in {"pause", "resume", "cancel", "reconcile"}:
            if self.child_id is not None or self.attempt_id is not None:
                raise DurableContractError(
                    "run control command cannot target child or attempt",
                    reason_code="multi_alpha_invalid_control_command",
                )
        elif self.action == "attempt_cancel":
            if self.child_id is None or self.attempt_id is None:
                raise DurableContractError(
                    "attempt_cancel requires exact child and attempt target",
                    reason_code="multi_alpha_invalid_control_command",
                )
        elif self.action == "child_retry":
            if self.child_id is None or self.attempt_id is not None:
                raise DurableContractError(
                    "child_retry requires child target without a fixed attempt",
                    reason_code="multi_alpha_invalid_control_command",
                )
        if self.child_id is not None:
            _require_prefixed_identity(self.child_id, prefix="macbc_", field="child_id")
        if self.attempt_id is not None:
            _require_prefixed_identity(self.attempt_id, prefix="macba_", field="attempt_id")
        expected_target_key = command_target_key_for(
            action=self.action,
            run_id=self.run_id,
            child_id=self.child_id,
            attempt_id=self.attempt_id,
        )
        if self.target_key != expected_target_key:
            raise DurableContractError(
                "target_key does not match canonical command target",
                reason_code="multi_alpha_identity_hash_mismatch",
                context={
                    "field": "target_key",
                    "expected": expected_target_key,
                    "actual": self.target_key,
                },
            )


@dataclass(frozen=True)
class DurableCancelDeliverySpec:
    delivery_id: str
    originating_command_id: str
    run_id: str
    child_id: str
    attempt_id: str
    node_id: str
    qe_task_id: str
    qe_loop_id: str
    submission_intent_hash: str
    kill_target_key: str
    expected_process_identity: Mapping[str, Any] | None = None
    expected_process_identity_hash: str | None = None
    kill_intent_generation: int = 1
    kill_intent_hash: str | None = None
    status: str = "pending"

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.delivery_id, prefix="macdl_", field="delivery_id")
        _require_prefixed_identity(self.originating_command_id, prefix="macmd_", field="originating_command_id")
        _require_prefixed_identity(self.run_id, prefix="macb_", field="run_id")
        _require_prefixed_identity(self.child_id, prefix="macbc_", field="child_id")
        _require_prefixed_identity(self.attempt_id, prefix="macba_", field="attempt_id")
        _require_identity_component(self.node_id, field="node_id")
        if not self.qe_task_id or not self.qe_loop_id:
            raise DurableContractError(
                "cancel delivery requires exact QE task and loop identity",
                reason_code="multi_alpha_invalid_cancel_delivery",
            )
        _require_sha256(self.submission_intent_hash, field="submission_intent_hash")
        _require_sha256(self.kill_target_key, field="kill_target_key")
        _require_choice(self.status, CANCEL_DELIVERY_STATUSES, field="status")
        if self.kill_intent_generation < 1:
            raise DurableContractError(
                "kill_intent_generation must be positive",
                reason_code="multi_alpha_invalid_cancel_delivery",
            )
        if self.kill_intent_hash is not None:
            _require_sha256(self.kill_intent_hash, field="kill_intent_hash")
        if (self.expected_process_identity is None) != (self.expected_process_identity_hash is None):
            raise DurableContractError(
                "process identity and process identity hash must be supplied together",
                reason_code="multi_alpha_invalid_cancel_delivery",
            )
        if self.expected_process_identity_hash is not None:
            _require_sha256(self.expected_process_identity_hash, field="expected_process_identity_hash")
            _validate_process_identity(self.expected_process_identity or {})
            expected_hash = sha256_identity(dict(self.expected_process_identity or {}))
            if self.expected_process_identity_hash != expected_hash:
                raise DurableContractError(
                    "expected_process_identity_hash does not match canonical process identity",
                    reason_code="multi_alpha_identity_hash_mismatch",
                    context={
                        "field": "expected_process_identity_hash",
                        "expected": expected_hash,
                        "actual": self.expected_process_identity_hash,
                    },
                )


def canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_identity(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def request_hash_for(request_payload: Mapping[str, Any]) -> str:
    return sha256_identity(request_payload)


def artifact_manifest_hash_for(manifest: Mapping[str, Any]) -> str:
    return sha256_identity(manifest)


def walk_forward_signature_for(walk_forward: Mapping[str, Any]) -> str:
    enabled = walk_forward.get("enabled", True)
    window = walk_forward.get("window", "na")
    min_periods = walk_forward.get("min_periods", "na")
    expanding = walk_forward.get("expanding", False)
    return (
        f"wf_w{window}_min{min_periods}_exp{str(bool(expanding)).lower()}_"
        f"en{str(bool(enabled)).lower()}"
    )


def implicit_task_group_key(
    *,
    roster_hash: str,
    normalize_method: str,
    walk_forward: Mapping[str, Any],
) -> str:
    if not roster_hash.strip() or not normalize_method.strip():
        raise DurableContractError(
            "implicit task identity requires roster_hash and normalize_method",
            reason_code="multi_alpha_invalid_task_identity",
            context={
                "roster_hash": roster_hash,
                "normalize_method": normalize_method,
            },
        )
    return f"{roster_hash}|{normalize_method}|{walk_forward_signature_for(walk_forward)}"


def durable_task_identity_payload(
    *,
    roster_hash: str,
    roster: Sequence[Mapping[str, Any]],
    default_request: Mapping[str, Any],
    legacy_group_key: str | None,
) -> dict[str, Any]:
    if (
        not isinstance(roster, Sequence)
        or isinstance(roster, (str, bytes))
        or any(not isinstance(item, Mapping) for item in roster)
    ):
        raise DurableContractError(
            "task identity roster must be an array of objects",
            reason_code="multi_alpha_invalid_task_identity",
        )
    if not isinstance(default_request, Mapping):
        raise DurableContractError(
            "task identity default_request must be an object",
            reason_code="multi_alpha_invalid_task_identity",
        )
    normalize_method = str(default_request.get("normalize_method") or "").strip()
    if normalize_method:
        _require_choice(normalize_method, frozenset({"zscore", "rank"}), field="normalize_method")
    raw_walk_forward = default_request.get("walk_forward")
    if raw_walk_forward is not None and not isinstance(raw_walk_forward, Mapping):
        raise DurableContractError(
            "task identity walk_forward must be an object",
            reason_code="multi_alpha_invalid_task_identity",
        )
    walk_forward = dict(raw_walk_forward) if isinstance(raw_walk_forward, Mapping) else {}
    computed_group_key = (
        implicit_task_group_key(
            roster_hash=roster_hash,
            normalize_method=normalize_method,
            walk_forward=walk_forward,
        )
        if normalize_method
        else None
    )
    if legacy_group_key is not None and computed_group_key is not None and legacy_group_key != computed_group_key:
        raise DurableContractError(
            "legacy_group_key does not match the immutable task identity",
            reason_code="multi_alpha_identity_hash_mismatch",
            context={
                "field": "legacy_group_key",
                "expected": computed_group_key,
                "actual": legacy_group_key,
            },
        )
    return {
        "roster_hash": roster_hash,
        "roster": [dict(item) for item in roster],
        "normalize_method": normalize_method or None,
        "walk_forward_signature": walk_forward_signature_for(walk_forward) if normalize_method else None,
        "group_key": computed_group_key or legacy_group_key,
    }


def durable_run_request_payload(
    *,
    roster_hash: str,
    roster: Sequence[Mapping[str, Any]],
    oos_start: date | str,
    oos_end: date | str,
    normalize_method: str,
    walk_forward: Mapping[str, Any],
    backtest_config: Mapping[str, Any],
    baseline_leg_id: str | None = None,
    retry_of_run_id: str | None = None,
    node_parallelism: Mapping[str, Any] | None = None,
    recovery_kind: str | None = None,
    recovery_scope: Mapping[str, Any] | None = None,
    recovery_scope_hash: str | None = None,
    execution_identity: Mapping[str, Any] | None = None,
    execution_identity_hash: str | None = None,
    execution_identity_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "roster_hash": roster_hash,
        "roster": [dict(item) for item in roster],
        "oos_start": _as_date(oos_start).isoformat(),
        "oos_end": _as_date(oos_end).isoformat(),
        "normalize_method": normalize_method,
        "walk_forward": dict(walk_forward),
        "backtest_config": dict(backtest_config),
        "baseline_leg_id": baseline_leg_id,
        "retry_of_run_id": retry_of_run_id,
        "node_parallelism": dict(node_parallelism or {}),
    }
    # Preserve the P0-1B request identity byte-for-byte for ordinary runs.
    # P0-2 recovery identity is additive only when the run is actually a
    # child-targeted successor.
    if recovery_kind is not None or recovery_scope or recovery_scope_hash is not None:
        payload.update(
            {
                "recovery_kind": recovery_kind,
                "recovery_scope": dict(recovery_scope or {}),
                "recovery_scope_hash": recovery_scope_hash,
            }
        )
    if execution_identity is not None or execution_identity_hash is not None or execution_identity_evidence is not None:
        payload.update(
            {
                "execution_identity": dict(execution_identity or {}),
                "execution_identity_hash": execution_identity_hash,
                "execution_identity_evidence": dict(execution_identity_evidence or {}),
            }
        )
    return payload


def submission_intent_payload(
    *,
    child_id: str,
    attempt_no: int,
    retry_mode: str,
    retry_of_attempt_id: str | None,
    source_attempt_id: str | None = None,
    execution_kind: str = "remote_execution",
    execution_identity_hash: str | None = None,
    node_id: str | None,
    qe_task_id: str,
    qe_loop_id: str,
) -> dict[str, Any]:
    payload = {
        "child_id": child_id,
        "attempt_no": attempt_no,
        "retry_mode": retry_mode,
        "retry_of_attempt_id": retry_of_attempt_id,
        "node_id": node_id,
        "qe_task_id": qe_task_id,
        "qe_loop_id": qe_loop_id,
    }
    # Preserve P0-1B remote intent hashes for ordinary remote attempts.
    # A successor must bind its source attempt into the remote intent identity.
    if source_attempt_id is not None or execution_kind != "remote_execution":
        payload.update(
            {
                "source_attempt_id": source_attempt_id,
                "execution_kind": execution_kind,
            }
        )
    if execution_identity_hash is not None:
        _require_sha256(execution_identity_hash, field="execution_identity_hash")
        payload["execution_identity_hash"] = execution_identity_hash
    return payload


def submission_intent_hash_for(
    *,
    child_id: str,
    attempt_no: int,
    retry_mode: str,
    retry_of_attempt_id: str | None,
    source_attempt_id: str | None = None,
    execution_kind: str = "remote_execution",
    execution_identity_hash: str | None = None,
    node_id: str | None,
    qe_task_id: str,
    qe_loop_id: str,
) -> str:
    return sha256_identity(
        submission_intent_payload(
            child_id=child_id,
            attempt_no=attempt_no,
            retry_mode=retry_mode,
            retry_of_attempt_id=retry_of_attempt_id,
            source_attempt_id=source_attempt_id,
            execution_kind=execution_kind,
            execution_identity_hash=execution_identity_hash,
            node_id=node_id,
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
        )
    )


def control_command_payload(
    *,
    action: str,
    run_id: str,
    child_id: str | None,
    attempt_id: str | None,
    request: Mapping[str, Any],
    scope: Mapping[str, Any] | None,
) -> dict[str, Any]:
    retry_mode = request.get("retry_mode")
    if retry_mode is not None:
        _require_choice(str(retry_mode), ATTEMPT_RETRY_MODES, field="request.retry_mode")
    return {
        "action": action,
        "run_id": run_id,
        "child_id": child_id,
        "attempt_id": attempt_id,
        "retry_mode": retry_mode,
        "request": dict(request),
        "scope": dict(scope) if scope is not None else None,
    }


def command_target_key_for(
    *,
    action: str,
    run_id: str,
    child_id: str | None = None,
    attempt_id: str | None = None,
) -> str:
    _require_choice(action, CONTROL_ACTIONS, field="action")
    _require_prefixed_identity(run_id, prefix="macb_", field="run_id")
    if child_id is not None:
        _require_prefixed_identity(child_id, prefix="macbc_", field="child_id")
    if attempt_id is not None:
        _require_prefixed_identity(attempt_id, prefix="macba_", field="attempt_id")
    return sha256_identity(
        {
            "action": action,
            "run_id": run_id,
            "child_id": child_id,
            "attempt_id": attempt_id,
        }
    )


def kill_target_key_for(
    *,
    node_id: str,
    qe_task_id: str,
    qe_loop_id: str,
    submission_intent_hash: str,
) -> str:
    _require_identity_component(node_id, field="node_id")
    if not qe_task_id or not qe_loop_id:
        raise DurableContractError(
            "kill target requires exact QE task and loop identity",
            reason_code="multi_alpha_invalid_cancel_delivery",
        )
    _require_sha256(submission_intent_hash, field="submission_intent_hash")
    return sha256_identity(
        {
            "node_id": node_id,
            "qe_task_id": qe_task_id,
            "qe_loop_id": qe_loop_id,
            "submission_intent_hash": submission_intent_hash,
        }
    )


def process_identity_hash_for(process_identity: Mapping[str, Any]) -> str:
    _validate_process_identity(process_identity)
    return sha256_identity(dict(process_identity))


def kill_intent_hash_for(
    *,
    kill_target_key: str,
    process_identity_hash: str | None,
    generation: int,
) -> str:
    _require_sha256(kill_target_key, field="kill_target_key")
    if process_identity_hash is not None:
        _require_sha256(process_identity_hash, field="process_identity_hash")
    if generation < 1:
        raise DurableContractError(
            "kill intent generation must be positive",
            reason_code="multi_alpha_invalid_cancel_delivery",
        )
    return sha256_identity(
        {
            "kill_target_key": kill_target_key,
            "process_identity_hash": process_identity_hash,
            "generation": generation,
        }
    )


def make_task_id() -> str:
    return f"mact_{uuid.uuid4().hex}"


def make_legacy_task_id(legacy_group_key: str) -> str:
    if not legacy_group_key:
        raise DurableContractError(
            "legacy_group_key must not be empty",
            reason_code="multi_alpha_invalid_legacy_group_key",
        )
    return f"mact_legacy_{hashlib.sha256(legacy_group_key.encode('utf-8')).hexdigest()[:24]}"


def make_implicit_task_id(group_key: str) -> str:
    if not group_key:
        raise DurableContractError(
            "implicit task group key must not be empty",
            reason_code="multi_alpha_invalid_legacy_group_key",
        )
    return f"mact_auto_{hashlib.sha256(group_key.encode('utf-8')).hexdigest()[:32]}"


def make_child_id(run_id: str, child_key: str) -> str:
    _require_prefixed_identity(run_id, prefix="macb_", field="run_id")
    _require_identity_component(child_key, field="child_key")
    digest = hashlib.sha256(f"{run_id}|{child_key}".encode("utf-8")).hexdigest()
    return f"macbc_{digest}"


def make_attempt_id(child_id: str, attempt_no: int) -> str:
    _require_prefixed_identity(child_id, prefix="macbc_", field="child_id")
    if attempt_no < 1:
        raise DurableContractError(
            "attempt_no must be positive",
            reason_code="multi_alpha_invalid_attempt",
            context={"attempt_no": attempt_no},
        )
    digest = hashlib.sha256(f"{child_id}|{attempt_no}".encode("utf-8")).hexdigest()
    return f"macba_{digest}"


def make_command_id(run_id: str, idempotency_key: str) -> str:
    _require_prefixed_identity(run_id, prefix="macb_", field="run_id")
    _require_identity_component(idempotency_key, field="idempotency_key")
    digest = hashlib.sha256(f"{run_id}|{idempotency_key}".encode("utf-8")).hexdigest()
    return f"macmd_{digest}"


def make_cancel_delivery_id(kill_target_key: str) -> str:
    _require_sha256(kill_target_key, field="kill_target_key")
    return f"macdl_{kill_target_key}"


def make_successor_run_id(
    *,
    source_run_id: str,
    command_id: str,
    scope_hash: str,
) -> str:
    _require_prefixed_identity(source_run_id, prefix="macb_", field="source_run_id")
    _require_prefixed_identity(command_id, prefix="macmd_", field="command_id")
    _require_sha256(scope_hash, field="scope_hash")
    digest = sha256_identity(
        {
            "source_run_id": source_run_id,
            "command_id": command_id,
            "scope_hash": scope_hash,
        }
    )
    return f"macb_recovery_{digest}"


def make_remote_task_id(run_id: str, child_id: str, attempt_no: int) -> str:
    _require_prefixed_identity(run_id, prefix="macb_", field="run_id")
    _require_prefixed_identity(child_id, prefix="macbc_", field="child_id")
    if attempt_no < 1:
        raise DurableContractError(
            "attempt_no must be positive",
            reason_code="multi_alpha_invalid_attempt",
        )
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:12]
    child_hash = hashlib.sha256(child_id.encode("utf-8")).hexdigest()[:12]
    return f"macb_remote_{run_hash}_{child_hash}_a{attempt_no}"


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DurableContractError(
                "non-finite float is not valid in an identity payload",
                reason_code="multi_alpha_noncanonical_identity_payload",
                context={"value": repr(value)},
            )
        return value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise DurableContractError(
        "value is not canonical-JSON serializable",
        reason_code="multi_alpha_noncanonical_identity_payload",
        context={"type": type(value).__name__},
    )


def _as_date(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DurableContractError(
            "date must use ISO YYYY-MM-DD",
            reason_code="multi_alpha_invalid_date",
            context={"value": value},
        ) from exc


def _require_choice(value: str, allowed: frozenset[str], *, field: str) -> None:
    if value not in allowed:
        raise DurableContractError(
            f"{field} has unsupported value",
            reason_code="multi_alpha_invalid_contract_value",
            context={"field": field, "value": value, "allowed": sorted(allowed)},
        )


def _require_prefixed_identity(value: str, *, prefix: str, field: str) -> None:
    if not value.startswith(prefix) or not _IDENTITY_COMPONENT.fullmatch(value):
        raise DurableContractError(
            f"{field} has invalid identity format",
            reason_code="multi_alpha_invalid_identity",
            context={"field": field, "value": value, "prefix": prefix},
        )


def _require_identity_component(value: str, *, field: str) -> None:
    if not value or not _IDENTITY_COMPONENT.fullmatch(value):
        raise DurableContractError(
            f"{field} contains unsupported characters",
            reason_code="multi_alpha_invalid_identity_component",
            context={"field": field, "value": value},
        )


def _require_sha256(value: str, *, field: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise DurableContractError(
            f"{field} must be a lowercase SHA-256 hex digest",
            reason_code="multi_alpha_invalid_hash",
            context={"field": field, "value": value},
        )


def _validate_process_identity(value: Mapping[str, Any]) -> None:
    if not isinstance(value, Mapping):
        raise DurableContractError(
            "process identity must be an object",
            reason_code="multi_alpha_invalid_process_identity",
        )
    required_values: dict[str, Any] = {
        "pid": value.get("pid"),
        "pgid": value.get("pgid"),
        "start_time_ticks": value.get("start_time_ticks"),
    }
    for field, raw in required_values.items():
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
            raise DurableContractError(
                "process identity requires positive integer pid, pgid, and start_time_ticks",
                reason_code="multi_alpha_invalid_process_identity",
                context={"field": field, "value": raw},
            )
