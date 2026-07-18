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
        "failed",
        "cancelled",
    }
)
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
        "failed",
        "cancelled",
    }
)
CHILD_SOURCE_KINDS = frozenset({"runtime", "legacy_result_backfill"})
ATTEMPT_RETRY_MODES = frozenset(
    {"initial", "backtest_only", "results_only", "rematerialize_and_backtest"}
)
ATTEMPT_STATUSES = frozenset(
    {"queued", "submitting", "running", "reconciling", "succeeded", "failed", "cancelled"}
)
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
        _require_choice(self.source_kind, TASK_SOURCE_KINDS, field="source_kind")


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

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.run_id, prefix="macb_", field="run_id")
        _require_prefixed_identity(self.task_id, prefix="mact_", field="task_id")
        _require_sha256(self.request_hash, field="request_hash")
        _require_choice(self.normalize_method, frozenset({"zscore", "rank"}), field="normalize_method")
        if self.retry_of_run_id is not None:
            _require_prefixed_identity(self.retry_of_run_id, prefix="macb_", field="retry_of_run_id")
        if _as_date(self.oos_end) < _as_date(self.oos_start):
            raise DurableContractError(
                "oos_end must be on or after oos_start",
                reason_code="multi_alpha_invalid_run_window",
                context={"oos_start": str(self.oos_start), "oos_end": str(self.oos_end)},
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

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.child_id, prefix="macbc_", field="child_id")
        _require_prefixed_identity(self.run_id, prefix="macb_", field="run_id")
        _require_identity_component(self.child_key, field="child_key")
        _require_choice(self.child_kind, CHILD_KINDS, field="child_kind")
        _require_choice(self.status, CHILD_STATUSES, field="status")
        _require_choice(self.source_kind, CHILD_SOURCE_KINDS, field="source_kind")
        _require_sha256(self.input_manifest_hash, field="input_manifest_hash")
        if self.prediction_artifact_hash is not None:
            _require_sha256(self.prediction_artifact_hash, field="prediction_artifact_hash")
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
    node_id: str | None = None
    qe_task_id: str | None = None
    qe_loop_id: str | None = None
    submission_intent_hash: str | None = None
    status: str = "queued"
    phase: str | None = None
    artifact_manifest: Mapping[str, Any] | None = None
    result_manifest: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        _require_prefixed_identity(self.attempt_id, prefix="macba_", field="attempt_id")
        _require_prefixed_identity(self.child_id, prefix="macbc_", field="child_id")
        _require_choice(self.retry_mode, ATTEMPT_RETRY_MODES, field="retry_mode")
        _require_choice(self.status, ATTEMPT_STATUSES, field="status")
        if self.attempt_no < 1:
            raise DurableContractError(
                "attempt_no must be positive",
                reason_code="multi_alpha_invalid_attempt",
                context={"attempt_no": self.attempt_no},
            )
        if self.retry_mode == RetryMode.INITIAL.value:
            if self.attempt_no != 1 or self.retry_of_attempt_id is not None:
                raise DurableContractError(
                    "initial attempt must be attempt 1 without retry lineage",
                    reason_code="multi_alpha_invalid_attempt_lineage",
                )
        elif self.attempt_no == 1 or self.retry_of_attempt_id is None:
            raise DurableContractError(
                "retry attempt requires a previous attempt",
                reason_code="multi_alpha_invalid_attempt_lineage",
            )
        if self.retry_of_attempt_id is not None:
            _require_prefixed_identity(self.retry_of_attempt_id, prefix="macba_", field="retry_of_attempt_id")
        if (self.qe_task_id is None) != (self.qe_loop_id is None):
            raise DurableContractError(
                "qe_task_id and qe_loop_id must both be present or both be absent",
                reason_code="multi_alpha_invalid_remote_identity",
            )
        if self.submission_intent_hash is not None:
            _require_sha256(self.submission_intent_hash, field="submission_intent_hash")


def canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_identity(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def request_hash_for(request_payload: Mapping[str, Any]) -> str:
    return sha256_identity(request_payload)


def artifact_manifest_hash_for(manifest: Mapping[str, Any]) -> str:
    return sha256_identity(manifest)


def make_task_id() -> str:
    return f"mact_{uuid.uuid4().hex}"


def make_legacy_task_id(legacy_group_key: str) -> str:
    if not legacy_group_key:
        raise DurableContractError(
            "legacy_group_key must not be empty",
            reason_code="multi_alpha_invalid_legacy_group_key",
        )
    return f"mact_legacy_{hashlib.sha256(legacy_group_key.encode('utf-8')).hexdigest()[:24]}"


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
    child_suffix = child_id.removeprefix("macbc_")[-16:]
    return f"macba_{child_suffix}_{attempt_no}_{uuid.uuid4().hex}"


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
