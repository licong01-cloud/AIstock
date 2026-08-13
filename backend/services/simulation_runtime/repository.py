"""Runtime release and simulation binding repositories."""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
import math
from typing import Any, Callable, Iterable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.tca_sidecar import (
    TCA_OBSERVATION_KEY,
    CaptureMergeOutcome,
    merge_parent_first_write,
    new_run_tca_sidecar,
    preserve_tca_sidecar,
)
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .models import (
    DailySelectionEvidence,
    ExecutionPlan,
    ExecutionPlanIntent,
    LocalSimEconomicReceiptV1,
    LocalSimExecutionStateV1,
    LocalSimProjectionOutboxStatus,
    LocalSimProjectionOutboxV1,
    LocalSimProjectionReceiptV1,
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    TradingRuleDecision,
    canonical_json_sha256,
)

ConnFactory = Callable[[], Iterator[Any]]
LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY = "local_sim_execution_states_v1"
LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY = "local_sim_economic_receipts_v1"
LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY = "local_sim_projection_outbox_v1"
LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY = "local_sim_projection_receipts_v1"
LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY = "local_sim_economic_generation"
LOCAL_SIM_PROJECTION_TERMINAL_FAILURE_PAYLOAD_KEY = "local_sim_projection_terminal_failure"
LOCAL_SIM_PROJECTION_GENERATION_PAYLOAD_KEY = "local_sim_projection_generation"
LOCAL_SIM_PROJECTION_READBACK_FAILURE_PAYLOAD_KEY = "local_sim_projection_readback_failure"
LOCAL_SIM_PROJECTION_READBACK_TERMINAL_FAILURE_PAYLOAD_KEY = "local_sim_projection_readback_terminal_failure"
LOCAL_SIM_VALUATION_PENDING_PAYLOAD_KEY = "local_sim_valuation_pending_v1"
LOCAL_SIM_VALUATION_COMPLETION_PAYLOAD_KEY = "local_sim_valuation_completion_v1"
SIMULATION_SCHEDULER_RETRY_CONTROL_PAYLOAD_KEY = "simulation_scheduler_retry_control_v1"
SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA = "simulation_scheduler_retry_control_v1"
SIMULATION_SCHEDULER_RETRY_ENTRY_SCHEMA = "simulation_scheduler_retry_entry_v1"
SIMULATION_SCHEDULER_RETRY_CLAIMS_PAYLOAD_KEY = "simulation_scheduler_retry_claims_v1"
SIMULATION_SCHEDULER_RETRY_CLAIMS_SCHEMA = "simulation_scheduler_retry_claims_v1"
SIMULATION_SCHEDULER_RETRY_CLAIM_SCHEMA = "simulation_scheduler_retry_claim_v1"
_SIMULATION_SCHEDULER_RETRY_MAX_ENTRIES = 16
_SIMULATION_SCHEDULER_RETRY_ENTRY_FIELDS = frozenset(
    {
        "schema_version",
        "retry_key",
        "source_fingerprint",
        "failure_fingerprint",
        "failure_stage",
        "consecutive_failure_count",
        "attempt_count",
        "first_failed_at",
        "last_failed_at",
        "next_retry_at",
        "last_attempt_at",
        "attempt_lease_until",
        "last_error",
        "entry_sha256",
    }
)
_SIMULATION_SCHEDULER_RETRY_CLAIM_FIELDS = frozenset(
    {
        "schema_version",
        "retry_key",
        "source_fingerprint",
        "claimed_at",
        "lease_until",
        "claim_token",
    }
)


@dataclass(frozen=True)
class SimulationRetryAttemptDecision:
    run: SimulationDailyRun
    should_execute: bool
    reason: str
    retry_entry: dict[str, Any] | None
    claim_token: str | None


def _retry_required_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidStateTransitionError(
            "simulation scheduler retry control text field is invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                "field": field,
                "actual_type": type(value).__name__,
            },
        )
    return value.strip()


def _retry_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise InvalidStateTransitionError(
            "simulation scheduler retry control string field is invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                "field": field,
                "actual_type": type(value).__name__,
            },
        )
    return value


def _retry_as_of_time(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise InvalidStateTransitionError(
            "simulation scheduler retry as-of time must be timezone-aware",
            context={"reason_code": "SIMULATION_SCHEDULER_RETRY_AS_OF_TIME_INVALID"},
        )
    return value.astimezone(UTC)


def _retry_sha256(value: Any, *, field: str) -> str:
    normalized = _retry_required_text(value, field=field).lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise InvalidStateTransitionError(
            "simulation scheduler retry control hash is invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                "field": field,
                "actual": normalized,
            },
        )
    return normalized


def _retry_time(value: Any, *, field: str, optional: bool = False) -> datetime | None:
    if value is None and optional:
        return None
    text = _retry_required_text(value, field=field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidStateTransitionError(
            "simulation scheduler retry control timestamp is invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                "field": field,
                "actual": text,
            },
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidStateTransitionError(
            "simulation scheduler retry control timestamp must be timezone-aware",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                "field": field,
                "actual": text,
            },
        )
    return parsed.astimezone(UTC)


def _retry_positive_int(value: Any, *, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise InvalidStateTransitionError(
            "simulation scheduler retry control counter is invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                "field": field,
                "actual": value,
            },
        )
    return value


def _retry_json_safe(value: Any) -> Any:
    if value is None or type(value) in {bool, int, str}:
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in sorted(value.items(), key=lambda pair: str(pair[0])):
            normalized_key = str(key)
            if normalized_key in normalized:
                raise InvalidStateTransitionError(
                    "simulation scheduler retry error context has duplicate normalized keys",
                    context={
                        "reason_code": "SIMULATION_SCHEDULER_RETRY_ERROR_CONTEXT_KEY_CONFLICT",
                        "key": normalized_key,
                    },
                )
            normalized[normalized_key] = _retry_json_safe(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_retry_json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_retry_json_safe(item) for item in value]
        return sorted(normalized, key=lambda item: repr(item))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return _retry_json_safe(value.value)
    if isinstance(value, bytes):
        return {
            "schema_version": "simulation_scheduler_retry_binary_evidence_v1",
            "hex": value.hex(),
        }
    return {
        "schema_version": "simulation_scheduler_retry_unsupported_evidence_v1",
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
    }


def simulation_retry_json_safe_evidence(value: Any) -> Any:
    """Normalize retry diagnostics before both identity hashing and persistence."""

    return _retry_json_safe(value)


def _retry_entry_hash_payload(entry: dict[str, Any]) -> dict[str, Any]:
    return {key: entry[key] for key in sorted(_SIMULATION_SCHEDULER_RETRY_ENTRY_FIELDS - {"entry_sha256"})}


def _retry_entry_with_hash(entry: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(entry)
    normalized["entry_sha256"] = canonical_json_sha256(_retry_entry_hash_payload(normalized))
    return normalized


def _retry_control_with_hash(entries: dict[str, dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_version": SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA,
        "entries": {key: entries[key] for key in sorted(entries)},
    }
    return {**payload, "control_sha256": canonical_json_sha256(payload)}


def _simulation_retry_control(payload: dict[str, Any]) -> dict[str, Any] | None:
    raw = payload.get(SIMULATION_SCHEDULER_RETRY_CONTROL_PAYLOAD_KEY)
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) != {"schema_version", "entries", "control_sha256"}:
        raise InvalidStateTransitionError(
            "simulation scheduler retry control envelope is invalid",
            context={"reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"},
        )
    if raw.get("schema_version") != SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA:
        raise InvalidStateTransitionError(
            "simulation scheduler retry control schema version is invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                "actual_schema_version": raw.get("schema_version"),
            },
        )
    entries = raw.get("entries")
    if not isinstance(entries, dict) or len(entries) > _SIMULATION_SCHEDULER_RETRY_MAX_ENTRIES:
        raise InvalidStateTransitionError(
            "simulation scheduler retry control entries are invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                "entry_count": len(entries) if isinstance(entries, dict) else None,
            },
        )
    normalized_entries: dict[str, dict[str, Any]] = {}
    for raw_key, raw_entry in entries.items():
        retry_key = _retry_required_text(raw_key, field="retry_key")
        if not isinstance(raw_entry, dict) or set(raw_entry) != _SIMULATION_SCHEDULER_RETRY_ENTRY_FIELDS:
            raise InvalidStateTransitionError(
                "simulation scheduler retry entry is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                    "retry_key": retry_key,
                },
            )
        entry = dict(raw_entry)
        if entry.get("schema_version") != SIMULATION_SCHEDULER_RETRY_ENTRY_SCHEMA:
            raise InvalidStateTransitionError(
                "simulation scheduler retry entry schema version is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                    "retry_key": retry_key,
                },
            )
        if _retry_required_text(entry.get("retry_key"), field="retry_key") != retry_key:
            raise InvalidStateTransitionError(
                "simulation scheduler retry entry map identity conflicts with its payload",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_IDENTITY_CONFLICT",
                    "map_retry_key": retry_key,
                    "payload_retry_key": entry.get("retry_key"),
                },
            )
        _retry_sha256(entry.get("source_fingerprint"), field="source_fingerprint")
        _retry_sha256(entry.get("failure_fingerprint"), field="failure_fingerprint")
        _retry_required_text(entry.get("failure_stage"), field="failure_stage")
        failure_count = _retry_positive_int(entry.get("consecutive_failure_count"), field="consecutive_failure_count")
        attempt_count = _retry_positive_int(entry.get("attempt_count"), field="attempt_count")
        first_failed_at = _retry_time(entry.get("first_failed_at"), field="first_failed_at")
        last_failed_at = _retry_time(entry.get("last_failed_at"), field="last_failed_at")
        next_retry_at = _retry_time(entry.get("next_retry_at"), field="next_retry_at")
        last_attempt_at = _retry_time(entry.get("last_attempt_at"), field="last_attempt_at", optional=True)
        lease_until = _retry_time(entry.get("attempt_lease_until"), field="attempt_lease_until", optional=True)
        if first_failed_at is None or last_failed_at is None or next_retry_at is None:
            raise AssertionError("required retry timestamps were not parsed")
        if last_failed_at < first_failed_at or next_retry_at < last_failed_at or attempt_count < failure_count:
            raise InvalidStateTransitionError(
                "simulation scheduler retry entry timeline is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_TIMELINE_INVALID",
                    "retry_key": retry_key,
                },
            )
        if (last_attempt_at is None) != (lease_until is None) or (
            last_attempt_at is not None and lease_until is not None and lease_until < last_attempt_at
        ):
            raise InvalidStateTransitionError(
                "simulation scheduler retry attempt lease is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_TIMELINE_INVALID",
                    "retry_key": retry_key,
                },
            )
        last_error = entry.get("last_error")
        if not isinstance(last_error, dict) or set(last_error) != {"type", "message", "reason_code", "context"}:
            raise InvalidStateTransitionError(
                "simulation scheduler retry error evidence is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID",
                    "retry_key": retry_key,
                },
            )
        _retry_required_text(last_error.get("type"), field="last_error.type")
        _retry_string(last_error.get("message"), field="last_error.message")
        expected_entry_hash = canonical_json_sha256(_retry_entry_hash_payload(entry))
        if _retry_sha256(entry.get("entry_sha256"), field="entry_sha256") != expected_entry_hash:
            raise InvalidStateTransitionError(
                "simulation scheduler retry entry hash drifted",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_HASH_DRIFT",
                    "retry_key": retry_key,
                    "expected": expected_entry_hash,
                    "actual": entry.get("entry_sha256"),
                },
            )
        normalized_entries[retry_key] = entry
    expected_control = _retry_control_with_hash(normalized_entries)
    if _retry_sha256(raw.get("control_sha256"), field="control_sha256") != expected_control["control_sha256"]:
        raise InvalidStateTransitionError(
            "simulation scheduler retry control hash drifted",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_HASH_DRIFT",
                "expected": expected_control["control_sha256"],
                "actual": raw.get("control_sha256"),
            },
        )
    return expected_control


def _retry_control_payload_with_entries(payload: dict[str, Any], entries: dict[str, dict[str, Any]]) -> dict[str, Any]:
    merged = dict(payload)
    if entries:
        merged[SIMULATION_SCHEDULER_RETRY_CONTROL_PAYLOAD_KEY] = _retry_control_with_hash(entries)
    else:
        merged.pop(SIMULATION_SCHEDULER_RETRY_CONTROL_PAYLOAD_KEY, None)
    return merged


def _retry_claim_hash_payload(claim: dict[str, Any]) -> dict[str, Any]:
    return {key: claim[key] for key in sorted(_SIMULATION_SCHEDULER_RETRY_CLAIM_FIELDS - {"claim_token"})}


def _retry_claim_with_token(claim: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(claim)
    normalized["claim_token"] = canonical_json_sha256(_retry_claim_hash_payload(normalized))
    return normalized


def _retry_claims_with_hash(claims: dict[str, dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_version": SIMULATION_SCHEDULER_RETRY_CLAIMS_SCHEMA,
        "claims": {key: claims[key] for key in sorted(claims)},
    }
    return {**payload, "claims_sha256": canonical_json_sha256(payload)}


def _simulation_retry_claims(payload: dict[str, Any]) -> dict[str, Any] | None:
    raw = payload.get(SIMULATION_SCHEDULER_RETRY_CLAIMS_PAYLOAD_KEY)
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) != {"schema_version", "claims", "claims_sha256"}:
        raise InvalidStateTransitionError(
            "simulation scheduler retry claims envelope is invalid",
            context={"reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIMS_SCHEMA_INVALID"},
        )
    if raw.get("schema_version") != SIMULATION_SCHEDULER_RETRY_CLAIMS_SCHEMA:
        raise InvalidStateTransitionError(
            "simulation scheduler retry claims schema version is invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIMS_SCHEMA_INVALID",
                "actual_schema_version": raw.get("schema_version"),
            },
        )
    claims = raw.get("claims")
    if not isinstance(claims, dict) or len(claims) > _SIMULATION_SCHEDULER_RETRY_MAX_ENTRIES:
        raise InvalidStateTransitionError(
            "simulation scheduler retry claims are invalid",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIMS_SCHEMA_INVALID",
                "claim_count": len(claims) if isinstance(claims, dict) else None,
            },
        )
    normalized_claims: dict[str, dict[str, Any]] = {}
    for raw_key, raw_claim in claims.items():
        retry_key = _retry_required_text(raw_key, field="retry_key")
        if not isinstance(raw_claim, dict) or set(raw_claim) != _SIMULATION_SCHEDULER_RETRY_CLAIM_FIELDS:
            raise InvalidStateTransitionError(
                "simulation scheduler retry claim is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIMS_SCHEMA_INVALID",
                    "retry_key": retry_key,
                },
            )
        claim = dict(raw_claim)
        if claim.get("schema_version") != SIMULATION_SCHEDULER_RETRY_CLAIM_SCHEMA:
            raise InvalidStateTransitionError(
                "simulation scheduler retry claim schema version is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIMS_SCHEMA_INVALID",
                    "retry_key": retry_key,
                },
            )
        if _retry_required_text(claim.get("retry_key"), field="retry_key") != retry_key:
            raise InvalidStateTransitionError(
                "simulation scheduler retry claim map identity conflicts with its payload",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_IDENTITY_CONFLICT",
                    "map_retry_key": retry_key,
                    "payload_retry_key": claim.get("retry_key"),
                },
            )
        _retry_sha256(claim.get("source_fingerprint"), field="source_fingerprint")
        claimed_at = _retry_time(claim.get("claimed_at"), field="claimed_at")
        lease_until = _retry_time(claim.get("lease_until"), field="lease_until")
        if claimed_at is None or lease_until is None:
            raise AssertionError("required retry claim timestamps were not parsed")
        if lease_until < claimed_at:
            raise InvalidStateTransitionError(
                "simulation scheduler retry claim timeline is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_TIMELINE_INVALID",
                    "retry_key": retry_key,
                },
            )
        expected_token = canonical_json_sha256(_retry_claim_hash_payload(claim))
        if _retry_sha256(claim.get("claim_token"), field="claim_token") != expected_token:
            raise InvalidStateTransitionError(
                "simulation scheduler retry claim token drifted",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_HASH_DRIFT",
                    "retry_key": retry_key,
                    "expected": expected_token,
                    "actual": claim.get("claim_token"),
                },
            )
        normalized_claims[retry_key] = claim
    expected_claims = _retry_claims_with_hash(normalized_claims)
    if _retry_sha256(raw.get("claims_sha256"), field="claims_sha256") != expected_claims["claims_sha256"]:
        raise InvalidStateTransitionError(
            "simulation scheduler retry claims hash drifted",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_HASH_DRIFT",
                "expected": expected_claims["claims_sha256"],
                "actual": raw.get("claims_sha256"),
            },
        )
    return expected_claims


def _retry_claims_payload_with_claims(payload: dict[str, Any], claims: dict[str, dict[str, Any]]) -> dict[str, Any]:
    merged = dict(payload)
    if claims:
        merged[SIMULATION_SCHEDULER_RETRY_CLAIMS_PAYLOAD_KEY] = _retry_claims_with_hash(claims)
    else:
        merged.pop(SIMULATION_SCHEDULER_RETRY_CLAIMS_PAYLOAD_KEY, None)
    return merged


def _simulation_retry_state(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    control = _simulation_retry_control(payload)
    claims = _simulation_retry_claims(payload)
    entry_keys = set(control["entries"]) if control is not None else set()
    claim_keys = set(claims["claims"]) if claims is not None else set()
    overlap = sorted(entry_keys & claim_keys)
    if overlap:
        raise InvalidStateTransitionError(
            "simulation scheduler retry failure entries and initial claims overlap",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_IDENTITY_CONFLICT",
                "retry_keys": overlap,
            },
        )
    return control, claims


def _new_retry_initial_claim(
    *, retry_key: str, source_fingerprint: str, as_of_time: datetime, lease_seconds: int
) -> dict[str, Any]:
    return _retry_claim_with_token(
        {
            "schema_version": SIMULATION_SCHEDULER_RETRY_CLAIM_SCHEMA,
            "retry_key": retry_key,
            "source_fingerprint": source_fingerprint,
            "claimed_at": as_of_time.isoformat(),
            "lease_until": (as_of_time + timedelta(seconds=lease_seconds)).isoformat(),
        }
    )


def _claim_retry_attempt_payload(
    *,
    payload: dict[str, Any],
    retry_key: str,
    source_fingerprint: str,
    as_of_time: datetime,
    lease_seconds: int,
) -> tuple[dict[str, Any], bool, str, dict[str, Any] | None, str | None]:
    retry_key = _retry_required_text(retry_key, field="retry_key")
    source_fingerprint = _retry_sha256(source_fingerprint, field="source_fingerprint")
    now = _retry_as_of_time(as_of_time)
    control, claims_control = _simulation_retry_state(payload)
    entries = dict(control["entries"]) if control is not None else {}
    claims = dict(claims_control["claims"]) if claims_control is not None else {}
    existing_claim = claims.get(retry_key)
    recovered_initial_claim = False
    initial_claim_source_changed = False
    if existing_claim is not None:
        claim_lease_until = _retry_time(existing_claim.get("lease_until"), field="lease_until")
        if claim_lease_until is None:
            raise AssertionError("retry claim lease timestamp was not parsed")
        if existing_claim["source_fingerprint"] == source_fingerprint and claim_lease_until > now:
            return dict(payload), False, "attempt_in_progress", existing_claim, existing_claim["claim_token"]
        recovered_initial_claim = existing_claim["source_fingerprint"] == source_fingerprint
        initial_claim_source_changed = not recovered_initial_claim
        claims.pop(retry_key)
    if retry_key not in entries:
        claim = _new_retry_initial_claim(
            retry_key=retry_key,
            source_fingerprint=source_fingerprint,
            as_of_time=now,
            lease_seconds=lease_seconds,
        )
        claims[retry_key] = claim
        next_payload = _retry_claims_payload_with_claims(payload, claims)
        reason = (
            "source_changed"
            if initial_claim_source_changed
            else "initial_claim_recovered"
            if recovered_initial_claim
            else "no_previous_failure"
        )
        return next_payload, True, reason, None, claim["claim_token"]
    entry = dict(entries[retry_key])
    if entry["source_fingerprint"] != source_fingerprint:
        entries.pop(retry_key)
        claim = _new_retry_initial_claim(
            retry_key=retry_key,
            source_fingerprint=source_fingerprint,
            as_of_time=now,
            lease_seconds=lease_seconds,
        )
        claims[retry_key] = claim
        next_payload = _retry_control_payload_with_entries(payload, entries)
        next_payload = _retry_claims_payload_with_claims(next_payload, claims)
        return next_payload, True, "source_changed", None, claim["claim_token"]
    lease_until = _retry_time(entry.get("attempt_lease_until"), field="attempt_lease_until", optional=True)
    if lease_until is not None and lease_until > now:
        return dict(payload), False, "attempt_in_progress", entry, entry["entry_sha256"]
    next_retry_at = _retry_time(entry.get("next_retry_at"), field="next_retry_at")
    if next_retry_at is None:
        raise AssertionError("next retry timestamp was not parsed")
    if next_retry_at > now:
        return dict(payload), False, "backoff_not_due", entry, None
    entry.update(
        {
            "attempt_count": int(entry["attempt_count"]) + 1,
            "last_attempt_at": now.isoformat(),
            "attempt_lease_until": (now + timedelta(seconds=lease_seconds)).isoformat(),
        }
    )
    entry = _retry_entry_with_hash(entry)
    entries[retry_key] = entry
    next_payload = _retry_control_payload_with_entries(payload, entries)
    next_payload = _retry_claims_payload_with_claims(next_payload, claims)
    return next_payload, True, "retry_claimed", entry, entry["entry_sha256"]


def inspect_simulation_retry_backoff(
    *,
    run: SimulationDailyRun,
    retry_key: str,
    source_fingerprint: str,
    as_of_time: datetime,
    lease_seconds: int,
) -> SimulationRetryAttemptDecision | None:
    """Return a strict non-due decision without opening another DB transaction.

    The run is already loaded by the scheduler's bounded candidate query.  A
    due/source-changed/first attempt still returns ``None`` so the repository
    row-lock claim remains the only writer authority.
    """

    if type(lease_seconds) is not int or lease_seconds <= 0:
        raise ValueError("lease_seconds must be a positive integer")
    _, should_execute, reason, retry_entry, claim_token = _claim_retry_attempt_payload(
        payload=run.run_payload_json,
        retry_key=retry_key,
        source_fingerprint=source_fingerprint,
        as_of_time=as_of_time,
        lease_seconds=lease_seconds,
    )
    if should_execute:
        return None
    return SimulationRetryAttemptDecision(
        run=run,
        should_execute=False,
        reason=reason,
        retry_entry=deepcopy(retry_entry),
        claim_token=claim_token,
    )


def simulation_retry_claim_token(payload: dict[str, Any], *, retry_key: str) -> str | None:
    retry_key = _retry_required_text(retry_key, field="retry_key")
    control, claims_control = _simulation_retry_state(payload)
    claims = claims_control["claims"] if claims_control is not None else {}
    claim = claims.get(retry_key)
    if claim is not None:
        return str(claim["claim_token"])
    entries = control["entries"] if control is not None else {}
    entry = entries.get(retry_key)
    if entry is None:
        return None
    if entry.get("last_attempt_at") is not None and entry.get("attempt_lease_until") is not None:
        return str(entry["entry_sha256"])
    return None


def _record_retry_failure_payload(
    *,
    payload: dict[str, Any],
    retry_key: str,
    source_fingerprint: str,
    failure_fingerprint: str,
    failure_stage: str,
    error: dict[str, Any],
    as_of_time: datetime,
    base_delay_seconds: int,
    max_delay_seconds: int,
    expected_claim_token: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    retry_key = _retry_required_text(retry_key, field="retry_key")
    source_fingerprint = _retry_sha256(source_fingerprint, field="source_fingerprint")
    failure_fingerprint = _retry_sha256(failure_fingerprint, field="failure_fingerprint")
    failure_stage = _retry_required_text(failure_stage, field="failure_stage")
    control, claims_control = _simulation_retry_state(payload)
    entries = dict(control["entries"]) if control is not None else {}
    claims = dict(claims_control["claims"]) if claims_control is not None else {}
    previous = entries.get(retry_key)
    active_token = simulation_retry_claim_token(payload, retry_key=retry_key)
    if expected_claim_token is not None:
        expected_claim_token = _retry_sha256(expected_claim_token, field="expected_claim_token")
        if active_token != expected_claim_token:
            raise InvalidStateTransitionError(
                "simulation scheduler retry failure writer no longer owns the attempt claim",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_STALE_WRITER",
                    "retry_key": retry_key,
                    "expected_claim_token": expected_claim_token,
                    "actual_claim_token": active_token,
                },
            )
        claim = claims.get(retry_key)
        active_source = claim.get("source_fingerprint") if claim is not None else previous.get("source_fingerprint")
        if active_source != source_fingerprint:
            raise InvalidStateTransitionError(
                "simulation scheduler retry failure source no longer matches the attempt claim",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_SOURCE_CONFLICT",
                    "retry_key": retry_key,
                    "expected_source_fingerprint": active_source,
                    "actual_source_fingerprint": source_fingerprint,
                },
            )
    elif active_token is not None:
        raise InvalidStateTransitionError(
            "simulation scheduler retry failure writer omitted the active attempt claim token",
            context={
                "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_TOKEN_REQUIRED",
                "retry_key": retry_key,
                "active_claim_token": active_token,
            },
        )
    same_failure = bool(
        previous
        and previous["source_fingerprint"] == source_fingerprint
        and previous["failure_fingerprint"] == failure_fingerprint
    )
    count = int(previous["consecutive_failure_count"]) + 1 if same_failure else 1
    attempt_count = max(int(previous["attempt_count"]) if same_failure else 0, count)
    now = _retry_as_of_time(as_of_time)
    delay_seconds = min(base_delay_seconds * (2 ** min(count - 1, 20)), max_delay_seconds)
    normalized_error = _retry_json_safe(error)
    if not isinstance(normalized_error, dict):
        raise InvalidStateTransitionError(
            "simulation scheduler retry error evidence must be an object",
            context={"reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"},
        )
    last_error = {
        "type": _retry_required_text(normalized_error.get("type"), field="last_error.type"),
        "message": _retry_string(normalized_error.get("message"), field="last_error.message"),
        "reason_code": normalized_error.get("reason_code"),
        "context": normalized_error.get("context"),
    }
    entry = _retry_entry_with_hash(
        {
            "schema_version": SIMULATION_SCHEDULER_RETRY_ENTRY_SCHEMA,
            "retry_key": retry_key,
            "source_fingerprint": source_fingerprint,
            "failure_fingerprint": failure_fingerprint,
            "failure_stage": failure_stage,
            "consecutive_failure_count": count,
            "attempt_count": attempt_count,
            "first_failed_at": previous["first_failed_at"] if same_failure else now.isoformat(),
            "last_failed_at": now.isoformat(),
            "next_retry_at": (now + timedelta(seconds=delay_seconds)).isoformat(),
            "last_attempt_at": None,
            "attempt_lease_until": None,
            "last_error": last_error,
        }
    )
    entries[retry_key] = entry
    claims.pop(retry_key, None)
    next_payload = _retry_control_payload_with_entries(payload, entries)
    next_payload = _retry_claims_payload_with_claims(next_payload, claims)
    return next_payload, entry


_LOCAL_SIM_EMPTY_AUTHORITY_DIRECT_CARRIERS = (
    LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY,
    LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY,
    LOCAL_SIM_PROJECTION_GENERATION_PAYLOAD_KEY,
    LOCAL_SIM_PROJECTION_TERMINAL_FAILURE_PAYLOAD_KEY,
    LOCAL_SIM_PROJECTION_READBACK_FAILURE_PAYLOAD_KEY,
    LOCAL_SIM_PROJECTION_READBACK_TERMINAL_FAILURE_PAYLOAD_KEY,
    LOCAL_SIM_VALUATION_PENDING_PAYLOAD_KEY,
    LOCAL_SIM_VALUATION_COMPLETION_PAYLOAD_KEY,
    "local_sim_persistence",
    "local_sim_durable_minute_loop",
)
_LOCAL_SIM_EMPTY_AUTHORITY_PROJECTION_SUMMARIES = (
    "strategy_performance",
    "performance_projection",
)


def _local_sim_economic_receipt_map(payload: dict[str, Any]) -> dict[str, LocalSimEconomicReceiptV1]:
    raw = payload.get(LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise InvalidStateTransitionError(
            "LocalSIM economic receipt payload must be an object",
            context={"reason_code": "LOCALSIM_ECONOMIC_RECEIPT_PAYLOAD_INVALID"},
        )
    receipts: dict[str, LocalSimEconomicReceiptV1] = {}
    for receipt_id, receipt_payload in raw.items():
        raw_generation = receipt_payload.get("generation") if isinstance(receipt_payload, dict) else None
        if type(raw_generation) is not int or raw_generation <= 0:
            raise InvalidStateTransitionError(
                "LocalSIM economic receipt generation must be a positive integer",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_GENERATION_INVALID",
                    "receipt_id": str(receipt_id),
                    "actual_generation": raw_generation,
                },
            )
        try:
            receipt = LocalSimEconomicReceiptV1.model_validate(receipt_payload)
        except Exception as exc:
            raise InvalidStateTransitionError(
                "LocalSIM economic receipt failed schema or hash validation",
                context={"reason_code": "LOCALSIM_ECONOMIC_RECEIPT_SCHEMA_INVALID", "receipt_id": str(receipt_id)},
            ) from exc
        if receipt.receipt_id != receipt_id:
            raise InvalidStateTransitionError(
                "LocalSIM economic receipt map key does not match identity",
                context={"reason_code": "LOCALSIM_ECONOMIC_RECEIPT_IDENTITY_CONFLICT", "receipt_id": str(receipt_id)},
            )
        receipts[receipt_id] = receipt
    return receipts


def _local_sim_projection_outbox(payload: dict[str, Any]) -> LocalSimProjectionOutboxV1 | None:
    raw = payload.get(LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY)
    if raw is None:
        return None
    try:
        return LocalSimProjectionOutboxV1.model_validate(raw)
    except Exception as exc:
        raise InvalidStateTransitionError(
            "LocalSIM projection outbox failed schema or hash validation",
            context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID"},
        ) from exc


def _local_sim_projection_receipt_map(payload: dict[str, Any]) -> dict[str, LocalSimProjectionReceiptV1]:
    raw = payload.get(LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY) or {}
    if not isinstance(raw, dict):
        raise InvalidStateTransitionError(
            "LocalSIM projection receipt payload must be an object",
            context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_PAYLOAD_INVALID"},
        )
    receipts: dict[str, LocalSimProjectionReceiptV1] = {}
    for receipt_id, receipt_payload in raw.items():
        try:
            receipt = LocalSimProjectionReceiptV1.model_validate(receipt_payload)
        except Exception as exc:
            raise InvalidStateTransitionError(
                "LocalSIM projection receipt failed schema or hash validation",
                context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_SCHEMA_INVALID", "receipt_id": str(receipt_id)},
            ) from exc
        if receipt.projection_receipt_id != receipt_id:
            raise InvalidStateTransitionError(
                "LocalSIM projection receipt map key does not match identity",
                context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_IDENTITY_CONFLICT", "receipt_id": str(receipt_id)},
            )
        receipts[receipt_id] = receipt
    return receipts


def _local_sim_state_map(payload: dict[str, Any]) -> dict[str, LocalSimExecutionStateV1]:
    raw = payload.get(LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise InvalidStateTransitionError(
            "LocalSIM durable state payload must be an object",
            context={"reason_code": "LOCALSIM_DURABLE_STATE_PAYLOAD_INVALID"},
        )
    states: dict[str, LocalSimExecutionStateV1] = {}
    for state_id, state_payload in raw.items():
        try:
            state = LocalSimExecutionStateV1.model_validate(state_payload)
        except Exception as exc:  # noqa: BLE001
            raise InvalidStateTransitionError(
                "LocalSIM durable state payload failed schema or hash validation",
                context={"reason_code": "LOCALSIM_DURABLE_STATE_SCHEMA_INVALID", "state_id": str(state_id)},
            ) from exc
        if state.state_id != state_id:
            raise InvalidStateTransitionError(
                "LocalSIM durable state map key does not match state identity",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_IDENTITY_CONFLICT",
                    "map_state_id": str(state_id),
                    "payload_state_id": state.state_id,
                },
            )
        states[state.state_id] = state
    return states


def _local_sim_json_safe_context_value(value: Any) -> Any:
    if value is None or type(value) in {bool, int, str}:
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    return str(value)


def _local_sim_empty_authority_carrier_context(
    payload: dict[str, Any],
    *,
    raw_generation: Any,
) -> dict[str, Any] | None:
    carrier_type: str | None = None
    carrier: Any = None
    for candidate in _LOCAL_SIM_EMPTY_AUTHORITY_DIRECT_CARRIERS:
        if candidate in payload:
            carrier_type = candidate
            carrier = payload.get(candidate)
            break
    if carrier_type is None:
        for candidate in _LOCAL_SIM_EMPTY_AUTHORITY_PROJECTION_SUMMARIES:
            raw = payload.get(candidate)
            if isinstance(raw, dict) and any(
                key in raw
                for key in (
                    "local_sim_generation",
                    "local_sim_outbox_id",
                    "local_sim_economic_hash",
                    "tca_generation",
                )
            ):
                carrier_type = candidate
                carrier = raw
                break
    if carrier_type is None and payload.get("broker_order_handles"):
        carrier_type = "broker_order_handles"
        carrier = payload.get("broker_order_handles")
    broker_called = payload.get("broker_called")
    if carrier_type is None and broker_called is not None and (type(broker_called) is not bool or broker_called):
        carrier_type = "broker_called"
        carrier = broker_called
    submitted_intents = payload.get("submitted_intents")
    if (
        carrier_type is None
        and submitted_intents is not None
        and (type(submitted_intents) is not int or submitted_intents != 0)
    ):
        carrier_type = "submitted_intents"
        carrier = submitted_intents
    if carrier_type is None:
        return None

    carrier_payload = carrier if isinstance(carrier, dict) else {}
    receipt_payload: dict[str, Any] = {}
    receipt_map_id: Any = None
    if carrier_type == LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY and isinstance(carrier, dict) and carrier:
        receipt_map_id, raw_receipt = sorted(carrier.items(), key=lambda item: str(item[0]))[0]
        if isinstance(raw_receipt, dict):
            receipt_payload = raw_receipt

    generation = carrier_payload.get("generation")
    if generation is None:
        generation = carrier_payload.get("local_sim_generation")
    if generation is None and isinstance(carrier_payload.get("tca_generation"), dict):
        generation = carrier_payload["tca_generation"].get("generation")
    if generation is None:
        generation = receipt_payload.get("generation")
    if generation is None:
        generation = raw_generation

    outbox_id = (
        carrier_payload.get("outbox_id")
        or carrier_payload.get("local_sim_outbox_id")
        or receipt_payload.get("outbox_id")
    )
    projection_receipt_id = (
        carrier_payload.get("projection_receipt_id") or receipt_payload.get("projection_receipt_id") or receipt_map_id
    )
    receipt_id = carrier_payload.get("receipt_id") or receipt_payload.get("receipt_id")
    if receipt_id is None and carrier_type == LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY:
        receipt_id = projection_receipt_id
    carrier_identity = outbox_id or receipt_id or projection_receipt_id
    return {
        "carrier_type": carrier_type,
        "carrier_identity": _local_sim_json_safe_context_value(carrier_identity),
        "outbox_id": _local_sim_json_safe_context_value(outbox_id),
        "receipt_id": _local_sim_json_safe_context_value(receipt_id),
        "projection_receipt_id": _local_sim_json_safe_context_value(projection_receipt_id),
        "expected_generation": 0,
        "actual_generation": _local_sim_json_safe_context_value(generation),
    }


@dataclass(frozen=True)
class _LocalSimStateAuthority:
    generation: int
    receipt: LocalSimEconomicReceiptV1 | None
    states: dict[str, LocalSimExecutionStateV1]


def _local_sim_state_authority_closure(
    *,
    run_id: str,
    binding_id: str,
    trade_date: date,
    plan_id: str | None,
    payload: dict[str, Any],
    states: dict[str, LocalSimExecutionStateV1] | None = None,
) -> _LocalSimStateAuthority:
    """Close current run identity, receipt history and durable state authority."""
    state_map = states if states is not None else _local_sim_state_map(payload)
    current_identity = {
        "run_id": run_id,
        "binding_id": binding_id,
        "trade_date": trade_date.isoformat(),
        "plan_id": plan_id,
    }
    raw_generation = payload.get(LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY)
    try:
        receipts = _local_sim_economic_receipt_map(payload)
    except InvalidStateTransitionError as exc:
        raise InvalidStateTransitionError(
            exc.message,
            context={"run_id": run_id, **exc.context},
        ) from exc

    if not state_map and not receipts:
        if raw_generation is not None and not (type(raw_generation) is int and raw_generation == 0):
            raise InvalidStateTransitionError(
                "LocalSIM economic generation is invalid for an empty durable authority",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_GENERATION_INVALID",
                    "run_id": run_id,
                    "carrier_type": LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY,
                    "carrier_identity": None,
                    "outbox_id": None,
                    "receipt_id": None,
                    "projection_receipt_id": None,
                    "expected_generation": 0,
                    "actual_generation": _local_sim_json_safe_context_value(raw_generation),
                },
            )
        orphan_carrier = _local_sim_empty_authority_carrier_context(
            payload,
            raw_generation=raw_generation,
        )
        if orphan_carrier is not None:
            raise InvalidStateTransitionError(
                "LocalSIM empty durable authority contains an orphan economic or projection carrier",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_ORPHAN_CARRIER",
                    "run_id": run_id,
                    **orphan_carrier,
                },
            )
        return _LocalSimStateAuthority(generation=0, receipt=None, states={})
    if not receipts:
        raise InvalidStateTransitionError(
            "LocalSIM durable state authority is missing its committed economic receipt",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_MISSING",
                "run_id": run_id,
                "expected_generation": raw_generation,
                "actual_generation": None,
                "receipt_id": None,
            },
        )
    if type(raw_generation) is not int or raw_generation <= 0:
        raise InvalidStateTransitionError(
            "LocalSIM economic generation must be a positive integer",
            context={
                "reason_code": "LOCALSIM_ECONOMIC_GENERATION_INVALID",
                "run_id": run_id,
                "expected_generation": "positive_integer",
                "actual_generation": raw_generation,
            },
        )

    by_generation: dict[int, list[LocalSimEconomicReceiptV1]] = {}
    for receipt in receipts.values():
        by_generation.setdefault(receipt.generation, []).append(receipt)
    duplicate_generation = next(
        (generation for generation, items in sorted(by_generation.items()) if len(items) != 1),
        None,
    )
    if duplicate_generation is not None:
        conflicts = by_generation[duplicate_generation]
        raise InvalidStateTransitionError(
            "LocalSIM economic receipt history has duplicate generation authority",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_GENERATION_CONFLICT",
                "run_id": run_id,
                "expected_generation": raw_generation,
                "actual_generation": duplicate_generation,
                "receipt_id": None,
                "receipt_ids": sorted(item.receipt_id for item in conflicts),
            },
        )
    receipt_generations = sorted(by_generation)
    receipt_max_generation = receipt_generations[-1]
    if raw_generation != receipt_max_generation:
        raise InvalidStateTransitionError(
            "LocalSIM economic generation high-watermark conflicts with receipt history",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_GENERATION_MISMATCH",
                "run_id": run_id,
                "expected_generation": receipt_max_generation,
                "actual_generation": raw_generation,
                "receipt_id": by_generation[receipt_max_generation][0].receipt_id,
            },
        )
    expected_generations = list(range(1, raw_generation + 1))
    if receipt_generations != expected_generations:
        raise InvalidStateTransitionError(
            "LocalSIM economic receipt generation history is not contiguous",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_GENERATION_GAP",
                "run_id": run_id,
                "expected_generation": expected_generations,
                "actual_generation": receipt_generations,
                "receipt_id": by_generation[receipt_max_generation][0].receipt_id,
            },
        )

    predecessor_plan_id: str | None = None
    if payload.get("rebuilt_after_side_effect_free_failure") is True:
        raw_predecessor = payload.get("rebuilt_from_execution_plan_id")
        raw_rebuilt = payload.get("rebuilt_execution_plan_id")
        raw_backend = payload.get("rebuilt_failure_backend")
        if (
            isinstance(raw_predecessor, str)
            and raw_predecessor.strip()
            and isinstance(raw_rebuilt, str)
            and raw_rebuilt.strip()
            and raw_backend == SimulationBrokerBackend.LOCAL_SIM.value
        ):
            normalized_predecessor = raw_predecessor.strip()
            normalized_rebuilt = raw_rebuilt.strip()
            cash_fit = payload.get("local_sim_cash_fit")
            prepared_successor_is_proven = normalized_rebuilt == plan_id
            if normalized_rebuilt != plan_id and isinstance(cash_fit, dict):
                prepared_successor_is_proven = (
                    cash_fit.get("schema_version") == "localsim_capital_dependency_v1"
                    and cash_fit.get("status") == "SELL_FIRST_DEPENDENCY_ORDERED"
                    and cash_fit.get("capital_waiting_owner") == "LocalSimExecutionStateV1"
                )
            if prepared_successor_is_proven:
                predecessor_plan_id = normalized_predecessor

    allowed_receipt_plan_ids = {plan_id}
    if predecessor_plan_id is not None:
        allowed_receipt_plan_ids.add(predecessor_plan_id)
    receipt_plan_ids = {receipt.plan_id for receipt in receipts.values()}
    latest = by_generation[raw_generation][0]
    ordered_receipt_plan_ids = [by_generation[generation][0].plan_id for generation in receipt_generations]
    expected_plan_sequence = (
        [predecessor_plan_id] * ordered_receipt_plan_ids.count(predecessor_plan_id)
        + [plan_id] * ordered_receipt_plan_ids.count(plan_id)
        if predecessor_plan_id is not None
        else [plan_id] * len(ordered_receipt_plan_ids)
    )
    if (
        latest.plan_id != plan_id
        or receipt_plan_ids - allowed_receipt_plan_ids
        or ordered_receipt_plan_ids != expected_plan_sequence
        or (predecessor_plan_id is not None and receipt_plan_ids != {predecessor_plan_id, plan_id})
    ):
        conflicting_receipt = next(
            (
                receipt
                for receipt in sorted(receipts.values(), key=lambda item: item.generation)
                if receipt.plan_id not in allowed_receipt_plan_ids or receipt.generation == raw_generation
            ),
            latest,
        )
        raise InvalidStateTransitionError(
            "LocalSIM economic receipt identity conflicts with the durable run",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_IDENTITY_CONFLICT",
                "run_id": run_id,
                "expected_generation": raw_generation,
                "actual_generation": conflicting_receipt.generation,
                "receipt_id": conflicting_receipt.receipt_id,
                "current_run_identity": current_identity,
                "receipt_identity": {
                    "run_id": conflicting_receipt.run_id,
                    "binding_id": conflicting_receipt.binding_id,
                    "trade_date": conflicting_receipt.trade_date.isoformat(),
                    "plan_id": conflicting_receipt.plan_id,
                },
                "allowed_receipt_plan_ids": sorted(item for item in allowed_receipt_plan_ids if item is not None),
                "ordered_receipt_plan_ids": ordered_receipt_plan_ids,
            },
        )

    for receipt in receipts.values():
        receipt_identity = {
            "run_id": receipt.run_id,
            "binding_id": receipt.binding_id,
            "trade_date": receipt.trade_date.isoformat(),
            "plan_id": receipt.plan_id,
        }
        expected_receipt_identity = {
            **current_identity,
            "plan_id": receipt.plan_id,
        }
        if receipt_identity != expected_receipt_identity:
            raise InvalidStateTransitionError(
                "LocalSIM economic receipt identity conflicts with the durable run",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_IDENTITY_CONFLICT",
                    "run_id": run_id,
                    "expected_generation": raw_generation,
                    "actual_generation": receipt.generation,
                    "receipt_id": receipt.receipt_id,
                    "current_run_identity": expected_receipt_identity,
                    "receipt_identity": receipt_identity,
                },
            )
        expected_fact_identity = expected_receipt_identity
        actual_fact_identity = {
            field: receipt.economic_facts.get(field)
            for field in ("run_id", "binding_id", "trade_date", "plan_id")
            if field in receipt.economic_facts
        }
        fact_drift = {
            field: {"expected": expected_fact_identity[field], "actual": actual}
            for field, actual in actual_fact_identity.items()
            if actual != expected_fact_identity[field]
        }
        if fact_drift:
            raise InvalidStateTransitionError(
                "LocalSIM economic fact identity conflicts with the durable run",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_FACT_IDENTITY_CONFLICT",
                    "run_id": run_id,
                    "expected_generation": raw_generation,
                    "actual_generation": receipt.generation,
                    "receipt_id": receipt.receipt_id,
                    "current_run_identity": current_identity,
                    "economic_fact_identity": actual_fact_identity,
                    "identity_drift": fact_drift,
                },
            )

    raw_hashes = latest.economic_facts.get("state_hashes")
    if raw_hashes == {} and not state_map:
        return _LocalSimStateAuthority(generation=raw_generation, receipt=latest, states={})
    if not isinstance(raw_hashes, dict) or not raw_hashes:
        raise InvalidStateTransitionError(
            "LocalSIM latest economic generation has no exact state authority",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_MISSING",
                "run_id": run_id,
                "expected_generation": raw_generation,
                "actual_generation": latest.generation,
                "receipt_id": latest.receipt_id,
            },
        )
    authoritative: dict[str, LocalSimExecutionStateV1] = {}
    for state_id, expected_hash in raw_hashes.items():
        if not isinstance(state_id, str) or not state_id or not isinstance(expected_hash, str) or not expected_hash:
            raise InvalidStateTransitionError(
                "LocalSIM economic generation state authority is malformed",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_SCHEMA_INVALID",
                    "run_id": run_id,
                    "expected_generation": raw_generation,
                    "actual_generation": latest.generation,
                    "receipt_id": latest.receipt_id,
                    "state_id": state_id if isinstance(state_id, str) else None,
                },
            )
        state = state_map.get(state_id)
        if state is None:
            raise InvalidStateTransitionError(
                "LocalSIM economic generation references a missing durable state",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_STATE_MISSING",
                    "run_id": run_id,
                    "expected_generation": raw_generation,
                    "actual_generation": latest.generation,
                    "receipt_id": latest.receipt_id,
                    "state_id": state_id,
                    "expected_state_hash": expected_hash,
                    "actual_state_hash": None,
                },
            )
        if state.state_hash != expected_hash:
            raise InvalidStateTransitionError(
                "LocalSIM economic generation state authority hash conflicts with durable state",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_HASH_CONFLICT",
                    "run_id": run_id,
                    "expected_generation": raw_generation,
                    "actual_generation": latest.generation,
                    "receipt_id": latest.receipt_id,
                    "state_id": state_id,
                    "expected_state_hash": expected_hash,
                    "actual_state_hash": state.state_hash,
                },
            )
        authoritative[state_id] = state

    invalid_authority = tuple(
        state
        for state in authoritative.values()
        if {
            "run_id": state.run_id,
            "binding_id": state.binding_id,
            "trade_date": state.trade_date.isoformat(),
            "plan_id": state.plan_id,
        }
        != current_identity
    )
    if invalid_authority:
        raise InvalidStateTransitionError(
            "LocalSIM authoritative state identity conflicts with the durable run",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_AUTHORITY_STATE_IDENTITY_CONFLICT",
                "run_id": run_id,
                "expected_generation": raw_generation,
                "actual_generation": latest.generation,
                "receipt_id": latest.receipt_id,
                "state_ids": sorted(state.state_id for state in invalid_authority),
                "current_run_identity": current_identity,
            },
        )
    authority_intents = {state.intent_id for state in authoritative.values()}
    superseded_authoritative: dict[str, LocalSimExecutionStateV1] = {}
    if predecessor_plan_id is not None:
        predecessor_receipt = max(
            (receipt for receipt in receipts.values() if receipt.plan_id == predecessor_plan_id),
            key=lambda item: item.generation,
        )
        predecessor_hashes = predecessor_receipt.economic_facts.get("state_hashes")
        if not isinstance(predecessor_hashes, dict) or not predecessor_hashes:
            raise InvalidStateTransitionError(
                "LocalSIM superseded plan has no exact terminal generation state authority",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_SUPERSEDED_AUTHORITY_MISSING",
                    "run_id": run_id,
                    "expected_generation": predecessor_receipt.generation,
                    "actual_generation": predecessor_receipt.generation,
                    "receipt_id": predecessor_receipt.receipt_id,
                    "plan_id": predecessor_plan_id,
                },
            )
        for state_id, expected_hash in predecessor_hashes.items():
            state = state_map.get(state_id) if isinstance(state_id, str) else None
            if (
                state is None
                or not isinstance(expected_hash, str)
                or not expected_hash
                or state.state_hash != expected_hash
                or state.run_id != run_id
                or state.binding_id != binding_id
                or state.trade_date != trade_date
                or state.plan_id != predecessor_plan_id
            ):
                raise InvalidStateTransitionError(
                    "LocalSIM superseded plan state authority does not close over its final committed generation",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_SUPERSEDED_AUTHORITY_CONFLICT",
                        "run_id": run_id,
                        "expected_generation": predecessor_receipt.generation,
                        "actual_generation": predecessor_receipt.generation,
                        "receipt_id": predecessor_receipt.receipt_id,
                        "plan_id": predecessor_plan_id,
                        "state_id": state_id if isinstance(state_id, str) else None,
                        "expected_state_hash": expected_hash,
                        "actual_state_hash": state.state_hash if state is not None else None,
                    },
                )
            superseded_authoritative[state_id] = state
        if {state.intent_id for state in superseded_authoritative.values()} != authority_intents:
            raise InvalidStateTransitionError(
                "LocalSIM superseded and current plan authorities do not close over the same intent set",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_SUPERSEDED_INTENT_CONFLICT",
                    "run_id": run_id,
                    "receipt_id": predecessor_receipt.receipt_id,
                    "predecessor_plan_id": predecessor_plan_id,
                    "current_plan_id": plan_id,
                    "predecessor_intent_ids": sorted(state.intent_id for state in superseded_authoritative.values()),
                    "current_intent_ids": sorted(authority_intents),
                },
            )
        predecessor_by_intent = {state.intent_id: state for state in superseded_authoritative.values()}
        current_by_intent = {state.intent_id: state for state in authoritative.values()}
        semantic_fields = (
            "portfolio_id",
            "symbol",
            "side",
            "total_quantity",
            "filled_quantity",
            "remaining_quantity",
            "algo_code",
            "schedule_version",
            "causality_cursor",
            "order_status",
            "runtime_status",
            "plan",
            "plan_sha256",
        )
        semantic_drift = {
            intent_id: {
                field: {
                    "predecessor": _local_sim_json_safe_context_value(getattr(predecessor_by_intent[intent_id], field)),
                    "current": _local_sim_json_safe_context_value(getattr(current_by_intent[intent_id], field)),
                }
                for field in semantic_fields
                if getattr(predecessor_by_intent[intent_id], field) != getattr(current_by_intent[intent_id], field)
            }
            for intent_id in sorted(authority_intents)
        }
        semantic_drift = {intent_id: fields for intent_id, fields in semantic_drift.items() if fields}
        if semantic_drift:
            raise InvalidStateTransitionError(
                "LocalSIM superseded and current plan authorities drift in execution semantics",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_SUPERSEDED_SEMANTIC_CONFLICT",
                    "run_id": run_id,
                    "receipt_id": predecessor_receipt.receipt_id,
                    "predecessor_plan_id": predecessor_plan_id,
                    "current_plan_id": plan_id,
                    "semantic_drift": semantic_drift,
                },
            )
    active_history = tuple(
        state
        for state_id, state in state_map.items()
        if state_id not in authoritative
        and state_id not in superseded_authoritative
        and not state.is_terminal
        and state.intent_id in authority_intents
        and state.plan_id == plan_id
    )
    if active_history:
        raise InvalidStateTransitionError(
            "LocalSIM durable state history contains a second active authority",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_ACTIVE_AUTHORITY_CONFLICT",
                "run_id": run_id,
                "expected_generation": raw_generation,
                "actual_generation": latest.generation,
                "receipt_id": latest.receipt_id,
                "active_state_ids": sorted(state.state_id for state in active_history),
            },
        )
    invalid_history = tuple(
        state
        for state_id, state in state_map.items()
        if state_id not in authoritative
        and state_id not in superseded_authoritative
        and not (
            state.intent_id in authority_intents
            and {
                "run_id": state.run_id,
                "binding_id": state.binding_id,
                "trade_date": state.trade_date.isoformat(),
                "plan_id": state.plan_id,
            }
            == current_identity
        )
    )
    if invalid_history:
        raise InvalidStateTransitionError(
            "LocalSIM durable state history does not belong to the committed plan authority",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_HISTORY_IDENTITY_CONFLICT",
                "run_id": run_id,
                "expected_generation": raw_generation,
                "actual_generation": latest.generation,
                "receipt_id": latest.receipt_id,
                "state_ids": sorted(state.state_id for state in invalid_history),
            },
        )
    active_history = tuple(
        state
        for state_id, state in state_map.items()
        if state_id not in authoritative and state_id not in superseded_authoritative and not state.is_terminal
    )
    if active_history:
        raise InvalidStateTransitionError(
            "LocalSIM durable state history contains a second active authority",
            context={
                "reason_code": "LOCALSIM_DURABLE_STATE_ACTIVE_AUTHORITY_CONFLICT",
                "run_id": run_id,
                "expected_generation": raw_generation,
                "actual_generation": latest.generation,
                "receipt_id": latest.receipt_id,
                "active_state_ids": sorted(state.state_id for state in active_history),
            },
        )
    return _LocalSimStateAuthority(generation=raw_generation, receipt=latest, states=authoritative)


def _validate_local_sim_economic_readback(
    *,
    run: SimulationDailyRun,
    receipt: LocalSimEconomicReceiptV1,
    outbox: LocalSimProjectionOutboxV1,
) -> None:
    state_map = _local_sim_state_map(run.run_payload_json)
    authority = _local_sim_state_authority_closure(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id=run.execution_plan_id,
        payload=run.run_payload_json,
        states=state_map,
    )
    persisted_receipt = authority.receipt
    if (
        persisted_receipt is None
        or persisted_receipt.receipt_id != receipt.receipt_id
        or persisted_receipt.receipt_hash != receipt.receipt_hash
    ):
        raise InvalidStateTransitionError(
            "LocalSIM economic receipt independent readback failed",
            context={
                "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_READBACK_FAILED",
                "run_id": run.run_id,
                "expected_generation": receipt.generation,
                "actual_generation": authority.generation,
                "receipt_id": receipt.receipt_id,
                "actual_receipt_id": persisted_receipt.receipt_id if persisted_receipt else None,
                "expected_receipt_hash": receipt.receipt_hash,
                "actual_receipt_hash": persisted_receipt.receipt_hash if persisted_receipt else None,
            },
        )
    persisted_outbox = _local_sim_projection_outbox(run.run_payload_json)
    if persisted_outbox is None or persisted_outbox.outbox_hash != outbox.outbox_hash:
        raise InvalidStateTransitionError(
            "LocalSIM projection outbox independent readback failed",
            context={
                "reason_code": "LOCALSIM_PROJECTION_OUTBOX_READBACK_FAILED",
                "run_id": run.run_id,
                "expected_generation": outbox.generation,
                "actual_generation": persisted_outbox.generation if persisted_outbox else None,
                "receipt_id": receipt.receipt_id,
                "outbox_id": outbox.outbox_id,
                "actual_outbox_id": persisted_outbox.outbox_id if persisted_outbox else None,
                "expected_outbox_hash": outbox.outbox_hash,
                "actual_outbox_hash": persisted_outbox.outbox_hash if persisted_outbox else None,
            },
        )
    expected_outbox_identity = {
        "receipt_id": persisted_receipt.receipt_id,
        "run_id": run.run_id,
        "plan_id": run.execution_plan_id,
        "generation": authority.generation,
        "economic_hash": persisted_receipt.economic_hash,
    }
    actual_outbox_identity = {
        "receipt_id": persisted_outbox.receipt_id,
        "run_id": persisted_outbox.run_id,
        "plan_id": persisted_outbox.plan_id,
        "generation": persisted_outbox.generation,
        "economic_hash": persisted_outbox.economic_hash,
    }
    if actual_outbox_identity != expected_outbox_identity:
        raise InvalidStateTransitionError(
            "LocalSIM projection outbox identity conflicts with the authoritative economic generation",
            context={
                "reason_code": "LOCALSIM_PROJECTION_OUTBOX_READBACK_IDENTITY_CONFLICT",
                "run_id": run.run_id,
                "expected_generation": authority.generation,
                "actual_generation": persisted_outbox.generation,
                "receipt_id": persisted_receipt.receipt_id,
                "outbox_id": persisted_outbox.outbox_id,
                "expected_outbox_identity": expected_outbox_identity,
                "actual_outbox_identity": actual_outbox_identity,
            },
        )
    expected_states = persisted_receipt.economic_facts.get("state_hashes")
    actual_states = {state_id: state.state_hash for state_id, state in authority.states.items()}
    if expected_states != actual_states:
        expected_state_ids = sorted(expected_states) if isinstance(expected_states, dict) else []
        raise InvalidStateTransitionError(
            "LocalSIM economic state independent readback failed",
            context={
                "reason_code": "LOCALSIM_ECONOMIC_STATE_READBACK_FAILED",
                "run_id": run.run_id,
                "expected_generation": receipt.generation,
                "actual_generation": authority.generation,
                "receipt_id": persisted_receipt.receipt_id,
                "expected_state_ids": expected_state_ids,
                "actual_state_ids": sorted(actual_states),
            },
        )


def _merge_local_sim_state_batch(
    *,
    run_id: str,
    payload: dict[str, Any],
    states: Iterable[LocalSimExecutionStateV1],
    expected_versions: dict[str, tuple[int, str] | None],
) -> dict[str, Any]:
    current = _local_sim_state_map(payload)
    incoming = list(states)
    if not incoming:
        raise ValueError("LocalSIM state batch cannot be empty")
    if len({state.state_id for state in incoming}) != len(incoming):
        raise InvalidStateTransitionError(
            "LocalSIM state batch contains duplicate state identities",
            context={"reason_code": "LOCALSIM_DURABLE_STATE_BATCH_DUPLICATE"},
        )
    for state in incoming:
        if state.run_id != run_id:
            raise InvalidStateTransitionError(
                "LocalSIM state run identity does not match repository target",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_RUN_IDENTITY_CONFLICT",
                    "run_id": run_id,
                    "state_run_id": state.run_id,
                    "state_id": state.state_id,
                },
            )
        if state.state_id not in expected_versions:
            raise InvalidStateTransitionError(
                "LocalSIM state batch is missing the expected CAS version",
                context={"reason_code": "LOCALSIM_DURABLE_STATE_CAS_EXPECTATION_MISSING", "state_id": state.state_id},
            )
        existing = current.get(state.state_id)
        expected = expected_versions[state.state_id]
        if existing is None:
            if expected is not None:
                raise InvalidStateTransitionError(
                    "LocalSIM initial state CAS precondition failed",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_CAS_CONFLICT",
                        "state_id": state.state_id,
                        "expected": expected,
                        "incoming_sequence": state.sequence,
                    },
                )
        else:
            if expected != (existing.sequence, existing.state_hash):
                raise InvalidStateTransitionError(
                    "LocalSIM state CAS precondition failed",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_CAS_CONFLICT",
                        "state_id": state.state_id,
                        "expected": expected,
                        "actual_sequence": existing.sequence,
                        "actual_state_hash": existing.state_hash,
                    },
                )
            if state.sequence == existing.sequence and state.state_hash == existing.state_hash:
                continue
            if state.sequence != existing.sequence + 1:
                raise InvalidStateTransitionError(
                    "LocalSIM state sequence must advance by exactly one",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_SEQUENCE_CONFLICT",
                        "state_id": state.state_id,
                        "previous_sequence": existing.sequence,
                        "incoming_sequence": state.sequence,
                    },
                )
            immutable_fields = (
                "run_id",
                "binding_id",
                "trade_date",
                "plan_id",
                "intent_id",
                "algo_instance_id",
                "portfolio_id",
                "order_id",
                "symbol",
                "side",
                "total_quantity",
                "algo_code",
                "schedule_version",
                "causality_cursor",
                "created_at",
            )
            drift = [field for field in immutable_fields if getattr(existing, field) != getattr(state, field)]
            if drift:
                raise InvalidStateTransitionError(
                    "LocalSIM state immutable identity drifted during transition",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_IDENTITY_CONFLICT",
                        "state_id": state.state_id,
                        "fields": drift,
                    },
                )
        current[state.state_id] = state
    merged = dict(payload)
    merged[LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY] = {
        state_id: state.model_dump(mode="json") for state_id, state in sorted(current.items())
    }
    return merged


def _merge_local_sim_economic_event(
    *,
    run_id: str,
    binding_id: str,
    trade_date: date,
    plan_id: str,
    payload: dict[str, Any],
    states: Iterable[LocalSimExecutionStateV1],
    expected_versions: dict[str, tuple[int, str] | None],
    economic_facts: dict[str, Any],
    projection_payload: dict[str, Any],
) -> tuple[dict[str, Any], LocalSimEconomicReceiptV1, LocalSimProjectionOutboxV1, bool]:
    incoming = list(states)
    economic_hash = canonical_json_sha256(economic_facts)
    idempotency_key = canonical_json_sha256(["local_sim_economic_event_v1", run_id, plan_id, economic_hash])
    receipts = _local_sim_economic_receipt_map(payload)
    existing_receipt = next((item for item in receipts.values() if item.idempotency_key == idempotency_key), None)
    existing_outbox = _local_sim_projection_outbox(payload)

    def merge_states() -> dict[str, Any]:
        if not incoming:
            return dict(payload)
        return _merge_local_sim_state_batch(
            run_id=run_id,
            payload=payload,
            states=incoming,
            expected_versions=expected_versions,
        )

    if existing_receipt is not None:
        if existing_outbox is None or existing_outbox.receipt_id != existing_receipt.receipt_id:
            raise InvalidStateTransitionError(
                "LocalSIM economic receipt is missing its projection outbox",
                context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_MISSING", "run_id": run_id},
            )
        return merge_states(), existing_receipt, existing_outbox, False
    if existing_outbox is not None and existing_outbox.status != LocalSimProjectionOutboxStatus.PROJECTED:
        raise InvalidStateTransitionError(
            "LocalSIM cannot commit a new economic event while projection outbox is pending",
            context={
                "reason_code": "LOCALSIM_PROJECTION_OUTBOX_PENDING",
                "run_id": run_id,
                "outbox_id": existing_outbox.outbox_id,
            },
        )
    raw_generation = payload.get(LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY, 0)
    if isinstance(raw_generation, bool) or not isinstance(raw_generation, int) or raw_generation < 0:
        raise InvalidStateTransitionError(
            "LocalSIM economic generation is invalid",
            context={"reason_code": "LOCALSIM_ECONOMIC_GENERATION_INVALID", "run_id": run_id},
        )
    receipt = LocalSimEconomicReceiptV1(
        run_id=run_id,
        binding_id=binding_id,
        trade_date=trade_date,
        plan_id=plan_id,
        generation=raw_generation + 1,
        economic_facts=economic_facts,
    )
    outbox = LocalSimProjectionOutboxV1(
        receipt_id=receipt.receipt_id,
        run_id=run_id,
        plan_id=plan_id,
        generation=receipt.generation,
        economic_hash=receipt.economic_hash,
        projection_payload=projection_payload,
    )
    merged = merge_states()
    receipts[receipt.receipt_id] = receipt
    merged[LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY] = {
        key: value.model_dump(mode="json") for key, value in sorted(receipts.items(), key=lambda row: row[1].generation)
    }
    merged[LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY] = outbox.model_dump(mode="json")
    merged[LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY] = receipt.generation
    return merged, receipt, outbox, True


def _merge_local_sim_projection_success(
    *,
    run_id: str,
    payload: dict[str, Any],
    outbox_id: str,
    generation: int,
    projection_result: dict[str, Any],
) -> tuple[dict[str, Any], LocalSimProjectionReceiptV1]:
    outbox = _local_sim_projection_outbox(payload)
    if outbox is None or outbox.outbox_id != outbox_id or outbox.generation != generation:
        raise InvalidStateTransitionError(
            "LocalSIM projection outbox identity changed before projection commit",
            context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_CAS_CONFLICT", "run_id": run_id},
        )
    if outbox.status == LocalSimProjectionOutboxStatus.PROJECTED:
        existing = next(
            (item for item in _local_sim_projection_receipt_map(payload).values() if item.outbox_id == outbox_id), None
        )
        if existing is None:
            raise InvalidStateTransitionError(
                "LocalSIM projected outbox is missing its projection receipt",
                context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING", "run_id": run_id},
            )
        return dict(payload), existing
    receipt = LocalSimProjectionReceiptV1(
        outbox_id=outbox.outbox_id,
        run_id=run_id,
        generation=outbox.generation,
        economic_hash=outbox.economic_hash,
        projection_payload_hash=outbox.projection_payload_hash,
        projection_hash=canonical_json_sha256(projection_result),
    )
    updated = outbox.model_copy(
        update={
            "status": LocalSimProjectionOutboxStatus.PROJECTED,
            "attempt_count": outbox.attempt_count + 1,
            "last_error": None,
            "updated_at": datetime.now(UTC),
        }
    )
    receipts = _local_sim_projection_receipt_map(payload)
    receipts[receipt.projection_receipt_id] = receipt
    merged = dict(payload)
    merged[LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY] = updated.model_dump(mode="json")
    merged[LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY] = {
        key: value.model_dump(mode="json") for key, value in sorted(receipts.items(), key=lambda row: row[1].generation)
    }
    return merged, receipt


def _merge_local_sim_projection_retryable(
    *, run_id: str, payload: dict[str, Any], outbox_id: str, error: dict[str, Any]
) -> dict[str, Any]:
    outbox = _local_sim_projection_outbox(payload)
    if outbox is None or outbox.outbox_id != outbox_id or outbox.status == LocalSimProjectionOutboxStatus.PROJECTED:
        raise InvalidStateTransitionError(
            "LocalSIM projection retry state CAS failed",
            context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_CAS_CONFLICT", "run_id": run_id},
        )
    updated = outbox.model_copy(
        update={
            "status": LocalSimProjectionOutboxStatus.PROJECTION_RETRYABLE,
            "attempt_count": outbox.attempt_count + 1,
            "last_error": error,
            "updated_at": datetime.now(UTC),
        }
    )
    merged = dict(payload)
    merged[LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY] = updated.model_dump(mode="json")
    return merged


def _merge_local_sim_projection_terminal(
    *, run_id: str, payload: dict[str, Any], outbox_id: str, error: dict[str, Any]
) -> dict[str, Any]:
    outbox = _local_sim_projection_outbox(payload)
    if outbox is None or outbox.outbox_id != outbox_id or outbox.status == LocalSimProjectionOutboxStatus.PROJECTED:
        raise InvalidStateTransitionError(
            "LocalSIM projection terminal state CAS failed",
            context={
                "reason_code": "LOCALSIM_PROJECTION_OUTBOX_CAS_CONFLICT",
                "run_id": run_id,
            },
        )
    attempt_count = outbox.attempt_count + 1
    updated = outbox.model_copy(
        update={
            "status": LocalSimProjectionOutboxStatus.PROJECTION_RETRYABLE,
            "attempt_count": attempt_count,
            "last_error": error,
            "updated_at": datetime.now(UTC),
        }
    )
    merged = dict(payload)
    merged[LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY] = updated.model_dump(mode="json")
    merged[LOCAL_SIM_PROJECTION_TERMINAL_FAILURE_PAYLOAD_KEY] = {
        "schema_version": "local_sim_projection_terminal_failure_v1",
        "run_id": run_id,
        "outbox_id": outbox.outbox_id,
        "generation": outbox.generation,
        "attempt_count": attempt_count,
        "error": error,
        "failed_at": datetime.now(UTC).isoformat(),
    }
    return merged


def _is_daily_selection_evidence_v2(evidence: DailySelectionEvidence) -> bool:
    return (evidence.evidence_payload_json or {}).get("schema_version") == "daily_selection_evidence_v2"


def _validate_daily_selection_evidence_v2(evidence: DailySelectionEvidence) -> None:
    """Validate the typed payload at the persistence boundary as well."""
    from backend.services.selection_center.prospective_evidence import DailySelectionEvidenceV2Payload

    DailySelectionEvidenceV2Payload.model_validate(evidence.evidence_payload_json)


class SimulationRuntimeRepository:
    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    @contextmanager
    def local_sim_economic_transaction_scope(self) -> Iterator[None]:
        yield

    def save_strategy_runtime_release(self, release: StrategyRuntimeRelease) -> StrategyRuntimeRelease:
        existing_by_hash = self.get_strategy_runtime_release_by_hash(release.release_hash or "")
        if existing_by_hash is not None:
            return existing_by_hash
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO strategy_pkg.strategy_runtime_release (
                            release_id, package_id, manifest_sha256, base_release_id,
                            runtime_profile_id, runtime_profile_version_id, runtime_profile_sha256,
                            daily_strategy_profile_version_id, execution_policy_version_id,
                            execution_policy_sha256, tail_policy_version_id, tail_policy_sha256,
                            release_config_json, release_hash, validation_state, validation_evidence,
                            effective_from, effective_to, created_by, created_reason, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            release.release_id,
                            release.package_id,
                            release.manifest_sha256,
                            release.base_release_id,
                            release.runtime_profile_id,
                            release.runtime_profile_version_id,
                            release.runtime_profile_sha256,
                            release.daily_strategy_profile_version_id,
                            release.execution_policy_version_id,
                            release.execution_policy_sha256,
                            release.tail_policy_version_id,
                            release.tail_policy_sha256,
                            psycopg2.extras.Json(release.release_config_json),
                            release.release_hash,
                            release.validation_state.value,
                            psycopg2.extras.Json(release.validation_evidence),
                            release.effective_from,
                            release.effective_to,
                            release.created_by,
                            release.created_reason,
                            release.created_at,
                            release.updated_at,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "strategy runtime release conflicts with an existing immutable release",
                        context={"release_id": release.release_id, "release_hash": release.release_hash},
                    ) from exc
        return release

    def get_strategy_runtime_release(self, release_id: str) -> StrategyRuntimeRelease:
        rows = self._fetch_rows(
            "SELECT * FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s",
            (release_id,),
        )
        if not rows:
            raise DataUnavailableError("strategy runtime release does not exist", context={"release_id": release_id})
        return self._release_from_row(rows[0])

    def get_strategy_runtime_release_by_hash(self, release_hash: str) -> StrategyRuntimeRelease | None:
        if not release_hash:
            return None
        rows = self._fetch_rows(
            "SELECT * FROM strategy_pkg.strategy_runtime_release WHERE release_hash = %s",
            (release_hash,),
        )
        return self._release_from_row(rows[0]) if rows else None

    def save_simulation_release_binding(self, binding: SimulationReleaseBinding) -> SimulationReleaseBinding:
        existing_by_hash = self.get_simulation_release_binding_by_hash(binding.binding_hash or "")
        if existing_by_hash is not None:
            return existing_by_hash
        self.get_strategy_runtime_release(binding.release_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.simulation_release_binding (
                            binding_id, strategy_id, release_id, release_hash, package_id, manifest_sha256,
                            broker_backend, broker_account_id, account_group_id, strategy_slot_id,
                            capital_allocation, strategy_name,
                            order_remark_prefix, effective_from, effective_to, approval_state,
                            binding_config_json, binding_hash, created_by, created_reason, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s
                        )
                        """,
                        (
                            binding.binding_id,
                            binding.strategy_id,
                            binding.release_id,
                            binding.release_hash,
                            binding.package_id,
                            binding.manifest_sha256,
                            binding.broker_backend.value,
                            binding.broker_account_id,
                            binding.account_group_id,
                            binding.strategy_slot_id,
                            binding.capital_allocation,
                            binding.strategy_name,
                            binding.order_remark_prefix,
                            binding.effective_from,
                            binding.effective_to,
                            binding.approval_state.value,
                            psycopg2.extras.Json(binding.binding_config_json),
                            binding.binding_hash,
                            binding.created_by,
                            binding.created_reason,
                            binding.created_at,
                            binding.updated_at,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "simulation release binding conflicts with an existing immutable binding",
                        context={"binding_id": binding.binding_id, "binding_hash": binding.binding_hash},
                    ) from exc
        return binding

    def migrate_miniqmt_binding_route(
        self,
        *,
        source_binding_id: str,
        expected_source_binding_hash: str,
        source_effective_to: date,
        target_binding: SimulationReleaseBinding,
    ) -> tuple[SimulationReleaseBinding, SimulationReleaseBinding]:
        """Atomically close one immutable LEGACY window and insert its B0 successor."""

        self.get_strategy_runtime_release(target_binding.release_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM paper_v2.simulation_release_binding WHERE binding_id = %s FOR UPDATE",
                    (source_binding_id,),
                )
                source_row = cur.fetchone()
                if source_row is None:
                    raise DataUnavailableError(
                        "MiniQMT route migration source binding does not exist",
                        context={"source_binding_id": source_binding_id},
                    )
                source = self._binding_from_row(dict(source_row))
                if source.binding_hash != expected_source_binding_hash:
                    raise InvalidStateTransitionError(
                        "MiniQMT route migration source binding hash changed",
                        context={
                            "reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_CAS_CONFLICT",
                            "source_binding_id": source_binding_id,
                            "expected_binding_hash": expected_source_binding_hash,
                            "actual_binding_hash": source.binding_hash,
                        },
                    )
                if source.effective_to not in {None, source_effective_to}:
                    raise InvalidStateTransitionError(
                        "MiniQMT route migration source effective window conflicts with requested cutover",
                        context={
                            "reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_WINDOW_CONFLICT",
                            "source_binding_id": source_binding_id,
                            "expected_effective_to": source_effective_to.isoformat(),
                            "actual_effective_to": source.effective_to.isoformat() if source.effective_to else None,
                        },
                    )

                marker = (
                    target_binding.binding_config_json.get("metadata", {}).get("miniqmt_route_migration")
                    if isinstance(target_binding.binding_config_json.get("metadata"), dict)
                    else None
                )
                if not isinstance(marker, dict):
                    raise InvalidStateTransitionError(
                        "MiniQMT route migration target binding is missing its durable marker",
                        context={"reason_code": "MINIQMT_ROUTE_MIGRATION_MARKER_MISSING"},
                    )
                cur.execute(
                    """
                    SELECT binding_id, binding_hash
                    FROM paper_v2.simulation_release_binding
                    WHERE strategy_id = %s
                      AND broker_backend = 'minqmt_sim'
                      AND binding_config_json->'metadata'->'miniqmt_route_migration'->>'source_binding_id' = %s
                      AND binding_config_json->'metadata'->'miniqmt_route_migration'->>'effective_trade_date' = %s
                      AND binding_hash <> %s
                    LIMIT 1
                    """,
                    (
                        source.strategy_id,
                        source_binding_id,
                        str(marker.get("effective_trade_date") or ""),
                        target_binding.binding_hash,
                    ),
                )
                conflicting_target = cur.fetchone()
                if conflicting_target is not None:
                    raise InvalidStateTransitionError(
                        "MiniQMT route migration already has a different target for this source/date",
                        context={
                            "reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_CONFLICT",
                            "source_binding_id": source_binding_id,
                            "conflicting_binding_id": conflicting_target["binding_id"],
                            "conflicting_binding_hash": conflicting_target["binding_hash"],
                        },
                    )

                cur.execute(
                    "SELECT * FROM paper_v2.simulation_release_binding WHERE binding_hash = %s",
                    (target_binding.binding_hash,),
                )
                target_row = cur.fetchone()
                if target_row is None:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.simulation_release_binding (
                            binding_id, strategy_id, release_id, release_hash, package_id, manifest_sha256,
                            broker_backend, broker_account_id, account_group_id, strategy_slot_id,
                            capital_allocation, strategy_name, order_remark_prefix, effective_from, effective_to,
                            approval_state, binding_config_json, binding_hash, created_by, created_reason,
                            created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s
                        )
                        """,
                        (
                            target_binding.binding_id,
                            target_binding.strategy_id,
                            target_binding.release_id,
                            target_binding.release_hash,
                            target_binding.package_id,
                            target_binding.manifest_sha256,
                            target_binding.broker_backend.value,
                            target_binding.broker_account_id,
                            target_binding.account_group_id,
                            target_binding.strategy_slot_id,
                            target_binding.capital_allocation,
                            target_binding.strategy_name,
                            target_binding.order_remark_prefix,
                            target_binding.effective_from,
                            target_binding.effective_to,
                            target_binding.approval_state.value,
                            psycopg2.extras.Json(target_binding.binding_config_json),
                            target_binding.binding_hash,
                            target_binding.created_by,
                            target_binding.created_reason,
                            target_binding.created_at,
                            target_binding.updated_at,
                        ),
                    )
                else:
                    persisted_target = self._binding_from_row(dict(target_row))
                    if persisted_target.model_dump(mode="json") != target_binding.model_dump(mode="json"):
                        raise InvalidStateTransitionError(
                            "MiniQMT route migration target hash readback differs from the requested binding",
                            context={
                                "reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_HASH_CONFLICT",
                                "target_binding_id": persisted_target.binding_id,
                                "target_binding_hash": persisted_target.binding_hash,
                            },
                        )

                if source.effective_to is None:
                    cur.execute(
                        """
                        UPDATE paper_v2.simulation_release_binding
                        SET effective_to = %s, updated_at = NOW()
                        WHERE binding_id = %s AND binding_hash = %s AND effective_to IS NULL
                        """,
                        (source_effective_to, source_binding_id, expected_source_binding_hash),
                    )
                    if cur.rowcount != 1:
                        raise InvalidStateTransitionError(
                            "MiniQMT route migration source window CAS update failed",
                            context={
                                "reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_CAS_CONFLICT",
                                "source_binding_id": source_binding_id,
                            },
                        )

                cur.execute(
                    "SELECT * FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                    (source_binding_id,),
                )
                persisted_source_row = cur.fetchone()
                cur.execute(
                    "SELECT * FROM paper_v2.simulation_release_binding WHERE binding_hash = %s",
                    (target_binding.binding_hash,),
                )
                persisted_target_row = cur.fetchone()
                if persisted_source_row is None or persisted_target_row is None:
                    raise InvalidStateTransitionError(
                        "MiniQMT route migration transaction readback is incomplete",
                        context={"reason_code": "MINIQMT_ROUTE_MIGRATION_TRANSACTION_READBACK_MISSING"},
                    )
                persisted_source = self._binding_from_row(dict(persisted_source_row))
                persisted_target = self._binding_from_row(dict(persisted_target_row))
                if persisted_source.effective_to != source_effective_to:
                    raise InvalidStateTransitionError(
                        "MiniQMT route migration source window transaction readback differs",
                        context={"reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_WINDOW_READBACK_MISMATCH"},
                    )
        return persisted_source, persisted_target

    def get_simulation_release_binding(self, binding_id: str) -> SimulationReleaseBinding:
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
            (binding_id,),
        )
        if not rows:
            raise DataUnavailableError("simulation release binding does not exist", context={"binding_id": binding_id})
        return self._binding_from_row(rows[0])

    def get_simulation_release_binding_by_hash(self, binding_hash: str) -> SimulationReleaseBinding | None:
        if not binding_hash:
            return None
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.simulation_release_binding WHERE binding_hash = %s",
            (binding_hash,),
        )
        return self._binding_from_row(rows[0]) if rows else None

    def find_miniqmt_route_migration_target(
        self,
        *,
        source_binding_id: str,
        effective_trade_date: date,
    ) -> SimulationReleaseBinding | None:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.simulation_release_binding
            WHERE broker_backend = 'minqmt_sim'
              AND binding_config_json->'metadata'->'miniqmt_route_migration'->>'source_binding_id' = %s
              AND binding_config_json->'metadata'->'miniqmt_route_migration'->>'effective_trade_date' = %s
            ORDER BY created_at DESC, binding_id DESC
            LIMIT 2
            """,
            (source_binding_id, effective_trade_date.isoformat()),
        )
        if len(rows) > 1:
            raise InvalidStateTransitionError(
                "MiniQMT route migration has multiple durable targets for one source/date",
                context={
                    "reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_CONFLICT",
                    "source_binding_id": source_binding_id,
                    "effective_trade_date": effective_trade_date.isoformat(),
                    "target_binding_ids": [str(row["binding_id"]) for row in rows],
                },
            )
        return self._binding_from_row(rows[0]) if rows else None

    def list_simulation_release_bindings(
        self,
        *,
        strategy_id: str | None = None,
        release_id: str | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        approval_states: Iterable[SimulationBindingApprovalState | str] | None = None,
        active_on: date | None = None,
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        clauses: list[str] = []
        params: list[Any] = []
        if strategy_id is not None:
            clauses.append("strategy_id = %s")
            params.append(strategy_id)
        if release_id is not None:
            clauses.append("release_id = %s")
            params.append(release_id)
        if broker_backend is not None:
            backend = (
                broker_backend.value if isinstance(broker_backend, SimulationBrokerBackend) else str(broker_backend)
            )
            clauses.append("broker_backend = %s")
            params.append(backend)
        states = [
            state.value if isinstance(state, SimulationBindingApprovalState) else str(state)
            for state in (approval_states or [])
        ]
        if states:
            placeholders = ", ".join(["%s"] * len(states))
            clauses.append(f"approval_state IN ({placeholders})")
            params.extend(states)
        if active_on is not None:
            clauses.append("(effective_from IS NULL OR effective_from <= %s)")
            params.append(active_on)
            clauses.append("(effective_to IS NULL OR effective_to >= %s)")
            params.append(active_on)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._fetch_rows(
            f"""
            SELECT *
            FROM paper_v2.simulation_release_binding
            {where}
            ORDER BY created_at DESC, binding_id
            LIMIT %s
            """,
            tuple(params),
        )
        return [self._binding_from_row(row) for row in rows]

    def list_latest_simulation_release_bindings(
        self,
        *,
        strategy_id: str | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        approval_states: Iterable[SimulationBindingApprovalState | str] | None = None,
        effective_from_on_or_before: date | None = None,
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        clauses: list[str] = []
        params: list[Any] = []
        if strategy_id is not None:
            clauses.append("strategy_id = %s")
            params.append(strategy_id)
        if broker_backend is not None:
            backend = (
                broker_backend.value if isinstance(broker_backend, SimulationBrokerBackend) else str(broker_backend)
            )
            clauses.append("broker_backend = %s")
            params.append(backend)
        states = [
            state.value if isinstance(state, SimulationBindingApprovalState) else str(state)
            for state in (approval_states or [])
        ]
        if states:
            placeholders = ", ".join(["%s"] * len(states))
            clauses.append(f"approval_state IN ({placeholders})")
            params.extend(states)
        if effective_from_on_or_before is not None:
            clauses.append("(effective_from IS NULL OR effective_from <= %s)")
            params.append(effective_from_on_or_before)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._fetch_rows(
            f"""
            SELECT *
            FROM (
                SELECT DISTINCT ON (strategy_id, broker_backend) *
                FROM paper_v2.simulation_release_binding
                {where}
                ORDER BY strategy_id, broker_backend, effective_from DESC NULLS LAST, created_at DESC, binding_id DESC
            ) latest
            ORDER BY created_at DESC, binding_id DESC
            LIMIT %s
            """,
            tuple(params),
        )
        return [self._binding_from_row(row) for row in rows]

    def save_daily_selection_evidence(self, evidence: DailySelectionEvidence) -> DailySelectionEvidence:
        if _is_daily_selection_evidence_v2(evidence):
            _validate_daily_selection_evidence_v2(evidence)
            return self._save_daily_selection_evidence_v2(evidence)
        existing_by_hash = self.get_daily_selection_evidence_by_hash(evidence.artifact_hash)
        if existing_by_hash is not None:
            return existing_by_hash
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO selection.daily_selection_evidence (
                            evidence_id, target_trade_date, cutoff_date, package_id, manifest_sha256,
                            release_id, release_hash, runtime_profile_version_id, runtime_profile_hash,
                            source_type, data_source, candidate_count, excluded_count, artifact_hash,
                            evidence_payload_json, created_at, created_by
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            evidence.evidence_id,
                            evidence.target_trade_date,
                            evidence.cutoff_date,
                            evidence.package_id,
                            evidence.manifest_sha256,
                            evidence.release_id,
                            evidence.release_hash,
                            evidence.runtime_profile_version_id,
                            evidence.runtime_profile_hash,
                            evidence.source_type,
                            evidence.data_source,
                            evidence.candidate_count,
                            evidence.excluded_count,
                            evidence.artifact_hash,
                            psycopg2.extras.Json(evidence.evidence_payload_json),
                            evidence.created_at,
                            evidence.created_by,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "daily selection evidence conflicts with an existing immutable evidence row",
                        context={"evidence_id": evidence.evidence_id, "artifact_hash": evidence.artifact_hash},
                    ) from exc
        return evidence

    def _save_daily_selection_evidence_v2(self, evidence: DailySelectionEvidence) -> DailySelectionEvidence:
        """Insert-or-compare for the v2 content-addressed prospective contract."""
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO selection.daily_selection_evidence (
                        evidence_id, target_trade_date, cutoff_date, package_id, manifest_sha256,
                        release_id, release_hash, runtime_profile_version_id, runtime_profile_hash,
                        source_type, data_source, candidate_count, excluded_count, artifact_hash,
                        evidence_payload_json, created_at, created_by
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    self._daily_selection_evidence_insert_params(evidence),
                )
                cur.execute(
                    """
                    SELECT *
                    FROM selection.daily_selection_evidence
                    WHERE evidence_id = %s OR artifact_hash = %s
                    ORDER BY evidence_id
                    """,
                    (evidence.evidence_id, evidence.artifact_hash),
                )
                rows = cur.fetchall()
        if len(rows) != 1:
            raise InvalidStateTransitionError(
                "daily selection evidence v2 conflicts with an immutable identity",
                context={"evidence_id": evidence.evidence_id, "artifact_hash": evidence.artifact_hash},
            )
        stored = self._evidence_from_row(dict(rows[0]))
        self._assert_same_v2_evidence(stored, evidence)
        return stored

    @staticmethod
    def _daily_selection_evidence_insert_params(evidence: DailySelectionEvidence) -> tuple[Any, ...]:
        return (
            evidence.evidence_id,
            evidence.target_trade_date,
            evidence.cutoff_date,
            evidence.package_id,
            evidence.manifest_sha256,
            evidence.release_id,
            evidence.release_hash,
            evidence.runtime_profile_version_id,
            evidence.runtime_profile_hash,
            evidence.source_type,
            evidence.data_source,
            evidence.candidate_count,
            evidence.excluded_count,
            evidence.artifact_hash,
            psycopg2.extras.Json(evidence.evidence_payload_json),
            evidence.created_at,
            evidence.created_by,
        )

    @staticmethod
    def _assert_same_v2_evidence(stored: DailySelectionEvidence, requested: DailySelectionEvidence) -> None:
        fields = (
            "evidence_id",
            "target_trade_date",
            "cutoff_date",
            "package_id",
            "manifest_sha256",
            "release_id",
            "release_hash",
            "runtime_profile_version_id",
            "runtime_profile_hash",
            "source_type",
            "data_source",
            "candidate_count",
            "excluded_count",
            "artifact_hash",
            "evidence_payload_json",
        )
        if any(getattr(stored, field) != getattr(requested, field) for field in fields):
            raise InvalidStateTransitionError(
                "daily selection evidence v2 conflicts with immutable content",
                context={"evidence_id": requested.evidence_id, "artifact_hash": requested.artifact_hash},
            )

    def get_daily_selection_evidence(self, evidence_id: str) -> DailySelectionEvidence:
        rows = self._fetch_rows(
            "SELECT * FROM selection.daily_selection_evidence WHERE evidence_id = %s",
            (evidence_id,),
        )
        if not rows:
            raise DataUnavailableError("daily selection evidence does not exist", context={"evidence_id": evidence_id})
        return self._evidence_from_row(rows[0])

    def get_daily_selection_evidence_by_hash(self, artifact_hash: str) -> DailySelectionEvidence | None:
        if not artifact_hash:
            return None
        rows = self._fetch_rows(
            "SELECT * FROM selection.daily_selection_evidence WHERE artifact_hash = %s",
            (artifact_hash,),
        )
        return self._evidence_from_row(rows[0]) if rows else None

    def save_execution_plan(self, plan: ExecutionPlan) -> ExecutionPlan:
        existing_by_hash = self.get_execution_plan_by_hash(plan.plan_hash)
        if existing_by_hash is not None:
            return existing_by_hash
        self.get_strategy_runtime_release(plan.release_id)
        self.get_simulation_release_binding(plan.binding_id)
        self.get_daily_selection_evidence(plan.selection_evidence_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.execution_plan (
                            plan_id, strategy_id, portfolio_id, package_id, release_id, release_hash,
                            binding_id, binding_hash, selection_evidence_id, selection_evidence_hash,
                            target_trade_date, execution_policy_version_id, execution_policy_sha256,
                            tail_policy_version_id, tail_policy_sha256, intent_count,
                            trading_rule_decision_count, plan_payload_json, plan_hash, created_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            plan.plan_id,
                            plan.strategy_id,
                            plan.portfolio_id,
                            plan.package_id,
                            plan.release_id,
                            plan.release_hash,
                            plan.binding_id,
                            plan.binding_hash,
                            plan.selection_evidence_id,
                            plan.selection_evidence_hash,
                            plan.target_trade_date,
                            plan.execution_policy_version_id,
                            plan.execution_policy_sha256,
                            plan.tail_policy_version_id,
                            plan.tail_policy_sha256,
                            len(plan.intents),
                            len(plan.trading_rule_decisions),
                            psycopg2.extras.Json(plan.plan_payload_json),
                            plan.plan_hash,
                            plan.created_at,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "execution plan conflicts with an existing immutable plan",
                        context={"plan_id": plan.plan_id, "plan_hash": plan.plan_hash},
                    ) from exc
        return plan

    def get_execution_plan(self, plan_id: str) -> ExecutionPlan:
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.execution_plan WHERE plan_id = %s",
            (plan_id,),
        )
        if not rows:
            raise DataUnavailableError("execution plan does not exist", context={"plan_id": plan_id})
        return self._execution_plan_from_row(rows[0])

    def get_execution_plan_by_hash(self, plan_hash: str) -> ExecutionPlan | None:
        if not plan_hash:
            return None
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.execution_plan WHERE plan_hash = %s",
            (plan_hash,),
        )
        return self._execution_plan_from_row(rows[0]) if rows else None

    def save_simulation_daily_run(self, run: SimulationDailyRun) -> SimulationDailyRun:
        existing = self.get_simulation_daily_run_by_key(
            strategy_id=run.strategy_id,
            binding_id=run.binding_id,
            trade_date=run.trade_date,
        )
        if existing is not None:
            return existing
        self.get_strategy_runtime_release(run.release_id)
        binding = self.get_simulation_release_binding(run.binding_id)
        run = self._daily_run_with_binding_slots(run, binding)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.simulation_daily_run (
                            run_id, trade_date, strategy_id, broker_backend, package_id, manifest_sha256,
                            release_id, release_hash, binding_id, binding_hash,
                            account_group_id, strategy_slot_id,
                            selection_evidence_id, selection_artifact_hash, execution_plan_id,
                            execution_plan_hash, status, run_payload_json, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            run.run_id,
                            run.trade_date,
                            run.strategy_id,
                            run.broker_backend.value,
                            run.package_id,
                            run.manifest_sha256,
                            run.release_id,
                            run.release_hash,
                            run.binding_id,
                            run.binding_hash,
                            run.account_group_id,
                            run.strategy_slot_id,
                            run.selection_evidence_id,
                            run.selection_artifact_hash,
                            run.execution_plan_id,
                            run.execution_plan_hash,
                            run.status.value,
                            psycopg2.extras.Json(run.run_payload_json),
                            run.created_at,
                            run.updated_at,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "simulation daily run conflicts with an existing run",
                        context={"run_id": run.run_id, "strategy_id": run.strategy_id, "binding_id": run.binding_id},
                    ) from exc
        return run

    def get_simulation_daily_run(self, run_id: str) -> SimulationDailyRun:
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.simulation_daily_run WHERE run_id = %s",
            (run_id,),
        )
        if not rows:
            raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
        return self._daily_run_from_row(rows[0])

    def get_simulation_daily_run_by_key(
        self,
        *,
        strategy_id: str,
        binding_id: str,
        trade_date: Any,
    ) -> SimulationDailyRun | None:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.simulation_daily_run
            WHERE strategy_id = %s AND binding_id = %s AND trade_date = %s
            """,
            (strategy_id, binding_id, trade_date),
        )
        return self._daily_run_from_row(rows[0]) if rows else None

    def list_simulation_daily_runs(
        self,
        *,
        trade_date: Any | None = None,
        trade_date_before: Any | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        status: SimulationDailyRunStatus | str | None = None,
        limit: int = 100,
    ) -> list[SimulationDailyRun]:
        clauses: list[str] = []
        params: list[Any] = []
        if trade_date is not None:
            clauses.append("trade_date = %s")
            params.append(trade_date)
        if trade_date_before is not None:
            clauses.append("trade_date < %s")
            params.append(trade_date_before)
        if broker_backend is not None:
            backend = (
                broker_backend.value if isinstance(broker_backend, SimulationBrokerBackend) else str(broker_backend)
            )
            clauses.append("broker_backend = %s")
            params.append(backend)
        if strategy_id is not None:
            clauses.append("strategy_id = %s")
            params.append(strategy_id)
        if status is not None:
            status_value = status.value if isinstance(status, SimulationDailyRunStatus) else str(status)
            clauses.append("status = %s")
            params.append(status_value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._fetch_rows(
            f"""
            SELECT *
            FROM paper_v2.simulation_daily_run
            {where}
            ORDER BY trade_date DESC, updated_at DESC, created_at DESC, run_id
            LIMIT %s
            """,
            tuple(params),
        )
        return [self._daily_run_from_row(row) for row in rows]

    def update_simulation_daily_run(
        self,
        run_id: str,
        *,
        status: SimulationDailyRunStatus | None = None,
        selection_evidence: DailySelectionEvidence | None = None,
        execution_plan: ExecutionPlan | None = None,
        payload_patch: dict[str, Any] | None = None,
        payload_unset: Iterable[str] | None = None,
    ) -> SimulationDailyRun:
        current = self.get_simulation_daily_run(run_id)
        merged_payload = {**current.run_payload_json, **(payload_patch or {})}
        merged_payload = preserve_tca_sidecar(current.run_payload_json, merged_payload)
        for key in payload_unset or ():
            merged_payload.pop(str(key), None)
        updated = current.model_copy(
            update={
                "status": status or current.status,
                "selection_evidence_id": selection_evidence.evidence_id
                if selection_evidence
                else current.selection_evidence_id,
                "selection_artifact_hash": selection_evidence.artifact_hash
                if selection_evidence
                else current.selection_artifact_hash,
                "execution_plan_id": execution_plan.plan_id if execution_plan else current.execution_plan_id,
                "execution_plan_hash": execution_plan.plan_hash if execution_plan else current.execution_plan_hash,
                "run_payload_json": merged_payload,
            }
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.simulation_daily_run
                    SET status = %s,
                        selection_evidence_id = %s,
                        selection_artifact_hash = %s,
                        execution_plan_id = %s,
                        execution_plan_hash = %s,
                        run_payload_json = %s,
                        updated_at = now()
                    WHERE run_id = %s
                    """,
                    (
                        updated.status.value,
                        updated.selection_evidence_id,
                        updated.selection_artifact_hash,
                        updated.execution_plan_id,
                        updated.execution_plan_hash,
                        psycopg2.extras.Json(updated.run_payload_json),
                        run_id,
                    ),
                )
        return self.get_simulation_daily_run(run_id)

    def claim_simulation_retry_attempt(
        self,
        *,
        run_id: str,
        retry_key: str,
        source_fingerprint: str,
        as_of_time: datetime,
        lease_seconds: int,
    ) -> SimulationRetryAttemptDecision:
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
                current_payload = dict(row.get("run_payload_json") or {})
                next_payload, should_execute, reason, retry_entry, claim_token = _claim_retry_attempt_payload(
                    payload=current_payload,
                    retry_key=retry_key,
                    source_fingerprint=source_fingerprint,
                    as_of_time=as_of_time,
                    lease_seconds=lease_seconds,
                )
                if next_payload != current_payload:
                    cur.execute(
                        """
                        UPDATE paper_v2.simulation_daily_run
                        SET run_payload_json = %s, updated_at = now()
                        WHERE run_id = %s
                        """,
                        (psycopg2.extras.Json(next_payload), run_id),
                    )
        readback = self.get_simulation_daily_run(run_id)
        _simulation_retry_state(readback.run_payload_json)
        return SimulationRetryAttemptDecision(
            run=readback,
            should_execute=should_execute,
            reason=reason,
            retry_entry=deepcopy(retry_entry),
            claim_token=claim_token,
        )

    def record_simulation_retry_failure(
        self,
        *,
        run_id: str,
        retry_key: str,
        source_fingerprint: str,
        failure_fingerprint: str,
        failure_stage: str,
        error: dict[str, Any],
        as_of_time: datetime,
        base_delay_seconds: int,
        max_delay_seconds: int,
        expected_claim_token: str | None = None,
    ) -> SimulationDailyRun:
        if (
            type(base_delay_seconds) is not int
            or type(max_delay_seconds) is not int
            or base_delay_seconds <= 0
            or max_delay_seconds < base_delay_seconds
        ):
            raise ValueError("retry delay bounds are invalid")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
                next_payload, _ = _record_retry_failure_payload(
                    payload=dict(row.get("run_payload_json") or {}),
                    retry_key=retry_key,
                    source_fingerprint=source_fingerprint,
                    failure_fingerprint=failure_fingerprint,
                    failure_stage=failure_stage,
                    error=error,
                    as_of_time=as_of_time,
                    base_delay_seconds=base_delay_seconds,
                    max_delay_seconds=max_delay_seconds,
                    expected_claim_token=expected_claim_token,
                )
                cur.execute(
                    """
                    UPDATE paper_v2.simulation_daily_run
                    SET run_payload_json = %s, updated_at = now()
                    WHERE run_id = %s
                    """,
                    (psycopg2.extras.Json(next_payload), run_id),
                )
        readback = self.get_simulation_daily_run(run_id)
        _simulation_retry_state(readback.run_payload_json)
        return readback

    def clear_simulation_retry_control(
        self,
        *,
        run_id: str,
        retry_key: str,
        expected_claim_token: str | None = None,
    ) -> SimulationDailyRun:
        retry_key = _retry_required_text(retry_key, field="retry_key")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
                current_payload = dict(row.get("run_payload_json") or {})
                control, claims_control = _simulation_retry_state(current_payload)
                entries = dict(control["entries"]) if control is not None else {}
                claims = dict(claims_control["claims"]) if claims_control is not None else {}
                active_token = simulation_retry_claim_token(current_payload, retry_key=retry_key)
                if expected_claim_token is not None:
                    expected_claim_token = _retry_sha256(expected_claim_token, field="expected_claim_token")
                    if active_token != expected_claim_token:
                        raise InvalidStateTransitionError(
                            "simulation scheduler retry success writer no longer owns the attempt claim",
                            context={
                                "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_STALE_WRITER",
                                "retry_key": retry_key,
                                "expected_claim_token": expected_claim_token,
                                "actual_claim_token": active_token,
                            },
                        )
                elif active_token is not None:
                    raise InvalidStateTransitionError(
                        "simulation scheduler retry success writer omitted the active attempt claim token",
                        context={
                            "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_TOKEN_REQUIRED",
                            "retry_key": retry_key,
                            "active_claim_token": active_token,
                        },
                    )
                if retry_key in entries or retry_key in claims:
                    entries.pop(retry_key, None)
                    claims.pop(retry_key, None)
                    next_payload = _retry_control_payload_with_entries(current_payload, entries)
                    next_payload = _retry_claims_payload_with_claims(next_payload, claims)
                    cur.execute(
                        """
                        UPDATE paper_v2.simulation_daily_run
                        SET run_payload_json = %s, updated_at = now()
                        WHERE run_id = %s
                        """,
                        (psycopg2.extras.Json(next_payload), run_id),
                    )
        readback = self.get_simulation_daily_run(run_id)
        _simulation_retry_state(readback.run_payload_json)
        return readback

    def list_local_sim_execution_states(
        self, run_id: str, *, authoritative: bool = False
    ) -> list[LocalSimExecutionStateV1]:
        run = self.get_simulation_daily_run(run_id)
        state_map = _local_sim_state_map(run.run_payload_json)
        if authoritative:
            state_map = _local_sim_state_authority_closure(
                run_id=run.run_id,
                binding_id=run.binding_id,
                trade_date=run.trade_date,
                plan_id=run.execution_plan_id,
                payload=run.run_payload_json,
                states=state_map,
            ).states
        states = list(state_map.values())
        states.sort(key=lambda item: (item.intent_id, item.algo_instance_id, item.state_id))
        return states

    def commit_local_sim_execution_states(
        self,
        *,
        run_id: str,
        states: Iterable[LocalSimExecutionStateV1],
        expected_versions: dict[str, tuple[int, str] | None],
    ) -> list[LocalSimExecutionStateV1]:
        incoming = list(states)
        with self._conn_factory() as conn:
            original_autocommit = bool(getattr(conn, "autocommit", False))
            try:
                if original_autocommit:
                    conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """SELECT run_payload_json FROM paper_v2.simulation_daily_run
                           WHERE run_id = %s FOR UPDATE""",
                        (run_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
                    merged = _merge_local_sim_state_batch(
                        run_id=run_id,
                        payload=dict(row.get("run_payload_json") or {}),
                        states=incoming,
                        expected_versions=expected_versions,
                    )
                    cur.execute(
                        """UPDATE paper_v2.simulation_daily_run
                           SET run_payload_json = %s, updated_at = now() WHERE run_id = %s""",
                        (psycopg2.extras.Json(merged), run_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                if original_autocommit:
                    conn.autocommit = True
        readback = {state.state_id: state for state in self.list_local_sim_execution_states(run_id)}
        for state in incoming:
            persisted = readback.get(state.state_id)
            if persisted is None or persisted.state_hash != state.state_hash:
                raise InvalidStateTransitionError(
                    "LocalSIM durable state independent readback failed",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_READBACK_FAILED",
                        "run_id": run_id,
                        "state_id": state.state_id,
                        "expected_state_hash": state.state_hash,
                        "actual_state_hash": persisted.state_hash if persisted else None,
                    },
                )
        return [readback[state.state_id] for state in incoming]

    def stage_local_sim_economic_commit(
        self,
        *,
        connection: Any,
        run_id: str,
        binding_id: str,
        trade_date: date,
        plan_id: str,
        states: Iterable[LocalSimExecutionStateV1],
        expected_versions: dict[str, tuple[int, str] | None],
        economic_facts: dict[str, Any],
        projection_payload: dict[str, Any],
        status: SimulationDailyRunStatus,
        payload_patch: dict[str, Any],
        payload_unset: Iterable[str] = (),
    ) -> tuple[LocalSimEconomicReceiptV1, LocalSimProjectionOutboxV1, bool]:
        if connection is None:
            raise RuntimeError("PostgreSQL LocalSIM economic commit requires the owning transaction connection")
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s FOR UPDATE", (run_id,)
            )
            row = cur.fetchone()
            if row is None:
                raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
            current_payload = dict(row.get("run_payload_json") or {})
            merged, receipt, outbox, created = _merge_local_sim_economic_event(
                run_id=run_id,
                binding_id=binding_id,
                trade_date=trade_date,
                plan_id=plan_id,
                payload=current_payload,
                states=states,
                expected_versions=expected_versions,
                economic_facts=economic_facts,
                projection_payload=projection_payload,
            )
            if created:
                merged.update(payload_patch)
                for key in payload_unset:
                    merged.pop(str(key), None)
                merged = preserve_tca_sidecar(current_payload, merged)
                cur.execute(
                    "UPDATE paper_v2.simulation_daily_run SET status = %s, run_payload_json = %s, updated_at = now() WHERE run_id = %s",
                    (status.value, psycopg2.extras.Json(merged), run_id),
                )
        return receipt, outbox, created

    def readback_local_sim_economic_commit(
        self, *, run_id: str, receipt: LocalSimEconomicReceiptV1, outbox: LocalSimProjectionOutboxV1
    ) -> SimulationDailyRun:
        run = self.get_simulation_daily_run(run_id)
        _validate_local_sim_economic_readback(run=run, receipt=receipt, outbox=outbox)
        return run

    def stage_local_sim_projection_commit(
        self,
        *,
        connection: Any,
        run_id: str,
        outbox_id: str,
        generation: int,
        final_status: SimulationDailyRunStatus,
        projection_result: dict[str, Any],
        payload_patch: dict[str, Any],
        payload_unset: Iterable[str] = (),
    ) -> LocalSimProjectionReceiptV1:
        if connection is None:
            raise RuntimeError("PostgreSQL LocalSIM projection commit requires the owning transaction connection")
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s FOR UPDATE", (run_id,)
            )
            row = cur.fetchone()
            if row is None:
                raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
            current_payload = dict(row.get("run_payload_json") or {})
            merged, receipt = _merge_local_sim_projection_success(
                run_id=run_id,
                payload=current_payload,
                outbox_id=outbox_id,
                generation=generation,
                projection_result=projection_result,
            )
            merged.update(payload_patch)
            merged.setdefault("local_sim_projection_generation", {})["projection_receipt_id"] = (
                receipt.projection_receipt_id
            )
            for key in payload_unset:
                merged.pop(str(key), None)
            merged = preserve_tca_sidecar(current_payload, merged)
            cur.execute(
                "UPDATE paper_v2.simulation_daily_run SET status = %s, run_payload_json = %s, updated_at = now() WHERE run_id = %s",
                (final_status.value, psycopg2.extras.Json(merged), run_id),
            )
        return receipt

    def mark_local_sim_projection_retryable(
        self, *, run_id: str, outbox_id: str, error: dict[str, Any]
    ) -> SimulationDailyRun:
        with self._conn_factory() as conn:
            original_autocommit = bool(getattr(conn, "autocommit", False))
            try:
                if original_autocommit:
                    conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s FOR UPDATE",
                        (run_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
                    merged = _merge_local_sim_projection_retryable(
                        run_id=run_id, payload=dict(row.get("run_payload_json") or {}), outbox_id=outbox_id, error=error
                    )
                    merged["last_stage"] = SimulationDailyRunStatus.FAILED_RETRYABLE.value
                    cur.execute(
                        "UPDATE paper_v2.simulation_daily_run SET status = %s, run_payload_json = %s, updated_at = now() WHERE run_id = %s",
                        (SimulationDailyRunStatus.FAILED_RETRYABLE.value, psycopg2.extras.Json(merged), run_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                if original_autocommit:
                    conn.autocommit = True
        return self.get_simulation_daily_run(run_id)

    def mark_local_sim_projection_terminal(
        self, *, run_id: str, outbox_id: str, error: dict[str, Any]
    ) -> SimulationDailyRun:
        with self._conn_factory() as conn:
            original_autocommit = bool(getattr(conn, "autocommit", False))
            try:
                if original_autocommit:
                    conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s FOR UPDATE",
                        (run_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise DataUnavailableError(
                            "simulation daily run does not exist",
                            context={"run_id": run_id},
                        )
                    merged = _merge_local_sim_projection_terminal(
                        run_id=run_id,
                        payload=dict(row.get("run_payload_json") or {}),
                        outbox_id=outbox_id,
                        error=error,
                    )
                    merged["last_stage"] = SimulationDailyRunStatus.FAILED_TERMINAL.value
                    cur.execute(
                        "UPDATE paper_v2.simulation_daily_run SET status = %s, "
                        "run_payload_json = %s, updated_at = now() WHERE run_id = %s",
                        (
                            SimulationDailyRunStatus.FAILED_TERMINAL.value,
                            psycopg2.extras.Json(merged),
                            run_id,
                        ),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                if original_autocommit:
                    conn.autocommit = True
        return self.get_simulation_daily_run(run_id)

    def readback_local_sim_projection_commit(
        self, *, run_id: str, receipt: LocalSimProjectionReceiptV1
    ) -> SimulationDailyRun:
        run = self.get_simulation_daily_run(run_id)
        outbox = _local_sim_projection_outbox(run.run_payload_json)
        persisted = _local_sim_projection_receipt_map(run.run_payload_json).get(receipt.projection_receipt_id)
        if (
            outbox is None
            or outbox.outbox_id != receipt.outbox_id
            or outbox.status != LocalSimProjectionOutboxStatus.PROJECTED
        ):
            raise InvalidStateTransitionError(
                "LocalSIM projection outbox status independent readback failed",
                context={"reason_code": "LOCALSIM_PROJECTION_STATUS_READBACK_FAILED", "run_id": run_id},
            )
        if persisted is None or persisted.receipt_hash != receipt.receipt_hash:
            raise InvalidStateTransitionError(
                "LocalSIM projection receipt independent readback failed",
                context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_READBACK_FAILED", "run_id": run_id},
            )
        return run

    def _update_local_sim_projection_readback_state(
        self,
        *,
        run_id: str,
        outbox_id: str,
        status: SimulationDailyRunStatus,
        patch: dict[str, Any],
        unset: Iterable[str] = (),
    ) -> SimulationDailyRun:
        with self._conn_factory() as conn:
            original_autocommit = bool(getattr(conn, "autocommit", False))
            try:
                if original_autocommit:
                    conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s FOR UPDATE",
                        (run_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
                    payload = dict(row.get("run_payload_json") or {})
                    outbox = _local_sim_projection_outbox(payload)
                    if (
                        outbox is None
                        or outbox.outbox_id != outbox_id
                        or outbox.status != LocalSimProjectionOutboxStatus.PROJECTED
                    ):
                        raise InvalidStateTransitionError(
                            "LocalSIM projection readback state CAS failed",
                            context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_CAS_CONFLICT", "run_id": run_id},
                        )
                    payload.update(patch)
                    for key in unset:
                        payload.pop(str(key), None)
                    cur.execute(
                        "UPDATE paper_v2.simulation_daily_run SET status = %s, run_payload_json = %s, updated_at = now() WHERE run_id = %s",
                        (status.value, psycopg2.extras.Json(payload), run_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                if original_autocommit:
                    conn.autocommit = True
        return self.get_simulation_daily_run(run_id)

    def mark_local_sim_projection_readback_retryable(
        self, *, run_id: str, outbox_id: str, error: dict[str, Any]
    ) -> SimulationDailyRun:
        return self._update_local_sim_projection_readback_state(
            run_id=run_id,
            outbox_id=outbox_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            patch={
                "local_sim_projection_readback_failure": error,
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            },
        )

    def clear_local_sim_projection_readback_failure(
        self, *, run_id: str, outbox_id: str, final_status: SimulationDailyRunStatus
    ) -> SimulationDailyRun:
        return self._update_local_sim_projection_readback_state(
            run_id=run_id,
            outbox_id=outbox_id,
            status=final_status,
            patch={"last_stage": final_status.value},
            unset=("local_sim_projection_readback_failure", "submit_failure", "local_sim_retry_diagnostics"),
        )

    def merge_run_tca_capture_sidecar(
        self,
        *,
        run_id: str,
        expected_plan_id: str,
        expected_plan_hash: str,
        parent_intent_id: str,
        decision_capture: dict[str, Any] | None = None,
        capture_error: dict[str, Any] | None = None,
        capture_batch_id: str | None = None,
    ) -> CaptureMergeOutcome:
        """CAS-merge one TCA parent observation without touching run state.

        The generic update path intentionally cannot replace this namespace.
        This is the sole PostgreSQL writer that obtains a row lock and applies
        the first-write/hash comparison contract for run-side evidence.
        """

        if sum(value is not None for value in (decision_capture, capture_error, capture_batch_id)) != 1:
            raise ValueError("exactly one run TCA capture mutation is required")
        with self._conn_factory() as conn:
            original_autocommit = bool(getattr(conn, "autocommit", False))
            try:
                if original_autocommit:
                    conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT execution_plan_id, execution_plan_hash, run_payload_json
                        FROM paper_v2.simulation_daily_run
                        WHERE run_id = %s
                        FOR UPDATE
                        """,
                        (run_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        conn.rollback()
                        return CaptureMergeOutcome.NOT_FOUND
                    if (
                        row.get("execution_plan_id") != expected_plan_id
                        or row.get("execution_plan_hash") != expected_plan_hash
                    ):
                        conn.rollback()
                        return CaptureMergeOutcome.IDENTITY_DRIFT
                    payload = dict(row.get("run_payload_json") or {})
                    existing = payload.get(TCA_OBSERVATION_KEY)
                    if existing is None:
                        sidecar = new_run_tca_sidecar(
                            execution_plan_id=expected_plan_id,
                            execution_plan_hash=expected_plan_hash,
                        )
                    elif not isinstance(existing, dict):
                        conn.rollback()
                        return CaptureMergeOutcome.IDENTITY_DRIFT
                    else:
                        sidecar = dict(existing)
                        if (
                            sidecar.get("execution_plan_id") != expected_plan_id
                            or sidecar.get("execution_plan_hash") != expected_plan_hash
                        ):
                            conn.rollback()
                            return CaptureMergeOutcome.IDENTITY_DRIFT
                    if decision_capture is not None:
                        outcome = merge_parent_first_write(
                            sidecar,
                            section="decision_capture_by_parent",
                            parent_intent_id=parent_intent_id,
                            value=decision_capture,
                        )
                    elif capture_error is not None:
                        outcome = merge_parent_first_write(
                            sidecar,
                            section="capture_errors",
                            parent_intent_id=parent_intent_id,
                            value=capture_error,
                        )
                    else:
                        outcome = merge_parent_first_write(
                            sidecar,
                            section="capture_batch_id_by_parent",
                            parent_intent_id=parent_intent_id,
                            value=str(capture_batch_id),
                        )
                    if outcome == CaptureMergeOutcome.CONFLICT:
                        conn.rollback()
                        return outcome
                    payload[TCA_OBSERVATION_KEY] = sidecar
                    cur.execute(
                        """
                        UPDATE paper_v2.simulation_daily_run
                        SET run_payload_json = %s, updated_at = now()
                        WHERE run_id = %s
                        """,
                        (psycopg2.extras.Json(payload), run_id),
                    )
                conn.commit()
                return outcome
            except Exception:
                conn.rollback()
                raise
            finally:
                if original_autocommit:
                    conn.autocommit = True

    def _fetch_rows(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    @staticmethod
    def _daily_run_with_binding_slots(
        run: SimulationDailyRun,
        binding: SimulationReleaseBinding,
    ) -> SimulationDailyRun:
        updates: dict[str, Any] = {}
        if run.account_group_id is None and binding.account_group_id is not None:
            updates["account_group_id"] = binding.account_group_id
        if run.strategy_slot_id is None and binding.strategy_slot_id is not None:
            updates["strategy_slot_id"] = binding.strategy_slot_id
        if not updates:
            return run
        payload = {
            **run.run_payload_json,
            "account_group_id": updates.get("account_group_id", run.account_group_id),
            "strategy_slot_id": updates.get("strategy_slot_id", run.strategy_slot_id),
        }
        return run.model_copy(update={**updates, "run_payload_json": payload})

    @staticmethod
    def _release_from_row(row: dict[str, Any]) -> StrategyRuntimeRelease:
        return StrategyRuntimeRelease(
            release_id=row["release_id"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            base_release_id=row.get("base_release_id"),
            runtime_profile_id=row["runtime_profile_id"],
            runtime_profile_version_id=row["runtime_profile_version_id"],
            runtime_profile_sha256=row["runtime_profile_sha256"],
            daily_strategy_profile_version_id=row["daily_strategy_profile_version_id"],
            execution_policy_version_id=row["execution_policy_version_id"],
            execution_policy_sha256=row["execution_policy_sha256"],
            tail_policy_version_id=row["tail_policy_version_id"],
            tail_policy_sha256=row["tail_policy_sha256"],
            release_config_json=row.get("release_config_json") or {},
            release_hash=row["release_hash"],
            validation_state=RuntimeReleaseValidationState(row["validation_state"]),
            validation_evidence=row.get("validation_evidence") or {},
            effective_from=row.get("effective_from"),
            effective_to=row.get("effective_to"),
            created_by=row.get("created_by"),
            created_reason=row.get("created_reason"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _evidence_from_row(row: dict[str, Any]) -> DailySelectionEvidence:
        return DailySelectionEvidence(
            evidence_id=row["evidence_id"],
            target_trade_date=row["target_trade_date"],
            cutoff_date=row.get("cutoff_date"),
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            release_id=row.get("release_id"),
            release_hash=row.get("release_hash"),
            runtime_profile_version_id=row["runtime_profile_version_id"],
            runtime_profile_hash=row["runtime_profile_hash"],
            source_type=row["source_type"],
            data_source=row["data_source"],
            candidate_count=int(row["candidate_count"]),
            excluded_count=int(row["excluded_count"]),
            artifact_hash=row["artifact_hash"],
            evidence_payload_json=row.get("evidence_payload_json") or {},
            created_at=row["created_at"],
            created_by=row.get("created_by"),
        )

    @staticmethod
    def _binding_from_row(row: dict[str, Any]) -> SimulationReleaseBinding:
        return SimulationReleaseBinding(
            binding_id=row["binding_id"],
            strategy_id=row["strategy_id"],
            release_id=row["release_id"],
            release_hash=row["release_hash"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            broker_backend=SimulationBrokerBackend(row["broker_backend"]),
            broker_account_id=row.get("broker_account_id"),
            account_group_id=row.get("account_group_id"),
            strategy_slot_id=row.get("strategy_slot_id"),
            capital_allocation=float(row["capital_allocation"]),
            strategy_name=row.get("strategy_name"),
            order_remark_prefix=row.get("order_remark_prefix"),
            effective_from=row.get("effective_from"),
            effective_to=row.get("effective_to"),
            approval_state=SimulationBindingApprovalState(row["approval_state"]),
            binding_config_json=row.get("binding_config_json") or {},
            binding_hash=row["binding_hash"],
            created_by=row.get("created_by"),
            created_reason=row.get("created_reason"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _execution_plan_from_row(row: dict[str, Any]) -> ExecutionPlan:
        payload = row.get("plan_payload_json") or {}
        intent_payloads = payload.get("intents") if isinstance(payload.get("intents"), list) else []
        decisions_payloads = (
            payload.get("trading_rule_decisions") if isinstance(payload.get("trading_rule_decisions"), list) else []
        )
        intents = [
            ExecutionPlanIntent(
                intent_id=item["intent_id"],
                plan_id=row["plan_id"],
                strategy_id=row["strategy_id"],
                portfolio_id=row["portfolio_id"],
                package_id=row["package_id"],
                release_id=row["release_id"],
                release_hash=row["release_hash"],
                binding_id=row["binding_id"],
                binding_hash=row["binding_hash"],
                symbol=item["symbol"],
                side=item["side"],
                target_quantity=int(item.get("target_quantity") or 0),
                delta_quantity=int(item.get("delta_quantity") or 0),
                order_quantity=int(item.get("order_quantity") or item.get("quantity") or 0),
                target_weight=item.get("target_weight"),
                current_quantity=int(item.get("current_quantity") or 0),
                current_available_quantity=item.get("current_available_quantity"),
                rebalance_reason=str(item.get("rebalance_reason") or ""),
                trading_rule_decision_id=str(item.get("trading_rule_decision_id") or ""),
                schedule_window=item.get("schedule_window") if isinstance(item.get("schedule_window"), dict) else {},
                price_policy=item.get("price_policy") if isinstance(item.get("price_policy"), dict) else {},
                risk_context=item.get("risk_context") if isinstance(item.get("risk_context"), dict) else {},
                metadata=item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
            )
            for item in intent_payloads
        ]
        decisions = [
            TradingRuleDecision(
                decision_id=item["decision_id"],
                symbol=item["symbol"],
                market_board=item["market_board"],
                side=item["side"],
                requested_quantity=int(item.get("requested_quantity") or 0),
                legal_quantity=int(item.get("legal_quantity") or 0),
                lot_rule=item.get("lot_rule") if isinstance(item.get("lot_rule"), dict) else {},
                price_limit_rule=item.get("price_limit_rule") if isinstance(item.get("price_limit_rule"), dict) else {},
                tplus1_available_quantity=item.get("tplus1_available_quantity"),
                decision=item["decision"],
                reason_code=item["reason_code"],
                source_version=item["source_version"],
                decision_hash=item["decision_hash"],
            )
            for item in decisions_payloads
        ]
        return ExecutionPlan(
            plan_id=row["plan_id"],
            strategy_id=row["strategy_id"],
            portfolio_id=row["portfolio_id"],
            package_id=row["package_id"],
            release_id=row["release_id"],
            release_hash=row["release_hash"],
            binding_id=row["binding_id"],
            binding_hash=row["binding_hash"],
            selection_evidence_id=row["selection_evidence_id"],
            selection_evidence_hash=row["selection_evidence_hash"],
            target_trade_date=row["target_trade_date"],
            execution_policy_version_id=row["execution_policy_version_id"],
            execution_policy_sha256=row["execution_policy_sha256"],
            tail_policy_version_id=row["tail_policy_version_id"],
            tail_policy_sha256=row["tail_policy_sha256"],
            intents=intents,
            trading_rule_decisions=decisions,
            plan_payload_json=payload,
            plan_hash=row["plan_hash"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _daily_run_from_row(row: dict[str, Any]) -> SimulationDailyRun:
        return SimulationDailyRun(
            run_id=row["run_id"],
            trade_date=row["trade_date"],
            strategy_id=row["strategy_id"],
            broker_backend=SimulationBrokerBackend(row["broker_backend"]),
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            release_id=row["release_id"],
            release_hash=row["release_hash"],
            binding_id=row["binding_id"],
            binding_hash=row["binding_hash"],
            account_group_id=row.get("account_group_id"),
            strategy_slot_id=row.get("strategy_slot_id"),
            selection_evidence_id=row.get("selection_evidence_id"),
            selection_artifact_hash=row.get("selection_artifact_hash"),
            execution_plan_id=row.get("execution_plan_id"),
            execution_plan_hash=row.get("execution_plan_hash"),
            status=SimulationDailyRunStatus(row["status"]),
            run_payload_json=row.get("run_payload_json") or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class InMemorySimulationRuntimeRepository:
    def __init__(self) -> None:
        self.releases: dict[str, StrategyRuntimeRelease] = {}
        self.release_hash_index: dict[str, str] = {}
        self.bindings: dict[str, SimulationReleaseBinding] = {}
        self.binding_hash_index: dict[str, str] = {}
        self.daily_selection_evidences: dict[str, DailySelectionEvidence] = {}
        self.daily_selection_hash_index: dict[str, str] = {}
        self.execution_plans: dict[str, ExecutionPlan] = {}
        self.execution_plan_hash_index: dict[str, str] = {}
        self.daily_runs: dict[str, SimulationDailyRun] = {}
        self.daily_run_key_index: dict[tuple[str, str, Any], str] = {}

    @contextmanager
    def local_sim_economic_transaction_scope(self) -> Iterator[None]:
        snapshot = deepcopy(self.daily_runs)
        try:
            yield
        except Exception:
            self.daily_runs = snapshot
            raise

    def save_strategy_runtime_release(self, release: StrategyRuntimeRelease) -> StrategyRuntimeRelease:
        if release.release_hash in self.release_hash_index:
            return self.releases[self.release_hash_index[release.release_hash or ""]]
        if release.release_id in self.releases:
            existing = self.releases[release.release_id]
            if existing.release_hash != release.release_hash:
                raise InvalidStateTransitionError(
                    "strategy runtime release conflicts with an existing immutable release",
                    context={"release_id": release.release_id, "release_hash": release.release_hash},
                )
            return existing
        self.releases[release.release_id] = release
        self.release_hash_index[release.release_hash or ""] = release.release_id
        return release

    def get_strategy_runtime_release(self, release_id: str) -> StrategyRuntimeRelease:
        try:
            return self.releases[release_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "strategy runtime release does not exist", context={"release_id": release_id}
            ) from exc

    def get_strategy_runtime_release_by_hash(self, release_hash: str) -> StrategyRuntimeRelease | None:
        release_id = self.release_hash_index.get(release_hash)
        return self.releases[release_id] if release_id else None

    def save_simulation_release_binding(self, binding: SimulationReleaseBinding) -> SimulationReleaseBinding:
        self.get_strategy_runtime_release(binding.release_id)
        if binding.binding_hash in self.binding_hash_index:
            return self.bindings[self.binding_hash_index[binding.binding_hash or ""]]
        if binding.binding_id in self.bindings:
            existing = self.bindings[binding.binding_id]
            if existing.binding_hash != binding.binding_hash:
                raise InvalidStateTransitionError(
                    "simulation release binding conflicts with an existing immutable binding",
                    context={"binding_id": binding.binding_id, "binding_hash": binding.binding_hash},
                )
            return existing
        self.bindings[binding.binding_id] = binding
        self.binding_hash_index[binding.binding_hash or ""] = binding.binding_id
        return binding

    def migrate_miniqmt_binding_route(
        self,
        *,
        source_binding_id: str,
        expected_source_binding_hash: str,
        source_effective_to: date,
        target_binding: SimulationReleaseBinding,
    ) -> tuple[SimulationReleaseBinding, SimulationReleaseBinding]:
        bindings_snapshot = dict(self.bindings)
        binding_hash_index_snapshot = dict(self.binding_hash_index)
        try:
            source = self.get_simulation_release_binding(source_binding_id)
            if source.binding_hash != expected_source_binding_hash:
                raise InvalidStateTransitionError(
                    "MiniQMT route migration source binding hash changed",
                    context={
                        "reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_CAS_CONFLICT",
                        "source_binding_id": source_binding_id,
                    },
                )
            if source.effective_to not in {None, source_effective_to}:
                raise InvalidStateTransitionError(
                    "MiniQMT route migration source effective window conflicts with requested cutover",
                    context={
                        "reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_WINDOW_CONFLICT",
                        "source_binding_id": source_binding_id,
                        "expected_effective_to": source_effective_to.isoformat(),
                        "actual_effective_to": source.effective_to.isoformat() if source.effective_to else None,
                    },
                )
            marker = (
                target_binding.binding_config_json.get("metadata", {}).get("miniqmt_route_migration")
                if isinstance(target_binding.binding_config_json.get("metadata"), dict)
                else None
            )
            if not isinstance(marker, dict):
                raise InvalidStateTransitionError(
                    "MiniQMT route migration target binding is missing its durable marker",
                    context={"reason_code": "MINIQMT_ROUTE_MIGRATION_MARKER_MISSING"},
                )
            for existing in self.bindings.values():
                metadata = existing.binding_config_json.get("metadata")
                existing_marker = metadata.get("miniqmt_route_migration") if isinstance(metadata, dict) else None
                if not isinstance(existing_marker, dict):
                    continue
                if (
                    existing_marker.get("source_binding_id") == source_binding_id
                    and existing_marker.get("effective_trade_date") == marker.get("effective_trade_date")
                    and existing.binding_hash != target_binding.binding_hash
                ):
                    raise InvalidStateTransitionError(
                        "MiniQMT route migration already has a different target for this source/date",
                        context={
                            "reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_CONFLICT",
                            "source_binding_id": source_binding_id,
                            "conflicting_binding_id": existing.binding_id,
                            "conflicting_binding_hash": existing.binding_hash,
                        },
                    )
            persisted_target = self.save_simulation_release_binding(target_binding)
            if persisted_target.model_dump(mode="json") != target_binding.model_dump(mode="json"):
                raise InvalidStateTransitionError(
                    "MiniQMT route migration target hash readback differs from the requested binding",
                    context={"reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_HASH_CONFLICT"},
                )
            persisted_source = source.model_copy(
                update={"effective_to": source_effective_to, "updated_at": datetime.now(UTC)}
            )
            self.bindings[source_binding_id] = persisted_source
            if self.get_simulation_release_binding(source_binding_id).effective_to != source_effective_to:
                raise InvalidStateTransitionError(
                    "MiniQMT route migration source window transaction readback differs",
                    context={"reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_WINDOW_READBACK_MISMATCH"},
                )
            return persisted_source, persisted_target
        except Exception:
            self.bindings = bindings_snapshot
            self.binding_hash_index = binding_hash_index_snapshot
            raise

    def get_simulation_release_binding(self, binding_id: str) -> SimulationReleaseBinding:
        try:
            return self.bindings[binding_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "simulation release binding does not exist", context={"binding_id": binding_id}
            ) from exc

    def get_simulation_release_binding_by_hash(self, binding_hash: str) -> SimulationReleaseBinding | None:
        binding_id = self.binding_hash_index.get(binding_hash)
        return self.bindings[binding_id] if binding_id else None

    def find_miniqmt_route_migration_target(
        self,
        *,
        source_binding_id: str,
        effective_trade_date: date,
    ) -> SimulationReleaseBinding | None:
        matches: list[SimulationReleaseBinding] = []
        for binding in self.bindings.values():
            metadata = binding.binding_config_json.get("metadata")
            marker = metadata.get("miniqmt_route_migration") if isinstance(metadata, dict) else None
            if not isinstance(marker, dict):
                continue
            if (
                marker.get("source_binding_id") == source_binding_id
                and marker.get("effective_trade_date") == effective_trade_date.isoformat()
            ):
                matches.append(binding)
        if len(matches) > 1:
            raise InvalidStateTransitionError(
                "MiniQMT route migration has multiple durable targets for one source/date",
                context={
                    "reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_CONFLICT",
                    "source_binding_id": source_binding_id,
                    "effective_trade_date": effective_trade_date.isoformat(),
                    "target_binding_ids": sorted(item.binding_id for item in matches),
                },
            )
        return matches[0] if matches else None

    def list_simulation_release_bindings(
        self,
        *,
        strategy_id: str | None = None,
        release_id: str | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        approval_states: Iterable[SimulationBindingApprovalState | str] | None = None,
        active_on: date | None = None,
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        rows = list(self.bindings.values())
        if strategy_id is not None:
            rows = [row for row in rows if row.strategy_id == strategy_id]
        if release_id is not None:
            rows = [row for row in rows if row.release_id == release_id]
        if broker_backend is not None:
            backend = (
                broker_backend
                if isinstance(broker_backend, SimulationBrokerBackend)
                else SimulationBrokerBackend(str(broker_backend))
            )
            rows = [row for row in rows if row.broker_backend == backend]
        states = {
            state if isinstance(state, SimulationBindingApprovalState) else SimulationBindingApprovalState(str(state))
            for state in (approval_states or [])
        }
        if states:
            rows = [row for row in rows if row.approval_state in states]
        if active_on is not None:
            rows = [
                row
                for row in rows
                if (row.effective_from is None or row.effective_from <= active_on)
                and (row.effective_to is None or row.effective_to >= active_on)
            ]
        rows.sort(key=lambda item: (item.created_at, item.binding_id), reverse=True)
        return rows[:limit]

    def list_latest_simulation_release_bindings(
        self,
        *,
        strategy_id: str | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        approval_states: Iterable[SimulationBindingApprovalState | str] | None = None,
        effective_from_on_or_before: date | None = None,
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        rows = list(self.bindings.values())
        if strategy_id is not None:
            rows = [row for row in rows if row.strategy_id == strategy_id]
        if broker_backend is not None:
            backend = (
                broker_backend
                if isinstance(broker_backend, SimulationBrokerBackend)
                else SimulationBrokerBackend(str(broker_backend))
            )
            rows = [row for row in rows if row.broker_backend == backend]
        states = {
            state if isinstance(state, SimulationBindingApprovalState) else SimulationBindingApprovalState(str(state))
            for state in (approval_states or [])
        }
        if states:
            rows = [row for row in rows if row.approval_state in states]
        if effective_from_on_or_before is not None:
            rows = [
                row for row in rows if row.effective_from is None or row.effective_from <= effective_from_on_or_before
            ]
        latest: dict[tuple[str, SimulationBrokerBackend], SimulationReleaseBinding] = {}
        for row in rows:
            key = (row.strategy_id, row.broker_backend)
            current = latest.get(key)
            if current is None or (
                row.effective_from or date.min,
                row.created_at,
                row.binding_id,
            ) > (
                current.effective_from or date.min,
                current.created_at,
                current.binding_id,
            ):
                latest[key] = row
        ordered = sorted(
            latest.values(),
            key=lambda item: (item.created_at, item.binding_id),
            reverse=True,
        )
        return ordered[:limit]

    def save_daily_selection_evidence(self, evidence: DailySelectionEvidence) -> DailySelectionEvidence:
        if _is_daily_selection_evidence_v2(evidence):
            _validate_daily_selection_evidence_v2(evidence)
        if evidence.artifact_hash in self.daily_selection_hash_index:
            existing = self.daily_selection_evidences[self.daily_selection_hash_index[evidence.artifact_hash]]
            if _is_daily_selection_evidence_v2(evidence):
                SimulationRuntimeRepository._assert_same_v2_evidence(existing, evidence)
            return existing
        existing_by_id = self.daily_selection_evidences.get(evidence.evidence_id)
        if existing_by_id is not None:
            if _is_daily_selection_evidence_v2(evidence):
                SimulationRuntimeRepository._assert_same_v2_evidence(existing_by_id, evidence)
                return existing_by_id
            raise InvalidStateTransitionError(
                "daily selection evidence conflicts with an existing immutable evidence id",
                context={"evidence_id": evidence.evidence_id, "artifact_hash": evidence.artifact_hash},
            )
        self.daily_selection_evidences[evidence.evidence_id] = evidence
        self.daily_selection_hash_index[evidence.artifact_hash] = evidence.evidence_id
        return evidence

    def get_daily_selection_evidence(self, evidence_id: str) -> DailySelectionEvidence:
        try:
            return self.daily_selection_evidences[evidence_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "daily selection evidence does not exist", context={"evidence_id": evidence_id}
            ) from exc

    def get_daily_selection_evidence_by_hash(self, artifact_hash: str) -> DailySelectionEvidence | None:
        evidence_id = self.daily_selection_hash_index.get(artifact_hash)
        return self.daily_selection_evidences[evidence_id] if evidence_id else None

    def save_execution_plan(self, plan: ExecutionPlan) -> ExecutionPlan:
        self.get_strategy_runtime_release(plan.release_id)
        self.get_simulation_release_binding(plan.binding_id)
        self.get_daily_selection_evidence(plan.selection_evidence_id)
        if plan.plan_hash in self.execution_plan_hash_index:
            return self.execution_plans[self.execution_plan_hash_index[plan.plan_hash]]
        if plan.plan_id in self.execution_plans:
            existing = self.execution_plans[plan.plan_id]
            if existing.plan_hash != plan.plan_hash:
                raise InvalidStateTransitionError(
                    "execution plan conflicts with an existing immutable plan",
                    context={"plan_id": plan.plan_id, "plan_hash": plan.plan_hash},
                )
            return existing
        self.execution_plans[plan.plan_id] = plan
        self.execution_plan_hash_index[plan.plan_hash] = plan.plan_id
        return plan

    def get_execution_plan(self, plan_id: str) -> ExecutionPlan:
        try:
            return self.execution_plans[plan_id]
        except KeyError as exc:
            raise DataUnavailableError("execution plan does not exist", context={"plan_id": plan_id}) from exc

    def get_execution_plan_by_hash(self, plan_hash: str) -> ExecutionPlan | None:
        plan_id = self.execution_plan_hash_index.get(plan_hash)
        return self.execution_plans[plan_id] if plan_id else None

    def save_simulation_daily_run(self, run: SimulationDailyRun) -> SimulationDailyRun:
        existing = self.get_simulation_daily_run_by_key(
            strategy_id=run.strategy_id,
            binding_id=run.binding_id,
            trade_date=run.trade_date,
        )
        if existing is not None:
            return existing
        self.get_strategy_runtime_release(run.release_id)
        binding = self.get_simulation_release_binding(run.binding_id)
        run = SimulationRuntimeRepository._daily_run_with_binding_slots(run, binding)
        if run.run_id in self.daily_runs:
            existing_by_id = self.daily_runs[run.run_id]
            if (
                existing_by_id.strategy_id,
                existing_by_id.binding_id,
                existing_by_id.trade_date,
            ) != (run.strategy_id, run.binding_id, run.trade_date):
                raise InvalidStateTransitionError(
                    "simulation daily run conflicts with an existing run",
                    context={"run_id": run.run_id, "strategy_id": run.strategy_id, "binding_id": run.binding_id},
                )
            return existing_by_id
        self.daily_runs[run.run_id] = run
        self.daily_run_key_index[(run.strategy_id, run.binding_id, run.trade_date)] = run.run_id
        return run

    def get_simulation_daily_run(self, run_id: str) -> SimulationDailyRun:
        try:
            return self.daily_runs[run_id]
        except KeyError as exc:
            raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id}) from exc

    def get_simulation_daily_run_by_key(
        self,
        *,
        strategy_id: str,
        binding_id: str,
        trade_date: Any,
    ) -> SimulationDailyRun | None:
        run_id = self.daily_run_key_index.get((strategy_id, binding_id, trade_date))
        return self.daily_runs[run_id] if run_id else None

    def list_simulation_daily_runs(
        self,
        *,
        trade_date: Any | None = None,
        trade_date_before: Any | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        status: SimulationDailyRunStatus | str | None = None,
        limit: int = 100,
    ) -> list[SimulationDailyRun]:
        rows = list(self.daily_runs.values())
        if trade_date is not None:
            rows = [row for row in rows if row.trade_date == trade_date]
        if trade_date_before is not None:
            rows = [row for row in rows if row.trade_date < trade_date_before]
        if broker_backend is not None:
            backend = (
                broker_backend
                if isinstance(broker_backend, SimulationBrokerBackend)
                else SimulationBrokerBackend(str(broker_backend))
            )
            rows = [row for row in rows if row.broker_backend == backend]
        if strategy_id is not None:
            rows = [row for row in rows if row.strategy_id == strategy_id]
        if status is not None:
            expected = status if isinstance(status, SimulationDailyRunStatus) else SimulationDailyRunStatus(str(status))
            rows = [row for row in rows if row.status == expected]
        rows.sort(key=lambda item: (item.trade_date, item.updated_at, item.created_at, item.run_id), reverse=True)
        return rows[:limit]

    def update_simulation_daily_run(
        self,
        run_id: str,
        *,
        status: SimulationDailyRunStatus | None = None,
        selection_evidence: DailySelectionEvidence | None = None,
        execution_plan: ExecutionPlan | None = None,
        payload_patch: dict[str, Any] | None = None,
        payload_unset: Iterable[str] | None = None,
    ) -> SimulationDailyRun:
        current = self.get_simulation_daily_run(run_id)
        merged_payload = {**current.run_payload_json, **(payload_patch or {})}
        merged_payload = preserve_tca_sidecar(current.run_payload_json, merged_payload)
        for key in payload_unset or ():
            merged_payload.pop(str(key), None)
        updated = current.model_copy(
            update={
                "status": status or current.status,
                "selection_evidence_id": selection_evidence.evidence_id
                if selection_evidence
                else current.selection_evidence_id,
                "selection_artifact_hash": selection_evidence.artifact_hash
                if selection_evidence
                else current.selection_artifact_hash,
                "execution_plan_id": execution_plan.plan_id if execution_plan else current.execution_plan_id,
                "execution_plan_hash": execution_plan.plan_hash if execution_plan else current.execution_plan_hash,
                "run_payload_json": merged_payload,
            }
        )
        self.daily_runs[run_id] = updated
        return updated

    def claim_simulation_retry_attempt(
        self,
        *,
        run_id: str,
        retry_key: str,
        source_fingerprint: str,
        as_of_time: datetime,
        lease_seconds: int,
    ) -> SimulationRetryAttemptDecision:
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        current = self.get_simulation_daily_run(run_id)
        next_payload, should_execute, reason, retry_entry, claim_token = _claim_retry_attempt_payload(
            payload=current.run_payload_json,
            retry_key=retry_key,
            source_fingerprint=source_fingerprint,
            as_of_time=as_of_time,
            lease_seconds=lease_seconds,
        )
        if next_payload != current.run_payload_json:
            current = current.model_copy(update={"run_payload_json": next_payload, "updated_at": datetime.now(UTC)})
            self.daily_runs[run_id] = current
        _simulation_retry_state(current.run_payload_json)
        return SimulationRetryAttemptDecision(
            run=current,
            should_execute=should_execute,
            reason=reason,
            retry_entry=deepcopy(retry_entry),
            claim_token=claim_token,
        )

    def record_simulation_retry_failure(
        self,
        *,
        run_id: str,
        retry_key: str,
        source_fingerprint: str,
        failure_fingerprint: str,
        failure_stage: str,
        error: dict[str, Any],
        as_of_time: datetime,
        base_delay_seconds: int,
        max_delay_seconds: int,
        expected_claim_token: str | None = None,
    ) -> SimulationDailyRun:
        if (
            type(base_delay_seconds) is not int
            or type(max_delay_seconds) is not int
            or base_delay_seconds <= 0
            or max_delay_seconds < base_delay_seconds
        ):
            raise ValueError("retry delay bounds are invalid")
        current = self.get_simulation_daily_run(run_id)
        next_payload, _ = _record_retry_failure_payload(
            payload=current.run_payload_json,
            retry_key=retry_key,
            source_fingerprint=source_fingerprint,
            failure_fingerprint=failure_fingerprint,
            failure_stage=failure_stage,
            error=error,
            as_of_time=as_of_time,
            base_delay_seconds=base_delay_seconds,
            max_delay_seconds=max_delay_seconds,
            expected_claim_token=expected_claim_token,
        )
        updated = current.model_copy(update={"run_payload_json": next_payload, "updated_at": datetime.now(UTC)})
        self.daily_runs[run_id] = updated
        _simulation_retry_state(updated.run_payload_json)
        return updated

    def clear_simulation_retry_control(
        self,
        *,
        run_id: str,
        retry_key: str,
        expected_claim_token: str | None = None,
    ) -> SimulationDailyRun:
        retry_key = _retry_required_text(retry_key, field="retry_key")
        current = self.get_simulation_daily_run(run_id)
        control, claims_control = _simulation_retry_state(current.run_payload_json)
        entries = dict(control["entries"]) if control is not None else {}
        claims = dict(claims_control["claims"]) if claims_control is not None else {}
        active_token = simulation_retry_claim_token(current.run_payload_json, retry_key=retry_key)
        if expected_claim_token is not None:
            expected_claim_token = _retry_sha256(expected_claim_token, field="expected_claim_token")
            if active_token != expected_claim_token:
                raise InvalidStateTransitionError(
                    "simulation scheduler retry success writer no longer owns the attempt claim",
                    context={
                        "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_STALE_WRITER",
                        "retry_key": retry_key,
                        "expected_claim_token": expected_claim_token,
                        "actual_claim_token": active_token,
                    },
                )
        elif active_token is not None:
            raise InvalidStateTransitionError(
                "simulation scheduler retry success writer omitted the active attempt claim token",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_RETRY_CLAIM_TOKEN_REQUIRED",
                    "retry_key": retry_key,
                    "active_claim_token": active_token,
                },
            )
        if retry_key not in entries and retry_key not in claims:
            return current
        entries.pop(retry_key, None)
        claims.pop(retry_key, None)
        next_payload = _retry_control_payload_with_entries(current.run_payload_json, entries)
        next_payload = _retry_claims_payload_with_claims(next_payload, claims)
        updated = current.model_copy(update={"run_payload_json": next_payload, "updated_at": datetime.now(UTC)})
        self.daily_runs[run_id] = updated
        return updated

    def list_local_sim_execution_states(
        self, run_id: str, *, authoritative: bool = False
    ) -> list[LocalSimExecutionStateV1]:
        run = self.get_simulation_daily_run(run_id)
        state_map = _local_sim_state_map(run.run_payload_json)
        if authoritative:
            state_map = _local_sim_state_authority_closure(
                run_id=run.run_id,
                binding_id=run.binding_id,
                trade_date=run.trade_date,
                plan_id=run.execution_plan_id,
                payload=run.run_payload_json,
                states=state_map,
            ).states
        states = list(state_map.values())
        states.sort(key=lambda item: (item.intent_id, item.algo_instance_id, item.state_id))
        return states

    def commit_local_sim_execution_states(
        self,
        *,
        run_id: str,
        states: Iterable[LocalSimExecutionStateV1],
        expected_versions: dict[str, tuple[int, str] | None],
    ) -> list[LocalSimExecutionStateV1]:
        incoming = list(states)
        current = self.get_simulation_daily_run(run_id)
        merged = _merge_local_sim_state_batch(
            run_id=run_id,
            payload=current.run_payload_json,
            states=incoming,
            expected_versions=expected_versions,
        )
        self.daily_runs[run_id] = current.model_copy(update={"run_payload_json": merged})
        readback = {state.state_id: state for state in self.list_local_sim_execution_states(run_id)}
        for state in incoming:
            persisted = readback.get(state.state_id)
            if persisted is None or persisted.state_hash != state.state_hash:
                raise InvalidStateTransitionError(
                    "LocalSIM durable state independent readback failed",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_READBACK_FAILED",
                        "run_id": run_id,
                        "state_id": state.state_id,
                        "expected_state_hash": state.state_hash,
                        "actual_state_hash": persisted.state_hash if persisted else None,
                    },
                )
        return [readback[state.state_id] for state in incoming]

    def stage_local_sim_economic_commit(
        self,
        *,
        connection: Any,
        run_id: str,
        binding_id: str,
        trade_date: date,
        plan_id: str,
        states: Iterable[LocalSimExecutionStateV1],
        expected_versions: dict[str, tuple[int, str] | None],
        economic_facts: dict[str, Any],
        projection_payload: dict[str, Any],
        status: SimulationDailyRunStatus,
        payload_patch: dict[str, Any],
        payload_unset: Iterable[str] = (),
    ) -> tuple[LocalSimEconomicReceiptV1, LocalSimProjectionOutboxV1, bool]:
        current = self.get_simulation_daily_run(run_id)
        merged, receipt, outbox, created = _merge_local_sim_economic_event(
            run_id=run_id,
            binding_id=binding_id,
            trade_date=trade_date,
            plan_id=plan_id,
            payload=current.run_payload_json,
            states=states,
            expected_versions=expected_versions,
            economic_facts=economic_facts,
            projection_payload=projection_payload,
        )
        if created:
            merged.update(payload_patch)
            for key in payload_unset:
                merged.pop(str(key), None)
            merged = preserve_tca_sidecar(current.run_payload_json, merged)
        self.daily_runs[run_id] = current.model_copy(
            update={
                "status": status if created else current.status,
                "run_payload_json": merged,
                "updated_at": datetime.now(UTC),
            }
        )
        return receipt, outbox, created

    def readback_local_sim_economic_commit(
        self, *, run_id: str, receipt: LocalSimEconomicReceiptV1, outbox: LocalSimProjectionOutboxV1
    ) -> SimulationDailyRun:
        run = self.get_simulation_daily_run(run_id)
        _validate_local_sim_economic_readback(run=run, receipt=receipt, outbox=outbox)
        return run

    def stage_local_sim_projection_commit(
        self,
        *,
        connection: Any,
        run_id: str,
        outbox_id: str,
        generation: int,
        final_status: SimulationDailyRunStatus,
        projection_result: dict[str, Any],
        payload_patch: dict[str, Any],
        payload_unset: Iterable[str] = (),
    ) -> LocalSimProjectionReceiptV1:
        current = self.get_simulation_daily_run(run_id)
        merged, receipt = _merge_local_sim_projection_success(
            run_id=run_id,
            payload=current.run_payload_json,
            outbox_id=outbox_id,
            generation=generation,
            projection_result=projection_result,
        )
        merged.update(payload_patch)
        merged.setdefault("local_sim_projection_generation", {})["projection_receipt_id"] = (
            receipt.projection_receipt_id
        )
        for key in payload_unset:
            merged.pop(str(key), None)
        merged = preserve_tca_sidecar(current.run_payload_json, merged)
        self.daily_runs[run_id] = current.model_copy(
            update={"status": final_status, "run_payload_json": merged, "updated_at": datetime.now(UTC)}
        )
        return receipt

    def mark_local_sim_projection_retryable(
        self, *, run_id: str, outbox_id: str, error: dict[str, Any]
    ) -> SimulationDailyRun:
        current = self.get_simulation_daily_run(run_id)
        merged = _merge_local_sim_projection_retryable(
            run_id=run_id, payload=current.run_payload_json, outbox_id=outbox_id, error=error
        )
        merged["last_stage"] = SimulationDailyRunStatus.FAILED_RETRYABLE.value
        updated = current.model_copy(
            update={
                "status": SimulationDailyRunStatus.FAILED_RETRYABLE,
                "run_payload_json": merged,
                "updated_at": datetime.now(UTC),
            }
        )
        self.daily_runs[run_id] = updated
        return updated

    def mark_local_sim_projection_terminal(
        self, *, run_id: str, outbox_id: str, error: dict[str, Any]
    ) -> SimulationDailyRun:
        current = self.get_simulation_daily_run(run_id)
        merged = _merge_local_sim_projection_terminal(
            run_id=run_id,
            payload=current.run_payload_json,
            outbox_id=outbox_id,
            error=error,
        )
        merged["last_stage"] = SimulationDailyRunStatus.FAILED_TERMINAL.value
        updated = current.model_copy(
            update={
                "status": SimulationDailyRunStatus.FAILED_TERMINAL,
                "run_payload_json": merged,
                "updated_at": datetime.now(UTC),
            }
        )
        self.daily_runs[run_id] = updated
        return updated

    def readback_local_sim_projection_commit(
        self, *, run_id: str, receipt: LocalSimProjectionReceiptV1
    ) -> SimulationDailyRun:
        run = self.get_simulation_daily_run(run_id)
        outbox = _local_sim_projection_outbox(run.run_payload_json)
        persisted = _local_sim_projection_receipt_map(run.run_payload_json).get(receipt.projection_receipt_id)
        if (
            outbox is None
            or outbox.outbox_id != receipt.outbox_id
            or outbox.status != LocalSimProjectionOutboxStatus.PROJECTED
        ):
            raise InvalidStateTransitionError(
                "LocalSIM projection outbox status independent readback failed",
                context={"reason_code": "LOCALSIM_PROJECTION_STATUS_READBACK_FAILED", "run_id": run_id},
            )
        if persisted is None or persisted.receipt_hash != receipt.receipt_hash:
            raise InvalidStateTransitionError(
                "LocalSIM projection receipt independent readback failed",
                context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_READBACK_FAILED", "run_id": run_id},
            )
        return run

    def mark_local_sim_projection_readback_retryable(
        self, *, run_id: str, outbox_id: str, error: dict[str, Any]
    ) -> SimulationDailyRun:
        current = self.get_simulation_daily_run(run_id)
        outbox = _local_sim_projection_outbox(current.run_payload_json)
        if outbox is None or outbox.outbox_id != outbox_id or outbox.status != LocalSimProjectionOutboxStatus.PROJECTED:
            raise InvalidStateTransitionError(
                "LocalSIM projection readback failure targets a non-projected outbox",
                context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_CAS_CONFLICT", "run_id": run_id},
            )
        return self.update_simulation_daily_run(
            run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "local_sim_projection_readback_failure": error,
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            },
        )

    def clear_local_sim_projection_readback_failure(
        self, *, run_id: str, outbox_id: str, final_status: SimulationDailyRunStatus
    ) -> SimulationDailyRun:
        current = self.get_simulation_daily_run(run_id)
        outbox = _local_sim_projection_outbox(current.run_payload_json)
        if outbox is None or outbox.outbox_id != outbox_id or outbox.status != LocalSimProjectionOutboxStatus.PROJECTED:
            raise InvalidStateTransitionError(
                "LocalSIM projection readback recovery targets a non-projected outbox",
                context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_CAS_CONFLICT", "run_id": run_id},
            )
        return self.update_simulation_daily_run(
            run_id,
            status=final_status,
            payload_patch={"last_stage": final_status.value},
            payload_unset=("local_sim_projection_readback_failure", "submit_failure", "local_sim_retry_diagnostics"),
        )

    def merge_run_tca_capture_sidecar(
        self,
        *,
        run_id: str,
        expected_plan_id: str,
        expected_plan_hash: str,
        parent_intent_id: str,
        decision_capture: dict[str, Any] | None = None,
        capture_error: dict[str, Any] | None = None,
        capture_batch_id: str | None = None,
    ) -> CaptureMergeOutcome:
        if sum(value is not None for value in (decision_capture, capture_error, capture_batch_id)) != 1:
            raise ValueError("exactly one run TCA capture mutation is required")
        current = self.daily_runs.get(run_id)
        if current is None:
            return CaptureMergeOutcome.NOT_FOUND
        if current.execution_plan_id != expected_plan_id or current.execution_plan_hash != expected_plan_hash:
            return CaptureMergeOutcome.IDENTITY_DRIFT
        payload = dict(current.run_payload_json or {})
        existing = payload.get(TCA_OBSERVATION_KEY)
        if existing is None:
            sidecar = new_run_tca_sidecar(
                execution_plan_id=expected_plan_id,
                execution_plan_hash=expected_plan_hash,
            )
        elif not isinstance(existing, dict):
            return CaptureMergeOutcome.IDENTITY_DRIFT
        else:
            sidecar = dict(existing)
            if (
                sidecar.get("execution_plan_id") != expected_plan_id
                or sidecar.get("execution_plan_hash") != expected_plan_hash
            ):
                return CaptureMergeOutcome.IDENTITY_DRIFT
        if decision_capture is not None:
            outcome = merge_parent_first_write(
                sidecar,
                section="decision_capture_by_parent",
                parent_intent_id=parent_intent_id,
                value=decision_capture,
            )
        elif capture_error is not None:
            outcome = merge_parent_first_write(
                sidecar,
                section="capture_errors",
                parent_intent_id=parent_intent_id,
                value=capture_error,
            )
        else:
            outcome = merge_parent_first_write(
                sidecar,
                section="capture_batch_id_by_parent",
                parent_intent_id=parent_intent_id,
                value=str(capture_batch_id),
            )
        if outcome == CaptureMergeOutcome.CONFLICT:
            return outcome
        payload[TCA_OBSERVATION_KEY] = sidecar
        self.daily_runs[run_id] = current.model_copy(update={"run_payload_json": payload})
        return outcome
