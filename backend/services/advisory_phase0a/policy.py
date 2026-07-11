"""Pure canonicalization and OOS policy functions for Advisory Phase 0A."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, time, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
from typing import Any, Iterable

from pydantic import BaseModel

from .models import (
    AvailabilityStatus,
    FormalOOSStatus,
    OOSClassification,
    OOSClassificationInput,
    OOSInterval,
    Phase0APolicyRegistry,
)


SERIALIZER_VERSION = "advisory_phase0a_canonical_v1"
REASON_AUDIT_NOT_READ_ONLY = "ADVISORY_PHASE0A_AUDIT_NOT_READ_ONLY"
REASON_CUTOFF_MISSING = "ADVISORY_PHASE0A_EFFECTIVE_CUTOFF_MISSING"
REASON_CUTOFF_AFTER_DECISION = "ADVISORY_PHASE0A_EFFECTIVE_CUTOFF_AFTER_DECISION"
REASON_FORMAL_WINDOW_EMPTY = "ADVISORY_PHASE0A_FORMAL_OOS_WINDOW_EMPTY"
REASON_RETROSPECTIVE_ONLY = "ADVISORY_PHASE0A_RETROSPECTIVE_ONLY"
REASON_BINDING_HISTORICAL_MISSING = "ADVISORY_PHASE0A_HISTORICAL_BINDING_MISSING"
REASON_RUNTIME_HISTORICAL_MISSING = "ADVISORY_PHASE0A_RUNTIME_HISTORICAL_VINTAGE_MISSING"
REASON_HMM_HISTORICAL_MISSING = "ADVISORY_PHASE0A_HMM_HISTORICAL_VINTAGE_MISSING"
REASON_SOURCE_PIT_MISSING = "ADVISORY_PHASE0A_SOURCE_PIT_MISSING"
REASON_CANDIDATE_AUTHORITY_MISSING = "ADVISORY_PHASE0A_CANDIDATE_AUTHORITY_MISSING"
REASON_NO_CANDIDATE_AUTHORITY_MISSING = "ADVISORY_PHASE0A_NO_CANDIDATE_AUTHORITY_MISSING"
REASON_BACKTEST_VINTAGE_FORBIDDEN = "ADVISORY_PHASE0A_BACKTEST_VINTAGE_FORBIDDEN"
REASON_POLICY_BENCHMARK_MISSING = "ADVISORY_PHASE0A_BENCHMARK_POLICY_MISSING"
REASON_POLICY_COST_MISSING = "ADVISORY_PHASE0A_COST_POLICY_MISSING"
REASON_POLICY_LABEL_MISSING = "ADVISORY_PHASE0A_LABEL_POLICY_MISSING"
REASON_POLICY_BENCHMARK_INCOMPLETE = "ADVISORY_PHASE0A_BENCHMARK_POLICY_INCOMPLETE"
REASON_POLICY_COST_INCOMPLETE = "ADVISORY_PHASE0A_COST_POLICY_INCOMPLETE"
REASON_POLICY_LABEL_INCOMPLETE = "ADVISORY_PHASE0A_LABEL_POLICY_INCOMPLETE"
REASON_POLICY_UNIVERSE_INCOMPLETE = "ADVISORY_PHASE0A_UNIVERSE_POLICY_INCOMPLETE"
REASON_POLICY_PRIOR_INCOMPLETE = "ADVISORY_PHASE0A_PRIOR_REGISTRY_INCOMPLETE"
REASON_POLICY_MULTIPLE_TESTING_INCOMPLETE = "ADVISORY_PHASE0A_MULTIPLE_TESTING_REGISTRY_INCOMPLETE"
REASON_EMBARGO_POLICY_MISSING = "ADVISORY_PHASE0A_EMBARGO_POLICY_MISSING"
REASON_EMBARGO_CALENDAR_MISSING = "ADVISORY_PHASE0A_EMBARGO_CALENDAR_MISSING"
REASON_EMBARGO_WINDOW_INSUFFICIENT = "ADVISORY_PHASE0A_EMBARGO_WINDOW_INSUFFICIENT"


class CanonicalJSONError(ValueError):
    """Raised when a payload cannot be represented by the Phase 0A serializer."""


def default_policy_registry(*, policy_version: str) -> Phase0APolicyRegistry:
    """Return the frozen policy shape without declaring missing policies as present."""

    return Phase0APolicyRegistry(policy_version=policy_version, serializer_version=SERIALIZER_VERSION)


def _decimal_scale_for_key(key: str | None) -> int:
    normalized = (key or "").lower()
    if "price" in normalized or normalized.endswith("_px"):
        return 6
    if any(token in normalized for token in ("score", "return", "weight", "ratio", "multiplier", "rate")):
        return 12
    return 12


def _format_decimal(value: Decimal, *, key: str | None) -> str:
    if not value.is_finite():
        raise CanonicalJSONError("NaN and infinity are not allowed in canonical payloads")
    quantized = value.quantize(Decimal(1).scaleb(-_decimal_scale_for_key(key)), rounding=ROUND_HALF_EVEN)
    if quantized == 0:
        quantized = Decimal(0)
    return format(quantized, "f")


def canonicalize(value: Any, *, key: str | None = None) -> Any:
    """Normalize an object into a JSON-safe, deterministic Phase 0A payload."""

    if isinstance(value, BaseModel):
        return canonicalize(value.model_dump(mode="python", exclude_none=False), key=key)
    if isinstance(value, Enum):
        return canonicalize(value.value, key=key)
    if isinstance(value, Decimal):
        return _format_decimal(value, key=key)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalJSONError("NaN and infinity are not allowed in canonical payloads")
        return _format_decimal(Decimal(str(value)), key=key)
    if isinstance(value, datetime):
        normalized = value.astimezone(timezone.utc) if value.tzinfo is not None else value
        return normalized.isoformat(timespec="microseconds")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            str(item_key): canonicalize(item_value, key=str(item_key))
            for item_key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [canonicalize(item, key=key) for item in value]
    if isinstance(value, set):
        normalized = [canonicalize(item, key=key) for item in value]
        return sorted(normalized, key=lambda item: canonical_json_text(item))
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise CanonicalJSONError(f"unsupported canonical payload type: {type(value).__name__}")


def canonical_json_text(payload: Any) -> str:
    return json.dumps(canonicalize(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def canonical_json_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json_text(payload).encode("utf-8")).hexdigest()


def stable_identifier(prefix: str, payload: Any) -> str:
    return f"{prefix}_{canonical_json_sha256(payload)[:16]}"


def normalized_reason_codes(values: Iterable[str]) -> list[str]:
    return sorted({str(value or "").strip() for value in values if str(value or "").strip()})


def missing_policy_reason_codes(policy: Phase0APolicyRegistry) -> list[str]:
    missing: list[str] = []
    if not policy.benchmark_policy:
        missing.append(REASON_POLICY_BENCHMARK_MISSING)
    elif not _has_keys(policy.benchmark_policy, "policy_id", "policy_hash", "universe_layer", "entry_basis", "effective_range"):
        missing.append(REASON_POLICY_BENCHMARK_INCOMPLETE)
    if not policy.cost_policy:
        missing.append(REASON_POLICY_COST_MISSING)
    elif not _has_keys(policy.cost_policy, "policy_id", "policy_hash", "effective_range"):
        missing.append(REASON_POLICY_COST_INCOMPLETE)
    if not policy.label_policy:
        missing.append(REASON_POLICY_LABEL_MISSING)
    elif not _has_keys(
        policy.label_policy,
        "policy_id",
        "policy_hash",
        "horizons",
        "entry_basis",
        "censor_rule",
        "barrier_event_order_policy_id",
        "barrier_event_order_policy_hash",
    ):
        missing.append(REASON_POLICY_LABEL_INCOMPLETE)
    if not _has_keys(policy.universe_policy, "policy_id", "policy_hash"):
        missing.append(REASON_POLICY_UNIVERSE_INCOMPLETE)
    if not _has_keys(policy.prior_policy, "registry_id", "registry_hash", "frozen_at"):
        missing.append(REASON_POLICY_PRIOR_INCOMPLETE)
    if not _has_keys(policy.multiple_testing_policy, "registry_id", "registry_hash", "frozen_at"):
        missing.append(REASON_POLICY_MULTIPLE_TESTING_INCOMPLETE)
    missing.extend(missing_embargo_policy_reason_codes(policy))
    return normalized_reason_codes(missing)


def _has_keys(payload: dict[str, Any], *keys: str) -> bool:
    return all(payload.get(key) not in (None, "", [], {}) for key in keys)


def missing_embargo_policy_reason_codes(policy: Phase0APolicyRegistry) -> list[str]:
    if not all(
        (
            policy.embargo_policy_id,
            policy.embargo_policy_version,
            policy.embargo_policy_hash,
            policy.cutoff_timestamp_normalization,
            policy.training_label_information_end_rule,
            policy.calendar_version,
            policy.calendar_hash,
        )
    ):
        return [REASON_EMBARGO_POLICY_MISSING]
    return []


def embargo_formal_start(
    *,
    effective_cutoff: date | None,
    trading_days: Iterable[date] | None,
    minimum_trading_day_gap: int,
) -> tuple[date | None, list[str]]:
    """Return the first decision day strictly after the configured trading-day embargo."""

    if effective_cutoff is None:
        return None, [REASON_CUTOFF_MISSING]
    if trading_days is None:
        return None, [REASON_EMBARGO_CALENDAR_MISSING]
    normalized_days = sorted(set(trading_days))
    days_after_cutoff = [item for item in normalized_days if item > effective_cutoff]
    if len(days_after_cutoff) <= minimum_trading_day_gap:
        return None, [REASON_EMBARGO_WINDOW_INSUFFICIENT]
    return days_after_cutoff[minimum_trading_day_gap], []


def effective_cutoff(cutoffs: dict[str, date | None]) -> tuple[date | None, list[str]]:
    """Return the latest mandatory source cutoff, failing closed on any missing input."""

    missing = sorted(name for name, cutoff in cutoffs.items() if cutoff is None)
    if missing:
        return None, [REASON_CUTOFF_MISSING, *[f"ADVISORY_PHASE0A_CUTOFF_MISSING_{name.upper()}" for name in missing]]
    return max(cutoff for cutoff in cutoffs.values() if cutoff is not None), []


def classify_oos(input_value: OOSClassificationInput) -> OOSClassification:
    """Classify one decision date without promoting retrospective evidence to formal OOS."""

    reasons = list(input_value.reason_codes)
    formal_ready = (
        input_value.mandatory_closure_complete
        and input_value.historical_semantics_available
        and input_value.point_in_time_source_available
        and input_value.candidate_authority_formal
        and input_value.effective_cutoff is not None
        and (input_value.formal_start_date is None or input_value.decision_date >= input_value.formal_start_date)
        and input_value.effective_cutoff <= input_value.decision_date
    )
    if input_value.effective_cutoff is None:
        reasons.append(REASON_CUTOFF_MISSING)
    elif input_value.effective_cutoff > input_value.decision_date:
        reasons.append(REASON_CUTOFF_AFTER_DECISION)
    if not input_value.historical_semantics_available:
        reasons.append(REASON_RUNTIME_HISTORICAL_MISSING)
    if not input_value.point_in_time_source_available:
        reasons.append(REASON_SOURCE_PIT_MISSING)
    if not input_value.candidate_authority_formal:
        reasons.append(REASON_CANDIDATE_AUTHORITY_MISSING)
    if input_value.formal_start_date is not None and input_value.decision_date < input_value.formal_start_date:
        reasons.append(REASON_FORMAL_WINDOW_EMPTY)

    if formal_ready:
        return OOSClassification(
            decision_date=input_value.decision_date,
            formal_oos_status=FormalOOSStatus.FORMAL_OOS,
            availability_status=AvailabilityStatus.AVAILABLE,
            effective_cutoff=input_value.effective_cutoff,
            phase0a_reason_codes=normalized_reason_codes(reasons),
            upstream_reason_codes=normalized_reason_codes(input_value.upstream_reason_codes),
        )

    if input_value.research_replay_eligible and input_value.mandatory_closure_complete:
        reasons.append(REASON_RETROSPECTIVE_ONLY)
        status = FormalOOSStatus.RETROSPECTIVE_RESEARCH_ONLY
    else:
        reasons.append(REASON_FORMAL_WINDOW_EMPTY)
        status = FormalOOSStatus.NONE
    return OOSClassification(
        decision_date=input_value.decision_date,
        formal_oos_status=status,
        availability_status=AvailabilityStatus.UNAVAILABLE,
        effective_cutoff=input_value.effective_cutoff,
        phase0a_reason_codes=normalized_reason_codes(reasons),
        upstream_reason_codes=normalized_reason_codes(input_value.upstream_reason_codes),
    )


def coalesce_oos_intervals(classifications: Iterable[OOSClassification]) -> list[OOSInterval]:
    """Coalesce adjacent equal daily classifications into deterministic atomic intervals."""

    ordered = sorted(classifications, key=lambda item: item.decision_date)
    if not ordered:
        return []
    groups: list[list[OOSClassification]] = [[ordered[0]]]
    for item in ordered[1:]:
        previous = groups[-1][-1]
        contiguous = (item.decision_date - previous.decision_date).days == 1
        same_payload = (
            item.signal_context_hash == previous.signal_context_hash
            and item.signal_capability == previous.signal_capability
            and item.formal_oos_status == previous.formal_oos_status
            and item.availability_status == previous.availability_status
            and item.effective_cutoff == previous.effective_cutoff
            and item.phase0a_reason_codes == previous.phase0a_reason_codes
            and item.upstream_reason_codes == previous.upstream_reason_codes
        )
        if contiguous and same_payload:
            groups[-1].append(item)
        else:
            groups.append([item])
    intervals: list[OOSInterval] = []
    for group in groups:
        first = group[0]
        last = group[-1]
        identity = {
            "schema_version": "advisory_phase0a_oos_interval_v1",
            "start_date": first.decision_date,
            "end_date": last.decision_date,
            "signal_context_hash": first.signal_context_hash,
            "signal_capability": first.signal_capability,
            "formal_oos_status": first.formal_oos_status,
            "availability_status": first.availability_status,
            "effective_cutoff": first.effective_cutoff,
            "phase0a_reason_codes": first.phase0a_reason_codes,
            "upstream_reason_codes": first.upstream_reason_codes,
        }
        intervals.append(
            OOSInterval(
                interval_id=stable_identifier("oos", identity),
                start_date=first.decision_date,
                end_date=last.decision_date,
                signal_context_hash=first.signal_context_hash,
                signal_capability=first.signal_capability,
                formal_oos_status=first.formal_oos_status,
                availability_status=first.availability_status,
                effective_cutoff=first.effective_cutoff,
                phase0a_reason_codes=first.phase0a_reason_codes,
                upstream_reason_codes=first.upstream_reason_codes,
            )
        )
    return intervals
