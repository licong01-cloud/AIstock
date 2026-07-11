"""Pure canonicalization and OOS policy functions for Advisory Phase 0A."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, time, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
from pathlib import Path
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
POLICY_REGISTRY_SCHEMA_VERSION = "advisory_phase0a_policy_registry_v1"
POLICY_REGISTRY_ROOT = Path(__file__).resolve().parent / "policy_registry"
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
REASON_POLICY_REGISTRY_NOT_FROZEN = "ADVISORY_PHASE0A_POLICY_REGISTRY_NOT_FROZEN"
REASON_POLICY_REGISTRY_HASH_MISMATCH = "ADVISORY_PHASE0A_POLICY_REGISTRY_HASH_MISMATCH"
REASON_POLICY_REGISTRY_IDENTITY_MISMATCH = "ADVISORY_PHASE0A_POLICY_REGISTRY_IDENTITY_MISMATCH"
REASON_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID = "ADVISORY_PHASE0A_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID"
REASON_POLICY_REGISTRY_PROHIBITED_FIELD = "ADVISORY_PHASE0A_POLICY_REGISTRY_PROHIBITED_FIELD"

_FROZEN_POLICY_REQUIRED_FIELDS = {
    "schema_version",
    "policy_registry_id",
    "policy_version",
    "serializer_version",
    "frozen_at",
    "effective_from_trade_date",
    "effective_to_trade_date",
    "benchmark_policy",
    "cost_policy",
    "label_policy",
    "universe_policy",
    "embargo_policy",
    "prior_policy",
    "multiple_testing_policy",
    "style_assignment_policy",
    "registry_content_hash",
}
_FROZEN_POLICY_ALLOWED_FIELDS = frozenset(_FROZEN_POLICY_REQUIRED_FIELDS)
_PROHIBITED_POLICY_FIELD_NAMES = frozenset(
    {
        "approved_by",
        "approval_status",
        "decision_chain",
        "revoke",
        "action_authorization",
        "signature",
        "signatures",
        "role",
        "roles",
    }
)


class CanonicalJSONError(ValueError):
    """Raised when a payload cannot be represented by the Phase 0A serializer."""


class PolicyRegistryValidationError(ValueError):
    """Raised when a policy registry cannot be used for a formal audit."""

    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


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


def policy_registry_path(
    *,
    policy_registry_id: str,
    policy_version: str,
    registry_root: Path | None = None,
) -> Path:
    """Return the only allowed repo-tracked location for one frozen registry version."""

    registry_id = _registry_identifier(policy_registry_id, field_name="policy_registry_id")
    version = _registry_identifier(policy_version, field_name="policy_version")
    root = (registry_root or POLICY_REGISTRY_ROOT).resolve()
    path = (root / registry_id / f"{version}.json").resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:  # pragma: no cover - identifier validation makes this defensive.
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_IDENTITY_MISMATCH,
            "policy registry path escapes the configured registry root",
        ) from exc
    return path


def load_frozen_policy_registry(
    *,
    policy_registry_id: str,
    policy_version: str,
    registry_root: Path | None = None,
) -> Phase0APolicyRegistry:
    """Load a formal-audit policy only from the immutable repository registry."""

    path = policy_registry_path(
        policy_registry_id=policy_registry_id,
        policy_version=policy_version,
        registry_root=registry_root,
    )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            f"unable to read frozen policy registry {path}: {exc}",
        ) from exc
    if not isinstance(payload, dict):
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            f"frozen policy registry {path} must contain a JSON object",
        )
    return validate_frozen_policy_registry_payload(
        payload,
        expected_policy_registry_id=policy_registry_id,
        expected_policy_version=policy_version,
    )


def validate_frozen_policy_registry_payload(
    payload: dict[str, Any],
    *,
    expected_policy_registry_id: str | None = None,
    expected_policy_version: str | None = None,
) -> Phase0APolicyRegistry:
    """Validate the stable frozen policy shape and its canonical content hash."""

    _reject_prohibited_policy_fields(payload)
    missing = sorted(_FROZEN_POLICY_REQUIRED_FIELDS - set(payload))
    extra = sorted(set(payload) - _FROZEN_POLICY_ALLOWED_FIELDS)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing required fields: {missing}")
        if extra:
            details.append(f"unsupported fields: {extra}")
        raise PolicyRegistryValidationError(REASON_POLICY_REGISTRY_NOT_FROZEN, "; ".join(details))

    try:
        policy = Phase0APolicyRegistry.model_validate(payload)
    except ValueError as exc:
        raise PolicyRegistryValidationError(REASON_POLICY_REGISTRY_NOT_FROZEN, str(exc)) from exc

    if policy.schema_version != POLICY_REGISTRY_SCHEMA_VERSION:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            f"unsupported schema_version={policy.schema_version}",
        )
    if policy.serializer_version != SERIALIZER_VERSION:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            f"unsupported serializer_version={policy.serializer_version}",
        )
    if policy.frozen_at is None or policy.frozen_at.tzinfo is None:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            "frozen_at must be a timezone-aware timestamp",
        )
    if policy.effective_from_trade_date is None:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID,
            "effective_from_trade_date is required",
        )
    if not policy.is_frozen:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            "policy registry id, version, frozen_at, effective range, and content hash are required",
        )
    if expected_policy_registry_id is not None and policy.policy_registry_id != expected_policy_registry_id:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_IDENTITY_MISMATCH,
            f"registry id mismatch: expected={expected_policy_registry_id} actual={policy.policy_registry_id}",
        )
    if expected_policy_version is not None and policy.policy_version != expected_policy_version:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_IDENTITY_MISMATCH,
            f"policy version mismatch: expected={expected_policy_version} actual={policy.policy_version}",
        )

    _validate_frozen_policy_sections(policy)
    declared_hash = _sha256_text(payload["registry_content_hash"], field_name="registry_content_hash")
    actual_hash = canonical_json_sha256({key: value for key, value in payload.items() if key != "registry_content_hash"})
    if declared_hash != actual_hash:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_HASH_MISMATCH,
            f"registry_content_hash mismatch: declared={declared_hash} actual={actual_hash}",
        )
    return policy


def _registry_identifier(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or any(not (character.isalnum() or character in ".-_") for character in normalized):
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_IDENTITY_MISMATCH,
            f"{field_name} must contain only letters, digits, '.', '-' or '_'",
        )
    return normalized


def _reject_prohibited_policy_fields(value: Any, *, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            if normalized in _PROHIBITED_POLICY_FIELD_NAMES:
                raise PolicyRegistryValidationError(
                    REASON_POLICY_REGISTRY_PROHIBITED_FIELD,
                    f"prohibited field at {path}.{key}",
                )
            _reject_prohibited_policy_fields(item, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_prohibited_policy_fields(item, path=f"{path}[{index}]")


def _validate_frozen_policy_sections(policy: Phase0APolicyRegistry) -> None:
    benchmark = _required_mapping(policy.benchmark_policy, "benchmark_policy")
    _require_fields(benchmark, "benchmark_policy", "policy_id", "policy_hash", "universe_layer", "entry_basis", "effective_range")
    _sha256_text(benchmark["policy_hash"], field_name="benchmark_policy.policy_hash")

    cost = _required_mapping(policy.cost_policy, "cost_policy")
    _require_fields(
        cost,
        "cost_policy",
        "policy_id",
        "policy_hash",
        "commission_buy_rate",
        "commission_sell_rate",
        "minimum_commission",
        "stamp_duty_sell_rate",
        "transfer_fee_rate",
        "slippage_bps",
        "lot_size",
        "reference_notional",
        "effective_range",
    )
    _sha256_text(cost["policy_hash"], field_name="cost_policy.policy_hash")
    _require_positive_number(cost["commission_buy_rate"], field_name="cost_policy.commission_buy_rate")
    _require_positive_number(cost["commission_sell_rate"], field_name="cost_policy.commission_sell_rate")
    _require_positive_number(cost["minimum_commission"], field_name="cost_policy.minimum_commission")
    _require_positive_number(cost["lot_size"], field_name="cost_policy.lot_size")
    _require_positive_number(cost["reference_notional"], field_name="cost_policy.reference_notional")

    label = _required_mapping(policy.label_policy, "label_policy")
    _require_fields(
        label,
        "label_policy",
        "policy_id",
        "policy_hash",
        "horizons",
        "entry_basis",
        "barrier_event_order_policy_id",
        "barrier_event_order_policy_hash",
        "terminal_return_rule",
        "censor_rule",
        "projection_policy",
    )
    _sha256_text(label["policy_hash"], field_name="label_policy.policy_hash")
    _sha256_text(label["barrier_event_order_policy_hash"], field_name="label_policy.barrier_event_order_policy_hash")
    if not isinstance(label["horizons"], list) or not label["horizons"]:
        raise PolicyRegistryValidationError(REASON_POLICY_REGISTRY_NOT_FROZEN, "label_policy.horizons must be a non-empty list")

    universe = _required_mapping(policy.universe_policy, "universe_policy")
    _require_fields(universe, "universe_policy", "policy_id", "policy_hash", "effective_range", "calendar_snapshot_required")
    _sha256_text(universe["policy_hash"], field_name="universe_policy.policy_hash")
    if universe["calendar_snapshot_required"] is not True:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            "universe_policy.calendar_snapshot_required must be true",
        )

    embargo = _required_mapping(policy.embargo_policy, "embargo_policy")
    _require_fields(
        embargo,
        "embargo_policy",
        "policy_id",
        "policy_version",
        "policy_hash",
        "minimum_trading_day_gap",
        "cutoff_timestamp_normalization",
        "training_label_information_end_rule",
        "calendar_snapshot_required",
    )
    _sha256_text(embargo["policy_hash"], field_name="embargo_policy.policy_hash")
    if embargo["policy_id"] != "ADVISORY_RESEARCH_EMBARGO_V1" or embargo["minimum_trading_day_gap"] != 20:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            "embargo_policy must fix ADVISORY_RESEARCH_EMBARGO_V1.minimum_trading_day_gap=20",
        )
    if embargo["calendar_snapshot_required"] is not True:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            "embargo_policy.calendar_snapshot_required must be true",
        )

    for name, section in (
        ("prior_policy", policy.prior_policy),
        ("multiple_testing_policy", policy.multiple_testing_policy),
    ):
        registry = _required_mapping(section, name)
        _require_fields(registry, name, "registry_id", "registry_hash", "frozen_at", "entries")
        _sha256_text(registry["registry_hash"], field_name=f"{name}.registry_hash")
        if not isinstance(registry["entries"], list):
            raise PolicyRegistryValidationError(REASON_POLICY_REGISTRY_NOT_FROZEN, f"{name}.entries must be a list")

    style = _required_mapping(policy.style_assignment_policy, "style_assignment_policy")
    _require_fields(style, "style_assignment_policy", "policy_id", "policy_hash", "effective_range", "assignment_method")
    _sha256_text(style["policy_hash"], field_name="style_assignment_policy.policy_hash")

    root_start = policy.effective_from_trade_date
    root_end = policy.effective_to_trade_date
    assert root_start is not None
    for name, section in (
        ("benchmark_policy", benchmark),
        ("cost_policy", cost),
        ("universe_policy", universe),
        ("style_assignment_policy", style),
    ):
        start, end = _effective_range(section["effective_range"], field_name=f"{name}.effective_range")
        if start > root_start or (end is not None and end < root_start) or (root_end is not None and (end is None or end < root_end)):
            raise PolicyRegistryValidationError(
                REASON_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID,
                f"{name}.effective_range does not cover the registry effective range",
            )


def _required_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not value:
        raise PolicyRegistryValidationError(REASON_POLICY_REGISTRY_NOT_FROZEN, f"{field_name} must be a non-empty object")
    return value


def _require_fields(payload: dict[str, Any], field_name: str, *required_fields: str) -> None:
    missing = [key for key in required_fields if key not in payload or payload[key] in (None, "")]
    if missing:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            f"{field_name} missing required fields: {missing}",
        )


def _sha256_text(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_NOT_FROZEN,
            f"{field_name} must be a lowercase 64-character sha256 digest",
        )
    return normalized


def _require_positive_number(value: Any, *, field_name: str) -> None:
    try:
        parsed = Decimal(str(value))
    except Exception as exc:  # pragma: no cover - Decimal has several implementation-specific exception types.
        raise PolicyRegistryValidationError(REASON_POLICY_REGISTRY_NOT_FROZEN, f"{field_name} must be numeric") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise PolicyRegistryValidationError(REASON_POLICY_REGISTRY_NOT_FROZEN, f"{field_name} must be positive")


def _effective_range(value: Any, *, field_name: str) -> tuple[date, date | None]:
    if not isinstance(value, str) or value.count("/") != 1:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID,
            f"{field_name} must use YYYY-MM-DD/YYYY-MM-DD or an open end",
        )
    start_text, end_text = value.split("/", 1)
    try:
        start = date.fromisoformat(start_text)
        end = date.fromisoformat(end_text) if end_text else None
    except ValueError as exc:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID,
            f"{field_name} contains an invalid date",
        ) from exc
    if end is not None and end < start:
        raise PolicyRegistryValidationError(
            REASON_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID,
            f"{field_name} end must not be before start",
        )
    return start, end


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
            research_replay_eligible=input_value.research_replay_eligible,
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
        research_replay_eligible=input_value.research_replay_eligible,
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
            and item.research_replay_eligible == previous.research_replay_eligible
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
                research_replay_eligible=first.research_replay_eligible,
                phase0a_reason_codes=first.phase0a_reason_codes,
                upstream_reason_codes=first.upstream_reason_codes,
            )
        )
    return intervals
