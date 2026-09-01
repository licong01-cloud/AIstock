"""Canonical PIT runtime binding for Selection, Paper, and Simulation.

This module is the online counterpart of the frozen QE/HMM dataset binding.
It never chooses the newest PIT state.  A new trading admission resolves the
singleton authority pointer once, persists an immutable generation lease, and
all downstream consumers inherit that exact lease.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
    LEGACY_PIT_RULE_VERSION,
    LEGACY_PIT_UNIVERSE_KEY,
    CanonicalPitAuthorityResolver,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
    legacy_rule_parameters_digest,
)
from backend.services.trading_core.errors import InvalidStateTransitionError, RuntimeConfigInvalidError


CANONICAL_PIT_RUNTIME_PROFILE_KEY = "canonical_pit_authority_profile"
CANONICAL_PIT_RUNTIME_LEASE_KEY = "canonical_pit_authority"
CANONICAL_PIT_POINTER_PROFILE_SCHEMA = "canonical_authority_pointer_v1"
CANONICAL_PIT_RUNTIME_LEASE_SCHEMA = "selection_canonical_pit_runtime_lease_v1"
LEGACY_PIT_RUNTIME_PROFILE = "legacy_explicit_v1"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_LEASE_FIELDS = frozenset(
    {
        "schema_version",
        "authority_id",
        "authority_status",
        "universe_key",
        "rule_version",
        "rule_parameters_digest",
        "activation_generation",
        "activation_envelope_digest",
        "expected_source_commit",
        "state_source_digest",
        "coverage_start",
        "coverage_end",
    }
)


class CanonicalPitRuntimeError(RuntimeConfigInvalidError):
    """Raised when an online consumer cannot prove one PIT runtime identity."""

    error_code = "CANONICAL_PIT_RUNTIME_INVALID"


class CanonicalPitGenerationDriftError(InvalidStateTransitionError):
    """Raised when a running session observes a different authority generation."""

    error_code = "CANONICAL_PIT_GENERATION_DRIFT"


@dataclass(frozen=True, slots=True)
class SelectionPitRuntimeLease:
    authority_id: str
    authority_status: PitAuthorityStatus
    universe_key: str
    rule_version: str
    rule_parameters_digest: str
    activation_generation: int
    coverage_start: date
    coverage_end: date
    activation_envelope_digest: str | None = None
    expected_source_commit: str | None = None
    state_source_digest: str | None = None

    @classmethod
    def from_binding(cls, binding: PitConsumerBinding) -> "SelectionPitRuntimeLease":
        _require_binding_types(binding)
        if binding.authority_id != CANONICAL_PIT_AUTHORITY_ID:
            raise CanonicalPitRuntimeError(
                "PIT runtime binding uses an unknown authority",
                context={"authority_id": binding.authority_id},
            )
        if binding.activation_generation < 0:
            raise CanonicalPitRuntimeError("PIT runtime binding has no valid activation_generation")
        if (
            binding.coverage_start is None
            or binding.coverage_end is None
            or binding.coverage_start > binding.coverage_end
        ):
            raise CanonicalPitRuntimeError("PIT runtime binding has invalid coverage")
        if not _SHA256_RE.fullmatch(str(binding.rule_parameters_digest or "")):
            raise CanonicalPitRuntimeError("PIT runtime binding rule_parameters_digest must be sha256")
        if not _SHA256_RE.fullmatch(str(binding.state_source_digest or "")):
            raise CanonicalPitRuntimeError("PIT runtime binding state_source_digest must be sha256")
        if binding.activation_envelope_digest is not None and not _SHA256_RE.fullmatch(
            str(binding.activation_envelope_digest)
        ):
            raise CanonicalPitRuntimeError("PIT runtime binding activation_envelope_digest must be sha256 when present")
        if binding.authority_status is PitAuthorityStatus.ACTIVE_CANONICAL:
            if (
                binding.universe_key != CANONICAL_PIT_UNIVERSE_KEY
                or binding.rule_version != CANONICAL_PIT_RULE_VERSION
                or binding.rule_parameters_digest != canonical_rule_parameters_digest()
                or binding.activation_generation < 1
                or binding.activation_envelope_digest is None
                or not str(binding.expected_source_commit or "").strip()
            ):
                raise CanonicalPitRuntimeError("active canonical PIT runtime identity is inconsistent")
        elif binding.authority_status in {
            PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION,
            PitAuthorityStatus.EMERGENCY_LEGACY_ROLLBACK,
        }:
            if (
                binding.universe_key != LEGACY_PIT_UNIVERSE_KEY
                or binding.rule_version != LEGACY_PIT_RULE_VERSION
                or binding.rule_parameters_digest != legacy_rule_parameters_digest()
            ):
                raise CanonicalPitRuntimeError("legacy PIT runtime identity is inconsistent")
        else:
            raise CanonicalPitRuntimeError(
                "PIT runtime lease uses a non-live authority status",
                context={"authority_status": binding.authority_status.value},
            )
        return cls(
            authority_id=binding.authority_id,
            authority_status=binding.authority_status,
            universe_key=binding.universe_key,
            rule_version=binding.rule_version,
            rule_parameters_digest=binding.rule_parameters_digest,
            activation_generation=binding.activation_generation,
            activation_envelope_digest=binding.activation_envelope_digest,
            expected_source_commit=binding.expected_source_commit,
            state_source_digest=binding.state_source_digest,
            coverage_start=binding.coverage_start,
            coverage_end=binding.coverage_end,
        )

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "SelectionPitRuntimeLease":
        if not isinstance(raw, Mapping):
            raise CanonicalPitRuntimeError("canonical PIT runtime lease must be an object")
        unknown = sorted(set(raw).difference(_LEASE_FIELDS))
        if unknown:
            raise CanonicalPitRuntimeError(
                "canonical PIT runtime lease contains unknown fields",
                context={"unknown_fields": unknown},
            )
        if raw.get("schema_version") != CANONICAL_PIT_RUNTIME_LEASE_SCHEMA:
            raise CanonicalPitRuntimeError(
                "canonical PIT runtime lease schema is unsupported",
                context={"schema_version": raw.get("schema_version")},
            )
        generation = raw.get("activation_generation")
        if type(generation) is not int:
            raise CanonicalPitRuntimeError("canonical PIT runtime activation_generation must be an integer")
        coverage_start_raw = raw.get("coverage_start")
        coverage_end_raw = raw.get("coverage_end")
        if not isinstance(coverage_start_raw, str) or not isinstance(coverage_end_raw, str):
            raise CanonicalPitRuntimeError("canonical PIT runtime lease coverage must use ISO date strings")
        try:
            status = PitAuthorityStatus(_required_text(raw, "authority_status"))
            coverage_start = date.fromisoformat(coverage_start_raw)
            coverage_end = date.fromisoformat(coverage_end_raw)
        except ValueError as exc:
            raise CanonicalPitRuntimeError("canonical PIT runtime lease has invalid typed fields") from exc
        binding = PitConsumerBinding(
            authority_id=_required_text(raw, "authority_id"),
            authority_status=status,
            universe_key=_required_text(raw, "universe_key"),
            rule_version=_required_text(raw, "rule_version"),
            rule_parameters_digest=_required_text(raw, "rule_parameters_digest"),
            activation_generation=generation,
            activation_envelope_digest=_optional_text(raw.get("activation_envelope_digest")),
            expected_source_commit=_optional_text(raw.get("expected_source_commit")),
            state_source_digest=_optional_text(raw.get("state_source_digest")),
            coverage_start=coverage_start,
            coverage_end=coverage_end,
        )
        return cls.from_binding(binding)

    def require_trade_date(self, trade_date: date) -> "SelectionPitRuntimeLease":
        if trade_date < self.coverage_start or trade_date > self.coverage_end:
            raise CanonicalPitRuntimeError(
                "canonical PIT runtime lease does not cover trade_date",
                context={
                    "trade_date": trade_date.isoformat(),
                    "coverage_start": self.coverage_start.isoformat(),
                    "coverage_end": self.coverage_end.isoformat(),
                    "activation_generation": self.activation_generation,
                },
            )
        return self

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": CANONICAL_PIT_RUNTIME_LEASE_SCHEMA,
            "authority_id": self.authority_id,
            "authority_status": self.authority_status.value,
            "universe_key": self.universe_key,
            "rule_version": self.rule_version,
            "rule_parameters_digest": self.rule_parameters_digest,
            "activation_generation": self.activation_generation,
            "activation_envelope_digest": self.activation_envelope_digest,
            "expected_source_commit": self.expected_source_commit,
            "state_source_digest": self.state_source_digest,
            "coverage_start": self.coverage_start.isoformat(),
            "coverage_end": self.coverage_end.isoformat(),
        }


def has_canonical_pit_runtime_profile(runtime_config: Mapping[str, Any] | None) -> bool:
    config = runtime_config or {}
    return CANONICAL_PIT_RUNTIME_PROFILE_KEY in config or CANONICAL_PIT_RUNTIME_LEASE_KEY in config


def validate_canonical_pit_runtime_profile(runtime_config: Mapping[str, Any] | None) -> str:
    """Classify legacy versus pointer profiles without mutating persisted JSON."""

    config = runtime_config or {}
    explicit_key = _explicit_st_universe_key(config)
    if CANONICAL_PIT_RUNTIME_PROFILE_KEY not in config:
        if explicit_key not in {None, LEGACY_PIT_UNIVERSE_KEY}:
            raise CanonicalPitRuntimeError(
                "legacy PIT runtime profile uses a non-authoritative universe key",
                context={"st_universe_key": explicit_key},
            )
        return LEGACY_PIT_RUNTIME_PROFILE
    raw = config.get(CANONICAL_PIT_RUNTIME_PROFILE_KEY)
    if not isinstance(raw, Mapping):
        raise CanonicalPitRuntimeError("canonical_pit_authority_profile must be an object")
    unknown = sorted(set(raw).difference({"schema_version", "authority_id"}))
    if unknown:
        raise CanonicalPitRuntimeError(
            "canonical PIT authority profile contains unknown fields",
            context={"unknown_fields": unknown},
        )
    if raw.get("schema_version") != CANONICAL_PIT_POINTER_PROFILE_SCHEMA:
        raise CanonicalPitRuntimeError(
            "canonical PIT authority profile schema is unsupported",
            context={"schema_version": raw.get("schema_version")},
        )
    if raw.get("authority_id") != CANONICAL_PIT_AUTHORITY_ID:
        raise CanonicalPitRuntimeError(
            "canonical PIT authority profile uses an unknown authority",
            context={"authority_id": raw.get("authority_id")},
        )
    if explicit_key is not None:
        raise CanonicalPitRuntimeError(
            "canonical pointer profile cannot be mixed with st_universe_key",
            context={"st_universe_key": explicit_key},
        )
    return CANONICAL_PIT_POINTER_PROFILE_SCHEMA


def migrate_runtime_config_to_canonical_pointer(runtime_config: Mapping[str, Any]) -> dict[str, Any]:
    """Return a new profile payload; never rewrite an existing persisted version."""

    migrated = copy.deepcopy(dict(runtime_config))
    validate_canonical_pit_runtime_profile(migrated)
    profile = migrated.get("runtime_profile")
    if isinstance(profile, dict):
        risk = profile.get("risk_policy")
        if isinstance(risk, dict):
            risk = dict(risk)
            risk.pop("st_universe_key", None)
            profile = dict(profile)
            profile["risk_policy"] = risk
            migrated["runtime_profile"] = profile
    top_level_risk = migrated.get("risk_policy")
    if isinstance(top_level_risk, dict):
        top_level_risk = dict(top_level_risk)
        top_level_risk.pop("st_universe_key", None)
        migrated["risk_policy"] = top_level_risk
    migrated[CANONICAL_PIT_RUNTIME_PROFILE_KEY] = {
        "schema_version": CANONICAL_PIT_POINTER_PROFILE_SCHEMA,
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
    }
    migrated.pop(CANONICAL_PIT_RUNTIME_LEASE_KEY, None)
    validate_canonical_pit_runtime_profile(migrated)
    return migrated


def freeze_canonical_pit_runtime_binding(
    runtime_config: Mapping[str, Any],
    *,
    trade_date: date,
    authority_resolver: CanonicalPitAuthorityResolver | None = None,
) -> tuple[dict[str, Any], SelectionPitRuntimeLease]:
    """Freeze the singleton pointer once for a new Selection/Paper admission."""

    config = copy.deepcopy(dict(runtime_config))
    profile_mode = validate_canonical_pit_runtime_profile(config)
    existing = config.get(CANONICAL_PIT_RUNTIME_LEASE_KEY)
    if existing is not None:
        lease = SelectionPitRuntimeLease.from_mapping(existing).require_trade_date(trade_date)
        if profile_mode != CANONICAL_PIT_POINTER_PROFILE_SCHEMA:
            raise CanonicalPitRuntimeError("legacy runtime profile cannot carry a canonical PIT lease")
        try:
            current = SelectionPitRuntimeLease.from_binding(
                (authority_resolver or CanonicalPitAuthorityResolver()).resolve_live_binding()
            )
        except Exception as exc:
            raise CanonicalPitRuntimeError("canonical PIT authority pointer cannot be resolved") from exc
        _require_same_runtime_identity(lease, current, message="supplied canonical PIT lease differs from live pointer")
        return config, lease
    if profile_mode != CANONICAL_PIT_POINTER_PROFILE_SCHEMA:
        raise CanonicalPitRuntimeError(
            "legacy PIT runtime profile requires explicit version migration before canonical admission",
            context={"profile_mode": profile_mode},
        )
    try:
        binding = (authority_resolver or CanonicalPitAuthorityResolver()).resolve_live_binding()
    except Exception as exc:
        raise CanonicalPitRuntimeError("canonical PIT authority pointer cannot be resolved") from exc
    lease = SelectionPitRuntimeLease.from_binding(binding).require_trade_date(trade_date)
    config[CANONICAL_PIT_RUNTIME_LEASE_KEY] = lease.as_dict()
    return config, lease


def inherit_canonical_pit_runtime_binding(
    runtime_config: Mapping[str, Any],
    *,
    trade_date: date,
) -> tuple[dict[str, Any], SelectionPitRuntimeLease]:
    """Validate and inherit a lease already frozen by an owning admission boundary."""

    config = copy.deepcopy(dict(runtime_config))
    if validate_canonical_pit_runtime_profile(config) != CANONICAL_PIT_POINTER_PROFILE_SCHEMA:
        raise CanonicalPitRuntimeError("legacy runtime profile cannot inherit a canonical PIT lease")
    lease = require_canonical_pit_runtime_binding(config, trade_date=trade_date)
    return config, lease


def require_canonical_pit_runtime_binding(
    runtime_config: Mapping[str, Any],
    *,
    trade_date: date,
) -> SelectionPitRuntimeLease:
    if validate_canonical_pit_runtime_profile(runtime_config) != CANONICAL_PIT_POINTER_PROFILE_SCHEMA:
        raise CanonicalPitRuntimeError("canonical PIT runtime lease requires the explicit pointer profile")
    raw = (runtime_config or {}).get(CANONICAL_PIT_RUNTIME_LEASE_KEY)
    if raw is None:
        raise CanonicalPitRuntimeError("canonical PIT runtime lease is required")
    return SelectionPitRuntimeLease.from_mapping(raw).require_trade_date(trade_date)


def require_canonical_pit_generation_current(
    runtime_config: Mapping[str, Any],
    *,
    authority_resolver: CanonicalPitAuthorityResolver | None = None,
) -> SelectionPitRuntimeLease:
    """Fail closed when a running session crosses an authority generation."""

    if validate_canonical_pit_runtime_profile(runtime_config) != CANONICAL_PIT_POINTER_PROFILE_SCHEMA:
        raise CanonicalPitRuntimeError("canonical PIT generation check requires the explicit pointer profile")
    raw = (runtime_config or {}).get(CANONICAL_PIT_RUNTIME_LEASE_KEY)
    if raw is None:
        raise CanonicalPitRuntimeError("canonical PIT runtime lease is required")
    lease = SelectionPitRuntimeLease.from_mapping(raw)
    try:
        current = SelectionPitRuntimeLease.from_binding(
            (authority_resolver or CanonicalPitAuthorityResolver()).resolve_live_binding()
        )
    except Exception as exc:
        raise CanonicalPitGenerationDriftError(
            "canonical PIT authority cannot be re-read for generation validation",
            context={"expected_generation": lease.activation_generation},
        ) from exc
    expected = _runtime_identity(lease)
    observed = _runtime_identity(current)
    if observed != expected:
        raise CanonicalPitGenerationDriftError(
            "canonical PIT authority changed during an active session",
            context={
                "expected_generation": lease.activation_generation,
                "actual_generation": current.activation_generation,
                "expected_universe_key": lease.universe_key,
                "actual_universe_key": current.universe_key,
                "identity_changed": True,
            },
        )
    return lease


def _require_same_runtime_identity(
    expected: SelectionPitRuntimeLease,
    observed: SelectionPitRuntimeLease,
    *,
    message: str,
) -> None:
    expected_identity = _runtime_identity(expected)
    observed_identity = _runtime_identity(observed)
    if observed_identity != expected_identity:
        raise CanonicalPitRuntimeError(
            message,
            context={
                "expected_generation": expected.activation_generation,
                "actual_generation": observed.activation_generation,
                "expected_universe_key": expected.universe_key,
                "actual_universe_key": observed.universe_key,
            },
        )


def _explicit_st_universe_key(runtime_config: Mapping[str, Any]) -> str | None:
    values: list[str] = []
    for risk in (
        runtime_config.get("risk_policy"),
        (runtime_config.get("runtime_profile") or {}).get("risk_policy")
        if isinstance(runtime_config.get("runtime_profile"), Mapping)
        else None,
    ):
        if not isinstance(risk, Mapping) or "st_universe_key" not in risk:
            continue
        value = str(risk.get("st_universe_key") or "").strip()
        if not value:
            raise CanonicalPitRuntimeError("st_universe_key cannot be empty")
        values.append(value)
    if len(set(values)) > 1:
        raise CanonicalPitRuntimeError(
            "runtime profile contains conflicting st_universe_key values",
            context={"st_universe_keys": sorted(set(values))},
        )
    return values[0] if values else None


def _runtime_identity(lease: SelectionPitRuntimeLease) -> tuple[Any, ...]:
    return (
        lease.authority_id,
        lease.authority_status,
        lease.universe_key,
        lease.rule_version,
        lease.rule_parameters_digest,
        lease.activation_generation,
        lease.activation_envelope_digest,
        lease.expected_source_commit,
        lease.state_source_digest,
        lease.coverage_start,
        lease.coverage_end,
    )


def _require_binding_types(binding: PitConsumerBinding) -> None:
    required_text = {
        "authority_id": binding.authority_id,
        "universe_key": binding.universe_key,
        "rule_version": binding.rule_version,
        "rule_parameters_digest": binding.rule_parameters_digest,
    }
    if any(
        not isinstance(value, str) or not value.strip() or value != value.strip() for value in required_text.values()
    ):
        raise CanonicalPitRuntimeError("PIT runtime binding identity fields must be non-empty strings")
    if not isinstance(binding.authority_status, PitAuthorityStatus):
        raise CanonicalPitRuntimeError("PIT runtime binding authority_status is invalid")
    if type(binding.activation_generation) is not int:
        raise CanonicalPitRuntimeError("PIT runtime binding activation_generation must be an integer")
    if type(binding.coverage_start) is not date or type(binding.coverage_end) is not date:
        raise CanonicalPitRuntimeError("PIT runtime binding coverage must use dates")
    for field_name in ("activation_envelope_digest", "expected_source_commit", "state_source_digest"):
        value = getattr(binding, field_name)
        if value is not None and (not isinstance(value, str) or not value.strip() or value != value.strip()):
            raise CanonicalPitRuntimeError(
                f"PIT runtime binding {field_name} must be a canonical non-empty string when present"
            )


def _required_text(raw: Mapping[str, Any], field_name: str) -> str:
    value = raw.get(field_name)
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise CanonicalPitRuntimeError(f"canonical PIT runtime lease {field_name} must be a non-empty string")
    return value


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CanonicalPitRuntimeError("canonical PIT runtime optional identity fields must be strings")
    text = value.strip()
    if not text or value != text:
        raise CanonicalPitRuntimeError(
            "canonical PIT runtime optional identity fields must be canonical non-empty strings when present"
        )
    return text


__all__ = [
    "CANONICAL_PIT_POINTER_PROFILE_SCHEMA",
    "CANONICAL_PIT_RUNTIME_LEASE_KEY",
    "CANONICAL_PIT_RUNTIME_LEASE_SCHEMA",
    "CANONICAL_PIT_RUNTIME_PROFILE_KEY",
    "CanonicalPitGenerationDriftError",
    "CanonicalPitRuntimeError",
    "SelectionPitRuntimeLease",
    "freeze_canonical_pit_runtime_binding",
    "has_canonical_pit_runtime_profile",
    "inherit_canonical_pit_runtime_binding",
    "migrate_runtime_config_to_canonical_pointer",
    "require_canonical_pit_generation_current",
    "require_canonical_pit_runtime_binding",
    "validate_canonical_pit_runtime_profile",
]
