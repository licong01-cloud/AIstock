"""Typed identity boundary shared by inference consumers.

The boundary separates frozen candidate, rolling runtime, and explicit legacy
reproduction requests.  It never resolves a missing identity from a newest
state or silently downgrades a v2 request to v1.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Mapping

from .canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
    LEGACY_NONCANONICAL_SNAPSHOT_PREFIXES,
    LEGACY_PIT_RULE_VERSION,
    LEGACY_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
    legacy_rule_parameters_digest,
)


class CanonicalPitInferenceBoundaryError(ValueError):
    code = "CANONICAL_PIT_INFERENCE_BOUNDARY_INVALID"


class InferencePitMode(str, Enum):
    FROZEN_CANDIDATE = "canonical_v2_frozen_candidate"
    ROLLING_RUNTIME = "canonical_v2_rolling_runtime"
    LEGACY_REPRODUCTION = "legacy_reproduction_only"


@dataclass(frozen=True, slots=True)
class InferencePitIdentity:
    mode: InferencePitMode
    binding: PitConsumerBinding
    universe_codes: tuple[str, ...] = ()

    @property
    def receipt_mode(self) -> str:
        if self.mode is InferencePitMode.ROLLING_RUNTIME and self.binding.authority_status in {
            PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION,
            PitAuthorityStatus.EMERGENCY_LEGACY_ROLLBACK,
        }:
            return self.binding.authority_status.value.lower()
        return self.mode.value

    def as_dict(self) -> dict[str, Any]:
        binding = self.binding
        return {
            "mode": self.mode.value,
            "authority_id": binding.authority_id,
            "authority_status": binding.authority_status.value,
            "universe_key": binding.universe_key,
            "rule_version": binding.rule_version,
            "rule_parameters_digest": binding.rule_parameters_digest,
            "activation_generation": binding.activation_generation,
            "activation_envelope_digest": binding.activation_envelope_digest,
            "expected_source_commit": binding.expected_source_commit,
            "state_source_digest": binding.state_source_digest,
            "coverage_start": binding.coverage_start.isoformat() if binding.coverage_start else None,
            "coverage_end": binding.coverage_end.isoformat() if binding.coverage_end else None,
            "release_id": binding.release_id,
            "cutoff": binding.cutoff.isoformat() if binding.cutoff else None,
            "snapshot_digest": binding.snapshot_digest,
            "universe_codes_digest": _codes_digest(self.universe_codes) if self.universe_codes else None,
        }


def resolve_inference_pit_identity(
    request: Mapping[str, Any] | None,
    *,
    manifest: Mapping[str, Any] | None = None,
    version_tag: str,
    live_binding: PitConsumerBinding | None = None,
    allow_active_canonical_pointer: bool = True,
) -> InferencePitIdentity:
    """Resolve an explicit identity or the singleton live authority pointer.

    ``live_binding`` must come from ``CanonicalPitAuthorityResolver``.  It is
    accepted during the legacy-pending migration so merging W6 source cannot
    silently switch or disable the production v1 universe.  Once the pointer
    is canonical, retrospective callers still need an explicit frozen
    identity and cannot borrow the current rolling generation.
    """

    value: Mapping[str, Any] | None = request
    if value is None:
        value = manifest_inference_pit_identity(manifest)
    if value is None:
        if live_binding is not None:
            return _live_pointer_identity(
                live_binding,
                allow_active_canonical_pointer=allow_active_canonical_pointer,
            )
        if version_tag in {"legacy_reproduction", "reproduction"}:
            raise CanonicalPitInferenceBoundaryError(
                "legacy reproduction requires an explicit legacy PIT identity; no default is allowed"
            )
        raise CanonicalPitInferenceBoundaryError(
            "formal/rolling inference requires an explicit canonical PIT identity"
        )
    if not isinstance(value, Mapping):
        raise CanonicalPitInferenceBoundaryError("pit identity must be an object")
    mode_text = str(value.get("mode") or value.get("usage_mode") or "").strip()
    try:
        mode = InferencePitMode(mode_text)
    except ValueError as exc:
        raise CanonicalPitInferenceBoundaryError(
            "pit identity mode must be canonical_v2_frozen_candidate, canonical_v2_rolling_runtime, "
            "or legacy_reproduction_only"
        ) from exc
    if mode is InferencePitMode.LEGACY_REPRODUCTION:
        return _legacy_identity(value)
    if mode is InferencePitMode.ROLLING_RUNTIME:
        return _rolling_identity(value)
    return _frozen_identity(value)


def manifest_inference_pit_identity(manifest: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """Return only an inference identity, not a training/package compatibility binding."""

    if not isinstance(manifest, Mapping):
        return None
    for key in ("canonical_pit_identity", "pit_identity"):
        if key not in manifest:
            continue
        candidate = manifest.get(key)
        if not isinstance(candidate, Mapping):
            raise CanonicalPitInferenceBoundaryError(f"manifest {key} must be an object")
        return candidate
    candidate = manifest.get("canonical_pit_binding")
    if not isinstance(candidate, Mapping):
        return None
    mode_text = str(candidate.get("mode") or candidate.get("usage_mode") or "").strip()
    if mode_text in {item.value for item in InferencePitMode}:
        return candidate
    return None


def _live_pointer_identity(
    binding: PitConsumerBinding,
    *,
    allow_active_canonical_pointer: bool,
) -> InferencePitIdentity:
    if binding.authority_id != CANONICAL_PIT_AUTHORITY_ID:
        raise CanonicalPitInferenceBoundaryError("live PIT pointer authority_id differs")
    _strict_int(binding.activation_generation, "activation_generation")
    if binding.coverage_start is None or binding.coverage_end is None or binding.coverage_start > binding.coverage_end:
        raise CanonicalPitInferenceBoundaryError("live PIT pointer coverage is invalid")
    _sha(binding.state_source_digest, "state_source_digest")
    if binding.authority_status is PitAuthorityStatus.ACTIVE_CANONICAL:
        if not allow_active_canonical_pointer:
            raise CanonicalPitInferenceBoundaryError(
                "retrospective inference requires an explicit frozen PIT identity after canonical activation"
            )
        if (
            binding.universe_key != CANONICAL_PIT_UNIVERSE_KEY
            or binding.rule_version != CANONICAL_PIT_RULE_VERSION
            or binding.rule_parameters_digest != canonical_rule_parameters_digest()
            or binding.activation_generation < 1
            or _sha(binding.activation_envelope_digest, "activation_envelope_digest")
            != binding.activation_envelope_digest
            or not str(binding.expected_source_commit or "").strip()
        ):
            raise CanonicalPitInferenceBoundaryError("active canonical PIT pointer identity differs")
    elif binding.authority_status in {
        PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION,
        PitAuthorityStatus.EMERGENCY_LEGACY_ROLLBACK,
    }:
        if (
            binding.universe_key != LEGACY_PIT_UNIVERSE_KEY
            or binding.rule_version != LEGACY_PIT_RULE_VERSION
            or binding.rule_parameters_digest != legacy_rule_parameters_digest()
        ):
            raise CanonicalPitInferenceBoundaryError("legacy migration PIT pointer identity differs")
    else:
        raise CanonicalPitInferenceBoundaryError("live PIT pointer status is not consumable")
    return InferencePitIdentity(mode=InferencePitMode.ROLLING_RUNTIME, binding=binding)


def _rolling_identity(value: Mapping[str, Any]) -> InferencePitIdentity:
    _require_common(value, canonical=True)
    generation = _strict_int(value.get("activation_generation"), "activation_generation")
    if generation < 1:
        raise CanonicalPitInferenceBoundaryError("active canonical rolling inference requires generation >= 1")
    key = str(value.get("universe_key") or CANONICAL_PIT_UNIVERSE_KEY)
    if key != CANONICAL_PIT_UNIVERSE_KEY:
        raise CanonicalPitInferenceBoundaryError("rolling inference must use the canonical v2 rolling key")
    envelope = _sha(value.get("activation_envelope_digest"), "activation_envelope_digest")
    expected_source_commit = _identifier(value.get("expected_source_commit"), "expected_source_commit")
    state_source_digest = _sha(value.get("state_source_digest"), "state_source_digest")
    coverage_start = _date(value.get("coverage_start"), "coverage_start")
    coverage_end = _date(value.get("coverage_end"), "coverage_end")
    if coverage_start > coverage_end:
        raise CanonicalPitInferenceBoundaryError("rolling inference coverage is invalid")
    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=key,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=generation,
        activation_envelope_digest=envelope,
        expected_source_commit=expected_source_commit,
        state_source_digest=state_source_digest,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    return InferencePitIdentity(mode=InferencePitMode.ROLLING_RUNTIME, binding=binding)


def _frozen_identity(value: Mapping[str, Any]) -> InferencePitIdentity:
    _require_common(value, canonical=True)
    release_id = _identifier(value.get("release_id"), "release_id")
    cutoff = _date(value.get("cutoff"), "cutoff")
    snapshot_digest = _sha(value.get("snapshot_digest") or value.get("frozen_snapshot_digest"), "snapshot_digest")
    codes = _universe_codes(value.get("universe_codes"), field="frozen inference universe_codes")
    frozen_key = f"aistock_equity_pit_snapshot_{release_id}"
    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=frozen_key,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        snapshot_digest=snapshot_digest,
        cutoff=cutoff,
        release_id=release_id,
    )
    return InferencePitIdentity(mode=InferencePitMode.FROZEN_CANDIDATE, binding=binding, universe_codes=codes)


def _legacy_identity(value: Mapping[str, Any]) -> InferencePitIdentity:
    if value.get("authority_id") not in {CANONICAL_PIT_AUTHORITY_ID, None}:
        raise CanonicalPitInferenceBoundaryError("legacy reproduction authority_id is invalid")
    if str(value.get("rule_version") or LEGACY_PIT_RULE_VERSION) != LEGACY_PIT_RULE_VERSION:
        raise CanonicalPitInferenceBoundaryError("legacy reproduction rule_version is invalid")
    release_id = _identifier(value.get("release_id"), "release_id")
    cutoff = _date(value.get("cutoff"), "cutoff")
    snapshot_digest = _sha(value.get("snapshot_digest") or value.get("frozen_snapshot_digest"), "snapshot_digest")
    key = str(value.get("universe_key") or f"{LEGACY_NONCANONICAL_SNAPSHOT_PREFIXES[0]}{release_id}")
    if not key.startswith(LEGACY_NONCANONICAL_SNAPSHOT_PREFIXES):
        raise CanonicalPitInferenceBoundaryError("legacy reproduction requires an immutable snapshot universe key")
    codes = _universe_codes(value.get("universe_codes"), field="legacy reproduction universe_codes")
    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ARCHIVED_NONCANONICAL,
        universe_key=key,
        rule_version=LEGACY_PIT_RULE_VERSION,
        rule_parameters_digest=legacy_rule_parameters_digest(),
        snapshot_digest=snapshot_digest,
        cutoff=cutoff,
        release_id=release_id,
        reproduction_mode=True,
    )
    return InferencePitIdentity(
        mode=InferencePitMode.LEGACY_REPRODUCTION,
        binding=binding,
        universe_codes=codes,
    )


def _require_common(value: Mapping[str, Any], *, canonical: bool) -> None:
    if canonical and value.get("authority_id") not in {None, CANONICAL_PIT_AUTHORITY_ID}:
        raise CanonicalPitInferenceBoundaryError("canonical PIT authority_id differs")
    if canonical and value.get("rule_version") not in {None, CANONICAL_PIT_RULE_VERSION}:
        raise CanonicalPitInferenceBoundaryError("canonical PIT rule_version differs")
    supplied_digest = value.get("rule_parameters_digest")
    if supplied_digest is not None and _sha(supplied_digest, "rule_parameters_digest") != canonical_rule_parameters_digest():
        raise CanonicalPitInferenceBoundaryError("canonical PIT rule_parameters_digest differs")


def _strict_int(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise CanonicalPitInferenceBoundaryError(f"{field} must be a non-negative integer")
    return value


def _sha(value: Any, field: str) -> str:
    text = str(value or "")
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise CanonicalPitInferenceBoundaryError(f"{field} must be lowercase SHA-256")
    return text


def _identifier(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not re.fullmatch(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$", text):
        raise CanonicalPitInferenceBoundaryError(f"{field} is invalid")
    return text


def _date(value: Any, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise CanonicalPitInferenceBoundaryError(f"{field} is not an ISO date") from exc


def _universe_codes(value: Any, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise CanonicalPitInferenceBoundaryError(
            f"{field} must be detached and non-empty; online PIT completion is forbidden"
        )
    normalized: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or raw != raw.strip().upper() or not re.fullmatch(r"\d{6}\.(?:SH|SZ)", raw):
            raise CanonicalPitInferenceBoundaryError(f"{field} contains an invalid A-share code")
        normalized.append(raw)
    if len(set(normalized)) != len(normalized):
        raise CanonicalPitInferenceBoundaryError(f"{field} contains duplicate codes")
    return tuple(sorted(normalized))


def _codes_digest(codes: tuple[str, ...]) -> str:
    return hashlib.sha256(repr(codes).encode("utf-8")).hexdigest()


__all__ = [
    "CanonicalPitInferenceBoundaryError",
    "InferencePitIdentity",
    "InferencePitMode",
    "manifest_inference_pit_identity",
    "resolve_inference_pit_identity",
]
