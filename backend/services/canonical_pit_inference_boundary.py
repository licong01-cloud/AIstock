"""Typed identity boundary shared by inference consumers.

The boundary separates frozen candidate, rolling runtime, and explicit legacy
reproduction requests.  It never resolves a missing identity from a newest
state or silently downgrades a v2 request to v1.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Mapping

from .canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
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
) -> InferencePitIdentity:
    """Resolve only an explicit request or a manifest-owned v2 binding."""

    value: Mapping[str, Any] | None = request
    if value is None and isinstance(manifest, Mapping):
        for key in ("canonical_pit_identity", "canonical_pit_binding", "pit_identity"):
            candidate = manifest.get(key)
            if isinstance(candidate, Mapping):
                value = candidate
                break
    if value is None:
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


def _rolling_identity(value: Mapping[str, Any]) -> InferencePitIdentity:
    _require_common(value, canonical=True)
    generation = _strict_int(value.get("activation_generation"), "activation_generation")
    key = str(value.get("universe_key") or CANONICAL_PIT_UNIVERSE_KEY)
    if key != CANONICAL_PIT_UNIVERSE_KEY:
        raise CanonicalPitInferenceBoundaryError("rolling inference must use the canonical v2 rolling key")
    envelope = _sha_or_none(value.get("activation_envelope_digest"), "activation_envelope_digest")
    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=key,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=generation,
        activation_envelope_digest=envelope,
    )
    return InferencePitIdentity(mode=InferencePitMode.ROLLING_RUNTIME, binding=binding)


def _frozen_identity(value: Mapping[str, Any]) -> InferencePitIdentity:
    _require_common(value, canonical=True)
    release_id = _identifier(value.get("release_id"), "release_id")
    cutoff = _date(value.get("cutoff"), "cutoff")
    snapshot_digest = _sha(value.get("snapshot_digest") or value.get("frozen_snapshot_digest"), "snapshot_digest")
    codes = value.get("universe_codes", ())
    if not isinstance(codes, (list, tuple)) or not codes or any(not isinstance(code, str) for code in codes):
        raise CanonicalPitInferenceBoundaryError(
            "frozen inference must carry detached universe_codes; online PIT completion is forbidden"
        )
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
    return InferencePitIdentity(mode=InferencePitMode.FROZEN_CANDIDATE, binding=binding, universe_codes=tuple(sorted(set(codes))))


def _legacy_identity(value: Mapping[str, Any]) -> InferencePitIdentity:
    if value.get("authority_id") not in {CANONICAL_PIT_AUTHORITY_ID, None}:
        raise CanonicalPitInferenceBoundaryError("legacy reproduction authority_id is invalid")
    if str(value.get("rule_version") or LEGACY_PIT_RULE_VERSION) != LEGACY_PIT_RULE_VERSION:
        raise CanonicalPitInferenceBoundaryError("legacy reproduction rule_version is invalid")
    key = str(value.get("universe_key") or LEGACY_PIT_UNIVERSE_KEY)
    if key != LEGACY_PIT_UNIVERSE_KEY:
        raise CanonicalPitInferenceBoundaryError("legacy reproduction universe key is invalid")
    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ARCHIVED_NONCANONICAL,
        universe_key=key,
        rule_version=LEGACY_PIT_RULE_VERSION,
        rule_parameters_digest=legacy_rule_parameters_digest(),
        reproduction_mode=True,
    )
    return InferencePitIdentity(mode=InferencePitMode.LEGACY_REPRODUCTION, binding=binding)


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


def _sha_or_none(value: Any, field: str) -> str | None:
    if value is None or value == "":
        return None
    return _sha(value, field)


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


def _codes_digest(codes: tuple[str, ...]) -> str:
    import hashlib

    return hashlib.sha256("\n".join(codes).encode("utf-8")).hexdigest()


__all__ = [
    "CanonicalPitInferenceBoundaryError",
    "InferencePitIdentity",
    "InferencePitMode",
    "resolve_inference_pit_identity",
]
