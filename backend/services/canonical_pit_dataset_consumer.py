"""Strict, database-free PIT identity adapter for formal dataset consumers.

QE and HMM use this module to turn an immutable dataset candidate manifest
into one canonical PIT identity.  Legacy reproduction remains a domain-level
concern and is deliberately not accepted here.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Mapping

from backend.services.canonical_equity_pit import CanonicalPitContractError, require_canonical_consumer_binding
from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.dataset_release.canonical import CanonicalizationError, ensure_sha256
from backend.services.dataset_release.pit import (
    DATASET_CANDIDATE_MANIFEST_SCHEMA,
    DatasetPitBinding,
    PitSnapshotError,
)


class CanonicalPitDatasetConsumerError(ValueError):
    """Raised when a formal consumer cannot prove one canonical PIT identity."""

    code = "CANONICAL_PIT_DATASET_CONSUMER_INVALID"


class FormalDatasetUsage(str, Enum):
    """Allowlisted formal uses of an immutable full dataset release."""

    TRAINING = "formal_training"
    PREDICTION = "formal_prediction"


@dataclass(frozen=True, slots=True)
class CanonicalPitDatasetIdentity:
    """Minimal immutable identity shared by formal QE and HMM consumers."""

    authority_id: str
    rule_version: str
    rule_parameters_digest: str
    release_id: str
    cutoff: date
    frozen_snapshot_digest: str
    manifest_digest: str

    def as_dict(self) -> dict[str, str]:
        return {
            "authority_id": self.authority_id,
            "rule_version": self.rule_version,
            "rule_parameters_digest": self.rule_parameters_digest,
            "release_id": self.release_id,
            "cutoff": self.cutoff.isoformat(),
            "frozen_snapshot_digest": self.frozen_snapshot_digest,
            "manifest_digest": self.manifest_digest,
        }


def require_formal_dataset_pit_identity(
    release_manifest: Mapping[str, Any],
    *,
    usage_mode: FormalDatasetUsage | str,
    expected_manifest_digest: str,
) -> CanonicalPitDatasetIdentity:
    """Validate a full v2 release manifest and return its canonical PIT identity.

    ``expected_manifest_digest`` is the immutable CAS identity supplied by the
    release reference.  Recomputing it here prevents a structurally valid but
    mutated manifest from entering formal training or prediction.
    """

    usage = _require_usage_mode(usage_mode)
    if not isinstance(release_manifest, Mapping):
        raise CanonicalPitDatasetConsumerError("release_manifest must be a mapping")

    try:
        expected_digest = ensure_sha256(expected_manifest_digest, field="expected_manifest_digest")
        encoded_manifest = canonical_json_bytes(dict(release_manifest))
        actual_digest = hashlib.sha256(encoded_manifest).hexdigest()
        manifest = json.loads(encoded_manifest)
    except (CanonicalizationError, TypeError, ValueError) as exc:
        raise CanonicalPitDatasetConsumerError(f"release manifest identity is invalid: {exc}") from exc
    if actual_digest != expected_digest:
        raise CanonicalPitDatasetConsumerError(
            f"release manifest digest differs from immutable reference: expected={expected_digest} actual={actual_digest}"
        )
    if not isinstance(manifest, dict) or manifest.get("schema_version") != DATASET_CANDIDATE_MANIFEST_SCHEMA:
        raise CanonicalPitDatasetConsumerError(
            "formal dataset consumers require a v2 candidate release manifest; legacy/v1 manifests are forbidden"
        )

    try:
        dataset_binding = DatasetPitBinding.from_release_manifest(manifest)
        consumer_binding = dataset_binding.consumer_binding(consumer=usage.value)
        # Keep the W1 validator explicit at this neutral boundary.  W2 already
        # calls it internally, while this second call prevents future W2 parser
        # refactors from weakening the formal-consumer contract.
        require_canonical_consumer_binding(
            consumer_binding,
            consumer=usage.value,
            immutable_snapshot_required=True,
        )
    except (CanonicalPitContractError, PitSnapshotError) as exc:
        raise CanonicalPitDatasetConsumerError(str(exc)) from exc

    return CanonicalPitDatasetIdentity(
        authority_id=consumer_binding.authority_id,
        rule_version=consumer_binding.rule_version,
        rule_parameters_digest=consumer_binding.rule_parameters_digest,
        release_id=dataset_binding.release_id,
        cutoff=dataset_binding.cutoff,
        frozen_snapshot_digest=dataset_binding.frozen_snapshot_digest,
        manifest_digest=actual_digest,
    )


def _require_usage_mode(value: FormalDatasetUsage | str) -> FormalDatasetUsage:
    if isinstance(value, FormalDatasetUsage):
        return value
    try:
        return FormalDatasetUsage(str(value).strip())
    except ValueError as exc:
        raise CanonicalPitDatasetConsumerError(
            "unsupported formal dataset usage_mode; allowed=formal_training, formal_prediction"
        ) from exc


__all__ = [
    "CanonicalPitDatasetConsumerError",
    "CanonicalPitDatasetIdentity",
    "FormalDatasetUsage",
    "require_formal_dataset_pit_identity",
]
