"""Canonical PIT compatibility and immutable v2 upgrade helpers.

The helpers in this module never mutate a published StrategyPackage.  A v1
manifest remains reproduction-only; continuing with canonical PIT requires a
new package version carrying a complete frozen dataset identity and explicit
retraining or revalidation evidence.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.services.quantevolver.qe_dataset_contract import QE_FORMAL_DATASET_BINDING_SCHEMA

from .manifest import compute_manifest_sha256, freeze_manifest
from .models import StrategyPackageCanonicalPitBindingV2, StrategyPackageManifest


PIT_COMPATIBILITY_SCHEMA = "strategy_package_pit_compatibility_v1"
LEGACY_REPRODUCTION_ONLY = "LEGACY_REPRODUCTION_ONLY"
CANONICAL_V2_READY = "CANONICAL_V2_READY"


@dataclass(frozen=True, slots=True)
class StrategyPackagePitCompatibility:
    schema_version: str
    disposition: Literal["LEGACY_REPRODUCTION_ONLY", "CANONICAL_V2_READY"]
    reproduction_only: bool
    canonical_binding: StrategyPackageCanonicalPitBindingV2 | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "disposition": self.disposition,
            "reproduction_only": self.reproduction_only,
            "canonical_binding": (
                self.canonical_binding.model_dump(mode="json")
                if self.canonical_binding is not None
                else None
            ),
        }


def classify_strategy_package_pit(
    manifest: StrategyPackageManifest,
) -> StrategyPackagePitCompatibility:
    """Classify one manifest without upgrading, rewriting, or resolving data."""

    if manifest.is_canonical_pit_v2_manifest:
        binding = manifest.canonical_pit_binding
        if binding is None:  # protected by the model, kept fail-closed for callers
            raise StrategyPackageValidationError(
                "canonical StrategyPackage manifest has no PIT binding",
                context={"package_id": manifest.package_id},
            )
        return StrategyPackagePitCompatibility(
            schema_version=PIT_COMPATIBILITY_SCHEMA,
            disposition=CANONICAL_V2_READY,
            reproduction_only=False,
            canonical_binding=binding,
        )
    return StrategyPackagePitCompatibility(
        schema_version=PIT_COMPATIBILITY_SCHEMA,
        disposition=LEGACY_REPRODUCTION_ONLY,
        reproduction_only=True,
        canonical_binding=None,
    )


def require_canonical_pit_strategy_package(
    manifest: StrategyPackageManifest,
    *,
    operation: str,
) -> StrategyPackageCanonicalPitBindingV2:
    compatibility = classify_strategy_package_pit(manifest)
    if compatibility.reproduction_only or compatibility.canonical_binding is None:
        raise StrategyPackageValidationError(
            "legacy StrategyPackage is restricted to explicit historical reproduction",
            context={
                "package_id": manifest.package_id,
                "manifest_version": manifest.manifest_version,
                "operation": str(operation or "").strip() or "unspecified",
                "required_disposition": CANONICAL_V2_READY,
            },
        )
    return compatibility.canonical_binding


def build_canonical_pit_v2_manifest(
    source: StrategyPackageManifest,
    *,
    package_id: str,
    package_version: str,
    dataset_binding: Mapping[str, Any],
    qualification_method: Literal["RETRAINED", "REVALIDATED"],
    qualification_evidence_digest: str,
) -> StrategyPackageManifest:
    """Create a new immutable v2 package version from an existing manifest."""

    target_package_id = str(package_id or "").strip()
    target_version = str(package_version or "").strip()
    if not target_package_id or target_package_id == source.package_id:
        raise StrategyPackageValidationError(
            "canonical PIT migration requires a distinct non-empty package_id",
            context={"source_package_id": source.package_id},
        )
    if not target_version or target_version == source.package_version:
        raise StrategyPackageValidationError(
            "canonical PIT migration requires a distinct non-empty package_version",
            context={"package_id": source.package_id, "source_package_version": source.package_version},
        )
    if source.is_canonical_pit_v2_manifest:
        raise StrategyPackageValidationError(
            "canonical PIT package migration source is already v2",
            context={"package_id": source.package_id, "package_version": source.package_version},
        )
    if not source.is_alpha_core_manifest:
        raise StrategyPackageValidationError(
            "legacy runtime manifest cannot be silently converted into an alpha-core v2 package",
            context={
                "package_id": source.package_id,
                "manifest_version": source.manifest_version,
                "required_source_manifest_version": "alpha_core_v1",
            },
        )
    source_digest = str(source.manifest_sha256 or "").strip().lower()
    actual_source_digest = compute_manifest_sha256(source)
    if source_digest != actual_source_digest:
        raise StrategyPackageValidationError(
            "canonical PIT migration source manifest identity is invalid",
            context={
                "package_id": source.package_id,
                "expected_manifest_sha256": source_digest or None,
                "actual_manifest_sha256": actual_source_digest,
            },
        )

    raw = dict(dataset_binding)
    required_binding_fields = {
        "schema_version",
        "usage_mode",
        "authority_id",
        "rule_version",
        "rule_parameters_digest",
        "release_id",
        "cutoff",
        "frozen_snapshot_digest",
        "manifest_digest",
    }
    if set(raw) != required_binding_fields:
        raise StrategyPackageValidationError(
            "canonical PIT package requires one complete QE formal dataset binding",
            context={
                "missing_fields": sorted(required_binding_fields.difference(raw)),
                "unknown_fields": sorted(set(raw).difference(required_binding_fields)),
            },
        )
    if raw.get("schema_version") != QE_FORMAL_DATASET_BINDING_SCHEMA or raw.get("usage_mode") != "formal_training":
        raise StrategyPackageValidationError(
            "canonical PIT package source must be a QE formal_training binding",
            context={
                "schema_version": raw.get("schema_version"),
                "usage_mode": raw.get("usage_mode"),
            },
        )
    binding = StrategyPackageCanonicalPitBindingV2.model_validate(
        {
            "authority_id": raw.get("authority_id"),
            "rule_version": raw.get("rule_version"),
            "rule_parameters_digest": raw.get("rule_parameters_digest"),
            "release_id": raw.get("release_id"),
            "release_cutoff": raw.get("cutoff"),
            "frozen_snapshot_digest": raw.get("frozen_snapshot_digest"),
            "release_manifest_digest": raw.get("manifest_digest"),
            "qualification_method": qualification_method,
            "qualification_evidence_digest": qualification_evidence_digest,
        }
    )
    source_evidence = deepcopy(source.source_evidence)
    if "canonical_pit_migration" in source_evidence:
        raise StrategyPackageValidationError(
            "canonical PIT migration source evidence already exists on the legacy source",
            context={"package_id": source.package_id},
        )
    source_evidence["canonical_pit_migration"] = {
        "schema_version": "strategy_package_canonical_pit_migration_source_v1",
        "source_package_id": source.package_id,
        "source_package_version": source.package_version,
        "source_manifest_sha256": source_digest,
    }
    candidate = source.model_copy(
        update={
            "manifest_version": "alpha_core_v2",
            "package_id": target_package_id,
            "package_version": target_version,
            "canonical_pit_binding": binding,
            "source_evidence": source_evidence,
            "manifest_sha256": None,
        }
    )
    # model_copy does not run model validators; validate the complete payload
    # before freezing so no partial v2 object can receive a trusted digest.
    validated = StrategyPackageManifest.model_validate(candidate.model_dump(mode="json"))
    return freeze_manifest(validated)


__all__ = [
    "CANONICAL_V2_READY",
    "LEGACY_REPRODUCTION_ONLY",
    "PIT_COMPATIBILITY_SCHEMA",
    "StrategyPackagePitCompatibility",
    "build_canonical_pit_v2_manifest",
    "classify_strategy_package_pit",
    "require_canonical_pit_strategy_package",
]
