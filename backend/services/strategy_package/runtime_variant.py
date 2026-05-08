"""Runtime variant records for governed StrategyPackage experiments."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from backend.services.trading_core.errors import StrategyPackageValidationError

from .models import StrategyPackageManifest


class RuntimeVariantKind(str, Enum):
    STRATEGY_CONFIG = "strategy_config"
    EXECUTION_POLICY = "execution_policy"
    RISK_POLICY = "risk_policy"
    HMM_OVERLAY = "hmm_overlay"
    COMBINED = "combined"


class RuntimeVariantValidationStatus(str, Enum):
    DRAFT = "DRAFT"
    VALIDATION_PENDING = "VALIDATION_PENDING"
    VALIDATION_PASSED = "VALIDATION_PASSED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    RETIRED = "RETIRED"


class StrategyPackageRuntimeVariant(BaseModel):
    model_config = ConfigDict(extra="forbid")

    variant_id: str = Field(default_factory=lambda: f"rtv_{uuid4().hex}")
    package_id: str
    manifest_sha256: str
    locked_core_hash: str
    variant_name: str
    variant_kind: RuntimeVariantKind
    variant_config: dict[str, Any]
    variant_hash: str
    validation_status: RuntimeVariantValidationStatus = RuntimeVariantValidationStatus.DRAFT
    paper_candidate: bool = False
    validation_evidence: dict[str, Any] = Field(default_factory=dict)
    created_by: str = "aistock_api"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


_ALLOWED_RUNTIME_KEYS = {
    "strategy_config",
    "portfolio_policy",
    "execution_policy",
    "minute_execution_policy",
    "risk_policy",
    "hmm_overlay",
    "notes",
}
_FORBIDDEN_CORE_KEYS = {
    "alpha_components",
    "alpha_combination_policy",
    "factor_set",
    "model_asset",
    "manifest_json",
    "manifest_sha256",
    "package_id",
    "package_status",
}


def canonical_json_sha256(payload: Any) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def derive_locked_core_hash(manifest: StrategyPackageManifest) -> str:
    core = {
        "alpha_mode": manifest.alpha_mode.value,
        "alpha_components": [item.model_dump(mode="json") for item in manifest.alpha_components],
        "alpha_combination_policy": manifest.alpha_combination_policy.model_dump(mode="json"),
        "factor_set": [item.model_dump(mode="json") for item in manifest.factor_set],
        "model_asset": (
            [item.model_dump(mode="json") for item in manifest.model_asset]
            if isinstance(manifest.model_asset, list)
            else manifest.model_asset.model_dump(mode="json")
        ),
    }
    return canonical_json_sha256(core)


def validate_runtime_variant_config(variant_config: dict[str, Any]) -> None:
    if not variant_config:
        raise StrategyPackageValidationError("runtime variant config is required")
    keys = set(variant_config)
    forbidden = sorted(keys.intersection(_FORBIDDEN_CORE_KEYS))
    if forbidden:
        raise StrategyPackageValidationError(
            "runtime variant cannot modify frozen StrategyPackage core",
            context={"forbidden_keys": forbidden},
        )
    unknown = sorted(keys.difference(_ALLOWED_RUNTIME_KEYS))
    if unknown:
        raise StrategyPackageValidationError(
            "runtime variant contains unsupported runtime keys",
            context={"unsupported_keys": unknown, "allowed_keys": sorted(_ALLOWED_RUNTIME_KEYS)},
        )


def build_runtime_variant(
    manifest: StrategyPackageManifest,
    *,
    variant_name: str,
    variant_kind: RuntimeVariantKind,
    variant_config: dict[str, Any],
    validation_status: RuntimeVariantValidationStatus = RuntimeVariantValidationStatus.DRAFT,
    paper_candidate: bool = False,
    validation_evidence: dict[str, Any] | None = None,
    created_by: str = "aistock_api",
) -> StrategyPackageRuntimeVariant:
    if not manifest.manifest_sha256:
        raise StrategyPackageValidationError("runtime variant requires a frozen manifest_sha256")
    if not variant_name.strip():
        raise StrategyPackageValidationError("runtime variant name is required")
    if not created_by.strip():
        raise StrategyPackageValidationError("runtime variant created_by is required")
    validate_runtime_variant_config(variant_config)
    _validate_paper_candidate(validation_status=validation_status, paper_candidate=paper_candidate)
    locked_core_hash = derive_locked_core_hash(manifest)
    variant_hash = canonical_json_sha256(
        {
            "package_id": manifest.package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "locked_core_hash": locked_core_hash,
            "variant_kind": variant_kind.value,
            "variant_config": variant_config,
        }
    )
    return StrategyPackageRuntimeVariant(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        locked_core_hash=locked_core_hash,
        variant_name=variant_name.strip(),
        variant_kind=variant_kind,
        variant_config=variant_config,
        variant_hash=variant_hash,
        validation_status=validation_status,
        paper_candidate=paper_candidate,
        validation_evidence=validation_evidence or {},
        created_by=created_by.strip(),
    )


def ensure_runtime_variant_status(
    *,
    validation_status: RuntimeVariantValidationStatus,
    paper_candidate: bool,
    validation_evidence: dict[str, Any] | None = None,
) -> None:
    _validate_paper_candidate(validation_status=validation_status, paper_candidate=paper_candidate)
    if validation_status == RuntimeVariantValidationStatus.VALIDATION_PASSED and not validation_evidence:
        raise StrategyPackageValidationError("passed runtime variant requires validation evidence")


def _validate_paper_candidate(
    *,
    validation_status: RuntimeVariantValidationStatus,
    paper_candidate: bool,
) -> None:
    if paper_candidate and validation_status != RuntimeVariantValidationStatus.VALIDATION_PASSED:
        raise StrategyPackageValidationError("runtime variant must pass validation before becoming a paper candidate")
