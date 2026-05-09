"""Validation run records for governed StrategyPackage retests."""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.trading_core.errors import StrategyPackageValidationError

from .models import StrategyPackageManifest


class PackageValidationType(str, Enum):
    ORIGINAL_FIXED_WEIGHT = "original_fixed_weight"
    ORIGINAL_RETRAIN = "original_retrain"
    LATEST_FIXED_WEIGHT = "latest_fixed_weight"
    LATEST_RETRAIN = "latest_retrain"
    WALK_FORWARD_ROLLING = "walk_forward_rolling"
    RUNTIME_VARIANT_BACKTEST = "runtime_variant_backtest"


class PackageValidationRetrainMode(str, Enum):
    NO_RETRAIN = "no_retrain"
    FIXED_SEED_RETRAIN = "fixed_seed_retrain"
    MULTI_SEED_RETRAIN = "multi_seed_retrain"
    ROLLING_RETRAIN = "rolling_retrain"


class PackageValidationStatus(str, Enum):
    REQUESTED = "REQUESTED"
    RUNNING = "RUNNING"
    PASSED = "PASSED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class PackageValidationReproducibility(str, Enum):
    UNKNOWN = "UNKNOWN"
    STRICT = "STRICT"
    STATISTICALLY_CLOSE = "STATISTICALLY_CLOSE"
    NON_DETERMINISTIC = "NON_DETERMINISTIC"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class StrategyPackageValidationRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    validation_run_id: str = Field(default_factory=lambda: f"vr_{uuid4().hex}")
    package_id: str
    manifest_sha256: str
    runtime_variant_id: str | None = None
    runtime_variant_hash: str | None = None
    validation_type: PackageValidationType
    retrain_mode: PackageValidationRetrainMode
    model_version_id: str | None = None
    seed_policy: str | None = None
    random_seed: int | None = None
    source_data_version: str | None = None
    target_data_version: str | None = None
    backtest_start: date | None = None
    backtest_end: date | None = None
    status: PackageValidationStatus = PackageValidationStatus.REQUESTED
    metrics_json: dict[str, Any] = Field(default_factory=dict)
    artifact_manifest_json: dict[str, Any] = Field(default_factory=dict)
    evidence_json: dict[str, Any] = Field(default_factory=dict)
    reproducibility_level: PackageValidationReproducibility = PackageValidationReproducibility.UNKNOWN
    created_by: str = "aistock_api"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None

    @model_validator(mode="after")
    def _validate_run_shape(self) -> "StrategyPackageValidationRun":
        if not self.package_id.strip():
            raise ValueError("validation run package_id is required")
        if not self.manifest_sha256.strip():
            raise ValueError("validation run manifest_sha256 is required")
        if not self.created_by.strip():
            raise ValueError("validation run created_by is required")
        if self.backtest_start and self.backtest_end and self.backtest_end < self.backtest_start:
            raise ValueError("validation run backtest_end must be >= backtest_start")
        return self


_NO_RETRAIN_TYPES = {
    PackageValidationType.ORIGINAL_FIXED_WEIGHT,
    PackageValidationType.LATEST_FIXED_WEIGHT,
    PackageValidationType.RUNTIME_VARIANT_BACKTEST,
}
_LATEST_TYPES = {
    PackageValidationType.LATEST_FIXED_WEIGHT,
    PackageValidationType.LATEST_RETRAIN,
    PackageValidationType.WALK_FORWARD_ROLLING,
}
_RETRAIN_TYPES = {
    PackageValidationType.ORIGINAL_RETRAIN,
    PackageValidationType.LATEST_RETRAIN,
    PackageValidationType.WALK_FORWARD_ROLLING,
}
_TERMINAL_STATUSES = {
    PackageValidationStatus.PASSED,
    PackageValidationStatus.FAILED,
    PackageValidationStatus.CANCELLED,
}


def build_package_validation_run(
    manifest: StrategyPackageManifest,
    *,
    validation_type: PackageValidationType,
    retrain_mode: PackageValidationRetrainMode,
    runtime_variant_id: str | None = None,
    runtime_variant_hash: str | None = None,
    model_version_id: str | None = None,
    seed_policy: str | None = None,
    random_seed: int | None = None,
    source_data_version: str | None = None,
    target_data_version: str | None = None,
    backtest_start: date | None = None,
    backtest_end: date | None = None,
    status: PackageValidationStatus = PackageValidationStatus.REQUESTED,
    metrics_json: dict[str, Any] | None = None,
    artifact_manifest_json: dict[str, Any] | None = None,
    evidence_json: dict[str, Any] | None = None,
    reproducibility_level: PackageValidationReproducibility = PackageValidationReproducibility.UNKNOWN,
    created_by: str = "aistock_api",
    completed_at: datetime | None = None,
) -> StrategyPackageValidationRun:
    if not manifest.manifest_sha256:
        raise StrategyPackageValidationError("validation run requires a frozen manifest_sha256")
    run = StrategyPackageValidationRun(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        runtime_variant_id=runtime_variant_id,
        runtime_variant_hash=runtime_variant_hash,
        validation_type=validation_type,
        retrain_mode=retrain_mode,
        model_version_id=model_version_id,
        seed_policy=seed_policy,
        random_seed=random_seed,
        source_data_version=source_data_version,
        target_data_version=target_data_version,
        backtest_start=backtest_start,
        backtest_end=backtest_end,
        status=status,
        metrics_json=metrics_json or {},
        artifact_manifest_json=artifact_manifest_json or {},
        evidence_json=evidence_json or {},
        reproducibility_level=reproducibility_level,
        created_by=created_by,
        completed_at=completed_at,
    )
    ensure_package_validation_run(run)
    return run


def ensure_package_validation_run(run: StrategyPackageValidationRun) -> None:
    if run.validation_type in _NO_RETRAIN_TYPES and run.retrain_mode != PackageValidationRetrainMode.NO_RETRAIN:
        raise StrategyPackageValidationError(
            "fixed-weight validation must use no_retrain",
            context={"validation_type": run.validation_type.value, "retrain_mode": run.retrain_mode.value},
        )
    if run.validation_type == PackageValidationType.RUNTIME_VARIANT_BACKTEST:
        if not run.runtime_variant_id or not run.runtime_variant_hash:
            raise StrategyPackageValidationError("runtime variant validation requires variant id and hash")
    elif run.runtime_variant_id or run.runtime_variant_hash:
        raise StrategyPackageValidationError("runtime variant fields are only allowed for runtime_variant_backtest")
    if run.validation_type in _LATEST_TYPES and not _non_empty(run.target_data_version):
        raise StrategyPackageValidationError(
            "latest-data validation requires target_data_version",
            context={"validation_type": run.validation_type.value},
        )
    if run.validation_type in _RETRAIN_TYPES:
        if run.retrain_mode == PackageValidationRetrainMode.NO_RETRAIN:
            raise StrategyPackageValidationError(
                "retrain validation requires a retrain mode",
                context={"validation_type": run.validation_type.value},
            )
        if not _non_empty(run.seed_policy):
            raise StrategyPackageValidationError(
                "retrain validation requires seed_policy",
                context={"validation_type": run.validation_type.value},
            )
    if run.validation_type == PackageValidationType.WALK_FORWARD_ROLLING:
        if run.retrain_mode != PackageValidationRetrainMode.ROLLING_RETRAIN:
            raise StrategyPackageValidationError("walk-forward validation requires rolling_retrain mode")
        windows = run.evidence_json.get("windows")
        if not isinstance(windows, list) or not windows:
            raise StrategyPackageValidationError("walk-forward validation requires non-empty window evidence")
    if run.status == PackageValidationStatus.PASSED and (not run.metrics_json or not run.artifact_manifest_json):
        raise StrategyPackageValidationError("passed validation run requires metrics and artifact manifest evidence")
    if run.status in _TERMINAL_STATUSES and run.completed_at is None:
        raise StrategyPackageValidationError("terminal validation run requires completed_at")


def _non_empty(value: str | None) -> bool:
    return bool(value and value.strip())
