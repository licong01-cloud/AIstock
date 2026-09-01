from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.outcome_contracts import (
    OUTCOME_HORIZONS,
    canonical_json_sha256,
)

CALIBRATION_POLICY_VERSION = "advisory_outcome_calibration_policy_v1"
BINARY_CALIBRATION_METHOD = "PLATT_RAW_MARGIN"
RETURN_INTERVAL_METHOD = "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION"
PATH_UPPER_METHOD = "CONFORMAL_UPPER_90_NONNEGATIVE_EXPANSION"


class OutcomeCalibrationArtifactV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(gt=0)
    row_count: int = Field(gt=0)
    columns: tuple[str, ...]


class FrozenAdvisoryOutcomeCalibrationRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_outcome_calibration_request_v1"] = (
        "frozen_advisory_outcome_calibration_request_v1"
    )
    request_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    output_root: str
    parent_outcome_request_id: str
    parent_outcome_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_outcome_bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_outcome_manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    style_profile_id: str
    style_profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feature_schema_version: str
    feature_schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    label_policy_version: str
    split_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_bundle_root: str
    features_artifact: OutcomeCalibrationArtifactV1
    outcome_labels_artifact: OutcomeCalibrationArtifactV1
    calibration_policy_version: Literal["advisory_outcome_calibration_policy_v1"] = (
        CALIBRATION_POLICY_VERSION
    )
    binary_method: Literal["PLATT_RAW_MARGIN"] = BINARY_CALIBRATION_METHOD
    return_interval_method: Literal["CQR_CENTRAL_80_NONNEGATIVE_EXPANSION"] = (
        RETURN_INTERVAL_METHOD
    )
    path_upper_method: Literal["CONFORMAL_UPPER_90_NONNEGATIVE_EXPANSION"] = (
        PATH_UPPER_METHOD
    )
    ece_bin_count: Literal[10] = 10
    repository_root: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    conda_environment: Literal["rdagent-gpu"] = "rdagent-gpu"
    resource_max_rss_bytes: int = Field(default=8 * 1024**3, gt=0, le=8 * 1024**3)

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryOutcomeCalibrationRequestV1":
        bundle_root = Path(self.parent_bundle_root)
        if bundle_root.name != self.parent_outcome_bundle_id:
            raise ValueError("M5B parent bundle root differs from parent outcome bundle identity")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected or self.request_id != f"advoutcal_{expected[:24]}":
            raise ValueError("M5B calibration request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at", "output_root"},
        )

    def write_json(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(target)


def build_frozen_outcome_calibration_request(
    **values: Any,
) -> FrozenAdvisoryOutcomeCalibrationRequestV1:
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    values["features_artifact"] = OutcomeCalibrationArtifactV1.model_validate(
        values["features_artifact"]
    )
    values["outcome_labels_artifact"] = OutcomeCalibrationArtifactV1.model_validate(
        values["outcome_labels_artifact"]
    )
    seed = FrozenAdvisoryOutcomeCalibrationRequestV1.model_construct(
        schema_version="frozen_advisory_outcome_calibration_request_v1",
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryOutcomeCalibrationRequestV1(
        schema_version="frozen_advisory_outcome_calibration_request_v1",
        request_id=f"advoutcal_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def expected_binary_calibration_heads() -> tuple[str, ...]:
    return tuple(
        f"{family}_h{horizon}"
        for horizon in OUTCOME_HORIZONS
        for family in ("positive_excess", "signal_survival")
    )
