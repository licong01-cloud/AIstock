from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.price_range_contracts import canonical_json_sha256

CALIBRATION_POLICY_VERSION = "advisory_price_range_calibration_policy_v1"
CALIBRATION_METHOD = "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION"
NOMINAL_COVERAGE = 0.8


class PriceRangeCalibrationArtifactV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(gt=0)
    row_count: int = Field(gt=0)
    columns: tuple[str, ...]


class FrozenAdvisoryPriceRangeCalibrationRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_price_range_calibration_request_v1"] = (
        "frozen_advisory_price_range_calibration_request_v1"
    )
    request_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    output_root: str
    parent_price_range_request_id: str
    parent_price_range_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_price_range_bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_price_range_manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    style_profile_id: str
    style_profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feature_schema_version: str
    feature_schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    label_policy_version: str
    split_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_bundle_root: str
    features_artifact: PriceRangeCalibrationArtifactV1
    price_range_labels_artifact: PriceRangeCalibrationArtifactV1
    calibration_policy_version: Literal["advisory_price_range_calibration_policy_v1"] = (
        CALIBRATION_POLICY_VERSION
    )
    calibration_method: Literal["CQR_CENTRAL_80_NONNEGATIVE_EXPANSION"] = CALIBRATION_METHOD
    nominal_coverage: Literal[0.8] = NOMINAL_COVERAGE
    repository_root: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    conda_environment: Literal["rdagent-gpu"] = "rdagent-gpu"
    resource_max_rss_bytes: int = Field(default=8 * 1024**3, gt=0, le=8 * 1024**3)

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryPriceRangeCalibrationRequestV1":
        if Path(self.parent_bundle_root).name != self.parent_price_range_bundle_id:
            raise ValueError("M5C parent bundle root differs from parent price-range identity")
        output_root = _explicit_path(self.output_root)
        for name, value in (
            ("parent_bundle_root", self.parent_bundle_root),
            ("features_artifact", self.features_artifact.path),
            ("price_range_labels_artifact", self.price_range_labels_artifact.path),
        ):
            path = _explicit_path(value)
            if not output_root.is_absolute() or not path.is_absolute():
                raise ValueError(f"M5C {name} must use an absolute explicit artifact root")
            try:
                path.relative_to(output_root)
            except ValueError as exc:
                raise ValueError(f"M5C {name} escapes the explicit artifact root") from exc
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected or self.request_id != f"advprcal_{expected[:24]}":
            raise ValueError("M5C calibration request identity mismatch")
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


def build_frozen_price_range_calibration_request(
    **values: Any,
) -> FrozenAdvisoryPriceRangeCalibrationRequestV1:
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    for name in ("features_artifact", "price_range_labels_artifact"):
        values[name] = PriceRangeCalibrationArtifactV1.model_validate(values[name])
    seed = FrozenAdvisoryPriceRangeCalibrationRequestV1.model_construct(
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryPriceRangeCalibrationRequestV1(
        request_id=f"advprcal_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def _explicit_path(value: str) -> Path | PurePosixPath:
    return PurePosixPath(value) if value.startswith("/") else Path(value)
