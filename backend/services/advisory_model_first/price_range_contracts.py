from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PRICE_RANGE_QUANTILES = (0.1, 0.5, 0.9)
PRICE_RANGE_MODEL_NAMES = (
    "entry_executable_probability",
    "entry_gap_q10",
    "entry_gap_q50",
    "entry_gap_q90",
)
ENTRY_GAP_CONDITION = "ENTRY_EXECUTABLE"


def canonical_json_sha256(payload: object) -> str:
    normalized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class PriceRangeInputArtifactV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(gt=0)
    row_count: int = Field(gt=0)
    columns: tuple[str, ...]


class FrozenAdvisoryPriceRangeTrainingRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_price_range_training_request_v1"] = (
        "frozen_advisory_price_range_training_request_v1"
    )
    request_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    parent_request_id: str
    parent_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_bundle_manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome_request_id: str
    outcome_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome_bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome_bundle_manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    style_profile_id: str
    style_profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feature_schema_version: str
    feature_schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    label_policy_version: Literal["advisory_price_range_label_policy_v1"] = (
        "advisory_price_range_label_policy_v1"
    )
    candidate_semantics_id: str
    entry_gap_condition: Literal["ENTRY_EXECUTABLE"] = ENTRY_GAP_CONDITION
    quantiles: tuple[float, ...] = PRICE_RANGE_QUANTILES
    candidates_artifact: PriceRangeInputArtifactV1
    features_artifact: PriceRangeInputArtifactV1
    parent_training_request_path: str
    parent_feature_schema_path: str
    outcome_training_request_path: str
    outcome_split_path: str
    qlib_daily_root: str
    suspend_data_root: str
    repository_root: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str
    decision_date_start: str = "2024-07-04"
    decision_date_end: str = "2026-03-10"
    data_cutoff: str = "2026-06-30"
    trainer_seed: int = 20260810
    resource_max_rss_bytes: int = Field(default=8 * 1024**3, gt=0, le=8 * 1024**3)

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryPriceRangeTrainingRequestV1":
        if self.quantiles != PRICE_RANGE_QUANTILES:
            raise ValueError(f"quantiles must equal {PRICE_RANGE_QUANTILES}")
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError(
                f"request_sha256 mismatch: expected={expected} actual={self.request_sha256}"
            )
        if self.request_id != f"advprreq_{expected[:24]}":
            raise ValueError("request_id does not match request_sha256")
        return self

    def functional_payload(self) -> dict[str, object]:
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


def build_frozen_price_range_training_request(
    **values: object,
) -> FrozenAdvisoryPriceRangeTrainingRequestV1:
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    seed = FrozenAdvisoryPriceRangeTrainingRequestV1.model_construct(
        schema_version="frozen_advisory_price_range_training_request_v1",
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryPriceRangeTrainingRequestV1(
        schema_version="frozen_advisory_price_range_training_request_v1",
        request_id=f"advprreq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )
