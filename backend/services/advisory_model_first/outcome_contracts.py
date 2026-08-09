from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

OUTCOME_HORIZONS = (1, 3, 5, 10, 20)
OUTCOME_QUANTILES = (0.1, 0.5, 0.9)


def canonical_json_sha256(payload: object) -> str:
    normalized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class OutcomeInputArtifactV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(gt=0)
    row_count: int = Field(gt=0)
    columns: tuple[str, ...]


class FrozenAdvisoryOutcomeTrainingRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_outcome_training_request_v1"] = (
        "frozen_advisory_outcome_training_request_v1"
    )
    request_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    parent_request_id: str
    parent_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_bundle_manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    style_profile_id: str
    style_profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feature_schema_version: str
    feature_schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    label_policy_version: Literal["advisory_outcome_label_policy_v1"] = (
        "advisory_outcome_label_policy_v1"
    )
    candidate_semantics_id: str
    horizons: tuple[int, ...] = OUTCOME_HORIZONS
    quantiles: tuple[float, ...] = OUTCOME_QUANTILES
    candidates_artifact: OutcomeInputArtifactV1
    features_artifact: OutcomeInputArtifactV1
    parent_test_predictions_artifact: OutcomeInputArtifactV1
    parent_training_request_path: str
    parent_feature_schema_path: str
    qlib_daily_root: str
    suspend_data_root: str
    repository_root: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str
    decision_date_start: str = "2024-07-04"
    decision_date_end: str = "2026-03-10"
    data_cutoff: str = "2026-06-30"
    trainer_seed: int = 20260809
    resource_max_rss_bytes: int = 8 * 1024**3

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryOutcomeTrainingRequestV1":
        if self.horizons != OUTCOME_HORIZONS:
            raise ValueError(f"horizons must equal {OUTCOME_HORIZONS}")
        if self.quantiles != OUTCOME_QUANTILES:
            raise ValueError(f"quantiles must equal {OUTCOME_QUANTILES}")
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError(f"request_sha256 mismatch: expected={expected} actual={self.request_sha256}")
        if self.request_id != f"advoutreq_{expected[:24]}":
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


def build_frozen_outcome_training_request(**values: object) -> FrozenAdvisoryOutcomeTrainingRequestV1:
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    seed = FrozenAdvisoryOutcomeTrainingRequestV1.model_construct(
        schema_version="frozen_advisory_outcome_training_request_v1",
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryOutcomeTrainingRequestV1(
        schema_version="frozen_advisory_outcome_training_request_v1",
        request_id=f"advoutreq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )
