from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.strategy_package.runtime_variant import canonical_json_sha256


class PredictionArtifactDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str
    run_key: str
    artifact_uri: str
    artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(gt=0)
    row_count: int = Field(gt=0)
    date_start: str
    date_end: str


class FrozenAdvisoryTrainingRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_training_request_v1"] = "frozen_advisory_training_request_v1"
    request_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    package_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_asset_closure_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    program_id: str
    binding_version_id: str
    style_profile_id: str
    style_profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    effective_package_oos_cutoff: str
    selection_runtime_semantics_id: str
    selection_runtime_semantics_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    selection_runtime_semantics: dict[str, Any]
    representative_seed_run_ids: dict[str, str]
    representative_model_asset_sha256: dict[str, str]
    full_seed_roster: dict[str, tuple[str, ...]]
    prediction_artifacts: dict[str, PredictionArtifactDescriptor]
    terminal_weights: dict[str, float]
    historical_weight_rows: tuple[dict[str, Any], ...] = ()
    combined_reference_path: str
    combined_reference_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    combined_reference_diagnostic_only: Literal[True] = True
    qlib_daily_root: str
    factor_data_root: str
    suspend_data_root: str
    prediction_store_root: str
    repository_root: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str
    decision_date_start: str = "2024-07-04"
    decision_date_end: str = "2026-03-10"
    data_cutoff: str = "2026-06-30"
    hmm_continuation_cutoff: str = "2026-03-10"
    feature_schema_version: str = "advisory_feature_schema_v1"
    label_policy_version: str = "advisory_label_policy_v1"
    decision_clock_version: str = "advisory_previous_close_target_next_trade_v1"
    resource_max_rss_bytes: int = 8 * 1024**3

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryTrainingRequestV1":
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError(f"request_sha256 mismatch: expected={expected} actual={self.request_sha256}")
        if self.request_id != f"advmreq_{expected[:24]}":
            raise ValueError("request_id does not match request_sha256")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at", "output_root"},
        )

    def write_json(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        tmp.replace(target)


def build_frozen_training_request(**values: Any) -> FrozenAdvisoryTrainingRequestV1:
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    seed = FrozenAdvisoryTrainingRequestV1.model_construct(
        schema_version="frozen_advisory_training_request_v1",
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryTrainingRequestV1(
        schema_version="frozen_advisory_training_request_v1",
        request_id=f"advmreq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )
