from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.strategy_package.runtime_variant import canonical_json_sha256


META_LABEL_SEEDS = (20260813, 20260817, 20260823)


class MetaLabelFamilySpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    family_id: Literal["FAMILY_CORE", "FAMILY_CORE_HMM"]
    include_hmm: bool
    num_leaves: Literal[15] = 15
    learning_rate: Literal[0.03] = 0.03
    min_data_in_leaf: Literal[80] = 80
    feature_fraction: Literal[0.8] = 0.8
    bagging_fraction: Literal[0.8] = 0.8
    bagging_freq: Literal[1] = 1
    lambda_l1: Literal[0.1] = 0.1
    lambda_l2: Literal[1.0] = 1.0
    num_threads: Literal[4] = 4
    max_boost_rounds: Literal[600] = 600
    early_stopping_rounds: Literal[60] = 60

    @model_validator(mode="after")
    def validate_family(self) -> "MetaLabelFamilySpecV1":
        if self.include_hmm != (self.family_id == "FAMILY_CORE_HMM"):
            raise ValueError("family_id and include_hmm disagree")
        return self


class FrozenAdvisoryMetaLabelTrainingRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_meta_label_training_request_v2"] = (
        "frozen_advisory_meta_label_training_request_v2"
    )
    request_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    policy_dataset_bundle_root: str
    policy_dataset_bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_dataset_manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    program_id: str
    binding_version_id: str
    package_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    style_profile_id: str
    style_profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    shadow_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    cost_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    split_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    qlib_daily_root: str
    factor_data_root: str
    factor_data_cutoff: str
    suspend_data_root: str
    repository_root: str
    repository_root_windows: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str
    feature_schema_version: Literal["advisory_feature_schema_v1"] = "advisory_feature_schema_v1"
    feature_schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    family_specs: tuple[MetaLabelFamilySpecV1, ...]
    seed_roster: tuple[int, ...] = META_LABEL_SEEDS
    primary_metric: Literal["mean_daily_net_excess_return_bps"] = "mean_daily_net_excess_return_bps"
    tie_break: Literal["family_id_seed_ascending"] = "family_id_seed_ascending"
    probability_threshold: Literal[0.5] = 0.5
    resource_max_rss_bytes: int = Field(default=8 * 1024**3, gt=0)

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryMetaLabelTrainingRequestV1":
        family_ids = tuple(item.family_id for item in self.family_specs)
        if family_ids != ("FAMILY_CORE", "FAMILY_CORE_HMM"):
            raise ValueError("meta-label request must contain the exact approved family order")
        if self.seed_roster != META_LABEL_SEEDS:
            raise ValueError("meta-label seed roster differs from the approved roster")
        datetime.fromisoformat(self.factor_data_cutoff)
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError("request_sha256 mismatch")
        if self.request_id != f"advmetareq_{expected[:24]}":
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
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(target)


def build_frozen_meta_label_request(**values: Any) -> FrozenAdvisoryMetaLabelTrainingRequestV1:
    values = dict(values)
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    seed = FrozenAdvisoryMetaLabelTrainingRequestV1.model_construct(
        schema_version="frozen_advisory_meta_label_training_request_v2",
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryMetaLabelTrainingRequestV1(
        schema_version="frozen_advisory_meta_label_training_request_v2",
        request_id=f"advmetareq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def approved_meta_label_families() -> tuple[MetaLabelFamilySpecV1, ...]:
    return (
        MetaLabelFamilySpecV1(family_id="FAMILY_CORE", include_hmm=False),
        MetaLabelFamilySpecV1(family_id="FAMILY_CORE_HMM", include_hmm=True),
    )
