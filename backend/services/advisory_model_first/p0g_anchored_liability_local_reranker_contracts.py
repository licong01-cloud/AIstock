from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.feature_schema_v2 import (
    FEATURE_SCHEMA_VERSION,
    feature_schema_hash,
)
from backend.services.advisory_model_first.policy_utility_contracts import FrozenDataIdentityV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


P0L_SEEDS = (20260813, 20260817, 20260823)
P0L_GAIN_ROSTER = (12, 8, 4, 1)
P0L_LINEAGE = (
    "P0-D-v2",
    "P0-G-v1",
    "P0-H-v1",
    "P0-K-v1",
    "P0-L-v1",
)
EXPECTED_LABEL_STATUS_COUNTS = {
    "MATURED": 7716,
    "NOT_ENTERED_LIMIT_UP": 3,
    "CENSORED_RIGHT_BOUNDARY": 1,
}


class P0LFamilySpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    family_id: Literal[
        "FAMILY_P0G_ANCHORED_LOCAL_RERANK_CORE",
        "FAMILY_P0G_ANCHORED_LOCAL_RERANK_CORE_HMM",
    ]
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
    def validate_family(self) -> "P0LFamilySpecV1":
        if self.include_hmm != self.family_id.endswith("_HMM"):
            raise ValueError("family_id and include_hmm disagree")
        return self


class ExactP0DReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["P0D_V2_REFERENCE"] = "P0D_V2_REFERENCE"
    bundle_root: str
    bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    arm_id: Literal["ARM_P0D_V2_BINARY_PARITY"] = "ARM_P0D_V2_BINARY_PARITY"
    winner_family_id: str
    winner_seed: int
    winner_training_objective: str
    winner_boost_rounds: int = Field(ge=1)


class ExactP0GAnchorReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["P0G_V1_ANCHOR"] = "P0G_V1_ANCHOR"
    bundle_root: str
    bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    arm_id: Literal["ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY"] = (
        "ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY"
    )
    winner_family_id: Literal["FAMILY_TURNOVER_CONSTRAINED_CORE"] = (
        "FAMILY_TURNOVER_CONSTRAINED_CORE"
    )
    winner_seed: Literal[20260817] = 20260817
    winner_training_objective: Literal[
        "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1"
    ] = "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1"
    winner_boost_rounds: Literal[19] = 19


class P0LEvidenceReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["P0H_V1_EVIDENCE", "P0K_V1_EVIDENCE"]
    bundle_root: str
    bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_experiment_status: Literal["NEGATIVE_STOP_NOT_ADVANCED"] = (
        "NEGATIVE_STOP_NOT_ADVANCED"
    )
    expected_model_available: Literal[True] = True


class FrozenAdvisoryP0LTrainingRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[
        "frozen_advisory_p0g_anchored_liability_local_reranker_request_v1"
    ] = "frozen_advisory_p0g_anchored_liability_local_reranker_request_v1"
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
    market_calendar_identity: FrozenDataIdentityV1
    suspend_sidecar_identity: FrozenDataIdentityV1
    repository_root: str
    repository_root_windows: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str
    feature_schema_version: Literal["advisory_feature_schema_v2_suspension_aware"] = (
        FEATURE_SCHEMA_VERSION
    )
    feature_schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    family_specs: tuple[P0LFamilySpecV1, ...]
    seed_roster: tuple[int, ...] = P0L_SEEDS
    exact_p0d_reference: ExactP0DReferenceV1
    exact_p0g_anchor_reference: ExactP0GAnchorReferenceV1
    p0h_evidence_reference: P0LEvidenceReferenceV1
    p0k_evidence_reference: P0LEvidenceReferenceV1
    identity_control: Literal["NO_SWAP_CONTROL_V1"] = "NO_SWAP_CONTROL_V1"
    liability_rank_gain_roster: tuple[int, ...] = P0L_GAIN_ROSTER
    max_anchor_displacement: Literal[1] = 1
    max_adjacent_swaps_per_date: Literal[1] = 1
    expected_candidate_row_count: Literal[7720] = 7720
    expected_matured_row_count: Literal[7716] = 7716
    expected_label_status_counts: dict[str, int] = Field(
        default_factory=lambda: dict(EXPECTED_LABEL_STATUS_COUNTS)
    )
    expected_decision_date_count: Literal[386] = 386
    expected_constraint_decision_date_count: Literal[385] = 385
    expected_candidates_per_date: Literal[20] = 20
    expected_cpcv_block_count: Literal[8] = 8
    expected_outer_validation_block_count: Literal[2] = 2
    expected_outer_path_count: Literal[28] = 28
    expected_outer_trial_path_count: Literal[168] = 168
    inner_embargo_trading_days: Literal[20] = 20
    turnover_action_count: Literal[2] = 2
    target_count: Literal[5] = 5
    max_holding_trading_days: Literal[20] = 20
    liability_clip_min: Literal[0.02] = 0.02
    liability_clip_max: Literal[0.4] = 0.4
    model_information_cutoff_trade_date: str
    latest_training_decision_trade_date: str
    latest_training_label_observation_trade_date: str
    experiment_lineage: tuple[str, ...] = P0L_LINEAGE
    liability_training_objective: Literal[
        "HUBER_TURNOVER_LIABILITY_FRACTION_PER_DAY_V1"
    ] = "HUBER_TURNOVER_LIABILITY_FRACTION_PER_DAY_V1"
    anchor_score_kind: Literal["TURNOVER_CONSTRAINED_POLICY_UTILITY_BPS"] = (
        "TURNOVER_CONSTRAINED_POLICY_UTILITY_BPS"
    )
    model_role: Literal["OFFLINE_P0G_ANCHORED_LIABILITY_LOCAL_RERANKER_V1"] = (
        "OFFLINE_P0G_ANCHORED_LIABILITY_LOCAL_RERANKER_V1"
    )
    primary_metric: Literal["mean_daily_net_excess_return_bps"] = (
        "mean_daily_net_excess_return_bps"
    )
    tie_break: Literal["family_id_seed_ascending"] = "family_id_seed_ascending"
    execution_mode: Literal["SEQUENTIAL_P0G_ANCHOR_REUSE_MAX_8GB_V1"] = (
        "SEQUENTIAL_P0G_ANCHOR_REUSE_MAX_8GB_V1"
    )
    resource_max_rss_bytes: Literal[8589934592] = 8589934592

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryP0LTrainingRequestV1":
        if tuple(item.family_id for item in self.family_specs) != (
            "FAMILY_P0G_ANCHORED_LOCAL_RERANK_CORE",
            "FAMILY_P0G_ANCHORED_LOCAL_RERANK_CORE_HMM",
        ):
            raise ValueError("P0-L request has an invalid family order")
        if self.seed_roster != P0L_SEEDS or self.liability_rank_gain_roster != P0L_GAIN_ROSTER:
            raise ValueError("P0-L request roster differs from the approved design")
        if self.experiment_lineage != P0L_LINEAGE:
            raise ValueError("P0-L lineage is invalid")
        if self.expected_label_status_counts != EXPECTED_LABEL_STATUS_COUNTS:
            raise ValueError("P0-L label status counts differ from P0-C")
        if (self.p0h_evidence_reference.role, self.p0k_evidence_reference.role) != (
            "P0H_V1_EVIDENCE",
            "P0K_V1_EVIDENCE",
        ):
            raise ValueError("P0-H/P0-K evidence roles are invalid")
        if self.market_calendar_identity.identity_kind != "MARKET_CALENDAR":
            raise ValueError("market calendar identity kind is invalid")
        if self.suspend_sidecar_identity.identity_kind != "SUSPEND_SIDECAR":
            raise ValueError("suspend sidecar identity kind is invalid")
        if (
            self.market_calendar_identity.cutoff_trade_date != self.factor_data_cutoff
            or self.suspend_sidecar_identity.cutoff_trade_date != self.factor_data_cutoff
        ):
            raise ValueError("calendar/suspend/factor cutoffs must be identical")
        expected_schema_hash = feature_schema_hash(
            market_calendar_identity=self.market_calendar_identity.model_dump(mode="json"),
            suspend_sidecar_identity=self.suspend_sidecar_identity.model_dump(mode="json"),
        )
        if self.feature_schema_hash != expected_schema_hash:
            raise ValueError("feature_schema_hash does not match bound v2 identities")
        if self.liability_clip_min != 0.02 or self.liability_clip_max != 0.4:
            raise ValueError("P0-L liability clip bounds differ from frozen policy units")
        decision = date.fromisoformat(self.latest_training_decision_trade_date)
        observation = date.fromisoformat(self.latest_training_label_observation_trade_date)
        information = date.fromisoformat(self.model_information_cutoff_trade_date)
        date.fromisoformat(self.factor_data_cutoff)
        if decision > observation or observation > information:
            raise ValueError("P0-L information cutoffs are inconsistent")
        datetime.fromisoformat(self.created_at)
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError("request_sha256 mismatch")
        if self.request_id != f"advp0lreq_{expected[:24]}":
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


def build_frozen_p0l_request(**values: Any) -> FrozenAdvisoryP0LTrainingRequestV1:
    values = dict(values)
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    for field in ("seed_roster", "experiment_lineage", "liability_rank_gain_roster"):
        if field in values:
            values[field] = tuple(values[field])
    values["family_specs"] = tuple(
        item if isinstance(item, P0LFamilySpecV1) else P0LFamilySpecV1.model_validate(item)
        for item in values.get("family_specs", approved_p0l_families())
    )
    for name in ("market_calendar_identity", "suspend_sidecar_identity"):
        if not isinstance(values[name], FrozenDataIdentityV1):
            values[name] = FrozenDataIdentityV1.model_validate(values[name])
    reference_types = {
        "exact_p0d_reference": ExactP0DReferenceV1,
        "exact_p0g_anchor_reference": ExactP0GAnchorReferenceV1,
        "p0h_evidence_reference": P0LEvidenceReferenceV1,
        "p0k_evidence_reference": P0LEvidenceReferenceV1,
    }
    for name, model in reference_types.items():
        if not isinstance(values[name], model):
            values[name] = model.model_validate(values[name])
    values.setdefault(
        "feature_schema_hash",
        feature_schema_hash(
            market_calendar_identity=values["market_calendar_identity"].model_dump(mode="json"),
            suspend_sidecar_identity=values["suspend_sidecar_identity"].model_dump(mode="json"),
        ),
    )
    seed = FrozenAdvisoryP0LTrainingRequestV1.model_construct(
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryP0LTrainingRequestV1(
        request_id=f"advp0lreq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def approved_p0l_families() -> tuple[P0LFamilySpecV1, ...]:
    return (
        P0LFamilySpecV1(
            family_id="FAMILY_P0G_ANCHORED_LOCAL_RERANK_CORE",
            include_hmm=False,
        ),
        P0LFamilySpecV1(
            family_id="FAMILY_P0G_ANCHORED_LOCAL_RERANK_CORE_HMM",
            include_hmm=True,
        ),
    )
