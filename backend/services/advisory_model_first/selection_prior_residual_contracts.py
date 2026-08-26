from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.feature_schema_v2 import FEATURE_SCHEMA_VERSION, feature_schema_hash
from backend.services.advisory_model_first.policy_utility_contracts import FrozenDataIdentityV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SELECTION_PRIOR_RESIDUAL_SEEDS = (20260813, 20260817, 20260823)
SELECTION_PRIOR_RESIDUAL_LINEAGE = (
    "P0-D-v2",
    "P0-E-v2",
    "P0-F-v2",
    "P0-G-v1",
    "P0-H-v1",
    "P0-I-v1",
    "P0-J-v1",
)
SELECTION_PRIOR_RESIDUAL_SHADOW_PRICE_MULTIPLIERS = (0.0, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0)
SELECTION_PRIOR_RANK_COUNT = 20
SELECTION_PRIOR_RELIABILITY_DENOMINATOR_EPSILON = 1e-12
EXPECTED_LABEL_STATUS_COUNTS = {
    "MATURED": 7716,
    "NOT_ENTERED_LIMIT_UP": 3,
    "CENSORED_RIGHT_BOUNDARY": 1,
}


class SelectionPriorResidualFamilySpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    family_id: Literal["FAMILY_SELECTION_PRIOR_RESIDUAL_CORE", "FAMILY_SELECTION_PRIOR_RESIDUAL_CORE_HMM"]
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
    def validate_family(self) -> "SelectionPriorResidualFamilySpecV1":
        if self.include_hmm != self.family_id.endswith("_HMM"):
            raise ValueError("family_id and include_hmm disagree")
        return self


class ExactSelectionPriorResidualReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal[
        "P0D_V2_REFERENCE",
        "P0F_V2_REFERENCE",
        "P0G_V1_REFERENCE",
        "P0H_V1_REFERENCE",
    ]
    bundle_root: str
    bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    arm_id: Literal[
        "ARM_P0D_V2_BINARY_PARITY",
        "ARM_P0F_V2_HUBER_UTILITY",
        "ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY",
        "ARM_P0H_V1_DUAL_HEAD_OUTPUT_CONSTRAINED_UTILITY",
    ]
    winner_family_id: str
    winner_seed: int
    winner_training_objective: str
    winner_boost_rounds: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_role(self) -> "ExactSelectionPriorResidualReferenceV1":
        expected = {
            "P0D_V2_REFERENCE": "ARM_P0D_V2_BINARY_PARITY",
            "P0F_V2_REFERENCE": "ARM_P0F_V2_HUBER_UTILITY",
            "P0G_V1_REFERENCE": "ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY",
            "P0H_V1_REFERENCE": "ARM_P0H_V1_DUAL_HEAD_OUTPUT_CONSTRAINED_UTILITY",
        }[self.role]
        if self.arm_id != expected:
            raise ValueError("reference role and arm_id disagree")
        return self


class P0ISelectionPriorResidualEvidenceReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["P0I_V1_EVIDENCE"] = "P0I_V1_EVIDENCE"
    bundle_root: str
    bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    arm_id: Literal["ARM_P0I_V1_GROUPED_RANK_OUTPUT_CONSTRAINED_UTILITY"] = (
        "ARM_P0I_V1_GROUPED_RANK_OUTPUT_CONSTRAINED_UTILITY"
    )
    experiment_status: Literal["NEGATIVE_STOP_INCOMPLETE_CPCV"] = (
        "NEGATIVE_STOP_INCOMPLETE_CPCV"
    )
    model_available: Literal[False] = False


class FrozenAdvisorySelectionPriorResidualTrainingRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_selection_prior_residual_training_request_v1"] = (
        "frozen_advisory_selection_prior_residual_training_request_v1"
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
    market_calendar_identity: FrozenDataIdentityV1
    suspend_sidecar_identity: FrozenDataIdentityV1
    repository_root: str
    repository_root_windows: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str
    feature_schema_version: Literal["advisory_feature_schema_v2_suspension_aware"] = FEATURE_SCHEMA_VERSION
    feature_schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    family_specs: tuple[SelectionPriorResidualFamilySpecV1, ...]
    seed_roster: tuple[int, ...] = SELECTION_PRIOR_RESIDUAL_SEEDS
    exact_p0d_reference: ExactSelectionPriorResidualReferenceV1
    exact_p0f_reference: ExactSelectionPriorResidualReferenceV1
    exact_p0g_reference: ExactSelectionPriorResidualReferenceV1
    exact_p0h_reference: ExactSelectionPriorResidualReferenceV1
    p0i_evidence_reference: P0ISelectionPriorResidualEvidenceReferenceV1
    shadow_price_multipliers: tuple[float, ...] = SELECTION_PRIOR_RESIDUAL_SHADOW_PRICE_MULTIPLIERS
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
    experiment_lineage: tuple[str, ...] = SELECTION_PRIOR_RESIDUAL_LINEAGE
    selection_prior_method: Literal["RANK_MEDIAN_COUNT_WEIGHTED_DECREASING_ISOTONIC_V1"] = (
        "RANK_MEDIAN_COUNT_WEIGHTED_DECREASING_ISOTONIC_V1"
    )
    selection_prior_rank_count: Literal[20] = SELECTION_PRIOR_RANK_COUNT
    reliability_method: Literal["ZERO_INTERCEPT_OOF_CLIPPED_0_1_V1"] = (
        "ZERO_INTERCEPT_OOF_CLIPPED_0_1_V1"
    )
    reliability_denominator_epsilon: Literal[1e-12] = (
        SELECTION_PRIOR_RELIABILITY_DENOMINATOR_EPSILON
    )
    return_training_objective: Literal["HUBER_SELECTION_RANK_RESIDUAL_BPS_V1"] = (
        "HUBER_SELECTION_RANK_RESIDUAL_BPS_V1"
    )
    liability_training_objective: Literal["HUBER_TURNOVER_LIABILITY_FRACTION_PER_DAY_V1"] = (
        "HUBER_TURNOVER_LIABILITY_FRACTION_PER_DAY_V1"
    )
    primary_metric: Literal["mean_daily_net_excess_return_bps"] = "mean_daily_net_excess_return_bps"
    tie_break: Literal["family_id_seed_ascending"] = "family_id_seed_ascending"
    execution_mode: Literal["SEQUENTIAL_SELECTION_PRIOR_RESIDUAL_MAX_8GB_V1"] = "SEQUENTIAL_SELECTION_PRIOR_RESIDUAL_MAX_8GB_V1"
    resource_max_rss_bytes: Literal[8589934592] = 8589934592

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisorySelectionPriorResidualTrainingRequestV1":
        if tuple(item.family_id for item in self.family_specs) != (
            "FAMILY_SELECTION_PRIOR_RESIDUAL_CORE",
            "FAMILY_SELECTION_PRIOR_RESIDUAL_CORE_HMM",
        ):
            raise ValueError("selection-prior-residual request must contain the approved family order")
        if self.seed_roster != SELECTION_PRIOR_RESIDUAL_SEEDS:
            raise ValueError("selection-prior-residual seed roster differs from the approved roster")
        if self.shadow_price_multipliers != SELECTION_PRIOR_RESIDUAL_SHADOW_PRICE_MULTIPLIERS:
            raise ValueError("selection-prior-residual shadow-price multiplier roster differs from approved roster")
        if self.experiment_lineage != SELECTION_PRIOR_RESIDUAL_LINEAGE:
            raise ValueError("selection-prior-residual experiment lineage is invalid")
        if self.expected_label_status_counts != EXPECTED_LABEL_STATUS_COUNTS:
            raise ValueError("label status counts differ from the frozen P0-C identity")
        if (
            self.exact_p0d_reference.role != "P0D_V2_REFERENCE"
            or self.exact_p0f_reference.role != "P0F_V2_REFERENCE"
            or self.exact_p0g_reference.role != "P0G_V1_REFERENCE"
            or self.exact_p0h_reference.role != "P0H_V1_REFERENCE"
            or self.p0i_evidence_reference.role != "P0I_V1_EVIDENCE"
        ):
            raise ValueError("selection-prior-residual exact reference roles are invalid")
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
        expected_min = self.turnover_action_count / (
            self.target_count * self.max_holding_trading_days
        )
        expected_max = self.turnover_action_count / self.target_count
        if self.liability_clip_min != expected_min or self.liability_clip_max != expected_max:
            raise ValueError("liability clip bounds differ from the frozen policy units")
        decision = date.fromisoformat(self.latest_training_decision_trade_date)
        observation = date.fromisoformat(self.latest_training_label_observation_trade_date)
        information = date.fromisoformat(self.model_information_cutoff_trade_date)
        date.fromisoformat(self.factor_data_cutoff)
        if decision > observation or observation > information:
            raise ValueError("selection-prior-residual information cutoffs are inconsistent")
        datetime.fromisoformat(self.created_at)
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError("request_sha256 mismatch")
        if self.request_id != f"advselpriorresreq_{expected[:24]}":
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


def build_frozen_selection_prior_residual_request(**values: Any) -> FrozenAdvisorySelectionPriorResidualTrainingRequestV1:
    values = dict(values)
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    for field in ("seed_roster", "experiment_lineage", "shadow_price_multipliers"):
        if field in values:
            values[field] = tuple(values[field])
    values["family_specs"] = tuple(
        item if isinstance(item, SelectionPriorResidualFamilySpecV1) else SelectionPriorResidualFamilySpecV1.model_validate(item)
        for item in values.get("family_specs", approved_selection_prior_residual_families())
    )
    for name in ("market_calendar_identity", "suspend_sidecar_identity"):
        if not isinstance(values[name], FrozenDataIdentityV1):
            values[name] = FrozenDataIdentityV1.model_validate(values[name])
    for name in (
        "exact_p0d_reference",
        "exact_p0f_reference",
        "exact_p0g_reference",
        "exact_p0h_reference",
    ):
        if not isinstance(values[name], ExactSelectionPriorResidualReferenceV1):
            values[name] = ExactSelectionPriorResidualReferenceV1.model_validate(values[name])
    if not isinstance(
        values["p0i_evidence_reference"],
        P0ISelectionPriorResidualEvidenceReferenceV1,
    ):
        values["p0i_evidence_reference"] = (
            P0ISelectionPriorResidualEvidenceReferenceV1.model_validate(
                values["p0i_evidence_reference"]
            )
        )
    values.setdefault(
        "feature_schema_hash",
        feature_schema_hash(
            market_calendar_identity=values["market_calendar_identity"].model_dump(mode="json"),
            suspend_sidecar_identity=values["suspend_sidecar_identity"].model_dump(mode="json"),
        ),
    )
    seed = FrozenAdvisorySelectionPriorResidualTrainingRequestV1.model_construct(
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisorySelectionPriorResidualTrainingRequestV1(
        request_id=f"advselpriorresreq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def approved_selection_prior_residual_families() -> tuple[SelectionPriorResidualFamilySpecV1, ...]:
    return (
        SelectionPriorResidualFamilySpecV1(family_id="FAMILY_SELECTION_PRIOR_RESIDUAL_CORE", include_hmm=False),
        SelectionPriorResidualFamilySpecV1(family_id="FAMILY_SELECTION_PRIOR_RESIDUAL_CORE_HMM", include_hmm=True),
    )
