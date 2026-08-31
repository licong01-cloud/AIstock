from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.feature_schema_v2 import (
    FEATURE_SCHEMA_VERSION,
    feature_schema_hash,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256

POLICY_UTILITY_SEEDS = (20260813, 20260817, 20260823)
POLICY_UTILITY_LINEAGE = ("P0-D-v2", "P0-E-v2", "P0-F-v2")
POLICY_UTILITY_ARM_IDS = (
    "ARM_P0D_V2_BINARY_PARITY",
    "ARM_P0E_V2_WEIGHTED_BINARY",
    "ARM_P0F_V2_HUBER_UTILITY",
)


class PolicyUtilityFamilySpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    family_id: Literal["FAMILY_POLICY_UTILITY_CORE", "FAMILY_POLICY_UTILITY_CORE_HMM"]
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
    def validate_family(self) -> "PolicyUtilityFamilySpecV1":
        if self.include_hmm != self.family_id.endswith("_HMM"):
            raise ValueError("family_id and include_hmm disagree")
        return self


class PolicyUtilityArmSpecV2(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    arm_id: Literal[
        "ARM_P0D_V2_BINARY_PARITY",
        "ARM_P0E_V2_WEIGHTED_BINARY",
        "ARM_P0F_V2_HUBER_UTILITY",
    ]
    training_objective: Literal[
        "BINARY_TAKE_SKIP_PARITY_V2",
        "OUTCOME_MAGNITUDE_WEIGHTED_BINARY_V2",
        "HUBER_CONTINUOUS_POLICY_NET_EXCESS_V2",
    ]
    prediction_column: Literal["take_probability", "predicted_policy_net_excess_return_bps"]
    outcome_weighted: bool = False

    @model_validator(mode="after")
    def validate_arm(self) -> "PolicyUtilityArmSpecV2":
        expected = {
            "ARM_P0D_V2_BINARY_PARITY": (
                "BINARY_TAKE_SKIP_PARITY_V2",
                "take_probability",
                False,
            ),
            "ARM_P0E_V2_WEIGHTED_BINARY": (
                "OUTCOME_MAGNITUDE_WEIGHTED_BINARY_V2",
                "take_probability",
                True,
            ),
            "ARM_P0F_V2_HUBER_UTILITY": (
                "HUBER_CONTINUOUS_POLICY_NET_EXCESS_V2",
                "predicted_policy_net_excess_return_bps",
                False,
            ),
        }[self.arm_id]
        if (self.training_objective, self.prediction_column, self.outcome_weighted) != expected:
            raise ValueError("policy utility arm semantics differ from the approved roster")
        return self


class FrozenDataIdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_kind: Literal["MARKET_CALENDAR", "SUSPEND_SIDECAR"]
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    cutoff_trade_date: str
    row_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_cutoff(self) -> "FrozenDataIdentityV1":
        date.fromisoformat(self.cutoff_trade_date)
        return self


class ExactMetaLabelReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["LEGACY_P0_D_LINEAGE", "LEGACY_P0_E_LINEAGE"]
    bundle_root: str
    bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FrozenAdvisoryPolicyUtilityTrainingRequestV2(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_policy_utility_training_request_v2"] = (
        "frozen_advisory_policy_utility_training_request_v2"
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
    family_specs: tuple[PolicyUtilityFamilySpecV1, ...]
    arm_specs: tuple[PolicyUtilityArmSpecV2, ...]
    seed_roster: tuple[int, ...] = POLICY_UTILITY_SEEDS
    legacy_p0d_reference: ExactMetaLabelReferenceV1
    legacy_p0e_reference: ExactMetaLabelReferenceV1
    expected_candidate_row_count: Literal[7720] = 7720
    expected_decision_date_count: Literal[386] = 386
    expected_candidates_per_date: Literal[20] = 20
    expected_cpcv_path_count: Literal[28] = 28
    expected_trial_path_count: Literal[504] = 504
    model_information_cutoff_trade_date: str
    latest_training_decision_trade_date: str
    latest_training_label_observation_trade_date: str
    experiment_lineage: tuple[str, ...] = POLICY_UTILITY_LINEAGE
    primary_metric: Literal["mean_daily_net_excess_return_bps"] = "mean_daily_net_excess_return_bps"
    tie_break: Literal["arm_id_family_id_seed_ascending"] = "arm_id_family_id_seed_ascending"
    execution_mode: Literal["SEQUENTIAL_ARMS_MAX_8GB_V1"] = "SEQUENTIAL_ARMS_MAX_8GB_V1"
    resource_max_rss_bytes: Literal[8589934592] = 8589934592

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryPolicyUtilityTrainingRequestV2":
        if tuple(item.family_id for item in self.family_specs) != (
            "FAMILY_POLICY_UTILITY_CORE",
            "FAMILY_POLICY_UTILITY_CORE_HMM",
        ):
            raise ValueError("policy utility request must contain the exact approved family order")
        if tuple(item.arm_id for item in self.arm_specs) != POLICY_UTILITY_ARM_IDS:
            raise ValueError("policy utility request must contain the exact approved arm roster")
        if self.seed_roster != POLICY_UTILITY_SEEDS:
            raise ValueError("policy utility seed roster differs from the approved roster")
        if self.experiment_lineage != POLICY_UTILITY_LINEAGE:
            raise ValueError("policy utility experiment lineage differs from P0-D-v2/P0-E-v2/P0-F-v2")
        if self.legacy_p0d_reference.role != "LEGACY_P0_D_LINEAGE":
            raise ValueError("legacy P0-D reference role is invalid")
        if self.legacy_p0e_reference.role != "LEGACY_P0_E_LINEAGE":
            raise ValueError("legacy P0-E reference role is invalid")
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
        decision = date.fromisoformat(self.latest_training_decision_trade_date)
        observation = date.fromisoformat(self.latest_training_label_observation_trade_date)
        information = date.fromisoformat(self.model_information_cutoff_trade_date)
        date.fromisoformat(self.factor_data_cutoff)
        if decision > observation or observation > information:
            raise ValueError("policy utility information cutoffs are inconsistent")
        datetime.fromisoformat(self.created_at)
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError("request_sha256 mismatch")
        if self.request_id != f"advutilityreq_{expected[:24]}":
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


def build_frozen_policy_utility_request(
    **values: Any,
) -> FrozenAdvisoryPolicyUtilityTrainingRequestV2:
    values = dict(values)
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    if "seed_roster" in values:
        values["seed_roster"] = tuple(values["seed_roster"])
    if "experiment_lineage" in values:
        values["experiment_lineage"] = tuple(values["experiment_lineage"])
    values["family_specs"] = tuple(
        item if isinstance(item, PolicyUtilityFamilySpecV1) else PolicyUtilityFamilySpecV1.model_validate(item)
        for item in values.get("family_specs", approved_policy_utility_families())
    )
    values["arm_specs"] = tuple(
        item if isinstance(item, PolicyUtilityArmSpecV2) else PolicyUtilityArmSpecV2.model_validate(item)
        for item in values.get("arm_specs", approved_policy_utility_arms())
    )
    for name in ("market_calendar_identity", "suspend_sidecar_identity"):
        if not isinstance(values[name], FrozenDataIdentityV1):
            values[name] = FrozenDataIdentityV1.model_validate(values[name])
    for name in ("legacy_p0d_reference", "legacy_p0e_reference"):
        if not isinstance(values[name], ExactMetaLabelReferenceV1):
            values[name] = ExactMetaLabelReferenceV1.model_validate(values[name])
    values.setdefault(
        "feature_schema_hash",
        feature_schema_hash(
            market_calendar_identity=values["market_calendar_identity"].model_dump(mode="json"),
            suspend_sidecar_identity=values["suspend_sidecar_identity"].model_dump(mode="json"),
        ),
    )
    seed = FrozenAdvisoryPolicyUtilityTrainingRequestV2.model_construct(
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryPolicyUtilityTrainingRequestV2(
        request_id=f"advutilityreq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def approved_policy_utility_families() -> tuple[PolicyUtilityFamilySpecV1, ...]:
    return (
        PolicyUtilityFamilySpecV1(family_id="FAMILY_POLICY_UTILITY_CORE", include_hmm=False),
        PolicyUtilityFamilySpecV1(family_id="FAMILY_POLICY_UTILITY_CORE_HMM", include_hmm=True),
    )


def approved_policy_utility_arms() -> tuple[PolicyUtilityArmSpecV2, ...]:
    return (
        PolicyUtilityArmSpecV2(
            arm_id="ARM_P0D_V2_BINARY_PARITY",
            training_objective="BINARY_TAKE_SKIP_PARITY_V2",
            prediction_column="take_probability",
        ),
        PolicyUtilityArmSpecV2(
            arm_id="ARM_P0E_V2_WEIGHTED_BINARY",
            training_objective="OUTCOME_MAGNITUDE_WEIGHTED_BINARY_V2",
            prediction_column="take_probability",
            outcome_weighted=True,
        ),
        PolicyUtilityArmSpecV2(
            arm_id="ARM_P0F_V2_HUBER_UTILITY",
            training_objective="HUBER_CONTINUOUS_POLICY_NET_EXCESS_V2",
            prediction_column="predicted_policy_net_excess_return_bps",
        ),
    )
