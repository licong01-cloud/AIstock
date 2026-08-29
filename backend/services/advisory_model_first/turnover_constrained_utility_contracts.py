from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.feature_schema_v2 import FEATURE_SCHEMA_VERSION, feature_schema_hash
from backend.services.advisory_model_first.policy_utility_contracts import FrozenDataIdentityV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


TURNOVER_UTILITY_SEEDS = (20260813, 20260817, 20260823)
TURNOVER_UTILITY_LINEAGE = ("P0-D-v2", "P0-E-v2", "P0-F-v2", "P0-G-v1")
TURNOVER_SHADOW_PRICE_MULTIPLIERS = (0.0, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0)
EXPECTED_LABEL_STATUS_COUNTS = {
    "MATURED": 7716,
    "NOT_ENTERED_LIMIT_UP": 3,
    "CENSORED_RIGHT_BOUNDARY": 1,
}


class TurnoverConstrainedUtilityFamilySpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    family_id: Literal[
        "FAMILY_TURNOVER_CONSTRAINED_CORE",
        "FAMILY_TURNOVER_CONSTRAINED_CORE_HMM",
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
    def validate_family(self) -> "TurnoverConstrainedUtilityFamilySpecV1":
        if self.include_hmm != self.family_id.endswith("_HMM"):
            raise ValueError("family_id and include_hmm disagree")
        return self


class ExactTurnoverUtilityReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["P0D_V2_REFERENCE", "P0F_V2_REFERENCE"]
    bundle_root: str
    bundle_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    arm_id: Literal["ARM_P0D_V2_BINARY_PARITY", "ARM_P0F_V2_HUBER_UTILITY"]
    winner_family_id: str
    winner_seed: int
    winner_training_objective: str

    @model_validator(mode="after")
    def validate_role(self) -> "ExactTurnoverUtilityReferenceV1":
        expected = {
            "P0D_V2_REFERENCE": "ARM_P0D_V2_BINARY_PARITY",
            "P0F_V2_REFERENCE": "ARM_P0F_V2_HUBER_UTILITY",
        }[self.role]
        if self.arm_id != expected:
            raise ValueError("reference role and arm_id disagree")
        return self


class FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_turnover_constrained_utility_training_request_v1"] = (
        "frozen_advisory_turnover_constrained_utility_training_request_v1"
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
    family_specs: tuple[TurnoverConstrainedUtilityFamilySpecV1, ...]
    seed_roster: tuple[int, ...] = TURNOVER_UTILITY_SEEDS
    exact_p0d_reference: ExactTurnoverUtilityReferenceV1
    exact_p0f_reference: ExactTurnoverUtilityReferenceV1
    shadow_price_multipliers: tuple[float, ...] = TURNOVER_SHADOW_PRICE_MULTIPLIERS
    expected_candidate_row_count: Literal[7720] = 7720
    expected_matured_row_count: Literal[7716] = 7716
    expected_label_status_counts: dict[str, int] = Field(
        default_factory=lambda: dict(EXPECTED_LABEL_STATUS_COUNTS)
    )
    expected_decision_date_count: Literal[386] = 386
    expected_candidates_per_date: Literal[20] = 20
    expected_cpcv_path_count: Literal[28] = 28
    expected_trial_path_count: Literal[168] = 168
    turnover_action_count: Literal[2] = 2
    model_information_cutoff_trade_date: str
    latest_training_decision_trade_date: str
    latest_training_label_observation_trade_date: str
    experiment_lineage: tuple[str, ...] = TURNOVER_UTILITY_LINEAGE
    training_objective: Literal["HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1"] = (
        "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1"
    )
    primary_metric: Literal["mean_daily_net_excess_return_bps"] = "mean_daily_net_excess_return_bps"
    tie_break: Literal["family_id_seed_ascending"] = "family_id_seed_ascending"
    execution_mode: Literal["SEQUENTIAL_FAMILIES_MAX_8GB_V1"] = "SEQUENTIAL_FAMILIES_MAX_8GB_V1"
    resource_max_rss_bytes: Literal[8589934592] = 8589934592

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1":
        if tuple(item.family_id for item in self.family_specs) != (
            "FAMILY_TURNOVER_CONSTRAINED_CORE",
            "FAMILY_TURNOVER_CONSTRAINED_CORE_HMM",
        ):
            raise ValueError("turnover utility request must contain the approved family order")
        if self.seed_roster != TURNOVER_UTILITY_SEEDS:
            raise ValueError("turnover utility seed roster differs from the approved roster")
        if self.shadow_price_multipliers != TURNOVER_SHADOW_PRICE_MULTIPLIERS:
            raise ValueError("shadow price multiplier roster differs from the approved roster")
        if self.experiment_lineage != TURNOVER_UTILITY_LINEAGE:
            raise ValueError("turnover utility experiment lineage is invalid")
        if self.expected_label_status_counts != EXPECTED_LABEL_STATUS_COUNTS:
            raise ValueError("label status counts differ from the frozen P0-C identity")
        if self.exact_p0d_reference.role != "P0D_V2_REFERENCE":
            raise ValueError("exact P0-D reference role is invalid")
        if self.exact_p0f_reference.role != "P0F_V2_REFERENCE":
            raise ValueError("exact P0-F reference role is invalid")
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
            raise ValueError("turnover utility information cutoffs are inconsistent")
        datetime.fromisoformat(self.created_at)
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError("request_sha256 mismatch")
        if self.request_id != f"advturnutilityreq_{expected[:24]}":
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


def build_frozen_turnover_constrained_utility_request(
    **values: Any,
) -> FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1:
    values = dict(values)
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    for field in ("seed_roster", "experiment_lineage", "shadow_price_multipliers"):
        if field in values:
            values[field] = tuple(values[field])
    values["family_specs"] = tuple(
        item
        if isinstance(item, TurnoverConstrainedUtilityFamilySpecV1)
        else TurnoverConstrainedUtilityFamilySpecV1.model_validate(item)
        for item in values.get("family_specs", approved_turnover_constrained_utility_families())
    )
    for name in ("market_calendar_identity", "suspend_sidecar_identity"):
        if not isinstance(values[name], FrozenDataIdentityV1):
            values[name] = FrozenDataIdentityV1.model_validate(values[name])
    for name in ("exact_p0d_reference", "exact_p0f_reference"):
        if not isinstance(values[name], ExactTurnoverUtilityReferenceV1):
            values[name] = ExactTurnoverUtilityReferenceV1.model_validate(values[name])
    values.setdefault(
        "feature_schema_hash",
        feature_schema_hash(
            market_calendar_identity=values["market_calendar_identity"].model_dump(mode="json"),
            suspend_sidecar_identity=values["suspend_sidecar_identity"].model_dump(mode="json"),
        ),
    )
    seed = FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1.model_construct(
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1(
        request_id=f"advturnutilityreq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def approved_turnover_constrained_utility_families(
) -> tuple[TurnoverConstrainedUtilityFamilySpecV1, ...]:
    return (
        TurnoverConstrainedUtilityFamilySpecV1(
            family_id="FAMILY_TURNOVER_CONSTRAINED_CORE", include_hmm=False
        ),
        TurnoverConstrainedUtilityFamilySpecV1(
            family_id="FAMILY_TURNOVER_CONSTRAINED_CORE_HMM", include_hmm=True
        ),
    )
