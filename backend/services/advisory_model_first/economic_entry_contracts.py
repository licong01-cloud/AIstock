"""Contracts for economic entry labels, separate from opening-price coverage."""

from __future__ import annotations

from datetime import date
import math
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    transition_policy_from_payload,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256 = r"^[0-9a-f]{64}$"
ECONOMIC_FEATURE_NAMES = (
    "parent_combined_score", "parent_rank_pct", "leg_norm_score_gap", "ret_1", "ret_5",
    "atr14_close", "csi300_ret_5", "market_up_ratio", "query_gap_bps",
)


class EconomicEntryTrainingRequestV1(BaseModel):
    """One pre-registered candidate, no data-driven parameter or feature search."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["economic_entry_training_request_v1"] = "economic_entry_training_request_v1"
    hypothesis: Literal["actual_open_conditioned_entry_value_v1"] = "actual_open_conditioned_entry_value_v1"
    input_identity_sha256: str = Field(pattern=SHA256)
    feature_source_sha256: str = Field(pattern=SHA256)
    implementation_sha256: str = Field(pattern=SHA256)
    feature_names: tuple[str, ...] = ECONOMIC_FEATURE_NAMES
    train_start: date
    train_end: date
    validation_start: date
    validation_end: date
    test_start: date
    test_end: date
    label_cutoff: date
    gap_bin_width_bps: Literal[100] = 100
    minimum_bin_observations: Literal[30] = 30
    minimum_bin_days: Literal[5] = 5
    downside_budget_bps: float = Field(gt=0)
    minimum_expected_net_value_bps: Literal[0] = 0
    rule_minimum_gap_bps: Literal[-300] = -300
    rule_maximum_gap_bps: Literal[300] = 300
    model_trial_count: Literal[1] = 1
    resource_max_rows: int = Field(default=100000, ge=30)
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    evidence_level: Literal["HISTORICAL_REPLAY"] = "HISTORICAL_REPLAY"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_clock(self) -> EconomicEntryTrainingRequestV1:
        if not (
            self.train_start <= self.train_end < self.validation_start <= self.validation_end
            < self.test_start <= self.test_end <= self.label_cutoff
        ):
            raise ValueError("training request must have disjoint chronological windows")
        if self.feature_names != ECONOMIC_FEATURE_NAMES:
            raise ValueError("first economic candidate has one fixed feature order")
        return self

    @property
    def effective_parameters(self) -> dict[str, Any]:
        return {
            "learning_rate": 0.05, "max_depth": 3, "num_leaves": 7,
            "min_data_in_leaf": 30, "seed": 20261002, "num_threads": 2,
            "verbosity": -1, "deterministic": True, "force_col_wise": True,
            "num_boost_round": 200,
            "return_objective": "regression", "risk_objective": "quantile", "risk_alpha": 0.9,
        }

    @property
    def request_sha256(self) -> str:
        return canonical_json_sha256({**self.model_dump(mode="json"), "parameters": self.effective_parameters})


class EconomicEntryInputIdentityV1(BaseModel):
    """Caller-verified frozen sources; this does not create a native receipt."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False, str_strip_whitespace=True)

    schema_version: Literal["economic_entry_input_identity_v1"] = "economic_entry_input_identity_v1"
    dataset_manifest_sha256: str = Field(pattern=SHA256)
    request_id: str = Field(min_length=1)
    request_sha256: str = Field(pattern=SHA256)
    program_id: str = Field(min_length=1)
    binding_version_id: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256)
    selection_runtime_semantics_hash: str = Field(pattern=SHA256)
    universe_identity_sha256: str = Field(pattern=SHA256)
    candidate_roster_sha256: str = Field(pattern=SHA256)
    price_coordinate_sha256: str = Field(pattern=SHA256)
    price_source_sha256: str = Field(pattern=SHA256)
    reference_source_sha256: str = Field(pattern=SHA256)
    shadow_policy: dict[str, Any]
    shadow_policy_sha256: str = Field(pattern=SHA256)
    cost_policy: AdvisoryPolicyCostV1
    cost_policy_sha256: str = Field(pattern=SHA256)
    source_evidence: Literal["NATIVE_COMPLETE", "RECOVERED_LIMITED"]
    evidence_limitations: tuple[str, ...] = ()
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    evidence_level: Literal["HISTORICAL_REPLAY"] = "HISTORICAL_REPLAY"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_policy(self) -> EconomicEntryInputIdentityV1:
        if canonical_json_sha256(self.shadow_policy) != self.shadow_policy_sha256:
            raise ValueError("shadow policy hash mismatch")
        policy = transition_policy_from_payload(self.shadow_policy)
        if policy.target_count != 5 or policy.rank_enter_threshold != 5:
            raise ValueError("economic entry preserves the frozen Top5 slots")
        if self.cost_policy.policy_sha256 != self.cost_policy_sha256:
            raise ValueError("cost policy hash mismatch")
        if self.cost_policy.sell_cost_bps >= 10000:
            raise ValueError("sell cost must leave positive liquidation proceeds")
        if self.source_evidence == "RECOVERED_LIMITED" and not self.evidence_limitations:
            raise ValueError("recovered evidence must retain its limitations")
        if any(not value.strip() for value in self.evidence_limitations):
            raise ValueError("evidence limitations cannot be blank")
        return self

    @property
    def identity_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))

    def episode_identity(self) -> dict[str, str]:
        names = (
            "request_id", "request_sha256", "program_id", "binding_version_id",
            "package_id", "manifest_sha256", "selection_runtime_semantics_hash",
            "shadow_policy_sha256", "cost_policy_sha256",
        )
        return {name: getattr(self, name) for name in names}


class EconomicEntryLabelV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["economic_entry_label_v1"] = "economic_entry_label_v1"
    role: Literal["ENTRY_VALUE"] = "ENTRY_VALUE"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    evidence_level: Literal["HISTORICAL_REPLAY"] = "HISTORICAL_REPLAY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    deployable: Literal[False] = False
    input_identity_sha256: str = Field(pattern=SHA256)
    episode_label_id: str = Field(min_length=1)
    decision_date: date
    target_date: date
    instrument: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    selection_rank: int = Field(gt=0)
    original_label_status: str = Field(min_length=1)
    label_information_end: date
    status: Literal["AVAILABLE", "NOT_ENTERED", "CENSORED_RIGHT_BOUNDARY", "DATA_UNAVAILABLE"]
    reason_code: str | None = None
    decision_raw_close_cny: float | None = Field(default=None, gt=0)
    target_reference_raw_cny: float | None = Field(default=None, gt=0)
    actual_open_raw_cny: float | None = Field(default=None, gt=0)
    actual_gap_bps: float | None = None
    enter_net_value_bps: float | None = None
    skip_net_value_bps: Literal[0.0] | None = None
    entry_advantage_bps: float | None = None
    daily_mark_max_drawdown_bps: float | None = Field(default=None, ge=0, le=10000)
    risk_mark_basis: Literal["policy_coordinate_open_close_net_liquidation_v1"] = (
        "policy_coordinate_open_close_net_liquidation_v1"
    )
    suspension_carried_mark_count: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_values(self) -> EconomicEntryLabelV1:
        if not self.decision_date < self.target_date <= self.label_information_end:
            raise ValueError("label clock must satisfy D < T <= information_end")
        values = (
            self.decision_raw_close_cny, self.target_reference_raw_cny, self.actual_open_raw_cny,
            self.actual_gap_bps, self.enter_net_value_bps, self.skip_net_value_bps,
            self.entry_advantage_bps, self.daily_mark_max_drawdown_bps,
        )
        if self.status == "AVAILABLE":
            if any(value is None for value in values) or self.reason_code is not None:
                raise ValueError("available labels require complete prices/value/risk and no failure reason")
            if self.entry_advantage_bps != self.enter_net_value_bps:
                raise ValueError("entry advantage must be relative to the same zero-return cash slot")
            if self.original_label_status != "MATURED":
                raise ValueError("only genuinely matured source episodes can be available")
            gap = (self.actual_open_raw_cny / self.target_reference_raw_cny - 1) * 10000
            if not math.isclose(gap, self.actual_gap_bps, rel_tol=1e-9, abs_tol=1e-6):
                raise ValueError("actual opening gap differs from its raw-price reference")
        elif any(value is not None for value in values) or not self.reason_code:
            raise ValueError("unavailable or non-entered labels cannot manufacture numeric training targets")
        return self

    @property
    def label_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))
