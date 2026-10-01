"""Versioned entry-loss semantics and daily prediction identities, not positions."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.economic_entry_contracts import (
    ECONOMIC_FEATURE_NAMES, EconomicEntryLabelV1, SHA256,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


class FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class EntryLossLabelV2(FrozenContract):
    schema_version: Literal["entry_loss_label_v2"] = "entry_loss_label_v2"
    risk_metric: Literal["entry_net_max_loss_open_close_bps"] = "entry_net_max_loss_open_close_bps"
    original: EconomicEntryLabelV1
    original_label_sha256: str = Field(pattern=SHA256)
    status: Literal["AVAILABLE", "UNAVAILABLE", "NOT_ENTERED", "CENSORED_RIGHT_BOUNDARY"]
    reason_code: str | None = None
    entry_net_max_loss_bps: float | None = Field(default=None, ge=0, le=10000)
    episode_peak_to_trough_drawdown_bps: float | None = Field(default=None, ge=0, le=10000)
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_values(self):
        if self.original.label_sha256 != self.original_label_sha256:
            raise ValueError("v2 label does not bind the unchanged original label")
        values = (self.entry_net_max_loss_bps, self.episode_peak_to_trough_drawdown_bps)
        if self.status == "AVAILABLE":
            if self.original.status != "AVAILABLE" or any(value is None for value in values) or self.reason_code:
                raise ValueError("available v2 risk requires an available original and complete risk")
        elif any(value is not None for value in values) or not self.reason_code:
            raise ValueError("unknown risk cannot manufacture numeric targets")
        if self.status in {"NOT_ENTERED", "CENSORED_RIGHT_BOUNDARY"} and self.status != self.original.status:
            raise ValueError("v2 cannot change a non-entered or censored original status")
        return self


class EconomicModelScopeV2(FrozenContract):
    """Stable applicability scope. Historical semantic hashes are not native pools."""

    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256)
    selection_runtime_semantics_hash: str = Field(pattern=SHA256)
    feature_names: tuple[str, ...] = ECONOMIC_FEATURE_NAMES
    feature_schema_sha256: str = Field(pattern=SHA256)
    shadow_policy_sha256: str = Field(pattern=SHA256)
    cost_policy_sha256: str = Field(pattern=SHA256)
    coordinate_algorithm_sha256: str = Field(pattern=SHA256)
    universe_definition_sha256: str | None = Field(default=None, pattern=SHA256)
    universe_definition_evidence: Literal["NATIVE_VERIFIED", "UNPROVEN"] = "UNPROVEN"

    @model_validator(mode="after")
    def validate_definition(self):
        if self.feature_names != ECONOMIC_FEATURE_NAMES:
            raise ValueError("v2 first candidate has one frozen feature order")
        if (self.universe_definition_sha256 is not None) != (self.universe_definition_evidence == "NATIVE_VERIFIED"):
            raise ValueError("unproven universe definition must not carry a invented native hash")
        return self

    @property
    def scope_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))


class PredictionInputContextV2(FrozenContract):
    """Consumer verified D input identity; it is NOT the training source identity."""

    schema_version: Literal["economic_prediction_input_v2"] = "economic_prediction_input_v2"
    scope: EconomicModelScopeV2
    decision_date: date
    target_date: date
    feature_visible_through: date
    price_visible_through: date
    instrument: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    selection_rank: int = Field(gt=0)
    run_id: str = Field(min_length=1)
    list_id: str = Field(min_length=1)
    candidate_roster_sha256: str = Field(pattern=SHA256)
    feature_source_sha256: str = Field(pattern=SHA256)
    price_context_sha256: str = Field(pattern=SHA256)
    universe_membership_sha256: str | None = Field(default=None, pattern=SHA256)
    source_evidence: Literal["NATIVE_COMPLETE", "RECOVERED_LIMITED"]
    evidence_limitations: tuple[str, ...] = ()
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_clock_and_evidence(self):
        if not self.feature_visible_through <= self.decision_date == self.price_visible_through < self.target_date:
            raise ValueError("daily prediction requires D-frozen features and exactly D price context")
        if any(not value.strip() for value in self.evidence_limitations):
            raise ValueError("evidence limitations cannot be blank")
        if self.source_evidence == "RECOVERED_LIMITED" and not self.evidence_limitations:
            raise ValueError("recovered daily input must retain its limitations")
        if self.source_evidence == "NATIVE_COMPLETE" and self.universe_membership_sha256 is None:
            raise ValueError("native daily identity requires membership evidence")
        return self

    @property
    def identity_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))


class EntryRiskBudgetV2(FrozenContract):
    risk_metric: Literal["entry_net_max_loss_open_close_bps"] = "entry_net_max_loss_open_close_bps"
    quantile: Literal[0.9] = 0.9
    maximum_loss_bps: float = Field(gt=0, lt=10000)
    reference_use: Literal["FIXED_RESEARCH_STOP_REFERENCE", "EXPLICIT_BUSINESS_CONFIGURATION"]
    configuration_sha256: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def validate_research_reference(self):
        if self.reference_use == "FIXED_RESEARCH_STOP_REFERENCE" and self.maximum_loss_bps != 800:
            raise ValueError("first v2 research cannot search a different loss budget")
        return self


class EntryLossStudyPlanV2(FrozenContract):
    schema_version: Literal["entry_loss_study_plan_v2"] = "entry_loss_study_plan_v2"
    parent_plan_ref: EvidenceReferenceV1
    parent_prepared_manifest_ref: EvidenceReferenceV1
    parent_trained_manifest_ref: EvidenceReferenceV1
    implementation_sha256: str = Field(pattern=SHA256)
    parent_lineage: tuple[str, ...] = Field(min_length=1)
    hypothesis: Literal["entry_anchored_net_loss_not_peak_drawdown"] = "entry_anchored_net_loss_not_peak_drawdown"
    risk_metric: Literal["entry_net_max_loss_open_close_bps"] = "entry_net_max_loss_open_close_bps"
    risk_quantile: Literal[0.9] = 0.9
    risk_reference_bps: Literal[800] = 800
    reuse_rule: Literal["EXACT_PARENT_ELIGIBILITY_OR_NO_FIT"] = "EXACT_PARENT_ELIGIBILITY_OR_NO_FIT"
    intended_model_trial_count: Literal[1] = 1
    resource_max_market_rows: Literal[500000] = 500000
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False

    @property
    def plan_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))

    @property
    def experiment_id(self) -> str:
        return f"adventryloss_{self.plan_sha256[:24]}"
