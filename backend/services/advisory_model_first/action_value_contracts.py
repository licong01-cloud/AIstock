from __future__ import annotations

import math
from datetime import date
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    ObjectiveContract,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"


class AdvisoryActionRole(str, Enum):
    ENTRY_GUARD = "ENTRY_GUARD"
    EXIT = "EXIT"


class AdvisoryEvidenceLevel(str, Enum):
    HISTORICAL_REPLAY = "HISTORICAL_REPLAY"
    SEALED_HOLDOUT_CONFIRMATION = "SEALED_HOLDOUT_CONFIRMATION"
    PROSPECTIVE_OOS = "PROSPECTIVE_OOS"


class AdvisoryActionValueStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    NON_NUMERIC_ADVICE_ONLY = "NON_NUMERIC_ADVICE_ONLY"
    WAITING = "WAITING"
    BASELINE_UNAVAILABLE = "BASELINE_UNAVAILABLE"
    DATA_UNAVAILABLE = "DATA_UNAVAILABLE"
    CENSORED_RIGHT_BOUNDARY = "CENSORED_RIGHT_BOUNDARY"


class AdvisoryInterventionEvidenceClass(str, Enum):
    CONFIRMATORY_ELIGIBLE = "CONFIRMATORY_ELIGIBLE"
    EXPLORATORY_ONLY = "EXPLORATORY_ONLY"


class AdvisoryIncrementalValueLabelV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_incremental_value_label_v1"] = "advisory_incremental_value_label_v1"
    label_id: str = Field(pattern=r"^advincr_[0-9a-f]{24}$")
    label_sha256: str = Field(pattern=SHA256_PATTERN)
    role: AdvisoryActionRole
    objective_contract: Literal[ObjectiveContract.RISK_MANAGED_ADVISORY] = ObjectiveContract.RISK_MANAGED_ADVISORY
    decision_use: DecisionUse
    evidence_level: AdvisoryEvidenceLevel
    sealed_holdout_accessed: bool
    deployable: Literal[False] = False
    decision_date: date
    target_action_date: date | None = None
    effective_action_date: date | None = None
    instrument: str = Field(min_length=1)
    episode_id: str = Field(min_length=1)
    baseline_action: str = Field(min_length=1)
    intervention_action: str = Field(min_length=1)
    status: AdvisoryActionValueStatus
    baseline_net_value_bps: float | None = None
    action_net_value_bps: float | None = None
    incremental_net_value_bps: float | None = None
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    intervention_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    shadow_simulator_sha256: str = Field(pattern=SHA256_PATTERN)
    information_start: date
    information_end: date
    reason_code: str | None = None

    @model_validator(mode="after")
    def validate_contract(self) -> "AdvisoryIncrementalValueLabelV1":
        instrument = self.instrument.strip().upper()
        object.__setattr__(self, "instrument", instrument)
        if self.information_end < self.information_start:
            raise ValueError("information_end cannot precede information_start")
        if self.target_action_date is not None and self.target_action_date <= self.decision_date:
            raise ValueError("target_action_date must follow decision_date")
        if self.effective_action_date is not None and self.target_action_date is not None:
            if self.effective_action_date < self.target_action_date:
                raise ValueError("effective_action_date cannot precede target_action_date")
        if self.evidence_level == AdvisoryEvidenceLevel.HISTORICAL_REPLAY:
            if self.sealed_holdout_accessed:
                raise ValueError("historical replay cannot access sealed holdout")
            if self.decision_use == DecisionUse.ACTIVATION_EVIDENCE:
                raise ValueError("historical replay cannot be activation evidence")
        elif self.evidence_level == AdvisoryEvidenceLevel.SEALED_HOLDOUT_CONFIRMATION:
            if not self.sealed_holdout_accessed:
                raise ValueError("sealed holdout confirmation must declare holdout access")
        elif self.sealed_holdout_accessed:
            raise ValueError("prospective OOS cannot be marked as sealed holdout access")

        numeric = (
            self.baseline_net_value_bps,
            self.action_net_value_bps,
            self.incremental_net_value_bps,
        )
        if self.status == AdvisoryActionValueStatus.AVAILABLE:
            if any(value is None for value in numeric):
                raise ValueError("available incremental label requires all numeric values")
            baseline = float(self.baseline_net_value_bps)
            action = float(self.action_net_value_bps)
            incremental = float(self.incremental_net_value_bps)
            expected = action - baseline
            if not math.isclose(incremental, expected, rel_tol=1e-12, abs_tol=1e-9):
                raise ValueError("incremental_net_value_bps differs from action minus baseline")
        elif any(value is not None for value in numeric):
            raise ValueError("non-available incremental label cannot carry numeric values")

        digest = canonical_json_sha256(self.functional_payload())
        if self.label_sha256 != digest or self.label_id != f"advincr_{digest[:24]}":
            raise ValueError("incremental label identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"label_id", "label_sha256"})


class AdvisoryActionInterventionSupportV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_action_intervention_support_v1"] = "advisory_action_intervention_support_v1"
    support_sha256: str = Field(pattern=SHA256_PATTERN)
    role: AdvisoryActionRole
    intervention_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    total_decision_count: int = Field(gt=0)
    intervention_count: int = Field(ge=0)
    decision_day_count: int = Field(gt=0)
    intervention_day_count: int = Field(ge=0)
    intervention_day_fraction: float = Field(ge=0, le=1)
    intervention_days_by_regime: dict[str, int]
    required_regimes: tuple[str, ...]
    minimum_intervention_count: int = Field(ge=0)
    minimum_intervention_day_fraction: float = Field(ge=0, le=1)
    minimum_days_per_required_regime: int = Field(ge=0)
    block_length_trading_days: int = Field(gt=0)
    effective_intervention_block_count: int = Field(ge=0)
    minimum_effective_intervention_block_count: int = Field(ge=0)
    evidence_class: AdvisoryInterventionEvidenceClass
    reason_codes: tuple[str, ...]

    @model_validator(mode="after")
    def validate_support(self) -> "AdvisoryActionInterventionSupportV1":
        if self.intervention_count > self.total_decision_count:
            raise ValueError("intervention_count cannot exceed total_decision_count")
        if self.intervention_day_count > self.decision_day_count:
            raise ValueError("intervention_day_count cannot exceed decision_day_count")
        expected_fraction = self.intervention_day_count / self.decision_day_count
        if not math.isclose(self.intervention_day_fraction, expected_fraction, rel_tol=0.0, abs_tol=1e-12):
            raise ValueError("intervention_day_fraction differs from counts")
        if any(value < 0 for value in self.intervention_days_by_regime.values()):
            raise ValueError("intervention_days_by_regime cannot contain negative counts")
        if len(set(self.required_regimes)) != len(self.required_regimes) or any(
            not str(value).strip() for value in self.required_regimes
        ):
            raise ValueError("required_regimes must contain distinct non-empty values")
        if sum(self.intervention_days_by_regime.values()) > self.intervention_day_count:
            raise ValueError("regime intervention days cannot exceed total intervention days")
        expected_blocks = math.ceil(self.intervention_day_count / self.block_length_trading_days)
        if self.effective_intervention_block_count != expected_blocks:
            raise ValueError("effective_intervention_block_count differs from frozen block rule")
        missing_regimes = [
            regime
            for regime in self.required_regimes
            if self.intervention_days_by_regime.get(regime, 0) < self.minimum_days_per_required_regime
        ]
        reasons: list[str] = []
        if self.intervention_count < self.minimum_intervention_count:
            reasons.append("INTERVENTION_COUNT_BELOW_MINIMUM")
        if self.intervention_day_fraction < self.minimum_intervention_day_fraction:
            reasons.append("INTERVENTION_DAY_FRACTION_BELOW_MINIMUM")
        if missing_regimes:
            reasons.append("REQUIRED_REGIME_SUPPORT_BELOW_MINIMUM")
        if self.effective_intervention_block_count < self.minimum_effective_intervention_block_count:
            reasons.append("EFFECTIVE_BLOCK_COUNT_BELOW_MINIMUM")
        expected_class = (
            AdvisoryInterventionEvidenceClass.CONFIRMATORY_ELIGIBLE
            if not reasons
            else AdvisoryInterventionEvidenceClass.EXPLORATORY_ONLY
        )
        if self.evidence_class != expected_class:
            raise ValueError("intervention evidence_class differs from frozen thresholds")
        if self.reason_codes != tuple(reasons):
            raise ValueError("intervention reason_codes differ from frozen thresholds")
        digest = canonical_json_sha256(self.functional_payload())
        if self.support_sha256 != digest:
            raise ValueError("intervention support identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"support_sha256"})


def build_incremental_value_label(**values: Any) -> AdvisoryIncrementalValueLabelV1:
    payload = {
        "schema_version": "advisory_incremental_value_label_v1",
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY,
        "deployable": False,
        **values,
    }
    payload["instrument"] = str(payload["instrument"]).strip().upper()
    draft = AdvisoryIncrementalValueLabelV1.model_construct(
        label_id="advincr_" + "0" * 24,
        label_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return AdvisoryIncrementalValueLabelV1(
        label_id=f"advincr_{digest[:24]}",
        label_sha256=digest,
        **payload,
    )


def build_intervention_support(**values: Any) -> AdvisoryActionInterventionSupportV1:
    values.pop("evidence_class", None)
    values.pop("reason_codes", None)
    reasons: list[str] = []
    if int(values["intervention_count"]) < int(values["minimum_intervention_count"]):
        reasons.append("INTERVENTION_COUNT_BELOW_MINIMUM")
    if float(values["intervention_day_fraction"]) < float(values["minimum_intervention_day_fraction"]):
        reasons.append("INTERVENTION_DAY_FRACTION_BELOW_MINIMUM")
    regime_counts = dict(values["intervention_days_by_regime"])
    if any(
        int(regime_counts.get(regime, 0)) < int(values["minimum_days_per_required_regime"])
        for regime in values["required_regimes"]
    ):
        reasons.append("REQUIRED_REGIME_SUPPORT_BELOW_MINIMUM")
    if int(values["effective_intervention_block_count"]) < int(values["minimum_effective_intervention_block_count"]):
        reasons.append("EFFECTIVE_BLOCK_COUNT_BELOW_MINIMUM")
    payload = {
        "schema_version": "advisory_action_intervention_support_v1",
        "evidence_class": (
            AdvisoryInterventionEvidenceClass.CONFIRMATORY_ELIGIBLE
            if not reasons
            else AdvisoryInterventionEvidenceClass.EXPLORATORY_ONLY
        ),
        "reason_codes": tuple(reasons),
        **values,
    }
    draft = AdvisoryActionInterventionSupportV1.model_construct(support_sha256="0" * 64, **payload)
    digest = canonical_json_sha256(draft.functional_payload())
    return AdvisoryActionInterventionSupportV1(support_sha256=digest, **payload)
