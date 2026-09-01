from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.entry_guard_decision import (
    EntryGuardMode,
    EntryGuardPolicyV1,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchStudyType,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
ENTRY_DECISION_START = date(2025, 11, 7)
ENTRY_DECISION_END = date(2026, 2, 2)
EXIT_DECISION_START = date(2024, 7, 4)
EXIT_DECISION_END = date(2026, 2, 2)
OUTCOME_CUTOFF = date(2026, 3, 10)
ENTRY_OVERLAP_DAY_COUNT = 60
ENTRY_OVERLAP_ROW_COUNT = 1200
ENTRY_MATURED_ROW_COUNT = 1199
RESOURCE_MAX_RSS_BYTES = 8 * 1024**3
EXIT_INTERVENTION_POLICY_SHA256 = canonical_json_sha256(
    {
        "policy": "EXIT_AT_FIRST_EXECUTABLE_OPEN_V1",
        "baseline": "FROZEN_SHADOW_POLICY",
        "position_semantics": "NO_DYNAMIC_POSITION",
    }
)
ORACLE_ENTRY_POLICY_SHA256 = canonical_json_sha256(
    {
        "policy": "PERFECT_SIGN_SKIP_V1",
        "replacement_order": "SELECTION_RANK_ASCENDING",
        "position_semantics": "FIVE_FIXED_EQUAL_SLOTS",
    }
)
ENTRY_ARM_IDS = (
    "NO_GUARD_BASELINE",
    "FIXED_3_CASH",
    "FIXED_3_REPLACE",
    "FIXED_5_CASH",
    "FIXED_5_REPLACE",
    "DYNAMIC_Q90_CASH",
    "DYNAMIC_Q90_REPLACE",
    "PERFECT_SKIP_CASH_ORACLE",
    "PERFECT_SKIP_REPLACE_ORACLE",
)


class ActionSupportSpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n2_action_support_spec_v1"] = "advisory_n2_action_support_spec_v1"
    minimum_intervention_count: Literal[20] = 20
    minimum_intervention_day_fraction: Literal[0.25] = 0.25
    required_regimes: tuple[Literal["UP_OR_FLAT", "DOWN"], Literal["UP_OR_FLAT", "DOWN"]] = ("UP_OR_FLAT", "DOWN")
    minimum_days_per_required_regime: Literal[5] = 5
    block_length_trading_days: Literal[20] = 20
    minimum_effective_intervention_block_count: Literal[2] = 2

    @model_validator(mode="after")
    def validate_regimes(self) -> "ActionSupportSpecV1":
        if self.required_regimes != ("UP_OR_FLAT", "DOWN"):
            raise ValueError("N2 action support regimes must match N1 UP_OR_FLAT/DOWN semantics")
        return self


class EntryFormalArmSpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    arm_id: str = Field(min_length=1)
    guard_mode: EntryGuardMode | None = None
    guard_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    fill_policy: Literal["BASELINE_TOP5", "CASH", "RANK_ONLY_REPLACEMENT"]
    oracle: bool = False
    deployable: Literal[False] = False


class FrozenAdvisoryN2ActionAuditRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_n2_action_audit_request_v1"] = "frozen_advisory_n2_action_audit_request_v1"
    request_id: str = Field(pattern=r"^advactionreq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: Literal[ObjectiveContract.RISK_MANAGED_ADVISORY] = ObjectiveContract.RISK_MANAGED_ADVISORY
    study_type: Literal[ResearchStudyType.ORACLE_DIAGNOSTIC] = ResearchStudyType.ORACLE_DIAGNOSTIC
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    planned_trial_count: Literal[0] = 0
    sealed_holdout_accessed: Literal[False] = False
    n1_request_path: str = Field(min_length=1)
    n1_request_ref: EvidenceReferenceV1
    n1_bundle_path: str = Field(min_length=1)
    n1_bundle_manifest_ref: EvidenceReferenceV1
    policy_dataset_manifest_ref: EvidenceReferenceV1
    m4_request_path: str = Field(min_length=1)
    m4_request_ref: EvidenceReferenceV1
    m4_bundle_path: str = Field(min_length=1)
    m4_bundle_manifest_ref: EvidenceReferenceV1
    m4_predictions_ref: EvidenceReferenceV1
    n0_completion_ref: EvidenceReferenceV1
    parent_spike_path: str = Field(min_length=1)
    parent_spike_ref: EvidenceReferenceV1
    research_window_contract_ref: EvidenceReferenceV1
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    shadow_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    entry_guard_policies: tuple[EntryGuardPolicyV1, ...]
    entry_arms: tuple[EntryFormalArmSpecV1, ...]
    exit_intervention_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    entry_support_spec: ActionSupportSpecV1
    exit_support_spec: ActionSupportSpecV1
    entry_decision_start: date = ENTRY_DECISION_START
    entry_decision_end: date = ENTRY_DECISION_END
    entry_overlap_day_count: int = ENTRY_OVERLAP_DAY_COUNT
    entry_overlap_row_count: int = ENTRY_OVERLAP_ROW_COUNT
    entry_matured_row_count: int = ENTRY_MATURED_ROW_COUNT
    exit_decision_start: date = EXIT_DECISION_START
    exit_decision_end: date = EXIT_DECISION_END
    outcome_cutoff: date = OUTCOME_CUTOFF
    dynamic_gap_policy: Literal["MAX_ZERO_AND_M4_Q90_BPS"] = "MAX_ZERO_AND_M4_Q90_BPS"
    reduce_shadow_action: Literal["ENTER_UNCHANGED_ADVICE_ONLY"] = "ENTER_UNCHANGED_ADVICE_ONLY"
    target_slot_count: Literal[5] = 5
    replacement_depth: Literal[20] = 20
    qlib_daily_root: str = Field(min_length=1)
    suspend_data_root: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: Literal[RESOURCE_MAX_RSS_BYTES] = RESOURCE_MAX_RSS_BYTES

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenAdvisoryN2ActionAuditRequestV1":
        if (
            self.entry_decision_start != ENTRY_DECISION_START
            or self.entry_decision_end != ENTRY_DECISION_END
            or self.exit_decision_start != EXIT_DECISION_START
            or self.exit_decision_end != EXIT_DECISION_END
            or self.outcome_cutoff != OUTCOME_CUTOFF
        ):
            raise ValueError("N2 action audit windows differ from frozen development windows")
        if (
            self.entry_overlap_day_count != ENTRY_OVERLAP_DAY_COUNT
            or self.entry_overlap_row_count != ENTRY_OVERLAP_ROW_COUNT
            or self.entry_matured_row_count != ENTRY_MATURED_ROW_COUNT
        ):
            raise ValueError("N2 Entry overlap shape differs from the frozen source intersection")
        policies = {item.mode: item for item in self.entry_guard_policies}
        if set(policies) != {
            EntryGuardMode.NO_GUARD,
            EntryGuardMode.FIXED_GAP_3,
            EntryGuardMode.FIXED_GAP_5,
            EntryGuardMode.FROZEN_DYNAMIC,
        }:
            raise ValueError("N2 Entry request requires the exact four guard policies")
        if tuple(item.arm_id for item in self.entry_arms) != ENTRY_ARM_IDS:
            raise ValueError("N2 Entry request arm order/identity drift")
        expected_arms = {
            "NO_GUARD_BASELINE": (EntryGuardMode.NO_GUARD, "BASELINE_TOP5", False),
            "FIXED_3_CASH": (EntryGuardMode.FIXED_GAP_3, "CASH", False),
            "FIXED_3_REPLACE": (
                EntryGuardMode.FIXED_GAP_3,
                "RANK_ONLY_REPLACEMENT",
                False,
            ),
            "FIXED_5_CASH": (EntryGuardMode.FIXED_GAP_5, "CASH", False),
            "FIXED_5_REPLACE": (
                EntryGuardMode.FIXED_GAP_5,
                "RANK_ONLY_REPLACEMENT",
                False,
            ),
            "DYNAMIC_Q90_CASH": (EntryGuardMode.FROZEN_DYNAMIC, "CASH", False),
            "DYNAMIC_Q90_REPLACE": (
                EntryGuardMode.FROZEN_DYNAMIC,
                "RANK_ONLY_REPLACEMENT",
                False,
            ),
            "PERFECT_SKIP_CASH_ORACLE": (None, "CASH", True),
            "PERFECT_SKIP_REPLACE_ORACLE": (
                None,
                "RANK_ONLY_REPLACEMENT",
                True,
            ),
        }
        for arm in self.entry_arms:
            if (arm.guard_mode, arm.fill_policy, arm.oracle) != expected_arms[arm.arm_id]:
                raise ValueError(f"N2 Entry arm semantics drift: {arm.arm_id}")
            if arm.guard_mode is not None:
                expected = policies[arm.guard_mode].policy_sha256
                if arm.guard_policy_sha256 != expected:
                    raise ValueError("N2 Entry arm guard policy hash mismatch")
            elif arm.guard_policy_sha256 != ORACLE_ENTRY_POLICY_SHA256:
                raise ValueError("N2 Entry oracle policy hash mismatch")
        if self.exit_intervention_policy_sha256 != EXIT_INTERVENTION_POLICY_SHA256:
            raise ValueError("N2 Exit intervention policy hash drift")
        expected_roles = {
            "n1_request_ref": "n2_action_n1_request",
            "n1_bundle_manifest_ref": "n2_action_n1_bundle_manifest",
            "policy_dataset_manifest_ref": "n2_action_policy_dataset_manifest",
            "m4_request_ref": "n2_action_m4_request",
            "m4_bundle_manifest_ref": "n2_action_m4_bundle_manifest",
            "m4_predictions_ref": "n2_action_m4_test_predictions",
            "n0_completion_ref": "n0_completion",
            "parent_spike_ref": "n2_action_parent_spike",
            "research_window_contract_ref": "n0_window_contract",
        }
        for field_name, role in expected_roles.items():
            if getattr(self, field_name).role != role:
                raise ValueError(f"N2 action evidence role drift: {field_name}")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advactionreq_{digest[:24]}":
            raise ValueError("N2 action request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class AdvisoryN2ActionAuditReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n2_action_audit_receipt_v1"] = "advisory_n2_action_audit_receipt_v1"
    receipt_id: str = Field(pattern=r"^advactionrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    deployable: Literal[False] = False
    objective_contract: Literal[ObjectiveContract.RISK_MANAGED_ADVISORY] = ObjectiveContract.RISK_MANAGED_ADVISORY
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    entry_summary: dict[str, Any]
    exit_summary: dict[str, Any]
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    resource_report_sha256: str = Field(pattern=SHA256_PATTERN)
    sealed_holdout_accessed: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryN2ActionAuditReceiptV1":
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advactionrcpt_{digest[:24]}":
            raise ValueError("N2 action receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_n2_action_request(**values: Any) -> FrozenAdvisoryN2ActionAuditRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_n2_action_audit_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY,
        "study_type": ResearchStudyType.ORACLE_DIAGNOSTIC,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "planned_trial_count": 0,
        "sealed_holdout_accessed": False,
        **values,
    }
    draft = FrozenAdvisoryN2ActionAuditRequestV1.model_construct(
        request_id="advactionreq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenAdvisoryN2ActionAuditRequestV1(
        request_id=f"advactionreq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_n2_action_receipt(**values: Any) -> AdvisoryN2ActionAuditReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_n2_action_audit_receipt_v1",
        "status": "COMPLETE",
        "deployable": False,
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "sealed_holdout_accessed": False,
        "created_at": created_at,
        **values,
    }
    draft = AdvisoryN2ActionAuditReceiptV1.model_construct(
        receipt_id="advactionrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return AdvisoryN2ActionAuditReceiptV1(
        receipt_id=f"advactionrcpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )
