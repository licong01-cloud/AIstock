from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"


class ResearchStudyType(str, Enum):
    ORACLE_DIAGNOSTIC = "ORACLE_DIAGNOSTIC"
    LEARNABILITY_AUDIT = "LEARNABILITY_AUDIT"
    EXPLORATORY_SCREEN = "EXPLORATORY_SCREEN"
    CANDIDATE_MODEL = "CANDIDATE_MODEL"
    CONFIRMATION = "CONFIRMATION"
    ACTIVATION = "ACTIVATION"


class ObjectiveContract(str, Enum):
    ALPHA_RANKING = "ALPHA_RANKING"
    RISK_MANAGED_ADVISORY = "RISK_MANAGED_ADVISORY"


class DecisionUse(str, Enum):
    NAVIGATION_ONLY = "NAVIGATION_ONLY"
    DIRECTION_GATE = "DIRECTION_GATE"
    ACTIVATION_EVIDENCE = "ACTIVATION_EVIDENCE"


class ResearchResultClass(str, Enum):
    EXPLORATORY = "EXPLORATORY"
    NEGATIVE = "NEGATIVE"
    INCOMPLETE_NEGATIVE = "INCOMPLETE_NEGATIVE"
    FAMILY_FROZEN = "FAMILY_FROZEN"
    CONTROL_READY = "CONTROL_READY"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    ACTIVATED = "ACTIVATED"


class ParentPredictionExtensionStatus(str, Enum):
    FROZEN_MODEL_CAN_INFER = "FROZEN_MODEL_CAN_INFER"
    HISTORICAL_PREDICTION_ONLY = "HISTORICAL_PREDICTION_ONLY"
    RETRAIN_NEW_LINEAGE_REQUIRED = "RETRAIN_NEW_LINEAGE_REQUIRED"


class ResearchWindowState(str, Enum):
    DEVELOPMENT_CONSUMED = "DEVELOPMENT_CONSUMED"
    FROZEN_TEST_CONSUMED = "FROZEN_TEST_CONSUMED"
    HISTORICAL_REPLAY_CONSUMED = "HISTORICAL_REPLAY_CONSUMED"
    SEALED_UNCONSUMED = "SEALED_UNCONSUMED"


class EvidenceReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: str = Field(min_length=1)
    artifact_uri: str = Field(min_length=1)
    sha256: str = Field(pattern=SHA256_PATTERN)
    size_bytes: int = Field(gt=0)


class ConsumedWindowV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    window_id: str = Field(min_length=1)
    dataset_identity: str = Field(min_length=1)
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_dates(self) -> "ConsumedWindowV1":
        if self.start_date > self.end_date:
            raise ValueError("consumed window start_date is after end_date")
        return self


class AdvisoryResearchTrialRecordV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_research_trial_record_v1"] = (
        "advisory_research_trial_record_v1"
    )
    registry_entry_id: str = Field(pattern=r"^advtrial_[0-9a-f]{24}$")
    experiment_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    research_stage: str = Field(min_length=1)
    study_type: ResearchStudyType
    hypothesis_family_id: str = Field(min_length=1)
    parent_lineage: tuple[str, ...] = Field(min_length=1)
    unique_variable: str = Field(min_length=1)
    objective_contract: ObjectiveContract
    dataset_identity: str = Field(min_length=1)
    schema_identity: str = Field(min_length=1)
    policy_identity: str = Field(min_length=1)
    planned_trial_count: int = Field(ge=0)
    generated_trial_count: int = Field(ge=0)
    evaluated_trial_count: int = Field(ge=0)
    selected_trial_count: int = Field(ge=0)
    consumed_windows: tuple[ConsumedWindowV1, ...]
    result_class: ResearchResultClass
    decision_use: DecisionUse
    evidence_refs: tuple[EvidenceReferenceV1, ...] = Field(min_length=1)
    recorded_at: datetime
    record_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_record(self) -> "AdvisoryResearchTrialRecordV1":
        counts = (
            self.selected_trial_count,
            self.evaluated_trial_count,
            self.generated_trial_count,
            self.planned_trial_count,
        )
        if tuple(sorted(counts)) != counts:
            raise ValueError(
                "trial counts must satisfy selected <= evaluated <= generated <= planned"
            )
        if self.study_type in {
            ResearchStudyType.ORACLE_DIAGNOSTIC,
            ResearchStudyType.LEARNABILITY_AUDIT,
        } and self.decision_use == DecisionUse.ACTIVATION_EVIDENCE:
            raise ValueError("oracle/learnability records cannot be activation evidence")
        if (
            self.study_type == ResearchStudyType.EXPLORATORY_SCREEN
            and self.decision_use != DecisionUse.NAVIGATION_ONLY
        ):
            raise ValueError("exploratory studies are navigation-only")
        if self.result_class in {
            ResearchResultClass.EXPLORATORY,
            ResearchResultClass.INCOMPLETE_NEGATIVE,
        } and self.decision_use != DecisionUse.NAVIGATION_ONLY:
            raise ValueError("exploratory/incomplete results are navigation-only")
        if self.study_type == ResearchStudyType.ACTIVATION:
            if self.decision_use != DecisionUse.ACTIVATION_EVIDENCE:
                raise ValueError("activation records must use ACTIVATION_EVIDENCE")
        elif self.decision_use == DecisionUse.ACTIVATION_EVIDENCE:
            raise ValueError("only ACTIVATION records may use ACTIVATION_EVIDENCE")
        if self.result_class == ResearchResultClass.ACTIVATED and (
            self.study_type != ResearchStudyType.ACTIVATION
        ):
            raise ValueError("ACTIVATED result requires ACTIVATION study_type")
        if self.result_class == ResearchResultClass.CONFIRMED and (
            self.study_type != ResearchStudyType.CONFIRMATION
        ):
            raise ValueError("CONFIRMED result requires CONFIRMATION study_type")
        expected = canonical_json_sha256(self.functional_payload())
        if self.record_sha256 != expected:
            raise ValueError("record_sha256 mismatch")
        if self.registry_entry_id != f"advtrial_{expected[:24]}":
            raise ValueError("registry_entry_id does not match record_sha256")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"registry_entry_id", "recorded_at", "record_sha256"},
        )


def build_trial_record(**values: Any) -> AdvisoryResearchTrialRecordV1:
    values = dict(values)
    values.setdefault("schema_version", "advisory_research_trial_record_v1")
    values.setdefault("recorded_at", datetime.now(timezone.utc))
    functional_fields = set(AdvisoryResearchTrialRecordV1.model_fields) - {
        "registry_entry_id",
        "recorded_at",
        "record_sha256",
    }
    functional = {
        key: value
        for key, value in values.items()
        if key in functional_fields
    }
    normalized = _normalize_functional_trial_payload(functional)
    digest = canonical_json_sha256(normalized)
    values["record_sha256"] = digest
    values["registry_entry_id"] = f"advtrial_{digest[:24]}"
    return AdvisoryResearchTrialRecordV1.model_validate(values)


def _normalize_functional_trial_payload(values: dict[str, Any]) -> dict[str, Any]:
    payload = dict(values)
    payload["study_type"] = ResearchStudyType(payload["study_type"]).value
    payload["objective_contract"] = ObjectiveContract(payload["objective_contract"]).value
    payload["result_class"] = ResearchResultClass(payload["result_class"]).value
    payload["decision_use"] = DecisionUse(payload["decision_use"]).value
    payload["parent_lineage"] = list(payload["parent_lineage"])
    payload["consumed_windows"] = [
        ConsumedWindowV1.model_validate(item).model_dump(mode="json")
        for item in payload.get("consumed_windows", ())
    ]
    payload["evidence_refs"] = [
        EvidenceReferenceV1.model_validate(item).model_dump(mode="json")
        for item in payload["evidence_refs"]
    ]
    return payload


class ParentLegEvidenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    leg_id: str = Field(min_length=1)
    representative_run_id: str = Field(min_length=1)
    prediction_ref: EvidenceReferenceV1
    prediction_row_count: int = Field(gt=0)
    prediction_date_start: date
    prediction_date_end: date
    runtime_asset_root: str = Field(min_length=1)
    runtime_ready: bool
    runtime_refs: tuple[EvidenceReferenceV1, ...]
    missing_runtime_assets: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_leg(self) -> "ParentLegEvidenceV1":
        if self.prediction_date_start > self.prediction_date_end:
            raise ValueError("prediction date range is invalid")
        if self.runtime_ready and (not self.runtime_refs or self.missing_runtime_assets):
            raise ValueError("runtime_ready leg must have refs and no missing assets")
        if not self.runtime_ready and not self.missing_runtime_assets:
            raise ValueError("runtime-unready leg must identify missing assets")
        return self


class PostCutoffInferenceEvidenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_ref: EvidenceReferenceV1
    comparison_state_ref: EvidenceReferenceV1
    decision_trade_date: date
    target_trade_date: date
    candidate_count: int = Field(gt=0)
    parent_candidate_artifact_hash: str = Field(pattern=SHA256_PATTERN)
    parent_candidate_set_hash: str = Field(pattern=SHA256_PATTERN)
    observed_duration_seconds: float = Field(gt=0)


class AdvisoryParentPredictionExtensionReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_parent_prediction_extension_receipt_v1"] = (
        "advisory_parent_prediction_extension_receipt_v1"
    )
    receipt_id: str = Field(pattern=r"^advparentext_[0-9a-f]{24}$")
    status: ParentPredictionExtensionStatus
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    runtime_semantics_id: str = Field(min_length=1)
    runtime_semantics_hash: str = Field(pattern=SHA256_PATTERN)
    common_historical_prediction_cutoff: date
    target_extension_start: date
    target_extension_end: date
    legs: tuple[ParentLegEvidenceV1, ...] = Field(min_length=1)
    post_cutoff_evidence: PostCutoffInferenceEvidenceV1 | None = None
    explicit_retrain_ref: EvidenceReferenceV1 | None = None
    capability_gaps: tuple[str, ...] = ()
    scan_duration_seconds: float = Field(ge=0)
    created_at: datetime
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryParentPredictionExtensionReceiptV1":
        if self.target_extension_start > self.target_extension_end:
            raise ValueError("target extension range is invalid")
        if self.status == ParentPredictionExtensionStatus.FROZEN_MODEL_CAN_INFER:
            if self.post_cutoff_evidence is None:
                raise ValueError("frozen infer status requires post-cutoff evidence")
            if self.explicit_retrain_ref is not None or self.capability_gaps:
                raise ValueError("frozen infer status cannot contain retrain/gap state")
            if not all(item.runtime_ready for item in self.legs):
                raise ValueError("frozen infer status requires all runtime legs")
            if (
                self.post_cutoff_evidence.decision_trade_date
                <= self.common_historical_prediction_cutoff
            ):
                raise ValueError("post-cutoff evidence is not after the common cutoff")
            if self.target_extension_start <= self.common_historical_prediction_cutoff:
                raise ValueError("target extension must start after the common historical cutoff")
            if not (
                self.target_extension_start
                <= self.post_cutoff_evidence.decision_trade_date
                <= self.target_extension_end
            ):
                raise ValueError("post-cutoff evidence is outside the requested extension range")
        elif self.status == ParentPredictionExtensionStatus.RETRAIN_NEW_LINEAGE_REQUIRED:
            if self.explicit_retrain_ref is None:
                raise ValueError("retrain status requires an explicit typed receipt")
        elif not self.capability_gaps:
            raise ValueError("historical-only status must report capability gaps")
        expected = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != expected:
            raise ValueError("parent extension receipt_sha256 mismatch")
        if self.receipt_id != f"advparentext_{expected[:24]}":
            raise ValueError("parent extension receipt_id mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"receipt_id", "created_at", "receipt_sha256", "scan_duration_seconds"},
        )


def build_parent_extension_receipt(**values: Any) -> AdvisoryParentPredictionExtensionReceiptV1:
    values = dict(values)
    values.setdefault("created_at", datetime.now(timezone.utc))
    values.setdefault("scan_duration_seconds", 0.0)
    values.setdefault("post_cutoff_evidence", None)
    values.setdefault("explicit_retrain_ref", None)
    values.setdefault("capability_gaps", ())
    functional = {
        key: value
        for key, value in values.items()
        if key
        not in {"receipt_id", "created_at", "receipt_sha256", "scan_duration_seconds"}
    }
    functional.setdefault("schema_version", "advisory_parent_prediction_extension_receipt_v1")
    functional["status"] = ParentPredictionExtensionStatus(functional["status"]).value
    for field in (
        "common_historical_prediction_cutoff",
        "target_extension_start",
        "target_extension_end",
    ):
        functional[field] = date.fromisoformat(str(functional[field])).isoformat()
    functional["legs"] = [
        ParentLegEvidenceV1.model_validate(item).model_dump(mode="json")
        for item in functional["legs"]
    ]
    if functional.get("post_cutoff_evidence") is not None:
        functional["post_cutoff_evidence"] = PostCutoffInferenceEvidenceV1.model_validate(
            functional["post_cutoff_evidence"]
        ).model_dump(mode="json")
    if functional.get("explicit_retrain_ref") is not None:
        functional["explicit_retrain_ref"] = EvidenceReferenceV1.model_validate(
            functional["explicit_retrain_ref"]
        ).model_dump(mode="json")
    functional["capability_gaps"] = list(functional.get("capability_gaps", ()))
    digest = canonical_json_sha256(functional)
    values["receipt_sha256"] = digest
    values["receipt_id"] = f"advparentext_{digest[:24]}"
    return AdvisoryParentPredictionExtensionReceiptV1.model_validate(values)


class AdvisoryResearchWindowV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    window_id: str = Field(min_length=1)
    dataset_identity: str = Field(min_length=1)
    start_date: date
    end_date: date
    state: ResearchWindowState
    purpose: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_dates(self) -> "AdvisoryResearchWindowV1":
        if self.start_date > self.end_date:
            raise ValueError("research window start_date is after end_date")
        return self


class AdvisoryResearchWindowContractV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_research_window_contract_v1"] = (
        "advisory_research_window_contract_v1"
    )
    contract_id: str = Field(pattern=r"^advwindow_[0-9a-f]{24}$")
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    runtime_semantics_hash: str = Field(pattern=SHA256_PATTERN)
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    shadow_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    source_policy: str = Field(min_length=1)
    artifact_root_uri: str = Field(min_length=1)
    sealed_consumption_receipt_uri: str = Field(min_length=1)
    windows: tuple[AdvisoryResearchWindowV1, ...] = Field(min_length=4)
    created_at: datetime
    contract_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_contract(self) -> "AdvisoryResearchWindowContractV1":
        if len({item.window_id for item in self.windows}) != len(self.windows):
            raise ValueError("research window ids must be unique")
        sealed = [item for item in self.windows if item.state == ResearchWindowState.SEALED_UNCONSUMED]
        if len(sealed) != 1:
            raise ValueError("window contract must contain exactly one sealed holdout")
        sealed_window = sealed[0]
        if not self.sealed_consumption_receipt_uri.endswith(
            "/sealed_holdout_consumption_receipt.json"
        ):
            raise ValueError("sealed consumption receipt must use the canonical filename")
        normalized_root = self.artifact_root_uri.rstrip("/")
        if not self.sealed_consumption_receipt_uri.startswith(normalized_root + "/"):
            raise ValueError("sealed consumption receipt must be inside the contract artifact root")
        for item in self.windows:
            if item is sealed_window:
                continue
            if _date_ranges_overlap(
                item.start_date,
                item.end_date,
                sealed_window.start_date,
                sealed_window.end_date,
            ):
                raise ValueError("sealed holdout overlaps a consumed/development window")
        expected = canonical_json_sha256(self.functional_payload())
        if self.contract_sha256 != expected:
            raise ValueError("window contract_sha256 mismatch")
        if self.contract_id != f"advwindow_{expected[:24]}":
            raise ValueError("window contract_id mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json", exclude={"contract_id", "created_at", "contract_sha256"}
        )


def build_window_contract(**values: Any) -> AdvisoryResearchWindowContractV1:
    values = dict(values)
    values.setdefault("created_at", datetime.now(timezone.utc))
    values.setdefault("schema_version", "advisory_research_window_contract_v1")
    normalized_windows = [
        AdvisoryResearchWindowV1.model_validate(item).model_dump(mode="json")
        for item in values["windows"]
    ]
    functional = {
        key: value
        for key, value in values.items()
        if key not in {"contract_id", "created_at", "contract_sha256"}
    }
    functional["windows"] = normalized_windows
    digest = canonical_json_sha256(functional)
    values["contract_sha256"] = digest
    values["contract_id"] = f"advwindow_{digest[:24]}"
    return AdvisoryResearchWindowContractV1.model_validate(values)


class ResearchWindowAccessRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_research_window_access_request_v1"] = (
        "advisory_research_window_access_request_v1"
    )
    request_id: str = Field(pattern=r"^advwindowaccess_[0-9a-f]{24}$")
    contract_sha256: str = Field(pattern=SHA256_PATTERN)
    study_type: ResearchStudyType
    objective_contract: ObjectiveContract
    decision_use: DecisionUse
    dataset_identity: str = Field(min_length=1)
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    start_date: date
    end_date: date
    frontier_id: str | None = None
    candidate_id: str | None = None
    request_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_request(self) -> "ResearchWindowAccessRequestV1":
        if self.start_date > self.end_date:
            raise ValueError("access request date range is invalid")
        if self.decision_use == DecisionUse.ACTIVATION_EVIDENCE:
            raise ValueError("raw research window access cannot be ACTIVATION_EVIDENCE")
        if (
            self.study_type == ResearchStudyType.EXPLORATORY_SCREEN
            and self.decision_use != DecisionUse.NAVIGATION_ONLY
        ):
            raise ValueError("exploratory window access is navigation-only")
        if self.study_type == ResearchStudyType.CONFIRMATION:
            if not self.frontier_id or not self.candidate_id:
                raise ValueError("confirmation requires frontier_id and candidate_id")
            if self.decision_use != DecisionUse.DIRECTION_GATE:
                raise ValueError("confirmation access must use DIRECTION_GATE")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected:
            raise ValueError("window access request_sha256 mismatch")
        if self.request_id != f"advwindowaccess_{expected[:24]}":
            raise ValueError("window access request_id mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256"})


def build_window_access_request(**values: Any) -> ResearchWindowAccessRequestV1:
    values = dict(values)
    values.setdefault("schema_version", "advisory_research_window_access_request_v1")
    values.setdefault("frontier_id", None)
    values.setdefault("candidate_id", None)
    functional = dict(values)
    functional["study_type"] = ResearchStudyType(functional["study_type"]).value
    functional["objective_contract"] = ObjectiveContract(
        functional["objective_contract"]
    ).value
    functional["decision_use"] = DecisionUse(functional["decision_use"]).value
    functional["start_date"] = date.fromisoformat(str(functional["start_date"])).isoformat()
    functional["end_date"] = date.fromisoformat(str(functional["end_date"])).isoformat()
    digest = canonical_json_sha256(functional)
    values["request_sha256"] = digest
    values["request_id"] = f"advwindowaccess_{digest[:24]}"
    return ResearchWindowAccessRequestV1.model_validate(values)


class SealedHoldoutConsumptionReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_sealed_holdout_consumption_receipt_v1"] = (
        "advisory_sealed_holdout_consumption_receipt_v1"
    )
    consumption_id: str = Field(pattern=r"^advholdoutconsume_[0-9a-f]{24}$")
    contract_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    window_id: str = Field(min_length=1)
    dataset_identity: str = Field(min_length=1)
    objective_contract: ObjectiveContract
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    frontier_id: str = Field(min_length=1)
    candidate_id: str = Field(min_length=1)
    consumed_at: datetime
    consumption_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "SealedHoldoutConsumptionReceiptV1":
        expected = canonical_json_sha256(self.functional_payload())
        if self.consumption_sha256 != expected:
            raise ValueError("holdout consumption_sha256 mismatch")
        if self.consumption_id != f"advholdoutconsume_{expected[:24]}":
            raise ValueError("holdout consumption_id mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"consumption_id", "consumed_at", "consumption_sha256"},
        )


def build_holdout_consumption_receipt(
    *,
    contract: AdvisoryResearchWindowContractV1,
    request: ResearchWindowAccessRequestV1,
    window_id: str,
) -> SealedHoldoutConsumptionReceiptV1:
    values: dict[str, Any] = {
        "schema_version": "advisory_sealed_holdout_consumption_receipt_v1",
        "contract_sha256": contract.contract_sha256,
        "request_sha256": request.request_sha256,
        "window_id": window_id,
        "dataset_identity": request.dataset_identity,
        "objective_contract": request.objective_contract,
        "policy_identity": request.policy_identity,
        "frontier_id": request.frontier_id,
        "candidate_id": request.candidate_id,
        "consumed_at": datetime.now(timezone.utc),
    }
    functional = dict(values)
    functional.pop("consumed_at")
    functional["objective_contract"] = request.objective_contract.value
    digest = canonical_json_sha256(functional)
    values["consumption_sha256"] = digest
    values["consumption_id"] = f"advholdoutconsume_{digest[:24]}"
    return SealedHoldoutConsumptionReceiptV1.model_validate(values)


class N0CompletionReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_n0_completion_receipt_v1"] = (
        "advisory_n0_completion_receipt_v1"
    )
    receipt_id: str = Field(pattern=r"^advn0_[0-9a-f]{24}$")
    status: Literal["COMPLETE"] = "COMPLETE"
    registry_ref: EvidenceReferenceV1
    route_ref: EvidenceReferenceV1
    parent_spike_ref: EvidenceReferenceV1
    window_contract_ref: EvidenceReferenceV1
    next_task: Literal[
        "N1_TIER1_ORACLE_LEARNABILITY", "PARENT_PREDICTION_EXTENSION_DECISION"
    ] = "N1_TIER1_ORACLE_LEARNABILITY"
    production_ddl_gate: Literal["noop"] = "noop"
    runtime_activation: Literal["noop"] = "noop"
    backend_restart: Literal["noop"] = "noop"
    created_at: datetime
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "N0CompletionReceiptV1":
        expected = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != expected:
            raise ValueError("N0 completion receipt_sha256 mismatch")
        if self.receipt_id != f"advn0_{expected[:24]}":
            raise ValueError("N0 completion receipt_id mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json", exclude={"receipt_id", "created_at", "receipt_sha256"}
        )


def build_n0_completion_receipt(**values: Any) -> N0CompletionReceiptV1:
    values = dict(values)
    values.setdefault("created_at", datetime.now(timezone.utc))
    values.setdefault("schema_version", "advisory_n0_completion_receipt_v1")
    values.setdefault("status", "COMPLETE")
    values.setdefault("next_task", "N1_TIER1_ORACLE_LEARNABILITY")
    values.setdefault("production_ddl_gate", "noop")
    values.setdefault("runtime_activation", "noop")
    values.setdefault("backend_restart", "noop")
    functional = {
        key: value
        for key, value in values.items()
        if key not in {"receipt_id", "created_at", "receipt_sha256"}
    }
    for field in ("registry_ref", "route_ref", "parent_spike_ref", "window_contract_ref"):
        functional[field] = EvidenceReferenceV1.model_validate(functional[field]).model_dump(
            mode="json"
        )
    digest = canonical_json_sha256(functional)
    values["receipt_sha256"] = digest
    values["receipt_id"] = f"advn0_{digest[:24]}"
    return N0CompletionReceiptV1.model_validate(values)


def _date_ranges_overlap(a_start: date, a_end: date, b_start: date, b_end: date) -> bool:
    return a_start <= b_end and b_start <= a_end
