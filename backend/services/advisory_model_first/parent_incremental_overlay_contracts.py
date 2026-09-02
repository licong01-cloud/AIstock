from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
PARENT_OVERLAY_EXPERIMENT_ID = "ADVISORY-N3-PARENT-INCREMENTAL-OVERLAY-V1"
PARENT_OVERLAY_HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-PARENT-INCREMENTAL-OVERLAY-V1"
PARENT_QE_ALPHA_EXPERIMENT_ID = "ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1"
PARENT_OVERLAY_SIGNAL_START = date(2024, 7, 4)
PARENT_OVERLAY_SIGNAL_END = date(2026, 2, 2)
PARENT_OVERLAY_TRIAL_COUNT = 24
PARENT_OVERLAY_BLOCK_LENGTH = 20
PARENT_OVERLAY_BOOTSTRAP_REPETITIONS = 2000
PARENT_OVERLAY_RANDOM_SEED = 20260902
PARENT_OVERLAY_MAX_RSS_BYTES = 16 * 1024**3
PARENT_OVERLAY_MAX_TEMP_BYTES = 16 * 1024**3

PARENT_OVERLAY_CANDIDATES = (
    "N3_PRICE_VOLUME_BEHAVIOR_02",
    "N3_SECTOR_RELATIVE_04",
    "N3_CROWDING_DISPERSION_01",
    "N3_CROWDING_DISPERSION_03",
    "N3_CROWDING_DISPERSION_04",
    "N3_REGIME_CONDITIONED_02",
)
PARENT_OVERLAY_WEIGHT_BPS = (500, 1000, 1500, 2000)
PARENT_OVERLAY_EXPRESSION_SHA256 = {
    "N3_PRICE_VOLUME_BEHAVIOR_02": "181210d2ac9bf969fb9805150d8d2f5162394ee482c1e9b27df8a3a06f278c33",
    "N3_SECTOR_RELATIVE_04": "f364baaa7ec77b10c29f9e012972e237f0b07b4028b29dd0c5d14cdfb1ba069f",
    "N3_CROWDING_DISPERSION_01": "273b6b9fb70a25ca50b0f0af2f1dbe307fcae257e2b6bebee23b77a1df14d17c",
    "N3_CROWDING_DISPERSION_03": "962413e9b34f0969bbd3c835d88811b4b269395bb74bea3df83df33694d16497",
    "N3_CROWDING_DISPERSION_04": "fb4d30258d276089e63a2c5661c13ed28ac304a87f8997e649b88392743521d2",
    "N3_REGIME_CONDITIONED_02": "e2d60af83bd42c67e896d4de7a2e8ccdec4a61e3c5051a3315a2f7be2defef6a",
}


class ParentIncrementalOverlayTrialV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_parent_incremental_overlay_trial_v1"] = (
        "advisory_parent_incremental_overlay_trial_v1"
    )
    trial_id: str = Field(pattern=r"^N3OVL_[A-Z0-9_]+_W(0500|1000|1500|2000)$")
    candidate_id: str = Field(min_length=1)
    source_expression_sha256: str = Field(pattern=SHA256_PATTERN)
    weight_bps: Literal[500, 1000, 1500, 2000]
    direction_frozen: Literal[True] = True

    @model_validator(mode="after")
    def validate_trial(self) -> "ParentIncrementalOverlayTrialV1":
        if self.candidate_id not in PARENT_OVERLAY_CANDIDATES:
            raise ValueError("parent overlay candidate is not frozen")
        if self.source_expression_sha256 != PARENT_OVERLAY_EXPRESSION_SHA256[self.candidate_id]:
            raise ValueError("parent overlay source expression identity drift")
        expected_id = f"N3OVL_{self.candidate_id.removeprefix('N3_')}_W{self.weight_bps:04d}"
        if self.trial_id != expected_id:
            raise ValueError("parent overlay trial identity drift")
        return self

    @property
    def weight(self) -> float:
        return self.weight_bps / 10_000.0


class FrozenParentIncrementalOverlayRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_parent_incremental_overlay_request_v1"] = (
        "frozen_advisory_parent_incremental_overlay_request_v1"
    )
    request_id: str = Field(pattern=r"^advn3ovlreq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: ObjectiveContract = ObjectiveContract.ALPHA_RANKING
    study_type: ResearchStudyType = ResearchStudyType.EXPLORATORY_SCREEN
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    planned_trial_count: Literal[24] = 24
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    trials: tuple[ParentIncrementalOverlayTrialV1, ...]
    evidence_refs: tuple[EvidenceReferenceV1, ...]
    parent_bundle_path: str = Field(min_length=1)
    parent_bundle_id: str = Field(pattern=SHA256_PATTERN)
    parent_request_sha256: str = Field(pattern=SHA256_PATTERN)
    parent_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    parent_frontier_sha256: str = Field(pattern=SHA256_PATTERN)
    parent_experiment_id: str = PARENT_QE_ALPHA_EXPERIMENT_ID
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    signal_start: date = PARENT_OVERLAY_SIGNAL_START
    signal_end: date = PARENT_OVERLAY_SIGNAL_END
    minimum_evaluable_days: Literal[382] = 382
    minimum_intervention_days: Literal[20] = 20
    minimum_intervention_fraction: float = Field(default=0.05, ge=0.0, le=1.0)
    minimum_intervention_quarters: Literal[2] = 2
    block_length_trading_days: Literal[20] = 20
    bootstrap_repetitions: Literal[2000] = 2000
    bootstrap_seed: Literal[20260902] = 20260902
    familywise_trial_count: Literal[24] = 24
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: int = Field(default=PARENT_OVERLAY_MAX_RSS_BYTES, gt=0)
    resource_max_temp_bytes: int = Field(default=PARENT_OVERLAY_MAX_TEMP_BYTES, gt=0)
    resource_max_wall_seconds: Literal[None] = None
    database_read_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    qlib_read_allowed: Literal[False] = False
    factor_catalog_write_allowed: Literal[False] = False
    strategy_package_write_allowed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    position_weight_output_allowed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenParentIncrementalOverlayRequestV1":
        if (
            self.objective_contract != ObjectiveContract.ALPHA_RANKING
            or self.study_type != ResearchStudyType.EXPLORATORY_SCREEN
            or self.decision_use != DecisionUse.NAVIGATION_ONLY
            or self.parent_experiment_id != PARENT_QE_ALPHA_EXPERIMENT_ID
        ):
            raise ValueError("parent overlay research contract drift")
        if (
            self.minimum_intervention_fraction != 0.05
            or self.resource_max_rss_bytes != PARENT_OVERLAY_MAX_RSS_BYTES
            or self.resource_max_temp_bytes != PARENT_OVERLAY_MAX_TEMP_BYTES
        ):
            raise ValueError("parent overlay frozen threshold/resource drift")
        if self.signal_start != PARENT_OVERLAY_SIGNAL_START or self.signal_end != PARENT_OVERLAY_SIGNAL_END:
            raise ValueError("parent overlay signal window drift")
        if self.trials != build_default_overlay_trials():
            raise ValueError("parent overlay 6x4 trial roster drift")
        roles = [item.role for item in self.evidence_refs]
        required_roles = {
            "n3_parent_overlay_parent_manifest",
            "n3_parent_overlay_parent_score_panel",
            "n3_parent_overlay_parent_proposal_summary",
            "n3_parent_overlay_parent_frontier",
        }
        if len(roles) != len(set(roles)) or set(roles) != required_roles:
            raise ValueError("parent overlay evidence role roster drift")
        if self.parent_bundle_path.replace("\\", "/").rstrip("/").split("/")[-1] != self.parent_bundle_id:
            raise ValueError("parent overlay bundle path/id drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advn3ovlreq_{digest[:24]}":
            raise ValueError("parent overlay request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class ParentIncrementalOverlayReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_parent_incremental_overlay_receipt_v1"] = (
        "advisory_parent_incremental_overlay_receipt_v1"
    )
    receipt_id: str = Field(pattern=r"^advn3ovlrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    planned_trial_count: Literal[24] = 24
    generated_trial_count: Literal[24] = 24
    evaluated_trial_count: Literal[24] = 24
    selected_trial_count: int = Field(ge=0, le=1)
    selected_trial_id: str | None
    eligible_trial_ids: tuple[str, ...]
    result_class: ResearchResultClass = ResearchResultClass.EXPLORATORY
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    next_task: Literal[
        "N3_PARENT_OVERLAY_CONFIRMATION_DESIGN",
        "N3_ALPHA_INFORMATION_SET_EXPANSION_MVE",
    ]
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    resource_report_sha256: str = Field(pattern=SHA256_PATTERN)
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    runtime_eligible: Literal[False] = False
    factor_catalog_written: Literal[False] = False
    strategy_package_written: Literal[False] = False
    position_weight_output: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "ParentIncrementalOverlayReceiptV1":
        if self.result_class != ResearchResultClass.EXPLORATORY or self.decision_use != DecisionUse.NAVIGATION_ONLY:
            raise ValueError("parent overlay receipt research contract drift")
        selected_count = 1 if self.selected_trial_id else 0
        expected_next = (
            "N3_PARENT_OVERLAY_CONFIRMATION_DESIGN" if selected_count else "N3_ALPHA_INFORMATION_SET_EXPANSION_MVE"
        )
        if self.selected_trial_count != selected_count or self.next_task != expected_next:
            raise ValueError("parent overlay selection/next-task relation drift")
        if self.selected_trial_id is not None and self.selected_trial_id not in self.eligible_trial_ids:
            raise ValueError("parent overlay selected trial is not eligible")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advn3ovlrcpt_{digest[:24]}":
            raise ValueError("parent overlay receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_default_overlay_trials() -> tuple[ParentIncrementalOverlayTrialV1, ...]:
    return tuple(
        ParentIncrementalOverlayTrialV1(
            trial_id=f"N3OVL_{candidate.removeprefix('N3_')}_W{weight_bps:04d}",
            candidate_id=candidate,
            source_expression_sha256=PARENT_OVERLAY_EXPRESSION_SHA256[candidate],
            weight_bps=weight_bps,
        )
        for candidate in PARENT_OVERLAY_CANDIDATES
        for weight_bps in PARENT_OVERLAY_WEIGHT_BPS
    )


def build_parent_overlay_request(**values: Any) -> FrozenParentIncrementalOverlayRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_parent_incremental_overlay_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "study_type": ResearchStudyType.EXPLORATORY_SCREEN,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "planned_trial_count": PARENT_OVERLAY_TRIAL_COUNT,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "trials": build_default_overlay_trials(),
        "parent_experiment_id": PARENT_QE_ALPHA_EXPERIMENT_ID,
        "signal_start": PARENT_OVERLAY_SIGNAL_START,
        "signal_end": PARENT_OVERLAY_SIGNAL_END,
        "resource_max_rss_bytes": PARENT_OVERLAY_MAX_RSS_BYTES,
        "resource_max_temp_bytes": PARENT_OVERLAY_MAX_TEMP_BYTES,
        "resource_max_wall_seconds": None,
        "database_read_allowed": False,
        "network_read_allowed": False,
        "qlib_read_allowed": False,
        "factor_catalog_write_allowed": False,
        "strategy_package_write_allowed": False,
        "runtime_activation_allowed": False,
        "position_weight_output_allowed": False,
        "sealed_holdout_accessed": False,
        "deployable": False,
        **values,
    }
    draft = FrozenParentIncrementalOverlayRequestV1.model_construct(
        request_id="advn3ovlreq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenParentIncrementalOverlayRequestV1(
        request_id=f"advn3ovlreq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_parent_overlay_receipt(**values: Any) -> ParentIncrementalOverlayReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_parent_incremental_overlay_receipt_v1",
        "status": "COMPLETE",
        "planned_trial_count": 24,
        "generated_trial_count": 24,
        "evaluated_trial_count": 24,
        "result_class": ResearchResultClass.EXPLORATORY,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
        "created_at": created_at,
        **values,
    }
    draft = ParentIncrementalOverlayReceiptV1.model_construct(
        receipt_id="advn3ovlrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return ParentIncrementalOverlayReceiptV1(
        receipt_id=f"advn3ovlrcpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


__all__ = [
    "FrozenParentIncrementalOverlayRequestV1",
    "PARENT_OVERLAY_CANDIDATES",
    "PARENT_OVERLAY_EXPERIMENT_ID",
    "PARENT_OVERLAY_HYPOTHESIS_FAMILY_ID",
    "PARENT_OVERLAY_TRIAL_COUNT",
    "PARENT_OVERLAY_WEIGHT_BPS",
    "ParentIncrementalOverlayReceiptV1",
    "ParentIncrementalOverlayTrialV1",
    "build_default_overlay_trials",
    "build_parent_overlay_receipt",
    "build_parent_overlay_request",
]
