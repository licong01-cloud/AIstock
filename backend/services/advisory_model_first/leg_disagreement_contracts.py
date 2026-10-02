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
LEG_MVE_EXPERIMENT_ID = "ADVISORY-N3-LEG-DISAGREEMENT-LEARNABILITY-V1"
LEG_MVE_HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-LEG-DISAGREEMENT-LEARNABILITY-V1"
LEG_MVE_SIGNAL_START = date(2024, 7, 4)
LEG_MVE_SIGNAL_END = date(2026, 2, 2)
LEG_MVE_SOURCE_ROW_COUNT = 1_710_301
LEG_MVE_KNOWN_ROW_COUNT = 1_709_387
LEG_MVE_EVALUABLE_ROW_COUNT = 1_705_332
LEG_MVE_NONFINITE_KNOWN_ROW_COUNT = 4_055
LEG_MVE_UNKNOWN_ROW_COUNT = 914
LEG_MVE_DECISION_DATE_COUNT = 386
LEG_MVE_MODEL_TRIAL_COUNT = 2
LEG_MVE_FAMILYWISE_HYPOTHESIS_COUNT = 4
LEG_MVE_EXPECTED_PATH_COUNT = 28
LEG_MVE_EXPECTED_OOF_PER_ROW = 7
LEG_MVE_BLOCK_LENGTH = 20
LEG_MVE_BOOTSTRAP_REPETITIONS = 2000
LEG_MVE_BOOTSTRAP_SEED = 20260902
LEG_MVE_MAX_RSS_BYTES = 16 * 1024**3
LEG_MVE_MAX_TEMP_BYTES = 16 * 1024**3

LEG_MVE_COMPARATOR_FEATURES = (
    "parent_rank_pct",
    "lstm_rank_pct",
    "fund_rank_pct",
)
LEG_MVE_EXPANDED_FEATURES = (
    *LEG_MVE_COMPARATOR_FEATURES,
    "leg_rank_signed_gap",
    "leg_rank_abs_gap",
    "leg_rank_consensus_min",
    "leg_rank_consensus_product",
    "parent_rank_x_agreement",
)
LEG_MVE_FEATURE_SCHEMA_VERSION = "advisory_n3_leg_disagreement_feature_schema_v1"
LEG_MVE_FEATURE_SCHEMA_HASH = canonical_json_sha256(
    {
        "schema_version": LEG_MVE_FEATURE_SCHEMA_VERSION,
        "comparator_features": list(LEG_MVE_COMPARATOR_FEATURES),
        "expanded_features": list(LEG_MVE_EXPANDED_FEATURES),
        "rank_semantics": "SAME_DATE_CANONICAL_MEMBER_AVERAGE_PCT_ASCENDING",
    }
)


class LegDisagreementModelTrialV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_leg_disagreement_model_trial_v1"] = (
        "advisory_n3_leg_disagreement_model_trial_v1"
    )
    trial_id: Literal[
        "N3_LEG_LINEAR_COMPARATOR_V1",
        "N3_LEG_DISAGREEMENT_EXPANDED_V1",
    ]
    role: Literal["COMPARATOR", "CANDIDATE"]
    feature_columns: tuple[str, ...]
    estimator_family: Literal["SKLEARN_RIDGE_V1"] = "SKLEARN_RIDGE_V1"
    alpha: float = Field(default=100.0, ge=0.0)
    solver: Literal["lsqr"] = "lsqr"
    fit_intercept: Literal[True] = True
    numeric_scaler: Literal["TRAIN_FOLD_STANDARD_SCALER"] = "TRAIN_FOLD_STANDARD_SCALER"
    direction_frozen: Literal[True] = True

    @model_validator(mode="after")
    def validate_trial(self) -> "LegDisagreementModelTrialV1":
        expected = {
            "N3_LEG_LINEAR_COMPARATOR_V1": ("COMPARATOR", LEG_MVE_COMPARATOR_FEATURES),
            "N3_LEG_DISAGREEMENT_EXPANDED_V1": ("CANDIDATE", LEG_MVE_EXPANDED_FEATURES),
        }[self.trial_id]
        if self.role != expected[0] or self.feature_columns != expected[1] or self.alpha != 100.0:
            raise ValueError("leg disagreement model trial identity drift")
        return self


def build_default_leg_model_trials() -> tuple[LegDisagreementModelTrialV1, ...]:
    return (
        LegDisagreementModelTrialV1(
            trial_id="N3_LEG_LINEAR_COMPARATOR_V1",
            role="COMPARATOR",
            feature_columns=LEG_MVE_COMPARATOR_FEATURES,
        ),
        LegDisagreementModelTrialV1(
            trial_id="N3_LEG_DISAGREEMENT_EXPANDED_V1",
            role="CANDIDATE",
            feature_columns=LEG_MVE_EXPANDED_FEATURES,
        ),
    )


class FrozenLegDisagreementRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_n3_leg_disagreement_request_v1"] = (
        "frozen_advisory_n3_leg_disagreement_request_v1"
    )
    request_id: str = Field(pattern=r"^advn3legreq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: ObjectiveContract = ObjectiveContract.ALPHA_RANKING
    study_type: ResearchStudyType = ResearchStudyType.LEARNABILITY_AUDIT
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    planned_trial_count: Literal[2] = 2
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    model_trials: tuple[LegDisagreementModelTrialV1, ...]
    feature_schema_version: Literal["advisory_n3_leg_disagreement_feature_schema_v1"] = (
        "advisory_n3_leg_disagreement_feature_schema_v1"
    )
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    evidence_refs: tuple[EvidenceReferenceV1, ...]
    parent_overlay_bundle_path: str = Field(min_length=1)
    parent_overlay_bundle_id: str = Field(pattern=SHA256_PATTERN)
    parent_overlay_request_sha256: str = Field(pattern=SHA256_PATTERN)
    parent_overlay_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    n2a_bundle_path: str = Field(min_length=1)
    n2a_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n2a_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n2a_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_bundle_path: str = Field(min_length=1)
    n1_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n1_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_split_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    source_dataset_identity: str = Field(pattern=SHA256_PATTERN)
    parent_dataset_identity: str = Field(pattern=SHA256_PATTERN)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    signal_start: date = LEG_MVE_SIGNAL_START
    signal_end: date = LEG_MVE_SIGNAL_END
    expected_source_row_count: int = Field(default=LEG_MVE_SOURCE_ROW_COUNT, gt=0)
    expected_known_row_count: int = Field(default=LEG_MVE_KNOWN_ROW_COUNT, gt=0)
    expected_evaluable_row_count: int = Field(default=LEG_MVE_EVALUABLE_ROW_COUNT, gt=0)
    expected_nonfinite_known_row_count: int = Field(default=LEG_MVE_NONFINITE_KNOWN_ROW_COUNT, ge=0)
    expected_unknown_row_count: int = Field(default=LEG_MVE_UNKNOWN_ROW_COUNT, ge=0)
    expected_decision_date_count: int = Field(default=LEG_MVE_DECISION_DATE_COUNT, gt=0)
    minimum_evaluable_days: int = Field(default=382, gt=0)
    minimum_intervention_days: int = Field(default=60, ge=0)
    minimum_intervention_fraction: float = Field(default=0.25, ge=0.0, le=1.0)
    minimum_intervention_days_per_regime: int = Field(default=20, ge=0)
    minimum_parent_lift_bps: float = 5.0
    expected_ready_path_count: int = Field(default=LEG_MVE_EXPECTED_PATH_COUNT, gt=0)
    expected_oof_predictions_per_row: int = Field(default=LEG_MVE_EXPECTED_OOF_PER_ROW, gt=0)
    familywise_hypothesis_count: int = Field(default=LEG_MVE_FAMILYWISE_HYPOTHESIS_COUNT, gt=0)
    block_length_trading_days: int = Field(default=LEG_MVE_BLOCK_LENGTH, gt=0)
    bootstrap_repetitions: int = Field(default=LEG_MVE_BOOTSTRAP_REPETITIONS, gt=0)
    bootstrap_seed: int = LEG_MVE_BOOTSTRAP_SEED
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: int = Field(default=LEG_MVE_MAX_RSS_BYTES, gt=0)
    resource_max_temp_bytes: int = Field(default=LEG_MVE_MAX_TEMP_BYTES, gt=0)
    resource_max_wall_seconds: Literal[None] = None
    database_read_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    qlib_read_allowed: Literal[False] = False
    minute_data_read_allowed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    factor_catalog_write_allowed: Literal[False] = False
    strategy_package_write_allowed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    position_weight_output_allowed: Literal[False] = False
    final_model_output_allowed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenLegDisagreementRequestV1":
        if (
            self.objective_contract != ObjectiveContract.ALPHA_RANKING
            or self.study_type != ResearchStudyType.LEARNABILITY_AUDIT
            or self.decision_use != DecisionUse.NAVIGATION_ONLY
            or self.model_trials != build_default_leg_model_trials()
            or self.feature_schema_hash != LEG_MVE_FEATURE_SCHEMA_HASH
            or self.signal_start != LEG_MVE_SIGNAL_START
            or self.signal_end != LEG_MVE_SIGNAL_END
            or self.resource_max_rss_bytes != LEG_MVE_MAX_RSS_BYTES
            or self.resource_max_temp_bytes != LEG_MVE_MAX_TEMP_BYTES
        ):
            raise ValueError("leg disagreement research contract drift")
        frozen_numeric = {
            "expected_source_row_count": LEG_MVE_SOURCE_ROW_COUNT,
            "expected_known_row_count": LEG_MVE_KNOWN_ROW_COUNT,
            "expected_evaluable_row_count": LEG_MVE_EVALUABLE_ROW_COUNT,
            "expected_nonfinite_known_row_count": LEG_MVE_NONFINITE_KNOWN_ROW_COUNT,
            "expected_unknown_row_count": LEG_MVE_UNKNOWN_ROW_COUNT,
            "expected_decision_date_count": LEG_MVE_DECISION_DATE_COUNT,
            "minimum_evaluable_days": 382,
            "minimum_intervention_days": 60,
            "minimum_intervention_fraction": 0.25,
            "minimum_intervention_days_per_regime": 20,
            "minimum_parent_lift_bps": 5.0,
            "expected_ready_path_count": LEG_MVE_EXPECTED_PATH_COUNT,
            "expected_oof_predictions_per_row": LEG_MVE_EXPECTED_OOF_PER_ROW,
            "familywise_hypothesis_count": LEG_MVE_FAMILYWISE_HYPOTHESIS_COUNT,
            "block_length_trading_days": LEG_MVE_BLOCK_LENGTH,
            "bootstrap_repetitions": LEG_MVE_BOOTSTRAP_REPETITIONS,
            "bootstrap_seed": LEG_MVE_BOOTSTRAP_SEED,
        }
        if any(getattr(self, name) != value for name, value in frozen_numeric.items()):
            raise ValueError("leg disagreement frozen threshold/count drift")
        roles = [item.role for item in self.evidence_refs]
        required_roles = {
            "n3_leg_parent_overlay_manifest",
            "n3_leg_parent_overlay_receipt",
            "n3_leg_parent_qe_score_panel",
            "n3_leg_n2a_manifest",
            "n3_leg_n2a_request",
            "n3_leg_n2a_full_universe",
            "n3_leg_n1_manifest",
            "n3_leg_n1_cpcv",
            "n3_leg_n1_regime_daily",
        }
        if len(roles) != len(set(roles)) or set(roles) != required_roles:
            raise ValueError("leg disagreement evidence role roster drift")
        for bundle_path, bundle_id in (
            (self.parent_overlay_bundle_path, self.parent_overlay_bundle_id),
            (self.n2a_bundle_path, self.n2a_bundle_id),
            (self.n1_bundle_path, self.n1_bundle_id),
        ):
            if bundle_path.replace("\\", "/").rstrip("/").split("/")[-1] != bundle_id:
                raise ValueError("leg disagreement bundle path/id drift")
        expected_dataset = canonical_json_sha256(
            {
                "source_dataset_identity": self.source_dataset_identity,
                "parent_dataset_identity": self.parent_dataset_identity,
                "n1_split_policy_sha256": self.n1_split_policy_sha256,
                "evidence_refs": [item.model_dump(mode="json") for item in self.evidence_refs],
            }
        )
        if self.dataset_identity != expected_dataset:
            raise ValueError("leg disagreement composite dataset identity drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advn3legreq_{digest[:24]}":
            raise ValueError("leg disagreement request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class LegDisagreementReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_leg_disagreement_receipt_v1"] = "advisory_n3_leg_disagreement_receipt_v1"
    receipt_id: str = Field(pattern=r"^advn3legrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    planned_trial_count: Literal[2] = 2
    generated_trial_count: Literal[2] = 2
    evaluated_trial_count: Literal[2] = 2
    selected_trial_count: int = Field(ge=0, le=1)
    selected_trial_id: Literal["N3_LEG_DISAGREEMENT_EXPANDED_V1"] | None
    eligible_trial_ids: tuple[str, ...]
    result_class: ResearchResultClass = ResearchResultClass.EXPLORATORY
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    next_task: Literal[
        "N3_LEG_DISAGREEMENT_CONFIRMATION_DESIGN",
        "N3_MINUTE_INFORMATION_SET_MVE",
    ]
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    resource_report_sha256: str = Field(pattern=SHA256_PATTERN)
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    runtime_eligible: Literal[False] = False
    final_model_written: Literal[False] = False
    factor_catalog_written: Literal[False] = False
    strategy_package_written: Literal[False] = False
    position_weight_output: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "LegDisagreementReceiptV1":
        if self.result_class != ResearchResultClass.EXPLORATORY or self.decision_use != DecisionUse.NAVIGATION_ONLY:
            raise ValueError("leg disagreement receipt research contract drift")
        selected_count = 1 if self.selected_trial_id else 0
        expected_next = "N3_LEG_DISAGREEMENT_CONFIRMATION_DESIGN" if selected_count else "N3_MINUTE_INFORMATION_SET_MVE"
        if self.selected_trial_count != selected_count or self.next_task != expected_next:
            raise ValueError("leg disagreement selection/next-task relation drift")
        expected_eligible = ("N3_LEG_DISAGREEMENT_EXPANDED_V1",) if selected_count else ()
        if self.eligible_trial_ids != expected_eligible:
            raise ValueError("leg disagreement eligible trial roster drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advn3legrcpt_{digest[:24]}":
            raise ValueError("leg disagreement receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_leg_disagreement_request(**values: Any) -> FrozenLegDisagreementRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_n3_leg_disagreement_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "planned_trial_count": 2,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "model_trials": build_default_leg_model_trials(),
        "feature_schema_version": LEG_MVE_FEATURE_SCHEMA_VERSION,
        "feature_schema_hash": LEG_MVE_FEATURE_SCHEMA_HASH,
        "resource_max_rss_bytes": LEG_MVE_MAX_RSS_BYTES,
        "resource_max_temp_bytes": LEG_MVE_MAX_TEMP_BYTES,
        "resource_max_wall_seconds": None,
        "database_read_allowed": False,
        "network_read_allowed": False,
        "qlib_read_allowed": False,
        "minute_data_read_allowed": False,
        "sealed_holdout_accessed": False,
        "factor_catalog_write_allowed": False,
        "strategy_package_write_allowed": False,
        "runtime_activation_allowed": False,
        "position_weight_output_allowed": False,
        "final_model_output_allowed": False,
        "deployable": False,
        **values,
    }
    draft = FrozenLegDisagreementRequestV1.model_construct(
        request_id="advn3legreq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenLegDisagreementRequestV1(
        request_id=f"advn3legreq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_leg_disagreement_receipt(**values: Any) -> LegDisagreementReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_n3_leg_disagreement_receipt_v1",
        "status": "COMPLETE",
        "planned_trial_count": 2,
        "generated_trial_count": 2,
        "evaluated_trial_count": 2,
        "result_class": ResearchResultClass.EXPLORATORY,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "final_model_written": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
        "created_at": created_at,
        **values,
    }
    draft = LegDisagreementReceiptV1.model_construct(
        receipt_id="advn3legrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return LegDisagreementReceiptV1(
        receipt_id=f"advn3legrcpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


__all__ = [
    "FrozenLegDisagreementRequestV1",
    "LEG_MVE_COMPARATOR_FEATURES",
    "LEG_MVE_EXPERIMENT_ID",
    "LEG_MVE_EVALUABLE_ROW_COUNT",
    "LEG_MVE_EXPANDED_FEATURES",
    "LEG_MVE_FEATURE_SCHEMA_HASH",
    "LEG_MVE_FEATURE_SCHEMA_VERSION",
    "LEG_MVE_HYPOTHESIS_FAMILY_ID",
    "LEG_MVE_KNOWN_ROW_COUNT",
    "LEG_MVE_SOURCE_ROW_COUNT",
    "LegDisagreementModelTrialV1",
    "LegDisagreementReceiptV1",
    "build_default_leg_model_trials",
    "build_leg_disagreement_receipt",
    "build_leg_disagreement_request",
]
