from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.qe_alpha_mve_contracts import (
    DAILY_FIELDS,
    EXPRESSION_OPERATORS,
    MVE_FAMILIES,
    validate_expression,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
)
from backend.services.dataset_release.static_schema import STATIC_ORDERED_COLUMNS
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
GENERATOR_EXPERIMENT_ID = "ADVISORY-N3-QE-ALPHA-GENERATOR-MVE-V1"
GENERATOR_FAMILY_ID = "ADVISORY-N3-QE-ALPHA-GENERATOR-MVE-V1"
GENERATOR_SIGNAL_START = date(2024, 7, 4)
GENERATOR_SIGNAL_END = date(2026, 2, 2)
GENERATOR_OUTCOME_CUTOFF = date(2026, 3, 10)
GENERATOR_MAX_CALLS = 12
GENERATOR_MAX_RAW_ATTEMPTS = 48
GENERATOR_MAX_EVALUATED = 24
GENERATOR_MIN_ACCEPTED = 12
GENERATOR_MIN_PER_FAMILY = 2
GENERATOR_OVERLAY_WEIGHT = 0.10
GENERATOR_CUMULATIVE_PRIOR_TRIALS = 48
GENERATOR_BLOCK_LENGTH = 20
GENERATOR_BOOTSTRAP_REPETITIONS = 2000
GENERATOR_RANDOM_SEED = 20260903
GENERATOR_MAX_RSS_BYTES = 16 * 1024**3
GENERATOR_MAX_TEMP_BYTES = 32 * 1024**3

GENERATOR_KNOWN_EFFECTS = (
    "MOMENTUM",
    "REVERSAL",
    "SIZE",
    "TURNOVER",
    "VOLATILITY",
    "LIQUIDITY",
    "VALUE",
    "QUALITY",
    "SECTOR_BETA",
)

GENERATOR_STATIC_FIELDS = frozenset(str(item) for item in STATIC_ORDERED_COLUMNS if item != "l2_code_id")
GENERATOR_ALLOWED_FIELDS = DAILY_FIELDS | GENERATOR_STATIC_FIELDS | frozenset({"market_regime"})


def generator_allowed_fields_for_source(
    *,
    available_static_fields: set[str] | frozenset[str],
    old_source_fields: tuple[str, ...],
) -> tuple[str, ...]:
    """Freeze only fields that the concrete source can physically provide."""
    allowed = (
        DAILY_FIELDS | (GENERATOR_STATIC_FIELDS & frozenset(available_static_fields)) | frozenset({"market_regime"})
    )
    missing_old = sorted(set(old_source_fields) - allowed)
    if missing_old:
        raise ValueError(f"QE alpha generator concrete source omits old proposal fields: {missing_old}")
    return tuple(sorted(allowed))


class QEAlphaGeneratorModelIdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_qe_alpha_generator_model_identity_v1"] = (
        "advisory_qe_alpha_generator_model_identity_v1"
    )
    agent_locator: Literal["evolution_researcher"] = "evolution_researcher"
    model: Literal["deepseek/deepseek-reasoner"] = "deepseek/deepseek-reasoner"
    temperature: Literal[0.0] = 0.0
    top_p: Literal[1.0] = 1.0
    timeout_seconds: Literal[1800] = 1800
    credential_value_persisted: Literal[False] = False


class QEAlphaGeneratorProposalV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_qe_alpha_generator_proposal_v1"] = "advisory_qe_alpha_generator_proposal_v1"
    proposal_id: str = Field(pattern=r"^N3G_[A-Z_]+_[0-9]{2}$")
    family: str
    economic_hypothesis: str = Field(min_length=8, max_length=500)
    mechanism: str = Field(min_length=8, max_length=1000)
    known_effect_exposures: tuple[str, ...] = Field(min_length=1, max_length=3)
    expression: dict[str, Any]
    expression_sha256: str = Field(pattern=SHA256_PATTERN)
    source_fields: tuple[str, ...]
    direction_frozen: Literal[True] = True

    @model_validator(mode="after")
    def validate_proposal(self) -> "QEAlphaGeneratorProposalV1":
        if self.family not in MVE_FAMILIES:
            raise ValueError("QE alpha generator family is not frozen")
        if not set(self.known_effect_exposures).issubset(GENERATOR_KNOWN_EFFECTS):
            raise ValueError("QE alpha generator known-effect roster is invalid")
        if len(set(self.known_effect_exposures)) != len(self.known_effect_exposures):
            raise ValueError("QE alpha generator known-effect roster is duplicated")
        stats = validate_expression(self.expression, allowed_fields=GENERATOR_ALLOWED_FIELDS)
        if self.source_fields != tuple(sorted(stats["fields"])):
            raise ValueError("QE alpha generator source field roster drift")
        if self.expression_sha256 != canonical_json_sha256(self.expression):
            raise ValueError("QE alpha generator expression hash drift")
        return self


class FrozenAdvisoryQEAlphaGeneratorRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_qe_alpha_generator_request_v1"] = (
        "frozen_advisory_qe_alpha_generator_request_v1"
    )
    request_id: str = Field(pattern=r"^advqegenreq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: Literal[ObjectiveContract.ALPHA_RANKING] = ObjectiveContract.ALPHA_RANKING
    study_type: Literal[ResearchStudyType.EXPLORATORY_SCREEN] = ResearchStudyType.EXPLORATORY_SCREEN
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    parent_qe_bundle_path: str = Field(min_length=1)
    parent_overlay_bundle_path: str = Field(min_length=1)
    minute_bundle_path: str = Field(min_length=1)
    catalog_snapshot_path: str = Field(min_length=1)
    evidence_refs: tuple[EvidenceReferenceV1, ...]
    factor_root: str = Field(min_length=1)
    qlib_daily_root: str = Field(min_length=1)
    n2b_bundle_path: str = Field(min_length=1)
    outcomes_path: str = Field(min_length=1)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    benchmark_instrument: str = Field(min_length=1)
    signal_start: date = GENERATOR_SIGNAL_START
    signal_end: date = GENERATOR_SIGNAL_END
    outcome_cutoff: date = GENERATOR_OUTCOME_CUTOFF
    old_expression_hashes: tuple[str, ...] = Field(min_length=24, max_length=24)
    old_source_fields: tuple[str, ...]
    allowed_fields: tuple[str, ...]
    allowed_operators: tuple[str, ...]
    known_effects: tuple[str, ...]
    model_identity: QEAlphaGeneratorModelIdentityV1
    prompt_schema_version: Literal["advisory_qe_alpha_generator_prompt_v1"] = "advisory_qe_alpha_generator_prompt_v1"
    max_generation_calls: Literal[GENERATOR_MAX_CALLS] = GENERATOR_MAX_CALLS
    max_raw_generation_attempts: Literal[GENERATOR_MAX_RAW_ATTEMPTS] = GENERATOR_MAX_RAW_ATTEMPTS
    max_evaluated_expressions: Literal[GENERATOR_MAX_EVALUATED] = GENERATOR_MAX_EVALUATED
    minimum_accepted_expressions: Literal[GENERATOR_MIN_ACCEPTED] = GENERATOR_MIN_ACCEPTED
    minimum_per_family: Literal[GENERATOR_MIN_PER_FAMILY] = GENERATOR_MIN_PER_FAMILY
    overlay_weight: Literal[GENERATOR_OVERLAY_WEIGHT] = GENERATOR_OVERLAY_WEIGHT
    cumulative_prior_trial_count: Literal[GENERATOR_CUMULATIVE_PRIOR_TRIALS] = GENERATOR_CUMULATIVE_PRIOR_TRIALS
    minimum_evaluable_days: Literal[382] = 382
    minimum_finite_fraction: Literal[0.95] = 0.95
    minimum_intervention_days: Literal[96] = 96
    minimum_intervention_fraction: Literal[0.25] = 0.25
    minimum_intervention_quarters: Literal[6] = 6
    maximum_parent_spearman: Literal[0.80] = 0.80
    maximum_old_score_spearman: Literal[0.90] = 0.90
    block_length_trading_days: Literal[GENERATOR_BLOCK_LENGTH] = GENERATOR_BLOCK_LENGTH
    bootstrap_repetitions: Literal[GENERATOR_BOOTSTRAP_REPETITIONS] = GENERATOR_BOOTSTRAP_REPETITIONS
    bootstrap_seed: Literal[GENERATOR_RANDOM_SEED] = GENERATOR_RANDOM_SEED
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: Literal[GENERATOR_MAX_RSS_BYTES] = GENERATOR_MAX_RSS_BYTES
    resource_max_temp_bytes: Literal[GENERATOR_MAX_TEMP_BYTES] = GENERATOR_MAX_TEMP_BYTES
    resource_max_wall_seconds: Literal[None] = None
    phase_a_database_read_only: Literal[True] = True
    phase_b_network_generation_only: Literal[True] = True
    phase_c_database_read_allowed: Literal[False] = False
    phase_c_network_read_allowed: Literal[False] = False
    factor_catalog_write_allowed: Literal[False] = False
    strategy_package_write_allowed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenAdvisoryQEAlphaGeneratorRequestV1":
        if len(set(self.old_expression_hashes)) != 24:
            raise ValueError("QE alpha generator requires 24 unique old expression hashes")
        allowed_fields = set(self.allowed_fields)
        if self.allowed_fields != tuple(sorted(allowed_fields)):
            raise ValueError("QE alpha generator allowed field roster drift")
        if not allowed_fields.issubset(GENERATOR_ALLOWED_FIELDS):
            raise ValueError("QE alpha generator allowed field roster exceeds the implemented schema")
        required_fields = DAILY_FIELDS | frozenset({"market_regime"}) | frozenset(self.old_source_fields)
        if not required_fields.issubset(allowed_fields):
            raise ValueError("QE alpha generator allowed field roster omits required source fields")
        if self.allowed_operators != tuple(sorted(EXPRESSION_OPERATORS)):
            raise ValueError("QE alpha generator operator roster drift")
        if self.known_effects != GENERATOR_KNOWN_EFFECTS:
            raise ValueError("QE alpha generator known-effect roster drift")
        roles = tuple(item.role for item in self.evidence_refs)
        required = {
            "n3_generator_parent_qe_manifest",
            "n3_generator_parent_overlay_manifest",
            "n3_generator_minute_manifest",
            "n3_generator_catalog_snapshot",
            "n3_generator_old_proposal_roster",
        }
        if set(roles) != required or len(roles) != len(set(roles)):
            raise ValueError("QE alpha generator evidence role roster drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advqegenreq_{digest[:24]}":
            raise ValueError("QE alpha generator request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class AdvisoryQEAlphaGenerationReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_qe_alpha_generation_receipt_v1"] = "advisory_qe_alpha_generation_receipt_v1"
    receipt_id: str = Field(pattern=r"^advqegenrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE", "INCOMPLETE_SUPPORT", "INFRASTRUCTURE_FAILURE"] = "COMPLETE"
    generation_call_count: int = Field(ge=6, le=12)
    raw_generation_attempt_count: int = Field(ge=0, le=48)
    accepted_expression_count: int = Field(ge=0, le=24)
    rejected_expression_count: int = Field(ge=0, le=48)
    proposals_sha256: str = Field(pattern=SHA256_PATTERN)
    model: Literal["deepseek/deepseek-reasoner"] = "deepseek/deepseek-reasoner"
    target_or_economic_metric_exposed: Literal[False] = False
    secret_persisted: Literal[False] = False
    support_reason_codes: tuple[str, ...] = ()
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryQEAlphaGenerationReceiptV1":
        if self.raw_generation_attempt_count != self.accepted_expression_count + self.rejected_expression_count:
            raise ValueError("QE alpha generation attempt accounting is invalid")
        infrastructure_failure = "LLM_PROVIDER_CALL_FAILURE" in self.support_reason_codes
        if self.status == "COMPLETE" and self.support_reason_codes:
            raise ValueError("QE alpha generation status/support relation is invalid")
        if self.status == "INCOMPLETE_SUPPORT" and (not self.support_reason_codes or infrastructure_failure):
            raise ValueError("QE alpha generation status/support relation is invalid")
        if self.status == "INFRASTRUCTURE_FAILURE" and not infrastructure_failure:
            raise ValueError("QE alpha generation status/support relation is invalid")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advqegenrcpt_{digest[:24]}":
            raise ValueError("QE alpha generation receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


class AdvisoryQEAlphaGeneratorMVEReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_qe_alpha_generator_mve_receipt_v1"] = "advisory_qe_alpha_generator_mve_receipt_v1"
    receipt_id: str = Field(pattern=r"^advqegenmvercpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    generation_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    planned_trial_count: Literal[48] = 48
    generated_trial_count: int = Field(ge=12, le=48)
    evaluated_trial_count: int = Field(ge=12, le=24)
    selected_trial_count: int = Field(ge=0, le=1)
    selected_proposal_id: str | None
    eligible_proposal_ids: tuple[str, ...]
    result_class: Literal[ResearchResultClass.EXPLORATORY] = ResearchResultClass.EXPLORATORY
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    next_task: Literal[
        "N3_QE_ALPHA_GENERATOR_CANDIDATE_CONFIRMATION_DESIGN",
        "N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN",
    ]
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    resource_report_sha256: str = Field(pattern=SHA256_PATTERN)
    factor_catalog_written: Literal[False] = False
    strategy_package_written: Literal[False] = False
    runtime_eligible: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryQEAlphaGeneratorMVEReceiptV1":
        expected_count = 1 if self.selected_proposal_id else 0
        expected_next = (
            "N3_QE_ALPHA_GENERATOR_CANDIDATE_CONFIRMATION_DESIGN"
            if expected_count
            else "N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN"
        )
        if self.selected_trial_count != expected_count or self.next_task != expected_next:
            raise ValueError("QE alpha generator selection/next-task relation drift")
        if self.selected_proposal_id and self.selected_proposal_id not in self.eligible_proposal_ids:
            raise ValueError("QE alpha generator selected proposal is not eligible")
        if not self.evaluated_trial_count <= self.generated_trial_count <= self.planned_trial_count:
            raise ValueError("QE alpha generator trial accounting is invalid")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advqegenmvercpt_{digest[:24]}":
            raise ValueError("QE alpha generator MVE receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_generator_proposal(**values: Any) -> QEAlphaGeneratorProposalV1:
    expression = values["expression"]
    stats = validate_expression(expression, allowed_fields=GENERATOR_ALLOWED_FIELDS)
    values.setdefault("expression_sha256", canonical_json_sha256(expression))
    values.setdefault("source_fields", tuple(sorted(stats["fields"])))
    return QEAlphaGeneratorProposalV1.model_validate(values)


def build_generator_request(**values: Any) -> FrozenAdvisoryQEAlphaGeneratorRequestV1:
    payload = dict(values)
    payload.setdefault("created_at", datetime.now(timezone.utc))
    payload.setdefault("allowed_fields", tuple(sorted(GENERATOR_ALLOWED_FIELDS)))
    payload.setdefault("allowed_operators", tuple(sorted(EXPRESSION_OPERATORS)))
    payload.setdefault("known_effects", GENERATOR_KNOWN_EFFECTS)
    draft = FrozenAdvisoryQEAlphaGeneratorRequestV1.model_construct(
        request_id="advqegenreq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenAdvisoryQEAlphaGeneratorRequestV1(
        request_id=f"advqegenreq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_generation_receipt(**values: Any) -> AdvisoryQEAlphaGenerationReceiptV1:
    payload = dict(values)
    payload.setdefault("created_at", datetime.now(timezone.utc))
    draft = AdvisoryQEAlphaGenerationReceiptV1.model_construct(
        receipt_id="advqegenrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return AdvisoryQEAlphaGenerationReceiptV1(
        receipt_id=f"advqegenrcpt_{digest[:24]}", receipt_sha256=digest, **payload
    )


def build_generator_mve_receipt(**values: Any) -> AdvisoryQEAlphaGeneratorMVEReceiptV1:
    payload = dict(values)
    payload.setdefault("created_at", datetime.now(timezone.utc))
    draft = AdvisoryQEAlphaGeneratorMVEReceiptV1.model_construct(
        receipt_id="advqegenmvercpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return AdvisoryQEAlphaGeneratorMVEReceiptV1(
        receipt_id=f"advqegenmvercpt_{digest[:24]}", receipt_sha256=digest, **payload
    )


__all__ = [
    "AdvisoryQEAlphaGenerationReceiptV1",
    "AdvisoryQEAlphaGeneratorMVEReceiptV1",
    "FrozenAdvisoryQEAlphaGeneratorRequestV1",
    "GENERATOR_ALLOWED_FIELDS",
    "GENERATOR_CUMULATIVE_PRIOR_TRIALS",
    "GENERATOR_EXPERIMENT_ID",
    "GENERATOR_FAMILY_ID",
    "GENERATOR_KNOWN_EFFECTS",
    "GENERATOR_MAX_EVALUATED",
    "GENERATOR_MAX_RAW_ATTEMPTS",
    "GENERATOR_MIN_ACCEPTED",
    "GENERATOR_MIN_PER_FAMILY",
    "GENERATOR_OVERLAY_WEIGHT",
    "QEAlphaGeneratorModelIdentityV1",
    "QEAlphaGeneratorProposalV1",
    "build_generation_receipt",
    "build_generator_mve_receipt",
    "build_generator_proposal",
    "build_generator_request",
    "generator_allowed_fields_for_source",
]
