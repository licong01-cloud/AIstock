from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.quantevolver.qe_dataset_contract import (
    QE_DATASET_CONTRACT_ID,
    QE_DATASET_SIGNAL_END_DATE,
    QE_DATASET_START_DATE,
    QE_FROZEN_BIN_SNAPSHOT_ID,
    QE_FROZEN_BIN_UNIVERSE_KEY,
    QE_FROZEN_CALENDAR_SHA256,
    QE_FROZEN_INSTRUMENTS_SHA256,
    QE_FROZEN_META_EXPORT_SHA256,
    QE_FROZEN_SUSPEND_DATASET_ID,
    QE_FROZEN_SUSPEND_MANIFEST_SHA256,
    QE_FROZEN_SUSPEND_PARQUET_SHA256,
    QE_FROZEN_SUSPEND_SOURCE_CONTRACT,
    QE_FROZEN_UNIVERSE_FINGERPRINT_SHA256,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
PREPARATION_SCHEMA = "frozen_advisory_qe_alpha_mve_preparation_v1"
GRAMMAR_ID = "advisory_qe_alpha_expression_grammar_v1"
HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1"

INPUT_FAMILIES = (
    "PRICE_VOLUME_DAILY",
    "MONEYFLOW_DAILY",
    "FUNDAMENTAL_PIT",
    "SECTOR_PIT",
    "MARKET_REGIME_T_VISIBLE",
)
ALLOWED_OPERATORS = (
    "ADD",
    "SUBTRACT",
    "MULTIPLY",
    "SAFE_DIVIDE",
    "ABS",
    "SIGN",
    "LOG1P_ABS",
    "SQRT_ABS",
    "CLIP",
    "LAG",
    "DELTA",
    "TRAILING_SUM",
    "TRAILING_MEAN",
    "TRAILING_STD",
    "TRAILING_MIN",
    "TRAILING_MAX",
    "TRAILING_CORR",
    "SAME_DATE_RANK",
    "SAME_DATE_ZSCORE",
    "INDUSTRY_DEMEAN",
)
FORBIDDEN_OPERATIONS = (
    "NEGATIVE_SHIFT_OR_LEAD",
    "CENTERED_ROLLING",
    "TIME_AXIS_BACKFILL",
    "REVERSE_THEN_FORWARD_FILL",
    "FUTURE_LABEL_OR_OUTCOME_INPUT",
    "IC_RETURN_OR_EVALUATION_ARTIFACT_INPUT",
    "POST_DECISION_CUTOFF_INPUT",
    "DYNAMIC_IMPORT_EVAL_EXEC_COMPILE",
    "FILE_WRITE",
    "DATABASE_ACCESS",
    "NETWORK_ACCESS",
    "SUBPROCESS_OR_SHELL",
    "PICKLE_LOAD",
    "SILENT_OPERATOR_FALLBACK",
    "RESULT_DRIVEN_PROMPT_GRAMMAR_OR_BUDGET_CHANGE",
)
SIGNAL_FAMILIES = (
    "PRICE_VOLUME_BEHAVIOR",
    "MONEYFLOW_BEHAVIOR",
    "FUNDAMENTAL_CHANGE",
    "SECTOR_RELATIVE",
    "CROWDING_DISPERSION",
    "REGIME_CONDITIONED",
)
FUTURE_EVIDENCE_OBLIGATIONS = (
    "CANONICAL_CODE_AND_FORMULA_HASH_DEDUP",
    "FACTOR_CATALOG_IDENTITY_OVERLAP",
    "KNOWN_EFFECT_EXPOSURE_OVERLAP",
    "CURRENT_SELECTION_LEG_RESIDUAL_CORRELATION",
    "PER_SEED_AND_SEED_MEAN_STABILITY",
    "LEAVE_ONE_OUT_MARGINAL_VALUE",
    "TIME_SUBWINDOW_QUARTER_AND_REGIME_STABILITY",
    "SIGNAL_DECAY_CURVE",
    "COST_AND_TURNOVER_ADJUSTED_VALUE",
    "SHAP_AND_ECONOMIC_DIRECTION_ATTRIBUTION",
)
KNOWN_EFFECTS = (
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
PARENT_EXPERIMENT_IDS = (
    "ADVISORY-N1-TIER1-ORACLE",
    "ADVISORY-N1-TIER1-LEARNABILITY",
    "ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT",
    "ADVISORY-N2B-INDEPENDENT-PACKAGE-ALPHA-AUDIT-V2",
    "ADVISORY-N2-ENTRY-GUARD-ORACLE",
    "ADVISORY-N2-EXIT-LABEL-ORACLE",
)


class QEAlphaPreparationOperation(str, Enum):
    BUILD = "BUILD"
    INSPECT = "INSPECT"
    GENERATE = "GENERATE"
    EXECUTE = "EXECUTE"
    ECONOMIC_EVALUATE = "ECONOMIC_EVALUATE"
    APPEND_REGISTRY = "APPEND_REGISTRY"


class QEAlphaDataIdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_qe_alpha_data_identity_v1"] = "advisory_qe_alpha_data_identity_v1"
    qe_dataset_contract_id: str = Field(min_length=1)
    signal_start_date: date
    signal_end_date: date
    qlib_bin_snapshot_id: str = Field(min_length=1)
    qlib_bin_universe_key: str = Field(min_length=1)
    qlib_instruments_sha256: str = Field(pattern=SHA256_PATTERN)
    qlib_calendar_sha256: str = Field(pattern=SHA256_PATTERN)
    qlib_meta_export_sha256: str = Field(pattern=SHA256_PATTERN)
    frozen_universe_fingerprint_sha256: str = Field(pattern=SHA256_PATTERN)
    suspend_dataset_id: str = Field(min_length=1)
    suspend_parquet_sha256: str = Field(pattern=SHA256_PATTERN)
    suspend_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    suspend_source_contract: str = Field(min_length=1)
    canonical_pit_required: Literal[True] = True
    immutable_release_required: Literal[True] = True
    database_read_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    rolling_latest_allowed: Literal[False] = False
    research_window_authorized: Literal[False] = False

    @model_validator(mode="after")
    def validate_current_source_identity(self) -> "QEAlphaDataIdentityV1":
        expected = _current_data_identity_payload()
        actual = self.model_dump(mode="json")
        if actual != expected:
            raise ValueError("QE alpha preparation data identity differs from current frozen QE pins")
        return self


class QEAlphaExpressionPolicyV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_qe_alpha_expression_policy_v1"] = "advisory_qe_alpha_expression_policy_v1"
    grammar_id: Literal[GRAMMAR_ID] = GRAMMAR_ID
    input_families: tuple[str, ...]
    allowed_operators: tuple[str, ...]
    forbidden_operations: tuple[str, ...]
    minimum_lag: Literal[1] = 1
    maximum_lag: Literal[252] = 252
    minimum_trailing_window: Literal[2] = 2
    maximum_trailing_window: Literal[252] = 252
    rolling_center_allowed: Literal[False] = False
    maximum_ast_nodes: Literal[64] = 64
    maximum_ast_depth: Literal[8] = 8
    maximum_raw_fields: Literal[8] = 8
    finite_division_operator: Literal["SAFE_DIVIDE"] = "SAFE_DIVIDE"
    compiler_implemented: Literal[False] = False
    silent_fallback_allowed: Literal[False] = False

    @model_validator(mode="after")
    def validate_rosters(self) -> "QEAlphaExpressionPolicyV1":
        if self.input_families != INPUT_FAMILIES:
            raise ValueError("QE alpha input family roster drift")
        if self.allowed_operators != ALLOWED_OPERATORS:
            raise ValueError("QE alpha operator allowlist drift")
        if self.forbidden_operations != FORBIDDEN_OPERATIONS:
            raise ValueError("QE alpha forbidden-operation roster drift")
        return self


class QEAlphaBudgetV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_qe_alpha_budget_v1"] = "advisory_qe_alpha_budget_v1"
    signal_families: tuple[str, ...]
    proposals_per_family: Literal[4] = 4
    total_proposal_budget: Literal[24] = 24
    current_generated_trial_count: Literal[0] = 0
    current_evaluated_trial_count: Literal[0] = 0
    current_selected_trial_count: Literal[0] = 0
    future_execution_concurrency: Literal[1] = 1
    future_resource_max_rss_bytes: Literal[17179869184] = 17179869184
    future_resource_max_temp_bytes: Literal[34359738368] = 34359738368
    future_resource_max_wall_seconds: Literal[None] = None
    failed_generation_counts_as_attempt: Literal[True] = True
    result_driven_budget_expansion_allowed: Literal[False] = False

    @model_validator(mode="after")
    def validate_budget(self) -> "QEAlphaBudgetV1":
        if self.signal_families != SIGNAL_FAMILIES:
            raise ValueError("QE alpha signal family roster drift")
        if len(self.signal_families) * self.proposals_per_family != self.total_proposal_budget:
            raise ValueError("QE alpha proposal family budget does not sum to total budget")
        return self


class QEAlphaFutureEvidenceObligationsV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_qe_alpha_future_evidence_obligations_v1"] = (
        "advisory_qe_alpha_future_evidence_obligations_v1"
    )
    obligations: tuple[str, ...]
    known_effects: tuple[str, ...]
    exact_duplicate_is_hard_failure: Literal[True] = True
    pit_violation_is_hard_failure: Literal[True] = True
    positive_result_is_activation_evidence: Literal[False] = False
    universal_post_result_cutoff_enabled: Literal[False] = False
    frontier_reporting_required: Literal[True] = True

    @model_validator(mode="after")
    def validate_obligations(self) -> "QEAlphaFutureEvidenceObligationsV1":
        if self.obligations != FUTURE_EVIDENCE_OBLIGATIONS:
            raise ValueError("QE alpha future evidence obligations drift")
        if self.known_effects != KNOWN_EFFECTS:
            raise ValueError("QE alpha known-effect roster drift")
        return self


class QEAlphaFutureLineageTemplateV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_qe_alpha_future_lineage_template_v1"] = (
        "advisory_qe_alpha_future_lineage_template_v1"
    )
    hypothesis_family_id: Literal[HYPOTHESIS_FAMILY_ID] = HYPOTHESIS_FAMILY_ID
    parent_experiment_ids: tuple[str, ...]
    objective_contract: Literal["ALPHA_RANKING"] = "ALPHA_RANKING"
    future_study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    future_decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    future_planned_trial_count: Literal[24] = 24
    sealed_holdout_accessed: Literal[False] = False
    preparation_is_trial_record: Literal[False] = False
    evidence_refs_allowed_in_preparation: Literal[False] = False
    exact_retry_requires_same_candidate_identity: Literal[True] = True

    @model_validator(mode="after")
    def validate_lineage(self) -> "QEAlphaFutureLineageTemplateV1":
        if self.parent_experiment_ids != PARENT_EXPERIMENT_IDS:
            raise ValueError("QE alpha future parent lineage drift")
        return self


class FrozenAdvisoryQEAlphaMVEPreparationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[PREPARATION_SCHEMA] = PREPARATION_SCHEMA
    preparation_id: str = Field(pattern=r"^advqeprep_[0-9a-f]{24}$")
    preparation_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    status: Literal["PREPARATION_ONLY_NO_RESEARCH_EVIDENCE"] = "PREPARATION_ONLY_NO_RESEARCH_EVIDENCE"
    data_identity: QEAlphaDataIdentityV1
    expression_policy: QEAlphaExpressionPolicyV1
    budget: QEAlphaBudgetV1
    future_evidence: QEAlphaFutureEvidenceObligationsV1
    future_lineage: QEAlphaFutureLineageTemplateV1
    generation_authorized: Literal[False] = False
    execution_authorized: Literal[False] = False
    economic_evaluation_authorized: Literal[False] = False
    registry_append_authorized: Literal[False] = False
    research_evidence_produced: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryQEAlphaMVEPreparationV1":
        digest = canonical_json_sha256(self.functional_payload())
        if self.preparation_sha256 != digest or self.preparation_id != f"advqeprep_{digest[:24]}":
            raise ValueError("QE alpha preparation identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"preparation_id", "preparation_sha256", "created_at"},
        )


def build_default_qe_alpha_mve_preparation(
    *,
    created_at: datetime | None = None,
) -> FrozenAdvisoryQEAlphaMVEPreparationV1:
    payload = {
        "schema_version": PREPARATION_SCHEMA,
        "created_at": created_at or datetime.now(timezone.utc),
        "status": "PREPARATION_ONLY_NO_RESEARCH_EVIDENCE",
        "data_identity": QEAlphaDataIdentityV1.model_validate(_current_data_identity_payload()),
        "expression_policy": QEAlphaExpressionPolicyV1(
            input_families=INPUT_FAMILIES,
            allowed_operators=ALLOWED_OPERATORS,
            forbidden_operations=FORBIDDEN_OPERATIONS,
        ),
        "budget": QEAlphaBudgetV1(signal_families=SIGNAL_FAMILIES),
        "future_evidence": QEAlphaFutureEvidenceObligationsV1(
            obligations=FUTURE_EVIDENCE_OBLIGATIONS,
            known_effects=KNOWN_EFFECTS,
        ),
        "future_lineage": QEAlphaFutureLineageTemplateV1(
            parent_experiment_ids=PARENT_EXPERIMENT_IDS,
        ),
        "generation_authorized": False,
        "execution_authorized": False,
        "economic_evaluation_authorized": False,
        "registry_append_authorized": False,
        "research_evidence_produced": False,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    draft = FrozenAdvisoryQEAlphaMVEPreparationV1.model_construct(
        preparation_id="advqeprep_" + "0" * 24,
        preparation_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenAdvisoryQEAlphaMVEPreparationV1(
        preparation_id=f"advqeprep_{digest[:24]}",
        preparation_sha256=digest,
        **payload,
    )


def require_preparation_operation(
    preparation: FrozenAdvisoryQEAlphaMVEPreparationV1,
    operation: QEAlphaPreparationOperation | str,
) -> None:
    try:
        normalized = QEAlphaPreparationOperation(operation)
    except (TypeError, ValueError):
        _raise(
            "QE alpha MVE preparation operation is invalid",
            "ADVISORY_QE_ALPHA_PREPARATION_INVALID",
            operation=str(operation),
        )
    if normalized in {QEAlphaPreparationOperation.BUILD, QEAlphaPreparationOperation.INSPECT}:
        return
    reason_by_operation = {
        QEAlphaPreparationOperation.GENERATE: "ADVISORY_QE_ALPHA_GENERATION_NOT_AUTHORIZED",
        QEAlphaPreparationOperation.EXECUTE: "ADVISORY_QE_ALPHA_GENERATION_NOT_AUTHORIZED",
        QEAlphaPreparationOperation.ECONOMIC_EVALUATE: "ADVISORY_QE_ALPHA_EVALUATION_NOT_AUTHORIZED",
        QEAlphaPreparationOperation.APPEND_REGISTRY: "ADVISORY_QE_ALPHA_REGISTRY_APPEND_NOT_AUTHORIZED",
    }
    _raise(
        "QE alpha MVE preparation does not authorize research execution",
        reason_by_operation[normalized],
        preparation_id=preparation.preparation_id,
        operation=normalized.value,
    )


def write_qe_alpha_mve_preparation(
    path: str | Path,
    preparation: FrozenAdvisoryQEAlphaMVEPreparationV1,
) -> dict[str, Any]:
    try:
        preparation = FrozenAdvisoryQEAlphaMVEPreparationV1.model_validate(preparation.model_dump(mode="json"))
    except Exception as exc:
        _raise(
            "QE alpha preparation object is invalid",
            "ADVISORY_QE_ALPHA_PREPARATION_INVALID",
            error_type=type(exc).__name__,
        )
    require_preparation_operation(preparation, QEAlphaPreparationOperation.BUILD)
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        try:
            existing = load_qe_alpha_mve_preparation(target)
        except AdvisoryModelFirstError as exc:
            _raise(
                "QE alpha preparation path contains invalid content",
                "ADVISORY_QE_ALPHA_PREPARATION_CONFLICT",
                path=target.as_posix(),
                source_reason_code=exc.reason_code,
            )
        if existing.preparation_sha256 != preparation.preparation_sha256:
            _raise(
                "QE alpha preparation path contains a different identity",
                "ADVISORY_QE_ALPHA_PREPARATION_CONFLICT",
                path=target.as_posix(),
            )
        return _write_response(target, preparation, status="EXISTING_PREPARATION")
    encoded = json.dumps(
        preparation.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    loaded = load_qe_alpha_mve_preparation(target)
    return _write_response(target, loaded, status="PREPARATION_WRITTEN")


def load_qe_alpha_mve_preparation(path: str | Path) -> FrozenAdvisoryQEAlphaMVEPreparationV1:
    target = Path(path).resolve()
    try:
        preparation = FrozenAdvisoryQEAlphaMVEPreparationV1.model_validate_json(target.read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "QE alpha preparation cannot be read or validated",
            "ADVISORY_QE_ALPHA_PREPARATION_INVALID",
            path=target.as_posix(),
            error_type=type(exc).__name__,
        )
    require_preparation_operation(preparation, QEAlphaPreparationOperation.INSPECT)
    return preparation


def inspect_qe_alpha_mve_preparation(path: str | Path) -> dict[str, Any]:
    target = Path(path).resolve()
    preparation = load_qe_alpha_mve_preparation(target)
    return _write_response(target, preparation, status="VALID_PREPARATION")


def _current_data_identity_payload() -> dict[str, Any]:
    return {
        "schema_version": "advisory_qe_alpha_data_identity_v1",
        "qe_dataset_contract_id": QE_DATASET_CONTRACT_ID,
        "signal_start_date": QE_DATASET_START_DATE.isoformat(),
        "signal_end_date": QE_DATASET_SIGNAL_END_DATE.isoformat(),
        "qlib_bin_snapshot_id": QE_FROZEN_BIN_SNAPSHOT_ID,
        "qlib_bin_universe_key": QE_FROZEN_BIN_UNIVERSE_KEY,
        "qlib_instruments_sha256": QE_FROZEN_INSTRUMENTS_SHA256,
        "qlib_calendar_sha256": QE_FROZEN_CALENDAR_SHA256,
        "qlib_meta_export_sha256": QE_FROZEN_META_EXPORT_SHA256,
        "frozen_universe_fingerprint_sha256": QE_FROZEN_UNIVERSE_FINGERPRINT_SHA256,
        "suspend_dataset_id": QE_FROZEN_SUSPEND_DATASET_ID,
        "suspend_parquet_sha256": QE_FROZEN_SUSPEND_PARQUET_SHA256,
        "suspend_manifest_sha256": QE_FROZEN_SUSPEND_MANIFEST_SHA256,
        "suspend_source_contract": QE_FROZEN_SUSPEND_SOURCE_CONTRACT,
        "canonical_pit_required": True,
        "immutable_release_required": True,
        "database_read_allowed": False,
        "network_read_allowed": False,
        "rolling_latest_allowed": False,
        "research_window_authorized": False,
    }


def _write_response(
    path: Path,
    preparation: FrozenAdvisoryQEAlphaMVEPreparationV1,
    *,
    status: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "preparation_id": preparation.preparation_id,
        "preparation_sha256": preparation.preparation_sha256,
        "total_proposal_budget": preparation.budget.total_proposal_budget,
        "generation_authorized": False,
        "economic_evaluation_authorized": False,
        "registry_append_authorized": False,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "output_path": path.as_posix(),
    }


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "ALLOWED_OPERATORS",
    "FORBIDDEN_OPERATIONS",
    "FUTURE_EVIDENCE_OBLIGATIONS",
    "INPUT_FAMILIES",
    "KNOWN_EFFECTS",
    "PARENT_EXPERIMENT_IDS",
    "SIGNAL_FAMILIES",
    "FrozenAdvisoryQEAlphaMVEPreparationV1",
    "QEAlphaPreparationOperation",
    "build_default_qe_alpha_mve_preparation",
    "inspect_qe_alpha_mve_preparation",
    "load_qe_alpha_mve_preparation",
    "require_preparation_operation",
    "write_qe_alpha_mve_preparation",
]
