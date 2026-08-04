from __future__ import annotations

import json
import math
import os
import struct
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from backend.services.hmm_risk.b3_acceptance import (
    D3_CONTRACT_VERSION,
    D4_COVARIANCE_VERSION,
    L2_RETRAIN_VERSION,
    RESTART_SCHEDULE,
)
from backend.services.hmm_risk.b3_training import (
    B3CoreFitEvidence,
    B3TrainingStageError,
    B3TrainOnlySeries,
    REFIT03_RAW_COVARIANCE_AUTHORITY,
    REFIT03_RAW_COVARIANCE_SCHEMA_VERSION,
    REFIT03_STAGE_EVIDENCE_SCHEMA_VERSION,
    fit_b3_preprocessed_train_only,
    formal_b3_parameter_profile,
)
from backend.services.hmm_risk.state_model_set import (
    ALL_CORE_FEATURES,
    StateModelSetError,
    _apply_preprocess,
    _float64_array_identity,
    canonical_json_bytes,
    canonical_sha256,
    sha256_bytes,
)


ALGORITHM_VERSION = "hmm_risk_c008_b3_d1_inactive_dimension_v1"
ATTEMPT_SCHEMA_VERSION_V1 = "hmm_risk_c008_b3_d1_controlled_attempt_v1"
ATTEMPT_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_attempt_v2"
PROJECTION_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_projection_receipt_v2"
INPUT_MIGRATION_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_input_migration_receipt_v1"
C010_A5_LINEAGE_MIGRATION_SCHEMA_VERSION = "hmm_risk_c010_a5_execution_lineage_migration_v1"
PROCESS_SCHEMA_VERSION_V1 = "hmm_risk_c008_b3_d1_controlled_process_v1"
PROCESS_SCHEMA_VERSION_V2 = "hmm_risk_c008_b3_d1_controlled_process_v2"
PROCESS_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_process_v3"
REPORT_SCHEMA_VERSION_V1 = "hmm_risk_c008_b3_d1_controlled_refit_report_v1"
REPORT_SCHEMA_VERSION_V2 = "hmm_risk_c008_b3_d1_controlled_refit_report_v2"
REPORT_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_refit_report_v3"
REFIT02_ALGORITHM_VERSION = "hmm_risk_c008_b3_d1_refit_02_a_v1"
REFIT02_AUTHORITY_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_current_a5_experiment_authority_v1"
REFIT02_ATTEMPT_SCHEMA_VERSION_ORIGINAL = "hmm_risk_c008_b3_d1_controlled_attempt_v3"
REFIT02_ATTEMPT_SCHEMA_VERSION_LEGACY = "hmm_risk_c008_b3_d1_controlled_attempt_v4"
REFIT02_ATTEMPT_SCHEMA_VERSION_MATCHED_FIT = "hmm_risk_c008_b3_d1_controlled_attempt_v5"
REFIT02_ATTEMPT_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_attempt_v6"
REFIT02_PROCESS_SCHEMA_VERSION_ORIGINAL = "hmm_risk_c008_b3_d1_controlled_process_v4"
REFIT02_PROCESS_SCHEMA_VERSION_LEGACY = "hmm_risk_c008_b3_d1_controlled_process_v5"
REFIT02_PROCESS_SCHEMA_VERSION_MATCHED_FIT = "hmm_risk_c008_b3_d1_controlled_process_v6"
REFIT02_PROCESS_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_process_v7"
REFIT02_REPORT_SCHEMA_VERSION_ORIGINAL = "hmm_risk_c008_b3_d1_controlled_refit_report_v4"
REFIT02_REPORT_SCHEMA_VERSION_LEGACY = "hmm_risk_c008_b3_d1_controlled_refit_report_v5"
REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT = "hmm_risk_c008_b3_d1_controlled_refit_report_v6"
REFIT02_REPORT_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_refit_report_v7"
REFIT02_DIAGNOSTIC_CONTRACT_LEGACY = "C-008-B3-REMEDIATION-D1-B-REFIT-02-A"
REFIT02_DIAGNOSTIC_CONTRACT_MATCHED_FIT = "C-008-B3-REMEDIATION-D1-B-REFIT-02-B"
REFIT02_DIAGNOSTIC_CONTRACT = "C-008-B3-D1-REFIT-03-COVARIANCE-DIAG-01"
REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY = "hmm_risk_c008_b3_d1_refit02_fit_budget_v1"
REFIT02_FIT_BUDGET_CONTRACT_VERSION = "hmm_risk_c008_b3_d1_refit02_fit_budget_v2"
REFIT03_COVARIANCE_EVIDENCE_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_covariance_evidence_v1"
REFIT03_RAW_FRAME_HEADER_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_covariance_frame_header_v1"
TREATMENT_ROLE = "treatment"
CONTROL_ROLE = "control"
REFIT02_TREATMENT_ROLE = "treatment_19d"
REFIT02_MATCHED_NEGATIVE_ROLE = "matched_identity20_negative"
REFIT02_HARNESS_ROLE = "harness_identity20_positive"
REFIT02_ROLES = (
    REFIT02_HARNESS_ROLE,
    REFIT02_MATCHED_NEGATIVE_ROLE,
    REFIT02_TREATMENT_ROLE,
)
TREATMENT_SECTOR = "801207.SI"
CONTROL_SECTOR = "801011.SI"
INACTIVE_FEATURE_INDEX = 19
INACTIVE_FEATURE_NAME = "sf_dispersion_5d_neg"
TREATMENT_PROFILE_RECEIPT_SHA256 = "36cc1afd004796ce3458ab7090010abd07ddd94807d2701318e39d6d80f84e3d"
TREATMENT_SOURCE_SET_SHA256 = "d75e40d3cd82cf232d9e7633bd982eb4189e7fc625d43c2f91f7d010cb7530fb"
TREATMENT_TRAIN_INPUT_MANIFEST_SHA256 = "159a4495899430f4fbba2cd04079b3c51a03f8279dde87333228d1339c02ef6a"
CONTROL_PROFILE_RECEIPT_SHA256 = "9e372d3bde299533fbbf28dee81f1cfc9bb614677f34f78fa49fd82230864929"
CONTROL_SOURCE_SET_SHA256 = "905d97c7987896e854c905a831be33f2732b6d35dd56bedf82571caec2fa2d06"
CONTROL_TRAIN_INPUT_MANIFEST_SHA256 = "63fad3c2f7e1ec7c2855685bcaaa74974f08c182246ed34c211f77a954da156e"
PREPROCESS_IDENTITY_SHA256 = "cd7d759178449c7ec9bda7d1fbad0969a55cc1756361a0d70936f226909ab976"
FEATURE_DEFINITION_SHA256 = "0445f91a5587dddb85e93fa5d08897ba967d41f10819e65eeb13a0353fac9aca"
FORMAL_REPORT_SHA256 = "e7992f87fb555eb26d6c2ef1ad9d45863954edd83fbfcc39f5ae01765cf3939f"
BLOCKER_REPORT_SHA256 = "10287e845f07bf3d9c15a68e5d09ad14e54613348824ac2af568f0244a1cffe8"
REMEDIATION_REPORT_SHA256 = "48157a4255e9d19b814b26b90b18ec38769e28fd0a18e58403edb83fc660bb58"
C010_A5_REPORT_SHA256 = "e7f7edc9fbe7f1cdb5ec739e1390fffec69a9ede6c8d719c9dda1a21df71773d"
C010_A5_PARTITION_SHA256 = "03d785347b35185fe9f9c771e0a4e69cd0deb8def31a0cb205d3ca7a86b8ead6"
C010_A5_MAPPING_SHA256 = "6ed16f4e8473d851be7e359aac431c241bb98f0ba18dd3e7b537ca519f7fd696"
SOURCE_AUTHORITY_V1 = {
    "formal_report_sha256": FORMAL_REPORT_SHA256,
    "blocker_report_sha256": BLOCKER_REPORT_SHA256,
    "remediation_report_sha256": REMEDIATION_REPORT_SHA256,
}
SOURCE_AUTHORITY_V2 = {
    **SOURCE_AUTHORITY_V1,
    "c010_a5_report_sha256": C010_A5_REPORT_SHA256,
    "c010_a5_partition_sha256": C010_A5_PARTITION_SHA256,
}
SOURCE_AUTHORITY = {
    **SOURCE_AUTHORITY_V2,
    "c010_a5_mapping_sha256": C010_A5_MAPPING_SHA256,
}

_MIGRATABLE_INPUT_IDENTITY_FIELDS = frozenset(
    {
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "calendar_manifest_hash",
        "l2_stock_fact_manifest_hash",
        "feature_domain_policy_sha256",
    }
)
C010_A5_LINEAGE_EXCLUDED_FIELDS = (
    "authority_identity_sha256",
    "authority_receipt_sha256",
    "entry_sha256",
    "expected_opportunity_receipt_sha256",
    "identity_sha256",
    "provider_absence_partition_receipt_sha256",
    "query_plan_contract",
    "receipt_sha256",
    "security_resolver_receipt_sha256",
)

_D1_REJECTION_REASONS = frozenset(
    {
        "hmm_risk_model_inactive_dimension_authority_mismatch",
        "hmm_risk_model_inactive_dimension_not_exact_zero",
        "hmm_risk_model_inactive_dimension_preprocess_mismatch",
        "hmm_risk_model_inactive_dimension_projection_invalid",
        "hmm_risk_model_inactive_dimension_contract_invalid",
        "hmm_risk_model_inactive_dimension_parameter_shape_invalid",
    }
)


class D1InactiveDimensionError(StateModelSetError):
    def __init__(self, reason_code: str, message: str, *, evidence: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.evidence = dict(evidence or {})


def _refit_diagnostic_contract_for_report(schema_version: str) -> str:
    if schema_version == REFIT02_REPORT_SCHEMA_VERSION:
        return REFIT02_DIAGNOSTIC_CONTRACT
    if schema_version == REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT:
        return REFIT02_DIAGNOSTIC_CONTRACT_MATCHED_FIT
    return REFIT02_DIAGNOSTIC_CONTRACT_LEGACY


def _refit_process_schema_for_report(schema_version: str) -> str:
    if schema_version == REFIT02_REPORT_SCHEMA_VERSION:
        return REFIT02_PROCESS_SCHEMA_VERSION
    if schema_version == REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT:
        return REFIT02_PROCESS_SCHEMA_VERSION_MATCHED_FIT
    if schema_version == REFIT02_REPORT_SCHEMA_VERSION_LEGACY:
        return REFIT02_PROCESS_SCHEMA_VERSION_LEGACY
    return REFIT02_PROCESS_SCHEMA_VERSION_ORIGINAL


def _refit_report_uses_v2_fit_budget(schema_version: str) -> bool:
    return schema_version in {REFIT02_REPORT_SCHEMA_VERSION, REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT}


def _require_sha256(value: Any, field: str) -> str:
    identity = str(value or "").lower()
    if len(identity) != 64 or any(character not in "0123456789abcdef" for character in identity):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            f"{field} must be a lowercase SHA-256 digest",
        )
    return identity


def _require_identity(actual: Any, expected: str, field: str) -> str:
    identity = _require_sha256(actual, field)
    if identity != expected:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            f"{field} does not match the approved D1 authority",
            evidence={"field": field, "actual": identity, "expected": expected},
        )
    return identity


def _require_commit(value: Any, field: str) -> str:
    identity = str(value or "").lower()
    if len(identity) != 40 or any(character not in "0123456789abcdef" for character in identity):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            f"{field} must be a 40-character lowercase Git SHA",
        )
    return identity


def _process_source_authority(schema_version: str) -> Mapping[str, str]:
    if schema_version == PROCESS_SCHEMA_VERSION:
        return SOURCE_AUTHORITY
    if schema_version == PROCESS_SCHEMA_VERSION_V2:
        return SOURCE_AUTHORITY_V2
    if schema_version == PROCESS_SCHEMA_VERSION_V1:
        return SOURCE_AUTHORITY_V1
    raise D1InactiveDimensionError(
        "hmm_risk_model_inactive_dimension_contract_invalid",
        "D1 process schema version is unsupported",
    )


def _report_source_authority(schema_version: str) -> Mapping[str, str]:
    if schema_version == REPORT_SCHEMA_VERSION:
        return SOURCE_AUTHORITY
    if schema_version == REPORT_SCHEMA_VERSION_V2:
        return SOURCE_AUTHORITY_V2
    if schema_version == REPORT_SCHEMA_VERSION_V1:
        return SOURCE_AUTHORITY_V1
    raise D1InactiveDimensionError(
        "hmm_risk_model_inactive_dimension_contract_invalid",
        "D1 report schema version is unsupported",
    )


def validate_source_identity_set(
    values: Sequence[Mapping[str, Any]],
    *,
    expected_sha256: str,
) -> tuple[dict[str, str | int], ...]:
    expected = _require_sha256(expected_sha256, "expected_source_set_sha256")
    normalized: list[dict[str, str | int]] = []
    for raw in values:
        if set(raw) != {"seed", "diagnostic_entry_sha256", "source_entry_receipt_sha256"}:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_authority_mismatch",
                "D1 source identity entry fields are invalid",
            )
        normalized.append(
            {
                "seed": int(raw["seed"]),
                "diagnostic_entry_sha256": _require_sha256(raw["diagnostic_entry_sha256"], "diagnostic_entry_sha256"),
                "source_entry_receipt_sha256": _require_sha256(
                    raw["source_entry_receipt_sha256"], "source_entry_receipt_sha256"
                ),
            }
        )
    normalized.sort(key=lambda entry: int(entry["seed"]))
    if tuple(int(entry["seed"]) for entry in normalized) != RESTART_SCHEDULE:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_attempt_set_incomplete",
            "D1 source identity set must contain seeds 42..49 exactly once",
        )
    if canonical_sha256(normalized) != expected:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 source identity set hash mismatch",
        )
    return tuple(normalized)


def _matrix(value: Any, *, field: str) -> np.ndarray:
    result = np.ascontiguousarray(np.asarray(value, dtype="<f8"))
    if result.ndim != 2 or result.shape[1] != len(ALL_CORE_FEATURES) or result.shape[0] < 1:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            f"{field} must be a non-empty full-20 float64 matrix",
        )
    if not np.isfinite(result).all():
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            f"{field} contains non-finite values",
        )
    return result


def _exact_zero_evidence(raw: np.ndarray, preprocessed: np.ndarray) -> dict[str, Any]:
    raw_vector = np.ascontiguousarray(raw[:, INACTIVE_FEATURE_INDEX], dtype="<f8")
    preprocessed_vector = np.ascontiguousarray(preprocessed[:, INACTIVE_FEATURE_INDEX], dtype="<f8")
    raw_bits = raw_vector.view("<u8")
    raw_unique_bits = np.unique(raw_bits)
    preprocessed_unique_bits = np.unique(preprocessed_vector.view("<u8"))
    evidence = {
        "raw_variance_ddof0": float(np.var(raw_vector, ddof=0)),
        "preprocessed_variance_ddof0": float(np.var(preprocessed_vector, ddof=0)),
        "raw_unique_bit_pattern_count": int(raw_unique_bits.size),
        "preprocessed_unique_bit_pattern_count": int(preprocessed_unique_bits.size),
        "raw_all_exact_zero": bool(np.all(raw_vector == 0.0)),
        "raw_vector_identity": _float64_array_identity(raw_vector),
        "preprocessed_vector_identity": _float64_array_identity(preprocessed_vector),
    }
    if (
        evidence["raw_variance_ddof0"] != 0.0
        or evidence["preprocessed_variance_ddof0"] != 0.0
        or evidence["raw_unique_bit_pattern_count"] != 1
        or evidence["preprocessed_unique_bit_pattern_count"] != 1
        or evidence["raw_all_exact_zero"] is not True
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_not_exact_zero",
            "approved D1 inactive feature is not exact zero before and after preprocessing",
            evidence=evidence,
        )
    return evidence


def _refit02_role_input(
    item: B3TrainOnlySeries,
    *,
    preprocess: Mapping[str, Any],
) -> tuple[dict[str, Any], np.ndarray, np.ndarray, dict[str, Any], bool]:
    item.validate(len(ALL_CORE_FEATURES))
    raw = _matrix(item.train_observations, field=f"{item.sector_code}.train_observations")
    preprocessed = _matrix(
        _apply_preprocess(raw, preprocess),
        field=f"{item.sector_code}.preprocessed_train_observations",
    )
    raw_vector = np.ascontiguousarray(raw[:, INACTIVE_FEATURE_INDEX], dtype="<f8")
    preprocessed_vector = np.ascontiguousarray(preprocessed[:, INACTIVE_FEATURE_INDEX], dtype="<f8")
    active_variances = np.asarray(np.var(preprocessed[:, :INACTIVE_FEATURE_INDEX], axis=0, ddof=0), dtype=np.float64)
    exact_zero = {
        "raw_variance_ddof0": float(np.var(raw_vector, ddof=0)),
        "preprocessed_variance_ddof0": float(np.var(preprocessed_vector, ddof=0)),
        "raw_unique_bit_pattern_count": int(np.unique(raw_vector.view("<u8")).size),
        "preprocessed_unique_bit_pattern_count": int(np.unique(preprocessed_vector.view("<u8")).size),
        "raw_all_exact_zero": bool(np.all(raw_vector == 0.0)),
        "raw_vector_identity": _float64_array_identity(raw_vector),
        "preprocessed_vector_identity": _float64_array_identity(preprocessed_vector),
        "active_variance_min": float(np.min(active_variances)),
        "active_variance_all_finite_positive": bool(
            np.isfinite(active_variances).all() and np.all(active_variances > 0.0)
        ),
    }
    eligible = bool(
        exact_zero["raw_variance_ddof0"] == 0.0
        and exact_zero["preprocessed_variance_ddof0"] == 0.0
        and exact_zero["raw_unique_bit_pattern_count"] == 1
        and exact_zero["preprocessed_unique_bit_pattern_count"] == 1
        and exact_zero["raw_all_exact_zero"] is True
        and exact_zero["active_variance_all_finite_positive"] is True
    )
    manifest = dict(item.train_input_manifest)
    for field in (
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "calendar_manifest_hash",
        "feature_domain_policy_sha256",
    ):
        _require_sha256(manifest.get(field), f"{item.sector_code}.{field}")
    if manifest.get("mapping_manifest_hash") != C010_A5_MAPPING_SHA256:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_authority_mismatch",
            f"{item.sector_code} mapping is not the approved C-010-A5 mapping",
        )
    role_input = {
        "sector_code": item.sector_code,
        "row_count": int(raw.shape[0]),
        "min_date": item.train_dates[0].isoformat(),
        "max_date": item.train_dates[-1].isoformat(),
        "train_observation_sha256": _require_sha256(
            manifest.get("train_observation_sha256"),
            f"{item.sector_code}.train_observation_sha256",
        ),
        "full20_preprocess_sha256": _float64_array_identity(preprocessed)["sha256"],
        "observation_manifest_sha256": _require_sha256(
            item.observation_manifest_hash,
            f"{item.sector_code}.observation_manifest_sha256",
        ),
        "pit_constituent_manifest_sha256": _require_sha256(
            item.pit_constituent_manifest_hash,
            f"{item.sector_code}.pit_constituent_manifest_sha256",
        ),
        "feature_names_sha256": canonical_sha256(list(ALL_CORE_FEATURES)),
        "train_input_manifest_sha256": canonical_sha256(manifest),
    }
    return role_input, raw, preprocessed, exact_zero, eligible


def build_refit02_current_a5_authority(
    *,
    treatment_item: B3TrainOnlySeries,
    harness_item: B3TrainOnlySeries,
    preprocess: Mapping[str, Any],
    current_policy_sha256: str,
    producer_commit: str,
) -> dict[str, Any]:
    """Freeze one current-A5 authority before any REFIT-02 HMM fit is allowed."""

    if treatment_item.sector_code != TREATMENT_SECTOR or harness_item.sector_code != CONTROL_SECTOR:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_authority_mismatch",
            "REFIT-02 treatment/harness sectors are invalid",
        )
    treatment_input, _, _, exact_zero, eligible = _refit02_role_input(
        treatment_item,
        preprocess=preprocess,
    )
    harness_input, _, _, _, _ = _refit02_role_input(harness_item, preprocess=preprocess)
    treatment_manifest = dict(treatment_item.train_input_manifest)
    harness_manifest = dict(harness_item.train_input_manifest)
    normalized_policy = _require_sha256(current_policy_sha256, "current_policy_sha256")
    shared_fields = (
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "calendar_manifest_hash",
        "feature_domain_policy_sha256",
    )
    if any(treatment_manifest.get(field) != harness_manifest.get(field) for field in shared_fields) or (
        treatment_manifest.get("feature_domain_policy_sha256") != normalized_policy
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_authority_mismatch",
            "REFIT-02 roles do not share one current-A5 train authority",
        )
    role_inputs = {
        REFIT02_HARNESS_ROLE: harness_input,
        REFIT02_MATCHED_NEGATIVE_ROLE: treatment_input,
        REFIT02_TREATMENT_ROLE: treatment_input,
    }
    authority_body = {
        "schema_version": REFIT02_AUTHORITY_SCHEMA_VERSION,
        "c010_a5_report_sha256": C010_A5_REPORT_SHA256,
        "c010_a5_partition_sha256": C010_A5_PARTITION_SHA256,
        "c010_a5_mapping_sha256": C010_A5_MAPPING_SHA256,
        "dataset_manifest_sha256": _require_sha256(
            treatment_manifest.get("dataset_manifest_hash"),
            "dataset_manifest_sha256",
        ),
        "calendar_sha256": _require_sha256(
            treatment_manifest.get("calendar_manifest_hash"),
            "calendar_sha256",
        ),
        "train_start": treatment_item.train_dates[0].isoformat(),
        "train_end": treatment_item.train_dates[-1].isoformat(),
        "family": "autocycle_all_core",
        "level": "L2",
        "feature_domain_policy_sha256": normalized_policy,
        "role_inputs": role_inputs,
    }
    body = {
        "schema_version": REFIT02_AUTHORITY_SCHEMA_VERSION,
        "algorithm_version": REFIT02_ALGORITHM_VERSION,
        "producer_commit": _require_commit(producer_commit, "producer_commit"),
        "source_authority": dict(SOURCE_AUTHORITY),
        "experiment_authority": authority_body,
        "current_a5_experiment_authority_sha256": canonical_sha256(authority_body),
        "current_profile_exact_zero_evidence": exact_zero,
        "current_profile_eligible": eligible,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def validate_refit02_current_a5_authority(
    receipt: Mapping[str, Any],
    *,
    treatment_item: B3TrainOnlySeries,
    harness_item: B3TrainOnlySeries,
    preprocess: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = dict(receipt)
    rebuilt = build_refit02_current_a5_authority(
        treatment_item=treatment_item,
        harness_item=harness_item,
        preprocess=preprocess,
        current_policy_sha256=str(
            (normalized.get("experiment_authority") or {}).get("feature_domain_policy_sha256") or ""
        ),
        producer_commit=str(normalized.get("producer_commit") or ""),
    )
    if rebuilt != normalized:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_authority_mismatch",
            "REFIT-02 current-A5 authority differs from its writer",
        )
    return normalized


def _validate_refit02_current_authority_envelope(receipt: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(receipt)
    body = {key: value for key, value in normalized.items() if key != "receipt_sha256"}
    experiment = normalized.get("experiment_authority")
    role_inputs = experiment.get("role_inputs") if isinstance(experiment, Mapping) else None
    false_flags = (
        "selection_performed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    )
    exact_zero = normalized.get("current_profile_exact_zero_evidence")
    eligible = bool(
        isinstance(exact_zero, Mapping)
        and exact_zero.get("raw_variance_ddof0") == 0.0
        and exact_zero.get("preprocessed_variance_ddof0") == 0.0
        and exact_zero.get("raw_unique_bit_pattern_count") == 1
        and exact_zero.get("preprocessed_unique_bit_pattern_count") == 1
        and exact_zero.get("raw_all_exact_zero") is True
        and exact_zero.get("active_variance_all_finite_positive") is True
    )
    if (
        normalized.get("schema_version") != REFIT02_AUTHORITY_SCHEMA_VERSION
        or normalized.get("algorithm_version") != REFIT02_ALGORITHM_VERSION
        or normalized.get("source_authority") != SOURCE_AUTHORITY
        or not isinstance(experiment, Mapping)
        or experiment.get("schema_version") != REFIT02_AUTHORITY_SCHEMA_VERSION
        or experiment.get("c010_a5_report_sha256") != C010_A5_REPORT_SHA256
        or experiment.get("c010_a5_partition_sha256") != C010_A5_PARTITION_SHA256
        or experiment.get("c010_a5_mapping_sha256") != C010_A5_MAPPING_SHA256
        or experiment.get("mapping_manifest_sha256") is not None
        or not isinstance(role_inputs, Mapping)
        or set(role_inputs) != set(REFIT02_ROLES)
        or role_inputs.get(REFIT02_TREATMENT_ROLE) != role_inputs.get(REFIT02_MATCHED_NEGATIVE_ROLE)
        or canonical_sha256(experiment) != normalized.get("current_a5_experiment_authority_sha256")
        or canonical_sha256(body) != normalized.get("receipt_sha256")
        or normalized.get("current_profile_eligible") is not eligible
        or any(normalized.get(field) is not False for field in false_flags)
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_authority_mismatch",
            "REFIT-02 current-A5 authority envelope is invalid",
        )
    for role, expected_sector in (
        (REFIT02_TREATMENT_ROLE, TREATMENT_SECTOR),
        (REFIT02_MATCHED_NEGATIVE_ROLE, TREATMENT_SECTOR),
        (REFIT02_HARNESS_ROLE, CONTROL_SECTOR),
    ):
        role_input = role_inputs.get(role)
        if not isinstance(role_input, Mapping) or role_input.get("sector_code") != expected_sector:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_current_authority_mismatch",
                "REFIT-02 current-A5 role input is invalid",
            )
        for field in (
            "train_observation_sha256",
            "full20_preprocess_sha256",
            "observation_manifest_sha256",
            "pit_constituent_manifest_sha256",
            "feature_names_sha256",
            "train_input_manifest_sha256",
        ):
            _require_sha256(role_input.get(field), f"{role}.{field}")
    return normalized


def build_refit02_historical_reference_receipt(
    *,
    treatment_item: B3TrainOnlySeries,
    harness_item: B3TrainOnlySeries,
    historical_treatment_manifest: Mapping[str, Any],
    historical_harness_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    pairs: dict[str, dict[str, Any]] = {}
    for sector, current_item, historical, expected_hash in (
        (TREATMENT_SECTOR, treatment_item, dict(historical_treatment_manifest), TREATMENT_TRAIN_INPUT_MANIFEST_SHA256),
        (CONTROL_SECTOR, harness_item, dict(historical_harness_manifest), CONTROL_TRAIN_INPUT_MANIFEST_SHA256),
    ):
        if canonical_sha256(historical) != expected_hash:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_historical_reference_invalid",
                f"REFIT-02 historical reference is invalid for {sector}",
            )
        current = dict(current_item.train_input_manifest)
        paths = sorted(key for key in set(historical) | set(current) if historical.get(key) != current.get(key))
        pairs[sector] = {
            "historical_train_input_manifest_sha256": expected_hash,
            "current_train_input_manifest_sha256": canonical_sha256(current),
            "historical_train_input_manifest": historical,
            "current_train_input_manifest": current,
            "changed_paths": paths,
            "historical_reference_status": "equal" if not paths else "drift_observed",
        }
    body = {
        "schema_version": "hmm_risk_c008_b3_d1_historical_reference_drift_v1",
        "algorithm_version": REFIT02_ALGORITHM_VERSION,
        "pairs": pairs,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def validate_refit02_historical_reference_receipt(
    receipt: Mapping[str, Any],
    *,
    current_authority: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = dict(receipt)
    authority = _validate_refit02_current_authority_envelope(current_authority)
    body = {key: value for key, value in normalized.items() if key != "receipt_sha256"}
    pairs = normalized.get("pairs")
    false_flags = (
        "selection_performed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    )
    if (
        normalized.get("schema_version") != "hmm_risk_c008_b3_d1_historical_reference_drift_v1"
        or normalized.get("algorithm_version") != REFIT02_ALGORITHM_VERSION
        or not isinstance(pairs, Mapping)
        or set(pairs) != {TREATMENT_SECTOR, CONTROL_SECTOR}
        or canonical_sha256(body) != normalized.get("receipt_sha256")
        or any(normalized.get(field) is not False for field in false_flags)
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_historical_reference_invalid",
            "REFIT-02 historical reference envelope is invalid",
        )
    role_inputs = authority["experiment_authority"]["role_inputs"]
    for sector, role, expected_historical_hash in (
        (TREATMENT_SECTOR, REFIT02_TREATMENT_ROLE, TREATMENT_TRAIN_INPUT_MANIFEST_SHA256),
        (CONTROL_SECTOR, REFIT02_HARNESS_ROLE, CONTROL_TRAIN_INPUT_MANIFEST_SHA256),
    ):
        pair = pairs.get(sector)
        if not isinstance(pair, Mapping):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_historical_reference_invalid",
                f"REFIT-02 historical reference pair is missing for {sector}",
            )
        historical = pair.get("historical_train_input_manifest")
        current = pair.get("current_train_input_manifest")
        if not isinstance(historical, Mapping) or not isinstance(current, Mapping):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_historical_reference_invalid",
                f"REFIT-02 historical/current manifest is missing for {sector}",
            )
        changed_paths = sorted(key for key in set(historical) | set(current) if historical.get(key) != current.get(key))
        expected_status = "equal" if not changed_paths else "drift_observed"
        if (
            canonical_sha256(historical) != expected_historical_hash
            or pair.get("historical_train_input_manifest_sha256") != expected_historical_hash
            or canonical_sha256(current) != pair.get("current_train_input_manifest_sha256")
            or pair.get("current_train_input_manifest_sha256") != role_inputs[role]["train_input_manifest_sha256"]
            or pair.get("changed_paths") != changed_paths
            or pair.get("historical_reference_status") != expected_status
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_historical_reference_invalid",
                f"REFIT-02 historical reference pair is invalid for {sector}",
            )
    return normalized


def build_input_migration_receipt(
    item: B3TrainOnlySeries,
    *,
    historical_train_input_manifest: Mapping[str, Any],
    role: str,
    current_policy_sha256: str,
    producer_commit: str,
    historical_observation_manifest_hash: str | None,
    historical_pit_constituent_manifest_hash: str | None,
    c010_a5_lineage_migration_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Prove that A5 changed only global lineage fields, never D1 sector observations."""

    if role not in {TREATMENT_ROLE, CONTROL_ROLE}:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 input migration role is invalid",
        )
    expected_sector = TREATMENT_SECTOR if role == TREATMENT_ROLE else CONTROL_SECTOR
    expected_historical_sha256 = (
        TREATMENT_TRAIN_INPUT_MANIFEST_SHA256 if role == TREATMENT_ROLE else CONTROL_TRAIN_INPUT_MANIFEST_SHA256
    )
    historical = dict(historical_train_input_manifest)
    current = dict(item.train_input_manifest)
    if item.sector_code != expected_sector or canonical_sha256(historical) != expected_historical_sha256:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 historical input migration authority is invalid",
        )
    if set(current) != set(historical) or not _MIGRATABLE_INPUT_IDENTITY_FIELDS.issubset(current):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 current and historical input manifest fields are incompatible",
        )
    historical_core = {key: value for key, value in historical.items() if key not in _MIGRATABLE_INPUT_IDENTITY_FIELDS}
    current_core = {key: value for key, value in current.items() if key not in _MIGRATABLE_INPUT_IDENTITY_FIELDS}
    if historical_core != current_core:
        changed = sorted(key for key in historical_core if historical_core.get(key) != current_core.get(key))
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 target/control train core changed across C-010-A5 migration: " + ",".join(changed),
            evidence={"changed_core_fields": changed},
        )
    normalized_policy_sha256 = _require_sha256(current_policy_sha256, "current_policy_sha256")
    lineage_migration = _validate_c010_a5_lineage_migration_envelope(
        c010_a5_lineage_migration_receipt,
        expected_producer_commit=producer_commit,
    )
    historical_observation_sha256 = None
    historical_pit_sha256 = None
    if role == CONTROL_ROLE:
        historical_observation_sha256 = _require_sha256(
            historical_observation_manifest_hash,
            "historical_observation_manifest_hash",
        )
        historical_pit_sha256 = _require_sha256(
            historical_pit_constituent_manifest_hash,
            "historical_pit_constituent_manifest_hash",
        )
    elif historical_observation_manifest_hash is not None or historical_pit_constituent_manifest_hash is not None:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 treatment cannot claim historical fitted-model lineage that was never produced",
        )
    if (
        current.get("mapping_manifest_hash") != C010_A5_MAPPING_SHA256
        or current.get("feature_domain_policy_sha256") != normalized_policy_sha256
        or (role == CONTROL_ROLE and item.observation_manifest_hash != historical_observation_sha256)
        or tuple(item.pit_l2_constituents) != (expected_sector,)
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 current input manifest is not bound to the approved C-010-A5 execution authority",
        )
    migrated_fields: dict[str, dict[str, str]] = {}
    for field in sorted(_MIGRATABLE_INPUT_IDENTITY_FIELDS):
        historical_identity = _require_sha256(historical.get(field), f"historical.{field}")
        current_identity = _require_sha256(current.get(field), f"current.{field}")
        migrated_fields[field] = {"historical": historical_identity, "current": current_identity}
    body = {
        "schema_version": INPUT_MIGRATION_SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "producer_commit": _require_commit(producer_commit, "producer_commit"),
        "role": role,
        "family": "autocycle_all_core",
        "level": "L2",
        "sector_code": expected_sector,
        "source_authority": dict(SOURCE_AUTHORITY),
        "historical_train_input_manifest": historical,
        "historical_train_input_manifest_sha256": expected_historical_sha256,
        "current_train_input_manifest": current,
        "current_train_input_manifest_sha256": canonical_sha256(current),
        "unchanged_core_manifest_sha256": canonical_sha256(current_core),
        "migrated_identity_fields": migrated_fields,
        "c010_a5_lineage_migration_receipt": lineage_migration,
        "c010_a5_lineage_migration_receipt_sha256": lineage_migration["receipt_sha256"],
        "historical_observation_manifest_hash": historical_observation_sha256,
        "current_observation_manifest_hash": item.observation_manifest_hash,
        "historical_pit_constituent_manifest_hash": historical_pit_sha256,
        "historical_fitted_model_lineage_available": role == CONTROL_ROLE,
        "current_pit_l2_constituents": list(item.pit_l2_constituents),
        "current_pit_constituent_manifest_hash": _require_sha256(
            item.pit_constituent_manifest_hash,
            "current_pit_constituent_manifest_hash",
        ),
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def validate_input_migration_receipt(
    receipt: Mapping[str, Any],
    *,
    item: B3TrainOnlySeries,
    expected_role: str,
) -> dict[str, Any]:
    normalized = dict(receipt)
    rebuilt = build_input_migration_receipt(
        item,
        historical_train_input_manifest=dict(normalized.get("historical_train_input_manifest") or {}),
        role=expected_role,
        current_policy_sha256=str(
            (normalized.get("current_train_input_manifest") or {}).get("feature_domain_policy_sha256") or ""
        ),
        producer_commit=str(normalized.get("producer_commit") or ""),
        historical_observation_manifest_hash=normalized.get("historical_observation_manifest_hash"),
        historical_pit_constituent_manifest_hash=normalized.get("historical_pit_constituent_manifest_hash"),
        c010_a5_lineage_migration_receipt=dict(normalized.get("c010_a5_lineage_migration_receipt") or {}),
    )
    if rebuilt != normalized:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 input migration receipt differs from the writer authority",
        )
    return normalized


def _validate_input_migration_envelope(receipt: Mapping[str, Any], *, expected_role: str) -> dict[str, Any]:
    normalized = dict(receipt)
    identity = str(normalized.get("receipt_sha256") or "")
    body = {key: value for key, value in normalized.items() if key != "receipt_sha256"}
    expected_sector = TREATMENT_SECTOR if expected_role == TREATMENT_ROLE else CONTROL_SECTOR
    expected_historical_sha256 = (
        TREATMENT_TRAIN_INPUT_MANIFEST_SHA256
        if expected_role == TREATMENT_ROLE
        else CONTROL_TRAIN_INPUT_MANIFEST_SHA256
    )
    historical = dict(normalized.get("historical_train_input_manifest") or {})
    current = dict(normalized.get("current_train_input_manifest") or {})
    historical_core = {key: value for key, value in historical.items() if key not in _MIGRATABLE_INPUT_IDENTITY_FIELDS}
    current_core = {key: value for key, value in current.items() if key not in _MIGRATABLE_INPUT_IDENTITY_FIELDS}
    expected_migrated_fields = {
        field: {"historical": historical.get(field), "current": current.get(field)}
        for field in sorted(_MIGRATABLE_INPUT_IDENTITY_FIELDS)
    }
    lineage_migration = normalized.get("c010_a5_lineage_migration_receipt")
    if (
        normalized.get("schema_version") != INPUT_MIGRATION_SCHEMA_VERSION
        or normalized.get("algorithm_version") != ALGORITHM_VERSION
        or normalized.get("role") != expected_role
        or normalized.get("family") != "autocycle_all_core"
        or normalized.get("level") != "L2"
        or normalized.get("sector_code") != expected_sector
        or normalized.get("source_authority") != SOURCE_AUTHORITY
        or normalized.get("historical_train_input_manifest_sha256") != expected_historical_sha256
        or set(current) != set(historical)
        or not _MIGRATABLE_INPUT_IDENTITY_FIELDS.issubset(current)
        or historical_core != current_core
        or current.get("mapping_manifest_hash") != C010_A5_MAPPING_SHA256
        or normalized.get("migrated_identity_fields") != expected_migrated_fields
        or not isinstance(lineage_migration, Mapping)
        or normalized.get("c010_a5_lineage_migration_receipt_sha256") != (lineage_migration or {}).get("receipt_sha256")
        or normalized.get("unchanged_core_manifest_sha256") != canonical_sha256(current_core)
        or canonical_sha256(historical) != expected_historical_sha256
        or canonical_sha256(current) != normalized.get("current_train_input_manifest_sha256")
        or normalized.get("historical_fitted_model_lineage_available") is not (expected_role == CONTROL_ROLE)
        or (
            expected_role == CONTROL_ROLE
            and normalized.get("current_observation_manifest_hash")
            != normalized.get("historical_observation_manifest_hash")
        )
        or (
            expected_role == TREATMENT_ROLE
            and (
                normalized.get("historical_observation_manifest_hash") is not None
                or normalized.get("historical_pit_constituent_manifest_hash") is not None
            )
        )
        or normalized.get("current_pit_l2_constituents") != [expected_sector]
        or not str(normalized.get("current_pit_constituent_manifest_hash") or "")
        or any(
            normalized.get(field) is not False
            for field in (
                "selection_performed",
                "model_write_performed",
                "ready_artifact_write_performed",
                "database_write_performed",
                "runtime_action_performed",
            )
        )
        or identity != canonical_sha256(body)
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 input migration receipt envelope is invalid",
        )
    _require_commit(normalized.get("producer_commit"), "producer_commit")
    _validate_c010_a5_lineage_migration_envelope(
        lineage_migration,
        expected_producer_commit=str(normalized["producer_commit"]),
    )
    for field in ("current_observation_manifest_hash", "current_pit_constituent_manifest_hash"):
        _require_sha256(normalized.get(field), field)
    if expected_role == CONTROL_ROLE:
        for field in ("historical_observation_manifest_hash", "historical_pit_constituent_manifest_hash"):
            _require_sha256(normalized.get(field), field)
    for field in sorted(_MIGRATABLE_INPUT_IDENTITY_FIELDS):
        _require_sha256(historical.get(field), f"historical.{field}")
        _require_sha256(current.get(field), f"current.{field}")
    return normalized


def _validate_c010_a5_lineage_migration_envelope(
    receipt: Mapping[str, Any],
    *,
    expected_producer_commit: str,
) -> dict[str, Any]:
    normalized = dict(receipt)
    identity = str(normalized.get("receipt_sha256") or "")
    body = {key: value for key, value in normalized.items() if key != "receipt_sha256"}
    pairs = normalized.get("receipt_pairs")
    if (
        normalized.get("schema_version") != C010_A5_LINEAGE_MIGRATION_SCHEMA_VERSION
        or normalized.get("producer_commit") != _require_commit(expected_producer_commit, "expected_producer_commit")
        or normalized.get("source_a5_report_sha256") != C010_A5_REPORT_SHA256
        or normalized.get("source_a5_partition_sha256") != C010_A5_PARTITION_SHA256
        or normalized.get("status") != "accepted"
        or normalized.get("excluded_non_business_fields") != list(C010_A5_LINEAGE_EXCLUDED_FIELDS)
        or not isinstance(pairs, Mapping)
        or set(pairs) != {"eligibility", "expected_opportunity", "provider_absence_partition"}
        or any(
            not isinstance(pair, Mapping)
            or pair.get("approved_semantic_payload_sha256") != pair.get("current_semantic_payload_sha256")
            for pair in (pairs or {}).values()
        )
        or any(
            normalized.get(field) is not False
            for field in (
                "selection_performed",
                "model_write_performed",
                "ready_artifact_write_performed",
                "database_write_performed",
                "runtime_action_performed",
            )
        )
        or identity != canonical_sha256(body)
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 C-010-A5 execution-lineage migration receipt is invalid",
        )
    for pair in pairs.values():
        for field in (
            "approved_receipt_sha256",
            "current_receipt_sha256",
            "approved_semantic_payload_sha256",
            "current_semantic_payload_sha256",
        ):
            _require_sha256(pair.get(field), field)
    return normalized


def build_projection(
    item: B3TrainOnlySeries,
    *,
    preprocess: Mapping[str, Any],
    role: str,
    profile_receipt_sha256: str,
    source_set_sha256: str,
    preprocess_identity_sha256: str,
    feature_definition_sha256: str,
    input_migration_receipt: Mapping[str, Any] | None = None,
) -> tuple[np.ndarray, dict[str, Any]]:
    if role not in {TREATMENT_ROLE, CONTROL_ROLE}:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 role must be treatment or control",
        )
    expected_sector = TREATMENT_SECTOR if role == TREATMENT_ROLE else CONTROL_SECTOR
    expected_profile = TREATMENT_PROFILE_RECEIPT_SHA256 if role == TREATMENT_ROLE else CONTROL_PROFILE_RECEIPT_SHA256
    expected_source_set = TREATMENT_SOURCE_SET_SHA256 if role == TREATMENT_ROLE else CONTROL_SOURCE_SET_SHA256
    expected_train_input = (
        TREATMENT_TRAIN_INPUT_MANIFEST_SHA256 if role == TREATMENT_ROLE else CONTROL_TRAIN_INPUT_MANIFEST_SHA256
    )
    if item.sector_code != expected_sector:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 role and sector identity mismatch",
        )
    try:
        item.validate(len(ALL_CORE_FEATURES))
    except StateModelSetError as exc:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            f"D1 full-20 input contract is invalid: {exc}",
        ) from exc
    _require_identity(profile_receipt_sha256, expected_profile, "profile_receipt_sha256")
    _require_identity(source_set_sha256, expected_source_set, "source_set_sha256")
    _require_identity(preprocess_identity_sha256, PREPROCESS_IDENTITY_SHA256, "preprocess_identity_sha256")
    _require_identity(feature_definition_sha256, FEATURE_DEFINITION_SHA256, "feature_definition_sha256")
    current_train_input_sha256 = canonical_sha256(dict(item.train_input_manifest))
    migration = None
    if current_train_input_sha256 != expected_train_input:
        if not isinstance(input_migration_receipt, Mapping):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_authority_mismatch",
                "D1 train input manifest differs without an approved migration receipt",
            )
        migration = validate_input_migration_receipt(
            input_migration_receipt,
            item=item,
            expected_role=role,
        )
    elif input_migration_receipt is not None:
        migration = validate_input_migration_receipt(
            input_migration_receipt,
            item=item,
            expected_role=role,
        )
    if canonical_sha256(dict(preprocess)) != PREPROCESS_IDENTITY_SHA256:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_preprocess_mismatch",
            "D1 preprocess payload differs from its approved identity",
        )
    raw = _matrix(item.train_observations, field="train_observations")
    try:
        preprocessed = _matrix(_apply_preprocess(raw, preprocess), field="preprocessed_train_observations")
    except D1InactiveDimensionError:
        raise
    except (StateModelSetError, ValueError, FloatingPointError) as exc:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_preprocess_mismatch",
            f"D1 full-20 preprocessing failed: {exc}",
        ) from exc
    inactive = [INACTIVE_FEATURE_INDEX] if role == TREATMENT_ROLE else []
    active = [index for index in range(len(ALL_CORE_FEATURES)) if index not in inactive]
    exact_zero = _exact_zero_evidence(raw, preprocessed) if role == TREATMENT_ROLE else None
    projected = np.ascontiguousarray(preprocessed[:, active], dtype="<f8")
    expected_dimension = 19 if role == TREATMENT_ROLE else 20
    if projected.shape != (raw.shape[0], expected_dimension):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_projection_invalid",
            "D1 projected matrix shape is invalid",
        )
    mask = [index in active for index in range(len(ALL_CORE_FEATURES))]
    body = {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "role": role,
        "family": "autocycle_all_core",
        "level": "L2",
        "sector_code": item.sector_code,
        "full_feature_names": list(ALL_CORE_FEATURES),
        "full_feature_count": len(ALL_CORE_FEATURES),
        "active_feature_names": [ALL_CORE_FEATURES[index] for index in active],
        "active_feature_indices": active,
        "inactive_feature_names": [ALL_CORE_FEATURES[index] for index in inactive],
        "inactive_feature_indices": inactive,
        "active_feature_mask": mask,
        "active_feature_mask_sha256": canonical_sha256(mask),
        "likelihood_feature_count": expected_dimension,
        "full_preprocess_identity_sha256": PREPROCESS_IDENTITY_SHA256,
        "feature_definition_sha256": FEATURE_DEFINITION_SHA256,
        "profile_receipt_sha256": expected_profile,
        "source_identity_set_sha256": expected_source_set,
        "historical_train_input_manifest_sha256": expected_train_input,
        "train_input_manifest_sha256": current_train_input_sha256,
        "input_migration_receipt_sha256": None if migration is None else migration["receipt_sha256"],
        "raw_matrix_identity": _float64_array_identity(raw),
        "preprocessed_matrix_identity": _float64_array_identity(preprocessed),
        "projected_matrix_identity": _float64_array_identity(projected),
        "exact_zero_evidence": exact_zero,
        "dynamic_activation": False,
        "projection_status": "accepted",
    }
    return projected, {**body, "projection_sha256": canonical_sha256(body)}


def _legacy_compatible_hashes(
    item: B3TrainOnlySeries,
    *,
    preprocess: Mapping[str, Any],
    seed: int,
    numeric_environment: Mapping[str, Any],
    core: B3CoreFitEvidence,
    input_migration_receipt: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    observation_manifest_hash = item.observation_manifest_hash
    pit_constituent_manifest_hash = item.pit_constituent_manifest_hash
    if input_migration_receipt is not None:
        migration = validate_input_migration_receipt(
            input_migration_receipt,
            item=item,
            expected_role=CONTROL_ROLE,
        )
        observation_manifest_hash = str(migration["historical_observation_manifest_hash"])
        pit_constituent_manifest_hash = str(migration["historical_pit_constituent_manifest_hash"])
    model_body = {
        "schema_version": "hmm_risk_b3_fitted_model_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "family": "autocycle_all_core",
        "level": "L2",
        "seed": seed,
        "sector_code": item.sector_code,
        "feature_names": list(ALL_CORE_FEATURES),
        "preprocess": dict(preprocess),
        "startprob": core.startprob.tolist(),
        "transmat": core.transmat.tolist(),
        "means": core.means.tolist(),
        "covariance_type": "diag",
        "covars": core.covars.tolist(),
        "parameter_profile_sha256": canonical_sha256(formal_b3_parameter_profile()),
        "numeric_environment_sha256": canonical_sha256(dict(numeric_environment)),
        "observation_manifest_hash": observation_manifest_hash,
        "pit_constituent_manifest_hash": pit_constituent_manifest_hash,
    }
    model_hash = canonical_sha256(model_body)
    entry_body = {
        "schema_version": "hmm_risk_b3_training_entry_receipt_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "retrain_contract_version": L2_RETRAIN_VERSION,
        "family": "autocycle_all_core",
        "level": "L2",
        "seed": seed,
        "sector_code": item.sector_code,
        "feature_count": len(ALL_CORE_FEATURES),
        "training_rows": int(item.train_observations.shape[0]),
        "fit_status": "accepted",
        "model_entry_status": core.model_entry_status,
        "model_entry_valid": core.model_entry_valid,
        "initialization_evidence": dict(core.initialization),
        "parameter_profile": formal_b3_parameter_profile(),
        "numeric_environment": dict(numeric_environment),
        "monitor_evidence": dict(core.monitor_evidence),
        "likelihood": dict(core.likelihood),
        "covariance": dict(core.covariance),
        "train_occupancy": dict(core.train_occupancy),
        "final_train_log_likelihood": core.terminal_likelihood,
        "final_train_log_likelihood_source": "monitor_history_terminal_value",
        "model_payload_sha256": model_hash,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "artifact_write_performed": False,
        "postfit_projection_performed": False,
    }
    return {
        "entry_receipt_sha256": canonical_sha256(entry_body),
        "model_payload_sha256": model_hash,
    }


def _validate_projected_parameter_shapes(core: B3CoreFitEvidence, *, feature_count: int) -> None:
    expected = {
        "startprob": (3,),
        "transmat": (3, 3),
        "means": (3, feature_count),
        "covars": (3, feature_count),
    }
    actual = {
        "startprob": tuple(np.asarray(core.startprob).shape),
        "transmat": tuple(np.asarray(core.transmat).shape),
        "means": tuple(np.asarray(core.means).shape),
        "covars": tuple(np.asarray(core.covars).shape),
    }
    if actual != expected:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_parameter_shape_invalid",
            "D1 fitted parameter shapes do not match the projected model identity",
            evidence={"actual": actual, "expected": expected},
        )


def fit_controlled_attempt(
    item: B3TrainOnlySeries,
    *,
    preprocess: Mapping[str, Any],
    role: str,
    seed: int,
    process_identity: str,
    numeric_environment: Mapping[str, Any],
    source_identity: Mapping[str, Any],
    profile_receipt_sha256: str,
    source_set_sha256: str,
    preprocess_identity_sha256: str,
    feature_definition_sha256: str,
    input_migration_receipt: Mapping[str, Any] | None = None,
    expected_control_entry_receipt_sha256: str | None = None,
    expected_control_model_payload_sha256: str | None = None,
) -> dict[str, Any]:
    if seed not in RESTART_SCHEDULE or int(source_identity.get("seed", -1)) != seed:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_attempt_set_incomplete",
            "D1 attempt seed is outside or inconsistent with the frozen schedule",
        )
    if not str(process_identity or "").strip():
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 process_identity must be non-empty",
        )
    if not isinstance(numeric_environment, Mapping) or not numeric_environment:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 numeric environment identity is missing",
        )
    numeric_environment_sha256 = canonical_sha256(dict(numeric_environment))
    expected_control_entry: str | None = None
    expected_control_model: str | None = None
    if role == CONTROL_ROLE:
        expected_control_entry = _require_sha256(
            expected_control_entry_receipt_sha256, "expected_control_entry_receipt_sha256"
        )
        expected_control_model = _require_sha256(
            expected_control_model_payload_sha256, "expected_control_model_payload_sha256"
        )
    diagnostic_entry_sha256 = _require_sha256(source_identity.get("diagnostic_entry_sha256"), "diagnostic_entry_sha256")
    source_entry_sha256 = _require_sha256(
        source_identity.get("source_entry_receipt_sha256"), "source_entry_receipt_sha256"
    )
    projected: np.ndarray | None = None
    projection: dict[str, Any] | None = None
    failure_stage: str | None = None
    failure_reason_codes: list[str] = []
    failure_message: str | None = None
    core: B3CoreFitEvidence | None = None
    try:
        projected, projection = build_projection(
            item,
            preprocess=preprocess,
            role=role,
            profile_receipt_sha256=profile_receipt_sha256,
            source_set_sha256=source_set_sha256,
            preprocess_identity_sha256=preprocess_identity_sha256,
            feature_definition_sha256=feature_definition_sha256,
            input_migration_receipt=input_migration_receipt,
        )
        core = fit_b3_preprocessed_train_only(item, train=projected, seed=seed)
        _validate_projected_parameter_shapes(core, feature_count=int(projected.shape[1]))
    except D1InactiveDimensionError as exc:
        failure_stage = (
            "parameter_shape"
            if exc.reason_code == "hmm_risk_model_inactive_dimension_parameter_shape_invalid"
            else "projection"
        )
        failure_reason_codes.append(exc.reason_code)
        failure_message = str(exc)
    except B3TrainingStageError as exc:
        failure_stage = exc.stage
        failure_reason_codes.append(exc.reason_code)
        failure_message = str(exc)
    control_hashes: dict[str, str] | None = None
    control_equal: bool | None = None
    if core is not None and role == CONTROL_ROLE:
        control_hashes = _legacy_compatible_hashes(
            item,
            preprocess=preprocess,
            seed=seed,
            numeric_environment=numeric_environment,
            core=core,
            input_migration_receipt=input_migration_receipt,
        )
        control_equal = (
            control_hashes["entry_receipt_sha256"] == expected_control_entry
            and control_hashes["model_payload_sha256"] == expected_control_model
        )
        if not control_equal:
            failure_stage = "control_compatibility"
            failure_reason_codes.append("hmm_risk_model_inactive_dimension_control_drift")
            failure_message = "identity20 control differs from the frozen formal payload"
    fit_completed = core is not None and not failure_reason_codes
    parameter_payload = None
    if core is not None:
        parameter_payload = {
            "startprob": _float64_array_identity(core.startprob),
            "transmat": _float64_array_identity(core.transmat),
            "means": _float64_array_identity(core.means),
            "covars": _float64_array_identity(core.covars),
        }
    body = {
        "schema_version": ATTEMPT_SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "process_identity": process_identity,
        "role": role,
        "family": "autocycle_all_core",
        "level": "L2",
        "sector_code": item.sector_code,
        "seed": seed,
        "diagnostic_entry_sha256": diagnostic_entry_sha256,
        "source_entry_receipt_sha256": source_entry_sha256,
        "input_migration_receipt_sha256": (
            None if input_migration_receipt is None else str(input_migration_receipt.get("receipt_sha256") or "")
        ),
        "status": "fit_completed" if fit_completed else "fit_failed",
        "fit_status": "accepted" if fit_completed else "failed",
        "failure_stage": failure_stage,
        "failure_reason_codes": list(dict.fromkeys(failure_reason_codes)),
        "failure_message": failure_message,
        "projection_receipt": projection,
        "projection_sha256": projection.get("projection_sha256") if projection else None,
        "likelihood_feature_count": int(projected.shape[1]) if projected is not None else None,
        "parameter_payload": parameter_payload,
        "numeric_environment": dict(numeric_environment),
        "numeric_environment_sha256": numeric_environment_sha256,
        "initialization_evidence": dict(core.initialization) if core else None,
        "monitor_evidence": dict(core.monitor_evidence) if core else None,
        "likelihood": dict(core.likelihood) if core else None,
        "covariance": dict(core.covariance) if core else None,
        "train_occupancy": dict(core.train_occupancy) if core else None,
        "final_train_log_likelihood": core.terminal_likelihood if core else None,
        "control_compatible_payload_hashes": control_hashes,
        "expected_control_entry_receipt_sha256": expected_control_entry,
        "expected_control_model_payload_sha256": expected_control_model,
        "control_payload_bitwise_equal": control_equal,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "attempt_receipt_sha256": canonical_sha256(body)}


def run_controlled_process(
    *,
    treatment_item: B3TrainOnlySeries,
    control_item: B3TrainOnlySeries,
    preprocess: Mapping[str, Any],
    process_identity: str,
    producer_commit: str,
    numeric_environment: Mapping[str, Any],
    treatment_source_identities: Sequence[Mapping[str, Any]],
    control_source_identities: Sequence[Mapping[str, Any]],
    frozen_control_hashes: Mapping[int, Mapping[str, Any]],
    treatment_input_migration_receipt: Mapping[str, Any] | None = None,
    control_input_migration_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run all 16 declared attempts for one fresh process without early stop or writes."""

    treatment_sources = validate_source_identity_set(
        treatment_source_identities, expected_sha256=TREATMENT_SOURCE_SET_SHA256
    )
    control_sources = validate_source_identity_set(control_source_identities, expected_sha256=CONTROL_SOURCE_SET_SHA256)
    treatment_by_seed = {int(value["seed"]): value for value in treatment_sources}
    control_by_seed = {int(value["seed"]): value for value in control_sources}
    if set(frozen_control_hashes) != set(RESTART_SCHEDULE):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_attempt_set_incomplete",
            "D1 frozen control hashes must cover seeds 42..49 exactly once",
        )
    normalized_control_hashes = {
        seed: {
            "entry_receipt_sha256": _require_sha256(
                frozen_control_hashes[seed].get("entry_receipt_sha256"),
                "expected_control_entry_receipt_sha256",
            ),
            "model_payload_sha256": _require_sha256(
                frozen_control_hashes[seed].get("model_payload_sha256"),
                "expected_control_model_payload_sha256",
            ),
        }
        for seed in RESTART_SCHEDULE
    }
    migrations: dict[str, dict[str, Any]] = {}
    for role, item, receipt in (
        (
            TREATMENT_ROLE,
            treatment_item,
            treatment_input_migration_receipt,
        ),
        (
            CONTROL_ROLE,
            control_item,
            control_input_migration_receipt,
        ),
    ):
        if not isinstance(receipt, Mapping):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_authority_mismatch",
                f"D1 {role} input is missing its approved migration receipt",
            )
        migrations[role] = validate_input_migration_receipt(receipt, item=item, expected_role=role)
    attempts: list[dict[str, Any]] = []
    for seed in RESTART_SCHEDULE:
        attempts.append(
            fit_controlled_attempt(
                treatment_item,
                preprocess=preprocess,
                role=TREATMENT_ROLE,
                seed=seed,
                process_identity=process_identity,
                numeric_environment=numeric_environment,
                source_identity=treatment_by_seed[seed],
                profile_receipt_sha256=TREATMENT_PROFILE_RECEIPT_SHA256,
                source_set_sha256=TREATMENT_SOURCE_SET_SHA256,
                preprocess_identity_sha256=PREPROCESS_IDENTITY_SHA256,
                feature_definition_sha256=FEATURE_DEFINITION_SHA256,
                input_migration_receipt=migrations.get(TREATMENT_ROLE),
            )
        )
        control_hashes = normalized_control_hashes[seed]
        attempts.append(
            fit_controlled_attempt(
                control_item,
                preprocess=preprocess,
                role=CONTROL_ROLE,
                seed=seed,
                process_identity=process_identity,
                numeric_environment=numeric_environment,
                source_identity=control_by_seed[seed],
                profile_receipt_sha256=CONTROL_PROFILE_RECEIPT_SHA256,
                source_set_sha256=CONTROL_SOURCE_SET_SHA256,
                preprocess_identity_sha256=PREPROCESS_IDENTITY_SHA256,
                feature_definition_sha256=FEATURE_DEFINITION_SHA256,
                input_migration_receipt=migrations.get(CONTROL_ROLE),
                expected_control_entry_receipt_sha256=_require_sha256(
                    control_hashes.get("entry_receipt_sha256"),
                    "expected_control_entry_receipt_sha256",
                ),
                expected_control_model_payload_sha256=_require_sha256(
                    control_hashes.get("model_payload_sha256"),
                    "expected_control_model_payload_sha256",
                ),
            )
        )
    return build_process_receipt(
        process_identity=process_identity,
        producer_commit=producer_commit,
        attempts=attempts,
        treatment_source_identities=treatment_sources,
        control_source_identities=control_sources,
        input_migration_receipts=migrations,
    )


def build_process_receipt(
    *,
    process_identity: str,
    producer_commit: str,
    attempts: Sequence[Mapping[str, Any]],
    treatment_source_identities: Sequence[Mapping[str, Any]],
    control_source_identities: Sequence[Mapping[str, Any]],
    input_migration_receipts: Mapping[str, Mapping[str, Any]] | None = None,
    _schema_version: str = PROCESS_SCHEMA_VERSION,
    _source_authority: Mapping[str, str] = SOURCE_AUTHORITY,
) -> dict[str, Any]:
    normalized_commit = _require_commit(producer_commit, "producer_commit")
    treatment_sources = validate_source_identity_set(
        treatment_source_identities, expected_sha256=TREATMENT_SOURCE_SET_SHA256
    )
    control_sources = validate_source_identity_set(control_source_identities, expected_sha256=CONTROL_SOURCE_SET_SHA256)
    if _schema_version not in {
        PROCESS_SCHEMA_VERSION_V1,
        PROCESS_SCHEMA_VERSION_V2,
        PROCESS_SCHEMA_VERSION,
    }:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 process schema version is invalid",
        )
    if dict(_source_authority) != dict(_process_source_authority(_schema_version)):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 process source authority does not match its schema version",
        )
    expected_attempt_schema = (
        ATTEMPT_SCHEMA_VERSION if _schema_version == PROCESS_SCHEMA_VERSION else ATTEMPT_SCHEMA_VERSION_V1
    )
    migrations: dict[str, dict[str, Any]] = {}
    raw_migrations = dict(input_migration_receipts or {})
    if _schema_version == PROCESS_SCHEMA_VERSION:
        if set(raw_migrations) != {TREATMENT_ROLE, CONTROL_ROLE}:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "D1 process must contain both approved input migration receipts",
            )
        migrations = {
            role: _validate_input_migration_envelope(receipt, expected_role=role)
            for role, receipt in sorted(raw_migrations.items())
        }
        if any(receipt.get("producer_commit") != normalized_commit for receipt in migrations.values()):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_authority_mismatch",
                "D1 input migration producer commit differs from the process authority",
            )
    elif raw_migrations:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 legacy process cannot contain input migration receipts",
        )
    ordered = sorted(
        (dict(value) for value in attempts), key=lambda value: (str(value.get("role")), int(value.get("seed", -1)))
    )
    expected_keys = {(role, seed) for role in (TREATMENT_ROLE, CONTROL_ROLE) for seed in RESTART_SCHEDULE}
    actual_keys = {(str(value.get("role")), int(value.get("seed", -1))) for value in ordered}
    if len(ordered) != 16 or actual_keys != expected_keys:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_attempt_set_incomplete",
            "D1 process receipt must contain exactly 16 terminal attempts",
        )
    source_by_key = {(TREATMENT_ROLE, int(value["seed"])): value for value in treatment_sources} | {
        (CONTROL_ROLE, int(value["seed"])): value for value in control_sources
    }
    for attempt in ordered:
        receipt = str(attempt.get("attempt_receipt_sha256") or "")
        body = {key: value for key, value in attempt.items() if key != "attempt_receipt_sha256"}
        role = str(attempt.get("role"))
        seed = int(attempt.get("seed", -1))
        source = source_by_key[(role, seed)]
        expected_sector = TREATMENT_SECTOR if role == TREATMENT_ROLE else CONTROL_SECTOR
        status = attempt.get("status")
        failure_reasons = attempt.get("failure_reason_codes")
        numeric_environment = attempt.get("numeric_environment")
        if (
            attempt.get("process_identity") != process_identity
            or canonical_sha256(body) != receipt
            or attempt.get("diagnostic_entry_sha256") != source["diagnostic_entry_sha256"]
            or attempt.get("source_entry_receipt_sha256") != source["source_entry_receipt_sha256"]
            or attempt.get("schema_version") != expected_attempt_schema
            or attempt.get("algorithm_version") != ALGORITHM_VERSION
            or attempt.get("family") != "autocycle_all_core"
            or attempt.get("level") != "L2"
            or attempt.get("sector_code") != expected_sector
            or (
                _schema_version == PROCESS_SCHEMA_VERSION
                and (
                    "input_migration_receipt_sha256" not in attempt
                    or attempt.get("input_migration_receipt_sha256")
                    != (migrations.get(role) or {}).get("receipt_sha256")
                )
            )
            or (
                _schema_version in {PROCESS_SCHEMA_VERSION_V1, PROCESS_SCHEMA_VERSION_V2}
                and "input_migration_receipt_sha256" in attempt
            )
            or status not in {"fit_completed", "fit_failed"}
            or attempt.get("fit_status") != ("accepted" if status == "fit_completed" else "failed")
            or not isinstance(failure_reasons, list)
            or (status == "fit_completed" and (failure_reasons or attempt.get("failure_stage") is not None))
            or (status == "fit_failed" and not failure_reasons)
            or not isinstance(numeric_environment, Mapping)
            or attempt.get("numeric_environment_sha256") != canonical_sha256(dict(numeric_environment))
            or any(
                attempt.get(field) is not False
                for field in (
                    "validation_accessed",
                    "future_utility_accessed",
                    "semantic_labelability_accessed",
                    "d6_status_accessed",
                    "selection_performed",
                    "model_write_performed",
                    "ready_artifact_write_performed",
                    "database_write_performed",
                    "runtime_action_performed",
                )
            )
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "D1 attempt receipt identity mismatch",
            )
        if status == "fit_completed" and (
            not isinstance(attempt.get("projection_receipt"), Mapping)
            or not isinstance(attempt.get("parameter_payload"), Mapping)
            or not isinstance(attempt.get("initialization_evidence"), Mapping)
            or not isinstance(attempt.get("monitor_evidence"), Mapping)
            or not isinstance(attempt.get("likelihood"), Mapping)
            or not isinstance(attempt.get("covariance"), Mapping)
            or not isinstance(attempt.get("train_occupancy"), Mapping)
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "D1 completed attempt evidence is incomplete",
            )
    comparable_attempts = [
        {key: value for key, value in attempt.items() if key not in {"process_identity", "attempt_receipt_sha256"}}
        for attempt in ordered
    ]
    numeric_environment_hashes = {
        _require_sha256(value.get("numeric_environment_sha256"), "numeric_environment_sha256") for value in ordered
    }
    if len(numeric_environment_hashes) != 1:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 process attempts do not share one numeric environment identity",
        )
    body = {
        "schema_version": _schema_version,
        "algorithm_version": ALGORITHM_VERSION,
        "process_identity": process_identity,
        "producer_commit": normalized_commit,
        "source_authority": dict(_source_authority),
        "attempts": ordered,
        "attempt_count": len(ordered),
        "terminal_attempt_count": sum(value.get("status") in {"fit_completed", "fit_failed"} for value in ordered),
        "treatment_source_identities": list(treatment_sources),
        "control_source_identities": list(control_sources),
        "treatment_source_identity_set_sha256": TREATMENT_SOURCE_SET_SHA256,
        "control_source_identity_set_sha256": CONTROL_SOURCE_SET_SHA256,
        "numeric_environment_sha256": next(iter(numeric_environment_hashes)),
        "comparable_payload_sha256": canonical_sha256(comparable_attempts),
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    if _schema_version == PROCESS_SCHEMA_VERSION:
        body["input_migration_receipts"] = migrations
    return {**body, "process_receipt_sha256": canonical_sha256(body)}


def validate_process_receipt(
    value: Mapping[str, Any],
    *,
    expected_process_identity: str,
    expected_producer_commit: str,
) -> dict[str, Any]:
    """Rebuild a child receipt with the writer authority before the parent trusts it."""

    normalized = dict(value)
    schema_version = str(normalized.get("schema_version") or "")
    source_authority = _process_source_authority(schema_version)
    if (
        normalized.get("algorithm_version") != ALGORITHM_VERSION
        or normalized.get("process_identity") != expected_process_identity
        or normalized.get("producer_commit") != _require_commit(expected_producer_commit, "expected_producer_commit")
        or normalized.get("source_authority") != source_authority
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 process authority fields are invalid",
        )
    attempts = normalized.get("attempts")
    treatment_sources = normalized.get("treatment_source_identities")
    control_sources = normalized.get("control_source_identities")
    migrations = normalized.get("input_migration_receipts") if schema_version == PROCESS_SCHEMA_VERSION else {}
    if (
        not isinstance(attempts, list)
        or not isinstance(treatment_sources, list)
        or not isinstance(control_sources, list)
        or not isinstance(migrations, Mapping)
        or (
            schema_version in {PROCESS_SCHEMA_VERSION_V1, PROCESS_SCHEMA_VERSION_V2}
            and "input_migration_receipts" in normalized
        )
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 process receipt collections are invalid",
        )
    rebuilt = build_process_receipt(
        process_identity=expected_process_identity,
        producer_commit=expected_producer_commit,
        attempts=attempts,
        treatment_source_identities=treatment_sources,
        control_source_identities=control_sources,
        input_migration_receipts=migrations,
        _schema_version=schema_version,
        _source_authority=source_authority,
    )
    if rebuilt != normalized:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 child process readback differs from the process writer authority",
        )
    return normalized


def build_controlled_refit_report(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
    *,
    producer_commit: str,
    _schema_version: str = REPORT_SCHEMA_VERSION,
    _source_authority: Mapping[str, str] = SOURCE_AUTHORITY,
) -> dict[str, Any]:
    normalized_commit = _require_commit(producer_commit, "producer_commit")
    if dict(_source_authority) != dict(_report_source_authority(_schema_version)):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 report source authority does not match its schema version",
        )
    reasons: list[str] = []
    processes = [dict(first), dict(second)]
    expected_process_identities = ("fresh_process_1", "fresh_process_2")
    for process, expected_process_identity in zip(processes, expected_process_identities, strict=True):
        if any(
            process.get(field) is not False
            for field in (
                "selection_performed",
                "model_write_performed",
                "ready_artifact_write_performed",
                "database_write_performed",
                "runtime_action_performed",
            )
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "D1 process receipt contains a forbidden side-effect flag",
            )
        try:
            validate_process_receipt(
                process,
                expected_process_identity=expected_process_identity,
                expected_producer_commit=normalized_commit,
            )
        except D1InactiveDimensionError as exc:
            reasons.append(exc.reason_code)
        receipt = str(process.get("process_receipt_sha256") or "")
        body = {key: value for key, value in process.items() if key != "process_receipt_sha256"}
        process_attempts = list(process.get("attempts") or ())
        comparable_attempts = [
            {key: value for key, value in attempt.items() if key not in {"process_identity", "attempt_receipt_sha256"}}
            for attempt in process_attempts
        ]
        if (
            canonical_sha256(body) != receipt
            or process.get("attempt_count") != 16
            or process.get("terminal_attempt_count") != 16
            or process.get("comparable_payload_sha256") != canonical_sha256(comparable_attempts)
        ):
            reasons.append("hmm_risk_model_inactive_dimension_attempt_set_incomplete")
    process_identities = tuple(str(value.get("process_identity") or "") for value in processes)
    if process_identities != expected_process_identities:
        reasons.append("hmm_risk_model_inactive_dimension_repeat_mismatch")
    repeat_equal = first.get("comparable_payload_sha256") == second.get("comparable_payload_sha256")
    if not repeat_equal:
        reasons.append("hmm_risk_model_inactive_dimension_repeat_mismatch")
    attempts = list(first.get("attempts") or ())
    treatment = [value for value in attempts if value.get("role") == TREATMENT_ROLE]
    controls = [value for value in attempts if value.get("role") == CONTROL_ROLE]
    if len(treatment) != 8 or len(controls) != 8:
        reasons.append("hmm_risk_model_inactive_dimension_attempt_set_incomplete")
    if any(value.get("control_payload_bitwise_equal") is not True for value in controls):
        reasons.append("hmm_risk_model_inactive_dimension_control_drift")
    treatment_reasons = {str(reason) for attempt in treatment for reason in (attempt.get("failure_reason_codes") or ())}
    all_attempt_reasons = {
        str(reason) for attempt in attempts for reason in (attempt.get("failure_reason_codes") or ())
    }
    if reasons:
        mechanism = "inconclusive"
    elif treatment_reasons & _D1_REJECTION_REASONS:
        mechanism = "constant_dimension_mechanism_rejected"
    else:
        mechanism = "constant_dimension_effect_supported"
    d4_ready = all(
        attempt.get("status") == "fit_completed"
        and isinstance(attempt.get("final_train_log_likelihood"), (int, float))
        and math.isfinite(float(attempt["final_train_log_likelihood"]))
        and all(isinstance(attempt.get(field), Mapping) for field in ("likelihood", "covariance", "train_occupancy"))
        for attempt in treatment
    )
    actual_attempt_count = sum(len(process.get("attempts") or ()) for process in processes)
    body = {
        "schema_version": _schema_version,
        "diagnostic_contract": "C-008-B3-REMEDIATION-D1-B-REFIT-01",
        "producer_commit": normalized_commit,
        "source_authority": dict(_source_authority),
        "status": "diagnostic_complete" if not reasons else "diagnostic_incomplete",
        "mechanism_assessment": mechanism,
        "mechanism_assessment_reason_codes": sorted(set(reasons) | all_attempt_reasons),
        "d5_compatibility_evidence_ready": bool(not reasons and repeat_equal and d4_ready),
        "process_receipts": processes,
        "canonical_payload_bitwise_equal": repeat_equal,
        "attempt_count": actual_attempt_count,
        "selection_performed": False,
        "d3_d4_descriptive_contracts_applied": True,
        "formal_model_set_acceptance_performed": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def validate_controlled_refit_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """Use the report writer authority for durable success/incomplete readback."""

    normalized = dict(report)
    schema_version = str(normalized.get("schema_version") or "")
    source_authority = _report_source_authority(schema_version)
    if (
        normalized.get("diagnostic_contract") != "C-008-B3-REMEDIATION-D1-B-REFIT-01"
        or normalized.get("status") not in {"diagnostic_complete", "diagnostic_incomplete"}
        or normalized.get("source_authority") != source_authority
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 controlled-refit report authority fields are invalid",
        )
    processes = normalized.get("process_receipts")
    if (
        not isinstance(processes, list)
        or len(processes) != 2
        or any(not isinstance(value, Mapping) for value in processes)
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 controlled-refit report must contain exactly two process receipts",
        )
    rebuilt = build_controlled_refit_report(
        processes[0],
        processes[1],
        producer_commit=_require_commit(normalized.get("producer_commit"), "producer_commit"),
        _schema_version=schema_version,
        _source_authority=source_authority,
    )
    if rebuilt != normalized:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 controlled-refit report readback differs from the report writer authority",
        )
    return normalized


def _refit02_projection_receipt(
    *,
    raw: np.ndarray,
    preprocessed: np.ndarray,
    role: str,
    current_authority_sha256: str,
    exact_zero: Mapping[str, Any],
) -> tuple[np.ndarray, dict[str, Any]]:
    if role not in REFIT02_ROLES:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 role is invalid",
        )
    treatment = role == REFIT02_TREATMENT_ROLE
    active = list(range(INACTIVE_FEATURE_INDEX)) if treatment else list(range(len(ALL_CORE_FEATURES)))
    inactive = [INACTIVE_FEATURE_INDEX] if treatment else []
    projected = np.ascontiguousarray(preprocessed[:, active], dtype="<f8")
    body = {
        "schema_version": "hmm_risk_c008_b3_d1_projection_receipt_v3",
        "algorithm_version": REFIT02_ALGORITHM_VERSION,
        "role": role,
        "full_feature_names": list(ALL_CORE_FEATURES),
        "full_feature_count": len(ALL_CORE_FEATURES),
        "active_feature_indices": active,
        "inactive_feature_indices": inactive,
        "active_feature_mask": [index in active for index in range(len(ALL_CORE_FEATURES))],
        "likelihood_feature_count": len(active),
        "current_a5_experiment_authority_sha256": _require_sha256(
            current_authority_sha256,
            "current_a5_experiment_authority_sha256",
        ),
        "raw_full20_identity": _float64_array_identity(raw),
        "preprocessed_full20_identity": _float64_array_identity(preprocessed),
        "projected_identity": _float64_array_identity(projected),
        "exact_zero_evidence": dict(exact_zero),
        "dynamic_activation": False,
    }
    return projected, {**body, "projection_sha256": canonical_sha256(body)}


def _refit03_feature_context(role: str) -> tuple[tuple[str, ...], tuple[bool, ...]]:
    if role == REFIT02_TREATMENT_ROLE:
        return tuple(ALL_CORE_FEATURES[:19]), (False,) * 19
    if role == REFIT02_MATCHED_NEGATIVE_ROLE:
        return tuple(ALL_CORE_FEATURES), (False,) * 19 + (True,)
    if role == REFIT02_HARNESS_ROLE:
        return tuple(ALL_CORE_FEATURES), (False,) * 20
    raise D1InactiveDimensionError(
        "hmm_risk_model_inactive_dimension_contract_invalid",
        "REFIT-03 covariance evidence role is invalid",
    )


def _validate_refit03_raw_capture(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(value)
    body = {key: item for key, item in normalized.items() if key != "capture_receipt_sha256"}
    reasons = normalized.get("reason_codes")
    cells = normalized.get("cells")
    if (
        normalized.get("schema_version") != REFIT03_RAW_COVARIANCE_SCHEMA_VERSION
        or normalized.get("raw_authority") != REFIT03_RAW_COVARIANCE_AUTHORITY
        or not isinstance(reasons, list)
        or any(not isinstance(reason, str) or not reason for reason in reasons)
        or len(reasons) != len(set(reasons))
        or not isinstance(cells, list)
        or not isinstance(normalized.get("raw_validity"), bool)
        or canonical_sha256(body) != normalized.get("capture_receipt_sha256")
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_covariance_evidence_incomplete",
            "REFIT-03 raw covariance capture receipt is invalid",
        )
    expected_shape = normalized.get("expected_shape")
    actual_shape = normalized.get("actual_shape")
    if (
        not isinstance(expected_shape, list)
        or len(expected_shape) != 2
        or any(not isinstance(item, int) or isinstance(item, bool) or item <= 0 for item in expected_shape)
        or (
            actual_shape is not None
            and (
                not isinstance(actual_shape, list)
                or any(not isinstance(item, int) or isinstance(item, bool) or item < 0 for item in actual_shape)
            )
        )
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_covariance_evidence_incomplete",
            "REFIT-03 raw covariance shape metadata is invalid",
        )

    inferred_reasons: list[str] = []
    if normalized.get("actual_python_type") != "numpy.ndarray":
        inferred_reasons.append("hmm_risk_model_covariance_raw_type_invalid")
    dtype: np.dtype[Any] | None = None
    if normalized.get("actual_dtype") is not None:
        try:
            dtype = np.dtype(str(normalized["actual_dtype"]))
        except (TypeError, ValueError):
            dtype = None
    if actual_shape is not None and (dtype is None or dtype.kind != "f" or dtype.itemsize != 8):
        inferred_reasons.append("hmm_risk_model_covariance_raw_dtype_invalid")
    if actual_shape is not None and (len(actual_shape) != 2 or actual_shape != expected_shape):
        inferred_reasons.append("hmm_risk_model_covariance_raw_shape_invalid")
    if actual_shape is not None and normalized.get("c_contiguous") is not True:
        inferred_reasons.append("hmm_risk_model_covariance_raw_layout_invalid")

    allowed_classifications = {
        "finite_positive",
        "positive_zero",
        "negative_zero",
        "finite_negative",
        "nan",
        "positive_infinity",
        "negative_infinity",
    }
    coordinates: set[tuple[int, int]] = set()
    non_finite = False
    non_positive = False
    for raw_cell in cells:
        if not isinstance(raw_cell, Mapping):
            raise D1InactiveDimensionError(
                "hmm_risk_model_covariance_evidence_incomplete",
                "REFIT-03 raw covariance cell is not a mapping",
            )
        state_index = raw_cell.get("state_index")
        feature_index = raw_cell.get("feature_index")
        bit_pattern = raw_cell.get("semantic_bit_pattern_hex")
        classification = raw_cell.get("classification")
        if (
            not isinstance(state_index, int)
            or isinstance(state_index, bool)
            or state_index < 0
            or not isinstance(feature_index, int)
            or isinstance(feature_index, bool)
            or feature_index < 0
            or not isinstance(bit_pattern, str)
            or len(bit_pattern) != 16
            or any(char not in "0123456789abcdef" for char in bit_pattern)
            or classification not in allowed_classifications
            or (state_index, feature_index) in coordinates
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_covariance_evidence_incomplete",
                "REFIT-03 raw covariance cell identity is invalid",
            )
        coordinates.add((state_index, feature_index))
        bits = int(bit_pattern, 16)
        sign = (bits >> 63) & 1
        exponent = (bits >> 52) & 0x7FF
        fraction = bits & ((1 << 52) - 1)
        if exponent == 0x7FF:
            expected_classification = "nan" if fraction else "negative_infinity" if sign else "positive_infinity"
        elif exponent == 0 and fraction == 0:
            expected_classification = "negative_zero" if sign else "positive_zero"
        else:
            expected_classification = "finite_negative" if sign else "finite_positive"
        if classification != expected_classification:
            raise D1InactiveDimensionError(
                "hmm_risk_model_covariance_bitpattern_conflict",
                "REFIT-03 raw covariance classification disagrees with its bit pattern",
            )
        finite = classification not in {"nan", "positive_infinity", "negative_infinity"}
        float_hex = raw_cell.get("float_hex")
        if finite:
            try:
                round_trip = float.fromhex(str(float_hex))
                round_trip_bits = struct.unpack(">Q", struct.pack(">d", round_trip))[0]
            except (TypeError, ValueError, OverflowError):
                round_trip_bits = -1
            if round_trip_bits != bits:
                raise D1InactiveDimensionError(
                    "hmm_risk_model_covariance_bitpattern_conflict",
                    "REFIT-03 finite covariance hex value disagrees with its bit pattern",
                )
        elif float_hex is not None:
            raise D1InactiveDimensionError(
                "hmm_risk_model_covariance_bitpattern_conflict",
                "REFIT-03 non-finite covariance cell contains a JSON float value",
            )
        non_finite = non_finite or not finite
        non_positive = non_positive or classification in {
            "positive_zero",
            "negative_zero",
            "finite_negative",
        }

    if dtype is not None and dtype.kind == "f" and dtype.itemsize == 8 and isinstance(actual_shape, list):
        expected_cell_count = math.prod(actual_shape) if len(actual_shape) == 2 else 0
        if len(cells) != expected_cell_count:
            raise D1InactiveDimensionError(
                "hmm_risk_model_covariance_evidence_incomplete",
                "REFIT-03 raw covariance logical-cell evidence is incomplete",
            )
    elif cells:
        raise D1InactiveDimensionError(
            "hmm_risk_model_covariance_evidence_incomplete",
            "REFIT-03 unsupported raw covariance carries fabricated logical cells",
        )
    if non_finite:
        inferred_reasons.append("hmm_risk_model_covariance_raw_non_finite")
    if non_positive:
        inferred_reasons.append("hmm_risk_model_covariance_raw_non_positive")
    inferred_reasons = list(dict.fromkeys(inferred_reasons))
    if reasons != inferred_reasons or normalized.get("raw_validity") is not (not inferred_reasons):
        raise D1InactiveDimensionError(
            "hmm_risk_model_covariance_bitpattern_conflict",
            "REFIT-03 raw covariance reason summary disagrees with its evidence",
        )
    return normalized


def _bind_refit03_covariance_evidence(
    raw_capture: Mapping[str, Any],
    *,
    role: str,
    stage_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    capture = _validate_refit03_raw_capture(raw_capture)
    feature_names, inactive_mask = _refit03_feature_context(role)
    feature_order_sha256 = canonical_sha256(list(feature_names))
    inactive_mask_sha256 = canonical_sha256(list(inactive_mask))
    cells: list[dict[str, Any]] = []
    classification_by_state: dict[str, dict[str, int]] = {}
    classification_by_feature: dict[str, dict[str, int]] = {}
    raw_invalid_feature_indices: set[int] = set()
    for raw_cell in capture["cells"]:
        state_index = int(raw_cell.get("state_index", -1))
        feature_index = int(raw_cell.get("feature_index", -1))
        classification = str(raw_cell.get("classification") or "")
        feature_name = feature_names[feature_index] if 0 <= feature_index < len(feature_names) else None
        inactive = bool(inactive_mask[feature_index]) if 0 <= feature_index < len(inactive_mask) else False
        cell = {
            **dict(raw_cell),
            "feature_name": feature_name,
            "is_inactive_coordinate": inactive,
        }
        cells.append(cell)
        state_counts = classification_by_state.setdefault(str(state_index), {})
        state_counts[classification] = state_counts.get(classification, 0) + 1
        feature_counts = classification_by_feature.setdefault(str(feature_index), {})
        feature_counts[classification] = feature_counts.get(classification, 0) + 1
        if classification != "finite_positive":
            raw_invalid_feature_indices.add(feature_index)

    actual_shape = capture.get("actual_shape")
    expected_shape = [3, len(feature_names)]
    capture_expected_shape = capture.get("expected_shape")
    frame_header = {
        "schema_version": REFIT03_RAW_FRAME_HEADER_SCHEMA_VERSION,
        "raw_authority": REFIT03_RAW_COVARIANCE_AUTHORITY,
        "actual_dtype": capture.get("actual_dtype"),
        "actual_shape": actual_shape,
        "actual_strides": capture.get("actual_strides"),
        "feature_order_sha256": feature_order_sha256,
        "inactive_mask_sha256": inactive_mask_sha256,
    }
    dtype_valid = False
    try:
        dtype = np.dtype(str(capture.get("actual_dtype") or ""))
        dtype_valid = dtype.kind == "f" and dtype.itemsize == 8
    except (TypeError, ValueError):
        dtype_valid = False
    frame_eligible = bool(
        capture.get("actual_python_type") == "numpy.ndarray"
        and dtype_valid
        and isinstance(actual_shape, list)
        and len(actual_shape) == 2
        and capture.get("c_contiguous") is True
        and len(cells) == int(actual_shape[0]) * int(actual_shape[1])
    )
    payload_sha256: str | None = None
    if frame_eligible:
        header_bytes = canonical_json_bytes(frame_header)
        cell_bits = b"".join(struct.pack(">Q", int(str(cell["semantic_bit_pattern_hex"]), 16)) for cell in cells)
        payload_sha256 = sha256_bytes(struct.pack(">Q", len(header_bytes)) + header_bytes + cell_bits)

    stage_cause = dict(stage_evidence.get("stage_specific_cause_evidence") or {})
    covariance_stage = dict(stage_evidence.get("covariance_evidence") or {})
    acceptance = dict(covariance_stage.get("acceptance") or {})
    acceptance_evidence = dict(acceptance.get("evidence") or {})
    if covariance_stage.get("dynamic_lower_reference") is not None:
        derived_status = "computed"
        covariance_status = str(acceptance.get("covariance_status") or "") or None
        covariance_valid = acceptance.get("covariance_valid") is True
    else:
        derived_status = str(stage_cause.get("d4_derived_evidence_status") or "") or None
        covariance_status = str(stage_cause.get("covariance_status") or "") or None
        covariance_valid = False
    derived_fields = {
        "state_posterior_mass": covariance_stage.get("state_posterior_mass"),
        "posterior_weighted_variance_about_weighted_mean": covariance_stage.get(
            "posterior_weighted_variance_about_weighted_mean"
        ),
        "posterior_second_moment_about_fitted_mean": covariance_stage.get("posterior_second_moment_about_fitted_mean"),
        "mstep_expected_covariance": covariance_stage.get("mstep_expected_covariance"),
        "dynamic_lower_reference": covariance_stage.get("dynamic_lower_reference"),
        "dynamic_upper_reference": covariance_stage.get("dynamic_upper_reference"),
        "mstep_relative_residual": covariance_stage.get("mstep_relative_residual"),
    }
    bound_anomaly_feature_indices = {
        index
        for index, count in enumerate(acceptance_evidence.get("per_feature_anomaly_count") or ())
        if int(count) > 0
    }
    residual_failure_feature_indices: set[int] = set()
    for row in acceptance_evidence.get("mstep_relative_residual") or ():
        if not isinstance(row, Sequence) or isinstance(row, (str, bytes, bytearray)):
            continue
        for feature_index, residual in enumerate(row):
            if isinstance(residual, (int, float)) and math.isfinite(float(residual)) and float(residual) > 0.02:
                residual_failure_feature_indices.add(feature_index)
    invalid_feature_indices = (
        raw_invalid_feature_indices | bound_anomaly_feature_indices | residual_failure_feature_indices
    )
    derived_failure_reason_codes = list(
        dict.fromkeys(
            [
                *(acceptance.get("failure_reason_codes") or ()),
                *(acceptance.get("blocking_reason_codes") or ()),
            ]
        )
    )
    derived_warning_reason_codes = list(dict.fromkeys(acceptance.get("warning_reason_codes") or ()))
    if bound_anomaly_feature_indices:
        derived_failure_reason_codes.append("hmm_risk_model_covariance_raw_bounds_failed")
    if isinstance(derived_status, str) and derived_status.startswith("not_computable_"):
        derived_failure_reason_codes.append("hmm_risk_model_covariance_derived_evidence_not_computable")
    derived_failure_reason_codes = list(dict.fromkeys(str(reason) for reason in derived_failure_reason_codes))
    derived_warning_reason_codes = list(dict.fromkeys(str(reason) for reason in derived_warning_reason_codes))
    derived_reason_codes = list(dict.fromkeys([*derived_failure_reason_codes, *derived_warning_reason_codes]))
    initialization_evidence = dict(stage_evidence.get("initialization_evidence") or {})
    d4_formula_identity = {
        "contract_version": D4_COVARIANCE_VERSION,
        "nu": initialization_evidence.get("nu"),
        "bound_tolerance": 0.005,
        "mstep_relative_residual_max": 0.02,
        "lower_formula": "nu*R_sj/(nu+M_k)",
        "upper_formula": "(nu+N_train)*R_sj/(nu+M_k)",
        "mstep_expected_formula": "(nu*R_sj+M_k*W_kj)/(nu+M_k)",
        "postfit_projection_allowed": False,
    }
    expected_cell_count = 3 * len(feature_names)
    diagnostic_evidence_complete = bool(
        frame_eligible
        and capture_expected_shape == expected_shape
        and actual_shape == expected_shape
        and len(cells) == expected_cell_count
        and derived_status
        in {
            "computed",
            "not_computable_raw_covariance_invalid",
            "not_computable_posterior_audit_unavailable",
            "not_computable_posterior_audit_invalid",
        }
    )
    body = {
        "schema_version": REFIT03_COVARIANCE_EVIDENCE_SCHEMA_VERSION,
        "raw_authority": REFIT03_RAW_COVARIANCE_AUTHORITY,
        "role": role,
        "expected_shape": expected_shape,
        "capture_expected_shape": capture_expected_shape,
        "actual_python_type": capture.get("actual_python_type"),
        "actual_dtype": capture.get("actual_dtype"),
        "actual_shape": actual_shape,
        "actual_strides": capture.get("actual_strides"),
        "actual_nbytes": capture.get("actual_nbytes"),
        "actual_byteorder": capture.get("actual_byteorder"),
        "c_contiguous": capture.get("c_contiguous"),
        "feature_names": list(feature_names),
        "feature_order_sha256": feature_order_sha256,
        "inactive_mask": list(inactive_mask),
        "inactive_mask_sha256": inactive_mask_sha256,
        "cells": cells,
        "classification_count_by_state": classification_by_state,
        "classification_count_by_feature": classification_by_feature,
        "raw_invalid_feature_indices": sorted(raw_invalid_feature_indices),
        "d4_bound_anomaly_feature_indices": sorted(bound_anomaly_feature_indices),
        "d4_residual_failure_feature_indices": sorted(residual_failure_feature_indices),
        "invalid_feature_indices": sorted(invalid_feature_indices),
        "raw_reason_codes": list(capture.get("reason_codes") or ()),
        "derived_failure_reason_codes": derived_failure_reason_codes,
        "derived_warning_reason_codes": derived_warning_reason_codes,
        "derived_reason_codes": derived_reason_codes,
        "evidence_unavailable_reason": (
            stage_cause.get("evidence_unavailable_reason") or capture.get("evidence_unavailable_reason")
        ),
        "raw_frame_header": frame_header if frame_eligible else None,
        "raw_covariance_payload_sha256": payload_sha256,
        "raw_capture_receipt_sha256": capture.get("capture_receipt_sha256"),
        "sector_local_reference_variance_R_sj": initialization_evidence.get("sector_local_reference_variance_R_sj"),
        "covariance_prior_nu": initialization_evidence.get("nu"),
        "d4_threshold_version": D4_COVARIANCE_VERSION,
        "d4_formula_identity": d4_formula_identity,
        "d4_formula_identity_sha256": canonical_sha256(d4_formula_identity),
        "d4_derived_evidence_status": derived_status,
        "covariance_status": covariance_status,
        "covariance_valid": covariance_valid,
        **derived_fields,
        "diagnostic_evidence_complete": diagnostic_evidence_complete,
        "cross_role_state_alignment_performed": False,
        "semantic_label_accessed": False,
    }
    return {**body, "covariance_evidence_sha256": canonical_sha256(body)}


def _bind_refit03_stage_evidence(value: Mapping[str, Any], *, role: str) -> dict[str, Any]:
    normalized = dict(value)
    body = {key: item for key, item in normalized.items() if key != "stage_evidence_sha256"}
    completed_stages = normalized.get("completed_stages")
    ordered_stages = [
        "initialization",
        "fit",
        "raw_covariance_capture",
        "monitor",
        "likelihood",
        "covariance",
        "train_posterior",
    ]
    if (
        normalized.get("schema_version") != REFIT03_STAGE_EVIDENCE_SCHEMA_VERSION
        or not isinstance(normalized.get("fit_invoked"), bool)
        or not isinstance(normalized.get("fit_returned"), bool)
        or normalized.get("fit_returned") is True
        and normalized.get("fit_invoked") is not True
        or not isinstance(completed_stages, list)
        or completed_stages != ordered_stages[: len(completed_stages)]
        or not isinstance(normalized.get("initialization_evidence"), Mapping)
        or not isinstance(normalized.get("stage_specific_cause_evidence"), Mapping)
        or (normalized.get("fit_invoked") is False and completed_stages not in ([], ["initialization"]))
        or (
            normalized.get("fit_invoked") is True
            and normalized.get("fit_returned") is False
            and completed_stages != ["initialization"]
        )
        or (normalized.get("fit_returned") is True and completed_stages[:2] != ["initialization", "fit"])
        or (
            "raw_covariance_capture" in completed_stages
            and not isinstance(normalized.get("raw_covariance_evidence"), Mapping)
        )
        or (
            isinstance(normalized.get("raw_covariance_evidence"), Mapping)
            and normalized.get("fit_returned") is not True
        )
        or ("monitor" in completed_stages and not isinstance(normalized.get("monitor_evidence"), Mapping))
        or ("likelihood" in completed_stages and not isinstance(normalized.get("likelihood_evidence"), Mapping))
        or ("covariance" in completed_stages and not isinstance(normalized.get("covariance_evidence"), Mapping))
        or canonical_sha256(body) != normalized.get("stage_evidence_sha256")
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_training_stage_evidence_incomplete",
            "REFIT-03 training-stage evidence is invalid",
        )
    raw_capture = normalized.get("raw_covariance_evidence")
    if not isinstance(raw_capture, Mapping):
        return normalized
    bound = _bind_refit03_covariance_evidence(
        raw_capture,
        role=role,
        stage_evidence=normalized,
    )
    rebound = {
        **body,
        "raw_covariance_evidence": bound,
    }
    return {**rebound, "stage_evidence_sha256": canonical_sha256(rebound)}


def fit_refit02_attempt(
    item: B3TrainOnlySeries,
    *,
    preprocess: Mapping[str, Any],
    role: str,
    seed: int,
    process_identity: str,
    numeric_environment: Mapping[str, Any],
    current_authority: Mapping[str, Any],
) -> dict[str, Any]:
    if role not in REFIT02_ROLES or seed not in RESTART_SCHEDULE:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_attempt_set_incomplete",
            "REFIT-02 role or seed is outside the approved schedule",
        )
    expected_sector = TREATMENT_SECTOR if role != REFIT02_HARNESS_ROLE else CONTROL_SECTOR
    if item.sector_code != expected_sector:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_authority_mismatch",
            "REFIT-02 attempt sector differs from its role",
        )
    authority = dict(current_authority)
    authority_body = {key: value for key, value in authority.items() if key != "receipt_sha256"}
    if (
        authority.get("schema_version") != REFIT02_AUTHORITY_SCHEMA_VERSION
        or authority.get("algorithm_version") != REFIT02_ALGORITHM_VERSION
        or canonical_sha256(authority_body) != authority.get("receipt_sha256")
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_authority_mismatch",
            "REFIT-02 attempt current-A5 authority is invalid",
        )
    role_input, raw, preprocessed, exact_zero, eligible = _refit02_role_input(item, preprocess=preprocess)
    expected_input = ((authority.get("experiment_authority") or {}).get("role_inputs") or {}).get(role)
    if role_input != expected_input:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_authority_mismatch",
            "REFIT-02 attempt input differs from the frozen current-A5 role input",
        )
    if role != REFIT02_HARNESS_ROLE and not eligible:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_profile_not_applicable",
            "REFIT-02 current treatment profile is not exact-zero eligible",
            evidence=exact_zero,
        )
    projected, projection = _refit02_projection_receipt(
        raw=raw,
        preprocessed=preprocessed,
        role=role,
        current_authority_sha256=str(authority.get("current_a5_experiment_authority_sha256") or ""),
        exact_zero=exact_zero,
    )
    core: B3CoreFitEvidence | None = None
    failure_stage: str | None = None
    failure_reason_codes: list[str] = []
    failure_message: str | None = None
    hmm_fit_invoked = False
    training_stage_evidence: dict[str, Any] | None = None
    try:
        core = fit_b3_preprocessed_train_only(item, train=projected, seed=seed)
        hmm_fit_invoked = True
        if isinstance(core.training_stage_evidence, Mapping):
            training_stage_evidence = _bind_refit03_stage_evidence(
                core.training_stage_evidence,
                role=role,
            )
        _validate_projected_parameter_shapes(core, feature_count=int(projected.shape[1]))
    except D1InactiveDimensionError as exc:
        hmm_fit_invoked = True
        failure_stage = (
            "training_stage_evidence"
            if exc.reason_code
            in {
                "hmm_risk_model_training_stage_evidence_incomplete",
                "hmm_risk_model_covariance_evidence_incomplete",
                "hmm_risk_model_covariance_bitpattern_conflict",
            }
            else "parameter_shape"
        )
        failure_reason_codes.append(exc.reason_code)
        failure_message = str(exc)
    except B3TrainingStageError as exc:
        if isinstance(exc.stage_evidence, Mapping) and exc.stage_evidence:
            try:
                training_stage_evidence = _bind_refit03_stage_evidence(exc.stage_evidence, role=role)
                hmm_fit_invoked = training_stage_evidence.get("fit_invoked") is True
            except D1InactiveDimensionError as binding_exc:
                failure_reason_codes.append(binding_exc.reason_code)
                hmm_fit_invoked = exc.stage != "initialization"
                fallback_body = {
                    "schema_version": REFIT03_STAGE_EVIDENCE_SCHEMA_VERSION,
                    "fit_invoked": hmm_fit_invoked,
                    "fit_returned": exc.stage not in {"initialization", "fit"},
                    "completed_stages": [],
                    "initialization_evidence": {},
                    "monitor_evidence": None,
                    "likelihood_evidence": None,
                    "raw_covariance_evidence": None,
                    "covariance_evidence": None,
                    "stage_specific_cause_evidence": {
                        "error_type": binding_exc.__class__.__name__,
                        "error": str(binding_exc),
                        "evidence_status": "invalid_training_stage_evidence",
                    },
                }
                training_stage_evidence = {
                    **fallback_body,
                    "stage_evidence_sha256": canonical_sha256(fallback_body),
                }
        else:
            hmm_fit_invoked = exc.stage != "initialization"
            fallback_body = {
                "schema_version": REFIT03_STAGE_EVIDENCE_SCHEMA_VERSION,
                "fit_invoked": hmm_fit_invoked,
                "fit_returned": exc.stage not in {"initialization", "fit"},
                "completed_stages": [],
                "initialization_evidence": {},
                "monitor_evidence": None,
                "likelihood_evidence": None,
                "raw_covariance_evidence": None,
                "covariance_evidence": None,
                "stage_specific_cause_evidence": {
                    "error_type": exc.cause_type,
                    "error": str(exc),
                    "evidence_status": "missing_from_training_stage_error",
                },
            }
            training_stage_evidence = {
                **fallback_body,
                "stage_evidence_sha256": canonical_sha256(fallback_body),
            }
        failure_stage = exc.stage
        failure_reason_codes.append(exc.reason_code)
        failure_message = str(exc)
    if core is not None and training_stage_evidence is None:
        fallback_body = {
            "schema_version": REFIT03_STAGE_EVIDENCE_SCHEMA_VERSION,
            "fit_invoked": True,
            "fit_returned": True,
            "completed_stages": [],
            "initialization_evidence": dict(core.initialization),
            "monitor_evidence": dict(core.monitor_evidence),
            "likelihood_evidence": dict(core.likelihood),
            "raw_covariance_evidence": None,
            "covariance_evidence": dict(core.covariance),
            "stage_specific_cause_evidence": {
                "evidence_status": "missing_from_successful_core_fit_evidence",
            },
        }
        training_stage_evidence = {
            **fallback_body,
            "stage_evidence_sha256": canonical_sha256(fallback_body),
        }
    raw_covariance_evidence = (
        None
        if not isinstance(training_stage_evidence, Mapping)
        else training_stage_evidence.get("raw_covariance_evidence")
    )
    if isinstance(raw_covariance_evidence, Mapping):
        failure_reason_codes.extend(raw_covariance_evidence.get("raw_reason_codes") or ())
        failure_reason_codes.extend(raw_covariance_evidence.get("derived_failure_reason_codes") or ())
        if core is not None and failure_reason_codes and failure_stage is None:
            failure_stage = "covariance"
            failure_message = "D4 covariance evidence did not satisfy the existing acceptance contract"
    initialization_evidence = (
        dict(core.initialization)
        if core is not None
        else (
            dict(training_stage_evidence.get("initialization_evidence") or {})
            if isinstance(training_stage_evidence, Mapping)
            else None
        )
    )
    monitor_evidence = (
        dict(core.monitor_evidence)
        if core is not None
        else (
            dict(training_stage_evidence.get("monitor_evidence") or {})
            if isinstance(training_stage_evidence, Mapping)
            and isinstance(training_stage_evidence.get("monitor_evidence"), Mapping)
            else None
        )
    )
    likelihood_evidence = (
        dict(core.likelihood)
        if core is not None
        else (
            dict(training_stage_evidence.get("likelihood_evidence") or {})
            if isinstance(training_stage_evidence, Mapping)
            and isinstance(training_stage_evidence.get("likelihood_evidence"), Mapping)
            else None
        )
    )
    covariance_evidence = dict(core.covariance) if core is not None else raw_covariance_evidence
    stage_evidence_status = (
        (training_stage_evidence.get("stage_specific_cause_evidence") or {}).get("evidence_status")
        if isinstance(training_stage_evidence, Mapping)
        else None
    )
    stage_evidence_complete = bool(
        isinstance(training_stage_evidence, Mapping)
        and stage_evidence_status
        not in {
            "missing_from_training_stage_error",
            "missing_from_successful_core_fit_evidence",
            "invalid_training_stage_evidence",
        }
    )
    if core is not None:
        diagnostic_evidence_complete = bool(
            stage_evidence_complete
            and isinstance(raw_covariance_evidence, Mapping)
            and raw_covariance_evidence.get("diagnostic_evidence_complete") is True
        )
    elif failure_stage in {"initialization", "fit"}:
        diagnostic_evidence_complete = stage_evidence_complete
    elif failure_stage == "likelihood":
        diagnostic_evidence_complete = bool(
            stage_evidence_complete
            and isinstance(initialization_evidence, Mapping)
            and isinstance(raw_covariance_evidence, Mapping)
            and raw_covariance_evidence.get("diagnostic_evidence_complete") is True
        )
    elif failure_stage in {"covariance", "train_posterior"}:
        diagnostic_evidence_complete = bool(
            stage_evidence_complete
            and isinstance(initialization_evidence, Mapping)
            and isinstance(monitor_evidence, Mapping)
            and isinstance(likelihood_evidence, Mapping)
            and isinstance(raw_covariance_evidence, Mapping)
            and raw_covariance_evidence.get("diagnostic_evidence_complete") is True
        )
    else:
        diagnostic_evidence_complete = False
    if not diagnostic_evidence_complete:
        failure_reason_codes.append("hmm_risk_model_training_stage_evidence_incomplete")
    if role == REFIT02_TREATMENT_ROLE:
        role_outcome = (
            "treatment_fit_completed" if core is not None and not failure_reason_codes else "treatment_failed"
        )
    elif role == REFIT02_MATCHED_NEGATIVE_ROLE:
        role_outcome = (
            "matched_control_fit_completed"
            if core is not None and not failure_reason_codes
            else "matched_control_failed"
        )
    else:
        role_outcome = "harness_fit_completed" if core is not None and not failure_reason_codes else "harness_failed"
        if role_outcome == "harness_failed":
            failure_reason_codes.append("hmm_risk_model_inactive_dimension_harness_control_failed")
    parameter_payload = None
    if core is not None:
        parameter_payload = {
            "startprob": _float64_array_identity(core.startprob),
            "transmat": _float64_array_identity(core.transmat),
            "means": _float64_array_identity(core.means),
            "covars": _float64_array_identity(core.covars),
        }
    body = {
        "schema_version": REFIT02_ATTEMPT_SCHEMA_VERSION,
        "algorithm_version": REFIT02_ALGORITHM_VERSION,
        "process_identity": process_identity,
        "role": role,
        "family": "autocycle_all_core",
        "level": "L2",
        "sector_code": item.sector_code,
        "seed": seed,
        "current_authority_receipt_sha256": authority["receipt_sha256"],
        "current_role_input_sha256": canonical_sha256(role_input),
        "status": "fit_completed" if core is not None and not failure_reason_codes else "fit_failed",
        "fit_status": "accepted" if core is not None and not failure_reason_codes else "failed",
        "fit_performed": hmm_fit_invoked,
        "fit_budget_contract_version": REFIT02_FIT_BUDGET_CONTRACT_VERSION,
        "role_outcome": role_outcome,
        "negative_control_blocker_reproduced": None,
        "failure_stage": failure_stage,
        "failure_reason_codes": list(dict.fromkeys(failure_reason_codes)),
        "failure_message": failure_message,
        "projection_receipt": projection,
        "projection_sha256": projection["projection_sha256"],
        "likelihood_feature_count": int(projected.shape[1]),
        "parameter_payload": parameter_payload,
        "numeric_environment": dict(numeric_environment),
        "numeric_environment_sha256": canonical_sha256(dict(numeric_environment)),
        "training_stage_evidence": training_stage_evidence,
        "diagnostic_evidence_complete": diagnostic_evidence_complete,
        "raw_covariance_evidence": raw_covariance_evidence,
        "initialization_evidence": initialization_evidence,
        "monitor_evidence": monitor_evidence,
        "likelihood": likelihood_evidence,
        "covariance": covariance_evidence,
        "train_occupancy": dict(core.train_occupancy) if core else None,
        "final_train_log_likelihood": (
            core.terminal_likelihood
            if core is not None
            else ((monitor_evidence.get("history") or [None])[-1] if isinstance(monitor_evidence, Mapping) else None)
        ),
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "selection_performed": False,
        "formal_model_set_acceptance_performed": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "attempt_receipt_sha256": canonical_sha256(body)}


def _validate_refit02_current_initialization_evidence(value: Mapping[str, Any]) -> None:
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != "hmm_risk_b3_manual_initialization_v1"
        or value.get("contract_version") != D3_CONTRACT_VERSION
        or value.get("diagnostic_source_contract") != "hmm_risk_c008_b3_diag04_manual_initialization_v1"
        or value.get("formal_initialization_contract_applied") is not True
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 current attempt initialization evidence is not authoritative",
        )


def _validate_refit02_attempt_receipt(
    attempt: Mapping[str, Any],
    *,
    process_identity: str,
    current_authority: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = dict(attempt)
    schema_version = normalized.get("schema_version")
    current_schema = schema_version == REFIT02_ATTEMPT_SCHEMA_VERSION
    matched_fit_schema = schema_version == REFIT02_ATTEMPT_SCHEMA_VERSION_MATCHED_FIT
    previous_schema = schema_version == REFIT02_ATTEMPT_SCHEMA_VERSION_LEGACY
    original_schema = schema_version == REFIT02_ATTEMPT_SCHEMA_VERSION_ORIGINAL
    role = normalized.get("role")
    seed = normalized.get("seed")
    role_inputs = current_authority["experiment_authority"]["role_inputs"]
    expected_sector = CONTROL_SECTOR if role == REFIT02_HARNESS_ROLE else TREATMENT_SECTOR
    expected_feature_count = 19 if role == REFIT02_TREATMENT_ROLE else 20
    body = {key: value for key, value in normalized.items() if key != "attempt_receipt_sha256"}
    projection = normalized.get("projection_receipt")
    false_flags = (
        "validation_accessed",
        "future_utility_accessed",
        "semantic_labelability_accessed",
        "d6_status_accessed",
        "selection_performed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    )
    current_false_flags = false_flags + (
        "formal_model_set_acceptance_performed",
        "hard_semantic_authority_changed",
    )
    if (
        not (current_schema or matched_fit_schema or previous_schema or original_schema)
        or normalized.get("algorithm_version") != REFIT02_ALGORITHM_VERSION
        or normalized.get("process_identity") != process_identity
        or role not in REFIT02_ROLES
        or seed not in RESTART_SCHEDULE
        or normalized.get("family") != "autocycle_all_core"
        or normalized.get("level") != "L2"
        or normalized.get("sector_code") != expected_sector
        or normalized.get("current_authority_receipt_sha256") != current_authority.get("receipt_sha256")
        or normalized.get("current_role_input_sha256") != canonical_sha256(role_inputs[role])
        or normalized.get("likelihood_feature_count") != expected_feature_count
        or normalized.get("status") not in {"fit_completed", "fit_failed"}
        or normalized.get("fit_status") not in {"accepted", "failed"}
        or (
            (current_schema or matched_fit_schema)
            and normalized.get("fit_budget_contract_version") != REFIT02_FIT_BUDGET_CONTRACT_VERSION
        )
        or (
            previous_schema
            and normalized.get("fit_budget_contract_version") != REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
        )
        or (original_schema and normalized.get("fit_budget_contract_version") is not None)
        or not isinstance(normalized.get("failure_reason_codes"), list)
        or not isinstance(normalized.get("numeric_environment"), Mapping)
        or canonical_sha256(dict(normalized["numeric_environment"])) != normalized.get("numeric_environment_sha256")
        or not isinstance(projection, Mapping)
        or projection.get("schema_version") != "hmm_risk_c008_b3_d1_projection_receipt_v3"
        or projection.get("algorithm_version") != REFIT02_ALGORITHM_VERSION
        or projection.get("role") != role
        or projection.get("likelihood_feature_count") != expected_feature_count
        or projection.get("current_a5_experiment_authority_sha256")
        != current_authority.get("current_a5_experiment_authority_sha256")
        or canonical_sha256({key: value for key, value in projection.items() if key != "projection_sha256"})
        != projection.get("projection_sha256")
        or normalized.get("projection_sha256") != projection.get("projection_sha256")
        or canonical_sha256(body) != normalized.get("attempt_receipt_sha256")
        or any(normalized.get(field) is not False for field in (current_false_flags if current_schema else false_flags))
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 attempt receipt is invalid",
        )
    refit03_only_fields = {
        "training_stage_evidence",
        "diagnostic_evidence_complete",
        "raw_covariance_evidence",
        "formal_model_set_acceptance_performed",
        "hard_semantic_authority_changed",
    }
    if not current_schema and any(field in normalized for field in refit03_only_fields):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 historical attempt schema contains REFIT-03-only evidence",
        )
    completed = normalized.get("status") == "fit_completed"
    accepted = normalized.get("fit_status") == "accepted"
    if completed != accepted:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 attempt completion and acceptance states disagree",
        )
    if previous_schema and role == REFIT02_MATCHED_NEGATIVE_ROLE and normalized.get("fit_performed") is True:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_fit_budget_exceeded",
            "REFIT-02 legacy matched negative control must not invoke HMM fit",
        )
    if completed:
        parameter_payload = normalized.get("parameter_payload")
        if (
            normalized.get("fit_performed") is not True
            or not isinstance(parameter_payload, Mapping)
            or any(
                not isinstance(normalized.get(field), Mapping)
                for field in (
                    "initialization_evidence",
                    "monitor_evidence",
                    "likelihood",
                    "covariance",
                    "train_occupancy",
                )
            )
            or not isinstance(normalized.get("final_train_log_likelihood"), (int, float))
            or not math.isfinite(float(normalized["final_train_log_likelihood"]))
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "REFIT-02 completed attempt lacks full train-only evidence",
            )
        if current_schema or matched_fit_schema:
            _validate_refit02_current_initialization_evidence(normalized["initialization_evidence"])
        expected_shapes = {
            "startprob": [3],
            "transmat": [3, 3],
            "means": [3, expected_feature_count],
            "covars": [3, expected_feature_count],
        }
        if any(
            not isinstance(parameter_payload.get(field), Mapping) or parameter_payload[field].get("shape") != shape
            for field, shape in expected_shapes.items()
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_parameter_shape_invalid",
                "REFIT-02 completed attempt parameter shapes are invalid",
            )
    expected_active = list(range(19)) if role == REFIT02_TREATMENT_ROLE else list(range(20))
    expected_inactive = [19] if role == REFIT02_TREATMENT_ROLE else []
    if (
        projection.get("active_feature_indices") != expected_active
        or projection.get("inactive_feature_indices") != expected_inactive
        or projection.get("active_feature_mask")
        != [index in expected_active for index in range(len(ALL_CORE_FEATURES))]
        or projection.get("dynamic_activation") is not False
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 projection receipt does not match its fixed role",
        )
    if role == REFIT02_MATCHED_NEGATIVE_ROLE:
        current_matched_contract = (
            (current_schema or matched_fit_schema)
            and normalized.get("negative_control_blocker_reproduced") is None
            and normalized.get("fit_budget_contract_version") == REFIT02_FIT_BUDGET_CONTRACT_VERSION
            and normalized.get("role_outcome")
            == ("matched_control_fit_completed" if completed else "matched_control_failed")
            and (
                completed
                or (isinstance(normalized.get("failure_stage"), str) and bool(normalized.get("failure_reason_codes")))
            )
        )
        reproduced = normalized.get("negative_control_blocker_reproduced") is True
        reproduced_contract = (
            not (current_schema or matched_fit_schema)
            and reproduced
            and normalized.get("fit_performed") is False
            and normalized.get("status") == "fit_failed"
            and normalized.get("fit_status") == "failed"
            and normalized.get("role_outcome") == "negative_control_blocker_reproduced"
            and normalized.get("failure_stage") == "initialization"
            and "hmm_risk_model_initialization_failed" in normalized["failure_reason_codes"]
        )
        current_not_reproduced_contract = (
            previous_schema
            and normalized.get("negative_control_blocker_reproduced") is False
            and normalized.get("fit_budget_contract_version") == REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
            and normalized.get("fit_performed") is False
            and normalized.get("status") == "fit_failed"
            and normalized.get("fit_status") == "failed"
            and normalized.get("role_outcome") == "negative_control_not_reproduced"
            and normalized.get("failure_stage") == "initialization"
            and isinstance(normalized.get("initialization_evidence"), Mapping)
            and "hmm_risk_model_inactive_dimension_negative_control_not_reproduced"
            in normalized["failure_reason_codes"]
        )
        legacy_not_reproduced_contract = (
            original_schema
            and normalized.get("negative_control_blocker_reproduced") is False
            and normalized.get("fit_budget_contract_version") is None
            and normalized.get("status") == "fit_failed"
            and normalized.get("fit_status") == "failed"
            and normalized.get("role_outcome") == "negative_control_not_reproduced"
            and "hmm_risk_model_inactive_dimension_negative_control_not_reproduced"
            in normalized["failure_reason_codes"]
        )
        if previous_schema and current_not_reproduced_contract:
            _validate_refit02_current_initialization_evidence(normalized["initialization_evidence"])
        if not any(
            (
                current_matched_contract,
                reproduced_contract,
                current_not_reproduced_contract,
                legacy_not_reproduced_contract,
            )
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "REFIT-02 matched negative-control evidence is internally inconsistent",
            )
    elif normalized.get("negative_control_blocker_reproduced") is not None:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 non-negative role carries negative-control evidence",
        )
    expected_outcome = (
        ("treatment_fit_completed" if completed else "treatment_failed")
        if role == REFIT02_TREATMENT_ROLE
        else ("harness_fit_completed" if completed else "harness_failed")
    )
    if role != REFIT02_MATCHED_NEGATIVE_ROLE and normalized.get("role_outcome") != expected_outcome:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 role outcome disagrees with its fit status",
        )
    if current_schema:
        stage_evidence = normalized.get("training_stage_evidence")
        raw_evidence = normalized.get("raw_covariance_evidence")
        stage_body = (
            {key: item for key, item in stage_evidence.items() if key != "stage_evidence_sha256"}
            if isinstance(stage_evidence, Mapping)
            else None
        )
        raw_body = (
            {key: item for key, item in raw_evidence.items() if key != "covariance_evidence_sha256"}
            if isinstance(raw_evidence, Mapping)
            else None
        )
        if (
            not isinstance(stage_evidence, Mapping)
            or stage_evidence.get("schema_version") != REFIT03_STAGE_EVIDENCE_SCHEMA_VERSION
            or canonical_sha256(stage_body) != stage_evidence.get("stage_evidence_sha256")
            or not isinstance(normalized.get("diagnostic_evidence_complete"), bool)
            or (
                isinstance(raw_evidence, Mapping)
                and raw_evidence.get("schema_version") != REFIT03_COVARIANCE_EVIDENCE_SCHEMA_VERSION
            )
            or (
                isinstance(raw_evidence, Mapping)
                and canonical_sha256(raw_body) != raw_evidence.get("covariance_evidence_sha256")
            )
            or (
                isinstance(raw_evidence, Mapping)
                and raw_evidence.get("cross_role_state_alignment_performed") is not False
            )
            or (isinstance(raw_evidence, Mapping) and raw_evidence.get("semantic_label_accessed") is not False)
            or (
                normalized.get("diagnostic_evidence_complete") is True
                and normalized.get("failure_stage") in {"covariance", "train_posterior"}
                and not isinstance(raw_evidence, Mapping)
            )
            or (
                normalized.get("diagnostic_evidence_complete") is True
                and stage_evidence.get("fit_returned") is True
                and not isinstance(raw_evidence, Mapping)
            )
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_training_stage_evidence_incomplete",
                "REFIT-03 attempt lacks complete covariance-stage evidence",
            )
        if normalized["diagnostic_evidence_complete"] is False and (
            normalized.get("status") != "fit_failed"
            or "hmm_risk_model_training_stage_evidence_incomplete" not in normalized.get("failure_reason_codes", [])
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_training_stage_evidence_incomplete",
                "REFIT-03 incomplete evidence is not represented as a terminal failure",
            )
    return normalized


def _build_refit03_pair_receipt(
    *,
    seed: int,
    attempts_by_key: Mapping[tuple[str, int], Mapping[str, Any]],
) -> dict[str, Any]:
    role_attempts = {role: dict(attempts_by_key[(role, seed)]) for role in REFIT02_ROLES}
    evidence_by_role = {role: attempt.get("raw_covariance_evidence") for role, attempt in role_attempts.items()}
    complete = all(
        attempt.get("diagnostic_evidence_complete") is True
        and (isinstance(evidence_by_role[role], Mapping) or attempt.get("failure_stage") in {"initialization", "fit"})
        for role, attempt in role_attempts.items()
    )
    invalid_by_role = {
        role: (list(evidence.get("invalid_feature_indices") or ()) if isinstance(evidence, Mapping) else [])
        for role, evidence in evidence_by_role.items()
    }
    labels: list[str] = []
    if not complete:
        labels.append("evidence_incomplete")
    else:
        treatment_invalid = set(invalid_by_role[REFIT02_TREATMENT_ROLE])
        matched_invalid = set(invalid_by_role[REFIT02_MATCHED_NEGATIVE_ROLE])
        harness_invalid = set(invalid_by_role[REFIT02_HARNESS_ROLE])
        covariance_stage_failures = {
            role for role, attempt in role_attempts.items() if attempt.get("failure_stage") == "covariance"
        }
        inactive_only_failure = (
            covariance_stage_failures == {REFIT02_MATCHED_NEGATIVE_ROLE}
            and matched_invalid == {INACTIVE_FEATURE_INDEX}
            and not treatment_invalid
            and not harness_invalid
        )
        if treatment_invalid or harness_invalid or (covariance_stage_failures and not inactive_only_failure):
            labels.append("cross_role_failure_present")
        if matched_invalid - {INACTIVE_FEATURE_INDEX}:
            labels.append("active_coordinate_failure_present")
        if inactive_only_failure:
            labels.append("inactive_coordinate_pattern_consistent")
    body = {
        "schema_version": "hmm_risk_c008_b3_d1_covariance_pair_receipt_v1",
        "seed": seed,
        "comparison_domain": "feature_level_invalid_coordinate_set_only",
        "cross_role_state_alignment_performed": False,
        "semantic_label_accessed": False,
        "role_attempt_receipt_sha256": {
            role: attempt.get("attempt_receipt_sha256") for role, attempt in role_attempts.items()
        },
        "role_projection_sha256": {role: attempt.get("projection_sha256") for role, attempt in role_attempts.items()},
        "role_raw_input_identity": {
            role: (attempt.get("projection_receipt") or {}).get("raw_full20_identity")
            for role, attempt in role_attempts.items()
        },
        "role_preprocessed_input_identity": {
            role: (attempt.get("projection_receipt") or {}).get("preprocessed_full20_identity")
            for role, attempt in role_attempts.items()
        },
        "role_invalid_feature_indices": invalid_by_role,
        "diagnostic_labels": sorted(labels),
        "diagnostic_evidence_complete": complete,
    }
    return {**body, "pair_receipt_sha256": canonical_sha256(body)}


def _validate_refit03_pair_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(value)
    body = {key: item for key, item in normalized.items() if key != "pair_receipt_sha256"}
    allowed_labels = {
        "inactive_coordinate_pattern_consistent",
        "active_coordinate_failure_present",
        "cross_role_failure_present",
        "mixed_seed_pattern",
        "evidence_incomplete",
    }
    if (
        normalized.get("schema_version") != "hmm_risk_c008_b3_d1_covariance_pair_receipt_v1"
        or normalized.get("seed") not in RESTART_SCHEDULE
        or normalized.get("comparison_domain") != "feature_level_invalid_coordinate_set_only"
        or normalized.get("cross_role_state_alignment_performed") is not False
        or normalized.get("semantic_label_accessed") is not False
        or not isinstance(normalized.get("diagnostic_labels"), list)
        or not set(normalized["diagnostic_labels"]).issubset(allowed_labels)
        or canonical_sha256(body) != normalized.get("pair_receipt_sha256")
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_covariance_evidence_incomplete",
            "REFIT-03 covariance pair receipt is invalid",
        )
    return normalized


def build_refit02_process_receipt(
    *,
    process_identity: str,
    producer_commit: str,
    attempts: Sequence[Mapping[str, Any]],
    current_authority: Mapping[str, Any],
    historical_reference: Mapping[str, Any],
) -> dict[str, Any]:
    if process_identity not in {"fresh_process_1", "fresh_process_2"}:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 process identity is invalid",
        )
    authority = _validate_refit02_current_authority_envelope(current_authority)
    historical = validate_refit02_historical_reference_receipt(
        historical_reference,
        current_authority=authority,
    )
    ordered = sorted(
        (dict(value) for value in attempts),
        key=lambda value: (str(value.get("role")), int(value.get("seed", -1))),
    )
    expected = {(role, seed) for role in REFIT02_ROLES for seed in RESTART_SCHEDULE}
    actual = {(str(value.get("role")), int(value.get("seed", -1))) for value in ordered}
    if len(ordered) != 24 or actual != expected:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_attempt_set_incomplete",
            "REFIT-02 process must contain exactly 24 terminal attempts",
        )
    ordered = [
        _validate_refit02_attempt_receipt(
            attempt,
            process_identity=process_identity,
            current_authority=authority,
        )
        for attempt in ordered
    ]
    attempt_schema_versions = {str(value.get("schema_version")) for value in ordered}
    if attempt_schema_versions == {REFIT02_ATTEMPT_SCHEMA_VERSION}:
        process_schema_version = REFIT02_PROCESS_SCHEMA_VERSION
        fit_budget_contract_version: str | None = REFIT02_FIT_BUDGET_CONTRACT_VERSION
    elif attempt_schema_versions == {REFIT02_ATTEMPT_SCHEMA_VERSION_MATCHED_FIT}:
        process_schema_version = REFIT02_PROCESS_SCHEMA_VERSION_MATCHED_FIT
        fit_budget_contract_version = REFIT02_FIT_BUDGET_CONTRACT_VERSION
    elif attempt_schema_versions == {REFIT02_ATTEMPT_SCHEMA_VERSION_LEGACY}:
        process_schema_version = REFIT02_PROCESS_SCHEMA_VERSION_LEGACY
        fit_budget_contract_version = REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
    elif attempt_schema_versions == {REFIT02_ATTEMPT_SCHEMA_VERSION_ORIGINAL}:
        process_schema_version = REFIT02_PROCESS_SCHEMA_VERSION_ORIGINAL
        fit_budget_contract_version = None
    else:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 process cannot mix current and legacy attempt schemas",
        )
    environment_hashes = {str(value.get("numeric_environment_sha256")) for value in ordered}
    if len(environment_hashes) != 1:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "REFIT-02 attempts do not share one numeric environment",
        )
    treatment_input = ((authority.get("experiment_authority") or {}).get("role_inputs") or {}).get(
        REFIT02_TREATMENT_ROLE
    )
    negative_input = ((authority.get("experiment_authority") or {}).get("role_inputs") or {}).get(
        REFIT02_MATCHED_NEGATIVE_ROLE
    )
    if treatment_input != negative_input:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_matched_input_mismatch",
            "REFIT-02 same-sector treatment and negative control inputs differ",
        )
    attempts_by_key = {(str(value["role"]), int(value["seed"])): value for value in ordered}
    for seed in RESTART_SCHEDULE:
        treatment_attempt = attempts_by_key[(REFIT02_TREATMENT_ROLE, seed)]
        negative_attempt = attempts_by_key[(REFIT02_MATCHED_NEGATIVE_ROLE, seed)]
        for field in ("raw_full20_identity", "preprocessed_full20_identity"):
            if treatment_attempt["projection_receipt"].get(field) != negative_attempt["projection_receipt"].get(field):
                raise D1InactiveDimensionError(
                    "hmm_risk_model_inactive_dimension_matched_input_mismatch",
                    "REFIT-02 treatment and negative control do not share identical full20 input",
                )
    pair_receipts = (
        [_build_refit03_pair_receipt(seed=seed, attempts_by_key=attempts_by_key) for seed in RESTART_SCHEDULE]
        if process_schema_version == REFIT02_PROCESS_SCHEMA_VERSION
        else []
    )
    comparable = [
        {key: value for key, value in attempt.items() if key not in {"process_identity", "attempt_receipt_sha256"}}
        for attempt in ordered
    ]
    comparable_pairs = [
        {key: value for key, value in pair.items() if key not in {"pair_receipt_sha256", "role_attempt_receipt_sha256"}}
        for pair in pair_receipts
    ]
    actual_hmm_fit_invocation_count = sum(value.get("fit_performed") is True for value in ordered)
    if process_schema_version in {REFIT02_PROCESS_SCHEMA_VERSION, REFIT02_PROCESS_SCHEMA_VERSION_MATCHED_FIT}:
        if actual_hmm_fit_invocation_count > 24:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_fit_budget_exceeded",
                "REFIT-02 current process exceeded its HMM fit budget",
            )
        planned_hmm_fit_count = 24
    else:
        negative_fit_attempts = [
            value
            for value in ordered
            if value.get("role") == REFIT02_MATCHED_NEGATIVE_ROLE and value.get("fit_performed") is True
        ]
        if process_schema_version == REFIT02_PROCESS_SCHEMA_VERSION_LEGACY and (
            negative_fit_attempts or actual_hmm_fit_invocation_count > 16
        ):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_fit_budget_exceeded",
                "REFIT-02 legacy process exceeded its HMM fit budget",
            )
        planned_hmm_fit_count = 16
    body = {
        "schema_version": process_schema_version,
        "algorithm_version": REFIT02_ALGORITHM_VERSION,
        "process_identity": process_identity,
        "producer_commit": _require_commit(producer_commit, "producer_commit"),
        "source_authority": dict(SOURCE_AUTHORITY),
        "current_authority": authority,
        "historical_reference": historical,
        "attempts": ordered,
        "attempt_count": 24,
        "terminal_attempt_count": 24,
        "planned_hmm_fit_count": planned_hmm_fit_count,
        "actual_hmm_fit_invocation_count": actual_hmm_fit_invocation_count,
        "numeric_environment_sha256": canonical_sha256(dict(ordered[0].get("numeric_environment") or {})),
        "comparable_payload_sha256": canonical_sha256(
            {"attempts": comparable, "pair_receipts": comparable_pairs}
            if process_schema_version == REFIT02_PROCESS_SCHEMA_VERSION
            else comparable
        ),
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    if process_schema_version == REFIT02_PROCESS_SCHEMA_VERSION:
        body.update(
            {
                "diagnostic_contract": REFIT02_DIAGNOSTIC_CONTRACT,
                "pair_receipts": pair_receipts,
                "diagnostic_evidence_complete": all(
                    value.get("diagnostic_evidence_complete") is True for value in ordered
                )
                and all(value.get("diagnostic_evidence_complete") is True for value in pair_receipts),
                "formal_model_set_acceptance_performed": False,
                "hard_semantic_authority_changed": False,
                "validation_accessed": False,
                "future_utility_accessed": False,
                "semantic_labelability_accessed": False,
                "d6_status_accessed": False,
            }
        )
    if fit_budget_contract_version is not None:
        body["fit_budget_contract_version"] = fit_budget_contract_version
    return {**body, "process_receipt_sha256": canonical_sha256(body)}


def validate_refit02_process_receipt(
    value: Mapping[str, Any],
    *,
    expected_process_identity: str,
    expected_producer_commit: str,
) -> dict[str, Any]:
    normalized = dict(value)
    schema_version = normalized.get("schema_version")
    if (
        schema_version
        not in {
            REFIT02_PROCESS_SCHEMA_VERSION,
            REFIT02_PROCESS_SCHEMA_VERSION_MATCHED_FIT,
            REFIT02_PROCESS_SCHEMA_VERSION_LEGACY,
            REFIT02_PROCESS_SCHEMA_VERSION_ORIGINAL,
        }
        or normalized.get("algorithm_version") != REFIT02_ALGORITHM_VERSION
        or normalized.get("process_identity") != expected_process_identity
        or normalized.get("producer_commit") != _require_commit(expected_producer_commit, "expected_producer_commit")
        or normalized.get("source_authority") != SOURCE_AUTHORITY
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 process authority is invalid",
        )
    rebuilt = build_refit02_process_receipt(
        process_identity=expected_process_identity,
        producer_commit=expected_producer_commit,
        attempts=list(normalized.get("attempts") or ()),
        current_authority=dict(normalized.get("current_authority") or {}),
        historical_reference=dict(normalized.get("historical_reference") or {}),
    )
    if rebuilt != normalized:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 process differs from its writer authority",
        )
    return normalized


def run_refit02_process(
    *,
    treatment_item: B3TrainOnlySeries,
    harness_item: B3TrainOnlySeries,
    preprocess: Mapping[str, Any],
    process_identity: str,
    producer_commit: str,
    numeric_environment: Mapping[str, Any],
    current_authority: Mapping[str, Any],
    historical_reference: Mapping[str, Any],
) -> dict[str, Any]:
    authority = validate_refit02_current_a5_authority(
        current_authority,
        treatment_item=treatment_item,
        harness_item=harness_item,
        preprocess=preprocess,
    )
    if authority.get("current_profile_eligible") is not True:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_current_profile_not_applicable",
            "REFIT-02 current treatment profile is no longer exact-zero eligible",
        )
    attempts: list[dict[str, Any]] = []
    for seed in RESTART_SCHEDULE:
        for role, item in (
            (REFIT02_TREATMENT_ROLE, treatment_item),
            (REFIT02_MATCHED_NEGATIVE_ROLE, treatment_item),
            (REFIT02_HARNESS_ROLE, harness_item),
        ):
            attempts.append(
                fit_refit02_attempt(
                    item,
                    preprocess=preprocess,
                    role=role,
                    seed=seed,
                    process_identity=process_identity,
                    numeric_environment=numeric_environment,
                    current_authority=authority,
                )
            )
    receipt = build_refit02_process_receipt(
        process_identity=process_identity,
        producer_commit=producer_commit,
        attempts=attempts,
        current_authority=authority,
        historical_reference=historical_reference,
    )
    if receipt["actual_hmm_fit_invocation_count"] > receipt["planned_hmm_fit_count"]:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_fit_budget_exceeded",
            "REFIT-02 actual HMM fit count exceeds the approved process budget",
        )
    return receipt


def build_refit02_report(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
    *,
    producer_commit: str,
) -> dict[str, Any]:
    processes = [dict(first), dict(second)]
    process_schema_versions = {str(value.get("schema_version")) for value in processes}
    if process_schema_versions == {REFIT02_PROCESS_SCHEMA_VERSION}:
        report_schema_version = REFIT02_REPORT_SCHEMA_VERSION
        fit_budget_contract_version: str | None = REFIT02_FIT_BUDGET_CONTRACT_VERSION
        planned_hmm_fit_count = 48
        current_matched_fit_contract = True
    elif process_schema_versions == {REFIT02_PROCESS_SCHEMA_VERSION_MATCHED_FIT}:
        report_schema_version = REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT
        fit_budget_contract_version = REFIT02_FIT_BUDGET_CONTRACT_VERSION
        planned_hmm_fit_count = 48
        current_matched_fit_contract = True
    elif process_schema_versions == {REFIT02_PROCESS_SCHEMA_VERSION_LEGACY}:
        report_schema_version = REFIT02_REPORT_SCHEMA_VERSION_LEGACY
        fit_budget_contract_version = REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
        planned_hmm_fit_count = 32
        current_matched_fit_contract = False
    elif process_schema_versions == {REFIT02_PROCESS_SCHEMA_VERSION_ORIGINAL}:
        report_schema_version = REFIT02_REPORT_SCHEMA_VERSION_ORIGINAL
        fit_budget_contract_version = None
        planned_hmm_fit_count = 32
        current_matched_fit_contract = False
    else:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 report cannot mix current and legacy process schemas",
        )
    reasons: set[str] = set()
    for process, identity in zip(processes, ("fresh_process_1", "fresh_process_2"), strict=True):
        try:
            validate_refit02_process_receipt(
                process,
                expected_process_identity=identity,
                expected_producer_commit=producer_commit,
            )
        except D1InactiveDimensionError as exc:
            reasons.add(exc.reason_code)
    repeat_equal = first.get("comparable_payload_sha256") == second.get("comparable_payload_sha256")
    if not repeat_equal:
        reasons.add("hmm_risk_model_inactive_dimension_repeat_mismatch")
    attempts = list(first.get("attempts") or ())
    by_role = {role: [value for value in attempts if value.get("role") == role] for role in REFIT02_ROLES}
    if any(len(values) != 8 for values in by_role.values()):
        reasons.add("hmm_risk_model_inactive_dimension_attempt_set_incomplete")
    diagnostic_reasons = set(reasons)
    matched = by_role[REFIT02_MATCHED_NEGATIVE_ROLE]
    negative_ok = all(
        value.get("negative_control_blocker_reproduced") is True
        and value.get("status") == "fit_failed"
        and value.get("fit_performed") is False
        for value in matched
    )
    matched_fit_ok = all(
        value.get("status") == "fit_completed"
        and value.get("fit_performed") is True
        and value.get("role_outcome") == "matched_control_fit_completed"
        for value in matched
    )
    matched_blocker_ok = all(
        value.get("status") == "fit_failed"
        and value.get("fit_performed") is False
        and value.get("role_outcome") == "matched_control_failed"
        and value.get("failure_stage") == "initialization"
        and "hmm_risk_model_initialization_failed" in (value.get("failure_reason_codes") or ())
        for value in matched
    )
    harness_ok = all(
        value.get("status") == "fit_completed" and value.get("role_outcome") == "harness_fit_completed"
        for value in by_role[REFIT02_HARNESS_ROLE]
    )
    if not current_matched_fit_contract and not negative_ok:
        reasons.add("hmm_risk_model_inactive_dimension_negative_control_not_reproduced")
    if not harness_ok:
        reasons.add("hmm_risk_model_inactive_dimension_harness_control_failed")
    treatment = by_role[REFIT02_TREATMENT_ROLE]
    treatment_reasons = {str(reason) for attempt in treatment for reason in (attempt.get("failure_reason_codes") or ())}
    all_attempt_reasons = {
        str(reason)
        for role_attempts in by_role.values()
        for attempt in role_attempts
        for reason in (attempt.get("failure_reason_codes") or ())
    }
    covariance_pattern_assessment: str | None = None
    pattern_reason_codes: set[str] = set()
    if report_schema_version == REFIT02_REPORT_SCHEMA_VERSION:
        pair_receipts = list(first.get("pair_receipts") or ())
        if len(pair_receipts) != 8:
            diagnostic_reasons.add("hmm_risk_model_covariance_evidence_incomplete")
        validated_pairs: list[dict[str, Any]] = []
        for pair in pair_receipts:
            try:
                validated_pairs.append(_validate_refit03_pair_receipt(pair))
            except D1InactiveDimensionError as exc:
                diagnostic_reasons.add(exc.reason_code)
        if (
            first.get("diagnostic_evidence_complete") is not True
            or second.get("diagnostic_evidence_complete") is not True
            or any(value.get("diagnostic_evidence_complete") is not True for value in validated_pairs)
        ):
            diagnostic_reasons.add("hmm_risk_model_covariance_evidence_incomplete")
        label_sets = {tuple(value.get("diagnostic_labels") or ()) for value in validated_pairs}
        labels = {label for values in label_sets for label in values}
        if "evidence_incomplete" in labels or diagnostic_reasons:
            covariance_pattern_assessment = "evidence_incomplete"
        elif len(label_sets) > 1:
            covariance_pattern_assessment = "mixed_seed_pattern"
            pattern_reason_codes.add("hmm_risk_model_inactive_dimension_covariance_pattern_mixed")
        elif "active_coordinate_failure_present" in labels:
            covariance_pattern_assessment = "active_coordinate_failure_present"
        elif "cross_role_failure_present" in labels:
            covariance_pattern_assessment = "cross_role_failure_present"
        elif labels == {"inactive_coordinate_pattern_consistent"}:
            covariance_pattern_assessment = "inactive_coordinate_pattern_consistent"
        else:
            covariance_pattern_assessment = None
        status_reasons = diagnostic_reasons
    else:
        status_reasons = reasons
    mechanism_reason_codes: set[str] = set()
    if report_schema_version != REFIT02_REPORT_SCHEMA_VERSION and any(
        value.get("fit_performed") is not True for value in treatment
    ):
        reasons.update(treatment_reasons or {"hmm_risk_model_initialization_failed"})
    if report_schema_version == REFIT02_REPORT_SCHEMA_VERSION:
        mechanism = "inconclusive"
    elif current_matched_fit_contract:
        if not matched_fit_ok and not matched_blocker_ok:
            reasons.add("hmm_risk_model_inactive_dimension_matched_control_inconclusive")
        if reasons:
            mechanism = "inconclusive"
        elif matched_fit_ok:
            mechanism = "constant_dimension_mechanism_rejected"
            mechanism_reason_codes.add("hmm_risk_model_inactive_dimension_matched_control_fit_completed")
        elif treatment_reasons & _D1_REJECTION_REASONS:
            mechanism = "constant_dimension_mechanism_rejected"
        elif all(value.get("fit_performed") is True for value in treatment):
            mechanism = "constant_dimension_effect_supported"
        else:
            mechanism = "inconclusive"
    elif reasons:
        mechanism = "inconclusive"
    elif treatment_reasons & _D1_REJECTION_REASONS:
        mechanism = "constant_dimension_mechanism_rejected"
    elif all(value.get("fit_performed") is True for value in treatment):
        mechanism = "constant_dimension_effect_supported"
    else:
        mechanism = "inconclusive"
    d4_ready = all(
        value.get("status") == "fit_completed"
        and isinstance(value.get("final_train_log_likelihood"), (int, float))
        and math.isfinite(float(value["final_train_log_likelihood"]))
        and all(isinstance(value.get(field), Mapping) for field in ("likelihood", "covariance", "train_occupancy"))
        for value in treatment
    )
    body = {
        "schema_version": report_schema_version,
        "diagnostic_contract": _refit_diagnostic_contract_for_report(report_schema_version),
        "producer_commit": _require_commit(producer_commit, "producer_commit"),
        "source_authority": dict(SOURCE_AUTHORITY),
        "status": "diagnostic_complete" if not status_reasons else "diagnostic_failed",
        "mechanism_assessment": mechanism,
        "mechanism_assessment_reason_codes": sorted(
            status_reasons | all_attempt_reasons | mechanism_reason_codes | pattern_reason_codes
        ),
        "d5_compatibility_evidence_ready": bool(
            report_schema_version != REFIT02_REPORT_SCHEMA_VERSION
            and not reasons
            and repeat_equal
            and d4_ready
            and mechanism == "constant_dimension_effect_supported"
        ),
        "process_receipts": processes,
        "canonical_payload_bitwise_equal": repeat_equal,
        "attempt_count": sum(int(value.get("attempt_count") or 0) for value in processes),
        "planned_hmm_fit_count": planned_hmm_fit_count,
        "actual_hmm_fit_invocation_count": sum(
            int(value.get("actual_hmm_fit_invocation_count") or 0) for value in processes
        ),
        "selection_performed": False,
        "d3_d4_descriptive_contracts_applied": True,
        "formal_model_set_acceptance_performed": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    if report_schema_version == REFIT02_REPORT_SCHEMA_VERSION:
        body.update(
            {
                "covariance_pattern_assessment": covariance_pattern_assessment,
                "diagnostic_evidence_complete": not status_reasons,
                "validation_accessed": False,
                "future_utility_accessed": False,
                "semantic_labelability_accessed": False,
                "d6_status_accessed": False,
            }
        )
    if fit_budget_contract_version is not None:
        body["fit_budget_contract_version"] = fit_budget_contract_version
    return {**body, "receipt_sha256": canonical_sha256(body)}


def build_refit02_not_applicable_report(
    current_authority: Mapping[str, Any],
    historical_reference: Mapping[str, Any],
    *,
    producer_commit: str,
    schema_version: str = REFIT02_REPORT_SCHEMA_VERSION,
) -> dict[str, Any]:
    authority = _validate_refit02_current_authority_envelope(current_authority)
    historical = validate_refit02_historical_reference_receipt(
        historical_reference,
        current_authority=authority,
    )
    if authority.get("current_profile_eligible") is not False:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 not-applicable report requires an ineligible current profile",
        )
    if schema_version not in {
        REFIT02_REPORT_SCHEMA_VERSION,
        REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT,
        REFIT02_REPORT_SCHEMA_VERSION_LEGACY,
        REFIT02_REPORT_SCHEMA_VERSION_ORIGINAL,
    }:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 not-applicable report schema is invalid",
        )
    body = {
        "schema_version": schema_version,
        "diagnostic_contract": _refit_diagnostic_contract_for_report(schema_version),
        "producer_commit": _require_commit(producer_commit, "producer_commit"),
        "source_authority": dict(SOURCE_AUTHORITY),
        "status": "not_applicable",
        "mechanism_assessment": "constant_dimension_mechanism_not_applicable_current_profile_changed",
        "mechanism_assessment_reason_codes": ["hmm_risk_model_inactive_dimension_current_profile_not_applicable"],
        "current_authority": authority,
        "historical_reference": historical,
        "d5_compatibility_evidence_ready": False,
        "process_receipts": [],
        "canonical_payload_bitwise_equal": False,
        "attempt_count": 0,
        "planned_hmm_fit_count": 0,
        "actual_hmm_fit_invocation_count": 0,
        "selection_performed": False,
        "d3_d4_descriptive_contracts_applied": False,
        "formal_model_set_acceptance_performed": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    if schema_version == REFIT02_REPORT_SCHEMA_VERSION:
        body.update(
            {
                "covariance_pattern_assessment": None,
                "diagnostic_evidence_complete": False,
                "validation_accessed": False,
                "future_utility_accessed": False,
                "semantic_labelability_accessed": False,
                "d6_status_accessed": False,
            }
        )
    if _refit_report_uses_v2_fit_budget(schema_version):
        body["fit_budget_contract_version"] = REFIT02_FIT_BUDGET_CONTRACT_VERSION
    elif schema_version == REFIT02_REPORT_SCHEMA_VERSION_LEGACY:
        body["fit_budget_contract_version"] = REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
    return {**body, "receipt_sha256": canonical_sha256(body)}


def build_refit02_execution_failure_report(
    *,
    producer_commit: str,
    current_authority: Mapping[str, Any],
    historical_reference: Mapping[str, Any],
    completed_processes: Sequence[Mapping[str, Any]],
    failed_process_receipt: Mapping[str, Any],
    schema_version: str = REFIT02_REPORT_SCHEMA_VERSION,
) -> dict[str, Any]:
    normalized_commit = _require_commit(producer_commit, "producer_commit")
    completed = [dict(value) for value in completed_processes]
    if len(completed) > 2:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 failure report has too many completed processes",
        )
    for index, process in enumerate(completed, start=1):
        validate_refit02_process_receipt(
            process,
            expected_process_identity=f"fresh_process_{index}",
            expected_producer_commit=normalized_commit,
        )
    failed = dict(failed_process_receipt)
    expected_failure_identity = f"fresh_process_{len(completed) + 1}" if len(completed) < 2 else "parent_finalize"
    validate_controlled_process_failure_receipt(
        failed,
        expected_process_identity=expected_failure_identity,
        expected_producer_commit=normalized_commit,
        expected_source_authority=SOURCE_AUTHORITY,
    )
    authority = _validate_refit02_current_authority_envelope(current_authority)
    historical = validate_refit02_historical_reference_receipt(
        historical_reference,
        current_authority=authority,
    )
    attempt_count = sum(int(value.get("attempt_count") or 0) for value in completed)
    if schema_version not in {
        REFIT02_REPORT_SCHEMA_VERSION,
        REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT,
        REFIT02_REPORT_SCHEMA_VERSION_LEGACY,
        REFIT02_REPORT_SCHEMA_VERSION_ORIGINAL,
    }:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 execution-failure report schema is invalid",
        )
    expected_process_schema = _refit_process_schema_for_report(schema_version)
    if any(value.get("schema_version") != expected_process_schema for value in completed):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 failure report process schemas disagree with its report schema",
        )
    body = {
        "schema_version": schema_version,
        "diagnostic_contract": _refit_diagnostic_contract_for_report(schema_version),
        "producer_commit": normalized_commit,
        "source_authority": dict(SOURCE_AUTHORITY),
        "status": "diagnostic_failed",
        "mechanism_assessment": "inconclusive",
        "mechanism_assessment_reason_codes": [str(failed.get("reason_code") or "")],
        "current_authority": authority,
        "historical_reference": historical,
        "d5_compatibility_evidence_ready": False,
        "process_receipts": completed,
        "completed_process_count": len(completed),
        "failed_process_receipt": failed,
        "canonical_payload_bitwise_equal": False,
        "attempt_count": attempt_count,
        "planned_hmm_fit_count": 48 if _refit_report_uses_v2_fit_budget(schema_version) else 32,
        "actual_hmm_fit_invocation_count": sum(
            int(value.get("actual_hmm_fit_invocation_count") or 0) for value in completed
        ),
        "fit_budget_completion_unknown": bool(failed.get("fit_budget_completion_unknown")),
        "selection_performed": False,
        "d3_d4_descriptive_contracts_applied": bool(completed),
        "formal_model_set_acceptance_performed": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    if schema_version == REFIT02_REPORT_SCHEMA_VERSION:
        body.update(
            {
                "covariance_pattern_assessment": "evidence_incomplete",
                "diagnostic_evidence_complete": False,
                "validation_accessed": False,
                "future_utility_accessed": False,
                "semantic_labelability_accessed": False,
                "d6_status_accessed": False,
            }
        )
    if _refit_report_uses_v2_fit_budget(schema_version):
        body["fit_budget_contract_version"] = REFIT02_FIT_BUDGET_CONTRACT_VERSION
    elif schema_version == REFIT02_REPORT_SCHEMA_VERSION_LEGACY:
        body["fit_budget_contract_version"] = REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
    return {**body, "receipt_sha256": canonical_sha256(body)}


def build_refit02_preflight_failure_report(
    *,
    producer_commit: str,
    reason_code: str,
    error_type: str,
    error: str,
    schema_version: str = REFIT02_REPORT_SCHEMA_VERSION,
) -> dict[str, Any]:
    if schema_version not in {
        REFIT02_REPORT_SCHEMA_VERSION,
        REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT,
        REFIT02_REPORT_SCHEMA_VERSION_LEGACY,
        REFIT02_REPORT_SCHEMA_VERSION_ORIGINAL,
    }:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 preflight-failure report schema is invalid",
        )
    body = {
        "schema_version": schema_version,
        "diagnostic_contract": _refit_diagnostic_contract_for_report(schema_version),
        "producer_commit": _require_commit(producer_commit, "producer_commit"),
        "source_authority": dict(SOURCE_AUTHORITY),
        "status": "diagnostic_failed",
        "mechanism_assessment": "inconclusive",
        "mechanism_assessment_reason_codes": [str(reason_code)],
        "error_type": str(error_type)[:256],
        "error": str(error)[-4000:],
        "current_authority": None,
        "historical_reference": None,
        "d5_compatibility_evidence_ready": False,
        "process_receipts": [],
        "completed_process_count": 0,
        "failed_process_receipt": None,
        "canonical_payload_bitwise_equal": False,
        "attempt_count": 0,
        "planned_hmm_fit_count": 0,
        "actual_hmm_fit_invocation_count": 0,
        "fit_budget_completion_unknown": False,
        "selection_performed": False,
        "d3_d4_descriptive_contracts_applied": False,
        "formal_model_set_acceptance_performed": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    if schema_version == REFIT02_REPORT_SCHEMA_VERSION:
        body.update(
            {
                "covariance_pattern_assessment": "evidence_incomplete",
                "diagnostic_evidence_complete": False,
                "validation_accessed": False,
                "future_utility_accessed": False,
                "semantic_labelability_accessed": False,
                "d6_status_accessed": False,
            }
        )
    if _refit_report_uses_v2_fit_budget(schema_version):
        body["fit_budget_contract_version"] = REFIT02_FIT_BUDGET_CONTRACT_VERSION
    elif schema_version == REFIT02_REPORT_SCHEMA_VERSION_LEGACY:
        body["fit_budget_contract_version"] = REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
    return {**body, "receipt_sha256": canonical_sha256(body)}


def validate_refit02_report(report: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(report)
    schema_version = normalized.get("schema_version")
    if (
        schema_version
        not in {
            REFIT02_REPORT_SCHEMA_VERSION,
            REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT,
            REFIT02_REPORT_SCHEMA_VERSION_LEGACY,
            REFIT02_REPORT_SCHEMA_VERSION_ORIGINAL,
        }
        or normalized.get("diagnostic_contract") != _refit_diagnostic_contract_for_report(str(schema_version))
        or normalized.get("status") not in {"not_applicable", "diagnostic_complete", "diagnostic_failed"}
        or normalized.get("source_authority") != SOURCE_AUTHORITY
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 report authority is invalid",
        )
    processes = normalized.get("process_receipts")
    preflight_failure = (
        normalized.get("status") == "diagnostic_failed"
        and normalized.get("current_authority") is None
        and normalized.get("historical_reference") is None
        and processes == []
        and normalized.get("failed_process_receipt") is None
    )
    execution_failure = (
        normalized.get("status") == "diagnostic_failed"
        and isinstance(normalized.get("current_authority"), Mapping)
        and isinstance(normalized.get("historical_reference"), Mapping)
        and isinstance(normalized.get("failed_process_receipt"), Mapping)
    )
    if normalized.get("status") == "not_applicable":
        rebuilt = build_refit02_not_applicable_report(
            dict(normalized.get("current_authority") or {}),
            dict(normalized.get("historical_reference") or {}),
            producer_commit=str(normalized.get("producer_commit") or ""),
            schema_version=str(schema_version),
        )
    elif preflight_failure:
        rebuilt = build_refit02_preflight_failure_report(
            producer_commit=str(normalized.get("producer_commit") or ""),
            reason_code=str((normalized.get("mechanism_assessment_reason_codes") or [""])[0]),
            error_type=str(normalized.get("error_type") or ""),
            error=str(normalized.get("error") or ""),
            schema_version=str(schema_version),
        )
    elif execution_failure:
        rebuilt = build_refit02_execution_failure_report(
            producer_commit=str(normalized.get("producer_commit") or ""),
            current_authority=dict(normalized.get("current_authority") or {}),
            historical_reference=dict(normalized.get("historical_reference") or {}),
            completed_processes=list(processes or ()),
            failed_process_receipt=dict(normalized.get("failed_process_receipt") or {}),
            schema_version=str(schema_version),
        )
    elif isinstance(processes, list) and len(processes) == 2:
        rebuilt = build_refit02_report(
            processes[0],
            processes[1],
            producer_commit=str(normalized.get("producer_commit") or ""),
        )
    else:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 report lifecycle is invalid",
        )
    if rebuilt != normalized:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "REFIT-02 report differs from its writer authority",
        )
    return normalized


def validate_controlled_process_failure_receipt(
    receipt: Mapping[str, Any],
    *,
    expected_process_identity: str,
    expected_producer_commit: str,
    expected_source_authority: Mapping[str, str] = SOURCE_AUTHORITY,
) -> dict[str, Any]:
    normalized = dict(receipt)
    identity = str(normalized.get("receipt_sha256") or "")
    body = {key: value for key, value in normalized.items() if key != "receipt_sha256"}
    required_false_flags = (
        "selection_performed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    )
    if (
        normalized.get("schema_version") != "hmm_risk_c008_b3_d1_child_failure_receipt_v1"
        or normalized.get("status") != "failed"
        or normalized.get("process_identity") != expected_process_identity
        or normalized.get("producer_commit") != _require_commit(expected_producer_commit, "expected_producer_commit")
        or normalized.get("source_authority") != dict(expected_source_authority)
        or not str(normalized.get("reason_code") or "").strip()
        or normalized.get("fit_budget_completion_unknown") not in {True, False}
        or any(normalized.get(field) is not False for field in required_false_flags)
        or identity != canonical_sha256(body)
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 controlled-process failure receipt is invalid",
        )
    for prefix in ("stdout", "stderr"):
        count = normalized.get(f"{prefix}_byte_count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                f"D1 controlled-process {prefix} byte count is invalid",
            )
        _require_sha256(normalized.get(f"{prefix}_sha256"), f"{prefix}_sha256")
    return normalized


def _validate_controlled_refit_failure_report(report: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(report)
    receipt = str(normalized.get("receipt_sha256") or "")
    body = {key: value for key, value in normalized.items() if key != "receipt_sha256"}
    required_false_flags = (
        "selection_performed",
        "formal_model_set_acceptance_performed",
        "hard_semantic_authority_changed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    )
    schema_version = str(normalized.get("schema_version") or "")
    source_authority = _report_source_authority(schema_version)
    if (
        normalized.get("diagnostic_contract") != "C-008-B3-REMEDIATION-D1-B-REFIT-01"
        or normalized.get("status") != "diagnostic_failed"
        or normalized.get("mechanism_assessment") != "inconclusive"
        or normalized.get("d5_compatibility_evidence_ready") is not False
        or normalized.get("source_authority") != source_authority
        or any(normalized.get(field) is not False for field in required_false_flags)
        or receipt != canonical_sha256(body)
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 controlled-refit failure report authority fields are invalid",
        )
    _require_commit(normalized.get("producer_commit"), "producer_commit")
    reasons = normalized.get("mechanism_assessment_reason_codes")
    completed = normalized.get("process_receipts")
    failed = normalized.get("failed_process_receipt")
    if (
        not isinstance(reasons, list)
        or not reasons
        or not isinstance(completed, list)
        or any(not isinstance(value, Mapping) for value in completed)
        or len(completed) > 2
        or normalized.get("completed_process_count") != len(completed)
        or normalized.get("attempt_count") != sum(int(value.get("attempt_count") or 0) for value in completed)
        or (failed is not None and not isinstance(failed, Mapping))
    ):
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 controlled-refit failure evidence is invalid",
        )
    for index, process in enumerate(completed, start=1):
        validate_process_receipt(
            process,
            expected_process_identity=f"fresh_process_{index}",
            expected_producer_commit=str(normalized["producer_commit"]),
        )
    if failed is not None:
        expected_failure_identity = f"fresh_process_{len(completed) + 1}" if len(completed) < 2 else "parent_finalize"
        validate_controlled_process_failure_receipt(
            failed,
            expected_process_identity=expected_failure_identity,
            expected_producer_commit=str(normalized["producer_commit"]),
            expected_source_authority=source_authority,
        )
        if normalized.get("fit_budget_completion_unknown") is not failed.get("fit_budget_completion_unknown"):
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "D1 controlled-refit failure completion state is inconsistent",
            )
    elif completed or normalized.get("fit_budget_completion_unknown") is not False:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_contract_invalid",
            "D1 controlled-refit failure is missing its process failure evidence",
        )
    return normalized


def write_controlled_refit_report(path: Path, report: Mapping[str, Any]) -> str:
    target = Path(path)
    normalized = dict(report)
    if normalized.get("schema_version") in {
        REFIT02_REPORT_SCHEMA_VERSION,
        REFIT02_REPORT_SCHEMA_VERSION_MATCHED_FIT,
        REFIT02_REPORT_SCHEMA_VERSION_LEGACY,
        REFIT02_REPORT_SCHEMA_VERSION_ORIGINAL,
    }:
        validate_refit02_report(normalized)
    elif normalized.get("status") == "diagnostic_failed":
        _validate_controlled_refit_failure_report(normalized)
    else:
        validate_controlled_refit_report(normalized)
    payload = canonical_json_bytes(normalized) + b"\n"
    identity = canonical_sha256(normalized)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.read_bytes() != payload:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                f"D1 controlled-refit artifact collision: {target}",
            )
        return identity
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if temporary.read_bytes() != payload:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "D1 controlled-refit temporary artifact readback mismatch",
            )
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.read_bytes() != payload:
                raise D1InactiveDimensionError(
                    "hmm_risk_model_inactive_dimension_contract_invalid",
                    f"D1 controlled-refit artifact collision: {target}",
                ) from None
        value = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or canonical_sha256(value) != identity:
            raise D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "D1 controlled-refit canonical readback mismatch",
            )
    finally:
        temporary.unlink(missing_ok=True)
    return identity
