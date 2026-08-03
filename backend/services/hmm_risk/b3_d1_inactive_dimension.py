from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from backend.services.hmm_risk.b3_acceptance import D3_CONTRACT_VERSION, L2_RETRAIN_VERSION, RESTART_SCHEDULE
from backend.services.hmm_risk.b3_training import (
    B3CoreFitEvidence,
    B3TrainingStageError,
    B3TrainOnlySeries,
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
)


ALGORITHM_VERSION = "hmm_risk_c008_b3_d1_inactive_dimension_v1"
ATTEMPT_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_attempt_v1"
PROCESS_SCHEMA_VERSION_V1 = "hmm_risk_c008_b3_d1_controlled_process_v1"
PROCESS_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_process_v2"
REPORT_SCHEMA_VERSION_V1 = "hmm_risk_c008_b3_d1_controlled_refit_report_v1"
REPORT_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_controlled_refit_report_v2"
TREATMENT_ROLE = "treatment"
CONTROL_ROLE = "control"
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
SOURCE_AUTHORITY_V1 = {
    "formal_report_sha256": FORMAL_REPORT_SHA256,
    "blocker_report_sha256": BLOCKER_REPORT_SHA256,
    "remediation_report_sha256": REMEDIATION_REPORT_SHA256,
}
SOURCE_AUTHORITY = {
    **SOURCE_AUTHORITY_V1,
    "c010_a5_report_sha256": C010_A5_REPORT_SHA256,
    "c010_a5_partition_sha256": C010_A5_PARTITION_SHA256,
}

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


def build_projection(
    item: B3TrainOnlySeries,
    *,
    preprocess: Mapping[str, Any],
    role: str,
    profile_receipt_sha256: str,
    source_set_sha256: str,
    preprocess_identity_sha256: str,
    feature_definition_sha256: str,
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
    if canonical_sha256(dict(item.train_input_manifest)) != expected_train_input:
        raise D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "D1 train input manifest differs from the approved profile authority",
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
        "schema_version": "hmm_risk_c008_b3_d1_projection_receipt_v1",
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
        "train_input_manifest_sha256": expected_train_input,
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
) -> dict[str, str]:
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
        "observation_manifest_hash": item.observation_manifest_hash,
        "pit_constituent_manifest_hash": item.pit_constituent_manifest_hash,
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
    )


def build_process_receipt(
    *,
    process_identity: str,
    producer_commit: str,
    attempts: Sequence[Mapping[str, Any]],
    treatment_source_identities: Sequence[Mapping[str, Any]],
    control_source_identities: Sequence[Mapping[str, Any]],
    _schema_version: str = PROCESS_SCHEMA_VERSION,
    _source_authority: Mapping[str, str] = SOURCE_AUTHORITY,
) -> dict[str, Any]:
    normalized_commit = _require_commit(producer_commit, "producer_commit")
    treatment_sources = validate_source_identity_set(
        treatment_source_identities, expected_sha256=TREATMENT_SOURCE_SET_SHA256
    )
    control_sources = validate_source_identity_set(control_source_identities, expected_sha256=CONTROL_SOURCE_SET_SHA256)
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
            or attempt.get("schema_version") != ATTEMPT_SCHEMA_VERSION
            or attempt.get("algorithm_version") != ALGORITHM_VERSION
            or attempt.get("family") != "autocycle_all_core"
            or attempt.get("level") != "L2"
            or attempt.get("sector_code") != expected_sector
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
    source_authority = SOURCE_AUTHORITY if schema_version == PROCESS_SCHEMA_VERSION else SOURCE_AUTHORITY_V1
    if (
        schema_version not in {PROCESS_SCHEMA_VERSION_V1, PROCESS_SCHEMA_VERSION}
        or normalized.get("algorithm_version") != ALGORITHM_VERSION
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
    if (
        not isinstance(attempts, list)
        or not isinstance(treatment_sources, list)
        or not isinstance(control_sources, list)
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
    source_authority = SOURCE_AUTHORITY if schema_version == REPORT_SCHEMA_VERSION else SOURCE_AUTHORITY_V1
    if (
        schema_version not in {REPORT_SCHEMA_VERSION_V1, REPORT_SCHEMA_VERSION}
        or normalized.get("diagnostic_contract") != "C-008-B3-REMEDIATION-D1-B-REFIT-01"
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
    source_authority = SOURCE_AUTHORITY if schema_version == REPORT_SCHEMA_VERSION else SOURCE_AUTHORITY_V1
    if (
        schema_version not in {REPORT_SCHEMA_VERSION_V1, REPORT_SCHEMA_VERSION}
        or normalized.get("diagnostic_contract") != "C-008-B3-REMEDIATION-D1-B-REFIT-01"
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
    if normalized.get("status") == "diagnostic_failed":
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
