from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import sys
from typing import Any

import numpy as np

from backend.services.hmm_risk.b3_acceptance import (
    D3_CONTRACT_VERSION,
    D4_COVARIANCE_VERSION,
    D4_LIKELIHOOD_VERSION,
    D4_OCCUPANCY_VERSION,
    D5_SELECTION_VERSION,
    D6_SEMANTIC_VERSION,
    L2_RETRAIN_VERSION,
    RESTART_SCHEDULE,
    evaluate_covariance_acceptance,
    evaluate_likelihood_acceptance,
    evaluate_semantic_validation,
    evaluate_train_occupancy,
    map_covariance_prior_objective,
    map_numeric_envelope,
    validate_d4_training_receipts,
)
from backend.services.hmm_risk.b3_mixed_dimension import (
    INACTIVE_DIMENSION_REASON_CODE,
    MIXED_DIMENSION_CONTRACT_VERSION,
    MIXED_LEVEL_SCHEMA_VERSION,
    MIXED_MODEL_SCHEMA_VERSION,
    MIXED_REPEAT_SCHEMA_VERSION,
    MIXED_TRAINING_ENTRY_SCHEMA_VERSION,
    build_level_dimension_identity,
    build_projection_receipt,
    uses_mixed_dimension_level,
    validate_projection_receipt,
)
from backend.services.hmm_risk.state_model_set import (
    ALL_CORE_FEATURES,
    BASE_FEATURES,
    C008_B3_DIAG04_NU,
    HMM_N_ITER,
    L1TrainingSeries,
    SCHEMA_VERSION,
    StateModelSetError,
    _apply_preprocess,
    _b3_diag04_covariance_evidence,
    _finite_array,
    _fit_preprocess,
    _manual_b3_diag04_initialization,
    _probability_vector,
    _sector_local_reference_variance,
    _transition_matrix,
    _write_immutable,
    c008_b3_diag04_fixed_numeric_environment,
    c008_b3_diag04_parameter_profile,
    canonical_json_bytes,
    canonical_sha256,
    causal_forward_posteriors,
)
from backend.services.hmm_risk.stock_fact_observation import C010_FORMULA_VERSION, validate_c010_policy_manifest


REFIT03_RAW_COVARIANCE_SCHEMA_VERSION = "hmm_risk_c008_b3_d1_covariance_raw_capture_v1"
REFIT03_RAW_COVARIANCE_AUTHORITY = "gaussian_hmm_internal_diag_covars_v1"
REFIT03_STAGE_EVIDENCE_SCHEMA_VERSION = "hmm_risk_b3_training_stage_evidence_v1"


class B3TrainingStageError(StateModelSetError):
    """Expected candidate-local failure with a stable stage and reason code."""

    def __init__(
        self,
        stage: str,
        reason_code: str,
        cause: Exception,
        *,
        stage_evidence: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(f"{stage}: {cause}")
        self.stage = stage
        self.reason_code = reason_code
        self.cause_type = type(cause).__name__
        self.cause_evidence = dict(getattr(cause, "evidence", {}) or {})
        self.cause_stage = str(getattr(cause, "stage", "") or "") or None
        self.stage_evidence = dict(stage_evidence or {})


class _MapJointFitFailure(StateModelSetError):
    def __init__(self, stage: str, reason_code: str, message: str, *, evidence: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.stage = stage
        self.reason_code = reason_code
        self.evidence = dict(evidence)


@dataclass(frozen=True)
class _MapJointFitResult:
    likelihood: Mapping[str, Any]
    covariance: Mapping[str, Any]
    covariance_evidence: Mapping[str, Any]
    raw_covariance_evidence: Mapping[str, Any]
    raw_covars: np.ndarray
    terminal_raw_likelihood: float


@dataclass(frozen=True)
class B3FittedModel:
    family: str
    level: str
    seed: int
    sector_code: str
    feature_names: tuple[str, ...]
    preprocess: Mapping[str, Any]
    startprob: np.ndarray
    transmat: np.ndarray
    means: np.ndarray
    covars: np.ndarray
    parameter_profile_sha256: str
    numeric_environment_sha256: str
    observation_manifest_hash: str
    pit_constituent_manifest_hash: str
    model_payload_sha256: str
    projection_receipt: Mapping[str, Any] | None = None

    def payload(self) -> dict[str, Any]:
        body = {
            "schema_version": (
                MIXED_MODEL_SCHEMA_VERSION if self.projection_receipt is not None else "hmm_risk_b3_fitted_model_v1"
            ),
            "contract_version": D3_CONTRACT_VERSION,
            "family": self.family,
            "level": self.level,
            "seed": self.seed,
            "sector_code": self.sector_code,
            "feature_names": list(self.feature_names),
            "preprocess": dict(self.preprocess),
            "startprob": self.startprob.tolist(),
            "transmat": self.transmat.tolist(),
            "means": self.means.tolist(),
            "covariance_type": "diag",
            "covars": self.covars.tolist(),
            "parameter_profile_sha256": self.parameter_profile_sha256,
            "numeric_environment_sha256": self.numeric_environment_sha256,
            "observation_manifest_hash": self.observation_manifest_hash,
            "pit_constituent_manifest_hash": self.pit_constituent_manifest_hash,
        }
        if self.projection_receipt is not None:
            body.update(
                {
                    "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                    "feature_count": len(self.feature_names),
                    "likelihood_feature_names": list(self.projection_receipt["active_feature_names"]),
                    "likelihood_feature_count": self.projection_receipt["likelihood_feature_count"],
                    "projection_receipt": dict(self.projection_receipt),
                    "projection_sha256": self.projection_receipt["projection_sha256"],
                }
            )
        return {**body, "model_payload_sha256": self.model_payload_sha256}


@dataclass(frozen=True)
class B3TrainOnlySeries:
    sector_code: str
    sector_name: str
    train_observations: np.ndarray
    train_dates: tuple[date, ...]
    pit_l2_constituents: tuple[str, ...]
    pit_constituent_manifest_hash: str
    observation_manifest_hash: str
    train_input_manifest: Mapping[str, Any]

    def validate(self, feature_count: int) -> None:
        if not self.sector_code.strip() or not self.sector_name.strip():
            raise StateModelSetError("B3 train-only sector code/name must be non-empty")
        train = np.asarray(self.train_observations, dtype=np.float64)
        if train.ndim != 2 or train.shape[1] != feature_count or train.shape[0] < 120:
            raise StateModelSetError(f"{self.sector_code} has insufficient train-only observations")
        if not np.isfinite(train).all():
            raise StateModelSetError(f"{self.sector_code} train-only observations are non-finite")
        if len(self.train_dates) != train.shape[0]:
            raise StateModelSetError(f"{self.sector_code} train-only dates do not align")
        if tuple(sorted(self.train_dates)) != self.train_dates or len(set(self.train_dates)) != len(self.train_dates):
            raise StateModelSetError(f"{self.sector_code} train-only dates must be strictly increasing")
        if not self.pit_l2_constituents:
            raise StateModelSetError(f"{self.sector_code} train-only constituent identity is missing")
        manifest = self.train_input_manifest
        dates = [value.isoformat() for value in self.train_dates]
        if (
            not isinstance(manifest, Mapping)
            or manifest.get("schema_version") != "hmm_risk_d4_train_frozen_input_manifest_v1"
            or manifest.get("direct_sector_level") not in {"L1", "L2"}
            or manifest.get("sector_code") != self.sector_code
            or manifest.get("train_dates") != dates
            or manifest.get("train_dates_sha256") != canonical_sha256(dates)
            or manifest.get("train_observation_sha256") != canonical_sha256(train.tolist())
        ):
            raise StateModelSetError(f"{self.sector_code} train-only frozen input manifest is invalid")
        for field in (
            "dataset_manifest_hash",
            "mapping_manifest_hash",
            "calendar_manifest_hash",
            "feature_domain_policy_sha256",
        ):
            _require_hex_identity(str(manifest.get(field) or ""), length=64, label=field)


@dataclass(frozen=True)
class B3CoreFitEvidence:
    """Artifact-neutral train-only HMM evidence shared by formal and controlled fits."""

    initialization: Mapping[str, Any]
    monitor_evidence: Mapping[str, Any]
    likelihood: Mapping[str, Any]
    covariance: Mapping[str, Any]
    train_occupancy: Mapping[str, Any]
    startprob: np.ndarray
    transmat: np.ndarray
    means: np.ndarray
    covars: np.ndarray
    terminal_likelihood: float | None
    model_entry_status: str
    model_entry_valid: bool
    training_stage_evidence: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class _B3PreparedTrainOnlyInitialization:
    """Shared, fit-free initialization authority for formal and diagnostic paths."""

    train: np.ndarray
    reference: np.ndarray
    startprob: np.ndarray
    transmat: np.ndarray
    means: np.ndarray
    covars: np.ndarray
    prior: np.ndarray
    evidence: Mapping[str, Any]


def _train_only_frame(
    panel: Any,
    *,
    sector_code: str,
    feature_names: tuple[str, ...],
    train_start: date,
    train_end: date,
) -> Any:
    sector = panel.xs(sector_code, level="l1_code")
    sector_dates = sector.index.date
    return sector.loc[
        (sector_dates >= train_start) & (sector_dates <= train_end),
        list(feature_names),
    ].dropna()


def audit_train_only_coverage(
    panel: Any,
    *,
    feature_names: Sequence[str],
    train_start: date,
    train_end: date,
    expected_sector_count: int,
    direct_sector_level: str,
) -> dict[str, Any]:
    """Audit the complete formal train matrix without fitting or reading validation evidence."""

    features = tuple(str(value) for value in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("B3 train-only feature family is invalid")
    if direct_sector_level not in {"L1", "L2"} or expected_sector_count not in {31, 131}:
        raise StateModelSetError("B3 train-only level/count contract is invalid")
    codes = tuple(sorted(str(value) for value in panel.index.get_level_values("l1_code").unique()))
    entries = []
    for code in codes:
        train = _train_only_frame(
            panel,
            sector_code=code,
            feature_names=features,
            train_start=train_start,
            train_end=train_end,
        )
        row_count = len(train)
        entry_body = {
            "sector_code": code,
            "train_row_count": row_count,
            "minimum_train_row_count": 120,
            "train_coverage_valid": row_count >= 120,
            "first_train_date": None if train.empty else train.index[0].date().isoformat(),
            "last_train_date": None if train.empty else train.index[-1].date().isoformat(),
            "train_dates_sha256": canonical_sha256([item.date().isoformat() for item in train.index]),
        }
        entries.append({**entry_body, "entry_sha256": canonical_sha256(entry_body)})
    insufficient = [entry for entry in entries if not entry["train_coverage_valid"]]
    sector_set_valid = len(codes) == expected_sector_count and len(set(codes)) == expected_sector_count
    valid = sector_set_valid and not insufficient
    body = {
        "schema_version": "hmm_risk_b3_train_coverage_preflight_v1",
        "direct_sector_level": direct_sector_level,
        "feature_names": list(features),
        "train_start": train_start.isoformat(),
        "train_end": train_end.isoformat(),
        "minimum_train_row_count": 120,
        "expected_sector_count": expected_sector_count,
        "actual_sector_count": len(codes),
        "canonical_sector_codes": list(codes),
        "sector_set_valid": sector_set_valid,
        "entry_count": len(entries),
        "entries": entries,
        "minimum_observed_train_row_count": min((entry["train_row_count"] for entry in entries), default=0),
        "maximum_observed_train_row_count": max((entry["train_row_count"] for entry in entries), default=0),
        "insufficient_sector_count": len(insufficient),
        "insufficient_sector_codes": [entry["sector_code"] for entry in insufficient],
        "failure_reason_codes": ([] if valid else ["hmm_risk_model_train_observation_coverage_insufficient"]),
        "train_coverage_valid": valid,
        "fit_performed": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "artifact_write_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def iter_train_only_series(
    panel: Any,
    *,
    feature_names: Sequence[str],
    train_start: date,
    train_end: date,
    constituent_manifest: Mapping[str, Mapping[str, Any]],
    expected_sector_count: int,
    direct_sector_level: str,
    frozen_input_identity: Mapping[str, Any] | None = None,
) -> Iterable[B3TrainOnlySeries]:
    """Yield one frozen D3/D4/D5 train profile at a time without retaining all matrices."""

    features = tuple(str(value) for value in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("B3 train-only feature family is invalid")
    if direct_sector_level not in {"L1", "L2"} or expected_sector_count not in {31, 131}:
        raise StateModelSetError("B3 train-only level/count contract is invalid")
    codes = tuple(sorted(panel.index.get_level_values("l1_code").unique()))
    if len(codes) != expected_sector_count:
        raise StateModelSetError(
            f"B3 train-only requires {expected_sector_count} direct {direct_sector_level} sectors; actual={len(codes)}"
        )
    for code in codes:
        sector = panel.xs(code, level="l1_code")
        train = _train_only_frame(
            panel,
            sector_code=str(code),
            feature_names=features,
            train_start=train_start,
            train_end=train_end,
        )
        if len(train) < 120:
            raise StateModelSetError(f"{code} train-only observation coverage is insufficient: {len(train)}")
        constituent = constituent_manifest.get(str(code))
        if not isinstance(constituent, Mapping):
            raise StateModelSetError(f"{code} train-only constituent manifest is missing")
        l2_codes = tuple(sorted(str(value) for value in constituent.get("l2_codes") or ()))
        if not l2_codes:
            raise StateModelSetError(f"{code} train-only L2 identity is missing")
        body = {
            "contract_version": D3_CONTRACT_VERSION,
            "direct_sector_level": direct_sector_level,
            "sector_code": str(code),
            "feature_names": list(features),
            "train_dates": [item.date().isoformat() for item in train.index],
            "train_sha256": canonical_sha256(train.to_numpy(dtype=np.float64).tolist()),
        }
        train_dates = [item.date().isoformat() for item in train.index]
        train_input_manifest = {
            **dict(frozen_input_identity or {}),
            "schema_version": "hmm_risk_d4_train_frozen_input_manifest_v1",
            "direct_sector_level": direct_sector_level,
            "sector_code": str(code),
            "train_dates": train_dates,
            "train_dates_sha256": canonical_sha256(train_dates),
            "train_observation_sha256": canonical_sha256(train.to_numpy(dtype=np.float64).tolist()),
        }
        yield B3TrainOnlySeries(
            sector_code=str(code),
            sector_name=str(sector["l1_name"].dropna().iloc[-1]),
            train_observations=train.to_numpy(dtype=np.float64),
            train_dates=tuple(item.date() for item in train.index),
            pit_l2_constituents=l2_codes,
            pit_constituent_manifest_hash=canonical_sha256(constituent),
            observation_manifest_hash=canonical_sha256(body),
            train_input_manifest=train_input_manifest,
        )


def build_train_only_series(
    panel: Any,
    *,
    feature_names: Sequence[str],
    train_start: date,
    train_end: date,
    constituent_manifest: Mapping[str, Mapping[str, Any]],
    expected_sector_count: int,
    direct_sector_level: str,
    frozen_input_identity: Mapping[str, Any] | None = None,
) -> dict[str, B3TrainOnlySeries]:
    """Freeze only D3/D4/D5 train inputs; validation and future utility remain unread."""

    values = iter_train_only_series(
        panel,
        feature_names=feature_names,
        train_start=train_start,
        train_end=train_end,
        constituent_manifest=constituent_manifest,
        expected_sector_count=expected_sector_count,
        direct_sector_level=direct_sector_level,
        frozen_input_identity=frozen_input_identity,
    )
    return {value.sector_code: value for value in values}


def formal_b3_parameter_profile() -> dict[str, Any]:
    diagnostic_profile = c008_b3_diag04_parameter_profile()
    return {
        **diagnostic_profile,
        "schema_version": "hmm_risk_b3_parameter_profile_v1",
        "contract": D3_CONTRACT_VERSION,
        "d4_likelihood_contract": D4_LIKELIHOOD_VERSION,
        "d4_covariance_contract": D4_COVARIANCE_VERSION,
        "d4_occupancy_contract": D4_OCCUPANCY_VERSION,
        "convergence_authority": "covariance_prior_map_objective_with_d4_02_joint_stop",
        "em_executor": "hmmlearn_0_3_3_private_estep_mstep_v1",
        "raw_likelihood_role": "diagnostic_and_d5_joint_stop_score_only",
        "numeric_contract_status": "USER_APPROVED_FORMAL_CONTRACT",
        "formal_acceptance_thresholds_applied_by_independent_d4_receipts": True,
        "selection_performed_by_profile": False,
    }


def _prepare_b3_preprocessed_train_only_initialization(
    item: B3TrainOnlySeries,
    *,
    train: np.ndarray,
    seed: int,
) -> _B3PreparedTrainOnlyInitialization:
    """Apply the exact D3 initialization contract without invoking HMM fit."""

    prepared = np.ascontiguousarray(np.asarray(train, dtype="<f8"))
    if prepared.ndim != 2 or prepared.shape[0] != len(item.train_dates) or prepared.shape[1] < 1:
        raise B3TrainingStageError(
            "initialization",
            "hmm_risk_model_initialization_failed",
            StateModelSetError("B3 preprocessed train matrix shape is invalid"),
        )
    if not np.isfinite(prepared).all():
        raise B3TrainingStageError(
            "initialization",
            "hmm_risk_model_initialization_failed",
            StateModelSetError("B3 preprocessed train matrix contains non-finite values"),
        )
    try:
        reference = _sector_local_reference_variance(prepared)
        startprob, transmat, means, initialized_covars, initialization = _manual_b3_diag04_initialization(
            prepared,
            sector_reference_variance=reference,
            random_seed=seed,
        )
    except (StateModelSetError, ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        raise B3TrainingStageError(
            "initialization",
            "hmm_risk_model_initialization_failed",
            exc,
        ) from exc
    initialization = {
        **initialization,
        "schema_version": "hmm_risk_b3_manual_initialization_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "diagnostic_source_contract": initialization.get("schema_version"),
        "formal_initialization_contract_applied": True,
    }
    prior = C008_B3_DIAG04_NU * np.broadcast_to(reference, (3, prepared.shape[1])).copy()
    return _B3PreparedTrainOnlyInitialization(
        train=prepared,
        reference=reference,
        startprob=startprob,
        transmat=transmat,
        means=means,
        covars=initialized_covars,
        prior=prior,
        evidence=initialization,
    )


def prepare_b3_preprocessed_train_only_initialization(
    item: B3TrainOnlySeries,
    *,
    train: np.ndarray,
    seed: int,
) -> Mapping[str, Any]:
    """Return canonical initialization evidence without importing or invoking HMM."""

    prepared = _prepare_b3_preprocessed_train_only_initialization(item, train=train, seed=seed)
    return dict(prepared.evidence)


def _raw_covariance_value_classification(bits: int) -> str:
    sign = (bits >> 63) & 1
    exponent = (bits >> 52) & 0x7FF
    fraction = bits & ((1 << 52) - 1)
    if exponent == 0x7FF:
        if fraction:
            return "nan"
        return "negative_infinity" if sign else "positive_infinity"
    if exponent == 0 and fraction == 0:
        return "negative_zero" if sign else "positive_zero"
    return "finite_negative" if sign else "finite_positive"


def capture_raw_diag_covariance_evidence(
    value: Any,
    *,
    expected_shape: tuple[int, int],
    evidence_unavailable_reason: str | None = None,
) -> dict[str, Any]:
    """Capture the private diagonal covariance buffer without coercion or repair."""

    python_type = f"{type(value).__module__}.{type(value).__qualname__}"
    body: dict[str, Any] = {
        "schema_version": REFIT03_RAW_COVARIANCE_SCHEMA_VERSION,
        "raw_authority": REFIT03_RAW_COVARIANCE_AUTHORITY,
        "expected_shape": list(expected_shape),
        "actual_python_type": python_type,
        "actual_dtype": None,
        "actual_shape": None,
        "actual_strides": None,
        "actual_nbytes": None,
        "actual_byteorder": None,
        "c_contiguous": None,
        "cells": [],
        "reason_codes": [],
        "raw_validity": False,
        "evidence_unavailable_reason": None,
    }
    if not isinstance(value, np.ndarray):
        body["reason_codes"] = ["hmm_risk_model_covariance_raw_type_invalid"]
        body["evidence_unavailable_reason"] = evidence_unavailable_reason or "raw_authority_is_not_numpy_ndarray"
        return {**body, "capture_receipt_sha256": canonical_sha256(body)}

    raw_view = value.view(np.ndarray)
    body.update(
        {
            "actual_dtype": raw_view.dtype.str,
            "actual_shape": list(raw_view.shape),
            "actual_strides": list(raw_view.strides),
            "actual_nbytes": int(raw_view.nbytes),
            "actual_byteorder": raw_view.dtype.byteorder,
            "c_contiguous": bool(raw_view.flags.c_contiguous),
        }
    )
    reasons: list[str] = []
    if type(value) is not np.ndarray:
        reasons.append("hmm_risk_model_covariance_raw_type_invalid")
    if raw_view.dtype.kind != "f" or raw_view.dtype.itemsize != 8:
        reasons.append("hmm_risk_model_covariance_raw_dtype_invalid")
        body["evidence_unavailable_reason"] = "raw_authority_is_not_ieee754_float64"
    if raw_view.ndim != 2 or tuple(raw_view.shape) != expected_shape:
        reasons.append("hmm_risk_model_covariance_raw_shape_invalid")
    if not raw_view.flags.c_contiguous:
        reasons.append("hmm_risk_model_covariance_raw_layout_invalid")

    cells: list[dict[str, Any]] = []
    non_finite = False
    non_positive = False
    if raw_view.dtype.kind == "f" and raw_view.dtype.itemsize == 8 and raw_view.ndim == 2:
        byteorder = raw_view.dtype.byteorder
        if byteorder == "=":
            byteorder = "<" if sys.byteorder == "little" else ">"
        semantic_byteorder = "little" if byteorder == "<" else "big"
        for state_index in range(raw_view.shape[0]):
            for feature_index in range(raw_view.shape[1]):
                scalar = raw_view[state_index, feature_index]
                raw_bytes = np.asarray(scalar, dtype=raw_view.dtype).tobytes()
                bits = int.from_bytes(raw_bytes, byteorder=semantic_byteorder, signed=False)
                classification = _raw_covariance_value_classification(bits)
                finite = classification not in {"nan", "positive_infinity", "negative_infinity"}
                positive = classification == "finite_positive"
                if not finite:
                    non_finite = True
                if finite and not positive:
                    non_positive = True
                cells.append(
                    {
                        "state_index": state_index,
                        "feature_index": feature_index,
                        "semantic_bit_pattern_hex": f"{bits:016x}",
                        "classification": classification,
                        "float_hex": float(scalar).hex() if finite else None,
                    }
                )
    body["cells"] = cells
    if non_finite:
        reasons.append("hmm_risk_model_covariance_raw_non_finite")
    if non_positive:
        reasons.append("hmm_risk_model_covariance_raw_non_positive")
    body["reason_codes"] = list(dict.fromkeys(reasons))
    body["raw_validity"] = not reasons
    return {**body, "capture_receipt_sha256": canonical_sha256(body)}


def _training_stage_evidence(
    *,
    fit_invoked: bool,
    fit_returned: bool,
    completed_stages: Sequence[str],
    initialization: Mapping[str, Any],
    monitor_evidence: Mapping[str, Any] | None = None,
    likelihood: Mapping[str, Any] | None = None,
    raw_covariance_evidence: Mapping[str, Any] | None = None,
    covariance_evidence: Mapping[str, Any] | None = None,
    stage_specific_cause_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    body = {
        "schema_version": REFIT03_STAGE_EVIDENCE_SCHEMA_VERSION,
        "fit_invoked": bool(fit_invoked),
        "fit_returned": bool(fit_returned),
        "completed_stages": list(completed_stages),
        "initialization_evidence": dict(initialization),
        "monitor_evidence": None if monitor_evidence is None else dict(monitor_evidence),
        "likelihood_evidence": None if likelihood is None else dict(likelihood),
        "raw_covariance_evidence": (None if raw_covariance_evidence is None else dict(raw_covariance_evidence)),
        "covariance_evidence": None if covariance_evidence is None else dict(covariance_evidence),
        "stage_specific_cause_evidence": dict(stage_specific_cause_evidence or {}),
    }
    return {**body, "stage_evidence_sha256": canonical_sha256(body)}


def _map_joint_loop_evidence(
    *,
    raw_likelihood_history: Sequence[float],
    map_objective_history: Sequence[float],
    map_prior_adjustment_history: Sequence[float],
    objective_component_history: Sequence[Mapping[str, Any]],
    covariance_valid_history: Sequence[bool],
    covariance_receipt_sha256_history: Sequence[str],
    joint_stop_iteration: int | None,
) -> dict[str, Any]:
    return {
        "authority": "covariance_prior_map_objective",
        "maximum_iterations": HMM_N_ITER,
        "raw_likelihood_history": list(raw_likelihood_history),
        "map_objective_history": list(map_objective_history),
        "map_prior_adjustment_history": list(map_prior_adjustment_history),
        "objective_component_history": [dict(value) for value in objective_component_history],
        "covariance_valid_history": list(covariance_valid_history),
        "covariance_receipt_sha256_history": list(covariance_receipt_sha256_history),
        "joint_stop_iteration": joint_stop_iteration,
        "raw_likelihood_is_diagnostic_only": True,
        "postfit_projection_performed": False,
    }


def _evaluate_map_loop_or_fail(
    loop_evidence: Mapping[str, Any],
    *,
    iteration: int,
    raw_covariance_evidence: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    try:
        return evaluate_likelihood_acceptance(loop_evidence)
    except (ValueError, FloatingPointError) as exc:
        raise _MapJointFitFailure(
            "likelihood",
            "hmm_risk_model_likelihood_evidence_invalid",
            str(exc),
            evidence={
                "iteration": iteration,
                "completed_stages": ["initialization", "fit", "raw_covariance_capture", "map_objective", "covariance"],
                "raw_covariance_evidence": (None if raw_covariance_evidence is None else dict(raw_covariance_evidence)),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "d4_derived_evidence_status": "not_computable_posterior_audit_unavailable",
                "covariance_status": "insufficient_evidence",
                "covariance_valid": False,
                "state_posterior_mass": None,
                "posterior_weighted_variance_about_weighted_mean": None,
                "posterior_second_moment_about_fitted_mean": None,
                "mstep_expected_covariance": None,
                "dynamic_lower_reference": None,
                "dynamic_upper_reference": None,
                "mstep_relative_residual": None,
            },
        ) from exc


def _run_b3_map_joint_em(model: Any, prepared: np.ndarray, reference: np.ndarray) -> _MapJointFitResult:
    """Run the approved MAP/D4-02 joint-stop loop without hmmlearn's raw-likelihood monitor."""

    lengths = np.asarray([prepared.shape[0]], dtype=np.int64)
    raw_history: list[float] = []
    map_history: list[float] = []
    prior_history: list[float] = []
    component_history: list[Mapping[str, Any]] = []
    covariance_valid_history: list[bool] = []
    covariance_hash_history: list[str] = []
    latest_covariance: Mapping[str, Any] | None = None
    latest_covariance_evidence: Mapping[str, Any] | None = None
    latest_raw_capture: Mapping[str, Any] | None = None
    latest_raw_covars: np.ndarray | None = None
    try:
        model._init(prepared, lengths)
        model._check()
    except (ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        raise _MapJointFitFailure(
            "fit",
            "hmm_risk_model_fit_failed",
            str(exc),
            evidence={"error_type": type(exc).__name__, "error": str(exc), "fit_phase": "model_check"},
        ) from exc

    for iteration in range(1, HMM_N_ITER + 1):
        try:
            stats, raw_log_likelihood = model._do_estep(prepared, lengths)
        except (ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
            raise _MapJointFitFailure(
                "fit",
                "hmm_risk_model_fit_failed",
                str(exc),
                evidence={"error_type": type(exc).__name__, "error": str(exc), "iteration": iteration},
            ) from exc
        try:
            raw_source = model._covars_
        except AttributeError as exc:
            raw_capture = capture_raw_diag_covariance_evidence(
                None,
                expected_shape=(3, prepared.shape[1]),
                evidence_unavailable_reason="gaussian_hmm_internal_diag_covars_missing",
            )
            raise _MapJointFitFailure(
                "covariance",
                "hmm_risk_model_covariance_raw_type_invalid",
                str(exc),
                evidence={
                    "iteration": iteration,
                    "completed_stages": ["initialization", "fit", "raw_covariance_capture"],
                    "raw_covariance_evidence": raw_capture,
                    "d4_derived_evidence_status": "not_computable_raw_covariance_invalid",
                    "covariance_status": "failed",
                    "covariance_valid": False,
                    "evidence_unavailable_reason": "gaussian_hmm_internal_diag_covars_missing",
                },
            ) from exc

        raw_capture = capture_raw_diag_covariance_evidence(
            raw_source,
            expected_shape=(3, prepared.shape[1]),
        )
        if raw_capture.get("raw_validity") is not True:
            raise _MapJointFitFailure(
                "covariance",
                str((raw_capture.get("reason_codes") or ["hmm_risk_model_covariance_invalid"])[0]),
                "raw covariance is invalid during MAP iteration",
                evidence={
                    "iteration": iteration,
                    "completed_stages": ["initialization", "fit", "raw_covariance_capture"],
                    "raw_covariance_evidence": raw_capture,
                    "d4_derived_evidence_status": "not_computable_raw_covariance_invalid",
                    "covariance_status": "failed",
                    "covariance_valid": False,
                    "state_posterior_mass": None,
                    "posterior_weighted_variance_about_weighted_mean": None,
                    "posterior_second_moment_about_fitted_mean": None,
                    "mstep_expected_covariance": None,
                    "dynamic_lower_reference": None,
                    "dynamic_upper_reference": None,
                    "mstep_relative_residual": None,
                },
            )
        raw_covars = np.asarray(raw_source, dtype=np.float64)
        try:
            objective = map_covariance_prior_objective(
                raw_log_likelihood,
                raw_covars,
                model.covars_prior,
                model.covars_weight,
            )
        except (TypeError, ValueError, FloatingPointError) as exc:
            raise _MapJointFitFailure(
                "likelihood",
                "hmm_risk_model_map_objective_non_finite",
                str(exc),
                evidence={"iteration": iteration, "raw_covariance_evidence": raw_capture},
            ) from exc

        try:
            covariance_evidence, _, smoothed_audit_log_likelihood = _b3_diag04_covariance_evidence(
                model,
                prepared,
                raw_covars=raw_covars,
                sector_reference_variance=reference,
            )
            covariance_evidence = {
                **covariance_evidence,
                "train_rows": int(prepared.shape[0]),
                "postfit_projection_performed": False,
                "smoothed_audit_log_likelihood": smoothed_audit_log_likelihood,
                "map_iteration": iteration,
            }
            covariance = evaluate_covariance_acceptance(covariance_evidence)
        except (StateModelSetError, ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
            audit_unavailable = getattr(exc, "stage", None) == "smoothed_posterior_audit" and bool(
                (getattr(exc, "evidence", {}) or {}).get("error_type")
            )
            raise _MapJointFitFailure(
                "covariance",
                "hmm_risk_model_covariance_invalid",
                str(exc),
                evidence={
                    "iteration": iteration,
                    "completed_stages": ["initialization", "fit", "raw_covariance_capture", "map_objective"],
                    "raw_covariance_evidence": raw_capture,
                    **dict(getattr(exc, "evidence", {}) or {}),
                    "d4_derived_evidence_status": (
                        "not_computable_posterior_audit_unavailable"
                        if audit_unavailable
                        else "not_computable_posterior_audit_invalid"
                    ),
                    "covariance_status": "insufficient_evidence" if audit_unavailable else "failed",
                    "covariance_valid": False,
                    "state_posterior_mass": None,
                    "posterior_weighted_variance_about_weighted_mean": None,
                    "posterior_second_moment_about_fitted_mean": None,
                    "mstep_expected_covariance": None,
                    "dynamic_lower_reference": None,
                    "dynamic_upper_reference": None,
                    "mstep_relative_residual": None,
                },
            ) from exc

        raw_history.append(float(objective["raw_log_likelihood"]))
        map_history.append(float(objective["map_objective"]))
        prior_history.append(float(objective["prior_adjustment"]))
        component_history.append({"iteration": iteration, **objective})
        covariance_valid_history.append(covariance.get("covariance_valid") is True)
        covariance_hash_history.append(str(covariance["receipt_sha256"]))
        latest_covariance = covariance
        latest_covariance_evidence = covariance_evidence
        latest_raw_capture = raw_capture
        latest_raw_covars = raw_covars

        joint_stop = False
        if len(map_history) >= 2:
            map_delta = map_history[-1] - map_history[-2]
            envelope = map_numeric_envelope(map_history[-2])
            if map_delta < -envelope:
                loop_evidence = _map_joint_loop_evidence(
                    raw_likelihood_history=raw_history,
                    map_objective_history=map_history,
                    map_prior_adjustment_history=prior_history,
                    objective_component_history=component_history,
                    covariance_valid_history=covariance_valid_history,
                    covariance_receipt_sha256_history=covariance_hash_history,
                    joint_stop_iteration=None,
                )
                likelihood = _evaluate_map_loop_or_fail(
                    loop_evidence, iteration=iteration, raw_covariance_evidence=raw_capture
                )
                raise _MapJointFitFailure(
                    "likelihood",
                    "hmm_risk_model_map_objective_decrease",
                    "MAP objective decreased beyond the approved numeric envelope",
                    evidence={
                        "iteration": iteration,
                        "completed_stages": [
                            "initialization",
                            "fit",
                            "raw_covariance_capture",
                            "map_objective",
                            "covariance",
                        ],
                        "likelihood": likelihood,
                        "raw_covariance_evidence": raw_capture,
                        "covariance_stage_evidence": {**covariance_evidence, "acceptance": dict(covariance)},
                    },
                )
            joint_stop = abs(map_delta) <= envelope and covariance.get("covariance_valid") is True

        if joint_stop:
            loop_evidence = _map_joint_loop_evidence(
                raw_likelihood_history=raw_history,
                map_objective_history=map_history,
                map_prior_adjustment_history=prior_history,
                objective_component_history=component_history,
                covariance_valid_history=covariance_valid_history,
                covariance_receipt_sha256_history=covariance_hash_history,
                joint_stop_iteration=iteration,
            )
            likelihood = _evaluate_map_loop_or_fail(
                loop_evidence, iteration=iteration, raw_covariance_evidence=raw_capture
            )
            if likelihood.get("convergence_valid") is not True or likelihood.get("likelihood_valid") is not True:
                raise _MapJointFitFailure(
                    "likelihood",
                    str(likelihood.get("primary_reason_code") or "hmm_risk_model_map_joint_convergence_unavailable"),
                    "MAP joint-stop receipt is not accepted",
                    evidence={
                        "iteration": iteration,
                        "completed_stages": [
                            "initialization",
                            "fit",
                            "raw_covariance_capture",
                            "map_objective",
                            "covariance",
                        ],
                        "likelihood": likelihood,
                        "raw_covariance_evidence": raw_capture,
                        "covariance_stage_evidence": {**covariance_evidence, "acceptance": dict(covariance)},
                    },
                )
            assert latest_covariance is not None
            assert latest_covariance_evidence is not None
            assert latest_raw_capture is not None
            assert latest_raw_covars is not None
            return _MapJointFitResult(
                likelihood=likelihood,
                covariance=latest_covariance,
                covariance_evidence=latest_covariance_evidence,
                raw_covariance_evidence=latest_raw_capture,
                raw_covars=latest_raw_covars,
                terminal_raw_likelihood=raw_history[-1],
            )

        if iteration < HMM_N_ITER:
            try:
                model._do_mstep(stats)
            except (ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
                raise _MapJointFitFailure(
                    "fit",
                    "hmm_risk_model_fit_failed",
                    str(exc),
                    evidence={"error_type": type(exc).__name__, "error": str(exc), "iteration": iteration},
                ) from exc

    loop_evidence = _map_joint_loop_evidence(
        raw_likelihood_history=raw_history,
        map_objective_history=map_history,
        map_prior_adjustment_history=prior_history,
        objective_component_history=component_history,
        covariance_valid_history=covariance_valid_history,
        covariance_receipt_sha256_history=covariance_hash_history,
        joint_stop_iteration=None,
    )
    likelihood = _evaluate_map_loop_or_fail(
        loop_evidence,
        iteration=HMM_N_ITER,
        raw_covariance_evidence=latest_raw_capture,
    )
    raise _MapJointFitFailure(
        "likelihood",
        "hmm_risk_model_map_joint_convergence_unavailable",
        "MAP objective and D4-02-A did not jointly converge within 300 iterations",
        evidence={
            "completed_stages": ["initialization", "fit", "raw_covariance_capture", "map_objective", "covariance"],
            "likelihood": likelihood,
            "covariance": latest_covariance,
        },
    )


def fit_b3_preprocessed_train_only(
    item: B3TrainOnlySeries,
    *,
    train: np.ndarray,
    seed: int,
) -> B3CoreFitEvidence:
    """Fit one already-preprocessed train matrix without artifact or selection semantics."""

    try:
        from hmmlearn.hmm import GaussianHMM
    except ImportError as exc:  # pragma: no cover - dependency gate is explicit.
        raise StateModelSetError("hmmlearn==0.3.3 is required for formal B3 training") from exc
    try:
        prepared_initialization = _prepare_b3_preprocessed_train_only_initialization(item, train=train, seed=seed)
    except (StateModelSetError, ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        stage_evidence = _training_stage_evidence(
            fit_invoked=False,
            fit_returned=False,
            completed_stages=[],
            initialization={},
            stage_specific_cause_evidence={
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        raise B3TrainingStageError(
            "initialization",
            "hmm_risk_model_initialization_failed",
            exc,
            stage_evidence=stage_evidence,
        ) from exc
    prepared = prepared_initialization.train
    reference = prepared_initialization.reference
    startprob = prepared_initialization.startprob
    transmat = prepared_initialization.transmat
    means = prepared_initialization.means
    initialized_covars = prepared_initialization.covars
    prior = prepared_initialization.prior
    initialization = dict(prepared_initialization.evidence)
    fit_invoked = False
    try:
        model = GaussianHMM(
            n_components=3,
            covariance_type="diag",
            min_covar=0.0,
            startprob_prior=1.0,
            transmat_prior=1.0,
            means_prior=0.0,
            means_weight=0.0,
            covars_prior=prior,
            covars_weight=C008_B3_DIAG04_NU + 1.0,
            algorithm="viterbi",
            random_state=seed,
            n_iter=HMM_N_ITER,
            tol=0.01,
            verbose=False,
            params="stmc",
            init_params="",
            implementation="log",
        )
        model.startprob_ = startprob.copy()
        model.transmat_ = transmat.copy()
        model.means_ = means.copy()
        model.covars_ = initialized_covars.copy()
        fit_invoked = True
        map_fit = _run_b3_map_joint_em(model, prepared, reference)
    except _MapJointFitFailure as exc:
        stage_evidence = _training_stage_evidence(
            fit_invoked=fit_invoked,
            fit_returned=False,
            completed_stages=list(exc.evidence.get("completed_stages") or ["initialization"]),
            initialization=initialization,
            likelihood=(
                exc.evidence.get("likelihood") if isinstance(exc.evidence.get("likelihood"), Mapping) else None
            ),
            raw_covariance_evidence=(
                exc.evidence.get("raw_covariance_evidence")
                if isinstance(exc.evidence.get("raw_covariance_evidence"), Mapping)
                else None
            ),
            covariance_evidence=(
                exc.evidence.get("covariance_stage_evidence")
                if isinstance(exc.evidence.get("covariance_stage_evidence"), Mapping)
                else None
            ),
            stage_specific_cause_evidence=exc.evidence,
        )
        raise B3TrainingStageError(
            exc.stage,
            exc.reason_code,
            exc,
            stage_evidence=stage_evidence,
        ) from exc
    except (ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        stage_evidence = _training_stage_evidence(
            fit_invoked=fit_invoked,
            fit_returned=False,
            completed_stages=["initialization"],
            initialization=initialization,
            stage_specific_cause_evidence={
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        raise B3TrainingStageError(
            "fit",
            "hmm_risk_model_fit_failed",
            exc,
            stage_evidence=stage_evidence,
        ) from exc
    try:
        raw_covariance_source = map_fit.raw_covars
    except AttributeError as exc:
        raw_covariance_evidence = capture_raw_diag_covariance_evidence(
            None,
            expected_shape=(3, prepared.shape[1]),
            evidence_unavailable_reason="gaussian_hmm_internal_diag_covars_missing",
        )
        cause_evidence = {
            "d4_derived_evidence_status": "not_computable_raw_covariance_invalid",
            "covariance_status": "failed",
            "covariance_valid": False,
            "evidence_unavailable_reason": "gaussian_hmm_internal_diag_covars_missing",
        }
        stage_evidence = _training_stage_evidence(
            fit_invoked=True,
            fit_returned=True,
            completed_stages=["initialization", "fit"],
            initialization=initialization,
            raw_covariance_evidence=raw_covariance_evidence,
            stage_specific_cause_evidence=cause_evidence,
        )
        raise B3TrainingStageError(
            "covariance",
            "hmm_risk_model_covariance_raw_type_invalid",
            exc,
            stage_evidence=stage_evidence,
        ) from exc
    raw_covariance_evidence = capture_raw_diag_covariance_evidence(
        raw_covariance_source,
        expected_shape=(3, prepared.shape[1]),
    )
    monitor_evidence: Mapping[str, Any] = dict(map_fit.likelihood["evidence"])
    likelihood = dict(map_fit.likelihood)
    raw_covars = np.asarray(raw_covariance_source, dtype=np.float64)
    covariance_evidence = dict(map_fit.covariance_evidence)
    covariance = dict(map_fit.covariance)
    covariance_stage_evidence = {
        **covariance_evidence,
        "acceptance": dict(covariance),
    }
    try:
        fitted_startprob = _probability_vector(model.startprob_, f"{item.sector_code}.startprob", 3)
        fitted_transmat = _transition_matrix(model.transmat_, f"{item.sector_code}.transmat", 3)
        fitted_means = _finite_array(model.means_, f"{item.sector_code}.means", ndim=2)
        train_posteriors = causal_forward_posteriors(
            prepared,
            startprob=fitted_startprob,
            transmat=fitted_transmat,
            means=fitted_means,
            covars=raw_covars,
        )
        occupancy = evaluate_train_occupancy(
            train_posteriors,
            item.train_dates,
            frozen_input_manifest=item.train_input_manifest,
        )
    except (StateModelSetError, ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        stage_evidence = _training_stage_evidence(
            fit_invoked=True,
            fit_returned=True,
            completed_stages=[
                "initialization",
                "fit",
                "raw_covariance_capture",
                "monitor",
                "likelihood",
                "covariance",
            ],
            initialization=initialization,
            monitor_evidence=monitor_evidence,
            likelihood=likelihood,
            raw_covariance_evidence=raw_covariance_evidence,
            covariance_evidence=covariance_stage_evidence,
            stage_specific_cause_evidence={
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        raise B3TrainingStageError(
            "train_posterior",
            "hmm_risk_model_posterior_invalid",
            exc,
            stage_evidence=stage_evidence,
        ) from exc
    terminal_likelihood = map_fit.terminal_raw_likelihood
    independent_valid = (
        likelihood.get("convergence_valid") is True
        and likelihood.get("likelihood_valid") is True
        and covariance.get("covariance_valid") is True
        and occupancy.get("train_occupancy_valid") is True
    )
    independent_statuses = {
        str(likelihood.get("monitor_status") or ""),
        str(likelihood.get("likelihood_status") or ""),
        str(covariance.get("covariance_status") or ""),
        str(occupancy.get("train_occupancy_status") or ""),
    }
    if independent_valid:
        model_entry_status = "accepted"
    elif "insufficient_evidence" in independent_statuses:
        model_entry_status = "insufficient_evidence"
    else:
        model_entry_status = "failed"
    training_stage_evidence = _training_stage_evidence(
        fit_invoked=True,
        fit_returned=True,
        completed_stages=[
            "initialization",
            "fit",
            "raw_covariance_capture",
            "monitor",
            "likelihood",
            "covariance",
            "train_posterior",
        ],
        initialization=initialization,
        monitor_evidence=monitor_evidence,
        likelihood=likelihood,
        raw_covariance_evidence=raw_covariance_evidence,
        covariance_evidence=covariance_stage_evidence,
    )
    return B3CoreFitEvidence(
        initialization=initialization,
        monitor_evidence=monitor_evidence,
        likelihood=likelihood,
        covariance=covariance,
        train_occupancy=occupancy,
        startprob=fitted_startprob,
        transmat=fitted_transmat,
        means=fitted_means,
        covars=raw_covars,
        terminal_likelihood=terminal_likelihood,
        model_entry_status=model_entry_status,
        model_entry_valid=independent_valid,
        training_stage_evidence=training_stage_evidence,
    )


def _fit_b3_train_only(
    item: B3TrainOnlySeries,
    *,
    family: str,
    level: str,
    feature_names: tuple[str, ...],
    preprocess: Mapping[str, Any],
    seed: int,
    numeric_environment: Mapping[str, Any],
    dimension_contract_version: str | None = None,
) -> tuple[dict[str, Any], B3FittedModel]:
    """Fit one formal B3 entry without touching validation or future utility."""

    item.validate(len(feature_names))
    try:
        full_train = _apply_preprocess(item.train_observations, preprocess)
    except (StateModelSetError, ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        raise B3TrainingStageError(
            "initialization",
            "hmm_risk_model_initialization_failed",
            exc,
        ) from exc
    projection_receipt: Mapping[str, Any] | None = None
    train = full_train
    mixed_dimension = dimension_contract_version is not None
    if mixed_dimension and (
        dimension_contract_version != MIXED_DIMENSION_CONTRACT_VERSION or not uses_mixed_dimension_level(family, level)
    ):
        raise B3TrainingStageError(
            "projection",
            INACTIVE_DIMENSION_REASON_CODE,
            StateModelSetError(INACTIVE_DIMENSION_REASON_CODE),
        )
    if mixed_dimension:
        try:
            projection_receipt, train = build_projection_receipt(
                family=family,
                level=level,
                sector_code=item.sector_code,
                full_feature_names=feature_names,
                preprocess=preprocess,
                raw_observations=item.train_observations,
                preprocessed_observations=full_train,
                train_input_manifest=item.train_input_manifest,
            )
        except (StateModelSetError, ValueError, FloatingPointError) as exc:
            raise B3TrainingStageError(
                "projection",
                INACTIVE_DIMENSION_REASON_CODE,
                exc,
            ) from exc
    core = fit_b3_preprocessed_train_only(item, train=train, seed=seed)
    fitted_startprob = core.startprob
    fitted_transmat = core.transmat
    fitted_means = core.means
    raw_covars = core.covars
    model_body = {
        "schema_version": (
            MIXED_MODEL_SCHEMA_VERSION if projection_receipt is not None else "hmm_risk_b3_fitted_model_v1"
        ),
        "contract_version": D3_CONTRACT_VERSION,
        "family": family,
        "level": level,
        "seed": seed,
        "sector_code": item.sector_code,
        "feature_names": list(feature_names),
        "preprocess": dict(preprocess),
        "startprob": fitted_startprob.tolist(),
        "transmat": fitted_transmat.tolist(),
        "means": fitted_means.tolist(),
        "covariance_type": "diag",
        "covars": raw_covars.tolist(),
        "parameter_profile_sha256": canonical_sha256(formal_b3_parameter_profile()),
        "numeric_environment_sha256": canonical_sha256(dict(numeric_environment)),
        "observation_manifest_hash": item.observation_manifest_hash,
        "pit_constituent_manifest_hash": item.pit_constituent_manifest_hash,
    }
    if projection_receipt is not None:
        model_body.update(
            {
                "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                "feature_count": len(feature_names),
                "likelihood_feature_names": list(projection_receipt["active_feature_names"]),
                "likelihood_feature_count": projection_receipt["likelihood_feature_count"],
                "projection_receipt": dict(projection_receipt),
                "projection_sha256": projection_receipt["projection_sha256"],
            }
        )
    model_hash = canonical_sha256(model_body)
    fitted = B3FittedModel(
        family=family,
        level=level,
        seed=seed,
        sector_code=item.sector_code,
        feature_names=feature_names,
        preprocess=dict(preprocess),
        startprob=fitted_startprob,
        transmat=fitted_transmat,
        means=fitted_means,
        covars=raw_covars,
        parameter_profile_sha256=model_body["parameter_profile_sha256"],
        numeric_environment_sha256=model_body["numeric_environment_sha256"],
        observation_manifest_hash=item.observation_manifest_hash,
        pit_constituent_manifest_hash=item.pit_constituent_manifest_hash,
        model_payload_sha256=model_hash,
        projection_receipt=projection_receipt,
    )
    entry_body = {
        "schema_version": (
            MIXED_TRAINING_ENTRY_SCHEMA_VERSION
            if projection_receipt is not None
            else "hmm_risk_b3_training_entry_receipt_v1"
        ),
        "contract_version": D3_CONTRACT_VERSION,
        "retrain_contract_version": L2_RETRAIN_VERSION if level == "L2" else None,
        "family": family,
        "level": level,
        "seed": seed,
        "sector_code": item.sector_code,
        "feature_count": len(feature_names),
        "training_rows": int(train.shape[0]),
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
        "final_train_log_likelihood_source": "map_joint_stop_raw_observed_log_likelihood",
        "model_payload_sha256": model_hash,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "artifact_write_performed": False,
        "postfit_projection_performed": False,
    }
    if projection_receipt is not None:
        entry_body.update(
            {
                "likelihood_feature_count": int(train.shape[1]),
                "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                "projection_receipt": dict(projection_receipt),
                "projection_sha256": projection_receipt["projection_sha256"],
            }
        )
    return {**entry_body, "entry_receipt_sha256": canonical_sha256(entry_body)}, fitted


def fit_b3_target_entry(
    item: B3TrainOnlySeries,
    *,
    family: str,
    level: str,
    feature_names: Sequence[str],
    preprocess: Mapping[str, Any],
    seed: int,
    numeric_environment: Mapping[str, Any],
) -> tuple[dict[str, Any], B3FittedModel]:
    """Fit one approved B3 identity for a bounded diagnostic without selection or writes."""

    features = tuple(str(value) for value in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("B3 target feature_names must match the approved 7/20 dimensional family")
    if seed not in RESTART_SCHEDULE:
        raise StateModelSetError("B3 target seed is outside the approved restart schedule")
    return _fit_b3_train_only(
        item,
        family=family,
        level=level,
        feature_names=features,
        preprocess=preprocess,
        seed=seed,
        numeric_environment=numeric_environment,
    )


def run_level_repeat(
    series: Mapping[str, B3TrainOnlySeries],
    *,
    family: str,
    level: str,
    feature_names: Sequence[str],
    preprocess_family: str,
    process_identity: str,
) -> tuple[dict[str, Any], dict[tuple[int, str], B3FittedModel]]:
    """Run the complete 8-seed level grid; failures are retained and never trigger early stop."""

    features = tuple(str(value) for value in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("formal B3 feature_names must be the approved 7/20 dimensional family")
    expected_count = 31 if level == "L1" else 131 if level == "L2" else 0
    codes = tuple(sorted(series))
    if expected_count == 0 or len(codes) != expected_count or len(set(codes)) != expected_count:
        raise StateModelSetError(f"formal B3 {level} requires exactly {expected_count} canonical sectors")
    for item in series.values():
        item.validate(len(features))
    environment = c008_b3_diag04_fixed_numeric_environment()
    package_version = str(environment.get("packages", {}).get("hmmlearn") or "")
    if package_version != "0.3.3":
        raise StateModelSetError(f"formal B3 requires hmmlearn==0.3.3 actual={package_version}")
    preprocess = _fit_preprocess(series, preprocess_family=preprocess_family)
    mixed_dimension = uses_mixed_dimension_level(family, level)
    entries: list[dict[str, Any]] = []
    models: dict[tuple[int, str], B3FittedModel] = {}
    for seed in RESTART_SCHEDULE:
        for code in codes:
            item = series[code]
            try:
                entry, fitted = _fit_b3_train_only(
                    item,
                    family=family,
                    level=level,
                    feature_names=features,
                    preprocess=preprocess,
                    seed=seed,
                    numeric_environment=environment,
                    dimension_contract_version=(MIXED_DIMENSION_CONTRACT_VERSION if mixed_dimension else None),
                )
            except B3TrainingStageError as exc:
                failure_body = {
                    "schema_version": (
                        MIXED_TRAINING_ENTRY_SCHEMA_VERSION
                        if mixed_dimension
                        else "hmm_risk_b3_training_entry_receipt_v1"
                    ),
                    "contract_version": D3_CONTRACT_VERSION,
                    "retrain_contract_version": L2_RETRAIN_VERSION if level == "L2" else None,
                    "family": family,
                    "level": level,
                    "seed": seed,
                    "sector_code": code,
                    "feature_count": len(features),
                    "training_rows": int(item.train_observations.shape[0]),
                    "fit_status": "failed",
                    "model_entry_status": "failed",
                    "model_entry_valid": False,
                    "failure_stage": exc.stage,
                    "failure_reason_codes": [exc.reason_code],
                    "failure_type": exc.cause_type,
                    "failure_message": str(exc),
                    "validation_accessed": False,
                    "future_utility_accessed": False,
                    "semantic_labelability_accessed": False,
                    "d6_status_accessed": False,
                    "artifact_write_performed": False,
                }
                if mixed_dimension:
                    failure_body.update(
                        {
                            "likelihood_feature_count": None,
                            "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                        }
                    )
                entries.append({**failure_body, "entry_receipt_sha256": canonical_sha256(failure_body)})
                continue
            entries.append(entry)
            models[(seed, code)] = fitted
    model_payloads = [models[key].payload() for key in sorted(models)]
    candidate_payload = {
        "family": family,
        "level": level,
        "schedule": list(RESTART_SCHEDULE),
        "canonical_sector_codes": list(codes),
        "feature_names": list(features),
        "preprocess": preprocess,
        "numeric_environment": environment,
        "entries": entries,
        "models": model_payloads,
    }
    if mixed_dimension:
        candidate_payload["dimension_contract_version"] = MIXED_DIMENSION_CONTRACT_VERSION
    payload = {
        "schema_version": (MIXED_REPEAT_SCHEMA_VERSION if mixed_dimension else "hmm_risk_b3_level_repeat_receipt_v1"),
        "contract_version": D3_CONTRACT_VERSION,
        "retrain_contract_version": L2_RETRAIN_VERSION if level == "L2" else None,
        "process_identity": process_identity,
        "family": family,
        "level": level,
        "schedule": list(RESTART_SCHEDULE),
        "canonical_sector_codes": list(codes),
        "canonical_sector_set_sha256": canonical_sha256(list(codes)),
        "feature_names": list(features),
        "preprocess": preprocess,
        "numeric_environment": environment,
        "entries": entries,
        "models": model_payloads,
        "entry_count": len(entries),
        "expected_entry_count": len(RESTART_SCHEDULE) * expected_count,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "selection_performed": False,
        "artifact_write_performed": False,
    }
    if mixed_dimension:
        payload.update(
            {
                "feature_count": len(features),
                "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
            }
        )
    return {
        **payload,
        "entry_payload_sha256": canonical_sha256(entries),
        "model_payload_sha256": canonical_sha256(model_payloads),
        "candidate_payload_sha256": canonical_sha256(candidate_payload),
        "repeat_receipt_sha256": canonical_sha256(payload),
    }, models


def models_from_repeat(repeat: Mapping[str, Any]) -> dict[tuple[int, str], B3FittedModel]:
    models: dict[tuple[int, str], B3FittedModel] = {}
    repeat_family = str(repeat.get("family") or "")
    repeat_level = str(repeat.get("level") or "")
    expected_codes = tuple(str(value) for value in repeat.get("canonical_sector_codes") or ())
    expected_features = tuple(str(value) for value in repeat.get("feature_names") or ())
    mixed_dimension = uses_mixed_dimension_level(repeat_family, repeat_level)
    if mixed_dimension:
        if (
            repeat.get("schema_version") != MIXED_REPEAT_SCHEMA_VERSION
            or repeat.get("dimension_contract_version") != MIXED_DIMENSION_CONTRACT_VERSION
            or expected_features != ALL_CORE_FEATURES
            or repeat.get("feature_count") != len(ALL_CORE_FEATURES)
        ):
            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
    for raw in repeat.get("models") or ():
        feature_names = tuple(str(value) for value in raw.get("feature_names") or ())
        startprob = _probability_vector(raw.get("startprob"), "repeat.startprob", 3)
        transmat = _transition_matrix(raw.get("transmat"), "repeat.transmat", 3)
        means = _finite_array(raw.get("means"), "repeat.means", ndim=2)
        covars = _finite_array(raw.get("covars"), "repeat.covars", ndim=2)
        projection_receipt = raw.get("projection_receipt")
        effective_count = len(feature_names)
        if mixed_dimension:
            if not isinstance(projection_receipt, Mapping):
                raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
            effective_count = validate_projection_receipt(
                projection_receipt,
                family=repeat_family,
                level=repeat_level,
                sector_code=str(raw.get("sector_code") or ""),
                full_feature_names=feature_names,
                preprocess=raw.get("preprocess") or {},
                means_shape=means.shape,
                covariance_shape=covars.shape,
            )
            if (
                raw.get("schema_version") != MIXED_MODEL_SCHEMA_VERSION
                or raw.get("dimension_contract_version") != MIXED_DIMENSION_CONTRACT_VERSION
                or raw.get("feature_count") != len(feature_names)
                or raw.get("likelihood_feature_count") != effective_count
                or tuple(raw.get("likelihood_feature_names") or ()) != tuple(projection_receipt["active_feature_names"])
                or raw.get("projection_sha256") != projection_receipt["projection_sha256"]
            ):
                raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
        if means.shape != covars.shape or means.shape != (3, effective_count) or np.any(covars <= 0.0):
            raise StateModelSetError("repeat model parameter shape is invalid")
        expected_hash = str(raw.get("model_payload_sha256") or "")
        body = {key: value for key, value in raw.items() if key != "model_payload_sha256"}
        if canonical_sha256(body) != expected_hash:
            raise StateModelSetError("repeat model payload hash mismatch")
        fitted = B3FittedModel(
            family=str(raw.get("family") or ""),
            level=str(raw.get("level") or ""),
            seed=int(raw.get("seed")),
            sector_code=str(raw.get("sector_code") or ""),
            feature_names=feature_names,
            preprocess=dict(raw.get("preprocess") or {}),
            startprob=startprob,
            transmat=transmat,
            means=means,
            covars=covars,
            parameter_profile_sha256=str(raw.get("parameter_profile_sha256") or ""),
            numeric_environment_sha256=str(raw.get("numeric_environment_sha256") or ""),
            observation_manifest_hash=str(raw.get("observation_manifest_hash") or ""),
            pit_constituent_manifest_hash=str(raw.get("pit_constituent_manifest_hash") or ""),
            model_payload_sha256=expected_hash,
            projection_receipt=(dict(projection_receipt) if isinstance(projection_receipt, Mapping) else None),
        )
        if (
            fitted.family != repeat_family
            or fitted.level != repeat_level
            or fitted.seed not in RESTART_SCHEDULE
            or fitted.sector_code not in expected_codes
            or fitted.feature_names != expected_features
        ):
            raise StateModelSetError("repeat model identity differs from its level receipt")
        key = (fitted.seed, fitted.sector_code)
        if key in models:
            raise StateModelSetError("repeat contains duplicate model identity")
        models[key] = fitted
    if canonical_sha256([models[key].payload() for key in sorted(models)]) != repeat.get("model_payload_sha256"):
        raise StateModelSetError("repeat aggregate model hash mismatch")
    return models


def build_selected_level_artifact(
    selection: Mapping[str, Any],
    models: Mapping[tuple[int, str], B3FittedModel],
    series: Mapping[str, L1TrainingSeries],
    training_repeat: Mapping[str, Any],
) -> dict[str, Any]:
    """Run D6 only for the frozen D5 selection; semantic failure never reselects another seed."""

    if selection.get("level_selection_valid") is not True:
        raise StateModelSetError("D6 cannot run before an accepted D5 level selection")
    selection_evidence = selection.get("evidence")
    if not isinstance(selection_evidence, Mapping):
        raise StateModelSetError("D6 cannot run without D5 selection evidence")
    expected_codes = tuple(sorted(str(code) for code in series))
    if (
        training_repeat.get("family") != selection_evidence.get("family")
        or training_repeat.get("level") != selection_evidence.get("level")
        or tuple(training_repeat.get("schedule") or ()) != RESTART_SCHEDULE
        or tuple(training_repeat.get("canonical_sector_codes") or ()) != expected_codes
        or tuple(selection_evidence.get("canonical_sector_codes") or ()) != expected_codes
    ):
        raise StateModelSetError("D6 frozen D5/training identity is inconsistent")
    selected_seed = selection_evidence.get("selected_seed")
    if selected_seed not in RESTART_SCHEDULE:
        raise StateModelSetError("D5 selected seed is missing from the approved schedule")
    _require_canonical_receipt_hash(selection, field="receipt_sha256", label="D5 selection")
    repeat_entries = list(training_repeat.get("entries") or ())
    entries: list[dict[str, Any]] = []
    for code in sorted(series):
        fitted = models.get((int(selected_seed), code))
        if fitted is None:
            raise StateModelSetError(f"selected fitted model is missing for {code}")
        item = series[code]
        item.validate(len(fitted.feature_names))
        matching_receipts = [
            receipt
            for receipt in repeat_entries
            if receipt.get("seed") == selected_seed and receipt.get("sector_code") == code
        ]
        if len(matching_receipts) != 1:
            raise StateModelSetError(f"selected training receipt is missing or duplicated for {code}")
        training_receipt = matching_receipts[0]
        _require_canonical_receipt_hash(
            training_receipt,
            field="entry_receipt_sha256",
            label=f"selected training receipt {code}",
        )
        if (
            training_receipt.get("model_entry_status") != "accepted"
            or training_receipt.get("model_entry_valid") is not True
            or training_receipt.get("model_payload_sha256") != fitted.model_payload_sha256
        ):
            raise StateModelSetError(f"selected training receipt is not accepted for {code}")
        validation = _apply_preprocess(item.validation_observations, fitted.preprocess)
        if fitted.projection_receipt is not None:
            validate_projection_receipt(
                fitted.projection_receipt,
                family=fitted.family,
                level=fitted.level,
                sector_code=fitted.sector_code,
                full_feature_names=fitted.feature_names,
                preprocess=fitted.preprocess,
                means_shape=fitted.means.shape,
                covariance_shape=fitted.covars.shape,
            )
            validation = np.ascontiguousarray(
                validation[:, tuple(fitted.projection_receipt["active_feature_indices"])],
                dtype=np.float64,
            )
        posterior = causal_forward_posteriors(
            validation,
            startprob=fitted.startprob,
            transmat=fitted.transmat,
            means=fitted.means,
            covars=fitted.covars,
        )
        semantic = evaluate_semantic_validation(
            posterior,
            item.validation_dates,
            {
                **item.validation_future_components,
                "source_cutoff": (
                    None
                    if item.validation_utility_source_cutoff is None
                    else item.validation_utility_source_cutoff.isoformat()
                ),
                "formula_version": item.validation_utility_formula_version,
            },
            frozen_input_manifest=item.validation_input_manifest,
            selected_model_payload_sha256=fitted.model_payload_sha256,
        )
        entry_body = {
            **fitted.payload(),
            "training_receipt": training_receipt,
            "semantic": semantic,
            "validation_accessed_after_selection": True,
            "future_utility_accessed_after_selection": True,
            "selection_reexecuted": False,
            "semantic_mapping": semantic.get("semantic_mapping"),
        }
        entries.append({**entry_body, "selected_entry_sha256": canonical_sha256(entry_body)})
    valid = all(
        entry["semantic"]["assignment"]["semantic_assignment_valid"]
        and entry["semantic"]["semantic_evidence"]["semantic_evidence_valid"]
        for entry in entries
    )
    mixed_dimension = uses_mixed_dimension_level(
        str(selection.get("evidence", {}).get("family") or ""),
        str(selection.get("evidence", {}).get("level") or ""),
    )
    dimension_identity = (
        build_level_dimension_identity(
            entries,
            family=str(selection.get("evidence", {}).get("family") or ""),
            level=str(selection.get("evidence", {}).get("level") or ""),
            expected_sector_codes=expected_codes,
        )
        if mixed_dimension
        else None
    )
    body = {
        "schema_version": (MIXED_LEVEL_SCHEMA_VERSION if mixed_dimension else "hmm_risk_b3_selected_level_artifact_v1"),
        "family": selection.get("evidence", {}).get("family"),
        "level": selection.get("evidence", {}).get("level"),
        "selected_seed": selected_seed,
        "selection_receipt_sha256": selection.get("receipt_sha256"),
        "status": "accepted" if valid else "blocked",
        "entry_count": len(entries),
        "entries": entries,
        "selection_reexecuted": False,
        "ready": False,
    }
    if dimension_identity is not None:
        body.update(dimension_identity)
    return {**body, "artifact_sha256": canonical_sha256(body)}


def _require_canonical_receipt_hash(receipt: Mapping[str, Any], *, field: str, label: str) -> None:
    expected = str(receipt.get(field) or "")
    body = {key: value for key, value in receipt.items() if key != field}
    if len(expected) != 64 or canonical_sha256(body) != expected:
        raise StateModelSetError(f"{label} canonical receipt hash mismatch")


def _require_hex_identity(value: str, *, length: int, label: str) -> None:
    if len(value) != length or any(character not in "0123456789abcdef" for character in value.lower()):
        raise StateModelSetError(f"{label} identity is invalid")


def _validate_ready_layer(
    artifact: Mapping[str, Any],
    selection: Mapping[str, Any],
    *,
    family: str,
    level: str,
    expected_count: int,
    dataset_manifest_hash: str,
    mapping_manifest_hash: str,
    calendar_manifest_hash: str,
    l2_stock_fact_manifest_hash: str,
    semantic_dataset_manifest_hash: str,
    semantic_mapping_manifest_hash: str,
    semantic_calendar_manifest_hash: str,
    semantic_l2_stock_fact_manifest_hash: str,
    feature_domain_policy_sha256: str,
) -> None:
    mixed_dimension = uses_mixed_dimension_level(family, level)
    expected_artifact_schema = (
        MIXED_LEVEL_SCHEMA_VERSION if mixed_dimension else "hmm_risk_b3_selected_level_artifact_v1"
    )
    if artifact.get("schema_version") != expected_artifact_schema:
        raise StateModelSetError(f"B3 READY selected artifact schema is invalid for {family}/{level}")
    if artifact.get("family") != family or artifact.get("level") != level:
        raise StateModelSetError(f"B3 READY selected artifact identity is invalid for {family}/{level}")
    _require_canonical_receipt_hash(artifact, field="artifact_sha256", label=f"{family}/{level} artifact")
    _require_canonical_receipt_hash(selection, field="receipt_sha256", label=f"{family}/{level} selection")
    evidence = selection.get("evidence")
    selected_seed = artifact.get("selected_seed")
    if not isinstance(evidence, Mapping):
        raise StateModelSetError(f"B3 READY selection evidence is missing for {family}/{level}")
    if (
        selection.get("contract_version") != D5_SELECTION_VERSION
        or selection.get("level_selection_status") != "accepted"
        or selection.get("level_selection_valid") is not True
        or selection.get("failure_reason_codes") != []
        or selection.get("blocking_reason_codes") != []
        or evidence.get("family") != family
        or evidence.get("level") != level
        or evidence.get("selected_seed") != selected_seed
        or evidence.get("feature_domain_policy_sha256") != feature_domain_policy_sha256
        or selected_seed not in RESTART_SCHEDULE
        or artifact.get("selection_receipt_sha256") != selection.get("receipt_sha256")
        or (mixed_dimension and evidence.get("dimension_contract_version") != MIXED_DIMENSION_CONTRACT_VERSION)
    ):
        raise StateModelSetError(f"B3 READY selection contract is invalid for {family}/{level}")
    canonical_codes = tuple(str(value) for value in evidence.get("canonical_sector_codes") or ())
    candidates = list(evidence.get("candidates") or ())
    selected_candidates = [candidate for candidate in candidates if candidate.get("seed") == selected_seed]
    expected_feature_count = len(BASE_FEATURES) if family == "legacy_covfix" else len(ALL_CORE_FEATURES)
    expected_features = BASE_FEATURES if family == "legacy_covfix" else ALL_CORE_FEATURES
    if (
        len(canonical_codes) != expected_count
        or tuple(sorted(set(canonical_codes))) != canonical_codes
        or evidence.get("canonical_sector_set_sha256") != canonical_sha256(list(canonical_codes))
        or tuple(evidence.get("schedule") or ()) != RESTART_SCHEDULE
        or len(str(evidence.get("repeat_entries_sha256") or "")) != 64
        or len(candidates) != len(RESTART_SCHEDULE)
        or len(selected_candidates) != 1
        or selected_candidates[0].get("eligible") is not True
        or selected_candidates[0].get("schedule_index") != evidence.get("selected_schedule_index")
        or evidence.get("feature_count") != expected_feature_count
        or evidence.get("validation_accessed") is not False
        or evidence.get("future_utility_accessed") is not False
        or evidence.get("semantic_labelability_accessed") is not False
        or evidence.get("d6_status_accessed") is not False
        or evidence.get("selection_followed_by_refit") is not False
        or len(evidence.get("lexicographic_filters") or ()) != 3
        or len(selected_candidates[0].get("entry_receipt_hashes") or ()) != expected_count
        or any(len(str(value or "")) != 64 for value in selected_candidates[0].get("entry_receipt_hashes") or ())
    ):
        raise StateModelSetError(f"B3 READY selection evidence is incomplete for {family}/{level}")
    selected_receipt_hashes = tuple(str(value) for value in selected_candidates[0]["entry_receipt_hashes"])
    entries = list(artifact.get("entries") or ())
    if (
        artifact.get("status") != "accepted"
        or artifact.get("entry_count") != expected_count
        or artifact.get("selection_reexecuted") is not False
        or artifact.get("ready") is not False
    ):
        raise StateModelSetError(f"B3 READY blocked by incomplete semantic evidence for {family}/{level}")
    if len(entries) != expected_count:
        raise StateModelSetError(f"B3 READY selected entry count is invalid for {family}/{level}")
    codes: set[str] = set()
    durable_training_receipt_hashes: dict[str, str] = {}
    durable_score_inputs: dict[str, dict[str, Any]] = {}
    model_keys = (
        "schema_version",
        "contract_version",
        "family",
        "level",
        "seed",
        "sector_code",
        "feature_names",
        "preprocess",
        "startprob",
        "transmat",
        "means",
        "covariance_type",
        "covars",
        "parameter_profile_sha256",
        "numeric_environment_sha256",
        "observation_manifest_hash",
        "pit_constituent_manifest_hash",
    )
    mixed_model_keys = (
        "dimension_contract_version",
        "feature_count",
        "likelihood_feature_names",
        "likelihood_feature_count",
        "projection_receipt",
        "projection_sha256",
    )
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise StateModelSetError(f"B3 READY selected entry is invalid for {family}/{level}")
        _require_canonical_receipt_hash(entry, field="selected_entry_sha256", label=f"{family}/{level} entry")
        required_model_keys = model_keys + mixed_model_keys if mixed_dimension else model_keys
        if any(key not in entry for key in required_model_keys):
            raise StateModelSetError(f"B3 READY model payload is incomplete for {family}/{level}")
        model_body = {key: entry[key] for key in required_model_keys}
        if canonical_sha256(model_body) != entry.get("model_payload_sha256"):
            raise StateModelSetError(f"B3 READY model payload hash mismatch for {family}/{level}")
        try:
            features = tuple(str(value) for value in entry["feature_names"])
            startprob = np.asarray(entry["startprob"], dtype=np.float64)
            transmat = np.asarray(entry["transmat"], dtype=np.float64)
            means = np.asarray(entry["means"], dtype=np.float64)
            covars = np.asarray(entry["covars"], dtype=np.float64)
        except (TypeError, ValueError):
            raise StateModelSetError(f"B3 READY model parameters are invalid for {family}/{level}") from None
        expected_preprocess = "identity" if family == "legacy_covfix" else "winsor_zscore_1_99_train_global_v1"
        preprocess = entry.get("preprocess")
        effective_feature_count = expected_feature_count
        if mixed_dimension:
            projection = entry.get("projection_receipt")
            if not isinstance(projection, Mapping):
                raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
            effective_feature_count = validate_projection_receipt(
                projection,
                family=family,
                level=level,
                sector_code=str(entry.get("sector_code") or ""),
                full_feature_names=features,
                preprocess=preprocess,
                means_shape=means.shape,
                covariance_shape=covars.shape,
            )
        if (
            entry.get("schema_version")
            != (MIXED_MODEL_SCHEMA_VERSION if mixed_dimension else "hmm_risk_b3_fitted_model_v1")
            or entry.get("contract_version") != D3_CONTRACT_VERSION
            or features != expected_features
            or not isinstance(preprocess, Mapping)
            or preprocess.get("family") != expected_preprocess
            or entry.get("covariance_type") != "diag"
            or startprob.shape != (3,)
            or transmat.shape != (3, 3)
            or means.shape != (3, effective_feature_count)
            or covars.shape != (3, effective_feature_count)
            or not all(np.isfinite(value).all() for value in (startprob, transmat, means, covars))
            or np.any(startprob < 0.0)
            or not np.isclose(startprob.sum(), 1.0, atol=1e-12, rtol=0)
            or np.any(transmat < 0.0)
            or not np.allclose(transmat.sum(axis=1), 1.0, atol=1e-12, rtol=0)
            or np.any(covars <= 0.0)
        ):
            raise StateModelSetError(f"B3 READY model parameter contract is invalid for {family}/{level}")
        if mixed_dimension and (
            entry.get("dimension_contract_version") != MIXED_DIMENSION_CONTRACT_VERSION
            or entry.get("feature_count") != expected_feature_count
            or entry.get("likelihood_feature_count") != effective_feature_count
            or tuple(entry.get("likelihood_feature_names") or ())
            != tuple(entry["projection_receipt"]["active_feature_names"])
            or entry.get("projection_sha256") != entry["projection_receipt"]["projection_sha256"]
        ):
            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
        if mixed_dimension:
            projection_sources = entry["projection_receipt"].get("source_identities", {})
            if projection_sources != {
                "dataset_manifest_hash": dataset_manifest_hash,
                "mapping_manifest_hash": mapping_manifest_hash,
                "calendar_manifest_hash": calendar_manifest_hash,
                "l2_stock_fact_manifest_hash": l2_stock_fact_manifest_hash,
                "feature_domain_policy_sha256": feature_domain_policy_sha256,
                "formula_version": C010_FORMULA_VERSION,
            }:
                raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
        if family == "legacy_covfix":
            if any(preprocess.get(field) is not None for field in ("winsor_low", "winsor_high", "center", "scale")):
                raise StateModelSetError(f"B3 READY legacy preprocess contract is invalid for {family}/{level}")
        else:
            try:
                low = np.asarray(preprocess["winsor_low"], dtype=np.float64)
                high = np.asarray(preprocess["winsor_high"], dtype=np.float64)
                center = np.asarray(preprocess["center"], dtype=np.float64)
                scale = np.asarray(preprocess["scale"], dtype=np.float64)
            except (KeyError, TypeError, ValueError):
                raise StateModelSetError(f"B3 READY autocycle preprocess is invalid for {family}/{level}") from None
            if (
                any(value.shape != (expected_feature_count,) for value in (low, high, center, scale))
                or not all(np.isfinite(value).all() for value in (low, high, center, scale))
                or np.any(low > high)
                or np.any(scale <= 0.0)
            ):
                raise StateModelSetError(f"B3 READY autocycle preprocess contract is invalid for {family}/{level}")
        for field in (
            "model_payload_sha256",
            "parameter_profile_sha256",
            "numeric_environment_sha256",
            "observation_manifest_hash",
            "pit_constituent_manifest_hash",
        ):
            _require_hex_identity(str(entry.get(field) or ""), length=64, label=f"{family}/{level}/{field}")
        code = str(entry.get("sector_code") or "")
        training_receipt = entry.get("training_receipt")
        semantic = entry.get("semantic")
        if (
            entry.get("family") != family
            or entry.get("level") != level
            or entry.get("seed") != selected_seed
            or not code
            or code in codes
            or code not in canonical_codes
            or not isinstance(training_receipt, Mapping)
            or not isinstance(semantic, Mapping)
            or semantic.get("contract_version") != D6_SEMANTIC_VERSION
            or entry.get("semantic_mapping") != semantic.get("semantic_mapping")
            or entry.get("validation_accessed_after_selection") is not True
            or entry.get("future_utility_accessed_after_selection") is not True
            or entry.get("selection_reexecuted") is not False
        ):
            raise StateModelSetError(f"B3 READY selected entry identity is invalid for {family}/{level}")
        _require_canonical_receipt_hash(
            training_receipt,
            field="entry_receipt_sha256",
            label=f"{family}/{level}/{code} training receipt",
        )
        durable_training_receipt_hashes[code] = str(training_receipt["entry_receipt_sha256"])
        if mixed_dimension:
            durable_score_inputs[code] = {
                "final_train_log_likelihood": training_receipt.get("final_train_log_likelihood"),
                "training_rows": training_receipt.get("training_rows"),
                "effective_dimension": training_receipt.get("likelihood_feature_count"),
                "projection_sha256": training_receipt.get("projection_sha256"),
            }
        likelihood = training_receipt.get("likelihood")
        covariance = training_receipt.get("covariance")
        occupancy = training_receipt.get("train_occupancy")
        if not all(isinstance(receipt, Mapping) for receipt in (likelihood, covariance, occupancy)):
            raise StateModelSetError(f"B3 READY D4 receipt is missing for {family}/{level}/{code}")
        for receipt, contract, label in (
            (likelihood, D4_LIKELIHOOD_VERSION, "likelihood"),
            (covariance, D4_COVARIANCE_VERSION, "covariance"),
            (occupancy, D4_OCCUPANCY_VERSION, "train occupancy"),
        ):
            _require_canonical_receipt_hash(
                receipt,
                field="receipt_sha256",
                label=f"{family}/{level}/{code} {label}",
            )
            if receipt.get("contract_version") != contract or not receipt.get("evidence"):
                raise StateModelSetError(f"B3 READY {label} evidence is incomplete for {family}/{level}/{code}")
        likelihood_evidence = likelihood.get("evidence")
        if not isinstance(likelihood_evidence, Mapping):
            raise StateModelSetError(f"B3 READY likelihood evidence is incomplete for {family}/{level}/{code}")
        if (
            training_receipt.get("fit_status") != "accepted"
            or training_receipt.get("schema_version")
            != (MIXED_TRAINING_ENTRY_SCHEMA_VERSION if mixed_dimension else "hmm_risk_b3_training_entry_receipt_v1")
            or training_receipt.get("model_entry_status") != "accepted"
            or training_receipt.get("model_entry_valid") is not True
            or training_receipt.get("model_payload_sha256") != entry.get("model_payload_sha256")
            or (
                mixed_dimension
                and (
                    training_receipt.get("feature_count") != expected_feature_count
                    or training_receipt.get("likelihood_feature_count") != effective_feature_count
                    or training_receipt.get("projection_sha256") != entry.get("projection_sha256")
                    or training_receipt.get("projection_receipt") != entry.get("projection_receipt")
                    or training_receipt.get("projection_receipt", {}).get("projected_matrix_shape", [None])[0]
                    != training_receipt.get("training_rows")
                )
            )
            or likelihood.get("monitor_status") != "accepted"
            or likelihood.get("convergence_valid") is not True
            or likelihood.get("likelihood_status") not in {"accepted", "accepted_with_warning"}
            or likelihood.get("likelihood_valid") is not True
            or not likelihood_evidence.get("covariance_receipt_sha256_history")
            or likelihood_evidence.get("covariance_receipt_sha256_history", [])[-1] != covariance.get("receipt_sha256")
            or covariance.get("covariance_status") != "accepted"
            or covariance.get("covariance_valid") is not True
            or occupancy.get("train_occupancy_status") != "accepted"
            or occupancy.get("train_occupancy_valid") is not True
            or not validate_d4_training_receipts(training_receipt)
        ):
            raise StateModelSetError(f"B3 READY training evidence is not accepted for {family}/{level}/{code}")
        occupancy_evidence = occupancy["evidence"]
        if (
            occupancy_evidence.get("direct_sector_level") != level
            or occupancy_evidence.get("sector_code") != code
            or occupancy_evidence.get("dataset_manifest_hash") != dataset_manifest_hash
            or occupancy_evidence.get("mapping_manifest_hash") != mapping_manifest_hash
            or occupancy_evidence.get("calendar_manifest_hash") != calendar_manifest_hash
            or occupancy_evidence.get("feature_domain_policy_sha256") != feature_domain_policy_sha256
        ):
            raise StateModelSetError(f"B3 READY train input lineage is invalid for {family}/{level}/{code}")
        assignment = semantic.get("assignment")
        semantic_evidence = semantic.get("semantic_evidence")
        if not isinstance(assignment, Mapping) or not isinstance(semantic_evidence, Mapping):
            raise StateModelSetError(f"B3 READY semantic receipt is missing for {family}/{level}")
        _require_canonical_receipt_hash(
            assignment,
            field="receipt_sha256",
            label=f"{family}/{level}/{code} assignment",
        )
        _require_canonical_receipt_hash(
            semantic_evidence,
            field="receipt_sha256",
            label=f"{family}/{level}/{code} semantic evidence",
        )
        if (
            assignment.get("semantic_assignment_status") != "accepted"
            or assignment.get("semantic_assignment_valid") is not True
            or semantic_evidence.get("semantic_evidence_status") != "accepted"
            or semantic_evidence.get("semantic_evidence_valid") is not True
            or not isinstance(semantic.get("semantic_mapping"), Mapping)
            or set(semantic["semantic_mapping"].values()) != {"fading", "neutral", "trending"}
            or not assignment.get("evidence")
            or not semantic_evidence.get("evidence")
            or assignment.get("evidence", {}).get("validation_rows") != 182
            or assignment.get("evidence", {}).get("selected_model_payload_sha256") != entry.get("model_payload_sha256")
            or semantic_evidence.get("evidence", {}).get("selected_model_payload_sha256")
            != entry.get("model_payload_sha256")
        ):
            raise StateModelSetError(f"B3 READY semantic evidence is not accepted for {family}/{level}/{code}")
        for evidence_receipt in (assignment, semantic_evidence):
            receipt_evidence = evidence_receipt["evidence"]
            if (
                receipt_evidence.get("direct_sector_level") != level
                or receipt_evidence.get("sector_code") != code
                or receipt_evidence.get("dataset_manifest_hash") != semantic_dataset_manifest_hash
                or receipt_evidence.get("mapping_manifest_hash") != semantic_mapping_manifest_hash
                or receipt_evidence.get("calendar_manifest_hash") != semantic_calendar_manifest_hash
                or receipt_evidence.get("l2_stock_fact_manifest_hash") != semantic_l2_stock_fact_manifest_hash
                or receipt_evidence.get("feature_domain_policy_sha256") != feature_domain_policy_sha256
            ):
                raise StateModelSetError(f"B3 READY frozen input lineage is invalid for {family}/{level}/{code}")
        codes.add(code)
    if codes != set(canonical_codes):
        raise StateModelSetError(f"B3 READY canonical sector set is incomplete for {family}/{level}")
    if mixed_dimension:
        expected_dimension_identity = build_level_dimension_identity(
            entries,
            family=family,
            level=level,
            expected_sector_codes=canonical_codes,
        )
        for field, expected_value in expected_dimension_identity.items():
            if artifact.get(field) != expected_value:
                raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
        selected_aggregate = selected_candidates[0].get("aggregate")
        score_receipts = (
            selected_aggregate.get("ordered_sector_scores") if isinstance(selected_aggregate, Mapping) else None
        )
        if not isinstance(score_receipts, list) or len(score_receipts) != expected_count:
            raise StateModelSetError("hmm_risk_model_selection_level_incomplete")
        for code, score_receipt in zip(canonical_codes, score_receipts, strict=True):
            if not isinstance(score_receipt, Mapping):
                raise StateModelSetError("hmm_risk_model_selection_contract_unsatisfied")
            score_body = {key: value for key, value in score_receipt.items() if key != "score_sha256"}
            score_input = durable_score_inputs.get(code)
            if not isinstance(score_input, Mapping):
                raise StateModelSetError("hmm_risk_model_selection_level_incomplete")
            try:
                final = float(score_input["final_train_log_likelihood"])
                rows = int(score_input["training_rows"])
                dimension = int(score_input["effective_dimension"])
                expected_score = final / (rows * dimension)
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                raise StateModelSetError("hmm_risk_model_selection_contract_unsatisfied") from None
            if (
                score_receipt.get("schema_version") != "hmm_risk_b3_d5_effective_dimension_score_receipt_v1"
                or score_receipt.get("dimension_contract_version") != MIXED_DIMENSION_CONTRACT_VERSION
                or score_receipt.get("sector_code") != code
                or score_receipt.get("final_train_log_likelihood") != final
                or score_receipt.get("training_rows") != rows
                or score_receipt.get("effective_dimension") != dimension
                or score_receipt.get("denominator") != rows * dimension
                or score_receipt.get("projection_sha256") != score_input["projection_sha256"]
                or score_receipt.get("score") != expected_score
                or score_receipt.get("score_sha256") != canonical_sha256(score_body)
            ):
                raise StateModelSetError("hmm_risk_model_selection_contract_unsatisfied")
    if selected_receipt_hashes != tuple(durable_training_receipt_hashes[code] for code in canonical_codes):
        raise StateModelSetError(f"B3 READY selection receipt lineage is invalid for {family}/{level}")


def write_b3_ready_model_set(
    output_root: str | Path,
    *,
    selected_artifacts: Mapping[tuple[str, str], Mapping[str, Any]],
    selection_receipts: Mapping[tuple[str, str], Mapping[str, Any]],
    dataset_manifest_hash: str,
    mapping_manifest_hash: str,
    calendar_manifest_hash: str,
    l2_stock_fact_manifest_hash: str,
    semantic_dataset_manifest_hash: str,
    semantic_mapping_manifest_hash: str,
    semantic_calendar_manifest_hash: str,
    semantic_l2_stock_fact_manifest_hash: str,
    feature_domain_policy_sha256: str,
    feature_domain_policy_manifest: Mapping[str, Any],
    producer_commit: str,
) -> Path:
    """Write a complete four-level READY set; blocked or partial inputs write nothing."""

    required = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    if set(selected_artifacts) != required or set(selection_receipts) != required:
        raise StateModelSetError("B3 READY requires both families and both direct levels")
    _require_hex_identity(dataset_manifest_hash, length=64, label="dataset manifest hash")
    _require_hex_identity(mapping_manifest_hash, length=64, label="mapping manifest hash")
    _require_hex_identity(calendar_manifest_hash, length=64, label="calendar manifest hash")
    _require_hex_identity(l2_stock_fact_manifest_hash, length=64, label="L2 stock-fact manifest hash")
    _require_hex_identity(semantic_dataset_manifest_hash, length=64, label="semantic dataset manifest hash")
    _require_hex_identity(semantic_mapping_manifest_hash, length=64, label="semantic mapping manifest hash")
    _require_hex_identity(semantic_calendar_manifest_hash, length=64, label="semantic calendar manifest hash")
    _require_hex_identity(
        semantic_l2_stock_fact_manifest_hash,
        length=64,
        label="semantic L2 stock-fact manifest hash",
    )
    _require_hex_identity(feature_domain_policy_sha256, length=64, label="feature-domain policy hash")
    policy_source_identities = {
        "dataset_manifest_hash": dataset_manifest_hash,
        "mapping_manifest_hash": mapping_manifest_hash,
        "calendar_manifest_hash": calendar_manifest_hash,
        "l2_stock_fact_manifest_hash": l2_stock_fact_manifest_hash,
    }
    if any(
        feature_domain_policy_manifest.get(field) != expected for field, expected in policy_source_identities.items()
    ):
        raise StateModelSetError("B3 READY feature-domain policy source identity is invalid")
    try:
        validated_policy = validate_c010_policy_manifest(feature_domain_policy_manifest)
    except StateModelSetError as exc:
        raise StateModelSetError(f"B3 READY feature-domain policy manifest is invalid: {exc}") from exc
    if validated_policy.get("receipt_sha256") != feature_domain_policy_sha256:
        raise StateModelSetError("B3 READY feature-domain policy manifest identity is invalid")
    _require_hex_identity(producer_commit, length=40, label="producer commit")
    layers: dict[str, Any] = {}
    payloads: dict[str, bytes] = {}
    for family, level in sorted(required):
        artifact = dict(selected_artifacts[(family, level)])
        expected_count = 31 if level == "L1" else 131
        selection = selection_receipts[(family, level)]
        _validate_ready_layer(
            artifact,
            selection,
            family=family,
            level=level,
            expected_count=expected_count,
            dataset_manifest_hash=dataset_manifest_hash,
            mapping_manifest_hash=mapping_manifest_hash,
            calendar_manifest_hash=calendar_manifest_hash,
            l2_stock_fact_manifest_hash=l2_stock_fact_manifest_hash,
            semantic_dataset_manifest_hash=semantic_dataset_manifest_hash,
            semantic_mapping_manifest_hash=semantic_mapping_manifest_hash,
            semantic_calendar_manifest_hash=semantic_calendar_manifest_hash,
            semantic_l2_stock_fact_manifest_hash=semantic_l2_stock_fact_manifest_hash,
            feature_domain_policy_sha256=feature_domain_policy_sha256,
        )
        payload = canonical_json_bytes(artifact)
        payload_sha = canonical_sha256(artifact)
        key = f"{family}:{level}"
        relative = f"artifacts/{payload_sha}.{family}.{level.lower()}.json"
        payloads[relative] = payload
        layers[key] = {
            "family": family,
            "level": level,
            "status": "accepted",
            "sector_count": expected_count,
            "artifact_uri": relative,
            "artifact_sha256": payload_sha,
            "selection_receipt_sha256": selection.get("receipt_sha256"),
            "selected_seed": selection.get("evidence", {}).get("selected_seed"),
        }
        if uses_mixed_dimension_level(family, level):
            layers[key].update(
                {
                    "dimension_contract_version": artifact.get("dimension_contract_version"),
                    "likelihood_feature_count_histogram": artifact.get("likelihood_feature_count_histogram"),
                    "ordered_entry_dimension_identities_sha256": artifact.get(
                        "ordered_entry_dimension_identities_sha256"
                    ),
                }
            )
    body = {
        "schema_version": SCHEMA_VERSION,
        "status": "READY",
        "producer_commit": producer_commit,
        "dataset_manifest_hash": dataset_manifest_hash,
        "mapping_manifest_hash": mapping_manifest_hash,
        "calendar_manifest_hash": calendar_manifest_hash,
        "l2_stock_fact_manifest_hash": l2_stock_fact_manifest_hash,
        "semantic_dataset_manifest_hash": semantic_dataset_manifest_hash,
        "semantic_mapping_manifest_hash": semantic_mapping_manifest_hash,
        "semantic_calendar_manifest_hash": semantic_calendar_manifest_hash,
        "semantic_l2_stock_fact_manifest_hash": semantic_l2_stock_fact_manifest_hash,
        "feature_domain_policy_sha256": feature_domain_policy_sha256,
        "feature_domain_policy_manifest": validated_policy,
        "contracts": {
            "d3": D3_CONTRACT_VERSION,
            "l2_retrain": L2_RETRAIN_VERSION,
            "d1_d5_compat": MIXED_DIMENSION_CONTRACT_VERSION,
        },
        "layers": layers,
        "selection_receipts": {
            f"{family}:{level}": selection_receipts[(family, level)] for family, level in sorted(required)
        },
        "ready_requires_both_families": True,
        "ready_requires_direct_l1_and_l2": True,
    }
    set_hash = canonical_sha256(body)
    manifest = {
        **body,
        "state_model_set_id": f"hmms_{set_hash[:24]}",
        "state_model_set_hash": set_hash,
    }
    root = Path(output_root).resolve() / manifest["state_model_set_id"]
    for relative, payload in sorted(payloads.items()):
        _write_immutable(root / relative, payload)
    manifest_path = root / "manifest.json"
    _write_immutable(manifest_path, canonical_json_bytes(manifest))
    return manifest_path


def read_b3_selected_level_artifact(
    artifact_path: str | Path,
    *,
    selection: Mapping[str, Any],
    family: str,
    level: str,
    expected_count: int,
    dataset_manifest_hash: str,
    mapping_manifest_hash: str,
    calendar_manifest_hash: str,
    l2_stock_fact_manifest_hash: str,
    semantic_dataset_manifest_hash: str,
    semantic_mapping_manifest_hash: str,
    semantic_calendar_manifest_hash: str,
    semantic_l2_stock_fact_manifest_hash: str,
    feature_domain_policy_sha256: str,
) -> dict[str, Any]:
    """Read back one durable selected-level artifact through the writer's validation authority."""

    path = Path(artifact_path).resolve()
    try:
        payload = path.read_bytes()
        artifact = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateModelSetError(f"B3 selected artifact readback failed: {path}") from exc
    if not isinstance(artifact, dict) or payload != canonical_json_bytes(artifact):
        raise StateModelSetError(f"B3 selected artifact canonical readback failed: {path}")
    _validate_ready_layer(
        artifact,
        selection,
        family=family,
        level=level,
        expected_count=expected_count,
        dataset_manifest_hash=dataset_manifest_hash,
        mapping_manifest_hash=mapping_manifest_hash,
        calendar_manifest_hash=calendar_manifest_hash,
        l2_stock_fact_manifest_hash=l2_stock_fact_manifest_hash,
        semantic_dataset_manifest_hash=semantic_dataset_manifest_hash,
        semantic_mapping_manifest_hash=semantic_mapping_manifest_hash,
        semantic_calendar_manifest_hash=semantic_calendar_manifest_hash,
        semantic_l2_stock_fact_manifest_hash=semantic_l2_stock_fact_manifest_hash,
        feature_domain_policy_sha256=feature_domain_policy_sha256,
    )
    return artifact


def read_b3_ready_model_set(manifest_path: str | Path) -> dict[str, Any]:
    """Read back a complete four-layer READY set without fallback to a prior or partial artifact."""

    path = Path(manifest_path).resolve()
    try:
        payload = path.read_bytes()
        manifest = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateModelSetError(f"B3 READY manifest readback failed: {path}") from exc
    if not isinstance(manifest, dict) or payload != canonical_json_bytes(manifest):
        raise StateModelSetError(f"B3 READY manifest canonical readback failed: {path}")
    body = {key: value for key, value in manifest.items() if key not in {"state_model_set_id", "state_model_set_hash"}}
    expected_hash = canonical_sha256(body)
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("status") != "READY"
        or manifest.get("state_model_set_hash") != expected_hash
        or manifest.get("state_model_set_id") != f"hmms_{expected_hash[:24]}"
        or manifest.get("ready_requires_both_families") is not True
        or manifest.get("ready_requires_direct_l1_and_l2") is not True
        or manifest.get("contracts")
        != {
            "d3": D3_CONTRACT_VERSION,
            "l2_retrain": L2_RETRAIN_VERSION,
            "d1_d5_compat": MIXED_DIMENSION_CONTRACT_VERSION,
        }
    ):
        raise StateModelSetError("B3 READY manifest identity is invalid")
    for field, label in (
        ("dataset_manifest_hash", "dataset manifest hash"),
        ("mapping_manifest_hash", "mapping manifest hash"),
        ("calendar_manifest_hash", "calendar manifest hash"),
        ("l2_stock_fact_manifest_hash", "L2 stock-fact manifest hash"),
        ("semantic_dataset_manifest_hash", "semantic dataset manifest hash"),
        ("semantic_mapping_manifest_hash", "semantic mapping manifest hash"),
        ("semantic_calendar_manifest_hash", "semantic calendar manifest hash"),
        ("semantic_l2_stock_fact_manifest_hash", "semantic L2 stock-fact manifest hash"),
        ("feature_domain_policy_sha256", "feature-domain policy hash"),
    ):
        _require_hex_identity(str(manifest.get(field) or ""), length=64, label=label)
    _require_hex_identity(str(manifest.get("producer_commit") or ""), length=40, label="producer commit")
    policy = manifest.get("feature_domain_policy_manifest")
    if not isinstance(policy, Mapping):
        raise StateModelSetError("B3 READY feature-domain policy manifest is missing")
    try:
        validated_policy = validate_c010_policy_manifest(policy)
    except StateModelSetError as exc:
        raise StateModelSetError(f"B3 READY feature-domain policy manifest is invalid: {exc}") from exc
    if (
        validated_policy.get("receipt_sha256") != manifest.get("feature_domain_policy_sha256")
        or validated_policy.get("dataset_manifest_hash") != manifest.get("dataset_manifest_hash")
        or validated_policy.get("mapping_manifest_hash") != manifest.get("mapping_manifest_hash")
        or validated_policy.get("calendar_manifest_hash") != manifest.get("calendar_manifest_hash")
        or validated_policy.get("l2_stock_fact_manifest_hash") != manifest.get("l2_stock_fact_manifest_hash")
    ):
        raise StateModelSetError("B3 READY feature-domain policy source identity is invalid")
    root = path.parent.resolve()
    if root.name != manifest["state_model_set_id"]:
        raise StateModelSetError("B3 READY manifest path identity is invalid")
    required = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    layers = manifest.get("layers")
    selections = manifest.get("selection_receipts")
    expected_keys = {f"{family}:{level}" for family, level in required}
    if not isinstance(layers, Mapping) or not isinstance(selections, Mapping):
        raise StateModelSetError("B3 READY manifest layer evidence is missing")
    if set(layers) != expected_keys or set(selections) != expected_keys:
        raise StateModelSetError("B3 READY requires both families and both direct levels")
    for family, level in sorted(required):
        key = f"{family}:{level}"
        layer = layers[key]
        selection = selections[key]
        expected_count = 31 if level == "L1" else 131
        if not isinstance(layer, Mapping) or not isinstance(selection, Mapping):
            raise StateModelSetError(f"B3 READY layer is invalid for {key}")
        artifact_sha256 = str(layer.get("artifact_sha256") or "")
        expected_uri = f"artifacts/{artifact_sha256}.{family}.{level.lower()}.json"
        if (
            layer.get("family") != family
            or layer.get("level") != level
            or layer.get("status") != "accepted"
            or layer.get("sector_count") != expected_count
            or layer.get("artifact_uri") != expected_uri
            or layer.get("selection_receipt_sha256") != selection.get("receipt_sha256")
            or layer.get("selected_seed") != selection.get("evidence", {}).get("selected_seed")
        ):
            raise StateModelSetError(f"B3 READY layer identity is invalid for {key}")
        artifact_path = (root / expected_uri).resolve()
        if not artifact_path.is_relative_to(root):
            raise StateModelSetError(f"B3 READY artifact escapes its model-set root for {key}")
        artifact = read_b3_selected_level_artifact(
            artifact_path,
            selection=selection,
            family=family,
            level=level,
            expected_count=expected_count,
            dataset_manifest_hash=str(manifest.get("dataset_manifest_hash") or ""),
            mapping_manifest_hash=str(manifest.get("mapping_manifest_hash") or ""),
            calendar_manifest_hash=str(manifest.get("calendar_manifest_hash") or ""),
            l2_stock_fact_manifest_hash=str(manifest.get("l2_stock_fact_manifest_hash") or ""),
            semantic_dataset_manifest_hash=str(manifest.get("semantic_dataset_manifest_hash") or ""),
            semantic_mapping_manifest_hash=str(manifest.get("semantic_mapping_manifest_hash") or ""),
            semantic_calendar_manifest_hash=str(manifest.get("semantic_calendar_manifest_hash") or ""),
            semantic_l2_stock_fact_manifest_hash=str(manifest.get("semantic_l2_stock_fact_manifest_hash") or ""),
            feature_domain_policy_sha256=str(manifest.get("feature_domain_policy_sha256") or ""),
        )
        if canonical_sha256(artifact) != artifact_sha256:
            raise StateModelSetError(f"B3 READY artifact identity is invalid for {key}")
        if uses_mixed_dimension_level(family, level) and (
            layer.get("dimension_contract_version") != artifact.get("dimension_contract_version")
            or layer.get("likelihood_feature_count_histogram") != artifact.get("likelihood_feature_count_histogram")
            or layer.get("ordered_entry_dimension_identities_sha256")
            != artifact.get("ordered_entry_dimension_identities_sha256")
        ):
            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
    return manifest
