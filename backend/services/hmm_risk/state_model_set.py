"""Controlled offline preparation for direct HMM Risk L1/L2 model sets."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import platform
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np


SCHEMA_VERSION = "hmm_risk_state_model_set_v1"
L1_ARTIFACT_SCHEMA = "hmm_risk_direct_l1_models_v1"
L2_ARTIFACT_SCHEMA = "hmm_risk_normalized_l2_models_v1"
SEMANTIC_LABELS = frozenset({"trending", "neutral", "fading"})
EXPECTED_L1_COUNT = 31
EXPECTED_L2_COUNT = 131
HMM_N_ITER = 300
HMM_MIN_COVAR = 1e-3
HMM_MAX_COVAR = 10.0
HMM_TRANSITION_ALPHA = 0.1
HMM_MIN_SELF_TRANSITION = 0.3
C008_DIAGNOSTIC_SEEDS = tuple(range(42, 50))
C008_B1_DIAGNOSTIC_VERSION = "hmm_risk_c008_b1_soft_evidence_v1"
C008_B3_STRUCTURAL_CONTRACT = "C-008-B3-STRUCTURAL-A"
C008_B3_DIAG02_CONTRACT = "C-008-B3-DIAG-02"
C008_B3_DIAG02_VERSION = "hmm_risk_c008_b3_diag02_structural_evidence_v1"
C008_B3_DIAG02_FIXED_THREAD_ENV = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)
BASE_FEATURES = (
    "daily_return",
    "excess_return_Nd",
    "volume_ratio",
    "limit_up_ratio",
    "volatility_Nd",
    "net_mf_ratio",
    "elg_net_mf_ratio",
)
ALL_CORE_FEATURES = BASE_FEATURES + (
    "sf_turnover_pctile_250d_neg",
    "sf_turnover_pctile_120d_neg",
    "sf_turnover_ma5_ma20_neg",
    "sf_mf_net_ratio_std_5d_neg",
    "sf_small_net_ratio_5d",
    "sf_intraday_range_5d_neg",
    "sf_atr14_pctile_250d_neg",
    "sf_range_vs_market_10d",
    "sf_vol_vs_market_20d",
    "sf_breadth_1d",
    "sf_breadth_5d",
    "sf_excess_breadth_5d",
    "sf_dispersion_5d_neg",
)
PARSER_AUTOCYCLE = "hmm_risk_l2_autocycle_models_v1"
PARSER_LEGACY_UNIFORM = "hmm_risk_l2_legacy_uniform_startprob_v1"
SUPPORTED_PARSERS = frozenset({PARSER_AUTOCYCLE, PARSER_LEGACY_UNIFORM})


class StateModelSetError(RuntimeError):
    """Raised when model-set preparation cannot prove the approved contract."""


class _L1FitDiagnosticError(StateModelSetError):
    """Preserve the exact failed fit stage and numeric evidence for C-008-B1."""

    def __init__(self, message: str, *, stage: str, evidence: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.stage = stage
        self.evidence = dict(evidence)


class _DuplicateKeyError(ValueError):
    pass


def _reject_duplicate_keys(items: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in items:
        if key in output:
            raise _DuplicateKeyError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_sha256(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def diagnostic_runtime_versions() -> dict[str, str]:
    try:
        hmmlearn_version = importlib.metadata.version("hmmlearn")
    except importlib.metadata.PackageNotFoundError:
        hmmlearn_version = "not-installed"
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_executable_name": Path(sys.executable).name,
        "numpy_version": np.__version__,
        "hmmlearn_version": hmmlearn_version,
    }


def c008_b3_diag02_parameter_profile() -> dict[str, Any]:
    """Return the explicit diagnostic-only profile; none of its numeric values are active gates."""

    return {
        "schema_version": "hmm_risk_c008_b3_diag02_parameter_profile_v1",
        "contract": C008_B3_DIAG02_CONTRACT,
        "structural_contract": C008_B3_STRUCTURAL_CONTRACT,
        "numeric_contract_status": "DIAGNOSTIC_ONLY_NOT_APPROVED",
        "restart_schedule": list(C008_DIAGNOSTIC_SEEDS),
        "kmeans": {
            "n_clusters": 3,
            "init": "k-means++",
            "n_init": 1,
            "random_state": "restart_seed",
            "max_iter": 300,
            "tol": 1e-4,
            "algorithm": "lloyd",
            "copy_x": True,
            "empty_or_lt_two_cluster_members": "fit_failed_initialization",
            "means_initialization": "cluster_centers",
            "diag_covariance_initialization": "cluster_population_variance_ddof_0_then_diagnostic_bounds",
        },
        "gaussian_hmm": {
            "n_components": 3,
            "covariance_type": "diag",
            "min_covar": HMM_MIN_COVAR,
            "startprob_prior": 1.0,
            "transmat_prior": 1.0,
            "means_prior": 0.0,
            "means_weight": 0.0,
            "covars_prior": 0.01,
            "covars_weight": 1.0,
            "algorithm": "viterbi",
            "random_state": "restart_seed",
            "n_iter": HMM_N_ITER,
            "tol": 0.01,
            "params": "stmc",
            "init_params": "",
            "implementation": "log",
            "verbose": False,
        },
        "manual_initialization": {
            "startprob": "uniform_1_over_3",
            "transition_alpha": HMM_TRANSITION_ALPHA,
            "minimum_self_transition": HMM_MIN_SELF_TRANSITION,
            "transition_order": "hard_transition_counts_then_alpha_then_self_floor",
            "initial_covariance_lower_bound": HMM_MIN_COVAR,
            "initial_covariance_upper_bound": HMM_MAX_COVAR,
            "postfit_parameter_projection_performed": False,
        },
        "formal_acceptance_thresholds_applied": False,
    }


def c008_b3_diag02_fixed_numeric_environment() -> dict[str, Any]:
    """Fail closed unless both fresh diagnostic processes use the approved single-thread environment."""

    thread_env = {key: os.environ.get(key) for key in C008_B3_DIAG02_FIXED_THREAD_ENV}
    invalid = {key: value for key, value in thread_env.items() if value != "1"}
    if invalid:
        raise StateModelSetError(f"C-008-B3-DIAG-02 requires fixed thread env value 1: {invalid}")
    try:
        from threadpoolctl import threadpool_info
    except ImportError as exc:  # pragma: no cover - scikit-learn dependency contract supplies it.
        raise StateModelSetError("threadpoolctl is required for C-008-B3-DIAG-02 environment evidence") from exc
    pools = threadpool_info()
    non_single = [
        {
            "user_api": item.get("user_api"),
            "internal_api": item.get("internal_api"),
            "num_threads": item.get("num_threads"),
        }
        for item in pools
        if int(item.get("num_threads") or 0) != 1
    ]
    if non_single:
        raise StateModelSetError(f"C-008-B3-DIAG-02 runtime thread pools are not single-threaded: {non_single}")
    packages = {}
    for name in ("numpy", "scipy", "scikit-learn", "hmmlearn", "threadpoolctl"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = "not-installed"
    return {
        "schema_version": "hmm_risk_c008_b3_diag02_numeric_environment_v1",
        "scope": "same_host_same_fixed_numeric_environment_only",
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_executable": str(Path(sys.executable).resolve()),
        "packages": packages,
        "thread_env": thread_env,
        "thread_pools": pools,
    }


def _float64_array_identity(value: Any) -> dict[str, Any]:
    values = np.ascontiguousarray(np.asarray(value, dtype="<f8"))
    return {
        "dtype": "float64_le",
        "shape": list(values.shape),
        "sha256": sha256_bytes(values.tobytes(order="C")),
    }


def _require_sha256(value: str, field: str) -> str:
    normalized = str(value or "").lower()
    if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
        raise StateModelSetError(f"{field} must be a lowercase SHA-256 hex digest")
    return normalized


def _finite_array(value: Any, field: str, *, ndim: int | None = None) -> np.ndarray:
    try:
        array = np.asarray(value, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise StateModelSetError(f"{field} must be numeric") from exc
    if ndim is not None and array.ndim != ndim:
        raise StateModelSetError(f"{field} must have ndim={ndim}; actual={array.ndim}")
    if array.size == 0 or not np.isfinite(array).all():
        raise StateModelSetError(f"{field} must contain only finite values")
    return array


def _probability_vector(value: Any, field: str, size: int) -> np.ndarray:
    array = _finite_array(value, field, ndim=1)
    if array.shape != (size,) or np.any(array < 0) or not math.isclose(float(array.sum()), 1.0, abs_tol=1e-9):
        raise StateModelSetError(f"{field} must be a non-negative length-{size} probability vector")
    return array


def _transition_matrix(value: Any, field: str, size: int) -> np.ndarray:
    array = _finite_array(value, field, ndim=2)
    if array.shape != (size, size) or np.any(array < 0):
        raise StateModelSetError(f"{field} must be a non-negative {size}x{size} matrix")
    if not np.allclose(array.sum(axis=1), np.ones(size), atol=1e-9, rtol=0):
        raise StateModelSetError(f"{field} rows must sum to one")
    return array


def _semantic_labels(value: Any, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise StateModelSetError(f"{field} must be an object")
    labels = {str(key): str(label) for key, label in value.items()}
    if set(labels) != {"0", "1", "2"} or frozenset(labels.values()) != SEMANTIC_LABELS:
        raise StateModelSetError(f"{field} must bijectively map states 0/1/2 to the three semantic labels")
    return labels


def _feature_names(entry: Mapping[str, Any], field: str) -> tuple[str, ...]:
    raw = entry.get("feature_names")
    if raw is None:
        raw = entry.get("obs_features")
    if not isinstance(raw, list) or not raw or any(not str(item).strip() for item in raw):
        raise StateModelSetError(f"{field} feature names are missing")
    names = tuple(str(item) for item in raw)
    if len(names) != len(set(names)):
        raise StateModelSetError(f"{field} feature names contain duplicates")
    return names


def _validate_model_entry(
    sector_code: str,
    raw: Any,
    *,
    parser_contract: str,
    expected_features: tuple[str, ...],
) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise StateModelSetError(f"L2 model {sector_code} must be an object")
    n_states = int(raw.get("n_states", 0))
    if n_states != 3:
        raise StateModelSetError(f"L2 model {sector_code} must have exactly three states")
    covariance_type = str(raw.get("covariance_type") or "")
    if covariance_type != "diag":
        raise StateModelSetError(f"L2 model {sector_code} covariance_type must be diag")
    features = _feature_names(raw, f"L2 model {sector_code}")
    if features != expected_features:
        raise StateModelSetError(f"L2 model {sector_code} feature definition differs from the family contract")

    means = _finite_array(raw.get("means"), f"L2 model {sector_code}.means", ndim=2)
    covars = _finite_array(raw.get("covars"), f"L2 model {sector_code}.covars")
    if means.shape != (3, len(features)):
        raise StateModelSetError(f"L2 model {sector_code}.means has an invalid shape")
    if covars.shape == (3, len(features), len(features)):
        off_diagonal = covars.copy()
        diagonal = np.diagonal(covars, axis1=1, axis2=2).copy()
        for state in range(3):
            np.fill_diagonal(off_diagonal[state], 0.0)
        if not np.allclose(off_diagonal, 0.0, atol=1e-12, rtol=0):
            raise StateModelSetError(f"L2 model {sector_code}.covars contradicts diag covariance_type")
        covars = diagonal
    if covars.shape != (3, len(features)) or np.any(covars <= 0):
        raise StateModelSetError(f"L2 model {sector_code}.covars must be positive diag variances")
    transmat = _transition_matrix(raw.get("transmat"), f"L2 model {sector_code}.transmat", 3)
    labels = _semantic_labels(raw.get("state_labels"), f"L2 model {sector_code}.state_labels")

    startprob_source = "artifact"
    if raw.get("startprob") is None:
        if parser_contract != PARSER_LEGACY_UNIFORM:
            raise StateModelSetError(f"L2 model {sector_code}.startprob is required by {parser_contract}")
        startprob = np.full(3, 1.0 / 3.0, dtype=np.float64)
        startprob_source = "legacy_uniform_startprob_v1"
    else:
        startprob = _probability_vector(raw.get("startprob"), f"L2 model {sector_code}.startprob", 3)

    training_rows = int(raw.get("training_days", raw.get("training_rows", 0)))
    if training_rows < 120:
        raise StateModelSetError(f"L2 model {sector_code} has fewer than 120 training rows")
    normalized = {
        "sector_code": sector_code,
        "sector_name": str(raw.get("sector_name") or sector_code),
        "sector_level": "L2",
        "state_origin": "direct_hmm",
        "n_states": 3,
        "covariance_type": "diag",
        "feature_names": list(features),
        "startprob": startprob.tolist(),
        "startprob_source": startprob_source,
        "transmat": transmat.tolist(),
        "means": means.tolist(),
        "covars": covars.tolist(),
        "state_labels": labels,
        "observation_version": str(raw.get("observation_version") or "source_family_observation_v1"),
        "training_rows": training_rows,
    }
    return normalized


def parse_l2_artifact(
    payload_bytes: bytes,
    *,
    parser_contract: str,
    expected_sha256: str,
    expected_sector_codes: Sequence[str],
    expected_features: Sequence[str],
) -> dict[str, Any]:
    """Parse one approved L2 artifact without path/latest inference."""

    if parser_contract not in SUPPORTED_PARSERS:
        raise StateModelSetError(f"unsupported L2 parser contract: {parser_contract}")
    actual_sha256 = sha256_bytes(payload_bytes)
    if actual_sha256 != _require_sha256(expected_sha256, "expected_sha256"):
        raise StateModelSetError(f"L2 artifact SHA-256 mismatch expected={expected_sha256} actual={actual_sha256}")
    try:
        root = json.loads(payload_bytes.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateKeyError) as exc:
        raise StateModelSetError(f"L2 artifact is not deterministic UTF-8 JSON: {exc}") from exc
    if not isinstance(root, Mapping):
        raise StateModelSetError("L2 artifact root must be an object")
    raw_models = root.get("models") if isinstance(root.get("models"), Mapping) else root
    expected_codes = tuple(sorted(str(code) for code in expected_sector_codes))
    if len(expected_codes) != EXPECTED_L2_COUNT or len(set(expected_codes)) != EXPECTED_L2_COUNT:
        raise StateModelSetError("expected L2 sector set must contain exactly 131 unique codes")
    actual_codes = tuple(sorted(str(code) for code in raw_models))
    if actual_codes != expected_codes:
        missing = sorted(set(expected_codes).difference(actual_codes))
        extra = sorted(set(actual_codes).difference(expected_codes))
        raise StateModelSetError(f"L2 sector coverage mismatch missing={missing} extra={extra}")
    feature_tuple = tuple(str(item) for item in expected_features)
    models = {
        code: _validate_model_entry(
            code,
            raw_models[code],
            parser_contract=parser_contract,
            expected_features=feature_tuple,
        )
        for code in expected_codes
    }
    return {
        "schema_version": L2_ARTIFACT_SCHEMA,
        "parser_contract": parser_contract,
        "source_artifact_sha256": actual_sha256,
        "sector_level": "L2",
        "sector_count": len(models),
        "expected_sector_set_hash": canonical_sha256(expected_codes),
        "feature_names": list(feature_tuple),
        "models": models,
    }


@dataclass(frozen=True)
class L1TrainingSeries:
    sector_code: str
    sector_name: str
    train_observations: np.ndarray
    train_dates: tuple[date, ...]
    validation_observations: np.ndarray
    validation_dates: tuple[date, ...]
    validation_future_utility: np.ndarray
    pit_l2_constituents: tuple[str, ...]
    pit_constituent_manifest_hash: str
    observation_manifest_hash: str

    def validate(self, feature_count: int) -> None:
        if not self.sector_code.strip() or not self.sector_name.strip():
            raise StateModelSetError("L1 sector code/name must be non-empty")
        train = _finite_array(self.train_observations, f"{self.sector_code}.train", ndim=2)
        validation = _finite_array(self.validation_observations, f"{self.sector_code}.validation", ndim=2)
        utility = _finite_array(self.validation_future_utility, f"{self.sector_code}.utility", ndim=1)
        if train.shape[1] != feature_count or validation.shape[1] != feature_count:
            raise StateModelSetError(f"{self.sector_code} feature count differs from family contract")
        if train.shape[0] < 120 or validation.shape[0] < 30 or utility.shape != (validation.shape[0],):
            raise StateModelSetError(f"{self.sector_code} has insufficient train/validation evidence")
        if len(self.train_dates) != train.shape[0] or len(self.validation_dates) != validation.shape[0]:
            raise StateModelSetError(f"{self.sector_code} observation dates must align with train/validation rows")
        for field, values in (("train_dates", self.train_dates), ("validation_dates", self.validation_dates)):
            if any(not isinstance(value, date) for value in values):
                raise StateModelSetError(f"{self.sector_code} {field} must contain dates")
            if tuple(sorted(values)) != values or len(set(values)) != len(values):
                raise StateModelSetError(f"{self.sector_code} {field} must be strictly increasing")
        constituents = tuple(sorted(self.pit_l2_constituents))
        if not constituents or constituents != self.pit_l2_constituents or len(set(constituents)) != len(constituents):
            raise StateModelSetError(f"{self.sector_code} PIT L2 constituents must be sorted and unique")
        _require_sha256(self.pit_constituent_manifest_hash, "pit_constituent_manifest_hash")
        _require_sha256(self.observation_manifest_hash, "observation_manifest_hash")


def _fit_preprocess(
    series: Mapping[str, L1TrainingSeries],
    *,
    preprocess_family: str,
) -> dict[str, Any]:
    train = np.vstack([np.asarray(item.train_observations, dtype=np.float64) for item in series.values()])
    if preprocess_family == "identity":
        return {"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None}
    if preprocess_family != "winsor_zscore_1_99_train_global_v1":
        raise StateModelSetError(f"unsupported preprocess family: {preprocess_family}")
    low = np.quantile(train, 0.01, axis=0)
    high = np.quantile(train, 0.99, axis=0)
    clipped = np.clip(train, low, high)
    center = clipped.mean(axis=0)
    scale = clipped.std(axis=0, ddof=0)
    if not np.isfinite(low).all() or not np.isfinite(high).all() or not np.isfinite(center).all():
        raise StateModelSetError("preprocess parameters are non-finite")
    if np.any(scale <= 1e-12) or not np.isfinite(scale).all():
        raise StateModelSetError("preprocess scale must be finite and non-zero; no unit fallback is allowed")
    return {
        "family": preprocess_family,
        "winsor_low": low.tolist(),
        "winsor_high": high.tolist(),
        "center": center.tolist(),
        "scale": scale.tolist(),
    }


def _apply_preprocess(observations: np.ndarray, params: Mapping[str, Any]) -> np.ndarray:
    values = np.asarray(observations, dtype=np.float64)
    if params["family"] == "identity":
        output = values.copy()
    else:
        low = np.asarray(params["winsor_low"], dtype=np.float64)
        high = np.asarray(params["winsor_high"], dtype=np.float64)
        center = np.asarray(params["center"], dtype=np.float64)
        scale = np.asarray(params["scale"], dtype=np.float64)
        output = (np.clip(values, low, high) - center) / scale
    if not np.isfinite(output).all():
        raise StateModelSetError("preprocessed observations contain non-finite values")
    return output


def causal_forward_posteriors(
    observations: np.ndarray,
    *,
    startprob: np.ndarray,
    transmat: np.ndarray,
    means: np.ndarray,
    covars: np.ndarray,
) -> np.ndarray:
    """Return filtered p(z_t|x_<=t); never performs backward smoothing."""

    values = _finite_array(observations, "causal observations", ndim=2)
    start = _probability_vector(startprob, "startprob", len(startprob))
    transition = _transition_matrix(transmat, "transmat", len(start))
    mean_array = _finite_array(means, "means", ndim=2)
    covariance = _finite_array(covars, "covars", ndim=2)
    if (
        mean_array.shape != covariance.shape
        or mean_array.shape[0] != len(start)
        or values.shape[1] != mean_array.shape[1]
    ):
        raise StateModelSetError("causal filter parameter shapes do not match")
    if np.any(covariance <= 0):
        raise StateModelSetError("causal filter covariance must be positive")

    log_emission = -0.5 * (
        np.log(2.0 * np.pi * covariance)[None, :, :]
        + ((values[:, None, :] - mean_array[None, :, :]) ** 2) / covariance[None, :, :]
    ).sum(axis=2)
    output = np.empty((values.shape[0], len(start)), dtype=np.float64)
    prior = start
    for index in range(values.shape[0]):
        shifted = log_emission[index] - float(np.max(log_emission[index]))
        likelihood = np.exp(shifted)
        posterior = prior * likelihood
        denominator = float(posterior.sum())
        if not math.isfinite(denominator) or denominator <= 0:
            raise StateModelSetError(f"causal posterior normalization failed at row {index}")
        posterior /= denominator
        output[index] = posterior
        prior = posterior @ transition
    return output


def _bounded_diag_covariance(covars: np.ndarray) -> tuple[np.ndarray, int]:
    values = _finite_array(covars, "fitted covariance", ndim=2)
    anomaly_mask = (values < HMM_MIN_COVAR) | (values > HMM_MAX_COVAR)
    bounded = np.clip(values, HMM_MIN_COVAR, HMM_MAX_COVAR)
    return bounded, int(anomaly_mask.sum())


def _covariance_diagnostic(value: Any, *, expected_shape: tuple[int, int]) -> dict[str, Any]:
    """Describe raw fitted covariance without treating clipping as acceptance."""

    try:
        values = np.asarray(value, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        return {
            "conversion_error": f"{type(exc).__name__}: {exc}",
            "expected_shape": list(expected_shape),
            "actual_shape": None,
            "valid_for_bounding": False,
        }
    finite = np.isfinite(values)
    finite_values = values[finite]
    non_finite_count = int((~finite).sum())
    non_positive_count = int(((values <= 0) & finite).sum())
    lower_mask = (values < HMM_MIN_COVAR) & finite
    upper_mask = (values > HMM_MAX_COVAR) & finite
    anomaly_mask = lower_mask | upper_mask
    shape_valid = values.shape == expected_shape
    per_state = anomaly_mask.sum(axis=1).astype(int).tolist() if values.ndim == 2 else None
    per_feature = anomaly_mask.sum(axis=0).astype(int).tolist() if values.ndim == 2 else None
    return {
        "expected_shape": list(expected_shape),
        "actual_shape": list(values.shape),
        "shape_valid": shape_valid,
        "finite_count": int(finite.sum()),
        "non_finite_count": non_finite_count,
        "non_positive_count": non_positive_count,
        "raw_min_finite": float(finite_values.min()) if finite_values.size else None,
        "raw_max_finite": float(finite_values.max()) if finite_values.size else None,
        "lower_bound_anomaly_count": int(lower_mask.sum()),
        "upper_bound_anomaly_count": int(upper_mask.sum()),
        "anomaly_count": int(anomaly_mask.sum()),
        "anomaly_count_by_state": per_state,
        "anomaly_count_by_feature": per_feature,
        "anomaly_mask_sha256": canonical_sha256(anomaly_mask.astype(np.uint8).tolist()),
        "min_covar": HMM_MIN_COVAR,
        "max_covar": HMM_MAX_COVAR,
        "valid_for_bounding": bool(shape_valid and non_finite_count == 0 and non_positive_count == 0),
    }


def _monitor_diagnostic(model: Any) -> dict[str, Any]:
    history = tuple(float(value) for value in model.monitor_.history)
    deltas: list[dict[str, Any]] = []
    for index in range(1, len(history)):
        previous = history[index - 1]
        current = history[index]
        comparable = math.isfinite(previous) and math.isfinite(current)
        absolute = current - previous if comparable else None
        deltas.append(
            {
                "history_index": index,
                "previous": previous if math.isfinite(previous) else None,
                "current": current if math.isfinite(current) else None,
                "absolute_delta": absolute,
                "relative_delta": (
                    absolute / max(abs(previous), np.finfo(np.float64).eps) if absolute is not None else None
                ),
                "comparable": comparable,
                "negative": bool(absolute is not None and absolute < 0.0),
                "terminal": index == len(history) - 1,
            }
        )
    converged = bool(model.monitor_.converged)
    iterations = int(model.monitor_.iter)
    tolerance = float(model.monitor_.tol)
    if not converged:
        reason = "not_converged"
    elif iterations >= int(model.monitor_.n_iter):
        reason = "maximum_iterations_reached"
    else:
        reason = "monitor_delta_below_tolerance"
    negative = [item for item in deltas if item["negative"]]
    return {
        "converged": converged,
        "reason": reason,
        "iterations": iterations,
        "maximum_iterations": int(model.monitor_.n_iter),
        "tolerance": tolerance,
        "history": [value if math.isfinite(value) else None for value in history],
        "history_non_finite_count": sum(not math.isfinite(value) for value in history),
        "deltas": deltas,
        "negative_delta_count": len(negative),
        "minimum_absolute_delta": min((item["absolute_delta"] for item in negative), default=None),
        "minimum_relative_delta": min((item["relative_delta"] for item in negative), default=None),
        "negative_delta_terminal_count": sum(bool(item["terminal"]) for item in negative),
    }


def _apply_minimum_self_transition(transmat: np.ndarray, *, field: str) -> np.ndarray:
    smoothed = _transition_matrix(transmat, field, 3).copy()
    for state in range(3):
        if smoothed[state, state] < HMM_MIN_SELF_TRANSITION:
            old_self = float(smoothed[state, state])
            other_sum = float(smoothed[state].sum() - old_self)
            smoothed[state, state] = HMM_MIN_SELF_TRANSITION
            remaining = 1.0 - HMM_MIN_SELF_TRANSITION
            if other_sum <= 0:
                smoothed[state, [item for item in range(3) if item != state]] = remaining / 2.0
            else:
                for target in range(3):
                    if target != state:
                        smoothed[state, target] = smoothed[state, target] / other_sum * remaining
        smoothed[state] /= smoothed[state].sum()
    return _transition_matrix(smoothed, f"{field} with self-transition floor", 3)


def _smooth_transition_matrix(transmat: np.ndarray) -> np.ndarray:
    values = _transition_matrix(transmat, "fitted transmat", 3)
    smoothed = (values + HMM_TRANSITION_ALPHA) / (1.0 + HMM_TRANSITION_ALPHA * 3.0)
    return _apply_minimum_self_transition(smoothed, field="smoothed transmat")


def _validation_state_statistics(
    posteriors: np.ndarray, utility: np.ndarray
) -> tuple[dict[str, int], dict[str, float | None]]:
    states = np.asarray(posteriors, dtype=np.float64).argmax(axis=1)
    utility_values = np.asarray(utility, dtype=np.float64)
    counts: dict[str, int] = {}
    scores: dict[str, float | None] = {}
    for state in range(3):
        values = utility_values[states == state]
        counts[str(state)] = int(values.size)
        scores[str(state)] = float(values.mean()) if values.size and np.isfinite(values).all() else None
    return counts, scores


def _posterior_state_evidence(posteriors: np.ndarray, utility: np.ndarray | None = None) -> dict[str, Any]:
    """Return diagnostic-only hard/soft state evidence without applying pass thresholds."""

    probabilities = np.asarray(posteriors, dtype=np.float64)
    if probabilities.ndim != 2 or probabilities.shape[1] != 3 or probabilities.shape[0] == 0:
        raise StateModelSetError("C-008-B1 posterior evidence requires non-empty Nx3 posteriors")
    finite_mask = np.isfinite(probabilities)
    row_sums = probabilities.sum(axis=1)
    hard_states = probabilities.argmax(axis=1)
    utility_values: np.ndarray | None = None
    if utility is not None:
        utility_values = np.asarray(utility, dtype=np.float64)
        if utility_values.shape != (probabilities.shape[0],):
            raise StateModelSetError("C-008-B1 utility rows must match posterior rows")

    states: dict[str, Any] = {}
    soft_utilities: dict[str, float | None] = {}
    hard_utilities: dict[str, float | None] = {}
    for state in range(3):
        weights = probabilities[:, state]
        mass = float(weights.sum())
        squared_mass = float(np.square(weights).sum())
        effective_sample_size = mass * mass / squared_mass if squared_mass > 0.0 else 0.0
        hard_values = utility_values[hard_states == state] if utility_values is not None else None
        hard_utility = (
            float(hard_values.mean())
            if hard_values is not None and hard_values.size and np.isfinite(hard_values).all()
            else None
        )
        weighted_utility: float | None = None
        weighted_variance: float | None = None
        standard_error: float | None = None
        if (
            utility_values is not None
            and mass > 0.0
            and np.isfinite(weights).all()
            and np.isfinite(utility_values).all()
        ):
            weighted_utility = float(np.dot(weights, utility_values) / mass)
            weighted_variance = float(np.dot(weights, np.square(utility_values - weighted_utility)) / mass)
            standard_error = (
                math.sqrt(max(weighted_variance, 0.0) / effective_sample_size) if effective_sample_size > 0.0 else None
            )
        key = str(state)
        states[key] = {
            "hard_count": int((hard_states == state).sum()),
            "posterior_mass": mass,
            "normalized_mass_ratio": mass / probabilities.shape[0],
            "effective_sample_size": effective_sample_size,
            "hard_utility": hard_utility,
            "posterior_weighted_utility": weighted_utility,
            "posterior_weighted_variance": weighted_variance,
            "posterior_weighted_standard_error": standard_error,
            "hard_soft_utility_delta": (
                weighted_utility - hard_utility if weighted_utility is not None and hard_utility is not None else None
            ),
        }
        soft_utilities[key] = weighted_utility
        hard_utilities[key] = hard_utility

    pair_separation: dict[str, float | None] = {}
    for left, right in ((0, 1), (0, 2), (1, 2)):
        left_value = soft_utilities[str(left)]
        right_value = soft_utilities[str(right)]
        pair_separation[f"{left}-{right}"] = (
            abs(left_value - right_value) if left_value is not None and right_value is not None else None
        )
    safe_probabilities = np.clip(probabilities, np.finfo(np.float64).tiny, 1.0)
    entropy = -np.sum(probabilities * np.log(safe_probabilities), axis=1)
    ordered = np.sort(probabilities, axis=1)
    margins = ordered[:, -1] - ordered[:, -2]
    return {
        "row_count": int(probabilities.shape[0]),
        "posterior_non_finite_count": int((~finite_mask).sum()),
        "posterior_negative_count": int((probabilities < 0.0).sum()),
        "row_sum_max_abs_error": float(np.max(np.abs(row_sums - 1.0))),
        "row_sum_mean_abs_error": float(np.mean(np.abs(row_sums - 1.0))),
        "entropy": {
            "min": float(entropy.min()),
            "max": float(entropy.max()),
            "mean": float(entropy.mean()),
        },
        "top1_top2_margin": {
            "min": float(margins.min()),
            "max": float(margins.max()),
            "mean": float(margins.mean()),
        },
        "states": states,
        "posterior_weighted_utility_pair_separation": pair_separation,
    }


def _posterior_time_segment_evidence(posteriors: np.ndarray, utility: np.ndarray) -> list[dict[str, Any]]:
    indices = np.array_split(np.arange(np.asarray(posteriors).shape[0]), 3)
    output: list[dict[str, Any]] = []
    for segment_number, segment in enumerate(indices, start=1):
        output.append(
            {
                "segment": segment_number,
                "start_row": int(segment[0]),
                "end_row_inclusive": int(segment[-1]),
                "evidence": _posterior_state_evidence(posteriors[segment], utility[segment]),
            }
        )
    return output


def _hard_sequence_evidence(
    posteriors: np.ndarray,
    dates: Sequence[date],
    *,
    utility: np.ndarray | None = None,
) -> dict[str, Any]:
    """Collect threshold-free hard occupancy, run, transition, month, and utility evidence."""

    probabilities = np.asarray(posteriors, dtype=np.float64)
    date_values = tuple(dates)
    if probabilities.ndim != 2 or probabilities.shape[1] != 3 or probabilities.shape[0] != len(date_values):
        raise StateModelSetError("C-008-B3-DIAG-02 hard sequence dates must align with Nx3 posteriors")
    if tuple(sorted(date_values)) != date_values or len(set(date_values)) != len(date_values):
        raise StateModelSetError("C-008-B3-DIAG-02 hard sequence dates must be strictly increasing")
    hard_states = probabilities.argmax(axis=1).astype(np.int64)
    transitions = np.zeros((3, 3), dtype=np.int64)
    if hard_states.size > 1:
        np.add.at(transitions, (hard_states[:-1], hard_states[1:]), 1)
    runs: dict[int, list[int]] = {state: [] for state in range(3)}
    run_state = int(hard_states[0])
    run_length = 1
    for value in hard_states[1:]:
        state = int(value)
        if state == run_state:
            run_length += 1
        else:
            runs[run_state].append(run_length)
            run_state = state
            run_length = 1
    runs[run_state].append(run_length)
    utility_values = None if utility is None else np.asarray(utility, dtype=np.float64)
    if utility_values is not None and utility_values.shape != (hard_states.size,):
        raise StateModelSetError("C-008-B3-DIAG-02 utility rows must align with hard states")
    states: dict[str, Any] = {}
    hard_means: dict[int, float] = {}
    for state in range(3):
        mask = hard_states == state
        count = int(mask.sum())
        lengths = runs[state]
        state_dates = [date_values[index] for index in np.flatnonzero(mask)]
        months = sorted({value.strftime("%Y-%m") for value in state_dates})
        state_utility = utility_values[mask] if utility_values is not None else None
        utility_evidence = None
        if state_utility is not None:
            finite = bool(np.isfinite(state_utility).all())
            mean = float(state_utility.mean()) if state_utility.size and finite else None
            if mean is not None:
                hard_means[state] = mean
            utility_evidence = {
                "count": int(state_utility.size),
                "finite": finite,
                "mean": mean,
                "sample_variance_ddof_1": (
                    float(np.var(state_utility, ddof=1)) if state_utility.size >= 2 and finite else None
                ),
                "min": float(state_utility.min()) if state_utility.size and finite else None,
                "max": float(state_utility.max()) if state_utility.size and finite else None,
            }
        states[str(state)] = {
            "hard_count": count,
            "normalized_occupancy": count / hard_states.size,
            "contiguous_run_count": len(lengths),
            "run_length_q0_median_max": (
                [int(min(lengths)), float(np.median(lengths)), int(max(lengths))] if lengths else None
            ),
            "maximum_run_share_of_state_count": max(lengths) / count if count and lengths else None,
            "incoming_transition_count": int(transitions[:, state].sum() - transitions[state, state]),
            "outgoing_transition_count": int(transitions[state, :].sum() - transitions[state, state]),
            "calendar_month_count": len(months),
            "calendar_months": months,
            "hard_utility": utility_evidence,
        }
    ordered_states = sorted(hard_means, key=hard_means.get) if len(hard_means) == 3 else []
    adjacent_gaps = None
    if ordered_states:
        adjacent_gaps = [
            hard_means[ordered_states[index + 1]] - hard_means[ordered_states[index]] for index in range(2)
        ]
    return {
        "schema_version": "hmm_risk_c008_b3_diag02_hard_sequence_evidence_v1",
        "thresholds_applied": False,
        "row_count": int(hard_states.size),
        "date_start": date_values[0].isoformat(),
        "date_end": date_values[-1].isoformat(),
        "date_sha256": canonical_sha256([value.isoformat() for value in date_values]),
        "hard_assignment_sha256": canonical_sha256(hard_states.tolist()),
        "transition_counts": transitions.tolist(),
        "states": states,
        "hard_utility_order_diagnostic": [str(state) for state in ordered_states] if ordered_states else None,
        "adjacent_hard_utility_gaps_diagnostic": adjacent_gaps,
        "posterior": _posterior_state_evidence(probabilities, utility_values),
    }


def _b3_diag02_covariance_evidence(
    raw_covars: np.ndarray,
    *,
    expected_shape: tuple[int, int],
    family_feature_variance: np.ndarray,
) -> dict[str, Any]:
    diagnostic = _covariance_diagnostic(raw_covars, expected_shape=expected_shape)
    values = np.asarray(raw_covars, dtype=np.float64)
    feature_variance = np.asarray(family_feature_variance, dtype=np.float64)
    ratios = values / feature_variance[None, :] if values.shape == expected_shape else None
    raw_values = (
        [[float(value) if math.isfinite(float(value)) else None for value in row] for row in values]
        if values.shape == expected_shape
        else None
    )
    ratio_values = (
        [[float(value) if math.isfinite(float(value)) else None for value in row] for row in ratios]
        if ratios is not None
        else None
    )
    return {
        **diagnostic,
        "formal_bounds_applied": False,
        "formal_anomaly_budget_applied": False,
        "postfit_projection_performed": False,
        "family_feature_variance_ddof_0": feature_variance.tolist(),
        "raw_covars": raw_values,
        "raw_to_family_feature_variance_ratio": ratio_values,
    }


def _labels_from_validation(
    posteriors: np.ndarray, utility: np.ndarray, sector_code: str
) -> tuple[dict[str, str], dict[str, float]]:
    counts, raw_scores = _validation_state_statistics(posteriors, utility)
    scores: dict[int, float] = {}
    for state in range(3):
        value = raw_scores[str(state)]
        if counts[str(state)] == 0 or value is None:
            raise StateModelSetError(f"{sector_code} semantic label evidence is missing for hidden state {state}")
        scores[state] = value
    ordered = sorted(scores, key=lambda state: scores[state])
    ordered_values = [scores[state] for state in ordered]
    if not ordered_values[0] < ordered_values[1] < ordered_values[2]:
        raise StateModelSetError(f"{sector_code} semantic utility tie is not labelable without fallback")
    labels = {str(ordered[0]): "fading", str(ordered[1]): "neutral", str(ordered[2]): "trending"}
    return labels, {str(state): scores[state] for state in range(3)}


@dataclass(frozen=True)
class _L1FitEvidence:
    train: np.ndarray
    validation: np.ndarray
    startprob: np.ndarray
    transmat: np.ndarray
    means: np.ndarray
    raw_covars: np.ndarray
    covars: np.ndarray
    posteriors: np.ndarray
    covariance_anomaly_count: int
    covariance_diagnostic: dict[str, Any]
    monitor_converged: bool
    monitor_iterations: int
    monitor_history: tuple[float, ...]
    monitor_diagnostic: dict[str, Any]


@dataclass(frozen=True)
class _B3Diag02FitEvidence:
    train: np.ndarray
    validation: np.ndarray
    train_posteriors: np.ndarray
    validation_posteriors: np.ndarray
    startprob: np.ndarray
    transmat: np.ndarray
    means: np.ndarray
    raw_covars: np.ndarray
    initialization_evidence: dict[str, Any]
    monitor_evidence: dict[str, Any]
    covariance_evidence: dict[str, Any]
    model_numeric_payload_sha256: str


def _manual_b3_diag02_initialization(
    train: np.ndarray,
    *,
    random_seed: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    try:
        from sklearn.cluster import KMeans
    except ImportError as exc:  # pragma: no cover - dependency contract reports this explicitly.
        raise StateModelSetError("scikit-learn is required for C-008-B3-DIAG-02 initialization") from exc
    kmeans = KMeans(
        n_clusters=3,
        init="k-means++",
        n_init=1,
        random_state=random_seed,
        max_iter=300,
        tol=1e-4,
        algorithm="lloyd",
        copy_x=True,
    )
    labels = np.asarray(kmeans.fit_predict(train), dtype=np.int64)
    counts = np.bincount(labels, minlength=3)
    if counts.shape != (3,) or np.any(counts < 2):
        raise _L1FitDiagnosticError(
            "C-008-B3-DIAG-02 KMeans requires three clusters with at least two rows each",
            stage="manual_kmeans_initialization",
            evidence={"cluster_counts": counts.astype(int).tolist()},
        )
    means = _finite_array(kmeans.cluster_centers_, "C-008-B3-DIAG-02 initial means", ndim=2)
    raw_covars = np.vstack([np.var(train[labels == state], axis=0, ddof=0) for state in range(3)])
    if raw_covars.shape != means.shape or not np.isfinite(raw_covars).all():
        raise _L1FitDiagnosticError(
            "C-008-B3-DIAG-02 KMeans covariance initialization is invalid",
            stage="manual_kmeans_initialization",
            evidence={"cluster_counts": counts.astype(int).tolist()},
        )
    initial_anomaly_mask = (raw_covars < HMM_MIN_COVAR) | (raw_covars > HMM_MAX_COVAR)
    covars = np.clip(raw_covars, HMM_MIN_COVAR, HMM_MAX_COVAR)
    transition_counts = np.zeros((3, 3), dtype=np.float64)
    np.add.at(transition_counts, (labels[:-1], labels[1:]), 1.0)
    transmat = (transition_counts + HMM_TRANSITION_ALPHA) / (
        transition_counts.sum(axis=1, keepdims=True) + HMM_TRANSITION_ALPHA * 3.0
    )
    transmat = _apply_minimum_self_transition(transmat, field="C-008-B3-DIAG-02 initial transmat")
    startprob = np.full(3, 1.0 / 3.0, dtype=np.float64)
    evidence = {
        "schema_version": "hmm_risk_c008_b3_diag02_manual_initialization_v1",
        "thresholds_applied": False,
        "random_seed": random_seed,
        "kmeans_parameters": c008_b3_diag02_parameter_profile()["kmeans"],
        "kmeans_iterations": int(kmeans.n_iter_),
        "kmeans_inertia": float(kmeans.inertia_),
        "cluster_counts": counts.astype(int).tolist(),
        "cluster_label_sha256": canonical_sha256(labels.tolist()),
        "means": means.tolist(),
        "raw_diag_covars_ddof_0": raw_covars.tolist(),
        "bounded_initial_diag_covars": covars.tolist(),
        "initial_covariance_anomaly_count": int(initial_anomaly_mask.sum()),
        "initial_covariance_anomaly_mask_sha256": canonical_sha256(initial_anomaly_mask.astype(np.uint8).tolist()),
        "transition_counts": transition_counts.astype(int).tolist(),
        "startprob": startprob.tolist(),
        "transmat": transmat.tolist(),
    }
    return startprob, transmat, means, covars, evidence


def _fit_l1_b3_diag02_evidence(
    item: L1TrainingSeries,
    *,
    preprocess: Mapping[str, Any],
    feature_count: int,
    family_feature_variance: np.ndarray,
    random_seed: int,
) -> _B3Diag02FitEvidence:
    try:
        from hmmlearn.hmm import GaussianHMM
    except ImportError as exc:  # pragma: no cover - production dependency gate reports this explicitly.
        raise StateModelSetError("hmmlearn is required for C-008-B3-DIAG-02") from exc
    code = item.sector_code
    train = _apply_preprocess(item.train_observations, preprocess)
    validation = _apply_preprocess(item.validation_observations, preprocess)
    startprob, transmat, means, covars, initialization = _manual_b3_diag02_initialization(
        train,
        random_seed=random_seed,
    )
    model = GaussianHMM(
        n_components=3,
        covariance_type="diag",
        min_covar=HMM_MIN_COVAR,
        startprob_prior=1.0,
        transmat_prior=1.0,
        means_prior=0.0,
        means_weight=0.0,
        covars_prior=0.01,
        covars_weight=1.0,
        algorithm="viterbi",
        random_state=random_seed,
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
    model.covars_ = covars.copy()
    try:
        model.fit(train)
    except Exception as exc:
        raise _L1FitDiagnosticError(
            f"C-008-B3-DIAG-02 model training failed for {code}: {exc}",
            stage="model_fit",
            evidence={"initialization": initialization, "error_type": type(exc).__name__, "error": str(exc)},
        ) from exc
    monitor = _monitor_diagnostic(model)
    raw_covars = np.asarray(model._covars_, dtype=np.float64)
    covariance = _b3_diag02_covariance_evidence(
        raw_covars,
        expected_shape=(3, feature_count),
        family_feature_variance=family_feature_variance,
    )
    if not covariance["valid_for_bounding"]:
        raise _L1FitDiagnosticError(
            f"C-008-B3-DIAG-02 raw covariance is invalid for {code}",
            stage="raw_covariance_validation",
            evidence={"initialization": initialization, "monitor": monitor, "covariance": covariance},
        )
    try:
        fitted_startprob = _probability_vector(model.startprob_, f"{code}.diag02.startprob", 3)
        fitted_transmat = _transition_matrix(model.transmat_, f"{code}.diag02.transmat", 3)
        fitted_means = _finite_array(model.means_, f"{code}.diag02.means", ndim=2)
        train_posteriors = causal_forward_posteriors(
            train,
            startprob=fitted_startprob,
            transmat=fitted_transmat,
            means=fitted_means,
            covars=raw_covars,
        )
        validation_posteriors = causal_forward_posteriors(
            validation,
            startprob=fitted_startprob,
            transmat=fitted_transmat,
            means=fitted_means,
            covars=raw_covars,
        )
    except StateModelSetError as exc:
        raise _L1FitDiagnosticError(
            str(exc),
            stage="parameter_or_posterior_validation",
            evidence={"initialization": initialization, "monitor": monitor, "covariance": covariance},
        ) from exc
    numeric_payload = {
        "parameter_profile_hash": canonical_sha256(c008_b3_diag02_parameter_profile()),
        "startprob": _float64_array_identity(fitted_startprob),
        "transmat": _float64_array_identity(fitted_transmat),
        "means": _float64_array_identity(fitted_means),
        "raw_covars": _float64_array_identity(raw_covars),
        "monitor_history": _float64_array_identity(tuple(float(value) for value in model.monitor_.history)),
        "train_posteriors": _float64_array_identity(train_posteriors),
        "validation_posteriors": _float64_array_identity(validation_posteriors),
    }
    return _B3Diag02FitEvidence(
        train=train,
        validation=validation,
        train_posteriors=train_posteriors,
        validation_posteriors=validation_posteriors,
        startprob=fitted_startprob,
        transmat=fitted_transmat,
        means=fitted_means,
        raw_covars=raw_covars,
        initialization_evidence=initialization,
        monitor_evidence=monitor,
        covariance_evidence=covariance,
        model_numeric_payload_sha256=canonical_sha256(numeric_payload),
    )


def _fit_l1_evidence(
    item: L1TrainingSeries,
    *,
    preprocess: Mapping[str, Any],
    feature_count: int,
    random_seed: int,
) -> _L1FitEvidence:
    try:
        from hmmlearn.hmm import GaussianHMM
    except ImportError as exc:  # pragma: no cover - production dependency gate reports this explicitly.
        raise StateModelSetError("hmmlearn is required for controlled L1 artifact preparation") from exc

    code = item.sector_code
    train = _apply_preprocess(item.train_observations, preprocess)
    validation = _apply_preprocess(item.validation_observations, preprocess)
    model = GaussianHMM(
        n_components=3,
        covariance_type="diag",
        n_iter=HMM_N_ITER,
        min_covar=HMM_MIN_COVAR,
        random_state=random_seed,
    )
    try:
        model.fit(train)
    except Exception as exc:
        raise _L1FitDiagnosticError(
            f"L1 model training failed for {code}: {exc}",
            stage="model_fit",
            evidence={"error_type": type(exc).__name__, "error": str(exc)},
        ) from exc
    monitor_diagnostic = _monitor_diagnostic(model)
    monitor_converged = bool(monitor_diagnostic["converged"])
    monitor_history = tuple(float(value) for value in model.monitor_.history)
    if not monitor_converged:
        raise _L1FitDiagnosticError(
            f"L1 model training did not converge for {code}",
            stage="monitor_not_converged",
            evidence={"monitor": monitor_diagnostic},
        )
    raw_covariance_source = model._covars_
    covariance_diagnostic = _covariance_diagnostic(raw_covariance_source, expected_shape=(3, feature_count))
    if not covariance_diagnostic["valid_for_bounding"]:
        raise _L1FitDiagnosticError(
            f"L1 model covariance is invalid for {code}",
            stage="raw_covariance_validation",
            evidence={"monitor": monitor_diagnostic, "covariance": covariance_diagnostic},
        )
    raw_covars = np.asarray(raw_covariance_source, dtype=np.float64)
    covars, covariance_anomaly_count = _bounded_diag_covariance(raw_covars)
    covariance_diagnostic = {
        **covariance_diagnostic,
        "clip_performed": covariance_anomaly_count > 0,
        "bounded_min": float(covars.min()),
        "bounded_max": float(covars.max()),
    }
    try:
        startprob = _probability_vector(model.startprob_, f"{code}.startprob", 3)
        transmat = _smooth_transition_matrix(model.transmat_)
        means = _finite_array(model.means_, f"{code}.means", ndim=2)
        posteriors = causal_forward_posteriors(
            validation,
            startprob=startprob,
            transmat=transmat,
            means=means,
            covars=covars,
        )
    except StateModelSetError as exc:
        raise _L1FitDiagnosticError(
            str(exc),
            stage="parameter_or_posterior_validation",
            evidence={"monitor": monitor_diagnostic, "covariance": covariance_diagnostic},
        ) from exc
    return _L1FitEvidence(
        train=train,
        validation=validation,
        startprob=startprob,
        transmat=transmat,
        means=means,
        raw_covars=raw_covars,
        covars=covars,
        posteriors=posteriors,
        covariance_anomaly_count=covariance_anomaly_count,
        covariance_diagnostic=covariance_diagnostic,
        monitor_converged=monitor_converged,
        monitor_iterations=int(model.monitor_.iter),
        monitor_history=monitor_history,
        monitor_diagnostic=monitor_diagnostic,
    )


def _diagnose_l1_seed_grid(
    series: Mapping[str, L1TrainingSeries],
    *,
    feature_names: Sequence[str],
    preprocess_family: str,
    seeds: Sequence[int] = C008_DIAGNOSTIC_SEEDS,
    include_b1_evidence: bool,
) -> dict[str, Any]:
    """Record the fixed C-008 grid without choosing a seed or building an artifact."""

    expected_codes = tuple(sorted(series))
    if len(expected_codes) != EXPECTED_L1_COUNT:
        raise StateModelSetError(f"L1 diagnostic requires exactly 31 sectors; actual={len(expected_codes)}")
    features = tuple(str(item) for item in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("L1 diagnostic feature definition is not an approved 7/20-dimensional family")
    diagnostic_seeds = tuple(int(seed) for seed in seeds)
    if diagnostic_seeds != C008_DIAGNOSTIC_SEEDS:
        raise StateModelSetError(f"C-008 diagnostic seeds must be exactly {C008_DIAGNOSTIC_SEEDS}")
    for item in series.values():
        item.validate(len(features))
    preprocess = _fit_preprocess(series, preprocess_family=preprocess_family)

    seed_results: dict[str, Any] = {}
    for seed in diagnostic_seeds:
        sectors: dict[str, Any] = {}
        for code in expected_codes:
            item = series[code]
            try:
                evidence = _fit_l1_evidence(
                    item,
                    preprocess=preprocess,
                    feature_count=len(features),
                    random_seed=seed,
                )
            except StateModelSetError as exc:
                failure = {
                    "status": "fit_failed",
                    "error": str(exc),
                    "training_rows": int(item.train_observations.shape[0]),
                    "validation_rows": int(item.validation_observations.shape[0]),
                }
                if include_b1_evidence:
                    failure.update(
                        {
                            "failure_stage": getattr(exc, "stage", "unclassified_state_model_set_error"),
                            "failure_evidence": getattr(exc, "evidence", {}),
                        }
                    )
                sectors[code] = failure
                continue
            counts, raw_utilities = _validation_state_statistics(
                evidence.posteriors,
                item.validation_future_utility,
            )
            labels: dict[str, str] | None = None
            utilities: dict[str, float] | None = None
            semantic_error: str | None = None
            try:
                labels, utilities = _labels_from_validation(
                    evidence.posteriors,
                    item.validation_future_utility,
                    code,
                )
            except StateModelSetError as exc:
                semantic_error = str(exc)
            deltas = [
                evidence.monitor_history[index] - evidence.monitor_history[index - 1]
                for index in range(1, len(evidence.monitor_history))
            ]
            negative_deltas = [value for value in deltas if value < 0.0]
            sector_record = {
                "status": "labelable" if semantic_error is None else "semantic_unlabelable",
                "semantic_error": semantic_error,
                "state_counts": counts,
                "state_utilities": raw_utilities,
                "state_labels": labels,
                "strict_state_utilities": utilities,
                "monitor_converged": evidence.monitor_converged,
                "monitor_iterations": evidence.monitor_iterations,
                "monitor_history": [value if math.isfinite(value) else None for value in evidence.monitor_history],
                "negative_likelihood_delta_count": len(negative_deltas),
                "minimum_likelihood_delta": min(negative_deltas) if negative_deltas else None,
                "final_training_log_likelihood": (
                    evidence.monitor_history[-1]
                    if evidence.monitor_history and math.isfinite(evidence.monitor_history[-1])
                    else None
                ),
                "training_rows": int(evidence.train.shape[0]),
                "validation_rows": int(evidence.validation.shape[0]),
                "covariance_anomaly_count": evidence.covariance_anomaly_count,
                "covariance_min_after": float(evidence.covars.min()),
                "covariance_max_after": float(evidence.covars.max()),
            }
            if include_b1_evidence:
                sector_record.update(
                    {
                        "diagnostic_algorithm_version": C008_B1_DIAGNOSTIC_VERSION,
                        "train_posterior_evidence": _posterior_state_evidence(
                            causal_forward_posteriors(
                                evidence.train,
                                startprob=evidence.startprob,
                                transmat=evidence.transmat,
                                means=evidence.means,
                                covars=evidence.covars,
                            )
                        ),
                        "validation_posterior_evidence": _posterior_state_evidence(
                            evidence.posteriors,
                            item.validation_future_utility,
                        ),
                        "validation_time_segment_evidence": _posterior_time_segment_evidence(
                            evidence.posteriors,
                            item.validation_future_utility,
                        ),
                        "convergence_evidence": evidence.monitor_diagnostic,
                        "covariance_evidence": evidence.covariance_diagnostic,
                    }
                )
            sectors[code] = sector_record
        values = list(sectors.values())
        seed_results[str(seed)] = {
            "seed": seed,
            "sector_count": len(sectors),
            "fit_failed_count": sum(item["status"] == "fit_failed" for item in values),
            "semantic_unlabelable_count": sum(item["status"] == "semantic_unlabelable" for item in values),
            "labelable_count": sum(item["status"] == "labelable" for item in values),
            "negative_likelihood_delta_sector_count": sum(
                int(item.get("negative_likelihood_delta_count") or 0) > 0 for item in values
            ),
            "sectors": sectors,
        }
    report = {
        "schema_version": (
            "hmm_risk_l1_seed_diagnostic_b1_v1" if include_b1_evidence else "hmm_risk_l1_seed_diagnostic_v1"
        ),
        "diagnostic_contract": "C-008-B1" if include_b1_evidence else "C-008-A",
        "seeds": list(diagnostic_seeds),
        "selection_performed": False,
        "artifact_write_performed": False,
        "feature_names": list(features),
        "preprocess": preprocess,
        "expected_sector_set_hash": canonical_sha256(expected_codes),
        "seed_results": seed_results,
    }
    if include_b1_evidence:
        report["diagnostic_algorithm_version"] = C008_B1_DIAGNOSTIC_VERSION
    return report


def diagnose_l1_seed_grid(
    series: Mapping[str, L1TrainingSeries],
    *,
    feature_names: Sequence[str],
    preprocess_family: str,
    seeds: Sequence[int] = C008_DIAGNOSTIC_SEEDS,
) -> dict[str, Any]:
    """Record C-008-A seed facts without choosing a seed or building an artifact."""

    return _diagnose_l1_seed_grid(
        series,
        feature_names=feature_names,
        preprocess_family=preprocess_family,
        seeds=seeds,
        include_b1_evidence=False,
    )


def diagnose_l1_seed_grid_b1(
    series: Mapping[str, L1TrainingSeries],
    *,
    feature_names: Sequence[str],
    preprocess_family: str,
    seeds: Sequence[int] = C008_DIAGNOSTIC_SEEDS,
) -> dict[str, Any]:
    """Record approved C-008-B1 soft/numeric evidence without changing hard-label semantics."""

    return _diagnose_l1_seed_grid(
        series,
        feature_names=feature_names,
        preprocess_family=preprocess_family,
        seeds=seeds,
        include_b1_evidence=True,
    )


def diagnose_l1_seed_grid_b3_diag02(
    series: Mapping[str, L1TrainingSeries],
    *,
    feature_names: Sequence[str],
    preprocess_family: str,
    seeds: Sequence[int] = C008_DIAGNOSTIC_SEEDS,
) -> dict[str, Any]:
    """Collect structural B3 evidence without applying D4/D5-01/D6 gates or selecting a restart."""

    expected_codes = tuple(sorted(series))
    if len(expected_codes) != EXPECTED_L1_COUNT:
        raise StateModelSetError(f"C-008-B3-DIAG-02 requires exactly 31 sectors; actual={len(expected_codes)}")
    features = tuple(str(item) for item in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("C-008-B3-DIAG-02 feature definition must be the approved 7/20-dimensional family")
    diagnostic_seeds = tuple(int(seed) for seed in seeds)
    if diagnostic_seeds != C008_DIAGNOSTIC_SEEDS:
        raise StateModelSetError(f"C-008-B3-DIAG-02 seeds must be exactly {C008_DIAGNOSTIC_SEEDS}")
    for item in series.values():
        item.validate(len(features))
    preprocess = _fit_preprocess(series, preprocess_family=preprocess_family)
    family_train = np.vstack(
        [_apply_preprocess(series[code].train_observations, preprocess) for code in expected_codes]
    )
    family_feature_variance = np.var(family_train, axis=0, ddof=0)
    if (
        family_feature_variance.shape != (len(features),)
        or not np.isfinite(family_feature_variance).all()
        or np.any(family_feature_variance <= 0.0)
    ):
        raise StateModelSetError("C-008-B3-DIAG-02 family feature variance must be finite and positive")
    seed_results: dict[str, Any] = {}
    for seed in diagnostic_seeds:
        sectors: dict[str, Any] = {}
        for code in expected_codes:
            item = series[code]
            try:
                evidence = _fit_l1_b3_diag02_evidence(
                    item,
                    preprocess=preprocess,
                    feature_count=len(features),
                    family_feature_variance=family_feature_variance,
                    random_seed=seed,
                )
            except StateModelSetError as exc:
                sectors[code] = {
                    "fit_status": "fit_failed",
                    "failure_stage": getattr(exc, "stage", "unclassified_state_model_set_error"),
                    "error": str(exc),
                    "failure_evidence": getattr(exc, "evidence", {}),
                    "training_rows": int(item.train_observations.shape[0]),
                    "validation_rows": int(item.validation_observations.shape[0]),
                    "formal_acceptance_thresholds_applied": False,
                }
                continue
            history = evidence.monitor_evidence["history"]
            final_likelihood = history[-1] if history and history[-1] is not None else None
            sector_score = (
                final_likelihood / (evidence.train.shape[0] * len(features)) if final_likelihood is not None else None
            )
            sectors[code] = {
                "fit_status": "fit_completed_diagnostic_only",
                "formal_acceptance_thresholds_applied": False,
                "d4_acceptance_evaluated": False,
                "d5_01_selection_score_approved": False,
                "d6_semantic_acceptance_evaluated": False,
                "training_rows": int(evidence.train.shape[0]),
                "validation_rows": int(evidence.validation.shape[0]),
                "initialization_evidence": evidence.initialization_evidence,
                "monitor_evidence": evidence.monitor_evidence,
                "covariance_evidence": evidence.covariance_evidence,
                "train_hard_sequence_evidence": _hard_sequence_evidence(
                    evidence.train_posteriors,
                    item.train_dates,
                ),
                "validation_hard_sequence_evidence": _hard_sequence_evidence(
                    evidence.validation_posteriors,
                    item.validation_dates,
                    utility=item.validation_future_utility,
                ),
                "train_log_likelihood_per_row_dimension_diagnostic": sector_score,
                "model_numeric_payload_sha256": evidence.model_numeric_payload_sha256,
                "fitted_parameter_identities": {
                    "startprob": _float64_array_identity(evidence.startprob),
                    "transmat": _float64_array_identity(evidence.transmat),
                    "means": _float64_array_identity(evidence.means),
                    "raw_covars": _float64_array_identity(evidence.raw_covars),
                },
            }
        completed = [item for item in sectors.values() if item["fit_status"] == "fit_completed_diagnostic_only"]
        observed_scores = [
            float(item["train_log_likelihood_per_row_dimension_diagnostic"])
            for item in completed
            if item["train_log_likelihood_per_row_dimension_diagnostic"] is not None
        ]
        seed_results[str(seed)] = {
            "seed": seed,
            "sector_count": len(sectors),
            "fit_completed_count": len(completed),
            "fit_failed_count": len(sectors) - len(completed),
            "all_31_fits_completed": len(completed) == EXPECTED_L1_COUNT,
            "observed_score_count": len(observed_scores),
            "observed_score_min_median_mean": (
                [min(observed_scores), float(np.median(observed_scores)), float(np.mean(observed_scores))]
                if observed_scores
                else None
            ),
            "family_candidate_eligibility_evaluated": False,
            "selection_performed": False,
            "sectors": sectors,
        }
    return {
        "schema_version": "hmm_risk_l1_c008_b3_diag02_v1",
        "diagnostic_contract": C008_B3_DIAG02_CONTRACT,
        "structural_contract": C008_B3_STRUCTURAL_CONTRACT,
        "diagnostic_algorithm_version": C008_B3_DIAG02_VERSION,
        "parameter_profile": c008_b3_diag02_parameter_profile(),
        "parameter_profile_sha256": canonical_sha256(c008_b3_diag02_parameter_profile()),
        "seeds": list(diagnostic_seeds),
        "all_restarts_completed": True,
        "selection_performed": False,
        "artifact_write_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "formal_acceptance_thresholds_applied": False,
        "hard_semantic_authority_changed": False,
        "validation_accessed_for_selection": False,
        "future_utility_accessed_for_selection": False,
        "d4_exact_contract_approved": False,
        "d5_01_exact_contract_approved": False,
        "d6_exact_contract_approved": False,
        "feature_names": list(features),
        "preprocess": preprocess,
        "family_feature_variance_ddof_0": family_feature_variance.tolist(),
        "expected_sector_set_hash": canonical_sha256(expected_codes),
        "seed_results": seed_results,
    }


def train_l1_models(
    series: Mapping[str, L1TrainingSeries],
    *,
    feature_names: Sequence[str],
    preprocess_family: str,
    random_seed: int,
    observation_version: str,
) -> dict[str, Any]:
    """Train all 31 independent direct L1 models or fail the whole family."""

    expected_codes = tuple(sorted(series))
    if len(expected_codes) != EXPECTED_L1_COUNT:
        raise StateModelSetError(f"L1 training requires exactly 31 sectors; actual={len(expected_codes)}")
    features = tuple(str(item) for item in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("L1 feature definition is not an approved 7/20-dimensional family")
    for item in series.values():
        item.validate(len(features))
    preprocess = _fit_preprocess(series, preprocess_family=preprocess_family)

    models: dict[str, Any] = {}
    for code in expected_codes:
        item = series[code]
        evidence = _fit_l1_evidence(
            item,
            preprocess=preprocess,
            feature_count=len(features),
            random_seed=random_seed,
        )
        labels, utilities = _labels_from_validation(
            evidence.posteriors,
            item.validation_future_utility,
            code,
        )
        prefix = causal_forward_posteriors(
            evidence.validation[:-1],
            startprob=evidence.startprob,
            transmat=evidence.transmat,
            means=evidence.means,
            covars=evidence.covars,
        )
        if not np.allclose(prefix, evidence.posteriors[:-1], atol=1e-12, rtol=0):
            raise StateModelSetError(f"causal prefix replay differs for {code}")
        models[code] = {
            "sector_code": code,
            "sector_name": item.sector_name,
            "sector_level": "L1",
            "state_origin": "direct_hmm",
            "n_states": 3,
            "covariance_type": "diag",
            "feature_names": list(features),
            "startprob": evidence.startprob.tolist(),
            "transmat": evidence.transmat.tolist(),
            "means": evidence.means.tolist(),
            "covars": evidence.covars.tolist(),
            "covariance_fixed": evidence.covariance_anomaly_count > 0,
            "covariance_anomaly_count": evidence.covariance_anomaly_count,
            "covariance_min_after": float(evidence.covars.min()),
            "covariance_max_after": float(evidence.covars.max()),
            "state_labels": labels,
            "state_validation_utilities": utilities,
            "observation_version": observation_version,
            "training_rows": int(evidence.train.shape[0]),
            "validation_rows": int(evidence.validation.shape[0]),
            "pit_l2_constituents": list(item.pit_l2_constituents),
            "pit_constituent_manifest_hash": item.pit_constituent_manifest_hash,
            "observation_manifest_hash": item.observation_manifest_hash,
            "causal_replay": "passed",
        }
    return {
        "schema_version": L1_ARTIFACT_SCHEMA,
        "sector_level": "L1",
        "sector_count": len(models),
        "expected_sector_set_hash": canonical_sha256(expected_codes),
        "feature_names": list(features),
        "preprocess": preprocess,
        "random_seed": random_seed,
        "training_algorithm": {
            "n_states": 3,
            "covariance_type": "diag",
            "n_iter": HMM_N_ITER,
            "min_covar": HMM_MIN_COVAR,
            "max_covar": HMM_MAX_COVAR,
            "transition_alpha": HMM_TRANSITION_ALPHA,
            "min_self_transition": HMM_MIN_SELF_TRANSITION,
            "semantic_label": "validation_future_excess_5_10_20_weights_0.35_0.35_0.30_no_fallback",
        },
        "models": models,
    }


@dataclass(frozen=True)
class StateModelSetSpec:
    family: str
    family_version: str
    producer_commit: str
    created_at: str
    candidate_ids: tuple[str, ...]
    parser_contract: str
    source_l2_artifact_uri: str
    source_l2_artifact_sha256: str
    train_start: date
    train_end: date
    validation_start: date
    validation_end: date
    common_data_watermark: date
    dataset_manifest: Mapping[str, Any]
    mapping_manifest: Mapping[str, Any]
    feature_definition: Mapping[str, Any]
    observation_version: str
    preprocess_family: str
    random_seed: int = 42

    def validate(self) -> None:
        for field, value in (
            ("family", self.family),
            ("family_version", self.family_version),
            ("producer_commit", self.producer_commit),
            ("created_at", self.created_at),
            ("source_l2_artifact_uri", self.source_l2_artifact_uri),
            ("observation_version", self.observation_version),
        ):
            if not str(value).strip():
                raise StateModelSetError(f"{field} is required")
        try:
            parsed_created_at = datetime.fromisoformat(self.created_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise StateModelSetError("created_at must be an explicit ISO timestamp") from exc
        if parsed_created_at.tzinfo is None:
            raise StateModelSetError("created_at must include a timezone")
        if not self.candidate_ids or tuple(sorted(set(self.candidate_ids))) != self.candidate_ids:
            raise StateModelSetError("candidate_ids must be non-empty, sorted and unique")
        if not self.train_start <= self.train_end < self.validation_start <= self.validation_end:
            raise StateModelSetError("train/validation windows are invalid")
        if self.validation_end > self.common_data_watermark:
            raise StateModelSetError("common data watermark does not cover validation")
        _require_sha256(self.source_l2_artifact_sha256, "source_l2_artifact_sha256")


def build_state_model_set(
    *,
    spec: StateModelSetSpec,
    l1_artifact: Mapping[str, Any],
    l2_artifact: Mapping[str, Any],
) -> tuple[dict[str, Any], bytes, bytes]:
    """Build a READY manifest only after both complete direct layers validate."""

    spec.validate()
    if l1_artifact.get("schema_version") != L1_ARTIFACT_SCHEMA or l1_artifact.get("sector_count") != EXPECTED_L1_COUNT:
        raise StateModelSetError("L1 artifact is incomplete")
    if l2_artifact.get("schema_version") != L2_ARTIFACT_SCHEMA or l2_artifact.get("sector_count") != EXPECTED_L2_COUNT:
        raise StateModelSetError("L2 artifact is incomplete")
    if tuple(l1_artifact.get("feature_names") or ()) != tuple(l2_artifact.get("feature_names") or ()):
        raise StateModelSetError("L1/L2 feature families differ")
    if l2_artifact.get("parser_contract") != spec.parser_contract:
        raise StateModelSetError("L2 parser contract differs from the preparation spec")
    if l2_artifact.get("source_artifact_sha256") != spec.source_l2_artifact_sha256:
        raise StateModelSetError("L2 source identity differs from the preparation spec")

    l1_bytes = canonical_json_bytes(l1_artifact)
    l2_bytes = canonical_json_bytes(l2_artifact)
    l1_sha256 = sha256_bytes(l1_bytes)
    l2_sha256 = sha256_bytes(l2_bytes)
    manifest_body = {
        "schema_version": SCHEMA_VERSION,
        "status": "READY",
        "family": spec.family,
        "family_version": spec.family_version,
        "producer_commit": spec.producer_commit,
        "created_at": spec.created_at,
        "candidate_ids": list(spec.candidate_ids),
        "train_start": spec.train_start.isoformat(),
        "train_end": spec.train_end.isoformat(),
        "validation_start": spec.validation_start.isoformat(),
        "validation_end": spec.validation_end.isoformat(),
        "common_data_watermark": spec.common_data_watermark.isoformat(),
        "dataset_manifest": dict(spec.dataset_manifest),
        "dataset_manifest_hash": canonical_sha256(spec.dataset_manifest),
        "mapping_manifest": dict(spec.mapping_manifest),
        "mapping_manifest_hash": canonical_sha256(spec.mapping_manifest),
        "feature_definition": dict(spec.feature_definition),
        "feature_definition_hash": canonical_sha256(spec.feature_definition),
        "preprocess_family": spec.preprocess_family,
        "random_seed": spec.random_seed,
        "observation_version": spec.observation_version,
        "source_l2_artifact_uri": spec.source_l2_artifact_uri,
        "source_l2_artifact_sha256": spec.source_l2_artifact_sha256,
        "layers": {
            "L1": {
                "artifact_uri": f"artifacts/{l1_sha256}.l1.json",
                "artifact_sha256": l1_sha256,
                "size_bytes": len(l1_bytes),
                "parser_contract": L1_ARTIFACT_SCHEMA,
                "sector_level": "L1",
                "sector_count": EXPECTED_L1_COUNT,
                "expected_sector_set_hash": l1_artifact["expected_sector_set_hash"],
            },
            "L2": {
                "artifact_uri": f"artifacts/{l2_sha256}.l2.json",
                "artifact_sha256": l2_sha256,
                "size_bytes": len(l2_bytes),
                "parser_contract": spec.parser_contract,
                "sector_level": "L2",
                "sector_count": EXPECTED_L2_COUNT,
                "expected_sector_set_hash": l2_artifact["expected_sector_set_hash"],
            },
        },
    }
    state_model_set_hash = canonical_sha256(manifest_body)
    manifest = {
        **manifest_body,
        "state_model_set_id": f"hmms_{state_model_set_hash[:24]}",
        "state_model_set_hash": state_model_set_hash,
    }
    return manifest, l1_bytes, l2_bytes


def _write_immutable(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise StateModelSetError(f"immutable artifact collision: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def write_state_model_set(
    output_root: str | Path,
    *,
    manifest: Mapping[str, Any],
    l1_bytes: bytes,
    l2_bytes: bytes,
) -> Path:
    """Atomically write a complete content-addressed set; never write partial READY."""

    root = Path(output_root).resolve()
    if not root.is_absolute():  # pragma: no cover - resolve always returns absolute; documents intent.
        raise StateModelSetError("output_root must resolve to an absolute path")
    if manifest.get("status") != "READY":
        raise StateModelSetError("only a fully validated READY model set can be written")
    set_id = str(manifest.get("state_model_set_id") or "")
    if not set_id.startswith("hmms_"):
        raise StateModelSetError("state_model_set_id is invalid")
    set_root = root / set_id
    l1 = manifest["layers"]["L1"]
    l2 = manifest["layers"]["L2"]
    if sha256_bytes(l1_bytes) != l1["artifact_sha256"] or sha256_bytes(l2_bytes) != l2["artifact_sha256"]:
        raise StateModelSetError("artifact bytes differ from manifest hashes")
    _write_immutable(set_root / l1["artifact_uri"], l1_bytes)
    _write_immutable(set_root / l2["artifact_uri"], l2_bytes)
    manifest_bytes = canonical_json_bytes(manifest)
    manifest_path = set_root / "manifest.json"
    _write_immutable(manifest_path, manifest_bytes)
    return manifest_path
