"""Controlled offline preparation for direct HMM Risk L1/L2 model sets."""

from __future__ import annotations

import hashlib
import json
import math
import os
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
        raise StateModelSetError(
            f"L2 artifact SHA-256 mismatch expected={expected_sha256} actual={actual_sha256}"
        )
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
    validation_observations: np.ndarray
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
    if mean_array.shape != covariance.shape or mean_array.shape[0] != len(start) or values.shape[1] != mean_array.shape[1]:
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


def _smooth_transition_matrix(transmat: np.ndarray) -> np.ndarray:
    values = _transition_matrix(transmat, "fitted transmat", 3)
    smoothed = (values + HMM_TRANSITION_ALPHA) / (1.0 + HMM_TRANSITION_ALPHA * 3.0)
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
    return _transition_matrix(smoothed, "smoothed transmat", 3)


def _validation_state_statistics(posteriors: np.ndarray, utility: np.ndarray) -> tuple[dict[str, int], dict[str, float | None]]:
    states = np.asarray(posteriors, dtype=np.float64).argmax(axis=1)
    utility_values = np.asarray(utility, dtype=np.float64)
    counts: dict[str, int] = {}
    scores: dict[str, float | None] = {}
    for state in range(3):
        values = utility_values[states == state]
        counts[str(state)] = int(values.size)
        scores[str(state)] = float(values.mean()) if values.size and np.isfinite(values).all() else None
    return counts, scores


def _labels_from_validation(posteriors: np.ndarray, utility: np.ndarray, sector_code: str) -> tuple[dict[str, str], dict[str, float]]:
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
    covars: np.ndarray
    posteriors: np.ndarray
    covariance_anomaly_count: int
    monitor_converged: bool
    monitor_iterations: int
    monitor_history: tuple[float, ...]


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
        raise StateModelSetError(f"L1 model training failed for {code}: {exc}") from exc
    monitor_converged = bool(model.monitor_.converged)
    monitor_history = tuple(float(value) for value in model.monitor_.history)
    if not monitor_converged:
        raise StateModelSetError(f"L1 model training did not converge for {code}")
    covars = np.asarray(model._covars_, dtype=np.float64)
    if covars.shape != (3, feature_count) or not np.isfinite(covars).all() or np.any(covars <= 0):
        raise StateModelSetError(f"L1 model covariance is invalid for {code}")
    covars, covariance_anomaly_count = _bounded_diag_covariance(covars)
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
    return _L1FitEvidence(
        train=train,
        validation=validation,
        startprob=startprob,
        transmat=transmat,
        means=means,
        covars=covars,
        posteriors=posteriors,
        covariance_anomaly_count=covariance_anomaly_count,
        monitor_converged=monitor_converged,
        monitor_iterations=int(model.monitor_.iter),
        monitor_history=monitor_history,
    )


def diagnose_l1_seed_grid(
    series: Mapping[str, L1TrainingSeries],
    *,
    feature_names: Sequence[str],
    preprocess_family: str,
    seeds: Sequence[int] = C008_DIAGNOSTIC_SEEDS,
) -> dict[str, Any]:
    """Record C-008-A seed facts without choosing a seed or building an artifact."""

    expected_codes = tuple(sorted(series))
    if len(expected_codes) != EXPECTED_L1_COUNT:
        raise StateModelSetError(f"L1 diagnostic requires exactly 31 sectors; actual={len(expected_codes)}")
    features = tuple(str(item) for item in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("L1 diagnostic feature definition is not an approved 7/20-dimensional family")
    diagnostic_seeds = tuple(int(seed) for seed in seeds)
    if diagnostic_seeds != C008_DIAGNOSTIC_SEEDS:
        raise StateModelSetError(f"C-008-A diagnostic seeds must be exactly {C008_DIAGNOSTIC_SEEDS}")
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
                sectors[code] = {
                    "status": "fit_failed",
                    "error": str(exc),
                    "training_rows": int(item.train_observations.shape[0]),
                    "validation_rows": int(item.validation_observations.shape[0]),
                }
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
            sectors[code] = {
                "status": "labelable" if semantic_error is None else "semantic_unlabelable",
                "semantic_error": semantic_error,
                "state_counts": counts,
                "state_utilities": raw_utilities,
                "state_labels": labels,
                "strict_state_utilities": utilities,
                "monitor_converged": evidence.monitor_converged,
                "monitor_iterations": evidence.monitor_iterations,
                "monitor_history": list(evidence.monitor_history),
                "negative_likelihood_delta_count": len(negative_deltas),
                "minimum_likelihood_delta": min(negative_deltas) if negative_deltas else None,
                "final_training_log_likelihood": evidence.monitor_history[-1] if evidence.monitor_history else None,
                "training_rows": int(evidence.train.shape[0]),
                "validation_rows": int(evidence.validation.shape[0]),
                "covariance_anomaly_count": evidence.covariance_anomaly_count,
                "covariance_min_after": float(evidence.covars.min()),
                "covariance_max_after": float(evidence.covars.max()),
            }
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
    return {
        "schema_version": "hmm_risk_l1_seed_diagnostic_v1",
        "diagnostic_contract": "C-008-A",
        "seeds": list(diagnostic_seeds),
        "selection_performed": False,
        "artifact_write_performed": False,
        "feature_names": list(features),
        "preprocess": preprocess,
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
