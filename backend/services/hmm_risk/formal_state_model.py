"""Current D3--D6 numerical contract; independent of retired experiment modes.

All training inputs are supplied by the file-only executor.  No repository,
database, runtime registry, or product capability is mutated by this module.
"""

from __future__ import annotations

import hashlib
import math
from datetime import date
from typing import Any, Mapping, Sequence

import numpy as np
from scipy.special import logsumexp

from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, BASE_FEATURES, canonical_sha256

VERSION = "hmm_risk_formal_state_executor_v1"
SEEDS = tuple(range(42, 50))
FAMILIES = ("legacy_covfix", "autocycle_all_core")
CONTRACTS = {
    "d3": "hmm_risk_c008_b3_d3_03_a_v1",
    "map": "hmm_risk_c008_b3_d4_01_map_a_v1",
    "covariance": "hmm_risk_c008_b3_d4_02_a_v1",
    "structure": "hmm_risk_c008_b3_d4_03_persistent_a_v1",
    "selection": "hmm_risk_c008_b3_d5_01_b_v1",
    "semantic": "hmm_risk_c008_b3_d6_01_b_na_a_v1",
}


class FormalStateError(ValueError):
    def __init__(self, reason: str, message: str, *, evidence: Any = None):
        super().__init__(message)
        self.reason_code = reason
        self.evidence = evidence


def receipt(body: Mapping[str, Any]) -> dict[str, Any]:
    return {**body, "receipt_sha256": canonical_sha256(body)}


def array(value: Any, shape: tuple[int, ...], label: str, *, positive: bool = False) -> np.ndarray:
    original = np.asarray(value)
    bool_in_json = isinstance(value, (list, tuple)) and any(
        isinstance(item, (bool, np.bool_)) for item in np.asarray(value, dtype=object).flat
    )
    if original.dtype.kind not in "iuf" or bool_in_json:
        raise FormalStateError("hmm_risk_model_numeric_contract_invalid", f"{label}: real numeric payload required")
    result = np.asarray(original, dtype=np.float64)
    # JSON has no two-dimensional empty array; retain the declared carrier shape.
    if result.shape == (0,) and len(shape) == 2 and shape[0] == 0:
        result = result.reshape(shape)
    if result.shape != shape or not np.isfinite(result).all() or (positive and np.any(result <= 0)):
        raise FormalStateError("hmm_risk_model_numeric_contract_invalid", label)
    return result


def preprocess_fit(values: Sequence[np.ndarray], family: str) -> dict[str, Any]:
    if family not in FAMILIES:
        raise FormalStateError("hmm_risk_model_contract_unsupported", family)
    if family == FAMILIES[0]:
        return {"family": "identity"}
    train = np.vstack(values)
    low, high = np.quantile(train, [0.01, 0.99], axis=0)
    clipped = np.clip(train, low, high)
    center, scale = clipped.mean(axis=0), clipped.std(axis=0, ddof=0)
    if not np.isfinite(clipped).all() or not np.isfinite(scale).all() or np.any(scale <= 0):
        raise FormalStateError("hmm_risk_model_preprocess_invalid", "train-global scale invalid")
    return {
        "family": "winsor_zscore_1_99_train_global_v1",
        "low": low.tolist(),
        "high": high.tolist(),
        "center": center.tolist(),
        "scale": scale.tolist(),
    }


def preprocess_apply(values: np.ndarray, parameters: Mapping[str, Any]) -> np.ndarray:
    if parameters["family"] == "identity":
        return np.array(values, dtype=np.float64, copy=True)
    return (np.clip(values, parameters["low"], parameters["high"]) - parameters["center"]) / parameters["scale"]


def _matrix_sha256(values: np.ndarray) -> str:
    return hashlib.sha256(np.asarray(values, dtype="<f8", order="C").tobytes(order="C")).hexdigest()


def project_training(
    raw: np.ndarray,
    parameters: Mapping[str, Any],
    *,
    family: str,
    level: str,
    sector: str,
    source_receipt_sha256: str,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Apply full preprocess, then the one approved fixed D1 mask.

    This is not automatic variance-based feature selection.  The global family
    feature identity remains 7/20 even when one L2 model uses 19 coordinates.
    """
    features = list(BASE_FEATURES if family == FAMILIES[0] else ALL_CORE_FEATURES)
    if family not in FAMILIES or level not in ("L1", "L2"):
        raise FormalStateError("hmm_risk_model_inactive_dimension_contract_invalid", "projection identity invalid")
    raw = np.asarray(raw)
    if raw.ndim != 2:
        raise FormalStateError("hmm_risk_model_inactive_dimension_contract_invalid", "raw feature matrix required")
    array(raw, (len(raw), len(features)), "full raw train features")
    if (
        not len(raw)
        or not isinstance(source_receipt_sha256, str)
        or len(source_receipt_sha256) != 64
        or any(c not in "0123456789abcdef" for c in source_receipt_sha256)
    ):
        raise FormalStateError("hmm_risk_model_inactive_dimension_contract_invalid", "source profile identity missing")
    inactive = [19] if (family, level, sector) == (FAMILIES[1], "L2", "801207.SI") else []
    if inactive and not np.all(raw[:, 19] == 0.0):
        raise FormalStateError(
            "hmm_risk_model_inactive_dimension_contract_invalid", "allowlisted raw coordinate not exact zero"
        )
    processed = preprocess_apply(raw, parameters)
    array(processed, raw.shape, "full preprocessed features")
    active = [i for i in range(len(features)) if i not in inactive]
    projected = processed[:, active]
    body = {
        "schema_version": "hmm_risk_formal_fixed_projection_v1",
        "algorithm_version": "hmm_risk_c008_b3_d1_inactive_dimension_v2",
        "family": family,
        "level": level,
        "sector_code": sector,
        "feature_names": features,
        "feature_count": len(features),
        "likelihood_feature_count": len(active),
        "active_feature_indices": active,
        "inactive_feature_indices": inactive,
        "active_feature_names": [features[i] for i in active],
        "inactive_feature_names": [features[i] for i in inactive],
        "active_feature_mask": [i in active for i in range(len(features))],
        "preprocess_sha256": canonical_sha256(parameters),
        "source_profile_receipt_sha256": source_receipt_sha256,
        "raw_matrix_sha256": _matrix_sha256(raw),
        "preprocessed_matrix_sha256": _matrix_sha256(processed),
        "raw_inactive_vector_sha256": _matrix_sha256(raw[:, inactive]),
        "expected_preprocessed_inactive_vector_sha256": _matrix_sha256(processed[:, inactive]),
        "observed_preprocessed_inactive_vector_sha256": _matrix_sha256(processed[:, inactive]),
        "preprocessed_matches_approved_transform": True,
        "all_raw_inactive_values_exact_zero": bool(np.all(raw[:, inactive] == 0.0)),
        "projected_matrix_shape": list(projected.shape),
        "projected_matrix_sha256": _matrix_sha256(projected),
        "dynamic_activation": False,
    }
    body["mask_sha256"] = canonical_sha256(body["active_feature_mask"])
    return projected, receipt(body)


def project_validation(raw: np.ndarray, parameters: Mapping[str, Any], projection: Mapping[str, Any]) -> np.ndarray:
    """Full finite O payload precedes preprocess and the frozen training mask."""
    if projection["receipt_sha256"] != canonical_sha256(
        {k: v for k, v in projection.items() if k != "receipt_sha256"}
    ) or projection["preprocess_sha256"] != canonical_sha256(parameters):
        raise FormalStateError(
            "hmm_risk_model_inactive_dimension_contract_invalid", "projection/preprocess hash differs"
        )
    features = list(BASE_FEATURES if projection["family"] == FAMILIES[0] else ALL_CORE_FEATURES)
    inactive = (
        [19]
        if (projection["family"], projection["level"], projection["sector_code"]) == (FAMILIES[1], "L2", "801207.SI")
        else []
    )
    active = [i for i in range(len(features)) if i not in inactive]
    mask = [i in active for i in range(len(features))]
    if (
        projection["schema_version"] != "hmm_risk_formal_fixed_projection_v1"
        or projection["algorithm_version"] != "hmm_risk_c008_b3_d1_inactive_dimension_v2"
        or projection["family"] not in FAMILIES
        or projection["level"] not in ("L1", "L2")
        or projection["feature_names"] != features
        or projection["feature_count"] != len(features)
        or type(projection["feature_count"]) is not int
        or projection["likelihood_feature_count"] != len(active)
        or type(projection["likelihood_feature_count"]) is not int
        or projection["active_feature_indices"] != active
        or projection["inactive_feature_indices"] != inactive
        or any(
            type(i) is not int for i in projection["active_feature_indices"] + projection["inactive_feature_indices"]
        )
        or projection["active_feature_mask"] != mask
        or any(type(value) is not bool for value in projection["active_feature_mask"])
        or projection["active_feature_names"] != [features[i] for i in active]
        or projection["inactive_feature_names"] != [features[i] for i in inactive]
        or projection["mask_sha256"] != canonical_sha256(mask)
        or projection["dynamic_activation"] is not False
    ):
        raise FormalStateError("hmm_risk_model_inactive_dimension_contract_invalid", "fixed mask identity differs")
    raw = array(raw, (len(raw), len(features)), "full validation features")
    if inactive and not np.all(raw[:, 19] == 0.0):
        raise FormalStateError(
            "hmm_risk_model_inactive_dimension_contract_invalid", "validation inactive raw coordinate not exact zero"
        )
    processed = preprocess_apply(raw, parameters)
    array(processed, raw.shape, "full validation preprocess")
    return processed[:, active]


def parameter_profile(seed: int, reference: np.ndarray) -> dict[str, Any]:
    if seed not in SEEDS:
        raise FormalStateError("hmm_risk_model_contract_unsupported", "undeclared seed")
    return dict(
        n_components=3,
        covariance_type="diag",
        min_covar=0.0,
        startprob_prior=1.0,
        transmat_prior=1.0,
        means_prior=0.0,
        means_weight=0.0,
        covars_prior=np.tile(reference, (3, 1)),
        covars_weight=2.0,
        algorithm="viterbi",
        random_state=seed,
        n_iter=300,
        tol=0.01,
        params="stmc",
        init_params="",
        implementation="log",
        verbose=False,
    )


def initialize(values: np.ndarray, seed: int) -> tuple[Any, dict[str, Any]]:
    from hmmlearn.hmm import GaussianHMM
    from sklearn.cluster import KMeans

    n, d = values.shape
    array(values, (n, d), "train observations")
    reference = values.var(axis=0, ddof=0)
    if np.any(reference <= 0) or not np.isfinite(reference).all() or np.any(np.ptp(values, axis=0) == 0):
        raise FormalStateError("hmm_risk_model_initialization_failed", "sector-local variance must be positive")
    km_parameters = dict(
        n_clusters=3,
        init="k-means++",
        n_init=1,
        random_state=seed,
        max_iter=300,
        tol=1e-4,
        algorithm="lloyd",
        copy_x=True,
    )
    km = KMeans(**km_parameters).fit(values)
    counts = np.bincount(km.labels_, minlength=3)
    if np.any(counts < 2):
        raise FormalStateError("hmm_risk_model_initialization_failed", "cluster has fewer than two samples")
    variance = np.asarray([values[km.labels_ == k].var(axis=0, ddof=0) for k in range(3)])
    covariance = (counts[:, None] * variance + reference) / (counts[:, None] + 1.0)
    transitions = np.zeros((3, 3), dtype=np.float64)
    np.add.at(transitions, (km.labels_[:-1], km.labels_[1:]), 1)
    transitions += 0.1
    transitions /= transitions.sum(axis=1, keepdims=True)
    # Approved initializer: floor, then renormalize; not a post-fit projection.
    np.fill_diagonal(transitions, np.maximum(np.diag(transitions), 0.3))
    transitions /= transitions.sum(axis=1, keepdims=True)
    model = GaussianHMM(**parameter_profile(seed, reference))
    model.startprob_ = np.full(3, 1 / 3)
    model.transmat_ = transitions
    model.means_ = km.cluster_centers_
    model.covars_ = covariance
    model._init(values, np.array([n], dtype=np.int64))
    model._check()
    return model, receipt(
        {
            "contract_version": CONTRACTS["d3"],
            "reference": reference.tolist(),
            "nu": 1.0,
            "kmeans_parameters": km_parameters,
            "labels": km.labels_.tolist(),
            "counts": counts.tolist(),
            "cluster_variance": variance.tolist(),
            "initial_covariance": covariance.tolist(),
            "initial_means": km.cluster_centers_.tolist(),
            "initial_transmat": transitions.tolist(),
            "initial_startprob": [1 / 3] * 3,
            "parameters": {
                k: v.tolist() if isinstance(v, np.ndarray) else v for k, v in parameter_profile(seed, reference).items()
            },
            "projection_performed": False,
        }
    )


def covariance_audit(model: Any, values: np.ndarray, reference: np.ndarray) -> dict[str, Any]:
    n, d = values.shape
    covariance = array(model._covars_, (3, d), "raw covariance", positive=True)
    means = array(model.means_, (3, d), "means")
    reference = array(reference, (d,), "sector reference", positive=True)
    likelihood, posterior = model.score_samples(values)
    validate_posterior(posterior, n, require_margin=False)
    mass = posterior.sum(axis=0)
    array(mass, (3,), "posterior mass", positive=True)
    weighted = np.einsum("nk,nkj->kj", posterior, (values[:, None, :] - means) ** 2) / mass[:, None]
    expected = (reference + mass[:, None] * weighted) / (1 + mass[:, None])
    lower = reference / (1 + mass[:, None])
    upper = (1 + n) * reference / (1 + mass[:, None])
    residual = np.abs(covariance - expected) / np.maximum(np.abs(expected), np.finfo(np.float64).tiny)
    anomaly = (covariance < 0.995 * lower) | (covariance > 1.005 * upper)
    reasons = []
    if anomaly.any():
        reasons += ["hmm_risk_model_covariance_bounds_failed", "hmm_risk_model_covariance_anomaly_budget_exceeded"]
    if not np.isfinite(weighted).all() or not np.isfinite(residual).all() or np.max(residual) > 0.02:
        reasons.append("hmm_risk_model_covariance_acceptance_failed")
    return receipt(
        {
            "contract_version": CONTRACTS["covariance"],
            "covariance_valid": not reasons,
            "reasons": reasons,
            "reference": reference.tolist(),
            "raw": covariance.tolist(),
            "mass": mass.tolist(),
            "weighted_variance": weighted.tolist(),
            "expected": expected.tolist(),
            "lower": lower.tolist(),
            "upper": upper.tolist(),
            "residual": residual.tolist(),
            "anomaly": anomaly.tolist(),
            "train_rows": n,
            "smoothed_likelihood": float(likelihood),
            "postfit_projection": False,
        }
    )


def validate_posterior(posterior: Any, n: int, *, require_margin: bool) -> np.ndarray:
    result = array(posterior, (n, 3), "posterior")
    if np.any(result < 0) or np.max(np.abs(result.sum(axis=1) - 1), initial=0) > 1e-12:
        raise FormalStateError("hmm_risk_model_posterior_invalid", "negative or unnormalized posterior")
    if require_margin and n:
        ordered = np.sort(result, axis=1)
        if np.any(ordered[:, -1] - ordered[:, -2] <= 1e-12):
            raise FormalStateError("hmm_risk_model_posterior_tie", "hard argmax tie")
    return result


def model_payload(model: Any) -> dict[str, Any]:
    return {
        "startprob": model.startprob_.tolist(),
        "transmat": model.transmat_.tolist(),
        "means": model.means_.tolist(),
        "covariance": model._covars_.tolist(),
    }


def restore_model(payload: Mapping[str, Any]) -> Any:
    from hmmlearn.hmm import GaussianHMM

    d = np.asarray(payload["means"]).shape[1]
    model = GaussianHMM(n_components=3, covariance_type="diag", init_params="", implementation="log")
    model.startprob_ = array(payload["startprob"], (3,), "startprob")
    model.transmat_ = array(payload["transmat"], (3, 3), "transmat")
    model.means_ = array(payload["means"], (3, d), "means")
    model.covars_ = array(payload["covariance"], (3, d), "covariance", positive=True)
    model.n_features = d
    model._check()
    return model


def causal_filter(model: Any, positions: Sequence[int], values: np.ndarray, total: int) -> np.ndarray:
    if list(positions) != sorted(set(positions)) or any(type(p) is not int or p < 0 or p >= total for p in positions):
        raise FormalStateError("hmm_risk_semantic_calendar_invalid", "observation positions invalid")
    values = array(values, (len(positions), model.means_.shape[1]), "compact observations")
    emission = model._compute_log_likelihood(values) if len(positions) else np.empty((0, 3))
    lookup = {p: i for i, p in enumerate(positions)}
    posterior = np.empty((total, 3))
    with np.errstate(divide="ignore"):
        log_transition = np.log(model.transmat_)
        previous = np.log(model.startprob_)
    for position in range(total):
        prior = previous if position == 0 else logsumexp(previous[:, None] + log_transition, axis=0)
        updated = prior + emission[lookup[position]] if position in lookup else prior
        normalizer = logsumexp(updated)
        if not np.isfinite(normalizer):
            raise FormalStateError("hmm_risk_semantic_posterior_invalid", "causal update unavailable")
        previous = updated - normalizer
        posterior[position] = np.exp(previous)
    return validate_posterior(posterior, total, require_margin=False)


def hard_structure(posterior: np.ndarray, dates: Sequence[str], positions: Sequence[int]) -> list[dict[str, Any]]:
    if len(dates) != len(posterior) or list(positions) != sorted(set(positions)):
        raise FormalStateError("hmm_risk_semantic_calendar_invalid", "structure date/position contract")
    validate_posterior(posterior[list(positions)], len(positions), require_margin=True)
    hard = posterior.argmax(axis=1)
    result = []
    position_set = set(positions)
    for state in range(3):
        members = [p for p in positions if hard[p] == state]
        runs: list[int] = []
        incoming = outgoing = 0
        previous = -2
        for p in members:
            if p == previous + 1:
                runs[-1] += 1
            else:
                runs.append(1)
            previous = p
        for p in positions:
            if p - 1 in position_set and hard[p] != hard[p - 1]:
                incoming += int(hard[p] == state)
                outgoing += int(hard[p - 1] == state)
        result.append(
            {
                "state": state,
                "count": len(members),
                "occupancy": len(members) / len(positions) if positions else 0.0,
                "months": len({dates[p][:7] for p in members}),
                "runs": len(runs),
                "incoming": incoming,
                "outgoing": outgoing,
                "max_run_share": max(runs, default=0) / len(members) if members else 0.0,
            }
        )
    return result


def train_date_receipt(dates: Sequence[str], calendar: Sequence[str] | None = None) -> dict[str, Any]:
    ordered = list(dates)
    canonical = ordered if calendar is None else list(calendar)
    if (
        not ordered
        or ordered != sorted(set(ordered))
        or canonical != sorted(set(canonical))
        or any(date.fromisoformat(day).isoformat() != day for day in canonical)
        or not set(ordered) <= set(canonical)
    ):
        raise FormalStateError("hmm_risk_model_train_date_sequence_invalid", "immutable train dates/calendar invalid")
    missing = sorted(set(canonical) - set(ordered))
    return receipt(
        {
            "ordered_dates": ordered,
            "ordered_date_sha256": canonical_sha256(ordered),
            "canonical_calendar_dates": canonical,
            "canonical_calendar_sha256": canonical_sha256(canonical),
            "missing_dates": missing,
            "missing_date_count": len(missing),
            "missing_date_sha256": canonical_sha256(missing),
            "invalid_dates": [],
            "invalid_date_count": 0,
            "invalid_date_sha256": canonical_sha256([]),
            "N_train": len(ordered),
            "run_basis": "immutable_observation_rows_not_calendar_contiguity",
        }
    )


def train_structure(
    posterior: np.ndarray, dates: Sequence[str], calendar: Sequence[str] | None = None
) -> dict[str, Any]:
    date_identity = train_date_receipt(dates, calendar)
    states = hard_structure(posterior, dates, list(range(len(dates))))
    reasons = []
    reason_by_common = {
        "count": "hmm_risk_model_train_state_count_insufficient",
        "occupancy": "hmm_risk_model_train_occupancy_insufficient",
        "months": "hmm_risk_model_train_month_coverage_insufficient",
        "incoming": "hmm_risk_model_train_transition_coverage_insufficient",
        "outgoing": "hmm_risk_model_train_transition_coverage_insufficient",
    }
    for state in states:
        common = {
            "count": state["count"] >= max(5, math.ceil(0.01 * len(dates))),
            "occupancy": state["occupancy"] >= 0.01,
            "months": state["months"] >= 3,
            "incoming": state["incoming"] >= 2,
            "outgoing": state["outgoing"] >= 2,
        }
        persistent = state["max_run_share"] > 0.8
        path = (
            {
                "count": state["count"] >= max(30, math.ceil(0.10 * len(dates))),
                "occupancy": state["occupancy"] >= 0.10,
                "months": state["months"] >= 6,
                "runs": state["runs"] >= 2,
            }
            if persistent
            else {"runs": state["runs"] >= 3, "share": state["max_run_share"] <= 0.8}
        )
        state.update(
            common_comparisons=common,
            common_thresholds={
                "count": max(5, math.ceil(0.01 * len(dates))),
                "occupancy": 0.01,
                "months": 3,
                "incoming": 2,
                "outgoing": 2,
            },
            path_comparisons=path,
            path_thresholds=(
                {"count": max(30, math.ceil(0.10 * len(dates))), "occupancy": 0.10, "months": 6, "runs": 2}
                if persistent
                else {"runs": 3, "share": 0.8}
            ),
            recurrent_eligible=all(common.values()) and not persistent,
            persistent_eligible=all(common.values()) and persistent,
            recurrent_result=all(common.values()) and not persistent and all(path.values()),
            persistent_result=all(common.values()) and persistent and all(path.values()),
            evidence_path=("persistent" if persistent else "recurrent") if all(common.values()) else "none",
        )
        state_reasons = list(dict.fromkeys(reason_by_common[name] for name, passed in common.items() if not passed))
        if all(common.values()) and not all(path.values()):
            state_reasons.append("hmm_risk_model_train_regime_path_unsatisfied")
        state["reasons"] = state_reasons
        reasons.extend(state_reasons)
    reason_order = [*dict.fromkeys(reason_by_common.values()), "hmm_risk_model_train_regime_path_unsatisfied"]
    ordered_reasons = [reason for reason in reason_order if reason in reasons]
    return receipt(
        {
            "contract_version": CONTRACTS["structure"],
            "states": states,
            "date_receipt": date_identity,
            "validation_accessed": False,
            "future_utility_accessed": False,
            "train_occupancy_valid": not reasons,
            "reasons": ordered_reasons,
            "primary_reason": ordered_reasons[0] if ordered_reasons else None,
            "train_occupancy_status": "failed" if reasons else "accepted",
        }
    )


def fit_entry(
    values: np.ndarray, dates: Sequence[str], seed: int, *, calendar: Sequence[str] | None = None
) -> dict[str, Any]:
    train_date_receipt(dates, calendar)
    model, initialization = initialize(values, seed)
    reference = np.asarray(initialization["reference"])
    history: list[dict[str, Any]] = []
    covariance = None
    for iteration in range(1, 301):
        stats, likelihood = model._do_estep(values, np.array([len(values)]))
        covariance = covariance_audit(model, values, reference)
        raw = np.asarray(model._covars_)
        prior = -0.5 * np.sum(np.log(raw) + reference / raw)
        objective = float(likelihood + prior)
        if not np.isfinite([likelihood, prior, objective]).all():
            raise FormalStateError("hmm_risk_model_map_objective_non_finite", "MAP objective invalid", evidence=history)
        delta = objective - history[-1]["map"] if history else None
        tolerance = (
            max(1e-8, math.sqrt(np.finfo(np.float64).eps) * max(1, abs(history[-1]["map"]))) if history else None
        )
        history.append(
            {
                "iteration": iteration,
                "raw_likelihood": float(likelihood),
                "prior_adjustment": float(prior),
                "map": objective,
                "delta": delta,
                "tolerance": tolerance,
                "numeric_warning": delta is not None and delta < 0,
                "covariance_valid": covariance["covariance_valid"],
                "covariance_evidence": covariance,
                "raw_delta": float(likelihood - history[-1]["raw_likelihood"]) if history else None,
                "covariance_receipt_sha256": covariance["receipt_sha256"],
            }
        )
        history[-1]["raw_relative_delta"] = (
            history[-1]["raw_delta"] / max(abs(history[-2]["raw_likelihood"]), np.finfo(np.float64).eps)
            if len(history) > 1
            else None
        )
        history[-1]["terminal"] = False
        if delta is not None and delta < -tolerance:
            raise FormalStateError(
                "hmm_risk_model_map_objective_decrease", "MAP decrease outside envelope", evidence=history
            )
        if delta is not None and abs(delta) <= tolerance and covariance["covariance_valid"]:
            break
        if iteration == 300:
            raise FormalStateError(
                "hmm_risk_model_map_joint_convergence_unavailable", "no joint stop", evidence=history
            )
        model._do_mstep(stats)
        model._check()
    history[-1]["terminal"] = True
    posterior = causal_filter(model, list(range(len(values))), values, len(values))
    structure = train_structure(posterior, dates, calendar)
    return receipt(
        {
            "initialization": initialization,
            "history": history,
            "covariance": covariance,
            "structure": structure,
            "model": model_payload(model),
            "train_dates": list(dates),
            "train_posterior": posterior.tolist(),
            "training_rows": len(values),
            "feature_count": values.shape[1],
            "final_train_likelihood": history[-1]["raw_likelihood"],
            "score_source": "map_joint_stop_raw_observed_log_likelihood",
            "fit_status": "accepted",
            "monitor_status": "accepted",
            "convergence_valid": True,
            "likelihood_status": "accepted",
            "likelihood_valid": True,
            "covariance_status": "accepted",
            "train_occupancy_status": "accepted" if structure["train_occupancy_valid"] else "failed",
            "warnings": sorted(
                {
                    "hmm_risk_model_raw_likelihood_decrease_diagnostic"
                    for step in history
                    if step["raw_delta"] is not None and step["raw_delta"] < 0
                }
                | {"hmm_risk_model_map_numeric_envelope_warning" for step in history if step["numeric_warning"]}
            ),
            "accepted": structure["train_occupancy_valid"],
            "reasons": structure["reasons"],
        }
    )


def validate_fit_entry(
    entry: Mapping[str, Any], values: np.ndarray, dates: Sequence[str], *, calendar: Sequence[str] | None = None
) -> None:
    """Semantic readback of successful numerical fits, including failed structure.

    Self-hashing a changed success flag never establishes acceptance.  This
    recomputes only evidence; it performs no KMeans/EM/refit.
    """
    if "model" not in entry:
        if entry.get("accepted") is not False or not entry.get("reasons"):
            raise FormalStateError("hmm_risk_model_receipt_invalid", "failed entry has no typed failure")
        return
    model = restore_model(entry["model"])
    initialization = entry["initialization"]
    if initialization["receipt_sha256"] != canonical_sha256(
        {k: v for k, v in initialization.items() if k != "receipt_sha256"}
    ):
        raise FormalStateError("hmm_risk_model_receipt_invalid", "initialization identity differs")
    reference = values.var(axis=0, ddof=0)
    if (
        initialization["reference"] != reference.tolist()
        or entry["train_dates"] != list(dates)
        or entry["training_rows"] != len(values)
        or entry["feature_count"] != values.shape[1]
    ):
        raise FormalStateError("hmm_risk_model_receipt_invalid", "input/reference identity differs")
    seed = initialization["kmeans_parameters"]["random_state"]
    expected_kmeans = dict(
        n_clusters=3,
        init="k-means++",
        n_init=1,
        random_state=seed,
        max_iter=300,
        tol=1e-4,
        algorithm="lloyd",
        copy_x=True,
    )
    expected_parameters = {
        k: v.tolist() if isinstance(v, np.ndarray) else v for k, v in parameter_profile(seed, reference).items()
    }
    labels = initialization["labels"]
    if (
        not isinstance(labels, list)
        or len(labels) != len(values)
        or any(type(k) is not int or k not in (0, 1, 2) for k in labels)
    ):
        raise FormalStateError("hmm_risk_model_receipt_invalid", "initial cluster labels invalid")
    labels_array = np.asarray(labels)
    counts = np.bincount(labels_array, minlength=3)
    if np.any(counts < 2):
        raise FormalStateError("hmm_risk_model_receipt_invalid", "initial cluster sample count invalid")
    variance = np.asarray([values[labels_array == k].var(axis=0, ddof=0) for k in range(3)])
    initial_covariance = (counts[:, None] * variance + reference) / (counts[:, None] + 1.0)
    transitions = np.zeros((3, 3))
    np.add.at(transitions, (labels_array[:-1], labels_array[1:]), 1)
    transitions += 0.1
    transitions /= transitions.sum(axis=1, keepdims=True)
    np.fill_diagonal(transitions, np.maximum(np.diag(transitions), 0.3))
    transitions /= transitions.sum(axis=1, keepdims=True)
    array(initialization["initial_means"], (3, values.shape[1]), "initial means")
    if (
        initialization["contract_version"] != CONTRACTS["d3"]
        or initialization["nu"] != 1.0
        or initialization["kmeans_parameters"] != expected_kmeans
        or initialization["parameters"] != expected_parameters
        or initialization["counts"] != counts.tolist()
        or initialization["cluster_variance"] != variance.tolist()
        or initialization["initial_covariance"] != initial_covariance.tolist()
        or initialization["initial_transmat"] != transitions.tolist()
        or initialization["initial_startprob"] != [1 / 3] * 3
        or initialization["projection_performed"] is not False
    ):
        raise FormalStateError("hmm_risk_model_receipt_invalid", "initialization formula/profile authority differs")
    history = entry["history"]
    if not 2 <= len(history) <= 300:
        raise FormalStateError("hmm_risk_model_receipt_invalid", "MAP history length differs")
    for i, step in enumerate(history):
        evidence = step["covariance_evidence"]
        if evidence["receipt_sha256"] != canonical_sha256({k: v for k, v in evidence.items() if k != "receipt_sha256"}):
            raise FormalStateError("hmm_risk_model_receipt_invalid", "covariance history hash differs")
        covariance = array(evidence["raw"], (3, values.shape[1]), "historical covariance", positive=True)
        prior = float(-0.5 * np.sum(np.log(covariance) + reference / covariance))
        objective = step["raw_likelihood"] + prior
        delta = objective - history[i - 1]["map"] if i else None
        tolerance = max(1e-8, math.sqrt(np.finfo(np.float64).eps) * max(1, abs(history[i - 1]["map"]))) if i else None
        if (
            step["iteration"] != i + 1
            or step["prior_adjustment"] != prior
            or step["map"] != objective
            or step["delta"] != delta
            or step["tolerance"] != tolerance
            or step["numeric_warning"] != (delta is not None and delta < 0)
            or step["raw_delta"] != (step["raw_likelihood"] - history[i - 1]["raw_likelihood"] if i else None)
            or step["covariance_receipt_sha256"] != evidence["receipt_sha256"]
            or step["covariance_valid"] != evidence["covariance_valid"]
            or step["terminal"] != (i == len(history) - 1)
            or step["raw_relative_delta"]
            != (step["raw_delta"] / max(abs(history[i - 1]["raw_likelihood"]), np.finfo(np.float64).eps) if i else None)
            or (delta is not None and delta < -tolerance)
        ):
            raise FormalStateError("hmm_risk_model_receipt_invalid", "MAP history formula differs")
        if i and i < len(history) - 1 and abs(delta) <= tolerance and evidence["covariance_valid"]:
            raise FormalStateError("hmm_risk_model_receipt_invalid", "EM continued after joint stop")
    final = history[-1]
    audit = covariance_audit(model, values, reference)
    posterior = causal_filter(model, list(range(len(values))), values, len(values))
    structure = train_structure(posterior, dates, calendar)
    if (
        audit != entry["covariance"]
        or audit != final["covariance_evidence"]
        or not audit["covariance_valid"]
        or abs(final["delta"]) > final["tolerance"]
        or model.score(values) != final["raw_likelihood"]
        or entry["final_train_likelihood"] != final["raw_likelihood"]
        or entry["train_posterior"] != posterior.tolist()
        or entry["structure"] != structure
        or entry["accepted"] != structure["train_occupancy_valid"]
        or entry["reasons"] != structure["reasons"]
    ):
        raise FormalStateError("hmm_risk_model_receipt_invalid", "final numerical/structure authority differs")
    expected_status = {
        "fit_status": "accepted",
        "monitor_status": "accepted",
        "convergence_valid": True,
        "likelihood_status": "accepted",
        "likelihood_valid": True,
        "covariance_status": "accepted",
        "train_occupancy_status": "accepted" if structure["train_occupancy_valid"] else "failed",
        "score_source": "map_joint_stop_raw_observed_log_likelihood",
    }
    if any(entry.get(key) != value for key, value in expected_status.items()):
        raise FormalStateError("hmm_risk_model_receipt_invalid", "independent status differs")
    warnings = sorted(
        {
            "hmm_risk_model_raw_likelihood_decrease_diagnostic"
            for step in history
            if step["raw_delta"] is not None and step["raw_delta"] < 0
        }
        | {"hmm_risk_model_map_numeric_envelope_warning" for step in history if step["numeric_warning"]}
    )
    if entry.get("warnings") != warnings:
        raise FormalStateError("hmm_risk_model_receipt_invalid", "warning persistence differs")


def select_restart(candidates: Sequence[Mapping[str, Any]], expected_codes: Sequence[str]) -> dict[str, Any]:
    if [c["seed"] for c in candidates] != list(SEEDS):
        raise FormalStateError("hmm_risk_model_restart_schedule_incomplete", "schedule differs")
    pool = []
    summaries = []
    for candidate in candidates:
        entries = candidate["entries"]
        if sorted(entries) != list(expected_codes):
            raise FormalStateError("hmm_risk_model_restart_family_incomplete", "sector set differs")
        eligible = all(e["accepted"] for e in entries.values())
        scores = (
            [
                entries[code]["final_train_likelihood"]
                / (entries[code]["training_rows"] * entries[code]["feature_count"])
                for code in expected_codes
            ]
            if eligible
            else []
        )
        if scores and not np.isfinite(scores).all():
            raise FormalStateError("hmm_risk_model_selection_contract_unsatisfied", "nonfinite score")
        summary = {
            "seed": candidate["seed"],
            "eligible": eligible,
            "scores": scores,
            "score_tuple": [min(scores), sorted(scores)[len(scores) // 2], math.fsum(scores) / len(scores)]
            if scores
            else None,
        }
        summaries.append(summary)
        if eligible:
            pool.append(summary)
    filters = []
    for dimension in range(3):
        if not pool:
            break
        best = max(c["score_tuple"][dimension] for c in pool)
        pool = [
            c
            for c in pool
            if best - c["score_tuple"][dimension] <= 1e-12 + 1e-12 * max(abs(best), abs(c["score_tuple"][dimension]))
        ]
        filters.append({"dimension": dimension, "best": best, "survivors": [c["seed"] for c in pool]})
    return receipt(
        {
            "contract_version": CONTRACTS["selection"],
            "candidates": summaries,
            "filters": filters,
            "selected_seed": pool[0]["seed"] if pool else None,
            "accepted": bool(pool),
            "validation_accessed": False,
            "future_utility_accessed": False,
            "semantic_labelability_accessed": False,
            "d6_status_accessed": False,
        }
    )


def semantic_evidence(
    model: Any,
    *,
    dates: Sequence[str],
    positions: Sequence[int],
    values: np.ndarray,
    components: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if len(dates) != 182 or sorted(set(dates)) != list(dates) or dates[0] != "2024-07-01" or dates[-1] != "2025-03-31":
        raise FormalStateError("hmm_risk_semantic_calendar_invalid", "validation calendar differs")
    for day in dates:
        date.fromisoformat(day)
    posterior = causal_filter(model, positions, values, len(dates))
    if set(components) != {"excess_return_5d", "excess_return_10d", "excess_return_20d"}:
        raise FormalStateError("hmm_risk_semantic_utility_non_finite", "utility component set differs")
    utility_positions = set(range(len(dates)))
    for component in components.values():
        p = component["positions"]
        if p != sorted(set(p)) or any(type(i) is not int or i < 0 or i >= len(dates) for i in p):
            raise FormalStateError("hmm_risk_semantic_calendar_invalid", "utility positions invalid")
        try:
            array(component["values"], (len(p),), "utility component")
        except FormalStateError as exc:
            raise FormalStateError(
                "hmm_risk_semantic_utility_non_finite", str(exc), evidence={"assignment_status": "accepted"}
            ) from exc
        utility_positions &= set(p)
    evidence_positions = sorted(set(positions) & utility_positions)
    ordered_posterior = np.sort(posterior, axis=1)
    diagnostic_positions = np.flatnonzero(ordered_posterior[:, -1] - ordered_posterior[:, -2] > 1e-12).tolist()
    hard = posterior.argmax(axis=1)
    try:
        validate_posterior(posterior[evidence_positions], len(evidence_positions), require_margin=True)
    except FormalStateError as exc:
        evidence = {
            "assignment_status": "failed",
            "posterior": posterior.tolist(),
            "diagnostic_hard_assignment_positions": diagnostic_positions,
            "diagnostic_hard_assignment_values": hard[diagnostic_positions].tolist(),
            "diagnostic_tie_positions": [p for p in range(len(dates)) if p not in diagnostic_positions],
        }
        raise FormalStateError("hmm_risk_semantic_validation_posterior_tie", str(exc), evidence=evidence) from exc
    utility = np.zeros(len(dates))
    for name, weight in (("excess_return_5d", 0.35), ("excess_return_10d", 0.35), ("excess_return_20d", 0.30)):
        component = components[name]
        utility[component["positions"]] += weight * np.asarray(component["values"])
    # The existing evidence-row gate runs before any ratio/state statistics.
    states = hard_structure(posterior, dates, evidence_positions) if len(evidence_positions) >= 30 else []
    failures = []
    if len(evidence_positions) < 30:
        failures.append("hmm_risk_semantic_validation_evidence_rows_insufficient")
    for state in states:
        selected = utility[[p for p in evidence_positions if hard[p] == state["state"]]]
        comparisons = {
            "count": state["count"] >= max(5, math.ceil(0.02 * len(evidence_positions))),
            "occupancy": state["occupancy"] >= 0.02,
            "months": state["months"] >= 2,
            "runs": state["runs"] >= 2,
            "incoming": state["incoming"] >= 2,
            "outgoing": state["outgoing"] >= 2,
            "share": state["max_run_share"] <= 0.9,
        }
        mean = float(selected.mean()) if len(selected) else None
        variance = float(selected.var(ddof=1)) if len(selected) >= 2 else None
        se = math.sqrt(variance / len(selected)) if variance is not None else None
        state.update(
            utility_mean=mean,
            utility_variance=variance,
            utility_standard_error=se,
            utility_count=len(selected),
            comparisons=comparisons,
        )
        for name, passed in comparisons.items():
            if not passed:
                failures.append(
                    {
                        "count": "hmm_risk_semantic_validation_state_count_insufficient",
                        "occupancy": "hmm_risk_semantic_validation_occupancy_insufficient",
                        "months": "hmm_risk_semantic_validation_month_coverage_insufficient",
                        "runs": "hmm_risk_semantic_validation_run_coverage_insufficient",
                        "incoming": "hmm_risk_semantic_validation_transition_coverage_insufficient",
                        "outgoing": "hmm_risk_semantic_validation_transition_coverage_insufficient",
                        "share": "hmm_risk_semantic_validation_run_concentration_exceeded",
                    }[name]
                )
        if state["count"] == 0:
            failures.append("hmm_risk_semantic_hard_state_missing")
        if mean is None or variance is None or not np.isfinite([mean, variance]).all():
            failures.append("hmm_risk_semantic_validation_utility_variance_non_finite")
    ordered = (
        sorted(states, key=lambda s: s["utility_mean"]) if all(s["utility_mean"] is not None for s in states) else []
    )
    gaps = []
    for a, b in zip(ordered, ordered[1:]):
        gap = b["utility_mean"] - a["utility_mean"]
        tolerance = max(1e-12, 32 * np.finfo(np.float64).eps * max(1, abs(a["utility_mean"]), abs(b["utility_mean"])))
        gaps.append({"gap": gap, "tolerance": tolerance, "passed": gap > tolerance})
        if gap <= tolerance:
            failures.append("hmm_risk_semantic_utility_tie")
            failures.append("hmm_risk_semantic_validation_utility_gap_insufficient")
    reason_priority = [
        "hmm_risk_semantic_validation_evidence_rows_insufficient",
        "hmm_risk_semantic_hard_state_missing",
        "hmm_risk_semantic_validation_state_count_insufficient",
        "hmm_risk_semantic_validation_occupancy_insufficient",
        "hmm_risk_semantic_validation_month_coverage_insufficient",
        "hmm_risk_semantic_validation_run_coverage_insufficient",
        "hmm_risk_semantic_validation_transition_coverage_insufficient",
        "hmm_risk_semantic_validation_run_concentration_exceeded",
        "hmm_risk_semantic_validation_utility_variance_non_finite",
        "hmm_risk_semantic_utility_tie",
        "hmm_risk_semantic_validation_utility_gap_insufficient",
    ]
    reasons = sorted(set(failures), key=reason_priority.index)
    return receipt(
        {
            "contract_version": CONTRACTS["semantic"],
            "base_contract_version": "hmm_risk_c008_b3_d6_01_b_v1",
            "availability_contract_version": "hmm_risk_c008_b3_d6_na_a_v1",
            "dates": list(dates),
            "observation_positions": list(positions),
            "utility_positions": sorted(utility_positions),
            "evidence_positions": evidence_positions,
            "posterior": posterior.tolist(),
            "diagnostic_hard_assignment_positions": diagnostic_positions,
            "diagnostic_hard_assignment_values": hard[diagnostic_positions].tolist(),
            "diagnostic_tie_positions": [p for p in range(len(dates)) if p not in diagnostic_positions],
            "evidence_hard_assignments": hard[evidence_positions].tolist(),
            "components": components,
            "utility_on_evidence": utility[evidence_positions].tolist(),
            "states": states,
            "gaps": gaps,
            "assignment_status": "accepted",
            "evidence_status": "failed" if failures else "accepted",
            "semantic_assignment_valid": True,
            "semantic_evidence_valid": not failures,
            "reasons": reasons,
            "primary_reason": reasons[0] if reasons else None,
            "mapping": {str(s["state"]): label for s, label in zip(ordered, ("fading", "neutral", "trending"))}
            if not failures
            else None,
        }
    )
