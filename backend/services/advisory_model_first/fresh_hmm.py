from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np
import pandas as pd
from scipy.special import logsumexp

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

OBSERVATION_COLUMNS = (
    "sector_return_1",
    "sector_excess_20",
    "sector_amount_share",
    "sector_limit_up_ratio",
)


@dataclass(frozen=True)
class FreshHMMResult:
    states: pd.DataFrame
    models: dict[str, Any]
    unavailable: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class FreshHMMContinuationResult:
    states: pd.DataFrame
    unavailable: tuple[dict[str, Any], ...]


def fit_fresh_sector_hmm(
    *,
    static_all: pd.DataFrame,
    market_daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    train_dates: Sequence[pd.Timestamp],
    continuation_cutoff: str,
    min_trading_days: int = 120,
) -> FreshHMMResult:
    observations = build_sector_observations(
        static_all=static_all,
        market_daily=market_daily,
        benchmark_daily=benchmark_daily,
    )
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    train = pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize().sort_values().unique()
    cutoff = pd.Timestamp(continuation_cutoff).normalize()
    inference_dates = calendar[(calendar >= train[0]) & (calendar <= cutoff)]
    models: dict[str, Any] = {}
    states: list[pd.DataFrame] = []
    unavailable: list[dict[str, Any]] = []
    for raw_code in sorted(observations.index.get_level_values("l2_code_id").unique()):
        code = int(raw_code)
        sector = observations.xs(raw_code, level="l2_code_id").reindex(inference_dates)
        train_frame = sector.reindex(train)[list(OBSERVATION_COLUMNS)]
        if len(train_frame) < min_trading_days or train_frame.isna().any().any():
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_HMM_TRAINING_FAILED",
                    "reason": "train_observation_incomplete",
                    "train_rows": len(train_frame),
                    "missing_cells": int(train_frame.isna().sum().sum()),
                }
            )
            continue
        inference_frame = sector[list(OBSERVATION_COLUMNS)]
        if inference_frame.isna().any().any():
            first_missing = inference_frame.index[inference_frame.isna().any(axis=1)][0]
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_HMM_TRAINING_FAILED",
                    "reason": "continuation_observation_gap",
                    "first_missing_date": first_missing.date().isoformat(),
                }
            )
            continue
        mean = train_frame.mean(axis=0).to_numpy(dtype=float)
        std = train_frame.std(axis=0, ddof=0).to_numpy(dtype=float)
        if not np.isfinite(std).all() or (std <= 0).any():
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_HMM_TRAINING_FAILED",
                    "reason": "train_observation_zero_variance",
                }
            )
            continue
        train_matrix = (train_frame.to_numpy(dtype=float) - mean) / std
        inference_matrix = (inference_frame.to_numpy(dtype=float) - mean) / std
        try:
            from hmmlearn.hmm import GaussianHMM

            model = GaussianHMM(
                n_components=2,
                covariance_type="full",
                n_iter=200,
                tol=1e-4,
                random_state=42,
                min_covar=1e-5,
            )
            model.fit(train_matrix)
        except Exception as exc:
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_HMM_TRAINING_FAILED",
                    "reason": "fit_exception",
                    "error_type": type(exc).__name__,
                }
            )
            continue
        if not bool(model.monitor_.converged):
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_HMM_TRAINING_FAILED",
                    "reason": "fit_not_converged",
                    "iterations": int(model.monitor_.iter),
                }
            )
            continue
        history = tuple(float(value) for value in model.monitor_.history)
        likelihood_delta = history[-1] - history[-2] if len(history) >= 2 else 0.0
        if likelihood_delta < -float(model.tol):
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_HMM_TRAINING_FAILED",
                    "reason": "fit_likelihood_regressed",
                    "log_likelihood_delta": likelihood_delta,
                }
            )
            continue
        excess_dimension = OBSERVATION_COLUMNS.index("sector_excess_20")
        raw_excess_means = model.means_[:, excess_dimension] * std[excess_dimension] + mean[excess_dimension]
        raw_order = np.argsort(raw_excess_means, kind="stable")
        canonical_by_raw = {int(raw_order[0]): 0, int(raw_order[1]): 1}
        posterior = _causal_forward_filter(model, inference_matrix)
        raw_state = posterior.argmax(axis=1)
        canonical_state = np.asarray([canonical_by_raw[int(value)] for value in raw_state], dtype=np.int8)
        bull_raw_state = int(raw_order[1])
        duration = _state_duration(canonical_state)
        state_frame = pd.DataFrame(
            {
                "decision_as_of_trade_date": inference_dates,
                "l2_code_id": code,
                "hmm_bull_posterior": posterior[:, bull_raw_state],
                "hmm_state": canonical_state,
                "hmm_state_duration": duration,
                "hmm_observation_completeness": 1.0,
            }
        )
        states.append(state_frame)
        models[str(code)] = {
            "schema_version": "fresh_sector_hmm_v1",
            "l2_code_id": code,
            "observation_order": list(OBSERVATION_COLUMNS),
            "train_start": train[0].date().isoformat(),
            "train_end": train[-1].date().isoformat(),
            "transform_mean": mean.tolist(),
            "transform_std": std.tolist(),
            "startprob": model.startprob_.tolist(),
            "transmat": model.transmat_.tolist(),
            "means": model.means_.tolist(),
            "covariances": model.covars_.tolist(),
            "canonical_state_by_raw": {str(key): value for key, value in canonical_by_raw.items()},
            "canonical_state_labels": {"0": "BEAR", "1": "BULL"},
            "continuation_cutoff": cutoff.date().isoformat(),
            "continuation_last_posterior": posterior[-1].tolist(),
            "continuation_state": int(canonical_state[-1]),
            "continuation_state_duration": int(duration[-1]),
            "continuation_last_observation_date": inference_dates[-1].date().isoformat(),
            "converged": True,
            "iterations": int(model.monitor_.iter),
            "final_log_likelihood_delta": likelihood_delta,
        }
    if not models or not states:
        raise AdvisoryModelFirstError(
            "fresh HMM produced no usable sector models",
            reason_code="ADVISORY_MODEL_HMM_TRAINING_FAILED",
            context={"unavailable_count": len(unavailable)},
        )
    return FreshHMMResult(
        states=pd.concat(states, ignore_index=True).sort_values(
            ["decision_as_of_trade_date", "l2_code_id"]
        ).reset_index(drop=True),
        models={
            "schema_version": "fresh_sector_hmm_bundle_v1",
            "observation_order": list(OBSERVATION_COLUMNS),
            "models": models,
        },
        unavailable=tuple(unavailable),
    )


def build_sector_observations(
    *,
    static_all: pd.DataFrame,
    market_daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
) -> pd.DataFrame:
    static_required = {"l2_code_id", "sw2_close", "sw2_amount"}
    if not static_required.issubset(static_all.columns):
        raise AdvisoryModelFirstError(
            "HMM static input is missing sector fields",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": sorted(static_required - set(static_all.columns))},
        )
    if "limit_up" not in market_daily.columns:
        raise AdvisoryModelFirstError(
            "HMM market input is missing true limit_up",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    static = static_all[["l2_code_id", "sw2_close", "sw2_amount"]].reset_index().dropna(
        subset=["l2_code_id"]
    )
    divergent = static.groupby(["datetime", "l2_code_id"])[["sw2_close", "sw2_amount"]].nunique(
        dropna=True
    ).max(axis=1)
    if (divergent > 1).any():
        item = divergent[divergent > 1].index[0]
        raise AdvisoryModelFirstError(
            "HMM sector fields disagree inside one date and L2 identity",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"datetime": str(item[0]), "l2_code_id": int(item[1])},
        )
    sector = static.groupby(["datetime", "l2_code_id"], sort=True)[["sw2_close", "sw2_amount"]].first()
    sector_close = pd.to_numeric(sector["sw2_close"], errors="coerce")
    sector_amount = pd.to_numeric(sector["sw2_amount"], errors="coerce")
    sector["sector_return_1"] = sector_close.groupby(level="l2_code_id", group_keys=False).pct_change(
        1, fill_method=None
    )
    sector_return_20 = sector_close.groupby(level="l2_code_id", group_keys=False).pct_change(
        20, fill_method=None
    )
    benchmark = _benchmark_close(benchmark_daily)
    benchmark_return_20 = benchmark.pct_change(20, fill_method=None)
    sector["sector_excess_20"] = sector_return_20 - sector.index.get_level_values("datetime").map(
        benchmark_return_20
    )
    sector["sector_amount_share"] = sector_amount / sector_amount.groupby(level="datetime").transform("sum").where(
        lambda value: value > 0
    )

    mapping = static[["datetime", "instrument", "l2_code_id"]].drop_duplicates(
        ["datetime", "instrument"]
    ).set_index(["datetime", "instrument"])
    limit_rows = market_daily[["limit_up"]].join(mapping, how="inner").dropna(subset=["l2_code_id"])
    limit_rows["valid_limit"] = pd.to_numeric(limit_rows["limit_up"], errors="coerce").notna()
    limit_rows["is_limit_up"] = pd.to_numeric(limit_rows["limit_up"], errors="coerce").gt(0)
    grouped = limit_rows.reset_index().groupby(["datetime", "l2_code_id"])
    valid_count = grouped["valid_limit"].sum()
    limit_count = grouped["is_limit_up"].sum()
    ratio = (limit_count / valid_count.where(valid_count > 0)).where(valid_count >= 5)
    sector["sector_limit_up_ratio"] = ratio.reindex(sector.index)
    return sector[list(OBSERVATION_COLUMNS)].sort_index()


def continue_sector_hmm(
    *,
    static_all: pd.DataFrame,
    market_daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    hmm_bundle: dict[str, Any],
    continuation_cutoff: str,
    required_l2_code_ids: Sequence[int],
    precomputed_observations: pd.DataFrame | None = None,
) -> FreshHMMContinuationResult:
    """Continue frozen per-sector posteriors using only post-cutoff observations."""

    if hmm_bundle.get("schema_version") != "fresh_sector_hmm_bundle_v1":
        raise AdvisoryModelFirstError(
            "fresh HMM bundle schema is invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"schema_version": hmm_bundle.get("schema_version")},
        )
    if tuple(hmm_bundle.get("observation_order") or ()) != OBSERVATION_COLUMNS:
        raise AdvisoryModelFirstError(
            "fresh HMM observation order differs from the frozen feature contract",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    cutoff = pd.Timestamp(continuation_cutoff).normalize()
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    continuation_dates = calendar[calendar > cutoff]
    if continuation_dates.empty:
        raise AdvisoryModelFirstError(
            "realtime HMM continuation has no post-cutoff trading dates",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"continuation_cutoff": cutoff.date().isoformat()},
        )
    observations = (
        precomputed_observations.sort_index()
        if precomputed_observations is not None
        else build_sector_observations(
            static_all=static_all,
            market_daily=market_daily,
            benchmark_daily=benchmark_daily,
        )
    )
    if not isinstance(observations.index, pd.MultiIndex) or tuple(observations.index.names) != (
        "datetime",
        "l2_code_id",
    ):
        raise AdvisoryModelFirstError(
            "realtime HMM observations have an invalid index",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"index_names": list(observations.index.names)},
        )
    missing_observation_columns = sorted(set(OBSERVATION_COLUMNS) - set(observations.columns))
    if missing_observation_columns:
        raise AdvisoryModelFirstError(
            "realtime HMM observations have an invalid schema",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"missing_columns": missing_observation_columns},
        )
    models = hmm_bundle.get("models")
    if not isinstance(models, dict) or not models:
        raise AdvisoryModelFirstError(
            "fresh HMM bundle has no sector models",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )

    states: list[pd.DataFrame] = []
    unavailable: list[dict[str, Any]] = []
    for code in sorted({int(value) for value in required_l2_code_ids if int(value) >= 0}):
        model = models.get(str(code))
        if model is None:
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_HMM_SECTOR_NOT_TRAINED",
                    "reason": "sector_model_unavailable_in_bundle",
                }
            )
            continue
        _validate_continuation_model(model, code=code, cutoff=cutoff)
        try:
            sector = observations.xs(code, level="l2_code_id").reindex(continuation_dates)
        except KeyError:
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
                    "reason": "continuation_observations_absent",
                }
            )
            continue
        matrix_frame = sector[list(OBSERVATION_COLUMNS)]
        missing = matrix_frame.isna().any(axis=1)
        if missing.any():
            first_missing_index = missing.index[missing][0]
            first_missing = pd.Timestamp(first_missing_index).date().isoformat()
            missing_columns = matrix_frame.loc[first_missing_index].index[
                matrix_frame.loc[first_missing_index].isna()
            ].tolist()
            unavailable.append(
                {
                    "l2_code_id": code,
                    "reason_code": "ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
                    "reason": "continuation_observation_gap",
                    "first_missing_date": first_missing,
                    "missing_columns": missing_columns,
                }
            )
            continue
        mean = np.asarray(model["transform_mean"], dtype=float)
        std = np.asarray(model["transform_std"], dtype=float)
        if mean.shape != (len(OBSERVATION_COLUMNS),) or std.shape != mean.shape or (std <= 0).any():
            raise AdvisoryModelFirstError(
                "fresh HMM transform parameters are invalid",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                context={"l2_code_id": code},
            )
        matrix = (matrix_frame.to_numpy(dtype=float) - mean) / std
        posterior = _continue_causal_filter(
            matrix=matrix,
            previous_posterior=np.asarray(model["continuation_last_posterior"], dtype=float),
            transmat=np.asarray(model["transmat"], dtype=float),
            means=np.asarray(model["means"], dtype=float),
            covariances=np.asarray(model["covariances"], dtype=float),
        )
        canonical_by_raw = {int(key): int(value) for key, value in model["canonical_state_by_raw"].items()}
        raw_state = posterior.argmax(axis=1)
        try:
            canonical_state = np.asarray([canonical_by_raw[int(value)] for value in raw_state], dtype=np.int8)
        except KeyError as exc:
            raise AdvisoryModelFirstError(
                "fresh HMM canonical state mapping is incomplete",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                context={"l2_code_id": code},
            ) from exc
        bull_raw_states = [raw for raw, canonical in canonical_by_raw.items() if canonical == 1]
        if len(bull_raw_states) != 1:
            raise AdvisoryModelFirstError(
                "fresh HMM bundle does not identify exactly one bull state",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                context={"l2_code_id": code},
            )
        duration = _continued_state_duration(
            canonical_state,
            previous_state=int(model["continuation_state"]),
            previous_duration=int(model["continuation_state_duration"]),
        )
        states.append(
            pd.DataFrame(
                {
                    "decision_as_of_trade_date": continuation_dates,
                    "l2_code_id": code,
                    "hmm_bull_posterior": posterior[:, bull_raw_states[0]],
                    "hmm_state": canonical_state,
                    "hmm_state_duration": duration,
                    "hmm_observation_completeness": 1.0,
                }
            )
        )
    if not states:
        return FreshHMMContinuationResult(
            states=pd.DataFrame(
                columns=[
                    "decision_as_of_trade_date",
                    "l2_code_id",
                    "hmm_bull_posterior",
                    "hmm_state",
                    "hmm_state_duration",
                    "hmm_observation_completeness",
                ]
            ),
            unavailable=tuple(unavailable),
        )
    return FreshHMMContinuationResult(
        states=pd.concat(states, ignore_index=True).sort_values(
            ["decision_as_of_trade_date", "l2_code_id"]
        ).reset_index(drop=True),
        unavailable=tuple(unavailable),
    )


def _validate_continuation_model(model: dict[str, Any], *, code: int, cutoff: pd.Timestamp) -> None:
    required = {
        "schema_version",
        "l2_code_id",
        "observation_order",
        "transform_mean",
        "transform_std",
        "transmat",
        "means",
        "covariances",
        "canonical_state_by_raw",
        "continuation_cutoff",
        "continuation_last_posterior",
        "continuation_state",
        "continuation_state_duration",
        "continuation_last_observation_date",
    }
    missing = sorted(required - set(model))
    if missing:
        raise AdvisoryModelFirstError(
            "fresh HMM sector model is incomplete",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"l2_code_id": code, "missing_fields": missing},
        )
    if (
        model["schema_version"] != "fresh_sector_hmm_v1"
        or int(model["l2_code_id"]) != code
        or tuple(model["observation_order"]) != OBSERVATION_COLUMNS
        or pd.Timestamp(model["continuation_cutoff"]).normalize() != cutoff
        or pd.Timestamp(model["continuation_last_observation_date"]).normalize() != cutoff
    ):
        raise AdvisoryModelFirstError(
            "fresh HMM continuation identity is inconsistent",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"l2_code_id": code},
        )


def _continue_causal_filter(
    *,
    matrix: np.ndarray,
    previous_posterior: np.ndarray,
    transmat: np.ndarray,
    means: np.ndarray,
    covariances: np.ndarray,
) -> np.ndarray:
    state_count = len(previous_posterior)
    if (
        previous_posterior.shape != (state_count,)
        or transmat.shape != (state_count, state_count)
        or means.shape != (state_count, matrix.shape[1])
        or covariances.shape != (state_count, matrix.shape[1], matrix.shape[1])
        or not np.isfinite(previous_posterior).all()
        or previous_posterior.sum() <= 0
    ):
        raise AdvisoryModelFirstError(
            "fresh HMM continuation parameters have invalid dimensions",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    log_emission = _gaussian_log_likelihood(matrix, means=means, covariances=covariances)
    log_transition = np.log(np.clip(transmat, 1e-300, None))
    alpha = np.log(np.clip(previous_posterior / previous_posterior.sum(), 1e-300, None))
    posterior = np.empty_like(log_emission)
    for index in range(len(matrix)):
        alpha = log_emission[index] + logsumexp(alpha[:, None] + log_transition, axis=0)
        alpha -= logsumexp(alpha)
        posterior[index] = np.exp(alpha)
    return posterior


def _gaussian_log_likelihood(
    matrix: np.ndarray,
    *,
    means: np.ndarray,
    covariances: np.ndarray,
) -> np.ndarray:
    output = np.empty((len(matrix), len(means)), dtype=float)
    dimension = matrix.shape[1]
    for state in range(len(means)):
        sign, log_det = np.linalg.slogdet(covariances[state])
        if sign <= 0 or not np.isfinite(log_det):
            raise AdvisoryModelFirstError(
                "fresh HMM covariance is not positive definite",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                context={"raw_state": state},
            )
        try:
            solved = np.linalg.solve(covariances[state], (matrix - means[state]).T).T
        except np.linalg.LinAlgError as exc:
            raise AdvisoryModelFirstError(
                "fresh HMM covariance cannot be solved",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                context={"raw_state": state},
            ) from exc
        quadratic = np.sum((matrix - means[state]) * solved, axis=1)
        output[:, state] = -0.5 * (dimension * np.log(2.0 * np.pi) + log_det + quadratic)
    return output


def _continued_state_duration(
    states: np.ndarray,
    *,
    previous_state: int,
    previous_duration: int,
) -> np.ndarray:
    if previous_duration <= 0:
        raise AdvisoryModelFirstError(
            "fresh HMM continuation duration is invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    duration = np.ones(len(states), dtype=np.int32)
    for index, state in enumerate(states):
        if index == 0:
            duration[index] = previous_duration + 1 if int(state) == previous_state else 1
        else:
            duration[index] = duration[index - 1] + 1 if state == states[index - 1] else 1
    return duration


def _causal_forward_filter(model: Any, matrix: np.ndarray) -> np.ndarray:
    log_emission = np.asarray(model._compute_log_likelihood(matrix), dtype=float)
    log_start = np.log(np.clip(model.startprob_, 1e-300, None))
    log_transition = np.log(np.clip(model.transmat_, 1e-300, None))
    posterior = np.empty_like(log_emission)
    alpha = log_start + log_emission[0]
    alpha -= logsumexp(alpha)
    posterior[0] = np.exp(alpha)
    for index in range(1, len(matrix)):
        alpha = log_emission[index] + logsumexp(alpha[:, None] + log_transition, axis=0)
        alpha -= logsumexp(alpha)
        posterior[index] = np.exp(alpha)
    return posterior


def _state_duration(states: np.ndarray) -> np.ndarray:
    duration = np.ones(len(states), dtype=np.int32)
    for index in range(1, len(states)):
        duration[index] = duration[index - 1] + 1 if states[index] == states[index - 1] else 1
    return duration


def _benchmark_close(frame: pd.DataFrame) -> pd.Series:
    if "close" not in frame.columns:
        raise AdvisoryModelFirstError(
            "HMM benchmark input is missing close",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    reset = frame.reset_index()
    if reset["datetime"].duplicated().any():
        raise AdvisoryModelFirstError(
            "HMM benchmark input contains duplicate dates",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    return pd.Series(
        pd.to_numeric(reset["close"], errors="coerce").to_numpy(),
        index=pd.DatetimeIndex(reset["datetime"]).normalize(),
    ).sort_index()
