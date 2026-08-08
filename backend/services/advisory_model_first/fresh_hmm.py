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
