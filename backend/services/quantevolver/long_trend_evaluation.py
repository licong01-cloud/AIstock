"""Pure QE long-trend signal-path and holding-episode evaluation engine.

The engine consumes already-resolved QE artifacts and dataset frames.  It does
not load H5, write databases, call HTTP, schedule work, or import any live
selection/trading module.  Platform wrappers can stream observation chunks to
the dedicated F-014 artifact store in later delivery phases.
"""

from __future__ import annotations

import math
from bisect import bisect_left
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping

import numpy as np
import pandas as pd

from backend.services.quantevolver.long_trend_data_reader import canonicalize_instrument
from backend.services.quantevolver.long_trend_evaluation_contract import (
    EVALUATOR_VERSION,
    FamilyComputationStatus,
    FamilyEvidenceStatus,
    QELongTrendEvaluationContext,
    QELongTrendError,
    QELongTrendProfile,
    QELongTrendReason,
    QE_LONG_TREND_PROFILE_V1,
    data_action,
    empty_family_statuses,
    require_registered_profile,
)


_MATURITY_STATES = {
    "matured",
    "right_censored",
    "open_event_censored",
    "invalid_entry",
    "path_incomplete",
    "instrument_exit_unresolved",
}

_ENTRY_BLOCK_REASONS = {
    "blocked_limit_up",
    "blocked_suspension",
    "blocked_market_closed",
    "blocked_volume_limit",
    "rejected_insufficient_cash",
}
_EXIT_BLOCK_REASONS = {
    "blocked_limit_down",
    "blocked_suspension",
    "blocked_market_closed",
    "blocked_volume_limit",
    "rejected_insufficient_position",
}

_EPISODE_COLUMNS = (
    "instrument",
    "episode_seq",
    "entry_date",
    "exit_date",
    "left_censored",
    "open_censored",
    "position_observation_end_date",
    "episode_maturity_state",
    "extended_censored",
    "entry_close_qfq",
    "exit_close_qfq",
    "entry_execution_status",
    "entry_execution_evidence_level",
    "actual_entry_date",
    "actual_entry_price",
    "entry_delay_days",
    "entry_block_reason",
    "exit_signal_date",
    "actual_exit_date",
    "actual_exit_price",
    "exit_execution_status",
    "exit_execution_evidence_level",
    "exit_delay_days",
    "exit_block_reason",
    "post_exit_signal_mae",
    "blocked_exit_extra_drawdown",
    "blocked_exit_extra_holding_days",
    "episode_close_return_qfq",
    "execution_gross_return",
    "execution_net_return",
    "episode_mfe",
    "episode_mae",
    "episode_path_coverage",
    "episode_capture_ratio",
    "extended_mfe_180",
    "extended_path_coverage",
    "extended_capture_ratio",
    "post_exit_mfe",
    "highest_stage_at_exit",
    "highest_stage_180",
    "false_early_exit",
    "cost_quality",
    "episode_quality_flags",
)


@dataclass(frozen=True)
class ExecutionEvidenceBundle:
    indicator: pd.DataFrame | None = None
    trades: pd.DataFrame | None = None
    orders: pd.DataFrame | None = None
    exit_signals: pd.DataFrame | None = None


@dataclass
class LongTrendEvaluationResult:
    evaluation_id: str
    profile_id: str
    profile_sha256: str
    evaluator_version: str
    evaluation_asof: str
    signal_observations: pd.DataFrame
    holding_episodes: pd.DataFrame
    metrics: list[dict[str, Any]]
    family_status: dict[str, FamilyEvidenceStatus]
    receipt: dict[str, Any]


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def newey_west_mean_test(values: Iterable[float], *, lag: int) -> dict[str, float | int | None]:
    """Newey-West test for a time-series mean using Bartlett weights."""

    arr = np.asarray(list(values), dtype="float64")
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)
    if n < 2:
        return {"n": n, "mean": None, "se": None, "t": None, "p_value": None}
    mean = float(arr.mean())
    centered = arr - mean
    bounded_lag = min(max(int(lag), 0), n - 1)
    long_run_variance = float(np.dot(centered, centered) / n)
    for step in range(1, bounded_lag + 1):
        covariance = float(np.dot(centered[step:], centered[:-step]) / n)
        weight = 1.0 - step / (bounded_lag + 1.0)
        long_run_variance += 2.0 * weight * covariance
    variance_of_mean = max(long_run_variance / n, 0.0)
    se = math.sqrt(variance_of_mean)
    if se == 0.0:
        t_value = math.inf if mean > 0 else (-math.inf if mean < 0 else 0.0)
        p_value = 0.0 if mean != 0.0 else 1.0
    else:
        t_value = mean / se
        p_value = 2.0 * (1.0 - _normal_cdf(abs(t_value)))
    return {
        "n": n,
        "mean": mean,
        "se": float(se),
        "t": float(t_value),
        "p_value": float(min(max(p_value, 0.0), 1.0)),
    }


def moving_block_bootstrap_mean(
    values: Iterable[float],
    *,
    block_length: int,
    samples: int,
    seed: int,
) -> dict[str, float | int | None]:
    """Deterministic moving-block bootstrap of a daily aggregate mean."""

    arr = np.asarray(list(values), dtype="float64")
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)
    if n == 0:
        return {"n": 0, "mean": None, "ci_low": None, "ci_high": None, "p_value": None}
    block = min(max(int(block_length), 1), n)
    if n == 1:
        value = float(arr[0])
        return {"n": 1, "mean": value, "ci_low": value, "ci_high": value, "p_value": None}
    starts = np.arange(0, n - block + 1, dtype="int64")
    blocks_needed = int(math.ceil(n / block))
    rng = np.random.default_rng(seed)
    boot_means = np.empty(samples, dtype="float64")
    for sample_index in range(samples):
        picked = rng.choice(starts, size=blocks_needed, replace=True)
        indices = np.concatenate([np.arange(start, start + block) for start in picked])[:n]
        boot_means[sample_index] = float(arr[indices].mean())
    lower, upper = np.quantile(boot_means, [0.025, 0.975])
    p_value = 2.0 * min(float(np.mean(boot_means <= 0.0)), float(np.mean(boot_means >= 0.0)))
    return {
        "n": n,
        "mean": float(arr.mean()),
        "ci_low": float(lower),
        "ci_high": float(upper),
        "p_value": float(min(max(p_value, 0.0), 1.0)),
    }


def benjamini_hochberg(p_values: Iterable[float | None]) -> list[float | None]:
    values = list(p_values)
    finite = [(index, float(value)) for index, value in enumerate(values) if value is not None and math.isfinite(value)]
    result: list[float | None] = [None] * len(values)
    if not finite:
        return result
    ordered = sorted(finite, key=lambda item: item[1])
    count = len(ordered)
    running = 1.0
    for reverse_rank, (index, value) in enumerate(reversed(ordered), start=1):
        rank = count - reverse_rank + 1
        adjusted = min(running, value * count / rank)
        running = adjusted
        result[index] = float(min(max(adjusted, 0.0), 1.0))
    return result


def _safe_nan_extreme(matrix: np.ndarray, *, kind: str) -> np.ndarray:
    if matrix.ndim != 2:
        raise ValueError("matrix must be two-dimensional")
    finite = np.isfinite(matrix)
    if kind == "max":
        filled = np.where(finite, matrix, -np.inf)
        result = filled.max(axis=0)
        result[~finite.any(axis=0)] = np.nan
        return result
    if kind == "min":
        filled = np.where(finite, matrix, np.inf)
        result = filled.min(axis=0)
        result[~finite.any(axis=0)] = np.nan
        return result
    raise ValueError(f"unsupported extreme kind {kind!r}")


def _normalize_prediction_frame(predictions: pd.DataFrame | pd.Series) -> pd.DataFrame:
    if isinstance(predictions, pd.Series):
        frame = predictions.rename("score").to_frame()
    elif isinstance(predictions, pd.DataFrame):
        frame = predictions.copy(deep=False)
    else:
        raise QELongTrendError(
            QELongTrendReason.PREDICTION_SCHEMA_INVALID,
            "predictions must be a pandas Series or DataFrame",
        )
    if not isinstance(frame.index, pd.MultiIndex):
        if {"datetime", "instrument"}.issubset(frame.columns):
            frame = frame.set_index(["datetime", "instrument"])
        elif {"signal_date", "instrument"}.issubset(frame.columns):
            frame = frame.set_index(["signal_date", "instrument"])
        else:
            raise QELongTrendError(
                QELongTrendReason.PREDICTION_SCHEMA_INVALID,
                "predictions require datetime/instrument identity",
            )
    if len(frame.index.names) != 2:
        raise QELongTrendError(
            QELongTrendReason.PREDICTION_SCHEMA_INVALID,
            "prediction index must have exactly two levels",
        )
    frame = frame.copy(deep=False)
    frame.index = frame.index.set_names(["signal_date", "instrument"])
    if "score" in frame.columns:
        score_column = "score"
    elif "pred" in frame.columns:
        score_column = "pred"
    elif len(frame.columns) == 1:
        score_column = frame.columns[0]
    else:
        raise QELongTrendError(
            QELongTrendReason.PREDICTION_SCHEMA_INVALID,
            f"cannot identify prediction score column from {list(frame.columns)!r}",
        )
    reset = frame.loc[:, [score_column]].rename(columns={score_column: "score"}).reset_index()
    reset["signal_date"] = pd.to_datetime(reset["signal_date"], errors="coerce").dt.normalize()
    if reset["signal_date"].isna().any():
        raise QELongTrendError(QELongTrendReason.PREDICTION_SCHEMA_INVALID, "invalid signal_date")
    try:
        reset["instrument"] = reset["instrument"].map(canonicalize_instrument)
    except ValueError as exc:
        raise QELongTrendError(QELongTrendReason.PREDICTION_SCHEMA_INVALID, str(exc)) from exc
    reset["score"] = pd.to_numeric(reset["score"], errors="coerce")
    if not np.isfinite(reset["score"].to_numpy(dtype="float64")).all():
        raise QELongTrendError(
            QELongTrendReason.PREDICTION_SCHEMA_INVALID,
            "prediction score contains NaN or infinite values",
        )
    if reset.duplicated(["signal_date", "instrument"]).any():
        raise QELongTrendError(
            QELongTrendReason.PREDICTION_SCHEMA_INVALID,
            "prediction identity must be unique by signal_date/instrument",
        )
    reset.sort_values(
        ["signal_date", "score", "instrument"],
        ascending=[True, False, True],
        kind="mergesort",
        inplace=True,
    )
    reset["stable_rank"] = reset.groupby("signal_date", sort=False).cumcount() + 1
    return reset.reset_index(drop=True)


def _normalize_price_frame(prices: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(prices, pd.DataFrame):
        raise QELongTrendError(QELongTrendReason.DAILY_PV_SCHEMA_INVALID, "prices must be a DataFrame")
    frame = prices.copy(deep=False)
    if not isinstance(frame.index, pd.MultiIndex):
        if {"datetime", "instrument"}.issubset(frame.columns):
            frame = frame.set_index(["datetime", "instrument"])
        else:
            raise QELongTrendError(
                QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                "prices require a datetime/instrument MultiIndex",
            )
    frame.index = frame.index.set_names(["datetime", "instrument"])
    required = {"close_qfq", "high_qfq", "low_qfq"}
    if not required.issubset(frame.columns):
        raise QELongTrendError(
            QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
            f"prices are missing columns {sorted(required - set(frame.columns))}",
        )
    if not frame.index.is_unique:
        raise QELongTrendError(QELongTrendReason.DAILY_PV_SCHEMA_INVALID, "price identity is duplicated")
    optional = {"volume_qfq", "suspend_d", "limit_state", "instrument_event"}
    selected_columns = sorted(required | (optional & set(frame.columns)))
    reset = frame.loc[:, selected_columns].reset_index()
    if reset.empty:
        raise QELongTrendError(
            QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
            "price frame contains no rows",
        )
    reset["datetime"] = pd.to_datetime(reset["datetime"], errors="coerce").dt.normalize()
    if reset["datetime"].isna().any():
        raise QELongTrendError(QELongTrendReason.DAILY_PV_SCHEMA_INVALID, "invalid price datetime")
    try:
        reset["instrument"] = reset["instrument"].map(canonicalize_instrument)
    except ValueError as exc:
        raise QELongTrendError(QELongTrendReason.DAILY_PV_SCHEMA_INVALID, str(exc)) from exc
    if reset.duplicated(["datetime", "instrument"]).any():
        raise QELongTrendError(
            QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
            "price identity duplicates after instrument canonicalization",
        )
    for column in required:
        reset[column] = pd.to_numeric(reset[column], errors="coerce")
        reset.loc[reset[column] <= 0.0, column] = np.nan
    finite_ohlc = reset[["close_qfq", "high_qfq", "low_qfq"]].notna().all(axis=1)
    invalid_ohlc = finite_ohlc & (
        (reset["high_qfq"] < reset["low_qfq"])
        | (reset["close_qfq"] > reset["high_qfq"])
        | (reset["close_qfq"] < reset["low_qfq"])
    )
    if bool(invalid_ohlc.any()):
        raise QELongTrendError(
            QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
            "prices contain inconsistent qfq high/low/close rows",
            context={"invalid_row_count": int(invalid_ohlc.sum())},
        )
    if "volume_qfq" in reset:
        reset["volume_qfq"] = pd.to_numeric(reset["volume_qfq"], errors="coerce")
        reset.loc[reset["volume_qfq"] < 0.0, "volume_qfq"] = np.nan
    if "suspend_d" in reset:
        reset["suspend_d"] = reset["suspend_d"].astype("boolean")
    for column in ("limit_state", "instrument_event"):
        if column in reset:
            reset[column] = reset[column].astype("string")
    return reset.set_index(["datetime", "instrument"]).sort_index()


def _normalize_sector_frame(sectors: pd.DataFrame | None) -> pd.DataFrame | None:
    if sectors is None:
        return None
    frame = sectors.copy(deep=False)
    if not isinstance(frame.index, pd.MultiIndex):
        if {"datetime", "instrument"}.issubset(frame.columns):
            frame = frame.set_index(["datetime", "instrument"])
        else:
            raise QELongTrendError(
                QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID,
                "sector frame requires datetime/instrument identity",
            )
    frame.index = frame.index.set_names(["datetime", "instrument"])
    if "l2_code_id" not in frame.columns:
        raise QELongTrendError(QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID, "l2_code_id is missing")
    reset = frame.loc[:, ["l2_code_id"]].reset_index()
    reset["datetime"] = pd.to_datetime(reset["datetime"], errors="coerce").dt.normalize()
    try:
        reset["instrument"] = reset["instrument"].map(canonicalize_instrument)
    except ValueError as exc:
        raise QELongTrendError(QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID, str(exc)) from exc
    numeric = pd.to_numeric(reset["l2_code_id"], errors="coerce")
    finite = numeric.dropna().to_numpy(dtype="float64")
    if finite.size and not np.allclose(finite, np.rint(finite), rtol=0.0, atol=0.0):
        raise QELongTrendError(
            QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID,
            "l2_code_id contains non-integer values",
        )
    if finite.size and bool((finite < -1.0).any()):
        raise QELongTrendError(
            QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID,
            "l2_code_id contains values below the registered -1 unknown sentinel",
        )
    reset["l2_code_id"] = numeric.mask(numeric.eq(-1.0)).astype("Int16")
    if reset.duplicated(["datetime", "instrument"]).any():
        raise QELongTrendError(QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID, "sector identity is duplicated")
    return reset.set_index(["datetime", "instrument"]).sort_index()


def _normalize_label_frame(labels: pd.DataFrame | pd.Series | None) -> pd.Series | None:
    if labels is None:
        return None
    if isinstance(labels, pd.Series):
        series = labels
    elif isinstance(labels, pd.DataFrame) and len(labels.columns) == 1:
        series = labels.iloc[:, 0]
    elif isinstance(labels, pd.DataFrame) and "label" in labels.columns:
        series = labels["label"]
    else:
        raise QELongTrendError(
            QELongTrendReason.PREDICTION_SCHEMA_INVALID,
            "label artifact must be a Series or single-column DataFrame",
        )
    if not isinstance(series.index, pd.MultiIndex):
        raise QELongTrendError(
            QELongTrendReason.PREDICTION_SCHEMA_INVALID,
            "label artifact requires datetime/instrument MultiIndex",
        )
    reset = series.rename("label").reset_index()
    reset.columns = ["signal_date", "instrument", "label"]
    reset["signal_date"] = pd.to_datetime(reset["signal_date"], errors="coerce").dt.normalize()
    try:
        reset["instrument"] = reset["instrument"].map(canonicalize_instrument)
    except ValueError as exc:
        raise QELongTrendError(QELongTrendReason.PREDICTION_SCHEMA_INVALID, str(exc)) from exc
    if reset.duplicated(["signal_date", "instrument"]).any():
        raise QELongTrendError(QELongTrendReason.PREDICTION_SCHEMA_INVALID, "label identity is duplicated")
    reset["label"] = pd.to_numeric(reset["label"], errors="coerce")
    return reset.set_index(["signal_date", "instrument"])["label"].sort_index()


class QELongTrendEvaluationEngine:
    """Compute F-014 signal-path, sector, execution, and episode evidence."""

    def __init__(self, profile: QELongTrendProfile = QE_LONG_TREND_PROFILE_V1) -> None:
        self.profile = require_registered_profile(profile)

    def iter_signal_observation_chunks(
        self,
        *,
        predictions: pd.DataFrame | pd.Series,
        prices: pd.DataFrame,
        sectors: pd.DataFrame | None = None,
        signal_dates_per_chunk: int = 16,
    ) -> Iterator[pd.DataFrame]:
        if signal_dates_per_chunk <= 0:
            raise ValueError("signal_dates_per_chunk must be positive")
        prediction_frame = _normalize_prediction_frame(predictions)
        price_frame = _normalize_price_frame(prices)
        sector_frame = _normalize_sector_frame(sectors)
        yield from self._iter_normalized_observation_chunks(
            prediction_frame=prediction_frame,
            price_frame=price_frame,
            sector_frame=sector_frame,
            signal_dates_per_chunk=signal_dates_per_chunk,
        )

    def build_signal_observations(
        self,
        *,
        predictions: pd.DataFrame | pd.Series,
        prices: pd.DataFrame,
        sectors: pd.DataFrame | None = None,
        execution_evidence: ExecutionEvidenceBundle | None = None,
        signal_dates_per_chunk: int = 16,
    ) -> pd.DataFrame:
        prediction_frame = _normalize_prediction_frame(predictions)
        price_frame = _normalize_price_frame(prices)
        sector_frame = _normalize_sector_frame(sectors)
        return self._build_normalized_signal_observations(
            prediction_frame=prediction_frame,
            price_frame=price_frame,
            sector_frame=sector_frame,
            execution_evidence=execution_evidence,
            signal_dates_per_chunk=signal_dates_per_chunk,
        )

    def _build_normalized_signal_observations(
        self,
        *,
        prediction_frame: pd.DataFrame,
        price_frame: pd.DataFrame,
        sector_frame: pd.DataFrame | None,
        execution_evidence: ExecutionEvidenceBundle | None,
        signal_dates_per_chunk: int,
    ) -> pd.DataFrame:
        chunks = list(
            self._iter_normalized_observation_chunks(
                prediction_frame=prediction_frame,
                price_frame=price_frame,
                sector_frame=sector_frame,
                signal_dates_per_chunk=signal_dates_per_chunk,
            )
        )
        observations = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        return attach_entry_execution_evidence(
            observations,
            evidence=execution_evidence,
            calendar=pd.DatetimeIndex(sorted(price_frame.index.get_level_values("datetime").unique())),
        )

    def _iter_normalized_observation_chunks(
        self,
        *,
        prediction_frame: pd.DataFrame,
        price_frame: pd.DataFrame,
        sector_frame: pd.DataFrame | None,
        signal_dates_per_chunk: int,
    ) -> Iterator[pd.DataFrame]:
        calendar = pd.DatetimeIndex(sorted(price_frame.index.get_level_values("datetime").unique()))
        close_wide = price_frame["close_qfq"].unstack("instrument").reindex(index=calendar)
        high_wide = (
            price_frame["high_qfq"]
            .unstack("instrument")
            .reindex(
                index=calendar,
                columns=close_wide.columns,
            )
        )
        low_wide = (
            price_frame["low_qfq"]
            .unstack("instrument")
            .reindex(
                index=calendar,
                columns=close_wide.columns,
            )
        )
        volume_wide = (
            price_frame["volume_qfq"]
            .unstack("instrument")
            .reindex(
                index=calendar,
                columns=close_wide.columns,
            )
            if "volume_qfq" in price_frame.columns
            else pd.DataFrame(np.nan, index=calendar, columns=close_wide.columns)
        )
        suspend_wide = (
            price_frame["suspend_d"]
            .unstack("instrument")
            .reindex(
                index=calendar,
                columns=close_wide.columns,
            )
            if "suspend_d" in price_frame.columns
            else pd.DataFrame(False, index=calendar, columns=close_wide.columns)
        )
        limit_wide = (
            price_frame["limit_state"]
            .unstack("instrument")
            .reindex(
                index=calendar,
                columns=close_wide.columns,
            )
            if "limit_state" in price_frame.columns
            else pd.DataFrame(pd.NA, index=calendar, columns=close_wide.columns, dtype="string")
        )
        event_wide = (
            price_frame["instrument_event"]
            .unstack("instrument")
            .reindex(
                index=calendar,
                columns=close_wide.columns,
            )
            if "instrument_event" in price_frame.columns
            else pd.DataFrame(pd.NA, index=calendar, columns=close_wide.columns, dtype="string")
        )
        instruments = close_wide.columns.astype(str)
        instrument_positions = {value: index for index, value in enumerate(instruments)}
        close_values = close_wide.to_numpy(dtype="float64", copy=False)
        high_values = high_wide.to_numpy(dtype="float64", copy=False)
        low_values = low_wide.to_numpy(dtype="float64", copy=False)
        volume_values = volume_wide.to_numpy(dtype="float64", copy=False)
        suspend_values = suspend_wide.fillna(False).to_numpy(dtype="bool", copy=False)
        limit_values = limit_wide.to_numpy(dtype="object", copy=False)
        event_values = event_wide.to_numpy(dtype="object", copy=False)
        calendar_positions = {value: index for index, value in enumerate(calendar)}

        unique_signal_dates = pd.DatetimeIndex(prediction_frame["signal_date"].drop_duplicates())
        invalid_dates = [value for value in unique_signal_dates if value not in calendar_positions]
        if invalid_dates:
            raise QELongTrendError(
                QELongTrendReason.PREDICTION_SCHEMA_INVALID,
                "prediction contains signal dates outside the QE trading calendar",
                context={"examples": [str(value.date()) for value in invalid_dates[:5]]},
            )

        batch_rows: list[pd.DataFrame] = []
        for signal_date, group in prediction_frame.groupby("signal_date", sort=True, observed=True):
            batch_rows.append(
                self._evaluate_signal_date(
                    group=group.copy(),
                    signal_date=pd.Timestamp(signal_date),
                    calendar=calendar,
                    calendar_positions=calendar_positions,
                    instrument_positions=instrument_positions,
                    close_values=close_values,
                    high_values=high_values,
                    low_values=low_values,
                    volume_values=volume_values,
                    suspend_values=suspend_values,
                    limit_values=limit_values,
                    event_values=event_values,
                    sector_frame=sector_frame,
                )
            )
            if len(batch_rows) >= signal_dates_per_chunk:
                yield pd.concat(batch_rows, ignore_index=True)
                batch_rows.clear()
        if batch_rows:
            yield pd.concat(batch_rows, ignore_index=True)

    def _evaluate_signal_date(
        self,
        *,
        group: pd.DataFrame,
        signal_date: pd.Timestamp,
        calendar: pd.DatetimeIndex,
        calendar_positions: Mapping[pd.Timestamp, int],
        instrument_positions: Mapping[str, int],
        close_values: np.ndarray,
        high_values: np.ndarray,
        low_values: np.ndarray,
        volume_values: np.ndarray,
        suspend_values: np.ndarray,
        limit_values: np.ndarray,
        event_values: np.ndarray,
        sector_frame: pd.DataFrame | None,
    ) -> pd.DataFrame:
        row_count = len(group)
        signal_position = calendar_positions[signal_date]
        entry_position = signal_position + 1
        column_positions = np.asarray(
            [instrument_positions.get(value, -1) for value in group["instrument"]],
            dtype="int64",
        )
        known_instrument = column_positions >= 0
        safe_columns = np.where(known_instrument, column_positions, 0)
        entry_values = np.full(row_count, np.nan, dtype="float64")
        entry_volume = np.full(row_count, np.nan, dtype="float64")
        entry_suspended = np.zeros(row_count, dtype="bool")
        entry_limit_state = np.full(row_count, None, dtype=object)
        entry_instrument_event = np.full(row_count, None, dtype=object)
        if entry_position < len(calendar):
            entry_values[known_instrument] = close_values[entry_position, safe_columns[known_instrument]]
            entry_volume[known_instrument] = volume_values[
                entry_position,
                safe_columns[known_instrument],
            ]
            entry_suspended[known_instrument] = suspend_values[
                entry_position,
                safe_columns[known_instrument],
            ]
            entry_limit_state[known_instrument] = limit_values[
                entry_position,
                safe_columns[known_instrument],
            ]
            entry_instrument_event[known_instrument] = event_values[
                entry_position,
                safe_columns[known_instrument],
            ]
            entry_date: pd.Timestamp | pd.NaT = calendar[entry_position]
        else:
            entry_date = pd.NaT
        valid_entry = known_instrument & np.isfinite(entry_values) & (entry_values > 0.0)

        output = group.loc[:, ["signal_date", "instrument", "score", "stable_rank"]].reset_index(drop=True)
        output["entry_date"] = entry_date
        output["entry_close_qfq"] = entry_values
        output["entry_volume_qfq"] = entry_volume
        output["entry_suspension_diagnostic"] = entry_suspended | (np.isfinite(entry_volume) & (entry_volume == 0.0))
        output["entry_limit_state_diagnostic"] = entry_limit_state
        output["entry_instrument_event"] = entry_instrument_event
        output["signal_calendar_position"] = signal_position
        output["evaluation_calendar_position"] = len(calendar) - 1

        quality_flags: list[set[str]] = [set() for _ in range(row_count)]
        for index in np.flatnonzero(~known_instrument):
            quality_flags[int(index)].add("instrument_missing_from_price_snapshot")
        for index in np.flatnonzero(known_instrument & ~valid_entry):
            quality_flags[int(index)].add("invalid_entry_close")
        for index in np.flatnonzero(output["entry_suspension_diagnostic"].to_numpy(dtype="bool")):
            quality_flags[int(index)].add("entry_suspension_diagnostic")

        for horizon in self.profile.horizons:
            maturity = np.full(row_count, "invalid_entry", dtype=object)
            if entry_position >= len(calendar):
                maturity[:] = "right_censored"
            observed_steps = np.zeros(row_count, dtype="int32")
            observed_prefix_steps = np.zeros(row_count, dtype="int32")
            observed_high_low_steps = np.zeros(row_count, dtype="int32")
            returns = np.full(row_count, np.nan, dtype="float64")
            close_mfe = np.full(row_count, np.nan, dtype="float64")
            close_mae = np.full(row_count, np.nan, dtype="float64")
            path_mfe = np.full(row_count, np.nan, dtype="float64")
            path_mae = np.full(row_count, np.nan, dtype="float64")

            if entry_position < len(calendar):
                available_steps = min(horizon, len(calendar) - entry_position - 1)
                if available_steps > 0:
                    close_path = close_values[
                        entry_position + 1 : entry_position + available_steps + 1,
                        safe_columns,
                    ]
                    high_path = high_values[
                        entry_position + 1 : entry_position + available_steps + 1,
                        safe_columns,
                    ]
                    low_path = low_values[
                        entry_position + 1 : entry_position + available_steps + 1,
                        safe_columns,
                    ]
                    close_path[:, ~known_instrument] = np.nan
                    high_path[:, ~known_instrument] = np.nan
                    low_path[:, ~known_instrument] = np.nan
                    finite_close_path = np.isfinite(close_path)
                    observed_steps = finite_close_path.sum(axis=0).astype("int32")
                    observed_prefix_steps = np.cumprod(
                        finite_close_path,
                        axis=0,
                        dtype="int32",
                    ).sum(axis=0, dtype="int32")
                    observed_high_low_steps = np.minimum(
                        np.isfinite(high_path).sum(axis=0),
                        np.isfinite(low_path).sum(axis=0),
                    ).astype("int32")
                else:
                    close_path = np.empty((0, row_count), dtype="float64")
                    high_path = np.empty((0, row_count), dtype="float64")
                    low_path = np.empty((0, row_count), dtype="float64")

                terminal_position = entry_position + horizon
                if terminal_position >= len(calendar):
                    maturity[valid_entry] = "right_censored"
                else:
                    terminal_values = close_values[terminal_position, safe_columns]
                    terminal_values[~known_instrument] = np.nan
                    path_coverage = observed_steps / float(horizon)
                    high_low_coverage = observed_high_low_steps / float(horizon)
                    close_complete = (
                        valid_entry
                        & np.isfinite(terminal_values)
                        & (path_coverage >= self.profile.path_coverage_reference)
                    )
                    terminal_missing = valid_entry & ~np.isfinite(terminal_values)
                    tail = close_values[terminal_position:, safe_columns]
                    tail[:, ~known_instrument] = np.nan
                    no_later_close = ~np.isfinite(tail).any(axis=0)
                    terminal_events = event_values[terminal_position, safe_columns]
                    explicit_exit = np.asarray(
                        [
                            str(value).strip().lower() in {"delisted", "instrument_exit", "terminated"}
                            if value is not None and not pd.isna(value)
                            else False
                            for value in terminal_events
                        ],
                        dtype="bool",
                    )
                    unresolved_exit = terminal_missing & (no_later_close | explicit_exit)
                    maturity[valid_entry & ~close_complete] = "path_incomplete"
                    maturity[unresolved_exit] = "instrument_exit_unresolved"
                    maturity[close_complete] = "matured"
                    path_complete = close_complete & (high_low_coverage >= self.profile.path_coverage_reference)
                    with np.errstate(divide="ignore", invalid="ignore"):
                        returns[close_complete] = terminal_values[close_complete] / entry_values[close_complete] - 1.0
                        close_returns = close_path / entry_values[None, :] - 1.0
                        high_returns = high_path / entry_values[None, :] - 1.0
                        low_returns = low_path / entry_values[None, :] - 1.0
                    close_max = _safe_nan_extreme(close_returns, kind="max")
                    close_min = _safe_nan_extreme(close_returns, kind="min")
                    high_max = _safe_nan_extreme(high_returns, kind="max")
                    low_min = _safe_nan_extreme(low_returns, kind="min")
                    close_mfe[close_complete] = np.maximum(close_max[close_complete], 0.0)
                    close_mae[close_complete] = np.minimum(close_min[close_complete], 0.0)
                    path_mfe[path_complete] = np.maximum(high_max[path_complete], 0.0)
                    path_mae[path_complete] = np.minimum(low_min[path_complete], 0.0)
                    for index in np.flatnonzero(close_complete & ~path_complete):
                        quality_flags[int(index)].add(f"high_low_path_incomplete_h{horizon}")
                    for index in np.flatnonzero(unresolved_exit):
                        quality_flags[int(index)].add(f"instrument_exit_unresolved_h{horizon}")

            output[f"return_{horizon}"] = returns
            output[f"close_mfe_{horizon}"] = close_mfe
            output[f"close_mae_{horizon}"] = close_mae
            output[f"path_mfe_{horizon}"] = path_mfe
            output[f"path_mae_{horizon}"] = path_mae
            output[f"maturity_{horizon}"] = maturity
            output[f"observed_steps_{horizon}"] = observed_steps
            output[f"observed_prefix_steps_{horizon}"] = observed_prefix_steps
            output[f"observed_high_low_steps_{horizon}"] = observed_high_low_steps
            output[f"path_quality_{horizon}"] = np.where(
                maturity == "matured",
                np.where(
                    observed_high_low_steps / float(horizon) >= self.profile.path_coverage_reference,
                    "complete",
                    "path_incomplete",
                ),
                maturity,
            )
            unexpected_states = sorted(set(maturity) - _MATURITY_STATES)
            if unexpected_states:
                raise QELongTrendError(
                    QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                    f"evaluator produced unsupported maturity states: {unexpected_states}",
                )

        max_horizon = max(self.profile.horizons)
        available_steps = max(0, min(max_horizon, len(calendar) - entry_position - 1))
        if entry_position < len(calendar) and available_steps > 0:
            close_path = close_values[
                entry_position + 1 : entry_position + available_steps + 1,
                safe_columns,
            ]
            high_path = high_values[
                entry_position + 1 : entry_position + available_steps + 1,
                safe_columns,
            ]
            close_path[:, ~known_instrument] = np.nan
            high_path[:, ~known_instrument] = np.nan
            with np.errstate(divide="ignore", invalid="ignore"):
                close_path_returns = close_path / entry_values[None, :] - 1.0
                high_path_returns = high_path / entry_values[None, :] - 1.0
        else:
            close_path_returns = np.empty((0, row_count), dtype="float64")
            high_path_returns = np.empty((0, row_count), dtype="float64")

        for barrier in self.profile.barriers:
            suffix = int(round(barrier * 100))
            close_times = _first_hit_steps(close_path_returns, barrier)
            high_times = _first_hit_steps(high_path_returns, barrier)
            output[f"time_to_close_hit_{suffix}"] = close_times
            close_hit = pd.array(np.isfinite(close_times), dtype="boolean")
            high_hit = pd.array(np.isfinite(high_times), dtype="boolean")
            censored = output[f"maturity_{max_horizon}"] != "matured"
            close_hit[censored & ~np.isfinite(close_times)] = pd.NA
            high_hit[censored & ~np.isfinite(high_times)] = pd.NA
            output[f"close_hit_{suffix}"] = close_hit
            output[f"high_path_hit_{suffix}"] = high_hit

        stage = np.full(row_count, None, dtype=object)
        mature_max = output[f"maturity_{max_horizon}"].to_numpy() == "matured"
        stage[mature_max] = "NONE"
        for barrier in self.profile.barriers:
            suffix = int(round(barrier * 100))
            hit = output[f"time_to_close_hit_{suffix}"].to_numpy(dtype="float64")
            stage[mature_max & np.isfinite(hit) & (hit <= max_horizon)] = f"HIT{suffix}"
        output[f"highest_close_stage_{max_horizon}"] = stage

        if sector_frame is not None:
            lookup = pd.MultiIndex.from_arrays(
                [[signal_date] * row_count, group["instrument"]],
                names=["datetime", "instrument"],
            )
            output["l2_code_id"] = sector_frame["l2_code_id"].reindex(lookup).reset_index(drop=True)
        else:
            output["l2_code_id"] = pd.array([pd.NA] * row_count, dtype="Int16")
        output["row_quality_flags"] = ["|".join(sorted(flags)) for flags in quality_flags]
        return output

    def evaluate(
        self,
        *,
        context: QELongTrendEvaluationContext,
        predictions: pd.DataFrame | pd.Series | None,
        prices: pd.DataFrame | None,
        sectors: pd.DataFrame | None = None,
        labels: pd.DataFrame | pd.Series | None = None,
        label_horizon: int | None = None,
        positions: pd.DataFrame | None = None,
        portfolio_report: pd.DataFrame | None = None,
        execution_evidence: ExecutionEvidenceBundle | None = None,
        strategy_topk: int | None = None,
    ) -> LongTrendEvaluationResult:
        if label_horizon is not None and (
            isinstance(label_horizon, bool)
            or not isinstance(label_horizon, int)
            or label_horizon not in self.profile.horizons
        ):
            raise QELongTrendError(
                QELongTrendReason.PROFILE_INVALID,
                f"label_horizon {label_horizon!r} is not in the registered profile",
            )
        if strategy_topk is not None and (
            isinstance(strategy_topk, bool)
            or not isinstance(strategy_topk, int)
            or strategy_topk <= 0
            or strategy_topk > self.profile.include_strategy_topk_up_to
        ):
            raise QELongTrendError(
                QELongTrendReason.PROFILE_INVALID,
                "strategy_topk must be a positive integer within the registered profile limit",
            )
        evaluation_parameters = {
            "label_horizon": label_horizon,
            "strategy_topk": strategy_topk,
        }
        price_error: QELongTrendError | None = None
        try:
            price_frame = _normalize_price_frame(prices)
        except QELongTrendError as exc:
            if exc.reason_code != QELongTrendReason.DAILY_PV_SCHEMA_INVALID.value:
                raise
            price_error = exc
            price_frame = pd.DataFrame()
            evaluation_asof = pd.Timestamp(context.outcome_snapshot.end_date)
        else:
            evaluation_start = price_frame.index.get_level_values("datetime").min()
            evaluation_asof = price_frame.index.get_level_values("datetime").max()
            if evaluation_start < pd.Timestamp(context.outcome_snapshot.start_date) or evaluation_asof > pd.Timestamp(
                context.outcome_snapshot.end_date
            ):
                price_error = QELongTrendError(
                    QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                    "price frame exceeds the declared outcome snapshot window",
                )
                price_frame = pd.DataFrame()
                evaluation_asof = pd.Timestamp(context.outcome_snapshot.end_date)
        family_status = empty_family_statuses()
        sector_error: QELongTrendError | None = None
        if sectors is None:
            sector_frame = None
        else:
            try:
                sector_frame = _normalize_sector_frame(sectors)
            except QELongTrendError as exc:
                if exc.reason_code != QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID.value:
                    raise
                sector_error = exc
                sector_frame = None

        if price_error is not None:
            observations = pd.DataFrame()
            family_status["signal_path"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                available_inputs=("prediction",) if predictions is not None else (),
                missing_inputs=("valid_qe_daily_qfq_price",),
                limitations=(price_error.message,),
                reason_codes=(price_error.reason_code,),
                data_actions=(
                    _family_data_action(
                        "restore_or_reconcile_qe_daily_price_snapshot",
                        "signal_path",
                        required_fields=("close_qfq", "high_qfq", "low_qfq"),
                        source_candidates=(
                            "qe_daily_pv_snapshot",
                            "versioned_qe_outcome_snapshot",
                        ),
                    ),
                ),
            )
        elif predictions is None:
            observations = pd.DataFrame()
            family_status["signal_path"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                missing_inputs=("prediction",),
                reason_codes=(QELongTrendReason.PREDICTION_ARTIFACT_MISSING.value,),
                data_actions=(
                    _family_data_action(
                        "restore_prediction_artifact",
                        "signal_path",
                        required_fields=("signal_date", "instrument", "score"),
                    ),
                ),
            )
        else:
            try:
                prediction_frame = _normalize_prediction_frame(predictions)
                feature_start = pd.Timestamp(context.feature_snapshot.start_date)
                feature_end = pd.Timestamp(context.feature_snapshot.end_date)
                outside_feature = prediction_frame["signal_date"].lt(feature_start) | prediction_frame[
                    "signal_date"
                ].gt(feature_end)
                if bool(outside_feature.any()):
                    raise QELongTrendError(
                        QELongTrendReason.PREDICTION_SCHEMA_INVALID,
                        "prediction signal dates fall outside the immutable feature snapshot window",
                        context={
                            "outside_count": int(outside_feature.sum()),
                            "feature_window": [str(feature_start.date()), str(feature_end.date())],
                        },
                    )
            except QELongTrendError as exc:
                if exc.reason_code != QELongTrendReason.PREDICTION_SCHEMA_INVALID.value:
                    raise
                observations = pd.DataFrame()
                family_status["signal_path"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.NOT_COMPUTABLE,
                    available_inputs=("prediction",),
                    limitations=(exc.message,),
                    reason_codes=(exc.reason_code,),
                    data_actions=(
                        _family_data_action(
                            "repair_prediction_artifact_schema",
                            "signal_path",
                            required_fields=("signal_date", "instrument", "score"),
                        ),
                    ),
                )
            else:
                observations = self._build_normalized_signal_observations(
                    prediction_frame=prediction_frame,
                    price_frame=price_frame,
                    sector_frame=sector_frame,
                    execution_evidence=None,
                    signal_dates_per_chunk=16,
                )
                family_status["signal_path"] = self._signal_family_status(observations)

        execution_errors: list[QELongTrendError] = []
        if not observations.empty and execution_evidence is not None:
            try:
                observations = attach_entry_execution_evidence(
                    observations,
                    evidence=execution_evidence,
                    calendar=pd.DatetimeIndex(sorted(price_frame.index.get_level_values("datetime").unique())),
                )
            except QELongTrendError as exc:
                if exc.reason_code != QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value:
                    raise
                execution_errors.append(exc)

        metrics = self.compute_signal_metrics(observations, strategy_topk=strategy_topk)
        try:
            parity = self._label_parity(
                observations=observations,
                labels=labels,
                label_horizon=label_horizon,
            )
        except QELongTrendError as exc:
            parity = _metric(
                "label_parity",
                "all_oos",
                label_horizon,
                value_json={"reason_code": exc.reason_code, "message": exc.message},
                quality_flag="not_computable",
            )
            current = family_status["signal_path"]
            if current.status in {
                FamilyComputationStatus.COMPUTED,
                FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS,
            }:
                family_status["signal_path"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS,
                    available_inputs=current.available_inputs,
                    missing_inputs=current.missing_inputs + ("valid_label",),
                    coverage=current.coverage,
                    limitations=current.limitations + (exc.message,),
                    supporting_artifacts=current.supporting_artifacts,
                    reason_codes=current.reason_codes + (exc.reason_code,),
                    data_actions=current.data_actions
                    + (
                        _family_data_action(
                            "restore_or_rebuild_label_artifact",
                            "signal_path",
                            required_fields=("label",),
                        ),
                    ),
                )
        if parity is not None:
            metrics.append(parity)
            if parity["quality_flag"] == "computed_with_limitations":
                current = family_status["signal_path"]
                family_status["signal_path"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS,
                    available_inputs=current.available_inputs + ("label",),
                    missing_inputs=current.missing_inputs,
                    coverage=current.coverage,
                    limitations=current.limitations + ("label parity exceeds tolerance",),
                    supporting_artifacts=current.supporting_artifacts,
                    reason_codes=current.reason_codes + (QELongTrendReason.LABEL_PARITY_FAILED.value,),
                    data_actions=current.data_actions
                    + (
                        _family_data_action(
                            "reconcile_label_formula_and_snapshot",
                            "signal_path",
                            required_fields=("label", "close_qfq"),
                        ),
                    ),
                )

        if sectors is None or sector_error is not None:
            family_status["sector_regime"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                missing_inputs=("signal_date_l2_code_id",),
                reason_codes=(QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID.value,),
                limitations=(sector_error.message,) if sector_error is not None else (),
                data_actions=(
                    _family_data_action(
                        "restore_qe_sector_data_l2_code_id",
                        "sector_regime",
                        required_fields=("l2_code_id",),
                        source_candidates=("qe_sector_data_snapshot", "market.sw_index_member_pit"),
                    ),
                ),
            )
        elif observations.empty:
            signal_status = family_status["signal_path"]
            family_status["sector_regime"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                available_inputs=("signal_date_l2_code_id",),
                missing_inputs=("valid_signal_observation",),
                limitations=("sector attribution requires valid signal observations",) + signal_status.limitations,
                reason_codes=signal_status.reason_codes,
                data_actions=(
                    _family_data_action(
                        "restore_sector_signal_observation_dependencies",
                        "sector_regime",
                        required_fields=("prediction", "close_qfq", "l2_code_id"),
                        source_candidates=(
                            "qe_prediction_artifact",
                            "qe_daily_pv_snapshot",
                            "qe_sector_data_snapshot",
                        ),
                    ),
                ),
            )
        else:
            family_status["sector_regime"] = self._sector_family_status(observations)
            metrics.extend(self.compute_sector_metrics(observations))

        if price_error is not None:
            episodes = pd.DataFrame(columns=_EPISODE_COLUMNS)
            family_status["position_episode"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                available_inputs=("position",) if positions is not None else (),
                missing_inputs=("valid_qe_daily_qfq_price",),
                limitations=(price_error.message,),
                reason_codes=(price_error.reason_code,),
                data_actions=(
                    _family_data_action(
                        "restore_or_reconcile_episode_price_snapshot",
                        "position_episode",
                        required_fields=("close_qfq", "high_qfq", "low_qfq"),
                        source_candidates=(
                            "qe_daily_pv_snapshot",
                            "versioned_qe_outcome_snapshot",
                        ),
                    ),
                ),
            )
        elif positions is None:
            episodes = pd.DataFrame(columns=_EPISODE_COLUMNS)
            family_status["position_episode"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                missing_inputs=("position",),
                reason_codes=(QELongTrendReason.POSITION_ARTIFACT_MISSING.value,),
                data_actions=(
                    _family_data_action(
                        "restore_qe_position_artifact",
                        "position_episode",
                        required_fields=("position_date", "instrument", "amount"),
                    ),
                ),
            )
        else:
            try:
                episodes = reconstruct_holding_episodes(
                    positions=positions,
                    prices=price_frame,
                    evaluation_asof=evaluation_asof,
                    profile=self.profile,
                )
            except QELongTrendError as exc:
                if exc.reason_code != QELongTrendReason.EPISODE_RECONCILIATION_FAILED.value:
                    raise
                episodes = pd.DataFrame(columns=_EPISODE_COLUMNS)
                family_status["position_episode"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.NOT_COMPUTABLE,
                    available_inputs=("position", "daily_qfq_price"),
                    limitations=(exc.message,),
                    reason_codes=(exc.reason_code,),
                    data_actions=(
                        _family_data_action(
                            "repair_or_restore_qe_position_artifact",
                            "position_episode",
                            required_fields=("position_date", "instrument", "amount"),
                        ),
                    ),
                )
            else:
                episodes = attach_episode_entry_evidence(episodes, observations)
                if execution_evidence is not None:
                    try:
                        episodes = attach_exit_execution_evidence(
                            episodes,
                            evidence=execution_evidence,
                            prices=price_frame,
                            calendar=pd.DatetimeIndex(sorted(price_frame.index.get_level_values("datetime").unique())),
                            evaluation_asof=evaluation_asof,
                        )
                    except QELongTrendError as exc:
                        if exc.reason_code != QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value:
                            raise
                        execution_errors.append(exc)
                episode_limitations = episodes["episode_quality_flags"].astype(str).str.len().gt(0)
                left_censored_count = int(episodes["left_censored"].sum())
                incomplete_path_count = int(
                    episodes["episode_quality_flags"].astype(str).str.contains("path_incomplete", regex=False).sum()
                )
                known_quality_tokens = {
                    "left_censored_position_history",
                    "episode_path_incomplete",
                    "extended_path_incomplete",
                }
                other_quality_count = sum(
                    any(token and token not in known_quality_tokens for token in str(value).split("|"))
                    for value in episodes["episode_quality_flags"]
                )
                episode_reason_codes: list[str] = []
                episode_limitation_text: list[str] = []
                episode_data_actions: list[dict[str, Any]] = []
                if left_censored_count:
                    episode_reason_codes.append(QELongTrendReason.POSITION_HISTORY_LEFT_CENSORED.value)
                    episode_limitation_text.append(
                        "some holding episodes begin before the first archived position snapshot"
                    )
                    episode_data_actions.append(
                        _family_data_action(
                            "restore_pre_window_qe_position_history",
                            "position_episode",
                            required_fields=("position_date", "instrument", "amount"),
                            source_candidates=(
                                "qe_recorder_position_artifact",
                                "qe_archive_position_rows",
                            ),
                            time_range={
                                "start": "available_pre_run_position_history",
                                "end": "first_archived_position_snapshot",
                            },
                        )
                    )
                if incomplete_path_count:
                    episode_reason_codes.append(QELongTrendReason.PATH_COVERAGE_LOW.value)
                    episode_limitation_text.append("some episode price paths are incomplete")
                    episode_data_actions.append(
                        _family_data_action(
                            "restore_incomplete_episode_price_paths",
                            "position_episode",
                            required_fields=("close_qfq", "high_qfq", "low_qfq"),
                            source_candidates=("qe_daily_pv_snapshot", "qe_snapshot_reexport"),
                        )
                    )
                if other_quality_count:
                    episode_reason_codes.append(QELongTrendReason.EPISODE_RECONCILIATION_FAILED.value)
                    episode_limitation_text.append("some episodes contain unresolved price or calendar evidence")
                    episode_data_actions.append(
                        _family_data_action(
                            "reconcile_episode_price_and_calendar_evidence",
                            "position_episode",
                            required_fields=(
                                "position_date",
                                "close_qfq",
                                "high_qfq",
                                "low_qfq",
                            ),
                            source_candidates=(
                                "qe_recorder_position_artifact",
                                "qe_daily_pv_snapshot",
                            ),
                        )
                    )
                family_status["position_episode"] = FamilyEvidenceStatus(
                    status=(
                        FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
                        if bool(episode_limitations.any())
                        else FamilyComputationStatus.COMPUTED
                    ),
                    available_inputs=("position", "daily_qfq_price"),
                    coverage={
                        "episode_count": int(len(episodes)),
                        "complete_episode_path_rate": (
                            float(episodes["episode_path_coverage"].eq(1.0).mean()) if len(episodes) else None
                        ),
                        "left_censored_episode_count": left_censored_count,
                        "incomplete_episode_path_count": incomplete_path_count,
                        "other_limited_episode_count": other_quality_count,
                    },
                    limitations=tuple(episode_limitation_text),
                    reason_codes=tuple(episode_reason_codes),
                    data_actions=tuple(episode_data_actions),
                )
                metrics.extend(compute_episode_metrics(episodes))

        if execution_errors:
            entry_verifiable = (
                not observations.empty
                and "entry_execution_status" in observations
                and bool(observations["entry_execution_status"].ne("not_verifiable").any())
            )
            exit_verifiable = (
                not episodes.empty
                and "exit_execution_status" in episodes
                and bool(episodes["exit_execution_status"].ne("not_verifiable").any())
            )
            error_messages = tuple(dict.fromkeys(error.message for error in execution_errors))
            error_reasons = tuple(dict.fromkeys(error.reason_code for error in execution_errors))
            if entry_verifiable or exit_verifiable:
                fill, cause = _execution_family_statuses(observations, episodes)
                repair_action = _family_data_action(
                    "repair_qe_execution_artifact_reconciliation",
                    "order_fill",
                    required_fields=("order_intent", "trade", "position_transition"),
                )
                family_status["order_fill"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS,
                    available_inputs=fill.available_inputs,
                    missing_inputs=fill.missing_inputs + ("valid_execution_evidence_subset",),
                    coverage=fill.coverage,
                    limitations=fill.limitations + error_messages,
                    supporting_artifacts=fill.supporting_artifacts,
                    reason_codes=fill.reason_codes + error_reasons,
                    data_actions=fill.data_actions + (repair_action,),
                )
                family_status["execution_cause"] = FamilyEvidenceStatus(
                    status=(
                        FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
                        if cause.status != FamilyComputationStatus.NOT_VERIFIABLE
                        else FamilyComputationStatus.NOT_VERIFIABLE
                    ),
                    available_inputs=cause.available_inputs,
                    missing_inputs=cause.missing_inputs + ("valid_execution_evidence_subset",),
                    coverage=cause.coverage,
                    limitations=cause.limitations + error_messages,
                    supporting_artifacts=cause.supporting_artifacts,
                    reason_codes=cause.reason_codes + error_reasons,
                    data_actions=cause.data_actions
                    + (
                        _family_data_action(
                            "repair_qe_execution_artifact_reconciliation",
                            "execution_cause",
                            required_fields=("reason_code", "order_intent", "trade"),
                        ),
                    ),
                )
                metrics.extend(compute_execution_metrics(observations, episodes))
            else:
                family_status["order_fill"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.NOT_COMPUTABLE,
                    available_inputs=("indicator_order_trade_evidence",),
                    limitations=error_messages,
                    reason_codes=error_reasons,
                    data_actions=(
                        _family_data_action(
                            "repair_qe_execution_artifact_reconciliation",
                            "order_fill",
                            required_fields=("order_intent", "trade", "position_transition"),
                        ),
                    ),
                )
                family_status["execution_cause"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.NOT_VERIFIABLE,
                    limitations=error_messages,
                    reason_codes=error_reasons,
                    data_actions=(
                        _family_data_action(
                            "repair_qe_execution_artifact_reconciliation",
                            "execution_cause",
                            required_fields=("reason_code", "order_intent", "trade"),
                        ),
                    ),
                )
        elif execution_evidence is None or all(
            value is None
            for value in (
                execution_evidence.indicator,
                execution_evidence.trades,
                execution_evidence.orders,
                execution_evidence.exit_signals,
            )
        ):
            family_status["order_fill"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                missing_inputs=("indicator_or_order_trade",),
                reason_codes=(QELongTrendReason.EXECUTION_EVIDENCE_INSUFFICIENT.value,),
                data_actions=(
                    _family_data_action(
                        "archive_qe_indicator_order_trade_evidence",
                        "order_fill",
                        required_fields=("amount", "deal_amount", "ffr", "trade", "position_transition"),
                    ),
                ),
            )
            family_status["execution_cause"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_VERIFIABLE,
                missing_inputs=("queue_or_reason_code",),
                reason_codes=(QELongTrendReason.EXECUTION_EVIDENCE_INSUFFICIENT.value,),
                data_actions=(
                    _family_data_action(
                        "archive_qe_execution_reason_evidence",
                        "execution_cause",
                        required_fields=("reason_code", "order_intent"),
                    ),
                ),
            )
        else:
            fill_verifiable = (
                not observations.empty
                and "entry_execution_status" in observations
                and bool(observations["entry_execution_status"].ne("not_verifiable").any())
            ) or (
                not episodes.empty
                and "exit_execution_status" in episodes
                and bool(episodes["exit_execution_status"].ne("not_verifiable").any())
            )
            cause_verifiable = (
                not observations.empty
                and "entry_block_reason" in observations
                and bool(observations["entry_block_reason"].notna().any())
            ) or (
                not episodes.empty
                and "exit_block_reason" in episodes
                and bool(episodes["exit_block_reason"].notna().any())
            )
            if not fill_verifiable and not cause_verifiable:
                family_status["order_fill"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.NOT_COMPUTABLE,
                    available_inputs=("indicator_order_trade_evidence",),
                    missing_inputs=("reconciled_signal_or_episode_execution_event",),
                    limitations=("execution artifacts could not be mapped to a verifiable signal or episode event",),
                    reason_codes=(QELongTrendReason.EXECUTION_EVIDENCE_INSUFFICIENT.value,),
                    data_actions=(
                        _family_data_action(
                            "restore_execution_event_identity_bridge",
                            "order_fill",
                            required_fields=(
                                "signal_date",
                                "instrument",
                                "order_intent",
                                "trade",
                                "position_transition",
                            ),
                        ),
                    ),
                )
                family_status["execution_cause"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.NOT_VERIFIABLE,
                    available_inputs=("indicator_order_trade_evidence",),
                    missing_inputs=("reconciled_event_reason",),
                    limitations=("execution reasons cannot be attributed without a reconciled event identity",),
                    reason_codes=(QELongTrendReason.EXECUTION_EVIDENCE_INSUFFICIENT.value,),
                    data_actions=(
                        _family_data_action(
                            "restore_execution_cause_identity_bridge",
                            "execution_cause",
                            required_fields=(
                                "signal_date",
                                "instrument",
                                "reason_code",
                                "order_intent",
                            ),
                        ),
                    ),
                )
            else:
                family_status["order_fill"], family_status["execution_cause"] = _execution_family_statuses(
                    observations, episodes
                )
                metrics.extend(compute_execution_metrics(observations, episodes))

        if portfolio_report is None:
            family_status["portfolio_result"] = FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                missing_inputs=("portfolio_report",),
                reason_codes=(QELongTrendReason.PORTFOLIO_REPORT_INVALID.value,),
                data_actions=(
                    _family_data_action(
                        "restore_qe_portfolio_report",
                        "portfolio_result",
                        required_fields=("report_date", "return", "cost", "turnover"),
                    ),
                ),
            )
        else:
            portfolio_execution_errors: list[QELongTrendError] = []
            try:
                executed_trade_count = _execution_evidence_trade_count(execution_evidence)
            except QELongTrendError as exc:
                if exc.reason_code != QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value:
                    raise
                # Execution evidence is supplemental to the authoritative portfolio
                # return series.  Preserve the portfolio result while reporting that
                # the zero-cost/zero-turnover cross-check could not be completed.
                executed_trade_count = None
                portfolio_execution_errors.append(exc)
            try:
                portfolio_metrics = compute_portfolio_metrics(
                    portfolio_report,
                    executed_trade_count=executed_trade_count,
                )
            except QELongTrendError as exc:
                if exc.reason_code != QELongTrendReason.PORTFOLIO_REPORT_INVALID.value:
                    raise
                family_status["portfolio_result"] = FamilyEvidenceStatus(
                    status=FamilyComputationStatus.NOT_COMPUTABLE,
                    available_inputs=("portfolio_report",),
                    limitations=(exc.message,),
                    reason_codes=(exc.reason_code,),
                    data_actions=(
                        _family_data_action(
                            "repair_qe_portfolio_report",
                            "portfolio_result",
                            required_fields=("report_date", "return", "cost", "turnover"),
                        ),
                    ),
                )
            else:
                portfolio_summary = portfolio_metrics[0]["value_json"]
                report_diagnostics_limited = (
                    portfolio_metrics[0]["quality_flag"] == "computed_with_limitations"
                )
                diagnostics_limited = report_diagnostics_limited or bool(portfolio_execution_errors)
                missing_inputs: list[str] = []
                limitations: list[str] = []
                reason_codes: list[str] = []
                data_actions: list[dict[str, Any]] = []
                if report_diagnostics_limited:
                    missing_inputs.append("complete_cost_and_turnover")
                    limitations.append(
                        "portfolio return is authoritative but cost or turnover diagnostics are incomplete"
                    )
                    reason_codes.append(QELongTrendReason.PORTFOLIO_DIAGNOSTICS_INCOMPLETE.value)
                    data_actions.append(
                        _family_data_action(
                            "restore_portfolio_cost_and_turnover_diagnostics",
                            "portfolio_result",
                            required_fields=("cost", "turnover"),
                            source_candidates=("qe_recorder_portfolio_report",),
                        )
                    )
                for execution_error in portfolio_execution_errors:
                    missing_inputs.append("valid_execution_activity_evidence")
                    limitations.append(
                        "portfolio return remains authoritative; execution activity cross-check failed: "
                        f"{execution_error.message}"
                    )
                    reason_codes.append(execution_error.reason_code)
                    data_actions.append(
                        _family_data_action(
                            "repair_qe_execution_activity_evidence",
                            "portfolio_result",
                            required_fields=("evidence_date", "instrument", "deal_amount_or_quantity"),
                            source_candidates=("qe_recorder_indicator", "qe_recorder_trade"),
                        )
                    )
                family_status["portfolio_result"] = FamilyEvidenceStatus(
                    status=(
                        FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
                        if diagnostics_limited
                        else FamilyComputationStatus.COMPUTED
                    ),
                    available_inputs=("portfolio_report",),
                    missing_inputs=tuple(dict.fromkeys(missing_inputs)),
                    coverage={
                        "trading_day_count": portfolio_summary["trading_day_count"],
                        "cost_coverage": portfolio_summary["cost_coverage"],
                        "turnover_coverage": portfolio_summary["turnover_coverage"],
                    },
                    limitations=tuple(dict.fromkeys(limitations)),
                    reason_codes=tuple(dict.fromkeys(reason_codes)),
                    data_actions=tuple(data_actions),
                )
                metrics.extend(portfolio_metrics)

        receipt = self._build_receipt(
            evaluation_asof=evaluation_asof,
            observations=observations,
            episodes=episodes,
            metrics=metrics,
            family_status=family_status,
            context=context,
            evaluation_parameters=evaluation_parameters,
        )
        return LongTrendEvaluationResult(
            evaluation_id=context.evaluation_id(
                profile_sha256=self.profile.profile_sha256,
                evaluation_parameters=evaluation_parameters,
            ),
            profile_id=self.profile.profile_id,
            profile_sha256=self.profile.profile_sha256,
            evaluator_version=EVALUATOR_VERSION,
            evaluation_asof=evaluation_asof.date().isoformat(),
            signal_observations=observations,
            holding_episodes=episodes,
            metrics=metrics,
            family_status=family_status,
            receipt=receipt,
        )

    def compute_signal_metrics(
        self,
        observations: pd.DataFrame,
        *,
        strategy_topk: int | None = None,
    ) -> list[dict[str, Any]]:
        if observations.empty:
            return []
        ks = set(self.profile.fixed_k)
        if strategy_topk is not None:
            if (
                isinstance(strategy_topk, bool)
                or not isinstance(strategy_topk, int)
                or strategy_topk <= 0
                or strategy_topk > self.profile.include_strategy_topk_up_to
            ):
                raise QELongTrendError(
                    QELongTrendReason.PROFILE_INVALID,
                    "strategy_topk must be a positive integer within the registered profile limit",
                )
            ks.add(int(strategy_topk))
        metrics: list[dict[str, Any]] = []
        primary_recall_indices: list[int] = []
        primary_p_values: list[float | None] = []
        for slice_name, slice_frame in _calendar_slices(observations, self.profile.calendar_slices).items():
            for horizon in self.profile.horizons:
                maturity_column = f"maturity_{horizon}"
                return_column = f"return_{horizon}"
                mature = slice_frame.loc[slice_frame[maturity_column] == "matured"].copy()
                maturity_counts = slice_frame[maturity_column].value_counts(dropna=False).to_dict()
                metrics.append(
                    _metric(
                        "maturity",
                        slice_name,
                        horizon,
                        value_json={str(key): int(value) for key, value in maturity_counts.items()},
                        quality_flag="ok" if not mature.empty else "insufficient_maturity",
                    )
                )
                daily_rank_ic = _daily_rank_ic(mature, return_column)
                hac = newey_west_mean_test(daily_rank_ic, lag=horizon - 1)
                rank_ic_bootstrap = moving_block_bootstrap_mean(
                    daily_rank_ic,
                    block_length=horizon,
                    samples=self.profile.bootstrap_samples,
                    seed=self.profile.bootstrap_seed + horizon,
                )
                metrics.append(
                    _metric(
                        "rank_ic",
                        slice_name,
                        horizon,
                        value_num=hac["mean"],
                        value_json={
                            "std": _finite_std(daily_rank_ic),
                            "icir": _safe_ratio(hac["mean"], _finite_std(daily_rank_ic)),
                            "positive_ratio": _positive_ratio(daily_rank_ic),
                            "date_count": hac["n"],
                            "hac_lag": horizon - 1,
                            "hac_se": hac["se"],
                            "raw_p_value": hac["p_value"],
                            "moving_block_bootstrap": rank_ic_bootstrap,
                        },
                        quality_flag="ok" if hac["mean"] is not None else "insufficient_maturity",
                    )
                )
                stage = pd.Series("NONE", index=mature.index, dtype="object")
                direct_hit_probabilities: dict[str, float | None] = {}
                survival_hit_probabilities: dict[str, dict[str, float | int | None]] = {}
                for barrier in self.profile.barriers:
                    suffix = int(round(barrier * 100))
                    hit = pd.to_numeric(
                        mature[f"time_to_close_hit_{suffix}"],
                        errors="coerce",
                    ).le(horizon)
                    stage.loc[hit] = f"HIT{suffix}"
                    direct_hit_probabilities[str(suffix)] = float(hit.mean()) if len(hit) else None
                    survival_hit_probabilities[str(suffix)] = _kaplan_meier_barrier_hit(
                        slice_frame,
                        horizon=horizon,
                        time_column=f"time_to_close_hit_{suffix}",
                    )
                direct_values = [
                    direct_hit_probabilities[str(int(round(value * 100)))] for value in self.profile.barriers
                ]
                finite_direct = [value for value in direct_values if value is not None]
                if any(later > earlier + 1e-12 for earlier, later in zip(finite_direct, finite_direct[1:])):
                    raise QELongTrendError(
                        QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                        "ordered close-barrier probabilities are not monotone",
                    )
                survival_values = [
                    survival_hit_probabilities[str(int(round(value * 100)))]["hit_probability"]
                    for value in self.profile.barriers
                ]
                finite_survival = [float(value) for value in survival_values if value is not None]
                if any(later > earlier + 1e-12 for earlier, later in zip(finite_survival, finite_survival[1:])):
                    raise QELongTrendError(
                        QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                        "ordered survival-adjusted barrier probabilities are not monotone",
                    )
                stage_counts = stage.value_counts().reindex(
                    ["NONE", "HIT30", "HIT50", "HIT70"],
                    fill_value=0,
                )
                metrics.append(
                    _metric(
                        "ordered_trend_stage_survival",
                        slice_name,
                        horizon,
                        value_json={
                            "mature_count": int(len(mature)),
                            "highest_stage_counts": {str(key): int(value) for key, value in stage_counts.items()},
                            "highest_stage_probabilities": {
                                str(key): (float(value / len(stage)) if len(stage) else None)
                                for key, value in stage_counts.items()
                            },
                            "direct_hit_probabilities": direct_hit_probabilities,
                            "kaplan_meier_hit_probabilities": survival_hit_probabilities,
                        },
                        quality_flag="ok" if len(mature) else "insufficient_maturity",
                    )
                )
                for k in sorted(ks):
                    topk = mature.loc[mature["stable_rank"] <= k]
                    daily_topk_return = topk.groupby("signal_date", sort=True)[return_column].mean().dropna().tolist()
                    topk_bootstrap = moving_block_bootstrap_mean(
                        daily_topk_return,
                        block_length=horizon,
                        samples=self.profile.bootstrap_samples,
                        seed=self.profile.bootstrap_seed + horizon + k,
                    )
                    metrics.append(
                        _metric(
                            "topk_return_distribution",
                            slice_name,
                            horizon,
                            k=k,
                            value_json={
                                "return": _distribution(topk[return_column]),
                                "close_mfe": _distribution(topk[f"close_mfe_{horizon}"]),
                                "close_mae": _distribution(topk[f"close_mae_{horizon}"]),
                                "path_mfe": _distribution(topk[f"path_mfe_{horizon}"]),
                                "path_mae": _distribution(topk[f"path_mae_{horizon}"]),
                                "daily_mean_return_bootstrap": topk_bootstrap,
                                "valid_signal_date_count": int(len(daily_topk_return)),
                            },
                            quality_flag="ok" if not topk.empty else "insufficient_maturity",
                        )
                    )
                    for barrier in self.profile.barriers:
                        suffix = int(round(barrier * 100))
                        winner = pd.to_numeric(mature[f"time_to_close_hit_{suffix}"], errors="coerce").le(horizon)
                        selected = mature["stable_rank"].le(k)
                        winner_count = int(winner.sum())
                        captured = int((winner & selected).sum())
                        precision = captured / int(selected.sum()) if int(selected.sum()) else None
                        recall = captured / winner_count if winner_count else None
                        daily_uplift = _daily_recall_uplift(mature, winner, k=k)
                        bootstrap = moving_block_bootstrap_mean(
                            daily_uplift,
                            block_length=horizon,
                            samples=self.profile.bootstrap_samples,
                            seed=self.profile.bootstrap_seed + horizon + suffix + k,
                        )
                        times = pd.to_numeric(
                            mature.loc[winner & selected, f"time_to_close_hit_{suffix}"],
                            errors="coerce",
                        )
                        metric_record = _metric(
                            "barrier_capture",
                            slice_name,
                            horizon,
                            barrier=barrier,
                            k=k,
                            value_json={
                                "precision_at_k": precision,
                                "recall_at_k": recall,
                                "winner_count": winner_count,
                                "captured_count": captured,
                                "time_to_hit": _quantiles(times),
                                "daily_recall_uplift": bootstrap,
                                "aucpr": _daily_average_precision(mature, winner),
                            },
                            quality_flag="ok" if winner_count else "insufficient_maturity",
                        )
                        metrics.append(metric_record)
                        if slice_name == "all_oos" and k == 50:
                            primary_recall_indices.append(len(metrics) - 1)
                            primary_p_values.append(bootstrap["p_value"])
        adjusted = benjamini_hochberg(primary_p_values)
        for metric_index, q_value in zip(primary_recall_indices, adjusted):
            metrics[metric_index]["value_json"]["bh_fdr_q_value"] = q_value
        return metrics

    def compute_sector_metrics(self, observations: pd.DataFrame) -> list[dict[str, Any]]:
        if observations.empty or "l2_code_id" not in observations:
            return []
        sector_ids = pd.to_numeric(observations["l2_code_id"], errors="coerce")
        valid_sector = sector_ids.notna() & sector_ids.ge(0)
        valid = observations.loc[valid_sector].copy()
        if valid.empty:
            return []
        top50 = valid.loc[valid["stable_rank"] <= 50]
        daily_counts = top50.groupby(["signal_date", "l2_code_id"], observed=True).size().rename("count")
        totals = daily_counts.groupby(level="signal_date").sum()
        shares = daily_counts / totals.reindex(daily_counts.index.get_level_values("signal_date")).to_numpy()
        hhi = shares.pow(2).groupby(level="signal_date").sum()
        top1 = shares.groupby(level="signal_date").max()
        dominant = (
            daily_counts.reset_index()
            .sort_values(
                ["signal_date", "count", "l2_code_id"],
                ascending=[True, False, True],
                kind="mergesort",
            )
            .drop_duplicates("signal_date", keep="first")
        )
        dominant.sort_values("signal_date", inplace=True, kind="mergesort")
        transitions = dominant["l2_code_id"].ne(dominant["l2_code_id"].shift()).iloc[1:]
        metrics: list[dict[str, Any]] = [
            _metric(
                "top50_sector_concentration",
                "all_oos",
                None,
                value_json={
                    "mapped_rate": float(valid_sector.mean()),
                    "daily_hhi_mean": _finite_mean(hhi),
                    "daily_top1_sector_share_mean": _finite_mean(top1),
                    "effective_sector_count_mean": _finite_mean(1.0 / hhi.replace(0.0, np.nan)),
                    "top1_sector_switch_count": int(transitions.sum()),
                    "top1_sector_transition_count": int(len(transitions)),
                    "top1_sector_switch_rate": (float(transitions.mean()) if len(transitions) else None),
                },
            )
        ]
        metrics[0]["metric_scope"] = "sector_regime"
        for slice_name, slice_frame in _calendar_slices(valid, self.profile.calendar_slices).items():
            for horizon in self.profile.horizons:
                mature = slice_frame.loc[slice_frame[f"maturity_{horizon}"] == "matured"]
                for sector_code, sector_frame in mature.groupby("l2_code_id", observed=True, sort=True):
                    selected = sector_frame["stable_rank"].le(50)
                    barrier_metrics: dict[str, Any] = {}
                    for barrier in self.profile.barriers:
                        suffix = int(round(barrier * 100))
                        winner = pd.to_numeric(sector_frame[f"time_to_close_hit_{suffix}"], errors="coerce").le(horizon)
                        winner_count = int(winner.sum())
                        captured = int((winner & selected).sum())
                        barrier_metrics[str(suffix)] = {
                            "winner_count": winner_count,
                            "captured_count": captured,
                            "precision_at_50": (captured / int(selected.sum()) if int(selected.sum()) else None),
                            "recall_at_50": captured / winner_count if winner_count else None,
                            "time_to_hit": _quantiles(
                                pd.to_numeric(
                                    sector_frame.loc[
                                        winner & selected,
                                        f"time_to_close_hit_{suffix}",
                                    ],
                                    errors="coerce",
                                )
                            ),
                        }
                    metrics.append(
                        {
                            "metric_scope": "sector_regime",
                            "metric_key": "sector_signal_path",
                            "slice": slice_name,
                            "horizon": horizon,
                            "sector_code": int(sector_code),
                            "barrier": None,
                            "k": 50,
                            "value_num": None,
                            "value_json": {
                                "sample_count": int(len(sector_frame)),
                                "top50_count": int(selected.sum()),
                                "return": _distribution(sector_frame[f"return_{horizon}"]),
                                "close_mfe": _distribution(sector_frame[f"close_mfe_{horizon}"]),
                                "close_mae": _distribution(sector_frame[f"close_mae_{horizon}"]),
                                "path_mfe": _distribution(sector_frame[f"path_mfe_{horizon}"]),
                                "path_mae": _distribution(sector_frame[f"path_mae_{horizon}"]),
                                "barriers": barrier_metrics,
                            },
                            "quality_flag": "ok",
                        }
                    )
        return metrics

    def _signal_family_status(self, observations: pd.DataFrame) -> FamilyEvidenceStatus:
        if observations.empty:
            return FamilyEvidenceStatus(
                status=FamilyComputationStatus.NOT_COMPUTABLE,
                missing_inputs=("prediction_or_price",),
            )
        max_horizon = max(self.profile.horizons)
        entry_expected = observations["entry_date"].notna()
        entry_coverage = (
            float(observations.loc[entry_expected, "entry_close_qfq"].notna().mean())
            if bool(entry_expected.any())
            else 1.0
        )
        mature_counts = {
            str(horizon): int((observations[f"maturity_{horizon}"] == "matured").sum())
            for horizon in self.profile.horizons
        }
        reasons: list[str] = []
        limitations: list[str] = []
        data_actions: list[dict[str, Any]] = []
        insufficient_horizons = [horizon for horizon in self.profile.horizons if mature_counts[str(horizon)] == 0]
        if insufficient_horizons:
            reasons.append(QELongTrendReason.INSUFFICIENT_MATURITY.value)
            limitations.append(
                "no mature observations for horizons " + ",".join(str(value) for value in insufficient_horizons)
            )
            data_actions.append(
                _family_data_action(
                    "reevaluate_when_outcome_snapshot_extends",
                    "signal_path",
                    required_fields=("close_qfq", "high_qfq", "low_qfq"),
                    source_candidates=("versioned_qe_outcome_snapshot",),
                    horizons=insufficient_horizons,
                )
            )
        if entry_coverage < self.profile.entry_coverage_reference:
            reasons.append(QELongTrendReason.ENTRY_COVERAGE_LOW.value)
            limitations.append("entry price coverage is below the profile reference")
            data_actions.append(
                _family_data_action(
                    "restore_missing_qe_entry_prices",
                    "signal_path",
                    required_fields=("close_qfq",),
                    source_candidates=("qe_daily_pv_snapshot", "qe_snapshot_reexport"),
                )
            )
        max_maturity = observations[f"maturity_{max_horizon}"]
        path_expected = ~max_maturity.isin({"right_censored", "invalid_entry"})
        max_path_coverage = (
            float(
                observations.loc[path_expected, f"observed_steps_{max_horizon}"].sum()
                / max(int(path_expected.sum()) * max_horizon, 1)
            )
            if bool(path_expected.any())
            else None
        )
        max_high_low_coverage = (
            float(
                observations.loc[
                    path_expected,
                    f"observed_high_low_steps_{max_horizon}",
                ].sum()
                / max(int(path_expected.sum()) * max_horizon, 1)
            )
            if bool(path_expected.any())
            else None
        )
        if max_path_coverage is not None and max_path_coverage < self.profile.path_coverage_reference:
            reasons.append(QELongTrendReason.PATH_COVERAGE_LOW.value)
            limitations.append("maximum-horizon path coverage is below the profile reference")
            data_actions.append(
                _family_data_action(
                    "restore_missing_qe_price_path",
                    "signal_path",
                    required_fields=("close_qfq",),
                    source_candidates=("qe_daily_pv_snapshot", "qe_snapshot_reexport"),
                )
            )
        if max_high_low_coverage is not None and max_high_low_coverage < self.profile.path_coverage_reference:
            if QELongTrendReason.PATH_COVERAGE_LOW.value not in reasons:
                reasons.append(QELongTrendReason.PATH_COVERAGE_LOW.value)
            limitations.append("maximum-horizon high/low path coverage is below the profile reference")
            data_actions.append(
                _family_data_action(
                    "restore_missing_qe_high_low_path",
                    "signal_path",
                    required_fields=("high_qfq", "low_qfq"),
                    source_candidates=("qe_daily_pv_snapshot", "qe_snapshot_reexport"),
                )
            )
        unresolved_exit_count = int((observations[f"maturity_{max_horizon}"] == "instrument_exit_unresolved").sum())
        if unresolved_exit_count:
            reasons.append(QELongTrendReason.INSTRUMENT_EXIT_UNRESOLVED.value)
            limitations.append(
                "some instruments terminate before the requested horizon without an authoritative exit outcome"
            )
            data_actions.append(
                _family_data_action(
                    "resolve_instrument_exit_outcome",
                    "signal_path",
                    required_fields=("instrument_event", "close_qfq"),
                    source_candidates=("qe_instrument_pit", "qe_snapshot_reexport"),
                    unresolved_count=unresolved_exit_count,
                )
            )
        status = FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS if limitations else FamilyComputationStatus.COMPUTED
        return FamilyEvidenceStatus(
            status=status,
            available_inputs=("prediction", "daily_qfq_price"),
            coverage={
                "signal_count": int(len(observations)),
                "entry_coverage": entry_coverage,
                "max_horizon_path_coverage": max_path_coverage,
                "max_horizon_high_low_coverage": max_high_low_coverage,
                "instrument_exit_unresolved_count": unresolved_exit_count,
                "mature_counts": mature_counts,
            },
            limitations=tuple(limitations),
            reason_codes=tuple(reasons),
            data_actions=tuple(data_actions),
        )

    def _sector_family_status(self, observations: pd.DataFrame) -> FamilyEvidenceStatus:
        if observations.empty:
            coverage = 0.0
        else:
            sector_ids = pd.to_numeric(observations["l2_code_id"], errors="coerce")
            coverage = float((sector_ids.notna() & sector_ids.ge(0)).mean())
        limited = coverage < self.profile.sector_coverage_reference
        return FamilyEvidenceStatus(
            status=(FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS if limited else FamilyComputationStatus.COMPUTED),
            available_inputs=("signal_date_l2_code_id",),
            coverage={"sector_coverage": coverage},
            limitations=("sector coverage is below the profile reference",) if limited else (),
            reason_codes=(QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID.value,) if limited else (),
            data_actions=(
                _family_data_action(
                    "backfill_missing_signal_date_l2_code_id",
                    "sector_regime",
                    required_fields=("l2_code_id",),
                    source_candidates=("qe_sector_data_snapshot", "market.sw_index_member_pit"),
                ),
            )
            if limited
            else (),
        )

    def _label_parity(
        self,
        *,
        observations: pd.DataFrame,
        labels: pd.DataFrame | pd.Series | None,
        label_horizon: int | None,
    ) -> dict[str, Any] | None:
        label_series = _normalize_label_frame(labels)
        if label_series is None:
            return None
        if observations.empty:
            return _metric(
                "label_parity",
                "all_oos",
                label_horizon,
                value_json={"compared_count": 0, "reason": "signal observations unavailable"},
                quality_flag="not_computable",
            )
        if label_horizon not in self.profile.horizons:
            raise QELongTrendError(
                QELongTrendReason.PROFILE_INVALID,
                f"label_horizon {label_horizon!r} is not in the registered profile",
            )
        keyed = observations.set_index(["signal_date", "instrument"])
        joined = keyed[[f"return_{label_horizon}", f"maturity_{label_horizon}"]].join(
            label_series.rename("label"),
            how="inner",
        )
        joined = joined.loc[
            (joined[f"maturity_{label_horizon}"] == "matured")
            & joined[f"return_{label_horizon}"].notna()
            & joined["label"].notna()
        ]
        differences = joined[f"return_{label_horizon}"].to_numpy(dtype="float64") - joined["label"].to_numpy(
            dtype="float64"
        )
        if differences.size == 0:
            raise QELongTrendError(
                QELongTrendReason.LABEL_PARITY_NO_OVERLAP,
                "label parity has no mature overlapping signal rows",
                context={"label_horizon": label_horizon},
            )
        max_abs = float(np.max(np.abs(differences))) if differences.size else None
        mismatch_count = int(np.sum(np.abs(differences) > 1e-6)) if differences.size else 0
        return _metric(
            "label_parity",
            "all_oos",
            label_horizon,
            value_json={
                "compared_count": int(differences.size),
                "mismatch_count": mismatch_count,
                "max_abs_diff": max_abs,
                "tolerance": 1e-6,
            },
            quality_flag="computed_with_limitations" if mismatch_count else "ok",
        )

    def _build_receipt(
        self,
        *,
        evaluation_asof: pd.Timestamp,
        observations: pd.DataFrame,
        episodes: pd.DataFrame,
        metrics: list[dict[str, Any]],
        family_status: Mapping[str, FamilyEvidenceStatus],
        context: QELongTrendEvaluationContext,
        evaluation_parameters: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema_version": self.profile.schema_version,
            "profile_id": self.profile.profile_id,
            "profile_sha256": self.profile.profile_sha256,
            "evaluator_version": EVALUATOR_VERSION,
            "evaluation_asof": evaluation_asof.date().isoformat(),
            "evaluation_context": context.as_dict(
                profile_sha256=self.profile.profile_sha256,
                evaluation_parameters=evaluation_parameters,
            ),
            "family_status": {name: value.as_dict() for name, value in family_status.items()},
            "stats": {
                "signal_observation_rows": int(len(observations)),
                "holding_episode_rows": int(len(episodes)),
                "metric_count": int(len(metrics)),
                "data_action_count": int(sum(len(value.data_actions) for value in family_status.values())),
            },
            "platform_delivery_status": {
                "core_compute": "verified_phase1",
                "cas": "pending_phase2",
                "database": "pending_phase3",
                "api": "pending_phase3",
                "mcp": "pending_phase3",
                "ui": "pending_phase4",
                "backfill": "pending_phase4",
                "e2e": "pending_phase5",
            },
            "no_training": True,
            "no_backtest": True,
            "no_live_data_access": True,
        }


def _first_hit_steps(path_returns: np.ndarray, barrier: float) -> np.ndarray:
    row_count = path_returns.shape[1] if path_returns.ndim == 2 else 0
    result = np.full(row_count, np.nan, dtype="float64")
    if path_returns.size == 0:
        return result
    hit = np.isfinite(path_returns) & (path_returns >= barrier)
    any_hit = hit.any(axis=0)
    result[any_hit] = np.argmax(hit[:, any_hit], axis=0) + 1
    return result


def _calendar_slices(
    observations: pd.DataFrame,
    slice_names: Iterable[str],
) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    required_positions = {"signal_calendar_position", "evaluation_calendar_position"}
    if not required_positions.issubset(observations.columns):
        raise QELongTrendError(
            QELongTrendReason.PREDICTION_SCHEMA_INVALID,
            "signal observations are missing trading-calendar positions",
        )
    evaluation_position = int(observations["evaluation_calendar_position"].max())
    for name in slice_names:
        if name == "all_oos":
            mask = pd.Series(True, index=observations.index)
        elif name == "last_252_signal_days":
            mask = observations["signal_calendar_position"] >= evaluation_position - 251
        elif name == "last_126_signal_days":
            mask = observations["signal_calendar_position"] >= evaluation_position - 125
        else:
            raise QELongTrendError(QELongTrendReason.PROFILE_INVALID, f"unknown calendar slice {name!r}")
        result[name] = observations.loc[mask]
    return result


def _daily_rank_ic(frame: pd.DataFrame, return_column: str) -> list[float]:
    values: list[float] = []
    for _, group in frame.groupby("signal_date", sort=True):
        valid = group.loc[:, ["score", return_column]].dropna()
        if len(valid) < 2 or valid["score"].nunique(dropna=True) < 2 or valid[return_column].nunique(dropna=True) < 2:
            continue
        score_rank = valid["score"].rank(method="average")
        return_rank = valid[return_column].rank(method="average")
        value = score_rank.corr(return_rank)
        if value is not None and math.isfinite(float(value)):
            values.append(float(value))
    return values


def _daily_recall_uplift(frame: pd.DataFrame, winner: pd.Series, *, k: int) -> list[float]:
    values: list[float] = []
    enriched = frame.loc[:, ["signal_date", "stable_rank"]].copy()
    enriched["winner"] = winner.to_numpy(dtype=bool)
    for _, group in enriched.groupby("signal_date", sort=True):
        winners = int(group["winner"].sum())
        if winners == 0 or len(group) == 0:
            continue
        captured = int((group["winner"] & group["stable_rank"].le(k)).sum())
        observed = captured / winners
        eligible_selected_count = int(group["stable_rank"].le(k).sum())
        random_expected = eligible_selected_count / len(group)
        values.append(float(observed - random_expected))
    return values


def _kaplan_meier_barrier_hit(
    frame: pd.DataFrame,
    *,
    horizon: int,
    time_column: str,
) -> dict[str, float | int | None]:
    """Estimate close-barrier hit probability while retaining right-censored rows."""

    if frame.empty:
        return {
            "sample_count": 0,
            "event_count": 0,
            "censored_count": 0,
            "hit_probability": None,
        }
    durations = pd.to_numeric(
        frame[f"observed_prefix_steps_{horizon}"],
        errors="coerce",
    ).clip(lower=0, upper=horizon)
    event_times = pd.to_numeric(frame[time_column], errors="coerce")
    valid_entry = frame[f"maturity_{horizon}"].ne("invalid_entry") & durations.notna()
    durations = durations.loc[valid_entry].astype("int64")
    event_times = event_times.loc[valid_entry]
    event = event_times.notna() & event_times.le(durations) & event_times.le(horizon)
    observed_time = durations.astype("float64")
    observed_time.loc[event] = event_times.loc[event]
    positive_followup = observed_time.gt(0.0)
    observed_time = observed_time.loc[positive_followup]
    event = event.loc[positive_followup]
    if observed_time.empty:
        return {
            "sample_count": 0,
            "event_count": 0,
            "censored_count": 0,
            "hit_probability": None,
        }
    survival = 1.0
    for step in sorted(observed_time.loc[event].astype("int64").unique()):
        at_risk = int(observed_time.ge(step).sum())
        events = int((event & observed_time.eq(float(step))).sum())
        if at_risk <= 0 or events > at_risk:
            raise QELongTrendError(
                QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                "invalid at-risk set while computing barrier survival",
            )
        survival *= 1.0 - events / at_risk
    event_count = int(event.sum())
    return {
        "sample_count": int(len(observed_time)),
        "event_count": event_count,
        "censored_count": int(len(observed_time) - event_count),
        "hit_probability": float(1.0 - survival),
    }


def _average_precision(scores: np.ndarray, targets: np.ndarray, instruments: np.ndarray) -> float | None:
    positive_count = int(targets.sum())
    if positive_count == 0:
        return None
    order = np.lexsort((instruments.astype(str), -scores))
    ordered = targets[order].astype(bool)
    cumulative = np.cumsum(ordered)
    positive_positions = np.flatnonzero(ordered)
    precision = cumulative[positive_positions] / (positive_positions + 1)
    return float(precision.mean())


def _daily_average_precision(frame: pd.DataFrame, winner: pd.Series) -> float | None:
    enriched = frame.loc[:, ["signal_date", "instrument", "score"]].copy()
    enriched["winner"] = winner.to_numpy(dtype=bool)
    values: list[float] = []
    for _, group in enriched.groupby("signal_date", sort=True):
        value = _average_precision(
            group["score"].to_numpy(dtype="float64"),
            group["winner"].to_numpy(dtype=bool),
            group["instrument"].to_numpy(dtype=str),
        )
        if value is not None:
            values.append(value)
    return _finite_mean(values)


def _metric(
    metric_key: str,
    slice_name: str,
    horizon: int | None,
    *,
    barrier: float | None = None,
    k: int | None = None,
    value_num: float | int | None = None,
    value_json: Mapping[str, Any] | None = None,
    quality_flag: str = "ok",
) -> dict[str, Any]:
    return {
        "metric_scope": "signal_path",
        "metric_key": metric_key,
        "slice": slice_name,
        "horizon": horizon,
        "barrier": barrier,
        "k": k,
        "value_num": value_num,
        "value_json": dict(value_json or {}),
        "quality_flag": quality_flag,
    }


def _family_data_action(
    action: str,
    family: str,
    *,
    required_fields: tuple[str, ...],
    source_candidates: tuple[str, ...] = ("qe_recorder", "qe_archive", "qe_only_cas"),
    historical_backfill: bool = True,
    **extra: Any,
) -> dict[str, Any]:
    return data_action(
        action=action,
        recoverable_family=family,
        source_candidates=source_candidates,
        required_fields=required_fields,
        time_range={"start": "run_signal_start", "end": "evaluation_asof"},
        historical_backfill=historical_backfill,
        **extra,
    )


def _distribution(values: pd.Series) -> dict[str, float | int | None]:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype="float64")
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return {"count": 0, "mean": None, "median": None, "p10": None, "p50": None, "p90": None, "positive_ratio": None}
    p10, p50, p90 = np.quantile(arr, [0.1, 0.5, 0.9])
    return {
        "count": int(arr.size),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "p10": float(p10),
        "p50": float(p50),
        "p90": float(p90),
        "positive_ratio": float(np.mean(arr > 0.0)),
    }


def _quantiles(values: pd.Series) -> dict[str, float | int | None]:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype="float64")
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return {"count": 0, "p25": None, "p50": None, "p75": None}
    p25, p50, p75 = np.quantile(arr, [0.25, 0.5, 0.75])
    return {"count": int(arr.size), "p25": float(p25), "p50": float(p50), "p75": float(p75)}


def _finite_mean(values: Iterable[float]) -> float | None:
    arr = np.asarray(list(values), dtype="float64")
    arr = arr[np.isfinite(arr)]
    return float(arr.mean()) if arr.size else None


def _finite_std(values: Iterable[float]) -> float | None:
    arr = np.asarray(list(values), dtype="float64")
    arr = arr[np.isfinite(arr)]
    return float(arr.std(ddof=1)) if arr.size >= 2 else None


def _positive_ratio(values: Iterable[float]) -> float | None:
    arr = np.asarray(list(values), dtype="float64")
    arr = arr[np.isfinite(arr)]
    return float(np.mean(arr > 0.0)) if arr.size else None


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return float(numerator / denominator)


def _normalize_evidence_frame(frame: pd.DataFrame | None, *, kind: str) -> pd.DataFrame | None:
    if frame is None:
        return None
    if not isinstance(frame, pd.DataFrame):
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            f"{kind} evidence must be a DataFrame",
        )
    normalized = frame.copy(deep=True)
    if isinstance(normalized.index, pd.MultiIndex):
        normalized = normalized.reset_index()
    if "trade_price" in normalized.columns and "price" not in normalized.columns:
        normalized = normalized.rename(columns={"trade_price": "price"})
    if "trade_cost" in normalized.columns and "fees" not in normalized.columns:
        normalized = normalized.rename(columns={"trade_cost": "fees"})
    if "trade_dir" in normalized.columns and "side" not in normalized.columns:
        normalized = normalized.rename(columns={"trade_dir": "side"})
    if "signal_date" in normalized.columns:
        normalized["source_signal_date"] = pd.to_datetime(
            normalized["signal_date"],
            errors="coerce",
        ).dt.normalize()
    date_column = next(
        (
            name
            for name in ("datetime", "date", "trade_date", "evidence_date", "signal_date")
            if name in normalized.columns
        ),
        None,
    )
    if date_column is None or "instrument" not in normalized.columns:
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            f"{kind} evidence requires date and instrument columns",
        )
    if date_column != "evidence_date":
        normalized = normalized.rename(columns={date_column: "evidence_date"})
    normalized["evidence_date"] = pd.to_datetime(normalized["evidence_date"], errors="coerce").dt.normalize()
    if normalized["evidence_date"].isna().any():
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            f"{kind} evidence contains invalid dates",
        )
    try:
        normalized["instrument"] = normalized["instrument"].map(canonicalize_instrument)
    except ValueError as exc:
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            str(exc),
        ) from exc
    for column in ("amount", "deal_amount", "ffr", "quantity", "price", "fees"):
        if column in normalized.columns:
            original_non_null = normalized[column].notna()
            numeric = pd.to_numeric(normalized[column], errors="coerce")
            invalid = original_non_null & (~np.isfinite(numeric.to_numpy(dtype="float64")))
            if bool(invalid.any()):
                raise QELongTrendError(
                    QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                    f"{kind} evidence contains invalid numeric {column} values",
                    context={"invalid_count": int(invalid.sum())},
                )
            normalized[column] = numeric
    if "side" in normalized.columns:
        normalized["side"] = normalized["side"].astype(str).str.strip().str.lower()
    return normalized.sort_values(["instrument", "evidence_date"], kind="mergesort").reset_index(drop=True)


def _trade_side_frame(trades: pd.DataFrame | None, *, side: str) -> pd.DataFrame | None:
    normalized = _normalize_evidence_frame(trades, kind="trade")
    if normalized is None or normalized.empty:
        return normalized
    if side not in {"buy", "sell"}:
        raise ValueError(f"unsupported trade side {side!r}")
    if "side" in normalized.columns:
        accepted = {"buy", "b", "1", "long"} if side == "buy" else {"sell", "s", "0", "short"}
        mask = normalized["side"].isin(accepted)
    elif "quantity" in normalized.columns:
        mask = normalized["quantity"] > 0 if side == "buy" else normalized["quantity"] < 0
    else:
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            "trade evidence requires side or signed quantity",
        )
    return normalized.loc[mask].copy()


def _filter_order_like_side(
    frame: pd.DataFrame | None,
    *,
    side: str,
    allow_unspecified_for_entry: bool,
) -> pd.DataFrame | None:
    if frame is None or frame.empty:
        return frame
    if side not in {"buy", "sell"}:
        raise ValueError(f"unsupported evidence side {side!r}")
    selected = frame.copy()
    if "side" in selected.columns:
        accepted = {"buy", "b", "1", "long"} if side == "buy" else {"sell", "s", "0", "short"}
        selected = selected.loc[selected["side"].isin(accepted)].copy()
    elif "amount" in selected.columns and (selected["amount"].dropna() < 0.0).any():
        selected = selected.loc[selected["amount"] > 0.0 if side == "buy" else selected["amount"] < 0.0].copy()
    elif side == "sell" or not allow_unspecified_for_entry:
        return selected.iloc[0:0].copy()
    if side == "sell":
        for column in ("amount", "deal_amount", "quantity"):
            if column in selected:
                selected[column] = selected[column].abs()
    return selected


def _aggregate_trade_rows(frame: pd.DataFrame | None) -> pd.DataFrame | None:
    if frame is None or frame.empty:
        return frame
    grouping = ["evidence_date", "instrument"]
    if "source_signal_date" in frame.columns:
        frame = frame.copy()
        frame["source_signal_date"] = frame["source_signal_date"].fillna(pd.NaT)
        grouping.append("source_signal_date")
    records: list[dict[str, Any]] = []
    for keys, group in frame.groupby(grouping, sort=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        record = dict(zip(grouping, keys))
        quantity = (
            pd.to_numeric(group["quantity"], errors="coerce").abs()
            if "quantity" in group
            else pd.Series(np.nan, index=group.index)
        )
        valid_quantity = quantity.where(quantity > 0.0)
        record["quantity"] = float(valid_quantity.sum()) if valid_quantity.notna().any() else None
        if "price" in group and group["price"].notna().any():
            price = pd.to_numeric(group["price"], errors="coerce")
            weighted = price.notna() & valid_quantity.notna()
            if bool(weighted.any()) and float(valid_quantity.loc[weighted].sum()) > 0.0:
                record["price"] = float(
                    (price.loc[weighted] * valid_quantity.loc[weighted]).sum() / valid_quantity.loc[weighted].sum()
                )
            else:
                record["price"] = float(price.dropna().iloc[0])
        else:
            record["price"] = None
        record["fees"] = (
            float(pd.to_numeric(group["fees"], errors="coerce").sum())
            if "fees" in group and group["fees"].notna().any()
            else None
        )
        record["trade_count"] = int(len(group))
        records.append(record)
    return (
        pd.DataFrame.from_records(records)
        .sort_values(
            ["instrument", "evidence_date"],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )


def _match_trade_rows(
    targets: pd.DataFrame,
    trades: pd.DataFrame | None,
    *,
    expected_date_column: str,
    signal_date_column: str,
) -> tuple[dict[int, pd.Series], set[int]]:
    """Match each normalized trade to at most one signal without guessing ambiguity."""

    if trades is None or trades.empty or targets.empty:
        return {}, set()
    aggregated = _aggregate_trade_rows(trades)
    if aggregated is None or aggregated.empty:
        return {}, set()
    assignments: dict[int, pd.Series] = {}
    ambiguous: set[int] = set()
    consumed: set[int] = set()
    for instrument, target_indices in targets.groupby("instrument", sort=True).groups.items():
        candidate_indices = list(target_indices)
        unassigned = set(candidate_indices)
        signal_date_map: dict[pd.Timestamp, list[int]] = {}
        expected_date_map: dict[pd.Timestamp, list[int]] = {}
        expected_pairs: list[tuple[pd.Timestamp, int]] = []
        for index in candidate_indices:
            signal_date = pd.Timestamp(targets.at[index, signal_date_column])
            signal_date_map.setdefault(signal_date, []).append(index)
            expected_value = targets.at[index, expected_date_column]
            if pd.notna(expected_value):
                expected_date = pd.Timestamp(expected_value)
                expected_date_map.setdefault(expected_date, []).append(index)
                expected_pairs.append((expected_date, index))
        expected_pairs.sort(key=lambda item: (item[0], item[1]))
        expected_dates = [item[0] for item in expected_pairs]
        instrument_trades = aggregated.loc[aggregated["instrument"] == instrument]
        for trade_index, trade in instrument_trades.iterrows():
            if trade_index in consumed:
                continue
            trade_date = pd.Timestamp(trade["evidence_date"])
            source_signal_date = trade.get("source_signal_date")
            if source_signal_date is not None and pd.notna(source_signal_date):
                matched = [
                    index for index in signal_date_map.get(pd.Timestamp(source_signal_date), ()) if index in unassigned
                ]
                if len(matched) != 1:
                    raise QELongTrendError(
                        QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                        "explicit trade signal identity does not resolve to exactly one target",
                        context={
                            "instrument": instrument,
                            "source_signal_date": str(pd.Timestamp(source_signal_date).date()),
                            "candidate_count": len(matched),
                        },
                    )
                expected_value = targets.at[matched[0], expected_date_column]
                if pd.isna(expected_value) or trade_date < pd.Timestamp(expected_value):
                    raise QELongTrendError(
                        QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                        "explicit trade signal identity resolves before the target execution date",
                        context={
                            "instrument": instrument,
                            "trade_date": str(trade_date.date()),
                            "expected_date": (
                                str(pd.Timestamp(expected_value).date()) if pd.notna(expected_value) else None
                            ),
                        },
                    )
                assignments[matched[0]] = trade
                unassigned.remove(matched[0])
                consumed.add(trade_index)
                continue
            exact = [index for index in expected_date_map.get(trade_date, ()) if index in unassigned]
            if len(exact) == 1:
                assignments[exact[0]] = trade
                unassigned.remove(exact[0])
                consumed.add(trade_index)
                continue
            if len(exact) > 1:
                ambiguous.update(exact)
                continue
            delayed_limit = bisect_left(expected_dates, trade_date)
            eligible = [index for _, index in expected_pairs[:delayed_limit] if index in unassigned]
            if len(eligible) == 1:
                assignments[eligible[0]] = trade
                unassigned.remove(eligible[0])
                consumed.add(trade_index)
            elif len(eligible) > 1:
                ambiguous.update(eligible)
    return assignments, ambiguous


def attach_entry_execution_evidence(
    observations: pd.DataFrame,
    *,
    evidence: ExecutionEvidenceBundle | None,
    calendar: pd.DatetimeIndex,
) -> pd.DataFrame:
    """Attach entry execution status without inferring causes from daily prices."""

    result = observations.reset_index(drop=True).copy()
    if result.empty:
        for column in (
            "entry_execution_status",
            "entry_execution_evidence_level",
            "actual_entry_date",
            "actual_entry_price",
            "entry_delay_days",
            "entry_block_reason",
            "missed_mfe_due_to_entry_block",
            "missed_barrier_winner_due_to_entry_block",
        ):
            result[column] = pd.Series(dtype="object")
        return result

    row_count = len(result)
    status = np.full(row_count, "not_verifiable", dtype=object)
    evidence_level = np.full(row_count, "none", dtype=object)
    actual_date = np.full(row_count, np.datetime64("NaT"), dtype="datetime64[ns]")
    actual_price = np.full(row_count, np.nan, dtype="float64")
    delay_days = np.full(row_count, np.nan, dtype="float64")
    block_reason = np.full(row_count, None, dtype=object)

    if evidence is not None:
        indicator = _filter_order_like_side(
            _normalize_evidence_frame(evidence.indicator, kind="indicator"),
            side="buy",
            allow_unspecified_for_entry=True,
        )
        orders = _filter_order_like_side(
            _normalize_evidence_frame(evidence.orders, kind="order"),
            side="buy",
            allow_unspecified_for_entry=True,
        )
        buy_trades = _trade_side_frame(evidence.trades, side="buy")

        keys = pd.MultiIndex.from_arrays(
            [pd.to_datetime(result["entry_date"]), result["instrument"]],
            names=["evidence_date", "instrument"],
        )
        indicator_lookup = _aggregate_indicator(indicator).reindex(keys) if indicator is not None else None
        order_lookup = _aggregate_orders(orders).reindex(keys) if orders is not None else None

        trade_assignments, ambiguous_trade_rows = _match_trade_rows(
            result,
            buy_trades,
            expected_date_column="entry_date",
            signal_date_column="signal_date",
        )
        if ambiguous_trade_rows:
            for row_index in ambiguous_trade_rows:
                evidence_level[row_index] = "ambiguous_trade_match"

        if trade_assignments:
            calendar_positions = {value: index for index, value in enumerate(calendar)}
            for row_index, trade in trade_assignments.items():
                trade_date = pd.Timestamp(trade["evidence_date"])
                expected = pd.Timestamp(result.at[row_index, "entry_date"])
                actual_date[row_index] = trade_date.to_datetime64()
                if pd.notna(trade.get("price")):
                    actual_price[row_index] = float(trade["price"])
                expected_position = calendar_positions.get(expected)
                actual_position = calendar_positions.get(trade_date)
                if expected_position is None or actual_position is None:
                    raise QELongTrendError(
                        QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                        "entry trade or expected date is outside the QE evaluation calendar",
                        context={
                            "expected_date": str(expected.date()),
                            "trade_date": str(trade_date.date()),
                        },
                    )
                if actual_position < expected_position:
                    raise QELongTrendError(
                        QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                        "entry trade precedes the expected T+1 execution date",
                    )
                delay_days[row_index] = float(actual_position - expected_position)
                status[row_index] = "filled_t1" if trade_date == expected else "delayed_fill"
                evidence_level[row_index] = "reconciled_trade"

        if indicator_lookup is not None:
            amount = pd.to_numeric(indicator_lookup.get("amount"), errors="coerce").to_numpy(dtype="float64")
            deal = pd.to_numeric(indicator_lookup.get("deal_amount"), errors="coerce").to_numpy(dtype="float64")
            ffr = pd.to_numeric(indicator_lookup.get("ffr"), errors="coerce").to_numpy(dtype="float64")
            attempted = np.isfinite(amount) & (amount > 0.0)
            partial = attempted & (
                (np.isfinite(deal) & (deal > 0.0) & (deal < amount)) | (np.isfinite(ffr) & (ffr > 0.0) & (ffr < 1.0))
            )
            full_fill = attempted & ((np.isfinite(deal) & (deal >= amount)) | (np.isfinite(ffr) & (ffr >= 1.0)))
            explicit_zero = np.isfinite(amount) & (amount == 0.0) & (np.nan_to_num(deal, nan=0.0) == 0.0)
            attempted_zero_fill = (
                attempted & (np.nan_to_num(deal, nan=0.0) == 0.0) & (np.nan_to_num(ffr, nan=0.0) == 0.0)
            )
            trade_t1 = status == "filled_t1"
            trade_delayed = status == "delayed_fill"
            conflict = (trade_t1 & (explicit_zero | attempted_zero_fill)) | (trade_delayed & (full_fill | partial))
            if bool(conflict.any()):
                raise QELongTrendError(
                    QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                    "indicator fill evidence conflicts with reconciled trade timing",
                    context={"conflict_count": int(conflict.sum())},
                )
            for row_index, trade in trade_assignments.items():
                trade_quantity = trade.get("quantity")
                if (
                    status[row_index] == "filled_t1"
                    and trade_quantity is not None
                    and pd.notna(trade_quantity)
                    and np.isfinite(deal[row_index])
                    and deal[row_index] > 0.0
                    and not math.isclose(
                        float(trade_quantity),
                        float(deal[row_index]),
                        rel_tol=0.0,
                        abs_tol=1e-9,
                    )
                ):
                    raise QELongTrendError(
                        QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                        "entry indicator deal_amount conflicts with reconciled trade quantity",
                        context={"row_index": int(row_index)},
                    )
            indicator_full = full_fill & ~trade_delayed
            indicator_partial = partial & ~trade_delayed
            status[indicator_full] = "filled_t1"
            status[indicator_partial] = "partial_fill_t1"
            indicator_filled = indicator_full | indicator_partial
            actual_date[indicator_filled] = pd.to_datetime(result.loc[indicator_filled, "entry_date"]).to_numpy(
                dtype="datetime64[ns]"
            )
            delay_days[indicator_filled] = 0.0
            zero_fill = attempted & (np.nan_to_num(deal, nan=0.0) == 0.0) & (status == "not_verifiable")
            status[zero_fill] = "never_filled"
            not_attempted = explicit_zero & (status == "not_verifiable")
            status[not_attempted] = "not_attempted_by_strategy"
            evidence_level[attempted | explicit_zero] = np.where(
                np.isin(status[attempted | explicit_zero], ["filled_t1", "partial_fill_t1"])
                & np.isin(np.flatnonzero(attempted | explicit_zero), list(trade_assignments)),
                "indicator_and_trade_reconciled",
                "qlib_indicator_object",
            )
            if "reason_code" in indicator_lookup.columns:
                reasons = indicator_lookup["reason_code"].to_numpy(dtype=object)
                for index in np.flatnonzero(pd.notna(reasons)):
                    block_reason[int(index)] = _validated_execution_reason(
                        reasons[int(index)],
                        side="entry",
                    )

        if order_lookup is not None:
            if "attempted" in order_lookup.columns:
                attempted = order_lookup["attempted"].map(_coerce_optional_bool).to_numpy(dtype=object)
                explicit_not_attempted = np.asarray([value is False for value in attempted])
                contradiction = explicit_not_attempted & np.isin(
                    status,
                    ["filled_t1", "partial_fill_t1", "delayed_fill", "never_filled"],
                )
                if bool(contradiction.any()):
                    raise QELongTrendError(
                        QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                        "explicit not-attempted order evidence conflicts with fill evidence",
                        context={"conflict_count": int(contradiction.sum())},
                    )
                not_attempted = explicit_not_attempted & (status == "not_verifiable")
                status[not_attempted] = "not_attempted_by_strategy"
                evidence_level[not_attempted] = "explicit_order_intent"
                attempted_true = np.asarray([value is True for value in attempted])
                order_only_unfilled = attempted_true & (status == "not_verifiable")
                status[order_only_unfilled] = "never_filled"
                evidence_level[order_only_unfilled] = "explicit_order_intent"
            if "reason_code" in order_lookup.columns:
                reasons = order_lookup["reason_code"].to_numpy(dtype=object)
                for index in np.flatnonzero(pd.notna(reasons)):
                    block_reason[int(index)] = _validated_execution_reason(
                        reasons[int(index)],
                        side="entry",
                    )

        blocking_reasons = pd.Series(block_reason).notna().to_numpy(dtype="bool")
        invalid_reason = blocking_reasons & (status == "filled_t1")
        if bool(invalid_reason.any()):
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "blocking reason conflicts with a fully filled T+1 entry",
                context={"conflict_count": int(invalid_reason.sum())},
            )

    result["entry_execution_status"] = status
    result["entry_execution_evidence_level"] = evidence_level
    result["actual_entry_date"] = pd.to_datetime(actual_date)
    result["actual_entry_price"] = actual_price
    result["entry_delay_days"] = pd.array(delay_days, dtype="Float64")
    result["entry_block_reason"] = block_reason
    max_horizon = max(
        (int(column.removeprefix("path_mfe_")) for column in result.columns if column.startswith("path_mfe_")),
        default=None,
    )
    missed_mfe = np.full(row_count, np.nan, dtype="float64")
    missed_winner = pd.array([pd.NA] * row_count, dtype="boolean")
    direct_blocked = pd.Series(block_reason).notna().to_numpy() & np.isin(
        status,
        ["never_filled", "delayed_fill"],
    )
    if max_horizon is not None:
        path_values = pd.to_numeric(result[f"path_mfe_{max_horizon}"], errors="coerce").to_numpy(dtype="float64")
        never = direct_blocked & (status == "never_filled") & np.isfinite(path_values)
        missed_mfe[never] = path_values[never]
        hit_column = "time_to_close_hit_30"
        if hit_column in result:
            hit = pd.to_numeric(result[hit_column], errors="coerce").to_numpy(dtype="float64")
            missed_winner[never] = np.isfinite(hit[never])
    result["missed_mfe_due_to_entry_block"] = missed_mfe
    result["missed_barrier_winner_due_to_entry_block"] = missed_winner
    return result


def _aggregate_indicator(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"amount", "deal_amount", "ffr"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            f"indicator evidence is missing columns {missing}",
        )
    records: list[dict[str, Any]] = []
    for (evidence_date, instrument), group in frame.groupby(
        ["evidence_date", "instrument"],
        sort=True,
    ):
        amount = pd.to_numeric(group["amount"], errors="coerce")
        deal = pd.to_numeric(group["deal_amount"], errors="coerce")
        ffr = pd.to_numeric(group["ffr"], errors="coerce")
        if (amount.dropna() < 0.0).any() or (deal.dropna() < 0.0).any():
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "entry indicator amount/deal_amount must be non-negative",
            )
        total_amount = float(amount.sum(min_count=1)) if amount.notna().any() else np.nan
        total_deal = float(deal.sum(min_count=1)) if deal.notna().any() else np.nan
        if np.isfinite(total_amount) and np.isfinite(total_deal) and total_deal > total_amount + 1e-9:
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "entry indicator deal_amount exceeds target amount",
            )
        derived_ffr = total_deal / total_amount if total_amount > 0.0 and np.isfinite(total_deal) else np.nan
        finite_ffr = ffr.dropna()
        if (finite_ffr < 0.0).any() or (finite_ffr > 1.0).any():
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "entry indicator ffr must be within [0, 1]",
            )
        if np.isfinite(derived_ffr) and not finite_ffr.empty:
            weighted_ffr = (
                float((finite_ffr * amount.loc[finite_ffr.index]).sum() / amount.loc[finite_ffr.index].sum())
                if float(amount.loc[finite_ffr.index].sum()) > 0.0
                else float(finite_ffr.mean())
            )
            if not math.isclose(weighted_ffr, derived_ffr, rel_tol=0.0, abs_tol=1e-6):
                raise QELongTrendError(
                    QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                    "entry indicator ffr conflicts with amount/deal_amount",
                )
        reasons = group["reason_code"].dropna().astype(str).unique().tolist() if "reason_code" in group else []
        if len(reasons) > 1:
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "entry indicator contains conflicting reason codes",
            )
        records.append(
            {
                "evidence_date": evidence_date,
                "instrument": instrument,
                "amount": total_amount,
                "deal_amount": total_deal,
                "ffr": derived_ffr
                if np.isfinite(derived_ffr)
                else (float(finite_ffr.mean()) if not finite_ffr.empty else np.nan),
                "reason_code": reasons[0] if reasons else None,
            }
        )
    return pd.DataFrame.from_records(records).set_index(["evidence_date", "instrument"]).sort_index()


def _aggregate_orders(frame: pd.DataFrame) -> pd.DataFrame:
    if "attempted" not in frame.columns and "reason_code" not in frame.columns:
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            "order evidence requires attempted or reason_code",
        )
    records: list[dict[str, Any]] = []
    for (evidence_date, instrument), group in frame.groupby(
        ["evidence_date", "instrument"],
        sort=True,
    ):
        attempted_values = (
            {_coerce_optional_bool(value) for value in group["attempted"]} - {None} if "attempted" in group else set()
        )
        if len(attempted_values) > 1:
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "order evidence contains conflicting attempted flags",
            )
        reasons = group["reason_code"].dropna().astype(str).unique().tolist() if "reason_code" in group else []
        if len(reasons) > 1:
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "order evidence contains conflicting reason codes",
            )
        records.append(
            {
                "evidence_date": evidence_date,
                "instrument": instrument,
                "attempted": next(iter(attempted_values), None),
                "reason_code": reasons[0] if reasons else None,
            }
        )
    return pd.DataFrame.from_records(records).set_index(["evidence_date", "instrument"]).sort_index()


def _coerce_optional_bool(value: object) -> bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    raise QELongTrendError(
        QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
        f"invalid attempted flag {value!r}",
    )


def _validated_execution_reason(value: object, *, side: str) -> str:
    reason = str(value).strip().lower()
    allowed = _ENTRY_BLOCK_REASONS if side == "entry" else _EXIT_BLOCK_REASONS
    if reason not in allowed:
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            f"unregistered {side} execution reason {value!r}",
            context={"allowed_reason_codes": sorted(allowed)},
        )
    return reason


def _normalize_positions(positions: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(positions, pd.DataFrame):
        raise QELongTrendError(
            QELongTrendReason.EPISODE_RECONCILIATION_FAILED,
            "positions must be a DataFrame",
        )
    frame = positions.copy(deep=False)
    if isinstance(frame.index, pd.MultiIndex):
        frame = frame.reset_index()
    date_column = next((name for name in ("datetime", "date", "trade_date") if name in frame.columns), None)
    if date_column is None or "instrument" not in frame.columns or "amount" not in frame.columns:
        raise QELongTrendError(
            QELongTrendReason.EPISODE_RECONCILIATION_FAILED,
            "positions require date, instrument, and amount columns",
        )
    frame = frame.rename(columns={date_column: "position_date"})
    frame["position_date"] = pd.to_datetime(frame["position_date"], errors="coerce").dt.normalize()
    try:
        frame["instrument"] = frame["instrument"].map(canonicalize_instrument)
    except ValueError as exc:
        raise QELongTrendError(QELongTrendReason.EPISODE_RECONCILIATION_FAILED, str(exc)) from exc
    frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
    if frame["position_date"].isna().any() or frame["amount"].isna().any() or (frame["amount"] < 0).any():
        raise QELongTrendError(
            QELongTrendReason.EPISODE_RECONCILIATION_FAILED,
            "positions contain invalid dates or non-negative amount violations",
        )
    if frame.duplicated(["position_date", "instrument"]).any():
        raise QELongTrendError(
            QELongTrendReason.EPISODE_RECONCILIATION_FAILED,
            "position identity must be unique by date/instrument",
        )
    return frame.sort_values(["position_date", "instrument"], kind="mergesort").reset_index(drop=True)


def reconstruct_holding_episodes(
    *,
    positions: pd.DataFrame,
    prices: pd.DataFrame,
    evaluation_asof: pd.Timestamp,
    profile: QELongTrendProfile = QE_LONG_TREND_PROFILE_V1,
) -> pd.DataFrame:
    profile = require_registered_profile(profile)
    position_frame = _normalize_positions(positions)
    price_frame = _normalize_price_frame(prices)
    calendar = pd.DatetimeIndex(sorted(price_frame.index.get_level_values("datetime").unique()))
    evaluation_asof = pd.Timestamp(evaluation_asof).normalize()
    if evaluation_asof not in calendar:
        raise QELongTrendError(
            QELongTrendReason.EPISODE_RECONCILIATION_FAILED,
            "evaluation_asof must be an exact QE trading-calendar date",
        )
    if position_frame.empty:
        raise QELongTrendError(
            QELongTrendReason.EPISODE_RECONCILIATION_FAILED,
            "position artifact contains no dated snapshots",
        )
    calendar_positions = {value: index for index, value in enumerate(calendar)}
    snapshot_dates = pd.DatetimeIndex(sorted(position_frame["position_date"].unique()))
    invalid_snapshot_dates = snapshot_dates.difference(calendar[calendar <= evaluation_asof])
    if len(invalid_snapshot_dates):
        raise QELongTrendError(
            QELongTrendReason.EPISODE_RECONCILIATION_FAILED,
            "position artifact contains dates outside the QE outcome trading calendar",
            context={
                "invalid_date_count": int(len(invalid_snapshot_dates)),
                "examples": [str(value.date()) for value in invalid_snapshot_dates[:5]],
            },
        )
    position_asof = pd.Timestamp(snapshot_dates.max())
    expected_dates = calendar[(calendar >= snapshot_dates.min()) & (calendar <= position_asof)]
    missing_snapshot_dates = expected_dates.difference(snapshot_dates)
    if len(missing_snapshot_dates):
        raise QELongTrendError(
            QELongTrendReason.EPISODE_RECONCILIATION_FAILED,
            "normalized position artifact omits complete daily snapshots",
            context={
                "missing_date_count": int(len(missing_snapshot_dates)),
                "examples": [str(value.date()) for value in missing_snapshot_dates[:5]],
            },
        )
    pivot = (
        position_frame.pivot(index="position_date", columns="instrument", values="amount")
        .reindex(snapshot_dates)
        .fillna(0.0)
    )
    records: list[dict[str, Any]] = []
    price_instruments = set(price_frame.index.get_level_values("instrument"))

    for instrument in pivot.columns:
        instrument_prices = (
            price_frame.xs(instrument, level="instrument", drop_level=True)
            if instrument in price_instruments
            else pd.DataFrame()
        )
        amounts = pivot[instrument].to_numpy(dtype="float64")
        active = amounts > 0.0
        previous = np.concatenate([[active[0]], active[:-1]])
        observed_entries = np.flatnonzero(active & ~previous)
        episode_starts = ([(0, True)] if bool(active[0]) else []) + [(int(index), False) for index in observed_entries]
        exit_indices = np.flatnonzero(~active & previous)
        exit_cursor = 0
        episode_seq = 0
        for entry_index, left_censored in episode_starts:
            while exit_cursor < len(exit_indices) and exit_indices[exit_cursor] <= entry_index:
                exit_cursor += 1
            exit_index = int(exit_indices[exit_cursor]) if exit_cursor < len(exit_indices) else None
            if exit_index is not None:
                exit_cursor += 1
            episode_seq += 1
            entry_date = pd.Timestamp(snapshot_dates[int(entry_index)])
            exit_date = pd.Timestamp(snapshot_dates[exit_index]) if exit_index is not None else pd.NaT
            records.append(
                _episode_record(
                    instrument=str(instrument),
                    episode_seq=episode_seq,
                    entry_date=entry_date,
                    exit_date=exit_date,
                    left_censored=left_censored,
                    position_asof=position_asof,
                    evaluation_asof=evaluation_asof,
                    instrument_prices=instrument_prices,
                    calendar=calendar,
                    calendar_positions=calendar_positions,
                    max_horizon=max(profile.horizons),
                )
            )
    return pd.DataFrame.from_records(records, columns=_EPISODE_COLUMNS)


def _episode_record(
    *,
    instrument: str,
    episode_seq: int,
    entry_date: pd.Timestamp,
    exit_date: pd.Timestamp | pd.NaT,
    left_censored: bool,
    position_asof: pd.Timestamp,
    evaluation_asof: pd.Timestamp,
    instrument_prices: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    calendar_positions: Mapping[pd.Timestamp, int],
    max_horizon: int,
) -> dict[str, Any]:
    flags: list[str] = []
    entry_position = calendar_positions.get(entry_date)
    if entry_position is None:
        flags.append("entry_date_not_in_price_calendar")
    closed = pd.notna(exit_date)
    terminal_date = pd.Timestamp(exit_date) if closed else position_asof
    terminal_position = calendar_positions.get(terminal_date)
    if terminal_position is None:
        terminal_position = int(calendar.searchsorted(terminal_date, side="right") - 1)
        flags.append("terminal_date_not_exact_price_calendar")

    entry_close = None if left_censored else _price_at(instrument_prices, entry_date, "close_qfq")
    exit_close = _price_at(instrument_prices, pd.Timestamp(exit_date), "close_qfq") if closed else None
    episode_close_return = _return_ratio(exit_close, entry_close) if closed else None

    episode_mfe = None
    episode_mae = None
    extended_mfe = None
    post_exit_mfe = None
    highest_at_exit: str | None = None
    highest_180: str | None = None
    false_early_exit: bool | None = None
    episode_capture_ratio = None
    extended_capture_ratio = None
    open_censored = not closed
    episode_path_coverage = None
    extended_path_coverage = None
    extended_censored = True

    if left_censored:
        flags.append("left_censored_position_history")
    if entry_position is not None and entry_close is not None and terminal_position >= entry_position:
        episode_end = min(terminal_position, len(calendar) - 1)
        episode_dates = calendar[entry_position + 1 : episode_end + 1]
        episode_path_coverage = _ohlc_path_coverage(instrument_prices, episode_dates)
        if episode_path_coverage == 1.0:
            episode_mfe, episode_mae, episode_close_path_max = _path_extremes(
                instrument_prices,
                episode_dates,
                entry_close,
            )
        else:
            episode_close_path_max = None
            flags.append("episode_path_incomplete")
        evaluation_position = calendar_positions[evaluation_asof]
        extended_end = min(entry_position + max_horizon, evaluation_position)
        extended_dates = calendar[entry_position + 1 : extended_end + 1]
        extended_path_coverage = _ohlc_path_coverage(instrument_prices, extended_dates)
        extended_censored = entry_position + max_horizon > evaluation_position
        if extended_path_coverage == 1.0:
            extended_mfe, _, extended_close_path_max = _path_extremes(
                instrument_prices,
                extended_dates,
                entry_close,
            )
        else:
            extended_close_path_max = None
            flags.append("extended_path_incomplete")
        if episode_mfe is not None and episode_close_return is not None:
            if episode_mfe > 1e-8:
                episode_capture_ratio = episode_close_return / episode_mfe
            else:
                flags.append("episode_capture_ratio_denominator_not_positive")
        if extended_mfe is not None and episode_close_return is not None:
            if extended_mfe > 1e-8:
                extended_capture_ratio = episode_close_return / extended_mfe
            else:
                flags.append("extended_capture_ratio_denominator_not_positive")
        highest_at_exit = _stage_from_return(episode_close_path_max)
        full_180_mature = entry_position + max_horizon < len(calendar) and extended_path_coverage == 1.0
        highest_180 = _stage_from_return(extended_close_path_max) if full_180_mature else None
        exit_within_horizon = closed and terminal_position <= entry_position + max_horizon
        if (
            exit_within_horizon
            and full_180_mature
            and highest_at_exit is not None
            and highest_180 not in (None, "NONE")
        ):
            false_early_exit = _stage_order(highest_at_exit) < _stage_order(highest_180)
        if closed and terminal_position < extended_end and exit_close is not None:
            post_dates = calendar[terminal_position + 1 : extended_end + 1]
            _, _, post_exit_mfe = _path_extremes(
                instrument_prices,
                post_dates,
                exit_close,
                use_high_for_primary=True,
            )

    if entry_close is None and not left_censored:
        flags.append("entry_close_missing")
    if closed and exit_close is None:
        flags.append("exit_close_missing")

    if open_censored:
        episode_maturity_state = "open_event_censored"
    elif exit_close is None:
        episode_maturity_state = "instrument_exit_unresolved"
    elif left_censored:
        episode_maturity_state = "left_censored"
    elif "episode_path_incomplete" in flags:
        episode_maturity_state = "path_incomplete"
    else:
        episode_maturity_state = "matured"

    return {
        "instrument": instrument,
        "episode_seq": episode_seq,
        "entry_date": entry_date,
        "exit_date": exit_date,
        "left_censored": left_censored,
        "open_censored": open_censored,
        "position_observation_end_date": position_asof,
        "episode_maturity_state": episode_maturity_state,
        "extended_censored": extended_censored,
        "entry_close_qfq": entry_close,
        "exit_close_qfq": exit_close,
        "entry_execution_status": "not_verifiable",
        "entry_execution_evidence_level": "position_transition_only",
        "actual_entry_date": None,
        "actual_entry_price": None,
        "entry_delay_days": None,
        "entry_block_reason": None,
        "exit_signal_date": None,
        "actual_exit_date": exit_date if closed else None,
        "actual_exit_price": None,
        "exit_execution_status": "not_verifiable",
        "exit_execution_evidence_level": "position_transition_only" if closed else "none",
        "exit_delay_days": None,
        "exit_block_reason": None,
        "post_exit_signal_mae": None,
        "blocked_exit_extra_drawdown": None,
        "blocked_exit_extra_holding_days": None,
        "episode_close_return_qfq": episode_close_return,
        "execution_gross_return": None,
        "execution_net_return": None,
        "episode_mfe": episode_mfe,
        "episode_mae": episode_mae,
        "episode_path_coverage": episode_path_coverage,
        "episode_capture_ratio": episode_capture_ratio,
        "extended_mfe_180": extended_mfe,
        "extended_path_coverage": extended_path_coverage,
        "extended_capture_ratio": extended_capture_ratio,
        "post_exit_mfe": post_exit_mfe,
        "highest_stage_at_exit": highest_at_exit,
        "highest_stage_180": highest_180,
        "false_early_exit": false_early_exit,
        "cost_quality": "not_verifiable",
        "episode_quality_flags": "|".join(flags),
    }


def attach_episode_entry_evidence(
    episodes: pd.DataFrame,
    observations: pd.DataFrame,
) -> pd.DataFrame:
    result = episodes.copy()
    if result.empty or observations.empty or "entry_execution_status" not in observations:
        return result
    observation_frame = observations.reset_index(drop=True).copy(deep=False)
    observation_frame = observation_frame.assign(
        _actual_entry_date=pd.to_datetime(
            observation_frame["actual_entry_date"],
            errors="coerce",
        ).dt.normalize(),
        _expected_entry_date=pd.to_datetime(
            observation_frame["entry_date"],
            errors="coerce",
        ).dt.normalize(),
    )
    actual_groups = (
        observation_frame.loc[observation_frame["_actual_entry_date"].notna()]
        .groupby(["instrument", "_actual_entry_date"], sort=False)
        .groups
    )
    expected_groups = (
        observation_frame.loc[observation_frame["_expected_entry_date"].notna()]
        .groupby(["instrument", "_expected_entry_date"], sort=False)
        .groups
    )
    episode_assignments: dict[int, int] = {}
    consumed_observations: set[int] = set()

    for episode_index, episode in result.iterrows():
        if bool(episode.get("left_censored", False)):
            continue
        entry_date = pd.Timestamp(episode["entry_date"]).normalize()
        key = (str(episode["instrument"]), entry_date)
        actual_indices = [int(index) for index in actual_groups.get(key, ())]
        if len(actual_indices) > 1:
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "multiple entry signals claim the same holding episode entry",
                context={
                    "instrument": str(episode["instrument"]),
                    "entry_date": str(entry_date.date()),
                },
            )
        if actual_indices:
            observation_index = actual_indices[0]
            if observation_index in consumed_observations:
                raise QELongTrendError(
                    QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                    "one entry signal cannot claim multiple holding episodes",
                )
            episode_assignments[int(episode_index)] = observation_index
            consumed_observations.add(observation_index)

    for episode_index, episode in result.iterrows():
        if bool(episode.get("left_censored", False)) or int(episode_index) in episode_assignments:
            continue
        entry_date = pd.Timestamp(episode["entry_date"]).normalize()
        key = (str(episode["instrument"]), entry_date)
        candidate_indices = [
            int(index) for index in expected_groups.get(key, ()) if int(index) not in consumed_observations
        ]
        if not candidate_indices:
            continue
        candidates = observation_frame.loc[sorted(candidate_indices)].copy()
        candidates.sort_values(
            ["signal_date", "stable_rank"],
            ascending=[False, True],
            kind="mergesort",
            inplace=True,
        )
        observation_index = int(candidates.index[0])
        episode_assignments[int(episode_index)] = observation_index
        consumed_observations.add(observation_index)

    for episode_index, observation_index in episode_assignments.items():
        best = observation_frame.loc[observation_index]
        result.at[episode_index, "entry_execution_status"] = best["entry_execution_status"]
        result.at[episode_index, "entry_execution_evidence_level"] = best.get("entry_execution_evidence_level")
        result.at[episode_index, "actual_entry_date"] = best.get("actual_entry_date")
        result.at[episode_index, "actual_entry_price"] = best.get("actual_entry_price")
        result.at[episode_index, "entry_delay_days"] = best.get("entry_delay_days")
        result.at[episode_index, "entry_block_reason"] = best.get("entry_block_reason")
    return result


def attach_exit_execution_evidence(
    episodes: pd.DataFrame,
    *,
    evidence: ExecutionEvidenceBundle | None,
    prices: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    evaluation_asof: pd.Timestamp,
) -> pd.DataFrame:
    """Attach exit intent/fill evidence with the same authority rules as entry."""

    result = episodes.reset_index(drop=True).copy()
    if result.empty:
        return result
    result["exit_signal_date"] = pd.NaT
    result["actual_exit_date"] = pd.to_datetime(result["exit_date"], errors="coerce")
    result["actual_exit_price"] = np.nan
    result["exit_execution_status"] = "not_verifiable"
    result["exit_execution_evidence_level"] = "none"
    result["exit_delay_days"] = pd.array([pd.NA] * len(result), dtype="Float64")
    result["exit_block_reason"] = None
    result["post_exit_signal_mae"] = np.nan
    result["blocked_exit_extra_drawdown"] = np.nan
    result["blocked_exit_extra_holding_days"] = pd.array(
        [pd.NA] * len(result),
        dtype="Float64",
    )
    if evidence is None or evidence.exit_signals is None:
        return result

    signals = _normalize_evidence_frame(evidence.exit_signals, kind="exit_signal")
    if signals is None or signals.empty:
        return result
    if signals.duplicated(["evidence_date", "instrument"]).any():
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
            "exit signal identity must be unique by date/instrument",
        )
    signal_dates_by_instrument = {
        str(instrument): pd.DatetimeIndex(group["evidence_date"].sort_values(kind="mergesort").to_numpy())
        for instrument, group in signals.groupby("instrument", sort=False)
    }
    for episode_index, episode in result.iterrows():
        terminal = (
            pd.Timestamp(episode["exit_date"]) if pd.notna(episode["exit_date"]) else pd.Timestamp(evaluation_asof)
        )
        instrument_dates = signal_dates_by_instrument.get(str(episode["instrument"]))
        if instrument_dates is None:
            continue
        position = int(instrument_dates.searchsorted(pd.Timestamp(episode["entry_date"]), side="left"))
        if position < len(instrument_dates) and instrument_dates[position] <= terminal:
            result.at[episode_index, "exit_signal_date"] = instrument_dates[position]

    target_mask = result["exit_signal_date"].notna()
    if not bool(target_mask.any()):
        return result
    target = result.loc[target_mask].copy()
    sell_trades = _trade_side_frame(evidence.trades, side="sell")
    trade_assignments, ambiguous_rows = _match_trade_rows(
        target,
        sell_trades,
        expected_date_column="exit_signal_date",
        signal_date_column="exit_signal_date",
    )
    for row_index in ambiguous_rows:
        result.at[row_index, "exit_execution_evidence_level"] = "ambiguous_trade_match"

    sell_indicator = _filter_order_like_side(
        _normalize_evidence_frame(evidence.indicator, kind="indicator"),
        side="sell",
        allow_unspecified_for_entry=False,
    )
    sell_orders = _filter_order_like_side(
        _normalize_evidence_frame(evidence.orders, kind="order"),
        side="sell",
        allow_unspecified_for_entry=False,
    )
    keys = pd.MultiIndex.from_arrays(
        [pd.to_datetime(target["exit_signal_date"]), target["instrument"]],
        names=["evidence_date", "instrument"],
    )
    indicator_lookup = (
        _aggregate_indicator(sell_indicator).reindex(keys)
        if sell_indicator is not None and not sell_indicator.empty
        else None
    )
    order_lookup = (
        _aggregate_orders(sell_orders).reindex(keys) if sell_orders is not None and not sell_orders.empty else None
    )
    calendar_positions = {value: index for index, value in enumerate(calendar)}
    price_instruments = set(prices.index.get_level_values("instrument"))
    price_cache: dict[str, pd.DataFrame] = {}

    for local_position, (episode_index, episode) in enumerate(target.iterrows()):
        signal_date = pd.Timestamp(episode["exit_signal_date"])
        position_exit = pd.Timestamp(episode["exit_date"]) if pd.notna(episode["exit_date"]) else None
        trade = trade_assignments.get(episode_index)
        trade_exit = pd.Timestamp(trade["evidence_date"]) if trade is not None else None
        if position_exit is not None and trade_exit is not None and position_exit != trade_exit:
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "sell trade date conflicts with the position exit transition",
                context={
                    "instrument": str(episode["instrument"]),
                    "position_exit_date": str(position_exit.date()),
                    "trade_exit_date": str(trade_exit.date()),
                },
            )
        actual_exit = trade_exit or position_exit
        signal_position = calendar_positions.get(signal_date)
        actual_position = calendar_positions.get(actual_exit) if actual_exit is not None else None
        if signal_position is None or (actual_exit is not None and actual_position is None):
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "exit signal or reconciled exit date is outside the QE evaluation calendar",
            )
        if actual_position is not None and actual_position < signal_position:
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "reconciled exit precedes the exit signal date",
            )
        status = "not_verifiable"
        evidence_level = "exit_signal_only"
        if actual_exit is not None:
            status = "filled_on_exit_signal_day" if actual_exit == signal_date else "delayed_exit"
            evidence_level = "reconciled_trade" if trade is not None else "position_transition"
            result.at[episode_index, "actual_exit_date"] = actual_exit
            if trade is not None and pd.notna(trade.get("price")):
                result.at[episode_index, "actual_exit_price"] = float(trade["price"])

        indicator_row = indicator_lookup.iloc[local_position] if indicator_lookup is not None else None
        indicator_attempted = False
        indicator_filled = False
        if indicator_row is not None and pd.notna(indicator_row.get("amount")):
            amount = float(indicator_row["amount"])
            deal = float(indicator_row["deal_amount"]) if pd.notna(indicator_row.get("deal_amount")) else 0.0
            indicator_attempted = amount > 0.0
            indicator_filled = deal > 0.0
            if actual_exit == signal_date and indicator_attempted and deal == 0.0:
                raise QELongTrendError(
                    QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                    "zero-fill exit indicator conflicts with same-day exit evidence",
                )
            if actual_exit is not None and actual_exit != signal_date and indicator_filled:
                raise QELongTrendError(
                    QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                    "same-day filled exit indicator conflicts with delayed position/trade exit",
                )
            if actual_exit is None and indicator_attempted:
                status = "never_exited"
                evidence_level = "qlib_indicator_object"
            if actual_exit is not None and indicator_filled:
                evidence_level = "indicator_and_exit_reconciled"
            if (
                trade is not None
                and actual_exit == signal_date
                and indicator_filled
                and trade.get("quantity") is not None
                and pd.notna(trade.get("quantity"))
                and not math.isclose(
                    float(trade["quantity"]),
                    deal,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
            ):
                raise QELongTrendError(
                    QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                    "exit indicator deal_amount conflicts with reconciled trade quantity",
                )

        order_row = order_lookup.iloc[local_position] if order_lookup is not None else None
        attempted_flag = _coerce_optional_bool(order_row.get("attempted")) if order_row is not None else None
        if attempted_flag is False and (actual_exit is not None or indicator_attempted):
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "explicit not-attempted exit order conflicts with fill evidence",
            )
        if attempted_flag is False:
            status = "not_attempted_by_strategy"
            evidence_level = "explicit_order_intent"
        elif attempted_flag is True and actual_exit is None:
            status = "never_exited"
            evidence_level = "explicit_order_intent"

        reason_candidates = []
        for row in (indicator_row, order_row):
            if row is not None and pd.notna(row.get("reason_code")):
                reason_candidates.append(str(row["reason_code"]))
        if len(set(reason_candidates)) > 1:
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "exit evidence contains conflicting reason codes",
            )
        block_reason = _validated_execution_reason(reason_candidates[0], side="exit") if reason_candidates else None
        if block_reason is not None and status == "filled_on_exit_signal_day":
            raise QELongTrendError(
                QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED,
                "blocking reason conflicts with same-day filled exit",
            )

        result.at[episode_index, "exit_execution_status"] = status
        result.at[episode_index, "exit_execution_evidence_level"] = evidence_level
        result.at[episode_index, "exit_block_reason"] = block_reason
        terminal = actual_exit or pd.Timestamp(evaluation_asof)
        terminal_position = calendar_positions.get(terminal)
        if signal_position is not None and terminal_position is not None:
            delay = max(terminal_position - signal_position, 0)
            result.at[episode_index, "exit_delay_days"] = float(delay)
            instrument = str(episode["instrument"])
            if instrument not in price_cache:
                price_cache[instrument] = (
                    prices.xs(instrument, level="instrument", drop_level=True)
                    if instrument in price_instruments
                    else pd.DataFrame()
                )
            baseline_frame = price_cache[instrument]
            baseline = _price_at(baseline_frame, signal_date, "close_qfq")
            path_dates = calendar[signal_position + 1 : terminal_position + 1]
            if baseline is not None and len(path_dates):
                selected = baseline_frame.reindex(path_dates)
                lows = pd.to_numeric(selected["low_qfq"], errors="coerce")
                if lows.notna().all():
                    mae = float(min(0.0, lows.min() / baseline - 1.0))
                    result.at[episode_index, "post_exit_signal_mae"] = mae
                    if block_reason is not None:
                        result.at[episode_index, "blocked_exit_extra_drawdown"] = mae
                        result.at[episode_index, "blocked_exit_extra_holding_days"] = float(delay)
    return result


def _price_at(frame: pd.DataFrame, date: pd.Timestamp, column: str) -> float | None:
    if frame.empty or date not in frame.index:
        return None
    value = frame.at[date, column]
    if value is None or not math.isfinite(float(value)) or float(value) <= 0.0:
        return None
    return float(value)


def _return_ratio(terminal: float | None, entry: float | None) -> float | None:
    if terminal is None or entry is None or entry <= 0.0:
        return None
    return float(terminal / entry - 1.0)


def _ohlc_path_coverage(frame: pd.DataFrame, dates: pd.DatetimeIndex) -> float | None:
    if len(dates) == 0:
        return 1.0
    if frame.empty:
        return 0.0
    selected = frame.reindex(dates)
    valid = selected[["close_qfq", "high_qfq", "low_qfq"]].notna().all(axis=1)
    return float(valid.mean())


def _path_extremes(
    frame: pd.DataFrame,
    dates: pd.DatetimeIndex,
    entry_price: float,
    *,
    use_high_for_primary: bool = False,
) -> tuple[float | None, float | None, float | None]:
    if frame.empty or len(dates) == 0:
        return None, None, None
    selected = frame.reindex(dates)
    highs = pd.to_numeric(selected["high_qfq"], errors="coerce").to_numpy(dtype="float64")
    lows = pd.to_numeric(selected["low_qfq"], errors="coerce").to_numpy(dtype="float64")
    closes = pd.to_numeric(selected["close_qfq"], errors="coerce").to_numpy(dtype="float64")
    high_returns = highs[np.isfinite(highs)] / entry_price - 1.0
    low_returns = lows[np.isfinite(lows)] / entry_price - 1.0
    close_returns = closes[np.isfinite(closes)] / entry_price - 1.0
    mfe = float(max(0.0, high_returns.max())) if high_returns.size else None
    mae = float(min(0.0, low_returns.min())) if low_returns.size else None
    if use_high_for_primary:
        primary = float(max(0.0, high_returns.max())) if high_returns.size else None
    else:
        primary = float(max(0.0, close_returns.max())) if close_returns.size else None
    return mfe, mae, primary


def _stage_from_return(value: float | None) -> str | None:
    if value is None or not math.isfinite(value):
        return None
    if value >= 0.70:
        return "HIT70"
    if value >= 0.50:
        return "HIT50"
    if value >= 0.30:
        return "HIT30"
    return "NONE"


def _stage_order(value: str | None) -> int:
    return {None: -1, "NONE": 0, "HIT30": 1, "HIT50": 2, "HIT70": 3}.get(value, -1)


def compute_episode_metrics(episodes: pd.DataFrame) -> list[dict[str, Any]]:
    if episodes.empty:
        return []
    false_exit = episodes["false_early_exit"].dropna()
    limited = episodes["episode_quality_flags"].astype(str).str.len().gt(0)
    return [
        {
            "metric_scope": "position_episode",
            "metric_key": "episode_capture_summary",
            "slice": "all_oos",
            "horizon": 180,
            "barrier": None,
            "k": None,
            "value_num": None,
            "value_json": {
                "episode_count": int(len(episodes)),
                "left_censored_count": int(episodes["left_censored"].sum()),
                "open_censored_count": int(episodes["open_censored"].sum()),
                "extended_censored_count": int(episodes["extended_censored"].sum()),
                "episode_path_coverage": _distribution(episodes["episode_path_coverage"]),
                "extended_path_coverage": _distribution(episodes["extended_path_coverage"]),
                "episode_capture_ratio": _distribution(episodes["episode_capture_ratio"]),
                "extended_capture_ratio": _distribution(episodes["extended_capture_ratio"]),
                "post_exit_mfe": _distribution(episodes["post_exit_mfe"]),
                "false_early_exit_count": int(false_exit.astype(bool).sum()),
                "false_early_exit_denominator": int(len(false_exit)),
                "false_early_exit_ratio": (float(false_exit.astype(bool).mean()) if len(false_exit) else None),
            },
            "quality_flag": "computed_with_limitations" if bool(limited.any()) else "ok",
        }
    ]


def normalize_portfolio_report(report: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(report, pd.DataFrame):
        raise QELongTrendError(
            QELongTrendReason.PORTFOLIO_REPORT_INVALID,
            "portfolio report must be a DataFrame",
        )
    frame = report.copy(deep=True)
    if isinstance(frame.index, pd.DatetimeIndex):
        frame = frame.reset_index().rename(columns={frame.index.name or "index": "report_date"})
    else:
        date_column = next(
            (name for name in ("datetime", "date", "trade_date", "report_date") if name in frame),
            None,
        )
        if date_column is None:
            raise QELongTrendError(
                QELongTrendReason.PORTFOLIO_REPORT_INVALID,
                "portfolio report requires a date index or column",
            )
        frame = frame.rename(columns={date_column: "report_date"})
    if "return" not in frame:
        raise QELongTrendError(
            QELongTrendReason.PORTFOLIO_REPORT_INVALID,
            "portfolio report is missing authoritative return",
        )
    if frame.empty:
        raise QELongTrendError(
            QELongTrendReason.PORTFOLIO_REPORT_INVALID,
            "portfolio report contains no rows",
        )
    frame["report_date"] = pd.to_datetime(frame["report_date"], errors="coerce").dt.normalize()
    if frame["report_date"].isna().any() or frame["report_date"].duplicated().any():
        raise QELongTrendError(
            QELongTrendReason.PORTFOLIO_REPORT_INVALID,
            "portfolio report dates must be valid and unique",
        )
    for column in ("return", "cost", "turnover", "total_turnover", "bench"):
        if column not in frame:
            continue
        original_non_null = frame[column].notna()
        numeric = pd.to_numeric(frame[column], errors="coerce")
        invalid = original_non_null & ~np.isfinite(numeric.to_numpy(dtype="float64"))
        if bool(invalid.any()):
            raise QELongTrendError(
                QELongTrendReason.PORTFOLIO_REPORT_INVALID,
                f"portfolio report contains invalid {column} values",
            )
        frame[column] = numeric
    if frame["return"].isna().any() or (frame["return"] <= -1.0).any():
        raise QELongTrendError(
            QELongTrendReason.PORTFOLIO_REPORT_INVALID,
            "portfolio report returns must be complete and greater than -1",
        )
    return frame.sort_values("report_date", kind="mergesort").reset_index(drop=True)


def _execution_evidence_trade_count(evidence: ExecutionEvidenceBundle | None) -> int | None:
    if evidence is None:
        return None
    if evidence.trades is not None:
        trades = _normalize_evidence_frame(evidence.trades, kind="trade")
        if trades is None:
            return None
        for column in ("quantity", "deal_amount", "amount"):
            if column in trades:
                return int(pd.to_numeric(trades[column], errors="coerce").abs().gt(0.0).sum())
        return int(len(trades))
    if evidence.indicator is not None:
        indicator = _normalize_evidence_frame(evidence.indicator, kind="indicator")
        if indicator is None:
            return None
        if "deal_amount" in indicator:
            return int(pd.to_numeric(indicator["deal_amount"], errors="coerce").abs().gt(0.0).sum())
    return None


def compute_portfolio_metrics(
    report: pd.DataFrame,
    *,
    executed_trade_count: int | None = None,
) -> list[dict[str, Any]]:
    frame = normalize_portfolio_report(report)
    returns = frame["return"].to_numpy(dtype="float64")
    nav = np.cumprod(1.0 + returns)
    running_peak = np.maximum.accumulate(nav)
    drawdown = nav / running_peak - 1.0
    annualized_volatility = float(np.std(returns, ddof=1) * math.sqrt(252.0)) if len(returns) >= 2 else None
    annualized_return = float(nav[-1] ** (252.0 / len(nav)) - 1.0) if len(nav) else None
    sharpe = (
        float(np.mean(returns) / np.std(returns, ddof=1) * math.sqrt(252.0))
        if len(returns) >= 2 and np.std(returns, ddof=1) > 0.0
        else None
    )
    cost_coverage = float(frame["cost"].notna().mean()) if "cost" in frame else 0.0
    turnover_column = "turnover" if "turnover" in frame else ("total_turnover" if "total_turnover" in frame else None)
    turnover_coverage = float(frame[turnover_column].notna().mean()) if turnover_column is not None else 0.0
    zero_diagnostics_conflict = bool(
        executed_trade_count is not None
        and executed_trade_count > 0
        and cost_coverage == 1.0
        and turnover_coverage == 1.0
        and float(frame["cost"].abs().sum()) == 0.0
        and float(frame[turnover_column].abs().sum()) == 0.0
    )
    complete_diagnostics = (
        cost_coverage == 1.0
        and turnover_coverage == 1.0
        and not zero_diagnostics_conflict
    )
    observed_cost_sum = float(frame["cost"].dropna().sum()) if "cost" in frame else None
    observed_turnover_mean = (
        float(frame[turnover_column].dropna().mean())
        if turnover_column is not None and frame[turnover_column].notna().any()
        else None
    )
    return [
        {
            "metric_scope": "portfolio_result",
            "metric_key": "authoritative_portfolio_summary",
            "slice": "all_oos",
            "horizon": None,
            "barrier": None,
            "k": None,
            "value_num": annualized_return,
            "value_json": {
                "trading_day_count": int(len(frame)),
                "cumulative_return": float(nav[-1] - 1.0),
                "annualized_return": annualized_return,
                "annualized_volatility": annualized_volatility,
                "sharpe": sharpe,
                "max_drawdown": float(drawdown.min()),
                "cost_coverage": cost_coverage,
                "turnover_coverage": turnover_coverage,
                "executed_trade_count": executed_trade_count,
                "zero_diagnostics_conflict": zero_diagnostics_conflict,
                "total_cost": (
                    observed_cost_sum
                    if cost_coverage == 1.0 and not zero_diagnostics_conflict
                    else None
                ),
                "observed_cost_sum": observed_cost_sum,
                "average_turnover": (
                    observed_turnover_mean
                    if turnover_coverage == 1.0 and not zero_diagnostics_conflict
                    else None
                ),
                "observed_average_turnover": observed_turnover_mean,
            },
            "quality_flag": "ok" if complete_diagnostics else "computed_with_limitations",
        }
    ]


def _execution_family_statuses(
    observations: pd.DataFrame,
    episodes: pd.DataFrame,
) -> tuple[FamilyEvidenceStatus, FamilyEvidenceStatus]:
    entry_statuses = (
        observations["entry_execution_status"]
        if not observations.empty and "entry_execution_status" in observations
        else pd.Series(dtype="object")
    )
    exit_statuses = (
        episodes["exit_execution_status"]
        if not episodes.empty and "exit_execution_status" in episodes
        else pd.Series(dtype="object")
    )
    entry_verifiable = entry_statuses.ne("not_verifiable")
    exit_verifiable = exit_statuses.ne("not_verifiable")
    all_verifiable = pd.concat([entry_verifiable, exit_verifiable], ignore_index=True)
    if len(all_verifiable) and bool(all_verifiable.all()):
        fill_status = FamilyComputationStatus.COMPUTED
    elif bool(all_verifiable.any()):
        fill_status = FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
    else:
        fill_status = FamilyComputationStatus.NOT_COMPUTABLE
    fill = FamilyEvidenceStatus(
        status=fill_status,
        available_inputs=("indicator_order_trade_evidence",),
        coverage={
            "verifiable_entry_rate": float(entry_verifiable.mean()) if len(entry_verifiable) else None,
            "verifiable_exit_rate": float(exit_verifiable.mean()) if len(exit_verifiable) else None,
        },
        limitations=("some entry or exit events lack authoritative fill evidence",)
        if not (len(all_verifiable) and bool(all_verifiable.all()))
        else (),
        reason_codes=(QELongTrendReason.EXECUTION_EVIDENCE_INSUFFICIENT.value,)
        if not (len(all_verifiable) and bool(all_verifiable.all()))
        else (),
        data_actions=(
            _family_data_action(
                "archive_missing_entry_exit_execution_evidence",
                "order_fill",
                required_fields=("order_intent", "trade", "position_transition"),
            ),
        )
        if not (len(all_verifiable) and bool(all_verifiable.all()))
        else (),
    )
    blocked_entry = (
        observations["entry_block_reason"].notna()
        if not observations.empty and "entry_block_reason" in observations
        else pd.Series(dtype="bool")
    )
    blocked_exit = (
        episodes["exit_block_reason"].notna()
        if not episodes.empty and "exit_block_reason" in episodes
        else pd.Series(dtype="bool")
    )
    entry_cause_required = entry_statuses.isin(("never_filled", "delayed_fill", "not_verifiable"))
    exit_cause_required = exit_statuses.isin(("never_exited", "delayed_exit", "not_verifiable"))
    required_count = int(entry_cause_required.sum() + exit_cause_required.sum())
    direct_count = int((entry_cause_required & blocked_entry).sum() + (exit_cause_required & blocked_exit).sum())
    unresolved_count = int((entry_cause_required & ~blocked_entry).sum() + (exit_cause_required & ~blocked_exit).sum())
    strategy_not_attempted_count = int(
        entry_statuses.eq("not_attempted_by_strategy").sum() + exit_statuses.eq("not_attempted_by_strategy").sum()
    )
    entry_loss_coverage = (
        float(observations.loc[blocked_entry, "missed_mfe_due_to_entry_block"].notna().mean())
        if bool(blocked_entry.any())
        else None
    )
    exit_loss_coverage = (
        float(episodes.loc[blocked_exit, "blocked_exit_extra_drawdown"].notna().mean())
        if bool(blocked_exit.any())
        else None
    )
    loss_complete = all(value is None or value == 1.0 for value in (entry_loss_coverage, exit_loss_coverage))
    direct_or_strategy_evidence = direct_count > 0 or strategy_not_attempted_count > 0
    cause_observation_count = int(len(entry_statuses) + len(exit_statuses))
    cause_complete = unresolved_count == 0
    cause = FamilyEvidenceStatus(
        status=(
            FamilyComputationStatus.COMPUTED
            if cause_observation_count > 0 and cause_complete and loss_complete
            else (
                FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
                if direct_or_strategy_evidence
                else FamilyComputationStatus.NOT_VERIFIABLE
            )
        ),
        available_inputs=("explicit_execution_reason_or_strategy_intent",) if direct_or_strategy_evidence else (),
        missing_inputs=("queue_or_reason_code",) if not cause_complete else (),
        coverage={
            "cause_required_event_count": required_count,
            "direct_cause_count": direct_count,
            "direct_cause_coverage": direct_count / required_count if required_count else None,
            "strategy_not_attempted_count": strategy_not_attempted_count,
            "unresolved_cause_count": unresolved_count,
            "entry_block_loss_coverage": entry_loss_coverage,
            "exit_block_loss_coverage": exit_loss_coverage,
        },
        limitations=(
            ("some failed or unverifiable execution events lack direct reason evidence",) if not cause_complete else ()
        )
        + (("some directly blocked events lack loss-path evidence",) if not loss_complete else ()),
        reason_codes=(QELongTrendReason.EXECUTION_EVIDENCE_INSUFFICIENT.value,)
        if not cause_complete or not loss_complete
        else (),
        data_actions=(
            _family_data_action(
                "archive_missing_execution_reason_evidence",
                "execution_cause",
                required_fields=("reason_code", "order_intent"),
            ),
        )
        if not cause_complete or not loss_complete
        else (),
    )
    return fill, cause


def compute_execution_metrics(
    observations: pd.DataFrame,
    episodes: pd.DataFrame,
) -> list[dict[str, Any]]:
    if (observations.empty or "entry_execution_status" not in observations) and (
        episodes.empty or "exit_execution_status" not in episodes
    ):
        return []
    entry_counts = (
        observations["entry_execution_status"].value_counts(dropna=False).to_dict()
        if not observations.empty and "entry_execution_status" in observations
        else {}
    )
    exit_counts = (
        episodes["exit_execution_status"].value_counts(dropna=False).to_dict()
        if not episodes.empty and "exit_execution_status" in episodes
        else {}
    )
    entry_delay = (
        pd.to_numeric(observations["entry_delay_days"], errors="coerce")
        if "entry_delay_days" in observations
        else pd.Series(dtype="float64")
    )
    exit_delay = (
        pd.to_numeric(episodes["exit_delay_days"], errors="coerce")
        if "exit_delay_days" in episodes
        else pd.Series(dtype="float64")
    )
    return [
        {
            "metric_scope": "order_fill",
            "metric_key": "entry_execution_summary",
            "slice": "all_oos",
            "horizon": None,
            "barrier": None,
            "k": None,
            "value_num": None,
            "value_json": {
                "entry_status_counts": {str(key): int(value) for key, value in entry_counts.items()},
                "exit_status_counts": {str(key): int(value) for key, value in exit_counts.items()},
                "entry_delay_days": _distribution(entry_delay),
                "exit_delay_days": _distribution(exit_delay),
                "direct_entry_block_reason_count": (
                    int(observations["entry_block_reason"].notna().sum()) if "entry_block_reason" in observations else 0
                ),
                "direct_exit_block_reason_count": (
                    int(episodes["exit_block_reason"].notna().sum()) if "exit_block_reason" in episodes else 0
                ),
                "missed_mfe_due_to_entry_block": (
                    _distribution(observations["missed_mfe_due_to_entry_block"])
                    if "missed_mfe_due_to_entry_block" in observations
                    else _distribution(pd.Series(dtype="float64"))
                ),
                "missed_barrier_winner_due_to_entry_block_count": (
                    int(observations["missed_barrier_winner_due_to_entry_block"].fillna(False).sum())
                    if "missed_barrier_winner_due_to_entry_block" in observations
                    else 0
                ),
                "blocked_exit_extra_drawdown": (
                    _distribution(episodes["blocked_exit_extra_drawdown"])
                    if "blocked_exit_extra_drawdown" in episodes
                    else _distribution(pd.Series(dtype="float64"))
                ),
                "blocked_exit_extra_holding_days": (
                    _distribution(episodes["blocked_exit_extra_holding_days"])
                    if "blocked_exit_extra_holding_days" in episodes
                    else _distribution(pd.Series(dtype="float64"))
                ),
            },
            "quality_flag": (
                "computed_with_limitations"
                if "not_verifiable" in entry_counts or "not_verifiable" in exit_counts
                else "ok"
            ),
        }
    ]
