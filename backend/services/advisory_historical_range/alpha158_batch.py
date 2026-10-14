"""Causal, bounded-window evaluation of the existing inference Alpha158 formulas.

This is an execution optimization, not a feature/label/model change.  Unknown
features retain the existing calculator.  No factor receives the batch buffer.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd


FEATURE_ORDER = (
    "KLEN", "KLOW", "WVMA5", "WVMA60", "CORR5", "CORR10", "CORR20", "CORR60",
    "CORD5", "CORD10", "CORD60", "RSQR5", "RESI5", "RSQR10", "RESI10",
    "RSQR20", "RSQR60", "VSTD5", "STD5", "ROC60",
)


def supports_batch(history: pd.DataFrame, columns: Sequence[str]) -> bool:
    """Select an equivalent optimization, never an alternative business policy."""
    return not history.empty and bool(columns) and set(columns).issubset(FEATURE_ORDER) and (
        isinstance(history.index, pd.MultiIndex)
        and history.index.names == ["datetime", "instrument"]
        and isinstance(history.index.get_level_values("datetime"), pd.DatetimeIndex)
        and history.index.get_level_values("datetime").tz is None
        and history.index.get_level_values("datetime").is_monotonic_increasing
        and history.index.is_unique
        and history.columns.is_unique
        and not isinstance(history.index.get_level_values("instrument"), pd.CategoricalIndex)
        and {"open", "high", "low", "close"}.issubset(history.columns)
        and ("volume" in history.columns or "vol" in history.columns)
        and all(isinstance(history[column].dtype, np.dtype) and history[column].dtype.kind in "biuf"
                for column in ("open", "high", "low", "close", "volume" if "volume" in history.columns else "vol"))
    )


class Alpha158BatchEvaluator:
    """Index each instrument once; evaluate only its exact causal tail per day.

    Memory is O(input rows + instrument count), not dates times warm-up rows.
    Window work is O(dates * instruments * feature window), without repeated
    full-frame ``xs`` or prefix copies.  Only the known causal formulas above
    are evaluated against a batch; arbitrary factor code uses its original view.
    """

    def __init__(self, history: pd.DataFrame) -> None:
        self._history = history
        self._dates = history.index.get_level_values("datetime")
        self._date_values = self._dates.to_numpy()
        self._groups = history.groupby(level="instrument", sort=True).indices
        self._instruments = history.index.get_level_values("instrument").unique()
        self._group_dates = {
            instrument: self._date_values[positions]
            for instrument, positions in self._groups.items()
        }
        volume_column = "volume" if "volume" in history.columns else "vol"
        close, volume = history["close"], history[volume_column]
        close_groups = close.groupby(level="instrument")
        volume_groups = volume.groupby(level="instrument")
        self._close = close.to_numpy()
        self._volume = volume.to_numpy()
        self._volume_log = np.log(volume.abs() + 1.0).to_numpy()
        self._volume_log_return = np.log(volume / (volume_groups.shift(1) + 1e-12) + 1.0).to_numpy()
        self._close_return = (close / (close_groups.shift(1) + 1e-12)).to_numpy()
        # Preserve pct_change's existing fill behavior, including long NaN runs.
        self._weighted_move = (close_groups.pct_change().abs() * volume).fillna(0.0).to_numpy()

    def for_dates(self, dates: Sequence[pd.Timestamp], columns: Sequence[str]) -> pd.DataFrame:
        return pd.concat([self._for_date(pd.Timestamp(day), columns) for day in dates])

    def _for_date(self, day: pd.Timestamp, columns: Sequence[str]) -> pd.DataFrame:
        requested = set(columns)
        day_value = day.to_datetime64()
        ends = {
            instrument: int(np.searchsorted(group_dates, day_value, side="right"))
            for instrument, group_dates in self._group_dates.items()
        }
        instruments = [instrument for instrument in self._instruments if ends[instrument]]
        sorted_instruments = [instrument for instrument in self._groups if ends[instrument]]
        index = pd.MultiIndex.from_product([[day], instruments], names=["datetime", "instrument"])
        sorted_index = pd.MultiIndex.from_product([[day], sorted_instruments], names=index.names)
        out: dict[str, pd.Series] = {}

        def tail(values: np.ndarray, instrument: str, window: int) -> np.ndarray | None:
            end = ends[instrument]
            if end < window:
                return None
            return values[self._groups[instrument][end - window:end]]

        last_day = self._history.loc[day]
        last_index = pd.MultiIndex.from_product([[day], last_day.index], names=index.names)
        if "KLEN" in requested:
            values = (last_day["high"] - last_day["low"]) / last_day["open"].replace(0.0, np.nan)
            out["KLEN"] = pd.Series(values.to_numpy(), index=last_index)
        if "KLOW" in requested:
            lower_body = last_day[["open", "close"]].min(axis=1)
            values = (lower_body - last_day["low"]) / last_day["open"].replace(0.0, np.nan)
            out["KLOW"] = pd.Series(values.to_numpy(), index=last_index)

        for name in ("WVMA5", "WVMA60"):
            if name in requested:
                window = int(name[4:])
                values = []
                for instrument in sorted_instruments:
                    sample = tail(self._weighted_move, instrument, window)
                    values.append(np.nan if sample is None else np.std(sample) / (np.mean(sample) + 1e-12))
                # Preserve the legacy groupby-result/unique-index positional
                # convention.  Correcting it would be a separate semantic fix.
                out[name] = pd.Series(values, index=index)

        for prefix, left, right in (
            ("CORR", self._close, self._volume_log),
            ("CORD", self._close_return, self._volume_log_return),
        ):
            for window in ((5, 10, 20, 60) if prefix == "CORR" else (5, 10, 60)):
                name = f"{prefix}{window}"
                if name not in requested:
                    continue
                values = []
                for instrument in instruments:
                    a, b = tail(left, instrument, window), tail(right, instrument, window)
                    values.append(np.nan if a is None else np.corrcoef(a, b)[0, 1])
                out[name] = pd.Series(values, index=index)

        for window in (5, 10, 20, 60):
            r2_name, residual_name = f"RSQR{window}", f"RESI{window}"
            if r2_name not in requested and residual_name not in requested:
                continue
            r2_values, residual_values = [], []
            x = np.arange(window)
            for instrument in sorted_instruments:
                sample = tail(self._close, instrument, window)
                r2, residual = np.nan, np.nan
                if sample is not None:
                    try:
                        slope, intercept = np.polyfit(x, sample, 1)
                        fitted = slope * x + intercept
                        residual = sample[-1] - fitted[-1]
                        sst = np.sum((sample - np.mean(sample)) ** 2)
                        sse = np.sum((sample - fitted) ** 2)
                        r2 = 1 - sse / (sst + 1e-12)
                    except (ValueError, np.linalg.LinAlgError, FloatingPointError):
                        # The established regression formula yields unavailable
                        # values on a failed fit; it does not invent a feature.
                        pass
                r2_values.append(r2)
                residual_values.append(residual)
            if r2_name in requested:
                out[r2_name] = pd.Series(r2_values, index=sorted_index)
            if residual_name in requested:
                residual = pd.Series(residual_values, index=sorted_index)
                denominator = pd.Series(last_day["close"].to_numpy(), index=last_index).replace(0.0, np.nan)
                out[residual_name] = residual / denominator

        for name, raw in (("VSTD5", self._volume), ("STD5", self._close), ("ROC60", self._close)):
            if name not in requested:
                continue
            window = 61 if name == "ROC60" else 5
            values = []
            for instrument in sorted_instruments:
                sample = tail(raw, instrument, window)
                values.append(np.nan if sample is None else (
                    sample[0] / sample[-1] if name == "ROC60" else np.std(sample)
                ))
            result = pd.Series(values, index=index)
            if name != "ROC60":
                column = "close" if name == "STD5" else ("volume" if "volume" in last_day else "vol")
                denominator = pd.Series(last_day[column].to_numpy(), index=last_index)
                denominator = denominator.replace(0.0, np.nan) if name == "STD5" else denominator.abs() + 1e-12
                result = result / denominator
            out[name] = result
        return pd.DataFrame({name: out[name] for name in FEATURE_ORDER if name in requested}).sort_index()
