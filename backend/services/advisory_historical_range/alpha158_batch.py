"""Bounded Alpha158 evaluation using the original Qlib feature semantics.

Instrument labels, sample standard deviation, partial windows and unavailable
values follow the training loader. No factor receives this private buffer.
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

    def __init__(self, history: pd.DataFrame, *, calendar: Sequence | None = None) -> None:
        if calendar is not None:
            # Qlib shifts and rolling windows count trading-calendar positions,
            # including missing bars. Keep those positions NA in this private
            # Alpha158 view; dynamic factor code still receives its original H5.
            dates = pd.DatetimeIndex(calendar)
            observed = history.index.get_level_values("datetime")
            dates = dates[(dates >= observed.min()) & (dates <= observed.max())]
            instruments = history.index.get_level_values("instrument").unique()
            history = history.reindex(pd.MultiIndex.from_product(
                [dates, instruments], names=["datetime", "instrument"]))
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
        self._volume_log = np.log(volume + 1.0).to_numpy()
        self._volume_log_return = np.log(volume / volume_groups.shift(1) + 1.0).to_numpy()
        self._close_return = (close / close_groups.shift(1)).to_numpy()
        self._weighted_move = ((close / close_groups.shift(1) - 1).abs() * volume).to_numpy()

    def for_dates(self, dates: Sequence[pd.Timestamp], columns: Sequence[str]) -> pd.DataFrame:
        return pd.concat([self._for_date(pd.Timestamp(day), columns) for day in dates])

    def _for_date(self, day: pd.Timestamp, columns: Sequence[str]) -> pd.DataFrame:
        requested = set(columns)
        day_value = day.to_datetime64()
        ends = {
            instrument: int(np.searchsorted(group_dates, day_value, side="right"))
            for instrument, group_dates in self._group_dates.items()
        }
        instruments = [instrument for instrument in self._groups if ends[instrument]]
        index = pd.MultiIndex.from_product([[day], instruments], names=["datetime", "instrument"])
        out: dict[str, pd.Series] = {}

        def tail(values: np.ndarray, instrument: str, window: int) -> np.ndarray | None:
            end = ends[instrument]
            if end == 0:
                return None
            return values[self._groups[instrument][max(0, end - window):end]]

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
                for instrument in instruments:
                    sample = tail(self._weighted_move, instrument, window)
                    finite = sample[np.isfinite(sample)] if sample is not None else np.array([])
                    values.append(np.nan if len(finite) < 2 else np.std(finite, ddof=1) / (np.mean(finite) + 1e-12))
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
                    if a is None or b is None:
                        values.append(np.nan)
                        continue
                    valid = np.isfinite(a) & np.isfinite(b)
                    a_valid, b_valid = a[valid], b[valid]
                    # Qlib Corr masks near-constant inputs using each complete
                    # rolling input's sample deviation, before pairwise corr.
                    a_finite, b_finite = a[np.isfinite(a)], b[np.isfinite(b)]
                    constant = (len(a_finite) < 2 or len(b_finite) < 2
                                or np.isclose(np.std(a_finite, ddof=1), 0, atol=2e-5)
                                or np.isclose(np.std(b_finite, ddof=1), 0, atol=2e-5))
                    values.append(np.nan if constant or len(a_valid) < 2 else np.corrcoef(a_valid, b_valid)[0, 1])
                out[name] = pd.Series(values, index=index)

        for window in (5, 10, 20, 60):
            r2_name, residual_name = f"RSQR{window}", f"RESI{window}"
            if r2_name not in requested and residual_name not in requested:
                continue
            r2_values, residual_values = [], []
            for instrument in instruments:
                sample = tail(self._close, instrument, window)
                r2, residual = np.nan, np.nan
                if sample is not None:
                    valid = np.isfinite(sample)
                    x = np.arange(len(sample), dtype=float)
                    if valid.sum() >= 2:
                        xv, yv = x[valid], sample[valid]
                        centered_x = xv - xv.mean()
                        centered_y = yv - yv.mean()
                        slope = np.dot(centered_x, centered_y) / np.dot(centered_x, centered_x)
                        fitted = yv.mean() + slope * (x - xv.mean())
                        residual = sample[-1] - fitted[-1]
                        if not np.isclose(np.std(yv, ddof=1), 0, atol=2e-5):
                            r2 = 1 - np.sum((yv - fitted[valid]) ** 2) / np.dot(centered_y, centered_y)
                r2_values.append(r2)
                residual_values.append(residual)
            if r2_name in requested:
                out[r2_name] = pd.Series(r2_values, index=index)
            if residual_name in requested:
                residual = pd.Series(residual_values, index=index)
                denominator = pd.Series(last_day["close"].to_numpy(), index=last_index).replace(0.0, np.nan)
                out[residual_name] = residual / denominator

        for name, raw in (("VSTD5", self._volume), ("STD5", self._close), ("ROC60", self._close)):
            if name not in requested:
                continue
            window = 61 if name == "ROC60" else 5
            values = []
            for instrument in instruments:
                sample = tail(raw, instrument, window)
                finite = sample[np.isfinite(sample)] if sample is not None else np.array([])
                if name == "ROC60":
                    values.append(np.nan if sample is None or len(sample) < 61 else sample[0] / sample[-1])
                else:
                    values.append(np.nan if len(finite) < 2 else np.std(finite, ddof=1))
            result = pd.Series(values, index=index)
            if name != "ROC60":
                column = "close" if name == "STD5" else ("volume" if "volume" in last_day else "vol")
                denominator = pd.Series(last_day[column].to_numpy(), index=last_index)
                denominator = denominator.replace(0.0, np.nan) if name == "STD5" else denominator.abs() + 1e-12
                result = result / denominator
            out[name] = result
        return pd.DataFrame({name: out[name] for name in FEATURE_ORDER if name in requested}).sort_index()
