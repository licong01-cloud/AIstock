"""Read-once base-data cache for official offline factor computation."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd

ALLOWED_BASE_DATA_FILES: tuple[str, ...] = (
    "daily_pv.h5",
    "daily_basic.h5",
    "moneyflow.h5",
    "bak_basic.h5",
    "cyq_perf.h5",
    "sector_data.h5",
    "margin_detail.h5",
    "static_factors.parquet",
)
SUPPLEMENTAL_DATA_FILES = ("index_factor_context.h5",)
_MARGIN_DATA_FILE = "margin_detail.h5"
_STATIC_DATA_FILE = "static_factors.parquet"
_MARGIN_COLUMN_PREFIX = "md_"


@dataclass
class BaseDataEntry:
    name: str
    path: Path
    dataframe: pd.DataFrame
    elapsed_sec: float
    size_mb: float
    rows: int
    columns: int


@dataclass
class BacktestBaseDataMemoryCache:
    """Immutable-ish in-process cache for h5/parquet factor base data."""

    factor_data_dir: Path
    start_date: str
    end_date: str
    entries: dict[str, BaseDataEntry] = field(default_factory=dict)
    read_counts: dict[str, int] = field(default_factory=dict)

    @classmethod
    def load_once(
        cls,
        factor_data_dir: str | os.PathLike[str],
        start_date: str,
        end_date: str,
        allowed_files: Iterable[str] = ALLOWED_BASE_DATA_FILES,
        *,
        hdf_reader: Callable[..., pd.DataFrame] | None = None,
        parquet_reader: Callable[..., pd.DataFrame] | None = None,
        supplemental_data_dir: str | os.PathLike[str] | None = None,
    ) -> "BacktestBaseDataMemoryCache":
        root = Path(factor_data_dir).expanduser().resolve()
        if not root.is_dir():
            raise FileNotFoundError(f"factor_data_dir not found: {root}")
        cache = cls(root, start_date, end_date)
        hdf_reader = hdf_reader or pd.read_hdf
        parquet_reader = parquet_reader or pd.read_parquet
        inputs = [(root, name) for name in allowed_files]
        if supplemental_data_dir is not None:
            extra_root = Path(supplemental_data_dir).expanduser().resolve(strict=True)
            inputs.extend((extra_root, name) for name in SUPPLEMENTAL_DATA_FILES)
        priority = {"daily_pv.h5": 0, _MARGIN_DATA_FILE: 1, _STATIC_DATA_FILE: 2}
        inputs.sort(key=lambda item: priority.get(item[1], 3))
        trading_calendar: pd.DatetimeIndex | None = None
        for input_root, name in inputs:
            path = (input_root / name).resolve()
            if not path.is_file():
                if name in SUPPLEMENTAL_DATA_FILES:
                    raise FileNotFoundError(f"explicit supplemental input missing: {path}")
                continue
            if input_root not in path.parents:
                raise RuntimeError(f"base data path escapes factor_data_dir: {path}")
            t0 = time.time()
            if name.endswith(".h5"):
                df = hdf_reader(path)
            elif name.endswith(".parquet"):
                df = parquet_reader(path)
            else:
                raise RuntimeError(f"unsupported base data file: {name}")
            original_rows = int(len(df))
            original_columns = int(len(df.columns)) if hasattr(df, "columns") else 0
            if name == "daily_pv.h5":
                trading_calendar = _trading_calendar(df)
            elif name == _MARGIN_DATA_FILE:
                if trading_calendar is None:
                    raise RuntimeError(
                        "official margin_detail availability projection requires daily_pv.h5 trading calendar"
                    )
                df = _project_margin_to_next_trading_day(df, trading_calendar)
            sliced_df = cache._slice_by_date(df)
            if name == _STATIC_DATA_FILE:
                margin_columns = [
                    column
                    for column in sliced_df.columns
                    if str(column).startswith(_MARGIN_COLUMN_PREFIX)
                ]
                if margin_columns:
                    margin_entry = cache.entries.get(_MARGIN_DATA_FILE)
                    if margin_entry is None:
                        raise RuntimeError(
                            "static md_* columns require margin_detail.h5 availability projection"
                        )
                    sliced_df = _replace_static_margin_columns(
                        sliced_df, margin_entry.dataframe, margin_columns
                    )
            del df
            cache.read_counts[name] = cache.read_counts.get(name, 0) + 1
            cache.entries[name] = BaseDataEntry(
                name=name,
                path=path,
                dataframe=sliced_df,
                elapsed_sec=round(time.time() - t0, 3),
                size_mb=round(path.stat().st_size / 1024 / 1024, 3),
                rows=original_rows,
                columns=original_columns,
            )
        if not cache.entries:
            raise RuntimeError(f"no allowed base data files found under {root}")
        return cache

    def get(self, name_or_path: str | os.PathLike[str], *, columns: Any = None) -> pd.DataFrame:
        name = Path(str(name_or_path)).name
        if name not in ALLOWED_BASE_DATA_FILES + SUPPLEMENTAL_DATA_FILES:
            raise FileNotFoundError(f"official offline factor code cannot read unknown base data file: {name}")
        entry = self.entries.get(name)
        if entry is None:
            raise FileNotFoundError(f"base data file was not loaded: {name}")
        df = entry.dataframe
        if columns is not None and hasattr(df, "loc"):
            try:
                return df.loc[:, list(columns)].copy(deep=False)
            except Exception:
                return df[columns].copy(deep=False)
        return df.copy(deep=False)

    def manifest(self) -> dict[str, Any]:
        return {
            "factor_data_dir": str(self.factor_data_dir),
            "data_start": self.start_date,
            "data_end": self.end_date,
            "base_data_cache_policy": "load_once_readonly",
            "files": {
                name: {
                    "path": str(entry.path),
                    "size_mb": entry.size_mb,
                    "rows": entry.rows,
                    "columns": entry.columns,
                    "elapsed_sec": entry.elapsed_sec,
                    "read_count": self.read_counts.get(name, 0),
                }
                for name, entry in self.entries.items()
            },
        }

    def _slice_by_date(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy(deep=False)
        try:
            if isinstance(df.index, pd.MultiIndex):
                level = "datetime" if "datetime" in df.index.names else 0
                dates = pd.to_datetime(df.index.get_level_values(level), errors="coerce")
                mask = (dates >= pd.Timestamp(self.start_date)) & (dates <= pd.Timestamp(self.end_date))
                return _copy_slice_releasing_parent(df, mask)
            for col in ("datetime", "trade_date", "date"):
                if col in df.columns:
                    dates = pd.to_datetime(df[col], errors="coerce")
                    mask = (dates >= pd.Timestamp(self.start_date)) & (dates <= pd.Timestamp(self.end_date))
                    return _copy_slice_releasing_parent(df, mask)
        except Exception:
            return df.copy(deep=False)
        return df.copy(deep=False)


def _copy_slice_releasing_parent(df: pd.DataFrame, mask: Any) -> pd.DataFrame:
    sliced = df.loc[mask]
    if len(sliced) < len(df):
        return sliced.copy(deep=True)
    return sliced.copy(deep=False)


def _trading_calendar(df: pd.DataFrame) -> pd.DatetimeIndex:
    if not isinstance(df.index, pd.MultiIndex) or "datetime" not in df.index.names:
        raise RuntimeError("daily_pv.h5 must use a MultiIndex with datetime for availability projection")
    raw_dates = df.index.get_level_values("datetime")
    dates = (
        raw_dates
        if isinstance(raw_dates, pd.DatetimeIndex)
        else pd.DatetimeIndex(pd.to_datetime(raw_dates, errors="coerce"))
    )
    if dates.isna().any():
        raise RuntimeError("daily_pv.h5 contains invalid trading dates")
    calendar = pd.DatetimeIndex(dates.unique()).sort_values()
    if calendar.empty or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise RuntimeError("daily_pv.h5 trading calendar must be ordered and unique")
    return calendar


def _project_margin_to_next_trading_day(
    df: pd.DataFrame, trading_calendar: pd.DatetimeIndex
) -> pd.DataFrame:
    if df.empty:
        return df.copy(deep=False)
    if not isinstance(df.index, pd.MultiIndex) or {
        "datetime",
        "instrument",
    } - set(df.index.names):
        raise RuntimeError("margin_detail.h5 must use a MultiIndex with datetime and instrument")
    if df.index.has_duplicates:
        raise RuntimeError("margin_detail.h5 contains duplicate source keys")

    raw_source_dates = df.index.get_level_values("datetime")
    source_dates = (
        raw_source_dates
        if isinstance(raw_source_dates, pd.DatetimeIndex)
        else pd.DatetimeIndex(pd.to_datetime(raw_source_dates, errors="coerce"))
    )
    if source_dates.isna().any():
        raise RuntimeError("margin_detail.h5 contains invalid source dates")
    source_positions = trading_calendar.get_indexer(source_dates)
    if (source_positions < 0).any():
        raise RuntimeError("margin_detail.h5 contains dates outside the daily trading calendar")

    has_next_trading_day = source_positions + 1 < len(trading_calendar)
    projected = df.loc[has_next_trading_day].copy(deep=False)
    if projected.empty:
        return projected

    index_arrays: list[Any] = []
    for name in df.index.names:
        if name == "datetime":
            index_arrays.append(trading_calendar[source_positions[has_next_trading_day] + 1])
        else:
            index_arrays.append(df.index.get_level_values(name)[has_next_trading_day])
    projected.index = pd.MultiIndex.from_arrays(index_arrays, names=df.index.names)
    if not projected.index.is_monotonic_increasing:
        projected = projected.sort_index()
    if projected.index.has_duplicates:
        raise RuntimeError("margin_detail availability projection produced duplicate keys")
    return projected


def _replace_static_margin_columns(
    static: pd.DataFrame,
    projected_margin: pd.DataFrame,
    margin_columns: list[Any],
) -> pd.DataFrame:
    output = static.copy(deep=False)
    replacement = projected_margin.reindex(
        index=output.index,
        columns=margin_columns,
    )
    output[margin_columns] = replacement
    return output
