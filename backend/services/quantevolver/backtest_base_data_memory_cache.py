"""Read-once base-data cache for official offline factor computation."""
from __future__ import annotations

import hashlib
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


@dataclass
class BaseDataEntry:
    name: str
    path: Path
    dataframe: pd.DataFrame
    elapsed_sec: float
    size_mb: float
    rows: int
    columns: int
    sha256_16: str


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
    ) -> "BacktestBaseDataMemoryCache":
        root = Path(factor_data_dir).expanduser().resolve()
        if not root.is_dir():
            raise FileNotFoundError(f"factor_data_dir not found: {root}")
        cache = cls(root, start_date, end_date)
        hdf_reader = hdf_reader or pd.read_hdf
        parquet_reader = parquet_reader or pd.read_parquet
        for name in allowed_files:
            path = (root / name).resolve()
            if not path.is_file():
                continue
            if root not in path.parents and path != root:
                raise RuntimeError(f"base data path escapes factor_data_dir: {path}")
            t0 = time.time()
            if name.endswith(".h5"):
                df = hdf_reader(path)
            elif name.endswith(".parquet"):
                df = parquet_reader(path)
            else:
                raise RuntimeError(f"unsupported base data file: {name}")
            sliced_df = cache._slice_by_date(df)
            original_rows = int(len(df))
            original_columns = int(len(df.columns)) if hasattr(df, "columns") else 0
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
                sha256_16=_file_sha256_16(path),
            )
        if not cache.entries:
            raise RuntimeError(f"no allowed base data files found under {root}")
        return cache

    def get(self, name_or_path: str | os.PathLike[str], *, columns: Any = None) -> pd.DataFrame:
        name = Path(str(name_or_path)).name
        if name not in ALLOWED_BASE_DATA_FILES:
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
                    "sha256_16": entry.sha256_16,
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


def _file_sha256_16(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _copy_slice_releasing_parent(df: pd.DataFrame, mask: Any) -> pd.DataFrame:
    sliced = df.loc[mask]
    if len(sliced) < len(df):
        return sliced.copy(deep=True)
    return sliced.copy(deep=False)
