"""Read close prices from qlib_bin directory without qlib dependency."""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Module-level cache: {qlib_bin_path_str: DataFrame}
_close_cache: dict[str, pd.DataFrame] = {}


def _default_qlib_bin_path() -> Path:
    """Resolve qlib_bin path from environment or default."""
    env = os.environ.get("QLIB_BIN_PATH")
    if env:
        return Path(env)
    # Windows path used in conf files
    candidates = [
        Path(os.environ["QLIB_DAY_DATA"]) if os.environ.get("QLIB_DAY_DATA") else None,
    ]
    for c in candidates:
        if c is not None and c.exists():
            return c
    raise FileNotFoundError("qlib_bin directory not found. Set QLIB_BIN_PATH env var.")


def _read_calendar(qlib_bin: Path) -> pd.DatetimeIndex:
    """Read trading calendar from qlib_bin."""
    cal_file = qlib_bin / "calendars" / "day.txt"
    dates = pd.read_csv(cal_file, header=None, names=["date"], parse_dates=["date"])
    return pd.DatetimeIndex(dates["date"])


def _read_instruments(qlib_bin: Path) -> dict[str, tuple[str, str]]:
    """Read instrument date ranges. Returns {INSTRUMENT: (start, end)}."""
    inst_file = qlib_bin / "instruments" / "all.txt"
    result = {}
    with open(inst_file, "r") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                result[parts[0]] = (parts[1], parts[2])
    return result


def _read_single_bin(bin_path: Path) -> tuple[np.ndarray, int]:
    """Read a single qlib bin file (qlib format: 4-byte start_index header + float32 data).

    Returns (data_array, start_index_in_calendar).
    """
    with open(bin_path, "rb") as f:
        buf = f.read()
    all_vals = np.frombuffer(buf, dtype="<f")
    start_index = int(all_vals[0])  # first float is calendar start index
    return all_vals[1:], start_index


def _load_all_close_prices(qlib_bin: Path) -> pd.DataFrame:
    """Load ALL close prices from qlib_bin (no filtering). Result is cached."""
    cache_key = str(qlib_bin)
    if cache_key in _close_cache:
        logger.info(f"Close price cache HIT ({cache_key})")
        return _close_cache[cache_key]

    logger.info(f"Close price cache MISS, loading from {qlib_bin} ...")
    t0 = time.time()

    calendar = _read_calendar(qlib_bin)
    instruments = _read_instruments(qlib_bin)
    features_dir = qlib_bin / "features"

    records = []
    for inst_upper, (inst_start, _inst_end) in instruments.items():
        bin_path = features_dir / inst_upper.lower() / "close.day.bin"
        if not bin_path.exists():
            continue
        values, cal_start_idx = _read_single_bin(bin_path)
        n_values = min(len(values), len(calendar) - cal_start_idx)
        values = values[:n_values]
        dates = calendar[cal_start_idx : cal_start_idx + n_values]
        # Respect inst_start from all.txt (IPO filter applied by snapshot_writer)
        start_ts = pd.Timestamp(inst_start)
        mask = dates >= start_ts
        dates = dates[mask]
        values = values[mask]
        if len(dates) == 0:
            continue
        df = pd.DataFrame(
            {"close": values},
            index=pd.MultiIndex.from_arrays(
                [dates, [inst_upper] * len(dates)], names=["datetime", "instrument"]
            ),
        )
        df.loc[df["close"] == 0.0, "close"] = np.nan
        records.append(df)

    if not records:
        raise ValueError("No close price data found in qlib_bin")

    result = pd.concat(records)
    result.sort_index(inplace=True)
    _close_cache[cache_key] = result
    logger.info(f"Close prices loaded: {len(result)} rows, {time.time()-t0:.1f}s (cached)")
    return result


def _expand_instrument_filter(instrument_filter: set[str]) -> set[str]:
    """Expand filter to include both naming conventions."""
    expanded = set(instrument_filter)
    for name in instrument_filter:
        if '.' in name:
            parts = name.split('.')
            if len(parts) == 2:
                expanded.add(parts[1] + parts[0])
        elif len(name) >= 8 and name[:2].isalpha():
            expanded.add(name[2:] + '.' + name[:2])
    return expanded


def read_close_prices(
    qlib_bin: Optional[Path] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    instrument_filter: Optional[set[str]] = None,
) -> pd.DataFrame:
    """Read close prices with module-level caching.

    First call loads all instruments into cache; subsequent calls filter from cache.
    """
    if qlib_bin is None:
        qlib_bin = _default_qlib_bin_path()

    full_df = _load_all_close_prices(qlib_bin)
    result = full_df

    # Filter instruments
    if instrument_filter:
        fset = _expand_instrument_filter(instrument_filter)
        insts = result.index.get_level_values("instrument")
        result = result[insts.isin(fset)]

    # Filter date range
    if start_date or end_date:
        idx = result.index.get_level_values("datetime")
        mask = np.ones(len(result), dtype=bool)
        if start_date:
            mask &= idx >= pd.Timestamp(start_date)
        if end_date:
            mask &= idx <= pd.Timestamp(end_date)
        result = result[mask]

    return result


def clear_close_cache():
    """Clear the close price cache (e.g. after qlib_bin data update)."""
    _close_cache.clear()
    logger.info("Close price cache cleared")
