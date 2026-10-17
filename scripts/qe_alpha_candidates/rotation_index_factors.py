"""Three PIT size-bucket candidates. Inputs are files, never live database calls."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

FACTOR_NAMES = (
    "m_pit_size_bucket_relative_strength_20d",
    "m_pit_size_rotation_acceleration_5d_20d",
    "m_pit_index_beta_break_20d_60d",
)


def compute(pv: pd.DataFrame, context: pd.DataFrame, name: str) -> pd.DataFrame:
    if name not in FACTOR_NAMES:
        raise ValueError(f"unknown index candidate: {name}")
    for frame in (pv, context):
        if frame.index.names != ["datetime", "instrument"] or frame.index.has_duplicates:
            raise ValueError("unique MultiIndex(datetime,instrument) required")
    required = {"pool_id", "relative_strength_20d", "acceleration_5d_20d", "index_return_1d"}
    if not required.issubset(context):
        raise ValueError(f"context fields missing: {sorted(required - set(context))}")
    if not context.pool_id.isin([300, 500, 1000]).all():
        raise ValueError("context contains an invalid size pool")
    context = context.reindex(pv.index)
    if name != FACTOR_NAMES[2]:
        column = "relative_strength_20d" if name == FACTOR_NAMES[0] else "acceleration_5d_20d"
        signal = context[column]
    else:
        close = pv.close.unstack("instrument").sort_index()
        close = close.where(np.isfinite(close) & (close > 0))
        stock_ret = close.pct_change(fill_method=None)
        index_ret = context.index_return_1d.unstack("instrument").reindex_like(close)
        pool = context.pool_id.unstack("instrument").reindex_like(close)
        # One regression cannot splice two distinct index return histories.
        unchanged = pool.rolling(60).max().eq(pool.rolling(60).min()) & pool.rolling(60).count().eq(60)
        short = stock_ret.rolling(20).cov(index_ret) / index_ret.rolling(20).var().replace(0, np.nan)
        long = stock_ret.rolling(60).cov(index_ret) / index_ret.rolling(60).var().replace(0, np.nan)
        signal = (short - long).where(unchanged).stack().reindex(pv.index)
    signal = signal.where(np.isfinite(signal) & pv.close.notna() & (pv.close > 0))
    return signal.dropna().rename(name).to_frame().sort_index()


def compute_factor(name: str) -> None:
    root = Path(__file__).resolve().parent.parent
    result = compute(pd.read_hdf(root / "daily_pv.h5"), pd.read_hdf(root / "index_factor_context.h5"), name)
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")
