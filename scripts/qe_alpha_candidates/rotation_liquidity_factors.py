"""Standalone, file-only liquidity/recovery candidates; no catalog or DB writes.

Signals use the completed day t and are for execution no earlier than t+1.
Missing observations stay missing; rolling windows use the input trade calendar.
The six candidates complement (not replace) the four index/industry directions.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


FACTOR_NAMES = (
    "m_rebound_participation_asymmetry_20d",
    "m_downside_illiquidity_recovery_5d_20d",
    "m_volume_confirmed_drawdown_repair_20d",
    "m_return_volume_coupling_break_10d_60d",
    "m_overnight_intraday_repair_divergence_20d",
    "m_downside_range_absorption_20d",
    "m_downside_semivariance_shift_10d_60d",
)


def _positive(panel: pd.DataFrame) -> pd.DataFrame:
    return panel.where(np.isfinite(panel) & (panel > 0))


def _panels(pv: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if not isinstance(pv.index, pd.MultiIndex) or pv.index.names != ["datetime", "instrument"]:
        raise ValueError("daily_pv requires MultiIndex(datetime, instrument)")
    if pv.index.has_duplicates:
        raise ValueError("duplicate stock/date observations")
    if not isinstance(pv.index.levels[0], pd.DatetimeIndex) or pv.index.get_level_values(0).hasnans:
        raise ValueError("datetime must contain valid timestamps")
    required = ["open", "high", "low", "close", "amount"]
    missing = set(required) - set(pv.columns)
    if missing:
        raise ValueError(f"missing daily_pv fields: {sorted(missing)}")
    result = {
        col: _positive(pv[col].unstack("instrument").sort_index().astype("float64"))
        for col in required
    }
    return result


def compute(pv: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    """Return one factor, retaining only finite original stock/date rows."""
    if factor_name not in FACTOR_NAMES:
        raise ValueError(f"unknown candidate: {factor_name}")
    p = _panels(pv)
    close, amount = p["close"], p["amount"]
    ret = close.pct_change(fill_method=None)
    valid = ret.notna() & amount.notna()

    if factor_name == FACTOR_NAMES[0]:
        # Amount-weighted directional participation minus equal-day participation.
        # This removes the level of the up/down-day imbalance itself.
        sign = np.sign(ret).where(valid)
        a = amount.where(valid)
        out = (a * sign).rolling(20).sum() / a.rolling(20).sum() - sign.rolling(20).mean()
    elif factor_name == FACTOR_NAMES[1]:
        # Conditional downside price impact, recent five vs preceding twenty days.
        # Zero is used only for an observed non-down day in the conditional sum.
        down = (ret < 0).astype(float).where(valid)
        impact = (ret.abs() / amount).where(ret < 0, 0.0).where(valid)
        now = impact.rolling(5).sum() / down.rolling(5).sum().replace(0, np.nan)
        before = impact.shift(5).rolling(20).sum() / down.shift(5).rolling(20).sum().replace(0, np.nan)
        out = np.log(_positive(before) / _positive(now))
    elif factor_name == FACTOR_NAMES[2]:
        # Repair of a pre-existing drawdown against the peak known five days ago.
        # Do not allow the later peak to redefine the earlier recovery target.
        peak = close.shift(5).rolling(20).max()
        deficit = (peak - close.shift(5)).where(peak > close.shift(5))
        repaired = (close.where(close <= peak, peak) - close.shift(5)) / deficit
        repaired = repaired.where(close.notna() & peak.notna())
        out = repaired * amount.rolling(5).mean() / amount.rolling(20).mean()
    elif factor_name == FACTOR_NAMES[3]:
        log_amount_change = np.log(amount).diff()
        out = ret.rolling(10).corr(log_amount_change) - ret.rolling(60).corr(log_amount_change)
    elif factor_name == FACTOR_NAMES[4]:
        # Intraday buying versus overnight repricing, scaled by realized risk.
        intraday = np.log(close / p["open"])
        overnight = np.log(p["open"] / close.shift(1))
        daily = np.log(close / close.shift(1))
        out = (intraday - overnight).rolling(20).mean() / daily.rolling(20).std().replace(0, np.nan)
    elif factor_name == FACTOR_NAMES[5]:
        # Lower-wick absorption on observed down days, weighted by amount surprise.
        high, low, opening = p["high"], p["low"], p["open"]
        valid_bar = (high >= close) & (high >= opening) & (low <= close) & (low <= opening) & (high > low)
        bottom = opening.where(opening <= close, close)
        wick = ((bottom - low) / (high - low)).where(valid_bar)
        relative_amount = amount / amount.shift(1).rolling(20).mean()
        observed = valid & wick.notna() & relative_amount.notna()
        down = (ret < 0).astype(float).where(observed)
        absorption = (wick * relative_amount).where(ret < 0, 0.0).where(observed)
        out = absorption.rolling(20).sum() / down.rolling(20).sum().replace(0, np.nan)
    else:
        total = ret.pow(2)
        downside = total.where(ret < 0, 0.0).where(ret.notna())
        short = downside.rolling(10).sum() / total.rolling(10).sum().replace(0, np.nan)
        long = downside.rolling(60).sum() / total.rolling(60).sum().replace(0, np.nan)
        out = long - short

    out = out.where(np.isfinite(out))
    result = out.rename_axis(index="datetime", columns="instrument").stack().reindex(pv.index)
    return result.dropna().rename(factor_name).to_frame().sort_index()


def compute_factor(factor_name: str) -> None:
    """Catalog entrypoint: generated source passes its exact name explicitly."""
    result = compute(pd.read_hdf(Path(__file__).resolve().parent.parent / "daily_pv.h5"), factor_name)
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")
