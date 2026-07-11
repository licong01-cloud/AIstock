"""Canonical Tushare moneyflow units and derived-factor formulas.

The source table ``market.moneyflow_ts`` intentionally preserves Tushare's
native units (volume=hand, amount=10k CNY).  Every AIstock consumer outside
that source boundary uses volume=share and amount=CNY.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


MONEYFLOW_UNIT_CONTRACT_VERSION = "tushare_moneyflow_shares_yuan_v1"
MONEYFLOW_VOLUME_MULTIPLIER = 100.0
MONEYFLOW_AMOUNT_MULTIPLIER = 10_000.0

TUSHARE_MONEYFLOW_VOLUME_COLUMNS = (
    "buy_sm_vol",
    "sell_sm_vol",
    "buy_md_vol",
    "sell_md_vol",
    "buy_lg_vol",
    "sell_lg_vol",
    "buy_elg_vol",
    "sell_elg_vol",
    "net_mf_vol",
)
TUSHARE_MONEYFLOW_AMOUNT_COLUMNS = (
    "buy_sm_amount",
    "sell_sm_amount",
    "buy_md_amount",
    "sell_md_amount",
    "buy_lg_amount",
    "sell_lg_amount",
    "buy_elg_amount",
    "sell_elg_amount",
    "net_mf_amount",
)

MONEYFLOW_FIELD_MAP = {
    "buy_sm_vol": "mf_sm_buy_vol",
    "buy_sm_amount": "mf_sm_buy_amt",
    "sell_sm_vol": "mf_sm_sell_vol",
    "sell_sm_amount": "mf_sm_sell_amt",
    "buy_md_vol": "mf_md_buy_vol",
    "buy_md_amount": "mf_md_buy_amt",
    "sell_md_vol": "mf_md_sell_vol",
    "sell_md_amount": "mf_md_sell_amt",
    "buy_lg_vol": "mf_lg_buy_vol",
    "buy_lg_amount": "mf_lg_buy_amt",
    "sell_lg_vol": "mf_lg_sell_vol",
    "sell_lg_amount": "mf_lg_sell_amt",
    "buy_elg_vol": "mf_elg_buy_vol",
    "buy_elg_amount": "mf_elg_buy_amt",
    "sell_elg_vol": "mf_elg_sell_vol",
    "sell_elg_amount": "mf_elg_sell_amt",
    "net_mf_vol": "mf_net_vol",
    "net_mf_amount": "mf_net_amt",
}

MONEYFLOW_FACTOR_VOLUME_COLUMNS = tuple(
    MONEYFLOW_FIELD_MAP[column] for column in TUSHARE_MONEYFLOW_VOLUME_COLUMNS
)
MONEYFLOW_FACTOR_AMOUNT_COLUMNS = tuple(
    MONEYFLOW_FIELD_MAP[column] for column in TUSHARE_MONEYFLOW_AMOUNT_COLUMNS
)
MONEYFLOW_FACTOR_COLUMNS = MONEYFLOW_FACTOR_VOLUME_COLUMNS + MONEYFLOW_FACTOR_AMOUNT_COLUMNS


def moneyflow_unit_contract_receipt() -> dict[str, Any]:
    return {
        "version": MONEYFLOW_UNIT_CONTRACT_VERSION,
        "source_volume_unit": "hand",
        "source_amount_unit": "10k_cny",
        "factor_volume_unit": "share",
        "factor_amount_unit": "cny",
        "volume_multiplier": MONEYFLOW_VOLUME_MULTIPLIER,
        "amount_multiplier": MONEYFLOW_AMOUNT_MULTIPLIER,
        "total_net_source": "tushare.net_mf_vol/net_mf_amount",
    }


def normalize_tushare_moneyflow_units(
    frame: pd.DataFrame,
    *,
    copy: bool = True,
    require_all: bool = True,
) -> pd.DataFrame:
    """Convert a raw Tushare/DB moneyflow frame from hand/10k-CNY to share/CNY.

    Call this exactly once at the DB boundary, before renaming source columns.
    The explicit function name avoids unsafe magnitude-based unit guessing.
    """

    out = frame.copy() if copy else frame
    if out.empty:
        out.attrs["moneyflow_unit_contract"] = MONEYFLOW_UNIT_CONTRACT_VERSION
        return out

    required = set(TUSHARE_MONEYFLOW_VOLUME_COLUMNS + TUSHARE_MONEYFLOW_AMOUNT_COLUMNS)
    missing = sorted(required.difference(out.columns))
    if require_all and missing:
        raise ValueError(f"moneyflow source fields missing: {missing}")

    for column in TUSHARE_MONEYFLOW_VOLUME_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce") * MONEYFLOW_VOLUME_MULTIPLIER
    for column in TUSHARE_MONEYFLOW_AMOUNT_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce") * MONEYFLOW_AMOUNT_MULTIPLIER

    out.attrs["moneyflow_unit_contract"] = MONEYFLOW_UNIT_CONTRACT_VERSION
    return out


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.mask(denominator == 0, np.nan)


def _rolling_sum_by_instrument(series: pd.Series, window: int) -> pd.Series:
    if series.empty:
        return series
    return (
        series.groupby(level="instrument")
        .rolling(window=window, min_periods=window)
        .sum()
        .reset_index(level=0, drop=True)
    )


def _daily_amount_and_raw_volume(df_pv: pd.DataFrame, index: pd.Index) -> tuple[pd.Series, pd.Series]:
    amount_col = "amount" if "amount" in df_pv.columns else "$amount" if "$amount" in df_pv.columns else None
    volume_col = "volume" if "volume" in df_pv.columns else "$volume" if "$volume" in df_pv.columns else None
    factor_col = "factor" if "factor" in df_pv.columns else "$factor" if "$factor" in df_pv.columns else None
    if amount_col is None or volume_col is None or factor_col is None:
        raise ValueError("daily price frame must contain amount, volume and factor for moneyflow ratios")

    amount = pd.to_numeric(df_pv[amount_col], errors="coerce").reindex(index).astype("float64")
    adjusted_volume = pd.to_numeric(df_pv[volume_col], errors="coerce").reindex(index).astype("float64")
    qfq_factor = pd.to_numeric(df_pv[factor_col], errors="coerce").reindex(index).astype("float64")

    # daily_pv volume is qfq-adjusted (raw shares / factor). Moneyflow volumes
    # are unadjusted traded shares, so ratios must use raw shares.
    raw_volume = adjusted_volume * qfq_factor
    return amount.mask(amount == 0, np.nan), raw_volume.mask(raw_volume == 0, np.nan)


def derive_moneyflow_factors(df_mf: pd.DataFrame, df_pv: pd.DataFrame) -> pd.DataFrame:
    """Derive all stable ``mf_*`` fields under the canonical share/CNY contract."""

    if df_mf is None or df_mf.empty:
        return pd.DataFrame()

    missing = sorted(set(MONEYFLOW_FACTOR_COLUMNS).difference(df_mf.columns))
    if missing:
        raise ValueError(f"canonical moneyflow fields missing: {missing}")

    df = df_mf.sort_index()
    amount, raw_volume = _daily_amount_and_raw_volume(df_pv, df.index)

    total_net_amt = pd.to_numeric(df["mf_net_amt"], errors="coerce").astype("float64")
    total_net_vol = pd.to_numeric(df["mf_net_vol"], errors="coerce").astype("float64")
    main_net_amt = (
        df["mf_lg_buy_amt"] + df["mf_elg_buy_amt"]
        - df["mf_lg_sell_amt"] - df["mf_elg_sell_amt"]
    ).astype("float64")
    main_net_vol = (
        df["mf_lg_buy_vol"] + df["mf_elg_buy_vol"]
        - df["mf_lg_sell_vol"] - df["mf_elg_sell_vol"]
    ).astype("float64")
    elg_net_amt = (df["mf_elg_buy_amt"] - df["mf_elg_sell_amt"]).astype("float64")
    elg_net_vol = (df["mf_elg_buy_vol"] - df["mf_elg_sell_vol"]).astype("float64")

    out = pd.DataFrame(index=df.index)
    out["mf_total_net_amt"] = total_net_amt
    out["mf_total_net_vol"] = total_net_vol
    out["mf_total_net_amt_ratio"] = _safe_div(total_net_amt, amount)
    out["mf_total_net_vol_ratio"] = _safe_div(total_net_vol, raw_volume)
    out["mf_main_net_amt"] = main_net_amt
    out["mf_main_net_vol"] = main_net_vol
    out["mf_main_net_amt_ratio"] = _safe_div(main_net_amt, amount)
    out["mf_main_net_vol_ratio"] = _safe_div(main_net_vol, raw_volume)
    out["mf_elg_net_amt"] = elg_net_amt
    out["mf_elg_net_vol"] = elg_net_vol
    out["mf_elg_net_amt_ratio"] = _safe_div(elg_net_amt, amount)
    out["mf_elg_net_vol_ratio"] = _safe_div(elg_net_vol, raw_volume)
    out["mf_elg_share_in_main_amt"] = _safe_div(elg_net_amt, main_net_amt)
    out["mf_elg_share_in_main_vol"] = _safe_div(elg_net_vol, main_net_vol)

    for window in (5, 20):
        out[f"mf_total_net_amt_{window}d"] = _rolling_sum_by_instrument(total_net_amt, window)
        out[f"mf_main_net_amt_{window}d"] = _rolling_sum_by_instrument(main_net_amt, window)
        out[f"mf_elg_net_amt_{window}d"] = _rolling_sum_by_instrument(elg_net_amt, window)
        amount_window = _rolling_sum_by_instrument(amount, window)
        out[f"mf_total_net_amt_ratio_{window}d"] = _safe_div(
            out[f"mf_total_net_amt_{window}d"], amount_window
        )
        out[f"mf_main_net_amt_ratio_{window}d"] = _safe_div(
            out[f"mf_main_net_amt_{window}d"], amount_window
        )
        out[f"mf_elg_net_amt_ratio_{window}d"] = _safe_div(
            out[f"mf_elg_net_amt_{window}d"], amount_window
        )

    out.attrs["moneyflow_unit_contract"] = MONEYFLOW_UNIT_CONTRACT_VERSION
    return out.sort_index()


def assert_moneyflow_frame_parity(
    moneyflow: pd.DataFrame,
    static_factors: pd.DataFrame,
    *,
    rtol: float = 2e-6,
    atol: float = 1e-3,
) -> None:
    """Fail when raw moneyflow columns differ between H5 and static factors."""

    missing_moneyflow = sorted(set(MONEYFLOW_FACTOR_COLUMNS).difference(moneyflow.columns))
    missing_static = sorted(set(MONEYFLOW_FACTOR_COLUMNS).difference(static_factors.columns))
    if missing_moneyflow or missing_static:
        raise ValueError(
            f"moneyflow parity fields missing: h5={missing_moneyflow}, static={missing_static}"
        )
    common = moneyflow.index.intersection(static_factors.index)
    if common.empty:
        raise ValueError("moneyflow H5 and static factors have no common index")
    # Compare one column at a time. Full bundles contain millions of rows, so
    # materializing two 18-column float64 copies would create a multi-GB spike.
    failures: dict[str, float] = {}
    for column in MONEYFLOW_FACTOR_COLUMNS:
        left = pd.to_numeric(moneyflow[column].reindex(common), errors="coerce").to_numpy()
        right = pd.to_numeric(static_factors[column].reindex(common), errors="coerce").to_numpy()
        if not np.allclose(left, right, rtol=rtol, atol=atol, equal_nan=True):
            finite = np.isfinite(left) & np.isfinite(right)
            failures[column] = float(np.max(np.abs(left[finite] - right[finite]))) if finite.any() else float("inf")
    if failures:
        worst = dict(sorted(failures.items(), key=lambda item: item[1], reverse=True)[:5])
        raise ValueError(f"moneyflow H5/static unit parity failed: max_abs_delta={worst}")
