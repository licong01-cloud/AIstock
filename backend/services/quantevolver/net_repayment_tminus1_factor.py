"""Canonical source for the existing causal T-1 net-repayment factor.

Raw release H5 is indexed by source date; the official memory input is already
indexed by next-trading-day availability. Both must delay the source once.
This file can be stored unchanged as catalog code_text; it has no DB writer.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

FACTOR_NAME = "m_md_net_repay_rate_10d_tminus1"
AVAILABILITY_KEY = "aistock_margin_availability"
PROJECTED_BASIS = "next_trade_decision_date_v1"


def calculate(frame: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    basis = frame.attrs.get(AVAILABILITY_KEY, "source_date")
    if basis not in {"source_date", PROJECTED_BASIS}:
        raise ValueError(f"Unknown margin input availability: {basis!r}")
    if not isinstance(frame.index, pd.MultiIndex):
        raise ValueError("margin_detail index must be datetime,instrument")
    if list(frame.index.names) != ["datetime", "instrument"]:
        frame = frame.reorder_levels(["datetime", "instrument"]).sort_index()
    if frame.index.has_duplicates:
        raise ValueError("margin_detail contains duplicate rows")
    calendar = pd.DatetimeIndex(calendar)
    if calendar.empty or calendar.hasnans or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise ValueError("calendar must be non-empty, valid, ordered and unique")
    if not frame.index.get_level_values("datetime").isin(calendar).all():
        raise ValueError("margin_detail dates outside trading calendar")
    required = {"md_rzche", "md_rzmre", "md_rzye"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"margin_detail missing fields: {sorted(missing)}")
    rate = (frame["md_rzche"] - frame["md_rzmre"]) / frame["md_rzye"].replace(0, np.nan)
    wide = rate.unstack("instrument").sort_index().reindex(calendar)
    values = wide.rolling(10, min_periods=5).mean()
    if basis == "source_date":
        values = values.shift(1)
    values = values.where(np.isfinite(values))
    result = values.stack().rename(FACTOR_NAME).to_frame().sort_index()
    result.index.names = ["datetime", "instrument"]
    return result


def _calendar_from_daily_pv(data_dir: Path) -> pd.DatetimeIndex:
    daily = pd.read_hdf(data_dir / "daily_pv.h5")
    if not isinstance(daily.index, pd.MultiIndex) or "datetime" not in daily.index.names:
        raise ValueError("daily_pv index must contain datetime")
    return pd.DatetimeIndex(daily.index.get_level_values("datetime").unique()).sort_values()


def compute_factor() -> None:
    data_dir = Path(__file__).resolve().parent.parent
    frame = pd.read_hdf(data_dir / "margin_detail.h5")
    frame = frame[["md_rzche", "md_rzmre", "md_rzye"]]
    result = calculate(frame.sort_index(), _calendar_from_daily_pv(data_dir))
    if result.empty:
        raise ValueError("causal factor has no finite values")
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")


def research_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--instruments", required=True)
    args = parser.parse_args()
    instruments = sorted(set(json.loads(args.instruments)))
    if not instruments:
        raise ValueError("instruments cannot be empty")
    calendar_path = args.data_dir.parent / "daily_bin_candidate" / "calendars" / "day.txt"
    calendar = pd.DatetimeIndex(pd.to_datetime(calendar_path.read_text(encoding="utf-8").splitlines()))
    calendar = calendar[(calendar >= args.start_date) & (calendar <= args.end_date)]
    where = [f"datetime >= '{args.start_date}'", f"datetime <= '{args.end_date}'",
             f"instrument in {instruments!r}"]
    with pd.HDFStore(args.data_dir / "margin_detail.h5", mode="r") as store:
        frame = store.select("data", where=where, columns=["md_rzche", "md_rzmre", "md_rzye"])
    result = calculate(frame.sort_index(), calendar)
    if result.empty:
        raise ValueError("causal candidate has no finite values")
    if args.output.exists():
        raise FileExistsError(f"output already exists: {args.output}")
    result.to_hdf(args.output, key="data", mode="w", format="fixed")


if __name__ == "__main__":
    import sys

    if "--data-dir" in sys.argv:
        research_main()
    else:
        compute_factor()
