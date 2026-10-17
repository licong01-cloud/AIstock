"""Prepare supplemental PIT context from an explicit membership file and index H5.

Only preparation reads the exported membership intervals. Factor execution never
queries a database. No original release files are modified or re-exported.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

POOLS = {"csi300": (300, "000300.SH"), "csi500": (500, "000905.SH"), "csi1000": (1000, "000852.SH")}


def build_context(index_daily: pd.DataFrame, memberships: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    if not calendar.is_unique or not calendar.is_monotonic_increasing or calendar.hasnans:
        raise ValueError("sorted unique valid calendar required")
    required = {"pool_id", "index_code", "ts_code", "effective_from", "effective_to_exclusive"}
    if not required.issubset(memberships):
        raise ValueError("membership interval schema missing")
    quotes = index_daily.loc[index_daily.ts_code.isin([v[1] for v in POOLS.values()])].copy()
    quotes["trade_date"] = pd.to_datetime(quotes.trade_date, errors="raise")
    if quotes.duplicated(["trade_date", "ts_code"]).any():
        raise ValueError("duplicate index/date quote")
    closes = quotes.pivot(index="trade_date", columns="ts_code", values="close").reindex(calendar)
    if set(closes.columns) != {v[1] for v in POOLS.values()}:
        raise ValueError("required index missing")
    closes = closes.where(np.isfinite(closes) & (closes > 0))
    returns = {n: closes.pct_change(n, fill_method=None) for n in (1, 5, 20)}
    relative = returns[20].sub(returns[20]["000300.SH"], axis=0)
    acceleration = returns[5] / 5 - returns[20] / 20
    acceleration = acceleration.sub(acceleration["000300.SH"], axis=0)
    pieces = []
    for row in memberships.itertuples(index=False):
        if row.pool_id not in POOLS:
            continue
        pool, code = POOLS[row.pool_id]
        if row.index_code != code:
            raise ValueError("membership authority/index mismatch")
        start = pd.Timestamp(row.effective_from)
        end = pd.Timestamp(row.effective_to_exclusive) if pd.notna(row.effective_to_exclusive) else None
        if pd.isna(start) or (end is not None and (pd.isna(end) or end <= start)):
            raise ValueError("invalid half-open membership interval")
        positions = (calendar >= start) & ((calendar < end) if end is not None else True)
        days = calendar[positions]
        if len(days) == 0:
            continue
        piece = pd.DataFrame({
            "pool_id": pool,
            "relative_strength_20d": relative.loc[days, code].to_numpy(),
            "acceleration_5d_20d": acceleration.loc[days, code].to_numpy(),
            "index_return_1d": returns[1].loc[days, code].to_numpy(),
        }, index=pd.MultiIndex.from_product([days, [row.ts_code]], names=["datetime", "instrument"]))
        pieces.append(piece)
    if not pieces:
        raise ValueError("no PIT membership observations in requested window")
    result = pd.concat(pieces).sort_index()
    if result.index.has_duplicates:
        raise ValueError("overlapping size memberships; arbitrary bucket selection forbidden")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-h5", type=Path, required=True)
    parser.add_argument("--membership-parquet", type=Path, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    calendar = pd.DatetimeIndex(pd.to_datetime(args.calendar.read_text(encoding="utf-8").splitlines()))
    result = build_context(pd.read_hdf(args.index_h5), pd.read_parquet(args.membership_parquet), calendar)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / "index_factor_context.h5"
    with output.open("xb"):
        pass
    result.to_hdf(output, key="data", mode="w")
    print(json.dumps({"rows": len(result), "stocks": result.index.get_level_values("instrument").nunique(),
                      "first": str(calendar.min()), "last": str(calendar.max()), "output": str(output)}))


if __name__ == "__main__":
    main()
