#!/usr/bin/env python3
"""
Validate 10-min H5 export: small scale test with 10 stocks × 3 days.
Run: conda activate rdagent-gpu && python validate_10min_h5.py
"""

import numpy as np
import pandas as pd
import sys

sys.path.insert(0, "/mnt/f/Dev/AIstock/scripts")
from export_10min_h5 import resample_batch_1min_to_10min

def main():
    import qlib
    from qlib.data import D

    qlib.init(provider_uri="/home/lc999/data/qlib_minute_bin")

    test_stocks = ["000001.SZ", "000002.SZ", "600519.SH", "300750.SZ", "601318.SH",
                   "000858.SZ", "002714.SZ", "600036.SH", "000333.SZ", "601012.SH"]

    fields = ["$open", "$close", "$high", "$low", "$volume", "$amount"]

    # ── Step 1: Read 1min reference data for verification ──
    print("=" * 60)
    print("Step 1: Load 1min reference data (10 stocks, 3 days)")
    print("=" * 60)

    df_1min = D.features(test_stocks, fields,
                         start_time="2024-01-02", end_time="2024-01-05", freq="1min")
    df_1min.columns = [c.lstrip("$") for c in df_1min.columns]
    print(f"1min data shape: {df_1min.shape}")

    # ── Step 2: Resample ──
    print("\n" + "=" * 60)
    print("Step 2: Resample to 10min")
    print("=" * 60)

    df_10min = resample_batch_1min_to_10min(df_1min)
    print(f"10min data shape: {df_10min.shape}")

    if df_10min.empty:
        print("ERROR: Empty result!")
        return

    # Compute vwap and returns
    df_10min["vwap"] = (df_10min["amount"] / df_10min["volume"]).replace([np.inf, -np.inf], np.nan)
    df_10min["returns"] = df_10min.groupby(level="instrument")["close"].pct_change()

    # ── Step 3: Verify bar count per stock per day ──
    print("\n" + "=" * 60)
    print("Check 1: Bars per stock per day (expect 24)")
    print("=" * 60)

    dt = df_10min.index.get_level_values("datetime")
    dates = dt.normalize().unique()
    instruments = df_10min.index.get_level_values("instrument").unique()

    for d in dates:
        day_data = df_10min.loc[d:d + pd.Timedelta(days=1)]
        for inst in instruments[:3]:  # check first 3 stocks
            try:
                stock_day = day_data.xs(inst, level="instrument")
                n_bars = len(stock_day)
                status = "OK" if n_bars == 24 else f"FAIL ({n_bars})"
                if inst == instruments[0]:
                    print(f"  {d.date()}: {inst} = {n_bars} bars {status}")
            except KeyError:
                print(f"  {d.date()}: {inst} = MISSING")

    # ── Step 4: Verify timestamps ──
    print("\n" + "=" * 60)
    print("Check 2: Expected 10min timestamps")
    print("=" * 60)

    expected_am = ["09:40", "09:50", "10:00", "10:10", "10:20", "10:30",
                   "10:40", "10:50", "11:00", "11:10", "11:20", "11:30"]
    expected_pm = ["13:10", "13:20", "13:30", "13:40", "13:50", "14:00",
                   "14:10", "14:20", "14:30", "14:40", "14:50", "15:00"]
    expected_all = expected_am + expected_pm

    first_date = dates[0]
    sample_stock = instruments[0]
    day_data = df_10min.xs(sample_stock, level="instrument").loc[first_date:first_date + pd.Timedelta(hours=16)]
    actual_times = day_data.index.strftime("%H:%M").tolist()

    print(f"  Stock: {sample_stock}, Date: {first_date.date()}")
    print(f"  Expected ({len(expected_all)}): {expected_all}")
    print(f"  Actual   ({len(actual_times)}): {actual_times}")
    match = actual_times == expected_all
    print(f"  Match: {'YES' if match else 'NO'}")

    # ── Step 5: OHLC consistency ──
    print("\n" + "=" * 60)
    print("Check 3: OHLC consistency (low <= open/close <= high)")
    print("=" * 60)

    violations = 0
    for col_pair in [("low", "open"), ("low", "close"), ("open", "high"), ("close", "high")]:
        lo, hi = col_pair
        bad = ((df_10min[lo] > df_10min[hi]) & df_10min[lo].notna() & df_10min[hi].notna()).sum()
        if bad > 0:
            print(f"  VIOLATION: {lo} > {hi} in {bad} rows")
            violations += bad
    if violations == 0:
        print("  All OK (0 violations)")

    # ── Step 6: Cross-check with 1min data ──
    print("\n" + "=" * 60)
    print("Check 4: Cross-check OHLCV against 1min source")
    print("=" * 60)

    # For 000001.SZ on first day, manually verify first 10min bar
    stock = "000001.SZ"
    day = pd.Timestamp("2024-01-02")
    df_1min_swapped = df_1min.swaplevel().sort_index() if df_1min.index.names == ["instrument", "datetime"] else df_1min

    # First 10min bar: 09:31-09:40
    mask_1min = df_1min_swapped.index.get_level_values("instrument") == stock
    stock_1min = df_1min_swapped[mask_1min].copy()
    stock_1min = stock_1min[stock_1min.index.get_level_values("datetime").strftime("%H:%M") != "09:30"]

    # Morning day 1 bars: 09:31-09:40
    day1_1min = stock_1min.loc[
        (stock_1min.index.get_level_values("datetime") >= pd.Timestamp("2024-01-02 09:31")) &
        (stock_1min.index.get_level_values("datetime") <= pd.Timestamp("2024-01-02 09:40"))
    ]

    if not day1_1min.empty:
        expected_open = day1_1min["open"].iloc[0]
        expected_high = day1_1min["high"].max()
        expected_low = day1_1min["low"].min()
        expected_close = day1_1min["close"].iloc[-1]
        expected_vol = day1_1min["volume"].sum()
        expected_amt = day1_1min["amount"].sum()

        # Get 10min bar
        ten_bar = df_10min.xs(stock, level="instrument").loc[pd.Timestamp("2024-01-02 09:40")]

        print(f"  Stock: {stock}, 10min bar 09:40 (from 1min bars 09:31-09:40)")
        print(f"  {'Field':<10} {'1min aggregate':>16} {'10min bar':>16} {'Match':>8}")
        for field_name, exp_val in [("open", expected_open), ("high", expected_high),
                                     ("low", expected_low), ("close", expected_close)]:
            act_val = ten_bar[field_name]
            match = abs(exp_val - act_val) < 1e-4
            print(f"  {field_name:<10} {exp_val:>16.4f} {act_val:>16.4f} {'OK' if match else 'FAIL':>8}")

        vol_match = abs(expected_vol - ten_bar["volume"]) / max(abs(expected_vol), 1) < 0.01
        amt_match = abs(expected_amt - ten_bar["amount"]) / max(abs(expected_amt), 1) < 0.01
        print(f"  {'volume':<10} {expected_vol:>16.2f} {ten_bar['volume']:>16.2f} {'OK' if vol_match else 'FAIL':>8}")
        print(f"  {'amount':<10} {expected_amt:>16.2f} {ten_bar['amount']:>16.2f} {'OK' if amt_match else 'FAIL':>8}")

    # ── Step 7: VWAP sanity ──
    print("\n" + "=" * 60)
    print("Check 5: VWAP sanity (should be near close price)")
    print("=" * 60)

    valid = df_10min[df_10min["vwap"].notna() & df_10min["close"].notna()]
    ratio = (valid["vwap"] / valid["close"]).describe()
    print(f"  VWAP/Close ratio statistics:")
    print(f"  {ratio}")

    # ── Step 8: Returns ──
    print("\n" + "=" * 60)
    print("Check 6: Returns (first bar of each day should be NaN for each stock)")
    print("=" * 60)

    sample = df_10min.xs("000001.SZ", level="instrument").head(30)
    print(f"  000001.SZ first 30 rows of returns:")
    for idx, row in sample.iterrows():
        ret = row["returns"]
        print(f"    {idx}  close={row['close']:.4f}  returns={'NaN' if pd.isna(ret) else f'{ret:.6f}'}")

    print("\n" + "=" * 60)
    print("ALL CHECKS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
