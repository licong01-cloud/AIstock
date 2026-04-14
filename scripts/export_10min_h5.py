#!/usr/bin/env python3
"""
Export 10-minute OHLCV+VWAP+Returns from Qlib 1-minute bin to HDF5.

Data source: /home/lc999/data/qlib_minute_bin/ (23GB, 5515 stocks)
Output:      /home/lc999/data/daily_pv_10min.h5

Format (aligned with daily_pv.h5):
  - HDF5 key: "data"
  - Index: MultiIndex(["datetime", "instrument"])
  - Columns: open, high, low, close, volume, amount, vwap, returns (float32)
  - 24 bars/day: 12 morning (09:40-11:30) + 12 afternoon (13:10-15:00)

Usage:
  conda activate rdagent-gpu
  python export_10min_h5.py [--batch-size 200] [--start 2024-01-02] [--end 2026-03-19]
"""

import argparse
import os
import sys
import time

import numpy as np
import pandas as pd

# ── Config ──────────────────────────────────────────────────────────────────
QLIB_PROVIDER = "/home/lc999/data/qlib_minute_bin"
OUTPUT_DIR = "/home/lc999/data"
FIELDS = ["$open", "$close", "$high", "$low", "$volume", "$amount"]

# ── Resampling logic ────────────────────────────────────────────────────────

def resample_batch_1min_to_10min(df_1min: pd.DataFrame) -> pd.DataFrame:
    """
    Resample a batch of 1-min OHLCV data to 10-min bars.

    Rules:
    - Drop 09:30 (集合竞价) bar
    - Morning: 09:31-11:30 = 120 bars → 12 × 10min bars (labels: 09:40, 09:50, ..., 11:30)
    - Afternoon: 13:01-15:00 = 120 bars → 12 × 10min bars (labels: 13:10, 13:20, ..., 15:00)
    - Position-based grouping (every 10 consecutive bars), not timestamp-based.
    """
    if df_1min.empty:
        return pd.DataFrame()

    # Reset index: original is MultiIndex(["datetime", "instrument"]) or (instrument, datetime)
    if df_1min.index.names == ["instrument", "datetime"]:
        df_1min = df_1min.swaplevel().sort_index()
    df = df_1min.reset_index()

    # Drop 09:30 bar
    mask = df["datetime"].dt.strftime("%H:%M") != "09:30"
    df = df[mask].copy()
    if df.empty:
        return pd.DataFrame()

    # Sort by instrument + datetime
    df = df.sort_values(["instrument", "datetime"]).reset_index(drop=True)

    # Assign session and date
    df["session"] = np.where(df["datetime"].dt.hour < 12, 0, 1)  # 0=am, 1=pm
    df["_date"] = df["datetime"].dt.date

    # Within each (instrument, date, session), assign position and 10min group
    df["_pos"] = df.groupby(["instrument", "_date", "session"]).cumcount()
    df["_grp"] = df["_pos"] // 10

    # Only keep first 12 groups per session (= 120 bars = 12 × 10min)
    # Some days have extra bars (e.g. 13:00 or >15:00) creating a 13th group
    df = df[df["_grp"] < 12]

    # Aggregate OHLCV
    agg_dict = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "amount": "sum",
    }
    grouped = df.groupby(["instrument", "_date", "session", "_grp"], sort=True).agg(agg_dict)

    if grouped.empty:
        return pd.DataFrame()

    # Compute 10min timestamp labels (vectorized)
    idx = grouped.index
    sessions = idx.get_level_values("session").astype(int).values
    groups = idx.get_level_values("_grp").astype(int).values
    dates = idx.get_level_values("_date")

    # Morning base: 09:40 (9*60+40=580 min from midnight)
    # Afternoon base: 13:10 (13*60+10=790 min from midnight)
    base_minutes = np.where(sessions == 0, 580, 790)
    total_minutes = base_minutes + groups * 10
    hours = total_minutes // 60
    minutes = total_minutes % 60

    timestamps = pd.to_datetime(dates) + pd.to_timedelta(hours * 3600 + minutes * 60, unit="s")

    # Build result DataFrame
    result = grouped.reset_index(drop=True)
    result["datetime"] = timestamps
    result["instrument"] = idx.get_level_values("instrument").values

    # Set MultiIndex(datetime, instrument)
    result = result.set_index(["datetime", "instrument"]).sort_index()
    return result[["open", "high", "low", "close", "volume", "amount"]]


def main():
    parser = argparse.ArgumentParser(description="Export 10-min OHLCV H5 from Qlib 1-min bin")
    parser.add_argument("--batch-size", type=int, default=200, help="Stocks per batch")
    parser.add_argument("--start", default="2024-01-02", help="Start date")
    parser.add_argument("--end", default="2026-03-19", help="End date")
    parser.add_argument("--output", default=None, help="Output H5 path")
    parser.add_argument("--max-stocks", type=int, default=None, help="Limit stock count (for testing)")
    args = parser.parse_args()

    output_path = args.output or os.path.join(OUTPUT_DIR, "daily_pv_10min.h5")

    # Init Qlib
    import qlib
    from qlib.data import D

    qlib.init(provider_uri=QLIB_PROVIDER)

    # Read instrument list from file (bypass Qlib instrument loader
    # which requires day-frequency instruments that don't exist for minute data)
    inst_file = os.path.join(QLIB_PROVIDER, "instruments", "all.txt")
    all_instruments = pd.read_csv(inst_file, sep="\t", header=None)[0].tolist()
    if args.max_stocks:
        all_instruments = all_instruments[:args.max_stocks]
    print(f"Instruments: {len(all_instruments)}, Date range: {args.start} ~ {args.end}")
    print(f"Batch size: {args.batch_size}, Output: {output_path}")

    all_frames = []
    t0 = time.time()
    total_batches = (len(all_instruments) + args.batch_size - 1) // args.batch_size

    for i in range(0, len(all_instruments), args.batch_size):
        batch = all_instruments[i : i + args.batch_size]
        batch_idx = i // args.batch_size + 1

        # Read 1min data from Qlib
        df_1min = D.features(batch, FIELDS, start_time=args.start, end_time=args.end, freq="1min")
        if df_1min.empty:
            print(f"  Batch {batch_idx}/{total_batches}: empty, skipped")
            continue

        # Rename columns (strip $)
        df_1min.columns = [c.lstrip("$") for c in df_1min.columns]

        # Resample to 10min
        df_10min = resample_batch_1min_to_10min(df_1min)
        if df_10min.empty:
            print(f"  Batch {batch_idx}/{total_batches}: empty after resample, skipped")
            continue

        all_frames.append(df_10min)
        elapsed = time.time() - t0
        print(f"  Batch {batch_idx}/{total_batches}: {len(batch)} stocks, "
              f"1min={df_1min.shape[0]:,} rows → 10min={df_10min.shape[0]:,} rows  "
              f"({elapsed:.1f}s)")

    if not all_frames:
        print("ERROR: No data produced!")
        sys.exit(1)

    # Concat all batches
    print("Concatenating all batches...")
    result = pd.concat(all_frames).sort_index()
    del all_frames

    # Compute derived fields
    print("Computing vwap and returns...")
    result["vwap"] = (result["amount"] / result["volume"]).replace([np.inf, -np.inf], np.nan)
    result["returns"] = result.groupby(level="instrument")["close"].pct_change()

    # Cast to float32
    result = result.astype(np.float32)

    # Write H5
    print(f"Writing {output_path} ...")
    result.to_hdf(output_path, key="data", mode="w")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Shape: {result.shape}")
    print(f"Columns: {list(result.columns)}")
    instruments = result.index.get_level_values("instrument").unique()
    datetimes = result.index.get_level_values("datetime")
    print(f"Instruments: {len(instruments)}")
    print(f"Date range: {datetimes.min()} ~ {datetimes.max()}")

    # Quick sanity check: bars per day
    sample_dates = datetimes.normalize().unique()[:3]
    for d in sample_dates:
        day_mask = datetimes.normalize() == d
        n_bars = day_mask.sum() / len(instruments)
        print(f"  {d.date()}: {n_bars:.0f} bars/stock")

    print(f"\nFile size: {os.path.getsize(output_path) / 1e9:.2f} GB")


if __name__ == "__main__":
    main()
