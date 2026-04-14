"""Diagnose 25 bars: check the actual 10min output"""
import pandas as pd
import numpy as np

df = pd.read_hdf("/home/lc999/data/daily_pv_10min.h5", key="data")

# Check a stock with 25 bars on 2026-03-18
stock = "000001.SZ"
sample = df.xs(stock, level="instrument")

# Show last few days
for date_str in ["2026-03-16", "2026-03-17", "2026-03-18"]:
    day_data = sample.loc[date_str:date_str + " 23:59"]
    times = [t.strftime("%H:%M") for t in day_data.index]
    print(f"{stock} {date_str}: {len(day_data)} bars")
    print(f"  {times}")

print("\n--- Good day ---")
day_data = sample.loc["2024-01-02":"2024-01-02 23:59"]
times = [t.strftime("%H:%M") for t in day_data.index]
print(f"{stock} 2024-01-02: {len(day_data)} bars")
print(f"  {times}")

# Check how many stocks have 25 bars on 2026-03-18
dt = df.index.get_level_values("datetime")
inst = df.index.get_level_values("instrument")
day_mask = dt.normalize() == pd.Timestamp("2026-03-18")
day_data = df[day_mask]
counts = day_data.groupby(level="instrument").size()
print(f"\n2026-03-18: {len(day_data)} total bars, {len(counts)} stocks")
print(f"  24 bars: {(counts == 24).sum()} stocks")
print(f"  25 bars: {(counts == 25).sum()} stocks")

# Show the extra timestamps for a 25-bar stock
stock_25 = counts[counts == 25].index[0]
s25 = df.xs(stock_25, level="instrument").loc["2026-03-18":"2026-03-18 23:59"]
times_25 = [t.strftime("%H:%M") for t in s25.index]
print(f"\n{stock_25} 2026-03-18 times ({len(s25)} bars): {times_25}")
