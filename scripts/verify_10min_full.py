"""Quick verify the full daily_pv_10min.h5"""
import pandas as pd
import numpy as np

df = pd.read_hdf("/home/lc999/data/daily_pv_10min.h5", key="data")
print("Shape:", df.shape)
print("Columns:", list(df.columns))

dt = df.index.get_level_values("datetime")
inst = df.index.get_level_values("instrument")
print(f"Date range: {dt.min()} ~ {dt.max()}")
print(f"Instruments: {len(inst.unique())}")

# Bar counts per day
dates = dt.normalize().unique()
print(f"\nTotal trading days: {len(dates)}")

# Check last 3 days
print("\nLast 3 days:")
for d in sorted(dates)[-3:]:
    day_mask = dt.normalize() == d
    day_count = day_mask.sum()
    n_inst = inst[day_mask].nunique()
    print(f"  {d.date()}: {day_count} bars, {n_inst} inst, {day_count/n_inst:.1f} bars/inst")

# Check first 3 days
print("\nFirst 3 days:")
for d in sorted(dates)[:3]:
    day_mask = dt.normalize() == d
    day_count = day_mask.sum()
    n_inst = inst[day_mask].nunique()
    print(f"  {d.date()}: {day_count} bars, {n_inst} inst, {day_count/n_inst:.1f} bars/inst")

# Check specific stock
sample = df.xs("000001.SZ", level="instrument")
d1 = sample.loc["2024-01-02":"2024-01-02 23:59"]
times = [t.strftime("%H:%M") for t in d1.index]
print(f"\n000001.SZ on 2024-01-02: {len(d1)} bars")
print(f"  Times: {times}")

# Bar count distribution
counts = df.groupby([dt.normalize(), inst]).size()
print(f"\nBars/inst/day stats:")
print(counts.describe())
print(f"\nValue counts of bars/inst/day:")
print(counts.value_counts().sort_index())
