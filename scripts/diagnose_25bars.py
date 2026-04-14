"""Diagnose why some days have 25 bars instead of 24"""
import pandas as pd
import numpy as np
import qlib

qlib.init(provider_uri="/home/lc999/data/qlib_minute_bin")
from qlib.data import D

# Check 1min bars on a problematic day (2026-03-18)
fields = ["$open", "$close"]
stock = "000001.SZ"
df = D.features([stock], fields, start_time="2026-03-18", end_time="2026-03-19", freq="1min")
df.columns = [c.lstrip("$") for c in df.columns]

# Swap to (datetime, instrument)
if df.index.names == ["instrument", "datetime"]:
    df = df.swaplevel().sort_index()

dt = df.index.get_level_values("datetime")
print(f"Total 1min bars for {stock} on 2026-03-18: {len(df)}")

# Show bars around boundaries
print("\nMorning start:")
morning_start = dt.strftime("%H:%M").isin(["09:29", "09:30", "09:31", "09:32"])
print(df[morning_start].index.strftime("%H:%M").tolist()[:5])

print("\nMorning end:")
morning_end = dt.strftime("%H:%M").isin(["11:28", "11:29", "11:30", "11:31"])
print(df[morning_end].index.strftime("%H:%M").tolist())

print("\nAfternoon start:")
pm_start = (dt.hour == 13) & (dt.minute <= 5)
print(df[pm_start].index.strftime("%H:%M").tolist())

print("\nAfternoon end:")
pm_end = (dt.hour >= 14) & (dt.hour <= 15)
print(df[pm_end].index.strftime("%H:%M").tolist()[-5:])

# Count morning/afternoon bars
am = dt.strftime("%H:%M").between("09:30", "11:30")
pm = dt.strftime("%H:%M").between("13:00", "15:00")
print(f"\nMorning bars (09:30-11:30): {am.sum()}")
print(f"Afternoon bars (13:00-15:00): {pm.sum()}")

# Check if 13:00 bar exists
has_1300 = dt.strftime("%H:%M").eq("13:00").any()
print(f"Has 13:00 bar: {has_1300}")

# Now check a good day (2024-01-02)
df2 = D.features([stock], fields, start_time="2024-01-02", end_time="2024-01-03", freq="1min")
df2.columns = [c.lstrip("$") for c in df2.columns]
if df2.index.names == ["instrument", "datetime"]:
    df2 = df2.swaplevel().sort_index()
dt2 = df2.index.get_level_values("datetime")
am2 = dt2.strftime("%H:%M").between("09:30", "11:30")
pm2 = dt2.strftime("%H:%M").between("13:00", "15:00")
has_1300_2 = dt2.strftime("%H:%M").eq("13:00").any()
print(f"\n2024-01-02: morning={am2.sum()}, afternoon={pm2.sum()}, has 13:00={has_1300_2}")

# Check 2026-03-19 (last calendar day)
df3 = D.features([stock], fields, start_time="2026-03-19", end_time="2026-03-20", freq="1min")
print(f"\n2026-03-19 data exists: {not df3.empty}, shape: {df3.shape}")
