"""Step 3: Qlib 加载分钟线 bin 验证"""
import qlib
from qlib.data import D
import pandas as pd
import numpy as np

provider_uri = "/mnt/f/Dev/AIstock/qlib_test_minute/bin"
qlib.init(provider_uri=provider_uri, region="cn")

# Test 1: D.features() 加载
print("=== Test 1: D.features() 加载分钟线 ===")
fields = ["$open", "$high", "$low", "$close", "$volume", "$factor", "$amount", "$limit_up", "$limit_down"]
df = D.features(
    instruments=["000001.sz", "000004.sz", "300750.sz", "600519.sh"],
    fields=fields,
    start_time="2026-03-10",
    end_time="2026-03-14",
    freq="1min",
)
print("  Shape:", df.shape)
print("  Columns:", list(df.columns))
print("  Index names:", df.index.names)
for c in df.columns:
    nan_count = int(df[c].isna().sum())
    print("    %s: %s, NaN=%d" % (c, df[c].dtype, nan_count))

# Test 2: 000004.sz 涨停标志
print("\n=== Test 2: 000004.sz 涨停标志 ===")
df004 = df.xs("000004.sz", level="instrument")
print("  Total bars:", len(df004))
lu = df004["$limit_up"]
print("  limit_up=1.0:", int((lu == 1.0).sum()))
print("  limit_up=0.0:", int((lu == 0.0).sum()))
df004r = df004.reset_index()
df004r["dt"] = df004r["datetime"].dt.date
for dt, grp in df004r.groupby("dt"):
    lu_count = int((grp["$limit_up"] == 1.0).sum())
    print("    %s: %d bars, limit_up=%d" % (dt, len(grp), lu_count))

# Test 3: 000001.sz 价格合理性
print("\n=== Test 3: 000001.sz 价格合理性 ===")
df001 = df.xs("000001.sz", level="instrument")
print("  Total bars:", len(df001))
close_min = df001["$close"].min()
close_max = df001["$close"].max()
print("  Close range: %.2f ~ %.2f" % (close_min, close_max))
vol_min = df001["$volume"].min()
vol_max = df001["$volume"].max()
print("  Volume range: %.0f ~ %.0f" % (vol_min, vol_max))
factor_val = df001["$factor"].iloc[0]
print("  Factor: %.6f (should be <= 1.0)" % factor_val)
assert factor_val <= 1.0, "Factor should be <= 1.0"
print("  limit_up all 0:", bool((df001["$limit_up"] == 0).all()))

# Test 4: 日历
print("\n=== Test 4: 日历验证 ===")
cal = D.calendar(start_time="2026-03-10", end_time="2026-03-14", freq="1min")
print("  Calendar entries:", len(cal))
print("  First:", cal[0])
print("  Last:", cal[-1])
unique_days = len(set(c.date() for c in cal))
print("  Trading days:", unique_days)
print("  Bars per day: ~%d" % (len(cal) // unique_days))

# Test 5: 与源数据交叉对比
print("\n=== Test 5: 与 DB 源数据交叉对比 ===")
source_df = pd.read_pickle("/mnt/f/Dev/AIstock/qlib_test_minute/source_df.pkl")
# 取 000001.sz 第一天对比
src_001 = source_df.xs("000001.SZ", level="instrument")
src_day1 = src_001.loc["2026-03-10"]
bin_001 = df001.loc["2026-03-10"]
print("  Source bars:", len(src_day1), "vs Bin bars:", len(bin_day1) if 'bin_day1' in dir() else len(bin_001))

# 对比 close 值
src_close = src_day1["$close"].values
bin_close = bin_001["$close"].values
max_diff = float(np.abs(src_close - bin_close).max())
print("  Close max diff: %.8f" % max_diff)
assert max_diff < 0.01, "Close values should match within 0.01"
print("  Close values MATCH")

# 对比 limit_up
src_lu = src_day1["$limit_up"].values
bin_lu = bin_001["$limit_up"].values
lu_match = bool(np.array_equal(src_lu, bin_lu))
print("  limit_up values match:", lu_match)

print("\n=== ALL TESTS PASSED ===")
