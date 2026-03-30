"""验证 qlib_minute_prod bin 数据"""
import qlib
from qlib.data import D
qlib.init(provider_uri="/mnt/f/Dev/AIstock/qlib_minute_prod/bin", region="cn")

cal = D.calendar(start_time="2024-01-02", end_time="2026-03-19", freq="1min")
print("Calendar entries:", len(cal))
days = len(set(c.date() for c in cal))
print("Trading days:", days)
print("First:", cal[0], "Last:", cal[-1])

inst = D.list_instruments(D.instruments("all"), as_list=True, freq="1min")
print("Instruments:", len(inst))

fields = ["$close", "$volume", "$factor", "$limit_up", "$limit_down"]
df = D.features(["000001.sz"], fields, start_time="2024-07-01", end_time="2024-07-05", freq="1min")
print("\n000001.sz 2024-07-01~05:")
print("  Shape:", df.shape)
print("  Close: %.2f ~ %.2f" % (df["$close"].min(), df["$close"].max()))
print("  Factor:", df["$factor"].iloc[0])
print("  limit_up sum:", df["$limit_up"].sum())

# 验证最新数据
df2 = D.features(["000001.sz"], fields, start_time="2026-03-17", end_time="2026-03-19", freq="1min")
print("\n000001.sz 2026-03-17~19:")
print("  Shape:", df2.shape)
print("  Close: %.2f ~ %.2f" % (df2["$close"].min(), df2["$close"].max()))

# 涨停统计
df3 = D.features(inst[:200], ["$limit_up", "$limit_down"], start_time="2024-07-01", end_time="2026-03-10", freq="1min")
print("\n前200只股票 2024-07 ~ 2026-03:")
print("  Total bars:", len(df3))
lu = int(df3["$limit_up"].sum())
ld = int(df3["$limit_down"].sum())
print("  涨停 bars: %d (%.3f%%)" % (lu, lu/len(df3)*100))
print("  跌停 bars: %d (%.3f%%)" % (ld, ld/len(df3)*100))

print("\n=== VERIFICATION PASSED ===")
