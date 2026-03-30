import qlib
from qlib.data import D
qlib.init(provider_uri="/mnt/f/Dev/AIstock/qlib_minute_full/bin", region="cn")
df = D.features(["000430.sz"], ["$close","$volume","$limit_up","$limit_down"],
                start_time="2025-12-24", end_time="2025-12-27", freq="1min")
print("000430.sz 2025-12-24~27:")
print("  shape:", df.shape)
df2 = df.reset_index()
df2["date"] = df2["datetime"].dt.date
for d, g in df2.groupby("date"):
    cn = int(g["$close"].isna().sum())
    lu = int(g["$limit_up"].sum())
    ld = int(g["$limit_down"].sum())
    cv = g["$close"].dropna()
    cstr = "ALL NaN" if len(cv)==0 else "%.2f~%.2f" % (cv.min(), cv.max())
    print("  %s: %d bars, close_nan=%d, lu=%d, ld=%d, close=%s" % (d, len(g), cn, lu, ld, cstr))
