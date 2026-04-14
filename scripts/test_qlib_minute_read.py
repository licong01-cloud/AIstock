"""Test reading Qlib minute bin data"""
import qlib
qlib.init(provider_uri="/home/lc999/data/qlib_minute_bin")
from qlib.data import D

fields = ["$open", "$close", "$high", "$low", "$volume", "$amount"]
df = D.features(["000001.SZ"], fields, start_time="2024-01-02", end_time="2024-01-05", freq="1min")
print("shape:", df.shape)
print("dtypes:")
print(df.dtypes)
print("head(5):")
print(df.head(5))
print("tail(5):")
print(df.tail(5))
print("---")
print("index names:", df.index.names)
print("index levels:", df.index.nlevels)
