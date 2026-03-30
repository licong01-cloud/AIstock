"""在 WSL 中测试 Qlib 读取 limit_up / limit_down 字段"""
import qlib
from qlib.data import D

QLIB_DIR = '/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20260311'

qlib.init(provider_uri=QLIB_DIR, region='cn')
print('Qlib initialized.')

# 测试读取
stocks = ['000001.SZ', '000006.SZ', '300001.SZ', '688001.SH']
fields = ['$limit_up', '$limit_down', '$close']
df = D.features(stocks, fields, freq='day')

print(f'Shape: {df.shape}')
print(f'Columns: {df.columns.tolist()}')
print(f'limit_up unique values: {sorted(df["$limit_up"].dropna().unique())}')

print()
for code in stocks:
    sub = df.loc[code]
    lu = (sub['$limit_up'] == 1.0).sum()
    ld = (sub['$limit_down'] == 1.0).sum()
    nan_lu = sub['$limit_up'].isna().sum()
    print(f'  {code}: limit_up=1共{lu}天, limit_down=1共{ld}天, NaN={nan_lu}天')

print()
print('Sample (000006.SZ, limit_up=1 days):')
sub6 = df.loc['000006.SZ']
print(sub6[sub6['$limit_up'] == 1.0][['$close', '$limit_up', '$limit_down']].head(10).to_string())

print()
print('Qlib D.features test PASSED')
