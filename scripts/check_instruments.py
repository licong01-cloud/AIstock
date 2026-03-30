import qlib
from qlib.data import D

qlib.init(provider_uri='/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20260311', region='cn')
inst = D.list_instruments(D.instruments('all'), as_list=True)
print('Total instruments:', len(inst))
print('Sample codes:', inst[:10])

# 测试用正确格式读取
df = D.features(inst[:4], ['$limit_up', '$limit_down', '$close'], freq='day')
print('Shape:', df.shape)
if not df.empty:
    print('Index levels:', df.index.names)
    # repaired truncated tail
