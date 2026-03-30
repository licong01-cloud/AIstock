"""端到端验证：分钟线导出含 limit_up/limit_down，使用一字涨停股 000004.SZ"""
import os, sys, warnings
warnings.filterwarnings('ignore')
os.environ.setdefault('TDX_DB_PASSWORD', 'lc78080808')
sys.path.insert(0, 'F:/Dev/AIstock')

from backend.qlib_exporter.db_reader import DBReader
from datetime import date

reader = DBReader()
df = reader.load_qlib_minute_data(
    ts_codes=['000004.SZ', '000001.SZ'],
    start=date(2026, 3, 10),
    end=date(2026, 3, 14),
    freq='1m'
)

print(f'Shape: {df.shape}')
print(f'Columns: {list(df.columns)}')

# 000004.SZ 的 limit_up 统计
df004 = df.xs('000004.SZ', level='instrument')
print(f'\n=== 000004.SZ ({len(df004)} bars) ===')
lu = df004['$limit_up']
ld = df004['$limit_down']
print(f'$limit_up:   1.0={int((lu==1.0).sum())}, 0.0={int((lu==0.0).sum())}, NaN={int(lu.isna().sum())}')
print(f'$limit_down: 1.0={int((ld==1.0).sum())}, 0.0={int((ld==0.0).sum())}, NaN={int(ld.isna().sum())}')

# 按日统计
import pandas as pd
df004r = df004.reset_index()
df004r['date'] = df004r['datetime'].dt.date
daily = df004r.groupby('date').agg(
    bars=('$close', 'count'),
    limit_up_bars=('$limit_up', lambda x: (x==1.0).sum()),
    limit_down_bars=('$limit_down', lambda x: (x==1.0).sum()),
    close_min=('$close', 'min'),
    close_max=('$close', 'max'),
)
print('\n按日统计:')
print(daily.to_string())

# 000001.SZ 验证（应该全部 0）
df001 = df.xs('000001.SZ', level='instrument')
print(f'\n=== 000001.SZ ({len(df001)} bars) ===')
lu1 = df001['$limit_up']
ld1 = df001['$limit_down']
print(f'$limit_up:   1.0={int((lu1==1.0).sum())}, 0.0={int((lu1==0.0).sum())}')
print(f'$limit_down: 1.0={int((ld1==1.0).sum())}, 0.0={int((ld1==0.0).sum())}')

print('\n验证完成')
