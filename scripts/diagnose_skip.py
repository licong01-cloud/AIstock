"""诊断为何有股票被跳过"""
import os
from pathlib import Path
import psycopg2
import pandas as pd

DB_CONFIG = dict(host='127.0.0.1', port=5432, dbname='aistock',
                 user='postgres', password='lc78080808')

qlib_dir = Path('/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20260311')
features_dir = qlib_dir / 'features'

# 取数据库前50只股票
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("SELECT DISTINCT ts_code FROM market.stk_limit ORDER BY ts_code LIMIT 50")
db_stocks = [row[0] for row in cur.fetchall()]
conn.close()

print(f'DB前50只股票代码样例: {db_stocks[:10]}')

# 检查 qlib_bin 里有哪些目录
qlib_dirs = {d.name for d in features_dir.iterdir() if d.is_dir()}
print(f'\nQlib features 目录总数: {len(qlib_dirs)}')
print(f'Qlib目录样例: {sorted(qlib_dirs)[:10]}')

# 逐一检查跳过原因
print('\n=== 跳过分析 ===')
skip_no_dir = []
for stock in db_stocks:
    # dump脚本中的转换逻辑
    parts = stock.split('.')
    if len(parts) == 2:
        stock_lower = parts[0] + '.' + parts[1].lower()
    else:
        stock_lower = stock.lower()

    if stock_lower not in qlib_dirs:
        skip_no_dir.append((stock, stock_lower))
        print(f'  SKIP: DB={stock} -> qlib_key={stock_lower} (NOT IN qlib_bin)')

print(f'\n跳过总数: {len(skip_no_dir)}')
print(f'跳过比例: {len(skip_no_dir)/50*100:.1f}%')

# 查qlib里有什么而DB里没有的
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("SELECT DISTINCT ts_code FROM market.stk_limit")
all_db_stocks = {row[0].lower().replace('.sz','.sz').replace('.sh','.sh') for row in cur.fetchall()}
conn.close()

# 转换qlib目录名为标准格式比对
qlib_stocks_upper = set()
for d in qlib_dirs:
    parts = d.split('.')
    if len(parts) == 2:
        qlib_stocks_upper.add(parts[0] + '.' + parts[1].upper())

in_qlib_not_db = qlib_stocks_upper - {s for s in all_db_stocks}
in_db_not_qlib = {s for s in all_db_stocks} - {s.lower() for s in qlib_stocks_upper}
print(f'\nQlib有但DB无: {len(in_qlib_not_db)} 只')
print(f'DB有但Qlib无: {len(in_db_not_qlib)} 只')
if in_db_not_qlib:
    print(f'  DB有Qlib无样例: {sorted(in_db_not_qlib)[:10]}')
