"""全量分钟线导出 → CSV → dump_bin → Qlib bin
覆盖 RDAgent/QE 回测范围: 2024-01-02 ~ 2026-04-28
  - QE test: 2024-07-01 ~ 2026-04-28 (需 60 天窗口 -> 2024-05-01 起)
  - RDAgent test: 2021-01-01 ~ 2026-04-28 (分钟线数据 2024-01-02 起)
  - 导出全量 2024-01-02 ~ 2026-04-28 覆盖所有需求
字段: open, high, low, close, volume, amount, factor, limit_up, limit_down
"""
import os, sys, time, gc, shutil
os.environ.setdefault('TDX_DB_PASSWORD', 'lc78080808')

import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path

# ========== 配置 ==========
START = '2024-01-02'
END = '2026-04-28'
OUTPUT_DIR = Path('F:/Dev/AIstock/qlib_minute_prod')
CSV_DIR = OUTPUT_DIR / 'csv'
BIN_DIR = OUTPUT_DIR / 'bin'
BATCH_SIZE = 50  # 每批 50 只股票 (数据量大，减小批次避免内存溢出)

# 清空旧 CSV (bin 由 dump_bin 覆盖)
if CSV_DIR.exists():
    shutil.rmtree(CSV_DIR)
CSV_DIR.mkdir(parents=True, exist_ok=True)
BIN_DIR.mkdir(parents=True, exist_ok=True)

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
conn.set_session(autocommit=True)
cur = conn.cursor()

# ========== Step 1: 获取股票列表 ==========
print('=== Step 1: 获取股票列表 ===')
cur.execute("""
    SELECT DISTINCT ts_code FROM market.kline_minute_raw WHERE freq='1m'
""")
all_codes = sorted([r[0] for r in cur.fetchall()])
print(f'  股票数: {len(all_codes)}')

# ========== Step 2: 加载 stk_limit ==========
print('=== Step 2: 加载 stk_limit ===')
cur.execute("""
    SELECT ts_code, trade_date, up_limit, down_limit
    FROM market.stk_limit
    WHERE trade_date >= %s AND trade_date <= %s
""", (START, END))
limit_rows = cur.fetchall()
limit_df = pd.DataFrame(limit_rows, columns=['ts_code', 'trade_date', 'up_limit', 'down_limit'])
limit_df['trade_date'] = pd.to_datetime(limit_df['trade_date']).dt.date
# 关键：psycopg2 返回 Decimal 类型，必须转 float，否则 float >= Decimal 比较结果随机错误
limit_df['up_limit'] = pd.to_numeric(limit_df['up_limit'], errors='coerce').astype(np.float64)
limit_df['down_limit'] = pd.to_numeric(limit_df['down_limit'], errors='coerce').astype(np.float64)
print(f'  stk_limit 行数: {len(limit_df):,}')

# ========== Step 3: 加载复权因子 ==========
print('=== Step 3: 加载复权因子 ===')
cur.execute("""
    SELECT ts_code, trade_date, adj_factor
    FROM market.adj_factor
    WHERE trade_date >= %s AND trade_date <= %s
""", (START, END))
adj_rows = cur.fetchall()
adj_df = pd.DataFrame(adj_rows, columns=['ts_code', 'trade_date', 'adj_factor'])
adj_df['trade_date'] = pd.to_datetime(adj_df['trade_date']).dt.date
# 前复权: qfq = adj_factor / max(adj_factor per stock)
max_adj = adj_df.groupby('ts_code')['adj_factor'].transform('max')
adj_df['qfq_factor'] = adj_df['adj_factor'] / max_adj
print(f'  adj_factor 行数: {len(adj_df):,}')

# ========== Step 4: 分批导出 CSV ==========
print(f'=== Step 4: 分批导出 CSV ({START} ~ {END}) ===')
total_rows = 0
t0 = time.time()

# 按季度分段查询，避免单次查询数据量过大
quarters = [
    ('2024-01-01', '2024-04-01'),
    ('2024-04-01', '2024-07-01'),
    ('2024-07-01', '2024-10-01'),
    ('2024-10-01', '2025-01-01'),
    ('2025-01-01', '2025-04-01'),
    ('2025-04-01', '2025-07-01'),
    ('2025-07-01', '2025-10-01'),
    ('2025-10-01', '2026-01-01'),
    ('2026-01-01', '2026-04-01'),
]

for q_idx, (q_start, q_end) in enumerate(quarters):
    q_start_ts = f'{q_start} 00:00:00+08'
    q_end_ts = f'{q_end} 00:00:00+08'
    q_rows = 0
    print(f'\n  --- 季度 {q_start[:7]} ---')

    for batch_idx in range(0, len(all_codes), BATCH_SIZE):
        batch_codes = all_codes[batch_idx:batch_idx + BATCH_SIZE]

        cur.execute("""
            SELECT trade_time, ts_code, open_li, high_li, low_li, close_li,
                   volume_hand, amount_li
            FROM market.kline_minute_raw
            WHERE ts_code = ANY(%s) AND freq='1m'
              AND trade_time >= %s AND trade_time < %s
            ORDER BY ts_code, trade_time
        """, (batch_codes, q_start_ts, q_end_ts))
        rows = cur.fetchall()
        if not rows:
            continue

        df = pd.DataFrame(rows, columns=['trade_time', 'ts_code', 'open_li', 'high_li',
                                         'low_li', 'close_li', 'volume_hand', 'amount_li'])
        df['trade_date'] = pd.to_datetime(df['trade_time']).dt.date

        # 合并复权因子
        df = df.merge(adj_df[['ts_code', 'trade_date', 'qfq_factor']],
                      on=['ts_code', 'trade_date'], how='left')
        df['qfq_factor'] = df['qfq_factor'].fillna(1.0)

        # 合并涨跌停价
        df = df.merge(limit_df[['ts_code', 'trade_date', 'up_limit', 'down_limit']],
                      on=['ts_code', 'trade_date'], how='left')

        # 计算各字段
        SCALE = 1000.0
        df['open']   = (df['open_li']  / SCALE * df['qfq_factor']).astype(np.float32)
        df['high']   = (df['high_li']  / SCALE * df['qfq_factor']).astype(np.float32)
        df['low']    = (df['low_li']   / SCALE * df['qfq_factor']).astype(np.float32)
        df['close']  = (df['close_li'] / SCALE * df['qfq_factor']).astype(np.float32)
        df['volume'] = (df['volume_hand'] * 100.0 / df['qfq_factor']).astype(np.float32)
        df['amount'] = (df['amount_li'] / SCALE).astype(np.float32)
        df['factor'] = df['qfq_factor'].astype(np.float32)

        # 涨跌停标志（基于收盘价判断：close 达到涨跌停价即标记）
        close_yuan = df['close_li'] / SCALE
        df['limit_up'] = (close_yuan >= df['up_limit']).astype(np.float32)
        df['limit_down'] = (close_yuan <= df['down_limit']).astype(np.float32)
        nan_mask = df['up_limit'].isna()
        df.loc[nan_mask, 'limit_up'] = 0.0
        df.loc[nan_mask, 'limit_down'] = 0.0

        # 格式化
        df['date'] = pd.to_datetime(df['trade_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['symbol'] = df['ts_code'].str.lower()

        csv_cols = ['date', 'symbol', 'open', 'high', 'low', 'close',
                    'volume', 'amount', 'factor', 'limit_up', 'limit_down']

        for sym, grp in df.groupby('symbol'):
            csv_path = CSV_DIR / f'{sym}.csv'
            if csv_path.exists():
                grp[csv_cols].to_csv(csv_path, index=False, mode='a', header=False)
            else:
                grp[csv_cols].to_csv(csv_path, index=False)

        batch_rows = len(df)
        total_rows += batch_rows
        q_rows += batch_rows
        del df, rows
        gc.collect()

    elapsed = time.time() - t0
    print(f'  季度 {q_start[:7]} 完成: {q_rows:,} 行, 累计 {total_rows:,} 行, {elapsed:.0f}s')

conn.close()
csv_count = len(list(CSV_DIR.glob("*.csv")))
elapsed = time.time() - t0
print(f'\n=== 导出完成 ===')
print(f'  总行数: {total_rows:,}')
print(f'  CSV 文件: {csv_count} 个')
print(f'  耗时: {elapsed:.0f}s ({elapsed/60:.1f} min)')

# ========== Step 5: dump_bin 命令 ==========
csv_wsl = str(CSV_DIR).replace('F:', '/mnt/f').replace('\\', '/')
bin_wsl = str(BIN_DIR).replace('F:', '/mnt/f').replace('\\', '/')
cmd = (f'source ~/miniconda3/etc/profile.d/conda.sh && '
       f'conda activate rdagent-gpu && '
       f'python /mnt/f/Dev/RD-Agent-main/scripts/dump_bin.py dump_all '
       f'--data_path {csv_wsl} '
       f'--qlib_dir {bin_wsl} '
       f'--freq 1min '
       f'--date_field_name date '
       f'--symbol_field_name symbol '
       f'--exclude_fields date,symbol')
print(f'\n=== Step 5: dump_bin 命令 ===')
print(f'wsl bash -c "{cmd}"')
