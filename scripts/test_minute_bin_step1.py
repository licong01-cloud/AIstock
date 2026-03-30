"""端到端验证：分钟线 bin 导出 + Qlib 加载
1. 从 DB 导出 10 天 × 少量股票的分钟线 CSV
2. 用 dump_bin.py 生成 bin 文件
3. 用 Qlib D.features() 加载验证
"""
import os, sys, shutil, warnings
warnings.filterwarnings('ignore')
os.environ.setdefault('TDX_DB_PASSWORD', 'lc78080808')
sys.path.insert(0, 'F:/Dev/AIstock')

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date
from backend.qlib_exporter.db_reader import DBReader

# ========== 配置 ==========
TEST_CODES = ['000001.SZ', '000004.SZ', '300750.SZ', '600519.SH']
START = date(2026, 3, 10)
END = date(2026, 3, 20)
TEST_DIR = Path('F:/Dev/AIstock/qlib_test_minute')
CSV_DIR = TEST_DIR / 'csv'
BIN_DIR = TEST_DIR / 'bin'

# ========== Step 1: 导出分钟线数据 ==========
print('=== Step 1: 从 DB 导出分钟线数据 ===')
reader = DBReader()
df = reader.load_qlib_minute_data(ts_codes=TEST_CODES, start=START, end=END, freq='1m')
print(f'  Shape: {df.shape}, Columns: {list(df.columns)}')

# 转换为 dump_bin.py 需要的 CSV 格式
df_reset = df.reset_index()
df_reset['date'] = df_reset['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df_reset['symbol'] = df_reset['instrument'].str.lower()

rename_map = {}
for col in df_reset.columns:
    if col.startswith('$'):
        rename_map[col] = col[1:]  # 去掉 $ 前缀
df_reset = df_reset.rename(columns=rename_map)

csv_cols = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume',
            'amount', 'factor', 'limit_up', 'limit_down']
for col in csv_cols:
    if col not in df_reset.columns:
        df_reset[col] = float('nan')

# 按 symbol 拆分写 CSV（dump_bin 需要每个 symbol 一个文件）
if CSV_DIR.exists():
    shutil.rmtree(CSV_DIR)
CSV_DIR.mkdir(parents=True, exist_ok=True)

for sym, grp in df_reset.groupby('symbol'):
    csv_path = CSV_DIR / f'{sym}.csv'
    grp[csv_cols].to_csv(csv_path, index=False)
    print(f'  写入 {csv_path.name}: {len(grp)} 行')

# 写 calendars
if BIN_DIR.exists():
    shutil.rmtree(BIN_DIR)
BIN_DIR.mkdir(parents=True, exist_ok=True)

print(f'\n=== Step 2: 请在 WSL 中运行以下命令生成 bin 文件 ===')
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
print(f'  wsl bash -c "{cmd}"')

# 保存原始 df 用于后续验证对比
df.to_pickle(TEST_DIR / 'source_df.pkl')
print(f'\n  源数据已保存到 {TEST_DIR / "source_df.pkl"}')
print(f'  dump_bin 完成后运行 Step 3 验证脚本')
