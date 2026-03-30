"""
从 Tushare stk_mins 获取 22 天缺失的分钟线数据，缓存到本地 parquet。
每分钟限 2 次调用，每次获取一天全市场数据。
"""
import os
import time
import tushare as ts
import pandas as pd

CACHE_DIR = r'F:\Dev\AIstock\scripts\tushare_min_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

MISSING_DATES = [
    '20240717', '20240725', '20240805', '20240820', '20240909', '20240910',
    '20240918', '20241021', '20241031', '20241106', '20241108', '20241118',
    '20241127', '20241128', '20241225', '20250102', '20250103', '20250114',
    '20250117', '20250121', '20250212', '20250305',
]

# 读取 token
with open(r'F:\Dev\AIstock\.env', 'r', encoding='utf-8') as f:
    for line in f:
        if line.startswith('TUSHARE_TOKEN'):
            token = line.split('=', 1)[1].strip().strip('"').strip("'")
            break

pro = ts.pro_api(token)

for i, d in enumerate(MISSING_DATES):
    cache_path = os.path.join(CACHE_DIR, f'{d}_1min.parquet')
    if os.path.exists(cache_path):
        df = pd.read_parquet(cache_path)
        print(f'[{i+1}/{len(MISSING_DATES)}] {d}: 已缓存 ({len(df):,} 行, {df["ts_code"].nunique()} 只)')
        continue

    print(f'[{i+1}/{len(MISSING_DATES)}] {d}: 获取中...', end='', flush=True)
    try:
        df = pro.stk_mins(
            start_date=f'{d} 09:30:00',
            end_date=f'{d} 15:00:00',
            freq='1min'
        )
        if df is not None and not df.empty:
            # 过滤掉北交所 (8xxxxx.BJ)
            df = df[~df['ts_code'].str.endswith('.BJ')]
            n_stocks = df['ts_code'].nunique()
            df.to_parquet(cache_path)
            print(f' {len(df):,} 行, {n_stocks} 只, {os.path.getsize(cache_path)/1024/1024:.1f}MB')
        else:
            print(f' 空数据!')
    except Exception as e:
        err = str(e)
        print(f' 失败: {err[:80]}')
        if '每分钟' in err or 'freq' in err.lower():
            print('  限流，等待 35 秒...')
            time.sleep(35)
            # 重试一次
            try:
                df = pro.stk_mins(
                    start_date=f'{d} 09:30:00',
                    end_date=f'{d} 15:00:00',
                    freq='1min'
                )
                if df is not None and not df.empty:
                    df = df[~df['ts_code'].str.endswith('.BJ')]
                    df.to_parquet(cache_path)
                    print(f'  重试成功: {len(df):,} 行')
                else:
                    print(f'  重试返回空数据')
            except Exception as e2:
                print(f'  重试失败: {str(e2)[:80]}')

    # 限流：每次调用后等 35 秒（每分钟 2 次限制）
    if i < len(MISSING_DATES) - 1:
        time.sleep(35)

print('\n=== 完成 ===')
cached = [f for f in os.listdir(CACHE_DIR) if f.endswith('.parquet')]
print(f'已缓存文件: {len(cached)} / {len(MISSING_DATES)}')
for f in sorted(cached):
    size = os.path.getsize(os.path.join(CACHE_DIR, f))
    print(f'  {f}: {size/1024/1024:.1f}MB')
