"""修复 market.stk_limit 数据缺失 + 增量修复分钟级 bin 文件。

问题1: 2024-07-23 全市场 pre_close=NULL (6868条)
  修复: 从 kline_daily_raw 取 20240722 的 close_li/1000 作为 pre_close
        (kline_daily_raw 为未复权数据, adjust_type='none')

问题2: 2024-07-24 缺失 2068 条记录
  修复: 从 Tushare stk_limit 接口重新拉取 20240724 全量数据,
        pre_close 从 kline_daily_raw 20240723 的 close 补充

增量修复 bin: 只重写受影响股票在受影响日期的 prev_close/up_limit_price/down_limit_price bin

用法:
  python fix_stk_limit_and_bins.py --dry-run     # 预览修复内容
  python fix_stk_limit_and_bins.py --fix-db       # 修复数据库
  python fix_stk_limit_and_bins.py --fix-bins     # 修复 bin 文件 (需先 fix-db)
  python fix_stk_limit_and_bins.py --fix-db --fix-bins  # 一步全修
"""
import argparse
import logging
import struct
import sys
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_db_conn():
    import psycopg2
    return psycopg2.connect(
        host="127.0.0.1", port=5432,
        user="postgres", password="lc78080808",
        dbname="aistock",
    )


def get_tushare_pro():
    import tushare as ts
    return ts.pro_api("befde989d7691539d75ec722e88ccc61b1932d1eab8260f785a4822e")


# ──────────────────────────────────────────────
# Step 1: 修复 DB — 20240723 pre_close
# ──────────────────────────────────────────────
def fix_20240723_pre_close(dry_run=True):
    """从 kline_daily_raw 20240722 close 补充 stk_limit 20240723 pre_close."""
    conn = get_db_conn()
    cur = conn.cursor()

    # 找可修复的记录: stk_limit 20240723 pre_close IS NULL 且 kline 20240722 有 close
    cur.execute("""
        SELECT s.ts_code, k.close_li / 1000.0 as pre_close
        FROM market.stk_limit s
        JOIN market.kline_daily_raw k ON k.ts_code = s.ts_code AND k.trade_date = '20240722'
        WHERE s.trade_date = '20240723' AND s.pre_close IS NULL
    """)
    rows = cur.fetchall()
    logger.info(f"[20240723] 可从 kline 20240722 补充 pre_close: {len(rows)} 条")

    if dry_run:
        for r in rows[:5]:
            logger.info(f"  {r[0]}: pre_close={r[1]}")
        logger.info("  ... (dry-run, not executing)")
        conn.close()
        return len(rows)

    updated = 0
    for ts_code, pre_close_val in rows:
        cur.execute("""
            UPDATE market.stk_limit SET pre_close = %s
            WHERE ts_code = %s AND trade_date = '20240723'
        """, (pre_close_val, ts_code))
        updated += cur.rowcount

    conn.commit()
    logger.info(f"[20240723] Updated {updated} records")

    # 检查剩余 NULL
    cur.execute("SELECT count(*) FROM market.stk_limit WHERE trade_date='20240723' AND pre_close IS NULL")
    remaining = cur.fetchone()[0]
    logger.info(f"[20240723] Remaining pre_close NULL: {remaining} (ETF/B股, 不影响回测)")
    conn.close()
    return updated


# ──────────────────────────────────────────────
# Step 2: 修复 DB — 20240724 缺失记录
# ──────────────────────────────────────────────
def fix_20240724_missing(dry_run=True):
    """从 Tushare 补充 stk_limit 20240724 缺失记录, pre_close 从 kline 20240723 获取."""
    conn = get_db_conn()
    cur = conn.cursor()

    # 找出 20240724 缺失的股票
    cur.execute("""
        SELECT DISTINCT s23.ts_code
        FROM market.stk_limit s23
        WHERE s23.trade_date = '20240723'
        AND NOT EXISTS (
            SELECT 1 FROM market.stk_limit s24
            WHERE s24.ts_code = s23.ts_code AND s24.trade_date = '20240724'
        )
    """)
    missing_stocks = [r[0] for r in cur.fetchall()]
    logger.info(f"[20240724] DB 中缺失股票: {len(missing_stocks)}")

    # 从 Tushare 拉取 20240724 全量 stk_limit
    pro = get_tushare_pro()
    ts_df = pro.stk_limit(trade_date='20240724')
    logger.info(f"[20240724] Tushare 返回 {len(ts_df)} 条记录")

    # 筛选 DB 缺失的
    ts_missing = ts_df[ts_df['ts_code'].isin(missing_stocks)]
    logger.info(f"[20240724] 其中 DB 缺失的: {len(ts_missing)} 条")

    # 从 kline_daily_raw 20240723 获取 pre_close
    cur.execute("""
        SELECT ts_code, close_li / 1000.0 as close_yuan
        FROM market.kline_daily_raw
        WHERE trade_date = '20240723'
    """)
    kline_close = {r[0]: float(r[1]) for r in cur.fetchall()}

    if dry_run:
        for _, row in ts_missing.head(5).iterrows():
            pc = kline_close.get(row['ts_code'])
            logger.info(f"  {row['ts_code']}: ul={row['up_limit']}, dl={row['down_limit']}, pc(from kline)={pc}")
        logger.info(f"  ... (dry-run, not executing)")
        conn.close()
        return len(ts_missing)

    inserted = 0
    for _, row in ts_missing.iterrows():
        pc = kline_close.get(row['ts_code'])
        cur.execute("""
            INSERT INTO market.stk_limit (trade_date, ts_code, pre_close, up_limit, down_limit)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (trade_date, ts_code) DO NOTHING
        """, ('20240724', row['ts_code'], pc, row['up_limit'], row['down_limit']))
        inserted += cur.rowcount

    conn.commit()
    logger.info(f"[20240724] Inserted {inserted} records")

    # 验证
    cur.execute("SELECT count(*) FROM market.stk_limit WHERE trade_date='20240724'")
    total = cur.fetchone()[0]
    logger.info(f"[20240724] 修复后总记录数: {total}")
    conn.close()
    return inserted


# ──────────────────────────────────────────────
# Step 3: 增量修复 bin 文件
# ──────────────────────────────────────────────
BIN_DIR = Path("/home/lc999/data/qlib_minute_bin")


def load_calendar():
    cal = pd.read_csv(BIN_DIR / "calendars" / "1min.txt", header=None, names=["datetime"])
    return pd.DatetimeIndex(pd.to_datetime(cal["datetime"]))


def read_bin(bin_path):
    """读取 qlib bin 文件, 返回 (start_idx, data_array)."""
    raw = np.fromfile(str(bin_path), dtype="<f4")
    if len(raw) < 2:
        return None, None
    return int(raw[0]), raw[1:]


def write_bin(bin_path, start_idx, data):
    """写入 qlib bin 文件."""
    header = np.array([start_idx], dtype="<f4")
    arr = np.array(data, dtype="<f4")
    with open(bin_path, "wb") as f:
        f.write(header.tobytes())
        f.write(arr.tobytes())


def fix_bins(dry_run=True):
    """增量修复受影响日期的 bin 文件."""
    conn = get_db_conn()
    cur = conn.cursor()

    cal_idx = load_calendar()

    # 获取需要修复的 (stock, date, field, value) 列表
    # 20240723: 所有股票的 prev_close 需要修复
    cur.execute("""
        SELECT ts_code, pre_close FROM market.stk_limit
        WHERE trade_date = '20240723' AND pre_close IS NOT NULL
    """)
    fix_0723 = {r[0]: {"prev_close": float(r[1])} for r in cur.fetchall()}

    # 20240724: 之前缺失的股票需要补全三个字段
    cur.execute("""
        SELECT ts_code, pre_close, up_limit, down_limit FROM market.stk_limit
        WHERE trade_date = '20240724'
    """)
    all_0724 = {r[0]: {"prev_close": float(r[1]) if r[1] else None,
                        "up_limit_price": float(r[2]) if r[2] else None,
                        "down_limit_price": float(r[3]) if r[3] else None}
                for r in cur.fetchall()}
    conn.close()

    # 找出 20240723 和 20240724 在 calendar 中对应的分钟索引范围
    dates_to_fix = {"20240723": "2024-07-23", "20240724": "2024-07-24"}
    date_ranges = {}
    for key, date_str in dates_to_fix.items():
        dt_start = pd.Timestamp(f"{date_str} 00:00:00")
        dt_end = pd.Timestamp(f"{date_str} 23:59:59")
        mask = (cal_idx >= dt_start) & (cal_idx <= dt_end)
        positions = np.where(mask)[0]
        if len(positions) > 0:
            date_ranges[key] = (positions[0], positions[-1])
            logger.info(f"Calendar {date_str}: idx {positions[0]}~{positions[-1]} ({len(positions)} bars)")

    if dry_run:
        logger.info(f"[bin] 20240723 需修复 prev_close: {len(fix_0723)} stocks")
        logger.info(f"[bin] 20240724 需修复 (三字段): {len(all_0724)} stocks")
        logger.info("  (dry-run, not writing)")
        return

    fixed_count = 0
    errors = 0

    # 修复 20240723 prev_close
    if "20240723" in date_ranges:
        idx_start, idx_end = date_ranges["20240723"]
        for ts_code, vals in fix_0723.items():
            stock_lower = ts_code.lower()
            bin_path = BIN_DIR / "features" / stock_lower / "prev_close.1min.bin"
            if not bin_path.exists():
                continue
            try:
                si, data = read_bin(bin_path)
                if data is None:
                    continue
                # 写入该日期范围的值
                for ci in range(idx_start, idx_end + 1):
                    data_idx = ci - si
                    if 0 <= data_idx < len(data):
                        data[data_idx] = np.float32(vals["prev_close"])
                write_bin(bin_path, si, data)
                fixed_count += 1
            except Exception as e:
                logger.warning(f"Error fixing {ts_code} prev_close 20240723: {e}")
                errors += 1

    # 修复 20240724 三个字段
    if "20240724" in date_ranges:
        idx_start, idx_end = date_ranges["20240724"]
        field_map = {
            "prev_close": "prev_close.1min.bin",
            "up_limit_price": "up_limit_price.1min.bin",
            "down_limit_price": "down_limit_price.1min.bin",
        }
        for ts_code, vals in all_0724.items():
            stock_lower = ts_code.lower()
            for field_key, bin_name in field_map.items():
                val = vals.get(field_key)
                if val is None:
                    continue
                bin_path = BIN_DIR / "features" / stock_lower / bin_name
                if not bin_path.exists():
                    continue
                try:
                    si, data = read_bin(bin_path)
                    if data is None:
                        continue
                    for ci in range(idx_start, idx_end + 1):
                        data_idx = ci - si
                        if 0 <= data_idx < len(data):
                            data[data_idx] = np.float32(val)
                    write_bin(bin_path, si, data)
                    fixed_count += 1
                except Exception as e:
                    logger.warning(f"Error fixing {ts_code} {field_key} 20240724: {e}")
                    errors += 1

    logger.info(f"[bin] Fixed {fixed_count} bin files, errors={errors}")


def verify(dates=("20240723", "20240724")):
    """验证修复结果."""
    cal_idx = load_calendar()
    test_stocks = ["603607.sh", "603232.sh", "688372.sh"]

    for date_str_raw in dates:
        date_str = f"{date_str_raw[:4]}-{date_str_raw[4:6]}-{date_str_raw[6:]}"
        dt_start = pd.Timestamp(f"{date_str} 09:30:00")
        mask = (cal_idx >= dt_start) & (cal_idx <= pd.Timestamp(f"{date_str} 10:00:00"))
        positions = np.where(mask)[0]
        if len(positions) == 0:
            continue

        print(f"\n=== {date_str} verification ===")
        for stock in test_stocks:
            for field in ["prev_close", "up_limit_price", "down_limit_price"]:
                bin_path = BIN_DIR / "features" / stock / f"{field}.1min.bin"
                if not bin_path.exists():
                    print(f"  {stock} {field}: bin not found")
                    continue
                si, data = read_bin(bin_path)
                if data is None:
                    print(f"  {stock} {field}: empty bin")
                    continue
                vals = []
                for ci in positions[:3]:
                    di = ci - si
                    if 0 <= di < len(data):
                        vals.append(float(data[di]))
                    else:
                        vals.append(None)
                nan_count = sum(1 for v in vals if v is None or np.isnan(v))
                print(f"  {stock} {field}: {vals} nan={nan_count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--fix-db", action="store_true", help="Fix database records")
    parser.add_argument("--fix-bins", action="store_true", help="Fix bin files (run from WSL)")
    parser.add_argument("--verify", action="store_true", help="Verify bin files (run from WSL)")
    args = parser.parse_args()

    if args.dry_run or (not args.fix_db and not args.fix_bins and not args.verify):
        logger.info("=== DRY RUN ===")
        fix_20240723_pre_close(dry_run=True)
        fix_20240724_missing(dry_run=True)
        fix_bins(dry_run=True)
        return

    if args.fix_db:
        logger.info("=== Fixing DB ===")
        fix_20240723_pre_close(dry_run=False)
        fix_20240724_missing(dry_run=False)

    if args.fix_bins:
        logger.info("=== Fixing bins ===")
        fix_bins(dry_run=False)

    if args.verify:
        verify()


if __name__ == "__main__":
    main()
