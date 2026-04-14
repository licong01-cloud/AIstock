#!/usr/bin/env python3
"""
股票池生成脚本

根据 sw2_pool_config 黑名单表，从 Qlib all.txt 中过滤掉被排除的行业股票，
生成适用于 Qlib 回测的股票池文件（三列 TSV 格式）。

用法:
    python generate_stock_pool.py --date 2024-01-01
    python generate_stock_pool.py  # 不传 --date 则使用今天

输出:
    F:/Dev/AIstock/stock_pools/filtered_pool_YYYYMMDD.txt
    格式与 Qlib all.txt 一致：000001.SZ\t2018-08-01\t2026-03-10
"""

import argparse
import os
import sys
from datetime import date, datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# 加载 AIstock .env
ENV_PATH = Path(__file__).parent.parent / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("TDX_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("TDX_DB_PORT", 5432))
DB_NAME = os.getenv("TDX_DB_NAME", "aistock")
DB_USER = os.getenv("TDX_DB_USER", "postgres")
DB_PASS = os.getenv("TDX_DB_PASSWORD", "")

# Qlib all.txt 路径（通过 WSL UNC 路径访问）
ALL_TXT_PATH = Path(os.getenv(
    "QLIB_INSTRUMENTS_WIN",
    r"\\wsl.localhost\Ubuntu\home\lc999\data\qlib_bin\instruments\all.txt"
))

# 输出目录
OUTPUT_DIR = Path(os.getenv(
    "STOCK_POOL_OUTPUT_DIR",
    "F:/Dev/AIstock/stock_pools"
))

# Qlib instruments 目录（WSL UNC路径），文件需放在此处才能被 Qlib 按名称引用
QLIB_INSTRUMENTS_DIR = Path(os.getenv(
    "QLIB_INSTRUMENTS_DIR_WIN",
    r"\\wsl.localhost\Ubuntu\home\lc999\data\qlib_bin\instruments"
))


def get_blocked_stocks(conn, query_date: date) -> set:
    """查询指定日期应被排除的股票代码集合。"""
    with conn.cursor() as cur:
        # 1. 获取当日 blocked 的申万二级行业代码
        cur.execute("""
            SELECT sw2_code FROM sw2_pool_config
            WHERE status = 'blocked'
              AND is_active = TRUE
              AND (effective_from IS NULL OR effective_from <= %s)
              AND (effective_to IS NULL OR effective_to >= %s)
        """, (query_date, query_date))
        blocked_sw2 = [row[0] for row in cur.fetchall()]

    if not blocked_sw2:
        print(f"[INFO] 日期 {query_date}: 无 blocked 行业，股票池不做过滤")
        return set()

    print(f"[INFO] 日期 {query_date}: blocked 申万二级行业 {len(blocked_sw2)} 个: {blocked_sw2}")

    with conn.cursor() as cur:
        # 2. 查询这些行业在该日期的成分股（PIT）
        placeholders = ",".join(["%s"] * len(blocked_sw2))
        cur.execute(f"""
            SELECT DISTINCT ts_code FROM market.sw_index_member
            WHERE l2_code IN ({placeholders})
              AND in_date <= %s
              AND (out_date IS NULL OR out_date >= %s)
        """, blocked_sw2 + [query_date, query_date])
        blocked_stocks = {row[0] for row in cur.fetchall()}

    print(f"[INFO] 日期 {query_date}: 排除股票 {len(blocked_stocks)} 只")
    return blocked_stocks


def load_all_txt(path: Path) -> list:
    """加载 Qlib all.txt，返回行列表（保留原始格式）。"""
    if not path.exists():
        raise FileNotFoundError(f"Qlib all.txt 不存在: {path}")
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f if line.strip()]
    print(f"[INFO] 加载 all.txt: {len(lines)} 只股票")
    return lines


def generate_pool(query_date: date, output_dir: Path) -> Path:
    """生成过滤后的股票池文件，返回输出路径。"""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    try:
        blocked = get_blocked_stocks(conn, query_date)
    finally:
        conn.close()

    all_lines = load_all_txt(ALL_TXT_PATH)

    if not blocked:
        # 无排除行业，直接复制 all.txt 内容
        filtered = all_lines
    else:
        filtered = []
        for line in all_lines:
            ts_code = line.split("\t")[0]
            if ts_code not in blocked:
                filtered.append(line)

    removed = len(all_lines) - len(filtered)
    print(f"[INFO] 过滤后: {len(filtered)} 只股票（排除 {removed} 只）")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"filtered_pool_{query_date.strftime('%Y%m%d')}.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(filtered))
        if filtered:
            f.write("\n")

    print(f"[INFO] 输出文件: {output_path}")

    # 同步到 Qlib instruments 目录（Qlib 按名称+自动加.txt查找）
    instrument_name = f"filtered_pool_{query_date.strftime('%Y%m%d')}"
    qlib_dest = QLIB_INSTRUMENTS_DIR / f"{instrument_name}.txt"
    try:
        import shutil, tempfile
        # 原子写入：先写临时文件再 rename，避免目标文件被其他进程锁定时 PermissionError
        tmp_fd, tmp_path = tempfile.mkstemp(dir=str(QLIB_INSTRUMENTS_DIR), suffix=".tmp")
        os.close(tmp_fd)
        try:
            shutil.copy2(str(output_path), tmp_path)
            os.replace(tmp_path, str(qlib_dest))
        except BaseException:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
        print(f"[INFO] 已同步到 Qlib instruments: {qlib_dest}")
    except Exception as e:
        print(f"[ERROR] 同步到 Qlib instruments 失败: {e}")
        raise

    return output_path, instrument_name


def main():
    parser = argparse.ArgumentParser(description="生成 Qlib 股票池过滤文件")
    parser.add_argument(
        "--date", type=str, default=None,
        help="查询日期 YYYY-MM-DD，默认今天"
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="输出目录，默认 F:/Dev/AIstock/stock_pools"
    )
    args = parser.parse_args()

    query_date = date.today()
    if args.date:
        try:
            query_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"[ERROR] 日期格式错误: {args.date}，应为 YYYY-MM-DD")
            sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR

    output_path, instrument_name = generate_pool(query_date, output_dir)
    # 输出 instrument 名称（不带 .txt），Qlib 会自动加 .txt 从 instruments/ 目录查找
    print(f"[WSL PATH] {instrument_name}")


if __name__ == "__main__":
    main()
