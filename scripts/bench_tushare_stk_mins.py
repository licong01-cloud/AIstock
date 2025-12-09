from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import tushare as ts


@dataclass
class BenchResult:
    ts_code: str
    calls: int
    rows: int
    elapsed: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark Tushare stk_mins throughput")
    p.add_argument("codes", nargs="+", help="一个或多个股票代码, e.g. 600000.SH 000001.SZ")
    p.add_argument("--freq", default="1min", choices=["1min", "5min", "15min", "30min", "60min"], help="分钟频度")
    p.add_argument("--start", required=True, help="开始时间, 格式: YYYY-MM-DD HH:MM:SS")
    p.add_argument("--end", required=True, help="结束时间, 格式: YYYY-MM-DD HH:MM:SS")
    p.add_argument(
        "--mode",
        default="single",
        choices=["single", "parallel"],
        help="single=按代码串行测试, parallel=多代码并行测试",
    )
    p.add_argument("--workers", type=int, default=2, help="并行模式下的最大并发代码数")
    return p.parse_args()


def load_token() -> str:
    """Load Tushare token from env or project .env file.

    优先读取环境变量 TUSHARE_TOKEN / TS_TOKEN，如果都不存在，则尝试
    从项目根目录的 .env 文件中解析 TUSHARE_TOKEN 行。
    """

    token = os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN")
    if token:
        return token

    # 尝试从项目根目录的 .env 读取
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent  # scripts/ 上一级
    env_path = project_root / ".env"
    if not env_path.exists():
        print("[ERROR] 环境变量 TUSHARE_TOKEN 未设置，且项目根目录不存在 .env 文件", file=sys.stderr)
        sys.exit(1)

    try:
        with env_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("TUSHARE_TOKEN"):
                    # 形如 TUSHARE_TOKEN="xxxx" 或 TUSHARE_TOKEN=xxxx
                    _, val = line.split("=", 1)
                    val = val.strip().strip('"').strip("'")
                    if val:
                        return val
    except Exception as e:  # noqa: BLE001
        print(f"[ERROR] 读取 .env 失败: {e}", file=sys.stderr)
        sys.exit(1)

    print("[ERROR] 未能在环境变量或 .env 中找到 TUSHARE_TOKEN", file=sys.stderr)
    sys.exit(1)


def bench_one_code(ts_code: str, freq: str, start_s: str, end_s: str, token: str) -> BenchResult:
    ts.set_token(token)
    pro = ts.pro_api()

    try:
        cur_start = datetime.strptime(start_s, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_s, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        raise SystemExit(f"时间格式错误: {e}") from e

    if cur_start >= end_dt:
        raise SystemExit("start 必须早于 end")

    total_rows = 0
    total_calls = 0

    print(f"[INFO] [{ts_code}] freq={freq}, start={start_s}, end={end_s}")
    t0 = time.time()

    while cur_start < end_dt:
        start_str = cur_start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[INFO] [{ts_code}] requesting stk_mins: start={start_str}, end={end_str} ...")

        # 简单限频处理：如果触发“每分钟最多访问该接口2次”，则 sleep 一段时间后重试
        while True:
            try:
                df = pro.stk_mins(
                    ts_code=ts_code,
                    freq=freq,
                    start_date=start_str,
                    end_date=end_str,
                )
                total_calls += 1
                break
            except Exception as e:  # noqa: BLE001
                msg = str(e)
                if "每分钟最多访问该接口2次" in msg:
                    print(f"[WARN] [{ts_code}] 命中Tushare限频，sleep 35s 后重试...")
                    time.sleep(35)
                    continue
                print(f"[ERROR] [{ts_code}] 调用 stk_mins 失败: {e}", file=sys.stderr)
                return BenchResult(ts_code=ts_code, calls=total_calls, rows=total_rows, elapsed=time.time() - t0)

        if df is None or df.empty:
            print(f"[INFO] [{ts_code}] got empty result, stop.")
            break

        rows = len(df)
        total_rows += rows
        print(f"[INFO] [{ts_code}] got {rows} rows, accumulated={total_rows}")

        if rows < 8000:
            break

        try:
            df["trade_time"] = pd.to_datetime(df["trade_time"])
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] [{ts_code}] 解析 trade_time 失败: {e}", file=sys.stderr)
            break

        max_ts = df["trade_time"].max()
        if pd.isna(max_ts):
            print(f"[WARN] [{ts_code}] max trade_time is NaT, stop.")
            break

        cur_start = (max_ts + timedelta(minutes=1)).to_pydatetime()
        if cur_start >= end_dt:
            break

    elapsed = time.time() - t0
    return BenchResult(ts_code=ts_code, calls=total_calls, rows=total_rows, elapsed=elapsed)


def main() -> None:
    args = parse_args()

    token = load_token()

    print(
        f"[INFO] mode={args.mode}, codes={args.codes}, freq={args.freq}, "
        f"start={args.start}, end={args.end}"
    )

    results: List[BenchResult] = []
    t0 = time.time()

    if args.mode == "single" or len(args.codes) == 1:
        # 串行按代码依次测试
        for code in args.codes:
            res = bench_one_code(code, args.freq, args.start, args.end, token)
            results.append(res)
    else:
        # 并行模式：按代码并发
        workers = max(1, args.workers)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut_map = {
                ex.submit(bench_one_code, code, args.freq, args.start, args.end, token): code
                for code in args.codes
            }
            for fut in as_completed(fut_map):
                res = fut.result()
                results.append(res)

    total_elapsed = time.time() - t0

    print("\n[SUMMARY]")
    agg_rows = 0
    for r in sorted(results, key=lambda x: x.ts_code):
        agg_rows += r.rows
        rps = r.rows / r.elapsed if r.elapsed > 0 else float("inf")
        print(
            f"  {r.ts_code}: calls={r.calls}, rows={r.rows}, "
            f"elapsed={r.elapsed:.3f}s, rows_per_sec={rps:.2f}"
        )

    print("\n[AGG]")
    print(f"  codes        = {len(results)}")
    print(f"  total_rows   = {agg_rows}")
    print(f"  wall_elapsed = {total_elapsed:.3f}s")
    if total_elapsed > 0:
        print(f"  wall_rows_per_sec = {agg_rows / total_elapsed:.2f}")
    else:
        print("  wall_rows_per_sec = inf (elapsed ~ 0)")


if __name__ == "__main__":
    main()
