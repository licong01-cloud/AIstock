"""检查 Qlib 导出日线/分钟线中 $volume/$amount 与原始数据库字段的一致性。

- 日线：
  - 验证 $volume 是否约等于 volume_hand * 100 / qfq_factor
  - 验证 $amount 是否约等于 amount_li / PRICE_UNIT_DIVISOR
- 分钟线：
  - 验证 $volume 是否约等于 volume_hand * 100 / qfq_factor
  - 验证 $amount 是否约等于 amount_li / PRICE_UNIT_DIVISOR

运行方式：
    python check_volume_amount_consistency.py
"""

from __future__ import annotations

from datetime import date
from typing import List

import os
import sys

import numpy as np
import pandas as pd


# 确保可以作为独立脚本运行时找到 app_pg 和 qlib_exporter 包
BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BACKEND_ROOT)
for p in (PROJECT_ROOT, BACKEND_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

from app_pg import get_conn  # type: ignore[attr-defined]

from qlib_exporter.config import DAILY_RAW_TABLE, MINUTE_RAW_TABLE, PRICE_UNIT_DIVISOR
from qlib_exporter.db_reader import DBReader


def check_daily(ts_codes: List[str], start: date, end: date) -> None:
    print("=== 日线检查 ===")
    reader = DBReader()

    df_q = reader.load_qlib_daily_data(ts_codes, start, end, use_tushare_adj=True)
    if df_q.empty:
        print("Qlib 日线数据为空")
        return

    # 重置索引，便于 merge
    q = df_q.reset_index()
    q["trade_date"] = pd.to_datetime(q["datetime"]).dt.date

    sql = f"""
        SELECT ts_code, trade_date, volume_hand, amount_li
        FROM {DAILY_RAW_TABLE}
        WHERE ts_code = ANY(%(codes)s)
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
        ORDER BY trade_date, ts_code
    """
    with get_conn() as conn:  # type: ignore[attr-defined]
        raw = pd.read_sql(sql, conn, params={"codes": ts_codes, "start": start, "end": end})

    if raw.empty:
        print("原始日线表为空")
        return

    raw["trade_date"] = pd.to_datetime(raw["trade_date"]).dt.date

    # 根据实际存在的列构建要 merge 的列列表
    cols = ["instrument", "trade_date", "$volume", "$factor"]
    if "$amount" in q.columns:
        cols.append("$amount")

    merged = pd.merge(
        raw,
        q[cols],
        left_on=["ts_code", "trade_date"],
        right_on=["instrument", "trade_date"],
        how="inner",
    )

    if merged.empty:
        print("原始与 Qlib 日线数据 merge 为空，请检查 ts_code / 日期范围")
        return

    # 手 -> 股，再按前复权因子反向调整
    vol_shares = merged["volume_hand"] * 100.0
    vol_expected = vol_shares / merged["$factor"].replace(0, np.nan)

    vol_diff = merged["$volume"] - vol_expected
    vol_rel_err = (vol_diff.abs() / vol_expected.replace(0, np.nan)).dropna()

    print(f"日线 volume 样本数: {len(vol_rel_err)}")
    print(f"日线 volume 最大相对误差: {vol_rel_err.max():.6g}")
    print(f"日线 volume 中位相对误差: {vol_rel_err.median():.6g}")

    if "$amount" in merged.columns and "amount_li" in merged.columns:
        amt_expected = merged["amount_li"] / PRICE_UNIT_DIVISOR
        amt_diff = merged["$amount"] - amt_expected
        amt_rel_err = (amt_diff.abs() / amt_expected.replace(0, np.nan)).dropna()
        print(f"日线 amount 样本数: {len(amt_rel_err)}")
        print(f"日线 amount 最大相对误差: {amt_rel_err.max():.6g}")
        print(f"日线 amount 中位相对误差: {amt_rel_err.median():.6g}")
    else:
        print("日线中缺少 $amount 或 amount_li 列，跳过 amount 检查")


def check_minute(ts_codes: List[str], start: date, end: date) -> None:
    print("=== 分钟线检查 ===")
    reader = DBReader()

    df_q = reader.load_qlib_minute_data(ts_codes, start, end, use_tushare_adj=True)
    if df_q.empty:
        print("Qlib 分钟线数据为空")
        return

    q = df_q.reset_index()
    q["trade_time"] = pd.to_datetime(q["datetime"])

    sql = f"""
        SELECT trade_time, ts_code, volume_hand, amount_li
        FROM {MINUTE_RAW_TABLE}
        WHERE ts_code = ANY(%(codes)s)
          AND freq = '1m'
          AND trade_time::date >= %(start)s
          AND trade_time::date <= %(end)s
        ORDER BY trade_time, ts_code
    """
    with get_conn() as conn:  # type: ignore[attr-defined]
        raw = pd.read_sql(sql, conn, params={"codes": ts_codes, "start": start, "end": end})

    if raw.empty:
        print("原始分钟线表为空")
        return

    raw["trade_time"] = pd.to_datetime(raw["trade_time"])

    merged = pd.merge(
        raw,
        q[["instrument", "trade_time", "$volume", "$amount", "$factor"]],
        left_on=["ts_code", "trade_time"],
        right_on=["instrument", "trade_time"],
        how="inner",
    )

    if merged.empty:
        print("原始与 Qlib 分钟线数据 merge 为空，请检查 ts_code / 日期范围")
        return

    vol_shares = merged["volume_hand"] * 100.0
    vol_expected = vol_shares / merged["$factor"].replace(0, np.nan)

    vol_diff = merged["$volume"] - vol_expected
    vol_rel_err = (vol_diff.abs() / vol_expected.replace(0, np.nan)).dropna()

    print(f"分钟线 volume 样本数: {len(vol_rel_err)}")
    print(f"分钟线 volume 最大相对误差: {vol_rel_err.max():.6g}")
    print(f"分钟线 volume 中位相对误差: {vol_rel_err.median():.6g}")

    if "$amount" in merged.columns and "amount_li" in merged.columns:
        amt_expected = merged["amount_li"] / PRICE_UNIT_DIVISOR
        amt_diff = merged["$amount"] - amt_expected
        amt_rel_err = (amt_diff.abs() / amt_expected.replace(0, np.nan)).dropna()
        print(f"分钟线 amount 样本数: {len(amt_rel_err)}")
        print(f"分钟线 amount 最大相对误差: {amt_rel_err.max():.6g}")
        print(f"分钟线 amount 中位相对误差: {amt_rel_err.median():.6g}")
    else:
        print("分钟线中缺少 $amount 或 amount_li 列，跳过 amount 检查")


def main() -> None:
    # 使用用户指定的测试用例：000001.SZ（平安银行）在 2018-01-04 附近
    ts_codes = ["000001.SZ"]
    daily_start = date(2018, 1, 2)
    daily_end = date(2018, 1, 6)

    minute_start = date(2018, 1, 4)
    minute_end = date(2018, 1, 4)

    check_daily(ts_codes, daily_start, daily_end)
    print()
    check_minute(ts_codes, minute_start, minute_end)


if __name__ == "__main__":
    main()
