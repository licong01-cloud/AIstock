"""DeepAR 训练数据集构建模块（基于 TimescaleDB，本地新程序专用）.

约定：
- 只读现有 TimescaleDB 表/视图：
  - market.kline_daily_raw: 未复权日线（价格单位：厘，成交量单位：手）
  - market.adj_factor: Tushare 复权因子
  - market.kline_5m: 由 1 分钟 K 聚合的 5 分钟 K 线（用于计算 VWAP 等）
  - market.kline_60m: 60 分钟聚合 K 线
- 不修改任何旧程序，只在 next_app.backend 体系内新增模块。

日级价格通过 kline_daily_raw + adj_factor 实时计算前复权：
  qfq_factor = adj_factor / max(adj_factor)  （每只股票以最新日期为基准）
  price_yuan = raw_li / 1000.0 × qfq_factor

返回结果以 trade_date / ts 为索引，方便后续 DeepAR 数据加载和切片。
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from ..db.pg_pool import get_conn


@dataclass
class DeepARDatasetConfig:
    """DeepAR 日级数据集配置."""

    history_years: float = 3.0
    include_hf_factors: bool = False


def load_deepar_daily_for_symbol(
    ts_code: str,
    start: dt.date,
    end: dt.date,
    config: Optional[DeepARDatasetConfig] = None,
) -> pd.DataFrame:
    """加载单个股票在 [start, end] 区间内的 DeepAR 日级样本.

    使用 kline_daily_raw + adj_factor 实时计算前复权价格。

    返回的 DataFrame 以 trade_date 为索引，基础列包括：
    - open, high, low, close, volume, amount  （单位：元 / 股 / 元）

    若 config.include_hf_factors=True，则额外包含：
    - intraday_high_low_ratio
    - close_vs_vwap
    """

    cfg = config or DeepARDatasetConfig()
    if start > end:
        raise ValueError("start date must be <= end date")

    with get_conn() as conn:
        # 1) 未复权日线 + 复权因子，实时计算前复权价格
        daily_sql = """
            WITH adj AS (
                SELECT ts_code, trade_date, adj_factor,
                       MAX(adj_factor) OVER (PARTITION BY ts_code) AS adj_latest
                FROM market.adj_factor
                WHERE ts_code = %s
                  AND trade_date BETWEEN %s AND %s
            )
            SELECT
              r.trade_date,
              r.ts_code,
              r.open_li,
              r.high_li,
              r.low_li,
              r.close_li,
              r.volume_hand,
              r.amount_li,
              COALESCE(a.adj_factor / NULLIF(a.adj_latest, 0), 1.0) AS qfq_factor
            FROM market.kline_daily_raw r
            LEFT JOIN adj a
              ON a.ts_code = r.ts_code AND a.trade_date = r.trade_date
            WHERE r.ts_code = %s
              AND r.trade_date BETWEEN %s AND %s
            ORDER BY r.trade_date
        """
        daily_df = pd.read_sql(
            daily_sql,
            conn,
            params=(ts_code, start, end, ts_code, start, end),
        )

        if daily_df.empty:
            return daily_df

        # 单位换算：厘/手 -> 元/股，价格乘以前复权因子
        daily_df = daily_df.copy()
        qfq = daily_df["qfq_factor"]
        daily_df["open"] = daily_df["open_li"] / 1000.0 * qfq
        daily_df["high"] = daily_df["high_li"] / 1000.0 * qfq
        daily_df["low"] = daily_df["low_li"] / 1000.0 * qfq
        daily_df["close"] = daily_df["close_li"] / 1000.0 * qfq
        daily_df["volume"] = daily_df["volume_hand"] * 100.0
        daily_df["amount"] = daily_df["amount_li"] / 1000.0
        daily_df = daily_df.drop(
            columns=["open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li", "qfq_factor"],
        )

        # 2) 可选：从 kline_5m 聚合日级 VWAP
        if cfg.include_hf_factors:
            end_plus_one = end + dt.timedelta(days=1)
            k5_sql = """
                SELECT
                  date(bucket) AS trade_date,
                  ts_code,
                  SUM(volume_hand) AS vol_hand_5m,
                  SUM(amount_li)   AS amount_li_5m
                FROM market.kline_5m
                WHERE ts_code = %s
                  AND bucket >= %s
                  AND bucket < %s
                GROUP BY date(bucket), ts_code
                ORDER BY trade_date
            """
            k5_df = pd.read_sql(
                k5_sql,
                conn,
                params=(ts_code, start, end_plus_one),
            )
        else:
            k5_df = pd.DataFrame()

    if not k5_df.empty:
        k5_df = k5_df.copy()
        denom = k5_df["vol_hand_5m"] * 100000.0
        k5_df["vwap"] = np.where(denom > 0, k5_df["amount_li_5m"] / denom, np.nan)

    # 把所有 DataFrame 的 trade_date 统一为 datetime.date
    def _normalize_date_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df[col] = pd.to_datetime(df[col]).dt.date
        return df

    daily_df = _normalize_date_col(daily_df, "trade_date")
    k5_df = _normalize_date_col(k5_df, "trade_date") if not k5_df.empty else k5_df

    # 合并基础日级 + VWAP
    merged = daily_df
    if not k5_df.empty:
        merged = merged.merge(
            k5_df[["trade_date", "vwap"]],
            on="trade_date",
            how="left",
        )

    # 基于基础日线计算 intraday_high_low_ratio（始终可用）
    merged["intraday_high_low_ratio"] = (merged["high"] - merged["low"]) / merged["close"]

    # close_vs_vwap 仅在有 VWAP 数据时计算
    if cfg.include_hf_factors and not k5_df.empty:
        merged["close_vs_vwap"] = np.where(
            merged["vwap"].notna() & (merged["vwap"] != 0),
            (merged["close"] - merged["vwap"]) / merged["vwap"],
            np.nan,
        )

    merged = merged.set_index("trade_date").sort_index()
    return merged


def load_deepar_60m_for_symbol(
    ts_code: str,
    start: dt.datetime,
    end: dt.datetime,
    config: Optional[DeepARDatasetConfig] = None,
) -> pd.DataFrame:
    """加载单个股票在 [start, end) 区间内的 60 分钟 DeepAR 样本.

    基础列与日级类似，但时间索引为 60m bucket：
    - open, high, low, close, volume, amount  （单位：元 / 股 / 元）
    """

    if start >= end:
        raise ValueError("start must be < end")

    with get_conn() as conn:
        k60_sql = """
            SELECT
              bucket      AS ts,
              ts_code,
              open_li,
              high_li,
              low_li,
              close_li,
              volume_hand,
              amount_li
            FROM market.kline_60m
            WHERE ts_code = %s
              AND bucket >= %s
              AND bucket < %s
            ORDER BY ts
        """
        k60_df = pd.read_sql(
            k60_sql,
            conn,
            params=(ts_code, start, end),
        )

        if k60_df.empty:
            return k60_df

    # 单位换算：厘/手 -> 元/股
    k60_df = k60_df.copy()
    k60_df["open"] = k60_df["open_li"] / 1000.0
    k60_df["high"] = k60_df["high_li"] / 1000.0
    k60_df["low"] = k60_df["low_li"] / 1000.0
    k60_df["close"] = k60_df["close_li"] / 1000.0
    k60_df["volume"] = k60_df["volume_hand"] * 100.0
    k60_df["amount"] = k60_df["amount_li"] / 1000.0
    k60_df = k60_df.drop(
        columns=["open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li"],
    )

    k60_df = k60_df.set_index("ts").sort_index()
    return k60_df


__all__ = [
    "DeepARDatasetConfig",
    "load_deepar_daily_for_symbol",
    "load_deepar_60m_for_symbol",
]
