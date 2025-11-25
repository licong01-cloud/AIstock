"""DeepAR 训练数据集构建模块（基于 TimescaleDB，本地新程序专用）.

约定：
- 只读现有 TimescaleDB 表/视图：
  - market.kline_daily_qfq: 日级前复权 K 线（价格单位：厘，成交量单位：手）
  - market.kline_5m: 由 1 分钟 K 聚合的 5 分钟 K 线（用于计算 VWAP 等）
  - app.ts_lstm_trade_agg: 高频聚合特征表（freq='5m'）
- 不修改任何旧程序，只在 next_app.backend 体系内新增模块。

本模块的目标是为 DeepAR 提供 **日级** 训练样本，其中可选地包含从 5 分钟 / 高频聚合得到的日级因子：
- intraday_realized_vol: sqrt(sum(realized_vol^2)) over all 5m buckets
- intraday_high_low_ratio: (high - low) / close
- close_vs_vwap: (close - vwap) / vwap，其中 vwap 基于 5m K 线聚合
- intraday_volume_imbalance: (buy_volume - sell_volume) / (buy_volume + sell_volume)
- big_trade_ratio: big_trade_volume / total_volume

返回结果以 trade_date 为索引，方便后续 DeepAR 数据加载和切片。
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
    include_hf_factors: bool = True


def load_deepar_daily_for_symbol(
    ts_code: str,
    start: dt.date,
    end: dt.date,
    config: Optional[DeepARDatasetConfig] = None,
) -> pd.DataFrame:
    """加载单个股票在 [start, end] 区间内的 DeepAR 日级样本.

    返回的 DataFrame 以 trade_date 为索引，基础列包括：
    - open, high, low, close, volume, amount  （单位：元 / 股 / 元）

    若 config.include_hf_factors=True，则额外包含高频因子列：
    - intraday_realized_vol
    - intraday_high_low_ratio
    - close_vs_vwap
    - intraday_volume_imbalance
    - big_trade_ratio
    """

    cfg = config or DeepARDatasetConfig()
    if start > end:
        raise ValueError("start date must be <= end date")

    with get_conn() as conn:
        # 1) 基础日级 K 线（前复权）
        daily_sql = """
            SELECT
              trade_date,
              ts_code,
              open_li,
              high_li,
              low_li,
              close_li,
              volume_hand,
              amount_li
            FROM market.kline_daily_qfq
            WHERE ts_code = %s
              AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
        """
        daily_df = pd.read_sql(
            daily_sql,
            conn,
            params=(ts_code, start, end),
        )

        if daily_df.empty:
            return daily_df

        # 单位换算：厘/手 -> 元/股
        daily_df = daily_df.copy()
        daily_df["open"] = daily_df["open_li"] / 1000.0
        daily_df["high"] = daily_df["high_li"] / 1000.0
        daily_df["low"] = daily_df["low_li"] / 1000.0
        daily_df["close"] = daily_df["close_li"] / 1000.0
        daily_df["volume"] = daily_df["volume_hand"] * 100.0
        daily_df["amount"] = daily_df["amount_li"] / 1000.0
        daily_df = daily_df.drop(
            columns=["open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li"],
        )

        # 2) 高频因子（可选）
        if cfg.include_hf_factors:
            # 2.1 从 app.ts_lstm_trade_agg 聚合日级成交结构
            # 使用半开区间 [start, end+1day) 覆盖完整日内 bucket
            end_plus_one = end + dt.timedelta(days=1)
            agg_sql = """
                SELECT
                  date(bucket_start_time) AS trade_date,
                  symbol                 AS ts_code,
                  SUM(buy_volume)        AS buy_volume,
                  SUM(sell_volume)       AS sell_volume,
                  SUM(neutral_volume)    AS neutral_volume,
                  SUM(big_trade_volume)  AS big_trade_volume,
                  SUM(big_trade_count)   AS big_trade_count,
                  SUM(trade_count)       AS trade_count,
                  SUM(buy_volume + sell_volume + neutral_volume) AS total_volume,
                  sqrt(SUM(realized_vol * realized_vol))        AS intraday_realized_vol
                FROM app.ts_lstm_trade_agg
                WHERE symbol = %s
                  AND freq   = '5m'
                  AND bucket_start_time >= %s
                  AND bucket_start_time < %s
                GROUP BY date(bucket_start_time), symbol
                ORDER BY trade_date
            """
            hf_df = pd.read_sql(
                agg_sql,
                conn,
                params=(ts_code, start, end_plus_one),
            )

            # 2.2 从 market.kline_5m 聚合日级 VWAP 相关量
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
            hf_df = pd.DataFrame()
            k5_df = pd.DataFrame()

    # 3) 派生日级高频因子
    if not hf_df.empty:
        hf_df = hf_df.copy()
        # 成交量不平衡度
        denom = hf_df["buy_volume"] + hf_df["sell_volume"]
        hf_df["intraday_volume_imbalance"] = np.where(
            denom > 0,
            (hf_df["buy_volume"] - hf_df["sell_volume"]) / denom,
            0.0,
        )
        # 大单占比
        total_vol = hf_df["total_volume"].replace(0.0, np.nan)
        hf_df["big_trade_ratio"] = (hf_df["big_trade_volume"] / total_vol).fillna(0.0)

    if not k5_df.empty:
        k5_df = k5_df.copy()
        # vwap = (sum(amount_yuan) / sum(volume_shares))
        #      = (amount_li/1000) / (volume_hand*100) = amount_li / (volume_hand*100000)
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
    hf_df = _normalize_date_col(hf_df, "trade_date") if not hf_df.empty else hf_df
    k5_df = _normalize_date_col(k5_df, "trade_date") if not k5_df.empty else k5_df

    # 4) 合并基础日级 + 高频因子
    merged = daily_df
    if not hf_df.empty:
        merged = merged.merge(
            hf_df[
                [
                    "trade_date",
                    "buy_volume",
                    "sell_volume",
                    "neutral_volume",
                    "big_trade_volume",
                    "big_trade_count",
                    "trade_count",
                    "total_volume",
                    "intraday_realized_vol",
                    "intraday_volume_imbalance",
                    "big_trade_ratio",
                ]
            ],
            on="trade_date",
            how="left",
        )
    if not k5_df.empty:
        merged = merged.merge(
            k5_df[["trade_date", "vwap"]],
            on="trade_date",
            how="left",
        )

    # 5) 基于基础日线和 vwap 计算 intraday_high_low_ratio / close_vs_vwap
    merged["intraday_high_low_ratio"] = (merged["high"] - merged["low"]) / merged["close"]
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

    若 config.include_hf_factors=True，则额外从 5m 高频聚合表按 60m 聚合：
    - buy_volume / sell_volume / neutral_volume / total_volume
    - order_flow_imbalance（基于 60m 聚合量）
    - big_trade_volume / big_trade_count / big_trade_ratio
    - realized_vol：sqrt(sum(realized_vol_5m^2)) over 60m
    - trade_count, avg_trade_size, intensity（按 60 分钟归一）
    """

    cfg = config or DeepARDatasetConfig()
    if start >= end:
        raise ValueError("start must be < end")

    with get_conn() as conn:
        # 1) 60 分钟聚合 K 线
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

        # 2) 高频特征按 60m 聚合（可选）
        if cfg.include_hf_factors:
            agg_sql = """
                SELECT
                  time_bucket('60 minutes', bucket_start_time) AS ts,
                  symbol                                     AS ts_code,
                  SUM(buy_volume)                            AS buy_volume,
                  SUM(sell_volume)                           AS sell_volume,
                  SUM(neutral_volume)                        AS neutral_volume,
                  SUM(big_trade_volume)                      AS big_trade_volume,
                  SUM(big_trade_count)                       AS big_trade_count,
                  SUM(trade_count)                           AS trade_count,
                  SUM(buy_volume + sell_volume + neutral_volume) AS total_volume,
                  sqrt(SUM(realized_vol * realized_vol))     AS realized_vol_60m
                FROM app.ts_lstm_trade_agg
                WHERE symbol = %s
                  AND freq = '5m'
                  AND bucket_start_time >= %s
                  AND bucket_start_time < %s
                GROUP BY time_bucket('60 minutes', bucket_start_time), symbol
                ORDER BY ts
            """
            hf_df = pd.read_sql(
                agg_sql,
                conn,
                params=(ts_code, start, end),
            )
        else:
            hf_df = pd.DataFrame()

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

    if not hf_df.empty:
        hf_df = hf_df.copy()
        # 计算 60m 成交量不平衡度、大单占比等
        denom = hf_df["buy_volume"] + hf_df["sell_volume"]
        hf_df["intraday_volume_imbalance"] = np.where(
            denom > 0,
            (hf_df["buy_volume"] - hf_df["sell_volume"]) / denom,
            0.0,
        )

        total_vol = hf_df["total_volume"].replace(0.0, np.nan)
        hf_df["big_trade_ratio"] = (hf_df["big_trade_volume"] / total_vol).fillna(0.0)

        hf_df["realized_vol"] = hf_df["realized_vol_60m"]
        hf_df = hf_df.drop(columns=["realized_vol_60m"])

        hf_df["avg_trade_size"] = np.where(
            hf_df["trade_count"] > 0,
            hf_df["total_volume"] / hf_df["trade_count"],
            0.0,
        )
        hf_df["intensity"] = hf_df["trade_count"] / 60.0

        # 与 K 线按照 ts+ts_code 左连接
        merged = pd.merge(
            k60_df,
            hf_df[
                [
                    "ts",
                    "ts_code",
                    "buy_volume",
                    "sell_volume",
                    "neutral_volume",
                    "big_trade_volume",
                    "big_trade_count",
                    "trade_count",
                    "total_volume",
                    "intraday_volume_imbalance",
                    "big_trade_ratio",
                    "realized_vol",
                    "avg_trade_size",
                    "intensity",
                ]
            ],
            on=["ts", "ts_code"],
            how="left",
        )
    else:
        merged = k60_df

    merged = merged.set_index("ts").sort_index()
    return merged


__all__ = [
    "DeepARDatasetConfig",
    "load_deepar_daily_for_symbol",
    "load_deepar_60m_for_symbol",
]
