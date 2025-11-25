"""LSTM 训练数据集构建模块（基于 TimescaleDB，本地新程序专用）.

约定：
- 只读现有 TimescaleDB 表/视图：
  - market.kline_5m: 由 1 分钟 K 聚合的 5 分钟行情（价格单位：厘，成交量单位：手）
  - app.ts_lstm_trade_agg: 高频聚合特征表（freq='5m'）
  - app.watchlist_items 及 watchlist_categories/item_categories: CoreUniverse 定义
- 不修改任何旧程序，只在 next_app.backend 体系内新增模块。

本模块提供两层 API：
- 获取 Universe：CoreUniverse（自选池，可按分类过滤）/ SharedUniverse（待后续扩展）
- 对单个 ts_code 构建 5 分钟级别的时序特征 DataFrame，用于后续 LSTM 训练/推理。

后续 LSTM 训练脚本可直接依赖本模块，而不需要直接访问数据库。
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd

from ..db.pg_pool import get_conn


@dataclass
class LSTMDatasetConfig:
    """LSTM 数据集配置（用于说明/日志，训练脚本可复用此配置对象）."""

    freq: str = "5m"
    price_unit: str = "yuan"  # 从厘换算到元
    volume_unit: str = "share"  # 从手换算到股
    include_trade_agg: bool = True


def get_core_universe(categories: Optional[Iterable[str]] = None) -> List[str]:
    """获取 CoreUniverse（自选池 ts_code 列表）.

    参数
    ------
    categories:
        可选的分类名称列表（如 ["持仓股票", "核心持仓"]）。
        - None 或空：返回所有 `app.watchlist_items` 中的 code 作为 ts_code；
        - 非空：通过 watchlist_item_categories + watchlist_categories 过滤。
    """

    cats = [c for c in (categories or []) if c]
    with get_conn() as conn:
        with conn.cursor() as cur:
            if not cats:
                cur.execute(
                    """
                    SELECT code
                      FROM app.watchlist_items
                     ORDER BY code
                    """
                )
                rows = cur.fetchall()
                return [r[0] for r in rows]

            placeholders = ",".join(["%s"] * len(cats))
            sql = f"""
                SELECT DISTINCT wi.code
                  FROM app.watchlist_items wi
                  JOIN app.watchlist_item_categories wic
                    ON wic.item_id = wi.id
                  JOIN app.watchlist_categories wc
                    ON wc.id = wic.category_id
                 WHERE wc.name IN ({placeholders})
                 ORDER BY wi.code
            """
            cur.execute(sql, tuple(cats))
            rows = cur.fetchall()
            return [r[0] for r in rows]


def load_lstm_timeseries_for_symbol(
    ts_code: str,
    start: dt.datetime,
    end: dt.datetime,
    config: Optional[LSTMDatasetConfig] = None,
) -> pd.DataFrame:
    """加载单个股票在指定时间区间内的 5 分钟级别时序特征.

    返回的 DataFrame 以时间为索引，列包括：
    - 来自 market.kline_5m 的 K 线特征：
      - open, high, low, close, volume, amount
    - 来自 app.ts_lstm_trade_agg 的高频特征（如已启用）：
      - buy_volume, sell_volume, neutral_volume, order_flow_imbalance,
        big_trade_volume, big_trade_count, big_trade_ratio,
        realized_vol, trade_count, avg_trade_size, intensity

    单位换算：
    - 价格：厘 -> 元（/1000.0）
    - 成交量：手 -> 股（*100）
    - 成交额：厘 -> 元（/1000.0）
    """

    cfg = config or LSTMDatasetConfig()
    if cfg.freq != "5m":  # 当前仅实现 5m，后续可扩展到 15m/60m
        raise ValueError(f"only freq='5m' is supported for now, got {cfg.freq!r}")

    # 为避免时区混乱，这里统一使用 >= start AND < end 的半开区间
    with get_conn() as conn:
        # 1) 读取 5 分钟聚合 K 线
        kline_sql = """
            SELECT
              bucket                AS ts,
              ts_code,
              open_li,
              high_li,
              low_li,
              close_li,
              volume_hand,
              amount_li
            FROM market.kline_5m
            WHERE ts_code = %s
              AND bucket >= %s
              AND bucket < %s
            ORDER BY ts
        """
        kline_df = pd.read_sql(
            kline_sql,
            conn,
            params=(ts_code, start, end),
        )

        if kline_df.empty:
            return kline_df

        # 2) 读取对应时间区间的高频聚合特征
        if cfg.include_trade_agg:
            agg_sql = """
                SELECT
                  bucket_start_time,
                  symbol,
                  freq,
                  buy_volume,
                  sell_volume,
                  neutral_volume,
                  order_flow_imbalance,
                  big_trade_volume,
                  big_trade_count,
                  big_trade_ratio,
                  realized_vol,
                  trade_count,
                  avg_trade_size,
                  intensity
                FROM app.ts_lstm_trade_agg
                WHERE symbol = %s
                  AND freq = '5m'
                  AND bucket_start_time >= %s
                  AND bucket_start_time < %s
                ORDER BY bucket_start_time
            """
            agg_df = pd.read_sql(
                agg_sql,
                conn,
                params=(ts_code, start, end),
            )
        else:
            agg_df = pd.DataFrame()

    # 单位换算 & 重命名列
    # K 线（厘/手 -> 元/股）
    kline_df = kline_df.copy()
    kline_df["open"] = kline_df["open_li"] / 1000.0
    kline_df["high"] = kline_df["high_li"] / 1000.0
    kline_df["low"] = kline_df["low_li"] / 1000.0
    kline_df["close"] = kline_df["close_li"] / 1000.0
    kline_df["volume"] = kline_df["volume_hand"] * 100.0
    kline_df["amount"] = kline_df["amount_li"] / 1000.0
    kline_df = kline_df.drop(columns=["open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li"])

    # 高频聚合特征
    if not agg_df.empty:
        agg_df = agg_df.copy()
        agg_df = agg_df.rename(
            columns={
                "bucket_start_time": "ts",
                "symbol": "ts_code",
            }
        )
        # 与 K 线按照 ts+ts_code 左连接
        merged = pd.merge(
            kline_df,
            agg_df,
            how="left",
            on=["ts", "ts_code"],
        )
    else:
        merged = kline_df

    merged = merged.set_index("ts").sort_index()
    return merged


__all__ = [
    "LSTMDatasetConfig",
    "get_core_universe",
    "load_lstm_timeseries_for_symbol",
]
