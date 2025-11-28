"""填充 Universe 配置与静态特征表的辅助脚本（新程序专用）。

与 `docs/quant_model_evaluation.md` 第 6 章保持一致：
- 初始化 app.model_universe_config 中的若干默认 Universe：
  - ALL_EQ_CLEAN：全市场合格 Universe（具体过滤规则记录在 config_json 中，占位即可）；
  - CORE_UNIVERSE：基于自选池（watchlist）得到的 Universe，占位配置；
- 基于 TimescaleDB 中的 5 分钟 K 线表 `market.kline_5m`，构造静态特征：
  - 以近 N 日（默认 60 日）为观察窗口；
  - 对每只股票计算：
    - 日均成交额（amount）；
    - 日收益率波动率（基于收盘价 log return）；
  - 按截面分位数将上述指标离散化为
    - size_bucket（市值/规模近似，基于日均成交额）；
    - volatility_bucket（波动水平，基于收益率波动率）；
    - liquidity_bucket（流动性，亦可基于成交额分位）；
- 结果写入 `app.stock_static_features`，不覆盖行业字段（后续可由其它脚本填充）。

注意：
- 本脚本只负责写入/更新元数据表，不做任何模型训练或推理；
- 所有写操作仅发生在 app.model_universe_config / app.stock_static_features 中，
  不修改旧 schema 或其它业务表。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

from .pg_pool import get_conn


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed Universe config and static features")
    parser.add_argument(
        "--as-of-date",
        type=str,
        default=None,
        help="静态特征观测日期（YYYY-MM-DD），默认使用当前日期（UTC）",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=60,
        help="计算静态特征的观察窗口长度（按自然日近 N 日）",
    )
    return parser.parse_args()


def _today_utc_date() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


# ---------------------------------------------------------------------------
# Universe 配置
# ---------------------------------------------------------------------------


def _ensure_default_universe_configs() -> None:
    """向 app.model_universe_config 中写入若干默认配置（若不存在）。"""

    defaults = [
        {
            "universe_name": "ALL_EQ_CLEAN",
            "description": "全市场合格 Universe：全量 A 股剔除垃圾股（ST/退市/长停牌/极低流动性等）",
            "config_json": {
                "source": "ALL_EQ_CLEAN",
                # 具体过滤规则占位，可在后续脚本或 UI 中细化：
                # - min_avg_turnover, max_suspension_days, exclude_st, etc.
            },
        },
        {
            "universe_name": "CORE_UNIVERSE",
            "description": "CoreUniverse：基于自选池（watchlist）定义的股票集合",
            "config_json": {
                "source": "CORE_UNIVERSE",
                # categories: 可在后续通过 UI 或手工设定，如 ["核心持仓", "重点跟踪"]
            },
        },
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in defaults:
                cur.execute(
                    """
                    INSERT INTO app.model_universe_config (universe_name, description, config_json, enabled)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (universe_name)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        config_json = EXCLUDED.config_json,
                        enabled = TRUE,
                        updated_at = NOW()
                    """,
                    (
                        item["universe_name"],
                        item["description"],
                        json_dumps(item["config_json"]),
                    ),
                )


def _ensure_default_model_configs() -> None:
    """向 app.model_config 中写入 ARIMA/HMM 的默认配置（若不存在）。"""

    defaults = [
        {
            "model_name": "ARIMA_DAILY",
            "description": "ARIMA 日级基线预测（ALL_EQ_CLEAN Universe）",
            "config_json": {
                "kind": "arima_daily",
                "params": {
                    "freq": "1d",
                    "history_years": 3.0,
                    "universe_name": "ALL_EQ_CLEAN",
                    "order": [1, 1, 1],
                    "seasonal_order": None,
                },
            },
        },
        {
            "model_name": "HMM_DAILY",
            "description": "HMM 日级行情状态识别（ALL_EQ_CLEAN Universe）",
            "config_json": {
                "kind": "hmm_daily",
                "params": {
                    "freq": "1d",
                    "history_years": 3.0,
                    "universe_name": "ALL_EQ_CLEAN",
                    "n_states": 3,
                },
            },
        },
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in defaults:
                cur.execute(
                    """
                    INSERT INTO app.model_config (model_name, description, config_json, enabled)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (model_name)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        config_json = EXCLUDED.config_json,
                        enabled = TRUE,
                        updated_at = NOW()
                    """,
                    (
                        item["model_name"],
                        item["description"],
                        json_dumps(item["config_json"]),
                    ),
                )


def json_dumps(payload: Any) -> str:
    """Helper for JSON serialization with UTF-8 support."""

    return json.dumps(payload, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 静态特征填充（基于 5 分钟 K 线聚合日度指标）
# ---------------------------------------------------------------------------


def _load_daily_aggregates(
    start_dt: dt.datetime,
    end_dt: dt.datetime,
) -> pd.DataFrame:
    """从 market.kline_5m 聚合出日度成交/价格数据.

    返回列：
    - ts_code
    - trade_date (date)
    - close_li (当日最后一个 5m bar 的收盘价，厘)
    - amount_li (当日成交额总和，厘)
    """

    with get_conn() as conn:
        sql = """
            SELECT
              ts_code,
              DATE(bucket) AS trade_date,
              MAX(bucket) AS last_bucket,
              SUM(amount_li) AS amount_li_sum
            FROM market.kline_5m
            WHERE bucket >= %s
              AND bucket < %s
            GROUP BY ts_code, DATE(bucket)
        """
        df = pd.read_sql(sql, conn, params=(start_dt, end_dt))

        if df.empty:
            return df

        # 获取每日最后一个 5m bar 的收盘价（厘）
        # 这里再次查询以获取对应 close_li
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code, bucket, close_li
                  FROM market.kline_5m
                 WHERE bucket >= %s
                   AND bucket < %s
                """,
                (start_dt, end_dt),
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        tick_df = pd.DataFrame(rows, columns=["ts_code", "bucket", "close_li"])
        tick_df["bucket"] = pd.to_datetime(tick_df["bucket"])

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    tick_df["date"] = tick_df["bucket"].dt.date

    # 取每天最后一个 bucket 的 close_li
    tick_df = tick_df.sort_values(["ts_code", "bucket"])
    last_close = tick_df.groupby(["ts_code", "date"]).tail(1)[
        ["ts_code", "date", "close_li"]
    ]
    last_close = last_close.rename(columns={"date": "trade_date"})

    merged = pd.merge(
        df,
        last_close,
        on=["ts_code", "trade_date"],
        how="inner",
    )
    merged = merged.rename(columns={"amount_li_sum": "amount_li"})
    return merged


def _build_static_features(
    daily_df: pd.DataFrame,
) -> pd.DataFrame:
    """根据日度聚合数据构建截面静态特征并分桶.

    输入：
    - daily_df: 包含 ts_code, trade_date, close_li, amount_li

    输出：
    - stat_df: Index=ts_code, 列包括：
      - avg_amount (日均成交额)
      - ret_std   (收益率波动率)
      - size_bucket / volatility_bucket / liquidity_bucket（字符串标签）
    """

    if daily_df.empty:
        return pd.DataFrame(columns=[
            "ts_code",
            "avg_amount",
            "ret_std",
            "size_bucket",
            "volatility_bucket",
            "liquidity_bucket",
        ]).set_index("ts_code")

    df = daily_df.copy()

    # 价格/金额单位换算（厘 -> 元），仅用于可读性；分桶结果与是否换算无关
    df["close"] = df["close_li"] / 1000.0
    df["amount"] = df["amount_li"] / 1000.0

    df = df.sort_values(["ts_code", "trade_date"])
    df["prev_close"] = df.groupby("ts_code")["close"].shift(1)
    df["log_ret"] = np.log(df["close"] / df["prev_close"]).replace([np.inf, -np.inf], np.nan)

    grouped = df.groupby("ts_code")

    stats = pd.DataFrame({
        "avg_amount": grouped["amount"].mean(),
        "ret_std": grouped["log_ret"].std(),
    })

    # 以 avg_amount 作为规模/流动性 proxy，以 ret_std 作为波动 proxy
    def _bucket_by_quantiles(series: pd.Series, labels: Tuple[str, ...]) -> pd.Series:
        if series.dropna().empty:
            return pd.Series(index=series.index, data=labels[len(labels) // 2])
        quantiles = series.quantile([0.25, 0.5, 0.75]).values
        q1, q2, q3 = quantiles

        def _map(v: float) -> str:
            if np.isnan(v):
                return labels[len(labels) // 2]
            if v <= q1:
                return labels[0]
            if v <= q2:
                return labels[1]
            if v <= q3:
                return labels[2]
            return labels[3]

        return series.apply(_map)

    size_labels = ("S", "M", "L", "XL")
    vol_labels = ("LOW", "MID", "HIGH", "VERY_HIGH")

    stats["size_bucket"] = _bucket_by_quantiles(stats["avg_amount"], size_labels)
    stats["volatility_bucket"] = _bucket_by_quantiles(stats["ret_std"], vol_labels)
    # 这里 liquidity_bucket 先与 size_bucket 保持一致，后续可根据更多微观指标细化
    stats["liquidity_bucket"] = stats["size_bucket"]

    stats = stats.reset_index()
    stats["ts_code"] = stats["ts_code"].astype(str)
    stats = stats.set_index("ts_code")
    return stats


def _upsert_static_features(
    as_of_date: dt.date,
    stat_df: pd.DataFrame,
) -> int:
    """将静态特征写入 app.stock_static_features，返回写入条数."""

    if stat_df.empty:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for ts_code, row in stat_df.iterrows():
                size_bucket = row.get("size_bucket")
                vol_bucket = row.get("volatility_bucket")
                liq_bucket = row.get("liquidity_bucket")

                cur.execute(
                    """
                    INSERT INTO app.stock_static_features (
                        ts_code,
                        as_of_date,
                        industry,
                        sub_industry,
                        size_bucket,
                        volatility_bucket,
                        liquidity_bucket,
                        extra_json
                    )
                    VALUES (%s, %s, NULL, NULL, %s, %s, %s, NULL)
                    ON CONFLICT (ts_code, as_of_date)
                    DO UPDATE SET
                        size_bucket = EXCLUDED.size_bucket,
                        volatility_bucket = EXCLUDED.volatility_bucket,
                        liquidity_bucket = EXCLUDED.liquidity_bucket,
                        updated_at = NOW()
                    """,
                    (ts_code, as_of_date, size_bucket, vol_bucket, liq_bucket),
                )
    return int(stat_df.shape[0])


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else _today_utc_date()
    lookback_days = int(args.lookback_days)

    print(f"[INFO] seeding model_universe_config (default entries)")
    _ensure_default_universe_configs()
    print(f"[INFO] seeding model_config (ARIMA/HMM defaults)")
    _ensure_default_model_configs()

    # 计算日度观察窗口
    end_dt = dt.datetime.combine(as_of_date + dt.timedelta(days=1), dt.time(0, 0), tzinfo=dt.timezone.utc)
    start_dt = end_dt - dt.timedelta(days=lookback_days)

    print(f"[INFO] loading daily aggregates from {start_dt} ~ {end_dt} (UTC)")
    daily_df = _load_daily_aggregates(start_dt, end_dt)
    if daily_df.empty:
        print("[WARN] no kline_5m data found for static feature window; skip static seeding")
        return

    print(f"[INFO] building static features for {daily_df['ts_code'].nunique()} symbols")
    stat_df = _build_static_features(daily_df)

    print(f"[INFO] upserting stock_static_features for as_of_date={as_of_date}")
    n_rows = _upsert_static_features(as_of_date, stat_df)
    print(f"[INFO] done, upserted rows={n_rows}")


if __name__ == "__main__":
    main()
