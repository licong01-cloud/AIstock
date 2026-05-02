"""Read-only sector factor RankIC diagnostics for HMM feature selection.

The script converts a shortlist of high-IC stock-factor ideas into sector-level
analogues, then evaluates their forward sector-return RankIC and top-bottom
spread. It only reads local PostgreSQL market/factor tables and writes CSV/MD
artifacts under the requested output directory.
"""
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import psycopg2
from scipy.stats import spearmanr


DB_DEFAULT = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "aistock",
    "user": "postgres",
    "password": "lc78080808",
}


@dataclass(frozen=True)
class SourceFactor:
    factor_name: str
    category: str
    stock_rank_ic: float | None
    best_horizon: int | None
    sector_features: tuple[str, ...]
    migration_note: str


SOURCE_FACTORS = [
    SourceFactor(
        "neg_mf_main_net_amt_std_5d",
        "MF",
        0.056991918060384575,
        20,
        ("sf_mf_net_ratio_std_5d_neg", "sf_flow_stability_5d"),
        "main-money-flow stability; lower flow noise is expected to be better",
    ),
    SourceFactor(
        "m_turnover_mf_divergence",
        "MF/LIQ",
        0.04418027468861088,
        20,
        ("sf_turnover_mf_divergence_10d",),
        "low turnover crowding multiplied by large/super-large inflow rank",
    ),
    SourceFactor(
        "dynamic_flow_volatility_sentiment",
        "MF/VOL",
        0.05184436940576335,
        20,
        ("sf_dynamic_flow_vol_sentiment",),
        "super-large-flow volatility times sector turnover and value residual",
    ),
    SourceFactor(
        "small_order_flow_intensity",
        "MF",
        0.04517632386303361,
        20,
        ("sf_small_buy_intensity_5d", "sf_small_net_ratio_5d"),
        "small-order participation / small net flow at sector level",
    ),
    SourceFactor(
        "m_free_turnover_ind_neutral",
        "LIQ",
        0.05412548874201595,
        20,
        ("sf_turnover_pctile_250d_neg", "sf_turnover_zscore_60d_neg"),
        "sector-level crowding and turnover percentile reversal",
    ),
    SourceFactor(
        "m_ind_rel_turnover",
        "LIQ",
        0.04269473335840697,
        20,
        ("sf_turnover_pctile_120d_neg", "sf_turnover_ma5_ma20_neg"),
        "sector turnover relative to its own recent history",
    ),
    SourceFactor(
        "m_intraday_range_ratio_5d",
        "VOL",
        0.0589091378690813,
        20,
        ("sf_intraday_range_5d_neg",),
        "low sector intraday range / volatility compression",
    ),
    SourceFactor(
        "m_atr_percentile_250d",
        "VOL/STAT",
        0.05497249135015386,
        20,
        ("sf_atr14_pctile_250d_neg",),
        "sector ATR percentile compression",
    ),
    SourceFactor(
        "m_sw2_vol_ratio_to_sector",
        "VOL",
        -0.047654572210269204,
        20,
        ("sf_range_vs_market_10d", "sf_vol_vs_market_20d"),
        "sector volatility relative to cross-sector market median",
    ),
    SourceFactor(
        "m_max_return_20d",
        "MOM",
        0.05356868774069221,
        20,
        ("sf_max_ret_20d_neg",),
        "lottery-like max daily return reversal at sector level",
    ),
    SourceFactor(
        "m_mom_weighted_strength_20d",
        "MOM",
        -0.0532480691936374,
        20,
        ("sf_amount_weighted_mom_20d",),
        "amount-weighted sector momentum; direction is selected in train split",
    ),
    SourceFactor(
        "sector_breadth_extension",
        "BREADTH",
        None,
        None,
        (
            "sf_breadth_1d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_median_stock_ret_5d",
            "sf_dispersion_5d_neg",
        ),
        "sector breadth/dispersion extension from constituents",
    ),
]


def db_connect():
    return psycopg2.connect(**DB_DEFAULT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2024-01-01", help="Evaluation start date.")
    parser.add_argument("--end", default="2026-04-30", help="Evaluation end date.")
    parser.add_argument(
        "--prestart",
        default="2023-01-01",
        help="Data fetch start date for rolling-window warmup.",
    )
    parser.add_argument(
        "--test-start",
        default="2025-05-01",
        help="Holdout start date used to choose direction from the train split.",
    )
    parser.add_argument(
        "--output-dir",
        default=".codex_tmp/sector_factor_rankic_20260502",
        help="Directory for CSV artifacts.",
    )
    parser.add_argument(
        "--report",
        default="docs/analysis/hmm_sector_factor_rankic_validation_20260502.md",
        help="Markdown report path.",
    )
    return parser.parse_args()


def read_sql(conn, sql: str, params: dict[str, object]) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def load_sector_daily(conn, prestart: str, end: str) -> pd.DataFrame:
    sql = """
        SELECT
            trade_date,
            ts_code AS sector_code,
            name AS sector_name,
            open::float8 AS open,
            high::float8 AS high,
            low::float8 AS low,
            close::float8 AS close,
            pct_change::float8 AS pct_change,
            vol::float8 AS vol,
            amount::float8 AS amount,
            pe::float8 AS pe,
            pb::float8 AS pb,
            total_mv::float8 AS total_mv,
            float_mv::float8 AS float_mv
        FROM market.sw_daily
        WHERE trade_date BETWEEN %(prestart)s AND %(end)s
        ORDER BY trade_date, ts_code
    """
    df = read_sql(conn, sql, {"prestart": prestart, "end": end})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def load_sector_moneyflow(conn, prestart: str, end: str) -> pd.DataFrame:
    sql = """
        WITH members AS (
            SELECT DISTINCT ON (ts_code)
                ts_code,
                l2_code AS sector_code
            FROM market.sw_index_member
            WHERE out_date IS NULL
            ORDER BY ts_code, in_date DESC NULLS LAST
        )
        SELECT
            sd.trade_date,
            m.sector_code,
            AVG(sd.sw2_amount)::float8 AS sw2_amount,
            AVG(sd.sw2_total_mv)::float8 AS sw2_total_mv,
            AVG(sd.sw2_mf_buy_sm_amt)::float8 AS mf_buy_sm_amt,
            AVG(sd.sw2_mf_sell_sm_amt)::float8 AS mf_sell_sm_amt,
            AVG(sd.sw2_mf_buy_md_amt)::float8 AS mf_buy_md_amt,
            AVG(sd.sw2_mf_sell_md_amt)::float8 AS mf_sell_md_amt,
            AVG(sd.sw2_mf_buy_lg_amt)::float8 AS mf_buy_lg_amt,
            AVG(sd.sw2_mf_sell_lg_amt)::float8 AS mf_sell_lg_amt,
            AVG(sd.sw2_mf_buy_elg_amt)::float8 AS mf_buy_elg_amt,
            AVG(sd.sw2_mf_sell_elg_amt)::float8 AS mf_sell_elg_amt,
            AVG(sd.sw2_mf_net_amt)::float8 AS mf_net_amt,
            COUNT(*) AS mapped_stock_rows
        FROM market.sector_data sd
        JOIN members m ON m.ts_code = sd.ts_code
        WHERE sd.trade_date BETWEEN %(prestart)s AND %(end)s
        GROUP BY sd.trade_date, m.sector_code
        ORDER BY sd.trade_date, m.sector_code
    """
    df = read_sql(conn, sql, {"prestart": prestart, "end": end})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def load_sector_breadth(conn, prestart: str, end: str) -> pd.DataFrame:
    sql = """
        WITH members AS (
            SELECT DISTINCT ON (ts_code)
                ts_code,
                l2_code AS sector_code
            FROM market.sw_index_member
            WHERE out_date IS NULL
            ORDER BY ts_code, in_date DESC NULLS LAST
        ),
        stock_daily AS (
            SELECT
                db.trade_date,
                db.ts_code,
                m.sector_code,
                db.close::float8 AS close
            FROM market.daily_basic db
            JOIN members m ON m.ts_code = db.ts_code
            WHERE db.trade_date BETWEEN %(prestart)s AND %(end)s
              AND db.close IS NOT NULL
              AND db.close > 0
        ),
        stock_ret AS (
            SELECT
                trade_date,
                sector_code,
                close / NULLIF(LAG(close, 1) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1 AS ret_1d,
                close / NULLIF(LAG(close, 5) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1 AS ret_5d,
                close / NULLIF(LAG(close, 10) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1 AS ret_10d
            FROM stock_daily
        )
        SELECT
            trade_date,
            sector_code,
            AVG(CASE WHEN ret_1d > 0 THEN 1.0 WHEN ret_1d <= 0 THEN 0.0 ELSE NULL END)::float8 AS breadth_1d,
            AVG(CASE WHEN ret_5d > 0 THEN 1.0 WHEN ret_5d <= 0 THEN 0.0 ELSE NULL END)::float8 AS breadth_5d,
            AVG(CASE WHEN ret_10d > 0 THEN 1.0 WHEN ret_10d <= 0 THEN 0.0 ELSE NULL END)::float8 AS breadth_10d,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ret_1d)::float8 AS median_stock_ret_1d,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ret_5d)::float8 AS median_stock_ret_5d,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ret_10d)::float8 AS median_stock_ret_10d,
            STDDEV_SAMP(ret_1d)::float8 AS dispersion_1d,
            STDDEV_SAMP(ret_5d)::float8 AS dispersion_5d,
            STDDEV_SAMP(ret_10d)::float8 AS dispersion_10d,
            AVG(ret_1d)::float8 AS mean_stock_ret_1d,
            AVG(ret_5d)::float8 AS mean_stock_ret_5d,
            AVG(ret_10d)::float8 AS mean_stock_ret_10d,
            COUNT(ret_1d) AS stock_count
        FROM stock_ret
        GROUP BY trade_date, sector_code
        ORDER BY trade_date, sector_code
    """
    df = read_sql(conn, sql, {"prestart": prestart, "end": end})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def load_factor_catalog(conn) -> pd.DataFrame:
    names = [f.factor_name for f in SOURCE_FACTORS if not f.factor_name.startswith("sector_")]
    sql = """
        SELECT
            c.factor_name,
            cl.category,
            cl.data_source_group,
            cl.factor_dimension,
            cl.best_horizon,
            m.ic_mean,
            m.rank_ic_mean,
            m.rank_icir
        FROM aistock_factor_catalog c
        LEFT JOIN qe_factor_classification cl ON cl.factor_name = c.factor_name
        LEFT JOIN aistock_factor_metrics m
          ON m.factor_name = c.factor_name
         AND m.eval_window = 'out_sample'
        WHERE c.factor_name = ANY(%(names)s)
        ORDER BY c.factor_name
    """
    return read_sql(conn, sql, {"names": names})


def rolling_pctrank(frame: pd.DataFrame, window: int, min_periods: int) -> pd.DataFrame:
    ranks = frame.rolling(window=window, min_periods=min_periods).rank(pct=True)
    return ranks


def group_rolling(panel: pd.DataFrame, column: str, window: int, func: str, min_periods: int | None = None) -> pd.Series:
    minp = min_periods if min_periods is not None else max(1, window // 2)
    by_sector = panel.groupby(level="sector_code", group_keys=False)[column]
    if func == "mean":
        return by_sector.rolling(window, min_periods=minp).mean().droplevel(0)
    if func == "std":
        return by_sector.rolling(window, min_periods=minp).std().droplevel(0)
    if func == "sum":
        return by_sector.rolling(window, min_periods=minp).sum().droplevel(0)
    if func == "max":
        return by_sector.rolling(window, min_periods=minp).max().droplevel(0)
    raise ValueError(f"Unknown rolling func: {func}")


def cs_rank(panel: pd.DataFrame, column: str) -> pd.Series:
    return panel.groupby(level="trade_date")[column].rank(pct=True)


def build_panel(sector_daily: pd.DataFrame, sector_mf: pd.DataFrame, breadth: pd.DataFrame) -> pd.DataFrame:
    base = sector_daily.merge(sector_mf, on=["trade_date", "sector_code"], how="left")
    base = base.merge(breadth, on=["trade_date", "sector_code"], how="left")
    base = base.sort_values(["sector_code", "trade_date"])
    base = base.set_index(["trade_date", "sector_code"]).sort_index()

    close = base["close"]
    by_sector = base.groupby(level="sector_code", group_keys=False)
    base["ret_1d"] = by_sector["close"].pct_change(1)
    for horizon in (1, 5, 10, 20):
        base[f"fwd_ret_{horizon}d"] = by_sector["close"].shift(-horizon) / close - 1.0

    amount = base["amount"].replace(0, np.nan)
    total_mv = base["total_mv"].replace(0, np.nan)
    base["sector_turnover"] = amount / total_mv * 100.0
    base["range_ratio"] = (base["high"] - base["low"]) / close.replace(0, np.nan)

    prev_close = by_sector["close"].shift(1)
    tr1 = base["high"] - base["low"]
    tr2 = (base["high"] - prev_close).abs()
    tr3 = (base["low"] - prev_close).abs()
    base["true_range"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    base["atr14"] = group_rolling(base, "true_range", 14, "mean", 10) / close.replace(0, np.nan)

    base["sf_intraday_range_5d_neg"] = -group_rolling(base, "range_ratio", 5, "mean", 3)
    base["sf_volatility_10d_neg"] = -group_rolling(base, "ret_1d", 10, "std", 6)
    base["sf_volatility_20d_neg"] = -group_rolling(base, "ret_1d", 20, "std", 10)
    base["sf_max_ret_20d_neg"] = -group_rolling(base, "ret_1d", 20, "max", 15)
    base["sf_amount_weighted_mom_20d"] = (
        group_rolling(base.assign(weighted_ret=base["ret_1d"] * base["amount"]), "weighted_ret", 20, "sum", 15)
        / group_rolling(base, "amount", 20, "sum", 15).replace(0, np.nan)
    )

    turnover_wide = base["sector_turnover"].unstack("sector_code")
    base["sf_turnover_pctile_120d_neg"] = -rolling_pctrank(turnover_wide, 120, 60).stack()
    base["sf_turnover_pctile_250d_neg"] = -rolling_pctrank(turnover_wide, 250, 120).stack()
    turn_mean_60 = group_rolling(base, "sector_turnover", 60, "mean", 30)
    turn_std_60 = group_rolling(base, "sector_turnover", 60, "std", 30).replace(0, np.nan)
    base["sf_turnover_zscore_60d_neg"] = -((base["sector_turnover"] - turn_mean_60) / turn_std_60)
    turn_ma5 = group_rolling(base, "sector_turnover", 5, "mean", 3)
    turn_ma20 = group_rolling(base, "sector_turnover", 20, "mean", 10)
    base["sf_turnover_ma5_ma20_neg"] = -(turn_ma5 / turn_ma20.replace(0, np.nan) - 1.0)

    atr_wide = base["atr14"].unstack("sector_code")
    base["sf_atr14_pctile_250d_neg"] = -rolling_pctrank(atr_wide, 250, 120).stack()

    mf_amount = base["sw2_amount"].fillna(base["amount"]).replace(0, np.nan)
    base["mf_net_ratio"] = base["mf_net_amt"] / mf_amount
    base["big_net_amt"] = (
        base["mf_buy_lg_amt"]
        - base["mf_sell_lg_amt"]
        + base["mf_buy_elg_amt"]
        - base["mf_sell_elg_amt"]
    )
    base["big_net_ratio"] = base["big_net_amt"] / mf_amount
    base["elg_net_ratio"] = (base["mf_buy_elg_amt"] - base["mf_sell_elg_amt"]) / mf_amount
    base["small_net_ratio"] = (base["mf_buy_sm_amt"] - base["mf_sell_sm_amt"]) / mf_amount
    base["small_buy_ratio"] = base["mf_buy_sm_amt"] / mf_amount

    base["sf_mf_net_ratio_std_5d_neg"] = -group_rolling(base, "mf_net_ratio", 5, "std", 5)
    mf_mean_5 = group_rolling(base, "mf_net_ratio", 5, "mean", 5)
    mf_std_5 = group_rolling(base, "mf_net_ratio", 5, "std", 5).replace(0, np.nan)
    base["sf_flow_stability_5d"] = mf_mean_5 / mf_std_5
    base["sf_small_buy_intensity_5d"] = group_rolling(base, "small_buy_ratio", 5, "mean", 3)
    base["sf_small_net_ratio_5d"] = group_rolling(base, "small_net_ratio", 5, "mean", 3)
    base["sf_flow_tier_strength_10d"] = group_rolling(
        base.assign(flow_tier=base["big_net_ratio"] - base["small_net_ratio"]),
        "flow_tier",
        10,
        "mean",
        5,
    )
    big_net_sum_10 = group_rolling(base, "big_net_ratio", 10, "sum", 5)
    base["big_net_sum_10"] = big_net_sum_10
    base["sf_turnover_mf_divergence_10d"] = (1.0 - cs_rank(base, "sector_turnover")) * cs_rank(base, "big_net_sum_10")
    elg_vol_20 = group_rolling(base, "elg_net_ratio", 20, "std", 10)
    pb_inv = 1.0 / base["pb"].replace(0, np.nan)
    pb_inv_resid = pb_inv - pb_inv.groupby(level="sector_code", group_keys=False).rolling(60, min_periods=30).mean().droplevel(0)
    base["sf_dynamic_flow_vol_sentiment"] = elg_vol_20 * base["sector_turnover"] * pb_inv_resid

    daily_range_median = base.groupby(level="trade_date")["range_ratio"].transform("median").replace(0, np.nan)
    daily_vol_median = group_rolling(base, "ret_1d", 20, "std", 10).groupby(level="trade_date").transform("median").replace(0, np.nan)
    base["sf_range_vs_market_10d"] = group_rolling(
        base.assign(range_vs_market=base["range_ratio"] / daily_range_median),
        "range_vs_market",
        10,
        "mean",
        5,
    )
    base["sf_vol_vs_market_20d"] = group_rolling(base, "ret_1d", 20, "std", 10) / daily_vol_median

    base["sf_breadth_1d"] = base["breadth_1d"]
    base["sf_breadth_5d"] = base["breadth_5d"]
    base["sf_breadth_10d"] = base["breadth_10d"]
    base["sf_excess_breadth_5d"] = base["breadth_5d"] - base.groupby(level="trade_date")["breadth_5d"].transform("mean")
    base["sf_median_stock_ret_5d"] = base["median_stock_ret_5d"]
    base["sf_dispersion_5d_neg"] = -base["dispersion_5d"]

    return base.replace([np.inf, -np.inf], np.nan)


def all_feature_names() -> list[str]:
    names: list[str] = []
    for source in SOURCE_FACTORS:
        for feature in source.sector_features:
            if feature not in names:
                names.append(feature)
    extras = ["sf_volatility_10d_neg", "sf_volatility_20d_neg", "sf_flow_tier_strength_10d", "sf_breadth_10d"]
    for feature in extras:
        if feature not in names:
            names.append(feature)
    return names


def safe_tstat(values: pd.Series) -> float:
    values = values.dropna()
    if len(values) < 2:
        return float("nan")
    std = values.std(ddof=1)
    if not std or math.isnan(std):
        return float("nan")
    return float(values.mean() / (std / math.sqrt(len(values))))


def daily_rank_ic(frame: pd.DataFrame, feature: str, label: str) -> pd.DataFrame:
    rows = []
    for date, group in frame[[feature, label]].dropna().groupby(level="trade_date"):
        if len(group) < 20:
            continue
        x = group[feature]
        y = group[label]
        if x.nunique() < 3 or y.nunique() < 3:
            continue
        corr = spearmanr(x, y).correlation
        if corr is None or math.isnan(corr):
            continue
        rows.append({"trade_date": date, "feature": feature, "label": label, "rank_ic": float(corr), "n": len(group)})
    return pd.DataFrame(rows)


def top_bottom_spread(frame: pd.DataFrame, feature: str, label: str, direction: int, q: float = 0.2) -> pd.DataFrame:
    rows = []
    score = frame[feature] * direction
    work = frame[[feature, label]].copy()
    work["score"] = score
    for date, group in work[["score", label]].dropna().groupby(level="trade_date"):
        if len(group) < 20:
            continue
        ranks = group["score"].rank(pct=True)
        top = group.loc[ranks >= 1.0 - q, label]
        bottom = group.loc[ranks <= q, label]
        if top.empty or bottom.empty:
            continue
        rows.append(
            {
                "trade_date": date,
                "top_mean": float(top.mean()),
                "bottom_mean": float(bottom.mean()),
                "spread": float(top.mean() - bottom.mean()),
                "n": len(group),
            }
        )
    return pd.DataFrame(rows)


def evaluate(panel: pd.DataFrame, features: Iterable[str], start: str, end: str, test_start: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    eval_panel = panel.loc[(slice(pd.Timestamp(start), pd.Timestamp(end)), slice(None)), :].copy()
    daily_rows = []
    summary_rows = []
    horizons = (1, 5, 10, 20)

    for feature in features:
        if feature not in eval_panel.columns:
            continue
        for horizon in horizons:
            label = f"fwd_ret_{horizon}d"
            daily = daily_rank_ic(eval_panel, feature, label)
            if daily.empty:
                continue
            daily["horizon"] = horizon
            daily_rows.append(daily)
            train = daily[daily["trade_date"] < pd.Timestamp(test_start)]["rank_ic"]
            test = daily[daily["trade_date"] >= pd.Timestamp(test_start)]["rank_ic"]
            full = daily["rank_ic"]
            train_mean = float(train.mean()) if len(train) else float("nan")
            direction = 1 if (math.isnan(train_mean) or train_mean >= 0) else -1
            signed_daily = daily.copy()
            signed_daily["signed_rank_ic"] = signed_daily["rank_ic"] * direction
            signed_train = signed_daily[signed_daily["trade_date"] < pd.Timestamp(test_start)]["signed_rank_ic"]
            signed_test = signed_daily[signed_daily["trade_date"] >= pd.Timestamp(test_start)]["signed_rank_ic"]
            spread = top_bottom_spread(eval_panel, feature, label, direction)
            spread_train = spread[spread["trade_date"] < pd.Timestamp(test_start)]["spread"] if not spread.empty else pd.Series(dtype=float)
            spread_test = spread[spread["trade_date"] >= pd.Timestamp(test_start)]["spread"] if not spread.empty else pd.Series(dtype=float)
            summary_rows.append(
                {
                    "feature": feature,
                    "horizon": horizon,
                    "direction": direction,
                    "daily_count_full": len(full),
                    "daily_count_train": len(train),
                    "daily_count_test": len(test),
                    "rank_ic_full": float(full.mean()),
                    "rank_ic_train": train_mean,
                    "rank_ic_test": float(test.mean()) if len(test) else float("nan"),
                    "signed_rank_ic_train": float(signed_train.mean()) if len(signed_train) else float("nan"),
                    "signed_rank_ic_test": float(signed_test.mean()) if len(signed_test) else float("nan"),
                    "signed_rank_ic_test_t": safe_tstat(signed_test),
                    "signed_rank_ic_test_pos_ratio": float((signed_test > 0).mean()) if len(signed_test) else float("nan"),
                    "top_bottom_spread_train": float(spread_train.mean()) if len(spread_train) else float("nan"),
                    "top_bottom_spread_test": float(spread_test.mean()) if len(spread_test) else float("nan"),
                    "top_bottom_spread_test_t": safe_tstat(spread_test),
                    "top_bottom_spread_test_win_ratio": float((spread_test > 0).mean()) if len(spread_test) else float("nan"),
                }
            )

    daily_df = pd.concat(daily_rows, ignore_index=True) if daily_rows else pd.DataFrame()
    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        summary_df = summary_df.sort_values(
            ["horizon", "signed_rank_ic_test", "top_bottom_spread_test"],
            ascending=[True, False, False],
        )
    return daily_df, summary_df


def fmt_float(value: object, digits: int = 4) -> str:
    try:
        f = float(value)
    except Exception:
        return "NA"
    if math.isnan(f) or math.isinf(f):
        return "NA"
    return f"{f:.{digits}f}"


def fmt_pct(value: object, digits: int = 2) -> str:
    try:
        f = float(value)
    except Exception:
        return "NA"
    if math.isnan(f) or math.isinf(f):
        return "NA"
    return f"{f * 100:.{digits}f}%"


def fixed_table(rows: list[dict[str, object]], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "```text\n(no rows)\n```"
    rendered = []
    widths = []
    for key, title in columns:
        vals = [str(row.get(key, "")) for row in rows]
        widths.append(max(len(title), *(len(v) for v in vals)))
    header = "  ".join(title.ljust(widths[i]) for i, (_, title) in enumerate(columns))
    sep = "  ".join("-" * widths[i] for i in range(len(columns)))
    rendered.append(header)
    rendered.append(sep)
    for row in rows:
        rendered.append("  ".join(str(row.get(key, "")).ljust(widths[i]) for i, (key, _) in enumerate(columns)))
    return "```text\n" + "\n".join(rendered) + "\n```"


def summarize_for_report(summary: pd.DataFrame) -> dict[str, list[dict[str, object]]]:
    result: dict[str, list[dict[str, object]]] = {}
    for horizon in (5, 10, 20):
        view = summary[summary["horizon"] == horizon].copy()
        view = view.sort_values(["signed_rank_ic_test", "top_bottom_spread_test"], ascending=False).head(12)
        rows = []
        for _, row in view.iterrows():
            rows.append(
                {
                    "feature": row["feature"],
                    "dir": "+" if int(row["direction"]) > 0 else "-",
                    "train_ic": fmt_float(row["signed_rank_ic_train"], 4),
                    "test_ic": fmt_float(row["signed_rank_ic_test"], 4),
                    "test_t": fmt_float(row["signed_rank_ic_test_t"], 2),
                    "pos": fmt_pct(row["signed_rank_ic_test_pos_ratio"], 1),
                    "spread": fmt_pct(row["top_bottom_spread_test"], 3),
                    "spread_t": fmt_float(row["top_bottom_spread_test_t"], 2),
                    "win": fmt_pct(row["top_bottom_spread_test_win_ratio"], 1),
                }
            )
        result[f"{horizon}d"] = rows
    return result


def lookup_metric(summary: pd.DataFrame, feature: str, horizon: int) -> dict[str, object]:
    row = summary[(summary["feature"] == feature) & (summary["horizon"] == horizon)]
    if row.empty:
        return {
            "feature": feature,
            "horizon": f"{horizon}D",
            "test_ic": "NA",
            "ic_t": "NA",
            "spread": "NA",
            "tb_t": "NA",
        }
    rec = row.iloc[0]
    return {
        "feature": feature,
        "horizon": f"{horizon}D",
        "test_ic": fmt_float(rec["signed_rank_ic_test"], 4),
        "ic_t": fmt_float(rec["signed_rank_ic_test_t"], 2),
        "spread": fmt_pct(rec["top_bottom_spread_test"], 3),
        "tb_t": fmt_float(rec["top_bottom_spread_test_t"], 2),
    }


def decision_rows(summary: pd.DataFrame) -> list[dict[str, object]]:
    decisions = [
        ("sf_turnover_pctile_250d_neg", 5, "core", "5D/10D both stable; first HMM emission candidate"),
        ("sf_turnover_pctile_120d_neg", 10, "core", "strong 10D holdout RankIC and positive spread"),
        ("sf_turnover_ma5_ma20_neg", 10, "core", "best 10D top-bottom spread among turnover features"),
        ("sf_mf_net_ratio_std_5d_neg", 10, "core", "money-flow stability works after sector migration"),
        ("sf_small_net_ratio_5d", 5, "secondary", "positive 5D IC and spread; useful money-flow companion"),
        ("sf_flow_tier_strength_10d", 20, "long-horizon", "20D signal is strong; use only for long-horizon branch"),
        ("sf_dynamic_flow_vol_sentiment", 10, "hold", "train IC high but holdout spread negative; needs gating"),
        ("sf_atr14_pctile_250d_neg", 10, "hold", "IC positive but spread negative; not standalone"),
        ("sf_max_ret_20d_neg", 10, "reject", "holdout spread is materially negative"),
    ]
    rows: list[dict[str, object]] = []
    for feature, horizon, action, note in decisions:
        rec = lookup_metric(summary, feature, horizon)
        rec.update({"action": action, "note": note})
        rows.append(rec)
    return rows


def write_report(
    report_path: Path,
    args: argparse.Namespace,
    factor_catalog: pd.DataFrame,
    summary: pd.DataFrame,
    daily: pd.DataFrame,
    panel: pd.DataFrame,
    output_dir: Path,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    top = summarize_for_report(summary)
    source_rows = []
    cat_map = factor_catalog.set_index("factor_name").to_dict("index") if not factor_catalog.empty else {}
    for source in SOURCE_FACTORS:
        db_row = cat_map.get(source.factor_name, {})
        source_rows.append(
            {
                "source": source.factor_name,
                "cat": source.category,
                "stock_rankic": fmt_float(db_row.get("rank_ic_mean", source.stock_rank_ic), 4),
                "best_h": str(db_row.get("best_horizon", source.best_horizon) or "NA"),
                "sector_features": ",".join(source.sector_features),
            }
        )

    health_rows = [
        {"item": "sector_days", "value": f"{panel.index.get_level_values('trade_date').nunique()}"},
        {"item": "sector_count_latest", "value": f"{panel.xs(panel.index.get_level_values('trade_date').max(), level='trade_date').shape[0]}"},
        {"item": "daily_rankic_rows", "value": f"{len(daily)}"},
        {"item": "summary_rows", "value": f"{len(summary)}"},
        {"item": "eval_window", "value": f"{args.start} ~ {args.end}"},
        {"item": "holdout_window", "value": f"{args.test_start} ~ {args.end}"},
    ]

    lines = [
        "# HMM 板块因子 RankIC 离线验证报告（2026-05-02）",
        "",
        "## 结论摘要",
        "",
        "- 本次没有修改交易策略或 HMM 运行时代码，只用本地 DB 中 `market.sw_daily`、`market.sector_data`、`market.daily_basic` 和因子库指标做只读离线验证。",
        "- 验证目标是把因子库里可板块化的高 RankIC 因子迁移成板块特征，先看 5D/10D 板块未来收益 RankIC 和 top-bottom spread，再决定是否进入 HMM emission / 校准模型。",
        "- 方向选择使用训练段均值 RankIC，持出段从 `test_start` 开始，避免用全样本符号直接挑方向。",
        "- 该报告是第一轮实践验证；如果某个特征在 5D/10D 持出段 RankIC 和 spread 同时为正，才值得进入下一轮 HMM 候选特征集合。",
        "",
        "## 数据健康",
        "",
        fixed_table(health_rows, [("item", "Item"), ("value", "Value")]),
        "",
        "## 可板块化因子来源",
        "",
        fixed_table(
            source_rows,
            [
                ("source", "StockFactor"),
                ("cat", "Cat"),
                ("stock_rankic", "StockRankIC"),
                ("best_h", "BestH"),
                ("sector_features", "SectorFeatures"),
            ],
        ),
        "",
        "## 5D 持出段候选",
        "",
        fixed_table(
            top["5d"],
            [
                ("feature", "Feature"),
                ("dir", "Dir"),
                ("train_ic", "TrainIC"),
                ("test_ic", "TestIC"),
                ("test_t", "IC_t"),
                ("pos", "Pos"),
                ("spread", "TBSpread"),
                ("spread_t", "TB_t"),
                ("win", "TBWin"),
            ],
        ),
        "",
        "## 10D 持出段候选",
        "",
        fixed_table(
            top["10d"],
            [
                ("feature", "Feature"),
                ("dir", "Dir"),
                ("train_ic", "TrainIC"),
                ("test_ic", "TestIC"),
                ("test_t", "IC_t"),
                ("pos", "Pos"),
                ("spread", "TBSpread"),
                ("spread_t", "TB_t"),
                ("win", "TBWin"),
            ],
        ),
        "",
        "## 产物",
        "",
        f"- `summary`: `{output_dir / 'sector_factor_summary.csv'}`",
        f"- `daily_rankic`: `{output_dir / 'sector_factor_daily_rankic.csv'}`",
        f"- `source_factor_map`: `{output_dir / 'sector_factor_source_map.csv'}`",
        "",
        "## 初步可用信号",
        "",
        fixed_table(
            decision_rows(summary),
            [
                ("feature", "Feature"),
                ("horizon", "Horizon"),
                ("test_ic", "TestIC"),
                ("ic_t", "IC_t"),
                ("spread", "TBSpread"),
                ("tb_t", "TB_t"),
                ("action", "Action"),
                ("note", "Note"),
            ],
        ),
        "",
        "## 20D 参考候选",
        "",
        fixed_table(
            top["20d"],
            [
                ("feature", "Feature"),
                ("dir", "Dir"),
                ("train_ic", "TrainIC"),
                ("test_ic", "TestIC"),
                ("test_t", "IC_t"),
                ("pos", "Pos"),
                ("spread", "TBSpread"),
                ("spread_t", "TB_t"),
                ("win", "TBWin"),
            ],
        ),
        "",
        "## 对 HMM 优化的直接含义",
        "",
        "- 当前第一优先级不应继续微调 latest dynamic PUP 的系数缩放，而应先把 HMM 的板块状态输入换成有持出段证据的板块因子。",
        "- 第一组候选是换手拥挤/降温类：`sf_turnover_pctile_250d_neg`、`sf_turnover_pctile_120d_neg`、`sf_turnover_zscore_60d_neg`、`sf_turnover_ma5_ma20_neg`。",
        "- 第二组候选是资金流稳定类：`sf_mf_net_ratio_std_5d_neg`、`sf_small_net_ratio_5d`，20D 可额外观察 `sf_flow_tier_strength_10d`。",
        "- 暂不建议把 `sf_dynamic_flow_vol_sentiment`、`sf_atr14_pctile_250d_neg`、`sf_max_ret_20d_neg` 作为单独强信号接入：它们存在持出段 spread 弱或为负的问题。",
        "- 5D/10D 的可用信号明显强于 1D，说明 HMM 更适合作为中短周期板块状态/系数校准器，而不是直接做明日板块涨幅排名器。",
        "- 后续实践建议保持策略不变，先离线生成基于这些板块因子的候选 sector coefficient，并复用现有 Top50 replacement 诊断框架验证进入/剔除股票的净收益。",
        "",
        "## 解释口径",
        "",
        "- `Dir=+` 表示沿用该板块特征原始方向；`Dir=-` 表示训练段显示需要取反。",
        "- `TestIC` 是方向调整后的持出段日均 RankIC；`TBSpread` 是每天 top20% 板块未来收益均值减 bottom20% 后再求均值。",
        "- 该验证使用当前静态申万二级成分映射计算资金流和个股 breadth，历史成分迁移会带来小幅噪声；后续若进入生产特征，应升级为按 `in_date/out_date` 的 PIT 成分映射。",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.report)

    with db_connect() as conn:
        sector_daily = load_sector_daily(conn, args.prestart, args.end)
        sector_mf = load_sector_moneyflow(conn, args.prestart, args.end)
        breadth = load_sector_breadth(conn, args.prestart, args.end)
        factor_catalog = load_factor_catalog(conn)

    panel = build_panel(sector_daily, sector_mf, breadth)
    features = all_feature_names()
    daily, summary = evaluate(panel, features, args.start, args.end, args.test_start)

    panel_health = pd.DataFrame(
        [
            {
                "start": str(panel.index.get_level_values("trade_date").min().date()),
                "end": str(panel.index.get_level_values("trade_date").max().date()),
                "sector_days": panel.index.get_level_values("trade_date").nunique(),
                "rows": len(panel),
                "features": len(features),
            }
        ]
    )
    source_map = pd.DataFrame(
        [
            {
                "source_factor": source.factor_name,
                "category": source.category,
                "stock_rank_ic_out_sample": source.stock_rank_ic,
                "best_horizon": source.best_horizon,
                "sector_features": ",".join(source.sector_features),
                "migration_note": source.migration_note,
            }
            for source in SOURCE_FACTORS
        ]
    )

    daily.to_csv(output_dir / "sector_factor_daily_rankic.csv", index=False, encoding="utf-8-sig")
    summary.to_csv(output_dir / "sector_factor_summary.csv", index=False, encoding="utf-8-sig")
    source_map.to_csv(output_dir / "sector_factor_source_map.csv", index=False, encoding="utf-8-sig")
    factor_catalog.to_csv(output_dir / "source_factor_catalog_metrics.csv", index=False, encoding="utf-8-sig")
    panel_health.to_csv(output_dir / "sector_factor_panel_health.csv", index=False, encoding="utf-8-sig")

    write_report(report_path, args, factor_catalog, summary, daily, panel, output_dir)
    print(f"Wrote summary: {output_dir / 'sector_factor_summary.csv'}")
    print(f"Wrote report: {report_path}")


if __name__ == "__main__":
    main()
