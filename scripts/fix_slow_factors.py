#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量修复 13 个超时因子代码 — pandas 性能反模式优化
==================================================
A 类: groupby().apply(rolling_corr) → 向量化 rolling().corr().droplevel(0)   [5 因子]
B 类: rolling().apply(func, raw=False) → raw=True / shift 向量化            [5 因子]
C 类: 冗余 sort_index / reset_index 优化                                    [3 因子]

同步写入 4 个存储位置:
  1. DB code_text        — 原始 QLib 版
  2. 文件 asset_path      — 原始 QLib 版文件
  3. DB realtime_code_text — 实盘版
  4. 文件 qe_code_path     — rdagent_assets/qe_factors/{name}.py

运行: conda run -n AIstock python scripts/fix_slow_factors.py [--dry-run]
"""
import ast
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, ".env"))

import psycopg2
from backend.db.pg_pool import _db_cfg

DRY_RUN = "--dry-run" in sys.argv

BACKUP_DIR = os.path.join(ROOT, "scripts", "backups", f"fix_slow_factors_{datetime.now():%Y%m%d_%H%M%S}")

FACTOR_NAMES = [
    # A 类
    "price_volume_corr",
    "pv_corr",
    "price_volume_correlation_5d",
    "fund_flow_persistence",
    "micro_flow_correlation",
    # B 类
    "EarningsGrowthMomentum",
    "elg_flow_acceleration_pb_trend",
    "main_flow_acceleration_turnover_normalized",
    "dynamic_volume_flow_regime_adjusted",
    "lstm_temporal_flow_valuation_signal",
    # C 类
    "market_sentiment_turnover_news",
    "market_state_volatility_regime",
    "ml_nonlinear_fusion_input",
]

BEGIN_MARKER = "# ==== BEGIN FACTOR COMPUTATION AREA ===="
END_MARKER = "# ==== END FACTOR COMPUTATION AREA ===="
# Also match "# 4. ==== BEGIN ..." or "# 5. ==== BEGIN ..." prefixed versions
BEGIN_PAT = re.compile(r"^([ \t]*)#\s*(?:\d+\.\s*)?====\s*BEGIN FACTOR COMPUTATION AREA\s*====", re.MULTILINE)
END_PAT = re.compile(r"^([ \t]*)#\s*(?:\d+\.\s*)?====\s*END FACTOR COMPUTATION AREA\s*====", re.MULTILINE)


def extract_computation_area(code: str) -> tuple[str, str, str, str]:
    """Extract (before, indent, computation_body, after) from factor code.

    Returns the code before BEGIN marker, the indentation, the computation body,
    and the code after END marker (including the END line itself in 'after').
    """
    bm = BEGIN_PAT.search(code)
    em = END_PAT.search(code)
    if not bm or not em:
        raise ValueError("Cannot find BEGIN/END FACTOR COMPUTATION AREA markers")

    # Everything up to and including the BEGIN line
    begin_line_end = code.index('\n', bm.start()) + 1
    before = code[:begin_line_end]

    # Everything from END marker line onward
    end_line_start = em.start()
    after = code[end_line_start:]

    # The computation body is between
    body = code[begin_line_end:end_line_start]
    indent = bm.group(1) or "    "

    return before, indent, body, after


def replace_computation_area(code: str, new_body: str) -> str:
    """Replace computation area body in code, preserving everything else."""
    before, indent, old_body, after = extract_computation_area(code)
    return before + new_body + after


# ════════════════════════════════════════════════════════════════
# A 类修复函数
# ════════════════════════════════════════════════════════════════

def fix_price_volume_corr(body: str, indent: str) -> str:
    """Fix price_volume_corr: remove rolling_corr function, use vectorized rolling().corr()"""
    I = indent
    return f"""{I}# 计算收盘价与成交量的20日滚动相关系数（向量化）
{I}series = (
{I}    df['close']
{I}    .groupby(level='instrument')
{I}    .rolling(window=20, min_periods=20)
{I}    .corr(df['volume'])
{I}    .droplevel(0)
{I})

"""


def fix_pv_corr(body: str, indent: str) -> str:
    """Fix pv_corr: remove rolling_corr function, use vectorized rolling().corr()"""
    I = indent
    return f"""{I}# 计算过去5日收盘价与成交量的滚动相关系数（向量化）
{I}series = (
{I}    df['close']
{I}    .groupby(level='instrument')
{I}    .rolling(window=5, min_periods=5)
{I}    .corr(df['volume'])
{I}    .droplevel(0)
{I})

"""


def fix_price_volume_correlation_5d(body: str, indent: str) -> str:
    """Fix price_volume_correlation_5d: same pattern as pv_corr"""
    I = indent
    return f"""{I}# 计算5日滚动收盘价与成交量的相关系数（向量化）
{I}series = (
{I}    df['close']
{I}    .groupby(level='instrument')
{I}    .rolling(window=5, min_periods=5)
{I}    .corr(df['volume'])
{I}    .droplevel(0)
{I})

"""


def fix_fund_flow_persistence(body: str, indent: str) -> str:
    """Fix fund_flow_persistence: vectorize rolling autocorrelation"""
    I = indent
    return f"""{I}# 计算资金流持续性因子：滚动窗口内 mf_net_amt 的自相关系数（滞后1）
{I}window = 5
{I}
{I}# 先计算滞后1的序列
{I}mf_net_amt = df["mf_net_amt"]
{I}mf_lag1 = mf_net_amt.groupby(level="instrument").shift(1)
{I}
{I}# 向量化计算滚动相关系数
{I}series = (
{I}    mf_net_amt
{I}    .groupby(level="instrument")
{I}    .rolling(window=window, min_periods=window)
{I}    .corr(mf_lag1)
{I}    .droplevel(0)
{I})

"""


def fix_micro_flow_correlation(body: str, indent: str) -> str:
    """Fix micro_flow_correlation: remove flow_df and rolling_corr function"""
    I = indent
    return f"""{I}# 计算大单净流入率（相对于流通市值）
{I}# db_circ_mv 单位是万元，mf_lg_buy_amt/mf_lg_sell_amt 单位是元，需要乘以10000转换
{I}lg_net_inflow_rate = (
{I}    (df["mf_lg_buy_amt"] - df["mf_lg_sell_amt"]) /
{I}    (df["db_circ_mv"] * 10000).where(df["db_circ_mv"] != 0, np.nan)
{I})
{I}
{I}# 计算中单净流入率（相对于流通市值）
{I}md_net_inflow_rate = (
{I}    (df["mf_md_buy_amt"] - df["mf_md_sell_amt"]) /
{I}    (df["db_circ_mv"] * 10000).where(df["db_circ_mv"] != 0, np.nan)
{I})
{I}
{I}# 向量化计算20日滚动相关系数
{I}series = (
{I}    lg_net_inflow_rate
{I}    .groupby(level="instrument")
{I}    .rolling(window=20, min_periods=20)
{I}    .corr(md_net_inflow_rate)
{I}    .droplevel(0)
{I})
{I}
{I}# 替换无穷值
{I}series = series.replace([np.inf, -np.inf], np.nan)

"""


# ════════════════════════════════════════════════════════════════
# B 类修复函数
# ════════════════════════════════════════════════════════════════

def fix_EarningsGrowthMomentum(body: str, indent: str) -> str:
    """Fix EarningsGrowthMomentum: replace rolling().apply(lambda) with shift() vectorization"""
    I = indent
    return f"""{I}# 计算每个 instrument 的 bb_eps 的 10 日和 20 日百分比变化（向量化）
{I}eps = df["bb_eps"]
{I}
{I}# 10日百分比变化: (current / 10日前) - 1
{I}eps_shifted_10 = eps.groupby(level="instrument").shift(10)
{I}eps_10d_pct = (eps / eps_shifted_10.where(eps_shifted_10 != 0, np.nan)) - 1
{I}
{I}# 20日百分比变化: (current / 20日前) - 1
{I}eps_shifted_20 = eps.groupby(level="instrument").shift(20)
{I}eps_20d_pct = (eps / eps_shifted_20.where(eps_shifted_20 != 0, np.nan)) - 1
{I}
{I}# 计算因子值：短期增长减去长期增长
{I}series = eps_10d_pct - eps_20d_pct

"""


# ── B 类共用: 向量化 slope 函数 ──
CALC_SLOPE_FAST = '''
def calc_slope_fast(y):
    # raw=True 版线性回归斜率 - 纯 numpy, 无 scipy/pandas 开销
    n = len(y)
    if np.any(np.isnan(y)):
        mask = ~np.isnan(y)
        if mask.sum() < 2:
            return np.nan
        x = np.arange(n, dtype=np.float64)[mask]
        y = y[mask]
    else:
        x = np.arange(n, dtype=np.float64)
    x_mean = x.mean()
    y_mean = y.mean()
    denom = np.sum((x - x_mean) ** 2)
    if denom < 1e-12:
        return np.nan
    return np.sum((x - x_mean) * (y - y_mean)) / denom
'''


def fix_elg_flow_acceleration_pb_trend(body: str, indent: str) -> str:
    """Fix elg_flow_acceleration_pb_trend: replace scipy linregress with raw=True numpy slope"""
    I = indent
    slope_fn = CALC_SLOPE_FAST.replace('\n', f'\n{I}').strip()
    return f"""{I}# 计算 mf_elg_net_ratio: 特大单净流入比率
{I}# (mf_elg_buy_amt - mf_elg_sell_amt) / amount
{I}mf_elg_net = df["mf_elg_buy_amt"] - df["mf_elg_sell_amt"]
{I}mf_elg_net_ratio = mf_elg_net / df["amount"]
{I}mf_elg_net_ratio = mf_elg_net_ratio.replace([np.inf, -np.inf], np.nan)
{I}
{I}# 计算 mf_elg_net_ratio 的移动平均
{I}ma5 = mf_elg_net_ratio.groupby(level="instrument").rolling(window=5, min_periods=5).mean().droplevel(0)
{I}ma10 = mf_elg_net_ratio.groupby(level="instrument").rolling(window=10, min_periods=10).mean().droplevel(0)
{I}ma20 = mf_elg_net_ratio.groupby(level="instrument").rolling(window=20, min_periods=20).mean().droplevel(0)
{I}
{I}# 计算加速度（二阶导数）: MA(5) - 2 * MA(10) + MA(20)
{I}acceleration = ma5 - 2 * ma10 + ma20
{I}
{I}# 计算 value_pb_inv: 倒数市净率
{I}value_pb_inv = 1.0 / df["db_pb"]
{I}value_pb_inv = value_pb_inv.replace([np.inf, -np.inf], np.nan)
{I}
{I}# raw=True 线性回归斜率（无 scipy 依赖）
{I}{slope_fn}
{I}
{I}pb_trend = value_pb_inv.groupby(level="instrument").rolling(window=20, min_periods=10).apply(calc_slope_fast, raw=True).droplevel(0)
{I}
{I}# 计算 close 的 20 日波动率（标准差）
{I}volatility = df["close"].groupby(level="instrument").rolling(window=20, min_periods=20).std().droplevel(0)
{I}
{I}# 计算因子值: acceleration × pb_trend × (1 / volatility)
{I}volatility_inv = 1.0 / volatility
{I}volatility_inv = volatility_inv.replace([np.inf, -np.inf], np.nan)
{I}
{I}series = acceleration * pb_trend * volatility_inv

"""


def fix_main_flow_acceleration_turnover_normalized(body: str, indent: str) -> str:
    """Fix main_flow_acceleration_turnover_normalized: raw=True slope"""
    I = indent
    slope_fn = CALC_SLOPE_FAST.replace('\n', f'\n{I}').strip()
    return f"""{I}# 计算主力净流入金额 mf_main_net_amt
{I}df["mf_main_net_amt"] = (df["mf_lg_buy_amt"] + df["mf_elg_buy_amt"]) - (df["mf_lg_sell_amt"] + df["mf_elg_sell_amt"])
{I}
{I}# raw=True 线性回归斜率（无 scipy 依赖）
{I}{slope_fn}
{I}
{I}# 对每个instrument分组，计算5日滚动斜率
{I}slope_5d = df["mf_main_net_amt"].groupby(level="instrument").rolling(window=5, min_periods=5).apply(calc_slope_fast, raw=True).droplevel(0)
{I}
{I}# 计算因子值：斜率除以换手率
{I}series = slope_5d / df["db_turnover_rate"]

"""


def fix_dynamic_volume_flow_regime_adjusted(body: str, indent: str) -> str:
    """Fix dynamic_volume_flow_regime_adjusted: raw=True slope + vectorized regime"""
    I = indent
    slope_fn = CALC_SLOPE_FAST.replace('\n', f'\n{I}').strip()
    return f"""{I}# 计算 mf_elg_net_ratio = (mf_elg_buy_amt - mf_elg_sell_amt) / amount
{I}with np.errstate(divide='ignore', invalid='ignore'):
{I}    df["mf_elg_net_ratio"] = (df["mf_elg_buy_amt"] - df["mf_elg_sell_amt"]) / df["amount"]
{I}
{I}# 计算 mf_elg_net_ratio 的20日移动平均
{I}df["elg_ma20"] = df["mf_elg_net_ratio"].groupby(level="instrument").rolling(window=20, min_periods=20).mean().droplevel(0)
{I}
{I}# 计算残差：Residual(mf_elg_net_ratio, 20)
{I}df["elg_residual"] = df["mf_elg_net_ratio"] - df["elg_ma20"]
{I}
{I}# 计算 amount 的20日 Z-score
{I}amount_mean_20 = df["amount"].groupby(level="instrument").rolling(window=20, min_periods=20).mean().droplevel(0)
{I}amount_std_20 = df["amount"].groupby(level="instrument").rolling(window=20, min_periods=20).std().droplevel(0)
{I}
{I}with np.errstate(divide='ignore', invalid='ignore'):
{I}    df["amount_zscore"] = (df["amount"] - amount_mean_20) / amount_std_20
{I}
{I}# 计算 |ZScore(amount, 20)| 的60日移动平均
{I}df["abs_zscore"] = df["amount_zscore"].abs()
{I}df["ma_abs_zscore_60"] = df["abs_zscore"].groupby(level="instrument").rolling(window=60, min_periods=60).mean().droplevel(0)
{I}
{I}# 归一化的 ZScore: ZScore / MA(|ZScore|, 60)
{I}with np.errstate(divide='ignore', invalid='ignore'):
{I}    df["normalized_zscore"] = df["amount_zscore"] / df["ma_abs_zscore_60"]
{I}
{I}# 计算20日波动率: Volatility(close, 20)
{I}df["volatility_20"] = df["close"].groupby(level="instrument").rolling(window=20, min_periods=20).std().droplevel(0)
{I}
{I}# raw=True 线性回归斜率（无 scipy 依赖）
{I}{slope_fn}
{I}
{I}# 计算10日趋势斜率: Trend(close, 10)
{I}df["trend_10"] = df["close"].groupby(level="instrument").rolling(window=10, min_periods=10).apply(calc_slope_fast, raw=True).droplevel(0)
{I}
{I}# 向量化计算波动率的历史分位数（expanding quantile）
{I}vol_40pct = df["volatility_20"].groupby(level="instrument").expanding(min_periods=60).quantile(0.40).droplevel(0)
{I}vol_60pct = df["volatility_20"].groupby(level="instrument").expanding(min_periods=60).quantile(0.60).droplevel(0)
{I}
{I}# 初始化调整因子为1.0
{I}regime_adjust = pd.Series(1.0, index=df.index)
{I}
{I}# 低波动率区间：adjust = 1 / vol
{I}low_vol_mask = df["volatility_20"] < vol_40pct
{I}with np.errstate(divide='ignore', invalid='ignore'):
{I}    regime_adjust = regime_adjust.where(~low_vol_mask, 1.0 / df["volatility_20"])
{I}
{I}# 高波动率区间：adjust = 1 + 0.3 * sign(trend) * |trend|^0.5
{I}high_vol_mask = df["volatility_20"] > vol_60pct
{I}trend_sign = np.sign(df["trend_10"])
{I}trend_abs_sqrt = np.abs(df["trend_10"]) ** 0.5
{I}high_vol_adjust = 1.0 + 0.3 * trend_sign * trend_abs_sqrt
{I}regime_adjust = regime_adjust.where(~high_vol_mask, high_vol_adjust)
{I}
{I}# 计算最终因子：F = Residual * normalized_zscore * regime_adjust
{I}series = df["elg_residual"] * df["normalized_zscore"] * regime_adjust
{I}
{I}# 处理无穷大和NaN
{I}series = series.replace([np.inf, -np.inf], np.nan)

"""


def fix_lstm_temporal_flow_valuation_signal(body: str, indent: str) -> str:
    """Fix lstm_temporal_flow_valuation_signal: raw=True slope in calc_trend"""
    I = indent
    slope_fn = CALC_SLOPE_FAST.replace('\n', f'\n{I}').strip()
    return f"""{I}# 注意：本因子使用手工设计的特征组合来模拟LSTM时序模式

{I}# 计算 mf_elg_net_ratio（特大单净流入比例）
{I}df["mf_elg_net_amt"] = df["mf_elg_buy_amt"] - df["mf_elg_sell_amt"]
{I}df["mf_elg_net_ratio"] = df["mf_elg_net_amt"] / df["amount"].replace(0, np.nan)
{I}
{I}# 特征1: Residual(mf_elg_net_ratio, 20) - 20日资金流残差
{I}ma_20 = df["mf_elg_net_ratio"].groupby(level="instrument", group_keys=False).rolling(window=20, min_periods=20).mean().droplevel(0)
{I}feature1 = df["mf_elg_net_ratio"] - ma_20
{I}
{I}# 特征2: Trend(value_pb_inv, 20) - 20日估值趋势（线性回归斜率）
{I}# raw=True 线性回归斜率（无 scipy 依赖）
{I}{slope_fn}
{I}
{I}feature2 = df["value_pb_inv"].groupby(level="instrument", group_keys=False).rolling(window=20, min_periods=20).apply(calc_slope_fast, raw=True).droplevel(0)
{I}
{I}# 特征3: 1 / Volatility(close, 20) - 20日波动率倒数（风险调整）
{I}volatility = df["close"].groupby(level="instrument", group_keys=False).rolling(window=20, min_periods=20).std().droplevel(0)
{I}feature3 = 1.0 / volatility.replace(0, np.nan)
{I}
{I}# 特征4: ZScore(amount, 20) - 20日成交额Z-score
{I}amount_mean = df["amount"].groupby(level="instrument", group_keys=False).rolling(window=20, min_periods=20).mean().droplevel(0)
{I}amount_std = df["amount"].groupby(level="instrument", group_keys=False).rolling(window=20, min_periods=20).std().droplevel(0)
{I}feature4 = (df["amount"] - amount_mean) / amount_std.replace(0, np.nan)
{I}
{I}# 模拟LSTM的时序依赖：使用多个滞后期的特征加权组合
{I}def create_temporal_composite(f1, f2, f3, f4, lags=[0, 5, 10, 15, 19]):
{I}    # 创建时序组合特征，模拟LSTM的多时间步依赖
{I}    weights = np.exp(-0.1 * np.arange(len(lags)))
{I}    weights = weights / weights.sum()
{I}
{I}    composite = pd.Series(0.0, index=df.index)
{I}
{I}    for lag, weight in zip(lags, weights):
{I}        f1_lag = f1.groupby(level="instrument").shift(lag)
{I}        f2_lag = f2.groupby(level="instrument").shift(lag)
{I}        f3_lag = f3.groupby(level="instrument").shift(lag)
{I}        f4_lag = f4.groupby(level="instrument").shift(lag)
{I}
{I}        f1_norm = (f1_lag - f1_lag.mean()) / (f1_lag.std() + 1e-8)
{I}        f2_norm = (f2_lag - f2_lag.mean()) / (f2_lag.std() + 1e-8)
{I}        f3_norm = (f3_lag - f3_lag.mean()) / (f3_lag.std() + 1e-8)
{I}        f4_norm = (f4_lag - f4_lag.mean()) / (f4_lag.std() + 1e-8)
{I}
{I}        lag_composite = (f1_norm + f2_norm + f3_norm + f4_norm) / 4.0
{I}        composite += weight * lag_composite.fillna(0)
{I}
{I}    return composite
{I}
{I}# 生成最终因子值
{I}series = create_temporal_composite(feature1, feature2, feature3, feature4)

"""


# ════════════════════════════════════════════════════════════════
# C 类修复函数
# ════════════════════════════════════════════════════════════════

def fix_market_sentiment_turnover_news(body: str, indent: str) -> str:
    """Fix market_sentiment_turnover_news: replace reset_index(level=0, drop=True) with droplevel(0)"""
    fixed = body.replace(".reset_index(level=0, drop=True)", ".droplevel(0)")
    return fixed


def fix_market_state_volatility_regime(body: str, indent: str) -> str:
    """Fix market_state_volatility_regime: remove redundant sort_index and intermediate vars"""
    I = indent
    return f"""{I}# 检查所需列是否存在
{I}required_cols = ["close", "db_turnover_rate"]
{I}missing = [c for c in required_cols if c not in df.columns]
{I}if missing:
{I}    raise ValueError(f"Missing columns: {{missing}}. Please redesign factor using available fields.")
{I}
{I}# 计算 close 的 20 日滚动标准差
{I}std20_close = (
{I}    df["close"]
{I}    .groupby(level="instrument")
{I}    .rolling(window=20, min_periods=20)
{I}    .std()
{I}    .droplevel(0)
{I})
{I}
{I}# 计算 std20_close 的 60 日简单移动平均（作为长期基准）
{I}sma60_std20_close = (
{I}    std20_close
{I}    .groupby(level="instrument")
{I}    .rolling(window=60, min_periods=60)
{I}    .mean()
{I}    .droplevel(0)
{I})
{I}
{I}# 计算 db_turnover_rate 的 20 日滚动标准差
{I}std20_turnover = (
{I}    df["db_turnover_rate"]
{I}    .groupby(level="instrument")
{I}    .rolling(window=20, min_periods=20)
{I}    .std()
{I}    .droplevel(0)
{I})
{I}
{I}# 计算 std20_turnover 的 60 日简单移动平均（作为长期基准）
{I}sma60_std20_turnover = (
{I}    std20_turnover
{I}    .groupby(level="instrument")
{I}    .rolling(window=60, min_periods=60)
{I}    .mean()
{I}    .droplevel(0)
{I})
{I}
{I}# 计算指示函数之和
{I}indicator_close = (std20_close > sma60_std20_close).astype("float32")
{I}indicator_turnover = (std20_turnover > sma60_std20_turnover).astype("float32")
{I}
{I}series = indicator_close + indicator_turnover

"""


def fix_ml_nonlinear_fusion_input(body: str, indent: str) -> str:
    """Fix ml_nonlinear_fusion_input: replace reset_index(level=0, drop=True) with droplevel(0)"""
    fixed = body.replace(".reset_index(level=0, drop=True)", ".droplevel(0)")
    return fixed


# ════════════════════════════════════════════════════════════════
# 还需移除顶层的 from scipy import stats
# ════════════════════════════════════════════════════════════════

def remove_scipy_import(code: str) -> str:
    """Remove 'from scipy import stats' or 'from scipy.stats import linregress' if present."""
    code = re.sub(r"^from scipy.*\n", "", code, flags=re.MULTILINE)
    code = re.sub(r"^import scipy.*\n", "", code, flags=re.MULTILINE)
    return code


# ════════════════════════════════════════════════════════════════
# 修复函数映射
# ════════════════════════════════════════════════════════════════

FIX_MAP = {
    "price_volume_corr": fix_price_volume_corr,
    "pv_corr": fix_pv_corr,
    "price_volume_correlation_5d": fix_price_volume_correlation_5d,
    "fund_flow_persistence": fix_fund_flow_persistence,
    "micro_flow_correlation": fix_micro_flow_correlation,
    "EarningsGrowthMomentum": fix_EarningsGrowthMomentum,
    "elg_flow_acceleration_pb_trend": fix_elg_flow_acceleration_pb_trend,
    "main_flow_acceleration_turnover_normalized": fix_main_flow_acceleration_turnover_normalized,
    "dynamic_volume_flow_regime_adjusted": fix_dynamic_volume_flow_regime_adjusted,
    "lstm_temporal_flow_valuation_signal": fix_lstm_temporal_flow_valuation_signal,
    "market_sentiment_turnover_news": fix_market_sentiment_turnover_news,
    "market_state_volatility_regime": fix_market_state_volatility_regime,
    "ml_nonlinear_fusion_input": fix_ml_nonlinear_fusion_input,
}

# B 类因子需要移除 scipy import
REMOVE_SCIPY = {
    "elg_flow_acceleration_pb_trend",
}


def apply_fix(factor_name: str, code: str) -> str:
    """Apply the appropriate fix to factor code. Returns fixed code."""
    fix_fn = FIX_MAP[factor_name]
    before, indent, body, after = extract_computation_area(code)
    new_body = fix_fn(body, indent)
    fixed = before + new_body + after
    if factor_name in REMOVE_SCIPY:
        fixed = remove_scipy_import(fixed)
    return fixed


def validate_syntax(code: str, name: str) -> bool:
    """Validate Python syntax using ast.parse()."""
    try:
        ast.parse(code)
        return True
    except SyntaxError as e:
        print(f"  [SYNTAX ERROR] {name}: {e}")
        return False


def backup_file(filepath: str) -> None:
    """Backup a file to BACKUP_DIR preserving relative path."""
    if not os.path.exists(filepath):
        return
    rel = os.path.relpath(filepath, ROOT)
    dest = os.path.join(BACKUP_DIR, rel)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(filepath, dest)


def main():
    print("=" * 70)
    print("批量修复 13 个超时因子代码")
    print(f"模式: {'DRY-RUN (只输出不写入)' if DRY_RUN else '实际写入'}")
    print(f"备份目录: {BACKUP_DIR}")
    print("=" * 70)

    # ── 连接数据库 ──
    conn = psycopg2.connect(**_db_cfg())
    conn.autocommit = False

    try:
        cur = conn.cursor()

        # ── 查询 13 因子的所有字段 ──
        placeholders = ", ".join(["%s"] * len(FACTOR_NAMES))
        cur.execute(f"""
            SELECT factor_name, source, code_text, realtime_code_text, asset_path, qe_code_path
            FROM aistock_factor_catalog
            WHERE factor_name IN ({placeholders})
        """, FACTOR_NAMES)

        rows = cur.fetchall()
        db_data = {row[0]: {
            "source": row[1],
            "code_text": row[2],
            "realtime_code_text": row[3],
            "asset_path": row[4],
            "qe_code_path": row[5],
        } for row in rows}

        print(f"\n从 DB 查到 {len(db_data)} / {len(FACTOR_NAMES)} 个因子")

        missing_in_db = [n for n in FACTOR_NAMES if n not in db_data]
        if missing_in_db:
            print(f"  [WARN] DB 中不存在: {missing_in_db}")

        # ── 备份 ──
        os.makedirs(BACKUP_DIR, exist_ok=True)

        # 备份 DB 数据为 JSON
        backup_json = os.path.join(BACKUP_DIR, "db_backup.json")
        with open(backup_json, "w", encoding="utf-8") as f:
            json.dump(db_data, f, ensure_ascii=False, indent=2, default=str)
        print(f"  DB 备份 → {backup_json}")

        # ── 逐因子修复 ──
        results = {"success": [], "failed": [], "skipped": []}

        for factor_name in FACTOR_NAMES:
            print(f"\n{'─' * 50}")
            print(f"处理: {factor_name}")

            if factor_name not in db_data:
                print(f"  [SKIP] 不在 DB 中")
                results["skipped"].append(factor_name)
                continue

            info = db_data[factor_name]
            realtime_code = info["realtime_code_text"]
            original_code = info["code_text"]
            asset_path = info["asset_path"]
            qe_code_path = info["qe_code_path"]

            if not realtime_code:
                print(f"  [SKIP] realtime_code_text 为空")
                results["skipped"].append(factor_name)
                continue

            # ── 修复实盘版代码 ──
            try:
                fixed_realtime = apply_fix(factor_name, realtime_code)
            except Exception as e:
                print(f"  [FAIL] 修复实盘版失败: {e}")
                results["failed"].append((factor_name, str(e)))
                continue

            if not validate_syntax(fixed_realtime, f"{factor_name} (realtime)"):
                results["failed"].append((factor_name, "realtime syntax error"))
                continue

            # ── 修复原始版代码 ──
            fixed_original = None
            if original_code:
                try:
                    fixed_original = apply_fix(factor_name, original_code)
                    if not validate_syntax(fixed_original, f"{factor_name} (original)"):
                        # 原始版语法错误不阻塞，只修复实盘版
                        print(f"  [WARN] 原始版语法验证失败，跳过原始版修复")
                        fixed_original = None
                except Exception as e:
                    print(f"  [WARN] 原始版修复失败: {e}，跳过原始版修复")
                    fixed_original = None

            # ── 写入 ──
            if DRY_RUN:
                # 输出 diff 摘要
                rt_lines_before = len(realtime_code.splitlines())
                rt_lines_after = len(fixed_realtime.splitlines())
                print(f"  [DRY-RUN] realtime: {rt_lines_before} → {rt_lines_after} 行")
                if fixed_original:
                    orig_lines_before = len(original_code.splitlines())
                    orig_lines_after = len(fixed_original.splitlines())
                    print(f"  [DRY-RUN] original: {orig_lines_before} → {orig_lines_after} 行")
                results["success"].append(factor_name)
                continue

            write_count = 0

            # 1. DB realtime_code_text
            update_fields = ["realtime_code_text = %s"]
            update_values = [fixed_realtime]
            if fixed_original:
                update_fields.append("code_text = %s")
                update_values.append(fixed_original)

            cur.execute(f"""
                UPDATE aistock_factor_catalog
                SET {', '.join(update_fields)}
                WHERE factor_name = %s
            """, update_values + [factor_name])
            write_count += len(update_fields)
            print(f"  [DB] 更新 {len(update_fields)} 个字段")

            # 2. 文件 qe_code_path (实盘版)
            if qe_code_path:
                fpath = qe_code_path if os.path.isabs(qe_code_path) else os.path.join(ROOT, qe_code_path)
                if os.path.exists(fpath):
                    backup_file(fpath)
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write(fixed_realtime)
                    write_count += 1
                    print(f"  [FILE] 写入 qe_code_path: {fpath}")
                else:
                    print(f"  [WARN] qe_code_path 不存在: {fpath}")

            # 也写入标准位置 rdagent_assets/qe_factors/{name}.py
            std_qe_path = os.path.join(ROOT, "rdagent_assets", "qe_factors", f"{factor_name}.py")
            if os.path.exists(std_qe_path):
                # 如果 qe_code_path 和 std_qe_path 不同，也要更新
                if not qe_code_path or os.path.abspath(std_qe_path) != os.path.abspath(
                    qe_code_path if os.path.isabs(qe_code_path) else os.path.join(ROOT, qe_code_path)
                ):
                    backup_file(std_qe_path)
                    with open(std_qe_path, "w", encoding="utf-8") as f:
                        f.write(fixed_realtime)
                    write_count += 1
                    print(f"  [FILE] 写入 qe_factors: {std_qe_path}")

            # 3. 文件 asset_path (原始版)
            if fixed_original and asset_path:
                fpath = asset_path if os.path.isabs(asset_path) else os.path.join(ROOT, asset_path)
                if os.path.exists(fpath):
                    backup_file(fpath)
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write(fixed_original)
                    write_count += 1
                    print(f"  [FILE] 写入 asset_path: {fpath}")
                else:
                    print(f"  [WARN] asset_path 不存在: {fpath}")

            print(f"  [OK] 共写入 {write_count} 个位置")
            results["success"].append(factor_name)

        # ── 提交事务 ──
        if not DRY_RUN:
            conn.commit()
            print("\n[DB] 事务已提交")

        # ── 一致性验证 ──
        if not DRY_RUN:
            print("\n" + "=" * 70)
            print("一致性验证: DB vs 文件内容对比")
            print("=" * 70)
            cur.execute(f"""
                SELECT factor_name, realtime_code_text, qe_code_path
                FROM aistock_factor_catalog
                WHERE factor_name IN ({placeholders})
            """, FACTOR_NAMES)
            for fname, db_rt_code, qe_path in cur.fetchall():
                if not db_rt_code or not qe_path:
                    continue
                fpath = qe_path if os.path.isabs(qe_path) else os.path.join(ROOT, qe_path)
                if os.path.exists(fpath):
                    with open(fpath, "r", encoding="utf-8") as f:
                        file_code = f.read()
                    if db_rt_code == file_code:
                        print(f"  [OK] {fname}: DB == 文件")
                    else:
                        print(f"  [MISMATCH] {fname}: DB != 文件 !!!")

        # ── 汇总报告 ──
        print("\n" + "=" * 70)
        print("修复报告")
        print("=" * 70)
        print(f"成功: {len(results['success'])} 个 — {results['success']}")
        if results["failed"]:
            print(f"失败: {len(results['failed'])} 个 — {results['failed']}")
        if results["skipped"]:
            print(f"跳过: {len(results['skipped'])} 个 — {results['skipped']}")

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] 事务已回滚: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
