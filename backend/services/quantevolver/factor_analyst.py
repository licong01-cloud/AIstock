"""
QuantEvolver Phase 2: FactorAnalyst（因子分析师）

功能：
1. 单因子分析：对指定因子进行分类和评级
2. 批量分析：对因子catalog中所有因子批量执行分类、评级
3. 因子间相关性矩阵：计算选定因子组之间的相关性
4. 因子组合建议：基于分类分级结果推荐因子组合
"""
from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import numpy as np

from ...db.pg_pool import get_conn
from .factor_value_loader import FactorValueLoader
from .correlation_engine import CorrelationEngine, CorrelationResult

logger = logging.getLogger("aistock.quantevolver.factor_analyst")

# 因子分类类别定义
FACTOR_CATEGORIES = {
    "MOM": "动量因子",
    "VOL": "波动率因子",
    "LIQ": "流动性因子",
    "VAL": "价值因子",
    "QUAL": "质量因子",
    "CORR": "相关性因子",
    "TECH": "技术指标因子",
    "SIZE": "规模因子",
    "STAT": "统计因子",
    "MF": "资金流因子",
    "CHIP": "筹码因子",
    "ML": "机器学习因子",
}

# 因子评级定义
FACTOR_GRADES = {
    "S": "卓越 - 统一规则引擎≥90分+hard gates通过",
    "A": "优秀 - 统一规则引擎≥75分+hard gates通过",
    "B": "良好 - 统一规则引擎≥60分",
    "C": "一般 - 统一规则引擎≥45分",
    "D": "较差 - 统一规则引擎<45分",
}

# IC半衰期分界点（天）— 基于2026-04-10 P0分析确认
HALF_LIFE_SHORT_THRESHOLD = 8    # <8d → short
HALF_LIFE_LONG_THRESHOLD = 25    # >25d → long, 8-25d → medium


def classify_holding_period(half_life) -> str:
    """基于IC半衰期分类因子持仓周期。

    分界点来源: dual_pipeline_strategy_design_20260410.md §3.3 方案2
    """
    if half_life is None:
        return "unknown"
    try:
        hl = float(half_life)
    except (TypeError, ValueError):
        return "unknown"
    import math
    if math.isnan(hl) or math.isinf(hl):
        return "unknown"
    if hl < HALF_LIFE_SHORT_THRESHOLD:
        return "short"
    elif hl > HALF_LIFE_LONG_THRESHOLD:
        return "long"
    else:
        return "medium"


# ── Multi-Alpha 架构：4个新分类维度 ──────────────────────────────────
#
# data_source_group: 因子主要依赖的数据源组，用于多Alpha分组建模
# ts_info_density:   时序信息密度（决定用 TSDatasetH 还是 DatasetH）
# update_freq:       因子更新频率（决定时序窗口有效性）
# linearity:         因子-收益的线性关系强度（决定用 Ridge 还是 LGB）

# 数据源字段映射（基于 static_factors.parquet 90字段 + price_volume 基础字段）
_DATA_SOURCE_FIELD_MAP = {
    "price_volume": {"open", "close", "high", "low", "volume", "amount", "vwap",
                     "pre_close", "change", "pct_chg"},
    "money_flow":   None,   # 动态: 前缀 mf_
    "fundamental":  None,   # 动态: 前缀 bb_
    "valuation":    {"db_pe", "db_pb", "db_ps", "db_ps_ttm", "db_pe_ttm",
                     "db_dv_ratio", "db_dv_ttm", "db_total_mv", "db_circ_mv"},
    "liquidity":    {"db_turnover_rate", "db_turnover_rate_f", "db_volume_ratio",
                     "db_float_share", "db_free_share",
                     "liquidity_turnover", "liquidity_vol_ratio",
                     "size_log_mv"},
    "chip":         None,   # 动态: 前缀 cp_
    "sector":       None,   # 动态: 前缀 sw2_
}

# 数据源组前缀匹配（替代上面 None 的条目）
_DATA_SOURCE_PREFIX_MAP = {
    "mf_":  "money_flow",
    "bb_":  "fundamental",
    "cp_":  "chip",
    "sw2_": "sector",
}


def _extract_fields_from_code(code_text: Optional[str],
                              expression: Optional[str] = None) -> set:
    """从因子代码/表达式中提取数据列字段名。

    支持多种语法:
      - Qlib 表达式: $close, $volume, $mf_main_net_amt
      - Python 代码: df["close"], df['mf_main_net_amt']
      - 预计算字段: value_pe_inv, liquidity_turnover, size_log_mv
      - 反斜杠转义: value\\_pe\\_inv
    """
    import re
    fields = set()
    text = (code_text or "") + "\n" + (expression or "")
    if not text.strip():
        return fields

    # 去除 Markdown 转义反斜杠 (value\_pe\_inv → value_pe_inv)
    text_clean = text.replace("\\_", "_")

    # Qlib $field 语法
    for m in re.finditer(r'\$(\w+)', text_clean):
        fields.add(m.group(1))

    # df["field"] / df['field'] / data["field"] / any_df["field"]
    for m in re.finditer(r'''(?:\w*_?df|data)\[["']([\w_]+)["']\]''', text_clean):
        fields.add(m.group(1))

    # 常见短别名: pv["close"], mf["mf_main_net_amt"], bb["bb_eps"], cp["cp_winner_rate"], sw2["sw2_pe"]
    for m in re.finditer(r'''\b(?:pv|mf|bb|cp|sw2|db)\[["']([\w_]+)["']\]''', text_clean):
        fields.add(m.group(1))

    # 扫描已知前缀的字段引用（独立 word 边界）
    # 这些前缀只出现在因子数据字段中，不会与普通代码冲突
    known_prefixes = ("mf_", "bb_", "cp_", "sw2_", "db_", "value_", "liquidity_", "size_")
    for prefix in known_prefixes:
        for m in re.finditer(rf'\b({re.escape(prefix)}\w+)', text_clean):
            fields.add(m.group(1))

    return fields


def classify_data_source(code_text: Optional[str] = None,
                         expression: Optional[str] = None,
                         factor_name: Optional[str] = None) -> str:
    """根据因子代码/表达式中引用的字段，判定 data_source_group。

    Returns:
        "price_volume" | "money_flow" | "fundamental" | "valuation" |
        "liquidity" | "chip" | "sector" | "cross_dataset" | "unknown"
    """
    fields = _extract_fields_from_code(code_text, expression)
    if not fields and factor_name:
        # 兜底: 从因子名提取前缀线索
        name_lower = factor_name.lower()
        for prefix, group in _DATA_SOURCE_PREFIX_MAP.items():
            if prefix in name_lower:
                return group

    if not fields:
        return "unknown"

    # 匹配到哪些组
    group_hits = set()
    for field in fields:
        # 精确集合匹配
        for group, field_set in _DATA_SOURCE_FIELD_MAP.items():
            if field_set is not None and field in field_set:
                group_hits.add(group)
        # 前缀匹配
        for prefix, group in _DATA_SOURCE_PREFIX_MAP.items():
            if field.startswith(prefix):
                group_hits.add(group)

    if len(group_hits) == 0:
        return "unknown"
    elif len(group_hits) == 1:
        return group_hits.pop()
    else:
        # 使用 2+ 数据源 → cross_dataset
        # 例外：price_volume 作为基础数据源不计入跨源（几乎所有因子都用到）
        group_hits_excl_pv = group_hits - {"price_volume"}
        if len(group_hits_excl_pv) == 1:
            return group_hits_excl_pv.pop()
        return "cross_dataset"


def determine_update_freq(data_source_group: str) -> str:
    """根据数据源组判定因子更新频率。

    Returns: "daily" | "quarterly"
    """
    if data_source_group == "fundamental":
        return "quarterly"
    return "daily"


def compute_ts_info_density(factor_values) -> Optional[str]:
    """基于因子值的 autocorr 和 CV 判定时序信息密度。

    Args:
        factor_values: pd.Series with MultiIndex(datetime, instrument) 或 flat Series

    Returns:
        "high" | "medium" | "low" | None (数据不足时)

    标准:
        autocorr > 0.95          → low  (值变化极慢，历史无新信息)
        autocorr < 0.7 且 CV>0.05 → high (变化剧烈，轨迹本身是信号)
        其他                      → medium
    """
    try:
        import pandas as pd
        import math
        if factor_values is None or len(factor_values) == 0:
            return None

        # 按股票分组算滞后1自相关，取平均
        if isinstance(factor_values.index, pd.MultiIndex):
            # 假设第二级是 instrument
            try:
                autocorrs = factor_values.groupby(level=-1).apply(
                    lambda s: s.autocorr(lag=1) if len(s) > 5 else None
                )
                autocorrs = autocorrs.dropna()
                if len(autocorrs) == 0:
                    return None
                daily_autocorr = float(autocorrs.mean())
            except Exception:
                return None
        else:
            if len(factor_values) < 5:
                return None
            ac = factor_values.autocorr(lag=1)
            if ac is None or (isinstance(ac, float) and math.isnan(ac)):
                return None
            daily_autocorr = float(ac)

        mean = float(factor_values.mean())
        std = float(factor_values.std())
        cv = std / (abs(mean) + 1e-8)

        if math.isnan(daily_autocorr) or math.isinf(daily_autocorr):
            return None

        if daily_autocorr > 0.95:
            return "low"
        if daily_autocorr < 0.7 and cv > 0.05:
            return "high"
        return "medium"
    except Exception as e:
        logger.debug(f"compute_ts_info_density failed: {e}")
        return None


def compute_linearity(pearson_ic: Optional[float],
                      spearman_ic: Optional[float]) -> Optional[str]:
    """基于 |Spearman IC| / |Pearson IC| 比值判定因子-收益线性度。

    Returns:
        "linear"    — ratio < 1.3  (线性关系为主 → 推荐 Ridge)
        "nonlinear" — ratio >= 1.3 (非线性关系 → 推荐 LGB/树模型)
        None        — 指标缺失
    """
    if pearson_ic is None or spearman_ic is None:
        return None
    try:
        p = abs(float(pearson_ic))
        s = abs(float(spearman_ic))
        if p < 1e-6:
            # pearson 几乎为0但 spearman 有值 → 强非线性
            return "nonlinear" if s >= 0.01 else None
        ratio = s / p
        return "linear" if ratio < 1.3 else "nonlinear"
    except (TypeError, ValueError):
        return None


# 基于因子名称前缀的规则分类（不依赖LLM的快速分类）
RULE_BASED_CLASSIFICATION = {
    # 动量因子
    "ROC": "MOM", "MA": "MOM", "RSQR": "MOM", "RESI": "MOM",
    "KMID": "MOM", "KLEN": "MOM", "KUP": "MOM", "KLOW": "MOM", "KSFT": "MOM",
    # 波动率因子
    "STD": "VOL", "VSTD": "VOL", "BETA": "VOL",
    # 流动性因子
    "VMA": "LIQ", "WVMA": "LIQ", "VOLUME": "LIQ",
    "VSUMP": "LIQ", "VSUMN": "LIQ", "VSUMD": "LIQ",
    # 技术指标
    "RSV": "TECH", "MAX": "TECH", "MIN": "TECH",
    "IMAX": "TECH", "IMIN": "TECH", "IMXD": "TECH",
    "QTLU": "TECH", "QTLD": "TECH",
    # 相关性因子
    "CORR": "CORR", "CORD": "CORR",
    # 统计因子
    "CNTP": "STAT", "CNTN": "STAT", "CNTD": "STAT",
    "SUMP": "STAT", "SUMN": "STAT", "SUMD": "STAT",
    "RANK": "STAT",
    # 价格因子
    "OPEN": "TECH", "HIGH": "TECH", "LOW": "TECH", "CLOSE": "TECH", "VWAP": "TECH",
}


def _get_llm_client():
    """获取LLM客户端（复用RDAgent的litellm配置）。"""
    try:
        import litellm
        return litellm
    except ImportError:
        logger.warning("litellm未安装，将使用规则分类")
        return None


def _utc_now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_jsonable(obj):
    """递归将 Decimal/date/datetime 转为 JSON 可序列化类型。"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(i) for i in obj]
    return obj


def _classify_by_rules(factor_name: str, code_text: Optional[str] = None,
                       expression: Optional[str] = None) -> tuple:
    """基于规则的因子分类（不依赖LLM）。

    分类优先级（表达式和源代码优先，名称兜底）：
    1. 表达式/源代码中的数据列扫描（最可靠：直接看用了什么数据）
    2. 表达式/源代码中的计算逻辑扫描
    3. 因子名称关键词匹配
    4. Alpha158前缀兜底

    Returns:
        (category, reason) 或 (None, None)
    """
    name_upper = factor_name.upper()
    name_lower = factor_name.lower()

    # 合并表达式和源代码用于扫描
    code_combined = ""
    if expression:
        code_combined += expression.lower() + " "
    if code_text:
        code_combined += code_text.lower()

    # ── 1. 数据列扫描（最高优先级：直接看因子使用了什么数据源） ──
    if code_combined:
        _DATA_COL_RULES = [
            # 资金流因子：使用mf_*资金流数据列
            (["mf_buy_sm_vol", "mf_sell_sm_vol", "mf_buy_md_vol", "mf_sell_md_vol",
              "mf_buy_lg_vol", "mf_sell_lg_vol", "mf_buy_elg_vol", "mf_sell_elg_vol",
              "mf_net_amount", "mf_amount", "mf_buy_sm_amount", "mf_sell_sm_amount",
              "moneyflow", "net_mf_vol", "buy_elg_vol", "sell_elg_vol",
              "buy_lg_vol", "sell_lg_vol"], "MF", "使用资金流数据列"),
            # 筹码因子：使用cp_*筹码数据列
            (["cp_winner_rate", "cp_avg_cost", "cp_cost_5pct", "cp_cost_15pct",
              "cp_cost_50pct", "cp_cost_85pct", "cp_cost_95pct",
              "cyq_perf", "winner_rate", "avg_cost", "cost_5pct", "cost_15pct",
              "cp_concentration"], "CHIP", "使用筹码分布数据列"),
            # 基本面/价值因子：使用bb_*基本面数据列
            (["bb_pe_ttm", "bb_pe_dyn", "bb_pb_mrq", "bb_ps_ttm", "bb_pcf_ocf",
              "bb_total_mv", "bb_circ_mv", "bb_total_share", "bb_float_share",
              "bb_free_share", "bb_dv_ttm", "bb_dv_ratio",
              "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ocf", "dividend_yield",
              "total_mv", "circ_mv"], "VAL", "使用基本面/估值数据列"),
            # 日频基本面：使用db_*每日基本面数据列
            (["db_turnover_rate", "db_turnover_rate_f", "db_volume_ratio",
              "db_pe", "db_pe_ttm", "db_pb", "db_ps", "db_ps_ttm",
              "db_dv_ratio", "db_dv_ttm", "db_total_mv", "db_circ_mv",
              "turnover_rate", "volume_ratio"], "LIQ", "使用日频基本面/换手率数据列"),
        ]
        for keywords, category, reason_hint in _DATA_COL_RULES:
            matched = [kw for kw in keywords if kw in code_combined]
            if matched:
                return (category, f"数据列扫描：{reason_hint}({', '.join(matched[:3])})")

    # ── 2. 计算逻辑扫描（看表达式/代码中的计算模式） ──
    if code_combined:
        _LOGIC_RULES = [
            # 动量因子：价格变化率、收益率计算
            (["$close/ref($close", "ref($close", "roc(", "pct_chg",
              "/ref(", "close/ref", "close - ref", "close-ref",
              "df['close']/", "df[\"close\"]/", "rolling_return", "price_change", "log_return"], "MOM",
             "计算逻辑包含价格变化率/收益率"),
            # 波动率因子：标准差、波动率计算
            (["std($close", "std($volume", "std($high", "std($low",
              "std(close", "std(volume", "std(high", "std(low",
              "std(abs(", "volatil", "atr(", ".std()", "rolling_std",
              "np.std", "variance"], "VOL",
             "计算逻辑包含标准差/波动率"),
            # 相关性因子：相关性、协方差计算
            (["corr($close", "corr($volume", "corr(close", "corr(volume", "corr(", "correlation",
              "cov(", ".corr()", "np.corrcoef"], "CORR",
             "计算逻辑包含相关性/协方差"),
            # 流动性因子：成交量相关计算
            (["$volume", "mean($volume", "sum($volume", "$amount",
              "df['volume']", "df[\"volume\"]", "df['amount']", "df[\"amount\"]",
              "volume_ma", "vol_ma", "amount_ratio"], "LIQ",
             "计算逻辑基于成交量/成交额"),
            # 技术指标：极值、分位数、RSI等
            (["idxmax(", "idxmin(", "max($high", "min($low",
              "max(high", "min(low", "df['high']", "df['low']",
              "quantile(", "rsi(", "macd(", "bollinger",
              "rsv(", "rank($close", "rank(close"], "TECH",
             "计算逻辑包含技术指标模式"),
            # 统计因子：排名、计数、统计量
            (["rank(", "count(", "sum(if(", "mean(if(",
              "skew(", "kurt(", "zscore"], "STAT",
             "计算逻辑包含统计聚合"),
        ]
        for keywords, category, reason_hint in _LOGIC_RULES:
            matched = [kw for kw in keywords if kw in code_combined]
            if matched:
                return (category, f"计算逻辑扫描：{reason_hint}")

    # ── 3. 因子名称关键词匹配（次优先级） ──
    _NAME_KEYWORD_RULES = [
        (["momentum", "mom_", "roc_", "return_", "trend"], "MOM", "名称含动量关键词"),
        (["volatil", "vol_", "std_", "atr_", "variance", "swing"], "VOL", "名称含波动率关键词"),
        (["liquid", "turnover", "vwap", "amihud", "illiquid"], "LIQ", "名称含流动性关键词"),
        (["value", "valuation", "pe_", "pb_", "ps_", "pcf_", "dividend",
          "earning", "book_", "ep_", "bp_", "pe_inv", "pe_dyn"], "VAL", "名称含价值关键词"),
        (["quality", "roe_", "roa_", "profit", "margin", "growth",
          "leverage", "debt_", "asset_"], "QUAL", "名称含质量关键词"),
        (["corr_", "correlation", "beta_", "covar"], "CORR", "名称含相关性关键词"),
        (["rsi_", "macd_", "bollinger", "kdj_", "cci_",
          "obv_", "sar_", "williams", "stoch", "dmi_", "adx_"], "TECH", "名称含技术指标关键词"),
        (["size_", "market_cap", "cap_", "ln_cap", "log_cap"], "SIZE", "名称含规模关键词"),
        (["skew", "kurt", "zscore", "rank_", "quantile", "percentile",
          "deviation", "residual"], "STAT", "名称含统计关键词"),
        (["flow", "inflow", "outflow", "net_inflow", "moneyflow", "mf_",
          "fund_flow", "capital_flow", "buy_sell"], "MF", "名称含资金流关键词"),
        (["chip", "concentration", "cost_", "winner_rate", "cp_",
          "cyq_", "avg_cost"], "CHIP", "名称含筹码关键词"),
        (["sentiment", "composite", "combined", "enhanced"], "TECH", "名称含情绪/复合关键词"),
    ]
    for keywords, category, reason_hint in _NAME_KEYWORD_RULES:
        for kw in keywords:
            if kw in name_lower:
                return (category, f"名称关键词：{reason_hint}")

    # ── 4. Alpha158前缀兜底 ──
    for prefix, category in RULE_BASED_CLASSIFICATION.items():
        if name_upper.startswith(prefix):
            return (category, f"Alpha158前缀匹配：{prefix}")

    return (None, None)


def _get_official_grade(factor_name: str) -> Optional[str]:
    """从 qe_factor_official_ratings 读取当前激活版本的正式评级。

    正式评级仅由 FactorRatingService (UI 工具栏) 产出，
    此处只读不改，无评级时返回 None。
    """
    from ...db.pg_pool import get_conn
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT r.official_grade
                    FROM qe_factor_official_ratings r
                    JOIN aistock_factor_catalog c ON c.id = r.factor_catalog_id
                    WHERE c.factor_name = %s
                      AND r.rule_version = (
                          SELECT rule_version FROM qe_rating_rule_versions
                          WHERE status = 'active' ORDER BY activated_at DESC LIMIT 1
                      )
                    ORDER BY r.graded_at DESC
                    LIMIT 1
                """, (factor_name,))
                row = cur.fetchone()
                return row[0] if row else None
    except Exception:
        return None


def _determine_factor_dimension(factor_name: str, category: str,
                                code_text: Optional[str] = None,
                                expression: Optional[str] = None) -> str:
    """判断因子是截面因子还是时序因子。

    截面因子(cross_sectional)：在同一时间点对不同股票进行横向比较/排名
    时序因子(time_series)：对同一股票在不同时间点进行纵向分析

    Returns:
        "cross_sectional" 或 "time_series"
    """
    name_lower = factor_name.lower()
    code_combined = ""
    if expression:
        code_combined += expression.lower() + " "
    if code_text:
        code_combined += code_text.lower()

    # ── 明确的截面因子特征 ──
    cs_keywords = [
        "rank(", "csrank", "cross_sectional", "zscore(",
        "cs_", "cross_section", "percentile", "quantile",
        "neutralize", "industry_neutral", "sector_neutral",
        "relative_to_market", "market_cap_weighted",
    ]
    cs_name_keywords = ["rank", "zscore", "percentile", "relative", "cs_"]

    # ── 明确的时序因子特征 ──
    ts_keywords = [
        "ref($", "ref(", "roc(", "ma(", "ema(", "sma(",
        "std($", "rolling", "shift(", "diff(", "pct_change",
        "lag_", "lead_", "window", "lookback", "period",
        "mean($", "sum($", "max($", "min($",
        "corr($", "cov(", "beta(",
        ".rolling(", ".shift(", ".diff(", ".pct_change(",
        "idxmax(", "idxmin(", "trend", "momentum",
    ]
    ts_name_keywords = ["ma_", "ema_", "roc_", "momentum", "trend",
                        "rolling", "lag_", "std_", "vol_", "atr_"]

    # 代码/表达式匹配
    cs_score = sum(1 for kw in cs_keywords if kw in code_combined)
    ts_score = sum(1 for kw in ts_keywords if kw in code_combined)

    # 名称匹配
    cs_score += sum(1 for kw in cs_name_keywords if kw in name_lower)
    ts_score += sum(1 for kw in ts_name_keywords if kw in name_lower)

    # 基于分类的先验倾向
    # 截面倾向：价值、规模、质量（天然是截面比较）
    cs_categories = {"VAL", "SIZE", "QUAL"}
    # 时序倾向：动量、波动率、技术指标（天然是时间序列）
    ts_categories = {"MOM", "VOL", "TECH", "CORR"}

    if category in cs_categories:
        cs_score += 2
    elif category in ts_categories:
        ts_score += 2

    # 资金流、筹码、流动性、统计因子需要看具体实现
    # 默认偏时序（因为大多数因子是基于时间窗口计算的）
    if cs_score == 0 and ts_score == 0:
        ts_score += 1

    return "cross_sectional" if cs_score > ts_score else "time_series"


def _generate_description_by_rules(factor_name: str, category: str,
                                    code_text: Optional[str] = None,
                                    expression: Optional[str] = None) -> str:
    """基于规则生成因子描述（100-200字），从金融设计思路角度描述。

    参考RDAgent假设阶段风格：说明因子捕捉什么市场异象、
    基于什么金融逻辑预期有效，不显示计算公式。
    """
    name_lower = factor_name.lower()

    # 合并代码用于分析
    code_combined = ""
    if expression:
        code_combined += expression.lower() + " "
    if code_text:
        code_combined += code_text.lower()

    # ── 1. 识别因子使用的数据源 ──
    data_sources = []
    _DATA_SOURCE_MAP = [
        (["mf_buy", "mf_sell", "mf_net", "mf_amount", "moneyflow",
          "buy_elg_vol", "sell_elg_vol", "buy_lg_vol", "sell_lg_vol"], "主力资金流向"),
        (["cp_winner_rate", "cp_avg_cost", "cp_cost_", "winner_rate",
          "avg_cost", "cyq_perf", "concentration"], "筹码分布"),
        (["bb_pe", "bb_pb", "bb_ps", "bb_pcf", "bb_total_mv", "bb_circ_mv",
          "bb_dv", "pe_ttm", "pb_mrq", "ps_ttm", "dividend"], "基本面估值"),
        (["db_turnover", "db_volume_ratio", "db_pe", "db_pb",
          "turnover_rate", "volume_ratio"], "日频换手/估值"),
        (["$close", "$open", "$high", "$low", "close", "open", "high", "low",
          "df['close']", "df['open']", "df['high']", "df['low']"], "日频价格"),
        (["$volume", "$amount", "volume", "amount",
          "df['volume']", "df['amount']"], "成交量"),
    ]
    for keywords, label in _DATA_SOURCE_MAP:
        if any(kw in code_combined for kw in keywords):
            data_sources.append(label)

    # ── 2. 识别因子的金融逻辑/市场异象 ──
    anomalies = []
    _ANOMALY_MAP = [
        # 动量/反转
        (["ref($close", "close/ref", "pct_chg", "roc(",
          "df['close']/", "df[\"close\"]/", "rolling_return", "price_change", "log_return",
          "daily_return", "pct_change"], "价格动量效应"),
        (["reversal", "mean_revert", "contrarian"], "均值回归/反转效应"),
        # 波动
        (["std(", ".std()", "volatil", "atr(", "variance",
          "np.std", "rolling_std"], "波动率异象"),
        # 资金流
        (["net_inflow", "inflow", "outflow", "buy.*sell",
          "mf_net", "mf_buy.*mf_sell"], "聪明资金流向信号"),
        # 筹码
        (["winner_rate", "avg_cost", "cost.*close",
          "concentration", "cp_cost"], "筹码获利盘/成本结构"),
        # 估值
        (["pe_", "pb_", "ps_", "pcf_", "1.0/", "1/",
          "pe_inv", "pe_dyn", "dividend"], "估值因子溢价"),
        # 流动性
        (["turnover", "volume.*mean", "amihud",
          "illiquid", "volume_ratio"], "流动性溢价"),
        # 量价背离
        (["corr($close", "corr(close", "corr(", ".corr()", "volume.*close",
          "price.*volume"], "量价关系/背离"),
        # 技术形态
        (["idxmax(", "idxmin(", "max($high", "min($low",
          "max(high", "min(low", "rsv(", "rsi(", "macd("], "技术形态/超买超卖"),
        # 截面排名
        (["rank(", "csrank", "cross_sectional", "zscore"], "截面相对强弱"),
    ]
    for keywords, label in _ANOMALY_MAP:
        if any(kw in code_combined for kw in keywords):
            if label not in anomalies:
                anomalies.append(label)

    # ── 3. 从名称补充语义 ──
    _NAME_SEMANTICS = {
        "momentum": "动量效应", "trend": "趋势跟踪", "reversal": "反转效应",
        "sentiment": "市场情绪", "pressure": "买卖压力", "mismatch": "供需错配",
        "divergence": "量价背离", "enhanced": "增强型信号", "adjusted": "风险调整后",
        "dynamic": "动态自适应", "composite": "多维度复合", "ratio": "比率结构",
        "intensity": "强度信号", "deviation": "偏离度",
    }
    name_hints = []
    for kw, hint in _NAME_SEMANTICS.items():
        if kw in name_lower and hint not in anomalies:
            name_hints.append(hint)

    # ── 4. 组装金融设计思路描述 ──
    cat_name = FACTOR_CATEGORIES.get(category, category)

    # 核心假设/设计思路
    if anomalies:
        core = f"该因子基于{anomalies[0]}构建"
        if len(anomalies) > 1:
            core += f"，同时融合{anomalies[1]}的信息"
    elif name_hints:
        core = f"该因子旨在捕捉{name_hints[0]}信号"
    else:
        core = f"该因子属于{cat_name}类别"

    # 数据来源
    if data_sources:
        unique_sources = list(dict.fromkeys(data_sources))[:3]
        data_part = f"，利用{'/'.join(unique_sources)}数据"
    else:
        data_part = ""

    # 预期有效性逻辑
    _EFFECTIVENESS = {
        "MOM": "。动量因子在趋势市场中表现优异，能够捕捉价格惯性带来的超额收益",
        "VOL": "。低波动异象表明波动率与预期收益存在负相关，波动率因子可用于风险调整和择时",
        "LIQ": "。流动性溢价理论认为低流动性资产应获得更高补偿，该因子有助于识别流动性错配机会",
        "VAL": "。价值因子利用市场对基本面信息的定价偏差，长期来看低估值股票倾向于获得超额收益",
        "QUAL": "。高质量公司通常具有更稳定的盈利能力和更强的抗风险能力，长期表现优于低质量公司",
        "CORR": "。量价相关性变化反映市场微观结构信息，有助于识别趋势确认或背离信号",
        "TECH": "。技术指标通过价格形态识别市场超买超卖状态，在短期择时中具有参考价值",
        "SIZE": "。小市值效应是经典的市场异象之一，小盘股长期倾向于获得超额收益",
        "STAT": "。统计因子通过截面排名和分布特征识别相对强弱，有助于构建多空组合",
        "MF": "。资金流向反映机构投资者的交易意图，主力资金净流入往往预示短期正向收益",
        "CHIP": "。筹码分布反映持仓成本结构，获利盘比例和成本偏离度是重要的支撑/压力信号",
        "ML": "。机器学习因子通过非线性建模捕捉传统因子难以发现的复杂模式",
    }
    effectiveness = _EFFECTIVENESS.get(category, "")

    # 使用场景
    _USAGE_SCENARIO = {
        "MOM": "。适合趋势明确的市场环境，中短线持仓效果较好",
        "VOL": "。适合震荡市中的风险管理和择时，可用于构建低波动组合",
        "LIQ": "。适合中长线价值投资，流动性溢价在小盘股中更显著",
        "VAL": "。适合价值投资风格，在市场回归理性时表现突出",
        "QUAL": "。适合保守型长线投资，在熊市中具有较好的防御性",
        "CORR": "。适合短线量价分析，在量价背离时信号较强",
        "TECH": "。适合短线交易和择时，在超买超卖区间信号较强",
        "SIZE": "。适合小盘股策略，在流动性充裕的市场环境中效果更好",
        "STAT": "。适合截面选股，通过相对排名识别强弱股",
        "MF": "。适合短中线交易，主力资金信号在个股层面更有效",
        "CHIP": "。适合中线波段操作，筹码结构变化预示支撑压力位",
        "ML": "。适合多因子组合，作为非线性信号补充传统因子",
    }
    usage_scenario = _USAGE_SCENARIO.get(category, "")

    # 组合搭配建议
    _COMBO_ADVICE = {
        "MOM": "。建议搭配价值/波动率因子形成风格对冲",
        "VOL": "。建议搭配动量因子平衡风险收益",
        "LIQ": "。建议搭配价值/质量因子增强选股稳定性",
        "VAL": "。建议搭配动量/资金流因子提升择时能力",
        "QUAL": "。适合保守型组合，搭配价值因子效果更佳",
        "CORR": "。建议搭配动量/技术因子验证趋势信号",
        "TECH": "。建议搭配资金流/筹码因子增强短期信号",
        "SIZE": "。建议搭配质量/价值因子控制小盘风险",
        "STAT": "。建议搭配多类别因子提升截面区分度",
        "MF": "。建议搭配筹码因子形成资金-筹码共振信号",
        "CHIP": "。建议搭配资金流因子形成筹码-资金共振信号",
        "ML": "。建议搭配传统因子提供可解释性补充",
    }
    combo_advice = _COMBO_ADVICE.get(category, "")

    desc = core + data_part + effectiveness + usage_scenario + combo_advice
    return desc[:250]


def _analyze_factor_v2(
    factor_name: str,
    factor_source: str,
    expression: Optional[str],
    code_text: Optional[str],
    metrics: Dict[str, Any],
    official_grade: Optional[str],
) -> Optional[Dict[str, Any]]:
    """合并后的单次 LLM 调用：分类 + 评级解释 + 描述生成。

    注意：official_grade 由 FactorRatingService 统一规则引擎给出，LLM 不得修改。
    LLM 仅做分类、维度判断、描述生成和评级解释。

    Returns:
        解析后的 JSON dict，包含 category, grade_reason, dimension,
        description, usage_guidance, risk_notes；失败返回 None。
    """
    llm = _get_llm_client()
    if llm is None:
        return None

    from .prompt_manager import PromptManager
    pm = PromptManager()
    prompt_data = pm.get_active_prompt_text("factor_analyst", "analyze_factor_v2")

    # 构建指标文本
    def _fmt(v, pct=False):
        if v is None:
            return "N/A"
        if pct:
            return f"{float(v):.2%}" if isinstance(v, (int, float, Decimal)) else str(v)
        return f"{float(v):.4f}" if isinstance(v, (float, Decimal)) else str(v)

    has_metrics = bool(metrics and metrics.get('ic_mean') is not None)

    if has_metrics:
        # 多持有期IC
        multi_holding = ""
        if metrics.get('rank_ic_1d') is not None:
            multi_holding = f"\n- 多持有期Rank IC: 1日={_fmt(metrics.get('rank_ic_1d'))} | 5日={_fmt(metrics.get('rank_ic_5d'))} | 10日={_fmt(metrics.get('rank_ic_10d'))} | 20日={_fmt(metrics.get('rank_ic_20d'))}"
        if metrics.get('ic_csz_mean') is not None:
            multi_holding += f" | 截面IC均值={_fmt(metrics.get('ic_csz_mean'))}"

        metrics_block = f"""## 独立评测指标
- IC均值: {_fmt(metrics.get('ic_mean'))} | Rank IC均值: {_fmt(metrics.get('rank_ic_mean'))}
- ICIR: {_fmt(metrics.get('icir'))} | Rank ICIR: {_fmt(metrics.get('rank_icir'))} | 年化ICIR: {_fmt(metrics.get('icir_annualized'))} | 年化Rank ICIR: {_fmt(metrics.get('rank_icir_annualized'))}{multi_holding}
- IC正比例: {_fmt(metrics.get('ic_positive_ratio'))} | IC衰减半衰期: {metrics.get('ic_decay_half_life', 'N/A')}天
- 多空超额Sharpe: {_fmt(metrics.get('top_excess_sharpe'))} | 多空超额年化: {_fmt(metrics.get('top_excess_annual_return'), True)}
- 分组单调性: {_fmt(metrics.get('group_return_monotonicity'))} | 换手率: {_fmt(metrics.get('turnover'), True)}
- 覆盖率: {_fmt(metrics.get('coverage'))} | 交易天数: {metrics.get('n_trading_days', 'N/A')}

## 纯多头收益
- 多头年化: {_fmt(metrics.get('top_annual_return'), True)}
- 多头最大回撤: {_fmt(metrics.get('top_max_drawdown'), True)}
- 基准年化: {_fmt(metrics.get('benchmark_annual_return'), True)}"""

        # 多窗口稳定性
        multi_window = metrics.get('multi_window')
        if multi_window:
            window_lines = []
            for window_name in ['out_sample', 'recent_6m', 'recent_3m']:
                if window_name in multi_window:
                    w = multi_window[window_name]
                    window_lines.append(f"- {window_name}: IC={_fmt(w.get('ic_mean'))} ICIR={_fmt(w.get('icir'))} 年化ICIR={_fmt(w.get('icir_annualized'))} RankIC={_fmt(w.get('rank_ic_mean'))} RankICIR={_fmt(w.get('rank_icir'))}")
            if window_lines:
                metrics_block += "\n\n## 多窗口稳定性\n" + "\n".join(window_lines)

        official_grade_line = f"\n## 正式评级: {official_grade or '未评级'}（由统一规则引擎给出，你不能修改）"
    else:
        metrics_block = "## 独立评测指标\n尚未计算独立指标，请仅根据因子代码和表达式进行分析分类。"
        official_grade_line = f"\n## 正式评级: {official_grade or '未评级'}"

    user_prompt = f"""## 因子信息
- 名称: {factor_name}
- 来源: {factor_source}
- 表达式: {expression or '无'}
- 代码片段: {(code_text or '')[:1000]}

{metrics_block}
{official_grade_line}

请综合以上信息，输出 JSON。"""

    if prompt_data:
        system_prompt = prompt_data["system_prompt"]
        # 安全兜底：即使 DB 级提示词包含旧版评级指令，也追加只读约束
        grade_guard = f"\n\n【重要】该因子的正式评级为 **{official_grade or '未评级'}**，由统一规则引擎（FactorRatingService）给出。你不能修改评级，只能解释。不要输出 S/A/B/C/D 等级字母。"
        system_prompt += grade_guard
    else:
        # 内置兜底 system prompt
        system_prompt = _get_default_v2_system_prompt(official_grade=official_grade)

    try:
        from .llm_client import get_llm_kwargs
        kwargs = get_llm_kwargs("factor_analyst")

        response = llm.completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=1500,
            response_format={"type": "json_object"},
            **kwargs
        )
        content = response.choices[0].message.content.strip()
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        result = json.loads(content)

        # 校验必要字段（LLM 不再输出 grade，只需 category）
        if "category" not in result:
            logger.warning(f"LLM v2 输出缺少分类字段 ({factor_name}): {list(result.keys())}")
            return None

        # 规范化
        cat = str(result["category"]).upper()
        if cat not in FACTOR_CATEGORIES:
            cat = "TECH"
        result["category"] = cat

        return result
    except Exception as e:
        logger.warning(f"LLM v2 分析失败 ({factor_name}): {e}")
        return None


def _get_default_v2_system_prompt(official_grade: Optional[str] = None) -> str:
    """内置的 factor_analyst/analyze_factor_v2 兜底 system prompt。
    
    正式评级由 FactorRatingService 统一规则引擎给出，LLM 只做分类、描述和评级解释，
    不得修改评级结果。
    """
    categories_desc = "\n".join(f"- {k}: {v}" for k, v in FACTOR_CATEGORIES.items())
    grade_instruction = f"该因子的正式评级为 **{official_grade}**（由统一规则引擎计算，你不能修改）。" if official_grade else "该因子尚未经过正式评级（需通过评级管理工具栏触发）。"
    return f"""你是一个专业的量化因子分析师。请根据提供的因子信息和独立评测指标，完成以下任务：

1. **分类**：将因子归入以下 12 类之一：
{categories_desc}

   **易混淆分类示例**：
   - MOM vs TECH：基于价格变化率/收益率 → MOM；基于技术指标公式(RSI/MACD/布林带) → TECH
   - STAT vs MOM：用统计算子(rank/zscore/percentile)包装动量信号 → MOM（看核心逻辑而非算子）
   - VOL vs LIQ：波动率(std/atr/价格振幅) → VOL；换手率/成交量/流动性比率 → LIQ
   - MF vs LIQ：资金流净额(mf_main_net/mf_elg_net) → MF；成交量比率/换手率 → LIQ

2. **评级解释**（只读，禁止修改评级）：
   {grade_instruction}
   你需要做的是：
   - 解释该评级是否合理，基于指标数据给出分析理由
   - 指出该因子的主要优势和风险项
   - 如有边界情况，给出人工复核建议
   **绝对不要**输出自己的评级（S/A/B/C/D），grade_reason 仅用于解释，不用于覆盖正式评级。

3. **维度判断**：判断因子是截面型(cross_sectional)还是时序型(time_series)。

   **权威定义**：
   - **截面因子(cross_sectional)**：在同一时间点对不同股票进行横向比较/排名。特征：使用rank/zscore/percentile等截面算子，或基于行业/市值等横向分组。
   - **时序因子(time_series)**：对同一股票在不同时间点进行纵向分析。特征：使用时间窗口(如MA5/STD20/ROC10)、滚动计算、滞后项(ref/shift)等时序算子。

   **判断标准**：
   - 若因子名称或表达式包含时间窗口参数(如MA5、STD20、ROC10)，必须判断为time_series
   - 若使用rolling/shift/ref/ma/std/roc等时序算子，必须判断为time_series
   - 若仅使用rank/zscore/percentile等截面算子且无时间窗口，判断为cross_sectional

4. **描述生成**：生成 300-500 字的可读文本描述，涵盖：核心逻辑 + 指标解读 + 适用场景 + 组合建议 + 风险提示。

5. **使用指引**：给出组合使用建议。

请严格输出以下 JSON 格式：
{{{{
  "category": "12类代码之一",
  "category_reason": "分类理由",
  "grade_reason": "对正式评级的解释说明（不输出评级字母）",
  "dimension": "cross_sectional 或 time_series",
  "description": "300-500字可读文本描述",
  "usage_guidance": {{{{
    "optimal_holding_period": "Nd 或 Nd-Md",
    "market_regime_fit": "适用市场环境",
    "complement_categories": ["互补类别代码"],
    "conflict_categories": ["冲突类别代码"],
    "combo_role": "核心因子/辅助因子/对冲因子",
    "suggested_weight_range": [min, max]
  }}}},
  "risk_notes": ["风险提示1", "风险提示2"]
}}}}"""



class FactorAnalyst:
    """因子分析师Agent。"""

    def analyze_single_factor(
        self,
        factor_name: str,
        factor_source: str,
        use_llm: bool = False,
    ) -> Dict[str, Any]:
        """分析单个因子：分类 + 描述。评级只读，从正式评级表读取。

        use_llm=True 时走 v2 合并调用（单次 LLM），否则走规则。
        正式评级由 FactorRatingService (UI 评级管理工具栏) 统一产出，
        此方法只做分类和描述，不得修改评级。
        """
        factor_info = self._get_factor_info(factor_name, factor_source)
        if not factor_info:
            return {"ok": False, "error": f"因子 {factor_name} (source={factor_source}) 不存在"}

        expression = factor_info.get("expression")
        code_text = factor_info.get("code_text")
        ic = factor_info.get("ic")
        sharpe = factor_info.get("sharpe")
        ann_ret = factor_info.get("annualized_return") or factor_info.get("best_performance_ann_ret")

        # 获取独立因子指标
        ind = self._get_independent_metrics(factor_name) or {}

        # 有独立指标 → 提取关键值
        ic = ind.get("ic_mean") or ic
        sharpe = ind.get("top_excess_sharpe") or sharpe
        ann_ret = ind.get("top_excess_annual_return") or ann_ret
        icir_ann = ind.get("icir_annualized")
        _hp_class = classify_holding_period(ind.get("ic_decay_half_life"))

        # ── 正式评级：只读，从 qe_factor_official_ratings 读取 ──
        # 正式评级唯一入口是 FactorRatingService (UI 评级管理工具栏)
        official_grade = _get_official_grade(factor_name)

        # ── Multi-Alpha 架构: 4个新分类维度 ──────────────────────────
        ds_group = classify_data_source(
            code_text=code_text, expression=expression, factor_name=factor_name)
        upd_freq = determine_update_freq(ds_group)
        linearity = compute_linearity(
            pearson_ic=ind.get("ic_mean"),
            spearman_ic=ind.get("rank_ic_mean"),
        )
        # ts_info_density 需要因子值数据，这里暂留 None (后续批量计算)
        ts_density = None

        # 获取多窗口稳定性指标
        multi_window = self._get_multi_window_metrics(factor_name)
        if multi_window:
            ind['multi_window'] = multi_window

        # ── v2 合并 LLM 调用 ──
        factor_profile = None
        if use_llm:
            v2_result = _analyze_factor_v2(
                factor_name, factor_source, expression, code_text, ind, official_grade)
            if v2_result:
                llm_category = v2_result["category"]
                llm_dimension = v2_result.get("dimension", "time_series")

                # ── 双重校验机制：LLM ↔ 规则引擎交叉验证 ──
                rule_category, rule_reason = _classify_by_rules(factor_name, code_text=code_text, expression=expression)
                rule_dimension = _determine_factor_dimension(factor_name, llm_category, code_text=code_text, expression=expression)

                # 计算规则置信度（基于匹配强度）
                rule_confidence = 0
                if rule_category:
                    # 数据列扫描（最高置信度=3）
                    if "数据列扫描" in rule_reason:
                        rule_confidence = 3
                    # 计算逻辑扫描（中等置信度=2）
                    elif "计算逻辑扫描" in rule_reason:
                        rule_confidence = 2
                    # 名称关键词匹配（低置信度=1）
                    else:
                        rule_confidence = 1

                # 规则置信度≥2时，以规则为准
                if rule_confidence >= 2 and rule_category and rule_category != llm_category:
                    category = rule_category
                    classification_reason = f"双重校验：规则分类(置信度{rule_confidence})优先于LLM。{rule_reason}"
                    logger.info(f"因子{factor_name}分类校验：LLM={llm_category} vs 规则={rule_category}(置信度{rule_confidence})，采用规则分类")
                else:
                    category = llm_category
                    classification_reason = v2_result.get("category_reason", "LLM v2 分类")
                    if rule_category and rule_category != llm_category:
                        classification_reason += f" (规则建议:{rule_category},置信度{rule_confidence})"

                # 维度校验：规则引擎的维度判断更可靠
                if rule_dimension != llm_dimension:
                    factor_dimension = rule_dimension
                    logger.info(f"因子{factor_name}维度校验：LLM={llm_dimension} vs 规则={rule_dimension}，采用规则判断")
                else:
                    factor_dimension = llm_dimension

                grade = official_grade or "N/A"
                grade_reason = v2_result.get("grade_reason", "")
                # 规范化维度值
                if factor_dimension == "cross_section":
                    factor_dimension = "cross_sectional"
                description = v2_result.get("description", "")

                # 构建 factor_profile
                factor_profile = {
                    "category": category,
                    "category_reason": classification_reason,
                    "grade_reason": grade_reason,
                    "dimension": factor_dimension,
                    "metrics_summary": _to_jsonable({k: v for k, v in ind.items() if v is not None}),
                    "usage_guidance": v2_result.get("usage_guidance", {}),
                    "risk_notes": v2_result.get("risk_notes", []),
                }

                # 聚合 L2 实验数据
                exp_track = self._get_experiment_track_summary(factor_name)
                if exp_track:
                    factor_profile["experiment_track"] = _to_jsonable(exp_track)

                self._upsert_classification(
                    factor_name=factor_name, factor_source=factor_source,
                    category=category,
                    grade_reason=grade_reason,
                    classification_reason=classification_reason,
                    ic_value=ic, sharpe_value=sharpe, ann_ret_value=ann_ret,
                    llm_analysis=classification_reason,
                    description=description, factor_dimension=factor_dimension,
                    factor_profile=factor_profile,
                    holding_period_class=_hp_class,
                    data_source_group=ds_group,
                    ts_info_density=ts_density,
                    update_freq=upd_freq,
                    linearity=linearity,
                )
                return {
                    "ok": True, "factor_name": factor_name,
                    "factor_source": factor_source,
                    "category": category,
                    "category_name": FACTOR_CATEGORIES.get(category, category),
                    "factor_dimension": factor_dimension,
                    "grade": grade,
                    "grade_name": FACTOR_GRADES.get(grade, grade),
                    "grade_reason": grade_reason,
                    "classification_reason": classification_reason,
                    "description": description,
                    "ic": ic, "sharpe": sharpe, "ann_ret": ann_ret,
                }

        # ── fallback: 规则路径（use_llm=False 或 LLM 失败）──
        category, classification_reason = _classify_by_rules(
            factor_name, code_text=code_text, expression=expression)
        if not category:
            category = "TECH"
            classification_reason = f"默认分类(source={factor_source})"
        else:
            classification_reason = f"规则分类: {classification_reason}"

        grade = official_grade or "N/A"
        grade_reason = f"[正式评级] {official_grade or '未评级'} | 指标参考: IC={ic}, Sharpe={sharpe}, AnnRet={ann_ret}"
        factor_dimension = _determine_factor_dimension(
            factor_name, category, code_text=code_text, expression=expression)
        description = _generate_description_by_rules(
            factor_name, category, code_text=code_text, expression=expression)

        self._upsert_classification(
            factor_name=factor_name, factor_source=factor_source,
            category=category,
            grade_reason=grade_reason,
            classification_reason=classification_reason,
            ic_value=ic, sharpe_value=sharpe, ann_ret_value=ann_ret,
            llm_analysis=None, description=description,
            factor_dimension=factor_dimension, factor_profile=None,
            holding_period_class=_hp_class,
            data_source_group=ds_group,
            ts_info_density=ts_density,
            update_freq=upd_freq,
            linearity=linearity,
        )

        return {
            "ok": True, "factor_name": factor_name,
            "factor_source": factor_source,
            "category": category,
            "category_name": FACTOR_CATEGORIES.get(category, category),
            "factor_dimension": factor_dimension,
            "grade": grade,
            "grade_name": FACTOR_GRADES.get(grade, grade),
            "grade_reason": grade_reason,
            "classification_reason": classification_reason,
            "description": description,
            "ic": ic, "sharpe": sharpe, "ann_ret": ann_ret,
        }

    async def batch_analyze_all_factors_async(
        self,
        use_llm: bool = False,
        source_filter: Optional[str] = None,
        factor_names: Optional[List[str]] = None,
    ):
        """批量分析因子（异步生成器，支持SSE流式推送）。

        Yields:
            进度事件字典: {"type": "progress", "current": int, "total": int, "factor_name": str}
            或错误事件: {"type": "error", "factor_name": str, "error": str}
            或完成事件: {"type": "done", "analyzed": int, "total": int, "errors": list}
        """
        import asyncio

        factors = self._get_all_factors(source_filter)
        if factor_names:
            name_set = set(factor_names)
            factors = [f for f in factors if f["factor_name"] in name_set]

        total = len(factors)
        analyzed = 0
        errors = []
        semaphore = asyncio.Semaphore(5)

        async def analyze_one(f, index):
            async with semaphore:
                try:
                    await asyncio.to_thread(
                        self.analyze_single_factor,
                        factor_name=f["factor_name"],
                        factor_source=f["source"],
                        use_llm=use_llm,
                    )
                    return {"ok": True, "factor_name": f["factor_name"], "index": index}
                except Exception as e:
                    return {"ok": False, "factor_name": f["factor_name"], "error": str(e), "index": index}

        tasks = [analyze_one(f, i) for i, f in enumerate(factors, 1)]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result["ok"]:
                analyzed += 1
                yield {"type": "progress", "current": analyzed, "total": total, "factor_name": result["factor_name"]}
            else:
                errors.append(f"{result['factor_name']}: {result['error']}")
                yield {"type": "error", "factor_name": result["factor_name"], "error": result["error"]}

        yield {"type": "done", "analyzed": analyzed, "total": total, "errors": errors}

    def batch_analyze_all_factors(
        self,
        use_llm: bool = False,
        source_filter: Optional[str] = None,
        factor_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """批量分析因子（同步版本，向后兼容）。"""
        factors = self._get_all_factors(source_filter)
        if factor_names:
            name_set = set(factor_names)
            factors = [f for f in factors if f["factor_name"] in name_set]
        total = len(factors)
        analyzed = 0
        errors = []

        for f in factors:
            try:
                self.analyze_single_factor(
                    factor_name=f["factor_name"],
                    factor_source=f["source"],
                    use_llm=use_llm,
                )
                analyzed += 1
            except Exception as e:
                errors.append(f"{f['factor_name']}: {e}")

        return {
            "ok": len(errors) == 0,
            "total": total,
            "analyzed": analyzed,
            "errors": errors,
        }

    def clear_classifications(self, source_filter: Optional[str] = None) -> int:
        """清空因子分类结果。

        Args:
            source_filter: 可选的source过滤，不提供则清空全部

        Returns:
            删除的记录数
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                if source_filter:
                    cur.execute("DELETE FROM qe_factor_classification WHERE factor_source = %s", (source_filter,))
                else:
                    cur.execute("DELETE FROM qe_factor_classification")
                return cur.rowcount

    def get_classifications(
        self,
        source_filter: Optional[str] = None,
        exclude_source_filter: Optional[str] = None,
        category_filter: Optional[str] = None,
        grade_filter: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """获取因子分类结果列表。"""
        conditions = []
        params = []

        if source_filter:
            conditions.append("factor_source = %s")
            params.append(source_filter)
        if exclude_source_filter:
            ex_list = [s.strip() for s in exclude_source_filter.split(",") if s.strip()]
            if ex_list:
                placeholders = ",".join(["%s"] * len(ex_list))
                conditions.append(f"factor_source NOT IN ({placeholders})")
                params.extend(ex_list)
        if category_filter:
            conditions.append("category = %s")
            params.append(category_filter)
        if grade_filter:
            conditions.append("grade = %s")
            params.append(grade_filter)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 总数
                cur.execute(
                    f"SELECT COUNT(*) FROM qe_factor_classification WHERE {where_clause}",
                    params,
                )
                total = cur.fetchone()[0]

                # 数据
                cur.execute(
                    f"""SELECT id, factor_name, factor_source, category, grade,
                               grade_reason, classification_reason, ic_value,
                               sharpe_value, ann_ret_value, description,
                               factor_dimension, factor_profile, analyzed_at
                        FROM qe_factor_classification
                        WHERE {where_clause}
                        ORDER BY grade ASC, ic_value DESC NULLS LAST
                        LIMIT %s OFFSET %s""",
                    params + [limit, offset],
                )
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        return {"ok": True, "total": total, "items": rows}

    def recommend_factor_combination(
        self,
        target_count: int = 20,
        include_categories: Optional[List[str]] = None,
        min_grade: str = "C",
        diversity_weight: float = 0.5,
    ) -> Dict[str, Any]:
        """推荐因子组合。

        策略：
        1. 按评级排序，优先选高评级因子
        2. 保证类别多样性（每个类别至少选1个）
        3. 避免同类别因子过多（单类别不超过总数30%）

        Args:
            target_count: 目标因子数量
            include_categories: 指定包含的类别
            min_grade: 最低评级
            diversity_weight: 多样性权重(0-1)

        Returns:
            推荐的因子列表
        """
        grade_order = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4}
        min_grade_val = grade_order.get(min_grade, 3)

        # 获取所有已分类因子
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT c.factor_name, c.factor_source, c.category, c.grade,
                           c.ic_value, c.sharpe_value, c.ann_ret_value,
                           c.factor_profile
                    FROM qe_factor_classification c
                    WHERE c.grade IS NOT NULL
                    ORDER BY
                        CASE c.grade
                            WHEN 'S' THEN 0 WHEN 'A' THEN 1 WHEN 'B' THEN 2
                            WHEN 'C' THEN 3 WHEN 'D' THEN 4 ELSE 5
                        END ASC,
                        c.ic_value DESC NULLS LAST
                """
                cur.execute(sql)
                cols = [desc[0] for desc in cur.description]
                all_factors = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 过滤
        candidates = []
        for f in all_factors:
            f_grade_val = grade_order.get(f["grade"], 5)
            if f_grade_val > min_grade_val:
                continue
            if include_categories and f["category"] not in include_categories:
                continue
            candidates.append(f)

        if not candidates:
            return {"ok": True, "factors": [], "message": "无符合条件的因子"}

        # 构建互补/冲突类别映射
        complement_map: Dict[str, List[str]] = {}  # category -> complementary categories
        conflict_map: Dict[str, List[str]] = {}   # category -> conflicting categories
        for f in candidates:
            profile = f.get("factor_profile") or {}
            if isinstance(profile, str):
                try:
                    profile = json.loads(profile)
                except Exception:
                    profile = {}
            usage = profile.get("usage_guidance", {})
            cat = f["category"]
            if usage.get("complementary_categories") and cat not in complement_map:
                complement_map[cat] = usage["complementary_categories"]
            if usage.get("conflicting_categories") and cat not in conflict_map:
                conflict_map[cat] = usage["conflicting_categories"]

        # 加载已有相关性数据
        candidate_names = [f["factor_name"] for f in candidates]
        correlation_data: Dict[str, float] = {}
        if len(candidate_names) >= 2:
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        # 先获取 factor_name -> catalog_id 映射
                        ph = ",".join(["%s"] * len(candidate_names))
                        cur.execute(f"""
                            SELECT DISTINCT ON (factor_name) factor_name, id
                            FROM aistock_factor_catalog
                            WHERE factor_name IN ({ph})
                            ORDER BY factor_name, id
                        """, candidate_names)
                        id_map = {row[0]: row[1] for row in cur.fetchall()}
                        id_to_name = {v: k for k, v in id_map.items()}
                        factor_ids = list(id_map.values())

                        if len(factor_ids) >= 2:
                            cur.execute("""
                                SELECT factor_a_id, factor_b_id, correlation
                                FROM qe_factor_correlations
                                WHERE factor_a_id = ANY(%s) AND factor_b_id = ANY(%s)
                            """, (factor_ids, factor_ids))
                            for row in cur.fetchall():
                                name_a = id_to_name.get(row[0], "")
                                name_b = id_to_name.get(row[1], "")
                                correlation_data[f"{name_a}_{name_b}"] = float(row[2])
                                correlation_data[f"{name_b}_{name_a}"] = float(row[2])
            except Exception:
                pass

        # 选择策略：先保证多样性，再按评级填充
        selected = []
        category_count: Dict[str, int] = {}
        max_per_category = max(1, int(target_count * 0.3))

        def _has_high_correlation(factor_name: str) -> bool:
            """检查候选因子是否与已选因子高度相关 (|corr| > 0.7)"""
            for s in selected:
                key = f"{factor_name}_{s['factor_name']}"
                corr = correlation_data.get(key)
                if corr is not None and abs(corr) > 0.7:
                    return True
            return False

        # 第一轮：每个类别选最好的1个（优先互补类别）
        seen_categories: set = set()
        # 收集所有类别，优先排列互补需求高的类别
        all_cats = list(dict.fromkeys(f["category"] for f in candidates))
        for f in candidates:
            cat = f["category"]
            if cat not in seen_categories and len(selected) < target_count:
                if not _has_high_correlation(f["factor_name"]):
                    selected.append(f)
                    seen_categories.add(cat)
                    category_count[cat] = category_count.get(cat, 0) + 1

        # 第二轮：按评级填充剩余（跳过冲突类别和高相关因子）
        selected_names = {f["factor_name"] for f in selected}
        selected_cats = set(category_count.keys())
        for f in candidates:
            if len(selected) >= target_count:
                break
            if f["factor_name"] in selected_names:
                continue
            cat = f["category"]
            if category_count.get(cat, 0) >= max_per_category:
                continue
            # 检查冲突类别
            is_conflict = False
            for sel_cat in selected_cats:
                if cat in conflict_map.get(sel_cat, []):
                    is_conflict = True
                    break
            if is_conflict and diversity_weight > 0.3:
                continue
            # 检查高相关
            if _has_high_correlation(f["factor_name"]):
                continue
            selected.append(f)
            selected_names.add(f["factor_name"])
            category_count[cat] = category_count.get(cat, 0) + 1
            selected_cats.add(cat)

        # 统计
        category_summary = {}
        for f in selected:
            cat = f["category"]
            if cat not in category_summary:
                category_summary[cat] = {"count": 0, "name": FACTOR_CATEGORIES.get(cat, cat)}
            category_summary[cat]["count"] += 1

        return {
            "ok": True,
            "total_selected": len(selected),
            "factors": selected,
            "category_summary": category_summary,
            "selection_criteria": {
                "target_count": target_count,
                "min_grade": min_grade,
                "include_categories": include_categories,
                "diversity_weight": diversity_weight,
            },
        }

    def compute_correlation_matrix(
        self,
        factor_names: List[str],
        method: str = "spearman_ewma",
        as_of_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """计算因子间相关性矩阵。

        优先使用 CorrelationEngine 基于真实截面数据计算（Spearman+EWMA）。
        如果引擎计算失败（数据不足等），回退到基于分类的估算。
        """
        # 1. 尝试使用真实计算引擎
        try:
            loader = FactorValueLoader()
            available = set(loader.get_available_factors())
            computable = [f for f in factor_names if f in available]

            if len(computable) >= 2:
                engine = CorrelationEngine(loader)
                result = engine.compute_full_matrix(
                    computable,
                    as_of_date=as_of_date,
                    save_hdf5=True,
                )

                # 构建返回结构 + 持久化高相关对
                correlations = []
                for i, fa in enumerate(result.factor_names):
                    for j, fb in enumerate(result.factor_names):
                        if j <= i:
                            continue
                        corr = float(result.matrix[i, j])
                        if np.isnan(corr):
                            continue
                        correlations.append({
                            "factor_a": fa,
                            "factor_b": fb,
                            "correlation": round(corr, 6),
                            "method": method,
                            "is_estimated": False,
                        })
                        # 持久化到 DB（全量存储，供组合分析参考）
                        self._upsert_correlation(fa, fb, round(corr, 6), method)

                # 对不在 Parquet 中的因子补充分类估算
                non_computable = [f for f in factor_names if f not in available]
                if non_computable:
                    est_corrs = self._estimate_by_category(
                        non_computable, computable + non_computable, method
                    )
                    correlations.extend(est_corrs)

                return {
                    "ok": True,
                    "factor_count": len(factor_names),
                    "computable_count": len(computable),
                    "correlation_count": len(correlations),
                    "correlations": correlations,
                    "method": method,
                    "engine": "spearman_ewma",
                    "effective_window": result.effective_window,
                    "metadata": result.metadata,
                }

        except Exception as e:
            logger.warning(f"CorrelationEngine 计算失败，回退到分类估算: {e}")

        # 2. 回退: 基于分类的估算（降级保护）
        est_corrs = self._estimate_by_category(factor_names, factor_names, method)
        return {
            "ok": True,
            "factor_count": len(factor_names),
            "computable_count": 0,
            "correlation_count": len(est_corrs),
            "correlations": est_corrs,
            "method": method,
            "engine": "category_estimation",
            "note": "基于因子类别估算（引擎不可用时的降级结果）",
        }

    def _estimate_by_category(
        self,
        target_factors: List[str],
        all_factors: List[str],
        method: str,
    ) -> List[Dict[str, Any]]:
        """基于因子类别估算相关性（降级回退方法）。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(all_factors))
                cur.execute(
                    f"""SELECT factor_name, category
                        FROM qe_factor_classification
                        WHERE factor_name IN ({placeholders})""",
                    all_factors,
                )
                factor_cats = {row[0]: row[1] for row in cur.fetchall()}

        correlations = []
        target_set = set(target_factors)
        for i, fa in enumerate(all_factors):
            for j, fb in enumerate(all_factors):
                if j <= i:
                    continue
                # 至少一个因子在 target 中
                if fa not in target_set and fb not in target_set:
                    continue
                cat_a = factor_cats.get(fa)
                cat_b = factor_cats.get(fb)
                if cat_a and cat_b and cat_a == cat_b:
                    corr = 0.6
                else:
                    corr = 0.1
                correlations.append({
                    "factor_a": fa,
                    "factor_b": fb,
                    "correlation": corr,
                    "method": method,
                    "is_estimated": True,
                })
                self._upsert_correlation(fa, fb, corr, method)

        return correlations

    # ---- 内部方法 ----

    def _get_factor_info(self, factor_name: str, factor_source: str) -> Optional[Dict]:
        """从aistock_factor_catalog获取因子信息。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT factor_name, source, expression, code_text,
                              ic, sharpe, annualized_return,
                              best_performance_ann_ret, best_performance_sharpe,
                              description_cn
                       FROM aistock_factor_catalog
                       WHERE factor_name = %s AND source = %s""",
                    (factor_name, factor_source),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))

    def _get_independent_metrics(self, factor_name: str) -> Optional[Dict]:
        """从 aistock_factor_metrics 获取独立因子指标（full 窗口最新一条）。"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT ic_mean, ic_std, rank_ic_mean, rank_ic_std,
                               icir, rank_icir, ic_positive_ratio,
                               icir_annualized, rank_icir_annualized,
                               top_annual_return, top_excess_annual_return,
                               top_sharpe, top_max_drawdown,
                               top_excess_sharpe, benchmark_annual_return,
                               group_return_monotonicity, turnover,
                               ic_decay_half_life, coverage, n_trading_days,
                               ic_csz_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d
                        FROM aistock_factor_metrics
                        WHERE factor_name = %s AND eval_window = 'full'
                        ORDER BY calculated_at DESC
                        LIMIT 1
                    """, (factor_name,))
                    row = cur.fetchone()
                    if not row:
                        return None
                    cols = [desc[0] for desc in cur.description]
                    return dict(zip(cols, row))
        except Exception:
            return None

    def _get_experiment_track_summary(self, factor_name: str) -> Optional[Dict]:
        """从 qe_factor_experiment_metrics 聚合 L2 实验表现摘要。"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) AS total_experiments,
                               AVG(ic) AS avg_ic,
                               AVG(sharpe_ratio) AS avg_sharpe_ratio,
                               AVG(ann_return_no_cost) AS avg_ann_return,
                               MAX(ic) AS best_ic,
                               MIN(ic) AS worst_ic,
                               MAX(collected_at) AS last_experiment_date
                        FROM qe_factor_experiment_metrics
                        WHERE factor_name = %s
                    """, (factor_name,))
                    row = cur.fetchone()
                    if not row or not row[0]:
                        return None
                    cols = [desc[0] for desc in cur.description]
                    summary = dict(zip(cols, row))
                    if summary["total_experiments"] == 0:
                        return None
                    # 转换 datetime 为字符串
                    if summary.get("last_experiment_date"):
                        summary["last_experiment_date"] = str(summary["last_experiment_date"])[:10]
                    return summary
        except Exception:
            return None

    def _get_multi_window_metrics(self, factor_name: str) -> Optional[Dict]:
        """从 aistock_factor_metrics 获取多窗口稳定性指标（out_sample/recent_6m/recent_3m）。"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT eval_window, ic_mean, icir, rank_ic_mean, rank_icir,
                               icir_annualized, rank_icir_annualized
                        FROM aistock_factor_metrics
                        WHERE factor_name = %s
                          AND eval_window IN ('out_sample', 'recent_6m', 'recent_3m')
                        ORDER BY calculated_at DESC, eval_window
                    """, (factor_name,))
                    rows = cur.fetchall()
                    if not rows:
                        return None
                    result = {}
                    for row in rows:
                        window = row[0]
                        if window not in result:
                            result[window] = {
                                'ic_mean': row[1],
                                'icir': row[2],
                                'rank_ic_mean': row[3],
                                'rank_icir': row[4]
                            }
                    return result
        except Exception:
            return None

    def _get_all_factors(self, source_filter: Optional[str] = None) -> List[Dict]:
        """获取所有因子列表。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                if source_filter:
                    cur.execute(
                        "SELECT factor_name, source FROM aistock_factor_catalog WHERE source = %s",
                        (source_filter,),
                    )
                else:
                    cur.execute("SELECT factor_name, source FROM aistock_factor_catalog")
                return [{"factor_name": row[0], "source": row[1]} for row in cur.fetchall()]

    def _upsert_classification(self, **kwargs) -> None:
        """UPSERT因子分类结果。"""
        profile = kwargs.get("factor_profile")
        profile_json = json.dumps(_to_jsonable(profile), ensure_ascii=False) if profile else None

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 解析 factor_catalog_id
                cur.execute("""
                    SELECT id FROM aistock_factor_catalog
                    WHERE factor_name = %s AND source = %s
                """, (kwargs["factor_name"], kwargs["factor_source"]))
                row = cur.fetchone()
                if not row:
                    # 尝试不限 source 匹配
                    cur.execute("""
                        SELECT id FROM aistock_factor_catalog
                        WHERE factor_name = %s ORDER BY id LIMIT 1
                    """, (kwargs["factor_name"],))
                    row = cur.fetchone()
                if not row:
                    logger.warning(f"因子 {kwargs['factor_name']} 未在 catalog 中找到，跳过分类写入")
                    return
                factor_catalog_id = row[0]

                cur.execute("""
                    INSERT INTO qe_factor_classification
                        (factor_name, factor_source, category,
                         grade_reason, classification_reason,
                         ic_value, sharpe_value, ann_ret_value,
                         llm_analysis, description, factor_dimension,
                         factor_profile, holding_period_class,
                         data_source_group, ts_info_density,
                         update_freq, linearity,
                         analyzed_at, factor_catalog_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, NOW(), %s)
                    ON CONFLICT (factor_name, factor_source) DO UPDATE SET
                        category = EXCLUDED.category,
                        grade_reason = EXCLUDED.grade_reason,
                        classification_reason = EXCLUDED.classification_reason,
                        ic_value = EXCLUDED.ic_value,
                        sharpe_value = EXCLUDED.sharpe_value,
                        ann_ret_value = EXCLUDED.ann_ret_value,
                        llm_analysis = EXCLUDED.llm_analysis,
                        description = EXCLUDED.description,
                        factor_dimension = EXCLUDED.factor_dimension,
                        factor_profile = COALESCE(EXCLUDED.factor_profile, qe_factor_classification.factor_profile),
                        holding_period_class = EXCLUDED.holding_period_class,
                        data_source_group = EXCLUDED.data_source_group,
                        ts_info_density = COALESCE(EXCLUDED.ts_info_density, qe_factor_classification.ts_info_density),
                        update_freq = EXCLUDED.update_freq,
                        linearity = COALESCE(EXCLUDED.linearity, qe_factor_classification.linearity),
                        factor_catalog_id = EXCLUDED.factor_catalog_id,
                        analyzed_at = NOW()
                """, (
                    kwargs["factor_name"],
                    kwargs["factor_source"],
                    kwargs["category"],
                    kwargs.get("grade_reason"),
                    kwargs.get("classification_reason"),
                    kwargs.get("ic_value"),
                    kwargs.get("sharpe_value"),
                    kwargs.get("ann_ret_value"),
                    kwargs.get("llm_analysis"),
                    kwargs.get("description"),
                    kwargs.get("factor_dimension"),
                    profile_json,
                    kwargs.get("holding_period_class"),
                    kwargs.get("data_source_group"),
                    kwargs.get("ts_info_density"),
                    kwargs.get("update_freq"),
                    kwargs.get("linearity"),
                    factor_catalog_id,
                ))

    def _upsert_correlation(self, factor_a: str, factor_b: str,
                            correlation: float, method: str) -> None:
        """UPSERT因子相关性（新表结构：catalog_id 主键，CHECK a_id < b_id）。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 解析 factor catalog IDs
                cur.execute("""
                    SELECT DISTINCT ON (factor_name) factor_name, id
                    FROM aistock_factor_catalog
                    WHERE factor_name IN (%s, %s)
                    ORDER BY factor_name, id
                """, (factor_a, factor_b))
                catalog_map = {row[0]: row[1] for row in cur.fetchall()}
                fa_id = catalog_map.get(factor_a)
                fb_id = catalog_map.get(factor_b)
                if fa_id is None or fb_id is None:
                    logger.warning(f"因子 {factor_a} 或 {factor_b} 未在 catalog 中找到，跳过相关性写入")
                    return

                a_id, b_id = min(fa_id, fb_id), max(fa_id, fb_id)
                cur.execute("""
                    INSERT INTO qe_factor_correlations
                        (factor_a_id, factor_b_id, correlation, method,
                         as_of_date, computed_at)
                    VALUES (%s, %s, %s, %s, CURRENT_DATE, NOW())
                    ON CONFLICT (factor_a_id, factor_b_id) DO UPDATE SET
                        correlation = EXCLUDED.correlation,
                        computed_at = NOW()
                """, (a_id, b_id, correlation, method))
