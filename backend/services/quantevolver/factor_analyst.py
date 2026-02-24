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
from datetime import datetime
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn

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
    "S": "卓越 - IC>0.05且夏普>2.0",
    "A": "优秀 - IC>0.03且夏普>1.5",
    "B": "良好 - IC>0.02且夏普>1.0",
    "C": "一般 - IC>0.01",
    "D": "较差 - IC<=0.01或无数据",
}

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


def _grade_by_metrics(ic: Optional[float], sharpe: Optional[float],
                      ann_ret: Optional[float]) -> str:
    """基于指标的因子评级。"""
    if ic is None and sharpe is None:
        return "D"

    ic_val = ic or 0.0
    sharpe_val = sharpe or 0.0

    if ic_val > 0.05 and sharpe_val > 2.0:
        return "S"
    elif ic_val > 0.03 and sharpe_val > 1.5:
        return "A"
    elif ic_val > 0.02 and sharpe_val > 1.0:
        return "B"
    elif ic_val > 0.01:
        return "C"
    else:
        return "D"


def _classify_with_llm(factor_name: str, expression: Optional[str],
                        code_text: Optional[str]) -> Optional[Dict[str, str]]:
    """使用LLM进行因子分类。返回 {"category": "...", "reason": "..."}"""
    llm = _get_llm_client()
    if llm is None:
        return None

    from .prompt_manager import PromptManager, safe_format
    pm = PromptManager()
    prompt_data = pm.get_active_prompt_text("factor_classifier", "classify_factor")

    if prompt_data:
        system_prompt = prompt_data["system_prompt"]
        user_prompt = safe_format(prompt_data["user_prompt_template"], 
            factor_name=factor_name,
            expression=expression or "无",
            code_text=(code_text or "")[:500],
        )
    else:
        raise ValueError("未配置 factor_classifier/classify_factor 的提示词，拒绝使用兜底策略")

    try:
        from .llm_client import get_llm_kwargs
        kwargs = get_llm_kwargs("factor_classifier")
        
        response = llm.completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=200,
            response_format={"type": "json_object"},
            **kwargs
        )
        content = response.choices[0].message.content.strip()
        # 解析JSON
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        result = json.loads(content)
        return result
    except Exception as e:
        logger.warning(f"LLM分类失败 ({factor_name}): {e}")
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


def _generate_description_with_llm(factor_name: str, code_text: Optional[str],
                                    expression: Optional[str]) -> Optional[str]:
    """使用LLM生成因子描述。"""
    llm = _get_llm_client()
    if llm is None:
        return None

    from .prompt_manager import PromptManager, safe_format
    pm = PromptManager()
    prompt_data = pm.get_active_prompt_text("factor_describer", "generate_description")

    if prompt_data:
        system_prompt = prompt_data["system_prompt"]
        user_prompt = f"""因子名称: {factor_name}
因子表达式（QLib格式，仅供理解逻辑，不要在描述中展示）: {expression or '无'}
因子代码（仅供理解逻辑，不要在描述中展示变量名）: {(code_text or '')[:800]}"""
    else:
        raise ValueError("未配置 factor_describer/generate_description 的提示词，拒绝使用兜底策略")

    try:
        from .llm_client import get_llm_kwargs
        kwargs = get_llm_kwargs("factor_describer")
        
        response = llm.completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=500,
            **kwargs
        )
        content = response.choices[0].message.content.strip()
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        result = json.loads(content)
        return result.get("description", "")[:250]
    except Exception as e:
        logger.warning(f"LLM描述生成失败 ({factor_name}): {e}")
        return None


class FactorAnalyst:
    """因子分析师Agent。"""

    def analyze_single_factor(
        self,
        factor_name: str,
        factor_source: str,
        use_llm: bool = False,
    ) -> Dict[str, Any]:
        """分析单个因子：分类 + 评级。

        Args:
            factor_name: 因子名称
            factor_source: 因子来源（rdagent_task_sync / alpha158 / alpha360）
            use_llm: 是否使用LLM进行分类

        Returns:
            分类和评级结果
        """
        # 从数据库获取因子信息
        factor_info = self._get_factor_info(factor_name, factor_source)
        if not factor_info:
            return {"ok": False, "error": f"因子 {factor_name} (source={factor_source}) 不存在"}

        expression = factor_info.get("expression")
        code_text = factor_info.get("code_text")
        ic = factor_info.get("ic")
        sharpe = factor_info.get("sharpe")
        ann_ret = factor_info.get("annualized_return") or factor_info.get("best_performance_ann_ret")

        # 分类
        category = None
        classification_reason = None

        if use_llm:
            llm_result = _classify_with_llm(factor_name, expression, code_text)
            if llm_result:
                category = llm_result.get("category")
                classification_reason = llm_result.get("reason")

        if not category:
            rule_cat, rule_reason = _classify_by_rules(factor_name, code_text=code_text, expression=expression)
            if rule_cat:
                category = rule_cat
                classification_reason = f"规则分类: {rule_reason}"
            else:
                category = "TECH"
                classification_reason = f"默认分类: 未匹配到明确类别(source={factor_source})"

        # 评级
        grade = _grade_by_metrics(ic, sharpe, ann_ret)
        grade_reason = f"IC={ic}, Sharpe={sharpe}, AnnRet={ann_ret}"

        # 判断因子维度（截面/时序）
        factor_dimension = _determine_factor_dimension(
            factor_name, category, code_text=code_text, expression=expression)

        # 生成因子描述
        description = None
        if use_llm:
            description = _generate_description_with_llm(factor_name, code_text, expression)
        if not description:
            description = _generate_description_by_rules(
                factor_name, category, code_text=code_text, expression=expression)

        # 保存到数据库
        self._upsert_classification(
            factor_name=factor_name,
            factor_source=factor_source,
            category=category,
            grade=grade,
            grade_reason=grade_reason,
            classification_reason=classification_reason,
            ic_value=ic,
            sharpe_value=sharpe,
            ann_ret_value=ann_ret,
            llm_analysis=classification_reason if use_llm else None,
            description=description,
            factor_dimension=factor_dimension,
        )

        return {
            "ok": True,
            "factor_name": factor_name,
            "factor_source": factor_source,
            "category": category,
            "category_name": FACTOR_CATEGORIES.get(category, category),
            "factor_dimension": factor_dimension,
            "grade": grade,
            "grade_name": FACTOR_GRADES.get(grade, grade),
            "grade_reason": grade_reason,
            "classification_reason": classification_reason,
            "description": description,
            "ic": ic,
            "sharpe": sharpe,
            "ann_ret": ann_ret,
        }

    def batch_analyze_all_factors(
        self,
        use_llm: bool = False,
        source_filter: Optional[str] = None,
        factor_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """批量分析因子。

        Args:
            use_llm: 是否使用LLM
            source_filter: 可选的source过滤
            factor_names: 可选的因子名称列表，指定后只分析这些因子

        Returns:
            批量分析结果统计
        """
        factors = self._get_all_factors(source_filter)
        # 如果指定了因子名称列表，只分析这些因子
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
                               factor_dimension, analyzed_at
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
                    SELECT factor_name, factor_source, category, grade,
                           ic_value, sharpe_value, ann_ret_value
                    FROM qe_factor_classification
                    WHERE grade IS NOT NULL
                    ORDER BY
                        CASE grade
                            WHEN 'S' THEN 0 WHEN 'A' THEN 1 WHEN 'B' THEN 2
                            WHEN 'C' THEN 3 WHEN 'D' THEN 4 ELSE 5
                        END ASC,
                        ic_value DESC NULLS LAST
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

        # 选择策略：先保证多样性，再按评级填充
        selected = []
        category_count = {}
        max_per_category = max(1, int(target_count * 0.3))

        # 第一轮：每个类别选最好的1个
        seen_categories = set()
        for f in candidates:
            cat = f["category"]
            if cat not in seen_categories and len(selected) < target_count:
                selected.append(f)
                seen_categories.add(cat)
                category_count[cat] = category_count.get(cat, 0) + 1

        # 第二轮：按评级填充剩余
        selected_names = {f["factor_name"] for f in selected}
        for f in candidates:
            if len(selected) >= target_count:
                break
            if f["factor_name"] in selected_names:
                continue
            cat = f["category"]
            if category_count.get(cat, 0) >= max_per_category:
                continue
            selected.append(f)
            selected_names.add(f["factor_name"])
            category_count[cat] = category_count.get(cat, 0) + 1

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
        method: str = "pearson",
    ) -> Dict[str, Any]:
        """计算因子间相关性矩阵。

        注意：此功能需要实际的因子数据（QLib数据集），
        当前版本基于因子类别的先验知识估算相关性。
        """
        # 获取因子分类信息
        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(factor_names))
                cur.execute(
                    f"""SELECT factor_name, category
                        FROM qe_factor_classification
                        WHERE factor_name IN ({placeholders})""",
                    factor_names,
                )
                factor_cats = {row[0]: row[1] for row in cur.fetchall()}

        # 基于类别估算相关性（同类别高相关，不同类别低相关）
        correlations = []
        for i, fa in enumerate(factor_names):
            for j, fb in enumerate(factor_names):
                if j <= i:
                    continue
                cat_a = factor_cats.get(fa)
                cat_b = factor_cats.get(fb)
                if cat_a and cat_b and cat_a == cat_b:
                    corr = 0.6  # 同类别估算高相关
                else:
                    corr = 0.1  # 不同类别估算低相关

                correlations.append({
                    "factor_a": fa,
                    "factor_b": fb,
                    "correlation": corr,
                    "method": method,
                    "is_estimated": True,
                })

                # 保存到数据库
                self._upsert_correlation(fa, fb, corr, method)

        return {
            "ok": True,
            "factor_count": len(factor_names),
            "correlation_count": len(correlations),
            "correlations": correlations,
            "note": "当前版本基于因子类别估算相关性，实际相关性需要QLib数据计算",
        }

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
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_factor_classification
                        (factor_name, factor_source, category, grade,
                         grade_reason, classification_reason,
                         ic_value, sharpe_value, ann_ret_value,
                         llm_analysis, description, factor_dimension, analyzed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (factor_name, factor_source) DO UPDATE SET
                        category = EXCLUDED.category,
                        grade = EXCLUDED.grade,
                        grade_reason = EXCLUDED.grade_reason,
                        classification_reason = EXCLUDED.classification_reason,
                        ic_value = EXCLUDED.ic_value,
                        sharpe_value = EXCLUDED.sharpe_value,
                        ann_ret_value = EXCLUDED.ann_ret_value,
                        llm_analysis = EXCLUDED.llm_analysis,
                        description = EXCLUDED.description,
                        factor_dimension = EXCLUDED.factor_dimension,
                        analyzed_at = NOW()
                """, (
                    kwargs["factor_name"],
                    kwargs["factor_source"],
                    kwargs["category"],
                    kwargs["grade"],
                    kwargs.get("grade_reason"),
                    kwargs.get("classification_reason"),
                    kwargs.get("ic_value"),
                    kwargs.get("sharpe_value"),
                    kwargs.get("ann_ret_value"),
                    kwargs.get("llm_analysis"),
                    kwargs.get("description"),
                    kwargs.get("factor_dimension"),
                ))

    def _upsert_correlation(self, factor_a: str, factor_b: str,
                            correlation: float, method: str) -> None:
        """UPSERT因子相关性。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_factor_correlations
                        (factor_a, factor_b, correlation, method, computed_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (factor_a, factor_b, method) DO UPDATE SET
                        correlation = EXCLUDED.correlation,
                        computed_at = NOW()
                """, (factor_a, factor_b, correlation, method))
