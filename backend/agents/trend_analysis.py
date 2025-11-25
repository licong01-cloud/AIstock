"""Trend analysis orchestrator for next_app.

This module implements a new, fully isolated stock trend analysis pipeline
that reuses the unified data access layer but does not interfere with the
existing multi‑agent stock analysis implementation.

Phase 3 initial version:
- Provides a rule‑based "技术资金分析师" that combines technical indicators,
  simple price/RSI/量比信号与筹码/资金数据，给出多周期概率预测；
- Produces structured predictions compatible with TrendPrediction* models;
- Returns a single PredictionStep for now (后续会按设计扩展更多分析师和修正轮次)。

Later phases can replace or增强这些规则为 LLM 驱动的分析，同时复用同样的数据结构和
持久化逻辑。
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from ..core.risk_data_fetcher_impl import RiskDataFetcher
from ..infra.debug_logger import debug_logger
from ..infra.deepseek_client import DeepSeekClient
from ..models.analysis import (
    PredictionStep,
    TrendAnalystResult,
    TrendPredictionHorizon,
    TrendPredictionScenario,
)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:  # noqa: BLE001
        return None


class StockTrendAnalysisAgents:
    """Orchestrator for the stock trend analysis pipeline.

    当前版本仅实现了第一位分析师："技术资金分析师"，用于输出基础的多周期
    趋势预测和概率分布。后续阶段会在此基础上按顺序加入：

    - 基本面分析师（趋势修正）
    - 研报分析师（趋势修正）
    - 公告分析师（趋势修正）
    - 情绪分析师（趋势修正）
    - 新闻分析师（趋势修正）
    - 风险分析师（风险补充）
    """

    def __init__(self, model: str = "deepseek-chat") -> None:
        # 目前规则化实现未直接调用 LLM；model 预留用于后续接入 DeepSeek。
        self.model = model
        self.deepseek_client = DeepSeekClient(model=model)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_trend_analysis(
        self,
        stock_info: Dict[str, Any],
        stock_data_with_indicators: Any,
        indicators: Dict[str, Any],
        financial_data: Dict[str, Any] | None = None,
        fund_flow_data: Dict[str, Any] | None = None,
        quarterly_data: Dict[str, Any] | None = None,
        risk_data: Dict[str, Any] | None = None,
        sentiment_data: Dict[str, Any] | None = None,
        news_data: Dict[str, Any] | None = None,
        research_data: Dict[str, Any] | None = None,
        announcement_data: Dict[str, Any] | None = None,
        chip_data: Dict[str, Any] | None = None,
        enabled_analysts: Dict[str, bool] | None = None,
    ) -> Tuple[
        List[TrendPredictionHorizon],
        List[TrendAnalystResult],
        TrendAnalystResult | None,
        List[PredictionStep],
    ]:
        """Run the trend analysis pipeline.

        当前阶段只返回：
        - horizons: 单轮"技术资金分析师"的预测结果
        - analysts: 仅包含技术资金分析师的报告
        - risk_report: 暂为 None（后续补充风险分析师）
        - prediction_evolution: 仅包含一步（技术资金初始预测）
        """

        enabled = enabled_analysts or {}
        flags = enabled or {}
        debug_logger.info(
            "run_trend_analysis开始",
            enabled_analysts=enabled,
        )

        now = datetime.now(timezone.utc)

        # 1. 技术资金分析师：基于价格涨跌、RSI、量比等信号构建基础概率分布
        horizons, tech_factors = self._build_tech_capital_baseline(
            stock_info=stock_info,
            indicators=indicators,
            fund_flow_data=fund_flow_data,
            chip_data=chip_data,
        )

        change_pct = tech_factors.get("change_pct")
        rsi_val = tech_factors.get("rsi")
        volume_ratio = tech_factors.get("volume_ratio")
        score = tech_factors.get("score")

        def _fmt_num(v: Any, digits: int = 2) -> str:
            try:
                if isinstance(v, (int, float)):
                    return f"{float(v):.{digits}f}"
            except Exception:  # noqa: BLE001
                pass
            return "--"

        orientation = "中性"
        if isinstance(score, (int, float)) and score > 0.5:
            orientation = "略偏多头"
        elif isinstance(score, (int, float)) and score < -0.5:
            orientation = "略偏空头"

        horizon_label_map = {
            "1d": "1天",
            "1w": "1周",
            "1m": "1个月",
            "long": "长线",
        }
        horizon_summary_lines: list[str] = []
        for h in horizons:
            up_s = next((s for s in h.scenarios if s.direction == "up"), None)
            flat_s = next((s for s in h.scenarios if s.direction == "flat"), None)
            down_s = next((s for s in h.scenarios if s.direction == "down"), None)
            if not (up_s and flat_s and down_s):
                continue
            label = horizon_label_map.get(h.horizon, h.horizon)
            horizon_summary_lines.append(
                (
                    f"- {label}：期望涨跌 {_fmt_num(h.base_expectation_pct, 2)}%，"
                    f"上涨情景概率约 {up_s.probability * 100:.1f}%、"
                    f"震荡 {flat_s.probability * 100:.1f}%、"
                    f"下跌 {down_s.probability * 100:.1f}%。"
                )
            )

        # 使用 DeepSeek 对技术、资金流、筹码等全部输入数据做详细分析
        tech_llm_text = ""
        fund_flow_llm_text = ""
        chip_llm_text = ""
        try:
            tech_llm_text = self.deepseek_client.technical_analysis(
                stock_info,
                stock_data_with_indicators,
                indicators,
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_technical_llm_failed", error=str(exc))

        try:
            fund_flow_llm_text = self.deepseek_client.fund_flow_analysis(
                stock_info,
                indicators,
                fund_flow_data,
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_fund_flow_llm_failed", error=str(exc))

        try:
            chip_llm_text = self.deepseek_client.chip_analysis(
                {"stock_info": stock_info, "chip_data": chip_data},
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_chip_llm_failed", error=str(exc))

        tech_text_lines = [
            (
                f"技术资金综合评分：{_fmt_num(score, 2)}（{orientation}），"
                "依据近期价格涨跌、RSI、量比以及资金/筹码等信号，对未来各周期的趋势进行初步量化评估。"
            ),
            "一、核心输入数据回顾：",
            f"- 近期价格涨跌幅：{_fmt_num(change_pct, 2)}%",
            f"- RSI 相对强弱指数：{_fmt_num(rsi_val, 2)}",
            f"- 量比（volume_ratio）：{_fmt_num(volume_ratio, 2)}",
            "上述信号被映射为一个综合评分（score），正值偏多头、负值偏空头，中性附近视为震荡结构。",
            "二、多周期基础情景构建：",
            (
                "在综合评分的基础上，为未来1天、1周、1个月和长线分别构建“上涨 / 震荡 / 下跌”三类情景，"
                "并根据评分大小分配不同的概率权重，同时计算各周期的期望涨跌幅（base_expectation_pct）。"
            ),
        ]
        if horizon_summary_lines:
            tech_text_lines.append("三、按周期拆解的基础预期：")
            tech_text_lines.extend(horizon_summary_lines)
        tech_text_lines.append(
            "该基础判断作为后续基本面、公告、情绪、新闻等分析师进一步修正的起点，最终趋势结论需要结合多维度信息综合评估。",
        )

        # 追加 LLM 生成的详细分析内容，确保对所有技术/资金/筹码数据做全面解读
        if isinstance(tech_llm_text, str) and tech_llm_text.strip():
            tech_text_lines.append("四、技术指标与K线结构的详细分析（LLM）：")
            tech_text_lines.append(tech_llm_text.strip())

        if isinstance(fund_flow_llm_text, str) and fund_flow_llm_text.strip():
            tech_text_lines.append("五、资金流向与融资融券的详细分析（LLM）：")
            tech_text_lines.append(fund_flow_llm_text.strip())

        if isinstance(chip_llm_text, str) and chip_llm_text.strip():
            tech_text_lines.append("六、筹码分布与持股结构的详细分析（LLM）：")
            tech_text_lines.append(chip_llm_text.strip())

        tech_result = TrendAnalystResult(
            name="技术资金分析师",
            role="综合技术指标、资金流向和筹码分布进行趋势预判（当前为规则化初版）",
            raw_text="\n".join(tech_text_lines),
            conclusion_json={
                "analyst_key": "tech_capital",
                "horizons": [h.model_dump() for h in horizons],
                "factors": tech_factors,
            },
            created_at=now,
        )

        analysts: List[TrendAnalystResult] = [tech_result]

        steps: List[PredictionStep] = [
            PredictionStep(
                step=0,
                analyst_key="tech_capital",
                analyst_name="技术资金分析师",
                description="基于技术指标、资金与筹码信号的初始趋势预测",
                horizons=horizons,
                created_at=now,
            )
        ]

        risk_report: TrendAnalystResult | None = None

        # 2. 基本面趋势修正（如启用且有估值/成长数据）
        if flags.get("fundamental", True):
            try:
                horizons, fund_result, fund_step = self._run_fundamental_adjustment(
                    base_horizons=horizons,
                    stock_info=stock_info,
                    financial_data=financial_data,
                    quarterly_data=quarterly_data,
                    now=now,
                )
                if fund_result is not None and fund_step is not None:
                    analysts.append(fund_result)
                    steps.append(fund_step)
            except Exception as e:  # noqa: BLE001
                debug_logger.exception("fundamental_trend_adjustment_failed", error=e)

        # 3. 研报趋势修正（如启用且有机构研报数据）
        if flags.get("research", False):
            try:
                horizons, research_result, research_step = self._run_research_adjustment(
                    base_horizons=horizons,
                    stock_info=stock_info,
                    research_data=research_data,
                    now=now,
                )
                if research_result is not None and research_step is not None:
                    analysts.append(research_result)
                    steps.append(research_step)
            except Exception as e:  # noqa: BLE001
                debug_logger.exception("research_trend_adjustment_failed", error=e)

        # 4. 公告趋势修正（如启用且有公告数据）
        if flags.get("announcement", False):
            try:
                horizons, ann_result, ann_step = self._run_announcement_adjustment(
                    base_horizons=horizons,
                    stock_info=stock_info,
                    announcement_data=announcement_data,
                    now=now,
                )
                if ann_result is not None and ann_step is not None:
                    analysts.append(ann_result)
                    steps.append(ann_step)
            except Exception as e:  # noqa: BLE001
                debug_logger.exception("announcement_trend_adjustment_failed", error=e)

        # 5. 情绪趋势修正（如启用且有情绪数据）
        if flags.get("sentiment", False):
            try:
                horizons, sent_result, sent_step = self._run_sentiment_adjustment(
                    base_horizons=horizons,
                    stock_info=stock_info,
                    sentiment_data=sentiment_data,
                    now=now,
                )
                if sent_result is not None and sent_step is not None:
                    analysts.append(sent_result)
                    steps.append(sent_step)
            except Exception as e:  # noqa: BLE001
                debug_logger.exception("sentiment_trend_adjustment_failed", error=e)

        # 6. 新闻趋势修正（如启用且有新闻数据）
        if flags.get("news", False):
            try:
                horizons, news_result, news_step = self._run_news_adjustment(
                    base_horizons=horizons,
                    stock_info=stock_info,
                    news_data=news_data,
                    now=now,
                )
                if news_result is not None and news_step is not None:
                    analysts.append(news_result)
                    steps.append(news_step)
            except Exception as e:  # noqa: BLE001
                debug_logger.exception("news_trend_adjustment_failed", error=e)

        # 7. 风险分析师（只输出风险报告，不直接修改概率分布）
        if flags.get("risk", True):
            try:
                risk_report = self._run_risk_analysis(
                    stock_info=stock_info,
                    indicators=indicators,
                    risk_data=risk_data,
                    fund_flow_data=fund_flow_data,
                    now=now,
                )
                if risk_report is not None:
                    analysts.append(risk_report)
            except Exception as e:  # noqa: BLE001
                debug_logger.exception("risk_trend_analysis_failed", error=e)

        debug_logger.info(
            "run_trend_analysis完成",
            steps=len(steps),
            horizon_count=len(horizons),
        )

        return horizons, analysts, risk_report, steps

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_tech_capital_baseline(
        self,
        stock_info: Dict[str, Any],
        indicators: Dict[str, Any],
        fund_flow_data: Dict[str, Any] | None,
        chip_data: Dict[str, Any] | None,
    ) -> Tuple[List[TrendPredictionHorizon], Dict[str, Any]]:
        """Construct a simple baseline prediction using technical + capital signals.

        该实现不依赖 LLM，而是使用一组可解释的启发式规则：
        - 最近涨跌幅（change_percent）
        - RSI
        - 量比（volume_ratio）
        - （预留）主力资金净流入、筹码集中度等

        返回 4 个 horizon：1d / 1w / 1m / long，每个 horizon 至少包含
        上涨 / 震荡 / 下跌 三种情景及其概率，并计算期望涨跌幅。
        """

        change_pct = _safe_float(stock_info.get("change_percent")) or 0.0
        rsi = _safe_float(indicators.get("rsi")) or 50.0
        volume_ratio = _safe_float(indicators.get("volume_ratio")) or 1.0

        # 预留：如果有资金/筹码数据，可以在这里叠加权重
        fund_bias = 0.0
        chip_bias = 0.0

        # 简单评分：正分偏多头，负分偏空头
        score = 0.0
        if change_pct > 0.5:
            score += 1.0
        if change_pct > 3.0:
            score += 1.0
        if change_pct < -0.5:
            score -= 1.0
        if change_pct < -3.0:
            score -= 1.0

        # RSI: 高位提示风险，低位提示反弹潜力，这里偏向风险中性的设定
        if rsi > 70:
            score -= 0.5
        elif rsi < 30:
            score += 0.5

        # 量比：放量上涨/下跌的信号
        if volume_ratio > 1.5:
            score += 0.5
        elif volume_ratio < 0.7:
            score -= 0.5

        score += fund_bias + chip_bias

        debug_logger.info(
            "tech_capital_baseline_score",
            change_pct=change_pct,
            rsi=rsi,
            volume_ratio=volume_ratio,
            score=score,
        )

        def build_probs(base_score: float) -> Tuple[float, float, float]:
            """根据评分粗略分配 上涨/震荡/下跌 概率。"""

            if base_score >= 2.0:
                up, flat, down = 0.6, 0.25, 0.15
            elif base_score <= -2.0:
                up, flat, down = 0.2, 0.3, 0.5
            else:
                up, flat, down = 0.4, 0.3, 0.3
            return up, flat, down

        def make_scenarios(
            horizon: str,
            up_range: Tuple[float, float],
            flat_range: Tuple[float, float],
            down_range: Tuple[float, float],
            adj: float,
        ) -> Tuple[List[TrendPredictionScenario], float]:
            """为某个 horizon 生成情景与期望收益。"""

            up_p, flat_p, down_p = build_probs(score + adj)

            def mid(v: Tuple[float, float]) -> float:
                return (v[0] + v[1]) / 2.0

            up_mid = mid(up_range)
            flat_mid = mid(flat_range)
            down_mid = mid(down_range)

            exp_ret = (
                up_p * up_mid
                + flat_p * flat_mid
                + down_p * down_mid
            )

            scenarios = [
                TrendPredictionScenario(
                    direction="up",
                    magnitude_min_pct=up_range[0],
                    magnitude_max_pct=up_range[1],
                    probability=up_p,
                    label=f"上涨{up_range[0]:.1f}%~{up_range[1]:.1f}%",
                    narrative="价格有较大概率在该区间内上涨",
                ),
                TrendPredictionScenario(
                    direction="flat",
                    magnitude_min_pct=flat_range[0],
                    magnitude_max_pct=flat_range[1],
                    probability=flat_p,
                    label=f"震荡{flat_range[0]:.1f}%~{flat_range[1]:.1f}%",
                    narrative="大概率维持区间震荡，涨跌有限",
                ),
                TrendPredictionScenario(
                    direction="down",
                    magnitude_min_pct=down_range[0],
                    magnitude_max_pct=down_range[1],
                    probability=down_p,
                    label=f"下跌{abs(down_range[0]):.1f}%~{abs(down_range[1]):.1f}%",
                    narrative="价格存在一定下跌风险",
                ),
            ]

            return scenarios, exp_ret

        horizons: List[TrendPredictionHorizon] = []

        # 1d：以短期波动为主
        s_1d, exp_1d = make_scenarios(
            "1d",
            up_range=(0.0, 5.0),
            flat_range=(-2.0, 2.0),
            down_range=(-5.0, 0.0),
            adj=0.0,
        )
        horizons.append(
            TrendPredictionHorizon(
                horizon="1d",
                scenarios=s_1d,
                base_expectation_pct=exp_1d,
            )
        )

        # 1w：波动区间稍放大
        s_1w, exp_1w = make_scenarios(
            "1w",
            up_range=(0.0, 10.0),
            flat_range=(-3.0, 3.0),
            down_range=(-10.0, 0.0),
            adj=0.2,
        )
        horizons.append(
            TrendPredictionHorizon(
                horizon="1w",
                scenarios=s_1w,
                base_expectation_pct=exp_1w,
            )
        )

        # 1m：允许更大的上下空间
        s_1m, exp_1m = make_scenarios(
            "1m",
            up_range=(0.0, 20.0),
            flat_range=(-5.0, 5.0),
            down_range=(-20.0, 0.0),
            adj=0.3,
        )
        horizons.append(
            TrendPredictionHorizon(
                horizon="1m",
                scenarios=s_1m,
                base_expectation_pct=exp_1m,
            )
        )

        # long：粗略视为更高波动和更依赖基本面，当前仍使用相似规则
        s_long, exp_long = make_scenarios(
            "long",
            up_range=(0.0, 50.0),
            flat_range=(-10.0, 10.0),
            down_range=(-40.0, 0.0),
            adj=0.0,
        )
        horizons.append(
            TrendPredictionHorizon(
                horizon="long",
                scenarios=s_long,
                base_expectation_pct=exp_long,
            )
        )

        factors: Dict[str, Any] = {
            "change_pct": change_pct,
            "rsi": rsi,
            "volume_ratio": volume_ratio,
            "fund_bias": fund_bias,
            "chip_bias": chip_bias,
            "score": score,
        }

        return horizons, factors

    def _run_fundamental_adjustment(
        self,
        base_horizons: List[TrendPredictionHorizon],
        stock_info: Dict[str, Any],
        financial_data: Dict[str, Any] | None,
        quarterly_data: Dict[str, Any] | None,
        now: datetime,
    ) -> Tuple[List[TrendPredictionHorizon], TrendAnalystResult | None, PredictionStep | None]:
        """Use simple valuation & growth heuristics to adjust mid/long-term trends.

        - 估值偏低 + 成长性较好 → 上涨概率略升、下跌概率略降；
        - 估值偏高 + 增长承压 → 上涨概率略降、下跌概率略升；
        - 短期(1d)影响较小，中期(1w/1m)与长线(long)影响更明显。
        """

        # 1. 构建一个粗略的基本面评分
        score = 0.0

        pe = _safe_float(stock_info.get("pe_ratio"))
        pb = _safe_float(stock_info.get("pb_ratio"))

        if pe is not None:
            if pe < 10:
                score += 1.0
            elif pe < 20:
                score += 0.5
            elif pe > 60:
                score -= 1.0
            elif pe > 40:
                score -= 0.5

        if pb is not None:
            if pb < 1.0:
                score += 0.5
            elif pb > 5.0:
                score -= 0.5

        ratios = None
        rev = None
        prof = None
        if isinstance(financial_data, dict):
            ratios = financial_data.get("financial_ratios") or {}

        if isinstance(ratios, dict) and ratios:
            rev = _safe_float(ratios.get("营业收入同比增长") or ratios.get("收入增长"))
            prof = _safe_float(ratios.get("净利润同比增长") or ratios.get("盈利增长"))

            if rev is not None:
                if rev > 10:
                    score += 0.5
                elif rev < 0:
                    score -= 0.5
            if prof is not None:
                if prof > 10:
                    score += 0.5
                elif prof < 0:
                    score -= 0.5

        # 将评分压缩到 [-2, 2] 区间
        score = max(-2.0, min(2.0, score))

        debug_logger.info("fundamental_trend_score", score=score, pe=pe, pb=pb)

        if score == 0.0:
            # 评分为中性时，暂不生成单独的基本面趋势分析师，以免噪声过大
            return base_horizons, None, None

        def adjust_probs(up: float, flat: float, down: float, horizon: str) -> Tuple[float, float, float]:
            """根据基本面评分对不同 horizon 的概率做轻微偏移。"""

            # 不同周期的权重：短期影响小，长线影响大
            if horizon == "1d":
                weight = 0.3
            elif horizon == "1w":
                weight = 0.5
            elif horizon == "1m":
                weight = 0.8
            else:
                weight = 1.0

            # 将 score [-2,2] 映射到 effect [-1,1]
            effect = score / 2.0
            delta = 0.1 * effect * weight

            up_new = up + delta
            down_new = down - delta

            # 先简单裁剪到 [0.05, 0.9]
            up_new = max(0.05, min(0.9, up_new))
            down_new = max(0.05, min(0.9, down_new))

            total = up_new + down_new
            if total > 0.95:
                scale = 0.95 / total
                up_new *= scale
                down_new *= scale

            flat_new = 1.0 - up_new - down_new
            # 确保震荡概率不会太离谱
            if flat_new < 0.05:
                flat_new = 0.05
                total = up_new + down_new + flat_new
                up_new /= total
                down_new /= total
                flat_new /= total

            return up_new, flat_new, down_new

        adjusted: List[TrendPredictionHorizon] = []

        for base in base_horizons:
            # 找出上涨/震荡/下跌三个情景（若缺失则跳过该 horizon）
            up_s = next((s for s in base.scenarios if s.direction == "up"), None)
            flat_s = next((s for s in base.scenarios if s.direction == "flat"), None)
            down_s = next((s for s in base.scenarios if s.direction == "down"), None)
            if not (up_s and flat_s and down_s):
                adjusted.append(base)
                continue

            up_p, flat_p, down_p = up_s.probability, flat_s.probability, down_s.probability

            up_p2, flat_p2, down_p2 = adjust_probs(up_p, flat_p, down_p, base.horizon)

            def mid(s: TrendPredictionScenario) -> float:
                return (s.magnitude_min_pct + s.magnitude_max_pct) / 2.0

            exp_ret = (
                up_p2 * mid(up_s)
                + flat_p2 * mid(flat_s)
                + down_p2 * mid(down_s)
            )

            new_scenarios = [
                TrendPredictionScenario(
                    direction=up_s.direction,
                    magnitude_min_pct=up_s.magnitude_min_pct,
                    magnitude_max_pct=up_s.magnitude_max_pct,
                    probability=up_p2,
                    label=up_s.label,
                    narrative=up_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=flat_s.direction,
                    magnitude_min_pct=flat_s.magnitude_min_pct,
                    magnitude_max_pct=flat_s.magnitude_max_pct,
                    probability=flat_p2,
                    label=flat_s.label,
                    narrative=flat_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=down_s.direction,
                    magnitude_min_pct=down_s.magnitude_min_pct,
                    magnitude_max_pct=down_s.magnitude_max_pct,
                    probability=down_p2,
                    label=down_s.label,
                    narrative=down_s.narrative,
                ),
            ]

            adjusted.append(
                TrendPredictionHorizon(
                    horizon=base.horizon,
                    scenarios=new_scenarios,
                    base_expectation_pct=exp_ret,
                )
            )

        # 生成简单的文字说明
        orientation = "中性"
        if score > 0.5:
            orientation = "略偏乐观"
        elif score < -0.5:
            orientation = "略偏谨慎"

        pe_str = f"{pe:.2f}" if pe is not None else "数据缺失"
        pb_str = f"{pb:.2f}" if pb is not None else "数据缺失"
        rev_str = f"{rev:.2f}%" if rev is not None else "数据缺失"
        prof_str = f"{prof:.2f}%" if prof is not None else "数据缺失"

        text_lines = [
            f"基本面趋势评分：{score:.2f}（{orientation}）",
            "一、估值与成长性数据回顾：",
            f"- PE（市盈率）：{pe_str}",
            f"- PB（市净率）：{pb_str}",
            f"- 收入同比增速：{rev_str}",
            f"- 净利润同比增速：{prof_str}",
            (
                "上述指标通过简单的区间打分映射为综合基本面评分：低估 + 高成长有利于抬升中长期上涨概率，"
                "高估 + 增长承压则会抬升下跌概率权重。"
            ),
            "二、对多周期趋势的具体影响：",
            (
                "在技术资金基础预测的前提下，本分析师主要对 1周、1个月和长线的上涨/震荡/下跌概率做温和修正，"
                "短期 1天 的调整幅度被刻意控制在较低水平，以避免短期噪声放大。"
            ),
        ]

        # 使用 DeepSeek 对完整财务数据与季报进行详细基本面分析
        fundamental_llm_text = ""
        try:
            fundamental_llm_text = self.deepseek_client.fundamental_analysis(
                stock_info,
                financial_data,
                quarterly_data,
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_fundamental_llm_failed", error=str(exc))

        if isinstance(fundamental_llm_text, str) and fundamental_llm_text.strip():
            text_lines.append("三、详细基本面分析报告（LLM）：")
            text_lines.append(fundamental_llm_text.strip())

        fund_result = TrendAnalystResult(
            name="基本面趋势分析师",
            role="基于估值与成长性对中长期趋势概率进行修正",
            raw_text="\n".join(text_lines),
            conclusion_json={
                "analyst_key": "fundamental",
                "score": score,
                "horizons": [h.model_dump() for h in adjusted],
            },
            created_at=now,
        )

        fund_step = PredictionStep(
            step=1,
            analyst_key="fundamental",
            analyst_name="基本面趋势分析师",
            description="在技术资金初判基础上，结合估值与成长性对中长期趋势概率进行了温和修正",
            horizons=adjusted,
            created_at=now,
        )

        return adjusted, fund_result, fund_step

    def _run_research_adjustment(
        self,
        base_horizons: List[TrendPredictionHorizon],
        stock_info: Dict[str, Any],
        research_data: Dict[str, Any] | None,
        now: datetime,
    ) -> Tuple[List[TrendPredictionHorizon], TrendAnalystResult | None, PredictionStep | None]:
        if not isinstance(research_data, dict) or not research_data.get("data_success"):
            return base_horizons, None, None

        score = 0.0

        analysis_summary = research_data.get("analysis_summary") or {}
        rating_ratio = analysis_summary.get("rating_ratio") or {}
        buy_ratio = _safe_float(rating_ratio.get("buy_ratio"))
        sell_ratio = _safe_float(rating_ratio.get("sell_ratio"))

        if buy_ratio is not None:
            if buy_ratio >= 70:
                score += 1.5
            elif buy_ratio >= 55:
                score += 1.0
            elif buy_ratio >= 45:
                score += 0.5

        if sell_ratio is not None:
            if sell_ratio >= 40:
                score -= 1.0
            elif sell_ratio >= 25:
                score -= 0.5

        content_analysis = research_data.get("content_analysis") or {}
        sentiment = content_analysis.get("sentiment_analysis") or {}
        sentiment_score = _safe_float(sentiment.get("sentiment_score"))
        if sentiment_score is not None:
            if sentiment_score >= 0.6:
                score += 0.5
            elif sentiment_score <= -0.6:
                score -= 0.5

        score = max(-2.0, min(2.0, score))

        if abs(score) < 0.25:
            return base_horizons, None, None

        def _adjust_probs(
            up: float,
            flat: float,
            down: float,
            horizon: str,
        ) -> Tuple[float, float, float]:
            if horizon == "1d":
                weight = 0.3
            elif horizon == "1w":
                weight = 0.6
            elif horizon == "1m":
                weight = 0.9
            else:
                weight = 1.0

            effect = score / 2.0
            delta = 0.08 * effect * weight

            up_new = up + delta
            down_new = down - delta

            up_new = max(0.05, min(0.9, up_new))
            down_new = max(0.05, min(0.9, down_new))

            total = up_new + down_new
            if total > 0.95:
                scale = 0.95 / total
                up_new *= scale
                down_new *= scale

            flat_new = 1.0 - up_new - down_new
            if flat_new < 0.05:
                flat_new = 0.05
                total = up_new + down_new + flat_new
                up_new /= total
                down_new /= total
                flat_new /= total

            return up_new, flat_new, down_new

        adjusted: List[TrendPredictionHorizon] = []

        for base in base_horizons:
            up_s = next((s for s in base.scenarios if s.direction == "up"), None)
            flat_s = next((s for s in base.scenarios if s.direction == "flat"), None)
            down_s = next((s for s in base.scenarios if s.direction == "down"), None)
            if not (up_s and flat_s and down_s):
                adjusted.append(base)
                continue

            up_p = up_s.probability
            flat_p = flat_s.probability
            down_p = down_s.probability

            up_p2, flat_p2, down_p2 = _adjust_probs(up_p, flat_p, down_p, base.horizon)

            def _mid(s: TrendPredictionScenario) -> float:
                return (s.magnitude_min_pct + s.magnitude_max_pct) / 2.0

            exp_ret = (
                up_p2 * _mid(up_s)
                + flat_p2 * _mid(flat_s)
                + down_p2 * _mid(down_s)
            )

            new_scenarios = [
                TrendPredictionScenario(
                    direction=up_s.direction,
                    magnitude_min_pct=up_s.magnitude_min_pct,
                    magnitude_max_pct=up_s.magnitude_max_pct,
                    probability=up_p2,
                    label=up_s.label,
                    narrative=up_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=flat_s.direction,
                    magnitude_min_pct=flat_s.magnitude_min_pct,
                    magnitude_max_pct=flat_s.magnitude_max_pct,
                    probability=flat_p2,
                    label=flat_s.label,
                    narrative=flat_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=down_s.direction,
                    magnitude_min_pct=down_s.magnitude_min_pct,
                    magnitude_max_pct=down_s.magnitude_max_pct,
                    probability=down_p2,
                    label=down_s.label,
                    narrative=down_s.narrative,
                ),
            ]

            adjusted.append(
                TrendPredictionHorizon(
                    horizon=base.horizon,
                    scenarios=new_scenarios,
                    base_expectation_pct=exp_ret,
                )
            )

        orientation = "机构观点整体中性"
        if score > 0.5:
            orientation = "机构观点偏乐观"
        elif score < -0.5:
            orientation = "机构观点偏谨慎"

        try:
            report_count = int(research_data.get("report_count") or 0)  # type: ignore[union-attr]
        except Exception:  # noqa: BLE001
            report_count = 0

        buy_ratio_str = f"{buy_ratio:.1f}%" if buy_ratio is not None else "数据缺失"
        sell_ratio_str = f"{sell_ratio:.1f}%" if sell_ratio is not None else "数据缺失"
        sentiment_str = f"{sentiment_score:.2f}" if sentiment_score is not None else "数据缺失"

        text_lines = [
            f"机构研报趋势评分：{score:.2f}（{orientation}）",
            f"统计区间内有效研报数量：{report_count} 条",
            "一、研报数据概览：",
            f"- 买入/增持评级占比：{buy_ratio_str}",
            f"- 减持/卖出评级占比：{sell_ratio_str}",
            f"- 研报文本情感均值：{sentiment_str}",
            "二、对趋势预测的影响：",
            (
                "在技术资金与基本面预测的基础上，本分析师根据评级结构与情感倾向，"
                "对 1周、1个月和长线的上涨/震荡/下跌概率做了进一步微调，短期 1天 权重较低。"
            ),
        ]

        # 使用 DeepSeek 对全部研报明细与内容分析结果进行深度解读
        research_llm_text = ""
        try:
            research_llm_text = self.deepseek_client.research_report_analysis(
                {"stock_info": stock_info, "research_data": research_data or {}},
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_research_llm_failed", error=str(exc))

        if isinstance(research_llm_text, str) and research_llm_text.strip():
            text_lines.append("三、机构研报详细解读（LLM）：")
            text_lines.append(research_llm_text.strip())

        research_result = TrendAnalystResult(
            name="机构研报趋势分析师",
            role="基于卖方研报的一致预期与内容情感对中长期趋势进行修正",
            raw_text="\n".join(text_lines),
            conclusion_json={
                "analyst_key": "research",
                "score": score,
                "horizons": [h.model_dump() for h in adjusted],
                "meta": {
                    "report_count": report_count,
                    "source": research_data.get("source") if isinstance(research_data, dict) else None,
                },
            },
            created_at=now,
        )

        research_step = PredictionStep(
            step=2,
            analyst_key="research",
            analyst_name="机构研报趋势分析师",
            description="在前述基础上，结合最近机构研报观点对1周、1个月和长线趋势概率进行了温和修正",
            horizons=adjusted,
            created_at=now,
        )

        return adjusted, research_result, research_step

    def _run_announcement_adjustment(
        self,
        base_horizons: List[TrendPredictionHorizon],
        stock_info: Dict[str, Any],
        announcement_data: Dict[str, Any] | None,
        now: datetime,
    ) -> Tuple[List[TrendPredictionHorizon], TrendAnalystResult | None, PredictionStep | None]:
        if not isinstance(announcement_data, dict) or not announcement_data.get("data_success"):
            return base_horizons, None, None

        announcements = announcement_data.get("announcements") or []
        if not isinstance(announcements, list) or not announcements:
            return base_horizons, None, None

        positive_keywords = ["增持", "回购", "高送转", "中标", "签约", "投资", "获批"]
        negative_keywords = ["减持", "预亏", "预减", "违规", "处罚", "风险提示", "诉讼"]

        pos_hits = 0
        neg_hits = 0

        for ann in announcements:
            if not isinstance(ann, dict):
                continue
            title = str(ann.get("公告标题") or ann.get("title") or "")
            summary = str(ann.get("公告摘要") or ann.get("summary") or "")
            text = f"{title} {summary}"
            if not text.strip():
                continue
            for kw in positive_keywords:
                if kw in text:
                    pos_hits += 1
            for kw in negative_keywords:
                if kw in text:
                    neg_hits += 1

        raw_score = pos_hits - neg_hits
        score = 0.4 * float(raw_score)
        score = max(-2.0, min(2.0, score))

        if abs(score) < 0.25:
            return base_horizons, None, None

        def _adjust_probs(
            up: float,
            flat: float,
            down: float,
            horizon: str,
        ) -> Tuple[float, float, float]:
            if horizon == "1d":
                weight = 0.9
            elif horizon == "1w":
                weight = 0.8
            elif horizon == "1m":
                weight = 0.6
            else:
                weight = 0.3

            effect = score / 2.0
            delta = 0.07 * effect * weight

            up_new = up + delta
            down_new = down - delta

            up_new = max(0.05, min(0.9, up_new))
            down_new = max(0.05, min(0.9, down_new))

            total = up_new + down_new
            if total > 0.95:
                scale = 0.95 / total
                up_new *= scale
                down_new *= scale

            flat_new = 1.0 - up_new - down_new
            if flat_new < 0.05:
                flat_new = 0.05
                total = up_new + down_new + flat_new
                up_new /= total
                down_new /= total
                flat_new /= total

            return up_new, flat_new, down_new

        adjusted: List[TrendPredictionHorizon] = []

        for base in base_horizons:
            up_s = next((s for s in base.scenarios if s.direction == "up"), None)
            flat_s = next((s for s in base.scenarios if s.direction == "flat"), None)
            down_s = next((s for s in base.scenarios if s.direction == "down"), None)
            if not (up_s and flat_s and down_s):
                adjusted.append(base)
                continue

            up_p = up_s.probability
            flat_p = flat_s.probability
            down_p = down_s.probability

            up_p2, flat_p2, down_p2 = _adjust_probs(up_p, flat_p, down_p, base.horizon)

            def _mid(s: TrendPredictionScenario) -> float:
                return (s.magnitude_min_pct + s.magnitude_max_pct) / 2.0

            exp_ret = (
                up_p2 * _mid(up_s)
                + flat_p2 * _mid(flat_s)
                + down_p2 * _mid(down_s)
            )

            new_scenarios = [
                TrendPredictionScenario(
                    direction=up_s.direction,
                    magnitude_min_pct=up_s.magnitude_min_pct,
                    magnitude_max_pct=up_s.magnitude_max_pct,
                    probability=up_p2,
                    label=up_s.label,
                    narrative=up_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=flat_s.direction,
                    magnitude_min_pct=flat_s.magnitude_min_pct,
                    magnitude_max_pct=flat_s.magnitude_max_pct,
                    probability=flat_p2,
                    label=flat_s.label,
                    narrative=flat_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=down_s.direction,
                    magnitude_min_pct=down_s.magnitude_min_pct,
                    magnitude_max_pct=down_s.magnitude_max_pct,
                    probability=down_p2,
                    label=down_s.label,
                    narrative=down_s.narrative,
                ),
            ]

            adjusted.append(
                TrendPredictionHorizon(
                    horizon=base.horizon,
                    scenarios=new_scenarios,
                    base_expectation_pct=exp_ret,
                )
            )

        orientation = "公告整体影响中性"
        if score > 0.5:
            orientation = "公告整体偏利好"
        elif score < -0.5:
            orientation = "公告整体偏利空"

        ann_count = len(announcements)

        text_lines = [
            f"公告事件趋势评分：{score:.2f}（{orientation}）",
            f"统计区间内公告数量：{ann_count} 条，其中利好关键词命中 {pos_hits} 次、利空关键词命中 {neg_hits} 次。",
            "一、重点关注的公告类型：",
            "- 利好：增持、回购、高送转、中标、签约、投资、获批等；",
            "- 利空：减持、预亏、预减、违规、处罚、风险提示、诉讼等；",
            "二、对趋势预测的影响：",
            (
                "根据利好/利空命中情况，对短中期（1天、1周、1个月）的上涨/下跌概率进行了小幅修正，"
                "其中 1天 和 1周 对单一重大公告更敏感，长线权重相对较低。"
            ),
        ]

        # 使用 DeepSeek 对所有公告明细与PDF内容进行逐条深度解读
        announcement_llm_text = ""
        try:
            announcement_llm_text = self.deepseek_client.announcement_analysis(
                {"stock_info": stock_info, "announcement_data": announcement_data or {}},
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_announcement_llm_failed", error=str(exc))

        if isinstance(announcement_llm_text, str) and announcement_llm_text.strip():
            text_lines.append("三、公告逐条详细分析（LLM）：")
            text_lines.append(announcement_llm_text.strip())

        ann_result = TrendAnalystResult(
            name="公告趋势分析师",
            role="基于近期重大公告和PDF内容摘要对短中期趋势进行修正",
            raw_text="\n".join(text_lines),
            conclusion_json={
                "analyst_key": "announcement",
                "score": score,
                "horizons": [h.model_dump() for h in adjusted],
                "meta": {
                    "announcement_count": ann_count,
                    "source": announcement_data.get("source") if isinstance(announcement_data, dict) else None,
                },
            },
            created_at=now,
        )

        ann_step = PredictionStep(
            step=3,
            analyst_key="announcement",
            analyst_name="公告趋势分析师",
            description="在前述基础上，结合最近公司公告对1天、1周和1个月的趋势概率进行了小幅修正",
            horizons=adjusted,
            created_at=now,
        )

        return adjusted, ann_result, ann_step

    def _run_sentiment_adjustment(
        self,
        base_horizons: List[TrendPredictionHorizon],
        stock_info: Dict[str, Any],
        sentiment_data: Dict[str, Any] | None,
        now: datetime,
    ) -> Tuple[List[TrendPredictionHorizon], TrendAnalystResult | None, PredictionStep | None]:
        if not isinstance(sentiment_data, dict) or not sentiment_data.get("data_success"):
            return base_horizons, None, None

        score = 0.0

        arbr = sentiment_data.get("arbr_data") or {}
        signals = arbr.get("signals") or {}
        overall_signal = str(signals.get("overall_signal") or "")

        if "强烈买入" in overall_signal:
            score += 1.5
        elif "买入" in overall_signal:
            score += 1.0
        elif "强烈卖出" in overall_signal:
            score -= 1.5
        elif "卖出" in overall_signal:
            score -= 1.0

        market_index = sentiment_data.get("market_index") or {}
        change_pct = _safe_float(market_index.get("change_percent"))
        if change_pct is not None:
            if change_pct >= 1.5:
                score += 0.5
            elif change_pct >= 0.5:
                score += 0.2
            elif change_pct <= -1.5:
                score -= 0.5
            elif change_pct <= -0.5:
                score -= 0.2

        score = max(-2.0, min(2.0, score))

        if abs(score) < 0.2:
            return base_horizons, None, None

        def _adjust_probs(
            up: float,
            flat: float,
            down: float,
            horizon: str,
        ) -> Tuple[float, float, float]:
            if horizon == "1d":
                weight = 1.0
            elif horizon == "1w":
                weight = 0.8
            elif horizon == "1m":
                weight = 0.5
            else:
                weight = 0.3

            effect = score / 2.0
            delta = 0.1 * effect * weight

            up_new = up + delta
            down_new = down - delta

            up_new = max(0.05, min(0.9, up_new))
            down_new = max(0.05, min(0.9, down_new))

            total = up_new + down_new
            if total > 0.95:
                scale = 0.95 / total
                up_new *= scale
                down_new *= scale

            flat_new = 1.0 - up_new - down_new
            if flat_new < 0.05:
                flat_new = 0.05
                total = up_new + down_new + flat_new
                up_new /= total
                down_new /= total
                flat_new /= total

            return up_new, flat_new, down_new

        adjusted: List[TrendPredictionHorizon] = []

        for base in base_horizons:
            up_s = next((s for s in base.scenarios if s.direction == "up"), None)
            flat_s = next((s for s in base.scenarios if s.direction == "flat"), None)
            down_s = next((s for s in base.scenarios if s.direction == "down"), None)
            if not (up_s and flat_s and down_s):
                adjusted.append(base)
                continue

            up_p = up_s.probability
            flat_p = flat_s.probability
            down_p = down_s.probability

            up_p2, flat_p2, down_p2 = _adjust_probs(up_p, flat_p, down_p, base.horizon)

            def _mid(s: TrendPredictionScenario) -> float:
                return (s.magnitude_min_pct + s.magnitude_max_pct) / 2.0

            exp_ret = (
                up_p2 * _mid(up_s)
                + flat_p2 * _mid(flat_s)
                + down_p2 * _mid(down_s)
            )

            new_scenarios = [
                TrendPredictionScenario(
                    direction=up_s.direction,
                    magnitude_min_pct=up_s.magnitude_min_pct,
                    magnitude_max_pct=up_s.magnitude_max_pct,
                    probability=up_p2,
                    label=up_s.label,
                    narrative=up_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=flat_s.direction,
                    magnitude_min_pct=flat_s.magnitude_min_pct,
                    magnitude_max_pct=flat_s.magnitude_max_pct,
                    probability=flat_p2,
                    label=flat_s.label,
                    narrative=flat_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=down_s.direction,
                    magnitude_min_pct=down_s.magnitude_min_pct,
                    magnitude_max_pct=down_s.magnitude_max_pct,
                    probability=down_p2,
                    label=down_s.label,
                    narrative=down_s.narrative,
                ),
            ]

            adjusted.append(
                TrendPredictionHorizon(
                    horizon=base.horizon,
                    scenarios=new_scenarios,
                    base_expectation_pct=exp_ret,
                )
            )

        orientation = "市场情绪中性"
        if score > 0.5:
            orientation = "市场情绪偏乐观"
        elif score < -0.5:
            orientation = "市场情绪偏谨慎/偏空"

        sentiment_change_str = (
            f"{change_pct:.2f}%" if change_pct is not None else "数据缺失"
        )

        text_lines = [
            f"市场情绪趋势评分：{score:.2f}（{orientation}）",
            "一、情绪与大盘数据回顾：",
            f"- ARBR 综合信号：{overall_signal or '数据缺失'}",
            f"- 代表指数当日涨跌幅：{sentiment_change_str}",
            "二、对趋势预测的影响：",
            (
                "本分析师主要针对短期（1天、1周）趋势进行调整：情绪偏乐观时适度抬升上涨概率、压低下跌概率；"
                "情绪偏谨慎或偏空时则反向处理，中长期影响权重较低。"
            ),
        ]

        # 使用 DeepSeek 对完整情绪数据进行多维度详细分析
        sentiment_llm_text = ""
        try:
            sentiment_llm_text = self.deepseek_client.sentiment_analysis(
                {"stock_info": stock_info, "sentiment_data": sentiment_data or {}},
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_sentiment_llm_failed", error=str(exc))

        if isinstance(sentiment_llm_text, str) and sentiment_llm_text.strip():
            text_lines.append("三、市场情绪详细分析（LLM）：")
            text_lines.append(sentiment_llm_text.strip())

        sentiment_result = TrendAnalystResult(
            name="市场情绪趋势分析师",
            role="基于ARBR、指数估值与换手率等情绪指标对短期趋势进行修正",
            raw_text="\n".join(text_lines),
            conclusion_json={
                "analyst_key": "sentiment",
                "score": score,
                "horizons": [h.model_dump() for h in adjusted],
                "meta": {
                    "overall_signal": overall_signal,
                },
            },
            created_at=now,
        )

        sentiment_step = PredictionStep(
            step=4,
            analyst_key="sentiment",
            analyst_name="市场情绪趋势分析师",
            description="结合市场情绪与大盘成交情况，对1天和1周的趋势概率进行了适度修正",
            horizons=adjusted,
            created_at=now,
        )

        return adjusted, sentiment_result, sentiment_step

    def _run_news_adjustment(
        self,
        base_horizons: List[TrendPredictionHorizon],
        stock_info: Dict[str, Any],
        news_data: Dict[str, Any] | None,
        now: datetime,
    ) -> Tuple[List[TrendPredictionHorizon], TrendAnalystResult | None, PredictionStep | None]:
        if not isinstance(news_data, dict) or not news_data.get("data_success"):
            return base_horizons, None, None

        news_root = news_data.get("news_data") or {}
        items = news_root.get("items") or []
        if not isinstance(items, list) or not items:
            return base_horizons, None, None

        positive_keywords = ["增持", "回购", "中标", "签约", "利好", "创新高", "突破"]
        negative_keywords = ["减持", "暴跌", "预亏", "亏损", "调查", "处罚", "风险"]

        pos_hits = 0
        neg_hits = 0

        for item in items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or item.get("标题") or "")
            content = str(item.get("content") or item.get("内容") or "")
            text = f"{title} {content}"
            if not text.strip():
                continue
            for kw in positive_keywords:
                if kw in text:
                    pos_hits += 1
            for kw in negative_keywords:
                if kw in text:
                    neg_hits += 1

        raw_score = pos_hits - neg_hits
        score = 0.3 * float(raw_score)
        score = max(-1.5, min(1.5, score))

        if abs(score) < 0.2:
            return base_horizons, None, None

        def _adjust_probs(
            up: float,
            flat: float,
            down: float,
            horizon: str,
        ) -> Tuple[float, float, float]:
            if horizon == "1d":
                weight = 0.9
            elif horizon == "1w":
                weight = 0.7
            elif horizon == "1m":
                weight = 0.4
            else:
                weight = 0.2

            effect = score / 1.5
            delta = 0.06 * effect * weight

            up_new = up + delta
            down_new = down - delta

            up_new = max(0.05, min(0.9, up_new))
            down_new = max(0.05, min(0.9, down_new))

            total = up_new + down_new
            if total > 0.95:
                scale = 0.95 / total
                up_new *= scale
                down_new *= scale

            flat_new = 1.0 - up_new - down_new
            if flat_new < 0.05:
                flat_new = 0.05
                total = up_new + down_new + flat_new
                up_new /= total
                down_new /= total
                flat_new /= total

            return up_new, flat_new, down_new

        adjusted: List[TrendPredictionHorizon] = []

        for base in base_horizons:
            up_s = next((s for s in base.scenarios if s.direction == "up"), None)
            flat_s = next((s for s in base.scenarios if s.direction == "flat"), None)
            down_s = next((s for s in base.scenarios if s.direction == "down"), None)
            if not (up_s and flat_s and down_s):
                adjusted.append(base)
                continue

            up_p = up_s.probability
            flat_p = flat_s.probability
            down_p = down_s.probability

            up_p2, flat_p2, down_p2 = _adjust_probs(up_p, flat_p, down_p, base.horizon)

            def _mid(s: TrendPredictionScenario) -> float:
                return (s.magnitude_min_pct + s.magnitude_max_pct) / 2.0

            exp_ret = (
                up_p2 * _mid(up_s)
                + flat_p2 * _mid(flat_s)
                + down_p2 * _mid(down_s)
            )

            new_scenarios = [
                TrendPredictionScenario(
                    direction=up_s.direction,
                    magnitude_min_pct=up_s.magnitude_min_pct,
                    magnitude_max_pct=up_s.magnitude_max_pct,
                    probability=up_p2,
                    label=up_s.label,
                    narrative=up_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=flat_s.direction,
                    magnitude_min_pct=flat_s.magnitude_min_pct,
                    magnitude_max_pct=flat_s.magnitude_max_pct,
                    probability=flat_p2,
                    label=flat_s.label,
                    narrative=flat_s.narrative,
                ),
                TrendPredictionScenario(
                    direction=down_s.direction,
                    magnitude_min_pct=down_s.magnitude_min_pct,
                    magnitude_max_pct=down_s.magnitude_max_pct,
                    probability=down_p2,
                    label=down_s.label,
                    narrative=down_s.narrative,
                ),
            ]

            adjusted.append(
                TrendPredictionHorizon(
                    horizon=base.horizon,
                    scenarios=new_scenarios,
                    base_expectation_pct=exp_ret,
                )
            )

        orientation = "新闻舆情整体中性"
        if score > 0.4:
            orientation = "新闻舆情偏利好"
        elif score < -0.4:
            orientation = "新闻舆情偏利空"

        total_news = len(items)

        text_lines = [
            f"新闻舆情趋势评分：{score:.2f}（{orientation}）",
            f"统计区间内新闻数量：{total_news} 条，其中利好关键词命中 {pos_hits} 次、利空关键词命中 {neg_hits} 次。",
            "一、关注的新闻类型：",
            "- 利好：增持、回购、中标、签约、利好、创新高、突破等；",
            "- 利空：减持、暴跌、预亏、亏损、调查、处罚、风险等；",
            "二、对趋势预测的影响：",
            (
                "本分析师主要根据近期新闻的利好/利空占比，对 1天 和 1周 的上涨/下跌概率做轻微偏移，"
                "以反映舆情对短线价格的扰动，中期影响有限、长线影响极弱。"
            ),
        ]

        # 使用 DeepSeek 对所有新闻明细进行详细解读
        news_llm_text = ""
        try:
            news_llm_text = self.deepseek_client.news_analysis(
                {"stock_info": stock_info, "news_data": news_data or {}},
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_news_llm_failed", error=str(exc))

        if isinstance(news_llm_text, str) and news_llm_text.strip():
            text_lines.append("三、新闻与舆情详细分析（LLM）：")
            text_lines.append(news_llm_text.strip())

        news_result = TrendAnalystResult(
            name="新闻趋势分析师",
            role="基于近期新闻与舆情关键词对短期趋势进行修正",
            raw_text="\n".join(text_lines),
            conclusion_json={
                "analyst_key": "news",
                "score": score,
                "horizons": [h.model_dump() for h in adjusted],
                "meta": {
                    "news_count": total_news,
                },
            },
            created_at=now,
        )

        news_step = PredictionStep(
            step=5,
            analyst_key="news",
            analyst_name="新闻趋势分析师",
            description="结合近期新闻与舆情关键词，对1天和1周的趋势概率进行了轻微调整",
            horizons=adjusted,
            created_at=now,
        )

        return adjusted, news_result, news_step

    def _run_risk_analysis(
        self,
        stock_info: Dict[str, Any],
        indicators: Dict[str, Any],
        risk_data: Dict[str, Any] | None,
        fund_flow_data: Dict[str, Any] | None,
        now: datetime,
    ) -> TrendAnalystResult | None:
        try:
            fetcher = RiskDataFetcher()
            formatted_risk = fetcher.format_risk_data_for_ai(risk_data or {})
        except Exception as e:  # noqa: BLE001
            debug_logger.exception("risk_data_format_failed", error=e)
            formatted_risk = "暂无完整风险数据，以下为通用风险提示。"

        code = str(stock_info.get("ts_code") or stock_info.get("symbol") or "").strip()
        name = str(stock_info.get("name") or "").strip()
        stock_label = f"{name}（{code}）" if (name or code) else "标的股票"

        header_lines = [
            (
                f"风险管理分析师正在对 {stock_label} 的多维度风险进行体检，对趋势概率本身不做直接修改，"
                "而是提供与仓位管理和止盈止损相关的补充视角。"
            ),
            "一、风险数据来源与覆盖范围：",
            "- 统一风险接口汇总了限售解禁、股东及高管增减持、重大事件、行业与宏观风险提示等维度；",
            "- 若部分维度缺失，将以“暂无相关数据”形式标注，以避免误判为零风险。",
            "二、结构化风险要点：",
            formatted_risk,
        ]

        # 使用 DeepSeek 对结构化风险数据做全面的逐条风险评估
        risk_llm_text = ""
        try:
            risk_prompt = (
                "作为资深风险管理专家，请基于以下股票信息、技术指标和结构化风险数据，"
                "对各类风险事件逐条进行深入分析，给出定性与定量的风险评估以及可操作的风险控制建议：\n\n"
            )
            risk_prompt += f"股票代码：{code or 'N/A'}\n股票名称：{name or 'N/A'}\n"
            risk_prompt += f"当前价格：{stock_info.get('current_price', 'N/A')}\n"
            risk_prompt += f"RSI：{indicators.get('rsi', 'N/A')}\n\n"
            risk_prompt += "【结构化风险数据】\n" + formatted_risk

            messages = [
                {
                    "role": "system",
                    "content": (
                        "你是一名资深的风险管理专家，擅长从限售解禁、股东减持、重大事件等多维数据中识别关键风险，"
                        "并给出严格的仓位管理、止盈止损与风控建议。"
                    ),
                },
                {"role": "user", "content": risk_prompt},
            ]

            risk_llm_text = self.deepseek_client.call_api(messages, max_tokens=6000)
        except Exception as exc:  # noqa: BLE001
            debug_logger.error("trend_risk_llm_failed", error=str(exc))

        full_text_lines = list(header_lines)
        if isinstance(risk_llm_text, str) and risk_llm_text.strip():
            full_text_lines.append("三、详细风险评估与风控建议（LLM）：")
            full_text_lines.append(risk_llm_text.strip())

        risk_result = TrendAnalystResult(
            name="风险管理分析师",
            role="识别并评估多维度风险，提供风险控制建议（不直接修改趋势概率）",
            raw_text="\n".join(full_text_lines),
            conclusion_json={
                "analyst_key": "risk",
                "risk_data": risk_data,
            },
            created_at=now,
        )

        return risk_result
