"""New implementation of StockAnalysisAgents for next_app.

This module re-implements the multi‑agent orchestration logic used by the
stock analysis service, so that the next_app backend no longer needs to
import the legacy ai_agents module from the project root.

The goal is to keep the overall behaviour and result structure compatible
with the old implementation, while keeping the code self‑contained under
next_app.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, Tuple
import threading
import time

from ..infra.deepseek_client import DeepSeekClient  # shared infrastructure module
from ..infra.deepseek_config import DEFAULT_DEEPSEEK_MODEL
from ..infra.debug_logger import debug_logger  # shared logging utility
from ..core.risk_data_fetcher_impl import RiskDataFetcher


DEFAULT_ENABLED_ANALYSTS: Dict[str, bool] = {
    # 技术相关统一为“技术资金分析师”，内部综合技术 + 资金流 + 筹码分析
    "technical": True,
    "fundamental": True,
    "risk": True,
    "sentiment": False,
    "news": False,
    "research": False,
    "announcement": False,
}


class StockAnalysisAgents:
    """Multi‑agent stock analysis orchestrator used by next_app.

    The public methods mirror the legacy StockAnalysisAgents API so that
    NextStockAnalysisAgents can keep its integration stable while this
    implementation lives entirely inside next_app.
    """

    def __init__(self, model: str = DEFAULT_DEEPSEEK_MODEL) -> None:
        self.model = model
        self.deepseek_client = DeepSeekClient(model=model)

    # ------------------------------------------------------------------
    # Individual agents
    # ------------------------------------------------------------------

    def technical_analyst_agent(
        self,
        stock_info: Dict[str, Any],
        stock_data: Any,
        indicators: Dict[str, Any],
        fund_flow_data: Dict[str, Any] | None = None,
        chip_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """综合技术 + 资金流 + 筹码的“技术资金分析师”。

        内部复用 DeepSeekClient.technical_analysis / fund_flow_analysis /
        chip_analysis 三个高层方法，将结果汇总为单一智能体报告，便于前端以
        “技术资金分析师”这一角色统一展示。
        """

        print("🔍 技术资金分析师正在分析中...")
        time.sleep(0.5)

        # 1. 技术面分析
        tech_text = ""
        try:
            tech_text = self.deepseek_client.technical_analysis(
                stock_info, stock_data, indicators
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error(
                "technical_analysis_for_tech_capital_failed", error=str(exc)
            )

        # 2. 资金面分析（可选）
        fund_flow_text = ""
        try:
            fund_flow_text = self.deepseek_client.fund_flow_analysis(
                stock_info, indicators, fund_flow_data
            )
        except Exception as exc:  # noqa: BLE001
            debug_logger.error(
                "fund_flow_analysis_for_tech_capital_failed", error=str(exc)
            )

        # 3. 筹码结构分析（可选）
        chip_text = ""
        try:
            prompt_context = {"stock_info": stock_info, "chip_data": chip_data}
            chip_text = self.deepseek_client.chip_analysis(prompt_context)
        except Exception as exc:  # noqa: BLE001
            debug_logger.error(
                "chip_analysis_for_tech_capital_failed", error=str(exc)
            )

        analysis_parts: list[str] = []
        if isinstance(tech_text, str) and tech_text.strip():
            analysis_parts.append(
                "【技术面分析】\n" + tech_text.strip()
            )
        if isinstance(fund_flow_text, str) and fund_flow_text.strip():
            analysis_parts.append(
                "【资金面分析】\n" + fund_flow_text.strip()
            )
        if isinstance(chip_text, str) and chip_text.strip():
            analysis_parts.append(
                "【筹码结构分析】\n" + chip_text.strip()
            )

        if analysis_parts:
            analysis = "\n\n".join(analysis_parts)
        else:
            analysis = "暂无技术 / 资金 / 筹码相关的有效分析结果。"

        return {
            "agent_name": "技术资金分析师",
            "agent_role": "综合技术指标、资金流向与筹码结构进行趋势与风险研判",
            "analysis": analysis,
            "focus_areas": [
                "技术指标与趋势",
                "资金流向与主力行为",
                "筹码分布与持股结构",
                "量价配合与风险信号",
            ],
            "fund_flow_data": fund_flow_data,
            "chip_data": chip_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def fundamental_analyst_agent(
        self,
        stock_info: Dict[str, Any],
        financial_data: Dict[str, Any] | None = None,
        quarterly_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Fundamental analysis agent.

        Performs basic type checks for diagnostic logging and then delegates
        to DeepSeekClient.
        """

        print("📊 基本面分析师正在分析中...")

        if financial_data is not None and not isinstance(financial_data, dict):
            debug_logger.warning(
                "fundamental_analyst_agent: financial_data is not dict",
                actual_type=type(financial_data).__name__,
            )
            financial_data = None

        if quarterly_data is not None and not isinstance(quarterly_data, dict):
            debug_logger.warning(
                "fundamental_analyst_agent: quarterly_data is not dict",
                actual_type=type(quarterly_data).__name__,
            )
            quarterly_data = None

        time.sleep(0.5)

        analysis = self.deepseek_client.fundamental_analysis(
            stock_info, financial_data, quarterly_data
        )

        return {
            "agent_name": "基本面分析师",
            "agent_role": "负责公司财务分析、行业研究、估值分析",
            "analysis": analysis,
            "focus_areas": ["财务指标", "行业分析", "公司价值", "成长性"],
            "quarterly_data": quarterly_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def fund_flow_analyst_agent(
        self,
        stock_info: Dict[str, Any],
        indicators: Dict[str, Any],
        fund_flow_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Fund‑flow analysis agent."""

        print("💰 资金面分析师正在分析中...")
        time.sleep(0.5)

        analysis = self.deepseek_client.fund_flow_analysis(
            stock_info, indicators, fund_flow_data
        )

        return {
            "agent_name": "资金面分析师",
            "agent_role": "负责资金流向分析、主力行为研究、市场情绪判断",
            "analysis": analysis,
            "focus_areas": ["资金流向", "主力动向", "市场情绪", "流动性"],
            "fund_flow_data": fund_flow_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def risk_management_agent(
        self,
        stock_info: Dict[str, Any],
        indicators: Dict[str, Any],
        risk_data: Dict[str, Any] | None = None,
        fund_flow_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """风险管理智能体（增强版，与旧版实现保持一致）。"""

        print("⚠️ 风险管理师正在评估中...")

        # 如果有风险数据，显示数据来源
        if risk_data and risk_data.get("data_success"):
            print(
                "   ✓ 已获取统一数据接口风险数据（Tushare：限售解禁、股东增减持、重要公告）"
            )
        else:
            print("   ⚠ 未获取到风险数据，将基于基本信息分析")

        if fund_flow_data and fund_flow_data.get("data_success"):
            print("   ✓ 已获取流动性参考数据（统一数据接口资金流向）")
        else:
            print("   ℹ️ 未获取到资金流向参考数据，流动性分析将基于其他指标")

        time.sleep(1)

        # 构建风险数据文本
        risk_data_text = ""
        if risk_data and risk_data.get("data_success"):
            # 使用格式化的风险数据（改为使用 next_app 内部实现）
            from ..core.risk_data_fetcher_impl import RiskDataFetcher

            fetcher = RiskDataFetcher()
            risk_data_text = f"""

【实际风险数据】（统一数据访问模块 / Tushare）
{fetcher.format_risk_data_for_ai(risk_data)}

以上风险数据已通过统一数据访问模块预先获取（Tushare官方接口），请基于这些实际数据进行深度风险分析。
"""

        liquidity_metrics = risk_data.get("liquidity_metrics") if risk_data else None
        liquidity_text = self._build_liquidity_context(fund_flow_data, liquidity_metrics)

        risk_prompt = f"""
作为资深风险管理专家，请基于以下信息进行全面深度的风险评估：

股票信息：
- 股票代码：{stock_info.get('symbol', 'N/A')}
- 股票名称：{stock_info.get('name', 'N/A')}
- 当前价格：{stock_info.get('current_price', 'N/A')}
- Beta系数：{stock_info.get('beta', 'N/A')}
- 52周最高：{stock_info.get('52_week_high', 'N/A')}
- 52周最低：{stock_info.get('52_week_low', 'N/A')}

技术指标：
- RSI：{indicators.get('rsi', 'N/A')}
- 布林带位置：当前价格相对于上下轨的位置
- 波动率指标等
{risk_data_text}
{liquidity_text}

⚠️ 重要提示：以上风险数据全部来自统一数据访问模块（Tushare官方接口），请你：
1. 仔细解析每一条记录的所有字段信息
2. 识别数据中的关键风险点（时间、规模、频率、股东身份等）
3. 对数据进行深度分析，不要遗漏任何重要信息
4. 如果数据中有日期字段，要特别关注最近的记录和即将发生的事件
5. 如果数据中有金额/比例字段，要评估其规模和影响力
6. 基于实际数据给出量化的风险评估，而不是空泛的描述

请基于实际数据进行客观、专业、严谨的风险评估，给出可操作的风险控制建议。
如果某些风险数据缺失，也要指出数据缺失本身可能带来的风险。
"""

        messages = [
            {
                "role": "system",
                "content": (
                    "你是一名资深的风险管理专家，具有20年以上的风险识别和控制经验，"
                    "擅长全面评估各类投资风险，特别关注限售解禁、股东减持、重要事件等可能影响股价的风险因素。"
                    "你擅长从海量原始数据中提取关键信息，进行深度解析和量化评估。"
                ),
            },
            {"role": "user", "content": risk_prompt},
        ]

        analysis = self.deepseek_client.call_api(messages, max_tokens=6000)

        return {
            "agent_name": "风险管理师",
            "agent_role": "识别并评估多维风险，提供风险控制建议",
            "analysis": analysis,
            "focus_areas": [
                "限售解禁",
                "股东减持",
                "重大事件",
                "系统性风险",
                "操作建议",
            ],
            "risk_data": risk_data,
            "fund_flow_data": fund_flow_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _build_liquidity_context(
        self,
        fund_flow_data: Dict[str, Any] | None,
        liquidity_metrics: Dict[str, Any] | None,
    ) -> str:
        """构建流动性参考数据文本（从旧版 ai_agents 迁移而来）。"""

        section_title = "\n【流动性参考数据】"
        lines: list[str] = [section_title]

        core = None
        if fund_flow_data and fund_flow_data.get("data_success"):
            if isinstance(fund_flow_data, dict):
                core = fund_flow_data.get("fund_flow_data") or fund_flow_data.get(
                    "fund_flow"
                )
            if core and isinstance(core, Dict):
                records = core.get("data") or core.get("records")
            else:
                records = None
        else:
            records = None

        def parse_date(value: Any):
            if value is None:
                return None
            candidates = ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]
            val = str(value).strip()
            if not val:
                return None
            for fmt in candidates:
                try:
                    return datetime.strptime(val, fmt)
                except Exception:  # noqa: BLE001
                    continue
            return None

        def to_float(val: Any):
            if val is None:
                return None
            if isinstance(val, (int, float)):
                return float(val)
            try:
                text = str(val).replace(",", "").replace("%", "").strip()
                if not text:
                    return None
                return float(text)
            except Exception:  # noqa: BLE001
                return None

        def pick(item: Dict[str, Any], keys: list[str]):
            for key in keys:
                if key in item and item[key] not in (None, ""):
                    value = to_float(item[key])
                    if value is not None:
                        return value
            return None

        fund_flow_lines: list[str] = []
        parsed_records: list[tuple[datetime, Dict[str, Any]]] = []
        if records:
            for item in records:
                if not isinstance(item, dict):
                    continue
                dt = None
                for key in ("trade_date", "日期", "date", "DAY"):
                    if key in item:
                        dt = parse_date(item[key])
                        if dt:
                            break
                if dt is None:
                    continue
                parsed_records.append((dt, item))

    def market_sentiment_agent(
        self,
        stock_info: Dict[str, Any],
        sentiment_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Market‑sentiment analysis agent."""

        print("📈 市场情绪分析师正在分析中...")
        time.sleep(0.5)

        prompt_context = {
            "stock_info": stock_info,
            "sentiment_data": sentiment_data,
        }

        analysis = self.deepseek_client.sentiment_analysis(prompt_context)

        return {
            "agent_name": "市场情绪分析师",
            "agent_role": "负责市场情绪研究、投资者心理分析、热点追踪",
            "analysis": analysis,
            "focus_areas": ["情绪指标", "活跃度", "热点", "情绪反转信号"],
            "sentiment_data": sentiment_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def news_analyst_agent(
        self,
        stock_info: Dict[str, Any],
        news_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """News analysis agent."""

        print("📰 新闻分析师正在分析中...")
        time.sleep(0.5)

        prompt_context = {
            "stock_info": stock_info,
            "news_data": news_data,
        }

        analysis = self.deepseek_client.news_analysis(prompt_context)

        return {
            "agent_name": "新闻分析师",
            "agent_role": "负责新闻事件分析、舆情研究、重大事件影响评估",
            "analysis": analysis,
            "focus_areas": ["新闻解读", "舆情分析", "事件影响", "市场反应"],
            "news_data": news_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def research_report_analyst_agent(
        self,
        stock_info: Dict[str, Any],
        research_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Sell‑side research report analysis agent."""

        print("📑 机构研报分析师正在分析中...")
        time.sleep(0.5)

        prompt_context = {
            "stock_info": stock_info,
            "research_data": research_data,
        }

        analysis = self.deepseek_client.research_report_analysis(prompt_context)

        return {
            "agent_name": "机构研报分析师",
            "agent_role": "负责机构研报解读与一致预期分析",
            "analysis": analysis,
            "focus_areas": ["目标价", "评级变动", "研报观点", "机构态度"],
            "research_data": research_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def announcement_analyst_agent(
        self,
        stock_info: Dict[str, Any],
        announcement_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Corporate announcement analysis agent."""

        print("📢 公告分析师正在分析中...")
        time.sleep(0.5)

        prompt_context = {
            "stock_info": stock_info,
            "announcement_data": announcement_data,
        }

        analysis = self.deepseek_client.announcement_analysis(prompt_context)

        return {
            "agent_name": "公告分析师",
            "agent_role": "负责公司公告解读与事件评估",
            "analysis": analysis,
            "focus_areas": ["重大事项", "盈利预警", "股权变动", "合规风险"],
            "announcement_data": announcement_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def chip_analyst_agent(
        self,
        stock_info: Dict[str, Any],
        chip_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Chip / holding‑structure analysis agent."""

        print("🎯 筹码分析师正在分析中...")
        time.sleep(0.5)

        prompt_context = {
            "stock_info": stock_info,
            "chip_data": chip_data,
        }

        analysis = self.deepseek_client.chip_analysis(prompt_context)

        return {
            "agent_name": "筹码分析师",
            "agent_role": "负责筹码分布、持股结构和换手行为分析",
            "analysis": analysis,
            "focus_areas": ["筹码分布", "集中度", "获利盘比例", "换手机会"],
            "chip_data": chip_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    # ------------------------------------------------------------------
    # Multi‑agent orchestration
    # ------------------------------------------------------------------

    def _iter_enabled_agents(
        self, enabled_analysts: Dict[str, bool]
    ) -> Iterable[Tuple[str, str]]:
        for key, flag in enabled_analysts.items():
            if flag:
                yield key, key

    def run_multi_agent_analysis(
        self,
        stock_info: Dict[str, Any],
        stock_data: Any,
        indicators: Dict[str, Any],
        financial_data: Dict[str, Any] | None = None,
        fund_flow_data: Dict[str, Any] | None = None,
        sentiment_data: Dict[str, Any] | None = None,
        news_data: Dict[str, Any] | None = None,
        quarterly_data: Dict[str, Any] | None = None,
        risk_data: Dict[str, Any] | None = None,
        research_data: Dict[str, Any] | None = None,
        announcement_data: Dict[str, Any] | None = None,
        chip_data: Dict[str, Any] | None = None,
        enabled_analysts: Dict[str, bool] | None = None,
    ) -> Dict[str, Any]:
        """Run all enabled agents (mostly in parallel) and collect results.

        The returned dict maps internal agent keys (technical, fundamental,
        fund_flow, risk, sentiment, news, research, announcement, chip) to
        their respective result dicts.
        """

        # 基于默认配置 + 用户显式传入配置构建最终启用表。
        # 为了向后兼容，若请求中仍包含 fund_flow / chip，则将其视为
        # “技术资金分析师”的别名，统一并入 technical 维度，而不再生成
        # 独立的资金面 / 筹码分析师。
        raw_flags: Dict[str, bool] = dict(DEFAULT_ENABLED_ANALYSTS)
        if enabled_analysts:
            raw_flags.update(enabled_analysts)

        legacy_ff = enabled_analysts.get("fund_flow") if enabled_analysts else None
        legacy_chip = enabled_analysts.get("chip") if enabled_analysts else None
        if legacy_ff is True or legacy_chip is True:
            raw_flags["technical"] = True

        flags: Dict[str, bool] = {}
        for k, v in raw_flags.items():
            if k in {"fund_flow", "chip"}:
                continue
            flags[k] = v

        debug_logger.info(
            "run_multi_agent_analysis开始",
            enabled_agents={k: v for k, v in flags.items() if v},
        )

        results: Dict[str, Any] = {}
        errors: Dict[str, str] = {}

        def _run_single(name: str) -> Tuple[str, Any]:
            try:
                if name == "technical":
                    res = self.technical_analyst_agent(
                        stock_info,
                        stock_data,
                        indicators,
                        fund_flow_data=fund_flow_data,
                        chip_data=chip_data,
                    )
                elif name == "fundamental":
                    res = self.fundamental_analyst_agent(
                        stock_info,
                        financial_data=financial_data,
                        quarterly_data=quarterly_data,
                    )
                elif name == "risk":
                    res = self.risk_management_agent(
                        stock_info,
                        indicators,
                        risk_data=risk_data,
                        fund_flow_data=fund_flow_data,
                    )
                elif name == "sentiment":
                    res = self.market_sentiment_agent(stock_info, sentiment_data)
                elif name == "news":
                    res = self.news_analyst_agent(stock_info, news_data)
                elif name == "research":
                    res = self.research_report_analyst_agent(stock_info, research_data)
                elif name == "announcement":
                    res = self.announcement_analyst_agent(
                        stock_info, announcement_data
                    )
                else:
                    raise ValueError(f"Unknown agent: {name}")

                return name, res
            except Exception as exc:  # noqa: BLE001
                debug_logger.error(
                    "agent执行失败", agent=name, error=str(exc)
                )
                errors[name] = str(exc)
                return name, {
                    "agent_name": name,
                    "agent_role": "ERROR",
                    "analysis": f"智能体执行失败: {exc}",
                    "error": str(exc),
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                }

        enabled_keys = [k for k, v in flags.items() if v]
        if not enabled_keys:
            return {
                "_meta": {
                    "enabled_agents": {},
                    "errors": {"system": "没有启用任何分析师"},
                }
            }

        max_workers = min(8, max(1, len(enabled_keys)))
        lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_run_single, name): name for name in enabled_keys
            }
            for fut in as_completed(future_map):
                name = future_map[fut]
                key, value = fut.result()
                with lock:
                    results[key] = value

        results["_meta"] = {
            "enabled_agents": {k: flags[k] for k in flags},
            "errors": errors,
            "finished_at": datetime.utcnow().isoformat(),
        }

        debug_logger.info(
            "run_multi_agent_analysis完成",
            enabled_agents={k: v for k, v in flags.items() if v},
            error_agents=list(errors.keys()),
        )

        return results

    # ------------------------------------------------------------------
    # Team discussion & final decision
    # ------------------------------------------------------------------

    def conduct_team_discussion(
        self,
        agents_results: Dict[str, Any],
        stock_info: Dict[str, Any],
    ) -> str:
        """Ask the model to synthesise a team discussion based on all agents.

        Returns a long‑form textual discussion in Chinese, similar in spirit
        to the legacy implementation.
        """

        print("🤝 分析团队正在进行综合讨论...")
        time.sleep(2)

        participants: list[str] = []
        reports: list[str] = []

        if "technical" in agents_results:
            participants.append("技术资金分析师")
            reports.append(
                f"【技术资金分析师报告】\n{agents_results['technical'].get('analysis', '')}"
            )

        if "fundamental" in agents_results:
            participants.append("基本面分析师")
            reports.append(
                f"【基本面分析师报告】\n{agents_results['fundamental'].get('analysis', '')}"
            )

        if "fund_flow" in agents_results:
            participants.append("资金面分析师")
            reports.append(
                f"【资金面分析师报告】\n{agents_results['fund_flow'].get('analysis', '')}"
            )

        if "risk" in agents_results:
            participants.append("风险管理师")
            reports.append(
                f"【风险管理师报告】\n{agents_results['risk'].get('analysis', '')}"
            )

        if "sentiment" in agents_results:
            participants.append("市场情绪分析师")
            reports.append(
                f"【市场情绪分析师报告】\n{agents_results['sentiment'].get('analysis', '')}"
            )

        if "news" in agents_results:
            participants.append("新闻分析师")
            reports.append(
                f"【新闻分析师报告】\n{agents_results['news'].get('analysis', '')}"
            )

        if "research" in agents_results:
            participants.append("机构研报分析师")
            reports.append(
                f"【机构研报分析师报告】\n{agents_results['research'].get('analysis', '')}"
            )

        if "announcement" in agents_results:
            participants.append("公告分析师")
            reports.append(
                f"【公告分析师报告】\n{agents_results['announcement'].get('analysis', '')}"
            )

        if "chip" in agents_results:
            participants.append("筹码分析师")
            reports.append(
                f"【筹码分析师报告】\n{agents_results['chip'].get('analysis', '')}"
            )

        all_reports = "\n\n".join(reports)

        discussion_prompt = f"""
现在进行投资决策团队会议，参会人员包括：{', '.join(participants)}。

股票：{stock_info.get('name', 'N/A')} ({stock_info.get('symbol', 'N/A')})

各分析师报告：

{all_reports}

请模拟一场真实的投资决策会议讨论：
1. 各分析师观点的一致性和分歧
2. 不同维度分析的权重考量
3. 风险收益评估
4. 投资时机判断
5. 策略制定思路
6. 达成初步共识

请以对话形式展现讨论过程，体现专业团队的思辨过程。
注意：只讨论参与分析的分析师的观点。
"""

        messages = [
            {
                "role": "system",
                "content": (
                    "你需要模拟一场专业的投资团队讨论会议，体现不同角色的观点碰撞和最终共识形成。"
                ),
            },
            {"role": "user", "content": discussion_prompt},
        ]

        discussion_result = self.deepseek_client.call_api(messages, max_tokens=6000)

        print("✅ 团队讨论完成")
        return discussion_result

    def make_final_decision(
        self,
        discussion_result: str,
        stock_info: Dict[str, Any],
        indicators: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Ask the model to convert the discussion into a structured decision.

        The returned dict always包含一个 `summary` 字段，便于上层直接展示，
        其余字段用于前端调试和后续扩展。
        """

        print("📋 正在制定最终投资决策...")
        time.sleep(1)

        # 委托给 DeepSeekClient.final_decision，以复用旧程序的决策提示词和字段结构
        decision = self.deepseek_client.final_decision(
            comprehensive_discussion=discussion_result,
            stock_info=stock_info,
            indicators=indicators,
        )

        # 确保始终提供一个可读性良好的 summary 字段，便于前端展示
        if isinstance(decision, dict) and "summary" not in decision:
            rating = str(decision.get("rating") or "").strip()
            target = str(decision.get("target_price") or "").strip()
            entry = str(decision.get("entry_range") or "").strip()
            tp = str(decision.get("take_profit") or "").strip()
            sl = str(decision.get("stop_loss") or "").strip()
            pos = str(decision.get("position_size") or "").strip()
            conf = str(decision.get("confidence_level") or "").strip()

            parts: list[str] = []
            if rating:
                parts.append(f"投资评级：{rating}")
            if target:
                parts.append(f"目标价：{target}")
            if entry:
                parts.append(f"建议进场区间：{entry}")
            if tp:
                parts.append(f"止盈位：{tp}")
            if sl:
                parts.append(f"止损位：{sl}")
            if pos:
                parts.append(f"仓位建议：{pos}")
            if conf:
                parts.append(f"信心度：{conf}/10")

            if parts:
                decision["summary"] = "；".join(parts)
            else:
                try:
                    import json as _json

                    decision["summary"] = _json.dumps(decision, ensure_ascii=False)
                except Exception:  # noqa: BLE001
                    decision["summary"] = str(decision)

        print("✅ 最终投资决策完成")
        return decision

