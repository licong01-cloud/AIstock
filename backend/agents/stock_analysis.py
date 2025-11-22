"""next_app 专用的多智能体股票分析封装。

内部委托给 next_app 自己实现的 StockAnalysisAgents，对外暴露
NextStockAnalysisAgents，避免直接依赖项目根目录的旧模块。
"""

from __future__ import annotations

from typing import Any, Dict

from .ai_agents_impl import StockAnalysisAgents


class NextStockAnalysisAgents:
    """包装旧应用多智能体，实现新后端的稳定接口层。"""

    def __init__(self, model: str = "deepseek-chat") -> None:
        self._core = StockAnalysisAgents(model=model)

    def run_core_analysis(
        self,
        stock_info: Dict[str, Any],
        stock_data_with_indicators,
        indicators: Dict[str, Any],
        financial_data: Dict[str, Any] | None,
        fund_flow_data: Dict[str, Any] | None,
        risk_data: Dict[str, Any] | None,
        sentiment_data: Dict[str, Any] | None = None,
        news_data: Dict[str, Any] | None = None,
        research_data: Dict[str, Any] | None = None,
        announcement_data: Dict[str, Any] | None = None,
        chip_data: Dict[str, Any] | None = None,
        enabled_analysts: Dict[str, bool] | None = None,
    ) -> tuple[Dict[str, Any], Any, Dict[str, Any]]:
        """执行核心多智能体分析，返回 (agents_results, discussion_result, final_decision)。"""

        if enabled_analysts is None:
            enabled_analysts = {
                "technical": True,
                "fundamental": True,
                "fund_flow": True,
                "risk": True,
                # 其余高成本分析先关闭，后续按需开放
                "sentiment": False,
                "news": False,
                "research": False,
                "announcement": False,
                "chip": False,
            }

        agents_results = self._core.run_multi_agent_analysis(
            stock_info=stock_info,
            stock_data=stock_data_with_indicators,
            indicators=indicators,
            financial_data=financial_data,
            fund_flow_data=fund_flow_data,
            sentiment_data=sentiment_data,
            news_data=news_data,
            quarterly_data=None,
            risk_data=risk_data,
            research_data=research_data,
            announcement_data=announcement_data,
            chip_data=chip_data,
            enabled_analysts=enabled_analysts,
        )

        discussion_result = self._core.conduct_team_discussion(agents_results, stock_info)
        final_decision = self._core.make_final_decision(discussion_result, stock_info, indicators)

        return agents_results, discussion_result, final_decision
