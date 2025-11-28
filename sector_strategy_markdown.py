from __future__ import annotations

from datetime import datetime
from typing import Any, Dict


def generate_sector_markdown_report(result_data: Dict[str, Any]) -> str:
    """生成智策分析Markdown报告。

    此实现与旧版 Streamlit UI 中的 generate_sector_markdown_report 保持一致，
    但不依赖任何 UI 组件，可在 FastAPI 等环境中直接复用。
    """

    current_time = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")

    markdown_content = f"""# 智策板块策略分析报告

**AI驱动的多维度板块投资决策支持系统**

---

## 📊 报告信息

- **生成时间**: {current_time}
- **分析周期**: 当日市场数据
- **AI模型**: DeepSeek Multi-Agent System
- **分析维度**: 宏观·板块·资金·情绪

> ⚠️ 本报告由AI系统自动生成，仅供参考，不构成投资建议。投资有风险，决策需谨慎。

---

## 📈 市场概况

本报告基于{result_data.get('timestamp', 'N/A')}的实时市场数据，
通过四位AI智能体的多维度分析，为您提供板块投资策略建议。

### 分析师团队:

- **宏观策略师** - 分析宏观经济、政策导向、新闻事件
- **板块诊断师** - 分析板块走势、估值水平、轮动特征
- **资金流向分析师** - 分析主力资金、北向资金流向
- **市场情绪解码员** - 分析市场情绪、热度、赚钱效应

"""

    predictions = result_data.get("final_predictions", {}) or {}

    if predictions.get("prediction_text"):
        markdown_content += f"""
## 🎯 核心预测

{predictions.get('prediction_text', '')}

"""
    else:
        markdown_content += "## 🎯 核心预测\n\n"

        long_short = predictions.get("long_short", {}) or {}
        bullish = long_short.get("bullish", []) or []
        bearish = long_short.get("bearish", []) or []

        markdown_content += "### 📊 板块多空预测\n\n"

        if bullish:
            markdown_content += "#### 🟢 看多板块\n\n"
            for idx, item in enumerate(bullish, 1):
                markdown_content += (
                    f"{idx}. **{item.get('sector', 'N/A')}** (信心度: {item.get('confidence', 0)}/10)\n"
                )
                markdown_content += f"   - 理由: {item.get('reason', 'N/A')}\n"
                markdown_content += f"   - 风险: {item.get('risk', 'N/A')}\n\n"

        if bearish:
            markdown_content += "#### 🔴 看空板块\n\n"
            for idx, item in enumerate(bearish, 1):
                markdown_content += (
                    f"{idx}. **{item.get('sector', 'N/A')}** (信心度: {item.get('confidence', 0)}/10)\n"
                )
                markdown_content += f"   - 理由: {item.get('reason', 'N/A')}\n"
                markdown_content += f"   - 风险: {item.get('risk', 'N/A')}\n\n"

        rotation = predictions.get("rotation", {}) or {}
        current_strong = rotation.get("current_strong", []) or []
        potential = rotation.get("potential", []) or []
        declining = rotation.get("declining", []) or []

        markdown_content += "### 🔄 板块轮动预测\n\n"

        if current_strong:
            markdown_content += "#### 💪 当前强势板块\n\n"
            for item in current_strong:
                markdown_content += f"- **{item.get('sector', 'N/A')}**\n"
                markdown_content += f"  - 轮动逻辑: {item.get('logic', 'N/A')}\n"
                markdown_content += f"  - 时间窗口: {item.get('time_window', 'N/A')}\n"
                markdown_content += f"  - 操作建议: {item.get('advice', 'N/A')}\n\n"

        if potential:
            markdown_content += "#### 🌱 潜力接力板块\n\n"
            for item in potential:
                markdown_content += f"- **{item.get('sector', 'N/A')}**\n"
                markdown_content += f"  - 轮动逻辑: {item.get('logic', 'N/A')}\n"
                markdown_content += f"  - 时间窗口: {item.get('time_window', 'N/A')}\n"
                markdown_content += f"  - 操作建议: {item.get('advice', 'N/A')}\n\n"

        if declining:
            markdown_content += "#### 📉 衰退板块\n\n"
            for item in declining:
                markdown_content += f"- **{item.get('sector', 'N/A')}**\n"
                markdown_content += f"  - 轮动逻辑: {item.get('logic', 'N/A')}\n"
                markdown_content += f"  - 时间窗口: {item.get('time_window', 'N/A')}\n"
                markdown_content += f"  - 操作建议: {item.get('advice', 'N/A')}\n\n"

        heat = predictions.get("heat", {}) or {}
        hottest = heat.get("hottest", []) or []
        heating = heat.get("heating", []) or []
        cooling = heat.get("cooling", []) or []

        markdown_content += "### 🔥 板块热度排行\n\n"

        if hottest:
            markdown_content += (
                "#### 最热板块\n\n| 排名 | 板块 | 热度评分 | 趋势 | 持续性 |\n|------|------|----------|------|--------|\n"
            )
            for idx, item in enumerate(hottest[:10], 1):
                markdown_content += (
                    f"| {idx} | {item.get('sector', 'N/A')} | {item.get('score', 0)} | "
                    f"{item.get('trend', 'N/A')} | {item.get('sustainability', 'N/A')} |\n"
                )
            markdown_content += "\n"

        if heating:
            markdown_content += "#### 升温板块\n\n"
            for idx, item in enumerate(heating[:5], 1):
                markdown_content += (
                    f"{idx}. {item.get('sector', 'N/A')} (评分: {item.get('score', 0)})\n"
                )
            markdown_content += "\n"

        if cooling:
            markdown_content += "#### 降温板块\n\n"
            for idx, item in enumerate(cooling[:5], 1):
                markdown_content += (
                    f"{idx}. {item.get('sector', 'N/A')} (评分: {item.get('score', 0)})\n"
                )
            markdown_content += "\n"

        summary = predictions.get("summary", {}) or {}
        if summary:
            markdown_content += "### 📝 策略总结\n\n"
            if summary.get("market_view"):
                markdown_content += f"**市场观点:** {summary.get('market_view', '')}\n\n"
            if summary.get("key_opportunity"):
                markdown_content += f"**核心机会:** {summary.get('key_opportunity', '')}\n\n"
            if summary.get("major_risk"):
                markdown_content += f"**主要风险:** {summary.get('major_risk', '')}\n\n"
            if summary.get("strategy"):
                markdown_content += f"**整体策略:** {summary.get('strategy', '')}\n\n"

    agents_analysis = result_data.get("agents_analysis", {}) or {}
    if agents_analysis:
        markdown_content += "## 🤖 AI智能体分析\n\n"
        for _, agent_data in agents_analysis.items():
            agent_name = agent_data.get("agent_name", "未知分析师")
            agent_role = agent_data.get("agent_role", "")
            focus_areas = ", ".join(agent_data.get("focus_areas", []) or [])
            analysis = agent_data.get("analysis", "")

            markdown_content += f"### {agent_name}\n\n"
            markdown_content += f"- **职责**: {agent_role}\n"
            markdown_content += f"- **关注领域**: {focus_areas}\n\n"
            markdown_content += f"{analysis}\n\n"
            markdown_content += "---\n\n"

    comprehensive_report = result_data.get("comprehensive_report", "")
    if comprehensive_report:
        markdown_content += "## 📊 综合研判\n\n"
        markdown_content += f"{comprehensive_report}\n\n"

    markdown_content += """
---

*报告由智策AI系统自动生成*
"""

    return markdown_content
