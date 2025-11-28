from __future__ import annotations

from datetime import datetime
from typing import Any, Dict


def generate_longhubang_markdown_report(result_data: Dict[str, Any]) -> str:
    """生成龙虎榜分析 Markdown 报告。

    该实现基于旧版 Streamlit 中的 generate_markdown_report 抽取，
    移除了对 st 的依赖，可在 FastAPI 等无 UI 环境中直接复用。
    """

    current_time = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")

    # 标题与报告概览
    markdown_content = f"""# 智瞰龙虎榜分析报告

**AI驱动的龙虎榜多维度分析系统**

---

## 📊 报告概览

- **生成时间**: {current_time}
- **数据记录**: {result_data.get('data_info', {}).get('total_records', 0)} 条
- **涉及股票**: {result_data.get('data_info', {}).get('total_stocks', 0)} 只
- **涉及游资**: {result_data.get('data_info', {}).get('total_youzi', 0)} 个
- **AI分析师**: 5位专业分析师团队
- **分析模型**: DeepSeek AI Multi-Agent System

> ⚠️ 本报告由AI系统基于龙虎榜公开数据自动生成，仅供参考，不构成投资建议。市场有风险，投资需谨慎。

---

## 📈 数据概况

本次分析共涵盖 **{result_data.get('data_info', {}).get('total_records', 0)}** 条龙虎榜记录，
涉及 **{result_data.get('data_info', {}).get('total_stocks', 0)}** 只股票和 
**{result_data.get('data_info', {}).get('total_youzi', 0)}** 个游资席位。

"""

    # 资金概况
    summary = result_data.get("data_info", {}).get("summary", {}) or {}
    markdown_content += f"""
### 💰 资金概况

- **总买入金额**: {summary.get('total_buy_amount', 0):,.2f} 元
- **总卖出金额**: {summary.get('total_sell_amount', 0):,.2f} 元
- **净流入金额**: {summary.get('total_net_inflow', 0):,.2f} 元

"""

    # 分析元信息
    data_range = result_data.get("data_date_range")
    meta = result_data.get("analysis_meta") or {}
    mode = meta.get("mode")
    date = meta.get("date")
    days = meta.get("days")

    mode_text = "未知"
    window_text = ""
    if mode == "date" and date:
        mode_text = "指定日期"
        window_text = f"分析日期：{date}"
    elif mode == "recent_days" and days:
        mode_text = "最近N天"
        window_text = f"最近 {days} 天数据"

    markdown_content += "### 🔎 分析元信息\n\n"
    markdown_content += f"- **数据日期范围**: {data_range or '未记录'}\n"
    markdown_content += f"- **分析模式**: {mode_text}\n"
    if window_text:
        markdown_content += f"- **分析窗口**: {window_text}\n"
    markdown_content += "\n"

    # 最终报告摘要
    final_report = result_data.get("final_report", {}) or {}
    final_summary = final_report.get("summary") or ""
    if final_summary:
        markdown_content += "### 📝 最终报告摘要\n\n"
        markdown_content += f"{final_summary}\n\n"

    # TOP 游资
    if summary.get("top_youzi"):
        markdown_content += (
            "### 🏆 活跃游资 TOP10\n\n"
            "| 排名 | 游资名称 | 净流入金额(元) |\n"
            "|------|----------|---------------|\n"
        )
        for idx, (name, amount) in enumerate(list(summary["top_youzi"].items())[:10], 1):
            markdown_content += f"| {idx} | {name} | {amount:,.2f} |\n"
        markdown_content += "\n"

    # TOP 股票
    if summary.get("top_stocks"):
        markdown_content += (
            "### 📈 资金净流入 TOP20 股票\n\n"
            "| 排名 | 股票代码 | 股票名称 | 净流入金额(元) |\n"
            "|------|----------|----------|---------------|\n"
        )
        for idx, stock in enumerate(summary["top_stocks"][:20], 1):
            markdown_content += (
                f"| {idx} | {stock['code']} | {stock['name']} | "
                f"{stock['net_inflow']:,.2f} |\n"
            )
        markdown_content += "\n"

    # 热门概念
    if summary.get("hot_concepts"):
        markdown_content += "### 🔥 热门概念 TOP15\n\n"
        for idx, (concept, count) in enumerate(
            list(summary["hot_concepts"].items())[:15], 1
        ):
            markdown_content += f"{idx}. {concept} ({count}次)  \n"
        markdown_content += "\n"

    # AI 智能评分排名
    scoring = result_data.get("scoring_ranking") or []
    if scoring:
        markdown_content += (
            "## 🏆 AI智能评分排名 (TOP10)\n\n"
            "| 排名 | 股票名称 | 股票代码 | 综合评分 | 资金含金量 | 净买入额 | 卖出压力 | 机构共振 | 加分项 | 顶级游资 | 买方数 | 机构参与 | 净流入(元) |\n"
            "|------|----------|----------|----------|------------|----------|----------|----------|--------|----------|--------|----------|------------|\n"
        )
        for row in scoring[:10]:
            rank = row.get("排名") or row.get("rank") or "-"
            name = row.get("股票名称") or row.get("name") or "-"
            code = row.get("股票代码") or row.get("code") or "-"
            total = row.get("综合评分") or row.get("score") or "-"
            gold = row.get("资金含金量") or "-"
            net_buy = row.get("净买入额") or row.get("net_buy") or "-"
            sell_p = row.get("卖出压力") or "-"
            inst = row.get("机构共振") or "-"
            bonus = row.get("加分项") or "-"
            top_yz = row.get("顶级游资") or "-"
            buyers = row.get("买方数") or "-"
            inst_part = row.get("机构参与") or "-"
            net_inflow = row.get("净流入") or row.get("total_net_inflow") or "-"

            markdown_content += (
                f"| {rank} | {name} | {code} | {total} | {gold} | {net_buy} | {sell_p} | {inst} | {bonus} | "
                f"{top_yz} | {buyers} | {inst_part} | {net_inflow} |\n"
            )
        markdown_content += "\n"

    # 推荐股票
    recommended = result_data.get("recommended_stocks", []) or []
    if recommended:
        markdown_content += f"""
## 🎯 AI推荐股票

基于5位AI分析师的综合分析，系统识别出以下 **{len(recommended)}** 只潜力股票，
这些股票在资金流向、游资关注度、题材热度等多个维度表现突出。

### 推荐股票清单

| 排名 | 股票代码 | 股票名称 | 净流入金额 | 确定性 | 持有周期 |
|------|----------|----------|------------|--------|----------|
"""
        for stock in recommended[:10]:
            markdown_content += (
                f"| {stock.get('rank', '-')} | {stock.get('code', '-')} | "
                f"{stock.get('name', '-')} | {stock.get('net_inflow', 0):,.0f} | "
                f"{stock.get('confidence', '-')} | {stock.get('hold_period', '-')} |\n"
            )

        markdown_content += "\n### 推荐理由详解\n\n"
        for stock in recommended[:5]:  # 只详细展示前 5 只
            markdown_content += (
                f"**{stock.get('rank', '-')}. {stock.get('name', '-')} "
                f"({stock.get('code', '-')})**\n\n"
            )
            markdown_content += f"- 推荐理由: {stock.get('reason', '暂无')}\n"
            markdown_content += f"- 确定性: {stock.get('confidence', '-')}\n"
            markdown_content += f"- 持有周期: {stock.get('hold_period', '-')}\n\n"

    # AI 分析师报告
    agents_analysis = result_data.get("agents_analysis", {}) or {}
    if agents_analysis:
        markdown_content += "## 🤖 AI分析师报告\n\n"
        markdown_content += "本报告由5位AI专业分析师从不同维度进行分析，综合形成投资建议：\n\n"
        markdown_content += "- **游资行为分析师** - 分析游资操作特征和意图\n"
        markdown_content += "- **个股潜力分析师** - 挖掘次日大概率上涨的股票\n"
        markdown_content += "- **题材追踪分析师** - 识别热点题材和轮动机会\n"
        markdown_content += "- **风险控制专家** - 识别高风险股票和市场陷阱\n"
        markdown_content += "- **首席策略师** - 综合研判并给出最终建议\n\n"

        agent_titles = {
            "youzi": "游资行为分析师",
            "stock": "个股潜力分析师",
            "theme": "题材追踪分析师",
            "risk": "风险控制专家",
            "chief": "首席策略师综合研判",
        }

        for agent_key, agent_title in agent_titles.items():
            agent_data = agents_analysis.get(agent_key, {}) or {}
            if agent_data:
                markdown_content += f"### {agent_title}\n\n"
                analysis_text = agent_data.get("analysis", "暂无分析")
                analysis_text = analysis_text.replace("\n", "\n\n")
                markdown_content += f"{analysis_text}\n\n"

    markdown_content += """
---

*报告由智瞰龙虎AI系统自动生成*
"""

    return markdown_content
