from __future__ import annotations

"""PDF/Markdown 报告生成实现（next_app 内部使用）。

基于根目录 pdf_generator.py / pdf_generator_fixed.py 的实现改写，
去掉所有 Streamlit 依赖，只保留纯函数用于生成报告内容。
"""

from datetime import datetime
from typing import Any, Dict

import io
import json
import os

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


def _register_chinese_fonts() -> str:
    """注册中文字体，返回字体名。

    与旧版 pdf_generator.register_chinese_fonts 等价，但移除打印与异常噪音。
    """

    try:
        if "ChineseFont" in pdfmetrics.getRegisteredFontNames():
            return "ChineseFont"

        windows_font_paths = [
            "C:/Windows/Fonts/simsun.ttc",
            "C:/Windows/Fonts/simhei.ttf",
            "C:/Windows/Fonts/msyh.ttc",
            "C:/Windows/Fonts/msyh.ttf",
        ]
        linux_font_paths = [
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
            "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
        ]
        for font_path in windows_font_paths + linux_font_paths:
            if os.path.exists(font_path):
                try:
                    pdfmetrics.registerFont(TTFont("ChineseFont", font_path))
                    return "ChineseFont"
                except Exception:  # noqa: BLE001
                    continue
        return "Helvetica"
    except Exception:  # noqa: BLE001
        return "Helvetica"


def create_pdf_report(
    stock_info: Dict[str, Any],
    agents_results: Dict[str, Any],
    discussion_result: Any,
    final_decision: Dict[str, Any],
) -> bytes:
    """创建 PDF 格式的单股分析报告，返回 PDF 字节流。

    参数与旧版 pdf_generator.create_pdf_report 一致。
    """

    chinese_font = _register_chinese_fonts()

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=18,
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "CustomTitle",
        parent=styles["Heading1"],
        fontName=chinese_font,
        fontSize=24,
        spaceAfter=30,
        alignment=TA_CENTER,
        textColor=colors.darkblue,
    )

    heading_style = ParagraphStyle(
        "CustomHeading",
        parent=styles["Heading2"],
        fontName=chinese_font,
        fontSize=16,
        spaceAfter=12,
        spaceBefore=20,
        textColor=colors.darkblue,
    )

    subheading_style = ParagraphStyle(
        "CustomSubHeading",
        parent=styles["Heading3"],
        fontName=chinese_font,
        fontSize=14,
        spaceAfter=8,
        spaceBefore=12,
        textColor=colors.darkgreen,
    )

    normal_style = ParagraphStyle(
        "CustomNormal",
        parent=styles["Normal"],
        fontName=chinese_font,
        fontSize=11,
        spaceAfter=6,
        alignment=TA_JUSTIFY,
    )

    story = []

    current_time = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")
    story.append(Paragraph("AI股票分析报告", title_style))
    story.append(Paragraph(f"生成时间: {current_time}", normal_style))
    story.append(Spacer(1, 20))

    # 股票基本信息
    story.append(Paragraph("股票基本信息", heading_style))

    stock_data = [
        ["项目", "值"],
        ["股票代码", stock_info.get("symbol", "N/A")],
        ["股票名称", stock_info.get("name", "N/A")],
        ["当前价格", str(stock_info.get("current_price", "N/A"))],
        ["涨跌幅", f"{stock_info.get('change_percent', 'N/A')}%"],
        ["市盈率(PE)", str(stock_info.get("pe_ratio", "N/A"))],
        ["市净率(PB)", str(stock_info.get("pb_ratio", "N/A"))],
        ["市值", str(stock_info.get("market_cap", "N/A"))],
        ["市场", stock_info.get("market", "N/A")],
        ["交易所", stock_info.get("exchange", "N/A")],
    ]

    stock_table = Table(stock_data, colWidths=[2 * inch, 3 * inch])
    stock_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (-1, 0), chinese_font),
                ("FONTSIZE", (0, 0), (-1, 0), 12),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("FONTNAME", (0, 1), (-1, -1), chinese_font),
                ("FONTSIZE", (0, 1), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ]
        )
    )

    story.append(stock_table)
    story.append(Spacer(1, 20))

    # 各分析师分析结果
    story.append(Paragraph("AI分析师团队分析", heading_style))

    # 优先按预定义顺序展示常见分析师角色
    agent_names = {
        "technical": "技术分析师",
        "fundamental": "基本面分析师",
        "fund_flow": "资金面分析师",
        "risk_management": "风险管理师",
        "market_sentiment": "市场情绪分析师",
    }

    used_keys = set()

    def _render_agent_block(title: str, result: Any) -> None:
        story.append(Paragraph(title, subheading_style))
        if isinstance(result, dict):
            analysis_text = result.get("analysis", "暂无分析")
        else:
            analysis_text = str(result)
        analysis_text = str(analysis_text).replace("\n", "<br/>")
        story.append(Paragraph(analysis_text, normal_style))
        story.append(Spacer(1, 12))

    # 先渲染内置映射中定义的分析师
    for agent_key, agent_name in agent_names.items():
        if agent_key in agents_results:
            used_keys.add(agent_key)
            _render_agent_block(f"{agent_name}分析", agents_results[agent_key])

    # 再渲染其余未在内置映射中的分析师，确保所有分析师都出现在报告中
    for agent_key, agent_result in agents_results.items():
        if agent_key in used_keys:
            continue
        display_name = None
        if isinstance(agent_result, dict):
            display_name = agent_result.get("agent_name")
        if not display_name:
            display_name = agent_names.get(agent_key) or str(agent_key)
        _render_agent_block(f"{display_name}分析", agent_result)

    # 团队讨论
    story.append(Paragraph("团队综合讨论", heading_style))
    discussion_text = str(discussion_result).replace("\n", "<br/>")
    story.append(Paragraph(discussion_text, normal_style))
    story.append(Spacer(1, 20))

    # 最终投资决策
    story.append(Paragraph("最终投资决策", heading_style))

    if isinstance(final_decision, dict) and "decision_text" not in final_decision:
        decision_data = [
            ["项目", "内容"],
            ["投资评级", final_decision.get("rating", "未知")],
            ["目标价位", str(final_decision.get("target_price", "N/A"))],
            ["操作建议", final_decision.get("operation_advice", "暂无建议")],
            ["进场区间", final_decision.get("entry_range", "N/A")],
            ["止盈位", str(final_decision.get("take_profit", "N/A"))],
            ["止损位", str(final_decision.get("stop_loss", "N/A"))],
            ["持有周期", final_decision.get("holding_period", "N/A")],
            ["仓位建议", final_decision.get("position_size", "N/A")],
            ["信心度", f"{final_decision.get('confidence_level', 'N/A')}/10"],
            ["风险提示", final_decision.get("risk_warning", "无")],
        ]

        decision_table = Table(decision_data, colWidths=[1.5 * inch, 3.5 * inch])
        decision_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.darkblue),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("FONTNAME", (0, 0), (-1, 0), chinese_font),
                    ("FONTSIZE", (0, 0), (-1, 0), 12),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.lightblue),
                    ("FONTNAME", (0, 1), (-1, -1), chinese_font),
                    ("FONTSIZE", (0, 1), (-1, -1), 10),
                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                ]
            )
        )
        story.append(decision_table)
    else:
        decision_text = (
            final_decision.get("decision_text")
            if isinstance(final_decision, dict)
            else str(final_decision)
        )
        decision_text = str(decision_text).replace("\n", "<br/>")
        story.append(Paragraph(decision_text, normal_style))

    story.append(Spacer(1, 20))

    # 免责声明
    story.append(Paragraph("免责声明", heading_style))
    disclaimer_text = (
        "本报告由AI系统生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。"
        "请在做出投资决策前咨询专业的投资顾问。本系统不对任何投资损失承担责任。"
    )
    story.append(Paragraph(disclaimer_text, normal_style))

    doc.build(story)
    pdf_content = buffer.getvalue()
    buffer.close()

    return pdf_content


def generate_markdown_report(
    stock_info: Dict[str, Any],
    agents_results: Dict[str, Any],
    discussion_result: Any,
    final_decision: Dict[str, Any],
) -> str:
    """生成 Markdown 格式的分析报告文本。

    直接移植自 pdf_generator.generate_markdown_report，做轻微整理。
    """

    current_time = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")

    md = f"""
# AI股票分析报告

**生成时间**: {current_time}

---

## 📊 股票基本信息

| 项目 | 值 |
|------|-----|
| **股票代码** | {stock_info.get('symbol', 'N/A')} |
| **股票名称** | {stock_info.get('name', 'N/A')} |
| **当前价格** | {stock_info.get('current_price', 'N/A')} |
| **涨跌幅** | {stock_info.get('change_percent', 'N/A')}% |
| **市盈率(PE)** | {stock_info.get('pe_ratio', 'N/A')} |
| **市净率(PB)** | {stock_info.get('pb_ratio', 'N/A')} |
| **市值** | {stock_info.get('market_cap', 'N/A')} |
| **市场** | {stock_info.get('market', 'N/A')} |
| **交易所** | {stock_info.get('exchange', 'N/A')} |

---

## 🔍 各分析师详细分析

"""

    agent_names = {
        "technical": "📈 技术分析师",
        "fundamental": "📊 基本面分析师",
        "fund_flow": "💰 资金面分析师",
        "risk_management": "⚠️ 风险管理师",
        "market_sentiment": "📈 市场情绪分析师",
    }

    for agent_key, agent_name in agent_names.items():
        if agent_key in agents_results:
            agent_result = agents_results[agent_key]
            if isinstance(agent_result, dict):
                analysis_text = agent_result.get("analysis", "暂无分析")
            else:
                analysis_text = str(agent_result)
            md += f"""
### {agent_name}

{analysis_text}

---

"""

    md += f"""
## 🤝 团队综合讨论

{discussion_result}

---

## 📋 最终投资决策

"""

    if isinstance(final_decision, dict) and "decision_text" not in final_decision:
        md += f"""
**投资评级**: {final_decision.get('rating', '未知')}

**目标价位**: {final_decision.get('target_price', 'N/A')}

**操作建议**: {final_decision.get('operation_advice', '暂无建议')}

**进场区间**: {final_decision.get('entry_range', 'N/A')}

**止盈位**: {final_decision.get('take_profit', 'N/A')}

**止损位**: {final_decision.get('stop_loss', 'N/A')}

**持有周期**: {final_decision.get('holding_period', 'N/A')}

**仓位建议**: {final_decision.get('position_size', 'N/A')}

**信心度**: {final_decision.get('confidence_level', 'N/A')}/10

**风险提示**: {final_decision.get('risk_warning', '无')}
"""
    else:
        if isinstance(final_decision, dict):
            decision_text = final_decision.get("decision_text", json.dumps(final_decision, ensure_ascii=False))
        else:
            decision_text = str(final_decision)
        md += decision_text

    md += """

---

## 📝 免责声明

本报告由AI系统生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。请在做出投资决策前咨询专业的投资顾问。

---

*报告生成时间: {current_time}*
*AI股票分析系统 v1.0*
"""

    return md
