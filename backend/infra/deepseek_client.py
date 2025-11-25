"""DeepSeek API 客户端（next_app 内部实现）。

与根目录 deepseek_client 行为等价：
- 统一通过 DeepSeekClient 调用 chat.completions 接口；
- 暴露 technical_analysis / fundamental_analysis / fund_flow_analysis /
  comprehensive_discussion / final_decision 等高层方法；
- 仅依赖环境变量配置，不再依赖根目录 config 模块。
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import openai


class DeepSeekClient:
    """DeepSeek API 客户端（供 next_app 使用）。"""

    def __init__(self, model: str = "deepseek-chat") -> None:
        self.model = model
        api_key = os.getenv("DEEPSEEK_API_KEY", "")
        base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
        self.client = openai.OpenAI(api_key=api_key, base_url=base_url)

    # ------------------------------------------------------------------
    # 底层调用封装
    # ------------------------------------------------------------------
    def call_api(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> str:
        model_to_use = model or self.model

        # reasoner 模型通常需要更长输出
        if "reasoner" in model_to_use.lower() and max_tokens <= 2000:
            max_tokens = 8000

        try:
            resp = self.client.chat.completions.create(
                model=model_to_use,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            message = resp.choices[0].message
            result = ""
            # DeepSeek reasoner 可能带有 reasoning_content
            reasoning = getattr(message, "reasoning_content", None)
            if reasoning:
                result += f"【推理过程】\n{reasoning}\n\n"
            if message.content:
                result += str(message.content)
            return result or "API返回空响应"
        except Exception as e:  # noqa: BLE001
            return f"API调用失败: {e}"

    # ------------------------------------------------------------------
    # 高层分析方法（提示词保持与旧实现语义接近）
    # ------------------------------------------------------------------
    def technical_analysis(
        self, stock_info: Dict[str, Any], stock_data: Any, indicators: Dict[str, Any]
    ) -> str:
        """技术面分析。"""

        prompt = f"""
你是一名资深的技术分析师，请基于以下信息做专业的技术面分析：

【股票信息】
- 代码：{stock_info.get('symbol', 'N/A')}
- 名称：{stock_info.get('name', 'N/A')}
- 当前价格：{stock_info.get('current_price', 'N/A')}
- 涨跌幅：{stock_info.get('change_percent', 'N/A')}%

【最新技术指标】
- 收盘价：{indicators.get('price', 'N/A')}
- MA5：{indicators.get('ma5', 'N/A')}
- MA10：{indicators.get('ma10', 'N/A')}
- MA20：{indicators.get('ma20', 'N/A')}
- MA60：{indicators.get('ma60', 'N/A')}
- RSI：{indicators.get('rsi', 'N/A')}
- MACD：{indicators.get('macd', 'N/A')}
- MACD信号线：{indicators.get('macd_signal', 'N/A')}
- 布林带上轨：{indicators.get('bb_upper', 'N/A')}
- 布林带下轨：{indicators.get('bb_lower', 'N/A')}
- K值：{indicators.get('k_value', 'N/A')}
- D值：{indicators.get('d_value', 'N/A')}
- 量比：{indicators.get('volume_ratio', 'N/A')}

请从以下角度系统分析：
1. 趋势与均线结构
2. 超买超卖与情绪（RSI、KDJ）
3. 动量与背离（MACD）
4. 支撑阻力与波动区间（布林带）
5. 成交量与量价配合
6. 短中长周期的技术判断
7. 明确给出技术面结论与风险提示。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名经验丰富的股票技术分析师，擅长基于指标做客观研判。",
            },
            {"role": "user", "content": prompt},
        ]
        return self.call_api(messages)

    def fundamental_analysis(
        self,
        stock_info: Dict[str, Any],
        financial_data: Optional[Dict[str, Any]] = None,
        quarterly_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """基本面分析。

        提示词和分析结构尽量与旧版 deepseek_client.fundamental_analysis 保持一致，
        在有季报数据时重点利用最近 8 期季报趋势。
        """

        # 构建财务数据部分（如有 financial_ratios 则使用，否则留空，不影响季报分析）
        financial_section = ""
        if isinstance(financial_data, dict) and not financial_data.get("error"):
            ratios = financial_data.get("financial_ratios", {})
            if ratios:
                financial_section = f"""
详细财务指标：
【盈利能力】
- 净资产收益率(ROE)：{ratios.get('净资产收益率ROE', ratios.get('ROE', 'N/A'))}
- 总资产收益率(ROA)：{ratios.get('总资产收益率ROA', ratios.get('ROA', 'N/A'))}
- 销售毛利率：{ratios.get('销售毛利率', ratios.get('毛利率', 'N/A'))}
- 销售净利率：{ratios.get('销售净利率', ratios.get('净利率', 'N/A'))}

【偿债能力】
- 资产负债率：{ratios.get('资产负债率', 'N/A')}
- 流动比率：{ratios.get('流动比率', 'N/A')}
- 速动比率：{ratios.get('速动比率', 'N/A')}

【运营能力】
- 存货周转率：{ratios.get('存货周转率', 'N/A')}
- 应收账款周转率：{ratios.get('应收账款周转率', 'N/A')}
- 总资产周转率：{ratios.get('总资产周转率', 'N/A')}

【成长能力】
- 营业收入同比增长：{ratios.get('营业收入同比增长', ratios.get('收入增长', 'N/A'))}
- 净利润同比增长：{ratios.get('净利润同比增长', ratios.get('盈利增长', 'N/A'))}

【每股指标】
- 每股收益(EPS)：{ratios.get('EPS', 'N/A')}
- 每股账面价值：{ratios.get('每股账面价值', 'N/A')}
- 股息率：{ratios.get('股息率', stock_info.get('dividend_yield', 'N/A'))}
- 派息率：{ratios.get('派息率', 'N/A')}
"""

                if ratios.get("报告期"):
                    financial_section = (
                        f"\n财务数据报告期：{ratios.get('报告期')}\n" + financial_section
                    )

        # 构建季报数据部分：使用 next_app 内部 QuarterlyReportDataFetcher 进行格式化
        quarterly_section = ""
        if isinstance(quarterly_data, dict) and quarterly_data.get("data_success"):
            try:
                from ..core.quarterly_report_data_impl import (
                    QuarterlyReportDataFetcher,
                )

                fetcher = QuarterlyReportDataFetcher()
                quarterly_section = f"""

【最近8期季报详细数据】
{fetcher.format_quarterly_reports_for_ai(quarterly_data)}

以上是通过统一数据访问模块获取的最近8期季度财务报告，请重点基于这些数据进行趋势分析。
"""
            except Exception:
                # 格式化失败时退回简单 JSON 文本，避免中断分析
                try:
                    quarterly_section = json.dumps(quarterly_data, ensure_ascii=False)[:6000]
                except Exception:
                    quarterly_section = str(quarterly_data)[:6000]

        prompt = f"""
你是一名资深的基本面分析师，拥有CFA资格和10年以上的证券分析经验。请基于以下详细信息进行深入的基本面分析：

【基本信息】
- 股票代码：{stock_info.get('symbol', 'N/A')}
- 股票名称：{stock_info.get('name', 'N/A')}
- 当前价格：{stock_info.get('current_price', 'N/A')}
- 市值：{stock_info.get('market_cap', 'N/A')}
- 行业：{stock_info.get('sector', 'N/A')}
- 细分行业：{stock_info.get('industry', 'N/A')}

【估值指标】
- 市盈率(PE)：{stock_info.get('pe_ratio', 'N/A')}
- 市净率(PB)：{stock_info.get('pb_ratio', 'N/A')}
- 市销率(PS)：{stock_info.get('ps_ratio', 'N/A')}
- Beta系数：{stock_info.get('beta', 'N/A')}
- 52周最高：{stock_info.get('52_week_high', 'N/A')}
- 52周最低：{stock_info.get('52_week_low', 'N/A')}
{financial_section}
{quarterly_section}

请从以下维度进行专业、深入的分析：

1. **公司质地分析**
   - 业务模式和核心竞争力
   - 行业地位和市场份额
   - 护城河分析（品牌、技术、规模等）

2. **盈利能力分析**
   - ROE和ROA水平评估
   - 毛利率和净利率趋势
   - 与行业平均水平对比
   - 盈利质量和持续性

3. **财务健康度分析**
   - 资产负债结构
   - 偿债能力评估
   - 现金流状况
   - 财务风险识别

4. **成长性分析**
   - 收入和利润增长趋势
   - 增长驱动因素
   - 未来成长空间
   - 行业发展前景

5. **季报趋势分析（如有季报数据）** ⭐ 重点分析
   - **营收趋势**：分析最近8期营业收入的变化趋势，识别增长或下滑
   - **利润趋势**：分析净利润和每股收益的变化，评估盈利能力变化
   - **现金流分析**：经营现金流、投资现金流、筹资现金流的变化趋势
   - **资产负债变化**：资产规模、负债水平、所有者权益的变化
   - **季度环比/同比**：计算关键指标的环比和同比变化率
   - **经营质量**：评估收入质量、利润质量、现金流质量
   - **异常识别**：识别异常波动，分析原因（季节性、一次性事件等）
   - **趋势预判**：基于最近8期数据预判未来1-2个季度趋势

6. **估值分析**
   - 当前估值水平（PE、PB）
   - 历史估值区间对比
   - 行业估值对比
   - 结合季报趋势调整估值预期
   - 合理估值区间判断

7. **投资价值判断**
   - 综合评分（0-100分）
   - 投资亮点（特别关注季报改善趋势）
   - 投资风险（关注季报恶化信号）
   - 适合的投资者类型

**分析要求：**
- 如果有季报数据，请重点分析8期数据的趋势变化
- 识别改善或恶化的早期信号
- 结合季报数据对未来业绩进行预判
- 数据分析要深入，结论要有依据
- 结合当前市场环境和行业发展趋势

请给出专业、详细的基本面分析报告。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名经验丰富的股票基本面分析师，擅长公司财务分析和行业研究。",
            },
            {"role": "user", "content": prompt},
        ]
        return self.call_api(messages, max_tokens=4000)

    def fund_flow_analysis(
        self,
        stock_info: Dict[str, Any],
        indicators: Dict[str, Any],
        fund_flow_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """资金面分析。

        保持与根目录 deepseek_client.fund_flow_analysis 一致的签名和提示词，
        以确保与 ai_agents_impl.fund_flow_analyst_agent 完全兼容。
        """

        # 构建资金流向数据部分 - 使用统一实现格式化数据
        fund_flow_section = ""
        margin_history = fund_flow_data.get("margin_trading_history") if fund_flow_data else None

        if fund_flow_data and fund_flow_data.get("data_success"):
            from ..core.fund_flow_akshare_impl import FundFlowAkshareDataFetcher

            fetcher = FundFlowAkshareDataFetcher()
            fund_flow_section = f"""

【近20个交易日资金流向详细数据】
{fetcher.format_fund_flow_for_ai(fund_flow_data)}

以上数据均由统一数据访问模块预先获取（Tushare优先，Akshare备用），请重点基于这些数据进行趋势分析。
"""
        else:
            fund_flow_section = "\n【资金流向数据】\n注意：未能获取到资金流向数据，将基于成交量进行分析。\n"

        margin_section = ""
        if margin_history and margin_history.get("records"):

            def fmt_num(value: Any) -> str:
                if value is None:
                    return "N/A"
                try:
                    num = float(value)
                except (TypeError, ValueError):
                    return str(value)
                if abs(num) >= 1e12:
                    return f"{num / 1e12:.2f}万亿"
                if abs(num) >= 1e8:
                    return f"{num / 1e8:.2f}亿"
                return f"{num:,.2f}"

            def fmt_record(rec: Dict[str, Any]) -> str:
                return (
                    f"  * {rec.get('trade_date', 'N/A')} | "
                    f"融资余额 {fmt_num(rec.get('margin_balance'))}元 | "
                    f"净融资买入 {fmt_num(rec.get('net_margin_buy'))}元 | "
                    f"融券余额 {fmt_num(rec.get('short_balance'))}元 | "
                    f"净融券卖出 {fmt_num(rec.get('net_short_sell'))}元"
                )

            margin_section = f"""

【近5个交易日融资融券数据】（来源：{margin_history.get('source', 'tushare')}，统一数据访问模块）
- 观察区间：{margin_history.get('first_date', 'N/A')} ~ {margin_history.get('last_date', 'N/A')}
- 融资余额变化：{fmt_num(margin_history.get('margin_balance_change'))}元
- 融券余额变化：{fmt_num(margin_history.get('short_balance_change'))}元
- 净融资买入合计：{fmt_num(margin_history.get('net_margin_buy_total'))}元
- 净融券卖出合计：{fmt_num(margin_history.get('net_short_sell_total'))}元
近5日明细：
{chr(10).join(fmt_record(rec) for rec in margin_history.get('records', []))}
"""
        else:
            margin_section = "\n【融资融券历史】\n注意：未能获取融资融券历史数据，将以资金流向数据为主。\n"

        prompt = f"""
你是一名资深的资金面分析师，擅长从资金流向数据中洞察主力行为和市场趋势。

【基本信息】
股票代码：{stock_info.get('symbol', 'N/A')}
股票名称：{stock_info.get('name', 'N/A')}
当前价格：{stock_info.get('current_price', 'N/A')}
市值：{stock_info.get('market_cap', 'N/A')}

【技术指标】
- 量比：{indicators.get('volume_ratio', 'N/A')}
- 当前成交量与5日均量比：{indicators.get('volume_ratio', 'N/A')}
{fund_flow_section}
{margin_section}

【分析要求】

请你**基于上述近20个交易日的资金流向数据，以及近5个交易日的融资融券数据**，从以下角度进行深入分析：

1. **资金流向趋势分析** ⭐ 重点
   - 分析近20个交易日主力资金的累计净流入/净流出
   - 识别资金流向的趋势性特征（持续流入、持续流出、震荡）
   - 计算主力资金净流入天数占比
   - 评估资金流向强度（累计金额、平均每日金额）

2. **主力资金行为分析** ⭐ 核心重点
   - **主力资金总体表现**：累计净流入金额、占比、趋势方向
   - **超大单分析**：机构大资金的进出动作
   - **大单分析**：主力资金的操作特征
   - **主力操作意图研判**：
     * 吸筹建仓：持续净流入 + 股价上涨/盘整
     * 派发出货：持续净流出 + 股价下跌/高位
     * 洗盘整理：震荡流入流出 + 股价调整
     * 拉升推动：集中大额流入 + 股价快速上涨

3. **散户资金行为分析**
   - **中单、小单的动向**：散户的买卖情绪
   - **主力与散户博弈**：
     * 主力流入、散户流出 → 专业资金吸筹
     * 主力流出、散户流入 → 高位接盘风险
     * 同向流动 → 趋势明确
   - 散户参与度和情绪判断

4. **融资融券动向分析** ⭐ 重点
   - 近5个交易日融资余额、融券余额、净融资买入和净融券卖出变化
   - 判断融资资金是持续加仓还是减仓，融券是否增加压制
   - 结合资金流向数据，分析多空力量变化及未来可能走势

5. **量价配合分析**
   - 资金流向与股价涨跌的配合度
   - 识别量价背离：
     * 价涨量缩 + 资金流出 → 警惕顶部
     * 价跌量增 + 资金流入 → 可能见底
   - 成交活跃度变化趋势

6. **关键信号识别**
   - **买入信号**：
     * 主力持续净流入
     * 大单明显流入
     * 资金流入 + 股价上涨
   - **卖出信号**：
     * 主力持续净流出
     * 大额资金出逃
     * 资金流出 + 股价滞涨或下跌
   - **观望信号**：
     * 资金流向不明确
     * 主力与散户博弈激烈

7. **阶段性特征**
   - 早期阶段（前10个交易日）vs 近期阶段（后10个交易日）
   - 资金流向的变化趋势
   - 转折点识别

8. **投资建议**
   - 基于资金面的明确操作建议
   - 买入/持有/卖出的判断依据
   - 仓位管理建议
   - 关注重点和风险提示
   - 资金面对后市的指示意义与预判

【分析原则】
- 主力资金持续流入 + 股价上涨 → 强势信号，主力看好
- 主力资金流出 + 股价上涨 → 警惕信号，可能是散户接盘
- 主力资金流入 + 股价下跌 → 可能是主力低位吸筹
- 主力资金流出 + 股价下跌 → 弱势信号，主力看空
- 注意区分短期波动与趋势性变化

请给出专业、详细、有深度的资金面分析报告。记住：要基于实际数据的内容进行分析，而不是假设！
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名经验丰富的资金面分析师，擅长市场资金流向和主力行为分析，能够深入解读资金数据背后的投资逻辑。",
            },
            {"role": "user", "content": prompt},
        ]

        return self.call_api(messages, max_tokens=3000)

    # ------------------------------------------------------------------
    # 其他分析方法：情绪 / 新闻 / 研报 / 公告 / 筹码
    # 这些方法接受 ai_agents_impl 传入的 prompt_context
    # ------------------------------------------------------------------

    def sentiment_analysis(self, prompt_context: Dict[str, Any]) -> str:
        """市场情绪分析。

        prompt_context: {"stock_info": {...}, "sentiment_data": {...}}
        """

        stock_info = prompt_context.get("stock_info") or {}
        sentiment_data = prompt_context.get("sentiment_data") or {}

        # 构建带有市场情绪数据的文本片段（尽量复刻旧版 ai_agents.market_sentiment_agent 行为）
        sentiment_data_text = ""
        if sentiment_data and sentiment_data.get("data_success"):
            try:
                from ..core.market_sentiment_data_impl import MarketSentimentDataFetcher

                fetcher = MarketSentimentDataFetcher()
                sentiment_data_text = f"""

【市场情绪实际数据】
{fetcher.format_sentiment_data_for_ai(sentiment_data)}

以上数据来自统一数据访问模块（Tushare优先、Akshare备用），请结合这些客观数据进行分析。
"""
            except Exception:
                # 如果格式化失败，退回到简单 JSON 视图，避免中断分析
                try:
                    sentiment_data_text = "\n【市场情绪原始数据(JSON)】\n" + json.dumps(
                        sentiment_data, ensure_ascii=False
                    )[:6000]
                except Exception:
                    sentiment_data_text = "\n【市场情绪原始数据】\n" + str(sentiment_data)[
                        :6000
                    ]

        sentiment_prompt = f"""
作为市场情绪分析专家，请基于当前市场环境和实际数据对以下股票进行情绪分析：

股票信息：
- 股票代码：{stock_info.get('symbol', 'N/A')}
- 股票名称：{stock_info.get('name', 'N/A')}
- 行业：{stock_info.get('sector', 'N/A')}
- 细分行业：{stock_info.get('industry', 'N/A')}
{sentiment_data_text}

请从以下角度进行深度分析：

1. **ARBR情绪指标分析**
   - 详细解读AR和BR数值的含义
   - 分析当前市场人气和投机意愿
   - 判断是否存在超买超卖情况
   - 基于ARBR历史统计数据评估当前位置

2. **个股活跃度分析**
   - 换手率反映的资金活跃程度
   - 个股关注度和讨论热度
   - 与历史水平对比

3. **整体市场情绪**
   - 大盘涨跌情况对个股的影响
   - 市场成交量是放量还是缩量，并分析成因
   - 市场涨跌家数、涨跌停数量反映的整体情绪
   - 恐慌贪婪指数带来的信号

4. **重点指数指标分析**
   - 上证综指、深证成指、上证50、中证500、中小板指、创业板指的PE/PB、换手率、总市值表现
   - 对比历史平均水平或相互之间的差异，判断指数估值是否偏高/偏低
   - 指出指数指标对市场风险偏好和结构性机会的启示

5. **资金情绪**
   - 融资融券数据反映的看多看空情绪
   - 主力资金动向
   - 市场流动性状况

6. **情绪对股价影响**
   - 当前情绪对股价的支撑或压制作用
   - 情绪反转的可能性和信号
   - 短期情绪波动风险

7. **投资建议**
   - 基于市场情绪的操作建议
   - 情绪面的机会和风险提示

请确保分析基于实际数据，给出客观专业的市场情绪评估。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名专业的市场情绪分析师，擅长解读市场心理和投资者行为，善于利用ARBR等情绪指标进行分析。",
            },
            {"role": "user", "content": sentiment_prompt},
        ]

        analysis = self.call_api(messages, max_tokens=4000)

        # 在报告头部增加统一数据访问模块生成的关键摘要，复刻旧版行为
        if sentiment_data and sentiment_data.get("data_success"):

            def _fmt(value: Any, suffix: str = "") -> str:
                if value is None:
                    return "N/A"
                try:
                    num = float(value)
                except (TypeError, ValueError):
                    return str(value)
                if abs(num) >= 1e12:
                    text = f"{num / 1e12:.2f}万亿"
                elif abs(num) >= 1e8:
                    text = f"{num / 1e8:.2f}亿"
                else:
                    text = f"{num:,.2f}"
                return text + suffix

            def _fmt_change(value: Any, suffix: str = "") -> str:
                if value is None:
                    return "持平"
                try:
                    num = float(value)
                except (TypeError, ValueError):
                    return str(value)
                if abs(num) < 1e-4:
                    return "持平"
                arrow = "↑" if num > 0 else "↓"
                return f"{arrow}{abs(num):.2f}{suffix}"

            header_lines: list[str] = ["【数据来源（统一数据访问模块）】"]

            mv = sentiment_data.get("market_volume")
            if mv:
                latest = mv.get("latest", {})
                header_lines.append(
                    f"- 大盘成交量（近10日，来源：{mv.get('source', 'tushare')}）"
                    f"：{latest.get('trade_date', 'N/A')} 成交额 {_fmt(latest.get('total_amount'), '亿元')}，"
                    f"成交量 {_fmt(latest.get('total_volume'), '亿股')}，趋势判定：{mv.get('trend', 'N/A')}"
                )

            metrics_root = (
                sentiment_data.get("index_daily_metrics", {}).get("indices", {})
                if sentiment_data.get("index_daily_metrics")
                else {}
            )
            if metrics_root:
                focus_codes = [
                    "000001.SH",
                    "399001.SZ",
                    "000016.SH",
                    "000905.SH",
                    "399005.SZ",
                    "399006.SZ",
                ]
                summary_parts: list[str] = []
                for code in focus_codes:
                    info = metrics_root.get(code)
                    if not info:
                        continue
                    summary_parts.append(
                        f"{info.get('index_name', code)}({info.get('trade_date', 'N/A')}): "
                        f"PE {_fmt(info.get('pe'))}({_fmt_change(info.get('pe_change'))}), "
                        f"PB {_fmt(info.get('pb'))}({_fmt_change(info.get('pb_change'))}), "
                        f"换手率 {_fmt(info.get('turnover_rate'), '%')}({_fmt_change(info.get('turnover_rate_change'), '%')})"
                    )
                if summary_parts:
                    header_lines.append(
                        "- 指数估值与换手（index_dailybasic）：" + "；".join(summary_parts)
                    )

            header_lines.append(
                "- 其他情绪指标：ARBR、换手率、涨跌停、融资融券、恐慌贪婪指数等均由统一接口预先获取"
            )

            analysis = "\n".join(header_lines) + "\n\n" + analysis

        return analysis

    def news_analysis(self, prompt_context: Dict[str, Any]) -> str:
        """新闻与舆情分析。

        prompt_context: {"stock_info": {...}, "news_data": {...}}
        """

        stock_info = prompt_context.get("stock_info") or {}
        news_data = prompt_context.get("news_data") or {}
        # 构建带有新闻数据的文本片段（复刻旧版 ai_agents.news_analyst_agent 行为）
        news_text = ""
        if news_data and news_data.get("data_success"):
            try:
                from ..core.qstock_news_data_impl import QStockNewsDataFetcher

                fetcher = QStockNewsDataFetcher()
                news_text = f"""

【最新新闻数据】
{fetcher.format_news_for_ai(news_data)}

以上是通过qstock获取的实际新闻数据，请重点基于这些数据进行分析。
"""
            except Exception:
                # 如果格式化失败，退回到简单 JSON 文本，避免中断分析
                try:
                    news_text = "\n【新闻原始数据(JSON)】\n" + json.dumps(
                        news_data, ensure_ascii=False
                    )[:6000]
                except Exception:
                    news_text = "\n【新闻原始数据】\n" + str(news_data)[:6000]

        news_prompt = f"""
作为专业的新闻分析师，请基于最新的新闻对以下股票进行深度分析：

股票信息：
- 股票代码：{stock_info.get('symbol', 'N/A')}
- 股票名称：{stock_info.get('name', 'N/A')}
- 行业：{stock_info.get('sector', 'N/A')}
- 细分行业：{stock_info.get('industry', 'N/A')}
{news_text}

请从以下角度进行深度分析：

1. **新闻概要**
   - 梳理最新的重要新闻
   - 总结核心要点和关键信息
   - 按重要性排序新闻

2. **新闻性质分析**
   - 分析新闻的性质（利好/利空/中性）
   - 评估新闻的可信度和权威性
   - 识别新闻来源和传播范围

3. **影响评估**
   - 评估新闻对股价的短期影响
   - 分析新闻对公司长期发展的影响
   - 判断新闻对行业的影响范围

4. **热点识别**
   - 识别市场关注的热点和焦点
   - 分析该股票在市场中的关注度
   - 评估舆论导向和市场情绪

5. **重大事件识别**
   - 识别可能影响股价的重大事件
   - 评估事件的紧迫性和重要性
   - 预判后续可能的发展和连锁反应

6. **市场反应预判**
   - 预测市场对新闻的可能反应
   - 判断是否存在预期差
   - 识别可能的交易机会窗口

7. **风险提示**
   - 识别新闻中的风险信号
   - 评估潜在的负面影响
   - 提示需要警惕的风险点

8. **投资建议**
   - 基于新闻的操作建议
   - 关键时间节点和观察点
   - 需要持续关注的事项

请确保分析客观、专业，重点关注对投资决策有实质性影响的内容。
如果某些新闻的重要性较低，可以简要提及或略过。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名专业的新闻分析师，擅长解读新闻事件、舆情分析，评估新闻对股价的影响。你具有敏锐的洞察力和丰富的市场经验。",
            },
            {"role": "user", "content": news_prompt},
        ]

        return self.call_api(messages, max_tokens=4000)

    def research_report_analysis(self, prompt_context: Dict[str, Any]) -> str:
        """机构研报分析。

        prompt_context: {"stock_info": {...}, "research_data": {...}}
        """

        stock_info = prompt_context.get("stock_info") or {}
        research_data = prompt_context.get("research_data") or {}

        # 构建研报数据文本（包含内容和内容分析），复刻旧版 ai_agents.research_report_analyst_agent 行为
        research_text = ""
        content_analysis_text = ""

        if research_data and research_data.get("data_success"):
            try:
                items = (
                    research_data.get("research_reports", [])
                    or research_data.get("items", [])
                    or research_data.get("reports", [])
                )
                top_items = items[:8]
                lines: list[str] = []
                for idx, item in enumerate(top_items, 1):
                    title = str(
                        item.get("研报标题")
                        or item.get("title")
                        or item.get("名称")
                        or ""
                    )
                    rating = str(item.get("评级") or item.get("rating") or "")
                    tp = str(item.get("目标价") or item.get("target_price") or "")
                    org = str(
                        item.get("机构名称")
                        or item.get("org")
                        or item.get("机构")
                        or ""
                    )
                    date = str(
                        item.get("日期")
                        or item.get("date")
                        or item.get("发布日期")
                        or ""
                    )
                    content_summary = str(
                        item.get("内容摘要") or item.get("content_summary") or ""
                    )

                    line = f"{idx}. [{date}] {org} | {title} | 评级: {rating} | 目标价: {tp}"
                    if content_summary:
                        line += f"\n   内容摘要: {content_summary[:200]}..."
                    lines.append(line)
                research_text = "\n".join(lines)

                # 添加内容分析结果
                content_analysis = research_data.get("content_analysis", {})
                if content_analysis and content_analysis.get("has_content"):
                    sentiment = content_analysis.get("sentiment_analysis", {})
                    content_analysis_text = f"""
【研报内容分析】
- 包含内容的研报数量: {content_analysis.get('total_reports_with_content', 0)}
- 总字符数: {content_analysis.get('total_length', 0)}
- 平均字符数: {content_analysis.get('avg_length', 0)}
- 关键词: {', '.join(content_analysis.get('key_topics', [])[:5])}
- 情感倾向: {sentiment.get('sentiment', 'N/A')} (得分: {sentiment.get('sentiment_score', 0)})
- 正面信号: {sentiment.get('positive_signals', 0)}, 负面信号: {sentiment.get('negative_signals', 0)}
"""
            except Exception:
                research_text = ""

        prompt = f"""
你是一名机构研报分析师，请基于研报内容与基本信息给出专业解读：

股票：{stock_info.get('name','N/A')} ({stock_info.get('symbol','N/A')})
行业：{stock_info.get('sector','N/A')} / {stock_info.get('industry','N/A')}

【最新机构研报摘要（过去6个月）】
{research_text or '暂无有效研报数据，需基于基本信息与市场共识进行分析。'}
{content_analysis_text}

请基于以上研报内容和内容分析结果，完成：
1) 评级与目标价的分布与变化（一致/分歧点）
2) **研报核心观点分析** ⭐ 重点：基于研报内容提取的核心观点，分析共性与差异，证据链是否充分
3) **内容情感倾向解读**：结合内容分析的情感得分，评估机构整体态度
4) 对基本面与估值的影响逻辑（短/中期）
5) 触发条件与风险提示（从研报内容中提取）
6) 操作建议（基于研报内容和信号的可执行建议）

注意：要充分结合研报的实际内容进行分析，而不是仅依赖评级和目标价。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名专业的卖方研报分析师，善于聚合多家机构观点形成可执行结论。",
            },
            {"role": "user", "content": prompt},
        ]

        return self.call_api(messages, max_tokens=4000)

    def announcement_analysis(self, prompt_context: Dict[str, Any]) -> str:
        """公司公告分析。

        prompt_context: {"stock_info": {...}, "announcement_data": {...}}
        """

        stock_info = prompt_context.get("stock_info") or {}
        announcement_data = prompt_context.get("announcement_data") or {}
        # 复刻旧版 announcement_analyst_agent：先在 Python 中格式化公告与 PDF 文本
        ann_text = ""
        ann_count = 0
        date_range_str = "N/A"
        pdf_section = ""
        url_section = ""

        if isinstance(announcement_data, dict) and announcement_data.get("data_success"):
            try:
                announcements = announcement_data.get("announcements", [])
                ann_count = len(announcements)

                # 时间范围
                if announcement_data.get("date_range"):
                    dr = announcement_data["date_range"]
                    try:
                        date_range_str = f"{dr['start']} ~ {dr['end']}"
                    except Exception:
                        date_range_str = "N/A"

                # 详细格式化前15条公告
                if announcements:
                    lines: list[str] = []
                    url_lines: list[str] = []
                    for idx, ann in enumerate(announcements[:15], 1):
                        date = ann.get("日期", "N/A")
                        title = ann.get("公告标题", "N/A")
                        ann_type = ann.get("公告类型", "N/A")
                        summary = ann.get("公告摘要", "")
                        link = ann.get("download_url") or ann.get("pdf_url")
                        origin = (
                            ann.get("原始数据", {})
                            if isinstance(ann.get("原始数据"), dict)
                            else {}
                        )
                        raw_url = (
                            ann.get("download_url")
                            or ann.get("detail_url")
                            or origin.get("url")
                            or origin.get("file_url")
                            or origin.get("adjunct_url")
                        )

                        line = f"{idx}. [{date}] {title}"
                        if ann_type and ann_type != "N/A":
                            line += f" (类型: {ann_type})"
                        if summary:
                            suffix = "..." if len(summary) > 100 else ""
                            line += f"\n   摘要: {summary[:100]}{suffix}"
                        if link and link != "N/A":
                            line += f"\n   PDF下载: {link}"
                        if raw_url and raw_url != "N/A":
                            url_lines.append(f"{idx}. {raw_url}")

                        lines.append(line)

                    ann_text = "\n\n".join(lines)
                    if url_lines:
                        url_section = "\n".join(url_lines)

                # PDF 文本分析部分
                pdf_analysis = announcement_data.get("pdf_analysis", []) or []
                if pdf_analysis:
                    pdf_lines: list[str] = []
                    for idx, item in enumerate(pdf_analysis, 1):
                        excerpt = item.get("text") or "未能解析PDF内容"
                        if excerpt and len(excerpt) > 500:
                            excerpt = excerpt[:500] + "..."
                        pdf_lines.append(
                            f"{idx}. [{item.get('date', 'N/A')}] {item.get('title', 'N/A')}\n"
                            f"   PDF链接: {item.get('pdf_url', 'N/A')}\n"
                            f"   PDF内容摘录: {excerpt}"
                        )
                    pdf_section = "\n".join(pdf_lines)
            except Exception:
                ann_text = ""
                pdf_section = ""

        # 构建分析提示词
        if ann_text:
            prompt = f"""
你是一名资深的上市公司公告分析专家，精通解读各类公告对股价的影响。

【股票信息】
股票：{stock_info.get('name','N/A')} ({stock_info.get('symbol','N/A')})
当前价格：{stock_info.get('current_price','N/A')}

【公告数据】
时间范围：{date_range_str}
公告数量：{ann_count} 条
数据来源：{announcement_data.get('source', 'N/A') if isinstance(announcement_data, dict) else 'N/A'}

【公告原始链接列表】
{url_section or '暂无可用URL，请检查统一数据接口输出。'}

【详细公告列表】
{ann_text}

【PDF公告原文（统一数据接口自动下载）】
{pdf_section if pdf_section else '暂无有效PDF文本，若需请自行下载公告查看原文。'}

请你作为专业公告分析师，针对以上实际公告进行深度分析：

## 一、公告整体评估
1. 公告活跃度与信息披露质量
2. 公告类型分布与重点关注方向

## 二、重大事项识别 ⭐核心
针对每条重要公告分析：
- 事项性质（利好/利空/中性）及影响程度
- 对业绩、估值、市场预期的具体影响
- 时效性（短期1-3月/中期3-12月/长期1年+）

## 三、风险与机会
- 潜在风险：业绩风险、股权风险、合规风险、经营风险
- 投资机会：业绩改善、重大利好、战略转型、地位提升

## 四、市场反应预判
- 公告发布后的可能市场反应（结合PDF原文核心内容）
- 是否已被充分消化
- 是否存在预期差

## 五、投资建议
- 短期操作建议（买入/持有/减仓/回避）
- 关键跟踪事项与触发条件
- 风险提示与止损建议

请基于实际公告内容给出专业、详细的分析。
"""
        else:
            error_msg = (
                announcement_data.get("error", "数据获取失败")
                if isinstance(announcement_data, dict)
                else "数据获取失败"
            )
            prompt = f"""
你是一名上市公司公告分析专家。

股票：{stock_info.get('name','N/A')} ({stock_info.get('symbol','N/A')})

⚠️ 当前未获取到该股票最近30天的公告数据（{error_msg}）

请提供：
1. 上市公司信息披露的重要性与投资价值
2. 投资者应关注的公告类型（业绩预告、重大合同、股权变动等）
3. 如何从公告中识别投资机会和风险
4. 公告分析的方法论与注意事项
5. 建议通过官方渠道（交易所网站）查阅公告

注意：因缺少实际公告数据，请提供方法论指导，不做具体投资建议。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名专业的公告解读分析师，擅长从公告中抽取关键信息、识别重大事项并量化影响。",
            },
            {"role": "user", "content": prompt},
        ]

        return self.call_api(messages, max_tokens=4000)

    def chip_analysis(self, prompt_context: Dict[str, Any]) -> str:
        """筹码结构与持股分布分析。

        prompt_context: {"stock_info": {...}, "chip_data": {...}}
        """

        stock_info = prompt_context.get("stock_info") or {}
        chip_data = prompt_context.get("chip_data") or {}

        # 复刻旧版 chip_analyst_agent：先在 Python 中构建筹码要点文本
        chip_text = ""
        if chip_data and isinstance(chip_data, dict) and chip_data.get("data_success"):
            try:
                summary = chip_data.get("summary", {})
                dist = chip_data.get("distribution", {})

                # 优先使用 summary（新结构），否则兼容 distribution
                if summary:
                    focus: list[str] = []
                    if summary.get("筹码集中度"):
                        focus.append(f"筹码集中度: {summary.get('筹码集中度')}")
                    if summary.get("加权平均成本"):
                        focus.append(f"加权平均成本: {summary.get('加权平均成本')}")
                    if summary.get("成本区间"):
                        focus.append(f"成本区间: {summary.get('成本区间')}")
                    if summary.get("50%成本（中位）"):
                        focus.append(f"中位成本: {summary.get('50%成本（中位）')}")
                    if summary.get("5%成本") and summary.get("95%成本"):
                        focus.append(
                            f"成本范围: {summary.get('5%成本')} ~ {summary.get('95%成本')}"
                        )
                    if summary.get("历史最低") and summary.get("历史最高"):
                        focus.append(
                            f"历史价格范围: {summary.get('历史最低')} ~ {summary.get('历史最高')}"
                        )

                    chip_text = "\n".join(focus) if focus else ""
                elif dist:
                    focus = [
                        f"集中度: {dist.get('concentration','N/A')}",
                        f"主力控盘: {dist.get('main_control','N/A')}",
                        f"成本区间: {dist.get('cost_range','N/A')}",
                    ]
                    chip_text = "\n".join(focus)

                # 30 天筹码变化分析
                change_analysis = chip_data.get("change_analysis") or summary.get(
                    "30天变化分析"
                )
                if isinstance(change_analysis, dict) and change_analysis:
                    chip_text += "\n\n【过去30天筹码分布变化分析】"
                    chip_text += (
                        f"\n分析期间: {change_analysis.get('period', 'N/A')} "
                        f"({change_analysis.get('days_count', 0)}个交易日)"
                    )

                    main_force = change_analysis.get("main_force_behavior", {})
                    if isinstance(main_force, dict) and main_force:
                        chip_text += (
                            f"\n\n主力资金行为: {main_force.get('judgment', 'N/A')} "
                            f"(置信度: {main_force.get('confidence', 'N/A')})"
                        )
                        if main_force.get("description"):
                            chip_text += f"\n{main_force.get('description')}"

                    peak_analysis = change_analysis.get("chip_peak_analysis", {})
                    if isinstance(peak_analysis, dict) and peak_analysis:
                        chip_text += (
                            f"\n\n筹码峰移动: {peak_analysis.get('peak_direction', 'N/A')} "
                            f"({peak_analysis.get('peak_speed', 'N/A')})"
                        )

                    cost_changes = change_analysis.get("cost_changes", {})
                    if isinstance(cost_changes, dict) and "weight_avg" in cost_changes:
                        avg_change = cost_changes["weight_avg"]
                        try:
                            chip_text += (
                                f"\n加权平均成本变化: {avg_change['earliest']:.2f} → {avg_change['latest']:.2f} "
                                f"({avg_change['change']:+.2f}, {avg_change['change_pct']:+.2f}%)"
                            )
                        except Exception:
                            pass

                    conc_changes = change_analysis.get("concentration_changes", {})
                    if isinstance(conc_changes, dict) and conc_changes:
                        chip_text += (
                            f"\n筹码集中度变化: {conc_changes.get('earliest_level', 'N/A')} "
                            f"→ {conc_changes.get('latest_level', 'N/A')} "
                            f"({conc_changes.get('trend', 'N/A')})"
                        )

                # 数据来源信息
                if chip_data.get("cyq_perf") or chip_data.get("cyq_chips"):
                    source_info: list[str] = []
                    if isinstance(chip_data.get("cyq_perf"), dict):
                        source_info.append(
                            f"cyq_perf数据: {chip_data['cyq_perf'].get('count', 0)}期"
                        )
                    if isinstance(chip_data.get("cyq_chips"), dict):
                        source_info.append(
                            f"cyq_chips数据: {chip_data['cyq_chips'].get('count', 0)}个数据点"
                        )
                    if source_info:
                        chip_text += "\n\n数据来源: " + " | ".join(source_info)
            except Exception:
                chip_text = ""

        prompt = f"""
你是一名筹码结构分析师，请结合筹码与量价关系给出判断：

股票：{stock_info.get('name','N/A')} ({stock_info.get('symbol','N/A')})
当前价格：{stock_info.get('current_price', 'N/A')}

【筹码要点】
{chip_text or '暂无筹码分布数据，请结合量价与换手的统计特征进行推断。'}

请完成：
1) **筹码集中度与主力控盘评估**
   - 评估当前筹码集中程度
   - 判断主力控盘情况
   - 分析主力操作意图

2) **过去30天筹码分布变化分析** ⭐ 重点
   - 分析筹码峰的移动方向和速度
   - 根据筹码峰变化判断主力资金行为：
     * **收集低价筹码**：低位成本稳定、集中度提升、平均成本下降
     * **获利出逃**：高位成本快速上升、筹码峰上移、集中度下降
     * **洗盘整理**：低位成本稳定、中位成本上移、震荡整理
     * **派发阶段**：高位出现新筹码峰、低位峰消失
   - 评估主力资金的吸筹/出货强度
   - 识别筹码迁移的关键转折点

3) **成本区间与潜在支撑/压力带**
   - 识别关键成本区间（5%、15%、50%、85%、95%成本位）
   - 确定支撑位和压力位
   - 评估价格运行空间
   - 分析成本区间的变化趋势

4) **换手与量价背离信号**
   - 分析换手率特征
   - 识别量价背离
   - 判断筹码转移方向
   - 结合筹码变化验证主力行为

5) **短/中期可能的筹码迁移路径**
   - 预测筹码流动方向
   - 评估价格走势可能性
   - 识别关键转折点
   - 预判主力下一步操作

6) **操作建议（介入/持有/减仓的触发条件与位置）**
   - 基于筹码分析和主力行为判断，给出明确的买卖建议
   - 设置触发条件
   - 确定关键价位
   - 提供仓位管理建议

**分析原则**：
- 筹码峰上移 + 高位成本增加 → 警惕获利出逃
- 筹码峰下移 + 低位成本稳定 → 可能是收集筹码
- 集中度提升 + 低位密集 → 主力可能建仓
- 集中度下降 + 高位密集 → 主力可能派发
- 结合价格、成交量、换手率综合判断
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名专业的筹码结构分析师，擅长结合量价与换手识别关键位置。",
            },
            {"role": "user", "content": prompt},
        ]

        return self.call_api(messages, max_tokens=3500)

    def comprehensive_discussion(
        self,
        technical_report: str,
        fundamental_report: str,
        fund_flow_report: str,
        stock_info: Dict[str, Any],
    ) -> str:
        """多维度综合讨论。"""

        prompt = f"""
现在进行一场投资决策会议，请你作为首席分析师，综合以下三份报告：

【股票基本信息】
- 代码：{stock_info.get('symbol', 'N/A')}
- 名称：{stock_info.get('name', 'N/A')}
- 当前价格：{stock_info.get('current_price', 'N/A')}

【技术面报告】
{technical_report}

【基本面报告】
{fundamental_report}

【资金面报告】
{fund_flow_report}

请讨论：
1. 三个维度的一致与分歧；
2. 不同结论在当前市场环境下的权重；
3. 主要机会与风险；
4. 在短期 / 中期 / 长期的不同操作思路。
最后请给出一个偏向性的总体观点（看多/中性/看空），但暂不给出具体买卖价位。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名首席投资分析师，擅长综合多维信息形成统一观点。",
            },
            {"role": "user", "content": prompt},
        ]
        return self.call_api(messages, max_tokens=4000)

    def final_decision(
        self,
        comprehensive_discussion: str,
        stock_info: Dict[str, Any],
        indicators: Dict[str, Any],
    ) -> Dict[str, Any]:
        """最终投资决策，返回 JSON 结构。"""

        prompt = f"""
基于以下综合讨论内容，为该股票给出最终投资决策：

【股票信息】
- 代码：{stock_info.get('symbol', 'N/A')}
- 名称：{stock_info.get('name', 'N/A')}
- 当前价格：{stock_info.get('current_price', 'N/A')}

【综合讨论纪要】
{comprehensive_discussion}

【关键技术位】
- MA20：{indicators.get('ma20', 'N/A')}
- 布林带上轨：{indicators.get('bb_upper', 'N/A')}
- 布林带下轨：{indicators.get('bb_lower', 'N/A')}

请以 JSON 格式输出最终决策，字段包含：
- rating: "买入"/"持有"/"卖出"
- target_price: 目标价位（数字或区间描述）
- operation_advice: 操作建议
- entry_range: 建议买入区间
- take_profit: 止盈价位
- stop_loss: 止损价位
- holding_period: 建议持有周期
- position_size: 仓位建议（如 3 成、5 成等）
- risk_warning: 主要风险提示
- confidence_level: 1-10 分的信心度

只输出 JSON，不要附加解释文本。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一名专业投资决策专家，需要给出清晰可执行的决策 JSON。",
            },
            {"role": "user", "content": prompt},
        ]
        raw = self.call_api(messages, temperature=0.3, max_tokens=4000)

        try:
            import re

            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if not m:
                return {"decision_text": raw}
            return json.loads(m.group())
        except Exception:
            return {"decision_text": raw}
