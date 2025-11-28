from deepseek_client import DeepSeekClient
from typing import Dict, Any
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from debug_logger import debug_logger
from datetime import datetime

class StockAnalysisAgents:
    """股票分析AI智能体集合"""
    
    def __init__(self, model="deepseek-chat"):
        self.model = model
        self.deepseek_client = DeepSeekClient(model=model)
        
    def technical_analyst_agent(self, stock_info: Dict, stock_data: Any, indicators: Dict) -> Dict[str, Any]:
        """技术面分析智能体"""
        print("🔍 技术分析师正在分析中...")
        time.sleep(1)  # 模拟分析时间
        
        analysis = self.deepseek_client.technical_analysis(stock_info, stock_data, indicators)
        
        return {
            "agent_name": "技术分析师",
            "agent_role": "负责技术指标分析、图表形态识别、趋势判断",
            "analysis": analysis,
            "focus_areas": ["技术指标", "趋势分析", "支撑阻力", "交易信号"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def fundamental_analyst_agent(self, stock_info: Dict, financial_data: Dict = None, quarterly_data: Dict = None) -> Dict[str, Any]:
        """基本面分析智能体"""
        print("📊 基本面分析师正在分析中...")
        
        # 类型检查和调试日志
        if financial_data is not None:
            financial_data_type = type(financial_data).__name__
            debug_logger.debug("fundamental_analyst_agent - financial_data类型",
                             type=financial_data_type,
                             is_dict=isinstance(financial_data, dict))
            
            # 如果不是字典，记录警告
            if not isinstance(financial_data, dict):
                debug_logger.warning("financial_data不是字典类型",
                                   actual_type=financial_data_type,
                                   expected_type="dict")
                # 如果financial_data是DataFrame或其他类型，转换为None避免后续错误
                financial_data = None
        
        if quarterly_data is not None:
            quarterly_data_type = type(quarterly_data).__name__
            debug_logger.debug("fundamental_analyst_agent - quarterly_data类型",
                             type=quarterly_data_type,
                             is_dict=isinstance(quarterly_data, dict))
            
            # 如果不是字典，记录警告
            if not isinstance(quarterly_data, dict):
                debug_logger.warning("quarterly_data不是字典类型",
                                   actual_type=quarterly_data_type,
                                   expected_type="dict")
                quarterly_data = None
        
        # 如果有季报数据，显示数据来源
        if quarterly_data is not None and isinstance(quarterly_data, dict) and quarterly_data.get('data_success'):
            income_count = quarterly_data.get('income_statement', {}).get('periods', 0) if quarterly_data.get('income_statement') else 0
            balance_count = quarterly_data.get('balance_sheet', {}).get('periods', 0) if quarterly_data.get('balance_sheet') else 0
            cash_flow_count = quarterly_data.get('cash_flow', {}).get('periods', 0) if quarterly_data.get('cash_flow') else 0
            print(f"   ✓ 已获取季报数据：利润表{income_count}期，资产负债表{balance_count}期，现金流量表{cash_flow_count}期")
        else:
            print("   ⚠ 未获取到季报数据，将基于基本财务数据分析")
        
        time.sleep(1)
        
        analysis = self.deepseek_client.fundamental_analysis(stock_info, financial_data, quarterly_data)
        
        return {
            "agent_name": "基本面分析师", 
            "agent_role": "负责公司财务分析、行业研究、估值分析",
            "analysis": analysis,
            "focus_areas": ["财务指标", "行业分析", "公司价值", "成长性", "季报趋势"],
            "quarterly_data": quarterly_data,  # 保存季报数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def fund_flow_analyst_agent(self, stock_info: Dict, indicators: Dict, fund_flow_data: Dict = None) -> Dict[str, Any]:
        """资金面分析智能体"""
        print("💰 资金面分析师正在分析中...")
        
        # 如果有资金流向数据，显示数据来源
        if fund_flow_data and fund_flow_data.get('data_success'):
            print("   ✓ 已获取资金流向数据（akshare数据源）")
            if fund_flow_data.get('margin_trading_history'):
                print("   ✓ 已获取融资融券历史数据（统一数据访问模块）")
        else:
            print("   ⚠ 未获取到资金流向数据，将基于技术指标分析")
        
        time.sleep(1)
        
        analysis = self.deepseek_client.fund_flow_analysis(stock_info, indicators, fund_flow_data)
        
        return {
            "agent_name": "资金面分析师",
            "agent_role": "负责资金流向分析、主力行为研究、市场情绪判断", 
            "analysis": analysis,
            "focus_areas": ["资金流向", "主力动向", "市场情绪", "流动性"],
            "fund_flow_data": fund_flow_data,  # 保存资金流向数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def risk_management_agent(self, stock_info: Dict, indicators: Dict, risk_data: Dict = None, fund_flow_data: Dict = None) -> Dict[str, Any]:
        """风险管理智能体（增强版）"""
        print("⚠️ 风险管理师正在评估中...")
        
        # 如果有风险数据，显示数据来源
        if risk_data and risk_data.get('data_success'):
            print("   ✓ 已获取统一数据接口风险数据（Tushare：限售解禁、股东增减持、重要公告）")
        else:
            print("   ⚠ 未获取到风险数据，将基于基本信息分析")

        if fund_flow_data and fund_flow_data.get('data_success'):
            print("   ✓ 已获取流动性参考数据（统一数据接口资金流向）")
        else:
            print("   ℹ️ 未获取到资金流向参考数据，流动性分析将基于其他指标")
        
        time.sleep(1)
        
        # 构建风险数据文本
        risk_data_text = ""
        if risk_data and risk_data.get('data_success'):
            # 使用格式化的风险数据
            from risk_data_fetcher import RiskDataFetcher
            fetcher = RiskDataFetcher()
            risk_data_text = f"""

【实际风险数据】（统一数据访问模块 / Tushare）
{fetcher.format_risk_data_for_ai(risk_data)}

以上风险数据已通过统一数据访问模块预先获取（Tushare官方接口），请基于这些实际数据进行深度风险分析。
"""
        
        liquidity_metrics = risk_data.get('liquidity_metrics') if risk_data else None
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

请从以下角度进行全面的风险评估：

1. **限售解禁风险分析** ⭐ 重点
   - 解禁时间和规模评估
   - 解禁对股价的潜在冲击
   - 解禁股东类型分析（创始人/投资机构/其他）
   - 历史解禁后股价走势参考
   - 风险等级评定和应对建议

2. **股东减持风险分析** ⭐ 重点
   - 减持频率和力度评估
   - 减持股东身份和意图分析
   - 减持对市场信心的影响
   - 是否存在连续减持或集中减持
   - 风险警示和投资建议

3. **重要事件风险分析** ⭐ 重点
   - 识别可能影响股价的重大事件
   - 事件性质判断（利好/利空/中性）
   - 事件影响的时间维度（短期/中期/长期）
   - 事件的确定性和不确定性
   - 风险提示和关注要点

4. **市场风险（系统性风险）**
   - 宏观经济环境风险
   - 市场整体走势风险
   - Beta系数反映的市场敏感度
   - 系统性风险应对策略

5. **个股风险（非系统性风险）**
   - 公司基本面风险
   - 经营管理风险
   - 竞争力风险
   - 行业地位风险

6. **流动性风险**
   - 成交量和换手率分析
   - 买卖盘深度评估
   - 流动性枯竭风险
   - 大额交易影响评估
   - 结合以上资金流向参考数据，判断主力资金动向对流动性的影响

7. **波动性风险**
   - 价格波动幅度分析
   - 52周最高最低位分析
   - RSI等技术指标的风险提示
   - 波动率对投资的影响

8. **估值风险**
   - 当前估值水平评估
   - 市场预期和估值偏差
   - 估值过高风险警示

9. **行业风险**
   - 行业周期阶段
   - 行业竞争格局
   - 行业政策风险
   - 行业技术变革风险

10. **综合风险评定**
    - 风险等级评定（低/中/高）
    - 主要风险因素排序
    - 风险暴露时间窗口
    - 风险演变趋势判断

11. **风险控制建议** ⭐ 核心
    - 仓位控制建议（具体比例）
    - 止损位设置建议（具体价位）
    - 风险规避策略（什么情况下不建议投资）
    - 风险对冲方案（如果适用）
    - 持仓时间建议
    - 重点关注指标和信号

请基于实际数据进行客观、专业、严谨的风险评估，给出可操作的风险控制建议。
如果某些风险数据缺失，也要指出数据缺失本身可能带来的风险。
"""
        
        messages = [
            {"role": "system", "content": "你是一名资深的风险管理专家，具有20年以上的风险识别和控制经验，擅长全面评估各类投资风险，特别关注限售解禁、股东减持、重要事件等可能影响股价的风险因素。你擅长从海量原始数据中提取关键信息，进行深度解析和量化评估。"},
            {"role": "user", "content": risk_prompt}
        ]
        
        analysis = self.deepseek_client.call_api(messages, max_tokens=6000)
        
        return {
            "agent_name": "风险管理师",
            "agent_role": "识别并评估多维风险，提供风险控制建议", 
            "analysis": analysis,
            "focus_areas": ["限售解禁", "股东减持", "重大事件", "系统性风险", "操作建议"],
            "risk_data": risk_data,
            "fund_flow_data": fund_flow_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def _build_liquidity_context(self, fund_flow_data: Dict[str, Any], liquidity_metrics: Dict[str, Any]) -> str:
        section_title = "\n【流动性参考数据】"
        lines = [section_title]

        core = None
        if fund_flow_data and fund_flow_data.get('data_success'):
            if isinstance(fund_flow_data, dict):
                core = fund_flow_data.get('fund_flow_data') or fund_flow_data.get('fund_flow')
            if core and isinstance(core, dict):
                records = core.get('data') or core.get('records')
            else:
                records = None
        else:
            records = None

        def parse_date(value):
            if value is None:
                return None
            candidates = ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]
            val = str(value)
            val = val.strip()
            if not val:
                return None
            for fmt in candidates:
                try:
                    return datetime.strptime(val, fmt)
                except Exception:
                    continue
            return None

        def to_float(val):
            if val is None:
                return None
            if isinstance(val, (int, float)):
                return float(val)
            try:
                text = str(val).replace(',', '').replace('%', '').strip()
                if not text:
                    return None
                return float(text)
            except Exception:
                return None

        def pick(item: Dict[str, Any], keys):
            for key in keys:
                if key in item and item[key] not in (None, ""):
                    value = to_float(item[key])
                    if value is not None:
                        return value
            return None

        fund_flow_lines = []
        parsed_records = []
        if records:
            for item in records:
                if not isinstance(item, dict):
                    continue
                dt = None
                for key in ('trade_date', '日期', 'date', 'DAY'):
                    if key in item:
                        dt = parse_date(item[key])
                        if dt:
                            break
                if dt is None:
                    continue
                parsed_records.append((dt, item))

        if parsed_records:
            parsed_records.sort(key=lambda x: x[0], reverse=True)
        
        has_fund_flow = bool(parsed_records)

        def fmt_amount(value):
            if value is None:
                return "N/A"
            if abs(value) >= 1e8:
                return f"{value / 1e8:.2f}亿元"
            if abs(value) >= 1e4:
                return f"{value / 1e4:.2f}亿元"
            return f"{value:.2f}万元"

        def fmt_ratio(value):
            if value is None:
                return "N/A"
            return f"{value:.2f}%"

        if has_fund_flow:
            latest_date, latest_item = parsed_records[0]
            recent_window = parsed_records[:5]

            main_keys = [
                '主力净流入-净额(万元)',
                '主力净流出-净额(万元)',
                'net_amount_main',
                'net_mf_amount',
                'net_amount',
            ]
            total_amount_keys = [
                '成交额',
                '成交额(万元)',
                '成交额-总额(万元)',
                'amount',
                'total_amount',
            ]
            ratio_keys = [
                '主力净流入-净占比(%)',
                '净流入占比(%)',
                'net_mf_rate',
                'net_rate',
            ]

            latest_main = pick(latest_item, main_keys)
            latest_turnover = pick(latest_item, total_amount_keys)
            latest_ratio = pick(latest_item, ratio_keys)

            window_values = [pick(item, main_keys) for _, item in recent_window]
            window_values = [v for v in window_values if v is not None]
            avg_main = sum(window_values) / len(window_values) if window_values else None
            pos_days = sum(1 for v in window_values if v and v > 0)

            min_date = parsed_records[-1][0]
            max_date = parsed_records[0][0]
            source_text = fund_flow_data.get('source') or core.get('source') or '未知'

            fund_flow_lines.append(f"资金流向数据来源：{source_text}（统一数据访问模块预先获取）")
            fund_flow_lines.append(f"覆盖区间：{min_date.strftime('%Y-%m-%d')} ~ {max_date.strftime('%Y-%m-%d')}，共 {len(parsed_records)} 个交易日")
            fund_flow_lines.append(
                f"最新交易日 {latest_date.strftime('%Y-%m-%d')}：主力净流入 {fmt_amount(latest_main)}，总成交额 {fmt_amount(latest_turnover)}，主力净流入占比 {fmt_ratio(latest_ratio)}"
            )
            if avg_main is not None:
                fund_flow_lines.append(
                    f"近5日主力净流入均值 {fmt_amount(avg_main)}，净流入天数 {pos_days}/{len(window_values) if window_values else 5}"
                )
            if latest_turnover not in (None, 0) and latest_main not in (None, 0):
                liquidity_ratio = latest_main / latest_turnover * 100
                fund_flow_lines.append(f"主力净流入占成交额比重约 {fmt_ratio(liquidity_ratio)}")

        if liquidity_metrics and liquidity_metrics.get('has_data'):
            latest_liq = liquidity_metrics.get('latest', {})
            lines.append(
                f"成交量与换手率（Tushare daily/daily_basic）：最新 {latest_liq.get('trade_date', 'N/A')} | "
                f"换手率 {fmt_ratio(latest_liq.get('turnover_rate'))} | 成交量 {fmt_amount(latest_liq.get('volume'))}股 | 成交额 {fmt_amount(latest_liq.get('amount'))}元"
            )
            if liquidity_metrics.get('volume_avg_5d') is not None:
                lines.append(
                    f"5日平均成交量 {fmt_amount(liquidity_metrics.get('volume_avg_5d'))}股，较前5日变化 {fmt_ratio(liquidity_metrics.get('volume_change_pct_prev'))}"
                )
            lines.append("近5日成交概览：")
            for rec in liquidity_metrics.get('records', [])[:5]:
                lines.append(
                    f"  - {rec.get('trade_date')} | 换手率 {fmt_ratio(rec.get('turnover_rate'))} | 成交量 {fmt_amount(rec.get('volume'))}股 | 日变动 {fmt_ratio(rec.get('volume_change_pct'))}"
                )
        else:
            lines.append("未获取到换手率/成交量数据，请结合市场成交统计评估流动性风险。")

        if fund_flow_lines:
            lines.append("")
            lines.extend(fund_flow_lines)
        elif not has_fund_flow:
            lines.append("未获取到统一数据接口资金流向数据，资金面流动性需结合其他指标判断。")

        lines.append("请结合成交量、换手率与主力资金变化，评估流动性风险及潜在压力。")
        lines.append("")

        return "\n".join(lines)
    
    def market_sentiment_agent(self, stock_info: Dict, sentiment_data: Dict = None) -> Dict[str, Any]:
        """市场情绪分析智能体"""
        print("📈 市场情绪分析师正在分析中...")
        
        # 如果有市场情绪数据，显示数据来源
        if sentiment_data and sentiment_data.get('data_success'):
            print("   ✓ 已获取市场情绪数据（ARBR、换手率、涨跌停等）")
        else:
            print("   ⚠ 未获取到详细情绪数据，将基于基本信息分析")
        
        time.sleep(1)
        
        # 构建带有市场情绪数据的prompt
        sentiment_data_text = ""
        if sentiment_data and sentiment_data.get('data_success'):
            # 使用格式化的市场情绪数据
            from market_sentiment_data import MarketSentimentDataFetcher
            fetcher = MarketSentimentDataFetcher()
            sentiment_data_text = f"""

【市场情绪实际数据】
{fetcher.format_sentiment_data_for_ai(sentiment_data)}

以上数据来自统一数据访问模块（Tushare优先、Akshare备用），请结合这些客观数据进行分析。
"""
        
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
            {"role": "system", "content": "你是一名专业的市场情绪分析师，擅长解读市场心理和投资者行为，善于利用ARBR等情绪指标进行分析。"},
            {"role": "user", "content": sentiment_prompt}
        ]
        
        analysis = self.deepseek_client.call_api(messages, max_tokens=4000)

        # 在报告头部打印本次分析使用的数据来源与关键摘要
        if sentiment_data and sentiment_data.get('data_success'):
            def fmt(value, suffix=""):
                if value is None:
                    return "N/A"
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    return str(value)
                if abs(value) >= 1e12:
                    text = f"{value / 1e12:.2f}万亿"
                elif abs(value) >= 1e8:
                    text = f"{value / 1e8:.2f}亿"
                else:
                    text = f"{value:,.2f}"
                return text + suffix

            def fmt_change(value, suffix=""):
                if value is None:
                    return "持平"
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    return str(value)
                if abs(value) < 1e-4:
                    return "持平"
                arrow = "↑" if value > 0 else "↓"
                return f"{arrow}{abs(value):.2f}{suffix}"

            header_lines = ["【数据来源（统一数据访问模块）】"]

            mv = sentiment_data.get('market_volume')
            if mv:
                latest = mv.get('latest', {})
                header_lines.append(
                    f"- 大盘成交量（近10日，来源：{mv.get('source', 'tushare')}）"
                    f"：{latest.get('trade_date', 'N/A')} 成交额 {fmt(latest.get('total_amount'), '亿元')}，"
                    f"成交量 {fmt(latest.get('total_volume'), '亿股')}，趋势判定：{mv.get('trend', 'N/A')}"
                )
            metrics = sentiment_data.get('index_daily_metrics', {}).get('indices', {}) if sentiment_data.get('index_daily_metrics') else {}
            if metrics:
                focus_codes = ['000001.SH', '399001.SZ', '000016.SH', '000905.SH', '399005.SZ', '399006.SZ']
                summary_parts = []
                for code in focus_codes:
                    info = metrics.get(code)
                    if not info:
                        continue
                    summary_parts.append(
                        f"{info.get('index_name', code)}({info.get('trade_date', 'N/A')}): "
                        f"PE {fmt(info.get('pe'))}({fmt_change(info.get('pe_change'))}), "
                        f"PB {fmt(info.get('pb'))}({fmt_change(info.get('pb_change'))}), "
                        f"换手率 {fmt(info.get('turnover_rate'), '%')}({fmt_change(info.get('turnover_rate_change'), '%')})"
                    )
                if summary_parts:
                    header_lines.append("- 指数估值与换手（index_dailybasic）：" + "；".join(summary_parts))

            header_lines.append("- 其他情绪指标：ARBR、换手率、涨跌停、融资融券、恐慌贪婪指数等均由统一接口预先获取")

            analysis = "\n".join(header_lines) + "\n\n" + analysis
        
        return {
            "agent_name": "市场情绪分析师",
            "agent_role": "负责市场情绪研究、投资者心理分析、热点追踪",
            "analysis": analysis,
            "focus_areas": ["ARBR指标", "市场情绪", "投资者心理", "资金活跃度", "恐慌贪婪指数"],
            "sentiment_data": sentiment_data,  # 保存市场情绪数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def news_analyst_agent(self, stock_info: Dict, news_data: Dict = None) -> Dict[str, Any]:
        """新闻分析智能体"""
        print("📰 新闻分析师正在分析中...")
        
        # 如果有新闻数据，显示数据来源
        if news_data and news_data.get('data_success'):
            news_count = news_data.get('news_data', {}).get('count', 0) if news_data.get('news_data') else 0
            source = news_data.get('source', 'unknown')
            print(f"   ✓ 已从 {source} 获取 {news_count} 条新闻")
        else:
            print("   ⚠ 未获取到新闻数据，将基于基本信息分析")
        
        time.sleep(1)
        
        # 构建带有新闻数据的prompt
        news_text = ""
        if news_data and news_data.get('data_success'):
            # 使用格式化的新闻数据
            from qstock_news_data import QStockNewsDataFetcher
            fetcher = QStockNewsDataFetcher()
            news_text = f"""

【最新新闻数据】
{fetcher.format_news_for_ai(news_data)}

以上是通过qstock获取的实际新闻数据，请重点基于这些数据进行分析。
"""
        
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
            {"role": "system", "content": "你是一名专业的新闻分析师，擅长解读新闻事件、舆情分析，评估新闻对股价的影响。你具有敏锐的洞察力和丰富的市场经验。"},
            {"role": "user", "content": news_prompt}
        ]
        
        analysis = self.deepseek_client.call_api(messages, max_tokens=4000)
        
        return {
            "agent_name": "新闻分析师",
            "agent_role": "负责新闻事件分析、舆情研究、重大事件影响评估",
            "analysis": analysis,
            "focus_areas": ["新闻解读", "舆情分析", "事件影响", "市场反应", "投资机会"],
            "news_data": news_data,  # 保存新闻数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def research_report_analyst_agent(self, stock_info: Dict, research_data: Dict = None) -> Dict[str, Any]:
        """机构研报分析智能体"""
        print("📑 机构研报分析师正在分析中...")

        # 构建研报数据文本（包含内容和内容分析）
        research_text = ""
        content_analysis_text = ""
        
        if research_data and research_data.get('data_success'):
            try:
                items = research_data.get('research_reports', []) or research_data.get('items', []) or research_data.get('reports', [])
                top_items = items[:8]  # 取前8条
                lines = []
                for idx, item in enumerate(top_items, 1):
                    title = str(item.get('研报标题') or item.get('title') or item.get('名称') or '')
                    rating = str(item.get('评级') or item.get('rating') or '')
                    tp = str(item.get('目标价') or item.get('target_price') or '')
                    org = str(item.get('机构名称') or item.get('org') or item.get('机构') or '')
                    date = str(item.get('日期') or item.get('date') or item.get('发布日期') or '')
                    content_summary = str(item.get('内容摘要') or item.get('content_summary') or '')
                    
                    line = f"{idx}. [{date}] {org} | {title} | 评级: {rating} | 目标价: {tp}"
                    if content_summary:
                        line += f"\n   内容摘要: {content_summary[:200]}..."  # 限制摘要长度
                    lines.append(line)
                research_text = "\n".join(lines)
                
                # 添加内容分析结果
                content_analysis = research_data.get('content_analysis', {})
                if content_analysis and content_analysis.get('has_content'):
                    sentiment = content_analysis.get('sentiment_analysis', {})
                    content_analysis_text = f"""
【研报内容分析】
- 包含内容的研报数量: {content_analysis.get('total_reports_with_content', 0)}
- 总字符数: {content_analysis.get('total_length', 0)}
- 平均字符数: {content_analysis.get('avg_length', 0)}
- 关键词: {', '.join(content_analysis.get('key_topics', [])[:5])}
- 情感倾向: {sentiment.get('sentiment', 'N/A')} (得分: {sentiment.get('sentiment_score', 0)})
- 正面信号: {sentiment.get('positive_signals', 0)}, 负面信号: {sentiment.get('negative_signals', 0)}
"""
            except Exception as e:
                import traceback
                traceback.print_exc()
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
            {"role": "system", "content": "你是一名专业的卖方研报分析师，善于聚合多家机构观点形成可执行结论。"},
            {"role": "user", "content": prompt}
        ]

        analysis = self.deepseek_client.call_api(messages, max_tokens=4000)

        return {
            "agent_name": "机构研报分析师",
            "agent_role": "聚合机构研报观点，分析评级/目标价与影响路径",
            "analysis": analysis,
            "focus_areas": ["机构评级", "目标价", "一致与分歧", "影响路径", "操作建议"],
            "research_data": research_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def announcement_analyst_agent(self, stock_info: Dict, announcement_data: Dict = None) -> Dict[str, Any]:
        """公告分析智能体 - 深度分析上市公司近30天公告"""
        print("📢 公告分析师正在分析中...")
        
        # 类型检查和调试日志
        if announcement_data is not None:
            announcement_data_type = type(announcement_data).__name__
            debug_logger.debug("announcement_analyst_agent - announcement_data类型",
                             type=announcement_data_type,
                             is_dict=isinstance(announcement_data, dict))
            
            # 如果不是字典，记录警告
            if not isinstance(announcement_data, dict):
                debug_logger.warning("announcement_data不是字典类型",
                                   actual_type=announcement_data_type,
                                   expected_type="dict")
                # 转换为None避免后续错误
                announcement_data = None
        else:
            debug_logger.debug("announcement_analyst_agent - announcement_data为None",
                             symbol=stock_info.get('symbol', 'N/A'))

        ann_text = ""
        ann_count = 0
        date_range_str = "N/A"
        pdf_section = ""
        url_section = ""
 
        # 安全的None检查和类型检查
        if announcement_data is not None and isinstance(announcement_data, dict) and announcement_data.get('data_success'):
            try:
                # 使用新的数据结构
                announcements = announcement_data.get('announcements', [])
                ann_count = len(announcements)
 
                # 获取时间范围
                if announcement_data.get('date_range'):
                    dr = announcement_data['date_range']
                    date_range_str = f"{dr['start']} ~ {dr['end']}"
 
                # 详细格式化前15条公告
                if announcements:
                    lines = []
                    url_lines = []
                    for idx, ann in enumerate(announcements[:15], 1):
                        date = ann.get('日期', 'N/A')
                        title = ann.get('公告标题', 'N/A')
                        ann_type = ann.get('公告类型', 'N/A')
                        summary = ann.get('公告摘要', '')
                        link = ann.get('download_url') or ann.get('pdf_url')
                        origin = ann.get('原始数据', {}) if isinstance(ann.get('原始数据'), dict) else {}
                        raw_url = ann.get('download_url') or ann.get('detail_url') or origin.get('url') or origin.get('file_url') or origin.get('adjunct_url')
 
                        line = f"{idx}. [{date}] {title}"
                        if ann_type and ann_type != 'N/A':
                            line += f" (类型: {ann_type})"
                        if summary:
                            line += f"\n   摘要: {summary[:100]}{'...' if len(summary) > 100 else ''}"
                        if link and link != 'N/A':
                            line += f"\n   PDF下载: {link}"
                        if raw_url and raw_url != 'N/A':
                            url_lines.append(f"{idx}. {raw_url}")
 
                        lines.append(line)
 
                    ann_text = "\n\n".join(lines)
                    print(f"   ✓ 将分析 {ann_count} 条公告 (时间: {date_range_str})")
                    if url_lines:
                        url_section = "\n".join(url_lines)
 
                pdf_analysis = announcement_data.get('pdf_analysis', []) or []
                if pdf_analysis:
                    pdf_lines = []
                    for idx, item in enumerate(pdf_analysis, 1):
                        excerpt = item.get('text') or '未能解析PDF内容'
                        if excerpt and len(excerpt) > 500:
                            excerpt = excerpt[:500] + '...'
                        pdf_lines.append(
                            f"{idx}. [{item.get('date', 'N/A')}] {item.get('title', 'N/A')}\n"
                            f"   PDF链接: {item.get('pdf_url', 'N/A')}\n"
                            f"   PDF内容摘录: {excerpt}"
                        )
                    pdf_section = "\n".join(pdf_lines)
                    print(f"   ✓ 已获取 {len(pdf_analysis)} 份公告PDF文本用于深度分析")
            except Exception as e:
                print(f"   ⚠️ 格式化公告数据出错: {e}")
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
数据来源：{announcement_data.get('source', 'N/A') if announcement_data and isinstance(announcement_data, dict) else 'N/A'}

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
            prompt = f"""
你是一名上市公司公告分析专家。

股票：{stock_info.get('name','N/A')} ({stock_info.get('symbol','N/A')})

⚠️ 当前未获取到该股票最近30天的公告数据（{announcement_data.get('error', '未知原因') if announcement_data and isinstance(announcement_data, dict) else '数据获取失败'}）

请提供：
1. 上市公司信息披露的重要性与投资价值
2. 投资者应关注的公告类型（业绩预告、重大合同、股权变动等）
3. 如何从公告中识别投资机会和风险
4. 公告分析的方法论与注意事项
5. 建议通过官方渠道（交易所网站）查阅公告

注意：因缺少实际公告数据，请提供方法论指导，不做具体投资建议。
"""

        messages = [
            {"role": "system", "content": "你是一名专业的公告解读分析师，擅长从公告中抽取关键信息、识别重大事项并量化影响。"},
            {"role": "user", "content": prompt}
        ]

        analysis = self.deepseek_client.call_api(messages, max_tokens=4000)

        return {
            "agent_name": "公告分析师",
            "agent_role": "深度解析上市公司公告，识别重大事项，评估影响并给出操作建议",
            "analysis": analysis,
            "focus_areas": ["重大事项识别", "影响评估", "风险机会", "市场反应", "操作建议"],
            "announcement_data": announcement_data,
            "announcement_count": ann_count,
            "date_range": date_range_str,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def chip_analyst_agent(self, stock_info: Dict, chip_data: Dict = None) -> Dict[str, Any]:
        """筹码分析智能体（仅A股）"""
        print("🎯 筹码分析师正在分析中...")

        chip_text = ""
        if chip_data and chip_data.get('data_success'):
            try:
                # 使用新的数据结构（summary或distribution）
                summary = chip_data.get('summary', {})
                dist = chip_data.get('distribution', {})
                
                # 优先使用summary（新结构），否则使用distribution（旧结构兼容）
                if summary:
                    focus = []
                    if summary.get('筹码集中度'):
                        focus.append(f"筹码集中度: {summary.get('筹码集中度')}")
                    if summary.get('加权平均成本'):
                        focus.append(f"加权平均成本: {summary.get('加权平均成本')}")
                    if summary.get('成本区间'):
                        focus.append(f"成本区间: {summary.get('成本区间')}")
                    if summary.get('50%成本（中位）'):
                        focus.append(f"中位成本: {summary.get('50%成本（中位）')}")
                    if summary.get('5%成本') and summary.get('95%成本'):
                        focus.append(f"成本范围: {summary.get('5%成本')} ~ {summary.get('95%成本')}")
                    if summary.get('历史最低') and summary.get('历史最高'):
                        focus.append(f"历史价格范围: {summary.get('历史最低')} ~ {summary.get('历史最高')}")
                    
                    chip_text = "\n".join(focus) if focus else ""
                elif dist:
                    # 兼容旧数据结构
                    focus = [
                        f"集中度: {dist.get('concentration','N/A')}",
                        f"主力控盘: {dist.get('main_control','N/A')}",
                        f"成本区间: {dist.get('cost_range','N/A')}",
                    ]
                    chip_text = "\n".join(focus)
                
                # 添加30天筹码变化分析
                change_analysis = chip_data.get('change_analysis') or summary.get('30天变化分析')
                if change_analysis:
                    chip_text += "\n\n【过去30天筹码分布变化分析】"
                    chip_text += f"\n分析期间: {change_analysis.get('period', 'N/A')} ({change_analysis.get('days_count', 0)}个交易日)"
                    
                    # 主力行为判断
                    main_force = change_analysis.get('main_force_behavior', {})
                    if main_force:
                        chip_text += f"\n\n主力资金行为: {main_force.get('judgment', 'N/A')} (置信度: {main_force.get('confidence', 'N/A')})"
                        if main_force.get('description'):
                            chip_text += f"\n{main_force.get('description')}"
                    
                    # 筹码峰变化
                    peak_analysis = change_analysis.get('chip_peak_analysis', {})
                    if peak_analysis:
                        chip_text += f"\n\n筹码峰移动: {peak_analysis.get('peak_direction', 'N/A')} ({peak_analysis.get('peak_speed', 'N/A')})"
                    
                    # 成本变化摘要
                    cost_changes = change_analysis.get('cost_changes', {})
                    if 'weight_avg' in cost_changes:
                        avg_change = cost_changes['weight_avg']
                        chip_text += f"\n加权平均成本变化: {avg_change['earliest']:.2f} → {avg_change['latest']:.2f} "
                        chip_text += f"({avg_change['change']:+.2f}, {avg_change['change_pct']:+.2f}%)"
                    
                    # 集中度变化
                    conc_changes = change_analysis.get('concentration_changes', {})
                    if conc_changes:
                        chip_text += f"\n筹码集中度变化: {conc_changes.get('earliest_level', 'N/A')} → {conc_changes.get('latest_level', 'N/A')} "
                        chip_text += f"({conc_changes.get('trend', 'N/A')})"
                
                # 添加数据来源信息
                if chip_data.get('cyq_perf') or chip_data.get('cyq_chips'):
                    source_info = []
                    if chip_data.get('cyq_perf'):
                        source_info.append(f"cyq_perf数据: {chip_data['cyq_perf'].get('count', 0)}期")
                    if chip_data.get('cyq_chips'):
                        source_info.append(f"cyq_chips数据: {chip_data['cyq_chips'].get('count', 0)}个数据点")
                    if source_info:
                        chip_text += "\n\n数据来源: " + " | ".join(source_info)
                        
            except Exception as e:
                debug_logger.warning(f"格式化筹码数据失败", error=e, symbol=stock_info.get('symbol'))
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
            {"role": "system", "content": "你是一名专业的筹码结构分析师，擅长结合量价与换手识别关键位置。"},
            {"role": "user", "content": prompt}
        ]

        analysis = self.deepseek_client.call_api(messages, max_tokens=3500)

        return {
            "agent_name": "筹码分析师",
            "agent_role": "分析筹码集中度、成本区间、主力控盘与关键位置",
            "analysis": analysis,
            "focus_areas": ["筹码集中度", "主力控盘", "成本区间", "关键位置", "操作建议"],
            "chip_data": chip_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def run_multi_agent_analysis(self, stock_info: Dict, stock_data: Any, indicators: Dict, 
                                 financial_data: Dict = None, fund_flow_data: Dict = None, 
                                 sentiment_data: Dict = None, news_data: Dict = None,
                                 quarterly_data: Dict = None, risk_data: Dict = None,
                                 research_data: Dict = None, announcement_data: Dict = None,
                                 chip_data: Dict = None,
                                 enabled_analysts: Dict = None) -> Dict[str, Any]:
        """运行多智能体分析（并行执行）
        
        Args:
            enabled_analysts: 字典，指定哪些分析师参与分析
                例如: {'technical': True, 'fundamental': True, ...}
                如果为None，则运行所有分析师
        
        Returns:
            包含所有分析结果和性能统计的字典
        """
        # 记录总体开始时间
        total_start_time = time.time()
        
        debug_logger.step(1, "开始多智能体分析", 
                         symbol=stock_info.get('symbol', 'N/A'),
                         stock_name=stock_info.get('name', 'N/A'))
        
        # 记录输入数据信息
        debug_logger.data_info("stock_info", stock_info)
        debug_logger.data_info("indicators", indicators)
        debug_logger.data_info("financial_data", financial_data)
        debug_logger.data_info("announcement_data", announcement_data)
        
        # 如果未指定，默认所有分析师都参与
        if enabled_analysts is None:
            enabled_analysts = {
                'technical': True,
                'fundamental': True,
                'fund_flow': True,
                'risk': True,
                'sentiment': True,
                'news': True,
                'research': True,
                'announcement': True,
                'chip': True
            }
        
        print("🚀 启动多智能体股票分析系统（并行模式）...")
        print("=" * 60)
        
        # 显示参与分析的分析师
        active_analysts = [name for name, enabled in enabled_analysts.items() if enabled]
        print(f"📋 参与分析的分析师: {', '.join(active_analysts)} (共 {len(active_analysts)} 位)")
        print("⚡ 分析模式: 并行执行（多线程）")
        print("=" * 60)
        
        # 准备分析任务
        analysis_tasks = []
        
        # 定义分析任务函数（带计时）
        def run_analyst_with_timing(analyst_name, analyst_func, *args):
            """运行单个分析师并记录用时"""
            start_time = time.time()
            try:
                result = analyst_func(*args)
                elapsed_time = time.time() - start_time
                result['elapsed_time'] = elapsed_time
                return analyst_name, result, elapsed_time, None
            except Exception as e:
                elapsed_time = time.time() - start_time
                print(f"❌ {analyst_name} 分析失败: {str(e)}")
                return analyst_name, None, elapsed_time, str(e)
        
        # 技术面分析
        if enabled_analysts.get('technical', True):
            analysis_tasks.append(('technical', self.technical_analyst_agent, stock_info, stock_data, indicators))
        
        # 基本面分析
        if enabled_analysts.get('fundamental', True):
            analysis_tasks.append(('fundamental', self.fundamental_analyst_agent, stock_info, financial_data, quarterly_data))
        
        # 资金面分析
        if enabled_analysts.get('fund_flow', True):
            analysis_tasks.append(('fund_flow', self.fund_flow_analyst_agent, stock_info, indicators, fund_flow_data))
        
        # 风险管理分析
        if enabled_analysts.get('risk', True):
            analysis_tasks.append(('risk_management', self.risk_management_agent, stock_info, indicators, risk_data, fund_flow_data))
        
        # 市场情绪分析
        if enabled_analysts.get('sentiment', False):
            analysis_tasks.append(('market_sentiment', self.market_sentiment_agent, stock_info, sentiment_data))
        
        # 新闻分析
        if enabled_analysts.get('news', False):
            analysis_tasks.append(('news', self.news_analyst_agent, stock_info, news_data))

        # 机构研报分析
        if enabled_analysts.get('research', False):
            analysis_tasks.append(('research_report', self.research_report_analyst_agent, stock_info, research_data))

        # 公告分析
        if enabled_analysts.get('announcement', False):
            analysis_tasks.append(('announcement', self.announcement_analyst_agent, stock_info, announcement_data))

        # 筹码分析
        if enabled_analysts.get('chip', False):
            analysis_tasks.append(('chip', self.chip_analyst_agent, stock_info, chip_data))
        
        # 使用线程池并行执行分析
        agents_results = {}
        timing_results = {}
        lock = threading.Lock()
        
        print(f"\n⏳ 开始并行分析... (启动 {len(analysis_tasks)} 个分析线程)")
        print("-" * 60)
        
        with ThreadPoolExecutor(max_workers=len(analysis_tasks)) as executor:
            # 提交所有任务
            futures = []
            for task in analysis_tasks:
                analyst_name = task[0]
                analyst_func = task[1]
                args = task[2:]
                future = executor.submit(run_analyst_with_timing, analyst_name, analyst_func, *args)
                futures.append(future)
            
            # 收集结果
            completed_count = 0
            for future in as_completed(futures):
                analyst_name, result, elapsed_time, error = future.result()
                completed_count += 1
                
                with lock:
                    if result is not None:
                        agents_results[analyst_name] = result
                        timing_results[analyst_name] = elapsed_time
                        print(f"✅ [{completed_count}/{len(analysis_tasks)}] {result.get('agent_name', analyst_name)} 完成分析 (用时: {elapsed_time:.2f}秒)")
                    else:
                        timing_results[analyst_name] = elapsed_time
                        print(f"❌ [{completed_count}/{len(analysis_tasks)}] {analyst_name} 分析失败 (用时: {elapsed_time:.2f}秒) - {error}")
        
        # 计算总用时
        total_elapsed_time = time.time() - total_start_time
        
        print("-" * 60)
        print(f"✅ 所有分析师完成分析!")
        print(f"\n⏱️  性能统计:")
        print(f"   总用时: {total_elapsed_time:.2f} 秒")
        print(f"   并行效率: 节省了 {sum(timing_results.values()) - total_elapsed_time:.2f} 秒")
        print(f"   平均单个分析用时: {sum(timing_results.values()) / len(timing_results):.2f} 秒" if timing_results else "")
        
        # 显示各分析师用时详情
        if timing_results:
            print(f"\n📊 分析师用时排行:")
            sorted_timing = sorted(timing_results.items(), key=lambda x: x[1], reverse=True)
            for idx, (name, elapsed) in enumerate(sorted_timing, 1):
                agent_name = agents_results.get(name, {}).get('agent_name', name)
                print(f"   {idx}. {agent_name}: {elapsed:.2f}秒")
        
        print("=" * 60)
        
        # 在结果中添加性能统计
        agents_results['_performance'] = {
            'total_time': total_elapsed_time,
            'analyst_times': timing_results,
            'parallel_efficiency': sum(timing_results.values()) - total_elapsed_time if timing_results else 0,
            'analyst_count': len(analysis_tasks)
        }
        
        return agents_results
    
    def conduct_team_discussion(self, agents_results: Dict[str, Any], stock_info: Dict) -> str:
        """进行团队讨论"""
        print("🤝 分析团队正在进行综合讨论...")
        time.sleep(2)
        
        # 收集参与分析的分析师名单和报告
        participants = []
        reports = []
        
        if "technical" in agents_results:
            participants.append("技术分析师")
            reports.append(f"【技术分析师报告】\n{agents_results['technical'].get('analysis', '')}")
        
        if "fundamental" in agents_results:
            participants.append("基本面分析师")
            reports.append(f"【基本面分析师报告】\n{agents_results['fundamental'].get('analysis', '')}")
        
        if "fund_flow" in agents_results:
            participants.append("资金面分析师")
            reports.append(f"【资金面分析师报告】\n{agents_results['fund_flow'].get('analysis', '')}")
        
        if "risk_management" in agents_results:
            participants.append("风险管理师")
            reports.append(f"【风险管理师报告】\n{agents_results['risk_management'].get('analysis', '')}")
        
        if "market_sentiment" in agents_results:
            participants.append("市场情绪分析师")
            reports.append(f"【市场情绪分析师报告】\n{agents_results['market_sentiment'].get('analysis', '')}")
        
        if "news" in agents_results:
            participants.append("新闻分析师")
            reports.append(f"【新闻分析师报告】\n{agents_results['news'].get('analysis', '')}")

        if "research_report" in agents_results:
            participants.append("机构研报分析师")
            reports.append(f"【机构研报分析师报告】\n{agents_results['research_report'].get('analysis', '')}")

        if "announcement" in agents_results:
            participants.append("公告分析师")
            reports.append(f"【公告分析师报告】\n{agents_results['announcement'].get('analysis', '')}")

        if "chip" in agents_results:
            participants.append("筹码分析师")
            reports.append(f"【筹码分析师报告】\n{agents_results['chip'].get('analysis', '')}")
        
        # 组合所有报告
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
            {"role": "system", "content": "你需要模拟一场专业的投资团队讨论会议，体现不同角色的观点碰撞和最终共识形成。"},
            {"role": "user", "content": discussion_prompt}
        ]
        
        discussion_result = self.deepseek_client.call_api(messages, max_tokens=6000)
        
        print("✅ 团队讨论完成")
        return discussion_result
    
    def make_final_decision(self, discussion_result: str, stock_info: Dict, indicators: Dict) -> Dict[str, Any]:
        """制定最终投资决策"""
        print("📋 正在制定最终投资决策...")
        time.sleep(1)
        
        decision = self.deepseek_client.final_decision(discussion_result, stock_info, indicators)
        
        print("✅ 最终投资决策完成")
        return decision
