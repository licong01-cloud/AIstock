from typing import Dict, Any
import logging
from datetime import datetime

from ..models.analysis import (
    StockAnalysisRequest,
    StockAnalysisResponse,
    AgentOpinion,
    StockContextResponse,
    StockKlineSeries,
    StockQuote,
    BatchStockAnalysisRequest,
    BatchStockAnalysisItemResult,
    BatchStockAnalysisResponse,
)
from ..data_access.unified import NextUnifiedDataAccess
from ..agents.stock_analysis import NextStockAnalysisAgents
from ..repositories.analysis_repo_impl import analysis_repo
from ..infra.pdf_report_impl import create_pdf_report, generate_markdown_report


logger = logging.getLogger(__name__)


def _choose_period(start_date: str | None, end_date: str | None) -> str:
    """根据起止日期粗略选择一个历史周期标签，用于复用现有 period 逻辑。

    这是一个启发式实现，后续可以改为直接使用统一数据访问的精确日期接口。
    """

    if not start_date or not end_date:
        return "1y"

    from datetime import datetime

    try:
        d1 = datetime.strptime(start_date, "%Y-%m-%d")
        d2 = datetime.strptime(end_date, "%Y-%m-%d")
        days = abs((d2 - d1).days)
    except Exception:
        return "1y"

    if days <= 40:
        return "1mo"
    if days <= 120:
        return "3mo"
    if days <= 220:
        return "6mo"
    if days <= 400:
        return "1y"
    if days <= 800:
        return "2y"
    if days <= 2000:
        return "5y"
    return "max"


def analyze_stock(req: StockAnalysisRequest) -> StockAnalysisResponse:
    """使用统一数据访问 + 多智能体实现真实的股票分析流程。

    - 通过 UnifiedDataAccess 获取股票信息、历史行情和技术指标；
    - 通过 StockAnalysisAgents 运行多智能体分析；
    - 将各智能体的结果压缩为统一的 Opinion 列表和总体结论。
    """

    # 1. 准备基础数据
    uda = NextUnifiedDataAccess()

    # 诊断信息容器：记录每一步数据获取的成功/失败及错误原因
    diagnostics: Dict[str, Any] = {}

    # period 粗略由起止日期推断；analysis_date 使用 end_date（若提供）
    period = _choose_period(req.start_date, req.end_date)
    analysis_date = None
    if req.end_date:
        # 统一数据访问内部以 YYYYMMDD 为主
        analysis_date = req.end_date.replace("-", "")

    # 股票代码统一直接使用请求中的 ts_code，内部会自行转换
    symbol = req.ts_code

    # 获取股票信息
    stock_info: Dict[str, Any] | None = None
    try:
        stock_info = uda.get_stock_info(symbol, analysis_date=analysis_date)
        diagnostics["stock_info"] = {"status": "success"}
    except Exception as e:  # noqa: BLE001 - 这里需要完整记录错误信息
        diagnostics["stock_info"] = {
            "status": "error",
            "error": repr(e),
        }
        logger.exception("get_stock_info failed for symbol=%s", symbol)
        # 兜底为最小结构，避免后续代码因为 None 崩溃
        stock_info = {"symbol": symbol}

    # 获取历史数据
    stock_data = None
    try:
        # 复用旧应用里 get_stock_data 的逻辑结构：
        try:
            stock_data = uda.get_stock_data(symbol, period, analysis_date=analysis_date)
        except TypeError as e:  # 兼容不支持 analysis_date 的旧实现
            if "analysis_date" in str(e):
                stock_data = uda.get_stock_data(symbol, period)
            else:
                raise
        diagnostics["stock_data"] = {"status": "success", "period": period}
    except Exception as e:  # noqa: BLE001
        diagnostics["stock_data"] = {
            "status": "error",
            "error": repr(e),
            "period": period,
        }
        logger.exception("get_stock_data failed for symbol=%s, period=%s", symbol, period)
        stock_data = None

    # 兜底：没有有效数据则返回简单提示，同时附带诊断信息
    if stock_data is None:
        agents = [
            AgentOpinion(
                name="系统提示",
                summary="无法获取该股票的历史数据，请稍后重试或检查代码是否正确。",
            )
        ]
        conclusion = "数据源暂不可用，无法完成多智能体分析。"
        logger.debug("Stock analysis aborted due to missing stock_data: ts_code=%s, diagnostics=%s", req.ts_code, diagnostics)
        return StockAnalysisResponse(
            ts_code=req.ts_code,
            agents=agents,
            conclusion=conclusion,
            data_fetch_diagnostics=diagnostics,
            technical_indicators=None,
        )

    # 计算技术指标：沿用 UnifiedDataAccess 内部的工具
    try:
        stock_data_with_indicators = uda.stock_data_fetcher.calculate_technical_indicators(stock_data)
        indicators = uda.stock_data_fetcher.get_latest_indicators(stock_data_with_indicators)
        diagnostics["technical_indicators"] = {"status": "success"}
    except Exception as e:  # noqa: BLE001
        stock_data_with_indicators = stock_data
        indicators = {}
        diagnostics["technical_indicators"] = {
            "status": "error",
            "error": repr(e),
        }
        logger.exception("calculate_technical_indicators failed for symbol=%s", symbol)

    # 2. 准备多维度数据（财务、资金流、风险等）
    financial_data: Dict[str, Any] | None = None
    fund_flow_data: Dict[str, Any] | None = None
    risk_data: Dict[str, Any] | None = None
    sentiment_data: Dict[str, Any] | None = None
    news_data: Dict[str, Any] | None = None
    research_data: Dict[str, Any] | None = None
    announcement_data: Dict[str, Any] | None = None
    chip_data: Dict[str, Any] | None = None

    try:
        financial_data = uda.get_financial_data(symbol, analysis_date=analysis_date)
        diagnostics["financial_data"] = {
            "status": "success",
            "has_data": financial_data is not None,
        }
    except Exception as e:  # noqa: BLE001
        financial_data = None
        diagnostics["financial_data"] = {
            "status": "error",
            "error": repr(e),
        }
        logger.exception("get_financial_data failed for symbol=%s", symbol)

    try:
        fund_flow_data = uda.get_fund_flow_data(symbol, analysis_date=analysis_date)
        diagnostics["fund_flow_data"] = {
            "status": "success",
            "has_data": fund_flow_data is not None,
        }
    except Exception as e:  # noqa: BLE001
        fund_flow_data = None
        diagnostics["fund_flow_data"] = {
            "status": "error",
            "error": repr(e),
        }
        logger.exception("get_fund_flow_data failed for symbol=%s", symbol)

    try:
        risk_data = uda.get_risk_data(symbol, analysis_date=analysis_date)
        diagnostics["risk_data"] = {
            "status": "success",
            "has_data": risk_data is not None,
        }
    except Exception as e:  # noqa: BLE001
        risk_data = None
        diagnostics["risk_data"] = {
            "status": "error",
            "error": repr(e),
        }
        logger.exception("get_risk_data failed for symbol=%s", symbol)

    # 额外数据源（情绪、新闻、研报、公告、筹码等）目前尚未在统一数据访问层集中获取，
    # 但为了便于排查问题，这里也给出显式的诊断条目，区分：
    # - skipped: 对应分析师未启用；
    # - not_implemented: 分析师启用，但尚未接入统一数据获取逻辑；
    # - unknown: 前端未显式传入 enabled_analysts 配置。
    enabled = req.enabled_analysts or {}

    # 若前端显式启用了情绪或新闻分析师，则尝试通过统一数据访问获取对应数据，
    # 并在 diagnostics 中记录真实的获取结果。

    if enabled.get("sentiment") is True:
        try:
            sentiment_data = uda.get_market_sentiment_data(
                symbol,
                stock_data_with_indicators,
                analysis_date=analysis_date,
            )
            diagnostics["sentiment_data"] = {
                "status": "success",
                "has_data": sentiment_data is not None,
            }
        except Exception as e:  # noqa: BLE001
            sentiment_data = None
            diagnostics["sentiment_data"] = {
                "status": "error",
                "error": repr(e),
            }
            logger.exception(
                "get_market_sentiment_data failed for symbol=%s", symbol
            )

    if enabled.get("news") is True:
        try:
            news_data = uda.get_news_data(symbol, analysis_date=analysis_date)

            has_data = False
            source = None
            count = None

            if isinstance(news_data, dict):
                source = news_data.get("source")
                inner = news_data.get("news_data")
                if isinstance(inner, dict):
                    # 优先使用 count 字段，其次回退到 items 长度
                    items = inner.get("items") or []
                    try:
                        count = int(inner.get("count")) if inner.get("count") is not None else len(items)
                    except Exception:
                        count = len(items)
                    has_data = bool(news_data.get("data_success") and count and count > 0)
                else:
                    # 兼容旧结构：仅依赖 data_success
                    has_data = bool(news_data.get("data_success"))

            diagnostics["news_data"] = {
                "status": "success",
                "has_data": has_data,
                "source": source,
                "count": count,
            }
        except Exception as e:  # noqa: BLE001
            news_data = None
            diagnostics["news_data"] = {
                "status": "error",
                "error": repr(e),
            }
            logger.exception("get_news_data failed for symbol=%s", symbol)

    if enabled.get("research") is True:
        try:
            research_data = uda.get_research_reports_data(
                symbol,
                days=180,
                analysis_date=analysis_date,
            )
            has_data = bool(
                isinstance(research_data, dict)
                and research_data.get("data_success")
                and research_data.get("report_count", 0) > 0
            )
            diagnostics["research_data"] = {
                "status": "success",
                "has_data": has_data,
                "source": research_data.get("source")
                if isinstance(research_data, dict)
                else None,
                "report_count": research_data.get("report_count")
                if isinstance(research_data, dict)
                else None,
            }
        except Exception as e:  # noqa: BLE001
            research_data = None
            diagnostics["research_data"] = {
                "status": "error",
                "error": repr(e),
            }
            logger.exception("get_research_reports_data failed for symbol=%s", symbol)

    if enabled.get("announcement") is True:
        try:
            announcement_data = uda.get_announcement_data(
                symbol,
                days=30,
                analysis_date=analysis_date,
            )
            has_data = bool(
                isinstance(announcement_data, dict)
                and announcement_data.get("data_success")
                and announcement_data.get("count", 0) > 0
            )
            diagnostics["announcement_data"] = {
                "status": "success",
                "has_data": has_data,
                "source": announcement_data.get("source")
                if isinstance(announcement_data, dict)
                else None,
                "count": announcement_data.get("count")
                if isinstance(announcement_data, dict)
                else None,
            }
        except Exception as e:  # noqa: BLE001
            announcement_data = None
            diagnostics["announcement_data"] = {
                "status": "error",
                "error": repr(e),
            }
            logger.exception("get_announcement_data failed for symbol=%s", symbol)

    if enabled.get("chip") is True:
        try:
            current_price_val = None
            try:
                if stock_info and isinstance(stock_info, dict):
                    cp = stock_info.get("current_price")
                    if cp is not None:
                        current_price_val = float(cp)
            except Exception:  # noqa: BLE001
                current_price_val = None

            chip_data = uda.get_chip_distribution_data(
                symbol,
                trade_date=None,
                current_price=current_price_val,
                analysis_date=analysis_date,
            )
            has_data = bool(
                isinstance(chip_data, dict) and chip_data.get("data_success")
            )
            diagnostics["chip_data"] = {
                "status": "success",
                "has_data": has_data,
                "source": chip_data.get("source") if isinstance(chip_data, dict) else None,
                "latest_date": chip_data.get("latest_date")
                if isinstance(chip_data, dict)
                else None,
            }
        except Exception as e:  # noqa: BLE001
            chip_data = None
            diagnostics["chip_data"] = {
                "status": "error",
                "error": repr(e),
            }
            logger.exception(
                "get_chip_distribution_data failed for symbol=%s", symbol
            )

    def _mark_optional_source(flag_name: str, diag_key: str) -> None:
        if diag_key in diagnostics:
            return
        flag = enabled.get(flag_name)
        if flag is True:
            diagnostics[diag_key] = {
                "status": "not_implemented",
                "reason": "data_fetch_not_wired",
            }
        elif flag is False:
            diagnostics[diag_key] = {
                "status": "skipped",
                "reason": "analyst_disabled",
            }
        else:
            diagnostics[diag_key] = {
                "status": "unknown",
                "reason": "no_explicit_flag",
            }

    _mark_optional_source("sentiment", "sentiment_data")
    _mark_optional_source("news", "news_data")
    _mark_optional_source("research", "research_data")
    _mark_optional_source("announcement", "announcement_data")
    _mark_optional_source("chip", "chip_data")

    # 3. 运行多智能体分析（通过 next_app 封装层调用旧多智能体实现）
    agents_core = NextStockAnalysisAgents(model="deepseek-chat")

    agents_results, discussion_result, final_decision = agents_core.run_core_analysis(
        stock_info=stock_info,
        stock_data_with_indicators=stock_data_with_indicators,
        indicators=indicators,
        financial_data=financial_data,
        fund_flow_data=fund_flow_data,
        risk_data=risk_data,
        sentiment_data=sentiment_data,
        news_data=news_data,
        research_data=research_data,
        announcement_data=announcement_data,
        chip_data=chip_data,
        enabled_analysts=req.enabled_analysts,
    )

    # 4. 将结果压缩为统一响应结构
    opinions: list[AgentOpinion] = []
    for key, value in agents_results.items():
        if key.startswith("_"):
            continue
        if not isinstance(value, dict):
            continue
        agent_name = value.get("agent_name") or key
        summary = value.get("analysis") or ""
        opinions.append(AgentOpinion(name=agent_name, summary=summary))

    # 最终结论：优先从 final_decision 中提取 summary，其次用团队讨论文本
    conclusion: str
    if isinstance(final_decision, dict) and "summary" in final_decision:
        conclusion = str(final_decision["summary"])
    else:
        conclusion = str(discussion_result)

    # 5. 持久化分析结果到 app.analysis_records（与旧版 pg_stock_analysis_repo 对齐）
    record_id: int | None = None
    saved_to_db = False
    try:
        stock_name = str(
            (stock_info or {}).get("name")
            or (stock_info or {}).get("stock_name")
            or ""
        )
        record_id = analysis_repo.save_analysis(
            symbol=symbol,
            stock_name=stock_name,
            period=period,
            stock_info=stock_info or {},
            agents_results=agents_results or {},
            discussion_result=discussion_result or {},
            final_decision=final_decision or {},
        )
        saved_to_db = True
    except Exception:  # noqa: BLE001
        logger.exception("save_analysis failed for symbol=%s", symbol)

    logger.debug(
        "Stock analysis completed: ts_code=%s, data_fetch_diagnostics=%s, saved_to_db=%s, record_id=%s",
        req.ts_code,
        diagnostics,
        saved_to_db,
        record_id,
    )

    return StockAnalysisResponse(
        ts_code=req.ts_code,
        agents=opinions,
        conclusion=conclusion,
        agents_raw=agents_results,
        discussion=str(discussion_result) if discussion_result is not None else None,
        final_decision=final_decision if isinstance(final_decision, dict) else None,
        data_fetch_diagnostics=diagnostics,
        technical_indicators=indicators if isinstance(indicators, dict) else None,
        record_id=record_id,
        saved_to_db=saved_to_db,
    )


def get_stock_context(req: StockAnalysisRequest) -> StockContextResponse:
    """返回单只股票的概览信息和基础K线数据，用于前端行情+走势区域。

    - 概览信息基于 NextUnifiedDataAccess.get_stock_info；
    - K线数据基于 get_stock_data 的结果，仅返回收盘价时间序列。
    """

    uda = NextUnifiedDataAccess()

    # period 与分析接口保持一致的推断逻辑
    period = _choose_period(req.start_date, req.end_date)
    analysis_date = None
    if req.end_date:
        analysis_date = req.end_date.replace("-", "")

    symbol = req.ts_code

    # 基础信息
    info = uda.get_stock_info(symbol, analysis_date=analysis_date)

    quote = StockQuote(
        symbol=str(info.get("symbol") or symbol),
        name=str(info.get("name") or ""),
        current_price=_safe_float(info.get("current_price")),
        change_percent=_safe_float(info.get("change_percent")),
        open_price=_safe_float(info.get("open_price")),
        high_price=_safe_float(info.get("high_price")),
        low_price=_safe_float(info.get("low_price")),
        pre_close=_safe_float(info.get("pre_close")),
        volume=_safe_float(info.get("volume")),
        amount=_safe_float(info.get("amount")),
        quote_source=str(info.get("quote_source") or "") or None,
        quote_timestamp=_parse_timestamp(info.get("quote_timestamp")),
        week52_high=_safe_float(info.get("52_week_high")),
        week52_low=_safe_float(info.get("52_week_low")),
    )

    # 历史K线（用于前端K线图展示，包含 OHLC）
    kline_series: StockKlineSeries | None = None
    try:
        df = uda.get_stock_data(symbol, period, analysis_date=analysis_date)
        if df is not None and hasattr(df, "index") and len(df.index) > 0:
            # Date 索引已经在 UnifiedDataAccess 中标准化
            dates = [idx.strftime("%Y-%m-%d") for idx in df.index]

            opens: list[float | None] = []
            highs: list[float | None] = []
            lows: list[float | None] = []
            closes: list[float | None] = []

            for _, row in df.iterrows():
                opens.append(_safe_float(row.get("Open")))
                highs.append(_safe_float(row.get("High")))
                lows.append(_safe_float(row.get("Low")))
                closes.append(_safe_float(row.get("Close")))

            kline_series = StockKlineSeries(
                dates=dates,
                open=opens,
                high=highs,
                low=lows,
                close=closes,
            )
    except Exception:
        kline_series = None

    return StockContextResponse(
        ts_code=req.ts_code,
        name=quote.name,
        quote=quote,
        kline=kline_series,
    )


def get_realtime_quote(symbol: str) -> StockQuote:
    """基于统一数据访问的实时行情接口（通常走 TDX），仅返回当前价格等轻量字段。

    用于历史详情中的“当前价格/涨跌幅”卡片，避免重新拉取完整 K 线等重型数据。
    """

    uda = NextUnifiedDataAccess()

    try:
        quotes = uda.get_realtime_quotes(symbol)
    except Exception:
        quotes = None

    if not isinstance(quotes, dict):
        # 兜底：没有可用行情时返回空值，占位显示 "--"
        return StockQuote(
            symbol=str(symbol),
            name="",
            current_price=None,
            change_percent=None,
            open_price=None,
            high_price=None,
            low_price=None,
            pre_close=None,
            volume=None,
            amount=None,
            quote_source=None,
            quote_timestamp=None,
        )

    sym = str(quotes.get("symbol") or symbol)
    name = str(quotes.get("name") or "")

    current_price = _safe_float(quotes.get("price"))
    change_percent = _safe_float(quotes.get("change_percent"))
    open_price = _safe_float(quotes.get("open"))
    high_price = _safe_float(quotes.get("high"))
    low_price = _safe_float(quotes.get("low"))
    pre_close = _safe_float(quotes.get("pre_close"))
    volume = _safe_float(quotes.get("volume"))
    amount = _safe_float(quotes.get("amount"))
    quote_source = str(quotes.get("source") or "") or None
    quote_timestamp = _parse_timestamp(quotes.get("timestamp"))

    return StockQuote(
        symbol=sym,
        name=name,
        current_price=current_price,
        change_percent=change_percent,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        pre_close=pre_close,
        volume=volume,
        amount=amount,
        quote_source=quote_source,
        quote_timestamp=quote_timestamp,
    )


def analyze_stocks_batch(req: BatchStockAnalysisRequest) -> BatchStockAnalysisResponse:
    """批量股票分析服务。

    - 语义上对应旧版 app.py 中的 run_batch_analysis；
    - 复用 analyze_stock 以确保与单股分析逻辑完全一致；
    - 支持顺序(sequential)和并行(parallel)两种模式，并行模式最大并发数为 3。
    """

    import concurrent.futures

    ts_codes = [c.strip() for c in req.ts_codes if c and c.strip()]
    total = len(ts_codes)
    if total == 0:
        return BatchStockAnalysisResponse(
            total=0,
            success_count=0,
            failed_count=0,
            results=[],
        )

    def _build_single_request(code: str) -> StockAnalysisRequest:
        return StockAnalysisRequest(
            ts_code=code,
            start_date=req.start_date,
            end_date=req.end_date,
            enabled_analysts=req.enabled_analysts,
        )

    results: list[BatchStockAnalysisItemResult] = []

    def _run_single(code: str) -> BatchStockAnalysisItemResult:
        try:
            single_req = _build_single_request(code)
            analysis = analyze_stock(single_req)
            return BatchStockAnalysisItemResult(
                ts_code=code,
                success=True,
                error=None,
                analysis=analysis,
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("Batch analyze failed for ts_code=%s", code)
            return BatchStockAnalysisItemResult(
                ts_code=code,
                success=False,
                error=str(e),
                analysis=None,
            )

    mode = (req.batch_mode or "sequential").lower()
    if mode == "parallel":
        # 多线程并行分析，最大并发数 3，参考旧版实现
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_code = {
                executor.submit(_run_single, code): code for code in ts_codes
            }
            for future in concurrent.futures.as_completed(future_to_code):
                item = future.result()
                results.append(item)
    else:
        # 顺序分析
        for code in ts_codes:
            item = _run_single(code)
            results.append(item)

    success_count = sum(1 for r in results if r.success)
    failed_count = total - success_count

    return BatchStockAnalysisResponse(
        total=total,
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )


def generate_stock_pdf_from_record(record_id: int) -> tuple[bytes, str]:
    """根据 analysis_records 记录生成单股分析 PDF 报告。

    返回 (pdf_bytes, filename)。
    """

    record = analysis_repo.get_record_by_id(record_id)
    if not record:
        raise ValueError(f"analysis record not found: {record_id}")

    stock_info = record.get("stock_info") or {}
    agents_results = record.get("agents_results") or {}
    discussion_result = record.get("discussion_result") or {}
    final_decision = record.get("final_decision") or {}

    pdf_bytes = create_pdf_report(
        stock_info=stock_info,
        agents_results=agents_results,
        discussion_result=discussion_result,
        final_decision=final_decision,
    )

    symbol = (stock_info.get("symbol") or record.get("symbol") or "unknown")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"股票分析报告_{symbol}_{ts}.pdf"
    return pdf_bytes, filename


def generate_stock_markdown_from_record(record_id: int) -> tuple[str, str]:
    """根据 analysis_records 记录生成 Markdown 报告文本。

    返回 (markdown_text, filename)。
    """

    record = analysis_repo.get_record_by_id(record_id)
    if not record:
        raise ValueError(f"analysis record not found: {record_id}")

    stock_info = record.get("stock_info") or {}
    agents_results = record.get("agents_results") or {}
    discussion_result = record.get("discussion_result") or {}
    final_decision = record.get("final_decision") or {}

    md_text = generate_markdown_report(
        stock_info=stock_info,
        agents_results=agents_results,
        discussion_result=discussion_result,
        final_decision=final_decision,
    )

    symbol = (stock_info.get("symbol") or record.get("symbol") or "unknown")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"股票分析报告_{symbol}_{ts}.md"
    return md_text, filename


def get_history_records(
    symbol_or_name: str | None,
    page: int = 1,
    page_size: int = 10,
    rating: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """分页查询历史分析记录列表，支持按代码/名称、评级与日期区间过滤。

    - 语义上对应旧版 app.py.display_history_records 中的筛选与分页；
    - 仅返回列表展示所需的摘要字段，不返回完整 JSON 载荷。
    """

    return analysis_repo.list_records(
        symbol_or_name=symbol_or_name or None,
        page=page,
        page_size=page_size,
        rating=rating or None,
        start_date=start_date or None,
        end_date=end_date or None,
    )


def delete_history_record(record_id: int) -> bool:
    """删除单条历史分析记录。

    语义对应旧版 db.delete_record(record_id)；返回 True/False。"""

    return analysis_repo.delete_record(record_id)


def add_history_record_to_monitor_quick(record_id: int) -> dict[str, Any]:
    """基于历史记录的一键加入监测。

    - 复用旧版 display_add_to_monitor_dialog 中的解析逻辑；
    - 从 final_decision 提取进场区间、止盈、止损和评级；
    - 调用 pg_monitor_repo.monitor_db.add_monitored_stock 并立即触发一次手动更新。
    """

    record = analysis_repo.get_record_by_id(record_id)
    if not record:
        raise ValueError(f"analysis record not found: {record_id}")

    final_decision = record.get("final_decision") or {}
    if not isinstance(final_decision, dict):
        final_decision = {}

    # 解析进场区间
    entry_min = 0.0
    entry_max = 0.0
    entry_range_str = final_decision.get("entry_range", "N/A")
    if entry_range_str and entry_range_str != "N/A":
        try:
            import re

            clean_str = (
                str(entry_range_str)
                .replace("¥", "")
                .replace("元", "")
                .replace("$", "")
            )
            numbers = re.findall(r"\d+\.?\d*", clean_str)
            if len(numbers) >= 2:
                entry_min = float(numbers[0])
                entry_max = float(numbers[1])
        except Exception:
            try:
                clean_str = (
                    str(entry_range_str)
                    .replace("¥", "")
                    .replace("元", "")
                    .replace("$", "")
                )
                for sep in ["-", "~", "至", "到"]:
                    if sep in clean_str:
                        parts = clean_str.split(sep)
                        if len(parts) == 2:
                            entry_min = float(parts[0].strip())
                            entry_max = float(parts[1].strip())
                            break
            except Exception:
                pass

    # 解析止盈/止损
    take_profit = 0.0
    stop_loss = 0.0
    take_profit_str = final_decision.get("take_profit", "N/A")
    stop_loss_str = final_decision.get("stop_loss", "N/A")

    if take_profit_str and take_profit_str != "N/A":
        try:
            import re

            clean_str = (
                str(take_profit_str)
                .replace("¥", "")
                .replace("元", "")
                .replace("$", "")
                .strip()
            )
            numbers = re.findall(r"\d+\.?\d*", clean_str)
            if numbers:
                take_profit = float(numbers[0])
        except Exception:
            pass

    if stop_loss_str and stop_loss_str != "N/A":
        try:
            import re

            clean_str = (
                str(stop_loss_str)
                .replace("¥", "")
                .replace("元", "")
                .replace("$", "")
                .strip()
            )
            numbers = re.findall(r"\d+\.?\d*", clean_str)
            if numbers:
                stop_loss = float(numbers[0])
        except Exception:
            pass

    rating = final_decision.get("rating", "买入")

    # 默认监测参数：与旧版对话框中的默认值保持一致
    check_interval = 30
    notification_enabled = True

    if not (entry_min > 0 and entry_max > 0 and entry_max > entry_min):
        raise ValueError("无法从历史记录的最终决策中解析有效的进场区间，暂不支持一键加入监测")

    from pg_monitor_repo import monitor_db
    from monitor_service import monitor_service

    entry_range = {"min": entry_min, "max": entry_max}
    stock_id = monitor_db.add_monitored_stock(
        symbol=record["symbol"],
        name=record["stock_name"],
        rating=rating,
        entry_range=entry_range,
        take_profit=take_profit if take_profit > 0 else None,
        stop_loss=stop_loss if stop_loss > 0 else None,
        check_interval=check_interval,
        notification_enabled=notification_enabled,
    )

    try:
        monitor_service.manual_update_stock(stock_id)
    except Exception:
        logger.exception("manual_update_stock failed for stock_id=%s", stock_id)

    return {
        "monitor_stock_id": stock_id,
        "symbol": record["symbol"],
        "stock_name": record["stock_name"],
        "rating": rating,
        "entry_range": entry_range,
        "take_profit": take_profit if take_profit > 0 else None,
        "stop_loss": stop_loss if stop_loss > 0 else None,
        "check_interval": check_interval,
        "notification_enabled": notification_enabled,
    }


def get_history_record_detail(record_id: int) -> StockAnalysisResponse:
    record = analysis_repo.get_record_by_id(record_id)
    if not record:
        raise ValueError(f"analysis record not found: {record_id}")

    stock_info = record.get("stock_info") or {}
    agents_results = record.get("agents_results") or {}
    discussion_result = record.get("discussion_result") or {}
    final_decision = record.get("final_decision") or {}

    symbol = str(stock_info.get("symbol") or record.get("symbol") or "")

    opinions: list[AgentOpinion] = []
    if isinstance(agents_results, dict):
        for key, value in agents_results.items():
            if isinstance(key, str) and key.startswith("_"):
                continue
            if not isinstance(value, dict):
                continue
            agent_name = str(value.get("agent_name") or key)
            summary = str(value.get("analysis") or "")
            opinions.append(AgentOpinion(name=agent_name, summary=summary))

    if isinstance(final_decision, dict) and "summary" in final_decision:
        conclusion = str(final_decision["summary"])
    else:
        conclusion = str(discussion_result)

    return StockAnalysisResponse(
        ts_code=symbol,
        agents=opinions,
        conclusion=conclusion,
        agents_raw=agents_results if isinstance(agents_results, dict) else None,
        discussion=str(discussion_result) if discussion_result is not None else None,
        final_decision=final_decision if isinstance(final_decision, dict) else None,
        data_fetch_diagnostics=None,
        record_id=int(record.get("id") or record_id),
        saved_to_db=True,
    )


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "N/A":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any):
    from datetime import datetime

    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # 尝试多种常见格式
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
    return None
