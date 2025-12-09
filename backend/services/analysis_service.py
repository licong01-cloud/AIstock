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
    StockTrendAnalysisRequest,
    StockTrendAnalysisResponse,
)
from ..data_access.unified import NextUnifiedDataAccess
from ..agents.stock_analysis import NextStockAnalysisAgents
from ..agents.trend_analysis import StockTrendAnalysisAgents
from ..repositories.analysis_repo_impl import analysis_repo
from ..repositories.trend_analysis_repo_impl import trend_analysis_repo
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

    # 2. 准备多维度数据（财务、季报、资金流、风险等）
    financial_data: Dict[str, Any] | None = None
    quarterly_data: Dict[str, Any] | None = None
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

    # 季报数据（最近8期），用于基本面分析重点的季报趋势部分
    try:
        quarterly_data = uda.get_quarterly_reports(symbol, analysis_date=analysis_date)
        diagnostics["quarterly_data"] = {
            "status": "success",
            "has_data": bool(
                isinstance(quarterly_data, dict)
                and quarterly_data.get("data_success")
            ),
        }
    except Exception as e:  # noqa: BLE001
        quarterly_data = None
        diagnostics["quarterly_data"] = {
            "status": "error",
            "error": repr(e),
        }
        logger.exception("get_quarterly_reports failed for symbol=%s", symbol)

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
        quarterly_data=quarterly_data,
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


def analyze_stock_trend(req: StockTrendAnalysisRequest) -> StockTrendAnalysisResponse:
    """Skeleton implementation of the stock trend analysis service.

    Phase 1: only define the public API shape and return a mock payload.

    Later phases will:
    - Reuse NextUnifiedDataAccess to fetch all required data;
    - Delegate to a dedicated StockTrendAnalysisAgents orchestrator;
    - Persist detailed results to dedicated trend analysis tables.
    """

    # 1. 复用与 analyze_stock 一致的 period / analysis_date 推断逻辑
    uda = NextUnifiedDataAccess()

    period = _choose_period(req.start_date, req.end_date)
    analysis_date_raw: str | None = None
    if req.analysis_date:
        analysis_date_raw = req.analysis_date.replace("-", "")
    elif req.end_date:
        analysis_date_raw = req.end_date.replace("-", "")

    if analysis_date_raw:
        analysis_date_str = f"{analysis_date_raw[:4]}-{analysis_date_raw[4:6]}-{analysis_date_raw[6:8]}"
    else:
        analysis_date_str = datetime.utcnow().strftime("%Y-%m-%d")

    # 2. 获取基础数据（信息 + 历史行情 + 技术指标）
    symbol = req.ts_code

    diagnostics: Dict[str, Any] = {}

    stock_info: Dict[str, Any] | None = None
    try:
        stock_info = uda.get_stock_info(symbol, analysis_date=analysis_date_raw)
        diagnostics["stock_info"] = {"status": "success"}
    except Exception as e:  # noqa: BLE001
        diagnostics["stock_info"] = {"status": "error", "error": repr(e)}
        logger.exception("get_stock_info failed for symbol=%s", symbol)
        stock_info = {"symbol": symbol}

    stock_data = None
    try:
        try:
            stock_data = uda.get_stock_data(symbol, period, analysis_date=analysis_date_raw)
        except TypeError as e:  # 兼容旧实现
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

    if stock_data is None:
        # 无法获取基础行情时，直接返回空预测并附带诊断信息
        return StockTrendAnalysisResponse(
            ts_code=req.ts_code,
            analysis_date=analysis_date_str,
            mode=req.mode,
            horizons=[],
            analysts=[],
            risk_report=None,
            prediction_evolution=[],
            record_id=None,
        )

    try:
        stock_data_with_indicators = uda.stock_data_fetcher.calculate_technical_indicators(stock_data)
        indicators = uda.stock_data_fetcher.get_latest_indicators(stock_data_with_indicators)
        diagnostics["technical_indicators"] = {"status": "success"}
    except Exception as e:  # noqa: BLE001
        stock_data_with_indicators = stock_data
        indicators = {}
        diagnostics["technical_indicators"] = {"status": "error", "error": repr(e)}
        logger.exception("calculate_technical_indicators failed for symbol=%s", symbol)

    # 获取基本面相关数据（估值与成长性）
    financial_data: Dict[str, Any] | None = None
    quarterly_data: Dict[str, Any] | None = None

    try:
        financial_data = uda.get_financial_data(symbol, analysis_date=analysis_date_raw)
        diagnostics["financial_data"] = {
            "status": "success",
            "has_data": financial_data is not None,
        }
    except Exception as e:  # noqa: BLE001
        financial_data = None
        diagnostics["financial_data"] = {"status": "error", "error": repr(e)}
        logger.exception("get_financial_data failed for symbol=%s", symbol)

    try:
        quarterly_data = uda.get_quarterly_reports(symbol, analysis_date=analysis_date_raw)
        diagnostics["quarterly_data"] = {
            "status": "success",
            "has_data": bool(
                isinstance(quarterly_data, dict)
                and quarterly_data.get("data_success"),
            ),
        }
    except Exception as e:  # noqa: BLE001
        quarterly_data = None
        diagnostics["quarterly_data"] = {"status": "error", "error": repr(e)}
        logger.exception("get_quarterly_reports failed for symbol=%s", symbol)

    # 3. 获取与趋势相关的关键数据（资金流、筹码等），预留更多数据以便后续扩展
    fund_flow_data: Dict[str, Any] | None = None
    chip_data: Dict[str, Any] | None = None
    risk_data: Dict[str, Any] | None = None
    sentiment_data: Dict[str, Any] | None = None
    news_data: Dict[str, Any] | None = None
    research_data: Dict[str, Any] | None = None
    announcement_data: Dict[str, Any] | None = None

    try:
        fund_flow_data = uda.get_fund_flow_data(symbol, analysis_date=analysis_date_raw)
        diagnostics["fund_flow_data"] = {
            "status": "success",
            "has_data": fund_flow_data is not None,
        }
    except Exception as e:  # noqa: BLE001
        fund_flow_data = None
        diagnostics["fund_flow_data"] = {"status": "error", "error": repr(e)}
        logger.exception("get_fund_flow_data failed for symbol=%s", symbol)

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
            analysis_date=analysis_date_raw,
        )
        diagnostics["chip_data"] = {
            "status": "success",
            "has_data": bool(isinstance(chip_data, dict) and chip_data.get("data_success")),
        }
    except Exception as e:  # noqa: BLE001
        chip_data = None
        diagnostics["chip_data"] = {"status": "error", "error": repr(e)}
        logger.exception("get_chip_distribution_data failed for symbol=%s", symbol)

    # 风险数据
    try:
        risk_data = uda.get_risk_data(symbol, analysis_date=analysis_date_raw)
        diagnostics["risk_data"] = {
            "status": "success",
            "has_data": risk_data is not None,
        }
    except Exception as e:  # noqa: BLE001
        risk_data = None
        diagnostics["risk_data"] = {"status": "error", "error": repr(e)}
        logger.exception("get_risk_data failed for symbol=%s", symbol)

    # 仅在前端显式启用相应分析师时，获取情绪 / 新闻 / 研报 / 公告数据
    enabled = req.enabled_analysts or {}

    if enabled.get("sentiment") is True:
        try:
            sentiment_data = uda.get_market_sentiment_data(
                symbol,
                stock_data_with_indicators,
                analysis_date=analysis_date_raw,
            )
            diagnostics["sentiment_data"] = {
                "status": "success",
                "has_data": bool(
                    isinstance(sentiment_data, dict)
                    and sentiment_data.get("data_success")
                ),
            }
        except Exception as e:  # noqa: BLE001
            sentiment_data = None
            diagnostics["sentiment_data"] = {"status": "error", "error": repr(e)}
            logger.exception("get_market_sentiment_data failed for symbol=%s", symbol)

    if enabled.get("news") is True:
        try:
            news_data = uda.get_news_data(symbol, analysis_date=analysis_date_raw)
            diagnostics["news_data"] = {
                "status": "success",
                "has_data": bool(
                    isinstance(news_data, dict)
                    and news_data.get("data_success")
                ),
            }
        except Exception as e:  # noqa: BLE001
            news_data = None
            diagnostics["news_data"] = {"status": "error", "error": repr(e)}
            logger.exception("get_news_data failed for symbol=%s", symbol)

    if enabled.get("research") is True:
        try:
            research_data = uda.get_research_reports_data(
                symbol,
                days=180,
                analysis_date=analysis_date_raw,
            )
            diagnostics["research_data"] = {
                "status": "success",
                "has_data": bool(
                    isinstance(research_data, dict)
                    and research_data.get("data_success")
                    and research_data.get("report_count", 0) > 0
                ),
            }
        except Exception as e:  # noqa: BLE001
            research_data = None
            diagnostics["research_data"] = {"status": "error", "error": repr(e)}
            logger.exception("get_research_reports_data failed for symbol=%s", symbol)

    if enabled.get("announcement") is True:
        try:
            announcement_data = uda.get_announcement_data(
                symbol,
                days=30,
                analysis_date=analysis_date_raw,
            )
            diagnostics["announcement_data"] = {
                "status": "success",
                "has_data": bool(
                    isinstance(announcement_data, dict)
                    and announcement_data.get("data_success")
                ),
            }
        except Exception as e:  # noqa: BLE001
            announcement_data = None
            diagnostics["announcement_data"] = {"status": "error", "error": repr(e)}
            logger.exception("get_announcement_data failed for symbol=%s", symbol)

    # 4. 调用趋势分析 orchestrator（当前仅技术资金分析师规则化初版）
    trend_agents = StockTrendAnalysisAgents(model="deepseek-chat")

    horizons, analyst_results, risk_report, evolution = trend_agents.run_trend_analysis(
        stock_info=stock_info or {},
        stock_data_with_indicators=stock_data_with_indicators,
        indicators=indicators,
        financial_data=financial_data,
        fund_flow_data=fund_flow_data,
        quarterly_data=quarterly_data,
        risk_data=risk_data,
        sentiment_data=sentiment_data,
        news_data=news_data,
        research_data=research_data,
        announcement_data=announcement_data,
        chip_data=chip_data,
        enabled_analysts=req.enabled_analysts,
    )

    # 5. 落库：将最终预测与每位分析师结构化结果写入专用表
    from datetime import datetime as _dt
    from datetime import timezone as _tz

    try:
        if analysis_date_raw:
            ad = _dt.strptime(analysis_date_str, "%Y-%m-%d").replace(tzinfo=_tz.utc)
        else:
            ad = _dt.now(_tz.utc)

        analyst_rows: list[dict[str, Any]] = []
        for r in analyst_results:
            analyst_key_val: str
            try:
                raw_key = r.conclusion_json.get("analyst_key")  # type: ignore[union-attr]
                analyst_key_val = str(raw_key) if raw_key is not None else ""
            except Exception:  # noqa: BLE001
                analyst_key_val = ""

            if not analyst_key_val:
                name = r.name
                if "技术资金" in name:
                    analyst_key_val = "tech_capital"
                elif "基本面" in name:
                    analyst_key_val = "fundamental"
                elif "研报" in name:
                    analyst_key_val = "research"
                elif "公告" in name:
                    analyst_key_val = "announcement"
                elif "情绪" in name:
                    analyst_key_val = "sentiment"
                elif "新闻" in name:
                    analyst_key_val = "news"
                elif "风险" in name:
                    analyst_key_val = "risk"
                else:
                    analyst_key_val = "unknown"

            analyst_rows.append(
                {
                    "analyst_key": analyst_key_val,  # 当前只有一位分析师
                    "analyst_name": r.name,
                    "role": r.role,
                    "raw_text": r.raw_text,
                    "conclusion_json": r.conclusion_json,
                    "created_at": r.created_at,
                }
            )

        record_id = trend_analysis_repo.save_trend_analysis(
            symbol=symbol,
            analysis_date=ad,
            mode=req.mode,
            stock_info=stock_info or {},
            final_predictions=[h.model_dump() for h in horizons],
            prediction_evolution=[step.model_dump() for step in evolution],
            analyst_rows=analyst_rows,
        )
    except Exception:  # noqa: BLE001
        logger.exception("save_trend_analysis failed for symbol=%s", symbol)
        record_id = None

    rating = _compute_trend_rating_from_horizons(horizons)

    return StockTrendAnalysisResponse(
        ts_code=req.ts_code,
        analysis_date=analysis_date_str,
        mode=req.mode,
        horizons=horizons,
        analysts=analyst_results,
        risk_report=risk_report,
        prediction_evolution=evolution,
        record_id=record_id,
        data_fetch_diagnostics=diagnostics,
        technical_indicators=indicators if isinstance(indicators, dict) else None,
        rating=rating,
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


def _compute_trend_rating_from_horizons(horizons: Any) -> str | None:
    """Compute a simple trend rating from long-horizon expectation.

    The logic is based on the "long" horizon base_expectation_pct (%):

    - x >= 30   -> 强烈买入
    - 15 <= x < 30 -> 买入
    - 5  <= x < 15 -> 增持
    - -5 < x < 5   -> 中性/持有
    - -15 < x <= -5 -> 减持
    - x <= -15     -> 卖出/回避
    """

    # Normalise horizons to a list of dict-like objects
    if horizons is None:
        return None

    try:
        hs = list(horizons)
    except TypeError:
        return None

    long_item: Any | None = None
    for h in hs:
        try:
            if isinstance(h, dict):
                hz = h.get("horizon")
            else:
                hz = getattr(h, "horizon", None)
        except Exception:  # noqa: BLE001
            hz = None
        if hz == "long":
            long_item = h
            break

    if long_item is None:
        return None

    try:
        if isinstance(long_item, dict):
            raw = long_item.get("base_expectation_pct")
        else:
            raw = getattr(long_item, "base_expectation_pct", None)
        if raw is None:
            return None
        x = float(raw)
    except Exception:  # noqa: BLE001
        return None

    if x >= 30:
        return "强烈买入"
    if x >= 15:
        return "买入"
    if x >= 5:
        return "增持"
    if x > -5:
        return "中性/持有"
    if x > -15:
        return "减持"
    return "卖出/回避"


def get_trend_history_records(
    symbol_or_name: str | None,
    page: int = 1,
    page_size: int = 10,
    rating: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """分页查询趋势分析历史记录列表。

    rating 过滤逻辑基于 long horizon 的预期涨跌幅，在服务层按计算结果过滤。
    """

    raw_items = trend_analysis_repo.list_records(
        symbol_or_name=symbol_or_name or None,
        start_date=start_date or None,
        end_date=end_date or None,
    )

    items: list[dict[str, Any]] = []
    for r in raw_items:
        r_analysis_date = r.get("analysis_date")
        r_created_at = r.get("created_at")
        r_predictions = r.get("final_predictions")
        trend_rating = _compute_trend_rating_from_horizons(r_predictions) or "未知"

        if rating and trend_rating != rating:
            continue

        items.append(
            {
                "id": r["id"],
                "symbol": r.get("symbol", ""),
                "stock_name": r.get("stock_name", ""),
                "analysis_date": r_analysis_date.isoformat()
                if hasattr(r_analysis_date, "isoformat")
                else str(r_analysis_date)
                if r_analysis_date is not None
                else None,
                "mode": r.get("mode", ""),
                "rating": trend_rating,
                "created_at": r_created_at.isoformat()
                if hasattr(r_created_at, "isoformat")
                else str(r_created_at)
                if r_created_at is not None
                else None,
            }
        )

    total = len(items)
    page = max(1, int(page))
    page_size = max(1, min(int(page_size), 100))
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]

    return {"total": total, "items": page_items}


def get_trend_history_record_detail(record_id: int) -> StockTrendAnalysisResponse:
    payload = trend_analysis_repo.get_trend_analysis(record_id)
    if not payload:
        raise ValueError(f"trend analysis record not found: {record_id}")

    record = payload["record"]
    analysts_rows = payload.get("analysts", [])

    horizons_raw = record.get("final_predictions") or []
    evolution_raw = record.get("prediction_evolution") or []

    analysts_payload: list[dict[str, Any]] = []
    risk_payload: dict[str, Any] | None = None

    for row in analysts_rows:
        base = {
            "name": row.get("analyst_name", ""),
            "role": row.get("role", ""),
            "raw_text": row.get("raw_text", ""),
            "conclusion_json": row.get("conclusion_json") or {},
            "created_at": row.get("created_at"),
        }

        if (row.get("analyst_key") or "") == "risk" and risk_payload is None:
            risk_payload = base

        analysts_payload.append(base)

    r_analysis_date = record.get("analysis_date")
    analysis_date_str = (
        r_analysis_date.strftime("%Y-%m-%d")
        if hasattr(r_analysis_date, "strftime")
        else str(r_analysis_date)
    )

    rating_val = _compute_trend_rating_from_horizons(horizons_raw)

    return StockTrendAnalysisResponse(
        ts_code=str(record.get("ts_code") or ""),
        analysis_date=analysis_date_str,
        mode=str(record.get("mode") or "realtime"),
        horizons=horizons_raw,
        analysts=analysts_payload,
        risk_report=risk_payload,
        prediction_evolution=evolution_raw,
        record_id=int(record.get("id") or record_id),
        data_fetch_diagnostics=None,
        technical_indicators=None,
        rating=rating_val,
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


def _build_trend_summary_text(final_predictions: Any, rating: str | None) -> str:
    """Generate a human-readable summary for trend analysis horizons."""

    try:
        horizons = list(final_predictions or [])
    except TypeError:
        horizons = []

    label_map = {"1d": "1天", "1w": "1周", "1m": "1个月", "long": "长线"}

    lines: list[str] = []
    if rating:
        lines.append(f"综合趋势评级：{rating}")

    if horizons:
        lines.append("多周期预期涨跌概览：")
        for h in horizons:
            try:
                if isinstance(h, dict):
                    hz = h.get("horizon")
                    pct = h.get("base_expectation_pct")
                else:
                    hz = getattr(h, "horizon", None)
                    pct = getattr(h, "base_expectation_pct", None)
                if hz is None:
                    continue
                label = label_map.get(str(hz), str(hz))
                if isinstance(pct, (int, float)):
                    lines.append(f"- {label}：预期涨跌 {pct:.2f}%")
                else:
                    lines.append(f"- {label}：预期涨跌 不明确")
            except Exception:  # noqa: BLE001
                continue

    return "\n".join(lines) if lines else "暂无多周期预期数据。"


def generate_trend_pdf_from_record(record_id: int) -> tuple[bytes, str]:
    """根据 trend_analysis_records 记录生成趋势分析 PDF 报告。"""

    payload = trend_analysis_repo.get_trend_analysis(record_id)
    if not payload:
        raise ValueError(f"trend analysis record not found: {record_id}")

    record = payload["record"]
    analysts_rows = payload.get("analysts", [])

    stock_info = record.get("stock_info") or {}
    final_predictions = record.get("final_predictions") or []

    # 综合趋势评级与多周期摘要
    rating_val = _compute_trend_rating_from_horizons(final_predictions)
    summary_text = _build_trend_summary_text(final_predictions, rating_val)

    # 1. 构造各分析师的详细报告：优先使用 raw_text，若为空则回退到结论 JSON
    agents_results: dict[str, Any] = {}
    for row in analysts_rows:
        key_raw = row.get("analyst_key") or ""
        try:
            analyst_id = row.get("id")
            if not key_raw and analyst_id is not None:
                key_raw = f"analyst_{analyst_id}"
        except Exception:  # noqa: BLE001
            pass

        agent_key = str(key_raw or "analyst")
        agent_name = str(row.get("analyst_name") or row.get("role") or agent_key)

        raw_text = row.get("raw_text")
        analysis_text: str
        if raw_text:
            analysis_text = str(raw_text)
        else:
            cj = row.get("conclusion_json")
            try:
                analysis_text = json.dumps(cj, ensure_ascii=False, indent=2)
            except Exception:  # noqa: BLE001
                analysis_text = str(cj)

        agents_results[agent_key] = {
            "agent_name": agent_name,
            "analysis": analysis_text,
        }

    # 附加一个“趋势分析总结”条目，便于在 PDF 中展示整体结论
    if "trend" not in agents_results:
        agents_results["trend"] = {
            "agent_name": "趋势分析总结",
            "analysis": summary_text,
        }

    # 2. 构造“团队综合讨论”：包含参与分析师名单 + 演化过程摘要
    analyst_names = ", ".join(
        str(row.get("analyst_name")) for row in analysts_rows if row.get("analyst_name")
    )

    discussion_lines: list[str] = []
    if analyst_names:
        discussion_lines.append(f"参与分析师：{analyst_names}")

    evolution = record.get("prediction_evolution") or []
    try:
        steps = list(evolution or [])
    except TypeError:
        steps = []

    if steps:
        if discussion_lines:
            discussion_lines.append("")
        discussion_lines.append("分析与讨论过程：")
        for idx, step in enumerate(steps, start=1):
            title = None
            summary = None
            if isinstance(step, dict):
                title = step.get("title") or step.get("label") or step.get("stage")
                summary = (
                    step.get("summary")
                    or step.get("description")
                    or step.get("detail")
                )
            if not title:
                title = f"步骤 {idx}"
            if summary is None:
                summary = step
            try:
                summary_str = (
                    json.dumps(summary, ensure_ascii=False, indent=2)
                    if not isinstance(summary, (str, int, float))
                    else str(summary)
                )
            except Exception:  # noqa: BLE001
                summary_str = str(summary)
            discussion_lines.append(f"{idx}. {title}：{summary_str}")

    discussion_result = "\n".join(discussion_lines) if discussion_lines else ""

    decision_lines: list[str] = []
    if rating_val:
        decision_lines.append(f"综合趋势评级：{rating_val}")
    decision_lines.append(summary_text)
    final_decision = {"decision_text": "\n".join(decision_lines)}

    pdf_bytes = create_pdf_report(
        stock_info=stock_info,
        agents_results=agents_results,
        discussion_result=discussion_result,
        final_decision=final_decision,
    )

    symbol = stock_info.get("symbol") or record.get("ts_code") or "unknown"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"股票趋势分析报告_{symbol}_{ts}.pdf"
    return pdf_bytes, filename


def generate_trend_markdown_from_record(record_id: int) -> tuple[str, str]:
    """根据 trend_analysis_records 记录生成 Markdown 趋势分析报告。"""

    payload = trend_analysis_repo.get_trend_analysis(record_id)
    if not payload:
        raise ValueError(f"trend analysis record not found: {record_id}")

    record = payload["record"]
    analysts_rows = payload.get("analysts", [])

    stock_info = record.get("stock_info") or {}
    final_predictions = record.get("final_predictions") or []

    rating_val = _compute_trend_rating_from_horizons(final_predictions) or "未知"

    ts_code = str(record.get("ts_code") or stock_info.get("symbol") or "")
    name = str(stock_info.get("name") or "")
    r_analysis_date = record.get("analysis_date")
    analysis_date_str = (
        r_analysis_date.strftime("%Y-%m-%d")
        if hasattr(r_analysis_date, "strftime")
        else str(r_analysis_date)
    )

    summary_text = _build_trend_summary_text(final_predictions, rating_val)

    label_map = {"1d": "1天", "1w": "1周", "1m": "1个月", "long": "长线"}

    try:
        horizons = list(final_predictions or [])
    except TypeError:
        horizons = []

    md_lines: list[str] = []
    md_lines.append("# AI股票趋势分析报告")
    md_lines.append("")
    md_lines.append(f"**股票代码**：{ts_code}")
    md_lines.append(f"**股票名称**：{name}")
    md_lines.append(f"**分析日期**：{analysis_date_str}")
    md_lines.append(f"**趋势评级**：{rating_val}")
    md_lines.append("")
    md_lines.append("---")
    md_lines.append("")
    md_lines.append("## 多周期预期涨跌")
    md_lines.append("")
    if horizons:
        md_lines.append("| 周期 | 预期涨跌幅(%) |")
        md_lines.append("|------|----------------|")
        for h in horizons:
            try:
                if isinstance(h, dict):
                    hz = h.get("horizon")
                    pct = h.get("base_expectation_pct")
                else:
                    hz = getattr(h, "horizon", None)
                    pct = getattr(h, "base_expectation_pct", None)
                if hz is None:
                    continue
                label = label_map.get(str(hz), str(hz))
                if isinstance(pct, (int, float)):
                    md_lines.append(f"| {label} | {pct:.2f} |")
                else:
                    md_lines.append(f"| {label} | 不明确 |")
            except Exception:  # noqa: BLE001
                continue
    else:
        md_lines.append("暂无多周期预期数据。")

    md_lines.append("")
    md_lines.append("---")
    md_lines.append("")
    md_lines.append("## 趋势综合说明")
    md_lines.append("")
    md_lines.append(summary_text or "暂无说明。")
    md_lines.append("")

    if analysts_rows:
        md_lines.append("---")
        md_lines.append("")
        md_lines.append("## 分析师观点摘要")
        md_lines.append("")
        for row in analysts_rows:
            name_i = str(row.get("analyst_name") or "")
            role_i = str(row.get("role") or "")
            text_i = str(row.get("raw_text") or "")
            md_lines.append(f"### {name_i}")
            if role_i:
                md_lines.append("")
                md_lines.append(f"角色：{role_i}")
            md_lines.append("")
            md_lines.append(text_i or "暂无内容。")
            md_lines.append("")

    md_text = "\n".join(md_lines)

    symbol = stock_info.get("symbol") or ts_code or "unknown"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"股票趋势分析报告_{symbol}_{ts}.md"
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
