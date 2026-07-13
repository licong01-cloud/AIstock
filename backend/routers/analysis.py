from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse, PlainTextResponse
import io
from urllib.parse import quote

from ..services import analysis_service as analysis_service_module
from ..services.analysis_service import (
    analyze_stock,
    analyze_stock_trend,
    get_stock_context,
    get_realtime_quote,
    analyze_stocks_batch,
    generate_stock_pdf_from_record,
    generate_stock_markdown_from_record,
    get_history_records,
    delete_history_record,
    add_history_record_to_monitor_quick,
    get_history_record_detail,
    get_trend_history_records,
    get_trend_history_record_detail,
    generate_trend_pdf_from_record,
    generate_trend_markdown_from_record,
)
from ..models.analysis import (
    StockAnalysisRequest,
    StockAnalysisResponse,
    StockContextResponse,
    StockQuote,
    BatchStockAnalysisRequest,
    BatchStockAnalysisResponse,
    StockTrendAnalysisRequest,
    StockTrendAnalysisResponse,
)


router = APIRouter(prefix="/analysis", tags=["analysis"])


def get_stock_analysis_evidence_service():
    return analysis_service_module


@router.post("/stock", response_model=StockAnalysisResponse, summary="股票分析（多智能体）")
def analyze_stock_endpoint(req: StockAnalysisRequest) -> StockAnalysisResponse:
    """股票分析接口，复用统一数据访问与旧多智能体实现。"""

    return analyze_stock(req)


@router.post(
    "/stock/trend",
    response_model=StockTrendAnalysisResponse,
    summary="股票趋势分析（多周期概率预测）",
)
def analyze_stock_trend_endpoint(
    req: StockTrendAnalysisRequest,
) -> StockTrendAnalysisResponse:
    """股票趋势分析接口，复用统一数据访问与新趋势分析管线。"""

    return analyze_stock_trend(req)


@router.post("/stock/context", response_model=StockContextResponse, summary="股票概览与K线基础数据")
def stock_context_endpoint(req: StockAnalysisRequest) -> StockContextResponse:
    """返回单只股票的概览信息与基础K线数据。"""

    return get_stock_context(req)


@router.get("/stock/quote/{symbol}", response_model=StockQuote, summary="获取单只股票的实时行情（TDX）")
def stock_quote_endpoint(symbol: str) -> StockQuote:
    """轻量级实时行情接口，仅依赖 TDX 等实时源。

    - 主要用于历史详情中的“当前价格/涨跌幅”卡片；
    - 不会触发完整的统一数据获取或多智能体分析。
    """

    return get_realtime_quote(symbol)


@router.get("/stock/evidence/quote/{symbol}", summary="个股行情证据卡（只读）")
def stock_quote_evidence_endpoint(symbol: str, service=Depends(get_stock_analysis_evidence_service)) -> dict:
    return service.get_stock_quote_evidence(symbol)


@router.get("/stock/evidence/kline/{symbol}", summary="个股K线证据卡（只读）")
def stock_kline_evidence_endpoint(symbol: str, period: str = Query("1y"), analysis_date: str | None = Query(None), service=Depends(get_stock_analysis_evidence_service)) -> dict:
    return service.get_stock_kline_evidence(symbol, period=period, analysis_date=analysis_date)


@router.get("/stock/evidence/financials/{symbol}", summary="个股财务摘要证据卡（只读）")
def stock_financials_evidence_endpoint(symbol: str, analysis_date: str | None = Query(None), service=Depends(get_stock_analysis_evidence_service)) -> dict:
    return service.get_stock_financials_evidence(symbol, analysis_date=analysis_date)


@router.get("/stock/evidence/quarterly/{symbol}", summary="个股季报证据卡（只读）")
def stock_quarterly_evidence_endpoint(symbol: str, analysis_date: str | None = Query(None), service=Depends(get_stock_analysis_evidence_service)) -> dict:
    return service.get_stock_quarterly_evidence(symbol, analysis_date=analysis_date)


@router.get("/stock/evidence/margin-financing/{symbol}", summary="个股融资融券证据卡（只读）")
def stock_margin_financing_evidence_endpoint(symbol: str, analysis_date: str | None = Query(None), service=Depends(get_stock_analysis_evidence_service)) -> dict:
    return service.get_stock_margin_financing_evidence(symbol, analysis_date=analysis_date)


@router.get("/stock/evidence/fund-flow/{symbol}", summary="个股资金流向证据卡（只读）")
def stock_fund_flow_evidence_endpoint(symbol: str, analysis_date: str | None = Query(None), service=Depends(get_stock_analysis_evidence_service)) -> dict:
    return service.get_stock_fund_flow_evidence(symbol, analysis_date=analysis_date)


@router.get("/stock/evidence/technicals/{symbol}", summary="个股技术指标证据卡（只读）")
def stock_technicals_evidence_endpoint(symbol: str, period: str = Query("1y"), analysis_date: str | None = Query(None), service=Depends(get_stock_analysis_evidence_service)) -> dict:
    return service.get_stock_technicals_evidence(symbol, period=period, analysis_date=analysis_date)


@router.post(
    "/stock/batch",
    response_model=BatchStockAnalysisResponse,
    summary="批量股票分析（顺序/并行）",
)
def analyze_stock_batch_endpoint(
    req: BatchStockAnalysisRequest,
) -> BatchStockAnalysisResponse:
    """批量股票分析接口。

    - 对应旧版 app.py 中的 run_batch_analysis；
    - 支持 sequential / parallel 两种模式；
    - 每只股票内部复用单股 analyze_stock，保证分析逻辑完全一致。
    """

    return analyze_stocks_batch(req)


@router.get("/stock/report/pdf/{record_id}")
def download_stock_report_pdf(record_id: int) -> StreamingResponse:
    """下载指定分析记录对应的 PDF 报告。

    - record_id 来自单股分析返回的 record_id 字段；
    - 内容基于 app.analysis_records 中保存的 stock_info / agents_results 等生成。
    """

    try:
        pdf_bytes, filename = generate_stock_pdf_from_record(record_id)
    except ValueError as e:  # 记录不存在
        raise HTTPException(status_code=404, detail=str(e)) from e

    # 使用 RFC5987 风格的 UTF-8 百分号编码文件名，避免非 ASCII 字符导致的 header 编码错误
    safe_filename = quote(filename)
    disposition = f"attachment; filename*=UTF-8''{safe_filename}"

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={
            "Content-Disposition": disposition,
        },
    )


@router.get(
    "/stock/report/markdown/{record_id}",
    response_class=PlainTextResponse,
)
def download_stock_report_markdown(record_id: int) -> PlainTextResponse:
    """下载指定分析记录对应的 Markdown 报告文本。"""

    try:
        md_text, filename = generate_stock_markdown_from_record(record_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    # 同样对 Markdown 报告的文件名做 UTF-8 百分号编码，保证 header 为 ASCII
    safe_filename = quote(filename)
    disposition = f"attachment; filename*=UTF-8''{safe_filename}"

    return PlainTextResponse(
        content=md_text,
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": disposition,
        },
    )


@router.get("/stock/trend/report/pdf/{record_id}")
def download_trend_report_pdf(record_id: int) -> StreamingResponse:
    """下载指定趋势分析记录对应的 PDF 报告。"""

    try:
        pdf_bytes, filename = generate_trend_pdf_from_record(record_id)
    except ValueError as e:  # 记录不存在
        raise HTTPException(status_code=404, detail=str(e)) from e

    safe_filename = quote(filename)
    disposition = f"attachment; filename*=UTF-8''{safe_filename}"

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={
            "Content-Disposition": disposition,
        },
    )


@router.get(
    "/stock/trend/report/markdown/{record_id}",
    response_class=PlainTextResponse,
)
def download_trend_report_markdown(
    record_id: int,
) -> PlainTextResponse:
    """下载指定趋势分析记录对应的 Markdown 报告文本。"""

    try:
        md_text, filename = generate_trend_markdown_from_record(record_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    safe_filename = quote(filename)
    disposition = f"attachment; filename*=UTF-8''{safe_filename}"

    return PlainTextResponse(
        content=md_text,
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": disposition,
        },
    )


@router.get("/history")
def list_history_records(
    q: str | None = Query(None, description="按股票代码或名称模糊搜索"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    rating: str | None = Query(
        None,
        description="按投资评级精确过滤，如 买入/持有/卖出/强烈买入/强烈卖出/未知",
    ),
    start_date: str | None = Query(
        None,
        description="起始分析日期 (YYYY-MM-DD)，按 analysis_date::date 过滤",
    ),
    end_date: str | None = Query(
        None,
        description="结束分析日期 (YYYY-MM-DD)，按 analysis_date::date 过滤",
    ),
) -> dict:
    """历史分析记录列表（分页 + 搜索）。

    - 语义对应旧版 display_history_records；
    - 仅返回列表展示所需的摘要信息。
    """

    return get_history_records(
        symbol_or_name=q,
        page=page,
        page_size=page_size,
        rating=rating,
        start_date=start_date,
        end_date=end_date,
    )


@router.delete("/history/{record_id}")
def delete_history_record_endpoint(record_id: int) -> dict:
    """删除单条历史分析记录。

    - 语义对应旧版 db.delete_record(record_id)。
    """

    ok = delete_history_record(record_id)
    if not ok:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True}


@router.get("/history/{record_id}", response_model=StockAnalysisResponse)
def get_history_record_detail_endpoint(record_id: int) -> StockAnalysisResponse:
    try:
        return get_history_record_detail(record_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/history/{record_id}/monitor_quick_add")
def history_quick_add_monitor(record_id: int) -> dict:
    """将指定历史记录一键加入监测。

    - 基于历史记录的 final_decision 自动提取关键价位和评级；
    - 调用现有 pg_monitor_repo / monitor_service 实现监测逻辑。
    """

    try:
        return add_history_record_to_monitor_quick(record_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/trend/history")
def list_trend_history_records(
    q: str | None = Query(None, description="按股票代码或名称模糊搜索"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    rating: str | None = Query(
        None,
        description="按趋势评级精确过滤，如 强烈买入/买入/增持/中性/持有/减持/卖出/回避/未知",
    ),
    start_date: str | None = Query(
        None,
        description="起始分析日期 (YYYY-MM-DD)",
    ),
    end_date: str | None = Query(
        None,
        description="结束分析日期 (YYYY-MM-DD)",
    ),
) -> dict:
    """趋势分析历史记录列表（分页 + 搜索）。"""

    return get_trend_history_records(
        symbol_or_name=q,
        page=page,
        page_size=page_size,
        rating=rating,
        start_date=start_date,
        end_date=end_date,
    )


@router.get(
    "/trend/history/{record_id}",
    response_model=StockTrendAnalysisResponse,
)
def get_trend_history_record_detail_endpoint(
    record_id: int,
) -> StockTrendAnalysisResponse:
    try:
        return get_trend_history_record_detail(record_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
