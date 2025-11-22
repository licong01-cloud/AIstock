from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse, PlainTextResponse
import io
from urllib.parse import quote

from ..services.analysis_service import (
    analyze_stock,
    get_stock_context,
    analyze_stocks_batch,
    generate_stock_pdf_from_record,
    generate_stock_markdown_from_record,
    get_history_records,
    delete_history_record,
    add_history_record_to_monitor_quick,
    get_history_record_detail,
)
from ..models.analysis import (
    StockAnalysisRequest,
    StockAnalysisResponse,
    StockContextResponse,
    BatchStockAnalysisRequest,
    BatchStockAnalysisResponse,
)


router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post("/stock", response_model=StockAnalysisResponse, summary="股票分析（多智能体）")
async def analyze_stock_endpoint(req: StockAnalysisRequest) -> StockAnalysisResponse:
    """股票分析接口，复用统一数据访问与旧多智能体实现。"""

    return analyze_stock(req)


@router.post("/stock/context", response_model=StockContextResponse, summary="股票概览与K线基础数据")
async def stock_context_endpoint(req: StockAnalysisRequest) -> StockContextResponse:
    """返回单只股票的概览信息与基础K线数据。"""

    return get_stock_context(req)


@router.post(
    "/stock/batch",
    response_model=BatchStockAnalysisResponse,
    summary="批量股票分析（顺序/并行）",
)
async def analyze_stock_batch_endpoint(
    req: BatchStockAnalysisRequest,
) -> BatchStockAnalysisResponse:
    """批量股票分析接口。

    - 对应旧版 app.py 中的 run_batch_analysis；
    - 支持 sequential / parallel 两种模式；
    - 每只股票内部复用单股 analyze_stock，保证分析逻辑完全一致。
    """

    return analyze_stocks_batch(req)


@router.get("/stock/report/pdf/{record_id}")
async def download_stock_report_pdf(record_id: int) -> StreamingResponse:
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
async def download_stock_report_markdown(record_id: int) -> PlainTextResponse:
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


@router.get("/history")
async def list_history_records(
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
async def delete_history_record_endpoint(record_id: int) -> dict:
    """删除单条历史分析记录。

    - 语义对应旧版 db.delete_record(record_id)。
    """

    ok = delete_history_record(record_id)
    if not ok:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True}


@router.get("/history/{record_id}", response_model=StockAnalysisResponse)
async def get_history_record_detail_endpoint(record_id: int) -> StockAnalysisResponse:
    try:
        return get_history_record_detail(record_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/history/{record_id}/monitor_quick_add")
async def history_quick_add_monitor(record_id: int) -> dict:
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
