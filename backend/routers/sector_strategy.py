from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from sector_strategy_data import SectorStrategyDataFetcher
from sector_strategy_engine import SectorStrategyEngine
from sector_strategy_markdown import generate_sector_markdown_report
from sector_strategy_pdf import SectorStrategyPDFGenerator


router = APIRouter(prefix="/sector-strategy", tags=["sector-strategy"])


class SectorStrategyAnalyzeRequest(BaseModel):
    """智策板块综合分析请求参数。

    目前仅暴露 AI 模型选择，后续如需支持日期/缓存策略等，可在保持
    与旧版语义一致的前提下扩展字段。
    """

    model: str = "deepseek-chat"


class SectorStrategyAnalyzeResponse(BaseModel):
    """智策板块综合分析响应。

    结构尽量贴合 SectorStrategyEngine.run_comprehensive_analysis 的返回：
    - success / error / timestamp
    - agents_analysis: 四位分析师的详细报告
    - comprehensive_report: 综合研判长文本
    - final_predictions: 板块多空 / 轮动 / 热度 等 JSON 结构
    - cache_meta: 缓存元信息（是否来自缓存、提示文案、数据时间戳）
    - saved_report: 刚刚保存到 SQLite 的摘要信息，供前端展示
    - data_summary: 为新前端补充的市场数据摘要（不影响旧版逻辑）
    """

    success: bool
    error: Optional[str] = None
    timestamp: Optional[str] = None
    agents_analysis: Dict[str, Any] = {}
    comprehensive_report: Optional[str] = None
    final_predictions: Dict[str, Any] = {}
    cache_meta: Optional[Dict[str, Any]] = None
    saved_report: Optional[Dict[str, Any]] = None
    report_id: Optional[int] = None
    data_summary: Optional[Dict[str, Any]] = None

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"


class SectorStrategyExportRequest(BaseModel):
    result: SectorStrategyAnalyzeResponse

    class Config:
        arbitrary_types_allowed = True


class SectorStrategyHistoryItem(BaseModel):
    id: int
    created_at: str
    data_date_range: Optional[str] = None
    summary: Optional[str] = None
    confidence_score: Optional[float] = None
    risk_level: Optional[str] = None
    market_outlook: Optional[str] = None


class SectorStrategyHistoryListResponse(BaseModel):
    items: List[SectorStrategyHistoryItem]


class SectorStrategyHistoryDetailResponse(BaseModel):
    report: Dict[str, Any]

    class Config:
        arbitrary_types_allowed = True


@router.post("/analyze", response_model=SectorStrategyAnalyzeResponse, summary="运行智策板块综合分析")
async def analyze_sector_strategy(req: SectorStrategyAnalyzeRequest) -> SectorStrategyAnalyzeResponse:
    """运行一轮完整的智策板块分析。

    - 复用旧版 SectorStrategyDataFetcher 和 SectorStrategyEngine；
    - 额外补充 data_summary 和 cache_meta，方便新前端展示市场概况与缓存提示；
    - 保持与旧版 run_sector_strategy_analysis 语义一致。
    """

    # 1. 获取板块数据（带缓存回退）
    fetcher = SectorStrategyDataFetcher()
    data = fetcher.get_cached_data_with_fallback()

    if not data.get("success"):
        raise HTTPException(
            status_code=500,
            detail=str(data.get("error") or "无法获取板块数据"),
        )

    # 2. 运行综合研判引擎
    engine = SectorStrategyEngine(model=req.model)
    result = engine.run_comprehensive_analysis(data)

    # 3. 补充缓存元信息（沿用旧 UI 中的提示语义）
    if data.get("from_cache") or data.get("cache_warning"):
        result["cache_meta"] = {
            "from_cache": bool(data.get("from_cache")),
            "cache_warning": data.get("cache_warning", ""),
            "data_timestamp": data.get("timestamp"),
        }

    # 4. 构造市场数据摘要，避免在分析报告中存入全部原始数据
    market = data.get("market_overview") or {}
    sectors = data.get("sectors") or {}
    concepts = data.get("concepts") or {}
    result["data_summary"] = {
        "market_overview": market,
        "sector_count": len(sectors),
        "concept_count": len(concepts),
    }

    return SectorStrategyAnalyzeResponse(**result)


@router.post(
    "/export/markdown",
    summary="导出智策板块Markdown报告",
)
async def export_sector_strategy_markdown(
    payload: SectorStrategyExportRequest,
) -> Response:
    result_dict = payload.result.dict()
    content = generate_sector_markdown_report(result_dict)
    ts = result_dict.get("timestamp") or "report"
    safe_ts = str(ts).replace(":", "").replace(" ", "_")
    filename = f"sector_strategy_{safe_ts}.md"
    return Response(
        content=content,
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
        },
    )


@router.post(
    "/export/pdf",
    summary="导出智策板块PDF报告",
)
async def export_sector_strategy_pdf(
    payload: SectorStrategyExportRequest,
) -> FileResponse:
    result_dict = payload.result.dict()
    generator = SectorStrategyPDFGenerator()
    pdf_path = generator.generate_pdf(result_dict)
    return FileResponse(
        path=pdf_path,
        media_type="application/pdf",
        filename=pdf_path.split("/")[-1].split("\\")[-1],
    )


@router.get(
    "/history",
    response_model=SectorStrategyHistoryListResponse,
    summary="智策板块历史报告列表",
)
async def list_sector_strategy_history(
    limit: int = Query(20, ge=1, le=100, description="返回的历史报告数量上限"),
) -> SectorStrategyHistoryListResponse:
    """获取最近若干份智策板块历史报告。

    语义对应旧版 `display_history_tab` 中的 `engine.get_historical_reports`，
    但这里只返回列表摘要字段，详情通过 /history/{id} 单独获取。
    """

    engine = SectorStrategyEngine()
    reports = engine.get_historical_reports(limit=limit)

    items: List[SectorStrategyHistoryItem] = []
    try:
        import pandas as pd  # type: ignore

        if reports is not None and isinstance(reports, pd.DataFrame) and not reports.empty:
            for _, row in reports.iterrows():
                try:
                    items.append(
                        SectorStrategyHistoryItem(
                            id=int(row.get("id")),
                            created_at=str(row.get("created_at") or ""),
                            data_date_range=str(row.get("data_date_range") or ""),
                            summary=str(row.get("summary") or ""),
                            confidence_score=(
                                float(row.get("confidence_score"))
                                if row.get("confidence_score") is not None
                                else None
                            ),
                            risk_level=(
                                str(row.get("risk_level")) if row.get("risk_level") is not None else None
                            ),
                            market_outlook=(
                                str(row.get("market_outlook"))
                                if row.get("market_outlook") is not None
                                else None
                            ),
                        )
                    )
                except Exception:  # noqa: BLE001
                    continue
    except Exception:  # noqa: BLE001
        items = []

    return SectorStrategyHistoryListResponse(items=items)


@router.get(
    "/history/{report_id}",
    response_model=SectorStrategyHistoryDetailResponse,
    summary="获取单份智策板块历史报告详情",
)
async def get_sector_strategy_history_detail(report_id: int) -> SectorStrategyHistoryDetailResponse:
    """获取单条历史报告完整内容。

    返回的数据结构与旧版 `SectorStrategyDatabase.get_analysis_report` 一致，
    其中 `analysis_content_parsed` 字段可直接作为前端的 `sector_strategy_result`，
    以复用同一套渲染逻辑。
    """

    engine = SectorStrategyEngine()
    detail = engine.get_report_detail(report_id)
    if not detail:
        raise HTTPException(status_code=404, detail="report not found")
    return SectorStrategyHistoryDetailResponse(report=detail)


@router.delete(
    "/history/{report_id}",
    summary="删除一条智策板块历史报告记录",
)
async def delete_sector_strategy_history(report_id: int) -> Dict[str, Any]:
    """删除单条历史报告。

    语义对应旧版 `SectorStrategyEngine.delete_report`。
    """

    engine = SectorStrategyEngine()
    ok = engine.delete_report(report_id)
    if not ok:
        raise HTTPException(status_code=404, detail="report not found")
    return {"success": True}
