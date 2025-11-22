from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from longhubang_engine import LonghubangEngine
from longhubang_db import LonghubangDatabase
from longhubang_markdown import generate_longhubang_markdown_report
from longhubang_pdf import LonghubangPDFGenerator


router = APIRouter(prefix="/longhubang", tags=["longhubang"])


class LonghubangAnalyzeRequest(BaseModel):
    """智瞰龙虎综合分析请求参数。

    - mode: "date" 表示按指定交易日分析；"recent_days" 表示分析最近 N 天数据。
    - date: 指定的交易日期（YYYY-MM-DD），仅在 mode="date" 时生效。
    - days: 最近 N 天，默认 1 天。
    - model: 使用的大模型名称，对应旧版 model_config 中的键。
    """

    mode: str = "recent_days"  # "date" | "recent_days"
    date: Optional[str] = None
    days: int = 1
    model: str = "deepseek-chat"


class LonghubangAnalyzeResponse(BaseModel):
    """智瞰龙虎综合分析响应。

    结构贴合 LonghubangEngine.run_comprehensive_analysis 的返回：
    - success / error / timestamp
    - data_info: 龙虎榜数据统计信息
    - agents_analysis: 5 位 AI 分析师报告
    - final_report: 首席策略师生成的综合报告摘要
    - scoring_ranking: AI 智能评分排名（列表形式，已可序列化）
    - recommended_stocks: 推荐股票列表
    - report_id: 保存到 SQLite 的历史报告 ID
    """

    success: bool
    error: Optional[str] = None
    timestamp: Optional[str] = None
    data_info: Dict[str, Any] = {}
    agents_analysis: Dict[str, Any] = {}
    final_report: Dict[str, Any] = {}
    scoring_ranking: List[Dict[str, Any]] = []
    recommended_stocks: List[Dict[str, Any]] = []
    report_id: Optional[int] = None

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"


class LonghubangHistoryItem(BaseModel):
    id: int
    analysis_date: str
    data_date_range: Optional[str] = None
    summary: Optional[str] = None
    created_at: Optional[str] = None


class LonghubangHistoryListResponse(BaseModel):
    items: List[LonghubangHistoryItem]


class LonghubangHistoryDetailResponse(BaseModel):
    report: Dict[str, Any]

    class Config:
        arbitrary_types_allowed = True


class LonghubangStatsResponse(BaseModel):
    stats: Dict[str, Any]


class LonghubangExportRequest(BaseModel):
    result: LonghubangAnalyzeResponse

    class Config:
        arbitrary_types_allowed = True


@router.post(
    "/analyze",
    response_model=LonghubangAnalyzeResponse,
    summary="运行智瞰龙虎综合分析",
)
async def analyze_longhubang(req: LonghubangAnalyzeRequest) -> LonghubangAnalyzeResponse:
    """运行一轮完整的智瞰龙虎分析。

    复用旧版 LonghubangEngine，支持：
    - 按指定日期分析（mode="date" + date）
    - 按最近 N 天分析（mode="recent_days" + days）
    """

    engine = LonghubangEngine(model=req.model)

    used_days: int | None = None

    if req.mode == "date":
        if not req.date:
            raise HTTPException(status_code=400, detail="mode=date 时必须提供 date 参数")
        # 指定日期模式固定视为 1 天窗口
        used_days = 1
        result = engine.run_comprehensive_analysis(date=req.date, days=1)
    else:
        # 默认按最近 N 天分析
        days = req.days if req.days and req.days > 0 else 1
        used_days = days
        result = engine.run_comprehensive_analysis(date=None, days=days)

    # 记录分析元信息，便于导出报告中展示
    try:  # noqa: BLE001
        result["analysis_meta"] = {
            "mode": req.mode,
            "date": req.date,
            "days": used_days,
        }
    except Exception:
        # 元信息记录失败不影响主流程
        pass

    if not result.get("success"):
        raise HTTPException(
            status_code=500,
            detail=str(result.get("error") or "龙虎榜分析失败"),
        )

    return LonghubangAnalyzeResponse(**result)


@router.get(
    "/history",
    response_model=LonghubangHistoryListResponse,
    summary="智瞰龙虎历史分析报告列表",
)
async def list_longhubang_history(
    limit: int = Query(20, ge=1, le=100, description="返回的历史报告数量上限"),
) -> LonghubangHistoryListResponse:
    """获取最近若干份智瞰龙虎历史报告列表。

    语义对应旧版 UI 中的 engine.get_historical_reports。
    """

    engine = LonghubangEngine()
    reports = engine.get_historical_reports(limit=limit)

    items: List[LonghubangHistoryItem] = []
    try:
        import pandas as pd  # type: ignore

        if reports is not None and isinstance(reports, pd.DataFrame) and not reports.empty:
            for _, row in reports.iterrows():
                try:
                    items.append(
                        LonghubangHistoryItem(
                            id=int(row.get("id")),
                            analysis_date=str(row.get("analysis_date") or ""),
                            data_date_range=str(row.get("data_date_range") or ""),
                            summary=str(row.get("summary") or ""),
                            created_at=str(row.get("created_at") or ""),
                        )
                    )
                except Exception:  # noqa: BLE001
                    continue
    except Exception:  # noqa: BLE001
        items = []

    return LonghubangHistoryListResponse(items=items)


@router.get(
    "/history/{report_id}",
    response_model=LonghubangHistoryDetailResponse,
    summary="获取单份智瞰龙虎历史报告详情",
)
async def get_longhubang_history_detail(report_id: int) -> LonghubangHistoryDetailResponse:
    """获取单条历史龙虎榜分析报告的完整内容。

    返回的数据结构与 LonghubangDatabase.get_analysis_report 一致，
    其中 analysis_content_parsed 字段（如存在）可作为前端复用的结构化结果。
    """

    engine = LonghubangEngine()
    detail = engine.get_report_detail(report_id)
    if not detail:
        raise HTTPException(status_code=404, detail="report not found")

    return LonghubangHistoryDetailResponse(report=detail)


@router.delete(
    "/history/{report_id}",
    summary="删除一条智瞰龙虎历史报告记录",
)
async def delete_longhubang_history(report_id: int) -> Dict[str, Any]:
    """删除单条历史龙虎榜分析报告。

    语义对应旧版 LonghubangDatabase.delete_analysis_report。
    """

    db = LonghubangDatabase()
    ok = db.delete_analysis_report(report_id)
    if not ok:
        raise HTTPException(status_code=404, detail="report not found")

    return {"success": True}


@router.get(
    "/stats",
    response_model=LonghubangStatsResponse,
    summary="获取智瞰龙虎数据库统计信息",
)
async def get_longhubang_stats() -> LonghubangStatsResponse:
    """获取龙虎榜历史库统计信息。

    对应旧版 engine.get_statistics，用于前端“数据统计”标签页展示。"""

    engine = LonghubangEngine()
    stats = engine.get_statistics() or {}

    # 近30天活跃游资与热门股票排名，贴近旧版 display_statistics_tab 实现
    try:  # noqa: BLE001
        import pandas as pd  # type: ignore

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        top_youzi_df = engine.get_top_youzi(start_date, end_date, limit=20)
        if top_youzi_df is not None and isinstance(top_youzi_df, pd.DataFrame):
            stats["top_youzi"] = top_youzi_df.to_dict("records")

        top_stocks_df = engine.get_top_stocks(start_date, end_date, limit=20)
        if top_stocks_df is not None and isinstance(top_stocks_df, pd.DataFrame):
            stats["top_stocks"] = top_stocks_df.to_dict("records")
    except Exception:
        # 统计扩展失败不应影响基础统计返回
        pass

    return LonghubangStatsResponse(stats=stats)


@router.post(
    "/export/markdown",
    summary="导出智瞰龙虎 Markdown 报告",
)
async def export_longhubang_markdown(
    payload: LonghubangExportRequest,
) -> Response:
    """将当前智瞰龙虎分析结果导出为 Markdown 文本。

    前端传入的 result 结构与 LonghubangAnalyzeResponse 一致，
    这里直接复用 generate_longhubang_markdown_report 生成报告内容。
    """
    result_dict = payload.result.dict()

    # 若存在 report_id，则从数据库补充数据日期范围信息
    report_id = result_dict.get("report_id")
    if report_id:
        try:  # noqa: BLE001
            db = LonghubangDatabase()
            detail = db.get_analysis_report(report_id)
            if detail and detail.get("data_date_range"):
                result_dict.setdefault("data_date_range", detail.get("data_date_range"))
        except Exception:
            pass

    content = generate_longhubang_markdown_report(result_dict)
    ts = result_dict.get("timestamp") or datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_ts = str(ts).replace(":", "").replace(" ", "_")
    filename = f"longhubang_{safe_ts}.md"
    return Response(
        content=content,
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
        },
    )


@router.post(
    "/export/pdf",
    summary="导出智瞰龙虎 PDF 报告",
)
async def export_longhubang_pdf(
    payload: LonghubangExportRequest,
) -> FileResponse:
    """将当前智瞰龙虎分析结果导出为 PDF 报告文件。"""
    result_dict = payload.result.dict()

    # 同样尝试补充数据日期范围，便于 PDF 报告展示
    report_id = result_dict.get("report_id")
    if report_id:
        try:  # noqa: BLE001
            db = LonghubangDatabase()
            detail = db.get_analysis_report(report_id)
            if detail and detail.get("data_date_range"):
                result_dict.setdefault("data_date_range", detail.get("data_date_range"))
        except Exception:
            pass

    generator = LonghubangPDFGenerator()
    pdf_path = generator.generate_pdf(result_dict)
    return FileResponse(
        path=pdf_path,
        media_type="application/pdf",
        filename=pdf_path.split("/")[-1].split("\\")[-1],
    )
