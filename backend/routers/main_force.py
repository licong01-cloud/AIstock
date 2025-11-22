from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

# 直接复用旧版主力选股分析器和批量历史数据库
from main_force_analysis import MainForceAnalyzer
from main_force_batch_db import batch_db
from main_force_pdf_generator import (
    generate_html_content,
    generate_main_force_markdown_report,
)


router = APIRouter(prefix="/main-force", tags=["main-force"])


class MainForceAnalyzeRequest(BaseModel):
    """主力选股分析请求参数。

    前端按照旧 UI 的语义组装参数：
    - 要么传入 days_ago（最近N天），要么传入 start_date（形如"2025年10月1日"）。
    - final_n: 最终精选只数。
    - max_range_change: 最大区间涨跌幅限制（%）。
    - min_market_cap / max_market_cap: 市值范围（亿）。
    - market: "all" / "asr" / "bse"。
    - model: DeepSeek 模型标识，例如 "deepseek-chat" 或 "deepseek-reasoner"。
    """

    start_date: Optional[str] = None
    days_ago: Optional[int] = None
    final_n: int = 5
    max_range_change: float = 30.0
    min_market_cap: float = 50.0
    max_market_cap: float = 5000.0
    market: str = "all"
    model: str = "deepseek-chat"


class MainForceRecommendation(BaseModel):
    rank: int
    symbol: str
    name: str
    reasons: List[str] = []
    highlights: Optional[str] = None
    risks: Optional[str] = None
    position: Optional[str] = None
    investment_period: Optional[str] = None
    stock_data: Dict[str, Any] = {}


class MainForceAnalyzeResponse(BaseModel):
    success: bool
    error: Optional[str]
    total_stocks: int
    filtered_stocks: int
    params: Dict[str, Any]
    final_recommendations: List[MainForceRecommendation]
    fund_flow_analysis: Optional[str]
    industry_analysis: Optional[str]
    fundamental_analysis: Optional[str]
    # 候选股票明细（与旧版 raw_stocks 对应），用于前端表格与 CSV 导出
    candidates: List[Dict[str, Any]]
    # 生成好的 Markdown/HTML 报告内容，便于前端直接提供下载
    report_markdown: Optional[str] = None
    report_html: Optional[str] = None


class MainForceBatchHistorySummary(BaseModel):
    total_records: int
    total_stocks_analyzed: int
    total_success: int
    total_failed: int
    average_time: float
    success_rate: float


class MainForceBatchHistoryRecord(BaseModel):
    id: int
    analysis_date: str
    batch_count: int
    analysis_mode: str
    success_count: int
    failed_count: int
    total_time: float
    created_at: str
    # 完整结果列表，结构与旧版 SQLite 中保存的一致
    results: List[Dict[str, Any]]


class MainForceBatchHistoryListResponse(BaseModel):
    items: List[MainForceBatchHistoryRecord]


@router.post("/analyze", response_model=MainForceAnalyzeResponse, summary="执行主力选股整体分析")
async def analyze_main_force(req: MainForceAnalyzeRequest) -> MainForceAnalyzeResponse:
    """运行一轮完整的主力选股分析。

    该接口是对旧版 `MainForceAnalyzer.run_full_analysis` 的包装，保持参数语义一致，
    并额外返回三位 AI 分析师的整体报告以及候选股票明细，供前端完整还原 UI。
    """

    analyzer = MainForceAnalyzer(model=req.model)

    result = analyzer.run_full_analysis(
        start_date=req.start_date,
        days_ago=req.days_ago,
        final_n=req.final_n,
        max_range_change=req.max_range_change,
        min_market_cap=req.min_market_cap,
        max_market_cap=req.max_market_cap,
        market=req.market,
    )

    success = bool(result.get("success"))
    error = result.get("error") if not success else None

    raw_recs = result.get("final_recommendations") or []
    recommendations: List[MainForceRecommendation] = []
    for idx, rec in enumerate(raw_recs):
        if not isinstance(rec, dict):
            continue
        symbol = str(rec.get("symbol") or rec.get("code") or "")
        name = str(rec.get("name") or "")
        rank_val = rec.get("rank")
        try:
            rank_int = int(rank_val) if rank_val is not None else idx + 1
        except Exception:  # noqa: BLE001
            rank_int = idx + 1

        recommendations.append(
            MainForceRecommendation(
                rank=rank_int,
                symbol=symbol,
                name=name,
                reasons=list(rec.get("reasons") or []),
                highlights=rec.get("highlights"),
                risks=rec.get("risks"),
                position=rec.get("position"),
                investment_period=rec.get("investment_period"),
                stock_data=dict(rec.get("stock_data") or {}),
            )
        )

    # 候选股票 DataFrame -> list[dict]
    candidates: List[Dict[str, Any]] = []
    raw_df = getattr(analyzer, "raw_stocks", None)
    try:
        import pandas as pd  # type: ignore

        if raw_df is not None and isinstance(raw_df, pd.DataFrame):
            candidates = raw_df.to_dict(orient="records")
    except Exception:  # noqa: BLE001
        candidates = []

    # 生成 Markdown / HTML 报告，保持与旧版 PDF/Markdown 下载区的语义一致
    report_md: Optional[str]
    report_html: Optional[str]
    try:
        report_md = generate_main_force_markdown_report(analyzer, result)
        report_html = generate_html_content(report_md)
    except Exception:  # noqa: BLE001
        report_md = None
        report_html = None

    return MainForceAnalyzeResponse(
        success=success,
        error=error,
        total_stocks=int(result.get("total_stocks") or 0),
        filtered_stocks=int(result.get("filtered_stocks") or 0),
        params=dict(result.get("params") or {}),
        final_recommendations=recommendations,
        fund_flow_analysis=getattr(analyzer, "fund_flow_analysis", None),
        industry_analysis=getattr(analyzer, "industry_analysis", None),
        fundamental_analysis=getattr(analyzer, "fundamental_analysis", None),
        candidates=candidates,
        report_markdown=report_md,
        report_html=report_html,
    )


@router.get(
    "/batch/history/summary",
    response_model=MainForceBatchHistorySummary,
    summary="主力选股批量分析历史统计",
)
async def batch_history_summary() -> MainForceBatchHistorySummary:
    """获取旧版主力批量分析历史的统计信息。

    直接复用 `main_force_batch_db.batch_db.get_statistics()`，方便前端在
    “📚 批量分析历史” 页展示汇总指标。
    """

    stats = batch_db.get_statistics()
    return MainForceBatchHistorySummary(**stats)


@router.get(
    "/batch/history",
    response_model=MainForceBatchHistoryListResponse,
    summary="主力选股批量分析历史记录列表",
)
async def batch_history(limit: int = 50) -> MainForceBatchHistoryListResponse:
    """获取批量分析历史记录列表。

    注意：results 字段可能较大，前端在展示时可做按需裁剪。
    """

    records = batch_db.get_all_history(limit=limit)
    # 直接透传字段结构
    items: List[MainForceBatchHistoryRecord] = []
    for r in records:
        if not isinstance(r, dict):
            continue
        items.append(
            MainForceBatchHistoryRecord(
                id=int(r.get("id")),
                analysis_date=str(r.get("analysis_date")),
                batch_count=int(r.get("batch_count") or 0),
                analysis_mode=str(r.get("analysis_mode") or ""),
                success_count=int(r.get("success_count") or 0),
                failed_count=int(r.get("failed_count") or 0),
                total_time=float(r.get("total_time") or 0.0),
                created_at=str(r.get("created_at")),
                results=list(r.get("results") or []),
            )
        )

    return MainForceBatchHistoryListResponse(items=items)


@router.delete("/batch/history/{record_id}", summary="删除一条主力批量分析历史记录")
async def delete_batch_history(record_id: int) -> Dict[str, Any]:
    ok = batch_db.delete_record(record_id)
    return {"success": ok}
