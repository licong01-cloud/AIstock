from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from ..services.indicator_screening_service import (
    StrategyFilterConfig,
    StrategyResult,
    get_indicator_screening_service,
)


router = APIRouter(prefix="/indicator-screening", tags=["indicator-screening"])


class Open0935Request(BaseModel):
    """开盘 9:35 指标选股请求参数。

    trade_date 支持 "YYYY-MM-DD" 或 "YYYYMMDD" 字符串，后端统一转换为 YYYYMMDD。
    """

    trade_date: str
    top_n: int = 100
    pct_chg_min: float = -1.5
    pct_chg_max: float = 2.5
    turnover_min: float = 3.0
    volume_hand_min: int = 50_000
    float_share_max: float = 1_500_000_000.0
    float_mv_max: float = 50_000_000_000.0
    net_inflow_today_min: float = 2_000_000.0
    net_inflow_10d_min: float = 2_000_000.0


class IndicatorScreeningResponse(BaseModel):
    """开盘 9:35 指标选股结果。"""

    success: bool
    error: Optional[str]
    filters_applied: List[str]
    filters_skipped: List[str]
    trade_date: str
    total_candidates: int
    selected_count: int
    rows: List[Dict[str, Any]]


@router.post("/open-0935", response_model=IndicatorScreeningResponse, summary="开盘 9:35 指标选股策略")
async def run_open_0935(req: Open0935Request) -> IndicatorScreeningResponse:
    svc = get_indicator_screening_service()

    trade_date = req.trade_date.replace("-", "")

    cfg = StrategyFilterConfig(
        trade_date=trade_date,
        pct_chg_min=req.pct_chg_min,
        pct_chg_max=req.pct_chg_max,
        turnover_min=req.turnover_min,
        volume_hand_min=req.volume_hand_min,
        float_share_max=req.float_share_max,
        float_mv_max=req.float_mv_max,
        net_inflow_today_min=req.net_inflow_today_min,
        net_inflow_10d_min=req.net_inflow_10d_min,
        top_n=req.top_n,
    )
    result: StrategyResult = svc.run_open_0935_strategy(cfg)
    payload = result.to_dict()
    rows = payload.pop("df", [])
    return IndicatorScreeningResponse(rows=rows, **payload)
