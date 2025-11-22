from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from ..services.cloud_screening_service import (
    search_stocks,
    get_hot_strategy_list,
)


router = APIRouter(prefix="/cloud-screening", tags=["cloud-screening"])


class CloudSearchRequest(BaseModel):
    """云选股搜索请求。

    由前端先合成最终关键词后再调用该接口：
    - 优先级：文本输入 > 自定义策略 > 热门策略。
    """

    keyword: str
    page_size: int = 100


class CloudSearchResponse(BaseModel):
    success: bool
    error: Optional[str]
    keyword: str
    total: int
    items: List[Dict[str, Any]]


class CloudHotStrategiesResponse(BaseModel):
    strategies: List[Dict[str, Any]]


@router.get("/hot-strategies", response_model=CloudHotStrategiesResponse, summary="获取热门云选股策略列表")
async def hot_strategies(limit: int = Query(20, ge=1, le=100)) -> CloudHotStrategiesResponse:
    strategies = get_hot_strategy_list(limit=limit)
    return CloudHotStrategiesResponse(strategies=strategies)


@router.post("/search", response_model=CloudSearchResponse, summary="执行云选股搜索")
async def cloud_search(req: CloudSearchRequest) -> CloudSearchResponse:
    keyword = (req.keyword or "").strip()
    if not keyword:
        return CloudSearchResponse(
            success=False,
            error="keyword 不能为空",
            keyword="",
            total=0,
            items=[],
        )

    try:
        items = search_stocks(keyword, page_size=req.page_size)
        return CloudSearchResponse(
            success=True,
            error=None,
            keyword=keyword,
            total=len(items),
            items=items,
        )
    except Exception as exc:  # noqa: BLE001
        return CloudSearchResponse(
            success=False,
            error=str(exc),
            keyword=keyword,
            total=0,
            items=[],
        )
