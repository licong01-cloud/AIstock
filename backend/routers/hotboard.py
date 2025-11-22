from fastapi import APIRouter, Query

from ..services.hotboard_service import (
    get_hotboard_realtime,
    get_hotboard_realtime_timestamps,
    get_hotboard_daily,
    get_tdx_board_types,
    get_tdx_board_daily,
    get_top_stocks_realtime,
    get_top_stocks_tdx,
)


router = APIRouter(prefix="/hotboard", tags=["hotboard"])


@router.get("/realtime", summary="实时热点板块")
def hotboard_realtime(
    metric: str = Query("combo", description="着色指标：combo/chg/flow"),
    alpha: float = Query(0.5, ge=0.0, le=1.0, description="复合权重α，用于组合涨幅与资金流"),
    cate_type: int | None = Query(None, description="板块分类：0行业/1概念/2证监会行业/None全部"),
    at: str | None = Query(None, description="指定时间点(ISO，可选)"),
):
    return get_hotboard_realtime(metric=metric, alpha=alpha, cate_type=cate_type, at=at)


@router.get("/realtime/timestamps", summary="实时热点时间轴")
def hotboard_realtime_timestamps(
    date: str | None = Query(None, description="日期(YYYY-MM-DD)，为空则取今日"),
    cate_type: int | None = Query(None, description="板块分类过滤"),
):
    return get_hotboard_realtime_timestamps(date=date, cate_type=cate_type)


@router.get("/daily", summary="新浪历史热点板块")
def hotboard_daily(
    date: str = Query(..., description="交易日期"),
    cate_type: int | None = Query(None, description="板块分类"),
):
    return get_hotboard_daily(date=date, cate_type=cate_type)


@router.get("/tdx/types", summary="TDX 板块类型列表")
def tdx_board_types():
    return get_tdx_board_types()


@router.get("/tdx/daily", summary="TDX 历史热点板块")
def tdx_board_daily(
    date: str = Query(..., description="交易日期"),
    idx_type: str | None = Query(None, description="板块类别"),
    limit: int = Query(50, ge=1, le=500, description="返回数量上限"),
):
    return get_tdx_board_daily(date=date, idx_type=idx_type, limit=limit)


@router.get("/top-stocks/realtime", summary="实时热点板块成分股Top")
def top_stocks_realtime(
    board_code: str = Query(..., description="板块代码(新浪concept code)"),
    metric: str = Query("chg", description="排序指标：chg/flow"),
    limit: int = Query(20, ge=1, le=200, description="返回Top数量"),
):
    return get_top_stocks_realtime(board_code=board_code, metric=metric, limit=limit)


@router.get("/top-stocks/tdx", summary="TDX 历史热点板块成分股Top")
def top_stocks_tdx(
    board_code: str = Query(..., description="板块代码(ts_code)"),
    date: str = Query(..., description="交易日期"),
    metric: str = Query("chg", description="排序指标：chg/flow"),
    limit: int = Query(20, ge=1, le=200, description="返回Top数量"),
):
    return get_top_stocks_tdx(board_code=board_code, date=date, metric=metric, limit=limit)
