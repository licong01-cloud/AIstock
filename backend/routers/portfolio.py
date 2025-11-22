from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel

from portfolio_manager import portfolio_manager
from portfolio_scheduler import portfolio_scheduler
from notification_service import notification_service


router = APIRouter(prefix="/portfolio", tags=["portfolio"])


class PortfolioStockCreate(BaseModel):
    code: str
    name: Optional[str] = None
    cost_price: Optional[float] = None
    quantity: Optional[int] = None
    note: Optional[str] = None
    auto_monitor: bool = True


class PortfolioStockUpdate(BaseModel):
    cost_price: Optional[float] = None
    quantity: Optional[int] = None
    note: Optional[str] = None
    auto_monitor: Optional[bool] = None


class BatchAnalyzeRequest(BaseModel):
    mode: str = "sequential"  # "sequential" or "parallel"
    max_workers: int = 3
    auto_sync_monitor: bool = True
    send_notification: bool = True


class SchedulerConfigPayload(BaseModel):
    schedule_times: List[str]
    analysis_mode: str
    max_workers: int
    auto_sync_monitor: bool
    send_notification: bool


@router.get("/stocks", summary="持仓股票列表")
async def list_stocks(auto_monitor_only: bool = Query(False)) -> List[Dict[str, Any]]:
    """返回所有持仓股票列表，对应 display_portfolio_stocks 中 portfolio_manager.get_all_stocks。"""

    return portfolio_manager.get_all_stocks(auto_monitor_only=auto_monitor_only)


@router.post("/stocks", summary="添加持仓股票")
async def add_stock(payload: PortfolioStockCreate) -> Dict[str, Any]:
    ok, msg, stock_id = portfolio_manager.add_stock(
        code=payload.code,
        name=payload.name or "",
        cost_price=payload.cost_price,
        quantity=payload.quantity,
        note=payload.note or "",
        auto_monitor=payload.auto_monitor,
    )
    if not ok or stock_id is None:
        raise HTTPException(status_code=400, detail=msg or "添加失败")
    return {"id": int(stock_id), "message": msg}


@router.put("/stocks/{stock_id}", summary="更新持仓股票")
async def update_stock(stock_id: int, payload: PortfolioStockUpdate) -> Dict[str, Any]:
    updates: Dict[str, Any] = {}
    if payload.cost_price is not None:
        updates["cost_price"] = payload.cost_price
    if payload.quantity is not None:
        updates["quantity"] = payload.quantity
    if payload.note is not None:
        updates["note"] = payload.note
    if payload.auto_monitor is not None:
        updates["auto_monitor"] = payload.auto_monitor

    if not updates:
        raise HTTPException(status_code=400, detail="没有需要更新的字段")

    ok, msg = portfolio_manager.update_stock(stock_id, **updates)
    if not ok:
        raise HTTPException(status_code=400, detail=msg or "更新失败")
    return {"success": True, "message": msg}


@router.delete("/stocks/{stock_id}", summary="删除持仓股票")
async def delete_stock(stock_id: int) -> Dict[str, Any]:
    ok, msg = portfolio_manager.delete_stock(stock_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg or "删除失败")
    return {"success": True, "message": msg}


@router.post("/batch-analyze", summary="批量分析所有持仓股票")
async def batch_analyze(payload: BatchAnalyzeRequest) -> Dict[str, Any]:
    """批量分析所有持仓股票，对应旧版 display_batch_analysis 的逻辑。"""

    res = portfolio_manager.batch_analyze_portfolio(
        mode=payload.mode,
        max_workers=payload.max_workers,
        progress_callback=None,
    )

    # 保存分析结果
    saved_ids = portfolio_manager.save_analysis_results(res)

    # 自动同步到监测列表
    sync_result: Optional[Dict[str, Any]] = None
    if payload.auto_sync_monitor:
        try:
            sync_result = portfolio_scheduler._sync_to_monitor(res)  # type: ignore[attr-defined]
        except Exception:
            sync_result = None

    # 发送通知
    if payload.send_notification:
        try:
            notification_service.send_portfolio_analysis_notification(
                res, sync_result
            )
        except Exception:
            # 通知失败不影响主流程
            pass

    out: Dict[str, Any] = dict(res)
    out["saved_count"] = len(saved_ids)
    out["sync_result"] = sync_result
    return out


@router.get("/analysis/latest-all", summary="所有持仓的最新分析")
async def latest_all_analysis() -> List[Dict[str, Any]]:
    """获取所有持仓股票的最新一次分析结果，用于“分析历史-全部”视图。"""

    return portfolio_manager.get_all_latest_analysis()


@router.get("/analysis/history", summary="单只股票分析历史")
async def analysis_history(
    stock_id: int = Query(..., description="持仓股票ID"),
    limit: int = Query(20, ge=1, le=200),
) -> List[Dict[str, Any]]:
    return portfolio_manager.get_analysis_history(stock_id, limit=limit)


@router.get("/scheduler/status", summary="持仓定时任务状态")
async def scheduler_status() -> Dict[str, Any]:
    status = portfolio_scheduler.get_status()
    status["schedule_times"] = portfolio_scheduler.get_schedule_times()
    return status


@router.post("/scheduler/config", summary="更新调度器配置")
async def update_scheduler_config(payload: SchedulerConfigPayload) -> Dict[str, Any]:
    # 先更新时间列表
    if payload.schedule_times:
        portfolio_scheduler.set_schedule_times(payload.schedule_times)

    portfolio_scheduler.update_config(
        analysis_mode=payload.analysis_mode,
        max_workers=payload.max_workers,
        auto_sync_monitor=payload.auto_sync_monitor,
        send_notification=payload.send_notification,
    )
    status = portfolio_scheduler.get_status()
    status["schedule_times"] = portfolio_scheduler.get_schedule_times()
    return status


@router.post("/scheduler/start", summary="启动持仓定时任务")
async def start_scheduler() -> Dict[str, Any]:
    ok = portfolio_scheduler.start_scheduler()
    if not ok:
        raise HTTPException(status_code=400, detail="启动调度器失败，请检查配置或持仓数量")
    status = portfolio_scheduler.get_status()
    status["schedule_times"] = portfolio_scheduler.get_schedule_times()
    return status


@router.post("/scheduler/stop", summary="停止持仓定时任务")
async def stop_scheduler() -> Dict[str, Any]:
    ok = portfolio_scheduler.stop_scheduler()
    if not ok:
        raise HTTPException(status_code=400, detail="调度器当前未运行")
    status = portfolio_scheduler.get_status()
    status["schedule_times"] = portfolio_scheduler.get_schedule_times()
    return status


@router.post("/scheduler/run-once", summary="立即执行一次持仓分析")
async def run_once() -> Dict[str, Any]:
    ok = portfolio_scheduler.run_analysis_now()
    if not ok:
        raise HTTPException(status_code=400, detail="执行失败，可能没有持仓股票")
    status = portfolio_scheduler.get_status()
    status["schedule_times"] = portfolio_scheduler.get_schedule_times()
    return status
