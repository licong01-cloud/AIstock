from typing import Any, Dict, List

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel

from pg_monitor_repo import monitor_db
from monitor_service import monitor_service
from notification_service import notification_service
from miniqmt_interface import get_miniqmt_status, init_miniqmt, miniqmt


router = APIRouter(prefix="/monitor", tags=["monitor"])


class MonitorStockBase(BaseModel):
    rating: str = "持有"
    entry_min: float
    entry_max: float
    take_profit: float | None = None
    stop_loss: float | None = None
    check_interval: int = 30
    notification_enabled: bool = True
    quant_enabled: bool = False
    quant_config: Dict[str, Any] | None = None


class MonitorStockCreate(MonitorStockBase):
    symbol: str
    name: str | None = None


class MonitorStockUpdate(MonitorStockBase):
    pass


class SchedulerConfigUpdate(BaseModel):
    enabled: bool
    market: str
    trading_days: List[int]
    auto_stop: bool = True
    pre_market_minutes: int = 5
    post_market_minutes: int = 5


def _build_monitor_summary() -> Dict[str, Any]:
    stocks = monitor_db.get_monitored_stocks()
    total_stocks = len(stocks)

    try:
        stocks_needing_update = len(monitor_service.get_stocks_needing_update())
    except Exception:
        stocks_needing_update = 0

    try:
        pending_notifications = len(monitor_db.get_pending_notifications())
    except Exception:
        pending_notifications = 0

    return {
        "total_stocks": total_stocks,
        "stocks_needing_update": stocks_needing_update,
        "pending_notifications": pending_notifications,
        "active_monitoring": bool(getattr(monitor_service, "running", False)),
    }


@router.get("/summary", summary="监测概览")
async def get_monitor_summary() -> Dict[str, Any]:
    """返回当前监测概览信息。

    语义等价于旧版 monitor_ui.get_monitor_summary / monitor_manager.get_monitor_summary。
    """

    return _build_monitor_summary()


@router.get("/service/status", summary="监测服务运行状态")
async def get_service_status() -> Dict[str, Any]:
    return {"running": bool(getattr(monitor_service, "running", False))}


@router.post("/service/start", summary="启动监测服务")
async def start_service() -> Dict[str, Any]:
    try:
        # start_monitoring 内部会启动后台线程，随后尝试调用 streamlit 的提示函数；
        # 在 FastAPI 环境下可能没有有效的 Streamlit 会话，这里忽略这类异常。
        monitor_service.start_monitoring()
    except Exception:
        pass

    return {"running": bool(getattr(monitor_service, "running", False))}


@router.post("/service/stop", summary="停止监测服务")
async def stop_service() -> Dict[str, Any]:
    try:
        monitor_service.stop_monitoring()
    except Exception:
        pass

    return {"running": bool(getattr(monitor_service, "running", False))}


@router.post("/service/manual-update-all", summary="手动更新所有需要更新的监测股票")
async def manual_update_all() -> Dict[str, Any]:
    try:
        stocks = monitor_service.get_stocks_needing_update()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e

    updated = 0
    for stock in stocks:
        try:
            if monitor_service.manual_update_stock(int(stock["id"])):
                updated += 1
        except Exception:
            # 单只失败不影响整体
            continue

    return {"updated": updated}


@router.get("/stocks", summary="监测股票列表")
async def list_monitored_stocks() -> List[Dict[str, Any]]:
    """返回当前所有监测股票列表。

    对应旧版 monitor_manager.display_monitored_stocks / monitor_ui.display_monitored_stocks
    中使用的 monitor_db.get_monitored_stocks() 结果。
    """

    return monitor_db.get_monitored_stocks()


def _validate_entry_range(data: MonitorStockBase) -> None:
    if not (data.entry_min > 0 and data.entry_max > 0 and data.entry_max > data.entry_min):
        raise HTTPException(status_code=400, detail="无效的进场区间：entry_min / entry_max 必须为正数且 max > min")


@router.post("/stocks", summary="添加监测股票")
async def add_monitored_stock(payload: MonitorStockCreate) -> Dict[str, Any]:
    """添加单只监测股票，语义等价于旧版“添加股票监测”表单提交。"""

    _validate_entry_range(payload)

    entry_range = {"min": float(payload.entry_min), "max": float(payload.entry_max)}

    stock_id = monitor_db.add_monitored_stock(
        symbol=payload.symbol,
        name=payload.name or payload.symbol,
        rating=payload.rating,
        entry_range=entry_range,
        take_profit=payload.take_profit,
        stop_loss=payload.stop_loss,
        check_interval=int(payload.check_interval),
        notification_enabled=bool(payload.notification_enabled),
        quant_enabled=bool(payload.quant_enabled),
        quant_config=payload.quant_config or {},
    )

    try:
        monitor_service.manual_update_stock(stock_id)
    except Exception:
        # 行情更新失败不影响添加本身
        pass

    return {"id": stock_id}


@router.put("/stocks/{stock_id}", summary="编辑监测股票")
async def update_monitored_stock(stock_id: int, payload: MonitorStockUpdate) -> Dict[str, Any]:
    """编辑单只监测股票设置，对应旧版编辑对话框保存。"""

    existing = monitor_db.get_stock_by_id(stock_id)
    if not existing:
        raise HTTPException(status_code=404, detail="监测股票不存在")

    _validate_entry_range(payload)

    entry_range = {"min": float(payload.entry_min), "max": float(payload.entry_max)}

    monitor_db.update_monitored_stock(
        stock_id=stock_id,
        rating=payload.rating,
        entry_range=entry_range,
        take_profit=payload.take_profit,
        stop_loss=payload.stop_loss,
        check_interval=int(payload.check_interval),
        notification_enabled=bool(payload.notification_enabled),
        quant_enabled=bool(payload.quant_enabled),
        quant_config=payload.quant_config or {},
    )

    return {"success": True}


@router.delete("/stocks/{stock_id}", summary="删除监测股票")
async def delete_monitored_stock(stock_id: int) -> Dict[str, Any]:
    ok = monitor_db.remove_monitored_stock(stock_id)
    if not ok:
        raise HTTPException(status_code=404, detail="监测股票不存在或已被删除")
    return {"success": True}


@router.post("/stocks/{stock_id}/manual-update", summary="手动更新指定监测股票")
async def manual_update_stock(stock_id: int) -> Dict[str, Any]:
    ok = monitor_service.manual_update_stock(stock_id)
    if not ok:
        raise HTTPException(status_code=404, detail="监测股票不存在")
    return {"success": True}


@router.post("/stocks/{stock_id}/notification", summary="切换通知开关")
async def toggle_stock_notification(
    stock_id: int,
    enabled: bool = Body(..., embed=True),
) -> Dict[str, Any]:
    stock = monitor_db.get_stock_by_id(stock_id)
    if not stock:
        raise HTTPException(status_code=404, detail="监测股票不存在")

    monitor_db.toggle_notification(stock_id, enabled)
    return {"success": True, "enabled": bool(enabled)}


@router.post("/stocks/batch-add", summary="批量添加或更新监测股票")
async def batch_add_or_update_monitors(
    items: List[Dict[str, Any]] = Body(..., embed=True),
) -> Dict[str, int]:
    """批量添加或更新监测股票。

    请求体中的每个元素语义等价于 pg_monitor_repo.StockMonitorDatabase.batch_add_or_update_monitors
    中的 monitors_data 条目，字段包括：code / symbol, name, rating, entry_min, entry_max,
    take_profit, stop_loss, check_interval, notification_enabled 等。
    """

    return monitor_db.batch_add_or_update_monitors(items)


@router.get("/notifications/recent", summary="最近通知列表")
async def get_recent_notifications(
    limit: int = Query(10, ge=1, le=100),
) -> List[Dict[str, Any]]:
    return monitor_db.get_all_recent_notifications(limit=limit)


@router.post("/notifications/mark-all-sent", summary="标记所有通知为已读")
async def mark_all_notifications_sent() -> Dict[str, Any]:
    count = monitor_db.mark_all_notifications_sent()
    return {"updated": int(count)}


@router.post("/notifications/clear", summary="清空所有通知")
async def clear_notifications() -> Dict[str, Any]:
    count = monitor_db.clear_all_notifications()
    return {"deleted": int(count)}


@router.get("/notifications/email-config-status", summary="邮件通知配置状态")
async def get_email_config_status() -> Dict[str, Any]:
    return notification_service.get_email_config_status()


@router.post("/notifications/send-test-email", summary="发送测试邮件")
async def send_test_email() -> Dict[str, Any]:
    success, message = notification_service.send_test_email()
    return {"success": success, "message": message}


@router.get("/scheduler/status", summary="定时调度状态")
async def get_scheduler_status() -> Dict[str, Any]:
    scheduler = monitor_service.get_scheduler()
    if scheduler is None:
        raise HTTPException(status_code=500, detail="调度器未初始化")
    return scheduler.get_status()


@router.post("/scheduler/config", summary="更新定时调度配置")
async def update_scheduler_config(payload: SchedulerConfigUpdate) -> Dict[str, Any]:
    scheduler = monitor_service.get_scheduler()
    if scheduler is None:
        raise HTTPException(status_code=500, detail="调度器未初始化")

    scheduler.update_config(
        enabled=payload.enabled,
        market=payload.market,
        trading_days=payload.trading_days,
        auto_stop=payload.auto_stop,
        pre_market_minutes=payload.pre_market_minutes,
        post_market_minutes=payload.post_market_minutes,
    )

    return scheduler.get_status()


@router.post("/scheduler/start", summary="启动定时调度器")
async def start_scheduler() -> Dict[str, Any]:
    scheduler = monitor_service.get_scheduler()
    if scheduler is None:
        raise HTTPException(status_code=500, detail="调度器未初始化")

    scheduler.start_scheduler()
    return scheduler.get_status()


@router.post("/scheduler/stop", summary="停止定时调度器")
async def stop_scheduler() -> Dict[str, Any]:
    scheduler = monitor_service.get_scheduler()
    if scheduler is None:
        raise HTTPException(status_code=500, detail="调度器未初始化")

    scheduler.stop_scheduler()
    return scheduler.get_status()


@router.get("/miniqmt/status", summary="MiniQMT 状态")
async def miniqmt_status() -> Dict[str, Any]:
    """返回 MiniQMT 量化接口当前状态。

    与旧版中 miniqmt_interface.get_miniqmt_status 语义一致，用于前端展示量化组件是否启用、连接状态和账户信息。
    """

    return get_miniqmt_status()


@router.post("/miniqmt/connect", summary="连接 MiniQMT")
async def miniqmt_connect() -> Dict[str, Any]:
    """尝试按照当前配置连接 MiniQMT。

    调用 init_miniqmt() 读取 config.MINIQMT_CONFIG / 环境变量并初始化全局 miniqmt 实例，
    然后返回最新状态和初始化结果信息。
    """

    success, message = init_miniqmt()
    status = get_miniqmt_status()
    return {"success": bool(success), "message": message, "status": status}


@router.post("/miniqmt/disconnect", summary="断开 MiniQMT")
async def miniqmt_disconnect() -> Dict[str, Any]:
    """断开与 MiniQMT 的连接，但保留当前启用配置。

    语义上等价于对全局 miniqmt 实例调用 disconnect()，不会修改 MINIQMT_ENABLED，便于之后再次手动连接。
    """

    try:
        disconnected = miniqmt.disconnect()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    status = get_miniqmt_status()
    return {"success": bool(disconnected), "status": status}
