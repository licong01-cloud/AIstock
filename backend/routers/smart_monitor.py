from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from ..smart_monitor_db import smart_monitor_db
from ..smart_monitor_engine import engine as smart_engine


router = APIRouter(prefix="/smart-monitor", tags=["smart-monitor"])


@router.get("/tasks", summary="获取监控任务列表")
async def list_monitor_tasks(enabled_only: bool = False) -> List[Dict[str, Any]]:
    """Return monitor tasks from SmartMonitorDB.

    This is a thin HTTP wrapper over the existing SmartMonitorDB
    interface and does not change behaviour.
    """

    return smart_monitor_db.get_monitor_tasks(enabled_only=enabled_only)


@router.post("/tasks", summary="新增监控任务")
async def create_monitor_task(task: Dict[str, Any]) -> Dict[str, Any]:
    if "stock_code" not in task or not task["stock_code"]:
        raise HTTPException(status_code=400, detail="股票代码不能为空")
    if "task_name" not in task or not task["task_name"]:
        raise HTTPException(status_code=400, detail="任务名称不能为空")

    task_id = smart_monitor_db.add_monitor_task(task)
    return {"id": task_id}


@router.patch("/tasks/{stock_code}", summary="更新监控任务（按股票代码）")
async def update_monitor_task(stock_code: str, body: Dict[str, Any]) -> Dict[str, Any]:
    if not stock_code:
        raise HTTPException(status_code=400, detail="股票代码不能为空")
    smart_monitor_db.update_monitor_task(stock_code, body)
    return {"ok": True}


@router.delete("/tasks/{task_id}", summary="删除监控任务")
async def delete_monitor_task(task_id: int) -> Dict[str, Any]:
    smart_monitor_db.delete_monitor_task(task_id)
    return {"ok": True}


@router.get("/decisions", summary="获取AI决策历史")
async def list_ai_decisions(
    stock_code: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    limit = max(1, min(limit, 200))
    return smart_monitor_db.get_ai_decisions(stock_code=stock_code, limit=limit)


@router.get("/trades", summary="获取交易记录")
async def list_trades(
    stock_code: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    limit = max(1, min(limit, 200))
    return smart_monitor_db.get_trade_records(stock_code=stock_code, limit=limit)


@router.get("/positions", summary="获取持仓监控列表")
async def list_positions() -> List[Dict[str, Any]]:
    return smart_monitor_db.get_positions()


@router.post("/analyze", summary="实时分析单只股票")
async def analyze_stock(payload: Dict[str, Any]) -> Dict[str, Any]:
  stock_code = str(payload.get("stock_code", "")).strip()
  if not stock_code:
      raise HTTPException(status_code=400, detail="stock_code 必填")
  auto_trade = bool(payload.get("auto_trade", False))
  notify = bool(payload.get("notify", True))
  has_position = bool(payload.get("has_position", False))
  position_cost = float(payload.get("position_cost", 0) or 0)
  position_quantity = int(payload.get("position_quantity", 0) or 0)
  result = smart_engine.analyze_stock(
      stock_code=stock_code,
      auto_trade=auto_trade,
      notify=notify,
      has_position=has_position,
      position_cost=position_cost,
      position_quantity=position_quantity,
  )
  if not result.get("success"):
      # 仍然返回 200，但附带 error 信息，保持与旧 UI 行为相似
      return result
  return result
