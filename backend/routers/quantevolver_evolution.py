import logging
import asyncio
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse

# 导入未来的 EvolutionService (目前可能为空实现)
from ..services.quantevolver.qe_evolution_service import AutoEvolutionScheduler

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/quantevolver/evolution",
    tags=["quantevolver_evolution"],
)

scheduler = AutoEvolutionScheduler()

class EvolutionTaskCreateRequest(BaseModel):
    task_name: str = Field(..., description="演进任务名称")
    target_desc: str = Field(..., description="演进目标描述")
    max_loops: int = Field(10, description="最大演进轮数")
    base_experiment_id: str = Field(..., description="作为起点的基础实验ID")

@router.post("/tasks", summary="创建并启动新的自动演进任务")
async def create_evolution_task(req: EvolutionTaskCreateRequest, background_tasks: BackgroundTasks):
    """
    创建一个新的演进任务，并在后台异步启动状态机流转
    """
    try:
        task_id = await scheduler.create_task(
            task_name=req.task_name,
            target_desc=req.target_desc,
            max_loops=req.max_loops,
            base_experiment_id=req.base_experiment_id
        )
        
        # 将任务放入后台异步执行，不阻塞 API 返回
        background_tasks.add_task(scheduler.start_task_loop, task_id)
        
        return {"status": "success", "task_id": task_id, "message": "演进任务已创建并在后台启动"}
    except Exception as e:
        logger.error(f"Failed to create evolution task: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create task: {str(e)}")

@router.get("/tasks", summary="获取所有演进任务列表")
async def list_evolution_tasks():
    """
    从数据库中读取所有 qe_evolution_tasks
    """
    try:
        tasks = await scheduler.get_all_tasks()
        return {"status": "success", "data": tasks}
    except Exception as e:
        logger.error(f"Failed to list evolution tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/{task_id}", summary="获取单个演进任务的详细信息与所有 LOOP")
async def get_evolution_task_detail(task_id: str):
    """
    获取单个任务详情，包括其下属所有的 qe_evolution_loops
    """
    try:
        detail = await scheduler.get_task_detail(task_id)
        if not detail:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"status": "success", "data": detail}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task detail: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/tasks/{task_id}/stop", summary="手动停止/暂停演进任务")
async def stop_evolution_task(task_id: str):
    try:
        await scheduler.stop_task(task_id)
        return {"status": "success", "message": f"Task {task_id} has been marked as stopped/paused."}
    except Exception as e:
        logger.error(f"Failed to stop task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/{task_id}/logs", summary="获取实时的任务运行及 Agent 思考日志流 (SSE)")
async def stream_task_logs(task_id: str):
    """
    通过 SSE (Server-Sent Events) 返回该任务当前 LOOP 的实时日志
    底层会调用 RDAgent 的日志 API 进行转发
    """
    try:
        return StreamingResponse(
            scheduler.stream_task_logs(task_id),
            media_type="text/event-stream"
        )
    except Exception as e:
        logger.error(f"Failed to establish log stream for task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sota", summary="获取全局 SOTA 历史榜单")
async def list_sota_registry():
    """
    读取 qe_sota_registry 表
    """
    try:
        sota_list = await scheduler.get_sota_registry()
        return {"status": "success", "data": sota_list}
    except Exception as e:
        logger.error(f"Failed to list SOTA registry: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/loops/{loop_id}/sync_assets", summary="一键将该 LOOP 的实体资产同步到 AIstock 实盘可用目录")
async def sync_loop_assets_to_local(loop_id: str):
    """
    触发对 RDAgent 资产下载 API 的调用，将 models/*.pkl 和 features_order.txt 下载解压
    """
    try:
        local_path = await scheduler.sync_loop_assets(loop_id)
        return {"status": "success", "message": "资产同步成功", "local_path": local_path}
    except Exception as e:
        logger.error(f"Failed to sync assets for loop {loop_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
