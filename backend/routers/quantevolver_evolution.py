import logging
import os
import json
import time
import uuid
import asyncio
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
import httpx

# 导入未来的 EvolutionService (目前可能为空实现)
from ..services.quantevolver.qe_evolution_service import AutoEvolutionScheduler
from ..services.quantevolver.factor_value_loader import FactorValueLoader
from ..services.quantevolver.correlation_engine import CorrelationEngine, CorrelationResult
from ..db.pg_pool import get_conn
from psycopg2.extras import RealDictCursor, execute_values

# RD-Agent QE workspace API base URL
RDAGENT_QE_BASE = os.getenv("RDAGENT_RESULTS_API_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
RDAGENT_QE_TIMEOUT = 30.0

# 项目根目录 (routers/ → 上 2 层)
_PROJECT_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)

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
    base_experiment_id: str = Field(None, description="作为起点的基础实验ID（source_type=qe_experiment时必填）")
    source_type: str = Field("qe_experiment", description="来源类型: qe_experiment / rdagent_task_sota")
    source_task_id: Optional[str] = Field(None, description="RDAgent task ID（source_type=rdagent_task_sota时必填）")
    include_alpha_baseline: bool = Field(False, description="是否包含Alpha基准因子")
    evolution_guidance: Optional[str] = Field(None, description="演进目标/指引文本")
    evolution_mode: Optional[str] = Field("auto", description="演进模式: auto / factor_only / model_only / joint")
    selected_model_id: Optional[str] = Field(None, description="手动选择的模型ID（Factor Task时必填）")
    selected_factor_keys: Optional[List[str]] = Field(None, description="手动选择的因子key列表，格式 ['name||source', ...]（Model Task时必填）")
    stock_pool: Optional[str] = Field(None, description="Qlib股票池文件WSL路径，None=使用all.txt全量股票池")
    strategy_id: Optional[str] = Field(None, description="交易策略ID，None=使用基础实验继承的策略")
    strategy_params: Optional[Dict[str, Any]] = Field(None, description="策略参数覆盖，如 {topk: 30, n_drop: 3}")
    execution_algo: Optional[str] = Field(None, description="日内执行算法code，如 TWAP/VWAP/CLOSE_PRICE")
    execution_algo_params: Optional[Dict[str, Any]] = Field(None, description="执行算法参数覆盖")
    enable_sector_hmm: bool = Field(False, description="是否启用行业 HMM 热度调整")
    hmm_model_version_id: Optional[str] = Field(None, description="HMM 模型快照 ID (snapshot_id)")
    hmm_signal_preset: Optional[str] = Field(None, description="HMM 信号系数档位: preset_A(保守,最高+5%) / preset_B(激进,最高+10%)")
    additional_factor_keys: Optional[List[str]] = Field(None, description="从因子库额外添加的因子key列表，格式 ['name||source', ...]，与来源默认因子合并")

@router.post("/tasks", summary="创建并启动新的自动演进任务")
async def create_evolution_task(req: EvolutionTaskCreateRequest, background_tasks: BackgroundTasks):
    """
    创建一个新的演进任务，并在后台异步启动状态机流转。
    支持两种来源:
    - qe_experiment: 从已有 QE 实验开始演进
    - rdagent_task_sota: 从 RDAgent task 的 SOTA 资产创建实验后演进
    """
    try:
        base_experiment_id = req.base_experiment_id

        if req.source_type == "rdagent_task_sota":
            if not req.source_task_id:
                raise HTTPException(status_code=400, detail="source_type=rdagent_task_sota 时需要提供 source_task_id")

            # 获取预览判断 task 类型，验证手动选择
            assets = await scheduler.get_task_sota_assets(req.source_task_id, include_alpha_baseline=False)
            has_sota_factors = assets["total_sota_factors"] > 0
            has_sota_models = assets["total_sota_models"] > 0

            if has_sota_factors and not has_sota_models:
                # Factor Task: 有 SOTA 因子，无 SOTA 模型 → 必须手动选模型
                if not req.selected_model_id:
                    raise HTTPException(status_code=400, detail="此 Task 为 Factor Task（有 SOTA 因子，无 SOTA 模型），请手动选择一个模型")
            elif has_sota_models and not has_sota_factors:
                # Model Task: 有 SOTA 模型，无 SOTA 因子
                # 仅在 Task 没有任何演进因子时才强制选因子（纯模型 Task）
                has_any_task_factors = assets.get("total_task_factors", 0) > 0
                if not has_any_task_factors and (not req.selected_factor_keys or len(req.selected_factor_keys) == 0):
                    raise HTTPException(status_code=400, detail="此 Task 为纯 Model Task（无演进因子），请手动选择因子")
            # Mixed Task (both > 0): 默认使用 SOTA 资产，可选手动覆盖，无需强制验证

            # 从 RDAgent task SOTA 资产创建真实实验
            create_result = await scheduler.create_experiment_from_task_sota(
                task_id=req.source_task_id,
                experiment_name=f"evo_{req.task_name}",
                include_alpha_baseline=req.include_alpha_baseline,
                model_id=req.selected_model_id,
                factor_keys=req.selected_factor_keys,
            )
            base_experiment_id = create_result["experiment_id"]

            # rdagent_task_sota: 实验刚创建(status='created')，Loop 1 即为初始回测

        if not base_experiment_id:
            raise HTTPException(status_code=400, detail="需要提供 base_experiment_id 或 source_task_id")

        # --- 合并从因子库额外添加的因子 ---
        if req.additional_factor_keys and len(req.additional_factor_keys) > 0:
            try:
                with get_conn() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT factor_names FROM qe_experiments WHERE experiment_id = %s", (base_experiment_id,))
                        row = cur.fetchone()
                        if row:
                            existing = row["factor_names"]
                            if isinstance(existing, str):
                                existing = json.loads(existing)
                            if not isinstance(existing, list):
                                existing = []
                            additional_names = [k.split("||")[0] for k in req.additional_factor_keys]
                            existing_set = set(existing)
                            merged = existing + [n for n in additional_names if n not in existing_set]
                            cur.execute(
                                "UPDATE qe_experiments SET factor_names = %s WHERE experiment_id = %s",
                                (json.dumps(merged), base_experiment_id),
                            )
                    conn.commit()
                logger.info(f"Merged {len(req.additional_factor_keys)} additional factors into experiment {base_experiment_id}")
            except Exception as e:
                logger.error(f"合并额外因子失败: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"合并额外因子失败: {e}")

        # --- HMM 模型验证 (Task 10.1) ---
        hmm_model_path = None
        if req.enable_sector_hmm:
            if not req.hmm_model_version_id:
                raise HTTPException(
                    status_code=400,
                    detail="启用行业 HMM 时必须提供 hmm_model_version_id",
                )
            from ..services.hmm_training_service import HMMTrainingService
            hmm_svc = HMMTrainingService()
            snapshot = hmm_svc.get_snapshot(req.hmm_model_version_id)
            if snapshot is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"HMM 快照 {req.hmm_model_version_id} 不存在",
                )
            if snapshot.get("status") != "completed":
                raise HTTPException(
                    status_code=400,
                    detail=f"HMM 快照状态为 '{snapshot.get('status')}'，需要 'completed'",
                )
            hmm_model_path = snapshot["model_path"]
            # 模型文件存储在 WSL 文件系统中，回测/推理在 WSL 环境执行，
            # Windows 侧无法直接用 os.path.exists 检查 WSL 路径，跳过文件检查。
            # 推理时若文件缺失会在 WSL 侧报错。

        # rdagent_task_sota 来源允许 created 状态（Loop 1 会执行初始回测建立基线）
        allow_created = (req.source_type == "rdagent_task_sota")
        start_from_loop_zero = (req.source_type == "rdagent_task_sota")
        task_id = await scheduler.create_task(
            task_name=req.task_name,
            target_desc=req.target_desc,
            max_loops=req.max_loops,
            base_experiment_id=base_experiment_id,
            allow_created=allow_created,
            start_from_loop_zero=start_from_loop_zero,
            stock_pool=req.stock_pool,
        )

        # 保存额外字段（含 evolution_mode）
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_evolution_tasks
                    SET evolution_guidance = %s, source_type = %s, source_task_id = %s,
                        evolution_mode = %s, strategy_id = %s, strategy_params = %s,
                        execution_algo = %s, execution_algo_params = %s, updated_at = NOW()
                    WHERE task_id = %s
                """, (req.evolution_guidance, req.source_type, req.source_task_id,
                      req.evolution_mode or "auto",
                      req.strategy_id,
                      json.dumps(req.strategy_params) if req.strategy_params else None,
                      req.execution_algo,
                      json.dumps(req.execution_algo_params) if req.execution_algo_params else None,
                      task_id))
            conn.commit()

        # --- 注入 HMM 模型路径到 strategy_params ---
        if req.enable_sector_hmm and hmm_model_path:
            merged_params = dict(req.strategy_params) if req.strategy_params else {}
            merged_params["sector_hmm_model_path"] = hmm_model_path
            merged_params["enable_sector_hmm"] = True
            if req.hmm_signal_preset:
                merged_params["hmm_signal_preset"] = req.hmm_signal_preset
                # 从 DB 读取模型的 signal_presets 注入到 strategy_params
                try:
                    snapshot = hmm_svc.get_snapshot(req.hmm_model_version_id)
                    if snapshot:
                        config_id = snapshot["config_id"]
                        configs = hmm_svc.list_configs("sector_hmm")
                        for cfg in configs:
                            if cfg["config_id"] == config_id:
                                cj = cfg["config_json"]
                                if isinstance(cj, str):
                                    cj = json.loads(cj)
                                if "signal_presets" in cj:
                                    merged_params["hmm_signal_presets"] = cj["signal_presets"]
                                break
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"读取 HMM signal_presets 失败: {e}")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE qe_evolution_tasks
                           SET strategy_params = %s, updated_at = NOW()
                           WHERE task_id = %s""",
                        (json.dumps(merged_params), task_id),
                    )
                conn.commit()

        # 提交第一轮 Loop（事件驱动路径，非阻塞）
        # 先验证因子可用性
        base_config = scheduler._load_base_config_from_experiment(base_experiment_id)
        factor_list = base_config.get("factor_list", [])
        validation = scheduler.validate_factor_availability(factor_list)

        if validation.get("has_issues"):
            # 不阻止创建，但返回验证信息让前端处理
            return {
                "status": "success",
                "task_id": task_id,
                "factor_validation": validation,
                "message": "演进任务已创建，部分因子需要处理",
            }

        background_tasks.add_task(scheduler.submit_next_loop, task_id)
        return {"status": "success", "task_id": task_id, "message": "演进任务已创建并在后台启动"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create evolution task: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create task: {str(e)}")


class FactorResolveRequest(BaseModel):
    action: str = Field(..., description="处理方式: remove=移除不可用因子, replace=替换因子")
    replacements: Optional[Dict[str, str]] = Field(None, description="替换映射 {old_name: new_name}，action=replace 时使用")


@router.post("/tasks/{task_id}/resolve-factors", summary="处理因子验证问题")
def resolve_factor_issues(task_id: str, req: FactorResolveRequest, background_tasks: BackgroundTasks):
    """处理因子验证问题后恢复任务执行。"""
    try:
        # 读取当前实验的因子列表
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT base_experiment_id FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task_row = cur.fetchone()
                if not task_row:
                    raise HTTPException(404, f"任务 {task_id} 不存在")

                base_exp_id = task_row["base_experiment_id"]
                cur.execute("SELECT factor_names FROM qe_experiments WHERE experiment_id = %s", (base_exp_id,))
                exp_row = cur.fetchone()
                if not exp_row:
                    raise HTTPException(404, f"基础实验 {base_exp_id} 不存在")

                factor_names = exp_row["factor_names"]
                if isinstance(factor_names, str):
                    import json
                    factor_names = json.loads(factor_names)

        # 获取当前验证状态
        validation = scheduler.validate_factor_availability(factor_names)
        removed_factors = validation.get("deleted_factors", []) + validation.get("unavailable_factors", [])

        if req.action == "remove":
            new_factor_names = validation.get("valid_factors", [])
        elif req.action == "replace":
            if not req.replacements:
                raise HTTPException(400, "action=replace 需要提供 replacements 映射")
            removed_set = set(removed_factors)
            new_factor_names = []
            for f in factor_names:
                if f in removed_set:
                    replacement = req.replacements.get(f)
                    if replacement:
                        new_factor_names.append(replacement)
                    # else: 无替换则移除
                else:
                    new_factor_names.append(f)
        else:
            raise HTTPException(400, f"不支持的 action: {req.action}")

        if not new_factor_names:
            raise HTTPException(400, "处理后无可用因子，无法继续")

        # 更新实验的 factor_names
        import json
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE qe_experiments SET factor_names = %s WHERE experiment_id = %s",
                    (json.dumps(new_factor_names), base_exp_id),
                )
            conn.commit()

        # 将移除的因子加入黑名单
        if removed_factors:
            scheduler._add_to_factor_blacklist(task_id, removed_factors)

        # 恢复任务执行
        background_tasks.add_task(scheduler.submit_next_loop, task_id)

        return {
            "status": "success",
            "task_id": task_id,
            "new_factor_count": len(new_factor_names),
            "removed_factors": removed_factors,
            "message": f"已处理因子问题，{len(new_factor_names)} 个因子继续演进",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to resolve factors for task {task_id}: {e}", exc_info=True)
        raise HTTPException(500, detail=str(e))

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
        stop_result = await scheduler.stop_task(task_id)
        # 检查是否有 kill 失败的情况，向前端透明报告
        loop_info = stop_result.get("loop_killed")
        if loop_info and loop_info.get("error"):
            return {
                "status": "warning",
                "message": f"Task {task_id} 已标记暂停，但终止远端进程时遇到问题: {loop_info['error']}",
                "detail": stop_result,
            }
        return {"status": "success", "message": f"Task {task_id} 已暂停。", "detail": stop_result}
    except Exception as e:
        logger.error(f"Failed to stop task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/tasks/{task_id}", summary="删除演进任务及其所有关联数据")
async def delete_evolution_task(task_id: str):
    """
    删除指定演进任务及其所有关联数据（Loops、SOTA注册、因子/模型记录、子实验、因子实验指标）。
    运行中的任务不允许删除，需先停止。
    """
    try:
        result = await scheduler.delete_task(task_id)
        return {"status": "success", "data": result, "message": f"任务 {result['task_name']} 已删除"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete task {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class EvolutionTaskResumeRequest(BaseModel):
    additional_loops: int = Field(0, description="额外增加的演进轮数（0 表示使用原 max_loops）")

@router.post("/tasks/{task_id}/resume", summary="恢复已暂停/已完成的演进任务，继续演进")
async def resume_evolution_task(task_id: str, req: EvolutionTaskResumeRequest, background_tasks: BackgroundTasks):
    """
    恢复已暂停、已完成或已失败的演进任务，从上次的 current_loop 继续。
    可选增加额外轮数。
    """
    try:
        resumed_id = await scheduler.resume_task(task_id, additional_loops=req.additional_loops)
        background_tasks.add_task(scheduler.submit_next_loop, resumed_id)
        return {"status": "success", "task_id": resumed_id, "message": "演进任务已恢复并在后台继续执行"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to resume task {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/loops/{loop_index}/retry", summary="重试失败的 Loop（自动判断从训练或回测恢复）")
async def retry_evolution_loop(task_id: str, loop_index: int, background_tasks: BackgroundTasks):
    """重试失败的 Loop。

    自动检测 workspace 中训练状态：
    - 训练已完成（params.pkl 存在）→ 跳过训练，只重跑回测（backtest-only）
    - 训练未完成 → 全量重跑（训练 + 回测）

    适用场景：回测因策略 bug 失败，修复后重试无需等待 1 小时训练。
    """
    try:
        # 预检查：验证 loop 状态（快速返回错误，不进 background）
        from ..services.quantevolver.qe_evolution_service import get_conn
        from psycopg2.extras import RealDictCursor
        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT status FROM qe_evolution_loops WHERE loop_id = %s", (evolution_loop_db_id,))
                row = cur.fetchone()
        if not row:
            raise ValueError(f"Loop {evolution_loop_db_id} 不存在")
        if row["status"] not in ("failed", "cancelled"):
            raise ValueError(f"Loop 状态为 '{row['status']}'，只有 failed/cancelled 可以重试")

        background_tasks.add_task(scheduler.retry_loop, task_id, loop_index)
        return {
            "status": "success",
            "loop_id": evolution_loop_db_id,
            "mode": "pending",
            "message": f"Loop {loop_index} 重试已提交到后台",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to retry loop {loop_index} for task {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class EvolutionTaskForkRequest(BaseModel):
    from_loop_index: int = Field(..., description="从哪个 loop 分叉（必须是已完成的 loop）")
    task_name: Optional[str] = Field(None, description="新任务名称，默认 '{原名}_from_L{N}'")
    max_loops: int = Field(10, description="新任务最大演进轮数")
    evolution_guidance: Optional[str] = Field(None, description="演进指引文本")
    evolution_mode: Optional[str] = Field("auto", description="演进模式: auto / factor_only / model_only / joint")
    inherit_history: bool = Field(False, description="是否继承截止到该 loop 的演进历史")
    strategy_id: Optional[str] = Field(None, description="覆盖交易策略ID，None=继承源任务")
    strategy_params: Optional[Dict[str, Any]] = Field(None, description="覆盖策略参数")
    execution_algo: Optional[str] = Field(None, description="覆盖执行算法code")
    execution_algo_params: Optional[Dict[str, Any]] = Field(None, description="覆盖执行算法参数")
    additional_factor_keys: Optional[List[str]] = Field(None, description="从因子库额外添加的因子key列表，格式 ['name||source', ...]")

@router.post("/tasks/{task_id}/fork", summary="从指定 Loop 分叉出全新演进任务")
async def fork_evolution_task(task_id: str, req: EvolutionTaskForkRequest, background_tasks: BackgroundTasks):
    """
    从指定 task 的某个已完成 loop 分叉，以该 loop 的因子+模型配置为基础创建新演进任务。
    新 task 的 Loop 1 会用该配置做初始回测建立基线，后续 Loop 由 Agent 正常演进。
    """
    try:
        new_task_id = await scheduler.fork_task(
            source_task_id=task_id,
            from_loop_index=req.from_loop_index,
            task_name=req.task_name,
            max_loops=req.max_loops,
            evolution_guidance=req.evolution_guidance,
            evolution_mode=req.evolution_mode or "auto",
            inherit_history=req.inherit_history,
            strategy_id=req.strategy_id,
            strategy_params=req.strategy_params,
            execution_algo=req.execution_algo,
            execution_algo_params=req.execution_algo_params,
        )

        # 合并从因子库额外添加的因子
        if req.additional_factor_keys and len(req.additional_factor_keys) > 0:
            try:
                with get_conn() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT base_experiment_id FROM qe_evolution_tasks WHERE task_id = %s", (new_task_id,))
                        task_row = cur.fetchone()
                        if task_row and task_row["base_experiment_id"]:
                            exp_id = task_row["base_experiment_id"]
                            cur.execute("SELECT factor_names FROM qe_experiments WHERE experiment_id = %s", (exp_id,))
                            exp_row = cur.fetchone()
                            if exp_row:
                                existing = exp_row["factor_names"]
                                if isinstance(existing, str):
                                    existing = json.loads(existing)
                                if not isinstance(existing, list):
                                    existing = []
                                additional_names = [k.split("||")[0] for k in req.additional_factor_keys]
                                existing_set = set(existing)
                                merged = existing + [n for n in additional_names if n not in existing_set]
                                cur.execute(
                                    "UPDATE qe_experiments SET factor_names = %s WHERE experiment_id = %s",
                                    (json.dumps(merged), exp_id),
                                )
                    conn.commit()
                logger.info(f"Merged {len(req.additional_factor_keys)} additional factors into forked task {new_task_id}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"合并额外因子到 fork 任务失败: {e}")

        background_tasks.add_task(scheduler.submit_next_loop, new_task_id)
        return {
            "status": "success",
            "task_id": new_task_id,
            "source_task_id": task_id,
            "from_loop_index": req.from_loop_index,
            "inherit_history": req.inherit_history,
            "message": f"已从 Loop {req.from_loop_index} 分叉创建新演进任务 {new_task_id}，后台启动中",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to fork task {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class StrategyLoopConfig(BaseModel):
    label: Optional[str] = Field(None, description="Loop 标签/描述")
    loop_index: Optional[int] = Field(None, description="Loop 索引（自动填充）")
    strategy_params: Dict[str, Any] = Field(..., description="策略参数: topk, n_drop, hold_thresh, risk_degree 等")
    strategy_id: Optional[str] = Field(None, description="交易策略ID，None=继承源Loop")
    execution_algo: Optional[str] = Field(None, description="日内执行算法code")
    execution_algo_params: Optional[Dict[str, Any]] = Field(None, description="执行算法参数")
    enable_sector_hmm: bool = Field(False, description="是否启用行业 HMM")
    hmm_model_version_id: Optional[str] = Field(None, description="HMM 模型快照 ID")
    hmm_signal_preset: Optional[str] = Field(None, description="HMM 信号系数档位: preset_A/preset_B")
    sector_blacklist: Optional[List[str]] = Field(None, description="行业黑名单")
    stock_pool: Optional[str] = Field(None, description="Qlib股票池文件WSL路径")

class StrategyEvolutionForkRequest(BaseModel):
    from_loop_index: int = Field(..., description="从哪个 loop 分叉（必须已完成且有模型文件）")
    task_name: Optional[str] = Field(None, description="新任务名称")
    loops: List[StrategyLoopConfig] = Field(..., description="每个 Loop 的策略参数配置", min_length=1)
    execution_mode: str = Field("serial", description="执行方式: serial / parallel")
    inherit_history: bool = Field(False, description="是否继承截止到该 loop 的演进历史")

@router.post("/tasks/{task_id}/strategy-fork", summary="从指定 Loop 分叉出策略演进任务（跳过训练）")
async def strategy_fork_task(task_id: str, req: StrategyEvolutionForkRequest):
    """
    从指定 task 的某个已完成 loop 创建策略演进任务。
    复用源 loop 的已训练模型（mlruns 中的 params.pkl），仅修改策略参数进行批量回测。
    所有 Loop 使用 --backtest-only 模式，跳过模型训练。
    """
    try:
        # 为 loops 配置分配 loop_index
        loops_config = []
        for i, loop_cfg in enumerate(req.loops, start=1):
            cfg_dict = loop_cfg.dict()
            cfg_dict["loop_index"] = i
            loops_config.append(cfg_dict)

        new_task_id = await scheduler.strategy_fork_task(
            source_task_id=task_id,
            from_loop_index=req.from_loop_index,
            task_name=req.task_name,
            loops_config=loops_config,
            execution_mode=req.execution_mode or "serial",
            inherit_history=req.inherit_history,
        )

        return {
            "status": "success",
            "task_id": new_task_id,
            "source_task_id": task_id,
            "from_loop_index": req.from_loop_index,
            "total_loops": len(loops_config),
            "execution_mode": req.execution_mode or "serial",
            "message": f"策略演进任务已创建，{len(loops_config)} 个策略回测 Loop 后台启动中",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create strategy evolution task from {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/{task_id}/logs", summary="获取实时的任务运行及 Agent 思考日志流 (SSE)")
def stream_task_logs(task_id: str):
    """
    通过 SSE (Server-Sent Events) 返回该任务当前 LOOP 的实时日志
    底层会调用 RDAgent 的日志 API 进行转发
    """
    try:
        return StreamingResponse(
            scheduler.stream_task_logs(task_id),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
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

@router.post("/tasks/{task_id}/loops/{loop_id}/sync_assets", summary="一键将该 LOOP 的实体资产同步到 AIstock 实盘可用目录")
async def sync_loop_assets_to_local(task_id: str, loop_id: str):
    """
    触发对 RDAgent 资产下载 API 的调用，将 models/*.pkl 和 features_order.txt 下载解压
    """
    try:
        local_path = await scheduler.sync_loop_assets(task_id, loop_id)
        return {"status": "success", "message": "资产同步成功", "local_path": local_path}
    except Exception as e:
        logger.error(f"Failed to sync assets for task {task_id} loop {loop_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Webhook: Loop 完成回调（事件驱动入口）
# ============================================================

QE_WEBHOOK_SECRET = os.getenv("QE_WEBHOOK_SECRET", "")

class LoopCompletedPayload(BaseModel):
    task_id: str = Field(..., description="演进任务ID")
    loop_id: str = Field(..., description="Loop DB ID, 格式: {task_id}_Loop{N}")

@router.post("/webhook/loop-completed", summary="Loop 完成回调（由 RDAgent 侧或扫描器触发）")
async def on_loop_completed_webhook(request: Request, payload: LoopCompletedPayload):
    """
    接收 Loop 完成通知，触发 process_completed_loop 处理。
    使用 X-Webhook-Secret header 进行身份验证。
    """
    # 验证 webhook secret
    if QE_WEBHOOK_SECRET:
        provided_secret = request.headers.get("X-Webhook-Secret", "")
        if provided_secret != QE_WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    async def _process_with_logging():
        try:
            await scheduler.process_completed_loop(payload.task_id, payload.loop_id)
        except Exception as e:
            logger.error(f"Webhook process_completed_loop failed for {payload.loop_id}: {e}", exc_info=True)

    _task = asyncio.create_task(_process_with_logging())
    _task.add_done_callback(lambda t: logger.error(f"Webhook task error: {t.exception()}") if t.exception() else None)
    return {"status": "accepted", "message": f"Processing loop {payload.loop_id}"}


# ============================================================
# Phase 3: 增强诊断代理 API
# ============================================================

@router.get("/tasks/{task_id}/loops/{loop_id}/enhanced-metrics", summary="获取单个 Loop 的增强诊断指标（代理转发至 RD-Agent）")
async def get_loop_enhanced_metrics(task_id: str, loop_id: str):
    """
    从 RD-Agent 侧获取增强指标（含 IC 时间序列、训练过程、收益曲线等），
    并缓存到 qe_evolution_loops 的 metrics_json 字段。
    若 RD-Agent 未返回 bottom_stocks，则从 all_stocks 或 top_stocks 对立面补充计算。
    """
    try:
        # 优先从 DB 缓存读取（loop 已完成或失败时数据已写入）
        from ..db.pg_pool import get_conn
        import json as _json
        evolution_loop_db_id = f"{task_id}_{loop_id}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT metrics_json FROM qe_evolution_loops WHERE loop_id = %s",
                    (evolution_loop_db_id,),
                )
                row = cur.fetchone()
        if row and row[0]:
            cached = row[0] if isinstance(row[0], dict) else _json.loads(row[0])
            cached_em = cached.get("enhanced_metrics")
            if cached_em:
                return {"status": "success", "data": cached_em}

        url = f"{RDAGENT_QE_BASE}/api/v1/qe_workspace/tasks/{task_id}/loops/{loop_id}/enhanced-metrics"
        async with httpx.AsyncClient(timeout=RDAGENT_QE_TIMEOUT) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        # 若 RD-Agent 未返回 bottom_stocks，尝试从 all_stocks 计算
        if not data.get("bottom_stocks"):
            all_stocks = data.get("all_stocks") or []
            if all_stocks:
                # 按 profit 升序取亏损最大的 10 只
                loss_stocks = sorted(
                    [s for s in all_stocks if s.get("profit", 0) < 0],
                    key=lambda s: s.get("profit", 0)
                )[:10]
                if loss_stocks:
                    data["bottom_stocks"] = loss_stocks
            elif data.get("top_stocks"):
                # 没有 all_stocks，记录日志提示数据不足
                logger.warning(
                    f"Task {task_id}/{loop_id}: RD-Agent returned top_stocks but no all_stocks/bottom_stocks. "
                    "Cannot compute bottom_stocks without full stock list."
                )

        # Cache to DB
        # DB 中 loop_id 格式为 "{task_id}_Loop{N}"，路由参数 loop_id 为 "Loop{N}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE qe_evolution_loops
                       SET metrics_json = COALESCE(metrics_json, '{}'::jsonb) || %s::jsonb,
                           updated_at = NOW()
                       WHERE loop_id = %s""",
                    (
                        _json.dumps({"enhanced_metrics": data}, ensure_ascii=False),
                        evolution_loop_db_id,
                    ),
                )
            conn.commit()

        return {"status": "success", "data": data}

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"RD-Agent unreachable: {e}")


@router.get("/tasks/{task_id}/trajectory", summary="获取演进轨迹（本地 qe_evolution_loops 数据）")
def get_evolution_trajectory(task_id: str):
    """
    从本地 qe_evolution_loops 表获取演进轨迹数据。
    """
    try:
        from ..db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT loop_id, loop_index, action_type, config_json,
                              metrics_json, agent_analysis, is_sota, status
                       FROM qe_evolution_loops
                       WHERE task_id = %s
                       ORDER BY loop_index ASC""",
                    (task_id,),
                )
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        return {"status": "success", "data": {"task_id": task_id, "trajectory": rows}}

    except Exception as e:
        logger.error(f"Failed to get trajectory for task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/leaderboard", summary="获取跨任务 SOTA 排行榜")
def get_sota_leaderboard():
    """
    从 qe_evolution_loops + qe_sota_registry 聚合所有任务的 SOTA 记录，按 IC 降序排列。
    """
    try:
        from ..db.pg_pool import get_conn
        import json as _json
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT l.task_id, l.loop_id, l.loop_index, l.action_type,
                           l.metrics_json, l.is_sota, l.status,
                           t.task_name,
                           r.evaluation_reason, r.created_at
                    FROM qe_sota_registry r
                    JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                    JOIN qe_evolution_tasks t ON l.task_id = t.task_id
                    ORDER BY r.created_at DESC
                    LIMIT 100
                """)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        # Extract IC from metrics_json for sorting
        for row in rows:
            m = row.get("metrics_json") or {}
            if isinstance(m, str):
                m = _json.loads(m)
            row["ic"] = m.get("IC")
            row["sharpe"] = m.get("sharpe")

        rows.sort(key=lambda r: r.get("ic") or 0, reverse=True)

        total_tasks = len(set(r["task_id"] for r in rows))
        total_loops = len(rows)
        best_ic = max((r["ic"] for r in rows if r.get("ic")), default=None)
        best_sharpe = max((r["sharpe"] for r in rows if r.get("sharpe")), default=None)

        return {
            "status": "success",
            "data": {
                "summary": {
                    "total_tasks": total_tasks,
                    "total_sota_loops": total_loops,
                    "best_ic": best_ic,
                    "best_sharpe": best_sharpe,
                },
                "leaderboard": rows,
            },
        }
    except Exception as e:
        logger.error(f"Failed to get leaderboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 4: 多入口演进 API
# ============================================================

@router.get("/source-tasks", summary="获取所有已同步的 RDAgent Task 列表")
async def list_source_tasks():
    """获取所有已同步的 RDAgent task，用于创建演进任务的来源选择。"""
    try:
        tasks = await scheduler.get_available_source_tasks()
        return {"status": "success", "data": tasks}
    except Exception as e:
        logger.error(f"Failed to list source tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/source-experiments", summary="获取已完成的 QE 实验列表")
async def list_source_experiments():
    """获取所有已完成的 QE 实验，用于作为演进起点选择。"""
    try:
        experiments = await scheduler.get_completed_experiments()
        return {"status": "success", "data": experiments}
    except Exception as e:
        logger.error(f"Failed to list source experiments: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/source-tasks/{task_id}/preview", summary="预览指定 RDAgent Task 的 SOTA 资产")
async def preview_source_task(task_id: str, include_alpha: bool = False, include_correlations: bool = False):
    """预览指定 RDAgent task 的 SOTA 因子和模型，用于创建演进任务前确认。"""
    try:
        assets = await scheduler.get_task_sota_assets(task_id, include_alpha_baseline=include_alpha)
        if include_correlations:
            sota_names = [f["factor_name"] for f in assets.get("sota_factors", [])]
            assets["correlation_pairs"] = scheduler._query_factor_correlation_pairs(sota_names)
        return {"status": "success", "data": assets}
    except Exception as e:
        logger.error(f"Failed to preview source task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 因子相关性 API (S7)
# ============================================================

# 模块级缓存：避免每次请求都重新实例化
_correlation_loader: Optional[FactorValueLoader] = None
_computing_lock = threading.Lock()
_latest_result: Optional[CorrelationResult] = None
_compute_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="corr-compute")
_stop_event = threading.Event()
_compute_future: Optional[Future] = None
_MATRIX_TIMEOUT_SEC = 3600  # 60 分钟 (首次加载 448 因子 ~8min + GEMM ~1min; 后续有合并缓存 ~1min)


class _CorrelationLogBuffer:
    """线程安全的环形日志缓冲区，跨页面导航持久化。"""

    MAX_ENTRIES = 2000

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: list[dict] = []  # [{index, ts, level, msg}]
        self._next_index = 0

    def append(self, msg: str, level: str = "INFO"):
        with self._lock:
            entry = {
                "index": self._next_index,
                "ts": datetime.now().strftime("%H:%M:%S"),
                "level": level,
                "msg": msg,
            }
            self._entries.append(entry)
            self._next_index += 1
            # 超过上限时截断前半部分
            if len(self._entries) > self.MAX_ENTRIES:
                self._entries = self._entries[-self.MAX_ENTRIES:]

    def get_since(self, after_index: int = -1) -> list[dict]:
        """返回 index > after_index 的所有日志条目。"""
        with self._lock:
            if after_index < 0:
                return list(self._entries)
            return [e for e in self._entries if e["index"] > after_index]

    def clear(self):
        with self._lock:
            self._entries.clear()
            self._next_index = 0


_correlation_logs = _CorrelationLogBuffer()


class _CorrelationProgress:
    """线程安全的相关性计算进度追踪器。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._data = {
            "status": "idle",
            "phase": "",
            "phase_label": "",
            "total": 0,
            "done": 0,
            "percent": 0,
            "started_at": None,
            "elapsed_sec": 0,
            "mode": "",
            "error": None,
            "job_id": None,
        }
        self._start_time = None

    def snapshot(self) -> dict:
        with self._lock:
            if self._start_time and self._data["status"] == "computing":
                self._data["elapsed_sec"] = round(time.time() - self._start_time, 1)
            return dict(self._data)

    def start(self, mode: str, total: int, job_id: str = None):
        with self._lock:
            self._start_time = time.time()
            self._data.update(
                status="computing", phase="cache_gen",
                phase_label="生成单因子缓存", total=total, done=0,
                percent=0, started_at=datetime.now().isoformat(),
                mode=mode, error=None, job_id=job_id,
            )

    def advance(self, done=None, phase=None, phase_label=None, total=None):
        with self._lock:
            if done is not None:
                self._data["done"] = done
            if phase:
                self._data["phase"] = phase
            if phase_label:
                self._data["phase_label"] = phase_label
            if total is not None:
                self._data["total"] = total
            p = self._data["phase"]
            t = max(self._data["total"], 1)
            d = self._data["done"]
            r = min(d / t, 1.0)
            if p == "cache_gen":
                self._data["percent"] = int(r * 60)
            elif p == "matrix_compute":
                self._data["percent"] = 60 + int(r * 30)
            elif p == "db_persist":
                self._data["percent"] = 90 + int(r * 10)

    def finish(self, status="success", error=None):
        with self._lock:
            self._data["status"] = status
            self._data["percent"] = 100 if status == "success" else self._data["percent"]
            self._data["error"] = error
            if self._start_time:
                self._data["elapsed_sec"] = round(time.time() - self._start_time, 1)


_correlation_progress = _CorrelationProgress()

# 缓存 status 端点的 DB 查询结果，计算中跳过 DB 直接返回缓存
_status_db_cache: Dict[str, Any] = {"meta": None, "db_count": 0, "uncorrelated_count": 0}


def _update_job_status(job_id, status, error=None):
    """更新 ingestion_jobs 表状态。"""
    if not job_id:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if status == "running":
                    cur.execute(
                        "UPDATE market.ingestion_jobs SET status='running', started_at=NOW() WHERE job_id=%s",
                        (str(job_id),),
                    )
                else:
                    cur.execute(
                        "UPDATE market.ingestion_jobs SET status=%s, finished_at=NOW() WHERE job_id=%s",
                        (status, str(job_id)),
                    )
            conn.commit()
    except Exception as e:
        logger.warning(f"更新 job 状态失败: {e}")


def _run_correlation_compute(mode: str, factor_names: list, as_of_date: str = None, job_id: str = None):
    """统一相关性计算入口 — 同步函数，在 ThreadPoolExecutor 中执行。

    mode: "full" | "cache_only" | "smart_incremental" | "incremental"
    """
    global _latest_result
    timeout_timer = None
    with _computing_lock:
        try:
            _stop_event.clear()
            # 超时保护延迟到阶段2矩阵计算前启动（阶段1缓存生成有自身per-factor超时）

            _correlation_logs.clear()
            _correlation_progress.start(mode, len(factor_names), str(job_id) if job_id else None)
            _update_job_status(job_id, "running")
            _correlation_logs.append(f"启动相关性计算: mode={mode}, 因子数={len(factor_names)}, as_of_date={as_of_date or 'latest'}")

            # 汇总统计变量
            phase1_elapsed = 0.0
            phase2_elapsed = 0.0
            phase3_elapsed = 0.0
            success_factors = []
            failed_factors: list[tuple[str, str]] = []  # [(factor_name, error_msg), ...]

            # Phase 1: 生成单因子缓存
            if mode in ("full", "smart_incremental", "incremental"):
                from ..services.quantevolver.factor_value_pipeline import FactorValuePipeline
                pipeline = FactorValuePipeline()

                # ── 日期策略（必须在预热之前确定，确保预热和因子计算使用同一日期） ──
                cache_meta = pipeline._load_meta()
                existing_as_of = cache_meta.get("as_of_date", "")
                effective_end_date = as_of_date  # 用户指定的日期

                if not effective_end_date and existing_as_of:
                    # 未指定日期：使用现有缓存日期（保证一致性）
                    effective_end_date = existing_as_of
                    _correlation_logs.append(f"[日期] 使用现有缓存日期: {effective_end_date}")

                # 预热数据缓存（静态因子 ~300s + 行情 ~40s），
                # 传入 effective_end_date 确保与因子计算使用相同日期
                _correlation_logs.append(f"[预热] 加载静态因子数据 + 行情数据到缓存 (end_date={effective_end_date or 'today'})...")
                _correlation_progress.advance(phase="cache_gen", phase_label="预热数据缓存")
                try:
                    warmup_timings = pipeline.warm_caches(end_date=effective_end_date or None)
                    _correlation_logs.append(
                        f"[预热完成] 静态因子={warmup_timings.get('static', 0)}s, "
                        f"行情数据={warmup_timings.get('realtime', 0)}s"
                    )
                except Exception as e:
                    logger.error(f"缓存预热失败: {e}", exc_info=True)
                    _correlation_logs.append(f"[预热失败] {e} — 继续执行，部分因子可能超时")

                # Phase 1a: 如果指定了新日期且比现有缓存新，增量扩展存量因子
                if effective_end_date and effective_end_date > existing_as_of and mode in ("smart_incremental", "incremental"):
                    cached_singles = pipeline.get_cached_singles()
                    new_factor_set = set(factor_names)
                    extend_factors = [c["factor_name"] for c in cached_singles
                                      if c["factor_name"] not in new_factor_set]
                    if extend_factors:
                        _correlation_logs.append(
                            f"[阶段1a/3] 增量扩展 {len(extend_factors)} 个存量因子缓存: "
                            f"{existing_as_of} → {effective_end_date}"
                        )
                        _correlation_progress.advance(
                            phase="cache_extend", phase_label="增量扩展存量缓存",
                            done=0, total=len(extend_factors),
                        )
                        extend_ok = 0
                        extend_fail = 0
                        _extend_lock = threading.Lock()

                        def _extend_one(fname_: str):
                            t0 = time.time()
                            r = pipeline.extend_single_factor_cache(
                                fname_, new_end_date=effective_end_date, timeout=120,
                            )
                            elapsed = round(time.time() - t0, 1)
                            return fname_, r, elapsed

                        max_w = 1  # 串行执行：每个 extend 峰值 ~210MB，并发会导致 OOM
                        with ThreadPoolExecutor(max_workers=max_w) as executor:
                            futures = {executor.submit(_extend_one, fn): fn for fn in extend_factors}
                            done_count = 0
                            for future in as_completed(futures):
                                done_count += 1
                                try:
                                    fname, r, elapsed = future.result()
                                    with _extend_lock:
                                        if r and r.success:
                                            extend_ok += 1
                                        else:
                                            extend_fail += 1
                                            err = getattr(r, 'error', '') if r else ''
                                            if err:
                                                _correlation_logs.append(
                                                    f"  [{done_count}/{len(extend_factors)}] {fname} 扩展失败: {err} ({elapsed}s)", "WARN"
                                                )
                                except Exception as e:
                                    fn = futures[future]
                                    with _extend_lock:
                                        extend_fail += 1
                                        _correlation_logs.append(
                                            f"  [{done_count}/{len(extend_factors)}] {fn} 扩展异常: {e}", "WARN"
                                        )
                                _correlation_progress.advance(done=done_count)
                                # 每 50 个因子打印一次进度
                                if done_count % 50 == 0 or done_count == len(extend_factors):
                                    _correlation_logs.append(
                                        f"  存量缓存扩展进度: {done_count}/{len(extend_factors)} "
                                        f"(成功 {extend_ok}, 失败 {extend_fail})"
                                    )
                        _correlation_logs.append(
                            f"阶段1a完成: 扩展 {extend_ok} 个, 失败 {extend_fail} 个"
                        )

                # Phase 1b: 生成新因子缓存
                _correlation_logs.append(f"[阶段1b/3] 生成新因子缓存 (共 {len(factor_names)} 个)")
                _correlation_progress.advance(
                    phase="cache_gen", phase_label="生成新因子缓存",
                    done=0, total=len(factor_names),
                )
                phase1_t0 = time.time()
                _gen_lock = threading.Lock()

                def _compute_one(fname_: str):
                    t0 = time.time()
                    logger.info(f"[{mode}] 生成 {fname_} 缓存")
                    r = pipeline.compute_single_factor(
                        fname_, end_date=effective_end_date or None,
                    )
                    elapsed = round(time.time() - t0, 1)
                    return fname_, r, elapsed

                max_w = min(4, len(factor_names))
                with ThreadPoolExecutor(max_workers=max_w) as executor:
                    futures = {executor.submit(_compute_one, fn): fn for fn in factor_names}
                    done_count = 0
                    for future in as_completed(futures):
                        done_count += 1
                        try:
                            fname, result, elapsed = future.result()
                            with _gen_lock:
                                if result and result.success:
                                    success_factors.append(fname)
                                    _correlation_logs.append(f"  [{done_count}/{len(factor_names)}] {fname} 缓存生成成功 ({elapsed}s)")
                                else:
                                    err_msg = getattr(result, 'error', '未知错误') if result else '返回None'
                                    failed_factors.append((fname, err_msg))
                                    _correlation_logs.append(f"  [{done_count}/{len(factor_names)}] {fname} 缓存生成失败: {err_msg} ({elapsed}s)", "WARN")
                        except Exception as e:
                            fn = futures[future]
                            with _gen_lock:
                                failed_factors.append((fn, str(e)))
                                _correlation_logs.append(f"  [{done_count}/{len(factor_names)}] {fn} 缓存生成异常: {e}", "WARN")
                        _correlation_progress.advance(done=done_count)
                phase1_elapsed = round(time.time() - phase1_t0, 1)
                compute_factors = success_factors if mode == "full" else factor_names
                _correlation_logs.append(f"阶段1b完成: 成功 {len(success_factors)}, 失败 {len(failed_factors)}, 耗时 {phase1_elapsed}s")
            else:
                # cache_only: 直接用已有缓存
                compute_factors = factor_names
                _correlation_logs.append(f"[缓存模式] 跳过因子代码执行，直接使用已有 {len(factor_names)} 个缓存")

            if not compute_factors:
                _correlation_logs.append("无可计算的因子，终止", "ERROR")
                logger.error(f"[{mode}] 无可计算的因子")
                _correlation_progress.finish("failed", "无可计算的因子")
                _update_job_status(job_id, "failed")
                return

            # ── Phase 1 → Phase 2 过渡：释放因子计算加载器，回收 ~15-20GB 内存 ──
            if mode in ("full", "smart_incremental", "incremental"):
                try:
                    pipeline._loader_instance = None
                    pipeline._static_loader_instance = None
                    del pipeline
                    import gc
                    gc.collect()
                    _correlation_logs.append(
                        f"已释放 Phase 1 数据加载器 (static_factors + realtime 缓存)"
                    )
                except Exception as e:
                    logger.warning(f"释放 Phase 1 加载器异常: {e}")

            # Phase 2: 计算相关性
            _correlation_progress.advance(phase="matrix_compute", phase_label="计算相关性矩阵", done=0, total=1)
            loader = FactorValueLoader(source="single")
            engine = CorrelationEngine(loader)
            phase2_t0 = time.time()

            # 超时保护: 仅保护阶段2矩阵计算（GEMM 应在 2 分钟内完成，30 分钟兜底）
            timeout_timer = threading.Timer(_MATRIX_TIMEOUT_SEC, _stop_event.set)
            timeout_timer.daemon = True
            timeout_timer.start()

            if mode in ("smart_incremental", "incremental"):
                # 增量模式：使用所有已缓存因子计算完整矩阵（向量化 GEMM，比逐对快 20-40 倍）
                matrix_factors = loader.get_available_factors()
                new_set = set(compute_factors)
                _correlation_logs.append(
                    f"[阶段2/3] 向量化矩阵计算: {len(matrix_factors)} 个因子 "
                    f"(含 {len(new_set)} 个新因子)"
                )
            else:
                # full / cache_only: 使用指定因子列表
                matrix_factors = compute_factors
                _correlation_logs.append(f"[阶段2/3] 计算 {len(matrix_factors)} 因子的完整相关性矩阵")

            def _matrix_progress(done: int, total: int):
                _correlation_progress.advance(done=done, total=total)

            result = engine.compute_full_matrix(
                matrix_factors,
                as_of_date=as_of_date,
                save_hdf5=True,
                on_progress=_matrix_progress,
                stop_event=_stop_event,
            )
            _latest_result = result
            _correlation_progress.advance(done=1)
            records = result.to_db_records(threshold=0)
            phase2_elapsed = round(time.time() - phase2_t0, 1)
            _correlation_logs.append(
                f"阶段2完成: {len(result.factor_names)} 因子矩阵, "
                f"{len(records)} 对相关性记录, 耗时 {phase2_elapsed}s"
            )
            if hasattr(result, 'high_corr_pairs'):
                high_pairs = [p for p in (result.high_corr_pairs or []) if abs(p.get('correlation', 0)) > 0.7]
                if high_pairs:
                    _correlation_logs.append(f"  发现 {len(high_pairs)} 对高相关因子 (|r|>0.7)")

            # Phase 3: 写 DB
            _correlation_progress.advance(phase="db_persist", phase_label="写入数据库", done=0, total=1)
            _correlation_logs.append(f"[阶段3/3] 写入数据库 ({len(records)} 条记录)")
            phase3_t0 = time.time()
            if records:
                _persist_correlations_batch(records)
            if _latest_result:
                _persist_correlation_metadata(_latest_result)
            _correlation_progress.advance(done=1)
            phase3_elapsed = round(time.time() - phase3_t0, 1)
            _correlation_logs.append(f"阶段3完成: DB 写入耗时 {phase3_elapsed}s")

            _correlation_progress.finish("success")
            _update_job_status(job_id, "success")
            total_elapsed = _correlation_progress.snapshot().get("elapsed_sec", 0)

            # --- 完整汇总日志 ---
            _correlation_logs.append("=" * 50)
            _correlation_logs.append(f"计算完成汇总 (mode={mode})")
            _correlation_logs.append(f"  总因子数: {len(factor_names)}")
            if mode in ("full", "smart_incremental", "incremental"):
                _correlation_logs.append(f"  缓存成功: {len(success_factors)}")
                _correlation_logs.append(f"  缓存失败: {len(failed_factors)}")
            _correlation_logs.append(f"  相关性记录: {len(records)} 对")
            _correlation_logs.append(f"  阶段耗时: 缓存={phase1_elapsed}s | 矩阵={phase2_elapsed}s | 写DB={phase3_elapsed}s")
            _correlation_logs.append(f"  总耗时: {total_elapsed}s")
            if failed_factors:
                # 错误分类统计
                timeout_errors = [(f, e) for f, e in failed_factors if "超时" in e or "timeout" in e.lower()]
                code_errors = [(f, e) for f, e in failed_factors if f not in dict(timeout_errors)]
                _correlation_logs.append(f"  错误分类: 超时 {len(timeout_errors)} 个, 代码/其他 {len(code_errors)} 个")
                _correlation_logs.append(f"  失败因子清单:")
                for fname, err in failed_factors:
                    err_short = err[:80] + "..." if len(err) > 80 else err
                    _correlation_logs.append(f"    - {fname}: {err_short}")
            _correlation_logs.append("=" * 50)
            logger.info(f"相关性计算完成: mode={mode}, factors={len(factor_names)}, records={len(records)}, elapsed={total_elapsed}s")

        except Exception as e:
            was_cancelled = _stop_event.is_set()
            status = "cancelled" if was_cancelled else "failed"
            error_msg = "计算被用户取消" if was_cancelled else str(e)
            logger.error(f"相关性计算{status}: {e}", exc_info=not was_cancelled)
            _correlation_logs.append(f"计算{status}: {error_msg}", "WARN" if was_cancelled else "ERROR")
            _correlation_progress.finish(status, error_msg)
            _update_job_status(job_id, status)
        finally:
            if timeout_timer is not None:
                timeout_timer.cancel()
            _stop_event.clear()

            # 强制内存清理：无论成功/失败/取消，都释放大对象
            try:
                FactorValueLoader.invalidate_single_cache()  # 清空类级别因子缓存
                import gc
                gc.collect()
                logger.info("已执行内存清理: _single_cache.clear() + gc.collect()")
            except Exception as e:
                logger.warning(f"内存清理异常: {e}")


def _get_loader(source: str = "auto") -> FactorValueLoader:
    global _correlation_loader
    if _correlation_loader is None or getattr(_correlation_loader, '_source', None) != source:
        _correlation_loader = FactorValueLoader(source=source)
    return _correlation_loader


class CorrelationComputeRequest(BaseModel):
    as_of_date: Optional[str] = Field(None, description="截止日期 (YYYY-MM-DD)，默认数据最新日期")
    factor_names: Optional[List[str]] = Field(None, description="指定因子列表，默认全部已改造因子")
    force_recompute: bool = Field(False, description="强制重新计算，忽略缓存")
    db_threshold: float = Field(0, description="写入 DB 的相关性阈值 (threshold=0 全量存储)")
    mode: str = Field("cache_only", description="full=执行因子代码+计算矩阵, cache_only=仅用已有缓存计算矩阵")


@router.post("/correlations/compute", summary="触发因子相关性矩阵计算")
def compute_correlations(req: CorrelationComputeRequest):
    """触发因子相关性矩阵计算（后台线程池执行）。

    mode:
    - "full": 先执行因子代码生成单因子缓存，再计算矩阵
    - "cache_only": 只用已有的 single/*.parquet 计算矩阵（快速）
    """
    if _computing_lock.locked() and not req.force_recompute:
        return {
            "status": "computing",
            "message": "相关性计算正在进行中",
            "progress": _correlation_progress.snapshot(),
        }

    # 确定因子列表
    if req.factor_names:
        factor_names = req.factor_names
    else:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT factor_name FROM aistock_factor_catalog
                    WHERE transformation_status = 'SUCCESS'
                      AND is_available = TRUE
                      AND qe_code_path IS NOT NULL
                    ORDER BY factor_name
                """)
                factor_names = [row[0] for row in cur.fetchall()]

    if not factor_names:
        return {"status": "error", "message": "无可计算的因子"}

    global _compute_future
    _compute_future = _compute_executor.submit(_run_correlation_compute, req.mode, factor_names, req.as_of_date)

    return {
        "status": "accepted",
        "message": f"已提交 {len(factor_names)} 因子的相关性计算任务 (mode={req.mode})",
        "factor_count": len(factor_names),
        "as_of_date": req.as_of_date or "latest",
        "mode": req.mode,
    }


class IncrementalComputeRequest(BaseModel):
    factor_names: List[str] = Field(..., description="新入库的因子名列表")
    as_of_date: Optional[str] = Field(None, description="截止日期")


@router.post("/correlations/compute-incremental", summary="增量相关性计算")
def compute_incremental_correlations(req: IncrementalComputeRequest):
    """增量计算指定因子与全部已缓存因子之间的相关性。

    流程: 执行新因子代码 → 存单因子缓存 → 读已有缓存 → 逐对计算 → UPSERT DB
    """
    if _computing_lock.locked():
        return {"status": "computing", "message": "正在计算中", "progress": _correlation_progress.snapshot()}

    global _compute_future
    _compute_future = _compute_executor.submit(_run_correlation_compute, "incremental", req.factor_names, req.as_of_date)

    return {
        "status": "accepted",
        "message": f"已提交 {len(req.factor_names)} 个因子的增量相关性计算",
        "factor_names": req.factor_names,
    }


@router.post("/correlations/compute-smart-incremental", summary="智能增量相关性计算")
def compute_correlations_smart_incremental(
    as_of_date: Optional[str] = None,
    dry_run: bool = False,
):
    """自动检测未计算过相关性的因子，只对这些因子做增量计算。

    检测逻辑：查询 transformation_status='SUCCESS' AND is_available=TRUE
    且 correlation_computed_at IS NULL 的因子。

    dry_run=True 时仅返回待计算因子列表，不启动计算。
    """
    if not dry_run and _computing_lock.locked():
        return {"status": "computing", "message": "正在计算中", "progress": _correlation_progress.snapshot()}

    # 查询未计算过相关性的因子
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT factor_name FROM aistock_factor_catalog
                WHERE transformation_status = 'SUCCESS'
                  AND is_available = TRUE
                  AND qe_code_path IS NOT NULL
                  AND correlation_computed_at IS NULL
                ORDER BY factor_name
            """)
            new_factors = [row[0] for row in cur.fetchall()]

    if not new_factors:
        return {
            "status": "no_new_factors",
            "message": "所有因子均已计算过相关性，无需增量计算",
        }

    if dry_run:
        return {
            "status": "preview",
            "message": f"检测到 {len(new_factors)} 个未计算因子",
            "new_factor_count": len(new_factors),
            "new_factors": new_factors,
        }

    global _compute_future
    _compute_future = _compute_executor.submit(_run_correlation_compute, "smart_incremental", new_factors, as_of_date)

    return {
        "status": "accepted",
        "message": f"已检测到 {len(new_factors)} 个未计算因子，正在启动智能增量计算",
        "new_factor_count": len(new_factors),
        "new_factors": new_factors[:20],
    }


@router.post("/correlations/cancel", summary="取消正在进行的相关性计算")
def cancel_correlation_compute():
    """发送取消信号，计算将在当前交易日完成后中断。"""
    if not _computing_lock.locked():
        return {"status": "idle", "message": "当前无计算任务在执行"}

    _stop_event.set()
    _correlation_logs.append("收到取消请求，正在中断计算...", "WARN")

    if _compute_future is not None:
        _compute_future.cancel()

    return {"status": "cancelling", "message": "已发送取消信号，计算将在当前天完成后中断"}


class CorrelationAnalyzeRequest(BaseModel):
    factor_a: str = Field(..., description="因子A名称")
    factor_b: str = Field(..., description="因子B名称")


@router.post("/correlations/pair/analyze", summary="LLM 分析因子对相关性")
async def analyze_correlation_pair(request: CorrelationAnalyzeRequest):
    """调用 correlation_analysis Agent 分析两个因子的相关性成因。

    读取因子源代码、指标和描述，通过 LLM 从金融学/数学/代码维度给出分析。
    Agent 模型和提示词可在 /quantevolver/prompts 页面配置。
    """
    from ..services.quantevolver.correlation_llm_agent import CorrelationLLMAgent

    agent = CorrelationLLMAgent()
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: agent.analyze_pair(request.factor_a, request.factor_b),
    )
    return result


@router.get("/correlations/cache-status", summary="因子缓存状态")
def get_cache_status():
    """返回单因子 parquet 缓存的状态概览。"""
    try:
        from ..services.quantevolver.factor_value_pipeline import FactorValuePipeline
        pipeline = FactorValuePipeline()
        status = pipeline.get_cache_status()
        return status
    except Exception as e:
        logger.error(f"获取缓存状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/correlations/status", summary="查询相关性计算状态")
def get_correlation_status():
    """查询最新的因子相关性计算状态和结果摘要。

    计算进行中时跳过 DB 查询（直接返回缓存值），避免与计算线程竞争连接池。
    仅 idle 状态才查 DB 刷新缓存。
    """
    is_computing = _computing_lock.locked()

    if not is_computing:
        # idle 状态: 查 DB 刷新缓存
        meta = None
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT as_of_date, num_factors, num_high_corr_pairs,
                               avg_correlation, computation_time_sec, hdf5_path, created_at
                        FROM qe_correlation_metadata
                        ORDER BY created_at DESC LIMIT 1
                    """)
                    row = cur.fetchone()
                    if row:
                        meta = {
                            "as_of_date": str(row[0]),
                            "num_factors": row[1],
                            "num_high_corr_pairs": row[2],
                            "avg_correlation": row[3],
                            "computation_time_sec": row[4],
                            "hdf5_path": row[5],
                            "created_at": row[6].isoformat() if row[6] else None,
                        }
        except Exception as e:
            logger.warning(f"读取相关性 job 元数据失败: {e}")

        db_count = 0
        uncorrelated_count = 0
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM qe_factor_correlations")
                    db_count = cur.fetchone()[0]
                    cur.execute("""
                        SELECT COUNT(*) FROM aistock_factor_catalog
                        WHERE transformation_status = 'SUCCESS'
                          AND is_available = TRUE
                          AND qe_code_path IS NOT NULL
                          AND correlation_computed_at IS NULL
                    """)
                    uncorrelated_count = cur.fetchone()[0]
        except Exception as e:
            logger.warning(f"读取相关性统计计数失败: {e}")

        _status_db_cache["meta"] = meta
        _status_db_cache["db_count"] = db_count
        _status_db_cache["uncorrelated_count"] = uncorrelated_count
    # else: computing 状态 — 跳过 DB 查询，使用缓存值

    return {
        "status": "computing" if is_computing else "idle",
        "progress": _correlation_progress.snapshot(),
        "db_correlation_count": _status_db_cache.get("db_count", 0),
        "uncorrelated_factor_count": _status_db_cache.get("uncorrelated_count", 0),
        "latest_computation": _status_db_cache.get("meta"),
        "in_memory_result": _latest_result is not None,
    }


@router.get("/correlations/logs", summary="获取相关性计算实时日志")
def get_correlation_logs(after_index: int = -1):
    """获取相关性计算的实时日志条目。

    前端轮询此端点实现实时日志流。参数 after_index 用于增量获取：
    - after_index=-1: 返回全部累积日志（页面首次加载或回访时）
    - after_index=N: 只返回 index > N 的新日志条目
    """
    entries = _correlation_logs.get_since(after_index)
    return {
        "entries": entries,
        "total_count": len(_correlation_logs.get_since(-1)),
    }


@router.get("/correlations/matrix", summary="获取完整相关性矩阵")
def get_correlation_matrix(
    as_of_date: Optional[str] = None,
    threshold: float = 0.0,
):
    """获取完整因子相关性矩阵（用于前端热力图）。

    优先返回内存中的最新结果，否则从 HDF5 加载。
    threshold: 只在 high_corr_pairs 中返回 |corr| > threshold 的对。
    """
    import numpy as np

    result = _latest_result

    # 尝试从 HDF5 加载
    if result is None:
        try:
            loader = _get_loader()
            engine = CorrelationEngine(loader)
            hdf5_path = engine.get_latest_hdf5()
            if hdf5_path and os.path.exists(hdf5_path):
                result = CorrelationResult.from_hdf5(hdf5_path)
                logger.info(f"从 HDF5 加载相关性矩阵: {hdf5_path}, {len(result.factor_names)} 因子")
        except Exception as e:
            logger.warning(f"HDF5 加载失败: {e}", exc_info=True)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="无可用的相关性矩阵。请先调用 POST /correlations/compute 触发计算。",
        )

    # 构建响应
    matrix_list = []
    for row in result.matrix:
        matrix_list.append([
            round(float(v), 6) if not np.isnan(v) else None
            for v in row
        ])

    pair_threshold = max(threshold, 0.5)
    high_pairs = result.get_high_corr_pairs(pair_threshold)

    # 过滤已删除的因子（HDF5 矩阵可能包含已删除因子的旧数据）
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT factor_name FROM aistock_factor_catalog")
                existing_factors = {row[0] for row in cur.fetchall()}
        high_pairs = [
            p for p in high_pairs
            if p.get("factor_a") in existing_factors and p.get("factor_b") in existing_factors
        ]
    except Exception as e:
        logger.warning(f"过滤已删除因子时出错: {e}")

    # 确保 metadata 中的 numpy 类型可序列化
    safe_metadata = {}
    for k, v in result.metadata.items():
        if isinstance(v, (np.integer,)):
            safe_metadata[k] = int(v)
        elif isinstance(v, (np.floating,)):
            safe_metadata[k] = float(v)
        elif isinstance(v, np.ndarray):
            safe_metadata[k] = v.tolist()
        else:
            safe_metadata[k] = v

    return {
        "as_of_date": result.as_of_date,
        "factor_names": result.factor_names,
        "factor_count": len(result.factor_names),
        "matrix": matrix_list,
        "effective_window": int(result.effective_window),
        "computation_time_sec": float(result.computation_time_sec),
        "high_corr_pairs": high_pairs[:200],
        "high_corr_count": len(high_pairs),
        "metadata": safe_metadata,
    }


@router.get("/correlations/pair", summary="查询特定因子对的相关性")
async def get_correlation_pair(
    fa: str,
    fb: str,
    as_of_date: Optional[str] = None,
    include_daily: bool = False,
):
    """查询两个因子的相关性。

    include_daily=True 时使用缓存计算每日截面相关性时序（用于趋势分析）。
    FactorValueLoader 内置单因子 DataFrame 缓存（TTL 1h），
    不同因子对共享已加载因子，避免重复 I/O。
    """
    logger.info(f"[correlations/pair] fa={fa}, fb={fb}, include_daily={include_daily}")
    # 查 DB
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取 catalog_id
                cur.execute("""
                    SELECT DISTINCT ON (factor_name) factor_name, id
                    FROM aistock_factor_catalog
                    WHERE factor_name IN (%s, %s)
                    ORDER BY factor_name, id
                """, (fa, fb))
                id_map = {row[0]: row[1] for row in cur.fetchall()}
                fa_id, fb_id = id_map.get(fa), id_map.get(fb)
                row = None
                if fa_id and fb_id:
                    a_id, b_id = min(fa_id, fb_id), max(fa_id, fb_id)
                    cur.execute("""
                        SELECT correlation, method, computed_at, as_of_date, data_window_days
                        FROM qe_factor_correlations
                        WHERE factor_a_id = %s AND factor_b_id = %s
                    """, (a_id, b_id))
                    row = cur.fetchone()
    except Exception as e:
        logger.warning(f"查询因子对相关性失败: {e}")
        row = None

    db_result = None
    if row:
        as_of = row[3]
        db_result = {
            "correlation": float(row[0]),
            "method": row[1],
            "computed_at": row[2].isoformat() if row[2] else None,
            "data_period": f"{row[4]}d_as_of_{as_of}" if as_of else None,
        }

    # include_daily: 使用带缓存的 compute_pairwise 计算每日时序
    daily_data = None
    if include_daily:
        try:
            loader = _get_loader(source="single")
            engine = CorrelationEngine(loader)
            loop = asyncio.get_event_loop()
            pair_result = await loop.run_in_executor(
                None,
                lambda: engine.compute_pairwise(fa, fb, as_of_date),
            )
            daily_data = {
                "correlation_ewma": pair_result.correlation,
                "effective_days": pair_result.effective_days,
                "avg_stocks_per_day": pair_result.avg_stocks_per_day,
                "daily_correlations": pair_result.daily_correlations[-60:],
            }
        except Exception as e:
            daily_data = {"error": str(e)}

    # 无 DB 记录且无 daily_data 时，使用缓存计算兜底
    if not db_result and not daily_data:
        try:
            loader = _get_loader(source="single")
            engine = CorrelationEngine(loader)
            loop = asyncio.get_event_loop()
            pair_result = await loop.run_in_executor(
                None,
                lambda: engine.compute_pairwise(fa, fb, as_of_date),
            )
            daily_data = {
                "correlation_ewma": pair_result.correlation,
                "effective_days": pair_result.effective_days,
                "avg_stocks_per_day": pair_result.avg_stocks_per_day,
                "daily_correlations": pair_result.daily_correlations[-60:],
            }
        except Exception as e:
            logger.warning(f"构建缓存的快速相关性结果失败: {e}")

    # 查询两个因子的完整指标（catalog + independent + classification + source_code）
    factor_metrics = {}
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for fn in [fa, fb]:
                    cur.execute("""
                        SELECT factor_name, source, ic, sharpe, annualized_return,
                               max_drawdown, is_sota_factor, is_available,
                               realtime_code_text, asset_path, qe_code_path
                        FROM aistock_factor_catalog WHERE factor_name = %s LIMIT 1
                    """, (fn,))
                    catalog_row = cur.fetchone()

                    cur.execute("""
                        SELECT ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir,
                               ic_positive_ratio, top_sharpe, top_max_drawdown, top_annual_return,
                               top_excess_annual_return, coverage, turnover, group_return_monotonicity
                        FROM aistock_factor_metrics
                        WHERE factor_name = %s AND eval_window = 'full'
                        ORDER BY calculated_at DESC LIMIT 1
                    """, (fn,))
                    ind_row = cur.fetchone()

                    cur.execute("""
                        SELECT grade, category, llm_analysis, description,
                               factor_dimension, grade_reason, factor_source
                        FROM qe_factor_classification WHERE factor_name = %s LIMIT 1
                    """, (fn,))
                    cl_row = cur.fetchone()

                    # 读取源代码: DB realtime_code_text → 文件系统 qe_code_path → asset_path
                    source_code = None
                    if catalog_row:
                        code_text = catalog_row.get("realtime_code_text")
                        if code_text and code_text.strip():
                            source_code = code_text
                        else:
                            for path_key in ["qe_code_path", "asset_path"]:
                                rel_path = catalog_row.get(path_key)
                                if rel_path:
                                    full_path = os.path.join(_PROJECT_ROOT, rel_path)
                                    if os.path.isfile(full_path):
                                        try:
                                            with open(full_path, "r", encoding="utf-8") as f:
                                                source_code = f.read()
                                        except Exception as e:
                                            logger.warning(f"读取因子源码文件失败 {full_path}: {e}")
                                        break

                    # 构建 catalog dict，移除内部字段
                    catalog_dict = None
                    if catalog_row:
                        catalog_dict = dict(catalog_row)
                        for k in ("realtime_code_text", "asset_path", "qe_code_path"):
                            catalog_dict.pop(k, None)

                    factor_metrics[fn] = {
                        "catalog": catalog_dict,
                        "independent": dict(ind_row) if ind_row else None,
                        "classification": dict(cl_row) if cl_row else None,
                        "source_code": source_code,
                    }
    except Exception as e:
        logger.warning(f"查询因子指标失败: {e}")

    return {
        "factor_a": fa,
        "factor_b": fb,
        "db_result": db_result,
        "daily_analysis": daily_data,
        "factor_metrics": factor_metrics,
    }


@router.post("/correlations/subset", summary="查询因子子集间的相关性")
def get_correlation_subset(
    body: dict,
):
    """查询指定因子列表内部的两两相关性。

    body: {"factor_names": ["f1", "f2", ...], "threshold": 0.5}
    返回 |corr| > threshold 的因子对列表，按 |corr| 降序。
    比 /correlations/matrix 轻量得多（不返回完整矩阵）。
    """
    factor_names = body.get("factor_names", [])
    threshold = body.get("threshold", 0.5)
    if len(factor_names) < 2:
        return {"pairs": [], "total": 0}

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT ON (factor_name) id, factor_name
                    FROM aistock_factor_catalog
                    WHERE factor_name = ANY(%s)
                    ORDER BY factor_name, id
                """, (factor_names,))
                id_map = {}
                name_map = {}
                for row in cur.fetchall():
                    fid, fname = row
                    if fname not in id_map:
                        id_map[fname] = fid
                        name_map[fid] = fname

                if len(id_map) < 2:
                    return {"pairs": [], "total": 0}

                fids = list(name_map.keys())
                cur.execute("""
                    SELECT factor_a_id, factor_b_id, correlation
                    FROM qe_factor_correlations
                    WHERE factor_a_id = ANY(%s) AND factor_b_id = ANY(%s)
                      AND ABS(correlation) > %s
                    ORDER BY ABS(correlation) DESC
                """, (fids, fids, threshold))

                pairs = []
                for row in cur.fetchall():
                    a_name = name_map.get(row[0])
                    b_name = name_map.get(row[1])
                    if a_name and b_name:
                        pairs.append({
                            "factor_a": a_name,
                            "factor_b": b_name,
                            "correlation": round(float(row[2]), 6),
                        })

        return {"pairs": pairs, "total": len(pairs)}
    except Exception as e:
        logger.exception("查询因子子集相关性失败")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/correlations/factors/{factor_name}/related", summary="查询某个因子的高相关因子")
def get_related_factors(
    factor_name: str,
    threshold: float = 0.5,
    limit: int = 200,
    include_metrics: bool = False,
):
    """查询某个因子的所有高相关因子（|corr| > threshold）。

    当 include_metrics=true 时，返回每个因子的指标（IC/Sharpe/年化/回撤）、source、is_available，
    以及基准因子自身的 base_metrics。用于因子去重批量管理。
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取因子的 catalog_id
                cur.execute("""
                    SELECT DISTINCT ON (factor_name) id
                    FROM aistock_factor_catalog
                    WHERE factor_name = %s
                    ORDER BY factor_name, id
                """, (factor_name,))
                f_row = cur.fetchone()
                if not f_row:
                    return {
                        "factor_name": factor_name,
                        "threshold": threshold,
                        "related_count": 0,
                        "related_factors": [],
                    }
                f_id = f_row[0]

                if include_metrics:
                    cur.execute("""
                        SELECT
                            CASE WHEN c.factor_a_id = %s THEN cat_b.factor_name
                                 ELSE cat_a.factor_name END AS related_factor,
                            c.correlation,
                            c.method,
                            c.computed_at,
                            CASE WHEN c.factor_a_id = %s THEN cat_b.source
                                 ELSE cat_a.source END AS factor_source,
                            CASE WHEN c.factor_a_id = %s THEN cat_b.is_available
                                 ELSE cat_a.is_available END AS factor_available
                        FROM qe_factor_correlations c
                        JOIN aistock_factor_catalog cat_a ON c.factor_a_id = cat_a.id
                        JOIN aistock_factor_catalog cat_b ON c.factor_b_id = cat_b.id
                        WHERE (c.factor_a_id = %s OR c.factor_b_id = %s)
                          AND ABS(c.correlation) > %s
                        ORDER BY ABS(c.correlation) DESC
                        LIMIT %s
                    """, (f_id, f_id, f_id, f_id, f_id, threshold, limit))

                    related = []
                    factor_names_to_fetch = []
                    for row in cur.fetchall():
                        factor_names_to_fetch.append(row[0])
                        related.append({
                            "factor": row[0],
                            "correlation": round(float(row[1]), 6),
                            "method": row[2],
                            "computed_at": row[3].isoformat() if row[3] else None,
                            "source": row[4],
                            "is_available": row[5],
                        })

                    # 批量获取独立指标（包含基准因子）
                    all_names = list(set(factor_names_to_fetch + [factor_name]))
                    metrics_map = {}
                    cur.execute("""
                        SELECT DISTINCT ON (factor_name)
                               factor_name, ic_mean, rank_ic_mean, icir, rank_icir,
                               top_sharpe, top_excess_sharpe,
                               top_annual_return, top_excess_annual_return,
                               top_max_drawdown, ic_positive_ratio
                        FROM aistock_factor_metrics
                        WHERE factor_name = ANY(%s) AND eval_window = 'full'
                        ORDER BY factor_name, calculated_at DESC
                    """, (all_names,))
                    for mrow in cur.fetchall():
                        metrics_map[mrow[0]] = {
                            "ic_mean": float(mrow[1]) if mrow[1] is not None else None,
                            "rank_ic_mean": float(mrow[2]) if mrow[2] is not None else None,
                            "icir": float(mrow[3]) if mrow[3] is not None else None,
                            "rank_icir": float(mrow[4]) if mrow[4] is not None else None,
                            "top_sharpe": float(mrow[5]) if mrow[5] is not None else None,
                            "top_excess_sharpe": float(mrow[6]) if mrow[6] is not None else None,
                            "top_annual_return": float(mrow[7]) if mrow[7] is not None else None,
                            "top_excess_annual_return": float(mrow[8]) if mrow[8] is not None else None,
                            "top_max_drawdown": float(mrow[9]) if mrow[9] is not None else None,
                            "ic_positive_ratio": float(mrow[10]) if mrow[10] is not None else None,
                        }

                    # 将指标附加到 related factors
                    for item in related:
                        item["metrics"] = metrics_map.get(item["factor"], {})

                    # 基准因子自身信息
                    cur.execute("""
                        SELECT source, is_available
                        FROM aistock_factor_catalog
                        WHERE id = %s
                    """, (f_id,))
                    base_row = cur.fetchone()
                    base_metrics = None
                    if base_row:
                        base_metrics = {
                            **metrics_map.get(factor_name, {}),
                            "source": base_row[0],
                            "is_available": base_row[1],
                        }

                    return {
                        "factor_name": factor_name,
                        "threshold": threshold,
                        "related_count": len(related),
                        "related_factors": related,
                        "base_metrics": base_metrics,
                    }
                else:
                    cur.execute("""
                        SELECT
                            CASE WHEN c.factor_a_id = %s THEN cat_b.factor_name
                                 ELSE cat_a.factor_name END AS related_factor,
                            c.correlation,
                            c.method,
                            c.computed_at
                        FROM qe_factor_correlations c
                        JOIN aistock_factor_catalog cat_a ON c.factor_a_id = cat_a.id
                        JOIN aistock_factor_catalog cat_b ON c.factor_b_id = cat_b.id
                        WHERE (c.factor_a_id = %s OR c.factor_b_id = %s)
                          AND ABS(c.correlation) > %s
                        ORDER BY ABS(c.correlation) DESC
                        LIMIT %s
                    """, (f_id, f_id, f_id, threshold, limit))

                    related = []
                    for row in cur.fetchall():
                        related.append({
                            "factor": row[0],
                            "correlation": round(float(row[1]), 6),
                            "method": row[2],
                            "computed_at": row[3].isoformat() if row[3] else None,
                        })

                    return {
                        "factor_name": factor_name,
                        "threshold": threshold,
                        "related_count": len(related),
                        "related_factors": related,
                    }
    except Exception as e:
        logger.error(f"查询 {factor_name} 相关因子失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class CorrelationBatchAnalyzeRequest(BaseModel):
    base_factor: str = Field(..., description="基准因子名称")
    compare_factors: List[str] = Field(..., description="要比较的因子列表（最多 15 个）")


@router.post("/correlations/batch-analyze", summary="批量 LLM 分析因子相关性")
def batch_analyze_factors(req: CorrelationBatchAnalyzeRequest):
    """使用 LLM 批量分析基准因子与多个比较因子的相关性，给出保留/删除建议。

    限制: compare_factors 最多 15 个（含基准共 ≤16 个因子）。
    """
    if len(req.compare_factors) > 15:
        raise HTTPException(400, f"比较因子最多 15 个，当前 {len(req.compare_factors)} 个")
    if not req.compare_factors:
        raise HTTPException(400, "compare_factors 不能为空")

    from ..services.quantevolver.correlation_llm_agent import CorrelationLLMAgent

    agent = CorrelationLLMAgent()
    try:
        result = agent.analyze_batch(req.base_factor, req.compare_factors)
        return result
    except Exception as e:
        logger.error(f"批量分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── 相关性 DB 持久化辅助函数 ──

def _persist_correlations_batch(records: List[Dict[str, Any]]) -> int:
    """批量写入相关性记录到 qe_factor_correlations 表。

    使用 execute_values 批量 UPSERT，替代逐条 INSERT。
    10 万条记录从 ~20 分钟降至 ~10 秒。
    """
    if not records:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 预加载 factor_name -> catalog_id 映射
            all_factors = set()
            for r in records:
                all_factors.add(r["factor_a"])
                all_factors.add(r["factor_b"])
            factor_list = list(all_factors)
            ph = ",".join(["%s"] * len(factor_list))
            cur.execute(f"""
                SELECT DISTINCT ON (factor_name) factor_name, id
                FROM aistock_factor_catalog
                WHERE factor_name IN ({ph})
                ORDER BY factor_name, id
            """, factor_list)
            catalog_map = {row[0]: row[1] for row in cur.fetchall()}

            # 预处理: 构建去重的 (a_id, b_id) -> row 映射
            seen = {}
            for r in records:
                fa_id = catalog_map.get(r["factor_a"])
                fb_id = catalog_map.get(r["factor_b"])
                if fa_id is None or fb_id is None:
                    continue
                a_id, b_id = min(fa_id, fb_id), max(fa_id, fb_id)
                # 从 data_period 解析 as_of_date
                as_of_date = None
                data_period = r.get("data_period", "")
                if data_period and "as_of_" in data_period:
                    try:
                        as_of_date = data_period.split("as_of_")[1]
                    except (IndexError, ValueError):
                        pass
                seen[(a_id, b_id)] = (a_id, b_id, r["correlation"], r["method"], as_of_date, 252)

            values = list(seen.values())
            if values:
                execute_values(
                    cur,
                    """
                    INSERT INTO qe_factor_correlations
                        (factor_a_id, factor_b_id, correlation, method,
                         as_of_date, data_window_days, computed_at)
                    VALUES %s
                    ON CONFLICT (factor_a_id, factor_b_id) DO UPDATE SET
                        correlation = EXCLUDED.correlation,
                        as_of_date = EXCLUDED.as_of_date,
                        computed_at = NOW()
                    """,
                    values,
                    template="(%s, %s, %s, %s, COALESCE(%s::DATE, CURRENT_DATE), %s, NOW())",
                    page_size=2000,
                )

            # 批量更新 catalog 的 correlation_computed_at 和 pair_count
            if catalog_map:
                catalog_ids = list(catalog_map.values())
                cur.execute("""
                    UPDATE aistock_factor_catalog c SET
                        correlation_computed_at = NOW(),
                        correlation_pair_count = COALESCE(sub.cnt, 0)
                    FROM (
                        SELECT factor_id, COUNT(*) AS cnt FROM (
                            SELECT factor_a_id AS factor_id FROM qe_factor_correlations
                            WHERE factor_a_id = ANY(%s)
                            UNION ALL
                            SELECT factor_b_id AS factor_id FROM qe_factor_correlations
                            WHERE factor_b_id = ANY(%s)
                        ) t GROUP BY factor_id
                    ) sub
                    WHERE c.id = sub.factor_id
                """, (catalog_ids, catalog_ids))

        conn.commit()

    written = len(values)
    logger.info(f"批量写入 {written} 条相关性记录（去重后）")
    return written


def _persist_correlation_metadata(result: CorrelationResult) -> None:
    """写入相关性计算元数据。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_correlation_metadata
                        (as_of_date, num_factors, num_high_corr_pairs,
                         avg_correlation, computation_time_sec, hdf5_path)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (as_of_date) DO UPDATE SET
                        num_factors = EXCLUDED.num_factors,
                        num_high_corr_pairs = EXCLUDED.num_high_corr_pairs,
                        avg_correlation = EXCLUDED.avg_correlation,
                        computation_time_sec = EXCLUDED.computation_time_sec,
                        hdf5_path = EXCLUDED.hdf5_path,
                        created_at = NOW()
                """, (
                    result.as_of_date,
                    len(result.factor_names),
                    result.metadata.get("num_high_corr_07", 0),
                    float(result.metadata.get("avg_correlation", 0)),
                    result.computation_time_sec,
                    result.metadata.get("hdf5_path"),
                ))
    except Exception as e:
        logger.warning(f"写入相关性元数据失败: {e}")


# ── 相关性计算调度 API ────────────────────────────────────────

class CorrelationScheduleRequest(BaseModel):
    dataset: str = Field(
        "correlation_full",
        description="correlation_full | correlation_cache_only | correlation_smart_incremental",
    )
    frequency: str = Field("weekly", description="weekly | daily | manual | 10s | 5m 等")
    enabled: bool = Field(True)
    options: Dict[str, Any] = Field(default_factory=dict, description="附加选项 (at, factor_names 等)")


@router.get("/correlations/schedules", summary="列出相关性计算调度配置")
def list_correlation_schedules():
    """列出所有 dataset LIKE 'correlation_%' 的调度配置。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schedule_id, dataset, mode, enabled, frequency, options,
                       last_run_at, next_run_at, last_status, last_error,
                       created_at, updated_at
                FROM market.ingestion_schedules
                WHERE dataset LIKE 'correlation_%%'
                ORDER BY created_at DESC
            """)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    # 序列化
    for row in rows:
        for k in ("schedule_id",):
            if row.get(k):
                row[k] = str(row[k])
        for k in ("last_run_at", "next_run_at", "created_at", "updated_at"):
            if row.get(k):
                row[k] = row[k].isoformat()
        if isinstance(row.get("options"), str):
            row["options"] = json.loads(row["options"])
    return {"items": rows}


@router.post("/correlations/schedules", summary="创建/更新相关性计算调度")
def upsert_correlation_schedule(req: CorrelationScheduleRequest):
    """创建或更新一个相关性计算调度。"""
    schedule_id = None
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 查是否已存在同 dataset 的调度
            cur.execute(
                "SELECT schedule_id FROM market.ingestion_schedules WHERE dataset=%s",
                (req.dataset,),
            )
            row = cur.fetchone()
            schedule_id = row[0] if row else uuid.uuid4()
            options_json = json.dumps(req.options, ensure_ascii=False, default=str)
            cur.execute("""
                INSERT INTO market.ingestion_schedules
                    (schedule_id, dataset, mode, enabled, frequency, options, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (schedule_id) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    frequency = EXCLUDED.frequency,
                    options = EXCLUDED.options,
                    dataset = EXCLUDED.dataset,
                    updated_at = NOW()
            """, (str(schedule_id), req.dataset, "init", req.enabled, req.frequency, options_json))
        conn.commit()

    # 刷新调度器
    from ..services.quantevolver.correlation_scheduler import correlation_scheduler
    correlation_scheduler.refresh_schedules()

    return {"schedule_id": str(schedule_id), "dataset": req.dataset, "frequency": req.frequency, "enabled": req.enabled}


@router.post("/correlations/schedules/{schedule_id}/toggle", summary="切换调度启用/禁用")
def toggle_correlation_schedule(schedule_id: str, enabled: bool = True):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE market.ingestion_schedules SET enabled=%s, updated_at=NOW() WHERE schedule_id=%s",
                (enabled, schedule_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"调度 {schedule_id} 不存在")
        conn.commit()
    from ..services.quantevolver.correlation_scheduler import correlation_scheduler
    correlation_scheduler.refresh_schedules()
    return {"schedule_id": schedule_id, "enabled": enabled}


@router.post("/correlations/schedules/{schedule_id}/run", summary="立即执行调度")
def run_correlation_schedule_now(schedule_id: str):
    """手动触发一个调度的立即执行。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT dataset, options FROM market.ingestion_schedules WHERE schedule_id=%s",
                (schedule_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"调度 {schedule_id} 不存在")
            dataset, options = row
            if isinstance(options, str):
                options = json.loads(options)
            elif options is None:
                options = {}

    if _computing_lock.locked():
        return {"status": "computing", "message": "正在计算中", "progress": _correlation_progress.snapshot()}

    from ..services.quantevolver.correlation_scheduler import correlation_scheduler
    job_id = correlation_scheduler.submit_job(schedule_id, dataset, options, triggered_by="manual")
    return {"status": "accepted", "job_id": str(job_id), "schedule_id": schedule_id}


@router.delete("/correlations/schedules/{schedule_id}", summary="删除调度")
def delete_correlation_schedule(schedule_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM market.ingestion_schedules WHERE schedule_id=%s AND dataset LIKE 'correlation_%%'",
                (schedule_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"相关性调度 {schedule_id} 不存在")
        conn.commit()
    from ..services.quantevolver.correlation_scheduler import correlation_scheduler
    correlation_scheduler.refresh_schedules()
    return {"deleted": True, "schedule_id": schedule_id}


@router.get("/correlations/jobs", summary="查询相关性计算任务历史")
def list_correlation_jobs(limit: int = 20):
    """返回最近的相关性计算任务记录。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT job_id, job_type, status, created_at, started_at, finished_at, summary
                FROM market.ingestion_jobs
                WHERE job_type LIKE 'correlation_%%'
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    for row in rows:
        if row.get("job_id"):
            row["job_id"] = str(row["job_id"])
        for k in ("created_at", "started_at", "finished_at"):
            if row.get(k):
                row[k] = row[k].isoformat()
        if isinstance(row.get("summary"), str):
            row["summary"] = json.loads(row["summary"])
    return {"items": rows}


# ── 因子值计算 API ──────────────────────────────────────────

_pipeline_instance = None
_pipeline_computing = False


def _get_pipeline():
    global _pipeline_instance
    if _pipeline_instance is None:
        from ..services.quantevolver.factor_value_pipeline import FactorValuePipeline
        _pipeline_instance = FactorValuePipeline()
    return _pipeline_instance


@router.post("/factor-values/compute")
async def compute_factor_values(
    background_tasks: BackgroundTasks,
    factor_names: Optional[List[str]] = None,
    start_date: str = "2024-12-01",
    end_date: str = "2025-12-01",
    max_workers: int = 1,
    timeout_per_factor: int = 600,
):
    """触发批量因子值计算。

    - factor_names: 指定因子列表；为空则计算所有已改造因子
    - start_date/end_date: 计算日期范围
    - max_workers: 并发线程数（建议 1-3）
    """
    global _pipeline_computing
    if _pipeline_computing:
        raise HTTPException(409, "因子值计算正在进行中，请等待完成")

    pipeline = _get_pipeline()

    async def _run():
        global _pipeline_computing
        _pipeline_computing = True
        try:
            result = await asyncio.to_thread(
                pipeline.compute_factor_values,
                factor_names=factor_names,
                start_date=start_date,
                end_date=end_date,
                max_workers=max_workers,
                timeout_per_factor=timeout_per_factor,
            )
            logger.info(
                f"因子值计算完成: {result.success}/{result.total} 成功, "
                f"耗时 {result.total_elapsed_sec}s"
            )
        except Exception as e:
            logger.error(f"因子值计算异常: {e}")
        finally:
            _pipeline_computing = False

    background_tasks.add_task(_run)
    return {
        "status": "started",
        "message": "因子值计算已在后台启动",
        "factor_count": len(factor_names) if factor_names else "all",
        "date_range": f"{start_date}~{end_date}",
    }


@router.get("/factor-values/status")
def factor_values_status():
    """查询因子值计算状态和可用缓存。"""
    pipeline = _get_pipeline()
    cached = pipeline.get_cached_parquets()
    return {
        "computing": _pipeline_computing,
        "cached_files": cached,
        "cache_count": len(cached),
    }


@router.get("/factor-values/available")
async def factor_values_available(
    limit: Optional[int] = None,
):
    """查询所有可计算因子（已改造成功的 RDAgent 因子）。"""
    pipeline = _get_pipeline()
    try:
        factors = await asyncio.to_thread(
            pipeline.get_computable_factors, limit=limit
        )
    except Exception as e:
        raise HTTPException(500, f"查询可计算因子失败: {e}")

    return {
        "total": len(factors),
        "factors": factors,
    }


@router.get("/system/logs", summary="读取后端日志文件末尾")
def get_system_logs(
    tail: int = 100,
    level: str = "",
    errors_only: bool = False,
):
    """
    返回 backend/logs/aistock.log（或 errors.log）的末尾 N 行。
    level: 过滤级别关键字，如 ERROR / WARN / INFO，空=全部。
    errors_only: True 则只读 errors.log。
    """
    from pathlib import Path
    log_file = Path(__file__).parent.parent / "logs" / ("errors.log" if errors_only else "aistock.log")
    if not log_file.exists():
        return {"ok": True, "lines": [], "exists": False}
    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    if level:
        lines = [l for l in lines if f" {level.upper()} " in l]
    return {"ok": True, "lines": [l.rstrip() for l in lines[-tail:]], "exists": True}
