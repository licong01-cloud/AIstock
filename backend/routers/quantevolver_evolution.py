import logging
import os
import json
import time
import uuid
import asyncio
import threading
import traceback
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, Dict, Any, List, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request, Query, Body
from fastapi.responses import Response, StreamingResponse
import httpx

# 导入未来的 EvolutionService (目前可能为空实现)
from ..services.quantevolver.qe_evolution_service import (
    AutoEvolutionScheduler,
    QE_LOOP_RETRY_MODE_AUTO,
    QE_LOOP_RETRY_MODE_BACKTEST_ONLY,
    QE_LOOP_RETRY_MODE_FULL_TRAIN,
    normalize_qe_loop_retry_mode,
)
from ..services.quantevolver.factor_value_loader import FactorValueLoader
from ..services.quantevolver.correlation_engine import CorrelationEngine, CorrelationResult
from ..services.quantevolver import correlation_compute_service as _correlation_compute_service
from ..db.pg_pool import get_conn
from psycopg2.extras import RealDictCursor, execute_values

from ..services.quantevolver.factor_eligibility_service import FactorEligibilityService
from ..services.quantevolver.experiment_config import (
    ensure_qe_risk_policy,
    normalize_label_horizon,
    normalize_qe_random_seed,
    split_qe_runtime_metadata,
)
from ..services.qe_templates.validator import validate_qe_historical_stock_pool_window
from ..services.quantevolver.factor_official_evaluation_service import CALC_ENGINE
from ..services.quantevolver.label_horizon_schema import ensure_qe_label_horizon_schema
from ..services.quantevolver.seed_contract import ensure_loop_fixed_seed, raise_http_seed_error
from ..services.quantevolver.payload_summary import derive_position_summary_from_enhanced_metrics
from ..services.quantevolver.node_execution import (
    QENodePreflightError,
    get_compute_node,
    normalize_node_parallelism,
    preflight_qe_nodes,
    resolve_custom_loop_nodes,
)
from ..services.strategy_package.promotion_review import PromotionReviewService
from ..services.trading_core.errors import TradingCoreError

OFFICIAL_FACTOR_WINDOW_START = "2018-08-01"
OFFICIAL_FACTOR_WINDOW_END = "2026-04-30"

# RD-Agent QE workspace API base URL
RDAGENT_QE_BASE = os.getenv("RDAGENT_RESULTS_API_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
RDAGENT_QE_TIMEOUT = 30.0

# 项目根目录 (routers/ → 上 2 层)
_PROJECT_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)

logger = logging.getLogger(__name__)



def _position_metric_missing(enhanced_metrics: Dict[str, Any]) -> bool:
    ar = enhanced_metrics.get("absolute_returns") if isinstance(enhanced_metrics, dict) else None
    pos = enhanced_metrics.get("position_summary") if isinstance(enhanced_metrics, dict) else None
    holding = enhanced_metrics.get("holding_audit") if isinstance(enhanced_metrics, dict) else None
    sources = [s for s in (ar, pos, holding) if isinstance(s, dict)]
    return not any(
        s.get("position_count_avg") is not None and s.get("position_count_max") is not None
        for s in sources
    )


def _normalize_rdagent_loop_id(task_id: str, loop_id: str) -> str:
    raw = str(loop_id or "").strip()
    if raw.startswith(task_id + "_"):
        raw = raw[len(task_id) + 1:]
    if raw.isdigit():
        return f"Loop{int(raw)}"
    if raw.lower().startswith("loop") and raw[4:].isdigit():
        return f"Loop{int(raw[4:])}"
    return raw


def _augment_enhanced_metrics_with_positions(
    task_id: str,
    loop_id: str,
    loop_index: Optional[int],
    enhanced_metrics: Dict[str, Any],
) -> tuple[Dict[str, Any], bool]:
    if not isinstance(enhanced_metrics, dict) or not _position_metric_missing(enhanced_metrics):
        return enhanced_metrics, False

    position_summary = derive_position_summary_from_enhanced_metrics(enhanced_metrics)
    if not position_summary:
        logger.debug(
            "QE position enrichment unavailable for %s/%s(loop_index=%s); "
            "enhanced metrics do not include enough compact trade evidence",
            task_id,
            loop_id,
            loop_index,
        )
        return enhanced_metrics, False

    enriched = dict(enhanced_metrics)
    existing = enriched.get("position_summary") if isinstance(enriched.get("position_summary"), dict) else {}
    next_position = {**existing, **{k: v for k, v in position_summary.items() if v is not None}}
    if next_position == existing:
        return enhanced_metrics, False
    enriched["position_summary"] = next_position
    return enriched, True

def _cache_loop_enhanced_metrics(task_id: str, loop_id: str, metrics_json: Dict[str, Any]) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE qe_evolution_loops
                   SET metrics_json = %s::jsonb, updated_at = NOW()
                   WHERE loop_id = %s""",
                (json.dumps(metrics_json, ensure_ascii=False), f"{task_id}_{loop_id}"),
            )
        conn.commit()


def _merge_strategy_runtime_flags(
    strategy_params: Optional[Dict[str, Any]],
    filter_suspended_on_signal: bool,
    suspend_filter_strict: bool = True,
    random_seed: Optional[int] = None,
) -> Dict[str, Any]:
    """Persist runtime signal-filter flags without requiring new task table columns."""
    merged = dict(strategy_params or {})
    _reject_nested_runtime_flags(merged, "strategy_params")
    merged = ensure_qe_risk_policy(merged, source="strategy_params")
    if random_seed not in (None, ""):
        merged["random_seed"] = normalize_qe_random_seed(
            random_seed,
            field_name="strategy_params.random_seed",
        )
    if filter_suspended_on_signal:
        merged["filter_suspended_on_signal"] = True
        merged["suspend_filter_strict"] = bool(suspend_filter_strict)
    return merged


def _reject_nested_runtime_flags(strategy_params: Optional[Dict[str, Any]], context: str) -> None:
    """Runtime execution flags must live at the explicit config layer only."""
    params = dict(strategy_params or {})
    duplicate_keys = {
        "filter_suspended_on_signal",
        "exclude_suspended",
        "suspend_filter_strict",
        "suspend_filter_file",
        "disable_alpha158",
    }.intersection(params)
    if duplicate_keys:
        raise HTTPException(
            status_code=400,
            detail=(
                "suspend filter runtime flags must be sent via top-level request fields, "
                f"not {context}: {sorted(duplicate_keys)}"
            ),
        )


def _hoist_runtime_metadata_from_strategy_params(cfg_dict: Dict[str, Any]) -> None:
    """Move archive/seed metadata out of strategy_params at the API boundary."""

    strategy_params, runtime_metadata = split_qe_runtime_metadata(
        cfg_dict.get("strategy_params") or {}
    )
    cfg_dict["strategy_params"] = strategy_params
    if runtime_metadata:
        runtime_flags = dict(cfg_dict.get("runtime_flags") or {})
        for key, value in runtime_metadata.items():
            runtime_flags.setdefault(key, value)
        cfg_dict["runtime_flags"] = runtime_flags


def _normalize_qe_execution_algo_for_request(execution_algo: Optional[str], context: str) -> Optional[str]:
    """Normalize QE execution algo at API boundary; unsupported values fail loudly."""
    if not execution_algo:
        return None
    try:
        from ..services.quantevolver.config_composer import ConfigComposer
        return ConfigComposer._normalize_execution_algo(execution_algo)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"{context}: {e}") from e


def _model_to_dict(model: BaseModel) -> Dict[str, Any]:
    """Support Pydantic v1/v2 without deprecation warnings."""
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _sync_stock_pool_to_remote(stock_pool_path: str, node: dict):
    """Synchronize one filtered_pool file to a remote node, fail-fast on any problem."""
    from ..services.quantevolver.stock_pool_sync import sync_stock_pool_to_remote_node

    return sync_stock_pool_to_remote_node(stock_pool_path, node)


def _sync_all_filtered_pools_to_remote(node: dict):
    """Synchronize all local filtered_pool_*.txt files to a remote node, fail-fast."""
    from ..services.quantevolver.stock_pool_sync import sync_all_filtered_pools_to_remote_node

    return sync_all_filtered_pools_to_remote_node(node)

router = APIRouter(
    prefix="/quantevolver/evolution",
    tags=["quantevolver_evolution"],
)

factor_metrics_router = APIRouter(
    prefix="/factor-metrics",
    tags=["factor_metrics"],
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
    filter_suspended_on_signal: bool = Field(False, description="生成日频选股信号时使用 suspend_d 过滤已停牌股票")
    suspend_filter_strict: bool = Field(True, description="启用停牌过滤时要求 suspend_d 每个回测交易日审计成功")
    unfilled_handler: Optional[str] = Field(None, description="尾盘涨停未成交处理: TAIL_BOOST(加仓持仓股) / TAIL_SUBSTITUTE(替补买入)")
    unfilled_handler_params: Optional[Dict[str, Any]] = Field(None, description="尾盘处理参数，如 {backup_depth: 15, trigger_minute: 210}")
    enable_sector_hmm: bool = Field(False, description="是否启用行业 HMM 热度调整")
    hmm_model_version_id: Optional[str] = Field(None, description="HMM 模型快照 ID (snapshot_id)")
    hmm_signal_preset: Optional[str] = Field(None, description="HMM 信号系数档位: preset_A(保守,最高+5%) / preset_B(激进,最高+10%)")
    additional_factor_keys: Optional[List[str]] = Field(None, description="从因子库额外添加的因子key列表，格式 ['name||source', ...]，与来源默认因子合并")
    node_id: Optional[str] = Field(None, description="执行节点 ID，None=默认本地节点")
    label_type: Optional[str] = Field(None, description="训练标签类型: close(默认) / open(可执行价) / vwap(均价)")
    label_horizon: Optional[int] = Field(None, description="训练标签期限: 1/3/5/10/20d，默认继承源实验或 1d")
    random_seed: Optional[int] = Field(None, description="QE trainable loops fixed random seed; required for reproducibility")
    # ── Multi-Alpha (Phase 3) ──────────────────────────────────────
    alpha_mode: Optional[str] = Field(None, description="single (默认) / multi")
    multi_alpha_config: Optional[Dict[str, Any]] = Field(None, description="Multi-Alpha 分组配置 JSON")

@router.post("/tasks", summary="创建并启动新的自动演进任务")
async def create_evolution_task(req: EvolutionTaskCreateRequest, background_tasks: BackgroundTasks):
    """
    创建一个新的演进任务，并在后台异步启动状态机流转。
    支持两种来源:
    - qe_experiment: 从已有 QE 实验开始演进
    - rdagent_task_sota: 从 RDAgent task 的 SOTA 资产创建实验后演进
    """
    try:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        base_experiment_id = req.base_experiment_id
        try:
            req_random_seed = normalize_qe_random_seed(req.random_seed, field_name="random_seed")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

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
                custom_params={"random_seed": req_random_seed},
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

        # 节点可用性校验
        if req.node_id:
            from ..services.dispatch_service import DispatchService
            svc = DispatchService()
            node = svc.get_node(req.node_id)
            if not node:
                raise HTTPException(status_code=400, detail=f"节点 {req.node_id} 不存在")
            if node.get("status") == "offline":
                raise HTTPException(status_code=400, detail=f"节点 {req.node_id} 离线，无法接受任务")

            # 远程节点：同步 filtered_pool 股票池文件到远端 qlib instruments 目录
            if req.stock_pool and "filtered_pool" in req.stock_pool:
                _sync_stock_pool_to_remote(req.stock_pool, node)

        # --- 严格参数验证（禁止静默兜底）---
        _VALID_EVOLUTION_MODES = {"auto", "factor_only", "model_only", "joint"}
        if req.evolution_mode and req.evolution_mode not in _VALID_EVOLUTION_MODES:
            raise HTTPException(
                status_code=400,
                detail=f"evolution_mode='{req.evolution_mode}' 无效，允许值: {', '.join(sorted(_VALID_EVOLUTION_MODES))}",
            )

        _VALID_LABEL_TYPES = {"close", "open", "vwap"}
        if req.label_type and req.label_type not in _VALID_LABEL_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"label_type='{req.label_type}' 无效，允许值: {', '.join(sorted(_VALID_LABEL_TYPES))}",
            )
        try:
            req_label_horizon = (
                normalize_label_horizon(req.label_horizon)
                if req.label_horizon not in (None, "")
                else None
            )
            req_random_seed = (
                normalize_qe_random_seed(req.random_seed, field_name="fork.random_seed")
                if req.random_seed not in (None, "")
                else None
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        normalized_execution_algo = _normalize_qe_execution_algo_for_request(
            req.execution_algo,
            "execution_algo",
        )

        if req.strategy_id:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM aistock_strategy_catalog WHERE strategy_id = %s", (req.strategy_id,))
                    if not cur.fetchone():
                        raise HTTPException(
                            status_code=400,
                            detail=f"strategy_id='{req.strategy_id}' 在策略目录中不存在",
                        )

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
            node_id=req.node_id,
            label_horizon=req_label_horizon,
            random_seed=req_random_seed,
        )

        # 保存额外字段（含 evolution_mode）
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_evolution_tasks
                    SET evolution_guidance = %s, source_type = %s, source_task_id = %s,
                        evolution_mode = %s, strategy_id = %s, strategy_params = %s,
                        execution_algo = %s, execution_algo_params = %s,
                        unfilled_handler = %s, unfilled_handler_params = %s,
                        label_type = %s, label_horizon = COALESCE(%s, label_horizon), updated_at = NOW()
                    WHERE task_id = %s
                """, (req.evolution_guidance, req.source_type, req.source_task_id,
                      req.evolution_mode or "auto",
                      req.strategy_id,
                      json.dumps(_merge_strategy_runtime_flags(req.strategy_params, req.filter_suspended_on_signal, req.suspend_filter_strict, req_random_seed)),
                      normalized_execution_algo,
                      json.dumps(req.execution_algo_params) if req.execution_algo_params else None,
                      req.unfilled_handler,
                      json.dumps(req.unfilled_handler_params) if req.unfilled_handler_params else None,
                      req.label_type,
                      req_label_horizon,
                      task_id))
            conn.commit()

        # --- 注入 HMM 模型路径到 strategy_params ---
        if req.enable_sector_hmm and hmm_model_path:
            merged_params = _merge_strategy_runtime_flags(
                req.strategy_params,
                req.filter_suspended_on_signal,
                req.suspend_filter_strict,
                req_random_seed,
            )
            merged_params["sector_hmm_model_path"] = hmm_model_path
            merged_params["enable_sector_hmm"] = True
            merged_params["hmm_model_version_id"] = req.hmm_model_version_id
            if req.hmm_signal_preset:
                merged_params["hmm_signal_preset"] = req.hmm_signal_preset
                # 从 DB 读取模型的 signal_presets 注入到 strategy_params
                try:
                    snapshot = hmm_svc.get_snapshot(req.hmm_model_version_id)
                    if snapshot is None:
                        raise HTTPException(status_code=400, detail=f"HMM 快照 {req.hmm_model_version_id} 不存在，无法读取 signal_presets")
                    config_id = snapshot["config_id"]
                    configs = hmm_svc.list_configs("sector_hmm")
                    for cfg in configs:
                        if cfg["config_id"] == config_id:
                            cj = cfg["config_json"]
                            if isinstance(cj, str):
                                cj = json.loads(cj)
                            merged_params["hmm_config_json"] = cj
                            if "signal_presets" in cj:
                                merged_params["hmm_signal_presets"] = cj["signal_presets"]
                            break
                except HTTPException:
                    raise
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
            # 严格模式：因子验证失败直接报错，不允许带问题启动
            issues = []
            if validation.get("unavailable_factors"):
                issues.append(f"不可用因子: {', '.join(validation['unavailable_factors'])}")
            if validation.get("deleted_factors"):
                issues.append(f"已删除因子: {', '.join(validation['deleted_factors'])}")
            raise HTTPException(
                status_code=400,
                detail=f"因子验证失败，无法启动演进任务。{'; '.join(issues)}。"
                       f"请先通过 /evolution/tasks/{{task_id}}/resolve-factors 修复因子问题。",
            )

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
async def list_evolution_tasks(
    detail: str = Query("summary", pattern="^(summary|full)$", description="summary 默认不返回大 JSON；full 保留旧完整字段"),
    status: str | None = Query(None, description="Optional status filter (e.g. completed, running, failed)"),
    limit: int = Query(50, ge=1, le=200, description="Maximum tasks to return"),
    offset: int = Query(0, ge=0, description="Tasks to skip before returning the current page"),
):
    """
    从数据库中读取所有 qe_evolution_tasks。默认 summary，避免 MCP/列表返回大 JSON。
    """
    try:
        tasks = await scheduler.get_all_tasks(detail=detail, status=status, limit=limit, offset=offset)
        return {"status": "success", "data": tasks, "detail": detail}
    except Exception as e:
        logger.error(f"Failed to list evolution tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def _task_detail_contains_full_loop_payloads(detail_data: dict) -> bool:
    loops = detail_data.get("loops") if isinstance(detail_data, dict) else None
    if not isinstance(loops, list):
        return False
    return any(
        isinstance(loop, dict) and any(key in loop for key in ("config_json", "metrics_json", "agent_analysis"))
        for loop in loops
    )


@router.get("/tasks/{task_id}", summary="获取单个演进任务的详细信息与所有 LOOP")
async def get_evolution_task_detail(
    task_id: str,
    detail: str = Query("summary", pattern="^(summary|full)$", description="summary 返回 compact loop 表；full 返回旧完整 JSON 字段"),
):
    """
    获取任务详情。默认 summary；前端深度页可显式 detail=full。
    """
    try:
        try:
            detail_data = await scheduler.get_task_detail(task_id, detail=detail)
        except TypeError as exc:
            if "detail" not in str(exc):
                raise
            detail_data = await scheduler.get_task_detail(task_id)
        if not detail_data:
            raise HTTPException(status_code=404, detail="Task not found")
        if detail != "full" and not _task_detail_contains_full_loop_payloads(detail_data):
            return {"status": "success", "data": detail_data, "detail": detail}
        for loop in detail_data.get("loops", []):
            metrics = loop.get("metrics_json")
            if isinstance(metrics, str):
                try:
                    metrics = json.loads(metrics)
                except Exception:
                    metrics = None
            if not isinstance(metrics, dict):
                metrics = {}
            if loop.get("status") not in ("completed", "failed"):
                continue
            cached_enhanced = metrics.get("enhanced_metrics")
            if not isinstance(cached_enhanced, dict):
                cached_enhanced = {}
            enhanced, changed = _augment_enhanced_metrics_with_positions(
                task_id,
                f"Loop{loop.get('loop_index')}",
                loop.get("loop_index"),
                cached_enhanced,
            )
            if not changed:
                continue
            metrics["enhanced_metrics"] = enhanced
            loop["metrics_json"] = metrics
        return {"status": "success", "data": detail_data, "detail": detail}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task detail: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/loops/comparison", summary="获取任务所有 Loop 的标量对比表")
def get_evolution_task_loop_comparison(task_id: str):
    try:
        result = scheduler.get_loop_comparison(task_id)
        if not result:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"status": "success", "data": result, "detail": "comparison"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get loop comparison for {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/loops/{loop_index}/config", summary="按需获取单个 Loop 配置")
def get_evolution_loop_config(task_id: str, loop_index: int):
    try:
        result = scheduler.get_loop_payload(task_id, loop_index, "config")
        if not result:
            raise HTTPException(status_code=404, detail="Loop not found")
        return {"status": "success", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get loop config for {task_id}/{loop_index}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/loops/{loop_index}/metrics", summary="按需获取单个 Loop 完整指标")
def get_evolution_loop_metrics(task_id: str, loop_index: int):
    try:
        result = scheduler.get_loop_payload(task_id, loop_index, "metrics")
        if not result:
            raise HTTPException(status_code=404, detail="Loop not found")
        return {"status": "success", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get loop metrics for {task_id}/{loop_index}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/loops/{loop_index}/analysis", summary="按需获取单个 Loop LLM 分析")
def get_evolution_loop_analysis(task_id: str, loop_index: int):
    try:
        result = scheduler.get_loop_payload(task_id, loop_index, "analysis")
        if not result:
            raise HTTPException(status_code=404, detail="Loop not found")
        return {"status": "success", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get loop analysis for {task_id}/{loop_index}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/stop", summary="手动停止/暂停演进任务")
async def stop_evolution_task(task_id: str):
    try:
        stop_result = await scheduler.stop_task(task_id)
        failed_kills = [
            item for item in stop_result.get("loops_killed", [])
            if item.get("error")
        ]
        if failed_kills:
            return {
                "status": "warning",
                "message": (
                    f"Task {task_id} 已停止，但 {len(failed_kills)} 个 Loop 清理失败，请检查详情"
                ),
                "detail": stop_result,
            }
        return {"status": "success", "message": f"Task {task_id} 已停止", "detail": stop_result}
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
    force_full_train: bool = Field(False, description="强制完整训练+回测，忽略 loop 配置中的 backtest_only")


class EvolutionLoopRetryRequest(BaseModel):
    retry_mode: str = Field(
        QE_LOOP_RETRY_MODE_AUTO,
        description=(
            "Retry mode: "
            f"{QE_LOOP_RETRY_MODE_AUTO} / "
            f"{QE_LOOP_RETRY_MODE_BACKTEST_ONLY} / "
            f"{QE_LOOP_RETRY_MODE_FULL_TRAIN}"
        ),
    )

@router.post("/tasks/{task_id}/resume", summary="恢复已暂停/已完成的演进任务，继续演进")
async def resume_evolution_task(task_id: str, req: EvolutionTaskResumeRequest, background_tasks: BackgroundTasks):
    """
    恢复已暂停、已完成或已失败的演进任务，从上次的 current_loop 继续。
    可选增加额外轮数。
    force_full_train=True 时，忽略各 loop 的 backtest_only 设置，强制执行完整训练+回测。
    """
    try:
        result = await scheduler.resume_task(
            task_id,
            additional_loops=req.additional_loops,
            force_full_train=req.force_full_train,
        )
        resumed_id = result["task_id"]
        task_type = result["task_type"]
        if task_type == "strategy_evo":
            background_tasks.add_task(scheduler.submit_strategy_evo_all_loops, resumed_id)
            return {"status": "success", "task_id": resumed_id, "message": "策略演进任务已恢复（backtest-only 模式）"}
        elif task_type == "custom_evo":
            background_tasks.add_task(
                scheduler.submit_custom_evo_all_loops, resumed_id,
                force_full_train=req.force_full_train,
            )
            msg = "自定义演进任务已恢复（强制完整训练模式）" if req.force_full_train else "自定义演进任务已恢复"
            return {"status": "success", "task_id": resumed_id, "message": msg}
        else:
            background_tasks.add_task(scheduler.submit_next_loop, resumed_id)
            return {"status": "success", "task_id": resumed_id, "message": "演进任务已恢复并在后台继续执行"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to resume task {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/loops/{loop_index}/retry", summary="Retry failed Loop with selectable mode")
async def retry_evolution_loop(
    task_id: str,
    loop_index: int,
    background_tasks: BackgroundTasks,
    req: Optional[EvolutionLoopRetryRequest] = Body(default=None),
):
    """重试失败的 Loop。

    自动检测 workspace 中训练状态：
    - 训练已完成（params.pkl 存在）→ 跳过训练，只重跑回测（backtest-only）
    - 训练未完成 → 全量重跑（训练 + 回测）

    适用场景：回测因策略 bug 失败，修复后重试无需等待 1 小时训练。
    """
    try:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        requested_retry_mode = normalize_qe_loop_retry_mode(req.retry_mode if req else None)

        # 预检查：验证 loop 状态（快速返回错误，不进 background）
        from ..services.quantevolver.qe_evolution_service import get_conn
        from psycopg2.extras import RealDictCursor
        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT status, config_json FROM qe_evolution_loops WHERE loop_id = %s", (evolution_loop_db_id,))
                row = cur.fetchone()
        if not row:
            raise ValueError(f"Loop {evolution_loop_db_id} 不存在")
        if row["status"] not in ("failed", "cancelled"):
            raise ValueError(f"Loop 状态为 '{row['status']}'，只有 failed/cancelled 可以重试")

        if requested_retry_mode in {"auto", "full_train"}:
            config_json = row.get("config_json") or {}
            if isinstance(config_json, str):
                config_json = json.loads(config_json)
            try:
                ensure_loop_fixed_seed(
                    dict(config_json or {}),
                    context=f"retry_loop[{evolution_loop_db_id}]",
                    trainable=True,
                )
            except ValueError as exc:
                raise_http_seed_error(exc)

        background_tasks.add_task(scheduler.retry_loop, task_id, loop_index, requested_retry_mode)
        return {
            "status": "success",
            "loop_id": evolution_loop_db_id,
            "mode": "pending",
            "requested_retry_mode": requested_retry_mode,
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
    filter_suspended_on_signal: bool = Field(False, description="生成日频选股信号时使用 suspend_d 过滤已停牌股票")
    suspend_filter_strict: bool = Field(True, description="启用停牌过滤时要求 suspend_d 每个回测交易日审计成功")
    unfilled_handler: Optional[str] = Field(None, description="尾盘涨停处理: TAIL_BOOST / TAIL_SUBSTITUTE / 空=不使用")
    unfilled_handler_params: Optional[Dict[str, Any]] = Field(None, description="尾盘处理参数，如 {backup_depth: 15}")
    additional_factor_keys: Optional[List[str]] = Field(None, description="从因子库额外添加的因子key列表")
    node_id: Optional[str] = Field(None, description="执行节点 ID, None=默认本地节点")
    label_horizon: Optional[int] = Field(None, description="训练标签期限: 1/3/5/10/20d；全量重训 fork 可覆盖源 Loop")
    random_seed: Optional[int] = Field(None, description="QE fork fixed random seed; required for reproducible full-train loops")


@router.post("/tasks/{task_id}/fork", summary="从指定 Loop 分叉出全新演进任务")
async def fork_evolution_task(task_id: str, req: EvolutionTaskForkRequest, background_tasks: BackgroundTasks):
    """
    从指定 task 的某个已完成 loop 分叉，以该 loop 的因子+模型配置为基础创建新演进任务。
    新 task 的 Loop 1 会用该配置做初始回测建立基线，后续 Loop 由 Agent 正常演进。
    """
    try:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        try:
            req_label_horizon = (
                normalize_label_horizon(req.label_horizon)
                if req.label_horizon not in (None, "")
                else None
            )
            req_random_seed = (
                normalize_qe_random_seed(req.random_seed, field_name="fork.random_seed")
                if req.random_seed not in (None, "")
                else None
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        new_task_id = await scheduler.fork_task(
            source_task_id=task_id,
            from_loop_index=req.from_loop_index,
            task_name=req.task_name,
            max_loops=req.max_loops,
            evolution_guidance=req.evolution_guidance,
            evolution_mode=req.evolution_mode or "auto",
            inherit_history=req.inherit_history,
            strategy_id=req.strategy_id,
            strategy_params=(
                _merge_strategy_runtime_flags(
                    req.strategy_params,
                    req.filter_suspended_on_signal,
                    req.suspend_filter_strict,
                    req_random_seed,
                )
                if (req.strategy_params is not None or req.filter_suspended_on_signal or req_random_seed is not None)
                else None
            ),
            execution_algo=_normalize_qe_execution_algo_for_request(req.execution_algo, "fork.execution_algo"),
            execution_algo_params=req.execution_algo_params,
            unfilled_handler=req.unfilled_handler,
            unfilled_handler_params=req.unfilled_handler_params,
            node_id=req.node_id,
            label_horizon=req_label_horizon,
            random_seed=req_random_seed,
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
    runtime_flags: Optional[Dict[str, Any]] = Field(None, description="Runtime-only archive/seed/provenance flags")
    strategy_id: Optional[str] = Field(None, description="交易策略ID，None=继承源Loop")
    execution_algo: Optional[str] = Field(None, description="日内执行算法code")
    execution_algo_params: Optional[Dict[str, Any]] = Field(None, description="执行算法参数")
    filter_suspended_on_signal: bool = Field(False, description="生成日频选股信号时使用 suspend_d 过滤已停牌股票")
    suspend_filter_strict: bool = Field(True, description="启用停牌过滤时要求 suspend_d 每个回测交易日审计成功")
    enable_sector_hmm: bool = Field(False, description="是否启用行业 HMM")
    hmm_model_version_id: Optional[str] = Field(None, description="HMM 模型快照 ID")
    hmm_signal_preset: Optional[str] = Field(None, description="HMM 信号系数档位: preset_A/preset_B")
    unfilled_handler: Optional[str] = Field(None, description="尾盘涨停处理: TAIL_BOOST / TAIL_SUBSTITUTE / 空=不使用")
    sector_blacklist: Optional[List[str]] = Field(None, description="行业黑名单")
    stock_pool: Optional[str] = Field(None, description="Qlib股票池文件WSL路径")

class StrategyEvolutionForkRequest(BaseModel):
    from_loop_index: int = Field(..., description="从哪个 loop 分叉（必须已完成且有模型文件）")
    task_name: Optional[str] = Field(None, description="新任务名称")
    loops: List[StrategyLoopConfig] = Field(..., description="每个 Loop 的策略参数配置", min_length=1)
    inherit_history: bool = Field(False, description="是否继承截止到该 loop 的演进历史")
    node_id: Optional[str] = Field(None, description="执行节点 ID, None=继承源任务节点")

@router.post("/tasks/{task_id}/strategy-fork", summary="从指定 Loop 分叉出策略演进任务（跳过训练）")
async def strategy_fork_task(task_id: str, req: StrategyEvolutionForkRequest):
    """
    从指定 task 的某个已完成 loop 创建策略演进任务。
    复用源 loop 的已训练模型（mlruns 中的 params.pkl），仅修改策略参数进行批量回测。
    所有 Loop 使用 --backtest-only 模式，跳过模型训练。
    """
    try:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        # 为 loops 配置分配 loop_index
        loops_config = []
        for i, loop_cfg in enumerate(req.loops, start=1):
            cfg_dict = loop_cfg.dict()
            _reject_nested_runtime_flags(
                cfg_dict.get("strategy_params"),
                f"strategy_loop[{i}].strategy_params",
            )
            _hoist_runtime_metadata_from_strategy_params(cfg_dict)
            cfg_dict["loop_index"] = i
            cfg_dict["execution_algo"] = _normalize_qe_execution_algo_for_request(
                cfg_dict.get("execution_algo"),
                f"strategy_loop[{i}].execution_algo",
            )
            loops_config.append(cfg_dict)

        new_task_id = await scheduler.strategy_fork_task(
            source_task_id=task_id,
            from_loop_index=req.from_loop_index,
            task_name=req.task_name,
            loops_config=loops_config,
            inherit_history=req.inherit_history,
            node_id=req.node_id,
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


def _get_source_loop_factors(task_id: str, loop_index: int) -> list[str] | None:
    """从源 Loop 的 config_json 中读取因子列表，用于 backtest-only 因子一致性校验。"""
    from ..services.quantevolver.qe_evolution_service import get_conn
    from psycopg2.extras import RealDictCursor
    import json as _json
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT config_json FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s",
                (task_id, loop_index),
            )
            row = cur.fetchone()
    if not row:
        # 源 Loop 可能来自 QE 单次实验（qe_experiments 表）
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT factor_names FROM qe_experiments WHERE qe_task_id = %s AND loop_index = %s",
                    (task_id, loop_index),
                )
                exp_row = cur.fetchone()
        if not exp_row:
            return None
        factor_names = exp_row.get("factor_names") or []
        if isinstance(factor_names, str):
            factor_names = _json.loads(factor_names)
        return sorted(factor_names)
    config = row.get("config_json") or {}
    if isinstance(config, str):
        config = _json.loads(config)
    return sorted(config.get("factor_list") or [])


def _get_source_loop_disable_alpha158(task_id: str, loop_index: int) -> bool | None:
    """Read the source model's Alpha158 baseline toggle for backtest-only validation."""
    from ..services.quantevolver.qe_evolution_service import get_conn
    from psycopg2.extras import RealDictCursor
    import json as _json

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT config_json FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s",
                (task_id, loop_index),
            )
            row = cur.fetchone()
    if row:
        config = row.get("config_json") or {}
        if isinstance(config, str):
            config = _json.loads(config)
        model_params = config.get("model_params") or {}
        return bool(config.get("disable_alpha158", model_params.get("disable_alpha158", False)))

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT custom_params FROM qe_experiments WHERE qe_task_id = %s AND loop_index = %s",
                (task_id, loop_index),
            )
            exp_row = cur.fetchone()
    if not exp_row:
        return None
    custom_params = exp_row.get("custom_params") or {}
    if isinstance(custom_params, str):
        custom_params = _json.loads(custom_params)
    return bool(custom_params.get("disable_alpha158", False))


class CustomEvoLoopConfig(BaseModel):
    """单个自定义演进 Loop 的完整配置"""
    label: Optional[str] = Field(None, description="Loop 标签/描述")
    loop_index: Optional[int] = Field(None, description="Loop 索引（自动填充）")
    factor_keys: List[str] = Field(..., description="因子 key 列表 ['name||source', ...]")
    disable_alpha158: bool = Field(False, description="Disable QE bundled Alpha158 20-factor baseline for this loop")
    model_id: str = Field(..., description="模型 ID")
    model_params: Optional[Dict[str, Any]] = Field(None, description="模型训练参数，如 ltr_loss_mode/topk_train_k 等")
    strategy_id: Optional[str] = Field(None, description="交易策略ID，None=使用默认 TopkDropoutStrategy")
    strategy_params: Optional[Dict[str, Any]] = Field(None, description="策略参数: topk, n_drop, hold_thresh, risk_degree 等")
    runtime_flags: Optional[Dict[str, Any]] = Field(None, description="Runtime-only archive/seed/provenance flags")
    ensemble: Optional[Dict[str, Any]] = Field(None, description="Optional deterministic seed ensemble: enabled, seeds, level, agg")
    execution_algo: Optional[str] = Field(None, description="日内执行算法code")
    execution_algo_params: Optional[Dict[str, Any]] = Field(None, description="执行算法参数")
    filter_suspended_on_signal: bool = Field(False, description="生成日频选股信号时使用 suspend_d 过滤已停牌股票")
    suspend_filter_strict: bool = Field(True, description="启用停牌过滤时要求 suspend_d 每个回测交易日审计成功")
    enable_sector_hmm: bool = Field(False, description="是否启用行业 HMM")
    hmm_model_version_id: Optional[str] = Field(None, description="HMM 模型快照 ID")
    hmm_signal_preset: Optional[str] = Field(None, description="HMM 信号系数档位: preset_A/preset_B")
    unfilled_handler: Optional[str] = Field(None, description="尾盘涨停处理: TAIL_BOOST / TAIL_SUBSTITUTE / 空=不使用")
    unfilled_handler_params: Optional[Dict[str, Any]] = Field(None, description="尾盘处理参数")
    sector_blacklist: Optional[List[str]] = Field(None, description="行业黑名单")
    stock_pool: Optional[str] = Field(None, description="Qlib股票池文件WSL路径")
    label_type: Optional[str] = Field(None, description="训练标签类型: close/open/vwap")
    label_horizon: Optional[int] = Field(None, description="训练标签期限: 1/3/5/10/20d")
    data_split: Optional[Dict[str, str]] = Field(None, description="数据划分覆盖，None=使用系统默认")
    # backtest-only 模式（复用已训练模型，仅回测）
    backtest_only: bool = Field(False, description="是否跳过训练仅回测（需提供 model_source，且因子不可变更）")
    model_source_task_id: Optional[str] = Field(None, description="模型来源任务 ID（backtest_only=True 时必填）")
    model_source_loop_index: Optional[int] = Field(None, description="模型来源 Loop 索引（backtest_only=True 时必填）")
    node_id: Optional[str] = Field(None, description="Loop execution node; blank inherits Loop1")

class CustomEvolutionCreateRequest(BaseModel):
    task_name: str = Field(..., description="任务名称")
    target_desc: str = Field("", description="任务描述")
    loops: List[CustomEvoLoopConfig] = Field(..., description="Loop 配置列表，至少1个", min_length=1)
    node_id: Optional[str] = Field(None, description="执行节点 ID, None=默认本地节点")
    node_parallelism: Optional[Dict[str, int]] = Field(None, description="Per-node parallelism, default 1, max 4")
    engine_mode: str = Field("unified", description="引擎模式: only unified is supported")

    auto_start: bool = Field(True, description="Create and immediately submit loops; template materialization sets false")
    clone_from_task_id: Optional[str] = Field(None, description="Optional source custom_evo task id for clone provenance")


class CustomEvoConfigUpdateRequest(BaseModel):
    task_name: str = Field(..., description="任务名称")
    target_desc: str = Field("", description="任务描述")
    loops: List[CustomEvoLoopConfig] = Field(..., description="Loop 配置列表，至少1个", min_length=1)
    node_id: Optional[str] = Field(None, description="执行节点 ID, None=默认本地节点")
    node_parallelism: Optional[Dict[str, int]] = Field(None, description="Per-node parallelism")
    engine_mode: str = Field("unified", description="Only unified is supported")


class CustomEvoLoopRerunRequest(BaseModel):
    loop: CustomEvoLoopConfig = Field(..., description="Replacement config for the target Loop")
    node_id: Optional[str] = Field(None, description="Default execution node for this mutation")
    node_parallelism: Optional[Dict[str, int]] = Field(None, description="Per-node parallelism")
    engine_mode: str = Field("unified", description="Only unified is supported")
    confirm_delete_old_result: bool = Field(False, description="Must be true because rerun deletes old results")


class CustomEvoAppendRequest(BaseModel):
    loops: List[CustomEvoLoopConfig] = Field(..., description="New Loop configs to append", min_length=1)
    node_id: Optional[str] = Field(None, description="Default execution node for appended loops")
    node_parallelism: Optional[Dict[str, int]] = Field(None, description="Per-node parallelism")
    engine_mode: str = Field("unified", description="Only unified is supported")
    ack_failed_loop_warning: bool = Field(False, description="Caller acknowledged existing failed/cancelled loops")


class CustomEvoRunRequest(BaseModel):
    confirm_custom_evo: str = Field("", description="Must equal QE_CUSTOM_EVO_RUN")
    force_full_train: bool = Field(False, description="Force full training when submitting a pending custom_evo task")


async def _prepare_custom_evo_loop_configs(
    loops: List[CustomEvoLoopConfig],
    *,
    request_node_id: Optional[str],
    node_parallelism_payload: Optional[Dict[str, int]],
    assigned_loop_indexes: Optional[List[int]] = None,
    node_parallelism_scope_node_ids: Optional[set[str]] = None,
) -> tuple[List[Dict[str, Any]], str, Dict[str, int]]:
    """Validate custom_evo loop payloads and resolve execution nodes fail-fast."""
    if assigned_loop_indexes and len(assigned_loop_indexes) != len(loops):
        raise HTTPException(status_code=400, detail="assigned_loop_indexes length must match loops length")

    loop_source_horizons: Dict[int, int] = {}
    for pos, loop_cfg in enumerate(loops, start=1):
        try:
            loop_label_horizon = normalize_label_horizon(loop_cfg.label_horizon)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Loop {pos}: {e}") from e
        if not loop_cfg.factor_keys:
            raise HTTPException(status_code=400, detail=f"Loop {pos}: factor_keys is required")
        if not loop_cfg.model_id:
            raise HTTPException(status_code=400, detail=f"Loop {pos}: model_id is required")
        if loop_cfg.enable_sector_hmm and not loop_cfg.hmm_model_version_id:
            raise HTTPException(status_code=400, detail=f"Loop {pos}: hmm_model_version_id is required when HMM is enabled")
        if loop_cfg.backtest_only:
            if not loop_cfg.model_source_task_id or loop_cfg.model_source_loop_index is None:
                raise HTTPException(status_code=400, detail=f"Loop {pos}: backtest-only requires model_source")
            try:
                source_label_horizon = scheduler._get_source_loop_label_horizon(
                    loop_cfg.model_source_task_id,
                    loop_cfg.model_source_loop_index,
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Loop {pos}: {e}") from e
            loop_source_horizons[pos] = source_label_horizon
            if loop_label_horizon != source_label_horizon:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Loop {pos}: backtest-only label_horizon={loop_label_horizon} does not match "
                        f"source model label_horizon={source_label_horizon}"
                    ),
                )
            source_factors = _get_source_loop_factors(
                loop_cfg.model_source_task_id,
                loop_cfg.model_source_loop_index,
            )
            if source_factors is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Loop {pos}: source loop factors cannot be read; backtest-only is not allowed",
                )
            current_factors = sorted(k.split("||")[0] for k in loop_cfg.factor_keys)
            if current_factors != sorted(source_factors):
                raise HTTPException(
                    status_code=400,
                    detail=f"Loop {pos}: backtest-only requires the same factor list as the source model",
                )
            source_disable_alpha158 = _get_source_loop_disable_alpha158(
                loop_cfg.model_source_task_id,
                loop_cfg.model_source_loop_index,
            )
            if source_disable_alpha158 is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Loop {pos}: source Alpha158 baseline setting cannot be read; backtest-only is not allowed",
                )
            if bool(loop_cfg.disable_alpha158) != bool(source_disable_alpha158):
                raise HTTPException(
                    status_code=400,
                    detail=f"Loop {pos}: backtest-only requires the same Alpha158 baseline setting as the source model",
                )

    loops_config: List[Dict[str, Any]] = []
    for pos, loop_cfg in enumerate(loops, start=1):
        cfg_dict = _model_to_dict(loop_cfg)
        _reject_nested_runtime_flags(
            cfg_dict.get("strategy_params"),
            f"custom_loop[{pos}].strategy_params",
        )
        _hoist_runtime_metadata_from_strategy_params(cfg_dict)
        stock_pool_errors = validate_qe_historical_stock_pool_window(
            cfg_dict,
            context=f"custom_evo.loops[{pos}]",
        )
        if stock_pool_errors:
            raise HTTPException(status_code=400, detail=stock_pool_errors[0])
        try:
            ensure_loop_fixed_seed(cfg_dict, context=f"custom_loop[{pos}]")
        except ValueError as exc:
            raise_http_seed_error(exc)
        cfg_dict["strategy_params"] = ensure_qe_risk_policy(
            cfg_dict.get("strategy_params") or {},
            source=f"custom_loop[{pos}].strategy_params",
        )
        cfg_dict["loop_index"] = assigned_loop_indexes[pos - 1] if assigned_loop_indexes else pos
        cfg_dict["execution_algo"] = _normalize_qe_execution_algo_for_request(
            cfg_dict.get("execution_algo"),
            f"custom_loop[{pos}].execution_algo",
        )
        cfg_dict["label_horizon"] = normalize_label_horizon(loop_cfg.label_horizon)
        if loop_cfg.backtest_only and pos in loop_source_horizons:
            cfg_dict["source_label_horizon"] = loop_source_horizons[pos]
        loops_config.append(cfg_dict)

    try:
        loops_config, loop1_node_id, selected_node_ids = resolve_custom_loop_nodes(
            loops_config,
            request_node_id,
        )
        parallelism_scope = node_parallelism_scope_node_ids or selected_node_ids
        node_parallelism = normalize_node_parallelism(
            parallelism_scope,
            node_parallelism_payload,
        )
        node_rows = await preflight_qe_nodes(selected_node_ids)
    except QENodePreflightError as e:
        raise HTTPException(status_code=400, detail=e.to_detail()) from e

    synced_stock_pool_keys: set[tuple[str, str]] = set()
    for cfg_dict in loops_config:
        stock_pool = cfg_dict.get("stock_pool")
        if stock_pool and "filtered_pool" in stock_pool:
            sync_key = (str(cfg_dict["node_id"]), str(stock_pool))
            if sync_key in synced_stock_pool_keys:
                continue
            node = node_rows.get(cfg_dict["node_id"]) or get_compute_node(cfg_dict["node_id"])
            if not node:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error_code": "QE_NODE_NOT_FOUND",
                        "message": f"Node {cfg_dict['node_id']} does not exist.",
                        "context": {"node_id": cfg_dict["node_id"]},
                    },
                )
            _sync_stock_pool_to_remote(stock_pool, node)
            synced_stock_pool_keys.add(sync_key)

    return loops_config, loop1_node_id, node_parallelism


def _resolve_custom_evo_node_scope(
    loops_config: List[Dict[str, Any]],
    request_node_id: Optional[str],
) -> set[str]:
    """Resolve the full post-mutation node set without touching remote nodes."""
    if not loops_config:
        return set()
    try:
        _resolved, _loop1_node_id, selected_node_ids = resolve_custom_loop_nodes(
            [dict(loop_cfg) for loop_cfg in loops_config],
            request_node_id,
        )
        return selected_node_ids
    except QENodePreflightError as e:
        raise HTTPException(status_code=400, detail=e.to_detail()) from e


def _filter_node_parallelism_for_scope(
    node_parallelism: Optional[Dict[str, int]],
    node_scope: set[str],
) -> Optional[Dict[str, int]]:
    """Drop stale UI parallelism entries for nodes no longer used by any Loop."""
    if not node_parallelism:
        return node_parallelism
    normalized_scope = {str(node_id).strip() for node_id in node_scope if str(node_id or "").strip()}
    filtered = {
        str(node_id).strip(): value
        for node_id, value in node_parallelism.items()
        if str(node_id).strip() in normalized_scope
    }
    ignored = sorted(set(str(node_id).strip() for node_id in node_parallelism) - set(filtered))
    if ignored:
        logger.warning(
            "Ignoring node_parallelism for nodes not used by custom_evo loop configs: %s",
            ignored,
        )
    return filtered

@router.post("/custom-tasks", summary="Create custom evolution task")
async def create_custom_evolution_task(req: CustomEvolutionCreateRequest, background_tasks: BackgroundTasks):
    """Create a custom_evo task with explicit per-loop execution nodes."""
    try:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        if (req.engine_mode or "unified") != "unified":
            raise HTTPException(
                status_code=400,
                detail="QE legacy execution engine has been removed; only engine_mode='unified' is supported.",
            )

        loops_config, loop1_node_id, node_parallelism = await _prepare_custom_evo_loop_configs(
            req.loops,
            request_node_id=req.node_id,
            node_parallelism_payload=req.node_parallelism,
        )

        new_task_id = await scheduler.create_custom_evo_task(
            task_name=req.task_name,
            target_desc=req.target_desc,
            loops_config=loops_config,
            node_id=loop1_node_id,
            node_parallelism=node_parallelism,
            engine_mode="unified",
            clone_from_task_id=req.clone_from_task_id,
            auto_start=req.auto_start,
        )

        response = {
            "status": "success",
            "task_id": new_task_id,
            "total_loops": len(loops_config),
            "node_assignments": [
                {"loop_index": cfg.get("loop_index"), "node_id": cfg.get("node_id")}
                for cfg in loops_config
            ],
            "node_parallelism": node_parallelism,
            "message": f"Custom evolution task created with {len(loops_config)} loops.",
        }
        if not req.auto_start:
            response.update({
                "operation": "create_pending",
                "editable": True,
                "startable": True,
                "resume_allowed": False,
                "start_reason": "custom_evo task has not been submitted",
            })
        return response
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create custom evolution task: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/custom-evo-config", summary="Get editable custom evolution config")
async def get_custom_evo_config(task_id: str):
    try:
        data = await scheduler.get_custom_evo_editable_config(task_id)
        return {"status": "success", "data": data}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to read custom_evo config for {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_id}/custom-evo-config", summary="Update a never-started custom_evo task config")
async def update_custom_evo_config(task_id: str, req: CustomEvoConfigUpdateRequest):
    try:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
        if (req.engine_mode or "unified") != "unified":
            raise HTTPException(
                status_code=400,
                detail="QE legacy execution engine has been removed; only engine_mode='unified' is supported.",
            )
        loops_config, loop1_node_id, node_parallelism = await _prepare_custom_evo_loop_configs(
            req.loops,
            request_node_id=req.node_id,
            node_parallelism_payload=req.node_parallelism,
        )
        result = await scheduler.update_custom_evo_editable_config(
            task_id=task_id,
            task_name=req.task_name,
            target_desc=req.target_desc,
            loops_config=loops_config,
            node_id=loop1_node_id,
            node_parallelism=node_parallelism,
            engine_mode="unified",
        )
        return {"status": "success", **result}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=409 if "already started" in str(e) or "not editable" in str(e) else 400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update custom_evo config for {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/custom-evo/run", summary="Submit a materialized custom_evo task")
async def run_custom_evo_task(task_id: str, req: CustomEvoRunRequest, background_tasks: BackgroundTasks):
    if req.confirm_custom_evo != "QE_CUSTOM_EVO_RUN":
        raise HTTPException(status_code=400, detail="confirm_custom_evo must equal QE_CUSTOM_EVO_RUN")
    try:
        config = await scheduler.get_custom_evo_editable_config(task_id)
        if config.get("task_type") != "custom_evo":
            raise HTTPException(status_code=400, detail=f"task {task_id} is not a custom_evo task")
        if config.get("status") == "running":
            raise HTTPException(status_code=409, detail=f"custom_evo task {task_id} is already running")
        if not config.get("startable"):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"custom_evo task {task_id} is not a never-started pending task; "
                    f"use resume/append/rerun as appropriate. reason={config.get('start_reason')}"
                ),
            )
        for idx, loop in enumerate(config.get("loops") or [], start=1):
            try:
                ensure_loop_fixed_seed(dict(loop), context=f"custom_evo.task[{task_id}].loops[{idx}]")
            except ValueError as exc:
                raise_http_seed_error(exc)
        background_tasks.add_task(
            scheduler.submit_custom_evo_all_loops,
            task_id,
            force_full_train=req.force_full_train,
        )
        return {
            "status": "success",
            "operation": "start",
            "task_id": task_id,
            "submitted": True,
            "force_full_train": req.force_full_train,
            "message": "custom_evo task started through canonical backend executor",
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to run custom_evo task {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/loops/{loop_index}/rerun", summary="Rerun a custom_evo Loop with full editable config")
async def rerun_custom_evo_loop(
    task_id: str,
    loop_index: int,
    req: CustomEvoLoopRerunRequest,
    background_tasks: BackgroundTasks,
):
    try:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
        if (req.engine_mode or "unified") != "unified":
            raise HTTPException(
                status_code=400,
                detail="QE legacy execution engine has been removed; only engine_mode='unified' is supported.",
            )
        if not req.confirm_delete_old_result:
            raise HTTPException(
                status_code=400,
                detail="confirm_delete_old_result must be true because rerun permanently deletes old Loop results.",
            )
        existing_config = await scheduler.get_custom_evo_editable_config(task_id)
        request_node_id = (req.node_id or "").strip() or existing_config.get("node_id")
        replacement_scope_cfg = _model_to_dict(req.loop)
        replacement_scope_cfg["loop_index"] = loop_index
        existing_loops = [dict(cfg) for cfg in (existing_config.get("loops") or [])]
        full_scope_loops: List[Dict[str, Any]] = []
        replaced = False
        for cfg in existing_loops:
            if int(cfg.get("loop_index") or 0) == loop_index:
                full_scope_loops.append(dict(replacement_scope_cfg))
                replaced = True
            else:
                full_scope_loops.append(dict(cfg))
        if not full_scope_loops:
            full_scope_loops = [dict(replacement_scope_cfg)]
        elif not replaced:
            full_scope_loops.append(dict(replacement_scope_cfg))
        full_node_scope = _resolve_custom_evo_node_scope(full_scope_loops, request_node_id)
        scoped_node_parallelism = _filter_node_parallelism_for_scope(req.node_parallelism, full_node_scope)
        loops_config, _loop1_node_id, node_parallelism = await _prepare_custom_evo_loop_configs(
            [req.loop],
            request_node_id=request_node_id,
            node_parallelism_payload=scoped_node_parallelism,
            assigned_loop_indexes=[loop_index],
            node_parallelism_scope_node_ids=full_node_scope,
        )
        result = await scheduler.rerun_custom_evo_loop(
            task_id=task_id,
            loop_index=loop_index,
            loop_config=loops_config[0],
            node_id=request_node_id,
            node_parallelism=node_parallelism,
        )
        background_tasks.add_task(scheduler.submit_custom_evo_selected_loops, task_id, [loop_index])
        return {"status": "success", **result}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to rerun custom_evo loop {task_id}/Loop{loop_index}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/custom-loops/append", summary="Append custom_evo Loops to an existing task")
async def append_custom_evo_loops(
    task_id: str,
    req: CustomEvoAppendRequest,
    background_tasks: BackgroundTasks,
):
    try:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
        if (req.engine_mode or "unified") != "unified":
            raise HTTPException(
                status_code=400,
                detail="QE legacy execution engine has been removed; only engine_mode='unified' is supported.",
            )
        existing_config = await scheduler.get_custom_evo_editable_config(task_id)
        request_node_id = (req.node_id or "").strip() or existing_config.get("node_id")
        full_scope_loops = [
            dict(cfg) for cfg in (existing_config.get("loops") or [])
        ] + [
            _model_to_dict(loop) for loop in req.loops
        ]
        full_node_scope = _resolve_custom_evo_node_scope(full_scope_loops, request_node_id)
        scoped_node_parallelism = _filter_node_parallelism_for_scope(req.node_parallelism, full_node_scope)
        loops_config, _loop1_node_id, node_parallelism = await _prepare_custom_evo_loop_configs(
            req.loops,
            request_node_id=request_node_id,
            node_parallelism_payload=scoped_node_parallelism,
            node_parallelism_scope_node_ids=full_node_scope,
        )
        result = await scheduler.append_custom_evo_loops(
            task_id=task_id,
            loops_config=loops_config,
            node_id=request_node_id,
            node_parallelism=node_parallelism,
            ack_failed_loop_warning=req.ack_failed_loop_warning,
        )
        background_tasks.add_task(scheduler.submit_custom_evo_selected_loops, task_id, result["new_loop_indexes"])
        return {"status": "success", **result}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to append custom_evo loops for {task_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/{task_id}/logs", summary="获取实时的任务运行及 Agent 思考日志流 (SSE)")
def stream_task_logs(task_id: str):
    """
    通过 SSE (Server-Sent Events) 返回该任务当前 LOOP 的实时日志
    底层会调用 RDAgent 的日志 API 进行转发
    """
    try:
        if not scheduler.task_exists(task_id):
            logger.info("Log stream requested for deleted/nonexistent task %s; returning 204", task_id)
            return Response(status_code=204)
        return StreamingResponse(
            scheduler.stream_task_logs(task_id),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )
    except Exception as e:
        logger.error(f"Failed to establish log stream for task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/{task_id}/logs/tail", summary="Read local evolution log tail without opening a live stream")
def get_task_log_tail(task_id: str, tail: int = Query(500, ge=1, le=5000)):
    try:
        if not scheduler.task_exists(task_id):
            raise HTTPException(status_code=404, detail="Task not found")
        return {"status": "success", "data": scheduler.get_task_log_tail(task_id, tail)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to read log tail for task {task_id}: {str(e)}", exc_info=True)
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


class PromotionReviewCreateRequest(BaseModel):
    requested_by: str = Field("manual_user", description="Operator or UI identity requesting manual SOTA review")
    review_reason: Optional[str] = Field(None, description="Manual review note; does not approve SOTA")

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


@router.post("/tasks/{task_id}/loops/{loop_id}/promotion-review", summary="Create a manual SOTA promotion review")
def create_loop_promotion_review(task_id: str, loop_id: str, req: PromotionReviewCreateRequest):
    """
    Create a REVIEW_PENDING audit record for a completed QE loop.

    This is the Phase 1 manual gate: it never marks the loop as approved SOTA,
    never enables Paper v2, and is idempotent while the source remains pending.
    """
    try:
        evolution_loop_db_id = loop_id if loop_id.startswith(f"{task_id}_") else f"{task_id}_{loop_id}"
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT loop_id, task_id, experiment_id, metrics_json, status
                    FROM qe_evolution_loops
                    WHERE loop_id = %s AND task_id = %s
                    """,
                    (evolution_loop_db_id, task_id),
                )
                row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="QE evolution loop not found")
        if row.get("status") != "completed":
            raise HTTPException(status_code=400, detail="Only completed QE loops can enter manual SOTA review")

        review = PromotionReviewService().request_loop_review(
            task_id=task_id,
            loop_id=evolution_loop_db_id,
            requested_by=req.requested_by,
            review_reason=req.review_reason,
            source_metrics=row.get("metrics_json") or {},
            experiment_id=row.get("experiment_id"),
        )
        return {
            "status": "success",
            "data": review.model_dump(mode="json"),
            "message": "Created REVIEW_PENDING record; no approved SOTA or Paper v2 state was changed.",
        }
    except HTTPException:
        raise
    except TradingCoreError as e:
        raise HTTPException(status_code=400, detail=e.to_dict()) from e
    except Exception as e:
        logger.error(f"Failed to create promotion review for {task_id}/{loop_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


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
        rdagent_loop_id = _normalize_rdagent_loop_id(task_id, loop_id)
        evolution_loop_db_id = f"{task_id}_{rdagent_loop_id}"
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
                loop_index = int(rdagent_loop_id.replace("Loop", "")) if rdagent_loop_id.startswith("Loop") and rdagent_loop_id[4:].isdigit() else None
                cached_em, changed = _augment_enhanced_metrics_with_positions(task_id, rdagent_loop_id, loop_index, cached_em)
                if changed:
                    cached["enhanced_metrics"] = cached_em
                    _cache_loop_enhanced_metrics(task_id, rdagent_loop_id, cached)
                return {"status": "success", "data": cached_em}

        # DB stores loop_id as "{task_id}_LoopN" while RD-Agent expects "LoopN".
        client = scheduler._get_workspace_client_for_loop(task_id, evolution_loop_db_id)
        data = await client.get_enhanced_metrics(task_id, rdagent_loop_id)
        loop_index = int(rdagent_loop_id.replace("Loop", "")) if rdagent_loop_id.startswith("Loop") and rdagent_loop_id[4:].isdigit() else None
        data, _ = _augment_enhanced_metrics_with_positions(task_id, rdagent_loop_id, loop_index, data)

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
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


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


@router.get("/leaderboard", summary="Get cross-task SOTA leaderboard")
def get_sota_leaderboard():
    """Return approved legacy registry rows only.

    Candidate StrategyPackages are now created by explicit user action through
    the StrategyPackage candidate APIs; ``is_sota`` is historical evidence, not
    an automatic candidate source.
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
                           r.evaluation_reason, r.created_at,
                           r.model_assets_synced, r.local_asset_path,
                           'LEGACY_REGISTRY'::text AS promotion_state,
                           TRUE AS approved_sota,
                           NULL::text AS review_id,
                           NULL::text AS review_requested_by,
                           NULL::timestamptz AS review_created_at
                    FROM qe_sota_registry r
                    JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                    JOIN qe_evolution_tasks t ON l.task_id = t.task_id
                    ORDER BY created_at DESC
                    LIMIT 100
                """)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        # Extract common metrics for sorting and frontend summaries.
        for row in rows:
            m = row.get("metrics_json") or {}
            if isinstance(m, str):
                try:
                    m = _json.loads(m)
                except Exception:
                    m = {}
            if not isinstance(m, dict):
                m = {}
            row["ic"] = m.get("IC")
            row["rank_ic"] = m.get("Rank_IC")
            row["icir"] = m.get("ICIR")
            row["sharpe"] = m.get("Sharpe", m.get("sharpe"))

        rows.sort(key=lambda r: r.get("ic") or 0, reverse=True)

        total_tasks = len(set(r["task_id"] for r in rows))
        total_loops = len(rows)
        best_ic = max((r["ic"] for r in rows if r.get("ic") is not None), default=None)
        best_sharpe = max((r["sharpe"] for r in rows if r.get("sharpe") is not None), default=None)

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
_active_dispatch_task_id: Optional[str] = None
_dispatch_service = None  # lazy init — 避免 WSL 脚本 import 时触发 aiofiles 依赖


def _get_dispatch_service():
    global _dispatch_service
    if _dispatch_service is None:
        from ..services.dispatch_service import DispatchService
        _dispatch_service = DispatchService()
    return _dispatch_service
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
        _emit_correlation_event({
            "type": "log",
            "level": level,
            "msg": msg,
            "entry": entry,
        })

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
_correlation_event_emitter: Optional[Callable[[Dict[str, Any]], None]] = None


def _set_local_correlation_event_emitter(emitter: Optional[Callable[[Dict[str, Any]], None]]) -> None:
    global _correlation_event_emitter
    _correlation_event_emitter = emitter


def _emit_correlation_event(event: Dict[str, Any]) -> None:
    if _correlation_event_emitter is None:
        return
    try:
        _correlation_event_emitter(event)
    except Exception as exc:
        logger.warning(f"correlation event emit failed: {exc}")


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

    def _emit_snapshot(self) -> None:
        _emit_correlation_event({
            "type": "progress",
            "data": dict(self._data),
        })

    def snapshot(self) -> dict:
        with self._lock:
            if self._start_time and self._data["status"] == "computing":
                self._data["elapsed_sec"] = round(time.time() - self._start_time, 1)
            return dict(self._data)

    def sync_from_event(self, data: dict) -> None:
        with self._lock:
            self._data.update(data or {})
            status = self._data.get("status")
            elapsed = float(self._data.get("elapsed_sec") or 0)
            if status == "computing":
                self._start_time = time.time() - elapsed
            else:
                self._start_time = None

    def start(self, mode: str, total: int, job_id: str = None):
        with self._lock:
            self._start_time = time.time()
            self._data.update(
                status="computing", phase="cache_gen",
                phase_label="生成单因子缓存", total=total, done=0,
                percent=0, started_at=datetime.now().isoformat(),
                mode=mode, error=None, job_id=job_id,
            )
        self._emit_snapshot()

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
        self._emit_snapshot()

    def finish(self, status="success", error=None):
        with self._lock:
            self._data["status"] = status
            self._data["percent"] = 100 if status == "success" else self._data["percent"]
            self._data["error"] = error
            if self._start_time:
                self._data["elapsed_sec"] = round(time.time() - self._start_time, 1)
            self._start_time = None
        self._emit_snapshot()


_correlation_progress = _CorrelationProgress()

# 缓存 status 端点的 DB 查询结果，计算中跳过 DB 直接返回缓存
# counts_by_include_disabled: { False: {...}, True: {...} } — 按 include_disabled 查询口径分桶
_status_db_cache: Dict[str, Any] = {
    "meta": None,
    "counts_by_include_disabled": {
        False: {"db_count": 0, "uncorrelated_count": 0, "high_corr_count_07": 0, "high_corr_count_05": 0},
        True: {"db_count": 0, "uncorrelated_count": 0, "high_corr_count_07": 0, "high_corr_count_05": 0},
    },
}


def _update_job_status(job_id, status, error=None):
    """更新 ingestion_jobs 表状态。"""
    if not job_id:
        return
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


def _current_correlation_eligible_factor_ids(include_disabled: bool = False) -> List[int]:
    rows = FactorEligibilityService().list_eligible_factors(include_disabled=include_disabled)
    factor_ids = [int(row["id"]) for row in rows if row.get("id") is not None]
    if not factor_ids:
        raise RuntimeError("当前无符合相关性 official 准入规则的因子")
    return factor_ids


def _reconcile_correlation_state(reset_all: bool = False) -> Dict[str, int]:
    """清理相关性历史脏状态，确保 DB 与当前 official 准入规则一致。"""
    stats = {
        "eligible_factors": 0,
        "deleted_pairs": 0,
        "reset_ineligible_catalog": 0,
        "reset_orphan_catalog": 0,
        "reset_all_catalog": 0,
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            if reset_all:
                # reset_all 不依赖准入规则, 清空全表状态
                cur.execute(
                    """
                    UPDATE aistock_factor_catalog
                    SET correlation_computed_at = NULL,
                        correlation_pair_count = 0
                    WHERE correlation_computed_at IS NOT NULL
                       OR COALESCE(correlation_pair_count, 0) <> 0
                    """
                )
                stats["reset_all_catalog"] = cur.rowcount
            else:
                # 增量收敛必须基于当前 eligible 集合
                eligible_ids = _current_correlation_eligible_factor_ids()
                stats["eligible_factors"] = len(eligible_ids)
                cur.execute(
                    """
                    DELETE FROM qe_factor_correlations
                    WHERE NOT (factor_a_id = ANY(%s) AND factor_b_id = ANY(%s))
                    """,
                    (eligible_ids, eligible_ids),
                )
                stats["deleted_pairs"] = cur.rowcount

                cur.execute(
                    """
                    UPDATE aistock_factor_catalog
                    SET correlation_computed_at = NULL,
                        correlation_pair_count = 0
                    WHERE (correlation_computed_at IS NOT NULL
                           OR COALESCE(correlation_pair_count, 0) <> 0)
                      AND NOT (id = ANY(%s))
                    """,
                    (eligible_ids,),
                )
                stats["reset_ineligible_catalog"] = cur.rowcount

                cur.execute(
                    """
                    UPDATE aistock_factor_catalog c
                    SET correlation_computed_at = NULL,
                        correlation_pair_count = 0
                    WHERE c.id = ANY(%s)
                      AND c.correlation_computed_at IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM qe_factor_correlations q
                          WHERE q.factor_a_id = c.id OR q.factor_b_id = c.id
                      )
                    """,
                    (eligible_ids,),
                )
                stats["reset_orphan_catalog"] = cur.rowcount
        conn.commit()

    return stats


def _run_correlation_compute_local(
    factor_names: list,
    as_of_date: str = None,
    job_id: str = None,
    data_date: str = None,
    **_kwargs,
):
    """Compatibility wrapper; authoritative implementation lives in correlation_compute_service."""
    return _correlation_compute_service.run_correlation_compute_local(
        factor_names=factor_names,
        as_of_date=as_of_date,
        job_id=job_id,
        data_date=data_date,
        **_kwargs,
    )


# 这样 WSL runner 不再需要导入本 router，也不会被 QE evolution 顶层 import 变化影响。
_run_correlation_compute_local = _correlation_compute_service.run_correlation_compute_local
set_correlation_event_emitter = _correlation_compute_service.set_correlation_event_emitter


def _env_truthy(key: str) -> bool:
    value = (os.getenv(key) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _run_correlation_compute_via_dispatch(
    factor_names: list,
    as_of_date: str = None,
    job_id: str = None,
    data_date: str = None,
):
    import asyncio
    import time as _time

    global _active_dispatch_task_id

    payload = {
        "factor_names": list(factor_names or []),
        "as_of_date": as_of_date,
        "job_id": str(job_id) if job_id else None,
        "data_date": data_date,
    }
    with _computing_lock:
        _correlation_logs.clear()
        _correlation_progress.start("full", len(factor_names), str(job_id) if job_id else None)
        _correlation_progress.advance(phase="cache_gen", phase_label="提交WSL节点任务", done=0, total=max(len(factor_names), 1))
        _correlation_logs.append(
            f"启动相关性计算(dispatch): 因子数={len(factor_names)}, as_of_date={as_of_date or 'latest'}"
        )
        _update_job_status(job_id, "running")
        try:
            created = asyncio.run(_get_dispatch_service().create_and_submit_task({
                "task_name": f"correlation_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "task_type": "correlation_compute",
                "node_id": os.getenv("AISTOCK_DEFAULT_GPU_NODE_ID", "wsl2-5080"),
                "payload": payload,
            }))
            task_id = created["task_id"]
            _active_dispatch_task_id = task_id
            deadline = _time.time() + _MATRIX_TIMEOUT_SEC
            last_task = created
            while _time.time() < deadline:
                asyncio.run(_get_dispatch_service().sync_running_tasks())
                last_task = _get_dispatch_service().get_task(task_id) or last_task
                status = last_task.get("status")
                if status in {"success", "failed", "canceled"}:
                    break
                _time.sleep(2)
            else:
                raise TimeoutError(f"correlation dispatch task timeout: {task_id}")

            result_bundle = asyncio.run(_get_dispatch_service().get_task_results(task_id))
            latest_result = result_bundle.get("latest_result") or {}
            latest_result.setdefault("dispatch_task_id", task_id)
            latest_result.setdefault("remote_task_id", last_task.get("remote_task_id"))
            if last_task.get("status") == "success":
                latest_result.setdefault("success", True)
                latest_result.setdefault("status", "success")
                _correlation_progress.finish("success")
                _update_job_status(job_id, "success")
                return latest_result

            error_msg = latest_result.get("error") or last_task.get("error_message") or f"dispatch task failed: {last_task.get('status')}"
            _correlation_logs.append(f"相关性 dispatch 失败: {error_msg}", "ERROR")
            _correlation_progress.finish("failed", error_msg)
            _update_job_status(job_id, "failed")
            latest_result.setdefault("success", False)
            latest_result.setdefault("status", "failed")
            latest_result.setdefault("error", error_msg)
            return latest_result
        except Exception as exc:
            logger.error(f"correlation dispatch failed: {exc}", exc_info=True)
            _correlation_logs.append(f"dispatch 调度失败: {exc}", "ERROR")
            _correlation_progress.finish("failed", str(exc))
            _update_job_status(job_id, "failed")
            return {
                "success": False,
                "status": "failed",
                "error": str(exc),
                "traceback": traceback.format_exc().splitlines()[-20:],
            }
        finally:
            _active_dispatch_task_id = None
            _stop_event.clear()


def _run_correlation_compute(factor_names: list, as_of_date: str = None, job_id: str = None, data_date: str = None, **_kwargs):
    return _run_correlation_compute_via_dispatch(
        factor_names=factor_names,
        as_of_date=as_of_date,
        job_id=job_id,
        data_date=data_date,
    )


def _get_loader(source: str = "single") -> FactorValueLoader:
    global _correlation_loader
    if _correlation_loader is None or getattr(_correlation_loader, '_source', None) != source:
        _correlation_loader = _correlation_compute_service.get_correlation_factor_value_loader(source=source)
    return _correlation_loader


class CorrelationComputeRequest(BaseModel):
    as_of_date: Optional[str] = Field(
        None,
        description=(
            "兼容字段：只用于校验 official cache 的截止日期；为空时使用 "
            "rdagent_assets/factor_values/_meta.json 的 as_of_date"
        ),
    )
    data_date: Optional[str] = Field(None, description="Scheduler metadata only; never selects non-official cache")
    factor_names: Optional[List[str]] = Field(None, description="指定因子列表，默认全部 official-eligible 因子")
    force_recompute: bool = Field(False, description="强制重新计算，忽略旧相关性结果")
    db_threshold: float = Field(0, description="写入 DB 的相关性阈值 (threshold=0 全量存储)")
    include_disabled: bool = Field(False, description="为 True 时包含已禁用因子")


@router.post("/correlations/compute", summary="触发因子相关性矩阵计算")
def compute_correlations(req: CorrelationComputeRequest):
    """触发因子相关性矩阵计算（后台线程池执行）。

    每次计算前清空所有历史相关性数据，使用独立指标计算的 single/*.parquet 缓存全量重算。
    """
    if _computing_lock.locked() and not req.force_recompute:
        return {
            "status": "computing",
            "message": "相关性计算正在进行中",
            "progress": _correlation_progress.snapshot(),
        }

    eligibility_service = FactorEligibilityService()

    # 确定因子列表
    if req.factor_names:
        factor_names = eligibility_service.get_eligible_factor_names(
            factor_names=req.factor_names,
            include_disabled=req.include_disabled,
        )
        skipped = set(req.factor_names) - set(factor_names)
        if skipped:
            logger.warning(f"跳过不可用/未改造的因子: {skipped}")
    else:
        factor_names = eligibility_service.get_eligible_factor_names(
            include_disabled=req.include_disabled,
        )

    if not factor_names:
        return {"status": "error", "message": "无可计算的因子"}

    cache_status = _correlation_compute_service.get_correlation_factor_cache_status()
    resolved_as_of_date = req.as_of_date or cache_status.get("as_of_date")

    global _compute_future
    _compute_future = _compute_executor.submit(
        _run_correlation_compute,
        factor_names,
        resolved_as_of_date,
        None,
        req.data_date,
    )

    return {
        "status": "accepted",
        "message": f"已提交 {len(factor_names)} 因子的相关性计算任务",
        "factor_count": len(factor_names),
        "as_of_date": resolved_as_of_date or "latest",
        "official_cache_window": {
            "start": cache_status.get("window_train_start") or "2018-08-01",
            "end": cache_status.get("window_backtest_end")
            or cache_status.get("as_of_date")
            or "2026-04-30",
            "cache_root": cache_status.get("cache_root"),
            "cache_source": cache_status.get("cache_source"),
        },
    }


@router.post("/correlations/cancel", summary="取消正在进行的相关性计算")
def cancel_correlation_compute():
    """发送取消信号，计算将在当前交易日完成后中断。"""
    if not _computing_lock.locked():
        return {"status": "idle", "message": "当前无计算任务在执行"}

    _stop_event.set()
    _correlation_logs.append("收到取消请求，正在中断计算...", "WARN")

    global _active_dispatch_task_id
    if _active_dispatch_task_id:
        import asyncio
        try:
            asyncio.run(_get_dispatch_service().cancel_task(_active_dispatch_task_id))
        except Exception as exc:
            logger.warning(f"取消 dispatch 相关性任务失败: {exc}")
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
        return _correlation_compute_service.get_correlation_factor_cache_status()
    except Exception as e:
        logger.error(f"获取缓存状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/correlations/overview", summary="Correlation overview using official offline factor cache")
def get_correlation_overview(data_date: Optional[str] = None):
    """Return correlation page overview from the official offline cache only.

    data_date filters the official metric snapshot_date only; it never
    changes the offline cache source used by correlation.
    """
    target_snapshot_date = None
    if data_date:
        raw = str(data_date).strip()
        if len(raw) == 8 and raw.isdigit():
            target_snapshot_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
        else:
            target_snapshot_date = raw

    metric_snapshots = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT snapshot_date, COUNT(DISTINCT factor_name) AS factor_count
                FROM aistock_factor_metrics
                WHERE calc_engine = 'qe_eval_v2'
                GROUP BY snapshot_date
                ORDER BY snapshot_date DESC
            """)
            for row in cur.fetchall():
                metric_snapshots.append({
                    "snapshot_date": str(row[0]),
                    "factor_count": row[1],
                })

    factor_stats = {
        "all": {"total": 0, "evaluated": 0, "correlation_cached": 0, "correlation_computed": 0},
        "enabled": {"total": 0, "evaluated": 0, "correlation_cached": 0, "correlation_computed": 0},
        "disabled": {"total": 0, "evaluated": 0, "correlation_cached": 0, "correlation_computed": 0},
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(is_available, TRUE) AS is_avail, COUNT(*)
                FROM aistock_factor_catalog
                WHERE transformation_status = 'SUCCESS'
                GROUP BY is_avail
            """)
            for row in cur.fetchall():
                is_avail, count = row[0], row[1]
                factor_stats["all"]["total"] += count
                if is_avail:
                    factor_stats["enabled"]["total"] += count
                else:
                    factor_stats["disabled"]["total"] += count

            if target_snapshot_date:
                cur.execute(
                    """
                    SELECT m.factor_name, COALESCE(c.is_available, TRUE) AS is_avail
                    FROM (
                        SELECT DISTINCT factor_name
                        FROM aistock_factor_metrics
                        WHERE calc_engine = 'qe_eval_v2'
                          AND snapshot_date = %s
                    ) m
                    JOIN aistock_factor_catalog c ON c.factor_name = m.factor_name
                    """,
                    (target_snapshot_date,),
                )
            else:
                cur.execute("""
                    SELECT m.factor_name, COALESCE(c.is_available, TRUE) AS is_avail
                    FROM (
                        SELECT DISTINCT factor_name
                        FROM aistock_factor_metrics
                        WHERE calc_engine = 'qe_eval_v2'
                    ) m
                    JOIN aistock_factor_catalog c ON c.factor_name = m.factor_name
                """)
            for row in cur.fetchall():
                is_avail = row[1]
                factor_stats["all"]["evaluated"] += 1
                if is_avail:
                    factor_stats["enabled"]["evaluated"] += 1
                else:
                    factor_stats["disabled"]["evaluated"] += 1

    pipeline = _correlation_compute_service.get_correlation_factor_value_pipeline()
    cached_singles = pipeline.get_cached_singles()
    cached_names = {c["factor_name"] for c in cached_singles}
    factor_stats["all"]["correlation_cached"] = len(cached_names)

    if cached_names:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(is_available, TRUE), COUNT(*)
                    FROM aistock_factor_catalog
                    WHERE factor_name = ANY(%s) AND transformation_status = 'SUCCESS'
                    GROUP BY is_available
                    """,
                    (list(cached_names),),
                )
                for row in cur.fetchall():
                    is_avail, count = row[0], row[1]
                    if is_avail:
                        factor_stats["enabled"]["correlation_cached"] = count
                    else:
                        factor_stats["disabled"]["correlation_cached"] = count

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(is_available, TRUE) AS is_avail, COUNT(*)
                FROM aistock_factor_catalog
                WHERE transformation_status = 'SUCCESS'
                  AND correlation_computed_at IS NOT NULL
                GROUP BY is_avail
            """)
            for row in cur.fetchall():
                is_avail, count = row[0], row[1]
                factor_stats["all"]["correlation_computed"] += count
                if is_avail:
                    factor_stats["enabled"]["correlation_computed"] = count
                else:
                    factor_stats["disabled"]["correlation_computed"] = count

    cs = _correlation_compute_service.get_correlation_factor_cache_status()
    single_cache = {
        "cached_count": cs.get("cached_count", 0),
        "total_size_mb": cs.get("total_size_mb", 0),
        "date_range": cs.get("date_range"),
        "as_of_date": cs.get("as_of_date"),
        "cache_source": cs.get("cache_source"),
        "cache_root": cs.get("cache_root"),
        "data_source_mode": cs.get("data_source_mode"),
        "window_train_start": cs.get("window_train_start"),
        "window_backtest_end": cs.get("window_backtest_end"),
        "disk_factor_count": cs.get("disk_factor_count"),
        "meta_factor_count": cs.get("meta_factor_count"),
        "orphan_parquet_count": cs.get("orphan_parquet_count"),
        "integrity_ok": cs.get("integrity_ok"),
    }

    correlation_meta = None
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
                correlation_meta = {
                    "as_of_date": str(row[0]),
                    "num_factors": row[1],
                    "num_high_corr_pairs": row[2],
                    "avg_correlation": row[3],
                    "computation_time_sec": row[4],
                    "hdf5_path": row[5],
                    "created_at": row[6].isoformat() if row[6] else None,
                }

    return {
        "metric_snapshots": metric_snapshots,
        "target_snapshot_date": target_snapshot_date,
        "official_cache_window": {
            "start": single_cache.get("window_train_start"),
            "end": single_cache.get("window_backtest_end") or single_cache.get("as_of_date"),
            "date_range": single_cache.get("date_range"),
            "cache_root": single_cache.get("cache_root"),
            "cache_source": single_cache.get("cache_source"),
        },
        "factor_stats": factor_stats,
        "single_cache": single_cache,
        "correlation_meta": correlation_meta,
    }


@router.get("/correlations/status", summary="查询相关性计算状态")
def get_correlation_status(include_disabled: bool = False):
    """查询最新的因子相关性计算状态和结果摘要。

    计算进行中时跳过 DB 查询（直接返回缓存值），避免与计算线程竞争连接池。
    仅 idle 状态才查 DB 刷新缓存。

    include_disabled: True 时, db_correlation_count/uncorrelated_factor_count
    按 "所有改造成功因子" 口径统计 (含禁用); False 时仅启用因子。
    缓存按 include_disabled 分桶, 勾选切换时会回落到对应桶的旧值直到下一次刷新完成。
    """
    is_computing = _computing_lock.locked()
    counts_bucket = _status_db_cache["counts_by_include_disabled"][include_disabled]

    refresh_errors: List[str] = []
    if not is_computing:
        # idle 状态: 查 DB 刷新缓存；若刷新失败，保留旧缓存并显式返回错误，避免静默回退为 0
        meta = _status_db_cache.get("meta")
        db_count = counts_bucket["db_count"]
        uncorrelated_count = counts_bucket["uncorrelated_count"]
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
                    meta = {
                        "as_of_date": str(row[0]),
                        "num_factors": row[1],
                        "num_high_corr_pairs": row[2],
                        "avg_correlation": row[3],
                        "computation_time_sec": row[4],
                        "hdf5_path": row[5],
                        "created_at": row[6].isoformat() if row[6] else None,
                    } if row else None
        except Exception as e:
            msg = f"读取相关性 job 元数据失败: {e}"
            logger.error(msg)
            refresh_errors.append(msg)

        try:
            eligible_ids = _current_correlation_eligible_factor_ids(include_disabled=include_disabled)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM qe_factor_correlations
                        WHERE factor_a_id = ANY(%s)
                          AND factor_b_id = ANY(%s)
                        """,
                        (eligible_ids, eligible_ids),
                    )
                    db_count = cur.fetchone()[0]
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM aistock_factor_catalog c
                        WHERE c.id = ANY(%s)
                          AND NOT EXISTS (
                              SELECT 1
                              FROM qe_factor_correlations q
                              WHERE q.factor_a_id = c.id OR q.factor_b_id = c.id
                          )
                        """,
                        (eligible_ids,),
                    )
                    uncorrelated_count = cur.fetchone()[0]
                    # live 高相关计数 (|corr|>0.7 和 |corr|>0.5), 按 eligible 过滤
                    cur.execute(
                        """
                        SELECT
                            COUNT(*) FILTER (WHERE ABS(correlation) > 0.7) AS hc07,
                            COUNT(*) FILTER (WHERE ABS(correlation) > 0.5) AS hc05
                        FROM qe_factor_correlations
                        WHERE factor_a_id = ANY(%s)
                          AND factor_b_id = ANY(%s)
                        """,
                        (eligible_ids, eligible_ids),
                    )
                    hc_row = cur.fetchone()
                    high_corr_count_07 = hc_row[0] if hc_row else 0
                    high_corr_count_05 = hc_row[1] if hc_row else 0
        except Exception as e:
            msg = f"读取相关性统计计数失败: {e}"
            logger.error(msg)
            refresh_errors.append(msg)

        if not refresh_errors:
            _status_db_cache["meta"] = meta
            counts_bucket["db_count"] = db_count
            counts_bucket["uncorrelated_count"] = uncorrelated_count
            counts_bucket["high_corr_count_07"] = high_corr_count_07
            counts_bucket["high_corr_count_05"] = high_corr_count_05
    # else: computing 状态 — 跳过 DB 查询，使用缓存值

    official_cache_status = _correlation_compute_service.get_correlation_factor_cache_status()

    return {
        "status": "computing" if is_computing else "idle",
        "progress": _correlation_progress.snapshot(),
        "db_correlation_count": counts_bucket["db_count"],
        "uncorrelated_factor_count": counts_bucket["uncorrelated_count"],
        # live 实时高相关计数 (随禁用/删除同步, 无需重算相关性)
        "live_high_corr_count_07": counts_bucket.get("high_corr_count_07", 0),
        "live_high_corr_count_05": counts_bucket.get("high_corr_count_05", 0),
        "include_disabled": include_disabled,
        "latest_computation": _status_db_cache.get("meta"),
        "in_memory_result": _latest_result is not None,
        "active_dispatch_task_id": _active_dispatch_task_id,
        "refresh_errors": refresh_errors,
        "official_cache": official_cache_status,
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
    include_disabled: bool = False,
):
    """获取完整因子相关性矩阵（用于前端热力图）。

    优先返回内存中的最新结果，否则从 HDF5 加载。
    as_of_date: 指定加载特定日期的矩阵（YYYY-MM-DD 或 YYYYMMDD）。
    threshold: 只在 high_corr_pairs 中返回 |corr| > threshold 的对。
    include_disabled: 为 True 时不过滤禁用因子，并返回 disabled_factors 列表。
    """
    import numpy as np

    result = _latest_result

    # 指定 as_of_date 时，从对应 HDF5 加载（不使用内存缓存）
    if as_of_date:
        loader = _get_loader()
        engine = CorrelationEngine(loader)
        hdf5_path = engine.get_hdf5_by_date(as_of_date)
        if not hdf5_path:
            raise HTTPException(
                status_code=404,
                detail=f"未找到 {as_of_date} 对应的相关性矩阵文件。请先用该日期触发计算。",
            )
        result = CorrelationResult.from_hdf5(hdf5_path)
        logger.info(f"从 HDF5 加载指定日期矩阵: {hdf5_path}, {len(result.factor_names)} 因子")

    # 未指定日期时，使用内存/最新 HDF5
    if result is None:
        loader = _get_loader()
        engine = CorrelationEngine(loader)
        hdf5_path = engine.get_latest_hdf5()
        if hdf5_path and os.path.exists(hdf5_path):
            result = CorrelationResult.from_hdf5(hdf5_path)
            logger.info(f"从 HDF5 加载相关性矩阵: {hdf5_path}, {len(result.factor_names)} 因子")

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="无可用的相关性矩阵。请先调用 POST /correlations/compute 触发计算。",
        )

    # 构建响应（先构建矩阵列表，后续过滤后可能被截断）
    pair_threshold = max(threshold, 0.5)
    high_pairs = result.get_high_corr_pairs(pair_threshold)

    # 过滤已删除/禁用的因子（HDF5 矩阵可能包含已删除因子的旧数据）
    disabled_factors = []
    # 拷贝 result，避免原地变异 _latest_result 影响后续请求
    factor_names = list(result.factor_names)
    matrix_copy = result.matrix.copy()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if include_disabled:
                    # 不过滤矩阵，但收集禁用因子名称供前端标识
                    cur.execute("""
                        SELECT factor_name FROM aistock_factor_catalog
                        WHERE is_available = FALSE
                    """)
                    disabled_factors = [row[0] for row in cur.fetchall()]
                else:
                    cur.execute("""
                        SELECT factor_name FROM aistock_factor_catalog
                        WHERE is_available = TRUE
                    """)
                    available_factors = {row[0] for row in cur.fetchall()}
                    high_pairs = [
                        p for p in high_pairs
                        if p.get("factor_a") in available_factors and p.get("factor_b") in available_factors
                    ]
                    # 过滤矩阵行/列：只保留可用因子（操作拷贝，不污染 _latest_result）
                    if factor_names and available_factors:
                        keep_indices = [i for i, fn in enumerate(factor_names) if fn in available_factors]
                        if len(keep_indices) < len(factor_names):
                            factor_names = [factor_names[i] for i in keep_indices]
                            matrix_copy = matrix_copy[np.ix_(keep_indices, keep_indices)]
    except Exception as e:
        logger.error(f"过滤已删除因子时出错: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"查询因子可用性失败: {e}")

    # 从（可能已过滤的）矩阵拷贝构建序列化列表
    matrix_list = []
    for row in matrix_copy:
        matrix_list.append([
            round(float(v), 6) if not np.isnan(v) else None
            for v in row
        ])

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
        "factor_names": factor_names,
        "factor_count": len(factor_names),
        "matrix": matrix_list,
        "effective_window": int(result.effective_window),
        "computation_time_sec": float(result.computation_time_sec),
        "high_corr_pairs": high_pairs[:200],
        "high_corr_count": len(high_pairs),
        "metadata": safe_metadata,
        "disabled_factors": disabled_factors,
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
        raise HTTPException(503, f"查询因子对相关性数据库失败: {e}")

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
            logger.warning("Daily correlation 计算失败: factor_a=%s, factor_b=%s, error=%s", fa, fb, e)
            daily_data = {"error": str(e)}

    # 无 DB 记录且无 daily_data 时，使用缓存计算（仅在 DB 无记录时触发，非 DB 故障兜底）
    if not db_result and not daily_data:
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

    # 查询两个因子的完整指标（catalog + independent + classification + source_code）
    factor_metrics = {}
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for fn in [fa, fb]:
                    cur.execute("""
                        SELECT factor_name, source, is_sota_factor, is_available,
                               code_text, asset_path
                        FROM aistock_factor_catalog WHERE factor_name = %s LIMIT 1
                    """, (fn,))
                    catalog_row = cur.fetchone()

                    cur.execute("""
                        SELECT ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir,
                               ic_positive_ratio, top_sharpe, top_max_drawdown, top_annual_return,
                               top_excess_annual_return, coverage, turnover, group_return_monotonicity
                        FROM aistock_factor_metrics
                        WHERE factor_name = %s AND eval_window = 'full' AND calc_engine = %s
                        ORDER BY calculated_at DESC LIMIT 1
                    """, (fn, CALC_ENGINE))
                    ind_row = cur.fetchone()

                    cur.execute("""
                        SELECT fr.official_grade, fr.official_score,
                               fr.rule_version AS official_rule_version,
                               cl.category, cl.llm_analysis, cl.description,
                               cl.factor_dimension, cl.grade_reason, cl.factor_source
                        FROM qe_factor_classification cl
                        LEFT JOIN aistock_factor_catalog cat
                            ON cat.factor_name = cl.factor_name AND cat.source = cl.factor_source
                        LEFT JOIN LATERAL (
                            SELECT official_grade, official_score, rule_version
                            FROM qe_factor_official_ratings r
                            WHERE r.factor_catalog_id = cat.id
                              AND r.rule_version = (
                                  SELECT rule_version
                                  FROM qe_rating_rule_versions
                                  WHERE status = 'active'
                                  ORDER BY activated_at DESC NULLS LAST, created_at DESC
                                  LIMIT 1
                              )
                            ORDER BY r.graded_at DESC
                            LIMIT 1
                        ) fr ON TRUE
                        WHERE cl.factor_name = %s
                        LIMIT 1
                    """, (fn,))
                    cl_row = cur.fetchone()

                    # 读取源代码: asset_path（官方原始源码文件）→ code_text（DB 展示兜底）
                    # 相关性详情不得读取非官方 live/simulation 改造代码路径。
                    source_code = None
                    if catalog_row:
                        rel_path = catalog_row.get("asset_path")
                        if rel_path:
                            full_path = os.path.join(_PROJECT_ROOT, rel_path)
                            if os.path.isfile(full_path):
                                try:
                                    with open(full_path, "r", encoding="utf-8") as f:
                                        source_code = f.read()
                                except Exception as e:
                                    logger.warning(f"读取官方因子源码文件失败 {full_path}: {e}")
                        if source_code is None:
                            code_text = catalog_row.get("code_text")
                            if code_text and code_text.strip():
                                source_code = code_text

                    # 构建 catalog dict，移除内部字段
                    catalog_dict = None
                    if catalog_row:
                        catalog_dict = dict(catalog_row)
                        for k in ("code_text", "asset_path"):
                            catalog_dict.pop(k, None)

                    factor_metrics[fn] = {
                        "catalog": catalog_dict,
                        "independent": dict(ind_row) if ind_row else None,
                        "classification": dict(cl_row) if cl_row else None,
                        "source_code": source_code,
                    }
    except Exception as e:
        raise HTTPException(503, f"查询因子指标数据库失败: {e}")

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
                      AND is_available = TRUE
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
    include_disabled: bool = False,
):
    """查询某个因子的所有高相关因子（|corr| > threshold）。

    当 include_metrics=true 时，返回每个因子的指标（IC/Sharpe/年化/回撤）、source、is_available，
    以及基准因子自身的 base_metrics。用于因子去重批量管理。
    include_disabled=True 时 JOIN 不过滤 is_available。
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
                    if include_disabled:
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
                    else:
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
                                AND cat_a.is_available = TRUE
                            JOIN aistock_factor_catalog cat_b ON c.factor_b_id = cat_b.id
                                AND cat_b.is_available = TRUE
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
                        WHERE factor_name = ANY(%s) AND eval_window = 'full' AND calc_engine = %s
                        ORDER BY factor_name, calculated_at DESC
                    """, (all_names, CALC_ENGINE))
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
                    if include_disabled:
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
                                AND cat_a.is_available = TRUE
                            JOIN aistock_factor_catalog cat_b ON c.factor_b_id = cat_b.id
                                AND cat_b.is_available = TRUE
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

    # 写库只做 catalog → id 映射, 不再按 is_available 过滤;
    # 准入策略由调用方（compute_correlations）通过 include_disabled 决定, 此处只负责持久化.
    catalog_rows = FactorEligibilityService().list_eligible_factors(include_disabled=True)
    catalog_name_to_id = {
        row["factor_name"]: int(row["id"])
        for row in catalog_rows
        if row.get("id") is not None
    }
    if not catalog_name_to_id:
        raise RuntimeError("catalog 中无可用于写入相关性的因子")

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 预处理: 构建去重的 (a_id, b_id) -> row 映射.
            # 如果某一侧因子在 catalog 里查不到 id (异常状态), 记 WARN 并 skip.
            seen = {}
            skipped_unknown = 0
            for r in records:
                fa_id = catalog_name_to_id.get(r["factor_a"])
                fb_id = catalog_name_to_id.get(r["factor_b"])
                if fa_id is None or fb_id is None:
                    skipped_unknown += 1
                    logger.warning(
                        "相关性记录因子在 catalog 中未找到 id, skip: factor_a=%s factor_b=%s",
                        r.get("factor_a"), r.get("factor_b"),
                    )
                    continue
                a_id, b_id = min(fa_id, fb_id), max(fa_id, fb_id)
                as_of_date = None
                data_period = r.get("data_period", "")
                if data_period and "as_of_" in data_period:
                    try:
                        as_of_date = data_period.split("as_of_")[1]
                    except (IndexError, ValueError):
                        raise RuntimeError(f"无法从 data_period 解析 as_of_date: {data_period}")
                if as_of_date is None:
                    raise RuntimeError(f"相关性记录缺少 as_of_date: {r}")
                seen[(a_id, b_id)] = (a_id, b_id, r["correlation"], r["method"], as_of_date, 252)

            values = list(seen.values())
            if not values:
                raise RuntimeError(
                    f"相关性结果全部无法映射到 catalog id, 拒绝写入, skip={skipped_unknown} 条"
                )

            execute_values(
                cur,
                """
                INSERT INTO qe_factor_correlations
                    (factor_a_id, factor_b_id, correlation, method,
                     as_of_date, data_window_days, computed_at)
                VALUES %s
                ON CONFLICT (factor_a_id, factor_b_id) DO UPDATE SET
                    correlation = EXCLUDED.correlation,
                    method = EXCLUDED.method,
                    as_of_date = EXCLUDED.as_of_date,
                    data_window_days = EXCLUDED.data_window_days,
                    computed_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s::DATE, %s, NOW())",
                page_size=2000,
            )

            computed_id_list = sorted({factor_id for pair in seen.keys() for factor_id in pair})
            cur.execute(
                """
                UPDATE aistock_factor_catalog
                SET correlation_computed_at = NULL,
                    correlation_pair_count = 0
                WHERE id = ANY(%s)
                """,
                (computed_id_list,),
            )
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
            """, (computed_id_list, computed_id_list))

        conn.commit()

    written = len(values)
    logger.info(
        f"批量写入 {written} 条相关性记录（去重后），skip {skipped_unknown} 条因子在 catalog 中未找到 id"
    )
    return written


def _persist_correlation_metadata(result: CorrelationResult) -> None:
    """写入相关性计算元数据。"""
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


# ── 相关性计算调度 API ────────────────────────────────────────

class CorrelationScheduleRequest(BaseModel):
    dataset: str = Field(
        "correlation_full",
        description="correlation_full",
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
    log_file = Path(__file__).parent.parent / "logs" / ("errors.log" if errors_only else "aistock.log")
    if not log_file.exists():
        return {"ok": True, "lines": [], "exists": False}
    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    if level:
        lines = [line for line in lines if f" {level.upper()} " in line]
    return {"ok": True, "lines": [line.rstrip() for line in lines[-tail:]], "exists": True}


# ================================================================
# 因子独立指标定时计算调度
# ================================================================

class FactorMetricsScheduleRequest(BaseModel):
    schedule_id: Optional[str] = Field(None, description="Optional schedule UUID; omitted keeps the default singleton schedule")
    factor_names: Optional[List[str]] = Field(None, description="Optional factor whitelist; omitted computes all eligible factors")
    include_disabled: bool = Field(False, description="是否包含禁用因子")
    frequency: str = Field("weekly", description="weekly | daily | manual")
    at: Optional[str] = Field("18:30", description="每日运行时间 HH:MM")
    day_of_week: Optional[str] = Field("sunday", description="周几运行（weekly 时有效）")
    data_date: Optional[str] = Field(None, description="数据快照日期 YYYYMMDD，留空用最新")
    start_date: str = Field(OFFICIAL_FACTOR_WINDOW_START, description="Official factor cache window start date YYYY-MM-DD")
    end_date: str = Field(OFFICIAL_FACTOR_WINDOW_END, description="Official factor cache window end date YYYY-MM-DD")
    workers: int = Field(4, description="并行度 1-8")
    one_shot: bool = Field(False, description="单次任务（执行完自动禁用）")
    enabled: bool = Field(True, description="是否启用")


@factor_metrics_router.get("/schedules", summary="List factor metrics schedules")
@router.get("/factor-metrics/schedules", summary="列出因子指标计算调度配置")
def list_factor_metrics_schedules():
    """列出所有 dataset LIKE 'factor_metrics_%' 的调度配置。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schedule_id, dataset, mode, enabled, frequency, options,
                       last_run_at, next_run_at, last_status, last_error,
                       created_at, updated_at
                FROM market.ingestion_schedules
                WHERE dataset LIKE 'factor_metrics_%%'
                ORDER BY created_at DESC
            """)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

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


@factor_metrics_router.post("/schedules", summary="Create or update factor metrics schedule")
@router.post("/factor-metrics/schedules", summary="创建/更新因子指标计算调度")
def upsert_factor_metrics_schedule(req: FactorMetricsScheduleRequest):
    """创建或更新一个因子指标计算调度。"""
    schedule_id = None
    dataset = "factor_metrics_compute"
    factor_names = None
    if req.factor_names:
        factor_names = [str(name).strip() for name in req.factor_names if str(name).strip()]
        if not factor_names:
            factor_names = None
    requested_schedule_id = None
    if req.schedule_id:
        try:
            requested_schedule_id = uuid.UUID(str(req.schedule_id))
        except ValueError:
            raise HTTPException(status_code=400, detail=f"schedule_id is not a valid UUID: {req.schedule_id}")
    options = {
        "include_disabled": req.include_disabled,
        "data_date": req.data_date,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "workers": max(1, min(8, req.workers)),
        "timeout_per_factor": 600,
        "one_shot": req.one_shot,
    }
    if factor_names:
        options["factor_names"] = factor_names
    if req.at:
        options["at"] = req.at
    if req.day_of_week and req.frequency == "weekly":
        options["day_of_week"] = req.day_of_week

    options_json = json.dumps(options, ensure_ascii=False, default=str)

    with get_conn() as conn:
        with conn.cursor() as cur:
            if requested_schedule_id is not None:
                schedule_id = requested_schedule_id
            else:
                cur.execute(
                    "SELECT schedule_id FROM market.ingestion_schedules WHERE dataset=%s",
                    (dataset,),
                )
                row = cur.fetchone()
                schedule_id = row[0] if row else uuid.uuid4()
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
            """, (str(schedule_id), dataset, "init", req.enabled, req.frequency, options_json))
        conn.commit()

    from ..services.quantevolver.factor_metrics_scheduler import factor_metrics_scheduler
    factor_metrics_scheduler.refresh_schedules()

    return {"schedule_id": str(schedule_id), "dataset": dataset,
            "frequency": req.frequency, "enabled": req.enabled, "options": options}


@factor_metrics_router.post("/schedules/{schedule_id}/toggle", summary="Toggle factor metrics schedule")
@router.post("/factor-metrics/schedules/{schedule_id}/toggle", summary="切换调度启用/禁用")
def toggle_factor_metrics_schedule(schedule_id: str, enabled: bool = True):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE market.ingestion_schedules SET enabled=%s, updated_at=NOW() WHERE schedule_id=%s AND dataset LIKE 'factor_metrics_%%'",
                (enabled, schedule_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"因子指标调度 {schedule_id} 不存在")
        conn.commit()
    from ..services.quantevolver.factor_metrics_scheduler import factor_metrics_scheduler
    factor_metrics_scheduler.refresh_schedules()
    return {"schedule_id": schedule_id, "enabled": enabled}


@factor_metrics_router.post("/schedules/{schedule_id}/run", summary="Run factor metrics schedule now")
@router.post("/factor-metrics/schedules/{schedule_id}/run", summary="立即执行因子指标调度")
def run_factor_metrics_schedule_now(schedule_id: str):
    """手动触发一个因子指标调度。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT dataset, options FROM market.ingestion_schedules WHERE schedule_id=%s AND dataset LIKE 'factor_metrics_%%'",
                (schedule_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"因子指标调度 {schedule_id} 不存在")
            dataset, options = row
            if isinstance(options, str):
                options = json.loads(options)
            elif options is None:
                options = {}

    from ..services.quantevolver.factor_metrics_scheduler import factor_metrics_scheduler
    job_id = factor_metrics_scheduler.submit_job(schedule_id, dataset, options, triggered_by="manual")
    return {"status": "accepted", "job_id": str(job_id), "schedule_id": schedule_id}


@factor_metrics_router.delete("/schedules/{schedule_id}", summary="Delete factor metrics schedule")
@router.delete("/factor-metrics/schedules/{schedule_id}", summary="删除因子指标调度")
def delete_factor_metrics_schedule(schedule_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM market.ingestion_schedules WHERE schedule_id=%s AND dataset LIKE 'factor_metrics_%%'",
                (schedule_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"因子指标调度 {schedule_id} 不存在")
        conn.commit()
    from ..services.quantevolver.factor_metrics_scheduler import factor_metrics_scheduler
    factor_metrics_scheduler.refresh_schedules()
    return {"deleted": True, "schedule_id": schedule_id}


@factor_metrics_router.get("/jobs", summary="List factor metrics jobs")
@router.get("/factor-metrics/jobs", summary="查询因子指标计算任务历史")
def list_factor_metrics_jobs(limit: int = 20):
    """返回最近的因子指标计算任务记录。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT job_id, job_type, status, created_at, started_at, finished_at, summary
                FROM market.ingestion_jobs
                WHERE (summary->>'dataset') LIKE 'factor_metrics_%%'
                   OR job_type LIKE 'factor_metrics_%%'
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
