"""
QuantEvolver 后端API路由

路由前缀: /quantevolver (在main.py中通过prefix="/api/v1"注册，最终路径为/api/v1/quantevolver/...)

Phase 1 API（数据同步）：
- POST /api/v1/quantevolver/sync/alpha-factors     触发Alpha158/360因子同步
- POST /api/v1/quantevolver/sync/model-task/{task_id}  同步指定task的模型数据
- GET  /api/v1/quantevolver/factors                 获取全部因子列表
- GET  /api/v1/quantevolver/models                  获取全部模型列表
- GET  /api/v1/quantevolver/strategies              获取全部策略列表

Phase 2 API（Agent + 配置）：
- POST /api/v1/quantevolver/factor-analyst/analyze          分析指定因子
- POST /api/v1/quantevolver/factor-analyst/batch-analyze    批量分析
- GET  /api/v1/quantevolver/factor-analyst/classifications  获取分类结果
- POST /api/v1/quantevolver/factor-analyst/recommend        推荐因子组合
- POST /api/v1/quantevolver/portfolio/evaluate              评估组合
- POST /api/v1/quantevolver/portfolio/recommend             智能推荐
- POST /api/v1/quantevolver/config/generate                 生成QLib配置
- GET  /api/v1/quantevolver/experiments                     实验列表
- GET  /api/v1/quantevolver/experiments/{id}                实验详情
- POST /api/v1/quantevolver/experiments/{id}/sync-results   同步结果
- POST /api/v1/quantevolver/experiments/{id}/run             一键执行实验
- GET  /api/v1/quantevolver/experiments/{id}/run-status      查询执行状态
- GET  /api/v1/quantevolver/experiments/{id}/logs            SSE实时日志流
- GET  /api/v1/quantevolver/prompts                        获取提示词列表
- GET  /api/v1/quantevolver/prompts/{agent_type}/{key}     获取指定提示词
- PUT  /api/v1/quantevolver/prompts/{agent_type}/{key}     更新提示词
- POST /api/v1/quantevolver/prompts                        创建提示词
- DELETE /api/v1/quantevolver/prompts/{id}                 删除提示词
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import httpx
from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from backend.services.quantevolver.factor_cache_coverage import (
    DEFAULT_WARMUP_TOLERANCE_DAYS,
    factor_cache_covers_window,
)
from ..db.pg_pool import get_conn
from ..services.quantevolver.callback_urls import build_aistock_callback_url
from ..services.quantevolver.experiment_config import ensure_qe_risk_policy, normalize_label_horizon
from ..services.quantevolver.label_horizon_schema import ensure_qe_label_horizon_schema
from ..services.quantevolver.node_execution import (
    QENodePreflightError,
    preflight_qe_node,
    resolve_default_qe_node_id,
)
from ..services.quantevolver.seed_contract import normalize_single_experiment_seed_config
from .model_registry import router as model_registry_router

AISTOCK_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _build_multi_alpha_group_command(gc: dict[str, Any], node_label: str | None = None) -> str:
    group_name = gc.get("group_name", "")
    prefix = f"echo '=== Running group: {group_name} on {node_label} ==='" if node_label else f"echo '=== Running group: {group_name} ==='"
    core_cmd = gc.get("wsl_command_core", "")
    if core_cmd and not core_cmd.startswith("#"):
        # 用 subshell 隔离 cd，避免多分组串联时路径污染
        return f"{prefix} && (cd group_{group_name} && {core_cmd})"

    raise ValueError(
        f"Multi-alpha group {group_name} has no unified executable command "
        "(missing wsl_command_core)"
    )


logger = logging.getLogger("aistock.routers.quantevolver")

router = APIRouter(prefix="/quantevolver", tags=["QuantEvolver"])

router.include_router(model_registry_router)

QE_EXPERIMENT_LOG_TERMINAL_STATUSES = {
    "completed",
    "failed",
    "interrupted",
    "timeout",
    "cancelled",
    "canceled",
    "stopped",
}
QE_EXPERIMENT_LOG_TAIL_DEFAULT_LINES = 500


def _multi_alpha_distributed_enabled() -> bool:
    """Return whether experimental distributed Multi-Alpha execution is enabled."""
    return os.getenv("AISTOCK_MULTI_ALPHA_DISTRIBUTED_ENABLED", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _assert_multi_alpha_execution_mode_supported(config: Any) -> None:
    """Fail loudly when a Multi-Alpha mode is not supported by the current rollout."""
    mode = "serial"
    if isinstance(config, dict):
        mode = str(config.get("execution_mode") or "serial")
    else:
        mode = str(getattr(config, "execution_mode", "serial") or "serial")

    if mode == "distributed" and not _multi_alpha_distributed_enabled():
        raise HTTPException(
            status_code=400,
            detail=(
                "Multi-Alpha distributed execution is disabled in the current rollout. "
                "Use execution_mode='serial' for WSL single-node validation, or enable "
                "AISTOCK_MULTI_ALPHA_DISTRIBUTED_ENABLED=1 after Phase 3 is implemented."
            ),
        )


# ============================================================
# Pydantic 请求/响应模型
# ============================================================

class SyncAlphaRequest(BaseModel):
    alpha_types: List[str] = Field(default=["alpha158", "alpha360"], description="要同步的Alpha类型")


class SyncModelTaskRequest(BaseModel):
    task_dir: Optional[str] = Field(None, description="任务资产目录，不提供则使用默认路径")


class AnalyzeFactorRequest(BaseModel):
    factor_name: str
    factor_source: str
    use_llm: bool = False


class BatchAnalyzeRequest(BaseModel):
    use_llm: bool = False
    source_filter: Optional[str] = None
    factor_names: Optional[List[str]] = None  # 指定因子名称列表，为空则分析全部


class FullPipelineRequest(BaseModel):
    task_ids: List[str] = []
    factor_names: Optional[List[str]] = None
    skip_completed: bool = True
    max_transform_retries: int = 3
    skip_transform: bool = False
    data_date: Optional[str] = Field(None, description="兼容字段：旧快照日期；官方离线链路将其解释为 end_date")
    start_date: Optional[str] = Field(None, description="官方离线训练/回测起始日期")
    end_date: Optional[str] = Field(None, description="官方离线训练/回测截止日期")


class RecommendFactorsRequest(BaseModel):
    target_count: int = 20
    include_categories: Optional[List[str]] = None
    min_grade: str = "C"
    diversity_weight: float = 0.5


class EvaluateCombinationRequest(BaseModel):
    factor_names: List[str]
    model_id: Optional[str] = None
    strategy_id: Optional[str] = None
    custom_params: Optional[Dict[str, Any]] = None


class RecommendCombinationsRequest(BaseModel):
    target_profiles: Optional[List[str]] = None
    max_combinations: int = 3


class GenerateFromRequirementRequest(BaseModel):
    user_requirement: str = Field(..., description="用户自然语言需求描述，如'稳健低回撤策略'、'高收益进攻型组合'")
    use_llm: bool = Field(True, description="是否使用LLM辅助决策")
    max_factors: int = Field(30, description="最大因子数量")


class SmartSelectRequest(BaseModel):
    user_requirement: str = Field(..., description="用户自然语言需求描述")
    max_factors: int = Field(20, description="最大因子数量")


class EvaluatePortfolioRequest(BaseModel):
    factor_names: List[str] = Field(..., description="选择的因子列表")
    model_id: Optional[str] = Field(None, description="选择的模型ID")
    strategy_id: Optional[str] = Field(None, description="选择的策略ID")
    custom_params: Optional[Dict[str, Any]] = None


class GenerateConfigRequest(BaseModel):
    factor_names: List[str] = Field(default_factory=list)
    factor_sources: Optional[Dict[str, str]] = None
    model_id: Optional[str] = None
    strategy_id: Optional[str] = None
    data_split: Optional[Dict[str, str]] = None
    custom_params: Optional[Dict[str, Any]] = None
    experiment_name: Optional[str] = None
    dispatch_mode: Optional[str] = Field(None, description="调度模式: normal / evolution")
    evolution_params: Optional[Dict[str, Any]] = Field(None, description="演进参数（dispatch_mode=evolution时）")
    enable_sector_hmm: bool = Field(False, description="是否启用行业 HMM 热度调整")
    hmm_model_version_id: Optional[str] = Field(None, description="HMM 模型快照 ID (snapshot_id)")
    hmm_signal_preset: Optional[str] = Field(None, description="HMM 信号系数档位: preset_A/preset_B")
    unfilled_handler: Optional[str] = Field(None, description="尾盘涨停未成交处理: TAIL_BOOST(加仓持仓股) / TAIL_SUBSTITUTE(替补买入)")
    unfilled_handler_params: Optional[Dict[str, Any]] = Field(None, description="尾盘处理参数，如 {backup_depth: 15, trigger_minute: 210}")
    # ── Multi-Alpha (Phase 3) ──────────────────────────────────────
    alpha_mode: Optional[str] = Field(None, description="single (默认) / multi")
    multi_alpha_config: Optional[Dict[str, Any]] = Field(None, description="Multi-Alpha 分组配置 JSON")
    parent_multi_alpha_id: Optional[str] = Field(None, description="源实验ID（演进血统追踪）")


class SingleExperimentPendingCreateRequest(GenerateConfigRequest):
    created_by_type: Optional[str] = Field("mcp", description="创建来源类型: mcp/ui/agent")
    created_by_name: Optional[str] = Field(None, description="创建来源名称，如 Codex 或 Claude Code")
    source_context_json: Optional[Dict[str, Any]] = Field(None, description="MCP/Agent 创建上下文")
    provenance: Optional[Dict[str, Any]] = Field(None, description="额外溯源信息，写入 custom_params.qe_mcp_provenance")


class SingleExperimentConfigUpdateRequest(GenerateConfigRequest):
    """Editable single-experiment payload; same shape as config generation."""


def _model_to_dict_compat(model: BaseModel) -> Dict[str, Any]:
    """Support both Pydantic v1 and v2 request objects."""
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _model_field_names(model_cls: type[BaseModel]) -> List[str]:
    fields = getattr(model_cls, "model_fields", None) or getattr(model_cls, "__fields__", {})
    return list(fields)


def _parse_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return list(parsed) if isinstance(parsed, list) else []
    return []


def _single_experiment_start_state(exp_record: Mapping[str, Any]) -> tuple[bool, str]:
    status = str(exp_record.get("status") or "").lower()
    if exp_record.get("is_evolution_loop"):
        return False, "evolution loop rows must be edited through the evolution task UI"
    if status not in {"created", "pending"}:
        return False, f"experiment status is {exp_record.get('status') or 'unknown'}"
    for runtime_key in ("qe_task_id", "qe_loop_id", "started_at", "completed_at"):
        if exp_record.get(runtime_key):
            return False, f"experiment already has runtime field {runtime_key}"
    return True, "single experiment has not been submitted"


def _single_experiment_editable_payload(exp_record: Mapping[str, Any]) -> Dict[str, Any]:
    custom_params = _parse_json_object(exp_record.get("custom_params"))
    startable, reason = _single_experiment_start_state(exp_record)
    alpha_mode = exp_record.get("alpha_mode") or "single"
    editable = startable and alpha_mode == "single"
    return {
        "experiment_id": exp_record.get("experiment_id"),
        "experiment_name": exp_record.get("experiment_name"),
        "status": exp_record.get("status"),
        "alpha_mode": alpha_mode,
        "factor_names": _parse_json_list(exp_record.get("factor_names")),
        "factor_sources": (custom_params.get("qe_factor_sources") or {}).copy()
        if isinstance(custom_params.get("qe_factor_sources"), dict)
        else None,
        "model_id": exp_record.get("model_id"),
        "strategy_id": exp_record.get("strategy_id"),
        "data_split": _parse_json_object(exp_record.get("data_split")),
        "custom_params": custom_params,
        "editable": editable,
        "startable": startable,
        "resume_allowed": False,
        "start_reason": reason if startable else f"not startable: {reason}",
        "created_at": exp_record.get("created_at"),
        "updated_at": exp_record.get("updated_at"),
        "started_at": exp_record.get("started_at"),
        "completed_at": exp_record.get("completed_at"),
    }


def _validate_qe_catalog_refs(strategy_id: Optional[str], model_id: Optional[str]) -> None:
    if strategy_id:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM aistock_strategy_catalog WHERE strategy_id = %s", (strategy_id,))
                if not cur.fetchone():
                    raise HTTPException(
                        status_code=400,
                        detail=f"strategy_id='{strategy_id}' 在策略目录中不存在",
                    )

    if model_id:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM aistock_model_catalog WHERE model_id = %s", (model_id,))
                if not cur.fetchone():
                    raise HTTPException(
                        status_code=400,
                        detail=f"model_id='{model_id}' 在模型目录中不存在",
                    )


def _normalize_single_experiment_custom_params(
    req: GenerateConfigRequest,
    *,
    source: str,
) -> Dict[str, Any]:
    """Normalize single-experiment executable params at create/edit boundary."""

    seed_config = normalize_single_experiment_seed_config({"custom_params": req.custom_params or {}})
    custom_params = dict(seed_config.get("custom_params") or {})

    try:
        from ..services.quantevolver.blacklist_snapshot import attach_persistent_blacklist_snapshot
        custom_params = attach_persistent_blacklist_snapshot(custom_params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        label_horizon = normalize_label_horizon(custom_params.get("label_horizon"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if label_horizon == 1:
        custom_params.pop("label_horizon", None)
    else:
        try:
            ensure_qe_label_horizon_schema()
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
        custom_params["label_horizon"] = label_horizon

    try:
        custom_params = ensure_qe_risk_policy(custom_params, source=source)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    _valid_unfilled_handlers = {"TAIL_BOOST", "TAIL_SUBSTITUTE"}
    if req.unfilled_handler:
        if req.unfilled_handler not in _valid_unfilled_handlers:
            raise HTTPException(
                status_code=400,
                detail=f"unfilled_handler='{req.unfilled_handler}' 无效，允许值: {', '.join(sorted(_valid_unfilled_handlers))}",
            )
        custom_params["unfilled_handler"] = req.unfilled_handler
        uf_params = req.unfilled_handler_params or {}
        if uf_params.get("trigger_minute"):
            custom_params["unfilled_trigger_minute"] = uf_params["trigger_minute"]
        if uf_params.get("backup_depth"):
            custom_params["unfilled_backup_depth"] = uf_params["backup_depth"]

    if custom_params.get("enable_sector_hmm"):
        hmm_version_id = custom_params.get("hmm_model_version_id")
        if not hmm_version_id:
            raise HTTPException(
                status_code=400,
                detail="启用行业 HMM 时必须提供 hmm_model_version_id",
            )
        from ..services.hmm_training_service import HMMTrainingService
        hmm_svc = HMMTrainingService()
        snapshot = hmm_svc.get_snapshot(hmm_version_id)
        if snapshot is None:
            raise HTTPException(
                status_code=400,
                detail=f"HMM 快照 {hmm_version_id} 不存在",
            )
        if snapshot.get("status") != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"HMM 快照状态为 '{snapshot.get('status')}'，需要 'completed'",
            )
        hmm_model_path = snapshot["model_path"]
        custom_params["sector_hmm_model_path"] = hmm_model_path
        try:
            config_id = snapshot["config_id"]
            configs = hmm_svc.list_configs("sector_hmm")
            found_config = False
            for cfg in configs:
                if cfg["config_id"] == config_id:
                    cj = cfg["config_json"]
                    if isinstance(cj, str):
                        cj = json.loads(cj)
                    custom_params["hmm_config_json"] = cj
                    if "signal_presets" in cj:
                        custom_params["hmm_signal_presets"] = cj["signal_presets"]
                    found_config = True
                    break
            if not found_config:
                raise HTTPException(
                    status_code=400,
                    detail=f"HMM config_id='{config_id}' 未在系统中找到",
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"读取 HMM signal_presets 失败: {e}",
            ) from e

    return custom_params


class CreateStrategyRequest(BaseModel):
    strategy_id: str = Field(..., description="策略唯一ID（英文+下划线）")
    display_name: str = Field(..., description="策略显示名称")
    description: Optional[str] = Field(None, description="策略描述")
    strategy_type: str = Field("daily", description="策略类型: daily/intraday")
    source_code: str = Field(..., description="策略Python源代码")
    market: Optional[str] = Field("csi300", description="适用市场")
    freq: Optional[str] = Field("day", description="频率: day/1min")
    default_kwargs: Optional[Dict[str, Any]] = Field(None, description="默认参数")
    param_schema: Optional[List[Dict[str, Any]]] = Field(None, description="参数定义")
    parent_strategy_id: Optional[str] = Field(None, description="来源模板策略ID")


class UpdateStrategyRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    strategy_type: Optional[str] = None
    source_code: Optional[str] = None
    market: Optional[str] = None
    freq: Optional[str] = None
    default_kwargs: Optional[Dict[str, Any]] = None
    param_schema: Optional[List[Dict[str, Any]]] = None


class AnalyzeStrategyRequest(BaseModel):
    strategy_id: str = Field(..., description="要分析的策略ID")
    source_code: Optional[str] = Field(None, description="可选：传入修改后的代码进行分析，不传则使用数据库中的代码")


class FactorRatingRunRequest(BaseModel):
    rule_version: Optional[str] = Field(None, description="评级规则版本，默认使用当前激活版本")
    scope_type: str = Field(..., description="评级范围: selected/filter/all")
    selected_factors: Optional[List[Dict[str, str]]] = Field(None, description="选中的因子列表，元素包含 factor_name/source")
    filters: Optional[Dict[str, Any]] = Field(None, description="当前筛选条件")
    triggered_from: str = Field("ui_toolbar", description="触发来源，正式评级只允许 ui_toolbar")

    model_config = ConfigDict(extra="forbid")


# ============================================================
# Phase 1 API: 数据同步
# ============================================================

@router.post("/sync/alpha-factors")
def sync_alpha_factors(req: SyncAlphaRequest):
    """触发Alpha158/360因子同步。"""
    try:
        import sys
        from pathlib import Path
        # 动态导入同步脚本中的函数
        debug_tools_dir = Path(__file__).resolve().parents[1].parent / "debug_tools"
        sys.path.insert(0, str(debug_tools_dir.parent))

        from debug_tools.qe_sync_alpha_factors import sync_alpha_factors as _sync

        results = {}
        for alpha_type in req.alpha_types:
            result = _sync(alpha_type)
            results[alpha_type] = result

        return {"ok": True, "results": results}
    except Exception as e:
        logger.exception("Alpha因子同步失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync/model-task/{task_id}")
def sync_model_task(task_id: str, req: SyncModelTaskRequest = None):
    """同步指定task的模型数据到aistock_model_catalog。"""
    try:
        from ..services.rdagent_model_catalog_sync import sync_models_from_task

        task_dir = req.task_dir if req and req.task_dir else None
        if not task_dir:
            # 使用默认路径
            default_root = AISTOCK_PROJECT_ROOT / "rdagent_assets" / "rdagent_tasks" / task_id
            default_root.mkdir(parents=True, exist_ok=True)
            task_dir = str(default_root)

        result = sync_models_from_task(task_id=task_id, task_dir=task_dir)
        return {
            "ok": result.ok,
            "total_models": result.total_models,
            "inserted": result.inserted,
            "errors": result.errors,
        }
    except Exception as e:
        logger.exception(f"模型同步失败: task_id={task_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors")
def list_factors(
    source: Optional[str] = Query(None, description="过滤source"),
    exclude_source: Optional[str] = Query(None, description="排除的source，逗号分隔"),
    search: Optional[str] = Query(None, description="搜索因子名称"),
    category: Optional[str] = Query(None, description="过滤类别，__empty__表示未分类"),
    grade: Optional[str] = Query(None, description="过滤评级，__empty__表示未评级"),
    availability: Optional[str] = Query(None, description="过滤可用状态: enabled/disabled/all"),
    cache_filter: Optional[str] = Query(None, description="因子值缓存过滤: has_cache/no_cache/covers_range/missing_range/hash_mismatch"),
    cache_start_date: Optional[str] = Query(None, description="用于判断缓存覆盖的目标开始日期"),
    cache_end_date: Optional[str] = Query(None, description="用于判断缓存覆盖的目标结束日期"),
    sort_field: Optional[str] = Query(None, description="排序字段"),
    sort_order: Optional[str] = Query("desc", description="排序方向: asc/desc"),
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """获取全部因子列表。"""
    try:
        from ..db.pg_pool import get_conn

        conditions = []
        params = []
        # 需要 JOIN 分类表的条件（category / grade 筛选）
        cl_conditions = []
        cl_params = []

        if source:
            conditions.append("c.source = %s")
            params.append(source)
        if exclude_source:
            ex_list = [s.strip() for s in exclude_source.split(",") if s.strip()]
            if ex_list:
                placeholders = ",".join(["%s"] * len(ex_list))
                conditions.append(f"c.source NOT IN ({placeholders})")
                params.extend(ex_list)
        if search:
            conditions.append("c.factor_name ILIKE %s")
            params.append(f"%{search}%")
        if availability == "enabled":
            conditions.append("c.is_available = TRUE")
        elif availability == "disabled":
            conditions.append("c.is_available = FALSE")

        # category / grade 筛选：__empty__ 表示未分类/未评级
        if category == "__empty__":
            cl_conditions.append("cl.category IS NULL")
        elif category:
            cl_conditions.append("cl.category = %s")
            cl_params.append(category)
        rating_join_sql = """
            LEFT JOIN LATERAL (
                SELECT official_grade, official_score, grade_reason_structured, rule_version,
                       llm_audit_summary, llm_risk_notes
                FROM qe_factor_official_ratings r
                WHERE r.factor_catalog_id = c.id
                  AND r.rule_version = (
                      SELECT rule_version FROM qe_rating_rule_versions
                      WHERE status = 'active'
                      ORDER BY activated_at DESC NULLS LAST, created_at DESC
                      LIMIT 1
                  )
                ORDER BY r.graded_at DESC
                LIMIT 1
            ) fr ON TRUE
        """
        rating_select_sql = """
                           fr.official_grade AS official_grade,
                           fr.official_score AS official_score,
                           fr.grade_reason_structured AS official_grade_reason_structured,
                           fr.rule_version AS official_rule_version,
                           fr.llm_audit_summary AS official_llm_audit_summary,
                           fr.llm_risk_notes AS official_llm_risk_notes
        """

        if grade == "__empty__":
            cl_conditions.append("fr.official_grade IS NULL")
        elif grade:
            cl_conditions.append("fr.official_grade = %s")
            cl_params.append(grade)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        # 分类表条件追加到 WHERE（需要 LEFT JOIN 后才能使用）
        cl_where = (" AND " + " AND ".join(cl_conditions)) if cl_conditions else ""
        cache_sort_fields = {"cache_status", "cache_start_date", "cache_end_date", "cache_computed_at", "cache_size_mb"}
        cache_filter_norm = (cache_filter or "all").strip().lower()
        cache_post_process = (
            cache_filter_norm != "all"
            or bool(cache_start_date)
            or bool(cache_end_date)
            or bool(sort_field in cache_sort_fields)
        )

        # 排序字段白名单映射（防SQL注入）
        SORT_FIELD_MAP = {
            "factor_name": "c.factor_name",
            "source": "c.source",
            "ic": "m.ic_mean",
            "sharpe": "m.top_excess_sharpe",
            "annualized_return": "m.top_excess_annual_return",
            "is_sota_factor": "c.is_sota_factor",
            "ind_rank_ic": "m.rank_ic_mean",
            "ind_rank_ic_1d": "m.rank_ic_1d",
            "ind_rank_ic_5d": "m.rank_ic_5d",
            "ind_rank_ic_10d": "m.rank_ic_10d",
            "ind_rank_ic_20d": "m.rank_ic_20d",
            "ind_rank_ic_best_abs": (
                "CASE WHEN m.rank_ic_1d IS NULL AND m.rank_ic_5d IS NULL "
                "AND m.rank_ic_10d IS NULL AND m.rank_ic_20d IS NULL THEN NULL "
                "ELSE GREATEST(COALESCE(ABS(m.rank_ic_1d), 0), "
                "COALESCE(ABS(m.rank_ic_5d), 0), "
                "COALESCE(ABS(m.rank_ic_10d), 0), "
                "COALESCE(ABS(m.rank_ic_20d), 0)) END"
            ),
            "ind_ic": "m.ic_mean",
            "ind_sharpe": "m.top_excess_sharpe",
            "ind_annual_return": "m.top_excess_annual_return",
            "ind_icir": "m.icir",
            "has_ind_metrics": "(m.ic_mean IS NOT NULL)::int",
            "grade": "fr.official_grade",
            "category": "cl.category",
            "factor_dimension": "cl.factor_dimension",
            "generated_at_utc": "c.generated_at_utc",
            "ind_calculated_at": "m.calculated_at",
            "decay_status": "m1m.rank_ic_mean",
        }
        direction = "ASC" if sort_order == "asc" else "DESC"
        nulls_clause = "NULLS FIRST" if direction == "ASC" else "NULLS LAST"
        if sort_field and sort_field in SORT_FIELD_MAP:
            if sort_field == "grade":
                # S>A>B>C>D 自定义排序
                order_clause = f"CASE fr.official_grade WHEN 'S' THEN 5 WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 WHEN 'D' THEN 1 ELSE 0 END {direction} {nulls_clause}"
            else:
                col = SORT_FIELD_MAP[sort_field]
                order_clause = f"{col} {direction} {nulls_clause}"
        else:
            order_clause = "c.is_sota_factor DESC NULLS LAST, m.ic_mean DESC NULLS LAST"
        query_limit = 5000 if cache_post_process else limit
        query_offset = 0 if cache_post_process else offset

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 当有 category/grade 筛选时，count 也需要 JOIN 分类表
                if cl_conditions:
                    cur.execute(
                        f"""SELECT COUNT(*) FROM aistock_factor_catalog c
                            LEFT JOIN qe_factor_classification cl
                                ON cl.factor_name = c.factor_name AND cl.factor_source = c.source
                            {rating_join_sql}
                            WHERE {where_clause}{cl_where}""",
                        params + cl_params,
                    )
                else:
                    cur.execute(
                        f"""SELECT COUNT(*) FROM aistock_factor_catalog c
                            LEFT JOIN qe_factor_classification cl
                                ON cl.factor_name = c.factor_name AND cl.factor_source = c.source
                            {rating_join_sql}
                            WHERE {where_clause}""",
                        params,
                    )
                total = cur.fetchone()[0]

                # LEFT JOIN 独立因子指标 + 分类表
                cur.execute(f"""
                    SELECT c.factor_name, c.source, c.expression,
                           m.ic_mean AS ic, m.top_excess_sharpe AS sharpe,
                           m.top_excess_annual_return AS annualized_return,
                           c.is_sota_factor, c.catalog_source,
                           c.description_cn, c.generated_at_utc, c.is_available,
                           c.factor_type, c.data_source,
                            m.ic_mean AS ind_ic, m.top_excess_sharpe AS ind_sharpe,
                            m.top_excess_annual_return AS ind_annual_return,
                            m.rank_ic_mean AS ind_rank_ic, m.icir AS ind_icir,
                            m.rank_ic_1d AS ind_rank_ic_1d,
                            m.rank_ic_5d AS ind_rank_ic_5d,
                            m.rank_ic_10d AS ind_rank_ic_10d,
                            m.rank_ic_20d AS ind_rank_ic_20d,
                            CASE WHEN m.rank_ic_1d IS NULL AND m.rank_ic_5d IS NULL
                                      AND m.rank_ic_10d IS NULL AND m.rank_ic_20d IS NULL
                                 THEN NULL
                                 ELSE GREATEST(COALESCE(ABS(m.rank_ic_1d), 0),
                                               COALESCE(ABS(m.rank_ic_5d), 0),
                                               COALESCE(ABS(m.rank_ic_10d), 0),
                                               COALESCE(ABS(m.rank_ic_20d), 0))
                            END AS ind_rank_ic_best_abs,
                            m.calculated_at AS ind_calculated_at,
                            m1m.rank_ic_mean AS ind_rank_ic_1m,
                           cl.category, cl.classification_reason, cl.factor_dimension,
                           cl.description AS cl_description, cl.id AS classification_id,
                           cl.ts_info_density, cl.cross_horizon_consistency,
                           m.direction, cl.signal_mechanism, cl.sector_exposure_corr,
                           CASE
                               WHEN m.best_horizon IS NULL THEN NULL
                               WHEN m.best_horizon <= 5 THEN 'short'
                               WHEN m.best_horizon <= 10 THEN 'medium'
                               ELSE 'long'
                           END AS horizon_class,
                           m.best_horizon, m.best_horizon_advantage,
                           cl.linearity, cl.holding_period_class, cl.data_source_group,
                           cl.update_freq,
                           mic.sign_consistency_12m AS ic_sign_consistency_12m,
                           mic.oos_is_ratio AS ic_oos_is_ratio,
                           mic.trend_slope_12m AS monthly_ic_trend_slope,
                           cl.cluster_id, cl.cluster_role, cl.cluster_size,
                           cl.intra_cluster_max_corr, cl.representative_score,
                           {rating_select_sql},
                           fr.llm_audit_summary AS official_llm_audit_summary,
                           fr.llm_risk_notes AS official_llm_risk_notes,
                           cl.grade AS legacy_grade, cl.grade_reason AS legacy_grade_reason
                    FROM aistock_factor_catalog c
                    LEFT JOIN LATERAL (
                        SELECT ic_mean, top_excess_sharpe, top_excess_annual_return,
                               rank_ic_mean, icir, calculated_at,
                               rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
                               direction, best_horizon, best_horizon_advantage
                        FROM aistock_factor_metrics
                        WHERE factor_name = c.factor_name AND eval_window = 'full'
                          AND calc_engine = 'qe_eval_v2'
                        ORDER BY calculated_at DESC
                        LIMIT 1
                    ) m ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT rank_ic_mean
                        FROM aistock_factor_metrics
                        WHERE factor_name = c.factor_name AND eval_window = 'recent_1m'
                          AND calc_engine = 'qe_eval_v2'
                        ORDER BY calculated_at DESC
                        LIMIT 1
                    ) m1m ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT sign_consistency_12m, trend_slope_12m, oos_is_ratio
                        FROM aistock_factor_monthly_ic
                        WHERE factor_name = c.factor_name
                        ORDER BY month_end DESC
                        LIMIT 1
                    ) mic ON TRUE
                    LEFT JOIN qe_factor_classification cl
                        ON cl.factor_name = c.factor_name AND cl.factor_source = c.source
                    {rating_join_sql}
                    WHERE {where_clause}{cl_where}
                    ORDER BY {order_clause}
                    LIMIT %s OFFSET %s
                """, params + cl_params + [query_limit, query_offset])
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 保留 TASK 原始指标（ic/sharpe/annualized_return）和独立指标（ind_*）同时返回
        # 前端自行决定展示优先级，不在后端覆盖

        # 合并 QE 回测因子值缓存信息。
        def _covers_target_range(row: Dict[str, Any]) -> bool:
            if not row.get("has_cache") or row.get("cache_hash_match") is False:
                return False
            covered, _ = factor_cache_covers_window(
                cache_start_date=row.get("cache_start_date"),
                cache_end_date=row.get("cache_end_date"),
                target_start=cache_start_date,
                target_end=cache_end_date,
                entry={
                    "window_train_start": row.get("cache_window_train_start"),
                    "window_backtest_end": row.get("cache_window_backtest_end"),
                },
            )
            return covered

        try:
            # 先标记缓存文件与元数据；hash 校验稍后补充。
            for row in rows:
                fn = row.get("factor_name")
                selected_cache = _choose_best_factor_cache_candidate(
                    _collect_factor_cache_candidates(str(fn)),
                    cache_start_date,
                    cache_end_date,
                )
                if selected_cache and selected_cache.get("valid_cache"):
                    entry = selected_cache.get("entry") or {}
                    row["has_cache"] = True
                    row["cache_date_range"] = selected_cache.get("cache_date_range") or ""
                    row["cache_start_date"] = selected_cache.get("cache_start_date")
                    row["cache_end_date"] = selected_cache.get("cache_end_date")
                    row["cache_computed_at"] = selected_cache.get("cache_computed_at")
                    row["cache_as_of_date"] = selected_cache.get("cache_as_of_date")
                    row["cache_window_train_start"] = selected_cache.get("cache_window_train_start")
                    row["cache_window_backtest_end"] = selected_cache.get("cache_window_backtest_end")
                    row["cache_data_source_mode"] = selected_cache.get("cache_data_source_mode")
                    row["cache_size_mb"] = selected_cache.get("cache_size_mb")
                    row["cache_source"] = selected_cache.get("source_key")
                    row["cache_source_label"] = selected_cache.get("source_label")
                    row["cache_status"] = "ok"
                    row["_cache_meta_entry"] = entry
                else:
                    entry = (selected_cache or {}).get("entry") or {}
                    row["has_cache"] = False
                    row["cache_date_range"] = None
                    row["cache_start_date"] = None
                    row["cache_end_date"] = None
                    row["cache_computed_at"] = (selected_cache or {}).get("cache_computed_at") or entry.get("computed_at")
                    row["cache_as_of_date"] = (selected_cache or {}).get("cache_as_of_date") or entry.get("as_of_date")
                    row["cache_window_train_start"] = (selected_cache or {}).get("cache_window_train_start") or entry.get("window_train_start")
                    row["cache_window_backtest_end"] = (selected_cache or {}).get("cache_window_backtest_end") or entry.get("window_backtest_end")
                    row["cache_data_source_mode"] = (selected_cache or {}).get("cache_data_source_mode") or entry.get("data_source_mode")
                    row["cache_size_mb"] = None
                    row["cache_hash_match"] = None
                    row["cache_source"] = (selected_cache or {}).get("source_key")
                    row["cache_source_label"] = (selected_cache or {}).get("source_label")
                    row["cache_status"] = (selected_cache or {}).get("cache_status") or ("error" if entry.get("status") == "error" else "no_cache")

            # hash 校验（优先 DB code_text 与缓存写入一致；失败不影响 has_cache）
            try:
                _factor_names = [r["factor_name"] for r in rows if r.get("has_cache")]
                _code_hashes = _get_current_factor_code_hashes(_factor_names)
                for row in rows:
                    if not row.get("has_cache"):
                        continue
                    fn = row["factor_name"]
                    entry = row.get("_cache_meta_entry") or {}
                    cached_hash = entry.get("source_hash_raw") or entry.get("source_hash")
                    current_hash = _code_hashes.get(fn)
                    row["cache_hash_match"] = (cached_hash == current_hash) if cached_hash and current_hash else None
                    if row["cache_hash_match"] is False:
                        row["cache_status"] = "hash_mismatch"
            except Exception as e:
                logger.warning(f"缓存 hash 校验失败（不影响 has_cache）: {e}")

            for row in rows:
                if _covers_target_range(row):
                    row["cache_coverage_status"] = "covered"
                elif row.get("has_cache"):
                    row["cache_coverage_status"] = "hash_mismatch" if row.get("cache_hash_match") is False else "partial"
                else:
                    row["cache_coverage_status"] = row.get("cache_status") or "no_cache"
                row.pop("_cache_meta_entry", None)
        except Exception as e:
            logger.warning(f"合并缓存信息失败: {e}")

        if cache_post_process:
            if cache_filter_norm == "has_cache":
                rows = [r for r in rows if r.get("has_cache")]
            elif cache_filter_norm == "no_cache":
                rows = [r for r in rows if not r.get("has_cache")]
            elif cache_filter_norm == "covers_range":
                rows = [r for r in rows if r.get("cache_coverage_status") == "covered"]
            elif cache_filter_norm == "missing_range":
                rows = [r for r in rows if r.get("cache_coverage_status") != "covered"]
            elif cache_filter_norm == "hash_mismatch":
                rows = [r for r in rows if r.get("cache_hash_match") is False]

            if sort_field in cache_sort_fields:
                status_score = {"no_cache": 0, "error": 0, "hash_mismatch": 1, "partial": 2, "covered": 3, "ok": 3}

                def _cache_sort_value(row: Dict[str, Any]) -> Any:
                    if sort_field == "cache_status":
                        return status_score.get(str(row.get("cache_coverage_status") or row.get("cache_status") or "no_cache"), 0)
                    if sort_field == "cache_size_mb":
                        return row.get("cache_size_mb")
                    return row.get(sort_field)

                def _cache_sort_key(row: Dict[str, Any]) -> Tuple[Any, Any]:
                    value = _cache_sort_value(row)
                    if sort_order == "desc":
                        return (value is not None, value or "")
                    return (value is None, value or "")

                rows.sort(key=_cache_sort_key, reverse=(sort_order == "desc"))

            total = len(rows)
            rows = rows[offset: offset + limit]

        return {"ok": True, "total": total, "items": rows}
    except Exception as e:
        logger.exception("获取因子列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rating/rules")
def list_rating_rules():
    """获取可用评级规则版本列表。"""
    try:
        from ..services.quantevolver.factor_rating_service import factor_rating_service
        return {"ok": True, **factor_rating_service.list_rule_versions()}
    except Exception as e:
        logger.exception("获取评级规则列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rating/rules/{version}")
def get_rating_rule(version: str):
    """获取指定评级规则版本详情与说明。"""
    try:
        from ..services.quantevolver.factor_rating_service import factor_rating_service
        return {"ok": True, **factor_rating_service.get_rule_detail(version)}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("获取评级规则详情失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rating/rules/activate")
def activate_rating_rule(version: str = Body(..., embed=True)):
    """切换当前激活的评级规则版本。"""
    try:
        from ..services.quantevolver.factor_rating_service import factor_rating_service
        return factor_rating_service.activate_rule_version(version)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("激活评级规则失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rating/run")
def run_factor_rating(req: FactorRatingRunRequest):
    """执行正式因子评级。"""
    try:
        from ..services.quantevolver.factor_rating_service import factor_rating_service
        rules = factor_rating_service.list_rule_versions()
        rule_version = req.rule_version or rules.get("active_version") or rules.get("default_version")
        if not rule_version:
            raise HTTPException(status_code=400, detail="当前无可用评级规则版本")
        scope_payload = {
            "selected_factors": req.selected_factors or [],
            "filters": req.filters or {},
        }
        return factor_rating_service.run_rating(
            rule_version=rule_version,
            scope_type=req.scope_type,
            scope_payload=scope_payload,
            triggered_from=req.triggered_from,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("执行正式因子评级失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rating/runs")
def list_rating_runs(limit: int = Query(20, ge=1, le=100)):
    """获取最近评级执行记录。"""
    try:
        from ..services.quantevolver.factor_rating_service import factor_rating_service
        return {"ok": True, "items": factor_rating_service.list_runs(limit=limit)}
    except Exception as e:
        logger.exception("获取评级运行记录失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rating/runs/{run_id}")
def get_rating_run(run_id: str):
    """获取单次评级执行记录。"""
    try:
        from ..services.quantevolver.factor_rating_service import factor_rating_service
        item = factor_rating_service.get_run(run_id)
        if not item:
            raise HTTPException(status_code=404, detail="评级运行记录不存在")
        return {"ok": True, **item}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取评级运行详情失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rating/results")
def list_rating_results(
    rule_version: Optional[str] = Query(None, description="指定规则版本，不传则使用当前激活版本"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取正式评级结果列表。"""
    try:
        from ..services.quantevolver.factor_rating_service import factor_rating_service
        return {"ok": True, **factor_rating_service.list_results(rule_version=rule_version, limit=limit, offset=offset)}
    except Exception as e:
        logger.exception("获取正式评级结果失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/factors", summary="删除因子及所有关联数据（级联清理）")
def delete_factor(
    factor_name: str = Query(..., description="因子名称"),
    source: str = Query(..., description="因子来源"),
):
    """
    删除指定因子及其在所有表中的关联数据。
    按 FK RESTRICT 约束顺序先删子表再删主表，整体在单事务内执行。
    """
    from ..db.pg_pool import get_conn

    with get_conn() as conn:
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # 0. 查找 catalog id
                cur.execute(
                    "SELECT id FROM aistock_factor_catalog WHERE factor_name = %s AND source = %s",
                    (factor_name, source),
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    raise HTTPException(status_code=404, detail=f"因子 {factor_name} (source={source}) 不存在")
                catalog_id = row[0]

                deleted_counts = {}

                # 1. aistock_factor_calc_log
                cur.execute("DELETE FROM aistock_factor_calc_log WHERE factor_name = %s", (factor_name,))
                deleted_counts["aistock_factor_calc_log"] = cur.rowcount

                # 2. aistock_factor_metrics
                cur.execute("DELETE FROM aistock_factor_metrics WHERE factor_name = %s", (factor_name,))
                deleted_counts["aistock_factor_metrics"] = cur.rowcount

                # 2b. 月频 IC 衰退趋势
                cur.execute("DELETE FROM aistock_factor_monthly_ic WHERE factor_name = %s", (factor_name,))
                deleted_counts["aistock_factor_monthly_ic"] = cur.rowcount

                # 3. factor_live_track
                cur.execute("DELETE FROM factor_live_track WHERE factor_catalog_id = %s", (catalog_id,))
                deleted_counts["factor_live_track"] = cur.rowcount

                # 4. qe_factor_classification
                cur.execute(
                    "DELETE FROM qe_factor_classification WHERE factor_name = %s AND factor_source = %s",
                    (factor_name, source),
                )
                deleted_counts["qe_factor_classification"] = cur.rowcount

                # 4.1 qe_factor_official_ratings（正式评级）
                cur.execute(
                    "DELETE FROM qe_factor_official_ratings WHERE factor_catalog_id = %s",
                    (catalog_id,),
                )
                deleted_counts["qe_factor_official_ratings"] = cur.rowcount

                # 5. qe_factor_experiment_metrics
                cur.execute(
                    "DELETE FROM qe_factor_experiment_metrics WHERE factor_name = %s AND factor_source = %s",
                    (factor_name, source),
                )
                deleted_counts["qe_factor_experiment_metrics"] = cur.rowcount

                # 6. qe_loop_factor_records
                cur.execute(
                    "DELETE FROM qe_loop_factor_records WHERE factor_name = %s AND factor_source = %s",
                    (factor_name, source),
                )
                deleted_counts["qe_loop_factor_records"] = cur.rowcount

                # 7. qe_factor_correlations — ON DELETE CASCADE 会自动清理
                # 先统计将被级联删除的记录数
                cur.execute("""
                    SELECT COUNT(*) FROM qe_factor_correlations c
                    JOIN aistock_factor_catalog cat ON cat.id = c.factor_a_id OR cat.id = c.factor_b_id
                    WHERE cat.factor_name = %s AND cat.source = %s
                """, (factor_name, source))
                deleted_counts["qe_factor_correlations"] = cur.fetchone()[0]

                # 8. qe_experiments.factor_names — 从 JSONB 数组移除该因子
                jsonb_contains = json.dumps([factor_name])
                cur.execute("""
                    UPDATE qe_experiments
                    SET factor_names = (
                        SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
                        FROM jsonb_array_elements(factor_names) AS elem
                        WHERE elem #>> '{}' != %s
                    )
                    WHERE factor_names IS NOT NULL AND factor_names @> %s::jsonb
                """, (factor_name, jsonb_contains))
                deleted_counts["qe_experiments_updated"] = cur.rowcount

                # 9. qe_loop_model_records.factor_list — 从 JSONB 数组移除
                cur.execute("""
                    UPDATE qe_loop_model_records
                    SET factor_list = (
                        SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
                        FROM jsonb_array_elements(factor_list) AS elem
                        WHERE elem #>> '{}' != %s
                    )
                    WHERE factor_list IS NOT NULL AND factor_list @> %s::jsonb
                """, (factor_name, jsonb_contains))
                deleted_counts["qe_loop_model_records_updated"] = cur.rowcount

                # 10. aistock_loop_catalog.factor_names — 从 JSONB 数组移除
                cur.execute("""
                    UPDATE aistock_loop_catalog
                    SET factor_names = (
                        SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
                        FROM jsonb_array_elements(factor_names) AS elem
                        WHERE elem #>> '{}' != %s
                    )
                    WHERE factor_names IS NOT NULL AND factor_names @> %s::jsonb
                """, (factor_name, jsonb_contains))
                deleted_counts["aistock_loop_catalog_updated"] = cur.rowcount

                # 11. qe_factor_correlations_backup — FK ON DELETE RESTRICT，必须手动清理
                cur.execute("""
                    DELETE FROM qe_factor_correlations_backup
                    WHERE factor_a_catalog_id = %s OR factor_b_catalog_id = %s
                """, (catalog_id, catalog_id))
                deleted_counts["qe_factor_correlations_backup"] = cur.rowcount

                # 12. 删除主表记录
                cur.execute(
                    "DELETE FROM aistock_factor_catalog WHERE id = %s",
                    (catalog_id,),
                )
                deleted_counts["aistock_factor_catalog"] = cur.rowcount

            conn.commit()
            logger.info(f"因子删除成功: {factor_name} (source={source}), counts={deleted_counts}")

            # 13. 清理缓存文件（DB 事务已提交，文件清理在事务外执行）
            cleaned_files = []
            try:
                _project_root = os.path.normpath(
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
                )
                from ..services.quantevolver.factor_value_loader import FactorValueLoader

                # Clean official cache entries for metrics, correlation, and QE backtests.
                for _cache_subdir in ("factor_values",):
                    _cache_root = os.path.join(_project_root, "rdagent_assets", _cache_subdir)

                    # 单因子 parquet 缓存
                    single_parquet = os.path.join(_cache_root, "single", f"{factor_name}.parquet")
                    if os.path.isfile(single_parquet):
                        os.remove(single_parquet)
                        cleaned_files.append(single_parquet)
                        logger.info(f"已删除因子缓存: {single_parquet}")

                    # 合并面板缓存（删除因子后缓存含已删除因子列，必须失效）
                    if os.path.isdir(_cache_root):
                        FactorValueLoader.invalidate_merged_cache(_cache_root)
                        cleaned_files.append(f"{_cache_subdir}/_merged_panel.parquet")

                    # _meta.json 清理：移除已删除因子的条目
                    meta_path = os.path.join(_cache_root, "_meta.json")
                    if os.path.isfile(meta_path):
                        try:
                            import json as _json
                            with open(meta_path, "r", encoding="utf-8") as _mf:
                                _meta = _json.load(_mf)
                            if factor_name in _meta.get("factors", {}):
                                del _meta["factors"][factor_name]
                                _meta["factor_count"] = len(_meta.get("factors", {}))
                                _tmp = meta_path + ".tmp"
                                with open(_tmp, "w", encoding="utf-8") as _mf:
                                    _json.dump(_meta, _mf, ensure_ascii=False, indent=2)
                                os.replace(_tmp, meta_path)
                                cleaned_files.append(f"{_cache_subdir}/_meta.json")
                        except Exception as e:
                            logger.warning(f"清理 {meta_path} 失败 (不影响删除结果): {e}")

                # Remove generated QE factor source file.
                qe_code = os.path.join(
                    _project_root, "rdagent_assets", "qe_factors",
                    f"{factor_name}.py",
                )
                if os.path.isfile(qe_code):
                    os.remove(qe_code)
                    cleaned_files.append(qe_code)
                    logger.info(f"已删除QE因子代码: {qe_code}")

                _invalidate_cache_meta()
            except Exception as e:
                logger.warning(f"清理因子缓存文件时出错 (不影响删除结果): {e}")

            deleted_counts["cleaned_files"] = len(cleaned_files)
            return {"ok": True, "factor_name": factor_name, "source": source, "deleted_counts": deleted_counts}

        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            logger.exception(f"删除因子失败: factor_name={factor_name}, source={source}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            conn.autocommit = old_autocommit


class FactorAvailabilityRequest(BaseModel):
    source: str = Field(..., description="因子来源")
    is_available: bool = Field(..., description="是否可用")


@router.patch("/factors/{factor_name}/availability", summary="设置因子可用状态")
def set_factor_availability(factor_name: str, req: FactorAvailabilityRequest):
    """设置因子为可用/不可用（软删除）。is_available=false 不参与 SOTA 保护和新实验，可恢复。

    重新启用时清除 correlation_computed_at，下次全量计算会包含该因子。
    设为不可用时清理 qe_factor_correlations 中的旧相关性记录。
    """
    from ..db.pg_pool import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            if req.is_available:
                cur.execute(
                    "UPDATE aistock_factor_catalog "
                    "SET is_available = TRUE, correlation_computed_at = NULL, updated_at = NOW() "
                    "WHERE factor_name = %s AND source = %s",
                    (factor_name, req.source),
                )
            else:
                # 获取因子 ID，清理相关性记录
                cur.execute(
                    "SELECT id FROM aistock_factor_catalog "
                    "WHERE factor_name = %s AND source = %s",
                    (factor_name, req.source),
                )
                row = cur.fetchone()
                if row:
                    fid = row[0]
                    cur.execute(
                        "DELETE FROM qe_factor_correlations "
                        "WHERE factor_a_id = %s OR factor_b_id = %s",
                        (fid, fid),
                    )
                cur.execute(
                    "UPDATE aistock_factor_catalog "
                    "SET is_available = FALSE, correlation_computed_at = NULL, "
                    "correlation_pair_count = 0, updated_at = NOW() "
                    "WHERE factor_name = %s AND source = %s",
                    (factor_name, req.source),
                )
            if cur.rowcount == 0:
                raise HTTPException(404, f"因子 {factor_name} (source={req.source}) 不存在")
        conn.commit()
    return {"ok": True, "factor_name": factor_name, "source": req.source, "is_available": req.is_available}


class BatchFactorActionRequest(BaseModel):
    action: str = Field(..., description="操作类型: delete / set_unavailable")
    factors: List[Dict[str, str]] = Field(
        ..., description='因子列表 [{"factor_name": "xxx", "source": "yyy"}, ...]'
    )


class CleanupPreviewRequest(BaseModel):
    rules: Optional[List[str]] = Field(
        None,
        description="启用的规则: near_identical / pure_noise_v2 / reverse_redundant; 默认全部",
    )
    thresholds: Optional[Dict[str, float]] = Field(
        None, description="覆盖默认阈值 (ic_th, rank_ic_th, pos_ratio_lo/hi, rank_icir_th, neg_corr_th)"
    )


class CleanupExecuteRequest(BaseModel):
    factor_ids: List[int] = Field(..., description="要禁用的 factor_catalog id 列表")
    reasons: Dict[str, str] = Field(
        ...,
        description='每个 id 的 disable_reason {"123": "v2_cleanup:pure_noise_v2", ...}',
    )
    batch_id: Optional[str] = Field(None, description="批次号; 留空自动生成")


class ManualFactorCreate(BaseModel):
    factor_name: str = Field(..., description="因子名（m_ 开头，英文+下划线）")
    code_text: str = Field(..., description="因子 Python 代码")
    description: Optional[str] = Field(None, description="因子描述")
    expression: Optional[str] = Field(None, description="因子表达式（可选）")
    data_date: Optional[str] = Field(None, description="兼容字段：旧快照日期；官方离线链路将其解释为 end_date")
    start_date: Optional[str] = Field(None, description="官方离线训练/回测起始日期")
    end_date: Optional[str] = Field(None, description="官方离线训练/回测截止日期")


class ManualFactorValidate(BaseModel):
    factor_name: str = Field(..., description="因子名")
    code_text: str = Field(..., description="因子 Python 代码")


class BatchComputeMetricsUnified(BaseModel):
    factor_names: Optional[List[str]] = Field(None, description="指定因子名列表")
    all_available: bool = Field(True, description="True=全部可用因子（含 disabled）；legacy 接口会转发到 official full-compute")
    data_date: Optional[str] = Field(None, description="兼容字段：旧快照日期；官方离线链路将其解释为 end_date")
    start_date: Optional[str] = Field(None, description="官方离线训练/回测起始日期")
    end_date: Optional[str] = Field(None, description="官方离线训练/回测截止日期")


class OfficialEvaluationComputeRequest(BaseModel):
    factor_names: Optional[List[str]] = Field(None, description="指定因子名列表；为空时计算全部符合 official 准入规则的因子")
    data_date: Optional[str] = Field(None, description="兼容字段：旧快照日期；官方离线链路将其解释为 end_date")
    start_date: str = Field("2018-08-01", description="官方离线训练/回测起始日期")
    end_date: Optional[str] = Field(None, description="官方离线训练/回测截止日期；空时使用 data_date 或默认 2026-04-30")
    include_disabled: bool = Field(True, description="是否包含 is_available=false 的因子；默认 True 以支持 disabled 因子指标计算")
    max_workers: int = Field(4, ge=1, le=16, description="并行 worker 数")
    timeout_per_factor: int = Field(600, ge=60, le=3600, description="单因子超时秒数")
    force: bool = Field(False, description="强制重新生成 official cache 并计算指标")


class DeletionAnalyzeRequest(BaseModel):
    thresholds: Optional[Dict[str, float]] = Field(
        None,
        description="可选阈值覆盖。未指定的项使用 DeletionCandidateService.DEFAULT_THRESHOLDS。",
    )


class UnifiedPipelineRequest(BaseModel):
    """一键流水线：分类 + 评级 + (可选) LLM 分析/审阅。"""
    scope_type: str = Field(..., description="范围: selected / filter / all")
    selected_factors: Optional[List[Dict[str, str]]] = Field(
        None, description="scope_type=selected 时使用；元素包含 factor_name/source")
    filters: Optional[Dict[str, Any]] = Field(
        None, description="scope_type=filter 时使用；与 /factors 查询同结构")
    parallelism: int = Field(4, ge=1, le=16, description="并行度, 默认 4")
    enable_llm_analysis: bool = Field(True, description="Step A 是否用 LLM 做分类（否则走规则）")
    enable_llm_audit: bool = Field(True, description="Step B 是否跑 LLM 评级审阅")
    rule_version: Optional[str] = Field(None, description="评级规则版本；缺省使用当前激活版本")

    model_config = ConfigDict(extra="forbid")


@router.post(
    "/pipeline/full-stream",
    summary="一键流水线：分类+评级+LLM 分析/审阅（SSE 流式）",
)
async def pipeline_full_stream(req: UnifiedPipelineRequest):
    """单次调用完成所有因子的分类与评级, 流式返回进度。

    事件:
      start        — 总数 / 规则版本 / 并行度
      factor_start — 因子开始
      factor_step  — Step A/B 阶段事件 (phase=start|done|error)
      factor_done  — 单因子完成
      progress     — 计数更新 (done/total/ok/failed)
      error        — 顶层错误
      done         — 全部完成 (含汇总)
    """
    from ..services.quantevolver.unified_factor_pipeline import (
        PipelineRequest, stream_pipeline,
    )

    pr = PipelineRequest(
        scope_type=req.scope_type,
        selected_factors=req.selected_factors,
        filters=req.filters,
        parallelism=req.parallelism,
        enable_llm_analysis=req.enable_llm_analysis,
        enable_llm_audit=req.enable_llm_audit,
        rule_version=req.rule_version,
    )

    return StreamingResponse(
        stream_pipeline(pr),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post("/deletion/analyze", summary="因子删除候选分析（精确孪生/纯噪声/模糊孪生 + 5条免疫规则）")
def deletion_analyze(req: DeletionAnalyzeRequest = Body(default=DeletionAnalyzeRequest())):
    """只读分析，不执行任何删除。

    返回三类候选清单（exact_twins / pure_noise / fuzzy_twins）供 UI 预览。
    用户勾选后走 /quantevolver/factors/batch-action action=delete 执行。
    """
    try:
        from ..services.quantevolver.deletion_candidate_service import deletion_candidate_service
        result = deletion_candidate_service.analyze(thresholds=req.thresholds)
        return {"ok": True, **result}
    except Exception as e:
        logger.exception("删除候选分析失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deletion/thresholds", summary="获取默认阈值配置")
def deletion_get_thresholds():
    """返回 DeletionCandidateService 的默认阈值，供 UI 初始化表单。"""
    try:
        from ..services.quantevolver.deletion_candidate_service import DeletionCandidateService
        return {"ok": True, "thresholds": DeletionCandidateService.DEFAULT_THRESHOLDS}
    except Exception as e:
        logger.exception("获取默认阈值失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factors/batch-action", summary="批量因子操作（删除/设不可用）")
def batch_factor_action(req: BatchFactorActionRequest):
    """批量对因子执行删除或设不可用操作。

    - set_unavailable: 单事务批量 UPDATE
    - delete: 逐个执行级联删除（每个因子独立事务），部分失败不影响其他因子
    """
    if req.action not in ("delete", "set_unavailable"):
        raise HTTPException(400, f"不支持的操作: {req.action}，仅支持 delete / set_unavailable")
    if not req.factors:
        raise HTTPException(400, "factors 列表不能为空")

    succeeded = []
    failed = []

    if req.action == "set_unavailable":
        with get_conn() as conn:
            with conn.cursor() as cur:
                for item in req.factors:
                    fn = item.get("factor_name", "")
                    src = item.get("source", "")
                    if not fn or not src:
                        failed.append({"factor_name": fn, "error": "缺少 factor_name 或 source"})
                        continue
                    # 获取因子 ID，清理相关性记录
                    cur.execute(
                        "SELECT id FROM aistock_factor_catalog "
                        "WHERE factor_name = %s AND source = %s",
                        (fn, src),
                    )
                    fid_row = cur.fetchone()
                    if fid_row:
                        cur.execute(
                            "DELETE FROM qe_factor_correlations "
                            "WHERE factor_a_id = %s OR factor_b_id = %s",
                            (fid_row[0], fid_row[0]),
                        )
                    cur.execute(
                        "UPDATE aistock_factor_catalog "
                        "SET is_available = FALSE, correlation_computed_at = NULL, "
                        "correlation_pair_count = 0, updated_at = NOW() "
                        "WHERE factor_name = %s AND source = %s",
                        (fn, src),
                    )
                    if cur.rowcount > 0:
                        succeeded.append({"factor_name": fn, "source": src})
                    else:
                        failed.append({"factor_name": fn, "error": f"因子不存在 (source={src})"})

                # 刷新幸存因子的 correlation_pair_count: 与被禁用因子配对的 pair 行已 DELETE,
                # 但其它因子的 pair_count 仍停留在旧值, FactorList 徽章会偏高直到下次相关性重算.
                if succeeded:
                    try:
                        cur.execute(
                            """
                            UPDATE aistock_factor_catalog c SET
                                correlation_pair_count = COALESCE(sub.cnt, 0)
                            FROM (
                                SELECT factor_id, COUNT(*) AS cnt FROM (
                                    SELECT factor_a_id AS factor_id FROM qe_factor_correlations
                                    UNION ALL
                                    SELECT factor_b_id AS factor_id FROM qe_factor_correlations
                                ) t GROUP BY factor_id
                            ) sub
                            WHERE c.id = sub.factor_id
                            """
                        )
                        cur.execute(
                            """
                            UPDATE aistock_factor_catalog
                            SET correlation_pair_count = 0
                            WHERE correlation_pair_count > 0
                              AND id NOT IN (
                                  SELECT factor_a_id FROM qe_factor_correlations
                                  UNION
                                  SELECT factor_b_id FROM qe_factor_correlations
                              )
                            """
                        )
                    except Exception as exc:
                        logger.exception("batch-action pair_count refresh failed: %s", exc)
            conn.commit()
    else:
        # delete: 逐个调用已有 delete_factor 逻辑（每个独立事务）
        for item in req.factors:
            fn = item.get("factor_name", "")
            src = item.get("source", "")
            if not fn or not src:
                failed.append({"factor_name": fn, "error": "缺少 factor_name 或 source"})
                continue
            try:
                delete_factor(factor_name=fn, source=src)
                succeeded.append({"factor_name": fn, "source": src})
            except HTTPException as he:
                failed.append({"factor_name": fn, "error": he.detail})
            except Exception as e:
                failed.append({"factor_name": fn, "error": str(e)})

    return {
        "ok": len(failed) == 0,
        "action": req.action,
        "succeeded": succeeded,
        "failed": failed,
        "succeeded_count": len(succeeded),
        "failed_count": len(failed),
    }


@router.post("/factors/cleanup/preview", summary="因子清洗预览 (dry-run, 不写库)")
def factor_cleanup_preview(req: CleanupPreviewRequest = Body(default=CleanupPreviewRequest())):
    """三规则清理预览, 返回候选清单 (列与 /factors 对齐).

    Rules:
      - near_identical    : cluster_role='member' (complete-linkage 0.999)
      - pure_noise_v2     : grade=D + |ic|<0.003 + |rank_ic|<0.003 + pos∈[0.45,0.55] + |rank_icir|<0.1
      - reverse_redundant : corr ≤ -0.999, 留正 IC / |IC| 大者

    Response:
        {ok, summary, candidates, reverse_pairs}
    """
    try:
        from ..services.quantevolver.factor_cleanup_service import factor_cleanup_service
        result = factor_cleanup_service.preview(
            rules=req.rules,
            thresholds=req.thresholds,
        )
        return {"ok": True, **result}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.exception("因子清洗预览失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factors/cleanup/execute", summary="因子清洗执行 (写库, 必须先调 preview)")
def factor_cleanup_execute(req: CleanupExecuteRequest):
    """正式禁用 preview 返回的候选 (前端确认后调).

    Body:
        factor_ids: [123, 456, ...]
        reasons:    {"123": "v2_cleanup:pure_noise_v2", ...}
        batch_id:   可选, 留空自动生成 v2_cleanup_YYYYmmdd_HHMMSS

    Response:
        {ok, batch_id, disabled_count, by_reason, errors, rollback_sql}
    """
    try:
        from ..services.quantevolver.factor_cleanup_service import factor_cleanup_service
        # JSON dict key 是 string, 转回 int
        reasons_int = {int(k): v for k, v in req.reasons.items()}
        result = factor_cleanup_service.execute(
            factor_ids=req.factor_ids,
            reasons=reasons_int,
            batch_id=req.batch_id,
        )
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.exception("因子清洗执行失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors/cleanup/batches", summary="最近的清洗批次")
def factor_cleanup_batches(limit: int = Query(20, ge=1, le=200)):
    try:
        from ..services.quantevolver.factor_cleanup_service import factor_cleanup_service
        batches = factor_cleanup_service.list_recent_batches(limit=limit)
        return {"ok": True, "batches": batches}
    except Exception as e:
        logger.exception("查询清洗批次失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factors/cleanup/rollback", summary="回滚指定清洗批次 (重新启用)")
def factor_cleanup_rollback(batch_id: str = Body(..., embed=True)):
    try:
        from ..services.quantevolver.factor_cleanup_service import factor_cleanup_service
        result = factor_cleanup_service.rollback_batch(batch_id=batch_id)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.exception("清洗批次回滚失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models")
def list_models(
    search: Optional[str] = Query(None),
    model_type: Optional[str] = Query(None, description="过滤模型类型"),
    grade: Optional[str] = Query(None, description="过滤评级: S/A/B/C/D"),
    sota_only: bool = Query(False),
    sort_field: Optional[str] = Query(None, description="排序字段"),
    sort_order: Optional[str] = Query("desc", description="排序方向: asc/desc"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """获取全部模型列表（含训练诊断、分析、代码等完整字段）。"""
    try:
        from ..db.pg_pool import get_conn

        conditions = []
        params = []

        if search:
            conditions.append("(model_name ILIKE %s OR model_type ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        if model_type:
            conditions.append("model_type = %s")
            params.append(model_type)
        if grade:
            conditions.append("model_grade = %s")
            params.append(grade)
        if sota_only:
            conditions.append("is_sota = TRUE")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # 排序白名单
        allowed_sort = {
            "ic", "annualized_return", "max_drawdown", "information_ratio",
            "model_grade", "training_quality_score", "loop_id", "model_name",
            "convergence_ratio", "overfit_ratio",
        }
        if sort_field and sort_field in allowed_sort:
            direction = "ASC" if sort_order == "asc" else "DESC"
            order_clause = f"{sort_field} {direction} NULLS LAST"
        else:
            order_clause = "is_sota DESC NULLS LAST, ic DESC NULLS LAST"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM aistock_model_catalog WHERE {where_clause}",
                    params,
                )
                total = cur.fetchone()[0]

                cur.execute(f"""
                    SELECT model_id, model_name, model_type, display_name,
                           catalog_source,
                           ic, annualized_return, max_drawdown, information_ratio,
                           sharpe, all_metrics,
                           is_sota, task_run_id, loop_id,
                           hypothesis_text, model_architecture, model_formulation,
                           description, generated_at_utc,
                           -- 训练诊断
                           best_epoch, total_epochs, convergence_ratio, overfit_ratio,
                           training_failed, train_loss_final, val_loss_final,
                           training_curves,
                           -- 分析
                           model_grade, grade_reason, training_quality_score,
                           analysis_profile,
                           -- 代码
                           code_text, source_code_relpath,
                           -- 反馈
                           feedback_observations, feedback_evaluation,
                           feedback_reason, feedback_decision
                    FROM aistock_model_catalog
                    WHERE {where_clause}
                    ORDER BY {order_clause}
                    LIMIT %s OFFSET %s
                """, params + [limit, offset])
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        return {"ok": True, "total": total, "items": rows}
    except Exception as e:
        logger.exception("获取模型列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/models/{model_id}", summary="删除模型及所有关联数据（级联清理）")
def delete_model(model_id: str):
    """
    删除指定模型及其在所有表中的关联数据。
    按 FK RESTRICT 约束顺序先删子表再删主表，整体在单事务内执行。
    qe_experiments 仅清空模型引用（不删除实验记录本身）。
    """
    from ..db.pg_pool import get_conn

    with get_conn() as conn:
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # 0. 查找 catalog id
                cur.execute(
                    "SELECT id FROM aistock_model_catalog WHERE model_id = %s",
                    (model_id,),
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    raise HTTPException(status_code=404, detail=f"模型 {model_id} 不存在")
                catalog_id = row[0]

                deleted_counts = {}

                # 1. qe_loop_model_records
                cur.execute(
                    "DELETE FROM qe_loop_model_records WHERE model_catalog_id = %s",
                    (catalog_id,),
                )
                deleted_counts["qe_loop_model_records"] = cur.rowcount

                # 2. qe_factor_experiment_metrics
                cur.execute(
                    "DELETE FROM qe_factor_experiment_metrics WHERE model_catalog_id = %s",
                    (catalog_id,),
                )
                deleted_counts["qe_factor_experiment_metrics"] = cur.rowcount

                # 3. qe_loop_factor_records
                cur.execute(
                    "DELETE FROM qe_loop_factor_records WHERE model_catalog_id = %s",
                    (catalog_id,),
                )
                deleted_counts["qe_loop_factor_records"] = cur.rowcount

                # 4. qe_experiments — 清空模型引用，保留实验记录
                cur.execute(
                    "UPDATE qe_experiments SET model_catalog_id = NULL, model_id = NULL WHERE model_catalog_id = %s",
                    (catalog_id,),
                )
                deleted_counts["qe_experiments_updated"] = cur.rowcount

                # 5. 删除主表记录
                cur.execute(
                    "DELETE FROM aistock_model_catalog WHERE id = %s",
                    (catalog_id,),
                )
                deleted_counts["aistock_model_catalog"] = cur.rowcount

            conn.commit()
            logger.info(f"模型删除成功: {model_id}, counts={deleted_counts}")
            return {"ok": True, "model_id": model_id, "deleted_counts": deleted_counts}

        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            logger.exception(f"删除模型失败: model_id={model_id}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            conn.autocommit = old_autocommit


# ── Multi-Alpha API Endpoints (Phase 3) ────────────────────────────────────


@router.post("/multi-alpha/auto-select", summary="自动因子分组选择")
def multi_alpha_auto_select(
    min_grade: str = Query("C", description="最低评级: S/A/B/C"),
    min_ic: float = Query(0.02, description="best_horizon_IC 最低阈值"),
    max_factors_per_group: int = Query(15, description="每组最多因子数"),
    max_intra_corr: float = Query(0.7, description="组内最大相关系数"),
    meta_method: str = Query("ic_weighted", description="Meta-Model方法: ic_weighted/ols/stacking"),
    execution_mode: str = Query("serial", description="执行模式: serial/local_parallel/distributed"),
):
    """自动生成 Multi-Alpha 分组配置。

    基于因子库的评级、数据源分组和相关性，自动选择因子并分组。
    返回可直接用于 GenerateConfigRequest.multi_alpha_config 的 JSON。
    """
    from ..services.quantevolver.multi_alpha_selector import MultiAlphaFactorSelector

    _assert_multi_alpha_execution_mode_supported({"execution_mode": execution_mode})

    try:
        selector = MultiAlphaFactorSelector()
        config = selector.auto_select(
            min_grade=min_grade,
            min_ic=min_ic,
            max_factors_per_group=max_factors_per_group,
            max_intra_corr=max_intra_corr,
            meta_method=meta_method,
            execution_mode=execution_mode,
        )
        preview = selector.preview(config)
        return {
            "ok": True,
            "multi_alpha_config": config.model_dump(),
            "preview": preview,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/multi-alpha/preview", summary="预览Multi-Alpha配置统计")
def multi_alpha_preview(config: Dict[str, Any] = Body(...)):
    """预览 Multi-Alpha 配置的统计信息（因子数/组数/计算资源分配）。"""
    from ..services.quantevolver.multi_alpha_selector import MultiAlphaFactorSelector
    from ..services.quantevolver.experiment_config import MultiAlphaConfig, AlphaGroup, MetaModelConfig

    try:
        groups = [AlphaGroup(**g) for g in config.get("alpha_groups", [])]
        meta_raw = config.get("meta_model") or {}
        meta = MetaModelConfig(**meta_raw) if isinstance(meta_raw, dict) else MetaModelConfig()
        ma_config = MultiAlphaConfig(
            alpha_groups=groups,
            meta_model=meta,
            execution_mode=config.get("execution_mode", "serial"),
        )
        selector = MultiAlphaFactorSelector()
        return {"ok": True, "preview": selector.preview(ma_config)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/multi-alpha/group-templates", summary="获取预定义分组模板")
def multi_alpha_group_templates():
    """返回 Multi-Alpha 因子分组模板列表（data_source_group → model/dataset 推荐）。"""
    from ..services.quantevolver.multi_alpha_selector import MultiAlphaFactorSelector

    selector = MultiAlphaFactorSelector()
    return {"ok": True, "templates": selector.get_group_templates()}


@router.get("/multi-alpha/reusable-groups", summary="查询可复用的已完成Alpha组")
def multi_alpha_reusable_groups(
    group_name: Optional[str] = Query(None, description="按组名过滤"),
    limit: int = Query(20, ge=1, le=100),
):
    """查询已完成的 Multi-Alpha 组，用于模型复用/backtest-only 场景。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT g.parent_experiment_id, g.group_name, g.model_id,
                           g.group_ic, g.group_icir, g.group_sharpe,
                           g.prediction_path, g.completed_at,
                           g.factor_names, g.dataset_type
                    FROM qe_multi_alpha_groups g
                    WHERE g.status = 'completed'
                      AND g.prediction_path IS NOT NULL
                """
                params: list = []
                if group_name:
                    sql += " AND g.group_name = %s"
                    params.append(group_name)
                sql += " ORDER BY g.completed_at DESC LIMIT %s"
                params.append(limit)

                cur.execute(sql, params)
                rows = cur.fetchall()
                items = []
                for r in rows:
                    items.append({
                        "experiment_id": r[0],
                        "group_name": r[1],
                        "model_id": r[2],
                        "group_ic": r[3],
                        "group_icir": r[4],
                        "group_sharpe": r[5],
                        "prediction_path": r[6],
                        "completed_at": r[7].isoformat() if r[7] else None,
                        "factor_names": r[8] if isinstance(r[8], list) else json.loads(r[8]) if r[8] else [],
                        "dataset_type": r[9],
                    })
                return {"ok": True, "items": items}
    except Exception as e:
        logger.exception("查询可复用组失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/multi-alpha/results",
            summary="获取Multi-Alpha实验结果")
def get_multi_alpha_results(experiment_id: str):
    """获取 Multi-Alpha 实验的各组结果和 Meta-Model 权重。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT status, result_metrics
                       FROM qe_experiments
                       WHERE experiment_id = %s""",
                    (experiment_id,),
                )
                exp_row = cur.fetchone()
                if not exp_row:
                    raise HTTPException(status_code=404, detail=f"实验不存在: {experiment_id}")
                exp_status, result_metrics_raw = exp_row

                # 各组结果
                cur.execute("""
                    SELECT group_name, factor_names, model_id, dataset_type,
                           group_ic, group_icir, group_sharpe, meta_weight,
                           assigned_node_id, status, error_message
                    FROM qe_multi_alpha_groups
                    WHERE parent_experiment_id = %s
                    ORDER BY group_name
                """, (experiment_id,))
                cols = [d[0] for d in cur.description]
                groups = [dict(zip(cols, r)) for r in cur.fetchall()]

                # Meta 权重历史
                cur.execute("""
                    SELECT as_of_date, method, weights, combined_ic
                    FROM qe_meta_model_weights
                    WHERE experiment_id = %s
                    ORDER BY as_of_date DESC LIMIT 10
                """, (experiment_id,))
                meta_cols = [d[0] for d in cur.description]
                meta_history = [dict(zip(meta_cols, r)) for r in cur.fetchall()]

        if isinstance(result_metrics_raw, str):
            try:
                result_metrics = json.loads(result_metrics_raw)
            except Exception:
                result_metrics = {}
        elif isinstance(result_metrics_raw, dict):
            result_metrics = result_metrics_raw
        else:
            result_metrics = {}

        lifecycle = result_metrics.get("multi_alpha_lifecycle") if isinstance(result_metrics, dict) else None
        if not isinstance(lifecycle, dict):
            lifecycle = {}
        multi_detail = result_metrics.get("multi_alpha_detail") or {}
        detail_groups = multi_detail.get("group_results") or []
        detail_by_name = {
            g.get("group_name"): g for g in detail_groups if isinstance(g, dict) and g.get("group_name")
        }

        if groups:
            enriched_groups = []
            for g in groups:
                merged = dict(g)
                detail = detail_by_name.get(merged.get("group_name")) or {}
                if detail:
                    if merged.get("group_ic") is None and detail.get("ic") is not None:
                        merged["group_ic"] = detail.get("ic")
                    if merged.get("group_icir") is None and detail.get("icir") is not None:
                        merged["group_icir"] = detail.get("icir")
                    if merged.get("group_sharpe") is None and detail.get("sharpe") is not None:
                        merged["group_sharpe"] = detail.get("sharpe")
                    if merged.get("meta_weight") is None and detail.get("meta_weight") is not None:
                        merged["meta_weight"] = detail.get("meta_weight")
                    if exp_status == "completed" and merged.get("status") in {"pending", "running"}:
                        merged["status"] = "completed"
                enriched_groups.append(merged)
            groups = enriched_groups
        elif detail_groups:
            groups = [
                {
                    "group_name": g.get("group_name"),
                    "factor_names": [],
                    "model_id": g.get("model_id"),
                    "dataset_type": None,
                    "group_ic": g.get("ic"),
                    "group_icir": g.get("icir"),
                    "group_sharpe": g.get("sharpe"),
                    "meta_weight": g.get("meta_weight"),
                    "assigned_node_id": None,
                    "status": "completed" if exp_status == "completed" else exp_status,
                    "error_message": None,
                }
                for g in detail_groups if isinstance(g, dict)
            ]

        if not meta_history and multi_detail.get("meta_weights"):
            meta_history = [{
                "as_of_date": None,
                "method": multi_detail.get("meta_method", "ic_weighted"),
                "weights": multi_detail.get("meta_weights") or {},
                "combined_ic": multi_detail.get("combined_ic"),
            }]

        if not groups:
            raise HTTPException(status_code=404, detail=f"No multi-alpha groups for {experiment_id}")

        ready = exp_status == "completed" and bool(meta_history) and bool(multi_detail)
        if exp_status == "completed" and not ready:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Multi-alpha results are not artifact-ready for {experiment_id}: "
                    "missing persisted meta weights or multi_alpha_detail"
                ),
            )

        unified_backtest = {}
        if isinstance(lifecycle.get("unified_backtest"), dict):
            unified_backtest = lifecycle["unified_backtest"]
        elif isinstance(multi_detail.get("unified_backtest"), dict):
            unified_backtest = multi_detail["unified_backtest"]
        backtest_loop_id = lifecycle.get("backtest_loop_id") or unified_backtest.get("loop_id")
        primary_node_id = lifecycle.get("primary_node_id") or unified_backtest.get("primary_node_id")

        return {
            "ok": True,
            "experiment_id": experiment_id,
            "experiment_status": exp_status,
            "ready": ready,
            "stage": lifecycle.get("stage") or ("completed" if ready else exp_status),
            "artifact_status": lifecycle.get("artifact_status") or ("ready" if ready else "pending"),
            "artifact_errors": lifecycle.get("errors", []),
            "backtest_loop_id": backtest_loop_id,
            "primary_node_id": primary_node_id,
            "unified_backtest": unified_backtest or None,
            "groups": groups,
            "meta_weights_history": meta_history,
            "multi_alpha_analysis": result_metrics.get("multi_alpha_analysis") if isinstance(result_metrics, dict) else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 3 v2.0: Multi-Alpha Diagnostics
# ============================================================

@router.get("/multi-alpha/{experiment_id}/diagnostics",
            summary="Multi-Alpha 完整诊断报告")
def multi_alpha_diagnostics(experiment_id: str):
    """获取 Multi-Alpha 实验的完整诊断报告：组性能、相关性、瓶颈、建议。"""
    try:
        from ..services.quantevolver.multi_alpha_diagnostics import MultiAlphaDiagnostics
        diag = MultiAlphaDiagnostics()
        return diag.analyze(experiment_id)
    except Exception as e:
        logger.exception("Multi-Alpha 诊断失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/multi-alpha/{experiment_id}/group-correlations",
            summary="Multi-Alpha 组间预测相关性")
def multi_alpha_group_correlations(experiment_id: str):
    """获取已缓存的组间预测相关性矩阵。"""
    try:
        from ..services.quantevolver.multi_alpha_diagnostics import MultiAlphaDiagnostics
        diag = MultiAlphaDiagnostics()
        return diag.compute_group_correlations(experiment_id)
    except Exception as e:
        logger.exception("获取组间相关性失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/multi-alpha/{experiment_id}/recommendations",
            summary="Multi-Alpha 演进建议")
def multi_alpha_recommendations(experiment_id: str):
    """基于诊断结果生成优先级排序的演进建议。"""
    try:
        from ..services.quantevolver.multi_alpha_diagnostics import MultiAlphaDiagnostics
        diag = MultiAlphaDiagnostics()
        return diag.get_recommendations(experiment_id)
    except Exception as e:
        logger.exception("获取演进建议失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 3 v2.0: Multi-Alpha 补充端点 (批次 C)
# ============================================================

@router.get("/multi-alpha/classified-factors",
            summary="查询已分类因子（按数据源组过滤）")
def multi_alpha_classified_factors(
    data_source_group: str = Query(..., description="数据源组: price_volume/money_flow/fundamental/valuation/chip/sector/cross_dataset"),
    min_grade: str = Query("D", description="最低评级: S/A/B/C/D"),
    exclude_names: str = Query("", description="排除的因子名（逗号分隔）"),
    limit: int = Query(50, ge=1, le=200),
):
    """查询指定 data_source_group 的已分类因子，按 IC 排序。

    用于因子编辑器的候选因子列表。
    """
    grade_order = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4, "P": 5}
    min_val = grade_order.get(min_grade, 4)
    exclude_set = set(n.strip() for n in exclude_names.split(",") if n.strip())

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT fc.factor_name,
                           fr.official_grade, fr.official_score,
                           fm.ic_mean AS ind_ic,
                           CASE WHEN fm.rank_ic_1d IS NULL AND fm.rank_ic_5d IS NULL
                                     AND fm.rank_ic_10d IS NULL AND fm.rank_ic_20d IS NULL THEN NULL
                                ELSE GREATEST(COALESCE(ABS(fm.rank_ic_1d), 0),
                                              COALESCE(ABS(fm.rank_ic_5d), 0),
                                              COALESCE(ABS(fm.rank_ic_10d), 0),
                                              COALESCE(ABS(fm.rank_ic_20d), 0))
                           END AS ind_rank_ic_best_abs,
                           fc.category,
                           fc.data_source_group, fc.holding_period_class,
                           fm.icir_annualized, fm.rank_ic_1d, fm.rank_ic_5d,
                           fm.rank_ic_10d, fm.rank_ic_20d
                    FROM qe_factor_classification fc
                    LEFT JOIN LATERAL (
                        SELECT ic_mean, icir_annualized, rank_ic_1d, rank_ic_5d,
                               rank_ic_10d, rank_ic_20d
                        FROM aistock_factor_metrics
                        WHERE factor_name = fc.factor_name AND eval_window = 'full'
                          AND calc_engine = 'qe_eval_v2'
                        ORDER BY calculated_at DESC LIMIT 1
                    ) fm ON TRUE
                    JOIN aistock_factor_catalog cat
                        ON cat.factor_name = fc.factor_name AND cat.source = fc.factor_source
                    LEFT JOIN LATERAL (
                        SELECT official_grade, official_score FROM qe_factor_official_ratings r
                        WHERE r.factor_catalog_id = cat.id
                          AND r.rule_version = (
                              SELECT rule_version FROM qe_rating_rule_versions
                              WHERE status = 'active' ORDER BY activated_at DESC LIMIT 1
                          )
                        ORDER BY r.graded_at DESC LIMIT 1
                    ) fr ON TRUE
                    WHERE cat.is_available = TRUE
                      AND fc.data_source_group = %s
                    ORDER BY
                        CASE fr.official_grade
                            WHEN 'S' THEN 0 WHEN 'A' THEN 1 WHEN 'B' THEN 2
                            WHEN 'C' THEN 3 WHEN 'D' THEN 4 ELSE 5
                        END ASC,
                        ind_rank_ic_best_abs DESC NULLS LAST
                """, (data_source_group,))
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        # 过滤评级和排除名单
        results = []
        for f in rows:
            if grade_order.get(f.get("official_grade"), 5) > min_val:
                continue
            if f["factor_name"] in exclude_set:
                continue
            results.append(f)
            if len(results) >= limit:
                break

        return {"ok": True, "factors": results, "total": len(results)}
    except Exception as e:
        logger.exception("查询已分类因子失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/multi-alpha/validate-config",
             summary="预验证 Multi-Alpha 配置")
def multi_alpha_validate_config(config: dict = Body(...)):
    """验证 MultiAlphaConfig 的合法性和质量。

    返回 warnings/errors + 分类覆盖率统计。
    """
    warnings = []
    errors = []

    alpha_groups = config.get("alpha_groups", [])

    # 基本校验
    if len(alpha_groups) < 2:
        errors.append({"level": "error", "message": f"至少需要 2 个组，当前 {len(alpha_groups)} 个"})

    for g in alpha_groups:
        g_name = g.get("group_name", "unknown")
        factors = g.get("factor_names", [])
        if len(factors) < 3:
            warnings.append({
                "level": "warn",
                "message": f"{g_name} 组只有 {len(factors)} 个因子，低于推荐的 5 个"
            })
        if not g.get("model_id"):
            errors.append({"level": "error", "message": f"{g_name} 组缺少 model_id"})

    # 分类覆盖率
    all_factor_names = []
    for g in alpha_groups:
        all_factor_names.extend(g.get("factor_names", []))

    coverage = {"total_factors": len(all_factor_names), "has_grade": 0, "has_data_source_group": 0, "missing": []}
    if all_factor_names:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    placeholders = ",".join(["%s"] * len(all_factor_names))
                    cur.execute(f"""
                        SELECT fc.factor_name, fr.official_grade, fc.data_source_group
                        FROM qe_factor_classification fc
                        JOIN aistock_factor_catalog cat
                            ON cat.factor_name = fc.factor_name AND cat.source = fc.factor_source
                        LEFT JOIN LATERAL (
                            SELECT official_grade FROM qe_factor_official_ratings r
                            WHERE r.factor_catalog_id = cat.id
                              AND r.rule_version = (
                                  SELECT rule_version FROM qe_rating_rule_versions
                                  WHERE status = 'active'
                                  ORDER BY activated_at DESC NULLS LAST, created_at DESC
                                  LIMIT 1
                              )
                            ORDER BY r.graded_at DESC LIMIT 1
                        ) fr ON TRUE
                        WHERE fc.factor_name IN ({placeholders})
                    """, all_factor_names)
                    classified = {r[0]: {"grade": r[1], "dsg": r[2]} for r in cur.fetchall()}

            found_names = set(classified.keys())
            coverage["has_grade"] = sum(1 for v in classified.values() if v["grade"])
            coverage["has_data_source_group"] = sum(1 for v in classified.values() if v["dsg"])
            coverage["missing"] = [n for n in all_factor_names if n not in found_names]
        except Exception as e:
            logger.warning(f"覆盖率检查失败: {e}")

    return {
        "valid": len(errors) == 0,
        "warnings": warnings,
        "errors": errors,
        "classification_coverage": coverage,
    }


@router.get("/multi-alpha/classification-coverage",
            summary="因子分类覆盖率统计")
def multi_alpha_classification_coverage():
    """统计各 data_source_group 的因子数量和平均 IC。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 总体统计
                cur.execute("""
                    SELECT COUNT(*) AS total,
                           COUNT(CASE WHEN fr.official_grade IS NOT NULL THEN 1 END) AS has_grade,
                           COUNT(CASE WHEN fc.data_source_group IS NOT NULL
                                       AND fc.data_source_group != 'unknown' THEN 1 END) AS has_dsg
                    FROM qe_factor_classification fc
                    JOIN aistock_factor_catalog cat
                        ON cat.factor_name = fc.factor_name AND cat.source = fc.factor_source
                    LEFT JOIN LATERAL (
                        SELECT official_grade FROM qe_factor_official_ratings r
                        WHERE r.factor_catalog_id = cat.id
                          AND r.rule_version = (
                              SELECT rule_version FROM qe_rating_rule_versions
                              WHERE status = 'active'
                              ORDER BY activated_at DESC NULLS LAST, created_at DESC
                              LIMIT 1
                          )
                        ORDER BY r.graded_at DESC LIMIT 1
                    ) fr ON TRUE
                    WHERE cat.is_available = TRUE
                """)
                total_row = cur.fetchone()
                total_available = total_row[0]
                has_grade = total_row[1]
                has_dsg = total_row[2]

                # 按组统计
                cur.execute("""
                    SELECT fc.data_source_group,
                           COUNT(*) AS count,
                           ROUND(AVG(ABS(fm.ic_mean))::numeric, 4) AS avg_ic
                    FROM qe_factor_classification fc
                    JOIN aistock_factor_catalog cat
                        ON cat.factor_name = fc.factor_name AND cat.source = fc.factor_source
                    LEFT JOIN LATERAL (
                        SELECT ic_mean
                        FROM aistock_factor_metrics
                        WHERE factor_name = fc.factor_name
                          AND eval_window = 'full'
                          AND calc_engine = 'qe_eval_v2'
                        ORDER BY calculated_at DESC
                        LIMIT 1
                    ) fm ON TRUE
                    WHERE cat.is_available = TRUE
                      AND fc.data_source_group IS NOT NULL
                      AND fc.data_source_group != 'unknown'
                    GROUP BY fc.data_source_group
                    ORDER BY count DESC
                """)
                by_group = {}
                for r in cur.fetchall():
                    by_group[r[0]] = {"count": r[1], "avg_ic": float(r[2]) if r[2] else None}

        return {
            "total_available": total_available,
            "fully_classified": has_dsg,
            "coverage_pct": round(has_dsg / total_available * 100, 1) if total_available > 0 else 0,
            "by_group": by_group,
            "missing_grade": total_available - has_grade,
            "missing_data_source_group": total_available - has_dsg,
        }
    except Exception as e:
        logger.exception("分类覆盖率统计失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/multi-alpha/compatible-models",
            summary="查询兼容模型列表")
def multi_alpha_compatible_models(
    dataset_type: str = Query(None, description="过滤 dataset_type: DatasetH/TSDatasetH"),
):
    """返回兼容的模型列表，含 builtin seed 模型和已导入模型。"""
    try:
        models = []

        # Builtin seed models
        _SEED_MODELS = [
            {"model_id": "__seed_ALSTM_default_v1__", "model_type": "ALSTM", "dataset_type": "TSDatasetH", "compute_resource": "gpu", "description": "ALSTM 默认配置"},
            {"model_id": "__seed_LGBModel_conservative_v1__", "model_type": "LGBModel", "dataset_type": "DatasetH", "compute_resource": "cpu", "description": "LGBModel 保守配置"},
            {"model_id": "__seed_LGBModel_aggressive_v1__", "model_type": "LGBModel", "dataset_type": "DatasetH", "compute_resource": "cpu", "description": "LGBModel 激进配置"},
            {"model_id": "__seed_Ridge_default_v1__", "model_type": "Ridge", "dataset_type": "DatasetH", "compute_resource": "cpu", "description": "Ridge 默认配置"},
            {"model_id": "__seed_ALSTM_deep_v1__", "model_type": "ALSTM", "dataset_type": "TSDatasetH", "compute_resource": "gpu", "description": "ALSTM 深层配置"},
            {"model_id": "__seed_GRU_light_v1__", "model_type": "GRU", "dataset_type": "TSDatasetH", "compute_resource": "gpu", "description": "GRU 轻量配置"},
            {"model_id": "__seed_Transformer_v1__", "model_type": "Transformer", "dataset_type": "TSDatasetH", "compute_resource": "gpu", "description": "Transformer 配置"},
        ]
        for m in _SEED_MODELS:
            if dataset_type and m["dataset_type"] != dataset_type:
                continue
            models.append(m)

        # Catalog models
        with get_conn() as conn:
            with conn.cursor() as cur:
                where = "WHERE is_available = TRUE"
                params = []
                if dataset_type:
                    where += " AND model_type IN %s"
                    # TSDatasetH → time-series models (ALSTM, GRU, Transformer)
                    # DatasetH → tabular models (LGBModel, Ridge, etc)
                    if dataset_type == "TSDatasetH":
                        params.append(("ALSTM", "GRU", "Transformer", "LSTM"))
                    else:
                        params.append(("LGBModel", "Ridge", "CatBoost", "XGBoost", "LinearModel"))

                cur.execute(f"""
                    SELECT model_id, model_name, model_type, display_name, catalog_source
                    FROM aistock_model_catalog
                    {where}
                    ORDER BY model_type, model_name
                    LIMIT 100
                """, params)
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall():
                    r = dict(zip(cols, row))
                    models.append({
                        "model_id": r["model_id"],
                        "model_type": r.get("model_type"),
                        "dataset_type": "TSDatasetH" if r.get("model_type") in ("ALSTM", "GRU", "Transformer", "LSTM") else "DatasetH",
                        "compute_resource": "gpu" if r.get("model_type") in ("ALSTM", "GRU", "Transformer", "LSTM") else "cpu",
                        "description": r.get("display_name") or r.get("model_name") or r["model_id"],
                        "source": r.get("catalog_source"),
                    })

        return {"ok": True, "models": models, "total": len(models)}
    except Exception as e:
        logger.exception("查询兼容模型失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies")
def list_strategies(
    search: Optional[str] = Query(None),
    strategy_type: Optional[str] = Query(None, description="过滤策略类型: daily/intraday"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """获取全部策略列表。"""
    try:
        from ..db.pg_pool import get_conn

        conditions = []
        params = []

        if search:
            conditions.append("(strategy_id ILIKE %s OR display_name ILIKE %s OR description ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
        if strategy_type:
            conditions.append("strategy_type = %s")
            params.append(strategy_type)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM aistock_strategy_catalog WHERE {where_clause}",
                    params,
                )
                total = cur.fetchone()[0]

                cur.execute(f"""
                    SELECT strategy_id, display_name, description, strategy_type,
                           catalog_source, scenario, market, freq,
                           portfolio_config, default_kwargs, param_schema,
                           in_selection_center, parent_strategy_id,
                           created_at, updated_at, llm_analysis
                    FROM aistock_strategy_catalog
                    WHERE {where_clause}
                    ORDER BY in_selection_center DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, params + [limit, offset])
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                # 序列化datetime
                for row in rows:
                    for k in ("created_at", "updated_at"):
                        if row.get(k):
                            row[k] = row[k].isoformat()

        return {"ok": True, "total": total, "items": rows}
    except Exception as e:
        logger.exception("获取策略列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies/{strategy_id}")
def get_strategy_detail(strategy_id: str):
    """获取策略详情（含源代码）。"""
    try:
        from ..db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT strategy_id, display_name, description, strategy_type,
                           catalog_source, scenario, market, freq,
                           source_code, portfolio_config, default_kwargs, param_schema,
                           in_selection_center, parent_strategy_id,
                           created_at, updated_at, llm_analysis
                    FROM aistock_strategy_catalog
                    WHERE strategy_id = %s
                """, (strategy_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")
                cols = [desc[0] for desc in cur.description]
                data = dict(zip(cols, row))
                for k in ("created_at", "updated_at"):
                    if data.get(k):
                        data[k] = data[k].isoformat()
        return {"ok": True, "data": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取策略详情失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategies")
def create_strategy(req: CreateStrategyRequest):
    """新建策略。"""
    try:
        import json as _json
        from ..db.pg_pool import get_conn
        
        # 保存源码到文件系统
        strategies_dir = AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_strategies"
        strategies_dir.mkdir(parents=True, exist_ok=True)
        file_path = strategies_dir / f"{req.strategy_id}.py"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(req.source_code)
        source_code_relpath = f"qe_strategies/{req.strategy_id}.py"

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查ID是否已存在
                cur.execute("SELECT 1 FROM aistock_strategy_catalog WHERE strategy_id = %s", (req.strategy_id,))
                if cur.fetchone():
                    raise HTTPException(status_code=409, detail=f"策略ID {req.strategy_id} 已存在")

                cur.execute("""
                    INSERT INTO aistock_strategy_catalog (
                        strategy_id, display_name, description, strategy_type,
                        catalog_source, market, freq,
                        source_code, source_code_relpath, default_kwargs, param_schema,
                        parent_strategy_id, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        'custom', %s, %s,
                        %s, %s, %s, %s,
                        %s, NOW(), NOW()
                    )
                """, (
                    req.strategy_id, req.display_name, req.description, req.strategy_type,
                    req.market, req.freq,
                    req.source_code, source_code_relpath,
                    _json.dumps(req.default_kwargs) if req.default_kwargs else None,
                    _json.dumps(req.param_schema) if req.param_schema else None,
                    req.parent_strategy_id,
                ))
        return {"ok": True, "strategy_id": req.strategy_id, "message": "策略创建成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("创建策略失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/strategies/{strategy_id}")
def update_strategy(strategy_id: str, req: UpdateStrategyRequest):
    """编辑策略。"""
    try:
        import json as _json
        from ..db.pg_pool import get_conn

        set_parts = []
        params = []
        if req.display_name is not None:
            set_parts.append("display_name = %s")
            params.append(req.display_name)
        if req.description is not None:
            set_parts.append("description = %s")
            params.append(req.description)
        if req.strategy_type is not None:
            set_parts.append("strategy_type = %s")
            params.append(req.strategy_type)
        if req.source_code is not None:
            # 同步更新文件系统中的源码
            strategies_dir = AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_strategies"
            strategies_dir.mkdir(parents=True, exist_ok=True)
            file_path = strategies_dir / f"{strategy_id}.py"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(req.source_code)
            source_code_relpath = f"qe_strategies/{strategy_id}.py"
            
            set_parts.append("source_code = %s")
            params.append(req.source_code)
            set_parts.append("source_code_relpath = %s")
            params.append(source_code_relpath)
        if req.market is not None:
            set_parts.append("market = %s")
            params.append(req.market)
        if req.freq is not None:
            set_parts.append("freq = %s")
            params.append(req.freq)
        if req.default_kwargs is not None:
            set_parts.append("default_kwargs = %s")
            params.append(_json.dumps(req.default_kwargs))
        if req.param_schema is not None:
            set_parts.append("param_schema = %s")
            params.append(_json.dumps(req.param_schema))

        if not set_parts:
            return {"ok": True, "message": "无更新内容"}

        set_parts.append("updated_at = NOW()")
        params.append(strategy_id)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE aistock_strategy_catalog SET {', '.join(set_parts)} WHERE strategy_id = %s",
                    params,
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

        return {"ok": True, "strategy_id": strategy_id, "message": "策略更新成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新策略失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/strategies/{strategy_id}")
def delete_strategy(strategy_id: str):
    """删除策略。"""
    try:
        from ..db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM aistock_strategy_catalog WHERE strategy_id = %s", (strategy_id,))
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")
        return {"ok": True, "message": f"策略 {strategy_id} 已删除"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("删除策略失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategies/{strategy_id}/clone")
def clone_strategy(strategy_id: str, req: CreateStrategyRequest):
    """从现有策略模板创建新策略。"""
    try:
        import json as _json
        from ..db.pg_pool import get_conn
        
        # 保存源码到文件系统
        strategies_dir = AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_strategies"
        strategies_dir.mkdir(parents=True, exist_ok=True)
        file_path = strategies_dir / f"{req.strategy_id}.py"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(req.source_code)
        source_code_relpath = f"qe_strategies/{req.strategy_id}.py"

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查模板策略是否存在
                cur.execute("SELECT 1 FROM aistock_strategy_catalog WHERE strategy_id = %s", (strategy_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail=f"模板策略 {strategy_id} 不存在")

                # 检查新ID是否已存在
                cur.execute("SELECT 1 FROM aistock_strategy_catalog WHERE strategy_id = %s", (req.strategy_id,))
                if cur.fetchone():
                    raise HTTPException(status_code=409, detail=f"策略ID {req.strategy_id} 已存在")

                cur.execute("""
                    INSERT INTO aistock_strategy_catalog (
                        strategy_id, display_name, description, strategy_type,
                        catalog_source, market, freq,
                        source_code, source_code_relpath, default_kwargs, param_schema,
                        parent_strategy_id, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        'custom', %s, %s,
                        %s, %s, %s, %s,
                        %s, NOW(), NOW()
                    )
                """, (
                    req.strategy_id, req.display_name, req.description, req.strategy_type,
                    req.market, req.freq,
                    req.source_code, source_code_relpath,
                    _json.dumps(req.default_kwargs) if req.default_kwargs else None,
                    _json.dumps(req.param_schema) if req.param_schema else None,
                    strategy_id,  # parent_strategy_id = 模板策略ID
                ))
        return {"ok": True, "strategy_id": req.strategy_id, "parent_strategy_id": strategy_id, "message": "从模板创建策略成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("从模板创建策略失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategies/analyze")
async def analyze_strategy_code(req: AnalyzeStrategyRequest):
    """调用LLM分析策略代码的语法和合理性。"""
    try:
        from ..db.pg_pool import get_conn
        from ..services.quantevolver.strategy_analyzer import StrategyAnalyzer

        code = req.source_code
        if not code:
            # 从数据库获取
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT source_code FROM aistock_strategy_catalog WHERE strategy_id = %s", (req.strategy_id,))
                    row = cur.fetchone()
                    if not row or not row[0]:
                        raise HTTPException(status_code=404, detail=f"策略 {req.strategy_id} 不存在或无源代码")
                    code = row[0]

        analyzer = StrategyAnalyzer()
        result = await asyncio.to_thread(analyzer.analyze, strategy_id=req.strategy_id, source_code=code)

        # 将分析结果写入数据库
        if result.get("ok"):
            with get_conn() as conn:
                with conn.cursor() as cur:
                    import json as _json
                    cur.execute(
                        "UPDATE aistock_strategy_catalog SET llm_analysis = %s, updated_at = NOW() WHERE strategy_id = %s",
                        (_json.dumps(result.get("analysis"), ensure_ascii=False), req.strategy_id),
                    )

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("策略分析失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 2 API: FactorAnalyst
# ============================================================

@router.post("/factor-analyst/analyze")
async def analyze_factor(req: AnalyzeFactorRequest):
    """分析指定因子（分类+描述）。评级只读，由 FactorRatingService 统一管理。"""
    try:
        from ..services.quantevolver.factor_analyst import FactorAnalyst
        fa = FactorAnalyst()
        result = await asyncio.to_thread(
            fa.analyze_single_factor,
            factor_name=req.factor_name,
            factor_source=req.factor_source,
            use_llm=req.use_llm,
        )
        return result
    except Exception as e:
        logger.exception("因子分析失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor-analyst/batch-analyze")
async def batch_analyze_factors(req: BatchAnalyzeRequest):
    """批量分析因子（同步版本，向后兼容）。"""
    try:
        from ..services.quantevolver.factor_analyst import FactorAnalyst
        fa = FactorAnalyst()
        result = await asyncio.to_thread(
            fa.batch_analyze_all_factors,
            use_llm=req.use_llm,
            source_filter=req.source_filter,
            factor_names=req.factor_names,
        )
        return result
    except Exception as e:
        logger.exception("批量因子分析失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor-analyst/batch-analyze-stream")
def batch_analyze_factors_stream(req: BatchAnalyzeRequest):
    """批量分析因子（SSE流式版本）。"""
    from ..services.quantevolver.factor_analyst import FactorAnalyst

    async def event_generator():
        fa = FactorAnalyst()
        try:
            async for event in fa.batch_analyze_all_factors_async(
                use_llm=req.use_llm,
                source_filter=req.source_filter,
                factor_names=req.factor_names,
            ):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.exception("批量因子分析失败")
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/factor-analyst/batch-rerun")
async def batch_rerun_factors(req: BatchAnalyzeRequest):
    """批量重跑因子分析（清空旧结果后重新分析）。"""
    try:
        from ..services.quantevolver.factor_analyst import FactorAnalyst
        fa = FactorAnalyst()

        # 清空旧分类结果
        deleted = fa.clear_classifications(source_filter=req.source_filter)
        logger.info(f"已清空 {deleted} 条旧分类结果")

        # 重新批量分析
        result = await asyncio.to_thread(
            fa.batch_analyze_all_factors,
            use_llm=req.use_llm,
            source_filter=req.source_filter,
            factor_names=req.factor_names,
        )
        result["deleted"] = deleted
        return result
    except Exception as e:
        logger.exception("批量重跑因子分析失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-analyst/classifications")
def get_classifications(
    source: Optional[str] = Query(None),
    exclude_source: Optional[str] = Query(None, description="排除的source，逗号分隔"),
    category: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取因子分类结果列表。"""
    try:
        from ..services.quantevolver.factor_analyst import FactorAnalyst
        fa = FactorAnalyst()
        result = fa.get_classifications(
            source_filter=source,
            exclude_source_filter=exclude_source,
            category_filter=category,
            grade_filter=grade,
            limit=limit,
            offset=offset,
        )
        return result
    except Exception as e:
        logger.exception("获取分类结果失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor-analyst/recommend")
async def recommend_factors(req: RecommendFactorsRequest):
    """推荐因子组合。"""
    try:
        from ..services.quantevolver.factor_analyst import FactorAnalyst
        fa = FactorAnalyst()
        result = await asyncio.to_thread(
            fa.recommend_factor_combination,
            target_count=req.target_count,
            include_categories=req.include_categories,
            min_grade=req.min_grade,
            diversity_weight=req.diversity_weight,
        )
        return result
    except Exception as e:
        logger.exception("因子推荐失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 2 API: PortfolioArchitect
# ============================================================

@router.post("/portfolio/evaluate")
def evaluate_combination(req: EvaluateCombinationRequest):
    """评估因子+模型+策略组合。"""
    try:
        from ..services.quantevolver.portfolio_architect import PortfolioArchitect
        pa = PortfolioArchitect()
        result = pa.evaluate_combination(
            factor_names=req.factor_names,
            model_id=req.model_id,
            strategy_id=req.strategy_id,
            custom_params=req.custom_params,
        )
        return result
    except Exception as e:
        logger.exception("组合评估失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/portfolio/recommend")
def recommend_combinations(req: RecommendCombinationsRequest):
    """智能推荐多种组合。"""
    try:
        from ..services.quantevolver.portfolio_architect import PortfolioArchitect
        pa = PortfolioArchitect()
        result = pa.recommend_combinations(
            target_profiles=req.target_profiles,
            max_combinations=req.max_combinations,
        )
        return result
    except Exception as e:
        logger.exception("组合推荐失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiment/smart-select")
def smart_select_components(req: SmartSelectRequest):
    """基于用户自然语言需求智能选择合适的因子、模型和策略。"""
    try:
        from ..services.quantevolver.portfolio_architect import PortfolioArchitect
        pa = PortfolioArchitect()
        result = pa.generate_from_requirement(
            user_requirement=req.user_requirement,
            use_llm=True,
            max_factors=req.max_factors,
        )
        return result
    except Exception as e:
        logger.exception("智能组件选择失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiment/evaluate-portfolio")
def evaluate_portfolio_design(req: EvaluatePortfolioRequest):
    """AI评估用户选择的组合配置（因子+模型+策略）。

    使用 PortfolioArchitect 进行多维度规则分析 + LLM 综合评估，
    返回格式与前端 EvalResult 类型对齐：
    { ok, overall_score, risks, suggestions, llm_commentary, factor_analysis, ... }
    """
    try:
        from ..services.quantevolver.portfolio_architect import PortfolioArchitect
        pa = PortfolioArchitect()
        result = pa.evaluate_combination(
            factor_names=req.factor_names,
            model_id=req.model_id,
            strategy_id=req.strategy_id,
            custom_params=req.custom_params,
        )
        return result
    except Exception as e:
        logger.exception("AI评估组合失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/portfolio/generate")
def generate_from_requirement(req: GenerateFromRequirementRequest):
    """基于用户自然语言需求智能生成最佳因子+模型组合。"""
    try:
        from ..services.quantevolver.portfolio_architect import PortfolioArchitect
        pa = PortfolioArchitect()
        result = pa.generate_from_requirement(
            user_requirement=req.user_requirement,
            use_llm=req.use_llm,
            max_factors=req.max_factors,
        )
        return result
    except Exception as e:
        logger.exception("智能组合生成失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 2 API: ConfigComposer
# ============================================================

@router.post("/config/generate")
def generate_config(req: GenerateConfigRequest):
    """生成QLib配置文件。支持 HMM 板块轮动和分钟线策略。dispatch_mode=evolution时标记为待演进。"""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer

        # --- 严格参数验证（禁止静默兜底）---
        _validate_qe_catalog_refs(req.strategy_id, req.model_id)
        custom_params = _normalize_single_experiment_custom_params(
            req,
            source="quantevolver.config.generate",
        )


        cc = ConfigComposer()

        # ── Multi-Alpha 分支 ────────────────────────────────────────
        if req.alpha_mode == "multi" and req.multi_alpha_config:
            from ..services.quantevolver.experiment_config_builders import build_config_from_multi_alpha
            from ..services.quantevolver.multi_alpha_engine import MultiAlphaEngine

            _assert_multi_alpha_execution_mode_supported(req.multi_alpha_config)

            # 构建 HMM 配置（传 HmmConfig 兼容的 dict 或 None）
            hmm_cfg_dict = None
            if custom_params.get("sector_hmm_model_path"):
                hmm_cfg_dict = {
                    "enable_sector_hmm": True,
                    "hmm_model_version_id": custom_params.get("hmm_model_version_id", "from_compose"),
                    "sector_hmm_model_path": custom_params["sector_hmm_model_path"],
                    "hmm_signal_preset": custom_params.get("hmm_signal_preset"),
                    "hmm_signal_presets": custom_params.get("hmm_signal_presets"),
                    "hmm_config_json": custom_params.get("hmm_config_json"),
                }

            # 构建 unfilled_handler_params（去掉 unfilled_ 前缀，匹配 ExperimentConfig 期望格式）
            uf_params = None
            if custom_params.get("unfilled_handler"):
                uf_params = {}
                if custom_params.get("unfilled_trigger_minute"):
                    uf_params["trigger_minute"] = custom_params["unfilled_trigger_minute"]
                if custom_params.get("unfilled_backup_depth"):
                    uf_params["backup_depth"] = custom_params["unfilled_backup_depth"]

            exp_cfg = build_config_from_multi_alpha(
                multi_alpha_config=req.multi_alpha_config,
                data_split=req.data_split,
                strategy_id=req.strategy_id,
                strategy_params={"topk": custom_params.get("topk"), "n_drop": custom_params.get("n_drop")} if custom_params else None,
                label_type=custom_params.get("label_type"),
                label_horizon=custom_params.get("label_horizon"),
                execution_algo=custom_params.get("execution_algo"),
                execution_algo_params=custom_params.get("execution_algo_params"),
                filter_suspended_on_signal=custom_params.get("filter_suspended_on_signal"),
                suspend_filter_strict=custom_params.get("suspend_filter_strict", True),
                unfilled_handler=custom_params.get("unfilled_handler"),
                unfilled_handler_params=uf_params,
                stock_pool=custom_params.get("stock_pool"),
                hmm_config=hmm_cfg_dict,
                experiment_name=req.experiment_name,
            )
            # 查询可用节点（用于分布式规划 + 前端展示）
            available_nodes = []
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT node_id, api_base_url, gpu_model, gpu_vram_mb, status
                        FROM infra.compute_nodes
                    """)
                    _ncols = [d[0] for d in cur.description]
                    available_nodes = [dict(zip(_ncols, r)) for r in cur.fetchall()]

            engine = MultiAlphaEngine(
                experiment_config=exp_cfg,
                composer=cc,
                available_nodes=available_nodes,
            )
            engine_result = engine.run()

            if not engine_result.get("ok"):
                raise HTTPException(status_code=500, detail="MultiAlphaEngine 执行失败")

            parent_exp_id = engine_result.get("parent_experiment_id")

            # 汇总所有组的因子名（用于实验记录的 factor_names 字段）
            all_factor_names = []
            for ag in exp_cfg.multi_alpha_config.alpha_groups:
                all_factor_names.extend(ag.factor_names)

            # 创建 parent 实验记录 + 设置 alpha_mode（INSERT 而非 UPDATE，因为之前从未创建）
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO qe_experiments
                            (experiment_id, experiment_name, status,
                             factor_names, model_id, strategy_id,
                             data_split, custom_params,
                             alpha_mode, multi_alpha_config,
                             parent_multi_alpha_id, created_at)
                        VALUES (%s, %s, 'created', %s, %s, %s, %s, %s,
                                'multi', %s::jsonb, %s, NOW())
                        ON CONFLICT (experiment_id) DO UPDATE SET
                            alpha_mode = 'multi',
                            multi_alpha_config = EXCLUDED.multi_alpha_config,
                            parent_multi_alpha_id = EXCLUDED.parent_multi_alpha_id,
                            factor_names = EXCLUDED.factor_names
                    """, (
                        parent_exp_id,
                        parent_exp_id,
                        json.dumps(all_factor_names),
                        exp_cfg.multi_alpha_config.alpha_groups[0].model_id if exp_cfg.multi_alpha_config.alpha_groups else None,
                        req.strategy_id,
                        json.dumps(req.data_split) if req.data_split else None,
                        json.dumps(custom_params) if custom_params else None,
                        json.dumps(req.multi_alpha_config),
                        req.parent_multi_alpha_id,
                    ))
                conn.commit()

            # 生成前端需要的展示信息
            group_configs = engine_result.get("group_configs", [])
            execution_mode = engine_result.get("execution_mode", "serial")
            node_groups: dict[str, list[str]] = {}
            for gc in group_configs:
                n_id = gc.get("node_id", "wsl2-5080")
                node_groups.setdefault(n_id, []).append(gc["group_name"])

            # 构建 WSL 命令预览（前端展示用）
            wsl_lines = [
                f"# Multi-Alpha 实验 ({len(group_configs)} 组, 模式: {execution_mode})"
            ]
            for n_id, g_names in node_groups.items():
                wsl_lines.append(f"# 节点 {n_id}: {', '.join(g_names)}")
            wsl_lines.append("")
            for gc in sorted(group_configs, key=lambda g: g.get("order", 0)):
                if gc.get("reuse_mode") in ("reuse_prediction", "reuse_model"):
                    src = gc.get("source_prediction_path") or gc.get("prediction_path") or "未记录 prediction_path"
                    wsl_lines.append(f"# [{gc.get('node_id', '?')}] {gc['group_name']}: 复用 ({gc['reuse_mode']}) -> {src}")
                    continue
                group_cmd = _build_multi_alpha_group_command(gc, gc.get("node_id"))
                wsl_lines.append(f"# [{gc.get('node_id', '?')}] {gc['group_name']}:")
                wsl_lines.append(group_cmd)
            wsl_lines.append("")
            wsl_lines.append("# Meta-Model 合并:")
            wsl_lines.append("python meta_model_runner.py")

            # 构建返回结构（兼容前端 ConfigResult 类型）
            result = {
                "ok": True,
                "experiment_id": parent_exp_id,
                "experiment_dir": f"qe_workspace/{parent_exp_id}",
                "wsl_command": "\n".join(wsl_lines),
                # 多Alpha特有字段
                "alpha_mode": "multi",
                "execution_mode": execution_mode,
                "total_groups": engine_result.get("total_groups"),
                "meta_method": engine_result.get("meta_method"),
                "group_configs": group_configs,
                "node_distribution": node_groups,
                "is_distributed": len(node_groups) > 1,
            }

            if req.dispatch_mode == "evolution":
                result["evolution_pending"] = True
                result["evolution_params"] = req.evolution_params or {}

            return result

        # ── Single-Alpha（原有逻辑）────────────────────────────────
        result = cc.compose_experiment(
            factor_names=req.factor_names,
            factor_sources=req.factor_sources,
            model_id=req.model_id,
            strategy_id=req.strategy_id,
            data_split=req.data_split,
            custom_params=custom_params,
            experiment_name=req.experiment_name,
        )

        if req.dispatch_mode == "evolution":
            result["evolution_pending"] = True
            result["evolution_params"] = req.evolution_params or {}

        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("配置生成失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiments/pending")
def create_pending_experiment(req: SingleExperimentPendingCreateRequest):
    """Create a real single-experiment runtime row without submitting execution."""

    if req.alpha_mode == "multi" or req.multi_alpha_config:
        raise HTTPException(status_code=400, detail="experiments/pending only supports single QE tasks")

    req_dict = _model_to_dict_compat(req)
    custom_params = dict(req_dict.get("custom_params") or {})
    provenance = {
        "runtime_first": True,
        "created_by_type": req.created_by_type or "mcp",
        "created_by_name": req.created_by_name,
        "source_context_json": req.source_context_json,
        "provenance": req.provenance,
    }
    custom_params["qe_mcp_provenance"] = {
        key: value for key, value in provenance.items() if value not in (None, "", {})
    }
    if req.factor_sources:
        custom_params["qe_factor_sources"] = dict(req.factor_sources)
    req_dict["custom_params"] = custom_params
    generate_req = GenerateConfigRequest(**{
        key: req_dict.get(key)
        for key in _model_field_names(GenerateConfigRequest)
        if key in req_dict
    })
    result = generate_config(generate_req)
    result["operation"] = "create_pending"
    result["editable"] = True
    result["startable"] = True
    result["resume_allowed"] = False
    result["start_reason"] = "single experiment has not been submitted"
    return result


@router.get("/experiments")
def list_experiments(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    alpha_mode: Optional[str] = Query(None, description="过滤 alpha_mode: single/multi"),
    include_children: bool = Query(False, description="按历史页分组返回父实验及其演进 Loop"),
    detail: str = Query("summary", pattern="^(summary|full)$", description="summary 默认不返回 result_metrics/custom_params 大 JSON；full 保留旧完整字段"),
):
    """获取实验列表。默认 summary，避免列表/MCP 返回超大 JSONB。"""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer
        cc = ConfigComposer()
        result = cc.list_experiments(limit=limit, offset=offset, include_children=include_children, detail=detail)
        # alpha_mode 过滤（在应用层过滤，避免改动 ConfigComposer 内部查询）
        if alpha_mode and result.get("ok") and result.get("items"):
            result["items"] = [
                exp for exp in result["items"]
                if exp.get("alpha_mode", "single") == alpha_mode
            ]
        return result
    except Exception as e:
        logger.exception("获取实验列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/editable-config")
def get_experiment_editable_config(experiment_id: str):
    """Return the editable config for a not-yet-started single QE experiment."""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer

        cc = ConfigComposer()
        exp_record = cc._get_experiment_record(experiment_id)
        if not exp_record:
            raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")
        return {"ok": True, "data": _single_experiment_editable_payload(exp_record)}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取可编辑实验配置失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/experiments/{experiment_id}/editable-config")
def update_experiment_editable_config(experiment_id: str, req: SingleExperimentConfigUpdateRequest):
    """Update a single QE experiment only while it has never been submitted."""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer

        cc = ConfigComposer()
        exp_record = cc._get_experiment_record(experiment_id)
        if not exp_record:
            raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")
        startable, reason = _single_experiment_start_state(exp_record)
        if not startable:
            raise HTTPException(
                status_code=409,
                detail=f"实验 {experiment_id} 已启动或不可编辑: {reason}",
            )
        if (exp_record.get("alpha_mode") or "single") != "single":
            raise HTTPException(status_code=409, detail="multi-alpha 实验暂不支持通过单次实验编辑器修改")

        _validate_qe_catalog_refs(req.strategy_id, req.model_id)
        custom_params = _normalize_single_experiment_custom_params(
            req,
            source="quantevolver.experiments.editable_config",
        )
        existing_custom_params = _parse_json_object(exp_record.get("custom_params"))
        for provenance_key in (
            "qe_mcp_provenance",
            "qe_pending_task_source",
            "qe_pending_created_by",
        ):
            if provenance_key in existing_custom_params and provenance_key not in custom_params:
                custom_params[provenance_key] = existing_custom_params[provenance_key]
        if req.factor_sources:
            custom_params["qe_factor_sources"] = dict(req.factor_sources)
        elif "qe_factor_sources" in existing_custom_params and "qe_factor_sources" not in custom_params:
            custom_params["qe_factor_sources"] = existing_custom_params["qe_factor_sources"]

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_experiments
                    SET experiment_name = COALESCE(%s, experiment_name),
                        factor_names = %s,
                        model_id = %s,
                        strategy_id = %s,
                        data_split = %s,
                        custom_params = %s,
                        updated_at = NOW()
                    WHERE experiment_id = %s
                      AND COALESCE(status, '') IN ('created', 'pending')
                      AND qe_task_id IS NULL
                      AND qe_loop_id IS NULL
                      AND started_at IS NULL
                      AND completed_at IS NULL
                    """,
                    (
                        req.experiment_name,
                        json.dumps(req.factor_names or []),
                        req.model_id,
                        req.strategy_id,
                        json.dumps(req.data_split) if req.data_split else None,
                        json.dumps(custom_params) if custom_params else None,
                        experiment_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise HTTPException(status_code=409, detail="实验状态已变化，请刷新后再编辑")
            conn.commit()

        updated = cc._get_experiment_record(experiment_id)
        return {
            "ok": True,
            "operation": "update_pending_config",
            "experiment_id": experiment_id,
            "data": _single_experiment_editable_payload(updated or {}),
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("更新可编辑实验配置失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}")
def get_experiment_detail(
    experiment_id: str,
    detail: str = Query("summary", pattern="^(summary|full)$", description="summary 默认不返回 result_metrics；full 保留旧完整字段"),
):
    """获取实验详情。默认 summary，完整指标请使用 enhanced-metrics/trade-stats 等专用端点。"""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer
        cc = ConfigComposer()
        result = cc.get_experiment_detail(experiment_id, detail=detail)
        if not result.get("ok"):
            raise HTTPException(status_code=404, detail=result.get("error", "实验不存在"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取实验详情失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiments/{experiment_id}/sync-results")
async def sync_experiment_results(experiment_id: str):
    """同步实验结果。

    通过 RDAgent API 获取回测指标并写入 DB。
    多Alpha实验走 MultiAlphaResultCollector 路径，
    同时写入3张扩展表 + 统一分析层。
    """
    from ..services.quantevolver.config_composer import ConfigComposer
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    cc = ConfigComposer()
    exp_record = cc._get_experiment_record(experiment_id)
    if not exp_record:
        raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")

    # 多Alpha路径：调用 ResultCollector
    if exp_record.get("alpha_mode") == "multi":
        from ..services.quantevolver.multi_alpha_result_collector import MultiAlphaResultCollector
        try:
            collector = MultiAlphaResultCollector()
            return await collector.collect_and_persist(experiment_id)
        except Exception as e:
            logger.exception(f"多Alpha结果同步失败: {experiment_id}")
            raise HTTPException(status_code=500, detail=f"多Alpha结果同步失败: {e}")

    # 单Alpha路径：原有逻辑不变
    qe_task_id = exp_record.get("qe_task_id") or exp_record.get("experiment_name")
    qe_loop_id = exp_record.get("qe_loop_id")
    if not qe_task_id:
        raise HTTPException(status_code=400, detail="实验缺少 qe_task_id，无法同步")
    if not qe_loop_id:
        raise HTTPException(status_code=400, detail="实验缺少 qe_loop_id，无法同步")

    execution_node_id = _get_recorded_experiment_node(exp_record) or resolve_default_qe_node_id()

    try:
        async with QEWorkspaceClient.for_node(execution_node_id) as client:
            metrics = await client.get_loop_metrics(qe_task_id, qe_loop_id)
            _update_experiment_with_metrics(experiment_id, metrics)
            return {"ok": True, "experiment_id": experiment_id, "node_id": execution_node_id, "metrics": metrics}
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="回测指标尚未生成")
        raise HTTPException(status_code=502, detail=f"RDAgent API 错误: {e.response.status_code}")
    except Exception as e:
        logger.exception(f"同步实验结果失败: {experiment_id}")
        raise HTTPException(status_code=502, detail=f"RDAgent API 不可用: {e}")


@router.post("/experiments/{experiment_id}/regenerate")
def regenerate_experiment(experiment_id: str):
    """重新生成实验脚本（复用同一实验ID和名称）。"""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer
        cc = ConfigComposer()
        result = cc.regenerate_experiment(experiment_id)
        return result
    except Exception as e:
        logger.exception("重新生成实验脚本失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 因子值缓存管理 (Factor Value Cache)
# ============================================================

FACTOR_CACHE_ROOT = AISTOCK_PROJECT_ROOT / "rdagent_assets" / "factor_values"
FACTOR_CACHE_SINGLE_DIR = FACTOR_CACHE_ROOT / "single"
FACTOR_CACHE_META_PATH = FACTOR_CACHE_ROOT / "_meta.json"
FACTOR_CACHE_SOURCE_SPECS: Tuple[Dict[str, Any], ...] = (
    {
        "key": "backtest",
        "label": "官方共用缓存",
        "single_dir": FACTOR_CACHE_SINGLE_DIR,
        "meta_path": FACTOR_CACHE_META_PATH,
    },
)
FACTOR_CODE_DIR = AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_factors"
FACTOR_CACHE_TASK_DIR = AISTOCK_PROJECT_ROOT / "rdagent_assets" / "factor_values" / "_tasks"


def _is_official_factor_cache_path_shape(path_value: Any) -> bool:
    if not path_value:
        return False
    normalized = str(path_value).strip().strip("\"'").replace("\\", "/").rstrip("/")
    parts = [part.lower() for part in normalized.split("/") if part]
    if parts and parts[-1] in {"single", "_meta.json"}:
        parts = parts[:-1]
    return bool(parts) and parts[-1] == "factor_values"


def _get_all_factors_with_code_text() -> list:
    """从因子库查询所有有 code_text 的因子。

    QE 回测因子代码的唯一权威来源是 aistock_factor_catalog.code_text。
    不在运行时回连 RDAgent API 补源码，避免历史实验删除或格式差异
    导致同名因子的缓存 hash 不稳定。
    """
    from ..db.pg_pool import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT factor_name, source, code_text, best_loop_task_run_id
                FROM aistock_factor_catalog
                WHERE is_available = true
                ORDER BY factor_name
            """)
            cols = [d[0] for d in cur.description]
            results = [dict(zip(cols, row)) for row in cur.fetchall()]

    missing_code = [r["factor_name"] for r in results if not r.get("code_text")]
    if missing_code:
        logger.warning(
            "[factor-cache] %s 个可用因子缺少因子库 code_text，跳过 QE 回测缓存计算: %s",
            len(missing_code),
            missing_code[:10],
        )

    return [r for r in results if r.get("code_text")]

# 当前运行中的后台任务
_active_cache_tasks: Dict[str, Dict[str, Any]] = {}
_cache_meta_ttl: Dict[str, Dict[str, Any]] = {}


def _load_cache_meta(ttl_sec: int = 30, meta_path: Optional[Path] = None) -> dict:
    """读取 _meta.json（带 TTL 缓存，避免高频 IO）。"""
    import time
    path = Path(meta_path) if meta_path is not None else FACTOR_CACHE_META_PATH
    cache_key = str(path.resolve())
    now = time.time()
    cached = _cache_meta_ttl.get(cache_key)
    if cached and now - cached.get("loaded_at", 0) < ttl_sec and cached.get("meta") is not None:
        return cached["meta"]
    meta = {}
    try:
        if path.exists():
            meta = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.error(f"_meta.json 已损坏（JSON 解析失败）: {e}，请检查文件 {path}")
        raise RuntimeError(f"_meta.json 已损坏: {e}") from e
    _cache_meta_ttl[cache_key] = {"loaded_at": now, "meta": meta}
    return meta


def _invalidate_cache_meta(meta_path: Optional[Path] = None):
    """使 meta 内存缓存失效。"""
    if meta_path is None:
        _cache_meta_ttl.clear()
    else:
        _cache_meta_ttl.pop(str(Path(meta_path).resolve()), None)


def _split_factor_cache_range(date_range: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not date_range or "~" not in str(date_range):
        return None, None
    start, end = str(date_range).split("~", 1)
    return start.strip() or None, end.strip() or None


def _factor_cache_candidate_covers(
    candidate: Dict[str, Any],
    target_start: Optional[str] = None,
    target_end: Optional[str] = None,
    *,
    max_start_gap_days: int = DEFAULT_WARMUP_TOLERANCE_DAYS,
    expected_universe_key: Optional[str] = None,
    expected_universe_fingerprint_sha256: Optional[str] = None,
    expected_index_policy: Optional[str] = None,
) -> bool:
    if not candidate.get("valid_cache"):
        return False
    covered, _ = factor_cache_covers_window(
        cache_start_date=candidate.get("cache_start_date"),
        cache_end_date=candidate.get("cache_end_date"),
        target_start=target_start,
        target_end=target_end,
        entry=candidate.get("entry") or {},
        expected_universe_key=expected_universe_key,
        expected_universe_fingerprint_sha256=expected_universe_fingerprint_sha256,
        expected_index_policy=expected_index_policy,
        max_start_gap_days=max_start_gap_days,
    )
    return covered


def _collect_factor_cache_candidates(
    factor_name: str,
    source_specs: Optional[Tuple[Dict[str, Any], ...]] = None,
) -> List[Dict[str, Any]]:
    """Collect official shared factor-value cache candidates.

    Missing cache must be generated by official full compute rather than by
    falling back to any non-official cache directory.
    """
    candidates: List[Dict[str, Any]] = []
    specs = source_specs or FACTOR_CACHE_SOURCE_SPECS
    for spec in specs:
        if spec.get("key") != "backtest":
            continue
        if not _is_official_factor_cache_path_shape(spec.get("single_dir")) or not _is_official_factor_cache_path_shape(spec.get("meta_path")):
            logger.warning("[factor-cache] ignore non-official cache path in QE backtest specs: %s", spec)
            continue
        meta_path = Path(spec["meta_path"])
        single_dir = Path(spec["single_dir"])
        meta = _load_cache_meta(meta_path=meta_path)
        entry = (meta.get("factors") or {}).get(factor_name) or {}
        parquet_path = single_dir / f"{factor_name}.parquet"
        has_entry = bool(entry)
        has_file = parquet_path.exists()
        if not has_entry and not has_file:
            continue

        status = str(entry.get("status") or ("ok" if has_file else "no_cache")).lower()
        cache_start, cache_end = _split_factor_cache_range(entry.get("date_range"))
        valid_cache = has_entry and has_file and status != "error"
        size_mb = round(parquet_path.stat().st_size / 1024 / 1024, 1) if has_file else None
        candidates.append(
            {
                "source_key": spec["key"],
                "source_label": spec["label"],
                "entry": entry,
                "meta_path": meta_path,
                "parquet_path": parquet_path,
                "has_entry": has_entry,
                "has_file": has_file,
                "valid_cache": valid_cache,
                "cache_status": "ok" if valid_cache else ("error" if status == "error" else "no_cache"),
                "cache_date_range": entry.get("date_range"),
                "cache_start_date": cache_start,
                "cache_end_date": cache_end,
                "cache_computed_at": entry.get("computed_at"),
                "cache_as_of_date": entry.get("as_of_date"),
                "cache_window_train_start": entry.get("window_train_start"),
                "cache_window_backtest_end": entry.get("window_backtest_end"),
                "cache_data_source_mode": entry.get("data_source_mode"),
                "cache_universe_key": entry.get("universe_key"),
                "cache_universe_rule_version": entry.get("universe_rule_version"),
                "cache_universe_fingerprint_sha256": entry.get("universe_fingerprint_sha256"),
                "cache_index_policy": entry.get("index_policy"),
                "cache_size_mb": size_mb,
            }
        )
    return candidates


def _choose_best_factor_cache_candidate(
    candidates: List[Dict[str, Any]],
    target_start: Optional[str] = None,
    target_end: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Choose the cache row to expose in the factor library.

    Valid parquet+meta caches always outrank error-only metadata. Among valid
    caches, prefer the one covering the requested range, then fresher end/as-of
    dates and computed time.
    """
    valid = [c for c in candidates if c.get("valid_cache")]
    if valid:
        def _valid_key(candidate: Dict[str, Any]) -> Tuple[int, str, str, str, float]:
            covers = 1 if _factor_cache_candidate_covers(candidate, target_start, target_end) else 0
            return (
                covers,
                str(candidate.get("cache_end_date") or ""),
                str(candidate.get("cache_as_of_date") or ""),
                str(candidate.get("cache_computed_at") or ""),
                float(candidate.get("cache_size_mb") or 0),
            )

        return max(valid, key=_valid_key)

    if not candidates:
        return None

    def _non_valid_key(candidate: Dict[str, Any]) -> Tuple[int, str]:
        is_error = 1 if candidate.get("cache_status") == "error" else 0
        return (is_error, str(candidate.get("cache_computed_at") or ""))

    return max(candidates, key=_non_valid_key)


def _read_file_from_path(path_str: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not path_str:
        return None, None, None
    try:
        p = Path(path_str)
        if not p.is_absolute():
            p = Path(__file__).resolve().parents[2] / path_str
        p = p.resolve()
        if not p.exists() or not p.is_file():
            return None, str(p), f"file not found: {p}"
        return p.read_text(encoding="utf-8"), str(p), None
    except (OSError, UnicodeDecodeError, ValueError) as e:
        return None, path_str, str(e)


def _get_current_factor_code_hashes(factor_names: List[str]) -> Dict[str, str]:
    """计算因子当前代码的 hash。

    QE 回测只以因子库 code_text 为权威来源；不 fallback 到
    qe_code_path 或 RDAgent API，避免展示层 hash 与实际回测代码源漂移。
    """
    if not factor_names:
        return {}
    import hashlib as _hl

    code_hashes: Dict[str, str] = {}
    placeholders = ",".join(["%s"] * len(factor_names))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT factor_name, code_text FROM aistock_factor_catalog WHERE factor_name IN ({placeholders})",
                factor_names,
            )
            for factor_name, code_text in cur.fetchall():
                if code_text:
                    code_hashes[factor_name] = _hl.sha256(code_text.encode("utf-8")).hexdigest()[:16]
    return code_hashes


def _load_json_file(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"读取 JSON 文件失败 {path}: {e}")
    return None


def _load_failed_tail(path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = [ln.strip() for ln in f.readlines() if ln.strip()]
        return [json.loads(line) for line in lines[-limit:]]
    except Exception as e:
        logger.warning(f"读取失败日志失败 {path}: {e}")
        return []


@router.get("/factor-cache/stats", summary="因子值缓存统计")
def factor_cache_stats():
    """返回缓存总览：总缓存数、占用空间、覆盖率、日期范围分布。"""
    try:
        cached_files: List[Path] = []
        for spec in FACTOR_CACHE_SOURCE_SPECS:
            single_dir = Path(spec["single_dir"])
            if single_dir.exists():
                cached_files.extend(single_dir.glob("*.parquet"))
        total_size = sum(f.stat().st_size for f in cached_files)

        # 代码因子总数 + 可用/禁用因子名集合（从因子库 code_text 查询）
        db_available_names: set = set()
        db_disabled_names: set = set()
        total_code_factors = 0
        total_disabled_factors = 0
        # 当前代码 hash：仅使用因子库 code_text
        db_code_hashes: Dict[str, str] = {}
        try:
            from ..db.pg_pool import get_conn
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT factor_name, is_available
                        FROM aistock_factor_catalog
                        WHERE code_text IS NOT NULL
                    """)
                    for fn, avail in cur.fetchall():
                        if avail:
                            db_available_names.add(fn)
                            total_code_factors += 1
                        else:
                            db_disabled_names.add(fn)
                            total_disabled_factors += 1
            db_code_hashes = _get_current_factor_code_hashes(list(db_available_names))
        except Exception as e:
            logger.error(f"查询因子总数失败: {e}")
            raise HTTPException(status_code=500, detail=f"数据库查询失败: {e}")

        range_dist: Dict[str, int] = {}
        by_source: Dict[str, int] = {}

        # 细分缓存状态：cache_ok / cache_error / hash_mismatch / no_cache
        cache_ok = 0
        cache_error = 0
        hash_mismatch = 0
        disabled_cached = 0

        for fn in db_available_names:
            selected_cache = _choose_best_factor_cache_candidate(_collect_factor_cache_candidates(fn))
            entry = (selected_cache or {}).get("entry") or {}
            cached_hash = entry.get("source_hash_raw") or entry.get("source_hash")
            current_hash = db_code_hashes.get(fn)

            if selected_cache and selected_cache.get("valid_cache"):
                if current_hash and cached_hash and cached_hash != current_hash:
                    hash_mismatch += 1
                else:
                    cache_ok += 1
                dr = selected_cache.get("cache_date_range") or "unknown"
                range_dist[dr] = range_dist.get(dr, 0) + 1
                source_key = str(selected_cache.get("source_key") or "unknown")
                by_source[source_key] = by_source.get(source_key, 0) + 1
            elif selected_cache and selected_cache.get("cache_status") == "error":
                # _meta.json 明确记录了失败
                cache_error += 1
            # else: no_cache, 不计入

        # 禁用因子缓存统计
        for fn in db_disabled_names:
            selected_cache = _choose_best_factor_cache_candidate(_collect_factor_cache_candidates(fn))
            if selected_cache and selected_cache.get("valid_cache"):
                disabled_cached += 1
                dr = selected_cache.get("cache_date_range") or "unknown"
                range_dist[dr] = range_dist.get(dr, 0) + 1

        dominant_range = max(range_dist.items(), key=lambda x: x[1])[0] if range_dist else "unknown"

        return {
            "ok": True,
            "total_cached": cache_ok + hash_mismatch,
            "total_code_factors": total_code_factors,
            "coverage_pct": round((cache_ok + hash_mismatch) / total_code_factors * 100, 1) if total_code_factors > 0 else 0,
            "total_size_mb": round(total_size / 1024 / 1024, 1),
            "date_range_dominant": dominant_range,
            "date_range_distribution": dict(sorted(range_dist.items(), key=lambda x: -x[1])[:10]),
            "hash_ok": cache_ok,
            "hash_mismatch": hash_mismatch,
            "cache_error": cache_error,
            "no_cache": total_code_factors - cache_ok - hash_mismatch - cache_error,
            "disabled_total": total_disabled_factors,
            "disabled_cached": disabled_cached,
            "by_source": by_source,
            "last_generation": _load_cache_meta().get("generated_at"),
            "active_tasks": sum(1 for t in _active_cache_tasks.values() if t.get("status") == "running"),
        }
    except Exception as e:
        logger.exception("获取缓存统计失败")
        raise HTTPException(status_code=500, detail=str(e))


class FactorCacheComputeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    factor_names: Optional[List[str]] = Field(None, description="因子名列表；空/None = 全部可用因子")
    include_disabled: bool = Field(False, description="仅在显式 factor_names 时允许计算 disabled 因子；全量默认只计算启用因子")
    experiment_id: Optional[str] = Field(None, description="实验 ID；仅用于继承节点/数据目录配置，不限制缓存作用域")
    start_date: str = Field(..., description="起始日期；必须由 UI 显式传入")
    end_date: str = Field(..., description="结束日期；必须由 UI 显式传入")
    workers: int = Field(4, description="并行度: 2/4/8/10")
    timeout_per_factor: int = Field(1200, description="单因子超时秒数")
    force: bool = Field(False, description="强制重算（忽略已覆盖的缓存）")
    strict_backtest_data: bool = Field(True, description="严格使用 QE 默认历史 factor_data_dir 数据（用于全局因子值缓存）")
    auto_sync_remote: bool = Field(True, description="本地缓存计算成功后自动同步到远端执行节点")


class FactorCacheRemoteSyncRequest(BaseModel):
    node_id: Optional[str] = Field(None, description="远端节点 ID；空=同步所有远端节点")
    factor_names: Optional[List[str]] = Field(None, description="仅同步指定因子；空=同步全部有效本地缓存")
    force: bool = Field(False, description="强制同步，即使远端 meta 看起来已一致")
    configure_default_dir: bool = Field(True, description="当节点 factor_cache_dir 为空时写入默认远端缓存目录")
    upload_workers: int = Field(4, ge=1, le=16, description="流式上传并发度；默认 4，避免串行单因子同步占不满局域网")


@router.post("/factor-cache/compute", summary="Submit official offline factor full compute via WSL dispatch")
def factor_cache_compute(req: FactorCacheComputeRequest, background_tasks: BackgroundTasks):
    """Submit official offline factor cache + metrics compute to WSL/compute-node dispatch.

    Windows FastAPI is a control plane only: no local WSL shell-out and no
    obsolete local backfill execution. The worker consumes catalog
    code_text plus factor_data_dir/qlib/ST-PIT data and writes the single
    official cache under rdagent_assets/factor_values.
    """
    if req.workers < 1 or req.workers > 10:
        raise HTTPException(400, "workers must be 1~10")

    from ..services.quantevolver.config_composer import ConfigComposer
    from ..services.quantevolver.official_factor_full_compute_dispatch_service import (
        OfficialFactorFullComputeDispatchService,
    )

    cc = ConfigComposer()
    node_id = None
    resolved_start = req.start_date.strip()
    resolved_end = req.end_date.strip()

    if req.experiment_id:
        exp_record = cc._get_experiment_record(req.experiment_id)
        if not exp_record:
            raise HTTPException(404, f"experiment {req.experiment_id} not found")
        node_id = exp_record.get("node_id") or None

    rdagent_cfg = cc._fetch_workspace_config(node_id)
    factor_data_dir = rdagent_cfg.get("factor_data_dir")
    qlib_bin_path = rdagent_cfg.get("qlib_data_path") or os.getenv("QLIB_BIN_PATH")
    if req.strict_backtest_data and not factor_data_dir:
        raise HTTPException(400, "failed to resolve QE factor_data_dir")

    if not resolved_start or not resolved_end:
        raise HTTPException(400, "start_date/end_date must be provided by UI")
    try:
        datetime.strptime(resolved_start, "%Y-%m-%d")
        datetime.strptime(resolved_end, "%Y-%m-%d")
    except ValueError as e:
        raise HTTPException(400, "start_date/end_date must be YYYY-MM-DD") from e
    if resolved_start > resolved_end:
        raise HTTPException(400, f"invalid cache window: {resolved_start} > {resolved_end}")
    task_id = f"official_factor_full_{int(time.time() * 1000)}_{os.getpid()}"
    try:
        dispatch_result = OfficialFactorFullComputeDispatchService().submit(
            factor_names=req.factor_names,
            factor_data_dir=str(factor_data_dir or ""),
            start_date=resolved_start,
            end_date=resolved_end,
            include_disabled=bool(req.factor_names and req.include_disabled),
            batch_size=max(1, min(int(req.workers or 1) * 4, 32)),
            workers=req.workers,
            timeout_per_factor=req.timeout_per_factor,
            force=req.force,
            qlib_bin_path=qlib_bin_path,
            node_id=node_id,
            task_id=task_id,
        )
    except Exception as e:
        logger.exception("failed to submit official factor full-compute dispatch")
        raise HTTPException(status_code=500, detail=str(e)) from e

    dispatch_task_id = dispatch_result.get("dispatch_task_id") or dispatch_result.get("task_id") or task_id
    _active_cache_tasks[str(dispatch_task_id)] = {
        "task_id": str(dispatch_task_id),
        "dispatch_task_id": str(dispatch_task_id),
        "remote_task_id": dispatch_result.get("remote_task_id"),
        "node_id": dispatch_result.get("node_id") or node_id,
        "status": dispatch_result.get("status", "queued"),
        "started_at": datetime.now().isoformat(),
        "workers": req.workers,
        "batch_size": dispatch_result.get("payload", {}).get("batch_size"),
        "factor_count": len(req.factor_names) if req.factor_names else "all_enabled_code_text",
        "include_disabled": bool(req.factor_names and req.include_disabled),
        "experiment_id": req.experiment_id,
        "strict_backtest_data": req.strict_backtest_data,
        "data_source_mode": "official_offline_backtest_factor_data",
        "cache_source": "official_offline_backtest_factor_data",
        "code_source": "code_text",
        "factor_data_dir": factor_data_dir,
        "qlib_bin_path": qlib_bin_path,
        "window_train_start": resolved_start,
        "window_backtest_end": resolved_end,
        "cache_root": dispatch_result.get("cache_root"),
        "dispatch_payload": dispatch_result.get("payload"),
    }
    _invalidate_cache_meta()
    return {
        "ok": bool(dispatch_result.get("ok", True)),
        "task_id": str(dispatch_task_id),
        "dispatch_task_id": str(dispatch_task_id),
        "remote_task_id": dispatch_result.get("remote_task_id"),
        "status": dispatch_result.get("status", "queued"),
        "experiment_id": req.experiment_id,
        "window_train_start": resolved_start,
        "window_backtest_end": resolved_end,
        "include_disabled": bool(req.factor_names and req.include_disabled),
        "factor_data_dir": factor_data_dir,
        "qlib_bin_path": qlib_bin_path,
        "node_id": dispatch_result.get("node_id") or node_id,
        "cache_source": "official_offline_backtest_factor_data",
        "code_source": "code_text",
        "cache_root": dispatch_result.get("cache_root"),
        "message": "submitted official offline factor full-compute task to WSL/compute-node dispatch; Windows control plane does not execute local recompute.",
    }


@router.get("/factor-cache/compute-status/{task_id}", summary="查询计算任务状态")
def factor_cache_compute_status(task_id: str):
    """查询因子值计算任务状态 + 尾部日志。"""
    task = _active_cache_tasks.get(task_id)
    if not task:
        raise HTTPException(404, f"任务 {task_id} 不存在")

    log_path = Path(task.get("log_path", ""))
    recent_log = ""
    if log_path.exists():
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                recent_log = "".join(lines[-50:])
        except Exception as e:
            logger.warning(f"读取任务日志失败 {log_path}: {e}")

    result = None
    result_path = Path(task.get("result_path", ""))
    if task.get("status") in ("completed", "failed") and result_path.exists():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"读取任务结果失败 {result_path}: {e}")

    task_state = _load_json_file(Path(task.get("task_state_path", "")))
    failed_tail = _load_failed_tail(Path(task.get("failed_log_path", "")), limit=10)

    merged = dict(task)
    for key in ("experiment_id", "factor_data_dir", "data_source_mode", "window_train_start", "window_backtest_end", "strict_backtest_data"):
        if merged.get(key) is None and task_state and key in task_state:
            merged[key] = task_state.get(key)
        if merged.get(key) is None and result and key in result:
            merged[key] = result.get(key)

    return {**merged, "recent_log": recent_log, "result": result, "task_state": task_state, "failed_tail": failed_tail}


@router.get("/factor-cache/active-tasks", summary="当前所有缓存任务")
def factor_cache_active_tasks():
    return {"ok": True, "tasks": list(_active_cache_tasks.values())}


@router.get("/factor-cache/remote-stats", summary="远端因子值缓存同步统计")
def factor_cache_remote_stats(node_id: Optional[str] = Query(None, description="选中的远端节点 ID")):
    try:
        from ..services.quantevolver.factor_cache_remote_sync_service import (
            FactorCacheRemoteSyncService,
        )

        return FactorCacheRemoteSyncService().get_stats(node_id=node_id)
    except Exception as e:
        logger.exception("获取远端因子缓存统计失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor-cache/sync-to-node", summary="增量同步因子值缓存到远端节点")
def factor_cache_sync_to_node(req: FactorCacheRemoteSyncRequest):
    try:
        from ..services.quantevolver.factor_cache_remote_sync_service import (
            FactorCacheRemoteSyncService,
        )

        svc = FactorCacheRemoteSyncService()
        if req.node_id:
            job = svc.sync_to_node(
                req.node_id,
                req.factor_names,
                force=req.force,
                configure_default_dir=req.configure_default_dir,
                upload_workers=req.upload_workers,
            )
            return {"ok": job.get("status") == "completed", "job": job}
        result = svc.sync_to_all_remote_nodes(
            req.factor_names,
            force=req.force,
            configure_default_dir=req.configure_default_dir,
            upload_workers=req.upload_workers,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("远端因子缓存同步失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/factor-cache/all", summary="一键清空所有因子值缓存")
def factor_cache_clear_all():
    """删除 single/*.parquet + 重置 _meta.json。"""
    try:
        if not FACTOR_CACHE_SINGLE_DIR.exists():
            return {"ok": True, "deleted": 0}
        deleted = 0
        for fpath in FACTOR_CACHE_SINGLE_DIR.glob("*.parquet"):
            try:
                fpath.unlink()
                deleted += 1
            except Exception as e:
                logger.warning(f"删除 {fpath} 失败: {e}")
        # 重置 meta
        if FACTOR_CACHE_META_PATH.exists():
            FACTOR_CACHE_META_PATH.write_text(
                json.dumps({"factors": {}, "cleared_at": datetime.now().isoformat()}, indent=2),
                encoding="utf-8",
            )
        _invalidate_cache_meta()
        return {"ok": True, "deleted": deleted}
    except Exception as e:
        logger.exception("清空缓存失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/factor-cache/{factor_name}", summary="清除单个因子缓存")
def factor_cache_clear_one(factor_name: str):
    try:
        parquet_path = FACTOR_CACHE_SINGLE_DIR / f"{factor_name}.parquet"
        deleted = False
        if parquet_path.exists():
            parquet_path.unlink()
            deleted = True

        # 清理 meta 条目
        meta = _load_cache_meta(ttl_sec=0)
        if factor_name in meta.get("factors", {}):
            meta["factors"].pop(factor_name, None)
            FACTOR_CACHE_META_PATH.write_text(
                json.dumps(meta, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            _invalidate_cache_meta()
        return {"ok": True, "deleted": deleted, "factor_name": factor_name}
    except Exception as e:
        logger.exception(f"清除缓存失败 {factor_name}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 2 API: 单因子独立指标（17项）
# ============================================================

# ============================================================
# 手工因子入库 + 统一独立指标计算
# ============================================================

@router.get("/factors/manual/template", summary="获取因子代码模板")
async def get_factor_template(factor_name: str = "m_example_factor"):
    """返回标准因子代码模板，包含所有数据集字段说明。"""
    from ..services.manual_factor_service import ManualFactorService
    svc = ManualFactorService()
    return {"template": svc.get_template(factor_name)}


@router.post("/factors/manual/validate", summary="验证因子代码")
async def validate_manual_factor(req: ManualFactorValidate):
    """在 WSL 中执行因子代码，验证 result.h5 格式。~30s。"""
    from ..services.manual_factor_service import ManualFactorService
    svc = ManualFactorService()
    result = await svc.validate_factor_code(req.factor_name, req.code_text)
    return result


@router.post("/factors/manual", summary="手工因子入库")
async def create_manual_factor(req: ManualFactorCreate):
    """入库因子到 catalog + LLM 分类评级（不计算独立指标）。~10s。"""
    from ..services.manual_factor_service import ManualFactorService
    svc = ManualFactorService()
    result = await svc.save_factor(
        factor_name=req.factor_name,
        code_text=req.code_text,
        description=req.description,
        expression=req.expression,
    )
    return result


@router.post("/factors/manual/full-pipeline", summary="手工因子完整流水线")
async def manual_factor_full_pipeline(req: ManualFactorCreate):
    """验证 → 入库 → 计算独立指标 → LLM 分类评级。~3min。"""
    from ..services.manual_factor_service import ManualFactorService
    svc = ManualFactorService()
    result = await svc.full_pipeline(
        factor_name=req.factor_name,
        code_text=req.code_text,
        description=req.description,
        expression=req.expression,
        data_date=req.data_date or req.end_date,
        start_date=req.start_date,
        end_date=req.end_date,
    )
    return result


@router.post("/factors/batch-compute-metrics-unified", summary="统一批量独立指标计算")
async def batch_compute_metrics_unified(req: BatchComputeMetricsUnified):
    """Legacy 接口：转调 official evaluation writer。"""
    from ..services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService
    svc = FactorOfficialEvaluationService()
    result = await asyncio.to_thread(
        svc.compute,
        factor_names=req.factor_names,
        data_date=req.data_date or req.end_date or "",
        include_disabled=req.all_available,
        start_date=req.start_date,
        end_date=req.end_date,
    )
    return {
        **result,
        "deprecated": True,
        "official_api": "/api/v1/quantevolver/official-evaluation/compute",
    }


@router.post("/factors/batch-compute-metrics-stream", summary="流式批量独立指标计算（SSE）")
def batch_compute_metrics_stream(req: BatchComputeMetricsUnified):
    """Legacy SSE 接口：内部改走 official evaluation service。"""
    from starlette.responses import StreamingResponse

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\\n\\n"

    async def event_generator():
        yield _sse({
            "type": "stream_start",
            "deprecated": True,
            "official_api": "/api/v1/quantevolver/official-evaluation/compute",
            "data_date": req.data_date or req.end_date,
            "start_date": req.start_date,
            "end_date": req.end_date,
        })
        try:
            from ..services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService
            svc = FactorOfficialEvaluationService()
            result = await asyncio.to_thread(
                svc.compute,
                factor_names=req.factor_names,
                data_date=req.data_date or req.end_date or "",
                include_disabled=req.all_available,
                start_date=req.start_date,
                end_date=req.end_date,
            )
            yield _sse({"type": "stream_complete", **result, "deprecated": True})
        except Exception as e:
            yield _sse({"type": "error", "error": str(e), "deprecated": True})

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/official-evaluation/compute", summary="官方独立指标计算")
async def official_evaluation_compute(req: OfficialEvaluationComputeRequest):
    from ..services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService

    svc = FactorOfficialEvaluationService()
    return await asyncio.to_thread(
        svc.compute,
        factor_names=req.factor_names,
        data_date=req.data_date or req.end_date or "",
        include_disabled=req.include_disabled,
        max_workers=req.max_workers,
        timeout_per_factor=req.timeout_per_factor,
        start_date=req.start_date,
        end_date=req.end_date,
        force=req.force,
    )


@router.get("/official-evaluation/factors/{factor_name}", summary="查询官方独立指标")
def get_official_evaluation_factor_metrics(
    factor_name: str,
    eval_window: Optional[str] = Query(None, description="评估窗口: full/out_sample/recent_6m/recent_3m"),
    limit: int = Query(10, ge=1, le=50),
):
    from ..services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService

    svc = FactorOfficialEvaluationService()
    return svc.get_factor_metrics(
        factor_name=factor_name,
        eval_window=eval_window,
        limit=limit,
    )


@router.get("/official-evaluation/summary", summary="官方独立指标摘要")
def get_official_evaluation_summary():
    from ..services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService

    svc = FactorOfficialEvaluationService()
    return svc.get_summary()


@router.get("/official-evaluation/factors/{factor_name}/ic-decay", summary="官方独立指标衰变趋势")
def get_official_evaluation_ic_decay(
    factor_name: str,
    eval_window: str = "full",
):
    from ..db.pg_pool import get_conn
    from ..services.quantevolver.factor_official_evaluation_service import CALC_ENGINE

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    snapshot_date,
                    data_start,
                    data_end,
                    ic_mean,
                    rank_ic_mean,
                    icir,
                    rank_icir,
                    ic_positive_ratio,
                    n_trading_days,
                    rank_ic_1d,
                    rank_ic_5d,
                    rank_ic_10d,
                    rank_ic_20d,
                    top_annual_return,
                    top_excess_annual_return,
                    top_sharpe,
                    group_return_monotonicity,
                    calculated_at
                FROM aistock_factor_metrics
                WHERE factor_name = %s
                  AND eval_window = %s
                  AND calc_engine = %s
                  AND snapshot_date IS NOT NULL
                ORDER BY snapshot_date ASC
            """, (factor_name, eval_window, CALC_ENGINE))

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    trend = []
    for row in rows:
        d = dict(zip(columns, row))
        for k in ("snapshot_date", "data_start", "data_end", "calculated_at"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        trend.append(d)

    return {
        "factor_name": factor_name,
        "eval_window": eval_window,
        "count": len(trend),
        "trend": trend,
        "calc_engine": CALC_ENGINE,
    }


@router.get("/official-evaluation/factors/{factor_name}/monthly-ic", summary="因子月频IC衰退趋势")
def get_monthly_ic_series(
    factor_name: str,
    snapshot_date: Optional[str] = None,
):
    """获取因子月频 IC 时间序列（用于衰退趋势曲线展示）。

    返回按月聚合的 IC 均值 + 6 个月 EWMA 趋势线。
    """
    from ..db.pg_pool import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            if snapshot_date:
                cur.execute("""
                    SELECT month_end, ic_mean, rank_ic_mean, ic_std, ic_ewma_6m, n_days
                    FROM aistock_factor_monthly_ic
                    WHERE factor_name = %s AND snapshot_date = %s
                    ORDER BY month_end
                """, (factor_name, snapshot_date))
            else:
                # 取最新 snapshot_date 的数据
                cur.execute("""
                    SELECT month_end, ic_mean, rank_ic_mean, ic_std, ic_ewma_6m, n_days
                    FROM aistock_factor_monthly_ic
                    WHERE factor_name = %s
                      AND snapshot_date = (
                          SELECT MAX(snapshot_date) FROM aistock_factor_monthly_ic WHERE factor_name = %s
                      )
                    ORDER BY month_end
                """, (factor_name, factor_name))

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    if not rows:
        return {
            "factor_name": factor_name,
            "count": 0,
            "series": [],
            "message": "无月频IC数据，请先完成因子独立指标计算",
        }

    series = []
    for row in rows:
        rec = {}
        for col, val in zip(columns, row):
            # NaN 不是合法 JSON，转为 None
            if isinstance(val, float) and (val != val):  # NaN check
                rec[col] = None
            else:
                rec[col] = val
        series.append(rec)
    return {
        "factor_name": factor_name,
        "count": len(series),
        "series": series,
    }


# ============================================================
# 全流程批处理 SSE 端点
# ============================================================

@router.post("/factors/full-pipeline-stream")
def full_pipeline_stream(req: FullPipelineRequest):
    """因子全流程一键批处理（SSE 流式推送）。

    3 个阶段按顺序执行：
    1. IC 指标计算 — 通过 ManualFactorService.batch_compute_metrics 统一计算
    2. 因子代码改造 — 逐个因子同步执行 transform_factor（跳过已完成）
    3. LLM 分析分类 — 复用 batch_analyze_all_factors_async

    每步异常被捕获为 error 事件，不中断整体流程（幂等）。
    """
    import time as _time

    from ..db.pg_pool import get_conn
    from ..services.quantevolver.factor_analyst import FactorAnalyst
    from ..services.quantevolver.factor_transformation_service import FactorTransformationService

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

    def _get_factors_for_tasks(task_ids: list) -> list:
        """查询指定 task_ids 下的所有因子 [(factor_name, source)]"""
        if not task_ids:
            return []
        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(task_ids))
                cur.execute(f"""
                    SELECT factor_name, source
                    FROM aistock_factor_catalog
                    WHERE source_task_id IN ({placeholders})
                """, task_ids)
                return cur.fetchall()

    def _resolve_task_ids_from_factors(factor_names: list) -> list:
        """从因子名反查去重后的 source_task_id 列表。"""
        if not factor_names:
            return []
        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT DISTINCT source_task_id
                    FROM aistock_factor_catalog
                    WHERE factor_name IN ({placeholders})
                      AND source_task_id IS NOT NULL
                """, factor_names)
                return [row[0] for row in cur.fetchall()]

    def _get_pending_transforms(factor_rows: list) -> list:
        """过滤出尚未改造成功的因子"""
        if not factor_rows:
            return []
        names = [r[0] for r in factor_rows]
        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(names))
                cur.execute(f"""
                    SELECT factor_name
                    FROM aistock_factor_catalog
                    WHERE factor_name IN ({placeholders})
                      AND transformation_status = 'SUCCESS'
                """, names)
                done_set = {row[0] for row in cur.fetchall()}
        return [r for r in factor_rows if r[0] not in done_set]

    async def event_generator():
        t0 = _time.time()

        # 0) 收集因子名称（支持 factor_names 或 task_ids 输入）
        if req.factor_names:
            factor_names = req.factor_names
            # 反查 factor_rows 用于 Phase 2/3
            factor_rows = []
            with get_conn() as conn:
                with conn.cursor() as cur:
                    placeholders = ",".join(["%s"] * len(factor_names))
                    cur.execute(f"SELECT factor_name, source FROM aistock_factor_catalog WHERE factor_name IN ({placeholders})", factor_names)
                    factor_rows = cur.fetchall()
            task_ids = req.task_ids or []
        elif req.task_ids:
            task_ids = req.task_ids
            factor_rows = await asyncio.to_thread(_get_factors_for_tasks, task_ids)
            factor_names = [r[0] for r in factor_rows]
        else:
            yield _sse({"type": "error", "message": "需要提供 factor_names 或 task_ids"})
            return

        if not factor_names:
            yield _sse({"type": "error", "message": "未找到可处理的因子"})
            return

        phases = ["ic_metrics", "transform", "analyze"] if not req.skip_transform else ["ic_metrics", "analyze"]
        yield _sse({
            "type": "pipeline_start",
            "task_ids": task_ids,
            "factor_count": len(factor_names),
            "phases": phases,
            "start_date": req.start_date,
            "end_date": req.end_date or req.data_date,
        })

        # ── Phase 1: IC 指标计算（统一计算，不依赖 task） ──
        BATCH_SIZE = 10
        yield _sse({"type": "phase_start", "phase": "ic_metrics", "phase_label": "IC指标计算(统一)", "total_tasks": len(factor_names)})
        ic_success = 0
        ic_failed = 0
        total_inserted = 0

        from ..services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService
        svc_metrics = FactorOfficialEvaluationService()
        # 分批计算，每批推送进度
        for i in range(0, len(factor_names), BATCH_SIZE):
            batch = factor_names[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            yield _sse({"type": "task_progress", "phase": "ic_metrics", "status": "computing",
                        "batch": batch_num, "factors": batch, "current": i, "total": len(factor_names)})
            try:
                result = await asyncio.to_thread(
                    svc_metrics.compute,
                    factor_names=batch,
                    data_date=req.data_date or req.end_date or "",
                    start_date=req.start_date,
                    end_date=req.end_date,
                )
                if result.get("success"):
                    db_res = result.get("db_result", {})
                    batch_ok = len(result.get("eligible_factors", []))
                    batch_err = len(batch) - batch_ok - len(result.get("skipped_factors", []))
                    batch_saved = db_res.get("inserted", 0)
                else:
                    batch_ok = 0
                    batch_err = len(batch)
                    batch_saved = 0
                ic_success += batch_ok
                ic_failed += max(batch_err, 0)
                total_inserted += batch_saved
                yield _sse({"type": "task_progress", "phase": "ic_metrics", "status": "done",
                            "batch": batch_num, "ok": batch_ok, "failed": max(batch_err, 0),
                            "current": min(i + BATCH_SIZE, len(factor_names)), "total": len(factor_names)})
            except Exception as e:
                ic_failed += len(batch)
                yield _sse({"type": "task_progress", "phase": "ic_metrics", "status": "failed",
                            "batch": batch_num, "error": str(e),
                            "current": min(i + BATCH_SIZE, len(factor_names)), "total": len(factor_names)})

        yield _sse({
            "type": "phase_complete", "phase": "ic_metrics",
            "success": ic_success, "failed": ic_failed,
            "inserted": total_inserted, "skipped": 0,
            "elapsed": round(_time.time() - t0, 1),
        })

        # ── Phase 2: 因子代码改造 ──
        tf_success = 0
        tf_failed = 0
        if not req.skip_transform:
            t_phase2 = _time.time()
            if req.skip_completed:
                pending = await asyncio.to_thread(_get_pending_transforms, factor_rows)
            else:
                pending = list(factor_rows)
            skipped_count = len(factor_rows) - len(pending)
            yield _sse({
                "type": "phase_start", "phase": "transform", "phase_label": "因子代码改造",
                "total": len(factor_rows), "pending": len(pending), "skipped": skipped_count,
            })
            TRANSFORM_CONCURRENCY = 3
            svc = FactorTransformationService()
            tf_sem = asyncio.Semaphore(TRANSFORM_CONCURRENCY)
            tf_queue: asyncio.Queue = asyncio.Queue()

            async def _tf_worker(fname: str, fsource: str):
                async with tf_sem:
                    ft0 = _time.time()
                    try:
                        result = await asyncio.to_thread(
                            svc.transform_factor,
                            factor_name=fname,
                            factor_source=fsource,
                            max_llm_retries=req.max_transform_retries,
                        )
                        elapsed = round(_time.time() - ft0, 1)
                        await tf_queue.put(("ok", fname, result, elapsed))
                    except Exception as e:
                        elapsed = round(_time.time() - ft0, 1)
                        await tf_queue.put(("error", fname, e, elapsed))

            tf_tasks = [asyncio.create_task(_tf_worker(fn, fs)) for fn, fs in pending]
            tf_done_count = 0

            while tf_done_count < len(pending):
                status_flag, fname, payload, elapsed = await tf_queue.get()
                tf_done_count += 1
                if status_flag == "ok":
                    result = payload
                    status = result.get("status", "").upper()
                    if status == "SUCCESS":
                        tf_success += 1
                        yield _sse({"type": "factor_progress", "phase": "transform", "factor_name": fname, "status": "success", "current": tf_done_count, "total": len(pending), "elapsed": elapsed})
                    else:
                        tf_failed += 1
                        yield _sse({"type": "factor_progress", "phase": "transform", "factor_name": fname, "status": "failed",
                                    "error": result.get("error", status), "current": tf_done_count, "total": len(pending), "elapsed": elapsed})
                else:
                    tf_failed += 1
                    yield _sse({"type": "factor_progress", "phase": "transform", "factor_name": fname, "status": "failed",
                                "error": str(payload), "current": tf_done_count, "total": len(pending), "elapsed": elapsed})

            await asyncio.gather(*tf_tasks, return_exceptions=True)
            yield _sse({
                "type": "phase_complete", "phase": "transform",
                "success": tf_success, "failed": tf_failed, "skipped": skipped_count,
                "elapsed": round(_time.time() - t_phase2, 1),
            })

        # ── Phase 3: LLM 分析分类 ──
        t_phase3 = _time.time()
        yield _sse({"type": "phase_start", "phase": "analyze", "phase_label": "LLM分析分类", "total": len(factor_names)})
        fa = FactorAnalyst()
        an_success = 0
        an_failed = 0
        try:
            async for event in fa.batch_analyze_all_factors_async(
                use_llm=True,
                factor_names=factor_names,
            ):
                if event.get("type") == "progress":
                    an_success += 1
                    yield _sse({
                        "type": "factor_progress", "phase": "analyze",
                        "factor_name": event.get("factor_name", ""),
                        "status": "done",
                        "current": event.get("current", an_success),
                        "total": event.get("total", len(factor_names)),
                    })
                elif event.get("type") == "error":
                    an_failed += 1
                    yield _sse({
                        "type": "factor_progress", "phase": "analyze",
                        "factor_name": event.get("factor_name", ""),
                        "status": "failed",
                        "error": event.get("error", ""),
                        "current": an_success + an_failed,
                        "total": event.get("total", len(factor_names)),
                    })
                # "done" 事件由 batch_analyze 内部发出，跳过
        except Exception as e:
            logger.exception("全流程 Phase 3 分析失败")
            yield _sse({"type": "error", "phase": "analyze", "message": str(e)})
        yield _sse({
            "type": "phase_complete", "phase": "analyze",
            "success": an_success, "failed": an_failed,
            "elapsed": round(_time.time() - t_phase3, 1),
        })

        # ── 流程结束 ──
        yield _sse({
            "type": "pipeline_complete",
            "total_time": round(_time.time() - t0, 1),
            "summary": {
                "ic_inserted": total_inserted,
                "transform_success": tf_success,
                "transform_failed": tf_failed,
                "analyze_success": an_success,
                "analyze_failed": an_failed,
            },
        })

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/factors/{factor_name}/independent-metrics")
def get_factor_independent_metrics(
    factor_name: str,
    eval_window: Optional[str] = Query(None, description="评估窗口: full/out_sample/recent_6m/recent_3m"),
    limit: int = Query(10, ge=1, le=50),
):
    """Legacy 接口：只返回 official evaluation 指标。"""
    from ..services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService

    svc = FactorOfficialEvaluationService()
    result = svc.get_factor_metrics(
        factor_name=factor_name,
        eval_window=eval_window,
        limit=limit,
    )
    result["deprecated"] = True
    result["official_api"] = f"/api/v1/quantevolver/official-evaluation/factors/{factor_name}"
    return result


@router.get("/factors/independent-metrics-summary")
def get_independent_metrics_summary():
    """Legacy 接口：只返回 official evaluation summary。"""
    from ..services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService

    svc = FactorOfficialEvaluationService()
    result = svc.get_summary()
    result["deprecated"] = True
    result["official_api"] = "/api/v1/quantevolver/official-evaluation/summary"
    return result


# ============================================================
# Phase 2 API: 因子实验表现查询 & 实验交易统计
# ============================================================

@router.get("/factors/{factor_name}/experiment-metrics")
def get_factor_experiment_metrics(
    factor_name: str,
    source: Optional[str] = Query(None, description="因子来源过滤"),
    limit: int = Query(20, ge=1, le=100),
    order_by: str = Query("collected_at", description="排序字段: collected_at / ic / ann_return_no_cost"),
):
    """查询因子的历史实验表现指标。"""
    try:
        valid_order = {
            "collected_at": "collected_at DESC",
            "ic": "ic DESC NULLS LAST",
            "ann_return_no_cost": "ann_return_no_cost DESC NULLS LAST",
            "sharpe_ratio": "sharpe_ratio DESC NULLS LAST",
        }
        order_clause = valid_order.get(order_by, "collected_at DESC")

        with get_conn() as conn:
            with conn.cursor() as cur:
                where = "WHERE factor_name = %s"
                params = [factor_name]
                if source:
                    where += " AND factor_source = %s"
                    params.append(source)

                # 查询指标列表
                cur.execute(f"""
                    SELECT id, factor_name, factor_source, experiment_id, experiment_name,
                           ic, icir, rank_ic, rank_icir,
                           ann_return_no_cost, info_ratio_no_cost, max_drawdown_no_cost,
                           ann_return_with_cost, info_ratio_with_cost, max_drawdown_with_cost,
                           daily_win_rate, weekly_win_rate, max_consecutive_win, max_consecutive_loss,
                           total_trades, winning_trades, losing_trades, stock_win_rate,
                           avg_profit_pct, avg_loss_pct, profit_loss_ratio,
                           max_single_profit_pct, max_single_loss_pct,
                           sharpe_ratio, calmar_ratio, avg_turnover, total_trading_days,
                           model_id, other_factors, data_split, collected_at
                    FROM qe_factor_experiment_metrics
                    {where}
                    ORDER BY {order_clause}
                    LIMIT %s
                """, params + [limit])
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]

                # 查询汇总统计
                cur.execute(f"""
                    SELECT
                        COUNT(*) as experiment_count,
                        AVG(ic) as avg_ic,
                        MAX(ic) as best_ic,
                        MIN(ic) as worst_ic,
                        AVG(ann_return_no_cost) as avg_ann_return,
                        AVG(daily_win_rate) as avg_daily_win_rate,
                        AVG(sharpe_ratio) as avg_sharpe,
                        AVG(stock_win_rate) as avg_stock_win_rate,
                        AVG(profit_loss_ratio) as avg_profit_loss_ratio
                    FROM qe_factor_experiment_metrics
                    {where}
                """, params)
                summary_row = cur.fetchone()
                summary_cols = [d[0] for d in cur.description]
                summary = dict(zip(summary_cols, summary_row)) if summary_row else {}

        # 序列化datetime
        for row in rows:
            for k, v in row.items():
                if hasattr(v, 'isoformat'):
                    row[k] = v.isoformat()
        for k, v in summary.items():
            if hasattr(v, 'isoformat'):
                summary[k] = v.isoformat()
            elif isinstance(v, float):
                summary[k] = round(v, 6)

        return {
            "ok": True,
            "factor_name": factor_name,
            "total": summary.get("experiment_count", 0),
            "metrics": rows,
            "summary": summary,
        }
    except Exception as e:
        logger.exception("查询因子实验表现失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/trade-stats")
def get_experiment_trade_stats(experiment_id: str):
    """查询实验的交易统计数据。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 从qe_factor_experiment_metrics取第一条（同实验的交易统计相同）
                cur.execute("""
                    SELECT
                        experiment_id, experiment_name,
                        daily_win_rate, weekly_win_rate,
                        max_consecutive_win, max_consecutive_loss,
                        total_trades, winning_trades, losing_trades, stock_win_rate,
                        avg_profit_pct, avg_loss_pct, profit_loss_ratio,
                        max_single_profit_pct, max_single_loss_pct,
                        sharpe_ratio, calmar_ratio, avg_turnover, total_trading_days,
                        ic, icir, rank_ic, rank_icir,
                        ann_return_no_cost, max_drawdown_no_cost,
                        ann_return_with_cost, max_drawdown_with_cost
                    FROM qe_factor_experiment_metrics
                    WHERE experiment_id = %s
                    LIMIT 1
                """, (experiment_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="实验交易统计不存在")
                cols = [d[0] for d in cur.description]
                result = dict(zip(cols, row))

        for k, v in result.items():
            if hasattr(v, 'isoformat'):
                result[k] = v.isoformat()

        return {"ok": True, **result}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("查询实验交易统计失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 2 API: ModelAnalyst（模型分析）
# ============================================================

class AnalyzeModelRequest(BaseModel):
    model_id: str = Field(..., description="模型ID")
    use_llm: bool = Field(False, description="是否使用LLM生成描述")


class BatchAnalyzeModelsRequest(BaseModel):
    use_llm: bool = Field(False, description="是否使用LLM")
    model_ids: Optional[list] = Field(None, description="指定模型ID列表，为空则分析全部")


@router.post("/model-analyst/analyze")
async def analyze_model(req: AnalyzeModelRequest):
    """分析单个模型，生成描述。"""
    try:
        from ..services.quantevolver.model_analyst import ModelAnalyst
        ma = ModelAnalyst()
        result = await asyncio.to_thread(
            ma.analyze_single_model,
            model_id=req.model_id,
            use_llm=req.use_llm,
        )
        if not result.get("ok"):
            raise HTTPException(status_code=404, detail=result.get("error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"分析模型失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/model-analyst/batch-analyze")
async def batch_analyze_models(req: BatchAnalyzeModelsRequest):
    """批量分析模型（同步版本）。"""
    try:
        from ..services.quantevolver.model_analyst import ModelAnalyst
        ma = ModelAnalyst()
        result = await asyncio.to_thread(ma.batch_analyze_all_models, use_llm=req.use_llm, model_ids=req.model_ids)
        return {"ok": True, **result}
    except Exception as e:
        logger.exception(f"批量分析模型失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/model-analyst/batch-analyze-stream")
def batch_analyze_models_stream(req: BatchAnalyzeModelsRequest):
    """批量分析模型（SSE流式版本）。"""
    from ..services.quantevolver.model_analyst import ModelAnalyst

    async def event_generator():
        ma = ModelAnalyst()
        try:
            async for event in ma.batch_analyze_all_models_async(
                use_llm=req.use_llm,
                model_ids=req.model_ids,
            ):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.exception("批量模型分析失败")
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


# ============================================================
# Phase 2 API: PromptManager（提示词管理）
# ============================================================

class CreatePromptRequest(BaseModel):
    agent_type: str = Field(..., description="Agent类型: factor_analyst / portfolio_architect")
    prompt_key: str = Field(..., description="提示词唯一键")
    display_name: str = Field(..., description="显示名称")
    description: Optional[str] = Field(None, description="描述")
    system_prompt: str = Field("", description="系统提示词")
    user_prompt_template: str = Field("", description="用户提示词模板")


class UpdatePromptRequest(BaseModel):
    system_prompt: Optional[str] = None
    user_prompt_template: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None


@router.get("/prompts")
def list_prompts(
    agent_type: Optional[str] = Query(None, description="按Agent类型筛选"),
    active_only: bool = Query(True, description="仅返回激活的提示词"),
):
    """获取提示词列表。"""
    try:
        from ..services.quantevolver.prompt_manager import PromptManager
        pm = PromptManager()
        return pm.list_prompts(agent_type=agent_type, active_only=active_only)
    except Exception as e:
        logger.exception("获取提示词列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/{agent_type}/{prompt_key}")
def get_prompt(agent_type: str, prompt_key: str):
    """获取指定提示词。"""
    try:
        from ..services.quantevolver.prompt_manager import PromptManager
        pm = PromptManager()
        result = pm.get_prompt(agent_type, prompt_key)
        if not result.get("ok"):
            raise HTTPException(status_code=404, detail=result.get("error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取提示词失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/prompts/{agent_type}/{prompt_key}")
def update_prompt(agent_type: str, prompt_key: str, req: UpdatePromptRequest):
    """更新提示词。"""
    try:
        from ..services.quantevolver.prompt_manager import PromptManager
        pm = PromptManager()
        result = pm.update_prompt(
            agent_type=agent_type,
            prompt_key=prompt_key,
            system_prompt=req.system_prompt,
            user_prompt_template=req.user_prompt_template,
            display_name=req.display_name,
            description=req.description,
            is_active=req.is_active,
        )
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新提示词失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/prompts")
def create_prompt(req: CreatePromptRequest):
    """创建新提示词。"""
    try:
        from ..services.quantevolver.prompt_manager import PromptManager
        pm = PromptManager()
        result = pm.create_prompt(
            agent_type=req.agent_type,
            prompt_key=req.prompt_key,
            display_name=req.display_name,
            system_prompt=req.system_prompt,
            user_prompt_template=req.user_prompt_template,
            description=req.description or "",
        )
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("创建提示词失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/prompts/{prompt_id}")
def delete_prompt(prompt_id: int):
    """删除提示词。"""
    try:
        from ..services.quantevolver.prompt_manager import PromptManager
        pm = PromptManager()
        result = pm.delete_prompt(prompt_id)
        if not result.get("ok"):
            raise HTTPException(status_code=404, detail=result.get("error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("删除提示词失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 2 API: LLM模型配置
# ============================================================

@router.get("/llm-models")
def get_available_llm_models():
    """获取可用的LLM模型列表（从AIstock LLM数据库读取）"""
    models = []
    
    try:
        from ..db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.id, m.full_model_id, m.display_name, m.model_type, m.model_category, p.provider_name
                    FROM aistock_llm_models m
                    JOIN aistock_llm_providers p ON m.provider_id = p.id
                    WHERE m.is_active = true
                      AND COALESCE(m.model_type, 'chat') <> 'embedding'
                    ORDER BY p.provider_name, m.display_name
                """)
                for row in cur.fetchall():
                    # model_id_db, full_model_id, display_name, model_type, model_category, provider_name
                    models.append({
                        "id": str(row[0]),  # 存储数据库的主键ID，以便后续关联
                        "name": f"{row[2]} ({row[5]})",
                        "full_model_id": row[1],
                        "model_type": row[3],
                        "model_category": row[4],
                        "source": "aistock_db"
                    })
    except Exception:
        logger.exception("获取可用LLM模型列表失败")
        
    return {"ok": True, "models": models}


class SaveAgentModelRequest(BaseModel):
    agent_type: str = Field(..., description="Agent类型")
    model_id: str = Field(..., description="选择的LLM模型ID")


@router.post("/agent-model-config")
def save_agent_model_config(req: SaveAgentModelRequest):
    """保存agent的LLM模型配置。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_agent_model_config (agent_type, model_id, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (agent_type) DO UPDATE SET
                        model_id = EXCLUDED.model_id,
                        updated_at = NOW()
                """, (req.agent_type, req.model_id))
        return {"ok": True, "agent_type": req.agent_type, "model_id": req.model_id}
    except Exception as e:
        logger.exception("保存agent模型配置失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/agent-model-config")
def get_agent_model_configs():
    """获取所有agent的LLM模型配置（含提示词）。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_type, model_id, display_name, description, system_prompt, updated_at
                    FROM qe_agent_model_config
                    ORDER BY agent_type
                """)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        for row in rows:
            if row.get("updated_at") and hasattr(row["updated_at"], "isoformat"):
                row["updated_at"] = row["updated_at"].isoformat()
        return {"ok": True, "agents": rows}
    except Exception as e:
        logger.warning(f"获取agent配置失败（表可能不存在）: {e}")
        return {"ok": True, "agents": []}


class AgentPromptRequest(BaseModel):
    agent_type: str
    model_id: Optional[str] = None
    system_prompt: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None


@router.post("/agent-model-config/prompt")
def save_agent_prompt(req: AgentPromptRequest):
    """保存agent的提示词和模型配置。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                set_parts = ["updated_at = NOW()"]
                params = []
                if req.model_id is not None:
                    set_parts.append("model_id = %s")
                    params.append(req.model_id)
                if req.system_prompt is not None:
                    set_parts.append("system_prompt = %s")
                    params.append(req.system_prompt)
                if req.display_name is not None:
                    set_parts.append("display_name = %s")
                    params.append(req.display_name)
                if req.description is not None:
                    set_parts.append("description = %s")
                    params.append(req.description)
                params.append(req.agent_type)
                cur.execute(
                    f"UPDATE qe_agent_model_config SET {', '.join(set_parts)} WHERE agent_type = %s",
                    params
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """INSERT INTO qe_agent_model_config
                               (agent_type, model_id, system_prompt, display_name, description, updated_at)
                           VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (req.agent_type, req.model_id or "", req.system_prompt,
                         req.display_name, req.description)
                    )
        return {"ok": True, "agent_type": req.agent_type}
    except Exception as e:
        logger.exception("保存agent提示词失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 因子改造 API
# ============================================================

class FactorTransformRequest(BaseModel):
    factor_name: str
    factor_source: str = "rdagent_sota"
    max_llm_retries: int = 3
    llm_model_id: Optional[str] = None
    test_instruments: Optional[list] = None
    test_start_date: str = "2022-01-01"
    test_end_date: str = "2026-04-28"


class BatchTransformRequest(BaseModel):
    factor_names: Optional[list] = None
    factor_source: Optional[str] = None
    max_llm_retries: int = 3
    llm_model_id: Optional[str] = None
    only_pending: bool = True


@router.post("/factor-transformation/transform")
def transform_factor(req: FactorTransformRequest, background_tasks: BackgroundTasks):
    """
    触发单个因子的改造工作流。

    工作流：规则转换 -> 编译测试 -> 执行测试 -> LLM修复（如需）-> LLM审核 -> 保存
    """
    try:
        from ..services.quantevolver.factor_transformation_service import FactorTransformationService
        svc = FactorTransformationService()

        def run_transform():
            svc.transform_factor(
                factor_name=req.factor_name,
                factor_source=req.factor_source,
                max_llm_retries=req.max_llm_retries,
                llm_model_id=req.llm_model_id,
                test_instruments=req.test_instruments,
                test_start_date=req.test_start_date,
                test_end_date=req.test_end_date,
            )

        background_tasks.add_task(run_transform)
        return {"ok": True, "message": f"因子改造任务已提交: {req.factor_name}", "factor_name": req.factor_name}
    except Exception as e:
        logger.exception("触发因子改造失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor-transformation/transform-sync")
def transform_factor_sync(req: FactorTransformRequest):
    """
    同步执行单个因子的改造工作流（等待完成后返回结果）。
    适合小批量测试使用。
    """
    try:
        from ..services.quantevolver.factor_transformation_service import FactorTransformationService
        svc = FactorTransformationService()
        result = svc.transform_factor(
            factor_name=req.factor_name,
            factor_source=req.factor_source,
            max_llm_retries=req.max_llm_retries,
            llm_model_id=req.llm_model_id,
            test_instruments=req.test_instruments,
            test_start_date=req.test_start_date,
            test_end_date=req.test_end_date,
        )
        return result
    except Exception as e:
        logger.exception("同步因子改造失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor-transformation/batch-transform")
def batch_transform_factors(req: BatchTransformRequest, background_tasks: BackgroundTasks):
    """
    批量触发因子改造工作流（后台异步执行）。
    """
    try:
        from ..services.quantevolver.factor_transformation_service import FactorTransformationService
        svc = FactorTransformationService()

        def run_batch():
            svc.batch_transform(
                factor_names=req.factor_names,
                factor_source=req.factor_source,
                max_llm_retries=req.max_llm_retries,
                llm_model_id=req.llm_model_id,
                only_pending=req.only_pending,
            )

        background_tasks.add_task(run_batch)
        return {"ok": True, "message": "批量因子改造任务已提交，正在后台执行"}
    except Exception as e:
        logger.exception("触发批量因子改造失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-transformation/status")
def get_factor_transformation_status(
    factor_source: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
):
    """
    获取因子改造状态列表（从 aistock_factor_catalog 查询）。
    """
    try:
        from ..services.quantevolver.factor_transformation_service import FactorTransformationService
        svc = FactorTransformationService()
        return svc.get_factor_transformation_status(
            factor_source=factor_source,
            limit=limit,
            offset=offset,
        )
    except Exception as e:
        logger.exception("获取因子改造状态失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-transformation/jobs")
def list_transformation_jobs(
    factor_name: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    """
    列出因子改造任务记录。
    """
    try:
        from ..services.quantevolver.factor_transformation_service import FactorTransformationService
        svc = FactorTransformationService()
        return svc.list_jobs(factor_name=factor_name, status=status, limit=limit, offset=offset)
    except Exception as e:
        logger.exception("获取改造任务列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-transformation/jobs/{job_id}")
def get_transformation_job(job_id: str):
    """
    获取指定改造任务的详细状态。
    """
    try:
        from ..services.quantevolver.factor_transformation_service import FactorTransformationService
        svc = FactorTransformationService()
        result = svc.get_job_status(job_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"任务不存在: {job_id}")
        return {"ok": True, "job": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取改造任务详情失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-transformation/jobs/{job_id}/progress")
def get_transformation_job_progress(job_id: str):
    """
    获取指定改造任务的完整进度日志（用于前端实时轮询）。
    返回结构化的步骤列表，每个步骤包含状态和日志信息。
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT job_id, factor_name, factor_source, status,
                              rule_transform_result, compile_test_result,
                              execution_test_result, llm_repair_attempts,
                              analysis_result, error_message,
                              llm_retry_count, max_llm_retries,
                              created_at, started_at, completed_at, updated_at
                       FROM qe_factor_transformation_jobs
                       WHERE job_id = %s""",
                    (job_id,)
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"任务不存在: {job_id}")
                cols = [d[0] for d in cur.description]
                job = dict(zip(cols, row))

        for k, v in job.items():
            if hasattr(v, 'isoformat'):
                job[k] = v.isoformat()

        # 构建步骤列表
        STEP_ORDER = [
            "RULE_TRANSFORMING", "COMPILE_TESTING", "EXECUTION_TESTING",
            "LLM_REPAIRING", "ANALYSIS_REVIEWING", "SUCCESS", "FAILED",
        ]
        current_status = job.get("status", "PENDING")

        def step_state(step_name: str) -> str:
            """根据当前状态判断步骤状态: done/active/pending/failed"""
            if current_status == "FAILED":
                idx_cur = STEP_ORDER.index(current_status) if current_status in STEP_ORDER else -1
                idx_step = STEP_ORDER.index(step_name) if step_name in STEP_ORDER else -1
                if idx_step < idx_cur:
                    return "done"
                if step_name == current_status:
                    return "failed"
                return "pending"
            if current_status == "SUCCESS":
                return "done"
            idx_cur = STEP_ORDER.index(current_status) if current_status in STEP_ORDER else -1
            idx_step = STEP_ORDER.index(step_name) if step_name in STEP_ORDER else -1
            if idx_step < idx_cur:
                return "done"
            if idx_step == idx_cur:
                return "active"
            return "pending"

        rule_result = job.get("rule_transform_result") or {}
        compile_result = job.get("compile_test_result") or {}
        exec_result = job.get("execution_test_result") or {}
        llm_attempts = job.get("llm_repair_attempts") or []
        analysis = job.get("analysis_result") or {}

        def _build_exec_logs(er: dict) -> list:
            """构建执行测试步骤的日志，包含原始vs改造后因子值对比"""
            logs = []
            if er.get("success"):
                logs.append("执行测试通过，因子函数运行正常")
            # 改造后因子值样本
            rs = er.get("result_sample")
            if rs and isinstance(rs, list):
                logs.append("── 改造后因子值（前10行）──")
                for row in rs[:10]:
                    parts = [f"{k}={v}" for k, v in row.items()]
                    logs.append("  " + " | ".join(parts))
            # 原始因子值样本
            orig = er.get("original_sample")
            orig_err = er.get("original_error")
            if orig and isinstance(orig, list):
                logs.append("── 原始因子值（前10行，h5文件方式）──")
                for row in orig[:10]:
                    parts = [f"{k}={v}" for k, v in row.items()]
                    logs.append("  " + " | ".join(parts))
            elif orig_err:
                logs.append(f"── 原始因子执行失败（不影响改造结论）: {orig_err[:200]}")
            return logs

        steps = [
            {
                "key": "RULE_TRANSFORMING",
                "label": "规则转换",
                "state": step_state("RULE_TRANSFORMING"),
                "logs": rule_result.get("changes", []) + rule_result.get("warnings", []),
                "error": rule_result.get("error"),
                "success": rule_result.get("success"),
            },
            {
                "key": "COMPILE_TESTING",
                "label": "编译测试",
                "state": step_state("COMPILE_TESTING"),
                "logs": ["编译测试通过"] if compile_result.get("success") else [],
                "error": compile_result.get("error"),
                "success": compile_result.get("success"),
            },
            {
                "key": "EXECUTION_TESTING",
                "label": "执行测试",
                "state": step_state("EXECUTION_TESTING"),
                "logs": _build_exec_logs(exec_result),
                "error": exec_result.get("error"),
                "success": exec_result.get("success"),
                "result_sample": exec_result.get("result_sample"),
                "original_sample": exec_result.get("original_sample"),
                "original_error": exec_result.get("original_error"),
            },
            {
                "key": "LLM_REPAIRING",
                "label": f"LLM修复 ({job.get('llm_retry_count', 0)}/{job.get('max_llm_retries', 3)})",
                "state": step_state("LLM_REPAIRING") if llm_attempts else "skipped",
                "logs": [
                    f"第{a.get('attempt')}次尝试: {a.get('result', '')} "
                    f"(错误类型: {a.get('error_type', '')})"
                    + (f" | 编译: {'✓' if a.get('compile_ok') else '✗'}" if 'compile_ok' in a else "")
                    + (f" | 执行: {'✓' if a.get('exec_ok') else '✗'}" if 'exec_ok' in a else "")
                    for a in llm_attempts
                ],
                "error": None,
                "success": any(a.get("result") == "修复成功" for a in llm_attempts) if llm_attempts else None,
            },
            {
                "key": "ANALYSIS_REVIEWING",
                "label": "AI审核",
                "state": step_state("ANALYSIS_REVIEWING"),
                "logs": (
                    [
                        f"审核结论: {'通过' if analysis.get('approved') else '未通过'}",
                        f"置信度: {analysis.get('confidence', 'N/A')}",
                        f"理由: {analysis.get('reason', '')}",
                    ] + [f"问题: {i}" for i in (analysis.get("issues") or [])]
                    + [f"建议: {s}" for s in (analysis.get("suggestions") or [])]
                ) if analysis else [],
                "error": None if analysis.get("approved", True) else analysis.get("reason"),
                "success": analysis.get("approved"),
            },
        ]

        return {
            "ok": True,
            "job_id": job_id,
            "factor_name": job.get("factor_name"),
            "factor_source": job.get("factor_source"),
            "status": current_status,
            "error_message": job.get("error_message"),
            "llm_retry_count": job.get("llm_retry_count", 0),
            "max_llm_retries": job.get("max_llm_retries", 3),
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "completed_at": job.get("completed_at"),
            "updated_at": job.get("updated_at"),
            "steps": steps,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取改造任务进度失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-transformation/factor/{factor_name}/code")
def get_factor_realtime_code(factor_name: str, source: str = "rdagent_task_sync"):
    """
    获取指定因子的改造后实时代码及原始代码。
    改造后代码优先从文件系统 qe_code_path 读取（权威数据源）；
    原始代码优先从文件系统 asset_path 读取（权威数据源）。
    数据库中的 realtime_code_text / code_text 仅作展示兜底，不作为改造依据。
    """
    import os
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT factor_name, source, transformation_status,
                              last_transformation_at, qe_code_path, asset_path
                       FROM aistock_factor_catalog
                       WHERE factor_name = %s AND source = %s
                       LIMIT 1""",
                    (factor_name, source)
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"因子不存在: {factor_name}")
                cols = [d[0] for d in cur.description]
                result = dict(zip(cols, row))
                for k, v in result.items():
                    if hasattr(v, 'isoformat'):
                        result[k] = v.isoformat()

        project_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)
        )))

        def _read_file_from_path(rel_or_abs: str) -> tuple:
            """读取文件内容，返回 (content, abs_path, error)"""
            if not rel_or_abs:
                return None, None, "路径为空"
            abs_path = rel_or_abs if os.path.isabs(rel_or_abs) else os.path.join(project_root, rel_or_abs)
            if not os.path.exists(abs_path):
                return None, abs_path, f"文件不存在: {abs_path}"
            try:
                with open(abs_path, "r", encoding="utf-8") as f:
                    content = f.read()
                return content, abs_path, None
            except (OSError, UnicodeDecodeError) as e:
                return None, abs_path, str(e)

        # 从文件系统读取改造后代码（权威数据源）
        transformed_code, transformed_abs, transformed_err = _read_file_from_path(result.get("qe_code_path"))
        # 从文件系统读取原始代码（权威数据源）
        original_code, original_abs, original_err = _read_file_from_path(result.get("asset_path"))

        result["realtime_code_text"] = transformed_code
        result["code_text"] = original_code
        result["_transformed_code_source"] = "filesystem" if transformed_code else "none"
        result["_original_code_source"] = "filesystem" if original_code else "none"
        if transformed_err:
            result["_transformed_code_error"] = transformed_err
        if original_err:
            result["_original_code_error"] = original_err

        return {"ok": True, "factor": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取因子实时代码失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor-transformation/factor/{factor_name}/reset")
def reset_factor_transformation(factor_name: str, source: str = "rdagent_sota"):
    """
    重置指定因子的改造状态为 PENDING，允许重新改造。
    重置时清空 realtime_code_text 和 qe_code_path（改造相关字段）。
    严禁修改 asset_path 和 code_text（原始因子源代码不可修改）。
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE aistock_factor_catalog
                       SET transformation_status = 'PENDING',
                           realtime_code_text = NULL,
                           qe_code_path = NULL,
                           last_transformation_at = NULL
                       WHERE factor_name = %s AND source = %s""",
                    (factor_name, source)
                )
                affected = cur.rowcount
        if affected == 0:
            raise HTTPException(status_code=404, detail=f"因子不存在: {factor_name}")
        return {"ok": True, "message": f"因子 {factor_name} 改造状态已重置为 PENDING，可重新改造"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("重置因子改造状态失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-transformation/stats")
def get_transformation_stats():
    """
    获取因子改造统计数据（各状态数量）。
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(CASE WHEN transformation_status = 'SUCCESS' THEN 1 END) AS success,
                        COUNT(CASE WHEN transformation_status = 'FAILED' THEN 1 END) AS failed,
                        COUNT(CASE WHEN transformation_status = 'PENDING' OR transformation_status IS NULL THEN 1 END) AS pending,
                        COUNT(CASE WHEN transformation_status NOT IN ('SUCCESS', 'FAILED', 'PENDING') AND transformation_status IS NOT NULL THEN 1 END) AS in_progress,
                        COUNT(CASE WHEN asset_path IS NOT NULL AND asset_path != '' THEN 1 END) AS has_original_code,
                        COUNT(CASE WHEN qe_code_path IS NOT NULL AND qe_code_path != '' THEN 1 END) AS has_realtime_code
                    FROM aistock_factor_catalog
                """)
                row = cur.fetchone()
                cols = [d[0] for d in cur.description]
                stats = dict(zip(cols, row))

                # 最近任务统计
                cur.execute("""
                    SELECT status, COUNT(*) AS cnt
                    FROM qe_factor_transformation_jobs
                    WHERE created_at >= NOW() - INTERVAL '7 days'
                    GROUP BY status
                    ORDER BY cnt DESC
                """)
                recent_jobs = {row[0]: row[1] for row in cur.fetchall()}

        return {
            "ok": True,
            "stats": stats,
            "recent_jobs_7d": recent_jobs,
        }
    except Exception as e:
        logger.exception("获取改造统计失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-transformation/sources")
def get_factor_sources():
    """
    获取 aistock_factor_catalog 中所有不同的 source 值列表。
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT source, COUNT(*) AS cnt
                    FROM aistock_factor_catalog
                    GROUP BY source
                    ORDER BY cnt DESC
                """)
                rows = cur.fetchall()
        sources = [{"source": r[0], "count": r[1]} for r in rows]
        return {"ok": True, "sources": sources}
    except Exception as e:
        logger.exception("获取因子来源列表失败")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 实验结果分析 API
# ============================================================

class ExperimentAnalysisRequest(BaseModel):
    experiment_id: str = Field(..., description="实验ID")
    llm_analysis_text: Optional[str] = Field(None, description="前端已有的LLM分析文本（可选）")


@router.post("/experiments/{experiment_id}/analyze")
def analyze_experiment_results(experiment_id: str, req: Optional[ExperimentAnalysisRequest] = None):
    """
    分析实验结果，生成LLM反馈报告。
    
    无论配置是LLM生成还是手工配置，都会生成统一的分析报告。
    """
    try:
        from ..services.quantevolver.qe_feedback_service import QEFeedbackService
        from ..services.quantevolver.config_composer import ConfigComposer
        
        # 获取实验记录
        composer = ConfigComposer()
        exp_record = composer._get_experiment_record(experiment_id)
        if not exp_record:
            raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")
        
        # 生成反馈
        feedback_svc = QEFeedbackService()
        feedback = feedback_svc.generate_feedback(
            experiment_id=experiment_id,
            experiment_record=exp_record,
            llm_analysis_text=req.llm_analysis_text if req else None,
        )
        
        return {
            "ok": True,
            "experiment_id": experiment_id,
            "feedback": feedback.to_dict(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"分析实验结果失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/evolution-context")
def get_evolution_context(experiment_id: str):
    """
    获取下一轮演进的LLM上下文。
    
    包含历史轨迹、因子库、策略库、模型库等信息。
    """
    try:
        from ..services.quantevolver.qe_feedback_service import QEFeedbackService
        from ..services.quantevolver.config_composer import ConfigComposer
        
        # 获取实验记录
        composer = ConfigComposer()
        exp_record = composer._get_experiment_record(experiment_id)
        if not exp_record:
            raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")
        
        # 构建上下文
        feedback_svc = QEFeedbackService()
        context = feedback_svc.build_next_loop_context(
            experiment_id=experiment_id,
            experiment_record=exp_record,
        )
        
        return {
            "ok": True,
            "experiment_id": experiment_id,
            "context": context,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"获取演进上下文失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 单次实验执行：一键执行 + 实时日志 + 自动同步结果
# ============================================================



async def _poll_multi_alpha_nodes(experiment_id: str, qe_task_id: str) -> str:
    """聚合多Alpha实验所有节点的运行状态。

    查询 qe_multi_alpha_groups 中各组的 assigned_node_id，
    逐节点查询 RDAgent 状态。

    Returns: "completed" / "running" / "failed"
    - 全部 completed → "completed"
    - 任一 failed → "failed"
    - 否则 → "running"
    """
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    # 获取组状态与仍在运行的节点分配
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT status, COUNT(*)
                   FROM qe_multi_alpha_groups
                   WHERE parent_experiment_id = %s
                   GROUP BY status""",
                (experiment_id,),
            )
            group_status_counts = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """SELECT assigned_node_id, qe_loop_id
                   FROM qe_multi_alpha_groups
                   WHERE parent_experiment_id = %s
                     AND assigned_node_id IS NOT NULL
                     AND qe_loop_id IS NOT NULL
                     AND status = 'running'""",
                (experiment_id,),
            )
            running_assignments = [(r[0], r[1]) for r in cur.fetchall()]

    if not group_status_counts:
        raise RuntimeError(
            f"Multi-alpha experiment {experiment_id} has no group records"
        )
    if group_status_counts.get("failed", 0) > 0:
        return "failed"
    if not running_assignments:
        total_groups = sum(group_status_counts.values())
        if group_status_counts.get("completed", 0) == total_groups:
            return "completed"
        raise RuntimeError(
            f"Multi-alpha experiment {experiment_id} has no running nodes but is not completed: {group_status_counts}"
        )

    statuses = []
    for n_id, node_loop_id in running_assignments:
        try:
            client = QEWorkspaceClient.for_node(n_id)
            async with client:
                live = await client.get_loop_status(qe_task_id, node_loop_id)
                st = live.get("status")
                if not st:
                    raise RuntimeError(f"节点 {n_id} 返回空状态: {live}")
                statuses.append(st)

                # 同步更新对应组的状态
                if st in ("completed", "failed"):
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """UPDATE qe_multi_alpha_groups
                                   SET status = %s
                                   WHERE parent_experiment_id = %s
                                     AND assigned_node_id = %s
                                     AND qe_loop_id = %s
                                     AND status = 'running'""",
                                (st, experiment_id, n_id, node_loop_id),
                            )
                        conn.commit()
        except Exception as e:
            raise RuntimeError(f"节点 {n_id} 状态查询失败: {e}") from e

    if any(s in ("failed", "error") for s in statuses):
        return "failed"
    if all(s == "completed" for s in statuses):
        return "completed"
    return "running"


def _load_multi_alpha_status_payload(experiment_id: str, experiment_status: str) -> dict:
    """Build a UI-friendly multi-alpha lifecycle snapshot from persisted group rows."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT group_name, status, assigned_node_id, qe_loop_id, error_message
                   FROM qe_multi_alpha_groups
                   WHERE parent_experiment_id = %s
                   ORDER BY group_name""",
                (experiment_id,),
            )
            cols = [d[0] for d in cur.description]
            groups = [dict(zip(cols, row)) for row in cur.fetchall()]

    counts: dict[str, int] = {}
    for group in groups:
        status = group.get("status") or "pending"
        counts[status] = counts.get(status, 0) + 1

    total = len(groups)
    completed = counts.get("completed", 0)
    failed = counts.get("failed", 0)
    running = counts.get("running", 0)

    if experiment_status == "completed":
        stage = "completed"
        artifact_status = "ready"
    elif experiment_status == "failed":
        has_artifact_error = any(g.get("error_message") for g in groups)
        stage = "failed_artifact" if has_artifact_error else "failed"
        artifact_status = "failed"
    elif failed:
        stage = "group_failed"
        artifact_status = "not_started"
    elif running:
        stage = "group_training"
        artifact_status = "not_started"
    elif total > 0 and completed == total:
        stage = "result_collection"
        artifact_status = "validating"
    elif total > 0:
        stage = "pending_groups"
        artifact_status = "not_started"
    else:
        stage = "pending_setup"
        artifact_status = "not_started"

    return {
        "stage": stage,
        "artifact_status": artifact_status,
        "total_groups": total,
        "completed_groups": completed,
        "failed_groups": failed,
        "running_groups": running,
        "group_status_counts": counts,
        "groups": groups,
    }


def _mark_multi_alpha_artifact_failure(experiment_id: str, error_message: str) -> None:
    """Persist artifact collection failure without downgrading runtime success.

    RD-Agent has already reported every group loop as completed before this
    helper is called.  The authoritative experiment status must therefore stay
    completed while the artifact lifecycle records the collection failure.
    """
    lifecycle = {
        "multi_alpha_lifecycle": {
            "stage": "failed_artifact",
            "runtime_status": "completed",
            "collection_status": "failed",
            "artifact_status": "failed",
            "errors": [error_message],
        }
    }
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE qe_experiments
                   SET status = 'completed',
                       result_metrics = COALESCE(result_metrics, '{}'::jsonb) || %s::jsonb,
                       completed_at = NOW()
                   WHERE experiment_id = %s""",
                (json.dumps(lifecycle, ensure_ascii=False), experiment_id),
            )
        conn.commit()


def _mark_experiment_collection_failure(experiment_id: str, error_message: str) -> None:
    """Persist single-loop artifact collection failure after runtime completion."""
    lifecycle = {
        "qe_completion_lifecycle": {
            "stage": "artifact_collection_failed",
            "runtime_status": "completed",
            "collection_status": "failed",
            "artifact_status": "failed",
            "errors": [error_message],
        }
    }
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE qe_experiments
                   SET status = 'completed',
                       result_metrics = COALESCE(result_metrics, '{}'::jsonb) || %s::jsonb,
                       completed_at = NOW()
                   WHERE experiment_id = %s""",
                (json.dumps(lifecycle, ensure_ascii=False), experiment_id),
            )
        conn.commit()


def _update_experiment_status(experiment_id: str, status: str):
    """安全更新实验状态。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE qe_experiments SET status = %s, completed_at = NOW() WHERE experiment_id = %s",
                (status, experiment_id),
            )
        conn.commit()


def _update_experiment_with_metrics(experiment_id: str, metrics: dict):
    """将 RDAgent API 返回的回测指标写入 DB（JSON + 独立列）。"""
    # result_metrics JSON key -> 独立列名
    _COL_MAP = {
        "IC": "ic",
        "ICIR": "icir",
        "Rank IC": "rank_ic",
        "Rank ICIR": "rank_icir",
        "1day.excess_return_with_cost.annualized_return": "annualized_return",
        "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
        "1day.excess_return_with_cost.information_ratio": "information_ratio",
        "1day.excess_return_with_cost.mean": "excess_return_with_cost_mean",
        "1day.excess_return_without_cost.mean": "excess_return_without_cost_mean",
        "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
        "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
        "1day.excess_return_without_cost.information_ratio": "information_ratio_no_cost",
    }
    save_metrics = {k: v for k, v in metrics.items() if k != "_raw_json"}
    # 构建独立列 SET 子句
    col_sets = []
    col_vals = []
    for json_key, col_name in _COL_MAP.items():
        v = save_metrics.get(json_key)
        if v is not None:
            col_sets.append(f"{col_name} = %s")
            col_vals.append(float(v))
    extra_set = (", " + ", ".join(col_sets)) if col_sets else ""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE qe_experiments
                SET result_metrics = %s, status = 'completed', completed_at = NOW(){extra_set}
                WHERE experiment_id = %s
            """, [json.dumps(save_metrics, default=str)] + col_vals + [experiment_id])
        conn.commit()
    _archive_experiment_best_effort(experiment_id)


def _archive_experiment_best_effort(experiment_id: str) -> None:
    """Best-effort QE archive hook; never changes experiment status."""

    try:
        from ..services.qe_archive.realtime_ingestion import safe_archive_experiment_completed

        safe_archive_experiment_completed(experiment_id=experiment_id)
    except Exception as exc:  # pragma: no cover - defensive isolation.
        logger.warning(
            "QE archive realtime experiment hook failed without changing QE status: experiment=%s error=%s",
            experiment_id,
            exc,
            exc_info=True,
        )


def _tail_text_lines(text: str, max_lines: int = QE_EXPERIMENT_LOG_TAIL_DEFAULT_LINES) -> list[str]:
    """Return a bounded tail from text fetched through the QE node API."""
    limit = max(1, min(int(max_lines or QE_EXPERIMENT_LOG_TAIL_DEFAULT_LINES), 5000))
    return str(text or "").splitlines()[-limit:]


def _workspace_file_payload_to_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for key in ("content", "text", "data", "logs"):
            value = payload.get(key)
            if isinstance(value, list):
                return "\n".join(str(item) for item in value)
            if isinstance(value, str):
                return value
        return json.dumps(payload, ensure_ascii=False, default=str)
    return "" if payload is None else str(payload)


async def _load_experiment_node_log_tail(
    *,
    qe_task_id: str | None,
    qe_loop_id: str | None,
    execution_node_id: str,
    tail_lines: int = QE_EXPERIMENT_LOG_TAIL_DEFAULT_LINES,
) -> tuple[dict[str, Any], list[str]]:
    source = {
        "log_source": "qe_workspace_api",
        "node_id": execution_node_id,
        "artifact": "run.log",
        "artifact_unavailable": False,
    }
    if not qe_task_id or not qe_loop_id:
        source["artifact_unavailable"] = True
        source["artifact_error"] = "experiment is missing qe_task_id or qe_loop_id"
        return source, []

    try:
        from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

        async with QEWorkspaceClient.for_node(execution_node_id) as client:
            payload = await client.get_workspace_file(qe_task_id, qe_loop_id, "run.log")
        return source, _tail_text_lines(_workspace_file_payload_to_text(payload), tail_lines)
    except Exception as exc:
        source["artifact_unavailable"] = True
        source["artifact_error"] = str(exc)
        logger.info(
            "QE node run.log tail unavailable: task=%s loop=%s node=%s error=%s",
            qe_task_id,
            qe_loop_id,
            execution_node_id,
            exc,
        )
        return source, []


async def _stream_experiment_node_log_tail(
    *,
    experiment_id: str,
    experiment_status: str | None,
    qe_task_id: str | None,
    qe_loop_id: str | None,
    execution_node_id: str,
    tail_lines: int = QE_EXPERIMENT_LOG_TAIL_DEFAULT_LINES,
):
    source, lines = await _load_experiment_node_log_tail(
        qe_task_id=qe_task_id,
        qe_loop_id=qe_loop_id,
        execution_node_id=execution_node_id,
        tail_lines=tail_lines,
    )
    yield (
        "data: [System] Experiment "
        f"{experiment_id} is terminal ({experiment_status}); showing QE node log tail only.\n\n"
    )
    if source.get("artifact_unavailable"):
        yield f"data: [System] QE node run.log tail unavailable: {source.get('artifact_error')}\n\n"
    else:
        yield f"data: [System] Log source: QE node {execution_node_id} run.log via API.\n\n"
    if lines:
        for log_line in lines:
            yield f"data: {log_line}\n\n"
    else:
        yield "data: [System] No QE node run.log tail is available.\n\n"
    yield f"data: [System] AIstock authoritative final status: {experiment_status}\n\n"


def _load_experiment_terminal_status(experiment_id: str) -> str | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM qe_experiments WHERE experiment_id = %s",
                (experiment_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None



def _resolve_qe_experiment_callback_url(
    node_id: str | None = None,
    node_callback_url: str | None = None,
) -> str:
    """Return the concrete webhook endpoint used by one-off QE experiments."""
    return build_aistock_callback_url(
        endpoint_path="/api/v1/quantevolver/webhook/loop-completed",
        full_url_env="AISTOCK_QE_LOOP_CALLBACK_URL",
        node_id=node_id,
        node_callback_url=node_callback_url,
        require_env_base=True,
    )


def _parse_qe_custom_params(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        parsed: Any = None
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as e:
            logger.warning("忽略无法解析的 QE custom_params JSON: %s", e)
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def _get_recorded_experiment_node(exp_record: dict[str, Any]) -> str | None:
    custom_params = _parse_qe_custom_params(exp_record.get("custom_params"))
    return custom_params.get("execution_node_id") or custom_params.get("node_id")


@router.post("/experiments/{experiment_id}/run")
async def run_experiment(experiment_id: str, engine_mode: Optional[str] = "unified", node_id: str = None):
    """一键执行单次实验：读取配置 → compose_in_memory → 提交 RDAgent。

    实验状态由前端通过 get_experiment_run_status 按需查询并自动同步，
    不再使用后台轮询（避免阻塞 uvicorn reload）。
    """
    if (engine_mode or "unified") != "unified":
        raise HTTPException(
            status_code=400,
            detail=(
                "QE legacy execution engine has been removed. "
                "Only engine_mode='unified' is supported."
            ),
        )


    # 多Alpha实验：专属执行路径（各组独立提交到 RDAgent）
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT alpha_mode, multi_alpha_config, experiment_name FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                _row = cur.fetchone()
        if _row and _row[0] == "multi":
            return await _run_multi_alpha_experiment(experiment_id, node_id=node_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"多Alpha实验执行失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=f"多Alpha实验执行失败: {e}")

    return await _run_experiment_unified(experiment_id, node_id=node_id)


async def _run_multi_alpha_experiment(experiment_id: str, node_id: str = None):
    """多Alpha实验执行路径（支持分布式并行）。

    执行策略:
    - serial / local_parallel: 所有组提交到一个节点，作为单个 task，本地执行 meta_model_runner
    - distributed: 按 assigned_node_id 把组拆分到多个节点并行执行，
                   待所有节点完成后由 ResultCollector 收集跨节点预测并运行 meta 合并

    qe_task_id = experiment_name (主task_id)
    每个节点的任务使用同一个 qe_task_id，不同节点 loop_id 独立记录在 qe_multi_alpha_groups.qe_loop_id
    """
    from ..services.quantevolver.config_composer import ConfigComposer
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    cc = ConfigComposer()
    exp_record = cc._get_experiment_record(experiment_id)
    if not exp_record:
        raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")
    if exp_record.get("status") == "running":
        raise HTTPException(status_code=409, detail="实验正在执行中，请勿重复提交")

    experiment_name = exp_record.get("experiment_name") or f"qe_exp_{experiment_id}"

    multi_alpha_config_raw = exp_record.get("multi_alpha_config")
    if not multi_alpha_config_raw:
        raise HTTPException(status_code=400, detail="实验缺少 multi_alpha_config，无法执行多Alpha实验")
    if isinstance(multi_alpha_config_raw, str):
        multi_alpha_config_raw = json.loads(multi_alpha_config_raw)
    _assert_multi_alpha_execution_mode_supported(multi_alpha_config_raw)

    from ..services.quantevolver.experiment_config_builders import build_config_from_multi_alpha
    from ..services.quantevolver.multi_alpha_engine import MultiAlphaEngine

    data_split = exp_record.get("data_split")
    if isinstance(data_split, str):
        data_split = json.loads(data_split)
    custom_params = exp_record.get("custom_params")
    if isinstance(custom_params, str):
        custom_params = json.loads(custom_params)
    _cp = custom_params or {}
    hmm_cfg_dict = None
    if _cp.get("sector_hmm_model_path"):
        hmm_cfg_dict = {
            "enable_sector_hmm": True,
            "hmm_model_version_id": _cp.get("hmm_model_version_id", "from_persisted_experiment"),
            "sector_hmm_model_path": _cp["sector_hmm_model_path"],
            "hmm_signal_preset": _cp.get("hmm_signal_preset"),
            "hmm_signal_presets": _cp.get("hmm_signal_presets"),
            "hmm_config_json": _cp.get("hmm_config_json"),
        }
    uf_params = None
    if _cp.get("unfilled_handler"):
        uf_params = {}
        if _cp.get("unfilled_trigger_minute"):
            uf_params["trigger_minute"] = _cp["unfilled_trigger_minute"]
        if _cp.get("unfilled_backup_depth"):
            uf_params["backup_depth"] = _cp["unfilled_backup_depth"]

    # 查询可用节点（用于分布式规划）
    available_nodes = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT node_id, api_base_url, gpu_model, gpu_vram_mb, status, callback_url
                FROM infra.compute_nodes
            """)
            cols = [d[0] for d in cur.description]
            available_nodes = [dict(zip(cols, row)) for row in cur.fetchall()]

    exp_cfg = build_config_from_multi_alpha(
        multi_alpha_config=multi_alpha_config_raw,
        data_split=data_split,
        strategy_id=exp_record.get("strategy_id"),
        strategy_params={"topk": _cp.get("topk"), "n_drop": _cp.get("n_drop")} if _cp else None,
        label_type=_cp.get("label_type"),
        label_horizon=_cp.get("label_horizon"),
        execution_algo=_cp.get("execution_algo"),
        execution_algo_params=_cp.get("execution_algo_params"),
        unfilled_handler=_cp.get("unfilled_handler"),
        unfilled_handler_params=uf_params,
        stock_pool=_cp.get("stock_pool"),
        hmm_config=hmm_cfg_dict,
        experiment_name=experiment_name,
        node_id=node_id,
    )
    engine = MultiAlphaEngine(
        experiment_config=exp_cfg,
        composer=cc,
        available_nodes=available_nodes,
    )
    engine_result = engine.run()
    if not engine_result.get("ok"):
        raise HTTPException(status_code=500, detail="MultiAlphaEngine 执行失败")

    all_experiment_files = engine_result.get("experiment_files", {})
    group_configs = engine_result.get("group_configs", [])
    execution_mode = engine_result.get("execution_mode", "serial")

    qe_task_id = experiment_name

    # ── 按 node_id 分组 ────────────────────────────────────────
    node_groups: dict[str, list[dict]] = {}
    for gc in group_configs:
        n_id = gc.get("node_id") or node_id or "wsl2-5080"
        node_groups.setdefault(n_id, []).append(gc)

    is_distributed = len(node_groups) > 1

    # 查询各节点 callback_url。compute_nodes.callback_url 可能只是 base URL，
    # 提交给 RD-Agent 前必须展开成当前 QE 实验 webhook 的完整路径。
    node_callbacks = {
        n["node_id"]: _resolve_qe_experiment_callback_url(
            n.get("node_id"), n.get("callback_url")
        )
        for n in available_nodes
    }

    submitted_nodes: list[dict] = []

    if is_distributed:
        # ── 分布式：各节点独立提交 ────────────────────────────
        logger.info(f"多Alpha分布式执行: {len(node_groups)} 节点, 共 {len(group_configs)} 组")

        for n_id, groups in node_groups.items():
            # 每个节点只拿到自己的组文件（避免传输冗余）
            node_files = {}
            for gc in groups:
                prefix = f"group_{gc['group_name']}/"
                for fname, content in all_experiment_files.items():
                    if fname.startswith(prefix):
                        node_files[fname] = content

            # 按 order 排序各节点内组命令
            node_cmds = []
            reuse_group_names = []
            for gc in sorted(groups, key=lambda g: g.get("order", 0)):
                if gc.get("reuse_mode") in ("reuse_prediction", "reuse_model"):
                    reuse_group_names.append(gc["group_name"])
                    continue
                node_cmds.append(_build_multi_alpha_group_command(gc, n_id))

            if not node_cmds:
                raise RuntimeError(
                    f"节点 {n_id} 没有可执行组命令；distributed 模式暂不支持 reuse-only 节点: {reuse_group_names}"
                )

            node_command = " && ".join(node_cmds)

            node_config = {
                "alpha_mode": "multi",
                "node_id": n_id,
                "group_names": [g["group_name"] for g in groups],
                "is_distributed_node": True,
            }

            client = QEWorkspaceClient.for_node(n_id)
            try:
                async with client:
                    node_loop_id = await client.create_and_run_loop(
                        qe_task_id, 1, node_config, node_files, node_command,
                        callback_url=node_callbacks.get(n_id) or _resolve_qe_experiment_callback_url(n_id),
                    )
            except Exception as e:
                raise RuntimeError(f"节点 {n_id} 提交失败: {e}") from e

            submitted_nodes.append({
                "node_id": n_id,
                "qe_loop_id": node_loop_id,
                "group_names": [g["group_name"] for g in groups],
            })
            logger.info(f"节点 {n_id}: 提交成功, loop_id={node_loop_id}, groups={[g['group_name'] for g in groups]}")

        # 主 loop_id 用第一个成功节点的（兼容单 loop_id 的查询逻辑）
        primary_loop_id = next(
            (n["qe_loop_id"] for n in submitted_nodes if n.get("qe_loop_id")),
            None,
        )
        if not primary_loop_id:
            raise RuntimeError(f"多Alpha分布式执行失败: {experiment_id} 没有成功提交的节点")

        # 持久化各节点 loop_id 到 qe_multi_alpha_groups
        with get_conn() as conn:
            with conn.cursor() as cur:
                for sn in submitted_nodes:
                    for g_name in sn["group_names"]:
                        cur.execute(
                            """UPDATE qe_multi_alpha_groups
                               SET assigned_node_id = %s,
                                   qe_loop_id = %s,
                                   status = 'running',
                                   error_message = NULL
                               WHERE parent_experiment_id = %s AND group_name = %s""",
                            (sn["node_id"], sn["qe_loop_id"], experiment_id, g_name),
                        )
            conn.commit()

    else:
        # ── 单节点：所有组 + meta_model_runner 一起跑 ───────────
        only_node_id = next(iter(node_groups.keys()))
        logger.info(f"多Alpha单节点执行: node={only_node_id}, {len(group_configs)} 组, mode={execution_mode}")

        group_cmds = []
        reuse_group_names = []
        for gc in sorted(group_configs, key=lambda g: g.get("order", 0)):
            if gc.get("reuse_mode") in ("reuse_prediction", "reuse_model"):
                reuse_group_names.append(gc["group_name"])
                continue
            group_cmds.append(_build_multi_alpha_group_command(gc))
        # 单节点场景：meta_model_runner 在同一 workspace 运行
        if group_cmds:
            group_cmds.append("echo '=== Running meta_model_runner.py ===' && python meta_model_runner.py")
            orchestration_command = " && ".join(group_cmds)
        elif reuse_group_names:
            orchestration_command = "python meta_model_runner.py"
        else:
            raise RuntimeError("多Alpha单节点执行缺少可执行组命令")

        config = {
            "alpha_mode": "multi",
            "group_configs": group_configs,
            "meta_method": engine_result.get("meta_method"),
            "total_groups": engine_result.get("total_groups"),
        }

        client = QEWorkspaceClient.for_node(only_node_id)
        async with client:
            primary_loop_id = await client.create_and_run_loop(
                qe_task_id, 1, config, all_experiment_files, orchestration_command,
                callback_url=node_callbacks.get(only_node_id) or _resolve_qe_experiment_callback_url(only_node_id),
            )

        submitted_nodes.append({
            "node_id": only_node_id,
            "qe_loop_id": primary_loop_id,
            "group_names": [g["group_name"] for g in group_configs],
        })

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE qe_multi_alpha_groups
                       SET assigned_node_id = %s, qe_loop_id = %s, status = 'running'
                       WHERE parent_experiment_id = %s""",
                    (only_node_id, primary_loop_id, experiment_id),
                )
            conn.commit()

    # 更新 qe_experiments
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qe_experiments
                SET status = 'running',
                    qe_task_id = %s,
                    qe_loop_id = %s,
                    started_at = NOW()
                WHERE experiment_id = %s
            """, (qe_task_id, primary_loop_id, experiment_id))
        conn.commit()

    return {
        "ok": True,
        "experiment_id": experiment_id,
        "qe_task_id": qe_task_id,
        "qe_loop_id": primary_loop_id,
        "engine": "multi_alpha",
        "is_distributed": is_distributed,
        "nodes": submitted_nodes,
        "total_groups": engine_result.get("total_groups"),
    }


async def _run_experiment_unified(experiment_id: str, node_id: str = None):
    """统一引擎路径：使用 ExperimentConfig + BacktestExecutor 执行单次实验（Path 1）。"""
    from ..services.quantevolver.config_composer import ConfigComposer
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient
    from ..services.quantevolver.experiment_config_builders import build_config_from_exp_record
    from ..services.quantevolver.executors.backtest import BacktestExecutor, BacktestMode
    from ..services.quantevolver.executors.base import ExecutionContext

    try:
        cc = ConfigComposer()
        exp_record = cc._get_experiment_record(experiment_id)
        if not exp_record:
            raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")
        if exp_record.get("status") == "running":
            raise HTTPException(status_code=409, detail="实验正在执行中，请勿重复提交")

        experiment_name = exp_record.get("experiment_name") or f"qe_exp_{experiment_id}"
        recorded_node_id = _get_recorded_experiment_node(exp_record)
        if recorded_node_id and node_id and recorded_node_id != node_id:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "QE_RERUN_NODE_LOCKED",
                    "message": (
                        f"Experiment {experiment_id} is locked to node {recorded_node_id}; "
                        f"refusing to rerun on {node_id}."
                    ),
                    "context": {
                        "experiment_id": experiment_id,
                        "recorded_node_id": recorded_node_id,
                        "requested_node_id": node_id,
                    },
                },
            )
        effective_node_id = node_id or recorded_node_id or resolve_default_qe_node_id()
        try:
            await preflight_qe_node(effective_node_id)
        except QENodePreflightError as e:
            raise HTTPException(status_code=400, detail=e.to_detail()) from e

        cfg = build_config_from_exp_record(exp_record, experiment_name=experiment_name)

        # 查询远端节点 callback_url
        callback_url = None
        node_callback_base = None
        if effective_node_id:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT callback_url FROM infra.compute_nodes WHERE node_id = %s", (effective_node_id,))
                    row = cur.fetchone()
                    if row:
                        node_callback_base = row[0]
        try:
            callback_url = _resolve_qe_experiment_callback_url(effective_node_id, node_callback_base)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "QE_CALLBACK_URL_UNREACHABLE",
                    "message": str(e),
                    "context": {"node_id": effective_node_id},
                },
            ) from e

        ctx = ExecutionContext(
            task_id=experiment_name,
            loop_index=1,
            experiment_name=f"{experiment_name}/Loop1",
            node_id=effective_node_id,
            callback_url=callback_url,
            require_fixed_seed=True,
        )

        client = QEWorkspaceClient.for_node(effective_node_id)
        async with client:
            executor = BacktestExecutor(cc, client)
            result = await executor.submit(cfg, ctx, mode=BacktestMode.FULL_TRAIN)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_experiments
                    SET status = 'running',
                        qe_task_id = %s,
                        qe_loop_id = %s,
                        custom_params = CASE
                            WHEN %s::text IS NULL THEN custom_params
                            ELSE COALESCE(custom_params, '{}'::jsonb) || jsonb_build_object('execution_node_id', %s::text)
                        END,
                        started_at = NOW()
                    WHERE experiment_id = %s
                """, (experiment_name, result.job_id, effective_node_id, effective_node_id, experiment_id))
            conn.commit()

        return {
            "ok": True,
            "experiment_id": experiment_id,
            "qe_task_id": experiment_name,
            "qe_loop_id": result.job_id,
            "engine": "unified",
            "node_id": effective_node_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[unified] 执行实验失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))




class QELoopCompletedPayload(BaseModel):
    task_id: str
    loop_id: str
    status: str = "completed"


def _callback_loop_candidates(task_id: str, loop_id: str) -> list[str]:
    candidates = {str(loop_id or "").strip()}
    prefix = f"{task_id}_"
    if loop_id and str(loop_id).startswith(prefix):
        candidates.add(str(loop_id)[len(prefix):])
    return [c for c in candidates if c]


def _find_experiments_for_loop_callback(task_id: str, loop_id: str) -> list[str]:
    loop_ids = _callback_loop_candidates(task_id, loop_id)
    if not task_id or not loop_ids:
        return []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT experiment_id
                FROM qe_experiments
                WHERE status = 'running'
                  AND qe_task_id = %s
                  AND qe_loop_id = ANY(%s)
                """,
                (task_id, loop_ids),
            )
            direct = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT DISTINCT e.experiment_id
                FROM qe_experiments e
                JOIN qe_multi_alpha_groups g
                  ON g.parent_experiment_id = e.experiment_id
                WHERE e.status = 'running'
                  AND e.qe_task_id = %s
                  AND g.qe_loop_id = ANY(%s)
                """,
                (task_id, loop_ids),
            )
            via_groups = [row[0] for row in cur.fetchall()]
    seen = set()
    result = []
    for exp_id in direct + via_groups:
        if exp_id not in seen:
            result.append(exp_id)
            seen.add(exp_id)
    return result


@router.post("/webhook/loop-completed", summary="QE loop ???????/?Alpha???")
async def on_qe_loop_completed_webhook(request: Request, payload: QELoopCompletedPayload):
    """Receive RD-Agent loop completion and trigger the same status sync as run-status."""
    secret = os.getenv("QE_WEBHOOK_SECRET", "")
    if secret:
        provided_secret = request.headers.get("X-Webhook-Secret", "")
        if provided_secret != secret:
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    experiment_ids = _find_experiments_for_loop_callback(payload.task_id, payload.loop_id)
    if not experiment_ids:
        logger.warning(
            "QE loop callback did not match running experiment: task=%s loop=%s status=%s",
            payload.task_id,
            payload.loop_id,
            payload.status,
        )
        return {
            "status": "ignored",
            "task_id": payload.task_id,
            "loop_id": payload.loop_id,
            "matched": 0,
        }

    async def _process():
        for exp_id in experiment_ids:
            try:
                await get_experiment_run_status(exp_id)
            except Exception as exc:
                logger.error(
                    "QE loop callback status sync failed: experiment=%s task=%s loop=%s error=%s",
                    exp_id,
                    payload.task_id,
                    payload.loop_id,
                    exc,
                    exc_info=True,
                )

    task = asyncio.create_task(_process())
    task.add_done_callback(
        lambda t: logger.error("QE callback task error: %s", t.exception(), exc_info=True)
        if t.exception()
        else None
    )
    return {
        "status": "accepted",
        "task_id": payload.task_id,
        "loop_id": payload.loop_id,
        "matched": len(experiment_ids),
        "experiment_ids": experiment_ids,
    }


@router.get("/experiments/{experiment_id}/run-status")
async def get_experiment_run_status(experiment_id: str):
    """查询实验执行状态。如果有 qe_loop_id 则实时查询 RDAgent 侧。"""
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, qe_task_id, qe_loop_id, result_metrics, alpha_mode, custom_params
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="实验不存在")
                cols = [desc[0] for desc in cur.description]
                record = dict(zip(cols, row))

        is_multi_alpha = record.get("alpha_mode") == "multi"

        result = {
            "experiment_id": experiment_id,
            "status": record["status"],
            "qe_task_id": record.get("qe_task_id"),
            "qe_loop_id": record.get("qe_loop_id"),
            "result_metrics": record.get("result_metrics"),
            "alpha_mode": record.get("alpha_mode", "single"),
        }

        if is_multi_alpha:
            multi_alpha_status = _load_multi_alpha_status_payload(
                experiment_id, record["status"]
            )
            lifecycle_source = record.get("result_metrics") or {}
            if isinstance(lifecycle_source, str):
                try:
                    lifecycle_source = json.loads(lifecycle_source)
                except Exception:
                    lifecycle_source = {}
            lifecycle = lifecycle_source.get("multi_alpha_lifecycle") if isinstance(lifecycle_source, dict) else None
            detail_source = lifecycle_source.get("multi_alpha_detail") if isinstance(lifecycle_source, dict) else None
            if isinstance(lifecycle, dict):
                multi_alpha_status["stage"] = lifecycle.get("stage", multi_alpha_status["stage"])
                multi_alpha_status["artifact_status"] = lifecycle.get(
                    "artifact_status", multi_alpha_status["artifact_status"]
                )
                multi_alpha_status["artifact_errors"] = lifecycle.get("errors", [])
                if lifecycle.get("backtest_loop_id"):
                    multi_alpha_status["backtest_loop_id"] = lifecycle.get("backtest_loop_id")
                if lifecycle.get("primary_node_id"):
                    multi_alpha_status["primary_node_id"] = lifecycle.get("primary_node_id")
                if isinstance(lifecycle.get("unified_backtest"), dict):
                    multi_alpha_status["unified_backtest"] = lifecycle.get("unified_backtest")
            elif isinstance(detail_source, dict) and isinstance(detail_source.get("unified_backtest"), dict):
                unified_backtest = detail_source["unified_backtest"]
                multi_alpha_status["unified_backtest"] = unified_backtest
                if unified_backtest.get("loop_id"):
                    multi_alpha_status["backtest_loop_id"] = unified_backtest.get("loop_id")
                if unified_backtest.get("primary_node_id"):
                    multi_alpha_status["primary_node_id"] = unified_backtest.get("primary_node_id")
            result["multi_alpha"] = multi_alpha_status
            result["multi_alpha_stage"] = multi_alpha_status["stage"]
            result["artifact_status"] = multi_alpha_status["artifact_status"]

        # 如果状态为 running，实时查询并自动同步
        if record["status"] == "running":
            if not record.get("qe_task_id"):
                raise HTTPException(status_code=400, detail="运行中实验缺少 qe_task_id")
            if not is_multi_alpha and not record.get("qe_loop_id"):
                raise HTTPException(status_code=400, detail="运行中实验缺少 qe_loop_id")
            if is_multi_alpha:
                # ── 多Alpha: 聚合所有节点的状态 ────────────────
                rd_status = await _poll_multi_alpha_nodes(
                    experiment_id, record.get("qe_task_id")
                )
                result["live_status"] = rd_status

                if rd_status == "completed":
                    result["multi_alpha_stage"] = "artifact_validation"
                    result["artifact_status"] = "validating"
                    if "multi_alpha" in result:
                        result["multi_alpha"]["stage"] = "artifact_validation"
                        result["multi_alpha"]["artifact_status"] = "validating"
                    try:
                        from ..services.quantevolver.multi_alpha_result_collector import MultiAlphaResultCollector
                        collector = MultiAlphaResultCollector()
                        collect_result = await collector.collect_and_persist(experiment_id)
                        result["status"] = "completed"
                        result["result_metrics"] = collect_result.get("result_metrics") or collect_result.get("combined_metrics", {})
                        result["multi_alpha_stage"] = "completed"
                        result["artifact_status"] = "ready"
                        fresh_multi_alpha_status = _load_multi_alpha_status_payload(
                            experiment_id, "completed"
                        )
                        fresh_multi_alpha_status["stage"] = "completed"
                        fresh_multi_alpha_status["artifact_status"] = "ready"
                        result_metrics = result["result_metrics"] if isinstance(result["result_metrics"], dict) else {}
                        lifecycle = result_metrics.get("multi_alpha_lifecycle") if isinstance(result_metrics, dict) else None
                        if isinstance(lifecycle, dict):
                            if lifecycle.get("backtest_loop_id"):
                                fresh_multi_alpha_status["backtest_loop_id"] = lifecycle.get("backtest_loop_id")
                            if lifecycle.get("primary_node_id"):
                                fresh_multi_alpha_status["primary_node_id"] = lifecycle.get("primary_node_id")
                            if isinstance(lifecycle.get("unified_backtest"), dict):
                                fresh_multi_alpha_status["unified_backtest"] = lifecycle.get("unified_backtest")
                        result["multi_alpha"] = fresh_multi_alpha_status
                        _archive_experiment_best_effort(experiment_id)
                    except Exception as me:
                        error_msg = f"Multi-Alpha result collection failed: {me}"
                        logger.error(f"Multi-Alpha result collection failed: {experiment_id}: {me}", exc_info=True)
                        _mark_multi_alpha_artifact_failure(experiment_id, error_msg)
                        result["status"] = "completed"
                        result["error"] = error_msg
                        result["multi_alpha_stage"] = "failed_artifact"
                        result["artifact_status"] = "failed"
                        if "multi_alpha" in result:
                            result["multi_alpha"]["stage"] = "failed_artifact"
                            result["multi_alpha"]["runtime_status"] = "completed"
                            result["multi_alpha"]["collection_status"] = "failed"
                            result["multi_alpha"]["artifact_status"] = "failed"
                            result["multi_alpha"]["artifact_errors"] = [error_msg]
                elif rd_status == "failed":
                    _update_experiment_status(experiment_id, "failed")
                    result["status"] = "failed"
                # running → 继续等待

            elif record.get("qe_loop_id"):
                # ── 单Alpha: 原有逻辑 ──────────────────────────
                custom_params = record.get("custom_params") or {}
                if isinstance(custom_params, str):
                    try:
                        custom_params = json.loads(custom_params)
                    except Exception:
                        custom_params = {}
                node_id = None
                if isinstance(custom_params, dict):
                    node_id = custom_params.get("execution_node_id") or custom_params.get("node_id")
                client_cm = QEWorkspaceClient.for_node(node_id) if node_id else QEWorkspaceClient()
                async with client_cm as client:
                    live_status = await client.get_loop_status(record["qe_task_id"], record["qe_loop_id"])
                    rd_status = live_status.get("status")
                    if not rd_status:
                        raise RuntimeError(f"节点返回空状态: {live_status}")
                    result["live_status"] = rd_status

                    if rd_status == "completed":
                        try:
                            metrics = await client.get_loop_metrics(record["qe_task_id"], record["qe_loop_id"])
                            _update_experiment_with_metrics(experiment_id, metrics)
                            result["status"] = "completed"
                            result["result_metrics"] = {k: v for k, v in metrics.items() if k != "_raw_json"}
                        except Exception as me:
                            logger.error(f"Auto-sync metrics failed for {experiment_id}: {me}", exc_info=True)
                            error_msg = f"Auto-sync metrics failed: {me}"
                            _mark_experiment_collection_failure(experiment_id, error_msg)
                            result["status"] = "completed"
                            result["artifact_status"] = "failed"
                            result["collection_status"] = "failed"
                            result["error"] = error_msg
                    elif rd_status in ("failed", "error"):
                        _update_experiment_status(experiment_id, "failed")
                        result["status"] = "failed"
                    elif rd_status in ("interrupted", "not_found"):
                        _update_experiment_status(experiment_id, "interrupted")
                        result["status"] = "interrupted"

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"查询执行状态失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/logs/tail")
async def get_experiment_logs_tail(
    experiment_id: str,
    tail: int = Query(QE_EXPERIMENT_LOG_TAIL_DEFAULT_LINES, ge=1, le=5000),
):
    """Return a QE node run.log tail without opening the live log stream."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT qe_task_id, qe_loop_id, status, custom_params
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Experiment not found")
                qe_task_id, qe_loop_id, db_status, custom_params = row

        params = _parse_qe_custom_params(custom_params)
        execution_node_id = params.get("execution_node_id") or params.get("node_id") or resolve_default_qe_node_id()
        source, lines = await _load_experiment_node_log_tail(
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
            execution_node_id=execution_node_id,
            tail_lines=tail,
        )
        return {
            "status": "success",
            "data": {
                "experiment_id": experiment_id,
                "experiment_status": db_status,
                "terminal": str(db_status or "").lower() in QE_EXPERIMENT_LOG_TERMINAL_STATUSES,
                **source,
                "logs": lines,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Read experiment log tail failed: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/logs")
async def stream_experiment_logs(experiment_id: str):
    """Stream experiment logs and append the authoritative AIstock terminal status."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT qe_task_id, qe_loop_id, status, custom_params
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="?????")
                qe_task_id, qe_loop_id, db_status, custom_params = row

        if not qe_task_id:
            raise HTTPException(status_code=400, detail="experiment has no QE task id")

        params = _parse_qe_custom_params(custom_params)
        node_id = params.get("execution_node_id") or params.get("node_id")
        execution_node_id = node_id or resolve_default_qe_node_id()

        if str(db_status or "").lower() in QE_EXPERIMENT_LOG_TERMINAL_STATUSES:
            return StreamingResponse(
                _stream_experiment_node_log_tail(
                    experiment_id=experiment_id,
                    experiment_status=db_status,
                    qe_task_id=qe_task_id,
                    qe_loop_id=qe_loop_id,
                    execution_node_id=execution_node_id,
                ),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

        try:
            client = QEWorkspaceClient.for_node(execution_node_id)
        except Exception as node_err:
            node_error = str(node_err)
            async def unavailable_node_generator():
                yield f"data: [ERROR] QE execution node unavailable for log stream: {node_error}\n\n"
                yield f"data: [System] AIstock authoritative status: {db_status}\n\n"

            return StreamingResponse(
                unavailable_node_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        async def event_generator():
            streamed_any = False
            try:
                async for line in client.stream_task_logs(qe_task_id):
                    streamed_any = True
                    raw = line
                    if raw.startswith("data:"):
                        raw = raw[5:].strip()
                    if raw:
                        try:
                            payload = json.loads(raw)
                            if isinstance(payload, dict) and "logs" in payload:
                                for log_line in payload["logs"]:
                                    yield f"data: {log_line}\n\n"
                                continue
                        except (json.JSONDecodeError, TypeError):
                            pass
                        for sub in raw.split("\n"):
                            yield f"data: {sub}\n\n"
            except Exception as e:
                if not streamed_any:
                    source, tail_lines = await _load_experiment_node_log_tail(
                        qe_task_id=qe_task_id,
                        qe_loop_id=qe_loop_id,
                        execution_node_id=execution_node_id,
                        tail_lines=QE_EXPERIMENT_LOG_TAIL_DEFAULT_LINES,
                    )
                    if tail_lines:
                        yield f"data: [WARN] RD-Agent live log stream unavailable, showing QE node run.log tail via API: {e}\n\n"
                        for log_line in tail_lines:
                            yield f"data: {log_line}\n\n"
                    elif source.get("artifact_unavailable"):
                        yield (
                            "data: [ERROR] log stream disconnected and QE node run.log tail unavailable: "
                            f"{e}; tail_error={source.get('artifact_error')}\n\n"
                        )
                    else:
                        yield f"data: [ERROR] log stream disconnected and QE node run.log tail is empty: {e}\n\n"
                else:
                    yield f"data: [ERROR] log stream disconnected: {e}\n\n"
            finally:
                try:
                    final_status = _load_experiment_terminal_status(experiment_id) or db_status
                    if str(final_status or "").lower() in QE_EXPERIMENT_LOG_TERMINAL_STATUSES:
                        yield f"data: [System] AIstock authoritative final status: {final_status}\n\n"
                finally:
                    await client.close()

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"???????: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/multi-node-logs")
async def stream_multi_node_logs(experiment_id: str):
    """Fan-in log streams from all compute nodes assigned to multi-alpha groups.

    For each running group in qe_multi_alpha_groups, opens an SSE connection
    to the assigned node via QEWorkspaceClient and merges log lines tagged
    with node_id into a single SSE response.
    """
    import asyncio

    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT group_name, assigned_node_id, qe_task_id, qe_loop_id, status
                   FROM qe_multi_alpha_groups
                   WHERE parent_experiment_id = %s
                   ORDER BY group_name""",
                (experiment_id,),
            )
            cols = [d[0] for d in cur.description]
            groups = [dict(zip(cols, row)) for row in cur.fetchall()]

            cur.execute(
                "SELECT status, custom_params FROM qe_experiments WHERE experiment_id = %s",
                (experiment_id,),
            )
            exp_row = cur.fetchone()
            if not exp_row:
                raise HTTPException(status_code=404, detail="实验不存在")

    if not groups:
        raise HTTPException(
            status_code=400,
            detail="该实验没有 multi-alpha 分组记录，请使用 /logs 端点",
        )

    exp_status, _ = exp_row

    async def _stream_node(
        group_name: str,
        node_id: str,
        qe_task_id: str,
        queue: asyncio.Queue,
    ):
        try:
            client = QEWorkspaceClient.for_node(node_id)
            async with client:
                async for line in client.stream_task_logs(qe_task_id):
                    raw = line
                    if raw.startswith("data:"):
                        raw = raw[5:].strip()
                    if not raw:
                        continue
                    try:
                        payload = json.loads(raw)
                        if isinstance(payload, dict) and "logs" in payload:
                            for log_line in payload["logs"]:
                                await queue.put(f"[{node_id}][{group_name}] {log_line}")
                            continue
                    except (json.JSONDecodeError, TypeError):
                        pass
                    for sub in raw.split("\n"):
                        if sub:
                            await queue.put(f"[{node_id}][{group_name}] {sub}")
        except Exception as e:
            await queue.put(f"[{node_id}][{group_name}] [ERROR] 日志流中断: {e}")

    async def event_generator():
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=2000)
        active_groups = [
            g for g in groups
            if g.get("assigned_node_id") and g.get("qe_task_id")
        ]

        if not active_groups:
            yield "data: [System] 没有已分配节点的分组，无法拉取远端日志\n\n"
            return

        yield f"data: [System] 已连接 {len(active_groups)} 个节点的日志流...\n\n"

        active_count = len(active_groups)
        done_count = 0
        all_done = asyncio.Event()

        async def _stream_node_tracked(gn, nid, tid):
            nonlocal done_count
            try:
                await _stream_node(gn, nid, tid, queue)
            finally:
                done_count += 1
                if done_count >= active_count:
                    all_done.set()

        tasks = [
            asyncio.create_task(
                _stream_node_tracked(
                    g["group_name"], g["assigned_node_id"], g["qe_task_id"]
                )
            )
            for g in active_groups
        ]

        try:
            while not all_done.is_set() or not queue.empty():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=5.0)
                    yield f"data: {item}\n\n"
                except asyncio.TimeoutError:
                    if all_done.is_set() and queue.empty():
                        break
                    yield ": heartbeat\n\n"
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/experiments/{experiment_id}/enhanced-metrics")
async def get_experiment_enhanced_metrics(experiment_id: str):
    """获取实验的增强诊断指标（IC 时序、Loss 曲线、收益曲线等）。
    优先返回 DB/result_metrics 中已有数据，必要时再代理到 RDAgent。
    """
    import httpx

    def _flatten_enhanced_payload(data: dict | None) -> dict:
        if not isinstance(data, dict):
            return {}

        def _normalized_summary(summary: Any) -> Any:
            if not isinstance(summary, dict):
                return summary
            normalized = dict(summary)
            aliases = {
                "ic": ("IC",),
                "icir": ("ICIR",),
                "rank_ic": ("Rank_IC", "Rank IC"),
                "rank_icir": ("Rank_ICIR", "Rank ICIR"),
                "annualized_return": (
                    "excess_return_with_cost_annualized",
                    "1day.excess_return_with_cost.annualized_return",
                ),
                "max_drawdown": (
                    "excess_return_with_cost_max_drawdown",
                    "1day.excess_return_with_cost.max_drawdown",
                ),
                "information_ratio": (
                    "excess_return_with_cost_IR",
                    "1day.excess_return_with_cost.information_ratio",
                ),
                "annualized_return_no_cost": (
                    "excess_return_without_cost_annualized",
                    "1day.excess_return_without_cost.annualized_return",
                ),
                "max_drawdown_no_cost": (
                    "excess_return_without_cost_max_drawdown",
                    "1day.excess_return_without_cost.max_drawdown",
                ),
                "information_ratio_no_cost": (
                    "excess_return_without_cost_IR",
                    "1day.excess_return_without_cost.information_ratio",
                ),
            }
            for canonical, source_keys in aliases.items():
                if normalized.get(canonical) is not None:
                    continue
                for source_key in source_keys:
                    if normalized.get(source_key) is not None:
                        normalized[canonical] = normalized[source_key]
                        break
            return normalized

        flat: dict[str, Any] = {}
        top_level_series_keys = [
            "dates", "return_dates", "ic_series", "rank_ic_series",
            "ic_rolling_30d_mean", "ic_rolling_30d_std", "ic_positive_ratio",
            "cumulative_excess_no_cost", "cumulative_excess_with_cost",
            "cumulative_benchmark", "drawdown_series", "train_loss_curve",
            "val_loss_curve", "best_epoch", "overfit_ratio", "convergence_ratio",
        ]
        for key in top_level_series_keys:
            if data.get(key) not in (None, [], {}):
                flat[key] = data[key]
        if "ic_diagnostics" in data:
            ic = data["ic_diagnostics"] or {}
            flat["dates"] = ic.get("ic_dates") or ic.get("dates", [])
            flat["ic_series"] = ic.get("ic_series")
            flat["rank_ic_series"] = ic.get("rank_ic_series")
            flat["ic_rolling_30d_mean"] = ic.get("ic_rolling_30d_mean")
            flat["ic_rolling_30d_std"] = ic.get("ic_rolling_30d_std")
            flat["ic_positive_ratio"] = ic.get("ic_positive_ratio")
        if "return_curves" in data:
            rc = data["return_curves"] or {}
            rc_dates = rc.get("dates", [])
            if not flat.get("dates"):
                flat["dates"] = rc_dates
            flat["return_dates"] = rc_dates
            flat["cumulative_excess_no_cost"] = rc.get("cumulative_excess_no_cost")
            flat["cumulative_excess_with_cost"] = rc.get("cumulative_excess_with_cost")
            flat["cumulative_benchmark"] = rc.get("cumulative_benchmark")
            flat["drawdown_series"] = rc.get("drawdown_series")
        if "training_diagnostics" in data:
            td = data["training_diagnostics"] or {}
            flat["train_loss_curve"] = td.get("train_loss_curve")
            flat["val_loss_curve"] = td.get("val_loss_curve")
            flat["best_epoch"] = td.get("best_epoch")
            flat["overfit_ratio"] = td.get("overfit_ratio")
            flat["convergence_ratio"] = td.get("convergence_ratio")
        if "summary" in data:
            flat["summary"] = _normalized_summary(data["summary"])

        passthrough_keys = [
            "top_stocks", "bottom_stocks", "stock_trades",
            "trade_diagnostics", "prediction_diagnostics",
            "all_stocks", "stock_pnl_summary", "limit_analysis",
            "factor_analysis", "feature_importance",
            "absolute_returns",
            "multi_alpha_detail", "multi_alpha_analysis",
        ]
        for passthrough_key in passthrough_keys:
            if passthrough_key not in data:
                continue
            val = data[passthrough_key]
            if val is None:
                continue
            if isinstance(val, (dict, list)) and len(val) == 0:
                continue
            flat[passthrough_key] = val
        return flat

    def _parse_jsonish(value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return value
        parsed: Any = None
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as e:
                logger.warning("忽略无法解析的 QE result_metrics JSON: %s", e)
        return parsed

    def _extract_cached_enhanced(result_metrics_raw: Any) -> dict:
        result_metrics = _parse_jsonish(result_metrics_raw) or {}
        if not isinstance(result_metrics, dict):
            return {}

        nested = result_metrics.get("enhanced_metrics")
        flat = _flatten_enhanced_payload(nested if isinstance(nested, dict) else None)
        if flat:
            for key in ("multi_alpha_detail", "multi_alpha_analysis"):
                val = result_metrics.get(key)
                if val not in (None, [], {}):
                    flat[key] = val
            return flat

        flat_keys = [
            "dates", "return_dates", "ic_series", "rank_ic_series",
            "ic_rolling_30d_mean", "ic_rolling_30d_std", "ic_positive_ratio",
            "cumulative_excess_no_cost", "cumulative_excess_with_cost",
            "cumulative_benchmark", "drawdown_series", "train_loss_curve",
            "val_loss_curve", "best_epoch", "overfit_ratio", "convergence_ratio",
            "top_stocks", "bottom_stocks", "all_stocks", "stock_trades",
            "trade_diagnostics", "prediction_diagnostics", "factor_analysis",
            "feature_importance", "absolute_returns", "summary",
            "multi_alpha_detail", "multi_alpha_analysis",
        ]
        cached = {k: result_metrics.get(k) for k in flat_keys if k in result_metrics}
        return {k: v for k, v in cached.items() if v not in (None, [], {})}

    def _has_enhanced_detail(flat: dict) -> bool:
        detail_keys = [
            "dates", "return_dates", "ic_series", "rank_ic_series",
            "cumulative_excess_no_cost", "cumulative_excess_with_cost",
            "drawdown_series", "top_stocks", "bottom_stocks", "all_stocks",
            "stock_trades", "trade_diagnostics", "prediction_diagnostics",
            "factor_analysis", "feature_importance", "absolute_returns",
            "multi_alpha_detail", "multi_alpha_analysis",
        ]
        return any(flat.get(k) not in (None, [], {}) for k in detail_keys)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT qe_loop_id, qe_task_id, result_metrics, custom_params FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="实验不存在")
                if len(row) >= 4:
                    qe_loop_id, qe_task_id, result_metrics_raw, custom_params_raw = row[:4]
                else:
                    qe_loop_id, qe_task_id, result_metrics_raw = row
                    custom_params_raw = None

        custom_params = _parse_qe_custom_params(custom_params_raw)
        recorded_execution_node_id = custom_params.get("execution_node_id") or custom_params.get("node_id")

        cached_flat = _extract_cached_enhanced(result_metrics_raw)
        if _has_enhanced_detail(cached_flat):
            return cached_flat

        if not qe_task_id or not qe_loop_id:
            raise HTTPException(status_code=404, detail="增强指标不可用")

        from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient
        execution_node_id = recorded_execution_node_id or resolve_default_qe_node_id()
        async with QEWorkspaceClient.for_node(execution_node_id) as client:
            data = await client.get_enhanced_metrics(qe_task_id, qe_loop_id)

        flat = _flatten_enhanced_payload(data)
        if flat:
            return flat
        raise HTTPException(status_code=404, detail="增强指标文件尚未生成")
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status == 404:
            raise HTTPException(status_code=404, detail="增强指标文件尚未生成")
        raise HTTPException(status_code=status, detail=str(e))
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"RDAgent 服务不可达: {e}")
    except RuntimeError as e:
        message = str(e)
        if "404" in message or "not found" in message.lower():
            raise HTTPException(status_code=404, detail="增强指标文件尚未生成")
        raise HTTPException(status_code=502, detail=f"RDAgent 增强指标读取失败: {message}")
    except ValueError as e:
        raise HTTPException(status_code=502, detail=f"QE 执行节点不可用: {e}")
    except Exception as e:
        logger.exception(f"获取增强指标失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiments/{experiment_id}/stop", summary="停止多Alpha实验（终止所有节点）")
async def stop_multi_alpha_experiment(experiment_id: str):
    """一键停止多Alpha实验：终止所有节点正在执行的训练/回测。"""
    from ..services.quantevolver.qe_evolution_service import AutoEvolutionScheduler

    try:
        svc = AutoEvolutionScheduler()
        result = await svc.stop_multi_alpha_experiment(experiment_id)
        if result.get("groups_failed"):
            return {
                "status": "warning",
                "message": "实验已停止，但部分节点终止失败",
                "detail": result,
            }
        return {"status": "success", "message": f"实验 {experiment_id} 已停止", "detail": result}
    except Exception as e:
        logger.error(f"Failed to stop multi-alpha experiment {experiment_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


_QE_DELETE_ACTIVE_STATUSES = {"running", "processing", "queued", "submitted"}


def _cursor_row_to_dict(cur: Any, row: Any) -> dict[str, Any] | None:
    """Convert psycopg/fake cursor rows without requiring RealDictCursor."""
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    description = getattr(cur, "description", None) or []
    names: list[str] = []
    for desc in description:
        if isinstance(desc, (tuple, list)):
            names.append(str(desc[0]))
        else:
            name = getattr(desc, "name", None)
            if name:
                names.append(str(name))
    if not names:
        return {}
    return {names[i]: row[i] for i in range(min(len(names), len(row)))}


def _fetchone_dict(cur: Any) -> dict[str, Any] | None:
    return _cursor_row_to_dict(cur, cur.fetchone())


def _fetchall_dicts(cur: Any) -> list[dict[str, Any]]:
    return [row for row in (_cursor_row_to_dict(cur, raw) for raw in cur.fetchall()) if row is not None]


def _safe_qe_workspace_id(value: Any, *, field_name: str) -> str | None:
    """Validate task/loop ids before sending them as node-API path parts."""
    text = str(value or "").strip()
    if not text:
        return None
    if text in {".", ".."} or "/" in text or "\\" in text or "\x00" in text:
        raise HTTPException(
            status_code=500,
            detail=f"QE实验记录包含非法{field_name}，拒绝清理workspace: {text!r}",
        )
    return text


def _append_unique(values: list[str], value: Any, *, field_name: str) -> str | None:
    normalized = _safe_qe_workspace_id(value, field_name=field_name)
    if normalized and normalized not in values:
        values.append(normalized)
    return normalized


@router.delete("/experiments/{experiment_id}")
async def delete_experiment(
    experiment_id: str,
    cleanup_workspace: bool = True,
):
    """删除QE实验及其所有关联数据"""
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    normalized_experiment_id = _safe_qe_workspace_id(experiment_id, field_name="experiment_id")
    if not normalized_experiment_id:
        raise HTTPException(status_code=400, detail="experiment_id不能为空")

    selected_exp: dict[str, Any] | None = None
    child_experiments: list[dict[str, Any]] = []
    related_tasks: list[dict[str, Any]] = []
    related_loops: list[dict[str, Any]] = []
    multi_alpha_groups: list[dict[str, Any]] = []

    # 1. 读取删除范围并检查实验存在且非运行中
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT experiment_id, status, qe_task_id, qe_loop_id, loop_index,
                       parent_experiment_id, is_evolution_loop, custom_params
                FROM qe_experiments
                WHERE experiment_id = %s
                """,
                (experiment_id,),
            )
            selected_exp = _fetchone_dict(cur)
            if not selected_exp:
                raise HTTPException(status_code=404, detail="实验不存在")
            selected_status = str(selected_exp.get("status") or "").lower()
            if selected_status in _QE_DELETE_ACTIVE_STATUSES:
                raise HTTPException(status_code=409, detail="实验正在运行中，请先停止")

            cur.execute(
                """
                SELECT experiment_id, status, qe_task_id, qe_loop_id, loop_index,
                       parent_experiment_id, is_evolution_loop, custom_params
                FROM qe_experiments
                WHERE parent_experiment_id = %s
                """,
                (experiment_id,),
            )
            child_experiments = _fetchall_dicts(cur)

            active_children = [
                row.get("experiment_id")
                for row in child_experiments
                if str(row.get("status") or "").lower() in _QE_DELETE_ACTIVE_STATUSES
            ]
            if active_children:
                raise HTTPException(
                    status_code=409,
                    detail=f"存在运行中的子实验，拒绝删除: {', '.join(map(str, active_children))}",
                )

            candidate_task_ids: list[str] = []
            candidate_experiment_ids: list[str] = []
            _append_unique(candidate_experiment_ids, experiment_id, field_name="experiment_id")
            _append_unique(candidate_task_ids, experiment_id, field_name="experiment_id")
            _append_unique(candidate_task_ids, selected_exp.get("qe_task_id"), field_name="qe_task_id")
            for child in child_experiments:
                _append_unique(candidate_experiment_ids, child.get("experiment_id"), field_name="experiment_id")
                _append_unique(candidate_task_ids, child.get("qe_task_id"), field_name="qe_task_id")

            cur.execute(
                """
                SELECT task_id, status, node_id, base_experiment_id
                FROM qe_evolution_tasks
                WHERE base_experiment_id = ANY(%s::text[])
                   OR task_id = ANY(%s::text[])
                """,
                (candidate_experiment_ids, candidate_task_ids),
            )
            related_tasks = _fetchall_dicts(cur)
            for task in related_tasks:
                _append_unique(candidate_task_ids, task.get("task_id"), field_name="task_id")

            active_tasks = [
                row.get("task_id")
                for row in related_tasks
                if str(row.get("status") or "").lower() in _QE_DELETE_ACTIVE_STATUSES
            ]
            if active_tasks:
                raise HTTPException(
                    status_code=409,
                    detail=f"存在运行中的演进任务，拒绝删除: {', '.join(map(str, active_tasks))}",
                )

            cur.execute(
                """
                SELECT loop_id, task_id, loop_index, status, node_id, experiment_id
                FROM qe_evolution_loops
                WHERE experiment_id = ANY(%s::text[])
                   OR task_id = ANY(%s::text[])
                """,
                (candidate_experiment_ids, candidate_task_ids),
            )
            related_loops = _fetchall_dicts(cur)
            active_loops = [
                row.get("loop_id")
                for row in related_loops
                if str(row.get("status") or "").lower() in _QE_DELETE_ACTIVE_STATUSES
            ]
            if active_loops:
                raise HTTPException(
                    status_code=409,
                    detail=f"存在运行中的Loop，拒绝删除: {', '.join(map(str, active_loops))}",
                )

            group_parent_ids = list(candidate_experiment_ids)
            parent_id = selected_exp.get("parent_experiment_id")
            if parent_id:
                _append_unique(group_parent_ids, parent_id, field_name="parent_experiment_id")
            cur.execute(
                """
                SELECT parent_experiment_id, assigned_node_id, qe_loop_id
                FROM qe_multi_alpha_groups
                WHERE parent_experiment_id = ANY(%s::text[])
                  AND assigned_node_id IS NOT NULL
                """,
                (group_parent_ids,),
            )
            multi_alpha_groups = _fetchall_dicts(cur)

    assert selected_exp is not None  # for type checkers
    default_node_id = resolve_default_qe_node_id()
    selected_is_child_loop = bool(selected_exp.get("parent_experiment_id") or selected_exp.get("is_evolution_loop")) and not child_experiments

    experiment_ids_to_delete: list[str] = []
    task_ids_to_delete_db: list[str] = []
    local_artifact_ids: list[str] = []
    task_nodes: dict[str, set[str]] = {}
    loop_nodes: dict[tuple[str, str], set[str]] = {}

    def add_local_id(value: Any, field_name: str = "experiment_id") -> None:
        _append_unique(local_artifact_ids, value, field_name=field_name)

    def add_task_node(
        task_id_value: Any,
        node_id_value: Any = None,
        *,
        field_name: str = "task_id",
        allow_default_node: bool = True,
    ) -> str | None:
        task_id_norm = _append_unique(local_artifact_ids, task_id_value, field_name=field_name)
        if not task_id_norm:
            return None
        node_text = str(node_id_value or "").strip()
        if not node_text and not allow_default_node:
            return task_id_norm
        node_norm = node_text or default_node_id
        task_nodes.setdefault(task_id_norm, set()).add(node_norm)
        return task_id_norm

    def add_loop_node(task_id_value: Any, loop_id_value: Any, node_id_value: Any = None) -> None:
        task_id_norm = _safe_qe_workspace_id(task_id_value, field_name="qe_task_id")
        loop_id_norm = _safe_qe_workspace_id(loop_id_value, field_name="qe_loop_id")
        if not task_id_norm or not loop_id_norm:
            return
        if loop_id_norm.startswith(task_id_norm + "_"):
            loop_id_norm = loop_id_norm[len(task_id_norm) + 1:]
        node_norm = str(node_id_value or "").strip() or default_node_id
        loop_nodes.setdefault((task_id_norm, loop_id_norm), set()).add(node_norm)
        add_local_id(task_id_norm, "qe_task_id")

    selected_node = _get_recorded_experiment_node(selected_exp)
    selected_qe_task_id = selected_exp.get("qe_task_id")

    if selected_is_child_loop:
        task_id_for_loop = _safe_qe_workspace_id(selected_qe_task_id, field_name="qe_task_id")
        loop_id_for_loop = _safe_qe_workspace_id(selected_exp.get("qe_loop_id"), field_name="qe_loop_id")
        if not task_id_for_loop or not loop_id_for_loop:
            raise HTTPException(
                status_code=500,
                detail="QE子实验缺少qe_task_id/qe_loop_id，无法通过节点API清理Loop workspace，数据库记录未删除",
            )
        _append_unique(experiment_ids_to_delete, experiment_id, field_name="experiment_id")
        add_local_id(experiment_id, "experiment_id")
        add_local_id(task_id_for_loop, "qe_task_id")
        add_loop_node(task_id_for_loop, loop_id_for_loop, selected_node)
        for loop in related_loops:
            if loop.get("experiment_id") == experiment_id:
                add_loop_node(loop.get("task_id") or task_id_for_loop, loop.get("loop_id") or loop_id_for_loop, loop.get("node_id"))
        for task in related_tasks:
            if task.get("task_id") == task_id_for_loop:
                add_loop_node(task_id_for_loop, loop_id_for_loop, task.get("node_id"))
        for group in multi_alpha_groups:
            if str(group.get("qe_loop_id") or "") == str(loop_id_for_loop):
                add_loop_node(task_id_for_loop, loop_id_for_loop, group.get("assigned_node_id"))
    else:
        all_experiments = [selected_exp] + child_experiments
        for exp in all_experiments:
            _append_unique(experiment_ids_to_delete, exp.get("experiment_id"), field_name="experiment_id")
            add_local_id(exp.get("experiment_id"), "experiment_id")
            node_id = _get_recorded_experiment_node(exp)
            add_task_node(
                exp.get("qe_task_id") or exp.get("experiment_id"),
                node_id,
                field_name="qe_task_id",
                allow_default_node=exp is selected_exp,
            )

        # Legacy rows may have the workspace named by experiment_id rather than qe_task_id.
        add_task_node(experiment_id, selected_node, field_name="experiment_id")
        for task in related_tasks:
            task_id = add_task_node(task.get("task_id"), task.get("node_id"), field_name="task_id")
            if task_id and task_id not in task_ids_to_delete_db:
                task_ids_to_delete_db.append(task_id)
        for loop in related_loops:
            add_task_node(loop.get("task_id"), loop.get("node_id"), field_name="task_id", allow_default_node=False)
        for group in multi_alpha_groups:
            add_task_node(selected_qe_task_id or experiment_id, group.get("assigned_node_id"), field_name="qe_task_id")

        for task_id in list(task_nodes):
            if task_id not in task_ids_to_delete_db:
                task_ids_to_delete_db.append(task_id)

    cleanup_results: list[dict[str, Any]] = []

    # 2. 清理QE执行节点workspace。只走节点API；失败时在DB删除前 fail-fast。
    if cleanup_workspace:
        try:
            for (task_id, loop_id), nodes in sorted(loop_nodes.items()):
                for node_id in sorted(nodes):
                    client = QEWorkspaceClient.for_node(node_id)
                    async with client:
                        await client.cleanup_loop_workspace(task_id, loop_id)
                    cleanup_results.append(
                        {"scope": "loop", "node_id": node_id, "task_id": task_id, "loop_id": loop_id}
                    )
                    logger.info("已通过节点API清理QE Loop workspace: node=%s task=%s loop=%s", node_id, task_id, loop_id)
            for task_id, nodes in sorted(task_nodes.items()):
                for node_id in sorted(nodes):
                    client = QEWorkspaceClient.for_node(node_id)
                    async with client:
                        await client.cleanup_task_workspace(task_id)
                    cleanup_results.append({"scope": "task", "node_id": node_id, "task_id": task_id})
                    logger.info("已通过节点API清理QE task workspace: node=%s task=%s", node_id, task_id)
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("QE workspace cleanup failed before DB delete: experiment=%s", experiment_id)
            raise HTTPException(
                status_code=502,
                detail=(
                    "QE workspace清理失败，数据库记录和本地AIstock缓存未删除；"
                    f"请确认执行节点API可用后重试。原始错误: {e}"
                ),
            ) from e

    # 2b. Clean AIstock-owned local artifacts only. Worker workspaces are cleaned via node API above.
    from ..services.quantevolver.config_composer import QE_EXPERIMENTS_ROOT
    from ..services.strategy_package.workspace_policy import (
        remove_aistock_artifact_tree,
        unlink_aistock_artifact_files,
    )

    sota_dir = Path(os.environ.get("QE_SOTA_ASSETS_DIR", str(AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_sota_assets")))
    local_cleanup_roots = [QE_EXPERIMENTS_ROOT, sota_dir]
    cleaned_dirs: list[str] = []
    local_cleanup_errors: list[str] = []
    for artifact_id in local_artifact_ids or [experiment_id]:
        for dir_path in [QE_EXPERIMENTS_ROOT / artifact_id, sota_dir / artifact_id]:
            try:
                if remove_aistock_artifact_tree(
                    dir_path,
                    purpose=f"QE experiment local artifact cleanup: {experiment_id}",
                    allowed_roots=local_cleanup_roots,
                    ignore_errors=False,
                ):
                    cleaned_dirs.append(str(dir_path))
                    logger.info(f"Cleaned local AIstock experiment directory: {dir_path}")
            except Exception as e:
                local_cleanup_errors.append(f"local AIstock artifact cleanup failed ({dir_path}): {e}")
    # Clean local Optuna study files under the AIstock-owned SOTA root.
    optuna_deleted = 0
    try:
        for artifact_id in local_artifact_ids or [experiment_id]:
            optuna_deleted += unlink_aistock_artifact_files(
                sota_dir / "optuna_studies",
                f"{artifact_id}_*.db",
                purpose=f"QE experiment Optuna study cleanup: {experiment_id}",
                allowed_roots=[sota_dir],
            )
        if optuna_deleted:
            logger.info("Cleaned %s Optuna study file(s) for experiment %s", optuna_deleted, experiment_id)
    except Exception as e:
        local_cleanup_errors.append(f"Optuna study cleanup failed: {e}")

    if local_cleanup_errors:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "本地AIstock缓存清理失败，数据库记录未删除",
                "errors": local_cleanup_errors,
                "worker_cleanup_results": cleanup_results,
            },
        )

    # 3. 清理DB记录（事务内，按外键依赖顺序删除）
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 删除多Alpha组记录
            cur.execute(
                "DELETE FROM qe_multi_alpha_groups WHERE parent_experiment_id = ANY(%s::text[])",
                (experiment_ids_to_delete,),
            )
            # 删除演进循环记录（引用 qe_evolution_tasks + qe_experiments）
            cur.execute(
                "DELETE FROM qe_evolution_loops WHERE experiment_id = ANY(%s::text[]) OR task_id = ANY(%s::text[])",
                (experiment_ids_to_delete, task_ids_to_delete_db),
            )
            if not selected_is_child_loop:
                # 删除演进任务记录（base_experiment_id 引用 qe_experiments）
                cur.execute(
                    "DELETE FROM qe_evolution_tasks WHERE base_experiment_id = ANY(%s::text[]) OR task_id = ANY(%s::text[])",
                    (experiment_ids_to_delete, task_ids_to_delete_db),
                )
            # 删除因子实验指标（主实验 + 子Loop）
            cur.execute(
                "DELETE FROM qe_factor_experiment_metrics WHERE experiment_id = ANY(%s::text[])",
                (experiment_ids_to_delete,),
            )
            # 删除所有子Loop实验记录
            cur.execute(
                "DELETE FROM qe_experiments WHERE parent_experiment_id = ANY(%s::text[])",
                (experiment_ids_to_delete,),
            )
            # 删除主实验记录
            cur.execute(
                "DELETE FROM qe_experiments WHERE experiment_id = ANY(%s::text[])",
                (experiment_ids_to_delete,),
            )
        conn.commit()

    return {
        "ok": True,
        "experiment_id": experiment_id,
        "worker_workspace_cleanup_mode": "node_api_only" if cleanup_workspace else "skipped",
        "worker_cleanup_results": cleanup_results,
        "local_cleanup": {
            "cleaned_dirs": cleaned_dirs,
            "optuna_files_deleted": optuna_deleted,
        },
        "deleted_experiment_ids": experiment_ids_to_delete,
    }


# ============================================================
# 执行算法库 API
# ============================================================


class UpdateExecutionAlgoRequest(BaseModel):
    is_enabled: Optional[bool] = None
    default_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    sort_order: Optional[int] = None


@router.get("/execution-algorithms")
def list_execution_algorithms(
    category: Optional[str] = Query(None, description="按分类筛选"),
    grade: Optional[str] = Query(None, description="按评级筛选"),
    enabled_only: bool = Query(False, description="仅显示启用的算法"),
):
    """获取全部执行算法列表。"""
    try:
        from ..services.quantevolver.execution_analyst import ExecutionAlgoAnalyst
        analyst = ExecutionAlgoAnalyst()
        items = analyst.get_all_algorithms()

        if category:
            items = [a for a in items if a.get("category") == category]
        if grade:
            items = [a for a in items if a.get("grade") == grade]
        if enabled_only:
            items = [a for a in items if a.get("is_enabled")]

        from ..services.quantevolver.config_composer import (
            ConfigComposer,
            SUPPORTED_QE_EXECUTION_ALGOS,
        )
        qe_modules = {
            "TWAP": "tail_twap_strategy.TailTWAPWithLimitStrategy",
            "CLOSE_PRICE": "close_execution_strategy.CloseExecutionStrategy",
            "V24_PLAN": "tail_twap_v24_strategy.TailTWAPWithV24PlanStrategy",
            "V25_TWO_STAGE": "tail_twap_v25_strategy.TailTWAPWithV25TwoStageStrategy",
            "V25_1_SMALL_CAP": "tail_twap_v25_1_strategy.TailTWAPWithV25_1SmallCapStrategy",
        }
        for item in items:
            code = str(item.get("algo_code") or "").upper()
            try:
                normalized = ConfigComposer._normalize_execution_algo(code)
                item["qe_supported"] = normalized in SUPPORTED_QE_EXECUTION_ALGOS
                item["qe_effective_algo"] = normalized
                item["qe_effective_module"] = qe_modules.get(normalized)
                item["qe_support_message"] = "supported by QE/Qlib config composer"
            except ValueError as exc:
                item["qe_supported"] = False
                item["qe_effective_algo"] = None
                item["qe_effective_module"] = None
                item["qe_support_message"] = str(exc)

        return {"ok": True, "total": len(items), "items": items}
    except Exception as e:
        logger.exception("获取执行算法列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/execution-algorithms/batch-analyze")
def batch_analyze_execution_algorithms(
    use_llm: bool = Query(True, description="是否使用LLM分析"),
):
    """批量分析所有执行算法（SSE 流式）。"""
    from ..services.quantevolver.execution_analyst import ExecutionAlgoAnalyst

    async def event_generator():
        try:
            analyst = ExecutionAlgoAnalyst()
            async for event in analyst.batch_analyze(use_llm=use_llm):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.exception("批量执行算法分析失败")
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/execution-algorithms/{algo_code}")
def get_execution_algorithm(algo_code: str):
    """获取单个执行算法详情。"""
    try:
        from ..services.quantevolver.execution_analyst import ExecutionAlgoAnalyst
        analyst = ExecutionAlgoAnalyst()
        data = analyst.get_algorithm(algo_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"算法 {algo_code} 不存在")
        return {"ok": True, "data": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取执行算法详情失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execution-algorithms/{algo_code}/analyze")
def analyze_execution_algorithm(
    algo_code: str,
    use_llm: bool = Query(True, description="是否使用LLM分析"),
):
    """触发单个算法 LLM 分析。"""
    try:
        from ..services.quantevolver.execution_analyst import ExecutionAlgoAnalyst
        analyst = ExecutionAlgoAnalyst()
        result = analyst.analyze_algorithm(algo_code, use_llm=use_llm)
        if not result.get("ok"):
            raise HTTPException(status_code=404, detail=result.get("error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"分析执行算法失败: {algo_code}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/execution-algorithms/{algo_code}")
def update_execution_algorithm(algo_code: str, req: UpdateExecutionAlgoRequest):
    """更新执行算法配置。"""
    try:
        from ..services.quantevolver.execution_analyst import ExecutionAlgoAnalyst
        analyst = ExecutionAlgoAnalyst()
        updates = req.model_dump(exclude_none=True)
        result = analyst.update_algorithm(algo_code, updates)
        if not result.get("ok"):
            raise HTTPException(status_code=404, detail=result.get("error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"更新执行算法失败: {algo_code}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 股票池黑名单 API（申万二级行业预筛选）
# ============================================================

class Sw2PoolConfigItem(BaseModel):
    sw2_code: str
    sw2_name: Optional[str] = None
    sw1_code: Optional[str] = None
    sw1_name: Optional[str] = None
    status: str = "blocked"
    effective_from: Optional[str] = None  # YYYY-MM-DD 或 None
    effective_to: Optional[str] = None    # YYYY-MM-DD 或 None
    is_active: bool = True
    reason: Optional[str] = None
    updated_by: Optional[str] = None


@router.get("/stock-pool/sw2-tree", summary="获取申万一级+二级行业树（用于前端选择器）")
def get_sw2_tree():
    """从 sw_index_member 中聚合申万一级/二级行业列表，返回树形结构。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT l1_code,
                           MAX(NULLIF(l1_name, '')) AS l1_name,
                           l2_code,
                           MAX(NULLIF(l2_name, '')) AS l2_name
                    FROM market.sw_index_member
                    WHERE l2_code IS NOT NULL AND l2_code != ''
                      AND l1_code LIKE '%.SI'
                    GROUP BY l1_code, l2_code
                    ORDER BY l1_code, l2_code
                """)
                rows = cur.fetchall()

        # 聚合为树形
        tree: dict = {}
        for l1_code, l1_name, l2_code, l2_name in rows:
            if l1_code not in tree:
                tree[l1_code] = {"l1_code": l1_code, "l1_name": l1_name, "children": []}
            tree[l1_code]["children"].append({"l2_code": l2_code, "l2_name": l2_name})

        return {"ok": True, "tree": list(tree.values())}
    except Exception as e:
        logger.exception("获取申万行业树失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock-pool/blacklist", summary="获取当前黑名单配置")
def get_blacklist():
    """获取 sw2_pool_config 表中所有记录。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sw2_code, sw2_name, sw1_code, sw1_name,
                           status, effective_from, effective_to,
                           is_active, reason, updated_at, updated_by
                    FROM sw2_pool_config
                    ORDER BY sw1_code, sw2_code
                """)
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        # 日期序列化
        for r in rows:
            for f in ("effective_from", "effective_to", "updated_at"):
                if r[f] is not None:
                    r[f] = str(r[f])
        return {"ok": True, "items": rows}
    except Exception as e:
        logger.exception("获取黑名单配置失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stock-pool/blacklist", summary="新增或更新黑名单行业（upsert）")
def upsert_blacklist_item(item: Sw2PoolConfigItem):
    """插入或更新一条黑名单记录（以 sw2_code 为主键 upsert）。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sw2_pool_config
                        (sw2_code, sw2_name, sw1_code, sw1_name, status,
                         effective_from, effective_to, is_active, reason, updated_by, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (sw2_code) DO UPDATE SET
                        sw2_name     = EXCLUDED.sw2_name,
                        sw1_code     = EXCLUDED.sw1_code,
                        sw1_name     = EXCLUDED.sw1_name,
                        status       = EXCLUDED.status,
                        effective_from = EXCLUDED.effective_from,
                        effective_to = EXCLUDED.effective_to,
                        is_active    = EXCLUDED.is_active,
                        reason       = EXCLUDED.reason,
                        updated_by   = EXCLUDED.updated_by,
                        updated_at   = NOW()
                """, (
                    item.sw2_code, item.sw2_name, item.sw1_code, item.sw1_name,
                    item.status,
                    item.effective_from or None,
                    item.effective_to or None,
                    item.is_active, item.reason, item.updated_by
                ))
            conn.commit()
        return {"ok": True, "sw2_code": item.sw2_code}
    except Exception as e:
        logger.exception(f"upsert 黑名单失败: {item.sw2_code}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/stock-pool/blacklist/{sw2_code}", summary="删除黑名单行业")
def delete_blacklist_item(sw2_code: str):
    """从黑名单中删除指定申万二级行业。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM sw2_pool_config WHERE sw2_code = %s", (sw2_code,))
            conn.commit()
        return {"ok": True, "sw2_code": sw2_code}
    except Exception as e:
        logger.exception(f"删除黑名单失败: {sw2_code}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stock-pool/generate", summary="生成过滤后的股票池文件")
def generate_stock_pool(date: Optional[str] = None):
    """
    调用 generate_stock_pool.py 生成 filtered_pool_{date}.txt。
    date: YYYY-MM-DD，默认今日。返回生成的 WSL 路径。
    """
    import subprocess
    import sys
    from pathlib import Path
    from datetime import date as date_cls

    target_date = date or str(date_cls.today())
    script = Path(__file__).parent.parent.parent / "scripts" / "generate_stock_pool.py"
    try:
        result = subprocess.run(
            [sys.executable, str(script), "--date", target_date],
            capture_output=True, text=True, timeout=60,
            encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stderr[-500:])
        # 从脚本 stdout 解析 WSL 路径和股票数量
        wsl_path = ""
        stock_count = 0
        for line in result.stdout.splitlines():
            if line.startswith("[WSL PATH]"):
                wsl_path = line.split("[WSL PATH]", 1)[1].strip()
            elif "过滤后:" in line and "只股票" in line:
                import re
                m = re.search(r'过滤后:\s*(\d+)\s*只', line)
                if m:
                    stock_count = int(m.group(1))
        if not wsl_path:
            raise HTTPException(status_code=500, detail="股票池脚本执行完成但未返回 [WSL PATH]")
        from ..services.quantevolver.blacklist_snapshot import get_effective_blacklist_snapshot
        blacklist_snapshot = get_effective_blacklist_snapshot(target_date)
        return {
            "ok": True,
            "date": target_date,
            "wsl_path": wsl_path,
            "stock_count": stock_count,
            "blacklist_snapshot": blacklist_snapshot,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("生成股票池文件失败")
        raise HTTPException(status_code=500, detail=str(e))


# ── IC 衰变趋势查询 ──

@router.get("/factors/{factor_name}/ic-decay-trend")
def get_ic_decay_trend(
    factor_name: str,
    eval_window: str = "full",
):
    """查询因子在不同 snapshot_date 下的 IC 指标变化趋势。

    返回按 snapshot_date 排序的 IC/ICIR/Rank IC 序列，用于绘制衰变趋势图。
    """
    from ..db.pg_pool import get_conn
    from ..services.quantevolver.factor_official_evaluation_service import CALC_ENGINE

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    snapshot_date,
                    data_start,
                    data_end,
                    ic_mean,
                    rank_ic_mean,
                    icir,
                    rank_icir,
                    ic_positive_ratio,
                    n_trading_days,
                    rank_ic_1d,
                    rank_ic_5d,
                    rank_ic_10d,
                    rank_ic_20d,
                    top_annual_return,
                    top_excess_annual_return,
                    top_sharpe,
                    group_return_monotonicity,
                    calculated_at
                FROM aistock_factor_metrics
                WHERE factor_name = %s
                  AND eval_window = %s
                  AND calc_engine = %s
                  AND snapshot_date IS NOT NULL
                ORDER BY snapshot_date ASC
            """, (factor_name, eval_window, CALC_ENGINE))

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    trend = []
    for row in rows:
        d = dict(zip(columns, row))
        for k in ("snapshot_date", "data_start", "data_end", "calculated_at"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        trend.append(d)

    return {
        "factor_name": factor_name,
        "eval_window": eval_window,
        "calc_engine": CALC_ENGINE,
        "count": len(trend),
        "trend": trend,
    }

