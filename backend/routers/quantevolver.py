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
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..db.pg_pool import get_conn

logger = logging.getLogger("aistock.routers.quantevolver")

router = APIRouter(prefix="/quantevolver", tags=["QuantEvolver"])


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
    factor_names: List[str]
    factor_sources: Optional[Dict[str, str]] = None
    model_id: Optional[str] = None
    strategy_id: Optional[str] = None
    data_split: Optional[Dict[str, str]] = None
    custom_params: Optional[Dict[str, Any]] = None
    experiment_name: Optional[str] = None
    dispatch_mode: Optional[str] = Field(None, description="调度模式: normal / evolution")
    evolution_params: Optional[Dict[str, Any]] = Field(None, description="演进参数（dispatch_mode=evolution时）")


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
            from pathlib import Path
            default_root = Path("f:/Dev/AIstock/rdagent_assets/rdagent_tasks") / task_id
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
    sort_field: Optional[str] = Query(None, description="排序字段"),
    sort_order: Optional[str] = Query("desc", description="排序方向: asc/desc"),
    limit: int = Query(200, ge=1, le=1000),
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

        # category / grade 筛选：__empty__ 表示未分类/未评级
        if category == "__empty__":
            cl_conditions.append("cl.category IS NULL")
        elif category:
            cl_conditions.append("cl.category = %s")
            cl_params.append(category)
        if grade == "__empty__":
            cl_conditions.append("cl.grade IS NULL")
        elif grade:
            cl_conditions.append("cl.grade = %s")
            cl_params.append(grade)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        # 分类表条件追加到 WHERE（需要 LEFT JOIN 后才能使用）
        cl_where = (" AND " + " AND ".join(cl_conditions)) if cl_conditions else ""

        # 排序字段白名单映射（防SQL注入）
        SORT_FIELD_MAP = {
            "factor_name": "c.factor_name",
            "source": "c.source",
            "ic": "c.ic",
            "sharpe": "c.sharpe",
            "annualized_return": "c.annualized_return",
            "is_sota_factor": "c.is_sota_factor",
            "ind_rank_ic": "m.rank_ic_mean",
            "ind_ic": "m.ic_mean",
            "ind_sharpe": "m.top_excess_sharpe",
            "ind_annual_return": "m.top_excess_annual_return",
            "ind_icir": "m.icir",
            "has_ind_metrics": "m.ic_mean",
            "grade": "cl.grade",
            "category": "cl.category",
            "factor_dimension": "cl.factor_dimension",
        }
        direction = "ASC" if sort_order == "asc" else "DESC"
        if sort_field and sort_field in SORT_FIELD_MAP:
            if sort_field == "grade":
                # S>A>B>C>D 自定义排序
                order_clause = f"CASE cl.grade WHEN 'S' THEN 5 WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 WHEN 'D' THEN 1 ELSE 0 END {direction} NULLS LAST"
            else:
                col = SORT_FIELD_MAP[sort_field]
                order_clause = f"{col} {direction} NULLS LAST"
        else:
            order_clause = "c.is_sota_factor DESC NULLS LAST, c.ic DESC NULLS LAST"

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 当有 category/grade 筛选时，count 也需要 JOIN 分类表
                if cl_conditions:
                    cur.execute(
                        f"""SELECT COUNT(*) FROM aistock_factor_catalog c
                            LEFT JOIN qe_factor_classification cl
                                ON cl.factor_name = c.factor_name AND cl.factor_source = c.source
                            WHERE {where_clause}{cl_where}""",
                        params + cl_params,
                    )
                else:
                    cur.execute(
                        f"SELECT COUNT(*) FROM aistock_factor_catalog c WHERE {where_clause}",
                        params,
                    )
                total = cur.fetchone()[0]

                # LEFT JOIN 独立因子指标 + 分类表
                cur.execute(f"""
                    SELECT c.factor_name, c.source, c.expression, c.ic, c.sharpe,
                           c.annualized_return, c.is_sota_factor, c.catalog_source,
                           c.description_cn, c.generated_at_utc,
                           m.ic_mean AS ind_ic, m.top_excess_sharpe AS ind_sharpe,
                           m.top_excess_annual_return AS ind_annual_return,
                           m.rank_ic_mean AS ind_rank_ic, m.icir AS ind_icir,
                           cl.category, cl.grade, cl.grade_reason,
                           cl.classification_reason, cl.factor_dimension,
                           cl.description AS cl_description, cl.id AS classification_id
                    FROM aistock_factor_catalog c
                    LEFT JOIN LATERAL (
                        SELECT ic_mean, top_excess_sharpe, top_excess_annual_return,
                               rank_ic_mean, icir
                        FROM aistock_factor_metrics
                        WHERE factor_name = c.factor_name AND eval_window = 'full'
                        ORDER BY calculated_at DESC
                        LIMIT 1
                    ) m ON TRUE
                    LEFT JOIN qe_factor_classification cl
                        ON cl.factor_name = c.factor_name AND cl.factor_source = c.source
                    WHERE {where_clause}{cl_where}
                    ORDER BY {order_clause}
                    LIMIT %s OFFSET %s
                """, params + cl_params + [limit, offset])
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 保留 TASK 原始指标（ic/sharpe/annualized_return）和独立指标（ind_*）同时返回
        # 前端自行决定展示优先级，不在后端覆盖

        return {"ok": True, "total": total, "items": rows}
    except Exception as e:
        logger.exception("获取因子列表失败")
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

                # 3. factor_live_track
                cur.execute("DELETE FROM factor_live_track WHERE factor_catalog_id = %s", (catalog_id,))
                deleted_counts["factor_live_track"] = cur.rowcount

                # 4. qe_factor_classification
                cur.execute(
                    "DELETE FROM qe_factor_classification WHERE factor_name = %s AND factor_source = %s",
                    (factor_name, source),
                )
                deleted_counts["qe_factor_classification"] = cur.rowcount

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
                # 单因子 parquet 缓存
                single_parquet = os.path.join(
                    _project_root, "rdagent_assets", "factor_values", "single",
                    f"{factor_name}.parquet",
                )
                if os.path.isfile(single_parquet):
                    os.remove(single_parquet)
                    cleaned_files.append(single_parquet)
                    logger.info(f"已删除因子缓存: {single_parquet}")

                # QE 因子源代码文件
                qe_code = os.path.join(
                    _project_root, "rdagent_assets", "qe_factors",
                    f"{factor_name}.py",
                )
                if os.path.isfile(qe_code):
                    os.remove(qe_code)
                    cleaned_files.append(qe_code)
                    logger.info(f"已删除QE因子代码: {qe_code}")

                # 合并面板缓存（删除因子后缓存含已删除因子列，必须失效）
                from ..services.quantevolver.factor_value_loader import FactorValueLoader
                pipeline_dir = os.path.join(
                    _project_root, "rdagent_assets", "factor_values",
                )
                FactorValueLoader.invalidate_merged_cache(pipeline_dir)
                cleaned_files.append("_merged_panel.parquet")
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
    """设置因子为可用/不可用（软删除）。is_available=false 不参与 SOTA 保护和新实验，可恢复。"""
    from ..db.pg_pool import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE aistock_factor_catalog SET is_available = %s, updated_at = NOW() "
                "WHERE factor_name = %s AND source = %s",
                (req.is_available, factor_name, req.source),
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


class ManualFactorCreate(BaseModel):
    factor_name: str = Field(..., description="因子名（m_ 开头，英文+下划线）")
    code_text: str = Field(..., description="因子 Python 代码")
    description: Optional[str] = Field(None, description="因子描述")
    expression: Optional[str] = Field(None, description="因子表达式（可选）")


class ManualFactorValidate(BaseModel):
    factor_name: str = Field(..., description="因子名")
    code_text: str = Field(..., description="因子 Python 代码")


class BatchComputeMetricsUnified(BaseModel):
    factor_names: Optional[List[str]] = Field(None, description="指定因子名列表")
    all_available: bool = Field(False, description="True=全部 is_available 因子")


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
                    cur.execute(
                        "UPDATE aistock_factor_catalog SET is_available = FALSE, updated_at = NOW() "
                        "WHERE factor_name = %s AND source = %s",
                        (fn, src),
                    )
                    if cur.rowcount > 0:
                        succeeded.append({"factor_name": fn, "source": src})
                    else:
                        failed.append({"factor_name": fn, "error": f"因子不存在 (source={src})"})
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
        import os
        from pathlib import Path
        from ..db.pg_pool import get_conn
        
        # 保存源码到文件系统
        strategies_dir = Path("F:/Dev/AIstock/rdagent_assets/qe_strategies")
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
        from pathlib import Path
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
            strategies_dir = Path("F:/Dev/AIstock/rdagent_assets/qe_strategies")
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
        from pathlib import Path
        from ..db.pg_pool import get_conn
        
        # 保存源码到文件系统
        strategies_dir = Path("F:/Dev/AIstock/rdagent_assets/qe_strategies")
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
    """分析指定因子（分类+评级）。"""
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
    """生成QLib配置文件。dispatch_mode=evolution时标记为待演进。"""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer
        cc = ConfigComposer()
        result = cc.compose_experiment(
            factor_names=req.factor_names,
            factor_sources=req.factor_sources,
            model_id=req.model_id,
            strategy_id=req.strategy_id,
            data_split=req.data_split,
            custom_params=req.custom_params,
            experiment_name=req.experiment_name,
        )

        if req.dispatch_mode == "evolution":
            result["evolution_pending"] = True
            result["evolution_params"] = req.evolution_params or {}

        return result
    except Exception as e:
        logger.exception("配置生成失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments")
def list_experiments(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """获取实验列表。"""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer
        cc = ConfigComposer()
        result = cc.list_experiments(limit=limit, offset=offset)
        return result
    except Exception as e:
        logger.exception("获取实验列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}")
def get_experiment_detail(experiment_id: str):
    """获取实验详情。"""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer
        cc = ConfigComposer()
        result = cc.get_experiment_detail(experiment_id)
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
    禁止本地文件访问，所有数据通过 API 获取。
    """
    from ..services.quantevolver.config_composer import ConfigComposer
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    cc = ConfigComposer()
    exp_record = cc._get_experiment_record(experiment_id)
    if not exp_record:
        raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")

    qe_task_id = exp_record.get("qe_task_id") or exp_record.get("experiment_name")
    qe_loop_id = exp_record.get("qe_loop_id") or "Loop1"
    if not qe_task_id:
        raise HTTPException(status_code=400, detail="实验缺少 qe_task_id，无法同步")

    try:
        async with QEWorkspaceClient() as client:
            metrics = await client.get_loop_metrics(qe_task_id, qe_loop_id)
            _update_experiment_with_metrics(experiment_id, metrics)
            return {"ok": True, "experiment_id": experiment_id, "metrics": metrics}
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


class ExperimentSelectionRequest(BaseModel):
    trade_date: Optional[str] = Field(None, description="推理日期 YYYY-MM-DD，默认当日")
    cutoff_date: Optional[str] = Field(None, description="数据截止日期 YYYY-MM-DD；若设置，则推理取数不得晚于该日期")
    top_k: int = Field(50, ge=1, le=500, description="返回候选数量，默认 50")


@router.post("/experiments/{experiment_id}/selection")
def trigger_experiment_selection(experiment_id: str, req: ExperimentSelectionRequest):
    """
    基于QE实验进行实盘选股（完全参考TASK选股架构）
    
    重要说明：
    1. 使用实盘最新数据从数据库获取，禁止使用回测历史数据
    2. 仅从实验中获取：模型权重、特征序列
    3. 重新计算所有股票的评分（使用实盘数据）
    4. 参考TASK选股逻辑，生成所需数据集（h5文件、static_factors.parquet等）
    5. 共用数据服务层，不干扰TASK选股功能
    """
    try:
        from ..services.quantevolver.qe_selection_service import build_experiment_selection
        
        result = build_experiment_selection(
            experiment_id=experiment_id,
            trade_date=req.trade_date,
            cutoff_date=req.cutoff_date,
            top_k=req.top_k
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"实验选股失败: experiment_id={experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Phase 2 API: 单因子独立指标（17项）
# ============================================================

@router.post("/factors/batch-fetch-metrics")
def batch_fetch_factor_metrics(
    factor_names: List[str] = Body(..., embed=True),
):
    """批量获取选中因子的独立指标。

    流程：factor_names → 查 aistock_factor_catalog 得到 source_task_id → 调 RD-Agent API 计算 → 写入 aistock_factor_metrics。
    """
    from ..db.pg_pool import get_conn
    from ..services.rdagent_factor_metrics_sync import sync_factor_metrics_batch

    # 1) 查找因子对应的 source_task_id
    task_id_map: Dict[str, List[str]] = {}  # task_id -> [factor_names]
    with get_conn() as conn:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(factor_names))
            cur.execute(f"""
                SELECT factor_name, source_task_id
                FROM aistock_factor_catalog
                WHERE factor_name IN ({placeholders})
                  AND source_task_id IS NOT NULL
            """, factor_names)
            for row in cur.fetchall():
                fname, tid = row
                task_id_map.setdefault(tid, []).append(fname)

    if not task_id_map:
        return {"ok": False, "error": "所选因子均无 source_task_id，无法获取独立指标"}

    # 2) 按 task_id 批量调用 RD-Agent API
    task_ids = list(task_id_map.keys())
    results = sync_factor_metrics_batch(task_ids)

    ok_count = sum(1 for r in results if r.ok)
    total_inserted = sum(r.metrics_inserted for r in results)
    total_skipped = sum(r.metrics_skipped for r in results)

    # 汇总失败原因到顶层 error 字段，方便前端直接展示
    fail_errors = []
    for r in results:
        if not r.ok and r.errors:
            tid_short = r.task_id[:20]
            fail_errors.append(f"{tid_short}: {r.errors[0]}")
    error_summary = "; ".join(fail_errors) if fail_errors else None

    return {
        "ok": ok_count > 0,
        "error": error_summary,
        "total_tasks": len(results),
        "success_count": ok_count,
        "fail_count": len(results) - ok_count,
        "total_metrics_inserted": total_inserted,
        "total_metrics_skipped": total_skipped,
        "task_factor_map": {tid: fnames for tid, fnames in task_id_map.items()},
        "details": [
            {
                "task_id": r.task_id,
                "ok": r.ok,
                "factor_count": r.factor_count,
                "inserted": r.metrics_inserted,
                "skipped": r.metrics_skipped,
                "errors": r.errors,
            }
            for r in results
        ],
    }


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
    )
    return result


@router.post("/factors/batch-compute-metrics-unified", summary="统一批量独立指标计算")
async def batch_compute_metrics_unified(req: BatchComputeMetricsUnified):
    """统一批量计算因子独立指标（所有因子通用，不依赖 RDAgent task）。

    从 DB 读取 code_text → WSL 批量执行 → engine 计算 17 指标 × 4 窗口。
    """
    from ..services.manual_factor_service import ManualFactorService
    svc = ManualFactorService()
    result = await svc.batch_compute_metrics(
        factor_names=req.factor_names,
        all_available=req.all_available,
    )
    return result


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
    from ..services.manual_factor_service import ManualFactorService

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
        })

        # ── Phase 1: IC 指标计算（统一计算，不依赖 task） ──
        BATCH_SIZE = 10
        yield _sse({"type": "phase_start", "phase": "ic_metrics", "phase_label": "IC指标计算(统一)", "total_tasks": len(factor_names)})
        ic_success = 0
        ic_failed = 0
        total_inserted = 0

        svc_metrics = ManualFactorService()
        # 分批计算，每批推送进度
        for i in range(0, len(factor_names), BATCH_SIZE):
            batch = factor_names[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            yield _sse({"type": "task_progress", "phase": "ic_metrics", "status": "computing",
                        "batch": batch_num, "factors": batch, "current": i, "total": len(factor_names)})
            try:
                result = await svc_metrics.batch_compute_metrics(factor_names=batch)
                if result.get("success"):
                    db_res = result.get("db_result", {})
                    batch_ok = len(result.get("factors", {}))
                    batch_err = len(batch) - batch_ok
                    batch_saved = db_res.get("inserted", 0)
                else:
                    batch_ok = 0
                    batch_err = len(batch)
                    batch_saved = 0
                ic_success += batch_ok
                ic_failed += batch_err
                total_inserted += batch_saved
                yield _sse({"type": "task_progress", "phase": "ic_metrics", "status": "done",
                            "batch": batch_num, "ok": batch_ok, "failed": batch_err,
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
    """查询因子的独立17项指标（从 aistock_factor_metrics 表读取）。"""
    from ..db.pg_pool import get_conn

    conditions = ["factor_name = %s"]
    params: list = [factor_name]
    if eval_window:
        conditions.append("eval_window = %s")
        params.append(eval_window)

    where = " AND ".join(conditions)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT factor_name, eval_window, data_start, data_end, calculated_at,
                       return_horizon, universe,
                       ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir,
                       ic_positive_ratio,
                       top_annual_return, top_excess_annual_return, top_sharpe,
                       top_max_drawdown, top_excess_sharpe, benchmark_annual_return,
                       group_return_monotonicity, turnover, ic_decay_half_life,
                       ic_csz_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
                       coverage, n_trading_days, source_task_id, calc_engine
                FROM aistock_factor_metrics
                WHERE {where}
                ORDER BY calculated_at DESC
                LIMIT %s
            """, params + [limit])
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    # 序列化 date/datetime 字段
    for row in rows:
        for k in ("data_start", "data_end", "calculated_at"):
            if row.get(k) is not None:
                row[k] = str(row[k])

    return {"ok": True, "factor_name": factor_name, "metrics": rows, "total": len(rows)}


@router.get("/factors/independent-metrics-summary")
def get_independent_metrics_summary():
    """批量返回所有有独立指标的因子摘要(full窗口最新一条的ic/sharpe/年化)。"""
    from ..db.pg_pool import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (factor_name)
                    factor_name, ic_mean, top_excess_sharpe, top_excess_annual_return
                FROM aistock_factor_metrics
                WHERE eval_window = 'full'
                ORDER BY factor_name, calculated_at DESC
            """)
            rows = cur.fetchall()
    summary = {}
    for r in rows:
        summary[r[0]] = {"ic_mean": r[1], "sharpe": r[2], "annual_return": r[3]}
    return {"ok": True, "summary": summary, "total": len(summary)}


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
    except Exception as e:
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
    test_start_date: str = "2023-01-01"
    test_end_date: str = "2023-12-31"


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
            except Exception as e:
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
        
        exp_dir = exp_record.get("experiment_dir") or exp_record.get("workspace_path")
        
        # 生成反馈
        feedback_svc = QEFeedbackService()
        feedback = feedback_svc.generate_feedback(
            experiment_id=experiment_id,
            experiment_dir=exp_dir,
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
        
        exp_dir = exp_record.get("experiment_dir") or exp_record.get("workspace_path")
        
        # 构建上下文
        feedback_svc = QEFeedbackService()
        context = feedback_svc.build_next_loop_context(
            experiment_id=experiment_id,
            experiment_dir=exp_dir,
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



def _update_experiment_status(experiment_id: str, status: str):
    """安全更新实验状态。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE qe_experiments SET status = %s, completed_at = NOW() WHERE experiment_id = %s",
                    (status, experiment_id),
                )
            conn.commit()
    except Exception as e:
        logger.error(f"DB update failed for {experiment_id}: {e}")


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
    try:
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
    except Exception as e:
        logger.error(f"DB metrics update failed for {experiment_id}: {e}")


@router.post("/experiments/{experiment_id}/run")
async def run_experiment(experiment_id: str):
    """一键执行单次实验：读取配置 → compose_in_memory → 提交 RDAgent。
    
    实验状态由前端通过 get_experiment_run_status 按需查询并自动同步，
    不再使用后台轮询（避免阻塞 uvicorn reload）。
    """
    from ..services.quantevolver.config_composer import ConfigComposer
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    try:
        # 1) 从 DB 读取实验记录
        cc = ConfigComposer()
        exp_record = cc._get_experiment_record(experiment_id)
        if not exp_record:
            raise HTTPException(status_code=404, detail=f"实验 {experiment_id} 不存在")

        if exp_record.get("status") == "running":
            raise HTTPException(status_code=409, detail="实验正在执行中，请勿重复提交")

        # 2) 组装实验文件（内存）
        factor_names_raw = exp_record.get("factor_names") or []
        if isinstance(factor_names_raw, str):
            factor_names_raw = json.loads(factor_names_raw)

        data_split = exp_record.get("data_split")
        if isinstance(data_split, str):
            data_split = json.loads(data_split)

        custom_params = exp_record.get("custom_params")
        if isinstance(custom_params, str):
            custom_params = json.loads(custom_params)

        experiment_name = exp_record.get("experiment_name") or f"qe_exp_{experiment_id}"

        compose_res = cc.compose_experiment_in_memory(
            factor_names=factor_names_raw,
            model_id=exp_record.get("model_id"),
            strategy_id=exp_record.get("strategy_id"),
            data_split=data_split,
            custom_params=custom_params,
            experiment_name=experiment_name,
            skip_db_save=True,  # 已有 DB 记录，不重复写入
        )
        experiment_files = compose_res["experiment_files"]
        wsl_command = compose_res.get("wsl_command", "")

        # 3) task_id = experiment_name（已与 experiment_id 统一），修复原 f"{name}_{id}" 拼接导致的 404
        qe_task_id = experiment_name  # experiment_name = experiment_id（已统一）
        loop_index = 1
        qe_loop_id = f"Loop{loop_index}"
        config = {
            "factor_list": factor_names_raw,
            "model_id": exp_record.get("model_id"),
            "strategy_id": exp_record.get("strategy_id"),
            "data_split": data_split,
            "model_params": custom_params,
        }

        # 4) 提交到 RDAgent 执行
        async with QEWorkspaceClient() as client:
            qe_loop_id = await client.create_and_run_loop(
                qe_task_id, loop_index, config, experiment_files, wsl_command
            )

        # 5) 更新 DB：状态 + qe_task_id / qe_loop_id
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_experiments
                    SET status = 'running',
                        qe_task_id = %s,
                        qe_loop_id = %s,
                        started_at = NOW()
                    WHERE experiment_id = %s
                """, (qe_task_id, qe_loop_id, experiment_id))
            conn.commit()

        return {
            "ok": True,
            "experiment_id": experiment_id,
            "qe_task_id": qe_task_id,
            "qe_loop_id": qe_loop_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"执行实验失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/run-status")
async def get_experiment_run_status(experiment_id: str):
    """查询实验执行状态。如果有 qe_loop_id 则实时查询 RDAgent 侧。"""
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status, qe_task_id, qe_loop_id, result_metrics FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="实验不存在")
                cols = [desc[0] for desc in cur.description]
                record = dict(zip(cols, row))

        result = {
            "experiment_id": experiment_id,
            "status": record["status"],
            "qe_task_id": record.get("qe_task_id"),
            "qe_loop_id": record.get("qe_loop_id"),
            "result_metrics": record.get("result_metrics"),
        }

        # 如果有 loop_id 且状态为 running，实时查询并自动同步
        if record.get("qe_loop_id") and record["status"] == "running":
            async with QEWorkspaceClient() as client:
                live_status = await client.get_loop_status(record["qe_task_id"], record["qe_loop_id"])
                rd_status = live_status.get("status")
                result["live_status"] = rd_status

                # 自动同步：RDAgent 已完成/失败/中断但 DB 仍为 running
                if rd_status == "completed":
                    try:
                        metrics = await client.get_loop_metrics(record["qe_task_id"], record["qe_loop_id"])
                        _update_experiment_with_metrics(experiment_id, metrics)
                        result["status"] = "completed"
                        result["result_metrics"] = {k: v for k, v in metrics.items() if k != "_raw_json"}
                    except Exception as me:
                        logger.warning(f"Auto-sync metrics failed for {experiment_id}: {me}")
                        _update_experiment_status(experiment_id, "completed")
                        result["status"] = "completed"
                elif rd_status in ("failed", "error"):
                    _update_experiment_status(experiment_id, "failed")
                    result["status"] = "failed"
                elif rd_status in ("interrupted", "not_found"):
                    # interrupted: 进程已不存在; not_found: workspace 已丢失（如 /tmp 被清空）
                    _update_experiment_status(experiment_id, "interrupted")
                    result["status"] = "interrupted"

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"查询执行状态失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/logs")
async def stream_experiment_logs(experiment_id: str):
    """SSE 实时日志流，转发 RDAgent 侧的任务日志。"""
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT qe_task_id FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="实验不存在")
                qe_task_id = row[0]

        if not qe_task_id:
            raise HTTPException(status_code=400, detail="实验尚未执行，无日志可查")

        client = QEWorkspaceClient()

        async def event_generator():
            try:
                async for line in client.stream_task_logs(qe_task_id):
                    # RDAgent 返回 SSE 格式: data: {"status":"running","logs":["line1","line2",...]}
                    # 需要解析 JSON，逐行转发给前端
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
                        # 非 JSON 格式，按纯文本逐行发送
                        for sub in raw.split("\n"):
                            yield f"data: {sub}\n\n"
            except Exception as e:
                yield f"data: [ERROR] 日志流断开: {e}\n\n"
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
        logger.exception(f"获取日志流失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/enhanced-metrics")
async def get_experiment_enhanced_metrics(experiment_id: str):
    """获取实验的增强诊断指标（IC 时序、Loss 曲线、收益曲线等），代理到 RDAgent 侧。
    将嵌套的 ic_diagnostics/return_curves/training_diagnostics 展平为前端图表组件所需的顶层字段。
    """
    import httpx

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT qe_loop_id, qe_task_id FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="实验不存在")
                qe_loop_id = row[0]
                qe_task_id = row[1]

        if not qe_loop_id:
            raise HTTPException(status_code=400, detail="实验尚未执行，无增强指标")

        from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient
        base_url = QEWorkspaceClient().base_url
        url = f"{base_url}/tasks/{qe_task_id}/loops/{qe_loop_id}/enhanced-metrics"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        # 展平嵌套结构为前端图表组件期望的顶层字段
        flat: dict = {}
        if "ic_diagnostics" in data:
            ic = data["ic_diagnostics"]
            flat["dates"] = ic.get("ic_dates") or ic.get("dates", [])
            flat["ic_series"] = ic.get("ic_series")
            flat["rank_ic_series"] = ic.get("rank_ic_series")
            flat["ic_rolling_30d_mean"] = ic.get("ic_rolling_30d_mean")
            flat["ic_rolling_30d_std"] = ic.get("ic_rolling_30d_std")
            flat["ic_positive_ratio"] = ic.get("ic_positive_ratio")
        if "return_curves" in data:
            rc = data["return_curves"]
            if not flat.get("dates"):
                flat["dates"] = rc.get("dates", [])
            flat["cumulative_excess_no_cost"] = rc.get("cumulative_excess_no_cost")
            flat["cumulative_excess_with_cost"] = rc.get("cumulative_excess_with_cost")
            flat["cumulative_benchmark"] = rc.get("cumulative_benchmark")
            flat["drawdown_series"] = rc.get("drawdown_series")
        if "training_diagnostics" in data:
            td = data["training_diagnostics"]
            flat["train_loss_curve"] = td.get("train_loss_curve")
            flat["val_loss_curve"] = td.get("val_loss_curve")
            flat["best_epoch"] = td.get("best_epoch")
            flat["overfit_ratio"] = td.get("overfit_ratio")
            flat["convergence_ratio"] = td.get("convergence_ratio")
        if "summary" in data:
            flat["summary"] = data["summary"]
        return flat
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status == 404:
            raise HTTPException(status_code=404, detail="增强指标文件尚未生成")
        raise HTTPException(status_code=status, detail=str(e))
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"RDAgent 服务不可达: {e}")
    except Exception as e:
        logger.exception(f"获取增强指标失败: {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/experiments/{experiment_id}")
async def delete_experiment(
    experiment_id: str,
    cleanup_workspace: bool = True,
):
    """删除QE实验及其所有关联数据"""
    from ..services.quantevolver.qe_workspace_client import QEWorkspaceClient

    # 1. 检查实验存在且非运行中
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM qe_experiments WHERE experiment_id = %s",
                (experiment_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="实验不存在")
            if row[0] == "running":
                raise HTTPException(status_code=409, detail="实验正在运行中，请先停止")

    errors = []

    # 2. 清理WSL侧workspace
    if cleanup_workspace:
        try:
            async with QEWorkspaceClient() as client:
                await client.cleanup_task_workspace(experiment_id)
        except Exception as e:
            errors.append(f"workspace清理失败: {e}")
            logger.warning(f"Workspace cleanup failed for {experiment_id}: {e}")

    # 3. 清理DB记录（事务内，按外键依赖顺序删除）
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 删除演进循环记录（引用 qe_evolution_tasks + qe_experiments）
            cur.execute(
                "DELETE FROM qe_evolution_loops WHERE experiment_id = %s OR experiment_id IN (SELECT experiment_id FROM qe_experiments WHERE parent_experiment_id = %s)",
                (experiment_id, experiment_id),
            )
            cur.execute(
                "DELETE FROM qe_evolution_loops WHERE task_id IN (SELECT task_id FROM qe_evolution_tasks WHERE base_experiment_id = %s)",
                (experiment_id,),
            )
            # 删除演进任务记录（base_experiment_id 引用 qe_experiments）
            cur.execute(
                "DELETE FROM qe_evolution_tasks WHERE base_experiment_id = %s",
                (experiment_id,),
            )
            cur.execute(
                "DELETE FROM qe_evolution_tasks WHERE task_id = %s",
                (experiment_id,),
            )
            # 删除因子实验指标（主实验 + 子Loop）
            cur.execute(
                "DELETE FROM qe_factor_experiment_metrics WHERE experiment_id = %s OR experiment_id IN (SELECT experiment_id FROM qe_experiments WHERE parent_experiment_id = %s)",
                (experiment_id, experiment_id),
            )
            # 删除所有子Loop实验记录
            cur.execute(
                "DELETE FROM qe_experiments WHERE parent_experiment_id = %s",
                (experiment_id,),
            )
            # 删除主实验记录
            cur.execute(
                "DELETE FROM qe_experiments WHERE experiment_id = %s",
                (experiment_id,),
            )
        conn.commit()

    return {
        "ok": True,
        "experiment_id": experiment_id,
        "warnings": errors if errors else None,
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
    import subprocess, sys
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
        return {"ok": True, "date": target_date, "wsl_path": wsl_path, "stock_count": stock_count}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("生成股票池文件失败")
        raise HTTPException(status_code=500, detail=str(e))
