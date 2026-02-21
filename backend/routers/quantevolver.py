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
- GET  /api/v1/quantevolver/prompts                        获取提示词列表
- GET  /api/v1/quantevolver/prompts/{agent_type}/{key}     获取指定提示词
- PUT  /api/v1/quantevolver/prompts/{agent_type}/{key}     更新提示词
- POST /api/v1/quantevolver/prompts                        创建提示词
- DELETE /api/v1/quantevolver/prompts/{id}                 删除提示词
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
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


class GenerateConfigRequest(BaseModel):
    factor_names: List[str]
    factor_sources: Optional[Dict[str, str]] = None
    model_id: Optional[str] = None
    strategy_id: Optional[str] = None
    data_split: Optional[Dict[str, str]] = None
    custom_params: Optional[Dict[str, Any]] = None
    experiment_name: Optional[str] = None


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
async def sync_alpha_factors(req: SyncAlphaRequest):
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
async def sync_model_task(task_id: str, req: SyncModelTaskRequest = None):
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
async def list_factors(
    source: Optional[str] = Query(None, description="过滤source"),
    exclude_source: Optional[str] = Query(None, description="排除的source，逗号分隔"),
    search: Optional[str] = Query(None, description="搜索因子名称"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取全部因子列表。"""
    try:
        from ..db.pg_pool import get_conn

        conditions = []
        params = []

        if source:
            conditions.append("source = %s")
            params.append(source)
        if exclude_source:
            ex_list = [s.strip() for s in exclude_source.split(",") if s.strip()]
            if ex_list:
                placeholders = ",".join(["%s"] * len(ex_list))
                conditions.append(f"source NOT IN ({placeholders})")
                params.extend(ex_list)
        if search:
            conditions.append("factor_name ILIKE %s")
            params.append(f"%{search}%")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM aistock_factor_catalog WHERE {where_clause}",
                    params,
                )
                total = cur.fetchone()[0]

                cur.execute(f"""
                    SELECT factor_name, source, expression, ic, sharpe,
                           annualized_return, is_sota_factor, catalog_source,
                           description_cn, generated_at_utc
                    FROM aistock_factor_catalog
                    WHERE {where_clause}
                    ORDER BY is_sota_factor DESC NULLS LAST, ic DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, params + [limit, offset])
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        return {"ok": True, "total": total, "items": rows}
    except Exception as e:
        logger.exception("获取因子列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models")
async def list_models(
    search: Optional[str] = Query(None),
    sota_only: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """获取全部模型列表。"""
    try:
        from ..db.pg_pool import get_conn

        conditions = []
        params = []

        if search:
            conditions.append("(model_name ILIKE %s OR model_type ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        if sota_only:
            conditions.append("is_sota = TRUE")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM aistock_model_catalog WHERE {where_clause}",
                    params,
                )
                total = cur.fetchone()[0]

                cur.execute(f"""
                    SELECT model_id, model_name, model_type, display_name,
                           ic, annualized_return, max_drawdown, information_ratio,
                           is_sota, task_run_id, loop_id,
                           hypothesis_text, model_architecture,
                           description, generated_at_utc
                    FROM aistock_model_catalog
                    WHERE {where_clause}
                    ORDER BY is_sota DESC NULLS LAST, ic DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, params + [limit, offset])
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        return {"ok": True, "total": total, "items": rows}
    except Exception as e:
        logger.exception("获取模型列表失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies")
async def list_strategies(
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
async def get_strategy_detail(strategy_id: str):
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
async def create_strategy(req: CreateStrategyRequest):
    """新建策略。"""
    try:
        import json as _json
        from ..db.pg_pool import get_conn
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
                        source_code, default_kwargs, param_schema,
                        parent_strategy_id, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        'custom', %s, %s,
                        %s, %s, %s,
                        %s, NOW(), NOW()
                    )
                """, (
                    req.strategy_id, req.display_name, req.description, req.strategy_type,
                    req.market, req.freq,
                    req.source_code,
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
async def update_strategy(strategy_id: str, req: UpdateStrategyRequest):
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
            set_parts.append("source_code = %s")
            params.append(req.source_code)
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
async def delete_strategy(strategy_id: str):
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
async def clone_strategy(strategy_id: str, req: CreateStrategyRequest):
    """从现有策略模板创建新策略。"""
    try:
        import json as _json
        from ..db.pg_pool import get_conn
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
                        source_code, default_kwargs, param_schema,
                        parent_strategy_id, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        'custom', %s, %s,
                        %s, %s, %s,
                        %s, NOW(), NOW()
                    )
                """, (
                    req.strategy_id, req.display_name, req.description, req.strategy_type,
                    req.market, req.freq,
                    req.source_code,
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
        result = analyzer.analyze(strategy_id=req.strategy_id, source_code=code)

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
        result = fa.analyze_single_factor(
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
    """批量分析因子。支持指定因子名称列表或按来源筛选。"""
    try:
        from ..services.quantevolver.factor_analyst import FactorAnalyst
        fa = FactorAnalyst()
        result = fa.batch_analyze_all_factors(
            use_llm=req.use_llm,
            source_filter=req.source_filter,
            factor_names=req.factor_names,
        )
        return result
    except Exception as e:
        logger.exception("批量因子分析失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-analyst/classifications")
async def get_classifications(
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
        result = fa.recommend_factor_combination(
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
async def evaluate_combination(req: EvaluateCombinationRequest):
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
async def recommend_combinations(req: RecommendCombinationsRequest):
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


@router.post("/portfolio/generate")
async def generate_from_requirement(req: GenerateFromRequirementRequest):
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
async def generate_config(req: GenerateConfigRequest):
    """生成QLib配置文件。"""
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
        return result
    except Exception as e:
        logger.exception("配置生成失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments")
async def list_experiments(
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
async def get_experiment_detail(experiment_id: str):
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
    """同步实验结果。"""
    try:
        from ..services.quantevolver.config_composer import ConfigComposer
        cc = ConfigComposer()
        result = cc.sync_experiment_results(experiment_id)
        return result
    except Exception as e:
        logger.exception("同步实验结果失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiments/{experiment_id}/regenerate")
async def regenerate_experiment(experiment_id: str):
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
async def trigger_experiment_selection(experiment_id: str, req: ExperimentSelectionRequest):
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
# Phase 2 API: 因子实验表现查询 & 实验交易统计
# ============================================================

@router.get("/factors/{factor_name}/experiment-metrics")
async def get_factor_experiment_metrics(
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
async def get_experiment_trade_stats(experiment_id: str):
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


@router.post("/model-analyst/analyze")
async def analyze_model(req: AnalyzeModelRequest):
    """分析单个模型，生成描述。"""
    try:
        from ..services.quantevolver.model_analyst import ModelAnalyst
        ma = ModelAnalyst()
        result = ma.analyze_single_model(
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
    """批量分析所有模型。"""
    try:
        from ..services.quantevolver.model_analyst import ModelAnalyst
        ma = ModelAnalyst()
        result = ma.batch_analyze_all_models(use_llm=req.use_llm)
        return {"ok": True, **result}
    except Exception as e:
        logger.exception(f"批量分析模型失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
async def list_prompts(
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
async def get_prompt(agent_type: str, prompt_key: str):
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
async def update_prompt(agent_type: str, prompt_key: str, req: UpdatePromptRequest):
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
async def create_prompt(req: CreatePromptRequest):
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
async def delete_prompt(prompt_id: int):
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
async def get_available_llm_models():
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
async def save_agent_model_config(req: SaveAgentModelRequest):
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
async def get_agent_model_configs():
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
async def save_agent_prompt(req: AgentPromptRequest):
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
async def transform_factor(req: FactorTransformRequest, background_tasks: BackgroundTasks):
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
async def transform_factor_sync(req: FactorTransformRequest):
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
async def batch_transform_factors(req: BatchTransformRequest, background_tasks: BackgroundTasks):
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
async def get_factor_transformation_status(
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
async def list_transformation_jobs(
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
async def get_transformation_job(job_id: str):
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
async def get_transformation_job_progress(job_id: str):
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
async def get_factor_realtime_code(factor_name: str, source: str = "rdagent_task_sync"):
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
async def reset_factor_transformation(factor_name: str, source: str = "rdagent_sota"):
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
async def get_transformation_stats():
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
async def get_factor_sources():
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
async def analyze_experiment_results(experiment_id: str, req: Optional[ExperimentAnalysisRequest] = None):
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
async def get_evolution_context(experiment_id: str):
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
