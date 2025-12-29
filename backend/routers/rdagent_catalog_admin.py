from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Literal, Optional

from fastapi import APIRouter, Body, Query

from ..db.pg_pool import get_conn
from ..services.rdagent_catalog_etl_service import (
    ImportSummary,
    import_all_catalogs_from_root,
    import_factor_catalog_from_json,
    import_loop_catalog_from_json,
    import_strategy_catalog_from_json,
)


router = APIRouter(prefix="/api/v1/rdagent/catalogs", tags=["rdagent-catalogs"])


@router.post("/import", summary="导入 RD-Agent Phase2 Catalog JSON 到本地 PG 表")
async def import_catalogs(
    kind: Literal["factor", "strategy", "loop", "all"] = Body(..., embed=True),
    root_dir: Optional[str] = Body(None, embed=True),
) -> Dict[str, Any]:
    """从 RD-Agent 导出的 JSON 文件导入 Catalog 数据到本地 PG.

    - kind: 指定导入的类型 (factor/strategy/loop/all)
    - root_dir: 可选 JSON 根目录, 不传则使用 RDAGENT_AISTOCK_JSON_ROOT 或默认路径
    """

    results: Dict[str, ImportSummary] = {}

    # 单类导入时, 若指定了 root_dir, 则显式拼接 JSON 路径; 否则交由 service 使用默认根目录
    if kind == "factor":
        json_path = None
        if root_dir is not None:
            json_path = str(Path(root_dir) / "factor_catalog.json")
        results["factor"] = import_factor_catalog_from_json(json_path)
    elif kind == "strategy":
        json_path = None
        if root_dir is not None:
            json_path = str(Path(root_dir) / "strategy_catalog.json")
        results["strategy"] = import_strategy_catalog_from_json(json_path)
    elif kind == "loop":
        json_path = None
        if root_dir is not None:
            json_path = str(Path(root_dir) / "loop_catalog.json")
        results["loop"] = import_loop_catalog_from_json(json_path)
    else:
        results = import_all_catalogs_from_root(root_dir=root_dir)

    return {k: vars(v) for k, v in results.items()}


@router.get("/factors", summary="查询因子 Catalog 列表")
async def list_factors(
    source: Optional[str] = Query(None, description="因子来源过滤, 例如 qlib_alpha158/rdagent"),
    region: Optional[str] = Query(None, description="区域过滤, 例如 cn"),
    tag: Optional[str] = Query(None, description="按单个标签过滤, 如 alpha158"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """从 aistock_factor_catalog 查询因子列表, 支持按 source/region/tag 过滤."""

    conds = []
    params: list[Any] = []

    if source:
        conds.append("source = %s")
        params.append(source)
    if region:
        conds.append("region = %s")
        params.append(region)
    if tag:
        conds.append("tags ? %s")
        params.append(tag)

    where_sql = ""
    if conds:
        where_sql = " WHERE " + " AND ".join(conds)

    sql = f"""
        SELECT factor_name, expression, source, region, tags
        FROM aistock_factor_catalog
        {where_sql}
        ORDER BY factor_name
        LIMIT %s OFFSET %s
    """

    params_with_page = params + [limit, offset]

    items: list[dict[str, Any]] = []
    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            # total
            cur.execute(f"SELECT COUNT(*) FROM aistock_factor_catalog{where_sql}", params)
            total = int(cur.fetchone()[0])

            # page data
            cur.execute(sql, params_with_page)
            rows = cur.fetchall()

    for name, expr, src, reg, tags in rows:
        items.append(
            {
                "factor_name": name,
                "expression": expr,
                "source": src,
                "region": reg,
                "tags": tags,
            }
        )

    return {"total": total, "items": items}


@router.get("/strategy-loop-best", summary="按 strategy_id 汇总代表性 loop 指标摘要")
async def get_strategy_best_loop_summaries() -> Dict[str, Any]:
    """为每个 strategy_id 选出一条代表性 loop 并返回其指标与摘要.

    选取规则（简化版）：
    - 仅考虑 strategy_id 非空的记录;
    - 优先 status='success' 且 has_result=true 的记录;
    - 在此基础上按 generated_at_utc 降序、task_run_id、loop_id 选择最新的一条;

    为了减少复杂度，这里不在 DB 端做 metrics 排序，只提供原始 JSON，由前端按需展示关键字段
    （如 ann_ret/annual_return/mdd/max_drawdown/ic_mean 等）。
    """

    sql = """
        WITH ranked AS (
            SELECT
                strategy_id,
                task_run_id,
                loop_id,
                status,
                has_result,
                metrics,
                decision,
                summary_execution,
                summary_value_feedback,
                summary_shape_feedback,
                ROW_NUMBER() OVER (
                    PARTITION BY strategy_id
                    ORDER BY
                        (status = 'success') DESC,
                        (has_result IS TRUE) DESC,
                        generated_at_utc DESC,
                        task_run_id DESC,
                        loop_id DESC
                ) AS rn
            FROM aistock_loop_catalog
            WHERE strategy_id IS NOT NULL
        )
        SELECT
            strategy_id,
            task_run_id,
            loop_id,
            status,
            has_result,
            metrics,
            decision,
            summary_execution,
            summary_value_feedback,
            summary_shape_feedback
        FROM ranked
        WHERE rn = 1
    """

    items: list[dict[str, Any]] = []
    
    # 为每个 strategy_id 自动尝试创建索引以优化性能（后台异步，小数据量下几乎瞬时）
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'aistock_loop_catalog' AND indexname = 'idx_loop_catalog_strategy_id'
                """)
                if not cur.fetchone():
                    cur.execute("CREATE INDEX idx_loop_catalog_strategy_id ON aistock_loop_catalog(strategy_id);")
                
                cur.execute("""
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'aistock_loop_catalog' AND indexname = 'idx_loop_catalog_composite'
                """)
                if not cur.fetchone():
                    cur.execute("CREATE INDEX idx_loop_catalog_composite ON aistock_loop_catalog(strategy_id, status, has_result, generated_at_utc DESC);")
    except Exception as e:
        print(f"DEBUG: Index check/creation failed: {e}")

    start_time = time.time()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 设置语句超时，防止慢查询挂起
                cur.execute("SET statement_timeout = 5000;") 
                cur.execute(sql)
                rows = cur.fetchall()
    finally:
        duration = time.time() - start_time
        print(f"DEBUG: get_strategy_best_loop_summaries took {duration:.4f}s")

    for (
        strategy_id,
        task_run_id,
        loop_id,
        status,
        has_result,
        metrics,
        decision,
        summary_execution,
        summary_value_feedback,
        summary_shape_feedback,
    ) in rows:
        items.append(
            {
                "strategy_id": strategy_id,
                "task_run_id": task_run_id,
                "loop_id": loop_id,
                "status": status,
                "has_result": has_result,
                "metrics": metrics,
                "decision": decision,
                "summary_execution": summary_execution,
                "summary_value_feedback": summary_value_feedback,
                "summary_shape_feedback": summary_shape_feedback,
            }
        )

    return {"items": items}


@router.get("/strategies", summary="查询策略 Catalog 列表")
async def list_strategies(
    step_name: Optional[str] = Query(None, description="按 step_name 过滤, 如 running/feedback"),
    action: Optional[str] = Query(None, description="按 action 过滤, 如 factor/model"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """从 aistock_strategy_catalog 查询策略模板列表."""

    conds = []
    params: list[Any] = []

    if step_name:
        conds.append("step_name = %s")
        params.append(step_name)
    if action:
        conds.append("action = %s")
        params.append(action)

    where_sql = ""
    if conds:
        where_sql = " WHERE " + " AND ".join(conds)

    sql = f"""
        SELECT strategy_id, scenario, step_name, action,
               example_task_run_id, example_loop_id, example_workspace_id, example_workspace_path
        FROM aistock_strategy_catalog
        {where_sql}
        ORDER BY strategy_id
        LIMIT %s OFFSET %s
    """

    params_with_page = params + [limit, offset]

    items: list[dict[str, Any]] = []
    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM aistock_strategy_catalog{where_sql}", params)
            total = int(cur.fetchone()[0])

            cur.execute(sql, params_with_page)
            rows = cur.fetchall()

    for (
        strategy_id,
        scenario,
        step,
        act,
        task_run_id,
        loop_id,
        workspace_id,
        workspace_path,
    ) in rows:
        items.append(
            {
                "strategy_id": strategy_id,
                "scenario": scenario,
                "step_name": step,
                "action": act,
                "example_task_run_id": task_run_id,
                "example_loop_id": loop_id,
                "example_workspace_id": workspace_id,
                "example_workspace_path": workspace_path,
            }
        )

    return {"total": total, "items": items}


@router.get("/loops", summary="查询 loop Catalog 列表")
async def list_loops(
    strategy_id: Optional[str] = Query(None, description="按 strategy_id 过滤"),
    status: Optional[str] = Query(None, description="按 loop 状态过滤, 如 success/failed"),
    step_name: Optional[str] = Query(None, description="按 step_name 过滤"),
    action: Optional[str] = Query(None, description="按 action 过滤"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """从 aistock_loop_catalog 查询 loop 记录列表."""

    conds = []
    params: list[Any] = []

    if strategy_id:
        conds.append("strategy_id = %s")
        params.append(strategy_id)
    if status:
        conds.append("status = %s")
        params.append(status)
    if step_name:
        conds.append("step_name = %s")
        params.append(step_name)
    if action:
        conds.append("action = %s")
        params.append(action)

    where_sql = ""
    if conds:
        where_sql = " WHERE " + " AND ".join(conds)

    sql = f"""
        SELECT task_run_id, loop_id, strategy_id, status, metrics,
               decision, summary_execution, summary_value_feedback, summary_shape_feedback,
               path_factor_meta, path_factor_perf, path_feedback, path_ret_curve, path_dd_curve
        FROM aistock_loop_catalog
        {where_sql}
        ORDER BY task_run_id, loop_id
        LIMIT %s OFFSET %s
    """

    params_with_page = params + [limit, offset]

    items: list[dict[str, Any]] = []
    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM aistock_loop_catalog{where_sql}", params)
            total = int(cur.fetchone()[0])

            cur.execute(sql, params_with_page)
            rows = cur.fetchall()

    for (
        task_run_id,
        loop_id,
        sid,
        st,
        metrics,
        decision,
        summary_execution,
        summary_value_feedback,
        summary_shape_feedback,
        p_factor_meta,
        p_factor_perf,
        p_feedback,
        p_ret_curve,
        p_dd_curve,
    ) in rows:
        items.append(
            {
                "task_run_id": task_run_id,
                "loop_id": loop_id,
                "strategy_id": sid,
                "status": st,
                "metrics": metrics,
                "decision": decision,
                "summary_execution": summary_execution,
                "summary_value_feedback": summary_value_feedback,
                "summary_shape_feedback": summary_shape_feedback,
                "paths": {
                    "factor_meta": p_factor_meta,
                    "factor_perf": p_factor_perf,
                    "feedback": p_feedback,
                    "ret_curve": p_ret_curve,
                    "dd_curve": p_dd_curve,
                },
            }
        )

    return {"total": total, "items": items}
