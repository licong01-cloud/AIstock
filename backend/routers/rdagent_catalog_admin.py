from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import FileResponse

from ..db.pg_pool import get_conn
from ..services.rdagent_asset_service import rdagent_asset_service
from ..services.rdagent_results_api_client import RDAgentResultsApiClient


router = APIRouter(prefix="/rdagent/catalogs", tags=["rdagent-catalogs"])


@router.post("/asset-bundles/{asset_bundle_id}/download", summary="下载并解压指定资产包到本地")
def download_asset_bundle(asset_bundle_id: str) -> dict[str, Any]:
    bundle_id = str(asset_bundle_id).strip()
    if not bundle_id:
        raise HTTPException(status_code=422, detail="asset_bundle_id 不能为空")

    existed = rdagent_asset_service.is_bundle_available(bundle_id)
    ok = rdagent_asset_service.download_and_extract_bundle(bundle_id)
    local_path = str(rdagent_asset_service.get_bundle_path(bundle_id))
    if not ok:
        raise HTTPException(
            status_code=422,
            detail=(
                f"资产包下载/解压失败: {bundle_id}。"
                "请确认 RD-Agent Results API 已启动，并检查环境变量 RDAGENT_RESULTS_API_BASE_URL（默认 http://127.0.0.1:9000）。"
            ),
        )

    return {"ok": True, "asset_bundle_id": bundle_id, "existed": existed, "local_path": local_path}


@router.post("/strategies/{strategy_id}/selection", summary="切换策略是否加入选股中心")
def toggle_strategy_selection(
    strategy_id: str,
    in_selection: bool = Body(..., embed=True)
) -> Dict[str, Any]:
    """REQ-UI-P3-020: 支持策略从 Catalog 动态添加至选股中心。"""
    sql = "UPDATE aistock_strategy_catalog SET in_selection_center = %s WHERE strategy_id = %s"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (in_selection, strategy_id))
        return {"ok": True, "strategy_id": strategy_id, "in_selection_center": in_selection}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/selection-center", summary="获取选股中心策略列表")
def list_selection_center_strategies() -> Dict[str, Any]:
    """获取所有勾选了 'in_selection_center' 的策略及其最新表现。"""
    sql = """
        SELECT sc.strategy_id, sc.example_task_run_id, sc.example_loop_id, 
               lc.annualized_return, lc.max_drawdown, lc.sharpe, lc.ic,
               sc.python_implementation, sc.freq
        FROM aistock_strategy_catalog sc
        LEFT JOIN aistock_loop_catalog lc ON (lc.task_run_id = sc.example_task_run_id AND lc.loop_id = sc.example_loop_id)
        WHERE sc.in_selection_center = TRUE
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        
        items = []
        for r in rows:
            items.append({
                "strategy_id": r[0],
                "task_run_id": r[1],
                "loop_id": r[2],
                "ann_ret": r[3],
                "mdd": r[4],
                "sharpe": r[5],
                "ic": r[6],
                "has_impl": bool(r[7]),
                "freq": r[8]
            })
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors/source-tasks", summary="获取所有已同步的来源 Task 列表")
def list_factor_source_tasks(
    node_id: Optional[str] = Query(None, description="按节点过滤"),
) -> Dict[str, Any]:
    """返回 source_task_id 分组，含因子数和 ok/skipped/error 统计。"""
    extra_where = ""
    params: list[Any] = []
    if node_id:
        extra_where = " AND c.node_id = %s"
        params.append(node_id)
    sql = f"""
        SELECT c.source_task_id,
               COUNT(DISTINCT c.factor_name) AS factor_count,
               COUNT(DISTINCT CASE WHEN l.status = 'ok' THEN l.factor_name || '|' || l.eval_window END) AS ok_count,
               COUNT(DISTINCT CASE WHEN l.status = 'skipped' THEN l.factor_name || '|' || l.eval_window END) AS skipped_count,
               COUNT(DISTINCT CASE WHEN l.status = 'error' THEN l.factor_name || '|' || l.eval_window END) AS error_count
        FROM aistock_factor_catalog c
        LEFT JOIN aistock_factor_calc_log l
          ON l.source_task_id = c.source_task_id
        WHERE c.source_task_id IS NOT NULL{extra_where}
        GROUP BY c.source_task_id
        ORDER BY c.source_task_id DESC
    """
    items: list[dict[str, Any]] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for tid, fc, ok_c, skip_c, err_c in cur.fetchall():
                items.append({
                    "task_id": tid,
                    "factor_count": fc,
                    "ok_count": ok_c,
                    "skipped_count": skip_c,
                    "error_count": err_c,
                })
    return {"items": items}


@router.get("/factors/source-tasks/{task_id}/calc-detail", summary="获取Task下因子计算详情")
def get_task_calc_detail(task_id: str) -> Dict[str, Any]:
    """返回该 Task 下所有因子在所有窗口的计算状态，按 factor_name 分组。"""
    sql = """
        SELECT factor_name, eval_window, status, error_message,
               n_trading_days, required_days, data_start, data_end, calculated_at
        FROM aistock_factor_calc_log
        WHERE source_task_id = %s
        ORDER BY factor_name, eval_window
    """
    rows: list[tuple] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [task_id])
            rows = cur.fetchall()

    if not rows:
        return {"factors": [], "summary": {"ok_count": 0, "skipped_count": 0, "error_count": 0}}

    from collections import OrderedDict
    grouped: OrderedDict[str, list] = OrderedDict()
    ok_count = skip_count = err_count = 0
    for fname, ew, st, err_msg, n_days, req_days, d_start, d_end, calc_at in rows:
        if fname not in grouped:
            grouped[fname] = []
        grouped[fname].append({
            "eval_window": ew,
            "status": st,
            "error_message": err_msg,
            "n_trading_days": n_days,
            "required_days": req_days,
            "data_start": str(d_start) if d_start else None,
            "data_end": str(d_end) if d_end else None,
            "calculated_at": str(calc_at) if calc_at else None,
        })
        if st == "ok":
            ok_count += 1
        elif st == "skipped":
            skip_count += 1
        elif st == "error":
            err_count += 1

    factors = [{"factor_name": fn, "windows": wins} for fn, wins in grouped.items()]
    return {
        "factors": factors,
        "summary": {"ok_count": ok_count, "skipped_count": skip_count, "error_count": err_count},
    }


@router.get("/factors", summary="查询因子 Catalog 列表")
def list_factors(
    source: Optional[str] = Query(None, description="因子来源过滤, 例如 qlib_alpha158/rdagent"),
    exclude_source: Optional[str] = Query(None, description="排除的source，逗号分隔，如 alpha158,alpha360"),
    source_task_id: Optional[str] = Query(None, description="按来源 Task ID 过滤"),
    region: Optional[str] = Query(None, description="区域过滤, 例如 cn"),
    tag: Optional[str] = Query(None, description="按单个标签过滤, 如 alpha158"),
    factor_type: Optional[str] = Query(None, description="因子类型过滤: CrossSection(截面) / TimeSeries(时序)"),
    data_source: Optional[str] = Query(None, description="数据来源过滤: daily_pv/daily_basic/moneyflow/cyq_perf/bak_basic/multi"),
    node_id: Optional[str] = Query(None, description="按来源节点过滤"),
    sort_by: Optional[str] = Query(None, description="排序字段: name/ic/sharpe/ann_ret"),
    sort_order: Optional[str] = Query(None, description="排序方向: asc/desc"),
    limit: int = Query(50, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """从 aistock_factor_catalog 查询因子列表, 支持按 source/region/tag/source_task_id/factor_type/data_source 过滤."""

    conds = []
    params: list[Any] = []

    if source:
        conds.append("source = %s")
        params.append(source)
    if exclude_source:
        ex_list = [s.strip() for s in exclude_source.split(",") if s.strip()]
        if ex_list:
            placeholders = ",".join(["%s"] * len(ex_list))
            conds.append(f"source NOT IN ({placeholders})")
            params.extend(ex_list)
    if source_task_id:
        conds.append("source_task_id = %s")
        params.append(source_task_id)
    if region:
        conds.append("region = %s")
        params.append(region)
    if tag:
        conds.append("tags ? %s")
        params.append(tag)
    if factor_type:
        conds.append("factor_type = %s")
        params.append(factor_type)
    if data_source:
        conds.append("data_source = %s")
        params.append(data_source)
    if node_id:
        conds.append("node_id = %s")
        params.append(node_id)

    where_sql = ""
    if conds:
        where_sql = " WHERE " + " AND ".join(conds)

    # 排序逻辑
    order_by = "factor_name"  # 默认按名称排序
    if sort_by:
        valid_sort_fields = {
            "name": "factor_name",
            "ic": "ic",
            "sharpe": "sharpe",
            "ann_ret": "annualized_return",
            "mdd": "max_drawdown",
        }
        if sort_by in valid_sort_fields:
            order_by = valid_sort_fields[sort_by]
            if sort_order == "desc":
                order_by += " DESC"
            elif sort_order == "asc":
                order_by += " ASC"

    sql = """
        SELECT factor_name, expression, source, region, tags,
               description_cn, formula_hint, variables, freq, align, nan_policy,
               created_at_utc, experiment_id, impl_module, impl_func, impl_version,
               performance_metrics, first_sota_task_id, is_sota_factor,
               interface_info, best_performance,
               best_performance_sharpe, best_performance_ann_ret, asset_bundle_id,
               source_task_id, source_code_relpath, source_code_origin, source_loop_tag, source_index,
               raw_payload,
               LEFT(code_text, 500) AS code_text_preview,
               ic, annualized_return, max_drawdown, sharpe,
               factor_type, data_source, asset_path, node_id
        FROM aistock_factor_catalog
        {where_sql}
        ORDER BY {order_by}
        LIMIT %s OFFSET %s
    """.format(where_sql=where_sql, order_by=order_by)

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

    for (
        name, expr, src, reg, tags,
        desc, formula, vars_json, freq, align, nan_pol,
        created, exp_id, impl_mod, impl_func, impl_v,
        perf, first_sota_task_id, is_sota_factor, info, best_perf, best_sharpe, best_ann, bundle_id,
        source_task_id, source_code_relpath, source_code_origin, source_loop_tag, source_index,
        raw_payload, code_text_preview,
        ic_val, ann_ret_val, max_dd_val, sharpe_val,
        factor_type_val, data_source_val, asset_path_val, node_id_val
    ) in rows:
        items.append(
            {
                "name": name,
                "expression": expr,
                "source": src,
                "region": reg,
                "tags": tags,
                "description_cn": desc,
                "formula_hint": formula,
                "variables": vars_json,
                "freq": freq,
                "align": align,
                "nan_policy": nan_pol,
                "created_at_utc": created,
                "experiment_id": exp_id,
                "impl_module": impl_mod,
                "impl_func": impl_func,
                "impl_version": impl_v,
                "performance_metrics": perf,
                "first_sota_task_id": first_sota_task_id,
                "is_sota_factor": is_sota_factor,
                "interface_info": info,
                "best_performance": best_perf,
                "best_performance_sharpe": best_sharpe,
                "best_performance_ann_ret": best_ann,
                "asset_bundle_id": bundle_id,
                "source_task_id": source_task_id,
                "source_code_relpath": source_code_relpath,
                "source_code_origin": source_code_origin,
                "source_loop_tag": source_loop_tag,
                "source_index": source_index,
                "raw_payload": raw_payload,
                "code_text_preview": code_text_preview,
                "ic": ic_val,
                "annualized_return": ann_ret_val,
                "max_drawdown": max_dd_val,
                "sharpe": sharpe_val,
                "factor_type": factor_type_val,
                "data_source": data_source_val,
                "asset_path": asset_path_val,
                "node_id": node_id_val,
            }
        )

    return {"total": total, "items": items}


@router.get("/factors/{factor_name}", summary="获取单因子完整元数据")
def get_factor_detail(
    factor_name: str,
    source: Optional[str] = Query(None, description="可选来源过滤"),
) -> Dict[str, Any]:
    """按名称返回单个因子的完整元数据 (REQ-API-P2-001)."""
    conds = ["c.factor_name = %s"]
    params = [factor_name]
    if source:
        conds.append("c.source = %s")
        params.append(source)

    where_sql = " WHERE " + " AND ".join(conds)
    sql = f"""
        SELECT c.factor_name, c.expression, c.source, c.region, c.tags,
               c.description_cn, c.formula_hint, c.variables, c.freq, c.align, c.nan_policy,
               c.created_at_utc, c.experiment_id, c.impl_module, c.impl_func, c.impl_version,
               c.performance_metrics, c.first_sota_task_id, c.is_sota_factor, c.interface_info,
               c.best_performance, c.best_performance_sharpe, c.best_performance_ann_ret,
               c.asset_bundle_id,
               c.source_task_id, c.source_code_relpath, c.source_code_origin,
               c.source_loop_tag, c.source_index,
               c.raw_payload, c.code_text,
               c.factor_type, c.data_source,
               cl.category AS cl_category, cl.grade AS cl_grade,
               cl.llm_analysis AS cl_llm_analysis, cl.description AS cl_description,
               cl.factor_dimension AS cl_factor_dimension,
               cl.factor_profile AS cl_factor_profile,
               cl.classification_reason AS cl_classification_reason,
               cl.grade_reason AS cl_grade_reason
        FROM aistock_factor_catalog c
        LEFT JOIN qe_factor_classification cl
            ON cl.factor_name = c.factor_name AND cl.factor_source = c.source
        {where_sql}
        LIMIT 1
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            col_names = [desc[0] for desc in cur.description]
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"因子 {factor_name} 不存在")

    data = dict(zip(col_names, row))

    # Flatten classification fields to top level with "classification_" prefix
    result = {
        "name": data["factor_name"],
        "expression": data["expression"],
        "source": data["source"],
        "region": data["region"],
        "tags": data["tags"],
        "description_cn": data["description_cn"],
        "formula_hint": data["formula_hint"],
        "variables": data["variables"],
        "freq": data["freq"],
        "align": data["align"],
        "nan_policy": data["nan_policy"],
        "created_at_utc": data["created_at_utc"],
        "experiment_id": data["experiment_id"],
        "impl_module": data["impl_module"],
        "impl_func": data["impl_func"],
        "impl_version": data["impl_version"],
        "performance_metrics": data["performance_metrics"],
        "first_sota_task_id": data["first_sota_task_id"],
        "is_sota_factor": data["is_sota_factor"],
        "interface_info": data["interface_info"],
        "best_performance": data["best_performance"],
        "best_performance_sharpe": data["best_performance_sharpe"],
        "best_performance_ann_ret": data["best_performance_ann_ret"],
        "asset_bundle_id": data["asset_bundle_id"],
        "source_task_id": data["source_task_id"],
        "source_code_relpath": data["source_code_relpath"],
        "source_code_origin": data["source_code_origin"],
        "source_loop_tag": data["source_loop_tag"],
        "source_index": data["source_index"],
        "raw_payload": data["raw_payload"],
        "code_text": data["code_text"],
        "factor_type": data.get("factor_type"),
        "data_source": data.get("data_source"),
        # Classification (from qe_factor_classification JOIN)
        "category": data.get("cl_category"),
        "grade": data.get("cl_grade"),
        "llm_analysis": data.get("cl_llm_analysis"),
        "classification_description": data.get("cl_description"),
        "factor_dimension": data.get("cl_factor_dimension"),
        "factor_profile": data.get("cl_factor_profile"),
        "classification_reason": data.get("cl_classification_reason"),
        "grade_reason": data.get("cl_grade_reason"),
    }
    return result


@router.get("/factors/{factor_name}/source-code", summary="下载因子源码文件")
def download_factor_source_code(
    factor_name: str,
    source: Optional[str] = Query(None, description="可选来源过滤"),
) -> FileResponse:
    conds = ["factor_name = %s"]
    params: list[Any] = [factor_name]
    if source:
        conds.append("source = %s")
        params.append(source)

    where_sql = " WHERE " + " AND ".join(conds)
    sql = f"""
        SELECT source_task_id, source_code_relpath, asset_path
        FROM aistock_factor_catalog
        {where_sql}
        ORDER BY created_at_utc DESC
        LIMIT 1
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"因子 {factor_name} 不存在")

    task_id, relpath, asset_path = row

    aistock_root = Path(__file__).resolve().parents[2]

    # 优先使用 asset_path（相对于 AIstock 根目录的 posix 路径，由回填脚本/同步服务写入）
    if asset_path:
        file_path = (aistock_root / asset_path).resolve()
        try:
            file_path.relative_to(aistock_root)
        except ValueError:
            raise HTTPException(status_code=404, detail="factor_source_path_invalid")
        if file_path.exists() and file_path.is_file():
            return FileResponse(path=str(file_path), filename=file_path.name, media_type="text/plain")

    # 兜底：使用 source_code_relpath（旧格式，相对于 task 目录）
    if not task_id or not relpath:
        raise HTTPException(status_code=404, detail="factor_source_code_not_available")

    assets_root = (aistock_root / "rdagent_assets" / "rdagent_tasks").resolve()
    task_dir = (assets_root / str(task_id)).resolve()
    rel = str(relpath).strip().lstrip("/\\")
    file_path = (task_dir / rel).resolve()

    try:
        file_path.relative_to(task_dir)
    except ValueError:
        raise HTTPException(status_code=404, detail="factor_source_path_invalid")

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="factor_source_code_missing")

    return FileResponse(path=str(file_path), filename=file_path.name, media_type="text/plain")


@router.get("/models", summary="查询 model Catalog 列表")
def list_models(
    task_run_id: Optional[str] = Query(None, description="按 task_run_id 过滤"),
    workspace_id: Optional[str] = Query(None, description="按 workspace_id 过滤"),
    node_id: Optional[str] = Query(None, description="按来源节点过滤"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """从 aistock_model_catalog 查询模型目录列表。"""

    conds = []
    params: list[Any] = []

    if task_run_id:
        conds.append("task_run_id = %s")
        params.append(task_run_id)
    if workspace_id:
        conds.append("workspace_id = %s")
        params.append(workspace_id)
    if node_id:
        conds.append("node_id = %s")
        params.append(node_id)

    where_sql = ""
    if conds:
        where_sql = " WHERE " + " AND ".join(conds)

    sql = f"""
        SELECT task_run_id, loop_id, workspace_id,
               model_config, dataset_config, model_artifacts,
               model_id, model_type, feature_schema, log_dir, display_name, flattened_feature_list,
               raw_payload, node_id
        FROM aistock_model_catalog
        {where_sql}
        ORDER BY task_run_id DESC, loop_id DESC
        LIMIT %s OFFSET %s
    """

    params_with_page = params + [limit, offset]

    items: list[dict[str, Any]] = []
    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM aistock_model_catalog{where_sql}", params)
            total = int(cur.fetchone()[0])
            cur.execute(sql, params_with_page)
            rows = cur.fetchall()

    for (
        tr,
        lid,
        wid,
        model_cfg,
        dataset_cfg,
        model_artifacts,
        model_id,
        model_type,
        feature_schema,
        log_dir,
        display_name,
        flattened_feature_list,
        raw_payload,
        node_id_val,
    ) in rows:
        items.append(
            {
                "task_run_id": tr,
                "loop_id": lid,
                "workspace_id": wid,
                "model_config": model_cfg,
                "dataset_config": dataset_cfg,
                "model_artifacts": model_artifacts,
                "model_id": model_id,
                "model_type": model_type,
                "feature_schema": feature_schema,
                "log_dir": log_dir,
                "display_name": display_name,
                "flattened_feature_list": flattened_feature_list,
                "raw_payload": raw_payload,
                "node_id": node_id_val,
            }
        )

    return {"total": total, "items": items}


@router.get("/loops/{task_run_id}/{loop_id}/artifacts", summary="通过 results-api 获取 loop artifacts 详情")
def get_loop_artifacts(task_run_id: str, loop_id: int) -> Dict[str, Any]:
    """Phase2 标准：AIstock 通过 HTTP 调用 RD-Agent results-api 获取 artifacts 详情。

    当前最小实现为 JSON 透传：直接返回 results-api 的响应内容。
    """

    client = RDAgentResultsApiClient()
    data = client.get_loop_artifacts(task_run_id=task_run_id, loop_id=loop_id)
    return {"task_run_id": task_run_id, "loop_id": loop_id, "artifacts": data}


@router.get("/alpha158/meta", summary="查询 Alpha158 因子库元信息")
def list_alpha158_meta(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """从 aistock_alpha158_meta 查询 Alpha158 因子元数据 (REQ-API-P2-001)."""
    sql = """
        SELECT factor_name, lib_version, generated_at_utc, expression, description_cn,
               variables, freq, align, nan_policy, impl_module, impl_func, impl_version,
               raw_payload
        FROM aistock_alpha158_meta
        ORDER BY factor_name
        LIMIT %s OFFSET %s
    """
    items: list[dict[str, Any]] = []
    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM aistock_alpha158_meta")
            total = int(cur.fetchone()[0])
            cur.execute(sql, [limit, offset])
            rows = cur.fetchall()

    for (
        name, lib_v, gen_at, expr, desc,
        vars_json, freq, align, nan_pol, impl_mod, impl_func, impl_v,
        raw_payload
    ) in rows:
        items.append({
            "name": name,
            "lib_version": lib_v,
            "generated_at_utc": gen_at,
            "expression": expr,
            "description_cn": desc,
            "variables": vars_json,
            "freq": freq,
            "align": align,
            "nan_policy": nan_pol,
            "impl_module": impl_mod,
            "impl_func": impl_func,
            "impl_version": impl_v,
            "raw_payload": raw_payload,
        })

    return {"total": total, "items": items}


@router.get("/strategy-loop-best", summary="按 strategy_id 汇总代表性 loop 指标摘要")
def get_strategy_best_loop_summaries() -> Dict[str, Any]:
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
                annualized_return,
                max_drawdown,
                sharpe,
                ic,
                ic_ir,
                win_rate,
                decision,
                summary_execution,
                raw_payload,
                ROW_NUMBER() OVER (
                    PARTITION BY strategy_id
                    ORDER BY
                        (status = 'success') DESC,
                        (has_result IS TRUE) DESC,
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
            annualized_return,
            max_drawdown,
            sharpe,
            ic,
            ic_ir,
            win_rate,
            decision,
            summary_execution,
            raw_payload
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
                    cur.execute("CREATE INDEX idx_loop_catalog_composite ON aistock_loop_catalog(strategy_id, status, has_result, task_run_id DESC, loop_id DESC);")
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
        ann_ret,
        mdd,
        sharpe,
        ic,
        ic_ir,
        win_rate,
        decision,
        summary_execution,
        raw_payload,
    ) in rows:
        metrics = {}
        summary_texts = {"execution": summary_execution}
        if raw_payload:
            metrics = raw_payload.get("metrics") or {}
            s_texts = raw_payload.get("summary_texts") or {}
            if isinstance(s_texts, dict):
                summary_texts.update(s_texts)

        items.append(
            {
                "strategy_id": strategy_id,
                "task_run_id": task_run_id,
                "loop_id": loop_id,
                "status": status,
                "has_result": has_result,
                "metrics": metrics,
                "annualized_return": ann_ret,
                "max_drawdown": mdd,
                "sharpe": sharpe,
                "ic": ic,
                "ic_ir": ic_ir,
                "win_rate": win_rate,
                "decision": decision,
                "summary_texts": summary_texts,
            }
        )

    return {"items": items}


@router.get("/factor-loop-best", summary="按 factor_name 汇总代表性 loop 指标摘要")
def get_factor_best_loop_summaries() -> Dict[str, Any]:
    """为每个 factor_name 选出一条代表性 loop 并返回其指标与摘要.
    
    由于 loop 表中 factor_names 是 JSONB 数组，需要使用 JSONB 路径操作符进行关联。
    选取规则：
    - 优先 status='success' 且 has_result=true 的记录;
    - 在此基础上按 generated_at_utc 降序、task_run_id、loop_id 选择最新的一条;
    """

    # 使用 LATERAL JOIN 展开 factor_names 数组，并自动剥离常见前缀以增强关联健壮性
    # 处理前缀: feature_, qlib_, alpha158_, alpha360_
    sql = """
        WITH factor_loops AS (
            SELECT 
                (CASE 
                    WHEN f.f_name LIKE 'feature_%' THEN SUBSTRING(f.f_name FROM 9)
                    WHEN f.f_name LIKE 'qlib_%' THEN SUBSTRING(f.f_name FROM 6)
                    WHEN f.f_name LIKE 'alpha158_%' THEN SUBSTRING(f.f_name FROM 10)
                    WHEN f.f_name LIKE 'alpha360_%' THEN SUBSTRING(f.f_name FROM 10)
                    ELSE f.f_name 
                 END) as factor_name,
                l.task_run_id,
                l.loop_id,
                l.status,
                l.has_result,
                l.annualized_return,
                l.max_drawdown,
                l.sharpe,
                l.ic,
                l.ic_ir,
                l.win_rate,
                l.summary_execution,
                l.raw_payload
            FROM aistock_loop_catalog l
            CROSS JOIN LATERAL jsonb_array_elements_text(l.factor_names) AS f(f_name)
            WHERE l.factor_names IS NOT NULL AND jsonb_array_length(l.factor_names) > 0
        ),
        ranked AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY factor_name
                    ORDER BY 
                        (status = 'success') DESC,
                        (has_result IS TRUE) DESC,
                        task_run_id DESC,
                        loop_id DESC
                ) as rn
            FROM factor_loops
        )
        SELECT 
            factor_name,
            task_run_id,
            loop_id,
            status,
            has_result,
            annualized_return,
            max_drawdown,
            sharpe,
            ic,
            ic_ir,
            win_rate,
            summary_execution,
            raw_payload
        FROM ranked
        WHERE rn = 1
    """

    items: list[dict[str, Any]] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 10000;") # 涉及展开大数组，超时给长一点
            cur.execute(sql)
            rows = cur.fetchall()

    for (
        f_name, tr, lid, st, has_res, ann_ret, mdd, sharpe, ic, ic_ir, win_rate,
        sum_exec, raw_payload
    ) in rows:
        metrics = {}
        summary_texts = {"execution": sum_exec}
        if raw_payload:
            metrics = raw_payload.get("metrics") or {}
            s_texts = raw_payload.get("summary_texts") or {}
            if isinstance(s_texts, dict):
                summary_texts.update(s_texts)

        items.append({
            "factor_name": f_name,
            "task_run_id": tr,
            "loop_id": lid,
            "status": st,
            "has_result": has_res,
            "metrics": metrics,
            "annualized_return": ann_ret,
            "max_drawdown": mdd,
            "sharpe": sharpe,
            "ic": ic,
            "ic_ir": ic_ir,
            "win_rate": win_rate,
            "summary_texts": summary_texts
        })

    return {"items": items}


@router.get("/strategies", summary="查询策略 Catalog 列表")
def list_strategies(
    step_name: Optional[str] = Query(None, description="按 step_name 过滤, 如 running/feedback"),
    action: Optional[str] = Query(None, description="按 action 过滤, 如 factor/model"),
    node_id: Optional[str] = Query(None, description="按来源节点过滤"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """从 aistock_strategy_catalog 查询策略模板列表."""

    conds = []
    params: list[Any] = []

    if step_name:
        conds.append("sc.step_name = %s")
        params.append(step_name)
    if action:
        conds.append("sc.action = %s")
        params.append(action)
    if node_id:
        conds.append("sc.node_id = %s")
        params.append(node_id)

    where_sql = ""
    if conds:
        where_sql = " WHERE " + " AND ".join(conds)

    sql = f"""
        WITH latest_loops AS (
            SELECT DISTINCT ON (strategy_id) 
                   strategy_id, annualized_return, max_drawdown, sharpe, ic, ic_ir, win_rate, asset_bundle_id
            FROM aistock_loop_catalog
            WHERE has_result = TRUE
            ORDER BY strategy_id, (task_run_id || '_' || loop_id::text) DESC
        )
        SELECT sc.strategy_id, sc.display_name, sc.scenario, sc.step_name, sc.action,
               sc.example_task_run_id, sc.example_loop_id, sc.example_workspace_id, sc.example_workspace_path,
               sc.template_files, sc.data_config, sc.dataset_config, sc.portfolio_config, sc.backtest_config, sc.model_config,
               sc.feature_list, sc.market, sc.instruments, sc.freq, sc.python_implementation,
               sc.raw_payload, sc.node_id,
               ll.annualized_return, ll.max_drawdown, ll.sharpe, ll.ic, ll.ic_ir, ll.win_rate, ll.asset_bundle_id
        FROM aistock_strategy_catalog sc
        LEFT JOIN latest_loops ll ON sc.strategy_id = ll.strategy_id
        {where_sql}
        ORDER BY sc.strategy_id
        LIMIT %s OFFSET %s
    """

    params_with_page = params + [limit, offset]

    items: list[dict[str, Any]] = []
    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM aistock_strategy_catalog sc{where_sql}", params)
            total = int(cur.fetchone()[0])

            cur.execute(sql, params_with_page)
            rows = cur.fetchall()

    for row in rows:
        (
            strategy_id, display_name, scenario, step, act,
            task_run_id, loop_id, workspace_id, workspace_path,
            template_files, data_config, dataset_config, portfolio_config, backtest_config, model_config,
            feature_list, market, instruments, freq, py_impl,
            raw_payload, node_id_val,
            ann_ret, mdd, sharpe, ic, ic_ir, win_rate, bundle_id
        ) = row
        items.append(
            {
                "strategy_id": strategy_id,
                "display_name": display_name,
                "scenario": scenario,
                "step_name": step,
                "action": act,
                "workspace_example": {
                    "task_run_id": task_run_id,
                    "loop_id": loop_id,
                    "workspace_id": workspace_id,
                    "workspace_path": workspace_path,
                },
                "template_files": template_files,
                "data_config": data_config,
                "dataset_config": dataset_config,
                "portfolio_config": portfolio_config,
                "backtest_config": backtest_config,
                "model_config": model_config,
                "feature_list": feature_list,
                "market": market,
                "instruments": instruments,
                "freq": freq,
                "python_implementation": py_impl,
                "raw_payload": raw_payload,
                "asset_bundle_id": bundle_id,
                "node_id": node_id_val,
                "metrics": {
                    "annualized_return": ann_ret,
                    "max_drawdown": mdd,
                    "sharpe": sharpe,
                    "ic": ic,
                    "ic_ir": ic_ir,
                    "win_rate": win_rate
                }
            }
        )

    return {"total": total, "items": items}


@router.get("/loops", summary="查询 loop Catalog 列表")
def list_loops(
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
        SELECT task_run_id, loop_id, display_name, strategy_id, status,
               annualized_return, max_drawdown, sharpe, ic, ic_ir, win_rate,
               decision, summary_execution, summary_value_feedback, summary_shape_feedback,
               factor_names, metrics,
               asset_bundle_id, is_solidified, sync_status,
               raw_payload
        FROM aistock_loop_catalog
        {where_sql}
        ORDER BY task_run_id DESC, loop_id DESC
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
        display_name,
        sid,
        st,
        ann_ret,
        mdd,
        sharpe,
        ic,
        ic_ir,
        win_rate,
        decision,
        summary_execution,
        summary_value_feedback,
        summary_shape_feedback,
        factor_names,
        metrics_col,
        bundle_id,
        is_solidified,
        sync_status,
        raw_payload,
    ) in rows:
        # 兼容性逻辑：如果 raw_payload 中包含 metrics 或 paths，提取出来
        metrics = metrics_col or {}
        paths = {}
        summary_texts = {
            "execution": summary_execution,
            "value_feedback": summary_value_feedback,
            "shape_feedback": summary_shape_feedback,
        }
        
        if raw_payload:
            if not metrics:
                metrics = raw_payload.get("metrics") or {}
            paths = raw_payload.get("paths") or {}
            
            # 提取 summary_texts
            s_texts = raw_payload.get("summary_texts") or {}
            if isinstance(s_texts, dict):
                summary_texts.update(s_texts)

        items.append(
            {
                "task_run_id": task_run_id,
                "loop_id": loop_id,
                "display_name": display_name,
                "strategy_id": sid,
                "status": st,
                "metrics": metrics,
                "annualized_return": ann_ret,
                "max_drawdown": mdd,
                "sharpe": sharpe,
                "ic": ic,
                "ic_ir": ic_ir,
                "win_rate": win_rate,
                "decision": decision,
                "summary_texts": summary_texts,
                "factor_names": factor_names,
                "factors": raw_payload.get("factors") if raw_payload else None,
                "model_weight": raw_payload.get("model_weight") if raw_payload else None,
                "materialization_status": raw_payload.get("materialization_status") if raw_payload else None,
                "materialization_error": raw_payload.get("materialization_error") if raw_payload else None,
                "materialization_updated_at_utc": raw_payload.get("materialization_updated_at_utc") if raw_payload else None,
                "asset_bundle_id": bundle_id,
                "is_solidified": is_solidified,
                "sync_status": sync_status,
                "paths": paths,
            }
        )

    return {"total": total, "items": items}


@router.get("/loops/{task_run_id}/{loop_id}/files/{file_path:path}", summary="访问 loop workspace 内的文件 (图片/JSON)")
def get_loop_file(task_run_id: str, loop_id: int, file_path: str):
    """根据 task_run_id 和 loop_id 定位 workspace 并返回指定文件.
    
    主要用于在前端展示回测曲线图片 (ret_curve.png) 和读取 feedback.json 等.
    """
    row = None
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                sql = """
                    SELECT workspace_id, workspace_path, raw_payload FROM aistock_loop_catalog
                    WHERE task_run_id = %s AND loop_id = %s
                """
                cur.execute(sql, (task_run_id, loop_id))
                row = cur.fetchone()
            except Exception:
                sql_fallback = """
                    SELECT workspace_id, raw_payload FROM aistock_loop_catalog
                    WHERE task_run_id = %s AND loop_id = %s
                """
                cur.execute(sql_fallback, (task_run_id, loop_id))
                fb = cur.fetchone()
                if fb:
                    row = (fb[0], None, fb[1])
    
    if not row:
        raise HTTPException(status_code=404, detail="Loop 记录不存在")
    
    # 路径转换 (处理 WSL/Windows 差异)
    from .rdagent import _normalize_workspace_path
    
    raw_ws_path = row[1]
    raw_payload = row[2] if len(row) > 2 else None
    if not raw_ws_path and isinstance(raw_payload, dict):
        raw_ws_path = raw_payload.get("workspace_path")
    if not raw_ws_path:
        raise HTTPException(status_code=404, detail="该 Loop 记录未记录 workspace_path，无法访问文件")
        
    ws_path = _normalize_workspace_path(raw_ws_path)
    
    abs_path = (Path(ws_path) / file_path).resolve()
    if not abs_path.exists():
        raise HTTPException(status_code=404, detail=f"文件不存在: {file_path}")
    
    # 安全性检查: 确保文件在 workspace 目录下
    if not str(abs_path).startswith(str(Path(ws_path).resolve())):
        raise HTTPException(status_code=403, detail="禁止访问 workspace 以外的文件")

    if abs_path.suffix.lower() in [".png", ".jpg", ".jpeg", ".svg"]:
        return FileResponse(abs_path)
    
    # JSON 或文本文件
    try:
        content = abs_path.read_text(encoding="utf-8")
        if abs_path.suffix.lower() == ".json":
            import json
            return json.loads(content)
        return content
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取文件失败: {str(e)}")


@router.post("/reinit-database", summary="重新初始化数据库结构")
def reinit_database():
    """重新初始化 RD-Agent Catalog 数据库结构。
    
    此操作将：
    1. 检查当前数据状态
    2. 如果需要，清空所有 catalog 表数据
    3. 添加最佳 Loop 关联字段
    4. 删除性能关联表
    
    警告：此操作会清空所有 catalog 数据，需要重新执行全量同步。
    """
    from ..db.pg_pool import get_conn
    
    result = {
        "needs_clear": False,
        "cleared": False,
        "structure_updated": False,
        "data_counts": {},
        "warnings": []
    }
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            
            # Step 1: Check current data status
            cur.execute("SELECT COUNT(*) FROM aistock_loop_catalog")
            loop_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog")
            factor_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM aistock_strategy_catalog")
            strategy_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM aistock_model_catalog")
            model_count = cur.fetchone()[0]
            
            result["data_counts"] = {
                "loop_catalog": loop_count,
                "factor_catalog": factor_count,
                "strategy_catalog": strategy_count,
                "model_catalog": model_count
            }
            
            # Check if best loop fields exist
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'aistock_factor_catalog' 
                AND table_schema = 'public'
                AND column_name = 'best_loop_id'
            """)
            factor_has_best_loop = cur.fetchone() is not None
            
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'aistock_strategy_catalog' 
                AND table_schema = 'public'
                AND column_name = 'best_loop_id'
            """)
            strategy_has_best_loop = cur.fetchone() is not None
            
            # Step 2: Decide if data needs to be cleared
            needs_clear = False
            clear_reasons = []
            
            if not factor_has_best_loop:
                needs_clear = True
                clear_reasons.append("Factor catalog missing best_loop_id field")
            
            if not strategy_has_best_loop:
                needs_clear = True
                clear_reasons.append("Strategy catalog missing best_loop_id field")
            
            # Check for performance fields that need to be removed
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'aistock_factor_catalog' 
                AND table_schema = 'public'
                AND column_name IN ('best_max_drawdown', 'best_ic', 'best_ic_ir', 'best_win_rate')
            """)
            perf_cols = [row[0] for row in cur.fetchall()]
            if perf_cols:
                needs_clear = True
                clear_reasons.append(f"Factor catalog has performance fields: {perf_cols}")
            
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'aistock_strategy_catalog' 
                AND table_schema = 'public'
                AND column_name IN ('best_annualized_return', 'best_max_drawdown', 'best_sharpe', 'best_ic', 'best_ic_ir', 'best_win_rate')
            """)
            perf_cols = [row[0] for row in cur.fetchall()]
            if perf_cols:
                needs_clear = True
                clear_reasons.append(f"Strategy catalog has performance fields: {perf_cols}")
            
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'aistock_model_catalog' 
                AND table_schema = 'public'
                AND column_name IN ('annualized_return', 'max_drawdown', 'sharpe', 'ic', 'ic_ir', 'win_rate')
            """)
            perf_cols = [row[0] for row in cur.fetchall()]
            if perf_cols:
                needs_clear = True
                clear_reasons.append(f"Model catalog has performance fields: {perf_cols}")
            
            result["needs_clear"] = needs_clear
            result["clear_reasons"] = clear_reasons
            
            # Step 3: Clear data if needed
            if needs_clear:
                tables_to_clear = [
                    'aistock_factor_catalog',
                    'aistock_strategy_catalog',
                    'aistock_loop_catalog',
                    'aistock_model_catalog',
                    'aistock_alpha158_meta',
                    'aistock_alpha360_meta'
                ]
                
                for table in tables_to_clear:
                    cur.execute(f"TRUNCATE TABLE {table} CASCADE")
                
                result["cleared"] = True
                result["warnings"].append(f"Cleared {len(tables_to_clear)} catalog tables")
            
            # Step 4: Add best loop relation fields
            cur.execute("""
                ALTER TABLE aistock_factor_catalog
                ADD COLUMN IF NOT EXISTS best_loop_task_run_id TEXT,
                ADD COLUMN IF NOT EXISTS best_loop_id INTEGER
            """)
            
            cur.execute("""
                ALTER TABLE aistock_strategy_catalog
                ADD COLUMN IF NOT EXISTS best_loop_task_run_id TEXT,
                ADD COLUMN IF NOT EXISTS best_loop_id INTEGER
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_factor_best_loop ON aistock_factor_catalog (best_loop_task_run_id, best_loop_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_best_loop ON aistock_strategy_catalog (best_loop_task_run_id, best_loop_id)")
            
            result["structure_updated"] = True
            
            # Step 5: Drop performance association tables
            for table in ['aistock_factor_loop_performance', 'aistock_strategy_loop_performance']:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            
            # Step 6: Verify structure
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'aistock_factor_catalog' 
                AND table_schema = 'public'
                AND column_name IN ('best_loop_task_run_id', 'best_loop_id')
            """)
            factor_cols = [row[0] for row in cur.fetchall()]
            
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'aistock_strategy_catalog' 
                AND table_schema = 'public'
                AND column_name IN ('best_loop_task_run_id', 'best_loop_id')
            """)
            strategy_cols = [row[0] for row in cur.fetchall()]
            
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'aistock_model_catalog' 
                AND table_schema = 'public'
                AND column_name IN ('task_run_id', 'loop_id')
            """)
            model_cols = [row[0] for row in cur.fetchall()]
            
            result["structure"] = {
                "factor_catalog": factor_cols,
                "strategy_catalog": strategy_cols,
                "model_catalog": model_cols
            }
            
            # Step 7: Check data count after clear
            cur.execute("SELECT COUNT(*) FROM aistock_loop_catalog")
            result["data_counts_after"] = {
                "loop_catalog": cur.fetchone()[0],
                "factor_catalog": 0,
                "strategy_catalog": 0,
                "model_catalog": 0
            }
    
    return result
