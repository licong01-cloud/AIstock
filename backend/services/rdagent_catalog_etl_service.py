from __future__ import annotations

import json
import math
import os
import numbers
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from psycopg2.extras import execute_values

from ..db.pg_pool import get_conn
from .rdagent_task_sync_service import _normalize_workspace_path

JsonDict = Dict[str, Any]


def _short_id(val: Any, keep: int = 10) -> str:
    s = "" if val is None else str(val)
    s = s.strip()
    if not s:
        return ""
    return s if len(s) <= keep else s[:keep]


def _sanitize_json_obj(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize_json_obj(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_json_obj(v) for v in obj]
    try:
        if isinstance(obj, numbers.Real):
            val = float(obj)
            return val if math.isfinite(val) else None
    except Exception:
        return obj
    return obj


@dataclass(frozen=True)
class ImportSummary:
    kind: Literal["factor", "strategy", "loop", "model", "alpha158", "alpha360"]
    total_in_json: int
    inserted: int
    updated: int
    processed: int  # 2026-01-05: inserted + updated
    skipped: int
    errors: int


def _load_json(path: Path) -> JsonDict:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def _normalize_factor_name(name: str) -> str:
    """统一标准化因子名称，剥离常见前缀，确保各表关联一致."""
    if not name:
        return ""
    prefixes = ["feature_", "qlib_", "alpha158_", "alpha360_"]
    changed = True
    while changed:
        changed = False
        for p in prefixes:
            if name.startswith(p):
                name = name[len(p):]
                changed = True
                break
    return name


def import_alpha_meta_from_payload(data: JsonDict, kind: Literal["alpha158", "alpha360"]) -> ImportSummary:
    """Import Alpha meta payload into respective table."""
    # 2026-01-05 Fix: RD-Agent 侧的 JSON Key 可能不统一，显式检查多种可能的 Key
    items = data.get("factors")
    if items is None:
        items = data.get("meta")
    if items is None:
        items = data.get(kind)
    if items is None:
        items = data.get(f"{kind}_meta")
    if items is None:
        # 特别针对 514+ 因子缺失的情况：探测 data_all 顶层是否存在 alpha360 或 alpha158
        items = data.get("alpha360") or data.get("alpha158")
    
    if items is None:
        # 最后的兜底：尝试遍历所有 values 寻找包含因子结构的列表
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                # 如果是列表且包含 factor_name 或 name 字段，则认为是因子列表
                if "name" in val[0] or "factor_name" in val[0]:
                    items = val
                    print(f"[rdagent][etl] Auto-detected factor list in key: {key}")
                    break
    
    items = items or []
    total = len(items)
    print(f"[rdagent][etl] Found {total} alpha factors in payload for {kind}")
    
    if total == 0:
        return ImportSummary(kind=kind, total_in_json=0, inserted=0, updated=0, processed=0, skipped=0, errors=0)

    lib_version = str(data.get("version", ""))
    generated_at_utc = data.get("generated_at_utc")
    
    table_name = f"aistock_{kind}_meta"

    seen_names = set()
    rows: List[tuple] = []
    for item in items:
        # 兼容两种可能的名称字段名
        raw_name = item.get("name") or item.get("factor_name")
        if not raw_name:
            continue

        # 2026-01-05 Fix: 标准化因子名，移除多种可能的前缀
        name = _normalize_factor_name(raw_name)
        
        # 2026-01-05 Fix: 显式去重，防止 PostgreSQL upsert 报错 "cannot affect row a second time"
        if name in seen_names:
            continue
        seen_names.add(name)

        expression = item.get("expression")
        # 2026-01-05 Fix: 深度恢复表达式逻辑 (针对 Alpha 专表)
        if not expression:
            # 1. 尝试从 formula_hint 或 impl_func 恢复
            expression = item.get("formula_hint") or item.get("impl_func")
            
            # 2. 如果还是没有，直接从 item 字典中探测 (Qlib 风格)
            if not expression:
                c_args = item.get("class_args", {})
                if isinstance(c_args, dict) and "feature" in c_args:
                    expression = str(c_args["feature"])
                elif "qlib_expression" in item:
                    expression = item["qlib_expression"]

        description_cn = item.get("description_cn")
        variables = item.get("variables")
        freq = item.get("freq")
        align = item.get("align")
        nan_policy = item.get("nan_policy")
        impl_module = item.get("impl_module")
        impl_func = item.get("impl_func")
        impl_version = item.get("impl_version")
        
        # 2026-01-05 Phase 3: Alpha 表新增字段提取
        best_performance = item.get("best_performance")
        best_performance_sharpe = item.get("best_performance_sharpe")
        best_performance_ann_ret = item.get("best_performance_ann_ret")
        interface_info = item.get("interface_info")
        asset_bundle_id = item.get("asset_bundle_id")

        structured_keys = {
            "name",
            "factor_name",
            "expression",
            "description_cn",
            "variables",
            "freq",
            "align",
            "nan_policy",
            "impl_module",
            "impl_func",
            "impl_version",
            "best_performance",
            "best_performance_sharpe",
            "best_performance_ann_ret",
            "interface_info",
            "asset_bundle_id",
            "source_task_id",
            "source_code_origin",
            "source_loop_tag",
            "source_index",
        }
        raw_payload = {k: v for k, v in item.items() if k not in structured_keys}

        rows.append(
            (
                name,
                lib_version,
                generated_at_utc,
                expression,
                description_cn,
                json.dumps(variables) if variables is not None else None,
                freq,
                align,
                nan_policy,
                impl_module,
                impl_func,
                impl_version,
                best_performance,
                best_performance_sharpe,
                best_performance_ann_ret,
                json.dumps(interface_info) if interface_info is not None else None,
                asset_bundle_id,
                json.dumps(raw_payload) if raw_payload is not None else None,
            ),
        )

    if not rows:
        return ImportSummary(kind=kind, total_in_json=total, inserted=0, updated=0, processed=0, skipped=total, errors=0)

    sql = f"""
        INSERT INTO {table_name} (
            factor_name,
            lib_version,
            generated_at_utc,
            expression,
            description_cn,
            variables,
            freq,
            align,
            nan_policy,
            impl_module,
            impl_func,
            impl_version,
            best_performance,
            best_performance_sharpe,
            best_performance_ann_ret,
            interface_info,
            asset_bundle_id,
            raw_payload
        )
        VALUES %s
        ON CONFLICT (factor_name) DO UPDATE SET
            lib_version = EXCLUDED.lib_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            expression = EXCLUDED.expression,
            description_cn = EXCLUDED.description_cn,
            variables = EXCLUDED.variables,
            freq = EXCLUDED.freq,
            align = EXCLUDED.align,
            nan_policy = EXCLUDED.nan_policy,
            impl_module = EXCLUDED.impl_module,
            impl_func = EXCLUDED.impl_func,
            impl_version = EXCLUDED.impl_version,
            best_performance = EXCLUDED.best_performance,
            best_performance_sharpe = EXCLUDED.best_performance_sharpe,
            best_performance_ann_ret = EXCLUDED.best_performance_ann_ret,
            interface_info = EXCLUDED.interface_info,
            asset_bundle_id = EXCLUDED.asset_bundle_id,
            source_task_id = EXCLUDED.source_task_id,
            source_code_origin = EXCLUDED.source_code_origin,
            source_loop_tag = EXCLUDED.source_loop_tag,
            source_index = EXCLUDED.source_index,
            raw_payload = EXCLUDED.raw_payload
    """

    # 2026-01-05 Fix: 优化 ImportSummary 计数逻辑
    total_rows = len(rows)

    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql, rows, page_size=1000)
                # 准确获取受影响行数
                processed = cur.rowcount if cur.rowcount > 0 else total_rows
            except Exception as exc:
                print(f"[rdagent][etl] import_{kind}_meta_from_payload failed: {exc!r}")
                raise exc

    return ImportSummary(
        kind=kind, 
        total_in_json=total, 
        inserted=processed, 
        updated=0, 
        processed=processed, 
        skipped=total - total_rows, 
        errors=0
    )


def import_alpha158_meta_from_payload(data: JsonDict) -> ImportSummary:
    return import_alpha_meta_from_payload(data, kind="alpha158")


def import_alpha360_meta_from_payload(data: JsonDict) -> ImportSummary:
    return import_alpha_meta_from_payload(data, kind="alpha360")


def backfill_alpha158_expressions() -> Dict[str, int]:
    """从QLib源码提取Alpha158因子的表达式，回填到aistock_factor_catalog中缺失expression的alpha158因子。

    Returns:
        {"updated": N, "total_qlib": M} 更新数量和QLib因子总数
    """
    try:
        from qlib.contrib.data.handler import Alpha158 as _Alpha158
        handler = _Alpha158.__new__(_Alpha158)
        expressions, names = handler.get_feature_config()
    except Exception as exc:
        print(f"[rdagent][etl] backfill_alpha158_expressions: QLib Alpha158 不可用: {exc}")
        return {"updated": 0, "total_qlib": 0, "error": str(exc)}

    factor_expr_map = dict(zip(names, expressions))

    updated = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for name, expr in factor_expr_map.items():
                cur.execute("""
                    UPDATE aistock_factor_catalog
                    SET expression = %s
                    WHERE factor_name = %s AND source = 'alpha158'
                      AND (expression IS NULL OR expression = '')
                """, (expr, name))
                if cur.rowcount > 0:
                    updated += 1

    if updated:
        print(f"[rdagent][etl] backfill_alpha158_expressions: 回填了 {updated} 个因子的expression")
    return {"updated": updated, "total_qlib": len(factor_expr_map)}


def import_factor_catalog_from_payload(data: JsonDict) -> ImportSummary:
    # 2026-01-05 Fix: 增加健壮的 Key 探测逻辑，确保能从多种 JSON 结构中提取因子
    factors = data.get("factors")
    if factors is None:
        factors = data.get("meta")
    if factors is None:
        # 兜底：尝试遍历寻找列表
        for val in data.values():
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict) and ("name" in val[0] or "factor_name" in val[0]):
                factors = val
                break
    
    factors = factors or []
    total = len(factors)
    if total == 0:
        return ImportSummary(kind="factor", total_in_json=0, inserted=0, updated=0, processed=0, skipped=0, errors=0)

    catalog_version = str(data.get("version", ""))
    generated_at_utc = data.get("generated_at_utc")
    catalog_source = str(data.get("source", ""))

    seen_keys = set()
    unique_rows = []
    for item in factors:
        raw_name = item.get("name") or item.get("factor_name")
        source = item.get("source")
        if not raw_name:
            continue

        # 2026-01-05 Fix: 标准化因子名称，移除多种常见前缀以便与 Loop 匹配
        name = _normalize_factor_name(raw_name)

        # 2026-01-04 Fix: 防止 ON CONFLICT DO UPDATE 报错 "cannot affect row a second time"
        key = (str(name), str(source))
        if key in seen_keys:
            continue
        seen_keys.add(key)

        expression = item.get("expression")
        # 2026-01-05 Fix: 深度恢复表达式逻辑 (针对 Factor 主表)
        if not expression:
            # 1. 尝试从 formula_hint 或 impl_func 恢复
            expression = item.get("formula_hint") or item.get("factor_formulation") or item.get("impl_func")
            
            # 2. 如果还是没有，直接从 item 字典中探测 (Qlib 风格)
            if not expression:
                c_args = item.get("class_args", {})
                if isinstance(c_args, dict) and "feature" in c_args:
                    expression = str(c_args["feature"])
                elif "qlib_expression" in item:
                    expression = item["qlib_expression"]

        region = item.get("region")
        
        # 2026-01-05 Fix: 确保 tags 总是列表且提取正确
        tags = item.get("tags")
        if tags is None:
            tags = []
        elif isinstance(tags, str):
            tags = [tags]

        # Phase 3 设计中要求的完整元数据字段
        description_cn = item.get("description_cn")
        formula_hint = item.get("formula_hint") or item.get("factor_formulation")
        variables = item.get("variables")
        freq = item.get("freq")
        align = item.get("align")
        nan_policy = item.get("nan_policy")
        created_at_utc_factor = item.get("created_at_utc")
        experiment_id = item.get("experiment_id")

        impl_module = item.get("impl_module")
        impl_func = item.get("impl_func")
        impl_version = item.get("impl_version")
        performance_metrics = item.get("performance_metrics")
        source_task_id = item.get("source_task_id")
        source_code_origin = item.get("source_code_origin")
        source_loop_tag = item.get("source_loop_tag")
        source_index = item.get("source_index")

        # 非 alpha 因子：记录首次进入 SOTA 的 task_id（只保留首次，不重复）
        first_sota_task_id = item.get("first_sota_task_id")
        raw_is_sota = item.get("is_sota_factor")
        if isinstance(raw_is_sota, str):
            raw_is_sota = raw_is_sota.strip().lower() in {"1", "true", "yes", "y", "on"}
        is_sota_factor = bool(raw_is_sota) if raw_is_sota is not None else None
        
        # 2026-01-05 Phase 3: 新增字段支持
        best_performance = item.get("best_performance")
        best_performance_sharpe = item.get("best_performance_sharpe")
        best_performance_ann_ret = item.get("best_performance_ann_ret")
        interface_info = item.get("interface_info")
        asset_bundle_id = item.get("asset_bundle_id")

        # raw_payload 中仅保留未结构化的剩余字段，避免信息丢失
        structured_keys = {
            "name",
            "expression",
            "source",
            "region",
            "tags",
            "description_cn",
            "formula_hint",
            "factor_formulation",
            "variables",
            "freq",
            "align",
            "nan_policy",
            "created_at_utc",
            "experiment_id",
            "impl_module",
            "impl_func",
            "impl_version",
            "performance_metrics",
            "first_sota_task_id",
            "is_sota_factor",
            "best_performance",
            "best_performance_sharpe",
            "best_performance_ann_ret",
            "interface_info",
            "asset_bundle_id",
        }
        raw_payload = {k: v for k, v in item.items() if k not in structured_keys}

        unique_rows.append(
            (
                name,
                catalog_version,
                generated_at_utc,
                catalog_source,
                expression,
                source,
                region,
                json.dumps(tags) if tags is not None else None,
                description_cn,
                formula_hint,
                json.dumps(variables) if variables is not None else None,
                freq,
                align,
                nan_policy,
                created_at_utc_factor,
                experiment_id,
                impl_module,
                impl_func,
                impl_version,
                json.dumps(performance_metrics) if performance_metrics is not None else None,
                first_sota_task_id,
                is_sota_factor,
                best_performance,
                json.dumps(interface_info) if interface_info is not None else None,
                asset_bundle_id,
                source_task_id,
                source_code_origin,
                source_loop_tag,
                int(source_index) if source_index is not None else None,
                json.dumps(raw_payload) if raw_payload is not None else None,
            )
        )

    sql = """
        INSERT INTO aistock_factor_catalog (
            factor_name,
            catalog_version,
            generated_at_utc,
            catalog_source,
            expression,
            source,
            region,
            tags,
            description_cn,
            formula_hint,
            variables,
            freq,
            align,
            nan_policy,
            created_at_utc,
            experiment_id,
            impl_module,
            impl_func,
            impl_version,
            performance_metrics,
            first_sota_task_id,
            is_sota_factor,
            best_performance,
            interface_info,
            asset_bundle_id,
            source_task_id,
            source_code_origin,
            source_loop_tag,
            source_index,
            raw_payload
        )
        VALUES %s
        ON CONFLICT (factor_name, source) DO UPDATE SET
            catalog_version = EXCLUDED.catalog_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            catalog_source = EXCLUDED.catalog_source,
            expression = EXCLUDED.expression,
            region = EXCLUDED.region,
            tags = EXCLUDED.tags,
            description_cn = EXCLUDED.description_cn,
            formula_hint = EXCLUDED.formula_hint,
            variables = EXCLUDED.variables,
            freq = EXCLUDED.freq,
            align = EXCLUDED.align,
            nan_policy = EXCLUDED.nan_policy,
            created_at_utc = EXCLUDED.created_at_utc,
            experiment_id = EXCLUDED.experiment_id,
            impl_module = EXCLUDED.impl_module,
            impl_func = EXCLUDED.impl_func,
            impl_version = EXCLUDED.impl_version,
            performance_metrics = EXCLUDED.performance_metrics,
            first_sota_task_id = COALESCE(aistock_factor_catalog.first_sota_task_id, EXCLUDED.first_sota_task_id),
            is_sota_factor = COALESCE(aistock_factor_catalog.is_sota_factor, FALSE) OR COALESCE(EXCLUDED.is_sota_factor, FALSE),
            best_performance = EXCLUDED.best_performance,
            interface_info = EXCLUDED.interface_info,
            asset_bundle_id = EXCLUDED.asset_bundle_id,
            raw_payload = EXCLUDED.raw_payload
    """

    # 2026-01-05 Fix: 优化 ImportSummary 计数逻辑
    total_rows = len(unique_rows)
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql, unique_rows, page_size=1000)
                processed = cur.rowcount if cur.rowcount > 0 else total_rows
            except Exception as e:
                print(f"[rdagent][etl] import_factor_catalog_from_payload failed: {e!r}")
                raise

    return ImportSummary(
        kind="factor", 
        total_in_json=total, 
        inserted=processed, 
        updated=0, 
        processed=processed, 
        skipped=total - total_rows, 
        errors=0
    )


def import_strategy_catalog_from_payload(data: JsonDict) -> ImportSummary:
    strategies = data.get("strategies") or []
    total = len(strategies)
    if total == 0:
        return ImportSummary(kind="strategy", total_in_json=0, inserted=0, updated=0, processed=0, skipped=0, errors=0)

    catalog_version = str(data.get("version", ""))
    generated_at_utc = data.get("generated_at_utc")
    catalog_source = str(data.get("source", ""))

    seen_keys = set()
    unique_rows = []
    for item in strategies:
        strategy_id = item.get("strategy_id")
        if not strategy_id:
            continue
        
        if strategy_id in seen_keys:
            continue
        seen_keys.add(strategy_id)

        scenario = item.get("scenario")
        step_name = item.get("step_name")
        action = item.get("action")

        ws_example = item.get("workspace_example") or {}
        example_task_run_id = ws_example.get("task_run_id")
        example_loop_id = ws_example.get("loop_id")
        example_workspace_id = ws_example.get("workspace_id")
        example_workspace_path = ws_example.get("workspace_path")

        template_files = item.get("template_files")
        data_config = item.get("data_config")
        dataset_config = item.get("dataset_config")
        portfolio_config = item.get("portfolio_config")
        backtest_config = item.get("backtest_config")
        model_config = item.get("model_config")
        feature_list = item.get("feature_list")
        market = item.get("market")
        instruments = item.get("instruments")
        freq = item.get("freq")
        python_implementation = item.get("python_implementation")
        in_selection_center = bool(item.get("in_selection_center", False))

        display_name = f"strategy-{_short_id(strategy_id)}"

        structured_keys = {
            "strategy_id",
            "scenario",
            "step_name",
            "action",
            "workspace_example",
            "template_files",
            "data_config",
            "dataset_config",
            "portfolio_config",
            "backtest_config",
            "model_config",
            "feature_list",
            "market",
            "instruments",
            "freq",
            "python_implementation",
            "in_selection_center",
        }
        raw_payload = {k: v for k, v in item.items() if k not in structured_keys}

        unique_rows.append(
            (
                strategy_id,
                display_name,
                catalog_version,
                generated_at_utc,
                catalog_source,
                scenario,
                step_name,
                action,
                example_task_run_id,
                example_loop_id,
                example_workspace_id,
                example_workspace_path,
                json.dumps(template_files) if template_files is not None else None,
                json.dumps(data_config) if data_config is not None else None,
                json.dumps(dataset_config) if dataset_config is not None else None,
                json.dumps(portfolio_config) if portfolio_config is not None else None,
                json.dumps(backtest_config) if backtest_config is not None else None,
                json.dumps(model_config) if model_config is not None else None,
                json.dumps(feature_list) if feature_list is not None else None,
                market,
                json.dumps(instruments) if instruments is not None else None,
                freq,
                json.dumps(python_implementation) if python_implementation is not None else None,
                in_selection_center,
                json.dumps(raw_payload) if raw_payload is not None else None,
            )
        )

    sql = """
        INSERT INTO aistock_strategy_catalog (
            strategy_id,
            display_name,
            catalog_version,
            generated_at_utc,
            catalog_source,
            scenario,
            step_name,
            action,
            example_task_run_id,
            example_loop_id,
            example_workspace_id,
            example_workspace_path,
            template_files,
            data_config,
            dataset_config,
            portfolio_config,
            backtest_config,
            model_config,
            feature_list,
            market,
            instruments,
            freq,
            python_implementation,
            in_selection_center,
            raw_payload
        )
        VALUES %s
        ON CONFLICT (strategy_id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            catalog_version = EXCLUDED.catalog_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            catalog_source = EXCLUDED.catalog_source,
            scenario = EXCLUDED.scenario,
            step_name = EXCLUDED.step_name,
            action = EXCLUDED.action,
            example_task_run_id = EXCLUDED.example_task_run_id,
            example_loop_id = EXCLUDED.example_loop_id,
            example_workspace_id = EXCLUDED.example_workspace_id,
            example_workspace_path = EXCLUDED.example_workspace_path,
            template_files = EXCLUDED.template_files,
            data_config = EXCLUDED.data_config,
            dataset_config = EXCLUDED.dataset_config,
            portfolio_config = EXCLUDED.portfolio_config,
            backtest_config = EXCLUDED.backtest_config,
            model_config = EXCLUDED.model_config,
            feature_list = EXCLUDED.feature_list,
            market = EXCLUDED.market,
            instruments = EXCLUDED.instruments,
            freq = EXCLUDED.freq,
            python_implementation = EXCLUDED.python_implementation,
            in_selection_center = EXCLUDED.in_selection_center,
            raw_payload = EXCLUDED.raw_payload
    """

    # 2026-01-05 Fix: 优化 ImportSummary 计数逻辑
    total_rows = len(unique_rows)
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql, unique_rows, page_size=500)
                processed = cur.rowcount if cur.rowcount > 0 else total_rows
            except Exception as e:
                print(f"[rdagent][etl] import_strategy_catalog_from_payload failed: {e!r}")
                raise

    return ImportSummary(
        kind="strategy", 
        total_in_json=total, 
        inserted=processed, 
        updated=0, 
        processed=processed, 
        skipped=total - total_rows, 
        errors=0
    )


def _build_loop_payload_from_scan(data: JsonDict) -> JsonDict:
    results = data.get("results")
    if not isinstance(results, list):
        return {"loops": []}

    def _extract_model_type(workspace_path: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        if not workspace_path:
            return None, None
        ws_norm = _normalize_workspace_path(workspace_path)
        if not ws_norm:
            return None, None
        meta_path = (Path(ws_norm) / "model_meta.json").resolve()
        if not meta_path.exists() or not meta_path.is_file():
            return None, None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            return None, str(meta_path)
        model_type = None
        if isinstance(meta, dict):
            model_type = meta.get("model_type") or meta.get("model_conf", {}).get("class")
        return (str(model_type) if model_type else None, str(meta_path))

    loops: list[dict[str, Any]] = []
    for task in results:
        task_id = task.get("task_id") or task.get("task_run_id")
        loop_results = task.get("loop_results") or []
        if not task_id or not isinstance(loop_results, list):
            continue

        for idx, loop in enumerate(loop_results):
            loop_id = loop.get("loop_id")
            if loop_id is None:
                loop_id = loop.get("trace_index", idx)
            try:
                loop_id_int = int(loop_id)
            except (TypeError, ValueError):
                continue

            metrics = loop.get("metrics")
            if not isinstance(metrics, dict) or not metrics:
                continue
            metrics = _sanitize_json_obj(metrics)

            factors = loop.get("factors") or []
            factor_names = [f.get("factor_name") for f in factors if f.get("factor_name")]
            workspace_path = loop.get("workspace_path")
            workspace_path_norm = _normalize_workspace_path(workspace_path) if workspace_path else None
            model_type, model_meta_path = _extract_model_type(workspace_path_norm or workspace_path)

            loops.append(
                {
                    "task_run_id": task_id,
                    "loop_id": loop_id_int,
                    "workspace_path": workspace_path_norm or workspace_path,
                    "action": loop.get("action"),
                    "decision": loop.get("decision"),
                    "status": "success",
                    "has_result": True,
                    "metrics": metrics,
                    "factor_names": factor_names,
                    "raw_payload": {
                        "task_id": task.get("task_id"),
                        "trace_index": loop.get("trace_index"),
                        "factors": factors,
                        "model_weight": loop.get("model_weight"),
                        "model_type": model_type,
                        "model_meta_path": model_meta_path,
                    },
                }
            )

    return {
        "version": data.get("version") or "scan_v1",
        "generated_at_utc": data.get("generated_at_utc") or datetime.now().isoformat(),
        "source": data.get("source") or "rdagent_scan",
        "loops": loops,
    }


def import_loop_catalog_from_payload(data: JsonDict) -> ImportSummary:
    loops = data.get("loops") or []
    total = len(loops)
    if total == 0:
        return ImportSummary(kind="loop", total_in_json=0, inserted=0, updated=0, processed=0, skipped=0, errors=0)

    catalog_version = str(data.get("version", ""))
    generated_at_utc = data.get("generated_at_utc")
    catalog_source = str(data.get("source", ""))

    seen_keys = set()
    unique_rows = []
    for item in loops:
        task_run_id = item.get("task_run_id")
        loop_id = item.get("loop_id")
        if not task_run_id or loop_id is None:
            continue
        
        key = (str(task_run_id), int(loop_id))
        if key in seen_keys:
            continue
        seen_keys.add(key)

        asset_bundle_id = item.get("asset_bundle_id")
        is_solidified = bool(item.get("is_solidified", 0))
        sync_status = item.get("sync_status", "pending")

        workspace_id = item.get("workspace_id")
        workspace_path = item.get("workspace_path")
        scenario = item.get("scenario")
        step_name = item.get("step_name")
        action = item.get("action")
        status = item.get("status")
        has_result = item.get("has_result")
        strategy_id = item.get("strategy_id")
        factor_names = item.get("factor_names")
        
        if not factor_names and strategy_id:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT feature_list FROM aistock_strategy_catalog WHERE strategy_id = %s", (strategy_id,))
                    row = cur.fetchone()
                    if row and row[0]:
                        factor_names = row[0] if isinstance(row[0], list) else []

        fname_top = item.get("factor_name")
        if not factor_names:
            if fname_top:
                factor_names = [fname_top]
            else:
                raw_payload_data = item.get("raw_payload", {})
                if isinstance(raw_payload_data, dict):
                    fname_raw = raw_payload_data.get("factor_name")
                    if fname_raw:
                        factor_names = [fname_raw]
        
        if factor_names:
            factor_names = [_normalize_factor_name(f) for f in factor_names]

        log_dir = item.get("log_dir")
        workspace_role = item.get("workspace_role")
        paths = item.get("paths") or {}
        if not isinstance(paths, dict):
            paths = {}
        path_factor_meta = paths.get("factor_meta")
        path_factor_perf = paths.get("factor_perf")
        path_feedback = paths.get("feedback")
        path_ret_curve = paths.get("ret_curve") or paths.get("ret_curve.png")
        path_dd_curve = paths.get("dd_curve") or paths.get("dd_curve.png")
        path_mlruns = paths.get("mlruns")
        path_model_files = paths.get("model_files")
        if isinstance(path_model_files, (list, dict)):
            path_model_files = json.dumps(path_model_files, ensure_ascii=False)

        metrics = item.get("metrics")

        def _metric(name: str) -> Any:
            if name in item and item.get(name) is not None:
                return item.get(name)
            
            if isinstance(metrics, dict):
                if name in metrics and metrics.get(name) is not None:
                    return metrics.get(name)
                
                mapping = {
                    "annualized_return": [
                        "1day.excess_return_with_cost.annualized_return",
                        "1day.excess_return_without_cost.annualized_return",
                        "annual_return"
                    ],
                    "max_drawdown": [
                        "1day.excess_return_with_cost.max_drawdown",
                        "1day.excess_return_without_cost.max_drawdown"
                    ],
                    "sharpe": [
                        "1day.excess_return_with_cost.information_ratio",
                        "1day.excess_return_without_cost.information_ratio",
                        "sharpe_ratio",
                        "information_ratio"
                    ],
                    "ic": ["IC", "Rank IC", "1day.Rank IC.mean", "1day.IC.mean"],
                    "ic_ir": ["ICIR", "Rank ICIR", "1day.Rank IC.std", "1day.IC.std"],
                }
                
                if name in mapping:
                    for alt_key in mapping[name]:
                        if alt_key in metrics and metrics.get(alt_key) is not None:
                            return metrics.get(alt_key)
            return None

        annualized_return = _metric("annualized_return")
        max_drawdown = _metric("max_drawdown")
        sharpe = _metric("sharpe")
        ic_val = _metric("ic")
        ic_ir = _metric("ic_ir")
        win_rate = _metric("win_rate")
        decision = item.get("decision")

        summary_texts = item.get("summary_texts") or {}
        summary_execution = item.get("summary_execution") or summary_texts.get("execution")
        summary_value_feedback = item.get("summary_value_feedback") or summary_texts.get("value_feedback")
        summary_shape_feedback = item.get("summary_shape_feedback") or summary_texts.get("shape_feedback")

        display_name = f"{_short_id(task_run_id)}-loop{int(loop_id)}"

        structured_keys = {
            "catalog_version",
            "generated_at_utc",
            "catalog_source",
            "task_run_id",
            "loop_id",
            "workspace_id",
            "workspace_path",
            "scenario",
            "step_name",
            "action",
            "status",
            "has_result",
            "strategy_id",
            "factor_names",
            "metrics",
            "annualized_return",
            "max_drawdown",
            "sharpe",
            "ic",
            "ic_ir",
            "win_rate",
            "decision",
            "summary_texts",
            "summary_execution",
            "summary_value_feedback",
            "summary_shape_feedback",
            "log_dir",
            "workspace_role",
            "paths",
        }
        raw_payload_dict = {k: v for k, v in item.items() if k not in structured_keys}

        unique_rows.append(
            (
                catalog_version,
                generated_at_utc,
                catalog_source,
                task_run_id,
                int(loop_id),
                display_name,
                workspace_id,
                workspace_path,
                asset_bundle_id,
                is_solidified,
                sync_status,
                scenario,
                step_name,
                action,
                status,
                bool(has_result) if has_result is not None else None,
                strategy_id,
                json.dumps(factor_names) if factor_names is not None else None,
                json.dumps(metrics) if metrics is not None else None,
                annualized_return,
                max_drawdown,
                sharpe,
                ic_val,
                ic_ir,
                win_rate,
                decision,
                summary_execution,
                summary_value_feedback,
                summary_shape_feedback,
                log_dir,
                workspace_role,
                path_factor_meta,
                path_factor_perf,
                path_feedback,
                path_ret_curve,
                path_dd_curve,
                path_mlruns,
                path_model_files,
                json.dumps(raw_payload_dict) if raw_payload_dict is not None else None,
            )
        )

    sql = """
        INSERT INTO aistock_loop_catalog (
            catalog_version,
            generated_at_utc,
            catalog_source,
            task_run_id,
            loop_id,
            display_name,
            workspace_id,
            workspace_path,
            asset_bundle_id,
            is_solidified,
            sync_status,
            scenario,
            step_name,
            action,
            status,
            has_result,
            strategy_id,
            factor_names,
            metrics,
            annualized_return,
            max_drawdown,
            sharpe,
            ic,
            ic_ir,
            win_rate,
            decision,
            summary_execution,
            summary_value_feedback,
            summary_shape_feedback,
            log_dir,
            workspace_role,
            path_factor_meta,
            path_factor_perf,
            path_feedback,
            path_ret_curve,
            path_dd_curve,
            path_mlruns,
            path_model_files,
            raw_payload
        )
        VALUES %s
        ON CONFLICT (task_run_id, loop_id) DO UPDATE SET
            catalog_version = EXCLUDED.catalog_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            catalog_source = EXCLUDED.catalog_source,
            display_name = EXCLUDED.display_name,
            workspace_id = EXCLUDED.workspace_id,
            workspace_path = EXCLUDED.workspace_path,
            asset_bundle_id = EXCLUDED.asset_bundle_id,
            is_solidified = EXCLUDED.is_solidified,
            sync_status = EXCLUDED.sync_status,
            scenario = EXCLUDED.scenario,
            step_name = EXCLUDED.step_name,
            action = EXCLUDED.action,
            status = EXCLUDED.status,
            has_result = EXCLUDED.has_result,
            strategy_id = EXCLUDED.strategy_id,
            factor_names = EXCLUDED.factor_names,
            metrics = EXCLUDED.metrics,
            annualized_return = EXCLUDED.annualized_return,
            max_drawdown = EXCLUDED.max_drawdown,
            sharpe = EXCLUDED.sharpe,
            ic = EXCLUDED.ic,
            ic_ir = EXCLUDED.ic_ir,
            win_rate = EXCLUDED.win_rate,
            decision = EXCLUDED.decision,
            summary_execution = EXCLUDED.summary_execution,
            summary_value_feedback = EXCLUDED.summary_value_feedback,
            summary_shape_feedback = EXCLUDED.summary_shape_feedback,
            log_dir = EXCLUDED.log_dir,
            workspace_role = EXCLUDED.workspace_role,
            path_factor_meta = EXCLUDED.path_factor_meta,
            path_factor_perf = EXCLUDED.path_factor_perf,
            path_feedback = EXCLUDED.path_feedback,
            path_ret_curve = EXCLUDED.path_ret_curve,
            path_dd_curve = EXCLUDED.path_dd_curve,
            path_mlruns = EXCLUDED.path_mlruns,
            path_model_files = EXCLUDED.path_model_files,
            raw_payload = EXCLUDED.raw_payload
    """

    # 2026-01-05 Fix: 优化 ImportSummary 计数逻辑
    total_rows = len(unique_rows)
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql, unique_rows, page_size=500)
                processed = cur.rowcount if cur.rowcount > 0 else total_rows
            except Exception as e:
                print(f"[import_loop_catalog_from_payload] failed to insert {total} rows: {e}")
                raise

    return ImportSummary(
        kind="loop", 
        total_in_json=total, 
        inserted=processed, 
        updated=0, 
        processed=processed, 
        skipped=total - total_rows, 
        errors=0
    )


def import_model_catalog_from_payload(data: JsonDict) -> ImportSummary:
    models = data.get("models") or []
    total = len(models)
    if total == 0:
        return ImportSummary(kind="model", total_in_json=0, inserted=0, updated=0, processed=0, skipped=0, errors=0)

    catalog_version = str(data.get("version", ""))
    generated_at_utc = data.get("generated_at_utc")
    catalog_source = str(data.get("source", ""))

    seen_keys = set()
    unique_rows = []
    for item in models:
        model_id = item.get("model_id")
        task_run_id = item.get("task_run_id")
        loop_id = item.get("loop_id")
        workspace_id = item.get("workspace_id")
        if not task_run_id or loop_id is None or not workspace_id:
            continue

        if not model_id:
            model_id = f"{task_run_id}_{loop_id}_{workspace_id}"
        
        if model_id in seen_keys:
            continue
        seen_keys.add(model_id)

        workspace_path = item.get("workspace_path")
        log_dir = item.get("log_dir")
        model_type = item.get("model_type")
        model_config = item.get("model_config")
        dataset_config = item.get("dataset_config")
        feature_schema = item.get("feature_schema")
        flattened_feature_list = item.get("flattened_feature_list")
        model_artifacts = item.get("model_artifacts")
        asset_bundle_id = item.get("asset_bundle_id")

        display_name = f"{_short_id(task_run_id)}-loop{int(loop_id)}-model-{_short_id(workspace_id)}"

        structured_keys = {
            "model_id",
            "task_run_id",
            "loop_id",
            "workspace_id",
            "workspace_path",
            "log_dir",
            "model_type",
            "model_config",
            "dataset_config",
            "feature_schema",
            "flattened_feature_list",
            "model_artifacts",
            "asset_bundle_id",
        }
        raw_payload_dict = {k: v for k, v in item.items() if k not in structured_keys}

        unique_rows.append(
            (
                catalog_version,
                generated_at_utc,
                catalog_source,
                model_id,
                task_run_id,
                int(loop_id),
                workspace_id,
                workspace_path,
                log_dir,
                display_name,
                model_type,
                json.dumps(model_config) if model_config is not None else None,
                json.dumps(dataset_config) if dataset_config is not None else None,
                json.dumps(feature_schema) if feature_schema is not None else None,
                json.dumps(flattened_feature_list) if flattened_feature_list is not None else None,
                json.dumps(model_artifacts) if model_artifacts is not None else None,
                asset_bundle_id,
                json.dumps(raw_payload_dict) if raw_payload_dict is not None else None,
            )
        )

    sql = """
        INSERT INTO aistock_model_catalog (
            catalog_version,
            generated_at_utc,
            catalog_source,
            model_id,
            task_run_id,
            loop_id,
            workspace_id,
            workspace_path,
            log_dir,
            display_name,
            model_type,
            model_config,
            dataset_config,
            feature_schema,
            flattened_feature_list,
            model_artifacts,
            asset_bundle_id,
            raw_payload
        )
        VALUES %s
        ON CONFLICT (model_id) DO UPDATE SET
            catalog_version = EXCLUDED.catalog_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            catalog_source = EXCLUDED.catalog_source,
            workspace_path = EXCLUDED.workspace_path,
            log_dir = EXCLUDED.log_dir,
            display_name = EXCLUDED.display_name,
            model_type = EXCLUDED.model_type,
            model_config = EXCLUDED.model_config,
            dataset_config = EXCLUDED.dataset_config,
            feature_schema = EXCLUDED.feature_schema,
            flattened_feature_list = EXCLUDED.flattened_feature_list,
            model_artifacts = EXCLUDED.model_artifacts,
            asset_bundle_id = EXCLUDED.asset_bundle_id,
            raw_payload = EXCLUDED.raw_payload
    """

    # 2026-01-05 Fix: 优化 ImportSummary 计数逻辑
    total_rows = len(unique_rows)
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql, unique_rows, page_size=500)
                processed = cur.rowcount if cur.rowcount > 0 else total_rows
            except Exception as e:
                print(f"[import_model_catalog_from_payload] failed to insert {total} rows: {e}")
                raise

    return ImportSummary(
        kind="model", 
        total_in_json=total, 
        inserted=processed, 
        updated=0, 
        processed=processed, 
        skipped=total - total_rows, 
        errors=0
    )



# --- 以下旧版 JSON 文件导入函数已删除 ---
# _json_root_from_env, import_*_from_json, truncate_all_catalogs, import_all_catalogs_from_root
# 因子入库现在由 rdagent_factor_catalog_sync.sync_factors_from_task 在 Task 同步后自动完成
