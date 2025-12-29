from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from psycopg2.extras import execute_values

from ..db.pg_pool import get_conn


JsonDict = Dict[str, Any]


@dataclass(frozen=True)
class ImportSummary:
    kind: Literal["factor", "strategy", "loop"]
    total_in_json: int
    inserted: int
    updated: int
    skipped: int
    errors: int


def _load_json(path: Path) -> JsonDict:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def _json_root_from_env() -> Path:
    explicit = (os.getenv("RDAGENT_AISTOCK_JSON_ROOT") or "").strip().strip("\"")
    if explicit:
        return Path(explicit)

    # fallback: RD-Agent-main/RDagentDB/aistock under repo root
    return Path(__file__).resolve().parents[3] / "RD-Agent-main" / "RDagentDB" / "aistock"


def import_factor_catalog_from_json(json_path: Optional[str] = None) -> ImportSummary:
    root = _json_root_from_env()
    path = Path(json_path) if json_path is not None else (root / "factor_catalog.json")
    data = _load_json(path)

    factors = data.get("factors") or []
    total = len(factors)
    if total == 0:
        return ImportSummary(kind="factor", total_in_json=0, inserted=0, updated=0, skipped=0, errors=0)

    catalog_version = str(data.get("version", ""))
    generated_at_utc = data.get("generated_at_utc")
    catalog_source = str(data.get("source", ""))

    rows: List[tuple] = []
    for item in factors:
        name = item.get("name")
        if not name:
            continue
        expression = item.get("expression")
        source = item.get("source")
        region = item.get("region")
        tags = item.get("tags")
        impl_module = item.get("impl_module")
        impl_func = item.get("impl_func")
        impl_version = item.get("impl_version")
        raw_payload = {k: v for k, v in item.items() if k != "name"}
        rows.append(
            (
                name,
                catalog_version,
                generated_at_utc,
                catalog_source,
                expression,
                source,
                region,
                json.dumps(tags) if tags is not None else None,
                impl_module,
                impl_func,
                impl_version,
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
            impl_module,
            impl_func,
            impl_version,
            raw_payload
        )
        VALUES %s
        ON CONFLICT (factor_name) DO UPDATE SET
            catalog_version = EXCLUDED.catalog_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            catalog_source = EXCLUDED.catalog_source,
            expression = EXCLUDED.expression,
            source = EXCLUDED.source,
            region = EXCLUDED.region,
            tags = EXCLUDED.tags,
            impl_module = EXCLUDED.impl_module,
            impl_func = EXCLUDED.impl_func,
            impl_version = EXCLUDED.impl_version,
            raw_payload = EXCLUDED.raw_payload
    """

    inserted = 0
    updated = 0
    errors = 0

    if not rows:
        return ImportSummary(kind="factor", total_in_json=total, inserted=0, updated=0, skipped=total, errors=0)

    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql, rows, page_size=1000)
            except Exception:
                errors = total
            else:
                # 无法区分 insert/update, 先全部计入 inserted
                inserted = total

    skipped = total - inserted
    return ImportSummary(kind="factor", total_in_json=total, inserted=inserted, updated=updated, skipped=skipped, errors=errors)


def import_strategy_catalog_from_json(json_path: Optional[str] = None) -> ImportSummary:
    root = _json_root_from_env()
    path = Path(json_path) if json_path is not None else (root / "strategy_catalog.json")
    data = _load_json(path)

    strategies = data.get("strategies") or []
    total = len(strategies)
    if total == 0:
        return ImportSummary(kind="strategy", total_in_json=0, inserted=0, updated=0, skipped=0, errors=0)

    catalog_version = str(data.get("version", ""))
    generated_at_utc = data.get("generated_at_utc")
    catalog_source = str(data.get("source", ""))

    rows: List[tuple] = []
    for item in strategies:
        strategy_id = item.get("strategy_id")
        if not strategy_id:
            continue
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

        rows.append(
            (
                strategy_id,
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
            )
        )

    sql = """
        INSERT INTO aistock_strategy_catalog (
            strategy_id,
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
            model_config
        )
        VALUES %s
        ON CONFLICT (strategy_id) DO UPDATE SET
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
            model_config = EXCLUDED.model_config
    """

    inserted = 0
    updated = 0
    errors = 0

    if not rows:
        return ImportSummary(kind="strategy", total_in_json=total, inserted=0, updated=0, skipped=total, errors=0)

    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql, rows, page_size=500)
            except Exception:
                errors = total
            else:
                inserted = total

    skipped = total - inserted
    return ImportSummary(kind="strategy", total_in_json=total, inserted=inserted, updated=updated, skipped=skipped, errors=errors)


def import_loop_catalog_from_json(json_path: Optional[str] = None) -> ImportSummary:
    root = _json_root_from_env()
    path = Path(json_path) if json_path is not None else (root / "loop_catalog.json")
    data = _load_json(path)

    loops = data.get("loops") or []
    total = len(loops)
    if total == 0:
        return ImportSummary(kind="loop", total_in_json=0, inserted=0, updated=0, skipped=0, errors=0)

    catalog_version = str(data.get("version", ""))
    generated_at_utc = data.get("generated_at_utc")
    catalog_source = str(data.get("source", ""))

    rows: List[tuple] = []
    for item in loops:
        task_run_id = item.get("task_run_id")
        loop_id = item.get("loop_id")
        if not task_run_id or loop_id is None:
            continue

        workspace_id = item.get("workspace_id")
        scenario = item.get("scenario")
        step_name = item.get("step_name")
        action = item.get("action")
        status = item.get("status")
        has_result = item.get("has_result")
        strategy_id = item.get("strategy_id")
        factor_names = item.get("factor_names")
        metrics = item.get("metrics")
        decision = item.get("decision")

        summary_texts = item.get("summary_texts") or {}
        summary_execution = summary_texts.get("execution")
        summary_value_feedback = summary_texts.get("value_feedback")
        summary_shape_feedback = summary_texts.get("shape_feedback")

        paths = item.get("paths") or {}
        path_factor_meta = paths.get("factor_meta")
        path_factor_perf = paths.get("factor_perf")
        path_feedback = paths.get("feedback")
        path_ret_curve = paths.get("ret_curve")
        path_dd_curve = paths.get("dd_curve")

        rows.append(
            (
                catalog_version,
                generated_at_utc,
                catalog_source,
                task_run_id,
                int(loop_id),
                workspace_id,
                scenario,
                step_name,
                action,
                status,
                bool(has_result) if has_result is not None else None,
                strategy_id,
                json.dumps(factor_names) if factor_names is not None else None,
                json.dumps(metrics) if metrics is not None else None,
                decision,
                summary_execution,
                summary_value_feedback,
                summary_shape_feedback,
                path_factor_meta,
                path_factor_perf,
                path_feedback,
                path_ret_curve,
                path_dd_curve,
            )
        )

    sql = """
        INSERT INTO aistock_loop_catalog (
            catalog_version,
            generated_at_utc,
            catalog_source,
            task_run_id,
            loop_id,
            workspace_id,
            scenario,
            step_name,
            action,
            status,
            has_result,
            strategy_id,
            factor_names,
            metrics,
            decision,
            summary_execution,
            summary_value_feedback,
            summary_shape_feedback,
            path_factor_meta,
            path_factor_perf,
            path_feedback,
            path_ret_curve,
            path_dd_curve
        )
        VALUES %s
        ON CONFLICT (task_run_id, loop_id, workspace_id) DO UPDATE SET
            catalog_version = EXCLUDED.catalog_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            catalog_source = EXCLUDED.catalog_source,
            workspace_id = EXCLUDED.workspace_id,
            scenario = EXCLUDED.scenario,
            step_name = EXCLUDED.step_name,
            action = EXCLUDED.action,
            status = EXCLUDED.status,
            has_result = EXCLUDED.has_result,
            strategy_id = EXCLUDED.strategy_id,
            factor_names = EXCLUDED.factor_names,
            metrics = EXCLUDED.metrics,
            decision = EXCLUDED.decision,
            summary_execution = EXCLUDED.summary_execution,
            summary_value_feedback = EXCLUDED.summary_value_feedback,
            summary_shape_feedback = EXCLUDED.summary_shape_feedback,
            path_factor_meta = EXCLUDED.path_factor_meta,
            path_factor_perf = EXCLUDED.path_factor_perf,
            path_feedback = EXCLUDED.path_feedback,
            path_ret_curve = EXCLUDED.path_ret_curve,
            path_dd_curve = EXCLUDED.path_dd_curve
    """

    inserted = 0
    updated = 0
    errors = 0

    if not rows:
        return ImportSummary(kind="loop", total_in_json=total, inserted=0, updated=0, skipped=total, errors=0)

    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql, rows, page_size=500)
            except Exception as e:
                # 将错误输出到标准输出/日志, 便于排查问题
                print(f"[import_loop_catalog_from_json] failed to insert {total} rows: {e}")
                errors = total
                # 直接抛出, 让上层看到 500 错误而不是静默失败
                raise
            else:
                inserted = total

    skipped = total - inserted
    return ImportSummary(kind="loop", total_in_json=total, inserted=inserted, updated=updated, skipped=skipped, errors=errors)


def import_all_catalogs_from_root(root_dir: Optional[str] = None) -> Dict[str, ImportSummary]:
    root = Path(root_dir) if root_dir is not None else _json_root_from_env()
    results: Dict[str, ImportSummary] = {}
    results["factor"] = import_factor_catalog_from_json(str(root / "factor_catalog.json"))
    results["strategy"] = import_strategy_catalog_from_json(str(root / "strategy_catalog.json"))
    results["loop"] = import_loop_catalog_from_json(str(root / "loop_catalog.json"))
    return results
