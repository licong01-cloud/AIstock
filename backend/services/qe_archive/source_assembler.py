"""Assemble QE archive payloads from existing AIstock database rows.

The assembler is read-only against existing QE tables. It does not inspect
or open worker-side artifacts; artifact sync/parsing is a later explicit phase.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from typing import Any

from backend.db.pg_pool import get_conn


ConnectionProvider = Callable[[], Any]

EXPERIMENT_COLUMNS = (
    "experiment_id",
    "task_id",
    "experiment_name",
    "status",
    "factor_names",
    "model_id",
    "strategy_id",
    "data_split",
    "custom_params",
    "result_metrics",
    "result_files",
    "qe_task_id",
    "qe_loop_id",
    "loop_index",
    "parent_experiment_id",
    "is_evolution_loop",
    "ic",
    "icir",
    "rank_ic",
    "rank_icir",
    "annualized_return",
    "max_drawdown",
    "information_ratio",
    "excess_return_with_cost_mean",
    "excess_return_without_cost_mean",
    "annualized_return_no_cost",
    "max_drawdown_no_cost",
    "information_ratio_no_cost",
    "model_catalog_id",
    "created_at",
    "started_at",
    "completed_at",
    "updated_at",
    "alpha_mode",
    "multi_alpha_config",
)

LOOP_COLUMNS = (
    "loop_id",
    "task_id",
    "loop_index",
    "action_type",
    "config_json",
    "metrics_json",
    "agent_analysis",
    "is_sota",
    "status",
    "node_id",
    "experiment_id",
    "created_at",
    "updated_at",
)

TASK_COLUMNS = (
    "task_id",
    "task_name",
    "target_desc",
    "max_loops",
    "current_loop",
    "status",
    "base_experiment_id",
    "node_id",
    "label_horizon",
    "created_at",
    "updated_at",
    "evolution_mode",
    "model_id",
    "model_catalog_id",
    "strategy_id",
    "base_factor_names",
    "factor_blacklist",
)


class QEArchiveSourceAssembler:
    """Build archive-service payloads from existing QE DB records."""

    def __init__(self, connection_provider: ConnectionProvider = get_conn) -> None:
        self._connection_provider = connection_provider

    def list_experiment_ids(
        self,
        *,
        status: str = "completed",
        limit: int = 20,
    ) -> list[str]:
        limit = max(1, min(int(limit), 500))
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                order_col = self._preferred_existing_column(cur, "qe_experiments", ("completed_at", "updated_at", "created_at"))
                cur.execute(
                    f"""
                    SELECT experiment_id
                    FROM qe_experiments
                    WHERE status = %s
                    ORDER BY {order_col} DESC NULLS LAST
                    LIMIT %s
                    """,
                    (status, limit),
                )
                return [str(row[0]) for row in cur.fetchall()]

    def list_loop_refs(
        self,
        *,
        status: str = "completed",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                order_col = self._preferred_existing_column(cur, "qe_evolution_loops", ("updated_at", "created_at"))
                cur.execute(
                    f"""
                    SELECT task_id, loop_id, loop_index
                    FROM qe_evolution_loops
                    WHERE status = %s
                    ORDER BY {order_col} DESC NULLS LAST
                    LIMIT %s
                    """,
                    (status, limit),
                )
                return [
                    {"task_id": row[0], "loop_id": row[1], "loop_index": row[2]}
                    for row in cur.fetchall()
                ]

    def assemble_experiment_payload(self, experiment_id: str) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                available = self._available_columns(cur, "qe_experiments")
                columns = [col for col in EXPERIMENT_COLUMNS if col in available]
                if "experiment_id" not in columns:
                    raise ValueError("qe_experiments.experiment_id is required for archive assembly")
                cur.execute(
                    f"""
                    SELECT {", ".join(columns)}
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (experiment_id,),
                )
                row = cur.fetchone()
        if not row:
            raise ValueError(f"QE experiment not found: {experiment_id}")
        return self.build_experiment_payload(dict(zip(columns, row)))

    def assemble_loop_payload(
        self,
        *,
        loop_id: str | None = None,
        task_id: str | None = None,
        loop_index: int | None = None,
    ) -> dict[str, Any]:
        if not loop_id and not (task_id and loop_index is not None):
            raise ValueError("loop_id or task_id+loop_index is required")

        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                loop_available = self._available_columns(cur, "qe_evolution_loops")
                task_available = self._available_columns(cur, "qe_evolution_tasks")
                loop_cols = [col for col in LOOP_COLUMNS if col in loop_available]
                task_cols = [col for col in TASK_COLUMNS if col in task_available]
                select_cols = [f"l.{col} AS loop__{col}" for col in loop_cols]
                select_cols.extend(f"t.{col} AS task__{col}" for col in task_cols)
                where_sql = "l.loop_id = %s" if loop_id else "l.task_id = %s AND l.loop_index = %s"
                params: tuple[Any, ...] = (loop_id,) if loop_id else (task_id, loop_index)
                cur.execute(
                    f"""
                    SELECT {", ".join(select_cols)}
                    FROM qe_evolution_loops l
                    LEFT JOIN qe_evolution_tasks t ON t.task_id = l.task_id
                    WHERE {where_sql}
                    """,
                    params,
                )
                row = cur.fetchone()
                descriptions = [desc[0] for desc in cur.description or []]
        if not row:
            key = loop_id or f"{task_id}/Loop{loop_index}"
            raise ValueError(f"QE evolution loop not found: {key}")

        joined = dict(zip(descriptions, row))
        loop_row = {key.removeprefix("loop__"): value for key, value in joined.items() if key.startswith("loop__")}
        task_row = {key.removeprefix("task__"): value for key, value in joined.items() if key.startswith("task__")}
        return self.build_loop_payload(loop_row, task_row)

    @staticmethod
    def build_experiment_payload(row: Mapping[str, Any]) -> dict[str, Any]:
        row = dict(row)
        metrics = _ensure_mapping(row.get("result_metrics"))
        custom_params = _ensure_mapping(row.get("custom_params"))
        data_split = _ensure_mapping(row.get("data_split"))
        factor_names = _ensure_list(row.get("factor_names"))
        freq = _infer_freq(custom_params, _ensure_mapping(row.get("result_files")))
        limit_suspend_authoritative = _infer_limit_suspend_authoritative(custom_params)

        metric_overrides = {
            "IC": row.get("ic"),
            "ICIR": row.get("icir"),
            "Rank IC": row.get("rank_ic"),
            "Rank ICIR": row.get("rank_icir"),
            "1day.excess_return_with_cost.annualized_return": row.get("annualized_return"),
            "1day.excess_return_with_cost.max_drawdown": row.get("max_drawdown"),
            "1day.excess_return_with_cost.information_ratio": row.get("information_ratio"),
            "1day.excess_return_with_cost.mean": row.get("excess_return_with_cost_mean"),
            "1day.excess_return_without_cost.mean": row.get("excess_return_without_cost_mean"),
            "1day.excess_return_without_cost.annualized_return": row.get("annualized_return_no_cost"),
            "1day.excess_return_without_cost.max_drawdown": row.get("max_drawdown_no_cost"),
            "1day.excess_return_without_cost.information_ratio": row.get("information_ratio_no_cost"),
        }
        for key, value in metric_overrides.items():
            if value is not None and metrics.get(key) is None:
                metrics[key] = value

        config = {
            "factor_list": factor_names,
            "model": {
                "model_id": row.get("model_id"),
                "model_catalog_id": row.get("model_catalog_id"),
            },
            "strategy": {"strategy_id": row.get("strategy_id")},
            "data_split": data_split,
            "runtime_flags": custom_params,
            "execution": _execution_context(custom_params),
            "data_context": {
                "freq": freq,
                "label_horizon": _first_present(custom_params, ("label_horizon",)),
                "limit_suspend_authoritative": limit_suspend_authoritative,
                "limit_handling": "authoritative" if limit_suspend_authoritative else "unknown",
                "suspend_handling": "authoritative" if limit_suspend_authoritative else "unknown",
                "data_quality_flags": {
                    "source": "qe_experiments",
                    "freq_inferred": "freq" not in custom_params and "backtest_freq" not in custom_params,
                    "worker_artifact_paths_omitted": True,
                },
            },
        }

        return {
            "source_system": "qe",
            "source_id": row.get("experiment_id"),
            "source_sub_id": row.get("qe_loop_id"),
            "logical_experiment_id": row.get("experiment_id"),
            "experiment_id": row.get("experiment_id"),
            "task_id": row.get("qe_task_id") or row.get("task_id"),
            "loop_id": row.get("qe_loop_id"),
            "loop_index": row.get("loop_index"),
            "run_type": "evolution_loop" if row.get("is_evolution_loop") else "single_experiment",
            "status": row.get("status") or "completed",
            "model_type": row.get("model_id"),
            "model_catalog_id": row.get("model_catalog_id"),
            "factor_list": factor_names,
            "freq": freq,
            "limit_suspend_authoritative": limit_suspend_authoritative,
            "config": config,
            "raw_config": {
                "experiment_name": row.get("experiment_name"),
                "data_split": data_split,
                "custom_params": custom_params,
                "alpha_mode": row.get("alpha_mode"),
                "multi_alpha_config": _ensure_mapping(row.get("multi_alpha_config")),
            },
            "metrics": metrics,
            "source_created_at": _jsonable(row.get("created_at")),
            "started_at": _jsonable(row.get("started_at")),
            "completed_at": _jsonable(row.get("completed_at")),
            "source_updated_at": _jsonable(row.get("updated_at")),
            "source_config_paths": {"worker_artifact_paths_omitted": True},
        }

    @staticmethod
    def build_loop_payload(loop_row: Mapping[str, Any], task_row: Mapping[str, Any] | None = None) -> dict[str, Any]:
        loop = dict(loop_row)
        task = dict(task_row or {})
        config_json = _ensure_mapping(loop.get("config_json"))
        metrics = _ensure_mapping(loop.get("metrics_json"))
        task_id = loop.get("task_id") or task.get("task_id")
        loop_id = loop.get("loop_id")
        runtime_flags = _ensure_mapping(config_json.get("runtime_flags") or config_json.get("custom_params") or {})
        factor_names = _extract_factors_from_config(config_json)
        if not factor_names:
            factor_names = _ensure_list(task.get("base_factor_names"))
        freq = _infer_freq(runtime_flags, config_json)
        label_horizon = _first_present(
            runtime_flags,
            ("label_horizon",),
            default=task.get("label_horizon"),
        )
        limit_suspend_authoritative = _infer_limit_suspend_authoritative(runtime_flags)

        merged_config = dict(config_json)
        merged_config.setdefault("factor_list", factor_names)
        merged_config.setdefault("runtime_flags", runtime_flags)
        merged_config.setdefault("data_context", {})
        data_context = _ensure_mapping(merged_config["data_context"])
        data_context.setdefault("freq", freq)
        data_context.setdefault("label_horizon", label_horizon)
        data_context.setdefault("limit_suspend_authoritative", limit_suspend_authoritative)
        data_context.setdefault("limit_handling", "authoritative" if limit_suspend_authoritative else "unknown")
        data_context.setdefault("suspend_handling", "authoritative" if limit_suspend_authoritative else "unknown")
        data_context.setdefault(
            "data_quality_flags",
            {
                "source": "qe_evolution_loops",
                "freq_inferred": "freq" not in runtime_flags and "backtest_freq" not in runtime_flags,
                "worker_artifact_paths_omitted": True,
            },
        )
        merged_config["data_context"] = data_context

        return {
            "source_system": "qe_evolution",
            "source_id": task_id,
            "source_sub_id": loop_id,
            "logical_experiment_id": f"{task_id}:{loop_id}",
            "experiment_id": loop.get("experiment_id"),
            "task_id": task_id,
            "loop_id": loop_id,
            "loop_index": loop.get("loop_index"),
            "run_type": "evolution_loop",
            "status": loop.get("status") or "completed",
            "node_id": loop.get("node_id") or task.get("node_id"),
            "model_type": _first_present(config_json, ("model_type", "model_id"), default=task.get("model_id")),
            "model_catalog_id": task.get("model_catalog_id"),
            "factor_list": factor_names,
            "freq": freq,
            "limit_suspend_authoritative": limit_suspend_authoritative,
            "config": merged_config,
            "raw_config": {
                "task": {key: _jsonable(value) for key, value in task.items()},
                "loop_config_json": config_json,
                "agent_analysis": _ensure_mapping(loop.get("agent_analysis")),
                "action_type": loop.get("action_type"),
                "is_sota": loop.get("is_sota"),
            },
            "metrics": metrics,
            "source_created_at": _jsonable(loop.get("created_at")),
            "source_updated_at": _jsonable(loop.get("updated_at")),
            "completed_at": _jsonable(loop.get("updated_at")),
            "source_config_paths": {"worker_artifact_paths_omitted": True},
        }

    @staticmethod
    def _available_columns(cur: Any, table_name: str) -> set[str]:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        return {str(row[0]) for row in cur.fetchall()}

    def _preferred_existing_column(self, cur: Any, table_name: str, candidates: Sequence[str]) -> str:
        available = self._available_columns(cur, table_name)
        for candidate in candidates:
            if candidate in available:
                return candidate
        return candidates[-1]


def _ensure_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


def _ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        return _ensure_list(parsed)
    if isinstance(value, Mapping):
        return list(value.values())
    return [value]


def _extract_factors_from_config(config: Mapping[str, Any]) -> list[Any]:
    for key in ("factor_list", "factors", "factor_names", "features", "feature_names"):
        factors = _ensure_list(config.get(key))
        if factors:
            return factors
    for nested_key in ("model", "dataset", "data_handler_config", "handler"):
        nested = _ensure_mapping(config.get(nested_key))
        for key in ("factor_list", "factors", "features", "feature_names"):
            factors = _ensure_list(nested.get(key))
            if factors:
                return factors
    return []


def _execution_context(params: Mapping[str, Any]) -> dict[str, Any]:
    execution_algo = params.get("execution_algo")
    execution_algo_params = _ensure_mapping(params.get("execution_algo_params"))
    return {
        "execution_algo": execution_algo,
        "execution_algo_params": execution_algo_params,
        "filter_suspended_on_signal": params.get("filter_suspended_on_signal"),
        "suspend_filter_strict": params.get("suspend_filter_strict"),
        "unfilled_handler": params.get("unfilled_handler"),
        "limit_suspend_authoritative": _infer_limit_suspend_authoritative(params),
    }


def _infer_freq(params: Mapping[str, Any], context: Mapping[str, Any]) -> str:
    explicit = (
        params.get("backtest_freq")
        or params.get("freq")
        or params.get("qlib_freq")
        or context.get("freq")
        or context.get("backtest_freq")
    )
    if explicit:
        return str(explicit)
    algo = str(params.get("execution_algo") or context.get("execution_algo") or "").lower()
    if any(token in algo for token in ("minute", "v24", "v25", "v26", "two_stage")):
        return "1min"
    return "day"


def _infer_limit_suspend_authoritative(params: Mapping[str, Any]) -> bool:
    explicit = _as_bool(params.get("limit_suspend_authoritative"))
    if explicit is not None:
        return explicit
    algo = str(params.get("execution_algo") or "").lower()
    if any(token in algo for token in ("v24", "v25", "v26", "two_stage")):
        return True
    return False


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _first_present(mapping: Mapping[str, Any], keys: Sequence[str], *, default: Any = None) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", [], {}):
            return value
    return default


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value
