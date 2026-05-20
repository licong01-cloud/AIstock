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
from backend.services.quantevolver.runtime_contract import (
    merge_qe_minute_runtime_contract,
    runtime_contract_missing,
)

from .policy import resolve_archive_policy


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

TERMINAL_STATUSES = ("completed", "failed", "interrupted", "cancelled")


class QEArchiveSourceAssembler:
    """Build archive-service payloads from existing QE DB records."""

    def __init__(self, connection_provider: ConnectionProvider = get_conn) -> None:
        self._connection_provider = connection_provider

    def list_experiment_ids(
        self,
        *,
        status: str = "completed",
        limit: int = 20,
        include_archived: bool = False,
    ) -> list[str]:
        limit = max(1, min(int(limit), 500))
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                order_col = self._preferred_existing_column(cur, "qe_experiments", ("completed_at", "updated_at", "created_at"))
                archive_filter = (
                    ""
                    if include_archived
                    else "AND NOT EXISTS (SELECT 1 FROM qe_archive.run r WHERE r.experiment_id = e.experiment_id)"
                )
                status_filter, status_params = _status_filter_sql("e.status", status)
                cur.execute(
                    f"""
                    SELECT e.experiment_id
                    FROM qe_experiments e
                    WHERE TRUE
                      {status_filter}
                      {archive_filter}
                    ORDER BY e.{order_col} DESC NULLS LAST
                    LIMIT %s
                    """,
                    [*status_params, limit],
                )
                return [str(row[0]) for row in cur.fetchall()]

    def list_loop_refs(
        self,
        *,
        status: str = "completed",
        limit: int = 20,
        include_archived: bool = False,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                order_col = self._preferred_existing_column(cur, "qe_evolution_loops", ("updated_at", "created_at"))
                archive_filter = (
                    ""
                    if include_archived
                    else """
                      AND NOT EXISTS (
                          SELECT 1
                          FROM qe_archive.run r
                          WHERE r.task_id = l.task_id
                            AND r.loop_id = l.loop_id
                      )
                    """
                )
                status_filter, status_params = _status_filter_sql("l.status", status)
                cur.execute(
                    f"""
                    SELECT l.task_id, l.loop_id, l.loop_index
                    FROM qe_evolution_loops l
                    WHERE TRUE
                      {status_filter}
                      {archive_filter}
                    ORDER BY l.{order_col} DESC NULLS LAST
                    LIMIT %s
                    """,
                    [*status_params, limit],
                )
                return [
                    {"task_id": row[0], "loop_id": row[1], "loop_index": row[2]}
                    for row in cur.fetchall()
                ]

    def list_loop_refs_for_tasks(
        self,
        task_ids: Sequence[str],
        *,
        status: str = "completed",
        include_archived: bool = False,
    ) -> list[dict[str, Any]]:
        task_ids = _dedupe_non_empty(task_ids)
        if not task_ids:
            return []

        status_filter, params = _status_filter_sql("l.status", status)
        archive_filter = (
            ""
            if include_archived
            else """
                      AND NOT EXISTS (
                          SELECT 1
                          FROM qe_archive.run r
                          WHERE r.task_id = l.task_id
                            AND r.loop_id = l.loop_id
                      )
            """
        )
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT l.task_id, l.loop_id, l.loop_index
                    FROM qe_evolution_loops l
                    WHERE l.task_id = ANY(%s)
                      {status_filter}
                      {archive_filter}
                    ORDER BY l.task_id ASC, l.loop_index ASC NULLS LAST, l.updated_at ASC NULLS LAST
                    """,
                    [task_ids, *params],
                )
                return [
                    {"task_id": row[0], "loop_id": row[1], "loop_index": row[2]}
                    for row in cur.fetchall()
                ]

    def list_loop_refs_for_task_indices(
        self,
        task_id: str,
        loop_indices: Sequence[int],
        *,
        status: str = "completed",
        include_archived: bool = False,
    ) -> list[dict[str, Any]]:
        task_id = str(task_id or "").strip()
        indices = _dedupe_positive_ints(loop_indices)
        if not task_id or not indices:
            return []

        status_filter, params = _status_filter_sql("l.status", status)
        archive_filter = (
            ""
            if include_archived
            else """
                      AND NOT EXISTS (
                          SELECT 1
                          FROM qe_archive.run r
                          WHERE r.task_id = l.task_id
                            AND r.loop_id = l.loop_id
                      )
            """
        )
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT l.task_id, l.loop_id, l.loop_index
                    FROM qe_evolution_loops l
                    WHERE l.task_id = %s
                      AND l.loop_index = ANY(%s)
                      {status_filter}
                      {archive_filter}
                    ORDER BY l.loop_index ASC NULLS LAST, l.updated_at ASC NULLS LAST
                    """,
                    [task_id, indices, *params],
                )
                return [
                    {"task_id": row[0], "loop_id": row[1], "loop_index": row[2]}
                    for row in cur.fetchall()
                ]

    def get_source_archive_status(
        self,
        *,
        experiment_ids: Sequence[str] = (),
        task_ids: Sequence[str] = (),
        loop_ids: Sequence[str] = (),
        include_recommendation: bool = True,
    ) -> dict[str, Any]:
        experiments = self._experiment_archive_status(_dedupe_non_empty(experiment_ids))
        tasks = self._task_archive_status(_dedupe_non_empty(task_ids))
        loops = self._loop_archive_status(_dedupe_non_empty(loop_ids))
        return {
            "experiments": experiments,
            "tasks": tasks,
            "loops": loops,
            "include_recommendation": bool(include_recommendation),
        }

    def list_backfill_candidates(
        self,
        *,
        status: str = "completed",
        limit: int = 100,
        offset: int = 0,
        include_archived: bool = False,
    ) -> list[dict[str, Any]]:
        """List QE source experiments/tasks with archive coverage for UI selection."""

        limit = max(1, min(int(limit or 100), 500))
        offset = max(0, int(offset or 0))
        fetch_limit = max(limit, min(offset + limit, 500))
        candidates = self._list_evolution_task_candidates(
            status=status,
            limit=fetch_limit,
            include_archived=include_archived,
        )
        candidates.extend(
            self._list_single_experiment_candidates(
                status=status,
                limit=fetch_limit,
                include_archived=include_archived,
            )
        )
        sorted_candidates = sorted(
            candidates,
            key=lambda row: str(row.get("sort_time") or ""),
            reverse=True,
        )
        return sorted_candidates[offset:offset + limit]

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

    def _experiment_archive_status(self, experiment_ids: Sequence[str]) -> dict[str, Any]:
        result = {
            experiment_id: {
                "archive_status": "not_recommended",
                "run_ids": [],
                "run_count": 0,
                "eligible": False,
                "recommended": False,
                "reason": "source_not_found",
            }
            for experiment_id in experiment_ids
        }
        if not experiment_ids:
            return result
        archived: dict[str, list[str]] = {}
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT experiment_id, array_agg(run_id ORDER BY archived_at DESC NULLS LAST) AS run_ids
                    FROM qe_archive.run
                    WHERE experiment_id = ANY(%s)
                    GROUP BY experiment_id
                    """,
                    (list(experiment_ids),),
                )
                for experiment_id, run_ids in cur.fetchall():
                    archived[str(experiment_id)] = [str(item) for item in (run_ids or [])]

                available = self._available_columns(cur, "qe_experiments")
                columns = [col for col in EXPERIMENT_COLUMNS if col in available]
                if "experiment_id" not in columns:
                    return result
                cur.execute(
                    f"""
                    SELECT {", ".join(columns)}
                    FROM qe_experiments
                    WHERE experiment_id = ANY(%s)
                    """,
                    (list(experiment_ids),),
                )
                for row in cur.fetchall():
                    source_row = dict(zip(columns, row))
                    experiment_id = str(source_row.get("experiment_id"))
                    run_ids = archived.get(experiment_id, [])
                    payload = self.build_experiment_payload(source_row) if not run_ids else {}
                    result[experiment_id] = _archive_status_from_policy(
                        source_type="experiment",
                        source_id=experiment_id,
                        source_sub_id=None,
                        source_status=str(source_row.get("status") or ""),
                        payload=payload,
                        run_ids=run_ids,
                    )
        for experiment_id, run_ids in archived.items():
            if run_ids and experiment_id in result and result[experiment_id].get("archive_status") != "archived":
                result[experiment_id] = _archived_status(run_ids)
        return result

    def _loop_archive_status(self, loop_ids: Sequence[str]) -> dict[str, Any]:
        result = {
            loop_id: {
                "archive_status": "not_recommended",
                "run_ids": [],
                "run_count": 0,
                "eligible": False,
                "recommended": False,
                "reason": "source_not_found",
            }
            for loop_id in loop_ids
        }
        if not loop_ids:
            return result
        archived: dict[str, list[str]] = {}
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT loop_id, array_agg(run_id ORDER BY archived_at DESC NULLS LAST) AS run_ids
                    FROM qe_archive.run
                    WHERE loop_id = ANY(%s)
                    GROUP BY loop_id
                    """,
                    (list(loop_ids),),
                )
                for loop_id, run_ids in cur.fetchall():
                    archived[str(loop_id)] = [str(item) for item in (run_ids or [])]

                loop_available = self._available_columns(cur, "qe_evolution_loops")
                task_available = self._available_columns(cur, "qe_evolution_tasks")
                loop_cols = [col for col in LOOP_COLUMNS if col in loop_available]
                task_cols = [col for col in TASK_COLUMNS if col in task_available]
                if "loop_id" not in loop_cols:
                    return result
                select_cols = [f"l.{col} AS loop__{col}" for col in loop_cols]
                select_cols.extend(f"t.{col} AS task__{col}" for col in task_cols)
                cur.execute(
                    f"""
                    SELECT {", ".join(select_cols)}
                    FROM qe_evolution_loops l
                    LEFT JOIN qe_evolution_tasks t ON t.task_id = l.task_id
                    WHERE l.loop_id = ANY(%s)
                    """,
                    (list(loop_ids),),
                )
                descriptions = [desc[0] for desc in cur.description or []]
                for row in cur.fetchall():
                    joined = dict(zip(descriptions, row))
                    loop_row = {key.removeprefix("loop__"): value for key, value in joined.items() if key.startswith("loop__")}
                    task_row = {key.removeprefix("task__"): value for key, value in joined.items() if key.startswith("task__")}
                    loop_id = str(loop_row.get("loop_id"))
                    run_ids = archived.get(loop_id, [])
                    payload = self.build_loop_payload(loop_row, task_row) if not run_ids else {}
                    result[loop_id] = _archive_status_from_policy(
                        source_type="loop",
                        source_id=str(loop_row.get("task_id") or loop_id),
                        source_sub_id=loop_id,
                        source_status=str(loop_row.get("status") or ""),
                        payload=payload,
                        run_ids=run_ids,
                    )
        for loop_id, run_ids in archived.items():
            if run_ids and loop_id in result and result[loop_id].get("archive_status") != "archived":
                result[loop_id] = _archived_status(run_ids)
        return result

    def _task_archive_status(self, task_ids: Sequence[str]) -> dict[str, Any]:
        result = {
            task_id: {
                "archive_status": "not_archived",
                "loop_count": 0,
                "archived_loop_count": 0,
                "eligible_loop_count": 0,
                "pending_loop_count": 0,
                "recommended_loop_count": 0,
                "manual_only_loop_count": 0,
                "not_recommended_loop_count": 0,
                "run_ids": [],
            }
            for task_id in task_ids
        }
        if not task_ids:
            return result
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        l.task_id,
                        l.loop_id,
                        l.loop_index,
                        l.status,
                        l.config_json,
                        array_remove(array_agg(DISTINCT r.run_id), NULL) AS run_ids
                    FROM qe_evolution_loops l
                    LEFT JOIN qe_archive.run r
                      ON r.task_id = l.task_id
                     AND r.loop_id = l.loop_id
                    WHERE l.task_id = ANY(%s)
                    GROUP BY l.task_id, l.loop_id, l.loop_index, l.status, l.config_json
                    ORDER BY l.task_id ASC, l.loop_index ASC NULLS LAST
                    """,
                    (list(task_ids),),
                )
                rows = self._fetch_dicts(cur)

        grouped: dict[str, list[dict[str, Any]]] = {task_id: [] for task_id in task_ids}
        for row in rows:
            grouped.setdefault(str(row.get("task_id")), []).append(row)

        for task_id, task_loops in grouped.items():
            loop_count = len(task_loops)
            completed = 0
            archived = 0
            eligible = 0
            recommended = 0
            manual_only = 0
            not_recommended = 0
            run_ids: list[str] = []
            for row in task_loops:
                row_run_ids = [str(item) for item in (row.get("run_ids") or [])]
                run_ids.extend(row_run_ids)
                if str(row.get("status") or "").lower() == "completed":
                    completed += 1
                loop_status = _loop_row_archive_status(row, run_ids=row_run_ids)
                if loop_status["archive_status"] == "archived":
                    archived += 1
                    continue
                if loop_status.get("eligible"):
                    eligible += 1
                if loop_status.get("recommended"):
                    recommended += 1
                if loop_status["archive_status"] == "manual_only":
                    manual_only += 1
                if loop_status["archive_status"] in {"not_recommended", "skipped"}:
                    not_recommended += 1
            pending = max(0, completed - archived)
            if completed and archived >= completed:
                status = "fully_archived"
            elif archived:
                status = "partially_archived"
            elif recommended:
                status = "recommended"
            elif eligible:
                status = "eligible"
            elif loop_count:
                status = "not_recommended"
            else:
                status = "not_archived"
            result[task_id] = {
                "archive_status": status,
                "loop_count": loop_count,
                "archived_loop_count": archived,
                "eligible_loop_count": completed,
                "pending_loop_count": pending,
                "recommended_loop_count": recommended,
                "manual_only_loop_count": manual_only,
                "not_recommended_loop_count": not_recommended,
                "run_ids": sorted(set(run_ids)),
            }
        return result

    def _list_task_loop_items(
        self,
        task_ids: Sequence[str],
    ) -> dict[str, list[dict[str, Any]]]:
        task_ids = _dedupe_non_empty(task_ids)
        if not task_ids:
            return {}
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        l.task_id,
                        l.loop_id,
                        l.loop_index,
                        l.status,
                        l.action_type,
                        l.experiment_id,
                        l.is_sota,
                        l.metrics_json,
                        l.config_json,
                        l.created_at,
                        l.updated_at,
                        array_remove(array_agg(DISTINCT r.run_id), NULL) AS run_ids
                    FROM qe_evolution_loops l
                    LEFT JOIN qe_archive.run r
                      ON r.task_id = l.task_id
                     AND r.loop_id = l.loop_id
                    WHERE l.task_id = ANY(%s)
                    GROUP BY
                        l.task_id,
                        l.loop_id,
                        l.loop_index,
                        l.status,
                        l.action_type,
                        l.experiment_id,
                        l.is_sota,
                        l.metrics_json,
                        l.config_json,
                        l.created_at,
                        l.updated_at
                    ORDER BY l.task_id ASC, l.loop_index ASC NULLS LAST, l.updated_at ASC NULLS LAST
                    """,
                    (task_ids,),
                )
                rows = self._fetch_dicts(cur)

        grouped: dict[str, list[dict[str, Any]]] = {task_id: [] for task_id in task_ids}
        for row in rows:
            status = _loop_row_archive_status(row, run_ids=[str(item) for item in (row.get("run_ids") or [])])
            item = {
                "task_id": row.get("task_id"),
                "loop_id": row.get("loop_id"),
                "loop_index": row.get("loop_index"),
                "status": row.get("status"),
                "action_type": row.get("action_type"),
                "experiment_id": row.get("experiment_id"),
                "is_sota": bool(row.get("is_sota")),
                "archive_status": status["archive_status"],
                "eligible": status["eligible"],
                "recommended": status["recommended"],
                "reason": status["reason"],
                "run_ids": status["run_ids"],
                "run_count": status["run_count"],
                "created_at": _jsonable(row.get("created_at")),
                "updated_at": _jsonable(row.get("updated_at")),
            }
            metrics = _ensure_mapping(row.get("metrics_json"))
            for key in ("IC", "ic", "Rank_IC", "rank_ic", "annualized_return", "max_drawdown", "information_ratio"):
                if key in metrics:
                    item[key] = metrics.get(key)
            grouped.setdefault(str(row.get("task_id")), []).append(item)
        return grouped

    def _list_evolution_task_candidates(
        self,
        *,
        status: str,
        limit: int,
        include_archived: bool,
    ) -> list[dict[str, Any]]:
        status_filter, status_params = _status_filter_sql("l.status", status)
        having_filter = "" if include_archived else "AND COUNT(DISTINCT l.loop_id) > COUNT(DISTINCT r.loop_id)"
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                task_available = self._available_columns(cur, "qe_evolution_tasks")
                optional_cols = ("evolution_mode", "task_type", "node_id", "label_horizon", "model_id", "model_catalog_id", "strategy_id")
                optional_select = [
                    f"t.{col}" if col in task_available else f"NULL AS {col}"
                    for col in optional_cols
                ]
                group_cols = [
                    "t.task_id",
                    "t.task_name",
                    "t.target_desc",
                    "t.status",
                    "t.max_loops",
                    "t.current_loop",
                    "t.created_at",
                    "t.updated_at",
                    *(f"t.{col}" for col in optional_cols if col in task_available),
                ]
                cur.execute(
                    f"""
                    SELECT
                        t.task_id,
                        t.task_name,
                        t.target_desc,
                        t.status,
                        t.max_loops,
                        t.current_loop,
                        {", ".join(optional_select)},
                        t.created_at,
                        t.updated_at,
                        COUNT(DISTINCT all_l.loop_id) AS loop_count,
                        COUNT(DISTINCT l.loop_id) AS selected_loop_count,
                        COUNT(DISTINCT r.loop_id) AS archived_loop_count,
                        MIN(l.created_at) AS first_loop_created_at,
                        MAX(l.updated_at) AS latest_loop_updated_at
                    FROM qe_evolution_tasks t
                    LEFT JOIN qe_evolution_loops all_l ON all_l.task_id = t.task_id
                    LEFT JOIN qe_evolution_loops l ON l.task_id = t.task_id {status_filter}
                    LEFT JOIN qe_archive.run r
                      ON r.task_id = t.task_id
                     AND r.loop_id = l.loop_id
                    GROUP BY {", ".join(group_cols)}
                    HAVING COUNT(DISTINCT l.loop_id) > 0
                       {having_filter}
                    ORDER BY COALESCE(MAX(l.updated_at), t.updated_at, t.created_at) DESC NULLS LAST
                    LIMIT %s
                    """,
                    [*status_params, limit],
                )
                rows = self._fetch_dicts(cur)

        task_loops = self._list_task_loop_items([str(row.get("task_id")) for row in rows if row.get("task_id")])
        result: list[dict[str, Any]] = []
        for row in rows:
            selected = int(row.get("selected_loop_count") or 0)
            archived = int(row.get("archived_loop_count") or 0)
            pending = max(0, selected - archived)
            loops = task_loops.get(str(row.get("task_id")), [])
            recommended = sum(1 for loop in loops if loop.get("recommended") and loop.get("archive_status") != "archived")
            manual_only = sum(1 for loop in loops if loop.get("archive_status") == "manual_only")
            not_recommended = sum(1 for loop in loops if loop.get("archive_status") in {"not_recommended", "skipped"})
            result.append(
                {
                    "candidate_id": f"task:{row.get('task_id')}",
                    "candidate_type": "evolution_task",
                    "source": "task",
                    "task_id": row.get("task_id"),
                    "experiment_id": None,
                    "display_name": row.get("task_name") or row.get("task_id"),
                    "description": row.get("target_desc"),
                    "status": row.get("status"),
                    "experiment_type": row.get("task_type") or row.get("evolution_mode") or "evolution",
                    "loop_count": int(row.get("loop_count") or 0),
                    "selected_run_count": selected,
                    "archived_run_count": archived,
                    "pending_run_count": pending,
                    "recommended_run_count": recommended,
                    "manual_only_run_count": manual_only,
                    "not_recommended_run_count": not_recommended,
                    "is_fully_archived": pending == 0,
                    "loops": loops,
                    "node_id": row.get("node_id"),
                    "model_id": row.get("model_id"),
                    "model_catalog_id": row.get("model_catalog_id"),
                    "strategy_id": row.get("strategy_id"),
                    "label_horizon": row.get("label_horizon"),
                    "created_at": _jsonable(row.get("created_at")),
                    "started_at": _jsonable(row.get("first_loop_created_at")),
                    "completed_at": _jsonable(row.get("latest_loop_updated_at")),
                    "updated_at": _jsonable(row.get("updated_at")),
                    "sort_time": _jsonable(row.get("latest_loop_updated_at") or row.get("updated_at") or row.get("created_at")),
                    "archive_action": "archive_all_completed_loops",
                }
            )
        return result

    def _list_single_experiment_candidates(
        self,
        *,
        status: str,
        limit: int,
        include_archived: bool,
    ) -> list[dict[str, Any]]:
        archive_filter = "" if include_archived else "AND r.run_id IS NULL"
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                available = self._available_columns(cur, "qe_experiments")
                if "experiment_id" not in available:
                    return []

                optional_cols = (
                    "experiment_name",
                    "status",
                    "model_id",
                    "model_catalog_id",
                    "strategy_id",
                    "factor_names",
                    "alpha_mode",
                    "created_at",
                    "started_at",
                    "completed_at",
                    "updated_at",
                )
                optional_select = [
                    f"e.{col}" if col in available else f"NULL AS {col}"
                    for col in optional_cols
                ]
                source_filters = []
                if "is_evolution_loop" in available:
                    source_filters.append("COALESCE(e.is_evolution_loop, FALSE) = FALSE")
                if "parent_experiment_id" in available:
                    source_filters.append("e.parent_experiment_id IS NULL")
                source_filter_sql = " AND ".join(source_filters) or "TRUE"
                status_filter, status_params = (
                    _status_filter_sql("e.status", status) if "status" in available else ("", [])
                )
                order_exprs = [
                    f"e.{col}" if col in available else "NULL"
                    for col in ("completed_at", "updated_at", "created_at")
                ]
                cur.execute(
                    f"""
                    SELECT
                        e.experiment_id,
                        {", ".join(optional_select)},
                        r.run_id AS archived_run_id
                    FROM qe_experiments e
                    LEFT JOIN qe_archive.run r ON r.experiment_id = e.experiment_id
                    WHERE {source_filter_sql}
                      {status_filter}
                      {archive_filter}
                    ORDER BY COALESCE({", ".join(order_exprs)}) DESC NULLS LAST
                    LIMIT %s
                    """,
                    [*status_params, limit],
                )
                rows = self._fetch_dicts(cur)

        result: list[dict[str, Any]] = []
        for row in rows:
            archived = 1 if row.get("archived_run_id") else 0
            pending = 0 if archived else 1
            result.append(
                {
                    "candidate_id": f"experiment:{row.get('experiment_id')}",
                    "candidate_type": "single_experiment",
                    "source": "experiment",
                    "task_id": None,
                    "experiment_id": row.get("experiment_id"),
                    "display_name": row.get("experiment_name") or row.get("experiment_id"),
                    "description": row.get("experiment_name"),
                    "status": row.get("status"),
                    "experiment_type": row.get("alpha_mode") or "single_experiment",
                    "loop_count": 0,
                    "selected_run_count": 1,
                    "archived_run_count": archived,
                    "pending_run_count": pending,
                    "is_fully_archived": pending == 0,
                    "node_id": None,
                    "model_id": row.get("model_id"),
                    "model_catalog_id": row.get("model_catalog_id"),
                    "strategy_id": row.get("strategy_id"),
                    "factor_count": len(_ensure_list(row.get("factor_names"))),
                    "created_at": _jsonable(row.get("created_at")),
                    "started_at": _jsonable(row.get("started_at")),
                    "completed_at": _jsonable(row.get("completed_at")),
                    "updated_at": _jsonable(row.get("updated_at")),
                    "sort_time": _jsonable(row.get("completed_at") or row.get("updated_at") or row.get("created_at")),
                    "archive_action": "archive_single_experiment",
                }
            )
        return result

    @staticmethod
    def build_experiment_payload(row: Mapping[str, Any]) -> dict[str, Any]:
        row = dict(row)
        metrics = _ensure_mapping(row.get("result_metrics"))
        custom_params = _ensure_mapping(row.get("custom_params"))
        if runtime_contract_missing(custom_params):
            custom_params = merge_qe_minute_runtime_contract(
                custom_params,
                config=_ensure_mapping(row.get("result_files")),
                source="qe_archive_experiment_payload",
                allow_default_execution_algo=False,
            )
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
        strategy_params = _ensure_mapping(config_json.get("strategy_params"))
        model_params = _ensure_mapping(config_json.get("model_params"))
        for key in ("archive_policy", "archive_reason", "archive_allow_override"):
            if key not in runtime_flags:
                if key in strategy_params:
                    runtime_flags[key] = strategy_params[key]
                elif key in model_params:
                    runtime_flags[key] = model_params[key]
        if runtime_contract_missing(runtime_flags):
            runtime_flags = merge_qe_minute_runtime_contract(
                runtime_flags,
                config=config_json,
                execution_algo=task.get("execution_algo"),
                execution_algo_params=task.get("execution_algo_params"),
                source="qe_archive_loop_payload",
                allow_default_execution_algo=False,
            )
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
        merged_config["runtime_flags"] = runtime_flags
        execution_context = _execution_context(runtime_flags)
        execution_context.update(_ensure_mapping(merged_config.get("execution")))
        merged_config["execution"] = execution_context
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

    @staticmethod
    def _fetch_dicts(cur: Any) -> list[dict[str, Any]]:
        rows = cur.fetchall()
        if not rows:
            return []
        columns = [desc[0] for desc in cur.description or []]
        return [dict(zip(columns, row)) for row in rows]


def _archived_status(run_ids: Sequence[str]) -> dict[str, Any]:
    ids = [str(item) for item in (run_ids or [])]
    return {
        "archive_status": "archived",
        "run_ids": ids,
        "run_count": len(ids),
        "eligible": False,
        "recommended": False,
        "reason": "archived",
    }


def _archive_status_from_policy(
    *,
    source_type: str,
    source_id: str,
    source_sub_id: str | None,
    source_status: str,
    payload: Mapping[str, Any],
    run_ids: Sequence[str],
) -> dict[str, Any]:
    ids = [str(item) for item in (run_ids or [])]
    if ids:
        return _archived_status(ids)

    normalized_status = str(source_status or "").strip().lower()
    if normalized_status != "completed":
        return {
            "archive_status": "not_recommended",
            "run_ids": [],
            "run_count": 0,
            "eligible": False,
            "recommended": False,
            "reason": f"source_status:{normalized_status or 'unknown'}",
            "source_status": normalized_status or None,
        }

    decision = resolve_archive_policy(
        source_system=str(payload.get("source_system") or "qe"),
        source_type=source_type,
        source_id=source_id,
        source_sub_id=source_sub_id,
        payload=payload,
        runtime_config=payload.get("config") if isinstance(payload.get("config"), Mapping) else {},
    )
    if decision.archive_policy == "AUTO":
        status = "recommended"
        eligible = True
        recommended = True
    elif decision.archive_policy == "MANUAL_ONLY":
        status = "manual_only"
        eligible = True
        recommended = False
    else:
        status = "skipped"
        eligible = False
        recommended = False
    return {
        "archive_status": status,
        "run_ids": [],
        "run_count": 0,
        "eligible": eligible,
        "recommended": recommended,
        "reason": decision.reason,
        "archive_policy": decision.archive_policy,
        "archive_policy_source": decision.archive_policy_source,
        "source_status": normalized_status,
    }


def _loop_row_archive_status(row: Mapping[str, Any], *, run_ids: Sequence[str]) -> dict[str, Any]:
    task_id = str(row.get("task_id") or "")
    loop_id = str(row.get("loop_id") or "")
    config = _ensure_mapping(row.get("config_json"))
    payload = {
        "source_system": "qe_evolution",
        "source_id": task_id,
        "source_sub_id": loop_id,
        "task_id": task_id,
        "loop_id": loop_id,
        "loop_index": row.get("loop_index"),
        "status": row.get("status"),
        "config": config,
        "raw_config": {"loop_config_json": config},
    }
    return _archive_status_from_policy(
        source_type="loop",
        source_id=task_id or loop_id,
        source_sub_id=loop_id or None,
        source_status=str(row.get("status") or ""),
        payload=payload,
        run_ids=run_ids,
    )


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


def _dedupe_non_empty(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _dedupe_positive_ints(values: Sequence[int]) -> list[int]:
    seen: set[int] = set()
    result: list[int] = []
    for value in values or []:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed < 1 or parsed in seen:
            continue
        seen.add(parsed)
        result.append(parsed)
    return result


def _status_filter_sql(column: str, status: str) -> tuple[str, list[Any]]:
    normalized = (status or "completed").strip().lower()
    if normalized in {"all", "*"}:
        return "", []
    if normalized == "terminal":
        return f"AND {column} = ANY(%s)", [list(TERMINAL_STATUSES)]
    return f"AND {column} = %s", [normalized]


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
