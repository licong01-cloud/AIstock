"""AIstock QE Archive MCP server.

Thin loopback HTTP wrapper for archive queries and confirmed warehouse jobs.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover
    raise SystemExit("mcp package is required: pip install mcp") from exc

from scripts.aistock_mcp_common import LoopbackApiClient, require_confirm, sanitize_identifier

DEFAULT_BASE_URL = "http://127.0.0.1:8001/api/v1/qe-archive"
QE_ARCHIVE_BACKFILL_CONFIRM = "QE_ARCHIVE_BACKFILL"
QE_ARCHIVE_WRITE_CONFIRM = "QE_ARCHIVE_WRITE"
QE_ARCHIVE_WORKER_CONFIRM = "QE_ARCHIVE_WORKER_RUN"

mcp = FastMCP("aistock-qe-archive")
_default_client = LoopbackApiClient(
    base_url=os.environ.get("AISTOCK_QE_ARCHIVE_BASE_URL", DEFAULT_BASE_URL),
    env_name="AISTOCK_QE_ARCHIVE_BASE_URL",
)


def _client() -> LoopbackApiClient:
    return _default_client


@mcp.tool()
def qe_archive_health() -> dict[str, Any]:
    return _client().get("/health")


@mcp.tool()
def qe_archive_query_resource_phases(
    run_id: str | None = None,
    task_id: str | None = None,
    loop_index: int | None = None,
    source_run_key: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    return _client().get(
        "/resource-phases",
        params={
            "run_id": run_id,
            "task_id": task_id,
            "loop_index": loop_index,
            "source_run_key": source_run_key,
            "limit": max(1, min(int(limit), 200)),
        },
    )


@mcp.tool()
def qe_archive_list_runs(status: str | None = None, run_type: str | None = None, search: str | None = None, limit: int = 20) -> dict[str, Any]:
    return _client().get("/runs", params={"status": status, "run_type": run_type, "search": search, "limit": limit})


@mcp.tool()
def qe_archive_get_run_quality(run_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(run_id, "run_id")
    return _client().get(f"/runs/{safe}/quality")


@mcp.tool()
def qe_archive_list_outbox(status: str | None = None, limit: int = 20) -> dict[str, Any]:
    return _client().get("/outbox", params={"status": status, "limit": limit})


@mcp.tool()
def qe_archive_list_jobs(status: str | None = None, limit: int = 20) -> dict[str, Any]:
    return _client().get("/jobs", params={"status": status, "limit": limit})


@mcp.tool()
def qe_archive_list_skips(archive_policy: str | None = None, source_type: str | None = None, limit: int = 20) -> dict[str, Any]:
    return _client().get("/skips", params={"archive_policy": archive_policy, "source_type": source_type, "limit": limit})


@mcp.tool()
def qe_archive_backfill_preview(source_mode: str = "completed_custom_evo_loops", limit: int = 20, include_archived: bool = False) -> dict[str, Any]:
    return _client().post("/backfill/preview", {"source_mode": source_mode, "limit": limit, "include_archived": include_archived})


@mcp.tool()
def qe_archive_backfill_execute_confirmed(source_mode: str = "completed_custom_evo_loops", limit: int = 20, include_archived: bool = False, confirm_backfill: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_backfill, QE_ARCHIVE_BACKFILL_CONFIRM, "confirm_backfill")
    return _client().post("/backfill/execute", {"source_mode": source_mode, "limit": limit, "include_archived": include_archived, "confirm_backfill": QE_ARCHIVE_BACKFILL_CONFIRM})


def _sanitize_ids(values: list[str] | None, field_name: str) -> list[str]:
    return [sanitize_identifier(value, field_name) for value in (values or []) if str(value or "").strip()]


def _positive_indices(values: list[int] | None) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for value in values or []:
        parsed = int(value)
        if parsed < 1 or parsed in seen:
            continue
        seen.add(parsed)
        result.append(parsed)
    return result


def _selection_payload(
    *,
    experiment_ids: list[str] | None,
    task_ids: list[str] | None,
    loop_ids: list[str] | None,
    task_id: str | None,
    loop_indices: list[int] | None,
    status: str,
    include_archived: bool,
) -> dict[str, Any]:
    safe_task_id = sanitize_identifier(task_id, "task_id") if task_id else None
    return {
        "source_mode": "specific_ids",
        "experiment_ids": _sanitize_ids(experiment_ids, "experiment_id"),
        "task_ids": _sanitize_ids(task_ids, "task_id"),
        "loop_ids": _sanitize_ids(loop_ids, "loop_id"),
        "task_id": safe_task_id,
        "loop_indices": _positive_indices(loop_indices),
        "status": status,
        "include_archived": include_archived,
        "requested_by": "qe_archive_mcp",
    }


@mcp.tool()
def qe_archive_backfill_selection_preview(
    experiment_ids: list[str] | None = None,
    task_ids: list[str] | None = None,
    loop_ids: list[str] | None = None,
    task_id: str | None = None,
    loop_indices: list[int] | None = None,
    status: str = "completed",
    include_archived: bool = False,
) -> dict[str, Any]:
    """Preview an explicit experiment/task/loop selection without writing."""

    return _client().post(
        "/backfill/preview",
        _selection_payload(
            experiment_ids=experiment_ids,
            task_ids=task_ids,
            loop_ids=loop_ids,
            task_id=task_id,
            loop_indices=loop_indices,
            status=status,
            include_archived=include_archived,
        ),
    )


@mcp.tool()
def qe_archive_backfill_selection_execute_confirmed(
    experiment_ids: list[str] | None = None,
    task_ids: list[str] | None = None,
    loop_ids: list[str] | None = None,
    task_id: str | None = None,
    loop_indices: list[int] | None = None,
    status: str = "completed",
    include_archived: bool = False,
    confirm_write: str | None = None,
) -> dict[str, Any]:
    """Write an explicit experiment/task/loop selection after confirmation."""

    require_confirm(confirm_write, QE_ARCHIVE_WRITE_CONFIRM, "confirm_write")
    payload = _selection_payload(
        experiment_ids=experiment_ids,
        task_ids=task_ids,
        loop_ids=loop_ids,
        task_id=task_id,
        loop_indices=loop_indices,
        status=status,
        include_archived=include_archived,
    )
    payload["confirm_backfill"] = QE_ARCHIVE_BACKFILL_CONFIRM
    return _client().post(
        "/backfill/execute",
        payload,
    )


@mcp.tool()
def qe_archive_get_source_status(
    experiment_ids: list[str] | None = None,
    task_ids: list[str] | None = None,
    loop_ids: list[str] | None = None,
    include_recommendation: bool = True,
) -> dict[str, Any]:
    return _client().post(
        "/source-status",
        {
            "experiment_ids": _sanitize_ids(experiment_ids, "experiment_id"),
            "task_ids": _sanitize_ids(task_ids, "task_id"),
            "loop_ids": _sanitize_ids(loop_ids, "loop_id"),
            "include_recommendation": include_recommendation,
        },
    )


@mcp.tool()
def qe_archive_list_backfill_runs(status: str | None = None, limit: int = 20) -> dict[str, Any]:
    return _client().get("/backfill/runs", params={"status": status, "limit": limit})


@mcp.tool()
def qe_archive_get_backfill_run(backfill_run_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(backfill_run_id, "backfill_run_id")
    return _client().get(f"/backfill/runs/{safe}")


@mcp.tool()
def qe_archive_worker_run_once_confirmed(limit: int = 10, worker_id: str = "qe_archive_mcp_worker", confirm_run: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_run, QE_ARCHIVE_WORKER_CONFIRM, "confirm_run")
    safe_worker = sanitize_identifier(worker_id, "worker_id")
    return _client().post("/worker/run-once", {"limit": limit, "worker_id": safe_worker, "confirm_run": QE_ARCHIVE_WORKER_CONFIRM})


@mcp.tool()
def qe_archive_query_factor_usage(limit: int = 20, min_runs: int = 1) -> dict[str, Any]:
    return _client().get("/query/factor-usage", params={"limit": limit, "min_runs": min_runs})


@mcp.tool()
def qe_archive_query_factor_importance(
    run_id: str | None = None,
    task_id: str | None = None,
    loop_index: int | None = None,
    factor_name: str | None = None,
    method: str | None = None,
    limit: int = 10,
    order: str = "desc",
) -> dict[str, Any]:
    return _client().get(
        "/query/factor-importance",
        params={
            "run_id": run_id,
            "task_id": task_id,
            "loop_index": loop_index,
            "factor_name": factor_name,
            "method": method,
            "limit": limit,
            "order": order,
        },
    )


@mcp.tool()
def qe_archive_query_factor_importance_stability(
    factor_name: str | None = None,
    method: str | None = None,
    min_runs: int = 2,
    limit: int = 10,
) -> dict[str, Any]:
    return _client().get(
        "/query/factor-importance/stability",
        params={
            "factor_name": factor_name,
            "method": method,
            "min_runs": min_runs,
            "limit": limit,
        },
    )


@mcp.tool()
def qe_archive_query_model_trials(model_type: str | None = None, limit: int = 20) -> dict[str, Any]:
    return _client().get("/query/model-trials", params={"model_type": model_type, "limit": limit})


@mcp.tool()
def qe_archive_query_seed_trials(limit: int = 20) -> dict[str, Any]:
    return _client().get("/query/seed-trials", params={"limit": limit})


@mcp.tool()
def qe_archive_query_hyperparam_history(model_type: str | None = None, param_key: str | None = None, limit: int = 20) -> dict[str, Any]:
    return _client().get("/query/hyperparams", params={"model_type": model_type, "param_key": param_key, "limit": limit})


@mcp.tool()
def qe_archive_query_analytics_view_status() -> dict[str, Any]:
    """Return availability and row counts for the compact QE analytics views."""

    return _client().get("/analytics/views")


@mcp.tool()
def qe_archive_query_run_leaderboard(
    model_type: str | None = None,
    min_icir: float | None = None,
    min_ir: float | None = None,
    limit: int = 20,
    order_by: str = "calmar",
) -> dict[str, Any]:
    """Query compact run-level signal/return leaderboard rows."""

    return _client().get(
        "/analytics/run-leaderboard",
        params={
            "model_type": model_type,
            "min_icir": min_icir,
            "min_ir": min_ir,
            "limit": limit,
            "order_by": order_by,
        },
    )


@mcp.tool()
def qe_archive_query_topk_quality(
    run_id: str | None = None,
    task_id: str | None = None,
    k: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Query forward-only prediction-rank Top-K quality rows."""

    return _client().get(
        "/analytics/topk-quality",
        params={"run_id": run_id, "task_id": task_id, "k": k, "limit": limit},
    )


@mcp.tool()
def qe_archive_query_seed_robustness(
    model_type: str | None = None,
    min_seed_count: int = 2,
    stable_only: bool = False,
    limit: int = 20,
    order_by: str = "cagr_mean",
) -> dict[str, Any]:
    """Query multi-seed robustness by compact config fingerprint."""

    return _client().get(
        "/analytics/seed-robustness",
        params={
            "model_type": model_type,
            "min_seed_count": min_seed_count,
            "stable_only": stable_only,
            "limit": limit,
            "order_by": order_by,
        },
    )


@mcp.tool()
def qe_archive_query_factor_performance(
    factor_name: str | None = None,
    min_runs: int = 1,
    limit: int = 20,
    order_by: str = "best_cagr",
) -> dict[str, Any]:
    """Query compact factor performance and usage footprint rows."""

    return _client().get(
        "/analytics/factor-performance",
        params={"factor_name": factor_name, "min_runs": min_runs, "limit": limit, "order_by": order_by},
    )


@mcp.tool()
def qe_archive_query_model_hyperparam_seed_perf(
    model_type: str | None = None,
    hyperparam_hash: str | None = None,
    limit: int = 20,
    order_by: str = "cagr",
) -> dict[str, Any]:
    """Query compact model hyperparameter-by-seed performance rows."""

    return _client().get(
        "/analytics/model-hyperparam-seed-perf",
        params={
            "model_type": model_type,
            "hyperparam_hash": hyperparam_hash,
            "limit": limit,
            "order_by": order_by,
        },
    )


@mcp.tool()
def qe_archive_query_overfit_flags(
    suspicious_only: bool = True,
    model_type: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Query compact overfit and seed-outlier flags."""

    return _client().get(
        "/analytics/overfit-flags",
        params={"suspicious_only": suspicious_only, "model_type": model_type, "limit": limit},
    )


@mcp.tool()
def qe_archive_query_promotion_candidates(
    model_type: str | None = None,
    min_seed_count: int = 5,
    limit: int = 20,
    order_by: str = "calmar",
) -> dict[str, Any]:
    """Query compact multi-seed promotion candidate configs."""

    return _client().get(
        "/analytics/promotion-candidates",
        params={
            "model_type": model_type,
            "min_seed_count": min_seed_count,
            "limit": limit,
            "order_by": order_by,
        },
    )


@mcp.tool()
def qe_archive_query_evolution_lineage(
    task_id: str | None = None,
    experiment_id: str | None = None,
    model_type: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Query compact task/loop/experiment/run lineage rows."""

    return _client().get(
        "/analytics/evolution-lineage",
        params={"task_id": task_id, "experiment_id": experiment_id, "model_type": model_type, "limit": limit},
    )


if __name__ == "__main__":
    mcp.run()
