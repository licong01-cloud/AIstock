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

DEFAULT_BASE_URL = "http://127.0.0.1:8011/api/v1/qe-archive"
QE_ARCHIVE_BACKFILL_CONFIRM = "QE_ARCHIVE_BACKFILL"
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
def qe_archive_list_runs(status: str | None = None, run_type: str | None = None, search: str | None = None, limit: int = 100) -> dict[str, Any]:
    return _client().get("/runs", params={"status": status, "run_type": run_type, "search": search, "limit": limit})


@mcp.tool()
def qe_archive_get_run_quality(run_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(run_id, "run_id")
    return _client().get(f"/runs/{safe}/quality")


@mcp.tool()
def qe_archive_list_outbox(status: str | None = None, limit: int = 50) -> dict[str, Any]:
    return _client().get("/outbox", params={"status": status, "limit": limit})


@mcp.tool()
def qe_archive_list_jobs(status: str | None = None, limit: int = 50) -> dict[str, Any]:
    return _client().get("/jobs", params={"status": status, "limit": limit})


@mcp.tool()
def qe_archive_list_skips(archive_policy: str | None = None, source_type: str | None = None, limit: int = 100) -> dict[str, Any]:
    return _client().get("/skips", params={"archive_policy": archive_policy, "source_type": source_type, "limit": limit})


@mcp.tool()
def qe_archive_backfill_preview(source_mode: str = "completed_custom_evo_loops", limit: int = 20, include_archived: bool = False) -> dict[str, Any]:
    return _client().post("/backfill/preview", {"source_mode": source_mode, "limit": limit, "include_archived": include_archived})


@mcp.tool()
def qe_archive_backfill_execute_confirmed(source_mode: str = "completed_custom_evo_loops", limit: int = 20, include_archived: bool = False, confirm_backfill: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_backfill, QE_ARCHIVE_BACKFILL_CONFIRM, "confirm_backfill")
    return _client().post("/backfill/execute", {"source_mode": source_mode, "limit": limit, "include_archived": include_archived, "confirm_backfill": QE_ARCHIVE_BACKFILL_CONFIRM})


@mcp.tool()
def qe_archive_list_backfill_runs(status: str | None = None, limit: int = 50) -> dict[str, Any]:
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
def qe_archive_query_factor_usage(limit: int = 50, min_runs: int = 1) -> dict[str, Any]:
    return _client().get("/query/factor-usage", params={"limit": limit, "min_runs": min_runs})


@mcp.tool()
def qe_archive_query_model_trials(model_type: str | None = None, limit: int = 50) -> dict[str, Any]:
    return _client().get("/query/model-trials", params={"model_type": model_type, "limit": limit})


@mcp.tool()
def qe_archive_query_seed_trials(limit: int = 50) -> dict[str, Any]:
    return _client().get("/query/seed-trials", params={"limit": limit})


@mcp.tool()
def qe_archive_query_hyperparam_history(model_type: str | None = None, param_key: str | None = None, limit: int = 50) -> dict[str, Any]:
    return _client().get("/query/hyperparams", params={"model_type": model_type, "param_key": param_key, "limit": limit})


if __name__ == "__main__":
    mcp.run()
