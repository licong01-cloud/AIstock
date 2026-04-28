"""Execution-node helpers for QE single-alpha and evolution runs.

The helpers centralize node resolution and fail-fast preflight so callers do not
silently fall back to the local RD-Agent API when a selected node is unavailable.
"""
from __future__ import annotations

import os
from typing import Any, Iterable

from psycopg2.extras import RealDictCursor

from ...db.pg_pool import get_conn
from .qe_workspace_client import QEWorkspaceClient

DEFAULT_QE_NODE_ID = "wsl2-5080"
MAX_QE_NODE_PARALLELISM = 4


class QENodePreflightError(RuntimeError):
    """Raised when a selected QE execution node cannot accept a task."""

    def __init__(self, error_code: str, message: str, context: dict[str, Any] | None = None):
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.context = context or {}

    def to_detail(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "message": self.message,
            "context": self.context,
        }


def resolve_default_qe_node_id() -> str:
    """Return the explicit default QE node id used when the UI leaves node blank."""
    return (os.getenv("AISTOCK_DEFAULT_GPU_NODE_ID") or DEFAULT_QE_NODE_ID).strip() or DEFAULT_QE_NODE_ID


def get_compute_node(node_id: str) -> dict[str, Any] | None:
    """Fetch a compute-node row as a dict without applying fallback behavior."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT node_id, display_name, api_base_url, gpu_model, gpu_vram_mb,
                       status, callback_url, workspace_base, factor_data_dir,
                       qlib_data_path, qlib_minute_path, qlib_rdagent_root
                FROM infra.compute_nodes
                WHERE node_id = %s
                """,
                (node_id,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def normalize_node_parallelism(
    selected_node_ids: Iterable[str],
    raw: dict[str, Any] | None,
) -> dict[str, int]:
    """Normalize per-node parallelism: selected nodes only, default 1, max 4."""
    selected = {str(node_id).strip() for node_id in selected_node_ids if str(node_id or "").strip()}
    if not selected:
        raise QENodePreflightError(
            "QE_NODE_SELECTION_EMPTY",
            "QE execution requires at least one resolved node.",
        )

    raw = raw or {}
    unknown = sorted(set(raw.keys()) - selected)
    if unknown:
        raise QENodePreflightError(
            "QE_NODE_PARALLELISM_UNKNOWN_NODE",
            "node_parallelism contains nodes that are not selected by any loop.",
            {"unknown_node_ids": unknown, "selected_node_ids": sorted(selected)},
        )

    normalized: dict[str, int] = {}
    for node_id in sorted(selected):
        value = raw.get(node_id, 1)
        try:
            limit = int(value)
        except Exception as exc:
            raise QENodePreflightError(
                "QE_NODE_PARALLELISM_INVALID",
                f"Invalid parallelism for node {node_id}: {value!r}.",
                {"node_id": node_id, "value": value},
            ) from exc
        if limit < 1 or limit > MAX_QE_NODE_PARALLELISM:
            raise QENodePreflightError(
                "QE_NODE_PARALLELISM_OUT_OF_RANGE",
                f"Node {node_id} parallelism must be between 1 and {MAX_QE_NODE_PARALLELISM}.",
                {"node_id": node_id, "value": limit, "max": MAX_QE_NODE_PARALLELISM},
            )
        normalized[node_id] = limit
    return normalized


def resolve_custom_loop_nodes(
    loops_config: list[dict[str, Any]],
    request_node_id: str | None,
) -> tuple[list[dict[str, Any]], str, set[str]]:
    """Resolve loop-level nodes; Loop2+ inherit Loop1 unless explicitly set."""
    if not loops_config:
        raise QENodePreflightError("QE_CUSTOM_LOOPS_EMPTY", "custom_evo requires at least one loop.")

    default_node = (request_node_id or "").strip() or resolve_default_qe_node_id()
    first_cfg = dict(loops_config[0])
    first_node = (first_cfg.get("node_id") or "").strip() or default_node

    resolved: list[dict[str, Any]] = []
    selected: set[str] = set()
    for idx, loop_cfg in enumerate(loops_config, start=1):
        cfg = dict(loop_cfg)
        raw_node = (cfg.get("node_id") or "").strip()
        effective_node = raw_node or first_node
        if not effective_node:
            raise QENodePreflightError(
                "QE_LOOP_NODE_UNRESOLVED",
                f"Loop {idx} execution node could not be resolved.",
                {"loop_index": idx},
            )
        cfg["node_id"] = effective_node
        selected.add(effective_node)
        resolved.append(cfg)
    return resolved, first_node, selected


async def preflight_qe_node(node_id: str) -> dict[str, Any]:
    """Fail fast unless the selected node is non-offline and its QE API is reachable."""
    node_id = (node_id or "").strip()
    if not node_id:
        raise QENodePreflightError("QE_NODE_ID_EMPTY", "QE execution node id is empty.")

    node = get_compute_node(node_id)
    if not node:
        raise QENodePreflightError(
            "QE_NODE_NOT_FOUND",
            f"Node {node_id} does not exist.",
            {"node_id": node_id},
        )

    status = str(node.get("status") or "").lower()
    if status == "offline":
        raise QENodePreflightError(
            "QE_NODE_OFFLINE",
            f"Node {node_id} is offline and cannot accept QE tasks.",
            {"node_id": node_id, "status": node.get("status")},
        )

    if not str(node.get("api_base_url") or "").strip():
        raise QENodePreflightError(
            "QE_NODE_API_BASE_MISSING",
            f"Node {node_id} has no api_base_url configured.",
            {"node_id": node_id},
        )

    try:
        async with QEWorkspaceClient.for_node(node_id) as client:
            workspace_config = await client.get_workspace_config()
    except Exception as exc:
        raise QENodePreflightError(
            "QE_NODE_API_UNREACHABLE",
            f"Node {node_id} QE workspace API is unreachable; refusing to submit.",
            {
                "node_id": node_id,
                "api_base_url": node.get("api_base_url"),
                "phase": "preflight_get_workspace_config",
                "error": str(exc),
            },
        ) from exc

    required = ["workspace_base", "factor_data_dir", "qlib_data_path", "qlib_minute_path", "qlib_rdagent_root"]
    missing = [key for key in required if not workspace_config.get(key) and not node.get(key)]
    if missing:
        raise QENodePreflightError(
            "QE_NODE_WORKSPACE_CONFIG_INCOMPLETE",
            f"Node {node_id} workspace config is incomplete: {', '.join(missing)}.",
            {"node_id": node_id, "missing": missing},
        )

    node["workspace_config"] = workspace_config
    return node


async def preflight_qe_nodes(node_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Preflight all selected nodes. The first failing node aborts the submission."""
    results: dict[str, dict[str, Any]] = {}
    for node_id in sorted({str(n).strip() for n in node_ids if str(n or "").strip()}):
        results[node_id] = await preflight_qe_node(node_id)
    return results
