"""AIstock QE Experiment MCP server.

Thin wrapper: every tool calls loopback FastAPI QE endpoints. It does not import
QE schedulers, backend DB repositories, or RD-Agent worker paths.
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

from scripts.aistock_mcp_common import LoopbackApiClient, require_confirm, sanitize_identifier, sanitize_tail
from backend.services.quantevolver.seed_contract import ensure_loop_fixed_seed, ensure_template_fixed_seeds
from backend.services.qe_templates.validator import validate_template_payload

DEFAULT_BASE_URL = "http://127.0.0.1:8001/api/v1"
QE_EXPERIMENT_RUN_CONFIRM = "QE_EXPERIMENT_RUN"
QE_EXPERIMENT_STOP_CONFIRM = "QE_EXPERIMENT_STOP"
QE_CUSTOM_EVO_RUN_CONFIRM = "QE_CUSTOM_EVO_RUN"
QE_CUSTOM_EVO_DELETE_CONFIRM = "QE_CUSTOM_EVO_DELETE"
QE_TEMPLATE_MATERIALIZE_CONFIRM = "QE_TEMPLATE_MATERIALIZE"
QE_TEMPLATE_DELETE_CONFIRM = "QE_TEMPLATE_DELETE"

mcp = FastMCP("aistock-qe-experiment")
_default_client = LoopbackApiClient(
    base_url=os.environ.get("AISTOCK_QE_EXPERIMENT_BASE_URL", DEFAULT_BASE_URL),
    env_name="AISTOCK_QE_EXPERIMENT_BASE_URL",
)


def _client() -> LoopbackApiClient:
    return _default_client


@mcp.tool()
def qe_experiment_list(limit: int = 50, offset: int = 0, include_children: bool = False, detail: str = "summary") -> dict[str, Any]:
    if detail not in {"summary", "full"}:
        raise ValueError("detail must be summary or full")
    return _client().get(
        "/quantevolver/experiments",
        params={"limit": limit, "offset": offset, "include_children": include_children, "detail": detail},
    )


@mcp.tool()
def qe_experiment_get(experiment_id: str, detail: str = "summary") -> dict[str, Any]:
    if detail not in {"summary", "full"}:
        raise ValueError("detail must be summary or full")
    safe = sanitize_identifier(experiment_id, "experiment_id")
    return _client().get(f"/quantevolver/experiments/{safe}", params={"detail": detail})


@mcp.tool()
def qe_experiment_get_status(experiment_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(experiment_id, "experiment_id")
    return _client().get(f"/quantevolver/experiments/{safe}/run-status")


@mcp.tool()
def qe_experiment_get_logs_tail(experiment_id: str, tail: int = 500) -> dict[str, Any]:
    safe = sanitize_identifier(experiment_id, "experiment_id")
    return _client().get(f"/quantevolver/experiments/{safe}/logs/tail", params={"tail": sanitize_tail(tail)})


@mcp.tool()
def qe_experiment_get_enhanced_metrics(experiment_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(experiment_id, "experiment_id")
    return _client().get(f"/quantevolver/experiments/{safe}/enhanced-metrics")


@mcp.tool()
def qe_experiment_get_trade_stats(experiment_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(experiment_id, "experiment_id")
    return _client().get(f"/quantevolver/experiments/{safe}/trade-stats")


@mcp.tool()
def qe_experiment_run_confirmed(experiment_id: str, node_id: str | None = None, confirm_run: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_run, QE_EXPERIMENT_RUN_CONFIRM, "confirm_run")
    safe = sanitize_identifier(experiment_id, "experiment_id")
    if node_id:
        node_id = sanitize_identifier(node_id, "node_id")
    return _client().post(f"/quantevolver/experiments/{safe}/run", params={"engine_mode": "unified", "node_id": node_id})


@mcp.tool()
def qe_experiment_stop_confirmed(experiment_id: str, confirm_stop: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_stop, QE_EXPERIMENT_STOP_CONFIRM, "confirm_stop")
    safe = sanitize_identifier(experiment_id, "experiment_id")
    return _client().post(f"/quantevolver/experiments/{safe}/stop")


@mcp.tool()
def qe_custom_evo_list_tasks(status: str | None = None, limit: int = 50, detail: str = "summary") -> dict[str, Any]:
    if detail not in {"summary", "full"}:
        raise ValueError("detail must be summary or full")
    return _client().get("/quantevolver/evolution/tasks", params={"status": status, "limit": limit, "detail": detail})


@mcp.tool()
def qe_custom_evo_get_task(task_id: str, detail: str = "summary") -> dict[str, Any]:
    if detail not in {"summary", "full"}:
        raise ValueError("detail must be summary or full")
    safe = sanitize_identifier(task_id, "task_id")
    return _client().get(f"/quantevolver/evolution/tasks/{safe}", params={"detail": detail})


@mcp.tool()
def qe_custom_evo_loop_comparison(task_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(task_id, "task_id")
    return _client().get(f"/quantevolver/evolution/tasks/{safe}/loops/comparison")


@mcp.tool()
def qe_custom_evo_get_loop_config(task_id: str, loop_index: int) -> dict[str, Any]:
    safe = sanitize_identifier(task_id, "task_id")
    if int(loop_index) < 1:
        raise ValueError("loop_index must be >= 1")
    return _client().get(f"/quantevolver/evolution/tasks/{safe}/loops/{int(loop_index)}/config")


@mcp.tool()
def qe_custom_evo_get_loop_metrics(task_id: str, loop_index: int) -> dict[str, Any]:
    safe = sanitize_identifier(task_id, "task_id")
    if int(loop_index) < 1:
        raise ValueError("loop_index must be >= 1")
    return _client().get(f"/quantevolver/evolution/tasks/{safe}/loops/{int(loop_index)}/metrics")


@mcp.tool()
def qe_custom_evo_get_loop_analysis(task_id: str, loop_index: int) -> dict[str, Any]:
    safe = sanitize_identifier(task_id, "task_id")
    if int(loop_index) < 1:
        raise ValueError("loop_index must be >= 1")
    return _client().get(f"/quantevolver/evolution/tasks/{safe}/loops/{int(loop_index)}/analysis")


@mcp.tool()
def qe_custom_evo_get_config(task_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(task_id, "task_id")
    return _client().get(f"/quantevolver/evolution/tasks/{safe}/custom-evo-config")


@mcp.tool()
def qe_custom_evo_get_logs_tail(task_id: str, tail: int = 500) -> dict[str, Any]:
    safe = sanitize_identifier(task_id, "task_id")
    return _client().get(f"/quantevolver/evolution/tasks/{safe}/logs/tail", params={"tail": sanitize_tail(tail)})


@mcp.tool()
def qe_custom_evo_run_confirmed(task_id: str, force_full_train: bool = False, confirm_custom_evo: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_custom_evo, QE_CUSTOM_EVO_RUN_CONFIRM, "confirm_custom_evo")
    safe = sanitize_identifier(task_id, "task_id")
    return _client().post(f"/quantevolver/evolution/tasks/{safe}/custom-evo/run", {"confirm_custom_evo": QE_CUSTOM_EVO_RUN_CONFIRM, "force_full_train": force_full_train})


@mcp.tool()
def qe_custom_evo_delete_confirmed(task_id: str, confirm_delete: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_delete, QE_CUSTOM_EVO_DELETE_CONFIRM, "confirm_delete")
    safe = sanitize_identifier(task_id, "task_id")
    return _client().delete(f"/quantevolver/evolution/tasks/{safe}")


@mcp.tool()
def qe_custom_evo_retry_loop_confirmed(task_id: str, loop_index: int, retry_mode: str = "auto", confirm_retry: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_retry, "QE_CUSTOM_EVO_RETRY", "confirm_retry")
    safe = sanitize_identifier(task_id, "task_id")
    if int(loop_index) < 1:
        raise ValueError("loop_index must be >= 1")
    return _client().post(f"/quantevolver/evolution/tasks/{safe}/loops/{int(loop_index)}/retry", {"retry_mode": retry_mode})


@mcp.tool()
def qe_custom_evo_rerun_loop_confirmed(task_id: str, loop_index: int, loop: dict[str, Any], confirm_rerun: str | None = None, phase_pipeline_enabled: bool | None = None, resource_telemetry_enabled: bool | None = None) -> dict[str, Any]:
    require_confirm(confirm_rerun, "QE_CUSTOM_EVO_RERUN", "confirm_rerun")
    loop_payload = dict(loop or {})
    ensure_loop_fixed_seed(loop_payload, context="qe_custom_evo_rerun_loop_confirmed.loop")
    safe = sanitize_identifier(task_id, "task_id")
    return _client().post(
        f"/quantevolver/evolution/tasks/{safe}/loops/{int(loop_index)}/rerun",
        {
            "loop": loop_payload,
            "confirm_delete_old_result": True,
            "phase_pipeline_enabled": phase_pipeline_enabled,
            "resource_telemetry_enabled": False,
        },
    )


@mcp.tool()
def qe_custom_evo_append_loops_confirmed(task_id: str, loops: list[dict[str, Any]], confirm_append: str | None = None, phase_pipeline_enabled: bool | None = None, resource_telemetry_enabled: bool | None = None) -> dict[str, Any]:
    require_confirm(confirm_append, "QE_CUSTOM_EVO_APPEND", "confirm_append")
    loop_payloads = [dict(loop or {}) for loop in (loops or [])]
    for idx, loop in enumerate(loop_payloads, start=1):
        ensure_loop_fixed_seed(loop, context=f"qe_custom_evo_append_loops_confirmed.loops[{idx}]")
    safe = sanitize_identifier(task_id, "task_id")
    return _client().post(
        f"/quantevolver/evolution/tasks/{safe}/custom-loops/append",
        {
            "loops": loop_payloads,
            "ack_failed_loop_warning": True,
            "phase_pipeline_enabled": phase_pipeline_enabled,
            "resource_telemetry_enabled": False,
        },
    )


@mcp.tool()
def qe_template_create(template_kind: str, title: str, config_json: dict[str, Any], archive_policy: str = "AUTO", description: str | None = None) -> dict[str, Any]:
    normalized_config = ensure_template_fixed_seeds(template_kind, config_json or {})
    validation = validate_template_payload(template_kind, normalized_config)
    if not validation.get("valid"):
        raise ValueError("template validation failed: " + "; ".join(validation.get("errors") or []))
    return _client().post("/qe-templates", {"template_kind": template_kind, "title": title, "description": description, "config_json": normalized_config, "archive_policy": archive_policy})


@mcp.tool()
def qe_template_get(template_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(template_id, "template_id")
    return _client().get(f"/qe-templates/{safe}")


@mcp.tool()
def qe_template_validate(template_id: str) -> dict[str, Any]:
    safe = sanitize_identifier(template_id, "template_id")
    return _client().post(f"/qe-templates/{safe}/validate")


@mcp.tool()
def qe_template_materialize_confirmed(template_id: str, confirm_template: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_template, QE_TEMPLATE_MATERIALIZE_CONFIRM, "confirm_template")
    safe = sanitize_identifier(template_id, "template_id")
    return _client().post(f"/qe-templates/{safe}/materialize", {"confirm_template": QE_TEMPLATE_MATERIALIZE_CONFIRM})


@mcp.tool()
def qe_template_delete_confirmed(template_id: str, confirm_delete: str | None = None) -> dict[str, Any]:
    require_confirm(confirm_delete, QE_TEMPLATE_DELETE_CONFIRM, "confirm_delete")
    safe = sanitize_identifier(template_id, "template_id")
    return _client().delete(f"/qe-templates/{safe}", {"confirm_delete": QE_TEMPLATE_DELETE_CONFIRM})


@mcp.tool()
def qe_template_run_confirmed(template_id: str, confirm_run: str | None = None, node_id: str | None = None) -> dict[str, Any]:
    safe = sanitize_identifier(template_id, "template_id")
    if node_id:
        node_id = sanitize_identifier(node_id, "node_id")
    if confirm_run not in {QE_EXPERIMENT_RUN_CONFIRM, QE_CUSTOM_EVO_RUN_CONFIRM}:
        raise ValueError("confirm_run must equal QE_EXPERIMENT_RUN or QE_CUSTOM_EVO_RUN")
    return _client().post(f"/qe-templates/{safe}/run", {"confirm_run": confirm_run, "node_id": node_id})


if __name__ == "__main__":
    mcp.run()
