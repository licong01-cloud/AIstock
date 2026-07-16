"""QE Experiment MCP tool wrappers for the unified gateway."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from backend.mcp.common import sanitize_tail
from backend.services.quantevolver.experiment_config import ALLOWED_LABEL_HORIZONS

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


QE_EXPERIMENT_RUN_CONFIRM = "QE_EXPERIMENT_RUN"
QE_EXPERIMENT_STOP_CONFIRM = "QE_EXPERIMENT_STOP"
QE_SINGLE_EXPERIMENT_UPDATE_CONFIG_CONFIRM = "QE_SINGLE_EXPERIMENT_UPDATE_CONFIG"
QE_CUSTOM_EVO_RUN_CONFIRM = "QE_CUSTOM_EVO_RUN"
QE_CUSTOM_EVO_UPDATE_CONFIG_CONFIRM = "QE_CUSTOM_EVO_UPDATE_CONFIG"
QE_CUSTOM_EVO_DELETE_CONFIRM = "QE_CUSTOM_EVO_DELETE"
QE_TEMPLATE_MATERIALIZE_CONFIRM = "QE_TEMPLATE_MATERIALIZE"
QE_TEMPLATE_DELETE_CONFIRM = "QE_TEMPLATE_DELETE"
QE_TEMPLATE_CREATE_AND_RUN_CONFIRM = "QE_TEMPLATE_CREATE_AND_RUN"

TOOL_NAMES = (
    "qe_experiment_list",
    "qe_experiment_get",
    "qe_experiment_get_status",
    "qe_experiment_get_logs_tail",
    "qe_experiment_get_enhanced_metrics",
    "qe_experiment_get_trade_stats",
    "qe_experiment_validate_config",
    "qe_single_experiment_create_pending",
    "qe_single_experiment_get_config",
    "qe_single_experiment_update_config_confirmed",
    "qe_experiment_run_confirmed",
    "qe_experiment_stop_confirmed",
    "qe_custom_evo_list_tasks",
    "qe_custom_evo_get_task",
    "qe_custom_evo_loop_comparison",
    "qe_custom_evo_get_loop_config",
    "qe_custom_evo_get_loop_metrics",
    "qe_custom_evo_get_loop_analysis",
    "qe_custom_evo_get_config",
    "qe_custom_evo_get_logs_tail",
    "qe_custom_evo_create_pending",
    "qe_custom_evo_update_config_confirmed",
    "qe_custom_evo_run_confirmed",
    "qe_custom_evo_delete_confirmed",
    "qe_custom_evo_retry_loop_confirmed",
    "qe_custom_evo_rerun_loop_confirmed",
    "qe_custom_evo_append_loops_confirmed",
    "qe_template_create",
    "qe_template_get",
    "qe_template_validate",
    "qe_template_materialize_confirmed",
    "qe_template_delete_confirmed",
    "qe_template_run_confirmed",
    "qe_template_create_and_run_confirmed",
)
TOOL_COUNT = len(TOOL_NAMES)

MAX_QE_NODE_PARALLELISM = 4
QE_LABEL_HORIZONS = frozenset(ALLOWED_LABEL_HORIZONS)
QE_TEMPLATE_KINDS = {"single_experiment", "custom_evo"}


def _require_detail(detail: str) -> None:
    if detail not in {"summary", "full"}:
        raise ValueError("detail must be summary or full")


def _require_positive_loop_index(loop_index: int) -> int:
    parsed = int(loop_index)
    if parsed < 1:
        raise ValueError("loop_index must be >= 1")
    return parsed


def _ensure_loop_fixed_seed(loop: dict[str, Any], *, context: str) -> None:
    """Fail fast before scheduling trainable custom-evo loops without a seed."""

    if bool(loop.get("backtest_only")):
        return
    runtime_flags = dict(loop.get("runtime_flags") or {})
    seed_keys = ("random_seed", "seed", "loop_seed", "random_state", "torch_seed", "numpy_seed")
    containers: list[Mapping[str, Any]] = [
        runtime_flags,
        loop,
        loop.get("strategy_params") if isinstance(loop.get("strategy_params"), Mapping) else {},
        loop.get("model_params") if isinstance(loop.get("model_params"), Mapping) else {},
    ]
    if any(container.get(key) not in (None, "") for container in containers for key in seed_keys):
        return
    raise ValueError(f"{context}: runtime_flags.random_seed is required for trainable QE loops")


def _as_mapping(value: Any, *, context: str, errors: list[str]) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        errors.append(f"{context} must be an object")
        return {}
    return dict(value)


def _require_non_empty_string(value: Any, *, context: str, errors: list[str]) -> str | None:
    if value is None or str(value).strip() == "":
        errors.append(f"{context} is required")
        return None
    return str(value).strip()


def _require_string_list(value: Any, *, context: str, errors: list[str]) -> list[str]:
    if not isinstance(value, list):
        errors.append(f"{context} must be a non-empty list")
        return []
    normalized = [str(item).strip() for item in value if str(item or "").strip()]
    if len(normalized) != len(value) or not normalized:
        errors.append(f"{context} must contain non-empty strings")
    return normalized


def _validate_optional_mapping(value: Any, *, context: str, errors: list[str]) -> None:
    if value is not None and not isinstance(value, Mapping):
        errors.append(f"{context} must be an object")


def _validate_optional_seed(value: Any, *, context: str, errors: list[str]) -> None:
    if value in (None, ""):
        return
    try:
        int(value)
    except (TypeError, ValueError):
        errors.append(f"{context} must be an integer")


def _validate_optional_label_horizon(value: Any, *, context: str, errors: list[str]) -> None:
    if value in (None, ""):
        return
    try:
        horizon = int(value)
    except (TypeError, ValueError):
        errors.append(f"{context} must be one of {sorted(QE_LABEL_HORIZONS)}")
        return
    if horizon not in QE_LABEL_HORIZONS:
        errors.append(f"{context} must be one of {sorted(QE_LABEL_HORIZONS)}")


def _validate_node_parallelism(
    *,
    loops: list[dict[str, Any]],
    task_node_id: str | None,
    node_parallelism: Any,
    errors: list[str],
) -> dict[str, int]:
    if node_parallelism is None:
        return {}
    if not isinstance(node_parallelism, Mapping):
        errors.append("node_parallelism must be an object keyed by node_id")
        return {}

    selected_nodes: set[str] = set()
    first_node = str((loops[0].get("node_id") if loops else None) or task_node_id or "").strip()
    for loop in loops:
        node_id = str(loop.get("node_id") or first_node).strip()
        if node_id:
            selected_nodes.add(node_id)

    normalized: dict[str, int] = {}
    for raw_node, raw_limit in node_parallelism.items():
        node_id = str(raw_node or "").strip()
        if not node_id:
            errors.append("node_parallelism contains an empty node_id")
            continue
        if selected_nodes and node_id not in selected_nodes:
            errors.append(
                "node_parallelism contains node_id "
                f"{node_id!r} that is not selected by task node_id or any loop node_id"
            )
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            errors.append(f"node_parallelism[{node_id!r}] must be an integer")
            continue
        if limit < 1 or limit > MAX_QE_NODE_PARALLELISM:
            errors.append(
                f"node_parallelism[{node_id!r}] must be between 1 and {MAX_QE_NODE_PARALLELISM}"
            )
        normalized[node_id] = limit
    return normalized


def _validate_single_experiment_config(config: dict[str, Any], errors: list[str]) -> None:
    _require_string_list(config.get("factor_names"), context="single_experiment.factor_names", errors=errors)
    _require_non_empty_string(config.get("model_id"), context="single_experiment.model_id", errors=errors)
    custom_params = _as_mapping(config.get("custom_params"), context="single_experiment.custom_params", errors=errors)
    _validate_optional_mapping(config.get("data_split"), context="single_experiment.data_split", errors=errors)
    _validate_optional_mapping(config.get("factor_sources"), context="single_experiment.factor_sources", errors=errors)
    _validate_optional_seed(
        custom_params.get("random_seed", config.get("random_seed")),
        context="single_experiment.custom_params.random_seed",
        errors=errors,
    )
    _validate_optional_label_horizon(
        custom_params.get("label_horizon", config.get("label_horizon")),
        context="single_experiment.custom_params.label_horizon",
        errors=errors,
    )


def _validate_custom_evo_config(config: dict[str, Any], errors: list[str]) -> None:
    engine_mode = str(config.get("engine_mode") or "unified").strip().lower()
    if engine_mode != "unified":
        errors.append("custom_evo.engine_mode must be 'unified'")

    raw_loops = config.get("loops")
    if not isinstance(raw_loops, list) or not raw_loops:
        errors.append("custom_evo.loops must be a non-empty list")
        return

    loops: list[dict[str, Any]] = []
    for idx, raw_loop in enumerate(raw_loops, start=1):
        loop = _as_mapping(raw_loop, context=f"custom_evo.loops[{idx}]", errors=errors)
        if not loop:
            continue
        loops.append(loop)
        _require_string_list(loop.get("factor_keys"), context=f"custom_evo.loops[{idx}].factor_keys", errors=errors)
        _require_non_empty_string(loop.get("model_id"), context=f"custom_evo.loops[{idx}].model_id", errors=errors)
        _validate_optional_mapping(loop.get("strategy_params"), context=f"custom_evo.loops[{idx}].strategy_params", errors=errors)
        _validate_optional_mapping(loop.get("model_params"), context=f"custom_evo.loops[{idx}].model_params", errors=errors)
        _validate_optional_mapping(loop.get("runtime_flags"), context=f"custom_evo.loops[{idx}].runtime_flags", errors=errors)
        _validate_optional_mapping(loop.get("data_split"), context=f"custom_evo.loops[{idx}].data_split", errors=errors)
        _validate_optional_label_horizon(
            loop.get("label_horizon"),
            context=f"custom_evo.loops[{idx}].label_horizon",
            errors=errors,
        )
        if bool(loop.get("backtest_only")):
            _require_non_empty_string(
                loop.get("model_source_task_id"),
                context=f"custom_evo.loops[{idx}].model_source_task_id",
                errors=errors,
            )
            if loop.get("model_source_loop_index") in (None, ""):
                errors.append(f"custom_evo.loops[{idx}].model_source_loop_index is required")
            else:
                try:
                    if int(loop["model_source_loop_index"]) < 1:
                        errors.append(f"custom_evo.loops[{idx}].model_source_loop_index must be >= 1")
                except (TypeError, ValueError):
                    errors.append(f"custom_evo.loops[{idx}].model_source_loop_index must be an integer")
        else:
            try:
                _ensure_loop_fixed_seed(loop, context=f"custom_evo.loops[{idx}]")
            except ValueError as exc:
                errors.append(str(exc))

    normalized_parallelism = _validate_node_parallelism(
        loops=loops,
        task_node_id=str(config.get("node_id") or "").strip() or None,
        node_parallelism=config.get("node_parallelism"),
        errors=errors,
    )
    if config.get("node_parallelism") is not None:
        config["node_parallelism"] = normalized_parallelism


def _normalize_template_config(template_kind: str, config_json: dict[str, Any]) -> dict[str, Any]:
    config = dict(config_json or {})
    if template_kind == "custom_evo":
        loops = config.get("loops") or []
        if isinstance(loops, list):
            normalized: list[Any] = []
            for idx, raw_loop in enumerate(loops, start=1):
                if isinstance(raw_loop, Mapping):
                    loop = dict(raw_loop)
                    _ensure_loop_fixed_seed(loop, context=f"custom_evo.loops[{idx}]")
                    normalized.append(loop)
                else:
                    normalized.append(raw_loop)
            config["loops"] = normalized
    if template_kind == "single_experiment":
        custom_params = dict(config.get("custom_params") or {})
        if not custom_params.get("random_seed") and not config.get("random_seed"):
            raise ValueError("single_experiment.custom_params.random_seed is required")
        config["custom_params"] = custom_params
    return config


def _validate_experiment_config(
    template_kind: str,
    config_json: dict[str, Any],
    *,
    include_normalized: bool = False,
) -> dict[str, Any]:
    kind = str(template_kind or "").strip()
    errors: list[str] = []
    warnings: list[str] = []
    if kind not in QE_TEMPLATE_KINDS:
        errors.append(f"template_kind must be one of {sorted(QE_TEMPLATE_KINDS)}")
        kind = kind or "unknown"

    raw_config = _as_mapping(config_json or {}, context="config_json", errors=errors)
    try:
        normalized = _normalize_template_config(kind, raw_config) if kind in QE_TEMPLATE_KINDS else dict(raw_config)
    except ValueError as exc:
        errors.append(str(exc))
        normalized = dict(raw_config)

    if kind == "single_experiment":
        _validate_single_experiment_config(normalized, errors)
    elif kind == "custom_evo":
        _validate_custom_evo_config(normalized, errors)

    result: dict[str, Any] = {
        "ok": not errors,
        "valid": not errors,
        "validation_mode": "mcp_dry_run",
        "writes": False,
        "template_kind": kind,
        "errors": errors,
        "warnings": warnings,
    }
    if include_normalized:
        result["normalized_config"] = normalized
    return result


def _require_valid_experiment_config(template_kind: str, config_json: dict[str, Any]) -> dict[str, Any]:
    result = _validate_experiment_config(template_kind, config_json, include_normalized=True)
    if not result["valid"]:
        raise ValueError("QE config validation failed: " + "; ".join(result["errors"]))
    return dict(result["normalized_config"])


def register(registry: "ModuleRegistry") -> None:
    """Register QE Experiment tools on the shared MCP gateway."""

    client = registry.client()

    @registry.mcp.tool(name="qe_experiment_list")
    def qe_experiment_list(limit: int = 50, offset: int = 0, include_children: bool = False, detail: str = "summary") -> Any:
        _require_detail(detail)
        return client.get(
            "/quantevolver/experiments",
            params={"limit": limit, "offset": offset, "include_children": include_children, "detail": detail},
        )

    @registry.mcp.tool(name="qe_experiment_get")
    def qe_experiment_get(experiment_id: str, detail: str = "summary") -> Any:
        _require_detail(detail)
        safe = registry.sanitize(experiment_id, "experiment_id")
        return client.get(f"/quantevolver/experiments/{safe}", params={"detail": detail})

    @registry.mcp.tool(name="qe_experiment_get_status")
    def qe_experiment_get_status(experiment_id: str) -> Any:
        safe = registry.sanitize(experiment_id, "experiment_id")
        return client.get(f"/quantevolver/experiments/{safe}/run-status")

    @registry.mcp.tool(name="qe_experiment_get_logs_tail")
    def qe_experiment_get_logs_tail(experiment_id: str, tail: int = 500) -> Any:
        safe = registry.sanitize(experiment_id, "experiment_id")
        return client.get(f"/quantevolver/experiments/{safe}/logs/tail", params={"tail": sanitize_tail(tail)})

    @registry.mcp.tool(name="qe_experiment_get_enhanced_metrics")
    def qe_experiment_get_enhanced_metrics(experiment_id: str) -> Any:
        safe = registry.sanitize(experiment_id, "experiment_id")
        return client.get(f"/quantevolver/experiments/{safe}/enhanced-metrics")

    @registry.mcp.tool(name="qe_experiment_get_trade_stats")
    def qe_experiment_get_trade_stats(experiment_id: str) -> Any:
        safe = registry.sanitize(experiment_id, "experiment_id")
        return client.get(f"/quantevolver/experiments/{safe}/trade-stats")

    @registry.mcp.tool(name="qe_experiment_validate_config")
    def qe_experiment_validate_config(template_kind: str, config_json: dict[str, Any], include_normalized: bool = False) -> Any:
        return _validate_experiment_config(template_kind, config_json or {}, include_normalized=include_normalized)

    @registry.mcp.tool(name="qe_single_experiment_create_pending")
    def qe_single_experiment_create_pending(config_json: dict[str, Any], created_by_name: str | None = None, source_context_json: dict[str, Any] | None = None) -> Any:
        normalized_config = _require_valid_experiment_config("single_experiment", config_json or {})
        normalized_config["created_by_type"] = "mcp"
        normalized_config["created_by_name"] = created_by_name or "mcp_gateway"
        normalized_config["source_context_json"] = source_context_json
        return client.post("/quantevolver/experiments/pending", normalized_config)

    @registry.mcp.tool(name="qe_single_experiment_get_config")
    def qe_single_experiment_get_config(experiment_id: str) -> Any:
        safe = registry.sanitize(experiment_id, "experiment_id")
        return client.get(f"/quantevolver/experiments/{safe}/editable-config")

    @registry.mcp.tool(name="qe_single_experiment_update_config_confirmed")
    def qe_single_experiment_update_config_confirmed(experiment_id: str, config_json: dict[str, Any], confirm_update: str | None = None) -> Any:
        registry.confirm(confirm_update, QE_SINGLE_EXPERIMENT_UPDATE_CONFIG_CONFIRM, "confirm_update")
        normalized_config = _require_valid_experiment_config("single_experiment", config_json or {})
        safe = registry.sanitize(experiment_id, "experiment_id")
        return client.put(f"/quantevolver/experiments/{safe}/editable-config", normalized_config)

    @registry.mcp.tool(name="qe_experiment_run_confirmed")
    def qe_experiment_run_confirmed(experiment_id: str, node_id: str | None = None, confirm_run: str | None = None) -> Any:
        registry.confirm(confirm_run, QE_EXPERIMENT_RUN_CONFIRM, "confirm_run")
        safe = registry.sanitize(experiment_id, "experiment_id")
        safe_node = registry.sanitize(node_id, "node_id") if node_id else None
        return client.post(f"/quantevolver/experiments/{safe}/run", params={"engine_mode": "unified", "node_id": safe_node})

    @registry.mcp.tool(name="qe_experiment_stop_confirmed")
    def qe_experiment_stop_confirmed(experiment_id: str, confirm_stop: str | None = None) -> Any:
        registry.confirm(confirm_stop, QE_EXPERIMENT_STOP_CONFIRM, "confirm_stop")
        safe = registry.sanitize(experiment_id, "experiment_id")
        return client.post(f"/quantevolver/experiments/{safe}/stop")

    @registry.mcp.tool(name="qe_custom_evo_list_tasks")
    def qe_custom_evo_list_tasks(status: str | None = None, limit: int = 50, detail: str = "summary") -> Any:
        _require_detail(detail)
        return client.get("/quantevolver/evolution/tasks", params={"status": status, "limit": limit, "detail": detail})

    @registry.mcp.tool(name="qe_custom_evo_get_task")
    def qe_custom_evo_get_task(task_id: str, detail: str = "summary") -> Any:
        _require_detail(detail)
        safe = registry.sanitize(task_id, "task_id")
        return client.get(f"/quantevolver/evolution/tasks/{safe}", params={"detail": detail})

    @registry.mcp.tool(name="qe_custom_evo_loop_comparison")
    def qe_custom_evo_loop_comparison(task_id: str) -> Any:
        safe = registry.sanitize(task_id, "task_id")
        return client.get(f"/quantevolver/evolution/tasks/{safe}/loops/comparison")

    @registry.mcp.tool(name="qe_custom_evo_get_loop_config")
    def qe_custom_evo_get_loop_config(task_id: str, loop_index: int) -> Any:
        safe = registry.sanitize(task_id, "task_id")
        return client.get(f"/quantevolver/evolution/tasks/{safe}/loops/{_require_positive_loop_index(loop_index)}/config")

    @registry.mcp.tool(name="qe_custom_evo_get_loop_metrics")
    def qe_custom_evo_get_loop_metrics(task_id: str, loop_index: int) -> Any:
        safe = registry.sanitize(task_id, "task_id")
        return client.get(f"/quantevolver/evolution/tasks/{safe}/loops/{_require_positive_loop_index(loop_index)}/metrics")

    @registry.mcp.tool(name="qe_custom_evo_get_loop_analysis")
    def qe_custom_evo_get_loop_analysis(task_id: str, loop_index: int) -> Any:
        safe = registry.sanitize(task_id, "task_id")
        return client.get(f"/quantevolver/evolution/tasks/{safe}/loops/{_require_positive_loop_index(loop_index)}/analysis")

    @registry.mcp.tool(name="qe_custom_evo_get_config")
    def qe_custom_evo_get_config(task_id: str) -> Any:
        safe = registry.sanitize(task_id, "task_id")
        return client.get(f"/quantevolver/evolution/tasks/{safe}/custom-evo-config")

    @registry.mcp.tool(name="qe_custom_evo_get_logs_tail")
    def qe_custom_evo_get_logs_tail(task_id: str, tail: int = 500) -> Any:
        safe = registry.sanitize(task_id, "task_id")
        return client.get(f"/quantevolver/evolution/tasks/{safe}/logs/tail", params={"tail": sanitize_tail(tail)})

    @registry.mcp.tool(name="qe_custom_evo_create_pending")
    def qe_custom_evo_create_pending(task_name: str, loops: list[dict[str, Any]], target_desc: str = "", node_id: str | None = None, node_parallelism: dict[str, int] | None = None, engine_mode: str = "unified", clone_from_task_id: str | None = None, phase_pipeline_enabled: bool = False, resource_telemetry_enabled: bool = False) -> Any:
        normalized_config = _require_valid_experiment_config(
            "custom_evo",
            {
                "loops": loops or [],
                "node_id": node_id,
                "node_parallelism": node_parallelism,
                "engine_mode": engine_mode,
                "phase_pipeline_enabled": phase_pipeline_enabled,
                "resource_telemetry_enabled": False,
            },
        )
        safe_node = registry.sanitize(node_id, "node_id") if node_id else None
        return client.post(
            "/quantevolver/evolution/custom-tasks",
            {
                "task_name": task_name,
                "target_desc": target_desc,
                "loops": normalized_config.get("loops") or [],
                "node_id": safe_node,
                "node_parallelism": normalized_config.get("node_parallelism"),
                "engine_mode": normalized_config.get("engine_mode") or "unified",
                "auto_start": False,
                "clone_from_task_id": clone_from_task_id,
                "phase_pipeline_enabled": bool(normalized_config.get("phase_pipeline_enabled", False)),
                "resource_telemetry_enabled": bool(normalized_config.get("resource_telemetry_enabled", False)),
            },
        )

    @registry.mcp.tool(name="qe_custom_evo_update_config_confirmed")
    def qe_custom_evo_update_config_confirmed(task_id: str, task_name: str, loops: list[dict[str, Any]], confirm_update: str | None = None, target_desc: str = "", node_id: str | None = None, node_parallelism: dict[str, int] | None = None, engine_mode: str = "unified", phase_pipeline_enabled: bool = False, resource_telemetry_enabled: bool = False) -> Any:
        registry.confirm(confirm_update, QE_CUSTOM_EVO_UPDATE_CONFIG_CONFIRM, "confirm_update")
        normalized_config = _require_valid_experiment_config(
            "custom_evo",
            {
                "loops": loops or [],
                "node_id": node_id,
                "node_parallelism": node_parallelism,
                "engine_mode": engine_mode,
                "phase_pipeline_enabled": phase_pipeline_enabled,
                "resource_telemetry_enabled": False,
            },
        )
        safe = registry.sanitize(task_id, "task_id")
        safe_node = registry.sanitize(node_id, "node_id") if node_id else None
        return client.put(
            f"/quantevolver/evolution/tasks/{safe}/custom-evo-config",
            {
                "task_name": task_name,
                "target_desc": target_desc,
                "loops": normalized_config.get("loops") or [],
                "node_id": safe_node,
                "node_parallelism": normalized_config.get("node_parallelism"),
                "engine_mode": normalized_config.get("engine_mode") or "unified",
                "phase_pipeline_enabled": bool(normalized_config.get("phase_pipeline_enabled", False)),
                "resource_telemetry_enabled": bool(normalized_config.get("resource_telemetry_enabled", False)),
            },
        )

    @registry.mcp.tool(name="qe_custom_evo_run_confirmed")
    def qe_custom_evo_run_confirmed(task_id: str, force_full_train: bool = False, confirm_custom_evo: str | None = None) -> Any:
        registry.confirm(confirm_custom_evo, QE_CUSTOM_EVO_RUN_CONFIRM, "confirm_custom_evo")
        safe = registry.sanitize(task_id, "task_id")
        return client.post(
            f"/quantevolver/evolution/tasks/{safe}/custom-evo/run",
            {"confirm_custom_evo": QE_CUSTOM_EVO_RUN_CONFIRM, "force_full_train": force_full_train},
        )

    @registry.mcp.tool(name="qe_custom_evo_delete_confirmed")
    def qe_custom_evo_delete_confirmed(task_id: str, confirm_delete: str | None = None) -> Any:
        registry.confirm(confirm_delete, QE_CUSTOM_EVO_DELETE_CONFIRM, "confirm_delete")
        safe = registry.sanitize(task_id, "task_id")
        return client.delete(f"/quantevolver/evolution/tasks/{safe}")

    @registry.mcp.tool(name="qe_custom_evo_retry_loop_confirmed")
    def qe_custom_evo_retry_loop_confirmed(task_id: str, loop_index: int, retry_mode: str = "auto", confirm_retry: str | None = None) -> Any:
        registry.confirm(confirm_retry, "QE_CUSTOM_EVO_RETRY", "confirm_retry")
        safe = registry.sanitize(task_id, "task_id")
        return client.post(
            f"/quantevolver/evolution/tasks/{safe}/loops/{_require_positive_loop_index(loop_index)}/retry",
            {"retry_mode": retry_mode},
        )

    @registry.mcp.tool(name="qe_custom_evo_rerun_loop_confirmed")
    def qe_custom_evo_rerun_loop_confirmed(task_id: str, loop_index: int, loop: dict[str, Any], confirm_rerun: str | None = None, phase_pipeline_enabled: bool | None = None, resource_telemetry_enabled: bool | None = None) -> Any:
        registry.confirm(confirm_rerun, "QE_CUSTOM_EVO_RERUN", "confirm_rerun")
        loop_payload = dict(loop or {})
        _ensure_loop_fixed_seed(loop_payload, context="qe_custom_evo_rerun_loop_confirmed.loop")
        safe = registry.sanitize(task_id, "task_id")
        return client.post(
            f"/quantevolver/evolution/tasks/{safe}/loops/{_require_positive_loop_index(loop_index)}/rerun",
            {
                "loop": loop_payload,
                "confirm_delete_old_result": True,
                "phase_pipeline_enabled": phase_pipeline_enabled,
                "resource_telemetry_enabled": False,
            },
        )

    @registry.mcp.tool(name="qe_custom_evo_append_loops_confirmed")
    def qe_custom_evo_append_loops_confirmed(task_id: str, loops: list[dict[str, Any]], confirm_append: str | None = None, phase_pipeline_enabled: bool | None = None, resource_telemetry_enabled: bool | None = None) -> Any:
        registry.confirm(confirm_append, "QE_CUSTOM_EVO_APPEND", "confirm_append")
        loop_payloads = [dict(loop or {}) for loop in (loops or [])]
        for idx, loop in enumerate(loop_payloads, start=1):
            _ensure_loop_fixed_seed(loop, context=f"qe_custom_evo_append_loops_confirmed.loops[{idx}]")
        safe = registry.sanitize(task_id, "task_id")
        return client.post(
            f"/quantevolver/evolution/tasks/{safe}/custom-loops/append",
            {
                "loops": loop_payloads,
                "ack_failed_loop_warning": True,
                "phase_pipeline_enabled": phase_pipeline_enabled,
                "resource_telemetry_enabled": False,
            },
        )

    @registry.mcp.tool(name="qe_template_create")
    def qe_template_create(
        template_kind: str,
        title: str,
        config_json: dict[str, Any],
        archive_policy: str = "AUTO",
        description: str | None = None,
    ) -> Any:
        normalized_config = _require_valid_experiment_config(template_kind, config_json or {})
        return client.post(
            "/qe-templates",
            {
                "template_kind": template_kind,
                "title": title,
                "description": description,
                "config_json": normalized_config,
                "archive_policy": archive_policy,
            },
        )

    @registry.mcp.tool(name="qe_template_get")
    def qe_template_get(template_id: str) -> Any:
        safe = registry.sanitize(template_id, "template_id")
        return client.get(f"/qe-templates/{safe}")

    @registry.mcp.tool(name="qe_template_validate")
    def qe_template_validate(template_id: str) -> Any:
        safe = registry.sanitize(template_id, "template_id")
        return client.post(f"/qe-templates/{safe}/validate")

    @registry.mcp.tool(name="qe_template_materialize_confirmed")
    def qe_template_materialize_confirmed(template_id: str, confirm_template: str | None = None) -> Any:
        registry.confirm(confirm_template, QE_TEMPLATE_MATERIALIZE_CONFIRM, "confirm_template")
        safe = registry.sanitize(template_id, "template_id")
        return client.post(f"/qe-templates/{safe}/materialize", {"confirm_template": QE_TEMPLATE_MATERIALIZE_CONFIRM})

    @registry.mcp.tool(name="qe_template_delete_confirmed")
    def qe_template_delete_confirmed(template_id: str, confirm_delete: str | None = None) -> Any:
        registry.confirm(confirm_delete, QE_TEMPLATE_DELETE_CONFIRM, "confirm_delete")
        safe = registry.sanitize(template_id, "template_id")
        return client.delete(f"/qe-templates/{safe}", {"confirm_delete": QE_TEMPLATE_DELETE_CONFIRM})

    @registry.mcp.tool(name="qe_template_run_confirmed")
    def qe_template_run_confirmed(template_id: str, confirm_run: str | None = None, node_id: str | None = None) -> Any:
        safe = registry.sanitize(template_id, "template_id")
        safe_node = registry.sanitize(node_id, "node_id") if node_id else None
        if confirm_run not in {QE_EXPERIMENT_RUN_CONFIRM, QE_CUSTOM_EVO_RUN_CONFIRM}:
            raise ValueError("confirm_run must equal QE_EXPERIMENT_RUN or QE_CUSTOM_EVO_RUN")
        return client.post(f"/qe-templates/{safe}/run", {"confirm_run": confirm_run, "node_id": safe_node})

    @registry.mcp.tool(name="qe_template_create_and_run_confirmed")
    def qe_template_create_and_run_confirmed(
        template_kind: str,
        title: str,
        config_json: dict[str, Any],
        confirm_direct_run: str | None = None,
        archive_policy: str = "AUTO",
        description: str | None = None,
        node_id: str | None = None,
        force_full_train: bool = False,
        approved_by: str = "mcp_gateway",
        approval_note: str | None = None,
    ) -> Any:
        registry.confirm(confirm_direct_run, QE_TEMPLATE_CREATE_AND_RUN_CONFIRM, "confirm_direct_run")
        normalized_config = _require_valid_experiment_config(template_kind, config_json or {})
        safe_node = registry.sanitize(node_id, "node_id") if node_id else None
        return client.post(
            "/qe-templates/create-and-run",
            {
                "template_kind": template_kind,
                "title": title,
                "description": description,
                "config_json": normalized_config,
                "archive_policy": archive_policy,
                "confirm_direct_run": QE_TEMPLATE_CREATE_AND_RUN_CONFIRM,
                "node_id": safe_node,
                "force_full_train": force_full_train,
                "approved_by": approved_by,
                "approval_note": approval_note,
            },
        )

    registry.register_tool_count("qe_experiment", TOOL_COUNT)
