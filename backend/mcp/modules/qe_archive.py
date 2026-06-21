"""QE Archive MCP tool wrappers for the unified gateway."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


QE_ARCHIVE_BACKFILL_CONFIRM = "QE_ARCHIVE_BACKFILL"
QE_ARCHIVE_WRITE_CONFIRM = "QE_ARCHIVE_WRITE"
QE_ARCHIVE_WORKER_CONFIRM = "QE_ARCHIVE_WORKER_RUN"
MULTI_ALPHA_COMBINE_BACKTEST_CONFIRM = "MULTI_ALPHA_COMBINE_BACKTEST_RUN"

TOOL_NAMES = (
    "qe_archive_health",
    "qe_archive_list_runs",
    "qe_archive_get_run_quality",
    "qe_archive_list_outbox",
    "qe_archive_list_jobs",
    "qe_archive_list_skips",
    "qe_archive_backfill_preview",
    "qe_archive_backfill_execute_confirmed",
    "qe_archive_backfill_selection_preview",
    "qe_archive_backfill_selection_execute_confirmed",
    "qe_archive_get_source_status",
    "qe_archive_list_backfill_runs",
    "qe_archive_get_backfill_run",
    "qe_archive_worker_run_once_confirmed",
    "qe_archive_query_factor_usage",
    "qe_archive_query_factor_importance",
    "qe_archive_query_factor_importance_stability",
    "qe_archive_query_model_trials",
    "qe_archive_query_seed_trials",
    "qe_archive_query_hyperparam_history",
    "qe_archive_query_analytics_view_status",
    "qe_archive_query_run_leaderboard",
    "qe_archive_query_topk_quality",
    "qe_archive_query_seed_robustness",
    "qe_archive_query_factor_performance",
    "qe_archive_query_model_hyperparam_seed_perf",
    "qe_archive_query_overfit_flags",
    "qe_archive_query_promotion_candidates",
    "qe_archive_query_evolution_lineage",
    "multi_alpha_orthogonality",
    "multi_alpha_combine_preview",
    "multi_alpha_combine_backtest_run_confirmed",
    "multi_alpha_combine_backtest_result_get",
    "multi_alpha_combine_backtest_list",
    "prediction_store_get_pointer",
    "prediction_store_pull_pred",
    "prediction_store_pull_label",
    "model_store_health",
)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry: "ModuleRegistry") -> None:
    """Register QE Archive tools on the shared MCP gateway."""

    client = registry.client("qe-archive")
    prediction_store_client = registry.client("prediction-store")
    multi_alpha_client = registry.client("multi-alpha")

    def _sanitize_ids(values: list[str] | None, field_name: str) -> list[str]:
        return [registry.sanitize(value, field_name) for value in (values or []) if str(value or "").strip()]

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
        return {
            "source_mode": "specific_ids",
            "experiment_ids": _sanitize_ids(experiment_ids, "experiment_id"),
            "task_ids": _sanitize_ids(task_ids, "task_id"),
            "loop_ids": _sanitize_ids(loop_ids, "loop_id"),
            "task_id": registry.sanitize(task_id, "task_id") if task_id else None,
            "loop_indices": _positive_indices(loop_indices),
            "status": status,
            "include_archived": include_archived,
            "requested_by": "qe_archive_mcp",
        }

    @registry.mcp.tool(name="qe_archive_health")
    def qe_archive_health() -> Any:
        return client.get("/health")

    @registry.mcp.tool(name="qe_archive_list_runs")
    def qe_archive_list_runs(status: str | None = None, run_type: str | None = None, search: str | None = None, limit: int = 20) -> Any:
        return client.get("/runs", params={"status": status, "run_type": run_type, "search": search, "limit": limit})

    @registry.mcp.tool(name="qe_archive_get_run_quality")
    def qe_archive_get_run_quality(run_id: str) -> Any:
        safe = registry.sanitize(run_id, "run_id")
        return client.get(f"/runs/{safe}/quality")

    @registry.mcp.tool(name="qe_archive_list_outbox")
    def qe_archive_list_outbox(status: str | None = None, limit: int = 20) -> Any:
        return client.get("/outbox", params={"status": status, "limit": limit})

    @registry.mcp.tool(name="qe_archive_list_jobs")
    def qe_archive_list_jobs(status: str | None = None, limit: int = 20) -> Any:
        return client.get("/jobs", params={"status": status, "limit": limit})

    @registry.mcp.tool(name="qe_archive_list_skips")
    def qe_archive_list_skips(archive_policy: str | None = None, source_type: str | None = None, limit: int = 20) -> Any:
        return client.get("/skips", params={"archive_policy": archive_policy, "source_type": source_type, "limit": limit})

    @registry.mcp.tool(name="qe_archive_backfill_preview")
    def qe_archive_backfill_preview(
        source_mode: str = "completed_custom_evo_loops",
        limit: int = 20,
        include_archived: bool = False,
    ) -> Any:
        return client.post("/backfill/preview", {"source_mode": source_mode, "limit": limit, "include_archived": include_archived})

    @registry.mcp.tool(name="qe_archive_backfill_execute_confirmed")
    def qe_archive_backfill_execute_confirmed(
        source_mode: str = "completed_custom_evo_loops",
        limit: int = 20,
        include_archived: bool = False,
        confirm_backfill: str | None = None,
    ) -> Any:
        registry.confirm(confirm_backfill, QE_ARCHIVE_BACKFILL_CONFIRM, "confirm_backfill")
        return client.post(
            "/backfill/execute",
            {
                "source_mode": source_mode,
                "limit": limit,
                "include_archived": include_archived,
                "confirm_backfill": QE_ARCHIVE_BACKFILL_CONFIRM,
            },
        )

    @registry.mcp.tool(name="qe_archive_backfill_selection_preview")
    def qe_archive_backfill_selection_preview(
        experiment_ids: list[str] | None = None,
        task_ids: list[str] | None = None,
        loop_ids: list[str] | None = None,
        task_id: str | None = None,
        loop_indices: list[int] | None = None,
        status: str = "completed",
        include_archived: bool = False,
    ) -> Any:
        return client.post(
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

    @registry.mcp.tool(name="qe_archive_backfill_selection_execute_confirmed")
    def qe_archive_backfill_selection_execute_confirmed(
        experiment_ids: list[str] | None = None,
        task_ids: list[str] | None = None,
        loop_ids: list[str] | None = None,
        task_id: str | None = None,
        loop_indices: list[int] | None = None,
        status: str = "completed",
        include_archived: bool = False,
        confirm_write: str | None = None,
    ) -> Any:
        registry.confirm(confirm_write, QE_ARCHIVE_WRITE_CONFIRM, "confirm_write")
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
        return client.post("/backfill/execute", payload)

    @registry.mcp.tool(name="qe_archive_get_source_status")
    def qe_archive_get_source_status(
        experiment_ids: list[str] | None = None,
        task_ids: list[str] | None = None,
        loop_ids: list[str] | None = None,
        include_recommendation: bool = True,
    ) -> Any:
        return client.post(
            "/source-status",
            {
                "experiment_ids": _sanitize_ids(experiment_ids, "experiment_id"),
                "task_ids": _sanitize_ids(task_ids, "task_id"),
                "loop_ids": _sanitize_ids(loop_ids, "loop_id"),
                "include_recommendation": include_recommendation,
            },
        )

    @registry.mcp.tool(name="qe_archive_list_backfill_runs")
    def qe_archive_list_backfill_runs(status: str | None = None, limit: int = 20) -> Any:
        return client.get("/backfill/runs", params={"status": status, "limit": limit})

    @registry.mcp.tool(name="qe_archive_get_backfill_run")
    def qe_archive_get_backfill_run(backfill_run_id: str) -> Any:
        safe = registry.sanitize(backfill_run_id, "backfill_run_id")
        return client.get(f"/backfill/runs/{safe}")

    @registry.mcp.tool(name="qe_archive_worker_run_once_confirmed")
    def qe_archive_worker_run_once_confirmed(limit: int = 10, worker_id: str = "qe_archive_mcp_worker", confirm_run: str | None = None) -> Any:
        registry.confirm(confirm_run, QE_ARCHIVE_WORKER_CONFIRM, "confirm_run")
        safe_worker = registry.sanitize(worker_id, "worker_id")
        return client.post("/worker/run-once", {"limit": limit, "worker_id": safe_worker, "confirm_run": QE_ARCHIVE_WORKER_CONFIRM})

    @registry.mcp.tool(name="qe_archive_query_factor_usage")
    def qe_archive_query_factor_usage(limit: int = 20, min_runs: int = 1) -> Any:
        return client.get("/query/factor-usage", params={"limit": limit, "min_runs": min_runs})

    @registry.mcp.tool(name="qe_archive_query_factor_importance")
    def qe_archive_query_factor_importance(
        run_id: str | None = None,
        task_id: str | None = None,
        loop_index: int | None = None,
        factor_name: str | None = None,
        method: str | None = None,
        limit: int = 10,
        order: str = "desc",
    ) -> Any:
        return client.get(
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

    @registry.mcp.tool(name="qe_archive_query_factor_importance_stability")
    def qe_archive_query_factor_importance_stability(
        factor_name: str | None = None,
        method: str | None = None,
        min_runs: int = 2,
        limit: int = 10,
    ) -> Any:
        return client.get(
            "/query/factor-importance/stability",
            params={"factor_name": factor_name, "method": method, "min_runs": min_runs, "limit": limit},
        )

    @registry.mcp.tool(name="qe_archive_query_model_trials")
    def qe_archive_query_model_trials(model_type: str | None = None, limit: int = 20) -> Any:
        return client.get("/query/model-trials", params={"model_type": model_type, "limit": limit})

    @registry.mcp.tool(name="qe_archive_query_seed_trials")
    def qe_archive_query_seed_trials(limit: int = 20) -> Any:
        return client.get("/query/seed-trials", params={"limit": limit})

    @registry.mcp.tool(name="qe_archive_query_hyperparam_history")
    def qe_archive_query_hyperparam_history(model_type: str | None = None, param_key: str | None = None, limit: int = 20) -> Any:
        return client.get("/query/hyperparams", params={"model_type": model_type, "param_key": param_key, "limit": limit})

    @registry.mcp.tool(name="qe_archive_query_analytics_view_status")
    def qe_archive_query_analytics_view_status() -> Any:
        return client.get("/analytics/views")

    @registry.mcp.tool(name="qe_archive_query_run_leaderboard")
    def qe_archive_query_run_leaderboard(
        model_type: str | None = None,
        min_icir: float | None = None,
        min_ir: float | None = None,
        limit: int = 20,
        order_by: str = "calmar",
    ) -> Any:
        return client.get(
            "/analytics/run-leaderboard",
            params={"model_type": model_type, "min_icir": min_icir, "min_ir": min_ir, "limit": limit, "order_by": order_by},
        )

    @registry.mcp.tool(name="qe_archive_query_topk_quality")
    def qe_archive_query_topk_quality(
        run_id: str | None = None,
        task_id: str | None = None,
        k: int | None = None,
        limit: int = 20,
    ) -> Any:
        return client.get(
            "/analytics/topk-quality",
            params={"run_id": run_id, "task_id": task_id, "k": k, "limit": limit},
        )

    @registry.mcp.tool(name="qe_archive_query_seed_robustness")
    def qe_archive_query_seed_robustness(
        model_type: str | None = None,
        min_seed_count: int = 2,
        stable_only: bool = False,
        limit: int = 20,
        order_by: str = "cagr_mean",
    ) -> Any:
        return client.get(
            "/analytics/seed-robustness",
            params={
                "model_type": model_type,
                "min_seed_count": min_seed_count,
                "stable_only": stable_only,
                "limit": limit,
                "order_by": order_by,
            },
        )

    @registry.mcp.tool(name="qe_archive_query_factor_performance")
    def qe_archive_query_factor_performance(
        factor_name: str | None = None,
        min_runs: int = 1,
        limit: int = 20,
        order_by: str = "best_cagr",
    ) -> Any:
        return client.get(
            "/analytics/factor-performance",
            params={"factor_name": factor_name, "min_runs": min_runs, "limit": limit, "order_by": order_by},
        )

    @registry.mcp.tool(name="qe_archive_query_model_hyperparam_seed_perf")
    def qe_archive_query_model_hyperparam_seed_perf(
        model_type: str | None = None,
        hyperparam_hash: str | None = None,
        limit: int = 20,
        order_by: str = "cagr",
    ) -> Any:
        return client.get(
            "/analytics/model-hyperparam-seed-perf",
            params={"model_type": model_type, "hyperparam_hash": hyperparam_hash, "limit": limit, "order_by": order_by},
        )

    @registry.mcp.tool(name="qe_archive_query_overfit_flags")
    def qe_archive_query_overfit_flags(suspicious_only: bool = True, model_type: str | None = None, limit: int = 20) -> Any:
        return client.get("/analytics/overfit-flags", params={"suspicious_only": suspicious_only, "model_type": model_type, "limit": limit})

    @registry.mcp.tool(name="qe_archive_query_promotion_candidates")
    def qe_archive_query_promotion_candidates(
        model_type: str | None = None,
        min_seed_count: int = 5,
        limit: int = 20,
        order_by: str = "calmar",
    ) -> Any:
        return client.get(
            "/analytics/promotion-candidates",
            params={"model_type": model_type, "min_seed_count": min_seed_count, "limit": limit, "order_by": order_by},
        )

    @registry.mcp.tool(name="qe_archive_query_evolution_lineage")
    def qe_archive_query_evolution_lineage(
        task_id: str | None = None,
        experiment_id: str | None = None,
        model_type: str | None = None,
        limit: int = 50,
    ) -> Any:
        return client.get(
            "/analytics/evolution-lineage",
            params={"task_id": task_id, "experiment_id": experiment_id, "model_type": model_type, "limit": limit},
        )

    @registry.mcp.tool(name="multi_alpha_orthogonality")
    def multi_alpha_orthogonality(run_ids: list[str], k: int = 25) -> Any:
        safe_run_ids = _sanitize_ids(run_ids, "run_id")
        bounded_k = max(1, min(int(k), 500))
        return multi_alpha_client.get("/orthogonality", params={"run_ids": safe_run_ids, "k": bounded_k})

    @registry.mcp.tool(name="multi_alpha_combine_preview")
    def multi_alpha_combine_preview(
        legs: list[dict[str, Any]],
        weighting_scheme: str = "equal",
        normalize_method: str = "zscore",
        walk_forward: dict[str, Any] | None = None,
        head: int = 20,
    ) -> Any:
        bounded_head = max(0, min(int(head), 1000))
        return multi_alpha_client.post(
            "/combine/preview",
            {
                "legs": legs,
                "weighting_scheme": weighting_scheme,
                "normalize_method": normalize_method,
                "walk_forward": walk_forward,
                "head": bounded_head,
            },
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_run_confirmed")
    def multi_alpha_combine_backtest_run_confirmed(
        action: str,
        payload: dict[str, Any],
        confirm_run: str | None = None,
    ) -> Any:
        registry.confirm(confirm_run, MULTI_ALPHA_COMBINE_BACKTEST_CONFIRM, "confirm_run")
        normalized_action = str(action or "").strip().lower()
        if normalized_action not in {"submit", "run"}:
            raise ValueError("action must be 'submit' or 'run'")
        body = dict(payload or {})
        body["run_async"] = bool(body.get("run_async", True))
        return multi_alpha_client.post("/combine-backtest/run", body)

    @registry.mcp.tool(name="multi_alpha_combine_backtest_result_get")
    def multi_alpha_combine_backtest_result_get(run_id: str) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        return multi_alpha_client.get(f"/combine-backtest/runs/{safe_run_id}")

    @registry.mcp.tool(name="multi_alpha_combine_backtest_list")
    def multi_alpha_combine_backtest_list(status: str | None = None, limit: int = 20) -> Any:
        bounded_limit = max(1, min(int(limit), 200))
        return multi_alpha_client.get("/combine-backtest/runs", params={"status": status, "limit": bounded_limit})

    @registry.mcp.tool(name="prediction_store_get_pointer")
    def prediction_store_get_pointer(run_id: str | None = None, experiment_id: str | None = None) -> Any:
        if run_id:
            safe_run_id = registry.sanitize(run_id, "run_id")
            return prediction_store_client.get(f"/pointers/{safe_run_id}", params={"experiment_id": experiment_id})
        if experiment_id:
            safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
            return prediction_store_client.get(f"/pointers/by-experiment/{safe_experiment_id}")
        raise ValueError("run_id or experiment_id is required")

    @registry.mcp.tool(name="prediction_store_pull_pred")
    def prediction_store_pull_pred(run_id: str, head: int = 5) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        bounded_head = max(0, min(int(head), 1000))
        return prediction_store_client.get(f"/pred/{safe_run_id}", params={"head": bounded_head})

    @registry.mcp.tool(name="prediction_store_pull_label")
    def prediction_store_pull_label(run_id: str) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        return prediction_store_client.get(f"/label/{safe_run_id}")

    @registry.mcp.tool(name="model_store_health")
    def model_store_health() -> Any:
        return prediction_store_client.get("/health")

    registry.register_tool_count("qe_archive", TOOL_COUNT)
