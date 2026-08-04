"""QE Archive MCP tool wrappers for the unified gateway."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

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
    "qe_archive_query_long_trend_quality",
    "qe_archive_query_run_leaderboard",
    "qe_archive_query_topk_quality",
    "qe_archive_query_seed_robustness",
    "qe_archive_query_factor_performance",
    "qe_archive_query_model_hyperparam_seed_perf",
    "qe_archive_query_overfit_flags",
    "qe_archive_query_promotion_candidates",
    "qe_archive_query_evolution_lineage",
    "qe_archive_query_resource_phases",
    "multi_alpha_orthogonality",
    "multi_alpha_combine_preview",
    "multi_alpha_combine_backtest_run_confirmed",
    "multi_alpha_combine_backtest_result_get",
    "multi_alpha_combine_backtest_archive_detail_get",
    "multi_alpha_combine_backtest_list",
    "multi_alpha_combine_backtest_controls_get",
    "multi_alpha_combine_backtest_children_list",
    "multi_alpha_combine_backtest_child_get",
    "multi_alpha_combine_backtest_child_attempts_list",
    "multi_alpha_combine_backtest_commands_list",
    "multi_alpha_combine_backtest_command_get",
    "multi_alpha_combine_backtest_recovery_preview",
    "multi_alpha_combine_backtest_pause",
    "multi_alpha_combine_backtest_resume",
    "multi_alpha_combine_backtest_cancel",
    "multi_alpha_combine_backtest_stop",
    "multi_alpha_combine_backtest_reconcile",
    "multi_alpha_combine_backtest_attempt_cancel",
    "multi_alpha_combine_backtest_child_recovery_execute",
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

    def _idempotency_headers(idempotency_key: str) -> dict[str, str]:
        normalized = str(idempotency_key or "").strip()
        if not normalized:
            raise ValueError("idempotency_key is required for durable multi-alpha mutation")
        if len(normalized) > 256 or any(ord(char) < 33 or ord(char) > 126 for char in normalized):
            raise ValueError("idempotency_key must be a visible ASCII token no longer than 256 characters")
        return {"Idempotency-Key": normalized}

    def _multi_alpha_post_with_idempotency(
        path: str,
        payload: dict[str, Any],
        *,
        idempotency_key: str,
    ) -> Any:
        """Keep the P0-2 header contract local to this MCP module."""

        with multi_alpha_client._client() as http_client:
            response = http_client.post(
                path,
                json=payload,
                headers=_idempotency_headers(idempotency_key),
            )
        return multi_alpha_client._decode(response, "POST", path)

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

    @registry.mcp.tool(name="qe_archive_query_long_trend_quality")
    def qe_archive_query_long_trend_quality(
        evaluation_id: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        loop_index: int | None = None,
        model_type: str | None = None,
        label_horizon: Literal[20, 40, 60, 120, 180] | None = None,
        evaluation_asof: str | None = None,
        outcome_dataset_snapshot_id: str | None = None,
        horizon: Literal[20, 40, 60, 120, 180] | None = None,
        sector_code: str | None = None,
        family_status: Literal["COMPUTED", "COMPUTED_WITH_LIMITATIONS", "NOT_COMPUTABLE", "NOT_VERIFIABLE"]
        | None = None,
        entry_execution_status: Literal[
            "filled_t1", "partial_fill_t1", "delayed_fill", "never_filled",
            "not_attempted_by_strategy", "not_verifiable",
        ]
        | None = None,
        exit_execution_status: Literal[
            "filled_on_exit_signal_day", "delayed_exit", "never_exited",
            "not_attempted_by_strategy", "not_verifiable",
        ]
        | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> Any:
        """Read bounded compact F-014 quality rows; never returns inline Parquet."""

        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        return client.get(
            "/analytics/long-trend-quality",
            params={
                "evaluation_id": evaluation_id,
                "run_id": run_id,
                "task_id": task_id,
                "loop_index": loop_index,
                "model_type": model_type,
                "label_horizon": label_horizon,
                "evaluation_asof": evaluation_asof,
                "outcome_dataset_snapshot_id": outcome_dataset_snapshot_id,
                "horizon": horizon,
                "sector_code": sector_code,
                "family_status": family_status,
                "entry_execution_status": entry_execution_status,
                "exit_execution_status": exit_execution_status,
                "limit": limit,
                "cursor": cursor,
            },
        )

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

    @registry.mcp.tool(name="qe_archive_query_resource_phases")
    def qe_archive_query_resource_phases(
        run_id: str | None = None,
        task_id: str | None = None,
        loop_index: int | None = None,
        source_run_key: str | None = None,
        limit: int = 20,
    ) -> Any:
        return client.get(
            "/resource-phases",
            params={
                "run_id": run_id,
                "task_id": task_id,
                "loop_index": loop_index,
                "source_run_key": source_run_key,
                "limit": max(1, min(int(limit), 200)),
            },
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

    @registry.mcp.tool(name="multi_alpha_combine_backtest_archive_detail_get")
    def multi_alpha_combine_backtest_archive_detail_get(run_id: str) -> Any:
        """Read the immutable Archive snapshot, including P0-2 recovery evidence."""

        safe_run_id = registry.sanitize(run_id, "run_id")
        return multi_alpha_client.get(f"/combine-backtest/runs/{safe_run_id}/archive-detail")

    @registry.mcp.tool(name="multi_alpha_combine_backtest_list")
    def multi_alpha_combine_backtest_list(status: str | None = None, limit: int = 20) -> Any:
        bounded_limit = max(1, min(int(limit), 200))
        return multi_alpha_client.get("/combine-backtest/runs", params={"status": status, "limit": bounded_limit})

    @registry.mcp.tool(name="multi_alpha_combine_backtest_controls_get")
    def multi_alpha_combine_backtest_controls_get(
        run_id: str,
        child_id: str | None = None,
        attempt_id: str | None = None,
    ) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        params = {
            "child_id": registry.sanitize(child_id, "child_id") if child_id else None,
            "attempt_id": registry.sanitize(attempt_id, "attempt_id") if attempt_id else None,
        }
        return multi_alpha_client.get(
            f"/combine-backtest/runs/{safe_run_id}/control-capabilities",
            params=params,
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_children_list")
    def multi_alpha_combine_backtest_children_list(run_id: str) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        return multi_alpha_client.get(f"/combine-backtest/runs/{safe_run_id}/children")

    @registry.mcp.tool(name="multi_alpha_combine_backtest_child_get")
    def multi_alpha_combine_backtest_child_get(run_id: str, child_id: str) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        safe_child_id = registry.sanitize(child_id, "child_id")
        return multi_alpha_client.get(
            f"/combine-backtest/runs/{safe_run_id}/children/{safe_child_id}"
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_child_attempts_list")
    def multi_alpha_combine_backtest_child_attempts_list(run_id: str, child_id: str) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        safe_child_id = registry.sanitize(child_id, "child_id")
        return multi_alpha_client.get(
            f"/combine-backtest/runs/{safe_run_id}/children/{safe_child_id}/attempts"
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_commands_list")
    def multi_alpha_combine_backtest_commands_list(
        run_id: str,
        after_command_seq: int | None = None,
        limit: int = 200,
    ) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        return multi_alpha_client.get(
            f"/combine-backtest/runs/{safe_run_id}/commands",
            params={
                "after_command_seq": after_command_seq,
                "limit": max(1, min(int(limit), 1000)),
            },
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_command_get")
    def multi_alpha_combine_backtest_command_get(run_id: str, command_id: str) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        safe_command_id = registry.sanitize(command_id, "command_id")
        return multi_alpha_client.get(
            f"/combine-backtest/runs/{safe_run_id}/commands/{safe_command_id}"
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_recovery_preview")
    def multi_alpha_combine_backtest_recovery_preview(
        run_id: str,
        child_id: str,
        retry_mode: str,
        idempotency_key: str,
    ) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        safe_child_id = registry.sanitize(child_id, "child_id")
        path = f"/combine-backtest/runs/{safe_run_id}/children/{safe_child_id}/recovery/preview"
        return _multi_alpha_post_with_idempotency(
            path,
            {"retry_mode": str(retry_mode or "")},
            idempotency_key=idempotency_key,
        )

    def _multi_alpha_durable_run_mutation(
        *,
        run_id: str,
        action: str,
        idempotency_key: str,
        request: dict[str, Any] | None,
    ) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        path = f"/combine-backtest/runs/{safe_run_id}/{action}"
        return _multi_alpha_post_with_idempotency(
            path,
            {"request": dict(request or {})},
            idempotency_key=idempotency_key,
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_pause")
    def multi_alpha_combine_backtest_pause(
        run_id: str,
        idempotency_key: str,
        request: dict[str, Any] | None = None,
    ) -> Any:
        return _multi_alpha_durable_run_mutation(
            run_id=run_id, action="pause", idempotency_key=idempotency_key, request=request
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_resume")
    def multi_alpha_combine_backtest_resume(
        run_id: str,
        idempotency_key: str,
        request: dict[str, Any] | None = None,
    ) -> Any:
        return _multi_alpha_durable_run_mutation(
            run_id=run_id, action="resume", idempotency_key=idempotency_key, request=request
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_cancel")
    def multi_alpha_combine_backtest_cancel(
        run_id: str,
        idempotency_key: str,
        request: dict[str, Any] | None = None,
    ) -> Any:
        return _multi_alpha_durable_run_mutation(
            run_id=run_id, action="cancel", idempotency_key=idempotency_key, request=request
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_stop")
    def multi_alpha_combine_backtest_stop(
        run_id: str,
        idempotency_key: str,
        request: dict[str, Any] | None = None,
    ) -> Any:
        return _multi_alpha_durable_run_mutation(
            run_id=run_id, action="stop", idempotency_key=idempotency_key, request=request
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_reconcile")
    def multi_alpha_combine_backtest_reconcile(
        run_id: str,
        idempotency_key: str,
        request: dict[str, Any] | None = None,
    ) -> Any:
        return _multi_alpha_durable_run_mutation(
            run_id=run_id, action="reconcile", idempotency_key=idempotency_key, request=request
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_attempt_cancel")
    def multi_alpha_combine_backtest_attempt_cancel(
        run_id: str,
        attempt_id: str,
        idempotency_key: str,
        request: dict[str, Any] | None = None,
    ) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        safe_attempt_id = registry.sanitize(attempt_id, "attempt_id")
        path = f"/combine-backtest/runs/{safe_run_id}/attempts/{safe_attempt_id}/cancel"
        return _multi_alpha_post_with_idempotency(
            path,
            {"request": dict(request or {})},
            idempotency_key=idempotency_key,
        )

    @registry.mcp.tool(name="multi_alpha_combine_backtest_child_recovery_execute")
    def multi_alpha_combine_backtest_child_recovery_execute(
        run_id: str,
        child_id: str,
        retry_mode: str,
        scope_hash: str,
        preview_command_id: str,
        idempotency_key: str,
    ) -> Any:
        safe_run_id = registry.sanitize(run_id, "run_id")
        safe_child_id = registry.sanitize(child_id, "child_id")
        path = f"/combine-backtest/runs/{safe_run_id}/children/{safe_child_id}/recovery"
        return _multi_alpha_post_with_idempotency(
            path,
            {
                "retry_mode": str(retry_mode or ""),
                "scope_hash": str(scope_hash or ""),
                "preview_command_id": str(preview_command_id or ""),
            },
            idempotency_key=idempotency_key,
        )

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
