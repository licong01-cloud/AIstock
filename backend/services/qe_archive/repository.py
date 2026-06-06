"""Repository methods for the QE realtime experiment warehouse."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from psycopg2.extras import Json, execute_values

from backend.db.pg_pool import get_conn

from .models import (
    AccountSummaryRecord,
    ArchiveJobRecord,
    BackfillRunItemRecord,
    BackfillRunRecord,
    BootstrapMarkerRecord,
    ClaimedOutboxEvent,
    CurveRecord,
    DataContextRecord,
    ExecutionEventRecord,
    IngestHistoryRecord,
    MetricRecord,
    OutboxEventRecord,
    RawPayloadRecord,
    ReproducibilityManifestRecord,
    RunFactorImportanceRecord,
    RunFactorRecord,
    RunConfigRecord,
    RunSourceRecord,
    SkipRegistryRecord,
    SymbolSummaryRecord,
    TradeRecord,
    canonical_json_dumps,
    normalize_json,
    sha256_json,
    sha256_text,
    to_record_dict,
)


ConnectionProvider = Callable[[], Any]


RUN_COLUMNS = (
    "run_id",
    "logical_experiment_id",
    "attempt_no",
    "is_latest_attempt",
    "source_system",
    "run_type",
    "task_id",
    "loop_id",
    "loop_index",
    "experiment_id",
    "node_id",
    "model_catalog_id",
    "model_family",
    "model_type",
    "factor_set_hash",
    "factor_count",
    "freq",
    "label_horizon",
    "status",
    "research_valid",
    "invalid_reason",
    "exclusion_tags",
    "score_total",
    "score_version",
    "priority_rank",
    "started_at",
    "completed_at",
    "archived_at",
    "source_created_at",
    "source_updated_at",
)

RUN_REQUIRED = ("run_id", "logical_experiment_id", "source_system", "run_type", "status")

RUN_SOURCE_COLUMNS = (
    "run_id",
    "source_system",
    "source_type",
    "source_id",
    "source_sub_id",
    "source_status",
    "source_uri",
    "recorder_experiment_id",
    "recorder_id",
    "mlflow_tracking_uri",
    "mlflow_artifact_uri",
    "qlib_recorder_name",
    "node_api_base_url",
    "metadata",
)

RUN_CONFIG_COLUMNS = (
    "run_id",
    "config_schema_version",
    "config_sha256",
    "canonical_config",
    "raw_config",
    "factor_list",
    "factor_set_hash",
    "model_config",
    "model_params",
    "strategy_config",
    "backtest_config",
    "data_split",
    "execution_config",
    "runtime_flags",
    "agent_context",
    "config_capture_complete",
    "config_provenance",
    "missing_config_items",
)

DATA_CONTEXT_COLUMNS = (
    "run_id",
    "context_type",
    "freq",
    "market",
    "universe",
    "benchmark",
    "train_start",
    "train_end",
    "valid_start",
    "valid_end",
    "test_start",
    "test_end",
    "backtest_start",
    "backtest_end",
    "label_horizon",
    "qlib_provider_uri",
    "qlib_dataset_version",
    "dataset_snapshot_id",
    "feature_snapshot_id",
    "factor_cache_snapshot_id",
    "data_version_hash",
    "pit_cutoff_date",
    "limit_handling",
    "suspend_handling",
    "limit_suspend_authoritative",
    "cost_config",
    "stock_pool_config",
    "data_quality_flags",
)

ACCOUNT_SUMMARY_COLUMNS = (
    "run_id",
    "initial_capital",
    "final_total_value",
    "final_account_value",
    "final_nav_value",
    "total_return",
    "cagr",
    "max_drawdown",
    "max_drawdown_date",
    "sharpe",
    "annualized_volatility",
    "avg_cash_ratio",
    "final_cash",
    "final_stock_value",
    "final_stock_count",
    "final_cash_ratio",
    "n_trading_days",
    "position_count_min",
    "position_count_avg",
    "position_count_max",
    "position_count_p95",
    "source_payload_path",
    "metadata",
)

REPRO_COLUMNS = (
    "run_id",
    "manifest_schema_version",
    "reproducibility_level",
    "verification_status",
    "config_sha256",
    "canonical_config_sha256",
    "raw_config_sha256",
    "factor_set_hash",
    "qlib_config_sha256",
    "model_params_sha256",
    "strategy_config_sha256",
    "data_context_sha256",
    "metrics_payload_sha256",
    "enhanced_metrics_sha256",
    "artifact_manifest_sha256",
    "git_commit",
    "git_dirty",
    "runner_script",
    "runner_script_sha256",
    "python_version",
    "qlib_version",
    "mlflow_version",
    "torch_version",
    "package_versions",
    "random_seed",
    "deterministic_flags",
    "source_config_paths",
    "required_artifact_types",
    "missing_items",
    "manifest_json",
)

METRIC_COLUMNS = (
    "run_id",
    "metric_key",
    "metric_scope",
    "period_start",
    "period_end",
    "horizon",
    "freq",
    "value_num",
    "value_text",
    "value_json",
    "unit",
    "direction",
    "source_key",
    "source_payload_path",
    "quality_flag",
)

CURVE_COLUMNS = (
    "run_id",
    "curve_key",
    "ts",
    "trade_date",
    "step",
    "epoch",
    "split_name",
    "value_num",
    "value_json",
    "source_key",
)

FACTOR_COLUMNS = (
    "run_id",
    "factor_catalog_id",
    "factor_name",
    "factor_source",
    "factor_version",
    "factor_order",
    "factor_group",
    "factor_classification",
    "factor_expression_hash",
    "factor_asset_hash",
    "inclusion_reason",
    "inclusion_source",
    "is_alpha158",
    "independent_metrics_snapshot",
    "official_rating_snapshot",
    "correlation_cluster",
)

FACTOR_IMPORTANCE_COLUMNS = (
    "run_id",
    "factor_catalog_id",
    "factor_name",
    "feature_name",
    "feature_index",
    "model_family",
    "model_type",
    "method",
    "method_version",
    "split_name",
    "time_bucket",
    "epoch",
    "step",
    "importance_value",
    "normalized_value",
    "weight_pct",
    "signed_value",
    "rank_in_run",
    "sample_count",
    "reliability",
    "metadata",
)

SYMBOL_SUMMARY_COLUMNS = (
    "run_id",
    "symbol",
    "source_list",
    "profit",
    "profit_pct",
    "avg_cost",
    "last_price",
    "holding_days",
    "first_date",
    "last_date",
    "rank_in_list",
    "metadata",
)

TRADE_COLUMNS = (
    "run_id",
    "trade_uid",
    "order_uid",
    "trade_date",
    "ts",
    "symbol",
    "side",
    "price",
    "quantity",
    "amount",
    "commission",
    "tax",
    "slippage",
    "pnl",
    "source_payload_path",
    "metadata",
)

EXECUTION_EVENT_COLUMNS = (
    "run_id",
    "event_ts",
    "trade_date",
    "symbol",
    "event_type",
    "severity",
    "message",
    "metadata",
)

SKIP_REGISTRY_COLUMNS = (
    "skip_id",
    "source_system",
    "source_type",
    "source_id",
    "source_sub_id",
    "event_type",
    "archive_policy",
    "archive_policy_source",
    "skip_reason",
    "allow_override",
    "override_required_token",
    "trigger_reason",
    "payload_sha256",
    "runtime_config_sha256",
    "created_by",
    "metadata",
)

INGEST_HISTORY_COLUMNS = (
    "history_id",
    "run_id",
    "logical_experiment_id",
    "event_id",
    "job_id",
    "backfill_run_id",
    "source_system",
    "source_type",
    "source_id",
    "source_sub_id",
    "trigger_reason",
    "archive_policy",
    "ingest_status",
    "attempt_no",
    "payload_sha256",
    "runtime_config_sha256",
    "result_fingerprint",
    "anomaly",
    "anomaly_reason",
    "stats",
    "error_message",
    "created_by",
)

BACKFILL_RUN_COLUMNS = (
    "backfill_run_id",
    "source_mode",
    "mode",
    "status",
    "request_payload",
    "force_rebackfill",
    "confirm_token_used",
    "requested_by",
    "candidate_count",
    "processed_count",
    "ingested_count",
    "skipped_count",
    "failed_count",
    "last_cursor",
    "error_message",
)

BACKFILL_RUN_ITEM_COLUMNS = (
    "item_id",
    "backfill_run_id",
    "source_system",
    "source_type",
    "source_id",
    "source_sub_id",
    "archive_policy",
    "status",
    "run_id",
    "skip_id",
    "error_message",
    "stats",
)

BOOTSTRAP_MARKER_COLUMNS = (
    "source_type",
    "status",
    "mode",
    "backfill_run_id",
    "operator",
    "ingested_count",
    "skipped_count",
    "failed_count",
    "stats",
)


JSON_COLUMNS = {
    "canonical_config",
    "raw_config",
    "factor_list",
    "model_config",
    "model_params",
    "strategy_config",
    "backtest_config",
    "data_split",
    "execution_config",
    "runtime_flags",
    "agent_context",
    "config_provenance",
    "missing_config_items",
    "cost_config",
    "stock_pool_config",
    "data_quality_flags",
    "package_versions",
    "deterministic_flags",
    "source_config_paths",
    "missing_items",
    "manifest_json",
    "value_json",
    "payload",
    "payload_json",
    "metadata",
    "stats",
    "request_payload",
    "last_cursor",
    "factor_classification",
    "independent_metrics_snapshot",
    "official_rating_snapshot",
}

QE_ARCHIVE_ANALYTICS_VIEW_DEFS: dict[str, dict[str, str]] = {
    "run_leaderboard": {
        "view_name": "v_run_leaderboard",
        "purpose": "Run-level signal and return/risk leaderboard.",
        "grain": "run_id",
    },
    "seed_robustness": {
        "view_name": "v_seed_robustness",
        "purpose": "Multi-seed robustness by config fingerprint.",
        "grain": "factor_set_hash x model_type x label_horizon x undertrain_mode x topk",
    },
    "factor_importance_stability": {
        "view_name": "v_factor_importance_stability",
        "purpose": "Factor importance stability across runs and seeds.",
        "grain": "factor_name x method",
    },
    "factor_performance": {
        "view_name": "v_factor_performance",
        "purpose": "Factor usage and performance footprint.",
        "grain": "factor_name",
    },
    "model_hyperparam_seed_perf": {
        "view_name": "v_model_hyperparam_seed_perf",
        "purpose": "Model hyperparameter by seed performance.",
        "grain": "model_type x hyperparam_hash x seed",
    },
    "overfit_flags": {
        "view_name": "v_overfit_flags",
        "purpose": "Run-level overfit and seed-outlier flags.",
        "grain": "run_id",
    },
    "promotion_candidates": {
        "view_name": "v_promotion_candidates",
        "purpose": "Stable dual-axis promotion candidate configs.",
        "grain": "config fingerprint",
    },
    "evolution_lineage": {
        "view_name": "v_evolution_lineage",
        "purpose": "Task, loop, experiment and run lineage.",
        "grain": "task_id x loop_index x experiment_id x run_id",
    },
}


def _clamped_limit(value: int | None, *, default: int = 20, maximum: int = 100) -> int:
    return max(1, min(int(value or default), maximum))


def _order_by_clause(value: str | None, allowed: set[str], default: str) -> str:
    column = str(value or default).strip().lower()
    if column not in allowed:
        column = default
    return f"{column} DESC NULLS LAST"


class QEArchiveRepository:
    """Small repository for explicit archive writes.

    The repository never reads QE worker files. Callers must provide already
    collected API payloads, normalized configs, and artifact manifests.
    """

    def __init__(self, connection_provider: ConnectionProvider = get_conn) -> None:
        self._connection_provider = connection_provider

    def upsert_run(self, run: Mapping[str, Any] | Any) -> str:
        record = self._prepare_record(run, RUN_COLUMNS, defaults={"attempt_no": 1, "is_latest_attempt": True})
        self._require(record, RUN_REQUIRED)

        columns = list(record.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        assignment_parts: list[str] = []
        for column in columns:
            if column == "run_id":
                continue
            if column == "archived_at":
                assignment_parts.append(
                    "archived_at = COALESCE(EXCLUDED.archived_at, qe_archive.run.archived_at)"
                )
            else:
                assignment_parts.append(f"{column} = EXCLUDED.{column}")
        assignments = ", ".join(assignment_parts)
        sql = f"""
            INSERT INTO qe_archive.run ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (run_id) DO UPDATE SET
                {assignments},
                updated_at = NOW()
        """

        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                if record.get("is_latest_attempt") is True:
                    cur.execute(
                        """
                        UPDATE qe_archive.run
                        SET is_latest_attempt = FALSE, updated_at = NOW()
                        WHERE logical_experiment_id = %s AND run_id <> %s
                        """,
                        (record["logical_experiment_id"], record["run_id"]),
                    )
                cur.execute(sql, [self._adapt_value(col, record[col]) for col in columns])
        return str(record["run_id"])

    def mark_latest_attempt(self, logical_experiment_id: str, run_id: str) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_archive.run
                    SET is_latest_attempt = FALSE, updated_at = NOW()
                    WHERE logical_experiment_id = %s AND run_id <> %s
                    """,
                    (logical_experiment_id, run_id),
                )
                cur.execute(
                    """
                    UPDATE qe_archive.run
                    SET is_latest_attempt = TRUE, updated_at = NOW()
                    WHERE logical_experiment_id = %s AND run_id = %s
                    """,
                    (logical_experiment_id, run_id),
                )

    def upsert_run_source(self, source: RunSourceRecord | Mapping[str, Any]) -> int | None:
        if isinstance(source, Mapping):
            source = RunSourceRecord(**dict(source))
        record = self._prepare_record(source, RUN_SOURCE_COLUMNS)
        self._require(record, ("run_id", "source_system", "source_type", "source_id"))

        columns = list(record.keys())
        sql = f"""
            INSERT INTO qe_archive.run_source ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            RETURNING id
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM qe_archive.run_source
                    WHERE source_system = %s
                      AND source_type = %s
                      AND source_id = %s
                      AND COALESCE(source_sub_id, '') = COALESCE(%s, '')
                    """,
                    (
                        record["source_system"],
                        record["source_type"],
                        record["source_id"],
                        record.get("source_sub_id"),
                    ),
                )
                cur.execute(sql, [self._adapt_value(col, record[col]) for col in columns])
                row = cur.fetchone()
        return int(row[0]) if row else None

    def upsert_run_config(self, config: RunConfigRecord | Mapping[str, Any]) -> str:
        if isinstance(config, Mapping):
            config = RunConfigRecord(**dict(config))
        record = self._prepare_record(config, RUN_CONFIG_COLUMNS)
        self._require(record, ("run_id", "config_schema_version", "config_sha256", "canonical_config"))

        columns = list(record.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        assignments = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in columns
            if column != "run_id"
        )
        sql = f"""
            INSERT INTO qe_archive.run_config ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (run_id) DO UPDATE SET
                {assignments},
                updated_at = NOW()
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, record[col]) for col in columns])
        return str(record["run_id"])

    def upsert_data_context(self, context: DataContextRecord | Mapping[str, Any]) -> int | None:
        if isinstance(context, Mapping):
            context = DataContextRecord(**dict(context))
        record = self._prepare_record(context, DATA_CONTEXT_COLUMNS)
        self._require(record, ("run_id", "context_type"))

        columns = list(record.keys())
        sql = f"""
            INSERT INTO qe_archive.run_data_context ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            RETURNING id
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM qe_archive.run_data_context
                    WHERE run_id = %s AND context_type = %s
                    """,
                    (record["run_id"], record["context_type"]),
                )
                cur.execute(sql, [self._adapt_value(col, record[col]) for col in columns])
                row = cur.fetchone()
        return int(row[0]) if row else None

    def upsert_account_summary(self, summary: AccountSummaryRecord | Mapping[str, Any]) -> str:
        if isinstance(summary, Mapping):
            summary = AccountSummaryRecord(**dict(summary))
        record = self._prepare_record(summary, ACCOUNT_SUMMARY_COLUMNS)
        self._require(record, ("run_id",))

        columns = list(record.keys())
        assignments = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in columns
            if column != "run_id"
        )
        sql = f"""
            INSERT INTO qe_archive.run_account_summary ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (run_id) DO UPDATE SET
                {assignments},
                updated_at = NOW()
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, record[col]) for col in columns])
        return str(record["run_id"])

    def upsert_reproducibility_manifest(
        self,
        manifest: ReproducibilityManifestRecord | Mapping[str, Any],
    ) -> str:
        if isinstance(manifest, Mapping):
            manifest = ReproducibilityManifestRecord(**dict(manifest))
        record = self._prepare_record(manifest, REPRO_COLUMNS)
        self._require(
            record,
            ("run_id", "manifest_schema_version", "reproducibility_level", "manifest_json"),
        )

        columns = list(record.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        assignments = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in columns
            if column != "run_id"
        )
        sql = f"""
            INSERT INTO qe_archive.run_reproducibility_manifest ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (run_id) DO UPDATE SET
                {assignments},
                updated_at = NOW()
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, record[col]) for col in columns])
        return str(record["run_id"])

    def insert_raw_payload(self, payload: RawPayloadRecord | Mapping[str, Any]) -> int | None:
        record = to_record_dict(payload)
        payload_json = record.get("payload_json")
        payload_text = record.get("payload_text")
        if not record.get("payload_sha256"):
            if payload_json is not None:
                record["payload_sha256"] = sha256_json(payload_json)
            elif payload_text is not None:
                record["payload_sha256"] = sha256_text(str(payload_text))

        columns = [
            "run_id",
            "payload_type",
            "source_system",
            "source_id",
            "payload_sha256",
            "payload_json",
            "payload_text",
            "provenance_level",
        ]
        self._require(record, ("payload_type", "source_system"))
        sql = f"""
            INSERT INTO qe_archive.raw_payload ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            RETURNING id
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, record.get(col)) for col in columns])
                row = cur.fetchone()
        return int(row[0]) if row else None

    def replace_raw_payloads(
        self,
        run_id: str,
        payloads: Sequence[RawPayloadRecord | Mapping[str, Any]],
    ) -> int:
        records = [to_record_dict(payload) for payload in payloads]
        if not records:
            return 0
        payload_types = sorted({str(record.get("payload_type")) for record in records if record.get("payload_type")})
        if not payload_types:
            raise ValueError("raw payload replacement requires payload_type")

        columns = [
            "run_id",
            "payload_type",
            "source_system",
            "source_id",
            "payload_sha256",
            "payload_json",
            "payload_text",
            "provenance_level",
        ]
        rows = []
        for record in records:
            record["run_id"] = run_id
            payload_json = record.get("payload_json")
            payload_text = record.get("payload_text")
            if not record.get("payload_sha256"):
                if payload_json is not None:
                    record["payload_sha256"] = sha256_json(payload_json)
                elif payload_text is not None:
                    record["payload_sha256"] = sha256_text(str(payload_text))
            self._require(record, ("run_id", "payload_type", "source_system"))
            rows.append(tuple(self._adapt_value(col, record.get(col)) for col in columns))

        sql = f"""
            INSERT INTO qe_archive.raw_payload ({", ".join(columns)})
            VALUES %s
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM qe_archive.raw_payload
                    WHERE run_id = %s AND payload_type = ANY(%s)
                    """,
                    (run_id, payload_types),
                )
                execute_values(cur, sql, rows, page_size=500)
        return len(records)

    def insert_outbox_event(self, event: OutboxEventRecord | Mapping[str, Any]) -> bool:
        if isinstance(event, Mapping):
            event = OutboxEventRecord(**dict(event))
        record = to_record_dict(event)
        columns = ("event_id", "event_type", "source_system", "source_id", "source_sub_id", "payload", "status")
        self._require(record, ("event_id", "event_type", "source_system", "source_id"))
        sql = f"""
            INSERT INTO qe_archive.outbox_event ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (event_id) DO NOTHING
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, record.get(col)) for col in columns])
                return cur.rowcount == 1

    def claim_outbox_events(
        self,
        *,
        worker_id: str,
        limit: int = 10,
        event_types: Sequence[str] | None = None,
    ) -> list[ClaimedOutboxEvent]:
        if limit <= 0:
            return []

        params: list[Any] = []
        event_filter = ""
        if event_types:
            event_filter = "AND event_type = ANY(%s)"
            params.append(list(event_types))
        params.extend([limit, worker_id])

        sql = f"""
            WITH next_events AS (
                SELECT event_id
                FROM qe_archive.outbox_event
                WHERE status = 'pending'
                  AND next_retry_at <= NOW()
                  {event_filter}
                ORDER BY next_retry_at ASC, created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE qe_archive.outbox_event AS e
            SET status = 'processing',
                locked_by = %s,
                locked_at = NOW(),
                updated_at = NOW()
            FROM next_events
            WHERE e.event_id = next_events.event_id
            RETURNING
                e.event_id,
                e.event_type,
                e.source_system,
                e.source_id,
                e.source_sub_id,
                e.payload,
                e.retry_count
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = self._fetch_dicts(cur)

        return [
            ClaimedOutboxEvent(
                event_id=str(row["event_id"]),
                event_type=str(row["event_type"]),
                source_system=str(row["source_system"]),
                source_id=str(row["source_id"]),
                source_sub_id=row.get("source_sub_id"),
                payload=row.get("payload") or {},
                retry_count=int(row.get("retry_count") or 0),
            )
            for row in rows
        ]

    def complete_outbox_event(self, event_id: str) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_archive.outbox_event
                    SET status = 'completed',
                        locked_by = NULL,
                        locked_at = NULL,
                        error_message = NULL,
                        updated_at = NOW()
                    WHERE event_id = %s
                    """,
                    (event_id,),
                )

    def fail_outbox_event(
        self,
        event_id: str,
        error: str,
        *,
        retry_after_seconds: int = 60,
        max_retries: int = 5,
    ) -> None:
        retry_after_seconds = max(0, retry_after_seconds)
        max_retries = max(1, max_retries)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_archive.outbox_event
                    SET retry_count = retry_count + 1,
                        status = CASE WHEN retry_count + 1 >= %s THEN 'failed' ELSE 'pending' END,
                        next_retry_at = CASE
                            WHEN retry_count + 1 >= %s THEN next_retry_at
                            ELSE NOW() + make_interval(secs => %s)
                        END,
                        locked_by = NULL,
                        locked_at = NULL,
                        error_message = %s,
                        updated_at = NOW()
                    WHERE event_id = %s
                    """,
                    (max_retries, max_retries, retry_after_seconds, error, event_id),
                )

    def create_archive_job(self, job: ArchiveJobRecord | Mapping[str, Any]) -> str:
        if isinstance(job, Mapping):
            job = ArchiveJobRecord(**dict(job))
        record = to_record_dict(job)
        columns = ("job_id", "event_id", "run_id", "job_type", "status", "level", "started_at", "stats")
        record.setdefault("status", "running")
        record.setdefault("level", "A")
        record["started_at"] = record.get("started_at") or None
        self._require(record, ("job_id", "event_id", "job_type", "status", "level"))
        sql = """
            INSERT INTO qe_archive.archive_job (
                job_id, event_id, run_id, job_type, status, level, started_at, stats
            )
            VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()), %s)
            ON CONFLICT (job_id) DO UPDATE SET
                event_id = EXCLUDED.event_id,
                run_id = EXCLUDED.run_id,
                job_type = EXCLUDED.job_type,
                status = EXCLUDED.status,
                level = EXCLUDED.level,
                started_at = COALESCE(qe_archive.archive_job.started_at, EXCLUDED.started_at),
                stats = EXCLUDED.stats,
                updated_at = NOW()
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, record.get(col)) for col in columns])
        return str(record["job_id"])

    def complete_archive_job(
        self,
        job_id: str,
        *,
        run_id: str | None = None,
        stats: Mapping[str, Any] | None = None,
    ) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_archive.archive_job
                    SET status = 'completed',
                        run_id = COALESCE(%s, run_id),
                        completed_at = NOW(),
                        stats = COALESCE(%s, stats),
                        error_message = NULL,
                        updated_at = NOW()
                    WHERE job_id = %s
                    """,
                    (run_id, self._adapt_value("stats", stats) if stats is not None else None, job_id),
                )

    def fail_archive_job(
        self,
        job_id: str,
        error: str,
        *,
        stats: Mapping[str, Any] | None = None,
    ) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_archive.archive_job
                    SET status = 'failed',
                        completed_at = NOW(),
                        retry_count = retry_count + 1,
                        error_message = %s,
                        stats = COALESCE(%s, stats),
                        updated_at = NOW()
                    WHERE job_id = %s
                    """,
                    (error, self._adapt_value("stats", stats) if stats is not None else None, job_id),
                )

    def upsert_skip_registry(self, record: SkipRegistryRecord | Mapping[str, Any]) -> str:
        if isinstance(record, Mapping):
            record = SkipRegistryRecord(**dict(record))
        row = self._prepare_record(record, SKIP_REGISTRY_COLUMNS)
        self._require(
            row,
            ("skip_id", "source_system", "source_type", "source_id", "archive_policy", "archive_policy_source", "skip_reason", "trigger_reason"),
        )
        columns = list(row.keys())
        assignments = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in columns
            if column not in {"skip_id", "created_by"}
        )
        sql = f"""
            INSERT INTO qe_archive.skip_registry ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (source_system, source_type, source_id, (COALESCE(source_sub_id, ''))) DO UPDATE SET
                {assignments},
                last_seen_at = NOW()
            RETURNING skip_id
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, row.get(col)) for col in columns])
                result = cur.fetchone()
        return str(result[0]) if result else str(row["skip_id"])

    def list_skips(
        self,
        *,
        archive_policy: str | None = None,
        source_type: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 100), 500))
        filters: list[str] = []
        params: list[Any] = []
        if archive_policy:
            filters.append("archive_policy = %s")
            params.append(archive_policy)
        if source_type:
            filters.append("source_type = %s")
            params.append(source_type)
        params.append(limit)
        # ALGO-COMPLEXITY-001: this analytics join is bounded by indexed
        # run/task/factor filters plus a hard LIMIT; it never scans raw
        # workspace artifacts or expands factor x symbol x date rows.
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT skip_id, source_system, source_type, source_id, source_sub_id,
                           event_type, archive_policy, archive_policy_source, skip_reason,
                           allow_override, trigger_reason, payload_sha256,
                           runtime_config_sha256, created_by, metadata, created_at, last_seen_at
                    FROM qe_archive.skip_registry
                    {where_sql}
                    ORDER BY last_seen_at DESC, created_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def insert_ingest_history(self, record: IngestHistoryRecord | Mapping[str, Any]) -> str:
        if isinstance(record, Mapping):
            record = IngestHistoryRecord(**dict(record))
        row = self._prepare_record(record, INGEST_HISTORY_COLUMNS)
        self._require(
            row,
            ("history_id", "source_system", "source_type", "source_id", "trigger_reason", "ingest_status"),
        )
        if self._detect_ingest_anomaly(row):
            row["anomaly"] = True
            row.setdefault("anomaly_reason", "source_fingerprint_changed")
        columns = list(row.keys())
        sql = f"""
            INSERT INTO qe_archive.ingest_history ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            RETURNING history_id
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, row.get(col)) for col in columns])
                result = cur.fetchone()
        return str(result[0]) if result else str(row["history_id"])

    def _detect_ingest_anomaly(self, row: Mapping[str, Any]) -> bool:
        payload_sha = row.get("payload_sha256")
        runtime_sha = row.get("runtime_config_sha256")
        result_fp = row.get("result_fingerprint")
        if not (payload_sha or runtime_sha or result_fp):
            return False
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT payload_sha256, runtime_config_sha256, result_fingerprint
                    FROM qe_archive.ingest_history
                    WHERE source_system = %s
                      AND source_type = %s
                      AND source_id = %s
                      AND COALESCE(source_sub_id, '') = COALESCE(%s, '')
                      AND ingest_status IN ('completed','skipped','manual_only')
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (row.get("source_system"), row.get("source_type"), row.get("source_id"), row.get("source_sub_id")),
                )
                existing = cur.fetchone()
        if not existing:
            return False
        old_payload, old_runtime, old_result = existing
        return any(
            new and old and new != old
            for new, old in ((payload_sha, old_payload), (runtime_sha, old_runtime), (result_fp, old_result))
        )

    def upsert_backfill_run(self, record: BackfillRunRecord | Mapping[str, Any]) -> str:
        if isinstance(record, Mapping):
            record = BackfillRunRecord(**dict(record))
        row = self._prepare_record(record, BACKFILL_RUN_COLUMNS)
        self._require(row, ("backfill_run_id", "source_mode", "mode", "status"))
        columns = list(row.keys())
        assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns if column != "backfill_run_id")
        sql = f"""
            INSERT INTO qe_archive.backfill_run ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (backfill_run_id) DO UPDATE SET
                {assignments},
                updated_at = NOW()
            RETURNING backfill_run_id
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, row.get(col)) for col in columns])
                result = cur.fetchone()
        return str(result[0]) if result else str(row["backfill_run_id"])

    def update_backfill_run_status(
        self,
        backfill_run_id: str,
        *,
        status: str,
        processed_count: int | None = None,
        ingested_count: int | None = None,
        skipped_count: int | None = None,
        failed_count: int | None = None,
        candidate_count: int | None = None,
        error_message: str | None = None,
        last_cursor: Mapping[str, Any] | None = None,
    ) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_archive.backfill_run
                    SET status = %s,
                        processed_count = COALESCE(%s, processed_count),
                        ingested_count = COALESCE(%s, ingested_count),
                        skipped_count = COALESCE(%s, skipped_count),
                        failed_count = COALESCE(%s, failed_count),
                        candidate_count = COALESCE(%s, candidate_count),
                        error_message = COALESCE(%s, error_message),
                        last_cursor = COALESCE(%s, last_cursor),
                        started_at = COALESCE(started_at, NOW()),
                        completed_at = CASE WHEN %s IN ('completed','failed','partial') THEN NOW() ELSE completed_at END,
                        updated_at = NOW()
                    WHERE backfill_run_id = %s
                    """,
                    (
                        status,
                        processed_count,
                        ingested_count,
                        skipped_count,
                        failed_count,
                        candidate_count,
                        error_message,
                        self._adapt_value("last_cursor", last_cursor) if last_cursor is not None else None,
                        status,
                        backfill_run_id,
                    ),
                )

    def upsert_backfill_run_item(self, record: BackfillRunItemRecord | Mapping[str, Any]) -> str:
        if isinstance(record, Mapping):
            record = BackfillRunItemRecord(**dict(record))
        row = self._prepare_record(record, BACKFILL_RUN_ITEM_COLUMNS)
        self._require(row, ("item_id", "backfill_run_id", "source_system", "source_type", "source_id", "status"))
        columns = list(row.keys())
        assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns if column != "item_id")
        sql = f"""
            INSERT INTO qe_archive.backfill_run_item ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (item_id) DO UPDATE SET
                {assignments},
                updated_at = NOW()
            RETURNING item_id
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, row.get(col)) for col in columns])
                result = cur.fetchone()
        return str(result[0]) if result else str(row["item_id"])

    def list_backfill_runs(self, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        params: list[Any] = []
        where_sql = ""
        if status:
            where_sql = "WHERE status = %s"
            params.append(status)
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM qe_archive.backfill_run
                    {where_sql}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def get_backfill_run(self, backfill_run_id: str) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM qe_archive.backfill_run WHERE backfill_run_id = %s", (backfill_run_id,))
                rows = self._fetch_dicts(cur)
                if not rows:
                    return None
                run = rows[0]
                cur.execute(
                    """
                    SELECT *
                    FROM qe_archive.backfill_run_item
                    WHERE backfill_run_id = %s
                    ORDER BY created_at ASC
                    """,
                    (backfill_run_id,),
                )
                run["items"] = self._fetch_dicts(cur)
                return run

    def upsert_bootstrap_marker(self, record: BootstrapMarkerRecord | Mapping[str, Any]) -> str:
        if isinstance(record, Mapping):
            record = BootstrapMarkerRecord(**dict(record))
        row = self._prepare_record(record, BOOTSTRAP_MARKER_COLUMNS)
        self._require(row, ("source_type", "status", "mode", "backfill_run_id"))
        columns = list(row.keys())
        assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns if column != "source_type")
        sql = f"""
            INSERT INTO qe_archive.bootstrap_marker ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (source_type) DO UPDATE SET
                {assignments},
                updated_at = NOW(),
                completed_at = CASE WHEN EXCLUDED.status IN ('completed','failed') THEN NOW() ELSE qe_archive.bootstrap_marker.completed_at END
            RETURNING source_type
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt_value(col, row.get(col)) for col in columns])
                result = cur.fetchone()
        return str(result[0]) if result else str(row["source_type"])

    def get_bootstrap_marker(self, source_type: str) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM qe_archive.bootstrap_marker WHERE source_type = %s", (source_type,))
                rows = self._fetch_dicts(cur)
                return rows[0] if rows else None

    def list_outbox_events(self, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent outbox events for UI/API monitoring."""

        limit = max(1, min(int(limit or 50), 500))
        params: list[Any] = []
        status_filter = ""
        if status:
            status_filter = "WHERE status = %s"
            params.append(status)
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT event_id, event_type, source_system, source_id, source_sub_id,
                           status, retry_count, next_retry_at, locked_by, locked_at,
                           error_message, created_at, updated_at, payload
                    FROM qe_archive.outbox_event
                    {status_filter}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def list_archive_jobs(self, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent archive worker jobs for UI/API monitoring."""

        limit = max(1, min(int(limit or 50), 500))
        params: list[Any] = []
        status_filter = ""
        if status:
            status_filter = "WHERE status = %s"
            params.append(status)
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT job_id, event_id, run_id, job_type, status, level,
                           started_at, completed_at, retry_count, error_message,
                           stats, created_at, updated_at
                    FROM qe_archive.archive_job
                    {status_filter}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def list_runs(
        self,
        *,
        status: str | None = None,
        run_type: str | None = None,
        search: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return recent archived runs for selection-oriented UI/API consumers."""

        limit = max(1, min(int(limit or 100), 500))
        params: list[Any] = []
        filters: list[str] = []
        if status and status not in {"all", "*"}:
            filters.append("r.status = %s")
            params.append(status)
        if run_type and run_type not in {"all", "*"}:
            filters.append("r.run_type = %s")
            params.append(run_type)
        if search:
            params.append(f"%{search}%")
            filters.append(
                """
                (
                    r.run_id ILIKE %s
                    OR r.logical_experiment_id ILIKE %s
                    OR r.experiment_id ILIKE %s
                    OR r.task_id ILIKE %s
                    OR r.loop_id ILIKE %s
                )
                """
            )
            params.extend([params[-1]] * 4)
        # ALGO-COMPLEXITY-001: stability aggregation groups only persisted
        # per-run factor-importance facts, optionally narrowed by factor/method,
        # and is capped by LIMIT to avoid unbounded analytics results.
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)

        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.run_id,
                        r.source_system,
                        r.run_type,
                        r.status,
                        r.research_valid,
                        r.invalid_reason,
                        r.logical_experiment_id,
                        r.experiment_id,
                        r.task_id,
                        r.loop_id,
                        r.loop_index,
                        r.node_id,
                        r.model_type,
                        r.model_catalog_id,
                        r.factor_count,
                        r.freq,
                        r.label_horizon,
                        r.completed_at,
                        r.archived_at,
                        (SELECT COUNT(*) FROM qe_archive.run_metric m WHERE m.run_id = r.run_id) AS metric_count,
                        (SELECT COUNT(*) FROM qe_archive.run_curve c WHERE c.run_id = r.run_id) AS curve_count,
                        (SELECT COUNT(*) FROM qe_archive.run_factor f WHERE f.run_id = r.run_id) AS factor_count_rows,
                        (SELECT COUNT(*) FROM qe_archive.run_symbol_summary s WHERE s.run_id = r.run_id) AS symbol_summary_count,
                        (SELECT COUNT(*) FROM qe_archive.run_trade t WHERE t.run_id = r.run_id) AS trade_count
                    FROM qe_archive.run r
                    {where_sql}
                    ORDER BY r.archived_at DESC NULLS LAST, r.completed_at DESC NULLS LAST, r.updated_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def get_archive_summary(self) -> dict[str, Any]:
        """Return a compact warehouse health summary for API consumers."""

        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM qe_archive.run")
                run_count = int(cur.fetchone()[0])
                cur.execute(
                    """
                    SELECT research_valid, COUNT(*)
                    FROM qe_archive.run
                    GROUP BY research_valid
                    ORDER BY research_valid
                    """
                )
                research_valid_counts = {str(row[0]).lower(): int(row[1]) for row in cur.fetchall()}
                cur.execute("SELECT COUNT(*) FROM qe_archive.outbox_event WHERE status = 'pending'")
                pending_outbox_count = int(cur.fetchone()[0])
                cur.execute(
                    """
                    SELECT status, COUNT(*)
                    FROM qe_archive.outbox_event
                    GROUP BY status
                    ORDER BY status
                    """
                )
                outbox_status_counts = {str(status): int(count) for status, count in cur.fetchall()}
                cur.execute(
                    """
                    SELECT status, COUNT(*)
                    FROM qe_archive.archive_job
                    GROUP BY status
                    ORDER BY status
                    """
                )
                archive_job_status_counts = {str(status): int(count) for status, count in cur.fetchall()}
                cur.execute("SELECT MAX(archived_at) FROM qe_archive.run")
                latest_archived_at = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM qe_archive.skip_registry WHERE archive_policy = 'SKIP'")
                skip_count = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM qe_archive.skip_registry WHERE archive_policy = 'MANUAL_ONLY'")
                manual_only_count = int(cur.fetchone()[0])
                cur.execute(
                    """
                    SELECT ingest_status, COUNT(*)
                    FROM qe_archive.ingest_history
                    GROUP BY ingest_status
                    ORDER BY ingest_status
                    """
                )
                ingest_history_status_counts = {str(status): int(count) for status, count in cur.fetchall()}
                cur.execute(
                    """
                    SELECT status, COUNT(*)
                    FROM qe_archive.backfill_run
                    GROUP BY status
                    ORDER BY status
                    """
                )
                backfill_run_status_counts = {str(status): int(count) for status, count in cur.fetchall()}
                cur.execute(
                    """
                    SELECT source_type, status, mode, backfill_run_id, updated_at
                    FROM qe_archive.bootstrap_marker
                    ORDER BY updated_at DESC
                    """
                )
                bootstrap_marker_status = self._fetch_dicts(cur)
        return {
            "run_count": run_count,
            "research_valid_counts": research_valid_counts,
            "pending_outbox_count": pending_outbox_count,
            "outbox_status_counts": outbox_status_counts,
            "archive_job_status_counts": archive_job_status_counts,
            "latest_archived_at": latest_archived_at,
            "skip_count": skip_count,
            "manual_only_count": manual_only_count,
            "ingest_history_status_counts": ingest_history_status_counts,
            "backfill_run_status_counts": backfill_run_status_counts,
            "bootstrap_marker_status": bootstrap_marker_status,
        }

    def get_run_quality_summary(self, run_id: str) -> dict[str, Any]:
        """Return row-count based completeness checks for one archived run."""

        count_tables = {
            "source_count": "run_source",
            "data_context_count": "run_data_context",
            "account_summary_count": "run_account_summary",
            "metric_count": "run_metric",
            "curve_count": "run_curve",
            "factor_count_rows": "run_factor",
            "symbol_summary_count": "run_symbol_summary",
            "trade_count": "run_trade",
            "execution_event_count": "run_execution_event",
            "artifact_count": "run_artifact",
            "raw_payload_count": "raw_payload",
            "priority_score_count": "run_priority_score",
        }
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, source_system, run_type, status, research_valid,
                           invalid_reason, freq, label_horizon, factor_count,
                           completed_at, archived_at
                    FROM qe_archive.run
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                run_row = cur.fetchone()
                if not run_row:
                    return {"run_id": run_id, "exists": False}
                run_columns = [desc[0] for desc in cur.description or []]
                run_detail = dict(zip(run_columns, run_row))

                cur.execute(
                    """
                    SELECT config_capture_complete, jsonb_array_length(missing_config_items)
                    FROM qe_archive.run_config
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                config_row = cur.fetchone()

                cur.execute(
                    """
                    SELECT reproducibility_level, verification_status, jsonb_array_length(missing_items)
                    FROM qe_archive.run_reproducibility_manifest
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                manifest_row = cur.fetchone()

                counts: dict[str, int] = {}
                for key, table in count_tables.items():
                    cur.execute(f"SELECT COUNT(*) FROM qe_archive.{table} WHERE run_id = %s", (run_id,))
                    counts[key] = int(cur.fetchone()[0])

        return {
            "run_id": run_id,
            "exists": True,
            **run_detail,
            "config_capture_complete": config_row[0] if config_row else None,
            "missing_config_item_count": int(config_row[1]) if config_row else None,
            "reproducibility_level": manifest_row[0] if manifest_row else None,
            "manifest_verification_status": manifest_row[1] if manifest_row else None,
            "manifest_missing_item_count": int(manifest_row[2]) if manifest_row else None,
            **counts,
        }

    def query_factor_usage(self, *, limit: int = 50, min_runs: int = 1) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        min_runs = max(1, int(min_runs or 1))
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        f.factor_name,
                        COUNT(DISTINCT f.run_id) AS run_count,
                        MAX(r.completed_at) AS latest_completed_at,
                        AVG(r.score_total) AS avg_score_total,
                        SUM(CASE WHEN r.research_valid THEN 1 ELSE 0 END) AS research_valid_count
                    FROM qe_archive.run_factor f
                    JOIN qe_archive.run r ON r.run_id = f.run_id
                    GROUP BY f.factor_name
                    HAVING COUNT(DISTINCT f.run_id) >= %s
                    ORDER BY run_count DESC, avg_score_total DESC NULLS LAST
                    LIMIT %s
                    """,
                    (min_runs, limit),
                )
                return self._fetch_dicts(cur)

    def query_factor_importance(
        self,
        *,
        run_id: str | None = None,
        task_id: str | None = None,
        loop_index: int | None = None,
        factor_name: str | None = None,
        method: str | None = None,
        limit: int = 50,
        order: str = "desc",
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        direction = "ASC" if str(order or "").lower() == "asc" else "DESC"
        filters: list[str] = []
        params: list[Any] = []
        if run_id:
            filters.append("i.run_id = %s")
            params.append(run_id)
        if task_id:
            filters.append("r.task_id = %s")
            params.append(task_id)
        if loop_index is not None:
            filters.append("r.loop_index = %s")
            params.append(int(loop_index))
        if factor_name:
            filters.append("i.factor_name = %s")
            params.append(factor_name)
        if method:
            filters.append("i.method = %s")
            params.append(method)
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        i.run_id,
                        r.task_id,
                        r.loop_index,
                        r.experiment_id,
                        r.logical_experiment_id,
                        r.model_family AS run_model_family,
                        r.model_type AS run_model_type,
                        r.factor_set_hash AS run_factor_set_hash,
                        r.factor_count,
                        r.freq,
                        r.label_horizon,
                        r.score_total,
                        r.research_valid,
                        r.invalid_reason,
                        r.completed_at,
                        s.source_system,
                        s.source_type,
                        s.source_id,
                        s.source_sub_id,
                        s.mlflow_artifact_uri,
                        c.config_sha256,
                        c.factor_set_hash AS config_factor_set_hash,
                        c.model_params,
                        c.strategy_config,
                        c.data_split,
                        c.execution_config,
                        c.runtime_flags,
                        repro.random_seed,
                        (repro.manifest_json ->> 'seed_policy') AS seed_policy,
                        repro.reproducibility_level,
                        repro.verification_status,
                        repro.deterministic_flags,
                        repro.package_versions,
                        repro.artifact_manifest_sha256,
                        dc.train_start,
                        dc.train_end,
                        dc.valid_start,
                        dc.valid_end,
                        dc.test_start,
                        dc.test_end,
                        dc.backtest_start,
                        dc.backtest_end,
                        acc.cagr,
                        acc.total_return,
                        acc.max_drawdown,
                        acc.sharpe,
                        acc.avg_cash_ratio,
                        acc.final_cash_ratio,
                        i.factor_name,
                        i.feature_name,
                        i.feature_index,
                        i.model_family,
                        i.model_type,
                        i.method,
                        i.split_name,
                        i.importance_value,
                        i.normalized_value,
                        i.weight_pct,
                        i.signed_value,
                        i.rank_in_run,
                        i.reliability,
                        i.metadata,
                        i.created_at
                    FROM qe_archive.run_factor_importance i
                    JOIN qe_archive.run r ON r.run_id = i.run_id
                    LEFT JOIN qe_archive.run_config c ON c.run_id = i.run_id
                    LEFT JOIN qe_archive.run_reproducibility_manifest repro ON repro.run_id = i.run_id
                    LEFT JOIN qe_archive.run_account_summary acc ON acc.run_id = i.run_id
                    LEFT JOIN qe_archive.run_data_context dc ON dc.run_id = i.run_id AND dc.context_type = 'primary'
                    LEFT JOIN qe_archive.run_source s ON s.run_id = i.run_id
                    {where_sql}
                    ORDER BY i.normalized_value {direction} NULLS LAST,
                             i.importance_value {direction},
                             i.created_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_factor_importance_stability(
        self,
        *,
        factor_name: str | None = None,
        method: str | None = None,
        min_runs: int = 2,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        min_runs = max(1, int(min_runs or 2))
        filters: list[str] = []
        params: list[Any] = []
        if factor_name:
            filters.append("i.factor_name = %s")
            params.append(factor_name)
        if method:
            filters.append("i.method = %s")
            params.append(method)
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.extend([min_runs, limit])
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        i.factor_name,
                        i.method,
                        COUNT(DISTINCT i.run_id) AS run_count,
                        COUNT(DISTINCT repro.random_seed) FILTER (WHERE repro.random_seed IS NOT NULL) AS distinct_seed_count,
                        ARRAY_AGG(DISTINCT repro.random_seed) FILTER (WHERE repro.random_seed IS NOT NULL) AS random_seeds,
                        ARRAY_AGG(DISTINCT repro.reproducibility_level) FILTER (WHERE repro.reproducibility_level IS NOT NULL) AS reproducibility_levels,
                        ARRAY_AGG(DISTINCT repro.verification_status) FILTER (WHERE repro.verification_status IS NOT NULL) AS verification_statuses,
                        SUM(CASE WHEN lower(COALESCE(c.runtime_flags ->> 'enable_sector_hmm', 'false')) IN ('1','true','yes','on') THEN 1 ELSE 0 END) AS hmm_enabled_run_count,
                        SUM(CASE WHEN lower(COALESCE(c.runtime_flags ->> 'enable_sector_hmm', 'false')) NOT IN ('1','true','yes','on') THEN 1 ELSE 0 END) AS no_hmm_run_count,
                        AVG(i.normalized_value) AS avg_normalized_value,
                        AVG(i.weight_pct) AS avg_weight_pct,
                        STDDEV_POP(i.normalized_value) AS std_normalized_value,
                        MIN(i.normalized_value) AS min_normalized_value,
                        MAX(i.normalized_value) AS max_normalized_value,
                        AVG(i.rank_in_run) AS avg_rank,
                        MIN(i.rank_in_run) AS best_rank,
                        AVG(acc.cagr) AS avg_cagr,
                        AVG(acc.total_return) AS avg_total_return,
                        AVG(acc.max_drawdown) AS avg_max_drawdown,
                        AVG(acc.sharpe) AS avg_sharpe,
                        AVG(acc.avg_cash_ratio) AS avg_cash_ratio,
                        AVG(acc.final_cash_ratio) AS avg_final_cash_ratio,
                        MAX(r.completed_at) AS latest_completed_at
                    FROM qe_archive.run_factor_importance i
                    JOIN qe_archive.run r ON r.run_id = i.run_id
                    LEFT JOIN qe_archive.run_config c ON c.run_id = i.run_id
                    LEFT JOIN qe_archive.run_reproducibility_manifest repro ON repro.run_id = i.run_id
                    LEFT JOIN qe_archive.run_account_summary acc ON acc.run_id = i.run_id
                    LEFT JOIN qe_archive.run_data_context dc ON dc.run_id = i.run_id AND dc.context_type = 'primary'
                    LEFT JOIN qe_archive.run_source s ON s.run_id = i.run_id
                    {where_sql}
                    GROUP BY i.factor_name, i.method
                    HAVING COUNT(DISTINCT i.run_id) >= %s
                    ORDER BY avg_normalized_value DESC NULLS LAST, run_count DESC
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_model_trials(self, *, model_type: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        params: list[Any] = []
        where_sql = ""
        if model_type:
            where_sql = "WHERE r.model_type = %s OR t.model_type = %s"
            params.extend([model_type, model_type])
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.run_id,
                        r.task_id,
                        r.loop_index,
                        r.experiment_id,
                        COALESCE(t.model_type, r.model_type) AS model_type,
                        t.trial_id,
                        t.params,
                        t.objective_name,
                        t.objective_value,
                        r.score_total,
                        r.research_valid,
                        r.completed_at
                    FROM qe_archive.run r
                    LEFT JOIN qe_archive.run_model_trial t ON t.run_id = r.run_id
                    {where_sql}
                    ORDER BY r.completed_at DESC NULLS LAST, r.archived_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_seed_trials(self, *, limit: int = 50) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.run_id,
                        r.task_id,
                        r.loop_index,
                        r.experiment_id,
                        repro.random_seed,
                        r.model_type,
                        r.score_total,
                        r.research_valid,
                        r.completed_at
                    FROM qe_archive.run r
                    JOIN qe_archive.run_reproducibility_manifest repro ON repro.run_id = r.run_id
                    WHERE repro.random_seed IS NOT NULL
                    ORDER BY r.completed_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    (limit,),
                )
                return self._fetch_dicts(cur)

    def query_hyperparam_history(
        self,
        *,
        model_type: str | None = None,
        param_key: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        params: list[Any] = []
        filters: list[str] = []
        if model_type:
            filters.append("r.model_type = %s")
            params.append(model_type)
        if param_key:
            filters.append("(c.model_params ? %s OR t.params ? %s)")
            params.extend([param_key, param_key])
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.run_id,
                        r.task_id,
                        r.loop_index,
                        r.experiment_id,
                        r.model_type,
                        c.model_params,
                        t.params AS trial_params,
                        t.objective_name,
                        t.objective_value,
                        r.score_total,
                        r.completed_at
                    FROM qe_archive.run r
                    LEFT JOIN qe_archive.run_config c ON c.run_id = r.run_id
                    LEFT JOIN qe_archive.run_model_trial t ON t.run_id = r.run_id
                    {where_sql}
                    ORDER BY r.completed_at DESC NULLS LAST, r.archived_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def get_analytics_view_status(self) -> list[dict[str, Any]]:
        """Return compact availability and row-count metadata for analytics views."""

        view_names = [definition["view_name"] for definition in QE_ARCHIVE_ANALYTICS_VIEW_DEFS.values()]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.views
                    WHERE table_schema = 'qe_archive'
                      AND table_name = ANY(%s)
                    """,
                    (view_names,),
                )
                existing = {str(row[0]) for row in cur.fetchall()}
                result: list[dict[str, Any]] = []
                for logical_name, definition in QE_ARCHIVE_ANALYTICS_VIEW_DEFS.items():
                    view_name = definition["view_name"]
                    item: dict[str, Any] = {
                        "logical_name": logical_name,
                        "view_name": view_name,
                        "available": view_name in existing,
                        "purpose": definition["purpose"],
                        "grain": definition["grain"],
                    }
                    if view_name in existing:
                        cur.execute(f"SELECT COUNT(*) FROM qe_archive.{view_name}")
                        item["row_count"] = int(cur.fetchone()[0])
                    result.append(item)
        return result

    def query_run_leaderboard(
        self,
        *,
        model_type: str | None = None,
        min_icir: float | None = None,
        min_ir: float | None = None,
        limit: int = 20,
        order_by: str = "cagr",
    ) -> list[dict[str, Any]]:
        limit = _clamped_limit(limit)
        filters: list[str] = []
        params: list[Any] = []
        if model_type:
            filters.append("model_type = %s")
            params.append(model_type)
        if min_icir is not None:
            filters.append("icir >= %s")
            params.append(float(min_icir))
        if min_ir is not None:
            filters.append("information_ratio >= %s")
            params.append(float(min_ir))
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        order_sql = _order_by_clause(
            order_by,
            {"cagr", "sharpe", "information_ratio", "icir", "rank_icir", "completed_at", "score_total"},
            "cagr",
        )
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT run_id, task_id, loop_index, experiment_id, model_type,
                           factor_count, label_horizon, ic, icir, rank_ic, rank_icir,
                           cagr, sharpe, information_ratio, max_drawdown, calmar,
                           random_seed, reproducibility_level, verification_status,
                           score_total, completed_at
                    FROM qe_archive.v_run_leaderboard
                    {where_sql}
                    ORDER BY {order_sql}
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_seed_robustness(
        self,
        *,
        model_type: str | None = None,
        min_seed_count: int = 2,
        stable_only: bool = False,
        limit: int = 20,
        order_by: str = "cagr_mean",
    ) -> list[dict[str, Any]]:
        limit = _clamped_limit(limit)
        filters = ["distinct_seed_count >= %s"]
        params: list[Any] = [max(1, int(min_seed_count or 2))]
        if model_type:
            filters.append("model_type = %s")
            params.append(model_type)
        if stable_only:
            filters.append("is_return_stable = TRUE")
        order_sql = _order_by_clause(
            order_by,
            {"cagr_mean", "sharpe_mean", "ir_mean", "icir_mean", "rank_icir_mean", "latest_completed_at"},
            "cagr_mean",
        )
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT factor_set_hash, model_type, label_horizon, undertrain_mode,
                           topk, run_count, distinct_seed_count, random_seeds,
                           cagr_mean, cagr_std, cagr_cv, cagr_worst, cagr_best,
                           sharpe_mean, ir_mean, ir_worst, max_drawdown_mean,
                           icir_mean, icir_std, rank_icir_mean,
                           is_return_stable, latest_completed_at
                    FROM qe_archive.v_seed_robustness
                    WHERE {' AND '.join(filters)}
                    ORDER BY {order_sql}
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_factor_performance(
        self,
        *,
        factor_name: str | None = None,
        min_runs: int = 1,
        limit: int = 20,
        order_by: str = "best_cagr",
    ) -> list[dict[str, Any]]:
        limit = _clamped_limit(limit)
        filters = ["run_count >= %s"]
        params: list[Any] = [max(1, int(min_runs or 1))]
        if factor_name:
            filters.append("factor_name = %s")
            params.append(factor_name)
        order_sql = _order_by_clause(
            order_by,
            {"best_cagr", "avg_cagr", "best_sharpe", "avg_sharpe", "best_icir", "avg_icir", "run_count", "latest_used_at"},
            "best_cagr",
        )
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT factor_name, is_alpha158, run_count, best_cagr, avg_cagr,
                           best_sharpe, avg_sharpe, best_icir, avg_icir, latest_used_at
                    FROM qe_archive.v_factor_performance
                    WHERE {' AND '.join(filters)}
                    ORDER BY {order_sql}
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_model_hyperparam_seed_perf(
        self,
        *,
        model_type: str | None = None,
        hyperparam_hash: str | None = None,
        limit: int = 20,
        order_by: str = "cagr",
    ) -> list[dict[str, Any]]:
        limit = _clamped_limit(limit)
        filters: list[str] = []
        params: list[Any] = []
        if model_type:
            filters.append("model_type = %s")
            params.append(model_type)
        if hyperparam_hash:
            filters.append("hyperparam_hash = %s")
            params.append(hyperparam_hash)
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        order_sql = _order_by_clause(
            order_by,
            {"cagr", "sharpe", "information_ratio", "icir", "objective_value", "completed_at"},
            "cagr",
        )
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT model_type, model_family, hyperparam_hash, label_horizon,
                           random_seed, objective_name, objective_value, ic, icir,
                           cagr, sharpe, information_ratio, max_drawdown,
                           run_id, task_id, loop_index, completed_at
                    FROM qe_archive.v_model_hyperparam_seed_perf
                    {where_sql}
                    ORDER BY {order_sql}
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_overfit_flags(
        self,
        *,
        suspicious_only: bool = True,
        model_type: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        limit = _clamped_limit(limit)
        filters: list[str] = []
        params: list[Any] = []
        if suspicious_only:
            filters.append("is_suspicious = TRUE")
        if model_type:
            filters.append("model_type = %s")
            params.append(model_type)
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT run_id, task_id, loop_index, model_type, label_horizon,
                           random_seed, cagr, information_ratio, icir,
                           training_failed, convergence_ratio, overfit_ratio,
                           flag_return_without_signal, flag_undertrained_highret,
                           flag_seed_outlier, is_suspicious
                    FROM qe_archive.v_overfit_flags
                    {where_sql}
                    ORDER BY is_suspicious DESC, cagr DESC NULLS LAST
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_promotion_candidates(
        self,
        *,
        model_type: str | None = None,
        min_seed_count: int = 5,
        limit: int = 20,
        order_by: str = "cagr_mean",
    ) -> list[dict[str, Any]]:
        limit = _clamped_limit(limit)
        filters = ["distinct_seed_count >= %s"]
        params: list[Any] = [max(1, int(min_seed_count or 5))]
        if model_type:
            filters.append("model_type = %s")
            params.append(model_type)
        order_sql = _order_by_clause(
            order_by,
            {"cagr_mean", "sharpe_mean", "ir_mean", "icir_mean", "rank_icir_mean", "latest_completed_at"},
            "cagr_mean",
        )
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT factor_set_hash, model_type, label_horizon, undertrain_mode,
                           topk, run_count, distinct_seed_count, random_seeds,
                           cagr_mean, cagr_std, cagr_cv, cagr_worst, cagr_best,
                           sharpe_mean, ir_mean, ir_worst, max_drawdown_mean,
                           icir_mean, rank_icir_mean, is_return_stable,
                           latest_completed_at, passes_gate
                    FROM qe_archive.v_promotion_candidates
                    WHERE {' AND '.join(filters)}
                    ORDER BY {order_sql}
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def query_evolution_lineage(
        self,
        *,
        task_id: str | None = None,
        experiment_id: str | None = None,
        model_type: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        limit = _clamped_limit(limit, default=50, maximum=200)
        filters: list[str] = []
        params: list[Any] = []
        if task_id:
            filters.append("task_id = %s")
            params.append(task_id)
        if experiment_id:
            filters.append("experiment_id = %s")
            params.append(experiment_id)
        if model_type:
            filters.append("model_type = %s")
            params.append(model_type)
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT task_id, loop_index, experiment_id, run_id, model_type,
                           label_horizon, factor_count, ic, icir, cagr, sharpe,
                           information_ratio, max_drawdown, random_seed, completed_at
                    FROM qe_archive.v_evolution_lineage
                    {where_sql}
                    ORDER BY task_id, loop_index, completed_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    params,
                )
                return self._fetch_dicts(cur)

    def upsert_metric_batch(
        self,
        metrics: Sequence[MetricRecord | Mapping[str, Any]],
        *,
        replace_existing: bool = True,
    ) -> int:
        records = [self._prepare_record(metric, METRIC_COLUMNS) for metric in metrics]
        if not records:
            return 0
        for record in records:
            self._require(record, ("run_id", "metric_key"))

        rows = [
            tuple(self._adapt_value(col, record.get(col)) for col in METRIC_COLUMNS)
            for record in records
        ]
        insert_sql = f"""
            INSERT INTO qe_archive.run_metric ({", ".join(METRIC_COLUMNS)})
            VALUES %s
        """

        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                if replace_existing:
                    self._delete_existing_metrics(cur, records)
                execute_values(cur, insert_sql, rows, page_size=1000)
        return len(records)

    def replace_run_curves(self, run_id: str, curves: Sequence[CurveRecord | Mapping[str, Any]]) -> int:
        records = [self._prepare_record(curve, CURVE_COLUMNS) for curve in curves]
        if not records:
            return 0
        for record in records:
            record["run_id"] = run_id
            self._require(record, ("run_id", "curve_key"))

        rows = [
            tuple(self._adapt_value(col, record.get(col)) for col in CURVE_COLUMNS)
            for record in records
        ]
        sql = f"""
            INSERT INTO qe_archive.run_curve ({", ".join(CURVE_COLUMNS)})
            VALUES %s
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM qe_archive.run_curve WHERE run_id = %s", (run_id,))
                execute_values(cur, sql, rows, page_size=1000)
        return len(records)

    def replace_run_factors(self, run_id: str, factors: Sequence[RunFactorRecord | Mapping[str, Any]]) -> int:
        records = [self._prepare_record(factor, FACTOR_COLUMNS) for factor in factors]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM qe_archive.run_factor WHERE run_id = %s", (run_id,))
                if not records:
                    return 0
                for record in records:
                    record["run_id"] = run_id
                    self._require(record, ("run_id", "factor_name"))
                rows = [
                    tuple(self._adapt_value(col, record.get(col)) for col in FACTOR_COLUMNS)
                    for record in records
                ]
                # ALGO-COMPLEXITY-001: ingestion writes one run at a time and
                # uses execute_values paging, so batch size is bounded by the
                # source run's extracted factor list rather than full market data.
                sql = f"""
                    INSERT INTO qe_archive.run_factor ({", ".join(FACTOR_COLUMNS)})
                    VALUES %s
                """
                execute_values(cur, sql, rows, page_size=1000)
        return len(records)

    def replace_run_factor_importance(
        self,
        run_id: str,
        importances: Sequence[RunFactorImportanceRecord | Mapping[str, Any]],
    ) -> int:
        records = [self._prepare_record(item, FACTOR_IMPORTANCE_COLUMNS) for item in importances]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM qe_archive.run_factor_importance WHERE run_id = %s", (run_id,))
                if not records:
                    return 0
                for record in records:
                    record["run_id"] = run_id
                    self._require(record, ("run_id", "factor_name", "method", "importance_value"))
                    record.setdefault("reliability", "unknown")
                    record.setdefault("metadata", {})
                rows = [
                    tuple(self._adapt_value(col, record.get(col)) for col in FACTOR_IMPORTANCE_COLUMNS)
                    for record in records
                ]
                sql = f"""
                    INSERT INTO qe_archive.run_factor_importance ({", ".join(FACTOR_IMPORTANCE_COLUMNS)})
                    VALUES %s
                """
                execute_values(cur, sql, rows, page_size=1000)
        return len(records)

    def replace_run_symbol_summaries(
        self,
        run_id: str,
        summaries: Sequence[SymbolSummaryRecord | Mapping[str, Any]],
    ) -> int:
        records = [self._prepare_record(summary, SYMBOL_SUMMARY_COLUMNS) for summary in summaries]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM qe_archive.run_symbol_summary WHERE run_id = %s", (run_id,))
                if not records:
                    return 0
                for record in records:
                    record["run_id"] = run_id
                    self._require(record, ("run_id", "symbol", "source_list"))
                rows = [
                    tuple(self._adapt_value(col, record.get(col)) for col in SYMBOL_SUMMARY_COLUMNS)
                    for record in records
                ]
                sql = f"""
                    INSERT INTO qe_archive.run_symbol_summary ({", ".join(SYMBOL_SUMMARY_COLUMNS)})
                    VALUES %s
                    ON CONFLICT (run_id, source_list, symbol) DO UPDATE SET
                        profit = EXCLUDED.profit,
                        profit_pct = EXCLUDED.profit_pct,
                        avg_cost = EXCLUDED.avg_cost,
                        last_price = EXCLUDED.last_price,
                        holding_days = EXCLUDED.holding_days,
                        first_date = EXCLUDED.first_date,
                        last_date = EXCLUDED.last_date,
                        rank_in_list = EXCLUDED.rank_in_list,
                        metadata = EXCLUDED.metadata
                """
                execute_values(cur, sql, rows, page_size=1000)
        return len(records)

    def replace_run_trades(self, run_id: str, trades: Sequence[TradeRecord | Mapping[str, Any]]) -> int:
        records = [self._prepare_record(trade, TRADE_COLUMNS) for trade in trades]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM qe_archive.run_trade WHERE run_id = %s", (run_id,))
                if not records:
                    return 0
                for record in records:
                    record["run_id"] = run_id
                    self._require(record, ("run_id", "symbol"))
                rows = [
                    tuple(self._adapt_value(col, record.get(col)) for col in TRADE_COLUMNS)
                    for record in records
                ]
                sql = f"""
                    INSERT INTO qe_archive.run_trade ({", ".join(TRADE_COLUMNS)})
                    VALUES %s
                """
                execute_values(cur, sql, rows, page_size=1000)
        return len(records)

    def replace_run_execution_events(
        self,
        run_id: str,
        events: Sequence[ExecutionEventRecord | Mapping[str, Any]],
    ) -> int:
        records = [self._prepare_record(event, EXECUTION_EVENT_COLUMNS) for event in events]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM qe_archive.run_execution_event WHERE run_id = %s", (run_id,))
                if not records:
                    return 0
                for record in records:
                    record["run_id"] = run_id
                    self._require(record, ("run_id", "event_ts", "event_type", "severity"))
                rows = [
                    tuple(self._adapt_value(col, record.get(col)) for col in EXECUTION_EVENT_COLUMNS)
                    for record in records
                ]
                sql = f"""
                    INSERT INTO qe_archive.run_execution_event ({", ".join(EXECUTION_EVENT_COLUMNS)})
                    VALUES %s
                """
                execute_values(cur, sql, rows, page_size=1000)
        return len(records)

    def upsert_artifact_manifest(
        self,
        run_id: str,
        artifacts: Sequence[Mapping[str, Any]],
        *,
        replace_existing: bool = True,
    ) -> int:
        if not artifacts:
            return 0

        columns = (
            "run_id",
            "artifact_type",
            "artifact_name",
            "storage_tier",
            "artifact_uri",
            "local_rel_path",
            "source_system",
            "source_uri",
            "source_node_id",
            "sha256",
            "size_bytes",
            "content_type",
            "compression",
            "collected_status",
            "collected_at",
            "parser_status",
            "parser_error",
            "metadata",
        )
        records = []
        for artifact in artifacts:
            record = dict(artifact)
            record["run_id"] = run_id
            record.setdefault("storage_tier", "local_hot")
            record.setdefault("collected_status", "pending")
            record.setdefault("parser_status", "not_required")
            record.setdefault("metadata", {})
            self._require(record, ("run_id", "artifact_type", "artifact_name", "artifact_uri"))
            records.append(record)

        rows = [
            tuple(self._adapt_value(col, record.get(col)) for col in columns)
            for record in records
        ]
        sql = f"""
            INSERT INTO qe_archive.run_artifact ({", ".join(columns)})
            VALUES %s
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                if replace_existing:
                    for record in records:
                        cur.execute(
                            """
                            DELETE FROM qe_archive.run_artifact
                            WHERE run_id = %s AND artifact_type = %s AND artifact_name = %s
                            """,
                            (run_id, record["artifact_type"], record["artifact_name"]),
                        )
                execute_values(cur, sql, rows, page_size=500)
        return len(records)

    @staticmethod
    def _prepare_record(
        record: Mapping[str, Any] | Any,
        allowed_columns: Iterable[str],
        *,
        defaults: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        raw = to_record_dict(record)
        if defaults:
            for key, value in defaults.items():
                raw.setdefault(key, value)
        allowed = set(allowed_columns)
        return {key: value for key, value in raw.items() if key in allowed}

    @staticmethod
    def _require(record: Mapping[str, Any], columns: Sequence[str]) -> None:
        missing = [column for column in columns if record.get(column) is None]
        if missing:
            raise ValueError(f"missing required QE archive fields: {', '.join(missing)}")

    @staticmethod
    def _adapt_value(column: str, value: Any) -> Any:
        if column in JSON_COLUMNS:
            if value is None:
                return None
            return Json(normalize_json(value), dumps=canonical_json_dumps)
        return value

    @staticmethod
    def _fetch_dicts(cur: Any) -> list[dict[str, Any]]:
        rows = cur.fetchall()
        if not rows:
            return []
        if isinstance(rows[0], Mapping):
            return [dict(row) for row in rows]
        if not cur.description:
            return []
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]

    @staticmethod
    def _delete_existing_metrics(cur: Any, records: Sequence[Mapping[str, Any]]) -> None:
        seen: set[tuple[Any, ...]] = set()
        for record in records:
            key = (
                record.get("run_id"),
                record.get("metric_key"),
                record.get("metric_scope") or "run",
                record.get("source_key"),
                record.get("period_start"),
                record.get("period_end"),
                record.get("horizon"),
                record.get("freq"),
            )
            if key in seen:
                continue
            seen.add(key)
            cur.execute(
                """
                DELETE FROM qe_archive.run_metric
                WHERE run_id = %s
                  AND metric_key = %s
                  AND metric_scope = %s
                  AND source_key IS NOT DISTINCT FROM %s
                  AND period_start IS NOT DISTINCT FROM %s
                  AND period_end IS NOT DISTINCT FROM %s
                  AND horizon IS NOT DISTINCT FROM %s
                  AND freq IS NOT DISTINCT FROM %s
                """,
                key,
            )
