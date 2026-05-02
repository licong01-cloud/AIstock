"""Repository methods for the QE realtime experiment warehouse."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from psycopg2.extras import Json, execute_values

from backend.db.pg_pool import get_conn

from .models import (
    AccountSummaryRecord,
    ArchiveJobRecord,
    ClaimedOutboxEvent,
    CurveRecord,
    DataContextRecord,
    MetricRecord,
    OutboxEventRecord,
    RawPayloadRecord,
    ReproducibilityManifestRecord,
    RunFactorRecord,
    RunConfigRecord,
    RunSourceRecord,
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
    "factor_classification",
    "independent_metrics_snapshot",
    "official_rating_snapshot",
}


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
        assignments = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in columns
            if column not in {"run_id"}
        )
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
                sql = f"""
                    INSERT INTO qe_archive.run_factor ({", ".join(FACTOR_COLUMNS)})
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
