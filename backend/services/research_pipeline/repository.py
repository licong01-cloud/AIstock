"""Persistence boundary for Research Pipeline metadata."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from psycopg2.extras import Json

from backend.db.pg_pool import get_conn
from backend.services.qe_archive.models import canonical_json_dumps, normalize_json

from .models import (
    ArtifactRefRecord,
    ComparisonRecord,
    ExperimentRecord,
    ExternalRunLinkRecord,
    PipelineEventRecord,
    StageAttemptRecord,
    StagePlanRecord,
)

ConnectionProvider = Callable[[], Any]

EXPERIMENT_COLUMNS = (
    "experiment_id",
    "pipeline_type",
    "title",
    "description",
    "status",
    "criteria_json",
    "baseline_ref_json",
    "issue_url",
    "blocked_reason",
    "metadata_json",
    "created_by",
    "validated_at",
    "promotion_requested_at",
    "promoted_at",
    "rejected_at",
    "blocked_at",
)
STAGE_PLAN_COLUMNS = (
    "stage_id",
    "experiment_id",
    "stage_name",
    "stage_order",
    "status",
    "planned_config_json",
    "latest_attempt_no",
)
STAGE_ATTEMPT_COLUMNS = (
    "stage_attempt_id",
    "stage_id",
    "experiment_id",
    "stage_name",
    "attempt_no",
    "status",
    "input_json",
    "result_json",
    "error_message",
    "started_at",
    "completed_at",
)
EXTERNAL_RUN_LINK_COLUMNS = (
    "link_id",
    "experiment_id",
    "stage_attempt_id",
    "run_type",
    "external_id",
    "external_url",
    "status",
    "metadata_json",
)
ARTIFACT_REF_COLUMNS = (
    "artifact_ref_id",
    "experiment_id",
    "stage_attempt_id",
    "domain_type",
    "domain_id",
    "artifact_uri",
    "artifact_sha256",
    "status",
    "metadata_json",
)
COMPARISON_COLUMNS = (
    "comparison_id",
    "experiment_id",
    "stage_attempt_id",
    "baseline_ref_json",
    "candidate_ref_json",
    "metrics_json",
    "criteria_json",
    "verdict",
    "reason_md",
    "created_by",
)
PIPELINE_EVENT_COLUMNS = (
    "event_id",
    "experiment_id",
    "stage_attempt_id",
    "event_type",
    "severity",
    "message",
    "payload_json",
    "created_by",
)
JSON_COLUMNS = {
    "criteria_json",
    "baseline_ref_json",
    "metadata_json",
    "planned_config_json",
    "input_json",
    "result_json",
    "candidate_ref_json",
    "metrics_json",
    "payload_json",
}


class ResearchPipelineRepository:
    def __init__(self, connection_provider: ConnectionProvider = get_conn) -> None:
        self._connection_provider = connection_provider

    def create_experiment(self, record: ExperimentRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = ExperimentRecord(**dict(record))
        return self._insert("research_pipeline.experiment", EXPERIMENT_COLUMNS, record.model_dump())

    def list_experiments(
        self,
        *,
        status: str | None = None,
        pipeline_type: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        offset = max(0, int(offset or 0))
        filters: list[str] = []
        params: list[Any] = []
        if status:
            filters.append("status = %s")
            params.append(status)
        if pipeline_type:
            filters.append("pipeline_type = %s")
            params.append(pipeline_type)
        if search:
            like = f"%{search.strip()}%"
            filters.append("(experiment_id ILIKE %s OR title ILIKE %s OR COALESCE(description, '') ILIKE %s)")
            params.extend([like, like, like])
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.extend([limit, offset])
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT * FROM research_pipeline.experiment
                    {where_sql}
                    ORDER BY updated_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    params,
                )
                return self._rows(cur)

    def get_experiment(self, experiment_id: str) -> dict[str, Any] | None:
        return self._get_one("SELECT * FROM research_pipeline.experiment WHERE experiment_id = %s", (experiment_id,))

    def update_experiment(self, experiment_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        return self._update("research_pipeline.experiment", "experiment_id", experiment_id, EXPERIMENT_COLUMNS, updates)

    def create_stage_plan(self, record: StagePlanRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = StagePlanRecord(**dict(record))
        return self._insert("research_pipeline.stage_plan", STAGE_PLAN_COLUMNS, record.model_dump())

    def list_stage_plans(self, experiment_id: str) -> list[dict[str, Any]]:
        return self._query(
            "SELECT * FROM research_pipeline.stage_plan WHERE experiment_id = %s ORDER BY stage_order ASC",
            (experiment_id,),
        )

    def get_stage_plan(self, experiment_id: str, stage_name: str) -> dict[str, Any] | None:
        return self._get_one(
            "SELECT * FROM research_pipeline.stage_plan WHERE experiment_id = %s AND stage_name = %s",
            (experiment_id, stage_name),
        )

    def update_stage_plan(self, stage_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        return self._update("research_pipeline.stage_plan", "stage_id", stage_id, STAGE_PLAN_COLUMNS, updates)

    def create_stage_attempt(self, record: StageAttemptRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = StageAttemptRecord(**dict(record))
        return self._insert("research_pipeline.stage_attempt", STAGE_ATTEMPT_COLUMNS, record.model_dump())

    def update_stage_attempt(self, stage_attempt_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        return self._update(
            "research_pipeline.stage_attempt",
            "stage_attempt_id",
            stage_attempt_id,
            STAGE_ATTEMPT_COLUMNS,
            updates,
        )

    def list_stage_attempts(self, experiment_id: str, stage_name: str | None = None) -> list[dict[str, Any]]:
        if stage_name is None:
            return self._query(
                """
                SELECT * FROM research_pipeline.stage_attempt
                WHERE experiment_id = %s
                ORDER BY stage_name ASC, attempt_no ASC
                """,
                (experiment_id,),
            )
        return self._query(
            """
            SELECT * FROM research_pipeline.stage_attempt
            WHERE experiment_id = %s AND stage_name = %s
            ORDER BY attempt_no ASC
            """,
            (experiment_id, stage_name),
        )

    def next_attempt_no(self, experiment_id: str, stage_name: str) -> int:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(attempt_no), 0) + 1
                    FROM research_pipeline.stage_attempt
                    WHERE experiment_id = %s AND stage_name = %s
                    """,
                    (experiment_id, stage_name),
                )
                value = cur.fetchone()[0]
                return int(value)

    def create_external_run_link(self, record: ExternalRunLinkRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = ExternalRunLinkRecord(**dict(record))
        return self._insert("research_pipeline.external_run_link", EXTERNAL_RUN_LINK_COLUMNS, record.model_dump())

    def list_external_run_links(self, experiment_id: str) -> list[dict[str, Any]]:
        return self._query(
            "SELECT * FROM research_pipeline.external_run_link WHERE experiment_id = %s ORDER BY created_at DESC",
            (experiment_id,),
        )

    def create_artifact_ref(self, record: ArtifactRefRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = ArtifactRefRecord(**dict(record))
        return self._insert(
            "research_pipeline.artifact_ref",
            ARTIFACT_REF_COLUMNS,
            record.model_dump(),
            on_conflict=(
                "ON CONFLICT ("
                "experiment_id, domain_type, "
                "(COALESCE(domain_id, '')), "
                "(COALESCE(artifact_uri, '')), "
                "(COALESCE(artifact_sha256, ''))"
                ") "
                "DO UPDATE SET status = EXCLUDED.status, metadata_json = EXCLUDED.metadata_json, updated_at = NOW()"
            ),
        )

    def list_artifact_refs(
        self,
        experiment_id: str,
        *,
        domain_type: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 100), 500))
        filters = ["experiment_id = %s"]
        params: list[Any] = [experiment_id]
        if domain_type:
            filters.append("domain_type = %s")
            params.append(domain_type)
        if status:
            filters.append("status = %s")
            params.append(status)
        params.append(limit)
        return self._query(
            f"""
            SELECT * FROM research_pipeline.artifact_ref
            WHERE {' AND '.join(filters)}
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            tuple(params),
        )

    def create_comparison(self, record: ComparisonRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = ComparisonRecord(**dict(record))
        return self._insert("research_pipeline.comparison", COMPARISON_COLUMNS, record.model_dump())

    def list_comparisons(self, experiment_id: str) -> list[dict[str, Any]]:
        return self._query(
            "SELECT * FROM research_pipeline.comparison WHERE experiment_id = %s ORDER BY created_at DESC",
            (experiment_id,),
        )

    def create_pipeline_event(self, record: PipelineEventRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = PipelineEventRecord(**dict(record))
        return self._insert("research_pipeline.pipeline_event", PIPELINE_EVENT_COLUMNS, record.model_dump())

    def list_pipeline_events(self, experiment_id: str) -> list[dict[str, Any]]:
        return self._query(
            "SELECT * FROM research_pipeline.pipeline_event WHERE experiment_id = %s ORDER BY created_at DESC",
            (experiment_id,),
        )

    def _insert(
        self,
        table: str,
        columns: tuple[str, ...],
        values: Mapping[str, Any],
        *,
        on_conflict: str | None = None,
    ) -> dict[str, Any]:
        row = {column: values.get(column) for column in columns if column in values}
        sql = f"""
            INSERT INTO {table} ({', '.join(row)})
            VALUES ({', '.join(['%s'] * len(row))})
            {on_conflict or ''}
            RETURNING *
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt(column, row[column]) for column in row])
                return self._row(cur)

    def _update(
        self,
        table: str,
        key_column: str,
        key_value: str,
        allowed_columns: tuple[str, ...],
        updates: Mapping[str, Any],
    ) -> dict[str, Any]:
        row = {key: value for key, value in dict(updates).items() if key in allowed_columns and key != key_column}
        if not row:
            existing = self._get_one(f"SELECT * FROM {table} WHERE {key_column} = %s", (key_value,))
            if not existing:
                raise ValueError(f"record not found: {key_value}")
            return existing
        set_sql = ", ".join(f"{column} = %s" for column in row)
        values = [self._adapt(column, row[column]) for column in row]
        values.append(key_value)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {table} SET {set_sql}, updated_at = NOW() WHERE {key_column} = %s RETURNING *",
                    values,
                )
                if cur.rowcount == 0:
                    raise ValueError(f"record not found: {key_value}")
                return self._row(cur)

    def _query(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return self._rows(cur)

    def _get_one(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        rows = self._query(sql, params)
        return rows[0] if rows else None

    @staticmethod
    def _adapt(column: str, value: Any) -> Any:
        if column in JSON_COLUMNS:
            return Json(normalize_json(value or {}), dumps=canonical_json_dumps)
        return value

    @staticmethod
    def _rows(cur: Any) -> list[dict[str, Any]]:
        rows = cur.fetchall()
        if not rows:
            return []
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in rows]

    @classmethod
    def _row(cls, cur: Any) -> dict[str, Any]:
        rows = cls._rows(cur)
        if not rows:
            raise ValueError("research pipeline repository write returned no row")
        return rows[0]
