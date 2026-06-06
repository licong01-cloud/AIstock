"""Repository for qe_execution_templates."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from psycopg2.extras import Json

from backend.db.pg_pool import get_conn
from backend.services.qe_archive.models import canonical_json_dumps, normalize_json, sha256_json

from .models import QETemplateRecord

ConnectionProvider = Callable[[], Any]
JSON_COLUMNS = {
    "config_json", "source_context_json", "validation_json", "approval_json",
    "proposed_metrics_json", "data_versions_json", "runtime_diff_json",
    "actual_metrics_json", "metric_delta_json",
}
COLUMNS = (
    "template_id", "template_kind", "status", "title", "description", "config_json",
    "config_sha256", "archive_policy", "archive_reason", "source_context_json",
    "analysis_summary_md", "risk_summary_md", "validation_json", "approval_json",
    "parent_template_id", "proposed_metrics_json", "created_by_type", "created_by_name",
    "data_versions_json", "submitted_experiment_id", "submitted_task_id", "runtime_config_sha256",
    "runtime_diff_json", "actual_metrics_json", "metric_delta_json",
)
PENDING_HARD_DELETE_STATUSES = ("approved", "draft", "ready_for_review")


def hard_delete_blocker(template_id: str, row: Mapping[str, Any] | None) -> str | None:
    if not row:
        return f"template not found: {template_id}"
    status = str(row.get("status") or "")
    if status not in PENDING_HARD_DELETE_STATUSES:
        return (
            "template hard delete is only allowed before materialization/execution; "
            f"status={status or '<empty>'}"
        )
    if row.get("submitted_experiment_id") or row.get("submitted_task_id") or row.get("runtime_config_sha256"):
        return "template hard delete is blocked because runtime materialization history exists"
    return None


class QETemplateRepository:
    def __init__(self, connection_provider: ConnectionProvider = get_conn) -> None:
        self._connection_provider = connection_provider

    def create(self, record: QETemplateRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = QETemplateRecord(**dict(record))
        row = {column: getattr(record, column) for column in COLUMNS if hasattr(record, column)}
        columns = list(row)
        assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns if column != "template_id")
        sql = f"""
            INSERT INTO qe_execution_templates ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (template_id) DO UPDATE SET
                {assignments},
                updated_at = NOW()
            RETURNING *
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt(col, row.get(col)) for col in columns])
                return self._row(cur)

    def list(
        self,
        *,
        status: str | None = None,
        template_kind: str | None = None,
        created_by_type: str | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 100), 500))
        offset = max(0, int(offset or 0))
        filters: list[str] = []
        params: list[Any] = []
        if status:
            filters.append("status = %s")
            params.append(status)
        if template_kind:
            filters.append("template_kind = %s")
            params.append(template_kind)
        if created_by_type:
            filters.append("created_by_type = %s")
            params.append(created_by_type)
        if search:
            like = f"%{search.strip()}%"
            filters.append(
                "("
                "template_id ILIKE %s OR title ILIKE %s OR COALESCE(description, '') ILIKE %s "
                "OR COALESCE(created_by_name, '') ILIKE %s OR COALESCE(submitted_experiment_id, '') ILIKE %s "
                "OR COALESCE(submitted_task_id, '') ILIKE %s"
                ")"
            )
            params.extend([like, like, like, like, like, like])
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.extend([limit, offset])
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM qe_execution_templates {where_sql} ORDER BY updated_at DESC LIMIT %s OFFSET %s",
                    params,
                )
                return self._rows(cur)

    def get(self, template_id: str) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM qe_execution_templates WHERE template_id = %s", (template_id,))
                rows = self._rows(cur)
                return rows[0] if rows else None

    def update(self, template_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        allowed = [column for column in COLUMNS if column != "template_id"]
        row = {key: value for key, value in dict(updates).items() if key in allowed}
        if "config_json" in row and "config_sha256" not in row:
            row["config_sha256"] = sha256_json(row["config_json"])
        if not row:
            existing = self.get(template_id)
            if not existing:
                raise ValueError(f"template not found: {template_id}")
            return existing
        set_sql = ", ".join(f"{column} = %s" for column in row)
        values = [self._adapt(column, row[column]) for column in row]
        values.append(template_id)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE qe_execution_templates SET {set_sql}, updated_at = NOW() WHERE template_id = %s RETURNING *",
                    values,
                )
                if cur.rowcount == 0:
                    raise ValueError(f"template not found: {template_id}")
                return self._row(cur)

    def mark_materialized(self, template_id: str, *, experiment_id: str | None = None, task_id: str | None = None, runtime_config: Mapping[str, Any] | None = None, diff: Mapping[str, Any] | None = None) -> dict[str, Any]:
        updates: dict[str, Any] = {"status": "materialized", "runtime_diff_json": dict(diff or {})}
        if experiment_id:
            updates["submitted_experiment_id"] = experiment_id
        if task_id:
            updates["submitted_task_id"] = task_id
        if runtime_config is not None:
            updates["runtime_config_sha256"] = sha256_json(runtime_config)
        return self.update(template_id, updates)

    def delete_pending(self, template_id: str) -> dict[str, Any]:
        placeholders = ", ".join(["%s"] * len(PENDING_HARD_DELETE_STATUSES))
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM qe_execution_templates
                    WHERE template_id = %s
                      AND status IN ({placeholders})
                      AND submitted_experiment_id IS NULL
                      AND submitted_task_id IS NULL
                      AND runtime_config_sha256 IS NULL
                    RETURNING *
                    """,
                    [template_id, *PENDING_HARD_DELETE_STATUSES],
                )
                if cur.rowcount:
                    return self._row(cur)
        blocker = hard_delete_blocker(template_id, self.get(template_id))
        raise ValueError(blocker or f"template hard delete was blocked: {template_id}")

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
            raise ValueError("template repository write returned no row")
        return rows[0]
