"""Repository/service for autonomous market data sync targets and attempts.

This module is intentionally passive: it records desired sync work and attempt
outcomes, but it does not schedule, dispatch, or execute sync jobs.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from psycopg2.extras import Json

from backend.db.pg_pool import get_conn

ConnectionProvider = Callable[[], Any]

TARGET_STATUSES = {"pending", "retry", "final_blocked", "reconciled"}
ATTEMPT_STATUSES = {"started", "failed", "retry", "final_blocked", "reconciled"}
JSON_COLUMNS = {"target_scope", "metadata", "context_json"}


@dataclass(frozen=True)
class DataSyncTargetRecord:
    dataset: str
    data_source: str
    target_date: date | str | None = None
    target_scope: Mapping[str, Any] = field(default_factory=dict)
    target_id: str | None = None
    target_status: str = "pending"
    priority: int = 100
    required_before: datetime | None = None
    next_retry_at: datetime | None = None
    expected_rows: int | None = None
    observed_rows: int | None = None
    data_max_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DataSyncAttemptRecord:
    target_id: str
    status: str
    attempt_id: str | None = None
    trigger_source: str | None = None
    worker_id: str | None = None
    run_id: str | None = None
    job_id: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    rows_written: int | None = None
    rows_observed: int | None = None
    coverage_ratio: float | None = None
    data_max_at: datetime | None = None
    error_message: str | None = None
    retry_after: datetime | None = None
    context_json: Mapping[str, Any] = field(default_factory=dict)


class DataSyncTargetRepository:
    """Persistence boundary for market.data_sync_targets and attempts."""

    def __init__(self, connection_provider: ConnectionProvider = get_conn) -> None:
        self._connection_provider = connection_provider

    def upsert_target(self, record: DataSyncTargetRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = DataSyncTargetRecord(**dict(record))
        self._validate_target_status(record.target_status)
        target_scope = _normalize_mapping(record.target_scope)
        target_key_sha256 = make_target_key_sha256(
            dataset=record.dataset,
            data_source=record.data_source,
            target_date=record.target_date,
            target_scope=target_scope,
        )
        target_id = record.target_id or make_target_id(target_key_sha256)
        row = {
            "target_id": target_id,
            "dataset": record.dataset,
            "data_source": record.data_source,
            "target_date": record.target_date,
            "target_scope": target_scope,
            "target_key_sha256": target_key_sha256,
            "target_status": record.target_status,
            "priority": record.priority,
            "required_before": record.required_before,
            "next_retry_at": record.next_retry_at,
            "expected_rows": record.expected_rows,
            "observed_rows": record.observed_rows,
            "data_max_at": record.data_max_at,
            "metadata": _normalize_mapping(record.metadata),
        }
        columns = list(row)
        update_columns = [
            "target_status",
            "priority",
            "required_before",
            "next_retry_at",
            "expected_rows",
            "observed_rows",
            "data_max_at",
            "metadata",
        ]
        assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
        sql = f"""
            INSERT INTO market.data_sync_targets ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (dataset, data_source, target_key_sha256) DO UPDATE SET
                {assignments},
                updated_at = NOW()
            RETURNING *
        """
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self._adapt(column, row[column]) for column in columns])
                return self._row(cur)

    def get_target(self, target_id: str) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM market.data_sync_targets WHERE target_id = %s", (target_id,))
                rows = self._rows(cur)
                return rows[0] if rows else None

    def record_attempt(self, record: DataSyncAttemptRecord | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(record, Mapping):
            record = DataSyncAttemptRecord(**dict(record))
        self._validate_attempt_status(record.status)
        attempt_id = record.attempt_id or f"dsa_{uuid4().hex}"
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(attempt_no), 0) + 1
                    FROM market.data_sync_attempts
                    WHERE target_id = %s
                    """,
                    (record.target_id,),
                )
                attempt_no = int(cur.fetchone()[0])
                row = {
                    "attempt_id": attempt_id,
                    "target_id": record.target_id,
                    "attempt_no": attempt_no,
                    "status": record.status,
                    "trigger_source": record.trigger_source,
                    "worker_id": record.worker_id,
                    "run_id": record.run_id,
                    "job_id": record.job_id,
                    "started_at": record.started_at,
                    "finished_at": record.finished_at,
                    "rows_written": record.rows_written,
                    "rows_observed": record.rows_observed,
                    "coverage_ratio": record.coverage_ratio,
                    "data_max_at": record.data_max_at,
                    "error_message": record.error_message,
                    "retry_after": record.retry_after,
                    "context_json": _normalize_mapping(record.context_json),
                }
                columns = list(row)
                cur.execute(
                    f"""
                    INSERT INTO market.data_sync_attempts ({", ".join(columns)})
                    VALUES ({", ".join(["%s"] * len(columns))})
                    RETURNING *
                    """,
                    [self._adapt(column, row[column]) for column in columns],
                )
                attempt = self._row(cur)
                self._touch_target_after_attempt(cur, record, attempt_id)
                return attempt

    def mark_retry(
        self,
        target_id: str,
        *,
        retry_after: datetime | None = None,
        reason: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._mark_target(
            target_id,
            status="retry",
            retry_after=retry_after,
            error_message=reason,
            context=context,
        )

    def mark_final_blocked(
        self,
        target_id: str,
        *,
        reason: str,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._mark_target(target_id, status="final_blocked", error_message=reason, context=context)

    def mark_reconciled(
        self,
        target_id: str,
        *,
        observed_rows: int | None = None,
        data_max_at: datetime | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._mark_target(
            target_id,
            status="reconciled",
            observed_rows=observed_rows,
            data_max_at=data_max_at,
            context=context,
        )

    def list_fillable_targets(
        self,
        *,
        dataset: str | None = None,
        data_source: str | None = None,
        due_at: datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        due_at = due_at or datetime.now(timezone.utc)
        limit = max(1, min(int(limit or 100), 1000))
        filters = [
            "target_status IN ('pending', 'retry')",
            "(next_retry_at IS NULL OR next_retry_at <= %s)",
        ]
        params: list[Any] = [due_at]
        if dataset:
            filters.append("dataset = %s")
            params.append(dataset)
        if data_source:
            filters.append("data_source = %s")
            params.append(data_source)
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM market.data_sync_targets
                    WHERE {" AND ".join(filters)}
                    ORDER BY priority ASC, COALESCE(required_before, created_at) ASC, created_at ASC
                    LIMIT %s
                    """,
                    params,
                )
                return self._rows(cur)

    def claim_fillable_target(
        self,
        target_id: str,
        *,
        claimed_until: datetime,
        due_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        """Atomically lease one due target before creating any job or attempt row."""

        due_at = due_at or datetime.now(timezone.utc)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE market.data_sync_targets
                    SET next_retry_at = %s,
                        updated_at = NOW()
                    WHERE target_id = %s
                      AND target_status IN ('pending', 'retry')
                      AND (next_retry_at IS NULL OR next_retry_at <= %s)
                    RETURNING *
                    """,
                    (claimed_until, target_id, due_at),
                )
                rows = self._rows(cur)
                return rows[0] if rows else None

    def list_attempts(self, target_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 500))
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM market.data_sync_attempts
                    WHERE target_id = %s
                    ORDER BY attempt_no DESC
                    LIMIT %s
                    """,
                    (target_id, limit),
                )
                return self._rows(cur)

    def _touch_target_after_attempt(self, cur: Any, record: DataSyncAttemptRecord, attempt_id: str) -> None:
        next_status = record.status if record.status in {"retry", "final_blocked", "reconciled"} else None

        status_sql = ", target_status = %s" if next_status else ""
        params: list[Any] = [
            attempt_id,
            record.status,
            record.error_message,
            record.retry_after,
            record.rows_observed,
            record.data_max_at,
        ]
        if next_status:
            params.append(next_status)
        cur.execute(
            f"""
            UPDATE market.data_sync_targets
            SET attempt_count = attempt_count + 1,
                last_attempt_id = %s,
                last_attempt_status = %s,
                last_error_message = %s,
                next_retry_at = COALESCE(%s, next_retry_at),
                observed_rows = COALESCE(%s, observed_rows),
                data_max_at = COALESCE(%s, data_max_at)
                {status_sql},
                updated_at = NOW(),
                reconciled_at = CASE WHEN %s = 'reconciled' THEN NOW() ELSE reconciled_at END,
                blocked_at = CASE WHEN %s = 'final_blocked' THEN NOW() ELSE blocked_at END
            WHERE target_id = %s
            """,
            [*params, next_status, next_status, record.target_id],
        )
        if cur.rowcount == 0:
            raise ValueError(f"data sync target not found: {record.target_id}")

    def _mark_target(
        self,
        target_id: str,
        *,
        status: str,
        retry_after: datetime | None = None,
        error_message: str | None = None,
        observed_rows: int | None = None,
        data_max_at: datetime | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_target_status(status)
        metadata_patch = _normalize_mapping(context or {})
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE market.data_sync_targets
                    SET target_status = %s,
                        next_retry_at = %s,
                        last_error_message = COALESCE(%s, last_error_message),
                        observed_rows = COALESCE(%s, observed_rows),
                        data_max_at = COALESCE(%s, data_max_at),
                        metadata = metadata || %s::jsonb,
                        updated_at = NOW(),
                        reconciled_at = CASE WHEN %s = 'reconciled' THEN NOW() ELSE reconciled_at END,
                        blocked_at = CASE WHEN %s = 'final_blocked' THEN NOW() ELSE blocked_at END
                    WHERE target_id = %s
                    RETURNING *
                    """,
                    (
                        status,
                        retry_after,
                        error_message,
                        observed_rows,
                        data_max_at,
                        Json(metadata_patch, dumps=_json_dumps),
                        status,
                        status,
                        target_id,
                    ),
                )
                if cur.rowcount == 0:
                    raise ValueError(f"data sync target not found: {target_id}")
                return self._row(cur)

    @staticmethod
    def _validate_target_status(status: str) -> None:
        if status not in TARGET_STATUSES:
            raise ValueError(f"unsupported data sync target status: {status}")

    @staticmethod
    def _validate_attempt_status(status: str) -> None:
        if status not in ATTEMPT_STATUSES:
            raise ValueError(f"unsupported data sync attempt status: {status}")

    @staticmethod
    def _adapt(column: str, value: Any) -> Any:
        if column in JSON_COLUMNS:
            return Json(value or {}, dumps=_json_dumps)
        return value

    @staticmethod
    def _rows(cur: Any) -> list[dict[str, Any]]:
        rows = cur.fetchall()
        if not rows:
            return []
        if isinstance(rows[0], Mapping):
            return [dict(row) for row in rows]
        columns = [description[0] for description in cur.description]
        return [dict(zip(columns, row)) for row in rows]

    @classmethod
    def _row(cls, cur: Any) -> dict[str, Any]:
        rows = cls._rows(cur)
        if not rows:
            raise ValueError("data sync repository write returned no row")
        return rows[0]


class DataSyncTargetService:
    """Thin service wrapper that exposes the repository as the status source."""

    def __init__(self, repository: DataSyncTargetRepository | None = None) -> None:
        self.repository = repository or DataSyncTargetRepository()

    def upsert_target(self, record: DataSyncTargetRecord | Mapping[str, Any]) -> dict[str, Any]:
        return self.repository.upsert_target(record)

    def record_attempt(self, record: DataSyncAttemptRecord | Mapping[str, Any]) -> dict[str, Any]:
        return self.repository.record_attempt(record)

    def mark_retry(self, target_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.repository.mark_retry(target_id, **kwargs)

    def mark_final_blocked(self, target_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.repository.mark_final_blocked(target_id, **kwargs)

    def mark_reconciled(self, target_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.repository.mark_reconciled(target_id, **kwargs)

    def get_target(self, target_id: str) -> dict[str, Any] | None:
        return self.repository.get_target(target_id)

    def list_fillable_targets(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.repository.list_fillable_targets(**kwargs)

    def claim_fillable_target(self, target_id: str, **kwargs: Any) -> dict[str, Any] | None:
        return self.repository.claim_fillable_target(target_id, **kwargs)

    def list_attempts(self, target_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        return self.repository.list_attempts(target_id, **kwargs)


def make_target_key_sha256(
    *,
    dataset: str,
    data_source: str,
    target_date: date | str | None = None,
    target_scope: Mapping[str, Any] | None = None,
) -> str:
    payload = {
        "dataset": dataset,
        "data_source": data_source,
        "target_date": target_date,
        "target_scope": _normalize_mapping(target_scope or {}),
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def make_target_id(target_key_sha256: str) -> str:
    return f"dst_{target_key_sha256[:24]}"


def _normalize_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(_json_dumps(dict(value)))


def _json_dumps(value: Any) -> str:
    return json.dumps(_normalize_json(value), ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _normalize_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value
