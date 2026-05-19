"""Autonomous local-data sync policy and persistence helpers.

The audit ledger remains the only readiness authority. This module stores
recoverable dataset/date targets and exposes a narrow Alert Gate so routine
freshness checks cannot page operators before automated retry has finished.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator, Optional

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.data_health_alerter import DataAlert, DataHealthAlerter

ConnFactory = Callable[[], Iterator[Any]]


class SyncTargetStatus(str, Enum):
    PLANNED = "planned"
    WAITING_RELEASE = "waiting_release"
    QUEUED = "queued"
    RUNNING = "running"
    RETRY_WAITING = "retry_waiting"
    SUCCESS = "success"
    EMPTY_VALID = "empty_valid"
    FINAL_BLOCKED = "final_blocked"
    NOT_REQUIRED = "not_required"


FINAL_ALERT_STATUSES = frozenset(
    {
        "final_blocked",
        "db_unavailable",
        "provider_contract_error",
    }
)

RECOVERABLE_STATUSES = frozenset(
    {
        "planned",
        "waiting_release",
        "queued",
        "running",
        "retry_waiting",
        "pending_publish",
        "cache_stale",
        "job_success_audit_missing",
        "physical_success_audit_missing",
        "audit_success_stats_stale",
    }
)


@dataclass(frozen=True)
class SyncPolicyDecision:
    """Decision for a dataset/date readiness state."""

    state: str
    should_retry: bool = False
    should_alert: bool = False
    next_retry_at: dt.datetime | None = None
    final_deadline_at: dt.datetime | None = None
    failure_category: str | None = None
    operator_action_required: bool = False


@dataclass(frozen=True)
class SyncTargetInput:
    dataset: str
    target_date: dt.date
    status: str = SyncTargetStatus.QUEUED.value
    source: str = "data_sync_autonomy"
    reason: str = "readiness_gap"
    failure_category: str | None = None
    next_retry_at: dt.datetime | None = None
    final_deadline_at: dt.datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class DataSyncPolicyEngine:
    """Small, deterministic policy layer shared by scheduler/API/tests."""

    def decide(
        self,
        *,
        now: dt.datetime,
        release_at: dt.datetime | None,
        final_deadline_at: dt.datetime | None,
        current_status: str,
        zero_rows: bool = False,
        zero_rows_allowed: bool = False,
        attempts_exhausted: bool = False,
        failure_category: str | None = None,
    ) -> SyncPolicyDecision:
        status = (current_status or "").strip().lower()
        if release_at is not None and now < release_at:
            return SyncPolicyDecision(
                state=SyncTargetStatus.WAITING_RELEASE.value,
                should_retry=False,
                final_deadline_at=final_deadline_at,
                failure_category=failure_category,
            )

        if zero_rows and zero_rows_allowed:
            return SyncPolicyDecision(
                state=SyncTargetStatus.EMPTY_VALID.value,
                should_retry=False,
                should_alert=False,
                final_deadline_at=final_deadline_at,
            )

        after_final = final_deadline_at is not None and now >= final_deadline_at
        terminal_failure = status in FINAL_ALERT_STATUSES or attempts_exhausted
        if after_final and (zero_rows or terminal_failure or status not in {"success", "ok", "ready"}):
            category = failure_category or ("empty_invalid" if zero_rows else "retry_exhausted")
            return SyncPolicyDecision(
                state=SyncTargetStatus.FINAL_BLOCKED.value,
                should_retry=False,
                should_alert=True,
                final_deadline_at=final_deadline_at,
                failure_category=category,
                operator_action_required=True,
            )

        retry_at = now + dt.timedelta(minutes=30)
        return SyncPolicyDecision(
            state=SyncTargetStatus.RETRY_WAITING.value if zero_rows or status in RECOVERABLE_STATUSES else status,
            should_retry=True,
            should_alert=False,
            next_retry_at=retry_at,
            final_deadline_at=final_deadline_at,
            failure_category=failure_category,
        )


class DataSyncTargetRepository:
    """Persistence wrapper for market.data_sync_targets and attempts."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    @staticmethod
    def fingerprint(dataset: str, target_date: dt.date, source: str, reason: str) -> str:
        # One dataset/date has exactly one autonomous target; source/reason are
        # mutable evidence, not identity. This avoids duplicate retry/alert rows.
        payload = f"{dataset.strip().lower()}|{target_date.isoformat()}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def upsert_target(self, target: SyncTargetInput) -> str:
        target_id = str(uuid.uuid4())
        fp = self.fingerprint(target.dataset, target.target_date, target.source, target.reason)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO market.data_sync_targets (
                        target_id, dataset, target_date, status, source, reason,
                        failure_category, next_retry_at, final_deadline_at, metadata,
                        fingerprint, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (fingerprint) DO UPDATE SET
                        status = CASE
                            WHEN market.data_sync_targets.status IN (
                                'final_blocked', 'db_unavailable', 'provider_contract_error'
                            )
                            AND EXCLUDED.status NOT IN (
                                'final_blocked', 'db_unavailable', 'provider_contract_error'
                            )
                            THEN market.data_sync_targets.status
                            ELSE EXCLUDED.status
                        END,
                        source = EXCLUDED.source,
                        reason = EXCLUDED.reason,
                        failure_category = COALESCE(EXCLUDED.failure_category, market.data_sync_targets.failure_category),
                        next_retry_at = COALESCE(EXCLUDED.next_retry_at, market.data_sync_targets.next_retry_at),
                        final_deadline_at = COALESCE(EXCLUDED.final_deadline_at, market.data_sync_targets.final_deadline_at),
                        metadata = COALESCE(market.data_sync_targets.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                        updated_at = NOW()
                    RETURNING target_id::text
                    """,
                    (
                        target_id,
                        target.dataset,
                        target.target_date,
                        target.status,
                        target.source,
                        target.reason,
                        target.failure_category,
                        target.next_retry_at,
                        target.final_deadline_at,
                        psycopg2.extras.Json(target.metadata),
                        fp,
                    ),
                )
                row = cur.fetchone()
        return str(row[0])

    def list_due_targets(
        self,
        *,
        limit: int = 100,
        now: dt.datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Return persisted retry targets due for another automated attempt."""

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT target_id::text, dataset, target_date, status, source,
                           reason, failure_category, next_retry_at,
                           final_deadline_at, metadata, created_at, updated_at
                      FROM market.data_sync_targets
                     WHERE (
                           status IN ('queued', 'retry_waiting', 'waiting_release')
                           AND (next_retry_at IS NULL OR next_retry_at <= COALESCE(%s, NOW()))
                       )
                        OR (
                           status = 'running'
                           AND updated_at <= COALESCE(%s, NOW()) - INTERVAL '2 hours'
                       )
                      ORDER BY target_date ASC, updated_at ASC
                      LIMIT %s
                    """,
                    (now, now, max(int(limit), 1)),
                )
                return [dict(row) for row in cur.fetchall()]

    def record_attempt(
        self,
        *,
        target_id: str,
        job_id: str | None,
        status: str,
        started_at: dt.datetime | None = None,
        finished_at: dt.datetime | None = None,
        inserted_rows: int | None = None,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        attempt_id = str(uuid.uuid4())
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO market.data_sync_attempts (
                        attempt_id, target_id, job_id, status, started_at, finished_at,
                        inserted_rows, error_message, metadata, created_at
                    ) VALUES (%s, %s, %s, %s, COALESCE(%s, NOW()), %s, %s, %s, %s, NOW())
                    RETURNING attempt_id::text
                    """,
                    (
                        attempt_id,
                        target_id,
                        job_id,
                        status,
                        started_at,
                        finished_at,
                        inserted_rows,
                        error_message,
                        psycopg2.extras.Json(metadata or {}),
                    ),
                )
                row = cur.fetchone()
        return str(row[0])

    def update_target_status(
        self,
        *,
        target_id: str,
        status: str,
        failure_category: str | None = None,
        next_retry_at: dt.datetime | None = None,
        final_deadline_at: dt.datetime | None = None,
        metadata: dict[str, Any] | None = None,
        clear_failure: bool = False,
        clear_retry: bool = False,
    ) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE market.data_sync_targets
                       SET status = %s,
                           failure_category = CASE
                               WHEN %s THEN NULL
                               ELSE COALESCE(%s, failure_category)
                           END,
                           next_retry_at = CASE
                               WHEN %s THEN NULL
                               ELSE COALESCE(%s, next_retry_at)
                           END,
                           final_deadline_at = COALESCE(%s, final_deadline_at),
                           metadata = COALESCE(metadata, '{}'::jsonb) || %s,
                           updated_at = NOW()
                     WHERE target_id = %s
                    """,
                    (
                        status,
                        clear_failure,
                        failure_category,
                        clear_retry,
                        next_retry_at,
                        final_deadline_at,
                        psycopg2.extras.Json(metadata or {}),
                        target_id,
                    ),
                )


class DataSyncAlertGate:
    """Only final, non-recoverable target states are allowed to write alerts."""

    def __init__(self, alerter: DataHealthAlerter | None = None) -> None:
        self._alerter = alerter or DataHealthAlerter()

    def build_alert(self, target: dict[str, Any]) -> DataAlert | None:
        status = str(target.get("status") or "").strip().lower()
        failure_category = str(target.get("failure_category") or "retry_exhausted")
        if status not in FINAL_ALERT_STATUSES and status != SyncTargetStatus.FINAL_BLOCKED.value:
            return None
        dataset = str(target.get("dataset") or "?")
        target_date = target.get("target_date")
        details = {
            "target_id": target.get("target_id"),
            "target_date": str(target_date) if target_date else None,
            "failure_category": failure_category,
            "final_deadline_at": target.get("final_deadline_at"),
            "source": target.get("source"),
            "reason": target.get("reason"),
            "alert_gate": "data_sync_autonomy",
        }
        return DataAlert(
            severity="error" if status == SyncTargetStatus.FINAL_BLOCKED.value else "critical",
            dataset=dataset,
            alert_type="final_blocked",
            title=f"{dataset} 数据同步最终阻塞",
            message=f"{dataset} {target_date} 已超过最终补齐窗口，自动同步无法恢复",
            details=details,
        )

    def flush_final_alerts(self, targets: list[dict[str, Any]]) -> dict[str, int]:
        alerts = [alert for target in targets if (alert := self.build_alert(target))]
        if not alerts:
            return {}
        return self._alerter.flush(alerts)


def target_to_json(target: dict[str, Any]) -> str:
    return json.dumps(target, ensure_ascii=False, default=str)
