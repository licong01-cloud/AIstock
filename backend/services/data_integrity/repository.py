"""Transactional canonical partition ledger; no runtime DDL or background thread."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import hashlib
from typing import Any, Callable, Iterable, Mapping
from uuid import uuid4

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn
from .contracts import CANONICAL_SOURCE, CONTRACT_VERSION, ValidationResult, digest


class IntegrityRevisionChanged(RuntimeError):
    """Discard work; never publish a stale/expired worker result."""


class IntegritySchemaUnavailable(RuntimeError):
    """Rollout is pending; do not claim readiness from legacy job success."""


def schema_installed(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regprocedure('market.mark_data_integrity_changed(text,date,text,text)') IS NOT NULL", None)
        row = cur.fetchone()
        return bool(row and row[0] is True)


def mark_changed(conn, dataset: str, dates: Iterable[date], *, reason: str, source: str) -> dict[date, int]:
    """Caller owns a non-autocommit transaction; idempotent retries pass no dates.

    Pre-migration writers remain compatible but explicitly return no tracking.
    New readiness APIs require the schema and never fall back to those writes.
    """
    days = sorted(set(dates))
    if not days:
        return {}
    if conn.autocommit:
        raise ValueError("integrity changes require the fact writer's transaction")
    if not schema_installed(conn):
        return {}
    revisions = {}
    with conn.cursor() as cur:
        for day in days:
            cur.execute("SELECT market.mark_data_integrity_changed(%s,%s,%s,%s)", (dataset, day, reason, source))
            revisions[day] = int(cur.fetchone()[0])
    return revisions


def enqueue_validation(conn, dataset: str, day: date) -> None:
    """A successful empty scan is work to verify, not permission for green."""
    from .catalog import RULE_VERSION, SCOPE_VERSION, canonical_dataset
    dataset = canonical_dataset(dataset)
    if not schema_installed(conn):
        return
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO market.dataset_date_refresh_audit
            (dataset,trade_date,data_source,status,row_count,quality_status)
            VALUES (%s,%s,'canonical_db','failed',0,'unproven') ON CONFLICT DO NOTHING""", (dataset, day))
        key = hashlib.sha256(f"{dataset}\n{day}\nVALIDATE_ONLY\nphysical_partition_v1".encode()).hexdigest()
        cur.execute("""INSERT INTO market.data_sync_targets AS t
            (target_id,dataset,data_source,target_date,target_scope,target_key_sha256,target_status,priority,metadata)
            SELECT %s,dataset,'canonical_db',trade_date,
                '{"operation":"VALIDATE_ONLY","scope":"physical_partition_v1"}'::jsonb,%s,'pending',50,
                jsonb_build_object('desired_revision',data_revision,'desired_scope',%s::text,'desired_rule',%s::text)
            FROM market.dataset_date_refresh_audit WHERE dataset=%s AND trade_date=%s AND data_source='canonical_db'
              AND (validated_revision IS DISTINCT FROM data_revision OR validation_status NOT IN ('VALID','NOT_APPLICABLE')
                   OR scope_version IS DISTINCT FROM %s OR rule_version IS DISTINCT FROM %s)
            ON CONFLICT (dataset,data_source,target_key_sha256) DO UPDATE SET
                metadata=CASE WHEN t.metadata->>'desired_scope' IS DISTINCT FROM EXCLUDED.metadata->>'desired_scope'
                    OR t.metadata->>'desired_rule' IS DISTINCT FROM EXCLUDED.metadata->>'desired_rule'
                    OR t.metadata->>'desired_revision' IS DISTINCT FROM EXCLUDED.metadata->>'desired_revision'
                    THEN (t.metadata-'lease_token'-'lease_until')||EXCLUDED.metadata ELSE t.metadata||EXCLUDED.metadata END,
                target_status=CASE WHEN t.target_status='reconciled' OR t.metadata->>'desired_scope' IS DISTINCT FROM EXCLUDED.metadata->>'desired_scope'
                    OR t.metadata->>'desired_rule' IS DISTINCT FROM EXCLUDED.metadata->>'desired_rule'
                    THEN 'pending' ELSE t.target_status END,
                next_retry_at=CASE WHEN t.target_status='reconciled' OR t.metadata->>'desired_scope' IS DISTINCT FROM EXCLUDED.metadata->>'desired_scope'
                    OR t.metadata->>'desired_rule' IS DISTINCT FROM EXCLUDED.metadata->>'desired_rule'
                    THEN clock_timestamp() ELSE t.next_retry_at END,
                updated_at=clock_timestamp()""", ("dst_"+key[:24], key, SCOPE_VERSION, RULE_VERSION, dataset, day, SCOPE_VERSION, RULE_VERSION))


class IntegrityRepository:
    def __init__(self, connection_provider: Callable[..., Any] = get_conn):
        self._connect = connection_provider

    @contextmanager
    def transaction(self, *, readonly=False):
        with self._connect() as conn:
            if not conn.autocommit:
                raise ValueError("integrity repository requires an idle connection")
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" + (" READ ONLY" if readonly else ""))
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.autocommit = True

    @staticmethod
    def partitions(conn, start: date, end: date) -> list[dict[str, Any]]:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT * FROM market.dataset_date_refresh_audit
                WHERE data_source=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date,dataset""",
                (CANONICAL_SOURCE, start, end))
            return [dict(row) for row in cur.fetchall()]

    def ensure_partition(self, conn, dataset: str, day: date):
        """Empty-event scans need a revision-zero partition, never invented facts."""
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO market.dataset_date_refresh_audit
                (dataset,trade_date,data_source,status,row_count,quality_status)
                VALUES (%s,%s,'canonical_db','failed',0,'unproven') ON CONFLICT DO NOTHING""", (dataset, day))

    def claim(self, dataset: str, day: date, *, lease_seconds: int = 300) -> dict[str, Any] | None:
        if not 10 <= lease_seconds <= 1800:
            raise ValueError("validation lease must be bounded")
        token = uuid4().hex
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""UPDATE market.data_sync_targets SET
                next_retry_at=clock_timestamp()+make_interval(secs=>%s),
                metadata=metadata || jsonb_build_object('lease_token',%s::text,
                    'lease_until',clock_timestamp()+make_interval(secs=>%s),
                    'validation_started_at',clock_timestamp()), updated_at=clock_timestamp()
                WHERE dataset=%s AND target_date=%s AND data_source='canonical_db'
                  AND target_scope->>'operation'='VALIDATE_ONLY'
                  AND target_status IN ('pending','retry')
                  AND (next_retry_at IS NULL OR next_retry_at<=clock_timestamp())
                RETURNING *""", (lease_seconds, token, lease_seconds, dataset, day))
            row = cur.fetchone()
            return dict(row) if row else None

    def heartbeat(self, target_id: str, token: str, *, lease_seconds: int = 300) -> bool:
        if not 10 <= lease_seconds <= 1800:
            raise ValueError("validation lease must be bounded")
        with self.transaction() as conn, conn.cursor() as cur:
            cur.execute("""UPDATE market.data_sync_targets SET
                next_retry_at=clock_timestamp()+make_interval(secs=>%s),
                metadata=metadata || jsonb_build_object('lease_until',clock_timestamp()+make_interval(secs=>%s)),
                updated_at=clock_timestamp()
                WHERE target_id=%s AND metadata->>'lease_token'=%s
                  AND (metadata->>'lease_until')::timestamptz>clock_timestamp()
                  AND target_status IN ('pending','retry')""", (lease_seconds, lease_seconds, target_id, token))
            return cur.rowcount == 1

    def publish(self, *, dataset: str, day: date, revision: int, scope_version: str,
                rule_version: str, target_id: str, token: str, result: ValidationResult) -> dict[str, Any]:
        identity = {"dataset": dataset, "fact_date": str(day), "data_revision": revision,
                    "scope_version": scope_version, "rule_version": rule_version, "result": result.as_dict()}
        validation_id = "div_" + digest(identity)
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Same lock order as the fact writer: partition, then persistent target.
            cur.execute("""SELECT * FROM market.dataset_date_refresh_audit WHERE dataset=%s
                AND trade_date=%s AND data_source='canonical_db' FOR UPDATE""", (dataset, day))
            partition = cur.fetchone()
            cur.execute("""SELECT *, (metadata->>'lease_until')::timestamptz>clock_timestamp() AS lease_valid
                FROM market.data_sync_targets WHERE target_id=%s FOR UPDATE""", (target_id,))
            target = cur.fetchone()
            if (not partition or partition["data_revision"] != revision or not target
                    or target["dataset"] != dataset or target["target_date"] != day
                    or target["metadata"].get("desired_revision") != revision
                    or target["metadata"].get("desired_scope", scope_version) != scope_version
                    or target["metadata"].get("desired_rule", rule_version) != rule_version
                    or target["metadata"].get("lease_token") != token or not target["lease_valid"]):
                raise IntegrityRevisionChanged("partition changed or validation lease expired")
            complete = result.state in {"VALID", "NOT_APPLICABLE"}
            counts = result.counts[0] if result.counts else None
            physical_rows = result.checks.get("physical_rows", result.checks.get("row_count",
                result.checks.get("physical_event_rows", partition["row_count"])))
            metadata = {**dict(partition["metadata"] or {}), "validation": result.as_dict()}
            cur.execute("""UPDATE market.dataset_date_refresh_audit SET
                validated_revision=%s,validation_status=%s,validation_id=%s,scope_version=%s,rule_version=%s,
                status=%s,quality_status=%s,row_count=%s,expected_rows=%s,metadata=%s,
                refreshed_at=CASE WHEN validation_id IS DISTINCT FROM %s THEN clock_timestamp() ELSE refreshed_at END
                WHERE dataset=%s AND trade_date=%s AND data_source='canonical_db' RETURNING *""",
                (revision, result.state.value, validation_id, scope_version, rule_version,
                 "success" if complete else "failed", "ok" if complete else "unproven",
                 physical_rows,
                 counts.expected if counts else None, Json(metadata), validation_id, dataset, day))
            readback = dict(cur.fetchone())
            retry_at = None if complete else datetime.now(timezone.utc) + timedelta(minutes=30)
            attempt_id = "dsa_"+uuid4().hex
            attempt_number = int(target.get("attempt_count") or 0)+1
            attempt_status = "reconciled" if complete else "failed" if result.state.value == "ERROR" else "retry"
            cur.execute("""INSERT INTO market.data_sync_attempts
                (attempt_id,target_id,attempt_no,status,trigger_source,started_at,finished_at,
                 rows_observed,error_message,retry_after,context_json)
                VALUES (%s,%s,%s,%s,'daily_integrity',%s,clock_timestamp(),%s,%s,%s,%s)""",
                (attempt_id,target_id,attempt_number,attempt_status,
                 target["metadata"].get("validation_started_at",target["updated_at"]),physical_rows,result.reason if not complete else None,
                 retry_at,Json({"operation":"VALIDATE_ONLY","data_revision":revision,"validation_id":validation_id,
                                "scope_version":scope_version,"rule_version":rule_version})))
            cur.execute("""UPDATE market.data_sync_targets SET target_status=%s,next_retry_at=%s,
                metadata=metadata-'lease_token'-'lease_until',updated_at=clock_timestamp(),
                attempt_count=%s,last_attempt_id=%s,last_attempt_status=%s,
                reconciled_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END WHERE target_id=%s""",
                ("reconciled" if complete else "retry", retry_at,attempt_number,attempt_id,attempt_status, complete, target_id))
            return readback

    def append_day(self, conn, day: date, summary: Mapping[str, Any]) -> dict[str, Any]:
        """Small idempotent projection; actual partition revisions are rechecked on GET."""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"integrity:{day}",))
            cur.execute("""SELECT * FROM market.data_daily_integrity_revisions WHERE fact_date=%s
                AND contract_version=%s ORDER BY revision DESC LIMIT 1""", (day, CONTRACT_VERSION))
            previous = cur.fetchone()
            content = dict(summary)
            content_hash = digest(content)
            if previous and previous["content_sha256"] == content_hash:
                return dict(previous)
            revision = int(previous["revision"]) + 1 if previous else 1
            integrity_id = "ddi_" + digest([str(day), CONTRACT_VERSION, revision, content_hash])
            cur.execute("""INSERT INTO market.data_daily_integrity_revisions
                (fact_date,contract_version,revision,daily_integrity_id,as_of,state,all_sync_complete,
                 completion_kind,observed_on_time,content_sha256,summary)
                VALUES (%s,%s,%s,%s,clock_timestamp(),%s,%s,%s,%s,%s,%s) RETURNING *""",
                (day, CONTRACT_VERSION, revision, integrity_id, content["state"], content["all_sync_complete"],
                 content.get("completion_kind"), content.get("observed_on_time"), content_hash, Json(content)))
            return dict(cur.fetchone())
