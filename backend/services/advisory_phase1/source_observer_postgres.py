"""PostgreSQL persistence and standalone execution for the Phase 1D observer."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.policy import canonicalize
from backend.services.advisory_phase1.source_ledger_postgres import PostgresSourceAvailabilityLedger
from backend.services.advisory_phase1.source_observer import (
    AuditRowSnapshot,
    ObservationOutcome,
    ObservedDatasetSpec,
    REASON_EVENT_CONFLICT,
    REASON_AUDIT_SCHEMA_MISSING,
    REASON_CURSOR_CONFLICT,
    REASON_OBSERVER_CONFIG_INVALID,
    REASON_OBSERVER_UNEXPECTED,
    REASON_RECEIPT_CONFLICT,
    REASON_RESOURCE_LIMIT,
    REASON_SCHEMA_MISMATCH,
    SOURCE_QUERY_TEMPLATES,
    SourceObservationReceipt,
    SourceObserverConfigBundle,
    SourceObserverCursor,
    SourceObserverError,
    SourcePartitionDescriptor,
    SourceQueryTemplate,
    audit_eligibility_reasons,
    build_observation_receipt,
    canonical_source_partition_descriptor,
    decide_observation,
    resolve_query_template,
)


_logger = logging.getLogger(__name__)
ConnFactory = Callable[[], Iterator[Any]]


def _transactional_conn_factory() -> Iterator[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


@dataclass(frozen=True)
class SourceObserverScope:
    dataset_name: str
    data_source: str
    source_role: str

    @property
    def log_key(self) -> str:
        return f"{self.dataset_name}:{self.data_source}:{self.source_role}"


@dataclass
class SourceObserverRunSummary:
    """Compact worker receipt. Individual errors remain explicit and non-silent."""

    config_hash: str
    processed: int = 0
    appended: int = 0
    unchanged: int = 0
    not_eligible: int = 0
    failed: int = 0
    scope_failures: list[dict[str, Any]] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.failed == 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "config_hash": self.config_hash,
            "processed": self.processed,
            "appended": self.appended,
            "unchanged": self.unchanged,
            "not_eligible": self.not_eligible,
            "failed": self.failed,
            "scope_failures": list(self.scope_failures),
            "succeeded": self.succeeded,
        }


class PostgresSourceObserverRepository:
    """Observe registered source partitions without modifying market source tables."""

    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        ledger: PostgresSourceAvailabilityLedger | None = None,
    ) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory
        self._ledger = ledger or PostgresSourceAvailabilityLedger()

    def observe_once(
        self,
        *,
        config: SourceObserverConfigBundle,
        registry: Mapping[str, SourceQueryTemplate] = SOURCE_QUERY_TEMPLATES,
    ) -> SourceObserverRunSummary:
        config_hash = config.config_hash(registry)
        summary = SourceObserverRunSummary(config_hash=config_hash)
        started = time.monotonic()
        scope_count = sum(
            len(spec.source_roles) * len(spec.allowed_data_sources) for spec in config.dataset_specs
        )
        _logger.info(
            "advisory_source_observer_started config_hash=%s scopes=%s",
            config_hash,
            scope_count,
        )
        for spec in config.dataset_specs:
            for source_role in spec.source_roles:
                for data_source in spec.allowed_data_sources:
                    scope = SourceObserverScope(spec.dataset_name, data_source, source_role)
                    try:
                        self._observe_scope(config=config, registry=registry, spec=spec, scope=scope, summary=summary)
                    except Exception as exc:  # Each scope is isolated; the CLI converts this into a non-zero result.
                        reason_code = str(getattr(exc, "reason_code", REASON_OBSERVER_UNEXPECTED))
                        context = dict(getattr(exc, "context", {}))
                        summary.failed += 1
                        failure = {
                            "scope": scope.log_key,
                            "reason_code": reason_code,
                            "error_type": type(exc).__name__,
                            "context": context,
                        }
                        summary.scope_failures.append(failure)
                        _logger.exception(
                            "advisory_source_observer_scope_failed scope=%s reason_code=%s error_type=%s context=%s",
                            scope.log_key,
                            reason_code,
                            type(exc).__name__,
                            context,
                        )
        _logger.info(
            "advisory_source_observer_finished duration_ms=%s summary=%s",
            int((time.monotonic() - started) * 1000),
            summary.as_dict(),
        )
        return summary

    def _observe_scope(
        self,
        *,
        config: SourceObserverConfigBundle,
        registry: Mapping[str, SourceQueryTemplate],
        spec: ObservedDatasetSpec,
        scope: SourceObserverScope,
        summary: SourceObserverRunSummary,
    ) -> None:
        template = resolve_query_template(spec, registry)
        for _ in range(config.audit_scan_batch_size):
            outcome = None
            for retry_no in range(config.serialization_retry_limit + 1):
                try:
                    outcome = self._observe_next_input(
                        config=config,
                        registry=registry,
                        spec=spec,
                        template=template,
                        scope=scope,
                    )
                    break
                except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as exc:
                    if retry_no >= config.serialization_retry_limit:
                        raise SourceObserverError(
                            REASON_CURSOR_CONFLICT,
                            "observer transaction serialization retry limit exhausted",
                            context={
                                "scope": scope.log_key,
                                "transaction_stage": "cursor_serialization",
                                "retry_count": retry_no,
                            },
                        ) from exc
                    _logger.warning(
                        "advisory_source_observer_serialization_retry scope=%s retry_no=%s error_type=%s",
                        scope.log_key,
                        retry_no + 1,
                        type(exc).__name__,
                    )
            if outcome is None:
                return
            summary.processed += 1
            if outcome is ObservationOutcome.EVENT_APPENDED:
                summary.appended += 1
            elif outcome is ObservationOutcome.UNCHANGED:
                summary.unchanged += 1
            else:
                summary.not_eligible += 1

    def _observe_next_input(
        self,
        *,
        config: SourceObserverConfigBundle,
        registry: Mapping[str, SourceQueryTemplate],
        spec: ObservedDatasetSpec,
        template: SourceQueryTemplate,
        scope: SourceObserverScope,
    ) -> ObservationOutcome | None:
        config_hash = config.config_hash(registry)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                try:
                    self._begin_observer_transaction(cur=cur, config=config)
                    cursor = self._get_or_create_cursor(
                        cur=cur,
                        config_hash=config_hash,
                        config=config,
                        scope=scope,
                    )
                    audit = self._next_unprocessed_audit(
                        cur=cur,
                        cursor=cursor,
                        scope=scope,
                        batch_size=config.audit_scan_batch_size,
                    )
                except (psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn) as exc:
                    raise SourceObserverError(
                        REASON_AUDIT_SCHEMA_MISSING,
                        "ingestion audit schema is unavailable",
                        context={"scope": scope.log_key, "transaction_stage": "audit_discovery"},
                    ) from exc
                except SourceObserverError as exc:
                    raise _enrich_observer_error(exc, scope=scope, stage="audit_discovery") from exc
                if audit is None:
                    return None
                try:
                    return self._process_audit_input(
                        conn=conn,
                        cur=cur,
                        config=config,
                        registry=registry,
                        config_hash=config_hash,
                        spec=spec,
                        template=template,
                        scope=scope,
                        cursor=cursor,
                        audit=audit,
                    )
                except SourceObserverError as exc:
                    raise _enrich_observer_error(
                        exc,
                        scope=scope,
                        stage=_transaction_stage(exc.reason_code),
                        audit=audit,
                    ) from exc
                except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected):
                    raise
                except Exception as exc:
                    raise SourceObserverError(
                        REASON_OBSERVER_UNEXPECTED,
                        "observer input transaction failed",
                        context={
                            "scope": scope.log_key,
                            "partition": audit.trade_date.isoformat(),
                            "audit_row_hash": audit.audit_row_hash,
                            "transaction_stage": "input_transaction",
                            "error_type": type(exc).__name__,
                        },
                    ) from exc

    def _process_audit_input(
        self,
        *,
        conn: Any,
        cur: Any,
        config: SourceObserverConfigBundle,
        registry: Mapping[str, SourceQueryTemplate],
        config_hash: str,
        spec: ObservedDatasetSpec,
        template: SourceQueryTemplate,
        scope: SourceObserverScope,
        cursor: SourceObserverCursor,
        audit: AuditRowSnapshot,
    ) -> ObservationOutcome:
        existing = self._find_receipt(
            cur=cur,
            observer_config_hash=config_hash,
            audit_row_hash=audit.audit_row_hash,
            source_role=scope.source_role,
        )
        if existing is not None:
            self._advance_cursor_if_needed(cur=cur, cursor=cursor, audit=audit)
            return existing.outcome
        reasons = audit_eligibility_reasons(spec, audit)
        if reasons:
            decision = decide_observation(
                config=config,
                spec=spec,
                template=template,
                audit=audit,
                source_role=scope.source_role,
                descriptor=None,
                terminal_event=None,
            )
            receipt = build_observation_receipt(
                config=config,
                registry=registry,
                audit=audit,
                source_role=scope.source_role,
                decision=decision,
                observed_at=self._database_now(cur),
                event=None,
            )
        else:
            self._validate_source_schema(cur=cur, template=template)
            descriptor = self._describe_source_partition(
                conn=conn,
                template=template,
                audit=audit,
                config=config,
            )
            partition_key = {"trade_date": audit.trade_date.isoformat()}
            terminal = self._ledger.terminal_for_partition_in_transaction(
                conn=conn,
                dataset_name=spec.dataset_name,
                source_role=scope.source_role,
                partition_key=partition_key,
            )
            decision = decide_observation(
                config=config,
                spec=spec,
                template=template,
                audit=audit,
                source_role=scope.source_role,
                descriptor=descriptor,
                terminal_event=terminal,
            )
            event = (
                self._ledger.append_in_transaction(conn=conn, request=decision.event_request)
                if decision.outcome is ObservationOutcome.EVENT_APPENDED and decision.event_request is not None
                else terminal
            )
            receipt = build_observation_receipt(
                config=config,
                registry=registry,
                audit=audit,
                source_role=scope.source_role,
                decision=decision,
                observed_at=self._database_now(cur),
                event=event,
            )
        persisted = self._insert_receipt(cur=cur, receipt=receipt)
        self._advance_cursor_if_needed(cur=cur, cursor=cursor, audit=audit)
        return persisted.outcome

    @staticmethod
    def _begin_observer_transaction(*, cur: Any, config: SourceObserverConfigBundle) -> None:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur.execute("SELECT set_config('statement_timeout', %s, true)", (str(config.statement_timeout_ms),))
        cur.execute("SELECT set_config('lock_timeout', %s, true)", (str(config.lock_timeout_ms),))

    @staticmethod
    def _database_now(cur: Any) -> datetime:
        cur.execute("SELECT clock_timestamp() AS observed_at")
        return cur.fetchone()["observed_at"]

    @staticmethod
    def _get_or_create_cursor(
        *,
        cur: Any,
        config_hash: str,
        config: SourceObserverConfigBundle,
        scope: SourceObserverScope,
    ) -> SourceObserverCursor:
        cur.execute(
            """
            INSERT INTO app.advisory_source_observer_cursor (
                observer_config_hash, dataset_name, data_source, source_role,
                last_audit_refreshed_at, last_trade_date, last_audit_row_hash, row_version
            ) VALUES (%s, %s, %s, %s, %s, NULL, NULL, 1)
            ON CONFLICT (observer_config_hash, dataset_name, data_source, source_role) DO NOTHING
            """,
            (
                config_hash,
                scope.dataset_name,
                scope.data_source,
                scope.source_role,
                config.effective_from_observed_at,
            ),
        )
        cur.execute(
            """
            SELECT * FROM app.advisory_source_observer_cursor
            WHERE observer_config_hash = %s AND dataset_name = %s AND data_source = %s AND source_role = %s
            FOR UPDATE
            """,
            (config_hash, scope.dataset_name, scope.data_source, scope.source_role),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceObserverError(REASON_OBSERVER_CONFIG_INVALID, "observer cursor could not be created", context={"scope": scope.log_key})
        return _cursor_from_row(dict(row))

    def _next_unprocessed_audit(
        self,
        *,
        cur: Any,
        cursor: SourceObserverCursor,
        scope: SourceObserverScope,
        batch_size: int,
    ) -> AuditRowSnapshot | None:
        # Replay the full equal-timestamp boundary, then deduplicate against immutable receipts.
        cur.execute(
            """
            SELECT dataset, trade_date, data_source, job_id, status, row_count, refreshed_at, error_message,
                   metadata, data_max_at, written_rows, expected_rows, coverage_ratio, quality_status, failure_category
            FROM market.dataset_date_refresh_audit
            WHERE dataset = %s AND data_source = %s AND refreshed_at = %s
            ORDER BY trade_date, dataset, data_source
            LIMIT %s
            """,
            (scope.dataset_name, scope.data_source, cursor.last_audit_refreshed_at, batch_size),
        )
        boundary_rows = tuple(_audit_from_row(dict(row)) for row in cur.fetchall())
        if len(boundary_rows) == batch_size:
            cur.execute(
                """
                SELECT count(*) AS boundary_count
                FROM market.dataset_date_refresh_audit
                WHERE dataset = %s AND data_source = %s AND refreshed_at = %s
                """,
                (scope.dataset_name, scope.data_source, cursor.last_audit_refreshed_at),
            )
            if int(cur.fetchone()["boundary_count"]) > batch_size:
                raise SourceObserverError(
                    REASON_RESOURCE_LIMIT,
                    "equal-timestamp audit boundary exceeds configured scan batch",
                    context={"scope": scope.log_key, "batch_size": batch_size},
                )
        for audit in boundary_rows:
            if self._find_receipt(
                cur=cur,
                observer_config_hash=cursor.observer_config_hash,
                audit_row_hash=audit.audit_row_hash,
                source_role=scope.source_role,
            ) is None:
                return audit

        if cursor.last_trade_date is None:
            predicate = "refreshed_at > %s"
            params: tuple[Any, ...] = (scope.dataset_name, scope.data_source, cursor.last_audit_refreshed_at)
        else:
            predicate = "(refreshed_at, trade_date, dataset, data_source) > (%s, %s, %s, %s)"
            params = (
                scope.dataset_name,
                scope.data_source,
                cursor.last_audit_refreshed_at,
                cursor.last_trade_date,
                scope.dataset_name,
                scope.data_source,
            )
        cur.execute(
            f"""
            SELECT dataset, trade_date, data_source, job_id, status, row_count, refreshed_at, error_message,
                   metadata, data_max_at, written_rows, expected_rows, coverage_ratio, quality_status, failure_category
            FROM market.dataset_date_refresh_audit
            WHERE dataset = %s AND data_source = %s AND {predicate}
            ORDER BY refreshed_at, trade_date, dataset, data_source
            LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()
        return None if row is None else _audit_from_row(dict(row))

    @staticmethod
    def _validate_source_schema(*, cur: Any, template: SourceQueryTemplate) -> None:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (template.schema_name, template.table_name),
        )
        actual_by_name = {str(row["column_name"]): (str(row["data_type"]), str(row["is_nullable"]) == "YES") for row in cur.fetchall()}
        expected = {column.name: (column.pg_data_type, column.nullable) for column in template.columns}
        mismatches = {
            name: {"expected": expected[name], "actual": actual_by_name.get(name)}
            for name in expected
            if actual_by_name.get(name) != expected[name]
        }
        if mismatches:
            raise SourceObserverError(
                REASON_SCHEMA_MISMATCH,
                "registered source schema differs from information_schema",
                context={"template_id": template.template_id, "mismatches": mismatches},
            )

    @staticmethod
    def _describe_source_partition(
        *,
        conn: Any,
        template: SourceQueryTemplate,
        audit: AuditRowSnapshot,
        config: SourceObserverConfigBundle,
    ) -> SourcePartitionDescriptor:
        cursor_name = f"advisory_source_{uuid.uuid4().hex}"
        source_cur = conn.cursor(name=cursor_name, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            source_cur.itersize = config.source_fetch_rows
            source_cur.execute(template.sql, (audit.trade_date,))
            first_batch = source_cur.fetchmany(config.source_fetch_rows)
            actual_columns = tuple(description.name for description in source_cur.description or ())
            expected_columns = tuple(column.name for column in template.columns)
            if actual_columns != expected_columns:
                raise SourceObserverError(
                    REASON_SCHEMA_MISMATCH,
                    "source query projection differs from compiled template",
                    context={"template_id": template.template_id, "expected": expected_columns, "actual": actual_columns},
                )

            def rows() -> Iterator[Mapping[str, Any]]:
                for row in first_batch:
                    yield dict(row)
                while True:
                    batch = source_cur.fetchmany(config.source_fetch_rows)
                    if not batch:
                        return
                    for row in batch:
                        yield dict(row)

            return canonical_source_partition_descriptor(
                template=template,
                rows=rows(),
                max_rows=config.max_partition_rows,
                max_bytes=config.max_partition_bytes,
            )
        finally:
            source_cur.close()

    @staticmethod
    def _find_receipt(
        *,
        cur: Any,
        observer_config_hash: str,
        audit_row_hash: str,
        source_role: str,
    ) -> SourceObservationReceipt | None:
        cur.execute(
            """
            SELECT * FROM app.advisory_source_observation_receipt
            WHERE observer_config_hash = %s AND audit_row_hash = %s AND source_role = %s
            FOR KEY SHARE
            """,
            (observer_config_hash, audit_row_hash, source_role),
        )
        row = cur.fetchone()
        return None if row is None else _receipt_from_row(dict(row))

    @staticmethod
    def _insert_receipt(*, cur: Any, receipt: SourceObservationReceipt) -> SourceObservationReceipt:
        existing = PostgresSourceObserverRepository._find_receipt(
            cur=cur,
            observer_config_hash=receipt.observer_config_hash,
            audit_row_hash=receipt.audit_row_hash,
            source_role=receipt.source_role,
        )
        if existing is not None:
            if existing.observation_receipt_hash != receipt.observation_receipt_hash:
                raise SourceObserverError(
                    REASON_RECEIPT_CONFLICT,
                    "same observer audit identity already binds different receipt content",
                    context={"receipt_id": existing.observation_receipt_id},
                )
            return existing
        cur.execute(
            """
            INSERT INTO app.advisory_source_observation_receipt (
                observation_receipt_id, observation_receipt_hash, observer_config_id, observer_config_version,
                observer_config_hash, dataset_name, data_source, source_role, trade_date, partition_key,
                partition_key_hash, audit_refreshed_at, audit_row_hash, outcome, availability_event_id,
                availability_event_hash, observed_schema_fingerprint, observed_row_count,
                observed_partition_content_hash, reason_codes, observed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            ) RETURNING *
            """,
            _receipt_insert_params(receipt),
        )
        return _receipt_from_row(dict(cur.fetchone()))

    @staticmethod
    def _advance_cursor_if_needed(*, cur: Any, cursor: SourceObserverCursor, audit: AuditRowSnapshot) -> SourceObserverCursor:
        should_advance = audit.refreshed_at > cursor.last_audit_refreshed_at or (
            audit.refreshed_at == cursor.last_audit_refreshed_at
            and (cursor.last_trade_date is None or audit.trade_date > cursor.last_trade_date)
        )
        if not should_advance:
            return cursor
        cur.execute(
            """
            UPDATE app.advisory_source_observer_cursor
            SET last_audit_refreshed_at = %s,
                last_trade_date = %s,
                last_audit_row_hash = %s,
                row_version = row_version + 1
            WHERE observer_config_hash = %s AND dataset_name = %s AND data_source = %s AND source_role = %s
              AND row_version = %s
            RETURNING *
            """,
            (
                audit.refreshed_at,
                audit.trade_date,
                audit.audit_row_hash,
                cursor.observer_config_hash,
                cursor.dataset_name,
                cursor.data_source,
                cursor.source_role,
                cursor.row_version,
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceObserverError(
                REASON_CURSOR_CONFLICT,
                "observer cursor row version changed",
                context={"source_role": cursor.source_role, "row_version": cursor.row_version},
            )
        return _cursor_from_row(dict(row))


def _audit_from_row(row: dict[str, Any]) -> AuditRowSnapshot:
    return AuditRowSnapshot(
        dataset_name=str(row["dataset"]),
        trade_date=row["trade_date"],
        data_source=str(row["data_source"]),
        job_id=str(row["job_id"]) if row.get("job_id") is not None else None,
        status=str(row["status"]),
        row_count=int(row["row_count"]),
        refreshed_at=row["refreshed_at"],
        error_message=str(row["error_message"]) if row.get("error_message") is not None else None,
        metadata=dict(row.get("metadata") or {}),
        data_max_at=row.get("data_max_at"),
        written_rows=int(row["written_rows"]) if row.get("written_rows") is not None else None,
        expected_rows=int(row["expected_rows"]) if row.get("expected_rows") is not None else None,
        coverage_ratio=float(row["coverage_ratio"]) if row.get("coverage_ratio") is not None else None,
        quality_status=str(row["quality_status"]),
        failure_category=str(row["failure_category"]) if row.get("failure_category") is not None else None,
    )


def _cursor_from_row(row: dict[str, Any]) -> SourceObserverCursor:
    return SourceObserverCursor(
        observer_config_hash=str(row["observer_config_hash"]),
        dataset_name=str(row["dataset_name"]),
        data_source=str(row["data_source"]),
        source_role=str(row["source_role"]),
        last_audit_refreshed_at=row["last_audit_refreshed_at"],
        last_trade_date=row.get("last_trade_date"),
        last_audit_row_hash=str(row["last_audit_row_hash"]) if row.get("last_audit_row_hash") is not None else None,
        row_version=int(row["row_version"]),
        updated_at=row["updated_at"],
    )


def _receipt_insert_params(receipt: SourceObservationReceipt) -> tuple[Any, ...]:
    return (
        receipt.observation_receipt_id,
        receipt.observation_receipt_hash,
        receipt.observer_config_id,
        receipt.observer_config_version,
        receipt.observer_config_hash,
        receipt.dataset_name,
        receipt.data_source,
        receipt.source_role,
        receipt.trade_date,
        psycopg2.extras.Json(canonicalize(receipt.partition_key)),
        receipt.partition_key_hash,
        receipt.audit_refreshed_at,
        receipt.audit_row_hash,
        receipt.outcome.value,
        receipt.availability_event_id,
        receipt.availability_event_hash,
        receipt.observed_schema_fingerprint,
        receipt.observed_row_count,
        receipt.observed_partition_content_hash,
        psycopg2.extras.Json(list(receipt.reason_codes)),
        receipt.observed_at,
    )


def _receipt_from_row(row: dict[str, Any]) -> SourceObservationReceipt:
    receipt = SourceObservationReceipt(
        observer_config_id=str(row["observer_config_id"]),
        observer_config_version=str(row["observer_config_version"]),
        observer_config_hash=str(row["observer_config_hash"]),
        dataset_name=str(row["dataset_name"]),
        data_source=str(row["data_source"]),
        source_role=str(row["source_role"]),
        trade_date=row["trade_date"],
        partition_key=dict(row["partition_key"]),
        partition_key_hash=str(row["partition_key_hash"]),
        audit_refreshed_at=row["audit_refreshed_at"],
        audit_row_hash=str(row["audit_row_hash"]),
        outcome=ObservationOutcome(str(row["outcome"])),
        availability_event_id=str(row["availability_event_id"]) if row.get("availability_event_id") is not None else None,
        availability_event_hash=str(row["availability_event_hash"]) if row.get("availability_event_hash") is not None else None,
        observed_schema_fingerprint=(str(row["observed_schema_fingerprint"]) if row.get("observed_schema_fingerprint") is not None else None),
        observed_row_count=int(row["observed_row_count"]) if row.get("observed_row_count") is not None else None,
        observed_partition_content_hash=(str(row["observed_partition_content_hash"]) if row.get("observed_partition_content_hash") is not None else None),
        reason_codes=tuple(str(item) for item in (row.get("reason_codes") or [])),
        observed_at=row["observed_at"],
    )
    if receipt.observation_receipt_id != str(row["observation_receipt_id"]) or receipt.observation_receipt_hash != str(row["observation_receipt_hash"]):
        raise SourceObserverError(
            REASON_RECEIPT_CONFLICT,
            "persisted observation receipt does not match canonical hash",
            context={"receipt_id": str(row["observation_receipt_id"])},
        )
    return receipt


def _transaction_stage(reason_code: str) -> str:
    if reason_code in {REASON_SCHEMA_MISMATCH, REASON_RESOURCE_LIMIT}:
        return "source_read_and_hash"
    if reason_code == REASON_CURSOR_CONFLICT:
        return "cursor_advance"
    if reason_code == REASON_RECEIPT_CONFLICT:
        return "receipt_append"
    if reason_code == REASON_EVENT_CONFLICT:
        return "event_append"
    return "input_validation"


def _enrich_observer_error(
    error: SourceObserverError,
    *,
    scope: SourceObserverScope,
    stage: str,
    audit: AuditRowSnapshot | None = None,
) -> SourceObserverError:
    context = dict(error.context)
    context.setdefault("scope", scope.log_key)
    context.setdefault("transaction_stage", stage)
    if audit is not None:
        context.setdefault("partition", audit.trade_date.isoformat())
        context.setdefault("audit_row_hash", audit.audit_row_hash)
    return SourceObserverError(error.reason_code, str(error), context=context)
