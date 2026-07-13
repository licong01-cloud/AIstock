"""Immutable PostgreSQL repositories for Phase 0A execution TCA evidence."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from typing import Any, Mapping, Sequence

import psycopg2.extras

from backend.db.pg_pool import get_conn

from .tca_models import (
    ExecutionParentBenchmark,
    ExecutionPlanningSubject,
    ExecutionTcaTradeConflict,
    ExecutionTcaTradeObservation,
    ImmutableTcaRow,
    TcaInsertOutcome,
    TcaMaterializationBundle,
    TcaMaterializationOutcome,
    TcaTradeObservationOutcome,
    canonical_json_sha256,
    canonical_json_value,
    content_id,
)


_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_JSON_COLUMNS = frozenset(
    {
        "evidence",
        "eligibility_evidence",
        "raw_evidence",
        "normalized_payload",
        "raw_payload",
        "selection_predicates",
        "db_snapshot_identity",
        "source_watermarks",
        "source_row_counts",
        "source_content_hashes",
        "coverage",
        "orphan_counts",
        "duplicate_counts",
        "conflict_counts",
        "invalid_counts",
        "invariant_results",
        "numeric_tolerances",
        "failure_context",
        "fee_breakdown",
        "markout_partial_metrics",
        "markout_coverage",
        "metric_validity",
        "join_coverage",
        "benchmark_coverage",
        "mark_coverage",
        "fee_coverage",
        "finality_evidence",
    }
)


@dataclass(frozen=True, slots=True)
class ExecutionTcaRebuildScope:
    """Explicit SIM-only source snapshot scope."""

    binding_ids: tuple[str, ...]
    trade_date_from: date
    trade_date_to: date
    account_ids: tuple[str, ...] = ()
    parent_intent_ids: tuple[str, ...] = ()
    environment: str = "SIM"

    def __post_init__(self) -> None:
        if self.environment != "SIM":
            raise ValueError("reason_code=ADAPTIVE_IS_TCA_LIVE_SCOPE_DENIED, stage=tca_source_scope")
        if not self.binding_ids:
            raise ValueError("reason_code=ADAPTIVE_IS_TCA_BINDING_SCOPE_MISSING, stage=tca_source_scope")
        if self.trade_date_from > self.trade_date_to:
            raise ValueError("trade_date_from must not exceed trade_date_to")


@dataclass(frozen=True, slots=True)
class ExecutionTcaSourceSnapshot:
    planning_subjects: tuple[Mapping[str, Any], ...]
    parents: tuple[Mapping[str, Any], ...]
    runtime_events: tuple[Mapping[str, Any], ...]
    child_orders: tuple[Mapping[str, Any], ...]
    orders: tuple[Mapping[str, Any], ...]
    order_status_events: tuple[Mapping[str, Any], ...]
    trades: tuple[Mapping[str, Any], ...]
    trade_observations: tuple[Mapping[str, Any], ...]
    trade_conflicts: tuple[Mapping[str, Any], ...]
    reconciliations: tuple[Mapping[str, Any], ...]
    reconciliation_issues: tuple[Mapping[str, Any], ...]
    unattributed_orders: tuple[Mapping[str, Any], ...]
    unattributed_trades: tuple[Mapping[str, Any], ...]


class ExecutionTcaEvidenceRepository:
    """Insert/get/list boundary for immutable TCA evidence tables."""

    def __init__(self, conn_factory: Any = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def insert_planning_subjects(
        self,
        *,
        cursor: Any,
        subjects: tuple[ExecutionPlanningSubject, ...],
    ) -> tuple[TcaInsertOutcome, ...]:
        return tuple(self.insert_immutable(cursor=cursor, row=subject) for subject in subjects)

    def insert_parent_benchmark(
        self,
        *,
        cursor: Any,
        benchmark: ExecutionParentBenchmark,
    ) -> TcaInsertOutcome:
        return self.insert_immutable(cursor=cursor, row=benchmark)

    def insert_immutable(self, *, cursor: Any, row: ImmutableTcaRow) -> TcaInsertOutcome:
        table = _checked_identifier(row.table_name)
        values = dict(row.values)
        columns = tuple(values)
        if not columns:
            raise ValueError(f"{table} cannot insert an empty row")
        for column in columns:
            _checked_identifier(column)
        identity_columns = tuple(_checked_identifier(column) for column in row.identity_fields)
        placeholders = ", ".join(["%s"] * len(columns))
        cursor.execute(
            f"""
            INSERT INTO qmt_strategy.{table} ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT ({', '.join(identity_columns)}) DO NOTHING
            RETURNING {', '.join(identity_columns)}
            """,
            tuple(_db_value(column, values[column]) for column in columns),
        )
        if cursor.fetchone() is not None:
            return TcaInsertOutcome.INSERTED
        where = " AND ".join(f"{column} = %s" for column in identity_columns)
        cursor.execute(
            f"SELECT * FROM qmt_strategy.{table} WHERE {where}",
            tuple(values[column] for column in identity_columns),
        )
        existing = cursor.fetchone()
        if existing is None:
            return TcaInsertOutcome.SOURCE_MISSING
        existing_mapping = _row_mapping(cursor, existing)
        existing_hash = (
            str(existing_mapping.get(row.evidence_hash_field) or "")
            if row.evidence_hash_field
            else canonical_json_sha256(existing_mapping)
        )
        return TcaInsertOutcome.IDEMPOTENT if existing_hash == row.evidence_sha256 else TcaInsertOutcome.CONFLICT

    def get_parent_benchmark(
        self,
        *,
        parent_intent_id: str,
        parent_revision: int = 1,
    ) -> ExecutionParentBenchmark | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM qmt_strategy.execution_parent_benchmark
                    WHERE parent_intent_id = %s AND parent_revision = %s
                    """,
                    (parent_intent_id, parent_revision),
                )
                row = cursor.fetchone()
        return ExecutionParentBenchmark(dict(row)) if row else None

    def materialize_receipt(
        self,
        *,
        cursor: Any,
        bundle: TcaMaterializationBundle,
    ) -> TcaMaterializationOutcome:
        """Write one receipt and every membership in the caller transaction."""

        rows: tuple[ImmutableTcaRow, ...] = (
            *bundle.planning_subjects,
            *bundle.parent_benchmarks,
            *bundle.marks,
            *bundle.results,
            bundle.receipt,
            *bundle.receipt_subjects,
            *bundle.receipt_results,
            *bundle.result_marks,
            *bundle.result_trade_observations,
        )
        inserted = 0
        idempotent = 0
        conflicts: list[str] = []
        receipt_outcome = TcaInsertOutcome.SOURCE_MISSING
        for row in rows:
            outcome = self.insert_immutable(cursor=cursor, row=row)
            if row is bundle.receipt:
                receipt_outcome = outcome
            if outcome == TcaInsertOutcome.INSERTED:
                inserted += 1
            elif outcome == TcaInsertOutcome.IDEMPOTENT:
                idempotent += 1
            else:
                conflicts.append(f"{row.table_name}:{row.identity}:{outcome.value}")
        if conflicts:
            raise RuntimeError(
                "reason_code=ADAPTIVE_IS_TCA_MATERIALIZATION_CONFLICT, "
                f"stage=tca_repository_materialize, conflicts={conflicts}"
            )
        return TcaMaterializationOutcome(
            receipt=receipt_outcome,
            inserted_rows=inserted,
            idempotent_rows=idempotent,
        )

    def acquire_scope_lock(self, *, cursor: Any, receipt_scope_hash: str) -> int:
        """Acquire the design-mandated transaction-scoped deterministic lock."""

        digest = bytes.fromhex(receipt_scope_hash) if re.fullmatch(r"[0-9a-f]{64}", receipt_scope_hash) else sha256(receipt_scope_hash.encode("utf-8")).digest()
        unsigned = int.from_bytes(digest[:8], byteorder="big", signed=False)
        lock_key = unsigned if unsigned < (1 << 63) else unsigned - (1 << 64)
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))
        return lock_key

    def receipt_head(self, *, cursor: Any, receipt_scope_hash: str) -> Mapping[str, Any] | None:
        cursor.execute(
            """
            SELECT * FROM qmt_strategy.execution_tca_rebuild_receipt
            WHERE receipt_scope_hash = %s AND receipt_status = 'COMPLETED'
            ORDER BY receipt_generation DESC LIMIT 1
            """,
            (receipt_scope_hash,),
        )
        row = cursor.fetchone()
        return _row_mapping(cursor, row) if row is not None else None

    def receipt_latest(self, *, cursor: Any, receipt_scope_hash: str) -> Mapping[str, Any] | None:
        cursor.execute(
            """
            SELECT * FROM qmt_strategy.execution_tca_rebuild_receipt
            WHERE receipt_scope_hash = %s ORDER BY receipt_generation DESC LIMIT 1
            """,
            (receipt_scope_hash,),
        )
        row = cursor.fetchone()
        return _row_mapping(cursor, row) if row is not None else None

    def result_head(self, *, cursor: Any, result_series_key: str) -> Mapping[str, Any] | None:
        cursor.execute(
            """
            SELECT * FROM qmt_strategy.execution_parent_tca
            WHERE result_series_key = %s ORDER BY result_generation DESC LIMIT 1
            """,
            (result_series_key,),
        )
        row = cursor.fetchone()
        return _row_mapping(cursor, row) if row is not None else None

    def mark_head(self, *, cursor: Any, mark_series_key: str) -> Mapping[str, Any] | None:
        cursor.execute(
            """
            SELECT * FROM qmt_strategy.execution_tca_mark
            WHERE mark_series_key = %s ORDER BY mark_revision DESC LIMIT 1
            """,
            (mark_series_key,),
        )
        row = cursor.fetchone()
        return _row_mapping(cursor, row) if row is not None else None

    def list_parent_joined(
        self,
        *,
        cursor: Any,
        binding_ids: Sequence[str],
        trade_date_from: date,
        trade_date_to: date,
    ) -> tuple[Mapping[str, Any], ...]:
        cursor.execute(
            """
            SELECT
                b.*,
                oi.submit_status AS order_intent_submit_status,
                rt.runtime_id AS joined_runtime_id,
                ai.algo_instance_id,
                co.child_order_id,
                ol.qmt_order_id,
                tl.trade_id,
                tl.trade_time,
                tl.canonical_trade_fact_sha256
            FROM qmt_strategy.execution_parent_benchmark b
            LEFT JOIN qmt_strategy.order_intent oi ON oi.intent_id = b.parent_intent_id
            LEFT JOIN qmt_strategy.execution_runtime rt ON rt.runtime_id = b.runtime_id
            LEFT JOIN qmt_strategy.execution_algo_instance ai ON ai.parent_intent_id = b.parent_intent_id
            LEFT JOIN qmt_strategy.execution_child_order co ON co.parent_intent_id = b.parent_intent_id
            LEFT JOIN qmt_strategy.order_ledger ol ON ol.intent_id = b.parent_intent_id
            LEFT JOIN qmt_strategy.trade_ledger tl ON tl.intent_id = b.parent_intent_id
            WHERE b.binding_id = ANY(%s)
              AND b.trade_date BETWEEN %s AND %s
            ORDER BY b.trade_date, b.parent_intent_id, ai.algo_instance_id, co.child_order_id, tl.trade_id
            """,
            (list(binding_ids), trade_date_from, trade_date_to),
        )
        return _rows(cursor)


class ExecutionTradeObservationRepository:
    """Conflict-aware trade observation writer used inside ledger transactions."""

    def __init__(self, evidence_repository: ExecutionTcaEvidenceRepository | None = None) -> None:
        self._evidence = evidence_repository or ExecutionTcaEvidenceRepository()

    def record_observation(
        self,
        *,
        cursor: Any,
        observation: ExecutionTcaTradeObservation,
    ) -> TcaTradeObservationOutcome:
        values = observation.values
        trade_key = (values["account_id"], values["trade_date"], values["trade_id"])
        cursor.execute(
            """
            SELECT * FROM qmt_strategy.trade_ledger
            WHERE account_id = %s AND trade_date = %s AND trade_id = %s
            FOR UPDATE
            """,
            trade_key,
        )
        ledger_row = cursor.fetchone()
        if ledger_row is None:
            raise RuntimeError(
                "reason_code=ADAPTIVE_IS_TCA_TRADE_LEDGER_SOURCE_MISSING, "
                f"stage=tca_trade_observation, trade_key={trade_key}"
            )
        ledger = _row_mapping(cursor, ledger_row)
        insert_outcome = self._evidence.insert_immutable(cursor=cursor, row=observation)
        if insert_outcome == TcaInsertOutcome.IDEMPOTENT:
            return TcaTradeObservationOutcome.IDEMPOTENT
        if insert_outcome != TcaInsertOutcome.INSERTED:
            raise RuntimeError(
                "reason_code=ADAPTIVE_IS_TCA_OBSERVATION_IDENTITY_CONFLICT, "
                f"stage=tca_trade_observation, outcome={insert_outcome.value}, trade_key={trade_key}"
            )

        existing_canonical = str(ledger.get("canonical_trade_fact_sha256") or "")
        incoming_canonical = str(values["canonical_trade_fact_sha256"])
        if existing_canonical and existing_canonical != incoming_canonical:
            self._append_open_conflict(
                cursor=cursor,
                ledger=ledger,
                incoming=observation,
                conflict_type="CORE_FACT",
            )
            return TcaTradeObservationOutcome.CANONICAL_CONFLICT

        cursor.execute(
            """
            SELECT * FROM qmt_strategy.execution_tca_trade_observation
            WHERE account_id = %s AND trade_date = %s AND trade_id = %s
              AND trade_observation_id <> %s
            ORDER BY observed_at, trade_observation_id
            LIMIT 1
            """,
            (*trade_key, values["trade_observation_id"]),
        )
        existing_observation_row = cursor.fetchone()
        if existing_observation_row is not None:
            existing_observation = _row_mapping(cursor, existing_observation_row)
            if existing_observation.get("canonical_trade_fact_sha256") != incoming_canonical:
                self._append_open_conflict(
                    cursor=cursor,
                    ledger=ledger,
                    incoming=observation,
                    conflict_type="CORE_FACT",
                    existing_observation=existing_observation,
                )
                return TcaTradeObservationOutcome.CANONICAL_CONFLICT

        cursor.execute(
            """
            SELECT * FROM qmt_strategy.execution_tca_trade_observation
            WHERE account_id = %s AND trade_date = %s AND trade_id = %s
              AND trade_observation_id <> %s AND broker_trade_time IS NOT NULL
            ORDER BY observed_at, trade_observation_id
            LIMIT 1
            """,
            (*trade_key, values["trade_observation_id"]),
        )
        existing_time_row = cursor.fetchone()
        if existing_time_row is not None:
            existing_time = _row_mapping(cursor, existing_time_row)
            if (
                values.get("broker_trade_time") is not None
                and existing_time.get("broker_trade_time") != values.get("broker_trade_time")
            ):
                self._append_open_conflict(
                    cursor=cursor,
                    ledger=ledger,
                    incoming=observation,
                    conflict_type="AUTHORITATIVE_TIME",
                    existing_observation=existing_time,
                )
                return TcaTradeObservationOutcome.TRADE_TIME_CONFLICT
        return TcaTradeObservationOutcome.INSERTED

    def list_open_conflict_heads(
        self,
        *,
        cursor: Any,
        account_ids: Sequence[str],
        trade_date_from: date,
        trade_date_to: date,
    ) -> tuple[ExecutionTcaTradeConflict, ...]:
        cursor.execute(
            """
            SELECT c.*
            FROM qmt_strategy.execution_tca_trade_conflict c
            LEFT JOIN qmt_strategy.execution_tca_trade_conflict successor
              ON successor.supersedes_conflict_fact_id = c.trade_conflict_fact_id
            WHERE c.account_id = ANY(%s)
              AND c.trade_date BETWEEN %s AND %s
              AND c.conflict_status = 'OPEN'
              AND successor.trade_conflict_fact_id IS NULL
            ORDER BY c.account_id, c.trade_date, c.trade_id, c.conflict_series_key
            """,
            (list(account_ids), trade_date_from, trade_date_to),
        )
        return tuple(ExecutionTcaTradeConflict(dict(row)) for row in _rows(cursor))

    def _append_open_conflict(
        self,
        *,
        cursor: Any,
        ledger: Mapping[str, Any],
        incoming: ExecutionTcaTradeObservation,
        conflict_type: str,
        existing_observation: Mapping[str, Any] | None = None,
    ) -> None:
        incoming_values = incoming.values
        existing_canonical = str(
            (existing_observation or {}).get("canonical_trade_fact_sha256")
            or ledger.get("canonical_trade_fact_sha256")
            or canonical_json_sha256({key: ledger.get(key) for key in ("account_id", "trade_date", "trade_id", "qmt_order_id", "symbol", "side", "price", "quantity", "amount")})
        )
        existing_timing = str(
            (existing_observation or {}).get("timing_observation_sha256")
            or canonical_json_sha256({"trade_time": ledger.get("trade_time"), "source": "LEGACY_LEDGER_BASELINE"})
        )
        hashes = sorted(
            {
                existing_canonical,
                str(incoming_values["canonical_trade_fact_sha256"]),
                existing_timing,
                str(incoming_values["timing_observation_sha256"]),
            }
        )
        series_key = canonical_json_sha256(
            {
                "trade_key": [ledger["account_id"], ledger["trade_date"], ledger["trade_id"]],
                "conflict_type": conflict_type,
                "hashes": hashes,
            }
        )
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (series_key,))
        cursor.execute(
            """
            SELECT trade_conflict_fact_id
            FROM qmt_strategy.execution_tca_trade_conflict
            WHERE conflict_series_key = %s AND conflict_status = 'OPEN'
            ORDER BY conflict_generation DESC
            LIMIT 1
            """,
            (series_key,),
        )
        if cursor.fetchone() is not None:
            return
        generation = 1
        legacy = existing_observation is None
        fact_payload = {
            "series_key": series_key,
            "generation": generation,
            "conflict_type": conflict_type,
            "incoming_observation_id": incoming_values["trade_observation_id"],
            "hashes": hashes,
        }
        fact_hash = canonical_json_sha256(fact_payload)
        conflict = ExecutionTcaTradeConflict(
            {
                "trade_conflict_fact_id": content_id("tcacf_", series_key, generation, fact_hash),
                "conflict_series_key": series_key,
                "conflict_generation": generation,
                "supersedes_conflict_fact_id": None,
                "account_id": ledger["account_id"],
                "trade_date": ledger["trade_date"],
                "trade_id": ledger["trade_id"],
                "conflict_type": conflict_type,
                "conflict_status": "OPEN",
                "existing_observation_id": None if legacy else existing_observation["trade_observation_id"],
                "incoming_observation_id": incoming_values["trade_observation_id"],
                "existing_ingest_source": "LEGACY_LEDGER_BASELINE" if legacy else existing_observation["ingest_source"],
                "incoming_ingest_source": incoming_values["ingest_source"],
                "existing_canonical_sha256": existing_canonical,
                "incoming_canonical_sha256": incoming_values["canonical_trade_fact_sha256"],
                "existing_timing_sha256": existing_timing,
                "incoming_timing_sha256": incoming_values["timing_observation_sha256"],
                "existing_ledger_evidence_sha256": canonical_json_sha256(ledger) if legacy else None,
                "resolution_authority": None,
                "resolution_reason": None,
                "resolution_evidence_sha256": None,
                "detected_at": incoming_values["observed_at"],
                "resolved_at": None,
                "fact_sha256": fact_hash,
            }
        )
        outcome = self._evidence.insert_immutable(cursor=cursor, row=conflict)
        if outcome not in {TcaInsertOutcome.INSERTED, TcaInsertOutcome.IDEMPOTENT}:
            raise RuntimeError(
                "reason_code=ADAPTIVE_IS_TCA_CONFLICT_FACT_WRITE_FAILED, "
                f"stage=tca_trade_conflict, outcome={outcome.value}, series_key={series_key}"
            )


class ExecutionTcaSourceRepository:
    """All source reads share the caller's repeatable-read cursor."""

    def read_scope(self, *, cursor: Any, scope: ExecutionTcaRebuildScope) -> ExecutionTcaSourceSnapshot:
        binding_ids = list(scope.binding_ids)
        account_scope = list(scope.account_ids)
        parent_scope = list(scope.parent_intent_ids)
        dates = (scope.trade_date_from, scope.trade_date_to)
        cursor.execute(
            """
            SELECT * FROM qmt_strategy.execution_planning_subject
            WHERE binding_id = ANY(%s) AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date, execution_plan_id, trading_rule_decision_id
            """,
            (binding_ids, *dates),
        )
        planning_subjects = _rows(cursor)
        cursor.execute(
            """
            SELECT b.*, r.status AS run_status, p.plan_payload_json
            FROM qmt_strategy.execution_parent_benchmark b
            JOIN paper_v2.simulation_daily_run r ON r.run_id = b.run_id
            JOIN paper_v2.execution_plan p ON p.plan_id = b.execution_plan_id
            WHERE b.binding_id = ANY(%s) AND b.trade_date BETWEEN %s AND %s
              AND (cardinality(%s::text[]) = 0 OR b.account_id = ANY(%s))
              AND (cardinality(%s::text[]) = 0 OR b.parent_intent_id = ANY(%s))
            ORDER BY b.trade_date, b.parent_intent_id, b.parent_revision
            """,
            (binding_ids, *dates, account_scope, account_scope, parent_scope, parent_scope),
        )
        parents = _rows(cursor)
        parent_ids = [str(row["parent_intent_id"]) for row in parents]
        if not parent_ids:
            empty: tuple[Mapping[str, Any], ...] = ()
            return ExecutionTcaSourceSnapshot(planning_subjects, parents, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty)
        runtime_ids = sorted({str(row["runtime_id"]) for row in parents if row.get("runtime_id")})
        if runtime_ids:
            cursor.execute(
                """
                SELECT * FROM qmt_strategy.execution_runtime_event
                WHERE runtime_id = ANY(%s) AND event_type = 'TICK'
                ORDER BY runtime_id, sequence, event_id
                """,
                (runtime_ids,),
            )
            runtime_events = _rows(cursor)
        else:
            runtime_events = ()
        cursor.execute(
            """
            SELECT * FROM qmt_strategy.execution_child_order
            WHERE parent_intent_id = ANY(%s)
            ORDER BY parent_intent_id, submitted_at, child_order_id
            """,
            (parent_ids,),
        )
        child_orders = _rows(cursor)
        cursor.execute(
            "SELECT * FROM qmt_strategy.order_ledger WHERE intent_id = ANY(%s) ORDER BY account_id, qmt_order_id",
            (parent_ids,),
        )
        orders = _rows(cursor)
        cursor.execute(
            "SELECT * FROM qmt_strategy.order_status_event WHERE intent_id = ANY(%s) ORDER BY event_time, event_id",
            (parent_ids,),
        )
        status_events = _rows(cursor)
        cursor.execute(
            "SELECT * FROM qmt_strategy.trade_ledger WHERE intent_id = ANY(%s) ORDER BY account_id, trade_date, trade_id",
            (parent_ids,),
        )
        trades = _rows(cursor)
        trade_keys = {(row["account_id"], row["trade_date"], row["trade_id"]) for row in trades}
        cursor.execute(
            """
            SELECT o.* FROM qmt_strategy.execution_tca_trade_observation o
            JOIN qmt_strategy.trade_ledger t
              ON t.account_id=o.account_id AND t.trade_date=o.trade_date AND t.trade_id=o.trade_id
            WHERE t.intent_id = ANY(%s)
            ORDER BY o.account_id, o.trade_date, o.trade_id, o.observed_at, o.trade_observation_id
            """,
            (parent_ids,),
        )
        observations = _rows(cursor)
        cursor.execute(
            """
            SELECT c.* FROM qmt_strategy.execution_tca_trade_conflict c
            JOIN qmt_strategy.trade_ledger t
              ON t.account_id=c.account_id AND t.trade_date=c.trade_date AND t.trade_id=c.trade_id
            WHERE t.intent_id = ANY(%s)
            ORDER BY c.account_id, c.trade_date, c.trade_id, c.conflict_series_key, c.conflict_generation
            """,
            (parent_ids,),
        )
        conflicts = _rows(cursor)
        account_ids = sorted(
            {str(row["account_id"]) for row in parents}
            | {str(row["account_id"]) for row in trades}
            | set(scope.account_ids)
        )
        if account_ids:
            cursor.execute(
                """
                SELECT * FROM qmt_strategy.reconciliation_run
                WHERE account_id = ANY(%s) AND trade_date BETWEEN %s AND %s
                ORDER BY account_id, trade_date, started_at, run_id
                """,
                (account_ids, *dates),
            )
            reconciliations = _rows(cursor)
            run_ids = [str(row["run_id"]) for row in reconciliations]
        else:
            reconciliations = ()
            run_ids = []
        if run_ids:
            cursor.execute(
                "SELECT * FROM qmt_strategy.reconciliation_issue WHERE run_id = ANY(%s) ORDER BY run_id, issue_id",
                (run_ids,),
            )
            reconciliation_issues = _rows(cursor)
        else:
            reconciliation_issues = ()
        if account_ids:
            cursor.execute(
                """
                SELECT * FROM qmt_strategy.unattributed_order
                WHERE account_id = ANY(%s) AND trade_date BETWEEN %s AND %s
                ORDER BY account_id, trade_date, qmt_order_id
                """,
                (account_ids, *dates),
            )
            unattributed_orders = _rows(cursor)
            cursor.execute(
                """
                SELECT * FROM qmt_strategy.unattributed_trade
                WHERE account_id = ANY(%s) AND trade_date BETWEEN %s AND %s
                ORDER BY account_id, trade_date, trade_id
                """,
                (account_ids, *dates),
            )
            unattributed_trades = _rows(cursor)
        else:
            unattributed_orders = ()
            unattributed_trades = ()
        _ = trade_keys  # retained for debugger visibility and future canonical content receipts
        return ExecutionTcaSourceSnapshot(
            planning_subjects,
            parents,
            runtime_events,
            child_orders,
            orders,
            status_events,
            trades,
            observations,
            conflicts,
            reconciliations,
            reconciliation_issues,
            unattributed_orders,
            unattributed_trades,
        )


def _checked_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return value


def _db_value(column: str, value: Any) -> Any:
    if column in _JSON_COLUMNS:
        return psycopg2.extras.Json(canonical_json_value(value))
    if isinstance(value, tuple):
        return list(value)
    return value


def _row_mapping(cursor: Any, row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    names = [description[0] for description in cursor.description]
    return dict(zip(names, row, strict=True))


def _rows(cursor: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(_row_mapping(cursor, row) for row in cursor.fetchall())
