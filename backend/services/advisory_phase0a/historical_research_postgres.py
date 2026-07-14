"""PostgreSQL-only implementations for manual historical Advisory research.

The module uses the historical database as its sole data boundary.  It does not
import or invoke Paper Trading, simulation, QMT, MiniQMT, broker, order, or
real-time provider code.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable, Iterator
from uuid import UUID
from zoneinfo import ZoneInfo

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.historical_research import (
    HISTORICAL_RESEARCH_DATA_SOURCE,
    HISTORICAL_RESEARCH_SCOPE,
    REASON_HISTORICAL_DATE_REQUIRED,
    REASON_PROGRAM_EVIDENCE_INVALID,
    HistoricalResearchBatch,
    HistoricalResearchBatchReceipt,
    HistoricalResearchBatchRequest,
    HistoricalResearchCandidate,
    HistoricalResearchInputUnavailable,
    HistoricalResearchProgramContext,
    HistoricalResearchProgramRun,
    HistoricalResearchRunStatus,
    HistoricalSelectionEvidence,
    _ProgramOutcome,
    _aggregate_status,
    _batch_receipt_payload,
    _conflict_error,
    _program_run_from_outcome,
)
from backend.services.advisory_phase0a.evidence_projection import (
    ProjectedHistoricalEvidenceV2,
    canonical_evidence_json_sha256,
    validate_projected_historical_evidence_v2,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


ConnFactory = Callable[[], Iterator[Any]]
_CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _transactional_conn_factory() -> Iterator[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class PostgresHistoricalResearchTradingDateResolver:
    """Validate date eligibility directly against ``market.trading_calendar``."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def require_completed_historical_trading_date(self, *, decision_trade_date: date, requested_at: datetime) -> None:
        request_day = requested_at.astimezone(_CHINA_TZ).date()
        if decision_trade_date >= request_day:
            raise _historical_date_error(
                "decision_trade_date must precede the request calendar date",
                decision_trade_date=decision_trade_date.isoformat(),
                request_date=request_day.isoformat(),
            )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT is_trading FROM market.trading_calendar WHERE cal_date = %s",
                    (decision_trade_date,),
                )
                row = cur.fetchone()
        if row is None or not bool(row[0]):
            raise _historical_date_error(
                "decision_trade_date must be a completed historical trading day",
                decision_trade_date=decision_trade_date.isoformat(),
            )


class PostgresHistoricalResearchProgramResolver:
    """Resolve dated Program binding and its one authoritative package."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def resolve(
        self,
        *,
        program_id: str,
        decision_trade_date: date,
        cursor: Any | None = None,
    ) -> HistoricalResearchProgramContext:
        if cursor is not None:
            return self._resolve_with_cursor(cursor=cursor, program_id=program_id, decision_trade_date=decision_trade_date)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._resolve_with_cursor(cursor=cur, program_id=program_id, decision_trade_date=decision_trade_date)

    @staticmethod
    def _resolve_with_cursor(*, cursor: Any, program_id: str, decision_trade_date: date) -> HistoricalResearchProgramContext:
        cursor.execute(
            """
            SELECT
                p.program_id,
                p.review_policy_sha256,
                b.binding_version_id,
                b.package_mode,
                b.package_ids,
                b.runtime_config_json,
                b.binding_payload_json
            FROM app.advisory_program AS p
            JOIN app.advisory_strategy_binding_version AS b
              ON b.program_id = p.program_id
            WHERE p.program_id = %s
              AND p.status <> 'ARCHIVED'
              AND b.activation_status <> 'DRAFT'
              AND b.effective_from_trade_date <= %s
              AND (b.effective_to_trade_date IS NULL OR %s < b.effective_to_trade_date)
            ORDER BY b.effective_from_trade_date DESC, b.created_at DESC
            LIMIT 2
            """,
            (program_id, decision_trade_date, decision_trade_date),
        )
        rows = cursor.fetchall()
        if len(rows) != 1:
            raise RuntimeConfigInvalidError(
                "historical research requires exactly one dated Program binding",
                context={
                    "reason_code": "ADVISORY_PHASE0A2D_PROGRAM_BINDING_INVALID",
                    "program_id": program_id,
                    "decision_trade_date": decision_trade_date.isoformat(),
                    "binding_count": len(rows),
                },
            )
        row = dict(rows[0])
        package_ids = list(row.get("package_ids") or [])
        if row.get("package_mode") != "single_package" or len(package_ids) != 1:
            raise RuntimeConfigInvalidError(
                "historical research accepts one single-alpha package or one native multi-alpha parent package",
                context={
                    "reason_code": "ADVISORY_PHASE0A2D_PROGRAM_BINDING_INVALID",
                    "program_id": program_id,
                    "package_mode": row.get("package_mode"),
                    "package_ids": package_ids,
                },
            )
        package_id = str(package_ids[0] or "").strip()
        if not package_id:
            raise RuntimeConfigInvalidError(
                "dated Program binding does not identify a StrategyPackage",
                context={"reason_code": "ADVISORY_PHASE0A2D_PROGRAM_BINDING_INVALID", "program_id": program_id},
            )
        cursor.execute(
            "SELECT manifest_sha256 FROM strategy_pkg.package WHERE package_id = %s",
            (package_id,),
        )
        package_row = cursor.fetchone()
        if package_row is None:
            raise RuntimeConfigInvalidError(
                "dated Program binding references a missing StrategyPackage",
                context={
                    "reason_code": "ADVISORY_PHASE0A2D_PROGRAM_BINDING_INVALID",
                    "program_id": program_id,
                    "package_id": package_id,
                },
            )
        manifest_sha256 = str(package_row["manifest_sha256"] if hasattr(package_row, "keys") else package_row[0] or "").strip()
        binding_payload = dict(row.get("binding_payload_json") or {})
        runtime_config = dict(row.get("runtime_config_json") or {})
        policy_hash = str(row.get("review_policy_sha256") or "").strip()
        if not manifest_sha256 or not policy_hash:
            raise RuntimeConfigInvalidError(
                "historical Program context is missing an immutable manifest or policy hash",
                context={"reason_code": "ADVISORY_PHASE0A2D_PROGRAM_BINDING_INVALID", "program_id": program_id},
            )
        return HistoricalResearchProgramContext(
            program_id=program_id,
            binding_version_id=str(row["binding_version_id"]),
            binding_payload_hash=canonical_json_sha256(binding_payload),
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            policy_hash=policy_hash,
            effective_runtime_config_hash=canonical_evidence_json_sha256(runtime_config),
        )


class PersistedHistoricalSelectionEvidenceAdapter:
    """Read a unique, complete research-only DSE v2 without running selection."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def load(
        self,
        *,
        context: HistoricalResearchProgramContext,
        decision_trade_date: date,
        cursor: Any | None = None,
    ) -> HistoricalSelectionEvidence:
        if cursor is not None:
            return self._load_with_cursor(cursor=cursor, context=context, decision_trade_date=decision_trade_date)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._load_with_cursor(cursor=cur, context=context, decision_trade_date=decision_trade_date)

    @staticmethod
    def _load_with_cursor(
        *,
        cursor: Any,
        context: HistoricalResearchProgramContext,
        decision_trade_date: date,
    ) -> HistoricalSelectionEvidence:
        cursor.execute(
            """
            SELECT evidence_id, artifact_hash, evidence_payload_json
            FROM selection.daily_selection_evidence
            WHERE package_id = %s
              AND manifest_sha256 = %s
              AND cutoff_date = %s
              AND data_source = %s
              AND evidence_payload_json ->> 'schema_version' = 'daily_selection_evidence_v2'
              AND evidence_payload_json #>> '{evidence_contract,research_scope}' = 'HISTORICAL_RESEARCH_ONLY'
              AND (evidence_payload_json #>> '{evidence_contract,execution_prohibited}')::boolean IS TRUE
              AND evidence_payload_json #>> '{evidence_contract,market_data_scope}' = 'DB_HISTORICAL'
            ORDER BY created_at ASC, evidence_id ASC
            """,
            (context.package_id, context.manifest_sha256, decision_trade_date, HISTORICAL_RESEARCH_DATA_SOURCE),
        )
        rows = cursor.fetchall()
        if not rows:
            raise HistoricalResearchInputUnavailable(
                "complete historical v2 DailySelectionEvidence is not available",
                context={
                    "program_id": context.program_id,
                    "package_id": context.package_id,
                    "decision_trade_date": decision_trade_date.isoformat(),
                },
            )
        parsed: list[tuple[dict[str, Any], ProjectedHistoricalEvidenceV2]] = []
        for raw in rows:
            row = dict(raw)
            payload = validate_projected_historical_evidence_v2(dict(row.get("evidence_payload_json") or {}))
            if payload is None:
                raise RuntimeConfigInvalidError(
                    "stored DailySelectionEvidence v2 is invalid",
                    context={
                        "reason_code": REASON_PROGRAM_EVIDENCE_INVALID,
                        "program_id": context.program_id,
                        "error": "advisory projection DTO validation failed",
                    },
                )
            PersistedHistoricalSelectionEvidenceAdapter._assert_context_matches(
                context=context,
                payload=payload,
                decision_trade_date=decision_trade_date,
            )
            parsed.append((row, payload))
        if len(parsed) != 1:
            raise RuntimeConfigInvalidError(
                "historical Program context resolves more than one immutable DSE",
                context={
                    "reason_code": REASON_PROGRAM_EVIDENCE_INVALID,
                    "program_id": context.program_id,
                    "decision_trade_date": decision_trade_date.isoformat(),
                    "evidence_ids": [str(row["evidence_id"]) for row, _payload in parsed],
                },
            )
        row, payload = parsed[0]
        candidate_lineage = payload.phase0a_candidate_lineage
        source_watermark_hash = canonical_evidence_json_sha256(
            payload.phase0a_source_evidence
        )
        candidates = [
            HistoricalResearchCandidate(
                symbol=str(item["symbol"]),
                rank=int(item["rank"]),
                score=float(item["score"]),
                stock_name=item.get("stock_name"),
                component_scores=dict(item.get("component_scores") or {}),
            )
            for item in payload.selected_candidates
        ]
        return HistoricalSelectionEvidence(
            evidence_id=str(row["evidence_id"]),
            evidence_hash=str(row["artifact_hash"]),
            artifact_id=str(candidate_lineage["selection_score_artifact_id"]),
            artifact_payload_hash=str(candidate_lineage["selection_score_artifact_payload_sha256"]),
            source_watermark_hash=source_watermark_hash,
            candidate_outcome=payload.candidate_outcome,
            candidates=candidates,
        )

    @staticmethod
    def _assert_context_matches(
        *,
        context: HistoricalResearchProgramContext,
        payload: ProjectedHistoricalEvidenceV2,
        decision_trade_date: date,
    ) -> None:
        lineage = dict(payload.phase0a_package_lineage or {})
        binding_ref = dict(lineage.get("binding_ref") or {})
        config_chain = payload.phase0a_effective_config_chain
        if (
            payload.phase0a_candidate_lineage.get("package_id") != context.package_id
            or payload.phase0a_candidate_lineage.get("manifest_sha256") != context.manifest_sha256
            or binding_ref.get("binding_id") != context.binding_version_id
            or binding_ref.get("binding_hash") != context.binding_payload_hash
            or config_chain.get("package_effective_config_hash") != context.effective_runtime_config_hash
        ):
            raise RuntimeConfigInvalidError(
                "stored DailySelectionEvidence does not match the dated Program context",
                context={"program_id": context.program_id, "reason_code": REASON_PROGRAM_EVIDENCE_INVALID},
            )
        decision_clock = payload.decision_clock
        if (
            _payload_date(decision_clock.get("decision_as_of_trade_date")) != decision_trade_date
            or _payload_date(decision_clock.get("selection_as_of_trade_date")) != decision_trade_date
            or _payload_date(decision_clock.get("effective_cutoff_date")) != decision_trade_date
        ):
            raise RuntimeConfigInvalidError(
                "stored DailySelectionEvidence decision clock does not match the requested historical date",
                context={"program_id": context.program_id, "reason_code": REASON_PROGRAM_EVIDENCE_INVALID},
            )
        binding_available_at = _payload_datetime(config_chain.get("binding_base_available_at"))
        decision_cutoff = _payload_datetime(decision_clock.get("decision_cutoff_ts"))
        if binding_available_at is None or decision_cutoff is None or binding_available_at > decision_cutoff:
            raise RuntimeConfigInvalidError(
                "dated Program binding was not available by the frozen decision cutoff",
                context={"program_id": context.program_id, "reason_code": REASON_PROGRAM_EVIDENCE_INVALID},
            )
        for receipt in payload.phase0a_source_evidence:
            available_at = _payload_datetime(receipt.get("available_at")) or _payload_datetime(receipt.get("first_observed_at"))
            if available_at is None or available_at > decision_cutoff:
                raise RuntimeConfigInvalidError(
                    "historical source receipt was not available by the frozen decision cutoff",
                    context={
                        "program_id": context.program_id,
                        "dataset_id": receipt.get("dataset_id"),
                        "reason_code": REASON_PROGRAM_EVIDENCE_INVALID,
                    },
                )


def _payload_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _payload_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else None
    return None


class PostgresHistoricalResearchRepository:
    """Transactional persistence for batches, immutable Program runs, and receipts."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def get_or_create_batch(self, request: HistoricalResearchBatchRequest) -> HistoricalResearchBatch:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_research_batch (
                        batch_id, request_id, batch_key, decision_trade_date, program_ids,
                        data_source, origin, request_payload_sha256, research_scope,
                        execution_prohibited, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (batch_key) DO NOTHING
                    RETURNING *
                    """,
                    (
                        f"arb_{request.batch_key[:16]}",
                        str(request.request_id),
                        request.batch_key,
                        request.decision_trade_date,
                        psycopg2.extras.Json(request.program_ids),
                        request.data_source,
                        request.origin,
                        request.request_payload_sha256,
                        request.research_scope,
                        request.execution_prohibited,
                        HistoricalResearchRunStatus.PENDING.value,
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute("SELECT * FROM app.advisory_research_batch WHERE batch_key = %s", (request.batch_key,))
                    row = cur.fetchone()
        if row is None:
            raise RuntimeConfigInvalidError("historical research batch insert did not return a row")
        batch = _batch_from_row(dict(row))
        if batch.request_payload_sha256 != request.request_payload_sha256:
            raise _conflict_error(batch=batch, program_id=None)
        return batch

    def execute_program(
        self,
        *,
        batch: HistoricalResearchBatch,
        program_id: str,
        worker: Callable[[Any | None], _ProgramOutcome],
    ) -> HistoricalResearchProgramRun:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM app.advisory_research_program_run
                    WHERE program_id = %s AND decision_trade_date = %s AND research_scope = %s
                    FOR UPDATE
                    """,
                    (program_id, batch.decision_trade_date, batch.research_scope),
                )
                existing_row = cur.fetchone()
                existing = _program_run_from_row(dict(existing_row)) if existing_row is not None else None
                if existing is None:
                    pending = HistoricalResearchProgramRun(
                        program_run_id=_program_run_id(program_id=program_id, decision_trade_date=batch.decision_trade_date),
                        program_id=program_id,
                        decision_trade_date=batch.decision_trade_date,
                        research_scope=batch.research_scope,
                        status=HistoricalResearchRunStatus.PENDING,
                    )
                    _insert_program_run(cur, pending)
                    cur.execute(
                        "UPDATE app.advisory_research_program_run SET status = %s, updated_at = NOW() WHERE program_run_id = %s",
                        (HistoricalResearchRunStatus.RUNNING.value, pending.program_run_id),
                    )
                outcome = worker(cur)
                if existing is not None:
                    _assert_existing_program_run_compatible(existing=existing, outcome=outcome)
                    if existing.status in {HistoricalResearchRunStatus.COMPLETE, HistoricalResearchRunStatus.FAILED}:
                        return existing
                run = _program_run_from_outcome(batch=batch, program_id=program_id, outcome=outcome, existing=existing)
                _update_program_run(cur, run)
                return run

    def save_batch_receipt(
        self,
        *,
        batch: HistoricalResearchBatch,
        program_runs: list[HistoricalResearchProgramRun],
    ) -> HistoricalResearchBatchReceipt:
        status = _aggregate_status(program_runs)
        payload = _batch_receipt_payload(batch=batch, status=status, program_runs=program_runs)
        receipt_hash = canonical_json_sha256(payload)
        receipt = HistoricalResearchBatchReceipt(
            receipt_id=f"arr_{receipt_hash[:16]}",
            batch_id=batch.batch_id,
            batch_key=batch.batch_key,
            status=status,
            program_runs=list(program_runs),
            receipt_hash=receipt_hash,
        )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM app.advisory_research_batch_receipt WHERE batch_id = %s FOR UPDATE", (batch.batch_id,))
                existing = cur.fetchone()
                if existing is not None:
                    existing_status = HistoricalResearchRunStatus(str(existing["status"]))
                    if existing_status not in {
                        HistoricalResearchRunStatus.PENDING,
                        HistoricalResearchRunStatus.RUNNING,
                        HistoricalResearchRunStatus.WAITING_INPUT,
                    }:
                        if str(existing["receipt_hash"]) != receipt_hash:
                            raise _conflict_error(batch=batch, program_id=None)
                        return HistoricalResearchBatchReceipt(
                            receipt_id=str(existing["receipt_id"]),
                            batch_id=str(existing["batch_id"]),
                            batch_key=str(existing["batch_key"]),
                            status=existing_status,
                            program_runs=list(program_runs),
                            receipt_hash=str(existing["receipt_hash"]),
                            created_at=existing["created_at"],
                        )
                    cur.execute(
                        """
                        UPDATE app.advisory_research_batch_receipt
                        SET receipt_id = %s, status = %s, receipt_hash = %s,
                            program_run_ids = %s, receipt_payload_json = %s, created_at = %s
                        WHERE batch_id = %s
                        """,
                        (
                            receipt.receipt_id,
                            receipt.status.value,
                            receipt.receipt_hash,
                            psycopg2.extras.Json([run.program_run_id for run in receipt.program_runs]),
                            psycopg2.extras.Json(canonicalize(payload)),
                            receipt.created_at,
                            batch.batch_id,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_research_batch_receipt (
                            receipt_id, batch_id, batch_key, status, receipt_hash,
                            program_run_ids, receipt_payload_json, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            receipt.receipt_id,
                            receipt.batch_id,
                            receipt.batch_key,
                            receipt.status.value,
                            receipt.receipt_hash,
                            psycopg2.extras.Json([run.program_run_id for run in receipt.program_runs]),
                            psycopg2.extras.Json(canonicalize(payload)),
                            receipt.created_at,
                        ),
                    )
                cur.execute(
                    "UPDATE app.advisory_research_batch SET status = %s, updated_at = NOW() WHERE batch_id = %s",
                    (status.value, batch.batch_id),
                )
        return receipt

    def get_batch(self, batch_id: str) -> HistoricalResearchBatch:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM app.advisory_research_batch WHERE batch_id = %s", (batch_id,))
                row = cur.fetchone()
        if row is None:
            raise DataUnavailableError("historical research batch does not exist", context={"batch_id": batch_id})
        return _batch_from_row(dict(row))

    def get_batch_receipt(self, batch_id: str) -> HistoricalResearchBatchReceipt | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM app.advisory_research_batch_receipt WHERE batch_id = %s", (batch_id,))
                receipt_row = cur.fetchone()
                if receipt_row is None:
                    return None
                program_run_ids = list(receipt_row["program_run_ids"] or [])
                cur.execute(
                    "SELECT * FROM app.advisory_research_program_run WHERE program_run_id = ANY(%s)",
                    (program_run_ids,),
                )
                runs_by_id = {str(row["program_run_id"]): _program_run_from_row(dict(row)) for row in cur.fetchall()}
        program_runs = [runs_by_id[run_id] for run_id in program_run_ids if run_id in runs_by_id]
        if len(program_runs) != len(program_run_ids):
            raise RuntimeConfigInvalidError(
                "historical research batch receipt references missing Program runs",
                context={"batch_id": batch_id},
            )
        return HistoricalResearchBatchReceipt(
            receipt_id=str(receipt_row["receipt_id"]),
            batch_id=str(receipt_row["batch_id"]),
            batch_key=str(receipt_row["batch_key"]),
            status=HistoricalResearchRunStatus(str(receipt_row["status"])),
            program_runs=program_runs,
            receipt_hash=str(receipt_row["receipt_hash"]),
            created_at=receipt_row["created_at"],
        )

    def get_program_run(self, *, program_id: str, decision_trade_date: date) -> HistoricalResearchProgramRun | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT * FROM app.advisory_research_program_run
                    WHERE program_id = %s AND decision_trade_date = %s AND research_scope = %s
                    """,
                    (program_id, decision_trade_date, HISTORICAL_RESEARCH_SCOPE),
                )
                row = cur.fetchone()
        return _program_run_from_row(dict(row)) if row is not None else None


def _insert_program_run(cursor: Any, run: HistoricalResearchProgramRun) -> None:
    cursor.execute(
        """
        INSERT INTO app.advisory_research_program_run (
            program_run_id, program_id, decision_trade_date, research_scope, status,
            program_payload_sha256, binding_version_id, binding_payload_hash, package_id,
            manifest_sha256, policy_hash, effective_runtime_config_hash, source_watermark_hash,
            evidence_id, evidence_hash, artifact_id, artifact_payload_hash,
            research_list_version_id, research_candidates_json, candidate_outcome,
            reason_codes, error_json, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        _program_run_params(run),
    )


def _update_program_run(cursor: Any, run: HistoricalResearchProgramRun) -> None:
    cursor.execute(
        """
        UPDATE app.advisory_research_program_run
        SET status = %s, program_payload_sha256 = %s, binding_version_id = %s,
            binding_payload_hash = %s, package_id = %s, manifest_sha256 = %s,
            policy_hash = %s, effective_runtime_config_hash = %s, source_watermark_hash = %s,
            evidence_id = %s, evidence_hash = %s, artifact_id = %s, artifact_payload_hash = %s,
            research_list_version_id = %s, research_candidates_json = %s, candidate_outcome = %s,
            reason_codes = %s, error_json = %s, updated_at = %s
        WHERE program_run_id = %s
        """,
        (
            run.status.value,
            run.program_payload_sha256,
            run.binding_version_id,
            run.binding_payload_hash,
            run.package_id,
            run.manifest_sha256,
            run.policy_hash,
            run.effective_runtime_config_hash,
            run.source_watermark_hash,
            run.evidence_id,
            run.evidence_hash,
            run.artifact_id,
            run.artifact_payload_hash,
            run.research_list_version_id,
            psycopg2.extras.Json([item.model_dump(mode="json") for item in run.research_candidates]),
            run.candidate_outcome,
            psycopg2.extras.Json(run.reason_codes),
            psycopg2.extras.Json(run.error_json) if run.error_json else None,
            run.updated_at,
            run.program_run_id,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeConfigInvalidError("historical research Program run disappeared during update", context={"program_run_id": run.program_run_id})


def _program_run_params(run: HistoricalResearchProgramRun) -> tuple[Any, ...]:
    return (
        run.program_run_id,
        run.program_id,
        run.decision_trade_date,
        run.research_scope,
        run.status.value,
        run.program_payload_sha256,
        run.binding_version_id,
        run.binding_payload_hash,
        run.package_id,
        run.manifest_sha256,
        run.policy_hash,
        run.effective_runtime_config_hash,
        run.source_watermark_hash,
        run.evidence_id,
        run.evidence_hash,
        run.artifact_id,
        run.artifact_payload_hash,
        run.research_list_version_id,
        psycopg2.extras.Json([item.model_dump(mode="json") for item in run.research_candidates]),
        run.candidate_outcome,
        psycopg2.extras.Json(run.reason_codes),
        psycopg2.extras.Json(run.error_json) if run.error_json else None,
        run.created_at,
        run.updated_at,
    )


def _batch_from_row(row: dict[str, Any]) -> HistoricalResearchBatch:
    return HistoricalResearchBatch(
        batch_id=str(row["batch_id"]),
        request_id=UUID(str(row["request_id"])),
        batch_key=str(row["batch_key"]),
        decision_trade_date=row["decision_trade_date"],
        program_ids=[str(value) for value in row["program_ids"]],
        data_source=str(row["data_source"]),
        origin=str(row["origin"]),
        request_payload_sha256=str(row["request_payload_sha256"]),
        research_scope=str(row["research_scope"]),
        execution_prohibited=bool(row["execution_prohibited"]),
        status=HistoricalResearchRunStatus(str(row["status"])),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _program_run_from_row(row: dict[str, Any]) -> HistoricalResearchProgramRun:
    return HistoricalResearchProgramRun(
        program_run_id=str(row["program_run_id"]),
        program_id=str(row["program_id"]),
        decision_trade_date=row["decision_trade_date"],
        research_scope=str(row["research_scope"]),
        status=HistoricalResearchRunStatus(str(row["status"])),
        program_payload_sha256=row.get("program_payload_sha256"),
        binding_version_id=row.get("binding_version_id"),
        binding_payload_hash=row.get("binding_payload_hash"),
        package_id=row.get("package_id"),
        manifest_sha256=row.get("manifest_sha256"),
        policy_hash=row.get("policy_hash"),
        effective_runtime_config_hash=row.get("effective_runtime_config_hash"),
        source_watermark_hash=row.get("source_watermark_hash"),
        evidence_id=row.get("evidence_id"),
        evidence_hash=row.get("evidence_hash"),
        artifact_id=row.get("artifact_id"),
        artifact_payload_hash=row.get("artifact_payload_hash"),
        research_list_version_id=row.get("research_list_version_id"),
        research_candidates=[HistoricalResearchCandidate.model_validate(item) for item in (row.get("research_candidates_json") or [])],
        candidate_outcome=row.get("candidate_outcome"),
        reason_codes=[str(value) for value in (row.get("reason_codes") or [])],
        error_json=dict(row["error_json"]) if row.get("error_json") else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _assert_existing_program_run_compatible(*, existing: HistoricalResearchProgramRun, outcome: _ProgramOutcome) -> None:
    if (
        existing.program_payload_sha256 is not None
        and outcome.program_payload_sha256 is not None
        and existing.program_payload_sha256 != outcome.program_payload_sha256
    ):
        raise _conflict_error(batch=None, program_id=existing.program_id)


def _program_run_id(*, program_id: str, decision_trade_date: date) -> str:
    return f"arpr_{canonical_json_sha256({'program_id': program_id, 'decision_trade_date': decision_trade_date, 'research_scope': HISTORICAL_RESEARCH_SCOPE})[:16]}"


def _historical_date_error(message: str, **context: Any) -> RuntimeConfigInvalidError:
    return RuntimeConfigInvalidError(message, context={"reason_code": REASON_HISTORICAL_DATE_REQUIRED, **context})
