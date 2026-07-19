"""PostgreSQL repository for the isolated Phase 1R persistence boundary.

The connection factory is mandatory.  This module never reads environment
database settings, never falls back to the global production pool, and never
writes ordinary Selection, Advisory, Paper, simulation, QE, or QMT tables.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import psycopg2.extras

from backend.services.advisory_historical_range.canonical import canonical_json_sha256, canonicalize
from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.models import (
    DAY_TRANSITIONS,
    OPERATION_TRANSITIONS,
    PROGRAM_TRANSITIONS,
    REASON_DAY_PLAN_CONFLICT,
    REASON_IDEMPOTENCY_CONFLICT,
    REASON_REPOSITORY_CONFLICT,
    REASON_ROW_VERSION_CONFLICT,
    HistoricalRangeArtifactBindingsV1,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeArtifactEnvelopeV1,
    HistoricalRangeBatchStatus,
    HistoricalRangeCandidateFactV1,
    HistoricalRangeContractError,
    HistoricalRangeDatePlanV1,
    HistoricalRangeDayAttemptV1,
    HistoricalRangeDayPlanEntryV1,
    HistoricalRangeDayStatus,
    HistoricalRangeEpisodeSnapshotFactV1,
    HistoricalRangeListItemFactV1,
    HistoricalRangeListVersionFactV1,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeProgramStatus,
    HistoricalRangeSummaryFactV1,
    ResolvedHistoricalRangeRequestV1,
    build_candidate_artifact_payload,
    build_day_receipt_payload,
    derive_day_run_id,
    derive_list_content_hash,
    derive_prefixed_id,
    require_batch_transition,
    require_sha256,
    require_state_transition,
)


ConnFactory = Callable[[], AbstractContextManager[Any]]


@dataclass(frozen=True)
class CreatedHistoricalRangeBatch:
    batch_id: str
    range_run_ids: tuple[str, ...]
    create_operation_id: str
    idempotent: bool


@dataclass(frozen=True)
class MaterializedDayPlanChunk:
    range_run_id: str
    previous_cursor_ordinal: int
    next_cursor_ordinal: int
    entries: tuple[HistoricalRangeDayPlanEntryV1, ...]
    exhausted: bool


@dataclass(frozen=True)
class DayCommitResult:
    day_run_id: str
    list_version_id: str
    day_receipt_hash: str
    idempotent: bool


class PostgresHistoricalRangeRepository:
    """Durable Phase 1R repository with exact-retry conflict detection."""

    def __init__(self, *, conn_factory: ConnFactory, artifact_store: HistoricalRangeArtifactStore) -> None:
        if conn_factory is None:
            raise ValueError("conn_factory is required")
        if artifact_store is None:
            raise ValueError("artifact_store is required")
        self._conn_factory = conn_factory
        self._artifact_store = artifact_store

    def create_batch(
        self,
        *,
        resolved: ResolvedHistoricalRangeRequestV1,
        artifacts: HistoricalRangeArtifactBindingsV1,
    ) -> CreatedHistoricalRangeBatch:
        self._validate_creation_artifacts(resolved=resolved, artifacts=artifacts)
        request_json = resolved.model_dump(mode="json")
        request_json["artifact_refs"] = {
            "request": artifacts.request_ref.model_dump(mode="json"),
            "date_plan": artifacts.date_plan_ref.model_dump(mode="json"),
            "frozen_programs": {
                key: value.model_dump(mode="json") for key, value in sorted(artifacts.frozen_program_refs.items())
            },
        }
        create_operation_key = f"create:{resolved.request.client_idempotency_key}"
        create_operation = HistoricalRangeOperationRequestV1(
            operation_id=derive_prefixed_id(
                "ahrop",
                {
                    "batch_id": resolved.batch_id,
                    "operation_type": "CREATE",
                    "operation_idempotency_key": create_operation_key,
                    "request_payload_sha256": resolved.request_payload_sha256,
                },
            ),
            batch_id=resolved.batch_id,
            operation_type="CREATE",
            operation_idempotency_key=create_operation_key,
            request_payload_sha256=str(resolved.request_payload_sha256),
            expected_row_version=2,
        )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (resolved.request.user_request_semantic_hash,),
                )
                existing = self._find_existing_batch(cur=cur, resolved=resolved)
                if existing is not None:
                    self._assert_existing_batch_matches(existing=existing, resolved=resolved)
                    self._bind_request_key(
                        cur=cur,
                        resolved=resolved,
                        batch_id=str(existing["batch_id"]),
                        request_ref=artifacts.request_ref,
                    )
                    return self._created_batch_result(
                        cur=cur,
                        batch_id=str(existing["batch_id"]),
                        create_operation_id=create_operation.operation_id,
                        idempotent=True,
                    )

                cur.execute(
                    """
                    SELECT previous.batch_id
                    FROM app.advisory_historical_range_batch AS previous
                    LEFT JOIN app.advisory_historical_range_batch AS successor
                      ON successor.supersedes_batch_id = previous.batch_id
                    WHERE previous.user_request_semantic_hash = %s
                      AND successor.batch_id IS NULL
                    ORDER BY previous.created_at DESC, previous.batch_id DESC
                    LIMIT 1
                    FOR UPDATE OF previous
                    """,
                    (resolved.request.user_request_semantic_hash,),
                )
                predecessor_row = cur.fetchone()
                supersedes_batch_id = str(predecessor_row["batch_id"]) if predecessor_row is not None else None
                cur.execute(
                    """
                    INSERT INTO app.advisory_historical_range_batch (
                        batch_id, request_id, client_idempotency_key,
                        user_request_semantic_hash, request_payload_sha256,
                        supersedes_batch_id, start_trade_date, end_trade_date,
                        calendar_id, calendar_version, ordered_trade_dates_hash,
                        date_plan_ref, date_plan_hash, source_revision_catalog_hash,
                        selection_semantics_version, selection_semantics_hash,
                        list_semantics_version, list_semantics_hash,
                        per_program_input_warmup_ranges_hash,
                        program_count, trade_date_count, planned_day_count,
                        status, row_version, artifact_root_identity_hash,
                        request_payload_json
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        'QUEUED', 1, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING batch_id
                    """,
                    (
                        resolved.batch_id,
                        resolved.request.request_id,
                        resolved.request.client_idempotency_key,
                        resolved.request.user_request_semantic_hash,
                        resolved.request_payload_sha256,
                        supersedes_batch_id,
                        resolved.date_plan.start_trade_date,
                        resolved.date_plan.end_trade_date,
                        resolved.date_plan.calendar_id,
                        resolved.date_plan.calendar_version,
                        resolved.date_plan.ordered_trade_dates_hash,
                        psycopg2.extras.Json(artifacts.date_plan_ref.model_dump(mode="json")),
                        artifacts.date_plan_ref.semantic_content_hash,
                        resolved.source_revision_catalog_hash,
                        resolved.selection_semantics_version,
                        resolved.selection_semantics_hash,
                        resolved.list_semantics_version,
                        resolved.list_semantics_hash,
                        resolved.date_plan.per_program_input_warmup_ranges_hash,
                        len(resolved.frozen_programs),
                        len(resolved.date_plan.ordered_trade_dates),
                        len(resolved.frozen_programs) * len(resolved.date_plan.ordered_trade_dates),
                        artifacts.artifact_root_identity_hash,
                        psycopg2.extras.Json(request_json),
                    ),
                )
                inserted_batch = cur.fetchone()
                if inserted_batch is None:
                    existing = self._find_existing_batch(cur=cur, resolved=resolved)
                    if existing is None:
                        raise self._repository_error(
                            "batch insert conflicted without a resolvable idempotent row",
                            batch_id=resolved.batch_id,
                        )
                    self._assert_existing_batch_matches(existing=existing, resolved=resolved)
                    self._bind_request_key(
                        cur=cur,
                        resolved=resolved,
                        batch_id=str(existing["batch_id"]),
                        request_ref=artifacts.request_ref,
                    )
                    return self._created_batch_result(
                        cur=cur,
                        batch_id=str(existing["batch_id"]),
                        create_operation_id=create_operation.operation_id,
                        idempotent=True,
                    )
                self._bind_request_key(
                    cur=cur,
                    resolved=resolved,
                    batch_id=resolved.batch_id,
                    request_ref=artifacts.request_ref,
                )
                for frozen in resolved.frozen_programs:
                    frozen_json = frozen.model_dump(mode="json")
                    frozen_json["artifact_ref"] = artifacts.frozen_program_refs[frozen.research_program_id].model_dump(
                        mode="json"
                    )
                    cur.execute(
                        """
                        INSERT INTO app.advisory_historical_range_run (
                            range_run_id, batch_id, research_program_id,
                            source_program_id, source_program_version,
                            source_binding_version_id, package_id, package_version,
                            manifest_sha256, alpha_mode, program_config_hash,
                            runtime_config_hash, review_policy_hash,
                            style_profile_hash, code_release_id, code_release_hash,
                            target_package_asset_root_hash,
                            input_warmup_contract_hash,
                            admitted_package_projection_hash,
                            status, row_version, day_plan_ref, day_plan_hash,
                            frozen_program_json
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, 'QUEUED', 1, %s, %s, %s
                        )
                        """,
                        (
                            resolved.range_run_id(frozen.research_program_id),
                            resolved.batch_id,
                            frozen.research_program_id,
                            frozen.source_program_id,
                            frozen.source_program_version,
                            frozen.source_binding_version_id,
                            frozen.package_id,
                            frozen.package_version,
                            frozen.manifest_sha256,
                            frozen.alpha_mode.value,
                            frozen.program_config_hash,
                            frozen.runtime_config_hash,
                            frozen.review_policy_hash,
                            frozen.style_profile_hash,
                            frozen.code_release_id,
                            frozen.code_release_hash,
                            frozen.target_package_asset_root_hash,
                            frozen.input_warmup_contract_hash,
                            frozen.admitted_package_projection_hash,
                            psycopg2.extras.Json(artifacts.date_plan_ref.model_dump(mode="json")),
                            artifacts.date_plan_ref.semantic_content_hash,
                            psycopg2.extras.Json(frozen_json),
                        ),
                    )
                self._sync_batch_aggregate(cur=cur, batch_id=resolved.batch_id)
                self._insert_operation(cur=cur, request=create_operation)
                return self._created_batch_result(
                    cur=cur,
                    batch_id=resolved.batch_id,
                    create_operation_id=create_operation.operation_id,
                    idempotent=False,
                )

    def get_or_create_operation(
        self,
        request: HistoricalRangeOperationRequestV1,
    ) -> tuple[dict[str, Any], bool]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT * FROM app.advisory_historical_range_operation
                    WHERE batch_id = %s AND operation_idempotency_key = %s
                    FOR UPDATE
                    """,
                    (request.batch_id, request.operation_idempotency_key),
                )
                row = cur.fetchone()
                if row is not None:
                    self._assert_operation_matches(dict(row), request)
                    return dict(row), True
                inserted = self._insert_operation(cur=cur, request=request)
                cur.execute(
                    """
                    SELECT * FROM app.advisory_historical_range_operation
                    WHERE batch_id = %s AND operation_idempotency_key = %s
                    """,
                    (request.batch_id, request.operation_idempotency_key),
                )
                created = cur.fetchone()
                if created is None:
                    raise self._repository_error("created operation was not readable")
                created_row = dict(created)
                self._assert_operation_matches(created_row, request)
                return created_row, not inserted

    def materialize_day_plan_chunk(
        self,
        *,
        range_run_id: str,
        date_plan: HistoricalRangeDatePlanV1,
        date_plan_ref: HistoricalRangeArtifactRefV1,
        expected_cursor_ordinal: int,
        chunk_size: int = 500,
    ) -> MaterializedDayPlanChunk:
        if not 1 <= chunk_size <= 500:
            raise ValueError("chunk_size must be between 1 and 500")
        if expected_cursor_ordinal < 0:
            raise ValueError("expected_cursor_ordinal cannot be negative")
        if date_plan_ref.artifact_kind is not HistoricalRangeArtifactKind.DATE_PLAN:
            raise ValueError("date_plan_ref must reference DATE_PLAN")
        if date_plan_ref.payload_sha256 != canonical_json_sha256(date_plan.model_dump(mode="json")):
            raise HistoricalRangeContractError(
                REASON_DAY_PLAN_CONFLICT,
                "date plan object differs from the exact artifact ref payload",
                context={"range_run_id": range_run_id},
            )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT r.*, b.trade_date_count, b.request_payload_sha256
                    FROM app.advisory_historical_range_run AS r
                    JOIN app.advisory_historical_range_batch AS b ON b.batch_id = r.batch_id
                    WHERE r.range_run_id = %s
                    FOR UPDATE OF r
                    """,
                    (range_run_id,),
                )
                run_row = cur.fetchone()
                if run_row is None:
                    raise self._repository_error("range run does not exist", range_run_id=range_run_id)
                run = dict(run_row)
                if (
                    canonicalize(run["day_plan_ref"]) != canonicalize(date_plan_ref.model_dump(mode="json"))
                    or run["day_plan_hash"] != date_plan_ref.semantic_content_hash
                    or int(run["trade_date_count"]) != len(date_plan.ordered_trade_dates)
                ):
                    raise HistoricalRangeContractError(
                        REASON_DAY_PLAN_CONFLICT,
                        "frozen date plan differs from the range run",
                        context={"range_run_id": range_run_id},
                    )
                self._load_artifact(
                    date_plan_ref,
                    expected_kind=HistoricalRangeArtifactKind.DATE_PLAN,
                    resolved_request_hash=str(run["request_payload_sha256"]),
                    expected_payload=date_plan.model_dump(mode="json"),
                )
                actual_cursor = int(run["day_plan_cursor_ordinal"])
                if actual_cursor != expected_cursor_ordinal:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "day plan cursor differs from the expected ordinal",
                        context={
                            "range_run_id": range_run_id,
                            "expected_cursor_ordinal": expected_cursor_ordinal,
                            "actual_cursor_ordinal": actual_cursor,
                        },
                    )
                stop = min(actual_cursor + chunk_size, len(date_plan.ordered_trade_dates))
                entries: list[HistoricalRangeDayPlanEntryV1] = []
                for ordinal in range(actual_cursor + 1, stop + 1):
                    trade_date = date_plan.ordered_trade_dates[ordinal - 1]
                    previous_id = (
                        derive_day_run_id(
                            range_run_id,
                            date_plan.ordered_trade_dates[ordinal - 2],
                            ordinal - 1,
                        )
                        if ordinal > 1
                        else None
                    )
                    entry = HistoricalRangeDayPlanEntryV1(
                        range_run_id=range_run_id,
                        decision_trade_date=trade_date,
                        ordinal=ordinal,
                        previous_day_run_id=previous_id,
                    )
                    cur.execute(
                        """
                        INSERT INTO app.advisory_historical_range_day_run (
                            day_run_id, range_run_id, decision_trade_date, ordinal,
                            status, row_version, attempt_no, previous_day_run_id
                        ) VALUES (%s, %s, %s, %s, 'PENDING', 1, 0, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            entry.day_run_id,
                            entry.range_run_id,
                            entry.decision_trade_date,
                            entry.ordinal,
                            entry.previous_day_run_id,
                        ),
                    )
                    cur.execute(
                        """
                        SELECT day_run_id, range_run_id, decision_trade_date,
                               ordinal, previous_day_run_id
                        FROM app.advisory_historical_range_day_run
                        WHERE day_run_id = %s
                        """,
                        (entry.day_run_id,),
                    )
                    persisted = cur.fetchone()
                    if persisted is None or not self._day_plan_entry_matches(dict(persisted), entry):
                        raise HistoricalRangeContractError(
                            REASON_DAY_PLAN_CONFLICT,
                            "day plan exact retry resolved to different identity fields",
                            context={"day_run_id": entry.day_run_id},
                        )
                    entries.append(entry)
                if stop != actual_cursor:
                    cur.execute(
                        """
                        UPDATE app.advisory_historical_range_run
                        SET materialized_day_count = %s,
                            day_plan_cursor_ordinal = %s,
                            row_version = row_version + 1
                        WHERE range_run_id = %s AND row_version = %s
                        """,
                        (stop, stop, range_run_id, run["row_version"]),
                    )
                    if cur.rowcount != 1:
                        raise HistoricalRangeContractError(
                            REASON_ROW_VERSION_CONFLICT,
                            "range run changed during day-plan materialization",
                            context={"range_run_id": range_run_id},
                        )
                return MaterializedDayPlanChunk(
                    range_run_id=range_run_id,
                    previous_cursor_ordinal=actual_cursor,
                    next_cursor_ordinal=stop,
                    entries=tuple(entries),
                    exhausted=stop == len(date_plan.ordered_trade_dates),
                )

    def append_day_attempt(self, attempt: HistoricalRangeDayAttemptV1) -> bool:
        range_run_id, resolved_request_hash = self._get_day_artifact_identity(attempt.day_run_id)
        self._validate_day_attempt_artifacts(
            attempt=attempt,
            range_run_id=range_run_id,
            resolved_request_hash=resolved_request_hash,
        )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._insert_day_attempt(cur=cur, attempt=attempt)

    def append_operation_attempt(self, attempt: HistoricalRangeOperationAttemptV1) -> bool:
        resolved_request_hash = self._get_operation_artifact_identity(attempt.operation_id)
        self._validate_operation_attempt_artifacts(
            attempt=attempt,
            resolved_request_hash=resolved_request_hash,
        )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._insert_operation_attempt(cur=cur, attempt=attempt)

    def transition_batch(
        self,
        *,
        batch_id: str,
        expected_row_version: int,
        target_status: HistoricalRangeBatchStatus,
        error_json: dict[str, Any] | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                current = self._lock_row(
                    cur,
                    table="advisory_historical_range_batch",
                    key_name="batch_id",
                    key_value=batch_id,
                )
                self._require_row_version(current, expected_row_version, entity="batch", identity=batch_id)
                if (
                    str(current["status"]) == HistoricalRangeBatchStatus.PARTIAL.value
                    and int(current["recoverable_program_count"]) == 0
                    and current["finished_at"] is not None
                ):
                    raise self._repository_error(
                        "finished PARTIAL batch is immutable",
                        batch_id=batch_id,
                    )
                aggregate = self._batch_aggregate(cur=cur, batch_id=batch_id)
                require_batch_transition(
                    HistoricalRangeBatchStatus(str(current["status"])),
                    target_status,
                    successful_day_count=aggregate["successful_day_count"],
                    program_count=int(current["program_count"]),
                    failed_program_count=aggregate["failed_program_count"],
                    recoverable_program_count=aggregate["recoverable_program_count"],
                )
                now = datetime.now(UTC)
                started = current["started_at"] or started_at or now
                terminal = target_status in {
                    HistoricalRangeBatchStatus.COMPLETED,
                    HistoricalRangeBatchStatus.FAILED,
                    HistoricalRangeBatchStatus.CANCELLED,
                } or (
                    target_status is HistoricalRangeBatchStatus.PARTIAL and aggregate["recoverable_program_count"] == 0
                )
                finished = finished_at or now if terminal else None
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_batch
                    SET status = %s,
                        row_version = row_version + 1,
                        successful_day_count = %s,
                        terminal_failed_day_count = %s,
                        completed_program_count = %s,
                        failed_program_count = %s,
                        waiting_program_count = %s,
                        retryable_program_count = %s,
                        partial_program_count = %s,
                        recoverable_program_count = %s,
                        error_json = %s,
                        started_at = COALESCE(started_at, %s),
                        finished_at = %s
                    WHERE batch_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        target_status.value,
                        aggregate["successful_day_count"],
                        aggregate["terminal_failed_day_count"],
                        aggregate["completed_program_count"],
                        aggregate["failed_program_count"],
                        aggregate["waiting_program_count"],
                        aggregate["retryable_program_count"],
                        aggregate["partial_program_count"],
                        aggregate["recoverable_program_count"],
                        psycopg2.extras.Json(error_json) if error_json is not None else None,
                        started,
                        finished,
                        batch_id,
                        expected_row_version,
                    ),
                )
                return self._return_updated(cur, entity="batch", identity=batch_id)

    def transition_run(
        self,
        *,
        range_run_id: str,
        expected_row_version: int,
        target_status: HistoricalRangeProgramStatus,
        resume_trade_date: date | None = None,
        cancelled_from_ordinal: int | None = None,
        final_receipt_ref: HistoricalRangeArtifactRefV1 | None = None,
        error_json: dict[str, Any] | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> dict[str, Any]:
        terminal = target_status in {
            HistoricalRangeProgramStatus.COMPLETED,
            HistoricalRangeProgramStatus.FAILED,
            HistoricalRangeProgramStatus.CANCELLED,
        }
        range_run_id_from_db, resolved_request_hash = self._get_run_artifact_identity(range_run_id)
        if terminal:
            if final_receipt_ref is None:
                raise ValueError("terminal range run requires final_receipt_ref")
            envelope = self._load_artifact(
                final_receipt_ref,
                expected_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id_from_db,
            )
            if (
                envelope.payload.get("range_run_id") != range_run_id
                or envelope.payload.get("status") != target_status.value
            ):
                raise ValueError("range receipt payload differs from the terminal run identity/status")
        elif final_receipt_ref is not None:
            raise ValueError("non-terminal range run cannot publish final_receipt_ref")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                current = self._lock_row(
                    cur,
                    table="advisory_historical_range_run",
                    key_name="range_run_id",
                    key_value=range_run_id,
                )
                self._require_row_version(current, expected_row_version, entity="run", identity=range_run_id)
                require_state_transition(
                    HistoricalRangeProgramStatus(str(current["status"])),
                    target_status,
                    PROGRAM_TRANSITIONS,
                    entity="Program range run",
                )
                aggregate = self._run_aggregate(cur=cur, range_run_id=range_run_id)
                if (
                    target_status
                    in {
                        HistoricalRangeProgramStatus.FAILED,
                        HistoricalRangeProgramStatus.CANCELLED,
                    }
                    and aggregate["nonterminal_day_count"] != 0
                ):
                    raise self._repository_error(
                        "terminal range run still contains non-terminal materialized days",
                        range_run_id=range_run_id,
                    )
                now = datetime.now(UTC)
                started = current["started_at"] or started_at or now
                finished = finished_at or now if terminal else None
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_run
                    SET status = %s,
                        row_version = row_version + 1,
                        completed_day_count = %s,
                        failed_day_count = %s,
                        waiting_day_count = %s,
                        retryable_day_count = %s,
                        resume_trade_date = %s,
                        cancelled_from_ordinal = COALESCE(cancelled_from_ordinal, %s),
                        final_receipt_ref = COALESCE(final_receipt_ref, %s),
                        final_receipt_hash = COALESCE(final_receipt_hash, %s),
                        error_json = %s,
                        started_at = COALESCE(started_at, %s),
                        finished_at = %s
                    WHERE range_run_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        target_status.value,
                        aggregate["completed_day_count"],
                        aggregate["failed_day_count"],
                        aggregate["waiting_day_count"],
                        aggregate["retryable_day_count"],
                        resume_trade_date,
                        cancelled_from_ordinal,
                        psycopg2.extras.Json(final_receipt_ref.model_dump(mode="json"))
                        if final_receipt_ref is not None
                        else None,
                        final_receipt_ref.semantic_content_hash if final_receipt_ref is not None else None,
                        psycopg2.extras.Json(error_json) if error_json is not None else None,
                        started,
                        finished,
                        range_run_id,
                        expected_row_version,
                    ),
                )
                updated = self._return_updated(cur, entity="run", identity=range_run_id)
                self._sync_batch_aggregate(cur=cur, batch_id=str(current["batch_id"]))
                return updated

    def transition_day(
        self,
        *,
        day_run_id: str,
        expected_row_version: int,
        target_status: HistoricalRangeDayStatus,
        attempt_no: int,
        lease_expires_at: datetime | None = None,
        fencing_token: int | None = None,
        previous_day_run_hash: str | None = None,
        previous_list_version_id: str | None = None,
        previous_list_version_hash: str | None = None,
        reason_codes: Sequence[str] = (),
        error_json: dict[str, Any] | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        attempt: HistoricalRangeDayAttemptV1 | None = None,
        expired_attempt: HistoricalRangeDayAttemptV1 | None = None,
    ) -> dict[str, Any]:
        if target_status in {
            HistoricalRangeDayStatus.COMPLETE,
            HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
        }:
            raise ValueError("successful day states must use commit_successful_day")
        range_run_id, resolved_request_hash = self._get_day_artifact_identity(day_run_id)
        for evidence in (attempt, expired_attempt):
            if evidence is not None:
                self._validate_day_attempt_artifacts(
                    attempt=evidence,
                    range_run_id=range_run_id,
                    resolved_request_hash=resolved_request_hash,
                )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                current = self._lock_row(
                    cur,
                    table="advisory_historical_range_day_run",
                    key_name="day_run_id",
                    key_value=day_run_id,
                )
                self._require_row_version(current, expected_row_version, entity="day", identity=day_run_id)
                current_status = HistoricalRangeDayStatus(str(current["status"]))
                if current_status is target_status:
                    self._require_running_lease_update(
                        current=current,
                        attempt_no=attempt_no,
                        fencing_token=fencing_token,
                        lease_expires_at=lease_expires_at,
                        entity="day",
                    )
                    if current_status is HistoricalRangeDayStatus.RUNNING and attempt_no == int(current["attempt_no"]):
                        if attempt is not None or expired_attempt is not None:
                            raise ValueError("day heartbeat cannot append an attempt receipt")
                    elif current_status is HistoricalRangeDayStatus.RUNNING:
                        self._require_expired_day_attempt(current=current, attempt=expired_attempt)
                        self._insert_day_attempt(cur=cur, attempt=expired_attempt)
                else:
                    require_state_transition(
                        current_status,
                        target_status,
                        DAY_TRANSITIONS,
                        entity="day run",
                    )
                    if current_status is HistoricalRangeDayStatus.RUNNING or target_status in {
                        HistoricalRangeDayStatus.COMPLETE,
                        HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
                        HistoricalRangeDayStatus.WAITING_INPUT,
                        HistoricalRangeDayStatus.RETRYABLE_FAILED,
                        HistoricalRangeDayStatus.FAILED,
                        HistoricalRangeDayStatus.CANCELLED,
                    }:
                        self._require_final_day_attempt(
                            current=current,
                            target_status=target_status,
                            attempt_no=attempt_no,
                            fencing_token=fencing_token,
                            attempt=attempt,
                        )
                        self._insert_day_attempt(cur=cur, attempt=attempt)
                        if fencing_token is None:
                            fencing_token = attempt.fencing_token
                        if tuple(sorted(reason_codes)) != attempt.reason_codes or canonicalize(
                            error_json
                        ) != canonicalize(attempt.error_json):
                            raise ValueError("day state reason/error differs from the final attempt receipt")
                    elif attempt is not None or expired_attempt is not None:
                        raise ValueError("day transition does not accept attempt evidence")
                terminal = target_status in {
                    HistoricalRangeDayStatus.COMPLETE,
                    HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
                    HistoricalRangeDayStatus.FAILED,
                    HistoricalRangeDayStatus.CANCELLED,
                }
                now = datetime.now(UTC)
                started = (
                    current["started_at"]
                    or started_at
                    or (now if target_status is HistoricalRangeDayStatus.RUNNING or terminal else None)
                )
                finished = finished_at or now if terminal else None
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_day_run
                    SET status = %s,
                        row_version = row_version + 1,
                        attempt_no = %s,
                        lease_expires_at = %s,
                        current_fencing_token = COALESCE(%s, current_fencing_token),
                        previous_day_run_hash = COALESCE(previous_day_run_hash, %s),
                        previous_list_version_id = COALESCE(previous_list_version_id, %s),
                        previous_list_version_hash = COALESCE(previous_list_version_hash, %s),
                        reason_codes_json = %s,
                        error_json = %s,
                        started_at = COALESCE(started_at, %s),
                        finished_at = %s
                    WHERE day_run_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        target_status.value,
                        attempt_no,
                        lease_expires_at,
                        fencing_token,
                        previous_day_run_hash,
                        previous_list_version_id,
                        previous_list_version_hash,
                        psycopg2.extras.Json(list(reason_codes)),
                        psycopg2.extras.Json(error_json) if error_json is not None else None,
                        started,
                        finished,
                        day_run_id,
                        expected_row_version,
                    ),
                )
                updated = self._return_updated(cur, entity="day", identity=day_run_id)
                self._sync_run_aggregate(cur=cur, range_run_id=str(current["range_run_id"]))
                batch_id = self._batch_id_for_run(cur=cur, range_run_id=str(current["range_run_id"]))
                self._sync_batch_aggregate(cur=cur, batch_id=batch_id)
                return updated

    def transition_operation(
        self,
        *,
        operation_id: str,
        expected_row_version: int,
        target_status: HistoricalRangeOperationStatus,
        attempt_no: int,
        worker_id: str | None = None,
        lease_token: str | None = None,
        lease_expires_at: datetime | None = None,
        fencing_token: int | None = None,
        stable_keyset_cursor_json: dict[str, Any] | None = None,
        result_row_version: int | None = None,
        result_status: str | None = None,
        result_ref: HistoricalRangeArtifactRefV1 | None = None,
        error_json: dict[str, Any] | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        attempt: HistoricalRangeOperationAttemptV1 | None = None,
        expired_attempt: HistoricalRangeOperationAttemptV1 | None = None,
    ) -> dict[str, Any]:
        resolved_request_hash = self._get_operation_artifact_identity(operation_id)
        for evidence in (attempt, expired_attempt):
            if evidence is not None:
                self._validate_operation_attempt_artifacts(
                    attempt=evidence,
                    resolved_request_hash=resolved_request_hash,
                )
        terminal = target_status in {
            HistoricalRangeOperationStatus.COMPLETED,
            HistoricalRangeOperationStatus.FAILED,
        }
        if terminal:
            if result_ref is None:
                raise ValueError("terminal operation requires result_ref")
            self._validate_operation_result_artifact(
                ref=result_ref,
                resolved_request_hash=resolved_request_hash,
            )
        elif result_ref is not None:
            raise ValueError("non-terminal operation cannot publish result_ref")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                current = self._lock_row(
                    cur,
                    table="advisory_historical_range_operation",
                    key_name="operation_id",
                    key_value=operation_id,
                )
                self._require_row_version(current, expected_row_version, entity="operation", identity=operation_id)
                current_status = HistoricalRangeOperationStatus(str(current["status"]))
                if current_status is target_status:
                    self._require_running_lease_update(
                        current=current,
                        attempt_no=attempt_no,
                        fencing_token=fencing_token,
                        lease_expires_at=lease_expires_at,
                        entity="operation",
                    )
                    if current_status is HistoricalRangeOperationStatus.RUNNING and attempt_no == int(
                        current["attempt_no"]
                    ):
                        if attempt is not None or expired_attempt is not None:
                            raise ValueError("operation heartbeat cannot append an attempt receipt")
                    elif current_status is HistoricalRangeOperationStatus.RUNNING:
                        self._require_expired_operation_attempt(current=current, attempt=expired_attempt)
                        self._insert_operation_attempt(cur=cur, attempt=expired_attempt)
                else:
                    require_state_transition(
                        current_status,
                        target_status,
                        OPERATION_TRANSITIONS,
                        entity="operation",
                    )
                    if current_status is HistoricalRangeOperationStatus.RUNNING:
                        self._require_final_operation_attempt(
                            current=current,
                            target_status=target_status,
                            attempt_no=attempt_no,
                            fencing_token=fencing_token,
                            attempt=attempt,
                        )
                        self._insert_operation_attempt(cur=cur, attempt=attempt)
                        if terminal and (
                            result_ref is None
                            or attempt.result_hash != result_ref.semantic_content_hash
                            or attempt.error_json is not None
                        ):
                            raise ValueError("terminal operation result differs from the final attempt receipt")
                        if not terminal and canonicalize(error_json) != canonicalize(attempt.error_json):
                            raise ValueError("operation error differs from the final attempt receipt")
                    elif attempt is not None or expired_attempt is not None:
                        raise ValueError("operation transition does not accept attempt evidence")
                now = datetime.now(UTC)
                started = (
                    current["started_at"]
                    or started_at
                    or (now if target_status is HistoricalRangeOperationStatus.RUNNING or terminal else None)
                )
                finished = finished_at or now if terminal else None
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_operation
                    SET status = %s,
                        row_version = row_version + 1,
                        attempt_no = %s,
                        worker_id = %s,
                        lease_token = %s,
                        lease_expires_at = %s,
                        fencing_token = COALESCE(%s, fencing_token),
                        stable_keyset_cursor_json = COALESCE(%s, stable_keyset_cursor_json),
                        result_row_version = COALESCE(result_row_version, %s),
                        result_status = COALESCE(result_status, %s),
                        result_ref = COALESCE(result_ref, %s),
                        result_hash = COALESCE(result_hash, %s),
                        error_json = %s,
                        started_at = COALESCE(started_at, %s),
                        finished_at = %s
                    WHERE operation_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        target_status.value,
                        attempt_no,
                        worker_id,
                        lease_token,
                        lease_expires_at,
                        fencing_token,
                        psycopg2.extras.Json(stable_keyset_cursor_json)
                        if stable_keyset_cursor_json is not None
                        else None,
                        result_row_version,
                        result_status,
                        psycopg2.extras.Json(result_ref.model_dump(mode="json")) if result_ref is not None else None,
                        result_ref.semantic_content_hash if result_ref is not None else None,
                        psycopg2.extras.Json(error_json) if error_json is not None else None,
                        started,
                        finished,
                        operation_id,
                        expected_row_version,
                    ),
                )
                return self._return_updated(cur, entity="operation", identity=operation_id)

    def commit_successful_day(
        self,
        *,
        day_run_id: str,
        expected_row_version: int,
        expected_fencing_token: int,
        terminal_status: HistoricalRangeDayStatus,
        day_input_hash: str,
        candidate_artifact_ref: HistoricalRangeArtifactRefV1,
        day_receipt_ref: HistoricalRangeArtifactRefV1,
        list_version: HistoricalRangeListVersionFactV1,
        candidates: Sequence[HistoricalRangeCandidateFactV1],
        items: Sequence[HistoricalRangeListItemFactV1],
        episodes: Sequence[HistoricalRangeEpisodeSnapshotFactV1],
        attempt: HistoricalRangeDayAttemptV1,
        reason_codes: Sequence[str] = (),
        finished_at: datetime | None = None,
    ) -> DayCommitResult:
        day_input_hash = require_sha256(day_input_hash, field_name="day_input_hash")
        if terminal_status not in {
            HistoricalRangeDayStatus.COMPLETE,
            HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
        }:
            raise ValueError("terminal_status must be COMPLETE or VALID_NO_CANDIDATE")
        if candidate_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT:
            raise ValueError("candidate_artifact_ref must reference CANDIDATE_ARTIFACT")
        if day_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT:
            raise ValueError("day_receipt_ref must reference DAY_RECEIPT")
        if list_version.day_run_id != day_run_id:
            raise ValueError("list version belongs to a different day")
        if any(candidate.day_run_id != day_run_id for candidate in candidates):
            raise ValueError("candidate belongs to a different day")
        if any(item.list_version_id != list_version.list_version_id for item in items):
            raise ValueError("list item belongs to a different list")
        if any(episode.list_version_id != list_version.list_version_id for episode in episodes):
            raise ValueError("episode snapshot belongs to a different list")
        if len({candidate.symbol for candidate in candidates}) != len(candidates):
            raise ValueError("candidate symbols must be unique")
        if len({item.symbol for item in items}) != len(items):
            raise ValueError("list item symbols must be unique")
        expected_list_hash = derive_list_content_hash(list_version, items, episodes)
        if list_version.list_content_hash != expected_list_hash:
            raise ValueError("list_content_hash does not close list items and episode snapshots")
        included_count = sum(1 for candidate in candidates if candidate.membership_status == "INCLUDED")
        included_symbols = {candidate.symbol for candidate in candidates if candidate.membership_status == "INCLUDED"}
        watch_or_enter_symbols = {item.symbol for item in items if item.action.value in {"WATCH", "ENTER"}}
        if not watch_or_enter_symbols <= included_symbols:
            raise ValueError("WATCH/ENTER list items must originate from canonical included candidates")
        if list_version.watch_count > included_count:
            raise ValueError("watch_count cannot exceed the included candidate depth")
        if terminal_status is HistoricalRangeDayStatus.COMPLETE and included_count == 0:
            raise ValueError("COMPLETE requires at least one included candidate")
        if terminal_status is HistoricalRangeDayStatus.VALID_NO_CANDIDATE and included_count != 0:
            raise ValueError("VALID_NO_CANDIDATE cannot contain included candidates")
        finished = finished_at or datetime.now(UTC)
        range_run_id, resolved_request_hash = self._get_day_artifact_identity(day_run_id)
        candidate_envelope = self._load_artifact(
            candidate_artifact_ref,
            expected_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
            resolved_request_hash=resolved_request_hash,
            range_run_id=range_run_id,
            day_run_id=day_run_id,
        )
        candidate_payload = build_candidate_artifact_payload(
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            candidates=candidates,
            source_revision_refs=candidate_envelope.source_revision_refs,
        )
        if canonicalize(candidate_envelope.payload) != canonicalize(candidate_payload):
            raise ValueError("candidate artifact payload differs from the canonical candidate/source facts")
        day_receipt_payload = build_day_receipt_payload(
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            terminal_status=terminal_status,
            day_input_hash=day_input_hash,
            candidate_artifact_ref=candidate_artifact_ref,
            list_version=list_version,
            items=items,
            episodes=episodes,
            reason_codes=reason_codes,
        )
        self._load_artifact(
            day_receipt_ref,
            expected_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
            resolved_request_hash=resolved_request_hash,
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            expected_payload=day_receipt_payload,
            required_upstream_refs=(candidate_artifact_ref,),
        )
        self._validate_day_attempt_artifacts(
            attempt=attempt,
            range_run_id=range_run_id,
            resolved_request_hash=resolved_request_hash,
        )
        if (
            attempt.day_run_id != day_run_id
            or attempt.attempt_no < 1
            or attempt.fencing_token != expected_fencing_token
            or attempt.status != terminal_status.value
            or attempt.input_hash != day_input_hash
            or attempt.candidate_artifact_ref != candidate_artifact_ref
            or attempt.attempt_receipt_ref != day_receipt_ref
            or attempt.reason_codes != tuple(sorted(reason_codes))
            or attempt.error_json is not None
        ):
            raise ValueError("successful day attempt does not close the canonical day facts")

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                day = self._lock_row(
                    cur,
                    table="advisory_historical_range_day_run",
                    key_name="day_run_id",
                    key_value=day_run_id,
                )
                if str(day["status"]) in {"COMPLETE", "VALID_NO_CANDIDATE"}:
                    if (
                        str(day["status"]) == terminal_status.value
                        and canonicalize(day["candidate_artifact_ref"])
                        == canonicalize(candidate_artifact_ref.model_dump(mode="json"))
                        and day["candidate_artifact_hash"] == candidate_artifact_ref.semantic_content_hash
                        and day["day_input_hash"] == day_input_hash
                        and day["list_version_id"] == list_version.list_version_id
                        and day["list_version_hash"] == list_version.list_content_hash
                        and canonicalize(day["day_receipt_ref"])
                        == canonicalize(day_receipt_ref.model_dump(mode="json"))
                        and day["day_receipt_hash"] == day_receipt_ref.semantic_content_hash
                    ):
                        self._assert_persisted_day_commit_matches(
                            cur=cur,
                            day_run_id=day_run_id,
                            candidate_artifact_ref=candidate_artifact_ref,
                            list_version=list_version,
                            candidates=candidates,
                            items=items,
                            episodes=episodes,
                            attempt=attempt,
                        )
                        return DayCommitResult(
                            day_run_id=day_run_id,
                            list_version_id=list_version.list_version_id,
                            day_receipt_hash=day_receipt_ref.semantic_content_hash,
                            idempotent=True,
                        )
                    raise self._repository_error(
                        "successful day exact retry differs from immutable terminal facts",
                        day_run_id=day_run_id,
                    )
                self._require_row_version(day, expected_row_version, entity="day", identity=day_run_id)
                if str(day["status"]) != HistoricalRangeDayStatus.RUNNING.value:
                    raise self._repository_error(
                        "successful day commit requires RUNNING state",
                        day_run_id=day_run_id,
                        status=day["status"],
                    )
                if int(day["current_fencing_token"] or 0) != expected_fencing_token:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "day fencing token differs from the active attempt",
                        context={"day_run_id": day_run_id},
                    )
                if attempt.attempt_no != int(day["attempt_no"]):
                    raise ValueError("successful day attempt_no differs from the active day attempt")
                if list_version.range_run_id != str(day["range_run_id"]):
                    raise ValueError("list version belongs to a different range run")
                if (
                    list_version.previous_list_version_id != day["previous_list_version_id"]
                    or list_version.previous_list_hash != day["previous_list_version_hash"]
                    or list_version.previous_day_receipt_hash != day["previous_day_run_hash"]
                ):
                    raise ValueError("list predecessor facts differ from the canonical day chain")
                self._insert_day_attempt(cur=cur, attempt=attempt)
                for candidate in candidates:
                    self._insert_candidate(
                        cur=cur,
                        candidate=candidate,
                        candidate_artifact_ref=candidate_artifact_ref,
                    )
                self._insert_list_version(cur=cur, fact=list_version)
                for item in items:
                    self._insert_list_item(cur=cur, fact=item)
                for episode in episodes:
                    self._insert_episode(cur=cur, fact=episode)
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_day_run
                    SET status = %s,
                        row_version = row_version + 1,
                        lease_expires_at = NULL,
                        day_input_hash = %s,
                        candidate_artifact_ref = %s,
                        candidate_artifact_hash = %s,
                        list_version_id = %s,
                        list_version_hash = %s,
                        day_receipt_ref = %s,
                        day_receipt_hash = %s,
                        reason_codes_json = %s,
                        error_json = NULL,
                        finished_at = %s
                    WHERE day_run_id = %s
                      AND row_version = %s
                      AND current_fencing_token = %s
                    """,
                    (
                        terminal_status.value,
                        day_input_hash,
                        psycopg2.extras.Json(candidate_artifact_ref.model_dump(mode="json")),
                        candidate_artifact_ref.semantic_content_hash,
                        list_version.list_version_id,
                        list_version.list_content_hash,
                        psycopg2.extras.Json(day_receipt_ref.model_dump(mode="json")),
                        day_receipt_ref.semantic_content_hash,
                        psycopg2.extras.Json(list(reason_codes)),
                        finished,
                        day_run_id,
                        expected_row_version,
                        expected_fencing_token,
                    ),
                )
                if cur.rowcount != 1:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "day changed before its terminal fact transaction committed",
                        context={"day_run_id": day_run_id},
                    )
                self._sync_run_aggregate(
                    cur=cur,
                    range_run_id=list_version.range_run_id,
                    first_list_hash=list_version.list_content_hash,
                    latest_list_hash=list_version.list_content_hash,
                )
                self._sync_batch_aggregate(
                    cur=cur,
                    batch_id=self._batch_id_for_run(cur=cur, range_run_id=list_version.range_run_id),
                )
                return DayCommitResult(
                    day_run_id=day_run_id,
                    list_version_id=list_version.list_version_id,
                    day_receipt_hash=day_receipt_ref.semantic_content_hash,
                    idempotent=False,
                )

    def append_outcome(self, fact: HistoricalRangeOutcomeFactV1) -> bool:
        range_run_id, resolved_request_hash, subject_day_run_id = self._get_outcome_subject_identity(
            subject_type=fact.subject_type.value,
            subject_id=fact.subject_id,
        )
        required_upstream: tuple[HistoricalRangeArtifactRefV1, ...] = ()
        if fact.calculation_evidence_ref is not None:
            self._load_artifact(
                fact.calculation_evidence_ref,
                expected_kind=fact.calculation_evidence_ref.artifact_kind,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=subject_day_run_id,
                allow_ancestor_identity=True,
            )
            required_upstream = (fact.calculation_evidence_ref,)
        self._load_artifact(
            fact.outcome_artifact_ref,
            expected_kind=HistoricalRangeArtifactKind.OUTCOME,
            resolved_request_hash=resolved_request_hash,
            range_run_id=range_run_id,
            expected_payload=fact.outcome_json,
            required_upstream_refs=required_upstream,
        )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_historical_range_outcome (
                        outcome_version_id, outcome_logical_id, outcome_version,
                        subject_type, subject_id, projection, horizon_trade_days,
                        label_policy_hash, source_revision_set_hash,
                        predecessor_outcome_version_id, predecessor_outcome_hash,
                        maturity_status, label_as_of_trade_date,
                        next_refresh_trade_date, entry_execution_evidence_json,
                        exit_execution_evidence_json, benchmark_hash,
                        cost_policy_hash, corporate_action_hash,
                        calculation_evidence_ref, calculation_evidence_hash,
                        outcome_artifact_ref, outcome_artifact_hash,
                        outcome_json, outcome_content_hash
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        fact.outcome_version_id,
                        fact.outcome_logical_id,
                        fact.outcome_version,
                        fact.subject_type.value,
                        fact.subject_id,
                        fact.projection.value,
                        fact.horizon_trade_days,
                        fact.label_policy_hash,
                        fact.source_revision_set_hash,
                        fact.predecessor_outcome_version_id,
                        fact.predecessor_outcome_hash,
                        fact.maturity_status.value,
                        fact.label_as_of_trade_date,
                        fact.next_refresh_trade_date,
                        psycopg2.extras.Json(fact.entry_execution_evidence_json)
                        if fact.entry_execution_evidence_json is not None
                        else None,
                        psycopg2.extras.Json(fact.exit_execution_evidence_json)
                        if fact.exit_execution_evidence_json is not None
                        else None,
                        fact.benchmark_hash,
                        fact.cost_policy_hash,
                        fact.corporate_action_hash,
                        psycopg2.extras.Json(fact.calculation_evidence_ref.model_dump(mode="json"))
                        if fact.calculation_evidence_ref is not None
                        else None,
                        fact.calculation_evidence_ref.semantic_content_hash
                        if fact.calculation_evidence_ref is not None
                        else None,
                        psycopg2.extras.Json(fact.outcome_artifact_ref.model_dump(mode="json")),
                        fact.outcome_artifact_ref.semantic_content_hash,
                        psycopg2.extras.Json(fact.outcome_json),
                        fact.outcome_content_hash,
                    ),
                )
                inserted = cur.rowcount == 1
                cur.execute(
                    """
                    SELECT outcome_version_id, outcome_content_hash, source_revision_set_hash,
                           outcome_artifact_ref, outcome_artifact_hash
                    FROM app.advisory_historical_range_outcome
                    WHERE outcome_logical_id = %s
                      AND outcome_version = %s
                    """,
                    (fact.outcome_logical_id, fact.outcome_version),
                )
                row = cur.fetchone()
                if (
                    row is None
                    or row["outcome_version_id"] != fact.outcome_version_id
                    or row["outcome_content_hash"] != fact.outcome_content_hash
                    or row["source_revision_set_hash"] != fact.source_revision_set_hash
                    or canonicalize(row["outcome_artifact_ref"])
                    != canonicalize(fact.outcome_artifact_ref.model_dump(mode="json"))
                    or row["outcome_artifact_hash"] != fact.outcome_artifact_ref.semantic_content_hash
                ):
                    raise self._repository_error(
                        "outcome exact retry payload conflict",
                        outcome_logical_id=fact.outcome_logical_id,
                        outcome_version=fact.outcome_version,
                    )
                return not inserted

    def append_summary(self, fact: HistoricalRangeSummaryFactV1) -> bool:
        range_run_id, resolved_request_hash = self._get_run_artifact_identity(fact.range_run_id)
        self._load_artifact(
            fact.summary_artifact_ref,
            expected_kind=HistoricalRangeArtifactKind.SUMMARY,
            resolved_request_hash=resolved_request_hash,
            range_run_id=range_run_id,
            expected_payload=fact.summary_json,
        )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_historical_range_summary (
                        summary_id, range_run_id, summary_version,
                        covered_outcome_set_hash, predecessor_summary_id,
                        predecessor_summary_hash, summary_artifact_ref,
                        summary_artifact_hash, summary_json, summary_content_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        fact.summary_id,
                        fact.range_run_id,
                        fact.summary_version,
                        fact.covered_outcome_set_hash,
                        fact.predecessor_summary_id,
                        fact.predecessor_summary_hash,
                        psycopg2.extras.Json(fact.summary_artifact_ref.model_dump(mode="json")),
                        fact.summary_artifact_ref.semantic_content_hash,
                        psycopg2.extras.Json(fact.summary_json),
                        fact.summary_content_hash,
                    ),
                )
                inserted = cur.rowcount == 1
                cur.execute(
                    """
                    SELECT summary_id, summary_content_hash, summary_artifact_ref, summary_artifact_hash
                    FROM app.advisory_historical_range_summary
                    WHERE range_run_id = %s AND summary_version = %s
                    """,
                    (fact.range_run_id, fact.summary_version),
                )
                row = cur.fetchone()
                if (
                    row is None
                    or row["summary_id"] != fact.summary_id
                    or row["summary_content_hash"] != fact.summary_content_hash
                    or canonicalize(row["summary_artifact_ref"])
                    != canonicalize(fact.summary_artifact_ref.model_dump(mode="json"))
                    or row["summary_artifact_hash"] != fact.summary_artifact_ref.semantic_content_hash
                ):
                    raise self._repository_error(
                        "summary exact retry payload conflict",
                        range_run_id=fact.range_run_id,
                        summary_version=fact.summary_version,
                    )
                return not inserted

    def _get_day_artifact_identity(self, day_run_id: str) -> tuple[str, str]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT day.range_run_id, batch.request_payload_sha256
                    FROM app.advisory_historical_range_day_run AS day
                    JOIN app.advisory_historical_range_run AS run
                      ON run.range_run_id = day.range_run_id
                    JOIN app.advisory_historical_range_batch AS batch
                      ON batch.batch_id = run.batch_id
                    WHERE day.day_run_id = %s
                    """,
                    (day_run_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise self._repository_error("day run does not exist", day_run_id=day_run_id)
        return str(row["range_run_id"]), str(row["request_payload_sha256"])

    def _get_run_artifact_identity(self, range_run_id: str) -> tuple[str, str]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run.range_run_id, batch.request_payload_sha256
                    FROM app.advisory_historical_range_run AS run
                    JOIN app.advisory_historical_range_batch AS batch
                      ON batch.batch_id = run.batch_id
                    WHERE run.range_run_id = %s
                    """,
                    (range_run_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise self._repository_error("range run does not exist", range_run_id=range_run_id)
        return str(row["range_run_id"]), str(row["request_payload_sha256"])

    def _get_operation_artifact_identity(self, operation_id: str) -> str:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT batch.request_payload_sha256
                    FROM app.advisory_historical_range_operation AS operation
                    JOIN app.advisory_historical_range_batch AS batch
                      ON batch.batch_id = operation.batch_id
                    WHERE operation.operation_id = %s
                    """,
                    (operation_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise self._repository_error("operation does not exist", operation_id=operation_id)
        return str(row["request_payload_sha256"])

    def _get_outcome_subject_identity(self, *, subject_type: str, subject_id: str) -> tuple[str, str, str | None]:
        relation = {
            "CANDIDATE": (
                "app.advisory_historical_range_candidate AS subject "
                "JOIN app.advisory_historical_range_day_run AS day ON day.day_run_id = subject.day_run_id",
                "subject.candidate_id",
                "day.range_run_id",
                "day.day_run_id",
            ),
            "EPISODE": (
                "app.advisory_historical_range_episode_snapshot AS subject "
                "JOIN app.advisory_historical_range_list_version AS list ON list.list_version_id = subject.list_version_id "
                "JOIN app.advisory_historical_range_day_run AS day ON day.day_run_id = list.day_run_id",
                "subject.episode_snapshot_id",
                "day.range_run_id",
                "day.day_run_id",
            ),
            "LIST_VERSION": (
                "app.advisory_historical_range_list_version AS subject "
                "JOIN app.advisory_historical_range_day_run AS day ON day.day_run_id = subject.day_run_id",
                "subject.list_version_id",
                "day.range_run_id",
                "day.day_run_id",
            ),
            "RANGE": (
                "app.advisory_historical_range_run AS subject",
                "subject.range_run_id",
                "subject.range_run_id",
                "NULL::TEXT",
            ),
        }.get(subject_type)
        if relation is None:
            raise ValueError("unsupported outcome subject_type")
        from_sql, key_sql, range_sql, day_sql = relation
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT {range_sql} AS range_run_id, {day_sql} AS day_run_id,
                           batch.request_payload_sha256
                    FROM {from_sql}
                    JOIN app.advisory_historical_range_run AS run
                      ON run.range_run_id = {range_sql}
                    JOIN app.advisory_historical_range_batch AS batch
                      ON batch.batch_id = run.batch_id
                    WHERE {key_sql} = %s
                    """,
                    (subject_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise self._repository_error(
                "outcome subject does not exist",
                subject_type=subject_type,
                subject_id=subject_id,
            )
        return (
            str(row["range_run_id"]),
            str(row["request_payload_sha256"]),
            (str(row["day_run_id"]) if row["day_run_id"] is not None else None),
        )

    def _validate_day_attempt_artifacts(
        self,
        *,
        attempt: HistoricalRangeDayAttemptV1,
        range_run_id: str,
        resolved_request_hash: str,
    ) -> None:
        if attempt.candidate_artifact_ref is not None:
            self._load_artifact(
                attempt.candidate_artifact_ref,
                expected_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=attempt.day_run_id,
            )
        if attempt.attempt_receipt_ref is not None:
            required = (attempt.candidate_artifact_ref,) if attempt.candidate_artifact_ref is not None else ()
            self._load_artifact(
                attempt.attempt_receipt_ref,
                expected_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=attempt.day_run_id,
                required_upstream_refs=required,
            )

    def _validate_operation_attempt_artifacts(
        self,
        *,
        attempt: HistoricalRangeOperationAttemptV1,
        resolved_request_hash: str,
    ) -> None:
        if attempt.attempt_receipt_ref is not None:
            self._load_artifact(
                attempt.attempt_receipt_ref,
                expected_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
                resolved_request_hash=resolved_request_hash,
            )

    def _validate_operation_result_artifact(
        self,
        *,
        ref: HistoricalRangeArtifactRefV1,
        resolved_request_hash: str,
    ) -> None:
        envelope = self._artifact_store.load(ref)
        if envelope.resolved_request_hash != resolved_request_hash or envelope.day_run_id is not None:
            raise ValueError("operation result artifact differs from the batch operation identity")
        self._load_upstream_closure(
            envelope=envelope,
            resolved_request_hash=resolved_request_hash,
            range_run_id=envelope.range_run_id,
            day_run_id=None,
            visited={ref.semantic_content_hash},
        )

    def _insert_day_attempt(self, *, cur: Any, attempt: HistoricalRangeDayAttemptV1) -> bool:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_day_attempt (
                attempt_id, day_run_id, attempt_no, worker_id, lease_token,
                fencing_token, status, input_hash, result_hash,
                candidate_artifact_ref, candidate_artifact_hash,
                attempt_receipt_ref, attempt_receipt_hash,
                reason_codes_json, error_json, started_at, finished_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
            """,
            (
                attempt.attempt_id,
                attempt.day_run_id,
                attempt.attempt_no,
                attempt.worker_id,
                attempt.lease_token,
                attempt.fencing_token,
                attempt.status,
                attempt.input_hash,
                attempt.result_hash,
                psycopg2.extras.Json(attempt.candidate_artifact_ref.model_dump(mode="json"))
                if attempt.candidate_artifact_ref is not None
                else None,
                attempt.candidate_artifact_ref.semantic_content_hash
                if attempt.candidate_artifact_ref is not None
                else None,
                psycopg2.extras.Json(attempt.attempt_receipt_ref.model_dump(mode="json"))
                if attempt.attempt_receipt_ref is not None
                else None,
                attempt.attempt_receipt_ref.semantic_content_hash if attempt.attempt_receipt_ref is not None else None,
                psycopg2.extras.Json(list(attempt.reason_codes)),
                psycopg2.extras.Json(attempt.error_json) if attempt.error_json is not None else None,
                attempt.started_at,
                attempt.finished_at,
            ),
        )
        inserted = cur.rowcount == 1
        cur.execute(
            """
            SELECT attempt_id, day_run_id, attempt_no, worker_id, lease_token,
                   fencing_token, status, input_hash, result_hash,
                   candidate_artifact_ref, candidate_artifact_hash,
                   attempt_receipt_ref, attempt_receipt_hash,
                   reason_codes_json, error_json, started_at, finished_at
            FROM app.advisory_historical_range_day_attempt
            WHERE day_run_id = %s AND attempt_no = %s
            """,
            (attempt.day_run_id, attempt.attempt_no),
        )
        row = cur.fetchone()
        if row is None or self._canonical_row(dict(row)) != self._canonical_attempt(attempt):
            raise self._repository_error(
                "day attempt exact retry payload conflict",
                day_run_id=attempt.day_run_id,
                attempt_no=attempt.attempt_no,
            )
        return not inserted

    def _insert_operation_attempt(self, *, cur: Any, attempt: HistoricalRangeOperationAttemptV1) -> bool:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_operation_attempt (
                attempt_id, operation_id, attempt_no, worker_id, lease_token,
                fencing_token, status, input_cursor_json, result_cursor_json,
                input_hash, result_hash, attempt_receipt_ref,
                attempt_receipt_hash, reason_codes_json, error_json,
                started_at, finished_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
            """,
            (
                attempt.attempt_id,
                attempt.operation_id,
                attempt.attempt_no,
                attempt.worker_id,
                attempt.lease_token,
                attempt.fencing_token,
                attempt.status,
                psycopg2.extras.Json(attempt.input_cursor_json) if attempt.input_cursor_json is not None else None,
                psycopg2.extras.Json(attempt.result_cursor_json) if attempt.result_cursor_json is not None else None,
                attempt.input_hash,
                attempt.result_hash,
                psycopg2.extras.Json(attempt.attempt_receipt_ref.model_dump(mode="json"))
                if attempt.attempt_receipt_ref is not None
                else None,
                attempt.attempt_receipt_ref.semantic_content_hash if attempt.attempt_receipt_ref is not None else None,
                psycopg2.extras.Json(list(attempt.reason_codes)),
                psycopg2.extras.Json(attempt.error_json) if attempt.error_json is not None else None,
                attempt.started_at,
                attempt.finished_at,
            ),
        )
        inserted = cur.rowcount == 1
        cur.execute(
            """
            SELECT attempt_id, operation_id, attempt_no, worker_id,
                   lease_token, fencing_token, status, input_cursor_json,
                   result_cursor_json, input_hash, result_hash,
                   attempt_receipt_ref, attempt_receipt_hash,
                   reason_codes_json, error_json, started_at, finished_at
            FROM app.advisory_historical_range_operation_attempt
            WHERE operation_id = %s AND attempt_no = %s
            """,
            (attempt.operation_id, attempt.attempt_no),
        )
        row = cur.fetchone()
        if row is None or self._canonical_row(dict(row)) != self._canonical_operation_attempt(attempt):
            raise self._repository_error(
                "operation attempt exact retry payload conflict",
                operation_id=attempt.operation_id,
                attempt_no=attempt.attempt_no,
            )
        return not inserted

    @staticmethod
    def _require_expired_day_attempt(*, current: dict[str, Any], attempt: HistoricalRangeDayAttemptV1 | None) -> None:
        if (
            attempt is None
            or attempt.day_run_id != current["day_run_id"]
            or attempt.attempt_no != int(current["attempt_no"])
            or attempt.fencing_token != int(current["current_fencing_token"] or 0)
            or attempt.status != HistoricalRangeDayStatus.RETRYABLE_FAILED.value
        ):
            raise ValueError("day takeover requires the expired attempt's RETRYABLE_FAILED receipt")

    @staticmethod
    def _require_final_day_attempt(
        *,
        current: dict[str, Any],
        target_status: HistoricalRangeDayStatus,
        attempt_no: int,
        fencing_token: int | None,
        attempt: HistoricalRangeDayAttemptV1 | None,
    ) -> None:
        expected_fencing = int(current["current_fencing_token"] or fencing_token or 0)
        if (
            attempt is None
            or attempt.day_run_id != current["day_run_id"]
            or attempt.attempt_no != attempt_no
            or attempt.fencing_token != expected_fencing
            or attempt.status != target_status.value
        ):
            raise ValueError("day transition requires the exact final attempt receipt")

    @staticmethod
    def _require_expired_operation_attempt(
        *,
        current: dict[str, Any],
        attempt: HistoricalRangeOperationAttemptV1 | None,
    ) -> None:
        if (
            attempt is None
            or attempt.operation_id != current["operation_id"]
            or attempt.attempt_no != int(current["attempt_no"])
            or attempt.fencing_token != int(current["fencing_token"] or 0)
            or attempt.worker_id != current["worker_id"]
            or attempt.lease_token != current["lease_token"]
            or attempt.status != HistoricalRangeOperationStatus.RETRYABLE_FAILED.value
        ):
            raise ValueError("operation takeover requires the expired attempt's RETRYABLE_FAILED receipt")

    @staticmethod
    def _require_final_operation_attempt(
        *,
        current: dict[str, Any],
        target_status: HistoricalRangeOperationStatus,
        attempt_no: int,
        fencing_token: int | None,
        attempt: HistoricalRangeOperationAttemptV1 | None,
    ) -> None:
        if (
            attempt is None
            or attempt.operation_id != current["operation_id"]
            or attempt.attempt_no != attempt_no
            or attempt.fencing_token != int(current["fencing_token"] or fencing_token or 0)
            or attempt.worker_id != current["worker_id"]
            or attempt.lease_token != current["lease_token"]
            or attempt.status != target_status.value
        ):
            raise ValueError("operation transition requires the exact final attempt receipt")

    @staticmethod
    def _run_aggregate(*, cur: Any, range_run_id: str) -> dict[str, int]:
        cur.execute(
            """
            SELECT COUNT(*)::INTEGER AS materialized_day_count,
                   COUNT(*) FILTER (WHERE status IN ('COMPLETE', 'VALID_NO_CANDIDATE'))::INTEGER AS completed_day_count,
                   COUNT(*) FILTER (WHERE status = 'FAILED')::INTEGER AS failed_day_count,
                   COUNT(*) FILTER (WHERE status = 'WAITING_INPUT')::INTEGER AS waiting_day_count,
                   COUNT(*) FILTER (WHERE status = 'RETRYABLE_FAILED')::INTEGER AS retryable_day_count,
                   COUNT(*) FILTER (
                       WHERE status IN ('PENDING', 'WAITING_PREVIOUS_DAY', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED')
                   )::INTEGER AS nonterminal_day_count
            FROM app.advisory_historical_range_day_run
            WHERE range_run_id = %s
            """,
            (range_run_id,),
        )
        return {key: int(value or 0) for key, value in dict(cur.fetchone()).items()}

    @staticmethod
    def _batch_aggregate(*, cur: Any, batch_id: str) -> dict[str, int]:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE day.status IN ('COMPLETE', 'VALID_NO_CANDIDATE'))::BIGINT
                    AS successful_day_count,
                COUNT(*) FILTER (WHERE day.status = 'FAILED')::BIGINT AS terminal_failed_day_count,
                COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'COMPLETED')::INTEGER
                    AS completed_program_count,
                COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'FAILED')::INTEGER
                    AS failed_program_count,
                COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'WAITING_INPUT')::INTEGER
                    AS waiting_program_count,
                COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'RETRYABLE_FAILED')::INTEGER
                    AS retryable_program_count,
                COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'PARTIAL')::INTEGER
                    AS partial_program_count,
                COUNT(DISTINCT run.range_run_id) FILTER (
                    WHERE run.status IN ('QUEUED', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED', 'PARTIAL')
                )::INTEGER AS recoverable_program_count
            FROM app.advisory_historical_range_run AS run
            LEFT JOIN app.advisory_historical_range_day_run AS day
              ON day.range_run_id = run.range_run_id
            WHERE run.batch_id = %s
            """,
            (batch_id,),
        )
        return {key: int(value or 0) for key, value in dict(cur.fetchone()).items()}

    def _sync_run_aggregate(
        self,
        *,
        cur: Any,
        range_run_id: str,
        first_list_hash: str | None = None,
        latest_list_hash: str | None = None,
    ) -> None:
        aggregate = self._run_aggregate(cur=cur, range_run_id=range_run_id)
        cur.execute(
            """
            UPDATE app.advisory_historical_range_run
            SET materialized_day_count = %s,
                day_plan_cursor_ordinal = %s,
                completed_day_count = %s,
                failed_day_count = %s,
                waiting_day_count = %s,
                retryable_day_count = %s,
                first_list_hash = COALESCE(first_list_hash, %s),
                latest_list_hash = COALESCE(%s, latest_list_hash),
                row_version = row_version + 1
            WHERE range_run_id = %s
              AND (
                  materialized_day_count, day_plan_cursor_ordinal,
                  completed_day_count, failed_day_count,
                  waiting_day_count, retryable_day_count,
                  first_list_hash, latest_list_hash
              ) IS DISTINCT FROM (
                  %s, %s, %s, %s, %s, %s,
                  COALESCE(first_list_hash, %s), COALESCE(%s, latest_list_hash)
              )
            """,
            (
                aggregate["materialized_day_count"],
                aggregate["materialized_day_count"],
                aggregate["completed_day_count"],
                aggregate["failed_day_count"],
                aggregate["waiting_day_count"],
                aggregate["retryable_day_count"],
                first_list_hash,
                latest_list_hash,
                range_run_id,
                aggregate["materialized_day_count"],
                aggregate["materialized_day_count"],
                aggregate["completed_day_count"],
                aggregate["failed_day_count"],
                aggregate["waiting_day_count"],
                aggregate["retryable_day_count"],
                first_list_hash,
                latest_list_hash,
            ),
        )

    def _sync_batch_aggregate(self, *, cur: Any, batch_id: str) -> None:
        aggregate = self._batch_aggregate(cur=cur, batch_id=batch_id)
        cur.execute(
            """
            UPDATE app.advisory_historical_range_batch
            SET successful_day_count = %s,
                terminal_failed_day_count = %s,
                completed_program_count = %s,
                failed_program_count = %s,
                waiting_program_count = %s,
                retryable_program_count = %s,
                partial_program_count = %s,
                recoverable_program_count = %s,
                row_version = row_version + 1
            WHERE batch_id = %s
              AND (
                  successful_day_count, terminal_failed_day_count,
                  completed_program_count, failed_program_count,
                  waiting_program_count, retryable_program_count,
                  partial_program_count, recoverable_program_count
              ) IS DISTINCT FROM (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                aggregate["successful_day_count"],
                aggregate["terminal_failed_day_count"],
                aggregate["completed_program_count"],
                aggregate["failed_program_count"],
                aggregate["waiting_program_count"],
                aggregate["retryable_program_count"],
                aggregate["partial_program_count"],
                aggregate["recoverable_program_count"],
                batch_id,
                aggregate["successful_day_count"],
                aggregate["terminal_failed_day_count"],
                aggregate["completed_program_count"],
                aggregate["failed_program_count"],
                aggregate["waiting_program_count"],
                aggregate["retryable_program_count"],
                aggregate["partial_program_count"],
                aggregate["recoverable_program_count"],
            ),
        )

    @staticmethod
    def _batch_id_for_run(*, cur: Any, range_run_id: str) -> str:
        cur.execute(
            "SELECT batch_id FROM app.advisory_historical_range_run WHERE range_run_id = %s",
            (range_run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "range run does not exist",
                context={"range_run_id": range_run_id},
            )
        return str(row["batch_id"])

    def _assert_persisted_day_commit_matches(
        self,
        *,
        cur: Any,
        day_run_id: str,
        candidate_artifact_ref: HistoricalRangeArtifactRefV1,
        list_version: HistoricalRangeListVersionFactV1,
        candidates: Sequence[HistoricalRangeCandidateFactV1],
        items: Sequence[HistoricalRangeListItemFactV1],
        episodes: Sequence[HistoricalRangeEpisodeSnapshotFactV1],
        attempt: HistoricalRangeDayAttemptV1,
    ) -> None:
        cur.execute(
            """
            SELECT list_version_id, day_run_id, range_run_id, list_content_hash
            FROM app.advisory_historical_range_list_version
            WHERE list_version_id = %s
            """,
            (list_version.list_version_id,),
        )
        persisted_list = cur.fetchone()
        if persisted_list is None or canonicalize(dict(persisted_list)) != canonicalize(
            {
                "list_version_id": list_version.list_version_id,
                "day_run_id": list_version.day_run_id,
                "range_run_id": list_version.range_run_id,
                "list_content_hash": list_version.list_content_hash,
            }
        ):
            raise self._repository_error("successful day list readback conflict", day_run_id=day_run_id)
        cur.execute(
            """
            SELECT candidate_id, symbol, candidate_content_hash, artifact_ref, artifact_hash
            FROM app.advisory_historical_range_candidate
            WHERE day_run_id = %s
            ORDER BY symbol, candidate_id
            """,
            (day_run_id,),
        )
        expected_candidates = [
            {
                "candidate_id": item.candidate_id,
                "symbol": item.symbol,
                "candidate_content_hash": item.candidate_content_hash,
                "artifact_ref": candidate_artifact_ref.model_dump(mode="json"),
                "artifact_hash": candidate_artifact_ref.semantic_content_hash,
            }
            for item in sorted(candidates, key=lambda item: (item.symbol, item.candidate_id))
        ]
        if canonicalize([dict(row) for row in cur.fetchall()]) != canonicalize(expected_candidates):
            raise self._repository_error("successful day candidate readback conflict", day_run_id=day_run_id)
        cur.execute(
            """
            SELECT list_item_id, symbol, evidence_hash
            FROM app.advisory_historical_range_list_item
            WHERE list_version_id = %s
            ORDER BY symbol, list_item_id
            """,
            (list_version.list_version_id,),
        )
        expected_items = [
            {"list_item_id": item.list_item_id, "symbol": item.symbol, "evidence_hash": item.evidence_hash}
            for item in sorted(items, key=lambda item: (item.symbol, item.list_item_id))
        ]
        if canonicalize([dict(row) for row in cur.fetchall()]) != canonicalize(expected_items):
            raise self._repository_error("successful day list item readback conflict", day_run_id=day_run_id)
        cur.execute(
            """
            SELECT episode_snapshot_id, symbol, evidence_hash
            FROM app.advisory_historical_range_episode_snapshot
            WHERE list_version_id = %s
            ORDER BY symbol, episode_snapshot_id
            """,
            (list_version.list_version_id,),
        )
        expected_episodes = [
            {
                "episode_snapshot_id": item.episode_snapshot_id,
                "symbol": item.symbol,
                "evidence_hash": item.evidence_hash,
            }
            for item in sorted(episodes, key=lambda item: (item.symbol, item.episode_snapshot_id))
        ]
        if canonicalize([dict(row) for row in cur.fetchall()]) != canonicalize(expected_episodes):
            raise self._repository_error("successful day episode readback conflict", day_run_id=day_run_id)
        cur.execute(
            """
            SELECT attempt_id, day_run_id, attempt_no, worker_id, lease_token,
                   fencing_token, status, input_hash, result_hash,
                   candidate_artifact_ref, candidate_artifact_hash,
                   attempt_receipt_ref, attempt_receipt_hash,
                   reason_codes_json, error_json, started_at, finished_at
            FROM app.advisory_historical_range_day_attempt
            WHERE day_run_id = %s AND attempt_no = %s
            """,
            (day_run_id, attempt.attempt_no),
        )
        persisted_attempt = cur.fetchone()
        if persisted_attempt is None or self._canonical_row(dict(persisted_attempt)) != self._canonical_attempt(
            attempt
        ):
            raise self._repository_error("successful day attempt readback conflict", day_run_id=day_run_id)

    def _validate_creation_artifacts(
        self,
        *,
        resolved: ResolvedHistoricalRangeRequestV1,
        artifacts: HistoricalRangeArtifactBindingsV1,
    ) -> None:
        if artifacts.artifact_root_identity_hash != self._artifact_store.root_identity_hash:
            raise ValueError("artifact_root_identity_hash differs from the configured artifact store")
        self._load_artifact(
            artifacts.request_ref,
            expected_kind=HistoricalRangeArtifactKind.REQUEST,
            resolved_request_hash=str(resolved.request_payload_sha256),
            expected_payload=resolved.model_dump(mode="json"),
        )
        self._load_artifact(
            artifacts.date_plan_ref,
            expected_kind=HistoricalRangeArtifactKind.DATE_PLAN,
            resolved_request_hash=str(resolved.request_payload_sha256),
            expected_payload=resolved.date_plan.model_dump(mode="json"),
        )
        expected_programs = {item.research_program_id for item in resolved.frozen_programs}
        if set(artifacts.frozen_program_refs) != expected_programs:
            raise ValueError("frozen Program artifact refs differ from resolved Programs")
        for frozen in resolved.frozen_programs:
            ref = artifacts.frozen_program_refs[frozen.research_program_id]
            self._load_artifact(
                ref,
                expected_kind=HistoricalRangeArtifactKind.FROZEN_PROGRAM,
                resolved_request_hash=str(resolved.request_payload_sha256),
                range_run_id=resolved.range_run_id(frozen.research_program_id),
                expected_payload=frozen.model_dump(mode="json"),
            )

    def _load_artifact(
        self,
        ref: HistoricalRangeArtifactRefV1,
        *,
        expected_kind: HistoricalRangeArtifactKind,
        resolved_request_hash: str,
        range_run_id: str | None = None,
        day_run_id: str | None = None,
        expected_payload: dict[str, Any] | None = None,
        required_upstream_refs: Sequence[HistoricalRangeArtifactRefV1] = (),
        allow_ancestor_identity: bool = False,
    ) -> HistoricalRangeArtifactEnvelopeV1:
        if ref.artifact_kind is not expected_kind:
            raise ValueError(f"artifact ref must reference {expected_kind.value}")
        envelope = self._artifact_store.load(ref)
        if envelope.resolved_request_hash != resolved_request_hash:
            raise ValueError("artifact resolved_request_hash differs from the repository request")
        if allow_ancestor_identity:
            if envelope.range_run_id not in {None, range_run_id} or envelope.day_run_id not in {None, day_run_id}:
                raise ValueError("artifact range/day identity differs from the repository operation")
        elif envelope.range_run_id != range_run_id or envelope.day_run_id != day_run_id:
            raise ValueError("artifact range/day identity differs from the repository operation")
        if expected_payload is not None and canonicalize(envelope.payload) != canonicalize(expected_payload):
            raise ValueError("artifact payload differs from the canonical repository facts")
        upstream_by_identity = {
            (item.artifact_kind, item.semantic_content_hash, item.relative_path): item
            for item in envelope.upstream_refs
        }
        for required in required_upstream_refs:
            identity = (required.artifact_kind, required.semantic_content_hash, required.relative_path)
            if identity not in upstream_by_identity:
                raise ValueError("artifact upstream closure omits a required exact ref")
        self._load_upstream_closure(
            envelope=envelope,
            resolved_request_hash=resolved_request_hash,
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            visited={ref.semantic_content_hash},
        )
        return envelope

    def _load_upstream_closure(
        self,
        *,
        envelope: HistoricalRangeArtifactEnvelopeV1,
        resolved_request_hash: str,
        range_run_id: str | None,
        day_run_id: str | None,
        visited: set[str],
    ) -> None:
        for upstream_ref in envelope.upstream_refs:
            if upstream_ref.semantic_content_hash in visited:
                continue
            visited.add(upstream_ref.semantic_content_hash)
            upstream = self._artifact_store.load(upstream_ref)
            if upstream.resolved_request_hash != resolved_request_hash:
                raise ValueError("upstream artifact resolved_request_hash differs from its consumer")
            if range_run_id is not None and upstream.range_run_id not in {None, range_run_id}:
                raise ValueError("upstream artifact belongs to a different range run")
            if day_run_id is not None and upstream.day_run_id not in {None, day_run_id}:
                raise ValueError("upstream artifact belongs to a different day run")
            self._load_upstream_closure(
                envelope=upstream,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=day_run_id,
                visited=visited,
            )

    @staticmethod
    def _find_existing_batch(
        *,
        cur: Any,
        resolved: ResolvedHistoricalRangeRequestV1,
    ) -> dict[str, Any] | None:
        cur.execute(
            """
            SELECT batch.*
            FROM app.advisory_historical_range_request_key AS request_key
            JOIN app.advisory_historical_range_batch AS batch
              ON batch.batch_id = request_key.batch_id
            WHERE request_key.client_idempotency_key = %s
            FOR UPDATE OF batch, request_key
            """,
            (resolved.request.client_idempotency_key,),
        )
        by_key = cur.fetchone()
        if by_key is not None:
            row = dict(by_key)
            if row["request_payload_sha256"] != resolved.request_payload_sha256:
                raise HistoricalRangeContractError(
                    REASON_IDEMPOTENCY_CONFLICT,
                    "same client idempotency key resolved to different business identity",
                    context={
                        "client_idempotency_key": resolved.request.client_idempotency_key,
                        "existing_batch_id": row["batch_id"],
                    },
                )
            return row
        cur.execute(
            """
            SELECT * FROM app.advisory_historical_range_batch
            WHERE request_payload_sha256 = %s
            FOR UPDATE
            """,
            (resolved.request_payload_sha256,),
        )
        by_payload = cur.fetchone()
        return dict(by_payload) if by_payload is not None else None

    @staticmethod
    def _bind_request_key(
        *,
        cur: Any,
        resolved: ResolvedHistoricalRangeRequestV1,
        batch_id: str,
        request_ref: HistoricalRangeArtifactRefV1,
    ) -> None:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_request_key (
                client_idempotency_key, batch_id, request_id,
                request_payload_sha256, request_artifact_ref,
                request_artifact_hash
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (client_idempotency_key) DO NOTHING
            """,
            (
                resolved.request.client_idempotency_key,
                batch_id,
                resolved.request.request_id,
                resolved.request_payload_sha256,
                psycopg2.extras.Json(request_ref.model_dump(mode="json")),
                request_ref.semantic_content_hash,
            ),
        )
        cur.execute(
            """
            SELECT batch_id, request_payload_sha256,
                   request_artifact_ref, request_artifact_hash
            FROM app.advisory_historical_range_request_key
            WHERE client_idempotency_key = %s
            """,
            (resolved.request.client_idempotency_key,),
        )
        row = cur.fetchone()
        if (
            row is None
            or str(row["batch_id"]) != batch_id
            or str(row["request_payload_sha256"]) != resolved.request_payload_sha256
            or canonicalize(row["request_artifact_ref"]) != canonicalize(request_ref.model_dump(mode="json"))
            or str(row["request_artifact_hash"]) != request_ref.semantic_content_hash
        ):
            raise HistoricalRangeContractError(
                REASON_IDEMPOTENCY_CONFLICT,
                "client idempotency key is already bound to different resolved semantics",
                context={
                    "client_idempotency_key": resolved.request.client_idempotency_key,
                    "batch_id": batch_id,
                },
            )

    @staticmethod
    def _assert_existing_batch_matches(
        *,
        existing: dict[str, Any],
        resolved: ResolvedHistoricalRangeRequestV1,
    ) -> None:
        exact = {
            "batch_id": resolved.batch_id,
            "user_request_semantic_hash": resolved.request.user_request_semantic_hash,
            "request_payload_sha256": resolved.request_payload_sha256,
            "start_trade_date": resolved.date_plan.start_trade_date,
            "end_trade_date": resolved.date_plan.end_trade_date,
            "calendar_id": resolved.date_plan.calendar_id,
            "calendar_version": resolved.date_plan.calendar_version,
            "ordered_trade_dates_hash": resolved.date_plan.ordered_trade_dates_hash,
            "source_revision_catalog_hash": resolved.source_revision_catalog_hash,
            "selection_semantics_version": resolved.selection_semantics_version,
            "selection_semantics_hash": resolved.selection_semantics_hash,
            "list_semantics_version": resolved.list_semantics_version,
            "list_semantics_hash": resolved.list_semantics_hash,
            "per_program_input_warmup_ranges_hash": resolved.date_plan.per_program_input_warmup_ranges_hash,
            "program_count": len(resolved.frozen_programs),
            "trade_date_count": len(resolved.date_plan.ordered_trade_dates),
            "planned_day_count": len(resolved.frozen_programs) * len(resolved.date_plan.ordered_trade_dates),
        }
        mismatches = {
            key: {"expected": value, "actual": existing.get(key)}
            for key, value in exact.items()
            if existing.get(key) != value
        }
        if mismatches:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "existing batch differs from the resolved request",
                context={"batch_id": existing.get("batch_id"), "mismatches": mismatches},
            )

    @staticmethod
    def _created_batch_result(
        *,
        cur: Any,
        batch_id: str,
        create_operation_id: str,
        idempotent: bool,
    ) -> CreatedHistoricalRangeBatch:
        cur.execute(
            """
            SELECT range_run_id
            FROM app.advisory_historical_range_run
            WHERE batch_id = %s
            ORDER BY research_program_id
            """,
            (batch_id,),
        )
        range_run_ids = tuple(str(row["range_run_id"]) for row in cur.fetchall())
        cur.execute(
            """
            SELECT operation_id FROM app.advisory_historical_range_operation
            WHERE batch_id = %s AND operation_type = 'CREATE'
            ORDER BY created_at, operation_id
            LIMIT 1
            """,
            (batch_id,),
        )
        operation = cur.fetchone()
        if operation is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "batch exists without its CREATE operation",
                context={"batch_id": batch_id},
            )
        persisted_operation_id = str(operation["operation_id"])
        if not idempotent and persisted_operation_id != create_operation_id:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "created batch operation identity differs from the request",
                context={"batch_id": batch_id},
            )
        return CreatedHistoricalRangeBatch(
            batch_id=batch_id,
            range_run_ids=range_run_ids,
            create_operation_id=persisted_operation_id,
            idempotent=idempotent,
        )

    @staticmethod
    def _insert_operation(*, cur: Any, request: HistoricalRangeOperationRequestV1) -> bool:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_operation (
                operation_id, batch_id, operation_type,
                operation_idempotency_key, request_payload_sha256,
                expected_row_version, status, row_version, attempt_no
            ) VALUES (%s, %s, %s, %s, %s, %s, 'QUEUED', 1, 0)
            ON CONFLICT DO NOTHING
            """,
            (
                request.operation_id,
                request.batch_id,
                request.operation_type.value,
                request.operation_idempotency_key,
                request.request_payload_sha256,
                request.expected_row_version,
            ),
        )
        return cur.rowcount == 1

    @staticmethod
    def _assert_operation_matches(
        row: dict[str, Any],
        request: HistoricalRangeOperationRequestV1,
    ) -> None:
        expected = {
            "operation_id": request.operation_id,
            "batch_id": request.batch_id,
            "operation_type": request.operation_type.value,
            "operation_idempotency_key": request.operation_idempotency_key,
            "request_payload_sha256": request.request_payload_sha256,
            "expected_row_version": request.expected_row_version,
        }
        if any(row.get(key) != value for key, value in expected.items()):
            raise HistoricalRangeContractError(
                REASON_IDEMPOTENCY_CONFLICT,
                "same operation key resolved to a different payload",
                context={
                    "batch_id": request.batch_id,
                    "operation_idempotency_key": request.operation_idempotency_key,
                },
            )

    @staticmethod
    def _day_plan_entry_matches(
        row: dict[str, Any],
        entry: HistoricalRangeDayPlanEntryV1,
    ) -> bool:
        return (
            row.get("day_run_id") == entry.day_run_id
            and row.get("range_run_id") == entry.range_run_id
            and row.get("decision_trade_date") == entry.decision_trade_date
            and int(row.get("ordinal")) == entry.ordinal
            and row.get("previous_day_run_id") == entry.previous_day_run_id
        )

    @staticmethod
    def _lock_row(
        cur: Any,
        *,
        table: str,
        key_name: str,
        key_value: str,
    ) -> dict[str, Any]:
        allowed = {
            ("advisory_historical_range_batch", "batch_id"),
            ("advisory_historical_range_run", "range_run_id"),
            ("advisory_historical_range_day_run", "day_run_id"),
            ("advisory_historical_range_operation", "operation_id"),
        }
        if (table, key_name) not in allowed:
            raise ValueError("unsupported orchestration row")
        cur.execute(
            f"SELECT * FROM app.{table} WHERE {key_name} = %s FOR UPDATE",  # noqa: S608 - identifiers are allowlisted above.
            (key_value,),
        )
        row = cur.fetchone()
        if row is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "orchestration row does not exist",
                context={"entity": table, "identity": key_value},
            )
        return dict(row)

    @staticmethod
    def _require_row_version(
        row: dict[str, Any],
        expected: int,
        *,
        entity: str,
        identity: str,
    ) -> None:
        actual = int(row["row_version"])
        if actual != expected:
            raise HistoricalRangeContractError(
                REASON_ROW_VERSION_CONFLICT,
                f"{entity} row_version differs from the expected value",
                context={"identity": identity, "expected": expected, "actual": actual},
            )

    @staticmethod
    def _require_running_lease_update(
        *,
        current: dict[str, Any],
        attempt_no: int,
        fencing_token: int | None,
        lease_expires_at: datetime | None,
        entity: str,
    ) -> None:
        if str(current["status"]) != "RUNNING" or lease_expires_at is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                f"same-state {entity} update is allowed only for a RUNNING lease",
            )
        current_attempt = int(current["attempt_no"])
        current_fencing_value = (
            current.get("current_fencing_token") if entity == "day" else current.get("fencing_token")
        )
        current_lease = current.get("lease_expires_at")
        if current_lease is None or current_fencing_value is None or fencing_token is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                f"RUNNING {entity} lease identity is incomplete",
            )
        if (
            lease_expires_at.tzinfo is None
            or lease_expires_at.utcoffset() is None
            or current_lease.tzinfo is None
            or current_lease.utcoffset() is None
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                f"RUNNING {entity} lease timestamps must be timezone-aware",
            )
        lease_expires_at = lease_expires_at.astimezone(UTC)
        current_lease = current_lease.astimezone(UTC)
        current_fencing = int(current_fencing_value)
        if attempt_no == current_attempt and fencing_token == current_fencing:
            if lease_expires_at <= current_lease:
                raise HistoricalRangeContractError(
                    REASON_REPOSITORY_CONFLICT,
                    f"RUNNING {entity} heartbeat must extend its lease",
                )
            return
        now = datetime.now(UTC)
        if current_lease > now or attempt_no != current_attempt + 1 or fencing_token <= current_fencing:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                f"RUNNING {entity} takeover requires an expired lease and higher attempt/fencing token",
                context={
                    "current_attempt_no": current_attempt,
                    "requested_attempt_no": attempt_no,
                    "current_fencing_token": current_fencing,
                    "requested_fencing_token": fencing_token,
                },
            )

    @staticmethod
    def _return_updated(cur: Any, *, entity: str, identity: str) -> dict[str, Any]:
        row = cur.fetchone()
        if row is None:
            raise HistoricalRangeContractError(
                REASON_ROW_VERSION_CONFLICT,
                f"{entity} changed before transition commit",
                context={"identity": identity},
            )
        return dict(row)

    @staticmethod
    def _insert_candidate(
        *,
        cur: Any,
        candidate: HistoricalRangeCandidateFactV1,
        candidate_artifact_ref: HistoricalRangeArtifactRefV1,
    ) -> None:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_candidate (
                candidate_id, day_run_id, symbol, membership_status,
                alpha_raw_rank, alpha_raw_score, hmm_adjusted_rank,
                hmm_adjusted_score, risk_policy_adjusted_rank,
                risk_policy_adjusted_score, selection_effective_rank,
                selection_effective_score, advisory_model_rank,
                advisory_model_score, component_lineage_json,
                component_lineage_hash, artifact_ref, artifact_hash,
                candidate_content_hash
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
            """,
            (
                candidate.candidate_id,
                candidate.day_run_id,
                candidate.symbol,
                candidate.membership_status,
                candidate.alpha_raw_rank,
                candidate.alpha_raw_score,
                candidate.hmm_adjusted_rank,
                candidate.hmm_adjusted_score,
                candidate.risk_policy_adjusted_rank,
                candidate.risk_policy_adjusted_score,
                candidate.selection_effective_rank,
                candidate.selection_effective_score,
                candidate.advisory_model_rank,
                candidate.advisory_model_score,
                psycopg2.extras.Json(candidate.component_lineage_json),
                candidate.component_lineage_hash,
                psycopg2.extras.Json(candidate_artifact_ref.model_dump(mode="json")),
                candidate_artifact_ref.semantic_content_hash,
                candidate.candidate_content_hash,
            ),
        )
        cur.execute(
            """
            SELECT candidate_id, candidate_content_hash, artifact_ref, artifact_hash
            FROM app.advisory_historical_range_candidate
            WHERE day_run_id = %s AND symbol = %s
            """,
            (candidate.day_run_id, candidate.symbol),
        )
        row = cur.fetchone()
        if (
            row is None
            or row["candidate_id"] != candidate.candidate_id
            or row["candidate_content_hash"] != candidate.candidate_content_hash
            or canonicalize(row["artifact_ref"]) != canonicalize(candidate_artifact_ref.model_dump(mode="json"))
            or row["artifact_hash"] != candidate_artifact_ref.semantic_content_hash
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "candidate exact retry payload conflict",
                context={"day_run_id": candidate.day_run_id, "symbol": candidate.symbol},
            )

    @staticmethod
    def _insert_list_version(*, cur: Any, fact: HistoricalRangeListVersionFactV1) -> None:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_list_version (
                list_version_id, day_run_id, range_run_id,
                previous_list_version_id, previous_list_hash,
                previous_day_receipt_hash, target_count, active_count,
                enter_count, hold_count, exit_count, watch_count,
                price_timing_policy, summary_json, list_content_hash
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
            """,
            (
                fact.list_version_id,
                fact.day_run_id,
                fact.range_run_id,
                fact.previous_list_version_id,
                fact.previous_list_hash,
                fact.previous_day_receipt_hash,
                fact.target_count,
                fact.active_count,
                fact.enter_count,
                fact.hold_count,
                fact.exit_count,
                fact.watch_count,
                fact.price_timing_policy,
                psycopg2.extras.Json(fact.summary_json),
                fact.list_content_hash,
            ),
        )
        cur.execute(
            """
            SELECT list_version_id, list_content_hash
            FROM app.advisory_historical_range_list_version
            WHERE day_run_id = %s
            """,
            (fact.day_run_id,),
        )
        row = cur.fetchone()
        if (
            row is None
            or row["list_version_id"] != fact.list_version_id
            or row["list_content_hash"] != fact.list_content_hash
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "list version exact retry payload conflict",
                context={"day_run_id": fact.day_run_id},
            )

    @staticmethod
    def _insert_list_item(*, cur: Any, fact: HistoricalRangeListItemFactV1) -> None:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_list_item (
                list_item_id, list_version_id, symbol, action, rank, score,
                reason_codes_json, episode_id, rule_guidance_json,
                intended_execution_trade_date, intended_execution_basis,
                execution_status, evidence_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                fact.list_item_id,
                fact.list_version_id,
                fact.symbol,
                fact.action.value,
                fact.rank,
                fact.score,
                psycopg2.extras.Json(list(fact.reason_codes)),
                fact.episode_id,
                psycopg2.extras.Json(fact.rule_guidance_json),
                fact.intended_execution_trade_date,
                fact.intended_execution_basis,
                fact.execution_status,
                fact.evidence_hash,
            ),
        )
        cur.execute(
            """
            SELECT list_item_id, evidence_hash
            FROM app.advisory_historical_range_list_item
            WHERE list_version_id = %s AND symbol = %s
            """,
            (fact.list_version_id, fact.symbol),
        )
        row = cur.fetchone()
        if row is None or row["list_item_id"] != fact.list_item_id or row["evidence_hash"] != fact.evidence_hash:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "list item exact retry payload conflict",
                context={"list_version_id": fact.list_version_id, "symbol": fact.symbol},
            )

    @staticmethod
    def _insert_episode(*, cur: Any, fact: HistoricalRangeEpisodeSnapshotFactV1) -> None:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_episode_snapshot (
                episode_snapshot_id, range_run_id, list_version_id, episode_id,
                symbol, decision_trade_date, entry_sequence,
                enter_decision_trade_date, exit_decision_trade_date,
                recommendation_state, action, execution_status, price_quality,
                weak_rank_confirmation_count, mark_json, evidence_hash
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
            """,
            (
                fact.episode_snapshot_id,
                fact.range_run_id,
                fact.list_version_id,
                fact.episode_id,
                fact.symbol,
                fact.decision_trade_date,
                fact.entry_sequence,
                fact.enter_decision_trade_date,
                fact.exit_decision_trade_date,
                fact.recommendation_state,
                fact.action,
                fact.execution_status,
                fact.price_quality,
                fact.weak_rank_confirmation_count,
                psycopg2.extras.Json(fact.mark_json),
                fact.evidence_hash,
            ),
        )
        cur.execute(
            """
            SELECT episode_snapshot_id, evidence_hash
            FROM app.advisory_historical_range_episode_snapshot
            WHERE range_run_id = %s AND episode_id = %s AND decision_trade_date = %s
            """,
            (fact.range_run_id, fact.episode_id, fact.decision_trade_date),
        )
        row = cur.fetchone()
        if (
            row is None
            or row["episode_snapshot_id"] != fact.episode_snapshot_id
            or row["evidence_hash"] != fact.evidence_hash
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "episode snapshot exact retry payload conflict",
                context={"range_run_id": fact.range_run_id, "episode_id": fact.episode_id},
            )

    @staticmethod
    def _canonical_row(row: dict[str, Any]) -> Any:
        return canonicalize(row)

    @staticmethod
    def _canonical_attempt(attempt: HistoricalRangeDayAttemptV1) -> Any:
        return canonicalize(
            {
                "attempt_id": attempt.attempt_id,
                "day_run_id": attempt.day_run_id,
                "attempt_no": attempt.attempt_no,
                "worker_id": attempt.worker_id,
                "lease_token": attempt.lease_token,
                "fencing_token": attempt.fencing_token,
                "status": attempt.status,
                "input_hash": attempt.input_hash,
                "result_hash": attempt.result_hash,
                "candidate_artifact_ref": attempt.candidate_artifact_ref.model_dump(mode="json")
                if attempt.candidate_artifact_ref is not None
                else None,
                "candidate_artifact_hash": attempt.candidate_artifact_ref.semantic_content_hash
                if attempt.candidate_artifact_ref is not None
                else None,
                "attempt_receipt_ref": attempt.attempt_receipt_ref.model_dump(mode="json")
                if attempt.attempt_receipt_ref is not None
                else None,
                "attempt_receipt_hash": attempt.attempt_receipt_ref.semantic_content_hash
                if attempt.attempt_receipt_ref is not None
                else None,
                "reason_codes_json": list(attempt.reason_codes),
                "error_json": attempt.error_json,
                "started_at": attempt.started_at,
                "finished_at": attempt.finished_at,
            }
        )

    @staticmethod
    def _canonical_operation_attempt(attempt: HistoricalRangeOperationAttemptV1) -> Any:
        return canonicalize(
            {
                "attempt_id": attempt.attempt_id,
                "operation_id": attempt.operation_id,
                "attempt_no": attempt.attempt_no,
                "worker_id": attempt.worker_id,
                "lease_token": attempt.lease_token,
                "fencing_token": attempt.fencing_token,
                "status": attempt.status,
                "input_cursor_json": attempt.input_cursor_json,
                "result_cursor_json": attempt.result_cursor_json,
                "input_hash": attempt.input_hash,
                "result_hash": attempt.result_hash,
                "attempt_receipt_ref": attempt.attempt_receipt_ref.model_dump(mode="json")
                if attempt.attempt_receipt_ref is not None
                else None,
                "attempt_receipt_hash": attempt.attempt_receipt_ref.semantic_content_hash
                if attempt.attempt_receipt_ref is not None
                else None,
                "reason_codes_json": list(attempt.reason_codes),
                "error_json": attempt.error_json,
                "started_at": attempt.started_at,
                "finished_at": attempt.finished_at,
            }
        )

    @staticmethod
    def _repository_error(message: str, **context: Any) -> HistoricalRangeContractError:
        return HistoricalRangeContractError(
            REASON_REPOSITORY_CONFLICT,
            message,
            context=context,
        )
