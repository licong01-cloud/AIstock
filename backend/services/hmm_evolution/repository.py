"""PostgreSQL repository for the isolated HMM evolution durable state machine."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from typing import Any
from uuid import uuid4

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn

from .errors import (
    BatchNotFoundError,
    CandidateNotFoundError,
    EvaluationNotFoundError,
    IdempotencyConflictError,
    InvalidStateTransitionError,
    SchemaUnavailableError,
    StaleFencingTokenError,
)
from .models import (
    BatchItemStatus,
    BatchStatus,
    CandidateLifecycle,
    CandidatePreview,
    CandidateRecord,
    EvaluationStatus,
    canonical_json_sha256,
)
from .scorer import RECOMMENDATION_VERSION, RecommendationCandidate, score_batch

WRITABLE_RELATIONS = frozenset(
    {
        "hmm_evolution.candidate",
        "hmm_evolution.offline_evaluation",
        "hmm_evolution.batch_test_run",
        "hmm_evolution.batch_test_item",
    }
)

TERMINAL_EVALUATION_STATUSES = frozenset(
    {
        EvaluationStatus.SUCCEEDED.value,
        EvaluationStatus.FAILED.value,
        EvaluationStatus.CANCELLED.value,
        EvaluationStatus.TIMED_OUT.value,
    }
)
TERMINAL_BATCH_STATUSES = frozenset(
    {
        BatchStatus.COMPLETED.value,
        BatchStatus.PARTIAL_FAILED.value,
        BatchStatus.FAILED.value,
        BatchStatus.CANCELLED.value,
        BatchStatus.TIMED_OUT.value,
    }
)
ACTIVE_ITEM_STATUSES = frozenset(
    {
        BatchItemStatus.PENDING.value,
        BatchItemStatus.WAITING_SHARED.value,
        BatchItemStatus.QUEUED.value,
        BatchItemStatus.RUNNING.value,
    }
)


def _json(value: Any) -> Json:
    return Json(value, dumps=lambda payload: json.dumps(payload, ensure_ascii=False, allow_nan=False))


def _candidate_from_row(row: Mapping[str, Any]) -> CandidateRecord:
    return CandidateRecord.model_validate(dict(row))


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _managed_transaction_conn() -> Any:
    """Return the repository's required atomic PostgreSQL transaction context."""

    return get_conn(autocommit=False, manage_transaction=True)


class HMMEvolutionRepository:
    """The only Phase 1 component permitted to write HMM evolution state."""

    def __init__(self, conn_factory: Callable[[], Any] | None = None) -> None:
        self._conn_factory = conn_factory or _managed_transaction_conn

    def register_candidate(
        self,
        preview: CandidatePreview,
        *,
        display_name: str,
        description: str | None,
        created_by: str,
    ) -> tuple[CandidateRecord, bool]:
        """Register content-addressed candidate, appending provenance aliases idempotently."""

        name = str(display_name or "").strip()
        actor = str(created_by or "").strip()
        if not name or not actor:
            raise ValueError("display_name and created_by are required")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    INSERT INTO hmm_evolution.candidate (
                        candidate_id, manifest_hash, display_name, description,
                        source_type, source_ref, artifact_manifest,
                        algorithm_version, lifecycle_status, created_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'research_only', %s)
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    (
                        preview.candidate_id,
                        preview.manifest_hash,
                        name,
                        description,
                        preview.manifest.source_type.value,
                        _json(preview.manifest.source_ref),
                        _json(preview.manifest.model_dump(mode="json")),
                        preview.manifest.algorithm_version,
                        actor,
                    ),
                )
                inserted = cursor.fetchone()
                if inserted is not None:
                    return _candidate_from_row(inserted), True
                cursor.execute(
                    """
                    SELECT * FROM hmm_evolution.candidate
                    WHERE candidate_id = %s OR manifest_hash = %s
                    FOR UPDATE
                    """,
                    (preview.candidate_id, preview.manifest_hash),
                )
                existing = cursor.fetchone()
                if existing is None:  # pragma: no cover - conflict row must be visible.
                    raise SchemaUnavailableError("candidate conflict row was not visible")
                if (
                    existing["candidate_id"] != preview.candidate_id
                    or not self._same_candidate_content(existing, preview)
                ):
                    raise InvalidStateTransitionError(
                        "candidate identity collision",
                        context={"candidate_id": preview.candidate_id},
                    )
                merged_source_ref = self._append_source_alias(
                    dict(existing["source_ref"]),
                    preview.manifest.source_ref,
                )
                if merged_source_ref != dict(existing["source_ref"]):
                    cursor.execute(
                        """
                        UPDATE hmm_evolution.candidate
                        SET source_ref = %s,
                            row_version = row_version + 1,
                            updated_at = clock_timestamp()
                        WHERE candidate_id = %s
                        RETURNING *
                        """,
                        (_json(merged_source_ref), preview.candidate_id),
                    )
                    existing = cursor.fetchone()
                return _candidate_from_row(existing), False

    def get_candidate(self, candidate_id: str) -> CandidateRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM hmm_evolution.candidate WHERE candidate_id = %s",
                    (candidate_id,),
                )
                row = cursor.fetchone()
        if row is None:
            raise CandidateNotFoundError(
                "HMM evolution candidate was not found",
                context={"candidate_id": candidate_id},
            )
        return _candidate_from_row(row)

    def list_candidates(
        self,
        *,
        lifecycle_status: CandidateLifecycle | None = None,
        source_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[CandidateRecord]:
        if not 1 <= limit <= 500 or offset < 0:
            raise ValueError("candidate pagination is out of range")
        clauses: list[str] = []
        params: list[Any] = []
        if lifecycle_status is not None:
            clauses.append("lifecycle_status = %s")
            params.append(lifecycle_status.value)
        if source_type is not None:
            clauses.append("source_type = %s")
            params.append(source_type)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend((limit, offset))
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"""
                    SELECT * FROM hmm_evolution.candidate
                    {where}
                    ORDER BY created_at DESC, candidate_id ASC
                    LIMIT %s OFFSET %s
                    """,  # noqa: S608 - where contains only fixed clauses.
                    tuple(params),
                )
                rows = cursor.fetchall()
        return [_candidate_from_row(row) for row in rows]

    def retire_candidate(
        self,
        candidate_id: str,
        *,
        expected_row_version: int,
    ) -> CandidateRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE hmm_evolution.candidate
                    SET lifecycle_status = 'retired', retired_at = clock_timestamp(),
                        row_version = row_version + 1, updated_at = clock_timestamp()
                    WHERE candidate_id = %s AND row_version = %s
                      AND lifecycle_status = 'research_only'
                    RETURNING *
                    """,
                    (candidate_id, expected_row_version),
                )
                row = cursor.fetchone()
        if row is None:
            raise InvalidStateTransitionError(
                "candidate cannot be retired from its current state or row version",
                context={"candidate_id": candidate_id},
            )
        return _candidate_from_row(row)

    def mark_candidate_invalid(
        self,
        candidate_id: str,
        *,
        expected_row_version: int,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> CandidateRecord:
        if not reason_code.startswith("hmm_evolution_"):
            raise ValueError("candidate invalid reason_code must use the hmm_evolution_ prefix")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE hmm_evolution.candidate
                    SET lifecycle_status = 'invalid', invalid_reason_code = %s,
                        invalid_context = %s, row_version = row_version + 1,
                        updated_at = clock_timestamp()
                    WHERE candidate_id = %s AND row_version = %s
                      AND lifecycle_status <> 'invalid'
                    RETURNING *
                    """,
                    (reason_code, _json(dict(context or {})), candidate_id, expected_row_version),
                )
                row = cursor.fetchone()
        if row is None:
            raise InvalidStateTransitionError(
                "candidate cannot be invalidated from its current state or row version",
                context={"candidate_id": candidate_id},
            )
        return _candidate_from_row(row)

    def create_or_get_evaluation(
        self,
        *,
        candidate_id: str,
        logical_evaluation_key: str,
        base_loop_ref: str,
        source_manifest: Mapping[str, Any],
        source_manifest_hash: str,
        candidate_manifest_hash: str,
        evaluation_spec: Mapping[str, Any],
        evaluation_spec_hash: str,
        evaluator_version: str,
        as_of_date: date,
        window_start: date,
        window_end: date,
        label_horizon_days: int,
        universe_id: str,
        universe_hash: str,
        topk: int,
    ) -> tuple[dict[str, Any], bool]:
        """Return the latest generation for a logical input, creating generation one."""

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                eval_id = _new_id("hmme")
                cursor.execute(
                    """
                    INSERT INTO hmm_evolution.offline_evaluation (
                        eval_id, logical_evaluation_key, run_generation, candidate_id,
                        base_loop_ref, source_manifest, source_manifest_hash,
                        candidate_manifest_hash, evaluation_spec, evaluation_spec_hash,
                        evaluator_version, input_hash, as_of_date, window_start, window_end,
                        label_horizon_days, universe_id, universe_hash, topk
                    ) VALUES (
                        %s, %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (logical_evaluation_key, run_generation) DO NOTHING
                    RETURNING *
                    """,
                    (
                        eval_id,
                        logical_evaluation_key,
                        candidate_id,
                        base_loop_ref,
                        _json(dict(source_manifest)),
                        source_manifest_hash,
                        candidate_manifest_hash,
                        _json(dict(evaluation_spec)),
                        evaluation_spec_hash,
                        evaluator_version,
                        logical_evaluation_key,
                        as_of_date,
                        window_start,
                        window_end,
                        label_horizon_days,
                        universe_id,
                        universe_hash,
                        topk,
                    ),
                )
                row = cursor.fetchone()
                if row is not None:
                    return dict(row), True
                cursor.execute(
                    """
                    SELECT * FROM hmm_evolution.offline_evaluation
                    WHERE logical_evaluation_key = %s
                    ORDER BY run_generation DESC
                    LIMIT 1
                    """,
                    (logical_evaluation_key,),
                )
                existing = cursor.fetchone()
                if existing is None:  # pragma: no cover - conflict row must be visible.
                    raise SchemaUnavailableError("evaluation conflict row was not visible")
                return dict(existing), False

    def create_or_get_batch(
        self,
        *,
        request_hash: str,
        items: Sequence[Mapping[str, Any]],
        recommendation_spec: Mapping[str, Any],
        recommendation_version: str,
        created_by: str,
        idempotency_key: str | None = None,
        retry_of_batch_id: str | None = None,
        retry_generation: int = 1,
    ) -> tuple[dict[str, Any], bool]:
        if not 1 <= len(items) <= 50:
            raise ValueError("a batch must contain 1..50 candidates")
        candidate_ids = [str(item["candidate_id"]) for item in items]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("a batch cannot contain duplicate candidate IDs")
        recommendation_hash = canonical_json_sha256(dict(recommendation_spec))
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                batch_id = _new_id("hmmb")
                queued_count = sum(
                    str(item["item_status"])
                    in {
                        BatchItemStatus.PENDING.value,
                        BatchItemStatus.WAITING_SHARED.value,
                        BatchItemStatus.QUEUED.value,
                    }
                    for item in items
                )
                cursor.execute(
                    """
                    INSERT INTO hmm_evolution.batch_test_run (
                        batch_id, request_hash, idempotency_key, retry_of_batch_id,
                        retry_generation, candidate_count, queued_count,
                        recommendation_spec, recommendation_spec_hash,
                        recommendation_version, created_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    (
                        batch_id,
                        request_hash,
                        idempotency_key,
                        retry_of_batch_id,
                        retry_generation,
                        len(items),
                        queued_count,
                        _json(dict(recommendation_spec)),
                        recommendation_hash,
                        recommendation_version,
                        created_by,
                    ),
                )
                batch = cursor.fetchone()
                if batch is None:
                    return (
                        self._existing_batch_after_conflict(
                            cursor,
                            request_hash=request_hash,
                            idempotency_key=idempotency_key,
                            conflict_message=(
                                "Idempotency-Key was already used for a different request"
                            ),
                        ),
                        False,
                    )
                for ordinal, item in enumerate(items):
                    cursor.execute(
                        """
                        INSERT INTO hmm_evolution.batch_test_item (
                            batch_id, candidate_id, eval_id, ordinal, item_status
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            batch_id,
                            item["candidate_id"],
                            item["eval_id"],
                            int(item.get("ordinal", ordinal)),
                            str(item["item_status"]),
                        ),
                    )
                batch = self._recompute_batch_state_with_cursor(
                    cursor,
                    batch_id,
                    release_lease=True,
                    locked_batch=dict(batch),
                )
                return batch, True

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM hmm_evolution.batch_test_run WHERE batch_id = %s",
                    (batch_id,),
                )
                batch = cursor.fetchone()
                if batch is None:
                    raise BatchNotFoundError(
                        "batch was not found",
                        context={"batch_id": batch_id},
                    )
                cursor.execute(
                    """
                    SELECT item.*,
                           candidate.display_name AS candidate_display_name,
                           candidate.source_type AS candidate_source_type,
                           candidate.lifecycle_status AS candidate_lifecycle_status,
                           evaluation.status AS evaluation_status,
                           evaluation.label_horizon_days,
                           evaluation.as_of_date,
                           evaluation.window_start,
                           evaluation.window_end,
                           evaluation.trading_days_count,
                           evaluation.changed_day_count,
                           evaluation.label_comparable_day_count,
                           evaluation.db_comparable_day_count,
                           evaluation.replacement_count,
                           evaluation.primary_coverage_ratio,
                           evaluation.net_label_return,
                           evaluation.net_db_10d,
                           evaluation.positive_net_label_day_ratio,
                           evaluation.evidence_quality,
                           evaluation.warnings_json,
                           evaluation.error_message AS evaluation_error_message,
                           evaluation.reason_code AS evaluation_reason_code,
                           evaluation.started_at AS evaluation_started_at,
                           evaluation.completed_at AS evaluation_completed_at
                    FROM hmm_evolution.batch_test_item AS item
                    JOIN hmm_evolution.candidate AS candidate
                      ON candidate.candidate_id = item.candidate_id
                    JOIN hmm_evolution.offline_evaluation AS evaluation
                      ON evaluation.eval_id = item.eval_id
                    WHERE item.batch_id = %s ORDER BY item.ordinal ASC
                    """,
                    (batch_id,),
                )
                items = [dict(row) for row in cursor.fetchall()]
        payload = dict(batch)
        payload["items"] = items
        return payload

    def get_evaluation(self, eval_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT evaluation.*,
                           candidate.display_name AS candidate_display_name,
                           candidate.source_type AS candidate_source_type,
                           candidate.lifecycle_status AS candidate_lifecycle_status
                    FROM hmm_evolution.offline_evaluation AS evaluation
                    JOIN hmm_evolution.candidate AS candidate
                      ON candidate.candidate_id = evaluation.candidate_id
                    WHERE evaluation.eval_id = %s
                    """,
                    (eval_id,),
                )
                row = cursor.fetchone()
        if row is None:
            raise EvaluationNotFoundError(
                "evaluation was not found",
                context={"eval_id": eval_id},
            )
        return dict(row)

    def list_evaluations(
        self,
        *,
        candidate_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 500 or offset < 0:
            raise ValueError("evaluation pagination is out of range")
        where = "WHERE evaluation.candidate_id = %s" if candidate_id else ""
        params: list[Any] = [candidate_id] if candidate_id else []
        params.extend((limit, offset))
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"""
                    SELECT evaluation.*,
                           candidate.display_name AS candidate_display_name,
                           candidate.source_type AS candidate_source_type
                    FROM hmm_evolution.offline_evaluation AS evaluation
                    JOIN hmm_evolution.candidate AS candidate
                      ON candidate.candidate_id = evaluation.candidate_id
                    {where}
                    ORDER BY evaluation.created_at DESC, evaluation.eval_id ASC
                    LIMIT %s OFFSET %s
                    """,  # noqa: S608 - where is selected from one fixed clause.
                    tuple(params),
                )
                return [dict(row) for row in cursor.fetchall()]

    def list_batches(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        if not 1 <= limit <= 500 or offset < 0:
            raise ValueError("batch pagination is out of range")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM hmm_evolution.batch_test_run
                    ORDER BY created_at DESC, batch_id ASC LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
                return [dict(row) for row in cursor.fetchall()]

    def claim_batch(self, *, owner_id: str, lease_seconds: int = 90) -> dict[str, Any] | None:
        return self._claim_one(
            relation="batch_test_run",
            id_column="batch_id",
            owner_id=owner_id,
            lease_seconds=lease_seconds,
        )

    def claim_evaluation(
        self,
        *,
        owner_id: str,
        lease_seconds: int = 90,
        batch_id: str | None = None,
    ) -> dict[str, Any] | None:
        return self._claim_one(
            relation="offline_evaluation",
            id_column="eval_id",
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            batch_id=batch_id,
        )

    def heartbeat_batch(
        self,
        *,
        batch_id: str,
        owner_id: str,
        fencing_token: int,
        expected_row_version: int,
        lease_seconds: int = 90,
    ) -> dict[str, Any]:
        return self._heartbeat(
            relation="batch_test_run",
            id_column="batch_id",
            object_id=batch_id,
            owner_id=owner_id,
            fencing_token=fencing_token,
            expected_row_version=expected_row_version,
            lease_seconds=lease_seconds,
            statuses=(BatchStatus.RUNNING.value, BatchStatus.CANCEL_REQUESTED.value),
        )

    def heartbeat_evaluation(
        self,
        *,
        eval_id: str,
        owner_id: str,
        fencing_token: int,
        expected_row_version: int,
        lease_seconds: int = 90,
    ) -> dict[str, Any]:
        return self._heartbeat(
            relation="offline_evaluation",
            id_column="eval_id",
            object_id=eval_id,
            owner_id=owner_id,
            fencing_token=fencing_token,
            expected_row_version=expected_row_version,
            lease_seconds=lease_seconds,
            statuses=(EvaluationStatus.RUNNING.value,),
        )

    def complete_evaluation(
        self,
        *,
        eval_id: str,
        owner_id: str,
        fencing_token: int,
        expected_row_version: int,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        required = {
            "trading_days_count",
            "changed_day_count",
            "label_comparable_day_count",
            "db_comparable_day_count",
            "replacement_count",
            "evidence_quality",
            "warnings_json",
            "metrics_json",
            "result_hash",
        }
        missing = sorted(required - set(result))
        if missing:
            raise ValueError(f"evaluation result is missing required fields: {missing}")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE hmm_evolution.offline_evaluation
                    SET status = 'succeeded', trading_days_count = %s,
                        changed_day_count = %s, label_comparable_day_count = %s,
                        db_comparable_day_count = %s, replacement_count = %s,
                        primary_coverage_ratio = %s, net_label_return = %s,
                        net_db_10d = %s, positive_net_label_day_ratio = %s,
                        evidence_quality = %s, warnings_json = %s, metrics_json = %s,
                        result_hash = %s, owner_id = NULL, lease_expires_at = NULL,
                        completed_at = clock_timestamp(), updated_at = clock_timestamp(),
                        row_version = row_version + 1
                    WHERE eval_id = %s AND status = 'running' AND owner_id = %s
                      AND fencing_token = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        result["trading_days_count"],
                        result["changed_day_count"],
                        result["label_comparable_day_count"],
                        result["db_comparable_day_count"],
                        result["replacement_count"],
                        result.get("primary_coverage_ratio"),
                        result.get("net_label_return"),
                        result.get("net_db_10d"),
                        result.get("positive_net_label_day_ratio"),
                        result["evidence_quality"],
                        _json(result["warnings_json"]),
                        _json(result["metrics_json"]),
                        result["result_hash"],
                        eval_id,
                        owner_id,
                        fencing_token,
                        expected_row_version,
                    ),
                )
                row = cursor.fetchone()
                if row is None:
                    self._raise_stale_fence(eval_id, "evaluation")
                cursor.execute(
                    """
                    UPDATE hmm_evolution.batch_test_item
                    SET item_status = 'succeeded', completed_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE eval_id = %s
                      AND item_status IN ('pending', 'waiting_shared', 'queued', 'running')
                    """,
                    (eval_id,),
                )
                self._recompute_batches_for_evaluation(
                    cursor,
                    eval_id,
                    release_lease=True,
                )
                return dict(row)

    def fail_evaluation(
        self,
        *,
        eval_id: str,
        owner_id: str,
        fencing_token: int,
        expected_row_version: int,
        error_code: str,
        reason_code: str,
        error_message: str,
        error_context: Mapping[str, Any] | None = None,
        terminal_status: EvaluationStatus = EvaluationStatus.FAILED,
    ) -> dict[str, Any]:
        if terminal_status not in {
            EvaluationStatus.FAILED,
            EvaluationStatus.CANCELLED,
            EvaluationStatus.TIMED_OUT,
        }:
            raise ValueError("fail_evaluation requires a non-success terminal status")
        if not reason_code.startswith("hmm_evolution_"):
            raise ValueError("reason_code must use the hmm_evolution_ prefix")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE hmm_evolution.offline_evaluation
                    SET status = %s, error_code = %s, reason_code = %s,
                        error_message = %s, error_context = %s,
                        owner_id = NULL, lease_expires_at = NULL,
                        completed_at = clock_timestamp(), updated_at = clock_timestamp(),
                        row_version = row_version + 1
                    WHERE eval_id = %s AND status = 'running' AND owner_id = %s
                      AND fencing_token = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        terminal_status.value,
                        error_code,
                        reason_code,
                        error_message,
                        _json(dict(error_context or {})),
                        eval_id,
                        owner_id,
                        fencing_token,
                        expected_row_version,
                    ),
                )
                row = cursor.fetchone()
                if row is None:
                    self._raise_stale_fence(eval_id, "evaluation")
                cursor.execute(
                    """
                    UPDATE hmm_evolution.batch_test_item
                    SET item_status = %s, error_code = %s, reason_code = %s,
                        error_context = %s, completed_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE eval_id = %s
                      AND item_status IN ('pending', 'waiting_shared', 'queued', 'running')
                    """,
                    (
                        terminal_status.value,
                        error_code,
                        reason_code,
                        _json(dict(error_context or {})),
                        eval_id,
                    ),
                )
                self._recompute_batches_for_evaluation(
                    cursor,
                    eval_id,
                    release_lease=True,
                )
                return dict(row)

    def request_batch_cancel(self, *, batch_id: str, requested_by: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM hmm_evolution.batch_test_run
                    WHERE batch_id = %s FOR UPDATE
                    """,
                    (batch_id,),
                )
                batch = cursor.fetchone()
                if batch is None:
                    raise BatchNotFoundError(
                        "batch was not found", context={"batch_id": batch_id}
                    )
                if batch["status"] in TERMINAL_BATCH_STATUSES:
                    return dict(batch)
                target_status = (
                    BatchStatus.CANCELLED.value
                    if batch["status"] == BatchStatus.QUEUED.value
                    else BatchStatus.CANCEL_REQUESTED.value
                )
                cursor.execute(
                    """
                    UPDATE hmm_evolution.batch_test_run
                    SET status = %s, cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp()),
                        cancel_requested_by = COALESCE(cancel_requested_by, %s),
                        completed_at = CASE WHEN %s = 'cancelled' THEN clock_timestamp() ELSE completed_at END,
                        updated_at = clock_timestamp(), row_version = row_version + 1
                    WHERE batch_id = %s RETURNING *
                    """,
                    (target_status, requested_by, target_status, batch_id),
                )
                updated = cursor.fetchone()
                cursor.execute(
                    """
                    UPDATE hmm_evolution.batch_test_item
                    SET item_status = 'cancelled', completed_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE batch_id = %s
                      AND item_status IN ('pending', 'waiting_shared', 'queued', 'running')
                    RETURNING eval_id
                    """,
                    (batch_id,),
                )
                affected_eval_ids = {str(row["eval_id"]) for row in cursor.fetchall()}
                for eval_id in affected_eval_ids:
                    cursor.execute(
                        """
                        SELECT 1 FROM hmm_evolution.batch_test_item
                        WHERE eval_id = %s AND batch_id <> %s
                          AND item_status IN ('pending', 'waiting_shared', 'queued', 'running')
                        LIMIT 1
                        """,
                        (eval_id, batch_id),
                    )
                    if cursor.fetchone() is None:
                        cursor.execute(
                            """
                            UPDATE hmm_evolution.offline_evaluation
                            SET cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp()),
                                updated_at = clock_timestamp(), row_version = row_version + 1
                            WHERE eval_id = %s AND status IN ('queued', 'running')
                            """,
                            (eval_id,),
                        )
                if updated is None:  # pragma: no cover
                    raise SchemaUnavailableError("batch cancel update returned no row")
                return dict(updated)

    def recompute_batch_state(self, batch_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                return self._recompute_batch_state_with_cursor(cursor, batch_id)

    def release_batch_after_empty_claim(
        self,
        *,
        batch_id: str,
        owner_id: str,
        fencing_token: int,
        expected_row_version: int,
    ) -> dict[str, Any]:
        """Release an orchestration lease when no local evaluation can be claimed."""

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM hmm_evolution.batch_test_run
                    WHERE batch_id = %s AND status = 'running' AND owner_id = %s
                      AND fencing_token = %s AND row_version = %s
                    FOR UPDATE
                    """,
                    (batch_id, owner_id, fencing_token, expected_row_version),
                )
                batch = cursor.fetchone()
                if batch is None:
                    self._raise_stale_fence(batch_id, "batch")
                return self._recompute_batch_state_with_cursor(
                    cursor,
                    batch_id,
                    release_lease=True,
                    locked_batch=dict(batch),
                )

    def apply_recommendation(
        self,
        *,
        batch_id: str,
        candidate_id: str,
        score: float | None,
        confidence: float | None,
        rank: int | None,
        is_top3: bool,
        components: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE hmm_evolution.batch_test_item
                    SET recommendation_score = %s, evidence_confidence = %s,
                        recommendation_rank = %s, is_top3 = %s,
                        recommendation_components = %s, updated_at = clock_timestamp()
                    WHERE batch_id = %s AND candidate_id = %s
                      AND item_status IN ('succeeded', 'reused')
                    RETURNING *
                    """,
                    (
                        score,
                        confidence,
                        rank,
                        is_top3,
                        _json(dict(components or {})),
                        batch_id,
                        candidate_id,
                    ),
                )
                row = cursor.fetchone()
        if row is None:
            raise InvalidStateTransitionError(
                "recommendation can be applied only to a successful batch item",
                context={"batch_id": batch_id, "candidate_id": candidate_id},
            )
        return dict(row)

    def create_retry_batch(
        self,
        *,
        batch_id: str,
        created_by: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Clone only failed/cancelled/timed-out items into new evaluation generations."""

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM hmm_evolution.batch_test_run
                    WHERE batch_id = %s FOR UPDATE
                    """,
                    (batch_id,),
                )
                original = cursor.fetchone()
                if original is None:
                    raise BatchNotFoundError(
                        "batch was not found",
                        context={"batch_id": batch_id},
                    )
                if original["status"] not in TERMINAL_BATCH_STATUSES:
                    raise InvalidStateTransitionError(
                        "retry requires a terminal source batch",
                        context={"batch_id": batch_id},
                    )
                cursor.execute(
                    """
                    SELECT i.*, e.*
                    FROM hmm_evolution.batch_test_item i
                    JOIN hmm_evolution.offline_evaluation e ON e.eval_id = i.eval_id
                    WHERE i.batch_id = %s
                      AND i.item_status IN ('failed', 'cancelled', 'timed_out')
                    ORDER BY i.ordinal ASC
                    FOR UPDATE OF e
                    """,
                    (batch_id,),
                )
                failed_rows = cursor.fetchall()
                if not failed_rows:
                    raise InvalidStateTransitionError(
                        "source batch has no retryable items",
                        context={"batch_id": batch_id},
                    )
                generation = int(original["retry_generation"]) + 1
                new_batch_id = _new_id("hmmb")
                candidate_ids = [str(row["candidate_id"]) for row in failed_rows]
                request_hash = canonical_json_sha256(
                    {
                        "retry_of_batch_id": batch_id,
                        "retry_generation": generation,
                        "candidate_ids": sorted(candidate_ids),
                    }
                )
                cursor.execute(
                    """
                    INSERT INTO hmm_evolution.batch_test_run (
                        batch_id, request_hash, idempotency_key, retry_of_batch_id,
                        retry_generation, candidate_count, queued_count,
                        recommendation_spec, recommendation_spec_hash,
                        recommendation_version, created_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    (
                        new_batch_id,
                        request_hash,
                        idempotency_key,
                        batch_id,
                        generation,
                        len(failed_rows),
                        len(failed_rows),
                        _json(original["recommendation_spec"]),
                        original["recommendation_spec_hash"],
                        original["recommendation_version"],
                        created_by,
                    ),
                )
                new_batch = cursor.fetchone()
                if new_batch is None:
                    return self._existing_batch_after_conflict(
                        cursor,
                        request_hash=request_hash,
                        idempotency_key=idempotency_key,
                        conflict_message=(
                            "Idempotency-Key was already used for a different retry"
                        ),
                    )
                for ordinal, source in enumerate(failed_rows):
                    new_eval_id = _new_id("hmme")
                    next_eval_generation = int(source["run_generation"]) + 1
                    cursor.execute(
                        """
                        INSERT INTO hmm_evolution.offline_evaluation (
                            eval_id, logical_evaluation_key, run_generation, candidate_id,
                            base_loop_ref, source_manifest, source_manifest_hash,
                            candidate_manifest_hash, evaluation_spec, evaluation_spec_hash,
                            evaluator_version, input_hash, as_of_date, window_start, window_end,
                            label_horizon_days, universe_id, universe_hash, topk
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            new_eval_id,
                            source["logical_evaluation_key"],
                            next_eval_generation,
                            source["candidate_id"],
                            source["base_loop_ref"],
                            _json(source["source_manifest"]),
                            source["source_manifest_hash"],
                            source["candidate_manifest_hash"],
                            _json(source["evaluation_spec"]),
                            source["evaluation_spec_hash"],
                            source["evaluator_version"],
                            source["input_hash"],
                            source["as_of_date"],
                            source["window_start"],
                            source["window_end"],
                            source["label_horizon_days"],
                            source["universe_id"],
                            source["universe_hash"],
                            source["topk"],
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO hmm_evolution.batch_test_item (
                            batch_id, candidate_id, eval_id, ordinal, item_status
                        ) VALUES (%s, %s, %s, %s, 'queued')
                        """,
                        (new_batch_id, source["candidate_id"], new_eval_id, ordinal),
                    )
                if new_batch is None:  # pragma: no cover
                    raise SchemaUnavailableError("retry batch insert returned no row")
                return dict(new_batch)

    def mark_expired_leases_timed_out(self) -> dict[str, int]:
        """Terminalize expired evaluations and derive affected batches from item truth."""

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE hmm_evolution.offline_evaluation
                    SET status = 'timed_out', error_code = 'HMM_EVOLUTION_ERROR',
                        reason_code = 'hmm_evolution_evaluation_timed_out',
                        error_message = 'evaluation lease expired', owner_id = NULL,
                        lease_expires_at = NULL, completed_at = clock_timestamp(),
                        updated_at = clock_timestamp(), row_version = row_version + 1
                    WHERE status = 'running' AND lease_expires_at < clock_timestamp()
                    RETURNING eval_id
                    """
                )
                expired_eval_ids = [str(row["eval_id"]) for row in cursor.fetchall()]
                evaluation_count = len(expired_eval_ids)
                if expired_eval_ids:
                    cursor.execute(
                        """
                        UPDATE hmm_evolution.batch_test_item
                        SET item_status = 'timed_out',
                            error_code = 'HMM_EVOLUTION_ERROR',
                            reason_code = 'hmm_evolution_evaluation_timed_out',
                            completed_at = clock_timestamp(), updated_at = clock_timestamp()
                        WHERE eval_id = ANY(%s)
                          AND item_status IN ('pending', 'waiting_shared', 'queued', 'running')
                        """,
                        (expired_eval_ids,),
                    )
                cursor.execute(
                    """
                    SELECT batch_id FROM hmm_evolution.batch_test_run
                    WHERE status IN ('running', 'cancel_requested')
                      AND lease_expires_at < clock_timestamp()
                    FOR UPDATE SKIP LOCKED
                    """
                )
                expired_batch_ids = [str(row["batch_id"]) for row in cursor.fetchall()]
                affected_batch_ids = set(expired_batch_ids)
                if expired_eval_ids:
                    cursor.execute(
                        """
                        SELECT DISTINCT batch_id
                        FROM hmm_evolution.batch_test_item
                        WHERE eval_id = ANY(%s)
                        """,
                        (expired_eval_ids,),
                    )
                    affected_batch_ids.update(
                        str(row["batch_id"]) for row in cursor.fetchall()
                    )
                for affected_batch_id in sorted(affected_batch_ids):
                    self._recompute_batch_state_with_cursor(
                        cursor,
                        affected_batch_id,
                        release_lease=True,
                    )
                batch_count = len(expired_batch_ids)
        return {"evaluations": evaluation_count, "batches": batch_count}

    def _claim_one(
        self,
        *,
        relation: str,
        id_column: str,
        owner_id: str,
        lease_seconds: int,
        batch_id: str | None = None,
    ) -> dict[str, Any] | None:
        if relation not in {"batch_test_run", "offline_evaluation"}:
            raise ValueError("unsupported claim relation")
        if not owner_id or lease_seconds < 1:
            raise ValueError("owner_id and positive lease_seconds are required")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if relation == "offline_evaluation" and batch_id is not None:
                    cursor.execute(
                        """
                        SELECT e.eval_id
                        FROM hmm_evolution.offline_evaluation e
                        JOIN hmm_evolution.batch_test_item i ON i.eval_id = e.eval_id
                        WHERE e.status = 'queued' AND i.batch_id = %s
                          AND i.item_status IN ('pending', 'waiting_shared', 'queued')
                        ORDER BY e.created_at ASC, e.eval_id ASC
                        FOR UPDATE OF e SKIP LOCKED LIMIT 1
                        """,
                        (batch_id,),
                    )
                elif relation == "batch_test_run":
                    cursor.execute(
                        """
                        SELECT b.batch_id
                        FROM hmm_evolution.batch_test_run b
                        WHERE b.status = 'queued'
                          AND EXISTS (
                              SELECT 1
                              FROM hmm_evolution.batch_test_item i
                              JOIN hmm_evolution.offline_evaluation e
                                ON e.eval_id = i.eval_id
                              WHERE i.batch_id = b.batch_id
                                AND e.status = 'queued'
                                AND i.item_status IN ('pending', 'waiting_shared', 'queued')
                          )
                        ORDER BY b.created_at ASC, b.batch_id ASC
                        FOR UPDATE OF b SKIP LOCKED LIMIT 1
                        """
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT {id_column} FROM hmm_evolution.{relation}
                        WHERE status = 'queued'
                        ORDER BY created_at ASC, {id_column} ASC
                        FOR UPDATE SKIP LOCKED LIMIT 1
                        """  # noqa: S608 - relation and id are allowlisted above.
                    )
                selected = cursor.fetchone()
                if selected is None:
                    return None
                object_id = selected[id_column]
                cursor.execute(
                    f"""
                    UPDATE hmm_evolution.{relation}
                    SET status = 'running', owner_id = %s,
                        fencing_token = fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                        heartbeat_at = clock_timestamp(),
                        started_at = COALESCE(started_at, clock_timestamp()),
                        updated_at = clock_timestamp(), row_version = row_version + 1
                        {", attempt_count = attempt_count + 1" if relation == "offline_evaluation" else ""}
                    WHERE {id_column} = %s AND status = 'queued'
                    RETURNING *
                    """,  # noqa: S608
                    (owner_id, lease_seconds, object_id),
                )
                row = cursor.fetchone()
                if row is None:  # pragma: no cover - locked row cannot race.
                    raise InvalidStateTransitionError("claim lost its locked row")
                if relation == "offline_evaluation":
                    cursor.execute(
                        """
                        UPDATE hmm_evolution.batch_test_item
                        SET item_status = 'running', updated_at = clock_timestamp()
                        WHERE eval_id = %s
                          AND item_status IN ('pending', 'waiting_shared', 'queued')
                        """,
                        (object_id,),
                    )
                return dict(row)

    def _heartbeat(
        self,
        *,
        relation: str,
        id_column: str,
        object_id: str,
        owner_id: str,
        fencing_token: int,
        expected_row_version: int,
        lease_seconds: int,
        statuses: tuple[str, ...],
    ) -> dict[str, Any]:
        if relation not in {"batch_test_run", "offline_evaluation"}:
            raise ValueError("unsupported heartbeat relation")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"""
                    UPDATE hmm_evolution.{relation}
                    SET lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                        heartbeat_at = clock_timestamp(), updated_at = clock_timestamp(),
                        row_version = row_version + 1
                    WHERE {id_column} = %s AND owner_id = %s AND fencing_token = %s
                      AND row_version = %s AND status = ANY(%s)
                    RETURNING *
                    """,  # noqa: S608
                    (
                        lease_seconds,
                        object_id,
                        owner_id,
                        fencing_token,
                        expected_row_version,
                        list(statuses),
                    ),
                )
                row = cursor.fetchone()
        if row is None:
            self._raise_stale_fence(object_id, relation)
        return dict(row)

    @staticmethod
    def _append_source_alias(existing: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
        incoming_dict = dict(incoming)
        primary = {key: value for key, value in existing.items() if key != "aliases"}
        if incoming_dict == primary:
            return existing
        aliases = [dict(item) for item in existing.get("aliases", []) if isinstance(item, Mapping)]
        if incoming_dict not in aliases:
            aliases.append(incoming_dict)
        result = dict(primary)
        result["aliases"] = aliases
        return result

    @staticmethod
    def _same_candidate_content(
        existing: Mapping[str, Any],
        incoming: CandidatePreview,
    ) -> bool:
        stored_manifest = existing.get("artifact_manifest")
        if not isinstance(stored_manifest, Mapping):
            return False
        incoming_manifest = incoming.manifest.model_dump(mode="json")
        identity_fields = (
            "artifact_sha256",
            "artifact_type",
            "detected_format",
            "algorithm_version",
        )
        return all(
            stored_manifest.get(field) == incoming_manifest.get(field)
            for field in identity_fields
        )

    @staticmethod
    def _existing_batch_after_conflict(
        cursor: Any,
        *,
        request_hash: str,
        idempotency_key: str | None,
        conflict_message: str,
    ) -> dict[str, Any]:
        if idempotency_key:
            cursor.execute(
                """
                SELECT * FROM hmm_evolution.batch_test_run
                WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            )
            by_key = cursor.fetchone()
            if by_key is not None:
                if by_key["request_hash"] != request_hash:
                    raise IdempotencyConflictError(
                        conflict_message,
                        context={"idempotency_key": idempotency_key},
                    )
                return dict(by_key)
        cursor.execute(
            """
            SELECT * FROM hmm_evolution.batch_test_run
            WHERE request_hash = %s
            """,
            (request_hash,),
        )
        existing = cursor.fetchone()
        if existing is None:  # pragma: no cover - conflict row must be visible.
            raise SchemaUnavailableError("batch conflict row was not visible")
        return dict(existing)

    def _recompute_batches_for_evaluation(
        self,
        cursor: Any,
        eval_id: str,
        *,
        release_lease: bool,
    ) -> None:
        cursor.execute(
            """
            SELECT DISTINCT batch_id
            FROM hmm_evolution.batch_test_item
            WHERE eval_id = %s
            ORDER BY batch_id
            """,
            (eval_id,),
        )
        for row in cursor.fetchall():
            batch = self._recompute_batch_state_with_cursor(
                cursor,
                str(row["batch_id"]),
                release_lease=release_lease,
            )
            if batch["status"] in TERMINAL_BATCH_STATUSES:
                self._apply_recommendations_with_cursor(cursor, str(row["batch_id"]))

    def _apply_recommendations_with_cursor(self, cursor: Any, batch_id: str) -> None:
        cursor.execute(
            """
            SELECT recommendation_version
            FROM hmm_evolution.batch_test_run
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        batch = cursor.fetchone()
        if batch is None:
            raise InvalidStateTransitionError(
                "batch was not found while applying recommendations",
                context={"batch_id": batch_id},
            )
        if str(batch["recommendation_version"]) != RECOMMENDATION_VERSION:
            raise InvalidStateTransitionError(
                "batch recommendation version is unsupported",
                context={
                    "batch_id": batch_id,
                    "recommendation_version": str(batch["recommendation_version"]),
                },
            )
        cursor.execute(
            """
            SELECT
                i.candidate_id,
                e.net_label_return,
                e.net_db_10d,
                e.positive_net_label_day_ratio,
                e.primary_coverage_ratio
            FROM hmm_evolution.batch_test_item i
            JOIN hmm_evolution.offline_evaluation e ON e.eval_id = i.eval_id
            WHERE i.batch_id = %s
              AND i.item_status IN ('succeeded', 'reused')
              AND e.status = 'succeeded'
            ORDER BY i.ordinal ASC
            """,
            (batch_id,),
        )
        rows = cursor.fetchall()
        if not rows:
            return
        recommendations = score_batch(
            [
                RecommendationCandidate(
                    candidate_id=str(row["candidate_id"]),
                    metrics={
                        "net_label_return": row["net_label_return"],
                        "net_db_10d": row["net_db_10d"],
                        "positive_net_label_day_ratio": row[
                            "positive_net_label_day_ratio"
                        ],
                        "primary_coverage_ratio": row["primary_coverage_ratio"],
                    },
                )
                for row in rows
            ]
        )
        cursor.execute(
            """
            UPDATE hmm_evolution.batch_test_item
            SET recommendation_score = NULL, evidence_confidence = NULL,
                recommendation_rank = NULL, is_top3 = FALSE,
                recommendation_components = NULL, updated_at = clock_timestamp()
            WHERE batch_id = %s AND item_status IN ('succeeded', 'reused')
            """,
            (batch_id,),
        )
        for recommendation in recommendations:
            cursor.execute(
                """
                UPDATE hmm_evolution.batch_test_item
                SET recommendation_score = %s, evidence_confidence = %s,
                    recommendation_rank = %s, is_top3 = %s,
                    recommendation_components = %s, updated_at = clock_timestamp()
                WHERE batch_id = %s AND candidate_id = %s
                  AND item_status IN ('succeeded', 'reused')
                RETURNING candidate_id
                """,
                (
                    recommendation.score,
                    recommendation.confidence,
                    recommendation.rank,
                    recommendation.is_top3,
                    _json(dict(recommendation.components)),
                    batch_id,
                    recommendation.candidate_id,
                ),
            )
            if cursor.fetchone() is None:
                raise InvalidStateTransitionError(
                    "successful batch item disappeared during recommendation persistence",
                    context={
                        "batch_id": batch_id,
                        "candidate_id": recommendation.candidate_id,
                    },
                )

    def _recompute_batch_state_with_cursor(
        self,
        cursor: Any,
        batch_id: str,
        *,
        release_lease: bool = False,
        locked_batch: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        batch = dict(locked_batch) if locked_batch is not None else None
        if batch is None:
            cursor.execute(
                """
                SELECT * FROM hmm_evolution.batch_test_run
                WHERE batch_id = %s FOR UPDATE
                """,
                (batch_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise InvalidStateTransitionError(
                    "batch was not found",
                    context={"batch_id": batch_id},
                )
            batch = dict(row)
        cursor.execute(
            """
            SELECT item_status, COUNT(*) AS count
            FROM hmm_evolution.batch_test_item
            WHERE batch_id = %s GROUP BY item_status
            """,
            (batch_id,),
        )
        counts = {
            str(row["item_status"]): int(row["count"])
            for row in cursor.fetchall()
        }
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM hmm_evolution.batch_test_item i
                JOIN hmm_evolution.offline_evaluation e ON e.eval_id = i.eval_id
                WHERE i.batch_id = %s AND e.status = 'queued'
                  AND i.item_status IN ('pending', 'waiting_shared', 'queued')
            ) AS has_queued_evaluation
            """,
            (batch_id,),
        )
        queued_row = cursor.fetchone()
        has_queued_evaluation = bool(queued_row and queued_row["has_queued_evaluation"])
        if (
            has_queued_evaluation
            and str(batch["status"]) != BatchStatus.CANCEL_REQUESTED.value
        ):
            status = BatchStatus.QUEUED.value
        else:
            status = self._derive_batch_status(str(batch["status"]), counts)
        terminal = status in TERMINAL_BATCH_STATUSES
        clear_lease = terminal or release_lease
        cursor.execute(
            """
            UPDATE hmm_evolution.batch_test_run
            SET status = %s,
                queued_count = %s, running_count = %s, succeeded_count = %s,
                failed_count = %s, cancelled_count = %s, timed_out_count = %s,
                completed_at = CASE WHEN %s THEN COALESCE(completed_at, clock_timestamp()) ELSE NULL END,
                owner_id = CASE WHEN %s THEN NULL ELSE owner_id END,
                lease_expires_at = CASE WHEN %s THEN NULL ELSE lease_expires_at END,
                heartbeat_at = CASE WHEN %s THEN NULL ELSE heartbeat_at END,
                updated_at = clock_timestamp(), row_version = row_version + 1
            WHERE batch_id = %s RETURNING *
            """,
            (
                status,
                sum(
                    counts.get(item, 0)
                    for item in ("pending", "waiting_shared", "queued")
                ),
                counts.get("running", 0),
                counts.get("succeeded", 0) + counts.get("reused", 0),
                counts.get("failed", 0),
                counts.get("cancelled", 0),
                counts.get("timed_out", 0),
                terminal,
                clear_lease,
                clear_lease,
                clear_lease,
                batch_id,
            ),
        )
        updated = cursor.fetchone()
        if updated is None:  # pragma: no cover
            raise SchemaUnavailableError("batch state recompute returned no row")
        return dict(updated)

    @staticmethod
    def _derive_batch_status(current: str, counts: Mapping[str, int]) -> str:
        active = sum(counts.get(item, 0) for item in ACTIVE_ITEM_STATUSES)
        success = counts.get("succeeded", 0) + counts.get("reused", 0)
        failed = counts.get("failed", 0)
        cancelled = counts.get("cancelled", 0)
        timed_out = counts.get("timed_out", 0)
        if active:
            return (
                BatchStatus.CANCEL_REQUESTED.value
                if current == BatchStatus.CANCEL_REQUESTED.value
                else BatchStatus.RUNNING.value
            )
        if failed == cancelled == timed_out == 0:
            return BatchStatus.COMPLETED.value
        if success > 0:
            return BatchStatus.PARTIAL_FAILED.value
        if cancelled > 0 and failed == timed_out == 0:
            return BatchStatus.CANCELLED.value
        if timed_out > 0 and failed == cancelled == 0:
            return BatchStatus.TIMED_OUT.value
        return BatchStatus.FAILED.value

    @staticmethod
    def _raise_stale_fence(object_id: str, object_type: str) -> None:
        raise StaleFencingTokenError(
            "owner, fencing token or row version no longer owns the durable state",
            context={"object_id": object_id, "object_type": object_type},
        )
