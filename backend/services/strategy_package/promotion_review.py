from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterator, Protocol
from uuid import uuid4

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError

ConnFactory = Callable[[], Iterator[Any]]


class PromotionReviewStatus(str, Enum):
    AUTO_CANDIDATE = "AUTO_CANDIDATE"
    REVIEW_PENDING = "REVIEW_PENDING"
    REVIEW_REJECTED = "REVIEW_REJECTED"
    SOTA_APPROVED = "SOTA_APPROVED"


class PromotionReviewSourceType(str, Enum):
    QE_EXPERIMENT = "qe_experiment"
    QE_EVOLUTION_LOOP = "qe_evolution_loop"


class PromotionReviewRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    review_id: str
    source_type: PromotionReviewSourceType
    source_id: str
    task_id: str | None = None
    loop_id: str | None = None
    experiment_id: str | None = None
    status: PromotionReviewStatus = PromotionReviewStatus.REVIEW_PENDING
    requested_by: str
    review_reason: str | None = None
    source_metrics: dict[str, Any] = Field(default_factory=dict)
    audit_context: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PromotionReviewRepository(Protocol):
    def create_pending_review(
        self,
        *,
        source_type: PromotionReviewSourceType,
        source_id: str,
        task_id: str | None,
        loop_id: str | None,
        experiment_id: str | None,
        requested_by: str,
        review_reason: str | None,
        source_metrics: dict[str, Any] | None,
        audit_context: dict[str, Any] | None,
    ) -> PromotionReviewRecord: ...

    def get_by_source(
        self,
        source_type: PromotionReviewSourceType,
        source_id: str,
    ) -> PromotionReviewRecord | None: ...


class PostgresPromotionReviewRepository:
    """PostgreSQL-backed promotion review repository; it never runs DDL."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def create_pending_review(
        self,
        *,
        source_type: PromotionReviewSourceType,
        source_id: str,
        task_id: str | None,
        loop_id: str | None,
        experiment_id: str | None,
        requested_by: str,
        review_reason: str | None,
        source_metrics: dict[str, Any] | None,
        audit_context: dict[str, Any] | None,
    ) -> PromotionReviewRecord:
        existing = self.get_by_source(source_type, source_id)
        if existing is not None:
            if existing.status == PromotionReviewStatus.REVIEW_PENDING:
                return existing
            raise InvalidStateTransitionError(
                "promotion review already exists for source",
                context={"source_type": source_type.value, "source_id": source_id, "status": existing.status.value},
            )

        review_id = f"pr_{uuid4().hex}"
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.promotion_review (
                        review_id, source_type, source_id, task_id, loop_id, experiment_id,
                        status, requested_by, review_reason, source_metrics_json, audit_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_type, source_id) DO NOTHING
                    RETURNING *
                    """,
                    (
                        review_id,
                        source_type.value,
                        source_id,
                        task_id,
                        loop_id,
                        experiment_id,
                        PromotionReviewStatus.REVIEW_PENDING.value,
                        requested_by,
                        review_reason,
                        psycopg2.extras.Json(source_metrics or {}),
                        psycopg2.extras.Json(audit_context or {}),
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    # Another request created it first; idempotently return pending, fail-fast otherwise.
                    return self.create_pending_review(
                        source_type=source_type,
                        source_id=source_id,
                        task_id=task_id,
                        loop_id=loop_id,
                        experiment_id=experiment_id,
                        requested_by=requested_by,
                        review_reason=review_reason,
                        source_metrics=source_metrics,
                        audit_context=audit_context,
                    )
        return self._record_from_row(dict(row))

    def get_by_source(
        self,
        source_type: PromotionReviewSourceType,
        source_id: str,
    ) -> PromotionReviewRecord | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.promotion_review
                    WHERE source_type = %s AND source_id = %s
                    """,
                    (source_type.value, source_id),
                )
                row = cur.fetchone()
        return self._record_from_row(dict(row)) if row else None

    @staticmethod
    def _record_from_row(row: dict[str, Any]) -> PromotionReviewRecord:
        return PromotionReviewRecord(
            review_id=row["review_id"],
            source_type=PromotionReviewSourceType(row["source_type"]),
            source_id=row["source_id"],
            task_id=row.get("task_id"),
            loop_id=row.get("loop_id"),
            experiment_id=row.get("experiment_id"),
            status=PromotionReviewStatus(row["status"]),
            requested_by=row["requested_by"],
            review_reason=row.get("review_reason"),
            source_metrics=row.get("source_metrics_json") or {},
            audit_context=row.get("audit_json") or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class InMemoryPromotionReviewRepository:
    """Unit-test repository with idempotent pending review semantics."""

    def __init__(self) -> None:
        self.records: dict[tuple[PromotionReviewSourceType, str], PromotionReviewRecord] = {}

    def create_pending_review(
        self,
        *,
        source_type: PromotionReviewSourceType,
        source_id: str,
        task_id: str | None,
        loop_id: str | None,
        experiment_id: str | None,
        requested_by: str,
        review_reason: str | None,
        source_metrics: dict[str, Any] | None,
        audit_context: dict[str, Any] | None,
    ) -> PromotionReviewRecord:
        key = (source_type, source_id)
        existing = self.records.get(key)
        if existing is not None:
            if existing.status == PromotionReviewStatus.REVIEW_PENDING:
                return existing
            raise InvalidStateTransitionError(
                "promotion review already exists for source",
                context={"source_type": source_type.value, "source_id": source_id, "status": existing.status.value},
            )
        now = datetime.now(timezone.utc)
        record = PromotionReviewRecord(
            review_id=f"pr_{uuid4().hex}",
            source_type=source_type,
            source_id=source_id,
            task_id=task_id,
            loop_id=loop_id,
            experiment_id=experiment_id,
            requested_by=requested_by,
            review_reason=review_reason,
            source_metrics=source_metrics or {},
            audit_context=audit_context or {},
            created_at=now,
            updated_at=now,
        )
        self.records[key] = record
        return record

    def get_by_source(
        self,
        source_type: PromotionReviewSourceType,
        source_id: str,
    ) -> PromotionReviewRecord | None:
        return self.records.get((source_type, source_id))


class PromotionReviewService:
    def __init__(self, repository: PromotionReviewRepository | None = None) -> None:
        self.repository = repository or PostgresPromotionReviewRepository()

    def request_loop_review(
        self,
        *,
        task_id: str,
        loop_id: str,
        requested_by: str,
        review_reason: str | None = None,
        source_metrics: dict[str, Any] | None = None,
        experiment_id: str | None = None,
    ) -> PromotionReviewRecord:
        if not task_id.strip() or not loop_id.strip():
            raise StrategyPackageValidationError("task_id and loop_id are required for promotion review")
        if not requested_by.strip():
            raise StrategyPackageValidationError("requested_by is required for promotion review")
        source_id = loop_id if loop_id.startswith(f"{task_id}_") else f"{task_id}_{loop_id}"
        return self.repository.create_pending_review(
            source_type=PromotionReviewSourceType.QE_EVOLUTION_LOOP,
            source_id=source_id,
            task_id=task_id,
            loop_id=source_id,
            experiment_id=experiment_id,
            requested_by=requested_by,
            review_reason=review_reason,
            source_metrics=source_metrics,
            audit_context={
                "created_by_flow": "manual_sota_promotion_phase1",
                "approved_sota": False,
                "paper_enabled": False,
            },
        )

    def request_experiment_review(
        self,
        *,
        experiment_id: str,
        requested_by: str,
        review_reason: str | None = None,
        source_metrics: dict[str, Any] | None = None,
    ) -> PromotionReviewRecord:
        if not experiment_id.strip():
            raise StrategyPackageValidationError("experiment_id is required for promotion review")
        if not requested_by.strip():
            raise StrategyPackageValidationError("requested_by is required for promotion review")
        return self.repository.create_pending_review(
            source_type=PromotionReviewSourceType.QE_EXPERIMENT,
            source_id=experiment_id,
            task_id=None,
            loop_id=None,
            experiment_id=experiment_id,
            requested_by=requested_by,
            review_reason=review_reason,
            source_metrics=source_metrics,
            audit_context={
                "created_by_flow": "manual_sota_promotion_phase1",
                "approved_sota": False,
                "paper_enabled": False,
            },
        )
