"""PostgreSQL repository for the isolated Phase 1R persistence boundary.

The connection factory is mandatory.  This module never reads environment
database settings, never falls back to the global production pool, and never
writes ordinary Selection, Advisory, Paper, simulation, QE, or QMT tables.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Any

import psycopg2.extras

from backend.services.advisory_historical_range.canonical import canonical_json_sha256, canonicalize
from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.source_roles import select_day_source_roles
from backend.services.advisory_historical_range.models import (
    DAY_TRANSITIONS,
    DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION,
    DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2,
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
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeContractError,
    HistoricalRangeDatePlanV1,
    HistoricalRangeHMMBindingSetV1,
    HistoricalRangeDayAttemptV1,
    HistoricalRangeDayAttemptReceiptPayloadV1,
    HistoricalRangeDayReceiptPayloadV2,
    HistoricalRangeDecisionMarkSetV1,
    HistoricalRangeClaimedDayV1,
    HistoricalRangeExecutionBatchV1,
    HistoricalRangeExecutionRunV1,
    HistoricalRangeExecutionOperationV1,
    HistoricalRangeExecutionOperationReceiptV1,
    HistoricalRangeExecutionOperationAttemptReceiptV1,
    HistoricalRangePredecessorStateV1,
    HistoricalRangeDayPlanEntryV1,
    HistoricalRangeDayStatus,
    HistoricalRangeEpisodeSnapshotFactV1,
    HistoricalRangeListItemFactV1,
    HistoricalRangeListVersionFactV1,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationCancelledDayResultV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOperationType,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeProgramStatus,
    HistoricalRangePlanningArtifactBindingsV1,
    HistoricalRangeResolvedRequestArtifactPayloadV1,
    HistoricalRangeRunExecutionReceiptV1,
    HistoricalRangeSuccessfulDayReadbackV1,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceCatalogCheckpointV1,
    HistoricalRangeCatalogPhase,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSummaryFactV1,
    ResolvedHistoricalRangeRequestV1,
    build_candidate_input_hash,
    build_day_input_hash,
    build_day_input_hash_v3,
    build_catalog_member_chain_hash,
    append_catalog_member_chain_hash,
    build_day_receipt_payload,
    build_day_receipt_payload_v2,
    derive_day_run_id,
    derive_list_content_hash,
    derive_prefixed_id,
    require_batch_transition,
    require_sha256,
    require_state_transition,
)


ConnFactory = Callable[[], AbstractContextManager[Any]]


@dataclass(frozen=True)
class CreatedHistoricalRangePlanningBatch:
    batch_id: str
    create_operation_id: str
    catalog_operation_id: str
    idempotent: bool


@dataclass(frozen=True)
class SealedHistoricalRangeBatch:
    batch_id: str
    canonical_batch_id: str
    range_run_ids: tuple[str, ...]
    deduplicated: bool


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


@dataclass(frozen=True)
class HistoricalRangeRunFinalizationFacts:
    run: HistoricalRangeExecutionRunV1
    resolved_request_hash: str
    total_day_count: int
    successful_days: tuple[HistoricalRangeSuccessfulDayReadbackV1, ...]
    blocking_day_run_id: str | None
    blocking_ordinal: int | None
    blocking_trade_date: date | None
    blocking_status: HistoricalRangeDayStatus | None
    blocking_attempt_receipt_ref: HistoricalRangeArtifactRefV1 | None
    unexecuted_day_count: int
    cancelled_from_ordinal: int | None


@dataclass(frozen=True)
class HistoricalRangeCancellationDayContext:
    batch_id: str
    range_run_id: str
    research_program_id: str
    day_run_id: str
    ordinal: int
    row_version: int
    status: HistoricalRangeDayStatus
    attempt_no: int
    worker_id: str | None
    lease_token: str | None
    fencing_token: int | None
    resolved_request_hash: str
    request_ref: HistoricalRangeArtifactRefV1
    previous_list_hash: str | None
    previous_day_receipt_ref: HistoricalRangeArtifactRefV1 | None


@dataclass(frozen=True)
class HistoricalRangeCatalogPlanningState:
    batch: dict[str, Any]
    operation: dict[str, Any]
    plan: HistoricalRangeSourceRequirementPlanV1
    checkpoint_chain: tuple[tuple[HistoricalRangeArtifactRefV1, HistoricalRangeSourceCatalogCheckpointV1], ...]
    discovered_members: dict[str, Any]
    current_phase_members: dict[str, Any]


def _catalog_operation_is_sealable(
    operation: Mapping[str, Any] | None,
    *,
    expected_catalog_generation: int,
    expected_requirement_count: int,
) -> bool:
    if operation is None:
        return False
    catalog_generation = operation.get("catalog_generation")
    resolved_count = operation.get("cumulative_resolved_count")
    unresolved_count = operation.get("cumulative_unresolved_count")
    return (
        str(operation.get("status")) == HistoricalRangeOperationStatus.COMPLETED.value
        and catalog_generation is not None
        and int(catalog_generation) == expected_catalog_generation
        and str(operation.get("catalog_phase") or "") == HistoricalRangeCatalogPhase.VERIFY.value
        and resolved_count is not None
        and int(resolved_count) == expected_requirement_count
        and unresolved_count is not None
        and int(unresolved_count) == 0
        and operation.get("latest_checkpoint_ref") is not None
    )


class PostgresHistoricalRangeRepository:
    """Durable Phase 1R repository with exact-retry conflict detection."""

    def __init__(self, *, conn_factory: ConnFactory, artifact_store: HistoricalRangeArtifactStore) -> None:
        if conn_factory is None:
            raise ValueError("conn_factory is required")
        if artifact_store is None:
            raise ValueError("artifact_store is required")
        self._conn_factory = conn_factory
        self._artifact_store = artifact_store

    def create_planning_batch(
        self,
        *,
        plan: HistoricalRangeSourceRequirementPlanV1,
        artifacts: HistoricalRangePlanningArtifactBindingsV1,
    ) -> CreatedHistoricalRangePlanningBatch:
        self._validate_planning_artifact(plan=plan, artifacts=artifacts)
        create_operation = self._planning_operation_request(
            plan=plan, operation_type=HistoricalRangeOperationType.CREATE
        )
        catalog_operation = self._planning_operation_request(
            plan=plan,
            operation_type=HistoricalRangeOperationType.BUILD_SOURCE_CATALOG,
        )
        request_json = {
            "schema_version": "advisory_historical_range_planning_request_payload_v1",
            "request": plan.request.model_dump(mode="json"),
            "planning_identity_hash": plan.planning_identity_hash,
            "requirement_plan_ref": artifacts.requirement_plan_ref.model_dump(mode="json"),
            "requirement_plan_hash": plan.requirement_plan_hash,
        }
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (plan.request.client_idempotency_key,),
                )
                existing = self._find_planning_batch_by_key(
                    cur=cur,
                    client_idempotency_key=plan.request.client_idempotency_key,
                )
                if existing is not None:
                    self._assert_planning_batch_matches(existing=existing, plan=plan, artifacts=artifacts)
                    return self._planning_batch_result(cur=cur, batch_id=str(existing["batch_id"]), idempotent=True)
                cur.execute(
                    """
                    INSERT INTO app.advisory_historical_range_batch (
                        batch_id, request_id, client_idempotency_key,
                        user_request_semantic_hash, planning_identity_hash,
                        requirement_plan_ref, requirement_plan_hash, requirement_plan_artifact_hash,
                        start_trade_date, end_trade_date, calendar_id, calendar_version,
                        ordered_trade_dates_hash, selection_semantics_version,
                        selection_semantics_hash, list_semantics_version, list_semantics_hash,
                        per_program_input_warmup_ranges_hash,
                        program_count, trade_date_count, planned_day_count,
                        status, catalog_generation, catalog_phase, catalog_cursor_ordinal,
                        catalog_resolved_count, catalog_unresolved_count,
                        catalog_member_chain_hash, row_version,
                        artifact_root_identity_hash, request_payload_json
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        'PLANNING', 1, 'DISCOVER', 1, 0, 0, %s, 1, %s, %s
                    )
                    RETURNING batch_id
                    """,
                    (
                        plan.batch_id,
                        plan.request.request_id,
                        plan.request.client_idempotency_key,
                        plan.request.user_request_semantic_hash,
                        plan.planning_identity_hash,
                        psycopg2.extras.Json(artifacts.requirement_plan_ref.model_dump(mode="json")),
                        plan.requirement_plan_hash,
                        artifacts.requirement_plan_ref.semantic_content_hash,
                        plan.date_plan.start_trade_date,
                        plan.date_plan.end_trade_date,
                        plan.date_plan.calendar_id,
                        plan.date_plan.calendar_version,
                        plan.date_plan.ordered_trade_dates_hash,
                        plan.frozen_programs[0].selection_semantics_version,
                        plan.frozen_programs[0].selection_semantics_hash,
                        plan.frozen_programs[0].list_semantics_version,
                        plan.frozen_programs[0].list_semantics_hash,
                        plan.date_plan.per_program_input_warmup_ranges_hash,
                        len(plan.frozen_programs),
                        len(plan.date_plan.ordered_trade_dates),
                        len(plan.frozen_programs) * len(plan.date_plan.ordered_trade_dates),
                        canonical_json_sha256([]),
                        artifacts.artifact_root_identity_hash,
                        psycopg2.extras.Json(request_json),
                    ),
                )
                if cur.fetchone() is None:
                    raise self._repository_error(
                        "planning batch insert did not return its identity", batch_id=plan.batch_id
                    )
                cur.execute(
                    """
                    INSERT INTO app.advisory_historical_range_request_key (
                        client_idempotency_key, batch_id, request_id,
                        user_request_semantic_hash, planning_identity_hash,
                        requirement_plan_ref, requirement_plan_hash, requirement_plan_artifact_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        plan.request.client_idempotency_key,
                        plan.batch_id,
                        plan.request.request_id,
                        plan.request.user_request_semantic_hash,
                        plan.planning_identity_hash,
                        psycopg2.extras.Json(artifacts.requirement_plan_ref.model_dump(mode="json")),
                        plan.requirement_plan_hash,
                        artifacts.requirement_plan_ref.semantic_content_hash,
                    ),
                )
                self._insert_operation(cur=cur, request=create_operation)
                self._complete_planning_create_operation(
                    cur=cur,
                    request=create_operation,
                    requirement_plan_ref=artifacts.requirement_plan_ref,
                )
                self._insert_operation(cur=cur, request=catalog_operation)
                return CreatedHistoricalRangePlanningBatch(
                    batch_id=plan.batch_id,
                    create_operation_id=create_operation.operation_id,
                    catalog_operation_id=catalog_operation.operation_id,
                    idempotent=False,
                )

    def load_catalog_planning_state(self, *, operation_id: str) -> HistoricalRangeCatalogPlanningState:
        """Load the exact current-generation checkpoint chain for worker resume."""

        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM app.advisory_historical_range_operation WHERE operation_id = %s",
                    (operation_id,),
                )
                operation_row = cur.fetchone()
                if operation_row is None:
                    raise self._repository_error("catalog operation does not exist", operation_id=operation_id)
                operation = dict(operation_row)
                if str(operation["operation_type"]) != HistoricalRangeOperationType.BUILD_SOURCE_CATALOG.value:
                    raise ValueError("load_catalog_planning_state requires BUILD_SOURCE_CATALOG")
                cur.execute(
                    "SELECT * FROM app.advisory_historical_range_batch WHERE batch_id = %s",
                    (operation["batch_id"],),
                )
                batch_row = cur.fetchone()
                if batch_row is None:
                    raise self._repository_error("catalog batch does not exist", operation_id=operation_id)
                batch = dict(batch_row)
            conn.rollback()
        plan_ref = HistoricalRangeArtifactRefV1.model_validate(batch["requirement_plan_ref"])
        plan = HistoricalRangeSourceRequirementPlanV1.model_validate(
            self._artifact_store.load_planning(plan_ref).payload
        )
        chain: list[tuple[HistoricalRangeArtifactRefV1, HistoricalRangeSourceCatalogCheckpointV1]] = []
        current_raw = operation.get("latest_checkpoint_ref")
        visited: set[str] = set()
        while current_raw is not None:
            ref = HistoricalRangeArtifactRefV1.model_validate(current_raw)
            if ref.semantic_content_hash in visited:
                raise self._repository_error("catalog checkpoint chain contains a cycle", operation_id=operation_id)
            visited.add(ref.semantic_content_hash)
            envelope = self._artifact_store.load_planning(ref)
            checkpoint = HistoricalRangeSourceCatalogCheckpointV1.model_validate(envelope.payload)
            if (
                envelope.batch_id != batch["batch_id"]
                or envelope.planning_identity_hash != batch["planning_identity_hash"]
                or checkpoint.catalog_generation != int(operation["catalog_generation"])
                or checkpoint.requirement_plan_hash != plan.requirement_plan_hash
            ):
                raise self._repository_error(
                    "catalog checkpoint chain differs from durable planning identity",
                    operation_id=operation_id,
                )
            chain.append((ref, checkpoint))
            current_raw = (
                checkpoint.previous_checkpoint_ref.model_dump(mode="json")
                if checkpoint.previous_checkpoint_ref is not None
                else None
            )
            if len(chain) > len(plan.requirements) * 2 + 2:
                raise self._repository_error("catalog checkpoint chain is longer than its plan", operation_id=operation_id)
        chain.reverse()
        discovered: dict[str, Any] = {}
        current_phase_members: dict[str, Any] = {}
        current_phase = HistoricalRangeCatalogPhase(str(operation["catalog_phase"]))
        for _ref, checkpoint in chain:
            for delta in checkpoint.member_delta:
                if checkpoint.phase is HistoricalRangeCatalogPhase.DISCOVER:
                    discovered[delta.member.requirement_id] = delta.member
                if checkpoint.phase is current_phase:
                    current_phase_members[delta.member.requirement_id] = delta.member
        return HistoricalRangeCatalogPlanningState(
            batch=batch,
            operation=operation,
            plan=plan,
            checkpoint_chain=tuple(chain),
            discovered_members=discovered,
            current_phase_members=current_phase_members,
        )

    def seal_planning_batch(
        self,
        *,
        batch_id: str,
        expected_row_version: int,
        plan: HistoricalRangeSourceRequirementPlanV1,
        resolved: ResolvedHistoricalRangeRequestV1,
        catalog: HistoricalRangeSourceRevisionCatalogV1,
        artifacts: HistoricalRangeArtifactBindingsV1,
    ) -> SealedHistoricalRangeBatch:
        if batch_id != plan.batch_id or batch_id != resolved.batch_id:
            raise ValueError("planning and resolved batch identities differ")
        if resolved.request != plan.request or resolved.date_plan != plan.date_plan:
            raise ValueError("resolved request/date plan differs from the frozen requirement plan")
        resolved_base_programs = tuple(item.without_resolved_hmm_binding() for item in resolved.frozen_programs)
        if resolved_base_programs != plan.frozen_programs:
            raise ValueError("resolved Program base semantics differ from the frozen requirement plan")
        if catalog.requirement_plan_hash != plan.requirement_plan_hash:
            raise ValueError("catalog requirement plan hash differs from planning request")
        self._validate_creation_artifacts(resolved=resolved, catalog=catalog, artifacts=artifacts)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                batch = self._lock_row(
                    cur,
                    table="advisory_historical_range_batch",
                    key_name="batch_id",
                    key_value=batch_id,
                )
                self._assert_planning_batch_matches(existing=batch, plan=plan, artifacts=None)
                if str(batch["status"]) == HistoricalRangeBatchStatus.DEDUPLICATED.value or batch.get(
                    "sealed_at"
                ) is not None:
                    return self._exact_sealed_batch_result(
                        cur=cur,
                        batch=batch,
                        resolved=resolved,
                        catalog=catalog,
                        artifacts=artifacts,
                    )
                self._require_row_version(batch, expected_row_version, entity="batch", identity=batch_id)
                if str(batch["status"]) != HistoricalRangeBatchStatus.PLANNING.value:
                    raise self._repository_error(
                        "request seal requires PLANNING batch state",
                        batch_id=batch_id,
                        status=batch["status"],
                    )
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 1))",
                    (resolved.request_payload_sha256,),
                )
                cur.execute(
                    """
                    SELECT *
                    FROM app.advisory_historical_range_operation
                    WHERE batch_id = %s AND operation_type = 'BUILD_SOURCE_CATALOG'
                    FOR UPDATE
                    """,
                    (batch_id,),
                )
                catalog_operation = cur.fetchone()
                expected_requirement_count = len(plan.requirements)
                if not _catalog_operation_is_sealable(
                    catalog_operation,
                    expected_catalog_generation=catalog.catalog_generation,
                    expected_requirement_count=expected_requirement_count,
                ):
                    raise self._repository_error(
                        "source catalog operation is not complete and sealable",
                        batch_id=batch_id,
                    )
                checkpoint_ref = HistoricalRangeArtifactRefV1.model_validate(catalog_operation["latest_checkpoint_ref"])
                checkpoint_envelope = self._artifact_store.load_planning(checkpoint_ref)
                checkpoint = HistoricalRangeSourceCatalogCheckpointV1.model_validate(checkpoint_envelope.payload)
                expected_chain_hash = build_catalog_member_chain_hash(
                    members=catalog.members,
                    ordered_requirement_ids=tuple(item.requirement_id for item in plan.requirements),
                )
                if (
                    checkpoint.catalog_generation != catalog.catalog_generation
                    or checkpoint.phase.value != "VERIFY"
                    or checkpoint.next_requirement_ordinal != expected_requirement_count + 1
                    or checkpoint.cumulative_resolved_count != expected_requirement_count
                    or checkpoint.cumulative_member_chain_hash != expected_chain_hash
                    or str(catalog_operation["cumulative_member_chain_hash"]) != expected_chain_hash
                ):
                    raise self._repository_error(
                        "source catalog checkpoint chain does not close the sealed catalog",
                        batch_id=batch_id,
                    )
                cur.execute(
                    """
                    SELECT batch_id
                    FROM app.advisory_historical_range_batch
                    WHERE request_payload_sha256 = %s
                      AND batch_id <> %s
                      AND status <> 'DEDUPLICATED'
                    FOR UPDATE
                    """,
                    (resolved.request_payload_sha256, batch_id),
                )
                canonical = cur.fetchone()
                if canonical is not None:
                    canonical_batch_id = str(canonical["batch_id"])
                    dedup_payload = {
                        "schema_version": "advisory_historical_range_dedup_receipt_v1",
                        "batch_id": batch_id,
                        "canonical_batch_id": canonical_batch_id,
                        "request_payload_sha256": resolved.request_payload_sha256,
                        "requirement_plan_hash": plan.requirement_plan_hash,
                    }
                    dedup = self._artifact_store.publish_payload(
                        artifact_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
                        producer_contract_version="phase1r_r2b",
                        payload_schema_version="advisory_historical_range_dedup_receipt_v1",
                        resolved_request_hash=str(resolved.request_payload_sha256),
                        payload=dedup_payload,
                        upstream_refs=(artifacts.request_ref,),
                    )
                    cur.execute(
                        """
                        UPDATE app.advisory_historical_range_batch
                        SET status = 'DEDUPLICATED', row_version = row_version + 1,
                            canonical_batch_id = %s,
                            deduplicated_request_payload_sha256 = %s,
                            dedup_receipt_ref = %s, dedup_receipt_hash = %s,
                            started_at = COALESCE(started_at, %s), finished_at = %s
                        WHERE batch_id = %s AND row_version = %s
                        """,
                        (
                            canonical_batch_id,
                            resolved.request_payload_sha256,
                            psycopg2.extras.Json(dedup.ref.model_dump(mode="json")),
                            dedup.ref.semantic_content_hash,
                            datetime.now(UTC),
                            datetime.now(UTC),
                            batch_id,
                            expected_row_version,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise HistoricalRangeContractError(
                            REASON_ROW_VERSION_CONFLICT,
                            "planning batch changed before deduplication committed",
                            context={"batch_id": batch_id},
                        )
                    return SealedHistoricalRangeBatch(
                        batch_id=batch_id,
                        canonical_batch_id=canonical_batch_id,
                        range_run_ids=(),
                        deduplicated=True,
                    )
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 2))",
                    (resolved.request.user_request_semantic_hash,),
                )
                cur.execute(
                    """
                    SELECT predecessor.batch_id
                    FROM app.advisory_historical_range_batch AS predecessor
                    LEFT JOIN app.advisory_historical_range_batch AS successor
                      ON successor.supersedes_batch_id = predecessor.batch_id
                    WHERE predecessor.user_request_semantic_hash = %s
                      AND predecessor.batch_id <> %s
                      AND predecessor.request_payload_sha256 IS NOT NULL
                      AND predecessor.request_payload_sha256 <> %s
                      AND predecessor.status <> 'DEDUPLICATED'
                      AND successor.batch_id IS NULL
                    ORDER BY predecessor.sealed_at DESC, predecessor.batch_id DESC
                    LIMIT 1
                    FOR UPDATE OF predecessor
                    """,
                    (
                        resolved.request.user_request_semantic_hash,
                        batch_id,
                        resolved.request_payload_sha256,
                    ),
                )
                predecessor = cur.fetchone()
                supersedes_batch_id = str(predecessor["batch_id"]) if predecessor is not None else None
                request_json = dict(batch["request_payload_json"])
                request_json["resolved_request"] = resolved.model_dump(mode="json")
                request_json["artifact_refs"] = {
                    "request": artifacts.request_ref.model_dump(mode="json"),
                    "date_plan": artifacts.date_plan_ref.model_dump(mode="json"),
                    "frozen_programs": {
                        key: value.model_dump(mode="json")
                        for key, value in sorted(artifacts.frozen_program_refs.items())
                    },
                }
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_batch
                    SET request_payload_sha256 = %s,
                        request_artifact_ref = %s, request_artifact_hash = %s,
                        date_plan_ref = %s, date_plan_hash = %s,
                        source_revision_catalog_ref = %s, source_revision_catalog_hash = %s,
                        supersedes_batch_id = %s,
                        status = 'QUEUED', row_version = row_version + 1,
                        request_payload_json = %s, sealed_at = %s
                    WHERE batch_id = %s AND row_version = %s
                    """,
                    (
                        resolved.request_payload_sha256,
                        psycopg2.extras.Json(artifacts.request_ref.model_dump(mode="json")),
                        artifacts.request_ref.semantic_content_hash,
                        psycopg2.extras.Json(artifacts.date_plan_ref.model_dump(mode="json")),
                        artifacts.date_plan_ref.semantic_content_hash,
                        psycopg2.extras.Json(artifacts.request_ref.model_dump(mode="json")),
                        catalog.catalog_hash,
                        supersedes_batch_id,
                        psycopg2.extras.Json(request_json),
                        datetime.now(UTC),
                        batch_id,
                        expected_row_version,
                    ),
                )
                if cur.rowcount != 1:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "planning batch changed before request seal committed",
                        context={"batch_id": batch_id},
                    )
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_request_key
                    SET request_payload_sha256 = %s,
                        request_artifact_ref = %s,
                        request_artifact_hash = %s
                    WHERE client_idempotency_key = %s AND batch_id = %s
                    """,
                    (
                        resolved.request_payload_sha256,
                        psycopg2.extras.Json(artifacts.request_ref.model_dump(mode="json")),
                        artifacts.request_ref.semantic_content_hash,
                        resolved.request.client_idempotency_key,
                        batch_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise self._repository_error("planning request key was not sealed", batch_id=batch_id)
                range_run_ids = self._insert_sealed_program_runs(
                    cur=cur,
                    resolved=resolved,
                    artifacts=artifacts,
                )
                self._sync_batch_aggregate(cur=cur, batch_id=batch_id)
                return SealedHistoricalRangeBatch(
                    batch_id=batch_id,
                    canonical_batch_id=batch_id,
                    range_run_ids=range_run_ids,
                    deduplicated=False,
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

    def load_execution_operation(self, *, operation_id: str) -> HistoricalRangeExecutionOperationV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT operation.*, batch.request_payload_sha256 AS batch_resolved_request_hash,
                           (operation.lease_expires_at IS NOT NULL
                            AND operation.lease_expires_at <= clock_timestamp()) AS lease_expired
                    FROM app.advisory_historical_range_operation AS operation
                    JOIN app.advisory_historical_range_batch AS batch ON batch.batch_id = operation.batch_id
                    WHERE operation.operation_id = %s
                    """,
                    (operation_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise self._repository_error("execution operation does not exist", operation_id=operation_id)
        return self._execution_operation_from_row(dict(row))

    def list_operation_attempt_receipt_refs(
        self,
        *,
        operation_id: str,
    ) -> tuple[HistoricalRangeArtifactRefV1, ...]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT attempt_receipt_ref
                    FROM app.advisory_historical_range_operation_attempt
                    WHERE operation_id = %s AND attempt_receipt_ref IS NOT NULL
                    ORDER BY attempt_no
                    """,
                    (operation_id,),
                )
                return tuple(
                    HistoricalRangeArtifactRefV1.model_validate(row["attempt_receipt_ref"])
                    for row in cur.fetchall()
                )

    def claim_execution_operation(
        self,
        *,
        operation_id: str,
        expected_row_version: int,
        worker_id: str,
        lease_token: str,
        lease_seconds: int,
        expired_attempt: HistoricalRangeOperationAttemptV1 | None = None,
    ) -> HistoricalRangeExecutionOperationV1:
        if not 1 <= lease_seconds <= 86_400:
            raise ValueError("operation lease_seconds must be between 1 and 86400")
        worker_id = str(worker_id or "").strip()
        lease_token = str(lease_token or "").strip()
        if not worker_id or not lease_token:
            raise ValueError("execution operation claim requires worker_id and lease_token")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                operation = self._lock_row(
                    cur,
                    table="advisory_historical_range_operation",
                    key_name="operation_id",
                    key_value=operation_id,
                )
                self._require_row_version(operation, expected_row_version, entity="operation", identity=operation_id)
                if str(operation["operation_type"]) not in {
                    HistoricalRangeOperationType.RESUME.value,
                    HistoricalRangeOperationType.CANCEL.value,
                }:
                    raise ValueError("claim_execution_operation accepts only RESUME or CANCEL")
                status = HistoricalRangeOperationStatus(str(operation["status"]))
                if status is HistoricalRangeOperationStatus.RUNNING:
                    cur.execute("SELECT clock_timestamp() AS now")
                    db_now = cur.fetchone()["now"]
                    if operation["lease_expires_at"] is None or operation["lease_expires_at"] > db_now:
                        raise HistoricalRangeContractError(
                            REASON_ROW_VERSION_CONFLICT,
                            "execution operation is already owned by an unexpired worker",
                            context={"operation_id": operation_id},
                        )
                    self._require_expired_operation_attempt(current=operation, attempt=expired_attempt)
                    self._validate_execution_operation_attempt_receipt(
                        operation=operation,
                        attempt=expired_attempt,
                    )
                    self._insert_operation_attempt(cur=cur, attempt=expired_attempt)
                elif status not in {
                    HistoricalRangeOperationStatus.QUEUED,
                    HistoricalRangeOperationStatus.RETRYABLE_FAILED,
                    HistoricalRangeOperationStatus.WAITING_INPUT,
                }:
                    raise self._repository_error(
                        "execution operation is not claimable",
                        operation_id=operation_id,
                        status=status.value,
                    )
                elif expired_attempt is not None:
                    raise ValueError("expired_attempt is accepted only for a RUNNING takeover")
                next_attempt = int(operation["attempt_no"]) + 1
                next_fencing = int(operation["fencing_token"] or 0) + 1
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_operation
                    SET status = 'RUNNING', row_version = row_version + 1,
                        attempt_no = %s, worker_id = %s, lease_token = %s,
                        lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                        fencing_token = %s, result_row_version = NULL,
                        result_status = NULL, result_ref = NULL, result_hash = NULL,
                        error_json = NULL, started_at = COALESCE(started_at, clock_timestamp()),
                        finished_at = NULL
                    WHERE operation_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        next_attempt,
                        worker_id,
                        lease_token,
                        lease_seconds,
                        next_fencing,
                        operation_id,
                        expected_row_version,
                    ),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "execution operation changed while being claimed",
                        context={"operation_id": operation_id},
                    )
        return self.load_execution_operation(operation_id=operation_id)

    def finish_execution_operation(
        self,
        *,
        claimed_operation: HistoricalRangeExecutionOperationV1,
        receipt: HistoricalRangeExecutionOperationReceiptV1,
        receipt_ref: HistoricalRangeArtifactRefV1,
        attempt: HistoricalRangeOperationAttemptV1,
    ) -> HistoricalRangeExecutionOperationV1:
        if (
            receipt.operation_id != claimed_operation.operation_id
            or receipt.operation_type != claimed_operation.operation_type
            or receipt.operation_idempotency_key != claimed_operation.operation_idempotency_key
            or receipt.idempotency_payload_hash != claimed_operation.idempotency_payload_hash
            or receipt.starting_batch_row_version != claimed_operation.expected_row_version
            or receipt.attempt_no != claimed_operation.attempt_no
            or receipt.fencing_token != claimed_operation.fencing_token
            or attempt.operation_id != claimed_operation.operation_id
            or attempt.attempt_no != claimed_operation.attempt_no
            or attempt.fencing_token != claimed_operation.fencing_token
            or attempt.worker_id != claimed_operation.worker_id
            or attempt.lease_token != claimed_operation.lease_token
            or attempt.attempt_receipt_ref != receipt_ref
            or attempt.status != HistoricalRangeOperationStatus.COMPLETED.value
            or attempt.input_hash != claimed_operation.idempotency_payload_hash
            or attempt.result_hash != receipt_ref.semantic_content_hash
            or canonicalize(attempt.result_cursor_json) != canonicalize(receipt.stable_cursor)
            or attempt.reason_codes
            or attempt.error_json is not None
        ):
            raise ValueError("terminal execution operation receipt/attempt differs from its durable claim")
        batch = self.load_execution_batch(batch_id=claimed_operation.batch_id)
        runs = self.list_all_execution_runs(batch_id=claimed_operation.batch_id)
        actual_program_results = tuple(
            (
                run.range_run_id,
                run.research_program_id,
                run.status.value,
                run.row_version,
                run.final_receipt_ref,
            )
            for run in sorted(runs, key=lambda item: item.research_program_id)
        )
        receipt_program_results = tuple(
            (
                item.range_run_id,
                item.research_program_id,
                item.status.value,
                item.row_version,
                item.final_receipt_ref,
            )
            for item in receipt.program_results
        )
        if (
            batch.row_version != receipt.ending_batch_row_version
            or batch.status is not receipt.result_status
            or actual_program_results != receipt_program_results
            or self.load_cancelled_day_results(batch_id=claimed_operation.batch_id)
            != receipt.cancelled_day_results
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "execution operation receipt differs from current batch/Program results",
                context={"operation_id": claimed_operation.operation_id},
            )
        envelope = self._load_artifact(
            receipt_ref,
            expected_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
            resolved_request_hash=claimed_operation.resolved_request_hash,
            expected_payload=receipt.model_dump(mode="json"),
        )
        upstream = tuple(
            item.final_receipt_ref
            for item in receipt.program_results
            if item.final_receipt_ref is not None
        ) + tuple(
            item.attempt_receipt_ref for item in receipt.cancelled_day_results
        ) + receipt.prior_nonterminal_attempt_receipt_refs
        expected_upstream = tuple(
            sorted(upstream, key=lambda ref: (ref.artifact_kind.value, ref.semantic_content_hash, ref.relative_path))
        )
        if tuple(envelope.upstream_refs) != expected_upstream:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "execution operation receipt upstream set differs from its typed payload",
                context={"operation_id": claimed_operation.operation_id},
            )
        self.transition_operation(
            operation_id=claimed_operation.operation_id,
            expected_row_version=claimed_operation.row_version,
            target_status=HistoricalRangeOperationStatus.COMPLETED,
            attempt_no=claimed_operation.attempt_no,
            worker_id=None,
            lease_token=None,
            lease_expires_at=None,
            fencing_token=claimed_operation.fencing_token,
            stable_keyset_cursor_json=receipt.stable_cursor,
            result_row_version=receipt.ending_batch_row_version,
            result_status=receipt.result_status.value,
            result_ref=receipt_ref,
            attempt=attempt,
        )
        return self.load_execution_operation(operation_id=claimed_operation.operation_id)

    def finish_execution_operation_failure(
        self,
        *,
        claimed_operation: HistoricalRangeExecutionOperationV1,
        attempt: HistoricalRangeOperationAttemptV1,
    ) -> HistoricalRangeExecutionOperationV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM app.advisory_historical_range_operation WHERE operation_id = %s",
                    (claimed_operation.operation_id,),
                )
                current = cur.fetchone()
        if current is None:
            raise self._repository_error(
                "execution operation does not exist",
                operation_id=claimed_operation.operation_id,
            )
        self._validate_execution_operation_attempt_receipt(
            operation=dict(current),
            attempt=attempt,
        )
        self.transition_operation(
            operation_id=claimed_operation.operation_id,
            expected_row_version=claimed_operation.row_version,
            target_status=HistoricalRangeOperationStatus.RETRYABLE_FAILED,
            attempt_no=claimed_operation.attempt_no,
            worker_id=None,
            lease_token=None,
            lease_expires_at=None,
            fencing_token=claimed_operation.fencing_token,
            stable_keyset_cursor_json=claimed_operation.stable_keyset_cursor_json,
            error_json=attempt.error_json,
            attempt=attempt,
        )
        return self.load_execution_operation(operation_id=claimed_operation.operation_id)

    def heartbeat_execution_operation(
        self,
        *,
        claimed_operation: HistoricalRangeExecutionOperationV1,
        lease_seconds: int,
    ) -> HistoricalRangeExecutionOperationV1:
        if not 1 <= lease_seconds <= 86_400:
            raise ValueError("operation lease_seconds must be between 1 and 86400")
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_operation
                    SET row_version = row_version + 1,
                        lease_expires_at = clock_timestamp() + make_interval(secs => %s)
                    WHERE operation_id = %s AND status = 'RUNNING'
                      AND row_version = %s AND attempt_no = %s
                      AND worker_id = %s AND lease_token = %s AND fencing_token = %s
                    """,
                    (
                        lease_seconds,
                        claimed_operation.operation_id,
                        claimed_operation.row_version,
                        claimed_operation.attempt_no,
                        claimed_operation.worker_id,
                        claimed_operation.lease_token,
                        claimed_operation.fencing_token,
                    ),
                )
                if cur.rowcount != 1:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "execution operation heartbeat lost durable ownership",
                        context={"operation_id": claimed_operation.operation_id},
                    )
        return self.load_execution_operation(operation_id=claimed_operation.operation_id)

    def _validate_execution_operation_attempt_receipt(
        self,
        *,
        operation: Mapping[str, Any],
        attempt: HistoricalRangeOperationAttemptV1,
    ) -> None:
        if attempt.attempt_receipt_ref is None:
            raise ValueError("expired execution operation attempt requires a receipt ref")
        envelope = self._load_artifact(
            attempt.attempt_receipt_ref,
            expected_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
            resolved_request_hash=self._get_operation_artifact_identity(str(operation["operation_id"])),
        )
        payload = HistoricalRangeExecutionOperationAttemptReceiptV1.model_validate(envelope.payload)
        if (
            payload.operation_id != operation["operation_id"]
            or payload.operation_type != operation["operation_type"]
            or payload.attempt_no != int(operation["attempt_no"])
            or payload.fencing_token != int(operation["fencing_token"])
            or payload.worker_id != operation["worker_id"]
            or payload.lease_token_hash != sha256(str(operation["lease_token"]).encode("utf-8")).hexdigest()
            or payload.status != attempt.status
            or payload.input_hash != attempt.input_hash
            or payload.starting_batch_row_version != int(operation["expected_row_version"])
            or canonicalize(payload.stable_cursor)
            != canonicalize(attempt.result_cursor_json or attempt.input_cursor_json or {})
            or payload.reason_codes != attempt.reason_codes
            or canonicalize(payload.sanitized_error) != canonicalize(attempt.error_json)
            or (
                payload.lease_expired_at is not None
                and payload.lease_expired_at != operation["lease_expires_at"]
            )
            or attempt.result_hash != attempt.attempt_receipt_ref.semantic_content_hash
        ):
            raise ValueError("execution operation attempt receipt differs from durable ownership")
        self._require_exact_upstream_refs(envelope=envelope, expected_refs=())

    def claim_catalog_operation(
        self,
        *,
        operation_id: str,
        expected_row_version: int,
        worker_id: str,
        lease_token: str,
        lease_expires_at: datetime,
        expired_attempt: HistoricalRangeOperationAttemptV1 | None = None,
    ) -> dict[str, Any]:
        worker_id = str(worker_id or "").strip()
        lease_token = str(lease_token or "").strip()
        if not worker_id or not lease_token:
            raise ValueError("catalog claim requires worker_id and lease_token")
        if lease_expires_at.tzinfo is None or lease_expires_at <= datetime.now(UTC):
            raise ValueError("catalog lease_expires_at must be a future timezone-aware timestamp")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                operation = self._lock_row(
                    cur,
                    table="advisory_historical_range_operation",
                    key_name="operation_id",
                    key_value=operation_id,
                )
                self._require_row_version(operation, expected_row_version, entity="operation", identity=operation_id)
                if str(operation["operation_type"]) != HistoricalRangeOperationType.BUILD_SOURCE_CATALOG.value:
                    raise ValueError("claim_catalog_operation requires BUILD_SOURCE_CATALOG")
                status = HistoricalRangeOperationStatus(str(operation["status"]))
                if status not in {
                    HistoricalRangeOperationStatus.QUEUED,
                    HistoricalRangeOperationStatus.WAITING_INPUT,
                    HistoricalRangeOperationStatus.RETRYABLE_FAILED,
                    HistoricalRangeOperationStatus.RUNNING,
                }:
                    raise self._repository_error(
                        "catalog operation is not claimable",
                        operation_id=operation_id,
                        status=status.value,
                    )
                batch = self._lock_row(
                    cur,
                    table="advisory_historical_range_batch",
                    key_name="batch_id",
                    key_value=str(operation["batch_id"]),
                )
                if status is HistoricalRangeOperationStatus.RUNNING:
                    current_lease = operation.get("lease_expires_at")
                    if current_lease is None or current_lease > datetime.now(UTC):
                        raise self._repository_error(
                            "catalog operation takeover requires an expired lease",
                            operation_id=operation_id,
                        )
                    self._require_expired_operation_attempt(current=operation, attempt=expired_attempt)
                    self._validate_planning_operation_attempt(
                        batch=batch,
                        operation=operation,
                        attempt=expired_attempt,
                    )
                    self._insert_operation_attempt(cur=cur, attempt=expired_attempt)
                    if str(batch["status"]) != HistoricalRangeBatchStatus.PLANNING.value:
                        raise self._repository_error(
                            "expired catalog operation requires PLANNING batch state",
                            operation_id=operation_id,
                            batch_status=batch["status"],
                        )
                elif expired_attempt is not None:
                    raise ValueError("expired_attempt is accepted only for a RUNNING catalog takeover")
                if status is HistoricalRangeOperationStatus.WAITING_INPUT:
                    if (
                        str(batch["status"]) != HistoricalRangeBatchStatus.WAITING_INPUT.value
                        or str(batch["waiting_stage"] or "") != "CATALOG"
                    ):
                        raise self._repository_error(
                            "waiting catalog operation does not match batch waiting state",
                            operation_id=operation_id,
                        )
                    cur.execute(
                        """
                        UPDATE app.advisory_historical_range_batch
                        SET status = 'PLANNING', waiting_stage = NULL,
                            row_version = row_version + 1
                        WHERE batch_id = %s AND row_version = %s
                        """,
                        (batch["batch_id"], batch["row_version"]),
                    )
                    if cur.rowcount != 1:
                        raise HistoricalRangeContractError(
                            REASON_ROW_VERSION_CONFLICT,
                            "batch changed while resuming its catalog operation",
                            context={"batch_id": batch["batch_id"]},
                        )
                elif str(batch["status"]) != HistoricalRangeBatchStatus.PLANNING.value:
                    raise self._repository_error(
                        "catalog operation requires PLANNING batch state",
                        operation_id=operation_id,
                        batch_status=batch["status"],
                    )
                next_attempt = int(operation["attempt_no"]) + 1
                next_fencing = int(operation["fencing_token"] or 0) + 1
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_operation
                    SET status = 'RUNNING', row_version = row_version + 1,
                        attempt_no = %s, worker_id = %s, lease_token = %s,
                        lease_expires_at = %s, fencing_token = %s,
                        result_row_version = NULL, result_status = NULL,
                        result_ref = NULL, result_hash = NULL,
                        error_json = NULL, started_at = COALESCE(started_at, %s),
                        finished_at = NULL
                    WHERE operation_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        next_attempt,
                        worker_id,
                        lease_token,
                        lease_expires_at,
                        next_fencing,
                        datetime.now(UTC),
                        operation_id,
                        expected_row_version,
                    ),
                )
                return self._return_updated(cur, entity="operation", identity=operation_id)

    def _validate_planning_operation_attempt(
        self,
        *,
        batch: dict[str, Any],
        operation: dict[str, Any],
        attempt: HistoricalRangeOperationAttemptV1,
    ) -> None:
        ref = attempt.attempt_receipt_ref
        if ref is None or attempt.result_hash != ref.semantic_content_hash:
            raise ValueError("planning operation attempt receipt/hash is incomplete")
        if ref.artifact_kind not in {
            HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN,
            HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
        }:
            raise ValueError("planning operation attempt must use a planning artifact receipt")
        envelope = self._artifact_store.load_planning(ref)
        if (
            envelope.batch_id != str(batch["batch_id"])
            or envelope.planning_identity_hash != str(batch["planning_identity_hash"])
        ):
            raise ValueError("planning operation attempt receipt belongs to another batch")
        if ref.artifact_kind is HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN:
            if canonicalize(ref.model_dump(mode="json")) != canonicalize(batch["requirement_plan_ref"]):
                raise ValueError("planning operation attempt requirement plan ref differs from the batch")
        elif envelope.catalog_generation != int(operation["catalog_generation"]):
            raise ValueError("planning operation attempt checkpoint uses another catalog generation")

    def commit_catalog_checkpoint(
        self,
        *,
        operation_id: str,
        expected_row_version: int,
        expected_fencing_token: int,
        checkpoint_ref: HistoricalRangeArtifactRefV1,
        checkpoint: HistoricalRangeSourceCatalogCheckpointV1,
        target_status: HistoricalRangeOperationStatus,
        advance_to_verify: bool = False,
        next_worker_id: str | None = None,
        next_lease_token: str | None = None,
        next_lease_expires_at: datetime | None = None,
        reason_codes: Sequence[str] = (),
        error_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if target_status not in {
            HistoricalRangeOperationStatus.RUNNING,
            HistoricalRangeOperationStatus.WAITING_INPUT,
            HistoricalRangeOperationStatus.COMPLETED,
        }:
            raise ValueError("catalog checkpoint target must be RUNNING, WAITING_INPUT, or COMPLETED")
        if advance_to_verify and target_status is not HistoricalRangeOperationStatus.RUNNING:
            raise ValueError("advance_to_verify requires a RUNNING rollover")
        if target_status is HistoricalRangeOperationStatus.RUNNING:
            if not str(next_worker_id or "").strip() or not str(next_lease_token or "").strip():
                raise ValueError("catalog rollover requires the next worker and lease token")
            if (
                next_lease_expires_at is None
                or next_lease_expires_at.tzinfo is None
                or next_lease_expires_at <= datetime.now(UTC)
            ):
                raise ValueError("catalog rollover requires a future timezone-aware lease")
        normalized_reason_codes = tuple(sorted(str(item or "").strip() for item in reason_codes))
        if any(not item for item in normalized_reason_codes) or len(normalized_reason_codes) != len(
            set(normalized_reason_codes)
        ):
            raise ValueError("catalog checkpoint reason_codes must be nonblank and duplicate-free")
        envelope = self._artifact_store.load_planning(checkpoint_ref)
        if (
            checkpoint_ref.artifact_kind is not HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT
            or envelope.payload != checkpoint.model_dump(mode="json")
        ):
            raise ValueError("catalog checkpoint ref does not read back the supplied checkpoint")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                operation = self._lock_row(
                    cur,
                    table="advisory_historical_range_operation",
                    key_name="operation_id",
                    key_value=operation_id,
                )
                latest_ref = operation.get("latest_checkpoint_ref")
                same_checkpoint = latest_ref is not None and canonicalize(latest_ref) == canonicalize(
                    checkpoint_ref.model_dump(mode="json")
                )
                if same_checkpoint and (
                    (target_status is HistoricalRangeOperationStatus.RUNNING and operation["status"] == "RUNNING")
                    or (
                        target_status is HistoricalRangeOperationStatus.WAITING_INPUT
                        and operation["status"] == "WAITING_INPUT"
                    )
                    or (
                        target_status is HistoricalRangeOperationStatus.COMPLETED and operation["status"] == "COMPLETED"
                    )
                ):
                    self._assert_catalog_checkpoint_exact_retry(
                        cur=cur,
                        operation=operation,
                        expected_row_version=expected_row_version,
                        expected_fencing_token=expected_fencing_token,
                        checkpoint_ref=checkpoint_ref,
                        checkpoint=checkpoint,
                        target_status=target_status,
                        advance_to_verify=advance_to_verify,
                        next_worker_id=next_worker_id,
                        next_lease_token=next_lease_token,
                        next_lease_expires_at=next_lease_expires_at,
                        reason_codes=normalized_reason_codes,
                        error_json=error_json,
                    )
                    return operation
                if (
                    str(operation["operation_type"]) != HistoricalRangeOperationType.BUILD_SOURCE_CATALOG.value
                    or str(operation["status"]) != HistoricalRangeOperationStatus.RUNNING.value
                ):
                    raise self._repository_error(
                        "catalog checkpoint commit requires RUNNING operation",
                        operation_id=operation_id,
                        status=operation["status"],
                    )
                self._require_row_version(operation, expected_row_version, entity="operation", identity=operation_id)
                if int(operation["fencing_token"] or 0) != expected_fencing_token:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "catalog operation fencing token differs from the active claim",
                        context={"operation_id": operation_id},
                    )
                batch = self._lock_row(
                    cur,
                    table="advisory_historical_range_batch",
                    key_name="batch_id",
                    key_value=str(operation["batch_id"]),
                )
                if (
                    envelope.batch_id != str(batch["batch_id"])
                    or envelope.planning_identity_hash != str(batch["planning_identity_hash"])
                    or checkpoint.requirement_plan_hash != str(batch["requirement_plan_hash"])
                    or checkpoint.catalog_generation != int(operation["catalog_generation"])
                    or checkpoint.phase.value != str(operation["catalog_phase"])
                ):
                    raise ValueError("catalog checkpoint identity differs from the claimed operation")
                current_cursor = operation.get("stable_keyset_cursor_json") or {"next_requirement_ordinal": 1}
                if int(current_cursor.get("next_requirement_ordinal") or 0) != checkpoint.ordinal_start:
                    raise ValueError("catalog checkpoint ordinal does not match the stable operation cursor")
                expected_resolved_count = int(operation["cumulative_resolved_count"] or 0) + len(
                    checkpoint.member_delta
                )
                if checkpoint.cumulative_resolved_count != expected_resolved_count:
                    raise ValueError("catalog checkpoint cumulative resolved count does not extend operation state")
                expected_chain_hash = str(operation["cumulative_member_chain_hash"])
                for delta in checkpoint.member_delta:
                    expected_chain_hash = append_catalog_member_chain_hash(
                        previous_chain_hash=expected_chain_hash,
                        ordinal=delta.ordinal,
                        member=delta.member,
                    )
                if checkpoint.cumulative_member_chain_hash != expected_chain_hash:
                    raise ValueError("catalog checkpoint member chain does not extend operation state")
                previous_ref = operation.get("latest_checkpoint_ref")
                if previous_ref is None:
                    if checkpoint.previous_checkpoint_ref is not None:
                        raise ValueError("first catalog checkpoint cannot reference a predecessor")
                elif canonicalize(previous_ref) != canonicalize(
                    checkpoint.previous_checkpoint_ref.model_dump(mode="json")
                    if checkpoint.previous_checkpoint_ref is not None
                    else None
                ):
                    raise ValueError("catalog checkpoint predecessor differs from the durable chain head")
                if (
                    target_status is HistoricalRangeOperationStatus.WAITING_INPUT
                    and not checkpoint.unresolved_requirement_delta
                ):
                    raise ValueError("WAITING_INPUT checkpoint requires an unresolved requirement")
                if (
                    target_status is not HistoricalRangeOperationStatus.WAITING_INPUT
                    and checkpoint.unresolved_requirement_delta
                ):
                    raise ValueError("resolved catalog checkpoint cannot contain unresolved requirements")
                unresolved_reason_codes = tuple(
                    sorted(item.reason_code for item in checkpoint.unresolved_requirement_delta)
                )
                if target_status is HistoricalRangeOperationStatus.WAITING_INPUT:
                    if normalized_reason_codes != unresolved_reason_codes:
                        raise ValueError("WAITING_INPUT attempt reasons differ from unresolved checkpoint reasons")
                elif normalized_reason_codes or error_json is not None:
                    raise ValueError("successful catalog checkpoint cannot carry error evidence")
                plan_envelope = self._artifact_store.load_planning(
                    HistoricalRangeArtifactRefV1.model_validate(batch["requirement_plan_ref"])
                )
                plan = HistoricalRangeSourceRequirementPlanV1.model_validate(plan_envelope.payload)
                if advance_to_verify and checkpoint.next_requirement_ordinal != len(plan.requirements) + 1:
                    raise ValueError("DISCOVER can advance to VERIFY only after all requirements resolve")
                if target_status is HistoricalRangeOperationStatus.COMPLETED and (
                    checkpoint.phase is not HistoricalRangeCatalogPhase.VERIFY
                    or checkpoint.next_requirement_ordinal != len(plan.requirements) + 1
                    or checkpoint.cumulative_resolved_count != len(plan.requirements)
                ):
                    raise ValueError("catalog operation can complete only after the full VERIFY pass")
                now = datetime.now(UTC)
                attempt_status = (
                    HistoricalRangeOperationStatus.WAITING_INPUT.value
                    if target_status is HistoricalRangeOperationStatus.WAITING_INPUT
                    else HistoricalRangeOperationStatus.COMPLETED.value
                )
                attempt = HistoricalRangeOperationAttemptV1(
                    attempt_id=derive_prefixed_id(
                        "ahroa",
                        {
                            "operation_id": operation_id,
                            "attempt_no": operation["attempt_no"],
                            "fencing_token": expected_fencing_token,
                            "checkpoint_hash": checkpoint_ref.semantic_content_hash,
                        },
                    ),
                    operation_id=operation_id,
                    attempt_no=int(operation["attempt_no"]),
                    worker_id=str(operation["worker_id"]),
                    lease_token=str(operation["lease_token"]),
                    fencing_token=expected_fencing_token,
                    status=attempt_status,
                    input_cursor_json=current_cursor,
                    result_cursor_json={"next_requirement_ordinal": checkpoint.next_requirement_ordinal},
                    input_hash=canonical_json_sha256(
                        {
                            "planning_identity_hash": batch["planning_identity_hash"],
                            "catalog_generation": checkpoint.catalog_generation,
                            "phase": checkpoint.phase.value,
                            "ordinal_start": checkpoint.ordinal_start,
                            "previous_checkpoint_hash": checkpoint.previous_checkpoint_hash,
                        }
                    ),
                    result_hash=checkpoint_ref.semantic_content_hash,
                    attempt_receipt_ref=checkpoint_ref,
                    reason_codes=normalized_reason_codes,
                    error_json=error_json,
                    started_at=operation["updated_at"],
                    finished_at=now,
                )
                self._insert_operation_attempt(cur=cur, attempt=attempt)
                next_phase = "VERIFY" if advance_to_verify else checkpoint.phase.value
                next_cursor = 1 if advance_to_verify else checkpoint.next_requirement_ordinal
                next_resolved_count = 0 if advance_to_verify else checkpoint.cumulative_resolved_count
                next_unresolved_count = 0 if advance_to_verify else len(checkpoint.unresolved_requirement_delta)
                next_chain_hash = (
                    canonical_json_sha256([]) if advance_to_verify else checkpoint.cumulative_member_chain_hash
                )
                rollover = target_status is HistoricalRangeOperationStatus.RUNNING
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_operation
                    SET status = %s, row_version = row_version + 1,
                        attempt_no = %s, worker_id = %s, lease_token = %s,
                        lease_expires_at = %s, fencing_token = %s,
                        stable_keyset_cursor_json = %s,
                        catalog_phase = %s,
                        latest_checkpoint_ref = %s, latest_checkpoint_hash = %s,
                        cumulative_resolved_count = %s,
                        cumulative_unresolved_count = %s,
                        cumulative_member_chain_hash = %s,
                        result_status = %s, result_ref = %s, result_hash = %s,
                        error_json = %s, finished_at = %s
                    WHERE operation_id = %s AND row_version = %s AND fencing_token = %s
                    RETURNING *
                    """,
                    (
                        target_status.value,
                        int(operation["attempt_no"]) + (1 if rollover else 0),
                        next_worker_id if rollover else None,
                        next_lease_token if rollover else None,
                        next_lease_expires_at if rollover else None,
                        expected_fencing_token + (1 if rollover else 0),
                        psycopg2.extras.Json({"next_requirement_ordinal": next_cursor}),
                        next_phase,
                        psycopg2.extras.Json(checkpoint_ref.model_dump(mode="json")),
                        checkpoint_ref.semantic_content_hash,
                        next_resolved_count,
                        next_unresolved_count,
                        next_chain_hash,
                        None if rollover else target_status.value,
                        None if rollover else psycopg2.extras.Json(checkpoint_ref.model_dump(mode="json")),
                        None if rollover else checkpoint_ref.semantic_content_hash,
                        psycopg2.extras.Json(error_json) if error_json is not None else None,
                        now if target_status is HistoricalRangeOperationStatus.COMPLETED else None,
                        operation_id,
                        expected_row_version,
                        expected_fencing_token,
                    ),
                )
                updated = self._return_updated(cur, entity="operation", identity=operation_id)
                batch_status = (
                    HistoricalRangeBatchStatus.WAITING_INPUT.value
                    if target_status is HistoricalRangeOperationStatus.WAITING_INPUT
                    else HistoricalRangeBatchStatus.PLANNING.value
                )
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_batch
                    SET status = %s, waiting_stage = %s,
                        catalog_generation = %s, catalog_phase = %s,
                        catalog_cursor_ordinal = %s,
                        catalog_resolved_count = %s,
                        catalog_unresolved_count = %s,
                        catalog_member_chain_hash = %s,
                        latest_catalog_checkpoint_ref = %s,
                        latest_catalog_checkpoint_hash = %s,
                        row_version = row_version + 1
                    WHERE batch_id = %s AND row_version = %s
                    """,
                    (
                        batch_status,
                        "CATALOG" if target_status is HistoricalRangeOperationStatus.WAITING_INPUT else None,
                        checkpoint.catalog_generation,
                        next_phase,
                        next_cursor,
                        next_resolved_count,
                        next_unresolved_count,
                        next_chain_hash,
                        psycopg2.extras.Json(checkpoint_ref.model_dump(mode="json")),
                        checkpoint_ref.semantic_content_hash,
                        batch["batch_id"],
                        batch["row_version"],
                    ),
                )
                if cur.rowcount != 1:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "batch changed before catalog checkpoint committed",
                        context={"batch_id": batch["batch_id"]},
                    )
                return updated

    @staticmethod
    def _assert_catalog_checkpoint_exact_retry(
        *,
        cur: Any,
        operation: dict[str, Any],
        expected_row_version: int,
        expected_fencing_token: int,
        checkpoint_ref: HistoricalRangeArtifactRefV1,
        checkpoint: HistoricalRangeSourceCatalogCheckpointV1,
        target_status: HistoricalRangeOperationStatus,
        advance_to_verify: bool,
        next_worker_id: str | None,
        next_lease_token: str | None,
        next_lease_expires_at: datetime | None,
        reason_codes: tuple[str, ...],
        error_json: dict[str, Any] | None,
    ) -> None:
        if int(operation["row_version"]) < expected_row_version + 1:
            raise HistoricalRangeContractError(
                REASON_ROW_VERSION_CONFLICT,
                "catalog checkpoint retry does not follow the supplied row version",
                context={"operation_id": operation["operation_id"]},
            )
        cur.execute(
            """
            SELECT *
            FROM app.advisory_historical_range_operation_attempt
            WHERE operation_id = %s AND fencing_token = %s AND result_hash = %s
            """,
            (
                operation["operation_id"],
                expected_fencing_token,
                checkpoint_ref.semantic_content_hash,
            ),
        )
        attempt = cur.fetchone()
        expected_attempt_status = (
            HistoricalRangeOperationStatus.WAITING_INPUT.value
            if target_status is HistoricalRangeOperationStatus.WAITING_INPUT
            else HistoricalRangeOperationStatus.COMPLETED.value
        )
        if (
            attempt is None
            or str(attempt["status"]) != expected_attempt_status
            or canonicalize(attempt.get("attempt_receipt_ref"))
            != canonicalize(checkpoint_ref.model_dump(mode="json"))
            or tuple(attempt.get("reason_codes_json") or ()) != reason_codes
            or canonicalize(attempt.get("error_json")) != canonicalize(error_json)
            or canonicalize(attempt.get("result_cursor_json"))
            != canonicalize({"next_requirement_ordinal": checkpoint.next_requirement_ordinal})
        ):
            raise HistoricalRangeContractError(
                REASON_IDEMPOTENCY_CONFLICT,
                "catalog checkpoint retry differs from its committed attempt",
                context={"operation_id": operation["operation_id"]},
            )
        if target_status is HistoricalRangeOperationStatus.RUNNING:
            expected_phase = "VERIFY" if advance_to_verify else checkpoint.phase.value
            expected_cursor = 1 if advance_to_verify else checkpoint.next_requirement_ordinal
            if (
                int(operation["fencing_token"]) != expected_fencing_token + 1
                or int(operation["attempt_no"]) != int(attempt["attempt_no"]) + 1
                or str(operation["worker_id"]) != str(next_worker_id)
                or str(operation["lease_token"]) != str(next_lease_token)
                or next_lease_expires_at is None
                or operation["lease_expires_at"] != next_lease_expires_at
                or str(operation["catalog_phase"]) != expected_phase
                or int((operation.get("stable_keyset_cursor_json") or {}).get("next_requirement_ordinal") or 0)
                != expected_cursor
            ):
                raise HistoricalRangeContractError(
                    REASON_IDEMPOTENCY_CONFLICT,
                    "catalog rollover retry differs from the committed successor claim",
                    context={"operation_id": operation["operation_id"]},
                )
        elif int(operation["fencing_token"]) != expected_fencing_token:
            raise HistoricalRangeContractError(
                REASON_IDEMPOTENCY_CONFLICT,
                "catalog terminal checkpoint retry uses a different fencing token",
                context={"operation_id": operation["operation_id"]},
            )

    def restart_catalog_generation(
        self,
        *,
        operation_id: str,
        expected_row_version: int,
        expected_fencing_token: int,
        drift_receipt_ref: HistoricalRangeArtifactRefV1,
        next_worker_id: str,
        next_lease_token: str,
        next_lease_expires_at: datetime,
        error_json: dict[str, Any],
    ) -> dict[str, Any]:
        if (
            not str(next_worker_id or "").strip()
            or not str(next_lease_token or "").strip()
            or next_lease_expires_at.tzinfo is None
            or next_lease_expires_at <= datetime.now(UTC)
        ):
            raise ValueError("catalog generation restart requires a complete future successor lease")
        reason_code = "ADVISORY_HR_SOURCE_REVISION_DRIFT"
        now = datetime.now(UTC)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                operation = self._lock_row(
                    cur,
                    table="advisory_historical_range_operation",
                    key_name="operation_id",
                    key_value=operation_id,
                )
                self._require_row_version(operation, expected_row_version, entity="operation", identity=operation_id)
                if (
                    str(operation["operation_type"]) != HistoricalRangeOperationType.BUILD_SOURCE_CATALOG.value
                    or str(operation["status"]) != HistoricalRangeOperationStatus.RUNNING.value
                    or int(operation["fencing_token"] or 0) != expected_fencing_token
                    or str(operation["catalog_phase"]) != HistoricalRangeCatalogPhase.VERIFY.value
                ):
                    raise self._repository_error(
                        "catalog generation restart requires the active VERIFY claim",
                        operation_id=operation_id,
                    )
                batch = self._lock_row(
                    cur,
                    table="advisory_historical_range_batch",
                    key_name="batch_id",
                    key_value=str(operation["batch_id"]),
                )
                if str(batch["status"]) != HistoricalRangeBatchStatus.PLANNING.value:
                    raise self._repository_error(
                        "catalog generation restart requires PLANNING batch state",
                        operation_id=operation_id,
                    )
                attempt = HistoricalRangeOperationAttemptV1(
                    attempt_id=derive_prefixed_id(
                        "ahroa",
                        {
                            "operation_id": operation_id,
                            "attempt_no": operation["attempt_no"],
                            "fencing_token": expected_fencing_token,
                            "reason_code": reason_code,
                        },
                    ),
                    operation_id=operation_id,
                    attempt_no=int(operation["attempt_no"]),
                    worker_id=str(operation["worker_id"]),
                    lease_token=str(operation["lease_token"]),
                    fencing_token=expected_fencing_token,
                    status=HistoricalRangeOperationStatus.RETRYABLE_FAILED.value,
                    input_cursor_json=operation.get("stable_keyset_cursor_json"),
                    result_cursor_json={"restart_catalog_generation": int(operation["catalog_generation"]) + 1},
                    input_hash=canonical_json_sha256(
                        {
                            "planning_identity_hash": batch["planning_identity_hash"],
                            "catalog_generation": operation["catalog_generation"],
                            "catalog_phase": operation["catalog_phase"],
                            "cursor": operation.get("stable_keyset_cursor_json"),
                        }
                    ),
                    result_hash=drift_receipt_ref.semantic_content_hash,
                    attempt_receipt_ref=drift_receipt_ref,
                    reason_codes=(reason_code,),
                    error_json=error_json,
                    started_at=operation["updated_at"],
                    finished_at=now,
                )
                self._validate_planning_operation_attempt(
                    batch=batch,
                    operation=operation,
                    attempt=attempt,
                )
                self._insert_operation_attempt(cur=cur, attempt=attempt)
                next_generation = int(operation["catalog_generation"]) + 1
                empty_chain_hash = canonical_json_sha256([])
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_operation
                    SET status = 'RUNNING', row_version = row_version + 1,
                        attempt_no = attempt_no + 1, worker_id = %s, lease_token = %s,
                        lease_expires_at = %s, fencing_token = fencing_token + 1,
                        stable_keyset_cursor_json = NULL,
                        catalog_generation = %s, catalog_phase = 'DISCOVER',
                        latest_checkpoint_ref = NULL, latest_checkpoint_hash = NULL,
                        cumulative_resolved_count = 0, cumulative_unresolved_count = 0,
                        cumulative_member_chain_hash = %s,
                        result_status = NULL, result_ref = NULL, result_hash = NULL,
                        error_json = NULL, finished_at = NULL
                    WHERE operation_id = %s AND row_version = %s AND fencing_token = %s
                    RETURNING *
                    """,
                    (
                        next_worker_id,
                        next_lease_token,
                        next_lease_expires_at,
                        next_generation,
                        empty_chain_hash,
                        operation_id,
                        expected_row_version,
                        expected_fencing_token,
                    ),
                )
                updated = self._return_updated(cur, entity="operation", identity=operation_id)
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_batch
                    SET catalog_generation = %s, catalog_phase = 'DISCOVER',
                        catalog_cursor_ordinal = 1,
                        catalog_resolved_count = 0, catalog_unresolved_count = 0,
                        catalog_member_chain_hash = %s,
                        latest_catalog_checkpoint_ref = NULL,
                        latest_catalog_checkpoint_hash = NULL,
                        row_version = row_version + 1
                    WHERE batch_id = %s AND row_version = %s
                    """,
                    (next_generation, empty_chain_hash, batch["batch_id"], batch["row_version"]),
                )
                if cur.rowcount != 1:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "batch changed before catalog generation restart committed",
                        context={"batch_id": batch["batch_id"]},
                    )
                return updated

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

    def load_execution_batch(self, *, batch_id: str) -> HistoricalRangeExecutionBatchV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT batch_id, status, row_version, request_payload_sha256,
                           request_artifact_ref, date_plan_ref, artifact_root_identity_hash
                    FROM app.advisory_historical_range_batch
                    WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise self._repository_error("execution batch does not exist", batch_id=batch_id)
        payload = dict(row)
        if payload["request_payload_sha256"] is None or payload["request_artifact_ref"] is None or payload["date_plan_ref"] is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "execution requires a sealed historical-range batch",
                context={"batch_id": batch_id, "status": payload.get("status")},
            )
        return HistoricalRangeExecutionBatchV1(
            batch_id=str(payload["batch_id"]),
            status=HistoricalRangeBatchStatus(str(payload["status"])),
            row_version=int(payload["row_version"]),
            resolved_request_hash=str(payload["request_payload_sha256"]),
            request_ref=HistoricalRangeArtifactRefV1.model_validate(payload["request_artifact_ref"]),
            date_plan_ref=HistoricalRangeArtifactRefV1.model_validate(payload["date_plan_ref"]),
            artifact_root_identity_hash=str(payload["artifact_root_identity_hash"]),
        )

    def list_execution_runs(
        self,
        *,
        batch_id: str,
        stable_after_research_program_id: str | None = None,
        limit: int = 500,
    ) -> tuple[HistoricalRangeExecutionRunV1, ...]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT batch_id, range_run_id, research_program_id, status, row_version,
                           materialized_day_count, day_plan_cursor_ordinal,
                           final_receipt_ref, final_receipt_hash
                    FROM app.advisory_historical_range_run
                    WHERE batch_id = %s
                      AND (%s IS NULL OR research_program_id > %s)
                    ORDER BY research_program_id, range_run_id
                    LIMIT %s
                    """,
                    (batch_id, stable_after_research_program_id, stable_after_research_program_id, limit),
                )
                rows = cur.fetchall()
        return tuple(
            HistoricalRangeExecutionRunV1(
                batch_id=str(row["batch_id"]),
                range_run_id=str(row["range_run_id"]),
                research_program_id=str(row["research_program_id"]),
                status=HistoricalRangeProgramStatus(str(row["status"])),
                row_version=int(row["row_version"]),
                materialized_day_count=int(row["materialized_day_count"]),
                day_plan_cursor_ordinal=int(row["day_plan_cursor_ordinal"]),
                final_receipt_ref=(
                    HistoricalRangeArtifactRefV1.model_validate(row["final_receipt_ref"])
                    if row["final_receipt_ref"] is not None
                    else None
                ),
                final_receipt_hash=str(row["final_receipt_hash"]) if row["final_receipt_hash"] is not None else None,
            )
            for row in rows
        )

    def list_all_execution_runs(self, *, batch_id: str) -> tuple[HistoricalRangeExecutionRunV1, ...]:
        """Read the complete Program set using the stable keyset page contract."""

        page_size = 500
        stable_after: str | None = None
        rows: list[HistoricalRangeExecutionRunV1] = []
        while True:
            page = self.list_execution_runs(
                batch_id=batch_id,
                stable_after_research_program_id=stable_after,
                limit=page_size,
            )
            if page and stable_after is not None and page[0].research_program_id <= stable_after:
                raise HistoricalRangeContractError(
                    REASON_REPOSITORY_CONFLICT,
                    "execution repository returned a non-advancing Program page",
                    context={"batch_id": batch_id, "stable_after_research_program_id": stable_after},
                )
            rows.extend(page)
            if len(page) < page_size:
                return tuple(rows)
            next_stable_after = page[-1].research_program_id
            if next_stable_after == stable_after:
                raise HistoricalRangeContractError(
                    REASON_REPOSITORY_CONFLICT,
                    "execution repository Program cursor did not advance",
                    context={"batch_id": batch_id, "stable_after_research_program_id": stable_after},
                )
            stable_after = next_stable_after

    def claim_next_day(
        self,
        *,
        batch_id: str,
        range_run_id: str,
        expected_run_row_version: int,
        worker_id: str,
        lease_token: str,
        lease_seconds: int,
    ) -> HistoricalRangeClaimedDayV1 | None:
        """Atomically assign only the earliest non-success day of one Program."""

        if expected_run_row_version < 1:
            raise ValueError("expected_run_row_version must be positive")
        if not str(worker_id or "").strip() or not str(lease_token or "").strip():
            raise ValueError("worker_id and lease_token are required")
        if not 1 <= lease_seconds <= 86_400:
            raise ValueError("lease_seconds must be between 1 and 86400")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run.*, batch.status AS batch_status,
                           batch.request_payload_sha256, batch.request_artifact_ref,
                           batch.list_semantics_version, batch.list_semantics_hash
                    FROM app.advisory_historical_range_run AS run
                    JOIN app.advisory_historical_range_batch AS batch ON batch.batch_id = run.batch_id
                    WHERE run.range_run_id = %s AND run.batch_id = %s
                    FOR UPDATE OF run, batch
                    """,
                    (range_run_id, batch_id),
                )
                run = cur.fetchone()
                if run is None:
                    raise self._repository_error(
                        "execution range run does not belong to batch",
                        batch_id=batch_id,
                        range_run_id=range_run_id,
                    )
                run_data = dict(run)
                self._require_row_version(run_data, expected_run_row_version, entity="run", identity=range_run_id)
                if str(run_data["status"]) != HistoricalRangeProgramStatus.RUNNING.value:
                    return None
                if str(run_data["batch_status"]) not in {
                    HistoricalRangeBatchStatus.RUNNING.value,
                    HistoricalRangeBatchStatus.PARTIAL.value,
                }:
                    return None
                cur.execute(
                    """
                    SELECT *
                    FROM app.advisory_historical_range_day_run
                    WHERE range_run_id = %s
                      AND status NOT IN ('COMPLETE', 'VALID_NO_CANDIDATE')
                    ORDER BY ordinal
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (range_run_id,),
                )
                day_row = cur.fetchone()
                if day_row is None:
                    return None
                day_data = dict(day_row)
                day_status = HistoricalRangeDayStatus(str(day_data["status"]))
                if day_status in {HistoricalRangeDayStatus.FAILED, HistoricalRangeDayStatus.CANCELLED, HistoricalRangeDayStatus.RUNNING}:
                    return None
                if day_status not in {
                    HistoricalRangeDayStatus.PENDING,
                    HistoricalRangeDayStatus.WAITING_INPUT,
                    HistoricalRangeDayStatus.RETRYABLE_FAILED,
                    HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY,
                }:
                    raise self._repository_error(
                        "next execution day has an unsupported non-success state",
                        day_run_id=str(day_data["day_run_id"]),
                        status=day_status.value,
                    )
                if day_status is not HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY:
                    cur.execute(
                        """
                        UPDATE app.advisory_historical_range_day_run
                        SET status = 'WAITING_PREVIOUS_DAY', row_version = row_version + 1
                        WHERE day_run_id = %s AND row_version = %s
                        RETURNING *
                        """,
                        (day_data["day_run_id"], day_data["row_version"]),
                    )
                    waiting = cur.fetchone()
                    if waiting is None:
                        raise HistoricalRangeContractError(
                            REASON_ROW_VERSION_CONFLICT,
                            "day changed while entering predecessor-wait state",
                            context={"day_run_id": day_data["day_run_id"]},
                        )
                    day_data = dict(waiting)
                predecessor = self._locked_predecessor_for_claim(cur=cur, day=day_data)
                fencing_token = int(day_data.get("current_fencing_token") or 0) + 1
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_day_run
                    SET status = 'RUNNING',
                        row_version = row_version + 1,
                        attempt_no = attempt_no + 1,
                        worker_id = %s,
                        lease_token = %s,
                        lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                        current_fencing_token = %s,
                        previous_day_run_hash = %s,
                        previous_list_version_id = %s,
                        previous_list_version_hash = %s,
                        reason_codes_json = '[]'::jsonb,
                        error_json = NULL,
                        started_at = COALESCE(started_at, clock_timestamp())
                    WHERE day_run_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        worker_id,
                        lease_token,
                        lease_seconds,
                        fencing_token,
                        predecessor["day_receipt_hash"],
                        predecessor["list_version_id"],
                        predecessor["list_version_hash"],
                        day_data["day_run_id"],
                        day_data["row_version"],
                    ),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "day changed while being claimed",
                        context={"day_run_id": day_data["day_run_id"]},
                    )
                self._sync_run_aggregate(cur=cur, range_run_id=range_run_id)
                return self._claimed_day_from_row(
                    row=dict(claimed),
                    batch_id=batch_id,
                    research_program_id=str(run_data["research_program_id"]),
                    resolved_request_hash=str(run_data["request_payload_sha256"]),
                    request_ref=HistoricalRangeArtifactRefV1.model_validate(run_data["request_artifact_ref"]),
                    list_semantics_version=str(run_data["list_semantics_version"]),
                    list_semantics_hash=str(run_data["list_semantics_hash"]),
                    predecessor=predecessor,
                )

    def heartbeat_day(
        self,
        *,
        claimed_day: HistoricalRangeClaimedDayV1,
        lease_seconds: int,
    ) -> HistoricalRangeClaimedDayV1:
        if not 1 <= lease_seconds <= 86_400:
            raise ValueError("lease_seconds must be between 1 and 86400")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_day_run AS day
                    SET row_version = day.row_version + 1,
                        lease_expires_at = clock_timestamp() + make_interval(secs => %s)
                    FROM app.advisory_historical_range_run AS run
                    JOIN app.advisory_historical_range_batch AS batch ON batch.batch_id = run.batch_id
                    WHERE day.day_run_id = %s
                      AND day.range_run_id = run.range_run_id
                      AND run.batch_id = %s
                      AND day.status = 'RUNNING'
                      AND day.row_version = %s
                      AND day.attempt_no = %s
                      AND day.worker_id = %s
                      AND day.lease_token = %s
                      AND day.current_fencing_token = %s
                    RETURNING day.*, run.research_program_id, batch.request_payload_sha256,
                              batch.request_artifact_ref, batch.list_semantics_version,
                              batch.list_semantics_hash
                    """,
                    (
                        lease_seconds,
                        claimed_day.day_run_id,
                        claimed_day.batch_id,
                        claimed_day.row_version,
                        claimed_day.attempt_no,
                        claimed_day.worker_id,
                        claimed_day.lease_token,
                        claimed_day.fencing_token,
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise HistoricalRangeContractError(
                REASON_ROW_VERSION_CONFLICT,
                "day heartbeat lost its durable worker/lease/fencing ownership",
                context={"day_run_id": claimed_day.day_run_id},
            )
        return self._claimed_day_from_row(
            row=dict(row),
            batch_id=claimed_day.batch_id,
            research_program_id=str(row["research_program_id"]),
            resolved_request_hash=str(row["request_payload_sha256"]),
            request_ref=HistoricalRangeArtifactRefV1.model_validate(row["request_artifact_ref"]),
            list_semantics_version=str(row["list_semantics_version"]),
            list_semantics_hash=str(row["list_semantics_hash"]),
            predecessor={
                "day_receipt_ref": claimed_day.previous_day_receipt_ref.model_dump(mode="json")
                if claimed_day.previous_day_receipt_ref is not None
                else None,
                "day_receipt_hash": claimed_day.previous_day_receipt_ref.semantic_content_hash
                if claimed_day.previous_day_receipt_ref is not None
                else None,
                "list_version_id": claimed_day.previous_list_version_id,
                "list_version_hash": claimed_day.previous_list_hash,
            },
        )

    def load_expired_claimable_day(
        self,
        *,
        batch_id: str,
        range_run_id: str,
    ) -> HistoricalRangeClaimedDayV1 | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT day.*, run.research_program_id, run.status AS run_status,
                           batch.status AS batch_status, batch.request_payload_sha256,
                           batch.request_artifact_ref, batch.list_semantics_version,
                           batch.list_semantics_hash
                    FROM app.advisory_historical_range_day_run AS day
                    JOIN app.advisory_historical_range_run AS run ON run.range_run_id = day.range_run_id
                    JOIN app.advisory_historical_range_batch AS batch ON batch.batch_id = run.batch_id
                    WHERE run.batch_id = %s AND run.range_run_id = %s
                      AND day.status NOT IN ('COMPLETE', 'VALID_NO_CANDIDATE')
                    ORDER BY day.ordinal
                    LIMIT 1
                    """,
                    (batch_id, range_run_id),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                day = dict(row)
                if str(day["status"]) != HistoricalRangeDayStatus.RUNNING.value:
                    return None
                cur.execute("SELECT clock_timestamp() AS now")
                db_now = cur.fetchone()["now"]
                if day["lease_expires_at"] is None or day["lease_expires_at"] > db_now:
                    return None
                if str(day["run_status"]) != HistoricalRangeProgramStatus.RUNNING.value or str(
                    day["batch_status"]
                ) not in {HistoricalRangeBatchStatus.RUNNING.value, HistoricalRangeBatchStatus.PARTIAL.value}:
                    raise HistoricalRangeContractError(
                        REASON_REPOSITORY_CONFLICT,
                        "expired day belongs to a non-running Program/batch",
                        context={"day_run_id": day["day_run_id"]},
                    )
                predecessor = self._locked_predecessor_for_claim(cur=cur, day=day)
        return self._claimed_day_from_row(
            row=day,
            batch_id=batch_id,
            research_program_id=str(day["research_program_id"]),
            resolved_request_hash=str(day["request_payload_sha256"]),
            request_ref=HistoricalRangeArtifactRefV1.model_validate(day["request_artifact_ref"]),
            list_semantics_version=str(day["list_semantics_version"]),
            list_semantics_hash=str(day["list_semantics_hash"]),
            predecessor=predecessor,
        )

    def take_over_expired_day(
        self,
        *,
        expired_claim: HistoricalRangeClaimedDayV1,
        expired_attempt: HistoricalRangeDayAttemptV1,
        worker_id: str,
        lease_token: str,
        lease_seconds: int,
    ) -> HistoricalRangeClaimedDayV1:
        if not 1 <= lease_seconds <= 86_400:
            raise ValueError("lease_seconds must be between 1 and 86400")
        worker_id = str(worker_id or "").strip()
        lease_token = str(lease_token or "").strip()
        if not worker_id or not lease_token:
            raise ValueError("day takeover requires worker_id and lease_token")
        self._validate_day_attempt_artifacts(
            attempt=expired_attempt,
            range_run_id=expired_claim.range_run_id,
            resolved_request_hash=expired_claim.resolved_request_hash,
        )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                current = self._lock_row(
                    cur,
                    table="advisory_historical_range_day_run",
                    key_name="day_run_id",
                    key_value=expired_claim.day_run_id,
                )
                if (
                    str(current["status"]) != HistoricalRangeDayStatus.RUNNING.value
                    or int(current["row_version"]) != expired_claim.row_version
                    or int(current["attempt_no"]) != expired_claim.attempt_no
                    or current["worker_id"] != expired_claim.worker_id
                    or current["lease_token"] != expired_claim.lease_token
                    or int(current["current_fencing_token"] or 0) != expired_claim.fencing_token
                ):
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "expired day ownership changed before takeover",
                        context={"day_run_id": expired_claim.day_run_id},
                    )
                cur.execute("SELECT clock_timestamp() AS now")
                db_now = cur.fetchone()["now"]
                if current["lease_expires_at"] is None or current["lease_expires_at"] > db_now:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "day takeover requires a DB-clock expired lease",
                        context={"day_run_id": expired_claim.day_run_id},
                    )
                self._require_expired_day_attempt(current=current, attempt=expired_attempt)
                self._insert_day_attempt(cur=cur, attempt=expired_attempt)
                next_attempt = int(current["attempt_no"]) + 1
                next_fencing = int(current["current_fencing_token"]) + 1
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_day_run
                    SET row_version = row_version + 1,
                        attempt_no = %s, worker_id = %s, lease_token = %s,
                        lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                        current_fencing_token = %s,
                        reason_codes_json = '[]'::jsonb, error_json = NULL
                    WHERE day_run_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (
                        next_attempt,
                        worker_id,
                        lease_token,
                        lease_seconds,
                        next_fencing,
                        expired_claim.day_run_id,
                        expired_claim.row_version,
                    ),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "expired day changed during takeover",
                        context={"day_run_id": expired_claim.day_run_id},
                    )
                predecessor = self._locked_predecessor_for_claim(cur=cur, day=dict(claimed))
        return self._claimed_day_from_row(
            row=dict(claimed),
            batch_id=expired_claim.batch_id,
            research_program_id=expired_claim.research_program_id,
            resolved_request_hash=expired_claim.resolved_request_hash,
            request_ref=expired_claim.request_ref,
            list_semantics_version=expired_claim.list_semantics_version,
            list_semantics_hash=expired_claim.list_semantics_hash,
            predecessor=predecessor,
        )

    def load_predecessor_state(self, *, day_run_id: str) -> HistoricalRangePredecessorStateV1:
        """Load and fully verify only the immediately preceding committed day."""
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT previous_day_run_id
                    FROM app.advisory_historical_range_day_run
                    WHERE day_run_id = %s
                    """,
                    (day_run_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise self._repository_error("day run does not exist", day_run_id=day_run_id)
        predecessor_day_run_id = row["previous_day_run_id"]
        if predecessor_day_run_id is None:
            return HistoricalRangePredecessorStateV1(day_run_id=day_run_id)
        readback = self.full_readback_successful_day(day_run_id=str(predecessor_day_run_id))
        active = tuple(
            item
            for item in readback.receipt.episode_snapshots
            if item.recommendation_state in {"ACTIVE", "ACTIVE_AT_RANGE_END"}
        )
        return HistoricalRangePredecessorStateV1(
            day_run_id=day_run_id,
            list_version=readback.receipt.list_version,
            active_episodes=active,
            day_receipt_ref=readback.receipt_ref,
        )

    def full_readback_successful_day(self, *, day_run_id: str) -> HistoricalRangeSuccessfulDayReadbackV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT day.*, run.research_program_id, batch.request_payload_sha256
                    FROM app.advisory_historical_range_day_run AS day
                    JOIN app.advisory_historical_range_run AS run ON run.range_run_id = day.range_run_id
                    JOIN app.advisory_historical_range_batch AS batch ON batch.batch_id = run.batch_id
                    WHERE day.day_run_id = %s
                    """,
                    (day_run_id,),
                )
                day_row = cur.fetchone()
                if day_row is None:
                    raise self._repository_error("day run does not exist", day_run_id=day_run_id)
                day = dict(day_row)
                if day["status"] not in {
                    HistoricalRangeDayStatus.COMPLETE.value,
                    HistoricalRangeDayStatus.VALID_NO_CANDIDATE.value,
                }:
                    raise HistoricalRangeContractError(
                        REASON_REPOSITORY_CONFLICT,
                        "full readback requires a successful day",
                        context={"day_run_id": day_run_id, "status": day["status"]},
                    )
                if any(
                    day.get(field) is None
                    for field in (
                        "day_input_hash",
                        "candidate_artifact_ref",
                        "list_version_id",
                        "day_receipt_ref",
                    )
                ):
                    raise HistoricalRangeContractError(
                        REASON_REPOSITORY_CONFLICT,
                        "successful day has incomplete canonical refs",
                        context={"day_run_id": day_run_id},
                    )
                cur.execute(
                    "SELECT * FROM app.advisory_historical_range_list_version WHERE list_version_id = %s",
                    (day["list_version_id"],),
                )
                list_row = cur.fetchone()
                cur.execute(
                    "SELECT * FROM app.advisory_historical_range_list_item WHERE list_version_id = %s ORDER BY symbol, action",
                    (day["list_version_id"],),
                )
                item_rows = tuple(dict(item) for item in cur.fetchall())
                cur.execute(
                    "SELECT * FROM app.advisory_historical_range_episode_snapshot WHERE list_version_id = %s ORDER BY symbol, episode_id",
                    (day["list_version_id"],),
                )
                episode_rows = tuple(dict(item) for item in cur.fetchall())
                cur.execute(
                    "SELECT * FROM app.advisory_historical_range_candidate WHERE day_run_id = %s ORDER BY symbol",
                    (day_run_id,),
                )
                candidate_rows = tuple(dict(item) for item in cur.fetchall())
                cur.execute(
                    "SELECT * FROM app.advisory_historical_range_day_attempt WHERE day_run_id = %s AND attempt_no = %s",
                    (day_run_id, day["attempt_no"]),
                )
                attempt_row = cur.fetchone()
        if list_row is None or attempt_row is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "successful day lacks its canonical list or final attempt",
                context={"day_run_id": day_run_id},
            )
        list_version = self._list_version_fact_from_row(dict(list_row))
        items = tuple(self._list_item_fact_from_row(item) for item in item_rows)
        episodes = tuple(self._episode_fact_from_row(item) for item in episode_rows)
        candidates = tuple(self._candidate_fact_from_row(item) for item in candidate_rows)
        attempt = self._day_attempt_from_row(dict(attempt_row))
        receipt_ref = HistoricalRangeArtifactRefV1.model_validate(day["day_receipt_ref"])
        candidate_ref = HistoricalRangeArtifactRefV1.model_validate(day["candidate_artifact_ref"])
        if (
            receipt_ref.semantic_content_hash != day["day_receipt_hash"]
            or candidate_ref.semantic_content_hash != day["candidate_artifact_hash"]
            or list_version.list_content_hash != day["list_version_hash"]
            or list_version.list_version_id != day["list_version_id"]
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "successful day ref/hash columns do not close their canonical facts",
                context={"day_run_id": day_run_id},
            )
        receipt_envelope = self._load_artifact(
            receipt_ref,
            expected_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
            resolved_request_hash=str(day["request_payload_sha256"]),
            range_run_id=str(day["range_run_id"]),
            day_run_id=day_run_id,
            allow_direct_predecessor_day_run_id=(
                str(day["previous_day_run_id"]) if day["previous_day_run_id"] is not None else None
            ),
            validate_recursive_upstream=False,
        )
        receipt = HistoricalRangeDayReceiptPayloadV2.model_validate(receipt_envelope.payload)
        expected_receipt = build_day_receipt_payload_v2(
            range_run_id=str(day["range_run_id"]),
            day_run_id=day_run_id,
            terminal_status=HistoricalRangeDayStatus(str(day["status"])),
            day_input_hash=str(day["day_input_hash"]),
            candidate_artifact_ref=candidate_ref,
            decision_mark_set_ref=receipt.decision_mark_set_ref,
            previous_day_receipt_ref=receipt.previous_day_receipt_ref,
            list_version=list_version,
            items=items,
            episodes=episodes,
        )
        if canonicalize(receipt.model_dump(mode="json")) != canonicalize(expected_receipt):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "successful DAY_RECEIPT differs from DB list facts",
                context={"day_run_id": day_run_id},
            )
        expected_upstream = (receipt.candidate_artifact_ref, receipt.decision_mark_set_ref) + (
            (receipt.previous_day_receipt_ref,) if receipt.previous_day_receipt_ref is not None else ()
        )
        if tuple(receipt_envelope.upstream_refs) != tuple(
            sorted(expected_upstream, key=lambda ref: (ref.artifact_kind.value, ref.semantic_content_hash, ref.relative_path))
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "successful DAY_RECEIPT upstream set differs from its typed payload",
                context={"day_run_id": day_run_id},
            )
        candidate_envelope = self._load_artifact(
            receipt.candidate_artifact_ref,
            expected_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
            resolved_request_hash=str(day["request_payload_sha256"]),
            range_run_id=str(day["range_run_id"]),
            day_run_id=day_run_id,
        )
        candidate_payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(candidate_envelope.payload)
        if tuple(sorted(candidate_payload.candidates, key=lambda item: item.symbol)) != candidates:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "candidate DB facts differ from the exact candidate artifact",
                context={"day_run_id": day_run_id},
            )
        mark_envelope = self._load_artifact(
            receipt.decision_mark_set_ref,
            expected_kind=HistoricalRangeArtifactKind.DECISION_MARK_SET,
            resolved_request_hash=str(day["request_payload_sha256"]),
            range_run_id=str(day["range_run_id"]),
            day_run_id=day_run_id,
            allow_direct_predecessor_day_run_id=(
                str(day["previous_day_run_id"]) if day["previous_day_run_id"] is not None else None
            ),
            validate_recursive_upstream=False,
        )
        mark_set = HistoricalRangeDecisionMarkSetV1.model_validate(mark_envelope.payload)
        if mark_set.predecessor_day_receipt_ref != receipt.previous_day_receipt_ref:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "decision mark predecessor differs from the successful day receipt",
                context={"day_run_id": day_run_id},
            )
        expected_mark_upstream = (mark_set.upstream_request_ref,) + (
            (mark_set.predecessor_day_receipt_ref,)
            if mark_set.predecessor_day_receipt_ref is not None
            else ()
        )
        if tuple(mark_envelope.upstream_refs) != tuple(
            sorted(
                expected_mark_upstream,
                key=lambda ref: (ref.artifact_kind.value, ref.semantic_content_hash, ref.relative_path),
            )
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "decision mark upstream set differs from its typed direct lineage",
                context={"day_run_id": day_run_id},
            )
        if attempt.attempt_receipt_ref != receipt_ref or attempt.status != str(day["status"]):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "successful day final attempt differs from the canonical receipt",
                context={"day_run_id": day_run_id},
            )
        return HistoricalRangeSuccessfulDayReadbackV1(
            ordinal=int(day["ordinal"]),
            decision_trade_date=day["decision_trade_date"],
            receipt_ref=receipt_ref,
            receipt=receipt,
            candidate_payload=candidate_payload,
            decision_mark_set=mark_set,
            attempt=attempt,
        )

    def load_episode_entry_sequences(self, *, range_run_id: str) -> dict[str, int]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT symbol, MAX(entry_sequence) AS entry_sequence
                    FROM app.advisory_historical_range_episode_snapshot
                    WHERE range_run_id = %s
                    GROUP BY symbol
                    """,
                    (range_run_id,),
                )
                return {str(row["symbol"]): int(row["entry_sequence"]) for row in cur.fetchall()}

    def load_reusable_candidate_ref(self, *, day_run_id: str) -> HistoricalRangeArtifactRefV1 | None:
        """Return only the latest durable non-success attempt's exact candidate ref."""

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT day.status AS day_status, attempt.candidate_artifact_ref
                    FROM app.advisory_historical_range_day_run AS day
                    LEFT JOIN LATERAL (
                        SELECT candidate_artifact_ref
                        FROM app.advisory_historical_range_day_attempt
                        WHERE day_run_id = day.day_run_id
                          AND status IN ('WAITING_INPUT', 'RETRYABLE_FAILED')
                          AND candidate_artifact_ref IS NOT NULL
                        ORDER BY attempt_no DESC
                        LIMIT 1
                    ) AS attempt ON TRUE
                    WHERE day.day_run_id = %s
                    """,
                    (day_run_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise self._repository_error("day run does not exist", day_run_id=day_run_id)
        if str(row["day_status"]) != HistoricalRangeDayStatus.RUNNING.value:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "candidate reuse requires a currently claimed RUNNING day",
                context={"day_run_id": day_run_id, "status": row["day_status"]},
            )
        if row["candidate_artifact_ref"] is None:
            return None
        return HistoricalRangeArtifactRefV1.model_validate(row["candidate_artifact_ref"])

    def load_cancellation_day_contexts(
        self,
        *,
        batch_id: str,
    ) -> tuple[HistoricalRangeCancellationDayContext, ...]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT day.*, run.research_program_id,
                           batch.request_payload_sha256, batch.request_artifact_ref,
                           predecessor.day_receipt_ref AS previous_day_receipt_ref,
                           predecessor.list_version_hash AS previous_list_hash
                    FROM app.advisory_historical_range_day_run AS day
                    JOIN app.advisory_historical_range_run AS run ON run.range_run_id = day.range_run_id
                    JOIN app.advisory_historical_range_batch AS batch ON batch.batch_id = run.batch_id
                    LEFT JOIN app.advisory_historical_range_day_run AS predecessor
                      ON predecessor.day_run_id = day.previous_day_run_id
                     AND predecessor.status IN ('COMPLETE', 'VALID_NO_CANDIDATE')
                    WHERE run.batch_id = %s
                      AND day.status NOT IN ('COMPLETE', 'VALID_NO_CANDIDATE', 'FAILED', 'CANCELLED')
                    ORDER BY run.research_program_id, day.ordinal
                    """,
                    (batch_id,),
                )
                rows = tuple(dict(item) for item in cur.fetchall())
        return tuple(
            HistoricalRangeCancellationDayContext(
                batch_id=batch_id,
                range_run_id=str(row["range_run_id"]),
                research_program_id=str(row["research_program_id"]),
                day_run_id=str(row["day_run_id"]),
                ordinal=int(row["ordinal"]),
                row_version=int(row["row_version"]),
                status=HistoricalRangeDayStatus(str(row["status"])),
                attempt_no=int(row["attempt_no"]),
                worker_id=(str(row["worker_id"]) if row.get("worker_id") is not None else None),
                lease_token=(str(row["lease_token"]) if row.get("lease_token") is not None else None),
                fencing_token=(
                    int(row["current_fencing_token"])
                    if row.get("current_fencing_token") is not None
                    else None
                ),
                resolved_request_hash=str(row["request_payload_sha256"]),
                request_ref=HistoricalRangeArtifactRefV1.model_validate(row["request_artifact_ref"]),
                previous_list_hash=(str(row["previous_list_hash"]) if row["previous_list_hash"] else None),
                previous_day_receipt_ref=(
                    HistoricalRangeArtifactRefV1.model_validate(row["previous_day_receipt_ref"])
                    if row["previous_day_receipt_ref"] is not None
                    else None
                ),
            )
            for row in rows
        )

    def load_cancelled_day_results(
        self,
        *,
        batch_id: str,
    ) -> tuple[HistoricalRangeOperationCancelledDayResultV1, ...]:
        """Read every materialized cancelled day and its exact terminal attempt."""

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run.research_program_id, day.range_run_id, day.day_run_id,
                           day.ordinal, day.row_version, day.attempt_no,
                           day.current_fencing_token, attempt.attempt_no AS receipt_attempt_no,
                           attempt.fencing_token AS receipt_fencing_token,
                           attempt.attempt_receipt_ref
                    FROM app.advisory_historical_range_day_run AS day
                    JOIN app.advisory_historical_range_run AS run
                      ON run.range_run_id = day.range_run_id
                    LEFT JOIN LATERAL (
                        SELECT terminal.attempt_no, terminal.fencing_token,
                               terminal.attempt_receipt_ref
                        FROM app.advisory_historical_range_day_attempt AS terminal
                        WHERE terminal.day_run_id = day.day_run_id
                          AND terminal.status = 'CANCELLED'
                        ORDER BY terminal.attempt_no DESC
                        LIMIT 1
                    ) AS attempt ON TRUE
                    WHERE run.batch_id = %s AND day.status = 'CANCELLED'
                    ORDER BY run.research_program_id, day.ordinal, day.day_run_id
                    """,
                    (batch_id,),
                )
                rows = tuple(dict(item) for item in cur.fetchall())
        results: list[HistoricalRangeOperationCancelledDayResultV1] = []
        for row in rows:
            if (
                row["attempt_receipt_ref"] is None
                or row["receipt_attempt_no"] is None
                or row["receipt_fencing_token"] is None
                or int(row["receipt_attempt_no"]) != int(row["attempt_no"])
                or int(row["receipt_fencing_token"]) != int(row["current_fencing_token"] or 0)
            ):
                raise HistoricalRangeContractError(
                    REASON_REPOSITORY_CONFLICT,
                    "cancelled day lacks its exact terminal attempt receipt",
                    context={"batch_id": batch_id, "day_run_id": row["day_run_id"]},
                )
            results.append(
                HistoricalRangeOperationCancelledDayResultV1(
                    range_run_id=str(row["range_run_id"]),
                    research_program_id=str(row["research_program_id"]),
                    day_run_id=str(row["day_run_id"]),
                    ordinal=int(row["ordinal"]),
                    row_version=int(row["row_version"]),
                    attempt_no=int(row["attempt_no"]),
                    fencing_token=int(row["current_fencing_token"]),
                    attempt_receipt_ref=HistoricalRangeArtifactRefV1.model_validate(
                        row["attempt_receipt_ref"]
                    ),
                )
            )
        return tuple(results)

    def cancel_execution_batch(
        self,
        *,
        batch_id: str,
        expected_batch_row_version: int,
        attempts: Mapping[str, HistoricalRangeDayAttemptV1],
    ) -> tuple[str, ...]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                batch = self._lock_row(
                    cur,
                    table="advisory_historical_range_batch",
                    key_name="batch_id",
                    key_value=batch_id,
                )
                self._require_row_version(batch, expected_batch_row_version, entity="batch", identity=batch_id)
                if str(batch["status"]) not in {
                    HistoricalRangeBatchStatus.QUEUED.value,
                    HistoricalRangeBatchStatus.RUNNING.value,
                    HistoricalRangeBatchStatus.PARTIAL.value,
                    HistoricalRangeBatchStatus.WAITING_INPUT.value,
                }:
                    raise self._repository_error(
                        "historical execution batch is not cancellable",
                        batch_id=batch_id,
                        status=batch["status"],
                    )
                cur.execute(
                    """
                    SELECT day.*
                    FROM app.advisory_historical_range_day_run AS day
                    JOIN app.advisory_historical_range_run AS run ON run.range_run_id = day.range_run_id
                    WHERE run.batch_id = %s
                      AND day.status NOT IN ('COMPLETE', 'VALID_NO_CANDIDATE', 'FAILED', 'CANCELLED')
                    ORDER BY day.range_run_id, day.ordinal
                    FOR UPDATE OF day
                    """,
                    (batch_id,),
                )
                days = tuple(dict(item) for item in cur.fetchall())
                if set(attempts) != {str(item["day_run_id"]) for item in days}:
                    raise HistoricalRangeContractError(
                        REASON_REPOSITORY_CONFLICT,
                        "cancel attempt set differs from current nonterminal day set",
                        context={"batch_id": batch_id},
                    )
                touched_runs: set[str] = set()
                for day in days:
                    attempt = attempts[str(day["day_run_id"])]
                    self._validate_day_attempt_artifacts(
                        attempt=attempt,
                        range_run_id=str(day["range_run_id"]),
                        resolved_request_hash=str(batch["request_payload_sha256"]),
                    )
                    expected_attempt = int(day["attempt_no"]) if day["status"] == "RUNNING" else int(day["attempt_no"]) + 1
                    expected_fencing = (
                        int(day["current_fencing_token"])
                        if day["status"] == "RUNNING"
                        else int(day["current_fencing_token"] or 0) + 1
                    )
                    if (
                        attempt.attempt_no != expected_attempt
                        or attempt.fencing_token != expected_fencing
                        or attempt.status != HistoricalRangeDayStatus.CANCELLED.value
                    ):
                        raise ValueError("cancel attempt differs from the current day ownership/state")
                    if day["status"] == "RUNNING" and (
                        attempt.worker_id != day["worker_id"] or attempt.lease_token != day["lease_token"]
                    ):
                        raise ValueError("RUNNING day cancel attempt must fence its current worker/lease")
                    self._insert_day_attempt(cur=cur, attempt=attempt)
                    cur.execute(
                        """
                        UPDATE app.advisory_historical_range_day_run
                        SET status = 'CANCELLED', row_version = row_version + 1,
                            attempt_no = %s, current_fencing_token = %s,
                            worker_id = NULL, lease_token = NULL, lease_expires_at = NULL,
                            reason_codes_json = %s, error_json = %s,
                            started_at = COALESCE(started_at, %s), finished_at = %s
                        WHERE day_run_id = %s AND row_version = %s
                        """,
                        (
                            attempt.attempt_no,
                            attempt.fencing_token,
                            psycopg2.extras.Json(list(attempt.reason_codes)),
                            psycopg2.extras.Json(attempt.error_json),
                            attempt.started_at,
                            attempt.finished_at,
                            day["day_run_id"],
                            day["row_version"],
                        ),
                    )
                    if cur.rowcount != 1:
                        raise HistoricalRangeContractError(
                            REASON_ROW_VERSION_CONFLICT,
                            "day changed while being cancelled",
                            context={"day_run_id": day["day_run_id"]},
                        )
                    touched_runs.add(str(day["range_run_id"]))
                cur.execute(
                    "SELECT range_run_id, day_plan_cursor_ordinal FROM app.advisory_historical_range_run WHERE batch_id = %s",
                    (batch_id,),
                )
                run_rows = tuple(dict(item) for item in cur.fetchall())
                total = int(batch["trade_date_count"])
                for run in run_rows:
                    if int(run["day_plan_cursor_ordinal"]) < total:
                        cur.execute(
                            """
                            UPDATE app.advisory_historical_range_run
                            SET cancelled_from_ordinal = COALESCE(cancelled_from_ordinal, %s),
                                row_version = row_version + 1
                            WHERE range_run_id = %s
                            """,
                            (int(run["day_plan_cursor_ordinal"]) + 1, run["range_run_id"]),
                        )
                    touched_runs.add(str(run["range_run_id"]))
                for range_run_id in sorted(touched_runs):
                    self._sync_run_aggregate(cur=cur, range_run_id=range_run_id)
                aggregate = self._batch_aggregate(cur=cur, batch_id=batch_id)
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_batch
                    SET status = 'CANCELLING', row_version = row_version + 1,
                        successful_day_count = %s, terminal_failed_day_count = %s,
                        completed_program_count = %s, failed_program_count = %s,
                        waiting_program_count = %s, retryable_program_count = %s,
                        partial_program_count = %s, recoverable_program_count = %s,
                        started_at = COALESCE(started_at, clock_timestamp())
                    WHERE batch_id = %s AND row_version = %s
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
                        expected_batch_row_version,
                    ),
                )
                if cur.rowcount != 1:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "batch changed while committing CANCELLING aggregate",
                        context={"batch_id": batch_id},
                    )
        return tuple(sorted(touched_runs))

    def load_run_finalization_facts(self, *, range_run_id: str) -> HistoricalRangeRunFinalizationFacts:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run.batch_id, run.range_run_id, run.research_program_id,
                           run.status, run.row_version, run.materialized_day_count,
                           run.day_plan_cursor_ordinal, run.final_receipt_ref,
                           run.final_receipt_hash, run.cancelled_from_ordinal,
                           batch.request_payload_sha256,
                           batch.trade_date_count
                    FROM app.advisory_historical_range_run AS run
                    JOIN app.advisory_historical_range_batch AS batch ON batch.batch_id = run.batch_id
                    WHERE run.range_run_id = %s
                    """,
                    (range_run_id,),
                )
                run_row = cur.fetchone()
                if run_row is None:
                    raise self._repository_error("range run does not exist", range_run_id=range_run_id)
                run_data = dict(run_row)
                cur.execute(
                    """
                    SELECT day_run_id, ordinal, decision_trade_date, status
                    FROM app.advisory_historical_range_day_run
                    WHERE range_run_id = %s
                    ORDER BY ordinal
                    """,
                    (range_run_id,),
                )
                day_rows = tuple(dict(item) for item in cur.fetchall())
        run = HistoricalRangeExecutionRunV1(
            batch_id=str(run_data["batch_id"]),
            range_run_id=str(run_data["range_run_id"]),
            research_program_id=str(run_data["research_program_id"]),
            status=HistoricalRangeProgramStatus(str(run_data["status"])),
            row_version=int(run_data["row_version"]),
            materialized_day_count=int(run_data["materialized_day_count"]),
            day_plan_cursor_ordinal=int(run_data["day_plan_cursor_ordinal"]),
            final_receipt_ref=(
                HistoricalRangeArtifactRefV1.model_validate(run_data["final_receipt_ref"])
                if run_data["final_receipt_ref"] is not None
                else None
            ),
            final_receipt_hash=(str(run_data["final_receipt_hash"]) if run_data["final_receipt_hash"] else None),
        )
        successful: list[HistoricalRangeSuccessfulDayReadbackV1] = []
        blocking: dict[str, Any] | None = None
        success_statuses = {
            HistoricalRangeDayStatus.COMPLETE.value,
            HistoricalRangeDayStatus.VALID_NO_CANDIDATE.value,
        }
        for expected_ordinal, day in enumerate(day_rows, start=1):
            if int(day["ordinal"]) != expected_ordinal:
                raise HistoricalRangeContractError(
                    REASON_REPOSITORY_CONFLICT,
                    "materialized day ordinals are not contiguous",
                    context={"range_run_id": range_run_id},
                )
            if blocking is None and str(day["status"]) in success_statuses:
                readback = self.full_readback_successful_day(day_run_id=str(day["day_run_id"]))
                if successful:
                    previous = successful[-1]
                    if (
                        readback.receipt.previous_day_receipt_ref != previous.receipt_ref
                        or readback.receipt.list_version.previous_list_version_id
                        != previous.receipt.list_version.list_version_id
                        or readback.receipt.list_version.previous_list_hash
                        != previous.receipt.list_version.list_content_hash
                        or readback.receipt.list_version.previous_day_receipt_hash
                        != previous.receipt_ref.semantic_content_hash
                    ):
                        raise HistoricalRangeContractError(
                            REASON_REPOSITORY_CONFLICT,
                            "successful day receipt chain has a non-exact predecessor edge",
                            context={"range_run_id": range_run_id, "ordinal": expected_ordinal},
                        )
                elif (
                    readback.receipt.previous_day_receipt_ref is not None
                    or readback.receipt.list_version.previous_list_version_id is not None
                ):
                    raise HistoricalRangeContractError(
                        REASON_REPOSITORY_CONFLICT,
                        "first successful day unexpectedly carries predecessor state",
                        context={"range_run_id": range_run_id},
                    )
                successful.append(readback)
                continue
            if blocking is None:
                blocking = day
            elif str(day["status"]) in success_statuses:
                raise HistoricalRangeContractError(
                    REASON_REPOSITORY_CONFLICT,
                    "range run contains a successful day after a blocking ordinal",
                    context={"range_run_id": range_run_id, "ordinal": expected_ordinal},
                )
        blocking_ref: HistoricalRangeArtifactRefV1 | None = None
        blocking_status: HistoricalRangeDayStatus | None = None
        blocking_ordinal: int | None = None
        blocking_day_run_id: str | None = None
        if blocking is not None:
            blocking_day_run_id = str(blocking["day_run_id"])
            blocking_ordinal = int(blocking["ordinal"])
            blocking_status = HistoricalRangeDayStatus(str(blocking["status"]))
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT attempt_receipt_ref
                        FROM app.advisory_historical_range_day_attempt
                        WHERE day_run_id = %s AND status = %s
                        ORDER BY attempt_no DESC
                        LIMIT 1
                        """,
                        (blocking_day_run_id, blocking_status.value),
                    )
                    attempt_row = cur.fetchone()
            if attempt_row is not None and attempt_row["attempt_receipt_ref"] is not None:
                blocking_ref = HistoricalRangeArtifactRefV1.model_validate(attempt_row["attempt_receipt_ref"])
        executed_count = len(successful) + (
            1
            if blocking_status
            in {HistoricalRangeDayStatus.FAILED, HistoricalRangeDayStatus.CANCELLED}
            else 0
        )
        total_day_count = int(run_data["trade_date_count"])
        return HistoricalRangeRunFinalizationFacts(
            run=run,
            resolved_request_hash=str(run_data["request_payload_sha256"]),
            total_day_count=total_day_count,
            successful_days=tuple(successful),
            blocking_day_run_id=blocking_day_run_id,
            blocking_ordinal=blocking_ordinal,
            blocking_trade_date=(blocking["decision_trade_date"] if blocking is not None else None),
            blocking_status=blocking_status,
            blocking_attempt_receipt_ref=blocking_ref,
            unexecuted_day_count=max(total_day_count - executed_count, 0),
            cancelled_from_ordinal=(
                int(run_data["cancelled_from_ordinal"])
                if run_data["cancelled_from_ordinal"] is not None
                else None
            ),
        )

    def finish_failed_day(
        self,
        *,
        claimed_day: HistoricalRangeClaimedDayV1,
        target_status: HistoricalRangeDayStatus,
        attempt: HistoricalRangeDayAttemptV1,
        reason_codes: Sequence[str],
        error_json: dict[str, Any],
    ) -> dict[str, Any]:
        if target_status not in {
            HistoricalRangeDayStatus.WAITING_INPUT,
            HistoricalRangeDayStatus.RETRYABLE_FAILED,
            HistoricalRangeDayStatus.FAILED,
            HistoricalRangeDayStatus.CANCELLED,
        }:
            raise ValueError("finish_failed_day requires a non-success terminal or recoverable status")
        day_identity = self._get_day_artifact_identity(claimed_day.day_run_id)
        range_run_id = day_identity["range_run_id"]
        resolved_request_hash = claimed_day.resolved_request_hash
        if str(range_run_id) != claimed_day.range_run_id:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "claimed day range identity differs from durable state",
                context={"day_run_id": claimed_day.day_run_id},
            )
        self._validate_day_attempt_artifacts(
            attempt=attempt,
            range_run_id=claimed_day.range_run_id,
            resolved_request_hash=resolved_request_hash,
        )
        if (
            attempt.day_run_id != claimed_day.day_run_id
            or attempt.attempt_no != claimed_day.attempt_no
            or attempt.worker_id != claimed_day.worker_id
            or attempt.lease_token != claimed_day.lease_token
            or attempt.fencing_token != claimed_day.fencing_token
            or attempt.status != target_status.value
            or attempt.reason_codes != tuple(sorted(reason_codes))
            or canonicalize(attempt.error_json) != canonicalize(error_json)
        ):
            raise ValueError("failed day attempt does not close the claimed worker/fencing/error identity")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                day = self._lock_row(
                    cur,
                    table="advisory_historical_range_day_run",
                    key_name="day_run_id",
                    key_value=claimed_day.day_run_id,
                )
                self._require_row_version(day, claimed_day.row_version, entity="day", identity=claimed_day.day_run_id)
                if (
                    str(day["status"]) != HistoricalRangeDayStatus.RUNNING.value
                    or int(day.get("attempt_no") or 0) != claimed_day.attempt_no
                    or day.get("worker_id") != claimed_day.worker_id
                    or day.get("lease_token") != claimed_day.lease_token
                    or int(day.get("current_fencing_token") or 0) != claimed_day.fencing_token
                ):
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "failed day commit lost its active worker/lease/fencing ownership",
                        context={"day_run_id": claimed_day.day_run_id},
                    )
                self._insert_day_attempt(cur=cur, attempt=attempt)
                terminal = target_status in {HistoricalRangeDayStatus.FAILED, HistoricalRangeDayStatus.CANCELLED}
                cur.execute(
                    """
                    UPDATE app.advisory_historical_range_day_run
                    SET status = %s,
                        row_version = row_version + 1,
                        worker_id = NULL,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        reason_codes_json = %s,
                        error_json = %s,
                        finished_at = CASE WHEN %s THEN clock_timestamp() ELSE NULL END
                    WHERE day_run_id = %s
                      AND row_version = %s
                      AND current_fencing_token = %s
                      AND worker_id = %s
                      AND lease_token = %s
                    RETURNING *
                    """,
                    (
                        target_status.value,
                        psycopg2.extras.Json(list(reason_codes)),
                        psycopg2.extras.Json(error_json),
                        terminal,
                        claimed_day.day_run_id,
                        claimed_day.row_version,
                        claimed_day.fencing_token,
                        claimed_day.worker_id,
                        claimed_day.lease_token,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "failed day update lost its fencing ownership",
                        context={"day_run_id": claimed_day.day_run_id},
                    )
                self._sync_run_aggregate(cur=cur, range_run_id=claimed_day.range_run_id)
                self._sync_batch_aggregate(cur=cur, batch_id=claimed_day.batch_id)
                return dict(updated)

    def append_day_attempt(self, attempt: HistoricalRangeDayAttemptV1) -> bool:
        day_identity = self._get_day_artifact_identity(attempt.day_run_id)
        range_run_id = str(day_identity["range_run_id"])
        resolved_request_hash = str(day_identity["request_payload_sha256"])
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
                        waiting_stage = %s,
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
                        "DAY_INPUT" if target_status is HistoricalRangeBatchStatus.WAITING_INPUT else None,
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
        } or (target_status is HistoricalRangeProgramStatus.PARTIAL and final_receipt_ref is not None)
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
                if (
                    str(current["status"]) == HistoricalRangeProgramStatus.PARTIAL.value
                    and current["finished_at"] is not None
                    and current["final_receipt_ref"] is not None
                ):
                    raise self._repository_error("finished PARTIAL range run is immutable", range_run_id=range_run_id)
                require_state_transition(
                    HistoricalRangeProgramStatus(str(current["status"])),
                    target_status,
                    PROGRAM_TRANSITIONS,
                    entity="Program range run",
                )
                aggregate = self._run_aggregate(cur=cur, range_run_id=range_run_id)
                if terminal and aggregate["active_nonterminal_day_count"] != 0:
                    raise self._repository_error(
                        "terminal range run still contains active non-terminal materialized days",
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

    def finish_range_run(
        self,
        *,
        range_run_id: str,
        expected_row_version: int,
        target_status: HistoricalRangeProgramStatus,
        receipt: HistoricalRangeRunExecutionReceiptV1,
        final_receipt_ref: HistoricalRangeArtifactRefV1,
    ) -> dict[str, Any]:
        if receipt.range_run_id != range_run_id or receipt.status != target_status.value:
            raise ValueError("run execution receipt differs from the requested terminal transition")
        envelope = self._load_artifact(
            final_receipt_ref,
            expected_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
            resolved_request_hash=receipt.resolved_request_hash,
            range_run_id=range_run_id,
            expected_payload=receipt.model_dump(mode="json"),
        )
        expected_upstream = receipt.ordered_success_day_receipt_refs + (
            (receipt.blocking_attempt_receipt_ref,) if receipt.blocking_attempt_receipt_ref is not None else ()
        )
        expected_upstream = tuple(
            sorted(expected_upstream, key=lambda ref: (ref.artifact_kind.value, ref.semantic_content_hash, ref.relative_path))
        )
        if tuple(envelope.upstream_refs) != expected_upstream:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "RANGE_RECEIPT upstream set differs from the typed run receipt payload",
                context={"range_run_id": range_run_id},
            )
        return self.transition_run(
            range_run_id=range_run_id,
            expected_row_version=expected_row_version,
            target_status=target_status,
            final_receipt_ref=final_receipt_ref,
        )

    def transition_day(
        self,
        *,
        day_run_id: str,
        expected_row_version: int,
        target_status: HistoricalRangeDayStatus,
        attempt_no: int,
        worker_id: str | None = None,
        lease_token: str | None = None,
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
        if target_status is HistoricalRangeDayStatus.RUNNING:
            worker_id = str(worker_id or "").strip()
            lease_token = str(lease_token or "").strip()
            if not worker_id or not lease_token:
                raise ValueError("RUNNING day transition requires worker_id and lease_token")
        elif worker_id is not None or lease_token is not None:
            raise ValueError("non-running day transition cannot retain worker_id or lease_token")
        day_identity = self._get_day_artifact_identity(day_run_id)
        range_run_id = str(day_identity["range_run_id"])
        resolved_request_hash = str(day_identity["request_payload_sha256"])
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
                        if worker_id != current.get("worker_id") or lease_token != current.get("lease_token"):
                            raise ValueError("day heartbeat cannot change worker/lease identity")
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
                        worker_id = %s,
                        lease_token = %s,
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
                        worker_id,
                        lease_token,
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
                        if worker_id != current.get("worker_id") or lease_token != current.get("lease_token"):
                            raise ValueError("operation heartbeat cannot change worker/lease identity")
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
        decision_mark_set_ref: HistoricalRangeArtifactRefV1 | None = None,
        previous_day_receipt_ref: HistoricalRangeArtifactRefV1 | None = None,
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
        day_identity = self._get_day_artifact_identity(day_run_id)
        range_run_id = str(day_identity["range_run_id"])
        resolved_request_hash = str(day_identity["request_payload_sha256"])
        candidate_envelope = self._load_artifact(
            candidate_artifact_ref,
            expected_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
            resolved_request_hash=resolved_request_hash,
            range_run_id=range_run_id,
            day_run_id=day_run_id,
        )
        candidate_payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(candidate_envelope.payload)
        if (
            candidate_payload.range_run_id != range_run_id
            or candidate_payload.day_run_id != day_run_id
            or candidate_payload.research_program_id != str(day_identity["research_program_id"])
            or candidate_payload.decision_trade_date != day_identity["decision_trade_date"]
        ):
            raise ValueError("candidate artifact day/Program identity differs from the repository day")
        if candidate_payload.source_revision_refs != candidate_envelope.source_revision_refs:
            raise ValueError("candidate artifact payload/envelope source revision refs differ")
        canonical_candidates = tuple(sorted(candidates, key=lambda item: (item.symbol, item.candidate_id)))
        if candidate_payload.candidates != canonical_candidates:
            raise ValueError("candidate artifact facts differ from the canonical day candidates")
        expected_outcome = (
            "CANDIDATES_AVAILABLE" if terminal_status is HistoricalRangeDayStatus.COMPLETE else "VALID_NO_CANDIDATE"
        )
        if candidate_payload.candidate_outcome != expected_outcome:
            raise ValueError("candidate artifact outcome differs from the successful day status")
        request_ref = HistoricalRangeArtifactRefV1.model_validate(day_identity["request_artifact_ref"])
        request_envelope = self._load_artifact(
            request_ref,
            expected_kind=HistoricalRangeArtifactKind.REQUEST,
            resolved_request_hash=resolved_request_hash,
        )
        request_payload = HistoricalRangeResolvedRequestArtifactPayloadV1.model_validate(request_envelope.payload)
        frozen_program = next(
            (
                item
                for item in request_payload.resolved_request.frozen_programs
                if item.research_program_id == candidate_payload.research_program_id
            ),
            None,
        )
        if frozen_program is None:
            raise ValueError("candidate artifact Program is absent from the sealed request")
        frozen_identity = {
            "package_id": frozen_program.package_id,
            "package_version": frozen_program.package_version,
            "manifest_sha256": frozen_program.manifest_sha256,
            "alpha_mode": frozen_program.alpha_mode,
            "selection_semantics_hash": frozen_program.selection_semantics_hash,
            "code_release_hash": frozen_program.code_release_hash,
        }
        candidate_identity = {
            "package_id": candidate_payload.package_id,
            "package_version": candidate_payload.package_version,
            "manifest_sha256": candidate_payload.manifest_sha256,
            "alpha_mode": candidate_payload.alpha_mode,
            "selection_semantics_hash": candidate_payload.selection_semantics_hash,
            "code_release_hash": candidate_payload.code_release_hash,
        }
        if candidate_identity != frozen_identity:
            raise ValueError("candidate artifact package/code semantics differ from the frozen Program")
        catalog = request_payload.source_revision_catalog
        if candidate_payload.calendar_identity_hash != catalog.calendar_identity_hash:
            raise ValueError("candidate artifact calendar identity differs from the sealed catalog")
        component_ids = {item.component_id for item in frozen_program.admitted_package_projection.components}
        r3_evidence = decision_mark_set_ref is not None
        source_role_selection = None
        if r3_evidence:
            source_role_selection = select_day_source_roles(
                catalog=catalog,
                research_program_id=frozen_program.research_program_id,
                package_id=frozen_program.package_id,
                component_ids=component_ids,
                decision_trade_date=candidate_payload.decision_trade_date,
            )
            expected_members = source_role_selection.candidate_members
        else:
            expected_members = tuple(
                member
                for member in catalog.members
                if member.decision_trade_date in {None, candidate_payload.decision_trade_date}
                and member.package_id in {None, frozen_program.package_id}
                and member.component_id in {None, *component_ids}
            )
        expected_refs = {
            (str(item.revision_id), str(item.revision_hash))
            for item in expected_members
        }
        candidate_refs = {(item.revision_id, item.revision_hash) for item in candidate_payload.source_revision_refs}
        if not expected_refs or candidate_refs != expected_refs:
            raise ValueError("candidate artifact source refs do not equal the sealed Program/day catalog members")
        mark_payload: HistoricalRangeDecisionMarkSetV1 | None = None
        if decision_mark_set_ref is not None:
            if source_role_selection is None:
                raise AssertionError("R3 mark evidence requires an exact source-role selection")
            mark_envelope = self._load_artifact(
                decision_mark_set_ref,
                expected_kind=HistoricalRangeArtifactKind.DECISION_MARK_SET,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=day_run_id,
                exact_upstream_refs=(
                    request_ref,
                    *((previous_day_receipt_ref,) if previous_day_receipt_ref is not None else ()),
                ),
                allow_direct_predecessor_day_run_id=(
                    str(day_identity.get("previous_day_run_id"))
                    if day_identity.get("previous_day_run_id") is not None
                    else None
                ),
            )
            mark_payload = HistoricalRangeDecisionMarkSetV1.model_validate(mark_envelope.payload)
            expected_mark_refs = {
                (item.revision_id, item.revision_hash)
                for item in source_role_selection.decision_mark_members
            }
            actual_mark_refs = {(item.revision_id, item.revision_hash) for item in mark_payload.source_revision_refs}
            if (
                mark_payload.range_run_id != range_run_id
                or mark_payload.day_run_id != day_run_id
                or mark_payload.decision_trade_date != candidate_payload.decision_trade_date
                or mark_payload.source_revision_refs != mark_envelope.source_revision_refs
                or not expected_mark_refs
                or actual_mark_refs != expected_mark_refs
                or mark_payload.upstream_request_ref != request_ref
                or mark_payload.predecessor_day_receipt_ref != previous_day_receipt_ref
            ):
                raise ValueError("decision-mark artifact does not close the exact R3 Program/day evidence")
        expected_candidate_input_hash = build_candidate_input_hash(
            range_run_id=range_run_id,
            research_program_id=candidate_payload.research_program_id,
            decision_trade_date=candidate_payload.decision_trade_date,
            frozen_program_hash=str(frozen_program.frozen_program_hash),
            runtime_profile_hash=candidate_payload.runtime_profile_hash,
            code_release_hash=frozen_program.code_release_hash,
            selection_semantics_hash=frozen_program.selection_semantics_hash,
            calendar_identity_hash=catalog.calendar_identity_hash,
            universe_identity_hash=candidate_payload.universe_identity_hash,
            source_revision_catalog_hash=str(catalog.catalog_hash),
            query_contract_hash=catalog.query_contract_hash,
        )
        if candidate_payload.candidate_input_hash != expected_candidate_input_hash:
            raise ValueError("candidate_input_hash does not close the sealed Program/day inputs")
        if r3_evidence:
            if decision_mark_set_ref.artifact_kind is not HistoricalRangeArtifactKind.DECISION_MARK_SET:
                raise ValueError("decision_mark_set_ref must reference DECISION_MARK_SET")
            predecessor_receipt_hash = (
                previous_day_receipt_ref.semantic_content_hash if previous_day_receipt_ref else None
            )
            if list_version.previous_day_receipt_hash != predecessor_receipt_hash:
                raise ValueError("R3 predecessor receipt hash must equal the exact predecessor receipt ref")
            if (previous_day_receipt_ref is None) != (list_version.previous_list_hash is None):
                raise ValueError("R3 predecessor list/receipt refs must be supplied together")
            expected_day_input_hash = build_day_input_hash_v3(
                candidate_input_hash=candidate_payload.candidate_input_hash,
                candidate_artifact_ref=candidate_artifact_ref,
                decision_mark_set_ref=decision_mark_set_ref,
                decision_mark_policy_hash=str(mark_payload.mark_policy_hash),
                previous_list_hash=list_version.previous_list_hash,
                previous_day_receipt_ref=previous_day_receipt_ref,
                list_semantics_version=str(day_identity["list_semantics_version"]),
                list_semantics_hash=str(day_identity["list_semantics_hash"]),
            )
        else:
            if previous_day_receipt_ref is not None:
                raise ValueError("legacy successful-day commit cannot supply an R3 predecessor receipt ref")
            expected_day_input_hash = build_day_input_hash(
                candidate_input_hash=candidate_payload.candidate_input_hash,
                candidate_artifact_ref=candidate_artifact_ref,
                previous_list_hash=list_version.previous_list_hash,
                previous_day_receipt_hash=list_version.previous_day_receipt_hash,
                list_semantics_hash=str(day_identity["list_semantics_hash"]),
            )
        if day_input_hash != expected_day_input_hash:
            raise ValueError("day_input_hash does not derive from the candidate artifact and list predecessor")
        if r3_evidence:
            day_receipt_payload = build_day_receipt_payload_v2(
                range_run_id=range_run_id,
                day_run_id=day_run_id,
                terminal_status=terminal_status,
                day_input_hash=day_input_hash,
                candidate_artifact_ref=candidate_artifact_ref,
                decision_mark_set_ref=decision_mark_set_ref,
                previous_day_receipt_ref=previous_day_receipt_ref,
                list_version=list_version,
                items=items,
                episodes=episodes,
                reason_codes=reason_codes,
            )
        else:
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
        required_day_receipt_upstream = (candidate_artifact_ref,)
        if r3_evidence:
            required_day_receipt_upstream = (
                candidate_artifact_ref,
                decision_mark_set_ref,
                *((previous_day_receipt_ref,) if previous_day_receipt_ref is not None else ()),
            )
        day_receipt_envelope = self._load_artifact(
            day_receipt_ref,
            expected_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
            resolved_request_hash=resolved_request_hash,
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            expected_payload=day_receipt_payload,
            required_upstream_refs=required_day_receipt_upstream,
            exact_upstream_refs=required_day_receipt_upstream if r3_evidence else None,
            allow_direct_predecessor_day_run_id=(
                str(day_identity.get("previous_day_run_id"))
                if r3_evidence and day_identity.get("previous_day_run_id") is not None
                else None
            ),
            validate_recursive_upstream=not r3_evidence,
        )
        if r3_evidence:
            parsed_receipt = HistoricalRangeDayReceiptPayloadV2.model_validate(day_receipt_envelope.payload)
            if (
                parsed_receipt.candidate_artifact_ref != candidate_artifact_ref
                or parsed_receipt.decision_mark_set_ref != decision_mark_set_ref
                or parsed_receipt.previous_day_receipt_ref != previous_day_receipt_ref
                or parsed_receipt.day_input_hash != day_input_hash
            ):
                raise ValueError("R3 successful day receipt does not close its exact candidate/mark/predecessor edges")
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
                if r3_evidence and (
                    day.get("worker_id") != attempt.worker_id or day.get("lease_token") != attempt.lease_token
                ):
                    raise HistoricalRangeContractError(
                        REASON_ROW_VERSION_CONFLICT,
                        "successful day attempt worker/lease differs from the active durable claim",
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
                        worker_id = NULL,
                        lease_token = NULL,
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

    def _get_day_artifact_identity(self, day_run_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT day.range_run_id, day.decision_trade_date, day.previous_day_run_id,
                           run.research_program_id,
                           batch.request_payload_sha256,
                           batch.request_artifact_ref,
                           batch.list_semantics_version,
                           batch.list_semantics_hash
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
        if row["request_payload_sha256"] is None or row["request_artifact_ref"] is None:
            raise self._repository_error("day run belongs to an unsealed batch", day_run_id=day_run_id)
        return dict(row)

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
        day_identity = self._get_day_artifact_identity(attempt.day_run_id)
        request_ref = HistoricalRangeArtifactRefV1.model_validate(day_identity["request_artifact_ref"])
        if attempt.candidate_artifact_ref is not None:
            self._load_artifact(
                attempt.candidate_artifact_ref,
                expected_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=attempt.day_run_id,
            )
        if attempt.attempt_receipt_ref is not None:
            receipt_header = self._artifact_store.load(attempt.attempt_receipt_ref)
            envelope = self._load_artifact(
                attempt.attempt_receipt_ref,
                expected_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=attempt.day_run_id,
                allow_direct_predecessor_day_run_id=(
                    str(day_identity["previous_day_run_id"])
                    if day_identity.get("previous_day_run_id") is not None
                    else None
                ),
                validate_recursive_upstream=receipt_header.payload_schema_version
                not in {
                    DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION,
                    DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2,
                },
            )
            if envelope.payload_schema_version == DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION:
                receipt = HistoricalRangeDayAttemptReceiptPayloadV1.model_validate(envelope.payload)
                if (
                    receipt.day_run_id != attempt.day_run_id
                    or receipt.attempt_no != attempt.attempt_no
                    or receipt.fencing_token != attempt.fencing_token
                    or receipt.worker_id != attempt.worker_id
                    or receipt.lease_token_hash != sha256(attempt.lease_token.encode("utf-8")).hexdigest()
                    or receipt.status != attempt.status
                    or receipt.attempt_input_hash != attempt.input_hash
                    or receipt.candidate_artifact_ref != attempt.candidate_artifact_ref
                    or receipt.reason_codes != attempt.reason_codes
                    or canonicalize(receipt.sanitized_error) != canonicalize(attempt.error_json)
                ):
                    raise ValueError("typed day failure receipt differs from its attempt row")
                expected_upstream = (request_ref,) + (
                    (attempt.candidate_artifact_ref,) if attempt.candidate_artifact_ref is not None else ()
                ) + ((receipt.decision_mark_set_ref,) if receipt.decision_mark_set_ref is not None else ()) + (
                    (receipt.previous_day_receipt_ref,) if receipt.previous_day_receipt_ref is not None else ()
                )
                self._require_exact_upstream_refs(envelope=envelope, expected_refs=expected_upstream)
            elif envelope.payload_schema_version == DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2:
                receipt = HistoricalRangeDayReceiptPayloadV2.model_validate(envelope.payload)
                if (
                    receipt.day_run_id != attempt.day_run_id
                    or receipt.terminal_status != attempt.status
                    or receipt.day_input_hash != attempt.input_hash
                    or receipt.candidate_artifact_ref != attempt.candidate_artifact_ref
                    or attempt.result_hash != attempt.attempt_receipt_ref.semantic_content_hash
                ):
                    raise ValueError("typed R3 successful day receipt differs from its attempt row")
                self._require_exact_upstream_refs(
                    envelope=envelope,
                    expected_refs=(receipt.candidate_artifact_ref, receipt.decision_mark_set_ref)
                    + ((receipt.previous_day_receipt_ref,) if receipt.previous_day_receipt_ref is not None else ()),
                )
            elif attempt.candidate_artifact_ref is not None:
                # Retained R1 receipt compatibility; R3 paths must use one of
                # the typed receipt schemas above.
                self._require_exact_upstream_refs(
                    envelope=envelope,
                    expected_refs=(attempt.candidate_artifact_ref,),
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
            or attempt.worker_id != current.get("worker_id")
            or attempt.lease_token != current.get("lease_token")
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
            or (
                str(current["status"]) == HistoricalRangeDayStatus.RUNNING.value
                and (
                    attempt.worker_id != current.get("worker_id")
                    or attempt.lease_token != current.get("lease_token")
                )
            )
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
                   )::INTEGER AS nonterminal_day_count,
                   COUNT(*) FILTER (
                       WHERE status IN ('RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED')
                   )::INTEGER AS active_nonterminal_day_count
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
                    WHERE run.status IN ('QUEUED', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED')
                       OR (run.status = 'PARTIAL' AND run.finished_at IS NULL)
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
        # Serialize aggregate refreshes for runs in the same batch. Without
        # this lock, another run can commit between the aggregate SELECT and
        # UPDATE, so the batch trigger observes a newer child snapshot than
        # the values being written.
        cur.execute(
            "SELECT batch_id FROM app.advisory_historical_range_batch WHERE batch_id = %s FOR UPDATE",
            (batch_id,),
        )
        if cur.fetchone() is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "historical-range batch does not exist",
                context={"batch_id": batch_id},
            )
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

    def _validate_planning_artifact(
        self,
        *,
        plan: HistoricalRangeSourceRequirementPlanV1,
        artifacts: HistoricalRangePlanningArtifactBindingsV1,
    ) -> None:
        if artifacts.artifact_root_identity_hash != self._artifact_store.root_identity_hash:
            raise ValueError("artifact_root_identity_hash differs from the configured artifact store")
        envelope = self._artifact_store.load_planning(artifacts.requirement_plan_ref)
        if (
            envelope.artifact_kind is not HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN
            or envelope.planning_identity_hash != plan.planning_identity_hash
            or envelope.batch_id != plan.batch_id
            or envelope.catalog_generation != 1
            or canonicalize(envelope.payload) != canonicalize(plan.model_dump(mode="json"))
        ):
            raise ValueError("requirement plan artifact differs from the planning request")

    @staticmethod
    def _planning_operation_request(
        *,
        plan: HistoricalRangeSourceRequirementPlanV1,
        operation_type: HistoricalRangeOperationType,
    ) -> HistoricalRangeOperationRequestV1:
        operation_key = f"{operation_type.value.lower()}:{plan.request.client_idempotency_key}"
        return HistoricalRangeOperationRequestV1(
            operation_id=derive_prefixed_id(
                "ahrop",
                {
                    "batch_id": plan.batch_id,
                    "operation_type": operation_type.value,
                    "operation_idempotency_key": operation_key,
                    "planning_identity_hash": plan.planning_identity_hash,
                },
            ),
            batch_id=plan.batch_id,
            operation_type=operation_type,
            operation_idempotency_key=operation_key,
            planning_identity_hash=plan.planning_identity_hash,
            expected_row_version=1,
        )

    @staticmethod
    def _find_planning_batch_by_key(
        *,
        cur: Any,
        client_idempotency_key: str,
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
            (client_idempotency_key,),
        )
        row = cur.fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def _assert_planning_batch_matches(
        *,
        existing: dict[str, Any],
        plan: HistoricalRangeSourceRequirementPlanV1,
        artifacts: HistoricalRangePlanningArtifactBindingsV1 | None,
    ) -> None:
        expected = {
            "batch_id": plan.batch_id,
            "request_id": plan.request.request_id,
            "client_idempotency_key": plan.request.client_idempotency_key,
            "user_request_semantic_hash": plan.request.user_request_semantic_hash,
            "planning_identity_hash": plan.planning_identity_hash,
            "requirement_plan_hash": plan.requirement_plan_hash,
            "start_trade_date": plan.date_plan.start_trade_date,
            "end_trade_date": plan.date_plan.end_trade_date,
            "calendar_id": plan.date_plan.calendar_id,
            "calendar_version": plan.date_plan.calendar_version,
            "ordered_trade_dates_hash": plan.date_plan.ordered_trade_dates_hash,
            "program_count": len(plan.frozen_programs),
            "trade_date_count": len(plan.date_plan.ordered_trade_dates),
            "planned_day_count": len(plan.frozen_programs) * len(plan.date_plan.ordered_trade_dates),
        }
        mismatches = {
            key: {"expected": value, "actual": existing.get(key)}
            for key, value in expected.items()
            if existing.get(key) != value
        }
        if artifacts is not None:
            expected_ref = artifacts.requirement_plan_ref.model_dump(mode="json")
            if canonicalize(existing.get("requirement_plan_ref")) != canonicalize(expected_ref):
                mismatches["requirement_plan_ref"] = {
                    "expected": expected_ref,
                    "actual": existing.get("requirement_plan_ref"),
                }
            if existing.get("requirement_plan_artifact_hash") != artifacts.requirement_plan_ref.semantic_content_hash:
                mismatches["requirement_plan_artifact_hash"] = {
                    "expected": artifacts.requirement_plan_ref.semantic_content_hash,
                    "actual": existing.get("requirement_plan_artifact_hash"),
                }
            if existing.get("artifact_root_identity_hash") != artifacts.artifact_root_identity_hash:
                mismatches["artifact_root_identity_hash"] = {
                    "expected": artifacts.artifact_root_identity_hash,
                    "actual": existing.get("artifact_root_identity_hash"),
                }
        if mismatches:
            raise HistoricalRangeContractError(
                REASON_IDEMPOTENCY_CONFLICT,
                "same client idempotency key resolved to different planning semantics",
                context={
                    "client_idempotency_key": plan.request.client_idempotency_key,
                    "existing_batch_id": existing.get("batch_id"),
                    "mismatches": mismatches,
                },
            )

    @staticmethod
    def _planning_batch_result(
        *,
        cur: Any,
        batch_id: str,
        idempotent: bool,
    ) -> CreatedHistoricalRangePlanningBatch:
        cur.execute(
            """
            SELECT operation_id, operation_type
            FROM app.advisory_historical_range_operation
            WHERE batch_id = %s AND operation_type IN ('CREATE', 'BUILD_SOURCE_CATALOG')
            """,
            (batch_id,),
        )
        operations = {str(row["operation_type"]): str(row["operation_id"]) for row in cur.fetchall()}
        if set(operations) != {"CREATE", "BUILD_SOURCE_CATALOG"}:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "planning batch does not contain its required operations",
                context={"batch_id": batch_id, "operation_types": sorted(operations)},
            )
        return CreatedHistoricalRangePlanningBatch(
            batch_id=batch_id,
            create_operation_id=operations["CREATE"],
            catalog_operation_id=operations["BUILD_SOURCE_CATALOG"],
            idempotent=idempotent,
        )

    @staticmethod
    def _exact_sealed_batch_result(
        *,
        cur: Any,
        batch: dict[str, Any],
        resolved: ResolvedHistoricalRangeRequestV1,
        catalog: HistoricalRangeSourceRevisionCatalogV1,
        artifacts: HistoricalRangeArtifactBindingsV1,
    ) -> SealedHistoricalRangeBatch:
        batch_id = str(batch["batch_id"])
        status = str(batch["status"])
        if status == HistoricalRangeBatchStatus.DEDUPLICATED.value:
            canonical_batch_id = str(batch.get("canonical_batch_id") or "")
            if (
                not canonical_batch_id
                or batch.get("deduplicated_request_payload_sha256") != resolved.request_payload_sha256
                or batch.get("request_payload_sha256") is not None
                or batch.get("sealed_at") is not None
            ):
                raise HistoricalRangeContractError(
                    REASON_IDEMPOTENCY_CONFLICT,
                    "deduplicated planning batch differs from the seal retry",
                    context={"batch_id": batch_id},
                )
            cur.execute(
                """
                SELECT request_payload_sha256, user_request_semantic_hash, status
                FROM app.advisory_historical_range_batch
                WHERE batch_id = %s
                """,
                (canonical_batch_id,),
            )
            canonical = cur.fetchone()
            if (
                canonical is None
                or canonical["request_payload_sha256"] != resolved.request_payload_sha256
                or canonical["user_request_semantic_hash"] != resolved.request.user_request_semantic_hash
                or str(canonical["status"]) == HistoricalRangeBatchStatus.DEDUPLICATED.value
            ):
                raise HistoricalRangeContractError(
                    REASON_IDEMPOTENCY_CONFLICT,
                    "deduplicated planning batch canonical target differs from the seal retry",
                    context={"batch_id": batch_id, "canonical_batch_id": canonical_batch_id},
                )
            return SealedHistoricalRangeBatch(
                batch_id=batch_id,
                canonical_batch_id=canonical_batch_id,
                range_run_ids=(),
                deduplicated=True,
            )

        expected = {
            "request_payload_sha256": resolved.request_payload_sha256,
            "request_artifact_ref": artifacts.request_ref.model_dump(mode="json"),
            "request_artifact_hash": artifacts.request_ref.semantic_content_hash,
            "date_plan_ref": artifacts.date_plan_ref.model_dump(mode="json"),
            "date_plan_hash": artifacts.date_plan_ref.semantic_content_hash,
            "source_revision_catalog_ref": artifacts.request_ref.model_dump(mode="json"),
            "source_revision_catalog_hash": catalog.catalog_hash,
        }
        mismatches = {
            key: {"expected": value, "actual": batch.get(key)}
            for key, value in expected.items()
            if canonicalize(batch.get(key)) != canonicalize(value)
        }
        if batch.get("sealed_at") is None or mismatches:
            raise HistoricalRangeContractError(
                REASON_IDEMPOTENCY_CONFLICT,
                "sealed planning batch differs from the seal retry",
                context={"batch_id": batch_id, "mismatches": mismatches},
            )
        cur.execute(
            """
            SELECT range_run_id, research_program_id
            FROM app.advisory_historical_range_run
            WHERE batch_id = %s
            ORDER BY research_program_id
            """,
            (batch_id,),
        )
        actual_runs = tuple((str(row["research_program_id"]), str(row["range_run_id"])) for row in cur.fetchall())
        expected_runs = tuple(
            (program.research_program_id, resolved.range_run_id(program.research_program_id))
            for program in resolved.frozen_programs
        )
        if actual_runs != expected_runs:
            raise HistoricalRangeContractError(
                REASON_IDEMPOTENCY_CONFLICT,
                "sealed planning batch Program runs differ from the seal retry",
                context={"batch_id": batch_id, "actual_runs": actual_runs, "expected_runs": expected_runs},
            )
        return SealedHistoricalRangeBatch(
            batch_id=batch_id,
            canonical_batch_id=batch_id,
            range_run_ids=tuple(run_id for _, run_id in actual_runs),
            deduplicated=False,
        )

    @staticmethod
    def _insert_sealed_program_runs(
        *,
        cur: Any,
        resolved: ResolvedHistoricalRangeRequestV1,
        artifacts: HistoricalRangeArtifactBindingsV1,
    ) -> tuple[str, ...]:
        range_run_ids: list[str] = []
        for frozen in resolved.frozen_programs:
            range_run_id = resolved.range_run_id(frozen.research_program_id)
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
                    range_run_id,
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
            if cur.rowcount != 1:
                raise HistoricalRangeContractError(
                    REASON_REPOSITORY_CONFLICT,
                    "sealed Program run insert did not affect exactly one row",
                    context={"range_run_id": range_run_id},
                )
            range_run_ids.append(range_run_id)
        return tuple(range_run_ids)

    def _validate_creation_artifacts(
        self,
        *,
        resolved: ResolvedHistoricalRangeRequestV1,
        catalog: HistoricalRangeSourceRevisionCatalogV1,
        artifacts: HistoricalRangeArtifactBindingsV1,
    ) -> None:
        if artifacts.artifact_root_identity_hash != self._artifact_store.root_identity_hash:
            raise ValueError("artifact_root_identity_hash differs from the configured artifact store")
        request_payload = HistoricalRangeResolvedRequestArtifactPayloadV1(
            resolved_request=resolved,
            source_revision_catalog=catalog,
        )
        request_envelope = self._load_artifact(
            artifacts.request_ref,
            expected_kind=HistoricalRangeArtifactKind.REQUEST,
            resolved_request_hash=str(resolved.request_payload_sha256),
            expected_payload=request_payload.model_dump(mode="json"),
        )
        if request_envelope.source_revision_refs != catalog.source_revision_refs():
            raise ValueError("request artifact source refs differ from the sealed source catalog")
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
            binding_ref = frozen.resolved_hmm_binding_set_ref
            if binding_ref is not None:
                envelope = self._artifact_store.load_planning(binding_ref)
                binding_set = HistoricalRangeHMMBindingSetV1.model_validate(envelope.payload)
                if (
                    envelope.artifact_kind is not HistoricalRangeArtifactKind.HMM_BINDING_SET
                    or binding_set.binding_set_hash != frozen.resolved_hmm_binding_set_hash
                    or binding_set.research_program_id != frozen.research_program_id
                    or binding_set.package_id != frozen.package_id
                    or binding_set.base_runtime_config_hash != frozen.runtime_config_hash
                ):
                    raise ValueError("resolved HMM binding set differs from the frozen Program")

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
        exact_upstream_refs: Sequence[HistoricalRangeArtifactRefV1] | None = None,
        allow_ancestor_identity: bool = False,
        allow_direct_predecessor_day_run_id: str | None = None,
        validate_recursive_upstream: bool = True,
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
        if exact_upstream_refs is not None:
            expected_upstream = tuple(
                sorted(
                    (
                        (item.artifact_kind.value, item.semantic_content_hash, item.relative_path)
                        for item in exact_upstream_refs
                    ),
                )
            )
            actual_upstream = tuple(sorted((kind.value, semantic_hash, path) for kind, semantic_hash, path in upstream_by_identity))
            if actual_upstream != expected_upstream:
                raise ValueError("artifact upstream closure does not equal the required exact ref set")
        if validate_recursive_upstream:
            self._load_upstream_closure(
                envelope=envelope,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=day_run_id,
                visited={ref.semantic_content_hash},
                allow_direct_predecessor_day_run_id=allow_direct_predecessor_day_run_id,
            )
        else:
            allowed_day_ids = {None, day_run_id, allow_direct_predecessor_day_run_id}
            for upstream_ref in envelope.upstream_refs:
                upstream = self._artifact_store.load(upstream_ref)
                if upstream.resolved_request_hash != resolved_request_hash:
                    raise ValueError("direct upstream resolved_request_hash differs from its consumer")
                if range_run_id is not None and upstream.range_run_id not in {None, range_run_id}:
                    raise ValueError("direct upstream belongs to a different range run")
                if day_run_id is not None and upstream.day_run_id not in allowed_day_ids:
                    raise ValueError("direct upstream belongs to a different day run")
        return envelope

    def _load_upstream_closure(
        self,
        *,
        envelope: HistoricalRangeArtifactEnvelopeV1,
        resolved_request_hash: str,
        range_run_id: str | None,
        day_run_id: str | None,
        visited: set[str],
        allow_direct_predecessor_day_run_id: str | None = None,
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
            allowed_day_ids = {None, day_run_id}
            if allow_direct_predecessor_day_run_id is not None:
                allowed_day_ids.add(allow_direct_predecessor_day_run_id)
            if day_run_id is not None and upstream.day_run_id not in allowed_day_ids:
                raise ValueError("upstream artifact belongs to a different day run")
            next_day_run_id = day_run_id
            next_predecessor_day_run_id: str | None = None
            if upstream_ref.artifact_kind is HistoricalRangeArtifactKind.DAY_RECEIPT and upstream.day_run_id is not None:
                range_receipt_to_day = day_run_id is None
                cross_day = day_run_id is not None and upstream.day_run_id != day_run_id
                if cross_day and upstream.payload_schema_version != DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2:
                    raise ValueError("cross-day ancestor must be a typed successful DAY_RECEIPT")
                if range_receipt_to_day or cross_day:
                    next_day_run_id = str(upstream.day_run_id)
                    if upstream.payload_schema_version == DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2:
                        typed_receipt = HistoricalRangeDayReceiptPayloadV2.model_validate(upstream.payload)
                        if typed_receipt.previous_day_receipt_ref is not None:
                            predecessor = self._artifact_store.load(typed_receipt.previous_day_receipt_ref)
                            if (
                                predecessor.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT
                                or predecessor.range_run_id != upstream.range_run_id
                                or predecessor.day_run_id is None
                            ):
                                raise ValueError("typed predecessor receipt has an invalid range/day identity")
                            next_predecessor_day_run_id = str(predecessor.day_run_id)
            self._load_upstream_closure(
                envelope=upstream,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=next_day_run_id,
                visited=visited,
                allow_direct_predecessor_day_run_id=next_predecessor_day_run_id,
            )

    @staticmethod
    def _require_exact_upstream_refs(
        *,
        envelope: HistoricalRangeArtifactEnvelopeV1,
        expected_refs: Sequence[HistoricalRangeArtifactRefV1],
    ) -> None:
        actual = tuple(
            sorted((item.artifact_kind.value, item.semantic_content_hash, item.relative_path) for item in envelope.upstream_refs)
        )
        expected = tuple(
            sorted((item.artifact_kind.value, item.semantic_content_hash, item.relative_path) for item in expected_refs)
        )
        if actual != expected:
            raise ValueError("artifact upstream closure does not equal the typed receipt direct refs")

    @staticmethod
    def _insert_operation(*, cur: Any, request: HistoricalRangeOperationRequestV1) -> bool:
        cur.execute(
            """
            INSERT INTO app.advisory_historical_range_operation (
                operation_id, batch_id, operation_type,
                operation_idempotency_key, request_payload_sha256,
                planning_identity_hash, expected_row_version,
                catalog_generation, catalog_phase,
                cumulative_resolved_count, cumulative_unresolved_count,
                cumulative_member_chain_hash,
                status, row_version, attempt_no
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                'QUEUED', 1, 0
            )
            ON CONFLICT DO NOTHING
            """,
            (
                request.operation_id,
                request.batch_id,
                request.operation_type.value,
                request.operation_idempotency_key,
                request.request_payload_sha256,
                request.planning_identity_hash,
                request.expected_row_version,
                1 if request.operation_type is HistoricalRangeOperationType.BUILD_SOURCE_CATALOG else None,
                "DISCOVER" if request.operation_type is HistoricalRangeOperationType.BUILD_SOURCE_CATALOG else None,
                0 if request.operation_type is HistoricalRangeOperationType.BUILD_SOURCE_CATALOG else None,
                0 if request.operation_type is HistoricalRangeOperationType.BUILD_SOURCE_CATALOG else None,
                canonical_json_sha256([])
                if request.operation_type is HistoricalRangeOperationType.BUILD_SOURCE_CATALOG
                else None,
            ),
        )
        return cur.rowcount == 1

    def _complete_planning_create_operation(
        self,
        *,
        cur: Any,
        request: HistoricalRangeOperationRequestV1,
        requirement_plan_ref: HistoricalRangeArtifactRefV1,
    ) -> None:
        now = datetime.now(UTC)
        worker_id = "phase1r-planning-create"
        lease_token = derive_prefixed_id(
            "ahrlease",
            {"operation_id": request.operation_id, "planning_identity_hash": request.planning_identity_hash},
        )
        cur.execute(
            """
            UPDATE app.advisory_historical_range_operation
            SET status = 'RUNNING', row_version = row_version + 1,
                attempt_no = 1, worker_id = %s, lease_token = %s,
                lease_expires_at = %s, fencing_token = 1,
                started_at = %s
            WHERE operation_id = %s AND row_version = 1 AND status = 'QUEUED'
            RETURNING *
            """,
            (
                worker_id,
                lease_token,
                now + timedelta(minutes=1),
                now,
                request.operation_id,
            ),
        )
        running = cur.fetchone()
        if running is None:
            raise self._repository_error(
                "CREATE operation could not enter RUNNING state",
                operation_id=request.operation_id,
            )
        attempt = HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahroa",
                {"operation_id": request.operation_id, "attempt_no": 1, "fencing_token": 1},
            ),
            operation_id=request.operation_id,
            attempt_no=1,
            worker_id=worker_id,
            lease_token=lease_token,
            fencing_token=1,
            status=HistoricalRangeOperationStatus.COMPLETED.value,
            input_hash=str(request.planning_identity_hash),
            result_hash=requirement_plan_ref.semantic_content_hash,
            attempt_receipt_ref=requirement_plan_ref,
            started_at=now,
            finished_at=now,
        )
        self._insert_operation_attempt(cur=cur, attempt=attempt)
        cur.execute(
            """
            UPDATE app.advisory_historical_range_operation
            SET status = 'COMPLETED', row_version = row_version + 1,
                result_status = 'PLANNING_CREATED',
                result_ref = %s, result_hash = %s,
                finished_at = %s
            WHERE operation_id = %s AND row_version = 2
              AND status = 'RUNNING' AND fencing_token = 1
            RETURNING operation_id
            """,
            (
                psycopg2.extras.Json(requirement_plan_ref.model_dump(mode="json")),
                requirement_plan_ref.semantic_content_hash,
                now,
                request.operation_id,
            ),
        )
        if cur.fetchone() is None:
            raise self._repository_error(
                "CREATE operation could not commit its planning receipt",
                operation_id=request.operation_id,
            )

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
            "planning_identity_hash": request.planning_identity_hash,
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

    def _locked_predecessor_for_claim(self, *, cur: Any, day: dict[str, Any]) -> dict[str, Any]:
        if int(day["ordinal"]) == 1:
            return {
                "day_receipt_ref": None,
                "day_receipt_hash": None,
                "list_version_id": None,
                "list_version_hash": None,
            }
        cur.execute(
            """
            SELECT day_run_id, range_run_id, ordinal, status,
                   day_receipt_ref, day_receipt_hash, list_version_id, list_version_hash
            FROM app.advisory_historical_range_day_run
            WHERE day_run_id = %s
            FOR KEY SHARE
            """,
            (day["previous_day_run_id"],),
        )
        predecessor = cur.fetchone()
        if predecessor is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "non-first day predecessor does not exist",
                context={"day_run_id": day["day_run_id"]},
            )
        row = dict(predecessor)
        if (
            row["range_run_id"] != day["range_run_id"]
            or int(row["ordinal"]) != int(day["ordinal"]) - 1
            or str(row["status"])
            not in {HistoricalRangeDayStatus.COMPLETE.value, HistoricalRangeDayStatus.VALID_NO_CANDIDATE.value}
            or row["day_receipt_ref"] is None
            or row["day_receipt_hash"] is None
            or row["list_version_id"] is None
            or row["list_version_hash"] is None
        ):
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "non-first day predecessor is not an exact successful list/receipt state",
                context={"day_run_id": day["day_run_id"], "previous_day_run_id": day["previous_day_run_id"]},
            )
        return row


    @staticmethod
    def _candidate_fact_from_row(row: Mapping[str, Any]) -> HistoricalRangeCandidateFactV1:
        return HistoricalRangeCandidateFactV1(
            candidate_id=str(row["candidate_id"]),
            day_run_id=str(row["day_run_id"]),
            symbol=str(row["symbol"]),
            membership_status=str(row["membership_status"]),
            alpha_raw_rank=row["alpha_raw_rank"],
            alpha_raw_score=row["alpha_raw_score"],
            hmm_adjusted_rank=row["hmm_adjusted_rank"],
            hmm_adjusted_score=row["hmm_adjusted_score"],
            risk_policy_adjusted_rank=row["risk_policy_adjusted_rank"],
            risk_policy_adjusted_score=row["risk_policy_adjusted_score"],
            selection_effective_rank=row["selection_effective_rank"],
            selection_effective_score=row["selection_effective_score"],
            advisory_model_rank=row["advisory_model_rank"],
            advisory_model_score=row["advisory_model_score"],
            component_lineage_json=dict(row["component_lineage_json"]),
            component_lineage_hash=str(row["component_lineage_hash"]),
            candidate_content_hash=str(row["candidate_content_hash"]),
        )

    @staticmethod
    def _list_version_fact_from_row(row: Mapping[str, Any]) -> HistoricalRangeListVersionFactV1:
        return HistoricalRangeListVersionFactV1(
            list_version_id=str(row["list_version_id"]),
            day_run_id=str(row["day_run_id"]),
            range_run_id=str(row["range_run_id"]),
            previous_list_version_id=(
                str(row["previous_list_version_id"]) if row["previous_list_version_id"] else None
            ),
            previous_list_hash=(str(row["previous_list_hash"]) if row["previous_list_hash"] else None),
            previous_day_receipt_hash=(
                str(row["previous_day_receipt_hash"]) if row["previous_day_receipt_hash"] else None
            ),
            target_count=int(row["target_count"]),
            active_count=int(row["active_count"]),
            enter_count=int(row["enter_count"]),
            hold_count=int(row["hold_count"]),
            exit_count=int(row["exit_count"]),
            watch_count=int(row["watch_count"]),
            price_timing_policy=str(row["price_timing_policy"]),
            summary_json=dict(row["summary_json"]),
            list_content_hash=str(row["list_content_hash"]),
        )

    @staticmethod
    def _list_item_fact_from_row(row: Mapping[str, Any]) -> HistoricalRangeListItemFactV1:
        return HistoricalRangeListItemFactV1(
            list_item_id=str(row["list_item_id"]),
            list_version_id=str(row["list_version_id"]),
            symbol=str(row["symbol"]),
            action=str(row["action"]),
            rank=row["rank"],
            score=row["score"],
            reason_codes=tuple(row["reason_codes_json"]),
            episode_id=(str(row["episode_id"]) if row["episode_id"] else None),
            rule_guidance_json=dict(row["rule_guidance_json"]),
            intended_execution_trade_date=row["intended_execution_trade_date"],
            intended_execution_basis=(
                str(row["intended_execution_basis"]) if row["intended_execution_basis"] else None
            ),
            execution_status=str(row["execution_status"]),
            evidence_hash=str(row["evidence_hash"]),
        )

    @staticmethod
    def _episode_fact_from_row(row: Mapping[str, Any]) -> HistoricalRangeEpisodeSnapshotFactV1:
        return HistoricalRangeEpisodeSnapshotFactV1(
            episode_snapshot_id=str(row["episode_snapshot_id"]),
            range_run_id=str(row["range_run_id"]),
            list_version_id=str(row["list_version_id"]),
            episode_id=str(row["episode_id"]),
            symbol=str(row["symbol"]),
            decision_trade_date=row["decision_trade_date"],
            entry_sequence=int(row["entry_sequence"]),
            enter_decision_trade_date=row["enter_decision_trade_date"],
            exit_decision_trade_date=row["exit_decision_trade_date"],
            recommendation_state=str(row["recommendation_state"]),
            action=str(row["action"]),
            execution_status=str(row["execution_status"]),
            price_quality=str(row["price_quality"]),
            weak_rank_confirmation_count=int(row["weak_rank_confirmation_count"]),
            mark_json=dict(row["mark_json"]),
            evidence_hash=str(row["evidence_hash"]),
        )

    @staticmethod
    def _day_attempt_from_row(row: Mapping[str, Any]) -> HistoricalRangeDayAttemptV1:
        return HistoricalRangeDayAttemptV1(
            attempt_id=str(row["attempt_id"]),
            day_run_id=str(row["day_run_id"]),
            attempt_no=int(row["attempt_no"]),
            worker_id=str(row["worker_id"]),
            lease_token=str(row["lease_token"]),
            fencing_token=int(row["fencing_token"]),
            status=str(row["status"]),
            input_hash=str(row["input_hash"]),
            result_hash=(str(row["result_hash"]) if row["result_hash"] else None),
            candidate_artifact_ref=(
                HistoricalRangeArtifactRefV1.model_validate(row["candidate_artifact_ref"])
                if row["candidate_artifact_ref"] is not None
                else None
            ),
            attempt_receipt_ref=(
                HistoricalRangeArtifactRefV1.model_validate(row["attempt_receipt_ref"])
                if row["attempt_receipt_ref"] is not None
                else None
            ),
            reason_codes=tuple(row["reason_codes_json"]),
            error_json=(dict(row["error_json"]) if row["error_json"] is not None else None),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )

    @staticmethod
    def _execution_operation_from_row(row: Mapping[str, Any]) -> HistoricalRangeExecutionOperationV1:
        return HistoricalRangeExecutionOperationV1(
            operation_id=str(row["operation_id"]),
            batch_id=str(row["batch_id"]),
            operation_type=str(row["operation_type"]),
            operation_idempotency_key=str(row["operation_idempotency_key"]),
            idempotency_payload_hash=str(row["request_payload_sha256"]),
            resolved_request_hash=str(row["batch_resolved_request_hash"]),
            expected_row_version=int(row["expected_row_version"]),
            status=HistoricalRangeOperationStatus(str(row["status"])),
            row_version=int(row["row_version"]),
            attempt_no=int(row["attempt_no"]),
            worker_id=(str(row["worker_id"]) if row["worker_id"] is not None else None),
            lease_token=(str(row["lease_token"]) if row["lease_token"] is not None else None),
            lease_expires_at=row["lease_expires_at"],
            lease_expired=bool(row.get("lease_expired", False)),
            fencing_token=(int(row["fencing_token"]) if row["fencing_token"] is not None else None),
            stable_keyset_cursor_json=(
                dict(row["stable_keyset_cursor_json"])
                if row["stable_keyset_cursor_json"] is not None
                else None
            ),
            result_row_version=(int(row["result_row_version"]) if row["result_row_version"] else None),
            result_status=(str(row["result_status"]) if row["result_status"] is not None else None),
            result_ref=(
                HistoricalRangeArtifactRefV1.model_validate(row["result_ref"])
                if row["result_ref"] is not None
                else None
            ),
        )

    @staticmethod
    def _claimed_day_from_row(
        *,
        row: dict[str, Any],
        batch_id: str,
        research_program_id: str,
        resolved_request_hash: str,
        request_ref: HistoricalRangeArtifactRefV1,
        list_semantics_version: str,
        list_semantics_hash: str,
        predecessor: Mapping[str, Any],
    ) -> HistoricalRangeClaimedDayV1:
        if row.get("lease_expires_at") is None or row.get("current_fencing_token") is None:
            raise HistoricalRangeContractError(
                REASON_REPOSITORY_CONFLICT,
                "claimed day has incomplete durable lease/fencing fields",
                context={"day_run_id": row.get("day_run_id")},
            )
        predecessor_ref = predecessor.get("day_receipt_ref")
        return HistoricalRangeClaimedDayV1(
            batch_id=batch_id,
            range_run_id=str(row["range_run_id"]),
            research_program_id=research_program_id,
            day_run_id=str(row["day_run_id"]),
            decision_trade_date=row["decision_trade_date"],
            ordinal=int(row["ordinal"]),
            row_version=int(row["row_version"]),
            attempt_no=int(row["attempt_no"]),
            worker_id=str(row["worker_id"]),
            lease_token=str(row["lease_token"]),
            fencing_token=int(row["current_fencing_token"]),
            lease_expires_at=row["lease_expires_at"],
            resolved_request_hash=resolved_request_hash,
            request_ref=request_ref,
            list_semantics_version=list_semantics_version,
            list_semantics_hash=list_semantics_hash,
            previous_day_run_id=(str(row["previous_day_run_id"]) if row["previous_day_run_id"] is not None else None),
            previous_day_receipt_ref=(
                HistoricalRangeArtifactRefV1.model_validate(predecessor_ref) if predecessor_ref is not None else None
            ),
            previous_list_version_id=(
                str(predecessor["list_version_id"]) if predecessor.get("list_version_id") is not None else None
            ),
            previous_list_hash=(
                str(predecessor["list_version_hash"]) if predecessor.get("list_version_hash") is not None else None
            ),
        )

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
