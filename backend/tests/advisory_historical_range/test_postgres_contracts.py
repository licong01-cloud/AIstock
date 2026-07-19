from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactBindingsV1,
    HistoricalRangeArtifactKind,
    HistoricalRangeBatchStatus,
    HistoricalRangeDayAttemptV1,
    HistoricalRangeDayStatus,
    HistoricalRangeListAction,
    HistoricalRangeListItemFactV1,
    HistoricalRangeListVersionFactV1,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOperationType,
    HistoricalRangeProgramStatus,
    HistoricalRangeSourceRevisionRefV1,
    build_candidate_artifact_payload,
    build_day_receipt_payload,
    derive_list_content_hash,
)
from backend.services.advisory_historical_range.repository import PostgresHistoricalRangeRepository
from backend.tests.advisory_historical_range.conftest import digest, research_spec, resolved_request


pytestmark = pytest.mark.skipif(
    not os.environ.get("AISTOCK_PHASE1R_TEST_DSN"),
    reason="explicit disposable AISTOCK_PHASE1R_TEST_DSN is required",
)


def _connection_factory():
    return psycopg2.connect(os.environ["AISTOCK_PHASE1R_TEST_DSN"])


def _publish_creation_artifacts(*, store: HistoricalRangeArtifactStore, resolved) -> HistoricalRangeArtifactBindingsV1:
    request = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="phase1r_r1",
        payload_schema_version=resolved.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=resolved.model_dump(mode="json"),
    )
    date_plan = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DATE_PLAN,
        producer_contract_version="phase1r_r1",
        payload_schema_version=resolved.date_plan.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=resolved.date_plan.model_dump(mode="json"),
        upstream_refs=(request.ref,),
    )
    frozen = {
        program.research_program_id: store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.FROZEN_PROGRAM,
            producer_contract_version="phase1r_r1",
            payload_schema_version=program.schema_version,
            resolved_request_hash=resolved.request_payload_sha256,
            range_run_id=resolved.range_run_id(program.research_program_id),
            payload=program.model_dump(mode="json"),
            upstream_refs=(request.ref, date_plan.ref),
        ).ref
        for program in resolved.frozen_programs
    }
    return HistoricalRangeArtifactBindingsV1(
        request_ref=request.ref,
        date_plan_ref=date_plan.ref,
        frozen_program_refs=frozen,
        artifact_root_identity_hash=store.root_identity_hash,
    )


def _scalar(query: str, params: tuple[object, ...]) -> object:
    with _connection_factory() as connection, connection.cursor() as cursor:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchone()[0]


def _create_running_day(
    *,
    repository: PostgresHistoricalRangeRepository,
    store: HistoricalRangeArtifactStore,
    client_key: str,
    lease_expires_at: datetime,
):
    resolved = resolved_request(
        specs=(research_spec(package_id=f"pkg-{client_key}"),),
        client_key=client_key,
        request_id=f"request-{client_key}",
        trade_dates=(date(2026, 6, 1),),
    )
    artifacts = _publish_creation_artifacts(store=store, resolved=resolved)
    created = repository.create_batch(resolved=resolved, artifacts=artifacts)
    batch = repository.transition_batch(
        batch_id=created.batch_id,
        expected_row_version=2,
        target_status=HistoricalRangeBatchStatus.RUNNING,
    )
    assert batch["status"] == "RUNNING"
    range_run_id = created.range_run_ids[0]
    repository.transition_run(
        range_run_id=range_run_id,
        expected_row_version=1,
        target_status=HistoricalRangeProgramStatus.RUNNING,
    )
    materialized = repository.materialize_day_plan_chunk(
        range_run_id=range_run_id,
        date_plan=resolved.date_plan,
        date_plan_ref=artifacts.date_plan_ref,
        expected_cursor_ordinal=0,
    )
    day_run_id = str(materialized.entries[0].day_run_id)
    repository.transition_day(
        day_run_id=day_run_id,
        expected_row_version=1,
        target_status=HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY,
        attempt_no=0,
    )
    repository.transition_day(
        day_run_id=day_run_id,
        expected_row_version=2,
        target_status=HistoricalRangeDayStatus.RUNNING,
        attempt_no=1,
        lease_expires_at=lease_expires_at,
        fencing_token=1,
    )
    return resolved, artifacts, created, range_run_id, day_run_id


def test_postgres_contracts_close_success_aggregates_attempts_watch_and_takeover(tmp_path: Path) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    repository = PostgresHistoricalRangeRepository(conn_factory=_connection_factory, artifact_store=store)
    now = datetime.now(UTC)
    assert (
        _scalar(
            """
            SELECT COUNT(*)
            FROM pg_catalog.pg_attribute AS attribute
            JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid
            JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'app'
              AND relation.relname LIKE 'advisory_historical_range_%'
              AND relation.relkind = 'r'
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
              AND pg_catalog.col_description(relation.oid, attribute.attnum) IS NULL
            """,
            (),
        )
        == 0
    )
    resolved, _, created, range_run_id, day_run_id = _create_running_day(
        repository=repository,
        store=store,
        client_key="pg-positive",
        lease_expires_at=now + timedelta(minutes=5),
    )

    empty_candidates: tuple = ()
    source_revision_refs = (
        HistoricalRangeSourceRevisionRefV1(
            revision_id="partition:2026-06-01",
            revision_hash=digest("partition:2026-06-01"),
        ),
    )
    candidate = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
        producer_contract_version="phase1r_r1",
        payload_schema_version="advisory_historical_range_candidate_artifact_payload_v1",
        resolved_request_hash=resolved.request_payload_sha256,
        range_run_id=range_run_id,
        day_run_id=day_run_id,
        source_revision_refs=source_revision_refs,
        payload=build_candidate_artifact_payload(
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            candidates=empty_candidates,
            source_revision_refs=source_revision_refs,
        ),
    )
    list_version = HistoricalRangeListVersionFactV1(
        list_version_id=f"list-{day_run_id}",
        day_run_id=day_run_id,
        range_run_id=range_run_id,
        target_count=5,
        active_count=0,
        enter_count=0,
        hold_count=0,
        exit_count=0,
        watch_count=0,
        summary_json={"status": "VALID_NO_CANDIDATE"},
        list_content_hash=digest("initial-list-hash-seed"),
    )
    list_version = list_version.model_copy(update={"list_content_hash": derive_list_content_hash(list_version, (), ())})
    day_input_hash = digest("day-input")
    receipt_payload = build_day_receipt_payload(
        range_run_id=range_run_id,
        day_run_id=day_run_id,
        terminal_status=HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
        day_input_hash=day_input_hash,
        candidate_artifact_ref=candidate.ref,
        list_version=list_version,
        items=(),
        episodes=(),
    )
    receipt = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
        producer_contract_version="phase1r_r1",
        payload_schema_version="advisory_historical_range_day_receipt_payload_v1",
        resolved_request_hash=resolved.request_payload_sha256,
        range_run_id=range_run_id,
        day_run_id=day_run_id,
        payload=receipt_payload,
        upstream_refs=(candidate.ref,),
    )
    attempt = HistoricalRangeDayAttemptV1(
        attempt_id=f"attempt-{day_run_id}-1",
        day_run_id=day_run_id,
        attempt_no=1,
        worker_id="worker-positive",
        lease_token="lease-positive",
        fencing_token=1,
        status="VALID_NO_CANDIDATE",
        input_hash=day_input_hash,
        result_hash=receipt.ref.semantic_content_hash,
        candidate_artifact_ref=candidate.ref,
        attempt_receipt_ref=receipt.ref,
        started_at=now,
        finished_at=now + timedelta(seconds=1),
    )
    first_commit = repository.commit_successful_day(
        day_run_id=day_run_id,
        expected_row_version=3,
        expected_fencing_token=1,
        terminal_status=HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
        day_input_hash=day_input_hash,
        candidate_artifact_ref=candidate.ref,
        day_receipt_ref=receipt.ref,
        list_version=list_version,
        candidates=(),
        items=(),
        episodes=(),
        attempt=attempt,
    )
    assert first_commit.idempotent is False
    retry = repository.commit_successful_day(
        day_run_id=day_run_id,
        expected_row_version=3,
        expected_fencing_token=1,
        terminal_status=HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
        day_input_hash=day_input_hash,
        candidate_artifact_ref=candidate.ref,
        day_receipt_ref=receipt.ref,
        list_version=list_version,
        candidates=(),
        items=(),
        episodes=(),
        attempt=attempt,
    )
    assert retry.idempotent is True
    assert (
        _scalar(
            "SELECT COUNT(*) FROM app.advisory_historical_range_day_attempt WHERE day_run_id = %s",
            (day_run_id,),
        )
        == 1
    )

    range_receipt = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
        producer_contract_version="phase1r_r1",
        payload_schema_version="advisory_historical_range_range_receipt_payload_v1",
        resolved_request_hash=resolved.request_payload_sha256,
        range_run_id=range_run_id,
        payload={"range_run_id": range_run_id, "status": "COMPLETED"},
        upstream_refs=(receipt.ref,),
    )
    run_version = int(
        _scalar(
            "SELECT row_version FROM app.advisory_historical_range_run WHERE range_run_id = %s",
            (range_run_id,),
        )
    )
    repository.transition_run(
        range_run_id=range_run_id,
        expected_row_version=run_version,
        target_status=HistoricalRangeProgramStatus.COMPLETED,
        final_receipt_ref=range_receipt.ref,
    )
    batch_version = int(
        _scalar(
            "SELECT row_version FROM app.advisory_historical_range_batch WHERE batch_id = %s",
            (created.batch_id,),
        )
    )
    completed = repository.transition_batch(
        batch_id=created.batch_id,
        expected_row_version=batch_version,
        target_status=HistoricalRangeBatchStatus.COMPLETED,
    )
    assert completed["finished_at"] is not None
    assert completed["successful_day_count"] == 1

    operation_id = f"operation-refresh-{created.batch_id}"
    operation_request = HistoricalRangeOperationRequestV1(
        operation_id=operation_id,
        batch_id=created.batch_id,
        operation_type=HistoricalRangeOperationType.REFRESH_OUTCOMES,
        operation_idempotency_key="refresh-positive",
        request_payload_sha256=digest({"operation": "refresh-positive"}),
        expected_row_version=int(completed["row_version"]),
    )
    _, operation_idempotent = repository.get_or_create_operation(operation_request)
    assert operation_idempotent is False
    repository.transition_operation(
        operation_id=operation_id,
        expected_row_version=1,
        target_status=HistoricalRangeOperationStatus.RUNNING,
        attempt_no=1,
        worker_id="operation-worker-expired",
        lease_token="operation-lease-expired",
        lease_expires_at=now - timedelta(seconds=5),
        fencing_token=1,
    )
    operation_receipt = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
        producer_contract_version="phase1r_r1",
        payload_schema_version="advisory_historical_range_operation_attempt_receipt_v1",
        resolved_request_hash=resolved.request_payload_sha256,
        payload={"operation_id": operation_id, "status": "RETRYABLE_FAILED", "reason": "LEASE_EXPIRED"},
    )
    expired_operation_attempt = HistoricalRangeOperationAttemptV1(
        attempt_id=f"expired-{operation_id}-1",
        operation_id=operation_id,
        attempt_no=1,
        worker_id="operation-worker-expired",
        lease_token="operation-lease-expired",
        fencing_token=1,
        status="RETRYABLE_FAILED",
        input_hash=digest("operation-input"),
        attempt_receipt_ref=operation_receipt.ref,
        reason_codes=("LEASE_EXPIRED",),
        started_at=now - timedelta(minutes=2),
        finished_at=now,
    )
    operation_takeover = repository.transition_operation(
        operation_id=operation_id,
        expected_row_version=2,
        target_status=HistoricalRangeOperationStatus.RUNNING,
        attempt_no=2,
        worker_id="operation-worker-new",
        lease_token="operation-lease-new",
        lease_expires_at=now + timedelta(minutes=5),
        fencing_token=2,
        expired_attempt=expired_operation_attempt,
    )
    assert operation_takeover["status"] == "RUNNING"
    assert operation_takeover["attempt_no"] == 2
    assert (
        _scalar(
            "SELECT COUNT(*) FROM app.advisory_historical_range_operation_attempt WHERE operation_id = %s",
            (operation_id,),
        )
        == 1
    )

    _, _, spoofed, spoofed_run_id, spoofed_day_id = _create_running_day(
        repository=repository,
        store=store,
        client_key="pg-spoofed-aggregate",
        lease_expires_at=now + timedelta(minutes=5),
    )
    with pytest.raises(psycopg2.Error) as aggregate_error, _connection_factory() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE app.advisory_historical_range_batch
                SET status = 'COMPLETED', row_version = row_version + 1,
                    successful_day_count = planned_day_count,
                    completed_program_count = program_count,
                    recoverable_program_count = 0,
                    started_at = clock_timestamp(), finished_at = clock_timestamp()
                WHERE batch_id = %s
                """,
                (spoofed.batch_id,),
            )
    assert "AGGREGATE" in str(aggregate_error.value).upper()

    with pytest.raises(psycopg2.Error) as missing_attempt, _connection_factory() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE app.advisory_historical_range_day_run
                SET status = 'CANCELLED', row_version = row_version + 1,
                    finished_at = clock_timestamp()
                WHERE day_run_id = %s
                """,
                (spoofed_day_id,),
            )
    assert "ATTEMPT_CLOSURE" in str(missing_attempt.value).upper()

    invalid_list = HistoricalRangeListVersionFactV1(
        list_version_id=f"invalid-watch-{spoofed_day_id}",
        day_run_id=spoofed_day_id,
        range_run_id=spoofed_run_id,
        target_count=5,
        active_count=0,
        enter_count=0,
        hold_count=0,
        exit_count=0,
        watch_count=1,
        summary_json={"test": "invalid-watch"},
        list_content_hash=digest("invalid-watch-list"),
    )
    invalid_watch = HistoricalRangeListItemFactV1(
        list_item_id=f"invalid-watch-item-{spoofed_day_id}",
        list_version_id=invalid_list.list_version_id,
        symbol="999999.SZ",
        action=HistoricalRangeListAction.WATCH,
        rule_guidance_json={"test": True},
        execution_status="NOT_DUE",
    )
    with pytest.raises(psycopg2.Error) as watch_error, _connection_factory() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app.advisory_historical_range_list_version (
                    list_version_id, day_run_id, range_run_id, target_count,
                    active_count, enter_count, hold_count, exit_count, watch_count,
                    price_timing_policy, summary_json, list_content_hash
                ) VALUES (%s, %s, %s, 5, 0, 0, 0, 0, 1,
                          'PIT_DECISION_THEN_MATURE', %s, %s)
                """,
                (
                    invalid_list.list_version_id,
                    spoofed_day_id,
                    spoofed_run_id,
                    psycopg2.extras.Json(invalid_list.summary_json),
                    invalid_list.list_content_hash,
                ),
            )
            cursor.execute(
                """
                INSERT INTO app.advisory_historical_range_list_item (
                    list_item_id, list_version_id, symbol, action, reason_codes_json,
                    rule_guidance_json, execution_status, evidence_hash
                ) VALUES (%s, %s, %s, 'WATCH', '[]'::jsonb, %s, %s, %s)
                """,
                (
                    invalid_watch.list_item_id,
                    invalid_watch.list_version_id,
                    invalid_watch.symbol,
                    psycopg2.extras.Json(invalid_watch.rule_guidance_json),
                    invalid_watch.execution_status,
                    invalid_watch.evidence_hash,
                ),
            )
    assert "CANDIDATE_PROJECTION" in str(watch_error.value).upper()

    takeover_resolved, _, _, takeover_run_id, takeover_day_id = _create_running_day(
        repository=repository,
        store=store,
        client_key="pg-takeover",
        lease_expires_at=now - timedelta(seconds=5),
    )
    takeover_receipt = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
        producer_contract_version="phase1r_r1",
        payload_schema_version="advisory_historical_range_attempt_receipt_payload_v1",
        resolved_request_hash=takeover_resolved.request_payload_sha256,
        range_run_id=takeover_run_id,
        day_run_id=takeover_day_id,
        payload={"day_run_id": takeover_day_id, "status": "RETRYABLE_FAILED", "reason": "LEASE_EXPIRED"},
    )
    expired_attempt = HistoricalRangeDayAttemptV1(
        attempt_id=f"expired-{takeover_day_id}-1",
        day_run_id=takeover_day_id,
        attempt_no=1,
        worker_id="worker-expired",
        lease_token="lease-expired",
        fencing_token=1,
        status="RETRYABLE_FAILED",
        input_hash=digest("takeover-input"),
        attempt_receipt_ref=takeover_receipt.ref,
        reason_codes=("LEASE_EXPIRED",),
        started_at=now - timedelta(minutes=2),
        finished_at=now,
    )
    takeover = repository.transition_day(
        day_run_id=takeover_day_id,
        expected_row_version=3,
        target_status=HistoricalRangeDayStatus.RUNNING,
        attempt_no=2,
        lease_expires_at=now + timedelta(minutes=5),
        fencing_token=2,
        expired_attempt=expired_attempt,
    )
    assert takeover["status"] == "RUNNING"
    assert takeover["attempt_no"] == 2
    assert (
        _scalar(
            "SELECT COUNT(*) FROM app.advisory_historical_range_day_attempt WHERE day_run_id = %s",
            (takeover_day_id,),
        )
        == 1
    )
