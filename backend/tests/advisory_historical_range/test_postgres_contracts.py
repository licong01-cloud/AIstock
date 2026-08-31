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
    HistoricalRangeCatalogMemberDeltaV1,
    HistoricalRangeCatalogPhase,
    HistoricalRangeDayAttemptV1,
    HistoricalRangeDayStatus,
    HistoricalRangeListAction,
    HistoricalRangeListItemFactV1,
    HistoricalRangeListVersionFactV1,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOperationType,
    HistoricalRangePlanningArtifactBindingsV1,
    HistoricalRangeProgramStatus,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeResolvedRequestArtifactPayloadV1,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceCatalogCheckpointV1,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionMemberV1,
    build_candidate_artifact_payload,
    build_candidate_input_hash,
    build_catalog_member_chain_hash,
    build_day_input_hash,
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


def _publish_creation_artifacts(
    *,
    store: HistoricalRangeArtifactStore,
    resolved,
    catalog: HistoricalRangeSourceRevisionCatalogV1,
) -> HistoricalRangeArtifactBindingsV1:
    request_payload = HistoricalRangeResolvedRequestArtifactPayloadV1(
        resolved_request=resolved,
        source_revision_catalog=catalog,
    )
    request = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=request_payload.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=request_payload.model_dump(mode="json"),
        source_revision_refs=catalog.source_revision_refs(),
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
    base_resolved = resolved_request(
        specs=(research_spec(package_id=f"pkg-{client_key}"),),
        client_key=client_key,
        request_id=f"request-{client_key}",
        trade_dates=(date(2026, 6, 1),),
    )
    requirement = HistoricalRangeSourceRequirementV1(
        requirement_id="universe",
        source_role="pit_universe",
        dataset_id="market.stock_universe_pit",
        query_template_id="StockUniversePitService.get_eligible_codes",
        query_template_version="v1",
        query_template_hash=digest("universe-query"),
        parameter_template={"trade_date": "2026-06-01"},
        partition_ref_template="shsz_st_pit_active_v1/2026-06-01",
        decision_trade_date=date(2026, 6, 1),
        required_for=HistoricalRangeRequirementPurpose.REQUEST_SEAL,
        missing_reason_code="ADVISORY_HR_PIT_INPUT_UNAVAILABLE",
    )
    plan = HistoricalRangeSourceRequirementPlanV1(
        request=base_resolved.request,
        date_plan=base_resolved.date_plan,
        frozen_programs=base_resolved.frozen_programs,
        query_contract_hash=digest("historical-query-contract"),
        calendar_identity_hash=digest("calendar-identity"),
        code_release_hash=base_resolved.frozen_programs[0].code_release_hash,
        requirements=(requirement,),
    )
    member = HistoricalRangeSourceRevisionMemberV1(
        requirement_id=requirement.requirement_id,
        source_role=requirement.source_role,
        dataset_id=requirement.dataset_id,
        partition_ref=requirement.partition_ref_template,
        decision_trade_date=requirement.decision_trade_date,
        query_template_id=requirement.query_template_id,
        query_template_version=requirement.query_template_version,
        query_template_hash=requirement.query_template_hash,
        parameter_hash=requirement.parameter_template_hash,
        row_count=5000,
        content_hash=digest(f"universe-content:{client_key}"),
        admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        observed_at=datetime(2026, 7, 20, 1, tzinfo=UTC),
    )
    catalog = HistoricalRangeSourceRevisionCatalogV1(
        requirement_plan_hash=plan.requirement_plan_hash,
        catalog_generation=1,
        query_contract_hash=plan.query_contract_hash,
        calendar_identity_hash=plan.calendar_identity_hash,
        members=(member,),
    )
    resolved_payload = base_resolved.model_dump(mode="python")
    resolved_payload.update(
        {
            "batch_id": plan.batch_id,
            "source_revision_catalog_hash": catalog.catalog_hash,
            "request_payload_sha256": None,
        }
    )
    resolved = type(base_resolved).model_validate(resolved_payload)
    planning_artifact = store.publish_planning_payload(
        artifact_kind=HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN,
        planning_identity_hash=plan.planning_identity_hash,
        batch_id=plan.batch_id,
        catalog_generation=1,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=plan.schema_version,
        payload=plan.model_dump(mode="json"),
    )
    created = repository.create_planning_batch(
        plan=plan,
        artifacts=HistoricalRangePlanningArtifactBindingsV1(
            requirement_plan_ref=planning_artifact.ref,
            artifact_root_identity_hash=store.root_identity_hash,
        ),
    )
    claimed = repository.claim_catalog_operation(
        operation_id=created.catalog_operation_id,
        expected_row_version=1,
        worker_id="catalog-worker-1",
        lease_token="test-1",
        lease_expires_at=max(lease_expires_at, datetime.now(UTC) + timedelta(minutes=5)),
    )
    member_chain_hash = build_catalog_member_chain_hash(
        members=(member,),
        ordered_requirement_ids=(requirement.requirement_id,),
    )
    discover_checkpoint = HistoricalRangeSourceCatalogCheckpointV1(
        requirement_plan_hash=plan.requirement_plan_hash,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        ordinal_start=1,
        ordinal_end=1,
        next_requirement_ordinal=2,
        member_delta=(HistoricalRangeCatalogMemberDeltaV1(ordinal=1, member=member),),
        cumulative_resolved_count=1,
        cumulative_member_chain_hash=member_chain_hash,
    )
    discover_artifact = store.publish_planning_payload(
        artifact_kind=HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
        planning_identity_hash=plan.planning_identity_hash,
        batch_id=plan.batch_id,
        catalog_generation=1,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=discover_checkpoint.schema_version,
        payload=discover_checkpoint.model_dump(mode="json"),
    )
    verifying = repository.commit_catalog_checkpoint(
        operation_id=created.catalog_operation_id,
        expected_row_version=int(claimed["row_version"]),
        expected_fencing_token=int(claimed["fencing_token"]),
        checkpoint_ref=discover_artifact.ref,
        checkpoint=discover_checkpoint,
        target_status=HistoricalRangeOperationStatus.RUNNING,
        advance_to_verify=True,
        next_worker_id="catalog-worker-2",
        next_lease_token="test-2",
        next_lease_expires_at=lease_expires_at + timedelta(minutes=1),
    )
    verify_checkpoint = HistoricalRangeSourceCatalogCheckpointV1(
        requirement_plan_hash=plan.requirement_plan_hash,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.VERIFY,
        ordinal_start=1,
        ordinal_end=1,
        next_requirement_ordinal=2,
        previous_checkpoint_ref=discover_artifact.ref,
        previous_checkpoint_hash=discover_artifact.ref.semantic_content_hash,
        member_delta=(HistoricalRangeCatalogMemberDeltaV1(ordinal=1, member=member),),
        cumulative_resolved_count=1,
        cumulative_member_chain_hash=member_chain_hash,
    )
    verify_artifact = store.publish_planning_payload(
        artifact_kind=HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
        planning_identity_hash=plan.planning_identity_hash,
        batch_id=plan.batch_id,
        catalog_generation=1,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=verify_checkpoint.schema_version,
        payload=verify_checkpoint.model_dump(mode="json"),
    )
    repository.commit_catalog_checkpoint(
        operation_id=created.catalog_operation_id,
        expected_row_version=int(verifying["row_version"]),
        expected_fencing_token=int(verifying["fencing_token"]),
        checkpoint_ref=verify_artifact.ref,
        checkpoint=verify_checkpoint,
        target_status=HistoricalRangeOperationStatus.COMPLETED,
    )
    artifacts = _publish_creation_artifacts(store=store, resolved=resolved, catalog=catalog)
    sealed = repository.seal_planning_batch(
        batch_id=plan.batch_id,
        expected_row_version=int(
            _scalar(
                "SELECT row_version FROM app.advisory_historical_range_batch WHERE batch_id = %s",
                (plan.batch_id,),
            )
        ),
        plan=plan,
        resolved=resolved,
        catalog=catalog,
        artifacts=artifacts,
    )
    batch = repository.transition_batch(
        batch_id=sealed.batch_id,
        expected_row_version=int(
            _scalar(
                "SELECT row_version FROM app.advisory_historical_range_batch WHERE batch_id = %s",
                (sealed.batch_id,),
            )
        ),
        target_status=HistoricalRangeBatchStatus.RUNNING,
    )
    assert batch["status"] == "RUNNING"
    range_run_id = sealed.range_run_ids[0]
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
        worker_id="worker-expired",
        lease_token=digest("lease-expired"),
        lease_expires_at=lease_expires_at,
        fencing_token=1,
    )
    return resolved, artifacts, created, range_run_id, day_run_id, catalog


def test_postgres_operation_idempotency_is_scoped_by_operation_type(
    tmp_path: Path,
) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r-operation-scope")
    repository = PostgresHistoricalRangeRepository(
        conn_factory=_connection_factory,
        artifact_store=store,
    )
    run_namespace = str(os.environ.get("AISTOCK_PHASE1R_TEST_RUN_ID") or "operation-type-scope").strip()
    _, _, created, _, _, _ = _create_running_day(
        repository=repository,
        store=store,
        client_key=f"pg-operation-type-scope-{run_namespace}",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    shared_operation_key = f"bridge-shared-type-scope-{run_namespace}"
    batch_version = int(
        _scalar(
            "SELECT row_version FROM app.advisory_historical_range_batch WHERE batch_id = %s",
            (created.batch_id,),
        )
    )
    parent_request = HistoricalRangeOperationRequestV1(
        operation_id=f"operation-bridge-parent-{created.batch_id}",
        batch_id=created.batch_id,
        operation_type=HistoricalRangeOperationType.BUILD_DATASET_BRIDGE,
        operation_idempotency_key=shared_operation_key,
        request_payload_sha256=digest({"operation": "bridge-parent"}),
        expected_row_version=batch_version,
    )
    child_request = HistoricalRangeOperationRequestV1(
        operation_id=f"operation-bridge-child-{created.batch_id}",
        batch_id=created.batch_id,
        operation_type=HistoricalRangeOperationType.BUILD_DATASET_BRIDGE_RUN,
        operation_idempotency_key=shared_operation_key,
        request_payload_sha256=digest({"operation": "bridge-child"}),
        expected_row_version=batch_version,
    )

    parent_row, parent_idempotent = repository.get_or_create_operation(parent_request)
    child_row, child_idempotent = repository.get_or_create_operation(child_request)
    retried_parent, exact_parent_retry = repository.get_or_create_operation(parent_request)
    retried_child, exact_child_retry = repository.get_or_create_operation(child_request)

    assert parent_idempotent is False
    assert child_idempotent is False
    assert exact_parent_retry is True
    assert exact_child_retry is True
    assert parent_row["operation_id"] == retried_parent["operation_id"]
    assert child_row["operation_id"] == retried_child["operation_id"]
    assert parent_row["operation_type"] == "BUILD_DATASET_BRIDGE"
    assert child_row["operation_type"] == "BUILD_DATASET_BRIDGE_RUN"


def test_postgres_contracts_close_success_aggregates_attempts_watch_and_takeover(tmp_path: Path) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    repository = PostgresHistoricalRangeRepository(conn_factory=_connection_factory, artifact_store=store)
    now = datetime.now(UTC)
    run_namespace = str(os.environ.get("AISTOCK_PHASE1R_TEST_RUN_ID") or "").strip()

    def client_key(value: str) -> str:
        return f"{value}-{run_namespace}" if run_namespace else value

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
    resolved, _, created, range_run_id, day_run_id, catalog = _create_running_day(
        repository=repository,
        store=store,
        client_key=client_key("pg-positive"),
        lease_expires_at=now + timedelta(minutes=5),
    )

    empty_candidates: tuple = ()
    source_revision_refs = catalog.source_revision_refs()
    frozen = resolved.frozen_programs[0]
    candidate_input_hash = build_candidate_input_hash(
        range_run_id=range_run_id,
        research_program_id=frozen.research_program_id,
        decision_trade_date=date(2026, 6, 1),
        frozen_program_hash=frozen.frozen_program_hash,
        runtime_profile_hash=digest("runtime-profile"),
        code_release_hash=frozen.code_release_hash,
        selection_semantics_hash=resolved.selection_semantics_hash,
        calendar_identity_hash=digest("calendar-identity"),
        universe_identity_hash=digest("universe-identity"),
        source_revision_catalog_hash=resolved.source_revision_catalog_hash,
        query_contract_hash=digest("historical-query-contract"),
    )
    stage_trace = {
        stage: {
            "stage": stage,
            "status": "COMPLETE",
            "input_count": 0,
            "output_count": 0,
            "excluded_count": 0,
        }
        for stage in ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
    }
    raw_header = {
        "artifact_id": "raw-signal",
        "runtime_profile_hash": digest("runtime-profile"),
        "selection_semantics_hash": resolved.selection_semantics_hash,
        "code_release_hash": frozen.code_release_hash,
        "calendar_identity_hash": digest("calendar-identity"),
        "universe_identity_hash": digest("universe-identity"),
    }
    candidate = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
        producer_contract_version="phase1r_r2b",
        payload_schema_version="advisory_historical_range_candidate_artifact_payload_v2",
        resolved_request_hash=resolved.request_payload_sha256,
        range_run_id=range_run_id,
        day_run_id=day_run_id,
        source_revision_refs=source_revision_refs,
        payload=build_candidate_artifact_payload(
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            research_program_id=frozen.research_program_id,
            decision_trade_date=date(2026, 6, 1),
            candidate_input_hash=candidate_input_hash,
            package_id=frozen.package_id,
            package_version=frozen.package_version,
            manifest_sha256=frozen.manifest_sha256,
            alpha_mode=frozen.alpha_mode,
            runtime_profile_hash=digest("runtime-profile"),
            selection_semantics_hash=resolved.selection_semantics_hash,
            code_release_hash=frozen.code_release_hash,
            calendar_identity_hash=digest("calendar-identity"),
            universe_identity_hash=digest("universe-identity"),
            universe_count=5000,
            raw_signal_identity_hash=digest(raw_header),
            raw_signal_semantic_header=raw_header,
            raw_inference_receipt={"status": "COMPLETE", "score_count": 0},
            source_read_receipt_hashes=(digest("source-read"),),
            stage_trace=stage_trace,
            candidate_outcome="VALID_NO_CANDIDATE",
            no_candidate_reason_codes=("NO_ALPHA_CANDIDATES",),
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
    day_input_hash = build_day_input_hash(
        candidate_input_hash=candidate_input_hash,
        candidate_artifact_ref=candidate.ref,
        previous_list_hash=None,
        previous_day_receipt_hash=None,
        list_semantics_hash=resolved.list_semantics_hash,
    )
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
        lease_token=digest("lease-positive"),
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
        lease_token=digest("operation-lease-expired"),
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
        lease_token=digest("operation-lease-expired"),
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
        lease_token=digest("operation-lease-new"),
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

    _, _, spoofed, spoofed_run_id, spoofed_day_id, _ = _create_running_day(
        repository=repository,
        store=store,
        client_key=client_key("pg-spoofed-aggregate"),
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

    takeover_resolved, _, _, takeover_run_id, takeover_day_id, _ = _create_running_day(
        repository=repository,
        store=store,
        client_key=client_key("pg-takeover"),
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
        lease_token=digest("lease-expired"),
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
        worker_id="worker-takeover",
        lease_token=digest("lease-takeover"),
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
