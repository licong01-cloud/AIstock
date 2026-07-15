from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatchRequest,
    CapturePlan,
    PostgresCaptureBatchRepository,
)
from backend.services.advisory_phase1.control_binding import (
    ControlBindingRequest,
    ControlType,
    PostgresControlBindingRepository,
)
from backend.services.advisory_phase1.observation_capture import (
    build_observation_semantic_draft,
    expected_evidence_bundle_hash,
)
from backend.services.advisory_phase1.phase1g_contract import (
    Phase1GTransactionalWriteRequest,
    REASON_G3_BATCH_ROW_VERSION_CONFLICT,
    REASON_G3_COMMIT_FAILED,
    REASON_G3_COMMIT_STATE_UNKNOWN,
    REASON_G3_CHILD_ROW_CONFLICT,
    REASON_G3_POST_COMMIT_VERIFY_FAILED,
    REASON_G3_UNEXPECTED_ERROR,
)
from backend.services.advisory_phase1.phase1g_historical_trace_contract import (
    build_phase1g_target_projection_snapshot,
    materialize_phase1g_stage_trace_envelope,
)
from backend.services.advisory_phase1.phase1g_source_replay import (
    Phase1GSourceSetRef,
)
from backend.services.advisory_phase1.phase1g_transactional_writer import (
    Phase1GTransactionalTargetInput,
    Phase1GTransactionalWriter,
    Phase1GTransactionalWriterError,
)
from backend.services.advisory_phase1.stage_trace import (
    TraceCaptureBinding,
    TraceCaptureContext,
    TraceCapturePolicy,
)
from backend.tests.advisory_phase1.test_phase1g_historical_trace_projection import (
    _project_case,
    historical_filtered_empty_case,
    historical_many_candidates_case,
    historical_multi_alpha_case,
    historical_raw_empty_case,
)
from backend.tests.advisory_phase1.test_release_schema_dev_db import _fresh_apply


pytest_plugins = ("backend.tests.advisory_phase1.test_release_schema_dev_db",)


@contextmanager
def _managed_factory(config) -> Iterator[Any]:  # type: ignore[no-untyped-def]
    conn = psycopg2.connect(**config.connect_kwargs())
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _raw_factory(config):  # type: ignore[no-untyped-def]
    def create():  # type: ignore[no-untyped-def]
        conn = psycopg2.connect(**config.connect_kwargs())
        conn.autocommit = False
        return conn

    return create


def _insert_source_event(config, event) -> None:  # type: ignore[no-untyped-def]
    item = event.input
    conn = psycopg2.connect(**config.connect_kwargs())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE app.advisory_source_availability_event DISABLE TRIGGER USER"
            )
            cur.execute(
                """
                INSERT INTO market.trading_calendar (cal_date, is_trading)
                VALUES ('2026-07-01', TRUE), ('2026-07-02', TRUE)
                ON CONFLICT (cal_date) DO UPDATE SET is_trading = EXCLUDED.is_trading
                """
            )
            cur.execute(
                """
                INSERT INTO app.advisory_source_availability_event (
                    availability_event_id, append_request_hash, dataset_name, source_role,
                    partition_key, partition_key_hash, partition_chain_key, revision_id,
                    event_revision_no, event_type, predecessor_event_hash, provider_job_id,
                    refresh_job_id, provider_published_at, first_observed_at,
                    formal_available_at, schema_fingerprint, row_count,
                    partition_content_hash, quality_status, reason_codes,
                    event_content_hash, created_by_service_principal
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (availability_event_id) DO NOTHING
                """,
                (
                    event.availability_event_id,
                    item.append_request_hash,
                    item.dataset_name,
                    item.source_role,
                    psycopg2.extras.Json(canonicalize(item.partition_key)),
                    item.partition_key_hash,
                    item.partition_chain_key,
                    item.revision_id,
                    item.event_revision_no,
                    item.event_type.value,
                    item.predecessor_event_hash,
                    item.provider_job_id,
                    item.refresh_job_id,
                    item.provider_published_at,
                    item.first_observed_at,
                    item.formal_available_at,
                    item.schema_fingerprint,
                    item.row_count,
                    item.partition_content_hash,
                    item.quality_status,
                    psycopg2.extras.Json(list(item.reason_codes)),
                    event.event_content_hash,
                    item.created_by_service_principal,
                ),
            )
            cur.execute(
                "ALTER TABLE app.advisory_source_availability_event ENABLE TRIGGER USER"
            )
    finally:
        conn.close()


def _build_target(
    config,
    *,
    suffix: str = "a",
    case_factory=historical_raw_empty_case,
):  # type: ignore[no-untyped-def]
    case = case_factory()
    source_operation, source_replay, historical = _project_case(case)
    _insert_source_event(config, case["event"])
    provisional_binding = TraceCaptureBinding(
        control_binding_event_hash="0" * 64,
        binding_id=f"phase1g-g3-binding-{suffix}",
        binding_version="1",
        handoff_readiness_hash=case[
            "plan"
        ].evidence_binding.handoff_readiness_report_hash,
        admission_scope_id=case["plan"].evidence_binding.admission_scope_id,
        admission_scope_hash=case["plan"].evidence_binding.admission_scope_hash,
        capture_batch_id=f"phase1g-g3-batch-{suffix}",
        capture_fencing_token=1,
        capture_policy=TraceCapturePolicy(
            policy_id=f"phase1g-g3-policy-{suffix}",
            policy_version="1",
            max_candidates=1000,
            max_bytes=10_000_000,
            max_capture_ms=60_000,
        ),
    )
    control_payload = provisional_binding.model_dump(
        mode="json", exclude={"control_binding_event_hash"}
    )
    control_request = ControlBindingRequest(
        control_type=ControlType.TRACE_CAPTURE,
        environment=f"DISPOSABLE_{suffix.upper()}",
        admission_scope_set_hash=provisional_binding.admission_scope_hash,
        config_source="phase1g-g3-test",
        config_payload=control_payload,
        config_or_store_backend_hash=canonical_json_sha256(control_payload),
        enabled=True,
        binding_event_revision_no=1,
        created_by_service_principal="phase1g-g3-test",
    )
    control = PostgresControlBindingRepository(
        conn_factory=lambda: _managed_factory(config)
    ).append(control_request)
    binding_payload = provisional_binding.model_dump(
        mode="python", exclude={"binding_hash"}
    )
    binding_payload["control_binding_event_hash"] = control.binding_event_hash
    binding = TraceCaptureBinding.model_validate(binding_payload)
    lineage = historical.dse.evidence.phase0a_candidate_lineage
    clock = historical.dse.evidence.decision_clock
    context = TraceCaptureContext(
        selection_run_id=str(lineage["selection_run_id"]),
        package_id=historical.dse.package_id,
        manifest_sha256=historical.dse.manifest_sha256,
        decision_as_of_trade_date=clock.decision_as_of_trade_date,
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        binding=binding,
    )
    envelope = materialize_phase1g_stage_trace_envelope(
        context=context, projection=historical
    )
    chain = historical.dse.evidence.phase0a_effective_config_chain
    stable_hash = canonical_json_sha256(
        {
            "package_id": historical.dse.package_id,
            "manifest_sha256": historical.dse.manifest_sha256,
            "decision_as_of_trade_date": clock.decision_as_of_trade_date,
            "program_id": "program-a",
        }
    )
    scope_hash = canonical_json_sha256(
        {
            "stable_signal_semantics_hash": stable_hash,
            "admission_scope_hash": binding.admission_scope_hash,
        }
    )
    plan_values = {
        "selection_run_id": context.selection_run_id,
        "package_id": context.package_id,
        "manifest_sha256": context.manifest_sha256,
        "decision_as_of_trade_date": clock.decision_as_of_trade_date.isoformat(),
        "selection_as_of_trade_date": clock.selection_as_of_trade_date.isoformat(),
        "target_trade_date": clock.target_trade_date.isoformat(),
        "decision_cutoff_ts": clock.decision_cutoff_ts,
        "alpha_mode": historical.package_manifest.alpha_mode,
        "selection_runtime_semantics_hash": canonical_json_sha256(
            historical.stage_trace_builder_input.runtime_config
        ),
        "package_effective_config_hash": chain.package_effective_config_hash,
        "calendar_version": clock.calendar_version,
        "calendar_hash": clock.calendar_hash,
        "stable_signal_semantics_hash": stable_hash,
        "canonical_signal_scope_hash": scope_hash,
        "phase0a_audit_id": case["plan"].evidence_binding.phase0a_audit_id,
        "phase0a_audit_manifest_hash": case[
            "plan"
        ].evidence_binding.phase0a_audit_manifest_hash,
        "handoff_readiness_hash": binding.handoff_readiness_hash,
        "admission_scope_id": binding.admission_scope_id,
        "admission_scope_hash": binding.admission_scope_hash,
        "signal_source_revision_set_id": source_replay.source_revision_set.source_revision_set_id,
        "signal_source_revision_set_hash": source_replay.source_revision_set.source_revision_set_hash,
        "phase0a_signal_context_hash": scope_hash,
        "evidence_bundle_hash": "0" * 64,
        "selection_evidence_id": historical.dse.evidence_id,
        "selection_evidence_hash": historical.dse.artifact_hash,
        "selection_run_content_hash": canonical_json_sha256(lineage),
        "selection_score_artifact_id": historical.artifact.artifact_id,
        "selection_score_artifact_hash": historical.artifact.artifact_payload_sha256,
        "runtime_profile_version_id": historical.dse.runtime_profile_version_id,
        "runtime_profile_version_hash": historical.dse.runtime_profile_hash,
        "hmm_snapshot_status": "NOT_APPLICABLE",
        "risk_policy_hash": canonical_json_sha256(
            historical.stage_trace_builder_input.risk_metadata
        ),
        "universe_policy_hash": canonical_json_sha256(
            historical.stage_trace_builder_input.universe_metadata
        ),
        "symbol_normalization_policy_hash": canonical_json_sha256(
            {"policy": "canonical_a_share_symbol_v1"}
        ),
        "valid_no_candidate": historical.candidate_outcome == "VALID_NO_CANDIDATE",
        "evidence_available_at": clock.data_available_at,
        "audit_target_id": "phase1g-g3-audit-target",
        "target_scope_hash": case["plan"].evidence_binding.target_scope_hash,
        "capability": "HISTORICAL_RESEARCH",
        "oos_interval_id": "phase1g-g3-oos",
        "oos_interval_hash": case["plan"].evidence_binding.oos_interval_hash,
        "evidence_scope": "RETROSPECTIVE_RESEARCH_ONLY",
        "signal_evidence_level": "RETROSPECTIVE_RESEARCH_ONLY",
        "effective_cutoff_date": clock.effective_cutoff_date.isoformat(),
        "program_id": "program-a",
        "binding_version_id": case["plan"].evidence_binding.binding_version_id,
        "source_run_id": case["plan"].evidence_binding.historical_program_run_id,
        "lineage_source_type": "HISTORICAL_REPLAY",
    }
    provisional_plan = CapturePlan.model_validate(plan_values)
    plan_values["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional_plan, trace_content_hash=envelope.trace_content_hash
    )
    plan = CapturePlan.model_validate(plan_values)
    source_ref = Phase1GSourceSetRef(
        source_revision_set_id=source_replay.source_revision_set.source_revision_set_id,
        source_revision_set_hash=source_replay.source_revision_set.source_revision_set_hash,
        capture_plan_hash=plan.plan_hash,
    )
    source_operation = source_operation.model_copy(
        update={
            "expected_capture_source_sets": (source_ref,),
            "source_operation_projection_hash": None,
        }
    )
    source_replay = source_replay.model_copy(
        update={
            "source_operation_projection_hash": source_operation.source_operation_projection_hash,
            "source_replay_result_hash": None,
        }
    )
    snapshot = build_phase1g_target_projection_snapshot(
        source_operation=source_operation,
        source_replay=source_replay,
        historical_trace=historical,
    )
    capture_request = CaptureBatchRequest(
        capture_batch_id=binding.capture_batch_id,
        binding=binding,
        plans=(plan,),
    )
    capture_repository = PostgresCaptureBatchRepository(
        conn_factory=lambda: _managed_factory(config)
    )
    planned = capture_repository.create(capture_request)
    running = capture_repository.acquire(
        capture_batch_id=capture_request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=600,
    )
    draft = build_observation_semantic_draft(
        plan=plan, envelope=envelope, binding=binding
    )
    request = Phase1GTransactionalWriteRequest(
        target_request_hash=snapshot.target_request_hash,
        phase1e_plan_id=snapshot.source_operation_projection.phase1e_plan_id,
        phase1e_plan_hash=snapshot.source_operation_projection.phase1e_plan_hash,
        g2_target_projection_snapshot_hash=snapshot.target_projection_snapshot_hash,
        capture_batch_id=binding.capture_batch_id,
        capture_request_hash=capture_request.capture_request_hash,
        capture_attempt_no=running.capture_attempt_no,
        expected_batch_row_version=running.row_version,
        capture_fencing_token=running.fencing_token,
        control_binding_event_hash=control.binding_event_hash,
        capture_plan_hash=plan.plan_hash,
        trace_capture_context_hash=canonical_json_sha256(
            context.model_dump(mode="json")
        ),
        trace_capture_binding_hash=binding.binding_hash,
        trace_outbox_id=envelope.trace_outbox_id,
        stage_trace_envelope_hash=envelope.trace_content_hash,
        observation_semantic_key=draft.semantic_observation_key,
        observation_semantic_draft_hash=draft.draft_content_hash,
        expected_rows=9 + 2 * len(draft.candidate_semantic_rows),
        expected_bytes=max(snapshot.projected_bytes, envelope.size_bytes),
    )
    return (
        capture_repository,
        Phase1GTransactionalTargetInput(
            request=request,
            target_snapshot=snapshot,
            capture_plan=plan,
            trace_context=context,
            persisted_binding=binding,
            current_writer_binding=binding,
            envelope=envelope,
            semantic_draft=draft,
        ),
    )


def _with_current_row_version(
    target: Phase1GTransactionalTargetInput, row_version: int
) -> Phase1GTransactionalTargetInput:
    payload = target.request.model_dump(mode="python", exclude={"write_request_hash"})
    payload["expected_batch_row_version"] = row_version
    request = Phase1GTransactionalWriteRequest.model_validate(payload)
    return Phase1GTransactionalTargetInput(**{**target.__dict__, "request": request})


def _build_recovery_target(
    config,
    predecessor: Phase1GTransactionalTargetInput,
    *,
    suffix: str,
) -> tuple[PostgresCaptureBatchRepository, Phase1GTransactionalTargetInput]:  # type: ignore[no-untyped-def]
    current_payload = predecessor.persisted_binding.model_dump(
        mode="python", exclude={"binding_hash"}
    )
    current_payload.update(
        control_binding_event_hash="0" * 64,
        capture_batch_id=f"phase1g-g3-recovery-batch-{suffix}",
        capture_fencing_token=1,
    )
    provisional = TraceCaptureBinding.model_validate(current_payload)
    control_payload = provisional.model_dump(
        mode="json", exclude={"control_binding_event_hash"}
    )
    control = PostgresControlBindingRepository(
        conn_factory=lambda: _managed_factory(config)
    ).append(
        ControlBindingRequest(
            control_type=ControlType.TRACE_CAPTURE,
            environment=f"DISPOSABLE_RECOVERY_{suffix.upper()}",
            admission_scope_set_hash=provisional.admission_scope_hash,
            config_source="phase1g-g3-recovery-test",
            config_payload=control_payload,
            config_or_store_backend_hash=canonical_json_sha256(control_payload),
            enabled=True,
            binding_event_revision_no=1,
            created_by_service_principal="phase1g-g3-recovery-test",
        )
    )
    current_payload["control_binding_event_hash"] = control.binding_event_hash
    current_binding = TraceCaptureBinding.model_validate(current_payload)
    context_payload = predecessor.trace_context.model_dump(mode="python")
    context_payload["binding"] = current_binding
    current_context = TraceCaptureContext.model_validate(context_payload)
    capture_request = CaptureBatchRequest(
        capture_batch_id=current_binding.capture_batch_id,
        binding=current_binding,
        plans=(predecessor.capture_plan,),
    )
    repository = PostgresCaptureBatchRepository(
        conn_factory=lambda: _managed_factory(config)
    )
    predecessor_batch = repository.get(predecessor.request.capture_batch_id)
    planned = repository.recover(
        request=capture_request,
        predecessor_capture_batch_id=predecessor.request.capture_batch_id,
        expected_predecessor_row_version=predecessor_batch.row_version,
        predecessor_fencing_token=predecessor_batch.fencing_token,
    )
    running = repository.acquire(
        capture_batch_id=capture_request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=600,
    )
    old_request = predecessor.request
    request = Phase1GTransactionalWriteRequest(
        target_request_hash=old_request.target_request_hash,
        phase1e_plan_id=old_request.phase1e_plan_id,
        phase1e_plan_hash=old_request.phase1e_plan_hash,
        g2_target_projection_snapshot_hash=old_request.g2_target_projection_snapshot_hash,
        capture_batch_id=current_binding.capture_batch_id,
        capture_request_hash=capture_request.capture_request_hash,
        capture_attempt_no=running.capture_attempt_no,
        expected_batch_row_version=running.row_version,
        capture_fencing_token=running.fencing_token,
        control_binding_event_hash=control.binding_event_hash,
        capture_plan_hash=old_request.capture_plan_hash,
        trace_capture_context_hash=canonical_json_sha256(
            current_context.model_dump(mode="json")
        ),
        trace_capture_binding_hash=current_binding.binding_hash,
        trace_outbox_id=old_request.trace_outbox_id,
        stage_trace_envelope_hash=old_request.stage_trace_envelope_hash,
        observation_semantic_key=old_request.observation_semantic_key,
        observation_semantic_draft_hash=old_request.observation_semantic_draft_hash,
        expected_rows=old_request.expected_rows,
        expected_bytes=old_request.expected_bytes,
    )
    return repository, Phase1GTransactionalTargetInput(
        request=request,
        target_snapshot=predecessor.target_snapshot,
        capture_plan=predecessor.capture_plan,
        trace_context=current_context,
        persisted_binding=predecessor.persisted_binding,
        current_writer_binding=current_binding,
        envelope=predecessor.envelope,
        semantic_draft=predecessor.semantic_draft,
    )


def _target_fact_counts(config) -> dict[str, int]:  # type: ignore[no-untyped-def]
    relations = (
        "app.advisory_source_revision_set",
        "app.advisory_source_revision_member",
        "app.advisory_selection_stage_trace_outbox",
        "app.advisory_signal_observation",
        "app.advisory_signal_observation_version",
        "app.advisory_signal_observation_lineage_identity",
        "app.advisory_signal_observation_lineage_payload",
        "app.advisory_signal_stage_evidence",
        "app.advisory_signal_stage_candidate_identity",
        "app.advisory_signal_stage_candidate_payload",
        "app.advisory_capture_batch_evidence_membership",
        "app.advisory_selection_stage_trace_delivery_event",
    )
    conn = psycopg2.connect(**config.connect_kwargs())
    try:
        with conn.cursor() as cur:
            result = {}
            for relation in relations:
                cur.execute(f"SELECT count(*) FROM {relation}")
                result[relation] = int(cur.fetchone()[0])
            return result
    finally:
        conn.close()


class _ConnectionProxy:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)


class _CommitResponseLossConnection(_ConnectionProxy):
    def __init__(self, connection: Any, *, committed: bool, after_commit=None) -> None:  # type: ignore[no-untyped-def]
        super().__init__(connection)
        self.committed = committed
        self.after_commit = after_commit

    def commit(self) -> None:
        if self.committed:
            self.connection.commit()
            if self.after_commit is not None:
                self.after_commit()
        else:
            self.connection.rollback()
        raise OSError("injected commit response loss")


class _AfterCommitConnection(_ConnectionProxy):
    def __init__(self, connection: Any, after_commit) -> None:  # type: ignore[no-untyped-def]
        super().__init__(connection)
        self.after_commit = after_commit

    def commit(self) -> None:
        self.connection.commit()
        self.after_commit()


class _FaultCursor:
    def __init__(self, cursor: Any, relation: str) -> None:
        self.cursor = cursor
        self.relation = relation.lower()
        self.injected = False

    def __enter__(self) -> "_FaultCursor":
        self.cursor.__enter__()
        return self

    def __exit__(self, *args: Any) -> Any:
        return self.cursor.__exit__(*args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.cursor, name)

    def execute(self, query: Any, variables: Any = None) -> Any:
        result = self.cursor.execute(query, variables)
        normalized = " ".join(str(query).lower().split())
        if not self.injected and f"insert into {self.relation}" in normalized:
            self.injected = True
            raise RuntimeError(f"injected post-write failure at {self.relation}")
        return result


class _FaultConnection(_ConnectionProxy):
    def __init__(self, connection: Any, relation: str) -> None:
        super().__init__(connection)
        self.relation = relation

    def cursor(self, *args: Any, **kwargs: Any) -> _FaultCursor:
        return _FaultCursor(self.connection.cursor(*args, **kwargs), self.relation)


def test_disposable_postgres_first_write_and_exact_retry_are_complete_and_stable(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    capture_repository, target = _build_target(config)
    writer = Phase1GTransactionalWriter(
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
    )

    first = writer.write_target(target)
    running = capture_repository.get(target.request.capture_batch_id)
    retry_payload = target.request.model_dump(
        mode="python", exclude={"write_request_hash"}
    )
    retry_payload["expected_batch_row_version"] = running.row_version
    retry = Phase1GTransactionalWriteRequest.model_validate(retry_payload)
    second = writer.write_target(
        Phase1GTransactionalTargetInput(**{**target.__dict__, "request": retry})
    )

    assert first.target_commit_projection_hash == second.target_commit_projection_hash
    assert first.target_membership_count == 3
    assert first.candidate_count == 0
    conn = psycopg2.connect(**config.connect_kwargs())
    try:
        with conn.cursor() as cur:
            for relation, expected in (
                ("app.advisory_signal_observation", 1),
                ("app.advisory_signal_observation_version", 1),
                ("app.advisory_signal_observation_lineage_identity", 1),
                ("app.advisory_signal_observation_lineage_payload", 1),
                ("app.advisory_signal_stage_evidence", 5),
                ("app.advisory_capture_batch_evidence_membership", 3),
                ("app.advisory_selection_stage_trace_delivery_event", 1),
            ):
                cur.execute(f"SELECT count(*) FROM {relation}")
                assert cur.fetchone()[0] == expected
            cur.execute("SELECT count(*) FROM app.advisory_signal_observation_lineage")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT capture_status FROM app.advisory_capture_batch")
            assert cur.fetchone()[0] == "RUNNING"
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("case_factory", "expected_alpha_mode", "expected_candidates"),
    (
        (historical_raw_empty_case, "single_alpha", 0),
        (historical_filtered_empty_case, "single_alpha", None),
        (historical_multi_alpha_case, "multi_alpha", None),
        (lambda: historical_many_candidates_case(16), "single_alpha", None),
    ),
)
def test_disposable_postgres_preserves_complete_candidate_and_alpha_semantics(
    database_factory,
    case_factory,
    expected_alpha_mode: str,
    expected_candidates: int | None,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    _capture_repository, target = _build_target(
        config, suffix="matrix", case_factory=case_factory
    )
    projection = Phase1GTransactionalWriter(
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
    ).write_target(target)

    assert target.capture_plan.alpha_mode == expected_alpha_mode
    assert projection.candidate_count == len(
        target.semantic_draft.candidate_semantic_rows
    )
    if expected_candidates is not None:
        assert projection.candidate_count == expected_candidates
    conn = psycopg2.connect(**config.connect_kwargs())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_signal_stage_candidate_identity"
            )
            identity_count = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM app.advisory_signal_stage_candidate_payload"
            )
            payload_count = int(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM app.advisory_signal_stage_candidate")
            view_count = int(cur.fetchone()[0])
            assert (
                identity_count
                == payload_count
                == view_count
                == projection.candidate_count
            )
            cur.execute(
                "SELECT count(*) FROM app.advisory_signal_observation_lineage_identity"
            )
            identity_lineage_count = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM app.advisory_signal_observation_lineage_payload"
            )
            payload_lineage_count = int(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM app.advisory_signal_observation_lineage")
            view_lineage_count = int(cur.fetchone()[0])
            assert (
                identity_lineage_count
                == payload_lineage_count
                == view_lineage_count
                == 1
            )
    finally:
        conn.close()


def test_disposable_postgres_legal_evidence_change_creates_one_linear_successor(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    repo_a, target_a = _build_target(config, suffix="successor-a")
    _repo_b, target_b = _build_target(
        config,
        suffix="successor-b",
        case_factory=lambda: historical_many_candidates_case(2),
    )
    writer = Phase1GTransactionalWriter(
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
    )

    first = writer.write_target(target_a)
    second = writer.write_target(target_b)

    assert first.canonical_signal_id == second.canonical_signal_id
    assert first.observation_content_hash != second.observation_content_hash
    assert first.candidate_count == 0 < second.candidate_count
    assert second.observation_revision_no == first.observation_revision_no + 1
    first_batch = repo_a.get(target_a.request.capture_batch_id)
    non_latest_retry = writer.write_target(
        _with_current_row_version(target_a, first_batch.row_version)
    )
    assert (
        non_latest_retry.target_commit_projection_hash
        == first.target_commit_projection_hash
    )
    conn = psycopg2.connect(**config.connect_kwargs())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT observation_version_id, observation_revision_no,
                       supersedes_observation_version_id
                FROM app.advisory_signal_observation_version
                ORDER BY observation_revision_no
                """
            )
            rows = cur.fetchall()
            assert rows == [
                (first.observation_version_id, 1, None),
                (second.observation_version_id, 2, first.observation_version_id),
            ]
    finally:
        conn.close()


def test_disposable_postgres_two_writers_have_one_cas_winner_and_rerun_converges(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    capture_repository, target = _build_target(config, suffix="concurrent")

    def run_once():  # type: ignore[no-untyped-def]
        return Phase1GTransactionalWriter(
            transaction_connection_factory=_raw_factory(config),
            readonly_connection_factory=_raw_factory(config),
        ).write_target(target)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(
            future.exception() or future.result()
            for future in (executor.submit(run_once), executor.submit(run_once))
        )
    successes = tuple(item for item in outcomes if not isinstance(item, BaseException))
    failures = tuple(item for item in outcomes if isinstance(item, BaseException))
    assert len(successes) == len(failures) == 1
    assert isinstance(failures[0], Phase1GTransactionalWriterError)
    assert failures[0].reason_code == REASON_G3_BATCH_ROW_VERSION_CONFLICT

    running = capture_repository.get(target.request.capture_batch_id)
    rerun = Phase1GTransactionalWriter(
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
    ).write_target(_with_current_row_version(target, running.row_version))
    assert (
        rerun.target_commit_projection_hash
        == successes[0].target_commit_projection_hash
    )


def test_disposable_postgres_commit_response_loss_classifies_committed_and_not_committed(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    committed_config = database_factory()
    _fresh_apply(committed_config)
    _repo, committed_target = _build_target(committed_config, suffix="commit-lost")
    committed_projection = Phase1GTransactionalWriter(
        transaction_connection_factory=lambda: _CommitResponseLossConnection(
            _raw_factory(committed_config)(), committed=True
        ),
        readonly_connection_factory=_raw_factory(committed_config),
    ).write_target(committed_target)
    assert committed_projection.target_membership_count == 3

    not_committed_config = database_factory()
    _fresh_apply(not_committed_config)
    _repo, not_committed_target = _build_target(
        not_committed_config, suffix="commit-rollback"
    )
    with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
        Phase1GTransactionalWriter(
            transaction_connection_factory=lambda: _CommitResponseLossConnection(
                _raw_factory(not_committed_config)(), committed=False
            ),
            readonly_connection_factory=_raw_factory(not_committed_config),
        ).write_target(not_committed_target)
    assert exc_info.value.reason_code == REASON_G3_COMMIT_FAILED
    assert all(
        value == 0 for value in _target_fact_counts(not_committed_config).values()
    )


def test_disposable_postgres_partial_commit_readback_is_unknown_not_success(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    _repo, target = _build_target(config, suffix="commit-partial")

    def remove_one_membership() -> None:
        conn = psycopg2.connect(**config.connect_kwargs())
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE app.advisory_capture_batch_evidence_membership DISABLE TRIGGER USER"
                )
                cur.execute(
                    """
                    DELETE FROM app.advisory_capture_batch_evidence_membership
                    WHERE capture_batch_id = %s AND evidence_role = 'TRACE_OUTBOX'
                    """,
                    (target.request.capture_batch_id,),
                )
                cur.execute(
                    "ALTER TABLE app.advisory_capture_batch_evidence_membership ENABLE TRIGGER USER"
                )
        finally:
            conn.close()

    with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
        Phase1GTransactionalWriter(
            transaction_connection_factory=lambda: _CommitResponseLossConnection(
                _raw_factory(config)(),
                committed=True,
                after_commit=remove_one_membership,
            ),
            readonly_connection_factory=_raw_factory(config),
        ).write_target(target)
    assert exc_info.value.reason_code == REASON_G3_COMMIT_STATE_UNKNOWN


def test_disposable_postgres_normal_commit_never_hides_post_commit_readback_loss(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    _repo, target = _build_target(config, suffix="post-commit-missing")

    def remove_one_membership() -> None:
        conn = psycopg2.connect(**config.connect_kwargs())
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE app.advisory_capture_batch_evidence_membership DISABLE TRIGGER USER"
                )
                cur.execute(
                    """
                    DELETE FROM app.advisory_capture_batch_evidence_membership
                    WHERE capture_batch_id = %s AND evidence_role = 'TRACE_OUTBOX'
                    """,
                    (target.request.capture_batch_id,),
                )
                cur.execute(
                    "ALTER TABLE app.advisory_capture_batch_evidence_membership ENABLE TRIGGER USER"
                )
        finally:
            conn.close()

    with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
        Phase1GTransactionalWriter(
            transaction_connection_factory=lambda: _AfterCommitConnection(
                _raw_factory(config)(), remove_one_membership
            ),
            readonly_connection_factory=_raw_factory(config),
        ).write_target(target)
    assert exc_info.value.reason_code == REASON_G3_POST_COMMIT_VERIFY_FAILED


def test_disposable_postgres_exact_retry_rejects_tampered_candidate_child(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    repository, target = _build_target(
        config,
        suffix="child-tamper",
        case_factory=lambda: historical_many_candidates_case(2),
    )
    writer = Phase1GTransactionalWriter(
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
    )
    writer.write_target(target)
    conn = psycopg2.connect(**config.connect_kwargs())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE app.advisory_signal_stage_candidate_payload DISABLE TRIGGER USER"
            )
            cur.execute(
                """
                UPDATE app.advisory_signal_stage_candidate_payload
                SET component_reason_codes = '["TAMPERED_CHILD"]'::jsonb
                WHERE (decision_as_of_trade_date, stage_evidence_id, symbol) = (
                    SELECT decision_as_of_trade_date, stage_evidence_id, symbol
                    FROM app.advisory_signal_stage_candidate_payload
                    ORDER BY decision_as_of_trade_date, stage_evidence_id, symbol
                    LIMIT 1
                )
                """
            )
            cur.execute(
                "ALTER TABLE app.advisory_signal_stage_candidate_payload ENABLE TRIGGER USER"
            )
    finally:
        conn.close()
    running = repository.get(target.request.capture_batch_id)
    with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
        writer.write_target(_with_current_row_version(target, running.row_version))
    assert exc_info.value.reason_code == REASON_G3_CHILD_ROW_CONFLICT


def test_disposable_postgres_recovery_reuses_immutable_facts_and_closes_current_batch(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    predecessor_repository, predecessor = _build_target(
        config, suffix="recovery-predecessor"
    )
    writer = Phase1GTransactionalWriter(
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
    )
    first = writer.write_target(predecessor)
    predecessor_running = predecessor_repository.get(
        predecessor.request.capture_batch_id
    )
    predecessor_repository.fail(
        capture_batch_id=predecessor.request.capture_batch_id,
        expected_row_version=predecessor_running.row_version,
        fencing_token=predecessor_running.fencing_token,
        reason_codes=("INJECTED_POST_TARGET_BATCH_FAILURE",),
    )

    recovery_repository, recovery = _build_recovery_target(
        config, predecessor, suffix="reuse"
    )
    recovered = writer.write_target(recovery)
    assert recovered.trace_outbox_id == first.trace_outbox_id
    assert recovered.observation_version_id == first.observation_version_id
    assert recovered.observation_content_hash == first.observation_content_hash
    assert recovered.delivery_event_id == first.delivery_event_id
    assert recovered.capture_batch_id != first.capture_batch_id
    assert recovery_repository.get(recovery.request.capture_batch_id).row_version == (
        recovery.request.expected_batch_row_version + 3
    )
    recovery_running = recovery_repository.get(recovery.request.capture_batch_id)
    recovery_repository.fail(
        capture_batch_id=recovery.request.capture_batch_id,
        expected_row_version=recovery_running.row_version,
        fencing_token=recovery_running.fencing_token,
        reason_codes=("INJECTED_POST_TARGET_BATCH_FAILURE",),
    )

    _failed_repository, not_committed_recovery = _build_recovery_target(
        config, recovery, suffix="rollback"
    )
    with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
        Phase1GTransactionalWriter(
            transaction_connection_factory=lambda: _CommitResponseLossConnection(
                _raw_factory(config)(), committed=False
            ),
            readonly_connection_factory=_raw_factory(config),
        ).write_target(not_committed_recovery)
    assert exc_info.value.reason_code == REASON_G3_COMMIT_FAILED
    conn = psycopg2.connect(**config.connect_kwargs())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM app.advisory_capture_batch_evidence_membership
                WHERE capture_batch_id = %s
                """,
                (not_committed_recovery.request.capture_batch_id,),
            )
            assert int(cur.fetchone()[0]) == 0
            for relation, expected in (
                ("app.advisory_selection_stage_trace_outbox", 1),
                ("app.advisory_signal_observation_version", 1),
                ("app.advisory_selection_stage_trace_delivery_event", 1),
            ):
                cur.execute(f"SELECT count(*) FROM {relation}")
                assert int(cur.fetchone()[0]) == expected
    finally:
        conn.close()


def test_disposable_postgres_every_target_write_node_rolls_back_without_residue(
    database_factory,
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _fresh_apply(config)
    capture_repository, target = _build_target(
        config,
        suffix="fault-matrix",
        case_factory=lambda: historical_many_candidates_case(2),
    )
    relations = (
        "app.advisory_source_revision_set",
        "app.advisory_source_revision_member",
        "app.advisory_selection_stage_trace_outbox",
        "app.advisory_signal_observation",
        "app.advisory_signal_observation_version",
        "app.advisory_signal_observation_lineage_identity",
        "app.advisory_signal_observation_lineage_payload",
        "app.advisory_signal_stage_evidence",
        "app.advisory_signal_stage_candidate_identity",
        "app.advisory_signal_stage_candidate_payload",
        "app.advisory_capture_batch_evidence_membership",
        "app.advisory_selection_stage_trace_delivery_event",
    )
    initial_row_version = target.request.expected_batch_row_version
    for relation in relations:
        writer = Phase1GTransactionalWriter(
            transaction_connection_factory=lambda relation=relation: _FaultConnection(
                _raw_factory(config)(), relation
            ),
            readonly_connection_factory=_raw_factory(config),
        )
        with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
            writer.write_target(target)
        assert exc_info.value.reason_code == REASON_G3_UNEXPECTED_ERROR
        assert all(value == 0 for value in _target_fact_counts(config).values())
        assert (
            capture_repository.get(target.request.capture_batch_id).row_version
            == initial_row_version
        )

    final = Phase1GTransactionalWriter(
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
    ).write_target(target)
    assert final.target_membership_count == 3
