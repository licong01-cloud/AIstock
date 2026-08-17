from __future__ import annotations

from datetime import UTC, datetime, timedelta
from threading import Event
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.advisory_historical_range import service as service_module
from backend.services.advisory_historical_range.api_models import HistoricalRangeBuildBridgeRequest
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeDatasetBridgeError,
)
from backend.services.advisory_historical_range.models import (
    REASON_DATASET_BRIDGE_VALID_EMPTY,
    REASON_REPOSITORY_CONFLICT,
    REASON_ROW_VERSION_CONFLICT,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeBackgroundDispatchFailureV1,
    HistoricalRangeBridgeResultStatus,
    HistoricalRangeContractError,
    HistoricalRangeDatasetBridgeReceiptV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOutcomeRefreshReceiptV1,
)
from backend.services.advisory_historical_range.service import (
    ResponseBoundHistoricalRangeDispatcher,
    _OutcomeParentLeaseHeartbeatSupervisor,
)
from backend.services.advisory_historical_range.planning_service import (
    _CatalogLeaseHeartbeatSupervisor,
)
from backend.services.advisory_historical_range.repository import (
    PostgresHistoricalRangePreclaimFailureRepository,
)
from backend.services.advisory_historical_range import runtime_factories


class _Background:
    def __init__(self) -> None:
        self.task = None

    def add_task(self, func, *args, **kwargs):
        self.task = (func, args, kwargs)


def test_catalog_heartbeat_renews_same_durable_ownership() -> None:
    renewed = Event()
    calls = []
    operation = {
        "operation_id": "ahrop_heartbeat",
        "row_version": 4,
        "attempt_no": 2,
        "worker_id": "worker-1",
        "lease_token": "lease-1",
        "fencing_token": 2,
        "stable_keyset_cursor_json": {"next_requirement_ordinal": 97},
    }

    def transition_operation(**kwargs):
        calls.append(kwargs)
        renewed.set()
        return {**operation, "row_version": kwargs["expected_row_version"] + 1}

    heartbeat = _CatalogLeaseHeartbeatSupervisor(
        repository=SimpleNamespace(transition_operation=transition_operation),
        operation=operation,
        lease_duration=timedelta(milliseconds=300),
    )
    heartbeat.start()
    assert renewed.wait(timeout=1)
    current = heartbeat.stop()

    assert current["row_version"] == 4 + len(calls)
    assert all(call["target_status"].value == "RUNNING" for call in calls)
    assert all(call["attempt_no"] == 2 for call in calls)
    assert all(call["worker_id"] == "worker-1" for call in calls)
    assert all(call["lease_token"] == "lease-1" for call in calls)
    assert all(call["fencing_token"] == 2 for call in calls)
    assert all(
        call["stable_keyset_cursor_json"] == {"next_requirement_ordinal": 97}
        for call in calls
    )
    assert calls[-1]["lease_expires_at"] > datetime.now(UTC)


def test_catalog_heartbeat_surfaces_lost_durable_ownership() -> None:
    attempted = Event()

    def transition_operation(**_kwargs):
        attempted.set()
        raise HistoricalRangeContractError(
            "ADVISORY_HR_ROW_VERSION_CONFLICT",
            "catalog heartbeat lost durable ownership",
        )

    heartbeat = _CatalogLeaseHeartbeatSupervisor(
        repository=SimpleNamespace(transition_operation=transition_operation),
        operation={
            "operation_id": "ahrop_lost",
            "row_version": 4,
            "attempt_no": 2,
            "worker_id": "worker-1",
            "lease_token": "lease-1",
            "fencing_token": 2,
            "stable_keyset_cursor_json": {"next_requirement_ordinal": 97},
        },
        lease_duration=timedelta(milliseconds=300),
    )
    heartbeat.start()
    assert attempted.wait(timeout=1)

    with pytest.raises(HistoricalRangeContractError, match="lost durable ownership"):
        heartbeat.stop()


def test_outcome_parent_heartbeat_renews_same_durable_ownership() -> None:
    renewed = Event()
    calls = []
    operation = {
        "operation_id": "ahrop_outcome_parent",
        "row_version": 2,
        "attempt_no": 1,
        "worker_id": "worker-1",
        "lease_token": "lease-1",
        "fencing_token": 1,
        "stable_keyset_cursor_json": None,
    }

    def transition_operation(**kwargs):
        calls.append(kwargs)
        renewed.set()
        return {
            **operation,
            "row_version": kwargs["expected_row_version"] + 1,
            "lease_expires_at": kwargs["lease_expires_at"],
        }

    heartbeat = _OutcomeParentLeaseHeartbeatSupervisor(
        repository=SimpleNamespace(transition_operation=transition_operation),
        operation=operation,
        lease_duration=timedelta(milliseconds=300),
    )
    heartbeat.start()
    assert renewed.wait(timeout=1)
    current = heartbeat.stop()

    assert current["row_version"] == 2 + len(calls)
    assert all(call["target_status"].value == "RUNNING" for call in calls)
    assert all(call["attempt_no"] == 1 for call in calls)
    assert all(call["worker_id"] == "worker-1" for call in calls)
    assert all(call["lease_token"] == "lease-1" for call in calls)
    assert all(call["fencing_token"] == 1 for call in calls)
    assert calls[-1]["lease_expires_at"] > datetime.now(UTC)


def test_outcome_parent_heartbeat_surfaces_lost_durable_ownership() -> None:
    attempted = Event()

    def transition_operation(**_kwargs):
        attempted.set()
        raise HistoricalRangeContractError(
            REASON_ROW_VERSION_CONFLICT,
            "outcome parent heartbeat lost durable ownership",
        )

    heartbeat = _OutcomeParentLeaseHeartbeatSupervisor(
        repository=SimpleNamespace(transition_operation=transition_operation),
        operation={
            "operation_id": "ahrop_outcome_parent_lost",
            "row_version": 2,
            "attempt_no": 1,
            "worker_id": "worker-1",
            "lease_token": "lease-1",
            "fencing_token": 1,
            "stable_keyset_cursor_json": None,
        },
        lease_duration=timedelta(milliseconds=300),
    )
    heartbeat.start()
    assert attempted.wait(timeout=1)

    with pytest.raises(
        HistoricalRangeContractError,
        match="outcome parent heartbeat lost durable ownership",
    ):
        heartbeat.stop()


@pytest.mark.parametrize("publish_fails", [False, True])
def test_outcome_dispatcher_keeps_heartbeat_through_final_receipt_publication(
    monkeypatch,
    publish_fails: bool,
) -> None:
    transitions = []
    events = []
    operation = {
        "operation_id": "ahrop_outcome_parent",
        "batch_id": "ahrb_1",
        "status": "QUEUED",
        "row_version": 1,
        "attempt_no": 0,
        "fencing_token": 0,
        "lease_expired": False,
    }

    def transition_operation(**kwargs):
        transitions.append(kwargs)
        if len(transitions) == 1:
            return {
                **operation,
                "status": "RUNNING",
                "row_version": 2,
                "attempt_no": 1,
                "worker_id": kwargs["worker_id"],
                "lease_token": kwargs["lease_token"],
                "lease_expires_at": kwargs["lease_expires_at"],
                "fencing_token": 1,
                "started_at": kwargs["started_at"],
            }
        return {**operation, "status": kwargs["target_status"].value}

    class _Heartbeat:
        def __init__(self, *, operation, **_kwargs):
            self.operation = dict(operation)

        def start(self):
            events.append("start")

        def stop(self):
            events.append("stop")
            return {**self.operation, "row_version": 3}

    receipt = HistoricalRangeOutcomeRefreshReceiptV1(
        operation_id="ahrop_outcome_child",
        request_hash="b" * 64,
        status="COMPLETED",
        processed_count=0,
    )
    receipt_ref = _artifact_ref(
        HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
        "c",
    )
    child = SimpleNamespace(
        refresh_until_stable_boundary=lambda **_kwargs: (receipt, receipt_ref)
    )

    def publish_payload(**_kwargs):
        events.append("publish")
        if publish_fails:
            raise RuntimeError("receipt publication failed")
        return SimpleNamespace(ref=receipt_ref)

    runtime = SimpleNamespace(
        query=SimpleNamespace(
            get_operation_internal=lambda _operation_id: dict(operation),
            resolved_request_hash=lambda _batch_id: "d" * 64,
        ),
        repository=SimpleNamespace(transition_operation=transition_operation),
        outcome=child,
        outcome_service_factory=None,
        artifact_store=SimpleNamespace(publish_payload=publish_payload),
    )
    plan = SimpleNamespace(
        request_hash="a" * 64,
        requests=(SimpleNamespace(range_run_ids=("ahrr_1",)),),
    )
    monkeypatch.setattr(
        service_module,
        "_OutcomeParentLeaseHeartbeatSupervisor",
        _Heartbeat,
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)

    if publish_fails:
        with pytest.raises(RuntimeError, match="receipt publication failed"):
            dispatcher._run_outcome_plan(
                runtime=runtime,
                plan=plan,
                operation_id="ahrop_outcome_parent",
                batch_id="ahrb_1",
                worker_id="worker-1",
            )
    else:
        dispatcher._run_outcome_plan(
            runtime=runtime,
            plan=plan,
            operation_id="ahrop_outcome_parent",
            batch_id="ahrb_1",
            worker_id="worker-1",
        )

    assert events == ["start", "publish", "stop"]
    if publish_fails:
        assert len(transitions) == 1
        return
    assert transitions[-1]["expected_row_version"] == 3
    assert transitions[-1]["target_status"] is HistoricalRangeOperationStatus.COMPLETED


def test_catalog_dispatcher_defers_rollover_deadline_until_after_chunk_execution() -> None:
    operation = {
        "operation_id": "ahrop_1",
        "batch_id": "ahrb_1",
        "status": "WAITING_INPUT",
        "row_version": 3,
        "attempt_no": 1,
        "fencing_token": 1,
    }
    query = SimpleNamespace(get_operation_internal=lambda _operation_id: operation)
    repository = SimpleNamespace(
        claim_catalog_operation=lambda **_kwargs: {
            **operation,
            "status": "RUNNING",
            "row_version": 4,
            "attempt_no": 2,
            "fencing_token": 2,
        }
    )
    calls = []

    def execute_claimed_chunk(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(operation={"status": "WAITING_INPUT"}, sealed_batch=None)

    runtime = SimpleNamespace(
        query=query,
        repository=repository,
        planning=SimpleNamespace(execute_claimed_chunk=execute_claimed_chunk),
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)

    dispatcher._run_catalog(
        runtime=runtime,
        payload={"operation_id": "ahrop_1", "batch_id": "ahrb_1"},
        worker_id="worker-1",
    )

    assert len(calls) == 1
    assert calls[0]["next_lease_duration"] == timedelta(minutes=5)
    assert "next_lease_expires_at" not in calls[0]


def test_expired_first_catalog_attempt_publishes_checkpoint_kind_receipt() -> None:
    operation = {
        "operation_id": "ahrop_1",
        "batch_id": "ahrb_1",
        "attempt_no": 1,
        "worker_id": "worker-1",
        "lease_token": "lease-1",
        "fencing_token": 1,
        "lease_expires_at": "2026-07-28T05:00:00+00:00",
        "planning_identity_hash": "a" * 64,
        "catalog_generation": 1,
        "catalog_phase": "DISCOVER",
        "cumulative_resolved_count": 0,
        "cumulative_member_chain_hash": "b" * 64,
        "stable_keyset_cursor_json": None,
    }
    state = SimpleNamespace(
        checkpoint_chain=(),
        plan=SimpleNamespace(
            requirement_plan_hash="c" * 64,
            requirements=(SimpleNamespace(requirement_id="requirement-1"),),
        ),
    )
    published = []
    checkpoint_ref = HistoricalRangeArtifactRefV1(
        artifact_kind=HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
        relative_path="source-catalog-checkpoints/checkpoint.json",
        producer_contract_version="phase1r_r2b",
        payload_schema_version="advisory_historical_range_source_catalog_checkpoint_v1",
        semantic_content_hash="d" * 64,
        payload_sha256="e" * 64,
        file_sha256="f" * 64,
    )

    def publish_planning_payload(**kwargs):
        published.append(kwargs)
        return SimpleNamespace(ref=checkpoint_ref)

    runtime = SimpleNamespace(
        repository=SimpleNamespace(load_catalog_planning_state=lambda **_kwargs: state),
        artifact_store=SimpleNamespace(publish_planning_payload=publish_planning_payload),
    )

    attempt = ResponseBoundHistoricalRangeDispatcher._expired_catalog_attempt(
        runtime=runtime,
        operation=operation,
    )

    assert attempt.attempt_receipt_ref == checkpoint_ref
    assert attempt.result_hash == checkpoint_ref.semantic_content_hash
    assert published[0]["artifact_kind"] is HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT
    assert published[0]["payload"]["unresolved_requirement_delta"] == [
        {
            "ordinal": 1,
            "requirement_id": "requirement-1",
            "reason_code": "ADVISORY_HR_OPERATION_LEASE_EXPIRED",
            "blocked_by_requirement_ids": [],
            "context": {"operation_id": "ahrop_1", "attempt_no": 1},
        }
    ]


def test_dispatcher_captures_only_json_round_tripped_identity() -> None:
    background = _Background()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: SimpleNamespace())
    dispatcher.schedule(
        background,
        command="CATALOG_EXECUTE",
        payload={"batch_id": "ahrb_1", "operation_id": "ahrop_1", "range_run_ids": ("run_1",)},
    )
    assert background.task is not None
    _, args, kwargs = background.task
    assert kwargs == {}
    assert args[0] == "CATALOG_EXECUTE"
    assert args[1] == {"batch_id": "ahrb_1", "operation_id": "ahrop_1", "range_run_ids": ["run_1"]}


def test_dispatcher_rejects_request_scoped_non_json_objects() -> None:
    background = _Background()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: SimpleNamespace())
    try:
        dispatcher.schedule(background, command="CATALOG_EXECUTE", payload={"connection": object()})
    except TypeError:
        pass
    else:
        raise AssertionError("request-scoped object must not be captured")
    assert background.task is None


@pytest.mark.parametrize(
    "payload,missing_identity",
    [
        ({"operation_id": "ahrop_1"}, "batch_id"),
        ({"batch_id": "ahrb_1"}, "operation_id"),
    ],
)
def test_dispatcher_rejects_missing_durable_identity(payload, missing_identity) -> None:
    background = _Background()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: SimpleNamespace())
    with pytest.raises(ValueError, match=missing_identity):
        dispatcher.schedule(background, command="CATALOG_EXECUTE", payload=payload)
    assert background.task is None


class _FailureRecorder:
    def __init__(self) -> None:
        self.failures = []

    def record_retryable_failure(self, failure):
        self.failures.append(failure)
        return failure.model_dump(mode="json")


class _FailureQuery:
    def get_operation_internal(self, operation_id: str):
        return {
            "operation_id": operation_id,
            "batch_id": "ahrb_1",
            "status": "QUEUED",
            "row_version": 1,
            "attempt_no": 0,
        }


def test_runtime_reconstruction_failure_records_structured_retryable_record() -> None:
    recorder = _FailureRecorder()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: (_ for _ in ()).throw(RuntimeError("runtime unavailable")),
        failure_recorder_factory=lambda: recorder,
    )
    with pytest.raises(RuntimeError, match="runtime unavailable"):
        dispatcher._run("CATALOG_EXECUTE", {"operation_id": "ahrop_1", "batch_id": "ahrb_1"})
    failure = recorder.failures[0]
    assert failure.stage == "RUNTIME_RECONSTRUCTION"
    assert failure.schema_version == "advisory_historical_range_background_dispatch_failure_v1"
    assert failure.retryable is True


def test_request_reconstruction_failure_records_structured_retryable_record() -> None:
    recorder = _FailureRecorder()
    runtime = SimpleNamespace(
        query=_FailureQuery(),
        outcome_requests=SimpleNamespace(build=lambda *_: (_ for _ in ()).throw(ValueError("bad request"))),
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: runtime,
        failure_recorder_factory=lambda: recorder,
    )
    with pytest.raises(Exception):
        dispatcher._run(
            "REFRESH_OUTCOMES",
            {"operation_id": "ahrop_2", "batch_id": "ahrb_1", "command_payload": {}},
        )
    assert recorder.failures[0].stage == "REQUEST_RECONSTRUCTION"


def test_preclaim_failure_records_structured_retryable_record() -> None:
    recorder = _FailureRecorder()
    repository = SimpleNamespace()
    repository.claim_catalog_operation = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("claim failed")
    )
    runtime = SimpleNamespace(query=_FailureQuery(), repository=repository)
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: runtime,
        failure_recorder_factory=lambda: recorder,
    )
    with pytest.raises(RuntimeError, match="claim failed"):
        dispatcher._run("CATALOG_EXECUTE", {"operation_id": "ahrop_3", "batch_id": "ahrb_1"})
    assert recorder.failures[0].stage == "CLAIM_AND_EXECUTION"


class _DbCursor:
    def __init__(self, *, update_succeeds: bool = True, select_row=None) -> None:
        self.statements: list[str] = []
        self._row = None
        self._update_succeeds = update_succeeds
        self._select_row = select_row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        self.statements.append(statement)
        if statement.lstrip().startswith("UPDATE"):
            payload = params[0].adapted
            default_row = {
                "operation_id": params[1],
                "batch_id": params[2],
                "status": "RETRYABLE_FAILED",
                "error_json": payload,
            }
            self._row = default_row if self._update_succeeds else None
        elif statement.lstrip().startswith("SELECT"):
            self._row = self._select_row

    def fetchone(self):
        return self._row


class _DbConnection:
    def __init__(self, *, update_succeeds: bool = True, select_row=None) -> None:
        self.cursor_instance = _DbCursor(
            update_succeeds=update_succeeds,
            select_row=select_row,
        )

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self.cursor_instance


def test_db_only_preclaim_failure_record_does_not_require_artifact_store() -> None:
    connection = _DbConnection()
    repository = PostgresHistoricalRangePreclaimFailureRepository(
        conn_factory=lambda: connection
    )
    failure = HistoricalRangeBackgroundDispatchFailureV1(
        operation_id="ahrop_1",
        batch_id="ahrb_1",
        command="CATALOG_EXECUTE",
        stage="RUNTIME_RECONSTRUCTION",
        reason_code="ADVISORY_HR_BACKGROUND_DISPATCH_FAILED",
        error_type="RuntimeError",
        recorded_at=datetime.now(UTC),
    )
    row = repository.record_retryable_failure(failure)
    assert row["status"] == "RETRYABLE_FAILED"
    assert row["error_json"]["schema_version"] == failure.schema_version
    assert "artifact" not in " ".join(connection.cursor_instance.statements).lower()


def test_db_only_preclaim_failure_does_not_overwrite_an_already_claimed_operation() -> None:
    current = {
        "operation_id": "ahrop_1",
        "batch_id": "ahrb_1",
        "status": "RUNNING",
        "row_version": 2,
    }
    connection = _DbConnection(update_succeeds=False, select_row=current)
    repository = PostgresHistoricalRangePreclaimFailureRepository(
        conn_factory=lambda: connection
    )
    failure = HistoricalRangeBackgroundDispatchFailureV1(
        operation_id="ahrop_1",
        batch_id="ahrb_1",
        command="CATALOG_EXECUTE",
        stage="CLAIM_AND_EXECUTION",
        reason_code="ADVISORY_HR_BACKGROUND_DISPATCH_FAILED",
        error_type="RuntimeError",
        recorded_at=datetime.now(UTC),
    )
    assert repository.record_retryable_failure(failure) == current
    assert len(connection.cursor_instance.statements) == 2


def test_db_only_preclaim_failure_rejects_cross_batch_identity() -> None:
    connection = _DbConnection(
        update_succeeds=False,
        select_row={
            "operation_id": "ahrop_1",
            "batch_id": "ahrb_other",
            "status": "RUNNING",
            "row_version": 2,
        },
    )
    repository = PostgresHistoricalRangePreclaimFailureRepository(
        conn_factory=lambda: connection
    )
    failure = HistoricalRangeBackgroundDispatchFailureV1(
        operation_id="ahrop_1",
        batch_id="ahrb_1",
        command="CATALOG_EXECUTE",
        stage="CLAIM_AND_EXECUTION",
        reason_code="ADVISORY_HR_BACKGROUND_DISPATCH_FAILED",
        error_type="RuntimeError",
        recorded_at=datetime.now(UTC),
    )
    with pytest.raises(HistoricalRangeContractError, match="different batch"):
        repository.record_retryable_failure(failure)


def test_db_only_preclaim_failure_rejects_an_unrecorded_queued_state() -> None:
    connection = _DbConnection(
        update_succeeds=False,
        select_row={
            "operation_id": "ahrop_1",
            "batch_id": "ahrb_1",
            "status": "QUEUED",
            "row_version": 1,
        },
    )
    repository = PostgresHistoricalRangePreclaimFailureRepository(
        conn_factory=lambda: connection
    )
    failure = HistoricalRangeBackgroundDispatchFailureV1(
        operation_id="ahrop_1",
        batch_id="ahrb_1",
        command="CATALOG_EXECUTE",
        stage="RUNTIME_RECONSTRUCTION",
        reason_code="ADVISORY_HR_BACKGROUND_DISPATCH_FAILED",
        error_type="RuntimeError",
        recorded_at=datetime.now(UTC),
    )
    with pytest.raises(HistoricalRangeContractError, match="remained queued"):
        repository.record_retryable_failure(failure)


def _artifact_ref(kind: HistoricalRangeArtifactKind, char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    return HistoricalRangeArtifactRefV1(
        artifact_kind=kind,
        relative_path=f"{kind.value.lower()}/{digest}.json",
        producer_contract_version="test_v1",
        payload_schema_version="test_v1",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


class _BridgeRefsCursor:
    def __init__(self, result_sets):
        self._result_sets = iter(result_sets)
        self._rows = ()
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        self.statements.append((statement, params))
        self._rows = next(self._result_sets)

    def fetchall(self):
        return self._rows


class _BridgeRefsConnection:
    def __init__(self, result_sets):
        self.cursor_instance = _BridgeRefsCursor(result_sets)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def set_session(self, **_kwargs):
        return None

    def cursor(self, **_kwargs):
        return self.cursor_instance

    def rollback(self):
        return None


def test_bridge_refs_uses_fine_grained_maturity_instead_of_outer_outcome_status(
    monkeypatch,
) -> None:
    day_ref = _artifact_ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "1")
    candidate_ref = _artifact_ref(
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
        "2",
    )
    matured_ref = _artifact_ref(HistoricalRangeArtifactKind.OUTCOME, "3")
    pending_ref = _artifact_ref(HistoricalRangeArtifactKind.OUTCOME, "4")
    connection = _BridgeRefsConnection(
        (
            (
                {
                    "day_receipt_ref": day_ref.model_dump(mode="json"),
                    "candidate_artifact_ref": candidate_ref.model_dump(mode="json"),
                },
            ),
            (
                {
                    "outcome_artifact_ref": matured_ref.model_dump(mode="json"),
                    "outcome_json": {"fine_grained_status": "COMPLETE"},
                },
                {
                    "outcome_artifact_ref": pending_ref.model_dump(mode="json"),
                    "outcome_json": {"fine_grained_status": "NOT_DUE"},
                },
            ),
            (),
        )
    )
    observed_statuses = []

    def eligible(outcome_json, *, requested_maturity_statuses):
        observed_statuses.append(requested_maturity_statuses)
        return (
            (object(),)
            if outcome_json["fine_grained_status"]
            in {status.value for status in requested_maturity_statuses}
            else ()
        )

    monkeypatch.setattr(runtime_factories, "_eligible_executable_results", eligible)

    refs = runtime_factories._bridge_refs(
        conn_factory=lambda: connection,
        range_run_id="ahrr_1",
        policy_hash="a" * 64,
        horizons=(1,),
        maturity_statuses=("COMPLETE",),
    )

    assert refs["outcome_refs"] == (matured_ref,)
    assert observed_statuses == [
        (runtime_factories.HistoricalRangeOutcomeStatus.COMPLETE,),
        (runtime_factories.HistoricalRangeOutcomeStatus.COMPLETE,),
    ]
    outcome_sql, outcome_params = connection.cursor_instance.statements[1]
    assert "outcome.maturity_status = ANY" not in outcome_sql
    assert outcome_params == ("ahrr_1", [1], "a" * 64)


def _bridge_receipt(status: HistoricalRangeBridgeResultStatus) -> HistoricalRangeDatasetBridgeReceiptV1:
    sealed = status is HistoricalRangeBridgeResultStatus.SEALED
    reason_codes = (
        (REASON_DATASET_BRIDGE_VALID_EMPTY,)
        if status is HistoricalRangeBridgeResultStatus.VALID_EMPTY
        else (() if sealed else ("ADVISORY_HR_DATASET_BRIDGE_LINEAGE_CONFLICT",))
    )
    return HistoricalRangeDatasetBridgeReceiptV1(
        operation_id="ahrop_child",
        request_hash="a" * 64,
        result_status=status,
        observation_count=1 if sealed else 0,
        label_count=1 if sealed else 0,
        canonical_signal_count=1 if sealed else 0,
        range_lineage_count=0,
        retrospective_selector_policy_hash="b" * 64,
        bridge_artifact_ref=_artifact_ref(HistoricalRangeArtifactKind.DATASET_BRIDGE, "c"),
        dataset_build_id="advbuild_1" if sealed else None,
        sealed_snapshot_id="advsnap_1" if sealed else None,
        reason_codes=reason_codes,
    )


class _BridgeParentQuery:
    def __init__(self, operation):
        self.operation = dict(operation)

    def get_operation_internal(self, operation_id):
        assert operation_id == self.operation["operation_id"]
        return dict(self.operation)

    def resolved_request_hash(self, batch_id):
        assert batch_id == "ahrb_1"
        return "d" * 64


class _BridgeParentRepository:
    def __init__(self, query):
        self.query = query
        self.transitions = []

    def transition_operation(self, **kwargs):
        self.transitions.append(kwargs)
        row = self.query.operation
        assert kwargs["operation_id"] == row["operation_id"]
        if kwargs["expected_row_version"] != row["row_version"]:
            raise HistoricalRangeContractError(
                REASON_ROW_VERSION_CONFLICT,
                "operation row_version differs from the expected value",
            )
        row["row_version"] += 1
        row["status"] = kwargs["target_status"].value
        for key in (
            "attempt_no",
            "worker_id",
            "lease_token",
            "lease_expires_at",
            "fencing_token",
            "started_at",
            "finished_at",
            "result_status",
            "result_ref",
            "error_json",
            "stable_keyset_cursor_json",
        ):
            if key in kwargs:
                row[key] = kwargs[key]
        return dict(row)


class _BridgeParentApplication:
    def __init__(self, status):
        self.child_receipt = _bridge_receipt(status)
        self.child_ref = _artifact_ref(HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT, "e")
        self.parent_ref = _artifact_ref(HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT, "f")
        self.published_parent = None
        self.expired_parent_receipts = []

    def build_until_stable_boundary(self, **_kwargs):
        return self.child_receipt, self.child_ref

    def publish_parent_receipt(self, *, operation_id, child_receipt, **_kwargs):
        assert child_receipt == self.child_receipt
        self.published_parent = HistoricalRangeDatasetBridgeReceiptV1.model_validate(
            {
                **child_receipt.model_dump(mode="json", exclude={"receipt_hash"}),
                "operation_id": operation_id,
            }
        )
        return self.published_parent, self.parent_ref

    def publish_failed_parent_receipt(self, *, operation_id, result_status, reason_code, **_kwargs):
        receipt = HistoricalRangeDatasetBridgeReceiptV1(
            operation_id=operation_id,
            request_hash="a" * 64,
            result_status=result_status,
            observation_count=0,
            label_count=0,
            canonical_signal_count=0,
            range_lineage_count=0,
            retrospective_selector_policy_hash="b" * 64,
            bridge_artifact_ref=_artifact_ref(HistoricalRangeArtifactKind.DATASET_BRIDGE, "1"),
            reason_codes=(reason_code,),
        )
        ref = _artifact_ref(HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT, "2")
        self.expired_parent_receipts.append((receipt, ref))
        return receipt, ref


class _BridgeRequestFactory:
    def __init__(self, request):
        self.request = request
        self.registered_requests = []

    def build(self, *_args):
        return self.request

    def register_frozen_policy_refs(self, request):
        self.registered_requests.append(request)


def _bridge_runtime(status, *, operation_status="QUEUED", lease_expired=False):
    operation = {
        "operation_id": "ahrop_parent",
        "batch_id": "ahrb_1",
        "request_payload_sha256": "a" * 64,
        "status": operation_status,
        "row_version": 1,
        "attempt_no": 0,
        "fencing_token": 0,
        "lease_expired": lease_expired,
    }
    if operation_status == "RUNNING":
        operation.update(
            worker_id="stale-worker",
            lease_token=uuid4().hex,
            attempt_no=1,
            fencing_token=1,
            started_at=datetime.now(UTC),
        )
    query = _BridgeParentQuery(operation)
    repository = _BridgeParentRepository(query)
    bridge = _BridgeParentApplication(status)
    request = SimpleNamespace(request_hash="a" * 64)
    runtime = SimpleNamespace(
        query=query,
        repository=repository,
        bridge=bridge,
        bridge_requests=_BridgeRequestFactory(request),
    )
    return runtime, repository, bridge


@pytest.mark.parametrize(
    "child_status,expected_parent_status",
    [
        (HistoricalRangeBridgeResultStatus.SEALED, "COMPLETED"),
        (HistoricalRangeBridgeResultStatus.VALID_EMPTY, "COMPLETED"),
        (HistoricalRangeBridgeResultStatus.FAILED, "FAILED"),
        (HistoricalRangeBridgeResultStatus.RETRYABLE_FAILED, "RETRYABLE_FAILED"),
    ],
)
def test_bridge_dispatcher_closes_parent_from_authoritative_child_receipt(child_status, expected_parent_status) -> None:
    runtime, repository, bridge = _bridge_runtime(child_status)
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)
    request = HistoricalRangeBuildBridgeRequest(
        operation_idempotency_key="bridge-key",
        expected_row_version=7,
        requested_horizons=[1],
        requested_maturity_statuses=["COMPLETE"],
    )

    dispatcher._run(
        "BUILD_DATASET_BRIDGE",
        {
            "operation_id": "ahrop_parent",
            "batch_id": "ahrb_1",
            "command_payload": request.model_dump(mode="json"),
        },
    )

    assert [item["target_status"].value for item in repository.transitions] == [
        "RUNNING",
        "RUNNING",
        "RUNNING",
        expected_parent_status,
    ]
    claim, child_heartbeat, receipt_heartbeat, final = repository.transitions
    assert child_heartbeat["attempt_no"] == claim["attempt_no"]
    assert child_heartbeat["worker_id"] == claim["worker_id"]
    assert child_heartbeat["lease_token"] == claim["lease_token"]
    assert child_heartbeat["fencing_token"] == claim["fencing_token"]
    assert child_heartbeat["stable_keyset_cursor_json"] == {
        "phase": "CHILD_TERMINAL"
    }
    assert (
        child_heartbeat["lease_expires_at"] - claim["lease_expires_at"]
        >= timedelta(minutes=30)
    )
    assert receipt_heartbeat["attempt_no"] == claim["attempt_no"]
    assert receipt_heartbeat["worker_id"] == claim["worker_id"]
    assert receipt_heartbeat["lease_token"] == claim["lease_token"]
    assert receipt_heartbeat["fencing_token"] == claim["fencing_token"]
    assert receipt_heartbeat["stable_keyset_cursor_json"] == {
        "phase": "PARENT_RECEIPT_PUBLISHED"
    }
    assert (
        receipt_heartbeat["lease_expires_at"]
        - child_heartbeat["lease_expires_at"]
        >= timedelta(minutes=30)
    )
    assert child_heartbeat["expected_row_version"] == 2
    assert receipt_heartbeat["expected_row_version"] == 3
    assert final["expected_row_version"] == 4
    assert bridge.published_parent.operation_id == "ahrop_parent"
    assert final["attempt"].operation_id == "ahrop_parent"
    assert final["attempt"].status == expected_parent_status
    assert final["result_ref"] == (bridge.parent_ref if expected_parent_status in {"COMPLETED", "FAILED"} else None)


def test_bridge_dispatcher_reclaims_expired_parent_with_durable_attempt() -> None:
    runtime, repository, bridge = _bridge_runtime(
        HistoricalRangeBridgeResultStatus.VALID_EMPTY,
        operation_status="RUNNING",
        lease_expired=True,
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)
    request = HistoricalRangeBuildBridgeRequest(
        operation_idempotency_key="bridge-key",
        expected_row_version=7,
        requested_horizons=[1],
        requested_maturity_statuses=["COMPLETE"],
    )

    dispatcher._run(
        "BUILD_DATASET_BRIDGE",
        {
            "operation_id": "ahrop_parent",
            "batch_id": "ahrb_1",
            "command_payload": request.model_dump(mode="json"),
        },
    )

    claim = repository.transitions[0]
    assert claim["expired_attempt"].status == "RETRYABLE_FAILED"
    assert claim["expired_attempt"].attempt_receipt_ref == bridge.expired_parent_receipts[0][1]
    assert repository.transitions[-1]["target_status"].value == "COMPLETED"


def test_bridge_parent_stale_worker_cannot_finish_after_higher_fencing_takeover() -> None:
    runtime, repository, bridge = _bridge_runtime(
        HistoricalRangeBridgeResultStatus.SEALED
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)
    request = SimpleNamespace(request_hash="a" * 64)
    claimed = dispatcher._claim_bridge_parent(
        runtime=runtime,
        operation=runtime.query.get_operation_internal("ahrop_parent"),
        request=request,
        resolved_request_hash="d" * 64,
        worker_id="original-worker",
    )
    repository.transition_operation(
        operation_id="ahrop_parent",
        expected_row_version=claimed["row_version"],
        target_status=HistoricalRangeOperationStatus.RUNNING,
        attempt_no=2,
        worker_id="takeover-worker",
        lease_token=uuid4().hex,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=30),
        fencing_token=2,
    )
    parent_receipt = HistoricalRangeDatasetBridgeReceiptV1.model_validate(
        {
            **bridge.child_receipt.model_dump(
                mode="json", exclude={"receipt_hash"}
            ),
            "operation_id": "ahrop_parent",
        }
    )

    with pytest.raises(
        HistoricalRangeContractError,
        match="row_version differs",
    ) as stale_finish:
        dispatcher._finish_bridge_parent(
            runtime=runtime,
            operation=claimed,
            request=request,
            receipt=parent_receipt,
            receipt_ref=bridge.parent_ref,
            target_status=HistoricalRangeOperationStatus.COMPLETED,
            error_json=None,
        )

    assert stale_finish.value.reason_code == REASON_ROW_VERSION_CONFLICT
    assert runtime.query.operation["status"] == "RUNNING"
    assert runtime.query.operation["attempt_no"] == 2
    assert runtime.query.operation["fencing_token"] == 2


def test_bridge_dispatcher_records_unexpected_child_failure_on_parent() -> None:
    runtime, repository, bridge = _bridge_runtime(HistoricalRangeBridgeResultStatus.VALID_EMPTY)
    bridge.build_until_stable_boundary = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("child failed unexpectedly")
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)
    request = HistoricalRangeBuildBridgeRequest(
        operation_idempotency_key="bridge-key",
        expected_row_version=7,
        requested_horizons=[1],
        requested_maturity_statuses=["COMPLETE"],
    )

    dispatcher._run(
        "BUILD_DATASET_BRIDGE",
        {
            "operation_id": "ahrop_parent",
            "batch_id": "ahrb_1",
            "command_payload": request.model_dump(mode="json"),
        },
    )

    final = repository.transitions[-1]
    assert final["target_status"].value == "FAILED"
    assert final["error_json"] == {
        "reason_codes": ["ADVISORY_HR_DATASET_BRIDGE_PARENT_FAILED"],
        "stage": "DATASET_BRIDGE_SUB_OPERATION",
        "error_type": "RuntimeError",
    }
    assert final["attempt"].attempt_receipt_ref == bridge.expired_parent_receipts[0][1]


def test_bridge_dispatcher_keeps_active_child_conflict_retryable() -> None:
    runtime, repository, bridge = _bridge_runtime(HistoricalRangeBridgeResultStatus.VALID_EMPTY)
    bridge.build_until_stable_boundary = lambda **_kwargs: (_ for _ in ()).throw(
        HistoricalRangeDatasetBridgeError(
            REASON_REPOSITORY_CONFLICT,
            "child operation already has an active lease",
            retryable=True,
        )
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)
    request = HistoricalRangeBuildBridgeRequest(
        operation_idempotency_key="bridge-key",
        expected_row_version=7,
        requested_horizons=[1],
        requested_maturity_statuses=["COMPLETE"],
    )

    dispatcher._run(
        "BUILD_DATASET_BRIDGE",
        {
            "operation_id": "ahrop_parent",
            "batch_id": "ahrb_1",
            "command_payload": request.model_dump(mode="json"),
        },
    )

    final = repository.transitions[-1]
    assert final["target_status"].value == "RETRYABLE_FAILED"
    assert final["result_ref"] is None
    assert final["error_json"]["reason_codes"] == [REASON_REPOSITORY_CONFLICT]
    assert bridge.expired_parent_receipts[0][0].result_status is (HistoricalRangeBridgeResultStatus.RETRYABLE_FAILED)


def test_bridge_dispatcher_fails_closed_on_nonretryable_lineage_conflict() -> None:
    runtime, repository, bridge = _bridge_runtime(HistoricalRangeBridgeResultStatus.VALID_EMPTY)
    bridge.build_until_stable_boundary = lambda **_kwargs: (_ for _ in ()).throw(
        HistoricalRangeDatasetBridgeError(
            REASON_REPOSITORY_CONFLICT,
            "child artifact identity differs from its receipt",
        )
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)
    request = HistoricalRangeBuildBridgeRequest(
        operation_idempotency_key="bridge-key",
        expected_row_version=7,
        requested_horizons=[1],
        requested_maturity_statuses=["COMPLETE"],
    )

    dispatcher._run(
        "BUILD_DATASET_BRIDGE",
        {
            "operation_id": "ahrop_parent",
            "batch_id": "ahrb_1",
            "command_payload": request.model_dump(mode="json"),
        },
    )

    final = repository.transitions[-1]
    assert final["target_status"].value == "FAILED"
    assert final["result_ref"] == bridge.expired_parent_receipts[0][1]
    assert bridge.expired_parent_receipts[0][0].result_status is (HistoricalRangeBridgeResultStatus.FAILED)
