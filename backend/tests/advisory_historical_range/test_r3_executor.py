from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.executor import (
    HistoricalRangeBatchExecutionService,
    HistoricalRangeDayExecutor,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeBatchStatus,
    HistoricalRangeClaimedDayV1,
    HistoricalRangeDayStatus,
    HistoricalRangeExecutionBatchV1,
    HistoricalRangeExecutionRunV1,
    HistoricalRangePredecessorStateV1,
    HistoricalRangeProgramStatus,
    HistoricalRangeResolvedRequestArtifactPayloadV1,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionMemberV1,
    HistoricalRangeRevisionAdmissibility,
    build_candidate_input_hash,
)
from backend.services.advisory_historical_range.semantics import canonical_list_semantics_v2
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.advisory_historical_range.conftest import digest, research_spec, resolved_request
from backend.tests.advisory_historical_range.test_r3_list_projection import _candidate_payload, _mark_set, _program


class _Repository:
    def __init__(self, *, batch, run, claim) -> None:
        self.batch = batch
        self.run = run
        self.claim = claim
        self.claimed_trade_date = claim.decision_trade_date
        self.commit_calls: list[dict] = []
        self.failure_calls: list[dict] = []

    def load_execution_batch(self, *, batch_id):
        assert batch_id == self.batch.batch_id
        return self.batch

    def list_execution_runs(self, **_kwargs):
        return (self.run,)

    def transition_batch(self, **_kwargs):
        self.batch = self.batch.model_copy(
            update={"status": _kwargs["target_status"], "row_version": self.batch.row_version + 1}
        )
        return {"status": self.batch.status.value, "row_version": self.batch.row_version}

    def transition_run(self, **_kwargs):
        self.run = self.run.model_copy(
            update={"status": _kwargs["target_status"], "row_version": self.run.row_version + 1}
        )
        return {"status": self.run.status.value, "row_version": self.run.row_version}

    def materialize_day_plan_chunk(self, **_kwargs):
        raise AssertionError("test run is already materialized")

    def claim_next_day(self, **_kwargs):
        claim, self.claim = self.claim, None
        return claim

    def load_expired_claimable_day(self, **_kwargs):
        return None

    def load_reusable_candidate_ref(self, **_kwargs):
        return None

    def load_predecessor_state(self, *, day_run_id):
        assert day_run_id == "day_1"
        return HistoricalRangePredecessorStateV1(day_run_id=day_run_id)

    def load_episode_entry_sequences(self, *, range_run_id):
        assert range_run_id == "range_1"
        return {}

    def commit_successful_day(self, **kwargs):
        self.commit_calls.append(kwargs)
        self.successful_readback = SimpleNamespace(
            receipt_ref=kwargs["day_receipt_ref"],
            decision_trade_date=self.claimed_trade_date,
            receipt=SimpleNamespace(list_version=kwargs["list_version"]),
        )

    def finish_failed_day(self, **kwargs):
        self.failure_calls.append(kwargs)

    def full_readback_successful_day(self, **_kwargs):
        return self.successful_readback

    def load_run_finalization_facts(self, **_kwargs):
        if self.commit_calls:
            return SimpleNamespace(
                run=self.run,
                resolved_request_hash=self.batch.resolved_request_hash,
                total_day_count=1,
                successful_days=(self.successful_readback,),
                blocking_day_run_id=None,
                blocking_ordinal=None,
                blocking_trade_date=None,
                blocking_status=None,
                blocking_attempt_receipt_ref=None,
                unexecuted_day_count=0,
                cancelled_from_ordinal=None,
            )
        failure = self.failure_calls[-1]
        return SimpleNamespace(
            run=self.run,
            resolved_request_hash=self.batch.resolved_request_hash,
            total_day_count=1,
            successful_days=(),
            blocking_day_run_id="day_1",
            blocking_ordinal=1,
            blocking_trade_date=self.claimed_trade_date,
            blocking_status=failure["target_status"],
            blocking_attempt_receipt_ref=failure["attempt"].attempt_receipt_ref,
            unexecuted_day_count=1,
            cancelled_from_ordinal=None,
        )

    def finish_range_run(self, **kwargs):
        self.run = self.run.model_copy(
            update={
                "status": kwargs["target_status"],
                "row_version": self.run.row_version + 1,
                "final_receipt_ref": kwargs["final_receipt_ref"],
                "final_receipt_hash": kwargs["final_receipt_ref"].semantic_content_hash,
            }
        )
        return {"status": self.run.status.value, "row_version": self.run.row_version}

    def heartbeat_day(self, **kwargs):
        return kwargs["claimed_day"]


def _sealed_execution_context(tmp_path):
    store = HistoricalRangeArtifactStore(root=tmp_path / "artifacts")
    program = _program()
    base = resolved_request(
        specs=(research_spec(target_count=2),),
        trade_dates=(date(2026, 6, 3),),
    )
    semantics = canonical_list_semantics_v2()
    catalog = HistoricalRangeSourceRevisionCatalogV1(
        requirement_plan_hash=digest("plan"),
        catalog_generation=1,
        query_contract_hash=digest("query"),
        calendar_identity_hash=digest("calendar"),
        members=(
            HistoricalRangeSourceRevisionMemberV1(
                requirement_id="catalog-member",
                source_role="pit_universe",
                dataset_id="market.stock_universe_pit",
                partition_ref="test/2026-06-03",
                decision_trade_date=date(2026, 6, 3),
                query_template_id="test",
                query_template_version="v1",
                query_template_hash=digest("query-template"),
                parameter_hash=digest("parameters"),
                row_count=1,
                content_hash=digest("catalog-content"),
                admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
                observed_at=datetime(2026, 7, 22, tzinfo=UTC),
            ),
        ),
    )
    resolved_document = base.model_dump(mode="json")
    resolved_document.update(
        {
            "frozen_programs": (program.model_dump(mode="json"),),
            "list_semantics_version": semantics.schema_version,
            "list_semantics_hash": semantics.semantics_hash,
            "source_revision_catalog_hash": catalog.catalog_hash,
            "resolved_program_set_hash": None,
            "request_payload_sha256": None,
        }
    )
    resolved = type(base).model_validate(resolved_document)
    request_payload = HistoricalRangeResolvedRequestArtifactPayloadV1(
        resolved_request=resolved,
        source_revision_catalog=catalog,
    )
    request_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="test",
        payload_schema_version=request_payload.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=request_payload.model_dump(mode="json"),
    ).ref
    date_plan_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DATE_PLAN,
        producer_contract_version="test",
        payload_schema_version=resolved.date_plan.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=resolved.date_plan.model_dump(mode="json"),
        upstream_refs=(request_ref,),
    ).ref
    batch = HistoricalRangeExecutionBatchV1(
        batch_id=resolved.batch_id,
        status=HistoricalRangeBatchStatus.RUNNING,
        row_version=2,
        resolved_request_hash=resolved.request_payload_sha256,
        request_ref=request_ref,
        date_plan_ref=date_plan_ref,
        artifact_root_identity_hash=store.root_identity_hash,
    )
    run = HistoricalRangeExecutionRunV1(
        batch_id=resolved.batch_id,
        range_run_id="range_1",
        research_program_id=program.research_program_id,
        status=HistoricalRangeProgramStatus.RUNNING,
        row_version=2,
        materialized_day_count=1,
        day_plan_cursor_ordinal=1,
    )
    claim = HistoricalRangeClaimedDayV1(
        batch_id=resolved.batch_id,
        range_run_id="range_1",
        research_program_id=program.research_program_id,
        day_run_id="day_1",
        decision_trade_date=date(2026, 6, 3),
        ordinal=1,
        row_version=3,
        attempt_no=1,
        worker_id="test-worker",
        lease_token=digest("executor lease fixture"),
        fencing_token=1,
        lease_expires_at=datetime(2026, 6, 3, 16, tzinfo=UTC),
        resolved_request_hash=resolved.request_payload_sha256,
        request_ref=request_ref,
        list_semantics_version=semantics.schema_version,
        list_semantics_hash=semantics.semantics_hash,
    )
    candidate_payload = _candidate_payload()
    candidate_document = candidate_payload.model_dump(mode="json")
    candidate_document.update(
        {
            "candidate_input_hash": build_candidate_input_hash(
                range_run_id="range_1",
                research_program_id=program.research_program_id,
                decision_trade_date=candidate_payload.decision_trade_date,
                frozen_program_hash=str(program.frozen_program_hash),
                runtime_profile_hash=candidate_payload.runtime_profile_hash,
                code_release_hash=program.code_release_hash,
                selection_semantics_hash=program.selection_semantics_hash,
                calendar_identity_hash=catalog.calendar_identity_hash,
                universe_identity_hash=candidate_payload.universe_identity_hash,
                source_revision_catalog_hash=str(catalog.catalog_hash),
                query_contract_hash=catalog.query_contract_hash,
            ),
            "stage_closure_hash": None,
        }
    )
    candidate_payload = type(candidate_payload).model_validate(candidate_document)
    candidate_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
        producer_contract_version="test",
        payload_schema_version=candidate_payload.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=candidate_payload.model_dump(mode="json"),
        range_run_id="range_1",
        day_run_id="day_1",
        source_revision_refs=candidate_payload.source_revision_refs,
        upstream_refs=(request_ref,),
    ).ref
    mark_set = _mark_set()
    mark_document = mark_set.model_dump(mode="json")
    mark_document.update({"upstream_request_ref": request_ref.model_dump(mode="json"), "mark_set_hash": None})
    mark_set = type(mark_set).model_validate(mark_document)
    mark_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DECISION_MARK_SET,
        producer_contract_version="test",
        payload_schema_version=mark_set.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=mark_set.model_dump(mode="json"),
        range_run_id="range_1",
        day_run_id="day_1",
        source_revision_refs=mark_set.source_revision_refs,
        upstream_refs=(request_ref,),
    ).ref
    return store, batch, run, claim, request_payload, candidate_payload, candidate_ref, mark_set, mark_ref


def test_execute_until_blocked_commits_closed_candidate_mark_list_receipt_chain(tmp_path) -> None:
    store, batch, run, claim, _request, candidate_payload, candidate_ref, mark_set, mark_ref = _sealed_execution_context(tmp_path)
    repository = _Repository(batch=batch, run=run, claim=claim)
    candidate_producer = SimpleNamespace(
        produce=lambda **_kwargs: SimpleNamespace(candidate_artifact_ref=candidate_ref)
    )
    mark_provider = SimpleNamespace(produce=lambda **_kwargs: SimpleNamespace(mark_set=mark_set, artifact_ref=mark_ref))
    service = HistoricalRangeBatchExecutionService(
        day_executor=HistoricalRangeDayExecutor(
            repository=repository,
            artifact_store=store,
            candidate_producer=candidate_producer,
            decision_mark_provider=mark_provider,
        )
    )

    result = service.execute_until_blocked(batch_id=batch.batch_id, worker_id="test-worker", day_slice_size=1)

    assert result.successful_day_count == 1
    assert result.failed_day_count == 0
    assert len(repository.commit_calls) == 1
    committed = repository.commit_calls[0]
    assert committed["candidate_artifact_ref"] == candidate_ref
    assert committed["decision_mark_set_ref"] == mark_ref
    assert committed["day_receipt_ref"].artifact_kind is HistoricalRangeArtifactKind.DAY_RECEIPT
    receipt = store.load(committed["day_receipt_ref"])
    assert receipt.upstream_refs == tuple(sorted((candidate_ref, mark_ref), key=lambda ref: (ref.artifact_kind.value, ref.semantic_content_hash, ref.relative_path)))
    assert candidate_payload.candidates == committed["candidates"]
    assert repository.failure_calls == []


def test_execute_until_blocked_recovers_run_receipt_when_day_was_already_committed(tmp_path) -> None:
    store, batch, run, claim, _request, _candidate_payload, _candidate_ref, _mark_set_value, _mark_ref = (
        _sealed_execution_context(tmp_path)
    )
    committed_receipt_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
        producer_contract_version="test",
        payload_schema_version="test_committed_day_receipt_v1",
        resolved_request_hash=batch.resolved_request_hash,
        payload={"day_run_id": claim.day_run_id},
        range_run_id=claim.range_run_id,
        day_run_id=claim.day_run_id,
    ).ref
    repository = _Repository(batch=batch, run=run, claim=claim)
    repository.claim = None
    repository.commit_calls.append({"already_committed": True})
    repository.successful_readback = SimpleNamespace(
        receipt_ref=committed_receipt_ref,
        decision_trade_date=date(2026, 6, 3),
        receipt=SimpleNamespace(list_version=SimpleNamespace(list_content_hash=digest("list"))),
    )
    service = HistoricalRangeBatchExecutionService(
        day_executor=HistoricalRangeDayExecutor(
            repository=repository,
            artifact_store=store,
            candidate_producer=SimpleNamespace(),
            decision_mark_provider=SimpleNamespace(),
        )
    )

    result = service.execute_until_blocked(batch_id=batch.batch_id, worker_id="recovery-worker")

    assert result.executed_day_count == 0
    assert repository.run.status is HistoricalRangeProgramStatus.COMPLETED
    assert repository.run.final_receipt_ref is not None
    assert repository.batch.status is HistoricalRangeBatchStatus.COMPLETED


def test_executor_persists_visible_retryable_failure_after_candidate_artifact(tmp_path) -> None:
    store, batch, run, claim, _request, _candidate_payload, candidate_ref, _mark_set, _mark_ref = _sealed_execution_context(tmp_path)
    repository = _Repository(batch=batch, run=run, claim=claim)
    candidate_producer = SimpleNamespace(
        produce=lambda **_kwargs: SimpleNamespace(candidate_artifact_ref=candidate_ref)
    )

    def _raise_mark_failure(**_kwargs):
        raise RuntimeError("test mark read failure")

    day_executor = HistoricalRangeDayExecutor(
        repository=repository,
        artifact_store=store,
        candidate_producer=candidate_producer,
        decision_mark_provider=SimpleNamespace(produce=_raise_mark_failure),
    )
    result = day_executor.execute_batch_slice(batch_id=batch.batch_id, worker_id="test-worker", max_day_commits_per_slice=1)

    assert result[0].status.value == "RETRYABLE_FAILED"
    assert result[0].reason_codes == ("ADVISORY_HR_DAY_UNCLASSIFIED_FAILURE",)
    assert repository.commit_calls == []
    assert len(repository.failure_calls) == 1
    attempt = repository.failure_calls[0]["attempt"]
    receipt = store.load(attempt.attempt_receipt_ref)
    assert receipt.payload_schema_version == "advisory_historical_range_day_attempt_receipt_v1"
    assert receipt.payload["input_hash_kind"] == "CANDIDATE_BOUND_INPUT"
    assert receipt.upstream_refs[0].artifact_kind is HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT


def test_executor_preserves_trading_core_reason_and_domain_error_code(tmp_path) -> None:
    store, batch, run, claim, _request, _candidate_payload, _candidate_ref, _mark_set, _mark_ref = (
        _sealed_execution_context(tmp_path)
    )
    repository = _Repository(batch=batch, run=run, claim=claim)

    def _raise_package_input(**_kwargs):
        raise DataUnavailableError(
            "package factor source unavailable",
            context={"reason_code": "STRATEGY_PACKAGE_RUNTIME_ASSETS_INCOMPLETE"},
        )

    executor = HistoricalRangeDayExecutor(
        repository=repository,
        artifact_store=store,
        candidate_producer=SimpleNamespace(produce=_raise_package_input),
        decision_mark_provider=SimpleNamespace(),
    )

    result = executor.execute_batch_slice(
        batch_id=batch.batch_id,
        worker_id="test-worker",
        max_day_commits_per_slice=1,
    )

    assert result[0].status is HistoricalRangeDayStatus.WAITING_INPUT
    assert result[0].reason_codes == ("STRATEGY_PACKAGE_RUNTIME_ASSETS_INCOMPLETE",)
    attempt = repository.failure_calls[0]["attempt"]
    assert attempt.error_json == {
        "reason_codes": ["STRATEGY_PACKAGE_RUNTIME_ASSETS_INCOMPLETE"],
        "stage": "CLAIM_INPUT",
        "error_type": "DataUnavailableError",
        "domain_error_code": "DATA_UNAVAILABLE",
    }
    assert repository.failure_calls[0]["error_json"] == attempt.error_json


def test_future_candidate_prefetch_failure_does_not_fail_current_day(tmp_path) -> None:
    store, batch, run, claim, _request_payload, _candidate_payload, candidate_ref, _mark_set, _mark_ref = (
        _sealed_execution_context(tmp_path)
    )
    future_date = date(2026, 6, 4)
    request = SimpleNamespace(
        resolved_request=SimpleNamespace(
            date_plan=SimpleNamespace(ordered_trade_dates=(claim.decision_trade_date, future_date))
        )
    )

    def _produce(*, decision_trade_date, **_kwargs):
        if decision_trade_date == future_date:
            raise RuntimeError("future prefetch unavailable")
        return SimpleNamespace(candidate_artifact_ref=candidate_ref)

    executor = HistoricalRangeDayExecutor(
        repository=_Repository(batch=batch, run=run, claim=claim),
        artifact_store=store,
        candidate_producer=SimpleNamespace(produce=_produce),
        decision_mark_provider=SimpleNamespace(),
    )
    assert executor._produce_prefetched_candidates(
        request_payload=request,
        claim=claim,
        candidate_prefetch_per_program=2,
    ) == candidate_ref
