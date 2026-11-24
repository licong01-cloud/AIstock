from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range import catalog_postgres
from backend.services.advisory_historical_range.executor import HistoricalRangeDayExecutor
from backend.services.advisory_historical_range.models import (
    HistoricalRangeContractError,
    HistoricalRangeDayStatus,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionMemberV1,
    HistoricalRangeSourceRevisionRefV1,
)
from backend.services.advisory_historical_range.source_roles import DECISION_MARK_SOURCE_ROLES_V1
from backend.tests.advisory_historical_range.conftest import digest
from backend.tests.advisory_historical_range.test_r2b_candidate_producer import TRADE_DATE, _fixture
from backend.tests.advisory_historical_range.test_r3_executor import (
    _Repository,
    _sealed_execution_context,
)


def _with_mark_source(request):
    member = HistoricalRangeSourceRevisionMemberV1(
        requirement_id="decision-market",
        source_role="decision_mark_daily_market",
        dataset_id="market.kline_daily_raw+market.adj_factor",
        partition_ref=TRADE_DATE.isoformat(),
        decision_trade_date=TRADE_DATE,
        query_template_id="historical_decision_mark_daily_market",
        query_template_version="v1",
        query_template_hash=digest("daily-query"),
        bound_parameters={"trade_date": TRADE_DATE.isoformat()},
        parameter_hash=digest({"trade_date": TRADE_DATE.isoformat()}),
        row_count=5476,
        content_hash=digest("sealed-factor-181.704"),
        admissibility="RETROSPECTIVE_DB_CONTENT_HASH",
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    document = request.source_revision_catalog.model_dump(mode="python", exclude={"catalog_hash"})
    document["members"] = (*request.source_revision_catalog.members, member)
    catalog = HistoricalRangeSourceRevisionCatalogV1.model_validate(document)
    resolved = request.resolved_request.model_dump(mode="python")
    semantic = request.resolved_request.semantic_payload()
    semantic["source_revision_catalog_hash"] = catalog.catalog_hash
    resolved.update(source_revision_catalog_hash=catalog.catalog_hash, request_payload_sha256=digest(semantic))
    return type(request)(resolved_request=type(request.resolved_request).model_validate(resolved),
                         source_revision_catalog=catalog), member


@pytest.mark.parametrize("batched", [False, True])
def test_already_changed_mark_partition_stops_before_inference(tmp_path, batched):
    producer, request, _verifier, program = _fixture(tmp_path)
    request, member = _with_mark_source(request)
    calls = []

    def verify(**kwargs):
        calls.append(kwargs["source_roles"])
        if kwargs["source_roles"] == DECISION_MARK_SOURCE_ROLES_V1:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_SOURCE_REVISION_MISMATCH", "historical factor changed",
                context={"source_role": member.source_role},
            )
        return request.source_revision_catalog.source_revision_refs()

    producer._source_verifier = SimpleNamespace(verify_program_day=verify)
    with pytest.raises(HistoricalRangeContractError, match="historical factor changed"):
        if batched:
            producer.produce_dates(request_payload=request, research_program_id=program.research_program_id,
                                   decision_trade_dates=(TRADE_DATE,), current_trade_date=TRADE_DATE)
        else:
            producer.produce(request_payload=request, research_program_id=program.research_program_id,
                             decision_trade_date=TRADE_DATE)
    assert DECISION_MARK_SOURCE_ROLES_V1 in calls
    assert producer._signal_preparation.calls == []
    assert list((tmp_path / "cas" / "candidate-artifacts").glob("*.json")) == []


def test_unchanged_mark_source_preserves_candidate_evidence_and_inference(tmp_path):
    producer, request, _verifier, program = _fixture(tmp_path)
    request, mark = _with_mark_source(request)
    checked_roles = []

    def verify(**kwargs):
        roles = kwargs["source_roles"]
        checked_roles.append(roles)
        return tuple(HistoricalRangeSourceRevisionRefV1(revision_id=m.revision_id, revision_hash=m.revision_hash)
                     for m in request.source_revision_catalog.members if m.source_role in roles)

    producer._source_verifier = SimpleNamespace(verify_program_day=verify)
    result = producer.produce(request_payload=request, research_program_id=program.research_program_id,
                              decision_trade_date=TRADE_DATE)
    assert len(producer._signal_preparation.calls) == 1
    assert checked_roles.count(DECISION_MARK_SOURCE_ROLES_V1) == 1
    assert mark.revision_id not in {ref.revision_id for ref in result.source_revision_refs}
    assert len(result.source_revision_refs) == len(request.source_revision_catalog.members) - 1


def test_catalog_mismatch_reports_exact_partition_and_content(tmp_path, monkeypatch):
    _producer, request, _verifier, program = _fixture(tmp_path)
    request, member = _with_mark_source(request)
    document = member.model_dump(mode="python", exclude={"revision_id", "revision_hash"})
    document["content_hash"] = digest("corrected-factor-181.7038")
    actual = HistoricalRangeSourceRevisionMemberV1.model_validate(document)
    monkeypatch.setattr(catalog_postgres._PostgresRequirementResolver, "resolve", lambda *_a, **_k: actual)

    @contextmanager
    def cursor(**_kwargs):
        yield SimpleNamespace(execute=lambda *_a: None,
                              fetchone=lambda: {"observed_at": datetime.now(UTC)})

    @contextmanager
    def connection():
        yield SimpleNamespace(set_session=lambda **_k: None, cursor=cursor, rollback=lambda: None)

    verifier = catalog_postgres.PostgresHistoricalRangeSourceRevisionVerifier(conn_factory=connection)
    with pytest.raises(HistoricalRangeContractError) as captured:
        verifier.verify_program_day(catalog=request.source_revision_catalog,
                                    research_program_id=program.research_program_id,
                                    package_id=program.package_id, component_ids={"alpha"},
                                    decision_trade_date=TRADE_DATE,
                                    source_roles=DECISION_MARK_SOURCE_ROLES_V1)
    context = captured.value.context
    assert context["partition_ref"] == TRADE_DATE.isoformat()
    assert context["dataset_id"] == member.dataset_id
    assert context["expected_content_hash"] == member.content_hash
    assert context["actual_content_hash"] == actual.content_hash
    assert context["expected_row_count"] == context["actual_row_count"] == 5476


def test_source_drift_evidence_survives_failure_receipt_without_arbitrary_context(tmp_path):
    store, batch, run, claim, *_rest = _sealed_execution_context(tmp_path)
    repository = _Repository(batch=batch, run=run, claim=claim)
    safe = {"source_role": "decision_mark_daily_market", "dataset_id": "market.adj_factor",
            "partition_ref": "2026-01-13", "revision_id": "frozen-revision",
            "expected_content_hash": digest("before"), "actual_content_hash": digest("after"),
            "expected_row_count": 5476, "actual_row_count": 5476}

    def fail(**_kwargs):
        raise HistoricalRangeContractError(
            "ADVISORY_HR_SOURCE_REVISION_MISMATCH", "not persisted: private connection detail",
            context={**safe, "dsn": "must-not-persist", "password": "must-not-persist"},
        )

    executor = HistoricalRangeDayExecutor(repository=repository, artifact_store=store,
                                          candidate_producer=SimpleNamespace(produce=fail),
                                          decision_mark_provider=SimpleNamespace())
    result = executor.execute_batch_slice(batch_id=batch.batch_id, worker_id="test-worker",
                                         max_day_commits_per_slice=1)
    assert result[0].status is HistoricalRangeDayStatus.FAILED
    assert repository.commit_calls == []
    attempt = repository.failure_calls[0]["attempt"]
    assert attempt.error_json["source_revision_context"] == safe
    assert "must-not-persist" not in str(attempt.error_json)
    assert "private connection" not in str(attempt.error_json)
    receipt = store.load(attempt.attempt_receipt_ref)
    assert receipt.payload["sanitized_error"] == attempt.error_json
