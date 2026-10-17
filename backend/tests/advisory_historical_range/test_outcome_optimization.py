"""Direct performance contracts: count repeated work, not machine-speed guesses."""
from datetime import date

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeContractError,
    HistoricalRangeOutcomeArtifactV2,
    HistoricalRangeOutcomeFactV1,
)
from backend.services.advisory_historical_range.outcome_file_inputs import HistoricalRangeOutcomeReadCache
from backend.services.advisory_historical_range.outcome_projection import HistoricalRangeOutcomeProjectionBuilder
from backend.services.advisory_historical_range.outcome_service import HistoricalRangeOutcomeApplicationService
from backend.tests.advisory_historical_range.test_r4_outcome_service import (
    _Evaluator, _Planner, _Repository, _ref, _request_contract, _work_item,
)


def _fact(store):
    item = _work_item()
    result = _Evaluator().evaluate(item)
    builder = HistoricalRangeOutcomeProjectionBuilder()
    artifact = builder.build_artifact(work_item=item, result=result, outcome_version=1)
    ref = store.publish_payload(artifact_kind=HistoricalRangeArtifactKind.OUTCOME,
        producer_contract_version="r4_v1", payload_schema_version=artifact.schema_version,
        resolved_request_hash="9" * 64, payload=artifact.model_dump(mode="json"),
        range_run_id="run-1", upstream_refs=artifact.direct_upstream_refs).ref
    return builder.build_fact(work_item=item, result=result, artifact=artifact,
        outcome_artifact_ref=ref, outcome_version=1)


def test_readonly_cache_reuses_objects_but_public_load_remains_mutable(tmp_path, monkeypatch):
    store = HistoricalRangeOutcomeReadCache(root=tmp_path)
    ref = store.publish_payload(artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="test_v1", payload_schema_version="test_v1",
        resolved_request_hash="a" * 64, payload={"rows": [{"value": 1}]}).ref
    loads = []
    original = HistoricalRangeArtifactStore.load
    def counted(self, ref):
        loads.append(ref)
        return original(self, ref)
    monkeypatch.setattr(HistoricalRangeArtifactStore, "load", counted)
    first = store.load_readonly(ref)
    assert store.load_readonly(ref) is first
    with pytest.raises(TypeError):
        first.payload["rows"][0]["value"] = 2
    mutable = store.load(ref)
    mutable.payload["rows"][0]["value"] = 3
    assert store.load_readonly(ref).payload["rows"][0]["value"] == 1
    assert len(loads) == 1
    path = tmp_path / ref.relative_path
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="changed"):
        store.load_readonly(ref)


def test_fact_retains_validated_typed_artifact_without_changing_financial_hash(tmp_path, monkeypatch):
    fact = _fact(HistoricalRangeOutcomeReadCache(root=tmp_path))
    payload = fact.model_dump(mode="json")
    restored = HistoricalRangeOutcomeFactV1.model_validate(payload)
    assert restored.model_dump(mode="json") == payload
    assert restored.outcome_content_hash == canonical_json_sha256(
        restored.model_dump(mode="json", exclude={"outcome_content_hash"}))
    copied = restored.model_copy(deep=True)
    assert copied.validated_artifact.model_dump(mode="json") == restored.validated_artifact.model_dump(mode="json")
    replacement = restored.model_copy(update={"outcome_json": {}})
    with pytest.raises(ValueError):
        _ = replacement.validated_artifact
    monkeypatch.setattr(HistoricalRangeOutcomeArtifactV2, "model_validate",
        lambda *args, **kwargs: pytest.fail("same immutable fact parsed again"))
    assert restored.validated_artifact.outcome_input_hash == restored.outcome_input_hash
    assert restored.validated_artifact is restored.validated_artifact
    with pytest.raises(TypeError):
        restored.outcome_json["calculation_results"][0]["reason_codes"] = ["forged"]


def test_explicit_defer_skips_summary_and_is_visible_in_receipt(tmp_path):
    from backend.services.advisory_historical_range.models import HistoricalRangeOutcomeRefreshRequestV1
    raw = _request_contract().model_dump(mode="json", exclude={"request_hash"})
    request = HistoricalRangeOutcomeRefreshRequestV1.model_validate({**raw, "summary_mode": "DEFER"})
    class Summary:
        def refresh(self, **kwargs):
            pytest.fail("intermediate stage recalculated full summary")
    service = HistoricalRangeOutcomeApplicationService(repository=_Repository(),
        artifact_store=HistoricalRangeOutcomeReadCache(root=tmp_path),
        planner=_Planner(_work_item()), evaluator=_Evaluator(), summary_coordinator=Summary())
    receipt, _ = service.refresh_until_stable_boundary(request=request,
        resolved_request_hash="9" * 64, worker_id="optimization-test")
    assert receipt.status == "COMPLETED" and receipt.processed_count == 1
    assert receipt.summary_deferred is True and receipt.summary_refs == ()


def test_legacy_request_hash_and_api_payload_remain_unchanged():
    from backend.services.advisory_historical_range.api_models import HistoricalRangeRefreshOutcomesRequest
    request = _request_contract()
    legacy = request.model_dump(mode="json", exclude={"request_hash", "file_source_binding", "summary_mode"})
    assert request.request_hash == canonical_json_sha256(legacy)
    api = HistoricalRangeRefreshOutcomesRequest(operation_idempotency_key="test", expected_row_version=1,
        label_as_of_trade_date=date(2026, 7, 10), horizons=[1])
    assert "summary_mode" not in api.model_dump(mode="json")


def test_file_verification_session_hashes_once_and_rejects_drift(tmp_path, monkeypatch):
    from backend.services.advisory_historical_range import outcome_file_source as source
    path = tmp_path / "sealed.bin"
    path.write_bytes(b"original")
    original = source._sha256
    calls = []
    def counted(path):
        calls.append(path)
        return original(path)
    monkeypatch.setattr(source, "_sha256", counted)
    session = source.OutcomeFileVerificationSession()
    digest = session.digest(path)
    assert session.digest(path, expected=digest) == digest
    assert len(calls) == 1
    with pytest.raises(Exception, match="hash"):
        session.digest(path, expected="f" * 64)
    path.write_bytes(b"changed-content")
    with pytest.raises(Exception, match="changed"):
        session.digest(path, expected=digest)


def test_summary_only_cannot_evaluate_or_append_financial_facts(tmp_path):
    from backend.services.advisory_historical_range.models import HistoricalRangeOutcomeRefreshRequestV1
    raw = _request_contract().model_dump(mode="json", exclude={"request_hash"})
    request = HistoricalRangeOutcomeRefreshRequestV1.model_validate({**raw, "summary_mode": "ONLY"})
    calls = []
    class Summary:
        def refresh(self, *, range_run_id, assert_active):
            assert_active()
            calls.append(range_run_id)
            return _ref(HistoricalRangeArtifactKind.SUMMARY, "e")
    class Forbidden:
        def plan_slice(self, **kwargs):
            pytest.fail("summary-only traversed financial planner")
        def evaluate(self, **kwargs):
            pytest.fail("summary-only evaluated a financial value")
    repository = _Repository()
    service = HistoricalRangeOutcomeApplicationService(repository=repository,
        artifact_store=HistoricalRangeOutcomeReadCache(root=tmp_path),
        planner=Forbidden(), evaluator=Forbidden(), summary_coordinator=Summary())
    receipt, _ = service.refresh_until_stable_boundary(request=request,
        resolved_request_hash="9" * 64, worker_id="summary-test")
    assert receipt.status == "COMPLETED" and not receipt.summary_deferred
    assert receipt.processed_count == 0 and calls == ["run-1"] and repository.append_count == 0
    assert receipt.summary_refs == (_ref(HistoricalRangeArtifactKind.SUMMARY, "e"),)


def _manifest():
    identity = {"producer_code_hash": "a" * 64, "file_binding_hash": "b" * 64, "policy_bundle_hash": "c" * 64}
    def stage(name, batch, run, dependencies=()):
        return {"name": name, "batch_id": batch, "range_run_id": run, "depends_on": list(dependencies),
            "request": {"operation_idempotency_key": name, "expected_row_version": 1,
                "label_as_of_trade_date": "2026-07-10", "horizons": [1], "range_run_ids": [run],
                "summary_mode": "DEFER", "expected_execution_identity": identity}}
    return {"schema_version": "advisory_outcome_continuation_v1", "stages": [
        stage("first", "batch-1", "run-1"),
        stage("dependent", "batch-1", "run-1", ("first",)),
        stage("independent", "batch-2", "run-2"),
    ]}


class _API:
    def __init__(self):
        self.operations = {}
        self.posts = []
        self.lose_response = False
    def call(self, path, body=None):
        batch = path.split("/")[2].split("?")[0]
        if body is not None:
            key = (batch, body["operation_idempotency_key"])
            self.posts.append(dict(body))
            operation = self.operations.setdefault(key, {"operation_id": "op-" + key[1],
                "operation_type": "REFRESH_OUTCOMES", "operation_idempotency_key": key[1],
                "expected_row_version": body["expected_row_version"],
                "request_payload_sha256": canonical_json_sha256(body),
                "status": "RUNNING", "lease_expired": False, "error_json": None, "result_ref": None})
            if self.lose_response:
                self.lose_response = False
                raise TimeoutError("response lost after registration")
            return {"operation": operation}
        if "/operations?" in path:
            if "operation_type=REFRESH_OUTCOMES_RUN" in path:
                return {"operations": []}
            # The target identity is deliberately not on the first page.
            if "cursor=" not in path:
                return {"operations": [], "next_cursor": "second"}
            return {"operations": [value for (key, _), value in self.operations.items() if key == batch]}
        return {"batch": {"row_version": 7}}


def test_continuation_paginates_and_dispatches_dependencies_without_report_delay():
    from backend.services.advisory_historical_range.outcome_continuation import OutcomeContinuation
    api, state = _API(), {}
    runner = OutcomeContinuation(manifest=_manifest(), state=state, api=api, save=lambda state: None, max_active=2)
    assert runner.advance() is False
    assert [body["operation_idempotency_key"] for body in api.posts] == ["first", "independent"]
    receipt = _ref(HistoricalRangeArtifactKind.SUMMARY, "e").model_copy(update={
        "artifact_kind": HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
        "relative_path": "outcome-refresh-receipts/" + "e" * 64 + ".json"})
    api.operations[("batch-1", "first")].update(status="COMPLETED", result_ref=receipt.model_dump(mode="json"))
    runner.advance()  # next tick; no 1800-second stage barrier or backend HEAD check
    assert api.posts[-1]["operation_idempotency_key"] == "dependent"
    assert state["stages"]["first"]["status"] == "COMPLETE"


def test_lost_post_response_uses_same_native_exact_retry_not_new_scope():
    from backend.services.advisory_historical_range.outcome_continuation import OutcomeContinuation
    api, state = _API(), {}
    api.lose_response = True
    manifest = _manifest()
    manifest["stages"] = manifest["stages"][:1]
    runner = OutcomeContinuation(manifest=manifest, state=state, api=api, save=lambda state: None)
    with pytest.raises(TimeoutError):
        runner.advance()
    saved = dict(state["stages"]["first"]["request"])
    runner.advance()
    assert len(api.operations) == 1 and api.posts == [saved, saved]
    assert state["stages"]["first"]["operation_id"] == "op-first"


@pytest.mark.parametrize("fault", ["cycle", "identity", "checkpoint", "lease", "missing_receipt", "diagnostic_receipt"])
def test_continuation_identity_and_native_failure_checks_remain_closed(fault):
    from backend.services.advisory_historical_range.outcome_continuation import OutcomeContinuation
    manifest, state, api = _manifest(), {}, _API()
    if fault == "cycle":
        manifest["stages"][0]["depends_on"] = ["dependent"]
    elif fault == "identity":
        manifest["stages"][0]["request"]["expected_execution_identity"] = None
    elif fault == "checkpoint":
        state["manifest_sha256"] = "0" * 64
    if fault in {"cycle", "identity", "checkpoint"}:
        with pytest.raises(ValueError):
            OutcomeContinuation(manifest=manifest, state=state, api=api, save=lambda state: None)
        return
    runner = OutcomeContinuation(manifest=manifest, state=state, api=api, save=lambda state: None)
    runner.advance()
    operation = api.operations[("batch-1", "first")]
    if fault == "lease":
        operation["lease_expired"] = True
    else:
        operation["status"] = "COMPLETED"
        if fault == "diagnostic_receipt":
            operation["result_ref"] = _ref(HistoricalRangeArtifactKind.REQUEST, "e").model_dump(mode="json")
    with pytest.raises((ValueError, RuntimeError)):
        runner.advance()


def test_missing_optional_industry_does_not_block_returns_or_hide_coverage(tmp_path):
    from backend.services.advisory_historical_range.outcome_file_source import FileHistoricalRangeOutcomeSourceProvider, freeze_outcome_file_binding
    from backend.tests.advisory_historical_range.test_outcome_file_batch import _dataset
    from backend.tests.advisory_historical_range.test_r4_summary_service import _row, _policy
    from backend.services.advisory_historical_range.summary_service import HistoricalRangeSummaryService
    source = FileHistoricalRangeOutcomeSourceProvider(binding=freeze_outcome_file_binding(_dataset(tmp_path)),
        start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 21))
    assert source.load_optional_industry_at_t(symbol="999999.SZ", decision_trade_date=date(2026, 1, 19))[0] is None
    with pytest.raises(Exception, match="missing"):
        source.load_industry_at_t(symbol="999999.SZ", decision_trade_date=date(2026, 1, 19))
    row = _row("a", signal="signal-1", symbol="000001.SZ", day=date(2026, 7, 3),
               list_id="list-1", value="0.1", industry=None)
    artifact = HistoricalRangeSummaryService().calculate(range_run_id="run-1", rows=[row], policy=_policy(), producer_code_hash="a" * 64)
    assert any(item.get("reason_code") == "PIT_INDUSTRY_CONTEXT_UNAVAILABLE" for item in artifact.unavailable_metrics)


@pytest.mark.parametrize("fault", ["scope", "operation_id", "unknown_stage", "child_lease"])
def test_continuation_rejects_foreign_scope_and_expired_child(fault):
    from backend.services.advisory_historical_range.outcome_continuation import OutcomeContinuation
    from backend.services.advisory_historical_range.models import derive_prefixed_id
    api, state, manifest = _API(), {}, _manifest()
    manifest["stages"] = manifest["stages"][:1]
    runner = OutcomeContinuation(manifest=manifest, state=state, api=api, save=lambda state: None)
    runner.advance()
    if fault == "scope":
        record = state["stages"]["first"]
        record["request"]["horizons"] = [2]
        record["api_request_sha256"] = canonical_json_sha256(record["request"])
    elif fault == "operation_id":
        api.operations[("batch-1", "first")]["operation_id"] = "foreign"
    elif fault == "unknown_stage":
        state["stages"]["foreign"] = {}
        with pytest.raises(ValueError, match="unknown stage"):
            OutcomeContinuation(manifest=manifest, state=state, api=api, save=lambda state: None)
        return
    else:
        original = api.call
        def call(path, body=None):
            if "operation_type=REFRESH_OUTCOMES_RUN" in path:
                return {"operations": [{"operation_idempotency_key": derive_prefixed_id("ahroutkey", {
                    "parent_key": "first", "range_run_id": "run-1", "policy_bundle_hash": "c" * 64}),
                    "status": "RUNNING", "lease_expired": True}]}
            return original(path, body)
        api.call = call
    with pytest.raises((ValueError, RuntimeError)):
        runner.advance()


def test_execution_pins_detect_changed_producer_policy_and_files():
    from backend.services.advisory_historical_range.api_models import HistoricalRangeRefreshOutcomesRequest
    from backend.services.advisory_historical_range.runtime_factories import HistoricalRangeR5OutcomeRequestFactory
    item = _request_contract()
    expected = {"producer_code_hash": item.producer_code_hash, "policy_bundle_hash": item.policy_bundle_hash,
                "file_binding_hash": "a" * 64}
    item = item.model_copy(update={"file_source_binding": {"binding_hash": "a" * 64}})
    api = HistoricalRangeRefreshOutcomesRequest.model_validate(_manifest()["stages"][0]["request"])
    api = api.model_copy(update={"expected_execution_identity": expected})
    HistoricalRangeR5OutcomeRequestFactory._check_execution_identity(api, (item,))
    for key in expected:
        changed = api.model_copy(update={"expected_execution_identity": {**expected, key: "f" * 64}})
        with pytest.raises(ValueError, match="identity differs"):
            HistoricalRangeR5OutcomeRequestFactory._check_execution_identity(changed, (item,))


def test_summary_only_without_a_real_summary_cannot_report_success(tmp_path):
    from backend.services.advisory_historical_range.models import HistoricalRangeOutcomeRefreshRequestV1
    request = HistoricalRangeOutcomeRefreshRequestV1.model_validate({
        **_request_contract().model_dump(mode="json", exclude={"request_hash"}), "summary_mode": "ONLY"})
    class Summary:
        def refresh(self, **kwargs):
            return None
    service = HistoricalRangeOutcomeApplicationService(repository=_Repository(),
        artifact_store=HistoricalRangeOutcomeReadCache(root=tmp_path),
        planner=_Planner(_work_item()), evaluator=_Evaluator(), summary_coordinator=Summary())
    receipt, _ = service.refresh_until_stable_boundary(request=request,
        resolved_request_hash="9" * 64, worker_id="summary-test")
    assert receipt.status != "COMPLETED" and not receipt.summary_refs


def test_readonly_cache_is_bounded_and_ref_validation_survives_eviction(tmp_path):
    store = HistoricalRangeOutcomeReadCache(root=tmp_path, max_entries=1)
    first = _fact(store).outcome_artifact_ref
    cached = store.load_readonly(first)
    other = store.publish_payload(artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="test_v1", payload_schema_version="test_v1",
        resolved_request_hash="a" * 64, payload={"value": "other"}).ref
    store.load_readonly(other)
    assert len(store._cache) == 1 and store.load_readonly(first) is not cached
    forged = first.model_copy(update={"file_sha256": "f" * 64})
    with pytest.raises(HistoricalRangeContractError):
        store.load_readonly(forged)


def test_summary_context_does_not_load_or_consume_financial_price_window(tmp_path, monkeypatch):
    from backend.services.advisory_historical_range.outcome_file_source import FileHistoricalRangeOutcomeSourceProvider, freeze_outcome_file_binding
    from backend.tests.advisory_historical_range.test_outcome_file_batch import _dataset
    binding = freeze_outcome_file_binding(_dataset(tmp_path))
    monkeypatch.setattr(FileHistoricalRangeOutcomeSourceProvider, "_load_batch",
        lambda self: pytest.fail("summary-only unnecessarily read OHLC"))
    source = FileHistoricalRangeOutcomeSourceProvider(binding=binding,
        start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 21), context_only=True)
    assert source.load_optional_industry_at_t(symbol="999999.SZ", decision_trade_date=date(2026, 1, 19))[0] is None
    assert source.batch_read_count == 0
    with pytest.raises(Exception, match="cannot execute financial"):
        source.begin_operation("a" * 64)


def test_fact_equality_does_not_depend_on_private_cache_access(tmp_path):
    fact = _fact(HistoricalRangeOutcomeReadCache(root=tmp_path))
    same = HistoricalRangeOutcomeFactV1.model_validate(fact.model_dump(mode="json"))
    assert fact == same
    _ = fact.canonical_outcome_json
    assert fact == same
    assert fact != same.model_copy(update={"producer_code_hash": "f" * 64})
