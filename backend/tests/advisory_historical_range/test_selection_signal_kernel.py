from dataclasses import replace
from copy import deepcopy
from datetime import timedelta

import pytest

from backend.services.advisory_historical_range.selection_signal_kernel import (
    SelectionSignalBatchError, SelectionSignalDayV1, SelectionSignalKernel, SelectionSignalRequestV1,
)
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName, StageReceiptStatus, build_stage_receipt, canonical_evidence_json_sha256,
)
from backend.services.selection_center.risk_policy import RiskDecision, StockRiskPolicyService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactRepository
from backend.services.strategy_package.selection_computation import (
    StrategyPackageSelectionComputation, StrategyPackageSelectionReadOnlyProvidersV1,
)
from backend.tests.advisory_historical_range.test_r2b_candidate_producer import _fixture, TRADE_DATE


def _request(program, *, consumer="ADVISORY", top_k=2, days=None):
    return SelectionSignalRequestV1(
        consumer_scope=consumer, run_id="independent-run", source_identity="frozen-revision",
        date_plan=days or (SelectionSignalDayV1(TRADE_DATE, TRADE_DATE, TRADE_DATE + timedelta(days=1)),),
        ordered_package_ids=(program.package_id,), manifest_hashes={program.package_id: program.manifest_sha256},
        runtime_configs={program.package_id: program.runtime_config}, package_top_k={program.package_id: top_k},
    )


def test_native_topk_policy_and_two_consumer_independent_results(tmp_path):
    producer, _, _, program = _fixture(tmp_path)
    prepared = producer._signal_preparation.result
    calls = []
    def prepare(**kwargs):
        calls.append(kwargs)
        return prepared
    kernel = producer._signal_kernel
    first = kernel.run(request=_request(program), prepare_day=prepare)[0]
    other = kernel.run(request=_request(program, consumer="LOCALSIM"), prepare_day=prepare)[0]
    assert len(calls) == 2 and [c["consumer_scope"] for c in calls] == ["ADVISORY", "LOCALSIM"]
    assert first.raw_scores == other.raw_scores and len(first.raw_scores[program.package_id]) == 3
    assert first.computation.package_results == other.computation.package_results
    assert len(first.computation.aggregate_results) == 2
    assert first.semantic_identity == other.semantic_identity and first.storage_identity != other.storage_identity
    first.raw_scores[program.package_id][0]["score"] = -99
    assert other.raw_scores[program.package_id][0]["score"] == .3
    small = kernel.run(request=_request(program, top_k=1), prepare_day=prepare)[0]
    assert len(small.computation.aggregate_results) == 1 and small.semantic_identity != other.semantic_identity


def test_partial_batch_preserves_native_error_and_original_date(tmp_path):
    producer, _, _, program = _fixture(tmp_path)
    first_day = SelectionSignalDayV1(TRADE_DATE, TRADE_DATE, TRADE_DATE)
    second_day = SelectionSignalDayV1(TRADE_DATE + timedelta(days=1), TRADE_DATE + timedelta(days=1), TRADE_DATE + timedelta(days=1))
    failure = RuntimeError("missing explicit frozen HMM provider")
    def prepare(**kwargs):
        if kwargs["clocks"] == second_day:
            raise failure
        return producer._signal_preparation.result
    with pytest.raises(SelectionSignalBatchError) as caught:
        producer._signal_kernel.run(request=_request(program, days=(first_day, second_day)), prepare_day=prepare)
    assert caught.value.cause is failure and caught.value.clocks == second_day
    assert caught.value.failed_stage == "raw_preparation" and len(caught.value.completed) == 1
    # A foreign prepared day is an error, never an empty/no-candidate success.
    with pytest.raises(SelectionSignalBatchError) as mismatch:
        producer._signal_kernel.run(request=_request(program, days=(second_day,)), prepare_day=lambda **kwargs: producer._signal_preparation.result)
    assert isinstance(mismatch.value.cause, ValueError) and "identity mismatch" in str(mismatch.value.cause)


def test_clock_and_package_contracts_not_silently_normalized(tmp_path):
    _, _, _, program = _fixture(tmp_path)
    request = _request(program)
    with pytest.raises(ValueError, match="increasing"):
        replace(request, date_plan=request.date_plan * 2)
    with pytest.raises(ValueError, match="TopK"):
        replace(request, package_top_k={program.package_id: True})
    with pytest.raises(ValueError, match="C/D/T"):
        SelectionSignalDayV1(TRADE_DATE, TRADE_DATE + timedelta(days=1), TRADE_DATE)
    with pytest.raises(ValueError, match="exact requested"):
        replace(request, manifest_hashes={"another-new-package": "a" * 64})


def test_explicit_cutoff_bounds_risk_but_preserves_D_selection_receipt(tmp_path, monkeypatch):
    producer, _, _, program = _fixture(tmp_path)
    prepared = deepcopy(producer._signal_preparation.result)
    cutoff = TRADE_DATE - timedelta(days=1)
    prepared.raw.artifact.metadata["artifact_input_context"]["effective_trade_date"] = cutoff.isoformat()
    kernel = producer._signal_kernel
    reads = []
    for provider, method_name in ((kernel._providers.risk_policy, "evaluate"),
                                  (kernel._providers.risk_policy, "apply_to_candidates_with_receipt"),
                                  (kernel._providers.tradability, "select_top_k_with_receipt")):
        original = getattr(provider, method_name)
        def record(_method=original, **kwargs):
            reads.append(kwargs["trade_date"])
            return _method(**kwargs)
        monkeypatch.setattr(provider, method_name, record)
    result = kernel.run(request=_request(program, days=(SelectionSignalDayV1(TRADE_DATE, cutoff, TRADE_DATE),)),
                        prepare_day=lambda **kwargs: prepared)[0]
    assert reads == [cutoff, cutoff, TRADE_DATE] and result.clocks.decision_date == TRADE_DATE


def _native_case(tmp_path, *, cutoff=None, suspended=None, exclude=True, top_k=2, rows=None, members=None):
    """Frozen inputs plus the actual risk/filter/compute/kernel; no policy doubles."""
    producer, _, _, program = _fixture(tmp_path)
    original = producer._signal_preparation.result
    cutoff = cutoff or TRADE_DATE - timedelta(days=1)
    config = deepcopy(program.runtime_config)
    config["runtime_profile"]["tradability"]["exclude_suspended"] = exclude
    request = replace(_request(program, top_k=top_k), runtime_configs={program.package_id: config},
                      date_plan=(SelectionSignalDayV1(TRADE_DATE, cutoff, TRADE_DATE),))
    metadata = deepcopy(original.raw.artifact.metadata)
    metadata["artifact_input_context"]["effective_trade_date"] = cutoff.isoformat()
    if members is not None:
        metadata["universe_members"] = members
    rows = deepcopy(rows if rows is not None else original.raw.artifact.scores_json)
    artifact = original.raw.artifact.model_copy(update={
        "scores_json": rows, "score_count": len(rows), "metadata": metadata,
        "top_score_symbol": rows[0]["symbol"] if rows else None,
        "universe_count": len(members) if members is not None else original.raw.artifact.universe_count,
        "artifact_input_context_hash": canonical_evidence_json_sha256(metadata["artifact_input_context"]),
        "artifact_payload_sha256": None,
    })
    artifact = StrategyPackageSelectionArtifactRepository._with_digest(artifact)
    candidates = tuple(SelectionCandidate.model_validate(row) for row in rows)
    signal = replace(original.prepared_signal, alpha_raw_candidates=candidates, hmm_adjusted_candidates=candidates,
                     valid_no_candidate=not candidates, no_candidate_reason="VALID_NO_CANDIDATE" if not candidates else None,
                     hmm_receipt=original.prepared_signal.hmm_receipt.model_copy(update={"input_count": len(rows)}),
                     artifact_header=replace(original.prepared_signal.artifact_header,
                                             artifact_sha256=artifact.artifact_sha256,
                                             artifact_payload_sha256=artifact.artifact_payload_sha256,
                                             artifact_input_context_hash=artifact.artifact_input_context_hash),
                     input_context_hash=artifact.artifact_input_context_hash)
    prepared = replace(original, raw=replace(original.raw, artifact=artifact, semantic_header=artifact.canonical_v2_header()),
                       prepared_signal=signal)
    reads = []

    class Suspensions:
        def get_suspended_symbols(self, symbols, trade_date):
            reads.append(("suspension", trade_date))
            return {s: {"source": "market.suspend_d", "suspend_type": "S", "suspend_timing": None}
                    for s in symbols if s in (suspended or {}).get(trade_date, set())}

    kernel = SelectionSignalKernel(computation=StrategyPackageSelectionComputation(),
                                  providers=StrategyPackageSelectionReadOnlyProvidersV1(
                                      StockRiskPolicyService(), TradabilityFilter(suspend_provider=Suspensions())))
    return kernel, request, prepared, reads


@pytest.mark.parametrize("c_suspended,d_suspended,expected", [(True, False, "000001.SZ"), (False, True, "000002.SZ")])
def test_kernel_run_uses_D_suspension_not_C(tmp_path, c_suspended, d_suspended, expected):
    cutoff = TRADE_DATE - timedelta(days=1)
    suspension_days = {cutoff: {"000001.SZ"} if c_suspended else set(),
                       TRADE_DATE: {"000001.SZ"} if d_suspended else set()}
    kernel, request, prepared, reads = _native_case(tmp_path, top_k=1, suspended=suspension_days)
    result = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
    assert result.computation.aggregate_results[0].symbol == expected
    assert reads == [("suspension", TRADE_DATE)]
    assert result.raw_scores[request.ordered_package_ids[0]] == tuple(prepared.raw.artifact.scores_json)
    exclusions = result.computation.excluded_results[request.ordered_package_ids[0]]
    assert all(row.context["trade_date"] == TRADE_DATE.isoformat()
               for row in exclusions if row.reason == "suspended_by_suspend_d")


@pytest.mark.parametrize("same_day,exclude", [(True, True), (False, False)])
def test_same_day_or_disabled_filter_keeps_native_policy(tmp_path, same_day, exclude):
    cutoff = TRADE_DATE if same_day else TRADE_DATE - timedelta(days=1)
    kernel, request, prepared, reads = _native_case(
        tmp_path, cutoff=cutoff, exclude=exclude, suspended={TRADE_DATE: {"000001.SZ"}})
    result = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
    expected = ["000002.SZ", "000003.SZ"] if exclude else ["000001.SZ", "000002.SZ"]
    assert [r.symbol for r in result.computation.aggregate_results] == expected
    assert reads == ([("suspension", TRADE_DATE)] if exclude else [])
    assert result.provider_clock_evidence["provider_calls"][-1]["lookup_dates"]["suspension"] == (
        TRADE_DATE.isoformat() if exclude else None)


@pytest.mark.parametrize("missing_rank", [1, 2])
def test_kernel_run_normal_or_refill_true_missing_price_remains_error(tmp_path, missing_rank):
    rows = [{"symbol": f"00000{i}.SZ", "rank": i, "score": .4-i*.1,
             "reference_price": None if i == missing_rank else 10.0,
             "component_scores": {"reference_price_required": True}}
            for i in (1, 2, 3)]
    suspended = {TRADE_DATE: {"000001.SZ"}} if missing_rank == 2 else {}
    kernel, request, prepared, _ = _native_case(tmp_path, rows=rows, top_k=1, suspended=suspended)
    with pytest.raises(SelectionSignalBatchError) as caught:
        kernel.run(request=request, prepare_day=lambda **_: prepared)
    assert caught.value.cause.context["reason_code"] == "SELECTION_EFFECTIVE_REFERENCE_PRICE_MISSING"
    assert caught.value.cause.context["missing_price_examples"] == [f"00000{missing_rank}.SZ"]
    assert prepared.raw.artifact.scores_json == rows


def test_kernel_risk_reads_C_and_native_industry_reads_C_not_D(tmp_path):
    from backend.services.selection_center.industry_provider import IndustryInfo

    kernel, request, prepared, reads = _native_case(tmp_path)
    cutoff = request.date_plan[0].data_cutoff
    config = deepcopy(request.runtime_configs)
    config[prepared.raw.artifact.package_id]["runtime_profile"].update(
        risk_policy={"enabled": True, "providers": ["st_pit"]}, industry_blacklist=["BLOCK"])
    request = replace(request, runtime_configs=config)

    class Risk:
        def evaluate(self, *, symbols, trade_date, **_):
            reads.append(("risk", trade_date))
            assert trade_date == cutoff
            return {s: RiskDecision(symbol=s) for s in symbols}

    class Industry:
        def get_industries(self, symbols, trade_date):
            reads.append(("industry", trade_date))
            assert trade_date == cutoff  # D's poisoned classification is never read.
            return {s: IndustryInfo(s, l1_name="BLOCK" if s == "000001.SZ" else "OK") for s in symbols}

    kernel._providers = replace(kernel._providers,
                                risk_policy=StockRiskPolicyService(providers={"st_pit": Risk()}),
                                tradability=TradabilityFilter(suspend_provider=kernel._providers.tradability.suspend_provider,
                                                             industry_provider=Industry()))
    result = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
    assert reads == [("risk", cutoff), ("suspension", TRADE_DATE), ("industry", cutoff)]
    assert [r.symbol for r in result.computation.aggregate_results] == ["000002.SZ", "000003.SZ"]
    trace = result.computation.stage_trace_by_package[request.ordered_package_ids[0]]
    assert trace.universe_metadata["provider_observation_dates"] == {
        "suspension": TRADE_DATE.isoformat(), "industry": cutoff.isoformat()}
    assert trace.risk_metadata["trade_date"] == cutoff.isoformat()


@pytest.mark.parametrize("future_field", [None, "input_data_max_dates", "training_information_cutoff", "available_at",
                                        "as_of_trade_date", "effective_trade_date"])
def test_kernel_HMM_preserves_D_label_but_checks_C_inputs(tmp_path, future_field):
    kernel, request, prepared, _ = _native_case(tmp_path)
    cutoff = request.date_plan[0].data_cutoff.isoformat()
    config = deepcopy(request.runtime_configs)
    config[request.ordered_package_ids[0]]["runtime_profile"]["hmm"].update(
        enabled=True, model_snapshot_id="clock-test-snapshot", signal_preset="clock-test-preset")
    request = replace(request, runtime_configs=config)
    metadata = {"enabled": True, "as_of_trade_date": TRADE_DATE.isoformat(),
                "effective_trade_date": TRADE_DATE.isoformat(), "coefficient_trade_date": TRADE_DATE.isoformat(),
                "input_data_max_dates": {"daily": cutoff}, "training_information_cutoff": cutoff,
                "available_at": cutoff}
    if future_field:
        future = (TRADE_DATE + timedelta(days=1)).isoformat()
        metadata[future_field] = {"daily": future} if future_field == "input_data_max_dates" else future
    signal = replace(prepared.prepared_signal, hmm_metadata=metadata,
                     hmm_receipt=build_stage_receipt(stage=CandidateStageName.HMM_ADJUSTED,
                                                    status=StageReceiptStatus.COMPLETE,
                                                    input_count=len(prepared.prepared_signal.alpha_raw_candidates),
                                                    candidates=list(prepared.prepared_signal.alpha_raw_candidates),
                                                    semantic_payload=metadata))
    prepared = replace(prepared, prepared_signal=signal)
    if future_field:
        with pytest.raises(SelectionSignalBatchError) as caught:
            kernel.run(request=request, prepare_day=lambda **_: prepared)
        assert caught.value.failed_stage == "raw_preparation"
        assert isinstance(caught.value.cause, ValueError) and "HMM" in str(caught.value.cause)
    else:
        result = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
        assert result.provider_clock_evidence["prepared"][request.ordered_package_ids[0]]["hmm"]["as_of_trade_date"] == TRADE_DATE.isoformat()


def test_full_population_and_contract_version_bind_identity_without_changing_scores(tmp_path, monkeypatch):
    import backend.services.advisory_historical_range.selection_signal_kernel as module

    rows = [{"symbol": f"{i:06d}.SZ", "rank": i+1, "score": float(4912-i)} for i in range(4912)]
    members = [r["symbol"] for r in rows] + [f"{i:06d}.SZ" for i in range(4912, 4918)]
    kernel, request, prepared, _ = _native_case(tmp_path, rows=rows, members=members, top_k=50)
    first = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
    p = request.ordered_package_ids[0]
    assert first.raw_scores[p] == tuple(rows) and len(first.unscored_members[p]) == 6
    assert first.membership_status[p] == "COMPLETE" and prepared.raw.artifact.universe_count == 4918
    assert [r.symbol for r in first.computation.aggregate_results] == members[:50]
    monkeypatch.setattr(module, "PROVIDER_CLOCK_CONTRACT", "different-clock-contract")
    other = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
    assert other.raw_scores == first.raw_scores
    assert other.semantic_identity != first.semantic_identity and other.storage_identity != first.storage_identity


@pytest.mark.parametrize("bad", ["price_cutoff", "quote_date", "prepared_cutoff", "manifest"])
def test_kernel_date_and_source_identity_isolation_fails_before_policy_reads(tmp_path, bad):
    kernel, request, prepared, reads = _native_case(tmp_path)
    cutoff = request.date_plan[0].data_cutoff
    if bad in {"price_cutoff", "quote_date"}:
        component = prepared.raw.artifact.scores_json[0]["component_scores"]
        component["reference_price_trade_date"] = cutoff.isoformat()
        if bad == "price_cutoff":
            component["reference_price_trade_date"] = TRADE_DATE.isoformat()
        else:
            component["reference_price_availability"] = {"as_of_date": cutoff.isoformat(), "quote_date": TRADE_DATE.isoformat()}
    elif bad == "prepared_cutoff":
        prepared.raw.artifact.metadata["artifact_input_context"]["effective_trade_date"] = TRADE_DATE.isoformat()
    else:
        request = replace(request, manifest_hashes={request.ordered_package_ids[0]: "f"*64})
    with pytest.raises(SelectionSignalBatchError) as caught:
        kernel.run(request=request, prepare_day=lambda **_: prepared)
    assert caught.value.failed_stage == "raw_preparation" and reads == []


def test_changed_D_suspension_membership_invalidates_filtered_cache_not_raw(tmp_path):
    suspended = {TRADE_DATE: {"000001.SZ"}}
    kernel, request, prepared, _ = _native_case(tmp_path, suspended=suspended, top_k=1)
    first = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
    suspended[TRADE_DATE] = {"000002.SZ"}
    second = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
    assert first.raw_scores == second.raw_scores
    assert first.computation.aggregate_results[0].symbol != second.computation.aggregate_results[0].symbol
    assert first.semantic_identity != second.semantic_identity and first.storage_identity != second.storage_identity


def test_future_target_suspension_never_changes_D_eligibility(tmp_path):
    future = TRADE_DATE+timedelta(days=1)
    kernel, request, prepared, reads = _native_case(tmp_path, suspended={future: {"000001.SZ"}})
    day = request.date_plan[0]
    request = replace(request, date_plan=(replace(day, target_trade_date=future),))
    result = kernel.run(request=request, prepare_day=lambda **_: prepared)[0]
    assert reads == [("suspension", TRADE_DATE)] and result.computation.aggregate_results[0].symbol == "000001.SZ"


def test_native_empty_alpha_does_not_claim_HMM_observation_or_require_generation(tmp_path):
    kernel, request, prepared, _ = _native_case(tmp_path, rows=[])
    config = deepcopy(request.runtime_configs)
    config[request.ordered_package_ids[0]]["runtime_profile"]["hmm"].update(
        enabled=True, model_snapshot_id="unused", signal_preset="unused")
    metadata = {"enabled": True, "generation_mode": "NO_ALPHA_CANDIDATES"}
    signal = replace(prepared.prepared_signal, hmm_metadata=metadata,
                     hmm_receipt=prepared.prepared_signal.hmm_receipt.model_copy(update={"semantic_payload": metadata}))
    result = kernel.run(request=replace(request, runtime_configs=config),
                        prepare_day=lambda **_: replace(prepared, prepared_signal=signal))[0]
    assert result.computation.valid_no_candidate and not result.computation.aggregate_results
    assert result.provider_clock_evidence["prepared"][request.ordered_package_ids[0]]["hmm"] == {"status": "NOT_APPLICABLE"}


def test_localsim_checkpoint_namespace_binds_shared_kernel_source(tmp_path, monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace
    from backend.services.selection_center.models import SelectionMode
    from backend.services.simulation_runtime.localsim_selection_signal import LocalSimSelectionSignalService
    import backend.services.advisory_historical_range.selection_signal_kernel as module

    # Exercise the actual consumer's pre-lookup identity, stop at the read-only
    # recovery seam; never enter native publication, account or execution.
    service = object.__new__(LocalSimSelectionSignalService)
    service.temporary_root = tmp_path
    service.package_repository = SimpleNamespace(get=lambda _: SimpleNamespace(manifest_sha256="a"*64))
    identities = []
    marker = object()
    def recovered(root, identity, **kwargs):
        identities.append(identity)
        return marker
    service._recover = recovered
    kwargs = dict(package_ids=["arbitrary-package"], trade_date=TRADE_DATE,
                  data_source="DB_HISTORICAL", mode=SelectionMode.SINGLE_PACKAGE, runtime_config={})
    assert service.run_selection(**kwargs) is marker
    original = Path.read_bytes
    kernel_path = Path(module.__file__).resolve()
    monkeypatch.setattr(Path, "read_bytes", lambda path: b"legacy-C-filter-source" if path.resolve() == kernel_path else original(path))
    assert service.run_selection(**kwargs) is marker
    assert len(identities) == 2 and identities[0] != identities[1]
