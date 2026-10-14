from dataclasses import replace
from copy import deepcopy
from datetime import timedelta

import pytest

from backend.services.advisory_historical_range.selection_signal_kernel import (
    SelectionSignalBatchError, SelectionSignalDayV1, SelectionSignalRequestV1,
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


def test_explicit_cutoff_also_bounds_native_risk_and_tradability_reads(tmp_path, monkeypatch):
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
    assert reads == [cutoff] * 3 and result.clocks.decision_date == TRADE_DATE
