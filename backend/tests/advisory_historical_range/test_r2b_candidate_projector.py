from __future__ import annotations

from dataclasses import replace

import pytest

from backend.services.advisory_historical_range.candidate_projector import HistoricalRangeCandidateProjector
from backend.services.selection_center.models import SelectionCandidate
from backend.services.strategy_package.selection_computation import (
    SelectionMode,
    StrategyPackageSelectionComputation,
    StrategyPackageSelectionComputationRequestV1,
    parse_selection_runtime_profile_for_computation,
    selection_runtime_profile_sha256,
)
from backend.services.trading_core.errors import ArtifactGenerationFailedError
from backend.tests.advisory_historical_range.test_r2b_candidate_producer import TRADE_DATE, _fixture


def _projector_inputs(tmp_path):  # noqa: ANN001, ANN202
    producer, _request_payload, _verifier, program = _fixture(tmp_path)
    prepared = producer._signal_preparation.result
    profile = parse_selection_runtime_profile_for_computation(program.runtime_config)
    profile_hash = selection_runtime_profile_sha256(profile)
    computation = StrategyPackageSelectionComputation().compute(
        request=StrategyPackageSelectionComputationRequestV1(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            selection_mode=SelectionMode.SINGLE_PACKAGE,
            ordered_package_ids=(program.package_id,),
            package_runtime_profiles={program.package_id: profile},
            package_runtime_profile_hashes={program.package_id: profile_hash},
            package_top_k={program.package_id: 2},
            exhaustive_selection_evidence=True,
        ),
        prepared_signals={program.package_id: prepared.prepared_signal},
        providers=producer._providers,
    )
    return program, prepared, profile_hash, computation


def test_projector_preserves_four_stage_facts_and_exclusion_lineage(tmp_path) -> None:  # noqa: ANN001
    program, prepared, profile_hash, computation = _projector_inputs(tmp_path)

    facts, trace = HistoricalRangeCandidateProjector().project(
        frozen_program=program,
        day_run_id="ahrd_projector_test",
        prepared_signal=prepared.prepared_signal,
        raw_artifact=prepared.raw,
        computation=computation,
        runtime_profile_hash=profile_hash,
    )

    assert len(facts) == 3
    assert [item.membership_status for item in facts] == ["INCLUDED", "INCLUDED", "EXCLUDED"]
    assert facts[0].alpha_raw_rank == 1
    assert facts[0].hmm_adjusted_rank == 1
    assert facts[0].risk_policy_adjusted_rank == 1
    assert facts[0].selection_effective_rank == 1
    assert facts[2].component_lineage_json["stage_exclusions"][0]["reason"] == "outside_selection_top_k"
    assert trace["selection_effective"]["excluded_count"] == 1


def test_projector_rejects_symbol_introduced_after_alpha_raw(tmp_path) -> None:  # noqa: ANN001
    program, prepared, profile_hash, computation = _projector_inputs(tmp_path)
    trace = computation.stage_trace_by_package[program.package_id]
    introduced = SelectionCandidate(symbol="999999.SZ", score=0.01, rank=3)
    selection_receipt = trace.selection_effective.model_copy(
        update={
            "candidates": [*trace.selection_effective.candidates, introduced.model_dump(mode="json")],
            "output_count": trace.selection_effective.output_count + 1,
        }
    )
    invalid_trace = trace.model_copy(update={"selection_effective": selection_receipt})
    invalid = replace(
        computation,
        stage_trace_by_package={program.package_id: invalid_trace},
    )

    with pytest.raises(ArtifactGenerationFailedError, match="introduced symbols"):
        HistoricalRangeCandidateProjector().project(
            frozen_program=program,
            day_run_id="ahrd_projector_invalid",
            prepared_signal=prepared.prepared_signal,
            raw_artifact=prepared.raw,
            computation=invalid,
            runtime_profile_hash=profile_hash,
        )
