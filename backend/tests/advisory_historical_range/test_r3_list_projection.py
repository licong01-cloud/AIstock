from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.list_transition import HistoricalRangeListTransitionAdapter
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeCandidateFactV1,
    HistoricalRangeDecisionMarkSetV1,
    HistoricalRangeDecisionMarkV2,
    HistoricalRangeListAction,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRevisionRefV1,
    derive_prefixed_id,
)
from backend.services.advisory_historical_range.semantics import canonical_list_semantics_v2
from backend.tests.advisory_historical_range.conftest import artifact_ref, digest, frozen_program, research_spec


_DAY = date(2026, 6, 3)
_CUTOFF = datetime(2026, 6, 3, 15, tzinfo=UTC)


def _program():
    base = frozen_program(research_spec(target_count=2))
    semantics = canonical_list_semantics_v2()
    review_policy = {
        "rank_enter_threshold": 2,
        "rank_exit_threshold": 3,
        "rank_exit_confirm_days": 2,
        "daily_replacement_budget": 1,
        "stop_loss_bps": 800,
        "take_profit_bps": 1800,
        "trailing_stop_bps": 600,
        "time_stop_days": 0,
        "take_profit_mode": "trailing",
    }
    payload = base.model_dump(mode="json")
    payload.update(
        {
            "review_policy": review_policy,
            "review_policy_hash": canonical_json_sha256(review_policy),
            "list_semantics_version": semantics.schema_version,
            "list_semantics_hash": semantics.semantics_hash,
            "frozen_program_hash": None,
        }
    )
    return type(base).model_validate(payload)


def _candidate_payload(*, outcome: str = "CANDIDATES_AVAILABLE") -> HistoricalRangeCandidateArtifactPayloadV2:
    program = _program()
    header = {
        "runtime_profile_hash": digest("runtime"),
        "selection_semantics_hash": program.selection_semantics_hash,
        "code_release_hash": program.code_release_hash,
        "calendar_identity_hash": digest("calendar"),
        "universe_identity_hash": digest("universe"),
    }
    candidate = HistoricalRangeCandidateFactV1(
        candidate_id=derive_prefixed_id("ahc", {"day_run_id": "day_1", "symbol": "000001.SZ"}),
        day_run_id="day_1",
        symbol="000001.SZ",
        membership_status="INCLUDED",
        alpha_raw_rank=1,
        alpha_raw_score=Decimal("0.9"),
        hmm_adjusted_rank=1,
        hmm_adjusted_score=Decimal("0.9"),
        risk_policy_adjusted_rank=1,
        risk_policy_adjusted_score=Decimal("0.9"),
        selection_effective_rank=1,
        selection_effective_score=Decimal("0.9"),
        component_lineage_json={"component": "leg_a"},
        component_lineage_hash=digest({"component": "leg_a"}),
    )
    stages = {
        name: {"stage": name, "status": "COMPLETE", "input_count": 1, "output_count": 1, "excluded_count": 0}
        for name in ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
    }
    return HistoricalRangeCandidateArtifactPayloadV2(
        range_run_id="range_1",
        day_run_id="day_1",
        research_program_id=program.research_program_id,
        decision_trade_date=_DAY,
        candidate_input_hash=digest("candidate-input"),
        package_id=program.package_id,
        package_version=program.package_version,
        manifest_sha256=program.manifest_sha256,
        alpha_mode=program.alpha_mode,
        runtime_profile_hash=header["runtime_profile_hash"],
        selection_semantics_hash=header["selection_semantics_hash"],
        code_release_hash=header["code_release_hash"],
        calendar_identity_hash=header["calendar_identity_hash"],
        universe_identity_hash=header["universe_identity_hash"],
        universe_count=1,
        raw_signal_identity_hash=canonical_json_sha256(header),
        raw_signal_semantic_header=header,
        raw_inference_receipt={"status": "COMPLETE", "score_count": 1},
        source_read_receipt_hashes=(digest("source-read"),),
        stage_trace=stages,
        candidate_outcome=outcome,
        no_candidate_reason_codes=(),
        source_revision_refs=(HistoricalRangeSourceRevisionRefV1(revision_id="candidate-rev", revision_hash=digest("candidate-rev")),),
        candidates=(candidate,),
    )


def _mark_set() -> HistoricalRangeDecisionMarkSetV1:
    source_ref = HistoricalRangeSourceRevisionRefV1(revision_id="mark-rev", revision_hash=digest("mark-rev"))
    mark = HistoricalRangeDecisionMarkV2(
        symbol="000001.SZ",
        decision_trade_date=_DAY,
        availability="AVAILABLE",
        raw_reference_yuan=Decimal("10"),
        adjustment_factor_as_of_t=Decimal("1.2"),
        normalized_reference_mark=Decimal("12"),
        mark_quality="T_CLOSE",
        tradability_status="TRADABLE",
        source_revision_refs=(source_ref,),
        source_evidence_hash=digest("mark-evidence"),
        fact_effective_at=_CUTOFF,
        decision_cutoff=_CUTOFF,
        source_observed_at=datetime(2026, 7, 22, tzinfo=UTC),
        revision_admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
    )
    return HistoricalRangeDecisionMarkSetV1(
        range_run_id="range_1",
        day_run_id="day_1",
        decision_trade_date=_DAY,
        subject_set_hash=canonical_json_sha256([mark.symbol]),
        mark_policy_version="pit_decision_then_mature_mark_v1",
        mark_policy_hash=digest("mark-policy"),
        source_revision_set_hash=canonical_json_sha256([source_ref.model_dump(mode="json")]),
        source_revision_refs=(source_ref,),
        upstream_request_ref=artifact_ref(HistoricalRangeArtifactKind.REQUEST, "request"),
        marks=(mark,),
    )


def test_r3_projection_uses_t_cutoff_mark_for_deterministic_enter_and_guidance() -> None:
    program = _program()
    result = HistoricalRangeListTransitionAdapter().build_projection(
        program=program,
        candidate_payload=_candidate_payload(),
        decision_mark_set=_mark_set(),
        decision_mark_set_ref=artifact_ref(HistoricalRangeArtifactKind.DECISION_MARK_SET, "mark-set"),
        previous_episodes=(),
        entry_sequences_by_symbol={},
        previous_list_version_id=None,
        previous_list_hash=None,
        previous_day_receipt_hash=None,
        day_input_hash=digest("day-input"),
        next_trade_date=date(2026, 6, 4),
        is_range_end=False,
        decision_cutoff=_CUTOFF,
        semantics=canonical_list_semantics_v2(),
    )

    assert result.blocking_diagnostics == ()
    assert result.list_version is not None
    assert [(item.action, item.symbol) for item in result.items] == [(HistoricalRangeListAction.ENTER, "000001.SZ")]
    assert result.episodes[0].mark_json["current_normalized_mark"] == "12"
    assert result.items[0].intended_execution_trade_date == date(2026, 6, 4)
    assert result.items[0].execution_status == "NOT_DUE"


def test_r3_projection_blocks_when_an_active_symbol_has_no_closed_mark() -> None:
    # The adapter must not turn unresolved market evidence into a zero-price HOLD/EXIT.
    program = _program()
    first = HistoricalRangeListTransitionAdapter().build_projection(
        program=program,
        candidate_payload=_candidate_payload(),
        decision_mark_set=_mark_set(),
        decision_mark_set_ref=artifact_ref(HistoricalRangeArtifactKind.DECISION_MARK_SET, "mark-set"),
        previous_episodes=(),
        entry_sequences_by_symbol={},
        previous_list_version_id=None,
        previous_list_hash=None,
        previous_day_receipt_hash=None,
        day_input_hash=digest("day-input"),
        next_trade_date=date(2026, 6, 4),
        is_range_end=False,
        decision_cutoff=_CUTOFF,
        semantics=canonical_list_semantics_v2(),
    )
    assert first.episodes

    original_mark_set = _mark_set()
    unavailable_payload = original_mark_set.marks[0].model_dump(mode="json")
    unavailable_payload.update(
        {
            "availability": "DATA_UNAVAILABLE",
            "raw_reference_yuan": None,
            "adjustment_factor_as_of_t": None,
            "normalized_reference_mark": None,
            "mark_quality": "UNAVAILABLE",
            "tradability_status": "QUOTE_OR_ADJUSTMENT_UNAVAILABLE",
        }
    )
    unavailable_mark = HistoricalRangeDecisionMarkV2.model_validate(unavailable_payload)
    missing_mark_payload = original_mark_set.model_dump(mode="json")
    missing_mark_payload.update({"marks": (unavailable_mark.model_dump(mode="json"),), "mark_set_hash": None})
    missing_mark_set = HistoricalRangeDecisionMarkSetV1.model_validate(missing_mark_payload)
    blocked = HistoricalRangeListTransitionAdapter().build_projection(
        program=program,
        candidate_payload=_candidate_payload(),
        decision_mark_set=missing_mark_set,
        decision_mark_set_ref=artifact_ref(HistoricalRangeArtifactKind.DECISION_MARK_SET, "missing-mark-set"),
        previous_episodes=first.episodes,
        entry_sequences_by_symbol={"000001.SZ": 1},
        previous_list_version_id=first.list_version.list_version_id if first.list_version else None,
        previous_list_hash=first.list_version.list_content_hash if first.list_version else None,
        previous_day_receipt_hash=digest("previous-receipt"),
        day_input_hash=digest("day-input-2"),
        next_trade_date=date(2026, 6, 5),
        is_range_end=False,
        decision_cutoff=datetime(2026, 6, 4, 15, tzinfo=UTC),
        semantics=canonical_list_semantics_v2(),
    )

    assert blocked.list_version is None
    assert blocked.blocking_diagnostics == ("ADVISORY_HR_WAITING_INPUT",)

    absent_payload = original_mark_set.model_dump(mode="json")
    absent_payload.update(
        {
            "subject_set_hash": canonical_json_sha256([]),
            "marks": (),
            "mark_set_hash": None,
        }
    )
    absent_mark_set = HistoricalRangeDecisionMarkSetV1.model_validate(absent_payload)
    absent_blocked = HistoricalRangeListTransitionAdapter().build_projection(
        program=program,
        candidate_payload=_candidate_payload(),
        decision_mark_set=absent_mark_set,
        decision_mark_set_ref=artifact_ref(HistoricalRangeArtifactKind.DECISION_MARK_SET, "absent-mark-set"),
        previous_episodes=first.episodes,
        entry_sequences_by_symbol={"000001.SZ": 1},
        previous_list_version_id=first.list_version.list_version_id if first.list_version else None,
        previous_list_hash=first.list_version.list_content_hash if first.list_version else None,
        previous_day_receipt_hash=digest("previous-receipt"),
        day_input_hash=digest("day-input-3"),
        next_trade_date=date(2026, 6, 5),
        is_range_end=False,
        decision_cutoff=datetime(2026, 6, 4, 15, tzinfo=UTC),
        semantics=canonical_list_semantics_v2(),
    )
    assert absent_blocked.list_version is None
    assert absent_blocked.blocking_diagnostics == ("ADVISORY_HR_WAITING_INPUT",)
