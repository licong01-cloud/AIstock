from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    ResearchResultClass,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    Tier1EvidenceState,
    Tier1InterventionSupportV1,
    Tier1MetricInferenceV1,
    Tier1Quadrant,
    build_learnability_receipt,
    build_oracle_receipt,
    build_quadrant_receipt,
)
from backend.services.advisory_model_first.tier1_oracle_pipeline import (
    Tier1LearnabilityResult,
    Tier1OracleResult,
    _benchmark_series,
    _build_one_date_outcomes,
    _eligible_symbols_by_date,
    _normalize_market_frame,
    _publish_n1_bundle,
    _read_n1_bundle,
    build_tier1_outcomes_and_oracle,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.tests.advisory_model_first.test_oracle_mini_contract import (
    HASH_A,
    HASH_B,
    _request,
)


def _fixture_frames():
    calendar = pd.bdate_range("2024-07-04", "2026-03-10")
    decisions = calendar[:59].append(pd.DatetimeIndex([pd.Timestamp("2026-02-02")]))
    symbols = [f"{index:06d}.SZ" for index in range(1, 61)]
    snapshot = freeze_pit_snapshot(
        [
            {
                "ts_code": symbol,
                "eligible_start": date(2024, 7, 4),
                "eligible_end": date(2026, 3, 10),
                "entry_reason": "252_sessions",
                "exit_reason": None,
            }
            for symbol in symbols
        ],
        universe_key="aistock_equity_pit_canonical_v2",
        rule_version="shsz_a_252td_st_delist_asof_v2",
        scope_start=date(2024, 7, 4),
        cutoff=date(2026, 3, 10),
        state_identity="ready-v1",
        source_fingerprint_sha256=HASH_A,
        parameter_hash=HASH_B,
    )
    ranking_rows = []
    for decision in decisions:
        for rank, symbol in enumerate(symbols[:50], start=1):
            ranking_rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "trade_date": decision,
                    "target_trade_date": calendar[calendar.get_loc(decision) + 1],
                    "instrument": symbol,
                    "selection_effective_rank": rank,
                }
            )
    market_rows = []
    for time_index, trade_date in enumerate(calendar):
        for symbol_index, symbol in enumerate(symbols, start=1):
            if symbol_index == 60:
                continue
            slope = symbol_index / 100_000.0
            price = 100.0 * np.exp(slope * time_index)
            market_rows.append(
                {
                    "datetime": trade_date,
                    "instrument": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "factor": 1.0,
                    "up_limit_price": price * 1.1,
                    "down_limit_price": price * 0.9,
                    "limit_up": 0.0,
                    "limit_down": 0.0,
                }
            )
    daily = pd.DataFrame(market_rows).set_index(["datetime", "instrument"])
    benchmark = pd.DataFrame(
        {
            "datetime": calendar,
            "instrument": "000300.SH",
            "open": 100.0,
            "close": 100.0,
        }
    ).set_index(["datetime", "instrument"])
    suspend = pd.DataFrame(
        {
            "trade_date": calendar,
            "instrument": symbols[0],
        }
    )
    return (
        pd.DataFrame(ranking_rows),
        daily,
        benchmark,
        suspend,
        snapshot,
        calendar,
        decisions,
        symbols,
    )


def test_oracle_preserves_suspension_and_missing_rows_without_future_leakage() -> None:
    rankings, daily, benchmark, suspend, snapshot, calendar, decisions, symbols = (
        _fixture_frames()
    )

    result = build_tier1_outcomes_and_oracle(
        rankings=rankings,
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        pit_snapshot=snapshot,
        trading_calendar=calendar,
        decision_dates=decisions,
        request=_request(),
        common_prediction_count_by_date={date_: 60 for date_ in decisions},
    )

    assert len(result.candidate_labels) == 60 * 50
    suspended = result.candidate_labels[
        result.candidate_labels["instrument"] == symbols[0]
    ]
    assert set(suspended["outcome_status"]) == {"NOT_ENTERED_SUSPENDED"}
    assert suspended["outcome_known"].all()
    assert (suspended["slot_return_bps"] == 0.0).all()
    assert result.universe_summary["minimum_known_outcome_fraction"] >= 0.95
    assert result.recall_summary["top20"]["mean_winner_recall"] == 0.0
    assert result.recall_summary["top50"]["mean_winner_recall"] == 0.0
    assert result.perfect_top5_lift.point_estimate_bps > 0.0
    assert result.oracle_daily["intervened"].all()


def test_rank_bucket_output_includes_the_derived_top41_to_top50_slice() -> None:
    rankings, daily, benchmark, suspend, snapshot, calendar, decisions, _ = _fixture_frames()
    result = build_tier1_outcomes_and_oracle(
        rankings=rankings,
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        pit_snapshot=snapshot,
        trading_calendar=calendar,
        decision_dates=decisions,
        request=_request(),
    )

    last_bucket = result.rank_bucket_summary[-1]
    assert (last_bucket["rank_start"], last_bucket["rank_end"]) == (41, 50)
    assert last_bucket["row_count"] == 60 * 10


def test_parent_and_outcome_target_date_mismatch_fails_closed() -> None:
    rankings, daily, benchmark, suspend, snapshot, calendar, decisions, _ = _fixture_frames()
    rankings.loc[
        rankings["decision_as_of_trade_date"] == decisions[0], "target_trade_date"
    ] = calendar[calendar.get_loc(decisions[0]) + 2]

    with pytest.raises(AdvisoryModelFirstError) as captured:
        build_tier1_outcomes_and_oracle(
            rankings=rankings,
            daily=daily,
            benchmark_daily=benchmark,
            suspend_rows=suspend,
            pit_snapshot=snapshot,
            trading_calendar=calendar,
            decision_dates=decisions,
            request=_request(),
        )

    assert captured.value.reason_code == "ADVISORY_N1_LABEL_CLOCK_INVALID"


def test_immutable_bundle_records_real_parquet_rows_and_exact_retry(tmp_path) -> None:
    request = _request(output_root=str(tmp_path))
    metric = Tier1MetricInferenceV1(
        point_estimate_bps=10.0,
        confidence_lower_bps=6.0,
        confidence_upper_bps=14.0,
        bootstrap_standard_error_bps=1.0,
        mde_bps=2.0,
        economic_threshold_bps=5.0,
        evidence_state=Tier1EvidenceState.HIGH,
        evaluated_day_count=60,
    )
    support = Tier1InterventionSupportV1(
        evaluated_day_count=60,
        intervention_day_count=60,
        intervention_fraction=1.0,
        intervention_days_by_regime={"UP_OR_FLAT": 60},
        minimum_day_count=60,
        minimum_fraction=0.25,
        minimum_days_per_observed_regime=20,
        support_sufficient=True,
        reason_codes=(),
    )
    oracle = Tier1OracleResult(
        candidate_labels=pd.DataFrame(
            {"instrument": ["000001.SZ"], "outcome_known": [True]}
        ),
        oracle_daily=pd.DataFrame(
            {
                "decision_as_of_trade_date": [pd.Timestamp("2024-07-04")],
                "baseline_instruments": [("000001.SZ",)],
            }
        ),
        recall_daily=pd.DataFrame({"status": ["AVAILABLE"]}),
        outcome_coverage=pd.DataFrame({"known_outcome_count": [1]}),
        recall_summary={"evaluated_day_count": 60},
        rank_bucket_summary=(),
        universe_summary={"decision_date_count": 60},
        perfect_top5_lift=metric,
        intervention_support=support,
        evidence_sufficient=True,
        evidence_reason_codes=(),
    )
    learnability = Tier1LearnabilityResult(
        oof_predictions=pd.DataFrame(
            {"instrument": ["000001.SZ"], "oof_prediction_count": [7]}
        ),
        daily=pd.DataFrame({"learnability_lift_bps": [10.0]}),
        lift=metric,
        intervention_support=support,
        evidence_sufficient=True,
        evidence_reason_codes=(),
    )
    oracle_receipt = build_oracle_receipt(
        request_sha256=request.request_sha256,
        decision_date_count=60,
        universe_summary=oracle.universe_summary,
        recall_summary=oracle.recall_summary,
        rank_bucket_summary=(),
        perfect_top5_lift=metric,
        intervention_support=support,
        evidence_sufficient=True,
        evidence_reason_codes=(),
        result_class=ResearchResultClass.CONTROL_READY,
        decision_use=DecisionUse.DIRECTION_GATE,
    )
    learnability_receipt = build_learnability_receipt(
        request_sha256=request.request_sha256,
        feature_schema_hash=request.feature_schema_hash,
        oof_row_count=1,
        oof_predictions_per_row=7,
        learnability_lift=metric,
        intervention_support=support,
        evidence_sufficient=True,
        evidence_reason_codes=(),
        result_class=ResearchResultClass.CONTROL_READY,
        decision_use=DecisionUse.DIRECTION_GATE,
    )
    quadrant_receipt = build_quadrant_receipt(
        request_sha256=request.request_sha256,
        oracle_receipt_sha256=oracle_receipt.receipt_sha256,
        learnability_receipt_sha256=learnability_receipt.receipt_sha256,
        point_quadrant=Tier1Quadrant.THEORETICAL_HIGH_LEARNABILITY_HIGH,
        typed_result=Tier1Quadrant.THEORETICAL_HIGH_LEARNABILITY_HIGH.value,
        direction_ready=True,
        reason_codes=(),
    )
    arguments = {
        "request": request,
        "environment": {"python": "test"},
        "source_receipt": {"sealed_holdout_accessed": False},
        "rankings": pd.DataFrame(
            {"instrument": ["000001.SZ"], "selection_effective_rank": [1]}
        ),
        "oracle": oracle,
        "learnability": learnability,
        "n1_cpcv_payload": {"schema_version": "test", "paths": []},
        "oracle_receipt": oracle_receipt,
        "learnability_receipt": learnability_receipt,
        "quadrant_receipt": quadrant_receipt,
        "resource_report": {"peak_rss_bytes": 1, "stages": []},
        "walk_forward_hmm_receipt": {"status": "test"},
    }

    first = _publish_n1_bundle(**arguments)
    second = _publish_n1_bundle(**arguments)
    loaded = _read_n1_bundle(first)

    assert first == second
    assert loaded["manifest"]["files"]["candidate_rankings_top50.parquet"][
        "row_count"
    ] == 1
    assert loaded["manifest"]["files"]["oracle_daily.parquet"]["row_count"] == 1


def test_h20_last_decision_exits_on_data_cutoff_not_one_session_later() -> None:
    calendar = pd.DatetimeIndex(
        pd.to_datetime(
            [
                "2026-01-26",
                "2026-01-27",
                "2026-01-28",
                "2026-01-29",
                "2026-01-30",
                "2026-02-02",
                "2026-02-03",
                "2026-02-04",
                "2026-02-05",
                "2026-02-06",
                "2026-02-09",
                "2026-02-10",
                "2026-02-11",
                "2026-02-12",
                "2026-02-13",
                "2026-02-24",
                "2026-02-25",
                "2026-02-26",
                "2026-02-27",
                "2026-03-02",
                "2026-03-03",
                "2026-03-04",
                "2026-03-05",
                "2026-03-06",
                "2026-03-09",
                "2026-03-10",
            ]
        )
    )
    symbols = [f"{index:06d}.SZ" for index in range(1, 6)]
    snapshot = freeze_pit_snapshot(
        [
            {
                "ts_code": symbol,
                "eligible_start": date(2024, 7, 4),
                "eligible_end": date(2026, 3, 10),
                "entry_reason": "252_sessions",
                "exit_reason": None,
            }
            for symbol in symbols
        ],
        universe_key="aistock_equity_pit_canonical_v2",
        rule_version="shsz_a_252td_st_delist_asof_v2",
        scope_start=date(2024, 7, 4),
        cutoff=date(2026, 3, 10),
        state_identity="ready-v1",
        source_fingerprint_sha256=HASH_A,
        parameter_hash=HASH_B,
    )
    rows = []
    for trade_date in calendar:
        for symbol in symbols:
            rows.append(
                {
                    "datetime": trade_date,
                    "instrument": symbol,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "factor": 1.0,
                    "up_limit_price": 110.0,
                    "down_limit_price": 90.0,
                    "limit_up": 0.0,
                    "limit_down": 0.0,
                }
            )
    benchmark_frame = pd.DataFrame(
        {
            "datetime": calendar,
            "instrument": "000300.SH",
            "open": 100.0,
        }
    ).set_index(["datetime", "instrument"])

    outcome = _build_one_date_outcomes(
        decision=pd.Timestamp("2026-02-02"),
        calendar=calendar,
        positions={value: index for index, value in enumerate(calendar)},
        eligible_by_date=_eligible_symbols_by_date(snapshot, calendar),
        market=_normalize_market_frame(
            pd.DataFrame(rows).set_index(["datetime", "instrument"])
        ),
        benchmark_open=_benchmark_series(benchmark_frame, "open"),
        suspended_by_date={},
        request=_request(),
    )

    assert set(outcome["target_trade_date"]) == {pd.Timestamp("2026-02-03")}
    assert set(outcome["planned_exit_trade_date"]) == {pd.Timestamp("2026-03-10")}
    assert set(outcome["effective_exit_trade_date"]) == {pd.Timestamp("2026-03-10")}
