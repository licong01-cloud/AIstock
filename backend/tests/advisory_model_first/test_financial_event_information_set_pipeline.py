from __future__ import annotations

from itertools import combinations
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import financial_event_information_set_pipeline as pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.financial_event_information_set_contracts import (
    EVENT_DIRECTION_BY_TYPE,
    EVENT_DISCLOSURE_FEATURES,
    EVENT_PARENT_FEATURES,
    EVENT_SIGNED_FEATURES,
    FrozenFinancialEventInformationSetRequestV1,
    build_default_event_model_trials,
)
from backend.services.advisory_model_first.financial_event_information_set_pipeline import (
    attach_event_outcomes,
    build_event_feature_panel,
    evaluate_event_models,
    run_event_crossfit,
    validate_event_feature_support,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _request(**overrides: object) -> FrozenFinancialEventInformationSetRequestV1:
    values: dict[str, object] = {
        "request_id": "advn3fevreq_" + "1" * 24,
        "request_sha256": "1" * 64,
        "model_trials": build_default_event_model_trials(),
        "expected_parent_row_count": 80,
        "expected_source_row_count": 12,
        "expected_decision_date_count": 8,
        "expected_ready_path_count": 28,
        "expected_oof_predictions_per_row": 7,
        "minimum_evaluable_days": 8,
        "minimum_intervention_days": 1,
        "minimum_intervention_fraction": 0.0,
        "minimum_intervention_days_per_regime": 0,
        "minimum_parent_lift_bps": 5.0,
        "minimum_top20_disclosure_fraction_120": 0.0,
        "minimum_top20_qualifying_fraction_120": 0.0,
        "minimum_top20_supported_days": 0,
        "minimum_top20_disclosure_count": 0,
        "minimum_top50_mixed_qualifying_days": 0,
        "current_familywise_hypothesis_count": 1,
        "cumulative_primary_comparison_count": 1,
        "cumulative_candidate_index": 83,
        "block_length_trading_days": 2,
        "bootstrap_repetitions": 100,
        "bootstrap_seed": 20260905,
    }
    values.update(overrides)
    return FrozenFinancialEventInformationSetRequestV1.model_construct(**values)


def _paths(dates: list[str]) -> list[dict[str, object]]:
    return [
        {
            "path_id": f"path-{index:02d}",
            "status": "READY",
            "train_dates": [day for offset, day in enumerate(dates) if offset not in validation],
            "validation_dates": [day for offset, day in enumerate(dates) if offset in validation],
        }
        for index, validation in enumerate(combinations(range(8), 2))
    ]


def _event_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    calendar = pd.bdate_range("2024-01-02", periods=100)
    decisions = calendar[-8:]
    instruments = tuple(f"{index:06d}.SZ" for index in range(10))
    parent_rows: list[dict[str, object]] = []
    for day_index, decision in enumerate(decisions):
        for instrument_index, instrument in enumerate(instruments):
            score = float(instrument_index + day_index / 100)
            parent_rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "instrument": instrument,
                    "score": score,
                    "economic_net_excess_bps": score * 10,
                    "outcome_known": True,
                    "future_label": 999.0,
                }
            )
    source_by_family = {
        "financial_forecast": "tushare_forecast",
        "financial_express": "tushare_express",
        "financial_indicator": "tushare_fina_indicator",
    }
    event_rows: list[dict[str, object]] = []
    for index, (event_type, direction) in enumerate(EVENT_DIRECTION_BY_TYPE.items()):
        family = "_".join(event_type.split("_")[:2])
        source_type = source_by_family[family]
        effective = calendar[20 + index * 3]
        event_rows.append(
            {
                "source_type": source_type,
                "source_record_key": f"event-{index:02d}",
                "raw_observation_id": index + 1,
                "source_row_hash": f"{index + 1:064x}",
                "instrument": instruments[index % 4],
                "source_event_date": effective.date().isoformat(),
                "report_period": "2024-03-31",
                "event_family": family,
                "event_type": event_type,
                "should_signal": direction != 0,
                "severity_score": 0.1 + index / 100,
                "confidence": 0.5 + index / 100,
                "effective_trade_date": effective,
                "source_time_quality": "DATE_ONLY_BACKFILLED_NON_VINTAGE",
                "effective_rule": "announcement_date_only_next_trading_day",
            }
        )
    return pd.DataFrame(parent_rows), pd.DataFrame(event_rows), calendar


def test_feature_builder_keeps_missing_keys_and_applies_frozen_direction_windows() -> None:
    parent, events, calendar = _event_inputs()
    features = build_event_feature_panel(parent=parent, events=events, trading_calendar=calendar)
    assert len(features) == len(parent)
    missing = features.loc[features["instrument"] == "000009.SZ"]
    assert missing["event_disclosure_seen_120"].eq(0).all()
    assert missing["event_latest_disclosure_age_120"].eq(121).all()
    assert missing[list(EVENT_SIGNED_FEATURES)].eq(0).all().all()
    supported = features.loc[features["instrument"] == "000000.SZ"].iloc[-1]
    assert supported["event_disclosure_seen_120"] == 1
    assert supported["event_signed_value_sum_120"] != 0
    assert (
        np.isfinite(features[[*EVENT_PARENT_FEATURES, *EVENT_DISCLOSURE_FEATURES, *EVENT_SIGNED_FEATURES]]).all().all()
    )


def test_future_and_outcome_poison_cannot_change_target_free_features() -> None:
    parent, events, calendar = _event_inputs()
    baseline = build_event_feature_panel(parent=parent, events=events, trading_calendar=calendar)
    poisoned_parent = parent.copy()
    poisoned_parent["economic_net_excess_bps"] = -1_000_000.0
    poisoned_parent["future_label"] = -999.0
    future = events.iloc[[0]].copy()
    future["source_record_key"] = "future-event"
    future["source_row_hash"] = "f" * 64
    future["effective_trade_date"] = calendar[-1] + pd.offsets.BDay(1)
    extended_calendar = calendar.append(pd.DatetimeIndex([future.iloc[0]["effective_trade_date"]]))
    rebuilt = build_event_feature_panel(
        parent=poisoned_parent,
        events=pd.concat([events, future], ignore_index=True),
        trading_calendar=extended_calendar,
    )
    pd.testing.assert_frame_equal(baseline, rebuilt)


def test_event_direction_contradiction_fails_closed() -> None:
    parent, events, calendar = _event_inputs()
    events.loc[events["event_type"].eq("financial_forecast_large_growth"), "should_signal"] = False
    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_event_feature_panel(parent=parent, events=events, trading_calendar=calendar)
    assert caught.value.reason_code == "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH"


def test_support_preserves_all_rows_and_reports_daily_intervention_state() -> None:
    parent, events, calendar = _event_inputs()
    features = build_event_feature_panel(parent=parent, events=events, trading_calendar=calendar)
    daily, report = validate_event_feature_support(features=features, events=events, request=_request())
    assert len(daily) == 8
    assert report["parent_row_count"] == 80
    assert report["source_row_count"] == 12
    assert set(report["direction_counts"]) == {"-1", "0", "1"}


def test_crossfit_scores_all_rows_exactly_seven_times() -> None:
    dates = pd.bdate_range("2025-01-02", periods=8)
    rows: list[dict[str, object]] = []
    for day_index, day in enumerate(dates):
        for instrument_index in range(10):
            row: dict[str, object] = {
                "decision_as_of_trade_date": day,
                "instrument": f"{instrument_index:06d}.SZ",
                "parent_rank_pct": (instrument_index + 1) / 10,
                "economic_net_excess_bps": float((9 - instrument_index) * 10 + day_index),
                "outcome_known": True,
            }
            for feature_index, name in enumerate((*EVENT_DISCLOSURE_FEATURES, *EVENT_SIGNED_FEATURES), 1):
                row[name] = float((instrument_index + 1) * feature_index + day_index / (feature_index + 1))
            rows.append(row)
    panel = pd.DataFrame(rows)
    paths = _paths([value.date().isoformat() for value in dates])
    scores, diagnostics = run_event_crossfit(panel=panel, paths=paths, request=_request())
    assert len(scores) == 80
    assert len(diagnostics) == 84
    for column in (
        "parent_comparator_oof_score",
        "disclosure_control_oof_score",
        "signed_candidate_oof_score",
    ):
        assert scores[f"{column}_count"].eq(7).all()
        assert np.isfinite(scores[column]).all()


def _evaluation_panel(*, candidate_wins: bool) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    regimes: list[dict[str, object]] = []
    coverage: list[dict[str, object]] = []
    for day_index, day in enumerate(pd.bdate_range("2025-02-03", periods=8)):
        regimes.append({"decision_as_of_trade_date": day, "regime": "DOWN" if day_index % 2 else "UP_OR_FLAT"})
        coverage.append({"decision_as_of_trade_date": day, "instrument_count": 10})
        for instrument_index in range(10):
            parent_score = float(instrument_index)
            candidate_score = float(10 - instrument_index) if candidate_wins else parent_score
            rows.append(
                {
                    "decision_as_of_trade_date": day,
                    "instrument": f"{instrument_index:06d}.SZ",
                    "parent_rank_pct": parent_score,
                    "economic_net_excess_bps": 100.0 if instrument_index < 5 else 0.0,
                    "outcome_known": True,
                    "parent_comparator_oof_score": parent_score,
                    "disclosure_control_oof_score": parent_score,
                    "signed_candidate_oof_score": candidate_score,
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(regimes), pd.DataFrame(coverage)


def test_evaluator_requires_both_controls_and_selects_once() -> None:
    scores, regimes, coverage = _evaluation_panel(candidate_wins=True)
    daily, summary, stability, frontier = evaluate_event_models(
        oof_scores=scores,
        regime_daily=regimes,
        feature_coverage_daily=coverage,
        feature_support={"support_sufficient": True, "reason_codes": []},
        request=_request(),
    )
    assert len(daily) == 8
    assert all(value["support_sufficient"] for value in summary["intervention_support"].values())
    assert stability["positive_joint_time_block_count"] == 4
    assert summary["eligible"] is True
    assert summary["evidence_class"] == "EXPLORATORY_CANDIDATE_SELECTED_NON_VINTAGE"
    assert frontier["selected_trial_id"] == "EVENT_SIGNED_CONTENT_V1"
    assert frontier["candidate_reselection_allowed"] is False


def test_evaluator_routes_insufficient_support_without_negative_claim() -> None:
    scores, regimes, coverage = _evaluation_panel(candidate_wins=False)
    _, summary, _, frontier = evaluate_event_models(
        oof_scores=scores,
        regime_daily=regimes,
        feature_coverage_daily=coverage,
        feature_support={"support_sufficient": True, "reason_codes": []},
        request=_request(),
    )
    assert summary["eligible"] is False
    assert summary["support_sufficient"] is False
    assert summary["evidence_class"] == "EXPLORATORY_INSUFFICIENT_SUPPORT_NON_VINTAGE"
    assert frontier["selected_trial_id"] is None


def test_familywise_interval_uses_registered_one_sided_alpha(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[float] = []

    def fake_interval(values, *, block_length, repetitions, seed, alpha):  # noqa: ANN001
        observed.append(alpha)
        return 1.0, 2.0

    monkeypatch.setattr(pipeline, "_moving_block_interval", fake_interval)
    result = pipeline._metric_inference([1.0, 2.0, 3.0], request=_request(), alpha=0.01, threshold=0.0, seed_offset=0)
    assert observed == [0.05, 0.02]
    assert result["familywise_alpha"] == 0.01


def test_outcomes_attach_requires_exact_parent_score_and_keys() -> None:
    parent, events, calendar = _event_inputs()
    features = build_event_feature_panel(parent=parent, events=events, trading_calendar=calendar)
    attached = attach_event_outcomes(features=features, parent_outcomes=parent)
    assert len(attached) == len(features)
    poisoned = parent.copy()
    poisoned.loc[0, "score"] += 1
    with pytest.raises(AdvisoryModelFirstError):
        attach_event_outcomes(features=features, parent_outcomes=poisoned)


def test_parent_parity_uses_all_rank_sets_and_only_evaluable_top5_values() -> None:
    dates = pd.bdate_range("2025-04-14", periods=2)
    instruments = "000001.SZ,000002.SZ,000003.SZ,000004.SZ,000005.SZ"
    daily = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "parent_rank_ic": [0.1, 0.2],
            "parent_top5_net_excess_bps": [np.nan, 20.0],
            "parent_instruments": [instruments, instruments],
        }
    )
    source_instruments = '["000001.SZ","000002.SZ","000003.SZ","000004.SZ","000005.SZ"]'
    parent_top5 = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "top5_net_excess_bps": [10.0, 20.0],
            "instruments": [source_instruments, source_instruments],
        }
    )
    parent_signal = pd.DataFrame({"decision_as_of_trade_date": dates, "matured_rank_ic": [0.1, 0.2]})
    pipeline._validate_parent_daily_parity(
        daily=daily,
        parent_top5_daily=parent_top5,
        parent_signal_daily=parent_signal,
    )
    parent_top5.loc[1, "top5_net_excess_bps"] = 21.0
    with pytest.raises(AdvisoryModelFirstError) as caught:
        pipeline._validate_parent_daily_parity(
            daily=daily,
            parent_top5_daily=parent_top5,
            parent_signal_daily=parent_signal,
        )
    assert caught.value.reason_code == "ADVISORY_N3_EVENT_MVE_BASELINE_PARITY_FAILED"


def test_calendar_rebuild_uses_n1_data_cutoff_not_declared_asset_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = pd.bdate_range("2023-01-02", "2026-03-10")
    identity_window = calendar[calendar >= pd.Timestamp("2023-09-01")]
    identity_hash = canonical_json_sha256(
        {"market_sessions": [item.date().isoformat() for item in identity_window]}
    )
    observed: list[tuple[str, str]] = []
    monkeypatch.setattr(pipeline, "initialize_qlib", lambda root: observed.append(("root", str(root))))

    def fake_calendar(start: str, end: str) -> pd.DatetimeIndex:
        observed.append((start, end))
        return calendar

    monkeypatch.setattr(pipeline, "load_trading_calendar", fake_calendar)
    request = SimpleNamespace(
        qlib_daily_root="/tmp/qlib",
        data_cutoff=pd.Timestamp("2026-03-10").date(),
        market_calendar_identity=SimpleNamespace(
            cutoff_trade_date=pd.Timestamp("2026-06-30").date(),
            row_count=len(identity_window),
            sha256=identity_hash,
        ),
    )
    result = pipeline._load_and_verify_calendar(request)
    assert result.equals(calendar)
    assert observed == [("root", "/tmp/qlib"), ("2023-01-01", "2026-03-10")]
