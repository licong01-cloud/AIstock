from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.minute_information_set_contracts import (
    MINUTE_MVE_EXPANDED_FEATURES,
    MINUTE_MVE_RAW_ECONOMIC_FEATURES,
    FrozenMinuteInformationSetRequestV1,
    build_default_minute_model_trials,
)
from backend.services.advisory_model_first.minute_information_set_pipeline import (
    _normalize_parent_daily,
    aggregate_minute_day,
    build_minute_feature_panel,
    evaluate_minute_models,
    run_minute_crossfit,
)


def _request() -> FrozenMinuteInformationSetRequestV1:
    return FrozenMinuteInformationSetRequestV1.model_construct(
        request_id="advn3minreq_" + "1" * 24,
        request_sha256="1" * 64,
        model_trials=build_default_minute_model_trials(),
        expected_ready_path_count=28,
        expected_oof_predictions_per_row=7,
        minimum_feature_coverage=0.8,
        minimum_evaluable_days=8,
        minimum_intervention_days=1,
        minimum_intervention_fraction=0.0,
        minimum_intervention_days_per_regime=0,
        minimum_parent_lift_bps=5.0,
        familywise_hypothesis_count=4,
        block_length_trading_days=2,
        bootstrap_repetitions=100,
        bootstrap_seed=20260903,
        expected_session_wide_single_bar_deficit_dates=(),
    )


def _source() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day_index, day in enumerate(pd.bdate_range("2025-01-02", periods=8)):
        for instrument_index in range(10):
            parent = float(instrument_index + day_index / 100)
            rows.append(
                {
                    "decision_as_of_trade_date": day,
                    "instrument": f"{instrument_index:06d}.SZ",
                    "score__IC_WEIGHTED_PARENT": parent,
                    "economic_net_excess_bps": parent * 10.0,
                    "outcome_known": True,
                    "future_only": 999.0,
                }
            )
    return pd.DataFrame(rows)


def _calendar() -> pd.DatetimeIndex:
    values: list[pd.Timestamp] = []
    for day in pd.bdate_range("2025-01-02", periods=8):
        for clock in ("09:30:00", "09:31:00", "14:30:00", "14:31:00"):
            values.append(pd.Timestamp(f"{day.date().isoformat()} {clock}"))
    return pd.DatetimeIndex(values)


def _loader(
    decision_date: pd.Timestamp, instruments: list[str] | tuple[str, ...], fields: tuple[str, ...]
) -> pd.DataFrame:
    slots = pd.DatetimeIndex(
        [
            f"{decision_date.date().isoformat()} 09:30:00",
            f"{decision_date.date().isoformat()} 09:31:00",
            f"{decision_date.date().isoformat()} 14:30:00",
            f"{decision_date.date().isoformat()} 14:31:00",
        ]
    )
    index = pd.MultiIndex.from_product([instruments, slots], names=["instrument", "datetime"])
    frame = pd.DataFrame(index=index, columns=fields, dtype=float)
    for instrument_index, instrument in enumerate(instruments):
        base = 10.0 + instrument_index
        frame.loc[instrument, "$open"] = [base, base + 0.1, base + 0.2, base + 0.3]
        frame.loc[instrument, "$high"] = [base + 0.2, base + 0.3, base + 0.4, base + 0.5]
        frame.loc[instrument, "$low"] = [base - 0.1, base, base + 0.1, base + 0.2]
        frame.loc[instrument, "$close"] = [base + 0.1, base + 0.2, base + 0.3, base + 0.4]
        frame.loc[instrument, "$volume"] = [100.0, 100.0, 100.0, 100.0]
        frame.loc[instrument, "$amount"] = np.asarray([100.0, 100.0, 100.0, 100.0]) * (base + 0.2)
        frame.loc[instrument, "$limit_up"] = 0.0
        frame.loc[instrument, "$limit_down"] = 0.0
    return frame


def _paths(dates: list[str]) -> list[dict[str, object]]:
    paths: list[dict[str, object]] = []
    for index, validation in enumerate(combinations(range(8), 2)):
        paths.append(
            {
                "path_id": f"path-{index:02d}",
                "status": "READY",
                "train_dates": [day for offset, day in enumerate(dates) if offset not in validation],
                "validation_dates": [day for offset, day in enumerate(dates) if offset in validation],
            }
        )
    return paths


def test_aggregate_preserves_whole_day_and_partial_missing_and_market_slot_gap() -> None:
    day = pd.Timestamp("2025-12-02")
    slots = pd.DatetimeIndex(
        [
            "2025-12-02 09:30:00",
            "2025-12-02 09:31:00",
            "2025-12-02 13:00:00",
            "2025-12-02 14:30:00",
            "2025-12-02 14:31:00",
        ]
    )
    instruments = ("000001.SZ", "000002.SZ", "000003.SZ")
    index = pd.MultiIndex.from_product([instruments, slots], names=["instrument", "datetime"])
    fields = tuple(
        f"${name}" for name in ("open", "high", "low", "close", "volume", "amount", "limit_up", "limit_down")
    )
    raw = pd.DataFrame(np.nan, index=index, columns=fields)
    effective_slots = slots.delete(2)
    raw.loc[(slice(None), slots[2]), ["$limit_up", "$limit_down"]] = 0.0
    for field in ("$open", "$high", "$low", "$close"):
        raw.loc[("000001.SZ", effective_slots), field] = [10.0, 10.1, 10.2, 10.3]
    raw.loc[("000001.SZ", effective_slots), "$volume"] = 100.0
    raw.loc[("000001.SZ", effective_slots), "$amount"] = 1_000.0
    raw.loc[("000001.SZ", effective_slots), ["$limit_up", "$limit_down"]] = 0.0
    for field in ("$open", "$high", "$low", "$close"):
        raw.loc[("000003.SZ", effective_slots[:1]), field] = 20.0
    raw.loc[("000003.SZ", effective_slots[:1]), "$volume"] = 100.0
    raw.loc[("000003.SZ", effective_slots[:1]), "$amount"] = 2_000.0
    raw.loc[("000003.SZ", effective_slots[:1]), ["$limit_up", "$limit_down"]] = 0.0

    result, coverage = aggregate_minute_day(
        decision_date=day,
        instruments=instruments,
        calendar_slots=slots,
        raw=raw,
        minimum_feature_coverage=0.8,
    )
    assert coverage["market_wide_empty_slots"] == ["2025-12-02 13:00:00"]
    assert coverage["complete_instrument_count"] == 1
    assert coverage["partial_instrument_count"] == 1
    assert coverage["whole_day_missing_instrument_count"] == 1
    complete = result.set_index("instrument").loc["000001.SZ"]
    missing = result.set_index("instrument").loc["000002.SZ"]
    partial = result.set_index("instrument").loc["000003.SZ"]
    assert complete["minute_coverage_fraction"] == 1.0
    assert complete[list(MINUTE_MVE_RAW_ECONOMIC_FEATURES)].notna().all()
    assert missing["minute_available"] == 0 and missing["minute_coverage_fraction"] == 0.0
    assert missing[list(MINUTE_MVE_RAW_ECONOMIC_FEATURES)].isna().all()
    assert partial["minute_available"] == 1 and partial["minute_coverage_fraction"] == 0.25
    assert partial[list(MINUTE_MVE_RAW_ECONOMIC_FEATURES)].isna().all()


def test_realized_volatility_does_not_bridge_nonconsecutive_calendar_slots() -> None:
    day = pd.Timestamp("2025-01-02")
    slots = pd.DatetimeIndex(
        [
            "2025-01-02 09:30:00",
            "2025-01-02 09:31:00",
            "2025-01-02 11:30:00",
            "2025-01-02 13:00:00",
            "2025-01-02 14:30:00",
            "2025-01-02 14:31:00",
        ]
    )
    index = pd.MultiIndex.from_product([("000001.SZ",), slots], names=["instrument", "datetime"])
    fields = [
        f"${name}"
        for name in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "limit_up",
            "limit_down",
        )
    ]
    raw = pd.DataFrame(0.0, index=index, columns=fields)
    closes = np.asarray([10.0, 11.0, 100.0, 50.0, 20.0, 22.0])
    for field in ("$open", "$high", "$low", "$close"):
        raw[field] = closes
    raw["$volume"] = 100.0
    raw["$amount"] = closes * 100.0

    result, _ = aggregate_minute_day(
        decision_date=day,
        instruments=("000001.SZ",),
        calendar_slots=slots,
        raw=raw,
        minimum_feature_coverage=0.8,
    )

    expected = np.sqrt(np.log(11.0 / 10.0) ** 2 + np.log(22.0 / 20.0) ** 2) * 10_000.0
    assert result.loc[0, "realized_volatility_bps"] == pytest.approx(expected, rel=1e-6)


def test_session_wide_single_bar_deficit_is_typed_without_hiding_raw_coverage() -> None:
    day = pd.Timestamp("2025-12-08")
    slots = pd.DatetimeIndex(
        [
            "2025-12-08 09:30:00",
            "2025-12-08 09:31:00",
            "2025-12-08 11:30:00",
            "2025-12-08 13:00:00",
        ]
    )
    instruments = ("000001.SZ", "000002.SZ")
    index = pd.MultiIndex.from_product([instruments, slots], names=["instrument", "datetime"])
    fields = [
        f"${name}"
        for name in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "limit_up",
            "limit_down",
        )
    ]
    raw = pd.DataFrame(1.0, index=index, columns=fields)
    raw[["$limit_up", "$limit_down"]] = 0.0
    raw.loc[("000001.SZ", slots[2]), ["$open", "$high", "$low", "$close"]] = np.nan
    raw.loc[("000002.SZ", slots[3]), ["$open", "$high", "$low", "$close"]] = np.nan

    result, coverage = aggregate_minute_day(
        decision_date=day,
        instruments=instruments,
        calendar_slots=slots,
        raw=raw,
        minimum_feature_coverage=0.5,
    )

    assert coverage["market_wide_empty_slots"] == []
    assert coverage["session_wide_single_bar_deficit"] is True
    assert coverage["complete_instrument_count"] == 0
    assert coverage["partial_instrument_count"] == 2
    assert coverage["normalized_complete_instrument_count"] == 2
    assert coverage["normalized_partial_instrument_count"] == 0
    assert result["minute_coverage_fraction"].tolist() == pytest.approx([0.75, 0.75])


def test_parent_daily_duplicate_date_is_rejected_instead_of_silently_collapsed() -> None:
    duplicated = pd.DataFrame(
        {
            "decision_as_of_trade_date": ["2025-01-02", "2025-01-02"],
            "parent_rank_ic": [0.1, 0.2],
        }
    )
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _normalize_parent_daily(duplicated)
    assert caught.value.reason_code == "ADVISORY_N3_MINUTE_MVE_BASELINE_PARITY_FAILED"


def test_aggregate_rejects_future_timestamp_and_nonbinary_limit_flag() -> None:
    day = pd.Timestamp("2025-01-02")
    slots = pd.DatetimeIndex(["2025-01-02 09:30:00", "2025-01-03 09:30:00"])
    raw = pd.DataFrame(
        1.0,
        index=pd.MultiIndex.from_product([("000001.SZ",), slots], names=["instrument", "datetime"]),
        columns=[f"${name}" for name in ("open", "high", "low", "close", "volume", "amount", "limit_up", "limit_down")],
    )
    with pytest.raises(AdvisoryModelFirstError) as caught:
        aggregate_minute_day(
            decision_date=day,
            instruments=("000001.SZ",),
            calendar_slots=slots,
            raw=raw,
            minimum_feature_coverage=0.8,
        )
    assert caught.value.reason_code == "ADVISORY_N3_MINUTE_MVE_PIT_LEAKAGE"

    valid_slots = pd.DatetimeIndex(["2025-01-02 09:30:00", "2025-01-02 15:00:00"])
    raw = raw.iloc[:2].copy()
    raw.index = pd.MultiIndex.from_product([("000001.SZ",), valid_slots], names=["instrument", "datetime"])
    raw["$limit_up"] = 2.0
    with pytest.raises(AdvisoryModelFirstError) as caught:
        aggregate_minute_day(
            decision_date=day,
            instruments=("000001.SZ",),
            calendar_slots=valid_slots,
            raw=raw,
            minimum_feature_coverage=0.8,
        )
    assert caught.value.reason_code == "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID"


def test_feature_builder_uses_exact_t_day_features_and_ignores_labels() -> None:
    source = _source()
    baseline, coverage = build_minute_feature_panel(
        source=source,
        calendar=_calendar(),
        loader=_loader,
        request=_request(),
    )
    poisoned = source.copy()
    poisoned["economic_net_excess_bps"] = -1_000_000.0
    poisoned["future_only"] = -999.0
    rebuilt, rebuilt_coverage = build_minute_feature_panel(
        source=poisoned,
        calendar=_calendar(),
        loader=_loader,
        request=_request(),
    )
    pd.testing.assert_frame_equal(
        baseline[["decision_as_of_trade_date", "instrument", *MINUTE_MVE_EXPANDED_FEATURES]],
        rebuilt[["decision_as_of_trade_date", "instrument", *MINUTE_MVE_EXPANDED_FEATURES]],
    )
    pd.testing.assert_frame_equal(coverage, rebuilt_coverage)


def test_crossfit_imputes_normal_missing_and_scores_every_row_seven_times() -> None:
    features, _ = build_minute_feature_panel(source=_source(), calendar=_calendar(), loader=_loader, request=_request())
    features.loc[0, list(MINUTE_MVE_EXPANDED_FEATURES[3:])] = np.nan
    features.loc[0, "outcome_known"] = False
    features.loc[0, "economic_net_excess_bps"] = np.nan
    dates = [value.date().isoformat() for value in sorted(features["decision_as_of_trade_date"].unique())]
    oof, diagnostics = run_minute_crossfit(features=features, paths=_paths(dates), request=_request())
    assert len(oof) == 80
    assert len(diagnostics) == 56
    assert oof["comparator_oof_score_count"].eq(7).all()
    assert oof["candidate_oof_score_count"].eq(7).all()
    assert np.isfinite(oof[["comparator_oof_score", "candidate_oof_score"]]).all().all()


def _evaluation_panel(*, candidate_wins: bool) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    regimes: list[dict[str, object]] = []
    coverage: list[dict[str, object]] = []
    for day_index, day in enumerate(pd.bdate_range("2025-02-03", periods=8)):
        regimes.append({"decision_as_of_trade_date": day, "regime": "DOWN" if day_index % 2 else "UP_OR_FLAT"})
        coverage.append(
            {
                "decision_as_of_trade_date": day,
                "instrument_count": 10,
                "raw_calendar_slot_count": 4,
                "effective_calendar_slot_count": 4,
                "market_wide_empty_slot_count": 0,
                "market_wide_empty_slots": [],
                "complete_instrument_count": 10,
                "partial_instrument_count": 0,
                "whole_day_missing_instrument_count": 0,
                "available_fraction": 1.0,
                "mean_coverage_fraction": 1.0,
            }
        )
        for instrument_index in range(10):
            parent = float(instrument_index)
            label = 100.0 if instrument_index < 5 else 0.0
            candidate = float(10 - instrument_index) if candidate_wins else parent
            rows.append(
                {
                    "decision_as_of_trade_date": day,
                    "instrument": f"{instrument_index:06d}.SZ",
                    "parent_rank_pct": parent,
                    "economic_net_excess_bps": label,
                    "outcome_known": True,
                    "comparator_oof_score": parent,
                    "candidate_oof_score": candidate,
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(regimes), pd.DataFrame(coverage)


def test_evaluator_selects_candidate_only_when_dual_baseline_gates_pass() -> None:
    scores, regimes, coverage = _evaluation_panel(candidate_wins=True)
    daily, summary, frontier = evaluate_minute_models(
        oof_scores=scores,
        regime_daily=regimes,
        minute_coverage_daily=coverage,
        request=_request(),
    )
    assert len(daily) == 8
    assert summary["support"]["parent"]["support_sufficient"] is True
    assert summary["support"]["comparator"]["support_sufficient"] is True
    assert summary["eligible"] is True
    assert frontier["selected_trial_id"] == "N3_MINUTE_INFORMATION_EXPANDED_V1"


def test_evaluator_routes_zero_when_candidate_does_not_intervene() -> None:
    scores, regimes, coverage = _evaluation_panel(candidate_wins=False)
    _, summary, frontier = evaluate_minute_models(
        oof_scores=scores,
        regime_daily=regimes,
        minute_coverage_daily=coverage,
        request=_request(),
    )
    assert summary["eligible"] is False
    assert any("INTERVENTION_DAY_COUNT_BELOW_MINIMUM" in value for value in summary["reason_codes"])
    assert frontier["selected_trial_id"] is None
