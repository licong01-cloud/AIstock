from __future__ import annotations

from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import margin_information_set_pipeline as pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.margin_information_set_contracts import (
    MARGIN_MVE_EXPANDED_FEATURES,
    MARGIN_MVE_RANKED_DYNAMICS_FEATURES,
    MARGIN_MVE_RAW_DYNAMICS_FEATURES,
    MARGIN_MVE_SOURCE_FIELDS,
    FrozenMarginInformationSetRequestV1,
    build_default_margin_model_trials,
)
from backend.services.advisory_model_first.margin_information_set_pipeline import (
    _read_margin_h5,
    _validate_parent_daily_parity,
    build_margin_feature_panel,
    build_margin_source_projection,
    evaluate_margin_models,
    run_margin_crossfit,
    validate_margin_feature_support,
)


def _request() -> FrozenMarginInformationSetRequestV1:
    return FrozenMarginInformationSetRequestV1.model_construct(
        request_id="advn3margreq_" + "1" * 24,
        request_sha256="1" * 64,
        model_trials=build_default_margin_model_trials(),
        expected_ready_path_count=28,
        expected_oof_predictions_per_row=7,
        minimum_evaluable_days=8,
        minimum_intervention_days=1,
        minimum_intervention_fraction=0.0,
        minimum_intervention_days_per_regime=0,
        minimum_parent_lift_bps=5.0,
        current_familywise_hypothesis_count=1,
        cumulative_primary_comparison_count=1,
        cumulative_candidate_index=80,
        block_length_trading_days=2,
        bootstrap_repetitions=100,
        bootstrap_seed=20260904,
    )


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


def _margin_rows(dates: pd.DatetimeIndex, instruments: tuple[str, ...]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day_index, day in enumerate(dates):
        for instrument_index, instrument in enumerate(instruments):
            base = float(100 + day_index * 10 + instrument_index)
            rows.append(
                {
                    "datetime": day,
                    "instrument": instrument,
                    "md_rzye": base + 1,
                    "md_rqye": base + 2,
                    "md_rzmre": base + 3,
                    "md_rqyl": base + 4,
                    "md_rzche": base + 5,
                    "md_rqchl": base + 6,
                    "md_rqmcl": base + 7,
                    "md_rzrqye": base + 8,
                }
            )
    return pd.DataFrame(rows)


def _feature_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    instruments = tuple(f"{index:06d}.SZ" for index in range(10))
    calendar = pd.bdate_range("2025-01-02", periods=14)
    decisions = calendar[6:14]
    mapping_rows: list[dict[str, object]] = []
    for offset, decision in enumerate(decisions, start=6):
        mapping_rows.append(
            {
                "decision_as_of_trade_date": decision,
                "source_date_d": calendar[offset - 1],
                "source_date_d1": calendar[offset - 2],
                "source_date_d5": calendar[offset - 6],
                "instrument_count": len(instruments),
            }
        )
    parent_rows: list[dict[str, object]] = []
    for day_index, decision in enumerate(decisions):
        for instrument_index, instrument in enumerate(instruments):
            score = float(instrument_index + day_index / 100)
            parent_rows.append(
                {
                    "arm_id": "CURRENT_IC_PARENT",
                    "decision_as_of_trade_date": decision,
                    "instrument": instrument,
                    "score": score,
                    "economic_net_excess_bps": score * 10,
                    "outcome_known": True,
                    "future_only": 999.0,
                }
            )
    return (
        pd.DataFrame(parent_rows),
        _margin_rows(calendar[:-1], instruments),
        pd.DataFrame(mapping_rows),
    )


def test_cross_snapshot_projection_accepts_key_drift_but_rejects_value_drift() -> None:
    dates = pd.DatetimeIndex(["2025-01-02", "2025-01-03"])
    current = _margin_rows(dates, ("000001.SZ", "000002.SZ"))
    secondary = current.iloc[:-1].copy()
    projection, parity = build_margin_source_projection(
        current=current,
        secondary=secondary,
        source_window_start=dates[0],
        source_window_end=dates[-1],
    )
    assert len(projection) == 3
    assert parity["common_key_count"] == 3
    assert parity["current_only_key_count"] == 1
    assert parity["secondary_only_key_count"] == 0
    assert parity["value_drift_row_count"] == 0

    drifted = secondary.copy()
    drifted.loc[0, "md_rzye"] += 1
    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_margin_source_projection(
            current=current,
            secondary=drifted,
            source_window_start=dates[0],
            source_window_end=dates[-1],
        )
    assert caught.value.reason_code == "ADVISORY_N3_MARGIN_MVE_SOURCE_VALUE_DRIFT"


@pytest.mark.parametrize("storage_format", ["table", "fixed"])
def test_margin_h5_reader_enforces_shape_and_projects_exact_keys(tmp_path: Path, storage_format: str) -> None:
    dates = pd.DatetimeIndex(["2025-01-02", "2025-01-03"])
    instruments = ("000001.SZ", "000002.SZ")
    index = pd.MultiIndex.from_product([dates, instruments], names=["datetime", "instrument"])
    raw = pd.DataFrame(
        np.ones((len(index), len(MARGIN_MVE_SOURCE_FIELDS)), dtype=np.float32),
        index=index,
        columns=MARGIN_MVE_SOURCE_FIELDS,
    )
    path = tmp_path / f"margin-{storage_format}.h5"
    raw.to_hdf(path, key="data", format=storage_format)
    projected, rows_read, invalid = _read_margin_h5(
        path,
        expected_format=storage_format,
        required_dates={dates[0]},
        instruments={instruments[0]},
        calendar=dates,
        chunk_rows=2,
    )
    assert rows_read == 4
    assert len(projected) == 1
    assert projected.loc[0, "datetime"] == dates[0]
    assert projected.loc[0, "instrument"] == instruments[0]
    assert invalid == {name: 0 for name in MARGIN_MVE_SOURCE_FIELDS}


def test_feature_builder_uses_exact_t_minus_one_history_and_keeps_missing_keys() -> None:
    parent, projection, coverage = _feature_inputs()
    missing_instrument = "000000.SZ"
    projection = projection.loc[projection["instrument"] != missing_instrument].copy()
    features = build_margin_feature_panel(
        parent_outcomes=parent,
        source_projection=projection,
        source_coverage_daily=coverage,
    )
    assert len(features) == len(parent)
    missing = features.loc[features["instrument"] == missing_instrument]
    assert missing["margin_row_available"].eq(0.0).all()
    assert missing["margin_history_coverage_fraction"].eq(0.0).all()
    assert missing[list(MARGIN_MVE_RAW_DYNAMICS_FEATURES)].isna().all().all()

    first = features.loc[features["instrument"] == "000001.SZ"].iloc[0]
    decision = pd.Timestamp(first["decision_as_of_trade_date"])
    date_map = coverage.set_index("decision_as_of_trade_date").loc[decision]
    current = projection.loc[
        (projection["datetime"] == date_map["source_date_d"]) & (projection["instrument"] == "000001.SZ"),
        "md_rzye",
    ].iloc[0]
    lag = projection.loc[
        (projection["datetime"] == date_map["source_date_d1"]) & (projection["instrument"] == "000001.SZ"),
        "md_rzye",
    ].iloc[0]
    assert first["rzye_log_delta_1d"] == pytest.approx(np.log1p(current) - np.log1p(lag), rel=1e-6)
    assert date_map["source_date_d"] < decision


def test_outcome_and_future_poison_cannot_change_margin_features() -> None:
    parent, projection, coverage = _feature_inputs()
    baseline = build_margin_feature_panel(
        parent_outcomes=parent,
        source_projection=projection,
        source_coverage_daily=coverage,
    )
    poisoned = parent.copy()
    poisoned["economic_net_excess_bps"] = -1_000_000.0
    poisoned["future_only"] = -999.0
    future_projection = projection.iloc[[0]].copy()
    future_projection["datetime"] = pd.Timestamp(parent["decision_as_of_trade_date"].max()) + pd.offsets.BDay(1)
    poisoned_projection = pd.concat([projection, future_projection], ignore_index=True)
    rebuilt = build_margin_feature_panel(
        parent_outcomes=poisoned,
        source_projection=poisoned_projection,
        source_coverage_daily=coverage,
    )
    pd.testing.assert_frame_equal(
        baseline[["decision_as_of_trade_date", "instrument", *MARGIN_MVE_EXPANDED_FEATURES]],
        rebuilt[["decision_as_of_trade_date", "instrument", *MARGIN_MVE_EXPANDED_FEATURES]],
    )


def test_feature_builder_rejects_future_or_misordered_lag_mapping() -> None:
    parent, projection, coverage = _feature_inputs()
    poisoned = coverage.copy()
    poisoned.loc[0, "source_date_d1"] = poisoned.loc[0, "decision_as_of_trade_date"]
    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_margin_feature_panel(
            parent_outcomes=parent,
            source_projection=projection,
            source_coverage_daily=poisoned,
        )
    assert caught.value.reason_code == "ADVISORY_N3_MARGIN_MVE_PIT_VIOLATION"


def test_feature_support_reports_exact_finite_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, projection, coverage = _feature_inputs()
    features = build_margin_feature_panel(
        parent_outcomes=parent,
        source_projection=projection,
        source_coverage_daily=coverage,
    )
    monkeypatch.setattr(pipeline, "MARGIN_MVE_MIN_DYNAMICS_FINITE_PER_DAY", 1)
    report = validate_margin_feature_support(features)
    assert report["support_sufficient"] is True
    assert set(report["dynamics_finite_fraction_on_available_source"]) == set(MARGIN_MVE_RANKED_DYNAMICS_FEATURES)


def test_crossfit_imputes_normal_missing_and_scores_every_row_seven_times() -> None:
    parent, projection, coverage = _feature_inputs()
    features = build_margin_feature_panel(
        parent_outcomes=parent,
        source_projection=projection,
        source_coverage_daily=coverage,
    )
    features.loc[0, list(MARGIN_MVE_EXPANDED_FEATURES[3:])] = np.nan
    features.loc[0, "outcome_known"] = False
    features.loc[0, "economic_net_excess_bps"] = np.nan
    dates = [value.date().isoformat() for value in sorted(features["decision_as_of_trade_date"].unique())]
    oof, diagnostics = run_margin_crossfit(features=features, paths=_paths(dates), request=_request())
    assert len(oof) == 80
    assert len(diagnostics) == 84
    for column in (
        "parent_comparator_oof_score",
        "membership_oof_score",
        "candidate_oof_score",
    ):
        assert oof[f"{column}_count"].eq(7).all()
        assert np.isfinite(oof[column]).all()


def _evaluation_panel(*, candidate_wins: bool) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    regimes: list[dict[str, object]] = []
    coverage: list[dict[str, object]] = []
    for day_index, day in enumerate(pd.bdate_range("2025-02-03", periods=8)):
        regimes.append({"decision_as_of_trade_date": day, "regime": "DOWN" if day_index % 2 else "UP_OR_FLAT"})
        coverage.append({"decision_as_of_trade_date": day, "instrument_count": 10})
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
                    "parent_comparator_oof_score": parent,
                    "membership_oof_score": parent,
                    "candidate_oof_score": candidate,
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(regimes), pd.DataFrame(coverage)


def test_evaluator_selects_candidate_only_when_three_baseline_gates_pass() -> None:
    scores, regimes, coverage = _evaluation_panel(candidate_wins=True)
    daily, summary, stability, frontier = evaluate_margin_models(
        oof_scores=scores,
        regime_daily=regimes,
        source_coverage_daily=coverage,
        feature_support={"support_sufficient": True},
        request=_request(),
    )
    assert len(daily) == 8
    assert all(value["support_sufficient"] for value in summary["intervention_support"].values())
    assert stability["positive_joint_time_block_count"] == 4
    assert summary["eligible"] is True
    assert summary["evidence_class"] == "EXPLORATORY_CANDIDATE_SELECTED"
    assert frontier["selected_trial_id"] == "N3_MARGIN_DYNAMICS_EXPANDED_V1"


def test_evaluator_routes_zero_with_baseline_scoped_support_reasons() -> None:
    scores, regimes, coverage = _evaluation_panel(candidate_wins=False)
    _, summary, _, frontier = evaluate_margin_models(
        oof_scores=scores,
        regime_daily=regimes,
        source_coverage_daily=coverage,
        feature_support={"support_sufficient": True},
        request=_request(),
    )
    assert summary["eligible"] is False
    assert "PARENT__INTERVENTION_DAY_COUNT_BELOW_MINIMUM" in summary["reason_codes"]
    assert "MEMBERSHIP__INTERVENTION_DAY_COUNT_BELOW_MINIMUM" in summary["reason_codes"]
    assert summary["support_sufficient"] is False
    assert summary["evidence_class"] == "EXPLORATORY_INSUFFICIENT_SUPPORT"
    assert frontier["selected_trial_id"] is None


def test_familywise_lower_uses_registered_one_sided_alpha(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[float] = []

    def fake_interval(values, *, block_length, repetitions, seed, alpha):  # noqa: ANN001
        observed.append(alpha)
        return 1.0, 2.0

    monkeypatch.setattr(pipeline, "_moving_block_interval", fake_interval)
    result = pipeline._metric_inference(
        [1.0, 2.0, 3.0],
        request=_request(),
        alpha=0.01,
        threshold=0.0,
        seed_offset=0,
    )
    assert observed == [0.05, 0.02]
    assert result["familywise_alpha"] == 0.01


def test_parent_parity_compares_top5_only_where_economic_label_is_evaluable() -> None:
    dates = pd.bdate_range("2025-04-14", periods=2)
    daily = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "parent_rank_ic": [0.1, 0.2],
            "parent_top5_net_excess_bps": [np.nan, 20.0],
            "parent_instruments": ["000001.SZ,000002.SZ,000003.SZ,000004.SZ,000005.SZ"] * 2,
        }
    )
    parent_top5 = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "top5_net_excess_bps": [10.0, 20.0],
            "instruments": ['["000001.SZ","000002.SZ","000003.SZ","000004.SZ","000005.SZ"]'] * 2,
        }
    )
    parent_signal = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "matured_rank_ic": [0.1, 0.2],
        }
    )
    _validate_parent_daily_parity(
        daily=daily,
        parent_top5_daily=parent_top5,
        parent_signal_daily=parent_signal,
    )
    parent_top5.loc[1, "top5_net_excess_bps"] = 21.0
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validate_parent_daily_parity(
            daily=daily,
            parent_top5_daily=parent_top5,
            parent_signal_daily=parent_signal,
        )
    assert caught.value.reason_code == "ADVISORY_N3_MARGIN_MVE_BASELINE_PARITY_FAILED"
