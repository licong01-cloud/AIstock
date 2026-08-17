from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import market_relative_jump_spike as jump_subject
from backend.services.hmm_risk import market_relative_ridge_candidate as subject
from scripts.hmm_risk import run_market_relative_ridge_candidate as cli


def _segment(start: date, end: date, count: int) -> list[date]:
    span = (end - start).days
    positions = np.linspace(0, span, count, dtype=int)
    assert len(set(positions.tolist())) == count
    result = [start + timedelta(days=int(position)) for position in positions]
    assert result[0] == start
    assert result[-1] == end
    return result


def _approved_calendar() -> list[date]:
    return [
        *_segment(date(2022, 1, 4), date(2023, 9, 1), 405),
        *_segment(date(2023, 9, 4), date(2024, 3, 14), 126),
        *_segment(date(2024, 3, 15), date(2024, 9, 18), 126),
        *_segment(date(2024, 9, 19), date(2025, 3, 31), 126),
    ]


def _panel(codes: list[str], calendar: list[date], *, level_name: str = "l1_code") -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    center = (len(codes) - 1) / 2.0
    for day_index, day in enumerate(calendar):
        for code_index, code in enumerate(codes):
            signal = code_index - center
            phase = 0.01 * day_index
            rows.append(
                {
                    "trade_date": pd.Timestamp(day),
                    level_name: code,
                    "daily_return": signal * 0.0002,
                    "excess_return_Nd": signal * 0.01 + phase * 0.001,
                    "net_mf_ratio": signal * 0.02 + phase * 0.002,
                    "elg_net_mf_ratio": signal * 0.03 - phase * 0.001,
                    "sf_excess_breadth_5d": signal * 0.04 + math.sin(phase) * 0.001,
                    "sf_turnover_pctile_120d_neg": signal * 0.05 + math.cos(phase) * 0.001,
                }
            )
    return pd.DataFrame(rows).set_index(["trade_date", level_name]).sort_index()


def _benchmark(calendar: list[date]) -> dict[date, float]:
    return {day: 0.0 for day in calendar}


def _request(commit: str = "a" * 40) -> dict[str, object]:
    return {
        "schema_version": subject.REQUEST_SCHEMA_VERSION,
        "contract_version": subject.CONTRACT_VERSION,
        "expected_producer_commit": commit,
        "holdout_start": subject.HOLDOUT_START.isoformat(),
        "holdout_end": subject.HOLDOUT_END.isoformat(),
        "forbidden_holdout_date_set_sha256": "b" * 64,
        "source": {
            "source_start": "2021-01-01",
            "source_end": subject.DEVELOPMENT_END.isoformat(),
        },
    }


def _p2_3c_request(commit: str = "a" * 40) -> dict[str, object]:
    return {
        "schema_version": subject.P2_3C_REQUEST_SCHEMA_VERSION,
        "contract_version": subject.P2_3C_CONTRACT_VERSION,
        "expected_producer_commit": commit,
        "candidate_attempt_index": subject.P2_3C_ATTEMPT_INDEX,
        "prior_not_available_report_sha256s": {
            "P2-3A": subject.P2_3A_NOT_AVAILABLE_REPORT_SHA256,
            "P2-3B": subject.P2_3B_NOT_AVAILABLE_REPORT_SHA256,
        },
        "fixed_market_parameters": {
            "jump_penalty": subject.P2_3C_FIXED_JUMP_PENALTY,
            "seed": subject.P2_3C_FIXED_SEED,
        },
        "holdout_start": subject.HOLDOUT_START.isoformat(),
        "holdout_end": subject.HOLDOUT_END.isoformat(),
        "forbidden_holdout_date_set_sha256": "b" * 64,
        "source": {
            "source_start": "2021-01-01",
            "source_end": subject.DEVELOPMENT_END.isoformat(),
        },
    }


def _top_level_inputs() -> dict[str, object]:
    calendar = _approved_calendar()
    return {
        "trading_dates": tuple(calendar),
        "dataset_manifest": {"calendar_benchmark": {"rows": [[day.isoformat(), 0.0] for day in calendar]}},
        "mapping_manifest": {"rows": []},
        "feature_definition": {"level": "L1"},
        "l2_feature_definition": {"level": "L2"},
        "database": {"host": "redacted", "port": 5432, "dbname": "dev"},
    }


def _stage_receipt(component: str, *, selected_alpha: float | None = None) -> dict[str, object]:
    body: dict[str, object] = {"component": component}
    if selected_alpha is not None:
        body["selected_alpha"] = selected_alpha
    return {**body, "receipt_sha256": subject.canonical_sha256(body)}


def _prepared_relative_component(values: np.ndarray) -> jump_subject.PreparedComponent:
    dates = tuple(date(2025, 1, 2) + timedelta(days=index) for index in range(values.shape[0]))
    preprocessor = jump_subject.Preprocessor(
        feature_names=subject.RELATIVE_FEATURES,
        lower=(0.0,) * len(subject.RELATIVE_FEATURES),
        upper=(1.0,) * len(subject.RELATIVE_FEATURES),
        mean=(0.0,) * len(subject.RELATIVE_FEATURES),
        std=(1.0,) * len(subject.RELATIVE_FEATURES),
        valid_row_count=values.shape[0],
        valid_identity_sha256="c" * 64,
    )
    sequence = jump_subject.SequenceData(
        key="sector",
        dates=dates,
        ordinals=tuple(range(values.shape[0])),
        values=np.asarray(values, dtype=np.float64),
    )
    return jump_subject.PreparedComponent(
        component="L1_ridge",
        level="L1",
        feature_names=subject.RELATIVE_FEATURES,
        expected_sector_count=1,
        minimum_daily_count=1,
        canonical_codes=("sector",),
        sequences=(sequence,),
        preprocessor=preprocessor,
        unavailable_items=(),
        valid_row_count=values.shape[0],
        valid_identity_sha256="d" * 64,
    )


def _prepared_market_component(start: date) -> jump_subject.PreparedComponent:
    dates = (start, start + timedelta(days=1))
    feature_count = len(jump_subject.MARKET_FEATURES)
    preprocessor = jump_subject.Preprocessor(
        feature_names=jump_subject.MARKET_FEATURES,
        lower=(0.0,) * feature_count,
        upper=(1.0,) * feature_count,
        mean=(0.0,) * feature_count,
        std=(1.0,) * feature_count,
        valid_row_count=2,
        valid_identity_sha256="e" * 64,
    )
    return jump_subject.PreparedComponent(
        component="market",
        level="L2",
        feature_names=jump_subject.MARKET_FEATURES,
        expected_sector_count=131,
        minimum_daily_count=118,
        canonical_codes=tuple(f"L2-{index:03d}" for index in range(131)),
        sequences=(
            jump_subject.SequenceData(
                key="market",
                dates=dates,
                ordinals=(0, 1),
                values=np.zeros((2, feature_count), dtype=np.float64),
            ),
        ),
        preprocessor=preprocessor,
        unavailable_items=(),
        valid_row_count=2,
        valid_identity_sha256="f" * 64,
    )


def test_target_is_daily_centered_and_purges_the_last_ten_dates() -> None:
    calendar = [date(2024, 1, 1) + timedelta(days=index) for index in range(15)]
    codes = [f"S{index}" for index in range(5)]
    panel = _panel(codes, calendar)

    result = subject.build_target_rows(
        panel,
        _benchmark(calendar),
        calendar,
        level="L1",
        start=calendar[0],
        end=calendar[-1],
        expected_days=15,
        expected_sector_count=5,
        minimum_daily_count=5,
    )

    assert result.eligible_dates == tuple(calendar[:5])
    assert result.receipt["excluded_tail_dates"] == [day.isoformat() for day in calendar[5:]]
    assert result.receipt["target_row_count"] == 25
    for day in result.eligible_dates:
        assert result.values[(codes[2], day)] == pytest.approx(0.0)
        assert result.values[(codes[-1], day)] > result.values[(codes[0], day)]


def test_target_denominator_failure_is_explicit_and_does_not_fill_values() -> None:
    calendar = [date(2024, 1, 1) + timedelta(days=index) for index in range(15)]
    codes = [f"S{index}" for index in range(5)]
    panel = _panel(codes, calendar)
    panel.loc[(pd.Timestamp(calendar[1]), "S3"), "daily_return"] = np.nan
    panel.loc[(pd.Timestamp(calendar[1]), "S4"), "daily_return"] = np.nan

    result = subject.build_target_rows(
        panel,
        _benchmark(calendar),
        calendar,
        level="L1",
        start=calendar[0],
        end=calendar[-1],
        expected_days=15,
        expected_sector_count=5,
        minimum_daily_count=4,
    )

    first = result.receipt["unavailable_dates"][0]
    assert first["reason_code"] == subject.REASON_TARGET_UNAVAILABLE
    assert first["available_count"] == 3
    assert not any(day == calendar[0] for _, day in result.values)


def test_ridge_fit_is_exact_deterministic_and_does_not_multiply_by_sector() -> None:
    calendar = [date(2024, 1, 1) + timedelta(days=index) for index in range(20)]
    codes = [f"S{index}" for index in range(10)]
    panel = _panel(codes, calendar)
    component = subject.prepare_component(
        panel,
        component="L1_ridge",
        level="L1",
        feature_names=subject.RELATIVE_FEATURES,
        calendar=calendar,
        start=calendar[0],
        end=calendar[-1],
        expected_days=20,
        expected_sector_count=10,
        minimum_daily_count=10,
        relative=True,
    )
    targets = subject.build_target_rows(
        panel,
        _benchmark(calendar),
        calendar,
        level="L1",
        start=calendar[0],
        end=calendar[-1],
        expected_days=20,
        expected_sector_count=10,
        minimum_daily_count=10,
    )
    attempts: list[dict[str, object]] = []
    first = subject._fit_ridge(
        component,
        targets,
        alpha=1.0,
        attempt_log=attempts,
        context={"component": "L1", "fold": "unit", "phase": "selection"},
    )
    second = subject._fit_ridge(
        component,
        targets,
        alpha=1.0,
        attempt_log=attempts,
        context={"component": "L1", "fold": "unit-repeat", "phase": "selection"},
    )

    assert len(attempts) == 2
    assert np.array_equal(first.coefficient, second.coefficient)
    assert first.intercept == second.intercept
    receipt = subject._fit_receipt(first)
    assert receipt["solver"] == "svd"
    assert receipt["fit_intercept"] is True
    assert receipt["random_state"] is None


def test_market_conditioning_builds_exact_ten_dimensions_and_slope_identity() -> None:
    base = np.asarray(
        [
            [1.0, 2.0, 3.0, 4.0, 5.0],
            [6.0, 7.0, 8.0, 9.0, 10.0],
        ],
        dtype=np.float64,
    )
    component = _prepared_relative_component(base)
    dates = component.sequences[0].dates
    conditioned = subject._condition_component(
        component,
        {dates[0]: "risk_on", dates[1]: "risk_off"},
        fold="unit",
        phase="validation",
    )

    values = conditioned.component.sequences[0].values
    assert isinstance(conditioned.component, subject.ConditionedFeatureComponent)
    assert not hasattr(conditioned.component, "preprocessor")
    assert conditioned.component.feature_names == subject.P2_3C_FEATURES
    assert values.shape == (2, 10)
    assert np.array_equal(values[0], np.concatenate([base[0], base[0]]))
    assert np.array_equal(values[1], np.concatenate([base[1], -base[1]]))
    assert "market_sign" not in conditioned.component.feature_names
    assert conditioned.receipt["dtype"] == "float64_le"

    coefficient = np.arange(1.0, 11.0, dtype="<f8")
    fit = subject.RidgeFit(
        alpha=1.0,
        coefficient=coefficient,
        intercept=0.0,
        row_count=2,
        feature_count=10,
        training_identity_sha256="a" * 64,
    )
    receipt = subject._conditioned_fit_receipt(fit)
    beta = coefficient[:5]
    gamma = coefficient[5:]
    assert receipt["risk_on_slope"] == pytest.approx((beta + gamma).tolist())
    assert receipt["risk_off_slope"] == pytest.approx((beta - gamma).tolist())


def test_market_conditioning_identity_and_non_finite_fail_typed() -> None:
    component = _prepared_relative_component(np.ones((2, 5), dtype=np.float64))
    dates = component.sequences[0].dates
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject._condition_component(
            component,
            {dates[0]: "risk_on"},
            fold="unit",
            phase="validation",
        )
    assert captured.value.reason_code == subject.REASON_MARKET_IDENTITY

    invalid = _prepared_relative_component(np.asarray([[1.0] * 5, [1.0, 1.0, np.nan, 1.0, 1.0]]))
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject._condition_component(
            invalid,
            {dates[0]: "risk_on", dates[1]: "risk_off"},
            fold="unit",
            phase="validation",
        )
    assert captured.value.reason_code == subject.REASON_INTERACTION_NON_FINITE


def test_market_conditioning_fold_uses_fixed_fit_and_separate_causal_segments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    train = _prepared_market_component(date(2022, 1, 4))
    validation = _prepared_market_component(date(2023, 9, 4))
    prepared = iter((train, validation))
    monkeypatch.setattr(subject, "_component_panel", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(subject, "prepare_component", lambda *args, **kwargs: next(prepared))
    fit_calls: list[tuple[float, int]] = []
    centers = np.zeros((2, len(jump_subject.MARKET_FEATURES)), dtype=np.float64)
    centers[0, jump_subject.MARKET_FEATURES.index("daily_return")] = -1.0
    centers[1, jump_subject.MARKET_FEATURES.index("daily_return")] = 1.0

    def fake_fit(*args: object, **kwargs: object) -> jump_subject.JumpFit:
        fit_calls.append((float(kwargs["jump_penalty"]), int(kwargs["seed"])))
        return jump_subject.JumpFit(
            centers=centers,
            paths=(np.asarray([0, 1], dtype=np.int64),),
            objective=1.0,
            normalized_objective=0.1,
            iterations=1,
            seed=int(kwargs["seed"]),
            jump_penalty=float(kwargs["jump_penalty"]),
            row_count=2,
            feature_count=len(jump_subject.MARKET_FEATURES),
        )

    causal_components: list[jump_subject.PreparedComponent] = []

    def fake_causal(component: jump_subject.PreparedComponent, *args: object) -> tuple[np.ndarray, ...]:
        causal_components.append(component)
        return (np.asarray([0, 1], dtype=np.int64),)

    monkeypatch.setattr(subject, "fit_jump_model", fake_fit)
    monkeypatch.setattr(subject, "causal_states", fake_causal)
    monkeypatch.setattr(
        subject,
        "market_fold_metrics",
        lambda *args, **kwargs: {"metric_valid": True, "receipt_sha256": "1" * 64},
    )
    attempts: list[dict[str, object]] = []
    result = subject._market_fold_conditioning(
        {"l2_panel": pd.DataFrame()},
        calendar=tuple(_approved_calendar()),
        benchmark=_benchmark(_approved_calendar()),
        fold=subject.FOLDS[0],
        attempt_log=attempts,
    )

    assert fit_calls == [(4.0, 42)]
    assert len(causal_components) == 2
    assert causal_components[0] is train
    assert causal_components[1] is validation
    assert len(attempts) == 1
    assert result.receipt["train_states"]["arrival_cost_policy"] == "zero_at_each_segment_start_no_train_carry"
    assert result.receipt["validation_states"]["arrival_cost_policy"] == "zero_at_each_segment_start_no_train_carry"
    assert result.receipt["train_states"]["state_rows"] == [
        [train.sequences[0].dates[0].isoformat(), "risk_off"],
        [train.sequences[0].dates[1].isoformat(), "risk_on"],
    ]
    assert result.receipt["centers"] == centers.tolist()
    assert len(result.receipt["centers_sha256"]) == 64
    assert result.receipt["validation_states"]["transition_counts_used_for_acceptance"] is False


def test_market_conditioning_missing_regime_fails_without_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    train = _prepared_market_component(date(2022, 1, 4))
    validation = _prepared_market_component(date(2023, 9, 4))
    prepared = iter((train, validation))
    monkeypatch.setattr(subject, "_component_panel", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(subject, "prepare_component", lambda *args, **kwargs: next(prepared))
    centers = np.zeros((2, len(jump_subject.MARKET_FEATURES)), dtype=np.float64)
    centers[0, jump_subject.MARKET_FEATURES.index("daily_return")] = -1.0
    centers[1, jump_subject.MARKET_FEATURES.index("daily_return")] = 1.0
    monkeypatch.setattr(
        subject,
        "fit_jump_model",
        lambda *args, **kwargs: jump_subject.JumpFit(
            centers=centers,
            paths=(np.asarray([0, 0], dtype=np.int64),),
            objective=1.0,
            normalized_objective=0.1,
            iterations=1,
            seed=42,
            jump_penalty=4.0,
            row_count=2,
            feature_count=len(jump_subject.MARKET_FEATURES),
        ),
    )
    monkeypatch.setattr(subject, "causal_states", lambda *args, **kwargs: (np.asarray([0, 0]),))
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject._market_fold_conditioning(
            {"l2_panel": pd.DataFrame()},
            calendar=tuple(_approved_calendar()),
            benchmark=_benchmark(_approved_calendar()),
            fold=subject.FOLDS[0],
            attempt_log=[],
        )
    assert captured.value.reason_code == subject.REASON_MARKET_REGIME


def test_market_development_acceptance_failure_stops_after_three_fixed_fits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[dict[str, object]] = []

    def fake_fold(*args: object, **kwargs: object) -> subject.MarketConditioningFold:
        fold = str(kwargs["fold"]["fold"])
        kwargs["attempt_log"].append({"component": "market", "fold": fold})
        receipt = _stage_receipt(fold)
        return subject.MarketConditioningFold(
            fold=fold,
            train_states={date(2024, 1, 1): "risk_on"},
            validation_states={date(2024, 1, 2): "risk_off"},
            receipt=receipt,
        )

    monkeypatch.setattr(subject, "_market_fold_conditioning", fake_fold)
    monkeypatch.setattr(
        subject,
        "market_development_score",
        lambda receipts: {
            "lambda_eligible": False,
            "market_selection_reason_code": "hmm_risk_jump_selection_metric_unavailable",
        },
    )
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject._run_market_conditioning_development(
            {}, calendar=tuple(_approved_calendar()), benchmark={}, attempt_log=attempts
        )
    assert len(attempts) == 3
    assert captured.value.stage == "market_development_acceptance"


def test_state_projection_keeps_exact_extremes_and_rejects_boundary_tie() -> None:
    day = date(2025, 1, 2)
    scores = {(f"S{index:02d}", day): float(index) for index in range(11)}
    states, receipt = subject.project_daily_states(scores, level="L1", minimum_daily_count=10)
    assert len([value for value in states.values() if value == "fading"]) == 5
    assert len([value for value in states.values() if value == "trending"]) == 5
    assert len([value for value in states.values() if value == "neutral"]) == 1
    assert receipt["unavailable_date_count"] == 0

    tied = dict(scores)
    tied[("S05", day)] = tied[("S04", day)]
    states, receipt = subject.project_daily_states(tied, level="L1", minimum_daily_count=10)
    assert states == {}
    assert receipt["unavailable_dates"][0]["reason_code"] == subject.REASON_STATE_TIE
    assert receipt["unavailable_dates"][0]["fading_count"] == 4


def test_fold_metrics_use_continuous_score_and_forecast_state_product_oracle() -> None:
    days = tuple(date(2025, 1, 1) + timedelta(days=index) for index in range(5))
    codes = [f"S{index:02d}" for index in range(10)]
    scores = {(code, day): float(index) for day in days for index, code in enumerate(codes)}
    values = {(code, day): float(index - 4.5) for day in days for index, code in enumerate(codes)}
    states, _ = subject.project_daily_states(scores, level="L1", minimum_daily_count=10)
    targets = subject.TargetRows(
        level="L1", start=days[0], end=days[-1], eligible_dates=days, values=values, receipt={}
    )

    result = subject.fold_metrics(scores, targets, states)

    assert result["metric_valid"] is True
    assert result["rank_ic_available_date_count"] == 5
    assert result["spread_available_date_count"] == 5
    assert result["mean_rank_ic"] == pytest.approx(1.0)
    assert result["mean_spread"] > 0.0


def test_alpha_selection_uses_rank_ic_then_spread_then_larger_alpha() -> None:
    receipts = [
        {"alpha": 0.1, "alpha_eligible": True, "median_rank_ic": 0.10, "median_spread": 0.0200},
        {"alpha": 1.0, "alpha_eligible": True, "median_rank_ic": 0.10005, "median_spread": 0.02005},
        {"alpha": 10.0, "alpha_eligible": True, "median_rank_ic": 0.10004, "median_spread": 0.02004},
    ]
    assert subject._select_alpha(receipts)["alpha"] == 10.0

    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject._select_alpha([{**receipts[0], "alpha_eligible": False}])
    assert captured.value.reason_code == subject.REASON_SELECTION_UNAVAILABLE


def test_development_effect_must_be_strictly_positive_and_score_shape_fails_typed() -> None:
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject._require_positive_development(
            "L1",
            {"alpha": 1.0, "median_rank_ic": 0.01, "median_spread": 0.0},
        )
    assert captured.value.reason_code == subject.REASON_DEVELOPMENT_NON_POSITIVE

    fit = subject.RidgeFit(
        alpha=1.0,
        coefficient=np.ones(5, dtype="<f8"),
        intercept=0.0,
        row_count=10,
        feature_count=5,
        training_identity_sha256="a" * 64,
    )
    with pytest.raises(subject.RidgeCandidateError) as captured:
        fit.predict(np.ones((2, 4), dtype="<f8"))
    assert captured.value.reason_code == subject.REASON_SCORE_NON_FINITE


def test_real_l1_level_runs_exact_sixteen_fits_and_selects_positive_candidate() -> None:
    calendar = _approved_calendar()
    panel = _panel([f"L1-{index:02d}" for index in range(31)], calendar)
    attempts: list[dict[str, object]] = []

    result = subject._run_level(
        "L1",
        inputs={"panel": panel},
        calendar=tuple(calendar),
        benchmark=_benchmark(calendar),
        attempt_log=attempts,
    )

    assert len(attempts) == 16
    assert result["component"] == "L1"
    assert result["selected_median_rank_ic"] > 0.0
    assert result["selected_median_spread"] > 0.0
    assert result["holdout_accessed"] is False
    assert result["final_target"]["excluded_tail_dates"] == [day.isoformat() for day in calendar[-10:]]


def test_real_conditioned_l1_runs_all_fifteen_folds_before_train_only_selection() -> None:
    calendar = _approved_calendar()
    panel = _panel([f"L1-{index:02d}" for index in range(31)], calendar)
    market_folds: list[subject.MarketConditioningFold] = []
    for fold in subject.FOLDS:
        train_dates = [day for day in calendar if fold["train_start"] <= day <= fold["train_end"]]
        validation_dates = [day for day in calendar if fold["validation_start"] <= day <= fold["validation_end"]]
        train_states = {day: "risk_on" if index % 2 else "risk_off" for index, day in enumerate(train_dates)}
        validation_states = {day: "risk_on" if index % 2 else "risk_off" for index, day in enumerate(validation_dates)}
        market_folds.append(
            subject.MarketConditioningFold(
                fold=str(fold["fold"]),
                train_states=train_states,
                validation_states=validation_states,
                receipt=_stage_receipt(str(fold["fold"])),
            )
        )
    attempts: list[dict[str, object]] = []

    result = subject._run_conditioned_level_development(
        "L1",
        inputs={"panel": panel},
        calendar=tuple(calendar),
        benchmark=_benchmark(calendar),
        market_folds=market_folds,
        attempt_log=attempts,
    )

    assert len(attempts) == 15
    assert result["conditioned_feature_names"] == list(subject.P2_3C_FEATURES)
    assert len(result["alpha_receipts"]) == 5
    assert all(item["fold_count"] == 3 for item in result["alpha_receipts"])
    assert result["selected_median_rank_ic"] > 0.0
    assert result["selected_median_spread"] > 0.0
    assert all(item["regime_split_used_for_selection"] is False for item in result["alpha_receipts"])


def test_conditioned_alpha_selection_failure_preserves_all_fold_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _approved_calendar()
    panel = _panel([f"L1-{index:02d}" for index in range(31)], calendar)
    market_folds: list[subject.MarketConditioningFold] = []
    for fold in subject.FOLDS:
        train_dates = [day for day in calendar if fold["train_start"] <= day <= fold["train_end"]]
        validation_dates = [day for day in calendar if fold["validation_start"] <= day <= fold["validation_end"]]
        market_folds.append(
            subject.MarketConditioningFold(
                fold=str(fold["fold"]),
                train_states={day: "risk_on" if index % 2 else "risk_off" for index, day in enumerate(train_dates)},
                validation_states={
                    day: "risk_on" if index % 2 else "risk_off" for index, day in enumerate(validation_dates)
                },
                receipt=_stage_receipt(str(fold["fold"])),
            )
        )
    monkeypatch.setattr(
        subject,
        "_select_alpha",
        lambda receipts: (_ for _ in ()).throw(
            subject.RidgeCandidateError(
                subject.REASON_SELECTION_UNAVAILABLE,
                "synthetic selection failure",
                stage="alpha_selection",
            )
        ),
    )
    attempts: list[dict[str, object]] = []

    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject._run_conditioned_level_development(
            "L1",
            inputs={"panel": panel},
            calendar=tuple(calendar),
            benchmark=_benchmark(calendar),
            market_folds=market_folds,
            attempt_log=attempts,
        )

    assert captured.value.reason_code == subject.REASON_SELECTION_UNAVAILABLE
    assert len(attempts) == 15
    receipts = captured.value.evidence["alpha_receipts"]
    assert len(receipts) == 5
    assert all(item["fold_count"] == 3 for item in receipts)
    assert captured.value.evidence["alpha_receipts_sha256"] == subject.canonical_sha256(receipts)


def test_top_level_requires_exact_184_attempts_and_has_zero_side_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    calendar = _approved_calendar()
    inputs = {
        "trading_dates": tuple(calendar),
        "dataset_manifest": {"calendar_benchmark": {"rows": [[day.isoformat(), 0.0] for day in calendar]}},
        "mapping_manifest": {"rows": []},
        "feature_definition": {"level": "L1"},
        "l2_feature_definition": {"level": "L2"},
        "database": {"host": "redacted", "port": 5432, "dbname": "dev"},
    }

    def fake_market(*args: object, **kwargs: object) -> dict[str, object]:
        attempts = kwargs["attempt_log"]
        assert isinstance(attempts, list)
        attempts.extend({"component": "market", "attempt": index} for index in range(152))
        body = {"component": "market"}
        return {**body, "receipt_sha256": subject.canonical_sha256(body)}

    def fake_level(level: str, **kwargs: object) -> dict[str, object]:
        attempts = kwargs["attempt_log"]
        assert isinstance(attempts, list)
        attempts.extend({"component": level, "attempt": index} for index in range(16))
        body = {"component": level}
        return {**body, "receipt_sha256": subject.canonical_sha256(body)}

    monkeypatch.setattr(subject, "run_market_component", fake_market)
    monkeypatch.setattr(subject, "_run_level", fake_level)
    report = subject.run_p2_3b_candidate(inputs, _request(), producer_commit="a" * 40)

    assert subject.planned_fit_count() == 184
    assert report["completed_fit_count"] == 184
    assert report["component_count"] == 3
    assert report["status"] == "P2_3B_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE"
    assert report["holdout_accessed"] is False
    assert report["product_acceptance_performed"] is False
    assert report["model_write"] is False
    assert report["ready_write"] is False
    assert report["database_write"] is False
    assert report["runtime_action"] is False


def test_top_level_rejects_holdout_before_any_fit(monkeypatch: pytest.MonkeyPatch) -> None:
    calendar = [*_approved_calendar(), subject.HOLDOUT_START]
    inputs = {
        "trading_dates": tuple(calendar),
        "dataset_manifest": {"calendar_benchmark": {"rows": [[day.isoformat(), 0.0] for day in calendar]}},
        "mapping_manifest": {"rows": []},
    }
    monkeypatch.setattr(
        subject,
        "run_market_component",
        lambda *args, **kwargs: pytest.fail("market fit must not run when holdout is present"),
    )
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject.run_p2_3b_candidate(inputs, _request(), producer_commit="a" * 40)
    assert captured.value.reason_code == subject.REASON_HOLDOUT


def test_top_level_preserves_completed_components_and_finalization_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _approved_calendar()
    inputs = {
        "trading_dates": tuple(calendar),
        "dataset_manifest": {"calendar_benchmark": {"rows": [[day.isoformat(), 0.0] for day in calendar]}},
        "mapping_manifest": {"rows": []},
    }

    def fake_market(*args: object, **kwargs: object) -> dict[str, object]:
        attempts = kwargs["attempt_log"]
        assert isinstance(attempts, list)
        attempts.extend({"component": "market", "attempt": index} for index in range(152))
        body = {"component": "market"}
        return {**body, "receipt_sha256": subject.canonical_sha256(body)}

    def fail_level(level: str, **kwargs: object) -> dict[str, object]:
        attempts = kwargs["attempt_log"]
        assert isinstance(attempts, list)
        attempts.append({"component": level, "status": "fit_completed"})
        raise subject.RidgeCandidateError(
            subject.REASON_DEVELOPMENT_NON_POSITIVE,
            "synthetic level failure",
            stage="development_acceptance",
        )

    monkeypatch.setattr(subject, "run_market_component", fake_market)
    monkeypatch.setattr(subject, "_run_level", fail_level)
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject.run_p2_3b_candidate(inputs, _request(), producer_commit="a" * 40)
    evidence = captured.value.evidence
    assert evidence["completed_fit_count"] == 153
    assert evidence["completed_component_count"] == 1
    assert evidence["completed_components"][0]["component"] == "market"
    report = subject.failure_report(
        _request(),
        producer_commit="a" * 40,
        error=captured.value,
        completed_fit_count=153,
    )
    assert report["selection_performed"] is False
    assert report["partial_component_selection_performed"] is True

    def complete_level(level: str, **kwargs: object) -> dict[str, object]:
        attempts = kwargs["attempt_log"]
        assert isinstance(attempts, list)
        attempts.extend({"component": level, "attempt": index} for index in range(16))
        body = {"component": level}
        return {**body, "receipt_sha256": subject.canonical_sha256(body)}

    monkeypatch.setattr(subject, "_run_level", complete_level)
    monkeypatch.setattr(subject, "_runtime_versions", lambda: (_ for _ in ()).throw(RuntimeError("receipt failure")))
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject.run_p2_3b_candidate(inputs, _request(), producer_commit="a" * 40)
    assert captured.value.reason_code == subject.REASON_UNEXPECTED
    assert captured.value.stage == "finalization"
    assert captured.value.evidence["completed_fit_count"] == 184
    assert captured.value.evidence["completed_component_count"] == 3
    failure = subject.failure_report(
        _request(),
        producer_commit="a" * 40,
        error=captured.value,
        completed_fit_count=184,
    )
    assert failure["runtime_versions"]["status"] == "unavailable"
    assert failure["runtime_versions"]["stage"] == "runtime_version_receipt"


@pytest.mark.parametrize(
    ("failure_stage", "expected_fit_count", "expected_component_count", "expected_partial_selection"),
    (("market", 3, 0, False), ("L1", 18, 1, True), ("L2", 33, 2, True)),
)
def test_p2_3c_stops_at_exact_market_l1_l2_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    expected_fit_count: int,
    expected_component_count: int,
    expected_partial_selection: bool,
) -> None:
    def fake_market(
        *args: object, **kwargs: object
    ) -> tuple[tuple[subject.MarketConditioningFold, ...], dict[str, object]]:
        attempts = kwargs["attempt_log"]
        assert isinstance(attempts, list)
        attempts.extend({"component": "market", "attempt": index} for index in range(3))
        if failure_stage == "market":
            raise subject.RidgeCandidateError(
                "hmm_risk_jump_selection_metric_unavailable",
                "market development failed",
                stage="market_development_acceptance",
            )
        return (), _stage_receipt("market-development")

    def fake_level(level: str, **kwargs: object) -> dict[str, object]:
        attempts = kwargs["attempt_log"]
        assert isinstance(attempts, list)
        attempts.extend({"component": level, "attempt": index} for index in range(15))
        if failure_stage == level:
            raise subject.RidgeCandidateError(
                subject.REASON_DEVELOPMENT_NON_POSITIVE,
                f"{level} development failed",
                stage="development_acceptance",
                evidence={"selected_alpha": 100.0},
            )
        return _stage_receipt(f"{level}-development", selected_alpha=100.0)

    monkeypatch.setattr(subject, "_run_market_conditioning_development", fake_market)
    monkeypatch.setattr(subject, "_run_conditioned_level_development", fake_level)
    monkeypatch.setattr(
        subject,
        "_run_market_conditioning_final",
        lambda *args, **kwargs: pytest.fail("final fits must not run after development failure"),
    )
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject.run_p2_3c_candidate(_top_level_inputs(), _p2_3c_request(), producer_commit="a" * 40)
    assert captured.value.evidence["completed_fit_count"] == expected_fit_count
    assert captured.value.evidence["completed_component_count"] == expected_component_count
    failure = subject.failure_report(
        _p2_3c_request(),
        producer_commit="a" * 40,
        error=captured.value,
        completed_fit_count=expected_fit_count,
        candidate_mode="p2-3c",
    )
    assert failure["partial_component_selection_performed"] is expected_partial_selection


def test_p2_3c_selection_unavailable_receipts_do_not_claim_partial_selection() -> None:
    error = subject.RidgeCandidateError(
        subject.REASON_SELECTION_UNAVAILABLE,
        "no eligible alpha",
        stage="alpha_selection",
        evidence={"alpha_receipts": [{"alpha": 0.1, "alpha_eligible": False}]},
    )
    report = subject.failure_report(
        _p2_3c_request(),
        producer_commit="a" * 40,
        error=error,
        completed_fit_count=18,
        candidate_mode="p2-3c",
    )

    assert report["partial_component_selection_performed"] is False


def test_p2_3c_success_requires_exact_thirty_six_attempts_and_writes_no_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_market(
        *args: object, **kwargs: object
    ) -> tuple[tuple[subject.MarketConditioningFold, ...], dict[str, object]]:
        attempts = kwargs["attempt_log"]
        attempts.extend({"component": "market", "attempt": index} for index in range(3))
        return (), _stage_receipt("market-development")

    def fake_level(level: str, **kwargs: object) -> dict[str, object]:
        attempts = kwargs["attempt_log"]
        attempts.extend({"component": level, "attempt": index} for index in range(15))
        return _stage_receipt(f"{level}-development", selected_alpha=100.0)

    def fake_market_final(*args: object, **kwargs: object) -> tuple[dict[date, str], dict[str, object]]:
        kwargs["attempt_log"].append({"component": "market", "phase": "final"})
        return {date(2025, 1, 2): "risk_on"}, _stage_receipt("market-final")

    def fake_level_final(level: str, **kwargs: object) -> dict[str, object]:
        kwargs["attempt_log"].append({"component": level, "phase": "final"})
        return _stage_receipt(f"{level}-final")

    monkeypatch.setattr(subject, "_run_market_conditioning_development", fake_market)
    monkeypatch.setattr(subject, "_run_conditioned_level_development", fake_level)
    monkeypatch.setattr(subject, "_run_market_conditioning_final", fake_market_final)
    monkeypatch.setattr(subject, "_run_conditioned_level_final", fake_level_final)
    monkeypatch.setattr(subject, "_runtime_versions", lambda: {"test": True})

    report = subject.run_p2_3c_candidate(_top_level_inputs(), _p2_3c_request(), producer_commit="a" * 40)

    assert subject.p2_3c_planned_fit_count() == 36
    assert report["completed_fit_count"] == 36
    assert report["component_count"] == 6
    assert report["status"] == "P2_3C_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE"
    assert report["candidate_attempt_index"] == 3
    assert report["fixed_market_parameters"] == {"jump_penalty": 4.0, "seed": 42}
    assert report["regime_split_used_for_selection"] is False
    assert report["holdout_accessed"] is False
    assert report["product_acceptance_performed"] is False
    assert report["model_write"] is False
    assert report["ready_write"] is False
    assert report["database_write"] is False
    assert report["runtime_action"] is False


def test_p2_3c_finalization_failure_preserves_all_completed_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_market(
        *args: object, **kwargs: object
    ) -> tuple[tuple[subject.MarketConditioningFold, ...], dict[str, object]]:
        kwargs["attempt_log"].extend({"component": "market", "attempt": index} for index in range(3))
        return (), _stage_receipt("market-development")

    def fake_level(level: str, **kwargs: object) -> dict[str, object]:
        kwargs["attempt_log"].extend({"component": level, "attempt": index} for index in range(15))
        return _stage_receipt(f"{level}-development", selected_alpha=100.0)

    def fake_market_final(*args: object, **kwargs: object) -> tuple[dict[date, str], dict[str, object]]:
        kwargs["attempt_log"].append({"component": "market", "phase": "final"})
        return {date(2025, 1, 2): "risk_on"}, _stage_receipt("market-final")

    def fake_level_final(level: str, **kwargs: object) -> dict[str, object]:
        kwargs["attempt_log"].append({"component": level, "phase": "final"})
        return _stage_receipt(f"{level}-final")

    monkeypatch.setattr(subject, "_run_market_conditioning_development", fake_market)
    monkeypatch.setattr(subject, "_run_conditioned_level_development", fake_level)
    monkeypatch.setattr(subject, "_run_market_conditioning_final", fake_market_final)
    monkeypatch.setattr(subject, "_run_conditioned_level_final", fake_level_final)
    monkeypatch.setattr(subject, "_runtime_versions", lambda: (_ for _ in ()).throw(RuntimeError("receipt failure")))

    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject.run_p2_3c_candidate(_top_level_inputs(), _p2_3c_request(), producer_commit="a" * 40)
    assert captured.value.reason_code == subject.REASON_UNEXPECTED
    assert captured.value.stage == "finalization"
    assert captured.value.evidence["completed_fit_count"] == 36
    assert captured.value.evidence["completed_component_count"] == 6
    failure = subject.failure_report(
        _p2_3c_request(),
        producer_commit="a" * 40,
        error=captured.value,
        completed_fit_count=36,
        candidate_mode="p2-3c",
    )
    assert failure["runtime_versions"]["status"] == "unavailable"
    assert failure["planned_fit_count"] == 36


def test_p2_3c_rejects_prior_identity_and_holdout_before_any_fit(monkeypatch: pytest.MonkeyPatch) -> None:
    request = _p2_3c_request()
    request["prior_not_available_report_sha256s"] = {"P2-3A": "0" * 64, "P2-3B": "1" * 64}
    monkeypatch.setattr(
        subject,
        "_run_market_conditioning_development",
        lambda *args, **kwargs: pytest.fail("fit must not run for a bad prior identity"),
    )
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject.run_p2_3c_candidate(_top_level_inputs(), request, producer_commit="a" * 40)
    assert captured.value.reason_code == subject.REASON_INPUT_IDENTITY

    request = _p2_3c_request()
    inputs = _top_level_inputs()
    inputs["trading_dates"] = (*inputs["trading_dates"], subject.HOLDOUT_START)
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject.run_p2_3c_candidate(inputs, request, producer_commit="a" * 40)
    assert captured.value.reason_code == subject.REASON_HOLDOUT


def test_report_write_is_external_immutable_and_readable(tmp_path: Path) -> None:
    target = tmp_path / "candidate.json"
    body = {"status": "unit", "database_write": False, "runtime_action": False}
    report = {**body, "report_sha256": subject.canonical_sha256(body)}

    subject.preflight_output_path(target, repository_root=Path(__file__).resolve().parents[3])
    subject.write_report(target, report, repository_root=Path(__file__).resolve().parents[3])
    assert json.loads(target.read_text(encoding="utf-8")) == report
    subject.write_report(target, report, repository_root=Path(__file__).resolve().parents[3])
    with pytest.raises(subject.RidgeCandidateError) as captured:
        subject.write_report(target, {**report, "status": "drift"}, repository_root=Path(__file__).resolve().parents[3])
    assert captured.value.reason_code == subject.REASON_COLLISION


def test_cli_has_no_defaults_and_writes_typed_failure_sibling(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        cli.main([])
    request = tmp_path / "invalid-request.json"
    request.write_text("{not-json", encoding="utf-8")
    output = tmp_path / "candidate.json"

    result = cli.main(
        [
            "--candidate-mode",
            "p2-3b",
            "--request",
            str(request),
            "--output",
            str(output),
            "--db-env-prefix",
            "UNUSED_FOR_INVALID_REQUEST",
        ]
    )

    assert result == 1
    assert not output.exists()
    failure = json.loads((tmp_path / "candidate.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_INPUT_IDENTITY
    assert failure["failure_receipt_write"] is True
    assert failure["model_write"] is False
    assert failure["database_write"] is False
    assert failure["runtime_action"] is False


def test_cli_rejects_holdout_source_before_database_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    request["source"] = {"source_start": "2021-01-01", "source_end": subject.HOLDOUT_START.isoformat()}
    request_path = tmp_path / "holdout-request.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(
        cli,
        "_load_l1_source_inputs",
        lambda *args, **kwargs: pytest.fail("database loader must not run for a holdout source"),
    )
    output = tmp_path / "holdout-candidate.json"

    result = cli.main(
        [
            "--candidate-mode",
            "p2-3b",
            "--request",
            str(request_path),
            "--output",
            str(output),
            "--db-env-prefix",
            "UNUSED",
        ]
    )

    assert result == 1
    failure = json.loads((tmp_path / "holdout-candidate.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_HOLDOUT


def test_cli_p2_3c_mode_keeps_distinct_request_and_failure_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _p2_3c_request()
    request["source"] = {"source_start": "2021-01-01", "source_end": subject.HOLDOUT_START.isoformat()}
    request_path = tmp_path / "p2-3c-request.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(
        cli,
        "_load_l1_source_inputs",
        lambda *args, **kwargs: pytest.fail("database loader must not run for a holdout source"),
    )
    output = tmp_path / "p2-3c-candidate.json"

    result = cli.main(
        [
            "--candidate-mode",
            "p2-3c",
            "--request",
            str(request_path),
            "--output",
            str(output),
            "--db-env-prefix",
            "UNUSED",
        ]
    )

    assert result == 1
    failure = json.loads((tmp_path / "p2-3c-candidate.failure.json").read_text(encoding="utf-8"))
    assert failure["schema_version"] == subject.P2_3C_REPORT_SCHEMA_VERSION
    assert failure["contract_version"] == subject.P2_3C_CONTRACT_VERSION
    assert failure["planned_fit_count"] == 36
    assert failure["failure_reason_code"] == subject.REASON_HOLDOUT
    assert failure["model_write"] is False


def test_cli_p2_3c_rejects_fixed_identity_before_database_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _p2_3c_request()
    request["fixed_market_parameters"] = {"jump_penalty": 8.0, "seed": 49}
    request_path = tmp_path / "p2-3c-drift.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(
        cli,
        "_load_l1_source_inputs",
        lambda *args, **kwargs: pytest.fail("database loader must not run for a drifted fixed identity"),
    )
    output = tmp_path / "p2-3c-drift-candidate.json"

    result = cli.main(
        [
            "--candidate-mode",
            "p2-3c",
            "--request",
            str(request_path),
            "--output",
            str(output),
            "--db-env-prefix",
            "UNUSED",
        ]
    )

    assert result == 1
    failure = json.loads((tmp_path / "p2-3c-drift-candidate.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_INPUT_IDENTITY
    assert failure["completed_fit_count"] == 0
