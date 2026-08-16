from __future__ import annotations

import itertools
import json
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import market_relative_jump_spike as subject
from scripts.hmm_risk import run_market_relative_jump_spike as cli


def _panel(codes: list[str], calendar: list[date], features: tuple[str, ...]) -> pd.DataFrame:
    rows = []
    for day_index, day in enumerate(calendar):
        for code_index, code in enumerate(codes):
            phase = day_index * 0.17 + code_index * 0.31
            row = {
                "trade_date": pd.Timestamp(day),
                "l1_code": code,
                "daily_return": 0.001 * (code_index - len(codes) / 2) + 0.0001 * day_index,
            }
            values = {
                "daily_return": row["daily_return"],
                "volatility_Nd": 0.01 + abs(math.sin(phase)) * 0.02,
                "net_mf_ratio": math.sin(phase) * 0.1,
                "sf_breadth_5d": 0.5 + math.cos(phase) * 0.2,
                "sf_dispersion_5d_neg": -0.01 - 0.001 * code_index - 0.00001 * day_index,
                "excess_return_Nd": (code_index - len(codes) / 2) * 0.002 + 0.0001 * day_index,
                "elg_net_mf_ratio": math.cos(phase) * 0.08,
                "sf_excess_breadth_5d": math.sin(phase) * 0.15,
                "sf_turnover_pctile_120d_neg": -(code_index + day_index / 100.0),
            }
            row.update({name: values[name] for name in set(features) | {"daily_return"}})
            rows.append(row)
    return pd.DataFrame(rows).set_index(["trade_date", "l1_code"]).sort_index()


def _dates(count: int, *, start: date = date(2024, 1, 2)) -> list[date]:
    return [start + timedelta(days=index) for index in range(count)]


def test_preprocess_is_level_global_and_na_invalidates_the_whole_row() -> None:
    calendar = _dates(8)
    panel = _panel(["S1", "S2", "S3"], calendar, subject.RELATIVE_FEATURES)
    panel.loc[(pd.Timestamp(calendar[3]), "S2"), "net_mf_ratio"] = np.nan

    prepared = subject.prepare_component(
        panel,
        component="L1_relative",
        level="L1",
        feature_names=subject.RELATIVE_FEATURES,
        calendar=calendar,
        start=calendar[0],
        end=calendar[-1],
        expected_days=8,
        expected_sector_count=3,
        minimum_daily_count=2,
        relative=True,
    )

    assert prepared.preprocessor.valid_row_count == 23
    assert prepared.preprocessor.payload()["quantile_method"] == "linear"
    assert prepared.preprocessor.payload()["ddof"] == 0
    assert any(
        item["sector_code"] == "S2" and item["trade_date"] == calendar[3].isoformat()
        for item in prepared.unavailable_items
    )
    assert len(prepared.sequences) == 3
    assert all(np.isfinite(sequence.values).all() for sequence in prepared.sequences)
    # A shared scaler preserves cross-sector location differences before the daily median residual.
    first_day = [sequence.values[0, 0] for sequence in prepared.sequences]
    assert len(set(np.round(first_day, 12))) == 3


def test_validation_reuses_train_preprocessor_without_refitting() -> None:
    calendar = _dates(12)
    panel = _panel(["S1", "S2"], calendar, subject.MARKET_FEATURES)
    train = subject.prepare_component(
        panel,
        component="market",
        level="L2",
        feature_names=subject.MARKET_FEATURES,
        calendar=calendar,
        start=calendar[0],
        end=calendar[7],
        expected_days=8,
        expected_sector_count=2,
        minimum_daily_count=2,
        relative=False,
    )
    validation = subject.prepare_component(
        panel,
        component="market",
        level="L2",
        feature_names=subject.MARKET_FEATURES,
        calendar=calendar,
        start=calendar[8],
        end=calendar[-1],
        expected_days=4,
        expected_sector_count=2,
        minimum_daily_count=2,
        relative=False,
        preprocessor=train.preprocessor,
    )
    assert validation.preprocessor == train.preprocessor


def test_exact_fold_contract_and_preprocess_numeric_authority() -> None:
    assert subject.FOLDS == (
        {
            "fold": "fold-1",
            "train_start": date(2022, 1, 4),
            "train_end": date(2023, 9, 1),
            "validation_start": date(2023, 9, 4),
            "validation_end": date(2024, 3, 14),
            "train_days": 405,
            "validation_days": 126,
        },
        {
            "fold": "fold-2",
            "train_start": date(2022, 1, 4),
            "train_end": date(2024, 3, 14),
            "validation_start": date(2024, 3, 15),
            "validation_end": date(2024, 9, 18),
            "train_days": 531,
            "validation_days": 126,
        },
        {
            "fold": "fold-3",
            "train_start": date(2022, 1, 4),
            "train_end": date(2024, 9, 18),
            "validation_start": date(2024, 9, 19),
            "validation_end": date(2025, 3, 31),
            "train_days": 657,
            "validation_days": 126,
        },
    )
    calendar = _dates(2)
    rows = []
    for day, values in zip(calendar, ((0.0, 1.0, 10.0, 20.0), (2.0, 3.0, 12.0, 22.0)), strict=True):
        rows.extend(
            {"trade_date": pd.Timestamp(day), "l1_code": f"S{index}", "x": value} for index, value in enumerate(values)
        )
    panel = pd.DataFrame(rows).set_index(["trade_date", "l1_code"]).sort_index()
    prepared = subject.prepare_component(
        panel,
        component="L1_relative",
        level="L1",
        feature_names=("x",),
        calendar=calendar,
        start=calendar[0],
        end=calendar[-1],
        expected_days=2,
        expected_sector_count=4,
        minimum_daily_count=4,
        relative=True,
    )
    processor = prepared.preprocessor
    clipped = np.clip(
        panel["x"].to_numpy(dtype=np.float64),
        processor.lower[0],
        processor.upper[0],
    )
    expected_mean = math.fsum(clipped.tolist()) / len(clipped)
    expected_variance = math.fsum((float(value) - expected_mean) ** 2 for value in clipped) / len(clipped)
    assert processor.mean[0] == expected_mean
    assert processor.std[0] == math.sqrt(expected_variance)
    first_day = sorted(sequence.values[0, 0] for sequence in prepared.sequences)
    assert first_day[1] == pytest.approx(-first_day[2])


def test_optimal_path_matches_brute_force_oracle() -> None:
    values = np.asarray([[0.0], [0.2], [4.8], [5.0]], dtype=np.float64)
    centers = np.asarray([[0.0], [5.0]], dtype=np.float64)
    penalty = 1.5
    actual = subject._optimal_segment_path(values, centers, penalty)

    def objective(path: tuple[int, ...]) -> float:
        emission = sum(float((values[index, 0] - centers[state, 0]) ** 2) for index, state in enumerate(path))
        jumps = sum(path[index] != path[index - 1] for index in range(1, len(path)))
        return emission + penalty * jumps

    expected = min(itertools.product(range(2), repeat=len(values)), key=lambda path: (objective(path), path))
    assert actual.tolist() == list(expected)

    tied = subject._optimal_segment_path(
        np.asarray([[0.0]], dtype=np.float64),
        np.asarray([[-1.0], [1.0]], dtype=np.float64),
        penalty,
    )
    assert tied.tolist() == [0]


def test_causal_inference_resets_cost_at_each_gap() -> None:
    component = subject.PreparedComponent(
        component="market",
        level="L2",
        feature_names=("x",),
        expected_sector_count=1,
        minimum_daily_count=1,
        canonical_codes=("S1",),
        sequences=(
            subject.SequenceData(
                key="market",
                dates=(date(2024, 1, 2), date(2024, 1, 4)),
                ordinals=(0, 2),
                values=np.asarray([[0.0], [10.0]], dtype=np.float64),
            ),
        ),
        preprocessor=subject.Preprocessor(("x",), (0.0,), (1.0,), (0.0,), (1.0,), 2, "a" * 64),
        unavailable_items=(),
        valid_row_count=2,
        valid_identity_sha256="b" * 64,
    )
    states = subject.causal_states(component, np.asarray([[0.0], [10.0]]), 1000.0)
    assert states[0].tolist() == [0, 1]


def test_fit_is_deterministic_for_same_seed() -> None:
    values = np.asarray([[-2.1], [-2.0], [-1.9], [1.9], [2.0], [2.1]], dtype=np.float64)
    component = subject.PreparedComponent(
        component="market",
        level="L2",
        feature_names=("x",),
        expected_sector_count=1,
        minimum_daily_count=1,
        canonical_codes=("S1",),
        sequences=(
            subject.SequenceData(
                key="market",
                dates=tuple(_dates(6)),
                ordinals=tuple(range(6)),
                values=values,
            ),
        ),
        preprocessor=subject.Preprocessor(("x",), (-3.0,), (3.0,), (0.0,), (1.0,), 6, "a" * 64),
        unavailable_items=(),
        valid_row_count=6,
        valid_identity_sha256="b" * 64,
    )
    left = subject.fit_jump_model(component, state_count=2, jump_penalty=1.0, seed=42)
    right = subject.fit_jump_model(component, state_count=2, jump_penalty=1.0, seed=42)
    assert np.array_equal(left.centers, right.centers)
    assert all(np.array_equal(a, b) for a, b in zip(left.paths, right.paths, strict=True))
    assert left.objective == right.objective
    receipt = subject._fit_summary(left, component)
    assert sum(receipt["state_counts"]) == 6
    assert receipt["sequence_count"] == 1
    assert receipt["run_count"] == receipt["jump_count"] + 1
    assert receipt["path_receipts"][0]["sequence_key"] == "market"
    assert receipt["path_receipts"][0]["state_counts"] == receipt["state_counts"]


def test_fit_uses_exact_kmeans_contract_and_typed_numeric_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    values = np.asarray([[-2.1], [-2.0], [-1.9], [1.9], [2.0], [2.1]], dtype=np.float64)
    component = subject.PreparedComponent(
        component="market",
        level="L2",
        feature_names=("x",),
        expected_sector_count=1,
        minimum_daily_count=1,
        canonical_codes=("S1",),
        sequences=(
            subject.SequenceData(
                key="market",
                dates=tuple(_dates(6)),
                ordinals=tuple(range(6)),
                values=values,
            ),
        ),
        preprocessor=subject.Preprocessor(("x",), (-3.0,), (3.0,), (0.0,), (1.0,), 6, "a" * 64),
        unavailable_items=(),
        valid_row_count=6,
        valid_identity_sha256="b" * 64,
    )
    real_kmeans = subject.KMeans
    parameters: dict[str, object] = {}

    def capture_kmeans(**kwargs: object) -> object:
        parameters.update(kwargs)
        return real_kmeans(**kwargs)

    monkeypatch.setattr(subject, "KMeans", capture_kmeans)
    subject.fit_jump_model(component, state_count=2, jump_penalty=1.0, seed=43)
    assert parameters == {
        "n_clusters": 2,
        "init": "k-means++",
        "n_init": 1,
        "random_state": 43,
        "max_iter": 300,
        "tol": 1e-4,
        "algorithm": "lloyd",
        "copy_x": True,
    }

    objective_values = iter((1.0, 2.0))
    monkeypatch.setattr(subject, "_objective", lambda *args, **kwargs: next(objective_values))
    with pytest.raises(subject.JumpSpikeError) as increased:
        subject.fit_jump_model(component, state_count=2, jump_penalty=1.0, seed=42)
    assert increased.value.reason_code == subject.REASON_OBJECTIVE_INCREASED


def test_fit_fails_on_empty_state_and_max_iteration(monkeypatch: pytest.MonkeyPatch) -> None:
    values = np.asarray([[-2.1], [-2.0], [-1.9], [1.9], [2.0], [2.1]], dtype=np.float64)
    component = subject.PreparedComponent(
        component="market",
        level="L2",
        feature_names=("x",),
        expected_sector_count=1,
        minimum_daily_count=1,
        canonical_codes=("S1",),
        sequences=(subject.SequenceData("market", tuple(_dates(6)), tuple(range(6)), values),),
        preprocessor=subject.Preprocessor(("x",), (-3.0,), (3.0,), (0.0,), (1.0,), 6, "a" * 64),
        unavailable_items=(),
        valid_row_count=6,
        valid_identity_sha256="b" * 64,
    )
    all_zero = (np.zeros(6, dtype=np.int64),)
    monkeypatch.setattr(subject, "_optimal_paths", lambda *args, **kwargs: all_zero)
    empty_objectives = iter((1.0, 0.0))
    monkeypatch.setattr(subject, "_objective", lambda *args, **kwargs: next(empty_objectives))
    with pytest.raises(subject.JumpSpikeError) as empty:
        subject.fit_jump_model(component, state_count=2, jump_penalty=1.0, seed=42)
    assert empty.value.reason_code == subject.REASON_STATE_EMPTY

    monkeypatch.setattr(subject, "MAX_JUMP_ITERATIONS", 1)
    objective_values = iter((1.0, 0.0))
    monkeypatch.setattr(subject, "_objective", lambda *args, **kwargs: next(objective_values))
    with pytest.raises(subject.JumpSpikeError) as exhausted:
        subject.fit_jump_model(component, state_count=2, jump_penalty=1.0, seed=42)
    assert exhausted.value.reason_code == subject.REASON_MAX_ITERATIONS


def test_semantic_mapping_is_score_based_and_ties_fail() -> None:
    market = subject.semantic_mapping(
        "market",
        subject.MARKET_FEATURES,
        np.asarray([[0.5, 0.1, 0, 0, 0], [-0.2, 0.3, 0, 0, 0]], dtype=np.float64),
    )
    assert market == {1: "risk_off", 0: "risk_on"}
    relative = subject.semantic_mapping(
        "L1_relative",
        subject.RELATIVE_FEATURES,
        np.asarray([[0.0, 0, 0, 0, 0], [1.0, 0, 0, 0, 0], [-1.0, 0, 0, 0, 0]], dtype=np.float64),
    )
    assert relative == {2: "fading", 0: "neutral", 1: "trending"}
    with pytest.raises(subject.JumpSpikeError, match="tied") as captured:
        subject.semantic_mapping(
            "L1_relative",
            subject.RELATIVE_FEATURES,
            np.asarray([[0.0, 0, 0, 0, 0], [0.0, 0, 0, 0, 0], [1.0, 0, 0, 0, 0]]),
        )
    assert captured.value.reason_code == subject.REASON_SEMANTIC_TIE
    with pytest.raises(subject.JumpSpikeError) as invalid:
        subject.semantic_mapping("unknown", subject.RELATIVE_FEATURES, np.zeros((3, 5)))
    assert invalid.value.reason_code == subject.REASON_INPUT_IDENTITY


def test_relative_metrics_require_real_cross_section_and_future_boundary() -> None:
    calendar = _dates(15)
    codes = [f"S{index:02d}" for index in range(12)]
    panel = _panel(codes, calendar, subject.RELATIVE_FEATURES)
    states = {}
    for day in calendar:
        for index, code in enumerate(codes):
            states[(code, day)] = "fading" if index < 6 else "trending"
    benchmark = {day: 0.0 for day in calendar}
    metrics = subject.relative_fold_metrics(
        states,
        panel,
        benchmark,
        calendar,
        validation_start=calendar[0],
        validation_end=calendar[-1],
        horizon=2,
    )
    assert metrics["metric_valid"] is True
    assert metrics["eligible_date_count"] == 13
    assert metrics["rank_ic_available_date_count"] == 13
    assert metrics["spread_available_date_count"] == 13
    assert len(metrics["eligible_decision_dates"]) == 13
    assert metrics["excluded_tail_dates"] == [calendar[-2].isoformat(), calendar[-1].isoformat()]
    assert metrics["mean_rank_ic"] > 0
    assert metrics["mean_spread"] > 0


def test_risk_and_newey_west_metrics_fail_closed() -> None:
    metrics = subject.risk_metrics([True, False, True, False], [True, False, False, False])
    assert metrics["metric_valid"] is True
    assert metrics["selection_metric_kind"] == "event_bearing"
    assert metrics["tp"] == 1
    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 0.5
    zero_event = subject.risk_metrics([False, False], [False, False])
    assert zero_event["metric_valid"] is True
    assert zero_event["selection_metric_kind"] == "zero_event_negative_control"
    assert zero_event["event_metric_valid"] is False
    assert zero_event["zero_event_metric_valid"] is True
    assert zero_event["recall"] is None
    assert zero_event["f1"] is None
    assert zero_event["false_positive_rate"] == 0.0
    assert zero_event["specificity"] == 1.0
    nw = subject.newey_west_t([0.01, 0.02, 0.03, 0.01, 0.04, 0.02], lag=1)
    assert nw["metric_valid"] is True
    assert math.isfinite(nw["t_stat"])
    assert subject.newey_west_t([1.0, 1.0, 1.0], lag=1)["metric_valid"] is False


def test_market_lambda_score_uses_zero_event_fold_as_negative_control() -> None:
    def fold(outcomes: list[bool], predictions: list[bool], identity: str) -> dict[str, object]:
        metrics = subject.risk_metrics(outcomes, predictions)
        metrics.update(
            {
                "schema_version": subject.MARKET_FOLD_METRICS_SCHEMA_VERSION,
                "eligible_date_count": len(outcomes),
                "available_date_count": len(outcomes),
                "market_risk_event_sha256": identity * 64,
            }
        )
        return {"metrics": metrics}

    folds = [
        fold([True, False, False], [True, True, False], "a"),
        fold([False, False, False], [True, False, False], "b"),
        fold([True, True, False], [True, False, False], "c"),
    ]
    for index, fold_receipt in enumerate(folds, start=1):
        fold_receipt["fold"] = f"fold-{index}"
    score = subject._market_lambda_score(folds)
    assert score["lambda_eligible"] is True
    assert score["market_event_bearing_fold_count"] == 2
    assert score["market_zero_event_fold_count"] == 1
    assert score["pooled_confusion_counts"] == {"tp": 2, "fp": 2, "fn": 1, "tn": 4}
    assert score["pooled_micro_f1"] == pytest.approx(4 / 7)
    assert score["pooled_precision_lift"] == pytest.approx(1 / 2 - 1 / 3)
    assert score["max_zero_event_false_positive_rate"] == pytest.approx(1 / 3)
    assert folds[1]["metrics"]["f1"] is None


def test_market_lambda_score_requires_two_event_bearing_folds() -> None:
    folds = []
    for index, (outcomes, predictions) in enumerate(
        (
            ([True, False], [True, False]),
            ([False, False], [False, False]),
            ([False, False], [True, False]),
        )
    ):
        metrics = subject.risk_metrics(outcomes, predictions)
        metrics.update(
            {
                "schema_version": subject.MARKET_FOLD_METRICS_SCHEMA_VERSION,
                "eligible_date_count": len(outcomes),
                "available_date_count": len(outcomes),
                "market_risk_event_sha256": str(index) * 64,
            }
        )
        folds.append({"fold": f"fold-{index + 1}", "metrics": metrics})
    score = subject._market_lambda_score(folds)
    assert score["lambda_eligible"] is False
    assert score["market_event_bearing_fold_count"] == 1
    assert score["market_zero_event_fold_count"] == 2


def test_market_lambda_score_rejects_v1_schema_and_incomplete_date_coverage() -> None:
    folds = []
    for index, (outcomes, predictions) in enumerate(
        (
            ([True, False], [True, False]),
            ([False, False], [False, False]),
            ([True, False], [True, False]),
        )
    ):
        metrics = subject.risk_metrics(outcomes, predictions)
        metrics.update(
            {
                "schema_version": subject.MARKET_FOLD_METRICS_SCHEMA_VERSION,
                "eligible_date_count": len(outcomes),
                "available_date_count": len(outcomes),
                "market_risk_event_sha256": format(index + 10, "x") * 64,
            }
        )
        folds.append({"fold": f"fold-{index + 1}", "metrics": metrics})

    old_schema = [{**item, "metrics": {**item["metrics"], "schema_version": "v1"}} for item in folds]
    assert subject._market_lambda_score(old_schema)["lambda_eligible"] is False

    incomplete = [{**item, "metrics": dict(item["metrics"])} for item in folds]
    incomplete[1]["metrics"]["available_date_count"] = 1
    assert subject._market_lambda_score(incomplete)["lambda_eligible"] is False


def test_market_lambda_selection_is_pooled_then_zero_event_fpr_then_grid_order() -> None:
    event_hashes = ["a" * 64, "b" * 64, "c" * 64]

    def receipt(
        jump_penalty: float,
        *,
        micro_f1: float,
        precision_lift: float,
        max_zero_event_fpr: float,
    ) -> dict[str, object]:
        return {
            "jump_penalty": jump_penalty,
            "lambda_eligible": True,
            "market_fold_event_sha256": event_hashes,
            "market_zero_event_fold_count": 1,
            "pooled_micro_f1": micro_f1,
            "pooled_precision_lift": precision_lift,
            "max_zero_event_false_positive_rate": max_zero_event_fpr,
        }

    receipts = [
        receipt(0.25, micro_f1=0.20, precision_lift=0.01, max_zero_event_fpr=0.60),
        receipt(0.5, micro_f1=0.20005, precision_lift=0.02005, max_zero_event_fpr=0.50),
        receipt(1.0, micro_f1=0.20004, precision_lift=0.02000, max_zero_event_fpr=0.40),
        receipt(2.0, micro_f1=0.18, precision_lift=0.10, max_zero_event_fpr=0.10),
    ]
    assert subject._select_lambda(receipts, component="market") == 1.0

    earlier_grid_tie = [
        receipt(0.25, micro_f1=0.2, precision_lift=0.02, max_zero_event_fpr=0.4),
        receipt(0.5, micro_f1=0.2, precision_lift=0.02, max_zero_event_fpr=0.4),
    ]
    assert subject._select_lambda(earlier_grid_tie, component="market") == 0.25

    inconsistent_identity = [dict(item) for item in earlier_grid_tie]
    inconsistent_identity[1]["market_fold_event_sha256"] = ["a" * 64, "b" * 64, "d" * 64]
    with pytest.raises(subject.JumpSpikeError) as captured:
        subject._select_lambda(inconsistent_identity, component="market")
    assert captured.value.reason_code == subject.REASON_SELECTION_METRIC
    assert captured.value.stage == "lambda_selection"


def test_report_write_is_external_immutable_and_readable(tmp_path: Path) -> None:
    report = subject.report_for_write(
        subject.failure_report(
            {"schema_version": subject.REQUEST_SCHEMA_VERSION},
            producer_commit="a" * 40,
            error=subject.JumpSpikeError("typed", "failed", stage="test"),
        ),
        failure=True,
    )
    target = tmp_path / "spike.failure.json"
    assert subject.preflight_output_path(target, repository_root=Path(__file__).resolve().parents[3]) == target
    written = subject.write_report(target, report, repository_root=Path(__file__).resolve().parents[3])
    assert written == target
    assert subject.write_report(target, report, repository_root=Path(__file__).resolve().parents[3]) == target
    changed = {**report, "status": "changed"}
    with pytest.raises(subject.JumpSpikeError) as captured:
        subject.write_report(target, changed, repository_root=Path(__file__).resolve().parents[3])
    assert captured.value.reason_code == subject.REASON_COLLISION
    with pytest.raises(subject.JumpSpikeError) as preflight:
        subject.preflight_output_path(target, repository_root=Path(__file__).resolve().parents[3])
    assert preflight.value.reason_code == subject.REASON_COLLISION
    with pytest.raises(subject.JumpSpikeError):
        subject.write_report(
            Path(__file__).resolve().parents[3] / "forbidden.json",
            report,
            repository_root=Path(__file__).resolve().parents[3],
        )


def test_planned_fit_count_is_exact_and_not_multiplied_by_sector() -> None:
    assert subject.planned_fit_count() == 456


def test_public_market_component_reuses_the_exact_private_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_component(name: str, **kwargs: object) -> dict[str, object]:
        captured["name"] = name
        captured.update(kwargs)
        return {"component": name, "receipt_sha256": "a" * 64}

    monkeypatch.setattr(subject, "_run_component", fake_component)
    attempts: list[dict[str, object]] = []
    result = subject.run_market_component(
        {"identity": "input"},
        calendar=(date(2024, 1, 2),),
        benchmark={date(2024, 1, 2): 0.0},
        attempt_log=attempts,
    )

    assert captured["name"] == "market"
    assert captured["attempt_log"] is attempts
    assert result["component"] == "market"
    assert subject.market_planned_fit_count() == 152


def test_development_quintiles_and_three_coverage_states() -> None:
    small_codes = [f"Q{index}" for index in range(5)]
    quintile_rows = [
        {
            "sector_code": code,
            "trade_date": day.isoformat(),
            "price_expected_weight": index + 1.0,
            "moneyflow_contributor_amount": 100.0 - index,
        }
        for day in _dates(5)
        for index, code in enumerate(small_codes)
    ]
    frozen = subject.freeze_quintiles(
        quintile_rows,
        canonical_codes=small_codes,
        development_dates=_dates(5),
        expected_development_days=5,
        expected_sector_count=5,
    )
    assert set(frozen["groups"]["size"].values()) == set(range(5))
    assert set(frozen["groups"]["liquidity"].values()) == set(range(5))

    dates = [subject.HOLDOUT_START]
    dates.extend(subject.HOLDOUT_START + timedelta(days=index) for index in range(1, 241))
    dates.append(subject.HOLDOUT_END)
    l1 = [f"L1-{index:02d}" for index in range(31)]
    l2 = [f"L2-{index:03d}" for index in range(131)]
    hierarchy = {code: l1[index % len(l1)] for index, code in enumerate(l2)}
    groups = {code: min(4, math.floor(index * 5 / len(l2))) for index, code in enumerate(l2)}
    l1_available = {(code, day) for code in l1 for day in dates}
    l2_available = {(code, day) for code in l2 for day in dates}
    full = subject.classify_coverage(
        holdout_dates=dates,
        l1_codes=l1,
        l2_codes=l2,
        l1_available=l1_available,
        l2_available=l2_available,
        l2_to_l1=hierarchy,
        size_quintiles=groups,
        liquidity_quintiles=groups,
        product_metrics_passed=True,
    )
    assert full["status"] == "FULL_READY"
    partial_keys = set(l2_available)
    partial_keys.remove((l2[0], dates[0]))
    partial = subject.classify_coverage(
        holdout_dates=dates,
        l1_codes=l1,
        l2_codes=l2,
        l1_available=l1_available,
        l2_available=partial_keys,
        l2_to_l1=hierarchy,
        size_quintiles=groups,
        liquidity_quintiles=groups,
        product_metrics_passed=True,
    )
    assert partial["status"] == "COVERAGE_AVAILABLE"
    blocked = subject.classify_coverage(
        holdout_dates=dates,
        l1_codes=l1,
        l2_codes=l2,
        l1_available=l1_available,
        l2_available={key for key in l2_available if key[0] != l2[0]},
        l2_to_l1=hierarchy,
        size_quintiles=groups,
        liquidity_quintiles=groups,
        product_metrics_passed=True,
    )
    assert blocked["status"] == "NOT_AVAILABLE"


def test_quintile_and_holdout_authorities_reject_wrong_date_sets() -> None:
    with pytest.raises(subject.JumpSpikeError) as captured:
        subject.freeze_quintiles(
            [
                {
                    "sector_code": "Q0",
                    "trade_date": date(2023, 1, 1),
                    "price_expected_weight": 1.0,
                    "moneyflow_contributor_amount": 1.0,
                }
            ],
            canonical_codes=["Q0"],
            development_dates=[date(2024, 1, 1)],
            expected_development_days=1,
            expected_sector_count=1,
        )
    assert captured.value.reason_code == subject.REASON_REPRESENTATIVENESS

    wrong_holdout = _dates(subject.HOLDOUT_TRADING_DAYS, start=date(2024, 1, 1))
    blocked = subject.classify_coverage(
        holdout_dates=wrong_holdout,
        l1_codes=[f"L1-{index:02d}" for index in range(31)],
        l2_codes=[f"L2-{index:03d}" for index in range(131)],
        l1_available=set(),
        l2_available=set(),
        l2_to_l1={},
        size_quintiles={},
        liquidity_quintiles={},
        product_metrics_passed=True,
    )
    assert blocked["status"] == "NOT_AVAILABLE"
    assert blocked["reason_code"] == subject.REASON_REPRESENTATIVENESS


def test_run_rejects_any_holdout_business_date_before_panels() -> None:
    request = {
        "schema_version": subject.REQUEST_SCHEMA_VERSION,
        "contract_version": subject.CONTRACT_VERSION,
        "expected_producer_commit": "a" * 40,
        "holdout_start": subject.HOLDOUT_START.isoformat(),
        "holdout_end": subject.HOLDOUT_END.isoformat(),
        "forbidden_holdout_date_set_sha256": "b" * 64,
        "source": {},
    }
    inputs = {
        "trading_dates": (subject.DEVELOPMENT_START, subject.HOLDOUT_START),
    }
    with pytest.raises(subject.JumpSpikeError) as captured:
        subject.run_p2_3_spike(inputs, request, producer_commit="a" * 40)
    assert captured.value.reason_code in {subject.REASON_FOLD_BOUNDARY, subject.REASON_HOLDOUT}


def test_top_level_orchestrator_requires_exact_456_attempts_and_zero_side_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = [subject.DEVELOPMENT_START + timedelta(days=index) for index in range(782)]
    calendar.append(subject.DEVELOPMENT_END)
    request = {
        "schema_version": subject.REQUEST_SCHEMA_VERSION,
        "contract_version": subject.CONTRACT_VERSION,
        "expected_producer_commit": "a" * 40,
        "holdout_start": subject.HOLDOUT_START.isoformat(),
        "holdout_end": subject.HOLDOUT_END.isoformat(),
        "forbidden_holdout_date_set_sha256": "b" * 64,
        "source": {"identity": "synthetic"},
    }
    inputs = {
        "trading_dates": tuple(calendar),
        "dataset_manifest": {
            "calendar_benchmark": {
                "rows": [[day.isoformat(), 0.0] for day in calendar],
            }
        },
        "mapping_manifest": {"rows": []},
        "feature_definition": {"level": "L1"},
        "l2_feature_definition": {"level": "L2"},
        "database": {"host": "redacted", "port": 5432, "dbname": "dev"},
    }

    def fake_component(name: str, **kwargs: object) -> dict[str, object]:
        attempt_log = kwargs["attempt_log"]
        assert isinstance(attempt_log, list)
        attempt_log.extend({"component": name, "attempt": index} for index in range(152))
        body = {"component": name}
        return {**body, "receipt_sha256": subject.canonical_sha256(body)}

    monkeypatch.setattr(subject, "_run_component", fake_component)
    report = subject.run_p2_3_spike(inputs, request, producer_commit="a" * 40)
    assert report["planned_fit_count"] == 456
    assert report["completed_fit_count"] == 456
    assert report["component_count"] == 3
    assert report["selection_performed"] is True
    assert report["selection_scope"] == "development_only"
    assert report["holdout_accessed"] is False
    assert report["product_acceptance_performed"] is False
    assert report["model_write"] is False
    assert report["ready_write"] is False
    assert report["database_write"] is False
    assert report["runtime_action"] is False


def test_top_level_orchestrator_preserves_partial_attempts_on_unknown_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = [subject.DEVELOPMENT_START + timedelta(days=index) for index in range(782)]
    calendar.append(subject.DEVELOPMENT_END)
    request = {
        "schema_version": subject.REQUEST_SCHEMA_VERSION,
        "contract_version": subject.CONTRACT_VERSION,
        "expected_producer_commit": "a" * 40,
        "holdout_start": subject.HOLDOUT_START.isoformat(),
        "holdout_end": subject.HOLDOUT_END.isoformat(),
        "forbidden_holdout_date_set_sha256": "b" * 64,
        "source": {"identity": "synthetic"},
    }
    inputs = {
        "trading_dates": tuple(calendar),
        "dataset_manifest": {"calendar_benchmark": {"rows": [[day.isoformat(), 0.0] for day in calendar]}},
        "mapping_manifest": {"rows": []},
    }

    def fail_after_attempts(name: str, **kwargs: object) -> dict[str, object]:
        attempt_log = kwargs["attempt_log"]
        assert isinstance(attempt_log, list)
        attempt_log.extend({"component": name, "attempt": index} for index in range(5))
        raise RuntimeError("synthetic unexpected failure")

    monkeypatch.setattr(subject, "_run_component", fail_after_attempts)
    with pytest.raises(subject.JumpSpikeError) as captured:
        subject.run_p2_3_spike(inputs, request, producer_commit="a" * 40)
    assert captured.value.reason_code == subject.REASON_UNEXPECTED
    assert captured.value.evidence["completed_fit_count"] == 5
    assert len(captured.value.evidence["fit_attempts"]) == 5


def test_top_level_orchestrator_preserves_typed_component_failure_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = [subject.DEVELOPMENT_START + timedelta(days=index) for index in range(782)]
    calendar.append(subject.DEVELOPMENT_END)
    request = {
        "schema_version": subject.REQUEST_SCHEMA_VERSION,
        "contract_version": subject.CONTRACT_VERSION,
        "expected_producer_commit": "a" * 40,
        "holdout_start": subject.HOLDOUT_START.isoformat(),
        "holdout_end": subject.HOLDOUT_END.isoformat(),
        "forbidden_holdout_date_set_sha256": "b" * 64,
        "source": {"identity": "synthetic"},
    }
    inputs = {
        "trading_dates": tuple(calendar),
        "dataset_manifest": {"calendar_benchmark": {"rows": [[day.isoformat(), 0.0] for day in calendar]}},
        "mapping_manifest": {"rows": []},
    }
    lambda_receipts = [{"jump_penalty": 0.25, "lambda_eligible": False, "folds": []}]

    def fail_with_evidence(name: str, **kwargs: object) -> dict[str, object]:
        attempt_log = kwargs["attempt_log"]
        assert isinstance(attempt_log, list)
        attempt_log.append({"component": name, "status": "fit_completed"})
        raise subject.JumpSpikeError(
            subject.REASON_SELECTION,
            "no lambda has three valid development folds",
            stage="lambda_selection",
            evidence={
                "component": name,
                "lambda_receipt_count": 1,
                "lambda_receipts_sha256": subject.canonical_sha256(lambda_receipts),
                "lambda_receipts": lambda_receipts,
            },
        )

    monkeypatch.setattr(subject, "_run_component", fail_with_evidence)
    with pytest.raises(subject.JumpSpikeError) as captured:
        subject.run_p2_3_spike(inputs, request, producer_commit="a" * 40)

    assert captured.value.evidence["component"] == "market"
    assert captured.value.evidence["lambda_receipts"] == lambda_receipts
    assert captured.value.evidence["lambda_receipts_sha256"] == subject.canonical_sha256(lambda_receipts)
    assert captured.value.evidence["completed_fit_count"] == 1
    assert captured.value.evidence["fit_attempts"] == [{"component": "market", "status": "fit_completed"}]


def test_component_orchestration_rejects_incomplete_lambda_and_refits_selected_lambda(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    folds = (
        {
            "fold": f"fold-{index}",
            "train_start": date(2024, 1, 2),
            "train_end": date(2024, 1, 2 + index),
            "validation_start": date(2024, 1, 3 + index),
            "validation_end": date(2024, 1, 3 + index),
            "train_days": 1,
            "validation_days": 1,
        }
        for index in range(1, 4)
    )
    folded = tuple(folds)
    monkeypatch.setattr(subject, "FOLDS", folded)
    monkeypatch.setattr(subject, "LAMBDA_GRID", (0.25, 0.5))
    monkeypatch.setattr(subject, "RESTART_SEEDS", (42,))

    def fake_prepare(
        panel: pd.DataFrame,
        *,
        component: str,
        level: str,
        feature_names: tuple[str, ...],
        start: date,
        **kwargs: object,
    ) -> subject.PreparedComponent:
        del panel, kwargs
        values = np.zeros((1, len(feature_names)), dtype=np.float64)
        return subject.PreparedComponent(
            component=component,
            level=level,
            feature_names=tuple(feature_names),
            expected_sector_count=31,
            minimum_daily_count=28,
            canonical_codes=("S1",),
            sequences=(subject.SequenceData(start.isoformat(), (start,), (0,), values),),
            preprocessor=subject.Preprocessor(
                tuple(feature_names),
                tuple(-1.0 for _ in feature_names),
                tuple(1.0 for _ in feature_names),
                tuple(0.0 for _ in feature_names),
                tuple(1.0 for _ in feature_names),
                1,
                "a" * 64,
            ),
            unavailable_items=(),
            valid_row_count=1,
            valid_identity_sha256="b" * 64,
        )

    def fake_restarts(
        component: subject.PreparedComponent,
        *,
        state_count: int,
        jump_penalty: float,
        attempt_log: list[dict[str, object]],
        context: dict[str, object],
    ) -> tuple[subject.JumpFit, list[dict[str, object]]]:
        attempt = {**context, "seed": 42, "jump_penalty": jump_penalty, "status": "fit_completed"}
        attempt_log.append(attempt)
        fit = subject.JumpFit(
            centers=np.full((state_count, len(component.feature_names)), jump_penalty, dtype=np.float64),
            paths=(np.zeros(1, dtype=np.int64),),
            objective=jump_penalty,
            normalized_objective=jump_penalty,
            iterations=1,
            seed=42,
            jump_penalty=jump_penalty,
            row_count=1,
            feature_count=len(component.feature_names),
        )
        return fit, [attempt]

    def fake_states(
        component: subject.PreparedComponent,
        paths: tuple[np.ndarray, ...],
        mapping: dict[int, str],
    ) -> dict[tuple[str, date], str]:
        del mapping
        return {(component.sequences[0].key, component.sequences[0].dates[0]): str(int(paths[0][0]))}

    def fake_metrics(states: dict[tuple[str, date], str], *args: object, **kwargs: object) -> dict[str, object]:
        del args
        penalty_code = int(next(iter(states.values())))
        fold_start = kwargs["validation_start"]
        valid = not (penalty_code == 25 and fold_start == folded[1]["validation_start"])
        return {
            "metric_valid": valid,
            "mean_rank_ic": penalty_code / 100.0 if valid else None,
            "mean_spread": penalty_code / 200.0 if valid else None,
        }

    monkeypatch.setattr(subject, "prepare_component", fake_prepare)
    monkeypatch.setattr(subject, "_run_restarts", fake_restarts)
    monkeypatch.setattr(subject, "semantic_mapping", lambda *args, **kwargs: {0: "fading", 1: "neutral", 2: "trending"})
    monkeypatch.setattr(
        subject,
        "causal_states",
        lambda component, centers, jump_penalty: (np.asarray([int(jump_penalty * 100)], dtype=np.int64),),
    )
    monkeypatch.setattr(subject, "state_rows", fake_states)
    monkeypatch.setattr(subject, "relative_fold_metrics", fake_metrics)

    attempt_log: list[dict[str, object]] = []
    result = subject._run_component(
        "L1_relative",
        inputs={"panel": pd.DataFrame()},
        calendar=(subject.DEVELOPMENT_START, subject.DEVELOPMENT_END),
        benchmark={},
        attempt_log=attempt_log,
    )

    assert [item["lambda_eligible"] for item in result["lambda_receipts"]] == [False, True]
    assert result["selected_lambda"] == 0.5
    assert result["final_selected_seed"] == 42
    assert len(attempt_log) == 7
    assert result["holdout_accessed"] is False


def test_component_selection_failure_preserves_lambda_fold_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fold = {
        "fold": "fold-1",
        "train_start": date(2024, 1, 2),
        "train_end": date(2024, 1, 2),
        "validation_start": date(2024, 1, 3),
        "validation_end": date(2024, 1, 3),
        "train_days": 1,
        "validation_days": 1,
    }
    monkeypatch.setattr(subject, "FOLDS", (fold,))
    monkeypatch.setattr(subject, "LAMBDA_GRID", (0.25,))
    monkeypatch.setattr(subject, "RESTART_SEEDS", (42,))

    def fake_prepare(
        panel: pd.DataFrame,
        *,
        component: str,
        level: str,
        feature_names: tuple[str, ...],
        start: date,
        **kwargs: object,
    ) -> subject.PreparedComponent:
        del panel, kwargs
        values = np.zeros((1, len(feature_names)), dtype=np.float64)
        return subject.PreparedComponent(
            component=component,
            level=level,
            feature_names=tuple(feature_names),
            expected_sector_count=131,
            minimum_daily_count=118,
            canonical_codes=("S1",),
            sequences=(subject.SequenceData("market", (start,), (0,), values),),
            preprocessor=subject.Preprocessor(
                tuple(feature_names),
                tuple(-1.0 for _ in feature_names),
                tuple(1.0 for _ in feature_names),
                tuple(0.0 for _ in feature_names),
                tuple(1.0 for _ in feature_names),
                1,
                "a" * 64,
            ),
            unavailable_items=(),
            valid_row_count=1,
            valid_identity_sha256="b" * 64,
        )

    def fake_restarts(
        component: subject.PreparedComponent,
        *,
        state_count: int,
        jump_penalty: float,
        attempt_log: list[dict[str, object]],
        context: dict[str, object],
    ) -> tuple[subject.JumpFit, list[dict[str, object]]]:
        attempt = {**context, "seed": 42, "jump_penalty": jump_penalty, "status": "fit_completed"}
        attempt_log.append(attempt)
        fit = subject.JumpFit(
            centers=np.zeros((state_count, len(component.feature_names)), dtype=np.float64),
            paths=(np.zeros(1, dtype=np.int64),),
            objective=1.0,
            normalized_objective=1.0,
            iterations=1,
            seed=42,
            jump_penalty=jump_penalty,
            row_count=1,
            feature_count=len(component.feature_names),
        )
        return fit, [attempt]

    monkeypatch.setattr(subject, "prepare_component", fake_prepare)
    monkeypatch.setattr(subject, "_run_restarts", fake_restarts)
    monkeypatch.setattr(subject, "semantic_mapping", lambda *args, **kwargs: {0: "risk_off", 1: "risk_on"})
    monkeypatch.setattr(subject, "causal_states", lambda *args, **kwargs: (np.zeros(1, dtype=np.int64),))
    monkeypatch.setattr(
        subject,
        "state_rows",
        lambda *args, **kwargs: {("market", fold["validation_start"]): "risk_off"},
    )
    monkeypatch.setattr(
        subject,
        "market_fold_metrics",
        lambda *args, **kwargs: {
            "metric_valid": False,
            "reason_code": subject.REASON_SELECTION_METRIC,
        },
    )

    attempt_log: list[dict[str, object]] = []
    with pytest.raises(subject.JumpSpikeError) as captured:
        subject._run_component(
            "market",
            inputs={"l2_panel": pd.DataFrame()},
            calendar=(subject.DEVELOPMENT_START, subject.DEVELOPMENT_END),
            benchmark={},
            attempt_log=attempt_log,
        )

    assert captured.value.reason_code == subject.REASON_SELECTION
    assert captured.value.stage == "lambda_selection"
    assert captured.value.evidence["component"] == "market"
    assert captured.value.evidence["lambda_receipt_count"] == 1
    lambda_receipts = captured.value.evidence["lambda_receipts"]
    assert lambda_receipts[0]["lambda_eligible"] is False
    assert lambda_receipts[0]["folds"][0]["metrics"]["reason_code"] == subject.REASON_SELECTION_METRIC
    assert captured.value.evidence["lambda_receipts_sha256"] == subject.canonical_sha256(lambda_receipts)

    def fail_select(*args: object, **kwargs: object) -> float:
        del args, kwargs
        raise TypeError("broken")

    monkeypatch.setattr(subject, "_select_lambda", fail_select)
    with pytest.raises(subject.JumpSpikeError) as unexpected:
        subject._run_component(
            "market",
            inputs={"l2_panel": pd.DataFrame()},
            calendar=(subject.DEVELOPMENT_START, subject.DEVELOPMENT_END),
            benchmark={},
            attempt_log=[],
        )
    assert unexpected.value.reason_code == subject.REASON_UNEXPECTED
    assert unexpected.value.stage == "lambda_selection"
    assert unexpected.value.evidence["exception_type"] == "TypeError"
    assert unexpected.value.evidence["error_message"] == "broken"
    assert unexpected.value.evidence["lambda_receipt_count"] == 1
    assert unexpected.value.evidence["lambda_receipts_sha256"] == subject.canonical_sha256(
        unexpected.value.evidence["lambda_receipts"]
    )


def test_cli_loader_request_stops_at_development_and_has_no_defaults() -> None:
    source = {"source_start": "2021-01-01", "source_end": subject.DEVELOPMENT_END.isoformat()}
    result = cli._loader_request({"source": source})
    assert result["source"] == source
    assert len(result["families"]) == 2
    assert all(item["train_end"] == subject.DEVELOPMENT_END.isoformat() for item in result["families"])
    with pytest.raises(subject.JumpSpikeError) as captured:
        cli._loader_request({"source": {"source_start": "2021-01-01", "source_end": subject.HOLDOUT_START.isoformat()}})
    assert captured.value.reason_code == subject.REASON_HOLDOUT
    with pytest.raises(SystemExit):
        cli.main([])


def test_cli_writes_typed_failure_sibling_without_touching_source(tmp_path: Path) -> None:
    request = tmp_path / "invalid-request.json"
    request.write_text("{not-json", encoding="utf-8")
    output = tmp_path / "candidate.json"

    result = cli.main(
        [
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
    failure = tmp_path / "candidate.failure.json"
    payload = json.loads(failure.read_text(encoding="utf-8"))
    assert payload["status"] == "NOT_AVAILABLE_FOR_PROMOTION"
    assert payload["failure_reason_code"] == subject.REASON_INPUT_IDENTITY
    assert payload["failure_receipt_write"] is True
    assert payload["database_write"] is False
    assert payload["runtime_action"] is False


def test_cli_rejects_holdout_source_before_database_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = tmp_path / "holdout-request.json"
    request.write_text(
        json.dumps(
            {
                "schema_version": subject.REQUEST_SCHEMA_VERSION,
                "contract_version": subject.CONTRACT_VERSION,
                "expected_producer_commit": "a" * 40,
                "holdout_start": subject.HOLDOUT_START.isoformat(),
                "holdout_end": subject.HOLDOUT_END.isoformat(),
                "forbidden_holdout_date_set_sha256": "b" * 64,
                "source": {
                    "source_start": "2021-01-01",
                    "source_end": subject.HOLDOUT_START.isoformat(),
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(
        cli,
        "_load_l1_source_inputs",
        lambda *args, **kwargs: pytest.fail("database loader must not run for a holdout source"),
    )
    output = tmp_path / "holdout-candidate.json"

    result = cli.main(
        [
            "--request",
            str(request),
            "--output",
            str(output),
            "--db-env-prefix",
            "UNUSED",
        ]
    )

    assert result == 1
    payload = json.loads((tmp_path / "holdout-candidate.failure.json").read_text(encoding="utf-8"))
    assert payload["failure_reason_code"] == subject.REASON_HOLDOUT
    assert payload["completed_fit_count"] == 0
    assert payload["database_write"] is False
