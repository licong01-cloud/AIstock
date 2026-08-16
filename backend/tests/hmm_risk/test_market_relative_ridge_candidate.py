from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

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
        ["--request", str(request), "--output", str(output), "--db-env-prefix", "UNUSED_FOR_INVALID_REQUEST"]
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

    result = cli.main(["--request", str(request_path), "--output", str(output), "--db-env-prefix", "UNUSED"])

    assert result == 1
    failure = json.loads((tmp_path / "holdout-candidate.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_HOLDOUT
