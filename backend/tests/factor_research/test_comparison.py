from pathlib import Path
from contextlib import contextmanager
import numpy as np
import pandas as pd
import pytest

from backend.services.factor_research.comparison import (
    _cost_report,
    _hac,
    _last_available_price_position,
    _mature_dates,
    _partial_rank_daily,
    _spearman,
    compute_comparison,
    validate_comparison_spec,
)
from backend.services.factor_research.comparison_context import enrich_catalog_context, expression_dependencies
from backend.services.factor_research.models import ResearchError
from backend.services.factor_research.runner import evaluation_context
from backend.services.factor_research.service import ResearchService


ROOT = Path(__file__).resolve().parents[3]
SYMBOLS = [f"{index:06d}.SZ" for index in range(1, 11)]


def frame(name, dates, values):
    index = pd.MultiIndex.from_product([dates, SYMBOLS], names=["datetime", "instrument"])
    return pd.DataFrame({name: np.asarray(values, dtype=float).reshape(-1)}, index=index)


def context(dates, returns_by_horizon=None):
    close = pd.DataFrame(
        np.arange(len(dates) * len(SYMBOLS), dtype=float).reshape(len(dates), -1) + 100,
        index=dates, columns=SYMBOLS,
    )
    returns = returns_by_horizon or {name: close.shift(-shift) / close.shift(-1) - 1
                                     for name, shift in {"1d": 2, "5d": 6, "10d": 11, "20d": 21}.items()}
    return {"close_unstacked": close, "fwd_ret_mats": returns, "dates": dates,
            "st_pit_eligible_mask": pd.DataFrame(True, index=dates, columns=SYMBOLS),
            "suspended_pairs": set()}


def comparison_spec(**updates):
    value = {"research_role": "predictive_increment", "horizon": "1d", "baseline": ["base"],
             "candidate": "candidate", "controls": {"style": ["style"], "neighbors": ["base"],
                                                        "categorical": []},
             "value_artifacts": {}, "fit_windows": [{"start": "2026-01-01", "end": "2026-01-08",
                                                        "knowledge_cutoff": {"date": "2026-01-08",
                                                                             "phase": "post_close"}}],
             "evaluation_windows": [{"start": "2026-01-09", "end": "2026-01-14", "fit_window_index": 0}],
             "hac_maxlags": 1,
             "knowledge_cutoff": {"date": "2026-01-14", "phase": "post_close"},
             "direction": {"source": "declared", "sign": 1, "locked_at": "2026-01-08"}}
    if "fit_window" in updates:
        fit_window = updates.pop("fit_window")
        updates["fit_windows"] = [{**fit_window, "knowledge_cutoff": fit_window.get(
            "knowledge_cutoff", {"date": fit_window["end"], "phase": "post_close"})}]
    if "evaluation_window" in updates:
        updates["evaluation_windows"] = [{**updates.pop("evaluation_window"), "fit_window_index": 0}]
    value.update(updates)
    value["fit_windows"] = [
        {**window, "knowledge_cutoff": window.get(
            "knowledge_cutoff", {"date": window["end"], "phase": "post_close"})}
        for window in value["fit_windows"]
    ]
    return value


def validated_spec(tmp_path, **updates):
    return validate_comparison_spec(comparison_spec(**updates),
                                    candidate_names={"base", "candidate", "style", "state"},
                                    repo_root=ROOT)


def first_window(result):
    return result["windows"][0]


def test_comparison_spec_is_optional_but_declared_request_is_strict(tmp_path):
    actual = validated_spec(tmp_path)
    assert actual["research_role"] == "predictive_increment" and actual["hac_maxlags"] == 1
    with pytest.raises(ResearchError, match="Unbound comparison signals"):
        validate_comparison_spec(comparison_spec(baseline=["missing"]),
                                 candidate_names={"candidate", "style"}, repo_root=ROOT)
    with pytest.raises(ResearchError, match="fit knowledge cutoff must precede"):
        validated_spec(tmp_path, fit_window={"start": "2026-01-01", "end": "2026-01-09"})
    missing_fit_cutoff = comparison_spec()
    missing_fit_cutoff["fit_windows"] = [{"start": "2026-01-01", "end": "2026-01-08"}]
    with pytest.raises(ResearchError, match="requires start, end and knowledge_cutoff"):
        validate_comparison_spec(missing_fit_cutoff, candidate_names={"base", "candidate", "style"}, repo_root=ROOT)
    with pytest.raises(ResearchError, match="one declared control role"):
        validated_spec(tmp_path, controls={"style": ["style"], "neighbors": ["style"], "categorical": []})


def test_context_dependency_scan_is_bounded_and_does_not_claim_equivalence():
    dependency = expression_dependencies("Ref($close, 5) / df['amount']")
    assert dependency["inputs"] == ["amount", "close"] and dependency["completeness"] == "unknown"
    assert expression_dependencies("dynamic(source)")["confirmation"] == "unknown"
    rows = [{"id": 1, "factor_name": "a", "source": "manual", "expression": "$close/$open"},
            {"id": 2, "factor_name": "b", "source": "qe", "expression": "$close-$open"}]
    enriched, neighbors = enrich_catalog_context(rows)
    assert enriched[0]["input_dependencies"]["inputs"] == ["close", "open"]
    assert neighbors == [{"factor_a": {"id": 1, "name": "a", "source": "manual"},
                          "factor_b": {"id": 2, "name": "b", "source": "qe"},
                          "input_jaccard": 1.0, "same_expression": False,
                          "basis": "bounded_requested_catalog_rows",
                          "interpretation": "retrieval_hint_not_equivalence"}]


def test_partial_rank_ic_residualizes_both_sides_and_never_fills_zero():
    dates = pd.date_range("2026-01-01", periods=2)
    x = pd.DataFrame([np.arange(10), np.arange(10)], index=dates, columns=SYMBOLS, dtype=float)
    y = x * 2
    eligible = x.notna()
    result = _partial_rank_daily(x, y, [x], [], eligible)
    assert result["status"] == "unavailable" and result["mean"] is None
    assert result["unavailable_by_reason"]["zero_residual_variation"] == 2


def test_rank_ic_uses_spearman_not_pearson_on_prediction_levels():
    left = pd.Series([1.0, 2.0, 4.0, 8.0, 16.0, 128.0])
    right = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0, 60.0])
    assert _spearman(left, right) == pytest.approx(1.0)
    assert left.corr(right, method="pearson") < 0.9


def test_partial_rank_ic_ranks_only_the_shared_control_sample():
    dates = pd.date_range("2026-01-01", periods=1)
    candidate = pd.DataFrame([np.arange(10)], index=dates, columns=SYMBOLS, dtype=float)
    returns = candidate.copy()
    control = pd.DataFrame([np.arange(10)], index=dates, columns=SYMBOLS, dtype=float)
    control.iloc[0, :2] = np.nan
    result = _partial_rank_daily(candidate, returns, [control], [], candidate.notna())
    assert result["status"] == "unavailable"
    assert result["unavailable_by_reason"]["zero_residual_variation"] == 1


def test_partial_rank_ic_uses_categorical_dummies_not_ordered_codes():
    dates = pd.date_range("2026-01-01", periods=1)
    candidate = pd.DataFrame([[0, 3, 1, 4, 2, 5, 6, 9, 7, 8]], index=dates, columns=SYMBOLS, dtype=float)
    returns = pd.DataFrame([[1, 4, 0, 5, 3, 2, 9, 6, 8, 7]], index=dates, columns=SYMBOLS, dtype=float)
    industries = pd.DataFrame([["bank"] * 5 + ["tech"] * 5], index=dates, columns=SYMBOLS)
    result = _partial_rank_daily(candidate, returns, [], [industries], candidate.notna())
    assert result["status"] == "computed"
    assert result["daily"][0]["n"] == 10
    assert result["raw_rank_ic_on_control_sample"]["daily"][0]["n"] == 10


def test_high_spanning_small_component_is_not_auto_rejected(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base_rows, candidate_rows, style_rows, returns = [], [], [], []
    for day_index in range(len(dates)):
        x = np.linspace(-1, 1, 10)
        signal = np.sin(np.arange(10) + day_index)
        base_rows.append(x)
        candidate_rows.append(x + 0.25 * signal)
        style_rows.append(np.cos(np.arange(10)))
        returns.append(0.3 * x + signal)
    returns_frame = pd.DataFrame(returns, index=dates, columns=SYMBOLS)
    ctx = context(dates, {"1d": returns_frame, "5d": returns_frame, "10d": returns_frame, "20d": returns_frame})
    signals = {"base": frame("base", dates, base_rows),
               "candidate": frame("candidate", dates, candidate_rows),
               "style": frame("style", dates, style_rows)}
    spec = validated_spec(tmp_path, fit_window={"start": "2026-01-01", "end": "2026-01-08"},
                          evaluation_window={"start": "2026-01-09", "end": "2026-01-16"}, hac_maxlags=0,
                          knowledge_cutoff={"date": "2026-01-16", "phase": "post_close"})
    result = compute_comparison(spec, signals, ctx, {"cutoff": "2026-01-16"})
    assert first_window(result)["span_diagnostic"]["r2"] > 0.9
    assert result["information_relation"]["classification"] == "unresolved_statistical_evidence"
    assert first_window(result)["paired_rank_ic_delta"]["daily"]


def test_multiple_evaluation_windows_bind_explicit_fit_windows(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=20)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(20)], dtype=float)
    candidate = base + np.asarray([np.sin(np.arange(10) + index) for index in range(20)])
    returns = pd.DataFrame(0.2 * base + candidate, index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, base[:, ::-1])}
    spec = validated_spec(
        tmp_path,
        fit_windows=[{"start": dates[0].date().isoformat(), "end": dates[4].date().isoformat()},
                     {"start": dates[0].date().isoformat(), "end": dates[8].date().isoformat()}],
        evaluation_windows=[{"start": dates[5].date().isoformat(), "end": dates[7].date().isoformat(),
                             "fit_window_index": 0},
                            {"start": dates[9].date().isoformat(), "end": dates[12].date().isoformat(),
                             "fit_window_index": 1}],
        knowledge_cutoff={"date": dates[-1].date().isoformat(), "phase": "post_close"},
        direction={"source": "declared", "sign": 1, "locked_at": dates[4].date().isoformat()},
        hac_maxlags=0,
    )
    ctx = context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")})
    result = compute_comparison(spec, signals, ctx, {"cutoff": dates[-1].date().isoformat()})
    assert [item["fit_window_index"] for item in result["windows"]] == [0, 1]
    assert result["windows"][1]["fit_window"] == spec["fit_windows"][1]


def test_future_label_change_does_not_change_earlier_prediction_comparison(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.tile(np.arange(10), (12, 1)).astype(float)
    candidate = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    style = np.tile(np.arange(10)[::-1], (12, 1)).astype(float)
    returns = candidate.copy()
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, style)}
    spec = validated_spec(tmp_path, fit_window={"start": "2026-01-01", "end": "2026-01-08"},
                          evaluation_window={"start": "2026-01-09", "end": "2026-01-16"}, hac_maxlags=0,
                          knowledge_cutoff={"date": "2026-01-16", "phase": "post_close"})
    returns_a = pd.DataFrame(returns, index=dates, columns=SYMBOLS)
    first = compute_comparison(spec, signals, context(dates, {name: returns_a.copy() for name in ("1d", "5d", "10d", "20d")}),
                               {"cutoff": "2026-01-16"})
    returns_b = returns_a.copy()
    returns_b.iloc[-1] = returns_b.iloc[-1, ::-1].to_numpy()
    second = compute_comparison(spec, signals, context(dates, {name: returns_b.copy() for name in ("1d", "5d", "10d", "20d")}),
                                {"cutoff": "2026-01-16"})
    assert first_window(first)["paired_rank_ic_delta"]["daily"][:-1] == first_window(second)["paired_rank_ic_delta"]["daily"][:-1]


def test_ineligible_extreme_values_do_not_change_rank_inputs_or_labels(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    candidate = base + np.asarray([np.sin(np.arange(10) + index) for index in range(12)])
    style = base[:, ::-1]
    returns = pd.DataFrame(candidate.copy(), index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, style)}
    spec = validated_spec(tmp_path, fit_window={"start": "2026-01-01", "end": "2026-01-08"},
                          evaluation_window={"start": "2026-01-09", "end": "2026-01-16"}, hac_maxlags=0,
                          knowledge_cutoff={"date": "2026-01-16", "phase": "post_close"})
    first_context = context(dates, {name: returns.copy() for name in ("1d", "5d", "10d", "20d")})
    first_context["st_pit_eligible_mask"].loc[:, SYMBOLS[-1]] = False
    second_context = context(dates, {name: returns.copy() for name in ("1d", "5d", "10d", "20d")})
    second_context["st_pit_eligible_mask"].loc[:, SYMBOLS[-1]] = False
    for value in second_context["fwd_ret_mats"].values():
        value.loc[:, SYMBOLS[-1]] = 1e12
    altered = signals["candidate"].copy()
    altered.loc[(slice(None), SYMBOLS[-1]), "candidate"] = -1e12
    second_signals = {**signals, "candidate": altered}
    first = compute_comparison(spec, signals, first_context, {"cutoff": "2026-01-16"})
    second = compute_comparison(spec, second_signals, second_context, {"cutoff": "2026-01-16"})
    assert first_window(first)["paired_rank_ic_delta"]["daily"] == first_window(second)["paired_rank_ic_delta"]["daily"]


def test_missing_diagnostic_control_does_not_shrink_prediction_comparison(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    candidate = base + np.asarray([np.sin(np.arange(10) + index) for index in range(12)])
    returns = pd.DataFrame(0.2 * base + candidate, index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, np.full_like(base, np.nan))}
    spec = validated_spec(tmp_path, fit_window={"start": "2026-01-01", "end": "2026-01-08"},
                          evaluation_window={"start": "2026-01-09", "end": "2026-01-16"}, hac_maxlags=0,
                          knowledge_cutoff={"date": "2026-01-16", "phase": "post_close"})
    result = compute_comparison(spec, signals, context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")}),
                                {"cutoff": "2026-01-16"})
    assert first_window(result)["paired_rank_ic_delta"]["daily"]
    assert first_window(result)["partial_rank_ic_style"]["status"] == "unavailable"


def test_label_maturity_uses_shared_matrix_and_keeps_immature_tail_out(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=55)
    rows = np.asarray([np.roll(np.arange(10), index) for index in range(55)], dtype=float)
    signals = {"base": frame("base", dates, rows),
               "candidate": frame("candidate", dates, rows + np.sin(np.arange(550)).reshape(55, 10)),
               "style": frame("style", dates, rows[:, ::-1])}
    ctx = context(dates)
    spec = validated_spec(
        tmp_path, horizon="20d",
        fit_window={"start": dates[0].date().isoformat(), "end": dates[5].date().isoformat(),
                    "knowledge_cutoff": {"date": dates[26].date().isoformat(), "phase": "post_close"}},
        evaluation_window={"start": dates[27].date().isoformat(), "end": dates[-1].date().isoformat()},
        hac_maxlags=0, knowledge_cutoff={"date": dates[-1].date().isoformat(), "phase": "post_close"},
        direction={"source": "declared", "sign": 1, "locked_at": dates[5].date().isoformat()},
    )
    result = compute_comparison(spec, signals, ctx, {"cutoff": dates[-1].date().isoformat()})
    observed = first_window(result)["paired_rank_ic_delta"]["daily"]
    assert observed and max(item["date"] for item in observed) == dates[33].date().isoformat()
    assert all(item["date"] < dates[34].date().isoformat() for item in observed)
    assert first_window(result)["fit_label_maturity"]["mature_fit_rows"] == 60


@pytest.mark.parametrize(("phase", "evaluation_end", "last_price_index"), [
    ("post_close", 11, 11),
    ("pre_open", 10, 10),
])
def test_cutoff_phase_changes_last_available_close_without_date_arithmetic(
        tmp_path, phase, evaluation_end, last_price_index):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    candidate = base + np.asarray([np.sin(np.arange(10) + index) for index in range(12)])
    returns = pd.DataFrame(0.2 * base + candidate, index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, base[:, ::-1])}
    cutoff = dates[11].date().isoformat()
    spec = validated_spec(tmp_path, fit_window={"start": dates[0].date().isoformat(), "end": dates[5].date().isoformat()},
                          evaluation_window={"start": dates[6].date().isoformat(),
                                             "end": dates[evaluation_end].date().isoformat()},
                          hac_maxlags=0, knowledge_cutoff={"date": cutoff, "phase": phase},
                          direction={"source": "declared", "sign": 1, "locked_at": dates[5].date().isoformat()})
    result = compute_comparison(spec, signals, context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")}),
                                {"cutoff": cutoff})
    assert result["knowledge_cutoff"]["last_available_price_date"] == dates[last_price_index].date().isoformat()


def test_label_maturity_boundary_uses_trading_calendar_and_cutoff_phase():
    calendar = pd.DatetimeIndex(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"])
    post_close = _last_available_price_position(calendar, {"date": "2026-01-06", "phase": "post_close"})
    pre_open = _last_available_price_position(calendar, {"date": "2026-01-06", "phase": "pre_open"})
    assert post_close == 2 and pre_open == 1
    assert _mature_dates(calendar, post_close, 2).tolist() == [True, False, False, False]
    assert _mature_dates(calendar, pre_open, 2).tolist() == [False, False, False, False]


def test_label_maturity_uses_read_tail_calendar_after_signal_slice(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=15)
    values = np.asarray([np.roll(np.arange(10), index) for index in range(15)], dtype=float)
    close = pd.DataFrame(values + 100, index=dates, columns=SYMBOLS)
    returns = close.shift(-6) / close.shift(-1) - 1
    raw_context = context(dates, {name: returns.copy() for name in ("1d", "5d", "10d", "20d")})
    raw_context["close_unstacked"] = close
    run = {"signal_start": dates[0].date().isoformat(), "signal_end": dates[9].date().isoformat()}
    sliced = evaluation_context(raw_context, run)
    signals = {"base": frame("base", dates[:10], values[:10]),
               "candidate": frame("candidate", dates[:10], values[:10] + np.sin(np.arange(100)).reshape(10, 10)),
               "style": frame("style", dates[:10], values[:10, ::-1])}
    spec = validated_spec(
        tmp_path, horizon="5d",
        fit_window={"start": dates[0].date().isoformat(), "end": dates[1].date().isoformat(),
                    "knowledge_cutoff": {"date": dates[7].date().isoformat(), "phase": "post_close"}},
        evaluation_window={"start": dates[8].date().isoformat(), "end": dates[9].date().isoformat()},
        knowledge_cutoff={"date": dates[14].date().isoformat(), "phase": "post_close"}, hac_maxlags=0,
        direction={"source": "declared", "sign": 1, "locked_at": dates[1].date().isoformat()},
    )
    result = compute_comparison(spec, signals, sliced, {"cutoff": dates[14].date().isoformat()})
    daily = first_window(result)["paired_rank_ic_delta"]["daily"]
    assert [item["date"] for item in daily] == [dates[8].date().isoformat()]
    assert result["knowledge_cutoff"]["last_available_price_date"] == dates[14].date().isoformat()


def test_fit_uses_only_labels_mature_by_its_own_cutoff(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    candidate = base + np.asarray([np.sin(np.arange(10) + index) for index in range(12)])
    returns = pd.DataFrame(candidate, index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, base[:, ::-1])}
    spec = validated_spec(
        tmp_path,
        fit_window={"start": dates[0].date().isoformat(), "end": dates[5].date().isoformat(),
                    "knowledge_cutoff": {"date": dates[5].date().isoformat(), "phase": "post_close"}},
        evaluation_window={"start": dates[6].date().isoformat(), "end": dates[-1].date().isoformat()},
        knowledge_cutoff={"date": dates[-1].date().isoformat(), "phase": "post_close"},
        direction={"source": "declared", "sign": 1, "locked_at": dates[5].date().isoformat()},
        hac_maxlags=0,
    )
    result = compute_comparison(
        spec, signals, context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")}),
        {"cutoff": dates[-1].date().isoformat()},
    )
    assert first_window(result)["fit_label_maturity"]["mature_fit_rows"] == 40
    assert first_window(result)["baseline_fit"]["rows"] == 40


def test_declared_direction_orients_candidate_diagnostics(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    candidate = np.asarray([np.roll(np.arange(10), index + 2) for index in range(12)], dtype=float)
    returns = pd.DataFrame(candidate, index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, base[:, ::-1])}
    common_updates = {
        "fit_window": {"start": dates[0].date().isoformat(), "end": dates[5].date().isoformat()},
        "evaluation_window": {"start": dates[6].date().isoformat(), "end": dates[-1].date().isoformat()},
        "knowledge_cutoff": {"date": dates[-1].date().isoformat(), "phase": "post_close"},
        "hac_maxlags": 0,
    }
    ctx = context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")})
    positive = compute_comparison(
        validated_spec(tmp_path, **common_updates,
                       direction={"source": "declared", "sign": 1, "locked_at": dates[5].date().isoformat()}),
        signals, ctx, {"cutoff": dates[-1].date().isoformat()},
    )
    negative = compute_comparison(
        validated_spec(tmp_path, **common_updates,
                       direction={"source": "declared", "sign": -1, "locked_at": dates[5].date().isoformat()}),
        signals, ctx, {"cutoff": dates[-1].date().isoformat()},
    )
    assert first_window(positive)["candidate_raw_rank_ic"]["mean"] == pytest.approx(
        -first_window(negative)["candidate_raw_rank_ic"]["mean"]
    )


def test_coverage_discloses_pit_input_and_label_losses(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    candidate = base + np.asarray([np.sin(np.arange(10) + index) for index in range(12)])
    returns = pd.DataFrame(candidate, index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, base[:, ::-1])}
    signals["candidate"].loc[(dates[6], SYMBOLS[0]), "candidate"] = np.nan
    ctx = context(dates, {name: returns.copy() for name in ("1d", "5d", "10d", "20d")})
    ctx["st_pit_eligible_mask"].loc[dates[6], SYMBOLS[1]] = False
    ctx["fwd_ret_mats"]["1d"].loc[dates[7], SYMBOLS[2]] = np.nan
    spec = validated_spec(
        tmp_path,
        fit_window={"start": dates[0].date().isoformat(), "end": dates[5].date().isoformat()},
        evaluation_window={"start": dates[6].date().isoformat(), "end": dates[-1].date().isoformat()},
        knowledge_cutoff={"date": dates[-1].date().isoformat(), "phase": "post_close"},
        direction={"source": "declared", "sign": 1, "locked_at": dates[5].date().isoformat()},
        hac_maxlags=0,
    )
    coverage = first_window(compute_comparison(
        spec, signals, ctx, {"cutoff": dates[-1].date().isoformat()},
    ))["coverage"]
    assert coverage["grid_opportunities"] == 60
    assert coverage["losses"]["pit_ineligible"] == 1
    assert coverage["losses"]["candidate_missing_on_pit"] == 1
    assert coverage["losses"]["label_missing_despite_maturity"] == 1
    assert coverage["losses"]["label_not_mature_on_common_input"] == 20


def test_invalid_return_or_pit_context_fails_closed(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    values = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    signals = {"base": frame("base", dates, values), "candidate": frame("candidate", dates, values + 0.1),
               "style": frame("style", dates, values[:, ::-1])}
    returns = pd.DataFrame(values, index=dates, columns=SYMBOLS)
    spec = validated_spec(tmp_path)
    invalid_returns = context(dates, {name: returns.copy() for name in ("1d", "5d", "10d", "20d")})
    invalid_returns["fwd_ret_mats"]["1d"].iloc[0, 0] = np.inf
    with pytest.raises(ResearchError, match="infinite"):
        compute_comparison(spec, signals, invalid_returns, {"cutoff": "2026-01-14"})
    invalid_mask = context(dates, {name: returns.copy() for name in ("1d", "5d", "10d", "20d")})
    invalid_mask["st_pit_eligible_mask"] = invalid_mask["st_pit_eligible_mask"].astype(object)
    invalid_mask["st_pit_eligible_mask"].iloc[0, 0] = "yes"
    with pytest.raises(ResearchError, match="eligibility mask must be boolean"):
        compute_comparison(spec, signals, invalid_mask, {"cutoff": "2026-01-14"})


def test_declared_exact_duplicate_reports_construction_and_degenerate_fit(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    returns = pd.DataFrame(base, index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, base),
               "style": frame("style", dates, base[:, ::-1])}
    spec = validated_spec(
        tmp_path, fit_window={"start": "2026-01-01", "end": "2026-01-08"},
        evaluation_window={"start": "2026-01-09", "end": "2026-01-16"}, hac_maxlags=0,
        knowledge_cutoff={"date": "2026-01-16", "phase": "post_close"},
        construction={"relation": "exact_duplicate", "baseline": "base",
                      "evidence": "reviewed fixture uses the same values"},
    )
    result = compute_comparison(spec, signals, context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")}),
                                {"cutoff": "2026-01-16"})
    assert result["information_relation"]["classification"] == "declared_exact_duplicate_values_verified"
    assert result["information_relation"]["independent_source_review"] == "not_performed_by_comparison_runtime"
    assert first_window(result)["augmented_fit"]["fit_status"] == "computed_rank_deficient"
    assert first_window(result)["span_diagnostic"]["reason"] == "zero_residual_variation"


def test_declared_construction_mismatch_fails_closed(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.tile(np.arange(10), (12, 1)).astype(float)
    candidate = base.copy()
    candidate[:, 0] += 1
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, base[:, ::-1])}
    spec = validated_spec(
        tmp_path, fit_window={"start": "2026-01-01", "end": "2026-01-08"},
        evaluation_window={"start": "2026-01-09", "end": "2026-01-16"}, hac_maxlags=0,
        knowledge_cutoff={"date": "2026-01-16", "phase": "post_close"},
        construction={"relation": "exact_duplicate", "baseline": "base", "evidence": "incorrect declaration"},
    )
    returns = pd.DataFrame(candidate, index=dates, columns=SYMBOLS)
    with pytest.raises(ResearchError, match="Declared exact duplicate differs"):
        compute_comparison(spec, signals, context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")}),
                           {"cutoff": "2026-01-16"})


def test_declared_decreasing_transform_reports_rank_direction_without_claiming_value(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, -base),
               "style": frame("style", dates, base[:, ::-1])}
    returns = pd.DataFrame(base, index=dates, columns=SYMBOLS)
    spec = validated_spec(
        tmp_path, fit_window={"start": dates[0].date().isoformat(), "end": dates[5].date().isoformat()},
        evaluation_window={"start": dates[6].date().isoformat(), "end": dates[-1].date().isoformat()},
        knowledge_cutoff={"date": dates[-1].date().isoformat(), "phase": "post_close"}, hac_maxlags=0,
        direction={"source": "declared", "sign": -1, "locked_at": dates[5].date().isoformat()},
        construction={"relation": "known_transform", "baseline": "base", "monotonicity": "decreasing",
                      "evidence": "reviewed fixture defines candidate as negative baseline"},
    )
    result = compute_comparison(
        spec, signals, context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")}),
        {"cutoff": dates[-1].date().isoformat()},
    )
    assert result["information_relation"]["classification"] == "declared_known_transform_ranks_verified"
    assert result["use_value"]["classification"] == "evidence_insufficient_until_interpreted_for_declared_role"


def test_conditional_role_uses_only_predeclared_interaction(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.tile(np.arange(10), (12, 1)).astype(float)
    candidate = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    state = np.tile(np.where(np.arange(10) < 5, -1, 1), (12, 1)).astype(float)
    returns = candidate * state
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, base[:, ::-1]), "state": frame("state", dates, state)}
    spec = validated_spec(tmp_path, research_role="conditional", state="state",
                          fit_window={"start": "2026-01-01", "end": "2026-01-08"},
                          evaluation_window={"start": "2026-01-09", "end": "2026-01-16"}, hac_maxlags=0,
                          knowledge_cutoff={"date": "2026-01-16", "phase": "post_close"})
    ctx = context(dates, {name: pd.DataFrame(returns, index=dates, columns=SYMBOLS)
                          for name in ("1d", "5d", "10d", "20d")})
    result = compute_comparison(spec, signals, ctx, {"cutoff": "2026-01-16"})
    assert "__candidate_state_interaction__" in first_window(result)["augmented_fit"]["features"]


def test_replacement_role_replaces_only_the_declared_baseline_component(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=12)
    base = np.asarray([np.roll(np.arange(10), index) for index in range(12)], dtype=float)
    style = base[:, ::-1]
    candidate = np.asarray([np.sin(np.arange(10) + index) for index in range(12)])
    returns = pd.DataFrame(base + candidate, index=dates, columns=SYMBOLS)
    signals = {"base": frame("base", dates, base), "candidate": frame("candidate", dates, candidate),
               "style": frame("style", dates, style)}
    spec = validated_spec(
        tmp_path, research_role="replacement", baseline=["base", "style"], replacement_target="style",
        controls={"style": [], "neighbors": ["base"], "categorical": []},
        fit_window={"start": dates[0].date().isoformat(), "end": dates[5].date().isoformat()},
        evaluation_window={"start": dates[6].date().isoformat(), "end": dates[-1].date().isoformat()},
        knowledge_cutoff={"date": dates[-1].date().isoformat(), "phase": "post_close"}, hac_maxlags=0,
        direction={"source": "declared", "sign": 1, "locked_at": dates[5].date().isoformat()},
    )
    result = compute_comparison(
        spec, signals, context(dates, {name: returns for name in ("1d", "5d", "10d", "20d")}),
        {"cutoff": dates[-1].date().isoformat()},
    )
    assert first_window(result)["baseline_fit"]["features"] == ["base", "style"]
    assert first_window(result)["augmented_fit"]["features"] == ["base", "candidate"]


def test_hac_reports_calendar_gaps_as_separate_segments():
    calendar = pd.bdate_range("2026-01-01", periods=5)
    values = [{"date": calendar[0].date().isoformat(), "value": 0.1},
              {"date": calendar[1].date().isoformat(), "value": 0.2},
              {"date": calendar[3].date().isoformat(), "value": -0.1}]
    result = _hac(values, 0, calendar)
    assert len(result["segments"]) == 2
    assert result["overall_t_value"] is None
    assert result["reason"] == "reported_by_contiguous_segment"


def test_cost_report_uses_actual_weight_changes_and_preserves_negative_break_even(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=3)
    left_weights = pd.DataFrame([[1, 0], [0.5, 0.5], [0.5, 0.5]], index=dates, columns=["a", "b"])
    right_weights = pd.DataFrame([[1, 0], [0, 1], [0, 1]], index=dates, columns=["a", "b"])
    left_returns = pd.DataFrame({"gross": [0.01, 0.0, 0.0]}, index=dates)
    right_returns = pd.DataFrame({"gross": [0.0, 0.0, 0.0]}, index=dates)
    paths = {}
    for name, value in (("baseline_weights", left_weights), ("augmented_weights", right_weights),
                        ("baseline_gross_returns", left_returns), ("augmented_gross_returns", right_returns)):
        path = tmp_path / f"{name}.h5"
        value.to_hdf(path, key="data")
        paths[name] = str(path)
    report = _cost_report({**paths, "currency": "CNY", "capital_normalization": "unit_nav",
                           "return_basis": "arithmetic_same_frequency",
                           "scenarios": [{"name": "30bp", "buy_bps": 30,
                                                    "sell_bps": 30, "fixed_cost": 0.0}],
                           "liquidate_at_end": False})
    assert report["status"] == "computed" and report["augmented_buy"] > report["baseline_buy"]
    assert report["scenarios"][0]["break_even_common_variable_bps"] < 0


def test_cost_without_declared_paths_is_explicitly_unavailable():
    assert _cost_report(None) == {"status": "unavailable", "reason": "weight_and_return_paths_not_declared"}


def test_cost_paths_cannot_silently_intersect_different_dates(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=3)
    paths = {}
    values = {
        "baseline_weights": pd.DataFrame([[1.0], [1.0], [1.0]], index=dates, columns=["a"]),
        "augmented_weights": pd.DataFrame([[1.0], [1.0]], index=dates[:2], columns=["a"]),
        "baseline_gross_returns": pd.DataFrame({"gross": [0.0, 0.0, 0.0]}, index=dates),
        "augmented_gross_returns": pd.DataFrame({"gross": [0.0, 0.0, 0.0]}, index=dates),
    }
    for name, value in values.items():
        path = tmp_path / f"{name}.h5"
        value.to_hdf(path, key="data")
        paths[name] = str(path)
    with pytest.raises(ResearchError, match="same explicit date path"):
        _cost_report({**paths, "currency": "CNY", "capital_normalization": "unit_nav",
                      "return_basis": "arithmetic_same_frequency",
                      "scenarios": [{"name": "zero", "buy_bps": 0, "sell_bps": 0,
                                     "fixed_cost": 0}], "liquidate_at_end": False})


def test_context_returns_expression_dependencies_and_only_bounded_neighbors():
    catalog = [{"id": 1, "factor_name": "a", "source": "manual", "asset_path": None,
                "is_available": True, "expression": "$close/$open", "description_cn": "a"},
               {"id": 2, "factor_name": "b", "source": "qe", "asset_path": None,
                "is_available": True, "expression": "$close-$open", "description_cn": "b"}]

    class Cursor:
        def __init__(self):
            self.call = 0

        def execute(self, *_args):
            self.call += 1

        def fetchall(self):
            return catalog if self.call == 1 else []

    class Repo:
        @contextmanager
        def cursor(self):
            yield Cursor()

    result = ResearchService(Repo(), target={"target": "dev"}).context(
        ["a", "b"], start_date="2026-01-01", end_date="2026-01-31", limit=10,
    )
    assert result["catalog"][0]["input_dependencies"]["inputs"] == ["close", "open"]
    assert len(result["comparison_context"]["neighbor_hints"]) == 1
    assert result["comparison_context"]["scope"] == "bounded_requested_catalog_rows"
