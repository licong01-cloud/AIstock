"""Compact comparison contracts: strict identity, partial ranks and causal maturity."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.factor_research.comparison import (
    _daily_rank_ic,
    _last_available_price_position,
    _mature_dates,
    _partial_rank_daily,
    _spearman,
    compute_comparison,
    validate_comparison_spec,
)
from backend.services.factor_research.models import ResearchError
from backend.services.factor_research import runner
from backend.tests.factor_research.test_contracts import spec as run_spec

SYMBOLS = [f"{index:06d}.SZ" for index in range(1, 11)]


@pytest.mark.parametrize("missing", [np.nan, np.inf, -np.inf])
def test_daily_ranks_use_common_finite_eligible_pairs(missing):
    day = pd.Timestamp("2026-01-05")
    left = pd.DataFrame([[1, 2, 3, missing] * 3 + [100]], index=[day])
    right = pd.DataFrame([[1, 4, 2, 3] * 3 + [-100]], index=[day])
    eligible = pd.DataFrame(True, index=left.index, columns=left.columns)
    eligible.iloc[0, -1] = False
    for x, y in ((left, right), (right, left)):
        actual = _daily_rank_ic(x, y, eligible)
        assert len(actual) == 1
        assert actual[0] == {"date": "2026-01-05", "value": pytest.approx(0.5), "n": 9}
    assert _daily_rank_ic(pd.DataFrame(1.0, index=left.index, columns=left.columns), right, eligible) == []
    assert _daily_rank_ic(left.iloc[:, :4], right.iloc[:, :4], eligible.iloc[:, :4]) == []


def comparison_spec():
    return {
        "research_role": "predictive_increment", "horizon": "1d", "baseline": ["base"], "candidate": "candidate",
        "controls": {"style": ["style"], "neighbors": [], "categorical": []},
        "fit_windows": [{"start": "2026-01-01", "end": "2026-01-05",
                         "knowledge_cutoff": {"date": "2026-01-05", "phase": "post_close"}}],
        "evaluation_windows": [{"start": "2026-01-06", "end": "2026-01-08", "fit_window_index": 0}],
        "hac_maxlags": 0,
        "knowledge_cutoff": {"date": "2026-01-08", "phase": "post_close"},
        "direction": {"source": "declared", "sign": 1, "locked_at": "2026-01-05"},
    }


@pytest.mark.parametrize(
    "mutation,match",
    [
        (lambda value: value.update(candidate="base"), "distinct"),
        (lambda value: value["controls"]["neighbors"].append("style"), "one declared control role"),
        (lambda value: value["fit_windows"][0]["knowledge_cutoff"].update(phase="pre_open"), "available"),
    ],
)
def test_comparison_identity_and_timing_are_strict(mutation, match):
    value = comparison_spec()
    mutation(value)
    with pytest.raises(ResearchError, match=match):
        validate_comparison_spec(value, candidate_names={"base", "candidate", "style"}, repo_root=Path.cwd())


def test_partial_rank_residualizes_both_sides_and_never_fills_zero():
    dates = pd.date_range("2026-01-01", periods=2)
    values = pd.DataFrame([np.arange(10)] * 2, index=dates, columns=SYMBOLS, dtype=float)
    result = _partial_rank_daily(values, values * 2, [values], [], values.notna())
    assert result["status"] == "unavailable"
    assert result["mean"] is None and result["unavailable_by_reason"]["zero_residual_variation"] == 2
    assert _spearman(pd.Series(range(6)), pd.Series(range(5, -1, -1))) == pytest.approx(-1)


@pytest.mark.parametrize(
    "phase,last_position,expected",
    [
        ("post_close", 2, [True, False, False, False]),
        ("pre_open", 1, [False, False, False, False]),
    ],
)
def test_label_maturity_uses_trading_calendar_and_cutoff_phase(phase, last_position, expected, monkeypatch):
    calendar = pd.DatetimeIndex(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"])
    position = _last_available_price_position(calendar, {"date": "2026-01-06", "phase": phase})
    assert position == last_position
    assert _mature_dates(calendar, position, 2).tolist() == expected
    frame = pd.DataFrame([np.arange(10)] * 4, index=calendar, columns=SYMBOLS, dtype=float)
    settings = {**comparison_spec(), "horizon": "60d"}
    settings = validate_comparison_spec(settings, candidate_names={"base", "candidate", "style"}, repo_root=Path.cwd())
    ctx = {"close_unstacked": frame, "label_calendar": calendar, "holding_periods": {"60d": 61},
           "fwd_ret_mats": {"60d": frame}, "st_pit_eligible_mask": frame.notna()}
    signals = {name: frame.rename_axis(index="datetime", columns="instrument").stack().to_frame(name) for name in ("base", "candidate", "style")}
    monkeypatch.setattr("backend.services.factor_research.comparison._compute_window_result", lambda **kw: {"shift": kw["shift_n"], "mature": kw["mature_dates"].tolist()})
    report = compute_comparison(settings, signals, ctx, {"cutoff": "2026-01-08"})
    assert report["windows"] == [{"shift": 61, "mature": [False] * 4}]
    ctx["holding_periods"] = {"20d": 21}
    with pytest.raises(ResearchError, match="engine mapping"):
        compute_comparison(settings, signals, ctx, {"cutoff": "2026-01-08"})

@pytest.mark.parametrize("periods", [[], [True], [60.0], [2], "60d"])
def test_research_rejects_invalid_horizon_requests_before_execution(tmp_path, periods):
    with pytest.raises(ResearchError, match="holding_periods"):
        runner.validate_spec({**run_spec(tmp_path), "holding_periods": periods})
