"""Compact comparison contracts: strict identity, partial ranks and causal maturity."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.factor_research.comparison import (
    _last_available_price_position,
    _mature_dates,
    _partial_rank_daily,
    _spearman,
    validate_comparison_spec,
)
from backend.services.factor_research.models import ResearchError

SYMBOLS = [f"{index:06d}.SZ" for index in range(1, 11)]


def comparison_spec():
    return {
        "research_role": "predictive_increment",
        "horizon": "1d",
        "baseline": ["base"],
        "candidate": "candidate",
        "controls": {"style": ["style"], "neighbors": [], "categorical": []},
        "fit_windows": [
            {
                "start": "2026-01-01",
                "end": "2026-01-05",
                "knowledge_cutoff": {"date": "2026-01-05", "phase": "post_close"},
            }
        ],
        "evaluation_windows": [{"start": "2026-01-06", "end": "2026-01-08", "fit_window_index": 0}],
        "hac_maxlags": 0,
        "knowledge_cutoff": {"date": "2026-01-08", "phase": "post_close"},
        "direction": {"source": "declared", "sign": 1, "locked_at": "2026-01-05"},
    }


def test_comparison_spec_accepts_declared_contract():
    actual = validate_comparison_spec(
        comparison_spec(), candidate_names={"base", "candidate", "style"}, repo_root=Path.cwd()
    )
    assert actual["candidate"] == "candidate"


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
def test_label_maturity_uses_trading_calendar_and_cutoff_phase(phase, last_position, expected):
    calendar = pd.DatetimeIndex(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"])
    position = _last_available_price_position(calendar, {"date": "2026-01-06", "phase": phase})
    assert position == last_position
    assert _mature_dates(calendar, position, 2).tolist() == expected
