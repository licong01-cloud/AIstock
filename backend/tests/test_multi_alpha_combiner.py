from __future__ import annotations

import math

import pandas as pd
import pytest

from backend.services.multi_alpha.combiner import CombinerLeg, MultiAlphaCombiner, MultiAlphaCombinerError


DATES = [pd.Timestamp("2026-01-02").date(), pd.Timestamp("2026-01-03").date(), pd.Timestamp("2026-01-04").date()]
INSTRUMENTS = ["A", "B", "C"]


def _frame(scores_by_date: list[list[float]]) -> pd.DataFrame:
    rows = []
    for trade_date, scores in zip(DATES, scores_by_date, strict=True):
        for instrument, score in zip(INSTRUMENTS, scores, strict=True):
            rows.append({"trade_date": trade_date, "instrument": instrument, "score": score})
    return pd.DataFrame(rows)


def _legs() -> list[CombinerLeg]:
    return [
        CombinerLeg(
            leg_id="fast",
            pred_frame=_frame([[1, 2, 3], [1, 2, 3], [1, 2, 3]]),
            ic=0.2,
            topk_return=0.1,
            realized_returns=[0.01, 0.03, 0.02],
            metric_by_date={DATES[0]: 0.1, DATES[1]: 0.8, DATES[2]: 0.8},
        ),
        CombinerLeg(
            leg_id="slow",
            pred_frame=_frame([[3, 2, 1], [3, 2, 1], [3, 2, 1]]),
            ic=-0.1,
            topk_return=0.3,
            realized_returns=[0.01, 0.01, 0.01],
            metric_by_date={DATES[0]: 0.9, DATES[1]: 0.2, DATES[2]: 0.2},
        ),
    ]


def test_equal_zscore_combines_known_scores() -> None:
    result = MultiAlphaCombiner().combine(legs=_legs(), weighting_scheme="equal", normalize_method="zscore")

    assert result.weights == {"fast": 0.5, "slow": 0.5}
    assert result.summary["row_count"] == 9
    assert result.combined_score_frame["combined_score"].abs().max() == pytest.approx(0.0)


def test_ic_weighted_uses_topk_or_ic_and_clips_negative() -> None:
    result = MultiAlphaCombiner().combine(legs=_legs(), weighting_scheme="ic_weighted", normalize_method="rank")

    assert result.weights["fast"] == pytest.approx(0.25)
    assert result.weights["slow"] == pytest.approx(0.75)
    first_day = result.combined_score_frame[result.combined_score_frame["trade_date"] == DATES[0]]
    score_a = first_day.loc[first_day["instrument"] == "A", "combined_score"].iloc[0]
    score_c = first_day.loc[first_day["instrument"] == "C", "combined_score"].iloc[0]
    assert score_a > score_c


def test_risk_parity_prefers_lower_realized_volatility() -> None:
    legs = [
        CombinerLeg("volatile", _frame([[1, 2, 3], [1, 2, 3], [1, 2, 3]]), realized_returns=[0.0, 0.2, -0.2]),
        CombinerLeg("steady", _frame([[2, 3, 4], [2, 3, 4], [2, 3, 4]]), realized_returns=[0.01, 0.02, 0.01]),
    ]

    result = MultiAlphaCombiner().combine(legs=legs, weighting_scheme="risk_parity")

    assert result.weights["steady"] > result.weights["volatile"]
    assert math.isclose(sum(result.weights.values()), 1.0)


def test_walk_forward_risk_parity_uses_dated_topk_returns_for_positive_weights() -> None:
    legs = [
        CombinerLeg(
            "volatile",
            _frame([[1, 2, 3], [1, 2, 3], [1, 2, 3]]),
            returns_by_date={DATES[0]: 0.0, DATES[1]: 0.2, DATES[2]: -0.2},
        ),
        CombinerLeg(
            "steady",
            _frame([[2, 3, 4], [2, 3, 4], [2, 3, 4]]),
            returns_by_date={DATES[0]: 0.01, DATES[1]: 0.02, DATES[2]: 0.01},
        ),
    ]

    result = MultiAlphaCombiner().combine(
        legs=legs,
        weighting_scheme="risk_parity",
        walk_forward={"enabled": True, "window": 2, "min_periods": 2},
    )

    assert result.summary["walk_forward"]["window_count"] == 1
    weights = result.per_window_weights[0]["weights"]
    assert result.per_window_weights[0]["apply_date"] == "2026-01-04"
    assert weights["steady"] > weights["volatile"] > 0
    assert math.isclose(sum(weights.values()), 1.0)


def test_walk_forward_risk_parity_reports_noncomputable_leg_details() -> None:
    legs = [
        CombinerLeg(
            "flat",
            _frame([[1, 2, 3], [1, 2, 3], [1, 2, 3]]),
            returns_by_date={DATES[0]: 0.01, DATES[1]: 0.01, DATES[2]: 0.01},
        ),
        CombinerLeg(
            "moving",
            _frame([[2, 3, 4], [2, 3, 4], [2, 3, 4]]),
            returns_by_date={DATES[0]: 0.0, DATES[1]: 0.2, DATES[2]: -0.2},
        ),
    ]

    with pytest.raises(MultiAlphaCombinerError) as excinfo:
        MultiAlphaCombiner().combine(
            legs=legs,
            weighting_scheme="risk_parity",
            walk_forward={"enabled": True, "window": 2, "min_periods": 2},
        )

    message = str(excinfo.value)
    assert "train_window=2026-01-02..2026-01-03" in message
    assert "leg=flat reason=non_positive_volatility vol=0" in message
    assert "valid_returns=2/2" in message
    assert "leg=moving reason=computable" in message


def test_risk_parity_requires_realized_returns() -> None:
    legs = [
        CombinerLeg("a", _frame([[1, 2, 3], [1, 2, 3], [1, 2, 3]])),
        CombinerLeg("b", _frame([[3, 2, 1], [3, 2, 1], [3, 2, 1]])),
    ]

    with pytest.raises(MultiAlphaCombinerError, match="requires realized_returns or returns_by_date"):
        MultiAlphaCombiner().combine(legs=legs, weighting_scheme="risk_parity")


def test_orthogonality_aware_prefers_less_correlated_leg() -> None:
    legs = [
        CombinerLeg("a", _frame([[1, 2, 3], [1, 2, 3], [1, 2, 3]])),
        CombinerLeg("b", _frame([[1, 2, 3], [1, 2, 3], [1, 2, 3]])),
        CombinerLeg("c", _frame([[2, 1, 3], [2, 1, 3], [2, 1, 3]])),
    ]

    result = MultiAlphaCombiner().combine(legs=legs, weighting_scheme="orthogonality_aware", normalize_method="rank")

    assert result.weights["c"] > result.weights["a"]
    assert result.weights["c"] > result.weights["b"]
    assert math.isclose(sum(result.weights.values()), 1.0)


def test_walk_forward_uses_only_past_metric_window() -> None:
    result = MultiAlphaCombiner().combine(
        legs=_legs(),
        weighting_scheme="ic_weighted",
        normalize_method="rank",
        walk_forward={"enabled": True, "window": 1, "min_periods": 1},
    )

    assert result.summary["walk_forward"]["window_count"] == 2
    first_oos = result.per_window_weights[0]
    second_oos = result.per_window_weights[1]
    assert first_oos["apply_date"] == "2026-01-03"
    assert first_oos["weights"]["slow"] == pytest.approx(0.9)
    assert first_oos["weights"]["fast"] == pytest.approx(0.1)
    assert second_oos["apply_date"] == "2026-01-04"
    assert second_oos["weights"]["fast"] == pytest.approx(0.8)
    assert second_oos["weights"]["slow"] == pytest.approx(0.2)
    assert set(result.combined_score_frame["trade_date"].unique()) == {DATES[1], DATES[2]}


def test_walk_forward_ic_requires_dated_metrics() -> None:
    legs = [CombinerLeg("a", _frame([[1, 2, 3], [1, 2, 3], [1, 2, 3]])), CombinerLeg("b", _frame([[3, 2, 1], [3, 2, 1], [3, 2, 1]]))]

    with pytest.raises(MultiAlphaCombinerError, match="requires metric_by_date"):
        MultiAlphaCombiner().combine(
            legs=legs,
            weighting_scheme="ic_weighted",
            walk_forward={"enabled": True, "window": 1, "min_periods": 1},
        )


def test_rejects_missing_common_rows() -> None:
    left = CombinerLeg("left", pd.DataFrame([{"trade_date": DATES[0], "instrument": "A", "score": 1.0}]))
    right = CombinerLeg("right", pd.DataFrame([{"trade_date": DATES[1], "instrument": "B", "score": 1.0}]))

    with pytest.raises(MultiAlphaCombinerError, match="no common"):
        MultiAlphaCombiner().combine(legs=[left, right])
